use std::{collections::BTreeSet, sync::Arc};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::{FutureExt as _, future::Either};
use lenso_capability_http_client::{
    ClientInvocationError, ClientProvider, SendError, SendRequest, SendResponse,
    SendResponseHeadersItem,
};
use lenso_kernel::{InvocationContext, RuntimeFailure};
use reqwest::{
    Method, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{HttpEgressConfig, request_origin};

const FORBIDDEN_REQUEST_HEADERS: &[&str] = &[
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

#[derive(Clone, Debug)]
pub(crate) struct HttpEgressProvider {
    client: reqwest::Client,
    config: HttpEgressConfig,
    allowed_origins: BTreeSet<String>,
    permits: Arc<Semaphore>,
}

impl HttpEgressProvider {
    pub(crate) fn new(
        client: reqwest::Client,
        config: HttpEgressConfig,
        allowed_origins: BTreeSet<String>,
    ) -> Self {
        let permits = Arc::new(Semaphore::new(config.max_concurrent_requests()));
        Self {
            client,
            config,
            allowed_origins,
            permits,
        }
    }

    async fn execute(
        &self,
        request: SendRequest,
        _permit: OwnedSemaphorePermit,
    ) -> Result<SendResponse, ClientInvocationError> {
        let prepared = self.prepare_request(request)?;
        let response = self
            .client
            .request(prepared.method, prepared.url)
            .headers(prepared.headers)
            .body(prepared.body)
            .send()
            .await
            .map_err(|error| classify_transport_error(&error))?;
        self.read_response(response).await
    }

    fn prepare_request(
        &self,
        request: SendRequest,
    ) -> Result<PreparedRequest, ClientInvocationError> {
        if request.method.len() > 32 || request.url.len() > 4_096 {
            return Err(invalid_request());
        }
        let method =
            Method::from_bytes(request.method.as_bytes()).map_err(|_| invalid_request())?;
        if matches!(method, Method::CONNECT | Method::TRACE) {
            return Err(invalid_request());
        }
        let url = Url::parse(&request.url).map_err(|_| invalid_request())?;
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err(invalid_request());
        }
        let origin = request_origin(&url).ok_or_else(invalid_request)?;
        if !self.allowed_origins.contains(&origin) {
            return Err(ClientInvocationError::Domain(
                SendError::DestinationNotAllowed,
            ));
        }
        let encoded_limit = self
            .config
            .max_request_body_bytes()
            .saturating_mul(4)
            .div_ceil(3)
            .saturating_add(4);
        if request.body.len() > encoded_limit {
            return Err(ClientInvocationError::Domain(SendError::RequestTooLarge));
        }
        let body = STANDARD
            .decode(request.body)
            .map_err(|_| invalid_request())?;
        if body.len() > self.config.max_request_body_bytes() {
            return Err(ClientInvocationError::Domain(SendError::RequestTooLarge));
        }
        let headers =
            parse_request_headers(&request.headers, self.config.max_request_head_bytes())?;
        Ok(PreparedRequest {
            method,
            url,
            headers,
            body,
        })
    }

    async fn read_response(
        &self,
        mut response: reqwest::Response,
    ) -> Result<SendResponse, ClientInvocationError> {
        let status = i64::from(response.status().as_u16());
        let headers = response_headers(response.headers(), self.config.max_response_head_bytes())?;
        if response
            .content_length()
            .is_some_and(|length| length > self.config.max_response_body_bytes() as u64)
        {
            return Err(ClientInvocationError::Domain(SendError::ResponseTooLarge));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| classify_transport_error(&error))?
        {
            if body.len().saturating_add(chunk.len()) > self.config.max_response_body_bytes() {
                return Err(ClientInvocationError::Domain(SendError::ResponseTooLarge));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(SendResponse {
            body: STANDARD.encode(body),
            headers,
            status,
        })
    }
}

impl ClientProvider for HttpEgressProvider {
    fn send(
        &self,
        context: InvocationContext,
        request: SendRequest,
    ) -> futures::future::LocalBoxFuture<'static, Result<SendResponse, ClientInvocationError>> {
        let provider = self.clone();
        Box::pin(async move {
            let request_id = context.request_id();
            let cancellation = context.cancellation();
            if cancellation.is_cancelled() {
                return Err(ClientInvocationError::Runtime(RuntimeFailure::Cancelled {
                    request_id,
                }));
            }
            let permit = provider.permits.clone().try_acquire_owned().map_err(|_| {
                ClientInvocationError::Runtime(RuntimeFailure::ResourceExhausted {
                    capability: lenso_capability_http_client::CAPABILITY_ID,
                    operation: lenso_capability_http_client::SEND_OPERATION.to_owned(),
                })
            })?;
            let operation = provider.execute(request, permit).fuse();
            let cancelled = cancellation.cancelled().fuse();
            futures::pin_mut!(operation, cancelled);
            match futures::future::select(cancelled, operation).await {
                Either::Left(((), _)) => {
                    Err(ClientInvocationError::Runtime(RuntimeFailure::Cancelled {
                        request_id,
                    }))
                }
                Either::Right((result, _)) => result,
            }
        })
    }
}

struct PreparedRequest {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: Vec<u8>,
}

fn parse_request_headers(
    headers: &[lenso_capability_http_client::SendRequestHeadersItem],
    max_head_bytes: usize,
) -> Result<HeaderMap, ClientInvocationError> {
    let mut parsed = HeaderMap::new();
    let mut head_bytes = 0_usize;
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| invalid_request())?;
        if FORBIDDEN_REQUEST_HEADERS.contains(&name.as_str()) {
            return Err(invalid_request());
        }
        let value = HeaderValue::from_str(&header.value).map_err(|_| invalid_request())?;
        head_bytes = head_bytes
            .checked_add(name.as_str().len() + value.as_bytes().len() + 4)
            .ok_or_else(request_too_large)?;
        if head_bytes > max_head_bytes {
            return Err(request_too_large());
        }
        parsed.append(name, value);
    }
    Ok(parsed)
}

fn response_headers(
    headers: &HeaderMap,
    max_head_bytes: usize,
) -> Result<Vec<SendResponseHeadersItem>, ClientInvocationError> {
    let mut connection_headers = BTreeSet::new();
    for value in headers.get_all("connection") {
        let value = value
            .to_str()
            .map_err(|_| ClientInvocationError::Domain(SendError::TransportFailure))?;
        for name in value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ClientInvocationError::Domain(SendError::TransportFailure))?;
            connection_headers.insert(name.as_str().to_owned());
        }
    }
    let mut result = Vec::with_capacity(headers.len());
    let mut head_bytes = 0_usize;
    for (name, value) in headers {
        if FORBIDDEN_REQUEST_HEADERS.contains(&name.as_str())
            || connection_headers.contains(name.as_str())
        {
            continue;
        }
        head_bytes = head_bytes
            .checked_add(name.as_str().len() + value.as_bytes().len() + 4)
            .ok_or_else(response_too_large)?;
        if head_bytes > max_head_bytes {
            return Err(response_too_large());
        }
        result.push(SendResponseHeadersItem {
            name: name.as_str().to_owned(),
            value: value
                .to_str()
                .map_err(|_| ClientInvocationError::Domain(SendError::TransportFailure))?
                .to_owned(),
        });
    }
    Ok(result)
}

fn classify_transport_error(error: &reqwest::Error) -> ClientInvocationError {
    ClientInvocationError::Domain(if error.is_timeout() {
        SendError::Timeout
    } else {
        SendError::TransportFailure
    })
}

fn invalid_request() -> ClientInvocationError {
    ClientInvocationError::Domain(SendError::InvalidRequest)
}

fn request_too_large() -> ClientInvocationError {
    ClientInvocationError::Domain(SendError::RequestTooLarge)
}

fn response_too_large() -> ClientInvocationError {
    ClientInvocationError::Domain(SendError::ResponseTooLarge)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{Router, routing::get};
    use lenso_kernel::CancellationToken;
    use reqwest::redirect::Policy;
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_provider_calls_fail_closed_at_the_instance_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/slow",
                    get(|| async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        "ok"
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let config = HttpEgressConfig::new([format!("http://{address}")])
            .unwrap()
            .with_max_concurrent_requests(1)
            .unwrap();
        let allowed_origins = config.validate().unwrap();
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .timeout(config.request_timeout())
            .build()
            .unwrap();
        let provider = HttpEgressProvider::new(client, config, allowed_origins);
        let request = || SendRequest {
            body: STANDARD.encode([]),
            headers: Vec::new(),
            method: "GET".to_owned(),
            url: format!("http://{address}/slow"),
        };
        let context =
            |request_id| InvocationContext::new(request_id, None, CancellationToken::new());

        let (first, second) = tokio::join!(
            provider.send(context(1), request()),
            provider.send(context(2), request())
        );
        assert_eq!(first.unwrap().status, 200);
        assert!(matches!(
            second,
            Err(ClientInvocationError::Runtime(
                RuntimeFailure::ResourceExhausted { capability, operation }
            )) if capability == lenso_capability_http_client::CAPABILITY_ID
                && operation == lenso_capability_http_client::SEND_OPERATION
        ));
        upstream.abort();
    }
}
