use std::{collections::BTreeSet, sync::Arc};

use bytes::{Bytes, BytesMut};
use futures::{FutureExt as _, future::Either};
use lenso_capability_http_client::{
    Client as ClientSend, ClientInvocationError, ClientProvider, SendError, SendRequest,
    SendResponse,
};
use lenso_kernel::{InvocationContext, NativeRequestFuture, RuntimeFailure};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::HttpEgressConfig;
use crate::policy::{PreparedRequest, prepare_request, response_headers};

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
        prepare_request(&self.config, &self.allowed_origins, request)
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
        let body = read_bounded_body(&mut response, self.config.max_response_body_bytes()).await?;
        Ok(SendResponse {
            body: body.into(),
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
    ) -> NativeRequestFuture<ClientSend> {
        let provider = self.clone();
        Box::pin(async move {
            let result = async move {
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
            }
            .await;
            match result {
                Ok(response) => Ok(Ok(response)),
                Err(ClientInvocationError::Domain(error)) => Ok(Err(error)),
                Err(ClientInvocationError::Runtime(error)) => Err(error),
            }
        })
    }
}

async fn read_bounded_body(
    response: &mut reqwest::Response,
    limit: usize,
) -> Result<Bytes, ClientInvocationError> {
    let Some(first) = response
        .chunk()
        .await
        .map_err(|error| classify_transport_error(&error))?
    else {
        return Ok(Bytes::new());
    };
    if first.len() > limit {
        return Err(ClientInvocationError::Domain(SendError::ResponseTooLarge));
    }
    let Some(second) = response
        .chunk()
        .await
        .map_err(|error| classify_transport_error(&error))?
    else {
        return Ok(first);
    };
    let total = first.len().saturating_add(second.len());
    if total > limit {
        return Err(ClientInvocationError::Domain(SendError::ResponseTooLarge));
    }
    let mut body = BytesMut::with_capacity(total);
    body.extend_from_slice(&first);
    body.extend_from_slice(&second);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| classify_transport_error(&error))?
    {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(ClientInvocationError::Domain(SendError::ResponseTooLarge));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn classify_transport_error(error: &reqwest::Error) -> ClientInvocationError {
    ClientInvocationError::Domain(if error.is_timeout() {
        SendError::Timeout
    } else {
        SendError::TransportFailure
    })
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
            body: Vec::new().into(),
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
        assert_eq!(first.unwrap().unwrap().status, 200);
        assert!(matches!(
            second,
            Err(RuntimeFailure::ResourceExhausted { capability, operation })
                if capability == lenso_capability_http_client::CAPABILITY_ID
                && operation == lenso_capability_http_client::SEND_OPERATION
        ));
        upstream.abort();
    }
}
