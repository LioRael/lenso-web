//! Explicit event-owned HTTP transport injection behind the existing Client contract.
use crate::{
    CONFIGURATION_SCHEMA_JSON, HttpEgressConfig, HttpVersionPolicy, PACKAGE_ID, PACKAGE_VERSION,
    policy::{prepare_request, response_headers},
};
use bytes::Bytes;
use futures::future::{Either, LocalBoxFuture, select};
use http::Response;
use lenso_app_plan::{CapabilityEndpointPlan, authoring::PluginDescriptor};
use lenso_capability_http_client::{
    self as client, ClientEndpoint, ClientInvocationError, ClientProvider, SendError, SendRequest,
    SendResponse,
};
use lenso_kernel::{InvocationContext, NativeRequestFuture, RuntimeFailure};
use lenso_native_adapter::{NativePluginFactory, NativePluginFactoryContext, NativePluginInstance};
use std::{cell::Cell, fmt, rc::Rc, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

/// Limits a transport must enforce while receiving bytes, before buffering them.
#[derive(Clone, Copy, Debug)]
pub struct HttpEventLimits {
    pub max_response_body_bytes: usize,
    pub max_response_head_bytes: usize,
    pub request_timeout: Duration,
}

/// An outbound request validated against the immutable exact-origin policy.
#[derive(Debug)]
pub struct HttpEventRequest {
    pub request: http::Request<Bytes>,
    pub limits: HttpEventLimits,
}

/// Host transport failures mapped to the existing generated domain errors.
#[derive(Clone, Copy, Debug)]
pub enum HttpEventError {
    Timeout,
    ResponseTooLarge,
    TransportFailure,
}
impl From<HttpEventError> for SendError {
    fn from(value: HttpEventError) -> Self {
        match value {
            HttpEventError::Timeout => Self::Timeout,
            HttpEventError::ResponseTooLarge => Self::ResponseTooLarge,
            HttpEventError::TransportFailure => Self::TransportFailure,
        }
    }
}

/// A trusted, event-owned transport. It must use manual redirects, no implicit
/// retries, proxies or cookie storage, and abort I/O when its future is dropped.
/// Limits and the total deadline include streamed response collection.
/// The Web-owned `event-fetch.mjs` bridge implements this contract for Workers.
pub trait HttpEventTransport: fmt::Debug {
    fn send(
        &self,
        request: HttpEventRequest,
    ) -> LocalBoxFuture<'static, Result<Response<Bytes>, HttpEventError>>;
}

/// Event implementation of `lenso.http-egress`, registered explicitly per App.
#[derive(Clone, Debug)]
pub struct HttpEgressEventFactory {
    transport: Rc<dyn HttpEventTransport>,
    instantiated: Rc<Cell<bool>>,
}
impl HttpEgressEventFactory {
    pub fn new(transport: impl HttpEventTransport + 'static) -> Self {
        Self {
            transport: Rc::new(transport),
            instantiated: Rc::new(Cell::new(false)),
        }
    }
    pub fn plugin_descriptor() -> PluginDescriptor {
        PluginDescriptor::new(PACKAGE_ID, PACKAGE_VERSION, "http-clients")
            .with_capability(CapabilityEndpointPlan::new(
                client::CAPABILITY_ID,
                client::DESCRIPTOR_VERSION,
                [client::SEND_OPERATION],
            ))
            .with_configuration_schema(
                serde_json::from_str(CONFIGURATION_SCHEMA_JSON)
                    .expect("embedded Egress schema is valid"),
            )
    }
}
impl NativePluginFactory for HttpEgressEventFactory {
    fn package_id(&self) -> &'static str {
        PACKAGE_ID
    }
    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }
    fn instantiate(
        &self,
        context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        let invalid = |detail: String| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("HTTP Egress configuration is invalid: {detail}"),
        };
        let config: HttpEgressConfig = serde_json::from_str(context.configuration())
            .map_err(|error| invalid(error.to_string()))?;
        let allowed_origins = config.validate().map_err(invalid)?;
        if config.http_version() != HttpVersionPolicy::Auto {
            return Err(invalid(
                "event Fetch cannot force an HTTP protocol version".into(),
            ));
        }
        // Fetch exposes no distinct socket-connect phase. Equal deadlines retain
        // both upper bounds using the same total timer; other policies fail closed.
        if config.connect_timeout() != config.request_timeout() {
            return Err(invalid(
                "event Fetch requires equal connect and total request timeouts".into(),
            ));
        }
        if self.instantiated.replace(true) {
            return Err(invalid(
                "create a fresh HTTP Egress event factory for each App".into(),
            ));
        }
        let provider = EventProvider {
            permits: Arc::new(Semaphore::new(config.max_concurrent_requests())),
            config,
            allowed_origins,
            transport: self.transport.clone(),
        };
        Ok(NativePluginInstance::new(vec![Rc::new(
            ClientEndpoint::new(provider),
        )]))
    }
}

#[derive(Clone, Debug)]
struct EventProvider {
    config: HttpEgressConfig,
    allowed_origins: std::collections::BTreeSet<String>,
    permits: Arc<Semaphore>,
    transport: Rc<dyn HttpEventTransport>,
}
impl EventProvider {
    async fn execute(&self, request: SendRequest) -> Result<SendResponse, ClientInvocationError> {
        let prepared = prepare_request(&self.config, &self.allowed_origins, request)?;
        let is_head = prepared.method == http::Method::HEAD;
        let mut request = http::Request::builder()
            .method(prepared.method)
            .uri(prepared.url.as_str())
            .body(prepared.body)
            .map_err(|_| ClientInvocationError::Domain(SendError::InvalidRequest))?;
        *request.headers_mut() = prepared.headers;
        request
            .headers_mut()
            .entry(http::header::USER_AGENT)
            .or_insert_with(|| {
                http::HeaderValue::from_str(&format!("lenso-http-egress-plugin/{PACKAGE_VERSION}"))
                    .expect("package version is valid header text")
            });
        let response = self
            .transport
            .send(HttpEventRequest {
                request,
                limits: HttpEventLimits {
                    max_response_body_bytes: self.config.max_response_body_bytes(),
                    max_response_head_bytes: self.config.max_response_head_bytes(),
                    request_timeout: self.config.request_timeout(),
                },
            })
            .await
            .map_err(|error| ClientInvocationError::Domain(error.into()))?;
        if response.body().len() > self.config.max_response_body_bytes()
            || (!is_head
                && !matches!(response.status().as_u16(), 204 | 304)
                && response
                    .headers()
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok()?.parse::<usize>().ok())
                    .is_some_and(|length| length > self.config.max_response_body_bytes()))
        {
            return Err(ClientInvocationError::Domain(SendError::ResponseTooLarge));
        }
        let headers = response_headers(response.headers(), self.config.max_response_head_bytes())?;
        let (parts, body) = response.into_parts();
        Ok(SendResponse {
            status: i64::from(parts.status.as_u16()),
            headers,
            body: body.into(),
        })
    }
}
impl ClientProvider for EventProvider {
    fn send(
        &self,
        context: InvocationContext,
        request: SendRequest,
    ) -> NativeRequestFuture<client::Client> {
        let provider = self.clone();
        Box::pin(async move {
            let request_id = context.request_id();
            let cancellation = context.cancellation();
            if cancellation.is_cancelled() {
                return Err(RuntimeFailure::Cancelled { request_id });
            }
            let _permit = provider.permits.clone().try_acquire_owned().map_err(|_| {
                RuntimeFailure::ResourceExhausted {
                    capability: client::CAPABILITY_ID,
                    operation: client::SEND_OPERATION.to_owned(),
                }
            })?;
            let operation = provider.execute(request);
            futures::pin_mut!(operation);
            let result = match select(cancellation.cancelled(), operation).await {
                Either::Left(((), _)) => return Err(RuntimeFailure::Cancelled { request_id }),
                Either::Right((result, _)) => result,
            };
            match result {
                Ok(response) => Ok(Ok(response)),
                Err(ClientInvocationError::Domain(error)) => Ok(Err(error)),
                Err(ClientInvocationError::Runtime(error)) => Err(error),
            }
        })
    }
}
