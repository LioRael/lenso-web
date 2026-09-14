//! Event-owned registration for hosts that provide buffered HTTP events.
use crate::{
    PACKAGE_ID, PACKAGE_VERSION, WebIngressConfig, WebIngressDiagnostics, WebIngressMiddleware,
    WebIngressRouteManifest, diagnostics,
    ingress::{
        IngressBody, IngressResponse, RequestIdSequence, acquire_request_permit,
        canonical_request_head_len, dispatch_buffered, mark_sensitive_headers, payload_too_large,
        replace_request_id, unavailable, with_transport_headers,
    },
    middleware, plugin_failure,
    routing::RouteTable,
    session_cookie::SessionCookiePolicy,
};
use bytes::Bytes;
use futures::future::{Either, select};
use http::{Request, Response, StatusCode, header::CONTENT_LENGTH};
use lenso::prelude::ManyPort;
use lenso_app_plan::authoring::PluginDescriptor;
use lenso_capability_http_endpoint::EndpointClient;
use lenso_capability_http_stream_endpoint::StreamEndpointClient;
use lenso_kernel::{
    ActivateContext, CancellationToken, DeactivateContext, PluginFuture, PluginLifecycle,
    ReadinessContext, RuntimeFailure,
};
use lenso_native_adapter::{NativePluginFactory, NativePluginFactoryContext, NativePluginInstance};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use tokio::sync::Semaphore;

#[derive(Debug)]
struct EventState {
    config: WebIngressConfig,
    routes: Rc<RouteTable>,
    readiness: ReadinessContext,
    concurrency: Semaphore,
    middleware: Vec<Rc<dyn WebIngressMiddleware>>,
    next_request_id: RequestIdSequence,
}

/// One event-owned instance of the existing `lenso.web-ingress` Plugin.
///
/// Register a clone in the native registry with the event Driver, then invoke
/// `handle` on this handle after Kernel startup. Create a new factory for each
/// event App; a factory rejects a second instantiation. No socket or Tokio
/// runtime is required with `default-features = false`.
///
/// The host owns bounded body collection and its read deadline, URL/header
/// conversion, and response serialization. Pass an origin-form URI preserving
/// raw path/query and append repeated headers. The shared ingress owns all
/// routing, credentials, middleware and Endpoint response normalization.
#[derive(Clone, Debug)]
pub struct WebIngressEventFactory {
    instantiated: Rc<Cell<bool>>,
    state: Rc<RefCell<Option<Rc<EventState>>>>,
    diagnostics: Rc<dyn WebIngressDiagnostics>,
    middleware: Vec<Rc<dyn WebIngressMiddleware>>,
}

impl Default for WebIngressEventFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl WebIngressEventFactory {
    pub fn new() -> Self {
        Self {
            instantiated: Rc::new(Cell::new(false)),
            state: Rc::default(),
            diagnostics: Rc::new(diagnostics::NoopDiagnostics),
            middleware: Vec::new(),
        }
    }

    pub fn plugin_descriptor() -> PluginDescriptor {
        crate::plugin_descriptor()
    }

    #[must_use]
    pub fn with_middleware(mut self, middleware: impl WebIngressMiddleware + 'static) -> Self {
        self.middleware.push(Rc::new(middleware));
        self
    }

    #[must_use]
    pub fn with_diagnostics(mut self, diagnostics: impl WebIngressDiagnostics + 'static) -> Self {
        self.diagnostics = Rc::new(diagnostics);
        self
    }

    pub fn route_manifest(&self) -> Option<WebIngressRouteManifest> {
        self.state
            .borrow()
            .as_ref()
            .map(|state| state.routes.manifest().clone())
    }

    /// Dispatches through the normal Plan-bound generated Endpoint clients.
    ///
    /// Admission fails before Ready and after drain begins. `cancellation` is
    /// supplied by this event's host I/O scope and must never be shared across
    /// events. The generated invocation receives a deadline from the Kernel's
    /// Driver. Streaming bindings are rejected during activation.
    pub async fn handle(
        &self,
        mut request: Request<Bytes>,
        cancellation: CancellationToken,
    ) -> Result<Response<Bytes>, RuntimeFailure> {
        let state = self
            .state
            .borrow()
            .clone()
            .ok_or_else(|| plugin_failure("event ingress is not active"))?;
        let session_cookie = state.config.session_cookie().map(SessionCookiePolicy::from);
        mark_sensitive_headers(request.headers_mut(), session_cookie.as_ref());
        let head_len = canonical_request_head_len(&request);
        let request_id = replace_request_id(request.headers_mut(), &state.next_request_id);
        let method = request.method().clone();
        let work = async {
            if !state.readiness.is_open() || !state.readiness.is_accepting() {
                return unavailable();
            }
            if head_len > state.config.max_request_head_bytes() {
                return IngressResponse::json(
                    StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                    r#"{"error":"request_header_fields_too_large"}"#,
                );
            }
            let Some(_permit) = acquire_request_permit(&state.concurrency, &cancellation).await
            else {
                return unavailable();
            };
            if !state.readiness.is_accepting() {
                return unavailable();
            }
            if request.body().len() > state.config.max_request_body_bytes()
                || request
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok()?.parse::<usize>().ok())
                    .is_some_and(|length| length > state.config.max_request_body_bytes())
            {
                return payload_too_large();
            }
            let (parts, body) = request.into_parts();
            dispatch_buffered(
                parts,
                body,
                cancellation.clone(),
                session_cookie.as_ref(),
                state.routes.clone(),
                &state.middleware,
            )
            .await
        };
        let app_cancel = state.readiness.cancellation();
        futures::pin_mut!(work);
        let response = match select(work, app_cancel.cancelled()).await {
            Either::Left((response, _)) => response,
            Either::Right(((), work)) => {
                cancellation.cancel();
                work.await
            }
        };
        let response = response.normalize_body(&method);
        let IngressBody::Buffered(body) = response.body else {
            return Err(plugin_failure(
                "event ingress cannot serialize a streaming response",
            ));
        };
        let mut result = Response::new(body);
        *result.status_mut() = response.status;
        *result.headers_mut() = response.headers;
        Ok(with_transport_headers(result, request_id))
    }
}

impl NativePluginFactory for WebIngressEventFactory {
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
        let config: WebIngressConfig =
            serde_json::from_str(context.configuration()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("Web Ingress configuration is invalid: {error}"),
                }
            })?;
        config
            .validate()
            .map_err(|detail| RuntimeFailure::InvalidResolvedPlan { detail })?;
        middleware::validate(&self.middleware)
            .map_err(|detail| RuntimeFailure::InvalidResolvedPlan { detail })?;
        if self.instantiated.replace(true) {
            return Err(plugin_failure(
                "create a fresh WebIngressEventFactory for each event App",
            ));
        }
        Ok(NativePluginInstance::with_lifecycle(
            Vec::new(),
            EventLifecycle {
                config,
                state: self.state.clone(),
                diagnostics: self.diagnostics.clone(),
                middleware: self.middleware.clone(),
            },
        ))
    }
}

#[derive(Debug)]
struct EventLifecycle {
    middleware: Vec<Rc<dyn WebIngressMiddleware>>,
    config: WebIngressConfig,
    state: Rc<RefCell<Option<Rc<EventState>>>>,
    diagnostics: Rc<dyn WebIngressDiagnostics>,
}

impl PluginLifecycle for EventLifecycle {
    fn activate(&self, context: ActivateContext) -> PluginFuture {
        let state = self.state.clone();
        let config = self.config.clone();
        let diagnostics = self.diagnostics.clone();
        let middleware = self.middleware.clone();
        Box::pin(async move {
            let endpoints = ManyPort::<EndpointClient>::default();
            let streams = ManyPort::<StreamEndpointClient>::default();
            endpoints.connect(context.dependencies())?;
            streams.connect(context.dependencies())?;
            if streams.iter().next().is_some() {
                return Err(plugin_failure(
                    "buffered event ingress does not support Stream Endpoint bindings",
                ));
            }
            let routes = RouteTable::resolve(
                endpoints,
                streams,
                context.dependencies(),
                config.request_timeout(),
                diagnostics,
            )
            .await?;
            *state.borrow_mut() = Some(Rc::new(EventState {
                concurrency: Semaphore::new(config.max_concurrent_requests()),
                middleware,
                config,
                routes,
                readiness: context.readiness(),
                next_request_id: RequestIdSequence::Local(Rc::new(Cell::new(0))),
            }));
            Ok(())
        })
    }
    fn deactivate(&self, _context: DeactivateContext) -> PluginFuture {
        self.state.borrow_mut().take();
        Box::pin(async { Ok(()) })
    }
}
