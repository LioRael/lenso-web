use std::{collections::HashMap, rc::Rc, time::Duration};

use crate::{WebSocketConfig, websocket::WebSocketUpgrade, websocket_handshake::Handshake};
use futures::future::{Either, select};
use http::Method;
use lenso::prelude::ManyPort;
use lenso_capability_http_endpoint::{
    DescribeRequest, EndpointClient, EndpointDescribeInvocationError,
    EndpointHandleInvocationError, HandleRequest, HandleRequestCredential,
    HandleRequestHeadersItem, HandleRequestPathParametersItem,
};
use lenso_capability_http_stream_endpoint as stream_endpoint;
use lenso_capability_websocket_endpoint as websocket_endpoint;
use lenso_kernel::{
    CancellationToken, NativeStream, PluginDependencies, RuntimeFailure, StreamEvent,
};
use matchit::Router;

use crate::{
    WebIngressDiagnostics, WebIngressEndpointFailure, WebIngressRoute, WebIngressRouteManifest,
    ingress::InboundRequest, plugin_failure,
};

#[derive(Debug)]
enum RouteProvider {
    Buffered(usize),
    Streaming(usize),
    WebSocket(usize),
}

#[derive(Debug)]
struct RouteTarget {
    route_id: String,
    provider: RouteProvider,
}

#[derive(Debug)]
pub(super) struct RouteTable {
    dependencies: PluginDependencies,
    diagnostics: Rc<dyn WebIngressDiagnostics>,
    providers: ManyPort<EndpointClient>,
    stream_providers: ManyPort<stream_endpoint::StreamEndpointClient>,
    websocket_providers: ManyPort<websocket_endpoint::EndpointClient>,
    websocket_policy: Option<WebSocketConfig>,
    methods: HashMap<Method, Router<RouteTarget>>,
    manifest: WebIngressRouteManifest,
    request_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DispatchError {
    NotFound,
    UpgradeRequired,
    BadHandshake,
    Unauthorized,
    Forbidden,
    MethodNotAllowed(Vec<Method>),
    Rejected,
    TimedOut,
    Unavailable,
}

#[derive(Debug)]
pub(super) struct StreamingResponse {
    pub(super) headers: Vec<stream_endpoint::HandleResponseHeadersItem>,
    pub(super) status: i64,
    pub(super) stream: NativeStream<stream_endpoint::StreamEndpointHandle>,
}

#[derive(Debug)]
pub(super) enum DispatchResponse {
    Buffered(lenso_capability_http_endpoint::HandleResponse),
    Streaming(StreamingResponse),
    WebSocket(WebSocketUpgrade),
}

impl RouteTable {
    #[allow(clippy::too_many_lines)] // Resolves both capability manifests as one collision domain.
    pub(super) async fn resolve(
        providers: ManyPort<EndpointClient>,
        stream_providers: ManyPort<stream_endpoint::StreamEndpointClient>,
        websocket_providers: ManyPort<websocket_endpoint::EndpointClient>,
        websocket_policy: Option<WebSocketConfig>,
        dependencies: &PluginDependencies,
        request_timeout: Duration,
        diagnostics: Rc<dyn WebIngressDiagnostics>,
    ) -> Result<Rc<Self>, RuntimeFailure> {
        let mut methods = HashMap::<Method, Router<RouteTarget>>::new();
        let mut manifest = Vec::new();
        for (provider_index, provider) in providers.iter().enumerate() {
            let description = provider
                .describe(DescribeRequest {})
                .await
                .map_err(|error| match error {
                    EndpointDescribeInvocationError::Domain(error) => plugin_failure(format!(
                        "HTTP Endpoint provider {provider_index} rejected its description: {error:?}"
                    )),
                    EndpointDescribeInvocationError::Runtime(error) => error,
                })?;
            for route in description.routes {
                let method = route.method.trim().to_ascii_uppercase();
                let Ok(method) = Method::from_bytes(method.as_bytes()) else {
                    return Err(plugin_failure(format!(
                        "HTTP Endpoint provider {provider_index} declared an invalid route"
                    )));
                };
                if route.route_id.trim().is_empty()
                    || !route.path.starts_with('/')
                    || route.path.contains(['?', '#'])
                {
                    return Err(plugin_failure(format!(
                        "HTTP Endpoint provider {provider_index} declared an invalid route"
                    )));
                }
                manifest.push(WebIngressRoute::new(
                    method.as_str(),
                    &route.path,
                    &route.route_id,
                ));
                methods
                    .entry(method.clone())
                    .or_default()
                    .insert(
                        route.path.clone(),
                        RouteTarget {
                            route_id: route.route_id,
                            provider: RouteProvider::Buffered(provider_index),
                        },
                    )
                    .map_err(|error| {
                        plugin_failure(format!(
                            "HTTP route collision for {method} {}: {error}",
                            route.path
                        ))
                    })?;
            }
        }
        for (provider_index, provider) in stream_providers.iter().enumerate() {
            let description = provider
                .describe_stream(stream_endpoint::DescribeRequest {})
                .await
                .map_err(|error| match error {
                    stream_endpoint::StreamEndpointDescribeInvocationError::Domain(error) => {
                        plugin_failure(format!(
                            "streaming HTTP Endpoint provider {provider_index} rejected its description: {error:?}"
                        ))
                    }
                    stream_endpoint::StreamEndpointDescribeInvocationError::Runtime(error) => error,
                })?;
            for route in description.routes {
                let method = route.method.trim().to_ascii_uppercase();
                let Ok(method) = Method::from_bytes(method.as_bytes()) else {
                    return Err(plugin_failure(format!(
                        "streaming HTTP Endpoint provider {provider_index} declared an invalid route"
                    )));
                };
                if route.route_id.trim().is_empty()
                    || !route.path.starts_with('/')
                    || route.path.contains(['?', '#'])
                {
                    return Err(plugin_failure(format!(
                        "streaming HTTP Endpoint provider {provider_index} declared an invalid route"
                    )));
                }
                manifest.push(WebIngressRoute::new(
                    method.as_str(),
                    &route.path,
                    &route.route_id,
                ));
                methods
                    .entry(method.clone())
                    .or_default()
                    .insert(
                        route.path.clone(),
                        RouteTarget {
                            route_id: route.route_id,
                            provider: RouteProvider::Streaming(provider_index),
                        },
                    )
                    .map_err(|error| {
                        plugin_failure(format!(
                            "HTTP route collision for {method} {}: {error}",
                            route.path
                        ))
                    })?;
            }
        }
        if methods.is_empty() {
            return Err(plugin_failure(
                "Web Ingress requires at least one bound HTTP Endpoint route",
            ));
        }
        for (provider_index, provider) in websocket_providers.iter().enumerate() {
            if websocket_policy.is_none() {
                return Err(plugin_failure(
                    "WebSocket routes require explicit transport policy",
                ));
            }
            let description = provider
                .describe_websocket(websocket_endpoint::DescribeWebsocketRequest {})
                .await
                .map_err(|_| plugin_failure("WebSocket route description failed"))?;
            for route in description.routes {
                if route.route_id.trim().is_empty()
                    || !route.path.starts_with('/')
                    || route.path.contains(['?', '#'])
                {
                    return Err(plugin_failure("invalid WebSocket route"));
                }
                manifest.push(WebIngressRoute::new("GET", &route.path, &route.route_id));
                methods
                    .entry(Method::GET)
                    .or_default()
                    .insert(
                        &route.path,
                        RouteTarget {
                            route_id: route.route_id,
                            provider: RouteProvider::WebSocket(provider_index),
                        },
                    )
                    .map_err(|_| plugin_failure("conflicting WebSocket route"))?;
            }
        }
        Ok(Rc::new(Self {
            dependencies: dependencies.clone(),
            diagnostics,
            providers,
            stream_providers,
            websocket_providers,
            websocket_policy,
            manifest: WebIngressRouteManifest::new(manifest),
            methods,
            request_timeout,
        }))
    }

    pub(super) fn websocket_policy(&self) -> Option<&WebSocketConfig> {
        self.websocket_policy.as_ref()
    }

    pub(super) const fn manifest(&self) -> &WebIngressRouteManifest {
        &self.manifest
    }

    #[allow(clippy::too_many_lines)] // Keeps shared cancellation around both endpoint interactions.
    pub(super) async fn dispatch(
        &self,
        request: InboundRequest,
        handshake: Option<Handshake>,
    ) -> Result<DispatchResponse, DispatchError> {
        let Some(router) = self.methods.get(&request.method) else {
            let allowed = self.allowed_methods(&request.path);
            return if allowed.is_empty() {
                Err(DispatchError::NotFound)
            } else {
                Err(DispatchError::MethodNotAllowed(allowed))
            };
        };
        let Ok(matched) = router.at(&request.path) else {
            let allowed = self.allowed_methods(&request.path);
            if allowed.is_empty() {
                return Err(DispatchError::NotFound);
            }
            return Err(DispatchError::MethodNotAllowed(allowed));
        };
        let path_parameters: Vec<HandleRequestPathParametersItem> = matched
            .params
            .iter()
            .map(|(name, value)| HandleRequestPathParametersItem {
                name: name.to_owned(),
                value: value.to_owned(),
            })
            .collect();
        let route_id = matched.value.route_id.clone();
        let provider = &matched.value.provider;
        let provider_index = match provider {
            RouteProvider::Buffered(index)
            | RouteProvider::Streaming(index)
            | RouteProvider::WebSocket(index) => *index,
        };
        let request_id = request.request_id.clone();
        let cancellation = CancellationToken::new();
        let context = self
            .dependencies
            .invocation_context_after(self.request_timeout, cancellation.clone())
            .map_err(|failure| {
                self.observe_failure(&request_id, &route_id, provider_index, &failure);
                DispatchError::Unavailable
            })?;
        let app_cancellation = request.cancellation;
        let disconnected = request.disconnected;
        let cancelled = async move {
            tokio::select! {
                _ = disconnected => {}
                () = app_cancellation.cancelled() => {}
            }
        };
        let credential = request
            .credential
            .map(|credential| (credential.scheme, credential.value));
        let headers = request
            .headers
            .into_iter()
            .map(|header| (header.name, header.value))
            .collect::<Vec<_>>();
        let method = request.method.as_str().to_owned();
        let path = request.path;
        let query = request.query;
        let body = request.body;
        let invocation = async {
            match provider {
                RouteProvider::WebSocket(provider_index) => {
                    let handshake = handshake.ok_or(DispatchError::UpgradeRequired)?;
                    if !body.is_empty() {
                        return Err(DispatchError::BadHandshake);
                    }
                    let stream = self.websocket_providers[*provider_index].connect_websocket_with_context(context,
                        websocket_endpoint::ConnectWebsocketRequest {
                            credential: credential.map(|(scheme,value)|websocket_endpoint::ConnectWebsocketRequestCredential {scheme,value}),
                            headers: headers.into_iter().map(|(name,value)|websocket_endpoint::ConnectWebsocketRequestHeadersItem {name,value}).collect(),
                            path, query, request_id:request_id.clone(), route_id:route_id.clone(),
                            path_parameters:path_parameters.into_iter().map(|parameter|websocket_endpoint::ConnectWebsocketRequestPathParametersItem {name:parameter.name,value:parameter.value}).collect(),
                            protocols:handshake.protocols.clone(),
                        }).await.map_err(|error|match error {
                            websocket_endpoint::EndpointConnectWebsocketInvocationError::Domain(error) => match error {
                                websocket_endpoint::ConnectWebsocketError::Unauthorized => DispatchError::Unauthorized,
                                websocket_endpoint::ConnectWebsocketError::Rejected => DispatchError::Forbidden,
                                websocket_endpoint::ConnectWebsocketError::UnsupportedProtocol => DispatchError::BadHandshake,
                                websocket_endpoint::ConnectWebsocketError::Unknown(_) => DispatchError::Rejected,
                            },
                            websocket_endpoint::EndpointConnectWebsocketInvocationError::Runtime(error) => self.runtime_dispatch_error(&request_id,&route_id,*provider_index,&error),
                        })?;
                    let policy = self
                        .websocket_policy
                        .as_ref()
                        .ok_or(DispatchError::Unavailable)?;
                    let session = crate::WebSocketSession::accept(
                        stream,
                        &handshake.protocols,
                        policy.max_message_bytes(),
                        policy.max_session_bytes(),
                    )
                    .await
                    .map_err(|_| DispatchError::Rejected)?;
                    Ok(DispatchResponse::WebSocket(WebSocketUpgrade {
                        session,
                        accept_key: handshake.accept,
                    }))
                }
                RouteProvider::Buffered(provider_index) => {
                    let response = self.providers[*provider_index]
                        .handle_with_context(
                            context,
                            HandleRequest {
                                body: body.into(),
                                credential: credential.map(|(scheme, value)| {
                                    HandleRequestCredential { scheme, value }
                                }),
                                headers: headers
                                    .into_iter()
                                    .map(|(name, value)| HandleRequestHeadersItem { name, value })
                                    .collect(),
                                method,
                                path,
                                path_parameters,
                                query,
                                request_id: request_id.clone(),
                                route_id: route_id.clone(),
                            },
                        )
                        .await
                        .map(DispatchResponse::Buffered)
                        .map_err(|error| match error {
                            EndpointHandleInvocationError::Domain(_) => DispatchError::Rejected,
                            EndpointHandleInvocationError::Runtime(failure) => self
                                .runtime_dispatch_error(
                                    &request_id,
                                    &route_id,
                                    *provider_index,
                                    &failure,
                                ),
                        })?;
                    Ok(response)
                }
                RouteProvider::Streaming(provider_index) => {
                    let stream = self.stream_providers[*provider_index]
                        .handle_stream_with_context(
                            context,
                            stream_endpoint::HandleRequest {
                                body: body.into(),
                                credential: credential.map(|(scheme, value)| {
                                    stream_endpoint::HandleRequestCredential { scheme, value }
                                }),
                                headers: headers
                                    .into_iter()
                                    .map(|(name, value)| {
                                        stream_endpoint::HandleRequestHeadersItem { name, value }
                                    })
                                    .collect(),
                                method,
                                path,
                                path_parameters: path_parameters
                                    .into_iter()
                                    .map(|parameter| {
                                        stream_endpoint::HandleRequestPathParametersItem {
                                            name: parameter.name,
                                            value: parameter.value,
                                        }
                                    })
                                    .collect(),
                                query,
                                request_id: request_id.clone(),
                                route_id: route_id.clone(),
                            },
                        )
                        .await
                        .map_err(|error| match error {
                            stream_endpoint::StreamEndpointHandleInvocationError::Domain(_) => {
                                DispatchError::Rejected
                            }
                            stream_endpoint::StreamEndpointHandleInvocationError::Runtime(
                                failure,
                            ) => self.runtime_dispatch_error(
                                &request_id,
                                &route_id,
                                *provider_index,
                                &failure,
                            ),
                        })?;
                    let head = loop {
                        match stream.receive().await.map_err(|failure| {
                            self.runtime_dispatch_error(
                                &request_id,
                                &route_id,
                                *provider_index,
                                &failure,
                            )
                        })? {
                            StreamEvent::Message(frame)
                                if frame.kind == stream_endpoint::HandleResponseKind::Head =>
                            {
                                break frame;
                            }
                            StreamEvent::PeerHalfClosed => {}
                            StreamEvent::Message(_) | StreamEvent::Terminal(_) => {
                                stream.cancel();
                                return Err(DispatchError::Rejected);
                            }
                        }
                    };
                    let (Some(status), Some(headers), None) =
                        (head.status, head.headers, head.body)
                    else {
                        stream.cancel();
                        return Err(DispatchError::Rejected);
                    };
                    Ok(DispatchResponse::Streaming(StreamingResponse {
                        headers,
                        status,
                        stream,
                    }))
                }
            }
        };
        futures::pin_mut!(invocation, cancelled);
        match select(invocation, cancelled).await {
            Either::Left((outcome, _)) => outcome,
            Either::Right(((), invocation)) => {
                cancellation.cancel();
                invocation.await
            }
        }
    }

    fn runtime_dispatch_error(
        &self,
        request_id: &str,
        route_id: &str,
        provider_index: usize,
        failure: &RuntimeFailure,
    ) -> DispatchError {
        self.observe_failure(request_id, route_id, provider_index, failure);
        if matches!(failure, RuntimeFailure::DeadlineExceeded { .. }) {
            DispatchError::TimedOut
        } else {
            DispatchError::Unavailable
        }
    }

    fn observe_failure(
        &self,
        request_id: &str,
        route_id: &str,
        provider_index: usize,
        failure: &RuntimeFailure,
    ) {
        self.diagnostics
            .endpoint_runtime_failure(WebIngressEndpointFailure::new(
                request_id,
                route_id,
                provider_index,
                failure,
            ));
    }

    fn allowed_methods(&self, path: &str) -> Vec<Method> {
        let mut allowed = self
            .methods
            .iter()
            .filter(|(_, router)| router.at(path).is_ok())
            .map(|(method, _)| method.clone())
            .collect::<Vec<_>>();
        allowed.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        allowed
    }
}
