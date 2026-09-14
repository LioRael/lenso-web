use std::{collections::HashMap, rc::Rc, time::Duration};

use futures::future::{Either, select};
use http::Method;
use lenso::prelude::ManyPort;
use lenso_capability_http_endpoint::{
    DescribeRequest, EndpointClient, EndpointDescribeInvocationError,
    EndpointHandleInvocationError, HandleRequest, HandleRequestCredential,
    HandleRequestHeadersItem, HandleRequestPathParametersItem,
};
use lenso_capability_http_stream_endpoint as stream_endpoint;
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
    methods: HashMap<Method, Router<RouteTarget>>,
    manifest: WebIngressRouteManifest,
    request_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum DispatchError {
    NotFound,
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
}

impl RouteTable {
    #[allow(clippy::too_many_lines)] // Resolves both capability manifests as one collision domain.
    pub(super) async fn resolve(
        providers: ManyPort<EndpointClient>,
        stream_providers: ManyPort<stream_endpoint::StreamEndpointClient>,
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
        Ok(Rc::new(Self {
            dependencies: dependencies.clone(),
            diagnostics,
            providers,
            stream_providers,
            manifest: WebIngressRouteManifest::new(manifest),
            methods,
            request_timeout,
        }))
    }

    pub(super) const fn manifest(&self) -> &WebIngressRouteManifest {
        &self.manifest
    }

    #[allow(clippy::too_many_lines)] // Keeps shared cancellation around both endpoint interactions.
    pub(super) async fn dispatch(
        &self,
        request: InboundRequest,
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
        let path_parameters = matched
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
            RouteProvider::Buffered(index) | RouteProvider::Streaming(index) => *index,
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
