use std::{collections::HashMap, rc::Rc, time::Duration};

use futures::future::{Either, select};
use http::Method;
use lenso::prelude::ManyPort;
use lenso_capability_http_endpoint::{
    DescribeRequest, EndpointClient, EndpointDescribeInvocationError,
    EndpointHandleInvocationError, HandleRequest, HandleRequestCredential,
    HandleRequestHeadersItem, HandleRequestPathParametersItem, HandleResponse,
};
use lenso_kernel::{CancellationToken, PluginDependencies, RuntimeFailure};
use matchit::Router;

use crate::{WebIngressRoute, WebIngressRouteManifest, plugin_failure, server::InboundRequest};

#[derive(Debug)]
struct RouteTarget {
    route_id: String,
    provider_index: usize,
}

#[derive(Debug)]
pub(super) struct RouteTable {
    dependencies: PluginDependencies,
    providers: ManyPort<EndpointClient>,
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

impl RouteTable {
    pub(super) async fn resolve(
        providers: ManyPort<EndpointClient>,
        dependencies: &PluginDependencies,
        request_timeout: Duration,
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
                            provider_index,
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
            providers,
            manifest: WebIngressRouteManifest::new(manifest),
            methods,
            request_timeout,
        }))
    }

    pub(super) const fn manifest(&self) -> &WebIngressRouteManifest {
        &self.manifest
    }

    pub(super) async fn dispatch(
        &self,
        request: InboundRequest,
    ) -> Result<HandleResponse, DispatchError> {
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
        let provider_index = matched.value.provider_index;
        let cancellation = CancellationToken::new();
        let context = self
            .dependencies
            .invocation_context_after(self.request_timeout, cancellation.clone())
            .map_err(|_| DispatchError::Unavailable)?;
        let app_cancellation = request.cancellation;
        let disconnected = request.disconnected;
        let cancelled = async move {
            tokio::select! {
                _ = disconnected => {}
                () = app_cancellation.cancelled() => {}
            }
        };
        let invocation = self.providers[provider_index].handle_with_context(
            context,
            HandleRequest {
                body: request.body.into(),
                credential: request
                    .credential
                    .map(|credential| HandleRequestCredential {
                        scheme: credential.scheme,
                        value: credential.value,
                    }),
                headers: request
                    .headers
                    .into_iter()
                    .map(|header| HandleRequestHeadersItem {
                        name: header.name,
                        value: header.value,
                    })
                    .collect(),
                method: request.method.as_str().to_owned(),
                path: request.path,
                path_parameters,
                query: request.query,
                request_id: request.request_id,
                route_id,
            },
        );
        futures::pin_mut!(invocation, cancelled);
        let outcome = match select(invocation, cancelled).await {
            Either::Left((outcome, _)) => outcome,
            Either::Right(((), invocation)) => {
                cancellation.cancel();
                invocation.await
            }
        };
        outcome.map_err(|error| match error {
            EndpointHandleInvocationError::Domain(_) => DispatchError::Rejected,
            EndpointHandleInvocationError::Runtime(RuntimeFailure::DeadlineExceeded { .. }) => {
                DispatchError::TimedOut
            }
            EndpointHandleInvocationError::Runtime(_) => DispatchError::Unavailable,
        })
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
