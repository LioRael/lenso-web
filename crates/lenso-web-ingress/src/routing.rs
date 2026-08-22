use std::{collections::BTreeMap, rc::Rc, time::Duration};

use axum::http::Method;
use lenso_capability_http_endpoint::{
    DESCRIBE_OPERATION, DescribeRequest, EndpointDescribe, EndpointHandle, HANDLE_OPERATION,
    HandleRequest, HandleRequestCredential, HandleRequestHeadersItem,
    HandleRequestPathParametersItem, HandleResponse,
};
use lenso_kernel::{CancellationToken, ModuleDependencies, NativeRequestHandle, RuntimeFailure};
use matchit::Router;

use crate::{module_failure, server::InboundRequest};

#[derive(Debug)]
struct RouteTarget {
    route_id: String,
    handler: Rc<NativeRequestHandle<EndpointHandle>>,
}

#[derive(Debug)]
pub(super) struct RouteTable {
    dependencies: ModuleDependencies,
    methods: BTreeMap<String, Router<RouteTarget>>,
    request_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DispatchError {
    NotFound,
    MethodNotAllowed,
    Rejected,
    Unavailable,
}

impl RouteTable {
    pub(super) async fn resolve(
        dependencies: &ModuleDependencies,
        request_timeout: Duration,
    ) -> Result<Rc<Self>, RuntimeFailure> {
        let descriptors = dependencies.many::<EndpointDescribe>()?;
        let handlers = dependencies.many::<EndpointHandle>()?;
        if descriptors.len() != handlers.len() {
            return Err(RuntimeFailure::Internal {
                detail: "HTTP Endpoint describe/handle bindings are inconsistent".to_owned(),
            });
        }
        let mut methods = BTreeMap::<String, Router<RouteTarget>>::new();
        for (provider_index, (descriptor, handler)) in
            descriptors.into_iter().zip(handlers).enumerate()
        {
            let description = descriptor
                .invoke(DESCRIBE_OPERATION, DescribeRequest {})
                .await?
                .map_err(|error| {
                    module_failure(format!(
                        "HTTP Endpoint provider {provider_index} rejected its description: {error:?}"
                    ))
                })?;
            let handler = Rc::new(handler);
            for route in description.routes {
                let method = route.method.trim().to_ascii_uppercase();
                if method.is_empty()
                    || Method::from_bytes(method.as_bytes()).is_err()
                    || route.route_id.trim().is_empty()
                    || !route.path.starts_with('/')
                    || route.path.contains(['?', '#'])
                {
                    return Err(module_failure(format!(
                        "HTTP Endpoint provider {provider_index} declared an invalid route"
                    )));
                }
                methods
                    .entry(method.clone())
                    .or_default()
                    .insert(
                        route.path.clone(),
                        RouteTarget {
                            route_id: route.route_id,
                            handler: handler.clone(),
                        },
                    )
                    .map_err(|error| {
                        module_failure(format!(
                            "HTTP route collision for {method} {}: {error}",
                            route.path
                        ))
                    })?;
            }
        }
        if methods.is_empty() {
            return Err(module_failure(
                "Web Ingress requires at least one bound HTTP Endpoint route",
            ));
        }
        Ok(Rc::new(Self {
            dependencies: dependencies.clone(),
            methods,
            request_timeout,
        }))
    }

    pub(super) async fn dispatch(
        &self,
        request: InboundRequest,
    ) -> Result<HandleResponse, DispatchError> {
        let method = request.method.to_ascii_uppercase();
        let Some(router) = self.methods.get(&method) else {
            return if self.path_exists(&request.path) {
                Err(DispatchError::MethodNotAllowed)
            } else {
                Err(DispatchError::NotFound)
            };
        };
        let matched = match router.at(&request.path) {
            Ok(matched) => matched,
            Err(_) if self.path_exists(&request.path) => {
                return Err(DispatchError::MethodNotAllowed);
            }
            Err(_) => return Err(DispatchError::NotFound),
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
        let handler = matched.value.handler.clone();
        let context = self
            .dependencies
            .invocation_context_after(self.request_timeout, CancellationToken::new())
            .map_err(|_| DispatchError::Unavailable)?;
        handler
            .invoke_with_context(
                HANDLE_OPERATION,
                context,
                HandleRequest {
                    body: request.body,
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
                    method,
                    path: request.path,
                    path_parameters,
                    query: request.query,
                    request_id: request.request_id,
                    route_id,
                },
            )
            .await
            .map_err(|_| DispatchError::Unavailable)?
            .map_err(|_| DispatchError::Rejected)
    }

    fn path_exists(&self, path: &str) -> bool {
        self.methods.values().any(|router| router.at(path).is_ok())
    }
}
