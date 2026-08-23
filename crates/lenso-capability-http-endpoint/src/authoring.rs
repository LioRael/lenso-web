use lenso_kernel::{InvocationContext, NativeRequestFuture};

use crate::{
    DescribeRequest, DescribeResponse, DescribeResponseRoutesItem, EndpointDescribe,
    EndpointHandle, EndpointProvider, HandleRequest,
};

/// One immutable HTTP route owned by an Endpoint provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointRoute {
    route_id: &'static str,
    method: &'static str,
    path: &'static str,
}

impl EndpointRoute {
    /// Declares one stable route identifier, canonical HTTP method, and path template.
    #[must_use]
    pub const fn new(route_id: &'static str, method: &'static str, path: &'static str) -> Self {
        Self {
            route_id,
            method,
            path,
        }
    }

    /// Returns the stable identifier dispatched to the owning handler.
    #[must_use]
    pub const fn route_id(self) -> &'static str {
        self.route_id
    }

    /// Returns the canonical uppercase HTTP method.
    #[must_use]
    pub const fn method(self) -> &'static str {
        self.method
    }

    /// Returns the absolute path template understood by Web Ingress.
    #[must_use]
    pub const fn path(self) -> &'static str {
        self.path
    }

    fn into_description(self) -> DescribeResponseRoutesItem {
        DescribeResponseRoutesItem {
            method: self.method.to_owned(),
            path: self.path.to_owned(),
            route_id: self.route_id.to_owned(),
        }
    }
}

/// Boxed local handler result used by an authored HTTP Endpoint.
pub type EndpointFuture = NativeRequestFuture<EndpointHandle>;

/// A statically routed HTTP Endpoint whose declarations and dispatch share one source.
///
/// Prefer [`crate::http_endpoint!`] so route identifiers cannot drift between
/// `describe` and `handle`. Implement this trait directly only when a provider needs
/// custom dispatch while retaining the generated `EndpointProvider` behavior.
pub trait HttpEndpoint: Clone + std::fmt::Debug + 'static {
    /// The complete immutable route table for this provider.
    const ROUTES: &'static [EndpointRoute];

    /// Dispatches one request already matched to a route in [`Self::ROUTES`].
    fn dispatch(&self, context: InvocationContext, request: HandleRequest) -> EndpointFuture;
}

impl<T> EndpointProvider for T
where
    T: HttpEndpoint,
{
    fn describe(
        &self,
        _context: InvocationContext,
        _request: DescribeRequest,
    ) -> NativeRequestFuture<EndpointDescribe> {
        let routes = T::ROUTES
            .iter()
            .copied()
            .map(EndpointRoute::into_description)
            .collect();
        Box::pin(async move { Ok(Ok(DescribeResponse { routes })) })
    }

    fn handle(
        &self,
        context: InvocationContext,
        request: HandleRequest,
    ) -> NativeRequestFuture<EndpointHandle> {
        self.dispatch(context, request)
    }
}

/// Validates a route table during const evaluation in [`crate::http_endpoint!`].
#[doc(hidden)]
pub const fn validate_endpoint_routes(routes: &[EndpointRoute]) {
    assert!(
        !routes.is_empty(),
        "an HTTP Endpoint needs at least one route"
    );
    let mut index = 0;
    while index < routes.len() {
        let route = routes[index];
        assert!(valid_route_id(route.route_id), "HTTP route id is invalid");
        assert!(valid_method(route.method), "HTTP route method is invalid");
        assert!(valid_path(route.path), "HTTP route path is invalid");

        let mut previous = 0;
        while previous < index {
            let candidate = routes[previous];
            assert!(
                !string_eq(candidate.route_id, route.route_id),
                "HTTP route ids must be unique"
            );
            assert!(
                !(string_eq(candidate.method, route.method)
                    && string_eq(candidate.path, route.path)),
                "HTTP method and path pairs must be unique"
            );
            previous += 1;
        }
        index += 1;
    }
}

const fn valid_route_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            return false;
        }
        index += 1;
    }
    true
}

const fn valid_method(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let valid = byte.is_ascii_uppercase()
            || byte.is_ascii_digit()
            || matches!(
                byte,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            );
        if !valid {
            return false;
        }
        index += 1;
    }
    true
}

const fn valid_path(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes[0] != b'/' {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        if matches!(bytes[index], b'?' | b'#') {
            return false;
        }
        index += 1;
    }
    true
}

const fn string_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// Implements a statically routed [`HttpEndpoint`] from one route table.
///
/// Each handler is an async method with the following shape:
///
/// ```ignore
/// async fn handler(
///     &self,
///     context: InvocationContext,
///     request: HandleRequest,
/// ) -> Result<HandleResponse, EndpointHandleInvocationError>
/// ```
///
/// Route identifiers, methods, and paths appear only in this invocation. The
/// generated implementation publishes them through `describe` and dispatches
/// `handle` to the selected method without an application-owned string match.
/// Duplicate declarations fail during const evaluation:
///
/// ```compile_fail
/// use lenso_capability_http_endpoint::{
///     EndpointHandleInvocationError, HandleRequest, HandleResponse, http_endpoint,
/// };
/// use lenso_kernel::InvocationContext;
///
/// #[derive(Clone, Debug)]
/// struct DuplicateRoutes;
///
/// impl DuplicateRoutes {
///     async fn handle(
///         &self,
///         _context: InvocationContext,
///         _request: HandleRequest,
///     ) -> Result<HandleResponse, EndpointHandleInvocationError> {
///         unimplemented!()
///     }
/// }
///
/// http_endpoint! {
///     impl DuplicateRoutes {
///         "orders.read" => ("GET", "/orders/{order_id}") => handle,
///         "orders.read" => ("GET", "/orders/{another_id}") => handle,
///     }
/// }
/// ```
#[macro_export]
macro_rules! http_endpoint {
    (
        impl $provider:ty {
            $(
                $route_id:literal => ($method:literal, $path:literal) => $handler:ident
            ),+ $(,)?
        }
    ) => {
        const _: () = {
            const ROUTES: &[$crate::EndpointRoute] = &[
                $(
                    $crate::EndpointRoute::new($route_id, $method, $path),
                )+
            ];
            $crate::validate_endpoint_routes(ROUTES);
        };

        impl $crate::HttpEndpoint for $provider {
            const ROUTES: &'static [$crate::EndpointRoute] = &[
                $(
                    $crate::EndpointRoute::new($route_id, $method, $path),
                )+
            ];

            fn dispatch(
                &self,
                context: $crate::__private::InvocationContext,
                request: $crate::HandleRequest,
            ) -> $crate::EndpointFuture {
                let provider = self.clone();
                Box::pin(async move {
                    let route_id = request.route_id.clone();
                    match route_id.as_str() {
                        $(
                            $route_id => match provider.$handler(context, request).await {
                                Ok(response) => Ok(Ok(response)),
                                Err($crate::EndpointHandleInvocationError::Domain(error)) => {
                                    Ok(Err(error))
                                }
                                Err($crate::EndpointHandleInvocationError::Runtime(error)) => {
                                    Err(error)
                                }
                            },
                        )+
                        _ => Ok(Err($crate::HandleError::Rejected)),
                    }
                })
            }
        }
    };
}

#[doc(hidden)]
pub mod __private {
    pub use lenso_kernel::InvocationContext;
}

#[cfg(test)]
mod tests {
    use super::{EndpointRoute, validate_endpoint_routes};

    #[test]
    fn route_validation_accepts_canonical_static_routes() {
        validate_endpoint_routes(&[
            EndpointRoute::new("orders.create", "POST", "/orders"),
            EndpointRoute::new("orders.read", "GET", "/orders/{order_id}"),
        ]);
    }

    #[test]
    #[should_panic(expected = "HTTP route ids must be unique")]
    fn route_validation_rejects_duplicate_ids() {
        validate_endpoint_routes(&[
            EndpointRoute::new("orders.read", "GET", "/orders/{order_id}"),
            EndpointRoute::new("orders.read", "GET", "/orders/{another_id}"),
        ]);
    }

    #[test]
    #[should_panic(expected = "HTTP method and path pairs must be unique")]
    fn route_validation_rejects_duplicate_method_and_path_pairs() {
        validate_endpoint_routes(&[
            EndpointRoute::new("orders.read", "GET", "/orders/{order_id}"),
            EndpointRoute::new("orders.copy", "GET", "/orders/{order_id}"),
        ]);
    }

    #[test]
    #[should_panic(expected = "HTTP route method is invalid")]
    fn route_validation_rejects_noncanonical_methods() {
        validate_endpoint_routes(&[EndpointRoute::new(
            "orders.read",
            "get",
            "/orders/{order_id}",
        )]);
    }
}
