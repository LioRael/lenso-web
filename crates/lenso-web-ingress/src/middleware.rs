//! Global middleware for one Web Ingress Module Instance.

use std::{fmt, future::Future, rc::Rc};

use bytes::Bytes;
use futures::future::LocalBoxFuture;
use http::{Request, Response};
use lenso_kernel::RuntimeFailure;

/// A normalized request after Ingress transport limits and credential isolation.
pub type WebIngressRequest = Request<Bytes>;

/// A normalized response before Ingress-owned transport headers are applied.
pub type WebIngressResponse = Response<Bytes>;

/// Result of one global middleware request step.
#[derive(Debug)]
pub enum WebIngressMiddlewareOutcome {
    /// Continue through the remaining middleware and route dispatch.
    Continue,
    /// Short-circuit route dispatch with an intentional response.
    Respond(WebIngressResponse),
}

/// Global network policy owned by one concrete Web Ingress Module factory.
///
/// Middleware runs in declaration order before route dispatch and in reverse
/// order after a response. Its identity must include immutable configuration;
/// same-port replicas reject different identity sequences before readiness.
pub trait WebIngressMiddleware: fmt::Debug {
    /// Stable identity including the middleware's immutable configuration.
    fn identity(&self) -> &str;

    /// Inspects, mutates, or rejects one normalized request.
    fn before_request<'a>(
        &'a self,
        request: &'a mut WebIngressRequest,
    ) -> LocalBoxFuture<'a, Result<WebIngressMiddlewareOutcome, RuntimeFailure>>;

    /// Inspects or mutates the response produced by inner middleware or dispatch.
    fn after_response<'a>(
        &'a self,
        _request: &'a WebIngressRequest,
        _response: &'a mut WebIngressResponse,
    ) -> LocalBoxFuture<'a, Result<(), RuntimeFailure>> {
        Box::pin(futures::future::ready(Ok(())))
    }
}

pub(crate) fn identities(middleware: &[Rc<dyn WebIngressMiddleware>]) -> Vec<String> {
    middleware
        .iter()
        .map(|middleware| middleware.identity().to_owned())
        .collect()
}

pub(crate) fn validate(middleware: &[Rc<dyn WebIngressMiddleware>]) -> Result<(), String> {
    if middleware
        .iter()
        .any(|middleware| middleware.identity().trim().is_empty())
    {
        return Err("Web Ingress middleware identity must not be empty".to_owned());
    }
    Ok(())
}

pub(crate) async fn run<F, Fut>(
    middleware: &[Rc<dyn WebIngressMiddleware>],
    mut request: WebIngressRequest,
    dispatch: F,
) -> Result<WebIngressResponse, RuntimeFailure>
where
    F: FnOnce(&WebIngressRequest) -> Fut,
    Fut: Future<Output = WebIngressResponse>,
{
    let mut entered = 0;
    let mut response = None;
    for layer in middleware {
        entered += 1;
        match layer.before_request(&mut request).await? {
            WebIngressMiddlewareOutcome::Continue => {}
            WebIngressMiddlewareOutcome::Respond(value) => {
                response = Some(value);
                break;
            }
        }
    }
    let mut response = match response {
        Some(response) => response,
        None => dispatch(&request).await,
    };
    for layer in middleware[..entered].iter().rev() {
        layer.after_response(&request, &mut response).await?;
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::{WebIngressMiddleware, WebIngressMiddlewareOutcome, WebIngressRequest, validate};
    use futures::future::LocalBoxFuture;
    use lenso_kernel::RuntimeFailure;

    #[derive(Debug)]
    struct EmptyIdentity;

    impl WebIngressMiddleware for EmptyIdentity {
        fn identity(&self) -> &'static str {
            ""
        }

        fn before_request<'a>(
            &'a self,
            _request: &'a mut WebIngressRequest,
        ) -> LocalBoxFuture<'a, Result<WebIngressMiddlewareOutcome, RuntimeFailure>> {
            Box::pin(futures::future::ready(Ok(
                WebIngressMiddlewareOutcome::Continue,
            )))
        }
    }

    #[test]
    fn empty_identity_is_rejected_before_startup() {
        let middleware: Vec<Rc<dyn WebIngressMiddleware>> = vec![Rc::new(EmptyIdentity)];

        assert_eq!(
            validate(&middleware).unwrap_err(),
            "Web Ingress middleware identity must not be empty"
        );
    }
}
