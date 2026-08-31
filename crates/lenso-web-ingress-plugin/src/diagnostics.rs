//! Host-owned diagnostics for failures hidden by the HTTP protocol boundary.

use std::fmt;

use lenso_kernel::RuntimeFailure;

/// Receives structured internal failures before Web Ingress maps them to safe HTTP responses.
///
/// Implementations must not mutate routing or response behavior. The observer receives no
/// credentials or request body, and installing one does not change the immutable route manifest.
pub trait WebIngressDiagnostics: fmt::Debug {
    /// Observes one Endpoint Runtime Failure before it becomes a generic `503` or `504` response.
    fn endpoint_runtime_failure(&self, _event: WebIngressEndpointFailure<'_>) {}
}

/// One Endpoint Runtime Failure correlated with its trusted Ingress request identity.
#[derive(Clone, Copy, Debug)]
pub struct WebIngressEndpointFailure<'a> {
    request_id: &'a str,
    route_id: &'a str,
    provider_index: usize,
    failure: &'a RuntimeFailure,
}

impl<'a> WebIngressEndpointFailure<'a> {
    pub(crate) const fn new(
        request_id: &'a str,
        route_id: &'a str,
        provider_index: usize,
        failure: &'a RuntimeFailure,
    ) -> Self {
        Self {
            request_id,
            route_id,
            provider_index,
            failure,
        }
    }

    /// Returns the trusted request ID assigned by Web Ingress.
    #[must_use]
    pub const fn request_id(self) -> &'a str {
        self.request_id
    }

    /// Returns the stable route ID selected by the immutable route table.
    #[must_use]
    pub const fn route_id(self) -> &'a str {
        self.route_id
    }

    /// Returns the bound Endpoint provider's stable index within this Ingress Instance.
    #[must_use]
    pub const fn provider_index(self) -> usize {
        self.provider_index
    }

    /// Returns the internal Runtime Failure retained for Host diagnostics.
    #[must_use]
    pub const fn failure(self) -> &'a RuntimeFailure {
        self.failure
    }
}

#[derive(Debug, Default)]
pub(crate) struct NoopDiagnostics;

impl WebIngressDiagnostics for NoopDiagnostics {}
