//! HTTP Ingress Plugin with shared semantics for socket and event hosts.
mod config;
mod diagnostics;
mod event;
mod ingress;
mod manifest;
mod middleware;
#[cfg(feature = "native")]
mod native;
#[cfg(feature = "native")]
mod replication;
mod routing;
#[cfg(feature = "native")]
mod server;
mod session_cookie;

pub use config::{SessionCookieConfig, WebIngressConfig};
pub use diagnostics::{WebIngressDiagnostics, WebIngressEndpointFailure};
pub use event::WebIngressEventFactory;
use lenso_app_plan::{CapabilityRequirementPlan, authoring::PluginDescriptor};
use lenso_kernel::RuntimeFailure;
pub use manifest::{WebIngressReplicaMismatch, WebIngressRoute, WebIngressRouteManifest};
pub use middleware::{
    WebIngressMiddleware, WebIngressMiddlewareOutcome, WebIngressRequest, WebIngressResponse,
};
#[cfg(feature = "native")]
pub use native::WebIngressFactory;
#[cfg(feature = "native")]
pub use replication::WebIngressListenerCoordinator;

pub const PACKAGE_ID: &str = "lenso.web-ingress";
pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Package-owned configuration schema, shared by native and event ingress.
pub const CONFIGURATION_SCHEMA_JSON: &str = include_str!("../config.schema.json");
fn plugin_descriptor() -> PluginDescriptor {
    PluginDescriptor::new(PACKAGE_ID, PACKAGE_VERSION, "http-ingress")
        .with_requirement(CapabilityRequirementPlan::many(
            lenso_capability_http_endpoint::CAPABILITY_ID,
            lenso_capability_http_endpoint::DESCRIPTOR_VERSION,
        ))
        .with_requirement(CapabilityRequirementPlan::many(
            lenso_capability_http_stream_endpoint::CAPABILITY_ID,
            lenso_capability_http_stream_endpoint::DESCRIPTOR_VERSION,
        ))
        .with_configuration_schema(
            serde_json::from_str(CONFIGURATION_SCHEMA_JSON)
                .expect("the embedded Web Ingress configuration schema is valid"),
        )
}

fn plugin_failure(detail: impl Into<String>) -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: detail.into(),
    }
}
