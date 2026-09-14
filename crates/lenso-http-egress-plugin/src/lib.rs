//! Exact-origin HTTP Egress Plugin for socket and event hosts.
mod config;
mod event;
#[cfg(feature = "native")]
mod native;
mod policy;
#[cfg(feature = "native")]
mod provider;
#[cfg(all(feature = "workers", target_arch = "wasm32"))]
mod workers;

pub use config::{HttpEgressConfig, HttpVersionPolicy};
pub use event::{
    HttpEgressEventFactory, HttpEventError, HttpEventLimits, HttpEventRequest, HttpEventTransport,
};
#[cfg(feature = "native")]
pub use native::*;
#[cfg(not(feature = "native"))]
pub const PACKAGE_ID: &str = "lenso.http-egress";
#[cfg(not(feature = "native"))]
pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Shared immutable configuration schema.
pub const CONFIGURATION_SCHEMA_JSON: &str = include_str!("../config.schema.json");
