//! General-purpose, policy-bounded linked Rust HTTP Egress Module.

mod config;
mod provider;

use std::rc::Rc;

use lenso_capability_http_client::ClientEndpoint;
use lenso_kernel::{NativeRequestEndpoint, RuntimeFailure};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use reqwest::redirect::Policy;

pub use config::HttpEgressConfig;

use crate::provider::HttpEgressProvider;

/// Package identity for the linked Rust HTTP Egress Module.
pub const PACKAGE_ID: &str = "lenso.http-egress";
/// Exact Cargo package version linked into the Host.
pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Factory for immutable outbound HTTP authority configured by the Resolved App Plan.
#[derive(Clone, Copy, Debug, Default)]
pub struct HttpEgressFactory;

impl NativeModuleFactory for HttpEgressFactory {
    fn package_id(&self) -> &'static str {
        PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }

    fn instantiate(
        &self,
        context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        let config =
            serde_json::from_str::<HttpEgressConfig>(context.configuration()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("HTTP Egress configuration is invalid: {error}"),
                }
            })?;
        let allowed_origins =
            config
                .validate()
                .map_err(|detail| RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("HTTP Egress configuration is invalid: {detail}"),
                })?;
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .no_proxy()
            .connect_timeout(config.connect_timeout())
            .timeout(config.request_timeout())
            .user_agent(format!("lenso-http-egress/{PACKAGE_VERSION}"))
            .build()
            .map_err(|error| RuntimeFailure::ModuleFailure {
                detail: format!("HTTP Egress client could not be prepared: {error}"),
            })?;
        let endpoint = Rc::new(ClientEndpoint::new(HttpEgressProvider::new(
            client,
            config,
            allowed_origins,
        ))) as Rc<dyn NativeRequestEndpoint>;
        Ok(NativeModuleInstance::new(vec![endpoint]))
    }
}
