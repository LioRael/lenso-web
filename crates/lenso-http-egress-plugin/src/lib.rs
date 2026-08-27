//! General-purpose, policy-bounded linked Rust HTTP Egress Plugin.

mod config;
mod provider;

use std::{cell::RefCell, rc::Rc};

use lenso::{DeactivateContext, Lifecycle, PrepareContext, provides};
use lenso_capability_http_client as http_client;
use lenso_capability_http_client::{Client, ClientProvider, SendRequest};
use lenso_kernel::{InvocationContext, NativeRequestFuture, RuntimeFailure};
use reqwest::redirect::Policy;

pub use config::HttpEgressConfig;

use crate::provider::HttpEgressProvider;

fn validate_config(config: &HttpEgressConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map(|_| ())
        .map_err(|detail| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("HTTP Egress configuration is invalid: {detail}"),
        })
}

#[lenso::plugin(
    lifecycle,
    validate = validate_config,
    configuration_schema = "config.schema.json"
)]
#[derive(Clone, Debug)]
struct HttpEgressPlugin {
    #[config]
    config: HttpEgressConfig,
    provider: Rc<RefCell<Option<HttpEgressProvider>>>,
}

#[provides(http_client::Client)]
impl ClientProvider for HttpEgressPlugin {
    fn send(
        &self,
        context: InvocationContext,
        request: SendRequest,
    ) -> NativeRequestFuture<Client> {
        let provider = self.provider.borrow().clone();
        match provider {
            Some(provider) => provider.send(context, request),
            None => Box::pin(async {
                Err(RuntimeFailure::PluginFailure {
                    detail: "HTTP Egress is not prepared".to_owned(),
                })
            }),
        }
    }
}

#[allow(unknown_lints, clippy::unused_async_trait_impl)]
impl Lifecycle for HttpEgressPlugin {
    async fn prepare(&self, _context: PrepareContext) -> Result<(), RuntimeFailure> {
        let allowed_origins =
            self.config
                .validate()
                .map_err(|detail| RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("HTTP Egress configuration is invalid: {detail}"),
                })?;
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .no_proxy()
            .connect_timeout(self.config.connect_timeout())
            .timeout(self.config.request_timeout())
            .user_agent(format!("lenso-http-egress-plugin/{PACKAGE_VERSION}"))
            .build()
            .map_err(|error| RuntimeFailure::PluginFailure {
                detail: format!("HTTP Egress client could not be prepared: {error}"),
            })?;
        self.provider.borrow_mut().replace(HttpEgressProvider::new(
            client,
            self.config.clone(),
            allowed_origins,
        ));
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        self.provider.borrow_mut().take();
        Ok(())
    }
}
