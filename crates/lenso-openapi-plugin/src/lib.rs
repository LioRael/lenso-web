//! Optional `OpenAPI` 3.1 document Plugin for explicitly bound HTTP Endpoints.
//!
//! Merely linking this crate changes no App. App Composition must select an
//! Instance, bind the Endpoint descriptions to document, and bind this Plugin's
//! own Endpoint to Web Ingress.

mod assemble;
mod config;

use std::{cell::RefCell, rc::Rc};

use lenso::prelude::ManyPort;
use lenso::{ActivateContext, DeactivateContext, Lifecycle, provides};
use lenso_capability_http_endpoint as http_endpoint;
use lenso_capability_http_endpoint::{
    DescribeError, DescribeRequest, DescribeResponse, DescribeResponseRoutesItem, EndpointDescribe,
    EndpointHandle, EndpointProvider, HandleError, HandleRequest, HandleResponse,
    HandleResponseHeadersItem,
};
use lenso_kernel::{InvocationContext, NativeRequestFuture, RuntimeFailure};
use serde_json::json;

pub use config::OpenApiConfig;

pub const DOCUMENT_ROUTE_ID: &str = "lenso.openapi.document";

fn validate_config(config: &OpenApiConfig) -> Result<(), RuntimeFailure> {
    config
        .validate()
        .map_err(|detail| RuntimeFailure::InvalidResolvedPlan {
            detail: format!("OpenAPI configuration is invalid: {detail}"),
        })
}

#[lenso::plugin(
    lifecycle,
    validate = validate_config,
    configuration_schema = "config.schema.json"
)]
#[derive(Clone, Debug)]
struct OpenApiPlugin {
    #[config]
    config: OpenApiConfig,
    endpoints: ManyPort<http_endpoint::EndpointClient>,
    document: Rc<RefCell<Option<Vec<u8>>>>,
}

#[provides(http_endpoint::Endpoint)]
impl EndpointProvider for OpenApiPlugin {
    fn describe(
        &self,
        _context: InvocationContext,
        _request: DescribeRequest,
    ) -> NativeRequestFuture<EndpointDescribe> {
        let route = DescribeResponseRoutesItem {
            method: "GET".to_owned(),
            openapi: Some(
                json!({
                    "summary": "Get the OpenAPI document",
                    "responses": {
                        "200": {
                            "description": "OpenAPI 3.1 document",
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object"}
                                }
                            }
                        }
                    }
                })
                .as_object()
                .expect("the OpenAPI operation is an object")
                .clone()
                .into_iter()
                .collect(),
            ),
            path: self.config.document_path().to_owned(),
            route_id: DOCUMENT_ROUTE_ID.to_owned(),
        };
        Box::pin(futures::future::ready(Ok(Ok(DescribeResponse {
            routes: vec![route],
        }))))
    }

    fn handle(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> NativeRequestFuture<EndpointHandle> {
        let result = if request.route_id == DOCUMENT_ROUTE_ID {
            self.document.borrow().clone().map_or_else(
                || {
                    Err(RuntimeFailure::Internal {
                        detail: "OpenAPI document is unavailable before activation".to_owned(),
                    })
                },
                |document| {
                    Ok(Ok(HandleResponse {
                        body: document.into(),
                        headers: vec![HandleResponseHeadersItem {
                            name: "content-type".to_owned(),
                            value: "application/json; charset=utf-8".to_owned(),
                        }],
                        status: 200,
                    }))
                },
            )
        } else {
            Ok(Err(HandleError::Rejected))
        };
        Box::pin(futures::future::ready(result))
    }
}

#[allow(unknown_lints, clippy::unused_async_trait_impl)]
impl Lifecycle for OpenApiPlugin {
    async fn activate(&self, _context: ActivateContext) -> Result<(), RuntimeFailure> {
        let mut descriptions = Vec::with_capacity(self.endpoints.len());
        for (provider_index, endpoint) in self.endpoints.iter().enumerate() {
            let description = endpoint
                .describe(DescribeRequest {})
                .await
                .map_err(|error| match error {
                    http_endpoint::EndpointDescribeInvocationError::Domain(error) => {
                        describe_failure(provider_index, &error)
                    }
                    http_endpoint::EndpointDescribeInvocationError::Runtime(error) => error,
                })?;
            descriptions.push(description);
        }
        let assembled = assemble::assemble(&self.config, descriptions).map_err(plugin_failure)?;
        self.document.borrow_mut().replace(assembled);
        Ok(())
    }

    async fn deactivate(&self, _context: DeactivateContext) -> Result<(), RuntimeFailure> {
        self.document.borrow_mut().take();
        Ok(())
    }
}

fn describe_failure(provider_index: usize, error: &DescribeError) -> RuntimeFailure {
    plugin_failure(format!(
        "OpenAPI Endpoint provider {provider_index} rejected its description: {error:?}"
    ))
}

fn plugin_failure(detail: impl Into<String>) -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: detail.into(),
    }
}
