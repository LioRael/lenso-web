//! Optional `OpenAPI` 3.1 document Module for explicitly bound HTTP Endpoints.
//!
//! Merely linking this crate changes no App. App Composition must select an
//! Instance, bind the Endpoint descriptions to document, and bind this Module's
//! own Endpoint to Web Ingress.

mod assemble;
mod config;

use std::{cell::RefCell, rc::Rc};

use lenso_capability_http_endpoint::{
    DESCRIBE_OPERATION, DescribeError, DescribeRequest, DescribeResponse,
    DescribeResponseRoutesItem, EndpointDescribe, EndpointEndpoint, EndpointHandle,
    EndpointProvider, HandleError, HandleRequest, HandleResponse, HandleResponseHeadersItem,
};
use lenso_kernel::{
    ActivateContext, DeactivateContext, InvocationContext, ModuleFuture, ModuleLifecycle,
    NativeRequestEndpoint, NativeRequestFuture, RuntimeFailure,
};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use serde_json::json;

pub use config::OpenApiConfig;

pub const PACKAGE_ID: &str = "lenso.openapi";
pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DOCUMENT_ROUTE_ID: &str = "lenso.openapi.document";

#[derive(Clone, Debug, Default)]
pub struct OpenApiFactory;

impl NativeModuleFactory for OpenApiFactory {
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
        if context.entrypoint() != "default" {
            return Err(RuntimeFailure::InvalidResolvedPlan {
                detail: format!("unsupported OpenAPI entrypoint {}", context.entrypoint()),
            });
        }
        let config =
            serde_json::from_str::<OpenApiConfig>(context.configuration()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("OpenAPI configuration is invalid: {error}"),
                }
            })?;
        config
            .validate()
            .map_err(|detail| RuntimeFailure::InvalidResolvedPlan {
                detail: format!("OpenAPI configuration is invalid: {detail}"),
            })?;
        let document = Rc::new(RefCell::new(None));
        let provider = OpenApiEndpoint {
            config: config.clone(),
            document: document.clone(),
        };
        let endpoint = Rc::new(EndpointEndpoint::new(provider)) as Rc<dyn NativeRequestEndpoint>;
        Ok(NativeModuleInstance::with_lifecycle(
            vec![endpoint],
            OpenApiLifecycle { config, document },
        ))
    }
}

#[derive(Clone, Debug)]
struct OpenApiEndpoint {
    config: OpenApiConfig,
    document: Rc<RefCell<Option<Vec<u8>>>>,
}

impl EndpointProvider for OpenApiEndpoint {
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

#[derive(Debug)]
struct OpenApiLifecycle {
    config: OpenApiConfig,
    document: Rc<RefCell<Option<Vec<u8>>>>,
}

impl ModuleLifecycle for OpenApiLifecycle {
    fn activate(&self, context: ActivateContext) -> ModuleFuture {
        let config = self.config.clone();
        let document = self.document.clone();
        let dependencies = context.dependencies().clone();
        Box::pin(async move {
            let descriptors = dependencies.many::<EndpointDescribe>()?;
            let mut descriptions = Vec::with_capacity(descriptors.len());
            for (provider_index, descriptor) in descriptors.into_iter().enumerate() {
                let description = descriptor
                    .invoke(DESCRIBE_OPERATION, DescribeRequest {})
                    .await?
                    .map_err(|error| describe_failure(provider_index, &error))?;
                descriptions.push(description);
            }
            let assembled = assemble::assemble(&config, descriptions).map_err(module_failure)?;
            document.borrow_mut().replace(assembled);
            Ok(())
        })
    }

    fn deactivate(&self, _context: DeactivateContext) -> ModuleFuture {
        self.document.borrow_mut().take();
        Box::pin(futures::future::ready(Ok(())))
    }
}

fn describe_failure(provider_index: usize, error: &DescribeError) -> RuntimeFailure {
    module_failure(format!(
        "OpenAPI Endpoint provider {provider_index} rejected its description: {error:?}"
    ))
}

fn module_failure(detail: impl Into<String>) -> RuntimeFailure {
    RuntimeFailure::ModuleFailure {
        detail: detail.into(),
    }
}
