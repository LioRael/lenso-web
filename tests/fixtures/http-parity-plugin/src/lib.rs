//! One generated-contract Endpoint fixture shared by native and Workers probes.
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan, ResolvedAppPlan,
};
use lenso_capability_http_endpoint::*;
use lenso_kernel::{InvocationContext, NativeRequestFuture, RuntimeFailure};
use lenso_native_adapter::{NativePluginFactory, NativePluginFactoryContext, NativePluginInstance};
use std::rc::Rc;

pub const PACKAGE_ID: &str = "fixture.http-parity";
pub const CORPUS: &str = include_str!("../corpus.json");

pub fn plan(configuration: String) -> ResolvedAppPlan {
    AppComposition::new(
        vec![
            PluginInstancePlan::new("endpoint", PACKAGE_ID).with_capability(
                CapabilityEndpointPlan::new(
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                    [DESCRIBE_OPERATION, HANDLE_OPERATION],
                ),
            ),
            PluginInstancePlan::new("ingress", "lenso.web-ingress")
                .with_configuration(configuration)
                .with_requirement(CapabilityRequirementPlan::many(
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                )),
        ],
        vec![
            CapabilityBinding::new("ingress", CAPABILITY_ID, DESCRIPTOR_VERSION, "endpoint")
                .with_limits(0, 16),
        ],
    )
    .resolve()
    .unwrap()
}

#[derive(Debug, Clone, Default)]
pub struct HttpParityEndpointFactory;
impl NativePluginFactory for HttpParityEndpointFactory {
    fn package_id(&self) -> &'static str {
        PACKAGE_ID
    }
    fn package_version(&self) -> &'static str {
        "0.0.0"
    }
    fn instantiate(
        &self,
        _: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::new(vec![Rc::new(
            EndpointEndpoint::new(HttpParityEndpoint),
        )]))
    }
}

#[derive(Debug)]
struct HttpParityEndpoint;
impl EndpointProvider for HttpParityEndpoint {
    fn describe(
        &self,
        _: InvocationContext,
        _: DescribeRequest,
    ) -> NativeRequestFuture<EndpointDescribe> {
        let mut routes = [
            ("echo", "GET", "/echo/{value}"),
            ("static", "GET", "/echo/static"),
            ("bytes", "POST", "/bytes"),
            ("cookies", "GET", "/cookies"),
            ("head", "HEAD", "/head"),
            ("no-content", "GET", "/no-content"),
            ("not-modified", "GET", "/not-modified"),
            ("reject", "GET", "/reject"),
            ("failure", "GET", "/failure"),
            ("invalid", "GET", "/invalid"),
            ("blocked", "GET", "/blocked"),
        ]
        .into_iter()
        .map(|(route_id, method, path)| DescribeResponseRoutesItem {
            route_id: route_id.into(),
            method: method.into(),
            path: path.into(),
            openapi: None,
        })
        .collect::<Vec<_>>();
        for method in [
            "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD", "TRACE",
        ] {
            routes.push(DescribeResponseRoutesItem {
                route_id: "method".into(),
                method: method.into(),
                path: "/method".into(),
                openapi: None,
            });
        }
        Box::pin(async move { Ok(Ok(DescribeResponse { routes })) })
    }
    fn handle(
        &self,
        _: InvocationContext,
        request: HandleRequest,
    ) -> NativeRequestFuture<EndpointHandle> {
        Box::pin(async move {
            let (status, headers, body) = match request.route_id.as_str() {
                "blocked" => return futures::future::pending().await,
                "reject" => return Ok(Err(HandleError::Rejected)),
                "failure" => {
                    return Err(RuntimeFailure::PluginFailure {
                        detail: "parity fixture failure".into(),
                    });
                }
                "invalid" => (1000, vec![], vec![]),
                "bytes" => (200, vec![], request.body.into_shared().to_vec()),
                "head" => (200, vec![], b"discard this HEAD body".to_vec()),
                "no-content" => (204, vec![], b"discard this 204 body".to_vec()),
                "not-modified" => (304, vec![], b"discard this 304 body".to_vec()),
                "cookies" => (
                    200,
                    vec![
                        HandleResponseHeadersItem {
                            name: "Set-Cookie".into(),
                            value: "a=1; Secure; HttpOnly; SameSite=Lax; Path=/".into(),
                        },
                        HandleResponseHeadersItem {
                            name: "set-cookie".into(),
                            value: "b=2; Secure; HttpOnly; SameSite=Strict; Path=/".into(),
                        },
                    ],
                    vec![],
                ),
                "method" => (200, vec![], request.method.into_bytes()),
                _ => (200, vec![], serde_json::to_vec(&request).unwrap()),
            };
            Ok(Ok(HandleResponse {
                status,
                headers,
                body: body.into(),
            }))
        })
    }
}
