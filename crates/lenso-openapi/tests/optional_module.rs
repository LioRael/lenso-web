use std::{collections::BTreeMap, fmt::Write as _, net::SocketAddr, rc::Rc, time::Duration};

use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    ModuleInstancePlan, ResolvedAppPlan,
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, EndpointEndpoint,
    EndpointHandleInvocationError, HANDLE_OPERATION, HandleResponse, endpoint,
    response::{self, StatusCode},
};
use lenso_kernel::{Kernel, NativeRequestEndpoint, RuntimeFailure, ShutdownOutcome};
use lenso_native_adapter::{
    NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance, NativeModuleRegistry,
};
use lenso_openapi::{OpenApiFactory, PACKAGE_ID as OPENAPI_PACKAGE_ID};
use lenso_runner::TokioDriver;
use lenso_web_ingress::{PACKAGE_ID as INGRESS_PACKAGE_ID, WebIngressFactory};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::LocalSet,
};

const ORDERS_PACKAGE_ID: &str = "fixture.orders-http";
const STATUS_PACKAGE_ID: &str = "fixture.status-http";
const FIXTURE_VERSION: &str = "0.0.0";

#[tokio::test(flavor = "current_thread")]
async fn app_composition_explicitly_enables_and_removes_openapi() {
    LocalSet::new()
        .run_until(async {
            let ingress = WebIngressFactory::default();
            let enabled = start(plan(true), ingress.clone()).await;
            let address = ingress.local_address().unwrap();

            let document = request(address, "/openapi.json").await;
            assert_eq!(document.status, 200);
            assert_eq!(
                document.headers.get("content-type").map(String::as_str),
                Some("application/json; charset=utf-8")
            );
            let document: serde_json::Value = serde_json::from_str(&document.body).unwrap();
            assert_eq!(document["info"]["title"], "Orders API");
            assert_eq!(
                document["paths"]["/orders/{order_id}"]["get"]["operationId"],
                "orders.read"
            );
            assert_eq!(
                document["paths"]["/orders/{order_id}"]["get"]["parameters"][0]["name"],
                "order_id"
            );
            assert!(document["paths"].get("/openapi.json").is_none());
            assert!(document["paths"].get("/status").is_none());
            assert_eq!(request(address, "/status").await.status, 204);
            assert_eq!(
                enabled.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );

            let ingress = WebIngressFactory::default();
            let disabled = start(plan(false), ingress.clone()).await;
            let address = ingress.local_address().unwrap();
            assert_eq!(request(address, "/openapi.json").await.status, 404);
            assert_eq!(request(address, "/orders/order-42").await.status, 200);
            assert_eq!(
                disabled.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
        })
        .await;
}

async fn start(plan: ResolvedAppPlan, ingress: WebIngressFactory) -> lenso_kernel::NativeApp {
    Kernel::start_native(
        plan,
        TokioDriver::new(),
        NativeModuleRegistry::new()
            .with_factory(ingress)
            .with_factory(OrdersFactory)
            .with_factory(StatusFactory)
            .with_factory(OpenApiFactory),
    )
    .await
    .unwrap()
}

fn plan(openapi: bool) -> ResolvedAppPlan {
    let ingress = ModuleInstancePlan::new("web-ingress", INGRESS_PACKAGE_ID).with_requirement(
        CapabilityRequirementPlan::many(CAPABILITY_ID, DESCRIPTOR_VERSION),
    );
    let orders = ModuleInstancePlan::new("orders-http", ORDERS_PACKAGE_ID).with_capability(
        CapabilityEndpointPlan::new(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            [DESCRIBE_OPERATION, HANDLE_OPERATION],
        ),
    );
    let status = ModuleInstancePlan::new("status-http", STATUS_PACKAGE_ID).with_capability(
        CapabilityEndpointPlan::new(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            [DESCRIBE_OPERATION, HANDLE_OPERATION],
        ),
    );
    let mut modules = vec![ingress, orders, status];
    let mut bindings = vec![
        CapabilityBinding::new(
            "web-ingress",
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            "orders-http",
        ),
        CapabilityBinding::new(
            "web-ingress",
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            "status-http",
        ),
    ];
    if openapi {
        modules.push(
            ModuleInstancePlan::new("openapi", OPENAPI_PACKAGE_ID)
                .with_configuration(r#"{"title":"Orders API","version":"1.0.0"}"#)
                .with_capability(CapabilityEndpointPlan::new(
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                    [DESCRIBE_OPERATION, HANDLE_OPERATION],
                ))
                .with_requirement(CapabilityRequirementPlan::many(
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                )),
        );
        bindings.push(CapabilityBinding::new(
            "openapi",
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            "orders-http",
        ));
        bindings.push(CapabilityBinding::new(
            "web-ingress",
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            "openapi",
        ));
    }
    AppComposition::new(modules, bindings).resolve().unwrap()
}

#[derive(Clone, Copy, Debug)]
struct OrdersFactory;

impl NativeModuleFactory for OrdersFactory {
    fn package_id(&self) -> &'static str {
        ORDERS_PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        FIXTURE_VERSION
    }

    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        let endpoint = Rc::new(EndpointEndpoint::new(OrdersHttp)) as Rc<dyn NativeRequestEndpoint>;
        Ok(NativeModuleInstance::new(vec![endpoint]))
    }
}

#[derive(Clone, Copy, Debug)]
struct OrdersHttp;

#[endpoint]
impl OrdersHttp {
    #[get("orders.read", "/orders/{order_id}")]
    #[openapi(
        r#"{"summary":"Read an order","responses":{"200":{"description":"Order","content":{"application/json":{"schema":{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}}}}}}"#
    )]
    async fn read(&self) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        Ok(response::json(
            StatusCode::OK,
            &serde_json::json!({"id": "order-42"}),
        )?)
    }
}

#[derive(Clone, Copy, Debug)]
struct StatusFactory;

impl NativeModuleFactory for StatusFactory {
    fn package_id(&self) -> &'static str {
        STATUS_PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        FIXTURE_VERSION
    }

    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        let endpoint = Rc::new(EndpointEndpoint::new(StatusHttp)) as Rc<dyn NativeRequestEndpoint>;
        Ok(NativeModuleInstance::new(vec![endpoint]))
    }
}

#[derive(Clone, Copy, Debug)]
struct StatusHttp;

#[endpoint]
impl StatusHttp {
    #[get("status.read", "/status")]
    #[openapi(r#"{"summary":"Read internal status"}"#)]
    async fn read(&self) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        Ok(response::empty(StatusCode::NO_CONTENT))
    }
}

struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

async fn request(address: SocketAddr, path: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut wire = String::new();
    write!(
        wire,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    stream.write_all(wire.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    HttpResponse {
        status: head.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers: head
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
            .collect(),
        body: body.to_owned(),
    }
}
