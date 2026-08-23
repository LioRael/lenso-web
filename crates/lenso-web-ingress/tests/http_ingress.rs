use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fmt::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};

use futures::future::LocalBoxFuture;
use lenso_authoring::{
    Binding, CapabilityEndpoint, CapabilityRequirement, ContractInput, Module, PackageInput,
    PackageSource, ProjectAuthoring, ProjectFile, ResolutionOptions,
};
use lenso_capability_http_endpoint::{
    Bytes as ContractBytes, CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, DescribeRequest,
    DescribeResponse, DescribeResponseRoutesItem, EndpointDescribeInvocationError,
    EndpointEndpoint, EndpointHandleInvocationError, EndpointProvider, HANDLE_OPERATION,
    HandleError, HandleRequest, HandleResponse, HandleResponseHeadersItem,
};
use lenso_kernel::{InvocationContext, Kernel, NativeApp, RuntimeFailure};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use lenso_runner::TokioDriver;
use lenso_web_ingress::{PACKAGE_ID, WebIngressConfig, WebIngressFactory};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::LocalSet,
};

const ORDERS_PACKAGE_ID: &str = "fixture.orders-http";
const STATUS_PACKAGE_ID: &str = "fixture.status-http";

#[tokio::test(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn routes_bound_backend_modules_and_preserves_http_evidence() {
    LocalSet::new()
        .run_until(async {
            let active_calls = Rc::new(Cell::new(0));
            let max_active_calls = Rc::new(Cell::new(0));
            let orders = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [
                    ("orders.read", "GET", "/orders/{order_id}"),
                    ("orders.slow", "GET", "/orders-slow"),
                    ("orders.panic", "GET", "/panic"),
                ],
            )
            .with_call_tracker(active_calls.clone(), max_active_calls.clone());
            let status = FixtureEndpointFactory::new(
                STATUS_PACKAGE_ID,
                [
                    ("status.read", "GET", "/health"),
                    ("status.slow", "GET", "/status-slow"),
                ],
            )
            .with_call_tracker(active_calls, max_active_calls);
            let ingress = WebIngressFactory::default();
            let app = start(
                project(&[
                    ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID),
                    ProviderPlan::new("status-http", STATUS_PACKAGE_ID),
                ]),
                &ingress,
                [orders.clone(), status.clone()],
            )
            .await
            .expect("App should start with two immutable HTTP Endpoint providers");
            let address = ingress.local_address().unwrap();

            let orders_response = request(
                address,
                "GET",
                "/orders/42?include=items",
                &[("Authorization", "Bearer good-token"), ("X-Tenant", "acme")],
                "",
            )
            .await;
            assert_eq!(orders_response.status, 200);
            assert_eq!(
                orders_response.body,
                r#"{"provider":"fixture.orders-http","route":"orders.read"}"#
            );
            assert!(
                orders_response
                    .headers
                    .get("x-request-id")
                    .is_some_and(|value| value.starts_with("lenso-"))
            );
            assert_eq!(
                orders_response
                    .headers
                    .get("x-content-type-options")
                    .map(String::as_str),
                Some("nosniff")
            );
            let observed = orders.observed().expect("orders provider saw request");
            assert_eq!(observed.route_id, "orders.read");
            assert_eq!(observed.path, "/orders/42");
            assert_eq!(observed.query.as_deref(), Some("include=items"));
            assert_eq!(
                observed
                    .path_parameters
                    .iter()
                    .map(|item| (item.name.as_str(), item.value.as_str()))
                    .collect::<Vec<_>>(),
                [("order_id", "42")]
            );
            assert_eq!(observed.credential.unwrap().scheme, "bearer");
            assert!(
                observed
                    .headers
                    .iter()
                    .any(|header| { header.name == "x-tenant" && header.value == "acme" })
            );
            assert!(
                !observed
                    .headers
                    .iter()
                    .any(|header| { header.name == "authorization" || header.name == "cookie" })
            );

            assert_hop_filtering_and_parallel_dispatch(address, &orders).await;

            assert_eq!(request(address, "GET", "/panic", &[], "").await.status, 503);

            let health = request(address, "GET", "/health", &[], "").await;
            assert_eq!(health.status, 200);
            assert!(health.body.contains(STATUS_PACKAGE_ID));
            assert!(status.observed().is_some());

            assert_eq!(
                request(address, "POST", "/health", &[], "").await.status,
                405
            );
            assert_eq!(
                request(address, "GET", "/missing", &[], "").await.status,
                404
            );
            app.shutdown(Duration::from_secs(1)).await;
        })
        .await;
}

async fn assert_hop_filtering_and_parallel_dispatch(
    address: SocketAddr,
    orders: &FixtureEndpointFactory,
) {
    let hop_filtered = request(
        address,
        "GET",
        "/orders/43",
        &[("Connection", "close, x-hop"), ("X-Hop", "secret")],
        "",
    )
    .await;
    assert_eq!(hop_filtered.status, 200);
    assert!(
        !orders
            .observed()
            .unwrap()
            .headers
            .iter()
            .any(|header| header.name == "x-hop")
    );

    let (slow_one, slow_two) = tokio::join!(
        request(address, "GET", "/orders-slow", &[], ""),
        request(address, "GET", "/status-slow", &[], "")
    );
    assert_eq!(slow_one.status, 200);
    assert_eq!(slow_two.status, 200);
    assert_eq!(orders.max_active_calls(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn route_collisions_fail_activation_before_readiness() {
    LocalSet::new()
        .run_until(async {
            let first = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [("orders.read", "GET", "/orders/{order_id}")],
            );
            let second = FixtureEndpointFactory::new(
                STATUS_PACKAGE_ID,
                [("orders.copy", "GET", "/orders/{another_id}")],
            );
            let ingress = WebIngressFactory::default();
            let error = start(
                project(&[
                    ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID),
                    ProviderPlan::new("status-http", STATUS_PACKAGE_ID),
                ]),
                &ingress,
                [first, second],
            )
            .await
            .expect_err("colliding routes must fail before App readiness");
            assert!(format!("{error:?}").contains("HTTP route collision"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn concurrency_limit_backpressures_without_dropping_requests() {
    LocalSet::new()
        .run_until(async {
            let endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [("orders.slow", "GET", "/orders-slow")],
            );
            let ingress = WebIngressFactory::new(WebIngressConfig {
                max_concurrent_requests: 1,
                ..WebIngressConfig::default()
            });
            let app = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &ingress,
                [endpoint.clone()],
            )
            .await
            .expect("App should start with a single-request concurrency limit");
            let address = ingress.local_address().unwrap();

            let (first, second) = tokio::join!(
                request(address, "GET", "/orders-slow", &[], ""),
                request(address, "GET", "/orders-slow", &[], "")
            );
            assert_eq!(first.status, 200);
            assert_eq!(second.status, 200);
            assert_eq!(endpoint.max_active_calls(), 1);
            app.shutdown(Duration::from_secs(1)).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn transport_limits_duplicate_credentials_and_endpoint_failures_are_mapped() {
    LocalSet::new()
        .run_until(async {
            let endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [
                    ("orders.read", "GET", "/orders/{order_id}"),
                    ("orders.reject", "GET", "/reject"),
                    ("orders.invalid", "GET", "/invalid"),
                    ("orders.hop-response", "GET", "/hop-response"),
                ],
            );
            let ingress = WebIngressFactory::new(WebIngressConfig {
                max_request_body_bytes: 32,
                max_request_head_bytes: 512,
                ..WebIngressConfig::default()
            });
            let app = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &ingress,
                [endpoint],
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();

            assert_eq!(
                request(address, "POST", "/orders/42", &[], &"a".repeat(33))
                    .await
                    .status,
                413
            );
            assert_eq!(
                request(
                    address,
                    "GET",
                    "/orders/42",
                    &[
                        ("Authorization", "Bearer first"),
                        ("Authorization", "Bearer second"),
                    ],
                    "",
                )
                .await
                .status,
                400
            );
            assert_eq!(
                request(address, "GET", "/reject", &[], "").await.status,
                502
            );
            assert_eq!(
                request(address, "GET", "/invalid", &[], "").await.status,
                502
            );
            assert_eq!(
                request(address, "GET", "/hop-response", &[], "")
                    .await
                    .status,
                502
            );
            app.shutdown(Duration::from_secs(1)).await;
        })
        .await;
}

#[derive(Clone, Debug)]
struct ProviderPlan {
    instance: &'static str,
    package: &'static str,
}

impl ProviderPlan {
    const fn new(instance: &'static str, package: &'static str) -> Self {
        Self { instance, package }
    }
}

fn project(providers: &[ProviderPlan]) -> ProjectFile {
    let mut project = ProjectFile::default();
    project.contracts_mut().push(ContractInput::new(
        CAPABILITY_ID,
        DESCRIPTOR_VERSION,
        "crates/lenso-capability-http-endpoint/capability.json",
        "crates/lenso-capability-http-endpoint/src/generated.rs",
        "crates/lenso-capability-http-endpoint/generated/bindings.ts",
    ));
    for package in providers
        .iter()
        .map(|provider| provider.package)
        .chain([PACKAGE_ID])
    {
        project.packages_mut().insert(
            package.to_owned(),
            PackageInput::new(package, PackageSource::Cargo, "0.1.0")
                .with_package_name("lenso-web-ingress")
                .with_manifest("crates/lenso-web-ingress/Cargo.toml")
                .with_lockfile("Cargo.lock"),
        );
    }
    let composition = project.composition_mut();
    for provider in providers {
        composition.add_module(
            Module::new(provider.instance, provider.package).with_capability(
                CapabilityEndpoint::request(
                    CAPABILITY_ID,
                    DESCRIPTOR_VERSION,
                    [DESCRIBE_OPERATION, HANDLE_OPERATION],
                ),
            ),
        );
    }
    composition.add_module(Module::new("web-ingress", PACKAGE_ID).with_requirement(
        CapabilityRequirement::many(CAPABILITY_ID, DESCRIPTOR_VERSION),
    ));
    for provider in providers {
        composition.add_binding(Binding::new(
            "web-ingress",
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            provider.instance,
        ));
    }
    project
}

async fn start<const N: usize>(
    project: ProjectFile,
    ingress: &WebIngressFactory,
    endpoints: [FixtureEndpointFactory; N],
) -> Result<NativeApp, RuntimeFailure> {
    let mut registry = lenso_native_adapter::NativeModuleRegistry::new();
    for endpoint in endpoints {
        registry = registry.with_factory(endpoint);
    }
    registry = registry.with_factory(ingress.clone());
    let plan = project
        .resolve(&workspace_root(), &ResolutionOptions::default())
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: error.to_string(),
        })?;
    Kernel::start_native(plan.plan().clone(), TokioDriver::new(), registry).await
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[derive(Clone, Debug)]
struct FixtureEndpointFactory {
    package_id: &'static str,
    routes: Rc<Vec<DescribeResponseRoutesItem>>,
    observed: Rc<RefCell<Option<HandleRequest>>>,
    active_calls: Rc<Cell<usize>>,
    max_active_calls: Rc<Cell<usize>>,
}

impl FixtureEndpointFactory {
    fn new<const N: usize>(
        package_id: &'static str,
        routes: [(&'static str, &'static str, &'static str); N],
    ) -> Self {
        Self {
            package_id,
            routes: Rc::new(
                routes
                    .into_iter()
                    .map(|(route_id, method, path)| DescribeResponseRoutesItem {
                        method: method.to_owned(),
                        path: path.to_owned(),
                        route_id: route_id.to_owned(),
                    })
                    .collect(),
            ),
            observed: Rc::new(RefCell::new(None)),
            active_calls: Rc::new(Cell::new(0)),
            max_active_calls: Rc::new(Cell::new(0)),
        }
    }

    fn observed(&self) -> Option<HandleRequest> {
        self.observed.borrow().clone()
    }

    fn with_call_tracker(
        mut self,
        active_calls: Rc<Cell<usize>>,
        max_active_calls: Rc<Cell<usize>>,
    ) -> Self {
        self.active_calls = active_calls;
        self.max_active_calls = max_active_calls;
        self
    }

    fn max_active_calls(&self) -> usize {
        self.max_active_calls.get()
    }
}

impl NativeModuleFactory for FixtureEndpointFactory {
    fn package_id(&self) -> &'static str {
        self.package_id
    }

    fn package_version(&self) -> &'static str {
        "0.1.0"
    }

    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        Ok(NativeModuleInstance::new(vec![Rc::new(
            EndpointEndpoint::new(FixtureEndpoint {
                package_id: self.package_id,
                routes: self.routes.clone(),
                observed: self.observed.clone(),
                active_calls: self.active_calls.clone(),
                max_active_calls: self.max_active_calls.clone(),
            }),
        )]))
    }
}

#[derive(Debug)]
struct FixtureEndpoint {
    package_id: &'static str,
    routes: Rc<Vec<DescribeResponseRoutesItem>>,
    observed: Rc<RefCell<Option<HandleRequest>>>,
    active_calls: Rc<Cell<usize>>,
    max_active_calls: Rc<Cell<usize>>,
}

impl EndpointProvider for FixtureEndpoint {
    fn describe(
        &self,
        _context: InvocationContext,
        _request: DescribeRequest,
    ) -> LocalBoxFuture<'static, Result<DescribeResponse, EndpointDescribeInvocationError>> {
        Box::pin(futures::future::ready(Ok(DescribeResponse {
            routes: self.routes.as_ref().clone(),
        })))
    }

    fn handle(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> LocalBoxFuture<'static, Result<HandleResponse, EndpointHandleInvocationError>> {
        assert_ne!(request.route_id, "orders.panic", "fixture endpoint panic");
        self.observed.borrow_mut().replace(request.clone());
        let package_id = self.package_id;
        let active_calls = self.active_calls.clone();
        let max_active_calls = self.max_active_calls.clone();
        Box::pin(async move {
            if request.route_id == "orders.slow" || request.route_id == "status.slow" {
                let active = active_calls.get() + 1;
                active_calls.set(active);
                max_active_calls.set(max_active_calls.get().max(active));
                tokio::time::sleep(Duration::from_millis(25)).await;
                active_calls.set(active_calls.get() - 1);
            }
            if request.route_id == "orders.reject" {
                return Err(EndpointHandleInvocationError::Domain(HandleError::Rejected));
            }
            if request.route_id == "orders.invalid" {
                return Ok(HandleResponse {
                    body: ContractBytes::from(b"invalid status".as_slice()),
                    headers: Vec::new(),
                    status: 1_000,
                });
            }
            if request.route_id == "orders.hop-response" {
                return Ok(HandleResponse {
                    body: ContractBytes::from(b"invalid hop header".as_slice()),
                    headers: vec![HandleResponseHeadersItem {
                        name: "connection".to_owned(),
                        value: "close".to_owned(),
                    }],
                    status: 200,
                });
            }
            let body = format!(
                r#"{{"provider":"{}","route":"{}"}}"#,
                package_id, request.route_id
            );
            Ok(HandleResponse {
                body: body.into_bytes().into(),
                headers: vec![HandleResponseHeadersItem {
                    name: "content-type".to_owned(),
                    value: "application/json; charset=utf-8".to_owned(),
                }],
                status: 200,
            })
        })
    }
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> HttpResponse {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("connect to Ingress");
    let headers = headers
        .iter()
        .fold(String::new(), |mut wire, (name, value)| {
            write!(wire, "{name}: {value}\r\n").expect("writing to a String cannot fail");
            wire
        });
    let connection = if headers
        .lines()
        .any(|line| line.to_ascii_lowercase().starts_with("connection:"))
    {
        String::new()
    } else {
        "Connection: close\r\n".to_owned()
    };
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\n{headers}Content-Length: {}\r\n{connection}\r\n{body}",
        body.len()
    );
    stream.write_all(wire.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let headers = head
        .split("\r\n")
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    HttpResponse {
        status,
        headers,
        body: body.to_owned(),
    }
}
