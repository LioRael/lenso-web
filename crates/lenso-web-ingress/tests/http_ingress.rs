use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fmt::Write as _,
    net::SocketAddr,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};

use futures::future::{LocalBoxFuture, pending};
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, Version, client::conn::http2};
use hyper_util::rt::{TokioExecutor, TokioIo};
use lenso_authoring::{
    Binding, CapabilityEndpoint, CapabilityRequirement, ContractInput, Module, PackageInput,
    PackageSource, ProjectAuthoring, ProjectFile, ResolutionOptions,
};
use lenso_capability_http_endpoint::{
    Bytes as ContractBytes, CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, DescribeRequest,
    DescribeResponse, DescribeResponseRoutesItem, EndpointDescribe, EndpointEndpoint,
    EndpointHandle, EndpointHandleInvocationError, EndpointProvider, HANDLE_OPERATION, HandleError,
    HandleRequest, HandleResponse, HandleResponseHeadersItem, endpoint,
};
use lenso_kernel::{
    InvocationContext, Kernel, NativeApp, NativeRequestFuture, RuntimeFailure, ShutdownOutcome,
};
use lenso_native_adapter::{
    NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance, NativeModuleRegistry,
};
use lenso_runner::TokioDriver;
use lenso_web_ingress::{
    PACKAGE_ID, PACKAGE_VERSION, WebIngressConfig, WebIngressFactory,
    WebIngressListenerCoordinator, WebIngressMiddleware, WebIngressMiddlewareOutcome,
    WebIngressRequest, WebIngressResponse,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::LocalSet,
};

const ORDERS_PACKAGE_ID: &str = "fixture.orders-http";
const STATUS_PACKAGE_ID: &str = "fixture.status-http";
const SDK_PACKAGE_ID: &str = "fixture.sdk-orders-http";

#[derive(Clone, Debug, Default)]
struct GlobalMiddleware {
    events: Rc<RefCell<Vec<String>>>,
}

impl WebIngressMiddleware for GlobalMiddleware {
    fn identity(&self) -> &'static str {
        "fixture.global:v1"
    }

    fn before_request<'a>(
        &'a self,
        request: &'a mut WebIngressRequest,
    ) -> LocalBoxFuture<'a, Result<WebIngressMiddlewareOutcome, RuntimeFailure>> {
        self.events
            .borrow_mut()
            .push(format!("before:{}", request.uri().path()));
        request
            .headers_mut()
            .insert("x-global-before", http::HeaderValue::from_static("present"));
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("Bearer middleware-must-not-smuggle-this"),
        );
        request.headers_mut().insert(
            "x-request-id",
            http::HeaderValue::from_static("middleware-controlled"),
        );
        if request.uri().path() == "/middleware-error" {
            return Box::pin(futures::future::ready(Err(RuntimeFailure::ModuleFailure {
                detail: "fixture middleware failed".to_owned(),
            })));
        }
        let outcome = if request.uri().path() == "/blocked" {
            let mut response = WebIngressResponse::new(bytes::Bytes::from_static(b"blocked"));
            *response.status_mut() = http::StatusCode::IM_A_TEAPOT;
            WebIngressMiddlewareOutcome::Respond(response)
        } else {
            WebIngressMiddlewareOutcome::Continue
        };
        Box::pin(futures::future::ready(Ok(outcome)))
    }

    fn after_response<'a>(
        &'a self,
        request: &'a WebIngressRequest,
        response: &'a mut WebIngressResponse,
    ) -> LocalBoxFuture<'a, Result<(), RuntimeFailure>> {
        self.events
            .borrow_mut()
            .push(format!("after:{}", request.uri().path()));
        response
            .headers_mut()
            .insert("x-global-after", http::HeaderValue::from_static("present"));
        response.headers_mut().insert(
            "x-request-id",
            http::HeaderValue::from_static("middleware-controlled"),
        );
        response.headers_mut().insert(
            "x-content-type-options",
            http::HeaderValue::from_static("middleware-controlled"),
        );
        Box::pin(futures::future::ready(Ok(())))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn global_middleware_wraps_routes_and_can_short_circuit() {
    LocalSet::new()
        .run_until(async {
            let endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [("orders.read", "GET", "/orders/{order_id}")],
            );
            let middleware = GlobalMiddleware::default();
            let events = middleware.events.clone();
            let ingress = WebIngressFactory::default().with_middleware(middleware);
            let app = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &ingress,
                [endpoint.clone()],
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();

            let accepted = request(address, "GET", "/orders/42", &[], "").await;
            assert_eq!(accepted.status, 200);
            assert_eq!(
                accepted.headers.get("x-global-after").map(String::as_str),
                Some("present")
            );
            assert_ne!(
                accepted.headers.get("x-request-id").map(String::as_str),
                Some("middleware-controlled")
            );
            assert_eq!(
                accepted
                    .headers
                    .get("x-content-type-options")
                    .map(String::as_str),
                Some("nosniff")
            );
            let observed = endpoint.observed().unwrap();
            assert!(
                observed.headers.iter().any(|header| {
                    header.name == "x-global-before" && header.value == "present"
                })
            );
            assert!(
                observed.headers.iter().all(|header| {
                    header.name != "authorization" && header.name != "x-request-id"
                }),
                "middleware must not reintroduce Ingress-owned evidence"
            );
            assert_ne!(observed.request_id, "middleware-controlled");

            let blocked = request(address, "GET", "/blocked", &[], "").await;
            assert_eq!(blocked.status, 418);
            assert_eq!(blocked.body, "blocked");
            assert_eq!(
                blocked.headers.get("x-global-after").map(String::as_str),
                Some("present")
            );

            let failed = request(address, "GET", "/middleware-error", &[], "").await;
            assert_error(&failed, 503, "endpoint_unavailable");
            assert_eq!(
                *events.borrow(),
                [
                    "before:/orders/42",
                    "after:/orders/42",
                    "before:/blocked",
                    "after:/blocked",
                    "before:/middleware-error",
                ]
            );

            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn equivalent_replicas_share_one_listener_and_receive_connections() {
    LocalSet::new()
        .run_until(async {
            let coordinator = WebIngressListenerCoordinator::bind(WebIngressConfig::default(), 2)
                .await
                .expect("coordinator should bind once");
            let first_ingress = WebIngressFactory::replicated(&coordinator).unwrap();
            let second_ingress = WebIngressFactory::replicated(&coordinator).unwrap();
            let first_endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [("orders.read", "GET", "/orders/{order_id}")],
            );
            let second_endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [("orders.read", "GET", "/orders/{order_id}")],
            );
            let first = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &first_ingress,
                [first_endpoint.clone()],
            )
            .await
            .unwrap();
            let second = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &second_ingress,
                [second_endpoint.clone()],
            )
            .await
            .unwrap();

            let address = coordinator.local_address();
            let first_response = request(address, "GET", "/orders/1", &[], "").await;
            let second_response = request(address, "GET", "/orders/2", &[], "").await;
            assert_eq!(first_response.status, 200);
            assert_eq!(second_response.status, 200);
            assert_ne!(
                first_response.headers.get("x-request-id"),
                second_response.headers.get("x-request-id")
            );
            assert!(first_endpoint.observed().is_some());
            assert!(second_endpoint.observed().is_some());

            first.shutdown(Duration::from_secs(1)).await;
            second.shutdown(Duration::from_secs(1)).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn ingress_accepts_http2_prior_knowledge() {
    LocalSet::new()
        .run_until(async {
            let endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [("orders.read", "GET", "/orders/{order_id}")],
            );
            let ingress = WebIngressFactory::default();
            let app = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &ingress,
                [endpoint],
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();
            let stream = TcpStream::connect(address).await.unwrap();
            let (mut sender, connection) =
                http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                    .await
                    .expect("HTTP/2 handshake should succeed");
            tokio::task::spawn_local(async move {
                connection
                    .await
                    .expect("HTTP/2 connection should stay valid");
            });
            let request = Request::builder()
                .version(Version::HTTP_2)
                .method("GET")
                .uri(format!("http://{address}/orders/42"))
                .body(Full::new(bytes::Bytes::new()))
                .unwrap();
            let response = sender.send_request(request).await.unwrap();
            assert_eq!(response.status(), 200);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(body.starts_with(br#"{"provider""#));
            app.shutdown(Duration::from_secs(1)).await;
        })
        .await;
}

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
            let manifest = ingress
                .route_manifest()
                .expect("activation should publish a canonical route manifest");
            assert_eq!(manifest.routes().len(), 5);
            assert!(manifest.routes().iter().any(|route| {
                route.method == "GET"
                    && route.path == "/orders/{order_id}"
                    && route.route_id == "orders.read"
            }));

            let orders_response = request(
                address,
                "GET",
                "/orders/42?include=items",
                &[
                    ("Authorization", "Bearer good-token"),
                    ("X-Tenant", "acme"),
                    ("X-Request-Id", "untrusted-client-id"),
                ],
                "",
            )
            .await;
            assert_eq!(orders_response.status, 200);
            assert_eq!(
                orders_response.body,
                r#"{"provider":"fixture.orders-http","route":"orders.read"}"#
            );
            let response_request_id = orders_response
                .headers
                .get("x-request-id")
                .expect("Ingress should return its request id");
            assert!(response_request_id.starts_with("lenso-"));
            assert_ne!(response_request_id, "untrusted-client-id");
            assert_eq!(
                orders_response
                    .headers
                    .get("x-content-type-options")
                    .map(String::as_str),
                Some("nosniff")
            );
            let observed = orders.observed().expect("orders provider saw request");
            assert_eq!(&observed.request_id, response_request_id);
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

            assert_error(
                &request(address, "GET", "/panic", &[], "").await,
                503,
                "endpoint_unavailable",
            );

            let health = request(address, "GET", "/health", &[], "").await;
            assert_eq!(health.status, 200);
            assert!(health.body.contains(STATUS_PACKAGE_ID));
            assert!(status.observed().is_some());

            let method_not_allowed = request(address, "POST", "/health", &[], "").await;
            assert_error(&method_not_allowed, 405, "method_not_allowed");
            assert_eq!(
                method_not_allowed.headers.get("allow").map(String::as_str),
                Some("GET")
            );
            assert_error(
                &request(address, "GET", "/missing", &[], "").await,
                404,
                "not_found",
            );
            app.shutdown(Duration::from_secs(1)).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_authored_endpoint_routes_through_the_real_ingress() {
    LocalSet::new()
        .run_until(async {
            let endpoint = SdkEndpointFactory::default();
            let ingress = WebIngressFactory::default();
            let app = start_with_registry(
                project(&[ProviderPlan::new("sdk-orders-http", SDK_PACKAGE_ID)]),
                &ingress,
                NativeModuleRegistry::new().with_factory(endpoint.clone()),
            )
            .await
            .expect("SDK-authored Endpoint should compose with Web Ingress");
            let response = request(
                ingress.local_address().unwrap(),
                "GET",
                "/sdk/orders/order-42",
                &[("Authorization", "Bearer sdk-token")],
                "",
            )
            .await;

            assert_eq!(response.status, 200);
            assert_eq!(response.body, r#"{"id":"order-42"}"#);
            let observed = endpoint.observed().expect("SDK handler should run");
            assert_eq!(observed.route_id, "sdk.orders.read");
            assert_eq!(observed.path_parameters[0].value, "order-42");
            assert_eq!(observed.credential.unwrap().value, "sdk-token");
            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
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
            let config = WebIngressConfig::default()
                .with_max_concurrent_requests(1)
                .unwrap();
            let ingress = WebIngressFactory::default();
            let app = start(
                project_with_config(
                    &[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)],
                    &config,
                ),
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
            let config = WebIngressConfig::default()
                .with_request_limits(32, 512)
                .unwrap();
            let ingress = WebIngressFactory::default();
            let app = start(
                project_with_config(
                    &[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)],
                    &config,
                ),
                &ingress,
                [endpoint],
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();

            let too_large = request(address, "POST", "/orders/42", &[], &"a".repeat(33)).await;
            assert_error(&too_large, 413, "payload_too_large");
            assert_error(
                &request(
                    address,
                    "GET",
                    "/orders/42",
                    &[
                        ("Authorization", "Bearer first"),
                        ("Authorization", "Bearer second"),
                    ],
                    "",
                )
                .await,
                400,
                "bad_request",
            );
            assert_error(
                &request(address, "GET", "/reject", &[], "").await,
                502,
                "endpoint_rejected",
            );
            assert_error(
                &request(address, "GET", "/invalid", &[], "").await,
                502,
                "invalid_endpoint_response",
            );
            assert_error(
                &request(address, "GET", "/hop-response", &[], "").await,
                502,
                "invalid_endpoint_response",
            );
            app.shutdown(Duration::from_secs(1)).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn plan_configuration_head_limit_and_endpoint_deadline_are_enforced() {
    LocalSet::new()
        .run_until(async {
            let endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [
                    ("orders.read", "GET", "/orders/{order_id}"),
                    ("orders.timeout", "GET", "/timeout"),
                ],
            );
            let config = WebIngressConfig::default()
                .with_request_limits(1024, 256)
                .unwrap()
                .with_request_timeout(Duration::from_millis(20))
                .unwrap();
            let ingress = WebIngressFactory::default();
            let app = start(
                project_with_config(
                    &[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)],
                    &config,
                ),
                &ingress,
                [endpoint],
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();

            let oversized_head = request(
                address,
                "GET",
                "/orders/42",
                &[("X-Fill", &"x".repeat(300))],
                "",
            )
            .await;
            assert_error(&oversized_head, 431, "request_header_fields_too_large");
            assert_error(
                &request(address, "GET", "/timeout", &[], "").await,
                504,
                "endpoint_timeout",
            );
            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_plan_configuration_fails_before_readiness() {
    LocalSet::new()
        .run_until(async {
            let endpoint = FixtureEndpointFactory::new(
                ORDERS_PACKAGE_ID,
                [("orders.read", "GET", "/orders/{order_id}")],
            );
            let ingress = WebIngressFactory::default();
            let error = start(
                project_with_configuration(
                    &[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)],
                    serde_json::json!({"max_concurrent_requests": 0}),
                ),
                &ingress,
                [endpoint],
            )
            .await
            .expect_err("invalid Ingress configuration must fail App preparation");
            assert!(
                matches!(error, RuntimeFailure::InvalidResolvedPlan { .. }),
                "unexpected failure: {error:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn client_disconnect_cancels_the_endpoint_invocation() {
    LocalSet::new()
        .run_until(async {
            let endpoint =
                FixtureEndpointFactory::new(ORDERS_PACKAGE_ID, [("orders.never", "GET", "/never")]);
            let ingress = WebIngressFactory::default();
            let app = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &ingress,
                [endpoint.clone()],
            )
            .await
            .unwrap();
            let stream = begin_request(ingress.local_address().unwrap(), "/never").await;
            wait_for(|| endpoint.blocked_started()).await;
            drop(stream);
            wait_for(|| endpoint.blocked_dropped()).await;

            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_stops_accepting_and_cancels_in_flight_endpoint_work() {
    LocalSet::new()
        .run_until(async {
            let endpoint =
                FixtureEndpointFactory::new(ORDERS_PACKAGE_ID, [("orders.never", "GET", "/never")]);
            let ingress = WebIngressFactory::default();
            let app = start(
                project(&[ProviderPlan::new("orders-http", ORDERS_PACKAGE_ID)]),
                &ingress,
                [endpoint.clone()],
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();
            let stream = begin_request(address, "/never").await;
            wait_for(|| endpoint.blocked_started()).await;

            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
            assert!(endpoint.blocked_dropped());
            drop(stream);
            assert!(TcpStream::connect(address).await.is_err());
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
    project_with_optional_configuration(providers, None)
}

fn project_with_config(providers: &[ProviderPlan], config: &WebIngressConfig) -> ProjectFile {
    project_with_configuration(providers, serde_json::to_value(config).unwrap())
}

fn project_with_configuration(
    providers: &[ProviderPlan],
    configuration: serde_json::Value,
) -> ProjectFile {
    project_with_optional_configuration(providers, Some(configuration))
}

fn project_with_optional_configuration(
    providers: &[ProviderPlan],
    configuration: Option<serde_json::Value>,
) -> ProjectFile {
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
            PackageInput::new(package, PackageSource::Cargo, PACKAGE_VERSION)
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
    let ingress = Module::new("web-ingress", PACKAGE_ID).with_requirement(
        CapabilityRequirement::many(CAPABILITY_ID, DESCRIPTOR_VERSION),
    );
    composition.add_module(if let Some(configuration) = configuration {
        ingress
            .with_configuration_schema("crates/lenso-web-ingress/config.schema.json")
            .with_configuration(configuration)
    } else {
        ingress
    });
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
    let mut registry = NativeModuleRegistry::new();
    for endpoint in endpoints {
        registry = registry.with_factory(endpoint);
    }
    start_with_registry(project, ingress, registry).await
}

async fn start_with_registry(
    project: ProjectFile,
    ingress: &WebIngressFactory,
    registry: NativeModuleRegistry,
) -> Result<NativeApp, RuntimeFailure> {
    let registry = registry.with_factory(ingress.clone());
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

#[derive(Clone, Debug, Default)]
struct SdkEndpointFactory {
    endpoint: SdkOrdersEndpoint,
}

impl SdkEndpointFactory {
    fn observed(&self) -> Option<HandleRequest> {
        self.endpoint.observed.borrow().clone()
    }
}

impl NativeModuleFactory for SdkEndpointFactory {
    fn package_id(&self) -> &'static str {
        SDK_PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }

    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        Ok(NativeModuleInstance::new(vec![Rc::new(
            EndpointEndpoint::new(self.endpoint.clone()),
        )]))
    }
}

#[derive(Clone, Debug, Default)]
struct SdkOrdersEndpoint {
    observed: Rc<RefCell<Option<HandleRequest>>>,
}

#[endpoint]
impl SdkOrdersEndpoint {
    #[get("sdk.orders.read", "/sdk/orders/{order_id}")]
    async fn read(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        let order_id = request
            .path_parameters
            .iter()
            .find(|parameter| parameter.name == "order_id")
            .map_or("missing", |parameter| parameter.value.as_str())
            .to_owned();
        self.observed.borrow_mut().replace(request);
        Ok(HandleResponse {
            body: format!(r#"{{"id":"{order_id}"}}"#).into_bytes().into(),
            headers: vec![HandleResponseHeadersItem {
                name: "content-type".to_owned(),
                value: "application/json; charset=utf-8".to_owned(),
            }],
            status: 200,
        })
    }
}

#[derive(Clone, Debug)]
struct FixtureEndpointFactory {
    package_id: &'static str,
    routes: Rc<Vec<DescribeResponseRoutesItem>>,
    observed: Rc<RefCell<Option<HandleRequest>>>,
    active_calls: Rc<Cell<usize>>,
    max_active_calls: Rc<Cell<usize>>,
    blocked_started: Rc<Cell<bool>>,
    blocked_dropped: Rc<Cell<bool>>,
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
                        openapi: None,
                        path: path.to_owned(),
                        route_id: route_id.to_owned(),
                    })
                    .collect(),
            ),
            observed: Rc::new(RefCell::new(None)),
            active_calls: Rc::new(Cell::new(0)),
            max_active_calls: Rc::new(Cell::new(0)),
            blocked_started: Rc::new(Cell::new(false)),
            blocked_dropped: Rc::new(Cell::new(false)),
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

    fn blocked_started(&self) -> bool {
        self.blocked_started.get()
    }

    fn blocked_dropped(&self) -> bool {
        self.blocked_dropped.get()
    }
}

impl NativeModuleFactory for FixtureEndpointFactory {
    fn package_id(&self) -> &'static str {
        self.package_id
    }

    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
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
                blocked_started: self.blocked_started.clone(),
                blocked_dropped: self.blocked_dropped.clone(),
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
    blocked_started: Rc<Cell<bool>>,
    blocked_dropped: Rc<Cell<bool>>,
}

struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

impl EndpointProvider for FixtureEndpoint {
    fn describe(
        &self,
        _context: InvocationContext,
        _request: DescribeRequest,
    ) -> NativeRequestFuture<EndpointDescribe> {
        Box::pin(futures::future::ready(Ok(Ok(DescribeResponse {
            routes: self.routes.as_ref().clone(),
        }))))
    }

    fn handle(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> NativeRequestFuture<EndpointHandle> {
        assert_ne!(request.route_id, "orders.panic", "fixture endpoint panic");
        self.observed.borrow_mut().replace(request.clone());
        let package_id = self.package_id;
        let active_calls = self.active_calls.clone();
        let max_active_calls = self.max_active_calls.clone();
        let blocked_started = self.blocked_started.clone();
        let blocked_dropped = self.blocked_dropped.clone();
        Box::pin(async move {
            if request.route_id == "orders.never" {
                let _drop_flag = DropFlag(blocked_dropped);
                blocked_started.set(true);
                return pending::<Result<Result<HandleResponse, HandleError>, RuntimeFailure>>()
                    .await;
            }
            if request.route_id == "orders.timeout" {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
            if request.route_id == "orders.slow" || request.route_id == "status.slow" {
                let active = active_calls.get() + 1;
                active_calls.set(active);
                max_active_calls.set(max_active_calls.get().max(active));
                tokio::time::sleep(Duration::from_millis(25)).await;
                active_calls.set(active_calls.get() - 1);
            }
            if request.route_id == "orders.reject" {
                return Ok(Err(HandleError::Rejected));
            }
            if request.route_id == "orders.invalid" {
                return Ok(Ok(HandleResponse {
                    body: ContractBytes::from(b"invalid status".as_slice()),
                    headers: Vec::new(),
                    status: 1_000,
                }));
            }
            if request.route_id == "orders.hop-response" {
                return Ok(Ok(HandleResponse {
                    body: ContractBytes::from(b"invalid hop header".as_slice()),
                    headers: vec![HandleResponseHeadersItem {
                        name: "connection".to_owned(),
                        value: "close".to_owned(),
                    }],
                    status: 200,
                }));
            }
            let body = format!(
                r#"{{"provider":"{}","route":"{}"}}"#,
                package_id, request.route_id
            );
            Ok(Ok(HandleResponse {
                body: body.into_bytes().into(),
                headers: vec![HandleResponseHeadersItem {
                    name: "content-type".to_owned(),
                    value: "application/json; charset=utf-8".to_owned(),
                }],
                status: 200,
            }))
        })
    }
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

fn assert_error(response: &HttpResponse, status: u16, code: &str) {
    assert_eq!(response.status, status);
    assert_eq!(
        response.headers.get("content-type").map(String::as_str),
        Some("application/json; charset=utf-8")
    );
    assert_eq!(response.body, format!(r#"{{"error":"{code}"}}"#));
    assert!(
        response
            .headers
            .get("x-request-id")
            .is_some_and(|value| value.starts_with("lenso-"))
    );
}

async fn begin_request(address: SocketAddr, path: &str) -> TcpStream {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("connect to Ingress");
    let wire = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\n\r\n");
    stream.write_all(wire.as_bytes()).await.unwrap();
    stream
}

async fn wait_for(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition should become true");
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
