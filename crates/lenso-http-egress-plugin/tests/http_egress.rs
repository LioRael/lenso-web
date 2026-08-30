use std::time::Duration;

use axum::{Router, body::Bytes, http::StatusCode, routing::get, routing::post};
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan, ResolvedAppPlan,
};
use lenso_capability_http_client::{
    CAPABILITY_ID, Client, DESCRIPTOR_VERSION, SEND_OPERATION, SendError, SendRequest,
    SendRequestHeadersItem,
};
use lenso_http_egress_plugin::{HttpEgressConfig, HttpVersionPolicy, PACKAGE_ID};
use lenso_kernel::{Kernel, RuntimeFailure, ShutdownOutcome};
use lenso_native_adapter::{
    NativePluginFactory, NativePluginFactoryContext, NativePluginInstance, NativePluginRegistry,
};
use lenso_runner::TokioDriver;
use tokio::{net::TcpListener, task::JoinHandle};

const CALLER_PACKAGE_ID: &str = "fixture.http-client";

#[derive(Debug)]
struct CallerFactory;

impl NativePluginFactory for CallerFactory {
    fn package_id(&self) -> &'static str {
        CALLER_PACKAGE_ID
    }

    fn instantiate(
        &self,
        _context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::default())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn composed_client_calls_only_allowed_origin_with_bounded_protocol_evidence() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (address, upstream) = spawn_upstream().await;
            let origin = format!("http://{address}");
            let config = HttpEgressConfig::new([&origin])
                .unwrap()
                .with_transfer_limits(32, 512, 32, 512)
                .unwrap();
            let app = start(config).await;

            let response = app
                .invoke::<Client>(
                    "caller",
                    SEND_OPERATION,
                    request(
                        "POST",
                        &format!("{origin}/echo?mode=binary"),
                        &[new_header("x-client", "orders")],
                        b"hello",
                    ),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status, 201);
            assert_eq!(response.body.as_slice(), b"hello");
            assert!(
                response
                    .headers
                    .iter()
                    .any(|header| header.name == "x-upstream" && header.value == "echo")
            );
            assert_policy_boundaries(&app, &origin).await;

            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
            upstream.abort();
        })
        .await;
}

async fn assert_policy_boundaries(app: &lenso_kernel::NativeApp, origin: &str) {
    let redirect = app
        .invoke::<Client>(
            "caller",
            SEND_OPERATION,
            request("GET", &format!("{origin}/redirect"), &[], b""),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(redirect.status, 307, "redirects must not be followed");

    let hop_filtered = app
        .invoke::<Client>(
            "caller",
            SEND_OPERATION,
            request("GET", &format!("{origin}/hop-response"), &[], b""),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        hop_filtered
            .headers
            .iter()
            .all(|header| header.name != "connection" && header.name != "x-hop")
    );
    assert!(
        hop_filtered
            .headers
            .iter()
            .any(|header| header.name == "x-end-to-end")
    );

    let denied = app
        .invoke::<Client>(
            "caller",
            SEND_OPERATION,
            request("GET", "http://127.0.0.1:1/private", &[], b""),
        )
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(denied, SendError::DestinationNotAllowed);

    let invalid_host = app
        .invoke::<Client>(
            "caller",
            SEND_OPERATION,
            request(
                "GET",
                &format!("{origin}/echo"),
                &[new_header("host", "other.example")],
                b"",
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(invalid_host, SendError::InvalidRequest);

    let oversized = app
        .invoke::<Client>(
            "caller",
            SEND_OPERATION,
            request("GET", &format!("{origin}/large"), &[], b""),
        )
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(oversized, SendError::ResponseTooLarge);
}

#[tokio::test(flavor = "current_thread")]
async fn total_timeout_is_owned_by_the_egress_instance() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (address, upstream) = spawn_upstream().await;
            let origin = format!("http://{address}");
            let config = HttpEgressConfig::new([&origin])
                .unwrap()
                .with_max_concurrent_requests(1)
                .unwrap()
                .with_timeouts(Duration::from_millis(20), Duration::from_millis(30))
                .unwrap();
            let app = start(config).await;
            let url = format!("{origin}/slow");

            let result = app
                .invoke::<Client>("caller", SEND_OPERATION, request("GET", &url, &[], b""))
                .await;
            assert_eq!(result.unwrap().unwrap_err(), SendError::Timeout);

            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
            upstream.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn composed_client_uses_http2_prior_knowledge_when_required() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (address, upstream) = spawn_http2_upstream().await;
            let origin = format!("http://{address}");
            let config = HttpEgressConfig::new([&origin])
                .unwrap()
                .with_http_version(HttpVersionPolicy::Http2PriorKnowledge);
            let app = start(config).await;

            let response = app
                .invoke::<Client>(
                    "caller",
                    SEND_OPERATION,
                    request("GET", &format!("{origin}/version"), &[], b""),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status, 200);
            assert_eq!(response.body.as_slice(), b"HTTP/2.0");

            assert_eq!(
                app.shutdown(Duration::from_secs(1)).await,
                ShutdownOutcome::Clean
            );
            upstream.abort();
        })
        .await;
}

async fn start(config: HttpEgressConfig) -> lenso_kernel::NativeApp {
    Kernel::start_native(
        plan(&config),
        TokioDriver::new(),
        NativePluginRegistry::new()
            .with_linked_factories()
            .with_factory(CallerFactory),
    )
    .await
    .unwrap()
}

fn plan(config: &HttpEgressConfig) -> ResolvedAppPlan {
    let caller = PluginInstancePlan::new("caller", CALLER_PACKAGE_ID).with_requirement(
        CapabilityRequirementPlan::one(CAPABILITY_ID, DESCRIPTOR_VERSION),
    );
    let egress = PluginInstancePlan::new("http-egress", PACKAGE_ID)
        .with_configuration(serde_json::to_string(config).unwrap())
        .with_capability(CapabilityEndpointPlan::new(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            [SEND_OPERATION],
        ));
    AppComposition::new(
        vec![caller, egress],
        vec![CapabilityBinding::new(
            "caller",
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            "http-egress",
        )],
    )
    .resolve()
    .unwrap()
}

fn request(
    method: &str,
    url: &str,
    headers: &[SendRequestHeadersItem],
    body: &[u8],
) -> SendRequest {
    SendRequest {
        body: body.into(),
        headers: headers.to_vec(),
        method: method.to_owned(),
        url: url.to_owned(),
    }
}

fn new_header(name: &str, value: &str) -> SendRequestHeadersItem {
    SendRequestHeadersItem {
        name: name.to_owned(),
        value: value.to_owned(),
    }
}

async fn spawn_upstream() -> (std::net::SocketAddr, JoinHandle<()>) {
    let app = Router::new()
        .route(
            "/echo",
            post(
                |body: Bytes| async move { (StatusCode::CREATED, [("x-upstream", "echo")], body) },
            ),
        )
        .route(
            "/redirect",
            get(|| async { (StatusCode::TEMPORARY_REDIRECT, [("location", "/echo")]) }),
        )
        .route(
            "/hop-response",
            get(|| async {
                (
                    StatusCode::OK,
                    [
                        ("connection", "close, x-hop"),
                        ("x-hop", "secret"),
                        ("x-end-to-end", "visible"),
                    ],
                )
            }),
        )
        .route("/large", get(|| async { "x".repeat(128) }))
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                "slow"
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (address, task)
}

async fn spawn_http2_upstream() -> (std::net::SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::task::spawn_local(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::task::spawn_local(async move {
                let service = service_fn(|request: Request<Incoming>| async move {
                    Ok::<_, std::convert::Infallible>(Response::new(Full::new(Bytes::from(
                        format!("{:?}", request.version()),
                    ))))
                });
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                    .unwrap();
            });
        }
    });
    (address, task)
}
