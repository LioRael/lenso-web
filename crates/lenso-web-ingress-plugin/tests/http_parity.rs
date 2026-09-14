use bytes::Bytes;
use http::{HeaderName, HeaderValue, Request, Response};
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::TokioIo;
use lenso_kernel::{CancellationToken, Kernel};
use lenso_native_adapter::NativePluginRegistry;
use lenso_runner::TokioDriver;
use lenso_web_http_parity_fixture::{CORPUS, HttpParityEndpointFactory, plan};
use lenso_web_ingress_plugin::{
    SessionCookieConfig, WebIngressConfig, WebIngressEventFactory, WebIngressFactory,
};
use serde_json::Value;
use tokio::{net::TcpStream, task::LocalSet};

fn config() -> WebIngressConfig {
    WebIngressConfig::default()
        .with_session_cookie(
            SessionCookieConfig::new("__Host-session", "__Host-csrf", "x-csrf-token").unwrap(),
        )
        .unwrap()
}
fn input(vector: &Value) -> Request<Bytes> {
    let body = vector["body"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| u8::try_from(v.as_u64().unwrap()).unwrap())
        .collect::<Vec<_>>();
    let mut request = Request::builder()
        .method(vector["method"].as_str().unwrap())
        .uri(vector["uri"].as_str().unwrap())
        .body(Bytes::from(body))
        .unwrap();
    for header in vector["headers"].as_array().unwrap() {
        request.headers_mut().append(
            HeaderName::from_bytes(header[0].as_str().unwrap().as_bytes()).unwrap(),
            HeaderValue::from_str(header[1].as_str().unwrap()).unwrap(),
        );
    }
    request
}
async fn native(address: std::net::SocketAddr, request: Request<Bytes>) -> Response<Bytes> {
    let stream = TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    tokio::task::spawn_local(async move {
        connection.await.unwrap();
    });
    let response = sender.send_request(request.map(Full::new)).await.unwrap();
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, body.collect().await.unwrap().to_bytes())
}
fn assert_vector(vector: &Value, response: &Response<Bytes>) {
    assert_eq!(
        response.status().as_u16(),
        u16::try_from(vector["status"].as_u64().unwrap()).unwrap(),
        "{}",
        vector["name"]
    );
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    assert_ne!(response.headers()["x-request-id"], "untrusted");
    if let Some(body) = vector.get("response_body") {
        assert_eq!(
            response.body().as_ref(),
            body.as_array()
                .unwrap()
                .iter()
                .map(|v| u8::try_from(v.as_u64().unwrap()).unwrap())
                .collect::<Vec<_>>(),
            "{}",
            vector["name"]
        );
    }
    if let Some(count) = vector.get("set_cookie_count") {
        assert_eq!(
            response.headers().get_all("set-cookie").iter().count(),
            usize::try_from(count.as_u64().unwrap()).unwrap()
        );
    }
    if vector.get("route").is_some()
        || vector.get("query").is_some()
        || vector.get("credential").is_some()
    {
        let body: Value = serde_json::from_slice(response.body()).unwrap();
        for (source, dest) in [
            ("route", "route_id"),
            ("path", "path"),
            ("query", "query"),
            ("credential", "credential"),
        ] {
            if let Some(expected) = vector.get(source) {
                assert_eq!(&body[dest], expected, "{}: {dest}", vector["name"]);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn same_corpus_runs_through_native_and_event_plugin_registration() {
    LocalSet::new()
        .run_until(Box::pin(async {
            let config = config();
            let configuration = serde_json::to_string(&config).unwrap();
            let vectors: Vec<Value> = serde_json::from_str(CORPUS).unwrap();
            for vector in vectors {
                let socket = WebIngressFactory::new();
                let event = WebIngressEventFactory::new();
                assert!(
                    event
                        .handle(Request::new(Bytes::new()), CancellationToken::new())
                        .await
                        .is_err()
                );
                let native_app = Kernel::start_native(
                    plan(configuration.clone()),
                    TokioDriver::new(),
                    NativePluginRegistry::new()
                        .with_factory(HttpParityEndpointFactory)
                        .with_factory(socket.clone()),
                )
                .await
                .unwrap();
                let event_app = Kernel::start_native(
                    plan(configuration.clone()),
                    TokioDriver::new(),
                    NativePluginRegistry::new()
                        .with_factory(HttpParityEndpointFactory)
                        .with_factory(event.clone()),
                )
                .await
                .unwrap();
                assert_eq!(socket.route_manifest(), event.route_manifest());
                let actual = event
                    .handle(input(&vector), CancellationToken::new())
                    .await
                    .unwrap();
                let reference = native(socket.local_address().unwrap(), input(&vector)).await;
                assert_vector(&vector, &actual);
                assert_vector(&vector, &reference);
                assert_eq!(actual.status(), reference.status(), "{}", vector["name"]);
                assert_eq!(actual.body(), reference.body(), "{}", vector["name"]);
                for header in [
                    "set-cookie",
                    "allow",
                    "content-type",
                    "x-request-id",
                    "x-content-type-options",
                ] {
                    assert_eq!(
                        actual.headers().get_all(header).iter().collect::<Vec<_>>(),
                        reference
                            .headers()
                            .get_all(header)
                            .iter()
                            .collect::<Vec<_>>(),
                        "{}: {header}",
                        vector["name"]
                    );
                }
                assert!(matches!(
                    native_app.shutdown(std::time::Duration::from_secs(1)).await,
                    lenso_kernel::ShutdownOutcome::Clean
                ));
                assert!(matches!(
                    event_app.shutdown(std::time::Duration::from_secs(1)).await,
                    lenso_kernel::ShutdownOutcome::Clean
                ));
                assert!(
                    event
                        .handle(Request::new(Bytes::new()), CancellationToken::new())
                        .await
                        .is_err()
                );
            }
        }))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn event_limits_deadlines_and_cancellation_keep_other_requests_healthy() {
    LocalSet::new()
        .run_until(Box::pin(async {
            let config = config()
                .with_request_limits(8, 1024)
                .unwrap()
                .with_request_timeout(std::time::Duration::from_millis(25))
                .unwrap();
            let event = WebIngressEventFactory::new();
            let app = Kernel::start_native(
                plan(serde_json::to_string(&config).unwrap()),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(HttpParityEndpointFactory)
                    .with_factory(event.clone()),
            )
            .await
            .unwrap();
            let oversized = Request::builder()
                .method("POST")
                .uri("/bytes")
                .body(Bytes::from_static(b"0123456789"))
                .unwrap();
            assert_eq!(
                event
                    .handle(oversized, CancellationToken::new())
                    .await
                    .unwrap()
                    .status(),
                413
            );
            let misleading = Request::builder()
                .method("POST")
                .uri("/bytes")
                .header("content-length", "9")
                .body(Bytes::new())
                .unwrap();
            assert_eq!(
                event
                    .handle(misleading, CancellationToken::new())
                    .await
                    .unwrap()
                    .status(),
                413
            );
            let blocked = || {
                Request::builder()
                    .uri("/blocked")
                    .body(Bytes::new())
                    .unwrap()
            };
            assert_eq!(
                event
                    .handle(blocked(), CancellationToken::new())
                    .await
                    .unwrap()
                    .status(),
                504
            );
            let identity_request = |name: &str| Request::builder().uri("/echo/a").header("authorization", format!("Bearer {name}")).body(Bytes::new()).unwrap();
            let (alice, bob) = futures::join!(
                event.handle(identity_request("alice"), CancellationToken::new()),
                event.handle(identity_request("bob"), CancellationToken::new())
            );
            assert_eq!(serde_json::from_slice::<Value>(alice.unwrap().body()).unwrap()["credential"]["value"], "alice");
            assert_eq!(serde_json::from_slice::<Value>(bob.unwrap().body()).unwrap()["credential"]["value"], "bob");
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            let (cancel_response, healthy_response) = futures::join!(
                event.handle(blocked(), cancelled),
                event.handle(
                    Request::builder()
                        .uri("/method")
                        .body(Bytes::new())
                        .unwrap(),
                    CancellationToken::new()
                )
            );
            assert_eq!(cancel_response.unwrap().status(), 503);
            assert_eq!(healthy_response.unwrap().status(), 200);
            assert!(matches!(
                app.shutdown(std::time::Duration::from_secs(1)).await,
                lenso_kernel::ShutdownOutcome::Clean
            ));
        }))
        .await;
}
