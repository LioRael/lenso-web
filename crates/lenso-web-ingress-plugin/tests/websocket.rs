use futures::{SinkExt, StreamExt};
use lenso_kernel::{Kernel, ShutdownOutcome};
use lenso_native_adapter::NativePluginRegistry;
use lenso_runner::TokioDriver;
use lenso_web_duplex_fixture::{DuplexFactory, plan};
use lenso_web_ingress_plugin::{WebIngressConfig, WebIngressFactory, WebSocketConfig};
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpStream, task::LocalSet};
use tokio_tungstenite::{
    client_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
fn request(
    address: SocketAddr,
    path: &str,
    credential: bool,
    protocol: &str,
    origin: Option<&str>,
) -> http::Request<()> {
    let mut request = format!("ws://{address}{path}")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", protocol.parse().unwrap());
    if credential {
        request
            .headers_mut()
            .insert("authorization", "Bearer proof".parse().unwrap());
    }
    if let Some(origin) = origin {
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
    }
    request
}
#[tokio::test(flavor = "current_thread")]
async fn native_websocket_uses_bound_provider_and_preserves_frames_and_close() {
    LocalSet::new()
        .run_until(Box::pin(async {
            let ingress = WebIngressFactory::default();
            let config = WebIngressConfig::default()
                .with_websocket(
                    WebSocketConfig::new(vec!["https://client.invalid".into()]).unwrap(),
                )
                .unwrap();
            let app = Kernel::start_native(
                plan(serde_json::to_string(&config).unwrap()),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(DuplexFactory)
                    .with_factory(ingress.clone()),
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();
            let (mut socket, response) = client_async(
                request(
                    address,
                    "/socket/test",
                    true,
                    "lenso.echo",
                    Some("https://client.invalid"),
                ),
                TcpStream::connect(address).await.unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(response.headers()["sec-websocket-protocol"], "lenso.echo");
            for message in [
                Message::Text("hello 世界".into()),
                Message::Binary(vec![0, 255, 1].into()),
                Message::Text("".into()),
                Message::Binary(Vec::new().into()),
            ] {
                socket.send(message.clone()).await.unwrap();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(2), socket.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                    message
                );
            }
            socket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "done".into(),
                })))
                .await
                .unwrap();
            let close = tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(matches!(
                close,
                Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    ..
                }))
            ));
            drop(socket);
            assert_eq!(
                app.shutdown(Duration::from_secs(2)).await,
                ShutdownOutcome::Clean
            );
        }))
        .await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_websocket_rejects_auth_origin_and_protocol_before_upgrade() {
    LocalSet::new()
        .run_until(Box::pin(async {
            let ingress = WebIngressFactory::default();
            let config = WebIngressConfig::default()
                .with_websocket(WebSocketConfig::new(vec![]).unwrap())
                .unwrap();
            let app = Kernel::start_native(
                plan(serde_json::to_string(&config).unwrap()),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(DuplexFactory)
                    .with_factory(ingress.clone()),
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();
            for (path, credential, protocol, origin, status) in [
                ("/socket/test", false, "lenso.echo", None, 401),
                ("/socket/denied", true, "lenso.echo", None, 403),
                ("/socket/test", true, "other", None, 400),
                (
                    "/socket/test",
                    true,
                    "lenso.echo",
                    Some("https://untrusted.invalid"),
                    400,
                ),
            ] {
                let error = client_async(
                    request(address, path, credential, protocol, origin),
                    TcpStream::connect(address).await.unwrap(),
                )
                .await
                .unwrap_err();
                match error {
                    tokio_tungstenite::tungstenite::Error::Http(response) => {
                        assert_eq!(response.status().as_u16(), status);
                    }
                    error => panic!("unexpected transport result: {error}"),
                }
            }
            assert_eq!(
                app.shutdown(Duration::from_secs(2)).await,
                ShutdownOutcome::Clean
            );
        }))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn event_stream_keeps_concurrency_permit_until_released() {
    use lenso_kernel::CancellationToken;
    use lenso_web_ingress_plugin::{WebIngressEventBody, WebIngressEventFactory};
    LocalSet::new()
        .run_until(Box::pin(async {
            let ingress = WebIngressEventFactory::new();
            let config = WebIngressConfig::default()
                .with_max_concurrent_requests(1)
                .unwrap()
                .with_websocket(WebSocketConfig::new(vec![]).unwrap())
                .unwrap();
            let app = Kernel::start_native(
                plan(serde_json::to_string(&config).unwrap()),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(DuplexFactory)
                    .with_factory(ingress.clone()),
            )
            .await
            .unwrap();
            let request = || {
                http::Request::builder()
                    .uri("/stream?hold")
                    .body(bytes::Bytes::new())
                    .unwrap()
            };
            let first = ingress
                .handle_response(request(), CancellationToken::new())
                .await
                .unwrap();
            let WebIngressEventBody::Streaming(first) = first.into_body() else {
                panic!("expected stream")
            };
            let next = ingress.handle_response(request(), CancellationToken::new());
            tokio::pin!(next);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut next)
                    .await
                    .is_err()
            );
            first.cancel();
            drop(first);
            let second = tokio::time::timeout(Duration::from_secs(1), &mut next)
                .await
                .unwrap()
                .unwrap();
            drop(second);
            assert_eq!(
                app.shutdown(Duration::from_secs(2)).await,
                ShutdownOutcome::Clean
            );
        }))
        .await;
}
