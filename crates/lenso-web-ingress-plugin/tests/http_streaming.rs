use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
    time::Duration,
};

use futures::future::LocalBoxFuture;
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan, ResolvedAppPlan,
};
use lenso_capability_http_stream_endpoint as stream_endpoint;
use lenso_kernel::{
    InvocationContext, Kernel, NativeRequestEndpoint, NativeRequestFuture, NativeStreamEndpoint,
    NativeStreamItem, NativeStreamSession, NoopPluginLifecycle, RuntimeFailure, ShutdownOutcome,
};
use lenso_native_adapter::{
    NativePluginFactory, NativePluginFactoryContext, NativePluginInstance, NativePluginRegistry,
};
use lenso_runner::TokioDriver;
use lenso_web_ingress_plugin::{PACKAGE_ID, WebIngressFactory};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    task::LocalSet,
};

const STREAM_PACKAGE_ID: &str = "fixture.streaming-http";

#[tokio::test(flavor = "current_thread")]
async fn streams_response_chunks_without_buffering_the_endpoint() {
    LocalSet::new()
        .run_until(Box::pin(async {
            let ingress = WebIngressFactory::default();
            let app = Kernel::start_native(
                plan(),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(StreamingEndpointFactory { malformed: false })
                    .with_factory(ingress.clone()),
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();
            let mut connection = TcpStream::connect(address).await.unwrap();
            connection
                .write_all(b"GET /events HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut response = String::new();
            connection.read_to_string(&mut response).await.unwrap();

            assert!(response.starts_with("HTTP/1.1 200"));
            assert!(response.contains("content-type: text/event-stream"));
            assert!(response.contains("5\r\nfirst\r\n6\r\nsecond\r\n"));
            assert!(response.ends_with("0\r\n\r\n"));
            assert_eq!(
                app.shutdown(Duration::from_secs(2)).await,
                ShutdownOutcome::Clean
            );
        }))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_a_stream_that_does_not_start_with_a_response_head() {
    LocalSet::new()
        .run_until(Box::pin(async {
            let ingress = WebIngressFactory::default();
            let app = Kernel::start_native(
                plan(),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(StreamingEndpointFactory { malformed: true })
                    .with_factory(ingress.clone()),
            )
            .await
            .unwrap();
            let address = ingress.local_address().unwrap();
            let mut connection = TcpStream::connect(address).await.unwrap();
            connection
                .write_all(b"GET /events HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut response = String::new();
            connection.read_to_string(&mut response).await.unwrap();

            assert!(response.starts_with("HTTP/1.1 502"));
            assert!(response.contains(r#"{"error":"endpoint_rejected"}"#));
            assert_eq!(
                app.shutdown(Duration::from_secs(2)).await,
                ShutdownOutcome::Clean
            );
        }))
        .await;
}

fn plan() -> ResolvedAppPlan {
    let endpoint = PluginInstancePlan::new("streaming-http", STREAM_PACKAGE_ID).with_capability(
        CapabilityEndpointPlan::new(
            stream_endpoint::CAPABILITY_ID,
            stream_endpoint::DESCRIPTOR_VERSION,
            [
                stream_endpoint::DESCRIBE_OPERATION,
                stream_endpoint::HANDLE_OPERATION,
            ],
        )
        .with_stream_operation(stream_endpoint::HANDLE_OPERATION),
    );
    let ingress = PluginInstancePlan::new("web-ingress", PACKAGE_ID).with_requirement(
        CapabilityRequirementPlan::many(
            stream_endpoint::CAPABILITY_ID,
            stream_endpoint::DESCRIPTOR_VERSION,
        ),
    );
    AppComposition::new(
        vec![endpoint, ingress],
        vec![CapabilityBinding::new(
            "web-ingress",
            stream_endpoint::CAPABILITY_ID,
            stream_endpoint::DESCRIPTOR_VERSION,
            "streaming-http",
        )],
    )
    .resolve()
    .unwrap()
}

#[derive(Clone, Copy, Debug)]
struct StreamingEndpointFactory {
    malformed: bool,
}

impl NativePluginFactory for StreamingEndpointFactory {
    fn package_id(&self) -> &'static str {
        STREAM_PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        "0.1.0"
    }

    fn instantiate(
        &self,
        _context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        let endpoint = Rc::new(stream_endpoint::StreamEndpointEndpoint::new(
            StreamingEndpoint {
                malformed: self.malformed,
            },
        ));
        let request: Rc<dyn NativeRequestEndpoint> = endpoint.clone();
        let stream: Rc<dyn NativeStreamEndpoint> = endpoint;
        Ok(NativePluginInstance::with_endpoints(
            vec![request],
            vec![stream],
            NoopPluginLifecycle,
        ))
    }
}

#[derive(Clone, Copy, Debug)]
struct StreamingEndpoint {
    malformed: bool,
}

impl stream_endpoint::StreamEndpointProvider for StreamingEndpoint {
    fn describe_stream(
        &self,
        _context: InvocationContext,
        _request: stream_endpoint::DescribeRequest,
    ) -> NativeRequestFuture<stream_endpoint::StreamEndpointDescribe> {
        Box::pin(futures::future::ready(Ok(Ok(
            stream_endpoint::DescribeResponse {
                routes: vec![stream_endpoint::DescribeResponseRoutesItem {
                    method: "GET".to_owned(),
                    path: "/events".to_owned(),
                    route_id: "events.watch".to_owned(),
                }],
            },
        ))))
    }

    fn handle_stream(
        &self,
        _context: InvocationContext,
        request: stream_endpoint::HandleRequest,
    ) -> LocalBoxFuture<
        'static,
        Result<Box<dyn NativeStreamSession>, stream_endpoint::StreamEndpointHandleInvocationError>,
    > {
        assert_eq!(request.route_id, "events.watch");
        let head = if self.malformed {
            message(chunk("missing-head"))
        } else {
            message(stream_endpoint::HandleResponse {
                body: None,
                headers: Some(vec![stream_endpoint::HandleResponseHeadersItem {
                    name: "content-type".to_owned(),
                    value: "text/event-stream".to_owned(),
                }]),
                kind: stream_endpoint::HandleResponseKind::Head,
                status: Some(200),
            })
        };
        let session = FixtureStream::new([
            head,
            message(chunk("first")),
            if request.query.as_deref() == Some("late-failure") {
                let mut invalid = chunk("second");
                invalid.status = Some(200);
                message(invalid)
            } else {
                message(chunk("second"))
            },
            NativeStreamItem::Terminal(Ok(())),
        ]);
        Box::pin(futures::future::ready(Ok(
            Box::new(session) as Box<dyn NativeStreamSession>
        )))
    }
}

fn chunk(body: &str) -> stream_endpoint::HandleResponse {
    stream_endpoint::HandleResponse {
        body: Some(body.as_bytes().to_vec().into()),
        headers: None,
        kind: stream_endpoint::HandleResponseKind::Chunk,
        status: None,
    }
}

fn message(value: stream_endpoint::HandleResponse) -> NativeStreamItem {
    NativeStreamItem::Message(Box::new(value))
}

#[derive(Debug)]
struct FixtureStream {
    cancelled: Cell<bool>,
    frames: RefCell<VecDeque<NativeStreamItem>>,
}

impl FixtureStream {
    fn new(frames: impl IntoIterator<Item = NativeStreamItem>) -> Self {
        Self {
            cancelled: Cell::new(false),
            frames: RefCell::new(frames.into_iter().collect()),
        }
    }
}

impl NativeStreamSession for FixtureStream {
    fn send(&self, _message: Box<dyn Any>) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(futures::future::ready(Ok(())))
    }

    fn receive(&self) -> LocalBoxFuture<'static, Result<NativeStreamItem, RuntimeFailure>> {
        let frame = self.frames.borrow_mut().pop_front().unwrap();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(2)).await;
            Ok(frame)
        })
    }

    fn close_send(&self) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(futures::future::ready(Ok(())))
    }

    fn cancel(&self) {
        self.cancelled.set(true);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn buffered_event_entrypoint_rejects_implicit_stream_collection() {
    LocalSet::new()
        .run_until(Box::pin(async {
            let ingress = lenso_web_ingress_plugin::WebIngressEventFactory::new();
            let app = Kernel::start_native(
                plan(),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(StreamingEndpointFactory { malformed: false })
                    .with_factory(ingress.clone()),
            )
            .await
            .unwrap();
            let error = ingress
                .handle(
                    http::Request::builder()
                        .uri("/events")
                        .body(bytes::Bytes::new())
                        .unwrap(),
                    lenso_kernel::CancellationToken::new(),
                )
                .await
                .unwrap_err();
            assert!(format!("{error:?}").contains("requires handle_response"));
            assert_eq!(
                app.shutdown(Duration::from_secs(2)).await,
                ShutdownOutcome::Clean
            );
        }))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn event_ingress_uses_the_same_stream_contract_and_clean_terminal() {
    use lenso_web_ingress_plugin::{WebIngressEventBody, WebIngressEventFactory};
    LocalSet::new()
        .run_until(Box::pin(async {
            let ingress = WebIngressEventFactory::new();
            let app = Kernel::start_native(
                plan(),
                TokioDriver::new(),
                NativePluginRegistry::new()
                    .with_factory(StreamingEndpointFactory { malformed: false })
                    .with_factory(ingress.clone()),
            )
            .await
            .unwrap();
            let response = ingress
                .handle_response(
                    http::Request::builder()
                        .uri("/events")
                        .body(bytes::Bytes::new())
                        .unwrap(),
                    lenso_kernel::CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers()["content-type"], "text/event-stream");
            let WebIngressEventBody::Streaming(stream) = response.into_body() else {
                panic!("expected a stream")
            };
            assert_eq!(stream.receive().await.unwrap().unwrap(), "first");
            assert_eq!(stream.receive().await.unwrap().unwrap(), "second");
            assert!(stream.receive().await.unwrap().is_none());
            assert!(stream.is_closed());
            drop(stream);
            assert_eq!(
                app.shutdown(Duration::from_secs(2)).await,
                ShutdownOutcome::Clean
            );
        }))
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn failed_stream_does_not_emit_a_successful_http_chunk_terminator() {
    LocalSet::new().run_until(Box::pin(async {
        let ingress = WebIngressFactory::default();
        let app = Kernel::start_native(plan(), TokioDriver::new(),
            NativePluginRegistry::new()
                .with_factory(StreamingEndpointFactory { malformed: false })
                .with_factory(ingress.clone())).await.unwrap();
        let mut connection = TcpStream::connect(ingress.local_address().unwrap()).await.unwrap();
        connection.write_all(b"GET /events?late-failure HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n").await.unwrap();
        let mut response = String::new();
        let _ = connection.read_to_string(&mut response).await;
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(response.contains("first"));
        assert!(!response.ends_with("0\r\n\r\n"));
        assert_eq!(app.shutdown(Duration::from_secs(2)).await, ShutdownOutcome::Clean);
    })).await;
}
