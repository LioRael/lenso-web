//! One portable provider corpus for native and actual Workers sessions.
use futures::{SinkExt, StreamExt, channel::mpsc, future::LocalBoxFuture};
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan, ResolvedAppPlan,
};
use lenso_capability_http_stream_endpoint as http;
use lenso_capability_websocket_endpoint as ws;
use lenso_kernel::{
    InvocationContext, NativeRequestEndpoint, NativeRequestFuture, NativeStreamEndpoint,
    NativeStreamItem, NativeStreamSession, NoopPluginLifecycle, RuntimeFailure,
};
use lenso_native_adapter::{NativePluginFactory, NativePluginFactoryContext, NativePluginInstance};
use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
};
pub const PACKAGE_ID: &str = "fixture.duplex";
pub fn plan(configuration: String) -> ResolvedAppPlan {
    let capabilities = [
        (
            http::CAPABILITY_ID,
            http::DESCRIPTOR_VERSION,
            http::DESCRIBE_OPERATION,
            http::HANDLE_OPERATION,
        ),
        (
            ws::CAPABILITY_ID,
            ws::DESCRIPTOR_VERSION,
            ws::DESCRIBE_WEBSOCKET_OPERATION,
            ws::CONNECT_WEBSOCKET_OPERATION,
        ),
    ];
    let mut endpoint = PluginInstancePlan::new("duplex", PACKAGE_ID);
    let mut ingress =
        PluginInstancePlan::new("ingress", "lenso.web-ingress").with_configuration(configuration);
    let mut bindings = Vec::new();
    for (id, version, describe, connect) in capabilities {
        endpoint = endpoint.with_capability(
            CapabilityEndpointPlan::new(id, version, [describe, connect])
                .with_stream_operation(connect),
        );
        ingress = ingress.with_requirement(CapabilityRequirementPlan::many(id, version));
        bindings.push(CapabilityBinding::new("ingress", id, version, "duplex"));
    }
    AppComposition::new(vec![endpoint, ingress], bindings)
        .resolve()
        .unwrap()
}
#[derive(Debug, Clone)]
pub struct DuplexFactory;
impl NativePluginFactory for DuplexFactory {
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
        let http = Rc::new(http::StreamEndpointEndpoint::new(Provider));
        let ws = Rc::new(ws::EndpointEndpoint::new(Provider));
        let requests: Vec<Rc<dyn NativeRequestEndpoint>> = vec![http.clone(), ws.clone()];
        let streams: Vec<Rc<dyn NativeStreamEndpoint>> = vec![http, ws];
        Ok(NativePluginInstance::with_endpoints(
            requests,
            streams,
            NoopPluginLifecycle,
        ))
    }
}
#[derive(Debug, Clone)]
struct Provider;
fn failure() -> RuntimeFailure {
    RuntimeFailure::PluginFailure {
        detail: "duplex fixture failure".into(),
    }
}
impl ws::EndpointProvider for Provider {
    fn describe_websocket(
        &self,
        _: InvocationContext,
        _: ws::DescribeWebsocketRequest,
    ) -> NativeRequestFuture<ws::EndpointDescribeWebsocket> {
        Box::pin(async {
            Ok(Ok(ws::DescribeWebsocketResponse {
                routes: vec![ws::DescribeWebsocketResponseRoutesItem {
                    route_id: "echo".into(),
                    path: "/socket/{room}".into(),
                }],
            }))
        })
    }
    fn connect_websocket(
        &self,
        _: InvocationContext,
        request: ws::ConnectWebsocketRequest,
    ) -> LocalBoxFuture<
        'static,
        Result<Box<dyn NativeStreamSession>, ws::EndpointConnectWebsocketInvocationError>,
    > {
        Box::pin(async move {
            use ws::{
                ConnectWebsocketError as Error, EndpointConnectWebsocketInvocationError as Failure,
            };
            if !request.credential.is_some_and(|credential| {
                credential.scheme == "bearer" && credential.value == "proof"
            }) {
                return Err(Failure::Domain(Error::Unauthorized));
            }
            if request
                .path_parameters
                .iter()
                .any(|parameter| parameter.name == "room" && parameter.value == "denied")
            {
                return Err(Failure::Domain(Error::Rejected));
            }
            if !request
                .protocols
                .iter()
                .any(|protocol| protocol == "lenso.echo")
            {
                return Err(Failure::Domain(Error::UnsupportedProtocol));
            }
            let (mut sender, receiver) = mpsc::channel(4);
            sender
                .try_send(NativeStreamItem::Message(Box::new(
                    ws::ConnectWebsocketResponse {
                        kind: ws::ConnectWebsocketResponseKind::Accept,
                        protocol: Some("lenso.echo".into()),
                        text: None,
                        body: None,
                        code: None,
                        reason: None,
                    },
                )))
                .map_err(|_| Failure::Runtime(failure()))?;
            Ok(Box::new(Echo {
                sender: RefCell::new(sender),
                receiver: Rc::new(futures::lock::Mutex::new(receiver)),
            }) as Box<dyn NativeStreamSession>)
        })
    }
}
#[derive(Debug)]
struct Echo {
    sender: RefCell<mpsc::Sender<NativeStreamItem>>,
    receiver: Rc<futures::lock::Mutex<mpsc::Receiver<NativeStreamItem>>>,
}
impl NativeStreamSession for Echo {
    fn send(&self, message: Box<dyn Any>) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        let mut sender = self.sender.borrow().clone();
        Box::pin(async move {
            let message = message
                .downcast::<ws::ConnectWebsocketResponse>()
                .map_err(|_| failure())?;
            let close = message.kind == ws::ConnectWebsocketResponseKind::Close;
            sender
                .send(NativeStreamItem::Message(message))
                .await
                .map_err(|_| failure())?;
            if close {
                sender
                    .send(NativeStreamItem::Terminal(Ok(())))
                    .await
                    .map_err(|_| failure())?;
            }
            Ok(())
        })
    }
    fn receive(&self) -> LocalBoxFuture<'static, Result<NativeStreamItem, RuntimeFailure>> {
        let receiver = self.receiver.clone();
        Box::pin(async move { receiver.lock().await.next().await.ok_or_else(failure) })
    }
    fn close_send(&self) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(async { Ok(()) })
    }
    fn cancel(&self) {
        self.sender.borrow_mut().close_channel();
    }
}
impl http::StreamEndpointProvider for Provider {
    fn describe_stream(
        &self,
        _: InvocationContext,
        _: http::DescribeRequest,
    ) -> NativeRequestFuture<http::StreamEndpointDescribe> {
        Box::pin(async {
            Ok(Ok(http::DescribeResponse {
                routes: vec![http::DescribeResponseRoutesItem {
                    route_id: "stream".into(),
                    method: "GET".into(),
                    path: "/stream".into(),
                }],
            }))
        })
    }
    fn handle_stream(
        &self,
        _: InvocationContext,
        request: http::HandleRequest,
    ) -> LocalBoxFuture<
        'static,
        Result<Box<dyn NativeStreamSession>, http::StreamEndpointHandleInvocationError>,
    > {
        Box::pin(async move {
            let mut frames = VecDeque::new();
            frames.push_back(NativeStreamItem::Message(Box::new(http::HandleResponse {
                kind: http::HandleResponseKind::Head,
                status: Some(200),
                headers: Some(vec![http::HandleResponseHeadersItem {
                    name: "content-type".into(),
                    value: "application/octet-stream".into(),
                }]),
                body: None,
            })));
            for bytes in [vec![0, 1, 255], vec![2, 3, 254]] {
                frames.push_back(NativeStreamItem::Message(Box::new(http::HandleResponse {
                    kind: http::HandleResponseKind::Chunk,
                    status: None,
                    headers: None,
                    body: Some(bytes.into()),
                })));
            }
            if request.query.as_deref() != Some("hold") {
                frames.push_back(NativeStreamItem::Terminal(Ok(())));
            }
            Ok(Box::new(HttpStream {
                frames: RefCell::new(frames),
                cancelled: Cell::new(false),
            }) as Box<dyn NativeStreamSession>)
        })
    }
}
#[derive(Debug)]
struct HttpStream {
    frames: RefCell<VecDeque<NativeStreamItem>>,
    cancelled: Cell<bool>,
}
impl NativeStreamSession for HttpStream {
    fn send(&self, _: Box<dyn Any>) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(async { Ok(()) })
    }
    fn receive(&self) -> LocalBoxFuture<'static, Result<NativeStreamItem, RuntimeFailure>> {
        let frame = self.frames.borrow_mut().pop_front();
        Box::pin(async move {
            match frame {
                Some(frame) => Ok(frame),
                None => futures::future::pending().await,
            }
        })
    }
    fn close_send(&self) -> LocalBoxFuture<'static, Result<(), RuntimeFailure>> {
        Box::pin(async { Ok(()) })
    }
    fn cancel(&self) {
        self.cancelled.set(true);
    }
}
