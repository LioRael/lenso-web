use crate::ingress::{
    IngressBody, IngressResponse, RequestIdSequence, acquire_request_permit, bad_request,
    canonical_request_head_len, dispatch_buffered, mark_sensitive_headers, payload_too_large,
    replace_request_id, request_timeout, unavailable, with_transport_headers,
};
use crate::{
    WebIngressConfig, WebIngressMiddleware,
    replication::{ReplicaConnection, ReplicaConnectionSource},
    routing::RouteTable,
    session_cookie::SessionCookiePolicy,
};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt as _, stream::FuturesUnordered};
use http::{Request, Response, StatusCode, header::CONTENT_LENGTH};
use http_body_util::{BodyExt as _, Full};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::conn::auto,
};
use lenso_capability_http_stream_endpoint as stream_endpoint;
use lenso_kernel::{CancellationToken, NativeStream, StreamEvent};
use std::{
    cell::Cell,
    convert::Infallible,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tokio::{net::TcpListener, sync::Semaphore};

const IDLE_CONNECTION_CLOSE_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

#[derive(Clone, Debug)]
struct IngressService {
    cancellation: CancellationToken,
    config: WebIngressConfig,
    middleware: Vec<Rc<dyn WebIngressMiddleware>>,
    routes: Rc<RouteTable>,
    global_concurrency: Arc<Semaphore>,
    local_concurrency: Option<Arc<Semaphore>>,
    activity: Option<ConnectionActivity>,
    session_cookie: Option<SessionCookiePolicy>,
    next_request_id: RequestIdSequence,
}

#[derive(Clone, Debug)]
struct ConnectionActivity {
    state: Rc<ConnectionActivityState>,
}

#[derive(Debug)]
struct ConnectionActivityState {
    active_requests: Cell<usize>,
    idle_since: Cell<Instant>,
}

impl ConnectionActivity {
    fn new() -> Self {
        Self {
            state: Rc::new(ConnectionActivityState {
                active_requests: Cell::new(0),
                idle_since: Cell::new(Instant::now()),
            }),
        }
    }

    fn begin(&self) -> ActiveRequest {
        self.state
            .active_requests
            .set(self.state.active_requests.get().saturating_add(1));
        ActiveRequest(self.clone())
    }
}

struct ActiveRequest(ConnectionActivity);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        let remaining = self.0.state.active_requests.get().saturating_sub(1);
        self.0.state.active_requests.set(remaining);
        if remaining == 0 {
            self.0.state.idle_since.set(Instant::now());
        }
    }
}

#[derive(Debug)]
enum ResponseBody {
    Buffered(Full<Bytes>),
    Streaming(StreamingBody),
}

struct StreamingBody {
    stream: Rc<NativeStream<stream_endpoint::StreamEndpointHandle>>,
    receive: Option<
        futures::future::LocalBoxFuture<
            'static,
            Result<
                StreamEvent<stream_endpoint::HandleResponse, stream_endpoint::HandleError>,
                lenso_kernel::RuntimeFailure,
            >,
        >,
    >,
    done: bool,
}

impl std::fmt::Debug for StreamingBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamingBody")
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl hyper::body::Body for ResponseBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            Self::Buffered(body) => Pin::new(body).poll_frame(context),
            Self::Streaming(body) => body.poll_frame(context),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Buffered(body) => body.is_end_stream(),
            Self::Streaming(body) => body.done,
        }
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        match self {
            Self::Buffered(body) => body.size_hint(),
            Self::Streaming(_) => hyper::body::SizeHint::default(),
        }
    }
}

impl StreamingBody {
    fn new(stream: NativeStream<stream_endpoint::StreamEndpointHandle>) -> Self {
        Self {
            stream: Rc::new(stream),
            receive: None,
            done: false,
        }
    }

    fn poll_frame(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        loop {
            if self.done {
                return Poll::Ready(None);
            }
            let receive = self.receive.get_or_insert_with(|| {
                let stream = self.stream.clone();
                Box::pin(async move { stream.receive().await })
            });
            let event = match receive.as_mut().poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(event) => {
                    self.receive = None;
                    event
                }
            };
            match event {
                Ok(StreamEvent::Message(frame))
                    if frame.kind == stream_endpoint::HandleResponseKind::Chunk
                        && frame.status.is_none()
                        && frame.headers.is_none() =>
                {
                    let body = frame.body.unwrap_or_default().into_shared();
                    return Poll::Ready(Some(Ok(Frame::data(body))));
                }
                Ok(StreamEvent::PeerHalfClosed) => {}
                Ok(StreamEvent::Terminal(_) | StreamEvent::Message(_)) | Err(_) => {
                    self.stream.cancel();
                    self.done = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl IngressResponse {
    fn into_response(self) -> Response<ResponseBody> {
        let body = match self.body {
            IngressBody::Buffered(body) => ResponseBody::Buffered(Full::new(body)),
            IngressBody::Streaming(stream) => ResponseBody::Streaming(StreamingBody::new(stream)),
        };
        let mut response = Response::new(body);
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers;
        response
    }
}

#[derive(Debug)]
pub(super) enum ConnectionSource {
    Listener(TcpListener),
    Replica(ReplicaConnectionSource),
}

impl ConnectionSource {
    fn request_concurrency(&self, limit: usize) -> (Arc<Semaphore>, Option<Arc<Semaphore>>) {
        match self {
            Self::Listener(_) => (Arc::new(Semaphore::new(limit)), None),
            Self::Replica(source) => (
                Arc::clone(&source.global_request_concurrency),
                Some(Arc::clone(&source.local_request_concurrency)),
            ),
        }
    }

    fn connection_concurrency(&self, limit: usize) -> Arc<Semaphore> {
        match self {
            Self::Listener(_) => Arc::new(Semaphore::new(limit)),
            Self::Replica(source) => Arc::clone(&source.global_connection_concurrency),
        }
    }

    fn request_ids(&self) -> RequestIdSequence {
        match self {
            Self::Listener(_) => RequestIdSequence::Local(Rc::new(Cell::new(0))),
            Self::Replica(source) => {
                RequestIdSequence::Replicated(Arc::clone(&source.next_request_id))
            }
        }
    }

    async fn accept(
        &mut self,
    ) -> std::io::Result<
        Option<(
            tokio::net::TcpStream,
            Option<tokio::sync::OwnedSemaphorePermit>,
        )>,
    > {
        match self {
            Self::Listener(listener) => listener
                .accept()
                .await
                .map(|(stream, _)| Some((stream, None))),
            Self::Replica(source) => match source.receive().await? {
                Some(ReplicaConnection { stream, permit }) => Ok(Some((
                    tokio::net::TcpStream::from_std(stream)?,
                    Some(permit),
                ))),
                None => Ok(None),
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LocalExecutor;

impl<F> hyper::rt::Executor<F> for LocalExecutor
where
    F: Future<Output = ()> + 'static,
{
    fn execute(&self, future: F) {
        tokio::task::spawn_local(future);
    }
}

pub(super) async fn serve(
    mut source: ConnectionSource,
    config: WebIngressConfig,
    routes: std::rc::Rc<RouteTable>,
    middleware: Vec<Rc<dyn WebIngressMiddleware>>,
    cancellation: CancellationToken,
) -> std::io::Result<()> {
    let next_request_id = source.request_ids();
    let connection_concurrency = source.connection_concurrency(config.max_connections());
    let (global_concurrency, local_concurrency) =
        source.request_concurrency(config.max_concurrent_requests());
    let session_cookie = config.session_cookie().map(SessionCookiePolicy::from);
    let service = IngressService {
        cancellation: cancellation.clone(),
        global_concurrency,
        local_concurrency,
        activity: None,
        config,
        middleware,
        routes,
        session_cookie,
        next_request_id,
    };
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let mut connections = FuturesUnordered::new();
    loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                shutdown.send_replace(true);
                while connections.next().await.is_some() {}
                return Ok(());
            }
            accepted = source.accept() => {
                let (stream, distributed_permit) = match accepted {
                    Ok(Some(accepted)) => accepted,
                    Ok(None) => {
                        shutdown.send_replace(true);
                        while connections.next().await.is_some() {}
                        return Ok(());
                    }
                    Err(error) => {
                        shutdown.send_replace(true);
                        while connections.next().await.is_some() {}
                        return Err(error);
                    }
                };
                let connection_permit = if let Some(permit) = distributed_permit {
                    permit
                } else {
                    let Ok(permit) = Arc::clone(&connection_concurrency).try_acquire_owned()
                    else {
                        continue;
                    };
                    permit
                };
                let activity = ConnectionActivity::new();
                let mut connection_service = service.clone();
                connection_service.activity = Some(activity.clone());
                let mut shutdown_signal = shutdown.subscribe();
                connections.push(tokio::task::spawn_local(async move {
                    let _connection_permit = connection_permit;
                    let idle_timeout = connection_service.config.connection_idle_timeout();
                    let shutdown_grace = connection_service.config.shutdown_grace_timeout();
                    let mut builder = auto::Builder::new(LocalExecutor);
                    configure_protocol_limits(&mut builder, &connection_service.config);
                    let connection = builder.serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request| connection_service.clone().call(request)),
                    );
                    tokio::pin!(connection);
                    tokio::select! {
                        _ = &mut connection => {}
                        () = wait_for_shutdown(&mut shutdown_signal) => {
                            connection.as_mut().graceful_shutdown();
                            let _ = tokio::time::timeout(
                                shutdown_grace,
                                connection.as_mut(),
                            ).await;
                        }
                        () = wait_for_connection_idle(
                            &activity,
                            idle_timeout,
                        ) => {
                            connection.as_mut().graceful_shutdown();
                            let _ = tokio::time::timeout(
                                IDLE_CONNECTION_CLOSE_GRACE,
                                connection.as_mut(),
                            ).await;
                        }
                    }
                }));
            }
            completed = connections.next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    shutdown.send_replace(true);
                    return Err(std::io::Error::other(format!(
                        "Web Ingress connection task failed: {error}"
                    )));
                }
            }
        }
    }
}

fn configure_protocol_limits(
    builder: &mut auto::Builder<LocalExecutor>,
    config: &WebIngressConfig,
) {
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(config.request_head_timeout());
    builder.http2().max_concurrent_streams(
        u32::try_from(config.max_concurrent_requests())
            .expect("validated Web Ingress concurrency fits HTTP/2 SETTINGS"),
    );
}

pub(super) fn assert_server_result(result: std::io::Result<()>) {
    result.unwrap_or_else(|error| panic!("Web Ingress server failed: {error}"));
}

async fn wait_for_shutdown(shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_for_connection_idle(activity: &ConnectionActivity, timeout: std::time::Duration) {
    loop {
        let active = activity.state.active_requests.get();
        let deadline = if active == 0 {
            activity.state.idle_since.get() + timeout
        } else {
            Instant::now() + timeout
        };
        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        if activity.state.active_requests.get() == 0
            && Instant::now().duration_since(activity.state.idle_since.get()) >= timeout
        {
            return;
        }
    }
}

impl IngressService {
    async fn call(
        self,
        mut request: Request<Incoming>,
    ) -> Result<Response<ResponseBody>, Infallible> {
        let method = request.method().clone();
        let _active_request = self.activity.as_ref().map(ConnectionActivity::begin);
        mark_sensitive_headers(request.headers_mut(), self.session_cookie.as_ref());
        let request_head_len = canonical_request_head_len(&request);
        let request_id = replace_request_id(request.headers_mut(), &self.next_request_id);
        let response = if request_head_len > self.config.max_request_head_bytes() {
            IngressResponse::json(
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                r#"{"error":"request_header_fields_too_large"}"#,
            )
        } else {
            let local_permit = if let Some(local) = &self.local_concurrency {
                let Some(permit) = acquire_request_permit(local, &self.cancellation).await else {
                    return Ok(with_transport_headers(
                        unavailable().into_response(),
                        request_id,
                    ));
                };
                Some(permit)
            } else {
                None
            };
            let global_permit =
                acquire_request_permit(&self.global_concurrency, &self.cancellation).await;
            if let Some(_global_permit) = global_permit {
                let _local_permit = local_permit;
                self.dispatch(request).await
            } else {
                unavailable()
            }
        };
        Ok(with_transport_headers(
            response.normalize_body(&method).into_response(),
            request_id,
        ))
    }

    async fn dispatch(&self, request: Request<Incoming>) -> IngressResponse {
        let (parts, body) = request.into_parts();
        let content_length = parts
            .headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()?.parse::<usize>().ok());
        if content_length.is_some_and(|length| length > self.config.max_request_body_bytes()) {
            return payload_too_large();
        }
        let body = if body_is_already_complete(&body) {
            Ok(Bytes::new())
        } else {
            tokio::select! {
                body = tokio::time::timeout(
                    self.config.request_body_timeout(),
                    collect_bounded_body(
                        body,
                        content_length,
                        self.config.max_request_body_bytes(),
                    ),
                ) => match body {
                    Ok(body) => body,
                    Err(_) => return request_timeout(),
                },
                () = self.cancellation.cancelled() => return unavailable(),
            }
        };
        let body = match body {
            Ok(body) => body,
            Err(BodyReadError::TooLarge) => return payload_too_large(),
            Err(BodyReadError::Invalid) => return bad_request(),
        };
        dispatch_buffered(
            parts,
            body,
            self.cancellation.clone(),
            self.session_cookie.as_ref(),
            self.routes.clone(),
            &self.middleware,
        )
        .await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyReadError {
    TooLarge,
    Invalid,
}

#[inline]
fn body_is_already_complete(body: &impl hyper::body::Body) -> bool {
    body.is_end_stream()
}

async fn collect_bounded_body(
    mut body: Incoming,
    content_length: Option<usize>,
    limit: usize,
) -> Result<Bytes, BodyReadError> {
    let mut first = None::<Bytes>;
    let mut combined = None::<BytesMut>;
    let mut total = 0_usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| BodyReadError::Invalid)?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        total = total
            .checked_add(data.len())
            .ok_or(BodyReadError::TooLarge)?;
        if total > limit {
            return Err(BodyReadError::TooLarge);
        }
        if data.is_empty() {
            continue;
        }
        if let Some(buffer) = &mut combined {
            buffer.extend_from_slice(&data);
        } else if let Some(initial) = first.take() {
            let capacity = content_length.unwrap_or(total).min(limit).max(total);
            let mut buffer = BytesMut::with_capacity(capacity);
            buffer.extend_from_slice(&initial);
            buffer.extend_from_slice(&data);
            combined = Some(buffer);
        } else {
            first = Some(data);
        }
    }
    Ok(match (combined, first) {
        (Some(buffer), _) => buffer.freeze(),
        (None, Some(data)) => data,
        (None, None) => Bytes::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionActivity, acquire_request_permit, assert_server_result, body_is_already_complete,
        canonical_request_head_len, wait_for_connection_idle,
    };
    use crate::ingress::{is_static_hop_by_hop_name, request_id_header_value, serialized_uri_len};
    use axum::http::{Request, Uri, Version};
    use bytes::Bytes;
    use http_body_util::Empty;
    use lenso_kernel::CancellationToken;
    use std::time::{Duration, Instant};
    use tokio::sync::Semaphore;

    #[test]
    fn an_already_complete_body_uses_the_empty_fast_path() {
        assert!(body_is_already_complete(&Empty::<Bytes>::new()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_permit_fast_path_respects_capacity_and_prior_cancellation() {
        let semaphore = Semaphore::new(1);
        let cancellation = CancellationToken::new();
        let permit = acquire_request_permit(&semaphore, &cancellation)
            .await
            .expect("available request capacity should be acquired immediately");
        assert_eq!(semaphore.available_permits(), 0);
        drop(permit);

        cancellation.cancel();
        assert!(
            acquire_request_permit(&semaphore, &cancellation)
                .await
                .is_none()
        );
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_watchdog_starts_at_the_last_request_completion() {
        let activity = ConnectionActivity::new();
        let active = activity.begin();
        let idle_timeout = Duration::from_millis(20);
        let wait = async {
            wait_for_connection_idle(&activity, idle_timeout).await;
            Instant::now()
        };
        let finish = async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(active);
            Instant::now()
        };

        let (closed_at, finished_at) = tokio::join!(wait, finish);
        let observed_idle = closed_at.duration_since(finished_at);
        assert!(observed_idle >= idle_timeout);
        assert!(observed_idle < idle_timeout + Duration::from_millis(50));
    }

    #[test]
    fn request_id_header_values_cover_the_full_counter_range() {
        assert_eq!(request_id_header_value(0), "lenso-0");
        assert_eq!(
            request_id_header_value(u64::MAX),
            "lenso-18446744073709551615"
        );
    }

    #[test]
    fn uri_length_matches_http_uri_serialization() {
        for uri in [
            "/orders/42?include=items",
            "http://example.com/orders/42?include=items",
            "*",
        ] {
            let uri = uri.parse::<Uri>().expect("fixture URI");
            assert_eq!(serialized_uri_len(&uri), uri.to_string().len());
        }
    }

    #[test]
    fn canonical_request_head_length_includes_wire_separators() {
        let request = Request::builder()
            .method("GET")
            .uri("/orders/42?include=items")
            .version(Version::HTTP_11)
            .header("host", "example.test")
            .body(())
            .unwrap();
        assert_eq!(
            canonical_request_head_len(&request),
            "GET /orders/42?include=items HTTP/1.1\r\nhost: example.test\r\n\r\n".len()
        );
    }

    #[test]
    #[should_panic(expected = "Web Ingress server failed")]
    fn server_errors_are_not_silently_discarded() {
        assert_server_result(Err(std::io::Error::other("fixture failure")));
    }

    #[test]
    fn hop_by_hop_names_are_filtered_before_header_name_allocation() {
        assert!(is_static_hop_by_hop_name("keep-alive"));
        assert!(is_static_hop_by_hop_name("Transfer-Encoding"));
        assert!(!is_static_hop_by_hop_name("x-forwarded-for"));
    }
}
