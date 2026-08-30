use std::{
    cell::Cell,
    collections::HashSet,
    convert::Infallible,
    future::Future,
    panic::AssertUnwindSafe,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, Version,
    header::{
        ALLOW, AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST, TE, TRAILER,
        TRANSFER_ENCODING, UPGRADE,
    },
};
use http_body_util::{BodyExt as _, Full};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::conn::auto,
};
use lenso_capability_http_endpoint::HandleResponse;
use lenso_kernel::CancellationToken;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot},
};

use crate::{
    WebIngressConfig, WebIngressMiddleware, WebIngressRequest, WebIngressResponse, middleware,
    replication::{ReplicaConnection, ReplicaConnectionSource},
    routing::DispatchError,
    routing::RouteTable,
    session_cookie::{
        CredentialEvidence, CredentialRejection, SessionCookiePolicy, select_credential,
    },
};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const NOSNIFF_HEADER: HeaderName = HeaderName::from_static("x-content-type-options");
const IDLE_CONNECTION_CLOSE_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

#[derive(Debug)]
pub(super) struct InboundHeader {
    pub(super) name: String,
    pub(super) value: String,
}

#[derive(Debug)]
pub(super) struct InboundRequest {
    pub(super) body: Bytes,
    pub(super) cancellation: CancellationToken,
    pub(super) credential: Option<CredentialEvidence>,
    pub(super) disconnected: oneshot::Receiver<()>,
    csrf_header_name: Option<HeaderName>,
    pub(super) headers: Vec<InboundHeader>,
    pub(super) method: Method,
    pub(super) path: String,
    pub(super) query: Option<String>,
    pub(super) request_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestRejection {
    BadRequest,
    CsrfForbidden,
}

impl From<CredentialRejection> for RequestRejection {
    fn from(value: CredentialRejection) -> Self {
        match value {
            CredentialRejection::BadRequest => Self::BadRequest,
            CredentialRejection::CsrfForbidden => Self::CsrfForbidden,
        }
    }
}

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

#[derive(Clone, Debug)]
enum RequestIdSequence {
    Local(Rc<Cell<u64>>),
    Replicated(Arc<AtomicU64>),
}

impl RequestIdSequence {
    fn next(&self) -> u64 {
        match self {
            Self::Local(next) => {
                let value = next.get();
                next.set(value.wrapping_add(1));
                value
            }
            Self::Replicated(next) => next.fetch_add(1, Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct CancelRequestOnDrop(Option<oneshot::Sender<()>>);

impl Drop for CancelRequestOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            let _ = cancel.send(());
        }
    }
}

fn request_id_header_value(mut value: u64) -> HeaderValue {
    let mut buffer = [0_u8; 26];
    let mut start = buffer.len();
    loop {
        start -= 1;
        buffer[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    start -= b"lenso-".len();
    buffer[start..start + b"lenso-".len()].copy_from_slice(b"lenso-");
    HeaderValue::from_bytes(&buffer[start..]).expect("generated request id is a valid header value")
}

#[derive(Debug)]
struct IngressResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl IngressResponse {
    fn json(status: StatusCode, body: &'static str) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        Self {
            status,
            headers,
            body: Bytes::from_static(body.as_bytes()),
        }
    }
}

impl IngressResponse {
    fn into_response(self) -> Response<Full<Bytes>> {
        let mut response = Response::new(Full::new(self.body));
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers;
        response
    }

    fn into_middleware_response(self) -> WebIngressResponse {
        let mut response = Response::new(self.body);
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers;
        response
    }

    fn from_middleware_response(response: WebIngressResponse) -> Self {
        let (parts, body) = response.into_parts();
        let mut headers = parts.headers;
        let ingress_owned = headers
            .keys()
            .filter(|name| is_ingress_owned_response_header(name))
            .cloned()
            .collect::<Vec<_>>();
        for name in ingress_owned {
            headers.remove(name);
        }
        Self {
            status: parts.status,
            headers,
            body,
        }
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

async fn acquire_request_permit<'a>(
    semaphore: &'a Semaphore,
    cancellation: &CancellationToken,
) -> Option<tokio::sync::SemaphorePermit<'a>> {
    if cancellation.is_cancelled() {
        return None;
    }
    match semaphore.try_acquire() {
        Ok(permit) => Some(permit),
        Err(tokio::sync::TryAcquireError::NoPermits) => tokio::select! {
            permit = semaphore.acquire() => Some(
                permit.expect("the Ingress concurrency semaphore remains open")
            ),
            () = cancellation.cancelled() => None,
        },
        Err(tokio::sync::TryAcquireError::Closed) => {
            panic!("the Ingress concurrency semaphore remains open")
        }
    }
}

impl IngressService {
    async fn call(
        self,
        mut request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
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
        Ok(with_transport_headers(response.into_response(), request_id))
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
        let (disconnect, disconnected) = oneshot::channel();
        let cancel_on_drop = CancelRequestOnDrop(Some(disconnect));
        let request = match inbound_request(
            &parts.method,
            &parts.uri,
            &parts.headers,
            body,
            self.cancellation.clone(),
            disconnected,
            self.session_cookie.as_ref(),
        ) {
            Ok(request) => request,
            Err(rejection) => return request_rejection(rejection),
        };
        let (request, control) = middleware_request(request, parts.version);
        let routes = self.routes.clone();
        let response =
            AssertUnwindSafe(middleware::run(&self.middleware, request, move |request| {
                let request = restore_inbound_request(request, control);
                async move {
                    match request {
                        Ok(request) => dispatch_response(routes.dispatch(request).await)
                            .into_middleware_response(),
                        Err(rejection) => request_rejection(rejection).into_middleware_response(),
                    }
                }
            }))
            .catch_unwind()
            .await
            .ok()
            .and_then(Result::ok)
            .map_or_else(unavailable, IngressResponse::from_middleware_response);
        drop(cancel_on_drop);
        response
    }
}

struct InboundRequestControl {
    cancellation: CancellationToken,
    credential: Option<CredentialEvidence>,
    csrf_header_name: Option<HeaderName>,
    disconnected: oneshot::Receiver<()>,
    request_id: String,
}

fn middleware_request(
    request: InboundRequest,
    version: Version,
) -> (WebIngressRequest, InboundRequestControl) {
    let InboundRequest {
        body,
        cancellation,
        credential,
        csrf_header_name,
        disconnected,
        headers,
        method,
        path,
        query,
        request_id,
    } = request;
    let uri = query.map_or_else(|| path.clone(), |query| format!("{path}?{query}"));
    let mut middleware_request = Request::builder()
        .method(method)
        .uri(uri)
        .version(version)
        .body(body)
        .expect("an accepted HTTP request remains valid");
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes())
            .expect("Ingress produced a valid header name");
        let value =
            HeaderValue::from_str(&header.value).expect("Ingress produced a valid header value");
        middleware_request.headers_mut().append(name, value);
    }
    middleware_request.headers_mut().insert(
        REQUEST_ID_HEADER.clone(),
        HeaderValue::from_str(&request_id).expect("Ingress request IDs are valid headers"),
    );
    (
        middleware_request,
        InboundRequestControl {
            cancellation,
            credential,
            csrf_header_name,
            disconnected,
            request_id,
        },
    )
}

fn restore_inbound_request(
    request: &WebIngressRequest,
    control: InboundRequestControl,
) -> Result<InboundRequest, RequestRejection> {
    let connection_owned = connection_owned_headers(request.headers())?;
    let headers = request
        .headers()
        .iter()
        .filter(|(name, _)| {
            !is_filtered_request_header(name)
                && !control
                    .csrf_header_name
                    .as_ref()
                    .is_some_and(|csrf| csrf == *name)
                && !connection_owned.contains(*name)
        })
        .map(|(name, value)| {
            value
                .to_str()
                .map(|value| InboundHeader {
                    name: name.as_str().to_owned(),
                    value: value.to_owned(),
                })
                .map_err(|_| RequestRejection::BadRequest)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(InboundRequest {
        body: request.body().clone(),
        cancellation: control.cancellation,
        credential: control.credential,
        csrf_header_name: control.csrf_header_name,
        disconnected: control.disconnected,
        headers,
        method: normalized_method(request.method()),
        path: request.uri().path().to_owned(),
        query: request.uri().query().map(ToOwned::to_owned),
        request_id: control.request_id,
    })
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

fn dispatch_response(result: Result<HandleResponse, DispatchError>) -> IngressResponse {
    match result {
        Ok(response) => from_endpoint(response),
        Err(DispatchError::NotFound) => {
            IngressResponse::json(StatusCode::NOT_FOUND, r#"{"error":"not_found"}"#)
        }
        Err(DispatchError::MethodNotAllowed(allowed)) => method_not_allowed(&allowed),
        Err(DispatchError::Rejected) => {
            IngressResponse::json(StatusCode::BAD_GATEWAY, r#"{"error":"endpoint_rejected"}"#)
        }
        Err(DispatchError::TimedOut) => IngressResponse::json(
            StatusCode::GATEWAY_TIMEOUT,
            r#"{"error":"endpoint_timeout"}"#,
        ),
        Err(DispatchError::Unavailable) => unavailable(),
    }
}

fn mark_sensitive_headers(headers: &mut HeaderMap, session_cookie: Option<&SessionCookiePolicy>) {
    for (name, value) in headers.iter_mut() {
        if name == AUTHORIZATION
            || name == COOKIE
            || session_cookie.is_some_and(|policy| name == policy.csrf_header_name())
        {
            value.set_sensitive(true);
        }
    }
}

fn replace_request_id(headers: &mut HeaderMap, next: &RequestIdSequence) -> HeaderValue {
    let request_id = request_id_header_value(next.next());
    headers.insert(REQUEST_ID_HEADER, request_id.clone());
    request_id
}

fn connection_owned_headers(headers: &HeaderMap) -> Result<HashSet<HeaderName>, RequestRejection> {
    let mut owned = HashSet::new();
    for value in headers.get_all(CONNECTION) {
        let value = value.to_str().map_err(|_| RequestRejection::BadRequest)?;
        for name in value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            if is_static_hop_by_hop_name(name) {
                continue;
            }
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| RequestRejection::BadRequest)?;
            owned.insert(name);
        }
    }
    Ok(owned)
}

fn is_static_hop_by_hop_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("te")
        || name.eq_ignore_ascii_case("trailer")
        || name.eq_ignore_ascii_case("transfer-encoding")
        || name.eq_ignore_ascii_case("upgrade")
        || name.eq_ignore_ascii_case("keep-alive")
        || name.eq_ignore_ascii_case("proxy-connection")
}

fn is_static_hop_by_hop_header(name: &HeaderName) -> bool {
    name == CONNECTION
        || name == TE
        || name == TRAILER
        || name == TRANSFER_ENCODING
        || name == UPGRADE
        || name.as_str() == "keep-alive"
        || name.as_str() == "proxy-connection"
}

fn is_filtered_request_header(name: &HeaderName) -> bool {
    name == AUTHORIZATION
        || name == COOKIE
        || is_static_hop_by_hop_header(name)
        || name == CONTENT_LENGTH
        || name == HOST
        || name == REQUEST_ID_HEADER
}

fn is_ingress_owned_response_header(name: &HeaderName) -> bool {
    is_static_hop_by_hop_header(name)
        || name == CONTENT_LENGTH
        || name == NOSNIFF_HEADER
        || name == REQUEST_ID_HEADER
}

fn inbound_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
    cancellation: CancellationToken,
    disconnected: oneshot::Receiver<()>,
    session_cookie: Option<&SessionCookiePolicy>,
) -> Result<InboundRequest, RequestRejection> {
    let request_id = request_id(headers)?;
    let credential = select_credential(method, headers, session_cookie)?;
    let csrf_header_name = session_cookie.map(|policy| policy.csrf_header_name().clone());
    let connection_owned = connection_owned_headers(headers)?;
    let headers = headers
        .iter()
        .filter(|(name, _)| {
            !is_filtered_request_header(name)
                && !csrf_header_name.as_ref().is_some_and(|csrf| csrf == *name)
                && !connection_owned.contains(*name)
        })
        .map(|(name, value)| {
            value
                .to_str()
                .map(|value| InboundHeader {
                    name: name.as_str().to_owned(),
                    value: value.to_owned(),
                })
                .map_err(|_| RequestRejection::BadRequest)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(InboundRequest {
        body,
        cancellation,
        credential,
        csrf_header_name,
        disconnected,
        headers,
        method: normalized_method(method),
        path: uri.path().to_owned(),
        query: uri.query().map(ToOwned::to_owned),
        request_id,
    })
}

fn normalized_method(method: &Method) -> Method {
    if method
        .as_str()
        .bytes()
        .any(|byte| byte.is_ascii_lowercase())
    {
        let uppercase = method.as_str().to_ascii_uppercase();
        Method::from_bytes(uppercase.as_bytes()).expect("an existing HTTP method remains valid")
    } else {
        method.clone()
    }
}

fn from_endpoint(response: HandleResponse) -> IngressResponse {
    let Some(status) = u16::try_from(response.status)
        .ok()
        .and_then(|status| StatusCode::from_u16(status).ok())
    else {
        return invalid_endpoint_response();
    };
    let body = response.body.into_shared();
    let mut headers = HeaderMap::with_capacity(response.headers.len());
    for header in response.headers {
        let Ok(name) = HeaderName::from_bytes(header.name.as_bytes()) else {
            return invalid_endpoint_response();
        };
        if is_ingress_owned_response_header(&name) {
            return invalid_endpoint_response();
        }
        let Ok(value) = HeaderValue::from_str(&header.value) else {
            return invalid_endpoint_response();
        };
        headers.append(name, value);
    }
    IngressResponse {
        status,
        headers,
        body,
    }
}

fn canonical_request_head_len<B>(request: &Request<B>) -> usize {
    request.method().as_str().len()
        + 1
        + serialized_uri_len(request.uri())
        + 1
        + version_len(request.version())
        + 2
        + request
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + 2 + value.as_bytes().len() + 2)
            .sum::<usize>()
        + 2
}

const fn version_len(_version: Version) -> usize {
    8
}

fn with_transport_headers(
    mut response: Response<Full<Bytes>>,
    request_id: HeaderValue,
) -> Response<Full<Bytes>> {
    response
        .headers_mut()
        .insert(REQUEST_ID_HEADER.clone(), request_id);
    response
        .headers_mut()
        .insert(NOSNIFF_HEADER, HeaderValue::from_static("nosniff"));
    response
}

fn serialized_uri_len(uri: &Uri) -> usize {
    let mut size = uri
        .path_and_query()
        .map_or(0, |path_and_query| path_and_query.as_str().len());
    if let Some(scheme) = uri.scheme_str() {
        size += scheme.len() + 3;
    }
    if let Some(authority) = uri.authority() {
        size += authority.as_str().len();
    }
    size
}

fn request_id(headers: &HeaderMap) -> Result<String, RequestRejection> {
    let request_id = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(RequestRejection::BadRequest)?;
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(RequestRejection::BadRequest);
    }
    Ok(request_id.to_owned())
}

fn bad_request() -> IngressResponse {
    IngressResponse::json(StatusCode::BAD_REQUEST, r#"{"error":"bad_request"}"#)
}

fn csrf_forbidden() -> IngressResponse {
    IngressResponse::json(StatusCode::FORBIDDEN, r#"{"error":"csrf_rejected"}"#)
}

fn request_rejection(rejection: RequestRejection) -> IngressResponse {
    match rejection {
        RequestRejection::BadRequest => bad_request(),
        RequestRejection::CsrfForbidden => csrf_forbidden(),
    }
}

fn payload_too_large() -> IngressResponse {
    IngressResponse::json(
        StatusCode::PAYLOAD_TOO_LARGE,
        r#"{"error":"payload_too_large"}"#,
    )
}

fn request_timeout() -> IngressResponse {
    IngressResponse::json(
        StatusCode::REQUEST_TIMEOUT,
        r#"{"error":"request_timeout"}"#,
    )
}

fn method_not_allowed(allowed: &[Method]) -> IngressResponse {
    let mut response = IngressResponse::json(
        StatusCode::METHOD_NOT_ALLOWED,
        r#"{"error":"method_not_allowed"}"#,
    );
    let value = allowed
        .iter()
        .map(Method::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    if let Ok(value) = HeaderValue::from_str(&value) {
        response.headers.insert(ALLOW, value);
    }
    response
}

fn unavailable() -> IngressResponse {
    IngressResponse::json(
        StatusCode::SERVICE_UNAVAILABLE,
        r#"{"error":"endpoint_unavailable"}"#,
    )
}

fn invalid_endpoint_response() -> IngressResponse {
    IngressResponse::json(
        StatusCode::BAD_GATEWAY,
        r#"{"error":"invalid_endpoint_response"}"#,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionActivity, acquire_request_permit, assert_server_result, body_is_already_complete,
        canonical_request_head_len, is_static_hop_by_hop_name, request_id_header_value,
        serialized_uri_len, wait_for_connection_idle,
    };
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
