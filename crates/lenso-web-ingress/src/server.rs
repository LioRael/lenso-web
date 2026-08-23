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
use hyper_util::{rt::TokioIo, server::conn::auto};
use lenso_capability_http_endpoint::HandleResponse;
use lenso_kernel::CancellationToken;
use tokio::{
    net::TcpListener,
    sync::{Semaphore, oneshot},
};

use crate::{
    WebIngressConfig, replication::ReplicaConnectionSource, routing::DispatchError,
    routing::RouteTable,
};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const NOSNIFF_HEADER: HeaderName = HeaderName::from_static("x-content-type-options");

#[derive(Debug)]
pub(super) struct CredentialEvidence {
    pub(super) scheme: String,
    pub(super) value: String,
}

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
    pub(super) headers: Vec<InboundHeader>,
    pub(super) method: Method,
    pub(super) path: String,
    pub(super) query: Option<String>,
    pub(super) request_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestRejection {
    BadRequest,
}

#[derive(Clone, Debug)]
struct IngressService {
    cancellation: CancellationToken,
    config: WebIngressConfig,
    routes: Rc<RouteTable>,
    concurrency: Arc<Semaphore>,
    next_request_id: RequestIdSequence,
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
}

#[derive(Debug)]
pub(super) enum ConnectionSource {
    Listener(TcpListener),
    Replica(ReplicaConnectionSource),
}

impl ConnectionSource {
    fn concurrency(&self, limit: usize) -> Arc<Semaphore> {
        match self {
            Self::Listener(_) => Arc::new(Semaphore::new(limit)),
            Self::Replica(source) => Arc::clone(&source.concurrency),
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

    async fn accept(&mut self) -> std::io::Result<Option<tokio::net::TcpStream>> {
        match self {
            Self::Listener(listener) => listener.accept().await.map(|(stream, _)| Some(stream)),
            Self::Replica(source) => loop {
                let Some(stream) = source.receiver.recv().await else {
                    return Ok(None);
                };
                if let Ok(stream) = tokio::net::TcpStream::from_std(stream) {
                    return Ok(Some(stream));
                }
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
    cancellation: CancellationToken,
) -> std::io::Result<()> {
    let next_request_id = source.request_ids();
    let service = IngressService {
        cancellation: cancellation.clone(),
        concurrency: source.concurrency(config.max_concurrent_requests()),
        config,
        routes,
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
                let stream = match accepted {
                    Ok(Some(stream)) => stream,
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
                let connection_service = service.clone();
                let mut shutdown_signal = shutdown.subscribe();
                connections.push(tokio::task::spawn_local(async move {
                    let builder = auto::Builder::new(LocalExecutor);
                    let connection = builder.serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |request| connection_service.clone().call(request)),
                    );
                    tokio::pin!(connection);
                    tokio::select! {
                        _ = &mut connection => {}
                        () = wait_for_shutdown(&mut shutdown_signal) => {
                            connection.as_mut().graceful_shutdown();
                            let _ = connection.await;
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

impl IngressService {
    async fn call(
        self,
        mut request: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        mark_sensitive_headers(request.headers_mut());
        let request_head_len = canonical_request_head_len(&request);
        let request_id = replace_request_id(request.headers_mut(), &self.next_request_id);
        let response = if request_head_len > self.config.max_request_head_bytes() {
            IngressResponse::json(
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                r#"{"error":"request_header_fields_too_large"}"#,
            )
        } else {
            let permit = tokio::select! {
                permit = self.concurrency.acquire() => Some(
                    permit.expect("the Ingress concurrency semaphore remains open")
                ),
                () = self.cancellation.cancelled() => None,
            };
            if let Some(_permit) = permit {
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
        let body = tokio::select! {
            body = collect_bounded_body(
                body,
                content_length,
                self.config.max_request_body_bytes(),
            ) => body,
            () = self.cancellation.cancelled() => return unavailable(),
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
        ) {
            Ok(request) => request,
            Err(RequestRejection::BadRequest) => return bad_request(),
        };
        let response = match AssertUnwindSafe(self.routes.dispatch(request))
            .catch_unwind()
            .await
        {
            Ok(result) => dispatch_response(result),
            Err(_) => unavailable(),
        };
        drop(cancel_on_drop);
        response
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyReadError {
    TooLarge,
    Invalid,
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

fn mark_sensitive_headers(headers: &mut HeaderMap) {
    for (name, value) in headers.iter_mut() {
        if name == AUTHORIZATION || name == COOKIE {
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
) -> Result<InboundRequest, RequestRejection> {
    let request_id = request_id(headers)?;
    let credential = credential(headers)?;
    let connection_owned = connection_owned_headers(headers)?;
    let headers = headers
        .iter()
        .filter(|(name, _)| !is_filtered_request_header(name) && !connection_owned.contains(*name))
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

fn credential(headers: &HeaderMap) -> Result<Option<CredentialEvidence>, RequestRejection> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(RequestRejection::BadRequest);
    }
    let value = value.to_str().map_err(|_| RequestRejection::BadRequest)?;
    let Some((scheme, value)) = value.split_once(' ') else {
        return Err(RequestRejection::BadRequest);
    };
    if scheme.is_empty() || value.is_empty() {
        return Err(RequestRejection::BadRequest);
    }
    Ok(Some(CredentialEvidence {
        scheme: scheme.to_ascii_lowercase(),
        value: value.to_owned(),
    }))
}

fn bad_request() -> IngressResponse {
    IngressResponse::json(StatusCode::BAD_REQUEST, r#"{"error":"bad_request"}"#)
}

fn payload_too_large() -> IngressResponse {
    IngressResponse::json(
        StatusCode::PAYLOAD_TOO_LARGE,
        r#"{"error":"payload_too_large"}"#,
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
        assert_server_result, canonical_request_head_len, is_static_hop_by_hop_name,
        request_id_header_value, serialized_uri_len,
    };
    use axum::http::{Request, Uri, Version};

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
