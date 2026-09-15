//! Shared HTTP semantics. Socket and event adapters supply already bounded bytes.
use crate::{
    WebIngressMiddleware, WebIngressRequest, WebIngressResponse, middleware,
    routing::{DispatchError, DispatchResponse, RouteTable},
    session_cookie::{
        CredentialEvidence, CredentialRejection, SessionCookiePolicy, select_credential,
    },
};
use bytes::Bytes;
use futures::FutureExt as _;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri, Version,
    header::{
        ALLOW, AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST, TE, TRAILER,
        TRANSFER_ENCODING, UPGRADE,
    },
};
use lenso_capability_http_endpoint::HandleResponse;
use lenso_capability_http_stream_endpoint as stream_endpoint;
use lenso_kernel::{CancellationToken, NativeStream};
#[cfg(feature = "native")]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{
    cell::{Cell, RefCell},
    collections::HashSet,
    panic::AssertUnwindSafe,
    rc::Rc,
};
#[cfg(feature = "native")]
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const NOSNIFF_HEADER: HeaderName = HeaderName::from_static("x-content-type-options");

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
pub(super) enum RequestRejection {
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
pub(super) enum RequestIdSequence {
    Local(Rc<Cell<u64>>),
    #[cfg(feature = "native")]
    Replicated(Arc<AtomicU64>),
}

impl RequestIdSequence {
    pub(super) fn next(&self) -> u64 {
        match self {
            Self::Local(next) => {
                let value = next.get();
                next.set(value.wrapping_add(1));
                value
            }
            #[cfg(feature = "native")]
            Self::Replicated(next) => next.fetch_add(1, Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub(super) struct CancelRequestOnDrop(Option<oneshot::Sender<()>>);

impl Drop for CancelRequestOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            let _ = cancel.send(());
        }
    }
}

pub(super) fn request_id_header_value(mut value: u64) -> HeaderValue {
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
pub(super) struct IngressResponse {
    pub(super) status: StatusCode,
    pub(super) headers: HeaderMap,
    pub(super) body: IngressBody,
}

#[derive(Debug)]
pub(super) enum IngressBody {
    Buffered(Bytes),
    Streaming(NativeStream<stream_endpoint::StreamEndpointHandle>),
    WebSocket(crate::websocket::WebSocketUpgrade),
}

impl IngressResponse {
    pub(super) fn json(status: StatusCode, body: &'static str) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        Self {
            status,
            headers,
            body: IngressBody::Buffered(Bytes::from_static(body.as_bytes())),
        }
    }
}

impl IngressResponse {
    pub(super) fn into_middleware_response(self) -> (WebIngressResponse, Option<IngressBody>) {
        let (body, stream) = match self.body {
            IngressBody::Buffered(body) => (body, None),
            body @ (IngressBody::Streaming(_) | IngressBody::WebSocket(_)) => {
                (Bytes::new(), Some(body))
            }
        };
        let mut response = Response::new(body);
        *response.status_mut() = self.status;
        *response.headers_mut() = self.headers;
        (response, stream)
    }

    pub(super) fn from_middleware_response(response: WebIngressResponse) -> Self {
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
            body: IngressBody::Buffered(body),
        }
    }

    pub(super) fn with_session(mut self, body: IngressBody) -> Self {
        if let IngressBody::WebSocket(upgrade) = &body {
            if self.status != StatusCode::SWITCHING_PROTOCOLS {
                upgrade.session.cancel();
                return self;
            }
            self.headers
                .insert(http::header::UPGRADE, HeaderValue::from_static("websocket"));
            self.headers.insert(
                http::header::CONNECTION,
                HeaderValue::from_static("Upgrade"),
            );
            self.headers.insert(
                "sec-websocket-accept",
                upgrade.accept_key.parse().expect("validated accept key"),
            );
            if let Some(protocol) = upgrade.session.protocol() {
                self.headers.insert(
                    "sec-websocket-protocol",
                    protocol.parse().expect("validated protocol"),
                );
            }
        }
        self.body = body;
        self
    }
}

#[cfg(feature = "native")]
pub(super) async fn acquire_request_permit<'a>(
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

pub(super) struct InboundRequestControl {
    cancellation: CancellationToken,
    credential: Option<CredentialEvidence>,
    csrf_header_name: Option<HeaderName>,
    disconnected: oneshot::Receiver<()>,
    request_id: String,
}

pub(super) fn middleware_request(
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

pub(super) fn restore_inbound_request(
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

pub(super) fn dispatch_response(
    result: Result<DispatchResponse, DispatchError>,
) -> IngressResponse {
    match result {
        Ok(DispatchResponse::Buffered(response)) => from_endpoint(response),
        Ok(DispatchResponse::Streaming(response)) => from_stream_endpoint(response),
        Ok(DispatchResponse::WebSocket(upgrade)) => IngressResponse {
            status: StatusCode::SWITCHING_PROTOCOLS,
            headers: HeaderMap::new(),
            body: IngressBody::WebSocket(upgrade),
        },
        Err(DispatchError::Unauthorized) => {
            IngressResponse::json(StatusCode::UNAUTHORIZED, r#"{"error":"unauthorized"}"#)
        }
        Err(DispatchError::Forbidden) => {
            IngressResponse::json(StatusCode::FORBIDDEN, r#"{"error":"forbidden"}"#)
        }
        Err(DispatchError::UpgradeRequired) => IngressResponse::json(
            StatusCode::UPGRADE_REQUIRED,
            r#"{"error":"websocket_upgrade_required"}"#,
        ),
        Err(DispatchError::BadHandshake) => IngressResponse::json(
            StatusCode::BAD_REQUEST,
            r#"{"error":"invalid_websocket_handshake"}"#,
        ),
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

pub(super) fn from_stream_endpoint(response: crate::routing::StreamingResponse) -> IngressResponse {
    let Some(status) = u16::try_from(response.status)
        .ok()
        .and_then(|status| StatusCode::from_u16(status).ok())
    else {
        response.stream.cancel();
        return invalid_endpoint_response();
    };
    let mut headers = HeaderMap::with_capacity(response.headers.len());
    for header in response.headers {
        let Ok(name) = HeaderName::from_bytes(header.name.as_bytes()) else {
            response.stream.cancel();
            return invalid_endpoint_response();
        };
        if is_ingress_owned_response_header(&name) {
            response.stream.cancel();
            return invalid_endpoint_response();
        }
        let Ok(value) = HeaderValue::from_str(&header.value) else {
            response.stream.cancel();
            return invalid_endpoint_response();
        };
        headers.append(name, value);
    }
    IngressResponse {
        status,
        headers,
        body: IngressBody::Streaming(response.stream),
    }
}

pub(super) fn mark_sensitive_headers(
    headers: &mut HeaderMap,
    session_cookie: Option<&SessionCookiePolicy>,
) {
    for (name, value) in headers.iter_mut() {
        if name == AUTHORIZATION
            || name == COOKIE
            || session_cookie.is_some_and(|policy| name == policy.csrf_header_name())
        {
            value.set_sensitive(true);
        }
    }
}

pub(super) fn replace_request_id(headers: &mut HeaderMap, next: &RequestIdSequence) -> HeaderValue {
    let request_id = request_id_header_value(next.next());
    headers.insert(REQUEST_ID_HEADER, request_id.clone());
    request_id
}

pub(super) fn connection_owned_headers(
    headers: &HeaderMap,
) -> Result<HashSet<HeaderName>, RequestRejection> {
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

pub(super) fn is_static_hop_by_hop_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("te")
        || name.eq_ignore_ascii_case("trailer")
        || name.eq_ignore_ascii_case("transfer-encoding")
        || name.eq_ignore_ascii_case("upgrade")
        || name.eq_ignore_ascii_case("keep-alive")
        || name.eq_ignore_ascii_case("proxy-connection")
}

pub(super) fn is_static_hop_by_hop_header(name: &HeaderName) -> bool {
    name == CONNECTION
        || name == TE
        || name == TRAILER
        || name == TRANSFER_ENCODING
        || name == UPGRADE
        || name.as_str() == "keep-alive"
        || name.as_str() == "proxy-connection"
}

pub(super) fn is_filtered_request_header(name: &HeaderName) -> bool {
    name == AUTHORIZATION
        || name == COOKIE
        || is_static_hop_by_hop_header(name)
        || name == CONTENT_LENGTH
        || name == HOST
        || name == REQUEST_ID_HEADER
}

pub(super) fn is_ingress_owned_response_header(name: &HeaderName) -> bool {
    is_static_hop_by_hop_header(name)
        || name == CONTENT_LENGTH
        || name == NOSNIFF_HEADER
        || name == REQUEST_ID_HEADER
}

pub(super) fn inbound_request(
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

pub(super) fn normalized_method(method: &Method) -> Method {
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

pub(super) fn from_endpoint(response: HandleResponse) -> IngressResponse {
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
        body: IngressBody::Buffered(body),
    }
}

pub(super) fn canonical_request_head_len<B>(request: &Request<B>) -> usize {
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

pub(super) fn with_transport_headers<B>(
    mut response: Response<B>,
    request_id: HeaderValue,
) -> Response<B> {
    response
        .headers_mut()
        .insert(REQUEST_ID_HEADER.clone(), request_id);
    response
        .headers_mut()
        .insert(NOSNIFF_HEADER, HeaderValue::from_static("nosniff"));
    response
}

pub(super) fn serialized_uri_len(uri: &Uri) -> usize {
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

pub(super) fn request_id(headers: &HeaderMap) -> Result<String, RequestRejection> {
    let request_id = headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(RequestRejection::BadRequest)?;
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(RequestRejection::BadRequest);
    }
    Ok(request_id.to_owned())
}

pub(super) fn bad_request() -> IngressResponse {
    IngressResponse::json(StatusCode::BAD_REQUEST, r#"{"error":"bad_request"}"#)
}

pub(super) fn csrf_forbidden() -> IngressResponse {
    IngressResponse::json(StatusCode::FORBIDDEN, r#"{"error":"csrf_rejected"}"#)
}

pub(super) fn request_rejection(rejection: RequestRejection) -> IngressResponse {
    match rejection {
        RequestRejection::BadRequest => bad_request(),
        RequestRejection::CsrfForbidden => csrf_forbidden(),
    }
}

pub(super) fn payload_too_large() -> IngressResponse {
    IngressResponse::json(
        StatusCode::PAYLOAD_TOO_LARGE,
        r#"{"error":"payload_too_large"}"#,
    )
}

#[cfg(feature = "native")]
pub(super) fn request_timeout() -> IngressResponse {
    IngressResponse::json(
        StatusCode::REQUEST_TIMEOUT,
        r#"{"error":"request_timeout"}"#,
    )
}

pub(super) fn method_not_allowed(allowed: &[Method]) -> IngressResponse {
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

pub(super) fn unavailable() -> IngressResponse {
    IngressResponse::json(
        StatusCode::SERVICE_UNAVAILABLE,
        r#"{"error":"endpoint_unavailable"}"#,
    )
}

pub(super) fn invalid_endpoint_response() -> IngressResponse {
    IngressResponse::json(
        StatusCode::BAD_GATEWAY,
        r#"{"error":"invalid_endpoint_response"}"#,
    )
}

pub(super) async fn dispatch_buffered(
    parts: http::request::Parts,
    body: Bytes,
    cancellation: CancellationToken,
    session_cookie: Option<&SessionCookiePolicy>,
    routes: Rc<RouteTable>,
    middleware: &[Rc<dyn WebIngressMiddleware>],
) -> IngressResponse {
    if parts.method == Method::CONNECT
        || (parts.headers.contains_key(UPGRADE) && routes.websocket_policy().is_none())
    {
        return IngressResponse::json(
            StatusCode::NOT_IMPLEMENTED,
            r#"{"error":"unsupported_interaction"}"#,
        );
    }
    let handshake = if parts.headers.contains_key(UPGRADE) {
        if parts.method != Method::GET
            || parts.version != http::Version::HTTP_11
            || !body.is_empty()
        {
            return bad_request();
        }
        match crate::websocket_handshake::Handshake::parse(
            &parts.headers,
            routes
                .websocket_policy()
                .expect("checked policy")
                .allowed_origins(),
        ) {
            Ok(handshake) => Some(handshake),
            Err(()) => return bad_request(),
        }
    } else {
        None
    };
    let (disconnect, disconnected) = oneshot::channel();
    let cancel_on_drop = CancelRequestOnDrop(Some(disconnect));
    let request = match inbound_request(
        &parts.method,
        &parts.uri,
        &parts.headers,
        body,
        cancellation,
        disconnected,
        session_cookie,
    ) {
        Ok(request) => request,
        Err(rejection) => return request_rejection(rejection),
    };
    let (request, control) = middleware_request(request, parts.version);
    let stream_slot = Rc::new(RefCell::new(None));
    let dispatch_stream_slot = stream_slot.clone();
    let response = AssertUnwindSafe(middleware::run(middleware, request, move |request| {
        let request = restore_inbound_request(request, control);
        let stream_slot = dispatch_stream_slot.clone();
        async move {
            let response = match request {
                Ok(request) => dispatch_response(routes.dispatch(request, handshake).await),
                Err(rejection) => request_rejection(rejection),
            };
            let (response, stream) = response.into_middleware_response();
            *stream_slot.borrow_mut() = stream;
            response
        }
    }))
    .catch_unwind()
    .await
    .ok()
    .and_then(Result::ok)
    .map_or_else(unavailable, |response| {
        let response = IngressResponse::from_middleware_response(response);
        let stream = stream_slot.borrow_mut().take();
        match stream {
            Some(stream) => response.with_session(stream),
            None => response,
        }
    });
    drop(cancel_on_drop);
    response
}
impl IngressResponse {
    pub(super) fn normalize_body(mut self, method: &Method) -> Self {
        if method == Method::HEAD
            || (self.status.is_informational() && !matches!(&self.body, IngressBody::WebSocket(_)))
            || self.status == StatusCode::NO_CONTENT
            || self.status == StatusCode::NOT_MODIFIED
        {
            if let IngressBody::Streaming(stream) = &self.body {
                stream.cancel();
            }
            if let IngressBody::WebSocket(upgrade) = &self.body {
                upgrade.session.cancel();
            }
            self.body = IngressBody::Buffered(Bytes::new());
        }
        self
    }
}
