use std::{
    collections::HashSet,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri,
        header::{
            AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST, TE, TRAILER,
            TRANSFER_ENCODING, UPGRADE,
        },
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use lenso_capability_http_endpoint::HandleResponse;
use lenso_kernel::CancellationToken;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
};
use tower::{ServiceBuilder, limit::GlobalConcurrencyLimitLayer};
use tower_http::{
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    set_header::SetResponseHeaderLayer,
};

use crate::{WebIngressConfig, routing::DispatchError, routing::RouteTable};

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
    pub(super) credential: Option<CredentialEvidence>,
    pub(super) headers: Vec<InboundHeader>,
    pub(super) method: Method,
    pub(super) path: String,
    pub(super) query: Option<String>,
    pub(super) request_id: String,
}

#[derive(Debug)]
struct IngressCall {
    request: InboundRequest,
    response: oneshot::Sender<IngressResponse>,
}

#[derive(Clone, Debug)]
struct IngressBridge {
    sender: mpsc::Sender<IngressCall>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestRejection {
    BadRequest,
}

#[derive(Clone, Debug, Default)]
struct MakeIngressRequestId {
    next: Arc<AtomicU64>,
}

impl MakeRequestId for MakeIngressRequestId {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let value = self.next.fetch_add(1, Ordering::Relaxed);
        let value = request_id_header_value(value);
        Some(RequestId::new(value))
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
    body: Vec<u8>,
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
            body: body.as_bytes().to_vec(),
        }
    }
}

impl IntoResponse for IngressResponse {
    fn into_response(self) -> Response {
        (self.status, self.headers, self.body).into_response()
    }
}

pub(super) async fn serve(
    listener: TcpListener,
    config: WebIngressConfig,
    routes: std::rc::Rc<RouteTable>,
    cancellation: CancellationToken,
) {
    let (sender, receiver) = mpsc::channel(config.max_concurrent_requests);
    let transport = ServiceBuilder::new()
        .layer(SetSensitiveRequestHeadersLayer::new([
            AUTHORIZATION,
            COOKIE,
        ]))
        .layer(SetRequestIdLayer::new(
            REQUEST_ID_HEADER.clone(),
            MakeIngressRequestId::default(),
        ))
        .layer(PropagateRequestIdLayer::new(REQUEST_ID_HEADER))
        .layer(SetResponseHeaderLayer::overriding(
            NOSNIFF_HEADER,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(RequestBodyLimitLayer::new(config.max_request_body_bytes))
        .layer(GlobalConcurrencyLimitLayer::new(
            config.max_concurrent_requests,
        ));
    let app = Router::new()
        .fallback(forward)
        .with_state(IngressBridge { sender })
        .layer(middleware::from_fn_with_state(
            config.max_request_head_bytes,
            enforce_head_limit,
        ))
        .layer(transport);
    let (shutdown, mut shutdown_signal) = watch::channel(false);
    let cancellation_bridge = async move {
        cancellation.cancelled().await;
        let _ = shutdown.send(true);
    };
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        while !*shutdown_signal.borrow_and_update() {
            if shutdown_signal.changed().await.is_err() {
                return;
            }
        }
    });
    let dispatcher = dispatch_requests(receiver, routes);
    let (server, (), ()) = tokio::join!(server, dispatcher, cancellation_bridge);
    let _ = server;
}

async fn forward(
    State(bridge): State<IngressBridge>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = match inbound_request(&method, &uri, &headers, body) {
        Ok(request) => request,
        Err(RequestRejection::BadRequest) => return bad_request().into_response(),
    };
    let (response, receive) = oneshot::channel();
    if bridge
        .sender
        .send(IngressCall { request, response })
        .await
        .is_err()
    {
        return unavailable().into_response();
    }
    receive
        .await
        .unwrap_or_else(|_| unavailable())
        .into_response()
}

async fn dispatch_requests(
    mut receiver: mpsc::Receiver<IngressCall>,
    routes: std::rc::Rc<RouteTable>,
) {
    let mut calls = FuturesUnordered::new();
    let mut accepting = true;
    loop {
        tokio::select! {
            call = receiver.recv(), if accepting => {
                match call {
                    Some(call) => {
                        let routes = routes.clone();
                        calls.push(AssertUnwindSafe(dispatch_request(call, routes)).catch_unwind());
                    }
                    None => accepting = false,
                }
            }
            Some(_) = calls.next(), if !calls.is_empty() => {}
            else => break,
        }
    }
}

async fn dispatch_request(call: IngressCall, routes: std::rc::Rc<RouteTable>) {
    let response = match routes.dispatch(call.request).await {
        Ok(response) => from_endpoint(response),
        Err(DispatchError::NotFound) => {
            IngressResponse::json(StatusCode::NOT_FOUND, r#"{"error":"not_found"}"#)
        }
        Err(DispatchError::MethodNotAllowed) => IngressResponse::json(
            StatusCode::METHOD_NOT_ALLOWED,
            r#"{"error":"method_not_allowed"}"#,
        ),
        Err(DispatchError::Rejected) => {
            IngressResponse::json(StatusCode::BAD_GATEWAY, r#"{"error":"endpoint_rejected"}"#)
        }
        Err(DispatchError::Unavailable) => unavailable(),
    };
    let _ = call.response.send(response);
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
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| RequestRejection::BadRequest)?;
            if !is_static_hop_by_hop_header(&name) {
                owned.insert(name);
            }
        }
    }
    Ok(owned)
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
        credential,
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
    let body = response.body.into_vec();
    let mut headers = HeaderMap::new();
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

async fn enforce_head_limit(
    State(max_request_head_bytes): State<usize>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let size = request.method().as_str().len()
        + serialized_uri_len(request.uri())
        + request
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum::<usize>();
    if size > max_request_head_bytes {
        return IngressResponse::json(
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            r#"{"error":"request_header_fields_too_large"}"#,
        )
        .into_response();
    }
    next.run(request).await
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
    use super::{request_id_header_value, serialized_uri_len};
    use axum::http::Uri;

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
}
