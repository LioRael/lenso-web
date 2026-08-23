use std::{cell::Cell, collections::HashSet, convert::Infallible, panic::AssertUnwindSafe, rc::Rc};

use bytes::Bytes;
use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri,
    header::{
        AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, COOKIE, HOST, TE, TRAILER,
        TRANSFER_ENCODING, UPGRADE,
    },
};
use http_body_util::{BodyExt as _, Full, LengthLimitError, Limited};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use lenso_capability_http_endpoint::HandleResponse;
use lenso_kernel::CancellationToken;
use tokio::{net::TcpListener, sync::Semaphore};

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestRejection {
    BadRequest,
}

#[derive(Clone, Debug)]
struct IngressService {
    config: WebIngressConfig,
    routes: Rc<RouteTable>,
    concurrency: Rc<Semaphore>,
    next_request_id: Rc<Cell<u64>>,
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
    fn empty(status: StatusCode) -> Self {
        Self {
            status,
            headers: HeaderMap::new(),
            body: Bytes::new(),
        }
    }

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

pub(super) async fn serve(
    listener: TcpListener,
    config: WebIngressConfig,
    routes: std::rc::Rc<RouteTable>,
    cancellation: CancellationToken,
) {
    let service = IngressService {
        concurrency: Rc::new(Semaphore::new(config.max_concurrent_requests)),
        config,
        routes,
        next_request_id: Rc::new(Cell::new(0)),
    };
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let mut connections = FuturesUnordered::new();
    loop {
        tokio::select! {
            () = cancellation.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    break;
                };
                let connection_service = service.clone();
                let mut shutdown_signal = shutdown.subscribe();
                connections.push(tokio::task::spawn_local(async move {
                    let connection = http1::Builder::new().serve_connection(
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
            Some(_) = connections.next(), if !connections.is_empty() => {}
        }
    }
    shutdown.send_replace(true);
    while connections.next().await.is_some() {}
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
        let _permit = self
            .concurrency
            .acquire()
            .await
            .expect("the Ingress concurrency semaphore remains open");
        mark_sensitive_headers(request.headers_mut());
        let request_id = ensure_request_id(request.headers_mut(), &self.next_request_id);
        let mut response = if request_head_size(&request) > self.config.max_request_head_bytes {
            IngressResponse::json(
                StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                r#"{"error":"request_header_fields_too_large"}"#,
            )
        } else {
            self.dispatch(request).await
        }
        .into_response();
        response.headers_mut().insert(REQUEST_ID_HEADER, request_id);
        response
            .headers_mut()
            .insert(NOSNIFF_HEADER, HeaderValue::from_static("nosniff"));
        Ok(response)
    }

    async fn dispatch(&self, request: Request<Incoming>) -> IngressResponse {
        let (parts, body) = request.into_parts();
        let content_length = parts
            .headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()?.parse::<usize>().ok());
        if content_length.is_some_and(|length| length > self.config.max_request_body_bytes) {
            return payload_too_large();
        }
        let body_limit = content_length.map_or(self.config.max_request_body_bytes, |length| {
            length.min(self.config.max_request_body_bytes)
        });
        let body = match Limited::new(body, body_limit).collect().await {
            Ok(body) => body.to_bytes(),
            Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => {
                return payload_too_large();
            }
            Err(_) => return bad_request(),
        };
        let request = match inbound_request(&parts.method, &parts.uri, &parts.headers, body) {
            Ok(request) => request,
            Err(RequestRejection::BadRequest) => return bad_request(),
        };
        match AssertUnwindSafe(self.routes.dispatch(request))
            .catch_unwind()
            .await
        {
            Ok(result) => dispatch_response(result),
            Err(_) => unavailable(),
        }
    }
}

fn dispatch_response(result: Result<HandleResponse, DispatchError>) -> IngressResponse {
    match result {
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
    }
}

fn mark_sensitive_headers(headers: &mut HeaderMap) {
    for (name, value) in headers.iter_mut() {
        if name == AUTHORIZATION || name == COOKIE {
            value.set_sensitive(true);
        }
    }
}

fn ensure_request_id(headers: &mut HeaderMap, next: &Cell<u64>) -> HeaderValue {
    if let Some(request_id) = headers.get(REQUEST_ID_HEADER) {
        return request_id.clone();
    }
    let value = next.get();
    next.set(value.wrapping_add(1));
    let request_id = request_id_header_value(value);
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

fn request_head_size<B>(request: &Request<B>) -> usize {
    request.method().as_str().len()
        + serialized_uri_len(request.uri())
        + request
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum::<usize>()
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

fn payload_too_large() -> IngressResponse {
    IngressResponse::empty(StatusCode::PAYLOAD_TOO_LARGE)
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
