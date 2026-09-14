//! Shared outbound authority, header and transfer validation.
use crate::config::{HttpEgressConfig, request_origin};
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use lenso_capability_http_client::{
    ClientInvocationError, SendError, SendRequest, SendResponseHeadersItem,
};
use std::collections::BTreeSet;
use url::Url;
const FORBIDDEN_REQUEST_HEADERS: &[&str] = &[
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub(crate) struct PreparedRequest {
    pub(crate) method: Method,
    pub(crate) url: Url,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

pub(crate) fn prepare_request(
    config: &HttpEgressConfig,
    allowed_origins: &BTreeSet<String>,
    request: SendRequest,
) -> Result<PreparedRequest, ClientInvocationError> {
    if request.method.len() > 32 || request.url.len() > 4_096 {
        return Err(invalid_request());
    }
    let method = Method::from_bytes(request.method.as_bytes()).map_err(|_| invalid_request())?;
    if matches!(method, Method::CONNECT | Method::TRACE) {
        return Err(invalid_request());
    }
    let url = Url::parse(&request.url).map_err(|_| invalid_request())?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(invalid_request());
    }
    let origin = request_origin(&url).ok_or_else(invalid_request)?;
    if !allowed_origins.contains(&origin) {
        return Err(ClientInvocationError::Domain(
            SendError::DestinationNotAllowed,
        ));
    }
    let body = request.body.into_shared();
    if body.len() > config.max_request_body_bytes() {
        return Err(ClientInvocationError::Domain(SendError::RequestTooLarge));
    }
    let headers = parse_request_headers(&request.headers, config.max_request_head_bytes())?;
    Ok(PreparedRequest {
        method,
        url,
        headers,
        body,
    })
}

fn parse_request_headers(
    headers: &[lenso_capability_http_client::SendRequestHeadersItem],
    max_head_bytes: usize,
) -> Result<HeaderMap, ClientInvocationError> {
    let mut parsed = HeaderMap::new();
    let mut head_bytes = 0_usize;
    for header in headers {
        let name = HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| invalid_request())?;
        if FORBIDDEN_REQUEST_HEADERS.contains(&name.as_str()) {
            return Err(invalid_request());
        }
        let value = HeaderValue::from_str(&header.value).map_err(|_| invalid_request())?;
        head_bytes = head_bytes
            .checked_add(name.as_str().len() + value.as_bytes().len() + 4)
            .ok_or_else(request_too_large)?;
        if head_bytes > max_head_bytes {
            return Err(request_too_large());
        }
        parsed.append(name, value);
    }
    Ok(parsed)
}

pub(crate) fn response_headers(
    headers: &HeaderMap,
    max_head_bytes: usize,
) -> Result<Vec<SendResponseHeadersItem>, ClientInvocationError> {
    let mut connection_headers = BTreeSet::new();
    for value in headers.get_all("connection") {
        let value = value
            .to_str()
            .map_err(|_| ClientInvocationError::Domain(SendError::TransportFailure))?;
        for name in value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ClientInvocationError::Domain(SendError::TransportFailure))?;
            connection_headers.insert(name.as_str().to_owned());
        }
    }
    let mut result = Vec::with_capacity(headers.len());
    let mut head_bytes = 0_usize;
    for (name, value) in headers {
        if FORBIDDEN_REQUEST_HEADERS.contains(&name.as_str())
            || connection_headers.contains(name.as_str())
        {
            continue;
        }
        head_bytes = head_bytes
            .checked_add(name.as_str().len() + value.as_bytes().len() + 4)
            .ok_or_else(response_too_large)?;
        if head_bytes > max_head_bytes {
            return Err(response_too_large());
        }
        result.push(SendResponseHeadersItem {
            name: name.as_str().to_owned(),
            value: value
                .to_str()
                .map_err(|_| ClientInvocationError::Domain(SendError::TransportFailure))?
                .to_owned(),
        });
    }
    Ok(result)
}

fn invalid_request() -> ClientInvocationError {
    ClientInvocationError::Domain(SendError::InvalidRequest)
}

fn request_too_large() -> ClientInvocationError {
    ClientInvocationError::Domain(SendError::RequestTooLarge)
}

fn response_too_large() -> ClientInvocationError {
    ClientInvocationError::Domain(SendError::ResponseTooLarge)
}
