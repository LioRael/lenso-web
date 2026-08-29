//! Typed response construction for authored HTTP Endpoint handlers.

use std::{error::Error, fmt};

pub use http::{HeaderName, HeaderValue, StatusCode, header};
use lenso_kernel::RuntimeFailure;
use serde::Serialize;

use crate::{
    EndpointHandleInvocationError, HandleError, HandleResponse, HandleResponseHeadersItem, Json,
};

const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
const PROBLEM_CONTENT_TYPE: &str = "application/problem+json; charset=utf-8";
const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// An authoring-time failure while constructing an HTTP response.
#[derive(Debug)]
pub enum ResponseBuildError {
    /// A typed response value could not be serialized as JSON.
    Json(serde_json::Error),
    /// The portable Endpoint contract cannot represent a binary header value.
    NonTextHeaderValue(http::header::ToStrError),
}

impl fmt::Display for ResponseBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(formatter, "response JSON serialization failed: {error}"),
            Self::NonTextHeaderValue(error) => {
                write!(formatter, "response header is not valid text: {error}")
            }
        }
    }
}

impl Error for ResponseBuildError {}

impl From<serde_json::Error> for ResponseBuildError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl From<ResponseBuildError> for EndpointHandleInvocationError {
    fn from(error: ResponseBuildError) -> Self {
        Self::Runtime(RuntimeFailure::Internal {
            detail: error.to_string(),
        })
    }
}

/// Converts one typed handler value into the portable HTTP response contract.
pub trait IntoResponse {
    /// Builds the response or preserves a serialization/header failure.
    fn into_response(self) -> Result<HandleResponse, ResponseBuildError>;
}

impl IntoResponse for HandleResponse {
    fn into_response(self) -> Result<HandleResponse, ResponseBuildError> {
        Ok(self)
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Result<HandleResponse, ResponseBuildError> {
        json(StatusCode::OK, &self.0)
    }
}

impl<T> IntoResponse for (StatusCode, T)
where
    T: IntoResponse,
{
    fn into_response(self) -> Result<HandleResponse, ResponseBuildError> {
        let (status, body) = self;
        let mut response = body.into_response()?;
        response.status = i64::from(status.as_u16());
        Ok(response)
    }
}

/// One intentional RFC 9457-compatible HTTP problem returned by a handler.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Problem {
    #[serde(rename = "type")]
    type_uri: &'static str,
    title: String,
    status: u16,
    detail: String,
    code: String,
}

impl Problem {
    /// Creates a problem with a stable machine-readable code.
    #[must_use]
    pub fn new(status: StatusCode, code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            type_uri: "about:blank",
            title: status.canonical_reason().unwrap_or("HTTP error").to_owned(),
            status: status.as_u16(),
            detail: detail.into(),
            code: code.into(),
        }
    }
}

impl IntoResponse for Problem {
    fn into_response(self) -> Result<HandleResponse, ResponseBuildError> {
        Ok(with_content_type(
            StatusCode::from_u16(self.status).expect("Problem stores a validated HTTP status"),
            PROBLEM_CONTENT_TYPE,
            serde_json::to_vec(&self).expect("serializing a string-only problem cannot fail"),
        ))
    }
}

/// Converts a typed handler error into an intentional response or Capability failure.
#[doc(hidden)]
pub trait IntoEndpointError {
    fn into_endpoint_error(self) -> Result<HandleResponse, EndpointHandleInvocationError>;
}

impl<T> IntoEndpointError for T
where
    T: IntoResponse,
{
    fn into_endpoint_error(self) -> Result<HandleResponse, EndpointHandleInvocationError> {
        self.into_response().map_err(Into::into)
    }
}

impl IntoEndpointError for EndpointHandleInvocationError {
    fn into_endpoint_error(self) -> Result<HandleResponse, EndpointHandleInvocationError> {
        Err(self)
    }
}

/// Lowers one typed handler result into the generated Endpoint contract.
#[doc(hidden)]
pub trait IntoEndpointResult {
    fn into_endpoint_result(self) -> Result<Result<HandleResponse, HandleError>, RuntimeFailure>;
}

impl<T, E> IntoEndpointResult for Result<T, E>
where
    T: IntoResponse,
    E: IntoEndpointError,
{
    fn into_endpoint_result(self) -> Result<Result<HandleResponse, HandleError>, RuntimeFailure> {
        match self {
            Ok(response) => {
                response
                    .into_response()
                    .map(Ok)
                    .map_err(|error| RuntimeFailure::Internal {
                        detail: error.to_string(),
                    })
            }
            Err(error) => match error.into_endpoint_error() {
                Ok(response) => Ok(Ok(response)),
                Err(EndpointHandleInvocationError::Domain(error)) => Ok(Err(error)),
                Err(EndpointHandleInvocationError::Runtime(error)) => Err(error),
            },
        }
    }
}

/// Serializes a typed value and returns a JSON response.
pub fn json(
    status: StatusCode,
    body: &impl Serialize,
) -> Result<HandleResponse, ResponseBuildError> {
    Ok(with_content_type(
        status,
        JSON_CONTENT_TYPE,
        serde_json::to_vec(body)?,
    ))
}

/// Returns an RFC 9457-compatible problem response with a stable extension code.
pub fn problem(
    status: StatusCode,
    code: impl Into<String>,
    detail: impl Into<String>,
) -> HandleResponse {
    Problem::new(status, code, detail)
        .into_response()
        .expect("serializing a string-only problem cannot fail")
}

/// Returns a UTF-8 plain-text response.
#[must_use]
pub fn text(status: StatusCode, body: impl Into<String>) -> HandleResponse {
    with_content_type(status, TEXT_CONTENT_TYPE, body.into().into_bytes())
}

/// Returns an empty response without a representation content type.
#[must_use]
pub fn empty(status: StatusCode) -> HandleResponse {
    HandleResponse {
        body: Vec::new().into(),
        headers: Vec::new(),
        status: i64::from(status.as_u16()),
    }
}

impl HandleResponse {
    /// Adds one validated HTTP header to an authored response.
    pub fn with_header(
        mut self,
        name: &HeaderName,
        value: &HeaderValue,
    ) -> Result<Self, ResponseBuildError> {
        self.headers.push(HandleResponseHeadersItem {
            name: name.as_str().to_owned(),
            value: value
                .to_str()
                .map_err(ResponseBuildError::NonTextHeaderValue)?
                .to_owned(),
        });
        Ok(self)
    }
}

fn with_content_type(
    status: StatusCode,
    content_type: &'static str,
    body: Vec<u8>,
) -> HandleResponse {
    HandleResponse {
        body: body.into(),
        headers: vec![HandleResponseHeadersItem {
            name: header::CONTENT_TYPE.as_str().to_owned(),
            value: content_type.to_owned(),
        }],
        status: i64::from(status.as_u16()),
    }
}
