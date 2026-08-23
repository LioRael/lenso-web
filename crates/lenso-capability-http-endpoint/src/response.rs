//! Typed response construction for authored HTTP Endpoint handlers.

use std::{error::Error, fmt};

pub use http::{HeaderName, HeaderValue, StatusCode, header};
use lenso_kernel::RuntimeFailure;
use serde::Serialize;

use crate::{EndpointHandleInvocationError, HandleResponse, HandleResponseHeadersItem};

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
    #[derive(Serialize)]
    struct Problem {
        r#type: &'static str,
        title: String,
        status: u16,
        detail: String,
        code: String,
    }

    let code = code.into();
    let problem = Problem {
        r#type: "about:blank",
        title: status.canonical_reason().unwrap_or("HTTP error").to_owned(),
        status: status.as_u16(),
        detail: detail.into(),
        code,
    };
    with_content_type(
        status,
        PROBLEM_CONTENT_TYPE,
        serde_json::to_vec(&problem).expect("serializing a string-only problem cannot fail"),
    )
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
