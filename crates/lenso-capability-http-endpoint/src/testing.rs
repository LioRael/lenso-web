//! Socket-free testing tools for authored HTTP Endpoint Plugins.

use std::{error::Error, fmt};

use lenso_kernel::{CancellationToken, InvocationContext, RuntimeFailure};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    EndpointHandleInvocationError, EndpointProvider, HandleError, HandleRequest,
    HandleRequestHeadersItem, HandleRequestPathParametersItem, HandleResponse, HttpEndpoint,
    response::StatusCode,
};

/// A direct test harness for one authored Endpoint Plugin.
#[derive(Debug)]
pub struct EndpointTest<P> {
    provider: P,
}

impl<P> EndpointTest<P>
where
    P: HttpEndpoint,
{
    /// Creates a harness without starting Web Ingress or binding a socket.
    #[must_use]
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    /// Starts a request using the method and path declared by `route_id`.
    #[must_use]
    pub fn request(&self, route_id: impl Into<String>) -> TestRequest<'_, P> {
        TestRequest {
            provider: &self.provider,
            route_id: route_id.into(),
            body: Vec::new(),
            headers: Vec::new(),
            path_parameters: Vec::new(),
            query: None,
        }
    }
}

/// One direct request being prepared for an [`EndpointTest`].
#[derive(Debug)]
pub struct TestRequest<'a, P> {
    provider: &'a P,
    route_id: String,
    body: Vec<u8>,
    headers: Vec<HandleRequestHeadersItem>,
    path_parameters: Vec<HandleRequestPathParametersItem>,
    query: Option<String>,
}

impl<P> TestRequest<'_, P>
where
    P: HttpEndpoint,
{
    /// Serializes a JSON body and supplies its content type.
    pub fn json(mut self, value: &impl Serialize) -> Result<Self, serde_json::Error> {
        self.body = serde_json::to_vec(value)?;
        self.headers.push(HandleRequestHeadersItem {
            name: "content-type".to_owned(),
            value: "application/json".to_owned(),
        });
        Ok(self)
    }

    /// Encodes typed URL query parameters.
    pub fn query(mut self, value: &impl Serialize) -> Result<Self, serde_urlencoded::ser::Error> {
        self.query = Some(serde_urlencoded::to_string(value)?);
        Ok(self)
    }

    /// Supplies one path parameter and expands it in the declared path template.
    #[must_use]
    pub fn path_parameter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.path_parameters.push(HandleRequestPathParametersItem {
            name: name.into(),
            value: value.into(),
        });
        self
    }

    /// Supplies one request header.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push(HandleRequestHeadersItem {
            name: name.into(),
            value: value.into(),
        });
        self
    }

    /// Invokes the Endpoint directly and returns its intentional HTTP response.
    pub async fn send(self) -> Result<TestResponse, EndpointTestError> {
        let route = P::ROUTES
            .iter()
            .find(|route| route.route_id() == self.route_id)
            .ok_or_else(|| EndpointTestError::UnknownRoute(self.route_id.clone()))?;
        let path = self
            .path_parameters
            .iter()
            .fold(route.path().to_owned(), |path, parameter| {
                path.replace(&format!("{{{}}}", parameter.name), &parameter.value)
            });
        let request = HandleRequest {
            body: self.body.into(),
            credential: None,
            headers: self.headers,
            method: route.method().to_owned(),
            path,
            path_parameters: self.path_parameters,
            query: self.query,
            request_id: "endpoint-test-1".to_owned(),
            route_id: self.route_id,
        };
        let context = InvocationContext::new(1, None, CancellationToken::new());
        let response = self
            .provider
            .handle(context, request)
            .await
            .map_err(EndpointTestError::Runtime)?
            .map_err(EndpointTestError::Domain)?;
        Ok(TestResponse(response))
    }
}

/// A response returned by [`EndpointTest`].
#[derive(Clone, Debug)]
pub struct TestResponse(HandleResponse);

impl TestResponse {
    /// Returns the validated HTTP status.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        u16::try_from(self.0.status)
            .ok()
            .and_then(|status| StatusCode::from_u16(status).ok())
            .expect("an Endpoint response must contain a valid HTTP status")
    }

    /// Deserializes the response body as JSON.
    pub fn json<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(&self.0.body)
    }

    /// Returns the first response header matching `name`, ignoring ASCII case.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.0
            .headers
            .iter()
            .find(|header| header.name.eq_ignore_ascii_case(name))
            .map(|header| header.value.as_str())
    }

    /// Returns the raw portable Endpoint response.
    #[must_use]
    pub fn into_inner(self) -> HandleResponse {
        self.0
    }
}

/// A failure to construct or directly invoke an Endpoint test request.
#[derive(Debug)]
pub enum EndpointTestError {
    /// No authored route has the requested stable identifier.
    UnknownRoute(String),
    /// The Endpoint intentionally returned a Capability-domain error.
    Domain(HandleError),
    /// The Endpoint could not complete because its runtime failed.
    Runtime(RuntimeFailure),
}

impl fmt::Display for EndpointTestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownRoute(route_id) => {
                write!(formatter, "unknown Endpoint route `{route_id}`")
            }
            Self::Domain(error) => write!(formatter, "Endpoint domain error: {error:?}"),
            Self::Runtime(error) => write!(formatter, "Endpoint runtime failure: {error:?}"),
        }
    }
}

impl Error for EndpointTestError {}

impl From<EndpointHandleInvocationError> for EndpointTestError {
    fn from(error: EndpointHandleInvocationError) -> Self {
        match error {
            EndpointHandleInvocationError::Domain(error) => Self::Domain(error),
            EndpointHandleInvocationError::Runtime(error) => Self::Runtime(error),
        }
    }
}
