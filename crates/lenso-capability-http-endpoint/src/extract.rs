//! Typed extraction from the portable HTTP Endpoint request.

use futures::future::LocalBoxFuture;
use lenso_kernel::InvocationContext;
use serde::de::DeserializeOwned;

use crate::{EndpointHandleInvocationError, HandleRequest, HandleResponse, response};

/// A typed extractor rejection before the authored handler runs.
#[derive(Debug)]
pub enum ExtractorRejection {
    /// Returns one intentional HTTP response.
    Response(HandleResponse),
    /// Preserves a Domain Error or Runtime Failure from asynchronous extraction.
    Invocation(EndpointHandleInvocationError),
}

impl From<HandleResponse> for ExtractorRejection {
    fn from(response: HandleResponse) -> Self {
        Self::Response(response)
    }
}

impl From<EndpointHandleInvocationError> for ExtractorRejection {
    fn from(error: EndpointHandleInvocationError) -> Self {
        Self::Invocation(error)
    }
}

impl From<response::ResponseBuildError> for ExtractorRejection {
    fn from(error: response::ResponseBuildError) -> Self {
        Self::Invocation(error.into())
    }
}

/// Boxed local extraction result used by an authored Endpoint handler.
pub type ExtractorFuture<'a, T> = LocalBoxFuture<'a, Result<T, ExtractorRejection>>;

/// Extracts one typed handler argument from an inbound HTTP request.
///
/// Extractors may inspect the Endpoint provider, await explicitly bound
/// Capability clients, and enrich the invocation context for later extractors
/// and the handler. They must not perform the target Plugin's final business
/// authorization decision.
pub trait FromRequest<P: ?Sized>: Sized {
    /// Extracts this value or rejects dispatch before the handler runs.
    fn from_request<'a>(
        provider: &'a P,
        context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self>;
}

/// A JSON request body decoded into `T`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Json<T>(pub T);

impl<P, T> FromRequest<P> for Json<T>
where
    P: ?Sized,
    T: DeserializeOwned + 'static,
{
    fn from_request<'a>(
        _provider: &'a P,
        _context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self> {
        Box::pin(futures::future::ready(extract_json(request)))
    }
}

/// Route path parameters decoded into `T` by field name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Path<T>(pub T);

impl<P, T> FromRequest<P> for Path<T>
where
    P: ?Sized,
    T: DeserializeOwned + 'static,
{
    fn from_request<'a>(
        _provider: &'a P,
        _context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self> {
        Box::pin(futures::future::ready(extract_path(request)))
    }
}

/// The URL query parameters decoded into `T`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryParams<T>(pub T);

impl<P, T> FromRequest<P> for QueryParams<T>
where
    P: ?Sized,
    T: DeserializeOwned + 'static,
{
    fn from_request<'a>(
        _provider: &'a P,
        _context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self> {
        Box::pin(futures::future::ready(extract_query(request)))
    }
}

/// The trusted request identifier assigned by Web Ingress.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestId(pub String);

impl<P> FromRequest<P> for RequestId
where
    P: ?Sized,
{
    fn from_request<'a>(
        _provider: &'a P,
        _context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self> {
        Box::pin(futures::future::ready(Ok(Self(request.request_id.clone()))))
    }
}

fn extract_json<T>(request: &HandleRequest) -> Result<Json<T>, ExtractorRejection>
where
    T: DeserializeOwned,
{
    if !has_json_content_type(request) {
        return Err(response::problem(
            response::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "json_content_type_required",
            "The request content type must be application/json.",
        )
        .into());
    }
    serde_json::from_slice(request.body.as_ref())
        .map(Json)
        .map_err(|_| {
            response::problem(
                response::StatusCode::BAD_REQUEST,
                "invalid_json_body",
                "The request body is not valid JSON for this endpoint.",
            )
            .into()
        })
}

fn extract_path<T>(request: &HandleRequest) -> Result<Path<T>, ExtractorRejection>
where
    T: DeserializeOwned,
{
    let parameters = request
        .path_parameters
        .iter()
        .map(|parameter| (parameter.name.as_str(), parameter.value.as_str()))
        .collect::<Vec<_>>();
    let encoded = serde_urlencoded::to_string(parameters).map_err(|_| invalid_path())?;
    serde_urlencoded::from_str(&encoded)
        .map(Path)
        .map_err(|_| invalid_path())
}

fn extract_query<T>(request: &HandleRequest) -> Result<QueryParams<T>, ExtractorRejection>
where
    T: DeserializeOwned,
{
    serde_urlencoded::from_str(request.query.as_deref().unwrap_or_default())
        .map(QueryParams)
        .map_err(|_| {
            response::problem(
                response::StatusCode::BAD_REQUEST,
                "invalid_query",
                "The query string is not valid for this endpoint.",
            )
            .into()
        })
}

fn has_json_content_type(request: &HandleRequest) -> bool {
    request.headers.iter().any(|header| {
        header.name.eq_ignore_ascii_case("content-type")
            && header
                .value
                .split(';')
                .next()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    })
}

fn invalid_path() -> ExtractorRejection {
    response::problem(
        response::StatusCode::BAD_REQUEST,
        "invalid_path_parameters",
        "The route path parameters are not valid for this endpoint.",
    )
    .into()
}
