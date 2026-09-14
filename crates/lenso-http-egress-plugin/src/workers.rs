//! Adapter for the Web-owned abortable Fetch bridge. No ambient JS fetch lookup.
use crate::{HttpEgressEventFactory, HttpEventError, HttpEventRequest, HttpEventTransport};
use bytes::Bytes;
use futures::future::LocalBoxFuture;
use http::{HeaderName, HeaderValue, Response};
use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;

#[derive(Clone, Debug)]
struct WorkersFetch(Function);
impl HttpEgressEventFactory {
    /// Accepts the event-owned function returned by `createEventHttpFetch` in
    /// `js/event-fetch.mjs`. The host injects event-scoped fetch/timer functions.
    pub fn from_js(transport: Function) -> Self {
        Self::new(WorkersFetch(transport))
    }
}
struct AbortOnDrop(Function);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        let _ = self.0.call0(&JsValue::UNDEFINED);
    }
}
fn field(value: &JsValue, name: &str) -> Result<JsValue, HttpEventError> {
    Reflect::get(value, &JsValue::from_str(name)).map_err(|_| HttpEventError::TransportFailure)
}
fn set(object: &Object, name: &str, value: &JsValue) -> Result<(), HttpEventError> {
    Reflect::set(object, &JsValue::from_str(name), value)
        .map(|_| ())
        .map_err(|_| HttpEventError::TransportFailure)
}
fn encode_request(input: &HttpEventRequest) -> Result<Object, HttpEventError> {
    let object = Object::new();
    set(
        &object,
        "url",
        &JsValue::from_str(&input.request.uri().to_string()),
    )?;
    set(
        &object,
        "method",
        &JsValue::from_str(input.request.method().as_str()),
    )?;
    let headers = Array::new();
    for (name, value) in input.request.headers() {
        let pair = Array::new();
        pair.push(&JsValue::from_str(name.as_str()));
        pair.push(&JsValue::from_str(
            value
                .to_str()
                .map_err(|_| HttpEventError::TransportFailure)?,
        ));
        headers.push(&pair);
    }
    set(&object, "headers", &headers)?;
    set(
        &object,
        "body",
        &Uint8Array::from(input.request.body().as_ref()),
    )?;
    let limits = Object::new();
    for (name, value) in [
        (
            "max_response_body_bytes",
            u128::try_from(input.limits.max_response_body_bytes)
                .map_err(|_| HttpEventError::TransportFailure)?,
        ),
        (
            "max_response_head_bytes",
            u128::try_from(input.limits.max_response_head_bytes)
                .map_err(|_| HttpEventError::TransportFailure)?,
        ),
        (
            "request_timeout_millis",
            input.limits.request_timeout.as_millis(),
        ),
    ] {
        set(
            &limits,
            name,
            &JsValue::from(u32::try_from(value).map_err(|_| HttpEventError::TransportFailure)?),
        )?;
    }
    set(&object, "limits", &limits)?;
    Ok(object)
}
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // Range and integral value checked before cast.
fn response_status(value: &JsValue) -> Result<u16, HttpEventError> {
    value
        .as_f64()
        .filter(|v| v.is_finite() && v.fract() == 0.0 && (100.0..=599.0).contains(v))
        .map(|v| v as u16)
        .ok_or(HttpEventError::TransportFailure)
}
fn decode_response(
    value: &JsValue,
    body_limit: usize,
    head_limit: usize,
) -> Result<Response<Bytes>, HttpEventError> {
    let status = response_status(&field(value, "status")?)?;
    let body: Uint8Array = field(value, "body")?
        .dyn_into()
        .map_err(|_| HttpEventError::TransportFailure)?;
    if body.length() as usize > body_limit {
        return Err(HttpEventError::ResponseTooLarge);
    }
    let headers: Array = field(value, "headers")?
        .dyn_into()
        .map_err(|_| HttpEventError::TransportFailure)?;
    if headers.length() as usize > head_limit / 4 {
        return Err(HttpEventError::ResponseTooLarge);
    }
    let mut response = Response::builder()
        .status(status)
        .body(Bytes::from(body.to_vec()))
        .map_err(|_| HttpEventError::TransportFailure)?;
    let mut head_bytes = 0usize;
    for pair in headers.iter() {
        let pair: Array = pair
            .dyn_into()
            .map_err(|_| HttpEventError::TransportFailure)?;
        if pair.length() != 2 {
            return Err(HttpEventError::TransportFailure);
        }
        let name = pair
            .get(0)
            .as_string()
            .ok_or(HttpEventError::TransportFailure)?;
        let value = pair
            .get(1)
            .as_string()
            .ok_or(HttpEventError::TransportFailure)?;
        head_bytes = head_bytes
            .saturating_add(name.len())
            .saturating_add(value.len())
            .saturating_add(4);
        if head_bytes > head_limit {
            return Err(HttpEventError::ResponseTooLarge);
        }
        response.headers_mut().append(
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| HttpEventError::TransportFailure)?,
            HeaderValue::from_str(&value).map_err(|_| HttpEventError::TransportFailure)?,
        );
    }
    Ok(response)
}
impl HttpEventTransport for WorkersFetch {
    fn send(
        &self,
        input: HttpEventRequest,
    ) -> LocalBoxFuture<'static, Result<Response<Bytes>, HttpEventError>> {
        let fetch = self.0.clone();
        Box::pin(async move {
            let encoded = encode_request(&input)?;
            let operation = fetch
                .call1(&JsValue::UNDEFINED, &encoded)
                .map_err(|_| HttpEventError::TransportFailure)?;
            let abort: Function = field(&operation, "abort")?
                .dyn_into()
                .map_err(|_| HttpEventError::TransportFailure)?;
            let _abort_on_drop = AbortOnDrop(abort);
            let promise: Promise = field(&operation, "promise")?
                .dyn_into()
                .map_err(|_| HttpEventError::TransportFailure)?;
            let value = JsFuture::from(promise).await.map_err(|error| {
                match field(&error, "code")
                    .ok()
                    .and_then(|value| value.as_string())
                    .as_deref()
                {
                    Some("timeout") => HttpEventError::Timeout,
                    Some("response_too_large") => HttpEventError::ResponseTooLarge,
                    _ => HttpEventError::TransportFailure,
                }
            })?;
            decode_response(
                &value,
                input.limits.max_response_body_bytes,
                input.limits.max_response_head_bytes,
            )
        })
    }
}
