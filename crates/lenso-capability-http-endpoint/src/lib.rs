//! Generated bindings for backend-owned HTTP Endpoint providers.

mod authoring;
mod extract;
pub mod response;
pub mod testing;

#[allow(unknown_lints)]
#[allow(
    clippy::chunks_exact_to_as_chunks,
    clippy::manual_div_ceil,
    clippy::manual_is_multiple_of,
    clippy::verbose_bit_mask
)]
mod generated {
    include!("generated.rs");
}

pub use authoring::{EndpointFuture, EndpointRoute, HttpEndpoint, MiddlewareOutcome};
/// Deprecated compatibility name for [`QueryParams`].
#[deprecated(
    since = "0.3.0",
    note = "use QueryParams to distinguish URL parameters from the QUERY HTTP method"
)]
pub use extract::QueryParams as Query;
pub use extract::{
    ExtractorFuture, ExtractorRejection, FromRequest, Json, Path, QueryParams, RequestId,
};
pub use generated::*;
pub use lenso_capability_http_endpoint_macros::{endpoint, openapi_operation};

/// Common imports for HTTP Endpoint Plugin authors.
pub mod prelude {
    pub use crate as http_endpoint_contract;
    pub use crate::{
        EndpointHandleInvocationError, HandleRequest, HandleResponse, Json, MiddlewareOutcome,
        Path, QueryParams, RequestId, endpoint,
        response::{IntoResponse, Problem, StatusCode},
    };
}

#[doc(hidden)]
pub use authoring::{__private, validate_endpoint_routes};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_contract_round_trips_routes_credentials_parameters_and_bytes() {
        let description = DescribeResponse {
            routes: vec![DescribeResponseRoutesItem {
                method: "GET".to_owned(),
                openapi: Some(std::collections::BTreeMap::from([(
                    "summary".to_owned(),
                    serde_json::json!("Read an order"),
                )])),
                path: "/orders/{order_id}".to_owned(),
                route_id: "orders.read".to_owned(),
            }],
        };
        let wire = encode_describe_response(&description).unwrap();
        assert_eq!(decode_describe_response(&wire).unwrap(), description);

        let request = HandleRequest {
            body: Bytes::from(vec![0, 1, 2]),
            credential: Some(HandleRequestCredential {
                scheme: "bearer".to_owned(),
                value: "token".to_owned(),
            }),
            headers: vec![HandleRequestHeadersItem {
                name: "accept".to_owned(),
                value: "application/json".to_owned(),
            }],
            method: "GET".to_owned(),
            path: "/orders/42".to_owned(),
            path_parameters: vec![HandleRequestPathParametersItem {
                name: "order_id".to_owned(),
                value: "42".to_owned(),
            }],
            query: Some("include=items".to_owned()),
            request_id: "request-1".to_owned(),
            route_id: "orders.read".to_owned(),
        };
        let wire = encode_handle_request(&request).unwrap();
        assert!(wire.contains(r#""body":"AAEC""#));
        assert_eq!(decode_handle_request(&wire).unwrap(), request);
    }

    #[test]
    fn endpoint_contract_values_support_cross_lane_transfer() {
        fn assert_send<T: Send>() {}

        const { assert!(CROSS_LANE_TRANSFER) };
        assert_send::<DescribeRequest>();
        assert_send::<DescribeResponse>();
        assert_send::<DescribeError>();
        assert_send::<HandleRequest>();
        assert_send::<HandleResponse>();
        assert_send::<HandleError>();
    }
}
