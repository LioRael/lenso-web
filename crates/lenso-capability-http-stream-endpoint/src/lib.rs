//! Generated bindings for backend-owned streaming HTTP Endpoint providers.

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

pub use generated::*;

// Keep the HTTP payload vocabulary aligned with the buffered Endpoint while
// the operation names remain distinct enough for one Plugin to provide both.
pub use generated::{
    DESCRIBE_STREAM_OPERATION as DESCRIBE_OPERATION, DescribeStreamError as DescribeError,
    DescribeStreamRequest as DescribeRequest, DescribeStreamResponse as DescribeResponse,
    DescribeStreamResponseRoutesItem as DescribeResponseRoutesItem,
    HANDLE_STREAM_OPERATION as HANDLE_OPERATION, HandleStreamError as HandleError,
    HandleStreamRequest as HandleRequest, HandleStreamRequestCredential as HandleRequestCredential,
    HandleStreamRequestHeadersItem as HandleRequestHeadersItem,
    HandleStreamRequestPathParametersItem as HandleRequestPathParametersItem,
    HandleStreamResponse as HandleResponse,
    HandleStreamResponseHeadersItem as HandleResponseHeadersItem,
    HandleStreamResponseKind as HandleResponseKind,
    StreamEndpointDescribeStream as StreamEndpointDescribe,
    StreamEndpointDescribeStreamInvocationError as StreamEndpointDescribeInvocationError,
    StreamEndpointHandleStream as StreamEndpointHandle,
    StreamEndpointHandleStreamInvocationError as StreamEndpointHandleInvocationError,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_endpoint_contract_round_trips_routes_requests_and_frames() {
        let description = DescribeResponse {
            routes: vec![DescribeResponseRoutesItem {
                method: "GET".to_owned(),
                path: "/events/{topic}".to_owned(),
                route_id: "events.watch".to_owned(),
            }],
        };
        let wire = encode_describe_stream_response(&description).unwrap();
        assert_eq!(decode_describe_stream_response(&wire).unwrap(), description);

        let request = HandleRequest {
            body: Bytes::from(vec![0, 1, 2]),
            credential: Some(HandleRequestCredential {
                scheme: "bearer".to_owned(),
                value: "token".to_owned(),
            }),
            headers: vec![HandleRequestHeadersItem {
                name: "accept".to_owned(),
                value: "text/event-stream".to_owned(),
            }],
            method: "GET".to_owned(),
            path: "/events/orders".to_owned(),
            path_parameters: vec![HandleRequestPathParametersItem {
                name: "topic".to_owned(),
                value: "orders".to_owned(),
            }],
            query: Some("after=3".to_owned()),
            request_id: "request-1".to_owned(),
            route_id: "events.watch".to_owned(),
        };
        let wire = encode_handle_stream_request(&request).unwrap();
        assert!(wire.contains(r#""body":"AAEC""#));
        assert_eq!(decode_handle_stream_request(&wire).unwrap(), request);

        let frame = HandleResponse {
            body: Some(Bytes::from(vec![3, 4, 5])),
            headers: None,
            kind: HandleResponseKind::Chunk,
            status: None,
        };
        let wire = encode_handle_stream_response(&frame).unwrap();
        assert_eq!(decode_handle_stream_response(&wire).unwrap(), frame);
    }

    #[test]
    fn stream_endpoint_contract_values_support_cross_lane_transfer() {
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
