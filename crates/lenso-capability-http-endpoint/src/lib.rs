//! Generated bindings for backend-owned HTTP Endpoint providers.

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_contract_round_trips_routes_credentials_parameters_and_bytes() {
        let description = DescribeResponse {
            routes: vec![DescribeResponseRoutesItem {
                method: "GET".to_owned(),
                path: "/orders/{order_id}".to_owned(),
                route_id: "orders.read".to_owned(),
            }],
        };
        let wire = encode_describe_response(&description).unwrap();
        assert_eq!(decode_describe_response(&wire).unwrap(), description);

        let request = HandleRequest {
            body: "AAEC".to_owned(),
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
        assert_eq!(decode_handle_request(&wire).unwrap(), request);
    }
}
