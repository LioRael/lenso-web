//! Generated bindings for an explicitly bound outbound HTTP client.

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_contract_round_trips_headers_and_bytes() {
        let request = SendRequest {
            body: Bytes::from(vec![0, 1, 2]),
            headers: vec![SendRequestHeadersItem {
                name: "content-type".to_owned(),
                value: "application/octet-stream".to_owned(),
            }],
            method: "POST".to_owned(),
            url: "https://api.example.test/v1/items?active=true".to_owned(),
        };
        let wire = encode_send_request(&request).unwrap();
        assert!(wire.contains(r#""body":"AAEC""#));
        assert_eq!(decode_send_request(&wire).unwrap(), request);
    }

    #[test]
    fn client_contract_values_support_cross_lane_transfer() {
        fn assert_send<T: Send>() {}

        const { assert!(CROSS_LANE_TRANSFER) };
        assert_send::<SendRequest>();
        assert_send::<SendResponse>();
        assert_send::<SendError>();
    }
}
