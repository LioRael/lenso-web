//! Generated bindings for an explicitly bound outbound HTTP client.

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_contract_round_trips_headers_and_bytes() {
        let request = SendRequest {
            body: "AAEC".to_owned(),
            headers: vec![SendRequestHeadersItem {
                name: "content-type".to_owned(),
                value: "application/octet-stream".to_owned(),
            }],
            method: "POST".to_owned(),
            url: "https://api.example.test/v1/items?active=true".to_owned(),
        };
        let wire = encode_send_request(&request).unwrap();
        assert_eq!(decode_send_request(&wire).unwrap(), request);
    }
}
