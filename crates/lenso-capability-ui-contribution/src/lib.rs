//! Generated bindings for the portable UI Contribution Capability.

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contribution_metadata_round_trips_with_assets_and_exact_requirements() {
        let metadata = DescribeResponse {
            contribution_id: "orders".to_owned(),
            route: "/orders".to_owned(),
            navigation_label: "Orders".to_owned(),
            body: "<h1>Orders</h1>".to_owned(),
            assets: vec![DescribeResponseAssetsItem {
                path: "/assets/orders.js".to_owned(),
                content_type: "text/javascript".to_owned(),
                content: "export const ready = true;".to_owned(),
            }],
            requirements: vec![DescribeResponseRequirementsItem {
                capability_id: "example.secure-greeting@1".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                operations: vec!["greet".to_owned()],
            }],
        };

        let wire = encode_describe_response(&metadata).expect("metadata should encode");
        assert_eq!(
            decode_describe_response(&wire).expect("metadata should decode"),
            metadata
        );
    }
}
