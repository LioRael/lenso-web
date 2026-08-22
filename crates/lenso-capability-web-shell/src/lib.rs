//! Generated bindings for the target-owned Web Shell Capability.

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_route_carries_navigation_assets_and_projected_requirements() {
        let route = RenderRouteResponse {
            contribution_id: "orders".to_owned(),
            body: "<!doctype html><h1>Orders</h1>".to_owned(),
            navigation: vec![RenderRouteResponseNavigationItem {
                route: "/orders".to_owned(),
                label: "Orders".to_owned(),
            }],
            asset_paths: vec!["/assets/orders.js".to_owned()],
            requirements: vec![RenderRouteResponseRequirementsItem {
                capability_id: "example.secure-greeting@1".to_owned(),
                descriptor_version: "1.0.0".to_owned(),
                operations: vec!["greet".to_owned()],
            }],
        };

        let wire = encode_render_route_response(&route).expect("route should encode");
        assert_eq!(
            decode_render_route_response(&wire).expect("route should decode"),
            route
        );
    }
}
