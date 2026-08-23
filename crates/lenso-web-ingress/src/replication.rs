use std::fmt;

/// One canonical route entry used to validate same-port Ingress replicas.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WebIngressRoute {
    pub method: String,
    pub path: String,
    pub route_id: String,
}

impl WebIngressRoute {
    pub(crate) fn new(method: &str, path: &str, route_id: &str) -> Self {
        Self {
            method: method.to_owned(),
            path: path.to_owned(),
            route_id: route_id.to_owned(),
        }
    }
}

/// Canonical, payload-free routing identity for one prepared Ingress replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebIngressRouteManifest {
    routes: Vec<WebIngressRoute>,
}

impl WebIngressRouteManifest {
    pub(crate) fn new(mut routes: Vec<WebIngressRoute>) -> Self {
        routes.sort_unstable();
        Self { routes }
    }

    /// Returns the canonical route entries.
    pub fn routes(&self) -> &[WebIngressRoute] {
        &self.routes
    }

    /// Rejects a same-port replica whose route ownership differs.
    pub fn ensure_equivalent(&self, replica: &Self) -> Result<(), WebIngressReplicaMismatch> {
        if self == replica {
            Ok(())
        } else {
            Err(WebIngressReplicaMismatch)
        }
    }
}

/// Same-port replicas do not expose an identical route manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebIngressReplicaMismatch;

impl fmt::Display for WebIngressReplicaMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("same-port Web Ingress replicas have different route manifests")
    }
}

impl std::error::Error for WebIngressReplicaMismatch {}

#[cfg(test)]
mod tests {
    use super::{WebIngressRoute, WebIngressRouteManifest};

    #[test]
    fn manifests_are_canonical_and_reject_route_shards() {
        let first = WebIngressRouteManifest::new(vec![
            WebIngressRoute::new("POST", "/orders", "orders.create"),
            WebIngressRoute::new("GET", "/orders/{id}", "orders.read"),
        ]);
        let reordered = WebIngressRouteManifest::new(vec![
            WebIngressRoute::new("GET", "/orders/{id}", "orders.read"),
            WebIngressRoute::new("POST", "/orders", "orders.create"),
        ]);
        let shard = WebIngressRouteManifest::new(vec![WebIngressRoute::new(
            "GET",
            "/orders/{id}",
            "orders.read",
        )]);

        assert_eq!(first, reordered);
        first.ensure_equivalent(&reordered).unwrap();
        assert!(first.ensure_equivalent(&shard).is_err());
    }
}
