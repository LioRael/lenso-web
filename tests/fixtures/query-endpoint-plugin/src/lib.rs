use lenso_capability_http_endpoint::prelude::*;
use serde::Deserialize;

#[lenso::plugin]
#[derive(Clone, Debug, Default)]
struct OrderSearchHttp {}

#[derive(Debug, Deserialize)]
struct SearchFilter {
    term: String,
}

#[endpoint]
impl OrderSearchHttp {
    #[query("orders.search", "/orders/search")]
    async fn search(
        &self,
        Json(filter): Json<SearchFilter>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        std::future::ready(()).await;
        Ok(lenso_capability_http_endpoint::response::json(
            lenso_capability_http_endpoint::response::StatusCode::OK,
            &serde_json::json!({ "term": filter.term }),
        )?)
    }
}

/// Forces this fixture crate to remain linked so its generated Plugin factory is discoverable.
pub const fn link() {}
