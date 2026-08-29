use std::{cell::RefCell, rc::Rc};

use futures::executor::block_on;
use lenso_capability_http_endpoint::{
    self as http_endpoint_contract, DescribeRequest, EndpointHandleInvocationError,
    EndpointProvider, HandleRequest, HandleRequestHeadersItem, HandleResponse,
    HandleResponseHeadersItem, Json, MiddlewareOutcome, Path, QueryParams, RequestId, endpoint,
    response::{Problem, StatusCode},
    testing::EndpointTest,
};
use lenso_kernel::{CancellationToken, InvocationContext};
use serde::{Deserialize, Serialize};

#[lenso::plugin]
#[derive(Clone, Debug, Default)]
struct OrdersHttp {
    handled: Rc<RefCell<Vec<&'static str>>>,
}

#[endpoint]
#[middleware(trace_all)]
impl OrdersHttp {
    async fn trace_all(
        &self,
        context: InvocationContext,
        request: HandleRequest,
    ) -> Result<MiddlewareOutcome, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        self.handled.borrow_mut().push("global");
        Ok(MiddlewareOutcome::next(context, request))
    }

    async fn observe(
        &self,
        context: InvocationContext,
        request: HandleRequest,
    ) -> Result<MiddlewareOutcome, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        self.handled.borrow_mut().push("middleware");
        Ok(MiddlewareOutcome::next(context, request))
    }

    #[post("orders.create", "/orders")]
    #[openapi({
        summary: "Create an order",
        tags: ["orders"],
        deprecated: false,
        "x-display-order": -1,
        "x-example": null,
        responses: {
            "201": { description: "Created" }
        }
    })]
    async fn create(
        &self,
        _context: InvocationContext,
        Json(order): Json<CreateOrder>,
    ) -> Result<(StatusCode, Json<CreatedOrder>), Problem> {
        futures::future::ready(()).await;
        if order.id.is_empty() {
            return Err(Problem::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_order_id",
                "id must not be empty",
            ));
        }
        self.handled.borrow_mut().push("create");
        Ok((StatusCode::CREATED, Json(CreatedOrder { id: order.id })))
    }

    #[middleware(observe)]
    #[get("orders.read", "/orders/{order_id}")]
    async fn read(
        &self,
        _context: InvocationContext,
        Path(path): Path<OrderPath>,
        RequestId(request_id): RequestId,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        assert_eq!(request_id, "request-1");
        self.handled.borrow_mut().push("read");
        Ok(json_response(
            200,
            &format!(r#"{{"id":"{}"}}"#, path.order_id),
        ))
    }

    #[get("orders.list", "/orders")]
    async fn list(
        &self,
        QueryParams(query): QueryParams<ListOrders>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        Ok(json_response(
            200,
            &format!(r#"{{"limit":{}}}"#, query.limit),
        ))
    }

    #[query("orders.search", "/orders/search")]
    async fn search(
        &self,
        Json(filter): Json<SearchOrders>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        Ok(json_response(
            200,
            &format!(r#"{{"term":"{}"}}"#, filter.term),
        ))
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateOrder {
    id: String,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct CreatedOrder {
    id: String,
}

#[derive(Debug, Deserialize)]
struct OrderPath {
    order_id: String,
}

#[derive(Debug, Deserialize)]
struct ListOrders {
    limit: u16,
}

#[derive(Debug, Deserialize)]
struct SearchOrders {
    term: String,
}

#[test]
fn handler_attributes_generate_description_and_dispatch() {
    let endpoint = OrdersHttp::default();
    let description = block_on(endpoint.describe(context(1), DescribeRequest {}))
        .unwrap()
        .unwrap();
    assert_eq!(description.routes.len(), 4);
    assert_eq!(description.routes[0].route_id, "orders.create");
    assert_eq!(description.routes[0].method, "POST");
    assert_eq!(description.routes[0].path, "/orders");
    assert_eq!(
        description.routes[0].openapi.as_ref().unwrap()["summary"],
        "Create an order"
    );
    assert_eq!(
        description.routes[0].openapi.as_ref().unwrap()["tags"],
        serde_json::json!(["orders"])
    );
    assert_eq!(
        description.routes[0].openapi.as_ref().unwrap()["deprecated"],
        false
    );
    assert_eq!(
        description.routes[0].openapi.as_ref().unwrap()["x-display-order"],
        -1
    );
    assert!(description.routes[0].openapi.as_ref().unwrap()["x-example"].is_null());
    assert_eq!(description.routes[1].route_id, "orders.read");
    assert_eq!(description.routes[3].route_id, "orders.search");
    assert_eq!(description.routes[3].method, "QUERY");

    let response = block_on(endpoint.handle(
        context(2),
        request("orders.read").with_path_parameter("order_id", "order-42"),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body.as_slice(), br#"{"id":"order-42"}"#);
    assert_eq!(*endpoint.handled.borrow(), ["global", "middleware", "read"]);
}

#[test]
fn endpoint_declares_the_http_capability_for_its_plugin() {
    let descriptor: serde_json::Value = serde_json::from_str(PLUGIN_DESCRIPTOR_JSON).unwrap();
    assert_eq!(
        descriptor["provided_capabilities"][0]["capability_id"],
        "lenso.http.endpoint@1"
    );
    assert_eq!(
        descriptor["provided_capabilities"][0]["descriptor_version"],
        "1.1.0"
    );
}

#[test]
fn query_method_dispatches_a_request_with_content() {
    let endpoint = OrdersHttp::default();
    let mut search = request("orders.search");
    search.method = "QUERY".to_owned();
    search.body = br#"{"term":"open orders"}"#.as_slice().into();
    search.headers.push(HandleRequestHeadersItem {
        name: "content-type".to_owned(),
        value: "application/json".to_owned(),
    });

    let response = block_on(endpoint.handle(context(7), search))
        .unwrap()
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body.as_slice(), br#"{"term":"open orders"}"#);
}

#[test]
fn query_extractor_decodes_typed_values_and_rejects_invalid_input() {
    let endpoint = OrdersHttp::default();
    let mut accepted = request("orders.list");
    accepted.query = Some("limit=25".to_owned());
    let response = block_on(endpoint.handle(context(5), accepted))
        .unwrap()
        .unwrap();
    assert_eq!(response.body.as_slice(), br#"{"limit":25}"#);

    let mut rejected = request("orders.list");
    rejected.query = Some("limit=many".to_owned());
    let response = block_on(endpoint.handle(context(6), rejected))
        .unwrap()
        .unwrap();
    assert_eq!(response.status, 400);
}

#[test]
fn json_extractor_rejects_invalid_content_type_before_the_handler() {
    let endpoint = OrdersHttp::default();
    let rejected = block_on(endpoint.handle(context(3), request("orders.create")))
        .unwrap()
        .unwrap();
    assert_eq!(rejected.status, 415);
    assert_eq!(*endpoint.handled.borrow(), ["global"]);

    let mut accepted = request("orders.create");
    accepted.body = br#"{"id":"order-42"}"#.as_slice().into();
    accepted.headers.push(HandleRequestHeadersItem {
        name: "content-type".to_owned(),
        value: "application/json; charset=utf-8".to_owned(),
    });
    let response = block_on(endpoint.handle(context(4), accepted))
        .unwrap()
        .unwrap();
    assert_eq!(response.status, 201);
    assert_eq!(response.body.as_slice(), br#"{"id":"order-42"}"#);
    assert_eq!(*endpoint.handled.borrow(), ["global", "global", "create"]);
}

#[test]
fn typed_problem_errors_become_intentional_http_responses() {
    let endpoint = OrdersHttp::default();
    let mut rejected = request("orders.create");
    rejected.body = br#"{"id":""}"#.as_slice().into();
    rejected.headers.push(HandleRequestHeadersItem {
        name: "content-type".to_owned(),
        value: "application/json".to_owned(),
    });

    let response = block_on(endpoint.handle(context(8), rejected))
        .unwrap()
        .unwrap();
    assert_eq!(response.status, 422);
    assert_eq!(
        response.headers[0].value,
        "application/problem+json; charset=utf-8"
    );
    let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["code"], "invalid_order_id");
}

#[test]
fn endpoint_test_exercises_typed_handlers_without_a_socket() {
    let response = block_on(async {
        EndpointTest::new(OrdersHttp::default())
            .request("orders.create")
            .json(&CreateOrder {
                id: "order-84".to_owned(),
            })
            .unwrap()
            .send()
            .await
            .unwrap()
    });

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.json::<CreatedOrder>().unwrap(),
        CreatedOrder {
            id: "order-84".to_owned(),
        }
    );
}

fn request(route_id: &str) -> HandleRequest {
    HandleRequest {
        body: Vec::new().into(),
        credential: None,
        headers: Vec::new(),
        method: "GET".to_owned(),
        path: "/orders/order-42".to_owned(),
        path_parameters: Vec::new(),
        query: None,
        request_id: "request-1".to_owned(),
        route_id: route_id.to_owned(),
    }
}

trait RequestExt {
    fn with_path_parameter(self, name: &str, value: &str) -> Self;
}

impl RequestExt for HandleRequest {
    fn with_path_parameter(mut self, name: &str, value: &str) -> Self {
        self.path_parameters.push(
            lenso_capability_http_endpoint::HandleRequestPathParametersItem {
                name: name.to_owned(),
                value: value.to_owned(),
            },
        );
        self
    }
}

fn context(invocation_id: u64) -> InvocationContext {
    InvocationContext::new(invocation_id, None, CancellationToken::new())
}

fn json_response(status: i64, body: &str) -> HandleResponse {
    HandleResponse {
        body: body.as_bytes().into(),
        headers: vec![HandleResponseHeadersItem {
            name: "content-type".to_owned(),
            value: "application/json".to_owned(),
        }],
        status,
    }
}
