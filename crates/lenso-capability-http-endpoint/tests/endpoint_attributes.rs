use std::{cell::RefCell, rc::Rc};

use futures::executor::block_on;
use lenso_capability_http_endpoint::{
    DescribeRequest, EndpointHandleInvocationError, EndpointProvider, HandleRequest,
    HandleRequestHeadersItem, HandleResponse, HandleResponseHeadersItem, Json, MiddlewareOutcome,
    Path, Query, RequestId, endpoint,
};
use lenso_kernel::{CancellationToken, InvocationContext};
use serde::Deserialize;

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
    #[openapi(r#"{"summary":"Create an order","responses":{"201":{"description":"Created"}}}"#)]
    async fn create(
        &self,
        _context: InvocationContext,
        Json(order): Json<CreateOrder>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        self.handled.borrow_mut().push("create");
        Ok(json_response(201, &format!(r#"{{"id":"{}"}}"#, order.id)))
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
        Query(query): Query<ListOrders>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        Ok(json_response(
            200,
            &format!(r#"{{"limit":{}}}"#, query.limit),
        ))
    }
}

#[derive(Debug, Deserialize)]
struct CreateOrder {
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

#[test]
fn handler_attributes_generate_description_and_dispatch() {
    let endpoint = OrdersHttp::default();
    let description = block_on(endpoint.describe(context(1), DescribeRequest {}))
        .unwrap()
        .unwrap();
    assert_eq!(description.routes.len(), 3);
    assert_eq!(description.routes[0].route_id, "orders.create");
    assert_eq!(description.routes[0].method, "POST");
    assert_eq!(description.routes[0].path, "/orders");
    assert_eq!(
        description.routes[0].openapi.as_ref().unwrap()["summary"],
        "Create an order"
    );
    assert_eq!(description.routes[1].route_id, "orders.read");

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
