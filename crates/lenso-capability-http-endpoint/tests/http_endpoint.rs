use std::{cell::RefCell, rc::Rc};

use futures::executor::block_on;
use lenso_capability_http_endpoint::{
    Bytes, DescribeError, DescribeRequest, EndpointHandleInvocationError, EndpointProvider,
    HandleError, HandleRequest, HandleResponse, HandleResponseHeadersItem, HttpEndpoint,
    http_endpoint, openapi_operation,
};
use lenso_kernel::{CancellationToken, InvocationContext};

#[derive(Clone, Debug, Default)]
struct OrdersHttp {
    handled: Rc<RefCell<Vec<&'static str>>>,
}

impl OrdersHttp {
    async fn create(
        &self,
        _context: InvocationContext,
        _request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        self.handled.borrow_mut().push("create");
        Ok(json_response(201, r#"{"id":"order-42"}"#))
    }

    async fn read(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        self.handled.borrow_mut().push("read");
        let order_id = request
            .path_parameters
            .iter()
            .find(|parameter| parameter.name == "order_id")
            .map_or("missing", |parameter| parameter.value.as_str());
        Ok(json_response(200, &format!(r#"{{"id":"{order_id}"}}"#)))
    }
}

http_endpoint! {
    impl OrdersHttp {
        "orders.create" => (
            "POST",
            "/orders",
            openapi = openapi_operation!({
                summary: "Create an order",
                responses: {
                    "201": { description: "Created" }
                }
            })
        ) => create,
        "orders.read" => ("GET", "/orders/{order_id}") => read,
    }
}

#[test]
fn one_route_table_drives_description_and_handler_dispatch() {
    let endpoint = OrdersHttp::default();
    assert_eq!(OrdersHttp::ROUTES.len(), 2);

    let description = block_on(endpoint.describe(context(1), DescribeRequest {}))
        .unwrap()
        .unwrap();
    assert_eq!(description.routes.len(), 2);
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
    assert_eq!(*endpoint.handled.borrow(), ["read"]);
}

#[test]
fn direct_invocation_of_an_undeclared_route_fails_closed() {
    let endpoint = OrdersHttp::default();
    let result = block_on(endpoint.handle(context(3), request("orders.delete").into()));
    assert_eq!(result, Ok(Err(HandleError::Rejected)));
    assert!(endpoint.handled.borrow().is_empty());
}

#[derive(Clone, Copy, Debug)]
struct InvalidOpenApi;

impl InvalidOpenApi {
    async fn read(
        &self,
        _context: InvocationContext,
        _request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        unreachable!()
    }
}

http_endpoint! {
    impl InvalidOpenApi {
        "invalid.read" => ("GET", "/invalid", openapi = "not-json") => read,
    }
}

#[test]
fn explicit_route_table_reports_invalid_openapi_as_a_domain_error() {
    let result = block_on(InvalidOpenApi.describe(context(4), DescribeRequest {}));
    assert_eq!(result, Ok(Err(DescribeError::InvalidConfiguration)));
}

fn context(request_id: u64) -> InvocationContext {
    InvocationContext::new(request_id, None, CancellationToken::new())
}

fn request(route_id: &str) -> RequestBuilder {
    RequestBuilder(HandleRequest {
        body: Bytes::default(),
        credential: None,
        headers: Vec::new(),
        method: "GET".to_owned(),
        path: "/orders/order-42".to_owned(),
        path_parameters: Vec::new(),
        query: None,
        request_id: "request-1".to_owned(),
        route_id: route_id.to_owned(),
    })
}

struct RequestBuilder(HandleRequest);

impl RequestBuilder {
    fn with_path_parameter(mut self, name: &str, value: &str) -> HandleRequest {
        self.0.path_parameters.push(
            lenso_capability_http_endpoint::HandleRequestPathParametersItem {
                name: name.to_owned(),
                value: value.to_owned(),
            },
        );
        self.0
    }
}

impl From<RequestBuilder> for HandleRequest {
    fn from(builder: RequestBuilder) -> Self {
        builder.0
    }
}

fn json_response(status: i64, body: &str) -> HandleResponse {
    HandleResponse {
        body: body.as_bytes().to_vec().into(),
        headers: vec![HandleResponseHeadersItem {
            name: "content-type".to_owned(),
            value: "application/json".to_owned(),
        }],
        status,
    }
}
