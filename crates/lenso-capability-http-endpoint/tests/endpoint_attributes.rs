use std::{cell::RefCell, rc::Rc};

use futures::executor::block_on;
use lenso_capability_http_endpoint::{
    DescribeRequest, EndpointHandleInvocationError, EndpointProvider, HandleRequest,
    HandleResponse, HandleResponseHeadersItem, endpoint,
};
use lenso_kernel::{CancellationToken, InvocationContext};

#[derive(Clone, Debug, Default)]
struct OrdersHttp {
    handled: Rc<RefCell<Vec<&'static str>>>,
}

#[endpoint]
impl OrdersHttp {
    #[post("orders.create", "/orders")]
    async fn create(
        &self,
        _context: InvocationContext,
        _request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        futures::future::ready(()).await;
        self.handled.borrow_mut().push("create");
        Ok(json_response(201, r#"{"id":"order-42"}"#))
    }

    #[get("orders.read", "/orders/{order_id}")]
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

#[test]
fn handler_attributes_generate_description_and_dispatch() {
    let endpoint = OrdersHttp::default();
    let description = block_on(endpoint.describe(context(1), DescribeRequest {}))
        .unwrap()
        .unwrap();
    assert_eq!(description.routes.len(), 2);
    assert_eq!(description.routes[0].route_id, "orders.create");
    assert_eq!(description.routes[0].method, "POST");
    assert_eq!(description.routes[0].path, "/orders");
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
