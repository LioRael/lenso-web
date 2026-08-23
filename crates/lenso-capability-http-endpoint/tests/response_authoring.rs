use lenso_capability_http_endpoint::{
    EndpointHandleInvocationError,
    response::{self, HeaderValue, ResponseBuildError, StatusCode, header},
};
use serde::Serialize;

#[derive(Serialize)]
struct CreatedOrder<'a> {
    id: &'a str,
}

#[test]
fn typed_json_response_sets_status_content_type_and_body() {
    let response = response::json(StatusCode::CREATED, &CreatedOrder { id: "order-42" })
        .unwrap()
        .with_header(
            &header::LOCATION,
            &HeaderValue::from_static("/orders/order-42"),
        )
        .unwrap();

    assert_eq!(response.status, 201);
    assert_eq!(response.body.as_slice(), br#"{"id":"order-42"}"#);
    assert_eq!(response.headers[0].name, "content-type");
    assert_eq!(response.headers[0].value, "application/json; charset=utf-8");
    assert_eq!(response.headers[1].name, "location");
}

#[test]
fn common_response_shapes_do_not_require_wire_dto_construction() {
    let problem = response::problem(
        StatusCode::UNAUTHORIZED,
        "authentication_required",
        "A bearer token is required.",
    );
    let text = response::text(StatusCode::OK, "ready");
    let empty = response::empty(StatusCode::NO_CONTENT);

    assert_eq!(problem.status, 401);
    assert_eq!(
        problem.headers[0].value,
        "application/problem+json; charset=utf-8"
    );
    let problem_body: serde_json::Value = serde_json::from_slice(&problem.body).unwrap();
    assert_eq!(problem_body["type"], "about:blank");
    assert_eq!(problem_body["title"], "Unauthorized");
    assert_eq!(problem_body["status"], 401);
    assert_eq!(problem_body["code"], "authentication_required");
    assert_eq!(text.body.as_slice(), b"ready");
    assert_eq!(empty.status, 204);
    assert!(empty.headers.is_empty());
    assert!(empty.body.is_empty());
}

#[test]
fn binary_headers_fail_before_reaching_ingress() {
    let error = response::empty(StatusCode::OK)
        .with_header(
            &header::SET_COOKIE,
            &HeaderValue::from_bytes(b"name=\xff").unwrap(),
        )
        .unwrap_err();
    assert!(matches!(error, ResponseBuildError::NonTextHeaderValue(_)));

    let invocation_error = EndpointHandleInvocationError::from(error);
    assert!(matches!(
        invocation_error,
        EndpointHandleInvocationError::Runtime(_)
    ));
}
