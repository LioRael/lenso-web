# Lenso Web

General-purpose backend Web Interfaces and Modules for Lenso.

This repository owns:

- `lenso.http.endpoint@1`, provided by backend Modules that own HTTP behavior;
- `lenso-web-ingress`, which assembles immutable routes and owns HTTP transport;
- `lenso.http.client@1`, required by backend Modules that call upstream HTTP APIs;
- `lenso-http-egress`, which provides policy-bounded outbound HTTP transport;
- credential-evidence extraction before a target-owned Auth decision; and
- protocol response mapping after the target Module completes.

It does not own portable Kernel semantics, authentication policy, business
authorization, Console/UI behavior, Web Shell, Browser Adapter, or a global
route registry.

## Current packages

- `lenso-capability-http-endpoint`
- `lenso-capability-http-client`
- `lenso-http-auth`
- `lenso-http-egress`
- `lenso-openapi`
- `lenso-web-ingress`

`lenso-http-egress` and `lenso-openapi` use the ordinary source-first native
authoring path: struct Modules declare configuration, typed Capability Ports,
provided Capabilities, and lifecycle hooks; linked factory registration and
Provider endpoints are generated. App Composition still decides whether an
Instance exists and exactly which bindings it receives.

`lenso-web-ingress` remains the deliberate compatibility exception. It is an
endpoint-free `many lenso.http.endpoint@1` consumer and its public Factory also
accepts Host-owned middleware, listener observation, and lane-replica handles.
The current struct authoring macro cannot inject those Host-only values into a
consumer-only Module. Ingress routing nevertheless uses the generated typed
`ManyPort<EndpointClient>`; raw request handles are confined to generated
Capability projection code and test fixtures.

The Capability crates keep their generated Rust bindings. Bun consumers import
the matching TypeScript projections from `@lenso/bun`, which locks the source
revision and verifies each projection independently.

`WebIngressConfig` is immutable Module Instance configuration from the Resolved
App Plan and defaults to an ephemeral loopback listener. App Composition may
explicitly bind a fixed private or public address; deployment policy stays with
the host. Ingress serves HTTP/1.1 and cleartext HTTP/2 prior knowledge, accepts
one `Authorization` credential, replaces untrusted external request identifiers,
removes credential and hop-by-hop headers before dispatch, enforces body/head,
concurrency, and Endpoint deadline limits, and cooperatively cancels work on
client disconnect or App shutdown. Request bodies are collected frame-by-frame
under the configured limit and avoid a copy when Hyper supplies one data frame.

Customized configuration uses
`crates/lenso-web-ingress/config.schema.json`. All fields are optional:

```json
{
  "bind_address": "0.0.0.0:8080",
  "max_request_body_bytes": 1048576,
  "max_request_head_bytes": 16384,
  "max_concurrent_requests": 128,
  "request_timeout_millis": 30000
}
```

An existing composition with empty Ingress configuration continues to receive
these defaults without a schema. Only customized Plan configuration needs the
schema.

Route providers are resolved through immutable `many` bindings. They describe
their method/path table during activation, before the App Ready Gate opens;
route collisions fail startup instead of changing behavior at runtime.

Endpoint providers can put route attributes directly on handlers. The outer
`#[endpoint]` attribute collects them at compile time and generates both the
Capability description and handler dispatch:

```rust,ignore
#[derive(Clone, Debug)]
struct OrdersHttp;

#[derive(serde::Deserialize)]
struct CreateOrder {
    total_cents: u64,
}

#[derive(serde::Serialize)]
struct CreatedOrder<'a> {
    id: &'a str,
}

#[endpoint]
impl OrdersHttp {
    #[post("orders.create", "/orders")]
    async fn create(
        &self,
        Json(_order): Json<CreateOrder>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        Ok(response::json(
            StatusCode::CREATED,
            &CreatedOrder { id: "order-42" },
        )?.with_header(
            &header::LOCATION,
            &HeaderValue::from_static("/orders/order-42"),
        )?)
    }
}
```

The omitted imports are available from
`lenso_capability_http_endpoint::response`: `response`, `StatusCode`,
`HeaderValue`, and `header`. Common responses use `response::json`,
`response::problem`, `response::text`, or `response::empty`; direct construction
of the generated wire DTO is only needed for unusual binary bodies.

The supported method attributes are `get`, `post`, `put`, `patch`, `delete`,
`head`, and `options`. Each handler declares its stable route ID and path.
`http_endpoint!` remains available for existing providers and generated source
that prefers one explicit route table.

Attribute-authored handlers may use `Path<T>`, `Query<T>`, `Json<T>`, and
`RequestId` extractors. Middleware on the `impl` applies to every route;
route-owned middleware follows it, before extraction:

```rust,ignore
#[endpoint]
#[middleware(trace_all)]
impl OrdersHttp {
    #[middleware(trace_request)]
    #[get("orders.read", "/orders/{order_id}")]
    async fn read(
        &self,
        context: InvocationContext,
        Path(path): Path<OrderPath>,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        self.read_authenticated(context, path.order_id).await
    }
}
```

The named async middleware method receives `InvocationContext` and
`HandleRequest`, then returns `MiddlewareOutcome::next` with an enriched
context/request or `MiddlewareOutcome::response` to short-circuit. Multiple
provider-wide and route middleware declarations run in order, with
provider-wide middleware first. Custom protocol-local extractors can implement
`FromRequest<Provider>`; extractors are asynchronous, can access the provider's
activation-time clients, and can enrich the context seen by later extractors
and the handler.

The macro rejects empty or malformed routes, duplicate route identifiers, and
duplicate exact method/path pairs during compilation. Web Ingress still owns
method/path matching, path-parameter extraction, semantic path-shape and
cross-provider collision detection, and transport limits. Endpoint handlers
own authentication orchestration, request decoding, business Capability calls,
and intentional HTTP responses.

For authenticated HTTP, Ingress selects one `Authorization` credential into
`HandleRequest::credential`; it does not decide identity or permission. The
`lenso-http-auth` integration crate handles Auth invocation, stable `401`/`403`
responses, and assertion attachment while the application owns meaningful
actor types:

```rust,ignore
struct UserActor { subject: String }

impl AuthenticatedHttpActor for UserActor {
    const KIND: &'static str = "user";

    fn from_assertion(assertion: &ActorAssertion) -> Self {
        Self { subject: assertion.subject().to_owned() }
    }
}

impl FromRequest<OrdersHttp> for UserActor {
    fn from_request<'a>(
        provider: &'a OrdersHttp,
        context: &'a mut InvocationContext,
        request: &'a HandleRequest,
    ) -> ExtractorFuture<'a, Self> {
        extract_authenticated_actor(provider, context, request)
    }
}
```

The Endpoint provider implements `AuthClientSource` to return its explicitly
bound activation-time `AuthClient`. A handler can then request `UserActor`
directly; an endpoint for a distinct credential actor kind can define
`AdminActor` in the same way. These `AuthenticatedHttpActor` types are edge
authentication projections, not
permission shortcuts: roles, tenant access, resource ownership, and every final
authorization decision remain in the target business Module, which verifies
the attached assertion. Release clients during deactivation rather than
resolving bindings per request.

`lenso-http-auth` is optional integration glue. Neither portable HTTP
Capability crate nor the Web Ingress library has a normal dependency on it;
removing the helper and its application integration leaves unauthenticated or
otherwise authenticated Endpoint providers usable without a Kernel branch.

The SDK is additive: existing providers that implement `EndpointProvider`
directly continue to work through the same immutable activation and dispatch
path.

## Optional OpenAPI 3.1 documents

OpenAPI is an opt-in Module, not an Ingress mode. Linking `lenso-openapi` does
not add a route or change an App. App Composition enables it by selecting one
`lenso.openapi` Instance, binding the Endpoint providers to document to that
Instance, and binding the Instance's own HTTP Endpoint to Web Ingress. Removing
that package selection, Instance, and those bindings removes the document
without changing the business Endpoints.

Endpoint authors may attach an OpenAPI 3.1 Operation Object while retaining the
stable route declaration as the source of `operationId`, method, and path:

```rust,ignore
#[endpoint]
impl OrdersHttp {
    #[get("orders.read", "/orders/{order_id}")]
    #[openapi({
        summary: "Read an order",
        responses: {
            "200": {
                description: "Order",
                content: {
                    "application/json": {
                        schema: {
                            "type": "object",
                            required: ["id"],
                            properties: { id: { "type": "string" } }
                        }
                    }
                }
            }
        }
    })]
    async fn read(&self) -> Result<HandleResponse, EndpointHandleInvocationError> {
        // ...
    }
}
```

The JSON-like syntax accepts ordinary identifier keys without quotes. Keys such
as `"type"`, `"application/json"`, and `"$ref"` stay quoted. String, number,
boolean, `null`, array, and nested object values are supported. The previous
JSON string form remains compatible.

`http_endpoint!` accepts the same syntax through
`openapi = openapi_operation!({ ... })`. The authoring macros validate the DSL
at compile time and reject a separately declared `operationId`. A route without
metadata remains valid and receives an `Undocumented response.` fallback only
when an OpenAPI document is assembled.

The selected `lenso.openapi` Instance uses immutable configuration validated by
`crates/lenso-openapi/config.schema.json`:

```json
{
  "title": "Orders API",
  "version": "1.0.0",
  "description": "Public order operations",
  "document_path": "/openapi.json",
  "servers": [{"url": "https://api.example.com"}],
  "components": {
    "securitySchemes": {
      "bearer": {"type": "http", "scheme": "bearer"}
    }
  }
}
```

The Module never infers a public server address from its listener and never
discovers Endpoint providers globally. Only providers explicitly bound to its
`many lenso.http.endpoint@1` requirement appear in the document. Swagger UI,
Redoc, pages, assets, and navigation remain with the application or Console UI
owner rather than this repository.

Ingress-owned failures have stable JSON codes. Missing routes return `404`,
method mismatches return `405` with `Allow`, malformed evidence returns `400`,
body/head limits return `413`/`431`, invalid or rejected Endpoint responses
return `502`, unavailable Endpoint execution returns `503`, and Endpoint
deadlines return `504`. Every Ingress-produced response carries a generated
`x-request-id` and `x-content-type-options: nosniff`.

Hosts can install transport-wide policy on one concrete Ingress factory.
Global Ingress middleware runs after request-head/body limits, request-ID
replacement, hop-by-hop filtering, and credential isolation. Request steps run
in declaration order before route matching; response steps run in reverse
order, including for short-circuited responses:

```rust,ignore
use futures::future::LocalBoxFuture;
use lenso_kernel::RuntimeFailure;
use lenso_web_ingress::{
    WebIngressFactory, WebIngressMiddleware, WebIngressMiddlewareOutcome,
    WebIngressRequest, WebIngressResponse,
};

#[derive(Debug)]
struct AccessLog;

impl WebIngressMiddleware for AccessLog {
    fn identity(&self) -> &'static str {
        // Include immutable settings so replicated lanes can compare policy.
        "access-log:json:v1"
    }

    fn before_request<'a>(
        &'a self,
        request: &'a mut WebIngressRequest,
    ) -> LocalBoxFuture<'a, Result<WebIngressMiddlewareOutcome, RuntimeFailure>> {
        tracing::info!(method = %request.method(), path = %request.uri().path());
        Box::pin(async { Ok(WebIngressMiddlewareOutcome::Continue) })
    }

    fn after_response<'a>(
        &'a self,
        _request: &'a WebIngressRequest,
        response: &'a mut WebIngressResponse,
    ) -> LocalBoxFuture<'a, Result<(), RuntimeFailure>> {
        tracing::info!(status = response.status().as_u16());
        Box::pin(async { Ok(()) })
    }
}

let ingress = WebIngressFactory::new().with_middleware(AccessLog);
```

This seam is suitable for transport-wide access logging, CORS, compression,
and coarse network admission policy. Middleware can mutate the normalized
method, URI, headers, and body, or return an intentional response. It cannot
override Ingress-owned response headers, reintroduce credential or hop-by-hop
headers to an Endpoint, bypass immutable transfer limits, or access Hyper's
connection body. Middleware failures map to `503`.

Authentication remains Endpoint orchestration: use `UserActor` or `AdminActor`
extractors backed by the Auth Module so the target Module can still make the
final authorization decision. Do not turn global Ingress middleware into a
business identity or permission registry.

Hosts that replicate one App across Runner lanes can bind once and create one
Ingress factory per lane through `WebIngressListenerCoordinator`. The
coordinator opens the Ready Gate only after every replica publishes the same
canonical route manifest and ordered middleware identity sequence, distributes
accepted sockets round-robin, and keeps the concurrency semaphore global to the
listener group. Every identity must include its immutable configuration:

```rust,no_run
let coordinator = WebIngressListenerCoordinator::bind(config, lane_count).await?;
let factories = (0..lane_count)
    .map(|_| WebIngressFactory::replicated(&coordinator).map(|factory| {
        factory.with_middleware(AccessLog)
    }))
    .collect::<Result<Vec<_>, _>>()?;
```

`HttpEgressConfig` requires at least one exact `http` or `https` origin. The
binding and immutable origin list are the caller's outbound authority. Egress
rejects origin changes, user info, URL fragments, authority/hop-by-hop request
headers, and oversized transfer evidence. It does not follow redirects, use
system proxies, set a Referer, store cookies, or retry automatically. Total and
connect timeouts plus concurrency and head/body limits are owned by each Egress
Instance.

## Verify

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
```

Release-plz opens release PRs from `main` and publishes the public crates through
the configured crates.io Trusted Publishers and GitHub OIDC. Dependency-aware
publication releases the Capability crates before the dependent
`lenso-web-ingress` and `lenso-http-egress` Modules, and workspace CI fully
verifies every package archive.

The independent-process benchmark compares transport-only Axum, the bridge,
and the complete Lenso path without sharing a Tokio runtime between client and
server. Filters keep short local runs reproducible:

```sh
LENSO_HTTP_BENCH_BODY_BYTES=0 \
LENSO_HTTP_BENCH_CONNECTIONS=8 \
cargo bench -p lenso-web-ingress --bench http_ingress_process
```

The Web execution profile adds the decision evidence needed before introducing
a different Web scheduler:

```sh
cargo bench -p lenso-web-ingress --bench web_execution_profile
```

It runs the same echo and delayed handlers through bare Axum, policy-equivalent
Axum transport, the bounded bridge, and the complete Lenso native path. The
report includes repeated throughput, unloaded p50/p99 latency, sampled server
CPU and RSS, and classified saturation outcomes. A two-lane Lenso fixture also
reports per-lane request counts for one, two, and eight HTTP/1.1 keep-alive
connections.

`LENSO_HTTP_BENCH_REQUESTS`, `LENSO_HTTP_BENCH_CONNECTIONS`, and
`LENSO_HTTP_BENCH_SERVER` can reduce the matrix. Process CPU/RSS sampling uses
the host `ps` command and prints `unavailable` when the host cannot provide it.
Treat results as host-specific evidence: a work-stealing Web profile is only
justified if repeated runs show a material user workload problem that cannot be
fixed at the Ingress routing or operation-granularity boundary. The benchmark
does not change the portable Kernel or imply live Instance migration.

The checked host-specific sample and its decision are recorded in
[`docs/evidence/web-execution-profile-2026-08-25.json`](docs/evidence/web-execution-profile-2026-08-25.json).
