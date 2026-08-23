# Lenso Web

General-purpose backend Web Interfaces and Modules for Lenso vNext.

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
- `lenso-http-egress`
- `lenso-web-ingress`

`WebIngressConfig` is immutable Module Instance configuration from the Resolved
App Plan and defaults to an ephemeral loopback listener. App Composition may
explicitly bind a fixed private or public address; deployment policy stays with
the host. The first version accepts one `Authorization` credential, replaces
untrusted external request identifiers, removes credential and hop-by-hop
headers before dispatch, enforces body/head, concurrency, and Endpoint deadline
limits, and cooperatively cancels work on client disconnect or App shutdown.

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

Endpoint providers can use `http_endpoint!` to declare each route once and
generate both the Capability description and handler dispatch:

```rust,ignore
#[derive(Clone, Debug)]
struct OrdersHttp;

impl OrdersHttp {
    async fn create(
        &self,
        context: InvocationContext,
        request: HandleRequest,
    ) -> Result<HandleResponse, EndpointHandleInvocationError> {
        // Authenticate, call the Orders Capability, and map its result to HTTP.
    }
}

http_endpoint! {
    impl OrdersHttp {
        "orders.create" => ("POST", "/orders") => create,
    }
}
```

The macro rejects empty or malformed routes, duplicate route identifiers, and
duplicate exact method/path pairs during compilation. Web Ingress still owns
method/path matching, path-parameter extraction, semantic path-shape and
cross-provider collision detection, and transport limits. Endpoint handlers
own authentication orchestration, request decoding, business Capability calls,
and intentional HTTP responses.

The SDK is additive: existing providers that implement `EndpointProvider`
directly continue to work through the same immutable activation and dispatch
path.

Ingress-owned failures have stable JSON codes. Missing routes return `404`,
method mismatches return `405` with `Allow`, malformed evidence returns `400`,
body/head limits return `413`/`431`, invalid or rejected Endpoint responses
return `502`, unavailable Endpoint execution returns `503`, and Endpoint
deadlines return `504`. Every Ingress-produced response carries a generated
`x-request-id` and `x-content-type-options: nosniff`.

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

Release-plz opens release PRs from `main` and publishes the four crates through
the configured crates.io Trusted Publishers and GitHub OIDC. Dependency-aware
publication releases the Capability crates before the dependent
`lenso-web-ingress` and `lenso-http-egress` Modules, and workspace CI fully
verifies every package archive.
