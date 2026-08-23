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

`WebIngressConfig` defaults to an ephemeral loopback listener. Backend hosts
may explicitly bind a fixed private or public address; deployment policy stays
with the host. The first version accepts one `Authorization` credential,
removes credential and hop-by-hop headers before dispatch, enforces body/head,
concurrency, and deadline limits, and drains on App cancellation.

Route providers are resolved through immutable `many` bindings. They describe
their method/path table during activation, before the App Ready Gate opens;
route collisions fail startup instead of changing behavior at runtime.

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
```

Publication remains parked until the post-extraction baseline and crates.io
Trusted Publishers are reviewed. The initial release must publish
the two Capability crates before packaging `lenso-web-ingress` and
`lenso-http-egress`; workspace CI fully compiles and tests all crates, while its
package gate dry-runs the leaf Capabilities and lists the Module package
contents until those dependencies are available from crates.io.
