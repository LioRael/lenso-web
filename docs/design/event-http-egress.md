# Event HTTP Egress

The event implementation retains `lenso.http-egress` and the generated
`lenso.http.client@1` contract. Native linked registration and reqwest defaults
remain behind the default `native` feature. `policy.rs` is shared by both
implementations: exact origin canonicalization, URL/method validation, forbidden
request headers, request limits, response header filtering and domain errors.
No generated binding or Auth implementation changes are required.

## Integration

Depend on `lenso-http-egress-plugin` with `default-features = false` and
`features = ["workers"]`. Create a transport function in the event Host:

```js
import { createEventHttpFetch } from './event-fetch.mjs';
const transport = createEventHttpFetch({
  fetch: eventScopedFetch,
  setTimeout: eventSetTimeout,
  clearTimeout: eventClearTimeout,
});
```

Use the Web-owned module at
`crates/lenso-http-egress-plugin/js/event-fetch.mjs`; pass the returned Function
explicitly to Rust and register `HttpEgressEventFactory::from_js(transport)` in
that event's `NativePluginRegistry`. Use
`HttpEgressEventFactory::plugin_descriptor()` for Host catalog construction and
bind the existing Client Capability to the consuming Plugin. Create a fresh
factory and host transport scope for each event App. There is no global fetch
lookup in the Rust adapter or global request state in the JS module.

The host policy must supply exact origins and equal connect/total deadlines:

```rust,ignore
let config = HttpEgressConfig::new([issuer_origin])?
    .with_timeouts(Duration::from_secs(30), Duration::from_secs(30))?;
```

Fetch does not expose a separate socket-connect phase or protocol selection.
The event factory therefore rejects unequal connect/total deadlines and any
`HttpVersionPolicy` other than `Auto` before startup. Equal deadlines allow the
total timer to enforce both upper bounds. Native defaults remain 5s connect,
30s total, automatic protocol negotiation. The event target cannot silently
reinterpret native configuration requiring a distinct connection deadline.

For other event hosts, `HttpEgressEventFactory::new(impl HttpEventTransport)`
provides the same injection seam. This is a trusted Host implementation contract:
its future must enforce the supplied streaming limits/deadline, manual redirects,
no implicit retries/proxies/cookie storage, and abort its I/O when dropped.

## Fetch bridge behavior

The Rust provider validates exact origin, method, URL userinfo/fragment, headers
and request limits before invoking JS. It passes a sanitized request plus
immutable response limits and total deadline. The default user agent matches the
native package's user agent when the caller has not supplied one.

Each JS call creates its own AbortController, reader and timer. Fetch uses
`redirect: 'manual'`, `credentials: 'omit'` and `cache: 'no-store'`. The response
reader checks each chunk before appending it; advertised response lengths and
headers are bounded too. HEAD/no-body response Content-Length is metadata rather
than a received-body bound. Multiple Set-Cookie values remain separate; a host
without an API to recover them fails closed instead of splitting cookie strings.
Rust rechecks body/header bounds and applies the same response filtering as
native reqwest before returning the generated response.

The transport races its I/O against an independent total deadline, so timeout
settles even when a host fetch fails to reject promptly on abort. Timeout and
transport/size failures map to existing `SendError` variants. Dropping the Rust
host future invokes the JS abort function; the Client invocation returns Kernel
cancellation. Timers clear on success and failure. Abort requests I/O cancellation;
it does not prove upstream rollback or replace the Runtime Runner's event-scope
settlement and trap/reset handling.

## Qualification limits

Fetch controls forbidden method/header behavior, connection reuse, TLS and
protocol negotiation. It may normalize response fields and content encodings.
The bounded OIDC fixture uses ordinary GET/POST JSON requests and exact configured
origins; this seam does not claim full raw-wire parity for every legal native HTTP
request. In particular, native requests requiring GET/HEAD bodies or forced HTTP
versions require separate target qualification. Explicit `Cookie` headers remain
caller data permitted by the existing native Client policy; the bridge itself
keeps no cookie jar and has no ambient cookie authority.

The response cap is the existing Egress configuration cap (4MiB by default),
not an inferred bound from the generic Client schema. Host CPU termination and
Wasm traps can prevent Rust destructors; the event Runner must independently
abandon the entire host I/O scope. No cleanup or retry claim implies reversal of
durable upstream work.

## Verification

- Rust event policy tests cross normal Kernel/native registry/generated Client
  invocation, checking exact-origin refusal before host I/O, binary/cookie
  response filtering, redirect exposure, bounds, timeout mapping, cancellation,
  and unsupported-policy startup rejection.
- Existing native request/HTTP2/timeout/concurrency regressions remain required.
- `node --test crates/lenso-http-egress-plugin/js/event-fetch.test.mjs` exercises
  real Fetch against a local HTTP server and focused abort/timeout/reader cases.
- Wasm feature closure: `cargo check -p lenso-http-egress-plugin
  --no-default-features --features workers --target wasm32-unknown-unknown`.
- The Auth owner must run its OIDC fixture on real workerd and deployed Workers;
  Rust/Node tests alone do not qualify that target.
