# Event HTTP ingress seam

`lenso.web-ingress` retains its Plugin ID, descriptor, configuration schema,
Endpoint contract and default native factory. The default `native` Cargo feature
owns Hyper, TCP, Tokio I/O/time, and same-port replication. With
`default-features = false`, `WebIngressEventFactory` registers through the existing
`NativePluginFactory`/`NativePluginRegistry` boundary using an event Driver.
The name of that registry identifies its in-process Rust dispatch mechanics;
it does not imply a socket host or Tokio runtime.

## Host integration

Create a new `WebIngressEventFactory` per event App, register a clone alongside
explicit Endpoint factories, and boot the resolved Plan with the host's Driver.
Call `handle(http::Request<bytes::Bytes>, CancellationToken)` after Ready. It
returns `Result<http::Response<bytes::Bytes>, RuntimeFailure>`. Then drain and
shut down the event App with the Runner's bounded shutdown mechanism.

The factory rejects a second instantiation: a new event or generation needs a
fresh factory, credentials, cancellation token and request-ID sequence. Calling
before activation or after deactivation returns a Runtime failure; admission
before Ready or after draining begins returns HTTP 503. Endpoint bindings resolve
inside normal Plugin activation. No public API accepts arbitrary Endpoint
closures or bypasses the generated invocation boundary.

The host must:

- Collect the incoming body with the configured byte limit and total body-read
  deadline, aborting its reader/I/O on cancellation. `handle` rechecks actual body
  length and an oversized declared Content-Length. Receiving already buffered
  bytes cannot retroactively bound the host's allocation.
- Build an origin-form URI from the host URL while preserving raw path/query,
  including an explicit empty query. Append the header pairs the host exposes.
- Preserve multiple Set-Cookie response values. Serialize bodyless responses
  without supplying a body to Fetch `Response` for HEAD, 204 and 304.
- Bind cancellation to the event-owned I/O scope, bound response serialization
  and run bounded shutdown. The existing Endpoint schema has no response size
  ceiling; this seam does not invent one or claim that a buffered body is bounded
  by the schema. Hosts requiring a response cap must declare their policy.

`ingress.rs` owns credential isolation, ingress-owned header filtering,
request-ID replacement, middleware wrapping, error projection and response
normalization. `routing.rs` resolves one immutable route table and calls the
existing generated Endpoint clients with Driver-based invocation deadlines.
`server.rs` and `event.rs` supply transport I/O and admission around that shared
path. Middleware snapshots are fixed at instantiation; later factory clones do
not change the active event's middleware.

## Supported slice and transport differences

| Concern | Shared behavior / host limit |
| --- | --- |
| Native defaults | Existing `WebIngressFactory`, descriptor and configuration defaults remain available |
| TCP, keepalive, head/body reads | Native listener owns these; an event host owns its platform reads and has no listener |
| Buffered HTTP | Same generated `lenso.http.endpoint@1` descriptor version 1.1.0 and byte bodies |
| Streaming | Native support remains; event activation rejects any Stream Endpoint binding |
| CONNECT / Upgrade | Shared dispatch rejects unsupported upgrades/tunnels with 501 |
| Raw paths | Passed unchanged to existing matchit routing, including percent escapes; no second decoding pass |
| Malformed percent escapes | Existing router treats `%ZZ` literally; the host URL parser may reject/normalize before Rust |
| Repeated headers | HeaderMap preserves host-exposed entries; Fetch may combine ordinary fields and restrict methods/headers |
| Authorization folding | Duplicate fields reject; combined Bearer/Basic values containing commas/whitespace also reject |
| Forwarded headers | Ordinary evidence only; they never establish identity or grant authority |
| HEAD / 204 / 304 | Shared finalization removes body bytes; transport framing remains host-owned |
| Request IDs | Caller value is replaced by an event-owned sequence, with no process-global request state |
| Wasm panic / platform CPU termination | No promise of Rust unwinding; the Runtime event Runner owns failure/reset qualification |

The native/event regression corpus is
`tests/fixtures/http-parity-plugin/corpus.json`. Its factory implements the
existing generated `EndpointProvider` and is reusable in a real Workers probe.
`tests/http_parity.rs` runs the same vectors through native Hyper and registered
event ingress, checks expected semantics and compares response bytes/header
values. Additional tests cover event body limits, deadlines, cancellation,
readiness admission and clean shutdown. Existing native auth, streaming,
replication and HTTP regressions remain required.

A successful Wasm compile and the native event harness do not qualify real
Workers. The Runtime owner's G2 probe must run the corpus on local workerd and a
deployed Worker, record edge/Fetch normalization restrictions, and demonstrate
actual event cancellation and cleanup. Auth/Marketplace portability remains a
separate qualification gate.
