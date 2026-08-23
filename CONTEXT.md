# Lenso Web context

`lenso-web` owns general-purpose backend Web Interfaces and linked Rust Modules. It
consumes released Lenso Plan, Kernel, runtime, protocol, Auth, and authoring
packages; none of those repositories depend back on this one.

Console and application UI concerns are explicitly outside this repository.
`lenso.ui.contribution@1`, `lenso.web.shell@1`, Browser Adapter, pages,
navigation, and assets remain with their UI product owner.

The durable topology is:

```text
HTTP client -> Web Ingress -> many lenso.http.endpoint@1 providers
                                  |
                                  v
                           backend business Modules

backend business Module -> one lenso.http.client@1 binding -> HTTP Egress
                                                               |
                                                               v
                                                     allowed upstream origins
```

Each provider describes its routes during activation and handles only requests
routed to its immutable binding. Web Ingress rejects invalid or colliding
routes before the App Ready Gate opens. No Module registers routes or discovers
providers after startup.

Listener policy and transport limits are immutable Module Instance
configuration validated by `crates/lenso-web-ingress/config.schema.json` and
the Ingress factory before resource preparation. App Composition owns the
configured address and limits; the native factory owns only linked execution
and an observable bound address for host integration and tests.

The optional `http_endpoint!` authoring macro implements that same Capability
from one static route table. It generates both the activation description and
handler dispatch, while leaving protocol decoding, authentication orchestration,
business Capability calls, and HTTP response choices inside the owning Endpoint
Module. Dynamic providers may continue to implement `EndpointProvider`
directly; the macro creates no runtime route-registration seam.

Each Egress Instance is a separate outbound authority boundary. App
Composition binds a consumer to one provider whose immutable configuration
contains exact allowed origins and transport limits. The provider never follows
redirects, reads system proxies, stores cookies, or retries a request implicitly.
