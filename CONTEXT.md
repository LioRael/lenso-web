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

Each Egress Instance is a separate outbound authority boundary. App
Composition binds a consumer to one provider whose immutable configuration
contains exact allowed origins and transport limits. The provider never follows
redirects, reads system proxies, stores cookies, or retries a request implicitly.
