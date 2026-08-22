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
```

Each provider describes its routes during activation and handles only requests
routed to its immutable binding. Web Ingress rejects invalid or colliding
routes before the App Ready Gate opens. No Module registers routes or discovers
providers after startup.
