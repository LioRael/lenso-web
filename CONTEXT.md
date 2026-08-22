# Lenso Web context

`lenso-web` owns target-App Web Interfaces and their linked Rust Modules. It
consumes released Lenso Plan, Kernel, runtime, protocol, Auth, and authoring
packages; none of those repositories depend back on this one.

The initial history was extracted from `LioRael/lenso-examples` main commit
`072364b22a6f43e13f260e4e21a73140d3d61907`. Those files originated in the
Lenso monorepo before ADR 0064 completed portable-core extraction.

The durable topology is:

```text
browser -> Web Ingress -> Browser Adapter -> resolved App Capabilities
                              |
                              v
                          Web Shell -> UI Contributions
```

Web Ingress and Browser Adapter collaborate through one owner-local,
versioned request-handler Capability. The binding is complete before boot.
No Module registers routes or discovers providers after startup.
