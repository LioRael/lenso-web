# ADR 0003: Make OpenAPI an optional Module

## Status

Accepted

## Context

HTTP Endpoint providers already publish an immutable method, path, and route ID
description before readiness. Web Ingress consumes those explicit bindings to
assemble transport routes. OpenAPI needs the same route identity plus optional
Operation Object metadata, but document publication is not required for an App
to accept HTTP traffic.

Putting an OpenAPI switch or reserved document route in Web Ingress would make
documentation a transport concern and leave a feature branch behind when the
concern is removed. Runtime provider discovery would also violate immutable App
Composition.

## Decision

`lenso.http.endpoint@1` route descriptions may carry an optional OpenAPI 3.1
Operation Object. The stable route ID remains the generated `operationId`; the
Endpoint method and path remain authoritative.

`lenso-openapi` is an ordinary optional native Rust Module. App Composition:

1. selects a keyed `lenso.openapi` Instance;
2. explicitly binds the HTTP Endpoint providers that should appear in its
   `many lenso.http.endpoint@1` requirement; and
3. explicitly binds the Module's document Endpoint to Web Ingress.

The Module assembles one deterministic OpenAPI 3.1 document during activation
and serves it from its configured static path. It fails startup for duplicate
operation IDs, duplicate method/path pairs, unsupported OpenAPI methods, or
invalid Operation Object fields that it owns.

There is deliberately no `enabled` configuration field. Selecting or removing
the Module Instance and bindings is the complete feature choice. App
Composition may bind only a subset of its Ingress providers when it wants a
partial document.

## Consequences

- Apps without the Module gain no document route, task, global registration, or
  Kernel/Ingress policy branch.
- Endpoint metadata remains portable and can cross Execution Lanes with the
  existing Capability description.
- Older Endpoint providers that omit the optional field remain valid on the
  wire; the Descriptor advances from `1.0.1` to the compatible `1.1.0` series.
- Rust packages using generated DTO struct literals must update for the new
  optional field, so the pre-1.0 Endpoint SDK package advances its minor
  version.
- Server URLs and reusable components are explicit non-secret configuration;
  listener topology is never inferred.
- Interactive documentation UI remains outside `lenso-web`.
