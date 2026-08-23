# ADR 0001: Bind outbound HTTP authority to exact origins

- Status: accepted
- Date: 2026-08-23
- Upstream: Lenso ADR 0039, ADR 0041, ADR 0042, and ADR 0064

## Context

Backend Modules need to call payment, messaging, AI, webhook, and internal HTTP
APIs without placing network APIs in the portable Kernel. Giving every linked
Module an ambient HTTP client would bypass App Composition and make its actual
authority impossible to review from the Resolved App Plan.

Outbound defaults also hide important behavior. Redirects can leave an allowed
destination, system proxies can reroute traffic, automatic retries can repeat a
side effect, and unbounded response reads can exhaust a Host.

## Decision

`lenso.http.client@1` is the portable request/response contract. A consumer gets
outbound HTTP authority only through an explicit binding to one
`lenso-http-egress` Module Instance.

Each Egress Instance owns an immutable set of exact allowed origins plus
connect/total timeouts, concurrency, and request/response head/body limits. It
accepts `http` and `https`, but rejects user info, fragments, disallowed origins,
`CONNECT`, `TRACE`, and caller-controlled authority or hop-by-hop headers.

The initial provider disables redirects, retries, system proxies, automatic
Referer generation, and cookie storage. A 3xx response is returned to the
consumer as protocol evidence. A later redirect, retry, or proxy feature must
be an explicit bounded policy with tests proving that it cannot expand origin
authority or repeat requests unexpectedly.

## Consequences

- Kernel and Plan remain independent of concrete networking libraries.
- Multiple Egress Instances can grant different upstream authority to different
  consumers in the same App.
- Secrets such as upstream API keys remain owned by the calling Module and its
  explicit Secrets dependency; Egress configuration contains no secret values.
- The first contract buffers bounded bodies. Streaming uploads/downloads need a
  separate Stream Capability instead of weakening these request limits.
- DNS resolution and TLS trust remain transport concerns inside Egress, while
  deployment policy decides which exact origins an App may bind.
