# ADR 0002: Coordinate same-port multi-lane Web Ingress above Modules

- Status: accepted
- Date: 2026-08-23
- Upstream: Lenso ADR 0063 and ADR 0064

## Context

One Web Ingress Module is owned by one Execution Lane. Replicating HTTP work
across lanes must not add network APIs to the portable Kernel, make listener
ownership ambient, or let the operating system send a request to a lane that
does not own its route.

`SO_REUSEPORT` distributes connections rather than requests. It is also
platform-specific and can produce persistent skew for long-lived HTTP/1.1
connections. It is only correct when every socket exposes an identical route
manifest. It cannot safely implement route sharding.

## Decision

The default same-port design is a target Runner or Host listener coordinator.
It binds the address once, validates every ready replica's canonical
`WebIngressRouteManifest`, and hands accepted connections to healthy lanes.
Connection transfer is target runtime infrastructure, not a Capability and not
a Kernel responsibility.

All replicas in one listener group must expose exactly the same method, path,
and route-id tuples before the listener starts accepting traffic. A mismatch
fails closed. The public manifest and `ensure_equivalent` check are the
executable validation boundary for that coordinator.

The coordinator owns global admission and listener shutdown. Each lane keeps a
local concurrency bound as a second containment boundary. Shutdown stops new
accepts first, asks connection owners to drain, and then applies the existing
bounded Kernel shutdown policy.

An explicit, platform-gated `SO_REUSEPORT` mode may be added later for identical
stateless replica sets after Linux and macOS behavior is benchmarked. It is not
the portable default and must use the same manifest validation.

## Consequences

- The current single-lane Ingress remains portable across native hosts and does
  not gain socket-sharing switches prematurely.
- Route-sharded same-port replicas are rejected; they require content-aware
  dispatch by a coordinator or proxy.
- Cross-lane request transfer remains the fallback for providers that cannot be
  replicated with the Ingress lane.
- A later coordinator milestone must define lane health, load selection,
  connection drain deadlines, global versus local backpressure, and structural
  diagnostics before exposing same-port replication as product behavior.
