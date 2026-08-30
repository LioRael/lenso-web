# Align HTTP Endpoint admission

Status: IMPLEMENTED AND VALIDATED

Finding: Ingress defaults to 128 concurrent requests while source-first Endpoint bindings inherit one execution slot and no queue unless the resolved Plan explicitly overrides admission, causing avoidable 503 responses under ordinary concurrency.

Scope:
- keep admission in resolved Plan/Host policy, not the Capability Descriptor;
- make the binding override explicit in the copyable source-first composition path and benchmark fixture;
- add a saturation regression and refresh the benchmark command/evidence.

Implementation:
- the Capability Descriptor and generated bindings remain unchanged;
- `WebIngressConfig::endpoint_admission_limits` exposes the Host-owned
  queue/concurrency pair and the README shows the exact `HostBinding` seam;
- the real source-first test composition builds `PluginDescriptor`s,
  `HostCatalog`, `HostDefaultPlugin`, `PluginRootSnapshot`, and a
  `HostBinding::with_admission`, then resolves the immutable Plan and runs it.

Validation:
- `host_binding_admission_matches_source_first_ingress_concurrency` resolved a
  0/4 Endpoint admission Plan and completed eight overlapping requests with
  observed Endpoint concurrency 4 and no inherited 0/1 rejection;
- the release saturation case completed all 512 delayed Lenso requests at the
  32-request ceiling in 448 ms with zero rejection, I/O error, or timeout;
- benchmark command and 2026-08-30 evidence were refreshed.

Boundary: this repository proves the real source-first composition path and
documents the owning Host seam. Product Host catalogs live in other
repositories, so their product-specific wiring is outside this repository and
is not claimed as implemented here.
