# Bound Web Ingress resources

Status: IMPLEMENTED AND VALIDATED

Finding: accepted connections are unbounded and request header/body/idle reads lack complete time bounds, so slow clients can retain transport and request-admission resources.

Scope:
- add immutable connection, header-read, body-read, and idle limits to `WebIngressConfig`;
- enforce a hard live-connection budget while preserving graceful shutdown;
- return stable bounded failures for body-read expiry and test slow header/body/idle clients.

Implementation:
- `WebIngressConfig` now owns validated live-connection, HTTP/1 header-read,
  total body-read, activity-reset idle, and shutdown-drain deadlines;
- HTTP/2 advertises the same per-connection concurrent-stream ceiling as the
  global request admission limit and refuses excess simultaneous streams;
- the uncontended request-admission path uses cancellation check plus
  `try_acquire`, already-complete bodies bypass timeout registration, and the
  idle watchdog tracks `active_requests` plus `idle_since` without per-request
  channel wakeups;
- the listener acquires one owned permit before a direct or replicated socket
  enters an accepted queue, so the configured live-connection count is a hard
  listener-group ceiling;
- shutdown first asks Hyper to drain, then hard-closes only after the configured
  grace; the default grace matches the request deadline.

Validation:
- Ingress unit tests: 18 passed, including the two-replica hard-cap, permit fast
  path, empty-body fast path, and last-completion idle-deadline regressions;
- `http_ingress`: 18 passed, including one-connection HTTP/2 stream pressure,
  slow header/body/idle release, 408 body timeout recovery, graceful response
  drain, and bounded slow-write shutdown;
- workspace fmt/check/clippy (`-D warnings`)/test all passed;
- release profile evidence: `docs/evidence/web-execution-profile-2026-08-30.json`;
  final 8-connection Lenso throughput was 91,758 requests/s, or 92.61% of the
  same-run policy Axum baseline, versus 84,719 and 84.92% before the hot-path
  revision. An immediate repeat measured 93.27%.
