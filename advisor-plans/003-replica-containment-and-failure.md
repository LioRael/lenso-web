# Contain and report replicated Ingress failures

Status: IMPLEMENTED AND VALIDATED

Finding: replicated lanes share only one global request semaphore despite ADR 0002 requiring a lane-local bound, and acceptor runtime/listener failures terminate silently without waking replica servers.

Scope:
- add a fair lane-local request bound beneath the coordinator-global bound;
- propagate acceptor setup/accept failures to every replica source;
- retain clean shutdown semantics and add deterministic coordinator tests.

Implementation:
- each replica now has a fair lane-local request semaphore beneath the shared
  global ceiling;
- the coordinator sends listener/runtime/accept/transfer failures through a
  watch channel that wakes every registered replica;
- connection permits are acquired in the acceptor before lane queueing and
  travel with the socket.

Validation:
- `replicas_have_global_and_fair_lane_local_request_bounds`,
  `acceptor_failures_wake_registered_replicas`, and
  `replicas_share_one_hard_live_connection_budget` passed;
- replicated listener integration and the full Web workspace test suite passed.
