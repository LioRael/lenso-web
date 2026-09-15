# WebSocket Endpoint

`lenso.websocket.endpoint@1` is the portable backend role for an explicitly bound
WebSocket route. `describe_websocket` publishes GET paths before readiness.
`connect_websocket` receives normalized route data, credential evidence and the
client's offered subprotocols. The backend remains the final authorization owner.

A provider's first frame must be `accept`; an optional `protocol` must be one of
the offered protocol tokens. No other fields are valid on that frame. Domain
rejection before accept remains an HTTP handshake rejection. A second accept is
a protocol failure.

After accept, both peers exchange the same bounded frame vocabulary:

- `text`: only `text` is present, including the empty string.
- `binary`: only `body` is present, including empty bytes.
- `close`: only an allowed wire close code and optional UTF-8 reason are present.
  The reason is at most 123 encoded bytes; the schema's character bound alone is
  insufficient. Reserved codes 1004, 1005, 1006 and 1015 cannot go on the wire.

Ping/pong and fragmentation belong to transport. A Close frame ends application
data in that direction. Transport disconnect or half-close alone never proves a
successful Plugin terminal result. Exactly one Kernel terminal outcome completes
the session; cancellation and runtime failure cannot be converted into success.
Transport queue/message limits apply independently of the business contract.

The descriptor and schemas are authoritative; regenerate `src/generated.rs`
with the workspace-pinned `lenso-contract-codegen`. This new 1.0.0 contract has no
previous released revision. Native and Workers ingress use this same contract and shared frame validator.
The duplex fixture verifies real network framing on both targets. Workers proof
receipts are in Runtime `experiments/workers-g2/evidence/duplex.json`.
The contract alone does not install an ingress or select an authorization provider.
