# Workers WebSocket transport

`@lenso/web-ingress-workers` connects a backend-authorized Lenso WebSocket session
to Cloudflare's `WebSocketPair`. Use `createWebSocketTransport()` as the
`upgradeWebSocket` option of Runtime's `createStreamingHttpHandler`.

The Web Ingress Plugin owns routes, credentials, Origin policy, subprotocol
selection and frame validation. The transport does not authorize an upgrade.
Every read/write enters Wasm through the Runtime session lease. The shared event
scope owns native close and fences late events when that generation is abandoned.

Defaults bound individual messages to 64 KiB, pending inbound messages to 16,
pending inbound bytes to 1 MiB, and total bidirectional payload to 1 MiB. Match
these limits to the Ingress configuration. Empty messages remain valid. Ping/Pong
is transport-owned. A peer close without a status is normalized to 1000; abnormal
closure fails the session. Sessions have a separate Runtime lifetime deadline.

Workers `send()` returns immediately and exposes no drain acknowledgement. This
transport bounds total output but does not claim network backpressure or peer
acknowledgement. Incoming platform buffering precedes the message event. HTTP
response streams, in contrast, pull only on downstream demand.

Run `npm test` after installing the Runtime package. Before registry publication,
install an explicitly supplied Runtime archive with `--no-save --package-lock=false`.
The package itself imports no sibling checkout.
