# Observe sample Web App

This runnable Lenso App proves the emitting side of Console Observe. A real
request passes through the Web Ingress Plugin and a business HTTP Endpoint
Plugin. Ingress middleware records a bounded server span through the removable
OTel Plugin, whose Host-private exporter sends OTLP/HTTP Protobuf to Observe.

Start Console with its Observe Workspace configured for this sample, then run:

```sh
LENSO_OBSERVE_TOKEN_FILE=/path/to/console/.lenso/console/observe/otlp-token \
  cargo run -p lenso-observe-sample-web-app
curl http://127.0.0.1:3000/orders/42
```

Set `LENSO_OBSERVE_OTLP_ENDPOINT` when Observe is not listening at
`http://127.0.0.1:4318`, and `LENSO_SAMPLE_BIND_ADDRESS` to change the sample
listener. Endpoint, token, and service identity remain Host inputs and do not
enter the Resolved App Plan.

The App request succeeds even when Observe is stopped or rejects telemetry.
Exporter loss remains visible through the OTel Plugin's bounded statistics; it
is never promoted into business truth or an App failure.
