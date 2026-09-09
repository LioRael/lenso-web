use std::{
    cell::Cell,
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, bail};
use futures::future::LocalBoxFuture;
use lenso_app_plan::{
    AppComposition, CapabilityBinding, CapabilityEndpointPlan, CapabilityRequirementPlan,
    PluginInstancePlan, RequestAdmissionPlan, ResolvedAppPlan,
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, DescribeRequest, DescribeResponse,
    DescribeResponseRoutesItem, EndpointDescribe, EndpointEndpoint, EndpointHandle,
    EndpointProvider, HANDLE_OPERATION, HandleRequest, HandleResponse, HandleResponseHeadersItem,
};
use lenso_kernel::{
    InvocationContext, Kernel, NativeRequestFuture, RuntimeDiagnostics, RuntimeFailure,
    ShutdownOutcome,
};
use lenso_native_adapter::{
    NativePluginFactory, NativePluginFactoryContext, NativePluginInstance, NativePluginRegistry,
};
use lenso_otel_plugin::{
    OTEL_PLUGIN_PACKAGE_ID, OtelPluginFactory, OtelSignal, OtelSpan, OtlpHttpExporter,
    TelemetryHandle, TraceContext,
};
use lenso_runner::TokioDriver;
use lenso_web_ingress_plugin::{
    PACKAGE_ID as INGRESS_PACKAGE_ID, WebIngressConfig, WebIngressFactory, WebIngressMiddleware,
    WebIngressMiddlewareOutcome, WebIngressRequest, WebIngressResponse,
};
use tokio::task::LocalSet;

const SAMPLE_PACKAGE_ID: &str = "lenso.example.observe-sample-web";
const OTEL_INSTANCE: &str = "otel";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let settings = Settings::read()?;
    let token = std::fs::read_to_string(&settings.token_file)
        .with_context(|| format!("read Observe token at {}", settings.token_file.display()))?;
    let exporter =
        OtlpHttpExporter::new(&settings.otlp_endpoint, token.trim(), "observe-sample-web")?;
    let diagnostics = RuntimeDiagnostics::new();
    let otel = OtelPluginFactory::new(diagnostics.clone(), exporter);
    let telemetry = otel.telemetry_for(OTEL_INSTANCE);
    let ingress_config = WebIngressConfig::default()
        .with_bind_address(settings.bind_address)
        .map_err(anyhow::Error::msg)?;
    let ingress = WebIngressFactory::new().with_middleware(ObserveRequests::new(telemetry));
    let app = Kernel::start_native_with_diagnostics(
        plan(&ingress_config)?,
        TokioDriver::new(),
        NativePluginRegistry::new()
            .with_factory(SampleEndpointFactory)
            .with_factory(ingress.clone())
            .with_factory(otel),
        diagnostics,
    )
    .await;
    let app = match app {
        Ok(app) => app,
        Err(error) => bail!("sample App failed: {error:?}"),
    };
    let address = ingress
        .local_address()
        .context("Web Ingress did not publish its bound address")?;
    println!("Observe sample Web App ready at http://{address}/orders/42");
    println!("Send one request, then open the sample App's Observe Workspace in Console.");
    tokio::signal::ctrl_c().await.context("wait for Ctrl-C")?;
    if app.shutdown(Duration::from_secs(3)).await != ShutdownOutcome::Clean {
        bail!("sample App did not shut down cleanly");
    }
    Ok(())
}

#[derive(Debug)]
struct Settings {
    bind_address: SocketAddr,
    otlp_endpoint: String,
    token_file: PathBuf,
}

impl Settings {
    fn read() -> Result<Self> {
        let bind_address = std::env::var("LENSO_SAMPLE_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:3000".to_owned())
            .parse()
            .context("parse LENSO_SAMPLE_BIND_ADDRESS")?;
        let otlp_endpoint = std::env::var("LENSO_OBSERVE_OTLP_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:4318".to_owned());
        let token_file = std::env::var_os("LENSO_OBSERVE_TOKEN_FILE").map_or_else(
            || PathBuf::from(".lenso/console/observe/otlp-token"),
            PathBuf::from,
        );
        Ok(Self {
            bind_address,
            otlp_endpoint,
            token_file,
        })
    }
}

#[derive(Clone, Debug)]
struct ObserveRequests {
    telemetry: TelemetryHandle,
    clock: Instant,
    next_span: Rc<Cell<u64>>,
}

impl ObserveRequests {
    fn new(telemetry: TelemetryHandle) -> Self {
        Self {
            telemetry,
            clock: Instant::now(),
            next_span: Rc::new(Cell::new(1)),
        }
    }
}

#[derive(Clone, Debug)]
struct ActiveRequestSpan {
    context: TraceContext,
    started_at: Duration,
}

impl WebIngressMiddleware for ObserveRequests {
    fn identity(&self) -> &'static str {
        "lenso.observe.sample:v1"
    }

    fn before_request<'a>(
        &'a self,
        request: &'a mut WebIngressRequest,
    ) -> LocalBoxFuture<'a, Result<WebIngressMiddlewareOutcome, RuntimeFailure>> {
        let sequence = self.next_span.get();
        self.next_span.set(sequence.saturating_add(1));
        let traceparent = format!("00-{sequence:032x}-{sequence:016x}-01");
        let context = TraceContext::from_traceparent(&traceparent, None).map_err(|error| {
            RuntimeFailure::PluginFailure {
                detail: format!("sample trace identity failed: {error}"),
            }
        });
        if let Ok(context) = context {
            request.extensions_mut().insert(ActiveRequestSpan {
                context,
                started_at: self.clock.elapsed(),
            });
        }
        Box::pin(futures::future::ready(Ok(
            WebIngressMiddlewareOutcome::Continue,
        )))
    }

    fn after_response<'a>(
        &'a self,
        request: &'a WebIngressRequest,
        response: &'a mut WebIngressResponse,
    ) -> LocalBoxFuture<'a, Result<(), RuntimeFailure>> {
        if let Some(active) = request.extensions().get::<ActiveRequestSpan>() {
            let method = request.method().to_string();
            let route = request.uri().path().to_owned();
            let attributes = BTreeMap::from([
                ("http.request.method".to_owned(), method.clone()),
                ("http.route".to_owned(), route.clone()),
                (
                    "http.response.status_code".to_owned(),
                    response.status().as_u16().to_string(),
                ),
            ]);
            let _ = self.telemetry.try_emit(OtelSignal::Span(OtelSpan {
                name: format!("{method} {route}"),
                trace_context: active.context.clone(),
                parent_span_id: None,
                started_at: active.started_at,
                ended_at: Some(self.clock.elapsed()),
                attributes,
            }));
        }
        Box::pin(futures::future::ready(Ok(())))
    }
}

#[derive(Clone, Copy, Debug)]
struct SampleEndpointFactory;

impl NativePluginFactory for SampleEndpointFactory {
    fn package_id(&self) -> &'static str {
        SAMPLE_PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    fn instantiate(
        &self,
        _context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::new(vec![Rc::new(
            EndpointEndpoint::new(SampleEndpoint),
        )]))
    }
}

#[derive(Debug)]
struct SampleEndpoint;

impl EndpointProvider for SampleEndpoint {
    fn describe(
        &self,
        _context: InvocationContext,
        _request: DescribeRequest,
    ) -> NativeRequestFuture<EndpointDescribe> {
        Box::pin(futures::future::ready(Ok(Ok(DescribeResponse {
            routes: vec![DescribeResponseRoutesItem {
                method: "GET".to_owned(),
                openapi: None,
                path: "/orders/{order_id}".to_owned(),
                route_id: "sample.orders.read".to_owned(),
            }],
        }))))
    }

    fn handle(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> NativeRequestFuture<EndpointHandle> {
        let order_id = request
            .path_parameters
            .iter()
            .find(|parameter| parameter.name == "order_id")
            .map_or("unknown", |parameter| parameter.value.as_str())
            .to_owned();
        Box::pin(futures::future::ready(Ok(Ok(HandleResponse {
            body: format!(r#"{{"id":"{order_id}","status":"ready"}}"#)
                .into_bytes()
                .into(),
            headers: vec![HandleResponseHeadersItem {
                name: "content-type".to_owned(),
                value: "application/json; charset=utf-8".to_owned(),
            }],
            status: 200,
        }))))
    }
}

fn plan(config: &WebIngressConfig) -> Result<ResolvedAppPlan> {
    let endpoint = PluginInstancePlan::new("sample-http", SAMPLE_PACKAGE_ID).with_capability(
        CapabilityEndpointPlan::new(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            [DESCRIBE_OPERATION, HANDLE_OPERATION],
        ),
    );
    let ingress = PluginInstancePlan::new("web-ingress", INGRESS_PACKAGE_ID)
        .with_requirement(CapabilityRequirementPlan::many(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
        ))
        .with_configuration(serde_json::to_string(config)?);
    let otel = PluginInstancePlan::new(OTEL_INSTANCE, OTEL_PLUGIN_PACKAGE_ID);
    let (queue_capacity, max_concurrency) = config.endpoint_admission_limits();
    AppComposition::new(
        vec![endpoint, ingress, otel],
        vec![
            CapabilityBinding::new(
                "web-ingress",
                CAPABILITY_ID,
                DESCRIPTOR_VERSION,
                "sample-http",
            )
            .with_admission(RequestAdmissionPlan::new(queue_capacity, max_concurrency)),
        ],
    )
    .resolve()
    .map_err(anyhow::Error::msg)
}
