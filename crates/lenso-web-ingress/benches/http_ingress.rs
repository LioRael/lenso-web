use std::{
    io::{BufRead as _, BufReader as StdBufReader, Write as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    rc::Rc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::State,
    http::{
        HeaderName, HeaderValue, Request, Uri,
        header::{AUTHORIZATION, COOKIE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use futures::future::join_all;
use lenso_authoring::{
    Binding, CapabilityEndpoint, CapabilityRequirement, ContractInput, Module, PackageInput,
    PackageSource, ProjectAuthoring, ProjectFile, ResolutionOptions,
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, DescribeRequest, DescribeResponse,
    DescribeResponseRoutesItem, EndpointDescribe, EndpointEndpoint, EndpointHandle,
    EndpointProvider, HANDLE_OPERATION, HandleRequest, HandleResponse,
};
use lenso_kernel::{InvocationContext, Kernel, NativeApp, NativeRequestFuture, RuntimeFailure};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use lenso_runner::TokioDriver;
use lenso_web_ingress::{PACKAGE_ID, PACKAGE_VERSION, WebIngressConfig, WebIngressFactory};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::LocalSet,
};
use tower::{ServiceBuilder, limit::GlobalConcurrencyLimitLayer};
use tower_http::{
    limit::RequestBodyLimitLayer,
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    set_header::SetResponseHeaderLayer,
};

const FIXTURE_PACKAGE_ID: &str = "benchmark.http-endpoint";
const REQUEST_BODY_SIZES: [usize; 3] = [0, 1024, 65_536];
const CONNECTION_COUNTS: [usize; 2] = [1, 8];
const SAMPLE_COUNT: usize = 3;
const WARMUP_REQUESTS_PER_CONNECTION: usize = 32;
const BENCHMARK_MAX_REQUEST_BYTES: usize = 1024 * 1024;
const BENCHMARK_MAX_REQUEST_HEAD_BYTES: usize = 32 * 1024;
const BENCHMARK_MAX_CONCURRENT_REQUESTS: usize = 1024;
const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const NOSNIFF_HEADER: HeaderName = HeaderName::from_static("x-content-type-options");

pub(crate) fn main() {
    main_with_observer(&mut NoopObserver);
}

pub(crate) trait MeasurementObserver {
    fn before_measure(&mut self) {}

    fn after_measure(
        &mut self,
        _server: &str,
        _request_body_size: usize,
        _connections: usize,
        _requests: usize,
    ) {
    }
}

struct NoopObserver;

impl MeasurementObserver for NoopObserver {}

pub(crate) fn main_with_observer(observer: &mut impl MeasurementObserver) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime")
        .block_on(LocalSet::new().run_until(run(observer)));
}

#[allow(dead_code)]
pub(crate) fn main_process() {
    if let Ok(server) = std::env::var("LENSO_HTTP_BENCH_CHILD") {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("benchmark child runtime")
            .block_on(LocalSet::new().run_until(run_child(&server)));
        return;
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark client runtime")
        .block_on(LocalSet::new().run_until(run_process_benchmark()));
}

#[allow(dead_code)]
async fn run_process_benchmark() {
    let selected = std::env::var("LENSO_HTTP_BENCH_SERVER").ok();
    println!("Independent-process HTTP ingress benchmark");
    println!(
        "server\trequest_body_bytes\tconnections\trequests\tmedian_req_s\tsamples_req_s\tp50_us\tp99_us"
    );
    for request_body_size in REQUEST_BODY_SIZES {
        if !selected_number("LENSO_HTTP_BENCH_BODY_BYTES", request_body_size) {
            continue;
        }
        let requests = request_count(request_body_size);
        for connections in CONNECTION_COUNTS {
            if !selected_number("LENSO_HTTP_BENCH_CONNECTIONS", connections) {
                continue;
            }
            for server in ["axum", "axum_transport", "bridge", "lenso"] {
                if selected
                    .as_deref()
                    .is_some_and(|selected| selected != server)
                {
                    continue;
                }
                let (mut child, address) = spawn_server_process(server);
                report_process(server, address, request_body_size, connections, requests).await;
                child.kill().expect("stop benchmark server process");
                child.wait().expect("reap benchmark server process");
            }
        }
    }
}

#[allow(dead_code)]
fn spawn_server_process(server: &str) -> (Child, SocketAddr) {
    let mut child = Command::new(std::env::current_exe().expect("benchmark executable"))
        .env("LENSO_HTTP_BENCH_CHILD", server)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn benchmark server process");
    let stdout = child.stdout.take().expect("benchmark child stdout");
    let mut stdout = StdBufReader::new(stdout);
    let mut address = String::new();
    stdout
        .read_line(&mut address)
        .expect("read benchmark child address");
    let address = address
        .trim()
        .parse()
        .expect("benchmark child printed a socket address");
    (child, address)
}

#[allow(dead_code)]
async fn run_child(server: &str) {
    match server {
        "lenso" => {
            let ingress = WebIngressFactory::default();
            let app = start_lenso(&ingress).await.expect("Lenso App should start");
            let address = ingress.local_address().expect("Ingress should be bound");
            hold_server(address, app).await;
        }
        "axum" => {
            let (address, shutdown) = start_axum().await;
            hold_server(address, shutdown).await;
        }
        "axum_transport" => {
            let (address, shutdown) = start_axum_transport().await;
            hold_server(address, shutdown).await;
        }
        "bridge" => {
            let (address, shutdown) = start_bridge().await;
            hold_server(address, shutdown).await;
        }
        _ => panic!("unknown benchmark child server `{server}`"),
    }
}

#[allow(dead_code)]
async fn hold_server<T>(address: SocketAddr, _guard: T) {
    println!("{address}");
    std::io::stdout()
        .flush()
        .expect("flush benchmark child address");
    std::future::pending::<()>().await;
}

#[allow(dead_code)]
async fn report_process(
    server: &str,
    address: SocketAddr,
    request_body_size: usize,
    connections: usize,
    requests: usize,
) {
    let mut observer = NoopObserver;
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(
            measure(
                &mut observer,
                server,
                address,
                request_body_size,
                connections,
                requests,
            )
            .await
            .as_secs_f64(),
        );
    }
    let request_count = u32::try_from(requests).expect("benchmark request count fits in u32");
    let mut rates = samples
        .into_iter()
        .map(|seconds| f64::from(request_count) / seconds)
        .collect::<Vec<_>>();
    rates.sort_by(f64::total_cmp);
    let mut client = Client::connect(address)
        .await
        .expect("connect latency benchmark client");
    let mut latencies = Vec::with_capacity(512);
    for _ in 0..512 {
        let started = Instant::now();
        client.requests(request_body_size, 1).await;
        latencies.push(started.elapsed().as_micros());
    }
    latencies.sort_unstable();
    let rendered = rates
        .iter()
        .map(|rate| format!("{rate:.0}"))
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "{server}\t{request_body_size}\t{connections}\t{requests}\t{:.0}\t{rendered}\t{}\t{}",
        rates[rates.len() / 2],
        latencies[latencies.len() / 2],
        latencies[latencies.len() * 99 / 100],
    );
}

async fn run(observer: &mut impl MeasurementObserver) {
    let ingress = WebIngressFactory::default();
    let app = start_lenso(&ingress).await.expect("Lenso App should start");
    let lenso_address = ingress.local_address().expect("Ingress should be bound");
    let (axum_address, axum_shutdown) = start_axum().await;
    let (transport_address, transport_shutdown) = start_axum_transport().await;
    let (bridge_address, bridge_shutdown) = start_bridge().await;
    let selected = std::env::var("LENSO_HTTP_BENCH_SERVER").ok();
    let servers = [
        ("axum", axum_address),
        ("axum_transport", transport_address),
        ("bridge", bridge_address),
        ("lenso", lenso_address),
    ];

    println!(
        "HTTP ingress benchmark: samples={SAMPLE_COUNT}, warmup_per_connection={WARMUP_REQUESTS_PER_CONNECTION}"
    );
    println!("server\trequest_body_bytes\tconnections\trequests\tmedian_req_s\tsamples_req_s");
    for request_body_size in REQUEST_BODY_SIZES {
        if !selected_number("LENSO_HTTP_BENCH_BODY_BYTES", request_body_size) {
            continue;
        }
        let requests = request_count(request_body_size);
        for connections in CONNECTION_COUNTS {
            if !selected_number("LENSO_HTTP_BENCH_CONNECTIONS", connections) {
                continue;
            }
            for (server, address) in servers {
                if selected
                    .as_deref()
                    .is_none_or(|selected| selected == server)
                {
                    report(
                        observer,
                        server,
                        address,
                        request_body_size,
                        connections,
                        requests,
                    )
                    .await;
                }
            }
        }
    }

    let _ = axum_shutdown.send(());
    let _ = transport_shutdown.send(());
    let _ = bridge_shutdown.send(());
    app.shutdown(Duration::from_secs(1)).await;
}

fn selected_number(name: &str, actual: usize) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .is_none_or(|selected| selected == actual)
}

fn request_count(request_body_size: usize) -> usize {
    let default = if request_body_size >= 65_536 {
        2_048
    } else {
        20_000
    };
    std::env::var("LENSO_HTTP_BENCH_REQUESTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

async fn report(
    observer: &mut impl MeasurementObserver,
    server: &str,
    address: SocketAddr,
    request_body_size: usize,
    connections: usize,
    requests: usize,
) {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(
            measure(
                observer,
                server,
                address,
                request_body_size,
                connections,
                requests,
            )
            .await
            .as_secs_f64(),
        );
    }
    let mut rates = samples
        .into_iter()
        .map(|seconds| {
            f64::from(u32::try_from(requests).expect("benchmark request count fits u32")) / seconds
        })
        .collect::<Vec<_>>();
    rates.sort_by(f64::total_cmp);
    let rendered = rates
        .iter()
        .map(|rate| format!("{rate:.0}"))
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "{server}\t{request_body_size}\t{connections}\t{requests}\t{:.0}\t{rendered}",
        rates[rates.len() / 2]
    );
}

async fn measure(
    observer: &mut impl MeasurementObserver,
    server: &str,
    address: SocketAddr,
    request_body_size: usize,
    connections: usize,
    requests: usize,
) -> Duration {
    let mut clients = join_all((0..connections).map(|_| Client::connect(address)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("connect benchmark clients");
    join_all(
        clients
            .iter_mut()
            .map(|client| client.requests(request_body_size, WARMUP_REQUESTS_PER_CONNECTION)),
    )
    .await;

    observer.before_measure();
    let started = Instant::now();
    let base = requests / connections;
    let remainder = requests % connections;
    join_all(clients.iter_mut().enumerate().map(|(index, client)| {
        client.requests(request_body_size, base + usize::from(index < remainder))
    }))
    .await;
    let elapsed = started.elapsed();
    observer.after_measure(server, request_body_size, connections, requests);
    elapsed
}

struct Client {
    stream: BufReader<TcpStream>,
    line: String,
}

impl Client {
    async fn connect(address: SocketAddr) -> std::io::Result<Self> {
        Ok(Self {
            stream: BufReader::new(TcpStream::connect(address).await?),
            line: String::with_capacity(128),
        })
    }

    async fn requests(&mut self, request_body_size: usize, count: usize) {
        let head = format!(
            "POST /bench/{request_body_size} HTTP/1.1\r\nHost: benchmark\r\nConnection: keep-alive\r\nContent-Length: {request_body_size}\r\n\r\n"
        );
        let mut request = Vec::with_capacity(head.len() + request_body_size);
        request.extend_from_slice(head.as_bytes());
        request.resize(request.len() + request_body_size, b'x');
        for _ in 0..count {
            self.stream
                .get_mut()
                .write_all(&request)
                .await
                .expect("write request");
            self.read_response(request_body_size).await;
        }
    }

    async fn read_response(&mut self, expected_body_bytes: usize) {
        self.line.clear();
        self.stream
            .read_line(&mut self.line)
            .await
            .expect("read status line");
        assert!(
            self.line.contains(" 200 "),
            "unexpected status: {}",
            self.line
        );
        let mut content_length = None;
        loop {
            self.line.clear();
            self.stream
                .read_line(&mut self.line)
                .await
                .expect("read response header");
            if self.line == "\r\n" {
                break;
            }
            if let Some((name, value)) = self.line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = Some(value.trim().parse::<usize>().expect("content length"));
            }
        }
        let content_length = content_length.expect("fixed response body length");
        assert_eq!(content_length, expected_body_bytes);
        let mut remaining = content_length;
        let mut buffer = [0_u8; 8 * 1024];
        while remaining != 0 {
            let take = remaining.min(buffer.len());
            self.stream
                .read_exact(&mut buffer[..take])
                .await
                .expect("read response body");
            remaining -= take;
        }
    }
}

async fn direct_response(uri: Uri, body: Bytes) -> impl IntoResponse {
    let expected = expected_body_size(&uri);
    assert_eq!(body.len(), expected);
    assert!(body.iter().all(|byte| *byte == b'x'));
    body
}

fn expected_body_size(uri: &Uri) -> usize {
    uri.path()
        .strip_prefix("/bench/")
        .expect("benchmark route prefix")
        .parse()
        .expect("benchmark body size")
}

async fn start_axum() -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind Axum benchmark listener");
    let address = listener.local_addr().expect("Axum benchmark address");
    let app = Router::new().route("/{*path}", post(direct_response));
    let (shutdown, receive_shutdown) = tokio::sync::oneshot::channel();
    tokio::task::spawn_local(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = receive_shutdown.await;
            })
            .await
            .expect("serve Axum benchmark");
    });
    (address, shutdown)
}

#[derive(Clone, Copy, Debug)]
struct BenchmarkRequestId;

impl MakeRequestId for BenchmarkRequestId {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        Some(RequestId::new(HeaderValue::from_static("benchmark")))
    }
}

fn with_transport(app: Router) -> Router {
    let transport = ServiceBuilder::new()
        .layer(SetSensitiveRequestHeadersLayer::new([
            AUTHORIZATION,
            COOKIE,
        ]))
        .layer(SetRequestIdLayer::new(
            REQUEST_ID_HEADER.clone(),
            BenchmarkRequestId,
        ))
        .layer(PropagateRequestIdLayer::new(REQUEST_ID_HEADER))
        .layer(SetResponseHeaderLayer::overriding(
            NOSNIFF_HEADER,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(RequestBodyLimitLayer::new(BENCHMARK_MAX_REQUEST_BYTES))
        .layer(GlobalConcurrencyLimitLayer::new(
            BENCHMARK_MAX_CONCURRENT_REQUESTS,
        ));
    app.layer(middleware::from_fn(enforce_benchmark_head_limit))
        .layer(transport)
}

async fn enforce_benchmark_head_limit(request: Request<Body>, next: Next) -> Response {
    let size = request.method().as_str().len()
        + request
            .uri()
            .path_and_query()
            .map_or(0, |path| path.as_str().len())
        + request
            .headers()
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum::<usize>();
    assert!(size <= BENCHMARK_MAX_REQUEST_HEAD_BYTES);
    next.run(request).await
}

async fn start_axum_transport() -> (SocketAddr, oneshot::Sender<()>) {
    start_benchmark_router(with_transport(
        Router::new().route("/{*path}", post(direct_response)),
    ))
    .await
}

#[derive(Debug)]
struct BridgeCall {
    body: Bytes,
    response: oneshot::Sender<Bytes>,
}

#[derive(Clone, Debug)]
struct BenchmarkBridge {
    sender: mpsc::Sender<BridgeCall>,
}

async fn bridge_response(State(bridge): State<BenchmarkBridge>, uri: Uri, body: Bytes) -> Bytes {
    let expected = expected_body_size(&uri);
    assert_eq!(body.len(), expected);
    assert!(body.iter().all(|byte| *byte == b'x'));
    let (response, receive) = oneshot::channel();
    bridge
        .sender
        .send(BridgeCall { body, response })
        .await
        .expect("benchmark bridge dispatcher");
    receive.await.expect("benchmark bridge response")
}

async fn start_bridge() -> (SocketAddr, oneshot::Sender<()>) {
    let (sender, mut receiver) = mpsc::channel::<BridgeCall>(BENCHMARK_MAX_CONCURRENT_REQUESTS);
    tokio::task::spawn_local(async move {
        while let Some(call) = receiver.recv().await {
            let _ = call.response.send(call.body);
        }
    });
    start_benchmark_router(with_transport(
        Router::new()
            .route("/{*path}", post(bridge_response))
            .with_state(BenchmarkBridge { sender }),
    ))
    .await
}

async fn start_benchmark_router(app: Router) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind layered benchmark listener");
    let address = listener.local_addr().expect("layered benchmark address");
    let (shutdown, receive_shutdown) = oneshot::channel();
    tokio::task::spawn_local(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = receive_shutdown.await;
            })
            .await
            .expect("serve layered benchmark");
    });
    (address, shutdown)
}

async fn start_lenso(ingress: &WebIngressFactory) -> Result<NativeApp, RuntimeFailure> {
    let endpoint = FixtureEndpointFactory::new();
    let registry = lenso_native_adapter::NativeModuleRegistry::new()
        .with_factory(endpoint)
        .with_factory(ingress.clone());
    let plan = project()
        .resolve(&workspace_root(), &ResolutionOptions::default())
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: error.to_string(),
        })?;
    Kernel::start_native(plan.plan().clone(), TokioDriver::new(), registry).await
}

fn project() -> ProjectFile {
    let mut project = ProjectFile::default();
    project.contracts_mut().push(ContractInput::new(
        CAPABILITY_ID,
        DESCRIPTOR_VERSION,
        "crates/lenso-capability-http-endpoint/capability.json",
        "crates/lenso-capability-http-endpoint/src/generated.rs",
        "crates/lenso-capability-http-endpoint/generated/bindings.ts",
    ));
    for package in [FIXTURE_PACKAGE_ID, PACKAGE_ID] {
        project.packages_mut().insert(
            package.to_owned(),
            PackageInput::new(package, PackageSource::Cargo, PACKAGE_VERSION)
                .with_package_name("lenso-web-ingress")
                .with_manifest("crates/lenso-web-ingress/Cargo.toml")
                .with_lockfile("Cargo.lock"),
        );
    }
    project.composition_mut().add_module(
        Module::new("benchmark-endpoint", FIXTURE_PACKAGE_ID).with_capability(
            CapabilityEndpoint::request(
                CAPABILITY_ID,
                DESCRIPTOR_VERSION,
                [DESCRIBE_OPERATION, HANDLE_OPERATION],
            ),
        ),
    );
    project.composition_mut().add_module(
        Module::new("web-ingress", PACKAGE_ID)
            .with_configuration_schema("crates/lenso-web-ingress/config.schema.json")
            .with_configuration(serde_json::to_value(WebIngressConfig::default()).unwrap())
            .with_requirement(CapabilityRequirement::many(
                CAPABILITY_ID,
                DESCRIPTOR_VERSION,
            )),
    );
    project.composition_mut().add_binding(Binding::new(
        "web-ingress",
        CAPABILITY_ID,
        DESCRIPTOR_VERSION,
        "benchmark-endpoint",
    ));
    project
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[derive(Clone, Debug)]
struct FixtureEndpointFactory {
    routes: Rc<Vec<DescribeResponseRoutesItem>>,
}

impl FixtureEndpointFactory {
    fn new() -> Self {
        Self {
            routes: Rc::new(
                REQUEST_BODY_SIZES
                    .into_iter()
                    .map(|size| DescribeResponseRoutesItem {
                        method: "POST".to_owned(),
                        path: format!("/bench/{size}"),
                        route_id: size.to_string(),
                    })
                    .collect(),
            ),
        }
    }
}

impl NativeModuleFactory for FixtureEndpointFactory {
    fn package_id(&self) -> &'static str {
        FIXTURE_PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }

    fn instantiate(
        &self,
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        Ok(NativeModuleInstance::new(vec![Rc::new(
            EndpointEndpoint::new(FixtureEndpoint {
                routes: self.routes.clone(),
            }),
        )]))
    }
}

#[derive(Debug)]
struct FixtureEndpoint {
    routes: Rc<Vec<DescribeResponseRoutesItem>>,
}

impl EndpointProvider for FixtureEndpoint {
    fn describe(
        &self,
        _context: InvocationContext,
        _request: DescribeRequest,
    ) -> NativeRequestFuture<EndpointDescribe> {
        Box::pin(futures::future::ready(Ok(Ok(DescribeResponse {
            routes: self.routes.as_ref().clone(),
        }))))
    }

    fn handle(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> NativeRequestFuture<EndpointHandle> {
        let expected = request
            .route_id
            .parse::<usize>()
            .expect("benchmark route is a body size");
        assert_eq!(request.body.len(), expected);
        assert!(request.body.iter().all(|byte| *byte == b'x'));
        Box::pin(futures::future::ready(Ok(Ok(HandleResponse {
            body: request.body,
            headers: Vec::new(),
            status: 200,
        }))))
    }
}
