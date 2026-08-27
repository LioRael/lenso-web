use std::{
    collections::BTreeMap,
    io::{BufRead as _, BufReader as StdBufReader, Write as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc,
    },
    thread::{self, JoinHandle},
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
    Binding, CapabilityEndpoint, CapabilityRequirement, ContractInput, PackageInput, PackageSource,
    Plugin, ProjectAuthoring, ProjectFile, ResolutionOptions,
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, DescribeRequest, DescribeResponse,
    DescribeResponseRoutesItem, EndpointDescribe, EndpointEndpoint, EndpointHandle,
    EndpointProvider, HANDLE_OPERATION, HandleRequest, HandleResponse, HandleResponseHeadersItem,
};
use lenso_kernel::{InvocationContext, Kernel, NativeApp, NativeRequestFuture, RuntimeFailure};
use lenso_native_adapter::{NativePluginFactory, NativePluginFactoryContext, NativePluginInstance};
use lenso_runner::TokioDriver;
use lenso_web_ingress_plugin::{
    PACKAGE_ID, PACKAGE_VERSION, WebIngressConfig, WebIngressFactory, WebIngressListenerCoordinator,
};
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
const LATENCY_SAMPLE_COUNT: usize = 512;
const BENCHMARK_MAX_REQUEST_BYTES: usize = 1024 * 1024;
const BENCHMARK_MAX_REQUEST_HEAD_BYTES: usize = 32 * 1024;
const BENCHMARK_MAX_CONCURRENT_REQUESTS: usize = 32;
const SATURATION_REQUESTS: usize = 512;
const SATURATION_HANDLER_DELAY: Duration = Duration::from_millis(25);
const SATURATION_CLIENT_DEADLINE: Duration = Duration::from_millis(250);
const SATURATION_PATH: &str = "/bench/saturation";
const LANE_HEADER: HeaderName = HeaderName::from_static("x-benchmark-lane");
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
pub(crate) fn main_profile() {
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
        .expect("benchmark profile runtime")
        .block_on(LocalSet::new().run_until(run_web_execution_profile()));
}

#[allow(dead_code)]
async fn run_web_execution_profile() {
    let selected = std::env::var("LENSO_HTTP_BENCH_SERVER").ok();
    let requests = request_count(0);
    println!("Web execution profile");
    println!(
        "throughput\tserver\tconnections\trequests\tmedian_req_s\tsamples_req_s\tunloaded_p50_us\tunloaded_p99_us\tserver_cpu_pct\tserver_rss_kib"
    );
    for server in ["axum", "axum_transport", "bridge", "lenso"] {
        if selected
            .as_deref()
            .is_some_and(|selected| selected != server)
        {
            continue;
        }
        let (mut child, address) = spawn_server_process(server);
        for connections in CONNECTION_COUNTS {
            if !selected_number("LENSO_HTTP_BENCH_CONNECTIONS", connections) {
                continue;
            }
            report_profile_process(server, child.id(), address, connections, requests).await;
        }
        report_saturation(server, address).await;
        stop_child(&mut child);
    }

    if selected
        .as_deref()
        .is_none_or(|selected| selected == "lenso_2lane")
    {
        let (mut child, address) = spawn_server_process("lenso_2lane");
        println!(
            "lane_distribution\tserver\tconnections\trequests\treq_s\tp50_us\tp99_us\tlane_counts"
        );
        for connections in [1, 2, 8] {
            report_lane_distribution(address, connections, requests).await;
        }
        stop_child(&mut child);
    }
}

fn stop_child(child: &mut Child) {
    child.kill().expect("stop benchmark server process");
    child.wait().expect("reap benchmark server process");
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
            let app = start_lenso(&ingress, benchmark_config(), "0")
                .await
                .expect("Lenso App should start");
            let address = ingress.local_address().expect("Ingress should be bound");
            hold_server(address, app).await;
        }
        "lenso_2lane" => {
            let (address, lanes) = start_replicated_lenso(2).await;
            hold_server(address, lanes).await;
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

fn benchmark_config() -> WebIngressConfig {
    WebIngressConfig::default()
        .with_max_concurrent_requests(BENCHMARK_MAX_CONCURRENT_REQUESTS)
        .expect("benchmark concurrency is valid")
}

async fn start_replicated_lenso(replica_count: usize) -> (SocketAddr, Vec<JoinHandle<()>>) {
    let config = benchmark_config();
    let coordinator = WebIngressListenerCoordinator::bind(config.clone(), replica_count)
        .await
        .expect("replicated benchmark listener should bind");
    let address = coordinator.local_address();
    let (ready, receive_ready) = std_mpsc::channel();
    let lanes = (0..replica_count)
        .map(|lane| {
            let coordinator = coordinator.clone();
            let config = config.clone();
            let ready = ready.clone();
            std::thread::Builder::new()
                .name(format!("lenso-benchmark-lane-{lane}"))
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("replicated benchmark runtime");
                    runtime.block_on(LocalSet::new().run_until(async move {
                        let result = async {
                            let ingress = WebIngressFactory::replicated(&coordinator)?;
                            let app = start_lenso(&ingress, config, &lane.to_string()).await?;
                            Ok::<_, RuntimeFailure>(app)
                        }
                        .await;
                        match result {
                            Ok(app) => {
                                ready.send(Ok(())).expect("report ready benchmark lane");
                                let _app = app;
                                std::future::pending::<()>().await;
                            }
                            Err(error) => {
                                ready
                                    .send(Err(format!("{error:?}")))
                                    .expect("report failed benchmark lane");
                            }
                        }
                    }));
                })
                .expect("spawn replicated benchmark lane")
        })
        .collect::<Vec<_>>();
    drop(ready);
    for result in receive_ready.iter().take(replica_count) {
        result.unwrap_or_else(|error| panic!("replicated benchmark lane failed: {error}"));
    }
    (address, lanes)
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

#[allow(dead_code)]
async fn report_profile_process(
    server: &str,
    process_id: u32,
    address: SocketAddr,
    connections: usize,
    requests: usize,
) {
    let mut observer = NoopObserver;
    let mut throughput_samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        throughput_samples.push(
            measure(&mut observer, server, address, 0, connections, requests)
                .await
                .as_secs_f64(),
        );
    }
    let request_count = u32::try_from(requests).expect("benchmark request count fits u32");
    let mut rates = throughput_samples
        .into_iter()
        .map(|seconds| f64::from(request_count) / seconds)
        .collect::<Vec<_>>();
    rates.sort_by(f64::total_cmp);
    let process_sampler = ProcessSampler::start(process_id);
    let _resource_sample = measure(&mut observer, server, address, 0, connections, requests).await;
    let metrics = process_sampler.finish();
    let mut client = Client::connect(address)
        .await
        .expect("connect latency benchmark client");
    let mut latencies = Vec::with_capacity(LATENCY_SAMPLE_COUNT);
    for _ in 0..LATENCY_SAMPLE_COUNT {
        let started = Instant::now();
        client.requests(0, 1).await;
        latencies.push(started.elapsed().as_micros());
    }
    latencies.sort_unstable();
    let rendered = rates
        .iter()
        .map(|rate| format!("{rate:.0}"))
        .collect::<Vec<_>>()
        .join(",");
    let (cpu, rss) = metrics.map_or_else(
        || ("unavailable".to_owned(), "unavailable".to_owned()),
        |metrics| {
            (
                format!("{:.1}", metrics.cpu_percent),
                metrics.rss_kib.to_string(),
            )
        },
    );
    println!(
        "throughput\t{server}\t{connections}\t{requests}\t{:.0}\t{rendered}\t{}\t{}\t{cpu}\t{rss}",
        rates[rates.len() / 2],
        percentile(&latencies, 50),
        percentile(&latencies, 99),
    );
}

#[derive(Clone, Copy, Debug)]
struct ProcessMetrics {
    cpu_percent: f64,
    rss_kib: u64,
}

#[derive(Debug)]
struct ProcessSampler {
    stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<ProcessMetrics>>>,
    thread: JoinHandle<()>,
}

impl ProcessSampler {
    fn start(process_id: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(Mutex::new(Vec::new()));
        let sample_stop = Arc::clone(&stop);
        let sample_output = Arc::clone(&samples);
        let thread = thread::Builder::new()
            .name("lenso-benchmark-process-sampler".to_owned())
            .spawn(move || {
                while !sample_stop.load(Ordering::Relaxed) {
                    if let Some(metrics) = process_metrics(process_id) {
                        sample_output
                            .lock()
                            .expect("benchmark sampler lock")
                            .push(metrics);
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            })
            .expect("spawn benchmark process sampler");
        Self {
            stop,
            samples,
            thread,
        }
    }

    fn finish(self) -> Option<ProcessMetrics> {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.join().expect("join benchmark process sampler");
        let samples = self.samples.lock().expect("benchmark sampler lock");
        if samples.is_empty() {
            return None;
        }
        Some(ProcessMetrics {
            cpu_percent: samples
                .iter()
                .map(|sample| sample.cpu_percent)
                .fold(0.0, f64::max),
            rss_kib: samples
                .iter()
                .map(|sample| sample.rss_kib)
                .max()
                .expect("non-empty process samples"),
        })
    }
}

fn process_metrics(process_id: u32) -> Option<ProcessMetrics> {
    let output = Command::new("ps")
        .args(["-o", "%cpu=", "-o", "rss=", "-p", &process_id.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let rendered = String::from_utf8(output.stdout).ok()?;
    let mut fields = rendered.split_whitespace();
    Some(ProcessMetrics {
        cpu_percent: fields.next()?.parse().ok()?,
        rss_kib: fields.next()?.parse().ok()?,
    })
}

#[allow(dead_code)]
async fn report_saturation(server: &str, address: SocketAddr) {
    let started = Instant::now();
    let outcomes = join_all((0..SATURATION_REQUESTS).map(|_| async move {
        let request = async {
            let mut client = Client::connect(address).await?;
            client.request(SATURATION_PATH, &[]).await
        };
        match tokio::time::timeout(SATURATION_CLIENT_DEADLINE, request).await {
            Ok(Ok(observation)) if (200..300).contains(&observation.status) => {
                SaturationOutcome::Success
            }
            Ok(Ok(observation)) => SaturationOutcome::HttpRejected(observation.status),
            Ok(Err(_)) => SaturationOutcome::IoError,
            Err(_) => SaturationOutcome::ClientTimeout,
        }
    }))
    .await;
    let elapsed = started.elapsed();
    let success = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, SaturationOutcome::Success))
        .count();
    let timeout = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, SaturationOutcome::ClientTimeout))
        .count();
    let io_error = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, SaturationOutcome::IoError))
        .count();
    let mut rejected = BTreeMap::new();
    for outcome in outcomes {
        if let SaturationOutcome::HttpRejected(status) = outcome {
            *rejected.entry(status).or_insert(0_usize) += 1;
        }
    }
    let rejected = rejected
        .into_iter()
        .map(|(status, count)| format!("{status}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    let concurrency_limit = if server == "axum" {
        "unbounded".to_owned()
    } else {
        BENCHMARK_MAX_CONCURRENT_REQUESTS.to_string()
    };
    println!(
        "saturation\t{server}\tlimit={concurrency_limit}\trequests={SATURATION_REQUESTS}\thandler_delay_ms={}\tclient_deadline_ms={}\tsuccess={success}\thttp_rejected={}\tio_error={io_error}\tclient_timeout={timeout}\telapsed_ms={}",
        SATURATION_HANDLER_DELAY.as_millis(),
        SATURATION_CLIENT_DEADLINE.as_millis(),
        if rejected.is_empty() {
            "none"
        } else {
            &rejected
        },
        elapsed.as_millis(),
    );
}

#[derive(Clone, Copy, Debug)]
enum SaturationOutcome {
    Success,
    HttpRejected(u16),
    IoError,
    ClientTimeout,
}

#[allow(dead_code)]
async fn report_lane_distribution(address: SocketAddr, connections: usize, requests: usize) {
    let clients = join_all((0..connections).map(|_| Client::connect(address)))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("connect lane benchmark clients");
    let base = requests / connections;
    let remainder = requests % connections;
    let started = Instant::now();
    let measurements = join_all(clients.into_iter().enumerate().map(
        |(index, mut client)| async move {
            let count = base + usize::from(index < remainder);
            let mut latencies = Vec::with_capacity(count);
            let mut lanes = BTreeMap::new();
            for _ in 0..count {
                let request_started = Instant::now();
                let observation = client
                    .request("/bench/0", &[])
                    .await
                    .expect("lane benchmark request");
                assert_eq!(observation.status, 200);
                latencies.push(request_started.elapsed().as_micros());
                let lane = observation.lane.expect("Lenso lane header");
                *lanes.entry(lane).or_insert(0_usize) += 1;
            }
            (latencies, lanes)
        },
    ))
    .await;
    let elapsed = started.elapsed();
    let mut latencies = Vec::with_capacity(requests);
    let mut lanes = BTreeMap::from([("0".to_owned(), 0_usize), ("1".to_owned(), 0_usize)]);
    for (sample_latencies, sample_lanes) in measurements {
        latencies.extend(sample_latencies);
        for (lane, count) in sample_lanes {
            *lanes.entry(lane).or_insert(0) += count;
        }
    }
    latencies.sort_unstable();
    let lane_counts = lanes
        .into_iter()
        .map(|(lane, count)| format!("{lane}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "lane_distribution\tlenso_2lane\t{connections}\t{requests}\t{:.0}\t{}\t{}\t{lane_counts}",
        f64::from(u32::try_from(requests).expect("benchmark request count fits u32"))
            / elapsed.as_secs_f64(),
        percentile(&latencies, 50),
        percentile(&latencies, 99),
    );
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    sorted[sorted.len() * percentile / 100]
}

async fn run(observer: &mut impl MeasurementObserver) {
    let ingress = WebIngressFactory::default();
    let app = start_lenso(&ingress, benchmark_config(), "0")
        .await
        .expect("Lenso App should start");
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResponseObservation {
    status: u16,
    lane: Option<String>,
}

impl Client {
    async fn connect(address: SocketAddr) -> std::io::Result<Self> {
        Ok(Self {
            stream: BufReader::new(TcpStream::connect(address).await?),
            line: String::with_capacity(128),
        })
    }

    async fn requests(&mut self, request_body_size: usize, count: usize) {
        let body = vec![b'x'; request_body_size];
        for _ in 0..count {
            let observation = self
                .request(&format!("/bench/{request_body_size}"), &body)
                .await
                .expect("benchmark request");
            assert_eq!(observation.status, 200);
        }
    }

    async fn request(&mut self, path: &str, body: &[u8]) -> std::io::Result<ResponseObservation> {
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: benchmark\r\nConnection: keep-alive\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        self.stream.get_mut().write_all(head.as_bytes()).await?;
        self.stream.get_mut().write_all(body).await?;
        self.read_response(body.len()).await
    }

    async fn read_response(
        &mut self,
        expected_body_bytes: usize,
    ) -> std::io::Result<ResponseObservation> {
        self.line.clear();
        self.stream.read_line(&mut self.line).await?;
        let status = self
            .line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| std::io::Error::other("missing HTTP status"))?
            .parse::<u16>()
            .map_err(std::io::Error::other)?;
        let mut content_length = None;
        let mut lane = None;
        loop {
            self.line.clear();
            self.stream.read_line(&mut self.line).await?;
            if self.line == "\r\n" {
                break;
            }
            if let Some((name, value)) = self.line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = Some(
                        value
                            .trim()
                            .parse::<usize>()
                            .map_err(std::io::Error::other)?,
                    );
                } else if name.eq_ignore_ascii_case(LANE_HEADER.as_str()) {
                    lane = Some(value.trim().to_owned());
                }
            }
        }
        let content_length = content_length
            .ok_or_else(|| std::io::Error::other("missing fixed response body length"))?;
        if status == 200 && content_length != expected_body_bytes {
            return Err(std::io::Error::other(format!(
                "expected {expected_body_bytes} response bytes, received {content_length}"
            )));
        }
        let mut remaining = content_length;
        let mut buffer = [0_u8; 8 * 1024];
        while remaining != 0 {
            let take = remaining.min(buffer.len());
            self.stream.read_exact(&mut buffer[..take]).await?;
            remaining -= take;
        }
        Ok(ResponseObservation { status, lane })
    }
}

async fn direct_response(uri: Uri, body: Bytes) -> Response {
    let expected = expected_body_size(&uri);
    assert_eq!(body.len(), expected);
    assert!(body.iter().all(|byte| *byte == b'x'));
    if uri.path() == SATURATION_PATH {
        tokio::time::sleep(SATURATION_HANDLER_DELAY).await;
    }
    ([(LANE_HEADER.as_str(), "axum")], body).into_response()
}

fn expected_body_size(uri: &Uri) -> usize {
    if uri.path() == SATURATION_PATH {
        return 0;
    }
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
    delay: bool,
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
        .send(BridgeCall {
            body,
            delay: uri.path() == SATURATION_PATH,
            response,
        })
        .await
        .expect("benchmark bridge dispatcher");
    receive.await.expect("benchmark bridge response")
}

async fn start_bridge() -> (SocketAddr, oneshot::Sender<()>) {
    let (sender, mut receiver) = mpsc::channel::<BridgeCall>(BENCHMARK_MAX_CONCURRENT_REQUESTS);
    tokio::task::spawn_local(async move {
        while let Some(call) = receiver.recv().await {
            if call.delay {
                tokio::time::sleep(SATURATION_HANDLER_DELAY).await;
            }
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

async fn start_lenso(
    ingress: &WebIngressFactory,
    config: WebIngressConfig,
    lane: &str,
) -> Result<NativeApp, RuntimeFailure> {
    let endpoint = FixtureEndpointFactory::new(lane);
    let registry = lenso_native_adapter::NativePluginRegistry::new()
        .with_factory(endpoint)
        .with_factory(ingress.clone());
    let plan = project(config)
        .resolve(&workspace_root(), &ResolutionOptions::default())
        .map_err(|error| RuntimeFailure::InvalidResolvedPlan {
            detail: error.to_string(),
        })?;
    Kernel::start_native(plan.plan().clone(), TokioDriver::new(), registry).await
}

fn project(config: WebIngressConfig) -> ProjectFile {
    let mut project = ProjectFile::default();
    project.contracts_mut().push(
        ContractInput::descriptor_only(
            CAPABILITY_ID,
            DESCRIPTOR_VERSION,
            "crates/lenso-capability-http-endpoint/capability.json",
        )
        .with_rust_projection("crates/lenso-capability-http-endpoint/src/generated.rs"),
    );
    for package in [FIXTURE_PACKAGE_ID, PACKAGE_ID] {
        project.packages_mut().insert(
            package.to_owned(),
            PackageInput::new(package, PackageSource::Cargo, PACKAGE_VERSION)
                .with_package_name("lenso-web-ingress-plugin")
                .with_manifest("crates/lenso-web-ingress-plugin/Cargo.toml")
                .with_lockfile("Cargo.lock"),
        );
    }
    project.composition_mut().add_module(
        Plugin::new("benchmark-endpoint", FIXTURE_PACKAGE_ID).with_capability(
            CapabilityEndpoint::request(
                CAPABILITY_ID,
                DESCRIPTOR_VERSION,
                [DESCRIBE_OPERATION, HANDLE_OPERATION],
            ),
        ),
    );
    project.composition_mut().add_module(
        Plugin::new("web-ingress", PACKAGE_ID)
            .with_configuration_schema("crates/lenso-web-ingress-plugin/config.schema.json")
            .with_configuration(serde_json::to_value(config).unwrap())
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
    lane: Rc<str>,
    routes: Rc<Vec<DescribeResponseRoutesItem>>,
}

impl FixtureEndpointFactory {
    fn new(lane: &str) -> Self {
        let mut routes = REQUEST_BODY_SIZES
            .into_iter()
            .map(|size| DescribeResponseRoutesItem {
                method: "POST".to_owned(),
                openapi: None,
                path: format!("/bench/{size}"),
                route_id: size.to_string(),
            })
            .collect::<Vec<_>>();
        routes.push(DescribeResponseRoutesItem {
            method: "POST".to_owned(),
            openapi: None,
            path: SATURATION_PATH.to_owned(),
            route_id: "saturation".to_owned(),
        });
        Self {
            lane: Rc::from(lane),
            routes: Rc::new(routes),
        }
    }
}

impl NativePluginFactory for FixtureEndpointFactory {
    fn package_id(&self) -> &'static str {
        FIXTURE_PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }

    fn instantiate(
        &self,
        _context: NativePluginFactoryContext<'_>,
    ) -> Result<NativePluginInstance, RuntimeFailure> {
        Ok(NativePluginInstance::new(vec![Rc::new(
            EndpointEndpoint::new(FixtureEndpoint {
                lane: self.lane.clone(),
                routes: self.routes.clone(),
            }),
        )]))
    }
}

#[derive(Debug)]
struct FixtureEndpoint {
    lane: Rc<str>,
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
        let saturation = request.route_id == "saturation";
        let expected = if saturation {
            0
        } else {
            request
                .route_id
                .parse::<usize>()
                .expect("benchmark route is a body size")
        };
        assert_eq!(request.body.len(), expected);
        assert!(request.body.iter().all(|byte| *byte == b'x'));
        let lane = self.lane.to_string();
        Box::pin(async move {
            if saturation {
                tokio::time::sleep(SATURATION_HANDLER_DELAY).await;
            }
            Ok(Ok(HandleResponse {
                body: request.body,
                headers: vec![HandleResponseHeadersItem {
                    name: LANE_HEADER.as_str().to_owned(),
                    value: lane,
                }],
                status: 200,
            }))
        })
    }
}
