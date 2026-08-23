use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    rc::Rc,
    time::{Duration, Instant},
};

use axum::{Router, body::Bytes, http::Uri, response::IntoResponse, routing::post};
use futures::{future::LocalBoxFuture, future::join_all};
use lenso_authoring::{
    Binding, CapabilityEndpoint, CapabilityRequirement, ContractInput, Module, PackageInput,
    PackageSource, ProjectAuthoring, ProjectFile, ResolutionOptions,
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID, DESCRIBE_OPERATION, DESCRIPTOR_VERSION, DescribeRequest, DescribeResponse,
    DescribeResponseRoutesItem, EndpointDescribeInvocationError, EndpointEndpoint,
    EndpointHandleInvocationError, EndpointProvider, HANDLE_OPERATION, HandleRequest,
    HandleResponse,
};
use lenso_kernel::{InvocationContext, Kernel, NativeApp, RuntimeFailure};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use lenso_runner::TokioDriver;
use lenso_web_ingress::{PACKAGE_ID, WebIngressFactory};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

const FIXTURE_PACKAGE_ID: &str = "benchmark.http-endpoint";
const REQUEST_BODY_SIZES: [usize; 3] = [0, 1024, 65_536];
const CONNECTION_COUNTS: [usize; 2] = [1, 8];
const SAMPLE_COUNT: usize = 3;
const WARMUP_REQUESTS_PER_CONNECTION: usize = 32;

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime")
        .block_on(LocalSet::new().run_until(run()));
}

async fn run() {
    let ingress = WebIngressFactory::default();
    let app = start_lenso(&ingress).await.expect("Lenso App should start");
    let lenso_address = ingress.local_address().expect("Ingress should be bound");
    let (axum_address, axum_shutdown) = start_axum().await;

    println!(
        "HTTP ingress benchmark: samples={SAMPLE_COUNT}, warmup_per_connection={WARMUP_REQUESTS_PER_CONNECTION}"
    );
    println!("server\trequest_body_bytes\tconnections\trequests\tmedian_req_s\tsamples_req_s");
    for request_body_size in REQUEST_BODY_SIZES {
        let requests = request_count(request_body_size);
        for connections in CONNECTION_COUNTS {
            report(
                "axum",
                axum_address,
                request_body_size,
                connections,
                requests,
            )
            .await;
            report(
                "lenso",
                lenso_address,
                request_body_size,
                connections,
                requests,
            )
            .await;
        }
    }

    let _ = axum_shutdown.send(());
    app.shutdown(Duration::from_secs(1)).await;
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
    server: &str,
    address: SocketAddr,
    request_body_size: usize,
    connections: usize,
    requests: usize,
) {
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    for _ in 0..SAMPLE_COUNT {
        samples.push(
            measure(address, request_body_size, connections, requests)
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

    let started = Instant::now();
    let base = requests / connections;
    let remainder = requests % connections;
    join_all(clients.iter_mut().enumerate().map(|(index, client)| {
        client.requests(request_body_size, base + usize::from(index < remainder))
    }))
    .await;
    started.elapsed()
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
            PackageInput::new(package, PackageSource::Cargo, "0.1.0")
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
    project
        .composition_mut()
        .add_module(Module::new("web-ingress", PACKAGE_ID).with_requirement(
            CapabilityRequirement::many(CAPABILITY_ID, DESCRIPTOR_VERSION),
        ));
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
        "0.1.0"
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
    ) -> LocalBoxFuture<'static, Result<DescribeResponse, EndpointDescribeInvocationError>> {
        Box::pin(futures::future::ready(Ok(DescribeResponse {
            routes: self.routes.as_ref().clone(),
        })))
    }

    fn handle(
        &self,
        _context: InvocationContext,
        request: HandleRequest,
    ) -> LocalBoxFuture<'static, Result<HandleResponse, EndpointHandleInvocationError>> {
        let expected = request
            .route_id
            .parse::<usize>()
            .expect("benchmark route is a body size");
        assert_eq!(request.body.len(), expected);
        assert!(request.body.iter().all(|byte| *byte == b'x'));
        Box::pin(futures::future::ready(Ok(HandleResponse {
            body: request.body,
            headers: Vec::new(),
            status: 200,
        })))
    }
}
