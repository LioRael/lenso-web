//! Linked native Host that starts Web Ingress from Plugin Descriptors already
//! present in the binary.
//!
//! Inventory fills the Host Catalog. [`NativeWebHost::plugin`] and
//! [`NativeWebHost::factory`] select running Instances. Ingress is a Host
//! default. Enabled HTTP, stream, and WebSocket Endpoint providers are bound to
//! it. Unique Capability requirements still derive through Plugin Root.
//! Extra Execution Adapters join the same App through [`NativeWebHost::with_adapter`].
//!
//! Call [`NativeWebHost::start`] or [`NativeWebHost::run`] from a Tokio
//! `current_thread` runtime. `start` must run on a [`tokio::task::LocalSet`].
//! [`NativeWebHost::run`] creates that set and waits for Ctrl-C.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;

use http::{Request, Response};
use lenso_app_plan::{
    ExecutionLaneId, ExecutionLanePlan, RequestAdmissionPlan,
    authoring::{
        HostBinding, HostCatalog, HostDefaultPlugin, HostPluginRelease, HostSlot, PluginDescriptor,
        PluginInstanceId, PluginRootInstance, PluginRootSnapshot, resolve_plugin_root,
    },
};
use lenso_capability_http_endpoint::{
    CAPABILITY_ID as HTTP_ENDPOINT, EndpointDescribe, EndpointHandle,
};
use lenso_capability_http_stream_endpoint::{
    CAPABILITY_ID as STREAM_ENDPOINT, StreamEndpointDescribe, StreamEndpointHandle,
};
use lenso_capability_websocket_endpoint::{
    CAPABILITY_ID as WEBSOCKET_ENDPOINT, EndpointConnectWebsocket, EndpointDescribeWebsocket,
};
use lenso_kernel::{
    CancellationToken, ExecutionAdapter, ExecutionAdapterCatalog, ExecutionAdapterCatalogError,
    Kernel, NativeApp, RuntimeFailure, ShutdownOutcome,
};
use lenso_native_adapter::{NativePluginDefinition, NativePluginFactory, NativePluginRegistry};
use lenso_runner::{
    CrossLaneTransferCatalog, ReplicatedNativeApp, ReplicatedRunnerError, TokioDriver,
};
use lenso_web_ingress_plugin::{
    PACKAGE_ID as INGRESS_PACKAGE_ID, WebIngressConfig, WebIngressEventFactory, WebIngressFactory,
    WebIngressListenerCoordinator, WebIngressMiddleware, WebIngressRequest, WebIngressResponse,
};
pub use lenso_web_ingress_plugin::{
    WebIngressDiagnostics, WebIngressEndpointFailure, WebIngressEventBody, WebIngressRouteManifest,
};
use serde::Serialize;
use serde_json::Value;
use tokio::task::LocalSet;
use tower::{Layer, Service};

const INSTANCE_KEY: &str = "default";

type ReplicatedIngressBuilder =
    Arc<dyn Fn(&ExecutionLaneId, WebIngressFactory) -> WebIngressFactory + Send + Sync>;
type ReplicatedLaneBuilder = Arc<
    dyn Fn(&ExecutionLaneId, NativePluginRegistry) -> Result<ExecutionAdapterCatalog, String>
        + Send
        + Sync,
>;
type ReplicatedTransferBuilder =
    Arc<dyn Fn(CrossLaneTransferCatalog) -> CrossLaneTransferCatalog + Send + Sync>;

/// Decision returned by a [`TowerIngressMiddleware`] policy service.
#[derive(Debug)]
pub enum TowerMiddlewareOutcome {
    /// Continue into Lenso route dispatch with the original normalized request.
    Continue,
    /// Stop dispatch and return this intentional HTTP response.
    Respond(WebIngressResponse),
}

/// Adapts one Tower policy service to Lenso's pre-dispatch middleware contract.
///
/// The service receives a clone of the normalized request after transport limits
/// and credential isolation. `Continue` leaves the original request untouched;
/// `Respond` short-circuits Endpoint dispatch. The adapter deliberately does not
/// wrap the full Ingress service, so streaming and WebSocket lifecycles remain
/// owned by Web Ingress.
pub struct TowerIngressMiddleware<S> {
    identity: String,
    service: S,
}

impl<S> TowerIngressMiddleware<S> {
    /// Creates a Tower policy adapter with a stable identity.
    #[must_use]
    pub fn new(identity: impl Into<String>, service: S) -> Self {
        Self {
            identity: identity.into(),
            service,
        }
    }
}

impl<S> fmt::Debug for TowerIngressMiddleware<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TowerIngressMiddleware")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl<S> WebIngressMiddleware for TowerIngressMiddleware<S>
where
    S: Service<WebIngressRequest, Response = TowerMiddlewareOutcome> + Clone + 'static,
    S::Future: 'static,
    S::Error: Into<RuntimeFailure>,
{
    fn identity(&self) -> &str {
        &self.identity
    }

    fn before_request<'a>(
        &'a self,
        request: &'a mut WebIngressRequest,
    ) -> futures::future::LocalBoxFuture<
        'a,
        Result<lenso_web_ingress_plugin::WebIngressMiddlewareOutcome, RuntimeFailure>,
    > {
        let mut service = self.service.clone();
        let observed = request.clone();
        Box::pin(async move {
            match service.call(observed).await.map_err(Into::into)? {
                TowerMiddlewareOutcome::Continue => {
                    Ok(lenso_web_ingress_plugin::WebIngressMiddlewareOutcome::Continue)
                }
                TowerMiddlewareOutcome::Respond(response) => {
                    Ok(lenso_web_ingress_plugin::WebIngressMiddlewareOutcome::Respond(response))
                }
            }
        })
    }
}

/// Linked native Web Host preset.
#[derive(Default)]
pub struct NativeWebHost {
    bind_address: Option<SocketAddr>,
    ingress_config: WebIngressConfig,
    root: PluginRootSnapshot,
    extra_defaults: Vec<HostDefaultPlugin>,
    extra_bindings: Vec<HostBinding>,
    extra_releases: Vec<HostPluginRelease>,
    factory_installers: Vec<FactoryInstaller>,
    middleware: Vec<Rc<dyn WebIngressMiddleware>>,
    diagnostics: Option<Rc<dyn WebIngressDiagnostics>>,
    extra_adapters: Vec<Rc<dyn ExecutionAdapter>>,
    replicated_ingress: Option<ReplicatedIngressBuilder>,
    replicated_lane: Option<ReplicatedLaneBuilder>,
    replicated_transfers: Option<ReplicatedTransferBuilder>,
    replicated_ready_timeout: Option<Duration>,
}

struct FactoryInstaller {
    package_id: &'static str,
    descriptor: PluginDescriptor,
    install: Box<dyn FnOnce(NativePluginRegistry) -> NativePluginRegistry>,
}

impl fmt::Debug for NativeWebHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeWebHost")
            .field("bind_address", &self.bind_address)
            .field("ingress_config", &self.ingress_config)
            .field("root", &self.root)
            .field("extra_defaults", &self.extra_defaults)
            .field("extra_bindings", &self.extra_bindings)
            .field("extra_releases", &self.extra_releases)
            .field(
                "factories",
                &self
                    .factory_installers
                    .iter()
                    .map(|installer| installer.package_id)
                    .collect::<Vec<_>>(),
            )
            .field("middleware", &self.middleware.len())
            .field("diagnostics", &self.diagnostics.is_some())
            .field("extra_adapters", &self.extra_adapters.len())
            .field("replicated_ingress", &self.replicated_ingress.is_some())
            .field("replicated_lane", &self.replicated_lane.is_some())
            .field("replicated_transfers", &self.replicated_transfers.is_some())
            .field("replicated_ready_timeout", &self.replicated_ready_timeout)
            .finish()
    }
}

impl NativeWebHost {
    /// Creates a Host that binds loopback port 8080.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the Ingress listener address. Port `0` asks the OS for an ephemeral port.
    #[must_use]
    pub fn bind(mut self, address: SocketAddr) -> Self {
        self.bind_address = Some(address);
        self
    }

    /// Replaces the Host's Web Ingress policy.
    ///
    /// `.bind(...)` remains available as a final listener-address override.
    /// Other limits, deadlines, cookies, and WebSocket policy come from this
    /// immutable configuration and are applied to both native and event Hosts.
    #[must_use]
    pub fn with_ingress_config(mut self, config: WebIngressConfig) -> Self {
        self.ingress_config = config;
        self
    }

    /// Links `P` into the binary and enables one Root Instance (`instance_key = "default"`).
    ///
    /// The Plugin reads merged configuration at construction. This call does not
    /// pass a live object or bypass Descriptor defaults.
    #[must_use]
    pub fn plugin<P: NativePluginDefinition>(self) -> Self {
        P::link();
        self.push_instance(P::PACKAGE_ID, INSTANCE_KEY, empty_configuration(), None)
    }

    /// Links `P` and places its default Instance on an explicit Execution Lane.
    ///
    /// This records placement in the Plugin Root and resolved Plan. It does not
    /// turn [`NativeWebHost`] into a replicated runner; [`Self::start`] remains
    /// the local single-Kernel preset.
    #[must_use]
    pub fn plugin_on_lane<P: NativePluginDefinition>(
        self,
        execution_lane: impl Into<String>,
    ) -> Self {
        P::link();
        self.push_instance(
            P::PACKAGE_ID,
            INSTANCE_KEY,
            empty_configuration(),
            Some(execution_lane.into()),
        )
    }

    /// Writes Plugin Root configuration for `P`'s default Instance.
    ///
    /// This is the in-code overlay on Descriptor defaults and Host defaults. The
    /// Plugin still reads `ConstructionContext::configuration()`; it does not
    /// receive this value as a constructor argument.
    pub fn plugin_with<P: NativePluginDefinition>(
        self,
        configuration: impl Serialize,
    ) -> Result<Self, WebHostError> {
        P::link();
        Ok(self.push_instance(
            P::PACKAGE_ID,
            INSTANCE_KEY,
            json_configuration(configuration)?,
            None,
        ))
    }

    /// Links `P`, places its default Instance on an explicit Execution Lane,
    /// and writes the typed Plugin Root configuration overlay.
    pub fn plugin_with_on_lane<P: NativePluginDefinition>(
        self,
        execution_lane: impl Into<String>,
        configuration: impl Serialize,
    ) -> Result<Self, WebHostError> {
        P::link();
        Ok(self.push_instance(
            P::PACKAGE_ID,
            INSTANCE_KEY,
            json_configuration(configuration)?,
            Some(execution_lane.into()),
        ))
    }

    /// Links `P` and enables a named Instance with typed configuration.
    pub fn instance<P: NativePluginDefinition>(
        self,
        instance_key: impl Into<String>,
        configuration: impl Serialize,
    ) -> Result<Self, WebHostError> {
        P::link();
        Ok(self.push_instance(
            P::PACKAGE_ID,
            instance_key,
            json_configuration(configuration)?,
            None,
        ))
    }

    /// Links `P` and enables a named Instance on an explicit Execution Lane.
    pub fn instance_on_lane<P: NativePluginDefinition>(
        self,
        instance_key: impl Into<String>,
        execution_lane: impl Into<String>,
        configuration: impl Serialize,
    ) -> Result<Self, WebHostError> {
        P::link();
        Ok(self.push_instance(
            P::PACKAGE_ID,
            instance_key,
            json_configuration(configuration)?,
            Some(execution_lane.into()),
        ))
    }

    /// Installs a hand-written native Factory, admits its Descriptor, and enables
    /// its default Instance.
    #[must_use]
    pub fn factory<F>(mut self, factory: F, descriptor: PluginDescriptor) -> Self
    where
        F: NativePluginFactory,
    {
        let package_id = factory.package_id();
        self.factory_installers.push(FactoryInstaller {
            package_id,
            descriptor,
            install: Box::new(move |registry| registry.with_factory(factory)),
        });
        self.push_instance(package_id, INSTANCE_KEY, empty_configuration(), None)
    }

    /// Admits one extra Plugin Descriptor without enabling an Instance.
    #[must_use]
    pub fn release(mut self, descriptor: PluginDescriptor) -> Self {
        self.extra_releases.push(HostPluginRelease::new(descriptor));
        self
    }

    /// Enables one Plugin Instance in the Plugin Root (`instance_key = "default"`).
    ///
    /// Prefer [`Self::plugin`] when the Plugin type implements
    /// [`NativePluginDefinition`].
    #[must_use]
    pub fn enable(self, plugin_id: impl Into<String>) -> Self {
        self.push_instance(plugin_id, INSTANCE_KEY, empty_configuration(), None)
    }

    /// Replaces the Plugin Root snapshot.
    #[must_use]
    pub fn root(mut self, root: PluginRootSnapshot) -> Self {
        self.root = root;
        self
    }

    /// Adds a Host default Instance.
    #[must_use]
    pub fn with_default(mut self, default: HostDefaultPlugin) -> Self {
        self.extra_defaults.push(default);
        self
    }

    /// Adds an explicit Host binding.
    #[must_use]
    pub fn with_binding(mut self, binding: HostBinding) -> Self {
        self.extra_bindings.push(binding);
        self
    }

    /// Adds a Tower policy service before Endpoint dispatch.
    ///
    /// Build the service with `tower::ServiceBuilder` or `tower::service_fn`.
    /// The service must return [`TowerMiddlewareOutcome::Continue`] or
    /// [`TowerMiddlewareOutcome::Respond`].
    #[must_use]
    pub fn with_tower_middleware<S>(self, identity: impl Into<String>, service: S) -> Self
    where
        S: Service<WebIngressRequest, Response = TowerMiddlewareOutcome> + Clone + 'static,
        S::Future: 'static,
        S::Error: Into<RuntimeFailure>,
    {
        self.with_middleware(TowerIngressMiddleware::new(identity, service))
    }

    /// Adds a Tower [`Layer`] around a policy service before Endpoint dispatch.
    #[must_use]
    pub fn with_tower_layer<L, S>(self, identity: impl Into<String>, layer: L, service: S) -> Self
    where
        L: Layer<S>,
        L::Service: Service<WebIngressRequest, Response = TowerMiddlewareOutcome> + Clone + 'static,
        <L::Service as Service<WebIngressRequest>>::Future: 'static,
        <L::Service as Service<WebIngressRequest>>::Error: Into<RuntimeFailure>,
    {
        self.with_tower_middleware(identity, layer.layer(service))
    }

    /// Adds one global Ingress middleware in declaration order.
    ///
    /// The same middleware list is applied to native listener and event-mode
    /// Hosts. Middleware runs after transport normalization and before Endpoint
    /// dispatch, with response hooks unwinding in reverse order.
    #[must_use]
    pub fn with_middleware(mut self, middleware: impl WebIngressMiddleware + 'static) -> Self {
        self.middleware.push(Rc::new(middleware));
        self
    }

    /// Installs one Host-owned observer for Endpoint failures hidden by HTTP.
    ///
    /// The observer receives the trusted request ID, route ID, provider index,
    /// and internal Runtime Failure before Ingress maps the failure to a safe
    /// response. The same observer is used by native and event Hosts.
    #[must_use]
    pub fn with_diagnostics(mut self, diagnostics: impl WebIngressDiagnostics + 'static) -> Self {
        self.diagnostics = Some(Rc::new(diagnostics));
        self
    }

    /// Adds one extra Execution Adapter (Bun, WASI, or process).
    ///
    /// Native linked Factories stay on the built-in native Adapter. Duplicate
    /// execution classes fail closed when [`Self::start`] builds the catalog.
    #[must_use]
    pub fn with_adapter(mut self, adapter: impl ExecutionAdapter) -> Self {
        self.extra_adapters.push(Rc::new(adapter));
        self
    }

    /// Customizes the Ingress factory independently for each replicated lane.
    ///
    /// The builder runs inside the lane thread and must construct only lane-local
    /// state. Capture immutable configuration in a `Send + Sync` value rather than
    /// sharing an `Rc`-backed middleware instance between lanes.
    #[must_use]
    pub fn with_replicated_ingress<F>(mut self, builder: F) -> Self
    where
        F: Fn(&ExecutionLaneId, WebIngressFactory) -> WebIngressFactory + Send + Sync + 'static,
    {
        self.replicated_ingress = Some(Arc::new(builder));
        self
    }

    /// Customizes the lane-local native registry and Execution Adapter catalog.
    ///
    /// The registry already contains the lane's replicated Web Ingress factory and
    /// every inventory-linked native factory. Return the complete catalog for the
    /// lane; native-only Hosts can omit this builder and use the built-in catalog.
    #[must_use]
    pub fn with_replicated_lane<F>(mut self, builder: F) -> Self
    where
        F: Fn(&ExecutionLaneId, NativePluginRegistry) -> Result<ExecutionAdapterCatalog, String>
            + Send
            + Sync
            + 'static,
    {
        self.replicated_lane = Some(Arc::new(builder));
        self
    }

    /// Extends the built-in cross-lane HTTP transfer catalog.
    ///
    /// Web Host registers buffered HTTP, streaming HTTP, and WebSocket Endpoint
    /// transfers by default. Use this callback to add application Capabilities;
    /// returning the supplied catalog unchanged is the safe default.
    #[must_use]
    pub fn with_replicated_transfers<F>(mut self, builder: F) -> Self
    where
        F: Fn(CrossLaneTransferCatalog) -> CrossLaneTransferCatalog + Send + Sync + 'static,
    {
        self.replicated_transfers = Some(Arc::new(builder));
        self
    }

    /// Sets the bounded Ready deadline for a replicated App Generation.
    #[must_use]
    pub fn with_replicated_ready_timeout(mut self, timeout: Duration) -> Self {
        self.replicated_ready_timeout = Some(timeout);
        self
    }

    /// Resolves the exact immutable Plan that this Host would start.
    ///
    /// This is the hand-off point for an advanced Runner integration. It
    /// includes linked Plugin Root Instances, Host defaults, automatic Ingress
    /// bindings, and any explicit Execution Lane placement. It does not start
    /// a Kernel or bind a listener.
    pub fn resolve_plan(&self) -> Result<lenso_app_plan::ResolvedAppPlan, WebHostError> {
        self.resolve_plan_with_replication(false)
    }

    fn resolve_replicated_plan(&self) -> Result<lenso_app_plan::ResolvedAppPlan, WebHostError> {
        self.resolve_plan_with_replication(true)
    }

    fn resolve_plan_with_replication(
        &self,
        replicated: bool,
    ) -> Result<lenso_app_plan::ResolvedAppPlan, WebHostError> {
        let config = match self.bind_address {
            Some(address) => self
                .ingress_config
                .clone()
                .with_bind_address(address)
                .map_err(WebHostError::Bind)?,
            None => self.ingress_config.clone(),
        };
        let extra_releases = self
            .factory_installers
            .iter()
            .map(|installer| HostPluginRelease::new(installer.descriptor.clone()))
            .chain(self.extra_releases.iter().cloned())
            .collect::<Vec<_>>();
        resolve_web_plan(
            &config,
            &self.root,
            &self.extra_defaults,
            &self.extra_bindings,
            &extra_releases,
            replicated,
        )
    }

    /// Starts Ingress and returns after the App is ready.
    ///
    /// Must be polled on a Tokio [`LocalSet`].
    pub async fn start(self) -> Result<RunningNativeWebHost, WebHostError> {
        let plan = self.resolve_plan()?;
        let mut ingress = self
            .middleware
            .into_iter()
            .fold(WebIngressFactory::new(), |ingress, middleware| {
                ingress.with_shared_middleware(middleware)
            });
        if let Some(diagnostics) = self.diagnostics.clone() {
            ingress = ingress.with_shared_diagnostics(diagnostics);
        }
        let mut registry = NativePluginRegistry::new().with_factory(ingress.clone());
        for installer in self.factory_installers {
            registry = (installer.install)(registry);
        }
        let registry = registry.with_linked_factories();
        let mut catalog = ExecutionAdapterCatalog::single(registry);
        for adapter in self.extra_adapters {
            catalog = catalog
                .with_shared_adapter(adapter)
                .map_err(WebHostError::Adapter)?;
        }
        let app = Kernel::start(plan, TokioDriver::new(), catalog)
            .await
            .map_err(WebHostError::Runtime)?;
        let address = ingress
            .local_address()
            .ok_or(WebHostError::ListenerNotPublished)?;
        Ok(RunningNativeWebHost {
            app,
            address,
            ingress,
        })
    }

    /// Starts one Kernel lane for every Execution Lane in the resolved Plan.
    ///
    /// The listener is bound once, each lane receives a deterministic Ingress
    /// replica slot, and inventory-linked native Plugins are discovered inside
    /// each lane-local registry. Ordinary [`Self::start`] remains the simpler
    /// single-Kernel path.
    pub async fn start_replicated(self) -> Result<RunningReplicatedWebHost, WebHostError> {
        if !self.factory_installers.is_empty() {
            return Err(WebHostError::ReplicationUnsupported(
                "hand-written factories from factory() are single-lane; install one per lane with with_replicated_lane",
            ));
        }
        if !self.middleware.is_empty() || self.diagnostics.is_some() {
            return Err(WebHostError::ReplicationUnsupported(
                "Ingress middleware and diagnostics require with_replicated_ingress",
            ));
        }
        if !self.extra_adapters.is_empty() {
            return Err(WebHostError::ReplicationUnsupported(
                "Execution Adapters require with_replicated_lane",
            ));
        }

        let mut host = self;
        let initial_plan = host.resolve_replicated_plan()?;
        let config = match host.bind_address {
            Some(address) => host
                .ingress_config
                .clone()
                .with_bind_address(address)
                .map_err(WebHostError::Bind)?,
            None => host.ingress_config.clone(),
        };
        let replica_configuration =
            serde_json::to_value(&config).map_err(|error| WebHostError::Plan(error.to_string()))?;
        for lane in initial_plan.execution_lanes() {
            if lane.id().as_str() == "main" {
                continue;
            }
            host = host.push_instance(
                INGRESS_PACKAGE_ID,
                format!("replica-{}", lane.id()),
                replica_configuration.clone(),
                Some(lane.id().to_string()),
            );
        }
        let plan = host.resolve_replicated_plan()?;
        let lane_indexes = plan
            .execution_lanes()
            .iter()
            .enumerate()
            .map(|(index, lane)| (lane.id().clone(), index))
            .collect::<BTreeMap<_, _>>();
        let lane_count = lane_indexes.len();
        let coordinator = WebIngressListenerCoordinator::bind(config, lane_count)
            .await
            .map_err(WebHostError::Runtime)?;
        let running_coordinator = coordinator.clone();
        let ingress_builder = host
            .replicated_ingress
            .unwrap_or_else(|| Arc::new(|_, ingress| ingress));
        let lane_builder = host.replicated_lane.unwrap_or_else(|| {
            Arc::new(|_, registry| Ok(ExecutionAdapterCatalog::single(registry)))
        });
        let ready_timeout = host.replicated_ready_timeout;
        let transfers = host
            .replicated_transfers
            .map_or_else(default_replicated_transfers, |builder| {
                builder(default_replicated_transfers())
            });
        let lane_builder_fn =
            move |lane: &ExecutionLaneId| -> Result<ExecutionAdapterCatalog, String> {
                let replica_index = lane_indexes.get(lane).copied().ok_or_else(|| {
                    format!("replicated Web Host received an undeclared Execution Lane `{lane}`")
                })?;
                let ingress = WebIngressFactory::replicated_at(&coordinator, replica_index)
                    .map_err(|error| {
                        format!("could not allocate Web Ingress replica: {error:?}")
                    })?;
                let ingress = ingress_builder(lane, ingress);
                let registry = NativePluginRegistry::new()
                    .with_factory(ingress)
                    .with_linked_factories();
                lane_builder(lane, registry)
            };
        let app = match ready_timeout {
            Some(timeout) => ReplicatedNativeApp::start_with_fallible_transfer_catalog(
                plan,
                lane_builder_fn,
                transfers,
                Some(timeout),
            ),
            None => ReplicatedNativeApp::start_with_fallible_transfer_catalog(
                plan,
                lane_builder_fn,
                transfers,
                None,
            ),
        }
        .map_err(WebHostError::Replicated)?;
        Ok(RunningReplicatedWebHost {
            app,
            coordinator: running_coordinator,
        })
    }

    /// Starts Ingress in event mode and returns a socket-free request harness.
    ///
    /// Event mode executes the same Plan-bound routing, limits, credentials,
    /// middleware, and response mapping as the native listener without binding
    /// a TCP socket. It is intended for contract tests and embedded Hosts.
    pub async fn start_event(self) -> Result<RunningEventWebHost, WebHostError> {
        let plan = self.resolve_plan()?;
        let mut ingress = self
            .middleware
            .into_iter()
            .fold(WebIngressEventFactory::new(), |ingress, middleware| {
                ingress.with_shared_middleware(middleware)
            });
        if let Some(diagnostics) = self.diagnostics {
            ingress = ingress.with_shared_diagnostics(diagnostics);
        }
        let mut registry = NativePluginRegistry::new().with_factory(ingress.clone());
        for installer in self.factory_installers {
            registry = (installer.install)(registry);
        }
        let registry = registry.with_linked_factories();
        let mut catalog = ExecutionAdapterCatalog::single(registry);
        for adapter in self.extra_adapters {
            catalog = catalog
                .with_shared_adapter(adapter)
                .map_err(WebHostError::Adapter)?;
        }
        let app = Kernel::start(plan, TokioDriver::new(), catalog)
            .await
            .map_err(WebHostError::Runtime)?;
        Ok(RunningEventWebHost { app, ingress })
    }

    /// Starts the App on a private [`LocalSet`] and shuts down on Ctrl-C.
    pub async fn run(self) -> Result<(), WebHostError> {
        LocalSet::new()
            .run_until(async move {
                let running = self.start().await?;
                tokio::signal::ctrl_c()
                    .await
                    .map_err(|error| WebHostError::Bind(error.to_string()))?;
                running.shutdown().await
            })
            .await
    }

    /// Starts the replicated App on a private [`LocalSet`] and shuts down on Ctrl-C.
    pub async fn run_replicated(self) -> Result<(), WebHostError> {
        LocalSet::new()
            .run_until(async move {
                let running = self.start_replicated().await?;
                tokio::signal::ctrl_c()
                    .await
                    .map_err(|error| WebHostError::Bind(error.to_string()))?;
                running.shutdown().await
            })
            .await
    }

    fn push_instance(
        mut self,
        plugin_id: impl Into<String>,
        instance_key: impl Into<String>,
        configuration: Value,
        execution_lane: Option<String>,
    ) -> Self {
        let plugin_id = plugin_id.into();
        let instance_key = instance_key.into();
        let dependency_selection_adopted = self.root.dependency_selection_adopted();
        let dependency_choices = self.root.dependency_choices().to_vec();
        let mut replacement = PluginRootInstance::new(plugin_id.clone(), instance_key.clone())
            .with_configuration(configuration);
        let mut instances = self.root.instances().to_vec();
        if let Some(existing) = instances.iter_mut().find(|instance| {
            instance.id().plugin_id() == plugin_id && instance.id().instance_key() == instance_key
        }) {
            if let Some(lane) = execution_lane.as_deref() {
                replacement = replacement.with_execution_lane(lane);
            } else if let Some(lane) = existing.execution_lane() {
                replacement = replacement.with_execution_lane(lane);
            }
            *existing = replacement;
        } else {
            if let Some(lane) = execution_lane {
                replacement = replacement.with_execution_lane(lane);
            }
            instances.push(replacement);
        }
        let mut root = PluginRootSnapshot::new(
            self.root.releases().to_vec(),
            instances,
            self.root.disabled().to_vec(),
        );
        if dependency_selection_adopted {
            root = root.with_dependency_choices(dependency_choices);
        }
        self.root = root;
        self
    }
}

/// A replicated Web App sharing one listener across its Plan-declared lanes.
#[derive(Debug)]
pub struct RunningReplicatedWebHost {
    app: ReplicatedNativeApp,
    coordinator: WebIngressListenerCoordinator,
}

impl RunningReplicatedWebHost {
    /// Returns the one listener address shared by every lane.
    #[must_use]
    pub fn address(&self) -> SocketAddr {
        self.coordinator.local_address()
    }

    /// Returns the number of Kernel lanes started from the resolved Plan.
    #[must_use]
    pub fn lane_count(&self) -> usize {
        self.app.lane_count()
    }

    /// Returns structural lane placement diagnostics from the Runner.
    #[must_use]
    pub fn diagnostics_snapshot(&self) -> lenso_runner::LaneDiagnosticsSnapshot {
        self.app.diagnostics_snapshot()
    }

    /// Returns whether any lane has reached a terminal failure.
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.app.is_failed()
    }

    /// Returns the first terminal Runner failure, when one has occurred.
    #[must_use]
    pub fn terminal_failure(&self) -> Option<ReplicatedRunnerError> {
        self.app.terminal_failure()
    }

    /// Waits for the replicated App Generation to become terminal.
    pub async fn wait_for_terminal(&self) -> ReplicatedRunnerError {
        self.app.wait_for_terminal().await
    }

    /// Stops every lane and the shared listener with one bounded timeout.
    pub async fn shutdown(self) -> Result<(), WebHostError> {
        self.app
            .shutdown(Duration::from_secs(3))
            .await
            .map_err(WebHostError::Replicated)
    }
}

/// A started Web App and a socket-free event Ingress harness.
#[derive(Debug)]
pub struct RunningEventWebHost {
    app: NativeApp,
    ingress: WebIngressEventFactory,
}

impl RunningEventWebHost {
    /// Handles one origin-form buffered HTTP request without opening a socket.
    pub async fn handle(&self, request: Request<Bytes>) -> Result<Response<Bytes>, RuntimeFailure> {
        self.ingress.handle(request, CancellationToken::new()).await
    }

    /// Handles one request while preserving streaming and WebSocket response bodies.
    ///
    /// Use this entrypoint when the selected Endpoint providers expose stream or
    /// WebSocket Capabilities; [`Self::handle`] intentionally rejects those bodies
    /// because it is the buffered convenience API.
    pub async fn handle_response(
        &self,
        request: Request<Bytes>,
    ) -> Result<Response<WebIngressEventBody>, RuntimeFailure> {
        self.ingress
            .handle_response(request, CancellationToken::new())
            .await
    }

    /// Returns the canonical route manifest published by the active Ingress.
    ///
    /// The manifest is available only after [`NativeWebHost::start_event`]
    /// succeeds and is the same immutable route view used for dispatch.
    #[must_use]
    pub fn route_manifest(&self) -> Option<WebIngressRouteManifest> {
        self.ingress.route_manifest()
    }

    /// Shuts the event App down and reports a non-clean outcome as an error.
    pub async fn shutdown(self) -> Result<(), WebHostError> {
        match self.app.shutdown(Duration::from_secs(3)).await {
            ShutdownOutcome::Clean => Ok(()),
            outcome => Err(WebHostError::Shutdown(outcome)),
        }
    }
}

/// A started Web App and the address Ingress actually bound.
#[derive(Debug)]
pub struct RunningNativeWebHost {
    app: NativeApp,
    address: SocketAddr,
    ingress: WebIngressFactory,
}

impl RunningNativeWebHost {
    /// Returns the bound listener address.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Returns the canonical route manifest published by the active Ingress.
    ///
    /// The manifest is available only after [`NativeWebHost::start`] succeeds
    /// and is the same immutable route view used for dispatch.
    #[must_use]
    pub fn route_manifest(&self) -> Option<WebIngressRouteManifest> {
        self.ingress.route_manifest()
    }

    /// Shuts the App down and reports a non-clean outcome as an error.
    pub async fn shutdown(self) -> Result<(), WebHostError> {
        match self.app.shutdown(Duration::from_secs(3)).await {
            ShutdownOutcome::Clean => Ok(()),
            outcome => Err(WebHostError::Shutdown(outcome)),
        }
    }
}

/// Failures that prevent a linked Web Host from becoming ready or stopping cleanly.
#[derive(Debug)]
pub enum WebHostError {
    /// Listener address or OS signal handling failed.
    Bind(String),
    /// Linked Plugin Descriptors could not form one App.
    Plan(String),
    /// No enabled Plugin provides `lenso.http.endpoint@1`.
    MissingEndpoint,
    /// Ingress started without publishing a bound address.
    ListenerNotPublished,
    /// Kernel startup failed.
    Runtime(RuntimeFailure),
    /// Duplicate Execution Adapter class.
    Adapter(ExecutionAdapterCatalogError),
    /// Shutdown did not finish cleanly.
    Shutdown(ShutdownOutcome),
    /// A replicated Host cannot safely replay a single-lane runtime value.
    ReplicationUnsupported(&'static str),
    /// Replicated Runner startup or shutdown failed.
    Replicated(ReplicatedRunnerError),
}

impl fmt::Display for WebHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(detail) => write!(formatter, "Web Host bind failed: {detail}"),
            Self::Plan(detail) => write!(formatter, "Web Host plan failed: {detail}"),
            Self::MissingEndpoint => {
                write!(
                    formatter,
                    "Web Host found no enabled lenso.http.endpoint@1 provider"
                )
            }
            Self::ListenerNotPublished => {
                write!(formatter, "Web Ingress did not publish its bound address")
            }
            Self::Runtime(error) => write!(formatter, "Web Host runtime failed: {error:?}"),
            Self::Adapter(error) => write!(formatter, "Web Host adapter catalog failed: {error}"),
            Self::Shutdown(outcome) => {
                write!(formatter, "Web Host did not shut down cleanly: {outcome:?}")
            }
            Self::ReplicationUnsupported(detail) => {
                write!(
                    formatter,
                    "replicated Web Host configuration is unsupported: {detail}"
                )
            }
            Self::Replicated(error) => write!(formatter, "replicated Web Host failed: {error:?}"),
        }
    }
}

impl Error for WebHostError {}

fn default_replicated_transfers() -> CrossLaneTransferCatalog {
    CrossLaneTransferCatalog::new()
        .with_request::<EndpointDescribe>(&["describe"])
        .with_request::<EndpointHandle>(&["handle"])
        .with_request::<StreamEndpointDescribe>(&["describe_stream"])
        .with_stream::<StreamEndpointHandle>(&["handle_stream"])
        .with_request::<EndpointDescribeWebsocket>(&["describe_websocket"])
        .with_stream::<EndpointConnectWebsocket>(&["connect_websocket"])
}

fn empty_configuration() -> Value {
    Value::Object(serde_json::Map::new())
}

fn json_configuration(configuration: impl Serialize) -> Result<Value, WebHostError> {
    serde_json::to_value(configuration).map_err(|error| WebHostError::Plan(error.to_string()))
}

fn provides_capability(descriptor: &PluginDescriptor, capability_id: &str) -> bool {
    descriptor
        .provided_capabilities()
        .iter()
        .any(|capability| capability.capability_id() == capability_id)
}

fn endpoint_ids_for(
    enabled_ids: &[PluginInstanceId],
    releases: &[HostPluginRelease],
    capability_id: &str,
) -> Vec<PluginInstanceId> {
    enabled_ids
        .iter()
        .filter(|id| {
            releases.iter().any(|release| {
                release.descriptor().plugin_id() == id.plugin_id()
                    && provides_capability(release.descriptor(), capability_id)
            })
        })
        .cloned()
        .collect()
}

fn resolve_web_plan(
    config: &WebIngressConfig,
    root: &PluginRootSnapshot,
    extra_defaults: &[HostDefaultPlugin],
    extra_bindings: &[HostBinding],
    extra_releases: &[HostPluginRelease],
    replicated: bool,
) -> Result<lenso_app_plan::ResolvedAppPlan, WebHostError> {
    let discovered = NativePluginRegistry::host_catalog([], []).map_err(WebHostError::Runtime)?;
    let ingress = WebIngressFactory::plugin_descriptor();
    let mut releases = vec![HostPluginRelease::new(ingress.clone())];
    for release in discovered.plugins().iter().chain(extra_releases) {
        if release.descriptor().plugin_id() == INGRESS_PACKAGE_ID {
            continue;
        }
        if releases
            .iter()
            .any(|existing| existing.descriptor().plugin_id() == release.descriptor().plugin_id())
        {
            continue;
        }
        releases.push(release.clone());
    }

    let mut slot_counts = BTreeMap::<String, usize>::new();
    for release in &releases {
        *slot_counts
            .entry(release.descriptor().root_slot().to_owned())
            .or_insert(0) += 1;
    }
    let ingress_slot = ingress.root_slot().to_owned();
    let slots = slot_counts.into_iter().map(|(id, count)| {
        if replicated && id == ingress_slot {
            HostSlot::many(id)
        } else if count == 1 {
            HostSlot::one(id)
        } else {
            HostSlot::many(id)
        }
    });

    let mut defaults = vec![
        HostDefaultPlugin::new(INGRESS_PACKAGE_ID, INSTANCE_KEY).with_configuration(
            serde_json::to_value(config).map_err(|error| WebHostError::Plan(error.to_string()))?,
        ),
    ];
    defaults.extend(extra_defaults.iter().cloned());

    let mut enabled_ids = defaults
        .iter()
        .map(|default| default.id().clone())
        .collect::<Vec<_>>();
    enabled_ids.extend(
        root.instances()
            .iter()
            .map(|instance| instance.id().clone()),
    );
    let http_ids = endpoint_ids_for(&enabled_ids, &releases, HTTP_ENDPOINT);
    if http_ids.is_empty() {
        return Err(WebHostError::MissingEndpoint);
    }

    let (queue_capacity, max_concurrency) = config.endpoint_admission_limits();
    let admission = RequestAdmissionPlan::new(queue_capacity, max_concurrency);
    let ingress_ids = enabled_ids
        .iter()
        .filter(|id| id.plugin_id() == INGRESS_PACKAGE_ID)
        .cloned()
        .collect::<Vec<_>>();
    let stream_ids = endpoint_ids_for(&enabled_ids, &releases, STREAM_ENDPOINT);
    let websocket_ids = endpoint_ids_for(&enabled_ids, &releases, WEBSOCKET_ENDPOINT);
    let mut bindings = Vec::new();
    for ingress_id in ingress_ids {
        bindings.push(
            HostBinding::to_instances(ingress_id.clone(), HTTP_ENDPOINT, http_ids.clone())
                .with_admission(admission),
        );
        if !stream_ids.is_empty() {
            bindings.push(
                HostBinding::to_instances(ingress_id.clone(), STREAM_ENDPOINT, stream_ids.clone())
                    .with_admission(admission),
            );
        }
        if !websocket_ids.is_empty() {
            bindings.push(
                HostBinding::to_instances(ingress_id, WEBSOCKET_ENDPOINT, websocket_ids.clone())
                    .with_admission(admission),
            );
        }
    }
    bindings.extend(extra_bindings.iter().cloned());

    let mut execution_lanes = BTreeSet::from(["main".to_owned()]);
    for instance in root.instances() {
        if let Some(execution_lane) = instance.execution_lane() {
            execution_lanes.insert(execution_lane.to_owned());
        }
    }
    let execution_lanes = execution_lanes
        .into_iter()
        .map(ExecutionLanePlan::new)
        .collect();
    let host = HostCatalog::new(slots, releases, defaults)
        .with_execution_lanes(execution_lanes)
        .with_bindings(bindings);
    resolve_plugin_root(&host, root)
        .map(|resolved| resolved.plan().clone())
        .map_err(|error| WebHostError::Plan(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, io::ErrorKind, rc::Rc};

    use futures::future::LocalBoxFuture;
    use http::{HeaderValue, Request, Response};
    use lenso_web_greetings_plugin_example::GreetingsHttp;
    use lenso_web_ingress_plugin::{
        WebIngressMiddlewareOutcome, WebIngressRequest, WebIngressResponse,
    };
    use lenso_web_query_endpoint_fixture::OrderSearchHttp;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        task::LocalSet,
    };

    use super::*;

    async fn post_greeting(address: SocketAddr) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                b"POST /greetings HTTP/1.1\r\n\
Host: localhost\r\n\
content-type: application/json\r\n\
content-length: 16\r\n\
connection: close\r\n\
\r\n\
{\"name\":\"Lenso\"}",
            )
            .await
            .unwrap();
        let mut body = Vec::new();
        match stream.read_to_end(&mut body).await {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::ConnectionReset => {}
            Err(error) => panic!("{error}"),
        }
        String::from_utf8_lossy(&body).into_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn serves_a_linked_endpoint_without_a_handwritten_plan() {
        LocalSet::new()
            .run_until(async {
                let running = NativeWebHost::new()
                    .with_middleware(TestHeaderMiddleware)
                    .plugin::<GreetingsHttp>()
                    .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .start()
                    .await
                    .unwrap();
                let response = post_greeting(running.address()).await;
                assert!(
                    response.starts_with("HTTP/1.1 201"),
                    "unexpected response: {response:?}"
                );
                assert!(response.contains("x-lenso-middleware: active"));
                assert!(
                    running
                        .route_manifest()
                        .is_some_and(|manifest| !manifest.routes().is_empty())
                );
                running.shutdown().await.unwrap();
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replicated_host_serves_a_linked_endpoint_on_one_lane() {
        let running = NativeWebHost::new()
            .plugin::<GreetingsHttp>()
            .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .start_replicated()
            .await
            .unwrap();
        assert_eq!(running.lane_count(), 1);
        let response = post_greeting(running.address()).await;
        assert!(
            response.starts_with("HTTP/1.1 201"),
            "unexpected response: {response:?}"
        );
        assert!(!running.is_failed());
        running.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replicated_host_starts_declared_execution_lanes() {
        let running = NativeWebHost::new()
            .plugin_on_lane::<GreetingsHttp>("web")
            .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .with_replicated_ready_timeout(Duration::from_secs(3))
            .start_replicated()
            .await
            .unwrap();
        assert_eq!(running.lane_count(), 2);
        let response =
            tokio::time::timeout(Duration::from_secs(3), post_greeting(running.address()))
                .await
                .expect("cross-lane HTTP request should complete");
        assert!(
            response.starts_with("HTTP/1.1 201"),
            "unexpected response: {response:?}"
        );
        assert!(!running.is_failed());
        running.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_host_composes_multiple_native_endpoint_plugins() {
        Box::pin(LocalSet::new().run_until(async {
            let running = NativeWebHost::new()
                .plugin::<GreetingsHttp>()
                .plugin::<OrderSearchHttp>()
                .start_event()
                .await
                .unwrap();
            let response = running
                .handle(
                    Request::builder()
                        .method("QUERY")
                        .uri("/orders/search")
                        .header("content-type", "application/json")
                        .body(Bytes::from_static(br#"{"term":"open orders"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(response.body().as_ref(), br#"{"term":"open orders"}"#);
            running.shutdown().await.unwrap();
        }))
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingress_configuration_owns_the_bind_address_without_an_override() {
        LocalSet::new()
            .run_until(async {
                let config = WebIngressConfig::default()
                    .with_bind_address(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .unwrap();
                let running = NativeWebHost::new()
                    .with_ingress_config(config)
                    .plugin::<GreetingsHttp>()
                    .start()
                    .await
                    .unwrap();
                assert_ne!(running.address().port(), 8080);
                running.shutdown().await.unwrap();
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plugin_with_empty_configuration_still_serves() {
        LocalSet::new()
            .run_until(async {
                let running = NativeWebHost::new()
                    .plugin_with::<GreetingsHttp>(serde_json::json!({}))
                    .unwrap()
                    .bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .start()
                    .await
                    .unwrap();
                let response = post_greeting(running.address()).await;
                assert!(
                    response.starts_with("HTTP/1.1 201"),
                    "unexpected response: {response:?}"
                );
                running.shutdown().await.unwrap();
            })
            .await;
    }

    #[derive(Debug)]
    struct TestDiagnostics(Rc<Cell<usize>>);

    impl WebIngressDiagnostics for TestDiagnostics {
        fn endpoint_runtime_failure(&self, _event: WebIngressEndpointFailure<'_>) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn host_shares_diagnostics_and_exposes_event_manifest() {
        LocalSet::new()
            .run_until(async {
                let observed = Rc::new(Cell::new(0));
                let running = NativeWebHost::new()
                    .with_diagnostics(TestDiagnostics(observed.clone()))
                    .plugin::<GreetingsHttp>()
                    .start_event()
                    .await
                    .unwrap();
                assert!(
                    running
                        .route_manifest()
                        .is_some_and(|manifest| !manifest.routes().is_empty())
                );
                assert_eq!(observed.get(), 0);
                running.shutdown().await.unwrap();
            })
            .await;
    }

    #[derive(Debug)]
    struct TestHeaderMiddleware;

    impl WebIngressMiddleware for TestHeaderMiddleware {
        fn identity(&self) -> &'static str {
            "test.header"
        }

        fn before_request<'a>(
            &'a self,
            _request: &'a mut WebIngressRequest,
        ) -> LocalBoxFuture<'a, Result<WebIngressMiddlewareOutcome, RuntimeFailure>> {
            Box::pin(std::future::ready(Ok(
                WebIngressMiddlewareOutcome::Continue,
            )))
        }

        fn after_response<'a>(
            &'a self,
            _request: &'a WebIngressRequest,
            response: &'a mut WebIngressResponse,
        ) -> LocalBoxFuture<'a, Result<(), RuntimeFailure>> {
            Box::pin(async move {
                response
                    .headers_mut()
                    .insert("x-lenso-middleware", HeaderValue::from_static("active"));
                Ok(())
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_mode_uses_host_ingress_configuration() {
        Box::pin(LocalSet::new().run_until(async {
            let config = WebIngressConfig::default()
                .with_request_limits(1, 1024)
                .unwrap();
            let running = NativeWebHost::new()
                .with_ingress_config(config)
                .plugin::<GreetingsHttp>()
                .start_event()
                .await
                .unwrap();
            let response = running
                .handle(
                    Request::post("/greetings")
                        .header("content-type", "application/json")
                        .body(Bytes::from_static(br#"{"name":"Lenso"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), 413);
            running.shutdown().await.unwrap();
        }))
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_host_accepts_a_tower_layer_policy() {
        let service = tower::service_fn(|request: WebIngressRequest| async move {
            let blocked = request.uri().path() == "/tower-blocked";
            if blocked {
                Ok::<_, RuntimeFailure>(TowerMiddlewareOutcome::Respond(
                    Response::builder()
                        .status(403)
                        .body(Bytes::from_static(b"blocked by tower"))
                        .unwrap(),
                ))
            } else {
                Ok(TowerMiddlewareOutcome::Continue)
            }
        });
        let layer = tower::layer::util::Identity::new();

        Box::pin(LocalSet::new().run_until(async {
            let running = NativeWebHost::new()
                .with_tower_layer("test.tower", layer, service)
                .plugin::<GreetingsHttp>()
                .start_event()
                .await
                .unwrap();
            let response = running
                .handle(Request::get("/tower-blocked").body(Bytes::new()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), 403);
            assert_eq!(response.body().as_ref(), b"blocked by tower");
            running.shutdown().await.unwrap();
        }))
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_mode_applies_host_middleware_before_shutdown() {
        Box::pin(LocalSet::new().run_until(async {
            let running = NativeWebHost::new()
                .with_middleware(TestHeaderMiddleware)
                .plugin::<GreetingsHttp>()
                .start_event()
                .await
                .unwrap();
            let response = running
                .handle(Request::get("/missing").body(Bytes::new()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.headers()["x-lenso-middleware"], "active");
            running.shutdown().await.unwrap();
        }))
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_mode_covers_route_errors_without_a_socket() {
        Box::pin(LocalSet::new().run_until(async {
            let running = NativeWebHost::new()
                .plugin::<GreetingsHttp>()
                .start_event()
                .await
                .unwrap();
            let missing = running
                .handle(Request::get("/missing").body(Bytes::new()).unwrap())
                .await
                .unwrap();
            assert_eq!(missing.status(), 404);
            assert_eq!(
                missing.headers()["content-type"],
                "application/json; charset=utf-8"
            );

            let preserved = running
                .handle_response(Request::get("/missing").body(Bytes::new()).unwrap())
                .await
                .unwrap();
            assert!(matches!(preserved.body(), WebIngressEventBody::Buffered(_)));

            let wrong_method = running
                .handle(
                    Request::post("/greetings/search")
                        .body(Bytes::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(wrong_method.status(), 405);
            assert_eq!(wrong_method.headers()["allow"], "GET");
            running.shutdown().await.unwrap();
        }))
        .await;
    }

    #[test]
    fn enabling_a_plugin_preserves_adopted_dependency_selection() {
        let root = PluginRootSnapshot::new([], [], []).with_dependency_choices(Vec::new());
        let host = NativeWebHost::new().root(root).plugin::<GreetingsHttp>();

        assert!(host.root.dependency_selection_adopted());
        assert!(host.root.dependency_choices().is_empty());
    }

    #[test]
    fn lane_authoring_is_preserved_in_the_resolved_web_plan() {
        let host = NativeWebHost::new().plugin_on_lane::<GreetingsHttp>("web");
        let plan = host.resolve_plan().unwrap();

        assert_eq!(
            plan.plugin_instances()
                .iter()
                .find(|instance| instance.package_id() == GreetingsHttp::PACKAGE_ID)
                .unwrap()
                .execution_lane()
                .as_str(),
            "web"
        );
        assert!(
            plan.execution_lanes()
                .iter()
                .any(|lane| lane.id().as_str() == "web")
        );
    }

    #[test]
    fn inventory_does_not_enable_linked_endpoints() {
        GreetingsHttp::link();
        let error = resolve_web_plan(
            &WebIngressConfig::default(),
            &PluginRootSnapshot::default(),
            &[],
            &[],
            &[],
            false,
        )
        .unwrap_err();
        assert!(matches!(error, WebHostError::MissingEndpoint));
    }

    #[test]
    fn empty_explicit_root_without_endpoints_fails_closed() {
        let error = resolve_web_plan(
            &WebIngressConfig::default(),
            &PluginRootSnapshot::new(
                [],
                [PluginRootInstance::new(
                    "lenso.example.unrelated",
                    "default",
                )],
                [],
            ),
            &[],
            &[],
            &[],
            false,
        )
        .unwrap_err();
        assert!(matches!(error, WebHostError::MissingEndpoint));
    }
}
