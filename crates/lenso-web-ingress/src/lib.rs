//! General-purpose linked Rust HTTP Ingress Module for Lenso backends.

mod config;
mod replication;
mod routing;
mod server;

use std::{
    cell::{Cell, RefCell},
    fmt,
    net::SocketAddr,
    rc::Rc,
};

use lenso_kernel::{
    ActivateContext, ModuleFuture, ModuleLifecycle, PrepareContext, RuntimeFailure,
};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use tokio::net::TcpListener;

pub use config::WebIngressConfig;
use replication::WebIngressReplica;
pub use replication::{
    WebIngressListenerCoordinator, WebIngressReplicaMismatch, WebIngressRoute,
    WebIngressRouteManifest,
};

pub const PACKAGE_ID: &str = "lenso.web-ingress";
pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Default)]
struct WebIngressObserver {
    local_address: Cell<Option<SocketAddr>>,
    route_manifest: RefCell<Option<WebIngressRouteManifest>>,
}

/// Native Module factory and observable endpoint handle for one Ingress.
#[derive(Clone, Debug)]
pub struct WebIngressFactory {
    observer: Rc<WebIngressObserver>,
    replica: Option<WebIngressReplica>,
}

impl WebIngressFactory {
    /// Creates a factory whose Module Instance policy comes from the Resolved App Plan.
    #[must_use]
    pub fn new() -> Self {
        Self {
            observer: Rc::new(WebIngressObserver::default()),
            replica: None,
        }
    }

    /// Creates one lane replica backed by a host-owned same-port listener coordinator.
    pub fn replicated(coordinator: &WebIngressListenerCoordinator) -> Result<Self, RuntimeFailure> {
        let replica = coordinator.allocate_replica()?;
        Ok(Self {
            observer: Rc::new(WebIngressObserver::default()),
            replica: Some(replica),
        })
    }

    #[must_use]
    pub fn local_address(&self) -> Option<SocketAddr> {
        self.observer.local_address.get()
    }

    /// Returns the canonical route manifest after activation succeeds.
    #[must_use]
    pub fn route_manifest(&self) -> Option<WebIngressRouteManifest> {
        self.observer.route_manifest.borrow().clone()
    }
}

impl Default for WebIngressFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeModuleFactory for WebIngressFactory {
    fn package_id(&self) -> &'static str {
        PACKAGE_ID
    }

    fn package_version(&self) -> &'static str {
        PACKAGE_VERSION
    }

    fn instantiate(
        &self,
        context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        let config =
            serde_json::from_str::<WebIngressConfig>(context.configuration()).map_err(|error| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("Web Ingress configuration is invalid: {error}"),
                }
            })?;
        config
            .validate()
            .map_err(|detail| RuntimeFailure::InvalidResolvedPlan {
                detail: format!("Web Ingress configuration is invalid: {detail}"),
            })?;
        if let Some(replica) = &self.replica {
            replica.validate_config(&config).map_err(|detail| {
                RuntimeFailure::InvalidResolvedPlan {
                    detail: format!("Web Ingress replica configuration is invalid: {detail}"),
                }
            })?;
        }
        Ok(NativeModuleInstance::with_lifecycle(
            Vec::new(),
            WebIngressLifecycle {
                config,
                observer: self.observer.clone(),
                listener: Rc::new(RefCell::new(None)),
                replica: self.replica.clone(),
            },
        ))
    }
}

struct WebIngressLifecycle {
    config: WebIngressConfig,
    observer: Rc<WebIngressObserver>,
    listener: Rc<RefCell<Option<TcpListener>>>,
    replica: Option<WebIngressReplica>,
}

impl fmt::Debug for WebIngressLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebIngressLifecycle")
            .field("config", &self.config)
            .field("observer", &self.observer)
            .finish_non_exhaustive()
    }
}

impl ModuleLifecycle for WebIngressLifecycle {
    fn prepare(&self, _context: PrepareContext) -> ModuleFuture {
        let config = self.config.clone();
        let observer = self.observer.clone();
        let listener = self.listener.clone();
        let replica = self.replica.clone();
        Box::pin(async move {
            if let Some(replica) = replica {
                observer.local_address.set(Some(replica.local_address()));
                return Ok(());
            }
            let bound = TcpListener::bind(config.bind_address())
                .await
                .map_err(|error| module_failure(format!("Web Ingress bind failed: {error}")))?;
            let address = bound
                .local_addr()
                .map_err(|error| module_failure(format!("Web Ingress address failed: {error}")))?;
            observer.local_address.set(Some(address));
            listener.borrow_mut().replace(bound);
            Ok(())
        })
    }

    fn activate(&self, context: ActivateContext) -> ModuleFuture {
        let listener = self.listener.borrow_mut().take();
        let replica = self.replica.clone();
        if listener.is_none() && replica.is_none() {
            return Box::pin(futures::future::ready(Err(RuntimeFailure::Internal {
                detail: "Web Ingress listener was not prepared".to_owned(),
            })));
        }
        let config = self.config.clone();
        let dependencies = context.dependencies().clone();
        let readiness = context.readiness();
        let tasks = context.tasks().clone();
        let cancellation = context.cancellation();
        let observer = self.observer.clone();
        Box::pin(async move {
            let routes =
                routing::RouteTable::resolve(&dependencies, config.request_timeout()).await?;
            observer
                .route_manifest
                .borrow_mut()
                .replace(routes.manifest().clone());
            let source = match replica {
                Some(replica) => {
                    let source = replica.register(routes.manifest().clone())?;
                    server::ConnectionSource::Replica(source)
                }
                None => server::ConnectionSource::Listener(
                    listener.expect("a non-replicated listener was prepared"),
                ),
            };
            tasks
                .spawn_local(Box::pin(async move {
                    readiness.wait().await;
                    let result = server::serve(source, config, routes, cancellation).await;
                    server::assert_server_result(result);
                }))
                .map_err(|error| {
                    module_failure(format!("Web Ingress task could not start: {error:?}"))
                })?;
            Ok(())
        })
    }
}

fn module_failure(detail: impl Into<String>) -> RuntimeFailure {
    RuntimeFailure::ModuleFailure {
        detail: detail.into(),
    }
}
