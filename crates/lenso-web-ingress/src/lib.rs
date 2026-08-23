//! General-purpose linked Rust HTTP Ingress Module for Lenso backends.

mod replication;
mod routing;
mod server;

use std::{
    cell::{Cell, RefCell},
    fmt,
    net::SocketAddr,
    rc::Rc,
    time::Duration,
};

use lenso_kernel::{
    ActivateContext, ModuleFuture, ModuleLifecycle, PrepareContext, RuntimeFailure,
};
use lenso_native_adapter::{NativeModuleFactory, NativeModuleFactoryContext, NativeModuleInstance};
use tokio::net::TcpListener;

pub use replication::{WebIngressReplicaMismatch, WebIngressRoute, WebIngressRouteManifest};

pub const PACKAGE_ID: &str = "lenso.web-ingress";
pub const PACKAGE_VERSION: &str = "0.1.0";

/// Immutable HTTP policy for one Web Ingress Module Instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebIngressConfig {
    pub bind_address: SocketAddr,
    pub max_request_body_bytes: usize,
    pub max_request_head_bytes: usize,
    pub max_concurrent_requests: usize,
    pub request_timeout: Duration,
}

impl Default for WebIngressConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 0)),
            max_request_body_bytes: 1024 * 1024,
            max_request_head_bytes: 16 * 1024,
            max_concurrent_requests: 128,
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl WebIngressConfig {
    fn validate(&self) -> Result<(), RuntimeFailure> {
        if self.max_request_body_bytes == 0
            || self.max_request_head_bytes == 0
            || self.max_concurrent_requests == 0
            || self.request_timeout.is_zero()
        {
            return Err(module_failure(
                "Web Ingress limits and timeout must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct WebIngressObserver {
    local_address: Cell<Option<SocketAddr>>,
    route_manifest: RefCell<Option<WebIngressRouteManifest>>,
}

/// Native Module factory and observable endpoint handle for one Ingress.
#[derive(Clone, Debug)]
pub struct WebIngressFactory {
    config: WebIngressConfig,
    observer: Rc<WebIngressObserver>,
}

impl WebIngressFactory {
    #[must_use]
    pub fn new(config: WebIngressConfig) -> Self {
        Self {
            config,
            observer: Rc::new(WebIngressObserver::default()),
        }
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
        Self::new(WebIngressConfig::default())
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
        _context: NativeModuleFactoryContext<'_>,
    ) -> Result<NativeModuleInstance, RuntimeFailure> {
        self.config.validate()?;
        Ok(NativeModuleInstance::with_lifecycle(
            Vec::new(),
            WebIngressLifecycle {
                config: self.config.clone(),
                observer: self.observer.clone(),
                listener: Rc::new(RefCell::new(None)),
            },
        ))
    }
}

struct WebIngressLifecycle {
    config: WebIngressConfig,
    observer: Rc<WebIngressObserver>,
    listener: Rc<RefCell<Option<TcpListener>>>,
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
        Box::pin(async move {
            let bound = TcpListener::bind(config.bind_address)
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
        let Some(listener) = self.listener.borrow_mut().take() else {
            return Box::pin(futures::future::ready(Err(RuntimeFailure::Internal {
                detail: "Web Ingress listener was not prepared".to_owned(),
            })));
        };
        let config = self.config.clone();
        let dependencies = context.dependencies().clone();
        let readiness = context.readiness();
        let tasks = context.tasks().clone();
        let cancellation = context.cancellation();
        let observer = self.observer.clone();
        Box::pin(async move {
            let routes =
                routing::RouteTable::resolve(&dependencies, config.request_timeout).await?;
            observer
                .route_manifest
                .borrow_mut()
                .replace(routes.manifest().clone());
            tasks
                .spawn_local(Box::pin(async move {
                    readiness.wait().await;
                    server::serve(listener, config, routes, cancellation).await;
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

#[cfg(test)]
mod tests {
    use super::WebIngressConfig;
    use std::net::SocketAddr;

    #[test]
    fn explicit_public_bind_addresses_are_supported() {
        let config = WebIngressConfig {
            bind_address: SocketAddr::from(([0, 0, 0, 0], 8080)),
            ..WebIngressConfig::default()
        };

        config.validate().expect("public bind should be valid");
    }

    #[test]
    fn zero_transport_limits_are_rejected() {
        let config = WebIngressConfig {
            max_concurrent_requests: 0,
            ..WebIngressConfig::default()
        };

        assert!(config.validate().is_err());
    }
}
