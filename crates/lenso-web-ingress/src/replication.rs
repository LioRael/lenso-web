use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use futures::{StreamExt as _, stream::FuturesUnordered};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc, watch},
};

use crate::{WebIngressConfig, module_failure};

/// One canonical route entry used to validate same-port Ingress replicas.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WebIngressRoute {
    pub method: String,
    pub path: String,
    pub route_id: String,
}

impl WebIngressRoute {
    pub(crate) fn new(method: &str, path: &str, route_id: &str) -> Self {
        Self {
            method: method.to_owned(),
            path: path.to_owned(),
            route_id: route_id.to_owned(),
        }
    }
}

/// Canonical, payload-free routing identity for one prepared Ingress replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebIngressRouteManifest {
    routes: Vec<WebIngressRoute>,
}

impl WebIngressRouteManifest {
    pub(crate) fn new(mut routes: Vec<WebIngressRoute>) -> Self {
        routes.sort_unstable();
        Self { routes }
    }

    /// Returns the canonical route entries.
    pub fn routes(&self) -> &[WebIngressRoute] {
        &self.routes
    }

    /// Rejects a same-port replica whose route ownership differs.
    pub fn ensure_equivalent(&self, replica: &Self) -> Result<(), WebIngressReplicaMismatch> {
        if self == replica {
            Ok(())
        } else {
            Err(WebIngressReplicaMismatch)
        }
    }
}

/// Same-port replicas do not expose an identical route manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebIngressReplicaMismatch;

impl fmt::Display for WebIngressReplicaMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("same-port Web Ingress replicas have different route manifests")
    }
}

impl std::error::Error for WebIngressReplicaMismatch {}

#[derive(Debug)]
struct ReplicaSlot {
    manifest: Option<WebIngressRouteManifest>,
    connections: Option<mpsc::Sender<std::net::TcpStream>>,
}

#[derive(Debug)]
struct CoordinatorState {
    slots: Vec<ReplicaSlot>,
    canonical_manifest: Option<WebIngressRouteManifest>,
}

impl CoordinatorState {
    fn ready(&self) -> bool {
        self.slots.iter().all(|slot| slot.manifest.is_some())
    }
}

#[derive(Debug)]
struct ListenerCoordinatorInner {
    config: WebIngressConfig,
    local_address: SocketAddr,
    state: Mutex<CoordinatorState>,
    next_slot: AtomicUsize,
    next_connection: AtomicUsize,
    next_request_id: Arc<AtomicU64>,
    concurrency: Arc<Semaphore>,
    ready: watch::Sender<bool>,
    _shutdown: watch::Sender<bool>,
}

/// One host-owned listener shared by equivalent Web Ingress lane replicas.
///
/// The coordinator binds once, waits for every replica to publish an equivalent immutable route
/// manifest, and then distributes accepted sockets round-robin. Each lane registers the socket
/// with its own Tokio reactor before serving it.
#[derive(Clone, Debug)]
pub struct WebIngressListenerCoordinator {
    inner: Arc<ListenerCoordinatorInner>,
}

impl WebIngressListenerCoordinator {
    /// Binds one listener for an exact number of same-port replicas.
    pub async fn bind(
        config: WebIngressConfig,
        replica_count: usize,
    ) -> Result<Self, lenso_kernel::RuntimeFailure> {
        config.validate().map_err(module_failure)?;
        if replica_count == 0 {
            return Err(module_failure(
                "Web Ingress listener coordinator requires at least one replica",
            ));
        }
        let listener = TcpListener::bind(config.bind_address())
            .await
            .map_err(|error| module_failure(format!("Web Ingress bind failed: {error}")))?;
        let address = listener
            .local_addr()
            .map_err(|error| module_failure(format!("Web Ingress address failed: {error}")))?;
        let listener = listener.into_std().map_err(|error| {
            module_failure(format!("Web Ingress listener transfer failed: {error}"))
        })?;
        let (ready, ready_signal) = watch::channel(false);
        let (shutdown, shutdown_signal) = watch::channel(false);
        let inner = Arc::new(ListenerCoordinatorInner {
            concurrency: Arc::new(Semaphore::new(config.max_concurrent_requests())),
            config,
            local_address: address,
            state: Mutex::new(CoordinatorState {
                slots: (0..replica_count)
                    .map(|_| ReplicaSlot {
                        manifest: None,
                        connections: None,
                    })
                    .collect(),
                canonical_manifest: None,
            }),
            next_slot: AtomicUsize::new(0),
            next_connection: AtomicUsize::new(0),
            next_request_id: Arc::new(AtomicU64::new(0)),
            ready,
            _shutdown: shutdown,
        });
        spawn_acceptor(
            Arc::downgrade(&inner),
            listener,
            ready_signal,
            shutdown_signal,
        )?;
        Ok(Self { inner })
    }

    /// Returns the actual bound address, including an OS-selected port.
    pub fn local_address(&self) -> SocketAddr {
        self.inner.local_address
    }

    pub(crate) fn allocate_replica(
        &self,
    ) -> Result<WebIngressReplica, lenso_kernel::RuntimeFailure> {
        let slot = self.inner.next_slot.fetch_add(1, Ordering::Relaxed);
        let slot_count = self
            .inner
            .state
            .lock()
            .expect("Web Ingress coordinator state is not poisoned")
            .slots
            .len();
        if slot >= slot_count {
            return Err(module_failure(format!(
                "Web Ingress listener coordinator expected {slot_count} replicas"
            )));
        }
        Ok(WebIngressReplica {
            coordinator: self.clone(),
            slot,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WebIngressReplica {
    coordinator: WebIngressListenerCoordinator,
    slot: usize,
}

impl WebIngressReplica {
    pub(crate) fn local_address(&self) -> SocketAddr {
        self.coordinator.local_address()
    }

    pub(crate) fn validate_config(&self, config: &WebIngressConfig) -> Result<(), String> {
        if config == &self.coordinator.inner.config {
            Ok(())
        } else {
            Err("same-port Web Ingress replicas must use the coordinator configuration".to_owned())
        }
    }

    pub(crate) fn register(
        &self,
        manifest: WebIngressRouteManifest,
    ) -> Result<ReplicaConnectionSource, lenso_kernel::RuntimeFailure> {
        let queue_capacity = self
            .coordinator
            .inner
            .config
            .max_concurrent_requests()
            .max(1);
        let (connections, receiver) = mpsc::channel(queue_capacity);
        let mut state = self
            .coordinator
            .inner
            .state
            .lock()
            .expect("Web Ingress coordinator state is not poisoned");
        if let Some(canonical) = &state.canonical_manifest {
            canonical
                .ensure_equivalent(&manifest)
                .map_err(|error| module_failure(error.to_string()))?;
        } else {
            state.canonical_manifest = Some(manifest.clone());
        }
        let slot = &mut state.slots[self.slot];
        slot.manifest = Some(manifest);
        slot.connections = Some(connections);
        let ready = state.ready();
        drop(state);
        if ready {
            self.coordinator.inner.ready.send_replace(true);
        }
        Ok(ReplicaConnectionSource {
            receiver,
            concurrency: Arc::clone(&self.coordinator.inner.concurrency),
            next_request_id: Arc::clone(&self.coordinator.inner.next_request_id),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ReplicaConnectionSource {
    pub(crate) receiver: mpsc::Receiver<std::net::TcpStream>,
    pub(crate) concurrency: Arc<Semaphore>,
    pub(crate) next_request_id: Arc<AtomicU64>,
}

fn spawn_acceptor(
    coordinator: Weak<ListenerCoordinatorInner>,
    listener: std::net::TcpListener,
    mut ready: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), lenso_kernel::RuntimeFailure> {
    std::thread::Builder::new()
        .name("lenso-web-listener".to_owned())
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                while !*ready.borrow_and_update() {
                    tokio::select! {
                        changed = ready.changed() => if changed.is_err() { return },
                        changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return },
                    }
                }
                let Ok(listener) = TcpListener::from_std(listener) else {
                    return;
                };
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let Ok((stream, _)) = accepted else { return };
                            let Ok(stream) = stream.into_std() else { continue };
                            distribute_connection(&coordinator, stream).await;
                        }
                        changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return },
                    }
                }
            });
        })
        .map(|_| ())
        .map_err(|error| module_failure(format!("Web Ingress acceptor could not start: {error}")))
}

async fn distribute_connection(
    coordinator: &Weak<ListenerCoordinatorInner>,
    mut stream: std::net::TcpStream,
) {
    let Some(coordinator) = coordinator.upgrade() else {
        return;
    };
    let waiting_senders = {
        let state = coordinator
            .state
            .lock()
            .expect("Web Ingress coordinator state is not poisoned");
        let slot_count = state.slots.len();
        let start = coordinator.next_connection.fetch_add(1, Ordering::Relaxed) % slot_count;
        let mut waiting_senders = Vec::new();
        for offset in 0..slot_count {
            let slot = (start + offset) % slot_count;
            let Some(sender) = &state.slots[slot].connections else {
                continue;
            };
            match sender.try_send(stream) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    stream = returned;
                    waiting_senders.push(sender.clone());
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => stream = returned,
            }
        }
        waiting_senders
    };
    let reservations = waiting_senders
        .into_iter()
        .map(mpsc::Sender::reserve_owned)
        .collect::<FuturesUnordered<_>>();
    tokio::pin!(reservations);
    while let Some(result) = reservations.next().await {
        if let Ok(permit) = result {
            permit.send(stream);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::TcpStream, sync::Arc, time::Duration};

    use super::{
        WebIngressListenerCoordinator, WebIngressRoute, WebIngressRouteManifest,
        distribute_connection,
    };
    use crate::WebIngressConfig;

    fn tcp_stream() -> (TcpStream, TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let stream = listener.accept().unwrap().0;
        (stream, peer)
    }

    #[test]
    fn manifests_are_canonical_and_reject_route_shards() {
        let first = WebIngressRouteManifest::new(vec![
            WebIngressRoute::new("POST", "/orders", "orders.create"),
            WebIngressRoute::new("GET", "/orders/{id}", "orders.read"),
        ]);
        let reordered = WebIngressRouteManifest::new(vec![
            WebIngressRoute::new("GET", "/orders/{id}", "orders.read"),
            WebIngressRoute::new("POST", "/orders", "orders.create"),
        ]);
        let shard = WebIngressRouteManifest::new(vec![WebIngressRoute::new(
            "GET",
            "/orders/{id}",
            "orders.read",
        )]);

        assert_eq!(first, reordered);
        first.ensure_equivalent(&reordered).unwrap();
        assert!(first.ensure_equivalent(&shard).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinator_rejects_mismatched_replica_before_accepting() {
        let coordinator = WebIngressListenerCoordinator::bind(WebIngressConfig::default(), 2)
            .await
            .unwrap();
        let first = coordinator.allocate_replica().unwrap();
        let second = coordinator.allocate_replica().unwrap();
        first
            .register(WebIngressRouteManifest::new(vec![WebIngressRoute::new(
                "GET",
                "/orders",
                "orders.list",
            )]))
            .unwrap();
        let error = second
            .register(WebIngressRouteManifest::new(vec![WebIngressRoute::new(
                "GET",
                "/health",
                "health.read",
            )]))
            .unwrap_err();

        assert!(format!("{error:?}").contains("different route manifest"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinator_backpressures_when_every_replica_queue_is_full() {
        let config = WebIngressConfig::default()
            .with_max_concurrent_requests(1)
            .unwrap();
        let coordinator = WebIngressListenerCoordinator::bind(config, 2)
            .await
            .unwrap();
        let manifest = WebIngressRouteManifest::new(vec![WebIngressRoute::new(
            "GET",
            "/orders",
            "orders.list",
        )]);
        let mut first = coordinator
            .allocate_replica()
            .unwrap()
            .register(manifest.clone())
            .unwrap();
        let _second = coordinator
            .allocate_replica()
            .unwrap()
            .register(manifest)
            .unwrap();
        let weak = Arc::downgrade(&coordinator.inner);
        let (first_stream, _first_peer) = tcp_stream();
        let (second_stream, _second_peer) = tcp_stream();
        distribute_connection(&weak, first_stream).await;
        distribute_connection(&weak, second_stream).await;

        let (backpressured_stream, _backpressured_peer) = tcp_stream();
        let task = tokio::spawn(async move {
            distribute_connection(&weak, backpressured_stream).await;
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());

        first.receiver.recv().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("connection should resume when one queue has capacity")
            .unwrap();
        assert!(first.receiver.try_recv().is_ok());
    }
}
