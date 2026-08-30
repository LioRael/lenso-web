use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

const MAX_TRANSFER_BYTES: usize = 64 * 1024 * 1024;
const MAX_HEAD_BYTES: usize = 1024 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 4_096;
const MAX_CONNECTIONS: usize = 65_536;
const MAX_TIMEOUT_MILLIS: u64 = 300_000;

/// Immutable HTTP policy for one Web Ingress Plugin Instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WebIngressConfig {
    #[serde(default = "default_bind_address")]
    bind_address: SocketAddr,
    #[serde(default = "default_max_request_body_bytes")]
    max_request_body_bytes: usize,
    #[serde(default = "default_max_request_head_bytes")]
    max_request_head_bytes: usize,
    #[serde(default = "default_max_concurrent_requests")]
    max_concurrent_requests: usize,
    #[serde(default = "default_max_connections")]
    max_connections: usize,
    #[serde(default = "default_request_head_timeout_millis")]
    request_head_timeout_millis: u64,
    #[serde(default = "default_request_body_timeout_millis")]
    request_body_timeout_millis: u64,
    #[serde(default = "default_connection_idle_timeout_millis")]
    connection_idle_timeout_millis: u64,
    #[serde(default = "default_shutdown_grace_timeout_millis")]
    shutdown_grace_timeout_millis: u64,
    #[serde(default = "default_request_timeout_millis")]
    request_timeout_millis: u64,
}

impl Default for WebIngressConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            max_request_body_bytes: default_max_request_body_bytes(),
            max_request_head_bytes: default_max_request_head_bytes(),
            max_concurrent_requests: default_max_concurrent_requests(),
            max_connections: default_max_connections(),
            request_head_timeout_millis: default_request_head_timeout_millis(),
            request_body_timeout_millis: default_request_body_timeout_millis(),
            connection_idle_timeout_millis: default_connection_idle_timeout_millis(),
            shutdown_grace_timeout_millis: default_shutdown_grace_timeout_millis(),
            request_timeout_millis: default_request_timeout_millis(),
        }
    }
}

impl WebIngressConfig {
    /// Replaces the listener address. Loopback with port zero remains the default.
    pub fn with_bind_address(mut self, address: SocketAddr) -> Result<Self, String> {
        self.bind_address = address;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the maximum accepted request body and canonical request head sizes.
    pub fn with_request_limits(
        mut self,
        max_request_body_bytes: usize,
        max_request_head_bytes: usize,
    ) -> Result<Self, String> {
        self.max_request_body_bytes = max_request_body_bytes;
        self.max_request_head_bytes = max_request_head_bytes;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the number of HTTP requests admitted concurrently by this Instance.
    pub fn with_max_concurrent_requests(mut self, maximum: usize) -> Result<Self, String> {
        self.max_concurrent_requests = maximum;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the maximum live connections and keep-alive idle deadline.
    pub fn with_connection_limits(
        mut self,
        maximum: usize,
        idle_timeout: Duration,
    ) -> Result<Self, String> {
        self.max_connections = maximum;
        self.connection_idle_timeout_millis = duration_millis(idle_timeout)?;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the total request-head and request-body read deadlines.
    pub fn with_request_read_timeouts(
        mut self,
        head_timeout: Duration,
        body_timeout: Duration,
    ) -> Result<Self, String> {
        self.request_head_timeout_millis = duration_millis(head_timeout)?;
        self.request_body_timeout_millis = duration_millis(body_timeout)?;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the deadline applied to one Endpoint Capability invocation.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, String> {
        self.request_timeout_millis = duration_millis(timeout)?;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the bounded drain window used after graceful App shutdown begins.
    pub fn with_shutdown_grace_timeout(mut self, timeout: Duration) -> Result<Self, String> {
        self.shutdown_grace_timeout_millis = duration_millis(timeout)?;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !(1..=MAX_TRANSFER_BYTES).contains(&self.max_request_body_bytes)
            || !(1..=MAX_HEAD_BYTES).contains(&self.max_request_head_bytes)
            || !(1..=MAX_CONCURRENT_REQUESTS).contains(&self.max_concurrent_requests)
            || !(1..=MAX_CONNECTIONS).contains(&self.max_connections)
            || !(1..=MAX_TIMEOUT_MILLIS).contains(&self.request_head_timeout_millis)
            || !(1..=MAX_TIMEOUT_MILLIS).contains(&self.request_body_timeout_millis)
            || !(1..=MAX_TIMEOUT_MILLIS).contains(&self.connection_idle_timeout_millis)
            || !(1..=MAX_TIMEOUT_MILLIS).contains(&self.shutdown_grace_timeout_millis)
            || !(1..=MAX_TIMEOUT_MILLIS).contains(&self.request_timeout_millis)
        {
            return Err("Web Ingress limits or timeout are invalid".to_owned());
        }
        Ok(())
    }

    pub(crate) const fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }

    pub(crate) const fn max_request_body_bytes(&self) -> usize {
        self.max_request_body_bytes
    }

    pub(crate) const fn max_request_head_bytes(&self) -> usize {
        self.max_request_head_bytes
    }

    pub(crate) const fn max_concurrent_requests(&self) -> usize {
        self.max_concurrent_requests
    }

    /// Returns the resolved-Plan limits Hosts should apply to each HTTP Endpoint binding.
    ///
    /// The returned copyable tuple is `(queue_capacity, max_concurrency)`. Ingress already queues
    /// at its own semaphore, so the Endpoint binding needs the same execution ceiling but no
    /// second queue. A Host may deliberately choose a stricter binding policy.
    pub const fn endpoint_admission_limits(&self) -> (usize, usize) {
        (0, self.max_concurrent_requests)
    }

    pub(crate) const fn max_connections(&self) -> usize {
        self.max_connections
    }

    pub(crate) fn request_head_timeout(&self) -> Duration {
        Duration::from_millis(self.request_head_timeout_millis)
    }

    pub(crate) fn request_body_timeout(&self) -> Duration {
        Duration::from_millis(self.request_body_timeout_millis)
    }

    pub(crate) fn connection_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_idle_timeout_millis)
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_millis)
    }

    pub(crate) fn shutdown_grace_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_grace_timeout_millis)
    }
}

const fn default_bind_address() -> SocketAddr {
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0)
}

const fn default_max_request_body_bytes() -> usize {
    1024 * 1024
}

const fn default_max_request_head_bytes() -> usize {
    16 * 1024
}

const fn default_max_concurrent_requests() -> usize {
    128
}

const fn default_max_connections() -> usize {
    1_024
}

const fn default_request_head_timeout_millis() -> u64 {
    10_000
}

const fn default_request_body_timeout_millis() -> u64 {
    30_000
}

const fn default_connection_idle_timeout_millis() -> u64 {
    60_000
}

const fn default_shutdown_grace_timeout_millis() -> u64 {
    default_request_timeout_millis()
}

const fn default_request_timeout_millis() -> u64 {
    30_000
}

fn duration_millis(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_millis())
        .map_err(|_| "Web Ingress timeout does not fit the Plan format".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_bind_addresses_are_valid_plan_configuration() {
        let config = WebIngressConfig::default()
            .with_bind_address(SocketAddr::from(([0, 0, 0, 0], 8080)))
            .expect("public bind should be valid");
        let encoded = serde_json::to_string(&config).expect("configuration should encode");
        assert_eq!(
            serde_json::from_str::<WebIngressConfig>(&encoded).unwrap(),
            config
        );
    }

    #[test]
    fn zero_and_excessive_limits_are_rejected() {
        assert!(
            WebIngressConfig::default()
                .with_max_concurrent_requests(0)
                .is_err()
        );
        assert!(
            WebIngressConfig::default()
                .with_request_limits(MAX_TRANSFER_BYTES + 1, 1024)
                .is_err()
        );
        assert!(
            WebIngressConfig::default()
                .with_request_timeout(Duration::from_millis(MAX_TIMEOUT_MILLIS + 1))
                .is_err()
        );
        assert!(
            WebIngressConfig::default()
                .with_connection_limits(0, Duration::from_secs(1))
                .is_err()
        );
        assert!(
            WebIngressConfig::default()
                .with_request_read_timeouts(Duration::ZERO, Duration::from_secs(1))
                .is_err()
        );
        assert!(
            WebIngressConfig::default()
                .with_shutdown_grace_timeout(Duration::ZERO)
                .is_err()
        );
    }

    #[test]
    fn endpoint_admission_tracks_the_ingress_execution_ceiling() {
        let config = WebIngressConfig::default()
            .with_max_concurrent_requests(17)
            .unwrap();
        assert_eq!(config.endpoint_admission_limits(), (0, 17));
    }
}
