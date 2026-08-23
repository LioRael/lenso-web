use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

const MAX_TRANSFER_BYTES: usize = 64 * 1024 * 1024;
const MAX_HEAD_BYTES: usize = 1024 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 4_096;
const MAX_TIMEOUT_MILLIS: u64 = 300_000;

/// Immutable HTTP policy for one Web Ingress Module Instance.
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

    /// Replaces the deadline applied to one Endpoint Capability invocation.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, String> {
        self.request_timeout_millis = u64::try_from(timeout.as_millis())
            .map_err(|_| "Web Ingress timeout does not fit the Plan format".to_owned())?;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if !(1..=MAX_TRANSFER_BYTES).contains(&self.max_request_body_bytes)
            || !(1..=MAX_HEAD_BYTES).contains(&self.max_request_head_bytes)
            || !(1..=MAX_CONCURRENT_REQUESTS).contains(&self.max_concurrent_requests)
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

    pub(crate) fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_millis)
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

const fn default_request_timeout_millis() -> u64 {
    30_000
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
    }
}
