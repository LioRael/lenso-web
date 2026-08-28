use std::{collections::BTreeSet, time::Duration};

use reqwest::Url;
use serde::{Deserialize, Serialize};

const MAX_TRANSFER_BYTES: usize = 64 * 1024 * 1024;
const MAX_HEAD_BYTES: usize = 1024 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 4_096;
const MAX_TIMEOUT_MILLIS: u64 = 300_000;
const MAX_ALLOWED_ORIGINS: usize = 256;
const MAX_URL_BYTES: usize = 4_096;

/// Immutable outbound HTTP authority and resource limits for one Plugin Instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpEgressConfig {
    allowed_origins: Vec<String>,
    #[serde(default = "default_max_request_body_bytes")]
    max_request_body_bytes: usize,
    #[serde(default = "default_max_request_head_bytes")]
    max_request_head_bytes: usize,
    #[serde(default = "default_max_response_body_bytes")]
    max_response_body_bytes: usize,
    #[serde(default = "default_max_response_head_bytes")]
    max_response_head_bytes: usize,
    #[serde(default = "default_max_concurrent_requests")]
    max_concurrent_requests: usize,
    #[serde(default = "default_connect_timeout_millis")]
    connect_timeout_millis: u64,
    #[serde(default = "default_request_timeout_millis")]
    request_timeout_millis: u64,
}

impl HttpEgressConfig {
    /// Creates a bounded policy with conservative defaults and exact allowed origins.
    pub fn new(
        allowed_origins: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, String> {
        let config = Self {
            allowed_origins: allowed_origins.into_iter().map(Into::into).collect(),
            max_request_body_bytes: default_max_request_body_bytes(),
            max_request_head_bytes: default_max_request_head_bytes(),
            max_response_body_bytes: default_max_response_body_bytes(),
            max_response_head_bytes: default_max_response_head_bytes(),
            max_concurrent_requests: default_max_concurrent_requests(),
            connect_timeout_millis: default_connect_timeout_millis(),
            request_timeout_millis: default_request_timeout_millis(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Replaces request and response body/head limits.
    pub fn with_transfer_limits(
        mut self,
        max_request_body_bytes: usize,
        max_request_head_bytes: usize,
        max_response_body_bytes: usize,
        max_response_head_bytes: usize,
    ) -> Result<Self, String> {
        self.max_request_body_bytes = max_request_body_bytes;
        self.max_request_head_bytes = max_request_head_bytes;
        self.max_response_body_bytes = max_response_body_bytes;
        self.max_response_head_bytes = max_response_head_bytes;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the maximum number of simultaneously active requests.
    pub fn with_max_concurrent_requests(mut self, maximum: usize) -> Result<Self, String> {
        self.max_concurrent_requests = maximum;
        self.validate()?;
        Ok(self)
    }

    /// Replaces connect and total request timeouts.
    pub fn with_timeouts(
        mut self,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, String> {
        self.connect_timeout_millis = duration_millis(connect_timeout)?;
        self.request_timeout_millis = duration_millis(request_timeout)?;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn validate(&self) -> Result<BTreeSet<String>, String> {
        if self.allowed_origins.is_empty() || self.allowed_origins.len() > MAX_ALLOWED_ORIGINS {
            return Err("outbound HTTP origin count must be between 1 and 256".to_owned());
        }
        let origins = self
            .allowed_origins
            .iter()
            .map(|origin| canonical_origin(origin))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if origins.len() != self.allowed_origins.len() {
            return Err("outbound HTTP origins must be unique".to_owned());
        }
        if !(1..=MAX_TRANSFER_BYTES).contains(&self.max_request_body_bytes)
            || !(1..=MAX_TRANSFER_BYTES).contains(&self.max_response_body_bytes)
            || !(1..=MAX_HEAD_BYTES).contains(&self.max_request_head_bytes)
            || !(1..=MAX_HEAD_BYTES).contains(&self.max_response_head_bytes)
            || !(1..=MAX_CONCURRENT_REQUESTS).contains(&self.max_concurrent_requests)
            || !(1..=MAX_TIMEOUT_MILLIS).contains(&self.connect_timeout_millis)
            || !(1..=MAX_TIMEOUT_MILLIS).contains(&self.request_timeout_millis)
            || self.connect_timeout_millis > self.request_timeout_millis
        {
            return Err("outbound HTTP limits or timeouts are invalid".to_owned());
        }
        Ok(origins)
    }

    pub(crate) const fn max_request_body_bytes(&self) -> usize {
        self.max_request_body_bytes
    }

    pub(crate) const fn max_request_head_bytes(&self) -> usize {
        self.max_request_head_bytes
    }

    pub(crate) const fn max_response_body_bytes(&self) -> usize {
        self.max_response_body_bytes
    }

    pub(crate) const fn max_response_head_bytes(&self) -> usize {
        self.max_response_head_bytes
    }

    pub(crate) const fn max_concurrent_requests(&self) -> usize {
        self.max_concurrent_requests
    }

    pub(crate) fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_millis)
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_millis)
    }
}

pub(crate) fn request_origin(url: &Url) -> Option<String> {
    matches!(url.scheme(), "http" | "https")
        .then(|| url.origin().ascii_serialization())
        .filter(|origin| origin != "null")
}

fn canonical_origin(value: &str) -> Result<String, String> {
    if value.len() > MAX_URL_BYTES {
        return Err("outbound HTTP origin is too long".to_owned());
    }
    let url = Url::parse(value).map_err(|_| format!("invalid outbound HTTP origin `{value}`"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(format!("invalid outbound HTTP origin `{value}`"));
    }
    request_origin(&url).ok_or_else(|| format!("invalid outbound HTTP origin `{value}`"))
}

fn duration_millis(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_millis())
        .map_err(|_| "outbound HTTP timeout does not fit the Plan format".to_owned())
}

const fn default_max_request_body_bytes() -> usize {
    1024 * 1024
}

const fn default_max_request_head_bytes() -> usize {
    16 * 1024
}

const fn default_max_response_body_bytes() -> usize {
    4 * 1024 * 1024
}

const fn default_max_response_head_bytes() -> usize {
    32 * 1024
}

const fn default_max_concurrent_requests() -> usize {
    64
}

const fn default_connect_timeout_millis() -> u64 {
    5_000
}

const fn default_request_timeout_millis() -> u64 {
    30_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_are_exact_authority_not_url_prefixes() {
        let config = HttpEgressConfig::new(["https://api.example.test:443"]).unwrap();
        let origins = config.validate().unwrap();
        assert!(origins.contains("https://api.example.test"));
        assert_eq!(
            request_origin(&Url::parse("https://api.example.test/v1").unwrap()).as_deref(),
            Some("https://api.example.test")
        );
        assert!(HttpEgressConfig::new(["https://api.example.test/v1"]).is_err());
        assert!(HttpEgressConfig::new(["file:///tmp/socket"]).is_err());
    }
}
