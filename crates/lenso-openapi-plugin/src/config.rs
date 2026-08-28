use serde::{Deserialize, Serialize};

/// Immutable `OpenAPI` document policy selected by App Composition.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpenApiConfig {
    title: String,
    version: String,
    description: Option<String>,
    document_path: String,
    servers: Vec<OpenApiServer>,
    components: Option<serde_json::Map<String, serde_json::Value>>,
}

impl OpenApiConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.title.trim().is_empty() {
            return Err("title must not be empty".to_owned());
        }
        if self.version.trim().is_empty() {
            return Err("version must not be empty".to_owned());
        }
        if !self.document_path.starts_with('/') || self.document_path.contains(['?', '#', '{', '}'])
        {
            return Err("document_path must be one static absolute path".to_owned());
        }
        if self
            .servers
            .iter()
            .any(|server| server.url.trim().is_empty())
        {
            return Err("server URLs must not be empty".to_owned());
        }
        Ok(())
    }

    pub(crate) fn title(&self) -> &str {
        &self.title
    }

    pub(crate) fn version(&self) -> &str {
        &self.version
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn document_path(&self) -> &str {
        &self.document_path
    }

    pub(crate) fn servers(&self) -> &[OpenApiServer] {
        &self.servers
    }

    pub(crate) fn components(&self) -> Option<&serde_json::Map<String, serde_json::Value>> {
        self.components.as_ref()
    }
}

impl Default for OpenApiConfig {
    fn default() -> Self {
        Self {
            title: "Lenso App".to_owned(),
            version: "0.0.0".to_owned(),
            description: None,
            document_path: "/openapi.json".to_owned(),
            servers: Vec::new(),
            components: None,
        }
    }
}

/// One explicitly configured `OpenAPI` server entry. No address is inferred from Ingress.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OpenApiServer {
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::OpenApiConfig;

    #[test]
    fn defaults_are_valid_and_unknown_configuration_is_rejected() {
        OpenApiConfig::default().validate().unwrap();
        let error = serde_json::from_str::<OpenApiConfig>(r#"{"enabled":true}"#).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn document_path_must_be_one_static_absolute_path() {
        for path in [
            "openapi.json",
            "/openapi/{version}.json",
            "/openapi.json?x=1",
        ] {
            let config = serde_json::from_value::<OpenApiConfig>(serde_json::json!({
                "document_path": path
            }))
            .unwrap();
            assert!(config.validate().is_err(), "accepted {path}");
        }
    }
}
