use std::env;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use flume::Sender;
use isahc::{AsyncReadResponseExt, HttpClient, Request};
use serde::Deserialize;
use serde_json::Value;

use crate::model::{Model, ModelEntry, ModelFamily, ModelInfo, ModelPricing, ModelTier};
use crate::provider::{BoxFuture, Provider};
use crate::{AgentError, Message, ProviderEvent, RequestOptions, StreamResponse};

use super::google;
use super::{Timeouts, http_client, user_agent};

const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const DEFAULT_LOCATION: &str = "global";
const METADATA_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
const OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const REFRESH_MARGIN: Duration = Duration::from_secs(60);
const CONTEXT_WINDOW: u32 = 1_048_576;
const MAX_OUTPUT_TOKENS: u32 = 65_536;

inventory::submit!(maki_config::providers::BuiltInProvider {
    slug: "vertex",
    display_name: "Google Vertex AI",
    protocol: maki_config::providers::Protocol::Google,
    default_base_url: "https://aiplatform.googleapis.com/v1",
    default_api_key_env: "",
    default_model: "vertex/gemini-2.5-pro",
    plans: None,
    login_url: None,
    needs_url: false,
});

const MODELS: &[ModelEntry] = &[
    model(
        &["gemini-2.5-pro"],
        ModelTier::Strong,
        true,
        1.25,
        10.0,
        0.375,
        0.125,
    ),
    model(
        &["gemini-2.5-flash"],
        ModelTier::Medium,
        true,
        0.3,
        2.5,
        0.083_333_333_333_333_3,
        0.03,
    ),
    model(
        &["gemini-2.5-flash-lite"],
        ModelTier::Weak,
        true,
        0.1,
        0.4,
        0.083_333_333_333_333_3,
        0.01,
    ),
    model(
        &["gemini-3-flash-preview"],
        ModelTier::Medium,
        false,
        0.5,
        3.0,
        0.083_333_333_333_333_3,
        0.05,
    ),
    model(
        &["gemini-3.1-pro-preview"],
        ModelTier::Medium,
        false,
        2.0,
        12.0,
        0.375,
        0.2,
    ),
    model(
        &["gemini-3.1-flash-lite"],
        ModelTier::Weak,
        false,
        0.25,
        1.5,
        0.083_333_333_333_333_3,
        0.025,
    ),
    model(
        &["gemini-3.5-flash"],
        ModelTier::Medium,
        false,
        1.5,
        9.0,
        0.083_333_333_333_333_3,
        0.15,
    ),
    model(
        &["gemini-3.5-flash-lite"],
        ModelTier::Weak,
        false,
        0.3,
        2.5,
        0.083_333_333_333_333_3,
        0.03,
    ),
    model(
        &["gemini-3.6-flash", "gemini-3.7-flash", "gemini-3.8-flash"],
        ModelTier::Medium,
        false,
        0.75,
        3.75,
        0.041_666_666_666_666_7,
        0.075,
    ),
];

pub(crate) const fn models() -> &'static [ModelEntry] {
    MODELS
}

const fn model(
    prefixes: &'static [&'static str],
    tier: ModelTier,
    default: bool,
    input: f64,
    output: f64,
    cache_write: f64,
    cache_read: f64,
) -> ModelEntry {
    ModelEntry {
        prefixes,
        tier,
        family: ModelFamily::Gemini,
        vision: true,
        default,
        pricing: ModelPricing {
            input,
            output,
            cache_write,
            cache_read,
            fast: None,
        },
        max_output_tokens: Some(MAX_OUTPUT_TOKENS),
        context_window: CONTEXT_WINDOW,
    }
}

pub struct Vertex {
    client: HttpClient,
    endpoint: VertexEndpoint,
    tokens: AdcTokenProvider,
    stream_timeout: Duration,
}

impl Vertex {
    pub fn new(timeouts: Timeouts) -> Result<Self, AgentError> {
        let config = maki_config::providers::ProvidersConfig::load();
        let provider = config.get("vertex");
        let project = env::var("GOOGLE_CLOUD_PROJECT")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| provider.and_then(|def| def.project.clone()))
            .ok_or_else(|| AgentError::Config {
                message: "Vertex AI requires GOOGLE_CLOUD_PROJECT or providers.vertex.project"
                    .into(),
            })?;
        let location = env::var("GOOGLE_CLOUD_LOCATION")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| provider.and_then(|def| def.location.clone()))
            .unwrap_or_else(|| DEFAULT_LOCATION.into());
        Ok(Self {
            client: http_client(timeouts),
            endpoint: VertexEndpoint::new(project, location),
            tokens: AdcTokenProvider::from_environment()?,
            stream_timeout: timeouts.stream,
        })
    }

    async fn send_stream(
        &self,
        model: &Model,
        body: &[u8],
        force_refresh: bool,
    ) -> Result<isahc::Response<isahc::AsyncBody>, AgentError> {
        let token = self.tokens.token(&self.client, force_refresh).await?;
        let mut request = Request::builder()
            .method("POST")
            .uri(self.endpoint.stream_url(&model.id))
            .header("user-agent", user_agent())
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"));
        if let Some(project) = self.tokens.quota_project() {
            request = request.header("x-goog-user-project", project);
        }
        Ok(self.client.send_async(request.body(body.to_vec())?).await?)
    }

    async fn do_stream(
        &self,
        model: &Model,
        messages: &[Message],
        system: &str,
        tools: &Value,
        event_tx: &Sender<ProviderEvent>,
        options: RequestOptions,
    ) -> Result<StreamResponse, AgentError> {
        let mut body = google::build_body(model, messages, system, tools, options.thinking);
        uppercase_schema_types(&mut body);
        let body = serde_json::to_vec(&body)?;
        let response = self.send_stream(model, &body, false).await?;
        if response.status().as_u16() == 401 {
            let response = self.send_stream(model, &body, true).await?;
            if response.status().as_u16() != 200 {
                return Err(AgentError::from_response(response).await);
            }
            return google::parse_sse(response, event_tx, self.stream_timeout).await;
        }
        if response.status().as_u16() != 200 {
            return Err(AgentError::from_response(response).await);
        }
        google::parse_sse(response, event_tx, self.stream_timeout).await
    }
}

impl Provider for Vertex {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        options: RequestOptions,
        _session_id: Option<&'a maki_storage::id::SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(self.do_stream(model, messages, system, tools, event_tx, options))
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async {
            Ok(models()
                .iter()
                .flat_map(|entry| entry.prefixes)
                .map(|id| ModelInfo::id_only((*id).into()))
                .collect())
        })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { self.tokens.clear() })
    }
}

struct VertexEndpoint {
    project: String,
    location: String,
}

impl VertexEndpoint {
    fn new(project: String, location: String) -> Self {
        Self { project, location }
    }

    fn stream_url(&self, model: &str) -> String {
        let host = if self.location == DEFAULT_LOCATION {
            "https://aiplatform.googleapis.com".to_string()
        } else {
            format!("https://{}-aiplatform.googleapis.com", self.location)
        };
        format!(
            "{host}/v1/projects/{}/locations/{}/publishers/google/models/{}:streamGenerateContent?alt=sse",
            super::urlenc(&self.project),
            super::urlenc(&self.location),
            super::urlenc(model),
        )
    }
}

enum AdcSource {
    AuthorizedUser {
        user: AuthorizedUser,
        quota_project: Option<String>,
    },
    Metadata,
}

struct AdcTokenProvider {
    source: AdcSource,
    cached: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    value: String,
    expires_at: Instant,
}

#[derive(Deserialize)]
struct AuthorizedUser {
    client_id: String,
    client_secret: String,
    refresh_token: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}

#[derive(Deserialize)]
struct AdcFile {
    r#type: String,
    quota_project_id: Option<String>,
    #[serde(flatten)]
    fields: serde_json::Map<String, Value>,
}

fn default_token_uri() -> String {
    OAUTH_TOKEN_URL.into()
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

impl AdcTokenProvider {
    fn from_environment() -> Result<Self, AgentError> {
        let path = env::var_os("GOOGLE_APPLICATION_CREDENTIALS")
            .map(PathBuf::from)
            .or_else(default_adc_path);
        let source = match path {
            Some(path) if path.exists() => Self::source_from_file(path)?,
            _ => AdcSource::Metadata,
        };
        Ok(Self {
            source,
            cached: Mutex::new(None),
        })
    }

    fn source_from_file(path: PathBuf) -> Result<AdcSource, AgentError> {
        let file: AdcFile = serde_json::from_slice(&std::fs::read(path)?)?;
        if file.r#type != "authorized_user" {
            return Err(AgentError::Config {
                message: format!(
                    "Vertex ADC credential type '{}' is not supported; use `gcloud auth application-default login` or an attached service account",
                    file.r#type
                ),
            });
        }
        let user = serde_json::from_value(Value::Object(file.fields))?;
        Ok(AdcSource::AuthorizedUser {
            user,
            quota_project: file.quota_project_id,
        })
    }

    fn clear(&self) -> Result<(), AgentError> {
        *self.cached.lock().unwrap() = None;
        Ok(())
    }

    async fn token(&self, client: &HttpClient, force_refresh: bool) -> Result<String, AgentError> {
        if !force_refresh
            && let Some(token) = self.cached.lock().unwrap().as_ref()
            && token.expires_at > Instant::now() + REFRESH_MARGIN
        {
            return Ok(token.value.clone());
        }
        let token = match &self.source {
            AdcSource::AuthorizedUser { user, .. } => refresh_authorized_user(client, user).await?,
            AdcSource::Metadata => refresh_metadata_token(client).await?,
        };
        let value = token.access_token;
        *self.cached.lock().unwrap() = Some(CachedToken {
            value: value.clone(),
            expires_at: Instant::now() + Duration::from_secs(token.expires_in),
        });
        Ok(value)
    }

    fn quota_project(&self) -> Option<&str> {
        match &self.source {
            AdcSource::AuthorizedUser { quota_project, .. } => quota_project.as_deref(),
            AdcSource::Metadata => None,
        }
    }
}

fn uppercase_schema_types(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(schema_type)) = map.get_mut("type") {
                *schema_type = schema_type.to_uppercase();
            }
            for value in map.values_mut() {
                uppercase_schema_types(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                uppercase_schema_types(value);
            }
        }
        _ => {}
    }
}

fn default_adc_path() -> Option<PathBuf> {
    env::var_os("HOME")
        .map(|home| PathBuf::from(home).join(".config/gcloud/application_default_credentials.json"))
}

async fn refresh_authorized_user(
    client: &HttpClient,
    user: &AuthorizedUser,
) -> Result<TokenResponse, AgentError> {
    let body = format!(
        "client_id={}&client_secret={}&refresh_token={}&grant_type=refresh_token&scope={}",
        super::urlenc(&user.client_id),
        super::urlenc(&user.client_secret),
        super::urlenc(&user.refresh_token),
        super::urlenc(CLOUD_PLATFORM_SCOPE),
    );
    let request = Request::builder()
        .method("POST")
        .uri(&user.token_uri)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)?;
    token_response(client.send_async(request).await?).await
}

async fn refresh_metadata_token(client: &HttpClient) -> Result<TokenResponse, AgentError> {
    let request = Request::builder()
        .method("GET")
        .uri(METADATA_TOKEN_URL)
        .header("metadata-flavor", "Google")
        .body(())?;
    token_response(client.send_async(request).await?).await
}

async fn token_response(
    mut response: isahc::Response<isahc::AsyncBody>,
) -> Result<TokenResponse, AgentError> {
    if response.status().as_u16() != 200 {
        return Err(AgentError::from_response(response).await);
    }
    Ok(serde_json::from_str(&response.text().await?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case("global", "https://aiplatform.googleapis.com/v1/projects/project/locations/global/publishers/google/models/gemini-2.5-flash:streamGenerateContent?alt=sse" ; "global")]
    #[test_case("us-central1", "https://us-central1-aiplatform.googleapis.com/v1/projects/project/locations/us-central1/publishers/google/models/gemini-2.5-flash:streamGenerateContent?alt=sse" ; "regional")]
    fn stream_url_uses_matching_location_host(location: &str, expected: &str) {
        assert_eq!(
            VertexEndpoint::new("project".into(), location.into()).stream_url("gemini-2.5-flash"),
            expected
        );
    }

    #[test]
    fn schema_types_are_uppercase_for_vertex() {
        let mut value = serde_json::json!({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        });
        uppercase_schema_types(&mut value);
        assert_eq!(value["type"], "OBJECT");
        assert_eq!(value["properties"]["path"]["type"], "STRING");
    }
}
