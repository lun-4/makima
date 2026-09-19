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
        let tokens = AdcTokenProvider::from_environment()?;
        let project = resolve_project(
            env::var("GOOGLE_CLOUD_PROJECT").ok(),
            env::var("GCLOUD_PROJECT").ok(),
            provider.and_then(|def| def.project.clone()),
            tokens.quota_project(),
        )
        .ok_or_else(|| AgentError::Config {
            message: "Vertex AI requires GOOGLE_CLOUD_PROJECT, GCLOUD_PROJECT, providers.vertex.project, or an ADC quota project".into(),
        })?;
        let location = resolve_location(
            env::var("GOOGLE_CLOUD_LOCATION").ok(),
            provider.and_then(|def| def.location.clone()),
        );
        Ok(Self {
            client: http_client(timeouts),
            endpoint: VertexEndpoint::new(project, location),
            tokens,
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
        let quota_project = self
            .tokens
            .quota_project()
            .unwrap_or(&self.endpoint.project);
        request = request.header("x-goog-user-project", quota_project);
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
        normalize_tool_schemas(&mut body);
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
        let source = if let Some(path) = env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
            let path = PathBuf::from(path);
            if !path.exists() {
                return Err(AgentError::Config {
                    message: format!(
                        "GOOGLE_APPLICATION_CREDENTIALS does not exist: {}",
                        path.display()
                    ),
                });
            }
            Self::source_from_file(path)?
        } else if let Some(path) = default_adc_path().filter(|path| path.exists()) {
            Self::source_from_file(path)?
        } else {
            AdcSource::Metadata
        };
        Ok(Self {
            source,
            cached: Mutex::new(None),
        })
    }

    fn source_from_file(path: PathBuf) -> Result<AdcSource, AgentError> {
        Self::source_from_json(&std::fs::read(path)?)
    }

    fn source_from_json(bytes: &[u8]) -> Result<AdcSource, AgentError> {
        let file: AdcFile = serde_json::from_slice(bytes)?;
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
        if let Some(token) = self.cached_token(force_refresh, Instant::now()) {
            return Ok(token);
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

    fn cached_token(&self, force_refresh: bool, now: Instant) -> Option<String> {
        if force_refresh {
            return None;
        }
        let token = self.cached.lock().unwrap();
        token
            .as_ref()
            .filter(|token| token.expires_at > now + REFRESH_MARGIN)
            .map(|token| token.value.clone())
    }

    fn quota_project(&self) -> Option<&str> {
        match &self.source {
            AdcSource::AuthorizedUser { quota_project, .. } => quota_project.as_deref(),
            AdcSource::Metadata => None,
        }
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

fn resolve_project(
    google_cloud_project: Option<String>,
    gcloud_project: Option<String>,
    configured_project: Option<String>,
    quota_project: Option<&str>,
) -> Option<String> {
    nonempty(google_cloud_project)
        .or_else(|| nonempty(gcloud_project))
        .or_else(|| nonempty(configured_project))
        .or_else(|| {
            quota_project
                .filter(|project| !project.is_empty())
                .map(str::to_owned)
        })
}

fn resolve_location(
    google_cloud_location: Option<String>,
    configured_location: Option<String>,
) -> String {
    nonempty(google_cloud_location)
        .or_else(|| nonempty(configured_location))
        .unwrap_or_else(|| DEFAULT_LOCATION.into())
}

fn normalize_tool_schemas(body: &mut Value) {
    if let Some(tools) = body.get_mut("tools") {
        uppercase_schema_types(tools);
    }
}

fn uppercase_schema_types(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(schema_type)) = map.get_mut("type")
                && matches!(
                    schema_type.as_str(),
                    "string" | "number" | "integer" | "boolean" | "array" | "object"
                )
            {
                schema_type.make_ascii_uppercase();
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
    if cfg!(windows) {
        env::var_os("APPDATA").map(|appdata| {
            PathBuf::from(appdata).join("gcloud/application_default_credentials.json")
        })
    } else {
        env::var_os("HOME").map(|home| {
            PathBuf::from(home).join(".config/gcloud/application_default_credentials.json")
        })
    }
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
    let response = client.send_async(request).await.map_err(|error| AgentError::Config {
        message: format!(
            "no Application Default Credentials found and the GCP metadata service is unavailable: {error}"
        ),
    })?;
    token_response(response).await
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

    #[test_case(Some("environment"), Some("gcloud"), Some("configured"), Some("quota"), Some("environment") ; "google cloud project")]
    #[test_case(None, Some("gcloud"), Some("configured"), Some("quota"), Some("gcloud") ; "gcloud project")]
    #[test_case(None, None, Some("configured"), Some("quota"), Some("configured") ; "configured project")]
    #[test_case(None, None, None, Some("quota"), Some("quota") ; "quota project")]
    #[test_case(None, None, Some(""), Some("quota"), Some("quota") ; "empty configured project")]
    #[test_case(None, None, None, None, None ; "missing project")]
    fn project_resolution_precedence(
        google_cloud_project: Option<&str>,
        gcloud_project: Option<&str>,
        configured_project: Option<&str>,
        quota_project: Option<&str>,
        expected: Option<&str>,
    ) {
        assert_eq!(
            resolve_project(
                google_cloud_project.map(str::to_owned),
                gcloud_project.map(str::to_owned),
                configured_project.map(str::to_owned),
                quota_project,
            )
            .as_deref(),
            expected
        );
    }

    #[test_case(Some("us-central1"), Some("configured"), "us-central1" ; "environment")]
    #[test_case(None, Some("configured"), "configured" ; "configured")]
    #[test_case(None, Some(""), DEFAULT_LOCATION ; "empty configured")]
    #[test_case(None, None, DEFAULT_LOCATION ; "default")]
    fn location_resolution_precedence(
        google_cloud_location: Option<&str>,
        configured_location: Option<&str>,
        expected: &str,
    ) {
        assert_eq!(
            resolve_location(
                google_cloud_location.map(str::to_owned),
                configured_location.map(str::to_owned),
            ),
            expected
        );
    }

    #[test]
    fn normalizing_tool_schemas_preserves_contents() {
        let mut body = serde_json::json!({
            "contents": [{
                "role": "user",
                "parts": [
                    {"text": "{\"type\": \"file\"}"},
                    {"functionCall": {"name": "read", "args": {"type": "file"}}},
                    {"functionResponse": {"name": "read", "response": {"type": "error"}}}
                ]
            }],
            "tools": [{"functionDeclarations": [{
                "name": "read",
                "parameters": {
                    "type": "object",
                    "properties": {"path": {"type": "string", "default": {"type": "file"}}}
                }
            }]}]
        });
        normalize_tool_schemas(&mut body);
        assert_eq!(
            body["contents"][0]["parts"][0]["text"],
            "{\"type\": \"file\"}"
        );
        assert_eq!(
            body["contents"][0]["parts"][1]["functionCall"]["args"]["type"],
            "file"
        );
        assert_eq!(
            body["contents"][0]["parts"][2]["functionResponse"]["response"]["type"],
            "error"
        );
        assert_eq!(
            body["tools"][0]["functionDeclarations"][0]["parameters"]["type"],
            "OBJECT"
        );
        assert_eq!(
            body["tools"][0]["functionDeclarations"][0]["parameters"]["properties"]["path"]["type"],
            "STRING"
        );
        assert_eq!(
            body["tools"][0]["functionDeclarations"][0]["parameters"]["properties"]["path"]["default"]
                ["type"],
            "file"
        );
    }

    #[test]
    fn authorized_user_adc_preserves_quota_project() {
        const ADC: &[u8] = br#"{
            "type": "authorized_user",
            "client_id": "client",
            "client_secret": "secret",
            "refresh_token": "refresh",
            "quota_project_id": "quota-project"
        }"#;
        let source = AdcTokenProvider::source_from_json(ADC).unwrap();
        let AdcSource::AuthorizedUser {
            user,
            quota_project,
        } = source
        else {
            panic!("expected authorized user credentials");
        };
        assert_eq!(user.client_id, "client");
        assert_eq!(user.token_uri, OAUTH_TOKEN_URL);
        assert_eq!(quota_project.as_deref(), Some("quota-project"));
    }

    #[test_case("service_account" ; "service account")]
    #[test_case("external_account" ; "external account")]
    fn unsupported_adc_type_is_an_error(credential_type: &str) {
        let adc = format!(r#"{{"type":"{credential_type}"}}"#);
        let Err(error) = AdcTokenProvider::source_from_json(adc.as_bytes()) else {
            panic!("unsupported ADC type unexpectedly succeeded");
        };
        assert!(matches!(error, AgentError::Config { .. }));
        assert!(error.to_string().contains(credential_type));
    }

    #[test]
    fn cached_token_respects_expiry_and_forced_refresh() {
        const TOKEN: &str = "token";
        let provider = AdcTokenProvider {
            source: AdcSource::Metadata,
            cached: Mutex::new(Some(CachedToken {
                value: TOKEN.into(),
                expires_at: Instant::now() + REFRESH_MARGIN + Duration::from_secs(1),
            })),
        };
        let now = Instant::now();
        assert_eq!(provider.cached_token(false, now).as_deref(), Some(TOKEN));
        assert_eq!(provider.cached_token(true, now), None);
        *provider.cached.lock().unwrap() = Some(CachedToken {
            value: TOKEN.into(),
            expires_at: now + REFRESH_MARGIN,
        });
        assert_eq!(provider.cached_token(false, now), None);
    }

    #[test]
    fn explicit_missing_adc_path_is_an_error() {
        const PATH: &str = "/does/not/exist";
        unsafe { env::set_var("GOOGLE_APPLICATION_CREDENTIALS", PATH) };
        let result = AdcTokenProvider::from_environment();
        unsafe { env::remove_var("GOOGLE_APPLICATION_CREDENTIALS") };
        let Err(error) = result else {
            panic!("missing ADC path unexpectedly succeeded");
        };
        assert!(matches!(error, AgentError::Config { .. }));
        assert!(error.to_string().contains(PATH));
    }
}
