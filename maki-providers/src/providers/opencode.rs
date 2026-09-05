use crate::types::ThinkingConfigExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flume::Sender;
use isahc::{HttpClient, Request};
use maki_storage::id::SessionRef;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;

use crate::model::{Model, ModelInfo};
use crate::provider::{BoxFuture, Provider};
use crate::providers::anthropic::shared;
use crate::providers::catalog::{
    CatalogMeta, EndpointType, OPENCODE_FAMILY_SLUGS, config_error, init_shared_catalog_if_needed,
};
use crate::providers::http_client;
use crate::providers::openai_compat::{OpenAiCompatConfig, OpenAiCompatProvider};
use crate::types::{ProviderUsage, UsageLimit, UsageWindow};
use crate::{AgentError, Message, ProviderEvent, RequestOptions, StreamResponse, dialect};

use super::{ResolvedAuth, user_agent, with_prefix};

const MESSAGES_PATH: &str = "/messages";
const SESSION_HEADER: &str = "x-opencode-session";
pub(crate) const USAGE_PATH: &str = "/usage";
const EMPTY_USAGE_ERROR: &str =
    "Opencode Go usage response contained no usage lanes; the endpoint schema likely changed";

static CATALOG_CHAT_CONFIG: OpenAiCompatConfig = OpenAiCompatConfig {
    slug: "opencode",
    api_key_env: "",
    base_url: "",
    max_tokens_field: "max_tokens",
    include_stream_usage: true,
    provider_name: "Opencode (Catalog)",
};

pub struct Opencode {
    client: HttpClient,
    chat_compat: OpenAiCompatProvider,
    auth: Option<Arc<Mutex<ResolvedAuth>>>,
    system_prefix: Option<String>,
    stream_timeout: Duration,
}

impl Opencode {
    pub fn new(timeouts: super::Timeouts) -> Result<Self, AgentError> {
        Ok(Self {
            client: http_client(timeouts),
            chat_compat: OpenAiCompatProvider::new(&CATALOG_CHAT_CONFIG, timeouts),
            auth: None,
            system_prefix: None,
            stream_timeout: timeouts.stream,
        })
    }

    pub(crate) fn with_auth(auth: Arc<Mutex<ResolvedAuth>>, timeouts: super::Timeouts) -> Self {
        Self {
            client: http_client(timeouts),
            chat_compat: OpenAiCompatProvider::new(&CATALOG_CHAT_CONFIG, timeouts),
            auth: Some(auth),
            system_prefix: None,
            stream_timeout: timeouts.stream,
        }
    }

    pub(crate) fn with_system_prefix(mut self, prefix: Option<String>) -> Self {
        self.system_prefix = prefix;
        self
    }

    async fn do_list_models(&self) -> Result<Vec<ModelInfo>, AgentError> {
        Ok(
            smol::unblock(move || init_shared_catalog_if_needed().lock().unwrap().all_models())
                .await,
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_catalog_chat_completions(
        &self,
        model: &Model,
        messages: &[Message],
        system: &str,
        tools: &Value,
        event_tx: &Sender<ProviderEvent>,
        auth: &ResolvedAuth,
        opts: &RequestOptions,
        provider_slug: &str,
        session_id: Option<&SessionRef>,
    ) -> Result<StreamResponse, AgentError> {
        let mut body = self.chat_compat.build_body(model, messages, system, tools);
        opts.thinking
            .apply_reasoning_effort(&mut body, &dialect::PREFER_HIGH, model);
        let extra_headers: Vec<_> = session_header(provider_slug, session_id)
            .into_iter()
            .collect();
        self.chat_compat
            .do_stream(model, &extra_headers, &body, event_tx, auth)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_catalog_messages(
        &self,
        model: &Model,
        messages: &[Message],
        system: &str,
        tools: &Value,
        event_tx: &Sender<ProviderEvent>,
        auth: &ResolvedAuth,
        opts: &RequestOptions,
        provider_slug: &str,
        session_id: Option<&SessionRef>,
    ) -> Result<StreamResponse, AgentError> {
        let system_blocks = vec![shared::SystemBlock {
            r#type: "text",
            text: system,
            cache_control: Some(shared::EPHEMERAL),
        }];
        let mut body = shared::build_request_body_with_system(
            model,
            messages,
            &system_blocks,
            tools,
            opts.thinking,
        );
        body["model"] = json!(model.id);
        body["stream"] = json!(true);
        let json_body = serde_json::to_vec(&body)?;
        let mut request = Request::builder()
            .method("POST")
            .uri(format!(
                "{}{}",
                auth.base_url.as_deref().unwrap_or(""),
                MESSAGES_PATH
            ))
            .header("user-agent", user_agent())
            .header("content-type", "application/json")
            .header("anthropic-version", "2023-06-01");
        if let Some((key, value)) = session_header(provider_slug, session_id) {
            request = request.header(key, value);
        }
        let request = auth.configure_request(request).body(json_body)?;

        debug!(model = %model.id, "sending Anthropic-format request via catalog");

        let response = self.client.send_async(request).await?;
        let status = response.status().as_u16();

        if status == 200 {
            crate::providers::anthropic::parse_sse(response, event_tx, self.stream_timeout).await
        } else {
            Err(AgentError::from_response(response).await)
        }
    }

    async fn lookup(
        &self,
        sub_provider: &str,
        actual_id: &str,
    ) -> Result<(CatalogMeta, EndpointType, ResolvedAuth), AgentError> {
        let sub_provider = sub_provider.to_string();
        let actual_id = actual_id.to_string();
        let auth_override = self.auth.clone();
        smol::unblock(move || {
            let guard = init_shared_catalog_if_needed().lock().unwrap();
            let (meta, provider_data) = guard.lookup(&sub_provider, &actual_id)?;
            let state_dir = &guard.state_dir;
            let auth = provider_data
                .resolve_auth_with_override(auth_override.as_ref(), state_dir)
                .ok_or_else(|| {
                    config_error(format!(
                        "authentication required for provider '{sub_provider}', run `maki auth login {sub_provider}`"
                    ))
                })?;
            Ok((meta.clone(), provider_data.api_format, auth))
        })
        .await
    }
}

impl Provider for Opencode {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let model_for_stream = model.clone();

            let model_id = &model_for_stream.id;
            let (sub_provider, actual_id) =
                model_id.split_once('/').unwrap_or(("opencode", model_id));

            let (meta, api_format, auth) = self.lookup(sub_provider, actual_id).await?;

            let mut buf = String::new();
            let system = with_prefix(&self.system_prefix, system, &mut buf);

            let model = Model {
                id: actual_id.to_string(),
                max_output_tokens: Some(meta.output),
                context_window: meta.context,
                ..model_for_stream
            };

            match api_format {
                EndpointType::ChatCompletions => {
                    self.handle_catalog_chat_completions(
                        &model,
                        messages,
                        system,
                        tools,
                        event_tx,
                        &auth,
                        &opts,
                        sub_provider,
                        session_id,
                    )
                    .await
                }
                EndpointType::Messages => {
                    self.handle_catalog_messages(
                        &model,
                        messages,
                        system,
                        tools,
                        event_tx,
                        &auth,
                        &opts,
                        sub_provider,
                        session_id,
                    )
                    .await
                }
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(self.do_list_models())
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }
}

pub(super) fn session_header<'a>(
    provider_slug: &str,
    session_id: Option<&'a SessionRef>,
) -> Option<(&'static str, &'a str)> {
    OPENCODE_FAMILY_SLUGS
        .contains(&provider_slug)
        .then_some(session_id)
        .flatten()
        .map(|id| (SESSION_HEADER, id.as_str()))
}

#[derive(Deserialize)]
struct UsageResponse {
    usage: UsageLanes,
}

#[derive(Deserialize)]
struct UsageLanes {
    #[serde(default)]
    rolling: Option<UsageLane>,
    #[serde(default)]
    weekly: Option<UsageLane>,
    #[serde(default)]
    monthly: Option<UsageLane>,
}

#[derive(Deserialize)]
struct UsageLane {
    #[serde(default)]
    percent: Option<u32>,
    #[serde(rename = "resetsAt")]
    resets_at: Option<String>,
}

/// `GET {go-base-url}/usage` reports dollar-value quota lanes as percentages.
pub(crate) fn parse_usage(response: &str) -> Result<ProviderUsage, AgentError> {
    let resp: UsageResponse = serde_json::from_str(response)?;
    let lanes = resp.usage;
    let mut limits = Vec::with_capacity(3);
    for (kind, lane) in [
        (UsageWindow::Hours(5), lanes.rolling),
        (UsageWindow::Weekly { model: None }, lanes.weekly),
        (UsageWindow::Monthly, lanes.monthly),
    ] {
        let Some(lane) = lane else {
            continue;
        };
        limits.push(UsageLimit {
            kind,
            percentage: lane.percent,
            reset_at: lane.resets_at.as_deref().and_then(parse_reset),
            detail: None,
        });
    }
    if limits.is_empty() {
        return Err(AgentError::Config {
            message: EMPTY_USAGE_ERROR.into(),
        });
    }
    Ok(ProviderUsage { plan: None, limits })
}

/// `/usage` timestamps are RFC 3339; the UI expects epoch milliseconds.
fn parse_reset(rfc3339: &str) -> Option<u64> {
    let ts: jiff::Timestamp = rfc3339.parse().ok()?;
    u64::try_from(ts.as_millisecond()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_USAGE: &str = r#"{"usage":{"rolling":{"status":"ok","percent":1,"resetsAt":"2026-08-29T06:01:16.964Z"},"weekly":{"status":"ok","percent":4,"resetsAt":"2026-08-31T00:00:00.964Z"},"monthly":{"status":"ok","percent":2,"resetsAt":"2026-09-18T15:23:06.964Z"}}}"#;
    const ROLLING_RESET_MS: u64 = 1_787_983_276_964;
    const WEEKLY_RESET_MS: u64 = 1_788_134_400_964;
    const MONTHLY_RESET_MS: u64 = 1_789_744_986_964;

    #[test]
    fn session_header_is_limited_to_opencode_providers() {
        let session = SessionRef::generate();
        assert_eq!(
            session_header("opencode", Some(&session)),
            Some((SESSION_HEADER, session.as_str()))
        );
        assert_eq!(
            session_header("opencode-go", Some(&session)),
            Some((SESSION_HEADER, session.as_str()))
        );
        assert_eq!(session_header("nvidia", Some(&session)), None);
        assert_eq!(session_header("opencode", None), None);
    }

    #[test]
    fn parse_usage_maps_lanes_to_windows() {
        let usage = parse_usage(SAMPLE_USAGE).unwrap();
        assert_eq!(usage.plan, None);
        assert_eq!(usage.limits.len(), 3);
        assert_eq!(usage.limits[0].kind, UsageWindow::Hours(5));
        assert_eq!(usage.limits[0].percentage, Some(1));
        assert_eq!(usage.limits[0].reset_at, Some(ROLLING_RESET_MS));
        assert_eq!(usage.limits[1].kind, UsageWindow::Weekly { model: None });
        assert_eq!(usage.limits[1].percentage, Some(4));
        assert_eq!(usage.limits[1].reset_at, Some(WEEKLY_RESET_MS));
        assert_eq!(usage.limits[2].kind, UsageWindow::Monthly);
        assert_eq!(usage.limits[2].percentage, Some(2));
        assert_eq!(usage.limits[2].reset_at, Some(MONTHLY_RESET_MS));
    }

    #[test]
    fn parse_usage_rejects_empty_lanes() {
        let parsed = parse_usage(r#"{"usage":{}}"#);
        assert!(
            matches!(parsed, Err(AgentError::Config { message }) if message == EMPTY_USAGE_ERROR)
        );
    }
}
