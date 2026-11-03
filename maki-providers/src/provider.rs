use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use flume::Sender;
use serde_json::Value;
use strum::{Display, EnumIter, EnumString};
use tracing::{debug, warn};

use maki_config::ModelPolicy;
use maki_storage::id::SessionRef;

use crate::model::{Model, ModelFamily, ModelInfo};
use crate::providers::KeyRotation;
use crate::providers::Timeouts;
use crate::providers::aperture::Aperture;
use crate::providers::catalog::{
    OPENCODE_FAMILY_SLUGS, available_if_warm, catalog_providers, catalog_providers_if_available,
};
use crate::providers::dynamic;
use crate::providers::google::Google;
use crate::providers::local::{LLAMACPP, LocalEndpoint, OLLAMA};
use crate::providers::opencode::Opencode;
use crate::providers::vertex::Vertex;
use crate::{
    AgentError, ConversationEnd, Invocation, Message, ProviderEvent, ProviderUsage, RequestOptions,
    StreamResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, EnumIter)]
#[strum(serialize_all = "kebab-case")]
pub enum ProviderKind {
    /// Only the protocol base that Aperture routes and dynamic and custom
    /// providers name. The `anthropic` provider is registered from Lua.
    Anthropic,
    /// Only the protocol base that dynamic and custom providers name. The
    /// `openai` provider is registered from Lua.
    #[strum(serialize = "openai")]
    OpenAi,
    Google,
    Vertex,
    Ollama,
    LlamaCpp,
    #[strum(serialize = "opencode")]
    Opencode,
    Aperture,
}

impl ProviderKind {
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Anthropic => "Anthropic",
            Self::OpenAi => "OpenAI",
            Self::Google => "Google",
            Self::Vertex => "Google Vertex AI",
            Self::Ollama => "Ollama",
            Self::LlamaCpp => "LlamaCpp",
            Self::Opencode => "Opencode Zen",
            Self::Aperture => "Aperture",
        }
    }

    pub const fn api_key_env(self) -> &'static str {
        match self {
            Self::Anthropic => "ANTHROPIC_API_KEY",
            Self::OpenAi => "OPENAI_API_KEY",
            Self::Google => "GEMINI_API_KEY",
            Self::Vertex => "",
            Self::Ollama => "OLLAMA_API_KEY",
            Self::LlamaCpp => "LLAMA_CPP_API_KEY",
            Self::Opencode => "OPENCODE_API_KEY",
            Self::Aperture => "",
        }
    }

    pub const fn base_url(self) -> &'static str {
        match self {
            Self::Anthropic => "https://api.anthropic.com/v1/messages",
            Self::OpenAi => "https://api.openai.com/v1",
            Self::Google => "https://generativelanguage.googleapis.com/v1beta",
            Self::Vertex => "https://aiplatform.googleapis.com/v1",
            Self::Ollama => "http://localhost:11434/v1",
            Self::LlamaCpp => "http://localhost:8080/v1",
            Self::Opencode => "https://opencode.ai/zen/v1",
            Self::Aperture => "Aperture gateway (set APERTURE_HOST)",
        }
    }

    pub const fn features(self) -> Option<&'static str> {
        match self {
            Self::Anthropic => {
                Some("Prompt caching, thinking mode (adaptive/budgeted), advanced tool use")
            }
            Self::Google => Some("Native Gemini API with thinking support"),
            Self::Vertex => {
                Some("Native Vertex AI Gemini API using Application Default Credentials")
            }
            Self::Ollama => {
                Some("Local or remote inference via OLLAMA_HOST, cloud fallback via OLLAMA_API_KEY")
            }
            Self::LlamaCpp => Some(
                "Local or remote inference via LLAMA_CPP_HOST, set optional key via LLAMA_CPP_API_KEY",
            ),
            Self::Opencode => Some(
                "Dynamically discovered models via [models.dev](https://models.dev/) + all the models provided by Opencode Zen API",
            ),
            Self::Aperture => Some(
                "Tailscale Aperture LLM gateway; set APERTURE_HOST or configure in providers.toml",
            ),
            _ => None,
        }
    }

    pub const fn family(self) -> ModelFamily {
        match self {
            Self::Anthropic => ModelFamily::Claude,
            Self::OpenAi => ModelFamily::Gpt,
            Self::Google | Self::Vertex => ModelFamily::Gemini,
            Self::Ollama => ModelFamily::Generic,
            Self::LlamaCpp => ModelFamily::Generic,
            Self::Opencode => ModelFamily::Generic,
            Self::Aperture => ModelFamily::Generic,
        }
    }

    /// `None` when we honestly don't know the output window: llama.cpp
    /// serves whatever model the user loaded. Unknown means "don't limit", never
    /// "assume small"; a `0` sentinel here once silently capped llama.cpp
    /// thinking budgets at the floor.
    pub const fn fallback_max_output(self) -> Option<u32> {
        match self {
            Self::Anthropic => Some(128_000),
            Self::OpenAi => Some(100_000),
            Self::Google | Self::Vertex => Some(65_536),
            Self::Ollama => Some(16_384),
            Self::LlamaCpp => None,
            Self::Opencode => Some(128_000),
            Self::Aperture => Some(16_384),
        }
    }

    pub const fn fallback_context_window(self) -> u32 {
        match self {
            Self::Anthropic => 200_000,
            Self::OpenAi => 200_000,
            Self::Google | Self::Vertex => 1_000_000,
            Self::Ollama => 128_000,
            Self::LlamaCpp => 128_000,
            Self::Opencode => 256_000,
            Self::Aperture => 128_000,
        }
    }

    pub(crate) fn create(self, timeouts: Timeouts) -> Result<Box<dyn NativeProvider>, AgentError> {
        match self {
            Self::Anthropic | Self::OpenAi => Err(AgentError::Config {
                message: format!("provider '{self}' is registered by the providers plugin"),
            }),
            Self::Google => Ok(Box::new(Google::new(timeouts)?)),
            Self::Vertex => Ok(Box::new(Vertex::new(timeouts)?)),
            Self::Ollama => Ok(Box::new(LocalEndpoint::new(&OLLAMA, timeouts)?)),
            Self::LlamaCpp => Ok(Box::new(LocalEndpoint::new(&LLAMACPP, timeouts)?)),
            Self::Opencode => Ok(Box::new(Opencode::new(timeouts)?)),
            Self::Aperture => Ok(Box::new(Aperture::new(timeouts)?)),
        }
    }
}

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Provider: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
        invocation: &'a Invocation,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>>;

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Fetch provider-side usage quota (remaining percentage / reset times).
    /// `Ok(None)` means the provider does not expose a programmatic usage endpoint.
    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        Box::pin(async { Ok(None) })
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }

    /// How many keys a retry walk can try before it is back where it started.
    fn key_count(&self) -> usize {
        1
    }

    /// Switch to the next key. `false` means there was nothing to switch to.
    fn rotate_key(&self) -> BoxFuture<'_, bool> {
        Box::pin(async { false })
    }

    /// The host will never send another request for `conversation_id`, and
    /// every stream for it has been dropped. Must be idempotent and accept
    /// ids the provider never saw.
    fn end_conversation(
        &self,
        _conversation_id: &str,
        _reason: ConversationEnd,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

pub(crate) trait NativeProvider: Provider {
    fn adjust_model(&self, _model: &mut Model) {}

    /// The keys this provider rotates through, and where the current one lives.
    /// `None` is one fixed credential that never changes. Both the count and
    /// the swap come off it, so they can never describe different pools.
    fn keys(&self) -> Option<KeyRotation<'_>> {
        None
    }
}

struct Native(Box<dyn NativeProvider>);

impl Provider for Native {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
        invocation: &'a Invocation,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        self.0.stream_message(
            model, messages, system, tools, event_tx, opts, session_id, invocation,
        )
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        self.0.list_models()
    }

    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        self.0.fetch_usage()
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        self.0.refresh_auth()
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        self.0.reload_auth()
    }

    fn key_count(&self) -> usize {
        self.0.keys().map_or(1, |keys| keys.key_count())
    }

    fn rotate_key(&self) -> BoxFuture<'_, bool> {
        let rotated = self.0.keys().is_some_and(|keys| keys.rotate());
        Box::pin(async move { rotated })
    }
}

pub(crate) fn provider_for_slug(
    slug: &str,
    timeouts: Timeouts,
) -> Result<Box<dyn NativeProvider>, AgentError> {
    if let Ok(kind) = ProviderKind::from_str(slug) {
        return kind.create(timeouts);
    }
    if dynamic::display_name(slug).is_some() {
        return dynamic::create(slug, timeouts);
    }
    if crate::providers::custom::base_kind(slug).is_some() {
        return crate::providers::custom::create(slug, timeouts);
    }
    if let Some(catalog) = crate::providers::catalog::try_create(slug, timeouts) {
        return catalog;
    }
    Err(AgentError::Config {
        message: format!("unknown provider '{slug}'"),
    })
}

pub fn provider_available(slug: &str) -> bool {
    provider_for_slug(slug, Timeouts::default()).is_ok()
}

/// Non-blocking variant of [`provider_available`] for offline model discovery:
/// catalog-backed slugs consult only the already-warm catalog, so a cold cache
/// reports them unavailable instead of blocking on a network fetch.
fn provider_available_offline(slug: &str) -> bool {
    if ProviderKind::from_str(slug).is_ok()
        || dynamic::display_name(slug).is_some()
        || crate::providers::custom::base_kind(slug).is_some()
    {
        return provider_available(slug);
    }
    available_if_warm(slug)
}

pub fn from_model(model: &mut Model, timeouts: Timeouts) -> Result<Box<dyn Provider>, AgentError> {
    let provider = provider_for_slug(&model.provider, timeouts)?;
    provider.adjust_model(model);
    debug!(provider = %model.provider, model = %model.id, "provider created");
    Ok(Box::new(Native(provider)))
}

pub fn from_model_fallback(model: &mut Model, timeouts: Timeouts) -> Box<dyn Provider> {
    match from_model(model, timeouts) {
        Ok(provider) => provider,
        Err(e) => {
            warn!(error = %e, "provider creation failed, using unconfigured provider");
            Box::new(UnconfiguredProvider)
        }
    }
}

/// A provider whose every request asks the user to log in.
pub fn unconfigured() -> Arc<dyn Provider> {
    Arc::new(UnconfiguredProvider)
}

struct UnconfiguredProvider;

const NOT_CONFIGURED: &str = "no provider configured — run /login or `makima auth login`";

impl Provider for UnconfiguredProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        _messages: &'a [Message],
        _system: &'a str,
        _tools: &'a Value,
        _event_tx: &'a Sender<ProviderEvent>,
        _opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async {
            Err(AgentError::Config {
                message: NOT_CONFIGURED.to_string(),
            })
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async {
            Err(AgentError::Config {
                message: NOT_CONFIGURED.to_string(),
            })
        })
    }
}

pub async fn from_model_async(
    model: &mut Model,
    timeouts: Timeouts,
) -> Result<Box<dyn Provider>, AgentError> {
    let slug = Arc::clone(&model.provider);
    let id = model.id.clone();
    let provider = smol::unblock(move || provider_for_slug(&slug, timeouts)).await?;
    provider.adjust_model(model);
    debug!(provider = %model.provider, model = %id, "provider created");
    Ok(Box::new(Native(provider)))
}

pub struct ModelBatch {
    /// The one provider this batch lists, if it lists one.
    pub provider: Option<Arc<str>>,
    pub models: Vec<String>,
    pub warnings: Vec<String>,
}

/// Offline version of model discovery: returns specs from static tables
/// and configured dynamic providers. See [`fetch_all_models`] for live lookups.
/// Never blocks on catalog download; catalog-backed providers appear only once
/// the catalog has warmed in the background.
pub fn available_model_specs(policy: &ModelPolicy) -> Vec<String> {
    let mut specs: Vec<String> = crate::manifest::ManifestRegistry::builtins()
        .iter()
        .filter(|m| provider_available_offline(m.slug))
        .flat_map(|m| {
            m.models
                .iter()
                .flat_map(|entry| entry.prefixes.iter())
                .map(move |p| format!("{}/{}", m.slug, p))
        })
        .collect();
    for slug in dynamic::discovered_slugs() {
        specs.extend(dynamic::dynamic_model_specs_for(slug));
    }
    for spec in crate::providers::custom::declared_model_specs() {
        if !specs.contains(&spec) {
            specs.push(spec);
        }
    }
    if let Some(catalog) = catalog_providers_if_available() {
        for cat in catalog {
            if ProviderKind::from_str(&cat.slug).is_ok()
                || dynamic::base_for_slug(&cat.slug).is_some()
                || crate::providers::custom::base_kind(&cat.slug).is_some()
                || OPENCODE_FAMILY_SLUGS.contains(&cat.slug.as_str())
            {
                continue;
            }
            if !provider_available(&cat.slug) {
                continue;
            }
            for model_id in cat.models.keys() {
                let spec = format!("{}/{}", cat.slug, model_id);
                if !specs.contains(&spec) {
                    specs.push(spec);
                }
            }
        }
    }
    specs.retain(|spec| {
        policy.allows(spec)
            && spec
                .split_once('/')
                .is_some_and(|(slug, _)| provider_available_offline(slug))
    });
    specs
}

pub async fn fetch_all_models(policy: &ModelPolicy, mut on_ready: impl FnMut(ModelBatch)) {
    let (tx, rx) = flume::unbounded();
    let timeouts = Timeouts::default();

    for manifest in crate::manifest::ManifestRegistry::builtins() {
        let slug = manifest.slug;
        let Ok(provider) = smol::unblock(move || provider_for_slug(slug, timeouts)).await else {
            warn!(provider = slug, "failed to create provider, skipping");
            continue;
        };
        let display_name = manifest.display_name;
        let tx = tx.clone();
        smol::spawn(async move {
            let batch = match provider.list_models().await {
                Ok(models) => {
                    let mut specs: Vec<String> =
                        models.iter().map(|m| format!("{slug}/{}", m.id)).collect();
                    crate::model_registry::set_known_models(slug, models);
                    for entry in manifest.models {
                        for prefix in entry.prefixes {
                            let spec = format!("{slug}/{prefix}");
                            if !specs.contains(&spec) {
                                specs.push(spec);
                            }
                        }
                    }
                    ModelBatch {
                        provider: Some(Arc::from(slug)),
                        models: specs,
                        warnings: Vec::new(),
                    }
                }
                Err(e) => {
                    warn!(provider = slug, error = %e, "failed to list models, using static fallback");
                    let fallback: Vec<String> = manifest
                        .models
                        .iter()
                        .flat_map(|entry| entry.prefixes.iter())
                        .map(|p| format!("{slug}/{p}"))
                        .collect();
                    ModelBatch {
                        provider: Some(Arc::from(slug)),
                        models: fallback,
                        warnings: vec![format!(
                            "{display_name}: {e} (using static fallback)"
                        )],
                    }
                }
            };
            let _ = tx.send_async(batch).await;
        })
        .detach();
    }

    for listed in dynamic::discovered_slugs() {
        let tx = tx.clone();
        let slug = listed.to_string();
        smol::spawn(async move {
            let static_fallback = |reason: String| {
                warn!(
                    slug,
                    error = reason,
                    "dynamic model listing failed, using static fallback"
                );
                ModelBatch {
                    provider: Some(Arc::from(listed)),
                    models: dynamic::dynamic_model_specs_for(&slug),
                    warnings: vec![format!("{slug}: {reason} (using static fallback)")],
                }
            };
            let batch = match dynamic::create(&slug, timeouts) {
                Ok(provider) => match provider.list_models().await {
                    Ok(models) => ModelBatch {
                        provider: Some(Arc::from(listed)),
                        models: models.iter().map(|m| format!("{slug}/{}", m.id)).collect(),
                        warnings: Vec::new(),
                    },
                    Err(e) => static_fallback(e.to_string()),
                },
                Err(e) => static_fallback(e.to_string()),
            };
            let _ = tx.send_async(batch).await;
        })
        .detach();
    }

    let tx_catalog = tx.clone();
    smol::spawn(async move {
        let catalog = smol::unblock(catalog_providers).await;
        for cat in catalog {
            if ProviderKind::from_str(&cat.slug).is_ok()
                || dynamic::base_for_slug(&cat.slug).is_some()
                || OPENCODE_FAMILY_SLUGS.contains(&cat.slug.as_str())
            {
                continue;
            }
            if !provider_available(&cat.slug) {
                continue;
            }
            let slug = cat.slug;
            let models: Vec<String> = cat.models.keys().map(|id| format!("{slug}/{id}")).collect();
            let _ = tx_catalog
                .send_async(ModelBatch {
                    provider: None,
                    models,
                    warnings: Vec::new(),
                })
                .await;
        }
    })
    .detach();

    let custom_timeouts = timeouts;
    let tx_custom = tx.clone();
    smol::spawn(async move {
        let declared = crate::providers::custom::declared_model_specs();
        if !declared.is_empty() {
            let _ = tx_custom
                .send_async(ModelBatch {
                    provider: None,
                    models: declared,
                    warnings: Vec::new(),
                })
                .await;
        }
        let custom_specs =
            smol::unblock(move || crate::providers::custom::discover_models(custom_timeouts)).await;
        if !custom_specs.is_empty() {
            let _ = tx_custom
                .send_async(ModelBatch {
                    provider: None,
                    models: custom_specs,
                    warnings: Vec::new(),
                })
                .await;
        }
    })
    .detach();

    drop(tx);

    while let Ok(mut batch) = rx.recv_async().await {
        batch.models.retain(|spec| policy.allows(spec));
        on_ready(batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(allowed: &[&str], excluded: &[&str]) -> ModelPolicy {
        ModelPolicy::new(
            &allowed
                .iter()
                .map(|pattern| (*pattern).into())
                .collect::<Vec<_>>(),
            &excluded
                .iter()
                .map(|pattern| (*pattern).into())
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn available_specs_apply_model_policy() {
        let tmp = tempfile::tempdir().unwrap();
        maki_storage::paths::init_at(tmp.path().to_path_buf()).unwrap();
        unsafe { std::env::set_var("GEMINI_API_KEY", "test-model-policy") };
        let policy = policy(&["google/*"], &["*/gemini-2.5-flash"]);

        let specs = available_model_specs(&policy);
        unsafe { std::env::remove_var("GEMINI_API_KEY") };

        assert!(!specs.is_empty());
        assert!(specs.iter().all(|spec| spec.starts_with("google/")));
        assert!(!specs.iter().any(|spec| spec == "google/gemini-2.5-flash"));
    }

    #[test]
    fn provider_for_slug_unknown_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let _catalog = crate::providers::catalog::warm_empty_catalog_for_tests(
            maki_storage::StateDir::from_path(tmp.path().to_path_buf()),
        );
        let result = provider_for_slug("nonexistent-provider-xyz", Timeouts::default());
        match result {
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("unknown provider"),
                    "expected 'unknown provider' message, got: {msg}"
                );
            }
            Ok(_) => panic!("expected error for unknown provider"),
        }
    }
}
