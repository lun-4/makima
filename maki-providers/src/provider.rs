use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use flume::Sender;
use serde_json::Value;
use strum::{Display, EnumIter, EnumString};

use maki_storage::id::SessionRef;

use crate::model::{Model, ModelFamily, ModelInfo};
use crate::{
    AgentError, ConversationEnd, Invocation, Message, ProviderEvent, ProviderUsage, RequestOptions,
    StreamResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, EnumString, EnumIter)]
#[strum(serialize_all = "kebab-case")]
pub enum ProviderKind {
    /// Only the protocol base that dynamic and custom providers name. The
    /// `anthropic` provider is registered from Lua.
    Anthropic,
    /// Only the protocol base that dynamic and custom providers name. The
    /// `openai` provider is registered from Lua.
    #[strum(serialize = "openai")]
    OpenAi,
    /// Only the protocol base that dynamic and custom providers name. The
    /// `google` provider is registered from Lua.
    Google,
    /// Only the protocol base that dynamic providers name. The `ollama`
    /// provider is registered from Lua.
    Ollama,
    /// Only the protocol base that dynamic providers name. The `llama-cpp`
    /// provider is registered from Lua.
    LlamaCpp,
}

impl ProviderKind {
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Anthropic => "Anthropic",
            Self::OpenAi => "OpenAI",
            Self::Google => "Google",
            Self::Ollama => "Ollama",
            Self::LlamaCpp => "LlamaCpp",
        }
    }

    pub const fn api_key_env(self) -> &'static str {
        match self {
            Self::Anthropic => "ANTHROPIC_API_KEY",
            Self::OpenAi => "OPENAI_API_KEY",
            Self::Google => "GEMINI_API_KEY",
            Self::Ollama => "OLLAMA_API_KEY",
            Self::LlamaCpp => "LLAMA_CPP_API_KEY",
        }
    }

    pub const fn base_url(self) -> &'static str {
        match self {
            Self::Anthropic => "https://api.anthropic.com/v1/messages",
            Self::OpenAi => "https://api.openai.com/v1",
            Self::Google => "https://generativelanguage.googleapis.com/v1beta",
            Self::Ollama => "http://localhost:11434/v1",
            Self::LlamaCpp => "http://localhost:8080/v1",
        }
    }

    pub const fn features(self) -> Option<&'static str> {
        match self {
            Self::Anthropic => {
                Some("Prompt caching, thinking mode (adaptive/budgeted), advanced tool use")
            }
            Self::Google => Some("Native Gemini API with thinking support"),
            _ => None,
        }
    }

    pub const fn family(self) -> ModelFamily {
        match self {
            Self::Anthropic => ModelFamily::Claude,
            Self::OpenAi => ModelFamily::Gpt,
            Self::Google => ModelFamily::Gemini,
            Self::Ollama => ModelFamily::Generic,
            Self::LlamaCpp => ModelFamily::Generic,
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
            Self::Google => Some(65_536),
            Self::Ollama => Some(16_384),
            Self::LlamaCpp => None,
        }
    }

    pub const fn fallback_context_window(self) -> u32 {
        match self {
            Self::Anthropic => 200_000,
            Self::OpenAi => 200_000,
            Self::Google => 1_000_000,
            Self::Ollama => 128_000,
            Self::LlamaCpp => 128_000,
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

pub struct ModelBatch {
    /// The one provider this batch lists, if it lists one.
    pub provider: Option<Arc<str>>,
    pub models: Vec<String>,
    pub warnings: Vec<String>,
}
