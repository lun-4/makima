use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use flume::Sender;
use maki_config::ProviderConfig;
use serde_json::Value;

use maki_storage::id::SessionRef;

use crate::model::Model;
use crate::retry::RetryPolicy;
use crate::{
    AgentError, ConversationEnd, Invocation, Message, ProviderEvent, ProviderUsage,
    RequestDelivery, RequestOptions, StreamResponse,
};

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub connect: Duration,
    pub low_speed: Duration,
    pub stream: Duration,
    pub retry: RetryPolicy,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            low_speed: Duration::from_secs(30),
            stream: Duration::from_secs(300),
            retry: RetryPolicy::default(),
        }
    }
}

impl From<&ProviderConfig> for Timeouts {
    fn from(config: &ProviderConfig) -> Self {
        Self {
            connect: config.connect_timeout,
            low_speed: config.low_speed_timeout,
            stream: config.stream_timeout,
            retry: RetryPolicy::from(config),
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
        delivery: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>>;

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
        _delivery: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
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
