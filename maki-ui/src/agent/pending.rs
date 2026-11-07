//! A provider that exists before it is prepared, so startup never waits on a
//! prepare: requests wait for it instead.

use std::sync::Arc;

use async_lock::OnceCell;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ConversationEnd, Invocation, Message, Model, ProviderEvent, ProviderUsage,
    RequestDelivery, RequestOptions, StreamResponse,
};
use maki_storage::id::SessionRef;
use serde_json::Value;

#[derive(Default)]
pub(crate) struct PendingProvider {
    ready: OnceCell<Result<Arc<dyn Provider>, Arc<str>>>,
}

impl PendingProvider {
    /// Settles the provider every waiting and later request uses. Only the
    /// first call counts.
    pub(crate) async fn fill(&self, result: Result<Arc<dyn Provider>, Arc<str>>) {
        let _ = self.ready.set(result).await;
    }

    async fn provider(&self) -> Result<&Arc<dyn Provider>, AgentError> {
        self.ready
            .wait()
            .await
            .as_ref()
            .map_err(|message| AgentError::Config {
                message: message.to_string(),
            })
    }
}

impl Provider for PendingProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a flume::Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
        invocation: &'a Invocation,
        delivery: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.provider()
                .await?
                .stream_message(
                    model, messages, system, tools, event_tx, opts, session_id, invocation,
                    delivery,
                )
                .await
        })
    }

    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        Box::pin(async { self.provider().await?.fetch_usage().await })
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { self.provider().await?.refresh_auth().await })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { self.provider().await?.reload_auth().await })
    }

    fn key_count(&self) -> usize {
        match self.ready.get() {
            Some(Ok(provider)) => provider.key_count(),
            _ => 1,
        }
    }

    fn rotate_key(&self) -> BoxFuture<'_, bool> {
        Box::pin(async {
            match self.provider().await {
                Ok(provider) => provider.rotate_key().await,
                Err(_) => false,
            }
        })
    }

    fn end_conversation(
        &self,
        conversation_id: &str,
        reason: ConversationEnd,
    ) -> BoxFuture<'_, ()> {
        let conversation_id = conversation_id.to_owned();
        Box::pin(async move {
            if let Ok(provider) = self.provider().await {
                provider.end_conversation(&conversation_id, reason).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAILURE: &str = "prepare failed";

    #[test]
    fn requests_wait_for_fill_then_fail_with_its_error() {
        let pending = Arc::new(PendingProvider::default());
        let waiting = smol::spawn({
            let pending = Arc::clone(&pending);
            async move { pending.fetch_usage().await }
        });
        smol::block_on(pending.fill(Err(FAILURE.into())));
        let error = smol::block_on(waiting).unwrap_err();
        assert!(matches!(error, AgentError::Config { message } if message == FAILURE));
    }
}
