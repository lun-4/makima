//! A provider that exists before it is prepared, so startup never waits on a
//! prepare: requests wait for it instead.

use std::any::Any;
use std::sync::Arc;

use async_lock::OnceCell;
use makima_agent::actor::PreparedModel;
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    AgentError, ConversationEnd, Invocation, Message, Model, ProviderEvent, ProviderUsage,
    RequestDelivery, RequestOptions, StreamResponse,
};
use makima_storage::id::SessionRef;
use serde_json::Value;

use super::TrackedProvider;

#[derive(Default)]
pub(crate) struct PendingProvider {
    ready: OnceCell<Result<PreparedModel, Arc<str>>>,
    #[cfg(test)]
    request_probe: Option<flume::Sender<()>>,
}

impl PendingProvider {
    pub(crate) fn from_provider(provider: &dyn Provider) -> Option<&Self> {
        let provider: &dyn Any = provider;
        provider
            .downcast_ref::<Self>()
            .or_else(|| provider.downcast_ref::<TrackedProvider>()?.pending())
    }

    /// Settles the provider every waiting and later request uses. Only the
    /// first call counts.
    pub(crate) async fn fill(&self, result: Result<PreparedModel, Arc<str>>) {
        let _ = self.ready.set(result).await;
    }

    #[cfg(test)]
    pub(crate) fn with_request_probe(request_probe: flume::Sender<()>) -> Self {
        Self {
            request_probe: Some(request_probe),
            ..Self::default()
        }
    }

    pub(crate) async fn prepared(&self) -> Result<&PreparedModel, AgentError> {
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
    fn model_preparation(&self) -> Option<BoxFuture<'_, Result<&PreparedModel, AgentError>>> {
        Some(Box::pin(async {
            #[cfg(test)]
            if let Some(probe) = &self.request_probe {
                let _ = probe.try_send(());
            }
            self.prepared().await
        }))
    }

    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
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
            let prepared = self.prepared().await?;
            prepared
                .provider
                .stream_message(
                    &prepared.model,
                    messages,
                    system,
                    tools,
                    event_tx,
                    opts.clamped(&prepared.model),
                    session_id,
                    invocation,
                    delivery,
                )
                .await
        })
    }

    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        Box::pin(async { self.prepared().await?.provider.fetch_usage().await })
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { self.prepared().await?.provider.refresh_auth().await })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async { self.prepared().await?.provider.reload_auth().await })
    }

    fn key_count(&self) -> usize {
        match self.ready.get() {
            Some(Ok(prepared)) => prepared.provider.key_count(),
            _ => 1,
        }
    }

    fn rotate_key(&self) -> BoxFuture<'_, bool> {
        Box::pin(async {
            match self.prepared().await {
                Ok(prepared) => prepared.provider.rotate_key().await,
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
            if let Ok(prepared) = self.prepared().await {
                prepared
                    .provider
                    .end_conversation(&conversation_id, reason)
                    .await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentError, Arc, PendingProvider, Provider};

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
