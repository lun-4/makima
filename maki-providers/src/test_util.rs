//! Registry fakes for tests that need providers without plugins or network.

use std::sync::Arc;

use serde_json::Value;

use crate::login::{AuthSpec, AuthStatus, InteractionService};
use crate::provider::{BoxFuture, unconfigured};
use crate::{
    AgentError, Model, ModelInfo, PreparedProvider, ProviderBackend, ProviderRegistry,
    RegisteredProvider,
};

struct OfflineBackend;

impl ProviderBackend for OfflineBackend {
    fn prepare(
        &self,
        model: Model,
        _info: ModelInfo,
    ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>> {
        Box::pin(async move {
            Ok(PreparedProvider {
                model,
                provider: unconfigured(),
            })
        })
    }

    fn discover(
        &self,
        _refresh: bool,
    ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
        None
    }

    fn login(
        &self,
        _method: String,
        _config: Value,
        _ui: Arc<dyn InteractionService>,
    ) -> BoxFuture<'static, Result<Option<String>, AgentError>> {
        Box::pin(async { Ok(None) })
    }

    fn auth_status(
        &self,
        _config: Value,
    ) -> Option<BoxFuture<'static, Result<AuthStatus, AgentError>>> {
        None
    }

    fn logout(&self) -> Option<BoxFuture<'static, Result<(), AgentError>>> {
        None
    }
}

/// A registry of {providers}, each an id with its curated models and the id
/// as its display name. Any model id of a listed provider resolves; `prepare`
/// hands out a provider that fails each request.
pub fn offline_registry(providers: Vec<(&str, Vec<ModelInfo>)>) -> ProviderRegistry {
    ProviderRegistry::new(
        providers
            .into_iter()
            .map(|(id, models)| RegisteredProvider {
                id: id.into(),
                display_name: id.into(),
                models,
                backend: Arc::new(OfflineBackend),
                auth: AuthSpec::default(),
                docs: None,
            })
            .collect(),
    )
}

/// [`offline_registry`] of {ids} without curated models.
pub fn offline_ids(ids: &[&str]) -> ProviderRegistry {
    offline_registry(ids.iter().map(|id| (*id, Vec::new())).collect())
}
