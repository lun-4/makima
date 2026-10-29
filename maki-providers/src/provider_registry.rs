use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

use crate::model::{
    Model, ModelDescriptorError, ModelError, ModelFamily, ModelInfo, ModelTier, ThinkingSupport,
    validate_model_descriptors,
};
use crate::provider::{BoxFuture, Provider};
use crate::{AgentError, Message, ProviderEvent, ProviderUsage, RequestOptions, StreamResponse};
use flume::Sender;
use maki_config::ModelPolicy;
use maki_storage::id::SessionRef;

const DEFAULT_CONTEXT_WINDOW: u32 = 128_000;

pub type PreparedProvider = Result<(Model, Box<dyn Provider>), AgentError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAuthMethod {
    pub id: String,
    pub label: String,
}

pub trait ProviderFactory: Send + Sync {
    fn auth_status<'a>(&'a self, _config: &'a Value) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async { Ok(Value::Null) })
    }

    fn logout<'a>(&'a self, _config: &'a Value) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async { Ok(Value::Null) })
    }

    fn login<'a>(
        &'a self,
        _method: &'a str,
        _interaction: &'a Value,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async {
            Err(AgentError::Config {
                message: "provider does not support login".into(),
            })
        })
    }

    fn discover<'a>(
        &'a self,
        _config: &'a Value,
    ) -> BoxFuture<'a, Result<Option<Vec<ModelInfo>>, AgentError>> {
        Box::pin(async { Ok(None) })
    }

    fn prepare<'a>(
        &'a self,
        model: &'a Model,
        config: &'a Value,
    ) -> BoxFuture<'a, PreparedProvider>;
}

#[derive(Clone)]
pub struct ProviderRegistration {
    pub id: String,
    pub display_name: String,
    pub auth_methods: Vec<ProviderAuthMethod>,
    pub models: Vec<ModelInfo>,
    pub factory: Arc<dyn ProviderFactory>,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("provider field '{0}' must not be empty")]
    EmptyField(&'static str),
    #[error("provider id {0:?} must not contain '/'")]
    InvalidProviderId(String),
    #[error("provider {0:?} is already registered")]
    DuplicateProvider(String),
    #[error("provider {provider:?}: {source}")]
    InvalidModels {
        provider: String,
        #[source]
        source: ModelDescriptorError,
    },
    #[error("provider {0:?} is not registered")]
    UnknownProvider(String),
    #[error("provider {0:?} was replaced since discovery started")]
    StaleProvider(String),
}

#[derive(Clone, Default)]
pub struct ProviderSnapshot {
    providers: BTreeMap<String, ProviderRegistration>,
    owners: BTreeMap<String, String>,
    generations: BTreeMap<String, Arc<()>>,
}

impl ProviderSnapshot {
    pub fn get(&self, id: &str) -> Option<&ProviderRegistration> {
        self.providers.get(id)
    }

    pub fn providers(&self) -> impl Iterator<Item = &ProviderRegistration> {
        self.providers.values()
    }

    pub fn resolve_model(&self, spec: &str, policy: &ModelPolicy) -> Result<Model, ModelError> {
        if !policy.allows(spec) {
            return Err(ModelError::NotAllowed(spec.to_string()));
        }
        let (slug, id) = Model::parse_spec(spec)?;
        let Some(registration) = self.get(slug) else {
            return Model::from_spec(spec);
        };
        let info = registration
            .models
            .iter()
            .filter(|model| id.starts_with(&model.id))
            .max_by_key(|model| model.id.len())
            .ok_or_else(|| ModelError::UnknownModel(spec.to_string()))?;
        Ok(Model {
            id: id.to_string(),
            provider: Arc::from(slug),
            tier: info.tier.unwrap_or(ModelTier::Medium),
            family: ModelFamily::Generic,
            supports_tool_examples_override: None,
            thinking_override: ThinkingSupport::from_flags(info.supports_thinking, false),
            supports_vision_override: info.supports_vision,
            supports_fast_override: None,
            pricing: info.pricing.clone().unwrap_or_default(),
            max_output_tokens: info.max_output_tokens,
            turn_output_tokens: None,
            context_window: info.context_window.unwrap_or(DEFAULT_CONTEXT_WINDOW),
            thinking_fields: None,
        })
    }

    pub async fn prepare(&self, model: &Model, config: &Value) -> Option<PreparedProvider> {
        let registration = self.get(&model.provider)?;
        Some(
            registration
                .factory
                .prepare(model, config)
                .await
                .map(|(model, provider)| {
                    (
                        model,
                        Box::new(RegistryProvider(provider)) as Box<dyn Provider>,
                    )
                }),
        )
    }
}

struct RegistryProvider(Box<dyn Provider>);

impl Provider for RegistryProvider {
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
        self.0
            .stream_message(model, messages, system, tools, event_tx, opts, session_id)
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
        self.0.key_count()
    }

    fn rotate_key(&self) -> BoxFuture<'_, Result<bool, AgentError>> {
        self.0.rotate_key()
    }

    fn adjust_model(&self, model: &mut Model) {
        self.0.adjust_model(model);
    }

    fn is_registry_owned(&self) -> bool {
        true
    }
}

#[derive(Default)]
pub struct ProviderRegistry {
    snapshot: RwLock<Arc<ProviderSnapshot>>,
}

impl ProviderRegistry {
    pub fn snapshot(&self) -> Arc<ProviderSnapshot> {
        self.snapshot
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn register(&self, registration: ProviderRegistration) -> Result<(), RegistryError> {
        validate_registration(&registration)?;
        let mut current = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        if current.providers.contains_key(&registration.id) {
            return Err(RegistryError::DuplicateProvider(registration.id));
        }
        let mut next = (**current).clone();
        next.generations
            .insert(registration.id.clone(), Arc::new(()));
        next.providers.insert(registration.id.clone(), registration);
        let previous = std::mem::replace(&mut *current, Arc::new(next));
        drop(current);
        drop(previous);
        Ok(())
    }

    pub fn replace_owner(
        &self,
        owner: &str,
        registrations: Vec<ProviderRegistration>,
    ) -> Result<(), RegistryError> {
        if owner.trim().is_empty() {
            return Err(RegistryError::EmptyField("owner"));
        }
        let mut candidate = BTreeMap::new();
        for registration in registrations {
            validate_registration(&registration)?;
            let id = registration.id.clone();
            if candidate.insert(id.clone(), registration).is_some() {
                return Err(RegistryError::DuplicateProvider(id));
            }
        }
        let mut current = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        for id in candidate.keys() {
            if current.providers.contains_key(id)
                && current.owners.get(id).map(String::as_str) != Some(owner)
            {
                return Err(RegistryError::DuplicateProvider(id.clone()));
            }
        }
        let mut next = (**current).clone();
        next.providers
            .retain(|id, _| next.owners.get(id).map(String::as_str) != Some(owner));
        next.owners.retain(|_, value| value != owner);
        next.generations
            .retain(|id, _| next.providers.contains_key(id));
        for (id, registration) in candidate {
            next.generations.insert(id.clone(), Arc::new(()));
            next.owners.insert(id.clone(), owner.to_owned());
            next.providers.insert(id, registration);
        }
        let previous = std::mem::replace(&mut *current, Arc::new(next));
        drop(current);
        drop(previous);
        Ok(())
    }

    pub fn remove_owner(&self, owner: &str) {
        // An empty replacement cannot conflict with another owner.
        let _ = self.replace_owner(owner, Vec::new());
    }

    /// Publishes models only if the registration in `source` is still current.
    /// Concurrent publications for the same registration take effect in lock order.
    /// Snapshot isolation does not extend to interior-mutable `provider_info` values.
    pub fn publish_models(
        &self,
        source: &ProviderSnapshot,
        id: &str,
        models: Vec<ModelInfo>,
    ) -> Result<(), RegistryError> {
        validate_model_descriptors(&models).map_err(|source| RegistryError::InvalidModels {
            provider: id.to_owned(),
            source,
        })?;
        let mut current = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        let generation = source
            .generations
            .get(id)
            .ok_or_else(|| RegistryError::UnknownProvider(id.to_owned()))?;
        if !current
            .generations
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, generation))
        {
            return Err(RegistryError::StaleProvider(id.to_owned()));
        }
        let mut next = (**current).clone();
        let provider = next
            .providers
            .get_mut(id)
            .ok_or_else(|| RegistryError::UnknownProvider(id.to_owned()))?;
        provider.models = models;
        let previous = std::mem::replace(&mut *current, Arc::new(next));
        drop(current);
        drop(previous);
        Ok(())
    }
}

fn validate_registration(registration: &ProviderRegistration) -> Result<(), RegistryError> {
    if registration.id.trim().is_empty() {
        return Err(RegistryError::EmptyField("id"));
    }
    if registration.id.contains('/') {
        return Err(RegistryError::InvalidProviderId(registration.id.clone()));
    }
    if registration.display_name.trim().is_empty() {
        return Err(RegistryError::EmptyField("display_name"));
    }
    validate_model_descriptors(&registration.models).map_err(|source| {
        RegistryError::InvalidModels {
            provider: registration.id.clone(),
            source,
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ProviderFactory, ProviderRegistration, ProviderRegistry, RegistryError};
    use crate::AgentError;
    use crate::model::{Model, ModelInfo};
    use crate::provider::{BoxFuture, Provider};
    use serde_json::Value;
    use std::sync::Arc;

    struct Factory;

    impl ProviderFactory for Factory {
        fn prepare<'a>(
            &'a self,
            _model: &'a Model,
            _config: &'a Value,
        ) -> BoxFuture<'a, Result<(Model, Box<dyn Provider>), AgentError>> {
            Box::pin(async { Err(AgentError::Cancelled) })
        }
    }

    fn registration(id: &str, model: &str) -> ProviderRegistration {
        ProviderRegistration {
            id: id.into(),
            display_name: id.into(),
            auth_methods: Vec::new(),
            models: vec![ModelInfo::id_only(model.into())],
            factory: Arc::new(Factory),
        }
    }

    #[test_case::test_case("example/model", 1; "exact")]
    #[test_case::test_case("example/model-20250101", 1; "dated")]
    #[test_case::test_case("example/model-pro", 2; "exact_over_prefix")]
    #[test_case::test_case("example/model-pro-extra", 2; "longest_prefix")]
    fn resolution_uses_most_specific_descriptor(spec: &str, expected_window: u32) {
        let registry = ProviderRegistry::default();
        let mut registration = registration("example", "model");
        registration.models[0].context_window = Some(1);
        let mut specific = ModelInfo::id_only("model-pro".into());
        specific.context_window = Some(2);
        registration.models.push(specific);
        registry.register(registration).unwrap();
        let model = registry
            .snapshot()
            .resolve_model(spec, &Default::default())
            .unwrap();
        assert_eq!(model.id, spec.split_once('/').unwrap().1);
        assert_eq!(model.context_window, expected_window);
    }

    #[test]
    fn resolution_enforces_policy_on_requested_id_and_is_host_scoped() {
        let first = ProviderRegistry::default();
        let second = ProviderRegistry::default();
        first.register(registration("example", "model")).unwrap();
        second.register(registration("example", "other")).unwrap();
        let policy = maki_config::ModelPolicy::new(&[], &["example/model-secret*".into()]).unwrap();
        assert!(matches!(
            first
                .snapshot()
                .resolve_model("example/model-secret", &policy),
            Err(crate::model::ModelError::NotAllowed(_))
        ));
        assert!(matches!(
            second
                .snapshot()
                .resolve_model("example/model", &Default::default()),
            Err(crate::model::ModelError::UnknownModel(_))
        ));
        assert!(
            first
                .snapshot()
                .resolve_model("example/model", &policy)
                .is_ok()
        );
    }

    #[test]
    fn owner_replacement_is_atomic_and_preserves_snapshots() {
        let registry = ProviderRegistry::default();
        registry
            .replace_owner("one", vec![registration("a", "old")])
            .unwrap();
        registry
            .replace_owner("two", vec![registration("b", "other")])
            .unwrap();
        let old = registry.snapshot();
        assert!(matches!(
            registry.replace_owner("one", vec![registration("b", "new")]),
            Err(RegistryError::DuplicateProvider(_))
        ));
        assert!(Arc::ptr_eq(&old, &registry.snapshot()));
        assert!(
            registry
                .replace_owner("one", vec![registration("c", "")])
                .is_err()
        );
        assert!(Arc::ptr_eq(&old, &registry.snapshot()));
        registry
            .replace_owner("one", vec![registration("c", "new")])
            .unwrap();
        assert!(registry.snapshot().get("a").is_none());
        assert_eq!(old.get("a").unwrap().models[0].id, "old");
        registry.remove_owner("one");
        assert!(registry.snapshot().get("c").is_none());
        assert!(registry.snapshot().get("b").is_some());
    }

    #[test]
    fn unreachable_provider_ids_are_rejected_atomically() {
        let registry = ProviderRegistry::default();
        registry
            .replace_owner("owner", vec![registration("valid", "model")])
            .unwrap();
        let before = registry.snapshot();
        assert!(matches!(
            registry.register(registration("invalid/id", "model")),
            Err(RegistryError::InvalidProviderId(id)) if id == "invalid/id"
        ));
        assert!(matches!(
            registry.replace_owner("owner", vec![registration("new", "model"), registration("invalid/id", "model")]),
            Err(RegistryError::InvalidProviderId(id)) if id == "invalid/id"
        ));
        assert!(Arc::ptr_eq(&before, &registry.snapshot()));
        assert!(registry.snapshot().get("valid").is_some());
    }

    #[test]
    fn stale_discovery_cannot_modify_replacement() {
        let registry = ProviderRegistry::default();
        registry
            .replace_owner("one", vec![registration("example", "old")])
            .unwrap();
        let old = registry.snapshot();
        registry
            .replace_owner("one", vec![registration("example", "reload")])
            .unwrap();
        assert!(matches!(
            registry.publish_models(&old, "example", vec![]),
            Err(RegistryError::StaleProvider(_))
        ));
        let reloaded = registry.snapshot();
        registry.remove_owner("one");
        registry
            .replace_owner("two", vec![registration("example", "new")])
            .unwrap();
        assert!(matches!(
            registry.publish_models(&reloaded, "example", vec![]),
            Err(RegistryError::StaleProvider(_))
        ));
        assert_eq!(
            registry.snapshot().get("example").unwrap().models[0].id,
            "new"
        );
    }

    #[test]
    fn replaced_metadata_is_dropped_after_unlocking() {
        struct Reenter(Arc<ProviderRegistry>);

        impl Drop for Reenter {
            fn drop(&mut self) {
                assert!(self.0.snapshot.try_read().is_ok());
            }
        }

        let registry = Arc::new(ProviderRegistry::default());
        let mut registration = registration("example", "old");
        registration.models[0].provider_info = Some(Arc::new(Reenter(registry.clone())));
        registry.register(registration).unwrap();
        registry
            .publish_models(&registry.snapshot(), "example", vec![])
            .unwrap();
    }

    #[test]
    fn snapshots_are_isolated_and_failed_publication_is_atomic() {
        let first = ProviderRegistry::default();
        let second = ProviderRegistry::default();
        first.register(registration("example", "old")).unwrap();
        second.register(registration("example", "other")).unwrap();
        let old = first.snapshot();
        first
            .publish_models(&old, "example", vec![ModelInfo::id_only("new".into())])
            .unwrap();
        let current = first.snapshot();
        assert_eq!(old.get("example").unwrap().models[0].id, "old");
        assert_eq!(current.get("example").unwrap().models[0].id, "new");
        assert_eq!(
            second.snapshot().get("example").unwrap().models[0].id,
            "other"
        );
        let invalid = vec![
            ModelInfo::id_only("valid".into()),
            ModelInfo::id_only("".into()),
        ];
        assert!(matches!(
            first.publish_models(&current, "example", invalid),
            Err(RegistryError::InvalidModels { .. })
        ));
        assert!(Arc::ptr_eq(&current, &first.snapshot()));
        assert!(matches!(
            first.register(registration("example", "replacement")),
            Err(RegistryError::DuplicateProvider(_))
        ));
        assert!(Arc::ptr_eq(&current, &first.snapshot()));
        assert!(matches!(
            first.publish_models(&current, "missing", vec![]),
            Err(RegistryError::UnknownProvider(_))
        ));
    }
}
