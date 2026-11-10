//! Providers registered by plugins.
//!
//! Resolution reads descriptors held here and never calls into a backend, so
//! it stays cheap enough for the UI thread.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use futures::stream::{FuturesUnordered, StreamExt};
use makima_config::ModelPolicy;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use crate::error::AgentError;
use crate::login::{AuthSpec, AuthStatus, InteractionService};
use crate::model::{Model, ModelError, ModelInfo, ModelTier};
use crate::model_registry;
use crate::provider::{BoxFuture, ModelBatch, Provider};

pub trait ProviderBackend: Send + Sync {
    /// Builds the provider for one model. The returned model may differ from
    /// the requested one where the backend knows better.
    fn prepare(
        &self,
        model: Model,
        info: ModelInfo,
    ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>>;

    /// Lists the models the provider offers right now; `None` when it only
    /// offers its curated descriptors. {refresh} asks it to bypass any cache.
    /// Building the future starts nothing, so a caller may only check it.
    fn discover(
        &self,
        refresh: bool,
    ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>>;

    /// Logs in with {method}, given the provider's `providers.toml` section
    /// with this login's answers applied. Returns the model id to default to.
    fn login(
        &self,
        method: String,
        config: Value,
        ui: Arc<dyn InteractionService>,
    ) -> BoxFuture<'static, Result<Option<String>, AgentError>>;

    /// `None` when the provider has no check of its own and its credentials
    /// file decides.
    fn auth_status(
        &self,
        config: Value,
    ) -> Option<BoxFuture<'static, Result<AuthStatus, AgentError>>>;

    /// `None` when deleting the credentials file is all a logout takes.
    fn logout(&self) -> Option<BoxFuture<'static, Result<(), AgentError>>>;
}

#[derive(Clone)]
pub struct PreparedProvider {
    pub model: Model,
    pub provider: Arc<dyn Provider>,
}

/// What the providers reference page says about a provider beyond its
/// models, login methods and config fields.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDocs {
    /// Environment variables the provider reads.
    #[serde(default)]
    pub env: Vec<String>,
    /// The endpoint it talks to by default.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Markdown shown under its heading.
    #[serde(default)]
    pub notes: Option<String>,
}

pub struct RegisteredProvider {
    pub id: Arc<str>,
    pub display_name: Arc<str>,
    pub models: Vec<ModelInfo>,
    pub backend: Arc<dyn ProviderBackend>,
    pub auth: AuthSpec,
    pub docs: Option<ProviderDocs>,
}

/// A registered provider as the reference docs read it.
pub struct ProviderListing<'a> {
    pub id: &'a str,
    pub display_name: &'a str,
    pub models: Arc<[ModelInfo]>,
    pub auth: &'a AuthSpec,
    pub docs: Option<&'a ProviderDocs>,
}

pub(crate) struct Entry {
    pub(crate) display_name: Arc<str>,
    pub(crate) backend: Arc<dyn ProviderBackend>,
    pub(crate) auth: AuthSpec,
    docs: Option<ProviderDocs>,
    models: RwLock<Arc<[ModelInfo]>>,
}

impl Entry {
    /// Whether the provider serves any model, curated or discovered.
    pub(crate) fn offers_models(&self) -> bool {
        !self.models().is_empty() || self.backend.discover(false).is_some()
    }

    fn models(&self) -> Arc<[ModelInfo]> {
        Arc::clone(&self.models.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// The descriptor whose id is the longest prefix of {model_id}, as the
    /// providers' own lookup matches a dated or suffixed id, renamed to it.
    fn resolved(&self, model_id: &str) -> ModelInfo {
        self.models()
            .iter()
            .filter(|info| model_id.starts_with(info.id.as_str()))
            .min_by_key(|info| Reverse(info.id.len()))
            .map_or_else(
                || ModelInfo::id_only(model_id.to_string()),
                |info| ModelInfo {
                    id: model_id.to_string(),
                    ..info.clone()
                },
            )
    }

    fn specs(&self, provider: &str) -> Vec<String> {
        self.models()
            .iter()
            .map(|info| format!("{provider}/{}", info.id))
            .collect()
    }

    fn tier_specs(&self, provider: &str, tier: ModelTier) -> Vec<String> {
        self.models()
            .iter()
            .filter(|info| info.tier == Some(tier))
            .map(|info| format!("{provider}/{}", info.id))
            .collect()
    }
}

/// Providers in registration order, which is the order auto-detect, the
/// login picker and the docs follow.
#[derive(Default)]
pub struct ProviderRegistry {
    entries: Vec<(Arc<str>, Entry)>,
    positions: HashMap<Arc<str>, usize>,
}

impl ProviderRegistry {
    pub fn new(providers: Vec<RegisteredProvider>) -> Self {
        let entries: Vec<_> = providers
            .into_iter()
            .map(|provider| {
                let entry = Entry {
                    display_name: provider.display_name,
                    backend: provider.backend,
                    auth: provider.auth,
                    docs: provider.docs,
                    models: RwLock::new(provider.models.into()),
                };
                (provider.id, entry)
            })
            .collect();
        let positions = entries
            .iter()
            .enumerate()
            .map(|(position, (id, _))| (Arc::clone(id), position))
            .collect();
        Self { entries, positions }
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = (&Arc<str>, &Entry)> {
        self.entries.iter().map(|(id, entry)| (id, entry))
    }

    fn get(&self, provider: &str) -> Option<&Entry> {
        self.get_key_value(provider).map(|(_, entry)| entry)
    }

    fn get_key_value(&self, provider: &str) -> Option<(&Arc<str>, &Entry)> {
        let (id, entry) = &self.entries[*self.positions.get(provider)?];
        Some((id, entry))
    }

    pub fn listings(&self) -> impl Iterator<Item = ProviderListing<'_>> {
        self.entries().map(|(id, entry)| ProviderListing {
            id,
            display_name: &entry.display_name,
            models: entry.models(),
            auth: &entry.auth,
            docs: entry.docs.as_ref(),
        })
    }

    /// A provider id, or else the first alias named {name}, with the
    /// alias's preset method.
    pub(crate) fn login_target(&self, name: &str) -> Option<(&Arc<str>, &Entry, Option<&str>)> {
        if let Some((id, entry)) = self.get_key_value(name) {
            return Some((id, entry, None));
        }
        self.entries().find_map(|(id, entry)| {
            let alias = entry.auth.aliases.iter().find(|alias| alias.id == name)?;
            Some((id, entry, Some(alias.method.as_str())))
        })
    }

    pub fn model(&self, spec: &str) -> Result<Model, ModelError> {
        let (provider, model_id) = Model::parse_spec(spec)?;
        let entry = self
            .get(provider)
            .ok_or_else(|| ModelError::UnsupportedProvider(provider.to_string()))?;
        Ok(Model::from_info(provider, &entry.resolved(model_id)))
    }

    /// User overrides first, then the first descriptor declaring the tier
    /// that no override already gave to another tier.
    pub fn spec_for_tier(&self, provider: &str, tier: ModelTier) -> Option<String> {
        let entry = self.get(provider)?;
        let prefix = format!("{provider}/");
        model_registry::tier_override(tier)
            .filter(|spec| spec.starts_with(&prefix))
            .or_else(|| {
                entry.tier_specs(provider, tier).into_iter().find(|spec| {
                    model_registry::override_tiers(spec)
                        .iter()
                        .all(|claimed| *claimed == tier)
                })
            })
    }

    pub fn spec_for_tier_any(&self, tier: ModelTier) -> Option<String> {
        model_registry::tier_override(tier).or_else(|| {
            self.entries
                .iter()
                .find_map(|(provider, _)| self.spec_for_tier(provider, tier))
        })
    }

    pub fn model_for_tier(
        &self,
        provider: &str,
        tier: ModelTier,
        policy: &ModelPolicy,
    ) -> Result<Model, ModelError> {
        let entry = self
            .get(provider)
            .ok_or_else(|| ModelError::UnsupportedProvider(provider.to_string()))?;
        self.spec_for_tier(provider, tier)
            .filter(|spec| policy.allows(spec))
            .or_else(|| {
                entry
                    .tier_specs(provider, tier)
                    .into_iter()
                    .find(|spec| policy.allows(spec))
            })
            .ok_or_else(|| ModelError::NoAllowedModel(provider.to_string(), tier))
            .and_then(|spec| self.model(&spec))
    }

    pub fn display_name(&self, provider: &str) -> Option<Arc<str>> {
        self.get(provider)
            .map(|entry| Arc::clone(&entry.display_name))
    }

    pub async fn prepare(&self, model: Model) -> Result<PreparedProvider, AgentError> {
        let Some(entry) = self.get(&model.provider) else {
            return Err(AgentError::Config {
                message: format!("unknown provider '{}'", model.provider),
            });
        };
        let info = entry.resolved(&model.id);
        entry.backend.prepare(model, info).await
    }

    /// Specs known without any network call.
    pub fn offline_specs(&self, policy: &ModelPolicy) -> Vec<String> {
        self.entries()
            .flat_map(|(provider, entry)| entry.specs(provider))
            .filter(|spec| policy.allows(spec))
            .collect()
    }

    /// Every provider's discovery, each batch delivered as it arrives.
    /// {refresh} asks providers to bypass caches.
    pub async fn fetch_all(
        &self,
        policy: &ModelPolicy,
        refresh: bool,
        mut on_ready: impl FnMut(ModelBatch) + Send,
    ) {
        let discoveries = self
            .entries()
            .filter_map(|(provider, entry)| {
                let discovery = entry.backend.discover(refresh)?;
                let provider = Arc::clone(provider);
                Some(async move { (provider, discovery.await) })
            })
            .collect::<FuturesUnordered<_>>();
        futures::pin_mut!(discoveries);
        while let Some((provider, result)) = discoveries.next().await {
            on_ready(self.apply_discovery(provider, result, policy));
        }
    }

    fn apply_discovery(
        &self,
        provider: Arc<str>,
        result: Result<Vec<ModelInfo>, AgentError>,
        policy: &ModelPolicy,
    ) -> ModelBatch {
        let entry = &self.entries[self.positions[&provider]].1;
        let warnings = match result {
            Ok(models) => {
                *entry.models.write().unwrap_or_else(PoisonError::into_inner) = models.into();
                Vec::new()
            }
            Err(error) => {
                warn!(provider = %provider, error = %error, "model discovery failed");
                vec![format!("{}: {error}", entry.display_name)]
            }
        };
        ModelBatch {
            models: entry
                .specs(&provider)
                .into_iter()
                .filter(|spec| policy.allows(spec))
                .collect(),
            provider: Some(provider),
            warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use test_case::test_case;

    use super::*;

    const FAKE: &str = "fake-registered";
    const STRONG: &str = "strong";
    const STRONG_FIRST: &str = "strong-first";
    const STRONG_SECOND: &str = "strong-second";
    const WEAK: &str = "weak-only";
    const DISCOVERED: &str = "discovered";
    const CONTEXT_WINDOW: u32 = 42_000;
    const DISCOVERY_FAILURE: &str = "listing failed";
    const REGISTERED_FIRST: &str = "zeta";
    const REGISTERED_SECOND: &str = "alpha";

    struct FakeBackend {
        discovered: Option<Result<Vec<ModelInfo>, String>>,
        prepared: AtomicBool,
    }

    impl ProviderBackend for FakeBackend {
        fn prepare(
            &self,
            model: Model,
            _info: ModelInfo,
        ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>> {
            self.prepared.store(true, Ordering::SeqCst);
            Box::pin(async move {
                Ok(PreparedProvider {
                    model,
                    provider: crate::provider::unconfigured(),
                })
            })
        }

        fn discover(
            &self,
            _refresh: bool,
        ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
            let result = self.discovered.clone()?;
            Some(Box::pin(async move {
                result.map_err(|message| AgentError::Config { message })
            }))
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

    fn info(id: &str, tier: Option<ModelTier>) -> ModelInfo {
        ModelInfo {
            tier,
            context_window: Some(CONTEXT_WINDOW),
            ..ModelInfo::id_only(id.to_string())
        }
    }

    fn registered(id: &str, backend: Arc<FakeBackend>) -> RegisteredProvider {
        RegisteredProvider {
            id: id.into(),
            display_name: id.into(),
            models: vec![
                info(STRONG_FIRST, Some(ModelTier::Strong)),
                info(STRONG_SECOND, Some(ModelTier::Strong)),
                info(WEAK, Some(ModelTier::Weak)),
            ],
            backend,
            auth: AuthSpec::default(),
            docs: None,
        }
    }

    fn backend(discovered: Option<Result<Vec<ModelInfo>, String>>) -> Arc<FakeBackend> {
        Arc::new(FakeBackend {
            discovered,
            prepared: AtomicBool::new(false),
        })
    }

    fn registry_with(
        id: &str,
        discovered: Option<Result<Vec<ModelInfo>, String>>,
    ) -> (ProviderRegistry, Arc<FakeBackend>) {
        let backend = backend(discovered);
        let registry = ProviderRegistry::new(vec![registered(id, Arc::clone(&backend))]);
        (registry, backend)
    }

    fn policy(allowed: &[&str]) -> ModelPolicy {
        let allowed: Vec<String> = allowed.iter().map(|pattern| (*pattern).into()).collect();
        ModelPolicy::new(&allowed, &[]).unwrap()
    }

    #[test]
    fn registered_provider_resolves_and_prepares() {
        let (registry, backend) = registry_with(FAKE, None);
        let model = registry.model(&format!("{FAKE}/{STRONG_FIRST}")).unwrap();
        assert_eq!(model.context_window, CONTEXT_WINDOW);
        assert_eq!(model.tier, ModelTier::Strong);

        let prepared = smol::block_on(registry.prepare(model)).unwrap();
        assert!(backend.prepared.load(Ordering::SeqCst));
        assert_eq!(prepared.model.id, STRONG_FIRST);
    }

    #[test_case("strong-first-20260101", ModelTier::Strong ; "dated_variant_of_the_longer_id")]
    #[test_case("strong-second", ModelTier::Medium ; "exact_id_beats_a_shorter_prefix")]
    #[test_case("strong-other", ModelTier::Weak ; "falls_back_to_the_shorter_prefix")]
    fn longest_descriptor_prefix_describes_a_model(model_id: &str, tier: ModelTier) {
        let discovered = vec![
            info(STRONG, Some(ModelTier::Weak)),
            info(STRONG_FIRST, Some(ModelTier::Strong)),
            info(STRONG_SECOND, Some(ModelTier::Medium)),
        ];
        let (registry, _) = registry_with(FAKE, Some(Ok(discovered)));
        smol::block_on(registry.fetch_all(&ModelPolicy::default(), false, |_| {}));
        let model = registry.model(&format!("{FAKE}/{model_id}")).unwrap();
        assert_eq!((model.id.as_str(), model.tier), (model_id, tier));
        assert_eq!(model.context_window, CONTEXT_WINDOW);
    }

    #[test]
    fn unknown_model_of_registered_provider_has_unknown_metadata() {
        let (registry, _) = registry_with(FAKE, None);
        let model = registry.model(&format!("{FAKE}/not-listed")).unwrap();
        assert_eq!(model.context_window, crate::model::UNKNOWN_CONTEXT_WINDOW);
        assert_eq!(model.max_output_tokens, None);
        assert_eq!(model.thinking_support, None);
        assert_eq!(model.vision, None);
    }

    #[test]
    fn first_descriptor_is_tier_default_and_override_wins() {
        let (registry, _) = registry_with(FAKE, None);
        let first = format!("{FAKE}/{STRONG_FIRST}");
        let second = format!("{FAKE}/{STRONG_SECOND}");
        assert_eq!(registry.spec_for_tier(FAKE, ModelTier::Strong), Some(first));

        let state = tempfile::tempdir().unwrap();
        let dir = makima_storage::StateDir::from_path(state.path().to_path_buf());
        model_registry::set_and_persist(second.clone(), ModelTier::Compaction, &dir);
        let compaction = registry.spec_for_tier(FAKE, ModelTier::Compaction);
        model_registry::unset_and_persist(&second, ModelTier::Compaction, &dir);
        assert_eq!(compaction, Some(second));
    }

    #[test_case(&[], STRONG_FIRST ; "default when allowed")]
    #[test_case(&["fake-registered/strong-second"], STRONG_SECOND ; "next allowed descriptor")]
    fn model_for_tier_respects_policy(allowed: &[&str], expected: &str) {
        let (registry, _) = registry_with(FAKE, None);
        let model = registry
            .model_for_tier(FAKE, ModelTier::Strong, &policy(allowed))
            .unwrap();
        assert_eq!(model.id, expected);
    }

    #[test]
    fn providers_keep_registration_order() {
        let registry = ProviderRegistry::new(vec![
            registered(REGISTERED_FIRST, backend(None)),
            registered(REGISTERED_SECOND, backend(None)),
        ]);
        let ids: Vec<_> = registry.listings().map(|listing| listing.id).collect();
        assert_eq!(ids, [REGISTERED_FIRST, REGISTERED_SECOND]);
        assert_eq!(
            registry.spec_for_tier_any(ModelTier::Strong),
            Some(format!("{REGISTERED_FIRST}/{STRONG_FIRST}"))
        );
        assert!(
            registry
                .model(&format!("{REGISTERED_SECOND}/{WEAK}"))
                .is_ok()
        );
    }

    #[test]
    fn unregistered_provider_does_not_resolve() {
        let registry = ProviderRegistry::default();
        assert!(matches!(
            registry.model(&format!("{FAKE}/{WEAK}")),
            Err(ModelError::UnsupportedProvider(_))
        ));
        assert!(registry.display_name(FAKE).is_none());
    }

    #[test]
    fn offline_specs_respect_policy() {
        let (registry, _) = registry_with(FAKE, None);
        let allowed = format!("{FAKE}/{WEAK}");
        assert_eq!(registry.offline_specs(&policy(&[&allowed])), vec![allowed]);
    }

    #[test]
    fn discovery_replaces_model_list() {
        let (registry, _) = registry_with(FAKE, Some(Ok(vec![info(DISCOVERED, None)])));
        let mut batches = Vec::new();
        smol::block_on(registry.fetch_all(&ModelPolicy::default(), false, |batch| {
            batches.push(batch.models);
        }));
        let discovered = format!("{FAKE}/{DISCOVERED}");
        assert_eq!(batches, vec![vec![discovered.clone()]]);
        assert_eq!(registry.spec_for_tier(FAKE, ModelTier::Strong), None);
        assert_eq!(
            registry.model(&discovered).unwrap().context_window,
            CONTEXT_WINDOW
        );
    }

    #[test]
    fn failed_discovery_keeps_curated_list() {
        let (registry, _) = registry_with(FAKE, Some(Err(DISCOVERY_FAILURE.into())));
        let mut warnings = Vec::new();
        smol::block_on(registry.fetch_all(&ModelPolicy::default(), false, |batch| {
            warnings.extend(batch.warnings);
        }));
        assert!(warnings[0].contains(DISCOVERY_FAILURE));
        assert!(registry.spec_for_tier(FAKE, ModelTier::Strong).is_some());
    }
}
