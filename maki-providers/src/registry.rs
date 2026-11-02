//! Providers registered by plugins, consulted before the native ones.
//!
//! Resolution reads descriptors held here and never calls into a backend, so
//! it stays cheap enough for the UI thread. A provider id the registry does
//! not know falls back to the native tables until those are removed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use maki_config::ModelPolicy;
use tracing::warn;

use crate::error::AgentError;
use crate::manifest::ManifestRegistry;
use crate::model::{Model, ModelError, ModelInfo, ModelTier};
use crate::model_registry;
use crate::provider::{self, BoxFuture, ModelBatch, Provider};
use crate::providers::Timeouts;

pub trait ProviderBackend: Send + Sync {
    /// Builds the provider for one model. The returned model may differ from
    /// the requested one where the backend knows better.
    fn prepare(
        &self,
        model: Model,
        info: ModelInfo,
    ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>>;

    /// Lists the models the provider offers right now; `None` when it only
    /// offers its curated descriptors.
    fn discover(&self) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>>;
}

#[derive(Clone)]
pub struct PreparedProvider {
    pub model: Model,
    pub provider: Arc<dyn Provider>,
}

pub struct RegisteredProvider {
    pub id: Arc<str>,
    pub display_name: Arc<str>,
    pub models: Vec<ModelInfo>,
    pub backend: Arc<dyn ProviderBackend>,
}

struct Entry {
    display_name: Arc<str>,
    backend: Arc<dyn ProviderBackend>,
    models: RwLock<Arc<[ModelInfo]>>,
}

impl Entry {
    fn models(&self) -> Arc<[ModelInfo]> {
        Arc::clone(&self.models.read().unwrap())
    }

    fn info(&self, model_id: &str) -> ModelInfo {
        self.models()
            .iter()
            .find(|info| info.id == model_id)
            .cloned()
            .unwrap_or_else(|| ModelInfo::id_only(model_id.to_string()))
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

#[derive(Default)]
pub struct ProviderRegistry {
    entries: BTreeMap<Arc<str>, Entry>,
    timeouts: Timeouts,
}

impl ProviderRegistry {
    pub fn new(providers: Vec<RegisteredProvider>, timeouts: Timeouts) -> Self {
        let entries = providers
            .into_iter()
            .map(|provider| {
                let entry = Entry {
                    display_name: provider.display_name,
                    backend: provider.backend,
                    models: RwLock::new(provider.models.into()),
                };
                (provider.id, entry)
            })
            .collect();
        Self { entries, timeouts }
    }

    fn registered(&self, spec: &str) -> bool {
        Model::parse_spec(spec).is_ok_and(|(provider, _)| self.entries.contains_key(provider))
    }

    pub fn model(&self, spec: &str) -> Result<Model, ModelError> {
        let (provider, model_id) = Model::parse_spec(spec)?;
        match self.entries.get(provider) {
            Some(entry) => Ok(Model::from_info(provider, &entry.info(model_id))),
            None => Model::from_spec(spec),
        }
    }

    /// User overrides first, then the first descriptor declaring the tier
    /// that no override already gave to another tier.
    pub fn spec_for_tier(&self, provider: &str, tier: ModelTier) -> Option<String> {
        let Some(entry) = self.entries.get(provider) else {
            return model_registry::spec_for_tier(provider, tier);
        };
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
        model_registry::spec_for_tier_any(tier).or_else(|| {
            self.entries
                .keys()
                .find_map(|provider| self.spec_for_tier(provider, tier))
        })
    }

    pub fn model_for_tier(
        &self,
        provider: &str,
        tier: ModelTier,
        policy: &ModelPolicy,
    ) -> Result<Model, ModelError> {
        let Some(entry) = self.entries.get(provider) else {
            return Model::from_tier_with_policy(provider, tier, policy);
        };
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

    pub fn available(&self, provider: &str) -> bool {
        self.entries.contains_key(provider) || provider::provider_available(provider)
    }

    pub fn display_name(&self, provider: &str) -> Option<Arc<str>> {
        match self.entries.get(provider) {
            Some(entry) => Some(Arc::clone(&entry.display_name)),
            None => {
                ManifestRegistry::for_slug(provider).map(|manifest| manifest.display_name.into())
            }
        }
    }

    pub async fn prepare(&self, mut model: Model) -> Result<PreparedProvider, AgentError> {
        if let Some(entry) = self.entries.get(&*model.provider) {
            let info = entry.info(&model.id);
            return entry.backend.prepare(model, info).await;
        }
        let provider = provider::from_model_async(&mut model, self.timeouts).await?;
        Ok(PreparedProvider {
            model,
            provider: Arc::from(provider),
        })
    }

    /// Specs known without any network call.
    pub fn offline_specs(&self, policy: &ModelPolicy) -> Vec<String> {
        let mut specs = provider::available_model_specs(policy);
        specs.retain(|spec| !self.registered(spec));
        for (provider, entry) in &self.entries {
            specs.extend(
                entry
                    .specs(provider)
                    .into_iter()
                    .filter(|spec| policy.allows(spec)),
            );
        }
        specs
    }

    /// Native discovery plus every registered provider's own, each batch
    /// delivered as it arrives.
    pub async fn fetch_all(&self, policy: &ModelPolicy, on_ready: impl FnMut(ModelBatch) + Send) {
        let on_ready = Mutex::new(on_ready);
        let native = provider::fetch_all_models(policy, |mut batch| {
            batch.models.retain(|spec| !self.registered(spec));
            (on_ready.lock().unwrap())(batch);
        });
        let registered = self.discover_registered(policy, |batch| {
            (on_ready.lock().unwrap())(batch);
        });
        futures_lite::future::zip(native, registered).await;
    }

    async fn discover_registered(
        &self,
        policy: &ModelPolicy,
        mut on_ready: impl FnMut(ModelBatch),
    ) {
        let (tx, rx) = flume::unbounded();
        for (provider, entry) in &self.entries {
            let Some(discovery) = entry.backend.discover() else {
                continue;
            };
            let tx = tx.clone();
            let provider = Arc::clone(provider);
            smol::spawn(async move {
                let _ = tx.send_async((provider, discovery.await)).await;
            })
            .detach();
        }
        drop(tx);
        while let Ok((provider, result)) = rx.recv_async().await {
            on_ready(self.apply_discovery(provider, result, policy));
        }
    }

    fn apply_discovery(
        &self,
        provider: Arc<str>,
        result: Result<Vec<ModelInfo>, AgentError>,
        policy: &ModelPolicy,
    ) -> ModelBatch {
        let entry = &self.entries[&provider];
        let warnings = match result {
            Ok(models) => {
                *entry.models.write().unwrap() = models.into();
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
    const SHADOWED: &str = "anthropic";
    const STRONG_FIRST: &str = "strong-first";
    const STRONG_SECOND: &str = "strong-second";
    const WEAK: &str = "weak-only";
    const DISCOVERED: &str = "discovered";
    const CONTEXT_WINDOW: u32 = 42_000;
    const NATIVE_SPEC: &str = "anthropic/claude-sonnet-4-5";
    const DISCOVERY_FAILURE: &str = "listing failed";
    const APERTURE_HOST_ENV: &str = "APERTURE_HOST";
    const APERTURE_HOST: &str = "https://example.com";
    /// Routed through aperture, it inherits zai's thinking support.
    const ROUTED_SPEC: &str = "aperture/zai/glm-5.2";

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
                    provider: provider::unconfigured(),
                })
            })
        }

        fn discover(&self) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
            let result = self.discovered.clone()?;
            Some(Box::pin(async move {
                result.map_err(|message| AgentError::Config { message })
            }))
        }
    }

    fn info(id: &str, tier: Option<ModelTier>) -> ModelInfo {
        ModelInfo {
            tier,
            context_window: Some(CONTEXT_WINDOW),
            ..ModelInfo::id_only(id.to_string())
        }
    }

    fn registry_with(
        id: &str,
        discovered: Option<Result<Vec<ModelInfo>, String>>,
    ) -> (ProviderRegistry, Arc<FakeBackend>) {
        let backend = Arc::new(FakeBackend {
            discovered,
            prepared: AtomicBool::new(false),
        });
        let registry = ProviderRegistry::new(
            vec![RegisteredProvider {
                id: id.into(),
                display_name: id.into(),
                models: vec![
                    info(STRONG_FIRST, Some(ModelTier::Strong)),
                    info(STRONG_SECOND, Some(ModelTier::Strong)),
                    info(WEAK, Some(ModelTier::Weak)),
                ],
                backend: Arc::clone(&backend) as Arc<dyn ProviderBackend>,
            }],
            Timeouts::default(),
        );
        (registry, backend)
    }

    fn policy(allowed: &[&str]) -> ModelPolicy {
        let allowed: Vec<String> = allowed.iter().map(|pattern| (*pattern).into()).collect();
        ModelPolicy::new(&allowed, &[]).unwrap()
    }

    #[test]
    fn registered_id_shadows_native_provider() {
        let (registry, backend) = registry_with(SHADOWED, None);
        let model = registry
            .model(&format!("{SHADOWED}/{STRONG_FIRST}"))
            .unwrap();
        assert_eq!(model.context_window, CONTEXT_WINDOW);
        assert_eq!(model.tier, ModelTier::Strong);

        let prepared = smol::block_on(registry.prepare(model)).unwrap();
        assert!(backend.prepared.load(Ordering::SeqCst));
        assert_eq!(prepared.model.id, STRONG_FIRST);
        assert!(
            !registry
                .offline_specs(&ModelPolicy::default())
                .contains(&NATIVE_SPEC.into())
        );
    }

    #[test]
    fn unknown_model_of_registered_provider_has_unknown_metadata() {
        let (registry, _) = registry_with(FAKE, None);
        let model = registry.model(&format!("{FAKE}/not-listed")).unwrap();
        assert_eq!(model.context_window, crate::model::UNKNOWN_CONTEXT_WINDOW);
        assert_eq!(model.max_output_tokens, None);
        assert_eq!(model.thinking_override, None);
        assert_eq!(model.supports_vision_override, None);
    }

    #[test]
    fn first_descriptor_is_tier_default_and_override_wins() {
        let (registry, _) = registry_with(FAKE, None);
        let first = format!("{FAKE}/{STRONG_FIRST}");
        let second = format!("{FAKE}/{STRONG_SECOND}");
        assert_eq!(registry.spec_for_tier(FAKE, ModelTier::Strong), Some(first));

        let state = tempfile::tempdir().unwrap();
        let dir = maki_storage::StateDir::from_path(state.path().to_path_buf());
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
    fn unregistered_provider_falls_back_to_native() {
        let registry = ProviderRegistry::default();
        let native = Model::from_spec(NATIVE_SPEC).unwrap();
        let resolved = registry.model(NATIVE_SPEC).unwrap();
        assert_eq!(resolved.spec(), native.spec());
        assert_eq!(resolved.context_window, native.context_window);
        assert_eq!(
            registry.display_name(SHADOWED).as_deref(),
            ManifestRegistry::for_slug(SHADOWED).map(|manifest| manifest.display_name)
        );
    }

    #[test]
    fn native_prepare_applies_provider_adjustments() {
        // SAFETY: nextest runs each test in its own process.
        unsafe { std::env::set_var(APERTURE_HOST_ENV, APERTURE_HOST) };
        let registry = ProviderRegistry::default();
        let model = registry.model(ROUTED_SPEC).unwrap();
        assert!(!model.supports_thinking());
        let prepared = smol::block_on(registry.prepare(model)).unwrap();
        assert!(prepared.model.supports_thinking());
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
        smol::block_on(
            registry.discover_registered(&ModelPolicy::default(), |batch| {
                batches.push(batch.models);
            }),
        );
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
        smol::block_on(
            registry.discover_registered(&ModelPolicy::default(), |batch| {
                warnings.extend(batch.warnings);
            }),
        );
        assert!(warnings[0].contains(DISCOVERY_FAILURE));
        assert!(registry.spec_for_tier(FAKE, ModelTier::Strong).is_some());
    }
}
