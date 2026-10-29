use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use maki_agent::actor::ConfigChange;
use maki_agent::session_coordinator::{SessionCoordinatorHandle, publication_claim};
use maki_config::ModelPolicy;
use maki_providers::provider::{ModelBatch, available_model_specs, fetch_all_models_excluding};
use maki_providers::provider_registry::{ProviderRegistry, ProviderSnapshot};
use tracing::warn;

use super::{AgentOrchestration, commit_config_change};

/// How long a one-shot run waits for its own provider's models before its
/// first turn. Past it the run starts on the fallback metadata and the
/// refresh lands whenever discovery does.
pub const ACTIVE_DISCOVERY_BOUND: Duration = Duration::from_secs(3);

/// The process's one model catalog. Discovery merges every provider's specs
/// into it, refreshes the agents of each attached orchestration and
/// republishes their sessions' model values.
#[derive(Clone, Default)]
pub struct ModelCatalog {
    specs: Arc<ArcSwapOption<Vec<String>>>,
    attached: Arc<Mutex<Vec<Weak<AgentOrchestration>>>>,
}

/// A running discovery; dropping it stops discovery.
pub struct Discovery {
    _task: smol::Task<()>,
    active: flume::Receiver<()>,
}

impl Discovery {
    /// A discovery that fetches nothing.
    pub fn idle() -> Self {
        Self {
            _task: smol::spawn(async {}),
            active: flume::bounded(0).1,
        }
    }

    /// Waits until the active provider's models are applied or every
    /// provider has answered, but no longer than {bound}.
    pub async fn finish(self) {
        self._task.await;
    }

    pub async fn wait_active(&self, bound: Duration) {
        futures_lite::future::or(
            async {
                let _ = self.active.recv_async().await;
            },
            async {
                smol::Timer::after(bound).await;
            },
        )
        .await;
    }
}

impl ModelCatalog {
    /// Seeded with the specs known without a network round trip.
    pub fn offline(policy: &ModelPolicy) -> Self {
        let catalog = Self::default();
        catalog
            .specs
            .store(Some(Arc::new(available_model_specs(policy))));
        catalog
    }

    pub fn apply_provider_snapshot(
        &self,
        providers: &ProviderSnapshot,
        policy: &ModelPolicy,
    ) -> impl Future<Output = ()> + Send + use<> {
        let models = providers
            .providers()
            .flat_map(|provider| {
                provider
                    .models
                    .iter()
                    .map(|model| format!("{}/{}", provider.id, model.id))
            })
            .filter(|spec| policy.allows(spec))
            .collect();
        self.apply(models)
    }

    pub fn specs(&self) -> Arc<ArcSwapOption<Vec<String>>> {
        Arc::clone(&self.specs)
    }

    pub fn snapshot(&self) -> Vec<Arc<str>> {
        self.specs
            .load()
            .iter()
            .flat_map(|specs| specs.iter())
            .map(|spec| Arc::from(spec.as_str()))
            .collect()
    }

    pub fn attach(&self, orchestration: &Arc<AgentOrchestration>) {
        let mut attached = self.attached.lock().unwrap_or_else(|e| e.into_inner());
        attached.retain(|weak| weak.strong_count() > 0);
        attached.push(Arc::downgrade(orchestration));
    }

    /// Discovers every provider's models; see [`Self::discover_from`].
    pub fn discover(
        &self,
        policy: Arc<ModelPolicy>,
        active: String,
        registry: Arc<ProviderRegistry>,
        applied: impl FnMut(Vec<String>) + Send + 'static,
    ) -> Discovery {
        self.discover_from(
            async move |mut on_ready| {
                let snapshot = registry.snapshot();
                let configs = maki_config::providers::ProvidersConfig::load_or_default();
                let (tx, rx) = flume::unbounded();
                let legacy_tx = tx.clone();
                let exclusions = snapshot.providers().map(|provider| provider.id.clone()).collect();
                let legacy = async {
                    fetch_all_models_excluding(&policy, &exclusions, move |batch| {
                        let _ = legacy_tx.send(batch);
                    })
                    .await;
                };
                let registered = async {
                    let jobs = snapshot.providers().map(|provider| {
                        let tx = tx.clone();
                        let registry = Arc::clone(&registry);
                        let snapshot = Arc::clone(&snapshot);
                        let config = configs.providers.get(&provider.id)
                            .and_then(|config| serde_json::to_value(config).ok())
                            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
                        let policy = Arc::clone(&policy);
                        async move {
                            let result = provider.factory.discover(&config).await;
                            let mut batch = ModelBatch {
                                provider: Some(provider.id.clone()),
                                models: Vec::new(),
                                warnings: Vec::new(),
                            };
                            match result {
                                Ok(Some(mut models)) => {
                                    for curated in &provider.models {
                                        if let Some(discovered) = models.iter_mut().find(|model| model.id == curated.id) {
                                            discovered.context_window = curated.context_window.or(discovered.context_window);
                                            discovered.max_output_tokens = curated.max_output_tokens.or(discovered.max_output_tokens);
                                            discovered.pricing = curated.pricing.clone().or(discovered.pricing.clone());
                                            discovered.supports_thinking = curated.supports_thinking.or(discovered.supports_thinking);
                                            discovered.supports_vision = curated.supports_vision.or(discovered.supports_vision);
                                            discovered.tier = curated.tier.or(discovered.tier);
                                            discovered.provider_info = curated.provider_info.clone().or(discovered.provider_info.clone());
                                        } else {
                                            models.push(curated.clone());
                                        }
                                    }
                                    let specs = models.iter().map(|model| format!("{}/{}", provider.id, model.id))
                                        .filter(|spec| policy.allows(spec)).collect();
                                    match registry.publish_models(&snapshot, &provider.id, models) {
                                        Ok(()) => batch.models = specs,
                                        Err(error) => warn!(provider = %provider.id, %error, "failed to publish discovered models"),
                                    }
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    warn!(provider = %provider.id, %error, "failed to discover models");
                                    batch.warnings.push(format!("{}: {error}", provider.display_name));
                                }
                            }
                            let _ = tx.send(batch);
                        }
                    });
                    futures::future::join_all(jobs).await;
                    drop(tx);
                };
                let receive = async {
                    while let Ok(batch) = rx.recv_async().await {
                        on_ready(batch);
                    }
                };
                futures_lite::future::zip(futures_lite::future::zip(legacy, registered), receive).await;
            },
            active,
            applied,
        )
    }

    /// Applies each batch {fetch} lists; {applied} gets each batch's
    /// warnings once the batch is in the catalog. Refreshes run one batch at
    /// a time and stop with the discovery. {active} names the provider
    /// [`Discovery::wait_active`] waits for.
    pub fn discover_from<F>(
        &self,
        fetch: impl FnOnce(Box<dyn FnMut(ModelBatch) + Send>) -> F + Send + 'static,
        active: String,
        mut applied: impl FnMut(Vec<String>) + Send + 'static,
    ) -> Discovery
    where
        F: Future<Output = ()> + Send,
    {
        let catalog = self.clone();
        let (ready, active_rx) = flume::bounded::<()>(0);
        let task = smol::spawn(async move {
            let (landed, refreshes) = flume::unbounded();
            let fetch = fetch(Box::new(move |batch| {
                let active = batch.provider.as_deref() == Some(active.as_str());
                let _ = landed.send((catalog.apply(batch.models), active));
                applied(batch.warnings);
            }));
            let mut ready = Some(ready);
            let refresh = async {
                while let Ok((refresh, active)) = refreshes.recv_async().await {
                    refresh.await;
                    ready.take_if(|_| active);
                }
            };
            futures_lite::future::zip(fetch, refresh).await;
        });
        Discovery {
            _task: task,
            active: active_rx,
        }
    }

    /// Merges {models} in; the returned future commits a metadata refresh
    /// for every attached agent whose model resolves differently now and,
    /// when the catalog grew, republishes each attached session's model
    /// values. It reads committed configs only after the batch's metadata is
    /// registered, and a change in flight re-resolves when it commits, so
    /// neither side misses the other.
    pub fn apply(&self, models: Vec<String>) -> impl Future<Output = ()> + Send + use<> {
        let mut grown = false;
        self.specs.rcu(|current| {
            let known = current.as_deref().map_or(&[][..], Vec::as_slice);
            let fresh = models.iter().filter(|spec| !known.contains(spec));
            let mut merged = known.to_vec();
            merged.extend(fresh.cloned());
            grown = merged.len() > known.len();
            if current.is_some() && !grown {
                return current.clone();
            }
            Some(Arc::new(merged))
        });
        let specs = grown.then(|| self.snapshot());
        let orchestrations = self
            .attached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        async move {
            for orchestration in orchestrations {
                for root in orchestration.roots() {
                    for target in root.target.manager().list() {
                        let Some(change) = target
                            .effective_config()
                            .ok()
                            .flatten()
                            .and_then(|config| ConfigChange::rediscovered(&config))
                        else {
                            continue;
                        };
                        let fixed = async |_, _| Ok((publication_claim(), change));
                        if let Err(error) =
                            commit_config_change(&*orchestration, &target, fixed).await
                        {
                            warn!(agent = %target.id(), %error, "failed to refresh discovered model");
                        }
                    }
                    let Some((coordinator, specs)) = root
                        .template
                        .session_id
                        .and_then(|session| SessionCoordinatorHandle::resolve(session.id()).ok())
                        .zip(specs.clone())
                    else {
                        continue;
                    };
                    if let Err(error) = coordinator.update_model_values(specs).await {
                        warn!(session = %coordinator.session_id(), %error, "failed to publish discovered models");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ModelBatch, ModelCatalog};
    use std::time::Duration;

    #[test]
    fn provider_snapshot_adds_only_allowed_models() {
        use maki_config::ModelPolicy;
        use maki_providers::AgentError;
        use maki_providers::model::{Model, ModelInfo};
        use maki_providers::provider::{BoxFuture, Provider};
        use maki_providers::provider_registry::{
            ProviderFactory, ProviderRegistration, ProviderRegistry,
        };
        use serde_json::Value;
        use std::sync::Arc;

        struct Factory;
        impl ProviderFactory for Factory {
            fn prepare<'a>(
                &'a self,
                _: &'a Model,
                _: &'a Value,
            ) -> BoxFuture<'a, Result<(Model, Box<dyn Provider>), AgentError>> {
                Box::pin(async { Err(AgentError::Cancelled) })
            }
        }

        let registry = ProviderRegistry::default();
        registry
            .register(ProviderRegistration {
                id: "lua-provider".into(),
                display_name: "Lua provider".into(),
                auth_methods: Vec::new(),
                models: vec![
                    ModelInfo::id_only("allowed".into()),
                    ModelInfo::id_only("blocked".into()),
                ],
                factory: Arc::new(Factory),
            })
            .unwrap();
        let policy = ModelPolicy::new(&["lua-provider/*".into()], &["*/blocked".into()]).unwrap();
        let catalog = ModelCatalog::default();
        smol::block_on(catalog.apply_provider_snapshot(&registry.snapshot(), &policy));
        smol::block_on(catalog.apply_provider_snapshot(&registry.snapshot(), &policy));
        assert_eq!(
            catalog.snapshot().as_slice(),
            &[Arc::<str>::from("lua-provider/allowed")]
        );
    }

    const TIMEOUT: Duration = Duration::from_secs(5);
    const ACTIVE: &str = "dynamic-provider";

    #[test]
    fn finish_waits_for_discovery_work() {
        let catalog = ModelCatalog::default();
        let (started, ready) = flume::bounded(1);
        let (release, gate) = flume::bounded::<()>(1);
        let discovery = catalog.discover_from(
            async move |_| {
                started.send(()).unwrap();
                gate.recv_async().await.unwrap();
            },
            ACTIVE.to_owned(),
            |_| {},
        );
        ready.recv_timeout(TIMEOUT).unwrap();
        let (finished, result) = flume::bounded(1);
        let thread = std::thread::spawn(move || {
            smol::block_on(discovery.finish());
            finished.send(()).unwrap();
        });
        assert!(result.try_recv().is_err());
        release.send(()).unwrap();
        result.recv_timeout(TIMEOUT).unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn discovery_matches_owned_dynamic_provider_identity() {
        let catalog = ModelCatalog::default();
        let (batches, incoming) = flume::unbounded::<ModelBatch>();
        let (applied, received) = flume::unbounded();
        let discovery = catalog.discover_from(
            async move |mut on_ready| {
                while let Ok(batch) = incoming.recv_async().await {
                    on_ready(batch);
                }
            },
            ACTIVE.to_owned(),
            move |warnings| {
                let _ = applied.send(warnings);
            },
        );

        for provider in [None, Some(format!("{ACTIVE}-other"))] {
            batches
                .send(ModelBatch {
                    provider,
                    models: vec![],
                    warnings: vec![],
                })
                .unwrap();
            received.recv_timeout(TIMEOUT).unwrap();
            assert!(matches!(
                discovery.active.try_recv(),
                Err(flume::TryRecvError::Empty)
            ));
        }

        let provider = ["dynamic", "provider"].join("-");
        let spec = format!("{provider}/model");
        batches
            .send(ModelBatch {
                provider: Some(provider),
                models: vec![spec.clone()],
                warnings: vec![],
            })
            .unwrap();
        received.recv_timeout(TIMEOUT).unwrap();
        assert!(matches!(
            discovery.active.recv_timeout(TIMEOUT),
            Err(flume::RecvTimeoutError::Disconnected)
        ));
        assert_eq!(catalog.snapshot().as_slice(), &[spec.into()]);
    }
}
