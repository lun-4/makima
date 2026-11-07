use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use maki_agent::actor::{ConfigChange, ConfigPatch, EffectiveAgentConfig};
use maki_agent::session_coordinator::{SessionCoordinatorHandle, publication_claim};
use maki_config::ModelPolicy;
use maki_providers::provider::ModelBatch;
use maki_providers::{Model, ProviderRegistry};
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
    providers: Arc<ProviderRegistry>,
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
    pub fn offline(providers: Arc<ProviderRegistry>, policy: &ModelPolicy) -> Self {
        let catalog = Self {
            providers,
            ..Self::default()
        };
        catalog
            .specs
            .store(Some(Arc::new(catalog.providers.offline_specs(policy))));
        catalog
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
        applied: impl FnMut(Vec<String>) + Send + 'static,
    ) -> Discovery {
        let providers = Arc::clone(&self.providers);
        self.discover_from(
            async move |on_ready| providers.fetch_all(&policy, false, on_ready).await,
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
                let _ = landed.send((catalog.apply(batch.models, batch.provider), active));
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

    /// Merges {models}, listed by {provider} when one, in; the returned
    /// future commits a metadata refresh for every attached agent on a listed
    /// spec or on {provider} whose model prepares differently now and, when
    /// the catalog grew, republishes each attached session's model values.
    /// A refresh is decided before its change is reserved, so an agent that
    /// is already right is never held; one that a change in flight overtook
    /// is refreshed from the config it commits over rather than lost.
    pub fn apply(
        &self,
        models: Vec<String>,
        provider: Option<Arc<str>>,
    ) -> impl Future<Output = ()> + Send + use<> {
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
        let providers = Arc::clone(&self.providers);
        let listed = move |model: &Model| {
            provider.as_deref() == Some(&*model.provider) || models.contains(&model.spec())
        };
        async move {
            for orchestration in orchestrations {
                for root in orchestration.roots() {
                    for target in root.target.manager().list() {
                        let Some(committed) = target
                            .effective_config()
                            .ok()
                            .flatten()
                            .filter(|config| listed(&config.model))
                        else {
                            continue;
                        };
                        let Some(change) = ConfigChange::rediscovered(&committed, &providers).await
                        else {
                            continue;
                        };
                        let refresh = async |_, predecessor: Arc<EffectiveAgentConfig>| {
                            let change = if same_target(&committed, &predecessor) {
                                Some(change)
                            } else if listed(&predecessor.model) {
                                ConfigChange::rediscovered(&predecessor, &providers).await
                            } else {
                                None
                            };
                            let unchanged = || ConfigChange::Patch(ConfigPatch::default());
                            Ok((publication_claim(), change.unwrap_or_else(unchanged)))
                        };
                        if let Err(error) =
                            commit_config_change(&*orchestration, &target, refresh).await
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

fn same_target(left: &EffectiveAgentConfig, right: &EffectiveAgentConfig) -> bool {
    Arc::ptr_eq(&left.provider, &right.provider) && left.model.spec() == right.model.spec()
}
