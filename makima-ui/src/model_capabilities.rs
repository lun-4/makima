use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use makima_providers::{ModelCapabilities, ProviderRegistry};
use tracing::debug;

type Resolve = Box<dyn Fn(&str) -> ModelCapabilities + Send + Sync>;
type Notify = Box<dyn Fn() + Send + Sync>;

/// Capabilities of models other than an agent's current one, keyed by spec.
///
/// Building a provider to adjust a model can read auth files, run an auth
/// script or fetch a catalog, so misses resolve on a blocking thread and
/// `notify` fires once the answer is stored.
pub(crate) struct ModelCapabilityCache {
    resolve: Resolve,
    notify: Notify,
    state: Mutex<CacheState>,
}

#[derive(Default)]
struct CacheState {
    epoch: u64,
    resolved: HashMap<Arc<str>, ModelCapabilities>,
    pending: HashSet<Arc<str>>,
}

impl ModelCapabilityCache {
    pub(crate) fn new(
        resolve: impl Fn(&str) -> ModelCapabilities + Send + Sync + 'static,
        notify: impl Fn() + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            resolve: Box::new(resolve),
            notify: Box::new(notify),
            state: Mutex::default(),
        })
    }

    pub(crate) fn provider_adjusted(
        providers: Arc<ProviderRegistry>,
        notify: impl Fn() + Send + Sync + 'static,
    ) -> Arc<Self> {
        Self::new(move |spec| provider_adjusted(spec, &providers), notify)
    }

    /// Returns the cached capabilities of `spec`, or starts resolving them.
    pub(crate) fn get(self: &Arc<Self>, spec: &str) -> Option<ModelCapabilities> {
        let mut state = self.lock();
        if let Some(capabilities) = state.resolved.get(spec) {
            return Some(*capabilities);
        }
        let spec: Arc<str> = Arc::from(spec);
        if state.pending.insert(Arc::clone(&spec)) {
            let epoch = state.epoch;
            let cache = Arc::clone(self);
            smol::unblock(move || {
                let capabilities = (cache.resolve)(&spec);
                cache.store(spec, epoch, capabilities);
            })
            .detach();
        }
        None
    }

    /// Returns the capabilities of `spec`, waiting for them if needed.
    pub(crate) async fn resolve(self: &Arc<Self>, spec: &str) -> ModelCapabilities {
        let epoch = {
            let state = self.lock();
            if let Some(capabilities) = state.resolved.get(spec) {
                return *capabilities;
            }
            state.epoch
        };
        let cache = Arc::clone(self);
        let spec: Arc<str> = Arc::from(spec);
        smol::unblock(move || {
            let capabilities = (cache.resolve)(&spec);
            cache.store(spec, epoch, capabilities);
            capabilities
        })
        .await
    }

    /// Forgets everything, for when discovery or provider auth changes what
    /// a spec resolves to. Resolutions already running are discarded.
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        state.epoch = state.epoch.wrapping_add(1);
        state.resolved.clear();
        state.pending.clear();
    }

    fn store(&self, spec: Arc<str>, epoch: u64, capabilities: ModelCapabilities) {
        {
            let mut state = self.lock();
            if state.epoch != epoch {
                return;
            }
            state.pending.remove(&spec);
            state.resolved.insert(spec, capabilities);
        }
        (self.notify)();
    }

    fn lock(&self) -> MutexGuard<'_, CacheState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// Falls back to the static capabilities when the provider cannot be built,
/// so a broken provider shows what the model table knows instead of loading
/// forever. Applying the selection still validates against the real provider.
fn provider_adjusted(spec: &str, providers: &ProviderRegistry) -> ModelCapabilities {
    let Ok(model) = providers.model(spec) else {
        return ModelCapabilities::NONE;
    };
    match smol::block_on(providers.prepare(model.clone())) {
        Ok(prepared) => ModelCapabilities::from(&prepared.model),
        Err(error) => {
            debug!(spec, %error, "model capabilities fall back to static metadata");
            ModelCapabilities::from(&model)
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Arc;

    use makima_providers::{ModelCapabilities, ThinkingSupport};

    use super::ModelCapabilityCache;

    pub(crate) const THINKING_ONLY: ModelCapabilities = ModelCapabilities {
        thinking: ThinkingSupport::Yes,
        fast: false,
    };
    pub(crate) const ALL_OPTIONS: ModelCapabilities = ModelCapabilities {
        thinking: ThinkingSupport::Yes,
        fast: true,
    };

    /// A cache holding `resolved`, with `pending` resolving forever, so
    /// tests never wait on a blocking thread.
    pub(crate) fn seeded(
        resolved: &[(&str, ModelCapabilities)],
        pending: &[&str],
    ) -> Arc<ModelCapabilityCache> {
        let cache = ModelCapabilityCache::new(|_| ModelCapabilities::NONE, || {});
        let mut state = cache.lock();
        for (spec, capabilities) in resolved {
            state.resolved.insert(Arc::from(*spec), *capabilities);
        }
        state
            .pending
            .extend(pending.iter().map(|spec| Arc::from(*spec)));
        drop(state);
        cache
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use makima_providers::{ModelCapabilities, ThinkingSupport};

    use super::ModelCapabilityCache;
    use super::test_support::THINKING_ONLY;

    const SPEC: &str = "test/model";
    const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);

    struct Gate {
        cache: Arc<ModelCapabilityCache>,
        started: flume::Receiver<flume::Sender<ModelCapabilities>>,
        notified: flume::Receiver<()>,
    }

    impl Gate {
        /// The answer channel of the next resolution once it starts. Each
        /// resolution has its own, so a stale one can never take the answer
        /// meant for a fresh one.
        fn next_resolution(&self) -> flume::Sender<ModelCapabilities> {
            self.started.recv_timeout(RESOLVE_TIMEOUT).unwrap()
        }
    }

    /// Each resolution announces itself with its answer channel, then waits.
    fn gate() -> Gate {
        let (started_tx, started) = flume::unbounded();
        let (notified_tx, notified) = flume::unbounded();
        let cache = ModelCapabilityCache::new(
            move |_| {
                let (answer, answer_rx) = flume::bounded(1);
                started_tx.send(answer).unwrap();
                answer_rx.recv().unwrap()
            },
            move || notified_tx.send(()).unwrap(),
        );
        Gate {
            cache,
            started,
            notified,
        }
    }

    #[test]
    fn miss_resolves_once_then_hits() {
        let gate = gate();
        assert_eq!(gate.cache.get(SPEC), None);
        assert_eq!(gate.cache.get(SPEC), None);
        gate.next_resolution().send(THINKING_ONLY).unwrap();
        gate.notified.recv_timeout(RESOLVE_TIMEOUT).unwrap();
        assert_eq!(gate.cache.get(SPEC), Some(THINKING_ONLY));
        assert!(gate.started.try_recv().is_err());
    }

    #[test]
    fn clear_discards_resolution_in_flight() {
        let gate = gate();
        assert_eq!(gate.cache.get(SPEC), None);
        let stale = gate.next_resolution();
        gate.cache.clear();
        stale.send(ModelCapabilities::NONE).unwrap();
        assert_eq!(gate.cache.get(SPEC), None);
        let fresh = ModelCapabilities {
            thinking: ThinkingSupport::Required,
            fast: true,
        };
        gate.next_resolution().send(fresh).unwrap();
        gate.notified.recv_timeout(RESOLVE_TIMEOUT).unwrap();
        assert_eq!(gate.cache.get(SPEC), Some(fresh));
    }
}
