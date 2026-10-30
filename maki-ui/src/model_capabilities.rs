use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use maki_agent::{
    actor::{ConfigCommit, rediscovered_model},
    config_change::prepare_model,
    session_coordinator::ModelPreparer,
};
use maki_commands::CancellationToken;
use maki_domain::ModelCapabilities;
use maki_lua::orchestration::TrustedTarget;

pub(crate) const MAX_RETAINED_PROJECTIONS: usize = 64;
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone)]
pub(crate) struct ModelCapabilityProjection {
    pub(crate) spec: Arc<str>,
    pub(crate) capabilities: ModelCapabilities,
    pub(crate) source_identity: Arc<()>,
    pub(crate) source_generation: u64,
    pub(crate) metadata_revision: u64,
    request: Arc<()>,
}

struct ProjectionEntry {
    target: TrustedTarget,
    projection: ModelCapabilityProjection,
    ready: bool,
    waiters: Vec<flume::Sender<ModelCapabilityProjection>>,
}

#[derive(Default)]
struct ResolverState {
    revision: u64,
    entries: VecDeque<ProjectionEntry>,
}

pub(crate) struct ModelCapabilityResolver {
    prepare: ModelPreparer,
    state: Mutex<ResolverState>,
}

impl ModelCapabilityResolver {
    pub(crate) fn new(prepare: ModelPreparer) -> Self {
        Self {
            prepare,
            state: Mutex::default(),
        }
    }

    pub(crate) fn revision(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .revision
    }

    pub(crate) fn invalidate_metadata(&self) -> u64 {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.revision = state.revision.wrapping_add(1);
        state.entries.clear();
        state.revision
    }

    #[cfg(test)]
    pub(crate) fn invalidate_target(&self, target: &TrustedTarget) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .retain(|entry| !same_target(&entry.target, target));
    }

    pub(crate) fn is_current(
        &self,
        target: &TrustedTarget,
        projection: &ModelCapabilityProjection,
    ) -> bool {
        if !valid_source(target, projection) {
            return false;
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.revision == projection.metadata_revision
            && state.entries.iter().any(|entry| {
                same_target(&entry.target, target)
                    && Arc::ptr_eq(&entry.projection.request, &projection.request)
            })
    }

    pub(crate) async fn resolve(
        self: &Arc<Self>,
        target: TrustedTarget,
        candidate: Option<Arc<str>>,
        metadata_revision: u64,
        cancellation: CancellationToken,
    ) -> Option<ModelCapabilityProjection> {
        if cancellation.is_cancelled() {
            return None;
        }
        target.target.validate_live().ok()?;
        let source = target.target.actor().ok()?.config_snapshot()?;
        let spec = candidate.unwrap_or_else(|| Arc::from(source.config.model.spec()));
        if !target.template.model_policy.allows(&spec) {
            return None;
        }
        let (projection, receiver, start) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.revision != metadata_revision {
                return None;
            }
            state
                .entries
                .retain(|entry| valid_source(&entry.target, &entry.projection));
            if let Some(entry) = state.entries.iter_mut().find(|entry| {
                same_target(&entry.target, &target)
                    && entry.projection.spec == spec
                    && same_source(&source, &entry.projection)
            }) {
                if entry.ready {
                    (entry.projection.clone(), None, false)
                } else {
                    let (tx, rx) = flume::bounded(1);
                    entry.waiters.retain(|waiter| !waiter.is_disconnected());
                    entry.waiters.push(tx);
                    (entry.projection.clone(), Some(rx), false)
                }
            } else {
                let projection = ModelCapabilityProjection {
                    spec,
                    capabilities: ModelCapabilities::default(),
                    source_identity: Arc::clone(&source.identity),
                    source_generation: source.generation,
                    metadata_revision,
                    request: Arc::new(()),
                };
                let (tx, rx) = flume::bounded(1);
                if state.entries.len() == MAX_RETAINED_PROJECTIONS {
                    state.entries.pop_front();
                }
                state.entries.push_back(ProjectionEntry {
                    target: target.clone(),
                    projection: projection.clone(),
                    ready: false,
                    waiters: vec![tx],
                });
                (projection, Some(rx), true)
            }
        };
        if start {
            let resolver = Arc::clone(self);
            let target = target.clone();
            let projection = projection.clone();
            smol::spawn(async move {
                let capabilities = if source.config.model.spec() == projection.spec.as_ref() {
                    smol::unblock(move || {
                        let model = (metadata_revision != 0)
                            .then(|| {
                                rediscovered_model(&*source.config.provider, &source.config.model)
                            })
                            .flatten()
                            .unwrap_or_else(|| source.config.model.clone());
                        ModelCapabilities::from(&model)
                    })
                    .await
                } else {
                    let spec = Arc::clone(&projection.spec);
                    let policy = target.template.model_policy.clone();
                    let prepare = Arc::clone(&resolver.prepare);
                    smol::unblock(move || {
                        smol::block_on(async {
                            prepare_model(&spec, &policy, &prepare)
                                .await
                                .map(|prepared| ModelCapabilities::from(&prepared.model))
                                .unwrap_or_default()
                        })
                    })
                    .await
                };
                if !resolver.is_current(&target, &projection) {
                    return;
                }
                let mut state = resolver
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if state.revision != projection.metadata_revision
                    || !valid_source(&target, &projection)
                {
                    return;
                }
                if let Some(entry) = state
                    .entries
                    .iter_mut()
                    .find(|entry| Arc::ptr_eq(&entry.projection.request, &projection.request))
                {
                    entry.projection.capabilities = capabilities;
                    entry.ready = true;
                    for waiter in entry.waiters.drain(..) {
                        let _ = waiter.send(entry.projection.clone());
                    }
                }
            })
            .detach();
        }
        let result = if let Some(receiver) = receiver {
            smol::future::or(async { receiver.recv_async().await.ok() }, async {
                loop {
                    if cancellation.is_cancelled() || !self.is_current(&target, &projection) {
                        return None;
                    }
                    smol::Timer::after(CANCELLATION_POLL_INTERVAL).await;
                }
            })
            .await?
        } else {
            projection
        };
        (!cancellation.is_cancelled() && self.is_current(&target, &result)).then_some(result)
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::{future::Future, pin::Pin, sync::Arc};

    use maki_agent::{
        ActorBackend, AgentInput, AgentManagerHandle, AgentMode, BackendResult, ControlWork,
        History, RunSettings, TurnContext, WorkKind,
        actor::{EffectiveAgentConfig, PreparedModel},
        session_coordinator::ModelPreparer,
    };
    use maki_lua::orchestration::TrustedTarget;
    use maki_providers::{
        AgentError, Message, Model, ModelInfo, ProviderEvent, RequestOptions, StreamResponse,
        model::{FastSupport, ThinkingSupport},
        provider::{BoxFuture, Provider},
    };
    use maki_storage::id::SessionRef;
    use serde_json::Value;

    pub(crate) const CURRENT_SPEC: &str = "anthropic/claude-sonnet-4-20250514";
    pub(crate) const OTHER_SPEC: &str = "openai/gpt-5";
    pub(crate) const FAILED_PREPARATION: &str = "fixture preparation failed";

    struct IdleBackend;

    impl ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(std::future::pending())
        }
        fn run_control<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::ControlDone })
        }
        fn run_compact<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::CompactDone })
        }
    }

    pub(crate) struct FixtureProvider;

    impl Provider for FixtureProvider {
        fn adjust_model(&self, model: &mut Model) {
            if model.provider.as_ref() != "synthetic" {
                model.thinking_override = Some(ThinkingSupport::Yes);
                model.supports_fast_override = Some(FastSupport::Supported);
            }
        }

        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async { unreachable!("capability resolution must not request a model turn") })
        }
        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    pub(crate) fn model(spec: &str, thinking: bool, fast: FastSupport) -> Model {
        let mut model = Model::from_spec(spec).unwrap();
        model.thinking_override = Some(if thinking {
            ThinkingSupport::Yes
        } else {
            ThinkingSupport::No
        });
        model.supports_fast_override = Some(fast);
        model
    }

    pub(crate) fn target(model: Model) -> TrustedTarget {
        let target = AgentManagerHandle::new(Default::default())
            .unwrap()
            .create_root(Vec::new(), None, Box::new(IdleBackend))
            .unwrap();
        target
            .actor()
            .unwrap()
            .initialize_config(EffectiveAgentConfig::new(
                RunSettings {
                    provider: Arc::new(FixtureProvider),
                    model,
                    thinking: Default::default(),
                    fast: false,
                    workflow: false,
                },
                AgentMode::Build,
            ))
            .unwrap();
        TrustedTarget {
            target,
            template: maki_agent::tools::cli_tool_ctx(std::env::current_dir().unwrap()),
        }
    }

    pub(crate) fn preparer() -> ModelPreparer {
        Arc::new(|model| {
            Box::pin(async move {
                Ok(PreparedModel {
                    provider: Arc::new(FixtureProvider),
                    model,
                })
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use maki_agent::actor::PreparedModel;
    use maki_commands::CancellationToken;
    use maki_config::ModelPolicy;
    use maki_domain::{FastCapability, ThinkingCapability};
    use maki_providers::model::FastSupport;
    use test_case::test_case;

    use super::{
        MAX_RETAINED_PROJECTIONS, ModelCapabilityResolver,
        test_support::{CURRENT_SPEC, FixtureProvider, OTHER_SPEC, model, preparer, target},
    };

    #[test]
    fn current_model_uses_provider_adjusted_snapshot_without_preparation() {
        smol::block_on(async {
            let target = target(model(CURRENT_SPEC, true, FastSupport::Supported));
            let resolver = Arc::new(ModelCapabilityResolver::new(Arc::new(|_| {
                panic!("current identity must reuse its provider-adjusted model")
            })));
            let before = target.target.actor().unwrap().config_snapshot().unwrap();
            for candidate in [None, Some(Arc::from(CURRENT_SPEC))] {
                let result = resolver
                    .resolve(target.clone(), candidate, 0, CancellationToken::default())
                    .await
                    .unwrap();
                assert_eq!(result.capabilities.thinking, ThinkingCapability::Supported);
                assert_eq!(result.capabilities.fast, FastCapability::Supported);
                assert!(Arc::ptr_eq(&result.source_identity, &before.identity));
            }
            let after = target.target.actor().unwrap().config_snapshot().unwrap();
            assert!(Arc::ptr_eq(&before.identity, &after.identity));
            assert_eq!(before.generation, after.generation);
            target.target.close_subtree().unwrap();
        });
    }

    #[test]
    fn concurrent_consumers_share_preparation_and_metadata_invalidates() {
        smol::block_on(async {
            let target = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let calls = Arc::new(Mutex::new(Vec::new()));
            let (entered_tx, entered_rx) = flume::unbounded();
            let (gate_tx, gate_rx) = flume::unbounded();
            let resolver = Arc::new(ModelCapabilityResolver::new(Arc::new({
                let calls = Arc::clone(&calls);
                move |model| {
                    calls.lock().unwrap().push(model.spec());
                    entered_tx.send(()).unwrap();
                    let gate = gate_rx.clone();
                    Box::pin(async move {
                        gate.recv_async().await.unwrap();
                        Ok(PreparedModel {
                            provider: Arc::new(FixtureProvider),
                            model,
                        })
                    })
                }
            })));
            let request = || {
                let resolver = Arc::clone(&resolver);
                let target = target.clone();
                smol::spawn(async move {
                    resolver
                        .resolve(
                            target,
                            Some(Arc::from(OTHER_SPEC)),
                            resolver.revision(),
                            CancellationToken::default(),
                        )
                        .await
                })
            };
            let first = request();
            entered_rx.recv_async().await.unwrap();
            let second = request();
            smol::future::yield_now().await;
            gate_tx.send(()).unwrap();
            let first = first.await.unwrap();
            let second = second.await.unwrap();
            assert!(Arc::ptr_eq(&first.request, &second.request));
            assert_eq!(*calls.lock().unwrap(), [OTHER_SPEC]);
            let revision = resolver.invalidate_metadata();
            assert!(!resolver.is_current(&target, &first));
            let next = request();
            entered_rx.recv_async().await.unwrap();
            gate_tx.send(()).unwrap();
            assert_eq!(next.await.unwrap().metadata_revision, revision);
            assert_eq!(calls.lock().unwrap().len(), 2);
            target.target.close_subtree().unwrap();
        });
    }

    #[test_case(false; "denied_cache_hit")]
    #[test_case(true; "closed_cache_hit")]
    fn cache_hits_recheck_target_and_policy(close: bool) {
        smol::block_on(async {
            let mut target = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let resolver = Arc::new(ModelCapabilityResolver::new(preparer()));
            assert!(
                resolver
                    .resolve(
                        target.clone(),
                        Some(Arc::from(OTHER_SPEC)),
                        0,
                        CancellationToken::default()
                    )
                    .await
                    .is_some()
            );
            if close {
                target.target.close_subtree().unwrap();
            } else {
                target.template.model_policy =
                    Arc::new(ModelPolicy::new(&[CURRENT_SPEC.into()], &[]).unwrap());
            }
            assert!(
                resolver
                    .resolve(
                        target.clone(),
                        Some(Arc::from(OTHER_SPEC)),
                        0,
                        CancellationToken::default()
                    )
                    .await
                    .is_none()
            );
            if !close {
                target.target.close_subtree().unwrap();
            }
        });
    }

    #[test]
    fn projections_do_not_cross_manager_or_target_boundaries() {
        smol::block_on(async {
            let first = target(model(CURRENT_SPEC, true, FastSupport::Supported));
            let second = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let calls = Arc::new(Mutex::new(Vec::new()));
            let resolver = Arc::new(ModelCapabilityResolver::new(Arc::new({
                let calls = Arc::clone(&calls);
                move |model| {
                    calls.lock().unwrap().push(model.spec());
                    Box::pin(async move {
                        Ok(PreparedModel {
                            provider: Arc::new(FixtureProvider),
                            model,
                        })
                    })
                }
            })));
            let first_current = resolver
                .resolve(first.clone(), None, 0, CancellationToken::default())
                .await
                .unwrap();
            let second_current = resolver
                .resolve(second.clone(), None, 0, CancellationToken::default())
                .await
                .unwrap();
            assert_eq!(
                first_current.capabilities.thinking,
                ThinkingCapability::Supported
            );
            assert_eq!(
                second_current.capabilities.thinking,
                ThinkingCapability::Unsupported
            );
            assert!(!resolver.is_current(&first, &second_current));
            for target in [&first, &second] {
                resolver
                    .resolve(
                        target.clone(),
                        Some(Arc::from(OTHER_SPEC)),
                        0,
                        CancellationToken::default(),
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(*calls.lock().unwrap(), [OTHER_SPEC, OTHER_SPEC]);
            first.target.close_subtree().unwrap();
            second.target.close_subtree().unwrap();
        });
    }

    #[test]
    fn invalidated_inflight_results_cannot_repopulate_cache() {
        smol::block_on(async {
            let target = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (gate_tx, gate_rx) = flume::bounded(1);
            let resolver = Arc::new(ModelCapabilityResolver::new(Arc::new(move |model| {
                entered_tx.send(()).unwrap();
                let gate = gate_rx.clone();
                Box::pin(async move {
                    gate.recv_async().await.unwrap();
                    Ok(PreparedModel {
                        provider: Arc::new(FixtureProvider),
                        model,
                    })
                })
            })));
            let request = smol::spawn({
                let target = target.clone();
                let resolver = Arc::clone(&resolver);
                async move {
                    resolver
                        .resolve(
                            target,
                            Some(Arc::from(OTHER_SPEC)),
                            0,
                            CancellationToken::default(),
                        )
                        .await
                }
            });
            entered_rx.recv_async().await.unwrap();
            resolver.invalidate_metadata();
            assert!(request.await.is_none());
            gate_tx.send(()).unwrap();
            assert!(resolver.state.lock().unwrap().entries.is_empty());
            target.target.close_subtree().unwrap();
        });
    }

    #[test]
    fn committed_source_generation_retires_cached_projection() {
        smol::block_on(async {
            let target = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let resolver = Arc::new(ModelCapabilityResolver::new(preparer()));
            let first = resolver
                .resolve(
                    target.clone(),
                    Some(Arc::from(OTHER_SPEC)),
                    0,
                    CancellationToken::default(),
                )
                .await
                .unwrap();
            target
                .target
                .actor()
                .unwrap()
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(maki_agent::actor::ConfigChange::Patch(
                    maki_agent::actor::ConfigPatch {
                        workflow: Some(true),
                        ..Default::default()
                    },
                )))
                .await
                .unwrap();
            assert!(!resolver.is_current(&target, &first));
            let next = resolver
                .resolve(
                    target.clone(),
                    Some(Arc::from(OTHER_SPEC)),
                    0,
                    CancellationToken::default(),
                )
                .await
                .unwrap();
            assert!(next.source_generation > first.source_generation);
            assert_eq!(resolver.state.lock().unwrap().entries.len(), 1);
            target.target.close_subtree().unwrap();
        });
    }

    #[test]
    fn same_identity_rediscovery_precedes_actor_refresh() {
        smol::block_on(async {
            const PROVIDER: &str = "synthetic";
            const MODEL_ID: &str = "ui-capability-rediscovery";
            const SPEC: &str = "synthetic/ui-capability-rediscovery";
            let target = target(model(SPEC, false, FastSupport::Unsupported));
            let resolver = Arc::new(ModelCapabilityResolver::new(Arc::new(|_| {
                panic!("rediscovery must reuse the pinned provider")
            })));
            let first = resolver
                .resolve(target.clone(), None, 0, CancellationToken::default())
                .await
                .unwrap();
            assert_eq!(first.capabilities.thinking, ThinkingCapability::Unsupported);
            maki_providers::model_registry::set_known_models(
                PROVIDER,
                vec![maki_providers::ModelInfo {
                    supports_thinking: Some(true),
                    ..maki_providers::ModelInfo::id_only(MODEL_ID.into())
                }],
            );
            let revision = resolver.invalidate_metadata();
            let next = resolver
                .resolve(target.clone(), None, revision, CancellationToken::default())
                .await
                .unwrap();
            assert_eq!(next.capabilities.thinking, ThinkingCapability::Supported);
            assert_eq!(next.source_generation, first.source_generation);
            assert!(
                !target
                    .target
                    .actor()
                    .unwrap()
                    .config_snapshot()
                    .unwrap()
                    .config
                    .model
                    .supports_thinking()
            );
            target.target.close_subtree().unwrap();
        });
    }

    #[test]
    fn eviction_finishes_inflight_waiter_without_retaining_its_result() {
        const EVICTED_SPEC: &str = "anthropic/evicted-capability-preview";

        smol::block_on(async {
            let source = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let pressure = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let (entered, started) = flume::bounded(1);
            let (release, gate) = flume::unbounded();
            let (finished, completed) = flume::bounded(1);
            let resolver = Arc::new(ModelCapabilityResolver::new(Arc::new(move |model| {
                let entered = entered.clone();
                let gate = gate.clone();
                let finished = finished.clone();
                Box::pin(async move {
                    if model.spec() == EVICTED_SPEC {
                        entered.send(()).unwrap();
                        gate.recv_async().await.unwrap();
                        finished.send(()).unwrap();
                    }
                    Ok(PreparedModel {
                        model,
                        provider: Arc::new(FixtureProvider),
                    })
                })
            })));
            let pending = {
                let resolver = Arc::clone(&resolver);
                let source = source.clone();
                smol::spawn(async move {
                    resolver
                        .resolve(source, Some(Arc::from(EVICTED_SPEC)), 0, Default::default())
                        .await
                })
            };
            started.recv_async().await.unwrap();
            let evicted = resolver
                .state
                .lock()
                .unwrap()
                .entries
                .front()
                .unwrap()
                .projection
                .clone();
            for index in 0..MAX_RETAINED_PROJECTIONS {
                resolver
                    .resolve(
                        pressure.clone(),
                        Some(Arc::from(format!("openai/eviction-pressure-{index}"))),
                        0,
                        Default::default(),
                    )
                    .await
                    .unwrap();
            }
            assert!(pending.await.is_none());
            assert_eq!(
                resolver.state.lock().unwrap().entries.len(),
                MAX_RETAINED_PROJECTIONS
            );
            assert!(!resolver.is_current(&source, &evicted));
            release.send(()).unwrap();
            completed.recv_async().await.unwrap();
            assert!(!resolver.is_current(&source, &evicted));
            release.send(()).unwrap();
            let retried = resolver
                .resolve(
                    source.clone(),
                    Some(Arc::from(EVICTED_SPEC)),
                    0,
                    Default::default(),
                )
                .await
                .unwrap();
            assert!(!Arc::ptr_eq(&retried.request, &evicted.request));
            assert_eq!(
                resolver.state.lock().unwrap().entries.len(),
                MAX_RETAINED_PROJECTIONS
            );
            source.target.close_subtree().unwrap();
            pressure.target.close_subtree().unwrap();
        });
    }

    #[test]
    fn retained_projections_are_bounded_and_explicitly_invalidated() {
        smol::block_on(async {
            let target = target(model(CURRENT_SPEC, false, FastSupport::Unsupported));
            let resolver = Arc::new(ModelCapabilityResolver::new(preparer()));
            for index in 0..=MAX_RETAINED_PROJECTIONS {
                let spec = Arc::from(format!("openai/capability-cache-{index}"));
                resolver
                    .resolve(target.clone(), Some(spec), 0, CancellationToken::default())
                    .await
                    .unwrap();
            }
            assert_eq!(
                resolver.state.lock().unwrap().entries.len(),
                MAX_RETAINED_PROJECTIONS
            );
            resolver.invalidate_target(&target);
            assert!(resolver.state.lock().unwrap().entries.is_empty());
            target.target.close_subtree().unwrap();
        });
    }
}

fn same_target(left: &TrustedTarget, right: &TrustedTarget) -> bool {
    left.target.same_manager(&right.target) && left.target.id() == right.target.id()
}

fn same_source(source: &ConfigCommit, projection: &ModelCapabilityProjection) -> bool {
    Arc::ptr_eq(&source.identity, &projection.source_identity)
        && source.generation == projection.source_generation
}

fn valid_source(target: &TrustedTarget, projection: &ModelCapabilityProjection) -> bool {
    target.target.validate_live().is_ok()
        && target.template.model_policy.allows(&projection.spec)
        && target
            .target
            .actor()
            .ok()
            .and_then(|actor| actor.config_snapshot())
            .is_some_and(|source| same_source(&source, projection))
}
