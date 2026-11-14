use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;

use futures_lite::future::poll_once;
use makima_config::ModelPolicy;
use makima_providers::{Message, Model, TokenUsage, provider::unconfigured};
use makima_storage::{
    id::{MakimaId, SessionRef},
    session_state::{
        MAX_INLINE_TURN_RESULT_BYTES, MAX_STORED_TURN_RESULTS, SessionStateView, StateChange,
        StoredAgentLifecycle, StoredAgentRecord, StoredDeliveryOp,
    },
};
use tempfile::TempDir;
use test_case::test_case;

use crate::actor::{
    ActorHistory, DeliveryStatus, EffectiveAgentConfig, PreparedModel, TurnProvenance, TurnResult,
};
use crate::journal::{JournalChange, JournalCommit, SessionJournal};
use crate::modes::ModeRegistry;
use crate::session_coordinator::ModelPreparer;
use crate::tools::offload::OffloadStore;
use crate::{
    ActorBackend, ActorEvent, AgentContextSnapshot, AgentId, AgentInput, AgentMode, BackendResult,
    ControlWork, HistorySnapshot, RunSettings, TurnContext, TurnOutcome, WorkKind,
};

use super::{
    AgentLimits, AgentManagerHandle, AgentMetadata, AgentResidency, GraphLifecycle, ManagerError,
    ReactivationError, ResidentAgent,
};

const TRANSCRIPT: &str = "rewind-child";
const INPUT: &str = "rewind admission";
const HISTORICAL_LABEL: &str = "historical";
const MODEL: &str = "test/rewind-model";
const TURN_USAGE: TokenUsage = TokenUsage {
    input: 7,
    output: 3,
    cache_creation: 2,
    cache_read: 1,
};
const LEGACY_USAGE: TokenUsage = TokenUsage {
    input: 101,
    output: 51,
    cache_creation: 21,
    cache_read: 11,
};

struct RewindBackend {
    entered: Option<flume::Sender<()>>,
    release: Option<flume::Receiver<()>>,
}

impl RewindBackend {
    fn idle() -> Box<dyn ActorBackend> {
        Box::new(Self {
            entered: None,
            release: None,
        })
    }
}

impl ActorBackend for RewindBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(entered) = &self.entered {
                entered.send(()).unwrap();
            }
            if let Some(release) = &self.release
                && let Some(reason) = futures_lite::future::or(
                    async {
                        let _ = release.recv_async().await;
                        None
                    },
                    async { Some(context.cancel_reason.cancelled().await) },
                )
                .await
            {
                return BackendResult::EnteredRun(TurnOutcome::cancelled(
                    context.agent_id,
                    context.turn_id.unwrap(),
                    TokenUsage::default(),
                    0,
                    reason,
                ));
            }
            if let Some(inbox) = &context.inbox {
                inbox.drain_completions();
            }
            BackendResult::EnteredRun(TurnOutcome::completed(
                context.agent_id,
                context.turn_id.unwrap(),
                TokenUsage::default(),
                1,
                crate::DoneReason::EndTurn,
            ))
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

fn graph() -> (AgentManagerHandle, AgentId, AgentId) {
    let root = AgentId::generate();
    let child = AgentId::generate();
    let records = [root, child]
        .into_iter()
        .map(|id| StoredAgentRecord {
            id,
            parent: (id == child).then_some(root),
            depth: u32::from(id == child),
            transcript_key: if id == child {
                TRANSCRIPT.into()
            } else {
                String::new()
            },
            lifecycle: StoredAgentLifecycle::Open,
            ancestry_unknown: false,
            label: None,
            spawned_by_tool_use_id: None,
            spec: None,
            restorable: Ok(()),
            usage: Default::default(),
            created_seq: 0,
            updated_seq: 0,
            legacy: false,
        })
        .collect();
    let histories: BTreeMap<_, _> = [root, child]
        .into_iter()
        .map(|id| (id, HistorySnapshot::new(Vec::<Message>::new())))
        .collect();
    let journal = Arc::new(SessionJournal::default());
    journal
        .commit(JournalCommit {
            agents: records,
            histories: histories.clone(),
            ..Default::default()
        })
        .unwrap();
    let snapshot = journal.snapshot();
    let view =
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap();
    let manager =
        AgentManagerHandle::restore_graph(AgentLimits::default(), journal, view, histories)
            .unwrap();
    (manager, root, child)
}

#[test]
fn dormant_rewind_fences_reactivation_and_reconciles_exact_graph() {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let stale_child = manager.lookup(child).unwrap();
        let mut guard = manager.begin_rewind(root).await.unwrap();
        assert_eq!(guard.affected_agents().len(), 2);
        assert!(guard.actor_guards().is_empty());
        assert!(matches!(
            manager
                .ensure_resident_with(child, |_| async { panic!("fenced factory ran") })
                .await,
            Err(ManagerError::RewindInProgress)
        ));
        let before = manager.journal().snapshot();
        let mut candidate = before.clone();
        candidate
            .agent_revisions
            .retain(|(_, record)| record.id != child);
        candidate.histories.remove(&child);
        candidate.managed_histories.remove(&child);
        candidate.agent_revisions[0].1.label = Some(HISTORICAL_LABEL.into());
        guard.validate_replacement(&candidate).unwrap();
        guard.reconcile(&candidate).unwrap();
        assert!(matches!(manager.node(child), Err(ManagerError::UnknownAgent(id)) if id == child));
        assert!(stale_child.snapshot().is_ok());
        assert!(manager.node(root).unwrap().children.is_empty());
        assert_eq!(
            manager.node(root).unwrap().metadata.label.as_deref(),
            Some(HISTORICAL_LABEL)
        );
        assert_eq!(manager.journal().snapshot().revision, before.revision);
        assert!(matches!(
            manager.begin_rewind(root).await,
            Err(ManagerError::RewindInProgress)
        ));
        drop(guard);
        assert!(manager.begin_rewind(root).await.is_ok());
    });
}

#[test]
fn root_rewind_restores_historical_child_outside_current_affected_set() {
    smol::block_on(async {
        let (manager, root, child) = graph();
        manager
            .journal()
            .commit(JournalCommit {
                histories: BTreeMap::from([(
                    child,
                    HistorySnapshot::new(vec![Message::user(INPUT.into())]),
                )]),
                ..Default::default()
            })
            .unwrap();
        let historical = manager.journal().snapshot();
        let mut current = historical.clone();
        current
            .agent_revisions
            .retain(|(_, record)| record.id != child);
        current.histories.remove(&child);
        current.managed_histories.remove(&child);
        let mut guard = manager.begin_rewind(root).await.unwrap();
        guard.validate_replacement(&current).unwrap();
        guard.reconcile(&current).unwrap();
        drop(guard);

        assert!(matches!(manager.node(child), Err(ManagerError::UnknownAgent(id)) if id == child));
        assert!(manager.node(root).unwrap().children.is_empty());
        let mut guard = manager.begin_rewind(root).await.unwrap();
        assert_eq!(
            guard.affected_agents().iter().copied().collect::<Vec<_>>(),
            vec![root]
        );
        assert!(guard.actor_guards().is_empty());
        guard.validate_replacement(&historical).unwrap();
        guard.reconcile(&historical).unwrap();
        let restored = manager.node(child).unwrap();
        assert_eq!(restored.parent_id, Some(root));
        assert_eq!(restored.graph_lifecycle, GraphLifecycle::Dormant);
        assert_eq!(restored.residency, AgentResidency::Dormant);
        assert!(restored.actor.is_none());
        assert_eq!(
            restored.metadata.transcript_key.as_deref(),
            Some(TRANSCRIPT)
        );
        assert_eq!(manager.node(root).unwrap().children, vec![child]);
        assert!(!guard.affected_agents().contains(&child));
    });
}

#[test]
fn child_reconcile_preserves_dormant_outside_binding_and_removes_retired_work() {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let outside = manager.incarnation(root).unwrap();
        let mut guard = manager.begin_rewind(child).await.unwrap();
        let candidate = manager.journal().snapshot();
        guard.validate_replacement(&candidate).unwrap();
        guard.reconcile(&candidate).unwrap();
        assert_eq!(manager.incarnation(root).unwrap(), outside);
        assert_eq!(manager.journal().snapshot().revision, candidate.revision);
        drop(guard);
        manager.0.work.remove(child).unwrap();
        let mut guard = manager.begin_rewind(root).await.unwrap();
        let mut candidate = manager.journal().snapshot();
        candidate
            .agent_revisions
            .retain(|(_, record)| record.id != child);
        candidate.histories.remove(&child);
        candidate.managed_histories.remove(&child);
        guard.validate_replacement(&candidate).unwrap();
        guard.reconcile(&candidate).unwrap();
        assert!(matches!(manager.node(child), Err(ManagerError::UnknownAgent(id)) if id == child));
    });
}

#[test_case(true; "missing_history")]
#[test_case(false; "changed_parent")]
fn rewind_prevalidation_rejects_without_mutating_runtime(history: bool) {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let guard = manager.begin_rewind(child).await.unwrap();
        let mut candidate = manager.journal().snapshot();
        if history {
            candidate.histories.remove(&child);
        } else {
            candidate
                .agent_revisions
                .iter_mut()
                .find(|(_, record)| record.id == child)
                .unwrap()
                .1
                .parent = None;
        }
        assert!(matches!(
            guard.validate_replacement(&candidate),
            Err(ManagerError::InvalidGraph(_))
        ));
        assert_eq!(manager.node(child).unwrap().parent_id, Some(root));
        assert_eq!(
            manager.node(child).unwrap().residency,
            AgentResidency::Dormant
        );
        drop(guard);
        assert!(manager.begin_rewind(child).await.is_ok());
    });
}

#[test]
fn resident_rewind_cancels_running_turn_and_holds_actor_until_drop() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (entered, started) = flume::bounded(1);
        let (_release, wait) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                Box::new(RewindBackend {
                    entered: Some(entered),
                    release: Some(wait),
                }),
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let ticket = actor
            .admit_turn(
                AgentInput::new(INPUT.into(), Vec::new()),
                None,
                String::new(),
            )
            .unwrap();
        started.recv_async().await.unwrap();
        let incarnation = root.incarnation().unwrap();
        let mut guard = manager.begin_rewind(root.id()).await.unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Cancelled { .. }));
        assert_eq!(guard.actor_guards().len(), 1);
        let candidate = manager.journal().snapshot();
        guard.validate_replacement(&candidate).unwrap();
        guard.reconcile(&candidate).unwrap();
        assert_eq!(root.incarnation().unwrap(), incarnation);
        assert_eq!(
            manager.node(root.id()).unwrap().graph_lifecycle,
            GraphLifecycle::Live
        );
        assert!(
            actor
                .admit_turn(
                    AgentInput::new(INPUT.into(), Vec::new()),
                    None,
                    String::new(),
                )
                .is_err()
        );
        drop(guard);
        assert!(actor.prepare_idle().is_ok());
        actor.shutdown();
    });
}

fn resident(backend: Box<dyn ActorBackend>) -> ResidentAgent {
    ResidentAgent {
        backend,
        config: EffectiveAgentConfig::new(
            RunSettings {
                provider: unconfigured(),
                model: Model::from_spec(MODEL).unwrap(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
        .with_context(AgentContextSnapshot::new(std::env::current_dir().unwrap())),
        shared_messages: None,
        start: None,
        initialize: None,
    }
}

#[test]
fn child_rewind_waits_for_outside_turn_without_cancelling_it() {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let (entered, started) = flume::bounded(1);
        let (release, wait) = flume::bounded(1);
        manager
            .ensure_resident_with(root, |_| async {
                Ok(resident(Box::new(RewindBackend {
                    entered: Some(entered),
                    release: Some(wait),
                })))
            })
            .await
            .unwrap();
        let actor = manager.actor(root).unwrap();
        let ticket = actor
            .admit_turn(
                AgentInput::new(INPUT.into(), Vec::new()),
                None,
                String::new(),
            )
            .unwrap();
        started.recv_async().await.unwrap();
        let mut rewind = Box::pin(manager.begin_rewind(child));
        assert!(poll_once(&mut rewind).await.is_none());
        assert!(ticket.peek().is_none());
        release.send(()).unwrap();
        let mut guard = rewind.await.unwrap();
        let outside = manager.incarnation(root).unwrap();
        let candidate = manager.journal().snapshot();
        guard.validate_replacement(&candidate).unwrap();
        guard.reconcile(&candidate).unwrap();
        assert_eq!(manager.incarnation(root).unwrap(), outside);
        assert_eq!(manager.journal().snapshot().revision, candidate.revision);
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(
            guard.affected_agents().iter().copied().collect::<Vec<_>>(),
            vec![child]
        );
        assert!(guard.actor_guards().contains_key(&root));
        assert!(
            actor
                .admit_turn(
                    AgentInput::new(INPUT.into(), Vec::new()),
                    None,
                    String::new()
                )
                .is_err()
        );
        drop(guard);
        actor.shutdown();
    });
}

#[test_case(false; "abort")]
#[test_case(true; "reconcile")]
fn child_rewind_preserves_outside_turn_report(commit: bool) {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let (start_parent, parent_ready) = flume::bounded(1);
        let parent = manager
            .ensure_resident_with(root, |_| async {
                let mut agent = resident(RewindBackend::idle());
                agent.start = Some(parent_ready);
                Ok(agent)
            })
            .await
            .unwrap();
        let (entered, started) = flume::bounded(1);
        let (release, wait) = flume::bounded(1);
        let sibling = manager
            .spawn_child_trusted_with_config(
                &parent,
                resident(RewindBackend::idle()).config,
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| {
                    Ok::<_, String>(Box::new(RewindBackend {
                        entered: Some(entered),
                        release: Some(wait),
                    }) as Box<dyn ActorBackend>)
                },
            )
            .unwrap();
        let parent_actor = parent.actor().unwrap();
        let sibling_actor = sibling.actor().unwrap();
        sibling_actor.configure_parent_delivery(true).unwrap();
        let events = parent_actor.subscribe_events();
        let parent_idle = parent_actor.prepare_idle().unwrap();
        let ticket = sibling_actor
            .admit_turn(
                AgentInput::new(INPUT.into(), Vec::new()),
                None,
                String::new(),
            )
            .unwrap();
        started.recv_async().await.unwrap();
        let mut rewind = Box::pin(manager.begin_rewind(child));
        assert!(poll_once(&mut rewind).await.is_none());
        release.send(()).unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let delivery = loop {
            if let ActorEvent::Delivery(event) = events.recv_async().await.unwrap() {
                break event;
            }
        };
        drop(parent_idle);
        let mut guard = rewind.await.unwrap();
        assert_eq!(delivery.status, DeliveryStatus::Accepted);
        assert_eq!(delivery.delivery.result.outcome.turn_id(), ticket.turn_id());
        let snapshot = manager.journal().snapshot();
        assert!(
            manager
                .deliver(root, Arc::clone(&delivery.delivery))
                .is_err()
        );
        assert_eq!(manager.journal().snapshot().revision, snapshot.revision);
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .unwrap();
        assert!(matches!(
            view.scopes[&Some(root)]
                .deliveries
                .get(&(sibling.id(), ticket.turn_id())),
            Some(StoredDeliveryOp::Queued(_))
        ));
        if commit {
            guard.reconcile(&snapshot).unwrap();
        }
        drop(guard);
        start_parent.send(()).unwrap();
        let executed = loop {
            if let ActorEvent::Delivery(event) = events.recv_async().await.unwrap()
                && matches!(event.status, DeliveryStatus::Executed { .. })
            {
                break event;
            }
        };
        assert_eq!(executed.delivery.result.outcome.turn_id(), ticket.turn_id());
        sibling_actor.shutdown();
        parent_actor.shutdown();
    });
}

#[test_case(false; "factory_pending")]
#[test_case(true; "initializer_pending")]
fn child_rewind_waits_for_outside_activation_and_abort_preserves_it(initialize: bool) {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let (release, wait) = flume::bounded(1);
        let mut activation = Box::pin(manager.ensure_resident_with(root, |_| async {
            let mut agent = resident(RewindBackend::idle());
            if initialize {
                agent.initialize = Some(Box::new(move |initialization| {
                    Box::pin(async move {
                        wait.recv_async().await.unwrap();
                        initialization
                            .actor
                            .configure_parent_delivery(false)
                            .map_err(|error| {
                                ReactivationError::Initialization(error.to_string())
                            })?;
                        Ok(())
                    })
                }));
            } else {
                wait.recv_async().await.unwrap();
            }
            Ok(agent)
        }));
        assert!(poll_once(&mut activation).await.is_none());
        let original = manager.incarnation(root).unwrap();
        let mut rewind = Box::pin(manager.begin_rewind(child));
        assert!(poll_once(&mut rewind).await.is_none());
        assert_eq!(manager.incarnation(root).unwrap(), original);
        assert_eq!(
            manager.node(root).unwrap().residency,
            AgentResidency::Activating
        );
        release.send(()).unwrap();
        let outside = activation.await.unwrap();
        let guard = rewind.await.unwrap();
        assert!(guard.actor_guards().contains_key(&root));
        assert_eq!(outside.incarnation().unwrap(), original);
        drop(guard);
        let actor = outside.actor().unwrap();
        let ticket = actor
            .admit_turn(
                AgentInput::new(INPUT.into(), Vec::new()),
                None,
                String::new(),
            )
            .unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        actor.shutdown();
    });
}

#[test_case(false; "outside_detach")]
#[test_case(true; "affected_detach")]
fn child_rewind_drains_preexisting_deferred_detach_before_holding_actors(affected: bool) {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let detached = if affected { child } else { root };
        manager
            .ensure_resident_with(detached, |_| async { Ok(resident(RewindBackend::idle())) })
            .await
            .unwrap();
        let old = manager.incarnation(detached).unwrap();
        let journal = manager.journal();
        let publication = journal.reserve_publication().await;
        manager.detach_incarnation(&old).unwrap();
        assert!(manager.lock_graph().nodes[&detached].detach_pending);
        let mut rewind = Box::pin(manager.begin_rewind(child));
        assert!(poll_once(&mut rewind).await.is_none());
        assert_eq!(manager.incarnation(detached).unwrap(), old);
        drop(publication);
        let guard = rewind.await.unwrap();
        assert!(!guard.actor_guards().contains_key(&detached));
        assert_eq!(
            manager.node(detached).unwrap().residency,
            AgentResidency::Dormant
        );
        assert!(!manager.lock_graph().nodes[&detached].detach_pending);
        assert_ne!(manager.incarnation(detached).unwrap(), old);
        drop(guard);
        let agent = manager
            .ensure_resident_with(detached, |_| async { Ok(resident(RewindBackend::idle())) })
            .await
            .unwrap();
        let actor = agent.actor().unwrap();
        let ticket = actor
            .admit_turn(
                AgentInput::new(INPUT.into(), Vec::new()),
                None,
                String::new(),
            )
            .unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        actor.shutdown();
    });
}

#[test_case(false; "outside_spawn_survives_abort")]
#[test_case(true; "inside_spawn_is_invalidated")]
fn child_rewind_handles_inflight_spawn_reservation(inside: bool) {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let parent_id = if inside { child } else { root };
        let parent = manager
            .ensure_resident_with(parent_id, |_| async { Ok(resident(RewindBackend::idle())) })
            .await
            .unwrap();
        let creating = manager.clone();
        let (reserved, entered) = flume::bounded(1);
        let (release, wait) = flume::bounded(1);
        let factory = thread::spawn(move || {
            creating.spawn_child_trusted_with_config(
                &parent,
                resident(RewindBackend::idle()).config,
                AgentMetadata::default(),
                Vec::new(),
                None,
                |id| {
                    reserved.send(id).unwrap();
                    wait.recv().unwrap();
                    Ok::<_, String>(RewindBackend::idle())
                },
            )
        });
        let spawned = entered.recv_async().await.unwrap();
        let original = manager.incarnation(spawned).unwrap();
        let mut rewind = Box::pin(manager.begin_rewind(child));
        let ready = poll_once(&mut rewind).await;
        if inside {
            let guard = match ready {
                Some(result) => result.unwrap(),
                None => rewind.await.unwrap(),
            };
            assert!(
                matches!(manager.node(spawned), Err(ManagerError::UnknownAgent(id)) if id == spawned)
            );
            drop(guard);
            release.send(()).unwrap();
            assert!(
                matches!(factory.join().unwrap(), Err(ManagerError::StaleIncarnation(id)) if id == spawned)
            );
        } else {
            assert!(ready.is_none());
            assert_eq!(manager.incarnation(spawned).unwrap(), original);
            release.send(()).unwrap();
            let outside = factory.join().unwrap().unwrap();
            let guard = rewind.await.unwrap();
            assert!(guard.actor_guards().contains_key(&spawned));
            assert_eq!(outside.incarnation().unwrap(), original);
            drop(guard);
            let actor = outside.actor().unwrap();
            let ticket = actor
                .admit_turn(
                    AgentInput::new(INPUT.into(), Vec::new()),
                    None,
                    String::new(),
                )
                .unwrap();
            assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
            actor.shutdown();
        }
        manager.actor(parent_id).unwrap().shutdown();
    });
}

#[test]
fn child_rewind_invalidates_inflight_activation_before_factory_returns() {
    smol::block_on(async {
        let (manager, root, child) = graph();
        let (release, wait) = flume::bounded(1);
        let mut activation = Box::pin(manager.ensure_resident_with(child, |_| async {
            wait.recv_async().await.unwrap();
            Ok(resident(RewindBackend::idle()))
        }));
        assert!(poll_once(&mut activation).await.is_none());
        let original = manager.incarnation(child).unwrap();
        let guard = manager.begin_rewind(root).await.unwrap();
        assert_ne!(manager.incarnation(child).unwrap(), original);
        release.send(()).unwrap();
        assert!(matches!(
            activation.await,
            Err(ManagerError::RewindInProgress) | Err(ManagerError::StaleIncarnation(_))
        ));
        assert!(manager.node(child).unwrap().actor.is_none());
        drop(guard);
        assert!(
            manager
                .ensure_resident_with(child, |_| async { Ok(resident(RewindBackend::idle())) })
                .await
                .is_ok()
        );
        manager.actor(child).unwrap().shutdown();
    });
}

#[test_case(false; "missing_offload_fails_without_losing_guard")]
#[test_case(true; "offloaded_results_are_hydrated_and_installed")]
fn prepare_rewind_actors_hydrates_results_without_defaults(bind: bool) {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let config = resident(RewindBackend::idle()).config;
        let root = manager
            .create_root_with_config(Some(config), Vec::new(), None, |_| {
                Ok::<_, String>(RewindBackend::idle())
            })
            .unwrap();
        let actor = root.actor().unwrap();
        let dir = TempDir::new().unwrap();
        let session = MakimaId::generate();
        manager.journal().bind_session(session).unwrap();
        let store = Arc::new(OffloadStore::for_session(
            dir.path(),
            &SessionRef::from(session),
        ));
        let result = TurnResult {
            outcome: TurnOutcome::completed(
                root.id(),
                crate::TurnId::generate(),
                TokenUsage::default(),
                1,
                crate::DoneReason::EndTurn,
            ),
            text: INPUT.repeat(MAX_INLINE_TURN_RESULT_BYTES),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 1,
        };
        let turn = result.outcome.turn_id();
        let stored = result.to_stored(session, Some(&store)).unwrap();
        manager
            .journal()
            .commit(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(root.id()),
                    generation: 0,
                    change: StateChange::Turn {
                        turn,
                        result: stored,
                    },
                }],
                ..Default::default()
            })
            .unwrap();
        if bind {
            manager.bind_result_store(store).unwrap();
        }
        let preparer: ModelPreparer = Arc::new(|spec| {
            Box::pin(async move {
                Ok(PreparedModel {
                    provider: unconfigured(),
                    model: Model::from_spec(&spec).unwrap(),
                })
            })
        });
        let mut guard = manager.begin_rewind(root.id()).await.unwrap();
        let snapshot = manager.journal().snapshot();
        let prepared = guard
            .prepare_actors(
                &snapshot,
                &ModelPolicy::default(),
                &preparer,
                &ModeRegistry::builtin(),
            )
            .await;
        if bind {
            let mut prepared = prepared.unwrap();
            assert_eq!(prepared.len(), 1);
            let installed = prepared.remove(0).install_held();
            assert_eq!(actor.turn_ticket(turn).unwrap().peek_result(), Some(result));
            assert_eq!(manager.journal().snapshot().revision, snapshot.revision);
            drop(installed);
        } else {
            assert!(
                matches!(prepared, Err(ManagerError::TurnResultUnavailable { agent_id, turn_id }) if agent_id == root.id() && turn_id == turn)
            );
            assert!(guard.actor_guard(root.id()).is_some());
        }
        drop(guard);
        actor.shutdown();
    });
}

#[test_case(false, false; "activation_legacy_usage")]
#[test_case(true, false; "rewind_legacy_usage")]
#[test_case(false, true; "activation_all_turns_deduplicated")]
#[test_case(true, true; "rewind_all_turns_deduplicated")]
fn restored_usage_uses_all_latest_turn_records(rewind: bool, turns: bool) {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root_with_config(
                Some(resident(RewindBackend::idle()).config),
                Vec::new(),
                None,
                |_| Ok::<_, String>(RewindBackend::idle()),
            )
            .unwrap();
        let mut record = manager.journal().agent(root.id()).unwrap();
        record.usage.input = LEGACY_USAGE.input;
        record.usage.output = LEGACY_USAGE.output;
        record.usage.cache_creation = LEGACY_USAGE.cache_creation;
        record.usage.cache_read = LEGACY_USAGE.cache_read;
        let mut expected = LEGACY_USAGE;
        let mut changes = Vec::new();
        if turns {
            expected = TokenUsage::default();
            for _ in 0..=MAX_STORED_TURN_RESULTS {
                let turn = crate::TurnId::generate();
                for usage in [LEGACY_USAGE, TURN_USAGE] {
                    let result = TurnResult {
                        outcome: TurnOutcome::completed(
                            root.id(),
                            turn,
                            usage,
                            1,
                            crate::DoneReason::EndTurn,
                        ),
                        text: String::new(),
                        output: Vec::new(),
                        provenance: TurnProvenance::default(),
                        duration_ms: 0,
                    };
                    changes.push(JournalChange {
                        agent: Some(root.id()),
                        generation: 0,
                        change: StateChange::Turn {
                            turn,
                            result: result
                                .to_stored(manager.journal().snapshot().epoch, None)
                                .unwrap(),
                        },
                    });
                }
                expected += TURN_USAGE;
            }
        }
        manager
            .journal()
            .commit(JournalCommit {
                agents: vec![record],
                changes,
                ..Default::default()
            })
            .unwrap();
        if rewind {
            let actor = root.actor().unwrap();
            let mut guard = manager.begin_rewind(root.id()).await.unwrap();
            let snapshot = manager.journal().snapshot();
            let preparer: ModelPreparer = Arc::new(|spec| {
                Box::pin(async move {
                    Ok(PreparedModel {
                        provider: unconfigured(),
                        model: Model::from_spec(&spec).unwrap(),
                    })
                })
            });
            let mut prepared = guard
                .prepare_actors(
                    &snapshot,
                    &ModelPolicy::default(),
                    &preparer,
                    &ModeRegistry::builtin(),
                )
                .await
                .unwrap();
            let installed = prepared.remove(0).install_held();
            guard.reconcile(&snapshot).unwrap();
            assert_eq!(actor.snapshot().cumulative_usage, expected);
            drop(installed);
            drop(guard);
            actor.shutdown();
        } else {
            let incarnation = root.incarnation().unwrap();
            manager.detach_incarnation(&incarnation).unwrap();
            manager
                .ensure_resident_with(root.id(), |_| async { Ok(resident(RewindBackend::idle())) })
                .await
                .unwrap();
            let actor = root.actor().unwrap();
            assert_eq!(actor.snapshot().cumulative_usage, expected);
            actor.shutdown();
        }
    });
}

#[test]
fn dropped_pending_rewind_releases_global_fence() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, RewindBackend::idle())
            .unwrap();
        let actor = root.actor().unwrap();
        let idle = actor.prepare_idle().unwrap();
        let mut pending = Box::pin(manager.begin_rewind(root.id()));
        assert!(poll_once(&mut pending).await.is_none());
        assert!(matches!(
            manager.begin_rewind(root.id()).await,
            Err(ManagerError::RewindInProgress)
        ));
        drop(pending);
        drop(idle);
        assert!(manager.begin_rewind(root.id()).await.is_ok());
        actor.shutdown();
    });
}
