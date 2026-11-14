use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use futures_lite::future::{or, poll_once, yield_now};
use makima_providers::TokenUsage;
use makima_providers::{Message, Model, provider::unconfigured};
use makima_storage::session_state::{
    SessionStateView, StateChange, StoredAgentConfig, StoredAgentLifecycle, StoredAgentRecord,
    StoredAgentSpec, StoredDelivery, StoredDeliveryOp, StoredModeCeiling, StoredTurnOrigin,
};
use makima_storage::sessions::Session;
use test_case::test_case;

use super::{
    AgentLimits, AgentManagerHandle, AgentMetadata, AgentResidency, GraphLifecycle, ManagerError,
    ReactivationError, ResidentAgent,
};
use crate::{
    ActorBackend, AgentId, AgentInput, AgentMode, BackendResult, ControlWork, History,
    HistorySnapshot, ModeDef, ModeId, ReasonedCancelToken, RunSettings, TurnContext, TurnId,
    WorkKind,
    actor::{
        ActiveCancel, ActorHistory, ChildDelivery, EffectiveAgentConfig, TurnProvenance, TurnResult,
    },
    journal::{
        JournalChange, JournalCommit, JournalIncarnation, SessionJournal, SessionJournalSnapshot,
    },
};

type StoredSession = Session<Message, TokenUsage, crate::ToolOutput>;

const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
const HISTORY_TEXT: &str = "restored history";
const INITIALIZATION_ERROR: &str = "initialization rejected";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const RETAINED_AGENTS: usize = 4;
const DETACH_TIMEOUT: Duration = Duration::from_secs(1);
const PUBLICATION_PLUGIN: &str = "detach-publication";
const PUBLICATION_VERSION: u32 = 2;
const PUBLICATION_VALUE: &str = "published candidate";

#[test]
fn deferred_close_cannot_close_replacement_incarnation() {
    smol::block_on(async {
        let records = records();
        let id = records[0].id;
        let journal = journal(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let old = manager.incarnation(id).unwrap();
        let (release, ready) = flume::bounded(1);
        let cleanup_manager = manager.clone();
        let cleanup_incarnation = old.clone();
        let mut deferred = Box::pin(async move {
            ready.recv_async().await.unwrap();
            cleanup_manager.close_incarnation(&cleanup_incarnation)
        });
        assert!(poll_once(deferred.as_mut()).await.is_none());
        manager.detach_incarnation(&old).unwrap();
        let replacement = manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let actor = replacement.actor().unwrap();
        let before = journal.checkpoint_snapshot().unwrap();
        release.send(()).unwrap();
        assert_eq!(deferred.await, Err(ManagerError::StaleIncarnation(id)));
        assert!(manager.actor(id).unwrap().same_actor(&actor));
        replacement.validate_live().unwrap();
        assert_checkpoint(&journal, &before);
        manager
            .close_incarnation(&manager.incarnation(id).unwrap())
            .unwrap();
        assert_eq!(
            journal.agent(id).unwrap().lifecycle,
            StoredAgentLifecycle::Closed
        );
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

fn publication_commit(binding: &JournalIncarnation) -> JournalCommit {
    JournalCommit {
        incarnations: vec![binding.clone()],
        changes: vec![JournalChange {
            agent: Some(binding.agent),
            generation: 0,
            change: StateChange::Plugin {
                key: PUBLICATION_PLUGIN.into(),
                version: PUBLICATION_VERSION,
                value: Some(serde_json::json!(PUBLICATION_VALUE)),
            },
        }],
        ..Default::default()
    }
}

async fn wait_for_incarnation_cleanup(journal: &SessionJournal, binding: &JournalIncarnation) {
    or(
        async {
            while journal.incarnation(binding.agent) == *binding {
                yield_now().await;
            }
        },
        async {
            smol::Timer::after(DETACH_TIMEOUT).await;
            panic!("incarnation cleanup did not finish after publication released");
        },
    )
    .await;
}

#[test]
fn detach_after_publication_waits_for_install_and_runtime_finalization() {
    smol::block_on(async {
        let records = records();
        let id = records[0].id;
        let journal = journal(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let old = manager.incarnation(id).unwrap();
        let binding = journal.incarnation(id);
        let directory = tempfile::tempdir().unwrap();
        let mut session = StoredSession::new(MODEL, ".");
        session.bind_root_id(id).unwrap();
        let mut token = journal.prepare(publication_commit(&binding)).await.unwrap();
        token.snapshot().apply_to(&mut session).unwrap();
        session.save_to(directory.path()).unwrap();
        let published = StoredSession::load_from(session.id, directory.path()).unwrap();
        let published_view = published.latest_state().unwrap();
        let candidate = &published_view.scopes[&Some(id)].plugins[PUBLICATION_PLUGIN];
        assert_eq!(candidate.version, PUBLICATION_VERSION);
        assert_eq!(candidate.value, Some(serde_json::json!(PUBLICATION_VALUE)));

        assert_eq!(manager.detach_incarnation(&old), Ok(()));
        assert!(!manager.is_live(id));
        assert_eq!(journal.incarnation(id), binding);
        assert_eq!(manager.incarnation(id).unwrap(), old);
        assert_eq!(
            manager.node(id).unwrap().residency,
            AgentResidency::Resident
        );
        assert_eq!(
            manager
                .ensure_resident_with(id, |_| async {
                    panic!("pending detach must reject replacement activation")
                })
                .await
                .unwrap_err(),
            ManagerError::Reactivation(ReactivationError::Cancelled)
        );
        token.install().unwrap();
        or(
            async {
                loop {
                    let finished = manager.lock_graph().nodes[&id]
                        .task
                        .as_ref()
                        .is_some_and(smol::Task::is_finished);
                    if finished {
                        break;
                    }
                    yield_now().await;
                }
            },
            async {
                smol::Timer::after(DETACH_TIMEOUT).await;
                panic!("detached actor runner did not finish before publication released");
            },
        )
        .await;
        assert_eq!(journal.incarnation(id), binding);
        assert_eq!(
            manager.node(id).unwrap().residency,
            AgentResidency::Resident
        );
        drop(token);

        wait_for_incarnation_cleanup(&journal, &binding).await;
        assert_dormant(&manager, id);
        manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        assert_ne!(manager.incarnation(id).unwrap(), old);
        journal
            .checkpoint_snapshot()
            .unwrap()
            .apply_to(&mut session)
            .unwrap();
        session.save_to(directory.path()).unwrap();
        let saved = StoredSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(
            &saved.latest_state().unwrap().scopes[&Some(id)].plugins[PUBLICATION_PLUGIN],
            candidate
        );
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn initializer_rollback_waits_for_prepared_publication_to_finalize() {
    smol::block_on(async {
        let records = records();
        let id = records[0].id;
        let journal = journal(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (release, wait) = flume::bounded(1);
        let mut activation = Box::pin(manager.ensure_resident_with(id, |_| async move {
            let mut resident = resident();
            resident.initialize = Some(Box::new(move |_| {
                Box::pin(async move {
                    wait.recv_async().await.unwrap();
                    Err(ReactivationError::Initialization(
                        INITIALIZATION_ERROR.into(),
                    ))
                })
            }));
            Ok(resident)
        }));
        assert!(poll_once(activation.as_mut()).await.is_none());
        let old = manager.incarnation(id).unwrap();
        let binding = journal.incarnation(id);
        let mut token = journal.prepare(publication_commit(&binding)).await.unwrap();
        release.send(()).unwrap();
        assert_eq!(
            activation.await.unwrap_err(),
            ManagerError::Reactivation(ReactivationError::Initialization(
                INITIALIZATION_ERROR.into()
            ))
        );
        assert_eq!(journal.incarnation(id), binding);
        assert_eq!(manager.incarnation(id).unwrap(), old);
        assert_eq!(
            manager.node(id).unwrap().residency,
            AgentResidency::Activating
        );
        assert!(!manager.is_live(id));
        assert_eq!(
            manager
                .ensure_resident_with(id, |_| async {
                    panic!("pending rollback must reject replacement activation")
                })
                .await
                .unwrap_err(),
            ManagerError::Reactivation(ReactivationError::Cancelled)
        );
        token.install().unwrap();
        yield_now().await;
        assert_eq!(journal.incarnation(id), binding);
        drop(token);
        wait_for_incarnation_cleanup(&journal, &binding).await;
        assert_dormant(&manager, id);
        let before = journal.checkpoint_snapshot().unwrap();
        manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        assert_ne!(manager.incarnation(id).unwrap(), old);
        assert_checkpoint(&journal, &before);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn shutdown_deadline_includes_waiting_for_prepared_publication() {
    smol::block_on(async {
        let records = records();
        let id = records[0].id;
        let journal = journal(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let agent = manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let actor = agent.actor().unwrap();
        let binding = journal.incarnation(id);
        let mut token = journal.prepare(publication_commit(&binding)).await.unwrap();
        let mut shutdown = Box::pin(manager.shutdown(Duration::ZERO));
        let report = poll_once(shutdown.as_mut())
            .await
            .expect("zero-deadline shutdown must not wait for the publication writer");
        assert_eq!(report.timed_out, vec![id]);
        assert!(report.joined.is_empty());
        assert_eq!(journal.incarnation(id), binding);
        assert!(matches!(
            actor.admit_turn(
                AgentInput {
                    message: HISTORY_TEXT.into(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
                },
                None,
                HISTORY_TEXT.into(),
            ),
            Err(crate::ActorError::Shutdown)
        ));
        token.install().unwrap();
        assert_eq!(journal.incarnation(id), binding);
        drop(token);
        or(manager.wait_until_reaped(), async {
            smol::Timer::after(DETACH_TIMEOUT).await;
            panic!("shutdown reaper did not finish after publication released");
        })
        .await;
        assert!(manager.runner_finished(id).unwrap());
        assert_dormant(&manager, id);
    });
}

#[test]
fn dormant_transcript_caps_latest_without_inventing_turn_coverage() {
    let records = records();
    let id = records[1].id;
    let journal = journal(&records);
    let turn = TurnId::generate();
    let result = crate::actor::TurnResult {
        outcome: crate::TurnOutcome::cancelled(
            id,
            turn,
            Default::default(),
            0,
            crate::TurnCancellationReason::User,
        ),
        text: String::new(),
        output: Vec::new(),
        provenance: crate::actor::TurnProvenance::default(),
        duration_ms: 0,
    }
    .to_stored(journal.snapshot().epoch, None)
    .unwrap();
    journal
        .commit(JournalCommit {
            changes: vec![JournalChange {
                agent: Some(id),
                generation: 0,
                change: StateChange::Turn { turn, result },
            }],
            histories: BTreeMap::from([(
                id,
                History::new(vec![
                    Message::user(HISTORY_TEXT.into()),
                    Message::user(INITIALIZATION_ERROR.into()),
                ])
                .snapshot()
                .clone(),
            )]),
            ..Default::default()
        })
        .unwrap();
    let before = journal.checkpoint_snapshot().unwrap();
    let manager = restore(&journal, AgentLimits::default()).unwrap();
    let request = crate::actor::TranscriptRequest {
        through_turn: None,
        last_messages: 1,
        max_bytes: 4096,
    };
    let latest = manager
        .lookup(id)
        .unwrap()
        .transcript(request.clone())
        .unwrap();
    assert_eq!(latest.total_messages, 2);
    assert_eq!(latest.omitted_messages, 1);
    assert_eq!(latest.messages.as_array().unwrap().len(), 1);
    assert_eq!(latest.epoch, before.histories[&id].epoch);
    assert_eq!(latest.through_turn, None);
    let capped = manager
        .transcript(
            id,
            crate::actor::TranscriptRequest {
                max_bytes: 2,
                ..request.clone()
            },
        )
        .unwrap();
    assert_eq!(capped.messages, serde_json::json!([]));
    assert!(capped.truncated);
    assert!(
        matches!(manager.transcript(id, crate::actor::TranscriptRequest { through_turn: Some(turn), ..request.clone() }), Err(crate::ActorError::UnavailableTurnHistory(actual)) if actual == turn)
    );
    let unknown = TurnId::generate();
    assert!(
        matches!(manager.transcript(id, crate::actor::TranscriptRequest { through_turn: Some(unknown), ..request }), Err(crate::ActorError::UnknownTurn(actual)) if actual == unknown)
    );
    assert_dormant(&manager, id);
    assert_checkpoint(&journal, &before);
}

#[test_case(false; "factory_failure")]
#[test_case(true; "factory_dropped")]
fn close_pending_factory_settles_closed_without_resources(drop_factory: bool) {
    smol::block_on(async {
        let records = records();
        let id = records[1].id;
        let journal = journal(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (release, wait) = flume::bounded(1);
        let mut activation = Box::pin(manager.ensure_resident_with(id, |_| async move {
            wait.recv_async().await.unwrap();
            Err(ReactivationError::Initialization(
                INITIALIZATION_ERROR.into(),
            ))
        }));
        assert!(poll_once(activation.as_mut()).await.is_none());
        manager.close_subtree(id).unwrap();
        let closed = journal.checkpoint_snapshot().unwrap();
        if drop_factory {
            drop(activation);
        } else {
            release.send(()).unwrap();
            assert!(activation.await.is_err());
        }
        let node = manager.node(id).unwrap();
        assert_eq!(node.graph_lifecycle, GraphLifecycle::Closed);
        assert_eq!(node.residency, AgentResidency::Dormant);
        assert!(node.actor.is_none());
        assert!(manager.runner_finished(id).unwrap());
        assert_eq!(
            journal.agent(id).unwrap().lifecycle,
            StoredAgentLifecycle::Closed
        );
        assert_checkpoint(&journal, &closed);
    });
}

#[test]
fn detached_backend_cleanup_cannot_overwrite_replacement_history() {
    smol::block_on(async {
        let records = records();
        let id = records[0].id;
        let journal = journal(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (entered, running) = flume::bounded(1);
        let (release, wait) = flume::bounded(1);
        let (cleaned, cleanup) = flume::bounded(1);
        let old = manager
            .ensure_resident_with(id, |_| async move {
                let mut resident = resident();
                resident.backend = Box::new(GatedCleanupBackend {
                    entered,
                    release: wait,
                    cleaned,
                });
                Ok(resident)
            })
            .await
            .unwrap();
        let actor = old.actor().unwrap();
        actor
            .submit(
                AgentInput {
                    message: HISTORY_TEXT.into(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
                },
                None,
                HISTORY_TEXT.into(),
                crate::TurnAdmissionOptions::default(),
            )
            .unwrap();
        running.recv_async().await.unwrap();
        manager
            .detach_incarnation(&manager.incarnation(id).unwrap())
            .unwrap();
        let replacement = manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let expected = journal.checkpoint_snapshot().unwrap().histories[&id].clone();
        let identity = replacement.actor().unwrap().identity();
        release.send(()).unwrap();
        cleanup.recv_async().await.unwrap();
        assert_history(
            &journal.checkpoint_snapshot().unwrap().histories[&id],
            &expected,
        );
        assert_history(
            &replacement.actor().unwrap().committed_history_snapshot(),
            &expected,
        );
        assert!(Arc::ptr_eq(
            &identity,
            &manager.actor(id).unwrap().identity()
        ));
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        assert_history(
            &journal.checkpoint_snapshot().unwrap().histories[&id],
            &expected,
        );
    });
}

struct OfflineBackend;

struct GatedCleanupBackend {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
    cleaned: flume::Sender<()>,
}

impl ActorBackend for GatedCleanupBackend {
    fn run_turn<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.entered.send(()).unwrap();
            context.cancel.cancelled().await;
            self.release.recv_async().await.unwrap();
            let _ = history
                .push(Message::user(INITIALIZATION_ERROR.into()))
                .await;
            self.cleaned.send(()).unwrap();
            BackendResult::EnteredRun(crate::TurnOutcome::cancelled(
                context.agent_id,
                context.turn_id.unwrap(),
                Default::default(),
                0,
                crate::TurnCancellationReason::Shutdown,
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

impl ActorBackend for OfflineBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { panic!("restore must not run a turn") })
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

fn config() -> EffectiveAgentConfig {
    EffectiveAgentConfig::new(
        RunSettings {
            provider: unconfigured(),
            model: Model::from_spec(MODEL).unwrap(),
            fast: true,
            workflow: false,
            thinking: Default::default(),
        },
        AgentMode::Build,
    )
    .with_mode_def(Some(ModeDef::default_for(ModeId::Build)))
}

fn resident() -> ResidentAgent {
    ResidentAgent {
        backend: Box::new(OfflineBackend),
        config: config(),
        shared_messages: None,
        start: None,
        initialize: None,
    }
}

fn records() -> [StoredAgentRecord; RETAINED_AGENTS] {
    let root = AgentId::generate();
    [
        StoredAgentLifecycle::Open,
        StoredAgentLifecycle::Open,
        StoredAgentLifecycle::Closed,
        StoredAgentLifecycle::Retired,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, lifecycle)| StoredAgentRecord {
        id: if index == 0 {
            root
        } else {
            AgentId::generate()
        },
        parent: (index != 0).then_some(root),
        depth: u32::from(index != 0),
        transcript_key: if index == 0 {
            String::new()
        } else {
            format!("child-{index}")
        },
        lifecycle,
        ancestry_unknown: false,
        label: Some(format!("agent-{index}")),
        spawned_by_tool_use_id: (index != 0).then(|| format!("tool-{index}")),
        spec: None,
        restorable: Ok(()),
        usage: Default::default(),
        created_seq: 0,
        updated_seq: 0,
        legacy: false,
    })
    .collect::<Vec<_>>()
    .try_into()
    .unwrap()
}

fn journal(records: &[StoredAgentRecord]) -> Arc<SessionJournal> {
    let journal = Arc::new(SessionJournal::default());
    let saved_config = StoredAgentConfig {
        cwd: ".".into(),
        model: MODEL.into(),
        thinking: None,
        fast: true,
        workflow: false,
        mode: ModeDef::default_for(ModeId::Build).stored_snapshot(&AgentMode::Build),
        options: BTreeMap::new(),
    };
    let mut history = History::new(vec![Message::user(HISTORY_TEXT.into())]);
    history.mark_sent();
    journal
        .commit(JournalCommit {
            agents: records.to_vec(),
            changes: records
                .iter()
                .map(|record| JournalChange {
                    agent: Some(record.id),
                    generation: 0,
                    change: StateChange::Config(saved_config.clone()),
                })
                .collect(),
            histories: records
                .iter()
                .map(|record| (record.id, history.snapshot().clone()))
                .collect(),
            ..Default::default()
        })
        .unwrap();
    journal
}

fn restore(
    journal: &Arc<SessionJournal>,
    limits: AgentLimits,
) -> Result<AgentManagerHandle, ManagerError> {
    let snapshot = journal.checkpoint_snapshot().unwrap();
    let view =
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap();
    AgentManagerHandle::restore_graph(limits, Arc::clone(journal), view, snapshot.histories)
}

fn queued_child_report(records: &[StoredAgentRecord]) -> (Arc<SessionJournal>, Arc<ChildDelivery>) {
    let journal = journal(records);
    let delivery = Arc::new(ChildDelivery {
        label: records[2].label.clone(),
        result: TurnResult {
            outcome: crate::TurnOutcome::cancelled(
                records[2].id,
                TurnId::generate(),
                Default::default(),
                0,
                crate::TurnCancellationReason::User,
            ),
            text: HISTORY_TEXT.into(),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        },
    });
    let turn = delivery.result.outcome.turn_id();
    let result = delivery
        .result
        .to_stored(journal.snapshot().epoch, None)
        .unwrap();
    journal
        .commit(JournalCommit {
            changes: vec![
                JournalChange {
                    agent: Some(records[2].id),
                    generation: 0,
                    change: StateChange::Turn {
                        turn,
                        result: result.clone(),
                    },
                },
                crate::actor::delivery::change(
                    records[0].id,
                    &delivery,
                    StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                        label: delivery.label.clone(),
                        result,
                        preview_bytes: None,
                        held: false,
                    })),
                ),
            ],
            ..Default::default()
        })
        .unwrap();
    (journal, delivery)
}

fn rebuild_journal(journal: Arc<SessionJournal>, root: AgentId) -> Arc<SessionJournal> {
    let mut session = StoredSession::new(MODEL, ".");
    session.bind_root_id(root).unwrap();
    journal
        .checkpoint_snapshot()
        .unwrap()
        .apply_to(&mut session)
        .unwrap();
    drop(journal);
    Arc::new(SessionJournal::from_session(&session).unwrap())
}

#[test]
fn restored_report_survives_initializer_failure_and_hydrates_once() {
    smol::block_on(async {
        let records = records();
        let parent = records[0].id;
        let (journal, delivery) = queued_child_report(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        assert_eq!(manager.active_agent_count(), 2);
        drop(manager);
        let journal = rebuild_journal(journal, parent);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        assert_dormant(&manager, parent);
        assert_eq!(manager.active_agent_count(), 2);
        let before = journal.checkpoint_snapshot().unwrap();
        let error = manager
            .ensure_resident_with(parent, |_| async {
                let mut resident = resident();
                resident.initialize = Some(Box::new(|input| {
                    Box::pin(async move {
                        assert_eq!(input.actor.inner.state.lock().unwrap().deliveries.len(), 1);
                        Err(ReactivationError::Initialization(
                            INITIALIZATION_ERROR.into(),
                        ))
                    })
                }));
                Ok(resident)
            })
            .await
            .unwrap_err();
        assert_eq!(
            error,
            ManagerError::Reactivation(ReactivationError::Initialization(
                INITIALIZATION_ERROR.into()
            ))
        );
        assert_dormant(&manager, parent);
        assert_eq!(manager.active_agent_count(), 2);
        assert_checkpoint(&journal, &before);
        let (_start, wait) = flume::bounded(1);
        let parent_ref = manager
            .ensure_resident_with(parent, |_| async move {
                let mut resident = resident();
                resident.start = Some(wait);
                Ok(resident)
            })
            .await
            .unwrap();
        let actor = parent_ref.actor().unwrap();
        assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
        assert_eq!(manager.active_agent_count(), 2);
        let before = journal.checkpoint_snapshot().unwrap();
        manager.deliver(parent, delivery).unwrap();
        assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
        assert_checkpoint(&journal, &before);
        manager.close_subtree(parent).unwrap();
        assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 0);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        assert_eq!(manager.active_agent_count(), 0);
    });
}

#[test]
fn report_arriving_during_initializer_is_reconciled_with_its_work_hold() {
    smol::block_on(async {
        let records = records();
        let parent = records[0].id;
        let (_, delivery) = queued_child_report(&records);
        let source = delivery.result.outcome.agent_id();
        let journal = journal(&records);
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (initializer_entered, entered) = flume::bounded(1);
        let (release, wait) = flume::bounded(1);
        let (_start, start_wait) = flume::bounded(1);
        let mut activation = Box::pin(manager.ensure_resident_with(parent, |_| async move {
            let mut resident = resident();
            resident.start = Some(start_wait);
            resident.initialize = Some(Box::new(move |input| {
                Box::pin(async move {
                    assert!(
                        input
                            .actor
                            .inner
                            .state
                            .lock()
                            .unwrap()
                            .deliveries
                            .is_empty()
                    );
                    initializer_entered.send(input.actor).unwrap();
                    wait.recv_async().await.unwrap();
                    Ok(())
                })
            }));
            Ok(resident)
        }));
        assert!(poll_once(activation.as_mut()).await.is_none());
        let initializing_actor = entered.recv().unwrap();
        let mut source_work = manager.0.work.restore_hold(source).unwrap();
        let report_work = source_work.take_report().unwrap();
        drop(source_work);
        manager
            .deliver_reserved(parent, Arc::clone(&delivery), vec![report_work])
            .unwrap();
        assert!(
            initializing_actor
                .inner
                .state
                .lock()
                .unwrap()
                .deliveries
                .is_empty()
        );
        assert_eq!(manager.active_agent_count(), 2);
        let queued = journal.checkpoint_snapshot().unwrap();
        release.send(()).unwrap();
        let actor = activation.await.unwrap().actor().unwrap();
        assert!(actor.same_actor(&initializing_actor));
        assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
        assert_eq!(manager.active_agent_count(), 2);
        assert_checkpoint(&journal, &queued);
        manager.deliver(parent, delivery).unwrap();
        assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
        assert_eq!(manager.active_agent_count(), 2);
        assert_checkpoint(&journal, &queued);
        manager.close_subtree(parent).unwrap();
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        assert_eq!(manager.active_agent_count(), 0);
    });
}

#[test]
fn report_withdrawn_during_initializer_is_not_resurrected() {
    smol::block_on(async {
        let records = records();
        let parent = records[0].id;
        let (journal, delivery) = queued_child_report(&records);
        let id = (
            delivery.result.outcome.agent_id(),
            delivery.result.outcome.turn_id(),
        );
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (initializer_entered, entered) = flume::bounded(1);
        let (release, wait) = flume::bounded(1);
        let (_start, start_wait) = flume::bounded(1);
        let mut activation = Box::pin(manager.ensure_resident_with(parent, |_| async move {
            let mut resident = resident();
            resident.start = Some(start_wait);
            resident.initialize = Some(Box::new(move |input| {
                Box::pin(async move {
                    assert_eq!(input.actor.inner.state.lock().unwrap().deliveries.len(), 1);
                    initializer_entered.send(()).unwrap();
                    wait.recv_async().await.unwrap();
                    assert!(input.actor.withdraw_child_delivery(id.1));
                    Ok(())
                })
            }));
            Ok(resident)
        }));
        assert!(poll_once(activation.as_mut()).await.is_none());
        entered.recv().unwrap();
        release.send(()).unwrap();
        let actor = activation.await.unwrap().actor().unwrap();
        assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
        let withdrawn = journal.checkpoint_snapshot().unwrap();
        let view =
            SessionStateView::fold(&withdrawn.state, &withdrawn.agent_revisions, withdrawn.seq)
                .unwrap();
        assert_eq!(
            view.scopes[&Some(parent)].deliveries[&id],
            StoredDeliveryOp::Withdrawn
        );
        manager.deliver(parent, delivery).unwrap();
        assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
        assert_checkpoint(&journal, &withdrawn);
        manager.close_subtree(parent).unwrap();
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        assert_eq!(manager.active_agent_count(), 0);
    });
}

#[test]
fn closing_dormant_restored_report_releases_hold_and_persists_drop() {
    let records = records();
    let parent = records[0].id;
    let (journal, delivery) = queued_child_report(&records);
    let id = (
        delivery.result.outcome.agent_id(),
        delivery.result.outcome.turn_id(),
    );
    let manager = restore(&journal, AgentLimits::default()).unwrap();
    assert_dormant(&manager, parent);
    assert_eq!(manager.active_agent_count(), 2);
    drop(manager);
    let journal = rebuild_journal(journal, parent);
    let manager = restore(&journal, AgentLimits::default()).unwrap();
    assert_eq!(manager.active_agent_count(), 2);
    manager.close_subtree(parent).unwrap();
    assert_eq!(manager.active_agent_count(), 0);
    manager.close_subtree(parent).unwrap();
    assert_eq!(manager.active_agent_count(), 0);
    drop(manager);
    let journal = rebuild_journal(journal, parent);
    let manager = restore(&journal, AgentLimits::default()).unwrap();
    assert_eq!(manager.active_agent_count(), 0);
    assert_eq!(
        manager.node(parent).unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
    let snapshot = journal.checkpoint_snapshot().unwrap();
    let view =
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap();
    assert_eq!(
        view.scopes[&Some(parent)].deliveries[&id],
        StoredDeliveryOp::Dropped
    );
    let drops = snapshot
        .state
        .iter()
        .filter(|record| {
            matches!(&record.change, StateChange::Delivery {
            id: actual,
            op: StoredDeliveryOp::Dropped,
        } if *actual == id)
        })
        .count();
    assert_eq!(drops, 1);
}

fn assert_history(actual: &HistorySnapshot, expected: &HistorySnapshot) {
    assert_eq!(actual.epoch, expected.epoch);
    assert_eq!(actual.compacted, expected.compacted);
    assert_eq!(actual.conversation, expected.conversation);
    assert_eq!(actual.messages, expected.messages);
}

fn assert_checkpoint(journal: &SessionJournal, expected: &SessionJournalSnapshot) {
    let actual = journal.checkpoint_snapshot().unwrap();
    assert_eq!(actual.epoch, expected.epoch);
    assert_eq!(actual.revision, expected.revision);
    assert_eq!(actual.seq, expected.seq);
    assert_eq!(actual.state, expected.state);
    assert_eq!(actual.agent_revisions, expected.agent_revisions);
    assert_eq!(actual.managed_histories, expected.managed_histories);
    assert_eq!(actual.histories.len(), expected.histories.len());
    for (id, history) in &expected.histories {
        assert_history(&actual.histories[id], history);
    }
}

fn assert_dormant(manager: &AgentManagerHandle, id: AgentId) {
    let node = manager.node(id).unwrap();
    assert_eq!(node.graph_lifecycle, GraphLifecycle::Dormant);
    assert_eq!(node.residency, AgentResidency::Dormant);
    assert!(node.actor.is_none());
    assert!(matches!(manager.actor(id), Err(ManagerError::NonLiveAgent(actual)) if actual == id));
}

#[test]
fn restore_preserves_dormant_root_and_readable_terminal_children() {
    let records = records();
    let journal = journal(&records);
    let before = journal.checkpoint_snapshot().unwrap();
    let manager = restore(&journal, AgentLimits::default()).unwrap();
    let root = manager.root().unwrap();
    assert_eq!(root.id(), records[0].id);
    assert_dormant(&manager, root.id());
    assert_eq!(root.snapshot().unwrap().children.len(), records.len() - 1);
    for (record, lifecycle) in records.iter().zip([
        GraphLifecycle::Dormant,
        GraphLifecycle::Dormant,
        GraphLifecycle::Closed,
        GraphLifecycle::Retired,
    ]) {
        let node = manager.lookup(record.id).unwrap().snapshot().unwrap();
        assert_eq!(node.graph_lifecycle, lifecycle);
        assert_eq!(node.parent_id, record.parent);
        assert_eq!(node.root_id, root.id());
        assert_eq!(node.depth, record.depth as usize);
        assert_eq!(node.metadata.label, record.label);
        assert_eq!(
            node.metadata.spawned_by_tool_use_id,
            record.spawned_by_tool_use_id
        );
        assert!(node.actor.is_none());
    }
    smol::block_on(async {
        for record in &records[2..] {
            let error = manager
                .ensure_resident_with(record.id, |_| async {
                    panic!("terminal identity must not invoke the factory")
                })
                .await
                .unwrap_err();
            assert_eq!(
                error,
                if record.lifecycle == StoredAgentLifecycle::Retired {
                    ManagerError::Retired(record.id)
                } else {
                    ManagerError::NonLiveAgent(record.id)
                }
            );
        }
    });
    assert_checkpoint(&journal, &before);
}

#[test_case(false; "child")]
#[test_case(true; "root")]
fn gated_singleflight_preserves_identity_and_hides_uninitialized_actor(root: bool) {
    smol::block_on(async {
        let records = records();
        let journal = journal(&records);
        let before = journal.checkpoint_snapshot().unwrap();
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let id = records[usize::from(!root)].id;
        let saved_record = journal.agent(id).unwrap();
        let saved_history = before.histories[&id].clone();
        let (factory_release, factory_wait) = flume::bounded(1);
        let (init_release, init_wait) = flume::bounded(1);
        let (initialized, initialization) = flume::bounded(1);
        let mut calls = 0;
        let mut first = Box::pin(manager.ensure_resident_with(id, |input| {
            calls += 1;
            assert_eq!(input.record, saved_record);
            assert_history(&input.history, &saved_history);
            assert_eq!(input.config.as_ref().unwrap().model, MODEL);
            assert!(input.config.as_ref().unwrap().fast);
            async move {
                factory_wait.recv_async().await.unwrap();
                let mut resident = resident();
                resident.initialize = Some(Box::new(move |input| Box::pin(async move {
                    assert_eq!(input.agent.id(), id);
                    assert_eq!(input.actor.agent_id(), id);
                    assert!(matches!(input.agent.actor(), Err(ManagerError::NonLiveAgent(actual)) if actual == id));
                    input.agent.manager().validate_incarnation(&input.incarnation).unwrap();
                    assert_history(&input.actor.committed_history_snapshot(), &saved_history);
                    initialized.send((input.actor.identity(), input.incarnation)).unwrap();
                    init_wait.recv_async().await.unwrap();
                    Ok(())
                })));
                Ok(resident)
            }
        }));
        assert!(poll_once(first.as_mut()).await.is_none());
        let mut sibling = Box::pin(manager.ensure_resident_with(id, |_| async {
            panic!("singleflight waiter must not invoke its factory")
        }));
        assert!(poll_once(sibling.as_mut()).await.is_none());
        factory_release.send(()).unwrap();
        assert!(poll_once(first.as_mut()).await.is_none());
        let (identity, incarnation) = initialization.recv().unwrap();
        assert_eq!(
            manager.node(id).unwrap().residency,
            AgentResidency::Activating
        );
        assert!(!manager.is_live(id));
        assert!(
            matches!(manager.actor(id), Err(ManagerError::NonLiveAgent(actual)) if actual == id)
        );
        assert!(poll_once(sibling.as_mut()).await.is_none());
        assert_checkpoint(&journal, &before);
        init_release.send(()).unwrap();
        let first = first.await.unwrap();
        let sibling = sibling.await.unwrap();
        assert_eq!(calls, 1);
        assert!(Arc::ptr_eq(&identity, &first.actor().unwrap().identity()));
        assert!(Arc::ptr_eq(&identity, &sibling.actor().unwrap().identity()));
        assert_eq!(manager.incarnation(id).unwrap(), incarnation);
        let again = manager
            .ensure_resident_with(id, |_| async {
                panic!("resident identity must not invoke another factory")
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&identity, &again.actor().unwrap().identity()));
        assert_eq!(manager.list().len(), records.len());
        assert_checkpoint(&journal, &before);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(ReactivationError::ProviderUnavailable { provider: "offline".into() }; "provider")]
#[test_case(ReactivationError::ToolUnavailable { tool: "missing".into() }; "tool")]
#[test_case(ReactivationError::McpUnavailable { server: "offline".into() }; "mcp")]
fn typed_factory_failure_preserves_exact_checkpoint_and_allows_retry(failure: ReactivationError) {
    smol::block_on(async {
        let records = records();
        let id = records[1].id;
        let journal = journal(&records);
        let before = journal.checkpoint_snapshot().unwrap();
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let expected = ManagerError::Reactivation(failure.clone());
        assert_eq!(
            manager
                .ensure_resident_with(id, |_| async { Err(failure) })
                .await
                .unwrap_err(),
            expected
        );
        assert_dormant(&manager, id);
        assert_checkpoint(&journal, &before);
        manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        assert!(manager.is_live(id));
        assert_checkpoint(&journal, &before);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false; "factory")]
#[test_case(true; "initializer")]
fn cancelled_activation_releases_waiter_and_retries_with_new_incarnation(initialize: bool) {
    smol::block_on(async {
        let records = records();
        let id = records[1].id;
        let journal = journal(&records);
        let before = journal.checkpoint_snapshot().unwrap();
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (_release, wait) = flume::bounded::<()>(1);
        let mut first = Box::pin(manager.ensure_resident_with(id, |_| async move {
            let mut resident = resident();
            if initialize {
                resident.initialize = Some(Box::new(move |_| {
                    Box::pin(async move {
                        wait.recv_async().await.unwrap();
                        Ok(())
                    })
                }));
            } else {
                wait.recv_async().await.unwrap();
            }
            Ok(resident)
        }));
        assert!(poll_once(first.as_mut()).await.is_none());
        let old = manager.incarnation(id).unwrap();
        let mut sibling = Box::pin(manager.ensure_resident_with(id, |_| async {
            panic!("cancelled waiter must not invoke its factory")
        }));
        assert!(poll_once(sibling.as_mut()).await.is_none());
        drop(first);
        assert_eq!(
            sibling.await.unwrap_err(),
            ManagerError::Reactivation(ReactivationError::Cancelled)
        );
        assert_dormant(&manager, id);
        assert_checkpoint(&journal, &before);
        let agent = manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let identity = agent.actor().unwrap().identity();
        assert_ne!(manager.incarnation(id).unwrap(), old);
        assert_eq!(
            manager.detach_incarnation(&old),
            Err(ManagerError::StaleIncarnation(id))
        );
        assert!(manager.is_live(id));
        assert!(Arc::ptr_eq(
            &identity,
            &manager.actor(id).unwrap().identity()
        ));
        assert_checkpoint(&journal, &before);
        let current = manager.incarnation(id).unwrap();
        manager.detach_incarnation(&current).unwrap();
        assert_dormant(&manager, id);
        assert_checkpoint(&journal, &before);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false; "typed_failure")]
#[test_case(true; "callback_closed_actor")]
fn initializer_failure_discards_hidden_actor_and_retries(close_actor: bool) {
    smol::block_on(async {
        let records = records();
        let id = records[1].id;
        let journal = journal(&records);
        let before = journal.checkpoint_snapshot().unwrap();
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (sent, received) = flume::bounded(1);
        let error = manager
            .ensure_resident_with(id, |_| async {
                let mut resident = resident();
                resident.initialize = Some(Box::new(move |input| {
                    Box::pin(async move {
                        sent.send(input.actor.identity()).unwrap();
                        if close_actor {
                            input.actor.close();
                            Ok(())
                        } else {
                            Err(ReactivationError::Initialization(
                                INITIALIZATION_ERROR.into(),
                            ))
                        }
                    })
                }));
                Ok(resident)
            })
            .await
            .unwrap_err();
        if !close_actor {
            assert_eq!(
                error,
                ManagerError::Reactivation(ReactivationError::Initialization(
                    INITIALIZATION_ERROR.into()
                ))
            );
        }
        let failed_identity = received.recv().unwrap();
        assert_dormant(&manager, id);
        assert_checkpoint(&journal, &before);
        let agent = manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(
            &failed_identity,
            &agent.actor().unwrap().identity()
        ));
        assert_checkpoint(&journal, &before);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false; "tool_ceiling")]
#[test_case(true; "write_ceiling")]
fn widened_restored_ceiling_fails_before_actor_or_journal_mutation(write: bool) {
    smol::block_on(async {
        let mut records = records();
        let id = records[1].id;
        records[1].spec = Some(StoredAgentSpec {
            model: Default::default(),
            system: String::new(),
            tools: Vec::new(),
            audience: String::new(),
            mcp: false,
            output_schema: None,
            name: None,
            silent: true,
            auto_deliver: false,
            inherit_provider: true,
            plugin: None,
            mode_ceiling: StoredModeCeiling {
                restrict_write_to: write.then(|| "allowed".into()),
                tools: (!write).then(|| vec!["read".into()]),
            },
        });
        let journal = journal(&records);
        let before = journal.checkpoint_snapshot().unwrap();
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let error = manager
            .ensure_resident_with(id, |_| async {
                let mut resident = resident();
                resident.initialize = Some(Box::new(|_| {
                    Box::pin(async { panic!("invalid ceiling must not reach initialization") })
                }));
                Ok(resident)
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ManagerError::Reactivation(ReactivationError::ModeCeiling(_))
        ));
        assert_dormant(&manager, id);
        assert_checkpoint(&journal, &before);
    });
}

#[derive(Clone)]
enum InvalidGraph {
    MissingParent,
    Cycle,
    Depth,
    Pending,
}

#[test_case(InvalidGraph::MissingParent; "missing_parent")]
#[test_case(InvalidGraph::Cycle; "parent_cycle")]
#[test_case(InvalidGraph::Depth; "invalid_depth")]
#[test_case(InvalidGraph::Pending; "pending_admission")]
fn rejects_invalid_coherent_checkpoint_without_mutation(invalid: InvalidGraph) {
    let mut records = records();
    match invalid {
        InvalidGraph::MissingParent => records[1].parent = Some(AgentId::generate()),
        InvalidGraph::Cycle => {
            records[1].parent = Some(records[2].id);
            records[2].parent = Some(records[1].id);
        }
        InvalidGraph::Depth => records[1].depth += 1,
        InvalidGraph::Pending => {}
    }
    let journal = journal(&records);
    if matches!(invalid, InvalidGraph::Pending) {
        journal
            .commit(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(records[1].id),
                    generation: 0,
                    change: StateChange::Admitted {
                        turn: TurnId::generate(),
                        origin: StoredTurnOrigin::User,
                    },
                }],
                ..Default::default()
            })
            .unwrap();
    }
    let before = journal.checkpoint_snapshot().unwrap();
    assert!(matches!(
        restore(&journal, AgentLimits::default()),
        Err(ManagerError::InvalidGraph(_))
    ));
    assert_checkpoint(&journal, &before);
}

#[test_case(false; "state_view")]
#[test_case(true; "history")]
fn rejects_mixed_checkpoint_inputs(history: bool) {
    let records = records();
    let journal = journal(&records);
    let before = journal.checkpoint_snapshot().unwrap();
    let mut view =
        SessionStateView::fold(&before.state, &before.agent_revisions, before.seq).unwrap();
    let mut histories = before.histories.clone();
    if history {
        histories.get_mut(&records[1].id).unwrap().epoch += 1;
    } else {
        view.agents.get_mut(&records[1].id).unwrap().label = None;
    }
    assert!(matches!(
        AgentManagerHandle::restore_graph(
            AgentLimits::default(),
            Arc::clone(&journal),
            view,
            histories
        ),
        Err(ManagerError::InvalidGraph(_))
    ));
    assert_checkpoint(&journal, &before);
}

#[test_case(false; "factory")]
#[test_case(true; "initializer")]
fn detached_activation_cannot_publish_or_roll_back_replacement(initialize: bool) {
    smol::block_on(async {
        let records = records();
        let id = records[1].id;
        let journal = journal(&records);
        let before = journal.checkpoint_snapshot().unwrap();
        let manager = restore(&journal, AgentLimits::default()).unwrap();
        let (release, wait) = flume::bounded(1);
        let mut stale = Box::pin(manager.ensure_resident_with(id, |_| async move {
            let mut resident = resident();
            if initialize {
                resident.initialize = Some(Box::new(move |_| {
                    Box::pin(async move {
                        wait.recv_async().await.unwrap();
                        Ok(())
                    })
                }));
            } else {
                wait.recv_async().await.unwrap();
                resident.initialize = Some(Box::new(|_| {
                    Box::pin(async { panic!("stale factory must not reach initialization") })
                }));
            }
            Ok(resident)
        }));
        assert!(poll_once(stale.as_mut()).await.is_none());
        let old = manager.incarnation(id).unwrap();
        manager.detach_incarnation(&old).unwrap();
        assert_eq!(
            manager.validate_incarnation(&old),
            Err(ManagerError::StaleIncarnation(id))
        );
        assert_dormant(&manager, id);
        let replacement = manager
            .ensure_resident_with(id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let current = manager.incarnation(id).unwrap();
        let identity = replacement.actor().unwrap().identity();
        release.send(()).unwrap();
        assert_eq!(stale.await.unwrap_err(), ManagerError::StaleIncarnation(id));
        assert!(manager.is_live(id));
        assert_eq!(manager.incarnation(id).unwrap(), current);
        assert!(Arc::ptr_eq(
            &identity,
            &manager.actor(id).unwrap().identity()
        ));
        manager.validate_incarnation(&current).unwrap();
        assert_checkpoint(&journal, &before);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn folded_legacy_identities_are_readable_but_exempt_from_retention_quota() {
    smol::block_on(async {
        let mut records = records();
        records[2].legacy = true;
        records[2].ancestry_unknown = true;
        let journal = journal(&records);
        let journal = rebuild_journal(journal, records[0].id);
        let quota = 3;
        let manager = restore(
            &journal,
            AgentLimits {
                max_retained_agents: quota,
                ..AgentLimits::default()
            },
        )
        .unwrap();
        let root = manager
            .ensure_resident_with(records[0].id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let spawn = || {
            manager.spawn_child_trusted_with_config(
                &root,
                config(),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| Ok::<_, String>(Box::new(OfflineBackend) as Box<dyn ActorBackend>),
            )
        };
        let child = spawn().unwrap();
        assert!(manager.lookup(records[2].id).is_ok());
        assert_eq!(
            manager.node(records[2].id).unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
        assert_eq!(
            spawn().unwrap_err(),
            ManagerError::RetentionLimit { max: quota }
        );
        child.close_subtree().unwrap();
        assert_eq!(
            spawn().unwrap_err(),
            ManagerError::RetentionLimit { max: quota }
        );
        manager.retire_trusted(&child).unwrap();
        let replacement = spawn().unwrap();
        assert_ne!(replacement.id(), child.id());
        assert_eq!(
            journal.agent(child.id()).unwrap().lifecycle,
            StoredAgentLifecycle::Retired
        );
        assert!(journal.agent(records[2].id).unwrap().legacy);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn retirement_preserves_identity_and_releases_retention_capacity() {
    smol::block_on(async {
        let records = records();
        let id = records[1].id;
        let journal = journal(&records);
        let retention_limit = RETAINED_AGENTS - 1;
        let manager = restore(
            &journal,
            AgentLimits {
                max_retained_agents: retention_limit,
                ..AgentLimits::default()
            },
        )
        .unwrap();
        let root = manager
            .ensure_resident_with(records[0].id, |_| async { Ok(resident()) })
            .await
            .unwrap();
        let before = journal.checkpoint_snapshot().unwrap();
        let error = manager
            .spawn_child_trusted_with_config(
                &root,
                config(),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| -> Result<Box<dyn ActorBackend>, String> {
                    panic!("retention failure must precede backend construction")
                },
            )
            .unwrap_err();
        assert_eq!(
            error,
            ManagerError::RetentionLimit {
                max: retention_limit
            }
        );
        assert_checkpoint(&journal, &before);
        assert_eq!(
            manager.retire_trusted(&root),
            Err(ManagerError::NotLeaf(root.id()))
        );
        let turn = TurnId::generate();
        let actor = root.actor().unwrap();
        {
            let mut state = actor.inner.state.lock().unwrap();
            state.status = crate::ActorStatus::Running(turn);
            state.active = Some(ActiveCancel::new(None).0);
        }
        let (_cancel, token) = ReasonedCancelToken::new();
        let (guard, current) =
            super::enter_managed_turn(&Arc::downgrade(&manager.0), root.id(), turn, &token)
                .await
                .unwrap();
        drop(guard);
        {
            let mut state = actor.inner.state.lock().unwrap();
            state.status = crate::ActorStatus::Idle;
            state.active = None;
        }
        assert_eq!(
            manager.retire(&current, id),
            Err(ManagerError::InactiveTurn {
                agent_id: root.id(),
                turn_id: turn
            })
        );
        assert_checkpoint(&journal, &before);
        let child = manager.lookup(id).unwrap();
        manager.retire_trusted(&child).unwrap();
        let retired = journal.agent(id).unwrap();
        assert_eq!(retired.lifecycle, StoredAgentLifecycle::Retired);
        assert_eq!(
            child.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Retired
        );
        let before = journal.checkpoint_snapshot().unwrap();
        manager.retire_trusted(&child).unwrap();
        assert_eq!(
            manager
                .ensure_resident_with(id, |_| async {
                    panic!("retired identity must not invoke a factory")
                })
                .await
                .unwrap_err(),
            ManagerError::Retired(id)
        );
        assert_checkpoint(&journal, &before);
        let replacement = manager
            .spawn_child_trusted_with_config(
                &root,
                config(),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(OfflineBackend)),
            )
            .unwrap();
        assert_ne!(replacement.id(), id);
        assert_eq!(manager.list().len(), RETAINED_AGENTS + 1);
        assert_eq!(journal.agent(id).unwrap(), retired);
        assert_history(
            &journal.checkpoint_snapshot().unwrap().histories[&id],
            &before.histories[&id],
        );
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}
