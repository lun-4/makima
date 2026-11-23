use super::{
    AgentLimits, AgentManagerHandle, AgentResidency, ManagerError, ReactivationError, ResidentAgent,
};
use crate::{
    ActorBackend, AgentActorHandle, AgentId, AgentInput, AgentMode, BackendResult, ControlWork,
    DoneReason, History, ModeDef, ModeId, RunSettings, ToolOutput, TurnContext, TurnId,
    TurnOutcome, WorkKind,
    actor::{
        ActorHistory, ChildDelivery, EffectiveAgentConfig, MAX_DELIVERY_PREVIEW_BYTES,
        MAX_PENDING_DELIVERIES, MAX_PENDING_PREVIEW_BYTES, TurnProvenance, TurnResult,
    },
    journal::{JournalChange, JournalCommit, SessionJournal},
    session_recovery::{CONTINUATION_DISABLED, RecoveredSession, RecoveryCandidate},
    session_writer::{Record, SessionWriter, forget},
    test_util::MockBackend,
};
use futures_lite::future::or;
use makima_providers::{Message, Model, ObservationSource, TokenUsage, provider::unconfigured};
use makima_storage::{
    checkpoint::CheckpointPublication,
    session_state::{
        StateChange, StoredAgentConfig, StoredAgentLifecycle, StoredAgentRecord, StoredAgentSpec,
        StoredDeliveryOp, StoredModeCeiling, StoredStateRecord, StoredTurnOrigin, StoredTurnStatus,
    },
    sessions::Session,
    test_util::test_record,
};
use std::{
    collections::BTreeMap,
    fs::{File, read},
    future::Future,
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use test_case::test_case;
const MODEL: &str = "test/restart-model";
const INITIAL: &str = "retained history before report";
const REPORT: &str = "disk-backed child report";
const REACTIVATION_FAILURE: &str = "restored model is unavailable";
const SUMMARY: &str = "compacted retained history";
const CHILD_KEY: &str = "restart-child";
const UNPUBLISHED: &str = "unpublished-after-checkpoint";
const PLUGIN_VERSION: u32 = 1;
const PRESENTATION_ID: u64 = 71;
const TIMEOUT: Duration = Duration::from_secs(5);
const FAILURE_REPORT_TIMEOUT: &str = "restored child failure report did not settle";
const FAILURE_REPORT_MISSING: &str = "restored child failure report did not execute";
const TERMINAL_RECORD_MISSING: &str = "recovery terminal record is missing from the log";
const QUEUED_RECORD_MISSING: &str = "recovery report record is missing from the log";
const PUBLICATION_REJECTED: &str = "recovery publication rejected";

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

#[derive(Debug, PartialEq, Eq)]
enum Progress {
    Turn(TurnId),
    Compacted,
    Controlled,
    Dropped,
}
struct ReportBackend {
    executions: Arc<AtomicUsize>,
    progress: flume::Sender<Progress>,
    expected_failure: Option<&'static str>,
}
impl Drop for ReportBackend {
    fn drop(&mut self) {
        let _ = self.progress.send(Progress::Dropped);
    }
}
impl ActorBackend for ReportBackend {
    fn run_turn<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            let inbox = context.inbox.unwrap();
            let mut consumed = 0;
            inbox.commit_completions(&mut |message| {
                let (_, envelope) = message
                    .first_text_content()
                    .unwrap()
                    .split_once('\n')
                    .unwrap();
                let envelope: serde_json::Value = serde_json::from_str(envelope).unwrap();
                if let Some(diagnostic) = self.expected_failure {
                    let outcome: TurnOutcome =
                        serde_json::from_value(envelope["outcome"].clone()).unwrap();
                    let TurnOutcome::Failed { failure, .. } = outcome else {
                        panic!("expected restored child failure");
                    };
                    assert_eq!(failure.diagnostic, diagnostic);
                    assert_eq!(
                        context
                            .managed_turn
                            .as_ref()
                            .unwrap()
                            .manager()
                            .active_child_count(context.agent_id),
                        1,
                    );
                } else {
                    assert_eq!(envelope["text"].as_str(), Some(REPORT));
                }
                history.push_observation(message).unwrap();
                consumed += 1;
            });
            assert_eq!(consumed, 1);
            assert!(inbox.finish());
            let turn = context.turn_id.unwrap();
            self.progress.send(Progress::Turn(turn)).unwrap();
            BackendResult::EnteredRun(TurnOutcome::completed(
                context.agent_id,
                turn,
                TokenUsage::default(),
                0,
                DoneReason::EndTurn,
            ))
        })
    }
    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.progress.send(Progress::Controlled).unwrap();
            BackendResult::ControlDone
        })
    }
    fn run_compact<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            history
                .replace_compacted(vec![Message::observation(SUMMARY.into())])
                .await
                .unwrap();
            self.progress.send(Progress::Compacted).unwrap();
            BackendResult::CompactDone
        })
    }
}
fn seed() -> (StoredSession, Arc<SessionJournal>, Arc<ChildDelivery>) {
    let root = AgentId::generate();
    let child = AgentId::generate();
    let mut session = StoredSession::new(MODEL, ".");
    session.bind_root_id(root).unwrap();
    let journal = Arc::new(SessionJournal::default());
    let records: Vec<_> = [(root, None), (child, Some(root))]
        .into_iter()
        .map(|(id, parent)| StoredAgentRecord {
            transcript_key: parent.map_or_else(String::new, |_| CHILD_KEY.into()),
            lifecycle: if parent.is_some() {
                StoredAgentLifecycle::Closed
            } else {
                StoredAgentLifecycle::Open
            },
            ..test_record(id, parent)
        })
        .collect();
    let config = StoredAgentConfig {
        cwd: ".".into(),
        model: MODEL.into(),
        thinking: None,
        fast: true,
        workflow: false,
        mode: ModeDef::default_for(ModeId::Build).stored_snapshot(&AgentMode::Build),
        options: BTreeMap::new(),
    };
    let history = History::new(vec![Message::user(INITIAL.into())]);
    let delivery = Arc::new(ChildDelivery {
        label: None,
        result: TurnResult {
            outcome: TurnOutcome::completed(
                child,
                TurnId::generate(),
                TokenUsage::default(),
                0,
                DoneReason::EndTurn,
            ),
            text: REPORT.into(),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        },
    });
    let mut changes: Vec<_> = records
        .iter()
        .map(|record| JournalChange {
            agent: Some(record.id),
            generation: 0,
            change: StateChange::Config(config.clone()),
        })
        .collect();
    changes.push(JournalChange {
        agent: Some(child),
        generation: 0,
        change: StateChange::Turn {
            turn: delivery.result.outcome.turn_id(),
            result: delivery
                .result
                .to_stored(journal.snapshot().epoch, None)
                .unwrap(),
        },
    });
    journal
        .commit(JournalCommit {
            histories: records
                .iter()
                .map(|record| (record.id, history.snapshot().clone()))
                .collect(),
            managed_histories: records.iter().map(|record| record.id).collect(),
            agents: records,
            changes,
            ..Default::default()
        })
        .unwrap();
    (session, journal, delivery)
}
fn restore(journal: &Arc<SessionJournal>) -> AgentManagerHandle {
    let snapshot = journal.checkpoint_snapshot().unwrap();
    let view = snapshot.fold().unwrap();
    AgentManagerHandle::restore_graph(
        AgentLimits::default(),
        Arc::clone(journal),
        view,
        snapshot.histories,
    )
    .unwrap()
}
async fn activate(
    manager: &AgentManagerHandle,
    parent: AgentId,
    executions: &Arc<AtomicUsize>,
    progress: &flume::Sender<Progress>,
    expected_failure: Option<&'static str>,
) -> (AgentActorHandle, flume::Sender<()>) {
    let (start, wait) = flume::bounded(1);
    let backend = ReportBackend {
        executions: Arc::clone(executions),
        progress: progress.clone(),
        expected_failure,
    };
    let agent = manager
        .ensure_resident_with(parent, |_| async move {
            Ok(resident_agent(backend, Some(wait)))
        })
        .await
        .unwrap();
    (agent.actor().unwrap(), start)
}

fn resident_agent(
    backend: impl ActorBackend + 'static,
    start: Option<flume::Receiver<()>>,
) -> ResidentAgent {
    ResidentAgent {
        backend: Box::new(backend),
        config: EffectiveAgentConfig::new(
            RunSettings {
                provider: unconfigured(),
                model: Model::from_spec(MODEL).unwrap(),
                fast: true,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
        .with_mode_def(Some(ModeDef::default_for(ModeId::Build))),
        shared_messages: None,
        start,
        initialize: None,
    }
}

async fn publish(
    journal: &SessionJournal,
    session: &mut StoredSession,
    directory: &Path,
) -> StoredSession {
    let checkpoint = journal.capture_checkpoint().await.unwrap();
    checkpoint.snapshot().apply_to(session).unwrap();
    session.save_to(directory).unwrap();
    journal.acknowledge(checkpoint.snapshot()).unwrap();
    StoredSession::load_from(session.id, directory).unwrap()
}
async fn crash(manager: AgentManagerHandle, events: &flume::Receiver<Progress>, resident: bool) {
    let tasks: Vec<_> = manager
        .lock_graph()
        .nodes
        .values_mut()
        .filter_map(|node| node.task.take())
        .collect();
    for task in tasks {
        task.cancel().await;
    }
    if resident {
        assert_eq!(events.recv_async().await.unwrap(), Progress::Dropped);
    }
    drop(manager);
}
fn report_count(session: &StoredSession, child: AgentId, turn: TurnId) -> usize {
    let source = ObservationSource::ChildResult {
        agent_id: child.to_string(),
        turn_id: turn.to_string(),
    };
    session
        .messages()
        .iter()
        .filter(|message| message.observation_source.as_ref() == Some(&source))
        .count()
}
#[test_case(false, false; "dormant_consumed")]
#[test_case(true, false; "resident_consumed")]
#[test_case(false, true; "dormant_withdrawn")]
#[test_case(true, true; "resident_withdrawn")]
fn report_restart_preserves_published_prefix_and_terminal_dedup(resident: bool, withdraw: bool) {
    smol::block_on(or(scenario(resident, withdraw), async {
        smol::Timer::after(TIMEOUT).await;
        panic!("disk-backed report restart did not settle");
    }));
}
async fn scenario(resident: bool, withdraw: bool) {
    let directory = TempDir::new().unwrap();
    let (mut session, journal, delivery) = seed();
    let parent = session.root_agent_id().unwrap();
    let identity = (
        delivery.result.outcome.agent_id(),
        delivery.result.outcome.turn_id(),
    );
    let session_id = session.id;
    let executions = Arc::new(AtomicUsize::new(0));
    let (progress, events) = flume::unbounded();
    let manager = restore(&journal);
    assert_eq!(
        manager.node(parent).unwrap().residency,
        AgentResidency::Dormant
    );
    let parked = if resident {
        Some(activate(&manager, parent, &executions, &progress, None).await)
    } else {
        None
    };
    manager.deliver(parent, Arc::clone(&delivery)).unwrap();
    let saved = publish(&journal, &mut session, directory.path()).await;
    let queued = saved.latest_state().unwrap();
    assert!(matches!(
        &queued.scopes[&Some(parent)].deliveries[&identity],
        StoredDeliveryOp::Queued(_)
    ));
    assert_eq!(report_count(&saved, identity.0, identity.1), 0);
    journal
        .commit(JournalCommit {
            changes: vec![JournalChange {
                agent: Some(parent),
                generation: 0,
                change: StateChange::Plugin {
                    key: UNPUBLISHED.into(),
                    version: PLUGIN_VERSION,
                    value: Some(true.into()),
                },
            }],
            ..Default::default()
        })
        .unwrap();
    assert!(journal.snapshot().seq > saved.state().last().unwrap().seq);
    crash(manager, &events, resident).await;
    drop(parked);
    drop(journal);
    drop(session);
    drop(saved);
    let mut session = StoredSession::load_from(session_id, directory.path()).unwrap();
    assert_eq!(session.id, session_id);
    assert_eq!(session.root_agent_id(), Some(parent));
    assert!(
        !session.latest_state().unwrap().scopes[&Some(parent)]
            .plugins
            .contains_key(UNPUBLISHED)
    );
    let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
    let manager = restore(&journal);
    let (actor, start) = activate(&manager, parent, &executions, &progress, None).await;
    assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
    if withdraw {
        assert!(actor.withdraw_child_delivery(identity.1));
    }
    start.send(()).unwrap();
    if !withdraw {
        let Progress::Turn(turn) = events.recv_async().await.unwrap() else {
            panic!("report did not execute")
        };
        journal
            .wait_terminal_conversion(parent, turn)
            .await
            .unwrap();
    }
    manager.wait_settled().await;
    let terminal = if withdraw {
        StoredDeliveryOp::Withdrawn
    } else {
        StoredDeliveryOp::Consumed
    };
    let expected = usize::from(!withdraw);
    let saved = publish(&journal, &mut session, directory.path()).await;
    assert_eq!(
        saved.latest_state().unwrap().scopes[&Some(parent)].deliveries[&identity],
        terminal
    );
    assert_eq!(report_count(&saved, identity.0, identity.1), expected);
    assert_eq!(executions.load(Ordering::SeqCst), expected);
    drop(actor);
    crash(manager, &events, true).await;
    drop(journal);
    drop(session);
    drop(saved);
    for compact in [true, false] {
        let mut session = StoredSession::load_from(session_id, directory.path()).unwrap();
        assert_eq!(
            session.latest_state().unwrap().scopes[&Some(parent)].deliveries[&identity],
            terminal
        );
        assert_eq!(
            report_count(&session, identity.0, identity.1),
            if compact { expected } else { 0 }
        );
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        let manager = restore(&journal);
        manager.deliver(parent, Arc::clone(&delivery)).unwrap();
        let (actor, start) = activate(&manager, parent, &executions, &progress, None).await;
        manager.deliver(parent, Arc::clone(&delivery)).unwrap();
        assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
        start.send(()).unwrap();
        actor
            .push_control(SUMMARY.into(), SUMMARY.into(), None)
            .unwrap();
        assert_eq!(events.recv_async().await.unwrap(), Progress::Controlled);
        manager.wait_settled().await;
        if compact {
            let epoch = journal.snapshot().histories[&parent].epoch;
            actor.push_compact(PRESENTATION_ID, None).unwrap();
            assert_eq!(events.recv_async().await.unwrap(), Progress::Compacted);
            manager.wait_settled().await;
            assert_ne!(journal.snapshot().histories[&parent].epoch, epoch);
            let saved = publish(&journal, &mut session, directory.path()).await;
            assert!(saved.history_snapshot().compacted);
            assert_eq!(saved.messages()[0].first_text_content(), Some(SUMMARY));
            assert_eq!(report_count(&saved, identity.0, identity.1), 0);
            assert_eq!(
                saved.latest_state().unwrap().scopes[&Some(parent)].deliveries[&identity],
                terminal
            );
        }
        assert_eq!(executions.load(Ordering::SeqCst), expected);
        assert!(events.is_empty());
        drop(actor);
        crash(manager, &events, true).await;
    }
}

#[test_case(true, None; "resident")]
#[test_case(false, None; "dormant")]
#[test_case(false, Some(false); "crash_before_terminal")]
#[test_case(false, Some(true); "crash_after_terminal")]
fn restored_child_failure_reaches_parent_exactly_once(
    resident: bool,
    terminal_on_disk: Option<bool>,
) {
    smol::block_on(or(
        restored_child_failure_scenario(resident, terminal_on_disk),
        async {
            smol::Timer::after(TIMEOUT).await;
            panic!("{FAILURE_REPORT_TIMEOUT}");
        },
    ));
}

#[derive(PartialEq, Eq)]
enum FailurePrefix {
    Queued,
    Terminal,
    Consumed,
}

#[test_case(FailurePrefix::Queued; "after_queue")]
#[test_case(FailurePrefix::Terminal; "after_terminal")]
#[test_case(FailurePrefix::Consumed; "after_consumption")]
fn disabled_continuation_failure_reaches_parent_exactly_once(prefix: FailurePrefix) {
    smol::block_on(or(disabled_continuation_scenario(prefix), async {
        smol::Timer::after(TIMEOUT).await;
        panic!("{FAILURE_REPORT_TIMEOUT}");
    }));
}

async fn disabled_continuation_scenario(prefix: FailurePrefix) {
    let directory = TempDir::new().unwrap();
    let directory = directory.path();
    let (session, child, turn) = seed_recovery_failure(directory).await;
    let id = session.id;
    let parent = session.root_agent_id().unwrap();
    recover(directory, session, false).await;
    forget(directory, id).unwrap();
    let path = directory.join(format!("{id}.jsonl"));
    let is_report = |record: &StoredStateRecord| {
        record.agent == Some(parent)
            && matches!(record.change, StateChange::Delivery {
                id, op: StoredDeliveryOp::Queued(_)
            } if id == (child, turn))
    };
    let terminal_on_disk = prefix == FailurePrefix::Terminal;
    if terminal_on_disk {
        truncate_log(
            &path,
            true,
            |record| {
                record.agent == Some(child)
                    && matches!(record.change, StateChange::Turn { turn: terminal, .. } if terminal == turn)
            },
            TERMINAL_RECORD_MISSING,
        );
    } else {
        truncate_log(&path, true, is_report, QUEUED_RECORD_MISSING);
    }
    let mut session = StoredSession::load_from(id, directory).unwrap();
    let view = session.latest_state().unwrap();
    assert_eq!(
        view.scopes[&Some(child)].admitted.contains_key(&turn),
        !terminal_on_disk
    );
    let Some(StoredDeliveryOp::Queued(report)) =
        view.scopes[&Some(parent)].deliveries.get(&(child, turn))
    else {
        panic!("{QUEUED_RECORD_MISSING}");
    };
    let executions = Arc::new(AtomicUsize::new(0));
    let (progress, events) = flume::unbounded();
    let activate_parent = async |manager: &AgentManagerHandle| {
        let (actor, start) = activate(
            manager,
            parent,
            &executions,
            &progress,
            Some(CONTINUATION_DISABLED),
        )
        .await;
        start.send(()).unwrap();
        actor
    };
    let already_consumed = prefix == FailurePrefix::Consumed;
    if already_consumed {
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        journal
            .bind_writer(SessionWriter::open_session(directory, &session).unwrap())
            .unwrap();
        let manager = restore(&journal);
        let actor = activate_parent(&manager).await;
        wait_for_report(&journal, parent, &events).await;
        manager.wait_settled().await;
        journal.flush().await.unwrap();
        drop(actor);
        crash(manager, &events, true).await;
        forget(directory, id).unwrap();
        session = StoredSession::load_from(id, directory).unwrap();
        let view = session.latest_state().unwrap();
        assert!(view.scopes[&Some(child)].admitted.contains_key(&turn));
        assert_eq!(
            view.scopes[&Some(parent)].deliveries[&(child, turn)],
            StoredDeliveryOp::Consumed
        );
    }
    let recovered = recover(directory, session, true).await;
    let journal = recovered.journal();
    let manager = restore(&journal);
    assert_child_does_not_rerun(&manager, child).await;
    let actor = activate_parent(&manager).await;
    if !already_consumed {
        wait_for_report(&journal, parent, &events).await;
    }
    manager.wait_settled().await;
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    journal.flush().await.unwrap();
    let saved = StoredSession::load_from(id, directory).unwrap();
    let view = saved.latest_state().unwrap();
    let child_scope = &view.scopes[&Some(child)];
    assert!(child_scope.admitted.is_empty());
    assert!(child_scope.results.contains(&(turn, report.result.clone())));
    assert_eq!(
        child_scope
            .results
            .iter()
            .filter(|(id, _)| *id == turn)
            .count(),
        1
    );
    assert_eq!(
        view.scopes[&Some(parent)].deliveries[&(child, turn)],
        StoredDeliveryOp::Consumed
    );
    assert_eq!(
        saved
            .state()
            .iter()
            .filter(|record| is_report(record))
            .count(),
        1
    );
    assert_eq!(report_count(&saved, child, turn), 1);
    drop(actor);
    crash(manager, &events, true).await;
    forget(directory, id).unwrap();
}

async fn seed_recovery_failure(directory: &Path) -> (StoredSession, AgentId, TurnId) {
    let (mut session, journal, delivery) = seed();
    let child = delivery.result.outcome.agent_id();
    let turn = TurnId::generate();
    let mut record = journal.agent(child).unwrap();
    record.lifecycle = StoredAgentLifecycle::Open;
    record.spec = Some(StoredAgentSpec {
        model: Default::default(),
        system: String::new(),
        tools: Vec::new(),
        audience: "general_sub".into(),
        mcp: false,
        output_schema: None,
        name: None,
        silent: false,
        auto_deliver: true,
        inherit_provider: false,
        mode_ceiling: StoredModeCeiling {
            restrict_write_to: None,
            tools: None,
        },
        plugin: None,
    });
    journal
        .commit(JournalCommit {
            agents: vec![record],
            changes: vec![JournalChange {
                agent: Some(child),
                generation: 0,
                change: StateChange::Admitted {
                    turn,
                    origin: StoredTurnOrigin::Internal,
                },
            }],
            ..Default::default()
        })
        .unwrap();
    let session = publish(&journal, &mut session, directory).await;
    (session, child, turn)
}

fn truncate_log(
    path: &Path,
    keep_record: bool,
    matches: impl Fn(&StoredStateRecord) -> bool,
    missing: &str,
) {
    let log = read(path).unwrap();
    let mut offset = 0;
    let boundary = log
        .split_inclusive(|byte| *byte == b'\n')
        .find_map(|line| {
            let start = offset;
            offset += line.len();
            match serde_json::from_slice::<Record>(line).unwrap() {
                Record::State { d } if matches(&d) => {
                    Some(if keep_record { offset } else { start })
                }
                _ => None,
            }
        })
        .expect(missing);
    File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(boundary as u64)
        .unwrap();
}

async fn recover(
    directory: &Path,
    session: StoredSession,
    continue_turns: bool,
) -> RecoveredSession {
    let writer = SessionWriter::open_session(directory, &session).unwrap();
    RecoveryCandidate::prepare_with_continuation(session, continue_turns)
        .unwrap()
        .publish(
            &writer,
            CheckpointPublication::default(),
            Arc::new(|publication| {
                publication
                    .try_publish()
                    .then_some(())
                    .ok_or_else(|| Arc::from(PUBLICATION_REJECTED))
            }),
        )
        .await
        .unwrap()
}

async fn assert_child_does_not_rerun(manager: &AgentManagerHandle, child: AgentId) {
    let child_executions = Arc::new(AtomicUsize::new(0));
    manager
        .ensure_resident_with(child, |_| {
            let child_executions = Arc::clone(&child_executions);
            async move {
                let backend = MockBackend::STRICT.with_turn(move |_, context, _, _| {
                    child_executions.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move {
                        BackendResult::EnteredRun(TurnOutcome::completed(
                            context.agent_id,
                            context.turn_id.unwrap(),
                            TokenUsage::default(),
                            0,
                            DoneReason::EndTurn,
                        ))
                    })
                });
                Ok(resident_agent(backend, None))
            }
        })
        .await
        .unwrap();
    manager.wait_settled().await;
    assert_eq!(child_executions.load(Ordering::SeqCst), 0);
}

async fn wait_for_report(
    journal: &SessionJournal,
    parent: AgentId,
    events: &flume::Receiver<Progress>,
) {
    let Progress::Turn(parent_turn) = events.recv_async().await.unwrap() else {
        panic!("{FAILURE_REPORT_MISSING}");
    };
    journal
        .wait_terminal_conversion(parent, parent_turn)
        .await
        .unwrap();
}

async fn restored_child_failure_scenario(resident: bool, terminal_on_disk: Option<bool>) {
    let directory = TempDir::new().unwrap();
    let directory = directory.path();
    let (mut session, child, turn) = seed_recovery_failure(directory).await;
    let parent = session.root_agent_id().unwrap();
    let mut journal = Arc::new(SessionJournal::from_session(&session).unwrap());
    if terminal_on_disk.is_some() {
        journal
            .bind_writer(SessionWriter::open_session(directory, &session).unwrap())
            .unwrap();
    }
    let mut manager = restore(&journal);
    let executions = Arc::new(AtomicUsize::new(0));
    let (progress, events) = flume::unbounded();
    let activate_parent = async |manager: &AgentManagerHandle| {
        activate(
            manager,
            parent,
            &executions,
            &progress,
            Some(REACTIVATION_FAILURE),
        )
        .await
    };
    let actor = if resident {
        let (actor, start) = activate_parent(&manager).await;
        start.send(()).unwrap();
        manager.wait_settled().await;
        Some(actor)
    } else {
        None
    };
    let error = manager
        .ensure_resident_with(child, |_| async {
            Err(ReactivationError::ModelUnavailable {
                model: MODEL.into(),
                reason: REACTIVATION_FAILURE.into(),
            })
        })
        .await;
    assert!(matches!(
        error,
        Err(ManagerError::Reactivation(
            ReactivationError::ModelUnavailable { .. }
        ))
    ));
    assert_eq!(
        manager
            .fail_restored_turns(child, REACTIVATION_FAILURE.into())
            .await
            .unwrap(),
        1
    );
    if let Some(terminal_on_disk) = terminal_on_disk {
        journal.flush().await.unwrap();
        crash(manager, &events, false).await;
        forget(directory, session.id).unwrap();
        let path = directory.join(format!("{}.jsonl", session.id));
        truncate_log(
            &path,
            terminal_on_disk,
            |record| {
                record.agent == Some(child)
                    && matches!(record.change, StateChange::Turn { turn: terminal, .. } if terminal == turn)
            },
            TERMINAL_RECORD_MISSING,
        );
        session = StoredSession::load_from(session.id, directory).unwrap();
        let view = session.latest_state().unwrap();
        assert_eq!(
            view.scopes[&Some(child)].admitted.contains_key(&turn),
            !terminal_on_disk
        );
        let StoredDeliveryOp::Queued(report) =
            &view.scopes[&Some(parent)].deliveries[&(child, turn)]
        else {
            panic!("{FAILURE_REPORT_MISSING}");
        };
        let recovered = recover(directory, session, true).await;
        session = StoredSession::load_from(recovered.session().id, directory).unwrap();
        journal = recovered.journal();
        manager = restore(&journal);
        assert_child_does_not_rerun(&manager, child).await;
        let view = session.latest_state().unwrap();
        assert!(
            view.scopes[&Some(child)]
                .results
                .contains(&(turn, report.result.clone()))
        );
    } else {
        assert_eq!(
            manager
                .fail_restored_turns(child, REACTIVATION_FAILURE.into())
                .await
                .unwrap(),
            0
        );
    }
    let actor = if let Some(actor) = actor {
        actor
    } else {
        assert_eq!(manager.active_child_count(parent), 1);
        let (actor, start) = activate_parent(&manager).await;
        start.send(()).unwrap();
        actor
    };
    wait_for_report(&journal, parent, &events).await;
    let ticket = manager
        .retained_turn_ticket(&manager.reference(child), turn)
        .unwrap();
    let delivery = Arc::new(ChildDelivery {
        label: None,
        result: ticket.peek_result().unwrap(),
    });
    manager.deliver(parent, Arc::clone(&delivery)).unwrap();
    manager.wait_settled().await;
    assert_eq!(manager.active_child_count(parent), 0);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    let saved = if terminal_on_disk.is_some() {
        journal.flush().await.unwrap();
        StoredSession::load_from(session.id, directory).unwrap()
    } else {
        publish(&journal, &mut session, directory).await
    };
    let view = saved.latest_state().unwrap();
    assert!(view.scopes[&Some(child)].admitted.is_empty());
    assert_eq!(
        view.scopes[&Some(parent)].deliveries[&(child, turn)],
        StoredDeliveryOp::Consumed
    );
    assert_eq!(report_count(&saved, child, turn), 1);
    drop(actor);
    crash(manager, &events, true).await;
    if terminal_on_disk.is_some() {
        forget(directory, session.id).unwrap();
    }
}

fn inbox_delivery(child: AgentId, text: String) -> ChildDelivery {
    ChildDelivery {
        label: None,
        result: TurnResult {
            outcome: TurnOutcome::completed(
                child,
                TurnId::generate(),
                TokenUsage::default(),
                0,
                DoneReason::EndTurn,
            ),
            text,
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        },
    }
}

#[test_case(false; "count_limit")]
#[test_case(true; "byte_limit")]
fn disabled_continuation_does_not_overflow_parent_inbox(byte_limit: bool) {
    smol::block_on(async {
        let directory = TempDir::new().unwrap();
        let directory = directory.path();
        let (mut session, child, turn) = seed_recovery_failure(directory).await;
        let id = session.id;
        let parent = session.root_agent_id().unwrap();
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        let manager = restore(&journal);
        let target_bytes = MAX_DELIVERY_PREVIEW_BYTES / 2;
        let count = if byte_limit {
            MAX_PENDING_PREVIEW_BYTES / target_bytes
        } else {
            MAX_PENDING_DELIVERIES
        };
        for _ in 0..count {
            let mut delivery = inbox_delivery(child, String::new());
            delivery.result.text = if byte_limit {
                let overhead = delivery.input().preamble[0]
                    .first_text_content()
                    .unwrap()
                    .len();
                "x".repeat(target_bytes - overhead)
            } else {
                REPORT.into()
            };
            manager.deliver(parent, Arc::new(delivery)).unwrap();
        }
        let session = publish(&journal, &mut session, directory).await;
        let view = session.latest_state().unwrap();
        let original_inbox = &view.scopes[&Some(parent)].deliveries;
        assert_eq!(original_inbox.len(), count);
        if byte_limit {
            let bytes: usize = original_inbox
                .values()
                .map(|op| {
                    let StoredDeliveryOp::Queued(delivery) = op else {
                        panic!("{QUEUED_RECORD_MISSING}");
                    };
                    delivery.preview_bytes.unwrap()
                })
                .sum();
            assert_eq!(bytes, MAX_PENDING_PREVIEW_BYTES);
        }
        drop(manager);
        drop(journal);
        let recovered = recover(directory, session, false).await;
        let saved = StoredSession::load_from(id, directory).unwrap();
        let view = saved.latest_state().unwrap();
        assert_eq!(&view.scopes[&Some(parent)].deliveries, original_inbox);
        let scope = &view.scopes[&Some(child)];
        assert!(!scope.admitted.contains_key(&turn));
        assert!(scope.results.iter().any(|(id, result)| {
            *id == turn
                && matches!(&result.outcome.status, StoredTurnStatus::Failed { failure }
                if failure.diagnostic == CONTINUATION_DISABLED)
        }));
        drop(recovered);
        forget(directory, id).unwrap();
    });
}

#[test_case(true; "resident")]
#[test_case(false; "dormant")]
fn restored_child_failure_does_not_overflow_parent_inbox(resident: bool) {
    smol::block_on(async {
        let directory = TempDir::new().unwrap();
        let (session, child, turn) = seed_recovery_failure(directory.path()).await;
        let parent = session.root_agent_id().unwrap();
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        let manager = restore(&journal);
        let (progress, events) = flume::unbounded();
        let executions = Arc::new(AtomicUsize::new(0));
        let parked = if resident {
            Some(activate(&manager, parent, &executions, &progress, None).await)
        } else {
            None
        };
        for _ in 0..MAX_PENDING_DELIVERIES {
            manager
                .deliver(parent, Arc::new(inbox_delivery(child, REPORT.into())))
                .unwrap();
        }
        assert_eq!(
            manager
                .fail_restored_turns(child, REACTIVATION_FAILURE.into())
                .await
                .unwrap(),
            1
        );
        let view = journal
            .capture_checkpoint()
            .await
            .unwrap()
            .snapshot()
            .fold()
            .unwrap();
        let inbox = &view.scopes[&Some(parent)].deliveries;
        assert_eq!(inbox.len(), MAX_PENDING_DELIVERIES);
        assert!(!inbox.contains_key(&(child, turn)));
        let scope = &view.scopes[&Some(child)];
        assert!(!scope.admitted.contains_key(&turn));
        let ticket = manager
            .retained_turn_ticket(&manager.reference(child), turn)
            .unwrap();
        let TurnOutcome::Failed { failure, .. } = ticket.peek_result().unwrap().outcome else {
            panic!("expected restored child failure");
        };
        assert_eq!(failure.diagnostic, REACTIVATION_FAILURE);
        if let Some((actor, _start)) = parked {
            assert_eq!(
                actor.inner.state.lock().unwrap().deliveries.len(),
                MAX_PENDING_DELIVERIES
            );
        }
        crash(manager, &events, resident).await;
    });
}
