use super::{AgentLimits, AgentManagerHandle, AgentResidency, ResidentAgent};
use crate::{
    ActorBackend, AgentActorHandle, AgentId, AgentInput, AgentMode, BackendResult, ControlWork,
    DoneReason, History, ModeDef, ModeId, RunSettings, ToolOutput, TurnContext, TurnId,
    TurnOutcome, WorkKind,
    actor::{
        ActorHistory, ChildDelivery, EffectiveAgentConfig, TurnProvenance, TurnResult,
        graph_quiescence_barrier,
    },
    journal::{JournalChange, JournalCommit, SessionJournal},
};
use futures_lite::future::or;
use makima_providers::{Message, Model, ObservationSource, TokenUsage, provider::unconfigured};
use makima_storage::{
    session_state::{
        SessionStateView, StateChange, StoredAgentConfig, StoredAgentLifecycle, StoredAgentRecord,
        StoredDeliveryOp,
    },
    sessions::Session,
};
use std::{
    collections::BTreeMap,
    future::Future,
    path::Path,
    pin::Pin,
    slice,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use test_case::test_case;
type StoredSession = Session<Message, TokenUsage, ToolOutput>;
const MODEL: &str = "test/restart-model";
const INITIAL: &str = "retained history before report";
const REPORT: &str = "disk-backed child report";
const SUMMARY: &str = "compacted retained history";
const CHILD_KEY: &str = "restart-child";
const UNPUBLISHED: &str = "unpublished-after-checkpoint";
const PLUGIN_VERSION: u32 = 1;
const PRESENTATION_ID: u64 = 71;
const TIMEOUT: Duration = Duration::from_secs(5);
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
                assert_eq!(envelope["text"].as_str(), Some(REPORT));
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
            id,
            parent,
            depth: u32::from(parent.is_some()),
            transcript_key: parent.map_or_else(String::new, |_| CHILD_KEY.into()),
            lifecycle: if parent.is_some() {
                StoredAgentLifecycle::Closed
            } else {
                StoredAgentLifecycle::Open
            },
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
    let view =
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap();
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
) -> (AgentActorHandle, flume::Sender<()>) {
    let (start, wait) = flume::bounded(1);
    let backend = ReportBackend {
        executions: Arc::clone(executions),
        progress: progress.clone(),
    };
    let agent = manager
        .ensure_resident_with(parent, |_| async move {
            Ok(ResidentAgent {
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
                start: Some(wait),
                initialize: None,
            })
        })
        .await
        .unwrap();
    (agent.actor().unwrap(), start)
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
fn report_count(session: &StoredSession, delivery: &ChildDelivery) -> usize {
    let source = ObservationSource::ChildResult {
        agent_id: delivery.result.outcome.agent_id().to_string(),
        turn_id: delivery.result.outcome.turn_id().to_string(),
    };
    session
        .messages()
        .iter()
        .filter(|message| message.observation_source.as_ref() == Some(&source))
        .count()
}
async fn settled(actor: &AgentActorHandle) {
    let probes = graph_quiescence_barrier(slice::from_ref(actor))
        .await
        .unwrap();
    assert!(probes[0].is_quiescent());
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
        Some(activate(&manager, parent, &executions, &progress).await)
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
    assert_eq!(report_count(&saved, &delivery), 0);
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
    let (actor, start) = activate(&manager, parent, &executions, &progress).await;
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
    settled(&actor).await;
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
    assert_eq!(report_count(&saved, &delivery), expected);
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
            report_count(&session, &delivery),
            if compact { expected } else { 0 }
        );
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        let manager = restore(&journal);
        manager.deliver(parent, Arc::clone(&delivery)).unwrap();
        let (actor, start) = activate(&manager, parent, &executions, &progress).await;
        manager.deliver(parent, Arc::clone(&delivery)).unwrap();
        assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
        start.send(()).unwrap();
        actor
            .push_control(SUMMARY.into(), SUMMARY.into(), None)
            .unwrap();
        assert_eq!(events.recv_async().await.unwrap(), Progress::Controlled);
        settled(&actor).await;
        if compact {
            let epoch = journal.snapshot().histories[&parent].epoch;
            actor.push_compact(PRESENTATION_ID, None).unwrap();
            assert_eq!(events.recv_async().await.unwrap(), Progress::Compacted);
            settled(&actor).await;
            assert_ne!(journal.snapshot().histories[&parent].epoch, epoch);
            let saved = publish(&journal, &mut session, directory.path()).await;
            assert!(saved.history_snapshot().compacted);
            assert_eq!(saved.messages()[0].first_text_content(), Some(SUMMARY));
            assert_eq!(report_count(&saved, &delivery), 0);
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
