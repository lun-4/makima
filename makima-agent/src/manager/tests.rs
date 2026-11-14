#[cfg(unix)]
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use test_case::test_case;

#[cfg(unix)]
use crate::file_restore::{StoredSession, recording::ToolRecording};
use crate::journal::{JournalChange, JournalCommit, JournalError, SessionJournal};
#[cfg(unix)]
use crate::tools::{offload::OffloadStore, test_support::stub_ctx};
use event_listener::Event;
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    Invocation, Model, ProviderEvent, RequestDelivery, RequestOptions, StreamResponse, TokenUsage,
};
#[cfg(unix)]
use makima_storage::StateDir;
#[cfg(unix)]
use makima_storage::blobs::{BlobStore, FileState};
use makima_storage::checkpoint::CheckpointPublication;
use makima_storage::id::SessionRef;
use makima_storage::session_state::{
    StateChange, StoredAgentLifecycle, StoredDelivery, StoredDeliveryOp,
};
use makima_storage::sessions::Session;
#[cfg(unix)]
use makima_storage::sessions::{SESSIONS_DIR, offload_dir};
use serde_json::Value;

use super::{
    AgentLimits, AgentManagerHandle, AgentMetadata, AgentResidency, GraphLifecycle, ManagerError,
};
use crate::actor::{
    ActorHistory, ChildDelivery, DeliveryRejection, MAX_PENDING_DELIVERIES,
    MAX_PENDING_PREVIEW_BYTES, PreparedTurn, TurnProvenance, TurnResult, graph_quiescence_barrier,
};
use crate::journal::{
    GraphCheckpoint, GraphMutation, GraphRestartError, GraphStore, MemoryGraphStore,
    MemoryGraphStoreError,
};
use crate::{
    ActorBackend, ActorError, AgentEvent, AgentInput, AgentMode, BackendResult, CancelToken,
    ConfigChange, ConfigPatch, ControlWork, EventSender, History, PreparedModel,
    TurnAdmissionOptions, TurnContext, TurnOutcome, WorkKind,
};

const GRAPH_SUBSCRIBER: u64 = 1;
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
const CONDITION_TIMEOUT: &str = "condition never became true";
const SMALL_RETAINED_QUOTA: usize = 4;
#[cfg(unix)]
const TOOL_WRITE_BYTES: &[u8] = b"write completed after shutdown cut";
#[cfg(unix)]
const TOOL_WRITE_MODEL: &str = "shutdown-tool-recording";

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestAgentRecord {
    id: crate::AgentId,
    parent: Option<crate::AgentId>,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestStateRecord {
    agent: crate::AgentId,
    turn: crate::TurnId,
}

type TestGraphStore = MemoryGraphStore<TestAgentRecord, TestStateRecord>;

fn restart_manager(store: &TestGraphStore) -> AgentManagerHandle {
    store.restart().unwrap();
    AgentManagerHandle::restore_empty_graph(AgentLimits::default(), store).unwrap()
}

#[cfg(unix)]
#[test_case(false; "shutdown_waits_for_postcapture")]
#[test_case(true; "timeout_reaper_preserves_postcapture")]
fn shutdown_drains_actual_write_before_incarnation_cleanup(timeout: bool) {
    smol::block_on(async {
        let temporary = tempfile::tempdir().unwrap();
        let state = StateDir::from_path(temporary.path().canonicalize().unwrap());
        let mut session = StoredSession::new(TOOL_WRITE_MODEL, temporary.path().to_str().unwrap());
        session.save(&state).unwrap();
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        manager.journal().bind_session(session.id).unwrap();
        let (entered, current) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                Box::new(ReportingCancellableBackend { current: entered }),
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let ticket = actor.admit_turn(input(), None, String::new()).unwrap();
        let turn = current.recv_async().await.unwrap();
        let incarnation = root.incarnation().unwrap();
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.managed_turn = Some(turn);
        ctx.offload = Some(Arc::new(OffloadStore::on_disk(offload_dir(
            &state.path().join(SESSIONS_DIR),
            session.id,
        ))));
        let recording = ToolRecording::from_context(&ctx).unwrap().unwrap();
        let path = temporary.path().canonicalize().unwrap().join("written");
        let capture = recording.capture(&path).unwrap();
        let write_path = path.clone();
        let (write_entered, writing) = flume::bounded(1);
        let (release, resume) = flume::bounded(1);
        let (finished, recorded) = flume::bounded(1);
        manager
            .retain_tool_completion(root.id(), async move {
                let result = smol::unblock(move || {
                    write_entered.send(()).unwrap();
                    resume.recv().unwrap();
                    fs::write(write_path, TOOL_WRITE_BYTES).unwrap();
                    recording.finish(capture)
                })
                .await;
                finished.send(result).unwrap();
            })
            .unwrap();
        writing.recv_async().await.unwrap();
        let journal = manager.journal();
        let publication = journal.reserve_publication().await;
        let mut shutdown = Box::pin(manager.shutdown(if timeout {
            Duration::ZERO
        } else {
            COMPLETION_TIMEOUT
        }));
        if timeout {
            let report = shutdown.as_mut().await;
            assert!(report.timed_out.contains(&root.id()));
        } else {
            assert!(
                futures_lite::future::poll_once(shutdown.as_mut())
                    .await
                    .is_none()
            );
            manager.detach_incarnation(&incarnation).unwrap();
        }
        assert_eq!(
            journal.incarnation(root.id()).reservation,
            incarnation.reservation
        );
        assert!(actor.admit_turn(input(), None, String::new()).is_err());
        assert!(manager.retain_tool_completion(root.id(), async {}).is_err());
        release.send(()).unwrap();
        recorded.recv_async().await.unwrap().unwrap();
        assert_eq!(fs::read(&path).unwrap(), TOOL_WRITE_BYTES);
        let snapshot = journal.snapshot();
        let after = snapshot
            .state
            .iter()
            .find_map(|record| match &record.change {
                StateChange::File {
                    path: recorded_path,
                    before: FileState::Absent,
                    after,
                } if recorded_path == &path => Some(after),
                _ => None,
            })
            .unwrap();
        let FileState::Blob(reference) = after else {
            panic!("written file requires post-image");
        };
        assert_eq!(
            BlobStore::new(state).read(reference).unwrap(),
            TOOL_WRITE_BYTES
        );
        drop(publication);
        if !timeout {
            assert!(shutdown.await.timed_out.is_empty());
        }
        ticket.wait_result().await;
        manager.wait_until_reaped().await;
        assert!(manager.tool_completions_finished(None));
        assert_ne!(
            journal.incarnation(root.id()).reservation,
            incarnation.reservation
        );
    });
}

#[test]
fn weak_manager_observer_does_not_retain_manager_or_journal() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let journal = manager.journal();
    let journal_weak = Arc::downgrade(&journal);
    let observer = manager.downgrade();
    assert!(observer.upgrade().unwrap().same_manager(&manager));
    let receiver = journal.subscribe_state_changes();
    drop(journal);
    drop(manager);
    assert!(observer.upgrade().is_none());
    assert!(journal_weak.upgrade().is_none());
    assert!(receiver.is_disconnected());
}

#[test]
fn restart_manager_round_trips_an_empty_graph() {
    let store = TestGraphStore::default();
    let manager = restart_manager(&store);
    let generation = manager.generation();
    assert!(manager.snapshot().is_empty());
    drop(manager);

    let manager = restart_manager(&store);
    assert!(manager.snapshot().is_empty());
    assert!(matches!(manager.root(), Err(ManagerError::MissingRoot)));
    assert_ne!(manager.generation(), generation);
    assert_eq!(manager.limits(), AgentLimits::default());

    smol::block_on(async {
        let (entered, current) = flume::unbounded();
        let gate = Gate::new();
        let root = manager
            .create_root(
                Vec::new(),
                None,
                TestBackend::reporting(entered, Some(gate.clone())),
            )
            .unwrap();
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, String::new())
            .unwrap();
        let context = current.recv_async().await.unwrap();
        assert_eq!(context.agent_id(), root.id());
        gate.release(1);
        let result = ticket.wait_result().await;
        assert!(matches!(result.outcome, TurnOutcome::Completed { .. }));
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false; "successful_publication")]
#[test_case(true; "retry_failed_publication")]
fn graph_store_restarts_at_the_published_immutable_prefix(fail_first: bool) {
    let store = TestGraphStore::default();
    let root = TestAgentRecord {
        id: crate::AgentId::generate(),
        parent: None,
        label: "root".into(),
    };
    let admitted = TestStateRecord {
        agent: root.id,
        turn: crate::TurnId::generate(),
    };
    let first_revision = store.put_agent(root.clone()).unwrap();
    let published_revision = store.append_state(admitted.clone()).unwrap();
    assert!(published_revision > first_revision);
    let checkpoint = store.snapshot().unwrap();
    let child = TestAgentRecord {
        id: crate::AgentId::generate(),
        parent: Some(root.id),
        label: "unpublished child".into(),
    };
    store.put_agent(child).unwrap();
    assert_eq!(checkpoint.revision(), published_revision);
    assert!(store.load().unwrap().records().is_empty());
    if fail_first {
        store.fail_next_publication().unwrap();
        assert_eq!(
            store.publish(&checkpoint),
            Err(MemoryGraphStoreError::PublicationFailed)
        );
        assert!(store.load().unwrap().records().is_empty());
    }
    store.publish(&checkpoint).unwrap();
    assert_eq!(store.load().unwrap().records(), checkpoint.records());
    assert!(store.snapshot().unwrap().revision() > checkpoint.revision());

    store.restart().unwrap();
    assert_eq!(
        store.load().unwrap().records(),
        &[GraphMutation::Agent(root), GraphMutation::State(admitted)]
    );
    assert_eq!(store.snapshot().unwrap().revision(), published_revision);
    assert_eq!(
        store.publish(&checkpoint),
        Err(MemoryGraphStoreError::StaleCheckpoint)
    );
    assert!(matches!(
        AgentManagerHandle::restore_empty_graph(AgentLimits::default(), &store),
        Err(GraphRestartError::NonemptyGraph)
    ));
}

#[test]
fn graph_store_publication_is_idempotent_and_cannot_regress() {
    let store = TestGraphStore::default();
    let agent = TestAgentRecord {
        id: crate::AgentId::generate(),
        parent: None,
        label: "before".into(),
    };
    store.put_agent(agent.clone()).unwrap();
    let older = store.snapshot().unwrap();
    let mut updated = agent;
    updated.label = "after".into();
    store.put_agent(updated.clone()).unwrap();
    let latest = store.snapshot().unwrap();
    store.publish(&latest).unwrap();
    store.publish(&older).unwrap();
    store.publish(&latest).unwrap();
    store.restart().unwrap();
    assert_eq!(store.load().unwrap().records(), latest.records());
    assert_eq!(
        store.load().unwrap().records().last(),
        Some(&GraphMutation::Agent(updated))
    );
}

#[test]
fn graph_store_rejects_a_forged_prefix_without_mutation() {
    let store = TestGraphStore::default();
    let checkpoint = store.snapshot().unwrap();
    let forged = GraphCheckpoint::new(
        checkpoint.epoch(),
        vec![GraphMutation::State(TestStateRecord {
            agent: crate::AgentId::generate(),
            turn: crate::TurnId::generate(),
        })],
    );
    assert_eq!(
        store.publish(&forged),
        Err(MemoryGraphStoreError::InvalidCheckpoint)
    );
    assert!(store.load().unwrap().records().is_empty());
}

#[test]
fn restart_manager_discards_unpublished_graph_mutations() {
    let store = TestGraphStore::default();
    let manager = restart_manager(&store);
    store
        .put_agent(TestAgentRecord {
            id: crate::AgentId::generate(),
            parent: None,
            label: "lost on crash".into(),
        })
        .unwrap();
    let unpublished = store.snapshot().unwrap();
    store.fail_next_publication().unwrap();
    assert_eq!(
        store.publish(&unpublished),
        Err(MemoryGraphStoreError::PublicationFailed)
    );
    drop(manager);
    let manager = restart_manager(&store);
    assert!(manager.snapshot().is_empty());
    assert!(store.snapshot().unwrap().records().is_empty());
    assert_eq!(
        store.publish(&unpublished),
        Err(MemoryGraphStoreError::StaleCheckpoint)
    );
}

#[test]
fn held_ticket_observation_checks_manager_and_survives_graph_removal() {
    smol::block_on(async {
        const CORRELATION: &str = "retained";
        const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let other = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();
        let result = ticket.wait_result().await;
        assert!(matches!(
            other.retained_turn_ticket(&root, ticket.turn_id()),
            Err(ManagerError::WrongManager)
        ));
        assert!(matches!(
            root.turn_ticket(crate::TurnId::generate()),
            Err(ManagerError::TurnResultUnavailable { .. })
        ));
        manager.close_subtree(root.id()).unwrap();
        assert!(root.validate_live().is_err());
        assert_eq!(ticket.wait_result().await.outcome, result.outcome);
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        assert_eq!(ticket.wait_result().await.text, result.text);
        let retained = manager.lookup(root.id()).unwrap();
        assert!(retained.actor().is_err());
        assert_eq!(
            retained
                .turn_ticket(ticket.turn_id())
                .unwrap()
                .wait_result()
                .await
                .text,
            result.text
        );
        assert!(other.shutdown(SHUTDOWN_TIMEOUT).await.timed_out.is_empty());
    });
}

#[test_case(false; "live_parent")]
#[test_case(true; "closed_parent")]
fn trusted_spawn_checks_runtime_identity_and_parent_liveness(close_parent: bool) {
    smol::block_on(async {
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let other = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        assert!(root.manager().same_manager(&manager));
        assert!(root.same_manager(&manager.root().unwrap()));
        assert_eq!(manager.list().len(), 1);
        assert_eq!(manager.lookup(root.id()).unwrap().id(), root.id());
        assert!(matches!(
            other.validate_live_ref(&root),
            Err(ManagerError::WrongManager)
        ));
        assert!(matches!(
            other.spawn_child_trusted_with_config(
                &root,
                config(MODEL, false),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::boxed())
            ),
            Err(ManagerError::WrongManager)
        ));
        if close_parent {
            root.actor().unwrap().close();
            yield_until(|| manager.runner_finished(root.id()).unwrap()).await;
        }
        let result = manager.spawn_child_trusted_with_config(
            &root,
            config(MODEL, false),
            AgentMetadata::default(),
            Vec::new(),
            None,
            |_| Ok::<_, String>(TestBackend::boxed()),
        );
        if close_parent {
            assert!(matches!(result, Err(ManagerError::NonLiveAgent(id)) if id == root.id()));
            let retained = manager.lookup(root.id()).unwrap();
            assert_eq!(retained.id(), root.id());
            assert_eq!(
                retained.snapshot().unwrap().graph_lifecycle,
                GraphLifecycle::Closed
            );
            assert!(retained.actor().is_err());
        } else {
            let child = result.unwrap();
            assert_eq!(child.snapshot().unwrap().parent_id, Some(root.id()));
            assert_eq!(manager.list().len(), 2);
            child.validate_live().unwrap();
        }
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        assert!(other.shutdown(SHUTDOWN_TIMEOUT).await.timed_out.is_empty());
    });
}

/// Yields until `cond` holds. Bounded only so a broken test cannot hang.
async fn yield_until(cond: impl Fn() -> bool) {
    futures_lite::future::or(
        async {
            while !cond() {
                smol::future::yield_now().await;
            }
        },
        async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            panic!("{CONDITION_TIMEOUT}");
        },
    )
    .await;
}

/// The deferred runner waits on its start signal and the actor's policy
/// event together, so a policy listener shows it is parked before start.
#[test]
fn deferred_runtime_does_not_execute_before_activation() {
    smol::block_on(async {
        const CORRELATION: &str = "deferred";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (start_tx, start_rx) = flume::bounded(1);
        let (entered_tx, entered_rx) = flume::unbounded();
        let root = manager
            .create_root_deferred_with_config(
                None,
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(entered_tx, None)),
                start_rx,
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let ticket = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        yield_until(|| actor.inner.policy_changed.total_listeners() > 0).await;
        assert_eq!(actor.snapshot().queued, 1);
        assert!(entered_rx.is_empty());
        assert!(manager.lock_graph().active_turns.is_empty());
        start_tx.send(()).unwrap();
        assert_eq!(
            entered_rx.recv_async().await.unwrap().turn_id(),
            ticket.turn_id()
        );
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn deferred_runtime_drop_settles_admissions() {
    smol::block_on(async {
        const CORRELATION: &str = "deferred-drop";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (start_tx, start_rx) = flume::bounded(1);
        let (entered_tx, entered_rx) = flume::unbounded();
        let root = manager
            .create_root_deferred_with_config(
                None,
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(entered_tx, None)),
                start_rx,
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let ticket = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        drop(start_tx);
        let outcome = futures_lite::future::or(async { Some(ticket.wait().await) }, async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            None
        })
        .await
        .unwrap();
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: crate::TurnCancellationReason::Closed,
                ..
            }
        ));
        assert_eq!(actor.snapshot().lifecycle, crate::ActorLifecycle::Closed);
        assert_eq!(actor.outcome(ticket.turn_id()), Some(outcome));
        assert!(matches!(
            actor.admit_turn(input(), None, CORRELATION.into()),
            Err(ActorError::Closed)
        ));
        assert!(entered_rx.is_empty());
        assert!(manager.lock_graph().active_turns.is_empty());
        let report = manager.shutdown(COMPLETION_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test_case(false; "close")]
#[test_case(true; "shutdown")]
fn deferred_runtime_terminal_lifecycle_exits_with_start_sender_retained(shutdown: bool) {
    smol::block_on(async {
        const CORRELATION: &str = "deferred-terminal";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (start_tx, start_rx) = flume::bounded(1);
        let (entered_tx, entered_rx) = flume::unbounded();
        let root = manager
            .create_root_deferred_with_config(
                None,
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(entered_tx, None)),
                start_rx,
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let ticket = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        if !shutdown {
            manager.close_subtree(root.id()).unwrap();
        }
        let report = manager.shutdown(COMPLETION_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
        assert!(manager.runner_finished(root.id()).unwrap());
        let reason = if shutdown {
            crate::TurnCancellationReason::Shutdown
        } else {
            crate::TurnCancellationReason::Closed
        };
        assert!(
            matches!(ticket.wait().await, TurnOutcome::Cancelled { reason: actual, .. } if actual == reason)
        );
        assert!(entered_rx.is_empty());
        assert!(manager.lock_graph().active_turns.is_empty());
        drop(start_tx);
    });
}

#[test]
fn idle_guard_rejects_popped_work() {
    smol::block_on(async {
        const CORRELATION: &str = "popped";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let actor = root.actor().unwrap();
        let (popped, release) = actor.pause_after_next_pop();
        let ticket = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        popped.recv_async().await.unwrap();
        assert!(matches!(
            manager.prepare_idle_subtree(root.id()),
            Err(ManagerError::BusySubtree(_))
        ));
        release.send(()).unwrap();
        ticket.wait().await;
        manager.shutdown(std::time::Duration::from_secs(1)).await;
    });
}

enum PoppedWork {
    Admitted,
    Prepared,
    Submitted,
}

enum MatchingSuccessor {
    None,
    Admitted,
    Prepared,
}

#[test_case(PoppedWork::Admitted, crate::TurnCancellationReason::User, MatchingSuccessor::None; "admitted_user")]
#[test_case(PoppedWork::Submitted, crate::TurnCancellationReason::Shutdown, MatchingSuccessor::None; "shared_submission_shutdown")]
#[test_case(PoppedWork::Prepared, crate::TurnCancellationReason::Shutdown, MatchingSuccessor::Admitted; "prepared_matching_admitted")]
#[test_case(PoppedWork::Admitted, crate::TurnCancellationReason::Shutdown, MatchingSuccessor::Prepared; "admitted_matching_prepared")]
fn correlation_cancel_settles_popped_work_without_backend_entry(
    work: PoppedWork,
    reason: crate::TurnCancellationReason,
    successor: MatchingSuccessor,
) {
    smol::block_on(async {
        const CORRELATION: &str = "cancelled-after-pop";
        const UNRELATED: &str = "unrelated";
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (entered_tx, entered_rx) = flume::unbounded();
        let root = manager
            .create_root_with_config(Some(config(MODEL, false)), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(entered_tx, None))
            })
            .unwrap();
        let actor = root.actor().unwrap();
        let identity = actor.identity();
        let events = actor.subscribe_events();
        let (event_tx, event_rx) = flume::unbounded();
        let (popped, release) = actor.pause_after_next_pop();
        let ticket = match work {
            PoppedWork::Admitted => Some(
                actor
                    .admit_turn(
                        input(),
                        Some(EventSender::new(event_tx)),
                        CORRELATION.into(),
                    )
                    .unwrap(),
            ),
            PoppedWork::Prepared => {
                let operation = actor
                    .reserve_prepared_operation(Some(PreparedTurn {
                        input: input(),
                        event_sender: Some(EventSender::new(event_tx)),
                        correlation: CORRELATION.into(),
                    }))
                    .unwrap();
                operation.resolve(None).unwrap();
                operation.wait_ready().await.unwrap();
                operation.commit().unwrap().ticket
            }
            PoppedWork::Submitted => Some(
                actor
                    .submit(
                        input(),
                        Some(EventSender::new(event_tx)),
                        CORRELATION.into(),
                        TurnAdmissionOptions::default(),
                    )
                    .unwrap(),
            ),
        };
        popped.recv_async().await.unwrap();
        let (successor_ticket, successor_operation) = match successor {
            MatchingSuccessor::None => (None, None),
            MatchingSuccessor::Admitted => (
                Some(actor.admit_turn(input(), None, CORRELATION.into()).unwrap()),
                None,
            ),
            MatchingSuccessor::Prepared => {
                let operation = actor
                    .reserve_prepared_operation(Some(PreparedTurn {
                        input: input(),
                        event_sender: None,
                        correlation: CORRELATION.into(),
                    }))
                    .unwrap();
                operation.resolve(None).unwrap();
                operation.wait_ready().await.unwrap();
                (None, Some(operation))
            }
        };
        let unrelated = actor.admit_turn(input(), None, UNRELATED.into()).unwrap();
        manager
            .cancel_correlation(root.id(), CORRELATION, reason)
            .unwrap();
        if let Some(successor) = successor_ticket {
            assert!(
                matches!(successor.wait().await, TurnOutcome::Cancelled { reason: actual, .. } if actual == reason)
            );
        }
        if let Some(successor) = successor_operation {
            assert!(matches!(
                successor.commit(),
                Err(ActorError::PolicyCancelled)
            ));
        }
        release.send(()).unwrap();
        let completed = futures_lite::future::or(async { Some(unrelated.wait().await) }, async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            None
        })
        .await
        .unwrap();
        assert!(matches!(completed, TurnOutcome::Completed { .. }));
        assert_eq!(
            entered_rx.recv_async().await.unwrap().turn_id(),
            unrelated.turn_id()
        );
        assert!(entered_rx.is_empty());
        assert!(
            !actor
                .inner
                .state
                .lock()
                .unwrap()
                .cancelled_correlations
                .contains_key(CORRELATION)
        );
        while !matches!(
            events.recv_async().await.unwrap(),
            crate::actor::ActorEvent::Idle { .. }
        ) {}
        assert!(actor.inner.state.lock().unwrap().processing.is_none());
        if let Some(ticket) = ticket {
            let outcome = ticket.wait().await;
            assert!(
                matches!(outcome, TurnOutcome::Cancelled { agent_id, turn_id, reason: actual, .. }
                if agent_id == root.id() && turn_id == ticket.turn_id() && actual == reason)
            );
            assert_eq!(actor.outcome(ticket.turn_id()), Some(outcome.clone()));
            assert!(
                matches!(event_rx.recv_async().await.unwrap().event, AgentEvent::TurnOutcome(delivered) if delivered == outcome)
            );
        }
        assert!(event_rx.is_empty());
        assert!(Arc::ptr_eq(&identity, &actor.identity()));
        let reused = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        assert!(matches!(reused.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(
            entered_rx.recv_async().await.unwrap().turn_id(),
            reused.turn_id()
        );
        assert!(entered_rx.is_empty());
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(crate::TurnCancellationReason::User; "user")]
#[test_case(crate::TurnCancellationReason::Shutdown; "shutdown")]
fn correlation_cancel_drops_popped_batch_when_earlier_root_matches(
    reason: crate::TurnCancellationReason,
) {
    smol::block_on(async {
        const EARLIER: &str = "earlier-root";
        const LAST: &str = "last-root";
        const UNRELATED: &str = "unrelated";
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (start_tx, start_rx) = flume::bounded(1);
        let (entered_tx, entered_rx) = flume::unbounded();
        let root = manager
            .create_root_deferred_with_config(
                Some(config(MODEL, false)),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(entered_tx, None)),
                start_rx,
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let identity = actor.identity();
        let events = actor.subscribe_events();
        let (popped, release) = actor.pause_after_next_pop();
        for correlation in [EARLIER, LAST] {
            actor
                .submit(
                    input(),
                    None,
                    correlation.into(),
                    TurnAdmissionOptions::default(),
                )
                .unwrap();
        }
        start_tx.send(()).unwrap();
        popped.recv_async().await.unwrap();
        assert_eq!(actor.snapshot().queued, 0);
        let unrelated = actor.admit_turn(input(), None, UNRELATED.into()).unwrap();
        manager
            .cancel_correlation(root.id(), EARLIER, reason)
            .unwrap();
        release.send(()).unwrap();
        let outcome = futures_lite::future::or(async { Some(unrelated.wait().await) }, async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            None
        })
        .await
        .unwrap();
        assert!(matches!(outcome, TurnOutcome::Completed { .. }));
        assert_eq!(
            entered_rx.recv_async().await.unwrap().turn_id(),
            unrelated.turn_id()
        );
        assert!(entered_rx.is_empty());
        assert!(
            actor
                .inner
                .state
                .lock()
                .unwrap()
                .cancelled_correlations
                .is_empty()
        );
        while !matches!(
            events.recv_async().await.unwrap(),
            crate::actor::ActorEvent::Idle { .. }
        ) {}
        assert!(actor.inner.state.lock().unwrap().processing.is_none());
        for correlation in [EARLIER, LAST] {
            let reused = actor.admit_turn(input(), None, correlation.into()).unwrap();
            assert!(matches!(reused.wait().await, TurnOutcome::Completed { .. }));
            assert_eq!(
                entered_rx.recv_async().await.unwrap().turn_id(),
                reused.turn_id()
            );
        }
        assert!(entered_rx.is_empty());
        assert!(Arc::ptr_eq(&identity, &actor.identity()));
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn queued_only_correlation_cancel_does_not_precancel_later_work() {
    smol::block_on(async {
        const CORRELATION: &str = "queued-only";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (start_tx, start_rx) = flume::bounded(1);
        let (entered_tx, entered_rx) = flume::unbounded();
        let root = manager
            .create_root_deferred_with_config(
                None,
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(entered_tx, None)),
                start_rx,
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let cancelled = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        manager
            .cancel_correlation(root.id(), CORRELATION, crate::TurnCancellationReason::User)
            .unwrap();
        assert!(matches!(
            cancelled.wait().await,
            TurnOutcome::Cancelled {
                reason: crate::TurnCancellationReason::User,
                ..
            }
        ));
        assert!(
            actor
                .inner
                .state
                .lock()
                .unwrap()
                .cancelled_correlations
                .is_empty()
        );
        let reused = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        start_tx.send(()).unwrap();
        assert!(matches!(reused.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(
            entered_rx.recv_async().await.unwrap().turn_id(),
            reused.turn_id()
        );
        assert!(entered_rx.is_empty());
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(true; "cancelled_after_permission")]
#[test_case(false; "cancelled_before_permission")]
fn idle_guard_permission_for_retired_turn_does_not_hold_actor(permitted_first: bool) {
    smol::block_on(async {
        const LATER: &str = "later";
        const APPROVED: &str = "approved";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (start_tx, start_rx) = flume::bounded(1);
        let (entered_tx, entered_rx) = flume::unbounded();
        let root = manager
            .create_root_deferred_with_config(
                None,
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(entered_tx, None)),
                start_rx,
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let guard = manager.prepare_idle_subtree(root.id()).unwrap();
        let approved = actor.admit_turn(input(), None, APPROVED.into()).unwrap();
        let later = actor.admit_turn(input(), None, LATER.into()).unwrap();
        if permitted_first {
            guard.allow_turn(&approved).unwrap();
            actor.cancel_turn(approved.turn_id()).unwrap();
        } else {
            actor.cancel_turn(approved.turn_id()).unwrap();
            assert!(matches!(
                guard.allow_turn(&approved),
                Err(ActorError::PolicyCancelled)
            ));
            drop(guard);
        }
        start_tx.send(()).unwrap();
        assert_eq!(
            entered_rx.recv_async().await.unwrap().turn_id(),
            later.turn_id()
        );
        later.wait().await;
        assert!(matches!(
            approved.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        manager.shutdown(Duration::from_secs(1)).await;
    });
}

#[test]
fn graph_events_tag_every_actor_and_end_with_the_graph() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let events = manager.subscribe_events(GRAPH_SUBSCRIBER).unwrap();
        assert!(manager.subscribe_events(GRAPH_SUBSCRIBER).is_none());
        let child = manager
            .spawn_child_trusted_with_config(
                &root,
                config("anthropic/claude-sonnet-4-20250514", false),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::boxed()),
            )
            .unwrap();
        for agent in [&child, &root] {
            agent.actor().unwrap().close();
            loop {
                let (source, event) = events.recv().await.unwrap();
                if matches!(event, crate::ActorEvent::Close { .. }) {
                    assert_eq!(source.id(), agent.id());
                    break;
                }
            }
        }
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        drop((manager, root, child));
        assert!(events.recv().await.is_none());
    });
}

#[test]
fn idle_guard_reports_closed_actor_as_non_live() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (start_tx, start_rx) = flume::bounded(1);
        let root = manager
            .create_root_deferred_with_config(
                None,
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::boxed()),
                start_rx,
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let guard = manager.prepare_idle_subtree(root.id()).unwrap();
        let approved = actor.admit_turn(input(), None, "approved".into()).unwrap();
        actor.close();
        assert!(matches!(
            guard.allow_turn(&approved),
            Err(ActorError::Closed)
        ));
        drop(guard);
        drop(start_tx);
        while !manager.runner_finished(root.id()).unwrap() {
            smol::future::yield_now().await;
        }
        assert!(matches!(
            manager.prepare_idle_subtree(root.id()),
            Err(ManagerError::NonLiveAgent(id)) if id == root.id()
        ));
        manager.shutdown(Duration::from_secs(1)).await;
    });
}

#[test]
fn root_cancelled_before_managed_admission_has_no_start_and_resolves_end() {
    const CORRELATION: &str = "root-before-permit";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let (entered_tx, entered_rx) = flume::bounded(1);
    let root = manager
        .create_root(Vec::new(), None, TestBackend::reporting(entered_tx, None))
        .unwrap();
    let actor = root.actor().unwrap();
    let events = actor.subscribe_events();
    let (acquire_tx, acquire_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    manager.set_managed_acquire_gate(acquire_tx, release_rx);
    actor
        .submit(
            input(),
            None,
            CORRELATION.into(),
            TurnAdmissionOptions::default(),
        )
        .unwrap();
    acquire_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let turn_id = actor.snapshot().active_turn.unwrap();
    let ticket = actor.turn_ticket(turn_id).unwrap();
    assert!(events.is_empty());
    actor.close();
    assert!(ticket.peek_result().is_none());
    release_tx.send(()).unwrap();
    assert!(matches!(
        smol::block_on(ticket.wait()),
        TurnOutcome::Cancelled { .. }
    ));
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
    let retained = events.drain().collect::<Vec<_>>();
    assert!(
        !retained
            .iter()
            .any(|event| matches!(event, crate::ActorEvent::Start { .. }))
    );
    assert_eq!(retained.iter().filter(|event| matches!(event, crate::ActorEvent::End(result) if result.outcome.turn_id() == turn_id)).count(), 1);
    assert!(entered_rx.is_empty());
}

#[test]
fn idle_acquisition_rejects_turn_awaiting_managed_registration() {
    const CORRELATION: &str = "before-registration";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let (entered_tx, entered_rx) = flume::unbounded();
    let root = manager
        .create_root(Vec::new(), None, TestBackend::reporting(entered_tx, None))
        .unwrap();
    let actor = root.actor().unwrap();
    let (acquire_tx, acquire_rx) = flume::bounded(1);
    let (acquire_release_tx, acquire_release_rx) = flume::bounded(1);
    manager.set_managed_acquire_gate(acquire_tx, acquire_release_rx);
    let events = actor.subscribe_events();
    let ticket = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
    acquire_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    assert!(manager.lock_graph().active_turns.is_empty());
    assert_eq!(actor.snapshot().active_turn, Some(ticket.turn_id()));
    assert!(matches!(
        manager.prepare_idle_subtree(root.id()),
        Err(ManagerError::BusySubtree(id)) if id == root.id()
    ));
    acquire_release_tx.send(()).unwrap();
    assert_eq!(
        entered_rx
            .recv_timeout(COMPLETION_TIMEOUT)
            .unwrap()
            .turn_id(),
        ticket.turn_id()
    );
    assert!(matches!(
        smol::block_on(ticket.wait()),
        TurnOutcome::Completed { .. }
    ));
    while !matches!(
        events.recv_timeout(COMPLETION_TIMEOUT).unwrap(),
        crate::actor::ActorEvent::Idle { .. }
    ) {}
    drop(manager.prepare_idle_subtree(root.id()).unwrap());
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
}

#[test_case(crate::TurnCancellationReason::User, false; "correlation_user")]
#[test_case(crate::TurnCancellationReason::Shutdown, false; "correlation_shutdown")]
#[test_case(crate::TurnCancellationReason::User, true; "nominal_user")]
#[test_case(crate::TurnCancellationReason::Shutdown, true; "nominal_shutdown")]
fn cancel_cut_prevents_late_managed_registration_before_trigger(
    reason: crate::TurnCancellationReason,
    nominal: bool,
) {
    const CORRELATION: &str = "cancel-before-registration";
    let manager = AgentManagerHandle::new(AgentLimits {
        max_concurrent_agent_turns: 1,
        ..AgentLimits::default()
    })
    .unwrap();
    let (entered_tx, entered_rx) = flume::unbounded();
    let root = manager
        .create_root(Vec::new(), None, TestBackend::reporting(entered_tx, None))
        .unwrap();
    let actor = root.actor().unwrap();
    let (acquire_tx, acquire_rx) = flume::bounded(1);
    let (acquire_release_tx, acquire_release_rx) = flume::bounded(1);
    manager.set_managed_acquire_gate(acquire_tx, acquire_release_rx);
    let ticket = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
    acquire_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let (cut_tx, cut_rx) = flume::bounded(1);
    let (cut_release_tx, cut_release_rx) = flume::bounded(1);
    manager.set_descendant_cut_gate(cut_tx, cut_release_rx);
    let cancelling = manager.clone();
    let root_id = root.id();
    let turn_id = ticket.turn_id();
    let cancel = std::thread::spawn(move || {
        if nominal {
            cancelling.cancel_turn(root_id, turn_id, reason)
        } else {
            cancelling.cancel_correlation(root_id, CORRELATION, reason)
        }
    });
    cut_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    assert!(actor.inner.state.try_lock().is_ok());
    assert!(manager.0.graph.try_lock().is_ok());
    acquire_release_tx.send(()).unwrap();
    let outcome = smol::block_on(futures_lite::future::or(
        async { Some(ticket.wait().await) },
        async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            None
        },
    ));
    smol::block_on(actor.runtime_barrier()).unwrap();
    assert!(manager.0.limiter.try_acquire().is_some());
    drop(actor.inner.state.lock().unwrap());
    drop(manager.lock_graph());
    cut_release_tx.send(()).unwrap();
    cancel.join().unwrap().unwrap();
    assert!(matches!(
        outcome.unwrap(),
        TurnOutcome::Cancelled { reason: actual, .. } if actual == reason
    ));
    assert!(entered_rx.is_empty());
    assert!(manager.lock_graph().active_turns.is_empty());
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
}

#[test_case(false; "correlation")]
#[test_case(true; "nominal_turn")]
fn turn_cancel_preserves_trusted_child(nominal: bool) {
    const CORRELATION: &str = "root";
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let delegated = current
        .spawn_child(
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    let trusted = manager
        .spawn_child_trusted_with_config(
            &root,
            config("anthropic/claude-sonnet-4-20250514", false),
            AgentMetadata::default(),
            Vec::new(),
            None,
            |_| Ok::<_, String>(TestBackend::boxed()),
        )
        .unwrap();
    if nominal {
        manager
            .cancel_turn(
                root.id(),
                current.turn_id(),
                crate::TurnCancellationReason::User,
            )
            .unwrap();
    } else {
        manager
            .cancel_correlation(root.id(), CORRELATION, crate::TurnCancellationReason::User)
            .unwrap();
    }
    assert!(current.validate_active().is_err());
    assert!(trusted.validate_live().is_ok());
    assert_eq!(
        trusted.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Live
    );
    assert!(delegated.validate_live().is_err());
    gate.release(1);
    let report = smol::block_on(manager.shutdown(COMPLETION_TIMEOUT));
    assert!(report.timed_out.is_empty());
    assert_eq!(
        trusted.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Dormant
    );
}

#[test]
fn runner_finished_observes_reclaimed_resources_after_gated_close() {
    let (manager, _, current, root_gate) = active_root(AgentLimits::default());
    let child_gate = Gate::new();
    let (entered, running) = flume::bounded(1);
    let child = current
        .spawn_child(
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::reporting(entered, Some(Arc::clone(&child_gate))),
        )
        .unwrap();
    let actor = child.actor().unwrap();
    let ticket = actor.admit_turn(input(), None, String::new()).unwrap();
    smol::block_on(async {
        running.recv_async().await.unwrap();
        child.close_subtree().unwrap();
        assert!(!manager.runner_finished(child.id()).unwrap());
        {
            let graph = manager.lock_graph();
            let node = &graph.nodes[&child.id()];
            assert!(node.task.is_some());
            assert_eq!(node.residency, AgentResidency::Resident);
        }
        child_gate.release(1);
        ticket.wait().await;
        yield_until(|| manager.runner_finished(child.id()).unwrap()).await;
        {
            let graph = manager.lock_graph();
            let node = &graph.nodes[&child.id()];
            assert!(node.task.is_none());
            assert_eq!(node.residency, AgentResidency::Dormant);
            assert!(node.actor.as_ref().unwrap().same_actor(&actor));
        }
        root_gate.release(1);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn spawn_close_cycles_reclaim_tasks_and_retain_terminal_nodes() {
    const CYCLES: usize = 32;
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let mut children = Vec::new();
    for _ in 0..CYCLES {
        let child = current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let held = child.clone();
        let terminal = Arc::downgrade(&child.terminal);
        child.close_subtree().unwrap();
        smol::block_on(yield_until(|| manager.runner_finished(child.id()).unwrap()));
        children.push(child.id());
        assert_eq!(manager.lock_graph().nodes.len(), children.len() + 1);
        assert_eq!(root.snapshot().unwrap().children, children);
        let retained = manager.lookup(child.id()).unwrap();
        assert_eq!(retained.id(), child.id());
        assert_eq!(
            manager.node(child.id()).unwrap().residency,
            AgentResidency::Dormant
        );
        {
            let graph = manager.lock_graph();
            let node = &graph.nodes[&child.id()];
            assert!(node.task.is_none());
            assert!(node.actor.is_some());
        }
        assert!(child.actor().is_err());
        assert!(child.validate_live().is_err());
        assert!(child.cancel().is_err());
        assert_eq!(
            child.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
        assert_eq!(
            held.snapshot().unwrap().actor.unwrap().lifecycle,
            crate::ActorLifecycle::Closed
        );
        drop(retained);
        drop(child);
        assert!(terminal.upgrade().is_some());
        drop(held);
        assert!(terminal.upgrade().is_some());
    }
    gate.release(1);
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
    assert_eq!(manager.snapshot().len(), CYCLES + 1);
    assert_eq!(root.snapshot().unwrap().children, children);
    assert_eq!(
        root.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Dormant
    );
    assert!(
        manager
            .lock_graph()
            .nodes
            .values()
            .all(|node| node.task.is_none())
    );
}

#[test_case(false; "correlation")]
#[test_case(true; "nominal_turn")]
fn cancel_cut_retires_authority_and_captures_children_before_close(nominal: bool) {
    const CORRELATION: &str = "root";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let (current_tx, current_rx) = flume::bounded(1);
    let (cancel_tx, cancel_rx) = flume::bounded(1);
    let root_gate = Gate::new();
    let root = manager
        .create_root_with_config(
            Some(config("anthropic/claude-sonnet-4-20250514", false)),
            Vec::new(),
            None,
            |_| {
                Ok::<_, String>(Box::new(TestBackend {
                    current: Some(current_tx),
                    cancel: Some(cancel_tx),
                    gate: Some(Arc::clone(&root_gate)),
                }))
            },
        )
        .unwrap();
    let root_actor = root.actor().unwrap();
    let ticket = root_actor
        .admit_turn(input(), None, CORRELATION.into())
        .unwrap();
    let current = current_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let root_cancel = cancel_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let root_publication = CheckpointPublication::default();
    let _root_registration = root_cancel.register_publication(&root_publication);
    assert_eq!(current.turn_id(), ticket.turn_id());
    assert!(!root_cancel.is_cancelled());
    assert!(!root_publication.is_cancelled());
    let independent_operation = root_actor.reserve_prepared_operation(None).unwrap();
    let independent_publication = CheckpointPublication::default();
    independent_operation
        .register_publication(&independent_publication)
        .unwrap();
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    let child_actor = child.actor().unwrap();
    let operation = child_actor.reserve_prepared_operation(None).unwrap();
    let publication = CheckpointPublication::default();
    operation.register_publication(&publication).unwrap();
    let (cut_tx, cut_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    manager.set_descendant_cut_gate(cut_tx, release_rx);
    let cancelling = manager.clone();
    let root_id = root.id();
    let turn_id = current.turn_id();
    assert_eq!(root_actor.processing_turn_id(), Some(turn_id));
    let cancel = std::thread::spawn(move || {
        if nominal {
            cancelling.cancel_turn(root_id, turn_id, crate::TurnCancellationReason::User)
        } else {
            cancelling.cancel_correlation(root_id, CORRELATION, crate::TurnCancellationReason::User)
        }
    });
    cut_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let child_snapshot = child.snapshot().unwrap();
    assert!(root_cancel.is_cancelled());
    assert!(root_publication.is_cancelled());
    assert!(!root_publication.try_publish());
    assert!(!root_cancel.claim_publication(&root_publication));
    let late_root_publication = CheckpointPublication::default();
    let _late_root_registration = root_cancel.register_publication(&late_root_publication);
    assert!(late_root_publication.is_cancelled());
    assert!(!root_cancel.claim_publication(&late_root_publication));
    assert!(current.validate_active().is_err());
    assert!(root.validate_live().is_ok());
    assert!(!independent_publication.is_cancelled());
    assert!(independent_publication.try_publish());
    assert!(publication.is_cancelled());
    assert!(!publication.try_publish());
    let late_publication = CheckpointPublication::default();
    assert!(operation.register_publication(&late_publication).is_err());
    assert!(late_publication.is_cancelled());
    assert!(child_actor.reserve_prepared_operation(None).is_err());
    let post_cut_spawn = manager.spawn_child(
        &current,
        AgentMetadata::default(),
        Vec::new(),
        None,
        TestBackend::boxed(),
    );
    release_tx.send(()).unwrap();
    cancel.join().unwrap().unwrap();
    assert!(matches!(
        child_snapshot.graph_lifecycle,
        GraphLifecycle::Closing | GraphLifecycle::Closed
    ));
    assert_eq!(
        child_snapshot.actor.unwrap().lifecycle,
        crate::ActorLifecycle::Closed
    );
    assert!(
        matches!(post_cut_spawn, Err(ManagerError::InactiveTurn { agent_id, turn_id })
        if agent_id == root_id && turn_id == current.turn_id())
    );
    assert_eq!(
        child_actor.snapshot().lifecycle,
        crate::ActorLifecycle::Closed
    );
    root_gate.release(1);
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
}

#[test_case(crate::TurnCancellationReason::User, false; "exact_admission_user")]
#[test_case(crate::TurnCancellationReason::Shutdown, false; "exact_admission_shutdown")]
#[test_case(crate::TurnCancellationReason::User, true; "shared_submission_user")]
#[test_case(crate::TurnCancellationReason::Shutdown, true; "shared_submission_shutdown")]
fn exact_cancel_pop_gap_preserves_same_correlation_successor(
    reason: crate::TurnCancellationReason,
    shared_submission: bool,
) {
    const CORRELATION: &str = "reused";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let (entered_tx, entered_rx) = flume::unbounded();
    let root = manager
        .create_root(Vec::new(), None, TestBackend::reporting(entered_tx, None))
        .unwrap();
    let actor = root.actor().unwrap();
    assert_eq!(actor.processing_turn_id(), None);
    let (popped, release) = actor.pause_after_next_pop();
    let target = if shared_submission {
        actor
            .submit(
                input(),
                None,
                CORRELATION.into(),
                TurnAdmissionOptions::default(),
            )
            .unwrap();
        None
    } else {
        Some(actor.admit_turn(input(), None, CORRELATION.into()).unwrap())
    };
    popped.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let target_id = actor.processing_turn_id().unwrap();
    let target = target.unwrap_or_else(|| actor.turn_ticket(target_id).unwrap());
    assert_eq!(target_id, target.turn_id());
    assert!(actor.turn_ticket(target_id).is_some());
    assert!(target.peek().is_none());
    assert!(entered_rx.is_empty());
    let successor = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
    manager
        .cancel_turn(root.id(), target.turn_id(), reason)
        .unwrap();
    release.send(()).unwrap();
    assert!(
        matches!(smol::block_on(target.wait()), TurnOutcome::Cancelled { reason: actual, .. } if actual == reason)
    );
    assert!(matches!(
        smol::block_on(successor.wait()),
        TurnOutcome::Completed { .. }
    ));
    assert_eq!(
        entered_rx
            .recv_timeout(COMPLETION_TIMEOUT)
            .unwrap()
            .turn_id(),
        successor.turn_id()
    );
    assert!(entered_rx.is_empty());
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
}

#[test]
fn late_exact_cancel_closes_children_of_a_turn_cut_first() {
    const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let (entered_tx, entered_rx) = flume::unbounded();
    let root = manager
        .create_root_with_config(Some(config(MODEL, false)), Vec::new(), None, |_| {
            Ok::<Box<dyn ActorBackend>, String>(Box::new(ReportingCancellableBackend {
                current: entered_tx,
            }))
        })
        .unwrap();
    let actor = root.actor().unwrap();
    let ticket = actor.admit_turn(input(), None, "cut".into()).unwrap();
    let current = entered_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    actor.cancel_turn(ticket.turn_id()).unwrap();
    smol::block_on(ticket.wait());
    assert_eq!(
        manager.cancel_turn(
            root.id(),
            ticket.turn_id(),
            crate::TurnCancellationReason::User
        ),
        Err(ManagerError::InactiveTurn {
            agent_id: root.id(),
            turn_id: ticket.turn_id()
        })
    );
    assert!(child.validate_live().is_err());
}

#[test_case(false; "exact_admission")]
#[test_case(true; "shared_submission")]
fn delayed_exact_cancel_preserves_successor_authority_and_children(shared_submission: bool) {
    const CORRELATION: &str = "reused";
    const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let (entered_tx, entered_rx) = flume::unbounded();
    let root = manager
        .create_root_with_config(Some(config(MODEL, false)), Vec::new(), None, |_| {
            Ok::<Box<dyn ActorBackend>, String>(Box::new(ReportingCancellableBackend {
                current: entered_tx,
            }))
        })
        .unwrap();
    let actor = root.actor().unwrap();
    let first = if shared_submission {
        actor
            .submit(
                input(),
                None,
                CORRELATION.into(),
                TurnAdmissionOptions::default(),
            )
            .unwrap();
        None
    } else {
        Some(actor.admit_turn(input(), None, CORRELATION.into()).unwrap())
    };
    let first_current = entered_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let captured = actor.processing_turn_id().unwrap();
    let first = first.unwrap_or_else(|| actor.turn_ticket(captured).unwrap());
    assert_eq!(captured, first_current.turn_id());
    manager
        .cancel_turn(root.id(), captured, crate::TurnCancellationReason::User)
        .unwrap();
    assert!(matches!(
        smol::block_on(first.wait()),
        TurnOutcome::Cancelled {
            reason: crate::TurnCancellationReason::User,
            ..
        }
    ));
    let successor = if shared_submission {
        actor
            .submit(
                input(),
                None,
                CORRELATION.into(),
                TurnAdmissionOptions::default(),
            )
            .unwrap();
        None
    } else {
        Some(actor.admit_turn(input(), None, CORRELATION.into()).unwrap())
    };
    let current = entered_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let successor = successor.unwrap_or_else(|| actor.turn_ticket(current.turn_id()).unwrap());
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    let stale = manager.cancel_turn(root.id(), captured, crate::TurnCancellationReason::User);
    assert_eq!(
        stale,
        Err(ManagerError::InactiveTurn {
            agent_id: root.id(),
            turn_id: captured
        })
    );
    assert_eq!(actor.processing_turn_id(), Some(successor.turn_id()));
    assert!(successor.peek().is_none());
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Live
    );
    assert_eq!(
        child.actor().unwrap().snapshot().lifecycle,
        crate::ActorLifecycle::Open
    );
    let another = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    let another_actor = another.actor().unwrap();
    let child_turn = child
        .actor()
        .unwrap()
        .admit_turn(input(), None, CORRELATION.into())
        .unwrap();
    assert!(matches!(
        smol::block_on(child_turn.wait()),
        TurnOutcome::Completed { .. }
    ));
    manager
        .cancel_turn(
            root.id(),
            successor.turn_id(),
            crate::TurnCancellationReason::User,
        )
        .unwrap();
    assert!(matches!(
        smol::block_on(successor.wait()),
        TurnOutcome::Cancelled { .. }
    ));
    assert_eq!(
        another_actor.snapshot().lifecycle,
        crate::ActorLifecycle::Closed
    );
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
}

#[test]
fn idle_guard_rejects_child_factory_reservation() {
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let (entered_tx, entered_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let creating = manager.clone();
    let child = std::thread::spawn(move || {
        creating.spawn_child_with(&current, AgentMetadata::default(), Vec::new(), None, |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, String>(TestBackend::boxed())
        })
    });
    entered_rx.recv().unwrap();
    assert!(matches!(
        manager.prepare_idle_subtree(root.id()),
        Err(ManagerError::BusySubtree(_))
    ));
    release_tx.send(()).unwrap();
    child.join().unwrap().unwrap();
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

fn idle_root_with_child(
    child_backend: Box<dyn ActorBackend>,
) -> (AgentManagerHandle, super::AgentRef, super::AgentRef) {
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            child_backend,
        )
        .unwrap();
    let root_actor = root.actor().unwrap();
    let events = root_actor.subscribe_events();
    gate.release(1);
    smol::block_on(root_actor.wait_outcome(current.turn_id())).unwrap();
    while !matches!(
        events.recv_timeout(COMPLETION_TIMEOUT).unwrap(),
        crate::actor::ActorEvent::Idle { .. }
    ) {}
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Live
    );
    (manager, root, child)
}

#[test_case(false, false; "drop_resumes_continuation")]
#[test_case(false, true; "drop_preserves_explicit_fifo")]
#[test_case(true, false; "abort_resumes_continuation")]
#[test_case(true, true; "abort_preserves_explicit_fifo")]
fn idle_guard_allows_idle_live_child(abort: bool, explicit: bool) {
    smol::block_on(async {
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        const SOURCE: &str = "source history";
        const CHILD: &str = "child after source acquisition";
        const FIRST: &str = "first explicit admission";
        const SECOND: &str = "second explicit admission";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root_gate = Gate::new();
        let (root_tx, root_rx) = flume::unbounded();
        let root = manager
            .create_root_with_config(
                Some(config(MODEL, false)),
                vec![makima_providers::Message::user(SOURCE.into())],
                None,
                |_| {
                    Ok::<_, String>(TestBackend::reporting(
                        root_tx,
                        Some(Arc::clone(&root_gate)),
                    ))
                },
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let initial = actor.admit_turn(input(), None, "root".into()).unwrap();
        let current = root_rx.recv_async().await.unwrap();
        let child_gate = Gate::new();
        let (child_tx, child_rx) = flume::unbounded();
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(child_tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let events = actor.subscribe_events();
        root_gate.release(1);
        initial.wait().await;
        while !matches!(
            events.recv_timeout(COMPLETION_TIMEOUT).unwrap(),
            crate::ActorEvent::Idle { .. }
        ) {}
        let guard = manager.prepare_idle_subtree(root.id()).unwrap();
        let source = guard.source_snapshot().unwrap();
        assert_eq!(source.agent_id, root.id());
        let config = actor.config_snapshot().unwrap();
        assert!(Arc::ptr_eq(&source.config.identity, &config.identity));
        assert!(Arc::ptr_eq(&source.config.config, &config.config));
        assert_eq!(source.config.generation, config.generation);
        assert_eq!(source.config.config.settings.model.spec(), MODEL);
        assert_eq!(
            source.history.messages[0].first_text_content(),
            Some(SOURCE)
        );
        let operation = guard.reserve_prepared_operation(None).unwrap();
        let predecessor = operation.wait_predecessor().await.unwrap();
        assert!(Arc::ptr_eq(&predecessor, &source.config.config));
        operation
            .resolve(Some(ConfigChange::ToggleWorkflow))
            .unwrap();
        operation.wait_ready().await.unwrap();
        assert!(operation.ready_config().unwrap().settings.workflow);
        assert!(!actor.effective_config().unwrap().settings.workflow);
        assert!(matches!(
            actor.reserve_prepared_operation(None),
            Err(ActorError::Busy)
        ));
        let queued: Vec<_> = if explicit {
            [FIRST, SECOND]
                .into_iter()
                .map(|correlation| actor.admit_turn(input(), None, correlation.into()).unwrap())
                .collect()
        } else {
            Vec::new()
        };
        let child_actor = child.actor().unwrap();
        child_actor.configure_parent_delivery(true).unwrap();
        let child_ticket = child_actor.admit_turn(input(), None, CHILD.into()).unwrap();
        child_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
        child_gate.release(1);
        assert!(matches!(
            child_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let accepted = loop {
            if let crate::ActorEvent::Delivery(event) =
                events.recv_timeout(COMPLETION_TIMEOUT).unwrap()
            {
                break event;
            }
        };
        assert_eq!(
            accepted.delivery.result.outcome.turn_id(),
            child_ticket.turn_id()
        );
        assert_eq!(accepted.status, crate::actor::DeliveryStatus::Accepted);
        child_actor.runtime_barrier().await.unwrap();
        actor.runtime_barrier().await.unwrap();
        let unchanged = guard.source_snapshot().unwrap();
        assert_eq!(unchanged.history.epoch, source.history.epoch);
        assert!(Arc::ptr_eq(
            &unchanged.history.messages,
            &source.history.messages
        ));
        assert_eq!(unchanged.config.generation, source.config.generation);
        assert!(Arc::ptr_eq(&unchanged.config.config, &source.config.config));
        assert_eq!(actor.snapshot().queued, queued.len());
        assert!(root_rx.is_empty());
        if abort {
            operation.cancel();
            assert!(matches!(
                operation.wait_ready().await,
                Err(ActorError::PolicyCancelled)
            ));
        }
        drop(operation);
        actor.runtime_barrier().await.unwrap();
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            source.config.generation
        );
        assert!(!actor.effective_config().unwrap().settings.fast);
        assert_eq!(actor.snapshot().queued, queued.len());
        assert!(root_rx.is_empty());
        assert!(matches!(actor.prepare_idle(), Err(ActorError::Busy)));
        drop(guard);
        let first = root_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
        if let Some(ticket) = queued.first() {
            assert_eq!(first.turn_id(), ticket.turn_id());
        }
        root_gate.release(1);
        let mut executed = Vec::new();
        let mut ended = Vec::new();
        let expected: Vec<_> = if explicit {
            let second = root_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
            assert_eq!(second.turn_id(), queued[1].turn_id());
            root_gate.release(1);
            queued.iter().map(|ticket| ticket.turn_id()).collect()
        } else {
            vec![first.turn_id()]
        };
        loop {
            match events.recv_timeout(COMPLETION_TIMEOUT).unwrap() {
                crate::ActorEvent::Delivery(event) => {
                    assert_eq!(
                        event.delivery.result.outcome.turn_id(),
                        child_ticket.turn_id()
                    );
                    assert_eq!(
                        event.status,
                        crate::actor::DeliveryStatus::Executed {
                            parent_turn_id: first.turn_id(),
                        }
                    );
                    executed.push(child_ticket.turn_id());
                }
                crate::ActorEvent::End(result) => ended.push(result.outcome.turn_id()),
                crate::ActorEvent::Idle { .. }
                    if ended.len() == expected.len() && !executed.is_empty() =>
                {
                    break;
                }
                _ => {}
            }
        }
        assert_eq!(executed, vec![child_ticket.turn_id()]);
        assert_eq!(ended, expected);
        assert!(root_rx.is_empty());
        let released = manager.prepare_idle_subtree(root.id()).unwrap();
        let resumed = released.source_snapshot().unwrap();
        assert_eq!(resumed.history.epoch, source.history.epoch);
        assert_eq!(
            resumed.history.messages.len(),
            source.history.messages.len() + 1
        );
        assert_eq!(
            resumed.history.messages[source.history.messages.len()].observation_source,
            Some(makima_providers::ObservationSource::ChildResult {
                agent_id: child.id().to_string(),
                turn_id: child_ticket.turn_id().to_string(),
            })
        );
        assert_eq!(source.history.messages.len(), 1);
        drop(released);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

enum ChildWork {
    Popped,
    Running,
}

#[test_case(ChildWork::Popped; "popped")]
#[test_case(ChildWork::Running; "running")]
fn idle_guard_rejects_busy_child(work: ChildWork) {
    let (entered_tx, entered_rx) = flume::unbounded();
    let child_gate = Gate::new();
    let (manager, root, child) = idle_root_with_child(TestBackend::reporting(
        entered_tx,
        Some(Arc::clone(&child_gate)),
    ));
    let child_actor = child.actor().unwrap();
    let events = child_actor.subscribe_events();
    let (popped, release) = child_actor.pause_after_next_pop();
    let ticket = child_actor
        .admit_turn(input(), None, "child".into())
        .unwrap();
    popped.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    if matches!(work, ChildWork::Running) {
        release.send(()).unwrap();
        entered_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    }
    assert!(matches!(
        manager.prepare_idle_subtree(root.id()),
        Err(ManagerError::BusySubtree(id)) if id == root.id()
    ));
    if matches!(work, ChildWork::Popped) {
        release.send(()).unwrap();
    }
    child_gate.release(1);
    smol::block_on(ticket.wait());
    while !matches!(
        events.recv_timeout(COMPLETION_TIMEOUT).unwrap(),
        crate::actor::ActorEvent::Idle { .. }
    ) {}
    drop(manager.prepare_idle_subtree(root.id()).unwrap());
    smol::block_on(manager.shutdown(COMPLETION_TIMEOUT));
}

struct Gate {
    entered: AtomicUsize,
    released: AtomicUsize,
    event: Event,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
            event: Event::new(),
        })
    }

    fn release(&self, count: usize) {
        self.released.fetch_add(count, Ordering::Release);
        self.event.notify(usize::MAX);
    }

    async fn enter(&self) {
        let position = self.entered.fetch_add(1, Ordering::AcqRel);
        loop {
            if self.released.load(Ordering::Acquire) > position {
                return;
            }
            let listener = self.event.listen();
            if self.released.load(Ordering::Acquire) > position {
                return;
            }
            listener.await;
        }
    }
}

struct TestBackend {
    current: Option<flume::Sender<crate::CurrentManagedTurn>>,
    cancel: Option<flume::Sender<CancelToken>>,
    gate: Option<Arc<Gate>>,
}

impl TestBackend {
    fn boxed() -> Box<dyn ActorBackend> {
        Box::new(Self {
            current: None,
            cancel: None,
            gate: None,
        })
    }

    fn reporting(
        current: flume::Sender<crate::CurrentManagedTurn>,
        gate: Option<Arc<Gate>>,
    ) -> Box<dyn ActorBackend> {
        Box::new(Self {
            current: Some(current),
            cancel: None,
            gate,
        })
    }
}

struct CancellableBackend {
    entered: flume::Sender<()>,
}

struct ReportingCancellableBackend {
    current: flume::Sender<crate::CurrentManagedTurn>,
}

impl ActorBackend for ReportingCancellableBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.current
                .send(context.managed_turn.clone().unwrap())
                .unwrap();
            let reason = context.cancel_reason.cancelled().await;
            BackendResult::EnteredRun(TurnOutcome::cancelled(
                context.agent_id,
                context.turn_id.unwrap(),
                TokenUsage::default(),
                0,
                reason,
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

impl ActorBackend for CancellableBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.entered.send(()).unwrap();
            let reason = context.cancel_reason.cancelled().await;
            BackendResult::EnteredRun(TurnOutcome::cancelled(
                context.agent_id,
                context.turn_id.unwrap(),
                TokenUsage::default(),
                0,
                reason,
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

struct SuspendDuringPollBackend {
    current: flume::Sender<crate::CurrentManagedTurn>,
    checked: flume::Sender<bool>,
}

struct SuspendDuringPollFuture {
    context: crate::CurrentManagedTurn,
    cancel: crate::ReasonedCancelToken,
    checked: flume::Sender<bool>,
    first_poll: bool,
    checked_once: bool,
}

impl Future for SuspendDuringPollFuture {
    type Output = BackendResult;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.first_poll {
            self.first_poll = false;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        if !self.checked_once {
            self.checked_once = true;
            let (cancel, _) = crate::ReasonedCancelToken::new();
            self.context.lease.inner.suspend(u64::MAX, cancel).unwrap();
            let limiter = Arc::clone(&self.context.lease.inner.limiter);
            let mut acquire = Box::pin(limiter.acquire_arc());
            self.checked
                .send(acquire.as_mut().poll(context).is_pending())
                .unwrap();
        }
        let mut cancelled = Box::pin(self.cancel.cancelled());
        let Poll::Ready(reason) = cancelled.as_mut().poll(context) else {
            return Poll::Pending;
        };
        Poll::Ready(BackendResult::EnteredRun(TurnOutcome::cancelled(
            self.context.agent_id(),
            self.context.turn_id(),
            TokenUsage::default(),
            0,
            reason,
        )))
    }
}

struct BlockingPendingFuture {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
}

impl Future for BlockingPendingFuture {
    type Output = BackendResult;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.entered.send(()).unwrap();
        self.release.recv().unwrap();
        Poll::Pending
    }
}

impl ActorBackend for SuspendDuringPollBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        let managed = context.managed_turn.unwrap();
        self.current.send(managed.clone()).unwrap();
        Box::pin(SuspendDuringPollFuture {
            context: managed,
            cancel: context.cancel_reason,
            checked: self.checked.clone(),
            first_poll: true,
            checked_once: false,
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

impl ActorBackend for TestBackend {
    fn run_turn<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(sender) = &self.current {
                sender.send(context.managed_turn.clone().unwrap()).unwrap();
            }
            if let Some(sender) = &self.cancel {
                sender.send(context.cancel.clone()).unwrap();
            }
            if let Some(gate) = &self.gate {
                gate.enter().await;
            }
            if let Some(inbox) = &context.inbox {
                for input in inbox.drain_instructions() {
                    history
                        .push(makima_providers::Message::user(input.message))
                        .await
                        .unwrap();
                }
                for message in inbox.drain_completions() {
                    history.push_observation(message).unwrap();
                }
                inbox.instructions_submitted();
                assert!(inbox.finish());
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

struct ConfigTestProvider;

impl Provider for ConfigTestProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [makima_providers::Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
        _delivery: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, makima_providers::AgentError>> {
        Box::pin(async { unreachable!() })
    }
}

fn config(model: &str, fast: bool) -> crate::actor::EffectiveAgentConfig {
    crate::actor::EffectiveAgentConfig::new(
        crate::RunSettings {
            provider: Arc::new(ConfigTestProvider),
            model: Model::from_spec(model).unwrap(),
            fast,
            workflow: false,
            thinking: Default::default(),
        },
        AgentMode::Build,
    )
}

fn input() -> AgentInput {
    AgentInput {
        message: "test".into(),
        images: Vec::new(),
        preamble: Vec::new(),
        prompt: None,
    }
}

fn active_root(
    limits: AgentLimits,
) -> (
    super::AgentManagerHandle,
    super::AgentRef,
    crate::CurrentManagedTurn,
    Arc<Gate>,
) {
    let manager = AgentManagerHandle::new(limits).unwrap();
    let (tx, rx) = flume::bounded(1);
    let gate = Gate::new();
    let root = manager
        .create_root_with_config(
            Some(config("anthropic/claude-sonnet-4-20250514", false)),
            Vec::new(),
            None,
            |_| Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate)))),
        )
        .unwrap();
    root.actor()
        .unwrap()
        .admit_turn(input(), None, "root".into())
        .unwrap();
    let current = smol::block_on(rx.recv_async()).unwrap();
    (manager, root, current, gate)
}

#[test_case("local"; "local_cancel")]
#[test_case("turn"; "cancel_turn")]
#[test_case("correlation"; "cancel_correlation")]
#[test_case("agent"; "cancel_agent")]
#[test_case("subtree"; "cancel_subtree")]
#[test_case("close"; "close_subtree")]
#[test_case("detach"; "detach_incarnation")]
#[test_case("end"; "turn_completion")]
#[test_case("history"; "history_replacement")]
#[test_case("shutdown"; "shutdown")]
fn state_write_authority_is_revoked_at_manager_transition(transition: &str) {
    use crate::{StateError, StateScope, StateSlotSpec, TurnCancellationReason};

    const SLOT_KEY: &str = "fixture/manager-state";
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let slot = manager
        .journal()
        .register_state::<Value>(StateSlotSpec {
            key: SLOT_KEY.into(),
            scope: StateScope::Agent,
            version: 1,
        })
        .unwrap();
    let access = current.state_access();
    slot.set(&access, Value::Bool(true), None).unwrap();
    match transition {
        "local" => {
            manager.local_cancel(root.id(), 0).unwrap();
        }
        "turn" => {
            manager
                .cancel_turn(root.id(), current.turn_id(), TurnCancellationReason::User)
                .unwrap();
        }
        "correlation" => {
            manager
                .cancel_correlation(root.id(), "root", TurnCancellationReason::User)
                .unwrap();
        }
        "agent" => {
            manager.cancel_agent(root.id()).unwrap();
        }
        "subtree" => {
            manager.cancel_subtree(root.id()).unwrap();
        }
        "close" => {
            manager.close_subtree(root.id()).unwrap();
        }
        "detach" => {
            manager
                .detach_incarnation(&root.incarnation().unwrap())
                .unwrap();
        }
        "end" => {
            let ticket = root.turn_ticket(current.turn_id()).unwrap();
            gate.release(1);
            smol::block_on(ticket.wait_result());
        }
        "history" => {
            let journal = manager.journal();
            journal
                .commit(JournalCommit {
                    state_epoch_agents: [root.id()].into(),
                    ..Default::default()
                })
                .unwrap();
            assert!(matches!(
                slot.set(&current.state_access(), Value::Bool(false), None),
                Err(StateError::StaleAccess)
            ));
        }
        "shutdown" => {
            smol::block_on(manager.shutdown(Duration::ZERO));
        }
        _ => unreachable!(),
    }
    assert!(matches!(
        slot.set(&access, Value::Bool(false), None),
        Err(StateError::StaleAccess)
    ));
    if matches!(transition, "detach" | "end" | "history") {
        slot.set(&root.state_access(), Value::Bool(false), None)
            .unwrap();
        assert_eq!(
            slot.get(&root.state_access()).unwrap().value,
            Some(Value::Bool(false))
        );
    }
    gate.release(1);
    smol::block_on(manager.shutdown(COMPLETION_TIMEOUT));
}

struct NonpersistentHistoryFenceBackend {
    compact: bool,
}

impl ActorBackend for NonpersistentHistoryFenceBackend {
    fn run_turn<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            use crate::{StateError, StateScope, StateSlotSpec};
            const SLOT_KEY: &str = "fixture/nonpersistent-invocation";
            let current = context.managed_turn.as_ref().unwrap();
            let slot = current
                .manager()
                .journal()
                .register_state::<Value>(StateSlotSpec {
                    key: SLOT_KEY.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            let old = current.capture_new_invocation_state_access();
            slot.set(&old, Value::Bool(true), None).unwrap();
            if self.compact {
                history.replace_compacted(Vec::new()).await.unwrap();
            } else {
                history.replace_snapshot(history.snapshot().clone()).await;
            }
            assert!(matches!(
                slot.set(&old, Value::Bool(false), None),
                Err(StateError::StaleAccess)
            ));
            slot.set(
                &current.capture_new_invocation_state_access(),
                Value::Bool(false),
                None,
            )
            .unwrap();
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

#[test_case(false; "identical_replacement")]
#[test_case(true; "compaction")]
fn nonpersistent_actor_history_fences_old_invocation_and_accepts_new(compact: bool) {
    smol::block_on(async {
        let journal = Arc::new(SessionJournal::nonpersistent());
        let manager =
            AgentManagerHandle::new_with_journal(AgentLimits::default(), Arc::clone(&journal))
                .unwrap();
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| {
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(
                        NonpersistentHistoryFenceBackend { compact },
                    ))
                },
            )
            .unwrap();
        let actor = root.actor().unwrap();
        assert!(actor.inner.journal.is_none());
        let ticket = actor
            .admit_turn(input(), None, "runtime-fence".into())
            .unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        assert!(!journal.snapshot().state.iter().any(|record| matches!(
            record.change,
            StateChange::Boundary { .. } | StateChange::Turn { .. } | StateChange::Delivery { .. }
        )));
        assert!(journal.snapshot().managed_histories.is_empty());
        manager.shutdown(COMPLETION_TIMEOUT).await;
    });
}

#[test]
fn new_invocation_captures_post_compaction_epoch_without_refreshing_old_access() {
    use crate::{StateError, StateScope, StateSlotSpec};

    const SLOT_KEY: &str = "fixture/invocation-state";
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let slot = manager
        .journal()
        .register_state::<Value>(StateSlotSpec {
            key: SLOT_KEY.into(),
            scope: StateScope::Agent,
            version: 1,
        })
        .unwrap();
    let old = current.capture_new_invocation_state_access();
    slot.set(&old, Value::Bool(true), None).unwrap();
    manager
        .journal()
        .commit(JournalCommit {
            state_epoch_agents: [root.id()].into(),
            ..Default::default()
        })
        .unwrap();
    assert!(matches!(
        slot.set(&old, Value::Bool(false), None),
        Err(StateError::StaleAccess)
    ));
    let fresh = current.capture_new_invocation_state_access();
    slot.set(&fresh, Value::Bool(false), None).unwrap();
    assert!(matches!(
        slot.set(&old, Value::Bool(true), None),
        Err(StateError::StaleAccess)
    ));
    manager.cancel_agent(root.id()).unwrap();
    assert!(matches!(
        slot.set(
            &current.capture_new_invocation_state_access(),
            Value::Bool(true),
            None
        ),
        Err(StateError::StaleAccess)
    ));
    gate.release(1);
    smol::block_on(manager.shutdown(COMPLETION_TIMEOUT));
}

#[test]
fn actor_owned_config_inherits_and_isolates_nodes() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (tx, rx) = flume::bounded(1);
        let gate = Gate::new();
        let initial = config("anthropic/claude-sonnet-4-20250514", false);
        let root = manager
            .create_root_with_config(Some(initial.clone()), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate))))
            })
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = rx.recv_async().await.unwrap();
        let captured = current.policy_snapshot().unwrap();
        assert_eq!(captured.model.id, initial.model.id);
        assert!(Arc::ptr_eq(&captured.provider, &initial.provider));

        let child = current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let inherited = child.effective_config().unwrap().unwrap();
        assert_eq!(inherited.model.id, initial.model.id);
        assert!(Arc::ptr_eq(&inherited.provider, &initial.provider));
        assert!(!inherited.fast);
        assert!(!root.effective_config().unwrap().unwrap().fast);
        assert!(!child.effective_config().unwrap().unwrap().fast);
        gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn agent_config_isolation_and_failed_switch() {
    smol::block_on(async {
        let first = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let second = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (tx, rx) = flume::unbounded();
        let gate = Gate::new();
        let initial = config("anthropic/claude-sonnet-4-20250514", false);
        let other = config("anthropic/claude-opus-4-20250514", true);
        let first_root = first
            .create_root_with_config(Some(initial.clone()), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(tx.clone(), Some(Arc::clone(&gate))))
            })
            .unwrap();
        let second_root = second
            .create_root_with_config(Some(other.clone()), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(tx.clone(), None))
            })
            .unwrap();
        let first_actor = first_root.actor().unwrap();
        let second_actor = second_root.actor().unwrap();
        let first_ticket = first_actor
            .admit_turn(input(), None, "first".into())
            .unwrap();
        let first_turn = rx.recv_async().await.unwrap();
        let child = first_turn
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let second_ticket = second_actor
            .admit_turn(input(), None, "second".into())
            .unwrap();
        let second_turn = rx.recv_async().await.unwrap();
        assert!(Arc::ptr_eq(
            &first_turn.policy_snapshot().unwrap().provider,
            &initial.provider
        ));
        assert!(Arc::ptr_eq(
            &second_turn.policy_snapshot().unwrap().provider,
            &other.provider
        ));
        assert_eq!(
            first_turn.policy_snapshot().unwrap().model.id,
            initial.model.id
        );
        assert_eq!(
            second_turn.policy_snapshot().unwrap().model.id,
            other.model.id
        );
        assert!(!first_turn.policy_snapshot().unwrap().fast);
        assert!(second_turn.policy_snapshot().unwrap().fast);
        assert!(!child.effective_config().unwrap().unwrap().fast);

        assert!(Arc::ptr_eq(
            &first_root.effective_config().unwrap().unwrap().provider,
            &initial.provider
        ));
        assert!(Arc::ptr_eq(
            &second_root.effective_config().unwrap().unwrap().provider,
            &other.provider
        ));
        gate.release(1);
        first_actor
            .wait_outcome(first_ticket.turn_id())
            .await
            .unwrap();
        second_actor
            .wait_outcome(second_ticket.turn_id())
            .await
            .unwrap();
        first.shutdown(std::time::Duration::from_secs(1)).await;
        second.shutdown(std::time::Duration::from_secs(1)).await;
    });
}

#[test]
fn child_spawn_without_parent_policy_fails_closed() {
    let (manager, root, current, gate) = {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (tx, rx) = flume::bounded(1);
        let gate = Gate::new();
        let root = manager
            .create_root(
                Vec::new(),
                None,
                TestBackend::reporting(tx, Some(Arc::clone(&gate))),
            )
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = smol::block_on(rx.recv_async()).unwrap();
        (manager, root, current, gate)
    };
    assert!(matches!(
        manager.spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed()
        ),
        Err(ManagerError::Policy(_))
    ));
    assert!(root.snapshot().unwrap().children.is_empty());
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn child_configuration_can_change_independently() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (tx, rx) = flume::bounded(1);
        let gate = Gate::new();
        let initial = config("anthropic/claude-sonnet-4-20250514", false);
        let root = manager
            .create_root_with_config(Some(initial.clone()), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate))))
            })
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = rx.recv_async().await.unwrap();
        let child = current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let before = manager.snapshot().len();
        let mut invalid = initial.clone();
        invalid.mode = AgentMode::Custom(crate::ModeId::Custom("missing-definition".into()));
        assert!(matches!(
            current.spawn_child_with_config(
                invalid,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            ),
            Err(ManagerError::Policy(_))
        ));
        assert_eq!(manager.snapshot().len(), before);
        let mut updated = config("anthropic/claude-opus-5", true);
        updated.settings.workflow = true;
        let update = child
            .actor()
            .unwrap()
            .reserve_prepared_operation(None)
            .unwrap();
        update
            .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                model: Some(PreparedModel {
                    provider: Arc::clone(&updated.provider),
                    model: updated.model.clone(),
                }),
                fast: Some(true),
                workflow: Some(true),
                ..Default::default()
            })))
            .await
            .unwrap();
        let actual = child.effective_config().unwrap().unwrap();
        assert_eq!(actual.model.id, updated.model.id);
        assert!(actual.fast);
        assert!(actual.workflow);
        assert!(Arc::ptr_eq(&actual.provider, &updated.provider));
        assert_eq!(
            root.effective_config().unwrap().unwrap().model.id,
            initial.model.id
        );
        gate.release(1);
        manager.shutdown(std::time::Duration::from_secs(1)).await;
    });
}

#[test]
fn restrictive_parent_delegates_without_broadening_child_mode() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (tx, rx) = flume::bounded(1);
        let gate = Gate::new();
        let plan_mode = AgentMode::Plan("plan.md".into());
        let mut root_config = config("anthropic/claude-sonnet-4-20250514", false);
        root_config.mode = plan_mode.clone();
        let root = manager
            .create_root_with_config(Some(root_config), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate))))
            })
            .unwrap();
        let actor = root.actor().unwrap();
        actor.admit_turn(input(), None, "root".into()).unwrap();
        let current = rx.recv_async().await.unwrap();
        let child = current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let child_actor = child.actor().unwrap();
        assert_eq!(child_actor.effective_config().unwrap().mode, plan_mode);
        assert!(
            child_actor
                .admit_turn(input(), None, "plan-child".into())
                .is_ok()
        );
        gate.release(1);
        manager.shutdown(std::time::Duration::from_secs(1)).await;
    });
}

#[test]
fn custom_parent_mode_is_admitted_by_managed_child() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (tx, rx) = flume::bounded(1);
        let gate = Gate::new();
        let custom_mode = AgentMode::Custom(crate::ModeId::parse("review"));
        let mut root_config = config("anthropic/claude-sonnet-4-20250514", false).with_mode_def(
            Some(crate::ModeDef::default_for(crate::ModeId::parse("review"))),
        );
        root_config.mode = custom_mode.clone();
        let root = manager
            .create_root_with_config(Some(root_config), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate))))
            })
            .unwrap();
        let actor = root.actor().unwrap();
        actor
            .admit_turn(input(), None, "root-custom".into())
            .unwrap();
        let current = rx.recv_async().await.unwrap();
        let child = current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let child_actor = child.actor().unwrap();
        assert_eq!(child_actor.effective_config().unwrap().mode, custom_mode);
        assert!(
            child_actor
                .admit_turn(input(), None, "custom-child".into())
                .is_ok()
        );
        gate.release(1);
        manager.shutdown(std::time::Duration::from_secs(1)).await;
    });
}

#[test]
fn zero_agent_limits_are_rejected() {
    for limits in [
        AgentLimits {
            max_concurrent_agent_turns: 0,
            ..AgentLimits::default()
        },
        AgentLimits {
            max_agent_depth: 0,
            ..AgentLimits::default()
        },
        AgentLimits {
            max_children_per_agent: 0,
            ..AgentLimits::default()
        },
        AgentLimits {
            max_active_agents: 0,
            ..AgentLimits::default()
        },
        AgentLimits {
            max_retained_agents: 0,
            ..AgentLimits::default()
        },
    ] {
        assert!(matches!(
            AgentManagerHandle::new(limits),
            Err(ManagerError::InvalidLimits)
        ));
    }
}

#[test]
fn backend_poll_holds_physical_permit_while_suspension_races() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let (current_tx, current_rx) = flume::bounded(1);
        let (checked_tx, checked_rx) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                Box::new(SuspendDuringPollBackend {
                    current: current_tx,
                    checked: checked_tx,
                }),
            )
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = current_rx.recv_async().await.unwrap();

        assert!(checked_rx.recv_async().await.unwrap());
        {
            let state = current.lease.inner.state.lock().unwrap();
            assert_eq!(state.suspensions, 1);
            assert!(state.permit.is_none());
        }
        current.lease.inner.retire(u64::MAX);

        root.actor().unwrap().shutdown();
        loop {
            let closed = {
                let state = current.lease.inner.state.lock().unwrap();
                state.closing && state.watcher_cancels.is_empty()
            };
            if closed {
                break;
            }
            smol::future::yield_now().await;
        }
        assert!(matches!(
            manager.spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            ),
            Err(ManagerError::InactiveTurn { .. })
        ));
        let permit = manager.0.limiter.acquire_arc().await;
        drop(permit);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

fn install_active_turn(
    manager: &AgentManagerHandle,
    agent_id: crate::AgentId,
    turn_id: crate::TurnId,
) {
    let actor = manager.actor(agent_id).unwrap();
    let mut state = actor.inner.state.lock().unwrap();
    state.status = crate::ActorStatus::Running(turn_id);
    state.active = Some(crate::actor::ActiveCancel::new(None).0);
}

fn initialize_delegating_turn(current: &mut crate::CurrentManagedTurn) {
    let config = config("anthropic/claude-sonnet-4-20250514", false).with_mode_def(Some(
        crate::modes::ModeRegistry::builtin().current(&AgentMode::Build),
    ));
    current.policy = Some(Arc::new(config.settings));
    current.mode = Some(config.mode);
    current.mode_def = config.mode_def;
    current.context = Some(config.context);
}

#[test]
fn local_cancel_retires_unpolled_turn_authority_without_closing_live_child() {
    const DISPLAYED: u64 = 7;
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let turn_id = crate::TurnId::generate();
        install_active_turn(&manager, root.id(), turn_id);
        let (_cancel, token) = crate::ReasonedCancelToken::new();
        let (guard, current) =
            super::enter_managed_turn(&Arc::downgrade(&manager.0), root.id(), turn_id, &token)
                .await
                .unwrap();
        let mut current = current;
        initialize_delegating_turn(&mut current);
        let execution = super::manage_execution(
            Box::pin(std::future::pending()),
            guard,
            Arc::clone(&current.lease.inner),
            token,
        );
        let child = current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let child_turn = crate::TurnId::generate();
        install_active_turn(&manager, child.id(), child_turn);
        let (_child_cancel, child_token) = crate::ReasonedCancelToken::new();
        let (child_guard, child_current) = super::enter_managed_turn(
            &Arc::downgrade(&manager.0),
            child.id(),
            child_turn,
            &child_token,
        )
        .await
        .unwrap();

        let mut child_current = child_current;
        initialize_delegating_turn(&mut child_current);
        let captured = manager.local_cancel(root.id(), DISPLAYED).unwrap();
        assert_eq!(captured, (Some(turn_id), DISPLAYED + 1));
        let inactive = ManagerError::InactiveTurn {
            agent_id: root.id(),
            turn_id,
        };
        assert_eq!(current.validate_active(), Err(inactive.clone()));
        assert!(matches!(
            current.spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            ),
            Err(error) if error == inactive
        ));
        assert_eq!(
            child.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Live
        );
        assert_eq!(
            child.actor().unwrap().snapshot().lifecycle,
            crate::ActorLifecycle::Open
        );
        child_current.validate_active().unwrap();
        child_current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();

        drop(execution);
        drop(child_guard);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn local_cancel_preserves_successor_authority_after_retired_execution_drops() {
    const DISPLAYED: u64 = 7;
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let turn_id = crate::TurnId::generate();
        install_active_turn(&manager, root.id(), turn_id);
        let (_cancel, token) = crate::ReasonedCancelToken::new();
        let (guard, current) =
            super::enter_managed_turn(&Arc::downgrade(&manager.0), root.id(), turn_id, &token)
                .await
                .unwrap();
        assert_eq!(
            manager.local_cancel(root.id(), DISPLAYED).unwrap(),
            (Some(turn_id), DISPLAYED + 1)
        );
        assert!(current.validate_active().is_err());

        let successor_id = crate::TurnId::generate();
        install_active_turn(&manager, root.id(), successor_id);
        let (_successor_cancel, successor_token) = crate::ReasonedCancelToken::new();
        let (successor_guard, successor) = super::enter_managed_turn(
            &Arc::downgrade(&manager.0),
            root.id(),
            successor_id,
            &successor_token,
        )
        .await
        .unwrap();
        let mut successor = successor;
        initialize_delegating_turn(&mut successor);
        drop(guard);
        successor.validate_active().unwrap();
        let child = successor
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        assert_eq!(
            child.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Live
        );
        drop(successor_guard);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn completed_waiter_observes_ownership_during_backend_poll() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let turn_id = crate::TurnId::generate();
        install_active_turn(&manager, root.id(), turn_id);
        let (_cancel, token) = crate::ReasonedCancelToken::new();
        let (guard, current) =
            super::enter_managed_turn(&Arc::downgrade(&manager.0), root.id(), turn_id, &token)
                .await
                .unwrap();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut execution = Box::pin(super::manage_execution(
            Box::pin(BlockingPendingFuture {
                entered: entered_tx,
                release: release_rx,
            }),
            guard,
            Arc::clone(&current.lease.inner),
            token,
        ));
        let poll_task =
            smol::spawn(async move { futures_lite::future::poll_once(&mut execution).await });
        entered_rx.recv_async().await.unwrap();
        let (wait_cancel, _) = crate::ReasonedCancelToken::new();
        current.lease.inner.suspend(u64::MAX, wait_cancel).unwrap();
        current.lease.inner.retire(u64::MAX);

        let mut owned = Box::pin(current.lease.inner.wait_until_owned());
        assert!(futures_lite::future::poll_once(&mut owned).await.is_none());
        release_tx.send(()).unwrap();
        assert!(poll_task.await.is_none());
        owned.await;

        root.actor().unwrap().shutdown();
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn cancellation_wakes_pending_managed_execution_and_runs_cleanup() {
    struct CancellationWake(AtomicUsize);

    impl Wake for CancellationWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let turn_id = crate::TurnId::generate();
        install_active_turn(&manager, root.id(), turn_id);
        let (cancel, token) = crate::ReasonedCancelToken::new();
        let (guard, current) =
            super::enter_managed_turn(&Arc::downgrade(&manager.0), root.id(), turn_id, &token)
                .await
                .unwrap();
        let (polled_tx, polled_rx) = flume::unbounded();
        let cleanup_gate = Gate::new();
        let backend_gate = Arc::clone(&cleanup_gate);
        let backend_cancel = token.clone();
        let backend_agent_id = root.id();
        let backend = async move {
            polled_tx.send(()).unwrap();
            let reason = futures_lite::future::poll_fn(|_| {
                backend_cancel.reason().map_or(Poll::Pending, Poll::Ready)
            })
            .await;
            polled_tx.send(()).unwrap();
            backend_gate.enter().await;
            BackendResult::EnteredRun(TurnOutcome::cancelled(
                backend_agent_id,
                turn_id,
                TokenUsage::default(),
                0,
                reason,
            ))
        };
        let mut execution = Box::pin(super::manage_execution(
            Box::pin(backend),
            guard,
            Arc::clone(&current.lease.inner),
            token,
        ));
        let wake = Arc::new(CancellationWake(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        assert!(execution.as_mut().poll(&mut context).is_pending());
        polled_rx.try_recv().unwrap();
        wake.0.store(0, Ordering::SeqCst);
        cancel.cancel(crate::TurnCancellationReason::User);
        assert!(wake.0.load(Ordering::SeqCst) > 0);
        assert!(execution.as_mut().poll(&mut context).is_pending());
        polled_rx.try_recv().unwrap();
        assert!(
            !manager
                .lock_graph()
                .active_turns
                .contains_key(&(root.id(), turn_id))
        );
        assert!(!current.lease.inner.state.lock().unwrap().closing);

        assert!(current.lease.inner.state.lock().unwrap().permit.is_some());
        cleanup_gate.release(1);
        assert!(matches!(
            execution.await,
            BackendResult::EnteredRun(TurnOutcome::Cancelled {
                reason: crate::TurnCancellationReason::User,
                ..
            })
        ));
        {
            let state = current.lease.inner.state.lock().unwrap();
            assert!(state.closing);
            assert!(state.permit.is_none());
        }
        let permit = manager.0.limiter.acquire_arc().await;
        drop(permit);
        root.actor().unwrap().shutdown();
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn dropping_pending_managed_execution_closes_lease_and_releases_permit() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let turn_id = crate::TurnId::generate();
        install_active_turn(&manager, root.id(), turn_id);
        let (_cancel, token) = crate::ReasonedCancelToken::new();
        let (guard, current) =
            super::enter_managed_turn(&Arc::downgrade(&manager.0), root.id(), turn_id, &token)
                .await
                .unwrap();
        let mut execution = Box::pin(super::manage_execution(
            Box::pin(std::future::pending()),
            guard,
            Arc::clone(&current.lease.inner),
            token,
        ));
        assert!(
            futures_lite::future::poll_once(&mut execution)
                .await
                .is_none()
        );
        drop(execution);

        {
            let state = current.lease.inner.state.lock().unwrap();
            assert!(state.closing);
            assert!(state.permit.is_none());
        }
        assert!(
            !manager
                .lock_graph()
                .active_turns
                .contains_key(&(root.id(), turn_id))
        );
        let permit = manager.0.limiter.acquire_arc().await;
        drop(permit);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn child_spawn_inherits_running_turn_cwd_not_later_config() {
    smol::block_on(async {
        const ORIGINAL: &str = "/original";
        const CHANGED: &str = "/changed";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (tx, rx) = flume::bounded(1);
        let gate = Gate::new();
        let initial = config("anthropic/claude-sonnet-4-20250514", false)
            .with_context(crate::actor::AgentContextSnapshot::new(ORIGINAL.into()));
        let root = manager
            .create_root_with_config(Some(initial), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate))))
            })
            .unwrap();
        let actor = root.actor().unwrap();
        actor.admit_turn(input(), None, ORIGINAL.into()).unwrap();
        let current = rx.recv_async().await.unwrap();
        let update = actor.reserve_prepared_operation(None).unwrap();
        update
            .commit_when_ready(Some(crate::actor::ConfigChange::WorkingDirectory(
                CHANGED.into(),
            )))
            .await
            .unwrap();
        assert_eq!(
            actor.effective_config().unwrap().context.cwd(),
            std::path::Path::new(CHANGED)
        );
        let child = current
            .spawn_child(
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        assert_eq!(
            child.effective_config().unwrap().unwrap().context.cwd(),
            std::path::Path::new(ORIGINAL)
        );
        gate.release(1);
        assert!(
            manager
                .shutdown(std::time::Duration::from_secs(1))
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn root_and_nested_children_use_one_factory() {
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let (tx, rx) = flume::bounded(1);
    let child_gate = Gate::new();
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::reporting(tx, Some(Arc::clone(&child_gate))),
        )
        .unwrap();
    child
        .actor()
        .unwrap()
        .admit_turn(input(), None, "child".into())
        .unwrap();
    let child_turn = smol::block_on(rx.recv_async()).unwrap();
    let grandchild = manager
        .spawn_child(
            &child_turn,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    let root_node = root.snapshot().unwrap();
    let child_node = child.snapshot().unwrap();
    let grandchild_node = grandchild.snapshot().unwrap();
    assert_eq!(root_node.parent_id, None);
    assert_eq!(root_node.root_id, root.id());
    assert_eq!(root_node.children, vec![child.id()]);
    assert_eq!(child_node.parent_id, Some(root.id()));
    assert_eq!(child_node.root_id, root.id());
    assert_eq!(child_node.depth, 1);
    assert_eq!(child_node.children, vec![grandchild.id()]);
    assert_eq!(grandchild_node.parent_id, Some(child.id()));
    assert_eq!(grandchild_node.root_id, root.id());
    assert_eq!(grandchild_node.depth, 2);
    gate.release(1);
    child_gate.release(1);
    let shutdown = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(shutdown.timed_out.is_empty());
}

#[test_case(false; "immediate")]
#[test_case(true; "deferred")]
fn supplied_root_identity_journals_config_and_reopens_without_recreating(deferred: bool) {
    const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    const INITIAL: &str = "initial root history";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let id = crate::AgentId::generate();
    let messages = vec![makima_providers::Message::user(INITIAL.into())];
    let (start, wait) = flume::bounded(1);
    let root = if deferred {
        manager.create_root_deferred_with_id_and_config(
            id,
            Some(config(MODEL, false)),
            messages,
            None,
            |actual| {
                assert_eq!(actual, id);
                Ok::<_, String>(TestBackend::boxed())
            },
            wait,
        )
    } else {
        manager.create_root_with_id_and_config(
            id,
            Some(config(MODEL, false)),
            messages,
            None,
            |actual| {
                assert_eq!(actual, id);
                Ok::<_, String>(TestBackend::boxed())
            },
        )
    }
    .unwrap();
    assert_eq!(root.id(), id);
    if deferred {
        start.send(()).unwrap();
    }
    let snapshot = manager.journal().snapshot();
    assert_eq!(snapshot.agent_revisions.len(), 1);
    let created = snapshot.agent_revisions[0].1.created_seq;
    assert_eq!(snapshot.state.len(), 1);
    assert!(
        matches!(&snapshot.state[0].change, StateChange::Config(saved) if saved.model == MODEL)
    );
    let mut session = Session::new(MODEL, ".");
    session.adopt_history(&snapshot.histories[&id]);
    snapshot.apply_to(&mut session).unwrap();
    assert_eq!(session.messages().len(), 1);
    let mut projection = session.clone();
    projection.adopt_history(&crate::HistorySnapshot::new(Vec::new()));
    snapshot.apply_to(&mut projection).unwrap();
    assert_eq!(projection.messages().len(), 1);
    assert_eq!(projection.messages()[0].user_text(), Some(INITIAL));
    assert_eq!(
        projection.history_snapshot().epoch,
        snapshot.histories[&id].epoch
    );
    smol::block_on(manager.shutdown(Duration::from_secs(1)));
    assert_eq!(
        manager.journal().agent(id).unwrap().lifecycle,
        StoredAgentLifecycle::Open
    );
    let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
    assert_ne!(journal.snapshot().epoch, snapshot.epoch);
    let restored =
        AgentManagerHandle::new_with_journal(AgentLimits::default(), Arc::clone(&journal)).unwrap();
    restored
        .create_root_with_id_and_config(
            id,
            Some(config(MODEL, true)),
            session.messages().to_vec(),
            None,
            |_| Ok::<_, String>(TestBackend::boxed()),
        )
        .unwrap();
    let reopened = journal.snapshot();
    assert_eq!(reopened.state, snapshot.state);
    assert_eq!(reopened.agent_revisions.len(), 2);
    assert_eq!(journal.agent(id).unwrap().created_seq, created);
    assert!(reopened.seq > snapshot.seq);
    reopened.apply_to(&mut session).unwrap();
    smol::block_on(restored.shutdown(Duration::from_secs(1)));
}

#[test]
fn journaled_root_history_clear_survives_checkpoint_reload() {
    const MODEL: &str = "test/model";
    const INITIAL: &str = "root history before clear";
    let mut session =
        Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::new(MODEL, ".");
    let id = crate::AgentId::generate();
    session.bind_root_id(id).unwrap();
    let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
    let mut history = History::new(vec![makima_providers::Message::user(INITIAL.into())])
        .with_journal(id, Arc::clone(&journal));
    let initial = journal.snapshot();
    initial.apply_to(&mut session).unwrap();
    assert_eq!(session.messages()[0].user_text(), Some(INITIAL));
    history.replace(Vec::new());
    let cleared_epoch = history.snapshot().epoch;
    let cleared = journal.snapshot();
    assert!(cleared.revision > initial.revision);
    assert!(cleared.histories[&id].messages.is_empty());
    cleared.apply_to(&mut session).unwrap();
    assert!(session.messages().is_empty());
    assert_eq!(session.history_snapshot().epoch, cleared_epoch);
    let directory = tempfile::tempdir().unwrap();
    session.save_to(directory.path()).unwrap();
    let mut loaded =
        Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::load_from(
            session.id,
            directory.path(),
        )
        .unwrap();
    assert!(loaded.messages().is_empty());
    let restored = Arc::new(SessionJournal::from_session(&loaded).unwrap());
    assert!(restored.snapshot().histories[&id].messages.is_empty());
    let resumed = History::restored_snapshot(loaded.history_snapshot())
        .with_journal(id, Arc::clone(&restored));
    assert!(resumed.is_empty());
    loaded.adopt_history(&initial.histories[&id]);
    restored.snapshot().apply_to(&mut loaded).unwrap();
    assert!(loaded.messages().is_empty());
}

#[test_case(false; "shutdown_wins")]
#[test_case(true; "explicit_close_wins")]
fn explicit_close_racing_shutdown_preserves_first_durable_lifecycle(close_first: bool) {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (_start, wait) = flume::bounded(1);
        let root = manager
            .create_root_deferred_with_config(
                None,
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::boxed()),
                wait,
            )
            .unwrap();
        if close_first {
            manager.close_subtree(root.id()).unwrap();
        }
        let (entered, captured) = flume::bounded(1);
        let (release, resume) = flume::bounded(1);
        *manager.0.shutdown_cut_gate.lock().unwrap() = Some(super::TestGate {
            entered,
            release: resume,
        });
        let shutting_down = manager.clone();
        let shutdown =
            std::thread::spawn(move || smol::block_on(shutting_down.shutdown(COMPLETION_TIMEOUT)));
        captured.recv_async().await.unwrap();
        let before = manager.journal().snapshot().revision;
        assert_eq!(
            manager.close_subtree(root.id()),
            Err(ManagerError::GraphShutdown)
        );
        let lifecycle = if close_first {
            StoredAgentLifecycle::Closed
        } else {
            StoredAgentLifecycle::Open
        };
        assert_eq!(
            manager.journal().agent(root.id()).unwrap().lifecycle,
            lifecycle
        );
        assert_eq!(manager.journal().snapshot().revision, before);
        release.send(()).unwrap();
        let report = shutdown.join().unwrap();
        assert!(report.timed_out.is_empty());
        assert_eq!(
            manager.journal().agent(root.id()).unwrap().lifecycle,
            lifecycle
        );
    });
}

fn queued_result(text: &str) -> Arc<ChildDelivery> {
    Arc::new(ChildDelivery {
        label: None,
        result: TurnResult {
            outcome: TurnOutcome::cancelled(
                crate::AgentId::generate(),
                crate::TurnId::generate(),
                TokenUsage::default(),
                0,
                crate::TurnCancellationReason::User,
            ),
            text: text.into(),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        },
    })
}

fn delivery_bytes(delivery: &ChildDelivery) -> usize {
    delivery.preview(None).input().preamble[0]
        .first_text_content()
        .unwrap()
        .len()
}

#[test_case(false; "count_bound")]
#[test_case(true; "preview_byte_bound")]
fn disk_restored_delivery_queue_preserves_exact_capacity(byte_bound: bool) {
    smol::block_on(async {
        const MODEL: &str = "test/model";
        const TEXT: &str = "queued result";
        const HALF_PREVIEW: usize = crate::actor::MAX_DELIVERY_PREVIEW_BYTES / 2;
        let mut session =
            Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::new(MODEL, ".");
        let parent = crate::AgentId::generate();
        session.bind_root_id(parent).unwrap();
        let journal = SessionJournal::from_session(&session).unwrap();
        let text = if byte_bound {
            "x".repeat(HALF_PREVIEW)
        } else {
            TEXT.into()
        };
        let envelope = queued_result(&text);
        let bytes = delivery_bytes(&envelope);
        let capacity = MAX_PENDING_DELIVERIES.min(MAX_PENDING_PREVIEW_BYTES / bytes);
        let restored_count = capacity / 2;
        assert!(restored_count > 0);
        journal
            .commit(JournalCommit {
                changes: (0..restored_count)
                    .map(|_| {
                        let delivery = queued_result(&text);
                        crate::actor::delivery::change(
                            parent,
                            &delivery,
                            StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                                label: delivery.label.clone(),
                                result: delivery.result.to_stored(session.id, None).unwrap(),
                                preview_bytes: None,
                                held: false,
                            })),
                        )
                    })
                    .collect(),
                ..Default::default()
            })
            .unwrap();
        journal.snapshot().apply_to(&mut session).unwrap();
        let directory = tempfile::tempdir().unwrap();
        session.save_to(directory.path()).unwrap();
        let loaded =
            Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::load_from(
                session.id,
                directory.path(),
            )
            .unwrap();
        let restored = Arc::new(SessionJournal::from_session(&loaded).unwrap());
        for _ in restored_count..capacity {
            assert!(
                restored
                    .queue_delivery(parent, queued_result(&text), bytes)
                    .unwrap()
            );
        }
        assert_eq!(
            restored.queue_delivery(parent, queued_result(&text), bytes),
            Err(DeliveryRejection::Overflow),
        );
        yield_until(|| restored.pending_revision().is_none()).await;
        let mut saved = loaded;
        restored.snapshot().apply_to(&mut saved).unwrap();
        saved.save_to(directory.path()).unwrap();
        let reloaded =
            Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::load_from(
                saved.id,
                directory.path(),
            )
            .unwrap();
        let view = reloaded.latest_state().unwrap();
        assert_eq!(view.scopes[&Some(parent)].deliveries.len(), capacity);
        assert!(
            view.scopes[&Some(parent)]
                .deliveries
                .values()
                .all(|op| matches!(op, StoredDeliveryOp::Queued(_)))
        );
    });
}

#[test_case(StoredDeliveryOp::Consumed; "consumed")]
#[test_case(StoredDeliveryOp::Withdrawn; "withdrawn")]
#[test_case(StoredDeliveryOp::Dropped; "dropped")]
fn disk_restored_terminal_delivery_deduplicates_before_bounds(op: StoredDeliveryOp) {
    smol::block_on(async {
        const MODEL: &str = "test/model";
        const TEXT: &str = "terminal delivery";
        let mut session =
            Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::new(MODEL, ".");
        let parent = crate::AgentId::generate();
        session.bind_root_id(parent).unwrap();
        let journal = SessionJournal::from_session(&session).unwrap();
        let delivery = queued_result(TEXT);
        let key = (
            delivery.result.outcome.agent_id(),
            delivery.result.outcome.turn_id(),
        );
        let queued = crate::actor::delivery::change(
            parent,
            &delivery,
            StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                label: None,
                result: delivery.result.to_stored(session.id, None).unwrap(),
                preview_bytes: None,
                held: false,
            })),
        );
        journal
            .commit(JournalCommit {
                changes: vec![
                    queued,
                    crate::actor::delivery::change(parent, &delivery, op.clone()),
                ],
                ..Default::default()
            })
            .unwrap();
        journal.snapshot().apply_to(&mut session).unwrap();
        let directory = tempfile::tempdir().unwrap();
        session.save_to(directory.path()).unwrap();
        let loaded =
            Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::load_from(
                session.id,
                directory.path(),
            )
            .unwrap();
        let restored = Arc::new(SessionJournal::from_session(&loaded).unwrap());
        let before = restored.snapshot();
        assert!(
            !restored
                .queue_delivery(parent, delivery, MAX_PENDING_PREVIEW_BYTES)
                .unwrap()
        );
        assert_eq!(restored.snapshot().state, before.state);
        assert_eq!(restored.snapshot().revision, before.revision);
        assert!(restored.pending_revision().is_none());
        let mut saved = loaded;
        restored.snapshot().apply_to(&mut saved).unwrap();
        saved.save_to(directory.path()).unwrap();
        let reloaded =
            Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::load_from(
                saved.id,
                directory.path(),
            )
            .unwrap();
        assert_eq!(
            reloaded.latest_state().unwrap().scopes[&Some(parent)].deliveries[&key],
            op
        );
    });
}

#[test]
fn transcript_key_query_includes_reservations_and_hidden_logical_agents() {
    const KEY: &str = "hidden-child-transcript";
    const RESERVED: &str = "reserved-child-transcript";
    const AVAILABLE: &str = "available-child-transcript";
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let journal = manager.journal();
    let barrier = journal.stage(JournalCommit::default(), 1).unwrap();
    let pending_revision = journal.pending_revision();
    assert!(pending_revision.is_some());
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata {
                transcript_key: Some(KEY.into()),
                ..Default::default()
            },
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    assert!(
        !journal
            .snapshot()
            .agent_revisions
            .iter()
            .any(|(_, record)| record.id == child.id())
    );
    let hidden =
        AgentManagerHandle::new_with_journal(AgentLimits::default(), Arc::clone(&journal)).unwrap();
    assert!(manager.transcript_key_in_use(KEY));
    assert!(hidden.transcript_key_in_use(KEY));
    assert!(!hidden.transcript_key_in_use(AVAILABLE));
    assert_eq!(journal.pending_revision(), pending_revision);
    let reserved_child = manager
        .spawn_child_with(
            &current,
            AgentMetadata {
                transcript_key: Some(RESERVED.into()),
                ..Default::default()
            },
            Vec::new(),
            None,
            |_| {
                assert!(manager.transcript_key_in_use(RESERVED));
                assert!(!hidden.transcript_key_in_use(RESERVED));
                Ok::<_, String>(TestBackend::boxed())
            },
        )
        .unwrap();
    assert!(hidden.transcript_key_in_use(RESERVED));
    assert!(matches!(
        manager.set_agent_transcript_key(reserved_child.id(), KEY.into()),
        Err(ManagerError::Policy(_))
    ));
    journal
        .complete(
            barrier,
            vec![JournalChange {
                agent: Some(root.id()),
                generation: 0,
                change: StateChange::Admitted {
                    turn: crate::TurnId::generate(),
                    origin: makima_storage::session_state::StoredTurnOrigin::Internal,
                },
            }],
        )
        .unwrap();
    gate.release(1);
    smol::block_on(manager.shutdown(COMPLETION_TIMEOUT));
}

#[test]
fn child_graph_metadata_is_persisted_and_explicit_close_is_durable() {
    const LABEL: &str = "durable child";
    const TOOL: &str = "spawn-tool";
    const INITIAL: &str = "child history";
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata {
                label: Some(LABEL.into()),
                spawned_by_tool_use_id: Some(TOOL.into()),
                transcript_key: Some(TOOL.into()),
            },
            vec![makima_providers::Message::user(INITIAL.into())],
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    let journal = manager.journal();
    let before_collision = journal.snapshot();
    assert!(matches!(
        manager.spawn_child_with(
            &current,
            AgentMetadata {
                label: None,
                spawned_by_tool_use_id: Some(TOOL.into()),
                transcript_key: Some(TOOL.into()),
            },
            Vec::new(),
            None,
            |_| -> Result<Box<dyn ActorBackend>, String> {
                panic!("duplicate transcript key reached the actor factory")
            },
        ),
        Err(ManagerError::Policy(_))
    ));
    let after_collision = journal.snapshot();
    assert_eq!(after_collision.revision, before_collision.revision);
    assert_eq!(after_collision.state, before_collision.state);
    assert_eq!(
        after_collision.agent_revisions,
        before_collision.agent_revisions
    );
    assert_eq!(
        after_collision.histories.len(),
        before_collision.histories.len()
    );
    const SIBLING_KEY: &str = "session-sibling";
    let sibling = manager
        .spawn_child(
            &current,
            AgentMetadata {
                label: None,
                spawned_by_tool_use_id: Some(TOOL.into()),
                transcript_key: Some(SIBLING_KEY.into()),
            },
            vec![makima_providers::Message::user(SIBLING_KEY.into())],
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    assert_eq!(
        journal.agent(sibling.id()).unwrap().transcript_key,
        SIBLING_KEY
    );
    let snapshot = journal.snapshot();
    assert_eq!(journal.agent(child.id()).unwrap().transcript_key, TOOL);
    let created = journal.agent(child.id()).unwrap().created_seq;
    manager
        .set_agent_transcript_key(child.id(), TOOL.into())
        .unwrap();
    assert_eq!(journal.snapshot().revision, snapshot.revision);
    assert!(
        manager
            .set_agent_transcript_key(child.id(), String::new())
            .is_err()
    );
    assert_eq!(journal.agent(child.id()).unwrap().created_seq, created);
    let mut session = Session::new("test/model", ".");
    snapshot.apply_to(&mut session).unwrap();
    let directory = tempfile::tempdir().unwrap();
    session.save_to(directory.path()).unwrap();
    let mut loaded =
        Session::<makima_providers::Message, TokenUsage, crate::ToolOutput>::load_from(
            session.id,
            directory.path(),
        )
        .unwrap();
    let saved = &loaded.agents()[&child.id()];
    assert_eq!(saved.parent, Some(root.id()));
    assert_eq!(saved.depth, 1);
    assert_eq!(saved.label.as_deref(), Some(LABEL));
    assert_eq!(saved.spawned_by_tool_use_id.as_deref(), Some(TOOL));
    assert!(saved.restorable.is_err());
    assert_eq!(loaded.subagent_messages()[&saved.transcript_key].len(), 1);
    manager.close_subtree(child.id()).unwrap();
    let closed = journal.snapshot();
    closed.apply_to(&mut loaded).unwrap();
    snapshot.apply_to(&mut loaded).unwrap();
    assert_eq!(
        loaded.agents()[&child.id()].lifecycle,
        StoredAgentLifecycle::Closed
    );
    assert_eq!(
        loaded.agents()[&root.id()].lifecycle,
        StoredAgentLifecycle::Open
    );
    assert_eq!(journal.published_revision(), 0);
    journal.acknowledge(&closed).unwrap();
    journal.acknowledge(&snapshot).unwrap();
    assert_eq!(journal.published_revision(), closed.revision);
    assert!(matches!(
        SessionJournal::default().acknowledge(&closed),
        Err(JournalError::StaleCheckpoint)
    ));
    gate.release(1);
    smol::block_on(manager.shutdown(Duration::from_secs(1)));
}

#[test]
fn journal_batch_validation_is_atomic_and_projection_rejects_sequence_collisions() {
    let journal = SessionJournal::default();
    let before = journal.snapshot();
    assert!(
        journal
            .commit(JournalCommit {
                changes: vec![JournalChange {
                    agent: None,
                    generation: 0,
                    change: StateChange::Compaction { epoch: 1 }
                }],
                ..Default::default()
            })
            .is_err()
    );
    assert_eq!(journal.snapshot().revision, before.revision);
    assert_eq!(journal.snapshot().seq, before.seq);
    journal
        .commit(JournalCommit {
            changes: vec![JournalChange {
                agent: None,
                generation: 0,
                change: StateChange::Rules {
                    rules: Vec::new(),
                    yolo: Some(true),
                },
            }],
            ..Default::default()
        })
        .unwrap();
    let mut session = Session::new("test/model", ".");
    session.push_state(makima_storage::session_state::StoredStateRecord {
        seq: journal.snapshot().seq,
        agent: None,
        generation: 0,
        change: StateChange::Rules {
            rules: Vec::new(),
            yolo: Some(false),
        },
    });
    assert!(matches!(
        journal.snapshot().apply_to(&mut session),
        Err(JournalError::Diverged(_))
    ));
    assert!(matches!(
        &session.state()[0].change,
        StateChange::Rules {
            yolo: Some(false),
            ..
        }
    ));
}

#[test]
fn failed_root_factory_leaves_no_durable_agent_or_history() {
    const FAILURE: &str = "injected root failure";
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let id = crate::AgentId::generate();
    assert!(matches!(manager.create_root_with_id_and_config(
        id, None, Vec::new(), None, |_| Err::<Box<dyn ActorBackend>, _>(FAILURE),
    ), Err(ManagerError::Factory(message)) if message == FAILURE));
    assert!(manager.journal().snapshot().agent_revisions.is_empty());
    assert!(manager.journal().snapshot().histories.is_empty());
    manager
        .create_root_with_id(id, Vec::new(), None, TestBackend::boxed())
        .unwrap();
    smol::block_on(manager.shutdown(Duration::from_secs(1)));
}

#[test]
fn unresolved_root_config_fails_without_publishing_agent_or_history() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let id = crate::AgentId::generate();
    let mut unresolved = config("test/model", false);
    unresolved.mode = AgentMode::Custom(crate::ModeId::parse("unresolved"));
    unresolved.mode_def = None;
    let expected = crate::session_checkpoint::stored_agent_config(&unresolved, Default::default())
        .unwrap_err()
        .to_string();
    let factory_called = AtomicUsize::new(0);
    let error = manager
        .create_root_with_id_and_config(id, Some(unresolved), Vec::new(), None, |_| {
            factory_called.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(TestBackend::boxed())
        })
        .unwrap_err();
    assert_eq!(error, ManagerError::Policy(expected));
    assert_eq!(factory_called.load(Ordering::SeqCst), 0);
    assert!(manager.snapshot().is_empty());
    let journal = manager.journal().snapshot();
    assert!(journal.agent_revisions.is_empty());
    assert!(journal.histories.is_empty());
    assert!(journal.state.is_empty());
    manager
        .create_root_with_id(id, Vec::new(), None, TestBackend::boxed())
        .unwrap();
    smol::block_on(manager.shutdown(Duration::from_secs(1)));
}

#[test]
fn duplicate_root_is_rejected_without_mutating_graph() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root(Vec::new(), None, TestBackend::boxed())
        .unwrap();

    let error = manager
        .create_root(Vec::new(), None, TestBackend::boxed())
        .unwrap_err();

    assert_eq!(error, ManagerError::DuplicateRoot);
    assert_eq!(manager.snapshot().len(), 1);
    assert_eq!(manager.root_id().unwrap(), root.id());
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn wrong_manager_capability_is_rejected() {
    let (manager, _, current, gate) = active_root(AgentLimits::default());
    let other = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let error = other
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap_err();
    assert_eq!(error, ManagerError::WrongManager);
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn retained_context_clone_cannot_spawn_after_guard_drop() {
    let (manager, _, current, gate) = active_root(AgentLimits::default());
    gate.release(1);
    smol::block_on(async {
        loop {
            let active = manager
                .lock_graph()
                .active_turns
                .contains_key(&(current.agent_id(), current.turn_id()));
            if !active {
                break;
            }
            smol::future::yield_now().await;
        }
    });
    let error = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap_err();
    assert!(matches!(error, ManagerError::InactiveTurn { .. }));
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn factory_failure_rolls_back_reservation() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let error = manager
        .create_root_with(Vec::new(), None, |_| {
            Err::<Box<dyn ActorBackend>, _>("boom")
        })
        .unwrap_err();
    assert_eq!(error, ManagerError::Factory("boom".into()));
    assert!(manager.snapshot().is_empty());
    manager
        .create_root(Vec::new(), None, TestBackend::boxed())
        .unwrap();
}

#[test]
fn panicking_root_factory_rolls_back_reservation() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let error = manager
        .create_root_with(
            Vec::new(),
            None,
            |_| -> Result<Box<dyn ActorBackend>, String> { panic!("factory panic") },
        )
        .unwrap_err();

    assert_eq!(
        error,
        ManagerError::Factory("agent factory panicked".into())
    );
    assert!(manager.snapshot().is_empty());
    manager
        .create_root(Vec::new(), None, TestBackend::boxed())
        .unwrap();
}

#[test]
fn panicking_child_factory_restores_capacity() {
    let limits = AgentLimits {
        max_children_per_agent: 1,
        max_retained_agents: 2,
        ..AgentLimits::default()
    };
    let (manager, root, current, gate) = active_root(limits);
    let error = manager
        .spawn_child_with(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            |_| -> Result<Box<dyn ActorBackend>, String> { panic!("factory panic") },
        )
        .unwrap_err();

    assert_eq!(
        error,
        ManagerError::Factory("agent factory panicked".into())
    );
    assert_eq!(manager.snapshot().len(), 1);
    assert!(root.snapshot().unwrap().children.is_empty());
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    assert_eq!(manager.snapshot().len(), 2);

    child.close_subtree().unwrap();
    drop(current);
    drop(gate);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn shutdown_does_not_join_reserved_root_before_factory_completes() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let creating = manager.clone();
    let (reserved_tx, reserved_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let factory = std::thread::spawn(move || {
        creating.create_root_with(Vec::new(), None, |agent_id| {
            reserved_tx.send(agent_id).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, String>(TestBackend::boxed())
        })
    });
    let root_id = reserved_rx.recv().unwrap();
    assert!(!manager.runner_finished(root_id).unwrap());

    let report = smol::block_on(manager.shutdown(std::time::Duration::ZERO));
    assert!(report.joined.is_empty());
    assert_eq!(report.timed_out, vec![root_id]);

    release_tx.send(()).unwrap();
    assert!(matches!(
        factory.join().unwrap(),
        Err(ManagerError::GraphShutdown)
    ));
    smol::block_on(manager.wait_until_reaped());
    assert!(manager.runner_finished(root_id).unwrap());
}

#[test]
fn close_owns_runner_when_reserved_root_factory_succeeds() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let creating = manager.clone();
    let (reserved_tx, reserved_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let factory = std::thread::spawn(move || {
        creating.create_root_with(Vec::new(), None, |agent_id| {
            reserved_tx.send(agent_id).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, String>(TestBackend::boxed())
        })
    });
    let root_id = reserved_rx.recv().unwrap();

    manager.close_subtree(root_id).unwrap();
    release_tx.send(()).unwrap();
    assert!(matches!(
        factory.join().unwrap(),
        Err(ManagerError::NonLiveAgent(id)) if id == root_id
    ));
    assert!(manager.lookup(root_id).is_err());
    smol::block_on(async {
        while !manager.runner_finished(root_id).unwrap() {
            smol::future::yield_now().await;
        }
    });
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.joined.is_empty());
    assert!(report.timed_out.is_empty());
    assert!(manager.runner_finished(root_id).unwrap());
    assert!(matches!(manager.node(root_id), Err(ManagerError::UnknownAgent(id)) if id == root_id));
}

#[test]
fn shutdown_excludes_finished_child_runner_and_preserves_retained_node() {
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    child.close_subtree().unwrap();
    smol::block_on(async {
        while !manager.runner_finished(child.id()).unwrap() {
            smol::future::yield_now().await;
        }
    });
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
    assert_eq!(manager.lookup(child.id()).unwrap().id(), child.id());
    assert_eq!(root.snapshot().unwrap().children, vec![child.id()]);
    assert!(manager.lock_graph().nodes[&child.id()].task.is_none());
    gate.release(1);

    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));

    assert_eq!(report.joined, vec![root.id()]);
    assert!(report.timed_out.is_empty());
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
    assert_eq!(manager.lookup(child.id()).unwrap().id(), child.id());
    assert_eq!(root.snapshot().unwrap().children, vec![child.id()]);
    assert!(manager.lock_graph().nodes[&child.id()].task.is_none());
}

#[test]
fn close_and_factory_failure_remove_never_committed_root() {
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let creating = manager.clone();
    let (reserved_tx, reserved_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let factory = std::thread::spawn(move || {
        creating.create_root_with(Vec::new(), None, |agent_id| {
            reserved_tx.send(agent_id).unwrap();
            release_rx.recv().unwrap();
            Err::<Box<dyn ActorBackend>, _>("factory failed")
        })
    });
    let root_id = reserved_rx.recv().unwrap();

    manager.close_subtree(root_id).unwrap();
    release_tx.send(()).unwrap();
    assert!(matches!(
        factory.join().unwrap(),
        Err(ManagerError::Factory(_))
    ));
    assert!(matches!(manager.root_id(), Err(ManagerError::MissingRoot)));
    assert!(manager.snapshot().is_empty());
}

#[test]
fn idle_spawn_ignores_active_and_child_limits_but_enforces_depth() {
    let limits = AgentLimits {
        max_agent_depth: 1,
        max_children_per_agent: 1,
        max_active_agents: 2,
        ..AgentLimits::default()
    };
    let (manager, root, current, gate) = active_root(limits);
    let child_gate = Gate::new();
    let (entered, contexts) = flume::bounded(1);
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::reporting(entered, Some(Arc::clone(&child_gate))),
        )
        .unwrap();
    child
        .actor()
        .unwrap()
        .admit_turn(input(), None, "child".into())
        .unwrap();
    let child_current = smol::block_on(contexts.recv_async()).unwrap();
    let idle = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    assert_eq!(
        root.snapshot().unwrap().children,
        vec![child.id(), idle.id()]
    );
    let error = manager
        .spawn_child(
            &child_current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap_err();
    assert_eq!(error, ManagerError::DepthExceeded { depth: 2, max: 1 });
    assert_eq!(manager.snapshot().len(), 3);
    child_gate.release(1);
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test_case(33, None; "thirty_three_completed_children")]
#[test_case(65, None; "sixty_five_completed_children")]
#[test_case(SMALL_RETAINED_QUOTA - 1, Some(SMALL_RETAINED_QUOTA); "full_retained_quota")]
fn completed_children_preserve_early_open_actor(task_count: usize, retained_quota: Option<usize>) {
    let mut limits = AgentLimits::default();
    if let Some(quota) = retained_quota {
        limits.max_retained_agents = quota;
    }
    let (manager, root, current, gate) = active_root(limits);
    smol::block_on(async {
        let mut children = Vec::new();
        for _ in 0..task_count {
            let child = current
                .spawn_child(
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    TestBackend::boxed(),
                )
                .unwrap();
            let actor = child.actor().unwrap();
            let ticket = actor.admit_turn(input(), None, String::new()).unwrap();
            assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
            children.push((child, actor.identity()));
            graph_quiescence_barrier(&[actor]).await.unwrap();
            assert_eq!(manager.snapshot().len(), children.len() + 1);
            assert_eq!(manager.active_agent_count(), 1);
            assert_eq!(manager.active_child_count(root.id()), 0);
        }
        let (early, identity) = &children[0];
        let actor = early.actor().unwrap();
        assert!(Arc::ptr_eq(identity, &actor.identity()));
        let ticket = actor.admit_turn(input(), None, String::new()).unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        graph_quiescence_barrier(&[actor]).await.unwrap();
        assert!(Arc::ptr_eq(identity, &early.actor().unwrap().identity()));
        assert_eq!(manager.snapshot().len(), task_count + 1);
        assert_eq!(manager.active_agent_count(), 1);
        assert_eq!(manager.active_child_count(root.id()), 0);

        if let Some(quota) = retained_quota {
            let spawn = || {
                current.spawn_child(
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    TestBackend::boxed(),
                )
            };
            assert_eq!(
                spawn().unwrap_err(),
                ManagerError::RetentionLimit { max: quota }
            );
            let (closed, _) = children.last().unwrap();
            closed.close_subtree().unwrap();
            yield_until(|| manager.runner_finished(closed.id()).unwrap()).await;
            assert_eq!(
                closed.snapshot().unwrap().graph_lifecycle,
                GraphLifecycle::Closed
            );
            assert_eq!(
                spawn().unwrap_err(),
                ManagerError::RetentionLimit { max: quota }
            );
            manager.retire(&current, closed.id()).unwrap();
            let replacement = spawn().unwrap();
            assert_ne!(replacement.id(), closed.id());
            assert_eq!(manager.snapshot().len(), quota + 1);
        }
        gate.release(1);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn retained_agent_limit_is_atomic() {
    let limits = AgentLimits {
        max_retained_agents: 2,
        ..AgentLimits::default()
    };
    let (manager, _, current, gate) = active_root(limits);
    manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();

    let error = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap_err();

    assert_eq!(error, ManagerError::RetentionLimit { max: 2 });
    assert_eq!(manager.snapshot().len(), 2);
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn closing_idle_child_retains_identity_quota_until_retired() {
    let limits = AgentLimits {
        max_retained_agents: 2,
        ..AgentLimits::default()
    };
    let (manager, root, current, gate) = active_root(limits);
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    manager.close_subtree(child.id()).unwrap();
    smol::block_on(async {
        while !manager.runner_finished(child.id()).unwrap() {
            smol::future::yield_now().await;
        }
    });
    assert!(matches!(
        manager.spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        ),
        Err(ManagerError::RetentionLimit { max: 2 })
    ));
    assert_eq!(root.snapshot().unwrap().children, vec![child.id()]);
    manager.retire(&current, child.id()).unwrap();
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Retired
    );
    let replacement = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    assert_ne!(child.id(), replacement.id());
    assert_eq!(
        root.snapshot().unwrap().children,
        vec![child.id(), replacement.id()]
    );
    assert_eq!(manager.snapshot().len(), 3);
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Retired
    );
    assert_eq!(
        root.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Live
    );
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn closing_descendants_does_not_resurrect_closed_tombstones() {
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();
    child.close_subtree().unwrap();
    smol::block_on(async {
        while !manager.runner_finished(child.id()).unwrap() {
            smol::future::yield_now().await;
        }
    });
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
    let revision = manager.lock_graph().revision;

    manager.close_descendants(root.id()).unwrap();

    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
    assert_eq!(manager.lock_graph().revision, revision);
    gate.release(1);
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
}

#[test]
fn direct_actor_close_retains_identity_quota_when_runner_finishes() {
    let limits = AgentLimits {
        max_retained_agents: 2,
        ..AgentLimits::default()
    };
    let (manager, _, current, gate) = active_root(limits);
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();

    child.actor().unwrap().close();
    smol::block_on(async {
        while !manager.runner_finished(child.id()).unwrap() {
            smol::future::yield_now().await;
        }
    });
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
    assert!(matches!(
        manager.spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        ),
        Err(ManagerError::RetentionLimit { max: 2 })
    ));
    assert_eq!(manager.lookup(child.id()).unwrap().id(), child.id());

    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn closing_active_child_retains_identity_quota_after_runner_finishes() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_retained_agents: 2,
            ..AgentLimits::default()
        };
        let (manager, _, current, root_gate) = active_root(limits);
        let child_gate = Gate::new();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(entered_tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let ticket = child
            .actor()
            .unwrap()
            .admit_turn(input(), None, "active".into())
            .unwrap();
        entered_rx.recv_async().await.unwrap();

        let actor = child.actor().unwrap();
        let events = actor.subscribe_events();
        child.close_subtree().unwrap();
        assert!(ticket.peek_result().is_none());
        assert!(events.is_empty());
        assert_eq!(
            child.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Closing
        );
        assert!(matches!(
            manager.spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            ),
            Err(ManagerError::RetentionLimit { max: 2 })
        ));

        child_gate.release(1);
        let outcome = ticket.wait().await;
        while !manager.runner_finished(child.id()).unwrap() {
            smol::future::yield_now().await;
        }
        assert!(
            matches!(events.try_recv().unwrap(), crate::ActorEvent::End(result) if result.outcome == outcome)
        );
        assert!(matches!(
            events.try_recv().unwrap(),
            crate::ActorEvent::Close { .. }
        ));
        assert!(events.is_empty());
        assert!(matches!(
            manager.spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            ),
            Err(ManagerError::RetentionLimit { max: 2 })
        ));
        assert_eq!(
            child.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
        assert_eq!(
            manager
                .lookup(child.id())
                .unwrap()
                .turn_ticket(ticket.turn_id())
                .unwrap()
                .peek(),
            Some(outcome)
        );

        root_gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_child_cancelled_while_waiting_for_permit_never_enters_backend() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        };
        let (manager, _, current, root_gate) = active_root(limits);
        let (acquire_entered_tx, acquire_entered_rx) = flume::bounded(1);
        let (acquire_release_tx, acquire_release_rx) = flume::bounded(1);
        manager.set_managed_acquire_gate(acquire_entered_tx, acquire_release_rx);
        let (backend_entered_tx, backend_entered_rx) = flume::bounded(1);
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                Box::new(CancellableBackend {
                    entered: backend_entered_tx,
                }),
            )
            .unwrap();
        let actor = child.actor().unwrap();
        let (event_tx, event_rx) = flume::unbounded();
        let ticket = actor
            .admit_turn(
                input(),
                Some(EventSender::new(event_tx)),
                "waiting-for-permit".into(),
            )
            .unwrap();
        let turn_id = ticket.turn_id();

        acquire_entered_rx.recv_async().await.unwrap();
        manager.cancel_agent(child.id()).unwrap();
        acquire_release_tx.send(()).unwrap();
        let outcome = ticket.wait().await;

        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: crate::TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(actor.outcome(turn_id), Some(outcome));
        let event = event_rx
            .try_recv()
            .expect("ticket resolved before cancellation delivery");
        assert!(matches!(
            event.event,
            AgentEvent::TurnOutcome(TurnOutcome::Cancelled { .. })
        ));
        assert!(
            event_rx.is_empty(),
            "cancelled outcome delivered more than once"
        );
        assert!(backend_entered_rx.try_recv().is_err());
        root_gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn cancel_agent_is_reusable_and_isolates_siblings() {
    smol::block_on(async {
        let (manager, _, current, root_gate) = active_root(AgentLimits::default());
        let (first_tx, first_rx) = flume::bounded(2);
        let first = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                Box::new(CancellableBackend { entered: first_tx }),
            )
            .unwrap();
        let (second_tx, second_rx) = flume::bounded(1);
        let second = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                Box::new(CancellableBackend { entered: second_tx }),
            )
            .unwrap();
        let first_ticket = first
            .actor()
            .unwrap()
            .admit_turn(input(), None, "first".into())
            .unwrap();
        let second_ticket = second
            .actor()
            .unwrap()
            .admit_turn(input(), None, "second".into())
            .unwrap();
        first_rx.recv_async().await.unwrap();
        second_rx.recv_async().await.unwrap();

        manager.cancel_agent(first.id()).unwrap();
        assert!(matches!(
            first_ticket.wait().await,
            TurnOutcome::Cancelled {
                reason: crate::TurnCancellationReason::User,
                ..
            }
        ));
        let mut second_wait = Box::pin(second_ticket.wait());
        assert!(
            futures_lite::future::poll_once(&mut second_wait)
                .await
                .is_none()
        );

        let reused = first
            .actor()
            .unwrap()
            .admit_turn(input(), None, "reused".into())
            .unwrap();
        first_rx.recv_async().await.unwrap();
        manager.cancel_agent(first.id()).unwrap();
        assert!(matches!(reused.wait().await, TurnOutcome::Cancelled { .. }));

        manager.cancel_agent(second.id()).unwrap();
        assert!(matches!(second_wait.await, TurnOutcome::Cancelled { .. }));
        root_gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn cancel_subtree_marks_reserved_descendant_and_preserves_reuse() {
    let (manager, root, current, root_gate) = active_root(AgentLimits::default());
    let creating = manager.clone();
    let child_current = current.clone();
    let (reserved_tx, reserved_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let factory = std::thread::spawn(move || {
        creating.spawn_child_with(
            &child_current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            |agent_id| {
                reserved_tx.send(agent_id).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, String>(TestBackend::boxed())
            },
        )
    });
    let child_id = reserved_rx.recv().unwrap();
    let revision = manager.lock_graph().revision;

    manager.cancel_subtree(root.id()).unwrap();
    let graph = manager.lock_graph();
    assert!(graph.nodes[&child_id].cancel_on_commit);
    assert_eq!(graph.revision, revision + 1);
    drop(graph);
    release_tx.send(()).unwrap();
    let child = factory.join().unwrap().unwrap();
    assert!(!manager.lock_graph().nodes[&child_id].cancel_on_commit);
    let ticket = child
        .actor()
        .unwrap()
        .admit_turn(input(), None, "after-cut".into())
        .unwrap();
    assert!(matches!(
        smol::block_on(ticket.wait()),
        TurnOutcome::Completed { .. }
    ));

    root_gate.release(1);
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
}

#[test]
fn root_snapshot_does_not_block_atomic_correlation_cancel_cut() {
    let (manager, root, current, root_gate) = active_root(AgentLimits::default());
    let root_actor = root.actor().unwrap();
    let (snapshot_entered, snapshot_release) = root_actor.pause_before_next_snapshot_state();
    let (snapshot_done_tx, snapshot_done_rx) = flume::bounded(1);
    let snapshot_manager = manager.clone();
    let root_id = root.id();
    let snapshot = std::thread::spawn(move || {
        let result = snapshot_manager.node(root_id);
        snapshot_done_tx.send(()).unwrap();
        result
    });
    snapshot_entered.recv().unwrap();

    let (cut_entered_tx, cut_entered_rx) = flume::bounded(1);
    let (cut_release_tx, cut_release_rx) = flume::bounded(1);
    manager.set_descendant_cut_gate(cut_entered_tx, cut_release_rx);
    let (cancel_done_tx, cancel_done_rx) = flume::bounded(1);
    let cancel_manager = manager.clone();
    let cancel = std::thread::spawn(move || {
        cancel_manager
            .cancel_correlation(root_id, "root", crate::TurnCancellationReason::User)
            .unwrap();
        cancel_done_tx.send(()).unwrap();
    });

    let cut_entered = cut_entered_rx.recv_timeout(COMPLETION_TIMEOUT);
    let spawn = if cut_entered.is_ok() {
        Some(manager.spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        ))
    } else {
        None
    };
    cut_release_tx.send(()).unwrap();
    snapshot_release.send(()).unwrap();
    let cancel_done = cancel_done_rx.recv_timeout(COMPLETION_TIMEOUT);
    let snapshot_done = snapshot_done_rx.recv_timeout(COMPLETION_TIMEOUT);
    cut_entered.unwrap();
    cancel_done.unwrap();
    snapshot_done.unwrap();
    assert!(matches!(
        spawn.unwrap(),
        Err(ManagerError::InactiveTurn { agent_id, turn_id })
            if agent_id == root.id() && turn_id == current.turn_id()
    ));

    cancel.join().unwrap();
    let snapshot = snapshot.join().unwrap().unwrap();
    assert_eq!(snapshot.agent_id, root.id());
    assert_eq!(snapshot.graph_lifecycle, GraphLifecycle::Live);
    assert!(snapshot.actor.is_some());

    root_gate.release(1);
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
}

#[test]
fn correlation_cancel_cut_rejects_post_cut_spawn_and_preserves_later_turn() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (current_tx, current_rx) = flume::unbounded();
        let root_gate = Gate::new();
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| {
                    Ok::<_, String>(TestBackend::reporting(
                        current_tx,
                        Some(Arc::clone(&root_gate)),
                    ))
                },
            )
            .unwrap();
        let root_actor = root.actor().unwrap();
        let first = root_actor
            .admit_turn(input(), None, "first".into())
            .unwrap();
        let first_current = current_rx.recv_async().await.unwrap();
        let (cut_tx, cut_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        manager.set_descendant_cut_gate(cut_tx, release_rx);
        let cutting = manager.clone();
        let root_id = root.id();
        let turn_id = first_current.turn_id();
        let cut = std::thread::spawn(move || {
            cutting.cancel_correlation(root_id, "first", crate::TurnCancellationReason::User)
        });

        cut_rx.recv().unwrap();
        assert!(matches!(
            manager.spawn_child(
                &first_current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            ),
            Err(ManagerError::InactiveTurn { agent_id, turn_id: rejected })
                if agent_id == root.id() && rejected == turn_id
        ));
        release_tx.send(()).unwrap();
        cut.join().unwrap().unwrap();
        assert!(root.snapshot().unwrap().children.is_empty());

        root_gate.release(1);
        first.wait().await;
        let later = root_actor
            .admit_turn(input(), None, "later".into())
            .unwrap();
        let later_current = current_rx.recv_async().await.unwrap();
        let child = manager
            .spawn_child(
                &later_current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        assert_eq!(child.snapshot().unwrap().parent_id, Some(root.id()));
        root_gate.release(1);
        assert!(matches!(later.wait().await, TurnOutcome::Completed { .. }));

        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn close_descendants_rejects_reserved_child_and_preserves_root() {
    let (manager, root, current, root_gate) = active_root(AgentLimits::default());
    let creating = manager.clone();
    let child_current = current.clone();
    let (reserved_tx, reserved_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let factory = std::thread::spawn(move || {
        creating.spawn_child_with(
            &child_current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            |agent_id| {
                reserved_tx.send(agent_id).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, String>(TestBackend::boxed())
            },
        )
    });
    let child_id = reserved_rx.recv().unwrap();

    manager.close_descendants(root.id()).unwrap();
    assert_eq!(
        manager.node(root.id()).unwrap().graph_lifecycle,
        GraphLifecycle::Live
    );
    assert_eq!(
        manager.node(child_id).unwrap().graph_lifecycle,
        GraphLifecycle::Closing
    );
    release_tx.send(()).unwrap();
    assert!(matches!(
        factory.join().unwrap(),
        Err(ManagerError::NonLiveAgent(id)) if id == child_id
    ));
    assert!(root.actor().is_ok());

    root_gate.release(1);
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
}

#[test]
fn reserved_cancellation_precedes_live_publication() {
    let (manager, root, current, root_gate) = active_root(AgentLimits::default());
    let creating = manager.clone();
    let child_current = current.clone();
    let (reserved_tx, reserved_rx) = flume::bounded(1);
    let (factory_release_tx, factory_release_rx) = flume::bounded(1);
    let factory = std::thread::spawn(move || {
        creating.spawn_child_with(
            &child_current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            |agent_id| {
                reserved_tx.send(agent_id).unwrap();
                factory_release_rx.recv().unwrap();
                Ok::<_, String>(TestBackend::boxed())
            },
        )
    });
    let child_id = reserved_rx.recv().unwrap();

    manager.cancel_subtree(root.id()).unwrap();
    let (commit_entered_tx, commit_entered_rx) = flume::bounded(1);
    let (commit_release_tx, commit_release_rx) = flume::bounded(1);
    manager.set_commit_gate(commit_entered_tx, commit_release_rx);
    let observing = manager.clone();
    let observer = std::thread::spawn(move || {
        loop {
            match observing.actor(child_id) {
                Ok(actor) => {
                    break actor
                        .admit_turn(input(), None, "after-publication".into())
                        .unwrap();
                }
                Err(ManagerError::NonLiveAgent(id)) if id == child_id => {
                    std::thread::yield_now();
                }
                Err(error) => panic!("unexpected actor lookup error: {error}"),
            }
        }
    });

    factory_release_tx.send(()).unwrap();
    commit_entered_rx.recv().unwrap();
    assert!(matches!(
        manager.actor(child_id),
        Err(ManagerError::NonLiveAgent(id)) if id == child_id
    ));
    assert!(manager.journal().agent(child_id).is_none());
    commit_release_tx.send(()).unwrap();
    let child = factory.join().unwrap().unwrap();
    assert_eq!(
        manager.journal().agent(child_id).unwrap().parent,
        Some(root.id())
    );
    let ticket = observer.join().unwrap();
    assert_eq!(child.id(), child_id);
    assert!(matches!(
        smol::block_on(ticket.wait()),
        TurnOutcome::Completed { .. }
    ));

    root_gate.release(1);
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
}

#[test]
fn descendant_preflight_rejects_before_suspension() {
    let (manager, root, current, gate) = active_root(AgentLimits::default());
    let other = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let other_root = other
        .create_root(Vec::new(), None, TestBackend::boxed())
        .unwrap();

    assert!(matches!(
        current.validate_descendant(root.id()),
        Err(ManagerError::NotDescendant { .. })
    ));
    assert!(matches!(
        current.validate_descendant(other_root.id()),
        Err(ManagerError::UnknownAgent(_))
    ));
    let state = current.lease.inner.state.lock().unwrap();
    assert_eq!(state.suspensions, 0);
    assert!(state.watcher_cancels.is_empty());
    drop(state);

    gate.release(1);
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
    let report = smol::block_on(other.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
}

#[test]
fn unauthorized_descendant_wait_does_not_cancel_child_turn() {
    smol::block_on(async {
        let (manager, _, current, root_gate) = active_root(AgentLimits::default());
        let child_gate = Gate::new();
        let (child_tx, child_rx) = flume::bounded(1);
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(child_tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let actor = child.actor().unwrap();
        let ticket = actor.admit_turn(input(), None, "child".into()).unwrap();
        child_rx.recv_async().await.unwrap();
        let (wrong_manager, _, wrong_current, wrong_gate) = active_root(AgentLimits::default());

        let Err(error) = wrong_current.lease().observe_descendant(
            &wrong_current,
            child.id(),
            &actor,
            ticket.clone(),
            None,
        ) else {
            panic!("wrong manager accepted descendant wait");
        };
        assert_eq!(error, ManagerError::UnknownAgent(child.id()));
        let mut pending = Box::pin(ticket.wait());
        assert!(
            futures_lite::future::poll_once(&mut pending)
                .await
                .is_none()
        );

        child_gate.release(1);
        assert!(matches!(pending.await, TurnOutcome::Completed { .. }));
        root_gate.release(1);
        wrong_gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
        let report = wrong_manager
            .shutdown(std::time::Duration::from_secs(1))
            .await;
        assert!(report.timed_out.is_empty());
    });
}

#[test_case(false; "parent_actor")]
#[test_case(true; "foreign_manager_actor")]
fn descendant_wait_rejects_mismatched_actor_without_suspending(foreign: bool) {
    smol::block_on(async {
        const CORRELATION: &str = "child";
        let (manager, root, current, root_gate) = active_root(AgentLimits::default());
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let child_actor = child.actor().unwrap();
        let foreign_root = foreign.then(|| active_root(AgentLimits::default()));
        let wrong_actor = foreign_root
            .as_ref()
            .map_or(&root, |(_, root, _, _)| root)
            .actor()
            .unwrap();
        let ticket = child_actor
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();
        let error = current
            .lease()
            .observe_descendant(&current, child.id(), &wrong_actor, ticket.clone(), None)
            .err()
            .unwrap();
        assert_eq!(
            error,
            ManagerError::ActorMismatch {
                expected_id: child.id(),
                actual_id: wrong_actor.agent_id(),
            }
        );
        assert_eq!(current.lease.inner.state.lock().unwrap().suspensions, 0);
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        root_gate.release(1);
        if let Some((_, _, _, gate)) = &foreign_root {
            gate.release(1);
        }
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
        if let Some((manager, _, _, _)) = foreign_root {
            assert!(
                manager
                    .shutdown(COMPLETION_TIMEOUT)
                    .await
                    .timed_out
                    .is_empty()
            );
        }
    });
}

#[test]
fn descendant_wait_rejects_ticket_from_another_actor_without_suspending() {
    smol::block_on(async {
        let (manager, _, current, root_gate) = active_root(AgentLimits::default());
        let first = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();
        let second_gate = Gate::new();
        let second = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                Box::new(TestBackend {
                    current: None,
                    cancel: None,
                    gate: Some(Arc::clone(&second_gate)),
                }),
            )
            .unwrap();
        let ticket = second
            .actor()
            .unwrap()
            .admit_turn(input(), None, "second".into())
            .unwrap();

        let error = current
            .lease()
            .observe_descendant(
                &current,
                first.id(),
                &first.actor().unwrap(),
                ticket.clone(),
                None,
            )
            .err()
            .unwrap();
        assert!(matches!(error, ManagerError::TicketActorMismatch { .. }));
        assert_eq!(current.lease.inner.state.lock().unwrap().suspensions, 0);
        let mut pending = Box::pin(ticket.wait());
        assert!(
            futures_lite::future::poll_once(&mut pending)
                .await
                .is_none()
        );

        second_gate.release(1);
        assert!(matches!(pending.await, TurnOutcome::Completed { .. }));
        root_gate.release(1);
        assert!(
            manager
                .shutdown(std::time::Duration::from_secs(1))
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false; "discard_accepted_before_start")]
#[test_case(true; "reject_closed_parent")]
fn real_nested_child_completion_targets_recorded_parent(closed: bool) {
    smol::block_on(async {
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        const CORRELATION: &str = "nested child";
        let (manager, root, current, root_gate) = active_root(AgentLimits::default());
        let parent = manager
            .spawn_child_trusted_with_config(
                &root,
                config(MODEL, false),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::boxed()),
            )
            .unwrap();
        let parent_actor = parent.actor().unwrap();
        let barrier = parent_actor.reserve_prepared_operation(None).unwrap();
        let parent_events = parent_actor.subscribe_events();
        let root_events = root.actor().unwrap().subscribe_events();
        let child = manager
            .spawn_child_trusted_with_config(
                &parent,
                config(MODEL, false),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::boxed()),
            )
            .unwrap();
        let child_actor = child.actor().unwrap();
        child_actor.configure_parent_delivery(true).unwrap();
        let child_events = child_actor.subscribe_events();
        if closed {
            parent_actor.close();
        }
        let ticket = child_actor
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();
        ticket.wait().await;
        if closed {
            let event = loop {
                if let crate::ActorEvent::Delivery(event) = child_events.recv_async().await.unwrap()
                {
                    break event;
                }
            };
            assert_eq!(event.delivery.result.outcome.agent_id(), child.id());
            assert_eq!(event.delivery.result.outcome.turn_id(), ticket.turn_id());
            assert_eq!(
                event.status,
                crate::actor::DeliveryStatus::Rejected(
                    crate::actor::DeliveryRejection::ParentClosed
                )
            );
        } else {
            let event = loop {
                if let crate::ActorEvent::Delivery(event) =
                    parent_events.recv_async().await.unwrap()
                {
                    break event;
                }
            };
            assert_eq!(event.delivery.result.outcome.agent_id(), child.id());
            assert_eq!(event.delivery.result.outcome.turn_id(), ticket.turn_id());
            let crate::actor::DeliveryStatus::Accepted = event.status else {
                panic!("delivery was not admitted")
            };
            parent_actor.clear();
            let event = loop {
                if let crate::ActorEvent::Delivery(event) =
                    parent_events.recv_async().await.unwrap()
                {
                    break event;
                }
            };
            assert_eq!(
                event.status,
                crate::actor::DeliveryStatus::Discarded(crate::TurnCancellationReason::User)
            );
        }
        assert!(
            root_events
                .try_iter()
                .all(|event| !matches!(event, crate::ActorEvent::Delivery(_)))
        );
        drop(barrier);
        current.validate_active().unwrap();
        root_gate.release(1);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn real_child_settlement_reports_parent_delivery_overflow() {
    smol::block_on(async {
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        const CHILD: &str = "overflow child";
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let parent = root.actor().unwrap();
        let barrier = parent.reserve_prepared_operation(None).unwrap();
        let parent_events = parent.subscribe_events();
        let child = manager
            .spawn_child_trusted_with_config(
                &root,
                config(MODEL, false),
                AgentMetadata::default(),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::boxed()),
            )
            .unwrap();
        let actor = child.actor().unwrap();
        actor.configure_parent_delivery(true).unwrap();
        let child_events = actor.subscribe_events();
        let mut pending_turns = Vec::new();
        for _ in 0..crate::actor::delivery::MAX_PENDING_DELIVERIES {
            let ticket = actor.admit_turn(input(), None, CHILD.into()).unwrap();
            ticket.wait().await;
            let event = loop {
                if let crate::ActorEvent::Delivery(event) =
                    parent_events.recv_async().await.unwrap()
                {
                    break event;
                }
            };
            assert!(matches!(
                event.status,
                crate::actor::DeliveryStatus::Accepted
            ));
            assert_eq!(event.delivery.result.outcome.turn_id(), ticket.turn_id());
            pending_turns.push(ticket.turn_id());
            assert_eq!(parent.snapshot().queued, 0);
            assert!(parent.latest_ticket().is_none());
        }
        let rejected = actor.admit_turn(input(), None, CHILD.into()).unwrap();
        rejected.wait().await;
        let event = loop {
            if let crate::ActorEvent::Delivery(event) = child_events.recv_async().await.unwrap() {
                break event;
            }
        };
        assert_eq!(
            event.status,
            crate::actor::DeliveryStatus::Rejected(crate::actor::DeliveryRejection::Overflow)
        );
        assert_eq!(event.delivery.result.outcome.turn_id(), rejected.turn_id());
        assert_eq!(parent.snapshot().queued, 0);
        assert!(parent.latest_ticket().is_none());
        assert!(
            parent_events
                .try_iter()
                .all(|event| !matches!(event, crate::ActorEvent::Delivery(_)))
        );
        parent.clear();
        let discarded: Vec<_> = parent_events
            .try_iter()
            .filter_map(|event| match event {
                crate::ActorEvent::Delivery(event) => {
                    assert_eq!(
                        event.status,
                        crate::actor::DeliveryStatus::Discarded(
                            crate::TurnCancellationReason::User
                        )
                    );
                    Some(event.delivery.result.outcome.turn_id())
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            discarded.len(),
            crate::actor::delivery::MAX_PENDING_DELIVERIES
        );
        for turn_id in pending_turns {
            assert!(discarded.contains(&turn_id));
            assert!(!parent.withdraw_child_delivery(turn_id));
        }
        assert!(!discarded.contains(&rejected.turn_id()));
        assert_eq!(parent.snapshot().queued, 0);
        assert!(parent.latest_ticket().is_none());
        drop(barrier);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test]
fn managed_delivery_wait_capacity_one_drains_into_active_parent_once() {
    smol::block_on(async {
        const CHILD: &str = "delivery child";
        const USER: &str = "queued user";
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..Default::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let root_gate = Gate::new();
        let (root_tx, root_rx) = flume::unbounded();
        let root = manager
            .create_root_with_config(Some(config(MODEL, false)), Vec::new(), None, |_| {
                Ok::<_, String>(TestBackend::reporting(
                    root_tx,
                    Some(Arc::clone(&root_gate)),
                ))
            })
            .unwrap();
        let parent = root.actor().unwrap();
        let root_ticket = parent.admit_turn(input(), None, "root".into()).unwrap();
        let current = root_rx.recv_async().await.unwrap();
        let events = parent.subscribe_events();
        let child_gate = Gate::new();
        let (tx, rx) = flume::unbounded();
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let child_actor = child.actor().unwrap();
        child_actor.configure_parent_delivery(true).unwrap();
        let user = parent.admit_turn(input(), None, USER.into()).unwrap();
        let ticket = child_actor.admit_turn(input(), None, CHILD.into()).unwrap();
        let wait = current
            .lease()
            .observe_descendant(&current, child.id(), &child_actor, ticket.clone(), None)
            .unwrap();
        rx.recv_async().await.unwrap();
        child_gate.release(1);
        let result = wait.wait_result().await.unwrap();
        assert_eq!(result.outcome, ticket.peek().unwrap());
        let delivery = loop {
            if let crate::ActorEvent::Delivery(event) = events.recv_async().await.unwrap() {
                break event;
            }
        };
        assert_eq!(delivery.delivery.result.outcome.agent_id(), child.id());
        assert_eq!(delivery.delivery.result.outcome.turn_id(), ticket.turn_id());
        assert_eq!(delivery.delivery.result.outcome, result.outcome);
        let crate::actor::DeliveryStatus::Accepted = delivery.status else {
            panic!("delivery was not admitted")
        };
        assert_eq!(parent.latest_ticket().unwrap().turn_id(), user.turn_id());
        root_gate.release(2);
        user.wait().await;
        let mut end_ids = Vec::new();
        let mut executed = Vec::new();
        loop {
            match events.recv_async().await.unwrap() {
                crate::ActorEvent::Delivery(event) => {
                    assert_eq!(
                        event.status,
                        crate::actor::DeliveryStatus::Executed {
                            parent_turn_id: root_ticket.turn_id(),
                        }
                    );
                    executed.push(event.delivery.result.outcome.turn_id());
                }
                crate::ActorEvent::End(result) => {
                    let turn_id = result.outcome.turn_id();
                    end_ids.push(turn_id);
                    if turn_id == user.turn_id() {
                        break;
                    }
                }
                _ => {}
            }
        }
        assert_eq!(executed, vec![ticket.turn_id()]);
        assert_eq!(end_ids, vec![root_ticket.turn_id(), user.turn_id()]);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false; "timeout")]
#[test_case(true; "drop")]
fn managed_observation_preserves_child_on_timeout_or_drop(drop_wait: bool) {
    smol::block_on(async {
        const CHILD_CORRELATION: &str = "observed-child";
        const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
        let (manager, root, current, root_gate) = active_root(AgentLimits::default());
        assert!(current.manager().same_manager(&manager));
        assert_eq!(current.agent_ref().unwrap().id(), root.id());
        assert!(current.agent_ref().unwrap().same_manager(&root));
        let child_gate = Gate::new();
        let (child_tx, child_rx) = flume::bounded(1);
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(child_tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let actor = child.actor().unwrap();
        let ticket = actor
            .admit_turn(input(), None, CHILD_CORRELATION.into())
            .unwrap();
        child_rx.recv_async().await.unwrap();
        let wait = current
            .lease()
            .observe_descendant(
                &current,
                child.id(),
                &actor,
                ticket.clone(),
                if drop_wait {
                    None
                } else {
                    Some(Duration::ZERO)
                },
            )
            .unwrap();
        let inner = Arc::clone(&wait.inner);
        if drop_wait {
            drop(wait);
            assert_eq!(
                inner.wait_result().await,
                Err(super::PromptWaitError::Cancelled)
            );
            current.lease.inner.wait_until_owned().await;
        } else {
            assert!(matches!(
                wait.wait_result().await,
                Err(super::PromptWaitError::Timeout)
            ));
        }
        assert_eq!(current.lease.inner.state.lock().unwrap().suspensions, 0);
        assert!(current.lease.inner.state.lock().unwrap().owns_permit);
        assert_eq!(actor.snapshot().lifecycle, crate::ActorLifecycle::Open);
        assert_eq!(
            child.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Live
        );
        assert!(ticket.peek().is_none());
        child_gate.release(1);
        assert!(matches!(
            ticket.wait_result().await.outcome,
            TurnOutcome::Completed { .. }
        ));
        let repeat = current
            .lease()
            .observe_descendant(&current, child.id(), &actor, ticket.clone(), None)
            .unwrap();
        assert_eq!(
            repeat.wait_result().await.unwrap().outcome,
            ticket.peek().unwrap()
        );
        root_gate.release(1);
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
fn parent_waiting_for_child_yields_permit() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let root_gate = Gate::new();
        let (root_tx, root_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| {
                    Ok::<_, String>(TestBackend::reporting(
                        root_tx,
                        Some(Arc::clone(&root_gate)),
                    ))
                },
            )
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = root_rx.recv_async().await.unwrap();
        let child_gate = Gate::new();
        let (child_tx, child_rx) = flume::bounded(1);
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(child_tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let ticket = child
            .actor()
            .unwrap()
            .admit_turn(input(), None, "child".into())
            .unwrap();
        let wait = current
            .lease()
            .observe_descendant(&current, child.id(), &child.actor().unwrap(), ticket, None)
            .unwrap();

        child_rx.recv_async().await.unwrap();
        child_gate.release(1);
        let outcome = wait.wait().await.unwrap();
        assert_eq!(outcome.agent_id(), child.id());

        root_gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn parent_cancellation_seals_descendant_wait_registration() {
    let limits = AgentLimits {
        max_concurrent_agent_turns: 2,
        ..AgentLimits::default()
    };
    let (manager, root, current, root_gate) = active_root(limits);
    let child_gate = Gate::new();
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            Box::new(TestBackend {
                current: None,
                cancel: None,
                gate: Some(Arc::clone(&child_gate)),
            }),
        )
        .unwrap();
    let child_actor = child.actor().unwrap();
    let child_ticket = child_actor
        .admit_turn(input(), None, "child".into())
        .unwrap();
    let (registration_entered_tx, registration_entered_rx) = flume::bounded(1);
    let (registration_release_tx, registration_release_rx) = flume::bounded(1);
    manager.set_prompt_wait_registration_gate(registration_entered_tx, registration_release_rx);

    let lease = current.lease();
    let wait_current = current.clone();
    let wait_actor = child_actor.clone();
    let child_id = child.id();
    let pending_ticket = child_ticket.clone();
    let registration = std::thread::spawn(move || {
        lease.observe_descendant(&wait_current, child_id, &wait_actor, pending_ticket, None)
    });
    registration_entered_rx.recv().unwrap();

    manager.cancel_agent(root.id()).unwrap();
    loop {
        if current.lease.inner.state.lock().unwrap().suspensions_sealed {
            break;
        }
        std::thread::yield_now();
    }
    registration_release_tx.send(()).unwrap();
    assert!(matches!(
        registration.join().unwrap(),
        Err(ManagerError::InactiveTurn { .. })
    ));
    {
        let state = current.lease.inner.state.lock().unwrap();
        assert!(state.suspensions_sealed);
        assert!(!state.closing);
        assert_eq!(state.suspensions, 0);
        assert!(state.watcher_cancels.is_empty());
    }
    let mut child_completion = Box::pin(child_ticket.wait());
    assert!(smol::block_on(futures_lite::future::poll_once(&mut child_completion)).is_none());

    child_gate.release(1);
    assert!(matches!(
        smol::block_on(child_completion),
        TurnOutcome::Completed { .. }
    ));
    root_gate.release(1);
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
}

#[test]
fn parent_cancellation_while_suspended_retires_wait_and_releases_permit() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let (current_tx, current_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| {
                    Ok::<_, String>(Box::new(ReportingCancellableBackend {
                        current: current_tx,
                    }) as Box<dyn ActorBackend>)
                },
            )
            .unwrap();
        let root_ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = current_rx.recv_async().await.unwrap();
        let child_gate = Gate::new();
        let (child_entered_tx, child_entered_rx) = flume::bounded(1);
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(child_entered_tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let child_ticket = child
            .actor()
            .unwrap()
            .admit_turn(input(), None, "child".into())
            .unwrap();
        let wait = current
            .lease()
            .observe_descendant(
                &current,
                child.id(),
                &child.actor().unwrap(),
                child_ticket.clone(),
                None,
            )
            .unwrap();
        let wait_inner = Arc::clone(&wait.inner);
        child_entered_rx.recv_async().await.unwrap();
        assert_eq!(current.lease.inner.state.lock().unwrap().suspensions, 1);
        let mut child_completion = Box::pin(child_ticket.wait());
        assert!(
            futures_lite::future::poll_once(&mut child_completion)
                .await
                .is_none()
        );

        manager.cancel_agent(root.id()).unwrap();
        assert!(matches!(
            wait_inner.wait_result().await,
            Err(super::PromptWaitError::Cancelled)
        ));
        let mut cancelled_wait = Box::pin(wait.wait());
        assert!(
            futures_lite::future::poll_once(&mut cancelled_wait)
                .await
                .is_none()
        );
        assert!(root_ticket.peek_result().is_none());
        {
            let state = current.lease.inner.state.lock().unwrap();
            assert!(!state.closing);
            assert!(state.permit.is_none());
        }
        child_gate.release(1);
        assert!(matches!(
            child_completion.await,
            TurnOutcome::Completed { .. }
        ));
        assert!(matches!(
            cancelled_wait.await,
            Err(super::PromptWaitError::Cancelled)
        ));
        assert!(matches!(
            root_ticket.wait().await,
            TurnOutcome::Cancelled {
                reason: crate::TurnCancellationReason::User,
                ..
            }
        ));
        {
            let state = current.lease.inner.state.lock().unwrap();
            assert!(state.closing);
            assert_eq!(state.suspensions, 0);
            assert!(state.watcher_cancels.is_empty());
        }
        let permit = manager.0.limiter.acquire_arc().await;
        drop(permit);
        child.close_subtree().unwrap();
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn two_child_waits_share_one_parent_suspension() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let root_gate = Gate::new();
        let (root_tx, root_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| {
                    Ok::<_, String>(TestBackend::reporting(
                        root_tx,
                        Some(Arc::clone(&root_gate)),
                    ))
                },
            )
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = root_rx.recv_async().await.unwrap();

        let first_gate = Gate::new();
        let (first_tx, first_rx) = flume::bounded(1);
        let first = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(first_tx, Some(Arc::clone(&first_gate))),
            )
            .unwrap();
        let first_ticket = first
            .actor()
            .unwrap()
            .admit_turn(input(), None, "first".into())
            .unwrap();
        let first_wait = current
            .lease()
            .observe_descendant(
                &current,
                first.id(),
                &first.actor().unwrap(),
                first_ticket,
                None,
            )
            .unwrap();
        let first_wait_inner = Arc::clone(&first_wait.inner);

        let second_gate = Gate::new();
        let (second_tx, second_rx) = flume::bounded(1);
        let second = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(second_tx, Some(Arc::clone(&second_gate))),
            )
            .unwrap();
        let second_ticket = second
            .actor()
            .unwrap()
            .admit_turn(input(), None, "second".into())
            .unwrap();
        let second_wait = current
            .lease()
            .observe_descendant(
                &current,
                second.id(),
                &second.actor().unwrap(),
                second_ticket,
                None,
            )
            .unwrap();
        {
            let state = current.lease.inner.state.lock().unwrap();
            assert_eq!(state.suspensions, 2);
            assert!(state.permit.is_none());
            assert_eq!(state.watcher_cancels.len(), 2);
        }

        first_rx.recv_async().await.unwrap();
        first_gate.release(1);
        assert!(matches!(
            first_wait_inner.wait_result().await,
            Ok(TurnOutcome::Completed { .. })
        ));
        second_rx.recv_async().await.unwrap();
        let mut first_wait = Box::pin(first_wait.wait());
        assert!(
            futures_lite::future::poll_once(&mut first_wait)
                .await
                .is_none()
        );
        {
            let state = current.lease.inner.state.lock().unwrap();
            assert_eq!(state.suspensions, 1);
            assert!(state.permit.is_none());
            assert_eq!(state.watcher_cancels.len(), 1);
        }
        second_gate.release(1);
        assert_eq!(first_wait.await.unwrap().agent_id(), first.id());
        assert_eq!(second_wait.wait().await.unwrap().agent_id(), second.id());
        {
            let state = current.lease.inner.state.lock().unwrap();
            assert_eq!(state.suspensions, 0);
            assert!(state.permit.is_some());
            assert!(state.watcher_cancels.is_empty());
        }

        root_gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn shutdown_timeout_reaper_eventually_joins_timed_out_node() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let gate = Gate::new();
        let (entered_tx, entered_rx) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                TestBackend::reporting(entered_tx, Some(Arc::clone(&gate))),
            )
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "held".into())
            .unwrap();
        entered_rx.recv_async().await.unwrap();

        let report = manager.shutdown(std::time::Duration::ZERO).await;
        assert!(report.joined.is_empty());
        assert_eq!(report.timed_out, vec![root.id()]);

        gate.release(1);
        manager.wait_until_reaped().await;
        assert!(manager.runner_finished(root.id()).unwrap());
        assert_eq!(
            root.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Dormant
        );
        assert_eq!(root.snapshot().unwrap().residency, AgentResidency::Dormant);
    });
}

#[test]
fn active_turns_never_exceed_manager_limit() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let gate = Gate::new();
        let (root_tx, root_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(root_tx, Some(Arc::clone(&gate)))),
            )
            .unwrap();
        root.actor()
            .unwrap()
            .admit_turn(input(), None, "root".into())
            .unwrap();
        let current = root_rx.recv_async().await.unwrap();
        let (child_tx, child_rx) = flume::bounded(1);
        let child = manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(child_tx, Some(Arc::clone(&gate))),
            )
            .unwrap();
        child
            .actor()
            .unwrap()
            .admit_turn(input(), None, "child".into())
            .unwrap();
        assert!(child_rx.try_recv().is_err());
        gate.release(1);
        child_rx.recv_async().await.unwrap();
        gate.release(1);
        let report = manager.shutdown(std::time::Duration::from_secs(1)).await;
        assert!(report.timed_out.is_empty());
    });
}

struct JoiningBackend {
    current: flume::Sender<crate::CurrentManagedTurn>,
    start: Option<Arc<Gate>>,
    joined: flume::Sender<usize>,
}

impl ActorBackend for JoiningBackend {
    fn run_turn<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.current
                .send(context.managed_turn.clone().unwrap())
                .unwrap();
            if let Some(start) = self.start.take() {
                start.enter().await;
            }
            let inbox = context.inbox.as_ref().unwrap();
            inbox.join_children().await;
            let completions = inbox.drain_completions();
            let _ = self.joined.send(completions.len());
            for message in completions {
                history.push_observation(message).unwrap();
            }
            inbox.instructions_submitted();
            while !inbox.finish() {
                for message in inbox.drain_completions() {
                    history.push_observation(message).unwrap();
                }
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

#[derive(Clone, Copy)]
enum JoinCase {
    ReportingChildOfChild,
    CancelledSuccessorOfRunningTurn,
    NonReportingChildOfChild,
    ChildOfRoot,
}

/// A subagent's turn does not settle while a child that reports to it is still
/// running, and it yields its permit so that child can run at capacity one.
/// Roots and children without automatic delivery are never joined.
#[test_case(JoinCase::ReportingChildOfChild; "subagent_joins_reporting_child")]
#[test_case(JoinCase::CancelledSuccessorOfRunningTurn; "joins_running_turn_behind_settled_latest")]
#[test_case(JoinCase::NonReportingChildOfChild; "non_reporting_child_not_joined")]
#[test_case(JoinCase::ChildOfRoot; "root_does_not_join")]
fn subagent_turn_joins_reporting_children_at_capacity_one(case: JoinCase) {
    smol::block_on(async {
        const ROOT: &str = "root";
        const JOINER: &str = "joiner";
        const CHILD: &str = "child";
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        let manager = AgentManagerHandle::new(AgentLimits {
            max_concurrent_agent_turns: 1,
            ..Default::default()
        })
        .unwrap();
        let (current_tx, current_rx) = flume::unbounded();
        let (joined_tx, joined_rx) = flume::unbounded();
        let start = Gate::new();
        let joiner_backend = || {
            Box::new(JoiningBackend {
                current: current_tx.clone(),
                start: Some(Arc::clone(&start)),
                joined: joined_tx.clone(),
            }) as Box<dyn ActorBackend>
        };
        let (joiner, joiner_ticket) = if matches!(case, JoinCase::ChildOfRoot) {
            let root = manager
                .create_root_with_config(Some(config(MODEL, false)), Vec::new(), None, |_| {
                    Ok::<_, String>(joiner_backend())
                })
                .unwrap();
            let ticket = root
                .actor()
                .unwrap()
                .admit_turn(input(), None, JOINER.into())
                .unwrap();
            (root, ticket)
        } else {
            let (root_tx, root_rx) = flume::unbounded();
            let root_gate = Gate::new();
            let root = manager
                .create_root_with_config(Some(config(MODEL, false)), Vec::new(), None, |_| {
                    Ok::<_, String>(TestBackend::reporting(
                        root_tx,
                        Some(Arc::clone(&root_gate)),
                    ))
                })
                .unwrap();
            let root_ticket = root
                .actor()
                .unwrap()
                .admit_turn(input(), None, ROOT.into())
                .unwrap();
            let root_turn = root_rx.recv_async().await.unwrap();
            let joiner = manager
                .spawn_child(
                    &root_turn,
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    joiner_backend(),
                )
                .unwrap();
            root_gate.release(1);
            root_ticket.wait().await;
            let ticket = joiner
                .actor()
                .unwrap()
                .admit_turn(input(), None, JOINER.into())
                .unwrap();
            (joiner, ticket)
        };
        let joiner_turn = current_rx.recv_async().await.unwrap();
        let (child_tx, child_rx) = flume::unbounded();
        let child_gate = Gate::new();
        let child = manager
            .spawn_child(
                &joiner_turn,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::reporting(child_tx, Some(Arc::clone(&child_gate))),
            )
            .unwrap();
        let child_actor = child.actor().unwrap();
        child_actor
            .configure_parent_delivery(!matches!(case, JoinCase::NonReportingChildOfChild))
            .unwrap();
        let child_ticket = child_actor.admit_turn(input(), None, CHILD.into()).unwrap();
        let cancelled_successor = matches!(case, JoinCase::CancelledSuccessorOfRunningTurn);
        if cancelled_successor {
            let successor = child_actor.admit_turn(input(), None, CHILD.into()).unwrap();
            child_actor.cancel_turn(successor.turn_id()).unwrap();
            assert!(matches!(
                successor.wait().await,
                TurnOutcome::Cancelled { .. }
            ));
        }
        start.release(1);
        if matches!(
            case,
            JoinCase::ReportingChildOfChild | JoinCase::CancelledSuccessorOfRunningTurn
        ) {
            child_rx.recv_async().await.unwrap();
            assert!(joined_rx.is_empty());
            assert!(joiner_ticket.peek().is_none());
            child_gate.release(1);
            assert_eq!(
                joined_rx.recv_async().await.unwrap(),
                1 + usize::from(cancelled_successor)
            );
            assert!(child_ticket.peek().is_some());
        } else {
            assert_eq!(joined_rx.recv_async().await.unwrap(), 0);
            assert!(child_ticket.peek().is_none());
            child_gate.release(1);
        }
        assert!(matches!(
            joiner_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        assert!(matches!(
            child_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        drop(joiner);
        assert!(
            manager
                .shutdown(COMPLETION_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}
