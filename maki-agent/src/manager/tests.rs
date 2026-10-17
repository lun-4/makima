use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use test_case::test_case;

use event_listener::Event;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{Model, ProviderEvent, RequestOptions, StreamResponse, TokenUsage};
use maki_storage::id::SessionRef;
use serde_json::Value;

use super::{AgentLimits, AgentManagerHandle, AgentMetadata, GraphLifecycle, ManagerError};
use crate::actor::PreparedTurn;
use crate::{
    ActorBackend, ActorError, AgentEvent, AgentInput, AgentMode, BackendResult, ConfigChange,
    ConfigPatch, ControlWork, EventSender, History, PreparedModel, RootWork, TurnContext,
    TurnOutcome, WorkKind,
};

const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
const CONDITION_TIMEOUT: &str = "condition never became true";

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
        assert!(
            matches!(root.turn_ticket(ticket.turn_id()), Err(ManagerError::UnknownAgent(id)) if id == root.id())
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
            assert!(matches!(
                manager.lookup(root.id()),
                Err(ManagerError::UnknownAgent(_))
            ));
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
    Root,
}

enum MatchingSuccessor {
    None,
    Admitted,
    Prepared,
}

#[test_case(PoppedWork::Admitted, crate::TurnCancellationReason::User, MatchingSuccessor::None; "admitted_user")]
#[test_case(PoppedWork::Root, crate::TurnCancellationReason::Shutdown, MatchingSuccessor::None; "root_shutdown")]
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
        const RUN_ID: u64 = 7;
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
                        Some(EventSender::new(event_tx, RUN_ID)),
                        CORRELATION.into(),
                    )
                    .unwrap(),
            ),
            PoppedWork::Prepared => {
                let operation = actor
                    .reserve_prepared_operation(Some(PreparedTurn {
                        input: input(),
                        event_sender: Some(EventSender::new(event_tx, RUN_ID)),
                        correlation: CORRELATION.into(),
                    }))
                    .unwrap();
                operation.resolve(None).unwrap();
                operation.wait_ready().await.unwrap();
                operation.commit().unwrap().ticket
            }
            PoppedWork::Root => {
                actor
                    .rush(RootWork::new(
                        input(),
                        RUN_ID,
                        false,
                        CORRELATION.into(),
                        Vec::new(),
                        CORRELATION.into(),
                    ))
                    .unwrap();
                None
            }
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
        const EARLIER_RUN: u64 = 7;
        const LAST_RUN: u64 = 8;
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
        for (run_id, correlation) in [(EARLIER_RUN, EARLIER), (LAST_RUN, LAST)] {
            actor
                .rush(RootWork::new(
                    input(),
                    run_id,
                    false,
                    correlation.into(),
                    Vec::new(),
                    correlation.into(),
                ))
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

#[test_case(true; "child_limit")]
#[test_case(false; "graph_limit")]
fn closed_callback_leases_hold_admission_capacity_until_drained(child_limit: bool) {
    smol::block_on(async {
        const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
        let limits = AgentLimits {
            max_children_per_agent: if child_limit { 1 } else { 2 },
            max_live_agents: 2,
            ..Default::default()
        };
        let manager = AgentManagerHandle::new(limits).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        for _ in 0..3 {
            let spawn = || {
                manager.spawn_child_trusted_with_config(
                    &root,
                    config(MODEL, false),
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    |_| Ok::<_, String>(TestBackend::boxed()),
                )
            };
            let child = spawn().unwrap();
            let actor = child.actor().unwrap();
            let events = actor.subscribe_leased_events();
            actor.close();
            let (event, lease) = events.recv_async().await.unwrap().into_parts();
            assert!(matches!(event, crate::ActorEvent::Close { .. }));
            yield_until(|| child.snapshot().unwrap().graph_lifecycle == GraphLifecycle::Closed)
                .await;
            let error = spawn().unwrap_err();
            if child_limit {
                assert_eq!(
                    error,
                    ManagerError::ChildLimit {
                        parent_id: root.id(),
                        max: 1
                    }
                );
            } else {
                assert_eq!(error, ManagerError::LiveAgentLimit { max: 2 });
            }
            assert_eq!(manager.snapshot().len(), 2);
            drop(lease);
            drop(events);
            yield_until(|| manager.runner_finished(child.id()).unwrap()).await;
            assert_eq!(manager.snapshot().len(), 1);
        }
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
fn closed_node_waits_for_queued_event_callback_lease() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let actor = root.actor().unwrap();
        let events = actor.subscribe_leased_events();
        actor.close();
        let (event, lease) = events.recv_async().await.unwrap().into_parts();
        assert!(matches!(event, crate::ActorEvent::Close { .. }));
        yield_until(|| root.snapshot().unwrap().graph_lifecycle == GraphLifecycle::Closed).await;
        assert!(manager.node(root.id()).is_ok());
        let report = manager.shutdown(Duration::ZERO).await;
        assert!(report.joined.is_empty());
        assert_eq!(report.timed_out, vec![root.id()]);
        assert_eq!(
            root.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
        assert!(!manager.runner_finished(root.id()).unwrap());
        drop(lease);
        drop(events);
        manager.wait_until_reaped().await;
        assert!(matches!(
            manager.node(root.id()),
            Err(ManagerError::UnknownAgent(_))
        ));
        assert_eq!(
            root.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
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
            Err(ManagerError::UnknownAgent(id)) if id == root.id()
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
        .rush(RootWork::new(
            input(),
            1,
            false,
            String::new(),
            Vec::new(),
            CORRELATION.into(),
        ))
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
    let (rejected_tx, rejected_rx) = flume::bounded(1);
    *manager.0.managed_registration_rejected.lock().unwrap() = Some(rejected_tx);
    acquire_release_tx.send(()).unwrap();
    let rejected = rejected_rx.recv_timeout(COMPLETION_TIMEOUT);
    assert!(manager.0.limiter.try_acquire().is_some());
    assert!(actor.inner.state.try_lock().is_ok());
    assert!(manager.0.graph.try_lock().is_ok());
    assert!(ticket.peek().is_none());
    cut_release_tx.send(()).unwrap();
    cancel.join().unwrap().unwrap();
    rejected.unwrap();
    let outcome = smol::block_on(futures_lite::future::or(
        async { Some(ticket.wait().await) },
        async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            None
        },
    ));
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
        GraphLifecycle::Closed
    );
}

#[test]
fn spawn_close_cycles_reclaim_nodes_and_share_terminal_snapshots() {
    const CYCLES: usize = 32;
    let (manager, root, current, gate) = active_root(AgentLimits::default());
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
        smol::block_on(async {
            loop {
                let listener = manager.0.reaped.listen();
                if !manager.lock_graph().nodes.contains_key(&child.id()) {
                    break;
                }
                listener.await;
            }
        });
        assert_eq!(manager.lock_graph().nodes.len(), 1);
        assert!(root.snapshot().unwrap().children.is_empty());
        assert!(
            matches!(manager.lookup(child.id()), Err(ManagerError::UnknownAgent(id)) if id == child.id())
        );
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
        drop(child);
        assert!(terminal.upgrade().is_some());
        drop(held);
        assert!(terminal.upgrade().is_none());
    }
    gate.release(1);
    assert!(
        smol::block_on(manager.shutdown(COMPLETION_TIMEOUT))
            .timed_out
            .is_empty()
    );
    assert!(manager.lock_graph().nodes.is_empty());
    assert_eq!(
        root.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
}

#[test_case(false; "correlation")]
#[test_case(true; "nominal_turn")]
fn cancel_cut_retires_authority_and_captures_children_before_close(nominal: bool) {
    const CORRELATION: &str = "root";
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
    let (cut_tx, cut_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    manager.set_descendant_cut_gate(cut_tx, release_rx);
    let cancelling = manager.clone();
    let root_id = root.id();
    let turn_id = current.turn_id();
    assert_eq!(root.actor().unwrap().processing_turn_id(), Some(turn_id));
    let cancel = std::thread::spawn(move || {
        if nominal {
            cancelling.cancel_turn(root_id, turn_id, crate::TurnCancellationReason::User)
        } else {
            cancelling.cancel_correlation(root_id, CORRELATION, crate::TurnCancellationReason::User)
        }
    });
    cut_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    let child_snapshot = child.snapshot().unwrap();
    let post_cut_spawn = manager.spawn_child(
        &current,
        AgentMetadata::default(),
        Vec::new(),
        None,
        TestBackend::boxed(),
    );
    release_tx.send(()).unwrap();
    cancel.join().unwrap().unwrap();
    assert_eq!(child_snapshot.graph_lifecycle, GraphLifecycle::Closing);
    assert_eq!(
        child_snapshot.actor.unwrap().lifecycle,
        crate::ActorLifecycle::Open
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

#[test_case(crate::TurnCancellationReason::User, false; "admitted_user")]
#[test_case(crate::TurnCancellationReason::Shutdown, false; "admitted_shutdown")]
#[test_case(crate::TurnCancellationReason::User, true; "root_user")]
#[test_case(crate::TurnCancellationReason::Shutdown, true; "root_shutdown")]
fn exact_cancel_pop_gap_preserves_same_correlation_successor(
    reason: crate::TurnCancellationReason,
    root_work: bool,
) {
    const CORRELATION: &str = "reused";
    const RUN_ID: u64 = 7;
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let (entered_tx, entered_rx) = flume::unbounded();
    let root = manager
        .create_root(Vec::new(), None, TestBackend::reporting(entered_tx, None))
        .unwrap();
    let actor = root.actor().unwrap();
    assert_eq!(actor.processing_turn_id(), None);
    let (popped, release) = actor.pause_after_next_pop();
    let target = if root_work {
        actor
            .rush(RootWork::new(
                input(),
                RUN_ID,
                false,
                CORRELATION.into(),
                Vec::new(),
                CORRELATION.into(),
            ))
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

#[test_case(false; "admitted")]
#[test_case(true; "standalone_root")]
fn delayed_exact_cancel_preserves_successor_authority_and_children(root_work: bool) {
    const CORRELATION: &str = "reused";
    const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    const FIRST_RUN_ID: u64 = 7;
    const SUCCESSOR_RUN_ID: u64 = 8;
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
    let first = if root_work {
        actor
            .rush(RootWork::new(
                input(),
                FIRST_RUN_ID,
                false,
                CORRELATION.into(),
                Vec::new(),
                CORRELATION.into(),
            ))
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
    let successor = if root_work {
        actor
            .rush(RootWork::new(
                input(),
                SUCCESSOR_RUN_ID,
                false,
                CORRELATION.into(),
                Vec::new(),
                CORRELATION.into(),
            ))
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

#[test]
fn idle_guard_allows_idle_live_child() {
    let (manager, root, _) = idle_root_with_child(TestBackend::boxed());
    drop(manager.prepare_idle_subtree(root.id()).unwrap());
    smol::block_on(manager.shutdown(Duration::from_secs(1)));
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
    gate: Option<Arc<Gate>>,
}

impl TestBackend {
    fn boxed() -> Box<dyn ActorBackend> {
        Box::new(Self {
            current: None,
            gate: None,
        })
    }

    fn reporting(
        current: flume::Sender<crate::CurrentManagedTurn>,
        gate: Option<Arc<Gate>>,
    ) -> Box<dyn ActorBackend> {
        Box::new(Self {
            current: Some(current),
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
        _: &'a mut History,
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

impl ActorBackend for CancellableBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
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
        _: &'a mut History,
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

impl ActorBackend for TestBackend {
    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(sender) = &self.current {
                sender.send(context.managed_turn.clone().unwrap()).unwrap();
            }
            if let Some(gate) = &self.gate {
                gate.enter().await;
            }
            if let Some(inbox) = &context.inbox {
                for input in inbox.drain_instructions() {
                    history.push(maki_providers::Message::user(input.message));
                }
                for message in inbox.drain_completions() {
                    history.push(message);
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

struct ConfigTestProvider;

impl Provider for ConfigTestProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [maki_providers::Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, maki_providers::AgentError>> {
        Box::pin(async { unreachable!() })
    }

    fn list_models(
        &self,
    ) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
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
        mode: AgentMode::Build,
        images: Vec::new(),
        preamble: Vec::new(),
        thinking: Default::default(),
        fast: false,
        workflow: false,
        prompt: None,
        cancel: None,
        lease_committer: None,
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
        let update = child.actor().unwrap().reserve_config_update().unwrap();
        update
            .resolve(Ok(ConfigChange::Patch(ConfigPatch {
                model: Some(PreparedModel {
                    provider: Arc::clone(&updated.provider),
                    model: updated.model.clone(),
                }),
                fast: Some(true),
                workflow: Some(true),
                ..Default::default()
            })))
            .unwrap();
        update.wait().await.unwrap();
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
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate)))),
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let plan_mode = AgentMode::Plan("plan.md".into());
        let mut plan_input = input();
        plan_input.mode = plan_mode.clone();
        actor.admit_turn(plan_input, None, "root".into()).unwrap();
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
        let mut plan_input = input();
        plan_input.mode = plan_mode;
        assert!(
            child_actor
                .admit_turn(plan_input, None, "plan-child".into())
                .is_ok()
        );
        assert!(
            child_actor
                .admit_turn(input(), None, "build-child".into())
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
        let root = manager
            .create_root_with_config(
                Some(config("anthropic/claude-sonnet-4-20250514", false)),
                Vec::new(),
                None,
                |_| Ok::<_, String>(TestBackend::reporting(tx, Some(Arc::clone(&gate)))),
            )
            .unwrap();
        let actor = root.actor().unwrap();
        let custom_mode = AgentMode::Custom(crate::ModeId::parse("review"));
        let mut parent_input = input();
        parent_input.mode = custom_mode.clone();
        actor
            .admit_turn(parent_input, None, "root-custom".into())
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
        let mut child_input = input();
        child_input.mode = custom_mode;
        assert!(
            child_actor
                .admit_turn(child_input, None, "custom-child".into())
                .is_ok()
        );
        assert!(
            child_actor
                .admit_turn(input(), None, "build-child".into())
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
            max_live_agents: 0,
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
            let reason = backend_cancel.cancelled().await;
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
        let execution = super::manage_execution(
            Box::pin(backend),
            guard,
            Arc::clone(&current.lease.inner),
            token,
        );
        let task = smol::spawn(execution);

        polled_rx.recv_async().await.unwrap();
        cancel.cancel(crate::TurnCancellationReason::User);
        polled_rx.recv_async().await.unwrap();
        assert!(
            !manager
                .lock_graph()
                .active_turns
                .contains_key(&(root.id(), turn_id))
        );
        assert!(!current.lease.inner.state.lock().unwrap().closing);

        while current.lease.inner.state.lock().unwrap().permit.is_none() {
            smol::future::yield_now().await;
        }
        assert!(!task.is_finished());
        cleanup_gate.release(1);
        assert!(matches!(
            task.await,
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
fn root_and_nested_children_use_one_factory() {
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
    let root_node = root.snapshot().unwrap();
    let child_node = child.snapshot().unwrap();
    assert_eq!(root_node.parent_id, None);
    assert_eq!(root_node.root_id, root.id());
    assert_eq!(root_node.children, vec![child.id()]);
    assert_eq!(child_node.parent_id, Some(root.id()));
    assert_eq!(child_node.root_id, root.id());
    assert_eq!(child_node.depth, 1);
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
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
        max_live_agents: 2,
        ..AgentLimits::default()
    };
    let (manager, _root, current, gate) = active_root(limits);
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
fn shutdown_excludes_removed_child_and_preserves_held_snapshot() {
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
    assert!(!manager.lock_graph().nodes.contains_key(&child.id()));
    gate.release(1);

    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));

    assert_eq!(report.joined, vec![root.id()]);
    assert!(report.timed_out.is_empty());
    assert_eq!(
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
    assert!(!manager.lock_graph().nodes.contains_key(&child.id()));
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
fn depth_and_child_limits_are_atomic() {
    let limits = AgentLimits {
        max_agent_depth: 1,
        max_children_per_agent: 1,
        ..AgentLimits::default()
    };
    let (manager, root, current, gate) = active_root(limits);
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
    assert_eq!(
        error,
        ManagerError::ChildLimit {
            parent_id: root.id(),
            max: 1
        }
    );
    assert_eq!(manager.snapshot().len(), 2);
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn live_agent_limit_is_atomic() {
    let limits = AgentLimits {
        max_live_agents: 2,
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

    assert_eq!(error, ManagerError::LiveAgentLimit { max: 2 });
    assert_eq!(manager.snapshot().len(), 2);
    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn closing_idle_child_releases_capacity_on_next_admission() {
    let limits = AgentLimits {
        max_children_per_agent: 1,
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
        child.snapshot().unwrap().graph_lifecycle,
        GraphLifecycle::Closed
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
fn direct_actor_close_releases_child_capacity_when_runner_finishes() {
    let limits = AgentLimits {
        max_children_per_agent: 1,
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
    manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        )
        .unwrap();

    gate.release(1);
    smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
}

#[test]
fn closing_active_child_consumes_capacity_until_runner_finishes() {
    smol::block_on(async {
        let limits = AgentLimits {
            max_children_per_agent: 1,
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
            Err(ManagerError::ChildLimit { .. })
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
        manager
            .spawn_child(
                &current,
                AgentMetadata::default(),
                Vec::new(),
                None,
                TestBackend::boxed(),
            )
            .unwrap();

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
                Some(EventSender::new(event_tx, 0)),
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
    commit_release_tx.send(()).unwrap();
    let child = factory.join().unwrap().unwrap();
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
fn unauthorized_prompt_wait_does_not_cancel_child_turn() {
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

        let Err(error) = wrong_current.lease().wait_for_descendant(
            &wrong_current,
            child.id(),
            &actor,
            ticket.clone(),
            None,
        ) else {
            panic!("wrong manager accepted prompt wait");
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
fn descendant_admission_rejects_mismatched_actor_without_suspending(foreign: bool) {
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
        let error = current
            .lease()
            .admit_and_wait_for_descendant(
                &current,
                child.id(),
                &wrong_actor,
                super::PromptAdmission {
                    input: input(),
                    event_sender: None,
                    correlation: CORRELATION.into(),
                },
                None,
            )
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
        assert_eq!(child_actor.snapshot().queued, 0);
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
            .wait_for_descendant(
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
        const COMPATIBILITY_ID: &str = "nested child";
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
        let barrier = parent_actor.reserve_config_update().unwrap();
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
        child_actor
            .configure_parent_delivery(true, Some(COMPATIBILITY_ID.into()))
            .unwrap();
        let child_events = child_actor.subscribe_events();
        if closed {
            parent_actor.close();
        }
        let ticket = child_actor
            .admit_turn(input(), None, COMPATIBILITY_ID.into())
            .unwrap();
        ticket.wait().await;
        if closed {
            let event = loop {
                if let crate::ActorEvent::Delivery(event) = child_events.recv_async().await.unwrap()
                {
                    break event;
                }
            };
            assert_eq!(event.delivery.parent_id, parent.id());
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
            assert_eq!(event.delivery.parent_id, parent.id());
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
        let barrier = parent.reserve_config_update().unwrap();
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
        actor
            .configure_parent_delivery(true, Some(CHILD.into()))
            .unwrap();
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
        child_actor
            .configure_parent_delivery(true, Some(CHILD.into()))
            .unwrap();
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
        assert_eq!(delivery.delivery.parent_id, root.id());
        assert_eq!(delivery.delivery.result.outcome, result.outcome);
        assert_eq!(delivery.delivery.compatibility_id.as_deref(), Some(CHILD));
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
            .wait_for_descendant(&current, child.id(), &child.actor().unwrap(), ticket, None)
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
        lease.wait_for_descendant(&wait_current, child_id, &wait_actor, pending_ticket, None)
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
            .wait_for_descendant(
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
fn authority_revoked_after_prompt_admission_cancels_only_that_ticket() {
    let limits = AgentLimits {
        max_concurrent_agent_turns: 1,
        ..AgentLimits::default()
    };
    let (manager, _, current, root_gate) = active_root(limits);
    let child_gate = Gate::new();
    let child = manager
        .spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            Box::new(TestBackend {
                current: None,
                gate: Some(Arc::clone(&child_gate)),
            }),
        )
        .unwrap();
    let actor = child.actor().unwrap();
    let surviving = actor.admit_turn(input(), None, "surviving".into()).unwrap();
    let (admitted_tx, admitted_rx) = flume::bounded(1);
    let (register_tx, register_rx) = flume::bounded(1);
    manager.set_prompt_admission_gate(admitted_tx, register_rx);

    let lease = current.lease();
    let prompt_current = current.clone();
    let prompt_actor = actor.clone();
    let prompt_child_id = child.id();
    let prompt = std::thread::spawn(move || {
        lease.admit_and_wait_for_descendant(
            &prompt_current,
            prompt_child_id,
            &prompt_actor,
            super::PromptAdmission {
                input: input(),
                event_sender: None,
                correlation: "cancelled".into(),
            },
            None,
        )
    });
    let cancelled_id = admitted_rx.recv().unwrap();

    root_gate.release(1);
    while current.validate_descendant(child.id()).is_ok() {
        std::thread::yield_now();
    }
    register_tx.send(()).unwrap();
    assert!(matches!(
        prompt.join().unwrap(),
        Err(ManagerError::InactiveTurn { .. })
    ));
    assert!(matches!(
        smol::block_on(actor.wait_outcome(cancelled_id)).unwrap(),
        TurnOutcome::Cancelled {
            reason: crate::TurnCancellationReason::User,
            ..
        }
    ));
    child_gate.release(1);
    assert!(matches!(
        smol::block_on(surviving.wait()),
        TurnOutcome::Completed { .. }
    ));

    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert!(report.timed_out.is_empty());
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
            .wait_for_descendant(
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
            .wait_for_descendant(
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
            GraphLifecycle::Closed
        );
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
        history: &'a mut History,
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
                history.push(message);
            }
            inbox.instructions_submitted();
            while !inbox.finish() {
                for message in inbox.drain_completions() {
                    history.push(message);
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
            .configure_parent_delivery(!matches!(case, JoinCase::NonReportingChildOfChild), None)
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
