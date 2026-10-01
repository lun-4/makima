use std::sync::Arc;

use super::{ScriptedBackend, input, policy, spawn, until};
use crate::actor::{
    ActorError, ConfigChange, ConfigPatch, ControlWork, EffectiveAgentConfig, QueueProjection,
};
use crate::types::TurnCancellationReason;
use crate::{AgentMode, CancelToken, EventSender, Instructions, TurnOutcome};

#[test]
fn admitted_prompt_dependencies_remain_pinned() {
    smol::block_on(async {
        let gate = super::Gate::new();
        let source = Arc::new(std::sync::Mutex::new(String::from("before")));
        let (resolved_tx, resolved_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::gated(Arc::clone(&gate));
        backend.preparation = Some(Arc::new({
            let source = Arc::clone(&source);
            move |_, _, _| {
                let value = source.lock().unwrap().clone();
                let mut snapshot = empty_snapshot();
                snapshot.prompt_inputs = Some(Arc::new(crate::agent::TurnPromptInputs {
                    cwd: value.clone().into(),
                    instructions: Instructions {
                        text: value,
                        ..Default::default()
                    },
                    mcp_prompt: None,
                    resolved: Some(resolved_rx.clone()),
                    ready: None,
                }));
                snapshot
            }
        }));
        let (actor, task) = spawn(backend);
        let active = actor
            .admit_turn(input("active"), None, "active".into())
            .unwrap();
        super::until(|| super::backend_reached(&actor)).await;
        let queued = actor
            .admit_turn(input("queued"), None, "queued".into())
            .unwrap();
        let barrier = actor.reserve_config_update().unwrap();
        barrier.resolve(Err(ActorError::PolicyCancelled)).unwrap();
        assert!(barrier.wait().await.is_err());
        *source.lock().unwrap() = "after".into();
        let work = actor.inner.queue.remove_turn(queued.turn_id()).unwrap();
        let prompt = work
            .admission
            .as_ref()
            .unwrap()
            .prompt_inputs
            .as_ref()
            .unwrap();
        assert_eq!(prompt.cwd, std::path::PathBuf::from("before"));
        assert_eq!(prompt.instructions.text, "before");
        resolved_tx
            .send(Err("pinned dependency failure".into()))
            .unwrap();
        assert!(
            matches!(prompt.resolved.as_ref().unwrap().recv().unwrap(), Err(error) if error == "pinned dependency failure")
        );
        actor.inner.queue.push(crate::actor::ActorWork::Turn(work));
        gate.open();
        active.wait().await;
        queued.wait().await;
        actor.close();
        task.await;
    });
}

#[test_case::test_case(AgentMode::Build; "build")]
#[test_case::test_case(AgentMode::Plan("plan.md".into()); "plan")]
#[test_case::test_case(AgentMode::Custom(crate::modes::ModeId::parse("custom")); "custom")]
fn mode_change_without_definition_preserves_config(mode: AgentMode) {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let before = actor.config_snapshot().unwrap();
        let change = actor.reserve_config_update().unwrap();
        change
            .resolve(Ok(ConfigChange::Mode {
                mode,
                mode_def: None,
            }))
            .unwrap();
        let Err(ActorError::InvalidConfig(message)) = change.wait().await else {
            panic!("unresolved mode change must fail")
        };
        const MISSING_DEFINITION: &str = "mode change requires a resolved definition";
        assert_eq!(message, MISSING_DEFINITION);
        let after = actor.config_snapshot().unwrap();
        assert_eq!(after.generation, before.generation);
        assert!(Arc::ptr_eq(&after.config, &before.config));
        actor.close();
        task.await;
    });
}

#[test]
fn combined_model_options_and_metadata_refresh() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let replacement = policy(false);
        let prepared = crate::PreparedModel {
            provider: replacement.provider,
            model: replacement.model,
        };
        let change = actor.reserve_config_update().unwrap();
        change
            .resolve(Ok(ConfigChange::Patch(ConfigPatch {
                model: Some(prepared.clone()),
                fast: Some(true),
                workflow: Some(true),
                ..Default::default()
            })))
            .unwrap();
        let committed = change.wait().await.unwrap();
        assert_eq!(committed.generation, 1);
        assert!(committed.config.fast && committed.config.workflow);
        assert!(Arc::ptr_eq(&committed.config.provider, &prepared.provider));
        let mut metadata = prepared.clone();
        metadata.model.context_window += 1;
        let refresh = actor.reserve_config_update().unwrap();
        refresh
            .resolve(Ok(ConfigChange::Refresh {
                expected: prepared.clone(),
                replacement: metadata.clone(),
            }))
            .unwrap();
        let refreshed = refresh.wait().await.unwrap();
        assert_eq!(refreshed.generation, 2);
        assert_eq!(
            refreshed.config.model.context_window,
            metadata.model.context_window
        );
        let obsolete = actor.reserve_config_update().unwrap();
        let stale = policy(false);
        obsolete
            .resolve(Ok(ConfigChange::Refresh {
                expected: crate::PreparedModel {
                    provider: stale.provider,
                    model: stale.model,
                },
                replacement: prepared,
            }))
            .unwrap();
        assert_eq!(obsolete.wait().await.unwrap().generation, 2);
        actor.close();
        task.await;
    });
}

#[test]
fn cancel_during_preparation_terminalizes_once() {
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        backend.preparation = Some(Arc::new(move |_, _, _| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            empty_snapshot()
        }));
        let (actor, task) = spawn(backend);
        let (events_tx, events_rx) = flume::unbounded();
        let ticket = actor
            .admit_turn(
                input("cancel"),
                Some(EventSender::new(events_tx, 0)),
                "cancel".into(),
            )
            .unwrap();
        entered_rx.recv_async().await.unwrap();
        let (stale_tx, stale_rx) = flume::bounded(1);
        *actor.inner.stale_preparation.lock().unwrap() = Some(stale_tx);
        actor.cancel_turn(ticket.turn_id()).unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Cancelled { .. }));
        release_tx.send(()).unwrap();
        stale_rx.recv_async().await.unwrap();
        assert_eq!(events_rx.drain().count(), 1);
        assert_eq!(actor.inner.outcomes.lock().unwrap().len(), 1);
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        actor.close();
        task.await;
    });
}

#[test]
fn prepared_turn_commit_pins_config_and_allocates_one_ticket() {
    const WORK: &str = "prepared turn";
    smol::block_on(async {
        let gate = super::Gate::new();
        let (actor, task) = spawn(ScriptedBackend::gated(Arc::clone(&gate)));
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let before = actor.config_snapshot().unwrap();
        let reservation = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        reservation
            .resolve(Some(ConfigChange::ToggleWorkflow))
            .unwrap();
        reservation.wait_ready().await.unwrap();
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        let (popped, release) = actor.pause_after_next_pop();
        let committed = reservation.commit().unwrap();
        assert_eq!(committed.config.generation, before.generation + 1);
        let ticket = committed.ticket.unwrap();
        assert_eq!(actor.inner.tickets.lock().unwrap().len(), 1);
        popped.recv_async().await.unwrap();
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            committed.config.generation
        );
        release.send(()).unwrap();
        gate.open();
        ticket.wait().await;
        actor.close();
        task.await;
    });
}

#[test_case::test_case("drop")]
#[test_case::test_case("remove")]
#[test_case::test_case("clear")]
#[test_case::test_case("close")]
fn prepared_retirement_releases_fifo_without_allocating_turn(action: &str) {
    const WORK: &str = "uncommitted";
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let reservation = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        let successor = actor.reserve_config_update().unwrap();
        successor.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        match action {
            "drop" => drop(reservation),
            "remove" => {
                assert!(actor.remove_at(0).is_some());
                assert_eq!(
                    reservation.wait_ready().await,
                    Err(ActorError::PolicyCancelled)
                );
            }
            "clear" => {
                assert_eq!(actor.clear(), 1);
                assert_eq!(
                    reservation.wait_ready().await,
                    Err(ActorError::PolicyCancelled)
                );
            }
            "close" => {
                actor.close();
                assert_eq!(reservation.wait_ready().await, Err(ActorError::Closed));
            }
            _ => unreachable!(),
        }
        if action != "close" {
            successor.wait().await.unwrap();
        }
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        actor.close();
        task.await;
    });
}

#[test_case::test_case("resolve"; "input_fixed_before_readiness")]
#[test_case::test_case("clear"; "clear_preserves_turn_intent")]
#[test_case::test_case("cancel_existing"; "cancel_preserves_turn_intent")]
fn prepared_turn_input_replacement_keeps_intent(action: &str) {
    const PLACEHOLDER: &str = "pending approved content";
    const APPROVED: &str = "captured approved content";
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let reservation = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(PLACEHOLDER),
                event_sender: None,
                correlation: PLACEHOLDER.into(),
            }))
            .unwrap();
        reservation.replace_turn_input(input(APPROVED)).unwrap();
        {
            let state = actor.inner.state.lock().unwrap();
            assert!(
                matches!(state.operations.front(), Some(super::super::ActorOperation::Prepared(pending)) if pending.turn.as_ref().unwrap().input.message == APPROVED)
            );
        }
        match action {
            "clear" => {
                assert_eq!(actor.clear(), 1);
            }
            "cancel_existing" => actor.cancel_existing(),
            "resolve" => {
                reservation.resolve(None).unwrap();
                assert_eq!(
                    reservation.replace_turn_input(input(PLACEHOLDER)),
                    Err(ActorError::PolicyPending)
                );
                reservation.wait_ready().await.unwrap();
                reservation.commit().unwrap().ticket.unwrap().wait().await;
                actor.close();
                task.await;
                return;
            }
            _ => unreachable!(),
        }
        assert_eq!(
            reservation.replace_turn_input(input(PLACEHOLDER)),
            Err(ActorError::PolicyCancelled)
        );
        let snapshot = actor.reserve_prepared_operation(None).unwrap();
        assert_eq!(
            snapshot.replace_turn_input(input(APPROVED)),
            Err(ActorError::PolicyCancelled)
        );
        drop(snapshot);
        actor.close();
        task.await;
    });
}

#[test]
fn prepared_config_waits_for_commit_and_uses_fifo_predecessor() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let first = actor.reserve_config_update().unwrap();
        let prepared = actor.reserve_prepared_operation(None).unwrap();
        prepared
            .resolve(Some(ConfigChange::ToggleWorkflow))
            .unwrap();
        first.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        let predecessor = first.wait().await.unwrap();
        prepared.wait_ready().await.unwrap();
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            predecessor.generation
        );
        assert_eq!(
            actor.effective_config().unwrap().workflow,
            predecessor.config.workflow
        );
        let result = prepared.commit().unwrap();
        assert_ne!(result.config.config.workflow, predecessor.config.workflow);
        assert_eq!(result.config.generation, predecessor.generation + 1);
        assert!(result.ticket.is_none());
        let noop = actor.reserve_prepared_operation(None).unwrap();
        noop.resolve(None).unwrap();
        noop.wait_ready().await.unwrap();
        assert_eq!(
            noop.commit().unwrap().config.generation,
            result.config.generation
        );
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false; "readiness_failure")]
#[test_case::test_case(true; "cancel_during_readiness")]
fn prepared_readiness_never_publishes_before_commit(cancel: bool) {
    const WORK: &str = "prepared";
    const FAILURE: &str = "readiness failed";
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        backend.preparation = Some(Arc::new(|_, _, _| empty_snapshot()));
        backend.readiness = Some(Arc::new(move |_| {
            let entered = entered_tx.clone();
            let release = release_rx.clone();
            Box::pin(async move {
                entered.send(()).unwrap();
                let _ = release.recv_async().await;
                Err(ActorError::InvalidConfig(FAILURE.into()))
            })
        }));
        let (actor, task) = spawn(backend);
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let before = actor.config_snapshot().unwrap();
        let reservation = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        reservation
            .resolve(Some(ConfigChange::ToggleWorkflow))
            .unwrap();
        entered_rx.recv_async().await.unwrap();
        assert_eq!(actor.snapshot().queued, 1);
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            before.generation
        );
        if cancel {
            reservation.cancel();
        }
        let _ = release_tx.send(());
        let error = reservation.wait_ready().await.unwrap_err();
        assert_eq!(
            error,
            if cancel {
                ActorError::PolicyCancelled
            } else {
                ActorError::InvalidConfig(FAILURE.into())
            }
        );
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            before.generation
        );
        assert_eq!(actor.snapshot().queued, 0);
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false, "cancel")]
#[test_case::test_case(true, "cancel")]
#[test_case::test_case(true, "clear")]
#[test_case::test_case(true, "cancel_existing")]
#[test_case::test_case(false, "close")]
#[test_case::test_case(true, "close")]
#[test_case::test_case(false, "shutdown")]
#[test_case::test_case(true, "shutdown")]
#[test_case::test_case(false, "drop")]
#[test_case::test_case(true, "drop")]
fn prepared_ready_retirement_settles_snapshot_and_turn(turn: bool, action: &str) {
    const WORK: &str = "ready reservation";
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let before = actor.config_snapshot().unwrap();
        let reservation = actor
            .reserve_prepared_operation(turn.then(|| crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        reservation
            .resolve(Some(ConfigChange::ToggleWorkflow))
            .unwrap();
        reservation.wait_ready().await.unwrap();
        assert_eq!(actor.snapshot().queued, usize::from(turn));
        assert_eq!(actor.snapshot().queue.len(), usize::from(turn));
        let mut published = false;
        actor.publish_if_empty(|| published = true);
        assert!(!published);
        let candidate = reservation.ready_config().unwrap();
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            before.generation
        );
        assert_ne!(candidate.workflow, before.config.workflow);
        if action == "drop" {
            drop(reservation);
        } else {
            match action {
                "cancel" => reservation.cancel(),
                "clear" => {
                    assert_eq!(actor.clear(), usize::from(turn));
                }
                "cancel_existing" => actor.cancel_existing(),
                "close" => actor.close(),
                "shutdown" => actor.shutdown(),
                _ => unreachable!(),
            }
            let expected = match action {
                "close" => ActorError::Closed,
                "shutdown" => ActorError::Shutdown,
                _ => ActorError::PolicyCancelled,
            };
            assert_eq!(reservation.wait_ready().await, Err(expected.clone()));
            assert!(matches!(reservation.ready_config(), Err(error) if error == expected));
            assert!(matches!(reservation.commit(), Err(error) if error == expected));
        }
        assert_eq!(actor.snapshot().queued, 0);
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            before.generation
        );
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        actor.publish_if_empty(|| published = true);
        assert!(published);
        if !matches!(action, "close" | "shutdown") {
            let next = actor.reserve_prepared_operation(None).unwrap();
            next.resolve(None).unwrap();
            next.wait_ready().await.unwrap();
            next.commit().unwrap();
        }
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false, 0; "clear_unresolved")]
#[test_case::test_case(false, 1; "clear_preparing")]
#[test_case::test_case(false, 2; "clear_ready")]
#[test_case::test_case(true, 0; "cancel_unresolved")]
#[test_case::test_case(true, 1; "cancel_preparing")]
#[test_case::test_case(true, 2; "cancel_ready")]
fn prepared_snapshot_survives_work_cancellation(cancel: bool, phase: u8) {
    const WORK: &str = "cancelled successor";
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let before = actor.config_snapshot().unwrap();
        let snapshot = actor.reserve_prepared_operation(None).unwrap();
        if phase > 0 {
            snapshot
                .resolve(Some(ConfigChange::ToggleWorkflow))
                .unwrap();
        }
        if phase == 2 {
            snapshot.wait_ready().await.unwrap();
        }
        let turn = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        let setter = actor.reserve_config_update().unwrap();
        setter.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        if cancel {
            actor.cancel_existing();
        } else {
            assert_eq!(actor.clear(), 1);
        }
        assert_eq!(turn.wait_ready().await, Err(ActorError::PolicyCancelled));
        assert_eq!(actor.snapshot().queued, 0);
        assert!(actor.snapshot().queue.is_empty());
        let mut published = false;
        actor.publish_if_empty(|| published = true);
        assert!(!published);
        assert_eq!(
            actor.config_snapshot().unwrap().generation,
            before.generation
        );
        if phase == 0 {
            snapshot
                .resolve(Some(ConfigChange::ToggleWorkflow))
                .unwrap();
        }
        snapshot.wait_ready().await.unwrap();
        assert_ne!(
            snapshot.ready_config().unwrap().workflow,
            before.config.workflow
        );
        let committed = snapshot.commit().unwrap();
        assert_eq!(committed.config.generation, before.generation + 1);
        let after = setter.wait().await.unwrap();
        assert_eq!(after.generation, before.generation + 2);
        assert_eq!(after.config.workflow, before.config.workflow);
        actor.publish_if_empty(|| published = true);
        assert!(published);
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false; "cancel_wins")]
#[test_case::test_case(true; "commit_wins")]
fn prepared_commit_and_correlation_cancel_are_serialized(commit_first: bool) {
    const WORK: &str = "racing prepared turn";
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let before = actor.config_snapshot().unwrap();
        let reservation = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        reservation
            .resolve(Some(ConfigChange::ToggleWorkflow))
            .unwrap();
        reservation.wait_ready().await.unwrap();
        let (commit_go, commit_wait) = flume::bounded(1);
        let (cancel_go, cancel_wait) = flume::bounded(1);
        let (done_tx, done_rx) = flume::bounded(1);
        let (result_tx, result_rx) = flume::bounded(1);
        std::thread::scope(|scope| {
            scope.spawn(move || {
                commit_wait.recv().unwrap();
                result_tx.send(reservation.commit()).unwrap();
            });
            scope.spawn(|| {
                cancel_wait.recv().unwrap();
                actor.cancel_correlation(WORK, TurnCancellationReason::User);
                done_tx.send(()).unwrap();
            });
            let result = if commit_first {
                commit_go.send(()).unwrap();
                let result = result_rx.recv().unwrap();
                cancel_go.send(()).unwrap();
                done_rx.recv().unwrap();
                result
            } else {
                cancel_go.send(()).unwrap();
                done_rx.recv().unwrap();
                commit_go.send(()).unwrap();
                result_rx.recv().unwrap()
            };
            if commit_first {
                assert_eq!(result.unwrap().config.generation, before.generation + 1);
            } else {
                assert!(matches!(result, Err(ActorError::PolicyCancelled)));
                assert_eq!(
                    actor.config_snapshot().unwrap().generation,
                    before.generation
                );
                assert!(actor.inner.tickets.lock().unwrap().is_empty());
            }
        });
        actor.close();
        task.await;
    });
}

#[test]
fn prepared_raw_and_visible_removal_keep_snapshot_reservation() {
    const WORK: &str = "raw prepared turn";
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let snapshot = actor.reserve_prepared_operation(None).unwrap();
        let turn = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        actor.push_compact(1, None).unwrap();
        assert_eq!(
            actor.snapshot().queue,
            [
                QueueProjection::Turn(WORK.into()),
                QueueProjection::Compact(None)
            ]
        );
        assert_eq!(
            actor.remove_visible_at(0),
            Some(QueueProjection::Compact(None))
        );
        assert_eq!(actor.remove_at(0), Some(QueueProjection::Turn(WORK.into())));
        assert_eq!(turn.wait_ready().await, Err(ActorError::PolicyCancelled));
        assert!(actor.remove_at(0).is_none());
        snapshot.resolve(None).unwrap();
        snapshot.wait_ready().await.unwrap();
        snapshot.commit().unwrap();
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false; "blocking_callback")]
#[test_case::test_case(true; "readiness_callback")]
fn prepared_callback_panic_settles_and_releases_successor(readiness: bool) {
    const WORK: &str = "panicking prepared turn";
    const PANIC: &str = "test preparation panic";
    smol::block_on(async {
        let mut backend = ScriptedBackend::new();
        backend.preparation = Some(Arc::new(move |_, _, _| {
            assert!(readiness, "{PANIC}");
            empty_snapshot()
        }));
        backend.readiness = Some(Arc::new(|_| Box::pin(async { panic!("{PANIC}") })));
        let (actor, task) = spawn(backend);
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let reservation = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: None,
                correlation: WORK.into(),
            }))
            .unwrap();
        let successor = actor.reserve_config_update().unwrap();
        successor.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        reservation.resolve(None).unwrap();
        assert!(matches!(
            reservation.wait_ready().await,
            Err(ActorError::InvalidConfig(_))
        ));
        successor.wait().await.unwrap();
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        actor.close();
        task.await;
    });
}

#[test]
fn prepared_late_async_readiness_is_disposed_without_replacing_successor() {
    const WORK: &str = "blocked readiness turn";
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let (returned_tx, returned_rx) = flume::bounded(1);
        let (disposed_tx, disposed_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        backend.preparation = Some(Arc::new(|_, _, _| empty_snapshot()));
        backend.readiness = Some(Arc::new(move |snapshot| {
            let entered = entered_tx.clone();
            let release = release_rx.clone();
            let returned = returned_tx.clone();
            let disposed = disposed_tx.clone();
            let (result_tx, result_rx) = flume::bounded(0);
            let (readiness_dropped, readiness_disposed) = CancelToken::new();
            smol::spawn(async move {
                entered.send(readiness_disposed).unwrap();
                release.recv_async().await.unwrap();
                returned.send(()).unwrap();
                disposed
                    .send(result_tx.send_async(Ok(snapshot)).await.is_err())
                    .unwrap();
            })
            .detach();
            Box::pin(async move {
                let _readiness_dropped = readiness_dropped;
                result_rx.recv_async().await.unwrap()
            })
        }));
        let (actor, task) = spawn(backend);
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let before = actor.config_snapshot().unwrap();
        let (events_tx, events_rx) = flume::unbounded();
        let reservation = actor
            .reserve_prepared_operation(Some(crate::actor::PreparedTurn {
                input: input(WORK),
                event_sender: Some(EventSender::new(events_tx, 0)),
                correlation: WORK.into(),
            }))
            .unwrap();
        let successor = actor.reserve_config_update().unwrap();
        successor.resolve(Ok(ConfigChange::ToggleFast)).unwrap();
        reservation
            .resolve(Some(ConfigChange::ToggleWorkflow))
            .unwrap();
        let readiness_disposed = entered_rx.recv_async().await.unwrap();
        assert_eq!(actor.snapshot().queued, 1);
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        let mut waiting = Box::pin(reservation.wait_ready());
        assert!(smol::future::poll_once(waiting.as_mut()).await.is_none());
        reservation.cancel();
        let expected = ActorError::PolicyCancelled;
        assert_eq!(waiting.await, Err(expected.clone()));
        let committed = successor.wait().await.unwrap();
        assert_eq!(committed.generation, before.generation + 1);
        assert_ne!(committed.config.fast, before.config.fast);
        assert_eq!(committed.config.workflow, before.config.workflow);
        assert!(returned_rx.try_recv().is_err());
        readiness_disposed.cancelled().await;
        release_tx.send(()).unwrap();
        returned_rx.recv_async().await.unwrap();
        assert!(disposed_rx.recv_async().await.unwrap());
        assert_eq!(reservation.wait_ready().await, Err(expected.clone()));
        assert!(matches!(reservation.ready_config(), Err(error) if error == expected));
        assert!(matches!(reservation.commit(), Err(error) if error == expected));
        let after = actor.config_snapshot().unwrap();
        assert_eq!(after.generation, committed.generation);
        assert!(Arc::ptr_eq(&after.config, &committed.config));
        assert_eq!(actor.snapshot().queued, 0);
        assert!(actor.snapshot().queue.is_empty());
        assert!(actor.inner.tickets.lock().unwrap().is_empty());
        assert!(actor.inner.outcomes.lock().unwrap().is_empty());
        assert!(observed.runs.lock().unwrap().is_empty());
        assert!(events_rx.try_recv().is_err());
        let mut published = false;
        actor.publish_if_empty(|| published = true);
        assert!(published);
        actor.close();
        task.await;
    });
}

fn empty_snapshot() -> crate::agent::TurnAdmissionSnapshot {
    crate::agent::TurnAdmissionSnapshot {
        mode_def: None,
        prompt_inputs: None,
        bindings: Arc::default(),
        mcp_startup_notice: None,
    }
}

#[test_case::test_case(false; "turn_preparation")]
#[test_case::test_case(true; "root_preparation")]
fn control_waits_for_earlier_preparation(root: bool) {
    const WORK: &str = "work";
    const CONTROL: &str = "control";
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let backend_gate = super::Gate::new();
        let mut backend = ScriptedBackend::gated(Arc::clone(&backend_gate));
        let observed = Arc::clone(&backend.state);
        backend.preparation = Some(Arc::new(move |_, _, _| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            empty_snapshot()
        }));
        let (actor, task) = spawn(backend);
        if root {
            actor
                .rush(crate::RootWork::new(
                    input(WORK),
                    1,
                    false,
                    WORK.into(),
                    Vec::new(),
                    WORK.into(),
                ))
                .unwrap();
        } else {
            actor.admit_turn(input(WORK), None, WORK.into()).unwrap();
        }
        entered_rx.recv_async().await.unwrap();
        actor
            .push_control(ControlWork {
                name: CONTROL.into(),
                correlation: CONTROL.into(),
            })
            .unwrap();
        assert!(actor.inner.queue.is_empty());
        let snapshot = actor.snapshot();
        assert_eq!(snapshot.queue.len(), 2);
        assert_eq!(snapshot.queue[1], QueueProjection::Control(CONTROL.into()));
        assert_eq!(snapshot.queued, 1);
        let (popped, release_pop) = actor.pause_after_next_pop();
        release_tx.send(()).unwrap();
        popped.recv_async().await.unwrap();
        assert_eq!(
            actor.snapshot().queue,
            [QueueProjection::Control(CONTROL.into())]
        );
        release_pop.send(()).unwrap();
        until(|| !observed.runs.lock().unwrap().is_empty()).await;
        assert!(observed.controls.lock().unwrap().is_empty());
        backend_gate.open();
        until(|| !observed.controls.lock().unwrap().is_empty()).await;
        assert_eq!(observed.controls.lock().unwrap().as_slice(), [CONTROL]);
        actor.close();
        task.await;
    });
}

#[test_case::test_case("remove")]
#[test_case::test_case("clear")]
#[test_case::test_case("cancel_existing")]
#[test_case::test_case("close")]
#[test_case::test_case("shutdown")]
#[test_case::test_case("cancel_turn")]
#[test_case::test_case("cancel_correlation")]
fn deferred_control_queue_operations(action: &str) {
    const WORK: &str = "work";
    const CONTROL: &str = "control";
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        backend.preparation = Some(Arc::new(move |_, _, _| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            empty_snapshot()
        }));
        let (actor, task) = spawn(backend);
        let ticket = actor.admit_turn(input(WORK), None, WORK.into()).unwrap();
        entered_rx.recv_async().await.unwrap();
        let control = || ControlWork {
            name: CONTROL.into(),
            correlation: WORK.into(),
        };
        actor.push_control(control()).unwrap();
        assert!(actor.inner.queue.is_empty());
        assert_eq!(actor.remove_visible_at(0), None);
        let (stale_tx, stale_rx) = flume::bounded(1);
        *actor.inner.stale_preparation.lock().unwrap() = Some(stale_tx);
        match action {
            "remove" => {
                assert_eq!(
                    actor.remove_at(1),
                    Some(QueueProjection::Control(CONTROL.into()))
                );
                actor.cancel_turn(ticket.turn_id()).unwrap();
            }
            "clear" => assert_eq!(actor.clear(), 2),
            "cancel_existing" => actor.cancel_existing(),
            "close" => {
                actor.close();
                assert_eq!(actor.push_control(control()), Err(ActorError::Closed));
            }
            "shutdown" => {
                actor.shutdown();
                assert_eq!(actor.push_control(control()), Err(ActorError::Shutdown));
            }
            "cancel_turn" => actor.cancel_turn(ticket.turn_id()).unwrap(),
            "cancel_correlation" => actor.cancel_correlation(WORK, TurnCancellationReason::User),
            _ => unreachable!(),
        }
        assert!(matches!(ticket.wait().await, TurnOutcome::Cancelled { .. }));
        release_tx.send(()).unwrap();
        stale_rx.recv_async().await.unwrap();
        if matches!(action, "cancel_turn" | "cancel_correlation") {
            until(|| !observed.controls.lock().unwrap().is_empty()).await;
            assert_eq!(observed.controls.lock().unwrap().as_slice(), [CONTROL]);
        } else {
            assert!(observed.controls.lock().unwrap().is_empty());
        }
        assert!(actor.snapshot().queue.is_empty());
        assert!(observed.runs.lock().unwrap().is_empty());
        actor.close();
        task.await;
    });
}

#[test_case::test_case(false; "config_cannot_overtake_turn_preparation")]
#[test_case::test_case(true; "config_cannot_overtake_rush_preparation")]
fn acceptance_does_not_run_blocking_preparation_inline(root: bool) {
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        let observed = Arc::clone(&backend.state);
        backend.preparation = Some(Arc::new(move |input, _, config| {
            assert!(!input.workflow);
            assert!(!config.unwrap().workflow);
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            empty_snapshot()
        }));
        let (actor, task) = spawn(backend);
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let ticket = if root {
            actor
                .rush(crate::RootWork::new(
                    input("work"),
                    1,
                    false,
                    "work".into(),
                    Vec::new(),
                    "work".into(),
                ))
                .unwrap();
            None
        } else {
            Some(
                actor
                    .admit_turn(input("work"), None, "work".into())
                    .unwrap(),
            )
        };
        entered_rx.recv_async().await.unwrap();
        let change = actor.reserve_config_update().unwrap();
        change.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        assert!(!actor.effective_config().unwrap().workflow);
        release_tx.send(()).unwrap();
        assert!(change.wait().await.unwrap().config.workflow);
        if let Some(ticket) = ticket {
            ticket.wait().await;
        } else {
            super::until(|| observed.entered.load(std::sync::atomic::Ordering::SeqCst) == 1).await;
        }
        assert_eq!(observed.policies.lock().unwrap()[0].2, 0);
        actor.close();
        task.await;
    });
}

#[test]
fn root_preparation_failure_is_visible_once_and_successors_progress() {
    const RUN_ID: u64 = 17;
    const FAILURE: &str = "The agent could not start this turn: admission preparation failed.";
    smol::block_on(async {
        let (sender, events) = flume::unbounded();
        let mut backend = ScriptedBackend::new();
        backend.root_preparation_error = Some(Arc::new(move |run_id, message| {
            EventSender::new(sender.clone(), run_id)
                .send(crate::AgentEvent::ControlError { message })
                .unwrap();
        }));
        backend.preparation = Some(Arc::new(|input, _, _| {
            assert_ne!(input.message, "panic");
            empty_snapshot()
        }));
        let (actor, task) = spawn(backend);
        actor
            .rush(crate::RootWork::new(
                input("panic"),
                RUN_ID,
                false,
                "panic".into(),
                Vec::new(),
                "root".into(),
            ))
            .unwrap();
        let next = actor
            .admit_turn(input("next"), None, "next".into())
            .unwrap();
        let error = events.recv_async().await.unwrap();
        assert_eq!(error.run_id, RUN_ID);
        assert!(
            matches!(error.event, crate::AgentEvent::ControlError { message } if message == FAILURE)
        );
        assert!(matches!(next.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(actor.inner.outcomes.lock().unwrap().len(), 1);
        actor.close();
        task.await;
        assert!(events.is_empty());
    });
}

#[test]
fn preparation_panic_releases_successors() {
    smol::block_on(async {
        let mut backend = ScriptedBackend::new();
        backend.preparation = Some(Arc::new(|input, _, _| {
            assert_ne!(input.message, "panic");
            empty_snapshot()
        }));
        let (actor, task) = spawn(backend);
        let failed = actor
            .admit_turn(input("panic"), None, "panic".into())
            .unwrap();
        let next = actor
            .admit_turn(input("next"), None, "next".into())
            .unwrap();
        assert!(matches!(failed.wait().await, TurnOutcome::Failed { .. }));
        assert!(matches!(next.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(actor.inner.outcomes.lock().unwrap().len(), 2);
        actor.close();
        task.await;
    });
}

#[test]
fn close_settles_fifo() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let first = actor.reserve_config_update().unwrap();
        let turn = actor
            .admit_turn(input("queued"), None, "queued".into())
            .unwrap();
        let second = actor.reserve_config_update().unwrap();
        second.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        actor.close();
        assert!(matches!(first.wait().await, Err(ActorError::Closed)));
        assert!(matches!(second.wait().await, Err(ActorError::Closed)));
        assert!(matches!(turn.wait().await, TurnOutcome::Cancelled { .. }));
        assert!(!actor.effective_config().unwrap().workflow);
        task.await;
    });
}

#[test]
fn config_changes_compose_fifo() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let first = actor.reserve_config_update().unwrap();
        let second = actor.reserve_config_update().unwrap();
        second
            .resolve(Ok(ConfigChange::Mode {
                mode: AgentMode::Plan("plan.md".into()),
                mode_def: Some(
                    crate::modes::ModeRegistry::builtin()
                        .current(&AgentMode::Plan("plan.md".into())),
                ),
            }))
            .unwrap();
        first
            .resolve(Ok(ConfigChange::Patch(ConfigPatch {
                workflow: Some(true),
                ..Default::default()
            })))
            .unwrap();
        let committed = second.wait().await.unwrap();
        assert!(committed.config.workflow);
        assert_eq!(committed.config.mode, AgentMode::Plan("plan.md".into()));
        assert_eq!(committed.generation, 2);
        actor.close();
        task.await;
    });
}

#[test]
fn config_noop_preserves_generation() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let reservation = actor.reserve_config_update().unwrap();
        reservation
            .resolve(Ok(ConfigChange::Patch(ConfigPatch::default())))
            .unwrap();
        assert_eq!(reservation.wait().await.unwrap().generation, 0);
        actor.close();
        task.await;
    });
}

#[test]
fn settings_change_preserves_mode_definition() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        let definition = crate::modes::ModeRegistry::builtin().current(&AgentMode::Build);
        actor
            .initialize_config(
                EffectiveAgentConfig::new(policy(false), AgentMode::Build)
                    .with_mode_def(Some(definition.clone())),
            )
            .unwrap();
        let change = actor.reserve_config_update().unwrap();
        change.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        assert_eq!(
            change.wait().await.unwrap().config.mode_def.as_ref(),
            Some(&definition)
        );
        actor.close();
        task.await;
    });
}

#[test]
fn expired_preparation_cannot_commit_late() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let reservation = actor.reserve_config_update().unwrap();
        assert!(reservation.expire().unwrap().is_none());
        assert!(matches!(
            reservation.resolve(Ok(ConfigChange::ToggleWorkflow)),
            Err(ActorError::ConfigExpired)
        ));
        assert!(matches!(
            reservation.wait().await,
            Err(ActorError::ConfigExpired)
        ));
        assert!(!actor.effective_config().unwrap().workflow);
        actor.close();
        task.await;
    });
}

#[test]
fn commit_wins_cancel_race() {
    smol::block_on(async {
        let (actor, task) = spawn(ScriptedBackend::new());
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let reservation = actor.reserve_config_update().unwrap();
        reservation
            .resolve(Ok(ConfigChange::ToggleWorkflow))
            .unwrap();
        let committed = reservation.cancel().unwrap().unwrap();
        assert!(committed.config.workflow);
        actor.close();
        assert_eq!(
            reservation.wait().await.unwrap().generation,
            committed.generation
        );
        task.await;
    });
}

#[test]
fn cancelled_preparation_does_not_block_successors() {
    smol::block_on(async {
        let (entered_tx, entered_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::bounded(1);
        let (successor_tx, successor_rx) = flume::bounded(1);
        let (release_successor_tx, release_successor_rx) = flume::bounded(1);
        let mut backend = ScriptedBackend::new();
        backend.preparation = Some(Arc::new(move |input, _, _| {
            if input.message == "blocked" {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            } else {
                successor_tx.send(()).unwrap();
                release_successor_rx.recv().unwrap();
            }
            crate::agent::TurnAdmissionSnapshot {
                mode_def: None,
                prompt_inputs: None,
                bindings: Arc::default(),
                mcp_startup_notice: None,
            }
        }));
        let (actor, task) = spawn(backend);
        actor
            .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
            .unwrap();
        let blocked = actor
            .admit_turn(input("blocked"), None, "blocked".into())
            .unwrap();
        entered_rx.recv_async().await.unwrap();
        let config = actor.reserve_config_update().unwrap();
        config.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        actor.cancel_turn(blocked.turn_id()).unwrap();
        assert!(config.wait().await.unwrap().config.workflow);
        let next = actor
            .admit_turn(input("next"), None, "next".into())
            .unwrap();
        successor_rx.recv_async().await.unwrap();
        let owner = actor.inner.state.lock().unwrap().preparing;
        let later = actor.reserve_config_update().unwrap();
        later.resolve(Ok(ConfigChange::ToggleWorkflow)).unwrap();
        let (stale_tx, stale_rx) = flume::bounded(1);
        *actor.inner.stale_preparation.lock().unwrap() = Some(stale_tx);
        release_tx.send(()).unwrap();
        stale_rx.recv_async().await.unwrap();
        assert_eq!(actor.inner.state.lock().unwrap().preparing, owner);
        assert!(actor.effective_config().unwrap().workflow);
        assert!(next.peek().is_none());
        release_successor_tx.send(()).unwrap();
        assert!(matches!(next.wait().await, TurnOutcome::Completed { .. }));
        assert!(!later.wait().await.unwrap().config.workflow);
        assert!(matches!(
            blocked.wait().await,
            TurnOutcome::Cancelled { .. }
        ));
        assert_eq!(actor.inner.outcomes.lock().unwrap().len(), 2);
        actor.close();
        task.await;
    });
}
