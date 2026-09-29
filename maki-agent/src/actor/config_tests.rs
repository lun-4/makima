use std::sync::Arc;

use super::{ScriptedBackend, input, policy, spawn};
use crate::actor::{ActorError, ConfigChange, ConfigPatch, EffectiveAgentConfig};
use crate::{AgentMode, EventSender, Instructions, TurnOutcome};

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

fn empty_snapshot() -> crate::agent::TurnAdmissionSnapshot {
    crate::agent::TurnAdmissionSnapshot {
        mode_def: None,
        prompt_inputs: None,
        bindings: Arc::default(),
        mcp_startup_notice: None,
    }
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
                mode_def: None,
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
