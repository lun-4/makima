use std::sync::Arc;

use maki_agent::actor::AgentActorHandle;
use maki_agent::{AgentId, AgentManagerHandle, CancelMap, CancelTrigger, TurnCancellationReason};

use super::AgentCommand;
use super::shared_queue::correlation;

/// Routes commands from the UI to the actor's cancellation APIs.
///
/// `AgentCommand::Cancel { run_id }` permanently closes managed descendants and
/// cancels only the matching root run, leaving the root actor reusable.
/// `CancelAll` cancels the manager's entire root subtree, compatibility
/// subagents outside that graph, and startup so an early command aborts MCP
/// readiness.
pub(super) fn spawn_command_router(
    cmd_rx: flume::Receiver<AgentCommand>,
    actor: Arc<AgentActorHandle>,
    manager: AgentManagerHandle,
    root_id: AgentId,
    subagent_cancels: Arc<CancelMap<String>>,
    init_trigger: CancelTrigger,
) {
    // `CancelTrigger` is single-fire and not `Clone`: one startup trigger is
    // consumed by the first CancelAll (aborting MCP readiness) while later
    // CancelAll calls still cancel managed and compatibility subagents.
    let mut init_trigger = Some(init_trigger);
    smol::spawn(async move {
        while let Ok(cmd) = cmd_rx.recv_async().await {
            match cmd {
                AgentCommand::Cancel { run_id } => {
                    let correlation = correlation(run_id);
                    actor.cancel_correlation_with_active(
                        &correlation,
                        TurnCancellationReason::User,
                        |turn_id| {
                            let _ = manager.close_descendants_for_turn(root_id, turn_id);
                        },
                    );
                }
                AgentCommand::CancelAll => {
                    if let Some(trigger) = init_trigger.take() {
                        trigger.cancel();
                    }
                    let _ = manager.cancel_subtree(root_id);
                    subagent_cancels.cancel_all();
                }
                AgentCommand::CancelSubagent { tool_use_id } => {
                    subagent_cancels.cancel_or_precancel(tool_use_id);
                }
            }
        }
    })
    .detach();
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;

    use maki_agent::{
        ActorBackend, ActorError, ActorLifecycle, AgentEvent, AgentInput, AgentLimits,
        AgentMetadata, AgentMode, BackendResult, ControlWork, DoneReason, EventSender,
        GraphLifecycle, History, ManagerError, TurnContext, TurnOutcome, WorkKind,
    };
    use maki_providers::TokenUsage;

    use super::*;

    struct CancellableBackend {
        current: Option<flume::Sender<maki_agent::CurrentManagedTurn>>,
        entered: Option<flume::Sender<()>>,
        event_tx: Option<EventSender>,
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
                if let Some(current) = &self.current {
                    current.send(context.managed_turn.clone().unwrap()).unwrap();
                }
                if let Some(entered) = &self.entered {
                    entered.send(()).unwrap();
                }
                if let Some(event_tx) = &self.event_tx {
                    event_tx
                        .send(AgentEvent::TextDelta {
                            text: "active child".into(),
                        })
                        .unwrap();
                }
                let reason = context.cancel_reason.cancelled().await;
                BackendResult::EnteredRun(TurnOutcome::Cancelled {
                    agent_id: context.agent_id,
                    turn_id: context.turn_id.unwrap(),
                    usage: TokenUsage::default(),
                    num_turns: 0,
                    reason,
                })
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
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::CompactDone })
        }
    }

    struct CancelThenCompleteBackend {
        currents: flume::Sender<maki_agent::CurrentManagedTurn>,
        later_release: flume::Receiver<()>,
    }

    impl ActorBackend for CancelThenCompleteBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut History,
            context: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                self.currents
                    .send(context.managed_turn.clone().unwrap())
                    .unwrap();
                if context.correlation == correlation(1) {
                    let reason = context.cancel_reason.cancelled().await;
                    BackendResult::EnteredRun(TurnOutcome::Cancelled {
                        agent_id: context.agent_id,
                        turn_id: context.turn_id.unwrap(),
                        usage: TokenUsage::default(),
                        num_turns: 0,
                        reason,
                    })
                } else {
                    self.later_release.recv_async().await.unwrap();
                    BackendResult::EnteredRun(TurnOutcome::Completed {
                        agent_id: context.agent_id,
                        turn_id: context.turn_id.unwrap(),
                        usage: TokenUsage::default(),
                        num_turns: 1,
                        reason: DoneReason::EndTurn,
                    })
                }
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
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::CompactDone })
        }
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
        }
    }

    #[test]
    fn cancel_closes_managed_descendants_and_preserves_root_reuse() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let (current_tx, current_rx) = flume::bounded(2);
            let (later_release_tx, later_release_rx) = flume::bounded(1);
            let root = manager
                .create_root(
                    Vec::new(),
                    None,
                    Box::new(CancelThenCompleteBackend {
                        currents: current_tx,
                        later_release: later_release_rx,
                    }),
                )
                .unwrap();
            let root_actor = root.actor().unwrap();
            let root_ticket = root_actor
                .admit_turn(input(), None, correlation(1))
                .unwrap();
            let current = current_rx.recv_async().await.unwrap();

            let (event_tx, event_rx) = flume::unbounded();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let child = manager
                .spawn_child(
                    &current,
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    Box::new(CancellableBackend {
                        current: None,
                        entered: Some(entered_tx),
                        event_tx: Some(EventSender::new(event_tx, 1)),
                    }),
                )
                .unwrap();
            let child_actor = child.actor().unwrap();
            let child_ticket = child_actor
                .admit_turn(input(), None, "child-active".into())
                .unwrap();
            entered_rx.recv_async().await.unwrap();
            assert!(matches!(
                event_rx.recv_async().await.unwrap().event,
                AgentEvent::TextDelta { .. }
            ));

            let (cmd_tx, cmd_rx) = flume::unbounded();
            let (init_trigger, _) = maki_agent::CancelToken::new();
            spawn_command_router(
                cmd_rx,
                Arc::new(root_actor.clone()),
                manager.clone(),
                root.id(),
                Arc::new(CancelMap::new()),
                init_trigger,
            );
            cmd_tx
                .send_async(AgentCommand::Cancel { run_id: 1 })
                .await
                .unwrap();

            assert!(matches!(
                child_ticket.wait().await,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::Closed,
                    ..
                }
            ));
            while !manager.runner_finished(child.id()).unwrap() {
                smol::future::yield_now().await;
            }
            assert_eq!(
                child.snapshot().unwrap().graph_lifecycle,
                GraphLifecycle::Closed
            );
            assert!(matches!(
                manager.actor(child.id()),
                Err(ManagerError::NonLiveAgent(id)) if id == child.id()
            ));
            assert!(matches!(
                child_actor.admit_turn(input(), None, "retained-task".into()),
                Err(ActorError::Closed)
            ));
            assert!(matches!(
                root_ticket.wait().await,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::User,
                    ..
                }
            ));
            assert_eq!(root_actor.snapshot().lifecycle, ActorLifecycle::Open);

            let later = root_actor
                .admit_turn(input(), None, correlation(2))
                .unwrap();
            current_rx.recv_async().await.unwrap();
            later_release_tx.send_async(()).await.unwrap();
            assert!(matches!(later.wait().await, TurnOutcome::Completed { .. }));
            assert_eq!(root_actor.snapshot().lifecycle, ActorLifecycle::Open);

            drop(cmd_tx);
            let report = manager.shutdown(Duration::from_secs(1)).await;
            assert!(report.timed_out.is_empty());
        });
    }

    #[test]
    fn cancel_all_cancels_active_and_queued_managed_child_turns() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let (current_tx, current_rx) = flume::bounded(1);
            let root = manager
                .create_root(
                    Vec::new(),
                    None,
                    Box::new(CancellableBackend {
                        current: Some(current_tx),
                        entered: None,
                        event_tx: None,
                    }),
                )
                .unwrap();
            let root_actor = root.actor().unwrap();
            let root_ticket = root_actor.admit_turn(input(), None, "root".into()).unwrap();
            let current = current_rx.recv_async().await.unwrap();

            let (entered_tx, entered_rx) = flume::bounded(1);
            let child = manager
                .spawn_child(
                    &current,
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    Box::new(CancellableBackend {
                        current: None,
                        entered: Some(entered_tx),
                        event_tx: None,
                    }),
                )
                .unwrap();
            let child_actor = child.actor().unwrap();
            let active = child_actor
                .admit_turn(input(), None, "active".into())
                .unwrap();
            entered_rx.recv_async().await.unwrap();
            let queued = child_actor
                .admit_turn(input(), None, "queued".into())
                .unwrap();

            let (cmd_tx, cmd_rx) = flume::unbounded();
            let (init_trigger, init_cancel) = maki_agent::CancelToken::new();
            spawn_command_router(
                cmd_rx,
                Arc::new(root_actor.clone()),
                manager.clone(),
                root.id(),
                Arc::new(CancelMap::new()),
                init_trigger,
            );
            cmd_tx.send_async(AgentCommand::CancelAll).await.unwrap();
            init_cancel.cancelled().await;

            for outcome in [active.wait().await, queued.wait().await] {
                assert!(matches!(
                    outcome,
                    TurnOutcome::Cancelled {
                        reason: TurnCancellationReason::User,
                        ..
                    }
                ));
            }
            assert_eq!(child_actor.snapshot().lifecycle, ActorLifecycle::Open);
            assert!(matches!(
                root_ticket.wait().await,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::User,
                    ..
                }
            ));
            assert_eq!(root_actor.snapshot().lifecycle, ActorLifecycle::Open);

            drop(cmd_tx);
            let report = manager.shutdown(Duration::from_secs(1)).await;
            assert!(report.timed_out.is_empty());
        });
    }
}
