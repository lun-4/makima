use crate::actor::ActorIdleGuard;
use crate::{ActorLifecycle, AgentId};
use std::collections::BTreeSet;

use super::{AgentManagerHandle, AgentResidency, GraphLifecycle, ManagerError};

pub struct ManagerTransitionGuard {
    manager: AgentManagerHandle,
    actors: Vec<ActorIdleGuard>,
}

impl AgentManagerHandle {
    pub fn reserve_graph_transition(&self) -> Result<ManagerTransitionGuard, ManagerError> {
        let mut graph = self.lock_graph();
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        if graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        let root = graph.root_id.ok_or(ManagerError::MissingRoot)?;
        if !graph.active_turns.is_empty()
            || graph.nodes.values().any(|node| {
                node.reservation_pending
                    || node.detach_pending
                    || node.residency == AgentResidency::Activating
                    || matches!(
                        node.lifecycle,
                        GraphLifecycle::Reserved | GraphLifecycle::Closing
                    )
            })
        {
            return Err(ManagerError::BusySubtree(root));
        }
        let busy = graph.nodes.keys().any(|id| self.0.work.is_busy(*id));
        if busy {
            return Err(ManagerError::BusySubtree(root));
        }
        self.0.work.fence_rewind(&BTreeSet::<AgentId>::new());
        graph.rewinding = true;
        graph.rewind_quiescent = true;
        let mut guard = ManagerTransitionGuard {
            manager: self.clone(),
            actors: Vec::new(),
        };
        let result = (|| {
            for (id, node) in &graph.nodes {
                if self.0.work.is_busy(*id) {
                    return Err(ManagerError::BusySubtree(root));
                }
                if let Some(actor) = &node.actor
                    && actor.lifecycle() == ActorLifecycle::Open
                {
                    guard.actors.push(
                        actor
                            .prepare_idle()
                            .map_err(|_| ManagerError::BusySubtree(root))?,
                    );
                }
            }
            Ok(())
        })();
        drop(graph);
        result?;
        Ok(guard)
    }
}

impl Drop for ManagerTransitionGuard {
    fn drop(&mut self) {
        let mut graph = self.manager.lock_graph();
        self.manager.0.work.release_rewind();
        graph.rewinding = false;
        graph.rewind_quiescent = false;
        self.actors.clear();
        self.manager.0.reaped.notify(usize::MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        AgentLimits, AgentManagerHandle, AgentMetadata, AgentResidency, GraphLifecycle,
        ManagerError,
    };
    use crate::actor::{ActorHistory, ControlWork, TurnContext, WorkKind};
    use crate::{
        ActorBackend, ActorError, ActorLifecycle, AgentInput, AgentMode, BackendResult,
        EffectiveAgentConfig, RunSettings,
    };
    use makima_providers::{Model, provider::unconfigured};
    use makima_storage::checkpoint::CheckpointPublication;
    use makima_storage::session_state::StoredAgentLifecycle;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;
    use test_case::test_case;

    const INPUT: &str = "source work must remain queued";
    const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
    const CONDITION_TIMEOUT: &str = "transition condition did not complete";

    async fn complete<T>(future: impl Future<Output = T>) -> T {
        futures_lite::future::or(future, async {
            smol::Timer::after(COMPLETION_TIMEOUT).await;
            panic!("{CONDITION_TIMEOUT}");
        })
        .await
    }

    fn config() -> EffectiveAgentConfig {
        EffectiveAgentConfig::new(
            RunSettings {
                provider: unconfigured(),
                model: Model::from_spec("test/transition").unwrap(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
        .with_context(crate::AgentContextSnapshot::new(
            std::env::current_dir().unwrap(),
        ))
    }

    struct Backend;

    impl ActorBackend for Backend {
        fn run_turn<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { panic!("deferred source must not run") })
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

    #[test]
    fn transition_fences_admissions_and_resumes_unchanged_on_drop() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let root = manager
                .create_root(Vec::new(), None, Box::new(Backend))
                .unwrap();
            let actor = root.actor().unwrap();
            let before = manager.journal().snapshot();
            let revision = manager.lock_graph().revision;
            let guard = manager.reserve_graph_transition().unwrap();
            assert!(matches!(
                manager.reserve_graph_transition(),
                Err(ManagerError::RewindInProgress)
            ));
            assert!(
                actor
                    .submit(
                        AgentInput::new(INPUT.into(), Vec::new()),
                        None,
                        INPUT.into(),
                        Default::default()
                    )
                    .is_err()
            );
            assert!(matches!(
                manager
                    .ensure_resident_with(root.id(), |_| async { panic!("factory must not run") })
                    .await,
                Err(ManagerError::RewindInProgress)
            ));
            assert!(
                manager
                    .create_root(Vec::new(), None, Box::new(Backend))
                    .is_err()
            );
            let config = config();
            assert!(matches!(
                manager.spawn_child_trusted_with_config(
                    &root,
                    config,
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(Backend))
                ),
                Err(ManagerError::RewindInProgress)
            ));
            drop(guard);
            assert_eq!(manager.lock_graph().revision, revision);
            assert_eq!(manager.journal().snapshot().state, before.state);
            assert_eq!(
                manager.journal().snapshot().agent_revisions,
                before.agent_revisions
            );
            assert!(actor.prepare_idle().is_ok());
            let _guard = manager.reserve_graph_transition().unwrap();
        });
    }

    #[test]
    fn in_flight_activation_reservation_is_busy_and_unchanged() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let root = manager
                .create_root(Vec::new(), None, Box::new(Backend))
                .unwrap();
            let reservation = manager.lock_graph().nodes[&root.id()].reservation;
            manager
                .lock_graph()
                .nodes
                .get_mut(&root.id())
                .unwrap()
                .reservation_pending = true;
            assert!(
                matches!(manager.reserve_graph_transition(), Err(ManagerError::BusySubtree(id)) if id == root.id())
            );
            let graph = manager.lock_graph();
            assert!(graph.nodes[&root.id()].reservation_pending);
            assert_eq!(graph.nodes[&root.id()].reservation, reservation);
            assert!(!graph.rewinding);
        });
    }

    #[test]
    fn failed_idle_reservation_preserves_publications_and_reopens_admission() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let root = manager
                .create_root(Vec::new(), None, Box::new(Backend))
                .unwrap();
            let actor = root.actor().unwrap();
            let idle = actor.prepare_idle().unwrap();
            let publication = CheckpointPublication::default();
            let incarnation = manager.incarnation(root.id()).unwrap();
            let admission = manager
                .0
                .work
                .bind_incarnation(root.id(), incarnation.reservation)
                .unwrap();
            let _registration = admission.register_publication(&publication).unwrap();
            assert!(matches!(
                manager.reserve_graph_transition(),
                Err(ManagerError::BusySubtree(_))
            ));
            assert!(!publication.is_cancelled());
            assert!(admission.validate_open().is_ok());
            assert!(!manager.lock_graph().rewinding);
            drop(idle);
            let _transition = manager.reserve_graph_transition().unwrap();
        });
    }

    #[test]
    fn config_publication_blocks_transition_without_cancelling_checkpoint() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let root = manager
                .create_root(Vec::new(), None, Box::new(Backend))
                .unwrap();
            let actor = root.actor().unwrap();
            let config = actor.reserve_config_publication().unwrap();
            let publication = CheckpointPublication::default();
            let _registrations = actor.register_publication(&publication).unwrap();
            let before = manager.journal().snapshot();
            let revision = manager.lock_graph().revision;
            assert!(matches!(
                manager.reserve_graph_transition(),
                Err(ManagerError::BusySubtree(id)) if id == root.id()
            ));
            assert!(!publication.is_cancelled());
            assert!(!manager.lock_graph().rewinding);
            assert_eq!(manager.lock_graph().revision, revision);
            assert_eq!(manager.journal().snapshot().state, before.state);
            assert!(actor.has_pending_work());
            assert!(matches!(
                actor.reserve_config_publication(),
                Err(ActorError::Busy)
            ));
            drop(config);
            let _transition = manager.reserve_graph_transition().unwrap();
            assert!(!publication.is_cancelled());
        });
    }

    #[test]
    fn config_publication_release_wakes_settlement_and_runner() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let (_start, wait) = flume::bounded(1);
            let root = manager
                .create_root_deferred_with_config(
                    None,
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(Backend)),
                    wait,
                )
                .unwrap();
            let actor = root.actor().unwrap();
            let notify = actor.inner.queue.take_notify_rx();
            while notify.try_recv().is_ok() {}
            let config = actor.reserve_config_publication().unwrap();
            let mut settled = Box::pin(manager.wait_settled());
            assert!(
                futures_lite::future::poll_once(settled.as_mut())
                    .await
                    .is_none()
            );
            drop(config);
            assert!(
                futures_lite::future::poll_once(settled.as_mut())
                    .await
                    .is_some()
            );
            assert!(notify.try_recv().is_ok());
        });
    }

    #[test]
    fn dormant_source_activation_is_fenced_without_changing_incarnation() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let root = manager
                .create_root(Vec::new(), None, Box::new(Backend))
                .unwrap();
            {
                let mut graph = manager.lock_graph();
                let node = graph.nodes.get_mut(&root.id()).unwrap();
                node.residency = AgentResidency::Dormant;
                node.lifecycle = GraphLifecycle::Dormant;
            }
            let incarnation = manager.incarnation(root.id()).unwrap();
            let guard = manager.reserve_graph_transition().unwrap();
            assert!(matches!(
                manager
                    .ensure_resident_with(root.id(), |_| async {
                        panic!("dormant factory must not run")
                    })
                    .await,
                Err(ManagerError::RewindInProgress)
            ));
            drop(guard);
            assert_eq!(manager.incarnation(root.id()).unwrap(), incarnation);
            assert!(!manager.lock_graph().rewinding);
        });
    }

    #[test_case(false; "close")]
    #[test_case(true; "shutdown")]
    fn runner_completion_during_transition_reconciles_after_release(shutdown: bool) {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let root = manager
                .create_root(Vec::new(), None, Box::new(Backend))
                .unwrap();
            let actor = root.actor().unwrap();
            let incarnation = manager.incarnation(root.id()).unwrap();
            let (deferred, reached) = flume::bounded(1);
            *manager.0.runner_deferred.lock().unwrap() = Some(deferred);
            let before = manager.journal().snapshot();
            let revision = manager.lock_graph().revision;
            let guard = manager.reserve_graph_transition().unwrap();
            if shutdown {
                actor.shutdown();
            } else {
                actor.close();
            }
            complete(reached.recv_async()).await.unwrap();
            assert_eq!(
                actor.lifecycle(),
                if shutdown {
                    ActorLifecycle::Shutdown
                } else {
                    ActorLifecycle::Closed
                }
            );
            assert_eq!(manager.lock_graph().revision, revision);
            assert_eq!(manager.journal().snapshot().state, before.state);
            assert_eq!(
                manager.lock_graph().nodes[&root.id()].lifecycle,
                GraphLifecycle::Live
            );
            drop(guard);
            complete(async {
                loop {
                    let reconciled = manager.0.reaped.listen();
                    if manager.lock_graph().nodes[&root.id()].lifecycle != GraphLifecycle::Live {
                        break;
                    }
                    reconciled.await;
                }
            })
            .await;
            assert_eq!(manager.incarnation(root.id()).unwrap(), incarnation);
            let graph = manager.lock_graph();
            let node = &graph.nodes[&root.id()];
            assert_eq!(node.residency, AgentResidency::Dormant);
            assert_eq!(
                node.lifecycle,
                if shutdown {
                    GraphLifecycle::Dormant
                } else {
                    GraphLifecycle::Closed
                }
            );
            assert!(graph.revision > revision);
            assert_eq!(
                manager.journal().agent(root.id()).unwrap().lifecycle,
                if shutdown {
                    StoredAgentLifecycle::Open
                } else {
                    StoredAgentLifecycle::Closed
                }
            );
        });
    }

    #[test]
    fn busy_source_work_is_not_cancelled_or_consumed() {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let (_start, wait) = flume::bounded(1);
            let root = manager
                .create_root_deferred_with_config(
                    Some(config()),
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(Backend)),
                    wait,
                )
                .unwrap();
            let actor = root.actor().unwrap();
            let ticket = actor
                .submit(
                    AgentInput::new(INPUT.into(), Vec::new()),
                    None,
                    INPUT.into(),
                    Default::default(),
                )
                .unwrap();
            let pending = actor.pending_submissions();
            let before = manager.journal().snapshot();
            assert!(
                matches!(manager.reserve_graph_transition(), Err(ManagerError::BusySubtree(id)) if id == root.id())
            );
            assert!(!manager.lock_graph().rewinding);
            assert_eq!(actor.pending_submissions().len(), pending.len());
            assert_eq!(actor.pending_submissions()[0].text, INPUT);
            assert!(ticket.peek_result().is_none());
            assert_eq!(manager.journal().snapshot().state, before.state);
        });
    }
}
