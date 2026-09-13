use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use event_listener::Event;
use maki_providers::TokenUsage;

use super::{AgentLimits, AgentManagerHandle, AgentMetadata, GraphLifecycle, ManagerError};
use crate::{
    ActorBackend, AgentInput, AgentMode, BackendResult, ControlWork, History, TurnContext,
    TurnOutcome, WorkKind,
};

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
        Poll::Ready(BackendResult::EnteredRun(TurnOutcome::Cancelled {
            agent_id: self.context.agent_id(),
            turn_id: self.context.turn_id(),
            usage: TokenUsage::default(),
            num_turns: 0,
            reason,
        }))
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
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

impl ActorBackend for TestBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
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
            BackendResult::EnteredRun(TurnOutcome::Completed {
                agent_id: context.agent_id,
                turn_id: context.turn_id.unwrap(),
                usage: TokenUsage::default(),
                num_turns: 1,
                reason: crate::DoneReason::EndTurn,
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

#[test]
fn completed_waiter_observes_ownership_during_backend_poll() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let root = manager
            .create_root(Vec::new(), None, TestBackend::boxed())
            .unwrap();
        let turn_id = crate::TurnId::generate();
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
        let (cancel, token) = crate::ReasonedCancelToken::new();
        let (guard, current) =
            super::enter_managed_turn(&Arc::downgrade(&manager.0), root.id(), turn_id, &token)
                .await
                .unwrap();
        let (polled_tx, polled_rx) = flume::unbounded();
        let backend = std::future::poll_fn(move |_| {
            polled_tx.send(()).unwrap();
            Poll::<BackendResult>::Pending
        });
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

        task.cancel().await;
        assert!(current.lease.inner.state.lock().unwrap().closing);
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
    assert_eq!(
        manager.node(root_id).unwrap().actor.unwrap().lifecycle,
        crate::ActorLifecycle::Closed
    );
    let report = smol::block_on(manager.shutdown(std::time::Duration::from_secs(1)));
    assert_eq!(report.joined, vec![root_id]);
    assert!(report.timed_out.is_empty());
    assert!(manager.runner_finished(root_id).unwrap());
    assert_eq!(
        manager.node(root_id).unwrap().graph_lifecycle,
        GraphLifecycle::Closed
    );
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

        child.close_subtree().unwrap();
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
        ticket.wait().await;
        while !manager.runner_finished(child.id()).unwrap() {
            smol::future::yield_now().await;
        }
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

    manager.cancel_subtree(root.id()).unwrap();
    assert!(manager.lock_graph().nodes[&child_id].cancel_on_commit);
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
    const COMPLETION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

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
    let cancel_actor = root_actor.clone();
    let cancel = std::thread::spawn(move || {
        cancel_actor.cancel_correlation_with_active(
            "root",
            crate::TurnCancellationReason::User,
            |turn_id| {
                cancel_manager
                    .close_descendants_for_turn(root_id, turn_id)
                    .unwrap();
            },
        );
        cancel_done_tx.send(()).unwrap();
    });

    cut_entered_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    assert!(matches!(
        manager.spawn_child(
            &current,
            AgentMetadata::default(),
            Vec::new(),
            None,
            TestBackend::boxed(),
        ),
        Err(ManagerError::InactiveTurn { agent_id, turn_id })
            if agent_id == root.id() && turn_id == current.turn_id()
    ));
    cut_release_tx.send(()).unwrap();
    cancel_done_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
    snapshot_release.send(()).unwrap();
    snapshot_done_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();

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
fn turn_descendant_cut_rejects_post_cut_spawn_and_preserves_later_turn() {
    smol::block_on(async {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (current_tx, current_rx) = flume::unbounded();
        let root_gate = Gate::new();
        let root = manager
            .create_root(
                Vec::new(),
                None,
                TestBackend::reporting(current_tx, Some(Arc::clone(&root_gate))),
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
        let cut = std::thread::spawn(move || cutting.close_descendants_for_turn(root_id, turn_id));

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
        assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));
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

#[test]
fn descendant_admission_rejects_parent_actor_without_suspending() {
    smol::block_on(async {
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

        let error = current
            .lease()
            .admit_and_wait_for_descendant(
                &current,
                child.id(),
                &root.actor().unwrap(),
                super::PromptAdmission {
                    input: input(),
                    event_sender: None,
                    correlation: "child".into(),
                },
                None,
            )
            .err()
            .unwrap();
        assert!(matches!(error, ManagerError::ActorMismatch { .. }));
        assert_eq!(current.lease.inner.state.lock().unwrap().suspensions, 0);
        assert_eq!(child_actor.snapshot().queued, 0);

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

#[test]
fn descendant_admission_rejects_foreign_manager_actor_without_suspending() {
    smol::block_on(async {
        let (manager, _, current, root_gate) = active_root(AgentLimits::default());
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
        let (foreign_manager, foreign_root, _, foreign_gate) = active_root(AgentLimits::default());

        let error = current
            .lease()
            .admit_and_wait_for_descendant(
                &current,
                child.id(),
                &foreign_root.actor().unwrap(),
                super::PromptAdmission {
                    input: input(),
                    event_sender: None,
                    correlation: "child".into(),
                },
                None,
            )
            .err()
            .unwrap();
        assert!(matches!(error, ManagerError::ActorMismatch { .. }));
        assert_eq!(current.lease.inner.state.lock().unwrap().suspensions, 0);
        assert_eq!(child_actor.snapshot().queued, 0);

        root_gate.release(1);
        foreign_gate.release(1);
        assert!(
            manager
                .shutdown(std::time::Duration::from_secs(1))
                .await
                .timed_out
                .is_empty()
        );
        assert!(
            foreign_manager
                .shutdown(std::time::Duration::from_secs(1))
                .await
                .timed_out
                .is_empty()
        );
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
            .create_root(
                Vec::new(),
                None,
                TestBackend::reporting(root_tx, Some(Arc::clone(&root_gate))),
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
            .create_root(
                Vec::new(),
                None,
                Box::new(ReportingCancellableBackend {
                    current: current_tx,
                }),
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
            .create_root(
                Vec::new(),
                None,
                TestBackend::reporting(root_tx, Some(Arc::clone(&root_gate))),
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
            manager.node(root.id()).unwrap().graph_lifecycle,
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
            .create_root(
                Vec::new(),
                None,
                TestBackend::reporting(root_tx, Some(Arc::clone(&gate))),
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
