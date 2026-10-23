//! The actor's scheduler loop: drains the FIFO queue, executes work through
//! the backend, and wakes on pushes and lifecycle changes.

use std::sync::Arc;

use futures_lite::future::{self};
use maki_providers::TokenUsage;
use tracing::{debug, info, warn};

use super::queue::{ActorQueue, InterruptQueue};
use super::types::{ActorStatus, BackendResult, ControlWork, TurnContext, WorkKind};
use super::{
    ActiveCancel, ActorEvent, ActorInner, ActorWork, CompactionStatus, ProcessingWork,
    TurnAdmission, cancelled_outcome, settle_and_finalize_turn,
};
use crate::types::{ActorWorkIdentity, TurnCancellationReason, TurnOutcome};
use crate::{ActorBackend, ActorLifecycle, History, InterruptSource};

#[cfg(test)]
fn take_after_pop_hook(inner: &ActorInner) -> Option<(flume::Sender<()>, flume::Receiver<()>)> {
    inner
        .after_pop
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take()
}

enum Step {
    /// Queue drained; wait for the next wake.
    Waiting,
    /// Lifecycle went terminal; exit the loop.
    Exit,
}

/// A shared wake flag the handle raises on close/shutdown so a runner
/// blocked on the queue's notify channel breaks out and exits.
pub(super) struct WakeFlag {
    flag: std::sync::atomic::AtomicBool,
    event: event_listener::Event,
}

impl WakeFlag {
    pub(super) fn new() -> Self {
        Self {
            flag: std::sync::atomic::AtomicBool::new(false),
            event: event_listener::Event::new(),
        }
    }

    async fn wait(&self) {
        loop {
            if self.flag.swap(false, std::sync::atomic::Ordering::AcqRel) {
                return;
            }
            let listener = self.event.listen();
            if self.flag.swap(false, std::sync::atomic::Ordering::AcqRel) {
                return;
            }
            listener.await;
        }
    }

    #[cfg(test)]
    pub(super) fn consume(&self) {
        self.flag.store(false, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn wake(&self) {
        self.flag.store(true, std::sync::atomic::Ordering::Release);
        self.event.notify(usize::MAX);
    }
}

pub(super) struct Runner {
    inner: Arc<ActorInner>,
    history: History,
    backend: Box<dyn ActorBackend>,
    queue: Arc<ActorQueue>,
    notify: flume::Receiver<()>,
    wake: Arc<WakeFlag>,
}

impl Runner {
    pub(super) fn new(
        inner: Arc<ActorInner>,
        history: History,
        backend: Box<dyn ActorBackend>,
        wake: Arc<WakeFlag>,
    ) -> Self {
        let queue = Arc::clone(&inner.queue);
        let notify = queue.take_notify_rx();
        Self {
            inner,
            history,
            backend,
            queue,
            notify,
            wake,
        }
    }

    pub(super) async fn run(mut self) {
        loop {
            match self.step().await {
                Step::Waiting => {}
                Step::Exit => {
                    info!(agent_id = %self.inner.agent_id, "actor runner exited");
                    return;
                }
            }
        }
    }

    /// Drains every queued item, then waits for a wake (a push or a
    /// lifecycle change).
    async fn step(&mut self) -> Step {
        loop {
            let popped = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                super::delivery::schedule(&self.inner, &mut state);
                let guarded = state.idle.is_some();
                let work = state.pop_work(&self.queue);
                if guarded && state.idle.is_none() {
                    self.inner.policy_changed.notify(usize::MAX);
                }
                if let Some(work) = &work {
                    state.idle_pending = true;
                    state.runner_busy = true;
                    state.retire_superseded_precancels(work);
                }
                state.processing = work.as_ref().map(|work| {
                    let mut processing = ProcessingWork::new(work);
                    if let Some(inbox) = processing
                        .turn_id
                        .and_then(|turn_id| state.instructions.get(&turn_id))
                    {
                        processing
                            .correlations
                            .extend(inbox.correlations.iter().cloned());
                    }
                    processing
                });
                work.map(|work| (work, state.cancellation_generation))
            };
            let Some((work, cancellation_generation)) = popped else {
                break;
            };
            // Controls and compacts retire no turn, so they record their
            // settlement here once they finish; turns record theirs on retire.
            let settles = work.identity();
            #[cfg(test)]
            if let Some((popped, release)) = take_after_pop_hook(&self.inner) {
                let _ = popped.send(());
                let _ = release.recv_async().await;
            }
            let completion = self.process(work, cancellation_generation).await;
            #[cfg(test)]
            {
                let hook = self.inner.after_work_execution.lock().unwrap().take();
                if let Some((executed, release)) = hook {
                    let _ = executed.send(());
                    let _ = release.recv_async().await;
                }
            }
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.processing = None;
            if let Some(identity) = settles {
                state.settled = Some(identity);
            }
            state.finish_close(&self.inner);
            state.publish_idle(&self.inner);
            drop(state);
            if let Some((sender, result)) = completion {
                let _ = sender.send(result);
            }
        }
        #[cfg(any(test, feature = "test-util"))]
        {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let probe = super::ActorRuntimeProbe {
                operations: state.operations.len(),
                queued: self.queue.len(),
                deliveries: state.deliveries.len(),
                pending_publications: state.pending_publications,
                retained_results: state.retained_order.len(),
                processing: state.processing.is_some()
                    || state.active.is_some()
                    || state.preparing.is_some(),
            };
            for observer in self
                .inner
                .runtime_probes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .drain(..)
            {
                let _ = observer.send(probe.clone());
            }
        }
        debug!(agent_id = %self.inner.agent_id, "actor queue drained");
        let lifecycle = {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.runner_busy = false;
            state.lifecycle
        };
        self.inner.drained.notify(usize::MAX);
        if lifecycle != ActorLifecycle::Open {
            return Step::Exit;
        }

        future::or(
            async {
                let _ = self.notify.recv_async().await;
            },
            async { self.wake.wait().await },
        )
        .await;

        match self
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .lifecycle
        {
            ActorLifecycle::Open => Step::Waiting,
            ActorLifecycle::Closed | ActorLifecycle::Shutdown => Step::Exit,
        }
    }

    async fn process(
        &mut self,
        work: ActorWork,
        cancellation_generation: u64,
    ) -> Option<super::history_edit::HistoryEditCompletion> {
        match work {
            ActorWork::Turn(admission) => self.run_turn(*admission, cancellation_generation).await,
            ActorWork::Control(control) => self.run_control(control).await,
            ActorWork::HistoryEdit(edit) => {
                let actor = super::AgentActorHandle {
                    inner: Arc::clone(&self.inner),
                    wake: Arc::clone(&self.wake),
                };
                return Some(edit.execute(&actor, &mut self.history).await);
            }
            ActorWork::Compact {
                run_id,
                instructions,
                generation,
                policy,
                work,
                ..
            } => {
                self.run_compact(run_id, instructions.as_deref(), generation, policy, work)
                    .await
            }
        }
        None
    }

    /// Runs one admitted turn. Strictly serial: the runner never starts the
    /// next turn until this one settles, so only one active cancellation
    /// wiring exists at a time.
    async fn run_turn(&mut self, mut admission: TurnAdmission, popped_generation: u64) {
        let turn_id = admission.turn_id;
        let agent_id = self.inner.agent_id;
        let correlation = admission.correlation.clone();
        let (active, plain, reasoned) = ActiveCancel::new(Some(correlation.clone()));
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state
                .delivery_runs
                .get(&turn_id)
                .is_some_and(|batch| batch.iter().all(|id| !state.deliveries.contains_key(id)))
            {
                admission.ticket.resolve(cancelled_outcome(
                    agent_id,
                    turn_id,
                    TurnCancellationReason::Withdrawn,
                ));
                state.delivery_runs.remove(&turn_id);
                self.inner
                    .tickets
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&turn_id);
                state.settle_runner();
                return;
            }
            // A reusable cancellation cut may have landed after this work was
            // popped but before active installation. It owns that popped work.
            if state.cancellation_generation != popped_generation {
                drop(state);
                let outcome = cancelled_outcome(agent_id, turn_id, TurnCancellationReason::User);
                self.settle_turn(&admission, Some(outcome), true);
                return;
            }
            // A concurrent close may have terminalized the actor; do not
            // start a run into it.
            if state.lifecycle != ActorLifecycle::Open {
                let outcome = Some(cancelled_outcome(
                    agent_id,
                    turn_id,
                    match state.lifecycle {
                        ActorLifecycle::Closed => TurnCancellationReason::Closed,
                        ActorLifecycle::Shutdown => TurnCancellationReason::Shutdown,
                        ActorLifecycle::Open => unreachable!(),
                    },
                ));
                drop(state);
                self.settle_turn(&admission, outcome, true);
                return;
            }
            if let Some(reason) = state
                .processing
                .as_ref()
                .and_then(|processing| processing.cancellation_reason)
            {
                drop(state);
                let outcome = Some(cancelled_outcome(agent_id, turn_id, reason));
                self.settle_turn(&admission, outcome, true);
                return;
            }
            state.active = Some(active);
            state.status = ActorStatus::Running(turn_id);
        }

        // A cancellation that landed between admission and this check (from a
        // racing cancel_all) is a setup cancellation: synthesize one Cancelled
        // outcome and deliver it once.
        if plain.is_cancelled() {
            let reason = reasoned.reason().unwrap_or(TurnCancellationReason::User);
            let outcome = cancelled_outcome(agent_id, turn_id, reason);
            self.settle_turn(&admission, Some(outcome), true);
            return;
        }

        let (managed_guard, managed_turn) = match &self.inner.managed_admission {
            Some(managed) => match crate::manager::enter_managed_turn(
                &managed.manager,
                managed.agent_id,
                turn_id,
                &reasoned,
            )
            .await
            {
                Ok((guard, mut current)) => {
                    current.policy = admission
                        .policy
                        .as_ref()
                        .map(|config| Arc::new(config.settings.clone()));
                    current.context = admission
                        .policy
                        .as_ref()
                        .map(|config| config.context.clone());
                    current.mode = admission.policy.as_ref().map(|config| config.mode.clone());
                    current.mode_def = admission
                        .policy
                        .as_ref()
                        .and_then(|config| config.mode_def.clone());
                    (Some(guard), Some(current))
                }
                Err(reason) => {
                    let outcome = cancelled_outcome(agent_id, turn_id, reason);
                    self.settle_turn(&admission, Some(outcome), true);
                    return;
                }
            },
            None => (None, None),
        };
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open || plain.is_cancelled() {
                drop(state);
                let reason = reasoned.reason().unwrap_or(TurnCancellationReason::Closed);
                self.settle_turn(
                    &admission,
                    Some(cancelled_outcome(agent_id, turn_id, reason)),
                    true,
                );
                return;
            }

            if state
                .delivery_runs
                .get(&turn_id)
                .is_some_and(|batch| batch.iter().all(|id| !state.deliveries.contains_key(id)))
            {
                admission.ticket.resolve(cancelled_outcome(
                    agent_id,
                    turn_id,
                    TurnCancellationReason::Withdrawn,
                ));
                state.delivery_runs.remove(&turn_id);
                self.inner
                    .tickets
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove(&turn_id);
                state.settle_runner();
                return;
            }
            admission.ticket.mark_started();
            state.emit(super::ActorEvent::Start {
                agent_id,
                turn_id,
                provenance: admission.ticket.provenance().clone(),
                config: admission.policy.as_ref().map(|config| super::ConfigCommit {
                    identity: Arc::clone(&self.inner.identity),
                    generation: admission.generation,
                    config: Arc::clone(config),
                }),
            });
        }
        let backend = self.backend.run_turn(
            &mut self.history,
            TurnContext {
                agent_id,
                turn_id: Some(turn_id),
                work: admission.work,
                output: admission.ticket.output(),
                provenance: admission.ticket.provenance().clone(),
                cancel: plain,
                cancel_reason: reasoned.clone(),
                generation: admission.generation,
                policy: admission.policy.clone(),
                interrupt: Some(Arc::new(InterruptQueue::new(
                    Arc::clone(&self.inner),
                    popped_generation,
                    admission.generation,
                )) as Arc<dyn InterruptSource>),
                inbox: Some(super::inbox::ActorTurnInbox::source(
                    &self.inner,
                    turn_id,
                    managed_turn.clone(),
                )),
                managed_turn: managed_turn.clone(),
                admission: admission.admission.clone(),
            },
            admission.input.take().expect("turn input taken once"),
            WorkKind::Turn,
        );
        let result = match (managed_guard, managed_turn) {
            (Some(guard), Some(current)) => {
                crate::manager::manage_execution(backend, guard, current.lease.inner, reasoned)
                    .await
            }
            (None, None) => backend.await,
            _ => unreachable!("managed guard and context are created together"),
        };
        let history_ready = matches!(&result, BackendResult::EnteredRun(_));
        let outcome = match result {
            BackendResult::EnteredRun(outcome) => outcome,
            // A setup failure never entered a run: synthesize one Failed
            // outcome and attempt its single delivery.
            BackendResult::SetupFailed(failure) => TurnOutcome::failed(
                agent_id,
                turn_id,
                TokenUsage::default(),
                0,
                failure.unwrap_or_else(|| crate::types::TurnFailure {
                    kind: crate::types::TurnFailureKind::Internal,
                    diagnostic: "actor setup failed".to_string(),
                    user_message: "The agent could not start this turn.".to_string(),
                    retryable: true,
                }),
            ),
            other => {
                warn!(%turn_id, ?other, "turn backend returned non-turn result");
                self.settle_turn(&admission, None, false);
                return;
            }
        };
        settle_and_finalize_turn(&self.inner, &admission, outcome, true, history_ready);
        self.wake.wake();
    }

    /// Runs a standalone control. Never carries a [`crate::TurnId`] and never
    /// produces a [`TurnOutcome`].
    async fn run_control(&mut self, control: ControlWork) {
        let correlation = control.correlation.clone();
        let (active, plain, reasoned) = ActiveCancel::new(Some(correlation.clone()));
        {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != ActorLifecycle::Open
                || state
                    .processing
                    .as_ref()
                    .is_some_and(|processing| processing.cancellation_reason.is_some())
            {
                return;
            }
            state.active = Some(active);
        }
        if plain.is_cancelled() {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.active = None;
            return;
        }
        let result = self
            .backend
            .run_control(
                &mut self.history,
                TurnContext {
                    agent_id: self.inner.agent_id,
                    turn_id: None,
                    work: control.work,
                    output: super::TurnOutput::default(),
                    provenance: super::TurnProvenance::default(),
                    cancel: plain,
                    cancel_reason: reasoned,
                    generation: 0,
                    policy: None,
                    interrupt: None,
                    inbox: None,
                    managed_turn: None,
                    admission: None,
                },
                &control,
            )
            .await;
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.active = None;
            if let BackendResult::ControlCompacted(status) = result {
                state.emit(ActorEvent::CompactionEnd(status));
                return;
            }
        }
        match result {
            BackendResult::ControlDone => {}
            BackendResult::ControlFailed => {
                warn!(control = %control.name, "control failed");
            }
            other => warn!(control = %control.name, ?other, "control returned unexpected result"),
        }
    }

    async fn run_compact(
        &mut self,
        run_id: u64,
        instructions: Option<&str>,
        policy_generation: u64,
        policy: Option<Arc<super::EffectiveAgentConfig>>,
        work: ActorWorkIdentity,
    ) {
        let correlation = super::run_correlation(run_id);
        let (active, plain, reasoned) = ActiveCancel::new(Some(correlation.clone()));
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return;
            }
            if state
                .processing
                .as_ref()
                .is_some_and(|processing| processing.cancellation_reason.is_some())
            {
                state.emit(ActorEvent::CompactionEnd(CompactionStatus::Cancelled));
                return;
            }
            state.active = Some(active);
        }
        if plain.is_cancelled() {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.active = None;
            state.emit(ActorEvent::CompactionEnd(CompactionStatus::Cancelled));
            return;
        }
        let cancel = plain.clone();
        let result = self
            .backend
            .run_compact(
                &mut self.history,
                TurnContext {
                    agent_id: self.inner.agent_id,
                    turn_id: None,
                    work,
                    output: super::TurnOutput::default(),
                    provenance: super::TurnProvenance::default(),
                    cancel: plain,
                    cancel_reason: reasoned,
                    generation: policy_generation,
                    policy,
                    interrupt: None,
                    inbox: None,
                    managed_turn: None,
                    admission: None,
                },
                instructions,
            )
            .await;
        let status = match result {
            BackendResult::CompactDone => CompactionStatus::Completed,
            _ if cancel.is_cancelled() => CompactionStatus::Cancelled,
            BackendResult::ControlFailed => CompactionStatus::Failed,
            other => {
                warn!(?other, "compact returned unexpected result");
                CompactionStatus::Failed
            }
        };
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active = None;
        state.emit(ActorEvent::CompactionEnd(status));
    }

    /// Clears the active-turn slot together with retaining the turn's outcome,
    /// then delivers it at most once and wakes the next runner step. Always
    /// clears state even when no outcome exists, so a misbehaving backend
    /// cannot strand the actor in a running state.
    fn settle_turn(
        &mut self,
        admission: &TurnAdmission,
        outcome: Option<TurnOutcome>,
        deliver: bool,
    ) {
        match outcome {
            Some(outcome) => {
                settle_and_finalize_turn(&self.inner, admission, outcome, deliver, false)
            }
            None => settle_and_finalize_turn(
                &self.inner,
                admission,
                crate::TurnOutcome::failed(
                    self.inner.agent_id,
                    admission.turn_id,
                    TokenUsage::default(),
                    0,
                    crate::types::TurnFailure {
                        kind: crate::types::TurnFailureKind::Internal,
                        diagnostic: "turn backend returned non-turn result".into(),
                        user_message: "The agent could not finish this turn.".into(),
                        retryable: true,
                    },
                ),
                deliver,
                false,
            ),
        }
        self.wake.wake();
    }
}
