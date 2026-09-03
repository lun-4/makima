//! The actor's scheduler loop: drains the FIFO queue, executes work through
//! the backend, and wakes on pushes and lifecycle changes.

use std::sync::Arc;

use futures_lite::future::{self};
use maki_providers::TokenUsage;
use tracing::{debug, info, warn};

use super::queue::{ActorQueue, InterruptQueue};
use super::types::{ActorStatus, BackendResult, ControlWork, RootWork, TurnContext, WorkKind};
use super::{ActiveCancel, ActorInner, ActorWork, TurnAdmission, finalize_turn};
use crate::cancel::{CancelToken, ReasonedCancelToken};
use crate::types::{TurnCancellationReason, TurnId, TurnOutcome};
use crate::{ActorBackend, ActorLifecycle, History, InterruptSource};

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
    interrupt: Arc<dyn InterruptSource>,
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
        let interrupt: Arc<dyn InterruptSource> =
            Arc::new(InterruptQueue::new(Arc::clone(&inner.queue)));
        Self {
            inner,
            history,
            backend,
            queue,
            notify,
            wake,
            interrupt,
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
            let Some(work) = self.queue.pop() else { break };
            self.process(work).await;
        }
        debug!(agent_id = %self.inner.agent_id, "actor queue drained");

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

    async fn process(&mut self, work: ActorWork) {
        match work {
            ActorWork::Turn(admission) => self.run_turn(admission, WorkKind::Turn).await,
            ActorWork::Root(root) => self.run_root(root).await,
            ActorWork::Control(control) => self.run_control(control).await,
            ActorWork::Compact { run_id } => self.run_compact(run_id).await,
        }
    }

    /// Runs one admitted turn. Strictly serial: the runner never starts the
    /// next turn until this one settles, so only one active cancellation
    /// wiring exists at a time.
    async fn run_turn(&mut self, mut admission: TurnAdmission, work: WorkKind) {
        let turn_id = admission.turn_id;
        let agent_id = self.inner.agent_id;
        let correlation = admission.correlation.clone();
        let (active, plain, reasoned) = ActiveCancel::new(Some(correlation.clone()));
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            // A concurrent close may have terminalized the actor; do not
            // start a run into it. Roots that never entered produce nothing.
            if state.lifecycle != ActorLifecycle::Open {
                let outcome = if admission.root {
                    None
                } else {
                    Some(match state.lifecycle {
                        ActorLifecycle::Closed => TurnOutcome::Cancelled {
                            agent_id,
                            turn_id,
                            usage: TokenUsage::default(),
                            num_turns: 0,
                            reason: TurnCancellationReason::Closed,
                        },
                        ActorLifecycle::Shutdown => TurnOutcome::Cancelled {
                            agent_id,
                            turn_id,
                            usage: TokenUsage::default(),
                            num_turns: 0,
                            reason: TurnCancellationReason::Shutdown,
                        },
                        ActorLifecycle::Open => unreachable!(),
                    })
                };
                drop(state);
                self.settle_turn(&admission, outcome, true);
                return;
            }
            // A precancelled correlation's mark is retired once its matching
            // work is consumed, so it cannot poison a later generation.
            state.cancelled_correlations.remove(&correlation);
            state.active = Some(active);
            state.status = ActorStatus::Running(turn_id);
        }

        // A cancellation that landed between admission and this check (from a
        // racing cancel_all) is a setup cancellation: synthesize one Cancelled
        // outcome and deliver it once. Root admissions that never entered
        // produce no outcome at all.
        if plain.is_cancelled() {
            if admission.root {
                self.settle_turn(&admission, None, false);
            } else {
                let reason = reasoned.reason().unwrap_or(TurnCancellationReason::User);
                let outcome = TurnOutcome::Cancelled {
                    agent_id,
                    turn_id,
                    usage: TokenUsage::default(),
                    num_turns: 0,
                    reason,
                };
                self.settle_turn(&admission, Some(outcome), true);
            }
            return;
        }

        let result = self
            .backend
            .run_turn(
                &mut self.history,
                TurnContext {
                    agent_id,
                    turn_id: Some(turn_id),
                    cancel: plain,
                    cancel_reason: reasoned,
                    correlation: admission.correlation.clone(),
                    interrupt: Some(Arc::clone(&self.interrupt)),
                },
                admission.input.take().expect("turn input taken once"),
                work,
            )
            .await;
        let (outcome, deliver) = match result {
            // EnteredRun is the authoritative outcome `Agent::run` already
            // emitted exactly once; retain it but never deliver again.
            BackendResult::EnteredRun(outcome) => (outcome, false),
            // A setup failure never entered a run: synthesize one Failed
            // outcome and attempt its single delivery.
            BackendResult::SetupFailed { .. } => (
                TurnOutcome::Failed {
                    agent_id,
                    turn_id,
                    usage: TokenUsage::default(),
                    num_turns: 0,
                    failure: crate::types::TurnFailure {
                        kind: crate::types::TurnFailureKind::Internal,
                        diagnostic: "actor setup failed".to_string(),
                        user_message: "The agent could not start this turn.".to_string(),
                        retryable: true,
                    },
                },
                true,
            ),
            other => {
                warn!(%turn_id, ?other, "turn backend returned non-turn result");
                self.settle_turn(&admission, None, false);
                return;
            }
        };
        if admission.root {
            // A root-started turn's authoritative outcome was already emitted
            // by `Agent::run`; the actor retains nothing and has no registered
            // ticket to resolve.
            self.settle_turn(&admission, None, false);
            return;
        }
        self.settle_turn(&admission, Some(outcome), deliver);
    }

    /// Runs a root input. The runner only ever sees a root when the actor is
    /// idle: a root popped during an active turn is folded by the interrupt
    /// source into the active run, so no orphan [`TurnId`] exists here.
    async fn run_root(&mut self, root: RootWork) {
        let admission = TurnAdmission {
            turn_id: TurnId::generate(),
            input: Some(root.input),
            event_sender: None,
            correlation: root.correlation,
            root: true,
            ticket: super::tickets::TurnTicket::new_anonymous(),
        };
        self.run_turn(
            admission,
            WorkKind::Root {
                run_id: root.run_id,
                displayed: root.displayed,
                text: root.text,
                image_count: root.image_count,
            },
        )
        .await
    }

    /// Runs a standalone control. Never carries a [`TurnId`] and never
    /// produces a [`TurnOutcome`].
    async fn run_control(&mut self, control: ControlWork) {
        let result = self
            .backend
            .run_control(
                &mut self.history,
                TurnContext {
                    agent_id: self.inner.agent_id,
                    turn_id: None,
                    cancel: CancelToken::none(),
                    cancel_reason: ReasonedCancelToken::none(),
                    correlation: control.correlation.clone(),
                    interrupt: None,
                },
                &control,
            )
            .await;
        match result {
            BackendResult::ControlDone => {}
            BackendResult::ControlFailed => {
                warn!(control = %control.name, "control failed");
            }
            other => warn!(control = %control.name, ?other, "control returned unexpected result"),
        }
    }

    async fn run_compact(&mut self, run_id: u64) {
        // Consumed: retire any precancel mark for this run_id's canonical
        // correlation.
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cancelled_correlations
            .remove(&super::run_correlation(run_id));
        let result = self
            .backend
            .run_compact(
                &mut self.history,
                TurnContext {
                    agent_id: self.inner.agent_id,
                    turn_id: None,
                    cancel: CancelToken::none(),
                    cancel_reason: ReasonedCancelToken::none(),
                    correlation: String::new(),
                    interrupt: None,
                },
            )
            .await;
        if !matches!(result, BackendResult::CompactDone) {
            warn!(?result, "compact returned unexpected result");
        }
    }

    /// Retains (and optionally delivers once) the turn's outcome, then clears
    /// the active-turn slot and wakes the next runner step. Always clears
    /// state even when no outcome exists, so a misbehaving backend cannot
    /// strand the actor in a running state.
    fn settle_turn(
        &mut self,
        admission: &TurnAdmission,
        outcome: Option<TurnOutcome>,
        deliver: bool,
    ) {
        if let Some(outcome) = outcome {
            finalize_turn(
                &self.inner,
                admission.turn_id,
                outcome,
                Some(admission),
                deliver,
            );
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active = None;
        state.status = ActorStatus::Idle;
        drop(state);
        self.wake.wake();
    }
}
