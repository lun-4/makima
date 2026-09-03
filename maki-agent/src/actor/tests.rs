//! Deterministic actor-core tests. No sleeps: every test drives the actor
//! through a scripted backend and waits on exact outcome tickets, which
//! resolve the moment the actor retains the outcome.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use maki_providers::{ContentBlock, Message, TokenUsage};

use super::queue::{ActorQueue, QueueProjection};
use super::types::{
    ActorLifecycle, ActorStatus, BackendResult, ControlWork, RootWork, TurnContext, WorkKind,
};
use super::{ActorBackend, ActorError, ActorWork, AgentActorHandle, TurnAdmission};
use crate::types::{
    AgentId, AgentInput, AgentMode, DoneReason, EventSender, TurnCancellationReason, TurnId,
    TurnOutcome,
};
use crate::{ExtractedCommand, SharedMessages};

/// Shared observations the scripted backend records for the test to assert.
#[derive(Default)]
struct ScriptedState {
    runs: Mutex<Vec<(WorkKind, String)>>,
    root_metadata: Mutex<Vec<(u64, bool, String, usize)>>,
    folds: Mutex<Vec<String>>,
    controls: Mutex<Vec<String>>,
    compacts: AtomicU32,
    entered: AtomicU32,
}

/// Scripted backend with an optional gate and a scripted outcome sequence.
/// `run_turn` always polls the interrupt source exactly once (before the
/// gate), which makes the root-fold and interrupt-extraction tests
/// deterministic.
struct ScriptedBackend {
    state: Arc<ScriptedState>,
    gate: Option<Arc<Gate>>,
    outcomes: Mutex<Vec<BackendResult>>,
}

struct Gate {
    opened: std::sync::atomic::AtomicBool,
    event: event_listener::Event,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            opened: std::sync::atomic::AtomicBool::new(false),
            event: event_listener::Event::new(),
        })
    }

    async fn wait(&self) {
        loop {
            if self.opened.load(Ordering::Acquire) {
                return;
            }
            let listener = self.event.listen();
            if self.opened.load(Ordering::Acquire) {
                return;
            }
            listener.await;
        }
    }

    fn open(&self) {
        self.opened.store(true, Ordering::Release);
        self.event.notify(usize::MAX);
    }
}

impl ScriptedBackend {
    fn new() -> Self {
        Self {
            state: Arc::new(ScriptedState::default()),
            gate: None,
            outcomes: Mutex::new(Vec::new()),
        }
    }

    fn gated(gate: Arc<Gate>) -> Self {
        Self {
            gate: Some(gate),
            ..Self::new()
        }
    }

    fn failing_setup() -> Self {
        let backend = Self::new();
        *backend.outcomes.lock().unwrap() = vec![BackendResult::SetupFailed {
            agent_id: AgentId::generate(),
            turn_id: crate::types::TurnId::generate(),
        }];
        backend
    }

    fn completed(context: &TurnContext, usage: TokenUsage) -> BackendResult {
        BackendResult::EnteredRun(TurnOutcome::Completed {
            agent_id: context.agent_id,
            turn_id: context.turn_id.unwrap(),
            usage,
            num_turns: 1,
            reason: DoneReason::EndTurn,
        })
    }
}

fn default_completed(context: &TurnContext) -> BackendResult {
    ScriptedBackend::completed(
        context,
        TokenUsage {
            input: 10,
            output: 5,
            cache_creation: 0,
            cache_read: 0,
        },
    )
}

impl ActorBackend for ScriptedBackend {
    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        input: crate::AgentInput,
        work: WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.state
                .runs
                .lock()
                .unwrap()
                .push((work.clone(), input.message.clone()));
            if let WorkKind::Root {
                run_id,
                displayed,
                text,
                image_count,
            } = work
            {
                self.state.root_metadata.lock().unwrap().push((
                    run_id,
                    displayed,
                    text,
                    image_count,
                ));
            }
            self.state.entered.fetch_add(1, Ordering::SeqCst);
            history.push(Message::user(input.message));

            // Poll the interrupt source exactly once per turn, after the
            // gate, so a test can queue a root mid-run and the fold is
            // deterministic. An extracted root folds into this turn (no new
            // TurnId, no outcome of its own).
            if let Some(gate) = &self.gate {
                gate.wait().await;
            }
            if let Some(ExtractedCommand::Interrupt(folded, _)) =
                context.interrupt.as_ref().and_then(|source| source.poll())
            {
                self.state
                    .folds
                    .lock()
                    .unwrap()
                    .push(folded.message.clone());
                history.push(Message::user(folded.message));
            }

            self.outcomes
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| default_completed(&context))
        })
    }

    fn run_control<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        _context: TurnContext,
        control: &'a ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.state
                .controls
                .lock()
                .unwrap()
                .push(control.name.clone());
            history.push(Message::user(format!("control:{}", control.name)));
            BackendResult::ControlDone
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        _context: TurnContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.state.compacts.fetch_add(1, Ordering::SeqCst);
            history.push(Message::user("compact".to_owned()));
            BackendResult::CompactDone
        })
    }
}

/// Backend whose first turn blocks until cancelled and then returns the
/// cancellation (the reason `Agent::run` would read) as its outcome. Later
/// turns complete normally so the actor's reuse is testable.
struct CancellableBackend {
    state: Arc<ScriptedState>,
    block_first: std::sync::atomic::AtomicU32,
}

impl CancellableBackend {
    fn new() -> Self {
        Self {
            state: Arc::new(ScriptedState::default()),
            block_first: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

impl ActorBackend for CancellableBackend {
    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        input: crate::AgentInput,
        _work: WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.state.entered.fetch_add(1, Ordering::SeqCst);
            history.push(Message::user(input.message));
            if self.block_first.fetch_add(1, Ordering::SeqCst) == 0 {
                let reason = context.cancel_reason.cancelled().await;
                return BackendResult::EnteredRun(TurnOutcome::Cancelled {
                    agent_id: context.agent_id,
                    turn_id: context.turn_id.unwrap(),
                    usage: TokenUsage::default(),
                    num_turns: 0,
                    reason,
                });
            }
            default_completed(&context)
        })
    }

    fn run_control<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        _context: TurnContext,
        control: &'a ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            history.push(Message::user(format!("control:{}", control.name)));
            BackendResult::ControlDone
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        _context: TurnContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            history.push(Message::user("compact".to_owned()));
            BackendResult::CompactDone
        })
    }
}

fn input(message: &str) -> AgentInput {
    AgentInput {
        message: message.to_owned(),
        mode: AgentMode::Build,
        images: Vec::new(),
        preamble: Vec::new(),
        thinking: maki_providers::ThinkingConfig::Off,
        fast: false,
        workflow: false,
        prompt: None,
    }
}

fn spawn(backend: impl ActorBackend + 'static) -> (AgentActorHandle, smol::Task<()>) {
    AgentActorHandle::spawn(AgentId::generate(), Vec::new(), None, Box::new(backend))
}

/// Yields to the single-threaded executor until `cond` holds. Bounded only
/// so a broken test cannot hang; never sleeps.
async fn until(cond: impl Fn() -> bool) {
    for _ in 0..100_000 {
        if cond() {
            return;
        }
        smol::future::yield_now().await;
    }
    panic!("condition never became true");
}

#[test]
fn fifo_order_is_strict() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let a = handle.admit_turn(input("first"), None, "a".into()).unwrap();
        let b = handle
            .admit_turn(input("second"), None, "b".into())
            .unwrap();
        let c = handle.admit_turn(input("third"), None, "c".into()).unwrap();
        assert_eq!(a.wait().await.turn_id(), a.turn_id());
        assert_eq!(b.wait().await.turn_id(), b.turn_id());
        assert_eq!(c.wait().await.turn_id(), c.turn_id());
        let messages: Vec<String> = state
            .runs
            .lock()
            .unwrap()
            .iter()
            .map(|(_, m)| m.clone())
            .collect();
        assert_eq!(messages, ["first", "second", "third"]);
        handle.close();
        task.await;
    });
}

#[test]
fn exact_wait_and_lookup() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        let ticket = handle.admit_turn(input("work"), None, "w".into()).unwrap();
        let outcome = ticket.wait().await;
        assert_eq!(outcome.turn_id(), ticket.turn_id());
        assert!(matches!(outcome, TurnOutcome::Completed { .. }));

        // Lookup by id and exact wait by id agree.
        assert_eq!(handle.outcome(ticket.turn_id()), Some(outcome.clone()));
        assert_eq!(
            handle.wait_outcome(ticket.turn_id()).await.unwrap(),
            outcome
        );

        // Waiting a turn that never existed errors instead of stranding.
        assert!(matches!(
            handle.wait_outcome(crate::types::TurnId::generate()).await,
            Err(ActorError::UnknownTurn(_))
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn failed_actor_is_reusable() {
    smol::block_on(async {
        let backend = ScriptedBackend::failing_setup();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let a = handle.admit_turn(input("one"), None, "a".into()).unwrap();
        // The actor synthesizes and delivers exactly one Failed outcome.
        let outcome = a.wait().await;
        assert!(matches!(outcome, TurnOutcome::Failed { num_turns: 0, .. }));

        // Reuse: the second turn succeeds.
        let b = handle.admit_turn(input("two"), None, "b".into()).unwrap();
        assert!(matches!(b.wait().await, TurnOutcome::Completed { .. }));
        assert_eq!(state.runs.lock().unwrap().len(), 2);
        handle.close();
        task.await;
    });
}

#[test]
fn close_drains_queued_turns_and_cancels_active_with_closed_reason() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "t1".into())
            .unwrap();
        let t2 = handle
            .admit_turn(input("queued"), None, "t2".into())
            .unwrap();
        let (tx, rx) = flume::unbounded();
        let t3 = handle
            .admit_turn(
                input("queued-with-sink"),
                Some(EventSender::new(tx, 0)),
                "t3".into(),
            )
            .unwrap();

        until(|| handle.snapshot().status != ActorStatus::Idle).await;
        handle.close();

        // Queued turns terminalize with the Closed reason and their tickets
        // resolve; the one with a sink gets exactly one delivery.
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        assert!(matches!(
            t3.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        let events: Vec<_> = rx.drain().collect();
        assert_eq!(
            events.len(),
            1,
            "each queued turn delivers its outcome exactly once"
        );

        // The active run was cancelled with Closed (reason-before-abort).
        assert!(matches!(
            t1.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        assert_eq!(handle.snapshot().queued, 0);
        task.await;
    });
}

#[test]
fn cancel_all_is_reusable() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "t1".into())
            .unwrap();
        until(|| handle.snapshot().status != ActorStatus::Idle).await;

        handle
            .rush(RootWork {
                input: input("root"),
                run_id: 1,
                displayed: false,
                text: "root".into(),
                image_count: 0,
                correlation: "r1".into(),
            })
            .unwrap();
        let t2 = handle
            .admit_turn(input("queued"), None, "t2".into())
            .unwrap();

        handle.cancel_all();

        // Queued admitted turn terminalized with User; root dropped silently.
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(handle.snapshot().queued, 0);

        // The active run reads the User reason from its reasoned token.
        assert!(matches!(
            t1.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(
            handle.snapshot().lifecycle,
            ActorLifecycle::Open,
            "cancel_all keeps the actor open"
        );

        // The actor stayed open: a new turn runs normally.
        let t3 = handle
            .admit_turn(input("after"), None, "t3".into())
            .unwrap();
        assert!(matches!(t3.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn entered_run_is_never_emitted_twice() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx, 0)), "w".into())
            .unwrap();
        ticket.wait().await;
        // Agent::run already emitted the authoritative outcome; the actor
        // retains but never re-delivers it.
        assert!(rx.is_empty());
        assert!(handle.outcome(ticket.turn_id()).is_some());

        // Close after completion does not re-emit either.
        handle.close();
        assert_eq!(rx.drain().count(), 0);
        task.await;
    });
}

#[test]
fn setup_failure_delivered_exactly_once() {
    smol::block_on(async {
        let backend = ScriptedBackend::failing_setup();
        let (handle, task) = spawn(backend);
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx, 0)), "w".into())
            .unwrap();
        let outcome = ticket.wait().await;
        let events: Vec<_> = rx.drain().collect();
        assert_eq!(
            events.len(),
            1,
            "synthesized outcome delivered exactly once"
        );
        assert!(matches!(outcome, TurnOutcome::Failed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn event_sink_failure_still_retains() {
    smol::block_on(async {
        let backend = ScriptedBackend::failing_setup();
        let (handle, task) = spawn(backend);
        let (tx, rx) = flume::unbounded();
        drop(rx); // receiver dropped before admission so delivery fails
        let ticket = handle
            .admit_turn(input("work"), Some(EventSender::new(tx, 0)), "w".into())
            .unwrap();
        let outcome = ticket.wait().await;
        assert!(matches!(outcome, TurnOutcome::Failed { .. }));
        assert!(
            handle.outcome(ticket.turn_id()).is_some(),
            "outcome retained despite sink failure"
        );
        handle.close();
        task.await;
    });
}

#[test]
fn root_folds_into_active_turn_with_no_orphan() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let main = handle.admit_turn(input("main"), None, "m".into()).unwrap();
        // Wait until the main turn has entered the backend, then queue a
        // root. The gated backend polls the interrupt source only after the
        // gate opens, so ordering is deterministic.
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle
            .rush(RootWork {
                input: input("fold-me"),
                run_id: 1,
                displayed: false,
                text: "fold-me".into(),
                image_count: 0,
                correlation: "r1".into(),
            })
            .unwrap();
        gate.open();
        main.wait().await;

        let runs: Vec<_> = state.runs.lock().unwrap().clone();
        assert_eq!(runs.len(), 1, "root folded, never started as its own turn");
        assert_eq!(runs[0].0, WorkKind::Turn);
        let folds = state.folds.lock().unwrap().clone();
        assert_eq!(folds, ["fold-me"]);

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 0);
        assert_eq!(
            snapshot.latest.as_ref().map(|o| o.turn_id()),
            Some(main.turn_id())
        );
        assert_eq!(snapshot.status, ActorStatus::Idle);
        handle.close();
        task.await;
    });
}

#[test]
fn idle_root_start_preserves_metadata_to_backend() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        // Queue the root while idle: the runner pops it and starts it as its
        // own turn, carrying the neutral display metadata.
        handle
            .rush(RootWork {
                input: input("deferred"),
                run_id: 7,
                displayed: false,
                text: "deferred bubble".into(),
                image_count: 3,
                correlation: "r7".into(),
            })
            .unwrap();
        // The runner starts the root (entered) and settles to Idle; both are
        // observed before asserting, so the backend's run completed.
        until(|| {
            state.entered.load(Ordering::SeqCst) > 0
                && handle.snapshot().status == ActorStatus::Idle
        })
        .await;

        // Backend observed the started root with its exact metadata.
        let runs: Vec<_> = state.runs.lock().unwrap().clone();
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].0,
            WorkKind::Root {
                run_id: 7,
                displayed: false,
                text: "deferred bubble".into(),
                image_count: 3,
            }
        );
        assert_eq!(runs[0].1, "deferred");
        let metadata = state.root_metadata.lock().unwrap().clone();
        assert_eq!(metadata, vec![(7, false, "deferred bubble".into(), 3)]);

        let snapshot = handle.snapshot();
        assert_eq!(snapshot.queued, 0);
        assert_eq!(snapshot.status, ActorStatus::Idle);
        handle.close();
        task.await;
    });
}

#[test]
fn control_produces_no_outcome() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        handle
            .push_control(ControlWork {
                name: "pin".into(),
                correlation: "c1".into(),
            })
            .unwrap();
        until(|| !state.controls.lock().unwrap().is_empty()).await;
        assert_eq!(state.controls.lock().unwrap().as_slice(), ["pin"]);
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.latest, None, "control must not produce an outcome");
        assert_eq!(snapshot.queued, 0);
        handle.close();
        task.await;
    });
}

#[test]
fn first_cancellation_reason_wins() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let ticket = handle.admit_turn(input("work"), None, "w".into()).unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle.close();
        let outcome = ticket.wait().await;
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            }
        ));
        task.await;
    });
}

#[test]
fn first_reason_wins_across_sources() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let ticket = handle.admit_turn(input("work"), None, "w".into()).unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        // cancel_all fires User first; the later close cannot override it.
        handle.cancel_all();
        handle.close();
        let outcome = ticket.wait().await;
        assert!(matches!(
            outcome,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        task.await;
    });
}

#[test]
fn close_rejects_new_admissions() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        handle.close();
        assert_eq!(
            handle
                .admit_turn(input("late"), None, "late".into())
                .unwrap_err(),
            ActorError::Closed
        );
        assert_eq!(handle.snapshot().lifecycle, ActorLifecycle::Closed);
        task.await;
    });
}

#[test]
fn queue_pop_interrupt_keeps_incompatible_entries() {
    let queue = ActorQueue::new();
    queue.push(ActorWork::Compact { run_id: 1 });
    queue.push(ActorWork::Control(ControlWork {
        name: "c".into(),
        correlation: "c".into(),
    }));
    assert!(matches!(
        queue.pop_interrupt(),
        Some(ExtractedCommand::Compact(1))
    ));
    // A control at the front is incompatible: poll must not consume it.
    assert_eq!(queue.pop_interrupt(), None);
    assert_eq!(queue.len(), 1);
    // A turn at the front shields a root behind it.
    let admission = TurnAdmission {
        turn_id: crate::types::TurnId::generate(),
        input: Some(input("t")),
        event_sender: None,
        correlation: "t".into(),
        root: false,
        ticket: super::TurnTicket::new(crate::types::TurnId::generate()),
    };
    queue.push(ActorWork::Turn(admission));
    queue.push(ActorWork::Root(RootWork {
        input: input("r"),
        run_id: 2,
        displayed: false,
        text: "r".into(),
        image_count: 0,
        correlation: "r2".into(),
    }));
    assert_eq!(queue.pop_interrupt(), None);
    assert_eq!(
        queue.len(),
        3,
        "incompatible FIFO entries must not be discarded"
    );
}

#[test]
fn queue_drain_publication_is_ordered() {
    let queue = ActorQueue::new();
    let published = Arc::new(Mutex::new(0));
    queue.publish_if_empty(|| *published.lock().unwrap() += 1);
    assert_eq!(*published.lock().unwrap(), 1, "empty queue publishes");
    queue.push(ActorWork::Compact { run_id: 1 });
    queue.publish_if_empty(|| *published.lock().unwrap() += 1);
    assert_eq!(
        *published.lock().unwrap(),
        1,
        "non-empty queue must not publish"
    );
    // After a drain the queue is empty again and publishes.
    assert_eq!(queue.drain_all().len(), 1);
    queue.publish_if_empty(|| *published.lock().unwrap() += 1);
    assert_eq!(*published.lock().unwrap(), 2);
}

#[test]
fn restored_mirror_is_synchronous() {
    smol::block_on(async {
        let mirror = SharedMessages::default();
        let backend = ScriptedBackend::new();
        let (handle, task) = AgentActorHandle::spawn(
            AgentId::generate(),
            vec![Message::user("restored".to_owned())],
            Some(mirror.clone()),
            Box::new(backend),
        );
        // The mirror already contains the restored history before the handle
        // escapes: no yield needed.
        let snapshot = mirror.load_full();
        assert_eq!(snapshot.messages.len(), 1);
        assert!(matches!(
            snapshot.messages[0].content[0],
            ContentBlock::Text { .. }
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn remove_terminalizes_instead_of_stranding() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "t1".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        let t2 = handle
            .admit_turn(input("queued"), None, "t2".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("also-queued"), None, "t3".into())
            .unwrap();

        let removed = handle.remove(t2.turn_id()).unwrap();
        assert!(matches!(
            removed,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(handle.snapshot().queued, 1, "only t3 remains");

        gate.open();
        t1.wait().await;
        t3.wait().await;
        handle.close();
        task.await;
    });
}

/// One of the running/queued turns reached the backend.
fn backend_reached(handle: &AgentActorHandle) -> bool {
    handle.snapshot().status != ActorStatus::Idle
}

#[test]
fn targeted_cancel_matching_queued_correlation_and_reuse() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("one"), None, "keep".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        let t2 = handle
            .admit_turn(input("two"), None, "drop".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("three"), None, "keep".into())
            .unwrap();
        // Queue root work sharing the "drop" correlation, behind the turns.
        handle
            .rush(RootWork {
                input: input("root-drop"),
                run_id: 9,
                displayed: false,
                text: "root-drop".into(),
                image_count: 0,
                correlation: "drop".into(),
            })
            .unwrap();

        handle.cancel_correlation("drop", TurnCancellationReason::User);

        // The matching queued turn is terminalized exactly once with the
        // reason; unrelated turns run normally.
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        // The matching root was dropped; only t1 and t3 remain queued.
        assert_eq!(handle.snapshot().queued, 2);
        gate.open();
        assert!(matches!(t1.wait().await, TurnOutcome::Completed { .. }));
        assert!(matches!(t3.wait().await, TurnOutcome::Completed { .. }));
        // The actor stays reusable.
        let t4 = handle
            .admit_turn(input("four"), None, "new".into())
            .unwrap();
        assert!(matches!(t4.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn targeted_cancel_matching_active_run() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let running = handle
            .admit_turn(input("running"), None, "run-a".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        handle.cancel_correlation("run-a", TurnCancellationReason::User);
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        // Unrelated correlation left the actor reusable.
        let later = handle
            .admit_turn(input("later"), None, "run-b".into())
            .unwrap();
        assert!(matches!(later.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn precancel_marks_match_and_do_not_poison() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        // Cancel before any work with this correlation exists.
        handle.cancel_correlation("run-5", TurnCancellationReason::User);
        // Precancelled turn admission is terminalized immediately and exactly
        // once, without executing or stranding.
        let (tx, rx) = flume::unbounded();
        let ticket = handle
            .admit_turn(input("late"), Some(EventSender::new(tx, 0)), "run-5".into())
            .unwrap();
        assert!(matches!(
            ticket.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        let events: Vec<_> = rx.drain().collect();
        assert_eq!(events.len(), 1, "precancelled admission delivered once");
        // Precancelled root is dropped without executing.
        handle
            .rush(RootWork {
                input: input("late-root"),
                run_id: 5,
                displayed: false,
                text: "late-root".into(),
                image_count: 0,
                correlation: "run-5".into(),
            })
            .unwrap();
        assert_eq!(handle.snapshot().queued, 0);
        // The mark is retired: a later unrelated correlation runs fine.
        let ok = handle
            .admit_turn(input("other"), None, "other".into())
            .unwrap();
        assert!(matches!(ok.wait().await, TurnOutcome::Completed { .. }));
        handle.close();
        task.await;
    });
}

#[test]
fn targeted_cancel_first_reason_wins_vs_close() {
    smol::block_on(async {
        let backend = CancellableBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);
        let running = handle
            .admit_turn(input("running"), None, "race-a".into())
            .unwrap();
        until(|| state.entered.load(Ordering::SeqCst) > 0).await;
        // Targeted cancel fires User; the later close cannot override.
        handle.cancel_correlation("race-a", TurnCancellationReason::User);
        handle.close();
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        task.await;
    });
}

#[test]
fn remove_visible_at_skips_hidden_and_removes_deferred() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        // The runner is parked in the gated backend on the admitted turn, so
        // everything pushed below stays queued and the queue order is stable.
        let running = handle
            .admit_turn(input("running"), None, "r0".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        // Hidden rows: an already-displayed root.
        handle
            .rush(RootWork {
                input: input("displayed-root"),
                run_id: 1,
                displayed: true,
                text: "displayed-root".into(),
                image_count: 0,
                correlation: "h2".into(),
            })
            .unwrap();
        // Visible rows: a deferred root and a compact.
        handle
            .rush(RootWork {
                input: input("deferred-root"),
                run_id: 2,
                displayed: false,
                text: "deferred-root".into(),
                image_count: 0,
                correlation: "v1".into(),
            })
            .unwrap();
        handle.push_compact(3).unwrap();

        // Panel shows [deferred-root, compact].
        let queue = handle.snapshot().queue;
        assert_eq!(queue.len(), 2);

        // Removing visible index 0 removes the deferred root, not the hidden
        // displayed root.
        let removed = handle.remove_visible_at(0).unwrap();
        assert!(matches!(
            removed,
            QueueProjection::Message { ref text, .. } if text == "deferred-root"
        ));
        assert_eq!(handle.snapshot().queued, 2);
        // The running turn finishes normally once the gate opens.
        gate.open();
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Completed { .. }
        ));
        handle.close();
        task.await;
    });
}

#[test]
fn remove_at_raw_index_terminalizes_real_turn() {
    smol::block_on(async {
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let t1 = handle
            .admit_turn(input("running"), None, "r1".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        let t2 = handle
            .admit_turn(input("queued"), None, "r2".into())
            .unwrap();
        let t3 = handle
            .admit_turn(input("queued2"), None, "r3".into())
            .unwrap();

        // The queue is [running is out of queue; queued, queued2]; raw index 0
        // is t2. Removing it terminalizes the real turn exactly once.
        let removed = handle.remove_at(0).unwrap();
        assert!(matches!(removed, QueueProjection::Turn(_)));
        assert!(matches!(
            t2.wait().await,
            TurnOutcome::Cancelled {
                reason: TurnCancellationReason::User,
                ..
            }
        ));
        assert_eq!(handle.snapshot().queued, 1);
        gate.open();
        t1.wait().await;
        t3.wait().await;
        handle.close();
        task.await;
    });
}

#[test]
fn remove_at_out_of_bounds_is_none() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let (handle, task) = spawn(backend);
        assert_eq!(handle.remove_at(0), None);
        assert_eq!(handle.remove_visible_at(0), None);
        handle.close();
        task.await;
    });
}

#[test]
fn cancel_r7_precancels_and_drops_compact_7_but_not_8() {
    smol::block_on(async {
        let backend = ScriptedBackend::new();
        let state = Arc::clone(&backend.state);
        let (handle, task) = spawn(backend);

        // Precancel r7 before any work with that correlation exists. Compact 7
        // queued afterwards is dropped; compact 8 is unrelated and runs.
        handle.cancel_correlation("r7", TurnCancellationReason::User);
        handle.push_compact(7).unwrap();
        handle.push_compact(8).unwrap();
        until(|| state.compacts.load(Ordering::SeqCst) >= 1).await;
        assert_eq!(
            state.compacts.load(Ordering::SeqCst),
            1,
            "compact 7 dropped, only compact 8 ran"
        );

        // Targeted queued cancellation removes compact 7 while compact 8
        // survives: park the runner in a gated turn so the compacts stay
        // queued, then cancel r7.
        let gate = Gate::new();
        let backend = ScriptedBackend::gated(Arc::clone(&gate));
        let (handle, task) = spawn(backend);
        let running = handle
            .admit_turn(input("running"), None, "run-1".into())
            .unwrap();
        until(|| backend_reached(&handle)).await;
        handle.push_compact(7).unwrap();
        handle.push_compact(8).unwrap();
        assert_eq!(handle.snapshot().queued, 2);

        handle.cancel_correlation("r7", TurnCancellationReason::User);
        let queue = handle.snapshot().queue;
        assert_eq!(queue.len(), 1, "compact 7 removed, compact 8 survives");
        assert!(matches!(queue[0], QueueProjection::Compact));

        gate.open();
        assert!(matches!(
            running.wait().await,
            TurnOutcome::Completed { .. }
        ));
        handle.close();
        task.await;
    });
}
