//! Exact outcome waiting for admitted turns.
//!
//! A [`TurnTicket`] is created the moment a turn is admitted, so an async
//! waiter can never strand: it resolves as soon as the actor retains the
//! turn's outcome, whether the turn completed, failed, was cancelled, was
//! removed, or was terminalized by a close/clear.

use std::mem::take;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use event_listener::Event;

use crate::types::{TurnId, TurnOutcome};

#[derive(Debug, Clone)]
pub struct TurnResult {
    pub outcome: TurnOutcome,
    pub text: String,
    pub output: Vec<serde_json::Value>,
    pub provenance: TurnProvenance,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum TurnOrigin {
    #[default]
    Internal,
    User,
    Plugin,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnProvenance {
    pub origin: TurnOrigin,
    pub plugin: Option<String>,
    pub plugin_generation: Option<u64>,
    pub source_agent: Option<crate::AgentId>,
    pub source_turn: Option<TurnId>,
}

#[derive(Debug, Default)]
struct OutputState {
    text: String,
    output: Vec<serde_json::Value>,
    result: Option<TurnResult>,
    started: Option<Instant>,
    terminal_sender: Option<crate::EventSender>,
}

#[derive(Debug, Clone, Default)]
pub struct TurnOutput(Arc<Mutex<OutputState>>);

impl TurnOutput {
    /// Registers the presentation sink used only after the actor seals its result.
    pub fn set_terminal_sender(&self, sender: crate::EventSender) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .terminal_sender = Some(sender.without_output());
    }

    pub(super) fn take_terminal_sender(&self) -> Option<crate::EventSender> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .terminal_sender
            .take()
    }

    pub fn begin_text_attempt(&self) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            state.text.clear();
        }
    }

    pub fn reset_text_attempt(&self) {
        self.begin_text_attempt();
    }

    pub fn reconcile_text_attempt(&self, text: &str) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            state.text.clear();
            state.text.push_str(text);
        }
    }

    pub fn append_text(&self, text: &str) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            state.text.push_str(text);
        }
    }

    pub fn append_output(&self, output: serde_json::Value) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            state.output.push(output);
        }
    }
}

#[derive(Clone)]
pub struct TurnTicket {
    turn_id: TurnId,
    actor_identity: Arc<()>,
    shared: Arc<Shared>,
    output: TurnOutput,
    provenance: TurnProvenance,
}

struct Shared {
    outcome: Mutex<Option<TurnOutcome>>,
    event: Event,
}

impl TurnTicket {
    pub(crate) fn new(turn_id: TurnId, actor_identity: Arc<()>) -> Self {
        Self {
            turn_id,
            actor_identity,
            output: TurnOutput::default(),
            provenance: TurnProvenance::default(),
            shared: Arc::new(Shared {
                outcome: Mutex::new(None),
                event: Event::new(),
            }),
        }
    }

    /// Assigns a ticket when a root input starts rather than when it queues.
    pub(crate) fn with_provenance(mut self, provenance: TurnProvenance) -> Self {
        self.provenance = provenance;
        self
    }

    pub fn provenance(&self) -> &TurnProvenance {
        &self.provenance
    }

    pub fn output(&self) -> TurnOutput {
        self.output.clone()
    }

    pub fn peek_result(&self) -> Option<TurnResult> {
        self.output
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .result
            .clone()
    }

    pub async fn wait_result(&self) -> TurnResult {
        self.wait().await;
        self.peek_result()
            .expect("resolved ticket has a frozen result")
    }

    pub(crate) fn belongs_to(&self, actor_identity: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.actor_identity, actor_identity)
    }

    pub fn turn_id(&self) -> TurnId {
        self.turn_id
    }

    pub(crate) fn mark_started(&self) {
        let mut state = self.output.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() && state.started.is_none() {
            state.started = Some(Instant::now());
        }
    }

    /// Freezes the result for [`Self::peek_result`] without waking waiters.
    pub(crate) fn seal(&self, outcome: &TurnOutcome) {
        let mut state = self.output.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            state.result = Some(TurnResult {
                outcome: outcome.clone(),
                text: take(&mut state.text),
                output: take(&mut state.output),
                provenance: self.provenance.clone(),
                duration_ms: state.started.map_or(0, |start| {
                    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
                }),
            });
        }
    }

    pub(crate) fn resolve(&self, outcome: TurnOutcome) {
        let mut outcome_slot = self
            .shared
            .outcome
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if outcome_slot.is_none() {
            self.seal(&outcome);
            *outcome_slot = Some(outcome);
            self.shared.event.notify(usize::MAX);
        }
    }

    /// Waits exactly for this turn's outcome. Returns immediately when it is
    /// already resolved; wakes the moment the actor retains it. Cannot strand.
    pub async fn wait(&self) -> TurnOutcome {
        loop {
            if let Some(outcome) = self.peek() {
                return outcome;
            }
            let listener = self.shared.event.listen();
            if let Some(outcome) = self.peek() {
                return outcome;
            }
            listener.await;
        }
    }

    pub fn peek(&self) -> Option<TurnOutcome> {
        self.shared
            .outcome
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}
