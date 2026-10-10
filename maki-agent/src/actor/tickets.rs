//! Exact outcome waiting for admitted turns.
//!
//! A [`TurnTicket`] is created the moment a turn is admitted, so an async
//! waiter can never strand: it resolves as soon as the actor retains the
//! turn's outcome, whether the turn completed, failed, was cancelled, was
//! removed, or was terminalized by a close/clear.

use std::mem::take;
use std::sync::{Arc, Mutex};

use event_listener::Event;

use crate::types::{TurnId, TurnOutcome};

#[derive(Debug, Clone)]
pub struct TurnResult {
    pub outcome: TurnOutcome,
    pub text: String,
    pub output: Vec<serde_json::Value>,
    pub provenance: TurnProvenance,
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
    attempt_start: usize,
    output: Vec<serde_json::Value>,
    result: Option<TurnResult>,
}

#[derive(Debug, Clone, Default)]
pub struct TurnOutput(Arc<Mutex<OutputState>>);

impl TurnOutput {
    pub fn begin_message(&self) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            state.attempt_start = state.text.len();
        }
    }

    pub fn reset_message(&self) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            let start = state.attempt_start;
            state.text.truncate(start);
        }
    }

    pub fn complete_message(&self, text: &str) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.result.is_none() {
            let start = state.attempt_start;
            state.text.truncate(start);
            state.text.push_str(text);
        }
    }

    pub fn begin_text_attempt(&self) {
        self.begin_message();
    }

    pub fn reset_text_attempt(&self) {
        self.reset_message();
    }

    pub fn reconcile_text_attempt(&self, text: &str) {
        self.complete_message(text);
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
    pub(crate) fn new_anonymous(actor_identity: Arc<()>) -> Self {
        Self::new(TurnId::generate(), actor_identity)
    }

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

    pub(crate) fn resolve(&self, outcome: TurnOutcome) {
        let mut outcome_slot = self
            .shared
            .outcome
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if outcome_slot.is_none() {
            let mut state = self.output.0.lock().unwrap_or_else(|e| e.into_inner());
            state.result = Some(TurnResult {
                outcome: outcome.clone(),
                text: take(&mut state.text),
                output: take(&mut state.output),
                provenance: self.provenance.clone(),
            });
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
