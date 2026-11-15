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
use serde::{Deserialize, Serialize};

use crate::types::{TurnId, TurnOutcome};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnResult {
    pub outcome: TurnOutcome,
    pub text: String,
    pub output: Vec<serde_json::Value>,
    pub provenance: TurnProvenance,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnOrigin {
    #[default]
    Internal,
    User,
    Plugin,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    pub fn from_result(result: TurnResult) -> Self {
        Self::from_result_for_actor(result, Arc::new(()))
    }

    pub(crate) fn from_result_for_actor(result: TurnResult, actor_identity: Arc<()>) -> Self {
        Self {
            turn_id: result.outcome.turn_id(),
            actor_identity,
            provenance: result.provenance.clone(),
            shared: Arc::new(Shared {
                outcome: Mutex::new(Some(result.outcome.clone())),
                event: Event::new(),
            }),
            output: TurnOutput(Arc::new(Mutex::new(OutputState {
                result: Some(result),
                ..Default::default()
            }))),
        }
    }

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

    pub(crate) fn replace_sealed_outcome(&self, outcome: &TurnOutcome) {
        let mut state = self
            .output
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(result) = &mut state.result {
            result.outcome = outcome.clone();
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_lite::future::poll_once;
    use makima_providers::TokenUsage;
    use serde_json::json;

    use super::{TurnOrigin, TurnProvenance, TurnResult, TurnTicket};
    use crate::{AgentId, TurnCancellationReason, TurnId, TurnOutcome};

    const TEXT: &str = "frozen response";
    const LATE_TEXT: &str = "late response";
    const PLUGIN: &str = "task";

    #[test]
    fn restored_ticket_is_resolved_without_actor_execution() {
        let result = TurnResult {
            outcome: TurnOutcome::cancelled(
                AgentId::generate(),
                TurnId::generate(),
                TokenUsage::default(),
                2,
                TurnCancellationReason::Interrupted,
            ),
            text: TEXT.into(),
            output: vec![json!({ "answer": [1, true, null] })],
            provenance: TurnProvenance {
                origin: TurnOrigin::Plugin,
                plugin: Some(PLUGIN.into()),
                plugin_generation: Some(3),
                source_agent: Some(AgentId::generate()),
                source_turn: Some(TurnId::generate()),
            },
            duration_ms: 42,
        };
        let encoded = serde_json::to_vec(&result).unwrap();
        assert_eq!(
            serde_json::from_slice::<TurnResult>(&encoded).unwrap(),
            result
        );
        let ticket = TurnTicket::from_result(result.clone());
        assert_eq!(ticket.turn_id(), result.outcome.turn_id());
        assert_eq!(ticket.provenance(), &result.provenance);
        assert!(!ticket.belongs_to(&Arc::new(())));
        assert_eq!(
            smol::block_on(poll_once(ticket.wait())),
            Some(result.outcome.clone())
        );
        assert_eq!(
            smol::block_on(poll_once(ticket.wait_result())),
            Some(result.clone())
        );
        ticket.output().append_text(LATE_TEXT);
        ticket.output().append_output(json!(LATE_TEXT));
        ticket.output().begin_text_attempt();
        ticket.mark_started();
        ticket.resolve(TurnOutcome::cancelled(
            result.outcome.agent_id(),
            result.outcome.turn_id(),
            TokenUsage::default(),
            0,
            TurnCancellationReason::User,
        ));
        assert_eq!(ticket.peek_result(), Some(result.clone()));
        assert_eq!(ticket.peek(), Some(result.outcome));
    }
}
