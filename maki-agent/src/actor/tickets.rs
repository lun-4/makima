//! Exact outcome waiting for admitted turns.
//!
//! A [`TurnTicket`] is created the moment a turn is admitted, so an async
//! waiter can never strand: it resolves as soon as the actor retains the
//! turn's outcome, whether the turn completed, failed, was cancelled, was
//! removed, or was terminalized by a close/clear.

use std::sync::{Arc, Mutex};

use event_listener::Event;

use crate::types::{TurnId, TurnOutcome};

#[derive(Clone)]
pub struct TurnTicket {
    turn_id: TurnId,
    shared: Arc<Shared>,
}

struct Shared {
    outcome: Mutex<Option<TurnOutcome>>,
    event: Event,
}

impl TurnTicket {
    pub(crate) fn new(turn_id: TurnId) -> Self {
        Self {
            turn_id,
            shared: Arc::new(Shared {
                outcome: Mutex::new(None),
                event: Event::new(),
            }),
        }
    }

    /// A ticket for a root-started turn. Root inputs carry no [`TurnId`]
    /// until they start, so nothing external can ever wait on it.
    pub(crate) fn new_anonymous() -> Self {
        Self::new(TurnId::generate())
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
