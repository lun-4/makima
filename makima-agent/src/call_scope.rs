//! One tool call's cancel subtree and the one gate that decides whether the
//! call still answers its turn or has moved to the background.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use event_listener::Event;
use makima_storage::id::MakimaId;
use thiserror::Error;

use crate::cancel::{CancelToken, CancelTrigger};
use crate::types::AgentId;

/// Why a call left its turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovedBy {
    /// The user pressed the background keybind.
    User,
    /// The tool or one of its plugins moved the call.
    Tool,
    /// The call outstayed its deadline.
    Deadline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallHome {
    pub id: String,
    pub tool: Arc<str>,
    pub summary: String,
    pub moved_by: MovedBy,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MoveError {
    #[error("call already moved to the background")]
    AlreadyMoved,
    #[error("call already ended")]
    Cancelled,
}

struct CallState {
    trigger: Option<CancelTrigger>,
    home: Option<CallHome>,
    /// The call finished before any move, so its real result answers the turn.
    foreground: bool,
    /// The dispatcher told the model the call moved; its result is a message.
    receipt: bool,
}

pub struct CallScope {
    id: String,
    tool: Arc<str>,
    summary: String,
    session: Option<MakimaId>,
    agent: Option<AgentId>,
    run: CancelToken,
    cancel: CancelToken,
    state: Mutex<CallState>,
    moved: Event,
    move_tx: Option<flume::Sender<String>>,
}

impl CallScope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        tool: Arc<str>,
        summary: String,
        session: Option<MakimaId>,
        agent: Option<AgentId>,
        run: &CancelToken,
        move_tx: Option<flume::Sender<String>>,
    ) -> Self {
        let (trigger, cancel) = run.child();
        Self {
            id,
            tool,
            summary,
            session,
            agent,
            run: run.clone(),
            cancel,
            state: Mutex::new(CallState {
                trigger: Some(trigger),
                home: None,
                foreground: false,
                receipt: false,
            }),
            moved: Event::new(),
            move_tx,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn tool(&self) -> &str {
        &self.tool
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn session(&self) -> Option<MakimaId> {
        self.session
    }

    pub fn agent(&self) -> Option<AgentId> {
        self.agent
    }

    pub fn cancel(&self) -> &CancelToken {
        &self.cancel
    }

    pub fn home(&self) -> Option<CallHome> {
        self.lock().home.clone()
    }

    pub fn is_moved(&self) -> bool {
        self.lock().home.is_some()
    }

    pub fn moved_listener(&self) -> event_listener::EventListener {
        self.moved.listen()
    }

    pub async fn moved(&self) {
        let listener = self.moved_listener();
        if !self.is_moved() {
            listener.await;
        }
    }

    /// Detaches the call from the turn and hands its trigger to the mover.
    pub fn move_to(&self, home: CallHome) -> Result<CancelTrigger, MoveError> {
        let trigger = {
            let mut state = self.lock();
            if state.home.is_some() || state.trigger.is_none() || state.foreground {
                return Err(MoveError::AlreadyMoved);
            }
            if self.cancel.is_cancelled() {
                return Err(MoveError::Cancelled);
            }
            state.trigger.take().ok_or(MoveError::AlreadyMoved)?
        };
        // Unlink before setting the home: a turn firing after this point
        // cannot reach the call, and one that fired before it is seen here.
        self.cancel.unlink(&self.run);
        if self.cancel.is_cancelled() {
            drop(trigger);
            return Err(MoveError::Cancelled);
        }
        self.lock().home = Some(home);
        if let Some(tx) = &self.move_tx {
            let _ = tx.try_send(self.id.clone());
        }
        self.moved.notify(usize::MAX);
        Ok(trigger)
    }

    pub fn move_home(&self, moved_by: MovedBy) -> Result<CancelTrigger, MoveError> {
        self.move_to(CallHome {
            id: self.id.clone(),
            tool: Arc::clone(&self.tool),
            summary: self.summary.clone(),
            moved_by,
        })
    }

    /// The call finished before a move: its result answers the turn.
    pub fn claim_foreground(&self) -> bool {
        let mut state = self.lock();
        if state.home.is_some() || state.foreground || state.receipt {
            return false;
        }
        state.foreground = true;
        true
    }

    /// A move that still owes the model a receipt. `false` when the call
    /// already claimed its foreground result, which the turn still waits for.
    pub fn claim_receipt(&self) -> bool {
        let mut state = self.lock();
        if state.home.is_some() && !state.foreground {
            state.receipt = true;
            true
        } else {
            false
        }
    }

    /// The model holds a receipt, so the task's result belongs to the mailbox.
    pub fn receipt(&self) -> bool {
        self.lock().receipt
    }

    /// Ends the call unless a move took it first. `false` when the move won.
    pub fn end(&self) -> bool {
        let trigger = self.lock().trigger.take();
        match trigger {
            Some(trigger) => {
                trigger.cancel();
                true
            }
            None => false,
        }
    }

    fn lock(&self) -> MutexGuard<'_, CallState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Fires the call's trigger when the call ends, unless a move took it first.
pub struct CallGuard(Arc<CallScope>);

impl CallGuard {
    pub fn new(scope: Arc<CallScope>) -> Self {
        Self(scope)
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        self.0.end();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const CALL_ID: &str = "toolu_1";
    const TOOL: &str = "bash";

    fn scope_under(run: &CancelToken) -> (Arc<CallScope>, CallGuard) {
        let scope = Arc::new(CallScope::new(
            CALL_ID.to_owned(),
            Arc::from(TOOL),
            "cargo test".into(),
            None,
            None,
            run,
            None,
        ));
        let guard = CallGuard::new(Arc::clone(&scope));
        (scope, guard)
    }

    fn home() -> CallHome {
        CallHome {
            id: CALL_ID.to_owned(),
            tool: Arc::from(TOOL),
            summary: "cargo test".into(),
            moved_by: MovedBy::User,
        }
    }

    #[test]
    fn the_guard_cancels_the_call_when_it_ends() {
        let (_run_trigger, run) = CancelToken::new();
        let (scope, guard) = scope_under(&run);
        assert!(!scope.cancel().is_cancelled());

        drop(guard);
        assert!(scope.cancel().is_cancelled());
        assert!(!run.is_cancelled());
    }

    #[test]
    fn a_move_takes_the_trigger_so_the_guard_does_nothing() {
        let (_run_trigger, run) = CancelToken::new();
        let (scope, guard) = scope_under(&run);
        let trigger = scope.move_to(home()).unwrap();

        drop(guard);
        assert!(!scope.cancel().is_cancelled());

        drop(trigger);
        assert!(scope.cancel().is_cancelled());
    }

    #[test]
    fn the_run_ending_after_a_move_reaches_nothing() {
        let (run_trigger, run) = CancelToken::new();
        let (scope, _guard) = scope_under(&run);
        let (_nested_trigger, nested) = scope.cancel().child();
        let _trigger = scope.move_to(home()).unwrap();

        run_trigger.cancel();
        assert!(!scope.cancel().is_cancelled());
        assert!(!nested.is_cancelled());
    }

    #[test]
    fn the_run_ending_before_a_move_cancels_the_call_and_refuses_the_move() {
        let (run_trigger, run) = CancelToken::new();
        let (scope, _guard) = scope_under(&run);
        run_trigger.cancel();

        assert!(scope.cancel().is_cancelled());
        assert!(matches!(scope.move_to(home()), Err(MoveError::Cancelled)));
    }

    #[test_case(true ; "after_a_move")]
    #[test_case(false ; "after_the_call_ended")]
    fn a_move_without_the_trigger_is_refused(already_moved: bool) {
        let (_run_trigger, run) = CancelToken::new();
        let (scope, guard) = scope_under(&run);
        if already_moved {
            let _first = scope.move_to(home()).unwrap();
        } else {
            drop(guard);
        }
        assert!(matches!(
            scope.move_to(home()),
            Err(MoveError::AlreadyMoved)
        ));
    }

    #[test]
    fn the_moved_listener_wakes_on_the_move() {
        let (_run_trigger, run) = CancelToken::new();
        let (scope, _guard) = scope_under(&run);
        let listener = scope.moved_listener();
        assert!(!scope.is_moved());

        let _trigger = scope.move_to(home()).unwrap();
        smol::block_on(listener);
        assert_eq!(scope.home(), Some(home()));
    }

    #[test]
    fn a_finished_foreground_call_keeps_its_result() {
        let (_run_trigger, run) = CancelToken::new();
        let (scope, _guard) = scope_under(&run);
        assert!(scope.claim_foreground());
        assert!(!scope.claim_receipt());
        assert!(matches!(
            scope.move_to(home()),
            Err(MoveError::AlreadyMoved)
        ));
    }

    #[test]
    fn a_moved_call_owes_a_receipt_until_it_finishes_in_the_background() {
        let (_run_trigger, run) = CancelToken::new();
        let (scope, _guard) = scope_under(&run);
        let _trigger = scope.move_to(home()).unwrap();
        assert!(scope.claim_receipt());
        assert!(!scope.claim_foreground());
    }

    #[test]
    fn a_move_notifies_the_batch() {
        let (_run_trigger, run) = CancelToken::new();
        let (tx, rx) = flume::unbounded();
        let scope = Arc::new(CallScope::new(
            CALL_ID.to_owned(),
            Arc::from(TOOL),
            "cargo test".into(),
            None,
            None,
            &run,
            Some(tx),
        ));
        let _guard = CallGuard::new(Arc::clone(&scope));
        let _trigger = scope.move_home(MovedBy::User).unwrap();
        assert_eq!(rx.try_recv().as_deref(), Ok(CALL_ID));
    }
}
