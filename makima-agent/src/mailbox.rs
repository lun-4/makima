use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use makima_providers::Message;
use makima_storage::id::MakimaId;
use thiserror::Error;

use crate::call_scope::{CallScope, MoveError, MovedBy};
use crate::cancel::CancelTrigger;
use crate::types::AgentId;

const MAILBOX_CAPACITY: usize = 100;
const NOTHING_RUNNING: &str = "no running tool call to move";

#[derive(Default)]
struct State {
    pending: VecDeque<Message>,
    wake: bool,
    /// Receipts the model holds and still waits on.
    outstanding: Vec<String>,
    calls: Vec<CallEntry>,
}

struct CallEntry {
    scope: Arc<CallScope>,
    started_at: Instant,
    moved: bool,
    finished: bool,
    trigger: Option<CancelTrigger>,
}

impl CallEntry {
    fn running(&self) -> bool {
        self.moved && !self.finished
    }
}

/// What `/tasks` and `task_stop` see of the calls in one session.
#[derive(Debug, Clone)]
pub struct BackgroundCallInfo {
    pub id: String,
    pub tool: Arc<str>,
    pub summary: String,
    pub started_at: Instant,
    pub moved: bool,
    pub finished: bool,
}

#[derive(Debug, Error)]
pub enum BackgroundMoveError {
    #[error("{NOTHING_RUNNING}")]
    NothingRunning,
    #[error(transparent)]
    Call(#[from] MoveError),
}

#[derive(Debug, Error)]
#[error("session not live: {0}")]
pub struct MailboxError(MakimaId);

#[derive(Clone)]
pub struct SessionMailbox {
    session_id: MakimaId,
    state: Arc<Mutex<State>>,
}

pub struct PreparedSessionMailbox(SessionMailbox);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn push_locked(state: &mut State, text: String, wake: bool) {
    if state.pending.len() == MAILBOX_CAPACITY {
        state.pending.pop_front();
    }
    state.pending.push_back(Message::observation(text));
    state.wake |= wake;
}

fn track_locked(state: &mut State, id: &str) {
    if !state.outstanding.iter().any(|known| known == id) {
        state.outstanding.push(id.to_owned());
    }
}

fn unknown_background_task(id: &str, calls: &[CallEntry]) -> String {
    let running: Vec<&str> = calls
        .iter()
        .filter(|entry| entry.running())
        .map(|entry| entry.scope.id())
        .collect();
    if running.is_empty() {
        format!("no running background task {id}")
    } else {
        format!(
            "no running background task {id}; running: {}",
            running.join(", ")
        )
    }
}

impl SessionMailbox {
    pub fn new(session_id: MakimaId) -> Self {
        Self {
            session_id,
            state: Arc::default(),
        }
    }

    pub fn prepare(session_id: MakimaId) -> PreparedSessionMailbox {
        PreparedSessionMailbox(Self::new(session_id))
    }

    pub fn open(session_id: MakimaId) -> Result<Self, MailboxError> {
        crate::session_coordinator::SessionCoordinatorHandle::resolve(session_id)
            .and_then(|coordinator| coordinator.mailbox())
            .map_err(|_| MailboxError(session_id))
    }

    pub fn notify(session_id: MakimaId, text: String, wake: bool) -> Result<(), MailboxError> {
        let mailbox = Self::open(session_id)?;
        mailbox.push(text, wake);
        Ok(())
    }

    /// Registers a running tool call so Ctrl+B and `task_stop` can find it.
    pub fn register_call(&self, scope: Arc<CallScope>) {
        lock(&self.state).calls.push(CallEntry {
            scope,
            started_at: Instant::now(),
            moved: false,
            finished: false,
            trigger: None,
        });
    }

    /// Moves one running call to the background and hands its trigger to the
    /// mailbox, which owns the work until it ends. `moved_by` says who asked,
    /// which decides what the model is told.
    pub fn move_call(&self, id: &str, moved_by: MovedBy) -> Result<(), BackgroundMoveError> {
        let scope = {
            let mut state = lock(&self.state);
            let scope = {
                let entry = state
                    .calls
                    .iter_mut()
                    .find(|entry| entry.scope.id() == id && !entry.moved && !entry.finished)
                    .ok_or(BackgroundMoveError::NothingRunning)?;
                entry.moved = true;
                Arc::clone(&entry.scope)
            };
            // Track before the move signal goes out: dispatch can settle a
            // fast call before this function stores the trigger.
            track_locked(&mut state, id);
            scope
        };
        let trigger = match scope.move_home(moved_by) {
            Ok(trigger) => trigger,
            Err(MoveError::AlreadyMoved) if scope.is_moved() => return Ok(()),
            Err(MoveError::AlreadyMoved) => {
                self.unmark_call(id);
                return Err(MoveError::Cancelled.into());
            }
            Err(error) => {
                self.unmark_call(id);
                return Err(error.into());
            }
        };
        let mut state = lock(&self.state);
        if let Some(entry) = state.calls.iter_mut().find(|entry| entry.scope.id() == id) {
            entry.trigger = Some(trigger);
        }
        Ok(())
    }

    /// A move that never happened leaves no receipt behind.
    fn unmark_call(&self, id: &str) {
        let mut state = lock(&self.state);
        state.outstanding.retain(|known| known != id);
        if let Some(entry) = state.calls.iter_mut().find(|entry| entry.scope.id() == id)
            && entry.trigger.is_none()
        {
            entry.moved = false;
        }
    }

    /// Moves the most recently started running call, which is the one the
    /// user just pressed Ctrl+B on. `agent` narrows it to one chat's calls.
    pub fn move_latest_for(&self, agent: Option<AgentId>) -> Result<String, BackgroundMoveError> {
        let id = {
            let state = lock(&self.state);
            state
                .calls
                .iter()
                .rev()
                .find(|entry| {
                    !entry.moved
                        && !entry.finished
                        && agent.is_none_or(|agent| entry.scope.agent() == Some(agent))
                })
                .map(|entry| entry.scope.id().to_owned())
                .ok_or(BackgroundMoveError::NothingRunning)?
        };
        self.move_call(&id, MovedBy::User)?;
        Ok(id)
    }

    pub fn move_latest(&self) -> Result<String, BackgroundMoveError> {
        self.move_latest_for(None)
    }

    /// The receipt never went out, so the model does not wait on it.
    pub(crate) fn forget_receipt(&self, id: &str) {
        lock(&self.state).outstanding.retain(|known| known != id);
    }

    /// A foreground call ended: nothing waits on it.
    pub(crate) fn finish_call(&self, id: &str) {
        let mut state = lock(&self.state);
        state.calls.retain(|entry| entry.scope.id() != id);
        state.outstanding.retain(|known| known != id);
    }

    /// The moved work ended and its result is ready. `false` when the model
    /// never got a receipt, so the result must not answer anything.
    pub(crate) fn settle_call(&self, id: &str, text: String, wake: bool) -> bool {
        let mut state = lock(&self.state);
        state.calls.retain(|entry| entry.scope.id() != id);
        let tracked = state.outstanding.iter().position(|known| known == id);
        let Some(index) = tracked else {
            return false;
        };
        state.outstanding.remove(index);
        push_locked(&mut state, text, wake);
        true
    }

    /// The work died without a result, so the model is told its receipt is
    /// lost instead of waiting forever.
    pub(crate) fn lose_call(&self, id: &str, text: String, wake: bool) -> bool {
        self.settle_call(id, text, wake)
    }

    /// Stops one moved call. Its final result still arrives as a message.
    pub fn stop_call(&self, id: &str) -> Result<(), String> {
        let trigger = {
            let mut state = lock(&self.state);
            let Some(entry) = state
                .calls
                .iter_mut()
                .find(|entry| entry.running() && entry.scope.id() == id)
            else {
                return Err(unknown_background_task(id, &state.calls));
            };
            entry.finished = true;
            entry.trigger.take()
        };
        drop(trigger);
        Ok(())
    }

    pub fn background_calls(&self) -> Vec<BackgroundCallInfo> {
        lock(&self.state)
            .calls
            .iter()
            .map(|entry| BackgroundCallInfo {
                id: entry.scope.id().to_owned(),
                tool: Arc::from(entry.scope.tool()),
                summary: entry.scope.summary().to_owned(),
                started_at: entry.started_at,
                moved: entry.moved,
                finished: entry.finished,
            })
            .collect()
    }

    pub fn outstanding(&self) -> usize {
        lock(&self.state).outstanding.len()
    }

    pub(crate) fn push(&self, text: String, wake: bool) {
        push_locked(&mut lock(&self.state), text, wake);
    }

    pub fn drain(&self) -> Vec<Message> {
        let mut state = lock(&self.state);
        state.wake = false;
        state.pending.drain(..).collect()
    }

    pub fn claim_wake(&self) -> Vec<Message> {
        let mut state = lock(&self.state);
        if !state.wake {
            return Vec::new();
        }
        state.wake = false;
        state.pending.drain(..).collect()
    }

    pub fn session_id(&self) -> MakimaId {
        self.session_id
    }
}

impl PreparedSessionMailbox {
    pub fn mailbox(&self) -> SessionMailbox {
        self.0.clone()
    }

    pub fn activate(self) -> SessionMailbox {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(message: &Message) -> &str {
        message.user_text().unwrap()
    }

    #[test]
    fn prepared_mailbox_preserves_state_on_activation() {
        let prepared = SessionMailbox::prepare(MakimaId::generate());
        let mailbox = prepared.mailbox();
        mailbox.push("ready".into(), true);

        let activated = prepared.activate();
        assert_eq!(
            activated.claim_wake().iter().map(text).collect::<Vec<_>>(),
            ["ready"]
        );
    }

    #[test]
    fn notifications_drain_in_order_and_clear_wake() {
        let id = MakimaId::generate();
        let mailbox = SessionMailbox::new(id);
        mailbox.push("first".into(), true);
        mailbox.push("second".into(), true);

        let messages = mailbox.drain();
        assert_eq!(
            messages.iter().map(text).collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert!(messages.iter().all(Message::is_observation));
        assert!(mailbox.claim_wake().is_empty());
    }

    #[test]
    fn quiet_notifications_do_not_claim_a_wake() {
        let mailbox = SessionMailbox::new(MakimaId::generate());
        mailbox.push("built".into(), false);

        assert!(mailbox.claim_wake().is_empty());
        assert_eq!(mailbox.drain().len(), 1);
    }

    #[test]
    fn waking_notification_claims_all_pending_messages() {
        let mailbox = SessionMailbox::new(MakimaId::generate());
        mailbox.push("quiet".into(), false);
        mailbox.push("wake".into(), true);

        let messages = mailbox.claim_wake();
        assert_eq!(
            messages.iter().map(text).collect::<Vec<_>>(),
            ["quiet", "wake"]
        );
        assert!(mailbox.drain().is_empty());
    }

    #[test]
    fn notifications_drop_the_oldest_message_at_capacity() {
        let mailbox = SessionMailbox::new(MakimaId::generate());
        for index in 0..=MAILBOX_CAPACITY {
            mailbox.push(index.to_string(), false);
        }

        let messages = mailbox.drain();
        assert_eq!(messages.len(), MAILBOX_CAPACITY);
        assert_eq!(text(&messages[0]), "1");
        assert_eq!(text(messages.last().unwrap()), MAILBOX_CAPACITY.to_string());
    }

    #[test]
    fn clones_share_state_without_owning_registration_lifetime() {
        let first = SessionMailbox::new(MakimaId::generate());
        let second = first.clone();
        first.push("built".into(), false);

        assert_eq!(second.drain().len(), 1);
        assert!(first.drain().is_empty());
    }
}

#[cfg(test)]
mod background_tests {
    use super::*;
    use crate::cancel::CancelToken;

    const CALL_ID: &str = "call_1";
    const TOOL: &str = "bash";
    const SUMMARY: &str = "cargo test";
    const RESULT: &str = "[background] bash finished";

    fn scope(run: &CancelToken) -> Arc<CallScope> {
        scope_for(run, None)
    }

    fn scope_for(run: &CancelToken, agent: Option<AgentId>) -> Arc<CallScope> {
        Arc::new(CallScope::new(
            CALL_ID.into(),
            Arc::from(TOOL),
            SUMMARY.into(),
            None,
            agent,
            run,
            None,
        ))
    }

    #[test]
    fn move_latest_marks_the_only_running_call() {
        let (_run_trigger, run) = CancelToken::new();
        let mailbox = SessionMailbox::new(MakimaId::generate());
        let call = scope(&run);
        mailbox.register_call(Arc::clone(&call));

        assert_eq!(mailbox.move_latest().unwrap(), CALL_ID);
        assert!(call.is_moved());
        assert!(mailbox.background_calls()[0].moved);
        assert!(!mailbox.background_calls()[0].finished);
    }

    #[test]
    fn move_latest_for_leaves_another_agents_call_alone() {
        let (_run_trigger, run) = CancelToken::new();
        let mailbox = SessionMailbox::new(MakimaId::generate());
        let first_agent = AgentId::generate();
        let second_agent = AgentId::generate();
        let first = scope_for(&run, Some(first_agent));
        let second = scope_for(&run, Some(second_agent));
        mailbox.register_call(Arc::clone(&first));
        mailbox.register_call(Arc::clone(&second));

        mailbox.move_latest_for(Some(first_agent)).unwrap();
        assert!(first.is_moved());
        assert!(!second.is_moved());
    }

    #[test]
    fn move_latest_without_a_call_reports_nothing_running() {
        let mailbox = SessionMailbox::new(MakimaId::generate());
        assert!(matches!(
            mailbox.move_latest(),
            Err(BackgroundMoveError::NothingRunning)
        ));
    }

    #[test]
    fn settling_answers_a_moved_calls_receipt() {
        let (_run_trigger, run) = CancelToken::new();
        let mailbox = SessionMailbox::new(MakimaId::generate());
        let call = scope(&run);
        mailbox.register_call(Arc::clone(&call));
        mailbox.move_latest().unwrap();

        assert_eq!(mailbox.outstanding(), 1);
        assert!(mailbox.settle_call(CALL_ID, RESULT.into(), true));
        assert_eq!(mailbox.outstanding(), 0);
        assert_eq!(mailbox.drain()[0].user_text(), Some(RESULT));
        assert!(mailbox.background_calls().is_empty());
    }

    #[test]
    fn a_lost_call_still_answers_its_receipt() {
        let (_run_trigger, run) = CancelToken::new();
        let mailbox = SessionMailbox::new(MakimaId::generate());
        mailbox.register_call(scope(&run));
        mailbox.move_latest().unwrap();

        assert!(mailbox.lose_call(CALL_ID, "lost".into(), false));
        assert_eq!(mailbox.outstanding(), 0);
        assert_eq!(mailbox.drain()[0].user_text(), Some("lost"));
    }

    #[test]
    fn stopping_a_moved_call_fires_it_and_keeps_the_reply_path() {
        let (_run_trigger, run) = CancelToken::new();
        let mailbox = SessionMailbox::new(MakimaId::generate());
        let call = scope(&run);
        mailbox.register_call(Arc::clone(&call));
        mailbox.move_latest().unwrap();

        mailbox.stop_call(CALL_ID).unwrap();
        assert!(call.cancel().is_cancelled());
        assert!(mailbox.background_calls()[0].finished);
        assert!(mailbox.settle_call(CALL_ID, RESULT.into(), true));
        assert_eq!(mailbox.drain()[0].user_text(), Some(RESULT));
    }

    #[test]
    fn stop_refuses_an_unknown_or_foreground_call() {
        let (_run_trigger, run) = CancelToken::new();
        let mailbox = SessionMailbox::new(MakimaId::generate());
        mailbox.register_call(scope(&run));

        assert!(mailbox.stop_call(CALL_ID).is_err());
        assert!(mailbox.stop_call("other").is_err());
    }
}
