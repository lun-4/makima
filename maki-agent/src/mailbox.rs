use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_providers::Message;
use maki_storage::id::MakiId;
use thiserror::Error;

const MAILBOX_CAPACITY: usize = 100;

#[derive(Default)]
struct State {
    pending: VecDeque<Message>,
    wake: bool,
}

#[derive(Debug, Error)]
#[error("session not live: {0}")]
pub struct MailboxError(MakiId);

#[derive(Clone)]
pub struct SessionMailbox {
    session_id: MakiId,
    state: Arc<Mutex<State>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SessionMailbox {
    pub fn new(session_id: MakiId) -> Self {
        Self {
            session_id,
            state: Arc::default(),
        }
    }

    pub fn notify(session_id: MakiId, text: String, wake: bool) -> Result<(), MailboxError> {
        let mailbox = crate::session_coordinator::SessionCoordinatorHandle::resolve(session_id)
            .and_then(|coordinator| coordinator.mailbox())
            .map_err(|_| MailboxError(session_id))?;
        mailbox.push(text, wake);
        Ok(())
    }

    pub(crate) fn push(&self, text: String, wake: bool) {
        let mut state = lock(&self.state);
        if state.pending.len() == MAILBOX_CAPACITY {
            state.pending.pop_front();
        }
        state.pending.push_back(Message::observation(text));
        state.wake |= wake;
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

    pub fn session_id(&self) -> MakiId {
        self.session_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(message: &Message) -> &str {
        message.user_text().unwrap()
    }

    #[test]
    fn notifications_drain_in_order_and_clear_wake() {
        let id = MakiId::generate();
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
        let mailbox = SessionMailbox::new(MakiId::generate());
        mailbox.push("built".into(), false);

        assert!(mailbox.claim_wake().is_empty());
        assert_eq!(mailbox.drain().len(), 1);
    }

    #[test]
    fn waking_notification_claims_all_pending_messages() {
        let mailbox = SessionMailbox::new(MakiId::generate());
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
        let mailbox = SessionMailbox::new(MakiId::generate());
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
        let first = SessionMailbox::new(MakiId::generate());
        let second = first.clone();
        first.push("built".into(), false);

        assert_eq!(second.drain().len(), 1);
        assert!(first.drain().is_empty());
    }
}
