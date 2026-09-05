use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};

use maki_providers::Message;
use maki_storage::id::MakiId;
use serde_json::Value;
use thiserror::Error;

const MAILBOX_CAPACITY: usize = 100;

static MAILBOXES: LazyLock<Mutex<HashMap<MakiId, Weak<Mutex<State>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Default)]
struct State {
    pending: VecDeque<Message>,
    wake: bool,
    plugin_data: HashMap<String, Value>,
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
    pub fn register(session_id: MakiId) -> Self {
        Self::register_with_data(session_id, HashMap::new())
    }

    pub fn register_with_data(session_id: MakiId, plugin_data: HashMap<String, Value>) -> Self {
        let mut mailboxes = lock(&MAILBOXES);
        if let Some(state) = mailboxes.get(&session_id).and_then(Weak::upgrade) {
            return Self { session_id, state };
        }

        let state = Arc::new(Mutex::new(State {
            plugin_data,
            ..State::default()
        }));
        mailboxes.insert(session_id, Arc::downgrade(&state));
        Self { session_id, state }
    }

    pub fn notify(session_id: MakiId, text: String, wake: bool) -> Result<(), MailboxError> {
        let mailbox = {
            let mut mailboxes = lock(&MAILBOXES);
            let Some(state) = mailboxes.get(&session_id).and_then(Weak::upgrade) else {
                mailboxes.remove(&session_id);
                return Err(MailboxError(session_id));
            };
            Self { session_id, state }
        };
        let mut state = lock(&mailbox.state);
        if state.pending.len() == MAILBOX_CAPACITY {
            state.pending.pop_front();
        }
        state.pending.push_back(Message::observation(text));
        state.wake |= wake;
        Ok(())
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

    pub fn plugin_data(session_id: MakiId, plugin: &str) -> Result<Option<Value>, MailboxError> {
        let state = Self::state(session_id)?;
        let value = lock(&state).plugin_data.get(plugin).cloned();
        Ok(value)
    }

    pub fn set_plugin_data(
        session_id: MakiId,
        plugin: String,
        value: Option<Value>,
    ) -> Result<(), MailboxError> {
        let state = Self::state(session_id)?;
        let mut state = lock(&state);
        if let Some(value) = value {
            state.plugin_data.insert(plugin, value);
        } else {
            state.plugin_data.remove(&plugin);
        }
        Ok(())
    }

    pub fn plugin_data_snapshot(
        session_id: MakiId,
    ) -> Result<HashMap<String, Value>, MailboxError> {
        let state = Self::state(session_id)?;
        let plugin_data = lock(&state).plugin_data.clone();
        Ok(plugin_data)
    }

    pub fn snapshot_plugin_data(&self) -> HashMap<String, Value> {
        lock(&self.state).plugin_data.clone()
    }

    fn state(session_id: MakiId) -> Result<Arc<Mutex<State>>, MailboxError> {
        let mut mailboxes = lock(&MAILBOXES);
        let Some(state) = mailboxes.get(&session_id).and_then(Weak::upgrade) else {
            mailboxes.remove(&session_id);
            return Err(MailboxError(session_id));
        };
        Ok(state)
    }
}

impl Drop for SessionMailbox {
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) != 1 {
            return;
        }
        let weak = Arc::downgrade(&self.state);
        let mut mailboxes = lock(&MAILBOXES);
        if Arc::strong_count(&self.state) == 1
            && mailboxes
                .get(&self.session_id)
                .is_some_and(|registered| registered.ptr_eq(&weak))
        {
            mailboxes.remove(&self.session_id);
        }
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
        let mailbox = SessionMailbox::register(id);
        SessionMailbox::notify(id, "first".into(), true).unwrap();
        SessionMailbox::notify(id, "second".into(), true).unwrap();

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
        let id = MakiId::generate();
        let mailbox = SessionMailbox::register(id);
        SessionMailbox::notify(id, "built".into(), false).unwrap();

        assert!(mailbox.claim_wake().is_empty());
        assert_eq!(mailbox.drain().len(), 1);
    }

    #[test]
    fn waking_notification_claims_all_pending_messages() {
        let id = MakiId::generate();
        let mailbox = SessionMailbox::register(id);
        SessionMailbox::notify(id, "quiet".into(), false).unwrap();
        SessionMailbox::notify(id, "wake".into(), true).unwrap();

        let messages = mailbox.claim_wake();
        assert_eq!(
            messages.iter().map(text).collect::<Vec<_>>(),
            ["quiet", "wake"]
        );
        assert!(mailbox.drain().is_empty());
    }

    #[test]
    fn notifications_drop_the_oldest_message_at_capacity() {
        let id = MakiId::generate();
        let mailbox = SessionMailbox::register(id);
        for index in 0..=MAILBOX_CAPACITY {
            SessionMailbox::notify(id, index.to_string(), false).unwrap();
        }

        let messages = mailbox.drain();
        assert_eq!(messages.len(), MAILBOX_CAPACITY);
        assert_eq!(text(&messages[0]), "1");
        assert_eq!(text(messages.last().unwrap()), MAILBOX_CAPACITY.to_string());
    }

    #[test]
    fn registrations_for_the_same_id_share_state() {
        let id = MakiId::generate();
        let first = SessionMailbox::register(id);
        let second = SessionMailbox::register(id);
        SessionMailbox::notify(id, "built".into(), false).unwrap();

        assert_eq!(second.drain().len(), 1);
        assert!(first.drain().is_empty());
    }

    #[test]
    fn plugin_data_is_seeded_shared_and_removable() {
        let id = MakiId::generate();
        let mut initial = HashMap::new();
        initial.insert("research".into(), serde_json::json!({"run": 2}));
        let mailbox = SessionMailbox::register_with_data(id, initial);

        assert_eq!(
            SessionMailbox::plugin_data(id, "research").unwrap(),
            Some(serde_json::json!({"run": 2}))
        );
        SessionMailbox::set_plugin_data(id, "research".into(), Some(serde_json::json!({"run": 3})))
            .unwrap();
        assert_eq!(
            SessionMailbox::plugin_data_snapshot(id).unwrap()["research"],
            serde_json::json!({"run": 3})
        );
        SessionMailbox::set_plugin_data(id, "research".into(), None).unwrap();
        assert!(SessionMailbox::plugin_data_snapshot(id).unwrap().is_empty());
        drop(mailbox);
    }

    #[test]
    fn dropping_the_last_registration_closes_the_mailbox() {
        let id = MakiId::generate();
        drop(SessionMailbox::register(id));

        assert!(!lock(&MAILBOXES).contains_key(&id));
        assert!(SessionMailbox::notify(id, "late".into(), false).is_err());
    }

    #[test]
    fn dropping_one_registration_keeps_the_shared_mailbox() {
        let id = MakiId::generate();
        let first = SessionMailbox::register(id);
        let second = SessionMailbox::register(id);

        drop(first);

        assert!(lock(&MAILBOXES).contains_key(&id));
        SessionMailbox::notify(id, "built".into(), false).unwrap();
        assert_eq!(second.drain().len(), 1);
    }

    #[test]
    fn stale_drop_does_not_remove_a_replacement() {
        let id = MakiId::generate();
        let stale = SessionMailbox::register(id);
        let replacement = SessionMailbox {
            session_id: id,
            state: Arc::new(Mutex::new(State::default())),
        };
        lock(&MAILBOXES).insert(id, Arc::downgrade(&replacement.state));

        drop(stale);
        SessionMailbox::notify(id, "built".into(), false).unwrap();

        assert_eq!(replacement.drain().len(), 1);
    }

    #[test]
    fn legacy_and_canonical_ids_address_the_same_mailbox() {
        let legacy: MakiId = "01965087-4c71-7f00-8000-000000000001".parse().unwrap();
        let canonical: MakiId = legacy.to_string().parse().unwrap();
        let mailbox = SessionMailbox::register(legacy);
        SessionMailbox::notify(canonical, "built".into(), false).unwrap();

        assert_eq!(mailbox.drain().len(), 1);
    }
}
