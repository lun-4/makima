use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use maki_providers::{ContentBlock, EMPTY_RESPONSE_MARKER, Message, Role};
use maki_storage::sessions::next_epoch;
use serde_json::Value;
use thiserror::Error;
use tracing::warn;

use crate::cancel::CancelToken;

const CANCEL_MARKER: &str = "[Cancelled by user]";
pub const UNAVAILABLE_RESULT: &str = "[Tool result not available]";

pub type HistorySnapshot = maki_storage::sessions::HistorySnapshot<Message>;
pub type SharedMessages = Arc<ArcSwap<HistorySnapshot>>;

type ChangeListener = Arc<dyn Fn(Option<String>) + Send + Sync>;

struct HistoryOwner {
    snapshot: HistorySnapshot,
    generation: u64,
    mirror: Option<SharedMessages>,
    listener: Option<ChangeListener>,
}

impl HistoryOwner {
    fn publish(&self) {
        if let Some(mirror) = &self.mirror {
            mirror.store(Arc::new(self.snapshot.clone()));
        }
    }

    fn invalidate_handles(&mut self) {
        self.generation = next_epoch();
    }
}

/// Clones share one synchronized history. Snapshots remain frozen after later commits.
#[derive(Clone)]
pub struct History {
    owner: Arc<Mutex<HistoryOwner>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HistoryStateError {
    #[error("history state handle belongs to a replaced history")]
    Stale,
    #[error("history state operation was cancelled")]
    Cancelled,
    #[error("history state boundary is outside the current history")]
    InvalidBoundary,
}

/// Commits namespace values at one tool-call boundary in owner-lock acquisition order.
/// Replacement invalidates the handle, including replacement with an earlier snapshot.
#[derive(Clone)]
pub struct HistoryStateHandle {
    history: History,
    generation: u64,
    boundary: usize,
    cancel: CancelToken,
}

impl HistoryStateHandle {
    fn validate(&self, owner: &HistoryOwner) -> Result<(), HistoryStateError> {
        if owner.generation != self.generation {
            return Err(HistoryStateError::Stale);
        }
        if self.boundary > owner.snapshot.messages.len() {
            return Err(HistoryStateError::InvalidBoundary);
        }
        Ok(())
    }

    pub fn get(&self, namespace: &str) -> Result<Option<Value>, HistoryStateError> {
        let owner = self
            .history
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.validate(&owner)?;
        self.cancel
            .commit_if_active(|| owner.snapshot.state.get(namespace).cloned())
            .ok_or(HistoryStateError::Cancelled)
    }

    pub fn set(&self, namespace: String, value: Value) -> Result<(), HistoryStateError> {
        self.commit(namespace, Some(value))
    }

    pub fn delete(&self, namespace: String) -> Result<(), HistoryStateError> {
        self.commit(namespace, None)
    }

    fn commit(&self, namespace: String, value: Option<Value>) -> Result<(), HistoryStateError> {
        let listener = {
            let mut owner = self
                .history
                .owner
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.validate(&owner)?;
            self.cancel
                .commit_if_active(|| {
                    let state = Arc::make_mut(&mut owner.snapshot.state);
                    match value {
                        Some(value) => state.set(self.boundary, namespace.clone(), value),
                        None => state.delete(self.boundary, namespace.clone()),
                    }
                    owner.publish();
                })
                .ok_or(HistoryStateError::Cancelled)?;
            owner.listener.clone()
        };
        if let Some(listener) = listener {
            listener(Some(namespace));
        }
        Ok(())
    }
}

impl History {
    pub fn new(messages: Vec<Message>) -> Self {
        Self::from_snapshot(HistorySnapshot::new(messages))
    }

    fn from_snapshot(snapshot: HistorySnapshot) -> Self {
        Self {
            owner: Arc::new(Mutex::new(HistoryOwner {
                snapshot,
                generation: next_epoch(),
                mirror: None,
                listener: None,
            })),
        }
    }

    pub fn restored_snapshot(mut snapshot: HistorySnapshot) -> Self {
        sanitize_restored(&mut snapshot);
        snapshot.epoch = next_epoch();
        Self::from_snapshot(snapshot)
    }

    pub fn restored(messages: Vec<Message>) -> Self {
        Self::restored_snapshot(HistorySnapshot::new(messages))
    }

    pub fn with_mirror(self, mirror: SharedMessages) -> Self {
        {
            let mut owner = self.owner.lock().unwrap_or_else(|error| error.into_inner());
            owner.mirror = Some(mirror);
            owner.publish();
        }
        self
    }

    /// Runs after publication and outside the owner lock. `None` denotes replacement.
    /// The callback must not retain a strong reference to this history's owner.
    pub fn set_change_listener(&self, listener: impl Fn(Option<String>) + Send + Sync + 'static) {
        self.owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .listener = Some(Arc::new(listener));
    }

    pub fn state_handle(&self, boundary: usize, cancel: CancelToken) -> HistoryStateHandle {
        let owner = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        HistoryStateHandle {
            history: self.clone(),
            generation: owner.generation,
            boundary,
            cancel,
        }
    }

    pub fn snapshot(&self) -> HistorySnapshot {
        self.owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot
            .clone()
    }

    pub fn as_slice(&self) -> Arc<Vec<Message>> {
        self.snapshot().messages
    }

    pub fn push(&mut self, msg: Message) {
        self.edit(|msgs| msgs.push(msg));
    }

    pub(crate) fn push_tool_call(
        &mut self,
        message: Message,
        cancel: CancelToken,
    ) -> HistoryStateHandle {
        let mut owner = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        Arc::make_mut(&mut owner.snapshot.messages).push(message);
        owner.publish();
        HistoryStateHandle {
            history: self.clone(),
            generation: owner.generation,
            boundary: owner.snapshot.messages.len(),
            cancel,
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Skips system padding so repeated nudges cannot push tool results
    /// out of the window.
    pub fn has_recent_tool_results(&self, depth: usize) -> bool {
        self.as_slice()
            .iter()
            .rev()
            .filter(|m| !is_system_padding(m))
            .take(depth)
            .any(|m| {
                m.content
                    .iter()
                    .any(|b| matches!(b, ContentBlock::ToolResult { .. }))
            })
    }

    /// Reads the padding tail instead of keeping a counter, so any real
    /// message resets the nudge budget on its own.
    pub fn recent_nudges(&self) -> u32 {
        self.as_slice()
            .iter()
            .rev()
            .take_while(|m| is_system_padding(m))
            .filter(|m| is_empty_marker(m))
            .count() as u32
    }

    pub fn replace(&mut self, messages: Vec<Message>) {
        self.replace_snapshot(HistorySnapshot::new(messages));
    }

    pub fn replace_snapshot(&mut self, snapshot: HistorySnapshot) {
        self.replace_with(|current| *current = snapshot);
    }

    pub fn replace_compacted(&mut self, messages: Vec<Message>) {
        self.replace_with(|snapshot| {
            Arc::make_mut(&mut snapshot.state).compact();
            snapshot.compacted = true;
            snapshot.epoch = next_epoch();
            snapshot.messages = Arc::new(messages);
        });
    }

    pub fn truncate(&mut self, len: usize) {
        self.replace_with(|snapshot| {
            Arc::make_mut(&mut snapshot.state).truncate(len);
            Arc::make_mut(&mut snapshot.messages).truncate(len);
            snapshot.epoch = next_epoch();
        });
    }

    pub fn into_vec(self) -> Vec<Message> {
        Arc::unwrap_or_clone(self.as_slice())
    }

    fn edit(&mut self, f: impl FnOnce(&mut Vec<Message>)) {
        let mut owner = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        f(Arc::make_mut(&mut owner.snapshot.messages));
        owner.publish();
    }

    fn replace_with(&mut self, f: impl FnOnce(&mut HistorySnapshot)) {
        let listener = {
            let mut owner = self.owner.lock().unwrap_or_else(|error| error.into_inner());
            owner.invalidate_handles();
            f(&mut owner.snapshot);
            owner.publish();
            owner.listener.clone()
        };
        if let Some(listener) = listener {
            listener(None);
        }
    }
}

pub(super) fn remove_orphaned_tool_results(messages: &mut Vec<Message>) -> bool {
    retain_tool_results(messages, |_| {})
}

fn retain_tool_results(messages: &mut Vec<Message>, mut retained: impl FnMut(bool)) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < messages.len() {
        if !matches!(messages[i].role, Role::User) {
            retained(true);
            i += 1;
            continue;
        }

        let valid_ids: Vec<String> = if i > 0 && matches!(messages[i - 1].role, Role::Assistant) {
            messages[i - 1]
                .tool_uses()
                .map(|(id, _, _)| id.to_owned())
                .collect()
        } else {
            Vec::new()
        };

        let content_len = messages[i].content.len();
        let (mut had_results, mut kept_results) = (false, false);
        messages[i].content.retain(|b| match b {
            ContentBlock::ToolResult { tool_use_id, .. } => {
                had_results = true;
                let keep = valid_ids.iter().any(|id| id == tool_use_id);
                kept_results |= keep;
                keep
            }
            _ => true,
        });
        if had_results && !kept_results {
            messages[i]
                .content
                .retain(|b| !matches!(b, ContentBlock::Image { .. }));
        }
        changed |= messages[i].content.len() != content_len;

        let keep = !messages[i].content.is_empty();
        retained(keep);
        if keep {
            i += 1;
        } else {
            messages.remove(i);
            changed = true;
        }
    }

    changed
}

/// Empty markers and synthetic prompts (empty `display_text`) are
/// bookkeeping, not conversation.
fn is_system_padding(m: &Message) -> bool {
    is_empty_marker(m)
        || (m.display_text.as_deref() == Some("")
            && m.content
                .iter()
                .all(|b| matches!(b, ContentBlock::Text { .. })))
}

/// Role and `display_text` are part of the shape: a user who types the marker
/// text verbatim writes a real message, and it has to break the nudge streak
/// like any other.
fn is_empty_marker(m: &Message) -> bool {
    matches!(m.role, Role::Assistant)
        && m.display_text.is_none()
        && matches!(&m.content[..], [ContentBlock::Text { text }] if text == EMPTY_RESPONSE_MARKER)
}

/// Restored sessions can have orphaned tool_results or unclosed tool_uses
/// (e.g. the process was killed mid-turn). The API returns 400 if it sees those.
fn sanitize_restored(snapshot: &mut HistorySnapshot) {
    let messages = Arc::make_mut(&mut snapshot.messages);
    let len_before = messages.len();
    let mut retained_prefix_counts = Vec::with_capacity(len_before + 1);
    retained_prefix_counts.push(0);
    let mut retained_count = 0;
    let mut changed = retain_tool_results(messages, |keep| {
        retained_count += usize::from(keep);
        retained_prefix_counts.push(retained_count);
    });
    Arc::make_mut(&mut snapshot.state).remap_boundaries(&retained_prefix_counts);
    close_dangling_tool_calls(messages, UNAVAILABLE_RESULT);
    changed |= messages.len() != len_before;

    if changed {
        warn!(
            before = len_before,
            after = messages.len(),
            "sanitized restored history"
        );
    }
}

pub fn close_dangling_tool_calls(messages: &mut Vec<Message>, note: &str) {
    let Some(last) = messages.last() else { return };
    if !matches!(last.role, Role::Assistant) || !last.has_tool_calls() {
        return;
    }
    let error_results: Vec<ContentBlock> = last
        .tool_uses()
        .map(|(id, _, _)| ContentBlock::ToolResult {
            tool_use_id: id.to_owned(),
            content: note.to_owned(),
            is_error: true,
        })
        .collect();
    messages.push(Message {
        role: Role::User,
        content: error_results,
        display_text: Some(String::new()),
        ..Default::default()
    });
}

pub(crate) fn sanitize_cancelled_history(history: &mut History, rollback_len: usize) {
    if history.len() <= rollback_len {
        return;
    }
    history.edit(|msgs| {
        close_dangling_tool_calls(msgs, CANCEL_MARKER);
        msgs.push(Message::synthetic(CANCEL_MARKER.into()));
    });
}

#[cfg(test)]
mod tests {
    use maki_providers::{ContentBlock, Message, Role};
    use test_case::test_case;

    use super::*;

    const FIRST: &str = "first";
    const SECOND: &str = "second";
    const GO: &str = "go";
    const STATE_NAMESPACE: &str = "test.work";
    const OTHER_NAMESPACE: &str = "test.other";

    #[test]
    fn history_state_concurrent_ordering_and_result_append() {
        let mirror = make_mirror();
        let mut history = History::new(vec![Message::user(GO.into()), make_tool_use_msg(&["t1"])])
            .with_mirror(Arc::clone(&mirror));
        let before = history.snapshot();
        let first = history.state_handle(history.len(), CancelToken::none());
        let second = first.clone();
        let (committed_tx, committed_rx) = flume::bounded(1);
        let (continue_tx, continue_rx) = flume::bounded(1);
        let writer = std::thread::spawn(move || {
            first
                .set(STATE_NAMESPACE.into(), Value::String(FIRST.into()))
                .unwrap();
            committed_tx.send(()).unwrap();
            continue_rx.recv().unwrap();
            second
                .set(STATE_NAMESPACE.into(), Value::String(SECOND.into()))
                .unwrap();
        });
        committed_rx.recv().unwrap();
        let first_commit = mirror.load_full();
        assert_eq!(first_commit.messages.len(), 2);
        assert_eq!(
            first_commit.state.get(STATE_NAMESPACE),
            Some(&Value::String(FIRST.into()))
        );
        history.push(make_tool_result_msg(&["t1"]));
        continue_tx.send(()).unwrap();
        writer.join().unwrap();
        let after = history.snapshot();
        assert_eq!(after.messages.len(), 3);
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(
            after.state.get(STATE_NAMESPACE),
            Some(&Value::String(SECOND.into()))
        );
        assert_eq!(
            after
                .state
                .updates
                .iter()
                .map(|update| update.boundary)
                .collect::<Vec<_>>(),
            [2, 2]
        );
        assert!(before.state.is_empty());
        assert_eq!(
            first_commit.state.get(STATE_NAMESPACE),
            Some(&Value::String(FIRST.into()))
        );
        let published = mirror.load_full();
        assert!(Arc::ptr_eq(&after.state, &published.state));
        assert!(Arc::ptr_eq(&after.messages, &published.messages));
        history.truncate(1);
        assert_eq!(history.snapshot().state.get(STATE_NAMESPACE), None);
    }

    #[test_case("rewind" ; "rewind")]
    #[test_case("compact" ; "compaction")]
    #[test_case("reset" ; "reset")]
    #[test_case("rollback" ; "rollback")]
    fn history_state_stale_handle_rejected(replacement: &str) {
        let mut history = History::new(vec![Message::user(GO.into())]);
        let handle = history.state_handle(history.len(), CancelToken::none());
        handle
            .set(STATE_NAMESPACE.into(), Value::String(FIRST.into()))
            .unwrap();
        let original = history.snapshot();
        match replacement {
            "rewind" => history.truncate(0),
            "compact" => history.replace_compacted(vec![Message::user(SECOND.into())]),
            "reset" => history.replace(Vec::new()),
            "rollback" => {
                history.replace_compacted(vec![Message::user(SECOND.into())]);
                history.replace_snapshot(original.clone());
            }
            _ => unreachable!(),
        }
        let before = history.snapshot();
        assert_eq!(handle.get(STATE_NAMESPACE), Err(HistoryStateError::Stale));
        assert_eq!(
            handle.set(STATE_NAMESPACE.into(), Value::Null),
            Err(HistoryStateError::Stale)
        );
        assert_eq!(
            handle.delete(STATE_NAMESPACE.into()),
            Err(HistoryStateError::Stale)
        );
        assert!(Arc::ptr_eq(&before.state, &history.snapshot().state));
        let fresh = history.state_handle(history.len(), CancelToken::none());
        fresh
            .set(STATE_NAMESPACE.into(), Value::String(SECOND.into()))
            .unwrap();
        assert_eq!(
            fresh.get(STATE_NAMESPACE).unwrap(),
            Some(Value::String(SECOND.into()))
        );
    }

    #[test]
    fn history_state_cancelled_before_lock_does_not_commit() {
        let history = History::new(vec![Message::user(GO.into())]);
        let (trigger, cancel) = CancelToken::new();
        let handle = history.state_handle(history.len(), cancel);
        handle
            .set(STATE_NAMESPACE.into(), Value::String(FIRST.into()))
            .unwrap();
        let owner = history.owner.lock().unwrap();
        let (started_tx, started_rx) = flume::bounded(1);
        let writer = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            handle.set(STATE_NAMESPACE.into(), Value::String(SECOND.into()))
        });
        started_rx.recv().unwrap();
        trigger.cancel();
        drop(owner);
        assert_eq!(writer.join().unwrap(), Err(HistoryStateError::Cancelled));
        assert_eq!(
            history.snapshot().state.get(STATE_NAMESPACE),
            Some(&Value::String(FIRST.into()))
        );
        assert_eq!(history.snapshot().state.updates.len(), 1);
    }

    #[test]
    fn history_state_listener_queries_published_owner_and_isolation() {
        let mirror = make_mirror();
        let mut history =
            History::new(vec![Message::user(GO.into())]).with_mirror(Arc::clone(&mirror));
        let child = History::new(vec![Message::user(GO.into())]);
        let (events_tx, events_rx) = flume::unbounded();
        let observed = Arc::downgrade(&history.owner);
        history.set_change_listener(move |namespace| {
            let history = History {
                owner: observed.upgrade().unwrap(),
            };
            let snapshot = history.snapshot();
            let published = mirror.load_full();
            assert!(Arc::ptr_eq(&snapshot.state, &published.state));
            assert!(Arc::ptr_eq(&snapshot.messages, &published.messages));
            events_tx.send(namespace).unwrap();
        });
        let handle = history.state_handle(history.len(), CancelToken::none());
        handle.set(STATE_NAMESPACE.into(), Value::Null).unwrap();
        assert_eq!(handle.get(STATE_NAMESPACE).unwrap(), Some(Value::Null));
        assert_eq!(child.snapshot().state.get(STATE_NAMESPACE), None);
        handle.delete(STATE_NAMESPACE.into()).unwrap();
        assert_eq!(handle.get(STATE_NAMESPACE).unwrap(), None);
        history.replace(Vec::new());
        assert_eq!(
            events_rx.try_iter().collect::<Vec<_>>(),
            [
                Some(STATE_NAMESPACE.into()),
                Some(STATE_NAMESPACE.into()),
                None
            ]
        );
    }

    #[test]
    fn history_state_invalid_boundary_does_not_mutate() {
        let history = History::new(Vec::new());
        let handle = history.state_handle(1, CancelToken::none());
        assert_eq!(
            handle.set(STATE_NAMESPACE.into(), Value::Null),
            Err(HistoryStateError::InvalidBoundary)
        );
        assert!(history.snapshot().state.is_empty());
    }

    #[test]
    fn history_state_sanitized_interrupted_resume_and_rewind() {
        let mut snapshot = HistorySnapshot::new(vec![
            make_tool_result_msg(&["orphan"]),
            Message::user(GO.into()),
            make_tool_use_msg(&["t1"]),
            make_tool_result_msg(&["wrong"]),
        ]);
        let state = Arc::make_mut(&mut snapshot.state);
        state.set(0, STATE_NAMESPACE.into(), serde_json::json!(FIRST));
        state.set(1, STATE_NAMESPACE.into(), serde_json::json!(SECOND));
        state.delete(2, STATE_NAMESPACE.into());
        state.set(3, OTHER_NAMESPACE.into(), serde_json::json!(GO));
        state.set(4, STATE_NAMESPACE.into(), serde_json::Value::Null);
        let original = snapshot.clone();
        let mut history = History::restored_snapshot(snapshot);
        assert_eq!(history.len(), 3);
        assert_eq!(extract_error_ids(&history.as_slice()[2]), ["t1"]);
        assert_eq!(
            history.snapshot().state.get(STATE_NAMESPACE),
            Some(&serde_json::Value::Null)
        );
        assert_eq!(
            history
                .snapshot()
                .state
                .updates
                .iter()
                .map(|update| update.boundary)
                .collect::<Vec<_>>(),
            [0, 0, 1, 2, 2]
        );
        history.truncate(2);
        assert_eq!(
            history.snapshot().state.get(STATE_NAMESPACE),
            Some(&serde_json::Value::Null)
        );
        history.truncate(1);
        assert_eq!(history.snapshot().state.get(STATE_NAMESPACE), None);
        assert_eq!(history.snapshot().state.get(OTHER_NAMESPACE), None);
        history.truncate(0);
        assert_eq!(
            history.snapshot().state.get(STATE_NAMESPACE),
            Some(&serde_json::json!(SECOND))
        );
        assert_eq!(original.state.updates.last().unwrap().boundary, 4);
        assert_eq!(original.messages.len(), 4);
    }

    #[test]
    fn history_state_compaction_baseline_and_reset() {
        let mut snapshot = HistorySnapshot::new(vec![Message::user(FIRST.into())]);
        let state = Arc::make_mut(&mut snapshot.state);
        state.set(
            1,
            STATE_NAMESPACE.into(),
            serde_json::json!({"work": [FIRST, SECOND]}),
        );
        state.set(1, OTHER_NAMESPACE.into(), serde_json::Value::Null);
        state.delete(1, OTHER_NAMESPACE.into());
        let mirror = make_mirror();
        let mut history = History::restored_snapshot(snapshot).with_mirror(Arc::clone(&mirror));
        let before = mirror.load_full();
        history.replace_compacted(vec![Message::observation(SECOND.into())]);
        let compacted = mirror.load_full();
        assert_eq!(
            compacted.state.get(STATE_NAMESPACE),
            before.state.get(STATE_NAMESPACE)
        );
        assert_eq!(compacted.state.get(OTHER_NAMESPACE), None);
        assert!(compacted.state.updates.is_empty());
        assert!(!before.state.updates.is_empty());
        assert_ne!(compacted.epoch, before.epoch);
        history.truncate(0);
        assert_eq!(
            history.snapshot().state.get(STATE_NAMESPACE),
            before.state.get(STATE_NAMESPACE)
        );
        history.replace(Vec::new());
        assert_eq!(history.snapshot().state.get(STATE_NAMESPACE), None);
        assert!(!history.snapshot().compacted);
        assert_eq!(
            compacted.state.get(STATE_NAMESPACE),
            before.state.get(STATE_NAMESPACE)
        );
    }

    #[test]
    fn compaction_metadata_survives_append_and_snapshot_restoration() {
        let mirror = Arc::new(ArcSwap::from_pointee(HistorySnapshot::default()));
        let mut history =
            History::new(vec![Message::user(FIRST.into())]).with_mirror(Arc::clone(&mirror));
        history.truncate(0);
        assert!(!mirror.load().compacted);
        history.replace_compacted(vec![Message::observation(SECOND.into())]);
        history.push(Message::user(GO.into()));
        assert!(mirror.load().compacted);
        let restored = History::restored_snapshot((*mirror.load_full()).clone());
        assert!(restored.snapshot().compacted);
        assert_eq!(restored.len(), 2);
    }

    #[track_caller]
    fn assert_ends_with_cancel_marker(history: &History) {
        let messages = history.as_slice();
        let last = messages.last().unwrap();
        assert!(matches!(last.role, Role::User));
        assert!(matches!(&last.content[0], ContentBlock::Text { text } if text == CANCEL_MARKER));
    }

    fn make_tool_use_msg(ids: &[&str]) -> Message {
        Message {
            role: Role::Assistant,
            content: ids
                .iter()
                .map(|id| ContentBlock::tool_use(*id, "read", serde_json::json!({})))
                .collect(),
            ..Default::default()
        }
    }

    fn make_tool_result_msg(ids: &[&str]) -> Message {
        Message {
            role: Role::User,
            content: ids
                .iter()
                .map(|id| ContentBlock::ToolResult {
                    tool_use_id: id.to_string(),
                    content: "ok".into(),
                    is_error: false,
                })
                .collect(),
            display_text: Some(String::new()),
            ..Default::default()
        }
    }

    fn make_mirror() -> SharedMessages {
        Arc::new(ArcSwap::from_pointee(HistorySnapshot::default()))
    }

    #[track_caller]
    fn extract_error_ids(msg: &Message) -> Vec<&str> {
        msg.content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolResult {
                    tool_use_id,
                    is_error: true,
                    ..
                } => Some(tool_use_id.as_str()),
                _ => None,
            })
            .collect()
    }

    #[test_case(
        vec![Message::user("old".into())],
        1,
        1,
        false
        ; "no_new_messages_is_noop"
    )]
    #[test_case(
        vec![Message::user("hello".into())],
        0,
        2,
        true
        ; "user_only_appends_marker"
    )]
    #[test_case(
        vec![
            Message::user("hello".into()),
            Message { role: Role::Assistant, content: vec![ContentBlock::Text { text: "hi".into() }], ..Default::default() },
        ],
        0,
        3,
        true
        ; "complete_turn_appends_marker"
    )]
    fn sanitize_cancelled_history_cases(
        messages: Vec<Message>,
        rollback_len: usize,
        expected_len: usize,
        expect_cancel_marker: bool,
    ) {
        let mut history = History::new(messages);
        sanitize_cancelled_history(&mut history, rollback_len);
        assert_eq!(history.len(), expected_len);
        if expect_cancel_marker {
            assert_ends_with_cancel_marker(&history);
        }
    }

    #[test]
    fn sanitize_dangling_tool_use_adds_error_results() {
        let mut history = History::new(vec![
            Message::user("hello".into()),
            make_tool_use_msg(&["t1", "t2"]),
        ]);
        sanitize_cancelled_history(&mut history, 0);

        assert_eq!(extract_error_ids(&history.as_slice()[2]), ["t1", "t2"]);
        assert_ends_with_cancel_marker(&history);
    }

    #[test]
    fn mirror_is_verbatim_and_epoch_tracks_appends() {
        let mirror = make_mirror();
        let mut history = History::new(Vec::new()).with_mirror(Arc::clone(&mirror));
        let append_epoch = mirror.load().epoch;

        for i in 0..10 {
            history.push(Message::user(format!("msg-{i}")));
            assert_eq!(mirror.load().messages.len(), i + 1);
            assert_eq!(mirror.load().epoch, append_epoch, "push is an append");
        }

        history.truncate(3);
        assert_eq!(mirror.load().messages.len(), 3);
        assert_ne!(
            mirror.load().epoch,
            append_epoch,
            "truncate is not an append"
        );

        history.push(make_tool_use_msg(&["t_final"]));
        assert_eq!(history.len(), 4);
        assert_eq!(
            mirror.load().messages.len(),
            4,
            "dangling tool_use is mirrored verbatim"
        );
    }

    #[test]
    fn close_dangling_tool_uses_appends_error_results() {
        let mut messages = vec![Message::user("go".into()), make_tool_use_msg(&["t1", "t2"])];
        close_dangling_tool_calls(&mut messages, UNAVAILABLE_RESULT);

        assert_eq!(messages.len(), 3);
        let closing = &messages[2];
        assert!(matches!(closing.role, Role::User));
        assert_eq!(extract_error_ids(closing), ["t1", "t2"]);
        assert_eq!(closing.display_text.as_deref(), Some(""));
    }

    #[test]
    fn close_dangling_is_noop_when_tool_result_already_present() {
        let mut messages = vec![
            Message::user("go".into()),
            make_tool_use_msg(&["t1"]),
            make_tool_result_msg(&["t1"]),
        ];
        close_dangling_tool_calls(&mut messages, UNAVAILABLE_RESULT);
        assert_eq!(messages.len(), 3, "no extra closing after real result");
    }

    #[test]
    fn into_vec_returns_inner_messages() {
        let mirror = make_mirror();
        let history = History::new(vec![Message::user("go".into()), make_tool_use_msg(&["t1"])])
            .with_mirror(Arc::clone(&mirror));

        assert_eq!(mirror.load().messages.len(), 2);
        assert_eq!(history.into_vec().len(), 2);
    }

    /// Cancelling closes the open tool calls and marks the turn, all onto the
    /// end of the list, so the log can keep appending instead of rewriting.
    #[test]
    fn sanitize_cancelled_history_appends_onto_the_mirror() {
        let mirror = make_mirror();
        let mut history = History::new(vec![Message::user(GO.into()), make_tool_use_msg(&["t1"])])
            .with_mirror(Arc::clone(&mirror));
        let epoch = mirror.load().epoch;

        sanitize_cancelled_history(&mut history, 0);

        let snap = mirror.load();
        assert_eq!(snap.epoch, epoch, "cancel cleanup is a pure append");
        assert_eq!(snap.messages.len(), history.len(), "mirror is verbatim");
        assert_eq!(extract_error_ids(&snap.messages[2]), ["t1"]);
        assert!(snap.messages[2].content.iter().any(|b| matches!(
            b,
            ContentBlock::ToolResult { content, .. } if content == CANCEL_MARKER
        )));
        assert!(matches!(
            &snap.messages[3].content[0],
            ContentBlock::Text { text } if text == CANCEL_MARKER
        ));
    }

    fn text_msg(role: Role, text: &str) -> Message {
        Message {
            role,
            content: vec![ContentBlock::Text { text: text.into() }],
            ..Default::default()
        }
    }

    #[test_case(
        vec![make_tool_result_msg(&["t1"])],
        0
        ; "orphan_at_start_removed"
    )]
    #[test_case(
        vec![
            Message::user("go".into()),
            text_msg(Role::Assistant, "done"),
            make_tool_result_msg(&["orphan1", "orphan2"]),
        ],
        2
        ; "orphans_after_non_tool_assistant_removed"
    )]
    #[test_case(
        vec![
            Message::user("go".into()),
            make_tool_use_msg(&["t1", "t2"]),
            make_tool_result_msg(&["t1", "t2"]),
        ],
        3
        ; "valid_pairing_preserved"
    )]
    #[test_case(
        vec![Message::user("go".into()), make_tool_use_msg(&["t1"])],
        3
        ; "dangling_tool_use_closed_with_synthetic_result"
    )]
    fn sanitize_restored_cases(messages: Vec<Message>, expected_len: usize) {
        let history = History::restored(messages);
        assert_eq!(history.len(), expected_len);
    }

    #[test]
    fn sanitize_restored_drops_image_when_all_results_orphaned() {
        let image_block = ContentBlock::Image {
            source: maki_providers::ImageSource::new(
                maki_providers::ImageMediaType::Png,
                std::sync::Arc::from("aGVsbG8="),
            ),
        };
        let mut orphaned = make_tool_result_msg(&["orphan"]);
        orphaned.content.push(image_block.clone());
        let history = History::restored(vec![Message::user("go".into()), orphaned]);
        assert_eq!(history.len(), 1);

        // Chat-pasted image (no tool results) is untouched.
        let history = History::restored(vec![Message {
            role: Role::User,
            content: vec![image_block],
            ..Default::default()
        }]);
        assert_eq!(history.len(), 1);
        assert!(matches!(
            history.as_slice()[0].content[0],
            ContentBlock::Image { .. }
        ));
    }

    #[test]
    fn sanitize_restored_keeps_image_when_any_result_survives() {
        let mut msg = make_tool_result_msg(&["t1", "orphan"]);
        msg.content.push(ContentBlock::Image {
            source: maki_providers::ImageSource::new(
                maki_providers::ImageMediaType::Png,
                std::sync::Arc::from("aGVsbG8="),
            ),
        });
        let history = History::restored(vec![
            Message::user("go".into()),
            make_tool_use_msg(&["t1"]),
            msg,
        ]);
        let content = &history.as_slice()[2].content;
        assert_eq!(content.len(), 2);
        assert!(matches!(
            &content[0],
            ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == "t1"
        ));
        assert!(matches!(content[1], ContentBlock::Image { .. }));
    }

    #[test]
    fn remove_orphaned_tool_results_reports_content_change() {
        let mut result = make_tool_result_msg(&["orphan"]);
        result.content.push(ContentBlock::Text {
            text: "keep me".into(),
        });
        let mut messages = vec![result];

        assert!(remove_orphaned_tool_results(&mut messages));
        assert_eq!(messages.len(), 1);
        assert!(matches!(
            &messages[0].content[..],
            [ContentBlock::Text { text }] if text == "keep me"
        ));
        assert!(!remove_orphaned_tool_results(&mut messages));
    }

    #[test]
    fn sanitize_restored_partial_orphan_keeps_matched_ids() {
        let history = History::restored(vec![
            Message::user("go".into()),
            make_tool_use_msg(&["t1"]),
            make_tool_result_msg(&["t1", "t2"]),
        ]);
        let messages = history.as_slice();
        let results: Vec<&str> = messages[2]
            .content
            .iter()
            .filter_map(|b| match b {
                ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(results, ["t1"]);
    }

    #[test_case(
        vec![Message::user("go".into())],
        0
        ; "no_tool_results"
    )]
    #[test_case(
        vec![
            Message::user("go".into()),
            make_tool_result_msg(&["t1"]),
        ],
        1
        ; "recent_tool_result"
    )]
    #[test_case(
        vec![
            Message::user("old1".into()),
            Message::user("old2".into()),
            Message::user("old3".into()),
            Message::user("old4".into()),
            Message::user("old5".into()),
            make_tool_result_msg(&["t1"]),
        ],
        1
        ; "at_depth_boundary"
    )]
    #[test_case(
        vec![
            make_tool_result_msg(&["t1"]),
            Message::empty_marker(),
            Message::synthetic("nudge".into()),
            Message::empty_marker(),
            Message::synthetic("nudge".into()),
            Message::empty_marker(),
            Message::synthetic("continue".into()),
        ],
        1
        ; "padding_does_not_hide_tool_results"
    )]
    fn has_recent_tool_results(messages: Vec<Message>, depth: usize) {
        let history = History::new(messages);
        let result = if depth == 0 {
            history.has_recent_tool_results(0)
        } else {
            history.has_recent_tool_results(depth)
        };
        assert_eq!(result, depth > 0);
    }

    #[test_case(vec![], 0 ; "empty_history")]
    #[test_case(
        vec![
            make_tool_result_msg(&["t1"]),
            Message::empty_marker(),
            Message::synthetic("nudge".into()),
            Message::empty_marker(),
            Message::synthetic("nudge".into()),
            Message::empty_marker(),
        ],
        3
        ; "counts_markers_in_padding_tail"
    )]
    #[test_case(
        vec![
            Message::empty_marker(),
            Message::synthetic("nudge".into()),
            Message::user("continue".into()),
        ],
        0
        ; "user_message_resets_streak"
    )]
    #[test_case(
        vec![
            Message::empty_marker(),
            Message::synthetic("nudge".into()),
            Message::user(EMPTY_RESPONSE_MARKER.into()),
        ],
        0
        ; "user_typing_the_marker_text_resets_streak"
    )]
    fn recent_nudges(messages: Vec<Message>, expected: u32) {
        assert_eq!(History::new(messages).recent_nudges(), expected);
    }

    /// The writer thread serializes a snapshot while the user keeps typing, so
    /// a published snapshot must never move under it.
    #[test]
    fn published_snapshot_is_frozen_against_later_mutations() {
        let mirror = make_mirror();
        let mut history =
            History::new(vec![Message::user(FIRST.into())]).with_mirror(Arc::clone(&mirror));

        let after_new = mirror.load_full();
        history.push(Message::user(SECOND.into()));
        assert_eq!(after_new.messages.len(), 1);
        assert_eq!(after_new.messages[0].user_text(), Some(FIRST));
        assert!(!Arc::ptr_eq(&after_new.messages, &mirror.load().messages));

        let after_push = mirror.load_full();
        history.truncate(1);
        assert_eq!(after_push.messages.len(), 2);
        assert_eq!(after_push.messages[1].user_text(), Some(SECOND));
        assert!(!Arc::ptr_eq(&after_push.messages, &mirror.load().messages));
    }

    /// When a new run binds the mirror, the old run's messages must be gone
    /// right away, not only once the new run pushes something.
    #[test]
    fn with_mirror_overwrites_previous_run_snapshot_immediately() {
        let mirror = make_mirror();
        let run1 = History::new(vec![Message::user(FIRST.into())]).with_mirror(Arc::clone(&mirror));
        let run1_epoch = mirror.load().epoch;
        drop(run1);

        let _run2 = History::new(vec![Message::user(SECOND.into()), Message::user(GO.into())])
            .with_mirror(Arc::clone(&mirror));

        let snap = mirror.load();
        assert_eq!(snap.messages.len(), 2);
        assert_eq!(snap.messages[0].user_text(), Some(SECOND));
        assert_ne!(snap.epoch, run1_epoch, "run 2 is not an append onto run 1");
    }

    #[test]
    fn restored_mints_fresh_epoch_and_mirrors_sanitized_messages() {
        let mirror = make_mirror();
        let seed_epoch = mirror.load().epoch;

        let history = History::restored(vec![
            Message::user(GO.into()),
            make_tool_use_msg(&["t1"]),
            make_tool_result_msg(&["orphan"]),
        ])
        .with_mirror(Arc::clone(&mirror));

        let snap = mirror.load();
        assert_ne!(snap.epoch, seed_epoch);
        assert!(
            Arc::ptr_eq(&snap.messages, &history.snapshot().messages),
            "mirror shares the sanitized buffer verbatim"
        );
        assert_eq!(snap.messages.len(), 3);
        assert_eq!(extract_error_ids(&snap.messages[2]), ["t1"]);
    }

    #[test]
    fn sanitize_cancelled_history_noop_publishes_nothing() {
        let mirror = make_mirror();
        let mut history =
            History::new(vec![Message::user(GO.into())]).with_mirror(Arc::clone(&mirror));
        let before = mirror.load_full();
        let rollback_len = history.len();

        sanitize_cancelled_history(&mut history, rollback_len);

        let after = mirror.load_full();
        assert_eq!(before.epoch, after.epoch);
        assert!(Arc::ptr_eq(&before.messages, &after.messages));
    }

    /// Compaction can swap the whole list for one of the same length, which a
    /// length-only check would miss and quietly corrupt the log.
    #[test]
    fn replace_mints_new_epoch_even_when_length_is_unchanged() {
        let mirror = make_mirror();
        let mut history =
            History::new(vec![Message::user(FIRST.into())]).with_mirror(Arc::clone(&mirror));
        let epoch = mirror.load().epoch;

        history.replace(vec![Message::user(SECOND.into())]);

        let snap = mirror.load();
        assert_ne!(snap.epoch, epoch);
        assert_eq!(snap.messages.len(), 1);
        assert_eq!(snap.messages[0].user_text(), Some(SECOND));
    }
}
