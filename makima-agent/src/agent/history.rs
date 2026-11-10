use std::mem;
use std::sync::Arc;

use arc_swap::ArcSwap;
use makima_providers::{
    ContentBlock, ConversationLink, ConversationRelation, EMPTY_RESPONSE_MARKER, Message, Role,
};
use makima_storage::conversation::ConversationState;
use makima_storage::sessions::next_epoch;
use tracing::warn;

const CANCEL_MARKER: &str = "[Cancelled by user]";
pub const UNAVAILABLE_RESULT: &str = "[Tool result not available]";

pub type HistorySnapshot = makima_storage::sessions::HistorySnapshot<Message>;
pub type SharedMessages = Arc<ArcSwap<HistorySnapshot>>;

pub struct History {
    /// The value the mirror publishes, held whole so the two can never
    /// disagree and so a new run can never inherit the last one's epoch.
    snapshot: HistorySnapshot,
    mirror: Option<SharedMessages>,
    /// The authority over `snapshot.conversation`, which only copies it for
    /// persistence: a rollback restores messages, never a conversation that
    /// may already have been ended.
    conversation: ConversationState,
}

impl History {
    pub fn new(messages: Vec<Message>) -> Self {
        let conversation = ConversationState::fresh(None);
        let mut snapshot = HistorySnapshot::new(messages);
        snapshot.conversation = Some(conversation.clone());
        Self {
            snapshot,
            mirror: None,
            conversation,
        }
    }

    pub fn restored_snapshot(snapshot: HistorySnapshot) -> Self {
        let mut history = Self::restored(Arc::unwrap_or_clone(snapshot.messages));
        history.snapshot.compacted = snapshot.compacted;
        if let Some(conversation) = snapshot.conversation {
            history.set_conversation(conversation);
        }
        history
    }

    pub fn restored(mut messages: Vec<Message>) -> Self {
        sanitize_restored(&mut messages);
        Self::new(messages)
    }

    /// A seeded mirror is the stored session, so its conversation id carries on.
    pub fn with_mirror(mut self, mirror: SharedMessages) -> Self {
        let seed = mirror.load_full();
        self.snapshot.compacted |= seed.compacted;
        if let Some(conversation) = &seed.conversation {
            self.set_conversation(conversation.clone());
        }
        self.mirror = Some(mirror);
        self.publish();
        self
    }

    pub fn snapshot(&self) -> &HistorySnapshot {
        &self.snapshot
    }

    pub fn conversation(&self) -> &ConversationState {
        &self.conversation
    }

    pub fn conversation_id(&self) -> &Arc<str> {
        &self.conversation.id
    }

    /// Records that the whole history went out as a request.
    pub fn mark_sent(&mut self) {
        self.conversation.sent = Some(self.len());
        self.snapshot.conversation = Some(self.conversation.clone());
        self.publish();
    }

    /// The link a copy of the first `kept` messages carries.
    pub fn branch(&self, kept: usize) -> Option<ConversationLink> {
        self.conversation.link(ConversationRelation::Branches, kept)
    }

    /// Starts a new conversation that carries on from the current one over
    /// the messages it still holds. Returns the outgoing id when a request
    /// under it was delivered, the only case that leaves remote state to end.
    pub fn continue_conversation(&mut self) -> Option<Arc<str>> {
        let previous = self
            .conversation
            .link(ConversationRelation::Continues, self.len());
        self.rotate(previous)
    }

    /// Starts a new conversation whose history does not come from the
    /// current one. Returns the outgoing id when a request under it was
    /// delivered.
    pub fn restart_conversation(&mut self) -> Option<Arc<str>> {
        self.rotate(None)
    }

    fn rotate(&mut self, previous: Option<ConversationLink>) -> Option<Arc<str>> {
        let ended = self.set_conversation(ConversationState::fresh(previous));
        self.publish();
        ended.sent.map(|_| ended.id)
    }

    fn set_conversation(&mut self, conversation: ConversationState) -> ConversationState {
        self.snapshot.conversation = Some(conversation.clone());
        mem::replace(&mut self.conversation, conversation)
    }

    pub fn as_slice(&self) -> &[Message] {
        &self.snapshot.messages
    }

    pub fn push(&mut self, msg: Message) {
        self.edit(|msgs| msgs.push(msg));
    }

    pub fn len(&self) -> usize {
        self.snapshot.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.snapshot.messages.is_empty()
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
        self.rewrite(|msgs| *msgs = messages);
    }

    pub fn replace_snapshot(&mut self, snapshot: HistorySnapshot) {
        self.snapshot = HistorySnapshot {
            conversation: Some(self.conversation.clone()),
            ..snapshot
        };
        self.publish();
    }

    pub(crate) fn restore_snapshot(&mut self, snapshot: HistorySnapshot) {
        if let Some(conversation) = &snapshot.conversation {
            self.conversation = conversation.clone();
        }
        self.snapshot = snapshot;
        self.snapshot.conversation = Some(self.conversation.clone());
        self.publish();
    }

    /// Returns the conversation id the compacted history leaves behind, when
    /// a request under it was delivered.
    pub fn replace_compacted(&mut self, messages: Vec<Message>) -> Option<Arc<str>> {
        self.snapshot.compacted = true;
        self.replace(messages);
        self.restart_conversation()
    }

    pub fn truncate(&mut self, len: usize) {
        self.rewrite(|msgs| msgs.truncate(len));
    }

    pub fn into_vec(self) -> Vec<Message> {
        Arc::unwrap_or_clone(self.snapshot.messages)
    }

    /// An append: whatever a consumer already holds of the list stays good.
    fn edit(&mut self, f: impl FnOnce(&mut Vec<Message>)) {
        f(Arc::make_mut(&mut self.snapshot.messages));
        self.publish();
    }

    /// Any other change, so a consumer has to start the list over.
    fn rewrite(&mut self, f: impl FnOnce(&mut Vec<Message>)) {
        self.snapshot.epoch = next_epoch();
        self.edit(f);
    }

    /// The mirror gets the messages as they are. Closing dangling tool calls
    /// here used to make the snapshot as long as the real results that came
    /// next, so the log never saw them. Callers that need an API-valid list
    /// close the dangling calls on their own copy.
    fn publish(&self) {
        let Some(mirror) = &self.mirror else { return };
        mirror.store(Arc::new(self.snapshot.clone()));
    }
}

pub(super) fn remove_orphaned_tool_results(messages: &mut Vec<Message>) -> bool {
    let mut changed = false;
    let mut i = 0;
    while i < messages.len() {
        if !matches!(messages[i].role, Role::User) {
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

        if messages[i].content.is_empty() {
            messages.remove(i);
            changed = true;
        } else {
            i += 1;
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
fn sanitize_restored(messages: &mut Vec<Message>) {
    let len_before = messages.len();
    let mut changed = remove_orphaned_tool_results(messages);
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
    use makima_providers::{ContentBlock, Message, Role};
    use test_case::test_case;

    use super::*;

    const FIRST: &str = "first";
    const SECOND: &str = "second";
    const GO: &str = "go";

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
        assert!(restored.snapshot.compacted);
        assert_eq!(restored.len(), 2);
    }

    fn mirror_conversation(mirror: &SharedMessages) -> ConversationState {
        mirror.load().conversation.clone().unwrap()
    }

    #[test]
    fn conversation_resumes_from_mirror_and_never_rolls_back() {
        const STORED: &str = "stored-conversation";
        let mut seed = HistorySnapshot::new(vec![Message::user(FIRST.into())]);
        seed.conversation = Some(ConversationState::new(STORED.into()));
        let mirror = Arc::new(ArcSwap::from_pointee(seed));
        let mut history =
            History::restored(vec![Message::user(FIRST.into())]).with_mirror(Arc::clone(&mirror));
        assert_eq!(history.conversation_id().as_ref(), STORED);
        assert_eq!(mirror_conversation(&mirror).id.as_ref(), STORED);

        let previous = history.snapshot().clone();
        history.mark_sent();
        let sent = history.conversation().clone();
        assert_eq!(mirror_conversation(&mirror), sent);
        let ended = history.continue_conversation().unwrap();
        assert_eq!(ended.as_ref(), STORED);
        let current = history.conversation().clone();
        assert_eq!(mirror_conversation(&mirror), current);

        history.replace_snapshot(previous);
        assert_eq!(history.conversation(), &current);
        assert_eq!(mirror_conversation(&mirror), current);
    }

    #[test]
    fn fresh_history_publishes_its_minted_conversation() {
        let mirror = Arc::new(ArcSwap::from_pointee(HistorySnapshot::default()));
        let history = History::new(Vec::new()).with_mirror(Arc::clone(&mirror));
        assert_eq!(&mirror_conversation(&mirror), history.conversation());
    }

    fn sent_history(sent: usize, total: usize) -> History {
        let messages = (0..total).map(|i| Message::user(i.to_string())).collect();
        let mut history = History::new(messages);
        history.truncate(sent);
        history.mark_sent();
        for i in sent..total {
            history.push(Message::user(i.to_string()));
        }
        history
    }

    #[test_case(3, 5, 5, 3 ; "cancel_keeps_the_sent_request")]
    #[test_case(4, 4, 2, 2 ; "rewind_keeps_its_cut")]
    fn continue_conversation_links_to_the_last_request(
        sent: usize,
        total: usize,
        kept: usize,
        shared: usize,
    ) {
        let mut history = sent_history(sent, total);
        history.truncate(kept);
        let ended = history.continue_conversation().unwrap();
        assert_eq!(
            history.conversation().previous,
            Some(ConversationLink {
                id: ended,
                relation: ConversationRelation::Continues,
                shared,
            })
        );
        assert_eq!(history.conversation().sent, None);
    }

    #[test]
    fn a_conversation_that_never_sent_passes_its_link_on() {
        let mut history = sent_history(3, 3);
        let first = history.continue_conversation().unwrap();
        history.truncate(1);
        assert_eq!(history.continue_conversation(), None);
        assert_eq!(
            history.conversation().previous,
            Some(ConversationLink {
                id: first,
                relation: ConversationRelation::Continues,
                shared: 1,
            })
        );
        assert_eq!(
            history.branch(history.len()).map(|link| link.relation),
            Some(ConversationRelation::Branches)
        );
    }

    #[test]
    fn rotating_an_undelivered_conversation_ends_nothing() {
        let mut history = History::new(vec![Message::user(FIRST.into())]);
        assert_eq!(history.restart_conversation(), None);
        assert_eq!(
            history.replace_compacted(vec![Message::user(SECOND.into())]),
            None
        );
        history.mark_sent();
        let delivered = Arc::clone(history.conversation_id());
        assert_eq!(history.restart_conversation(), Some(delivered));
        history.mark_sent();
        let compacted = Arc::clone(history.conversation_id());
        assert_eq!(
            history.replace_compacted(vec![Message::user(SECOND.into())]),
            Some(compacted)
        );
    }

    #[test]
    fn restart_drops_the_link() {
        let mut history = sent_history(2, 2);
        history.continue_conversation();
        history.restart_conversation();
        assert_eq!(history.conversation().previous, None);
    }

    #[track_caller]
    fn assert_ends_with_cancel_marker(history: &History) {
        let last = history.as_slice().last().unwrap();
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
            source: makima_providers::ImageSource::new(
                makima_providers::ImageMediaType::Png,
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
            source: makima_providers::ImageSource::new(
                makima_providers::ImageMediaType::Png,
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
        let results: Vec<&str> = history.as_slice()[2]
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
            Arc::ptr_eq(&snap.messages, &history.snapshot.messages),
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
