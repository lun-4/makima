use std::collections::HashMap;

use maki_providers::{ContentBlock, Message};

use super::{ActorError, AgentActorHandle};
use crate::TurnId;

const MAX_MESSAGES: usize = 1000;
const MIN_BYTES: usize = 2;
const MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct TranscriptRequest {
    pub through_turn: Option<TurnId>,
    pub last_messages: usize,
    /// Compact JSON byte budget, from 2 (the empty array) through 1 MiB.
    pub max_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct TranscriptSnapshot {
    pub messages: serde_json::Value,
    pub through_turn: Option<TurnId>,
    pub epoch: u64,
    pub total_messages: usize,
    pub omitted_messages: usize,
    /// Omitted indivisible tool-call/response groups, not individual calls.
    pub omitted_tool_groups: usize,
    /// Whether the committed history contains a compaction summary.
    pub compacted: bool,
    pub bytes: usize,
    pub truncated: bool,
}

impl AgentActorHandle {
    pub fn committed_message_count(&self) -> usize {
        self.inner.history.load().messages.len()
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn committed_history(&self) -> std::sync::Arc<Vec<Message>> {
        std::sync::Arc::clone(&self.inner.history.load().messages)
    }

    pub fn transcript(&self, request: TranscriptRequest) -> Result<TranscriptSnapshot, ActorError> {
        self.transcript_inner(
            request,
            #[cfg(test)]
            || {},
        )
    }

    pub(super) fn transcript_inner(
        &self,
        request: TranscriptRequest,
        #[cfg(test)] after_snapshot: impl FnOnce(),
    ) -> Result<TranscriptSnapshot, ActorError> {
        if !(1..=MAX_MESSAGES).contains(&request.last_messages)
            || !(MIN_BYTES..=MAX_BYTES).contains(&request.max_bytes)
        {
            return Err(ActorError::InvalidTranscriptCaps);
        }
        let (history, through_turn, end) = {
            // Settlement takes outcomes before state, then coverage. Keep that order
            // so a boundary and its history are captured in the same observation.
            let _outcomes = self
                .inner
                .outcomes
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            let history = self.inner.history.load_full();
            let through_turn = request
                .through_turn
                .or(state.latest_history_turn.map(|(id, _, _)| id));
            let end = if let Some(turn_id) = through_turn {
                let coverage = self
                    .inner
                    .coverage
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let latest_boundary = state
                    .latest_history_turn
                    .filter(|(id, _, _)| *id == turn_id)
                    .map(|(_, epoch, end)| Some((epoch, end)));
                match coverage.get(&turn_id).or(latest_boundary.as_ref()) {
                    Some(&Some((epoch, end)))
                        if epoch == history.epoch && end <= history.messages.len() =>
                    {
                        end
                    }
                    Some(None) => return Err(ActorError::UnavailableTurnHistory(turn_id)),
                    Some(Some(_)) => return Err(ActorError::CompactedTurn(turn_id)),
                    None if self
                        .inner
                        .tickets
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .contains_key(&turn_id) =>
                    {
                        return Err(ActorError::PendingTurn(turn_id));
                    }
                    None => return Err(ActorError::UnknownTurn(turn_id)),
                }
            } else {
                0
            };
            (history, through_turn, end)
        };
        #[cfg(test)]
        after_snapshot();
        bounded_snapshot(
            &history.messages[..end],
            through_turn,
            history.epoch,
            history.compacted,
            &request,
        )
    }
}

fn bounded_snapshot(
    messages: &[Message],
    through_turn: Option<TurnId>,
    epoch: u64,
    compacted: bool,
    request: &TranscriptRequest,
) -> Result<TranscriptSnapshot, ActorError> {
    let end = messages.len();
    let mut blocked_until = vec![0; end + 1];
    let mut calls = HashMap::new();
    for (index, message) in messages.iter().enumerate() {
        for block in &message.content {
            match block {
                ContentBlock::ToolUse { id, .. } => {
                    calls.insert(id, index);
                }
                ContentBlock::ToolResult { tool_use_id, .. } => {
                    if let Some(&start) = calls.get(tool_use_id) {
                        if start < index {
                            blocked_until[start + 1] = blocked_until[start + 1].max(index + 1);
                        }
                    } else {
                        blocked_until[0] = blocked_until[0].max(index + 1);
                    }
                }
                _ => {}
            }
        }
    }
    let mut blocked_end = 0;
    let safe = blocked_until
        .into_iter()
        .enumerate()
        .map(|(index, until)| {
            blocked_end = blocked_end.max(until);
            index >= blocked_end
        })
        .collect::<Vec<_>>();
    let lower = end.saturating_sub(request.last_messages);
    let mut start = end;
    let mut bytes = MIN_BYTES;
    let mut encoded = Vec::new();
    for index in (lower..end).rev() {
        let json = serde_json::to_string(&messages[index])
            .map_err(|error| ActorError::TranscriptSerialization(error.to_string()))?;
        let added = json.len() + usize::from(!encoded.is_empty());
        if added > request.max_bytes - bytes {
            break;
        }
        bytes += added;
        encoded.push(json);
        start = index;
    }
    while !safe[start] {
        let removed = encoded.pop().expect("unsafe boundary has a message");
        bytes -= removed.len() + usize::from(!encoded.is_empty());
        start += 1;
    }
    let mut omitted_tool_groups = 0;
    let mut contains_tool = false;
    for (index, message) in messages[..start].iter().enumerate() {
        contains_tool |= message.content.iter().any(|block| {
            matches!(
                block,
                ContentBlock::ToolUse { .. } | ContentBlock::ToolResult { .. }
            )
        });
        if safe[index + 1] {
            omitted_tool_groups += usize::from(contains_tool);
            contains_tool = false;
        }
    }
    let values = encoded
        .into_iter()
        .rev()
        .map(|json| {
            serde_json::from_str(&json)
                .map_err(|error| ActorError::TranscriptSerialization(error.to_string()))
        })
        .collect::<Result<Vec<serde_json::Value>, _>>()?;
    Ok(TranscriptSnapshot {
        messages: serde_json::Value::Array(values),
        through_turn,
        epoch,
        total_messages: end,
        omitted_messages: start,
        omitted_tool_groups,
        compacted,
        bytes,
        truncated: start != 0,
    })
}

#[cfg(test)]
mod tests {
    use maki_providers::{ContentBlock, Message, Role};
    use test_case::test_case;

    use super::{MAX_BYTES, TranscriptRequest, bounded_snapshot};
    use crate::TurnId;

    const FIRST_CALL: &str = "first-call";
    const SECOND_CALL: &str = "second-call";
    const TOOL_NAME: &str = "test-tool";
    const TEXT: &str = "尾部 🐈";
    const EPOCH: u64 = 17;

    fn calls(ids: &[&str]) -> Message {
        Message {
            role: Role::Assistant,
            content: ids
                .iter()
                .map(|id| ContentBlock::tool_use(*id, TOOL_NAME, serde_json::json!({})))
                .collect(),
            ..Default::default()
        }
    }

    fn result(id: &str) -> Message {
        Message {
            role: Role::User,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: id.into(),
                content: TEXT.into(),
                is_error: false,
            }],
            ..Default::default()
        }
    }

    #[test_case(1, MAX_BYTES, 1, 3, 1; "count_intersects_parallel_group")]
    #[test_case(3, MAX_BYTES, 1, 3, 1; "results_cannot_outlive_calls")]
    #[test_case(4, MAX_BYTES, 4, 0, 0; "parallel_group_retained")]
    #[test_case(4, 2, 0, 4, 1; "nothing_fits")]
    fn parallel_group_omissions(
        last_messages: usize,
        max_bytes: usize,
        retained: usize,
        omitted: usize,
        groups: usize,
    ) {
        let messages = vec![
            calls(&[FIRST_CALL, SECOND_CALL]),
            result(SECOND_CALL),
            result(FIRST_CALL),
            Message::user(TEXT.into()),
        ];
        let snapshot = bounded_snapshot(
            &messages,
            None,
            EPOCH,
            false,
            &TranscriptRequest {
                through_turn: None,
                last_messages,
                max_bytes,
            },
        )
        .unwrap();
        assert_eq!(snapshot.messages.as_array().unwrap().len(), retained);
        assert_eq!(snapshot.total_messages, messages.len());
        assert_eq!(snapshot.omitted_messages, omitted);
        assert_eq!(snapshot.omitted_tool_groups, groups);
        assert_eq!(snapshot.truncated, omitted != 0);
        assert_eq!(
            snapshot.bytes,
            serde_json::to_vec(&snapshot.messages).unwrap().len()
        );
        assert!(snapshot.bytes <= max_bytes);
    }

    #[test_case(0, 3, 1; "exact_utf8_suffix_budget")]
    #[test_case(1, 5, 2; "one_byte_below_suffix_budget")]
    fn byte_budget_trims_complete_groups(shortfall: usize, omitted: usize, groups: usize) {
        let messages = vec![
            Message::user(TEXT.into()),
            calls(&[FIRST_CALL]),
            result(FIRST_CALL),
            calls(&[SECOND_CALL]),
            result(SECOND_CALL),
        ];
        let max_bytes = serde_json::to_vec(&messages[3..]).unwrap().len() - shortfall;
        let snapshot = bounded_snapshot(
            &messages,
            None,
            EPOCH,
            false,
            &TranscriptRequest {
                through_turn: None,
                last_messages: messages.len(),
                max_bytes,
            },
        )
        .unwrap();
        assert_eq!(snapshot.omitted_messages, omitted);
        assert_eq!(snapshot.omitted_tool_groups, groups);
        assert_eq!(
            snapshot.bytes,
            serde_json::to_vec(&snapshot.messages).unwrap().len()
        );
        assert!(snapshot.bytes <= max_bytes);
        if shortfall == 0 {
            assert_eq!(
                snapshot.messages,
                serde_json::to_value(&messages[3..]).unwrap()
            );
            assert_eq!(snapshot.bytes, max_bytes);
        } else {
            assert_eq!(snapshot.messages, serde_json::json!([]));
        }
    }

    #[test]
    fn orphan_results_are_not_returned() {
        let messages = vec![result(FIRST_CALL), Message::user(TEXT.into())];
        let snapshot = bounded_snapshot(
            &messages,
            None,
            EPOCH,
            false,
            &TranscriptRequest {
                through_turn: None,
                last_messages: 2,
                max_bytes: MAX_BYTES,
            },
        )
        .unwrap();
        assert_eq!(
            snapshot.messages,
            serde_json::to_value(&messages[1..]).unwrap()
        );
        assert_eq!(snapshot.omitted_messages, 1);
        assert_eq!(snapshot.omitted_tool_groups, 1);
    }

    #[test_case(false, MAX_BYTES, false; "rewrite_is_not_compaction")]
    #[test_case(true, MAX_BYTES, false; "compacted_untruncated")]
    #[test_case(true, 2, true; "compacted_and_truncated")]
    fn compaction_metadata_is_independent_of_caps(
        compacted: bool,
        max_bytes: usize,
        truncated: bool,
    ) {
        let turn_id = TurnId::generate();
        let snapshot = bounded_snapshot(
            &[Message::user(TEXT.into())],
            Some(turn_id),
            EPOCH,
            compacted,
            &TranscriptRequest {
                through_turn: Some(turn_id),
                last_messages: 1,
                max_bytes,
            },
        )
        .unwrap();
        assert_eq!(snapshot.compacted, compacted);
        assert_eq!(snapshot.truncated, truncated);
        assert_eq!(snapshot.through_turn, Some(turn_id));
        assert_eq!(snapshot.epoch, EPOCH);
        assert_eq!(snapshot.omitted_tool_groups, 0);
        assert_eq!(snapshot.omitted_messages, usize::from(truncated));
    }
}
