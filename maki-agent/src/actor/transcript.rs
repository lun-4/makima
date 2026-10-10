use std::collections::HashMap;

use maki_providers::ContentBlock;

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
    pub bytes: usize,
    pub truncated: bool,
}

impl AgentActorHandle {
    pub fn transcript(&self, request: TranscriptRequest) -> Result<TranscriptSnapshot, ActorError> {
        if !(1..=MAX_MESSAGES).contains(&request.last_messages)
            || !(MIN_BYTES..=MAX_BYTES).contains(&request.max_bytes)
        {
            return Err(ActorError::InvalidTranscriptCaps);
        }
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
            .or_else(|| state.latest.as_ref().map(crate::TurnOutcome::turn_id));
        let end = if let Some(turn_id) = through_turn {
            let coverage = self
                .inner
                .coverage
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match coverage.get(&turn_id) {
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
        let messages = &history.messages[..end];
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
            epoch: history.epoch,
            total_messages: end,
            omitted_messages: start,
            bytes,
            truncated: start != 0,
        })
    }
}
