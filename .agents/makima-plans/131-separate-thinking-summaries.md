# Separate consecutive OpenAI thinking summaries into distinct blocks

## Goal

OpenAI Responses thinking summaries render as separate `thinking>` blocks in the transcript, instead of being concatenated into one run of text. Each summary part becomes its own `ContentBlock::Thinking`, and the streaming UI starts a fresh `thinking>` block at every part boundary. Replay of the persisted message shows the same separation.

## Implementation Summary

The root cause is in the OpenAI Responses SSE parser (`maki-providers/src/providers/openai/responses.rs`). Today every `response.reasoning_text.delta` / `response.reasoning_summary_text.delta` lands in a single `reasoning_text` buffer; `response.reasoning_summary_part.added` only appends `"\n\n"` to the eventual persisted block, and sends no streaming event at all. So the live transcript concatenates parts with no boundary, and the persisted message is one Thinking block.

Fix: introduce an explicit boundary event, `ThinkingBlockEnd`, on `ProviderEvent` (`maki-providers`) and `AgentEvent` (`maki-agent`). The Responses parser keeps each summary part in its own buffer, emits one `ContentBlock::Thinking` per part, and emits `ThinkingBlockEnd` whenever a new part begins while the current buffer is non-empty. The TUI flushes its in-progress thinking buffer on that event, so the next `ThinkingDelta` opens a new `thinking>` block. Restore already maps each `ContentBlock::Thinking` to its own `DisplayMessage`, so live and replay agree.

Touch points:

- `maki-providers/src/types.rs` - `ProviderEvent` gains a unit variant.
- `maki-providers/src/providers/openai/responses.rs` - `parse_sse` accumulation + boundary emission + per-part blocks; tests.
- `maki-providers/src/providers/openai_compat.rs` - one exhaustive test match needs a new arm.
- `maki-agent/src/types.rs` - `AgentEvent` gains the same unit variant.
- `maki-agent/src/agent/streaming.rs` - forward mapping (`forward_provider_events`).
- `maki-ui/src/components/messages/mod.rs` - public `end_thinking_block()` that flushes.
- `maki-ui/src/chat.rs` - `handle_event` arm; tests.
- `src/sdk_mode.rs` - `StreamSynth::thinking_block_end()` + `handle` arm.
- `maki-acp/src/server.rs` - arm emitting a separator; `maki-acp/src/translate.rs` - separator helper + replay split.
- `src/print.rs` - ignore the new event.
- `maki-ui/src/app/btw.rs` - consumes `ProviderEvent` with a `_ => continue` wildcard, so it compiles unchanged. Boundaries are intentionally not surfaced in the side-question panel (out of scope for the transcript); no edit there.

Non-goals: no change to Anthropic/Google/openai_compat reasoning accumulation (they do not emit per-part summary boundaries today); no config or keybinding changes; no change to the `show_thinking` behavior.

## Implementation Plan

### Phase 1 - Event plumbing

1. `maki-providers/src/types.rs`: add to `ProviderEvent` (currently line ~335):
   ```rust
   /// The current reasoning block is complete; the next `ThinkingDelta`
   /// belongs to a new block.
   ThinkingBlockEnd,
   ```
2. `maki-agent/src/types.rs`: add the identical unit variant to `AgentEvent` (currently line ~749). The enum derives only `Serialize` with `#[serde(tag = "type", rename_all = "snake_case")]`, so this is additive on the wire and no deserializer needs updating.
3. `maki-agent/src/agent/streaming.rs` (`forward_provider_events`): map
   ```rust
   ProviderEvent::ThinkingBlockEnd => AgentEvent::ThinkingBlockEnd,
   ```
4. Fix the now non-exhaustive match in `maki-providers/src/providers/openai_compat.rs` (test around line 826-832) by adding `ProviderEvent::ThinkingBlockEnd => {}`.

### Phase 2 - OpenAI Responses parser

In `maki-providers/src/providers/openai/responses.rs::parse_sse`:

- Replace `let mut reasoning_text = String::new();` (line ~241) with:
  ```rust
  let mut thinking_parts: Vec<String> = Vec::new();
  let mut current_thinking = String::new();
  ```
- In the `"response.reasoning_text.delta" | "response.reasoning_summary_text.delta"` arm (line ~444): append to `current_thinking` instead of `reasoning_text`, keep sending `ProviderEvent::ThinkingDelta`.
- Replace the `"response.reasoning_summary_part.added" if !reasoning_text.is_empty()` arm (line ~461) with:
  ```rust
  "response.reasoning_summary_part.added" if !current_thinking.is_empty() => {
      thinking_parts.push(std::mem::take(&mut current_thinking));
      event_tx.send_async(ProviderEvent::ThinkingBlockEnd).await?;
  }
  ```
  The first `part.added` (empty buffer) is a no-op; the boundary is emitted before the next part's deltas, which is what the UI needs.
- At the block-build site (line ~528): flush the trailing buffer, then push one Thinking block per part before the Text block:
  ```rust
  if !current_thinking.is_empty() {
      thinking_parts.push(std::mem::take(&mut current_thinking));
  }
  for part in thinking_parts {
      content_blocks.push(ContentBlock::Thinking { thinking: part, signature: None });
  }
  ```
- Rewrite the `parse_sse_reasoning_summary_part_added` test (line ~1093): expect `[Thinking("First part"), Thinking("Second part"), Text("Answer")]` and the event sequence `[ThinkingDelta("First part"), ThinkingBlockEnd, ThinkingDelta("Second part")]`.

### Phase 3 - TUI

1. `maki-ui/src/components/messages/mod.rs`: add next to `thinking_delta` (line ~208):
   ```rust
   pub fn end_thinking_block(&mut self) {
       self.flush_thinking();
   }
   ```
   `flush_thinking` (line ~1266) already pushes a `DisplayRole::Thinking` message and resets `thinking_collapsed`, and it no-ops on an empty buffer.
2. `maki-ui/src/chat.rs` `handle_event` (line ~101): add
   ```rust
   AgentEvent::ThinkingBlockEnd => self.messages_panel.end_thinking_block(),
   ```
   The match is exhaustive today, so the compiler will point at this site.

No change is needed in `history_to_display` (line ~476): it already pushes a separate `DisplayMessage` per `ContentBlock::Thinking`.

### Phase 4 - Other consumers

1. `src/sdk_mode.rs`: add to `StreamSynth` (near `close_block`, line ~437) a method that only closes a thinking block:
   ```rust
   fn thinking_block_end(&mut self) -> Vec<Value> {
       if self.current_block == Some(BlockKind::Thinking) {
           self.close_block().into_iter().collect()
       } else {
           Vec::new()
       }
   }
   ```
   In `handle` (line ~1370) add, gated like its neighbours:
   ```rust
   AgentEvent::ThinkingBlockEnd => {
       if self.include_partial_messages {
           let events = self.synth.thinking_block_end();
           self.emit_stream(events)?;
       }
   }
   ```
   The next `ThinkingDelta` then reopens a thinking block with an incremented index, producing two Anthropic thinking blocks.
2. `maki-acp/src/translate.rs`: add
   ```rust
   const THINKING_SEPARATOR: &str = "\n\n";
   ```
   and a helper `pub fn thinking_block_end() -> SessionUpdate { thinking_delta(THINKING_SEPARATOR) }`. ACP has no block-boundary concept, so a paragraph break is the only representation that keeps chunks visually separate.
3. `maki-acp/src/translate.rs::replay_assistant` (line ~338): when the previous emitted block was `Thinking` and the current block is `Thinking`, push the separator chunk before the second block, so replay matches the live boundary.
4. `maki-acp/src/server.rs` (line ~1001): add `AgentEvent::ThinkingBlockEnd => translate::thinking_block_end(),`. The match currently ends in `_ => continue` (line ~1098), so nothing forces this arm; cover the live wiring with a pump-level test (see Test Strategy).
5. `src/print.rs` (line ~365): add `AgentEvent::ThinkingBlockEnd` to the ignored or-pattern arm next to `AgentEvent::ThinkingDelta { .. }`.

### Phase 5 - Tests

See Test Strategy for named tests. Add them alongside the existing tests in each file, and run the cheapest scope first (`just check`, then `just lint`, then the crate's tests via `cargo nextest run -p <crate>`), finishing with `just test`.

## Acceptance Criteria

- AC.1 The OpenAI Responses parser produces one `ContentBlock::Thinking` per summary part, with no `"\n\n"` concatenation, and preserves content order (thinking parts before text). Verified by `parse_sse_summary_parts_become_separate_thinking_blocks`.
- AC.2 The parser emits exactly one `ProviderEvent::ThinkingBlockEnd` between the deltas of consecutive summary parts, and none before the first part. Verified by `parse_sse_summary_parts_become_separate_thinking_blocks`; `parse_sse_summary_single_part_has_no_boundary` additionally guards against over-emission (it passes pre-change, so it is a guard, not validation of the new behavior).
- AC.3 The agent forwards `ProviderEvent::ThinkingBlockEnd` as `AgentEvent::ThinkingBlockEnd` (and does not drop it). Verified by `forward_provider_events_maps_thinking_block_end`.
- AC.4 A live `ThinkingBlockEnd` in the TUI ends the current streaming thinking message so a following `ThinkingDelta` starts a new `thinking>` block. Verified by `thinking_block_end_starts_new_block` (panel level) and `chat_thinking_block_end_flushes_panel` (chat level).
- AC.5 Restore does not merge consecutive `ContentBlock::Thinking` entries: `history_to_display` yields separate, ordered `DisplayRole::Thinking` `DisplayMessage`s for the shape the parser now emits. Verified by `history_to_display_splits_consecutive_thinking_blocks`, a regression guard on unchanged restore code (the new behavior itself is validated by AC.1).
- AC.6 SDK stream-json mode closes the thinking content block on `ThinkingBlockEnd`, so a following `ThinkingDelta` emits a new `content_block_start` with an incremented index. Verified by `thinking_block_end_closes_thinking_block`.
- AC.7 ACP emits a paragraph break between consecutive summaries both in the live event pump and on replay. Verified by `event_pump_emits_thinking_separator` (live wiring), `thinking_block_end_emits_separator` (translate helper), and `replay_separates_consecutive_thinking_blocks` (replay).
- AC.8 The workspace builds and lints clean with the new variants handled everywhere. Verified by `just check` and `just lint` exiting 0.

## Test Strategy

All tests are unit/render tests in the crates touched; no new infrastructure is required (each layer already has a `#[cfg(test)]` module, the provider has the `run_sse` helper at `maki-providers/src/providers/openai/responses.rs:612`, and ACP has the `start_event_pump` harness used at `maki-acp/src/server.rs:1388`).

| AC | Test | Location / layer |
| --- | --- | --- |
| AC.1, AC.2 | `parse_sse_summary_parts_become_separate_thinking_blocks` | `maki-providers/src/providers/openai/responses.rs` (parser unit) |
| AC.2 | `parse_sse_summary_single_part_has_no_boundary` | `maki-providers/src/providers/openai/responses.rs` (parser unit; over-emission guard) |
| AC.3 | `forward_provider_events_maps_thinking_block_end` | extend the existing `mod tests` in `maki-agent/src/agent/streaming.rs` (line 159); drive `forward_provider_events` with a `flume` channel and `EventSender::new(tx, 0)`, assert the received `AgentEvent` |
| AC.4 | `thinking_block_end_starts_new_block` | `maki-ui/src/components/messages/tests.rs` |
| AC.4 | `chat_thinking_block_end_flushes_panel` | `maki-ui/src/chat.rs` tests |
| AC.5 | `history_to_display_splits_consecutive_thinking_blocks` | `maki-ui/src/chat.rs` tests (restore regression guard) |
| AC.6 | `thinking_block_end_closes_thinking_block` | `src/sdk_mode.rs` tests (next to `block_transition_closes_previous_and_increments_index`) |
| AC.7 | `event_pump_emits_thinking_separator` | `maki-acp/src/server.rs` tests; `start_event_pump` as in `malformed_question_in_elicitation_mode_unblocks_the_tool` (`server.rs:1388`) |
| AC.7 | `thinking_block_end_emits_separator` | `maki-acp/src/translate.rs` tests |
| AC.7 | `replay_separates_consecutive_thinking_blocks` | `maki-acp/src/translate.rs` tests |
| AC.8 | `just check` + `just lint` | workspace build |

The tests that exercise changed code (AC.1-AC.4, AC.6, AC.7) fail if the boundary event or per-part splitting regresses. AC.5 is an explicit regression guard on restore code this plan does not modify, fed by the parser shape introduced in AC.1. AC.8 is a build/lint gate, not a behavioral test. No criterion relies on code inspection.

## Review Strategy

Plan-mode review: run a `plan_reviewer` subagent over this file before `plan_submit`; fix or rebut every finding and re-run if any critical/high finding appears.

Implementation review: after all automatable tests pass, dispatch a `general` subagent to review the diff against this plan (correctness of the SSE boundary logic, every event consumer handled, tests actually exercising the new behavior). Fix or rebut all findings; re-run if critical findings remain.

## Documentation Strategy

No user-facing documentation change is required. The change affects how reasoning is displayed, not configuration, commands, providers, or the Lua API, so the generated docs (`just gen-docs`) and the `show_thinking` entry in `site/docs/content/configuration/_index.md` are unaffected. The new `AgentEvent` variant is internal to the process boundary: the SDK synthesizes Anthropic wire events, so no documented stream-json shape changes (the partial-message sequence for thinking now contains N `content_block_start`/`content_block_stop` pairs instead of one, but no doc enumerates that shape). `AGENTS.md` needs no update because no architecture or workflow changes.

## Risks, Blockers, and Required Decisions

- Event semantics assumption: the fix relies on `response.reasoning_summary_part.added` being emitted once per summary part. The current parser already depends on this event, and the existing test encodes it, so the risk is low. If a provider omits it, parts stay merged as today (graceful degradation).
- Persisted shape change: a message now carries N `ContentBlock::Thinking` blocks instead of 1. Audited consumers: UI `history_to_display` (handles N), ACP `replay_assistant` (updated in Phase 4), `maki-agent/src/agent/compaction.rs::strip_thinking` (block-wise, fine), `maki-agent/src/agent/run.rs` token estimate (block-wise, fine, `run.rs:683`), OpenAI `convert_input` (skips Thinking, fine), Google `push_or_extend_thinking` (`google.rs:550`, coalesces at creation, not affected by read-side changes), Anthropic conversion (`anthropic/mod.rs`, skips thinking on outbound), Mistral (keyed off `reasoning_content`, not `ContentBlock::Thinking`). No new failure mode with multiple thinking blocks.
- ACP separator: inserting `"\n\n"` is presentation-only because ACP has no block-boundary primitive. This is a deliberate, documented choice; the fallback if a reviewer objects to synthetic text is to ignore the event in ACP and accept concatenated live chunks (replay would still be separate chunks). No operator decision is required; the plan defaults to keeping summaries visually separated.
- No blockers remain. Required decisions: none.
