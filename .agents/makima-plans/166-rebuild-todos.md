## Goal

Make todos reliable session work state without building todo-specific policy into native code. The Lua plugin owns the todo behavior; a small generic history-state primitive provides persistence and history semantics.

## Implementation Summary

Add per-agent, namespaced JSON history state to `maki-storage`, `maki-agent`, and the Lua API. State travels with history snapshots, rolls back with rewind, and is carried forward through compaction. Native code knows only namespaces and JSON values, not todo schemas or UI.

`plugins/todo_write/init.lua` owns validation, replace-all writes, a `todo_read` tool, model output, and the panel. Retain Ctrl+T for todos and Alt+P for the plan. Do not add dependency graphs, a general plugin framework, or cross-key transactions.

## Implementation Plan

### 1. Generic history state

Add a history-owned state timeline: a baseline map from namespace to JSON value, and ordered replacements or deletions anchored to message counts. Namespace values are whole JSON values; a distinct deletion operation removes a key. Same-boundary updates retain commit order. Implement shared pure operations for read, set, delete, prefix truncation, and compaction to baseline.

A boundary count `n` means after the first `n` messages. Rewind keeps updates anchored at or before the retained prefix. A tool update attaches after its enclosing assistant tool-call message; nested calls use that same boundary. During sanitation map each old boundary to the number of retained original messages preceding it; removed messages collapse adjacent boundaries in original order. Synthetic tool results appended at the end follow all mapped old boundaries. Preserve same-boundary update order.

### 2. Checkpoint and history lifecycle

Integrate the generic timeline into `HistorySnapshot` in `maki-storage/src/sessions.rs`, including root and child histories, adoption, orphan pruning, serialization, loading, and state-only checkpoint detection. Extend the existing metadata representation with defaulted fields. Older sessions start with empty generic state, without reconstructing state through tool rendering.

In `maki-agent/src/agent/history.rs`, propagate state with snapshots and use the shared timeline operations for truncation and compaction. Include the truncated timeline in the prepared rewind transaction in `maki-agent/src/actor/history_edit.rs`; failure leaves live and durable state unchanged. Both manual and automatic compaction fold the current values into the new baseline and preserve the original snapshot on failure. History and state must always be checkpointed as the same revision.

### 3. Minimal Lua bridge

Expose generic `get`, `set`, and `delete` operations scoped to the invoking agent's history through the Lua tool context. Bind the capability to the execution agent, not the inherited session ID; cloned nested tool contexts retain it. Child agents get separate history state. One-shot contexts use the same in-memory behavior without persistence. Values must be JSON-serializable; reject invalid values without changing state.

Replace the split mutable `History`/mirror authority with one shared synchronized snapshot owner: ordinary message appends, tool-side state changes, generation checks, snapshot replacement and checkpoint reads all operate on that same owner. Never hold its lock across async tool execution. The mirror is only a published read projection, never a mutation source. Serialize mutations in lock-acquisition order; reads see committed updates in that order. A successful set/delete commits at the invocation, including nested calls; failure of an enclosing tool does not undo a committed nested update. Cancellation preserves already committed changes but does not commit rejected or cancelled-before-commit operations. Publish matching history and state together immediately, including before the tool join barrier. Fence handles by history generation so late writes after rewind, compaction replacement, or reset are rejected.

Expose a read-only agent-handle query for the UI. Add an agent-focus notification based on `App::selected_agent_id()` in `maki-ui/src/app/mod.rs`, including child switches within a session and no-agent focus. Notify on committed generic state changes and history replacement. Guard asynchronous UI refreshes against stale focus. Keep generic APIs namespaced and document their scope and ordering; no todo-specific native API or event.

### 4. Lua todo plugin

Replace the plugin's process-local authoritative table with one namespaced state value. Keep its existing item fields, statuses, and replace-all `todo_write` contract. Validate in Lua before committing, return the resulting list to the model, and render it in the tool card. Add `todo_read` to return current state; an empty write explicitly clears it.

Use the selected agent's generic state to render the panel, refreshing on state changes, focus, resume, and committed rewind. Ctrl+T changes visibility only. Keep UI visibility local; remove turn-end clearing. Reset clears state through the history lifecycle. Restore callbacks render historical tool cards only and never mutate current state.

Remove model-dependent todo summary/restore instructions in `maki-agent/src/prompts/compaction_user.md` and `maki-agent/src/agent/compaction.rs` after deterministic preservation works.

### 5. Extend lifecycle test harnesses

Extend the existing agent tool-dispatch harness to run real nested Lua calls with barriers around the join and checkpoint. Extend storage tests with disk-backed root and child state-only adoption, resume and orphan pruning. Extend event-loop plugin scenarios with controllable agent focus and delayed query responses. Add harness smoke tests proving each can drive and observe these transitions before using it for the acceptance scenarios.

### 6. Documentation

Update Lua API reference and user-facing tool and context documentation. Explain agent isolation, session lifetime, explicit clearing, rewind, compaction, and panel visibility. Regenerate generated references. Update project guidance if needed to document the generic history-state contract.

## Acceptance Criteria

- AC.1: Lua plugins can read, replace and delete independent namespaced JSON values per agent; invalid values are rejected without mutation.
- AC.2: Generic state survives turn end, state-only checkpoints and resume; older sessions load with empty state.
- AC.3: Rewind restores state at the retained boundary, including deletions; failed publication leaves live and persisted state unchanged.
- AC.4: Manual and automatic compaction retain exact generic state; failure rolls back, and post-compaction rewind respects the baseline.
- AC.5: Child histories are isolated; concurrent and nested updates follow the specified ordering, cancellation and generation-fence rules.
- AC.6: Todo tools validate, replace, read, clear and report the authoritative list, with schema and behavior owned by Lua.
- AC.7: The focused agent's todo panel shows current state after turn end, resume, focus switch and rewind; Ctrl+T only toggles visibility, and historical rendering cannot change state.
- AC.8: Generated and user-facing documentation reflects the generic API and todo lifecycle.
- AC.9: The extended dispatch, disk checkpoint and UI harnesses can deterministically drive and observe nested calls, child state-only checkpoints and delayed focus responses.

## Test Strategy

- AC.1 → `history_state_namespaces_and_json_validation`: Lua API integration with two independent namespaces and invalid values.
- AC.2 → `history_state_survives_turn_and_resume`, `history_state_only_checkpoint_round_trip`, `legacy_session_defaults_empty`, `child_state_only_checkpoint_resume_and_prune`: agent and disk-backed storage integration tests.
- AC.3 → `history_state_rewind_prefix_and_delete`, `history_state_rewind_publication_failure`; pure `history_state_timeline_boundaries` tests same-boundary ordering and truncation; `history_state_sanitized_interrupted_resume_and_rewind` tests removed messages and appended synthetic results with anchors on both sides.
- AC.4 → `history_state_manual_and_automatic_compaction`, `history_state_compaction_failure`, `history_state_rewind_after_compaction`; pure `history_state_compaction_baseline` tests fold and later writes.
- AC.5 → `history_state_parent_child_isolation`, `history_state_nested_commit`, `history_state_concurrent_ordering`, `history_state_nested_commit_outer_failure`, `history_state_cancellation`, `history_state_pre_join_write_survives_result_append_and_resume`, `history_state_stale_handle_rejected` (rewind, compaction, reset). Use deterministic barriers rather than sleeps.
- AC.6 → `todo_replace_read_clear_and_validate`: real Lua plugin and model-facing tool integration.
- AC.7 → `todo_panel_tracks_focused_history`, `todo_ctrl_t_after_turn`, `todo_historical_restore_is_pure`, `todo_stale_focus_refresh_is_ignored`: real plugin and event-loop scenarios, including a delayed old-agent response after focus changes.
- AC.8 → `just gen-docs-check`, API/schema assertions, and `review_todo_lifecycle_docs`, a human check of lifecycle and isolation descriptions against tested behavior.

- AC.9 → `dispatch_harness_nested_barrier_smoke`, `checkpoint_harness_child_round_trip_smoke`, `ui_harness_delayed_focus_smoke`: exercise the new test controls and assert observed transitions.

Run scoped crate checks and tests, `cargo fmt --all -- --check`, and `git diff --check`; run `just lint` and `just test` when feasible.

## Review Strategy

Review the plan with `plan_reviewer` and address its findings. After implementation and automated checks, dispatch a `general` reviewer focused on generic API boundaries, concurrency, persistence, and rollback. Fix or explicitly rebut findings; repeat on critical findings until resolved or blocked.

## Documentation Strategy

Document the generic Lua API and its ordering, isolation, value and history semantics in the canonical Lua reference. Document todo behavior in user-facing tool/context docs; generate references with `just gen-docs`. Avoid duplicating reference details.

## Risks, Blockers, and Required Decisions

- Generic state must be synchronized with history, not independently sampled at checkpoint time. Nested calls, cancellation and stale handles need explicit tests.
- History sanitation can move message boundaries; preserve their intended associations.
- Older sessions may have lost todo state to compaction; do not guess from historical render callbacks. Older binaries may discard unknown metadata when rewriting.
- Full snapshots per update trade space for simplicity. Measure before optimizing; do not introduce a general plugin framework.
- Existing event-loop, checkpoint and tool harnesses need extensions for real Lua calls, agent focus and disk-backed lifecycle tests.
