## Context

Follow-ups from an eight-agent read-only review of `d617e130..d956a5b1` on `luna.agent-5-lua-api` (PR #160): the post commits `1f516711` and `998268cc`, Luna's `aca94b7c` "Unify main and subagent runtime and input handling", and `d956a5b1` "Prevent restored subagent input from reaching the main agent". The spec for `aca94b7c` is `24-pr-5-3-align-subagent-runtime.md`. A summary was posted to PR #160 (issuecomment-6251649876).

Line numbers are at `d956a5b1`. Each item says how it was verified: "hand-checked" means the code was re-read outside the reviewing agent; "agent-read" means the reviewer traced it in code but nobody reproduced it; "unreproduced race" means reproduce it with a test before fixing.

## Suggested order

1. Posts and rewind (H4, M8). Same code, fix together.
2. Restored-chat targeting (H1). Highest user impact.
3. Small self-contained fixes: H2, H3, M7.
4. Actor races (M5, M6, L1, L2), each with a test first.
5. Approvals and queue (M9, L3, L4).
6. Spec and docs (M10, M11), then the cleanup list.

## High

### H1. Commands from a restored subagent chat reach the main agent (hand-checked)

`selected_agent` (`maki-ui/src/event_loop.rs:805-810`) does `manager.lookup(runtime.app.selected_agent_id().unwrap_or(root))`. Its doc comment says "there is no fallback to the root". `d956a5b1` leaves restored chats with `agent_id == None`, so they resolve to the main agent. `orchestration_revision` (`:1999-2001`) uses the same `unwrap_or(root)`, so focusing a restored chat keeps `selected` equal to the root id and `sync_orchestration` never resyncs.

Only typed text and the `!` prefix are guarded (`submit_prompt` and shell check `active_subagent_closed()`; `handle_cancel` filters by selected id). Unguarded paths, all reaching the main agent from a restored chat:

- `sync_orchestration` (`:2416-2433`) registers the root as the command admission for `command_target`, which every pinned command uses: `/compact`, `/cd`, `/fast`, `/workflow`, `/model <spec>`, the `/model` picker, working-directory and fast-mode-supported queries, custom `AgentTurn`/`IsolatedTurn` commands (`submit_command_turn` only checks liveness).
- Actions via `selected_target` (roughly `:4500-4590`): `Action::Compact`, `Action::ChangeDirectory`, `Action::ChangeModel`, `ToggleSessionOption` for fast/workflow.
- `UiAction::SetMode` via `dispatch_mode_change_op` (`:638`).
- `orchestration.select(focused)` for Lua focused-agent APIs.

Repro: resume a session with a finished subagent, open Tasks, select its chat, run `/compact` (main history compacts) or `/cd foo` (main cwd changes).

Fix: resolve strictly, e.g. `selected_agent_id().or(app.is_main_chat().then_some(root))`, returning `None` for a non-main chat without an actor. Apply the same rule in `orchestration_revision` (give it a sentinel such as `live: false` so focus changes resync). Callers must surface a closed-target or `StaleTarget` error rather than do nothing silently.

Tests: an event-loop test dispatching `/compact`, `/cd` and a mode change from a restored chat, asserting the main agent's history, cwd and mode are unchanged and an error is reported. This is the spec's `closed_agent_rejects_without_root_fallback` and `focused_agent_command_target_matrix`. Existing `restored_task_keyboard_submission_never_reaches_main` and `tasks_selection_routes_keyboard_submission_through_child_actor` only cover typed text and `!pwd`.

### H2. Images count toward the 256 KiB instruction limit (hand-checked)

`submission_bytes` (`maki-agent/src/actor/mod.rs:760-768`) sums `message.len()` plus `serde_json::to_vec(&input.images)` (base64 data included) plus the serialized preamble. `submit` (`:2036-2042`) and the outstanding-bounds folds (`:2105-2117`, `:2176-2184`, `:2244-2256`) compare it against `MAX_PENDING_INSTRUCTION_BYTES` (256 KiB, `inbox.rs:12`). Keyboard input now goes through `actor.submit` (`maki-ui/src/app/queue.rs:398`, `agent/shared_queue.rs:329`), so a pasted ~300 KB PNG fails with "instruction inbox capacity exceeded (64 messages or 256 KiB)". The UI allows `MAX_IMAGE_BYTES` = 20 MiB (`maki-ui/src/image.rs:11`). Lua `admit_turn_after` gets the same check, and `drained` keeps bytes for the whole turn, so one large image also blocks later steering.

Fix: count text and preamble only, or give images a separate bound aligned with `MAX_IMAGE_BYTES`/`MAX_IMAGES`. Measure without serializing (sum `data.len()` if images stay counted) to avoid copying every image per submit.

Test: submit an input with an image over 256 KiB through the actor and the UI path; it must be admitted. Spec name: `shared_submission_preserves_images_and_preamble` / `shared_pending_bounds_and_multibyte_payloads`.

### H3. Lua slash commands cannot open the editor (hand-checked)

Outside a turn, `open_editor` (`maki-lua/src/api/ui/mod.rs:436-450`) takes its target from `pinned_target(&lua)`. With orchestration services installed (the TUI installs them, `event_loop.rs:2349`), `None` returns -1. `pinned_target` is only set on the event-callback path (`maki-lua/src/runtime.rs:5173`) and the keybind path (`:5481`). The `ExecuteCommand` path (`:5185-5222`) builds `LuaCtx::trusted_context(target)` and runs `run_command_scoped`, which sets only `command_depth` and `command_invocation`. The UI side would also reject a `None` target (`event_loop.rs:3215-3218`).

Repro: `/memory` (`plugins/memory/init.lua:329`), pick a file, Enter: `maki.ui.open_editor(path)` returns -1.

Fix: in the `ExecuteCommand` branch set `lock_cell(scope.handle()).pinned_target = Some(target.clone())` when the target resolves, like the keybind path. Alternative: fall back to `services.resolve_target(command_invocation)` in `open_editor`.

Test: go through `register_command` + `run_command` and assert the editor request carries the invocation agent. `editor_request_retains_the_invocation_agent` (`maki-lua/src/api/agent/handles/mod.rs:735`) sets `pinned_target` manually and should stop doing so.

### H4. Post pruning and display broke with the rewind refactor (hand-checked)

- `prune_orphans` (`maki-storage/src/sessions.rs:1675`) has no production caller. Its only caller was the old `rewind_to` in `maki-ui/src/app/session.rs`; rewind now goes `Action::RewindAgent` -> `dispatch_rewind` -> `coordinator.replace_history` -> `apply_history_change` -> `replace_shared_messages`, which never prunes. Remaining calls are tests (`sessions.rs:2011`, `:2304`, `:3847`).
- Effects: main-chat posts past the rewind point survive and, on restore, `push_posts_at` clamps them onto later unrelated history (post at `after_message = 6`, rewind to 2, grow to 8: the post lands after the new message 6). Orphaned `subagents`, `subagent_messages`, `tool_outputs` (and their offload state) are also kept. The `!tool_outputs().contains_key("tool-1")` assertion was dropped from `rewind_to_first_turn_clears_everything`.
- `apply_history_change` (`maki-ui/src/app/session.rs:371-376`) calls `history_to_display(..., &[])`, so after a rewind or a plan clear-context every post in that chat vanishes from the live view (including spawn announcements), then reappears on the next restore.
- `prune_orphans` never pruned child-chat posts by position. Child rewind (`checkpoint_child_history`) and child plan clear-context (`maki-ui/src/event_loop/approval.rs:300-445`) leave `chat: Some(child)` posts past the new child history.
- `Post.agent` is only read by `prune_orphans`, so the `agent` option currently has no production effect.
- The plan's line 95 ("prune only the selected history's orphan display/tool references") was not built.

Fix: after a successful history change for chat C (main or child), drop C's posts with `after_message > new_len`; for main, also run the orphan prune for tool outputs and subagent records, keeping children that surviving posts or messages reference, and keeping live children per the spec. Then rebuild with `posts_in(self.state.session.posts(), chat_key)` instead of `&[]`. Restore the `tool-1` assertion.

Tests: UI test with posts before and after a rewind point in main and in a child chat: after rewind the earlier post is still displayed, the later one is gone from the screen and from `session.posts()`, and a reload matches.

## Medium

### M5. Close racing a queued history edit hangs forever (agent-read; `close_internal` does not touch `state.idle`, confirmed)

The edit is parked in `state.idle.edit` (`maki-agent/src/actor/history_edit.rs:126-133`, `154-168`). `close_internal` (`actor/mod.rs:3004-3059`) never clears `state.idle`. The runner (`runner.rs` `step`) wakes, sees the lifecycle and returns `Step::Exit` without `pop_work`. The completion sender lives in `ActorInner`, kept alive by the awaiting future's `self.actor`, so `response.recv_async()` never resolves. The detached tasks at `event_loop.rs:5041` (rewind) and `approval.rs:415` (plan approval) never send `HistoryRewound`/`PlanApprovalReady`, and the prepared operation is never failed.

Fix: in `close_internal`, take `state.idle`; if it holds an edit, send `Err(lifecycle_error(lifecycle))` and `fail()` the prepared op. Or drain `idle.edit` in the runner before exiting.

Test: queue a rewind, close before the runner is scheduled (gate the runner), assert the rewind future resolves with a lifecycle error. `rewind_checkpoint_is_unpublished_until_ack_and_survives_close` only closes after the pop.

### M6. Local cancel can finish a turn Completed after dropping joined input (unreproduced race)

`capture_local_cancel` (`actor/mod.rs:1572`) removes the running turn's inbox under the state lock, but the cancel token fires later in `settle_local_cancel`, after the lock is released. `close_admission` (`inbox.rs:182-184`) returns `true` when the inbox is missing. Sequence: turn T reaches `TurnProgress::Done` and passes `cancel.is_cancelled()` in `run_loop` (`maki-agent/src/agent/run.rs:539`); message M joins T's inbox (e.g. `task_send` into a child); UI `cancel_local` removes the inbox with M; before `fire()`, the loop drains nothing, `join_children` resolves (main), `finish()` returns true, `run_loop` returns `Ok`, and T's ticket (also M's) resolves Completed. M never reached the provider (AC.3). `cancel_existing` (`mod.rs:2690`) has the same window.

Fix options: have `close_admission` return `false` when the processing record for this turn has `cancellation_reason` set; or fire the token inside the cut; or have `retire_and_publish` convert the outcome to Cancelled when the processing cut was set.

Test first: gate the agent loop between `Done` and `finish`, join a message, `cancel_local`, release; assert the ticket is Cancelled (or M ran). Spec name: `local_cancel_submission_and_install_races`. No existing test runs `cancel_local` against an active turn with joined input; `local_cancellation_preserves_pending_configuration_and_advances_fence` (`actor/tests.rs:4387`) has no active turn.

### M7. `maki.session.prompt` / `session.new{prompt}` regressed (agent-read)

`SessionRuntime::submit_text` (`maki-ui/src/event_loop.rs:1323-1353`) used to call `app.submit_prompt`; it now pushes a raw `QueueItem::Message`. Lost: `@prefix:value` expansion (`expand_references` is only called in `app/queue.rs:325`), pending main-chat shell results (`shell.drain_results`) as preamble, and the `"started"` return: it always returns `SubmitOutcome::Queued`, breaking the documented `"started" | "queued"` contract (`maki-lua/src/api/session.rs:338`). The doc on `submit_prompt` ("shared by the keyboard and Lua paths so they cannot drift") is now false. `EMPTY_SESSION_PROMPT_ERR` duplicates `queue::EMPTY_PROMPT_ERR`.

Repro: `maki.session.prompt("@subagent:research do x")` on an idle session: the model gets the literal text and the call returns `"queued"`.

Fix: keep session scope (targeting the root is correct) but expand references through `app.lua_event_handle`, drain root shell results into `input.preamble`, return `Started` when the root actor was idle (or change the Lua doc), reuse `EMPTY_PROMPT_ERR`.

### M8. Child post positions are stale during a run (agent-read)

`push_post` child branch (`maki-ui/src/app/mod.rs:2493`) takes `after_message` from `session.subagent_messages()[id].len()`. That copy is only refreshed by `relay_history` (`maki-lua/src/api/agent.rs:509`, `:646`, `:708`) at run end, cancellation or compaction. A post during the child's first run records 0 and restores above the child's task prompt. `subagent_post_lands_in_the_child_chat` asserts 0 with no history and cannot catch it.

Fix: count from the child chat's committed message count (or the child actor's live history). Do this together with H4.

### M9. Child plan clear-context can erase the start of the implementation turn (agent-read, ordering-dependent)

The child path in `maki-ui/src/event_loop/approval.rs:377`, `435-459` never calls `operation.set_turn_displayed(true)` (main paths do at `:471` and `:672`). So the actor emits `QueueItemConsumed` (`run.rs:412`) on `agent_tx` when the committed turn starts, drawing the prompt bubble, while `ChildCommitted` arrives via the commit task on `internal_tx`; `finish_child_plan_approval` then runs `apply_history_change` -> `load_messages(display)` and erases the bubble and any early stream output.

Fix: mirror main: `set_turn_displayed(true)` before `commit_prepared`; in `finish_child_plan_approval`, after `apply_history_change`, push the plan and `show_user_message(plan.message)`. Test: extend `managed_child_approval_is_pinned` to assert the child chat display after ClearAndImplement.

### M10. Spec drift in `24-pr-5-3`

- Line 170 still says "No implementation has started". Line 95's prune requirement is H4.
- `docs/agent-system.md:99` still says "The TUI root uses `rush(RootWork)`"; that API was removed (spec step 8 asks for this update).
- Acceptance-criteria tests that do not exist under any name in `*.rs`/`*.lua` (grep at `d956a5b1`). Either write them or map the AC to the existing tests that cover it:
  - AC.1: `shared_submission_preserves_images_and_preamble` (see H2)
  - AC.2: `submission_respects_queued_boundaries`, `mixed_sender_submission_preserves_authority`, `cwd_admission_order_is_target_local`
  - AC.3: `shared_submit_finish_race_both_sides` (partly `submit_races_finish_on_both_sides`), `shared_consumption_wrapper_and_request_order`, `shared_instruction_failure_and_budget_outcomes`, `steering_invalidates_structured_report`
  - AC.4: `focused_queue_consumption_and_recall_parity`, `pending_submission_removal_races_drain` (partly `stable_submission_removal_preserves_promoted_input_and_metadata`), `rejected_admission_has_no_phantom_bubble`
  - AC.5: `exact_and_idle_admission_policies_survive_unification`, `task_shared_turn_wait_and_retention`, `shared_turn_events_and_usage_once`, `nested_wait_capacity_one_progress`
  - AC.6: `completion_policy_main_vs_nonmain`
  - AC.7: `local_cancel_submission_and_install_races` (see M6), `cancel_preserves_config_and_reports`, `cancel_releases_wait_without_closing_child`
  - AC.8: `continuation_finalization_arrival_and_wait_claim_races`
  - AC.9/AC.11: `focused_agent_command_target_matrix`, `rewind_rejects_compacted_picker_boundary`, `closed_agent_rejects_without_root_fallback`, `delayed_ui_action_keeps_agent_target`, `focused_controls_enforce_mode_ceiling` (see H1)
  - AC.12: `restored_and_headless_input_use_shared_submission`, `child_completion_notices_remain_typed`
  - AC.14: `task_and_agent_docs_match_shared_runtime`
- `graph_quiescence_probe_detects_extra_continuation` (`maki-lua/tests/runtime_alignment.rs:557-688`) does not prove AC.13: it answers every extra request and waits for idle before probing, so it passes if the barrier returns immediately. Call the barrier while the extra continuation's request is still gated and assert it does not settle until released.
- `history_edit.rs` coverage: `rewind_serializes_concurrent_child_report` (`:687-746`) withdraws the report before releasing the checkpoint, so it never shows the delivery committing into rewritten history (AC.9). No test for a boundary from another actor (`boundary.agent_id` check, `:286`) or one invalidated by compaction.
- No test drives `/compact`, `/cd` etc. through `EventLoop::handle_command`; App tests exercise branches production no longer reaches (see cleanup).

### M11. Docs

- The main/non-main completion and late-report rules are duplicated across `site/docs/content/quick-start/_index.md:126-136`, `plugins/_index.md` ("Focused-agent UI" and paragraph ~102), `tools/_index.md` (`task_wait`/`task_send`), `token-economy/_index.md:33,61`. AGENTS.md wants one canonical home: pick a concept page and link from the others; quick-start should not carry reference material.
- Tone: spec register ("captured current work", "pre-cut", "configuration/control work", "boundary-separated sends receive a successor ID", slash pairs like "model/tool", "close/despawn"). token-economy lost its bold lead-ins ("Subagents as garbage collectors.", "Interrupted work is not wasted."); quick-start's "Cancellation." bullet lost its bold.
- `maki-lua/src/docs_render.rs:1612`, `:1629` dropped the only statement that cancelling a managed turn closes the child subtrees it delegated, now pointing at "other cancellation APIs retain their documented ownership rules" (documented nowhere). It is still true: `ticket:cancel` (`handles/tickets.rs:160`) -> `manager.cancel_turn` -> `capture_turn_children` (`manager/mod.rs:1238-1290`). Restore a sentence like "Cancelling a managed turn (for example `ticket:cancel`) closes the child subtrees that turn delegated; UI cancellation does not", and say it in the `ticket:cancel` doc ("Cancel only this admitted turn." misleads).
- `plugins/task/init.lua:544`, `:557`: `task_wait` now explains main vs non-main completion and `task_send` repeats the boundary list. These go to the model every request; trim to what the caller needs.
- `maki.ui.post` docs (`maki-lua/src/api/ui/mod.rs:740`) say `agent` names "a child … that the post is about" without saying its effect (keeps the child's transcript through rewind, once H4 is fixed). `show` doc in `maki-lua/src/api/agent.rs` and `module_fns.rs:269`: "which already have its row" -> "which already have a tool row". Regenerate with `just gen-docs`.

### M12. Duplicated child-history checkpoint closure

`maki-ui/src/event_loop.rs:5014-5033` and `maki-ui/src/event_loop/approval.rs:300-318` both look up the chat's `subagent_id`, build the same `checkpoint_child_history` closure, and inline "child history storage identity is unavailable". Extract a helper and a const.

## Low

### L1. Steering lockout after any reserved operation (agent-read)

`submit`'s active-target filter (`actor/mod.rs:2057-2064`) requires `inbox.operation == state.next_operation_id`. Every `reserve_config_update`/`reserve_prepared_operation` bumps `next_operation_id`, including ones later cancelled, expired or committed as no-ops; the old fold compared `policy_generation`, which only moves on real change. Trigger: model-discovery refresh `reserve_refined_model_updates` (`event_loop.rs:880`) committing an equivalent config, or a mode/cwd change failing validation. After that, every ordinary message waits for the current turn instead of steering it. Fix: treat the turn as joinable when no live operation with id > `inbox.operation` remains in `operations`/queue (or when `policy_generation` is unchanged and none is pending).

### L2. `cancel_local` keeps reserved prepared turns (agent-read)

The keep closure at `actor/mod.rs:1575-1578` (with `drain_work_except`, `:973`) is `!matches!(Turn non-delivery)`, which keeps `ActorOperation::Prepared { turn: Some(..) }`. A reserved but uncommitted plan-approval implementation turn (`approval.rs:321`, `:646`) still starts after the user presses stop. Fix: drain prepared ops with `turn.is_some()` via `pending.fail(PolicyCancelled)` through `settle_deferred`, or document it as intended.

### L3. Queued compacts removed by shared `run_id`

`queue_compact` stamps `self.run_id`, constant while busy; `remove_pending_compact` (`maki-ui/src/agent/shared_queue.rs:228`, `maki-ui/src/app/queue.rs:145`) drops the first match and searches `operations` before the queue while `pending_queue` lists the queue first. `/compact A`, `/compact B` while busy, remove B's row: A is removed. Fix: expose the operation's unique id in `PendingQueueEntry::Compact` and remove by it (AC.4).

### L4. Child plan-approval commit has no timeout

`commit_child_plan_approval` (`approval.rs:377-433`) sets `committing = true`, takes `cancel`, and `end_plan_approval` returns early while committing, so Esc is ignored. The commit task is not wrapped in `bounded_session_op` like other steps. A stuck checkpoint gates child input and, since `pending_approval` is per-session, every later `ApprovePlan` (`:262`, `:705`). Fix: wrap in `bounded_session_op(…, SESSION_OP_TIMEOUT)`. Also `ApprovalStep::ChildCommitted(_) => unreachable!()` (`:254`) is a production panic held off only by the early return at `:219-224`; fold it into the match or route to `abort_plan_approval`.

### L5. Smaller behaviour issues

- `history_boundaries` (`history_edit.rs:73-87`, `322-353`) is O(n²) (`valid_boundary` rescans the prefix per offset) and holds the state lock from line 74 throughout; the picker (`rewind_picker.rs:52-58`) calls `history_boundary_message` per boundary, rerunning it. Drop the lock after the lifecycle check, compute boundaries in one pass with an incremental pending-tool-use set, and hand messages to the picker from the same snapshot.
- Edit completion is sent before the reservation is released (`history_edit.rs:186-187`, `264-265`), so an immediate `prepare_idle` can see `Busy`; tests paper over it with `runtime_barrier()` (`:807`, `:815`). Release first, then send.
- Main rewind sets `context_size = estimate_message_tokens(messages)` (`maki-ui/src/app/session.rs:378`), dropping the system/tool baseline the old code kept (`baseline = context_size - estimate(old)` plus `kept`).
- `submit_to_focused_actor` (`maki-ui/src/app/queue.rs:380-382`) drains shell results into `input.preamble` before `actor.submit`; on `Err` they are lost. Drain after admission or restore on error.
- Lua `admission_preparation` (`maki-lua/src/api/agent.rs:387`) calls `load_instructions` (sync disk I/O) on every admission, but only `.loaded` is used (`:613`); the system prompt is `build_mode_system_prompt` + `custom_system`. `lua_admission_pins_cwd_instructions_and_tool_inheritance` (`:4489-4528`) pins the unused text. Either feed instructions into the Lua prompt or pass `Instructions::default()` and drop the assertions.
- `spawn_child_with` (`maki-agent/src/manager/mod.rs:493`) uses `EffectiveAgentConfig::new(...)`, defaulting cwd to `"."`. Public via `CurrentManagedTurn::spawn_child` (`manager/types.rs:187`). Production Lua avoids it via `spawn_child_with_config(...).with_context(...)`. Carry the parent's committed cwd on `CurrentManagedTurn`.
- Removed queued turns stay joinable until retired (`actor/mod.rs:2491` `remove`, `:2526` `remove_at`, `:2573` `remove_visible_at`, precancel branch of `submit` `:2224-2227`): the lock is released before `finalize_turn` without removing the inbox. Test-only callers today; remove the inbox under the lock or delete the APIs.
- `meta_record` (`maki-storage/src/sessions.rs:803`) writes the full unbounded `posts` list into every meta line; `StorageWriter::send` (`maki-ui/src/storage_writer.rs:343`) deep-clones the whole `AppSession` on every send under the pending lock, before the `closed` check. Cap or append posts; clone only when `preserve_newer_child_histories` will adopt something.
- `drain_graph_for_test` (`manager/mod.rs:1137-1165`) borrows `MAX_RETAINED_TURN_RESULTS` as its retry bound and fails with `Err(Closed)` if an actor closes between snapshot and barrier. Own const; skip closed actors.
- Pre-existing, not from this range: `src/print.rs:554` ends print mode on any outcome event, including a subagent's (SDK guards this at `sdk_mode.rs:2013`). Skip subagent-tagged outcomes.

## Cleanup

Code reachable only from tests, or tests exercising it instead of production:

- App's `execute_host_request` branches for `Compact`, `ToggleFast`/`ToggleWorkflow`, `ChangeDirectory`, `WorkingDirectory`, `FastModeSupported` (`maki-ui/src/app/mod.rs:2601-2630`, `2726-2760`), reachable only via test-only `execute_pending_commands` since the event loop intercepts them (`event_loop.rs:2833-2975`).
- `AgentCommand::Cancel` (`maki-ui/src/agent/command_router.rs:32-45`, `agent/mod.rs:245`, `agent_loop.rs:1581`, `:1636`) is `#[cfg(test)]`; production stop is `manager.local_cancel` from `App::handle_cancel`. Move those tests to `local_cancel` and delete the variant.
- `ActorQueue::has_work_after_generation` (`queue.rs:194`) has no callers; index-based `AgentActorHandle::remove_at`/`remove_visible_at`, `ActorQueue::remove_at`/`remove_visible_at`/`is_visible`, the actor-side `QueueProjection::Message` arm; `capture_processing_presentation_cut` (`mod.rs:1539`, test alias that cancels despite the name); `TurnInbox::drain_presented_instructions` default inventing `SubmissionId(0)` (`inbox.rs:18-33`).
- `RewindPicker::open(&[Message])`, `RewindEntry::prompt_text`, `ShellState::reserve_id` are `#[cfg(test)]`; picker unit tests (`rewind_picker.rs` ~203-256) use the test `open`, not `open_actor`, and the preview formatting is duplicated. Share the preview builder and test through `open_actor`.
- Manager test labels `root_shutdown`, `root_user`, `standalone_root`, the `root_work` parameter (`manager/tests.rs` ~304, ~1115, ~1219) now just call `submit()`; rename or make them test something distinct.
- Retry state lives on `App.retry_info` for main and `Chat.retry_info` for children (`app/mod.rs:388`, `716-721`, `2314-2326`, `app/view.rs:472-476`, `chat.rs:60`), forcing `chat_idx == 0` branches. Keep it on `Chat` only.

Conventions (AGENTS.md; FQN only for single use):

- Inline paths used repeatedly: `event_loop.rs` (`maki_commands::CommandError` x15, `HostResponse` x10, `BuiltinOperation` x7, `maki_agent::AgentRef` x7, `maki_agent::actor::{ConfigChange, HistoryChange, AgentContextSnapshot}`, `std::path::Path`); `history_edit.rs` tests (`std::sync::Arc` x15, `crate::actor::AgentActorHandle` x9, `std::pin::Pin` x3; also `use std::sync::Arc;` sits after `futures_lite` at line 7); `app/tests.rs` (~52: `AgentRef`, `AgentMode`, `EffectiveAgentConfig`, `AgentManagerHandle`, `ActorBackend`, `History`, `TurnContext`); `app/mod.rs:699-705` (`AgentMode`, `ModeId`); `rewind_picker.rs:36-37,50,85` and `components/mod.rs:214,229-234` (`AgentRef`, `HistoryBoundary`); `app/queue.rs` (`AgentId`); `approval.rs:35,83,93` (`AgentRef`, `AgentId`, `EffectiveAgentConfig`); `agent_loop.rs` tests (`TurnAdmissionOptions`); `maki-lua/src/api/agent.rs` (`ModeCeiling` at `:1826-1848`, `:2702`; `AgentContextSnapshot` `:1870`, `:4512`; `crate::runtime::Request` `:191`); `ui/mod.rs` (`mlua::UserDataRef`); `handles/mod.rs` tests (`test_case::test_case`, `crate::agent_errors::*`, `crate::api::ui::create_ui_table`); `actor/queue.rs:131-135`, `234-246` (`std::borrow::Cow`, `super::PendingQueueEntry`); `inbox.rs` (`super::TurnTicket` though imported at line 8, `maki_providers::ImageSource`, `super::EffectiveAgentConfig`, `crate::BatchKey`); `chat.rs:60` (`crate::components::RetryInfo`).
- Consts mid-file: `POST_OPTIONS` (`maki-lua/src/api/ui/mod.rs:725`).
- Inline strings: "child history storage identity is unavailable" (M12); "unsupported agent option: {id}" (`event_loop.rs`); `session.rs:325`, `:340` flash/error strings (sibling `FLASH_*` consts exist); `run_compact` strings (`agent.rs:338-347`); "show must be a boolean or string…"; "unknown task_id" in `runtime_alignment.rs:800` (`UNKNOWN_TASK_ERR` exists in task_policy); `"<user-interrupt>\n"` in `task_policy.rs:1232`; "checkpoint unavailable" (`history_edit.rs:790`, test matches `HistoryCheckpoint(_)` instead of a const); `"sibling"` repeated in `storage_writer` tests; `Duration::from_secs(5)` in a `ui/mod.rs` test (`:1243`).
- `task_send_after_{done,cancel,provider_failure}` are three `#[test]` wrappers; use `#[test_case]`.
- Duplication in `actor/mod.rs`: the outstanding-bounds fold three times (`2105-2112`, `2176-2183`, `2247-2251`), `PendingSubmission` built four times (`2138`, `2209`, `2287`, `819`).
- `maki-domain/src/post.rs`: `use super::*;` in tests (line 42); `POST_LEVELS` repeats the `parse` match (could derive via strum `VariantNames`, already used in maki-lua).
- `pinned_editor_path` (`event_loop.rs:3509`) was inserted under `open_editor`'s doc comment, which now describes the wrong function; the pinned `UiAction::OpenEditor` failure path returns -1 without the flash that comment promises.

## Checked and fine

No need to re-review these: approval targeting and stale `PlanApprovalReady` filtering, child approvals across `settle_plan_approval`, key blocking during approvals, the `waiting_for_lock` resume; agent-loop setup failure sealing; respawn ordering in `apply_to_app`; foreign prepared ops refused by `belongs_to`; rewind agent/epoch checks; mode ceiling on both config paths; checkpoint failure/panic leaving history and config unchanged; deferred terminal outcomes for both backends; outcome immutability across continuations; presentation failure keeping drained instructions in history; `local_cancel` lock order and nonce-guarded `active_turns` removal; headless/SDK/ACP unaffected (`Post` ignored by design); `show` validation and defaults, `ui.post` option validation, spawn-time post routing from Lua-child parents; removal of `spawn_ui_input_relay`; child cwd inheritance for TUI roots; `checkpoint_child_history` stale-epoch handling; d956a5b1's typed-text, shell, cancel and rewind guards; `just gen-docs-check` passes; no em-dashes in added docs; no trivial comments in the diff.

`code_review.lua` (user dotfiles, not in this repo) was also checked: the command ctx stays valid across `ticket:wait`, a wait outside a turn does not withdraw `auto_deliver`, and every termination path posts a row or notifies.

## Implementation verification, 2026-10-18

The first verification pass treated H1-H4, M5-M12, L1-L5, and the cleanup list as complete. A subsequent independent audit found that L4 still retained a committing approval after its deadline. The audit also found the unsupported-agent-option error prefix and repeated sibling test identity still inline. The remaining audit corrections below resolve these findings with new deterministic coverage, frozen-source review, and cumulative checks. The earlier runtime plan contains an assertion-level AC.1-AC.14 map. That map records the remaining limits of the original broad interaction/backend matrices rather than treating matching test names as proof.

Deterministic regressions reproduced the actor close/shutdown completion cut, history-edit response-before-idle race, divergent checkpoint false acknowledgement, and acknowledged in-flight history rollback before fixes. Storage re-review exposed metadata-candidate and rebased-history interleavings; both also failed before their fixes. Real Lua-provider tests exposed child cwd inheritance from a later queued configuration. Registered command/keybind tests exposed absent-admission mode calls adopting later focus. Those assertions pass after the corresponding source changes.

Independent targeted re-reviews approve the earlier actor cancellation/history scope, storage checkpoint ownership/promotion, and Lua mode admission fixes. The proposed physical-terminal-frame gate is not required: M9 requires history and prompt installation before child output extends chat state. The held prepared-commit guard enforces that ordering. The reviewer accepted this clarification and withdrew the frame-gating finding. The first implementation retains the definitive checkpoint observer after its deadline. That avoids falsely treating a dropped storage waiter as cancellation, but does not satisfy L4 because the approval remains blocked. The subsequent audit requires cancellation shared by the UI, actor edit, and final storage publication.

Executed checks on the cumulative implementation:

- `OPENSSL_NO_VENDOR=1 NEXTEST_TEST_THREADS=4 just ci`: passed, 7,474 tests passed with none skipped. This includes Rust/Lua formatting, Clippy for tests and benches with warnings denied, Python checks, generated references, and unused-dependency checks.
- `cargo nextest run -p maki-lua --test runtime_alignment`: 17 passed, none skipped, including the strict child cwd inheritance case.
- Scoped actor/manager tests: 280 and 103 passed. Writer tests: 53 passed. Scoped EventLoop tests: 183 passed. These runs preceded the cumulative run above.
- `OPENSSL_NO_VENDOR=1 just build`: passed.
- `just gen-docs`: regenerated references; the cumulative generated-doc check passed.
- The prefixed docs site build completed with Zola 0.22.1.
- Nix built the full package from a source-only snapshot containing tracked changes and new source files. The snapshot's `git-dep-hashes` and `fmt` checks passed.
- `git diff --check`: passed.

The checkout has no `AGENTS.local.md` or `.ssh/remote-ci.sh`, so verification ran locally. Windows CI was not executed on this Linux host. No ready native Windows runner was identified. Cross-compilation and Wine tooling do not reproduce the workflow's native Windows filesystem checks. The operator explicitly permits completion without Windows verification and requires the existing CI errors to be fixed. Earlier suppressed permission prompts did not supply that consent. No push is authorized. Commit hooks remain enabled; intermediate commits run their full checks in a clean checkout to exclude unfinished changes. The first snapshot attempt included changing build artifacts and failed before evaluation; the source-only retry passed.

The working branch contains these implementation commits:

- `f9591eb3`: record the review follow-up plan.
- `9005db9a`: derive post levels from their enum variants.
- `d94e9f66`: ignore child outcomes in print mode.
- `e8e48529`: persist posts incrementally and prune orphaned chat state.
- `3fdb665e`: make actor cancellation and history edits settle atomically.
- `d50e3834`: preserve durable history across coalesced checkpoint writes.
- `c3969c3e`: keep focused controls and Lua work on their admitted agents. Its hooks passed all 7,474 tests with none skipped, plus the full local CI checks.
- `18f571a4`: remove the redundant actor projection closure borrow reported by CI.
- `ac2337b7`: isolate bulk highlighting from script boundary tests. Both CI-fix commits' hooks pass all 7,475 tests with none skipped on Rust 1.99.

The operator waives native Windows verification. The implementation commits above include the generated references.

### Existing CI failures

The PR's failed Rust workflow runs, `39086633915` and `39086629937`, test `d956a5b1`, before these local commits. Both Linux and Windows lint report `needless_borrows_for_generic_args` for the same actor closure argument. CI uses Rust 1.99; the earlier local checks use Rust 1.95. Passing the immutable, copyable closure by value removes the redundant borrow. Workspace lint and the native Linux build pass on Rust 1.99 after that fix.

The failed test run reports `code_execution_policy::restore_expanded_shows_full_script_beyond_cap` losing its snapshot sender after 61.4 seconds. The preview boundary cases also exceed 60 seconds in that run. Restore waits for syntax highlighting before returning its buffer and has a 60-second execution deadline. The unchanged cases pass locally in 1.1 to 1.4 seconds, so the CI slowdown is not locally reproduced. The test correction retains the production plugin, its 2,000-line cap, and the 2,000/2,001-line fixtures. A test-local highlighter supplies deterministic styled lines only for the large boundary cases. A separate small restore test checks the real syntax highlighter and colored output. No production deadline, timeout, or retry changes. Removing click replay still makes the expanded-restore assertion fail. All 1,710 Lua tests pass on Rust 1.99 with four test threads after the correction.

Independent review approves both CI corrections. `OPENSSL_NO_VENDOR=1 RUSTUP_TOOLCHAIN=1.99.0 just ci` passes at `6a7be613` with the repository's default 64 test threads: 7,475 tests pass with none skipped, plus formatting, workspace Clippy, Python checks, generated references, and unused-dependency checks. The prefixed documentation site build also passes after the final wording correction. Nix rebuilds that source-only snapshot's package and passes its hash and format checks. No current-state hosted CI result is claimed because these commits have not been pushed. Native Windows remains explicitly unverified under the operator's waiver.

### Remaining audit corrections

The unsupported-agent-option prefix and storage-writer sibling test identity now use constants beside their respective error and fixture constants.

Child approval cancellation uses one publication token across the UI, actor edit, and storage writer. Deadline, Esc, detected lock loss, runtime replacement, and session deletion cancel a request before publication wins. A queued or executing actor edit fails the prepared operation and releases its idle reservation without waiting for a blocked checkpoint writer. A cancelled request leaves history, configuration, and the implementation turn unpublished.

Approval checkpoints use an isolated queue rather than the ordinary provisional-save state. The writer builds from authoritative session state, serializes and syncs a temporary file, validates session-lock ownership, then checks runtime identity and claims publication immediately before file replacement. Successful publication updates the selected child's history and posts in authoritative, provisional, and queued snapshots without replacing root, sibling, or newer metadata changes. It also invalidates the cached append cursor. Cancelled or failed candidates cannot be retried by ordinary saves. Once publication wins, deadline or Esc observes the actual result; the synchronous replacement and directory-sync tail cannot be interrupted safely. The held actor guard still prevents implementation output until the UI installs history and the visible prompt.

The zero-deadline UI regression failed before the fix because `pending_approval` remained present while the checkpoint was gated. Deterministic cases now cover deadline and Esc cancellation, lock loss, cancellation before writer dequeue and actor execution, permanently gated callbacks, simultaneous checkpoint success and cancellation, publication-owned success and failure, runtime replacement during staging, post pruning, and a failed newer-root save followed by child approval and retry. The no-clear-context lock-loss case combines a UI cancellation-wiring assertion with the actor's queued-before-runner cancellation test; it does not add a cross-crate runner gate.

Scoped Rust 1.99 verification passes: `cargo check -p maki-storage -p maki-agent -p maki-ui --tests`, Clippy for those crates' tests and benches with warnings denied, and nextest with 3,933 tests passed and none skipped. Formatting and whitespace checks pass. These results supersede the interrupted first implementation worker and its transient compile/test failures. Independent frozen-source review approves the fix with no remaining findings. It confirms that the earlier lock-loss finding is resolved and supersedes its request-changes verdict.

Cumulative `OPENSSL_NO_VENDOR=1 RUSTUP_TOOLCHAIN=1.99.0 just ci` passes with default test concurrency: 7,499 tests pass with none skipped. Formatting, workspace Clippy, Python checks, generated references, and unused-dependency checks also pass. `just build` passes on the same toolchain. Nix builds the corrected source-only snapshot's full package and passes `git-dep-hashes` and format checks. Language-server diagnostics report no issues in the actor edit or approval source; storage has only an inactive Windows-code hint. Native Windows remains unverified under the operator's waiver. Hosted CI is not claimed and no push is performed.

The constant cleanup is committed as `fa805837`. Its enabled hooks pass all 7,475 tests from the pre-timeout-fix snapshot with none skipped and the full local CI checks. An initial partial-staging placement error stopped that commit's formatting hook before any commit was created; rebuilding the staged cleanup from the base files corrected it.
