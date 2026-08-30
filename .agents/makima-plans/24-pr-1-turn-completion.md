# Goal

Implement PR 1, **Reliable turn completion**, for issue #24: give every accepted agent turn stable agent/turn identity and one typed terminal outcome, and guarantee that completion, failure, and cancellation terminalize the turn exactly once. Fix #54 and #55 while preserving the current TUI, headless, ACP, and `maki.agent.Session` compatibility surfaces.

# Implementation Summary

Add the foundational lifecycle contract to `maki-agent` and make it authoritative at `Agent::run`. Introduce nominal `AgentId` and `TurnId` newtypes backed by the existing UUIDv7/base58 `maki_storage::id::MakiId`, plus a cloneable/serializable `TurnOutcome` with explicit completed, failed, and cancelled variants. A serializable `TurnFailure` will snapshot a stable failure kind, full diagnostic text, user-facing text, and retryability from `maki_providers::AgentError`.

`Agent::run` will keep using `Result` internally where it is idiomatic, but its public boundary will normalize every exit into `TurnOutcome`, make exactly one terminal outcome emission attempt, and return the same value even if event delivery fails. Existing callers will stop synthesizing separate success/error terminal events and instead adapt the shared outcome into their current UI/protocol behavior. The Lua subagent driver will consume the returned outcome directly rather than waiting for a success-only `Done` usage event, and ordinary failed turns will leave the compatibility session live for queued or later turns.

Primary touch points:

- `maki-agent/src/types.rs`, `maki-agent/src/lib.rs`, and possibly a focused lifecycle module under `maki-agent/src/`: IDs, failure snapshot, outcome, terminal event, exports, and unit tests.
- `maki-agent/src/agent/run.rs`: authoritative normalization and exactly-once terminal emission.
- `maki-ui/src/agent/agent_loop.rs`: root-agent compatibility adaptation and stable root/turn identity assignment.
- `maki-agent/src/headless.rs`: one-shot and interactive headless adaptation.
- `maki-lua/src/api/agent.rs`: subagent identity, per-input turn identity, outcome-driven completion/usage/error delivery, queue/cancellation cleanup, and reusable post-failure sessions.
- `maki-lua/tests/subagent_run_end.rs` and focused in-module tests: #54/#55 and exactly-once regressions.
- `maki-acp/src/server.rs` / `maki-acp/src/translate.rs` only as required to preserve the current wire behavior after the terminal event changes.

Scope boundaries:

- Do not introduce the persistent shared actor from PR 2, the manager/graph from PR 3, graph persistence, first-class Lua `Agent` userdata, waits/notifications, guest presets/modes, or UI graph navigation.
- Do not persist agent/turn identities yet. The types and event contract must be persistence-ready, but graph persistence belongs to a later PR.
- Do not continue using tool-use IDs or UI `run_id` as agent identity. Keep them only as temporary compatibility correlation fields.
- Do not redesign provider errors. Snapshot them into the lifecycle contract at the `maki-agent` boundary.

# Implementation Plan

## Phase 1: Define the lifecycle types and invariants in `maki-agent`

1. Add nominal `AgentId` and `TurnId` types around `MakiId`.
   - Derive/implement `Clone`, `Copy`, `Debug`, `Eq`, `Hash`, serde, `Display`, and `FromStr` consistently with `MakiId`.
   - Provide explicit `generate()` constructors and only deliberate conversions/accessors, so agent IDs and turn IDs cannot be interchanged accidentally.
   - Keep the current compact canonical base58 encoding and UUIDv7 generation.

2. Replace the split terminal model with one public lifecycle outcome.
   - Keep `DoneReason` for successful stop detail, but remove cancellation from the success reason space once callers are migrated; cancellation must be a top-level outcome variant rather than `Completed(Cancelled)`.
   - Define a `TurnOutcome` carrying `agent_id`, `turn_id`, accumulated `TokenUsage`, model-turn count, and exactly one terminal state:
     - completed with `DoneReason` (`EndTurn`, `MaxTokens`, or `MaxTurns`);
     - failed with `TurnFailure`;
     - cancelled with an explicit cancellation reason/kind suitable for future queued-turn, user, shutdown, and interruption distinctions without requiring those later systems now.
   - Prefer a serde-tagged enum or a common metadata struct plus tagged terminal state, whichever keeps exhaustive matching and serialized records clear.

3. Define `TurnFailure` as the durable/public snapshot of an `AgentError`.
   - Include a stable coarse `TurnFailureKind` sufficient to distinguish at least provider/API, authentication, timeout, tool, I/O/transport, invalid response/config, channel/internal, and compaction failures. Do not expose provider error enums directly as the lifecycle contract.
   - Store the full diagnostic string, `user_message()`, and `is_retryable()` result.
   - Convert once from `&AgentError`; do not reconstruct failure details from display events.
   - Use this exhaustive stable mapping for the current provider error taxonomy:
     - `Api { status: 401 }` → authentication;
     - every other `Api` status → provider/API;
     - `Timeout` → timeout;
     - `Tool` → tool;
     - `Io`, `Http`, and `HttpRequest` → I/O/transport;
     - `Json` and `Config` → invalid response/config;
     - `Channel` → channel/internal;
     - `EmptySummary` → compaction.
   - Treat `AgentError::Cancelled` as cancellation, never failure. Keep the conversion match exhaustive so adding a provider error variant requires an explicit lifecycle classification.

4. Replace `AgentEvent::Done` and `AgentEvent::Error` as independent terminal signals with one `AgentEvent::TurnOutcome` (or equivalently named single terminal variant) carrying the complete outcome.
   - Preserve non-terminal streaming events, including `TurnComplete`, unchanged.
   - Keep compatibility translation at consumers, not duplicate terminal events in the producer.
   - Document the invariant in the type/API docs: an accepted turn determines one and only one terminal outcome, makes one correlated emission attempt, and returns that same outcome regardless of delivery success.

## Phase 2: Make `Agent::run` the authoritative terminalization boundary

1. Pass stable identity into each run without conflating it with `AgentInput` content.
   - Give an `Agent` instance an `AgentId` through construction/configuration.
   - Require a `TurnId` for each accepted invocation of `run`, either through a small `AgentTurn`/run request wrapper or an explicit run argument. Reserve the ID before admission in each current coordinator.
   - Keep root-agent identity stable across turns in one outer session and subagent identity stable across all messages to one compatibility session.

2. Change `Agent::run` to return `TurnOutcome` rather than `Result<DoneReason, AgentError>`.
   - Keep `run_loop`, streaming, tool dispatch, compaction, and other internal operations fallible.
   - At the single outer boundary, normalize:
     - normal stop into completed;
     - cooperative cancellation, including `AgentError::Cancelled`, into cancelled after current history sanitation;
     - every other error into failed using `TurnFailure`.
   - Reset invocation-scoped state at admission before mutating history: `total_usage`, `num_turns`, reauthentication attempts, rollback boundary, and any other field whose budget or metadata belongs to one `TurnId`. Preserve conversation-scoped state such as history, model/context information needed by the next request, loaded tools, and agent identity.
   - Capture usage and turn count on all paths, including failures before the first provider response and failures after partial progress. Values in each outcome are local to that accepted turn, never cumulative across repeated `run` calls on the same `Agent`.
   - Emit the terminal outcome once from one code path after normalization, then return that same value.
   - If sending the terminal event fails, retain and return the already-determined outcome rather than converting it into a second lifecycle result or attempting another terminal event. Log the delivery failure with agent/turn IDs.

3. Remove caller-owned terminal synthesis after `Agent::run`.
   - Delete post-run `AgentEvent::Error` emission in the TUI and headless loops.
   - Update cancellation checks to match the cancelled outcome.
   - Ensure terminal logging includes `agent_id`, `turn_id`, terminal state, retryability where applicable, usage, and model-turn count.

4. Keep pre-admission/setup failures distinct, with one explicit admission point per coordinator.
   - A queued input becomes accepted when its coordinator removes/reserves it for execution and assigns its `TurnId`; after that point every exit, including model/provider replacement or other setup, must produce the correlated shared outcome.
   - Startup failures that occur before any input exists or before an input is removed from its queue may retain the current frontend error path in PR 1.
   - In interactive headless, the current model replacement happens after dequeue. Either move replacement/provider construction before dequeue without consuming the input, or keep the current ordering and route replacement failure through a small terminalization helper that produces `TurnOutcome::Failed` with the already-reserved IDs. Do not advance `run_id` while silently discarding an accepted input.
   - Audit root, one-shot headless, interactive headless, and Lua admission paths against this definition.

5. Remove standalone manual compaction from the turn-terminal event contract.
   - `agent::compact` is a control operation, not an accepted agent turn. Replace its current `AgentEvent::Done` emission with a dedicated non-turn operation result carrying success usage or failure detail where event-driven consumers require it, or return `Result<TokenUsage, AgentError>` to its control caller and let that caller acknowledge success/failure through a non-turn adapter.
   - Keep in-turn `AutoCompacting`/`CompactionDone` progress semantics separate from standalone control completion. A failed manual compaction must visibly restore/settle frontend state and report the control error without emitting `TurnOutcome`.
   - Ensure only `AgentEvent::TurnOutcome` terminalizes an accepted agent turn; standalone compaction must never mint a fake `TurnId` or affect exactly-once turn counts.
   - Update TUI and interactive headless control handling plus any print/SDK consumers that relied on standalone `Done`.

## Phase 3: Adapt current root/headless/ACP behavior without doing PR 2

1. In `maki-ui/src/agent/agent_loop.rs`, allocate one stable root `AgentId` when the existing loop/session coordinator is created and allocate a `TurnId` when each queue item is accepted for execution.
   - Preserve numeric `run_id` only for stale-render filtering and existing UI correlation.
   - Consume the authoritative outcome for cancellation bookkeeping and return an adapter-level success/error only where the existing loop requires it; do not emit another terminal event.

2. Update `maki-ui` event handling to consume the single outcome event.
   - Completed renders and exits as current `Done` behavior.
   - Failed uses `TurnFailure.user_message` for the current status/error UI while retaining full diagnostic data in the event.
   - Cancelled preserves current cancelled-history and stale-event behavior.
   - Ensure `--exit-on-done` and queued-message progression retain their current semantics for all three variants.

3. Update one-shot and interactive headless paths in `maki-agent/src/headless.rs`.
   - Allocate stable agent identity per headless outer session and a turn identity per accepted input.
   - Remove duplicate error-event synthesis and map the single outcome to existing process/API behavior.
   - Preserve current provider/model setup failures that occur before turn acceptance.

4. Update ACP event translation only as needed to preserve protocol behavior.
   - Completed maps through the existing `DoneReason` translation.
   - Cancelled maps to ACP cancelled.
   - Failed completes the pending prompt with the current structured ACP error using the user-facing failure message/full data as appropriate.
   - Avoid adding graph navigation or new ACP extensions in this PR.

## Phase 4: Fix the compatibility subagent driver and #54/#55

Replace the current branch-heavy `subagent_driver` accounting with a small per-input lifecycle implementation while retaining the surrounding `maki.agent.Session` API. Use one admission function that atomically checks closure, reserves `TurnId`, increments pending, and enqueues, and one idempotent finalizer that records/delivers the outcome, resolves any waiter, updates status, and decrements pending exactly once. Route normal completion, failure, cancellation while waiting for a permit, cancellation after a run, channel closure, and queue draining through that finalizer rather than adding more early-exit branches.

1. Give every `LuaSession`/`SubagentDriver` a generated `AgentId` independent of `ui_id`/parent tool-use ID.
   - Keep `ui_id` solely for existing task-row, history, and cancellation adapters.
   - Add `TurnId` to `TurnInput` and reserve it while holding the existing admission lock, before incrementing pending state and sending to the queue.
   - Carry the ID through blocking `prompt`, non-blocking `send`, and UI-injected input.

2. Remove the success-only usage barrier in `run_agent_inner`.
   - Use usage from the returned `TurnOutcome`; eliminate `usage_tx`/`usage_rx` as the correctness path.
   - If a lightweight relay ordering acknowledgement is still required to flush live usage before a tool returns, make it acknowledge every terminal outcome rather than only success, and do not make turn completion depend indefinitely on a lossy event channel. Prefer draining/ordering based on the returned outcome and channel closure where practical.
   - Let the relay forward or translate the typed failure instead of discarding `AgentEvent::Error`; there should no longer be a separate error terminal event to drop.
   - Preserve `silent = true`: silence suppresses all parent/UI envelopes and automatic parent notification, including the stamped terminal outcome, but never suppresses driver finalization. `run_agent_inner` must complete from the directly returned outcome, with correct waiter/status/error/usage behavior independent of relay delivery.

3. Make `SubagentRunResult` an adapter over `TurnOutcome`, not a second source of lifecycle truth.
   - Preserve the current Lua pair convention for `prompt`: success returns the result; failure/cancellation returns `(partial_result?, err)`.
   - Retain the exact per-turn outcome internally for identity, terminal state, failure, and outcome-local usage. Maintain a separate session usage aggregate only for existing Lua `input_tokens`/`output_tokens` compatibility fields, which currently report accumulated session usage.
   - Define shared-slot status deterministically: while any accepted turn is running or queued, `status()` reports `running`; after pending reaches zero it reports the most recently finalized turn as `done`, including its compatibility result/error and session aggregate usage; after closure it reports `closed`.
   - Include/retain the typed outcome internally so future PRs can expose it without re-deriving information.

4. Establish the chosen failure semantics: ordinary failure ends only its turn.
   - After a failed turn, update status with that failure, reply to any blocking waiter, deliver the failed history/outcome to the parent for non-silent sessions, decrement pending exactly once, and continue the driver loop.
   - Process already queued and later turns in FIFO order after failure.
   - Keep `retryable` as metadata about the failed operation; it does not control whether the agent accepts another turn.
   - Only explicit close/despawn/global cancellation or an unrecoverable driver shutdown closes the compatibility session.

5. Terminalize outstanding accepted inputs on closure/cancellation.
   - When cancellation wins while waiting for input, a permit, or an active run, produce a cancelled outcome for the affected accepted turn.
   - Before driver shutdown, drain accepted queued `TurnInput`s and resolve their blocking reply channels as cancelled/closed rather than dropping senders and returning an uncorrelated generic closure.
   - Keep pending counters/status consistent and ensure admission rejects new turns after closure begins.
   - Preserve the existing rule that a background subagent outlives the parent turn that spawned it.

6. Deliver useful failure detail through one typed compatibility path without polluting provider history.
   - For non-silent sessions, relay the child `AgentEvent::TurnOutcome` to the parent with the existing `Envelope.subagent` correlation, whose `parent_tool_use_id` remains the compatibility `ui_id`. The child outcome retains `AgentId`/`TurnId`; `ui_id` is used only to find the existing task row/chat.
   - In the TUI parent adapter, distinguish subagent-stamped outcomes from root outcomes before generic chat terminal handling. A child failed outcome marks only the correlated task row/chat failed, renders a bounded `TurnFailure.user_message` summary in the row, and stores/renders the full diagnostic as a typed terminal observation in the opened child view.
   - Deliver an automatic structured failure notification to the parent queue using the same subagent header/correlation path currently used for successful `SubagentHistory`, but derive it from `TurnFailure` rather than fabricating an assistant message. Keep presentation/notification data out of `SubagentDriver.history`, so the next provider request sees only genuine conversation history.
   - Keep `SubagentHistory` as the transcript compatibility payload and successful-reply source. Pair it with the typed child outcome for terminal state; do not infer failure from missing assistant text and do not let `SubagentHistory` unconditionally overwrite a prior failed/cancelled marker with `Done`.
   - Keep `task_get`/Session status programmatically exposing the error and retryability through compatibility data where feasible without changing the documented Lua result shape incompatibly.

## Phase 5: Verification and compatibility cleanup

1. Update all exhaustive matches and tests for the single terminal event and removal of cancellation from `DoneReason`.
2. Search production code to prove no caller emits a second post-`Agent::run` terminal error and no subagent completion waits only for a success event.
3. Keep deprecated/current public names (`maki.agent.Session`, task tools, `SubagentHistory`, numeric UI `run_id`) working as adapters; do not add the first-class Lua agent API yet.
4. Run formatting and crate-scoped checks/tests first, then the repository CI workflow on the remote build box as required by `AGENTS.local.md`.

# Acceptance Criteria

- **AC.1:** `AgentId` and `TurnId` are distinct UUIDv7/base58-backed types; generated values round-trip through display/parse and serde, and the Rust type system prevents accidental interchange.
- **AC.2:** Every turn accepted by `Agent::run` determines and returns exactly one correlated terminal `TurnOutcome` containing matching agent/turn IDs, usage, and model-turn count, and makes exactly one terminal emission attempt. When the event channel accepts it, exactly one matching event is observed; when delivery fails, the returned outcome remains authoritative and no retry or second outcome is produced.
- **AC.3:** A provider/agent-loop failure before any successful response produces a failed outcome without waiting for a success-only event, and the failure preserves kind, diagnostic message, user-facing message, and retryability.
- **AC.4:** Cancellation is represented as `TurnOutcome::Cancelled`, not a successful `DoneReason` and not a failure, while preserving current cancellation history sanitation.
- **AC.5:** Current TUI, headless, and ACP consumers render/map completed, failed, and cancelled outcomes once, with no duplicate terminal event synthesized after `Agent::run`.
- **AC.6:** An asynchronous compatibility subagent failure reaches a terminal status, resolves a blocking waiter, surfaces the underlying cause to the parent/task UI, and never blocks waiting for usage or completion that cannot arrive.
- **AC.7:** After an ordinary failed subagent turn, the same compatibility session accepts and completes a subsequent turn; queued turns continue in FIFO order and queue admission does not falsely report a dead driver as live.
- **AC.8:** Explicit cancellation/closure terminalizes affected accepted active and queued subagent turns once, resolves their waiters, leaves pending/status accounting consistent, and rejects new admission after closure begins.
- **AC.9:** Existing successful Session/task behavior, partial text on failure/cancellation, structured captured output, usage reporting, and the rule that a subagent outlives its spawning parent turn remain functional through compatibility adapters.
- **AC.10:** Scoped Rust checks, lints, tests, generated-doc checks affected by Lua API annotations, and the project CI workflow pass.

# Test Strategy

| Acceptance criterion | Named tests/checks |
|---|---|
| AC.1 | `agent_id_roundtrips`, `turn_id_roundtrips`, serde canonical-base58 tests, and a `compile_fail` Rustdoc example on the nominal ID API showing that an `AgentId` cannot be passed where `TurnId` is required. Do not add `trybuild` unless Rustdoc cannot express the check. |
| AC.2 | `run_emits_same_completed_outcome_once`, `run_emits_same_failed_outcome_once`, `run_emits_same_cancelled_outcome_once`, `reused_agent_reports_per_turn_usage_and_turn_count`, and `reused_agent_fails_then_succeeds` in `maki-agent/src/agent/run.rs`; each terminal-event test drains the channel and asserts one event with exact IDs/metadata. |
| AC.3 | `provider_failure_terminalizes_with_failure_metadata` using the existing stub stream provider plus table-driven `agent_error_snapshots_to_stable_failure_kind`, covering every current `AgentError` variant, the 401 authentication split, retryability, diagnostic, and user-facing message. No timer/sleep dependency. |
| AC.4 | Update existing cancellation tests (`cancel_token_aborts_during_api_call`, `cancel_mid_stream_keeps_partial_text_in_history`, `cancel_during_retry_backoff_discards_failed_attempt_text`) to assert the cancelled outcome and exactly one terminal event. |
| AC.5 | Update/add TUI event-loop/app tests `completed_outcome_finishes_once`, `failed_outcome_renders_failure_once`, and `cancelled_outcome_preserves_stale_run_filter`; ACP translation tests for all outcome variants; headless `interactive_model_replacement_failure_terminalizes_accepted_turn`; plus `standalone_compaction_completes_without_turn_outcome` and `standalone_compaction_failure_reports_control_error_without_turn_outcome` across the TUI and interactive-headless control harnesses. |
| AC.6 | Add Lua integration test `failed_subagent_turn_reports_cause_and_finishes` in `maki-lua/tests/subagent_run_end.rs` for deterministic provider failure, waiter resolution, status, typed child outcome, and parent notification. Add `maki-ui` app test `failed_subagent_outcome_shows_bounded_task_summary_and_full_detail` that feeds the subagent-stamped outcome and asserts the task row/opened child view without terminalizing the root. Add `failure_presentation_does_not_enter_next_provider_history` at the Lua driver/provider boundary. |
| AC.7 | Add `subagent_processes_queued_turn_after_failure` and `subagent_accepts_later_turn_after_failure`; use a scripted provider that fails then succeeds and assert exact `TurnId` order/outcomes and truthful admission. Add `status_remains_running_until_all_queued_turns_finalize` and assert outcome-local usage versus compatibility session aggregate usage. |
| AC.8 | Add in-module driver tests `cancel_while_waiting_for_permit_resolves_turn`, `close_resolves_active_and_queued_waiters`, `close_rejects_new_turn`, and `pending_count_reaches_zero_after_shutdown`, coordinated with channels/events and no sleeps. |
| AC.9 | Keep/update `subagent_outlives_the_run_that_spawned_it` and `subagent_completion_surfaces_reply_to_parent`; add named adapter tests `failed_prompt_returns_partial_text`, `successful_prompt_preserves_captured_output`, `session_status_reports_compatible_usage`, and `silent_failed_prompt_finalizes_without_parent_events`. Replace current polling/sleep loops in lifecycle regressions with deterministic completion channels while touching this harness. |
| AC.10 | Run `cargo check -p maki-agent --tests`, `cargo nextest run -p maki-agent`, `cargo check -p maki-lua --tests`, `cargo nextest run -p maki-lua`, relevant `maki-ui`/`maki-acp` tests, `just fmt-check`, `just gen-docs-check` if generated Lua docs change, then `.ssh/remote-ci.sh` for the full remote `just ci`. |

The exactly-once tests must count terminal events, compare the returned and emitted values, and cover event-channel send failure. Cancellation/queue tests must use deterministic channels or event listeners rather than elapsed-time polling.

# Review Strategy

Before handoff, use `plan-reviewer` to verify contract completeness, PR 1 scope, and acceptance-test mapping; resolve or explicitly rebut all critical/high findings and re-review if necessary.

After implementation and automated checks, dispatch a `general-purpose` review agent focused on:

- lifecycle exhaustiveness and exactly-once terminalization across error/cancellation races;
- truthful admission and waiter resolution in the compatibility subagent driver;
- accidental duplicate terminal events in TUI/headless/ACP adapters;
- preservation of compatibility behavior and avoidance of PR 2/3 architecture leaking into PR 1.

Fix or explicitly rebut every finding. If any critical finding remains after fixes, run another review pass before presenting the PR.

# Documentation Strategy

Update Rust API documentation on the new IDs, failure snapshot, outcome, and terminal event invariant. Update generated Lua API annotations only where the compatibility Session status/error behavior becomes more explicit, then regenerate/check docs through the existing docgen workflow.

Do not write user-facing first-class agent API or mode documentation in PR 1 because those APIs do not exist yet. Add a concise developer-facing note to the issue/PR description explaining that tool-use IDs and numeric `run_id` remain compatibility correlation only, while `AgentId`/`TurnId` are the future runtime identities.

# Risks, Blockers, and Required Decisions

- **Event send failure:** `EventSender` is currently fallible. The returned outcome must remain authoritative even if event delivery fails; retrying terminal emission could violate exactly-once semantics. Tests must cover this explicitly.
- **Meaning of “exactly once”:** PR 1 can guarantee one producer terminalization attempt and one retained/returned outcome. Durable subscriber delivery and replay are PR 2/PR 6 concerns; this PR must not claim crash-durable exactly-once delivery.
- **Failure detail in history:** Appending a failure observation to subagent history must not make that synthetic message look like model output or contaminate later provider context incorrectly. Prefer a clearly typed observation/current event-to-view adapter and test the next turn’s history.
- **Cancellation races:** The current driver can break before replying or decrementing pending state. Centralize per-input finalization so active cancellation, permit-wait cancellation, explicit close, dropped channels, and normal completion all resolve the input once.
- **Compatibility event churn:** Replacing `Done`/`Error` affects many exhaustive matches. Keep the change mechanical at consumers and resist adding manager/graph abstractions before PR 2/3.
- **Identity persistence:** IDs are generated and propagated now but intentionally not added to the session storage format in PR 1. Stable means stable for the lifetime of the current runtime object; restart durability arrives with graph persistence.
- **Settled operator decisions:** `TurnOutcome` is authoritative at `Agent::run`; ordinary failures end the turn but leave the agent/session reusable; `AgentId` and `TurnId` are nominal `MakiId`-backed UUIDv7 types.

# Post-Implementation Record

This section records what happened while executing the approved plan. The sections above are retained as the starting context and intended design; this section describes the resulting implementation, review discoveries, verification evidence, and remaining boundaries.

## Result

PR 1 was implemented across the core agent lifecycle and all current compatibility consumers. The implementation commit is `486280d7` (`feat(agent): make turn completion reliable`).

The resulting contract is:

- `AgentId` and `TurnId` are distinct nominal wrappers around `MakiId`, with UUIDv7 generation and canonical base58 display, parse, and serde behavior.
- `Agent::run` accepts a reserved `TurnId`, returns a typed `TurnOutcome`, and makes one terminal event delivery attempt from its outer boundary.
- Completed, failed, and cancelled are disjoint terminal states. Cancellation is no longer represented by `DoneReason` or failure.
- `TurnFailure` snapshots stable kind, diagnostic text, user-facing text, and retryability from `AgentError`.
- Outcome usage and model-turn counts are invocation-local even when an `Agent` is reused.
- A failed terminal-event send does not change or retry the authoritative returned outcome.
- Standalone compaction uses `ControlComplete`/`ControlError`; it does not mint a turn or trigger turn-end behavior.

## Implementation Differences and Additional Touch Points

The plan named the main files correctly, but execution exposed additional compatibility surfaces that also required migration:

- `src/print.rs` and `src/sdk_mode.rs` still matched the removed `AgentEvent::Done`/`Error` variants. They now adapt `TurnOutcome` and control-operation events explicitly.
- `maki-ui/src/chat.rs`, `maki-ui/src/event_loop.rs`, `maki-ui/src/app/queue.rs`, and `maki-ui/src/app/tests.rs` required changes to keep turn completion, queue progression, exit behavior, and manual compaction separate.
- `maki-acp/src/server.rs` needed more than mechanical event translation: pending prompt state must be registered before agent input is sent, with rollback on send failure, so a fast terminal outcome cannot race past registration.
- Post-admission model/provider setup failures existed in both interactive headless and the TUI loop. Each path now reserves a `TurnId` at dequeue and emits a correlated failed outcome if setup fails after acceptance.
- Child outcome replay was not called out explicitly in the starting implementation steps. The TUI now deduplicates stamped child terminal outcomes before applying task-row, chat, or parent-notification effects.

No PR 2/3 manager, persistent actor, graph storage, first-class Lua `Agent` userdata, or graph navigation was introduced.

## Lua Driver Details Learned During Implementation

The compatibility subagent driver needed stronger state mechanics than the initial high-level “one idempotent finalizer” wording made explicit:

- Every accepted `TurnId` is tracked independently of whether it has a blocking waiter, because asynchronous `send` turns must also be terminalized on close.
- Finalized IDs are recorded under the shared state lock. This makes finalization, pending decrements, status updates, and waiter delivery idempotent across close/run races.
- The currently claimed active ID is tracked separately from queued IDs. `close()` synthesizes closed cancellation outcomes only for queued/unclaimed accepted turns; a turn already inside `Agent::run` remains authoritative and is finalized from the outcome returned by that run.
- Closing retires the existing cancellation trigger to wake an active run. If that run returns cancelled after closure, the compatibility adapter may refine the cancellation reason to closure while retaining its identity, usage, turn count, and terminal variant.
- Admission send failure is treated as failure of an already accepted turn and passes through the same finalizer rather than manually repairing counters.
- Session aggregate usage remains separate from each outcome’s local usage.
- Ordinary failed turns do not close the driver. Already queued and later inputs continue through the same session.
- `SubagentStatus::Done` boxes its result to avoid making every status value as large as the full typed outcome.

These mechanics fix the original success-only completion wait and the dropped-waiter/pending-count failure modes behind #54 and #55.

## Review Findings Resolved

The required reviews found issues that scoped compilation alone did not expose:

1. TUI and interactive-headless setup failures could consume accepted input without a correlated terminal outcome. Turn reservation and setup-failure terminalization were moved to the admission boundary.
2. Lua close initially relied on `saturating_sub`, which hid double-finalization rather than preventing it. Accepted/finalized ID tracking replaced that behavior.
3. A first close fix could synthesize a competing outcome for a turn already running. Active-ID ownership now defers that turn to `Agent::run`.
4. Manual `ControlComplete` initially still flowed through generic done/turn-end behavior. Chat and app handling now keep control completion non-terminal with respect to accepted turns.
5. Top-level print and SDK consumers were missed by early crate-scoped checks and were migrated after a workspace-wide review.
6. ACP pending-prompt registration occurred after sending input, creating a fast-completion race. Registration now occurs first and is rolled back if sending fails.
7. Duplicate stamped child outcomes could repeat UI and parent effects. The stamped-ID guard now rejects duplicates before those effects.

The final blocker re-review reported no remaining critical or high findings in these areas.

## Test Strategy as Executed

The acceptance behavior is covered by core lifecycle tests, existing cancellation tests updated for typed outcomes, deterministic Lua shared-state tests, TUI/app tests, ACP tests, and the existing Lua integration suite. Coverage includes:

- ID display/parse/serde round trips and a compile-fail Rustdoc example proving nominal IDs cannot be interchanged;
- completed, failed, and cancelled normalization;
- exact returned/emitted outcome comparison and one terminal event attempt;
- terminal channel send failure with the returned outcome retained;
- per-turn usage and turn-count reset on reused agents;
- failure metadata mapping across the current `AgentError` taxonomy;
- cancellation history sanitation;
- closed-session admission rejection, waiter resolution, queued-turn closure, and pending-count convergence;
- failed-turn session reuse and outcome-driven Lua completion;
- child failure presentation and duplicate child outcome suppression;
- manual compaction completing without root turn-end behavior;
- ACP completed/cancelled/failed translation and pending-prompt ordering.

Some test coverage landed as focused in-module tests rather than under every exact proposed test name or integration-file location in the starting table. The acceptance properties, not exact test placement, were treated as authoritative. Existing sleep-based integration tests outside the touched lifecycle mechanics were not broadly rewritten when deterministic in-module channels covered the race directly.

## Verification Record

The final implementation tree passed:

- workspace-wide `cargo check --workspace --tests --benches`;
- affected `maki-agent`, `maki-lua`, `maki-ui`, and `maki-acp` suites;
- the nominal-ID compile-fail doctest;
- formatting and diff hygiene checks;
- required plan and implementation reviews, including blocker re-review;
- `.ssh/remote-ci.sh`, which ran the repository `just ci` workflow:
  - `cargo fmt --all -- --check`;
  - `stylua --check plugins/`;
  - `cargo clippy --all --tests --benches -- -D warnings`;
  - `ruff check scripts/` and `ty check scripts/`;
  - `cargo nextest run --workspace`, with 4,666 tests passed and none skipped;
  - generated documentation verification;
  - `cargo machete` dependency analysis.

The only observed warning was the existing future-incompatibility notice for `proc-macro-error2 v2.0.1`.

## Remaining Boundaries and Follow-Up

The following remain intentionally outside this PR and should not be inferred as delivered:

- Agent and turn IDs are runtime-stable only; they are not persisted across process restarts.
- Exactly-once means one producer terminalization decision and one event delivery attempt. It does not provide crash-durable subscriber delivery, replay, or acknowledgement.
- Tool-use IDs and numeric UI `run_id` remain compatibility correlation fields, not runtime identity.
- The Lua surface remains the compatibility `maki.agent.Session` API; no first-class agent userdata or graph API was added.
- No shared persistent actor, manager/graph, wait/notification system, guest modes, or graph navigation was added.
- The `proc-macro-error2` future-incompatibility warning is unrelated dependency maintenance.
- The developer-facing PR/issue note about compatibility IDs versus runtime IDs still belongs in the eventual PR description; no separate user-facing API documentation was needed because the public Lua API shape did not change.