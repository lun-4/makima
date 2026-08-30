### Goal

Implement issue #24 PR 2, **Shared agent actor**, assuming PR 1’s `AgentId`, `TurnId`, `TurnOutcome`, and exactly-once terminal event contract are already merged. Introduce one persistent FIFO actor in `maki-agent` that owns agent history, lifecycle, scheduling, active-turn cancellation, and retained outcomes, then adapt both the TUI root agent and `maki.agent.session` subagents to use it without changing their observable APIs or behavior.

### Implementation Summary

Add a dependency-neutral actor runtime under `maki-agent/src/actor/`. The actor will own one stable `AgentId`, one `History`, a FIFO work queue, active/closed lifecycle state, per-turn cancellation, and a retained `TurnId -> TurnOutcome` store. It will remain alive after ordinary completed, failed, or cancelled turns; ordinary failures are terminal only for their turn, per the operator’s decision. Explicit close/shutdown closes the actor and terminalizes accepted queued work.

The actor must not depend on `maki-ui` or `maki-lua`. Define a small object-safe async backend contract in `maki-agent` using the repository’s boxed-future pattern. Backend implementations in `maki-ui` and `maki-lua` receive actor-owned history and turn context, perform caller-specific preparation, construct the existing transient `Agent`, and return its authoritative `TurnOutcome`. This keeps dynamic TUI concerns such as cwd/instruction reload, Lua prompt slots, MCP prompt expansion, model lookup, tool rebuilding, and `run_id` event stamping outside `maki-agent`, while centralizing the duplicated coordinator state machine.

Primary touch points:

- New: `maki-agent/src/actor/mod.rs`, plus focused `queue.rs` and/or `state.rs` modules if they reduce complexity.
- Export actor APIs from `maki-agent/src/lib.rs`.
- Preserve the low-level execution contract in `maki-agent/src/agent/run.rs`; only add narrowly required builder hooks or helpers.
- Replace root coordination in `maki-ui/src/agent/agent_loop.rs`; reduce `maki-ui/src/agent/shared_queue.rs`, `cancel_map.rs`, and `command_router.rs` to compatibility/presentation adapters or remove them when actor APIs fully replace them.
- Replace `SessionState`, `SubagentDriver`, `admit_turn`, and `subagent_driver` in `maki-lua/src/api/agent.rs` with a `maki-agent` actor handle plus Lua result/presentation adapters.
- Preserve `AgentHandles`, `MessageQueue`, `LuaSession`, and the documented `maki.agent.session` methods as compatibility surfaces.
- Keep `maki-agent/src/headless.rs`, ACP, SDK, print, manager/graph topology, persistence, nested spawning, policy snapshots, mode ownership, queue coalescing, and first-class Lua agent APIs out of scope. Those belong to later PRs in `.agents/makima-issues/24-agent-system-deliverables.md`.

Implementation delegation is part of the execution approach. The execute lead should use `general-purpose-mini` subagents for bounded implementation slices, explicitly instructing each to mutate only its assigned files and to report changes/tests. Suggested slices are actor core/tests, TUI adapter, and Lua adapter/integration tests. The execute lead remains responsible for API design, resolving overlapping edits, integration, all verification, and final review. Do not delegate two concurrent writers to overlapping files, especially `maki-lua/src/api/agent.rs` or `maki-ui/src/agent/mod.rs`.

### Implementation Plan

#### Phase 1: Define the dependency-neutral actor contract in `maki-agent`

1. Add a public `actor` module with concise types along these lines, adjusting names to Rust idioms during implementation:
   - `AgentActor` or an internal actor task that exclusively owns `AgentId`, `History`, queue state, active turn state, retained outcomes, and lifecycle.
   - Cloneable `AgentActorHandle` for admission, exact-turn waiting/result lookup needed by adapters, active cancellation, close/shutdown, queue inspection/removal needed by the existing TUI, and history snapshot access.
   - `ActorStatus`/`ActorSnapshot` with at least open/idle/running/closed state, active `TurnId`, queued turn count, latest terminal outcome, and cumulative usage where compatibility requires it.
   - Typed work that distinguishes `TurnAdmission`, root `InterruptInput`, and control operations. Lua admission immediately creates a real turn and returns its actor-generated `TurnId`. Root queue input remains input work without a `TurnId` until the scheduler starts a standalone `Agent::run`; if `InterruptSource` extracts it into the active run, it is folded into that active turn and never creates an orphan waiter or retained outcome. Compaction never receives a `TurnId`.
   - A domain error type using `thiserror` for closed actor/channel/admission errors. Adapters translate this to their current strings such as `SESSION_CLOSED_ERR`.
2. Define a lifetime-parameterized, object-safe async backend trait using `maki_providers::provider::BoxFuture` or an equivalent local alias. Its concrete contract must be fixed by the execute lead before delegation, along these lines: `execute<'a>(&'a mut self, &'a mut History, TurnContext, AgentInput, ReasonedCancelToken, Option<Arc<dyn InterruptSource>>) -> BoxFuture<'a, ExecuteResult>`. It must let adapters:
   - initialize caller-specific runtime state before work is consumed;
   - execute one accepted turn using actor-owned history, stable identity, actor-generated `TurnId`, caller correlation metadata, and a reason-aware per-turn cancellation source;
   - report whether `Agent::run` was entered and therefore already attempted terminal event delivery;
   - execute standalone control work without pretending it is a turn;
   - attempt actor-synthesized terminal delivery, stamped from admission metadata, only for setup failures or queued turns that never entered `Agent::run`.
   Retention and waiter completion must not depend on event-sink success. The actor must never re-emit or mutate the authoritative outcome returned after `Agent::run` has attempted delivery. `AgentInput` is not `Clone`: every accepted input has one ownership transfer into the actor/backend. Do not derive `Clone` or duplicate image/content payloads to satisfy adapter lifetimes. Root promotion, interrupt folding, and `Agent::run` construction move the input exactly once; presentation metadata lives separately in root/work metadata.
3. Make one dependency-neutral, lock-backed queue core in `maki-agent` the single scheduling source. The actor scheduler, synchronous `InterruptSource`, TUI inspection/removal, and drain publication must all operate on that same deque; the TUI may only project rendering metadata and must not maintain a shadow scheduling queue. Define a linearization protocol under the queue lock for admission, close, interrupt extraction versus scheduler pop, remove/clear, and empty/drain publication.
   - Every accepted `TurnAdmission` reaches exactly one retained terminal `TurnOutcome`; root input extracted into an active turn belongs to that turn instead of becoming a separate admission.
   - Normal `Completed`, `Failed`, and user-cancelled turns leave the actor reusable.
   - Explicit close/shutdown rejects new work, cancels the active turn, terminalizes every queued real `TurnAdmission` as `Cancelled(Closed|Shutdown)`, resolves exact-turn waiters, and drops non-turn root input/control work according to its existing presentation contract.
   - Removing or clearing queued real turns must terminalize them rather than silently deleting accepted work. Root inputs that do not yet have a `TurnId` may be removed with their current queue-panel semantics.
   - Duplicate completion cannot retain, notify, or attempt public terminal delivery twice. Queue/control-channel failure terminalizes accepted real turns rather than stranding waiters.
4. Retain outcomes by `TurnId` inside the actor. Provide exact-turn waiting or a per-admission receiver as an internal compatibility primitive so Lua `prompt()` does not poll. Admission, ticket registration, and retained-outcome lookup must use a linearized protocol: `wait(turn_id)` either observes a retained outcome or obtains/registers a receiver before finalization can make the ticket disappear. It must never return `UnknownTurn` merely because finalization won between lookup and registration; duplicate or late waits deterministically return the retained outcome. Keep the latest outcome/status query for Lua `status()`. Do not add the public first-class Lua wait API planned for PR 5.
5. Preserve cumulative per-agent usage in actor state as the sum of terminal turn outcomes. Per-turn `TurnOutcome.usage()` remains unchanged. The Lua adapter will expose the cumulative value currently returned by `prompt()`/`status()`.
6. Keep history in the actor and preserve optional `SharedMessages` mirroring. Initial restored history must be sanitized and published synchronously before a handle escapes, matching the current TUI checkpoint invariant in `maki-ui/src/agent/mod.rs`.
7. Preserve root interrupt behavior without coupling the generic queue to TUI presentation:
   - expose a synchronous actor queue view implementing `InterruptSource`; it extracts the next compatible root input from the same queue core and returns `ExtractedCommand::Interrupt`, folding that input into the active turn without allocating another `TurnId`;
   - preserve the active turn’s event correlation when the folded input is consumed, and ensure no waiter/outcome is created for the folded input;
   - model in-turn extracted compaction separately: it emits the existing `CompactionDone` event and continues the active turn, while idle standalone compaction uses `ControlComplete`/`ControlError`; neither path gets a `TurnId`;
   - carry `run_id` only as presentation/cancellation correlation, never as agent or turn identity. Choose one canonical encoding (currently `r{run_id}`) and centralize it: targeted cancel, precancel storage, queued root matching, compact matching, and backend event stamping must normalize through the same representation rather than comparing bare compact ids with prefixed root correlations;
   - when a deferred root starts a standalone turn, preserve its original `run_id`, `displayed` bit, display text, and image count through promotion. Decide `QueueItemConsumed` from that original metadata before moving the input; never replace promotion metadata with defaults;
   - keep queue inspection/removal atomic and retain the push-versus-drained publication ordering guarantee.
8. Add structured tracing fields for `agent_id`, `turn_id`, queue depth, lifecycle transition, and terminal status. Do not add a new dependency unless the existing workspace crates cannot express the design.

Delegate this phase to one `general-purpose-mini` implementation subagent after the execute lead fixes the public trait and state-machine shape. Require it to add the actor’s colocated unit/scenario tests and restrict its writes to `maki-agent`.

#### Phase 2: Adapt the TUI root loop without changing UI behavior

1. Implement the actor backend in `maki-ui/src/agent/agent_loop.rs` (or a replacement module) and move only root-specific execution preparation into it:
   - initial environment variables, instruction loading, `btw_system`, and MCP readiness/cancellation;
   - per-turn cwd change detection and instruction reload;
   - MCP prompt lookup and preamble insertion;
   - prompt-slot collection and mode/system prompt construction;
   - model/provider snapshot selection and tool definition rebuild;
   - user-answer receiver draining/wiring;
   - transient `Agent::new(...).with_loaded_instructions(...).with_user_response_rx(...).with_interrupt_source(...).with_cancel(...).with_mcp(...)` construction;
   - root compaction through the existing `agent::compact` path.
2. Make `AgentHandles` spawn and own the new actor task/handle while preserving its current fields or equivalent adapter methods used by `EventLoop` and `App`: command sender behavior, event channel, answer sender, shared history, `btw_system`, MCP handles/errors, queue handle, timeout metadata, respawn, cancellation, and task joining.
3. Preserve envelope compatibility exactly:
   - each standalone root turn’s events use the caller’s current `run_id` through `EventSender`; root input folded into an active turn uses the active turn’s correlation and creates no second terminal event;
   - `QueueItemConsumed` is emitted only for deferred, not-already-displayed messages;
   - `QueueDrained` is emitted once after the queue becomes empty and cannot race ahead of a concurrent push. Trigger drain consideration from a finally-equivalent completion path for every popped work item, including initialization/setup failure and defensive unsupported-work exits. Actor-synthesized cancellation, targeted removal, and precancelled/dropped root or compact paths must also leave drain state publishable rather than stranding the panel;
   - idle compaction preserves `ControlComplete`/`ControlError`; compaction extracted during streaming preserves `CompactionDone` and continuation of the active turn; neither creates a retained outcome;
   - the output channel survives respawn and stale events remain filtered by `run_id`.
4. Route commands through an explicit cancellation state machine linearized under the actor state/queue lock. The first cancellation reason installed for an active turn is authoritative and immutable; later user/close/shutdown requests cannot rewrite an outcome that may already be emitted. Preserve precancellation before backend trigger installation.
   - `Cancel(run_id)` installs `User` for matching active work, drops stale no-`TurnId` root input under the existing correlation rule, terminalizes any affected queued real turns, and leaves the actor reusable.
   - UI `CancelAll` remains a reusable cancel-all: cancel active root work as `User`, terminalize queued real turns, drop queued root input/control work per current behavior, and continue accepting later root work. It must also continue calling `subagent_cancels.cancel_all()` so compatibility child sessions are cancelled.
   - Lua `close` installs `Closed`, rejects future admission, terminalizes queued turns, and retires only that session’s compatibility cancel slot.
   - Process exit and old-actor respawn use `shutdown`, install `Shutdown` where no earlier active reason won, close the actor, and continue cancelling compatibility subagents globally.
   Add deterministic races for user cancel versus shutdown and close versus shutdown; expected reason follows whichever operation wins the state-lock linearization, and emitted, retained, and waiter values must match.
5. Keep `shared_queue.rs` as a thin UI presentation adapter if needed for `QueueEntry` colors/visibility. Move scheduling truth into `maki-agent`; do not duplicate a second deque. Preserve queue panel methods (`remove`, `clear`, `text_messages`, `panel_entries`) through actor-backed operations.
6. Preserve respawn semantics: publish restored history immediately, repoint the app to the new queue before closing the old actor, flush restored queued messages once, retain the per-tab output channel, and prevent an old actor’s close/cancel from poisoning the replacement.

Delegate the bounded TUI adapter work to a separate `general-purpose-mini` subagent only after Phase 1’s API is integrated. Its allowed write set should be `maki-ui/src/agent/**`, `maki-ui/src/app/queue.rs`, and directly affected TUI tests. The execute lead should integrate any event-loop call-site changes.

#### Phase 3: Adapt `maki.agent.session` without changing Lua behavior

1. In `maki-lua/src/api/agent.rs`, keep session option parsing and provider/model/tool construction at the Lua boundary. Construct a shared actor with the same stable `AgentId`, system string, tool JSON, model/provider, audience, MCP freshness, thinking/fast options, answer channel, local tools, and independent session cancellation.
2. Implement a Lua actor backend that creates the transient low-level `Agent` for each actor turn using the actor-owned history. Preserve:
   - `.with_user_response_rx`, `.with_cancel`, `.with_mcp`, and `.with_local_tools`;
   - the fixed current compatibility `AgentMode::Build` input;
   - the per-session semaphore acquired immediately before each turn and released for every terminal path; race acquisition against close cancellation so parked turns cannot hang;
   - extraction of the latest non-empty assistant text for that turn;
   - session-local structured-output commit capture and reset per completed turn;
   - `SubagentInfo` initialization on first run, including answer/input channels and current UI/task id.
3. Replace `SessionState`, `SubagentDriver`, `admit_turn`, `subagent_ui_input_relay`, and `subagent_driver` with a thin `LuaSession` adapter over `AgentActorHandle`:
   - `prompt(message)` performs actor admission and waits for that exact turn’s retained result without polling;
   - `send(message)` returns current compatibility success/error pairs based on real actor admission;
   - `status()` preserves `running | done | closed`, latest result text/captured value/error/retryable flag, and cumulative token totals. Closed wins; otherwise any active or queued accepted turn reports `running`, even when a previous latest outcome exists. Only an open actor with no pending work reports `done`. Pending status does not erase latest-result metadata, and token fields come from actor cumulative usage across all terminal turns rather than the latest outcome;
   - `close()` and `Drop` are idempotent, reject later work, resolve queued/blocking waiters as closed cancellations, retire only this session’s `CancelSlot`, and do not affect sibling sessions sharing a UI/tool-use id;
   - `session_id()` continues returning the current UI/task id rather than exposing `AgentId` as a breaking API change.
4. Preserve session independence from the parent turn’s cancellation. A normal parent run ending must not cancel an already spawned subagent. Continue wiring cancellation only through the session token and existing `subagent_cancels` compatibility map.
5. Preserve parent/UI relay behavior:
   - keep `relay_session_events` and its silent/live event filtering;
   - continue stamping subagent envelopes with `SubagentInfo`;
   - forward structured `TurnOutcome` failures and usage;
   - emit `SubagentHistory` once after the first executed terminal turn, whether completed, failed, or cancelled, so partial/error transcripts reach the parent. Track an explicit entered/executed marker rather than inferring execution from actor status or the existence of a history snapshot. Close fallback is eligible only after at least one turn entered backend execution and produced a non-empty transcript that no terminal path relayed;
   - actor-synthesized cancellation of queued work, setup that never enters execution, and empty initial/unrelated history must not fabricate executed transcript content, and later close must not duplicate history delivery.
6. Keep bundled task and bash plugin APIs unchanged. This PR must not replace polling in `plugins/task/init.lua`; PR 5 owns first-class waits/notifications. Existing blocking `task` behavior, task spawn/send/get/despawn, automode classifier sessions, local result capture, and retries must continue through the compatibility adapter.

Delegate this phase to one `general-purpose-mini` subagent after the actor API and TUI integration are stable. Restrict its writes to `maki-lua/src/api/agent.rs`, focused `maki-lua/tests/**`, and only directly necessary compatibility tests. The execute lead must review this large-file edit carefully and resolve any overlap.

#### Phase 4: Remove duplicated coordinators and tighten integration

1. Delete obsolete TUI/Lua coordinator state only after both adapters pass their focused scenario tests. There must be one FIFO/lifecycle/outcome implementation in `maki-agent`, not renamed copies in each caller.
2. Keep the existing low-level `Agent::run` tests because they prove provider/tool-loop behavior and PR 1’s terminal contract. Add actor-level tests rather than replacing all low-level tests.
3. Check all direct `Agent::run` consumers. Explicitly leave headless, ACP, SDK, and print unchanged in this PR and ensure actor exports do not accidentally force their migration.
4. Update `AGENTS.md` architecture text to identify `maki-agent` as owning the shared persistent actor used by the TUI root and Lua sessions. Add concise module-level Rust documentation for the actor’s ownership and lifecycle invariants. No user-facing documentation or generated Lua API change is expected because compatibility behavior and signatures remain unchanged.
5. Run formatting, focused checks/tests, full lint/test/doc generation checks, and dependency hygiene. Follow `AGENTS.local.md`: use `.ssh/remote-ci.sh` for real build/test execution; local `just fmt`/`just fmt-check` are acceptable.

### Acceptance Criteria

- **AC.1:** Both the TUI root and Lua session compatibility surfaces route admission, scheduling, history, lifecycle status, cancellation, cumulative usage, and retained outcomes through the generic `maki-agent` actor and expose actor-owned state consistently.
- **AC.2:** Every successfully admitted real turn receives a stable `TurnId` and reaches exactly one retained `Completed`, `Failed`, or `Cancelled` outcome, including queued turns during close/shutdown and setup/backend failures. Exact-turn waiters always resolve. Root input synchronously folded into an already active turn is explicitly not a second turn and creates no `TurnId`, waiter, or retained outcome.
- **AC.3:** An ordinary failed turn leaves the actor reusable. A later queued or newly admitted turn runs on the same agent/history, while the earlier failed outcome remains queryable by its `TurnId`.
- **AC.4:** Actor admission and execution are strict FIFO. Close/shutdown atomically reject later admission, cancel active work, terminalize accepted queued work without executing it, and are idempotent.
- **AC.5:** TUI root behavior is unchanged: restored history is mirrored before handles escape; dynamic turn preparation and compaction still work; deferred queue display/drain events retain their ordering; interrupts and cancellation retain current semantics; event envelopes retain the correct `run_id`; respawn preserves the tab event channel and consumes restored queued messages exactly once.
- **AC.6:** `maki.agent.session` keeps the existing `session`, `prompt`, `send`, `status`, `close`, and `session_id` behavior, including cumulative usage, exact blocking result delivery, retryable error reporting, local-tool capture, semaphore gating, and rejection after close.
- **AC.7:** A Lua subagent survives normal completion/cancellation of the parent turn, forwards terminal success/failure/cancellation and UI events, surfaces its history to the parent exactly as today, and remains reusable after an ordinary failed turn.
- **AC.8:** Compaction remains a no-`TurnId` control operation and never creates or overwrites a retained turn outcome. Idle compaction emits `ControlComplete`/`ControlError`; compaction extracted during an active turn emits `CompactionDone` and the active turn continues to its one terminal outcome.
- **AC.9:** Focused crate checks, actor/TUI/Lua scenario tests, workspace lint/tests, generated-doc checks, formatting, and dependency hygiene all pass on the remote build workflow.

### Test Strategy

Prefer actor-level state-machine scenarios and real compatibility integration tests over isolated tautologies. Use deterministic fake/canned providers and channel barriers, not sleeps. Existing sleep-based polling in `maki-lua/tests/subagent_run_end.rs` should be replaced with bounded channel/event synchronization when touched. Any legacy/in-memory queue fixture retained for presentation tests must be `#[cfg(test)]` or behind a test-support feature and unreachable from production actor scheduling. Production compatibility tests exercise the actor-backed queue; fixture tests cover projection helpers only.

| Acceptance criterion | Named test or check |
|---|---|
| AC.1 | `maki-agent::actor::tests::actor_owns_history_queue_and_retained_state`, TUI `root_adapter_reports_actor_owned_history_and_lifecycle`, and real Lua-session `lua_session_status_and_history_follow_actor_state`; each compatibility path admits work and observes the same actor-owned snapshot/outcome semantics. |
| AC.2 | `accepted_turns_terminalize_exactly_once_and_waiters_resolve`; `exact_wait_registers_before_or_observes_finalization_without_unknown_turn`, using a barrier to finalize between lookup/registration boundaries; `executed_turn_is_retained_without_actor_reemission`; `setup_failure_synthesizes_one_terminal_delivery`; `queued_close_synthesizes_one_terminal_delivery`; `closed_terminal_sink_still_retains_and_resolves_waiter`; and `duplicate_finalize_does_not_redeliver_or_renotify`. Count delivery attempts and compare emitted, retained, and waiter values where applicable. Add `interrupt_input_folds_into_active_turn_without_orphan_outcome` to prove extracted root input creates no second `TurnId`, waiter, retained result, or terminal event and uses the active correlation. Exercise non-clone input ownership through real root promotion and interrupt folding rather than adding a compile-only tautology. |
| AC.3 | `failed_turn_is_retained_and_same_actor_runs_later_turn`; extend/retain `maki-lua/tests/subagent_run_end.rs::failed_subagent_turn_resolves_and_same_session_recovers`. |
| AC.4 | `admission_is_fifo_and_close_drains_without_running_queued_turns`, `close_is_idempotent_and_rejects_racing_admission`, `interrupt_poll_linearizes_against_scheduler_pop`, `remove_clear_linearize_against_close`, `first_cancellation_reason_wins_user_vs_shutdown`, `first_cancellation_reason_wins_close_vs_shutdown`, and `reusable_cancel_all_accepts_later_work`; use barriers/channels to make race ordering deterministic and compare emitted, retained, and waiter reasons. |
| AC.5 | TUI tests: `spawn_publishes_the_resumed_history_before_the_handles_escape`, `respawn_twice_keeps_channel_and_delivers_restored_queue`, `respawn_publishes_the_new_history_into_the_app_mirror`, actor-backed `queue_drain_is_serialized_with_push`, `setup_failure_still_publishes_queue_drained`, `root_promotion_preserves_display_metadata_and_consumed_event_rule`, `targeted_cancel_uses_canonical_root_and_compact_correlation` (including adjacent run-id isolation), `deferred_message_emits_consumed_once`, `precancel_before_backend_trigger_terminalizes_once`, `active_cancel_keeps_actor_reusable`, `cancelled_run_terminalizes_real_queued_turns_and_drops_stale_root_input`, `cancel_all_cancels_compatibility_subagents_and_root_remains_reusable`, `exit_shutdown_cancels_compatibility_subagents`, `interrupt_consumes_next_queued_input_under_active_run_id`, and separate idle/in-turn compaction event tests. Verify the test-only presentation queue is absent from non-test compilation. |
| AC.6 | Lua unit/integration tests: port `close_rejects_new_admission_and_resolves_blocking_waiter`, `finalization_tracks_pending_turns_and_status`, `lua_status_running_precedes_latest_done_while_turn_is_queued`, `lua_status_and_prompt_report_cumulative_usage_after_multiple_turns`, plus `session_semaphore_close_terminalizes_parked_turn` and `session_local_tool_commit_is_returned_by_prompt`. Assert pending status retains previous latest-result metadata. Run bundled task/bash plugin scenario tests that exercise the real session API. |
| AC.7 | Retain and make deterministic `subagent_outlives_the_run_that_spawned_it` and `subagent_completion_surfaces_reply_to_parent`; add `subagent_failed_outcome_reaches_parent_with_diagnostic`, `failed_first_turn_surfaces_history_once`, `cancelled_first_turn_surfaces_history_once`, `lua_close_does_not_fallback_history_for_queued_only_work` with non-empty unrelated initial history, setup-failed-versus-entered coverage, and `close_does_not_emit_duplicate_subagent_history`. |
| AC.8 | `idle_compact_has_no_turn_id_and_emits_control_completion` plus `in_turn_compact_emits_compaction_done_and_active_turn_terminalizes_once`; both assert no retained control outcome is created or overwritten. |
| AC.9 | Run `just fmt-check`; `cargo check -p maki-agent --tests`; `cargo check -p maki-ui --tests`; `cargo check -p maki-lua --tests`; focused `cargo nextest run -p maki-agent`, `-p maki-ui`, and `-p maki-lua` filters while iterating; then `.ssh/remote-ci.sh` for `just ci` (`fmt-check`, clippy with warnings denied, Python checks, workspace nextest, generated docs, and cargo machete). Review the generated Lua API diff as a golden compatibility check: it must be empty unless exact approved lines are listed. If an untouched workspace test fails, rerun that exact test/filter once to classify flakiness and record both results; do not modify unrelated code or weaken tests, and report an unresolved unrelated failure separately from touched-crate regressions. |

The execute lead should run the cheapest relevant test after each delegated slice, then integration tests after each adapter migration. Do not wait until the end to discover a crate-boundary or `Send`/lifetime problem in the backend trait. Reserve workspace/root `Cargo.toml`, lockfile, `maki-ui/src/event_loop.rs`, and other cross-slice call-site edits to the execute lead unless a mini is given an expanded exclusive allowlist. Minis must avoid formatting outside their allowlist and report required out-of-scope edits instead of making them. Delegation is fail-stop per slice: if a subagent times out, returns an incomplete or contract-incompatible change, exceeds its allowlist, or fails focused tests, do not begin dependent slices. Inspect and revert or repair the partial change, then issue a bounded follow-up with the same exclusive allowlist. Record the failure and resulting regression test; a subagent completion claim never substitutes for lead-owned diff review and verification.

### Review Strategy

Before execution handoff, run `plan-reviewer` and resolve or rebut all critical/high findings.

During implementation:

1. The execute lead defines and reviews the actor trait/state-machine boundary before delegation. Treat the explicit PR 3+ non-goals as a scope gate: diff review must reject manager/graph, persistence, nested spawning, policy ownership, queue coalescing, first-class Lua API, or unrelated frontend migration. The same mandatory diff gate must confirm `SubagentDriver`, its lifecycle `SessionState`, and the root coordinator’s lifecycle/queue stores are removed or reduced to backend/presentation adapters, with no second coordinator state machine surviving under a new name.
2. Each `general-purpose-mini` implementation subagent receives a non-overlapping file allowlist and returns a concise change summary, tests added/run, and known risks. The execute lead records the delegated slices, inspects every diff, integrates cross-cutting edits, and runs focused checks before starting the dependent slice.
3. After all automatable tests pass, dispatch a `general-purpose` subagent for a cross-crate implementation review. It should focus on exactly-once terminalization, admission/close races, actor/backend ownership, cancellation reason correctness, retained-outcome consistency, queue interrupt behavior, `run_id` compatibility, Lua session close/drop races, and accidental PR 3+ scope.
4. Fix or explicitly rebut every review finding. If any critical finding remains, run another review pass after fixes until no critical findings remain or report a concrete blocker to the operator.

### Documentation Strategy

Update `AGENTS.md` only where its architecture summary should say that `maki-agent` owns the persistent shared actor used by TUI roots and Lua agent sessions. Add module/type documentation in `maki-agent/src/actor/` describing ownership, admission, terminalization, failure reuse, and close/shutdown invariants.

Do not alter public Lua doc comments, signatures, parameter descriptions, return wording, or generated ordering in this PR. Run `just gen-docs-check` and inspect the generated diff. Any changed wording must be listed line-by-line as an intentional compatibility-preserving correction and reviewed; an internal refactor alone is not justification for generated API text changes.

### Risks, Blockers, and Required Decisions

- **Resolved decision:** Ordinary failed turns do not close the actor. They retain a failed outcome and the same actor accepts/executes later turns. Only explicit close/shutdown or an unrecoverable actor-control failure closes it.
- **Crate-boundary risk:** `maki-agent` must not import `maki-ui` or `maki-lua`. Keep dynamic preparation behind an object-safe boxed-future backend contract. Prototype its lifetimes and `Send` bounds before broad migration.
- **Exactly-once risk:** `Agent::run` both returns and attempts to emit the same `TurnOutcome`. `ExecuteResult` must indicate whether execution entered `Agent::run`; the actor retains/finalizes its returned value without a second public emission. Setup failures and queued cancellations that never entered `Agent::run` receive one actor-synthesized outcome and one delivery attempt through admission-stamped metadata. Sink failure never blocks retention or waiter completion. Tests cover closed sinks and duplicate finalization attempts.
- **Cancellation reason risk:** `Agent::run` currently normalizes cancellation as `User`. Add a narrow reason-aware cancellation input/helper so the actor selects `User`, `Closed`, or `Shutdown` before `Agent::run` constructs and emits its authoritative outcome. Never rewrite the returned value after emission. For active cancellation, tests must assert the emitted envelope, retained outcome, and waiter result are equal and carry the requested reason; queued unstarted turns may use actor-synthesized cancellation.
- **Interrupt/queue risk:** The current root queue doubles as `InterruptSource`. Moving FIFO ownership without preserving mid-turn interrupt extraction would silently change how queued prompts interrupt or follow active work.
- **Presentation risk:** TUI queue entries contain display-only fields and theme colors. Keep these in a UI adapter over actor queue snapshots rather than contaminating the core actor type or maintaining a second scheduling deque.
- **Lua result risk:** Actor-retained `TurnOutcome` alone does not contain assistant text or local structured capture. Keep a typed adapter result keyed by `TurnId`, derived from actor history boundaries and commit state, while ensuring lifecycle truth remains actor-owned.
- **Close race risk:** `LuaSession::Drop`, explicit `close`, cancellation-map retirement, semaphore waiting, and concurrent admission can race. Make actor close idempotent and serialize admission against closure; retain existing sibling cancel-slot behavior.
- **Parent cancellation risk:** A subagent session must continue using an independent cancellation token. Never derive it from the parent turn token.
- **Respawn risk:** Old TUI senders intentionally survive respawn. Preserve the output channel and `run_id` stale-event filter while replacing only the actor input/queue/task.
- **Scope risk:** `maki-agent/src/headless.rs` already contains another interactive loop, but the delivery sequence defers ACP/headless/SDK/print migration to PR 9. Do not expand PR 2 to those consumers.
- **Implementation delegation risk:** Mini subagents are appropriate for bounded mechanical phases, not for independently inventing incompatible actor APIs. The execute lead must establish interfaces first and avoid overlapping concurrent edits.
- **Working-tree risk:** Session-start status reports unrelated untracked `.agents/makima-issues/`, `.agents/skills/testing-methodology/`, and `AGENTS.local.md`. Preserve them and do not include or overwrite unrelated changes.