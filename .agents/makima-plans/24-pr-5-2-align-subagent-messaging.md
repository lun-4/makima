## Goal

Align TUI and Lua subagent messaging so messages sent during work reach the next safe model request within that turn. Deliver child completions to an active parent at the same boundary, and batch pending completions into one idle continuation instead of one parent turn per result.

## Implementation Summary

Baseline: `31fb678f4f1253a1fd30bec4725a2e685809a055`. This follow-up supersedes PR5's distinct-follow-up-turn behavior for the bundled `task_send` tool only. The low-level exact `Agent:send`, `Agent:enqueue`, `Agent:prompt`, ticket waits, and cancellation APIs remain available with their current contracts.

Use the shared actor, existing manager permits, retained turn results, and existing agent model loop. Add actor-owned start-or-steer admission and a completion inbox. Keep user instructions and attributed child observations distinct. Do not concatenate their text or merge their identities. Children report automatically when their work turn finishes; no child-to-parent or sibling messaging feature is required.

Affected surfaces:

- `maki-agent/src/actor/{mod,types,queue,runner,tickets,delivery}.rs`, actor tests, `agent/run.rs`, and manager tests.
- `maki-ui/src/agent/{shared_queue,agent_loop}.rs`, `app/queue.rs`, queue presentation/events, and their tests.
- `maki-lua/src/api/agent.rs`, `api/agent/handles/{agent_methods,tickets,projection,subscriptions}.rs`, `api/agent/structured_output.rs`, and actual-plugin integration tests.
- `plugins/task/init.lua`, task specs, Lua API metadata, existing user documentation, and generated references.

Scope: shared-actor TUI roots and Lua child actors, including nested and standalone Lua children. ACP/SDK/print host roots still use their current execution loops and admission contracts. Their child actors gain start-or-steer messaging; this does not grant their roots managed completion delivery. No host migration, persistence format, sibling communication, new scheduler, model-side task dependency inference, or per-message cancellation is included.

The incident analysis is https://gist.github.com/philpax/1f6bc19dc094508648a1fd7e50f4edcb. It identifies distinct unconsumed completion records, not an infinite echo of one result. Preserve those records while changing scheduling.

## Implementation Plan

### 1. Establish production-path regression fixtures and repair TUI parity

Extend existing gated providers and actual Lua hosts rather than reproducing production queue logic inside tests. Add narrowly scoped gates at admission close, request submission, delivery claim, and finalization where existing gates are insufficient. Tests must control interleavings without sleeps.

The initial TUI adapter uses `actor.admit_turn` with default `Internal` provenance (`maki-ui/src/agent/shared_queue.rs:298`), while deferred `RootWork::new` uses `User` (`maki-agent/src/actor/types.rs:180`). Exact provenance comparison prevents active folding. Admit immediate user input with explicit `User` provenance; do not weaken the shared provenance guard.

Deferred input is not drawn before consumption, but `Agent::handle_queued_command` currently emits `already_displayed: true`. Carry presentation metadata through interrupt extraction rather than hard-code that flag. Preserve each input's images, text, notice, and display state. Assert one visible bubble per consumed input and queue removal without a new agent turn.

Create the TUI regression through the real `QueueSender` adapter and backend. Gate the first provider request, submit two deferred messages, finish the current request/tool batch, and assert the next request contains both messages under the original turn ID. A manually constructed actor admission with `User` provenance alone is not sufficient coverage.

### 2. Add actor-owned start-or-steer admission

Add an actor primitive and an authorized Lua `Agent:submit(ctx, message)` method. It returns an `AgentTurn` ticket. Keep `send` idle-only, `enqueue` exact FIFO, and `prompt` unchanged. The task adapter and TUI child input relay use `submit`; callers needing a distinct execution can still use `enqueue`.

Maintain a bounded per-turn instruction inbox and an explicit admission state. Target selection, lifecycle checks, inbox insertion, and the decision to start a successor are atomic under the actor state lock. Never implement this as `status()` followed by another operation.

Resolved target rules:

1. An ordinary turn with open input admission receives the message and returns its existing ticket, including while preparing, waiting for a permit, streaming, running tools, or waiting for descendants.
2. If no turn is executing but there is a not-yet-started `submit` turn in the same uninterrupted admission/configuration domain, join it and return its ticket. This groups rapid initial sends before the runner starts.
3. A completed, failed, cancelled, or admission-closed turn never reopens. Admit a successor through normal actor operations and return its new ticket.
4. Do not absorb separately admitted exact FIFO turns. If only such work is pending, append a new `submit` turn after it. If an ordinary turn is active, explicit steering targets that turn ahead of queued work; it does not reorder those queued operations relative to each other.
5. A configuration setter remains FIFO work. Steering an active turn retains that turn's admitted configuration even if a newer configuration has committed. Joining a pending `submit` turn cannot cross an intervening configuration/control/exact-work boundary.
6. Delivery-only continuations are not ordinary task work and are not steering targets. A submission during such a continuation admits or joins ordinary successor work.

Validate existing ownership, runtime/plugin generation, plan-mode, lifecycle, and policy constraints before acceptance. Keep each message's host-stamped sender provenance without overwriting the execution's original provenance or granting the sender new authority. A later authorized parent turn may steer an existing child turn despite differing source-turn metadata; implement this as explicitly authorized steering, not by relaxing root coalescing checks globally.

Use named initial limits: 64 outstanding instruction messages and 256 KiB of UTF-8 instruction payload per actor; reject overflow without cancelling the child or dropping prior messages. Reject a single oversized message. Existing TUI images stay on the TUI root path with their existing handling; `submit` remains text-only like current Lua sends. Count retained tickets separately from messages.

### 3. Coordinate safe-boundary input consumption with turn completion

Expose the targeted instruction source through `TurnContext` and connect it in both `TuiActorBackend` and `LuaActorBackend`. The latter currently omits `context.interrupt` at `maki-lua/src/api/agent.rs:479`. Reuse common boundary handling instead of creating a second model loop.

Drain instructions before the first request where possible and before subsequent provider requests, after all outstanding tool results are present. Preserve FIFO message order, separate user-message identity, images on existing TUI paths, and internal-data framing for observations. A blocked tool or descendant wait delays consumption until it returns; steering does not interrupt an in-flight provider request or undo side effects.

Add an actor-owned finish handshake to the model loop before successful terminal outcome publication. Under the same synchronization used by `submit`, either observe pending accepted instructions and continue, or close admission for that turn. Closing must occur before `Agent::run` emits its authoritative terminal event, not only when the actor later seals the ticket. Do not restart `Agent::run` with the same turn ID as a race workaround.

Track whether accepted instructions were included in a provider request, not merely removed from the queue or appended to history. Normal completion cannot silently leave accepted instructions unsubmitted. Cancellation and provider/setup failure settle the shared turn unsuccessfully. If the request budget prevents processing accepted instructions, return a clear budget-exhausted failure rather than successful completion. Do not reset budgets when a message arrives, retry side effects automatically, reopen terminal results, or migrate instructions silently to another ticket.

Close/cancel cuts must own pending input as well as queued/popped work. No post-cut input can be accepted under the cancelled turn. Never hold actor locks across provider calls, Lua callbacks, disk work, or awaits; document lock ordering alongside the admission/finish state machine.

### 4. Integrate task tickets, UI child sends, and structured results

Change the task adapter from `agent:enqueue` to `agent:submit`. Preserve `{ queued = true, turn_id }`, task IDs, wait response fields, and timeout behavior. A busy send can return the spawn/current turn ID; an idle send returns a new ID. Update descriptions to state this semantic change explicitly.

Deduplicate `tickets` and `order` by turn ID. An existing unresolved ticket can accept further messages even when the retained unique-ticket limit is reached. New unique turns remain subject to the limit. Actor inbox overflow is a nonfatal send rejection, not grounds for `fail_task` to close the child. Retained result eviction must never leave duplicate or dangling adapter entries.

An exact `task_wait` still observes one ticket. Additional accepted messages extend that unresolved turn; a later successor does not retarget an already-started wait. Omitted `turn_id` continues to snapshot latest once. Preserve parent permit release/reacquisition and descendant/self-wait checks. One execution has one start/end lifecycle, one usage charge, and one genuine automatic completion report, regardless of the number of attached messages.

Route focused subagent-tab text through the same `submit` primitive in `spawn_ui_input_relay`, instead of `actor.admit_turn`. Preserve current display, image rejection, authority, and closed-channel behavior.

When new instructions are consumed, mark earlier structured reports stale for final-result selection and completion validation. Keep prior reports in history, but require a valid report after the latest consumed instruction and expose only a fresh report as `captured`. Apply the same freshness rule to local captured final-result values. A failure before a fresh report must not return an old report as the revised task's final result. Ordinary child observations do not invalidate a report as if they were new user instructions.

### 5. Replace per-result parent turns with a completion inbox

Replace the delivery-turn-keyed pending store in `actor/delivery.rs` with actor-owned FIFO records keyed by source child agent/turn. Admission records a completion and wakes the actor, but does not allocate one parent ticket or operation per result. Preserve canonical child tickets/results independently. Duplicate publication of the same identity does not create another record; distinct completions are never discarded because their text resembles another result.

Expose a typed completion source separately from the instruction source. Each committed insertion remains an internal `ObservationSource::ChildResult`, with the immediate parent, child identity, original turn ID, status, diagnostics, provenance, and child configuration. Never promote completion text into a user instruction or change the receiving turn's tools, mode, or ownership.

For active ordinary turns, consume pending records at safe request boundaries. Unlike targeted instructions, a completion arriving after admission closes may remain pending for a successor. It is not promised coverage by the already-closing work ticket.

For an idle actor, schedule at most one delivery continuation marker for the pending batch. Use normal admission, configuration preparation, manager permits, and lifecycle events. Claim records only at the request boundary, not when reserving the continuation. If exact waits remove every record before it starts, discard the marker without a model call or fabricated cancelled delivery ticket.

Scheduling and fairness:

- Prefer existing explicit FIFO work over creating a notification-only continuation. That ordinary work can consume pending reports under its own configuration.
- An active turn may observe a completion admitted after a queued setter or work item. This is an explicit observation-visibility exception; queued setters/work retain relative FIFO execution order and are not applied inside the active turn.
- Drain at most 8 records and 32 KiB of model-facing data per boundary. Freeze an idle continuation's initial eligible batch; newer arrivals remain pending for later ordinary work or a successor continuation. Existing model-call limits still apply.
- Use a single scheduling marker so several pending reports do not reserve several continuations. Clear/reconcile the marker on claim, withdrawal, setup failure, cancellation, and close. A later arrival after a completed batch may legitimately schedule another continuation.
- If delivery-continuation preparation/setup fails before consumption, retain the records, clear the marker, emit an observable failure, and suspend automatic notification-only scheduling. Further result arrivals alone do not retry. The next explicit ordinary work admission or successful configuration change clears the suspension; ordinary work consumes the pending records, or an idle configuration change permits one fresh continuation attempt. Apply the same suspension after a delivery-only continuation terminates unsuccessfully with unconsumed records. Do not retry already-consumed records. This bounds attempts without hiding pending output.
- Suppress upward completion reporting only for delivery-only continuations. An ordinary child turn that consumes grandchild reports still publishes its own final result once. Do not infer suppression from `Internal` provenance alone.

### 6. Define consumption, bounds, delivery events, and failures

Preserve publication-before-ticket-resolution ordering. A successful exact wait by the recorded parent claims only its matching unconsumed record. A pending poll, timeout, or retained `result()` read does not consume it. Automatic drain and exact wait claim under the same actor synchronization. If automatic history insertion won first, the retained result remains readable, but it is not automatically inserted again. Do not promise that the result cannot be explicitly read twice.

Consumption means committed insertion into parent history. It does not prove provider success or model compliance. A consumed observation stays in history on subsequent provider failure and is not queued again. Claims and synchronous history insertion form a no-await commit path; handle close/cancel before that path by leaving records pending or emitting an explicit discard event, never silently dropping them.

Keep the count cap at 64 pending completion records. Bound model envelopes to 16 KiB each and total pending preview bytes to 256 KiB. Each record must fit the 32 KiB drain budget. Keep full canonical outputs behind retained result lookup and offload large payloads using the existing offload store before actor locking. Preserve valid JSON, IDs/status/error summaries, explicit truncation, and a retrievable full-result reference. If offload fails, emit a bounded preview with an explicit retrieval-unavailable diagnostic; never grow the envelope without limit. Do not retain full unbounded result clones in pending/event preview records. Overflow is observable rejection, never silent oldest-item eviction.

Retain stable per-record identity in delivery events. Replace required synthetic `parent_turn_id` on acceptance with acceptance independent of execution; consumption carries the actual optional/shared parent turn ID. Keep rejected/discarded reasons explicit and update all Rust consumers/tests. No new Lua subscription API is needed. Existing start/end callbacks represent real executions only; injected records do not fabricate lifecycle events. TUI presentation uses typed consumption metadata for a bounded notice instead of parsing a synthetic user input's JSON. Render exactly one child-attributed notice per consumed record, no ordinary user bubble, and no notice for a record withdrawn before consumption.

Closing a child does not acknowledge already-completed records in its parent. Owner-induced close/shutdown cancellations retain current report suppression. Parent cancellation stops its active execution without falsely consuming pending records; explicit clear/close accounts for discarded records. Plugin revocation and unavailable parents retain authority/lifetime failure behavior. Persistence and restart-safe consumption remain later PR6 work.

### 7. Update contracts and verification evidence

Update task descriptions, Lua method metadata, existing migration notes, existing plugin-authoring documentation, and generated tool/Lua references. State same-ID busy sends, fresh IDs after completion, exact wait scope, next-request delivery, non-preemption, bounded rejection, and batched automatic completion behavior. Update obsolete distinct-turn tests rather than leaving contradictory compatibility assertions.

Document the supported-host boundary explicitly. ACP root concurrent prompts still reject as before; shared child actors use `submit`. Do not claim universal ACP root steering or durable inboxes. The task tool names and JSON shapes remain unchanged, but send scheduling semantics intentionally change.

## Acceptance Criteria

- AC.1 A message submitted through the production TUI adapter during an ordinary user turn appears in the next eligible provider request with no new turn, and every user bubble appears exactly once. Tests: `tui_queue_adapter_folds_into_initial_user_turn`, `folded_tui_inputs_render_once_with_images`.
- AC.2 Busy and pending-compatible child sends return the existing turn ID; idle or admission-closed sends return a new ID. Exact FIFO work remains separate and admission boundaries are respected. Tests: `task_submit_busy_and_idle_turn_ids`, `submit_pending_work_and_configuration_boundaries`, `submit_during_delivery_continuation_starts_work`.
- AC.3 No successful turn settles with accepted instructions omitted from provider requests, including the final-poll race. Cancellation/failure/budget paths terminate observably and never reopen results. Tests: `submit_races_finish_on_both_sides`, `accepted_instruction_request_coverage`, `steered_turn_terminal_failure_cases`.
- AC.4 Reused tickets remain bounded by unique IDs, exact waits retain their target and permit safety, and each execution publishes usage/lifecycle once. Tests: `repeated_busy_sends_retain_one_ticket`, `task_wait_same_turn_includes_later_send`, `task_wait_does_not_follow_successor`, `steered_nested_wait_capacity_one`, `steered_turn_usage_and_events_once`.
- AC.5 A revised task cannot complete with a stale structured/local final result. Tests: `steering_requires_fresh_structured_report`, `steering_failure_does_not_project_stale_capture`.
- AC.6 Both TUI and Lua active parent backends receive child reports after tool results and before the next request without waiting for the parent turn to end. Tests: `active_parent_consumes_child_batch_after_tools` parameterized by backend, `nested_lua_parent_receives_completions_mid_turn`.
- AC.7 Pending idle reports schedule one bounded continuation; distinct records survive, explicit work is not starved, late arrivals remain accounted for, and notification-only continuations do not echo upstream. Failed continuations preserve unconsumed records without automatic retry loops and recover on explicit work or successful configuration change. Tests: `idle_child_batch_uses_one_continuation`, `delivery_arrival_during_finalization`, `delivery_budget_preserves_fifo_and_explicit_work`, `nested_delivery_continuations_do_not_echo`, `failed_delivery_continuation_waits_for_recovery_trigger`.
- AC.8 Exact waits and automatic consumption race safely; read/timeout do not acknowledge, child close does not erase prior completed reports, and consumed records are not requeued after provider failure. Tests: `wait_and_automatic_delivery_claim_race`, `delivery_read_timeout_and_close_cases`, `consumed_delivery_provider_failure_not_requeued`, `withdrawn_idle_batch_makes_no_model_call`.
- AC.9 Input/delivery bounds, deduplication, failure reporting, and authority remain enforced without unbounded previews or silent eviction. Tests: `instruction_inbox_limits_are_nonfatal`, `completion_preview_and_pending_byte_bounds`, `completion_identity_deduplicates_only_same_turn`, `steering_authority_and_lifecycle_cases`, `parent_inbox_cancel_clear_and_close_cases`.
- AC.10 Focused subagent TUI input uses same-turn delivery; unchanged exact Lua APIs and legacy host-root boundaries still satisfy their documented contracts. Tests: `subagent_tab_input_steers_active_turn`, `exact_enqueue_remains_distinct_fifo`, `non_actor_host_root_operation_matrix_after_submit`.
- AC.11 TUI completion consumption renders one bounded notice per record with correct child attribution, no user bubble, and no notice after exact-wait withdrawal. Test: `typed_completion_notices_render_once_and_skip_withdrawn`.
- AC.12 Public descriptions/generated references match the changed behavior and repository checks pass. Tests/checks: `task_docs_describe_same_turn_submission`, `agent_submit_docs_and_delivery_event_contracts`, `generated_docs_current` (`just gen-docs-check`), `fmt_check` (`just fmt-check`), `workspace_ci` (`.ssh/remote-ci.sh`).

## Test Strategy

Every named test above is required new coverage or a deliberate update of an existing scenario, not a claim that the current test already verifies the change. Map test names to ACs as listed. Use `#[test_case]` for boundary/failure variants and shared constants for messages and diagnostics.

Actor state-machine unit tests cover target selection, finish admission, pending grouping, queue barriers, deduplication, bounded drains, and cancellation cuts. Add deterministic gates to the real state transitions. Test their two-sided control using `submit_races_finish_on_both_sides` and `delivery_arrival_during_finalization`; these also prove the new race-test seams reach the intended production paths.

Integration tests in `maki-lua/tests/{task_policy,agent_handles,managed_session}.rs` load the real bundled task plugin and fake providers. Assert provider request contents, original turn identity, immutable returned results, exact wait responses, structured-output freshness, sender authority, capacity-one progress, and event/usage counts. Use actual provider/request loops rather than only `ScriptedBackend::poll`. Add the missing fixture controls in phase 1 as implementation work.

TUI tests in `maki-ui/src/agent/{shared_queue,agent_loop}.rs`, app tests, and event-loop tests submit through production adapters and assert visible bubbles/queue state and typed completion notices. Capture model requests and complete tool groups so the test fails if delivery only occurs after runtime turn completion. Standalone child tests prove the same submit behavior without inventing managed host-root support.

Bounds tests exercise multibyte text, oversized structured output, offload failure, full count/byte limits, repeated same IDs, and a large record at the FIFO head. Fairness tests keep explicit work queued while results arrive under controlled gates. Do not require a live model to obey instructions; assert the instructions reached the provider and that harness state remains valid.

No Rust builds during planning. During execution run local `just fmt-check`/`just fmt`, scoped `cargo check -p <crate> --tests` and nextest runs on the remote build box, then `.ssh/remote-ci.sh` for final `just ci`. Follow `AGENTS.local.md` and inspect the existing remote script before use; do not run compute-heavy builds locally or commit SSH state. Regenerate docs remotely before the final drift check. Record commands, revision, and results in implementation handoff evidence, not as preclaimed plan verification.

## Review Strategy

Before approval, invoke `plan_reviewer` on this file with the original user intent and the shared `plugins/lib/maki/plan_spec.lua` contract. The operator explicitly permits a read-only research subagent running the repository's `PLAN_REVIEWER_PROMPT` when the dedicated reviewer is unavailable outside plan mode. Apply the same severity rules and test-coverage audit in that fallback. Fix every finding or explicitly rebut it to the operator. Repeat review after any critical/high findings until none remain.

Plan review record, 2026-10-16: the operator-approved read-only fallback applied the repository reviewer prompt and shared specification. Pass 1 returned `VERDICT: pass` with two medium findings: missing explicit typed-notice rendering coverage and unspecified failed-continuation recovery. Both were fixed in the plan. Pass 2 returned no findings and `VERDICT: pass`. This records plan review only; no implementation, test execution, or operator implementation approval is claimed.

After implementation and all automatable checks, load the repository `code-review` guidance and run independent review. Focus on admission-close atomicity before terminal publication, lock ordering, failed/budgeted request coverage, exact-wait claim races, authority/configuration boundaries, payload bounds, no-echo behavior, and production TUI/Lua parity. Fix or explicitly rebut every finding. Repeat after critical findings until none remain or request an operator decision. Review documentation and changed tests for residual distinct-turn claims.

## Documentation Strategy

Edit existing API metadata, `maki-lua/src/docs_render.rs`, bundled task descriptions, plugin-authoring reference sources, and relevant existing user docs. Regenerate `site/docs` references through `maki-docgen`; do not hand-edit generated output. Explain acceptance versus processing, same-turn waits, idle successor behavior, child reports at turn end, active delivery timing, and host boundaries. Update existing issue/delivery tracking only after verified implementation evidence. No additional documentation file is required beyond this requested plan.

## Risks, Blockers, and Required Decisions

Resolved product decisions: busy child input joins the current open work turn and returns that ID; idle input starts work; child completion occurs at turn end; completion reports reach active parents at safe request boundaries; idle reports batch; exact task tool JSON shapes stay unchanged; no sibling or child-initiated messaging feature is added.

Execution must preserve the distinction between instruction coverage by a ticket and completion-observation delivery. A late completion can use a successor; a targeted instruction cannot be acknowledged under a ticket that finishes without submitting it normally. Semantic compliance remains the model's responsibility.

Messages can arrive too late to prevent actions, and blocked tools delay steering. These are documented limits, not reasons to manufacture immediate interruption. Continuous instructions can prolong an unresolved turn; existing execution budgets remain enforced and exhaustion is explicit.

The principal technical risk is the finish handshake crossing `Agent::run` terminal publication and actor retirement. A post-seal check is insufficient. Replace the incorrect per-result delivery-turn structure rather than layering a shadow queue around it. Keep narrow typed instruction/completion sources; do not use the lossy generic `SessionMailbox` as a substitute.

The selected preview limits are implementation defaults, not new user settings. Canonical retained outcomes and full-result references remain independent of previews. In-memory consumption does not claim restart-safe exactly-once delivery. No unresolved product decision blocks this scoped implementation; broader ACP root migration and persistence remain explicitly excluded.

## Implementation verification, 2026-10-16

Implemented in the working tree based on `31fb678f4f1253a1fd30bec4725a2e685809a055`; no commit created. Subagents used initial assignments and exact waits, without task_send coordination.

`.ssh/remote-ci.sh` passed against the final source tree. Nextest run `85d8187c-99a1-43f4-ac3f-a87d06feb6fb` passed all 7,211 tests across 45 binaries. Rust/Lua formatting, workspace Clippy with warnings denied, ruff, ty, generated-document drift checks, and cargo-machete passed. Documentation was generated on the remote build box and copied back. Local `just fmt-check` was unavailable because `just` is not installed; its component checks passed remotely. Rust builds ran remotely.

Independent reviews identified and prompted fixes for unsuccessful admission closure, completion claim/history atomicity, pending operation boundary revalidation, initial instruction accounting, prepared-turn inbox registration, offloading, TUI presentation startup, and deferred-input preparation timing. The final review found no critical/high defect. Its pending-constraint finding referred to an earlier snapshot: constrained joins now return `PolicyPending` until the target policy is available, rather than silently dropping a joining constraint.

Coverage limitation: the full set of named acceptance scenarios above is not independently implemented one-for-one. In particular, actor finish tests exercise the handshake directly and model-loop race coverage uses a controlled inbox; a deterministic gate spanning the real actor and final model-loop handshake remains follow-up coverage. Passing workspace CI does not claim that stronger interleaving test exists.
