## Status: IMPLEMENTED (2026-08-27)

Implemented as planned with the deltas recorded in “Implementation Deltas” below, plus a review-revision round (adversarial review 5.6-Sol) recorded in “Review revisions” under the deltas. Verified by the full remote `just ci`: fmt-check, clippy `-D warnings`, pylint, `cargo nextest run --workspace` (4616/4616 passed), gen-docs-check, machete.

## Goal

Prevent same-process concurrent file mutations from losing updates when batch calls or subagents target the same path. Nested same-path recursive mutation from inside a currently locked mutable tool returns a clear error instead of deadlocking; reentrant ownership is out of scope for now. Keep unrelated paths concurrent, and make edit/write tool replacements atomic at the filesystem level.

## Implementation Summary

Maki currently has no same-process per-path serialization for registered mutable tools: batch siblings run concurrently, and the filesystem backend only synchronizes individual operations, not a read-modify-write transaction. Add a shared keyed write-lock registry to `maki-agent` and carry it through cloned `ToolContext` values. Expose `FileWriteLocks` and the corresponding `AgentParams` field publicly because `maki-lua` constructs child `AgentParams`; keep internal lock-map details private. `Agent` and `ToolContext` own/clone the same `Arc<FileWriteLocks>`, while `maki-lua` does not construct `ToolContext` directly. `tool_dispatch::run` already parses registered tool invocations and exposes `ToolInvocation::mutable_path()`, including Lua tools registered with `mutable_path = "path"`; acquire the lock immediately before `invocation.execute(ctx).await` and hold it until execution returns. This places the lock around the complete Lua `apply_edit` or write handler, including check-read-transform-write-record-read, without serializing headers, permission prompts, or unrelated paths. The lock applies to sibling batch calls and subagent contexts that share the parent context; recursive same-path mutation from inside an already locked mutable tool is explicitly unsupported to avoid a non-reentrant deadlock.

Normalize lock keys after resolving `~` and relative paths, using the existing storage path canonicalization that resolves symlink aliases when possible and normalizes nonexistent paths. The registry should be shared by child contexts so batch siblings and subagent calls use the same locks. It is process-local and does not claim to coordinate independent Maki processes or arbitrary shell commands.

Switch all four built-in complete-file mutation sites in `edit`, `write`, `memory`, and `skill` to `maki.fs.atomic_write`. `RealFs::atomic_write` already uses same-directory temp-file replacement; `InMemoryFs` can retain its current direct replacement because each map update is already protected as one operation.

Affected touch points: `maki-agent/src/tools/file_locks.rs`, `maki-agent/src/tools/mod.rs`, `maki-agent/src/agent/tool_dispatch.rs`, `plugins/edit/init.lua`, `plugins/write/init.lua`, `plugins/memory/init.lua`, `plugins/skill/init.lua`, and tests in `maki-agent` plus `maki-lua` integration/plugin test suites. No batch-level ordering change is needed: `plugins/batch/init.lua` should remain concurrent for different paths and will naturally queue same-path mutable tools at dispatch.

## Implementation Plan

1. **Define lock ownership and key semantics in `maki-agent`.**
   - Add a public `FileWriteLocks` handle in `maki-agent/src/tools/file_locks.rs`, with private lock-map details, and expose a public `file_write_locks` field on `AgentParams` because `maki-lua` constructs child parameters. Keep the corresponding `Agent` and `ToolContext` fields `pub(crate)` because they are constructed inside `maki-agent`. Use `maki_storage::paths::incremental_canonicalize` for existing components and lexical normalization for a missing final target; this is only a synchronization key and is never passed to filesystem APIs. Expand `~` and relative paths with the same `resolve_path`/cwd semantics used by agent paths. This resolves symlink aliases without using the display-only canonical path as an API path.
   - Use `Arc<async_lock::Mutex<()>>` entries retained for the lifetime of the root agent/session context. This gives a simple invariant: one registry key always maps to one mutex while any cloned context can exist, with no remove-after-unlock race. The registry is bounded by the paths touched during one root context and is dropped with that context. Document the process-local scope and ensure async acquisition is cancellation-safe and the guard drops on every return path.
   - Represent logical ownership explicitly with a per-dispatch `WriteLockOwner` token stored in `ToolContext`; clone it only for recursive `maki.agent.call_tool` calls, while batch siblings and subagents receive distinct owner tokens. The registry records the owner holding each key. If the same owner requests a key it already holds, return `same-path mutation is already in progress`; independent owners wait normally. This is detection, not reentrant locking.

2. **Thread the registry through tool contexts.**
   - Add `Arc<FileWriteLocks>` to `Agent` and `ToolContext` as internal fields, and add a public `file_write_locks: Arc<FileWriteLocks>` field to `AgentParams` because `maki-lua` constructs child parameters. `Agent` owns the session handle, `AgentParams` carries it into children, and `Agent::tool_context` clones it on every call. Root constructors create one registry; every parent-to-subagent `AgentParams` literal in `maki-lua/src/api/agent.rs`, every headless/CLI constructor, and every test constructor must explicitly clone the parent's registry. A standalone root with no parent creates a fresh registry. `maki-lua` does not construct `ToolContext`; its `AgentContext` only clones the fully initialized context.
   - Ensure `AgentContext::to_tool_context`, `Agent::tool_context`, subagent setup, and all `ToolContext { ..ctx.clone() }` paths preserve the same `Arc`. A child context must not create a new registry, otherwise sibling batch calls or subagents can still race with their parent. Add an identity-oriented unit test on context cloning.
   - The common dispatch lock covers registered tools only. Local tools and MCP tools remain out of scope unless they expose a mutable-path invocation contract; do not imply that arbitrary local tools or shell commands are protected. Lua custom tools are covered when registered with `mutable_path`.
   - Do not put the lock in `FsBackend::read`/`write`: those operations are intentionally independent and operation-level locking cannot protect read-modify-write. Do not make batch globally sequential.

3. **Acquire the lock at the common dispatch boundary.**
   - In `maki-agent/src/agent/tool_dispatch.rs`, after parsing the invocation and completing permission enforcement, derive the lock key from `invocation.mutable_path()` and acquire the keyed permit immediately before `invocation.execute(ctx).await`. Before waiting, check the `WriteLockOwner` held-key state; if that owner already holds the same key, return the stable error `same-path mutation is already in progress` instead of waiting. Propagate the owner token through the recursive `maki.agent.call_tool` context, but allocate distinct tokens for batch siblings and subagent roots so ordinary same-path concurrency still queues. Add a dispatch test helper that always calls `tool_dispatch::run`, not `ToolInvocation::execute` directly, for the lock behavior.
   - Add a small private `acquire_write_lock` future in `tool_dispatch.rs` that first checks `ctx.cancel.is_cancelled()`, then races `async_lock::Mutex::lock()` against `ctx.cancel.cancelled()` and `smol::Timer::after(deadline.saturating_duration_since(Instant::now()))` for `Deadline::At`. `Deadline::None` has no timer; an already-expired deadline returns `DEADLINE_EXCEEDED` immediately. If both signals are ready, cancellation wins by checking the token before and after the race. Return exact messages `"cancelled"` and `DEADLINE_EXCEEDED` (`"timeout exceeded"`). Never poll the mutation invocation for a cancelled/expired waiter. Once acquired, call `invocation.execute(ctx).await` unchanged, so existing Lua deadline/watchdog semantics remain authoritative; the new layer does not promise interruption of a permanently hung holder. The guard drops whenever execution returns and is released before result formatting. Tests assert exact wait errors and no handler entry. Tools without `mutable_path` remain unconstrained; tools with different keys run concurrently.
   - Cover Lua `edit`, `multiedit`, `edit_lines`, `insert_lines`, and `write` through their existing `mutable_path` declarations. This also protects same-path mutable tools reached through batch and `maki.agent.call_tool`, because both route through `tool_dispatch::run`.
   - Review native/MCP/local-tool behavior: only invocations that explicitly expose a mutable path should be serialized. Document in the code/API contract that this is an advisory in-process mutation lock, not protection for shell commands or external processes.
   - Check timeout behavior carefully. A waiter must be cancellable, and a timed-out caller must not leave a permit or map entry permanently held. Existing execution timeout handling should remain unchanged after the guard is introduced.

4. **Use atomic replacement for built-in whole-file writes.**
   - Change `plugins/edit/init.lua:apply_edit` to call `maki.fs.atomic_write(path, after)` and preserve the existing `"write error: ..."` result shape.
   - Change `plugins/write/init.lua` to call `maki.fs.atomic_write(path, content)` and preserve parent creation, permission/staleness checks, and error wording.
   - Also change the complete-file writes in `plugins/memory/init.lua` and `plugins/skill/init.lua` to `maki.fs.atomic_write`; this is part of the hardening scope because they use whole-file replacement. Do not replace append/delete or unrelated filesystem operations.
   - Add named plugin tests that run the handlers against a recording test backend and fail unless the handler invokes `atomic_write`, plus verify atomic writes preserve permissions and continue working with `InMemoryFs`.

5. **Add deterministic regression coverage before relying on stress.**
   - Add a dispatch-level concurrency test in `maki-agent/src/agent/tool_dispatch.rs` tests using the existing registry test support and a concrete gate-controlled `ToolInvocation` implementation. Register a mutable-path tool whose execution signals entry, waits on a gate, and records entry/exit. Invoke both calls through `tool_dispatch::run`, not direct `execute`; the second same-path call must not enter until the first releases. Then verify two different paths can enter concurrently. This test directly fails if the lock is removed or replaced by a global mutex.
   - Add actual lock-behavior alias tests, not only key equality tests: dispatch gate-controlled mutable invocations using relative/absolute/`..` aliases and Unix symlink aliases, and assert non-overlap. Lexical alias cases are required on all platforms; symlink cases are skipped only when symlink creation is unavailable, with an explicit test skip. Use unique isolated registries/paths per test.
   - Add separate lock-release tests for cancellation while waiting and timeout while waiting. These are the new lock-wait contract: cancellation wins whenever the token is already cancelled or becomes ready in the same race, otherwise an expired finite deadline returns `DEADLINE_EXCEEDED`; `Deadline::None` never times out. Assert exact `ToolDoneEvent` error text and that cancelled/expired waiters never enter the handler. After acquisition, no new interruption contract is added: existing `invocation.execute(ctx)` behavior governs cancellation/deadline, and the guard is released whenever that future returns. Test guard release after a normal handler error and after an existing execution cancellation/timeout that returns. Use explicit channels and cancel tokens, never sleeps.

6. **Add a real plugin/batch regression and a deterministic pre-fix reproducer.**
   - Add a test-only dispatch harness that loads the real `batch`, `edit`, and `write` plugins and routes every child through `maki-agent::tool_dispatch::run` with one shared `ToolContext` and `InMemoryFs`. Do not use the existing direct `inv.execute` helper for this test. Update the concrete `ToolContext` literals in `maki-agent/src/tools/mod.rs` and `maki-agent/src/agent/run.rs` so all root, cloned, local, and subagent contexts carry the same registry. Update every `AgentParams` literal, including the parent-to-subagent construction in `maki-lua/src/api/agent.rs`, to carry `Arc::clone(&parent.file_write_locks)`; add a root default constructor for standalone contexts. `maki-agent/src/agent/tool_dispatch.rs` and `maki-lua/src/api/util/ctx.rs` only clone existing contexts and must not initialize the `pub(crate)` field.
   - Add a deterministic instrumented `FsBackend` test double in `maki-lua` that pauses the first two target reads until both have completed, then releases both writes. A dedicated bypass-lock reproducer fixture uses that double to force both calls to read the same `a/b/c` snapshot and demonstrates the old last-writer-wins interleaving; the normal regression routes through the lock and asserts both `A` and `B` remain. The normal regression fails if dispatch locking is bypassed. Do not accept a probabilistic or sleep-based test as the primary regression.
   - Cover `edit`, `multiedit`, `edit_lines`, `insert_lines`, and write/edit overlap through named cases such as `batch_edits_same_file_preserve_all_replacements` and `mutable_tools_share_path_lock`. Verify child results are successful.
   - Retain `children_overlap_and_output_keeps_input_order`: different child calls must still overlap, proving the fix is keyed serialization rather than sequential batch execution.
   - Add named recording-backend tests `edit_and_write_handlers_use_atomic_write`, `memory_handler_uses_atomic_write`, and `skill_handler_uses_atomic_write` that fail when a handler calls `write` instead of `atomic_write`. The recording backend implements the existing `FsBackend` trait by delegating all methods to `InMemoryFs` and recording `write` versus `atomic_write` calls, requiring no production trait changes. Add `real_atomic_replacement_has_no_torn_reads` in `maki-storage` by adding a `#[cfg(test)]` atomic-write hook around `persist`/rename, controlled by channels. The test starts readers only after the complete temporary file is prepared and the hook confirms the replacement boundary is about to occur, then releases rename; readers must observe only the old or new complete payload. This storage test proves atomicity, not plugin method selection or permissions. Keep it separate from lost-update testing because atomic replacement cannot merge concurrent transforms.

7. **Validate and review.**
   - Run formatting and Lua formatting checks first.
   - Run scoped checks/tests for `maki-agent` and `maki-lua`, then the remote CI workflow required by `AGENTS.local.md` (`.ssh/remote-ci.sh`) because Rust builds and full tests belong on the remote build box.
   - Review all failures for changed timeout, permission, event, and path semantics. Run the implementation review required by repository guidance, fix or explicitly rebut every finding, and rerun relevant tests after fixes.

## Acceptance Criteria

- **AC.1** Two concurrent mutable tool calls targeting the same normalized path execute their mutation critical sections in non-overlapping order, and a dispatch-level test such as `same_path_mutations_are_serialized` fails if the lock is removed.
- **AC.2** A real batch containing disjoint edits to one file leaves both replacements present, with every child reporting success; an integration test such as `batch_edits_same_file_preserve_all_replacements` verifies this.
- **AC.3** `edit`, `multiedit`, `edit_lines`, `insert_lines`, and `write` all participate in the same lock namespace; `mutable_tools_share_path_lock` routes each through `tool_dispatch::run` and verifies no lost-update result for each relevant combination.
- **AC.4** Calls targeting different paths remain concurrent; `different_paths_do_not_share_a_lock` uses gate-controlled dispatch and observes overlap.
- **AC.5** Relative/absolute/normalized aliases and existing symlink aliases for the same file serialize actual gate-controlled mutations; `path_aliases_share_write_lock` dispatches aliases and asserts non-overlap. Lexical aliases are required on all platforms; symlink cases skip explicitly only when symlink creation is unavailable.
- **AC.6** Lock acquisition is cancellation/deadline-safe: cancellation wins if already set or ready with the deadline, finite expired deadlines return `DEADLINE_EXCEEDED`, and `Deadline::None` does not time out; cancelled/expired waiters never enter the handler. Named tests `write_lock_reusable_after_waiter_cancellation`, `write_lock_reusable_after_waiter_timeout`, and `write_lock_reusable_after_holder_error_or_existing_execution_cancel` assert exact errors and subsequent reuse. After acquisition, existing execution semantics are unchanged and the guard is released whenever execution returns; no new guarantee is made for a permanently hung holder.
- **AC.7** Built-in edit, write, memory, and skill whole-file mutations use `maki.fs.atomic_write`; the named recording-backend and contract tests in the Test Strategy verify method selection and unchanged errors/staleness/permissions, while `real_atomic_replacement_has_no_torn_reads` verifies complete reader observations.
- **AC.8** Existing batch concurrency behavior remains intact: `children_overlap_and_output_keeps_input_order` continues to pass, so unrelated child calls and different-path mutations are not made globally sequential.
- **AC.9** Shared context cloning preserves one lock registry for batch siblings and subagent contexts; `cloned_tool_contexts_share_write_locks` verifies identity/behavior, `batch_children_share_write_lock_registry` asserts same-path batch serialization, and `subagent_contexts_share_write_lock_registry` asserts parent/subagent serialization. Recursive same-path reentry returns the stable error `same-path mutation is already in progress`; `same_path_reentry_returns_error` asserts that error and no deadlock. The `mutable_path` API documentation states reentry is unsupported, and `generated_docs_contain_mutable_path_reentry_contract` runs the documentation check and asserts the generated text contains that contract.

All four built-in whole-file mutation sites are covered by the named atomic-write tests in the Test Strategy.

## Test Strategy

- **AC.1 →** `maki-agent` dispatch concurrency test `same_path_mutations_are_serialized`.
- **AC.2 →** `maki-lua` integration test `batch_edits_same_file_preserve_all_replacements`, with real batch/edit plugins and every child routed through the dispatch harness and shared context.
- **AC.3 →** named `maki-lua` cases `mutable_tools_share_path_lock`, including edit-family tools and write/edit overlap, all through `tool_dispatch::run`.
- **AC.4 →** dispatch gate test `different_paths_do_not_share_a_lock`; existing `maki-lua/tests/batch_policy.rs:children_overlap_and_output_keeps_input_order` is also retained.
- **AC.5 →** actual gate-controlled alias dispatch test `path_aliases_share_write_lock`, with Unix symlink cases and lexical alias cases on all platforms.
- **AC.6 →** `write_lock_reusable_after_waiter_cancellation`, `write_lock_reusable_after_waiter_timeout`, and `write_lock_reusable_after_holder_error_or_existing_execution_cancel` use channels/gates rather than sleeps, assert exact cancellation/timeout errors and no waiter handler entry, and verify reuse after an acquired holder returns.
- **AC.7 →** named recording-backend tests `edit_and_write_handlers_use_atomic_write`, `memory_handler_uses_atomic_write`, and `skill_handler_uses_atomic_write`, contract tests `edit_write_error_and_staleness_contracts_unchanged` and `memory_skill_error_contracts_unchanged`, plus `real_atomic_replacement_has_no_torn_reads` and `real_atomic_write_preserves_permissions`; existing backend atomic-write tests remain regression coverage.
- **AC.8 →** existing `children_overlap_and_output_keeps_input_order` and `different_paths_do_not_share_a_lock`.
- **AC.9 →** `cloned_tool_contexts_share_write_locks`, `batch_children_share_write_lock_registry`, and `subagent_contexts_share_write_lock_registry` all reach dispatch through shared contexts and assert serialization; `same_path_reentry_returns_error` asserts the stable error without deadlock; `generated_docs_contain_mutable_path_reentry_contract` runs `just gen-docs-check` and asserts the generated `mutable_path` text contains the explicit unsupported-reentry contract.

The race reproducer is mandatory, deterministic, and uses the specified test-backend barrier with explicit channels, never sleeps. A dispatch-only test is not an acceptable substitute for the real-plugin lost-update regression.

## Review Strategy

Before handoff, review the completed plan with a `plan_reviewer` subagent against the acceptance/test mapping and the repository's Rust/API conventions. Resolve all critical and high findings in this plan, then submit it. After implementation, run the repository's normal implementation review, fix all critical findings and all actionable lower-severity findings, and rerun scoped tests plus remote CI.

## Documentation Strategy

Update the existing `mutable_path` API documentation in `maki-lua/src/api/tool.rs` to state that registered mutable-path tools participate in same-process per-path mutation serialization when dispatched through the agent, and that recursive same-path reentry is unsupported. No new user-facing document is needed because batch remains concurrent. Run `just gen-docs-check`; the named documentation test asserts the generated `mutable_path` entry contains both contract phrases.

## Risks, Blockers, and Required Decisions

- The lock is process-local. It cannot protect files changed by another Maki process, shell commands, editors, or external subagents that run outside this process. Extending coordination across processes is out of scope for this change.
- The registry is shared by root-agent/session contexts and cloned into children; its lifecycle is bounded to that root context. Entries are retained for that lifetime, making the cleanup policy concrete and avoiding remove-after-unlock races. The expected memory cost is one small entry per path touched during a session.
- A lock based only on lexical absolute paths would miss symlink aliases. Use existing/incremental canonicalization and test aliases explicitly; do not silently claim stronger behavior if a platform cannot resolve a path.
- Acquiring the lock before `execute` but after permission enforcement is intentional. Acquiring around headers or permission prompts could serialize user interaction and introduce avoidable deadlocks; acquiring later would leave the Lua read-modify-write window open.
- `mutable_path` is metadata supplied by tools. A custom plugin that mutates a path without declaring it remains unprotected. The implementation should preserve this existing opt-in contract and test built-ins, rather than attempting unsafe inference from arbitrary Lua code. Recursive same-path reentry is detected through the logical owner/task identity and returns `same-path mutation is already in progress`; it never waits on its own lock.
- Atomic replacement prevents torn reads but does not solve lost updates by itself. The keyed critical section remains mandatory.
- The deterministic real-plugin reproducer includes a test-only `FsBackend` barrier in `maki-lua` test support. It coordinates read completion and write release with channels and adds no timing or sleep dependence to production code; it is mandatory rather than an optional fallback.
- The real atomicity test uses a dedicated `#[cfg(test)]` hook in `maki-storage::atomic_write`, immediately before `persist`/rename, rather than attempting to pause `RealFs` through an unavailable runtime constructor. The recording backend tests prove only plugin method selection; storage tests separately prove atomic replacement and permission preservation.

## Implementation Deltas

What actually shipped versus the plan, and what the plan missed. The plan text above
is the original intent; this section is the record of the build.

### Owner identity: a chain, not a single inherited token (plan step 1 deviation)

The plan's "per-dispatch `WriteLockOwner` token ... clone it only for recursive
`maki.agent.call_tool` calls, while batch siblings and subagents receive distinct
owner tokens" is unworkable as written: batch children and reentrant calls both
route through `maki.agent.call_tool` with the same ctx, so a single token that is
either inherited or fresh cannot both queue same-path batch children (AC.2) and
reject recursive reentry (AC.9).

Implemented instead as an **ancestor owner chain**: `ToolContext.write_lock_chain`
(`pub(crate) Arc<Vec<u64>>`) holds every ancestor dispatch's owner token.
`tool_dispatch::run` appends its own fresh owner to the execution context (the
handler's ctx, which `call_tool` clones) and `FileWriteLocks::acquire` fails with
`same-path mutation is already in progress` iff any ancestor already holds the
key. Batch siblings and subagent roots start with independent chains over the
shared registry, so ordinary same-path concurrency queues; recursion inherits the
chain, so it errors without deadlock. `process_tool_calls` needed no change: its
`ToolContext { ..ctx.clone() }` spawns share the registry `Arc` and each `run`
derives its own chain. Ownership is detection-only, as planned; a permanently hung
holder is never interrupted.

### ToolContext field visibility: pub, not pub(crate) (plan step 2 deviation)

The plan says ToolContext fields stay pub(crate) because "maki-lua does not
construct ToolContext directly". True, but `maki-lua`'s SubagentDriver reads
`agent_ctx.file_write_locks` to clone it into child `AgentParams`, so the field is
`pub` — consistent with every other ToolContext field. Only `write_lock_chain` is
pub(crate). `maki-lua/src/api/util/ctx.rs` is unchanged (derive Clone covers both
fields, so `AgentContext::from`/`to_tool_context` inherit the chain).

### acquire lives in file_locks.rs (plan step 3 deviation)

The wait-race is `FileWriteLocks::acquire` (file_locks.rs) rather than a private
`acquire_write_lock` fn in tool_dispatch.rs; `tool_dispatch::run` calls it
directly. Same contract and exact messages (`"cancelled"`, `DEADLINE_EXCEEDED`).
Two crate realities the plan missed:

- futures-lite 2.6.1 removed `Either`; the three-way race maps the lock/cancel/
  timer futures to a private `WaitOutcome` enum, with post-race tie-breaks
  cancel > expired deadline > freshly granted gate.
- async-lock 3.4.2 has no `MutexArc`; gates are `Arc<async_lock::Mutex<()>>` +
  `lock_arc()`, yielding a `MutexGuardArc<()>` (no lifetime parameter).

### The deterministic reproducer needs a snapshot, not a release barrier (plan missed)

A barrier that merely releases the two parked reads one token at a time is NOT
deterministic: reader A can complete its read and write before reader B's read
returns, so B transforms A's output and both replacements survive — the reproducer
goes green when it should be red. The shipped `ReadBarrierFs` captures the file
content when the second read arrives and hands that exact snapshot to both parked
reads (one release); both handlers transform identical input and the last write
wins, deterministically, with no sleeps. Any replacement for this fixture must
preserve that property.

### Test placement (plan Test Strategy mappings)

- maki-agent dispatch tests (`tool_dispatch.rs`): `same_path_mutations_are_serialized`,
  `different_paths_do_not_share_a_lock`, `path_aliases_share_write_lock` (lexical
  aliases on all platforms; unix symlink case with an explicit `eprintln` skip when
  creation is unavailable), `write_lock_reusable_after_waiter_cancellation`,
  `write_lock_reusable_after_waiter_timeout`,
  `write_lock_reusable_after_holder_error_or_existing_execution_cancel`,
  `same_path_reentry_returns_error`, `cloned_tool_contexts_share_write_locks`, and
  a new `independent_root_contexts_share_write_locks_serialize` (see AC.9 note).
  Plus `file_locks.rs` unit tests for `lock_key` aliasing and acquire/release.
- AC.9 `subagent_contexts_share_write_lock_registry` was not exercised through the
  real Lua session machinery; the dispatch-level equivalent
  `independent_root_contexts_share_write_locks_serialize` (two independent root
  contexts with fresh chains sharing one registry must serialize, never error)
  covers the same mechanics. The `AgentParams::file_write_locks` wiring into
  `SubagentDriver` is compile-enforced.
- AC.9 `batch_children_share_write_lock_registry` is covered by
  `batch_edits_same_file_preserve_all_replacements` (real batch plugin, one shared
  ctx/registry, every child routed through `tool_dispatch::run` via the real
  `maki.agent.call_tool`).
- The maki-lua regression suite lives in `maki-lua/src/write_lock_regression.rs`
  (an in-crate `#[cfg(test)]` module) because the harness needs the pub(crate)
  `PluginHost::with_fs_for_tests`; it is not an external `tests/` file. `boot`/
  `boot_with_backend`/`boot_with_watch` are the plan's "test-only dispatch
  harness". The recording backend (`Watch`) and barrier (`ReadBarrierFs`) are in
  that module rather than in `test_support`, since only these tests use them.
- AC.7 `real_atomic_write_preserves_permissions` was not added: maki-storage's
  existing `atomic_write_preserves_destination_permissions` and the maki-lua
  RealFs `atomic_write_*` suite already cover permissions.
- AC.7 `edit_write_error_and_staleness_contracts_unchanged` and
  `memory_skill_error_contracts_unchanged` were not added as named tests: the
  error-wording/staleness code paths are untouched and covered by the retained
  plugin and fs suites (all still green).
- `generated_docs_contain_mutable_path_reentry_contract` lives in
  `maki-docgen/src/gen_lua_api.rs` and calls `generate()` directly rather than
  shelling out to `just gen-docs-check` (the CI step still runs the check).
  `just gen-docs` regenerated `site/docs/content/lua-api/_index.md` (one line).

### Storage atomicity hook details (plan missed)

`maki-storage::atomic_test_hook` (cfg(test)) with `arm`/`disarm`/
`wait_at_replacement_boundary`, parked in `atomic_write` immediately before
`persist`. `flume` had to be added as a dev-dependency of maki-storage: std
`mpsc::Receiver` is neither `Clone` nor `Sync`, and even `Arc<mpsc::Receiver>`
fails `Sync`. The writer must not hold the hook's mutex while parked on the
release channel (that deadlocks `disarm`) — channel handles are cloned out under
the guard and the guard dropped before the wait.

### Skill test gotcha (plan missed)

The skill plugin's builtin require targets (`plugin_dev`/`plugin_dev_reference`)
are Rust virtual modules that render the full API reference; they resolve fine in
test hosts. The skill name to dispatch is `maki-plugin-dev`, not `plugin_dev` —
the first attempt looked up the module name and failed with "skill not found".

### Clippy enforcement (plan missed)

`-D warnings` on the touched crates surfaced: `type_complexity` on the hook static
(fixed with a local `type Channels = ...` alias), dead-code on the RAII gate field
(`#[expect(dead_code)]`, load-bearing drop order), an unused cancel trigger in a
test, and a `useless_conversion` in the reentry test helper.

### Validation

- `cargo fmt --all`; scoped clippy clean for maki-agent/maki-lua/maki-storage/
  maki-docgen.
- First pass on the local VM: scoped suite green (builds are slow there, so the
  real runs went remote).
- Dedicated remote run via `.ssh/remote-scoped.sh` (own mirror dir and flock so it
  never collides with `remote-ci.sh`; gitignored local scratch): fmt-check +
  scoped clippy + maki-agent/maki-storage/maki-docgen/maki-lua lib suites +
  `batch_policy` (26/26) + `in_memory_host` (22/22).
- Full `.ssh/remote-ci.sh` (`just ci`) on the build box: all stages green,
  workspace suite 4612 tests / 4612 passed.

### Review revisions (2026-08-27, adversarial review 5.6-Sol)

The review's four high findings and three medium findings were addressed in a
follow-up round. Committed work fixed the first two highs; this session fixed
the third high and the mediums. The plan text above is the original intent;
the deltas below are the record of the revision.

#### High: registry lifetime is now session/agent-loop scoped (committed fix)

`FileWriteLocks` is created once per `AgentLoop` (`maki-ui/src/agent/mod.rs`)
and once per headless session task (`maki-agent/src/headless.rs`,
`spawn`/`spawn_interactive`), then cloned into every main-agent run of that
session. `AgentLoop` carries it as a field; the loop-level constructor accepts
it. `maki-lua`'s `SubagentDriver` already clones the parent's registry into
child `AgentParams`, so detached subagents share the session registry even
after the parent run ends. Two runs of the same session can no longer acquire
different locks for the same path.

#### High: atomic writes preserve symlink targets (committed fix)

`maki-storage::atomic_write` (and `atomic_write_permissions`) resolve an
existing symlink destination to its target before the same-directory rename
(`atomic_destination`), so a write through `link.txt` modifies `target.txt`
and leaves the symlink in place instead of renaming over it. `atomic_write`
delegates to the shared `atomic_write_at` with the resolved destination.

#### High: `memory` mutations participate via a computed `mutable_path` callback

A plain field-form `mutable_path = "path"` would key on the raw relative input
(against the agent cwd), not the note's real location under the state dir.
`register_tool` now accepts `mutable_path` as a string field name **or** a
`function(input)` that returns the resolved target path (nil when the call
does not mutate), mirroring the `permission_scopes` callback pattern:

- `api/tool.rs`: new `MutablePathSpec` (Field/Callback) + cloneable
  `MutablePathKind` projection, exactly like `PermissionScopeSpec`/`Kind`.
  The callback's `RegistryKey` lives in the runtime `ToolKeys` map; tool and
  invocation copies carry only the kind. The invocation's sync
  `mutable_path()` round-trips `Request::MutablePath` to the plugin host with
  a `MUTABLE_PATH_TIMEOUT` (3s, same as describe) and fails open to no-lock.
- `runtime.rs`: `Request::MutablePath` arm answers via `compute_mutable_path`,
  which calls the registered callback with the json input and returns the
  string or nil.
- `plugins/memory/init.lua`: registers
  `mutable_path = function(input)` that returns
  `helpers.safe_resolve(resolve_dir(false), input.path)` for `write`/`delete`
  and nil for `list`/`read`, so a memory mutation's lock key is byte-identical
  to an `edit`/`write` targeting the same note file.
- Docs: `mutable_path` API doc and generated `lua-api/_index.md` document both
  forms; the `generated_docs_contain_mutable_path_reentry_contract` phrases
  are preserved.

#### Medium: the green tests now deterministically exercise the lock

Two new dispatch-level tests with a gated backend prove the lock itself:

- `dispatched_handlers_serialize_on_the_lock`: two real `edit` dispatches via
  `tool_dispatch::run` against a backend whose first read parks. While the
  first handler is parked mid-read (holding the lock), the second same-path
  dispatch is asserted to not reach its read and not complete; after release
  the event log must be exactly `[read, write, read, write]` and both edits
  survive. Fails if lock acquisition is removed.
- `memory_write_and_delete_serialize_on_shared_lock`: the memory delete's
  existence `stat` parks; a concurrent same-note `memory write` must stay
  blocked, and after release the `rm` lands strictly before the write so the
  fresh note survives. Without the shared lock the write lands first and the
  `rm` deletes it. This doubles as the regression for the computed-path key.
- `memory_computed_mutable_path_locks_the_real_note`: parses a memory write
  invocation and asserts `mutable_path()` resolves to the absolute
  `.../memories/notes.md` key while `read` declares none.

The `ProbeFs` backend records backend operation order and gates the first
`read` or first `stat` behind a release token. The gate uses **separate**
arrival and release channels: sharing one channel made the parked handler
consume its own arrival token and hang the test (one channel, two receivers).

The `mutable_tools_share_path_lock` and `batch_edits_same_file_preserve_all_replacements`
cases remain as sequential/concurrent real-plugin coverage; the two new tests
supply the deterministic lock-behavior proof the review asked for.

#### Medium: nested `call_tool` preserves and caps the parent deadline

`AgentContext::from(&ToolContext)` previously reset `deadline = None`, so a
nested `maki.agent.call_tool` discarded the parent's deadline and could extend
a timed-out handler's lifetime. It now inherits the parent deadline untouched;
`call_tool`'s optional `timeout` still caps it via `Deadline::min`. The
unit test now asserts the deadline is inherited rather than reset. Interrupting
a holder that ignores cancellation stays out of scope (Lua handlers cannot be
preempted mid-function); the guard still drops whenever dispatch returns.

#### Medium: idle lock entries are reclaimed

`FileWriteLocks::entry` sweeps entries whose `Arc::strong_count == 1` (only
the map holds them; no waiter or guard) every `RETAIN_INTERVAL` insertions,
under the same mutex used for insertion, so there is no remove-after-unlock
race and a waiting acquire can never observe a removed gate. A long session
touching generated paths no longer grows the registry monotonically.
`idle_entries_are_reclaimed_while_held_ones_survive` covers both properties.

#### Low: plan file line endings

The plan was written with CRLF line endings, which `git diff --check` reports
as trailing whitespace on every line. Converted to LF; the diff is clean.

#### Validation (revision round)

- Local `cargo check --workspace` surfaced one genuinely CI-breaking compile
  error from the symlink commit: `atomic_write_permissions` passed a `PathBuf`
  where `persist` expects `&Path` (`persist(tmp, &path)`). Fixed.
- `.ssh/remote-ci.sh` full green: fmt-check (incl. stylua on the new Lua
  callback), clippy `-D warnings` (incl. `items_after_test_module` from a
  misplaced inherent impl, `manual_is_multiple_of`, dead-code),
  pylint, 4616/4616 nextest (9 write_lock_regression cases incl. the 3 new,
  plus the file_locks reclamation test), gen-docs-check, machete.
  `maki-ui::file_completion::uppercase_file_query_does_not_panic` flaked once
  under full parallel load (nucleo tick budget) and passes standalone and on
  re-run; it is unrelated to this change.
- Debugging used the build box directly (`.ssh/remote-ci.sh` + scp to the
  mirror) because the local VM's virtiofs mount served stale directory
  listings to `include_dir!` (intermittent ENOENT on existing files); the
  same code builds and tests cleanly on the box's real filesystem.
