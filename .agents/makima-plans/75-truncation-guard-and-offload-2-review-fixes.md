# PR #156 review fixes and follow-ups

## Goal

Address the three requested changes and three nonblocking follow-ups in https://github.com/lun-4/makima/pull/156#pullrequestreview-5423667663. Preserve saved-output recovery, cancellation partial results, quota enforcement, and print-mode artifact cleanup while removing the reported bypasses and shared-thread blocking.

## Implementation Summary

The operator selected all six review items. The inspected checkout is `truncation-guard-offload` at `917b42a4`, the reviewed commit. Investigation used repository reads and GitHub metadata only. No builds or tests were run during planning.

Affected components:

- `maki-agent/src/agent/tool_dispatch.rs`: MCP output-hook ordering.
- `maki-agent/src/tools/offload.rs`: safe candidate comparison, combined directory scan, lifecycle closure, repeated-result advice.
- `maki-agent/src/headless.rs` and `src/print.rs`: nonblocking cleanup requests with explicit completion ownership.
- `maki-lua/src/api/util/ctx.rs`, `maki-lua/src/api/tool.rs`, and the reply-construction sites in `maki-lua/src/runtime.rs`: yielding output limiting and callback-safe deferred limit options.
- `maki-lua/src/api/fs/in_memory.rs` and backend implementations in tests, including `maki-ui/src/event_loop.rs`: updated store contract.
- `plugins/lib/maki/long_lines.lua`, `plugins/lib/maki/fuzzy_replace.lua`, `plugins/lib/maki/partial.lua`, and bundled output-limiting plugins.
- Rust/Lua regression tests, a small Criterion benchmark, generated Lua/tool references, handwritten hook/token-economy documentation, and Windows test execution.

Keep the 8 MiB saved-body cap and 256 MiB session quota. Do not add an artifact-count limit or cached quota/index that could become stale after file edits. Do not broaden MCP interception changes into a redesign of every built-in output hook. Preserve MCP passthrough without a store, host-local tool ownership of output, permission-scope truncation, file naming, private Unix permissions, deduplication of mutable artifacts, persisted-session lifetime, and same-session store identity.

## Implementation Plan

### 1. Intercept full MCP output before persistence

Current evidence: `run` applies `Hook::filter_output` after `run_inner` (`tool_dispatch.rs:125-128`), but `execute_mcp_tool` offloads inside its success branch (`:934-948`). A hook therefore receives only a preview and cannot redact already-saved text. `RecordingHook`, `answering_ctx`, and stub MCP sessions already exist in the same file's tests.

1. Make `execute_mcp_tool` return the complete MCP text without limiting it.
2. Record whether the resolved route is `Route::Mcp` before moving `Resolved` into `run_inner`. In `run`, apply the output hook exactly once, then apply MCP limiting to the resulting text using `smol::unblock`.
3. Limit only successful MCP executions whose post-hook event is also successful, and only with `ctx.offload: Some`. Retain the pre-hook success flag locally so a permission/transport error changed to success by a hook does not suddenly become an offloaded MCP result. No new persisted event field is needed.
4. A hook denial or replacement with `is_error = true` leaves its final error text intact and creates no artifact. A successful replacement is limited/saved only after interception. A successful replacement small enough to fit creates no artifact. A malformed replacement without `text` retains existing hook behavior.
5. Keep hooks' existing fail-open behavior on timeout, cancellation, or plugin failure. This change repairs ordering, not the documented security semantics of failed hooks.
6. Test both model and nested dispatch origins. Add a Lua-backed integration case in `maki-lua/tests/events_slots.rs` that installs an actual output slot, dispatches a stubbed MCP tool with a disk store, and reads the artifact through the filesystem API. The input contains a sentinel beyond the preview boundary; the hook must observe it, and no saved file may contain it after redaction.

### 2. Make artifact comparison bounded and no-follow

Current evidence: `OffloadBackend::read` returns an allocated body (`offload.rs:57-65`); `DiskBackend::read` uses unrestricted `fs::read` (`:227-232`). `put` compares candidate contents while holding the operation mutex (`:115-153`). `stored_form` adds a cap note after the saved prefix (`:191-200`). `DirEntry::metadata` already provides no-follow quota metadata.

1. Replace `read(name) -> Option<Vec<u8>>` with `matches(name, expected: &[u8]) -> io::Result<bool>`. Compare against the complete stored form, including the cap note. Update every backend implementation, including `Arc<MapBackend>`, `GatedBackend`, `InMemoryOffloadBackend`, and `GatedQuotaBackend`.
2. Disk comparison opens a candidate once and validates that opened handle. Do not use a path type check followed by an ordinary open.
   - Unix: `OpenOptions::read(true)` plus `OpenOptionsExt::custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)`. Reject nonregular opened objects using handle metadata before reading. `O_NONBLOCK` prevents FIFO open from waiting for a writer. Use the existing `libc` dependency.
   - Windows: `OpenOptionsExt::custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS)`; the second flag permits opening directory handles so their type can be rejected without guessing from access errors. Inspect opened-handle metadata using `MetadataExt::file_attributes`, reject `FILE_ATTRIBUTE_REPARSE_POINT`, and require a regular file before any read. Add `windows-sys = { workspace = true, features = ["Win32_Storage_FileSystem"] }` under the agent crate's Windows-target dependencies. This reuses an existing workspace crate. Do not classify arbitrary access-denied errors as directories. Directory reparse points and normal directories are rejected using the opened handle, with no path-check/open race.
3. Return false for missing or vanished candidates, directories, symlinks/reparse points, special objects, size differences, and byte differences. Treat expected no-follow/type errors such as Unix `ELOOP`/`EISDIR` as nonmatches. Propagate genuine permission or I/O failures. A nonmatch never frees the occupied name; allocate a later slot with the existing no-clobber creation behavior.
4. Factor a bounded reader comparison. Require the handle's initial length to equal `expected.len()`. Compare in fixed-size chunks and read at most `expected.len() + 1` bytes; reject early EOF, mismatches, and an extra byte. Recheck handle length after comparison. Avoid a second artifact-sized allocation. The bound includes the stored cap note, so it is slightly greater than the saved-body cap when capped.
5. Preserve exact reads of mutable files on every duplicate candidate. Do not trust a hash filename or cached digest. A path replacement after open cannot redirect the handle. Same-inode, noncooperating writers do not provide a portable transactional snapshot; do not claim this stronger guarantee.
6. Add deterministic open/reader test seams only where needed, kept private/test-only. Exercise replacement by a symlink immediately before opening, replacement after opening, and grow/shrink during bounded reading without sleep-based races. Ensure cleanup/release guards unblock every gated test even when assertions fail.
7. Cover directory, symlink to an identical external file, dangling symlink, oversized regular file, Unix FIFO, and Unix socket occupants. Tests must assert no target reads/writes, collision allocation, and unchanged occupants. Use sparse files or `set_len` for large-file fixtures rather than allocating hundreds of MiB. Put FIFO probes on a supervised worker with a timeout used only as a hang guard and a cleanup mechanism that can unblock the worker if the regression occurs.
8. Add an explicit focused agent offload test command to the Windows CI job. Provision symlink-capable test setup in that job (Developer Mode/equivalent runner privilege) and fail setup rather than silently skipping the symlink assertions. Unix-only special-object tests remain cfg-gated; Windows verifies files, directories, and reparse links.

Authoritative API references used during investigation:

- https://doc.rust-lang.org/std/os/unix/fs/trait.OpenOptionsExt.html
- https://doc.rust-lang.org/std/os/windows/fs/trait.OpenOptionsExt.html
- https://doc.rust-lang.org/std/os/windows/fs/trait.MetadataExt.html
- https://doc.rust-lang.org/std/fs/struct.DirEntry.html
- https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew

### 3. Reduce scan work without stale quota state

1. Replace separate backend `names()` and `total_bytes()` enumerations with `snapshot() -> io::Result<OffloadSnapshot>`, containing all occupied UTF-8 names and the total regular-file byte count. Keep names for directories, dangling links, and special objects. Count all no-follow regular files, regardless of filename or size. Ignore disappeared entries during enumeration; propagate other metadata errors. Use checked/saturating quota arithmetic so external large-file edits cannot overflow accounting.
2. `put` uses one snapshot per allocation attempt, checks same-hash candidates first, then quota, then creation. Existing identical content remains reusable at full quota. Re-snapshot after a creation collision. Maintain serialization across quota check and create.
3. Update in-memory snapshots to include directory occupants as names, not only files. Quota sums only regular file entries.
4. Add a counting-backend test that requires one snapshot and no separate full-directory enumeration for an ordinary successful put. Preserve edited/deleted artifact dedup, gap allocation, cap-note accounting, growth/shrink/delete quota behavior, concurrent puts, and TUI same-session replacement tests.
5. Before functional offload changes, add `maki-agent/benches/offload_perf.rs`, a `harness = false` benchmark target, and the existing workspace `criterion` as an agent dev-dependency. Run this benchmark-only state against unchanged production code from `917b42a4`, saving the Criterion baseline before implementing the fixes. Keep the identical benchmark source for the changed run on the same host. Benchmark 10, 100, and 1,000 preexisting small artifacts for a new result and an identical result, plus a sequence-of-puts workload. Use `iter_batched_ref` with the TempDir/store fixture retained as the batch input so setup and input-directory destruction occur outside timed puts; each sample has an independent fixture. Add private shared fixture helpers and `offload_benchmark_fixtures_reset_per_sample`, wired into an ordinary unit-test module, to assert occupied entry counts before/after independent new, duplicate, and sequence fixtures. Report directory size and per-operation latency with baseline comparison. No wall-clock performance threshold belongs in CI. This measures remaining linear scan cost without adding a product quota.

### 4. Close marker and fuzzy whitespace bypasses

1. In `plugins/lib/maki/long_lines.lua:88-102`, remove the `%]%s*$` gate and call `maki.text.truncation_marker(line)` for every after-line. Rust's classifier remains the only marker grammar and trailing-whitespace authority. Preserve exact whole-line occurrence accounting for existing literal markers.
2. Add parameterized real mutation-tool tests for counted, legacy, and preview-cut markers followed by NBSP (U+00A0), em space (U+2003), and narrow NBSP (U+202F), including mixed ASCII/Unicode whitespace. Cover new-file `write`, replacement `write`, `edit`, `multi_edit`, `edit_lines`, and `insert_lines`. Rejected new writes must create no file; rejected existing-file mutations must leave bytes unchanged. Use the existing `edit_tools_host` test setup to enable optional line tools.
3. Cover identical existing literal marker lines with the same Unicode suffix, occurrence-count preservation, duplication rejection, marker removal, and unrecognized bracket suffixes. Do not reject ordinary text merely because it contains brackets or Unicode whitespace.
4. In `fuzzy_replace.lua:631-648`, retain trimmed-equality coverage for nonblank long lines, but require byte-for-byte occurrence-counted coverage for long lines whose trimmed value is empty. Keep separate exact-blank and trimmed-nonblank counts so an empty string cannot cover a long whitespace-only line. Preserve short-line fuzzy matching, long nonblank indentation drift, CRLF behavior, escape-normalized matching, and exact substring edits.
5. Add Lua matcher tests and real `edit`/`multi_edit` cases for spaces, tabs, mixed whitespace, and duplicate long blank lines. `BEGIN\n\nEND` must not replace a block containing an over-cap whitespace line. A full exact `old_string` containing that line remains allowed. A fuzzy block with complete byte-identical whitespace coverage is allowed; a shorter or differently spaced blank line is not. Duplicate occurrences must each be covered. Assert failed `multi_edit` leaves the file unchanged.

### 5. Remove blocking work from the shared Lua thread

Current evidence: `ctx:limit_output` is deliberately synchronous (`ctx.rs:344-390`), because bash calls it from `on_exit` and `on_cancel` (`plugins/bash/init.lua:510-579`). Code execution also calls it from a cancel hook (`plugins/code_execution/init.lua:283-300`). Runtime cancel hooks are synchronous (`runtime.rs:4009-4020`), and cancel replies win through `ctx:finish`. `LuaToolInvocation::execute` receives a Rust-owned `ToolCallReply` outside Lua (`api/tool.rs:496-625`). A direct async conversion without callback migration is invalid.

1. Change ordinary `ctx:limit_output` to `add_async_method`. Parse options into one owned Rust options representation, copy/own the input text and clone the store/config, then release the userdata borrow before awaiting. Execute the entire `limit_output` call, including hashing, scanning, file I/O, and operation-lock waits, in `smol::unblock`. Preserve `(value, err)` conventions, capability errors, option validation, zero overrides, preview shape, and trailers. Update bare-Lua tests to use `eval_async` under `smol::block_on`.
2. Add one optional reply-table field, `output_limits`, using the same option keys as `ctx:limit_output`. The reply's `llm_output` is the raw body; `output_limits.trailer` is metadata that must appear last. Parse it in `ToolCallReply::from_lua_value` into owned limit options and carry it on `ToolCallReply`. Both handler returns and `ctx:finish(reply)` use the same parser. Do not add a second finish method or change its arity.
3. Validate this field as a text-output feature. Reject malformed options or its use with image/diff/structured-state output instead of silently saving or corrupting structured results. Preserve the original success/error result while replacing only its text after limiting. Keep the descriptor out of persisted tool state and provider output.
4. In `LuaToolInvocation::execute`, immediately after receiving the reply, finalize deferred limits on a blocking worker before constructing `ToolExecResult`. This happens after the Lua coroutine/cancel callback has supplied its terminal reply and outside the Lua task's cancellation window. Transform both `Ok(String)` and `Err(String)` text while retaining their success/error variant. Move owned body/options/store into `smol::unblock`; a closure already running continues if the awaiting caller is dropped. Do not claim queued but unstarted work survives task drop: dropping `blocking::unblock`'s task can cancel it before execution. That unstarted save is allowed to be abandoned; the store's closure checks prevent later recreation after print cleanup. This plan does not require every enqueued deferred save to survive abandonment. Do not retain Lua objects or userdata borrows in the worker. No generic async cancellation-hook runtime is needed.
5. Migrate bash completion callbacks to `ctx:finish` with raw output and `output_limits`, preserving a failed command's `Exit code: N` as the final trailer. The existing `finished` guard still prevents duplicate completion. Empty-output behavior and visible buffer updates stay unchanged.
6. Refactor `partial.cut` with its only callers (bash and code execution) so it still paints the partial marker once, but returns raw output plus `output_limits` with that marker as trailer. A callback sends this reply immediately with `ctx:finish`. The interpreter's ordinary cancellation return uses the same memoized reply table. Derive the partial/no-output wording from raw body emptiness, not from preview availability. The saved artifact must contain raw streamed text only, never preview/footer/cancel markers.
7. Ordinary calls in glob, grep, webfetch, websearch, and successful code execution continue to call yielding `ctx:limit_output` from handler coroutines. Lua async calls do not require an `await` keyword. Audit all bundled call sites to ensure no synchronous callback invokes this method.
8. Build a test-only gated backend and same-host coroutine harness. Hold `create_new`, run a sibling Lua tool, and assert sibling completion before releasing storage. Also block an operation lock held by another put and verify this wait does not block Lua. Exercise cancellation with deferred partial output while storage is gated: cancel hooks must return/paint and sibling work must continue before releasing the gate; afterward the final result and artifact must be correct. Add `deferred_limit_started_worker_survives_caller_drop`: dispatch a real Lua tool with reply limits, rendezvous after `create_new` starts, cancel/drop and drain the Rust invocation task, prove sibling Lua progress, release storage, and verify the raw artifact. Add `deferred_caller_drop_cleanup_removes_late_artifact`, requesting cleanup before releasing that worker and asserting creation precedes removal with no later reappearance. Use channel rendezvous, release-on-drop guards, and timeout hang guards, not sleeps.

### 6. Make cleanup requests nonblocking and retain print cleanup completion

Current evidence: `RemoveOffloadOnDrop::drop` calls `close_and_remove` synchronously (`headless.rs:104-117`), potentially waiting on the disk-spanning store mutex. Print waits for the agent task or a five-second timer (`src/print.rs:500-505`); dropping the task currently invokes that blocking destructor.

1. Separate admission closure from the serialized operation lock in `OffloadStore`. Use an atomic closed flag plus the existing operation mutex. `request_close` sets the flag without acquiring the operation lock. `put` checks it before waiting and again after acquiring the lock, before hashing/I/O. A put that passed the second check is in flight and may finish; queued/later puts reject closure without creating anything. `close_and_remove` requests closure, acquires the operation lock on a worker, then removes files. No I/O runs under the short lifecycle state lock; no separate admission counters are necessary for this serialized store.
2. Add a small shared print-cleanup handle/controller around the store. Its request operation closes admission and starts exactly one detached worker. The worker calls `close_and_remove`, records completion/error under short-lived synchronization, and wakes async waiters. Wait registration must be race-safe and idempotent. Expose a completion waiter through `HeadlessHandle` (optional when no store exists).
3. `RemoveOffloadOnDrop` owns a clone of that handle; Drop only requests cleanup. It must never wait for the operation mutex, filesystem removal, or an executor task. Normal headless completion requests and awaits cleanup after dropping the agent and before normal stream closure. Early returns and panic unwinding request the same worker through the guard.
4. Update `src/print.rs` so cleanup ownership survives the agent task. Keep the task and cleanup waiter outside both unwind boundaries. First catch the event/output body, retaining either its result or panic payload. Separately catch panics while polling/settling the agent task, including a provider/task panic propagated at join; settle/drop with the existing five-second agent timeout. In every case request and await cleanup before returning or resuming panic. If both stages panic, preserve and resume the original body panic, logging the secondary settlement panic without allowing double unwinding. Early serialization/output errors use the same settlement path. Do not convert a panic to success. Use `catch_unwind` for the synchronous body and an unwind-catching future wrapper or `FutureExt::catch_unwind` for task settlement; use the existing workspace `futures` dependency if needed, with justified `AssertUnwindSafe` around the owned boundary rather than arbitrary application state.
5. Keep the five-second timeout for the agent task, not for abandoning directory cleanup. Cleanup can wait for bounded in-flight filesystem work as the previous destructor did, but it no longer blocks Lua or executor threads. Never spin, call nested `block_on` from async Drop, or rely solely on detached worker scheduling before CLI exit.
6. Log removal failure once with directory/error context. A failure must not reopen admission or report successful removal; publish the failure to waiters and keep current best-effort run-result policy. Persistent/TUI stores are not closed when a root actor is replaced. Maintain tests for shared-store identity and concurrent put serialization.
7. Add channel-gated tests for nonblocking cleanup request and Drop, queued-put rejection, create-before-remove ordering, removal completion, idempotent requests, completion before waiter registration, removal error publication, normal headless completion, task drop, early run error, and panic cleanup. Factor print settlement with an injectable immediate shutdown future so task-drop behavior is deterministic without waiting five seconds. Separate `print_body_panic_drains_cleanup` and `print_agent_task_panic_drains_cleanup` tests exercise body and synthetic-provider/task-join panics; gated removal must complete before panic reaches the test catch boundary. Add a dual-panic variant asserting the original body payload wins. Serializer/output errors and timeout-triggered task drop are distinct cases.

### 7. Preserve recovery advice on repeated results

Current evidence: `footer` contains `advice` and `CLIPPED_NOTE` (`offload.rs:422-440`); `pointer` omits them (`:442-461`). Another agent can create the artifact, so the current conversation may see only a pointer.

1. Reuse the same advice/disclosure helpers for created and existing outcomes. For pointers, inspect the saved portion `body[..saved.saved_bytes]`, include bash inspection advice for long physical lines, include shell quoting when needed, and include the clipping disclosure when `lines_clipped` is true.
2. Preserve pointer recognition prefix, label, original/saved/discarded sizes, cross-agent wording, and the instruction to inspect an earlier result absent from the current conversation. Preserve trailers and the rule that complete metadata can exceed tiny limits without a preview.
3. Add pointer unit tests for plain lines, long lines, clipped grep results, capped artifacts, paths needing quoting, tiny/zero limits, and error trailers. Add a real repeated grep result and live/restored view comparison, and preserve the cat-artifact-no-new-artifact behavior.

## Acceptance Criteria

- AC.1: A full MCP result reaches output interception exactly once before any artifact save. Redacted sentinels outside the preview are absent from all artifacts and the final output. Denied/error-marked output creates no artifact; no-store and non-MCP contracts remain intact. Tests: `mcp_output_hook_redacts_before_offload`, `mcp_output_hook_denial_does_not_persist`, `mcp_hook_replacement_controls_offloading`, `lua_mcp_output_slot_redacts_saved_artifact`.
- AC.2: Artifact comparison does not follow candidate symlinks/reparse points, wait for a FIFO writer, or read/allocate beyond the expected stored form plus one byte. Wrong-type, changed-size, and mismatching occupants reserve their names while a new slot is chosen. Tests: `disk_dedup_wrong_object_uses_next_slot`, `disk_dedup_symlink_swapped_before_open`, `disk_compare_stays_on_opened_handle`, `bounded_compare_rejects_growth_and_shrink`, `bounded_compare_caps_bytes_read` (including cap-note cases).
- AC.3: One snapshot supplies occupied names and live no-follow regular-file quota bytes per ordinary allocation attempt. Existing duplicates remain reusable at quota, file mutations change quota, and concurrent saves do not exceed quota. Tests: `put_uses_one_snapshot_per_attempt`, `snapshot_counts_regular_bytes_and_all_occupants`, `dedup_existing_at_full_quota`, `quota_reflects_grow_shrink_delete`, `concurrent_puts_respect_quota`, `same_id_replacement_serializes_in_flight_offload_put`.
- AC.4: All mutation tools reject recognized marker lines with Unicode trailing whitespace without changing files, while exact existing-literal occurrence allowances and marker removal remain supported. Tests: `mutation_tools_reject_unicode_suffixed_markers`, `unicode_literal_marker_occurrences_are_preserved`, `unicode_marker_removal_and_unrecognized_suffixes_pass`.
- AC.5: Fuzzy replacement cannot erase or collapse an over-cap whitespace-only line using blank/short/different whitespace in `old_string`. Full byte-identical occurrences remain editable and short/nonblank compatibility is retained. Tests: `fuzzy_rejects_uncovered_long_whitespace`, `fuzzy_requires_each_long_whitespace_occurrence`, `fuzzy_accepts_complete_long_whitespace`, `real_edit_rejects_uncovered_long_whitespace`, `multi_edit_whitespace_guard_is_atomic`.
- AC.6: Output limiting and store-lock waits do not stall sibling coroutines on the same Lua host. Capability errors, option validation, no-store truncation, and zero limits keep their documented result shape. Tests: `limit_output_async_yields_to_sibling_lua`, `limit_output_lock_wait_yields_to_sibling_lua`, `limit_output_is_handler_only`, `limit_output_zero_keeps_metadata_and_success_pair`, `limit_output_invalid_options_fail_without_saving`.
- AC.7: Bash completion and bash/code-execution cancellation callbacks produce bounded final output without storage I/O on Lua. Saved partial files contain raw body only; partial markers and failing exit codes are terminal trailers; completion remains single-use. Already-started storage survives abandonment of its Rust caller, without blocking sibling Lua work; queued unstarted saves may be canceled. Tests: `finish_deferred_partial_output_offloads_raw_body_with_marker_trailer`, `deferred_limit_preserves_error_and_metadata`, `deferred_limit_rejects_structured_output`, `callback_storage_does_not_block_sibling_lua`, `deferred_limit_started_worker_survives_caller_drop`, `bash_zero_output_override_finishes_on_exit`, `bash_zero_output_override_finishes_on_cancel`, `code_execution_cancel_deferred_output_offloads`, `bash_failure_keeps_exit_code_after_footer`.
- AC.8: Cleanup request and guard Drop return while a save is gated. New/queued saves reject closure; in-flight work completes before removal; completion/error is observable and requests are idempotent. Tests: `close_request_and_drop_do_not_wait_for_put`, `close_rejects_queued_and_later_puts`, `cleanup_waits_for_in_flight_put`, `cleanup_completion_is_idempotent_and_race_safe`, `cleanup_failure_is_published_once`.
- AC.9: Print settlement drains cleanup before returning on normal completion, task drop, early errors, or panic propagation from either the run body or agent-task join. The original body panic wins over a second settlement panic. Saved files do not reappear after an abandoned caller's in-flight save, and removal failures are reported rather than called successful. Tests: `print_settlement_drains_cleanup`, `headless_cleanup_is_drained_after_task_drop`, `print_body_panic_drains_cleanup`, `print_agent_task_panic_drains_cleanup`, `print_dual_panic_preserves_body_payload`, `print_early_output_error_drains_cleanup`, `deferred_caller_drop_cleanup_removes_late_artifact`, `print_cleanup_error_is_reported`.
- AC.10: Repeated-result pointers preserve clipping disclosure, long-line/quoted-path inspection advice, cap accounting, and terminal trailers. Real grep restore retains the notice and catting an artifact does not create another. Tests: `repeated_pointer_preserves_recovery_advice`, `repeated_pointer_metadata_and_trailer_survive_tiny_limits`, `grep_repeated_pointer_live_equals_restore`, `cat_offload_file_returns_pointer`.
- AC.11: The many-small-artifact benchmark runs with independently reset fixture sizes and separates setup and fixture destruction from measured puts. Baseline results are captured in a benchmark-only state with unchanged production code, and changed results use the identical harness on the same host. Checks: `offload_benchmark_fixtures_reset_per_sample`, `offload_perf_smoke` via Criterion `--test`, and `offload_perf_baseline_comparison` for new, duplicate, and sequential saves; `put_uses_one_snapshot_per_attempt` fails if double enumeration returns.
- AC.12: Documentation reflects full-result MCP interception, yielding handler limiting, callback-safe reply limits, whitespace-only fuzzy coverage, repeated-pointer advice, and cleanup semantics. Generated references are current. Checks: `generated_docs_match_sources` (`just gen-docs-check`), `lua_api_output_limit_contract_docs` (API-doc source/render assertion for the async and reply-field contract), and `review_documentation_scenarios` (manual review against AC.1, AC.5, AC.7, AC.9, and AC.10 examples).

## Test Strategy

The named tests above are intended additions or explicit extensions; do not treat passing unrelated existing tests as coverage. Use `#[test_case]` for Rust matrices and shared constants for expected messages. Keep Rust unit tests beside the implementation; Lua pure-logic tests use existing plugin specs. Filesystem/admission logic uses `offload.rs` unit tests and temp disk fixtures. Mutation behavior, Lua scheduling, MCP slots, and callback results use real registered tools in `maki-lua/tests/plugin_host.rs`, `events_slots.rs`, and `real_plugins_restore.rs`. Print settlement uses focused root/headless tests with synthetic providers and injectable shutdown completion. AC.6-AC.9 require new gated-backend/settlement harness code in this change; existing tests alone cannot demonstrate nonblocking behavior.

Coverage mapping:

| Criterion | Layer and named checks |
| --- | --- |
| AC.1 | Agent dispatch tests plus `lua_mcp_output_slot_redacts_saved_artifact` full slot/MCP/store/read integration |
| AC.2 | Disk wrong-object and deterministic open-race tests plus bounded-reader unit tests; Windows focused execution |
| AC.3 | Snapshot/counting backend unit tests, quota/concurrency tests, and TUI replacement scenario |
| AC.4 | Parameterized real mutation tools with before/after filesystem assertions plus shared-marker Lua specs |
| AC.5 | Matcher Lua specs and real edit/multi-edit filesystem tests |
| AC.6 | Same-host gated storage and lock-wait coroutine tests plus async ctx contract tests |
| AC.7 | Reply parser/finalizer unit tests and real bash/interpreter callback scenarios, with render/trailer assertions |
| AC.8 | Channel-gated store/cleanup controller tests, including early-notification and error cases |
| AC.9 | Injected print settlement and headless lifecycle integration tests, including caught/repropagated panic |
| AC.10 | Pointer formatter tests and real grep live/restore/cat scenarios |
| AC.11 | Criterion smoke, same-host baseline comparison, and deterministic enumeration-count regression |
| AC.12 | Doc generation/source contract assertions and named human documentation scenario review |

Execution sequence, cheapest first:

1. Introduce only the benchmark target, dev-dependency, and fixture helpers first. Run `offload_benchmark_fixtures_reset_per_sample` and the smoke check, then capture `cargo bench -p maki-agent --bench offload_perf -- --save-baseline review-917b42a4` before changing production offload code. Preserve that benchmark harness for the later comparison. Then add regression probes before fixes and demonstrate they fail for the reported behavior where safe. Do not run an unsupervised FIFO probe. Keep test helpers private or behind existing `test-support` features.
2. `cargo check -p maki-agent -p maki-lua -p maki-ui --tests --benches`, adding the root package check when print settlement changes. Iterate with focused `cargo test`/nextest filters and plugin specs.
3. `cargo test -p maki-agent --lib`, `cargo test -p maki-lua --test plugin_host --test events_slots --test real_plugins_restore --test spec`, affected ctx/API unit tests, headless tests, and root print tests. Run the gated tests under a single-threaded and normal test execution to catch shared-global assumptions.
4. Run `offload_benchmark_fixtures_reset_per_sample` and `cargo bench -p maki-agent --bench offload_perf -- --test`; then run `cargo bench -p maki-agent --bench offload_perf -- --baseline review-917b42a4` with the unchanged harness to compare against the saved pre-fix baseline. Record environment and results without setting flaky elapsed-time assertions.
5. `just fmt-check`, `just lint`, `just test`, `just gen-docs-check`, and `just machete` (or `just ci`, which also runs Python checks). Use the repository's available tools; report missing tools plainly. Do not suppress the existing interpreter deprecation warning merely to obtain green lint without investigating it.
6. Native `just build`; Windows CI executes focused agent offload tests as well as existing lint/build. Reuse the project CI environment and update Nix dependency hashes only if the package graph actually requires it; run the Nix hash drift check when relevant.

Timeouts are hang guards, not timing assertions. Channel signals establish causal ordering. Every blocked test has unconditional gate release and joins/drains its worker. No sleeps establish correctness. A Linux-only green run is insufficient evidence for the Windows no-follow implementation; the updated Windows test job is part of this work. Benchmark latency is a measurement rather than an automated pass/fail budget. Manual documentation review supplements, not replaces, executable behavior tests.

## Review Strategy

Plan-mode review uses `plan-reviewer` before handoff. Fix or explicitly rebut all findings; re-run review after critical/high findings until none remain.

After implementation and automatable testing, use the repository's `nat-code-reviewer` guidance, as established in `.agents/makima-plans/75-truncation-guard-and-offload-1.md`. Give the reviewer all six review items, the diff, test results, platform evidence, benchmark results, and the callback/cleanup lifecycle contract. Focus on raw-text persistence before hooks, race-safe bounded opening, occurrence accounting, Lua borrow release, deferred error-text limiting, worker ownership after cancellation, cleanup on task drop/panic, and mutable quota accounting. Fix or explicitly rebut every finding. Re-run after critical findings. Do not commit, push, or edit the GitHub review without separate operator instruction.

## Documentation Strategy

Update canonical handwritten behavior in `site/docs/content/hooks/_index.md` and `site/docs/content/token-economy/_index.md`. State that MCP hooks see full results before offload; preserve the documented fail-open hook caveat. Explain that whitespace-only long fuzzy candidates require exact coverage, while nonblank indentation tolerance remains. Keep artifact caps/lifetime and print removal behavior consistent with the implementation.

Update API source documentation around `maki-lua/src/api/tool.rs:720-752` to define the new `output_limits` reply field, raw body/trailer semantics, and yielding `ctx:limit_output`. Identify callback migration requirements explicitly: synchronous job/cancel callbacks use `ctx:finish` with reply limits, not the yielding method. Regenerate Lua API/tool references with `just gen-docs`; do not hand-edit generated pages. Update directly relevant module guidance only if necessary to explain off-thread offload I/O and nonblocking cleanup requests. Avoid unrelated prose rewrites and trivial comments.

## Risks, Blockers, and Required Decisions

- Scope is resolved: the operator requested all six items. No remaining product decision blocks execution.
- The yielding Lua method changes compatibility for external plugins that invoke `ctx:limit_output` from synchronous callbacks. The callback-safe reply field, documentation, and built-in migration are required, not optional. Normal coroutine handler call syntax is unchanged.
- `smol::unblock` does not interrupt a filesystem syscall when its awaiting future is dropped. Safe candidate opening and read bounds reduce known hangs; worker ownership plus admission closure/removal ordering handles late completion. Hardware/network filesystem faults can still delay cleanup. Preserve current wait-for-cleanup semantics rather than silently weaken them to a new timeout policy.
- Cleanup on process kill cannot be guaranteed. This preexisting limitation remains documented. A caught run panic is drained before propagation; an abrupt process termination is outside that contract.
- Safe opening covers mutable candidate objects under the configured store directory. It is not a new adversarial filesystem-root sandbox, and no portable atomic snapshot is guaranteed against noncooperating same-inode writes.
- Windows symlink creation requires a capable runner. The plan includes setup and focused Windows tests; missing privilege must fail clearly, not silently mark AC.2 verified. Special Unix files have no ordinary-directory Windows analogue.
- Directory enumeration remains linear in artifact count despite combining scans. AC.11 measures this cost without adding an arbitrary count limit or stale persistent index. A further indexing redesign is outside this change.
- Pointer metadata grows when advice is added. Tiny-limit metadata exceptions remain intentional; grep restore tests must cover the new notices.
- The existing review observed an intermittent actor test failure in parallel but not in isolation or serial tests. Treat any recurrence as an observed test failure, investigate it, and distinguish it from this change rather than assuming it is harmless.
- All required scheduling/lifecycle test infrastructure is included in the implementation phases. No test infrastructure gap is intentionally deferred. Windows runtime results and manual documentation review remain completion evidence to obtain during execution, not claims made by this plan.

## Execution verification

### Baseline

Criterion baseline `review-917b42a4` was captured before changing production offload logic. The benchmark fixture reset test and all nine Criterion `--test` cases passed. A read-only comparison against `917b42a4` confirmed that the offload module had only test fixture additions at capture time. The benchmark source and shared fixture remain unchanged for the comparison run.

Commands:

```
cargo test -p maki-agent --lib offload_benchmark_fixtures_reset_per_sample
cargo bench -p maki-agent --bench offload_perf -- --test
cargo bench -p maki-agent --bench offload_perf -- --save-baseline review-917b42a4
```

Environment: Linux 7.2.4 x86_64, glibc 2.42, rustc 1.95.0 (59807616e, 2026-04-14), cargo 1.95.0. Each independently prepared fixture contains 10, 100, or 1,000 small deterministic files. The sequence adds as many new files as the initial directory contains. Setup and fixture destruction are outside timed puts. Measurements below are Criterion mean estimates, in microseconds.

| Initial files | New put | Identical put | Sequence total | Sequence per put |
| --- | ---: | ---: | ---: | ---: |
| 10 | 43.52 | 7.00 | 328.14 | 32.81 |
| 100 | 95.84 | 17.43 | 9,472.61 | 94.73 |
| 1,000 | 472.69 | 103.72 | 701,529.18 | 701.53 |

The local Criterion data is under `target/criterion/offload_put/*/review-917b42a4`. These values are measurements, not CI performance limits.

### Regression probes before fixes

`cargo test -p maki-agent --lib mcp_output_hook -- --nocapture` ran four new tests against the original dispatch ordering. Both model and nested redaction tests failed because the output hook received a preview instead of the complete text. Both denial tests failed because the artifact had already been saved. An earlier invocation did not run tests because the benchmark target source had not yet been created during manifest editing; the retry produced the behavior evidence.

### Focused checks after fixes

`cargo test -p maki-agent --lib mcp_ -- --nocapture` passed 48 tests. This includes both dispatch origins for full-result redaction and denial, small/large successful replacements and error replacements, and a denied execution rewritten as success that remains unsaved. Intermediate compilation attempts encountered a wrong MCP tuple pattern and an unnecessary `String::into_owned`; both were corrected. Other intermediate failures came from backend APIs during concurrent migration. The passing run still had warnings in actively edited store/headless tests and does not replace final lint.

`cargo test -p maki-lua --test events_slots lua_mcp_output_slot_redacts_saved_artifact -- --nocapture` passed both model and nested variants. The integration uses an actual Lua output slot and reads the saved disk artifact with `maki.fs`. An initial fixture registered the slot under the qualified MCP name rather than the requested wire name; it did not run and failed its invocation-count assertion. The fixture now uses `srv__probe`, preserving dispatch naming behavior.

The whitespace worker demonstrated pre-fix failures for all 12 Unicode marker spec cases, 10 fuzzy blank-line cases, six real marker mutation paths, and six real blank edit/multiedit paths. After the fixes, 35 real mutation tests passed with 422 real tool invocations and before/after disk assertions. All 18 Lua spec targets passed. Focused Clippy with `-D warnings` passed for those targets, as did existing plugin-host long-line and marker filters (10 tests total). The real matrix covers new and replacement writes, edit, multiedit, edit_lines, and insert_lines. It also covers literal occurrence retention/removal, duplication rejection, unrecognized suffixes, exact/full blank-line coverage, CRLF, escapes, and atomic multiedit rejection.

### Windows execution setup

The Windows CI job now checks file and directory symlink creation and verifies reparse attributes before running `cargo test -p maki-agent --lib tools::offload::tests`. Setup failure is fatal. GitHub-hosted Windows runners provide administrator privileges; the probe verifies actual capability instead of assuming it. Linux checks cannot establish Windows runtime behavior. No Windows result has been observed in this session.

### Workspace and lifecycle checks

The combined focused run passed 1,256 agent tests, 295 plugin-host tests, 56 slot tests, 30 restore tests, 19 scheduling tests, 18 Lua spec targets, and 35 whitespace tests. Focused root print tests passed 18 cases and headless tests passed 20 cases. Single-threaded gate checks initially passed all 81 offload, 19 scheduling, and 18 print tests. After the review follow-ups, serial runs passed all 81 offload, 23 headless, 21 scheduling, and 21 print tests. Final LSP diagnostics for headless and print were clean.

The first workspace nextest run passed 6,886 of 6,887 tests. `cancel_mid_flight_repaints_parked_children_and_spares_finished_ones` lost a partial child result. Investigation reproduced a new cancellation-precedence defect: a queued early `ctx:finish` summary replaced the richer structured handler return. The fix gives that precedence only to replies with deferred output limits. All 26 batch-policy tests and 19 scheduling tests passed after the fix. A subsequent full run passed all 6,890 tests across 43 binaries.

The complete local check chain passed before the review follow-ups. It passed again on the final follow-up code: `cargo check -p maki-agent -p maki-lua -p maki-ui -p maki --tests --benches`, `just ci`, and `just build`. `just ci` includes Rust/Lua formatting, Clippy with `-D warnings`, Python checks, workspace nextest, generated-reference freshness, and dependency-use checks. The final workspace run passed all 6,902 tests across 43 binaries, with none skipped. `nix build .#checks.x86_64-linux.git-dep-hashes --no-link` passed again, as did `git diff --check`. Cargo.lock adds use of existing workspace dependencies only: Criterion, futures, and windows-sys. No Nix dependency hash change was needed.

### Review follow-ups

The hook and whitespace review approved its scope. Its optional error-to-success coverage was expanded to both model and nested permission and transport failures; all four variants passed.

The full lifecycle/store review requested changes for pending-future destructor panic supervision and a mismatch between accepted `content` aliases and reply coercion. The alias fix now uses one text extractor for validation and result construction. Its direct-return and `ctx:finish` tests cover success and error replies and inspect the saved raw body. The regression failed before the fix, then all seven coercion tests and 21 scheduling tests passed. Scoped Clippy with `-D warnings` passed. Future destruction supervision now catches the actual owned inner future's destructor and publishes completion or its panic payload through `HeadlessHandle.teardown`. Print cancels or joins the task, awaits that teardown, then drains offload cleanup before returning or resuming the selected panic. A real pending provider-resource destructor test gates destruction and removal independently. Additional tests cover unpolled cancellation, completion on Drop rather than Ready, and a poll panic followed by a separate destructor panic. All 23 headless and 21 print tests passed, including return-boundary assertions. Scoped check and Clippy with `-D warnings` passed. The ineffective catch around the task handle's Drop was removed. The optional collision-slot lookup finding is addressed by filtering same-hash names once before free-slot membership checks. The final `nat-code-reviewer:pr156-final-review` review approved the complete change against `917b42a4`, including the untracked benchmark, fixture helper, scheduling tests, and whitespace tests. It reported no remaining findings and confirmed the prior high, medium, and optional low findings are fixed. It verified that Windows tests do not skip privilege failures, while retaining native Windows execution as an outstanding verification gap. The reviewer inspected code and tests but did not independently rerun the parent-observed checks.

### Benchmark comparison

The first changed run exposed a regression caused by opening every entry during quota snapshots. The snapshot now uses no-follow `DirEntry::metadata`; candidate comparisons still validate and read one no-follow opened handle. The identical harness comparison after this correction measured the following mean estimates, in microseconds:

| Initial files | Operation | Baseline | Changed | Mean change |
| --- | --- | ---: | ---: | ---: |
| 10 | new | 43.52 | 34.16 | -21.5% |
| 10 | identical | 7.00 | 14.02 | +100.4% |
| 10 | sequence total | 328.14 | 293.71 | -10.5% |
| 100 | new | 95.84 | 84.48 | -11.9% |
| 100 | identical | 17.43 | 65.29 | +274.6% |
| 100 | sequence total | 9,472.61 | 7,829.68 | -17.3% |
| 1,000 | new | 472.69 | 386.38 | -18.3% |
| 1,000 | identical | 103.72 | 357.34 | +244.5% |
| 1,000 | sequence total | 701,529.18 | 579,533.55 | -17.4% |

Duplicate reuse is slower because the required combined snapshot gathers live quota metadata before comparison; the old duplicate fast path enumerated names only. New and sequence saves improve at 1,000 files. Directory scans remain linear; sequence workloads grow quadratically overall. No performance threshold is added to CI. The table records the final comparison after the collision-name filtering follow-up, using the unchanged harness. All 81 offload tests, fixture reset checks, and nine benchmark smoke cases passed after that follow-up. Small-sample noise remains; Criterion marked the 100-file new-save change as statistically insignificant.

### Documentation scenario review

Manual source and generated-reference review checked the following scenarios against the behavior tests:

- AC.1: A sentinel beyond the MCP preview reaches the output slot before persistence. The hook documentation states that the successful replacement is saved and that denial or error-marked replacement creates no artifact. The token-economy documentation retains the failed-hook fail-open caveat and no-store passthrough.
- AC.5: An empty or shorter blank line cannot cover a long whitespace-only candidate. The token-economy documentation requires byte-identical coverage for each occurrence and retains nonblank indentation tolerance and exact substring edits.
- AC.7: Synchronous callbacks send raw `llm_output` plus `output_limits` through `ctx:finish`. The API source and generated reference describe yielding handler calls, plain-text-only deferred limits, retained error status, raw saved bodies, and terminal trailers. Bash failure and interpreter cancellation tests exercise these contracts.
- AC.9: Print closes admission and waits for in-flight saves and removal before returning or propagating a caught panic. The token-economy documentation qualifies the missing final-answer path with successful cleanup, logs removal failure, and excludes abrupt termination. The future-destruction review follow-up strengthens implementation coverage without changing this documented contract.
- AC.10: Repeated pointers retain inspection advice and clipping notices, including when another agent created the artifact. The token-economy documentation states this behavior. Real grep live/restore tests inspect the pointer, quoted paths, clipping notice, unchanged artifact count, and restored spans.

`stylua --check plugins/` and `just pylint` passed again during the follow-up work. Final generated-reference freshness passed as part of `just ci`.

### Remaining verification

The reviewer-requested follow-up tests, final workspace rerun, and final follow-up review are complete. Native Windows runtime evidence remains pending. Windows cross-check attempts initially used a compiler without the target's std/core. Selecting rustup's compiler explicitly progressed into dependency builds, then failed because the environment supplied GCC for an MSVC target and lacks the required Windows C toolchain. The installed cargo-xwin path also could not execute. None of these attempts compiled or ran the Windows offload implementation, and none is Windows correctness evidence. The CI job must supply the native result. No commit, push, or GitHub review edit has been made.
