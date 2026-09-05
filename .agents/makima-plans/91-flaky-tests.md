# Goal

Eliminate the eight contention-sensitive or incorrectly asserted tests identified in the embedded flaky test report by making valuable tests deterministic and removing redundant, timing-dependent tests. Update the report so it records each disposition and the verification results.

# Implementation Summary

This is primarily test-harness work in `maki-ui`, `maki-lua`, and `maki-commands`; production behavior should not change. Keep five tests that protect distinct UI, callback, and locking contracts, replacing fixed polling budgets or invalid asynchronous assertions with observable completion barriers. Remove three negative-value integration tests and preserve their useful contracts at deterministic boundaries.

Affected touch points:

- `maki-ui/src/components/file_completion.rs`: deterministic Nucleo convergence for two file-completion tests.
- `maki-ui/src/app/tests.rs`: deterministic argument-request cleanup; removal of redundant model-Enter and empty-result cross-layer tests.
- `maki-commands/src/tests.rs`: deterministic coverage that canceling and reopening completion allocates a fresh session ID.
- `maki-lua/src/loader.rs`: assert the actual post-unload provider-usage acknowledgement.
- `maki-lua/src/write_lock_regression.rs`: gate the delete operation after lock acquisition by matching the note path.
- `maki-lua/tests/in_memory_host.rs`: remove the timing-dependent splash startup test.
- The embedded flaky test report: mark all eight findings fixed or removed, explain the final disposition, and append verification evidence.

Non-goals: changing Nucleo, Lua-host scheduling, completion lifecycle semantics, splash startup durability, provider-usage protocol, file-lock ordering guarantees, nextest thread configuration, or session-lock behavior.

# Implementation Plan

## Phase 1: Make Nucleo-backed completion tests converge on their actual preconditions

1. In the `file_completion` test module, introduce or reuse a small deadline-based matcher-settling helper with a named timeout and failure message. It must repeatedly call `FileCompletionMenu::tick()`, yield the thread, and stop only when a supplied state predicate is true. Avoid fixed iteration counts and sleeps.
2. Update `switching_from_explicit_to_project_restores_project_and_lua_matches` (`maki-ui/src/components/file_completion.rs:1305`) to wait until the injected project file has appeared and refresh is no longer pending. Preserve assertions that project-file and Lua/skill candidates coexist, and explicitly ensure the stale explicit candidate does not survive the transition.
3. Update `query_refresh_keeps_previous_result_set_until_matching_finishes` (`maki-ui/src/components/file_completion.rs:2325`) to use the same helper both when waiting for the initial three injected files and when waiting for the filtered `gamma-file` result. Preserve the assertion that the previous complete result set remains visible while `query_refresh_pending` is true.
4. Remove `enter_inserts_model_reference_with_trailing_space` (`maki-ui/src/app/tests.rs:6852`) rather than extending its Nucleo/filesystem convergence wait. Its useful contract is already covered deterministically by `model_replacement_has_prefix_and_trailing_space` in the component tests, while `ctx_models_flow_into_model_items` in `maki-lua/tests/completion_plugins.rs` covers real model-source wiring. Retain the neighboring subagent Enter test as the app-level Enter-routing check; do not add another asynchronous model-specific integration test.

## Phase 2: Correct asynchronous completion and provider-usage assertions

1. Keep `argument_completion_clears_old_rows_while_request_pending` (`maki-ui/src/app/tests.rs:1111`) because it uniquely verifies that stale argument rows disappear before the replacement request resolves. Leave the render assertion before request completion, then replace the immediate `try_finish_command_arguments` assertion with the existing bounded deadline/yield pattern used by sibling argument-completion tests. Finish the queued request with an empty result so no detached task remains awaiting its probe response.
2. Remove `empty_completion_cancels_once_and_next_request_uses_new_session` (`maki-ui/src/app/tests.rs:1043`). Its exact cross-layer sequence depends on detached executor scheduling, no internal 300-second blocking path was found, and its useful properties remain covered by:
   - `maki-ui`'s `unmatched_completion_items_cancel_the_argument_session` for palette-to-Lua cancellation integration;
   - `maki-commands`' `final_session_owner_drop_cancels_once` for once-only cancellation;
   - existing command registry/session tests for invalidation.
   Add a synchronous `maki-commands` unit test named `cancelled_completion_session_reopens_with_fresh_id` that opens a completion session, cancels it, opens another session for the same command/input, and asserts distinct `CompletionSessionId`s. This preserves the useful fresh-session contract without App, Lua probes, detached tasks, storage, or polling. Do not increase timeouts or change storage/session-lock code.
3. In `provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it` (`maki-lua/src/loader.rs:1365`), retain the initial Flash then `ProviderUsageAck(1)` checks. After unloading the observer and publishing invalidation 2, use a bounded receive and require `ProviderUsageAck(2)` instead of asserting the channel is immediately empty. Because callback actions are emitted before the Ack, receiving the Ack first also deterministically proves that the unloaded callback did not emit a Flash.

## Phase 3: Repair the write-lock regression probe

1. In `maki-lua/src/write_lock_regression.rs`, replace the probe's unqualified first-stat `AtomicBool` gate with synchronized optional target-path state. `arm_stat_gate` should accept the seeded note's `PathBuf`; `ProbeFs::stat` should atomically consume and block only when the incoming path equals that target.
2. Update `memory_write_and_delete_serialize_on_shared_lock` to arm the gate for the resolved `memories/notes.md` path. This lets project-root stats used by the `mutable_path` callback pass and parks the delete at its note metadata check, after dispatch has acquired the write lock.
3. Retain assertions that the concurrent write neither executes nor completes while delete is parked, that `rm` precedes the later write, and that the final note exists. The test should verify mutual exclusion, not unsupported FIFO ordering before mutable-path computation.

## Phase 4: Remove the negative-value splash test

1. Remove `splash_picker_startup_option_wins_over_saved` from `maki-lua/tests/in_memory_host.rs`. It accepts any nonempty frame, checks eventual persistence separately, and therefore does not establish its claimed before-first-frame ordering. Enforcing that ordering would require an unwanted synchronous filesystem durability barrier.
2. Keep the adjacent deterministic coverage unchanged: `splash_picker_startup_option_selects_load_time_contribution` verifies an identifiable renderer and eventual persisted choice; the typo and invalid-saved-selection tests cover fallback and repair behavior.

## Phase 5: Maintain the report and validate under contention

1. Update the embedded flaky test report rather than deleting historical evidence. Add a `## Resolutions` section containing one Markdown table with exactly these columns: `test`, `disposition`, `root cause`, `resolution`, `verification`. Give each of the eight tests exactly one row, using only `retained/repaired` or `removed` in `disposition`:
   - five retained/repaired: the two file-completion tests, argument rows while pending, provider usage after unload, and memory write/delete serialization;
   - three removed: model-reference Enter, empty completion/session renewal, and splash startup option versus saved selection.
2. Record why no production semantics changed and replace now-obsolete “needs investigation” language with the resolved root causes.
3. After implementation, run formatting locally, then use the repository's remote build workflow for Rust checks and contention testing. Record actual commands/run counts and outcomes in the report; do not claim iterations that were not run.

# Acceptance Criteria

- **AC.1:** Both retained file-completion tests wait on wall-clock-bounded, target-specific Nucleo convergence with scheduler yielding, contain no fixed tick-count settling loop, and continue to assert their respective transition and previous-results UI contracts.
- **AC.2:** The argument-completion pending-request test still observes stale rows cleared before resolving the new request and no longer performs a one-shot probe assertion.
- **AC.3:** The post-unload provider-usage path deterministically observes `ProviderUsageAck(2)` and fails if the unloaded callback emits a Flash first.
- **AC.4:** The memory serialization test parks the delete's stat for the exact seeded note path after lock acquisition, proves the concurrent write remains blocked, and passes only when delete then write are serialized in that order.
- **AC.5:** The redundant model-reference Enter and empty-completion/session-renewal tests and the misleading splash startup-option-versus-saved test are absent, while named deterministic lower-level/integration tests cover model replacement, Enter routing, cancellation, fresh session allocation, and splash selection behavior.
- **AC.6:** The embedded flaky test report has a machine-checkable resolution table containing each of the eight test names exactly once, five `retained/repaired` and three `removed` dispositions, resolved root causes, and only verification actually performed; unresolved language is either removed or explicitly labeled as historical.
- **AC.7:** Formatting, crate checks, linting, relevant targeted tests, and the complete remote CI workflow pass.

# Test Strategy

| Acceptance criterion | Regression test or observable check |
|---|---|
| AC.1 | `components::file_completion::tests::switching_from_explicit_to_project_restores_project_and_lua_matches`; `components::file_completion::tests::query_refresh_keeps_previous_result_set_until_matching_finishes`, repeated under oversubscription, plus all `components::file_completion::tests` |
| AC.2 | `app::tests::argument_completion_clears_old_rows_while_request_pending`, repeated under oversubscription |
| AC.3 | `loader::tests::provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it`, repeated under oversubscription |
| AC.4 | `write_lock_regression::memory_write_and_delete_serialize_on_shared_lock`, repeated under oversubscription; all `write_lock_regression` tests |
| AC.5 | `components::file_completion::tests::model_replacement_has_prefix_and_trailing_space`, `completion_plugins::ctx_models_flow_into_model_items`, `app::tests::enter_inserts_subagent_reference_with_trailing_space`, `app::tests::unmatched_completion_items_cancel_the_argument_session`, `maki_commands::tests::final_session_owner_drop_cancels_once`, `maki_commands::tests::cancelled_completion_session_reopens_with_fresh_id`, `in_memory_host::splash_picker_startup_option_selects_load_time_contribution`, `in_memory_host::splash_picker_startup_option_typo_keeps_fallback`, `in_memory_host::splash_picker_repairs_unknown_selection`, and `in_memory_host::splash_picker_repairs_malformed_selection`; run `rg 'fn (enter_inserts_model_reference_with_trailing_space|empty_completion_cancels_once_and_next_request_uses_new_session|splash_picker_startup_option_wins_over_saved)' --glob '*.rs'` and require exit status 1/no output |
| AC.6 | Run the exact Python heredoc below to validate the canonical report table, then manually reconcile its verification cells with terminal output before final review. |
| AC.7 | `just fmt-check`; crate-scoped checks/tests while iterating; `just lint`; `.ssh/remote-ci.sh` for full `just ci` on the remote build box |

Validate the report table from the repository root with:

```bash
python3 - <<'PY'
from pathlib import Path

expected = {
    "switching_from_explicit_to_project_restores_project_and_lua_matches",
    "query_refresh_keeps_previous_result_set_until_matching_finishes",
    "enter_inserts_model_reference_with_trailing_space",
    "empty_completion_cancels_once_and_next_request_uses_new_session",
    "memory_write_and_delete_serialize_on_shared_lock",
    "argument_completion_clears_old_rows_while_request_pending",
    "provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it",
    "splash_picker_startup_option_wins_over_saved",
}
section = Path(".agents/makima-plans/91-flaky-tests.md").read_text().split("\n## Resolutions\n", 1)[1]
lines = section.splitlines()
header = next(line for line in lines if line.startswith("|"))
assert [cell.strip() for cell in header.strip().strip("|").split("|")] == [
    "test", "disposition", "root cause", "resolution", "verification"
], header
rows = []
for line in lines:
    if line.startswith("## "):
        break
    if line.startswith("|") and "---" not in line and "| test |" not in line:
        cells = [cell.strip().strip("`") for cell in line.strip().strip("|").split("|")]
        assert len(cells) == 5, cells
        rows.append(cells)
assert len(rows) == 8, rows
names = [row[0].split("::")[-1] for row in rows]
assert set(names) == expected and len(names) == len(set(names)), names
dispositions = [row[1] for row in rows]
assert dispositions.count("retained/repaired") == 5, dispositions
assert dispositions.count("removed") == 3, dispositions
assert all(row[2] and row[3] and row[4] for row in rows), rows
assert all("needs investigation" not in " ".join(row).lower() for row in rows), rows
PY
```

For the contention-sensitive tests, use `cargo nextest run` on the remote build box with `--retries=0` and high `--test-threads` in repeated targeted runs. The five targets are `switching_from_explicit_to_project_restores_project_and_lua_matches`, `query_refresh_keeps_previous_result_set_until_matching_finishes`, `argument_completion_clears_old_rows_while_request_pending`, `provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it`, and `memory_write_and_delete_serialize_on_shared_lock`. Each target must complete at least 100 successful runs under the repository's 64-thread configuration, whether selected together or individually, followed by full remote CI. If time or CI capacity prevents 100 runs for any target, record that target's actual count and flag the reduced evidence rather than silently lowering it.

# Review Strategy

Before handoff, run a `plan_reviewer` pass and resolve all critical/high findings. After implementation and all automated checks, dispatch a `general` review subagent to inspect the diff for accidental production changes, weakened assertions, waits that can succeed on unrelated state, incorrect path-gate synchronization, and report accuracy. Fix or explicitly rebut every finding; repeat review if any critical finding remains.

# Documentation Strategy

No user-facing documentation is needed because runtime behavior and public contracts do not change. The report embedded below is the canonical task artifact. It contains the final test dispositions, root-cause corrections, and measured verification results. A separate report file is not needed.

# Risks, Blockers, and Required Decisions

- Deadline-based waits still need finite bounds. Use one shared named timeout large enough for an oversubscribed runner, always yield, and require the exact target state so the wait cannot pass on stale/unrelated results.
- The path-targeted stat gate must consume the target atomically under a mutex. Comparing outside synchronized state or gating every stat would reintroduce races or deadlock mutable-path computation.
- Receiving `ProviderUsageAck(2)` is intentionally stronger than checking for an empty channel: callback actions precede the Ack. Preserve that runtime ordering assumption in the test structure.
- Removing the splash test deliberately declines a new guarantee that startup choice is durably written before the first frame. Existing behavior is visual precedence followed by asynchronous persistence.
- The single observed 300-second completion timeout could not be tied to an internal blocking path. Removing the redundant test avoids preserving a weak cross-layer harness, but full-suite stress remains necessary to detect whether a separate unidentified hang exists elsewhere.
- No blocker or operator decision remains; the request explicitly permits removing negative-value tests, covering the three removals above.

---

# Flaky test report

Hunt run on the remote build box (12 cores, nextest 0.9.143, rustc 1.97.1),
58 full suite runs (`cargo nextest run --workspace --retries=0`) across
`--test-threads` levels 1, 2, 4, 8, 12, 64. 4739 tests per run.

| thread level | runs | failures |
|---|---|---|
| 1  | 6  | 0 |
| 2  | 6  | 0 |
| 4  | 8  | 0 |
| 8  | 10 | 0 |
| 12 | 10 | 2 |
| 64 (config default) | 18 | 3 |

Note: the repo's `.config/nextest.toml` pins `test-threads = 64` on a 12-core
box, so CI always runs 5x oversubscribed. Every observed flake happened at
high oversubscription; the suite is clean at 1-8 threads. Raw logs are on the
build box under `/home/luna/rsync-ci/flaky-logs/`.

5 failing runs out of 58, 5 distinct tests:

| test | failure mode | seen |
|---|---|---|
| `maki-ui components::file_completion::tests::switching_from_explicit_to_project_restores_project_and_lua_matches` | assertion | t64 |
| `maki-ui components::file_completion::tests::query_refresh_keeps_previous_result_set_until_matching_finishes` | assertion | t12 |
| `maki-ui app::tests::enter_inserts_model_reference_with_trailing_space` | assertion | t64 |
| `maki-ui app::tests::empty_completion_cancels_once_and_next_request_uses_new_session` | timeout (300s) | t64 |
| `maki-lua write_lock_regression::memory_write_and_delete_serialize_on_shared_lock` | assertion | t12 |

## 1. switching_from_explicit_to_project_restores_project_and_lua_matches

Panic: `assertion failed: kinds.contains(&FILE_KIND.to_string())`
at `maki-ui/src/components/file_completion.rs:1342`.

The test injects a project match into nucleo and then waits for it to surface
with a **bounded tick loop**:

```rust
for _ in 0..20 {
    let _ = menu.tick();
    if menu.match_items().iter().any(|item| item.kind == FILE_KIND) { break; }
}
```

Nucleo matching runs on a background thread. Each `tick()` does not yield to
it, so under heavy CPU contention the matcher may not have produced results
within 20 ticks and the loop exits with the previous (lua-only) item set.
Other tests in the same file already use a deadline-based wait
(`refresh_then_accept_inserts_selected_item`,
`maki-ui/src/components/file_completion.rs:2302`) instead of a fixed tick
budget. Suggested fix direction: replace the `0..20` loop with the same
deadline + `yield_now()` pattern.

## 2. query_refresh_keeps_previous_result_set_until_matching_finishes

Panic: `left: []`, `right: ["alpha-file", "beta-file", "gamma-file"]`
at `maki-ui/src/components/file_completion.rs:2346`.

Same root cause as (1). The first phase waits for the three injected files to
appear with `for _ in 0..100 { tick(); ... }` and no deadline; under load the
background matcher had not pushed results within 100 ticks, so `before` was
empty and the assertion on the previous result set failed. The *second* wait
in the same test already uses a 1s deadline, which is why the failure lands on
the first assert. Suggested fix direction: deadline-based wait for
`file_matches.len() == 3` too.

## 3. enter_inserts_model_reference_with_trailing_space

Panic: `left: ""`, `right: "@model:zai/glm-5 "`
at `maki-ui/src/app/tests.rs:6728`.

The typed `@m:glm` was replaced by an empty insertion on Enter. The test
relies on `converge_completion`
(`maki-ui/src/app/tests.rs:6243`), which returns as soon as *any* selectable
item exists. Model items are seeded asynchronously through the test completion
backend, so under contention the first selectable item can be a different
candidate (with an empty insertion) rather than the intended model item, and
Enter accepts that instead. The test never asserts *which* item is selected
before pressing Enter. Suggested fix direction: converge on the expected
model item (match on kind/label) rather than on any selectable item.

## 4. empty_completion_cancels_once_and_next_request_uses_new_session

> Historical. Resolved by removal; see the row for this test in
> [Resolutions](#resolutions) below.

`TIMEOUT [ 300.013s]` (slow-timeout 60s, terminate-after 5). The test hung
somewhere without hitting any of its own 1s-deadline panics, i.e. it blocked
inside a `recv`/lock rather than in the polling loops. Hypotheses worth
checking (not confirmed):

- `StateDir::from_path(env::temp_dir())` shares one global temp dir across
  concurrently running test processes; combined with maki-storage's
  cross-process session locks this is the only cross-process shared state in
  the test, and a lock-holder killed by another test's timeout or an
  undismissed lock could park a waiter indefinitely.
- The probed Lua event handle channels (`maki_lua::test_support`) may lose a
  wakeup under scheduler pressure, leaving a `recv_async` parked forever.

Needs a rerun with backtraces on hang (`RUST_BACKTRACE=full` plus a SIGQUIT
style dump or `--test-threads=64` repro loop) to confirm.

## 5. memory_write_and_delete_serialize_on_shared_lock

> Historical. Root-caused and repaired; see the row for this test in
> [Resolutions](#resolutions) below.

Panic: `delete must complete before the write starts: ["stat" x18, "mkdir",
"write", "stat" x6, "rm"]`
at `maki-lua/src/write_lock_regression.rs:974`.

The probe (`ProbeFs`, `maki-lua/src/write_lock_regression.rs:606`) parks the
delete at its first `stat` behind a gate, and the test's premise is that the
parked delete still holds the memory write lock, so the write cannot run
until after the delete's `rm`. The failing event log shows the write's
`mkdir`/`write` executing *before* the delete's `rm`, and the pre-release
asserts (write must not run while parked) had already passed. Two candidate
explanations:

- The delete releases (or has not yet acquired) the memory lock across the
  stat gate, leaving a window where the parked write task can win the lock
  when the gate opens, i.e. a real, rare lock-scope gap in the memory tool.
- The gate sits outside the lock acquisition, so lock ordering between the
  two tasks is decided by the scheduler once the gate opens.

Since this test exists to guard a previously fixed serialization bug, this
flake deserves a real root-cause pass before anything else. (Resolved: the
unqualified stat gate was the cause; see Resolutions.) Reproduce with a
loop of this single test at high thread counts:
`cargo nextest run -p maki-lua memory_write_and_delete_serialize_on_shared_lock --test-threads=64` (repeat).

## Targeted verification of three additional suspects

800 targeted iterations (400 clean + 400 with a full maki-ui/maki-lua suite
running concurrently as load) of the three tests below, plus a 320-iteration
8-way concurrent burst for the splash test.

| test | runs | failures | verdict |
|---|---|---|---|
| `maki-ui app::tests::argument_completion_clears_old_rows_while_request_pending` | 800 | 1 (loaded) | flaky, confirmed |
| `maki-lua loader::tests::provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it` | 800 | 1 (clean) | flaky, confirmed |
| `maki-lua in_memory_host::splash_picker_startup_option_wins_over_saved` | 15570 | 0 | not flaky at detectable rate |

### argument_completion_clears_old_rows_while_request_pending

Panic: `assertion failed: probe.try_finish_command_arguments(Vec::new()).is_some()`
at `maki-ui/src/app/tests.rs:1144` (last line of the test), once out of 800,
under load. `sync_arguments` hands the completion request to the Lua host
thread asynchronously, and the test then makes a single immediate
`try_finish_command_arguments` attempt with no retry loop. If the host thread
has not surfaced the request yet, the probe returns `None` and the assert
panics. The sibling test `empty_completion_cancels_once_and_next_request_uses_new_session`
already wraps the same probe call in a 1s-deadline polling loop; this test
should do the same.

### provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it

Panic: `assertion failed: actions.try_recv().is_err()` at
`maki-lua/src/loader.rs:1416`, once out of 800. This one is a real test bug,
not just a missing poll: the handler for `Request::ProviderUsageChanged`
(`maki-lua/src/runtime.rs:3708`) emits `UiAction::ProviderUsageAck`
unconditionally whenever no usage status content is registered, which is
exactly the post-unload state. So the second `provider_usage_changed` call
after `unload` legitimately produces an Ack; the final `try_recv().is_err()`
passes only when the test thread wins the race against the host thread
processing the queued request. The test asserts a stronger property than the
system provides. Suggested fix direction: assert that no `UiAction::Flash`
arrives (drain the channel and match on variants), or expect and consume the
Ack explicitly.

### splash_picker_startup_option_wins_over_saved

A coworker reported this test as flaky ("the host never produced a frame
during the 30s window"). Repeated hammering on the build box could not
reproduce it: 800 targeted runs, a 320-run 8-way concurrent burst, a 2400-run
12-way concurrent burst, a 12000-run 12-way concurrent burst, and 58
full-suite runs, all green — 15,520 splash executions plus 58 full-suite
runs. At zero observed
failures the 95% confidence upper bound on the per-run flake rate is about
0.02%, i.e. roughly 1 in 5000 or rarer.

The reported symptom matches a one-time transient boot failure of the Lua
host thread: `splash_pull` (`maki-lua/src/loader.rs:955`) submits the frame
request through `CoalescedLatest` onto the priority lane, and if the lane is
disconnected or the host thread is wedged every pull returns
`SplashPull::Unknown` until the test's 30s deadline panics. Nothing in the
frame path showed instability under 12-way oversubscription here.

If the failure was seen on a different machine or CI runner, environment
factors (different core count/OS, memory pressure, cold build cache) or a
misattributed failure mode are plausible. Next steps if it resurfaces: keep
the raw nextest log, and capture `RUST_BACKTRACE=full`; the panic message
will distinguish `splash frame never matched` (host thread dead/wedged at
boot) from `selection file never contained` (host alive but the picker
persist never ran).

## Resolutions

| test | disposition | root cause | resolution | verification |
|---|---|---|---|---|
| `switching_from_explicit_to_project_restores_project_and_lua_matches` | retained/repaired | Waited for the injected project file with a fixed 20-tick loop that never yielded to the background Nucleo matcher thread; under load the loop expired with the stale lua-only result set. | Replaced with a shared deadline-based `wait_for_matcher` helper (5s budget, `yield_now` per iteration) that waits until `query_refresh_pending` is false and the exact injected `review.txt` item is visible, and added an assertion that the stale explicit candidate `outside.txt` does not survive the transition. Production code unchanged. | Targeted nextest runs, 600 green repetitions (6 chunks of 100) at `--test-threads=64 --retries=0` on the build box (see Appendix). |
| `query_refresh_keeps_previous_result_set_until_matching_finishes` | retained/repaired | First phase waited for the three injected files with a fixed 100-tick loop and no deadline; under load the matcher had not pushed results, so `before` was empty and the previous-results assertion failed. | Both waits (initial three files and the filtered `gamma-file` result) now go through the same `wait_for_matcher` helper; the previous-result-set assertion while `query_refresh_pending` is true is preserved. Production code unchanged. | Targeted nextest runs, 600 green repetitions (6 chunks of 100) at `--test-threads=64 --retries=0` on the build box (see Appendix). |
| `enter_inserts_model_reference_with_trailing_space` | removed | `converge_completion` returns on any selectable item, and model items are seeded asynchronously, so under contention Enter could accept a different candidate. The contract is already covered deterministically at lower levels. | Removed the app-level integration test. Model replacement formatting is covered by `model_replacement_has_prefix_and_trailing_space` (file_completion component tests), real model-source wiring by `ctx_models_flow_into_model_items` (`maki-lua/tests/completion_plugins.rs`), and app-level Enter routing by the retained `enter_inserts_subagent_reference_with_trailing_space`. | `rg 'fn enter_inserts_model_reference_with_trailing_space' --glob '*.rs'` returns no matches; the covering tests pass in CI. |
| `empty_completion_cancels_once_and_next_request_uses_new_session` | removed | Cross-layer test depended on detached executor scheduling and shared temp-dir state; it timed out once at 300s without hitting its own deadlines, and no internal blocking path was found. Its useful properties are covered at deterministic boundaries. | Removed the app-level test. Palette-to-Lua cancellation stays covered by `unmatched_completion_items_cancel_the_argument_session`, once-only cancellation by `final_session_owner_drop_cancels_once`, and the fresh-session contract by a new synchronous maki-commands test `cancelled_completion_session_reopens_with_fresh_id`. No timeouts, storage, or session-lock code changed. | `rg 'fn empty_completion_cancels_once_and_next_request_uses_new_session' --glob '*.rs'` returns no matches; `cancelled_completion_session_reopens_with_fresh_id` passes in CI. |
| `memory_write_and_delete_serialize_on_shared_lock` | retained/repaired | The stat gate was an unqualified first-stat latch, so a stat issued by the delete dispatch before its lock-protected note existence check could consume the latch; the delete then parked outside the lock and the write could run first. | `arm_stat_gate` now takes the seeded note's `PathBuf` and `ProbeFs::stat` atomically consumes the gate under a mutex, parking only when the incoming path equals the note path. This parks the delete at its note existence check after lock acquisition. Serialization, non-overlap, and rm-before-write assertions are unchanged. Production code unchanged. | Targeted nextest runs, 600 green repetitions (6 chunks of 100) at `--test-threads=64 --retries=0` on the build box (see Appendix). |
| `argument_completion_clears_old_rows_while_request_pending` | retained/repaired | Made a single immediate `try_finish_command_arguments` attempt; if the Lua host thread had not surfaced the queued request yet the probe returned `None` and the final assert panicked. | The render assertion (stale rows cleared before resolution) is unchanged and still runs first; the probe call is now wrapped in the same bounded deadline/yield loop the sibling tests use, finishing the request with an empty result so no detached task remains. Production code unchanged. | Targeted nextest runs, 600 green repetitions (6 chunks of 100) at `--test-threads=64 --retries=0` on the build box (see Appendix). |
| `provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it` | retained/repaired | Asserted the action channel was empty after the post-unload invalidation, but the runtime emits `ProviderUsageAck` unconditionally when no usage status is registered; the assert only passed when the test thread won a race against the host thread. | The final check now receives with a bounded timeout and requires `ProviderUsageAck(2)`. Because callback actions are emitted before the Ack, receiving the Ack first also deterministically proves the unloaded callback emitted no `Flash`. Production code unchanged. | Targeted nextest runs, 600 green repetitions (6 chunks of 100) at `--test-threads=64 --retries=0` on the build box (see Appendix). |
| `splash_picker_startup_option_wins_over_saved` | removed | Accepted any nonempty frame, so it did not establish the claimed before-first-frame ordering; enforcing that ordering would need a synchronous filesystem durability barrier the runtime does not provide. Startup selection stays covered by deterministic tests. | Removed the test. `splash_picker_startup_option_selects_load_time_contribution` verifies an identifiable renderer and eventual persisted choice; the typo and repair tests cover fallback and repair behavior. Visual precedence followed by asynchronous persistence remains the documented behavior. | `rg 'fn splash_picker_startup_option_wins_over_saved' --glob '*.rs'` returns no matches; the covering splash tests pass in CI. |

No production semantics changed anywhere in this work: every fix is confined
to test helpers and assertions (Nucleo convergence waits, probe gating,
provider-usage acknowledgement, completion session coverage), and the three
removals replace tests whose contracts are preserved at deterministic lower
levels. The historical "needs a hang dump" and "deserves a real root-cause
pass" language above refers to the pre-fix state and is resolved by the
retained/repaired rows: finding 4's cross-layer harness was removed rather
than root-caused further, and finding 5's root cause was the unqualified stat
gate described in the resolutions table.

## Appendix: commands run

Everything ran on the remote build box (`luna@100.122.23.69`, 12 cores,
nextest 0.9.143, rustc 1.97.1), driven over SSH from the local VM. The
working tree was rsync-mirrored once and never rebuilt afterwards (same
sources for all runs):

```bash
rsync -a --delete \
  --exclude 'target/' --exclude '.git/' --exclude '.ssh/' \
  -e "ssh -i .ssh/id_ed25519 -o BatchMode=yes -o IdentitiesOnly=yes" \
  ./ luna@100.122.23.69:/home/luna/rsync-ci/maki
```

Each phase was a self-contained bash script uploaded to
`/home/luna/rsync-ci/` and run detached under the shared CI flock, so it
never collided with a `remote-ci.sh` run:

```bash
setsid nohup sh -c "flock -w 120 /home/luna/rsync-ci/.maki.lock \
  bash /home/luna/rsync-ci/<script>.sh" > <script>.out 2>&1 < /dev/null &
```

Shared env in every script:

```bash
export OPENSSL_NO_VENDOR=1
export CARGO_TARGET_DIR=/run/media/root/0b89f0d4-dce6-4cae-8b80-24350f551852/data/maki-target
```

Phase 1, full-suite matrix (script `flaky-hunt.sh`, logs in
`flaky-logs/`, results in `flaky-summary.tsv`): 58 runs, one nextest
invocation per run, `--retries=0` so nothing is auto-retried, thread level
varying per block (6x1, 6x2, 8x4, 10x8, 10x12, 18x64):

```bash
cargo nextest run --workspace --retries=0 --test-threads="$threads"
```

Failures were extracted from each run's log with:

```bash
grep -E '^[[:space:]]*(FAIL|TIMEOUT) \[' "$out" | \
  sed -E 's/^[[:space:]]*[A-Z]+ \[[^]]*\][[:space:]]*(\([0-9]+\/[0-9]+\))?[[:space:]]*//'
```

Phase 2, targeted verification of the three coworker-suspected tests (script
`flaky-targeted.sh`, logs in `flaky-targeted-logs/`): 400 clean iterations,
then 400 iterations with a full-suite load loop running concurrently in the
background:

```bash
cargo nextest run -p maki-ui -p maki-lua --retries=0 --test-threads=4 \
  argument_completion_clears_old_rows_while_request_pending \
  splash_picker_startup_option_wins_over_saved \
  provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it

# background load during the second 400:
cargo nextest run -p maki-ui -p maki-lua --retries=0 --test-threads=64  # in a while-true loop
```

Phase 3, splash test bursts (scripts `flaky-splash.sh`, `flaky-splash2.sh`,
`flaky-splash3.sh`, logs in `flaky-splash{,2,3}-logs/`): N concurrent
nextest worker processes each running the single test in a loop — 8 workers
x 40, 12 x 200, 12 x 1000 (320, 2400 and 12000 runs). Oversubscribing the
box with concurrent test processes is what creates scheduler pressure; the
single-test nextest invocation per iteration was:

```bash
cargo nextest run -p maki-lua --retries=0 --test-threads=4 \
  splash_picker_startup_option_wins_over_saved
```

Phase 4, post-fix contention verification. Two campaigns ran with the same
methodology: for each of the five retained/repaired tests, sequential
single-test runs with `--retries=0 --test-threads=64` while a full
maki-ui+maki-lua suite loop ran concurrently as load:

```bash
while true; do
  cargo nextest run -p maki-ui -p maki-lua --retries=0 --test-threads=64
done  # background load during the verification runs

cargo nextest run -p <pkg> --retries=0 --test-threads=64 <test>  # x100 per chunk
```

Campaign 1 (script `flaky-verify.sh`, logs in `flaky-verify-*.log`): 100
runs per target, all first-attempt green. After a review pass strengthened
the stale-candidate assertion in the switching test (see below), campaign 2
(script `flaky-3000.sh`, logs in `flaky-3000-*.log`, summary in
`flaky-3000-summary.txt`) ran the final sources at 600 runs per target,
executed as 6 chunks of 100 per test for management purposes. All 30 chunks
completed between 03:23 and 05:40 (box local time) with zero failures.

Campaign 2 results, 600 runs per target, all first-attempt green:

| test | chunks | runs | failures |
|---|---|---|---|
| `switching_from_explicit_to_project_restores_project_and_lua_matches` | 6 | 600 | 0 |
| `query_refresh_keeps_previous_result_set_until_matching_finishes` | 6 | 600 | 0 |
| `argument_completion_clears_old_rows_while_request_pending` | 6 | 600 | 0 |
| `provider_usage_publication_updates_mirror_before_callback_and_unload_cleans_it` | 6 | 600 | 0 |
| `memory_write_and_delete_serialize_on_shared_lock` | 6 | 600 | 0 |

A full `just ci` run on the box passed after the fixes (fmt, clippy, 4756
tests, docgen, machete). One unrelated transient failure of
`maki-lua::subagent_run_end::failed_subagent_turn_resolves_and_same_session_recovers`
occurred in the first CI run and did not reproduce on the second; it is not
one of the tests in this report and was left untouched. After a final review
pass strengthened the stale-candidate assertion in
`switching_from_explicit_to_project_restores_project_and_lua_matches`
(compare against `../outside.txt`, the display-prefixed label), that test was
re-verified with a 100-run block and then absorbed into the 3000-run campaign
below, all on the final sources with 0 failures, and `just ci` was re-run
green on those exact sources.

Per-run PASS/FAIL summaries landed in `flaky*-summary.tsv` on the box and
full stdout/stderr of every run in the `flaky*-logs/` dirs; failing-run
panics were pulled back with `grep -E 'panicked at'` plus the surrounding
stderr block. Totals: 58 + 800 + 14720 + 3600 = 19,178 suite or single-test
runs (the 3600 covers both post-fix campaigns: 500 + 100 + 3000).
No retries were configured anywhere (`--retries=0`) so every recorded
failure is a first-attempt failure.
