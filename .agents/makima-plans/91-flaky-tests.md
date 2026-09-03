# Goal

Eliminate the eight contention-sensitive or incorrectly asserted tests identified in `flaky_test_report.md` by making valuable tests deterministic and removing redundant, timing-dependent tests. Update the report so it records each disposition and the verification results.

# Implementation Summary

This is primarily test-harness work in `maki-ui`, `maki-lua`, and `maki-commands`; production behavior should not change. Keep five tests that protect distinct UI, callback, and locking contracts, replacing fixed polling budgets or invalid asynchronous assertions with observable completion barriers. Remove three negative-value integration tests and preserve their useful contracts at deterministic boundaries.

Affected touch points:

- `maki-ui/src/components/file_completion.rs`: deterministic Nucleo convergence for two file-completion tests.
- `maki-ui/src/app/tests.rs`: deterministic argument-request cleanup; removal of redundant model-Enter and empty-result cross-layer tests.
- `maki-commands/src/tests.rs`: deterministic coverage that canceling and reopening completion allocates a fresh session ID.
- `maki-lua/src/loader.rs`: assert the actual post-unload provider-usage acknowledgement.
- `maki-lua/src/write_lock_regression.rs`: gate the delete operation after lock acquisition by matching the note path.
- `maki-lua/tests/in_memory_host.rs`: remove the timing-dependent splash startup test.
- `flaky_test_report.md`: mark all eight findings fixed or removed, explain the final disposition, and append verification evidence.

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

1. Update `flaky_test_report.md` in place rather than deleting historical evidence. Add a `## Resolutions` section containing one Markdown table with exactly these columns: `test`, `disposition`, `root cause`, `resolution`, `verification`. Give each of the eight tests exactly one row, using only `retained/repaired` or `removed` in `disposition`:
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
- **AC.6:** `flaky_test_report.md` has a machine-checkable resolution table containing each of the eight test names exactly once, five `retained/repaired` and three `removed` dispositions, resolved root causes, and only verification actually performed; unresolved language is either removed or explicitly labeled as historical.
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
section = Path("flaky_test_report.md").read_text().split("## Resolutions", 1)[1]
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

No user-facing documentation is needed because runtime behavior and public contracts do not change. `flaky_test_report.md` is the canonical task artifact and must be updated with the final test dispositions, root-cause corrections, and measured verification results. Do not add a separate documentation file.

# Risks, Blockers, and Required Decisions

- Deadline-based waits still need finite bounds. Use one shared named timeout large enough for an oversubscribed runner, always yield, and require the exact target state so the wait cannot pass on stale/unrelated results.
- The path-targeted stat gate must consume the target atomically under a mutex. Comparing outside synchronized state or gating every stat would reintroduce races or deadlock mutable-path computation.
- Receiving `ProviderUsageAck(2)` is intentionally stronger than checking for an empty channel: callback actions precede the Ack. Preserve that runtime ordering assumption in the test structure.
- Removing the splash test deliberately declines a new guarantee that startup choice is durably written before the first frame. Existing behavior is visual precedence followed by asynchronous persistence.
- The single observed 300-second completion timeout could not be tied to an internal blocking path. Removing the redundant test avoids preserving a weak cross-layer harness, but full-suite stress remains necessary to detect whether a separate unidentified hang exists elsewhere.
- No blocker or operator decision remains; the request explicitly permits removing negative-value tests, covering the three removals above.
