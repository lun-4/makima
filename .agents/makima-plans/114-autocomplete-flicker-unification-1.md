## Goal

Eliminate transient shrink/grow frames while filtering `@` completion and the standalone file picker by centralizing the policy that publishes only settled Nucleo query results. Audit every other completion surface and preserve its existing behavior where intermediate asynchronous matcher snapshots cannot reach the screen.

## Implementation Summary

Add a private `maki-ui` completion refresh state machine in `maki-ui/src/components/` and migrate the two asynchronously filtered Nucleo surfaces to it: `FileCompletionMenu` (`@` files plus Lua reference items) and `FilePickerModal`. The helper will separate a query's pending matcher work from the last committed result snapshot, coalesce Nucleo's intermediate `Status { changed: true, running: true }` states, permit progressive file-discovery updates when no query change is pending, and expose whether displayed rows are safe to accept.

Keep ranking, candidate materialization, layout, and source-specific behavior in the owning components. Do not migrate synchronous command-name filtering, synchronous `ListPicker`/search filtering, or request/response command-argument completion because they do not publish incremental Nucleo snapshots. No user-visible matching syntax, ordering, popup limits, or configuration contract changes are intended.

Affected touch points:

- New private helper module registered from `maki-ui/src/components/mod.rs`.
- `maki-ui/src/components/file_completion.rs` for `@` query publication and mixed file/reference snapshots.
- `maki-ui/src/components/file_picker.rs` for modal query publication.
- Focused unit/component tests in those same files; command/list/search tests remain audit evidence rather than migration targets.

## Implementation Plan

### Phase 1: Centralize incremental matcher publication policy

1. Add a small private state machine, named around coherent/incremental completion publication rather than a specific widget. It must track whether the current Nucleo query has an uncommitted result and expose candidate-acceptance readiness.
2. Give the helper explicit transitions for:
   - a query reparse, marking the currently displayed result snapshot stale only when there are indexed candidates whose rematch is asynchronous;
   - observation of a Nucleo snapshot frontier, returning a decision such as `Wait`, `Stream`, or `Commit`;
   - readiness for accepting a displayed candidate;
   - whether matcher work requires pending repaint cadence.
3. Determine query coherence from the semantic pattern in the public Nucleo snapshot, not `Status::running` or item counts. At each query reparse, clone the requested one-column `Pattern` (or its atom vector) from `nucleo.pattern.column_pattern(0)`. `MultiPattern` itself is not equality-comparable in Nucleo 0.5 and must not be used as the key. After each `tick`, a snapshot is publishable for the pending query when `snapshot.pattern().column_pattern(0).atoms` equals the requested pattern atoms.
4. Define the policy precisely:
   - after a query change, retain the last committed rows until Nucleo publishes a complete worker snapshot produced with the latest requested semantic pattern;
   - treat items concurrently being initialized or injected as later discovery, not part of query settlement. Every Nucleo snapshot is internally atomic for the worker corpus it processed, and all candidates visible in the prior committed snapshot are already initialized rather than in-flight;
   - do not compare `Snapshot::item_count()` with `Injector::injected_items()`: Nucleo's concurrent boxcar count is not a contiguous initialized frontier, and either live or fixed count comparisons can starve or falsely certify publication;
   - do not require `Status::running == false`, walker completion, or zero active injectors, because Nucleo can publish a complete current-pattern snapshot while scheduling another pass for later discovery;
   - once the new query has a first current-pattern commit, publish later `changed` snapshots with the same semantic pattern so background discovery continues to stream;
   - reject snapshots for an older pattern; repeated query changes replace the requested-pattern key, so only the latest query may commit.
5. Define the zero-indexed-item path explicitly: if no file candidate has been injected, no asynchronous file rematch can invalidate the combined list, so publish synchronously filtered Lua references immediately and leave Nucleo to stream future files under the requested pattern. If indexed files exist, retain the old mixed snapshot until current-pattern publication.
6. Keep candidate materialization and rendering outside the helper, but let the helper own the comparable requested `Pattern`/atoms, pending state, and Nucleo status/snapshot inputs needed for the shared decision. Avoid a broad completion controller that would couple the different ranking and layout models.
7. Add deterministic unit tests for stale-pattern rejection, current-pattern commit despite `running = true`, rapid consecutive queries, zero-indexed-item immediate readiness, later discovery streaming, acceptance readiness, and pending cadence.

### Phase 2: Migrate `@` completion to the shared policy

1. Replace `Session::query_refresh_pending` and overlapping matcher-pending bookkeeping in `maki-ui/src/components/file_completion.rs` with the shared helper. Continue retaining the `walking` state separately because it controls discovery and spinner behavior.
2. In `sync_query`, reparse Nucleo, capture the latest requested column `Pattern`/atoms in the helper, and mark a query transition before any visible result rebuild when indexed files exist. Continue computing the new `QueryIntent` and Lua `ref_matches` synchronously, but do not publish them into `matches` until the file side has a snapshot for that same semantic pattern. This preserves the entire previous mixed snapshot rather than exposing a partial new-query source set. When no file is indexed, publish reference-only filtering immediately as the explicit zero-item path.
3. In `tick`, call Nucleo, then pass the current snapshot's column pattern and status into the helper:
   - on `Wait`, leave `file_matches`, combined `matches`, selection, scroll, and popup geometry untouched;
   - on `Commit`, refresh file matches from that current-pattern snapshot and atomically rebuild the combined file/reference list for the latest query, even if Nucleo reports `running = true` because later discovery work has started;
   - on `Stream`, retain current progressive walker behavior by refreshing and rebuilding from a later same-pattern snapshot.
4. Reset query-driven selection and scroll when the new query commits rather than when its reparse begins. Preserve existing clamp/visibility behavior for discovery-only updates. Do not expand this fix into selection-by-identity behavior, which is independent of the reported frame flicker.
5. Gate Enter/Tab candidate acceptance through the helper's committed/readiness state so stale rows are never inserted. Preserve the current `Passthrough` behavior while pending to avoid an unrelated key-routing change.
6. Preserve all current source and ranking contracts in `parse_query`, `match_candidate`, and `rebuild_combined`: file tie precedence, Lua source order, kind-prefix filtering, smart normalization/case behavior, explicit path completion, and the existing lowercase Nucleo query safety rule.
7. Keep initial visibility debounce and explicit-path completion behavior intact. Explicit paths are synchronously discovered and should commit immediately without incremental Nucleo hysteresis.

### Phase 3: Migrate the standalone file picker

1. Add the shared refresh state to `FilePickerModal::Session` and mark it pending from every operation that reparses the query: character insertion, Backspace, delete-word, and paste.
2. Change `tick` to use the shared publication decision instead of rebuilding on every `status.changed`:
   - retain old rows, count metadata, selection/scroll state, and therefore modal dimensions while the snapshot has an old semantic pattern;
   - commit `refresh_matches` and selection clamping as soon as a complete Nucleo snapshot matches the latest requested column pattern, regardless of concurrently in-flight discovery, walker completion, or `Status::running`;
   - continue streaming later same-pattern snapshots as newly initialized files are matched.
3. Prevent Enter from selecting a displayed stale path while a query is pending. Return the picker's existing consumed action until the latest result snapshot commits.
4. Preserve file-walker completion/error behavior, visibility debounce, truncation/materialization accounting, ranking, scroll behavior, and popup size limits.

### Phase 4: Complete and record the autocomplete-surface audit in code/test scope

1. Verify no shared-state migration is needed for:
   - slash command names, because `CommandPalette::tick` drains Nucleo synchronously before rendering (`maki-ui/src/components/command.rs`);
   - command arguments, because request generations reject stale async replies and candidates are filtered atomically after a response (`maki-ui/src/components/command.rs`);
   - `ListPicker` consumers such as task, theme, model, login, MCP, rewind, and Lua pickers, because filtering is synchronous (`maki-ui/src/components/list_picker.rs`);
   - chat search, because it synchronously scans the current message set (`maki-ui/src/components/search_modal.rs`);
   - Lua completion sources, because they only provide `@` items and Rust owns filtering/publication.
2. Add no abstraction to these surfaces solely for uniformity. Keep the centralized helper specific to incremental Nucleo publication, the shared failure mode discovered by the audit.
3. Update any comments/tests that still describe `query_refresh_pending` directly so they describe the shared settled-publication contract instead.

## Acceptance Criteria

- **AC.1:** After an `@` query changes with indexed files present, old-pattern Nucleo snapshots leave the complete previously rendered result set and popup rectangle unchanged; the latest mixed file/reference result set appears in one committed update at the first complete current-pattern snapshot.
- **AC.2:** Rapid consecutive `@` query changes publish only the final query's results, Enter/Tab cannot insert a candidate from a stale displayed snapshot, and reference-only completion remains immediate when no file candidate is indexed.
- **AC.3:** The standalone file picker retains its committed rows and modal dimensions through old-pattern snapshots, then publishes the first complete current-pattern snapshot in one update; Enter cannot select a stale row while pending.
- **AC.4:** Both affected surfaces commit a changed query from a current-pattern snapshot even if a concurrent producer remains inside an item fill or keeps injecting, and subsequently publish additional same-pattern discovery snapshots before the walker finishes.
- **AC.5:** Explicit-path `@` completion remains immediate; file/reference matching and precedence, file-picker materialization/truncation, initial visibility debounce, selection/scroll bounds, and idle/pending/spinner cadence retain their existing observable behavior outside publication timing.
- **AC.6:** The coherent-publication decision is implemented once in a private shared helper and both incremental Nucleo consumers route snapshot publication through it; no duplicate component-local pending-query policy remains.
- **AC.7:** The touched crate passes formatting, compilation, linting, and its focused/full test suites without new failures.

## Test Strategy

| Acceptance criterion | Named validation |
|---|---|
| AC.1 | Shared-helper test `stale_pattern_waits`; `file_completion` regression `query_refresh_keeps_mixed_snapshot_and_geometry_until_current_pattern` using the existing in-memory Nucleo/session and `TestBackend` seams. |
| AC.2 | Shared-helper tests `repeated_queries_commit_latest_pattern` and `zero_indexed_items_are_ready_immediately`; `file_completion` regressions `pending_query_rejects_stale_candidate_and_commits_latest_query` and `reference_only_query_publishes_immediately`, exercising Enter and Tab before and after convergence. |
| AC.3 | `file_picker` regressions `query_refresh_keeps_rows_and_geometry_until_current_pattern` and `pending_query_consumes_enter_until_commit`, built on `pending_picker`, `inject_file`, and bounded `tick_until`. |
| AC.4 | Shared-helper tests `current_pattern_commits_while_running` and `later_same_pattern_snapshot_streams`; component tests `file_completion_query_commit_survives_in_flight_injection` and `file_picker_query_commit_survives_in_flight_injection`. Each component test must establish old rows, reparse, deliberately hold one injector inside its fill closure while another initialized item proceeds, observe a current-pattern commit before the held producer or walker completes, release it, and observe a later same-pattern publication. Use channels/barriers and a deadline so injection is demonstrably active at first commit. |
| AC.5 | `file_completion`: `explicit_completion_commits_without_pending_refresh`; existing `host_and_guest_candidates_share_heuristic_order`, `skill_prefix_filters_to_skills_only`, `subagent_prefix_filters_to_subagents`, `model_prefix_filters_to_models`, `case_insensitive_ranking_and_codepoint_highlights`, `equal_rank_preserves_source_order`, `file_refresh_uses_lexical_source_order_for_ties`, `uppercase_file_query_does_not_panic`, `uppercase_ref_query_does_not_panic`, and `pending_debounce_controls_visibility`; new `pending_query_resets_selection_and_scroll_only_on_commit` and `publication_state_preserves_pending_and_spinner_cadence`. `file_picker`: existing `refresh_tracks_materialization_boundary`, `refresh_uses_lexical_source_order_for_ties`, `pending_debounce_controls_visibility`, `resize_clamps_scroll_offset`, `a_matcher_mid_answer_keeps_the_loop_coming_back`, and `settled_picker_owes_no_frame_and_does_not_animate`. |
| AC.6 | Add `components/coherent_completion.rs` unit test `both_incremental_consumers_use_shared_policy`, a source-contract test that reads `file_completion.rs` and `file_picker.rs`, requires both to import/hold the shared helper, and rejects the retired `query_refresh_pending` field plus direct publication conditions based solely on `status.changed`/`status.running`. The completion-surface audit itself is recorded in Phase 4 and does not impose new behavioral acceptance criteria on unchanged surfaces. |
| AC.7 | Run `cargo fmt --all -- --check`, `cargo check -p maki-ui --tests`, focused `cargo nextest run -p maki-ui` filters for the helper/file completion/file picker tests, then `cargo clippy -p maki-ui --all-targets -- -D warnings` and `cargo nextest run -p maki-ui`. If repository policy or touched dependencies require broader validation, finish with `just check`, `just lint`, and `just test`. |

Tests involving Nucleo workers must use existing wall-clock deadlines plus `yield_now`, not fixed tick counts or sleeps. At least one component test per migrated surface must use real Nucleo status and snapshot transitions in addition to the deterministic state-machine tests. Use channel/barrier-controlled injection, including a producer held inside its fill closure, while the done sender remains alive. Assert first current-pattern publication occurs while `walking` and injection are still active, then release/complete another candidate and assert a later progressive publication.

## Review Strategy

Before handoff, run a `plan_reviewer` subagent and resolve or explicitly rebut every finding; repeat after any critical/high correction.

After implementation and all automatable validation, dispatch a `general` review subagent focused on state-machine correctness, Nucleo `changed`/`running` semantics, long-running walker behavior, stale-candidate acceptance, and accidental behavior changes in explicit completion or repaint cadence. Fix or explicitly rebut every finding, repeating review if any critical issue remains.

## Documentation Strategy

No user-facing documentation is needed because this restores stable rendering without changing completion syntax, controls, configuration, or documented results. Keep concise implementation comments near the shared state machine where Nucleo's `changed` versus `running` semantics are non-obvious; update existing local comments in the migrated components rather than adding a separate architecture document.

## Risks, Blockers, and Required Decisions

- Nucleo 0.5 does not identify query generations in `Status`, `running` also covers later injected items, and `MultiPattern` does not implement equality. The wrapper must compare the public one-column `Pattern::atoms`; tests must exercise this against actual Nucleo workers.
- Nucleo's injected and snapshot item counts are not contiguous initialized frontiers because `Injector::push` reserves before its fill closure completes. Do not use count equality or watermarks as a coherence proof. The current semantic pattern identifies query completion; in-flight candidates are later discovery and must appear in subsequent same-pattern snapshots. Controlled tests must hold a fill closure open to validate this boundary.
- `@` completion mixes synchronous Lua reference filtering with asynchronous file matching. Publishing `ref_matches` early would recreate a partial-source transition, so only the combined snapshot may change at commit.
- Existing `@` pending Enter/Tab behavior is intentionally preserved as `Passthrough`; changing whether Enter submits the prompt is outside this visual-stability fix.
- Existing numeric selection clamping during discovery can move selection identity when ranking changes. That behavior predates this issue and is not required to eliminate intermediate frames; selection-by-identity should remain out of scope unless implementation reveals it is necessary for safe publication.
- The repository has known order-dependent `maki-ui` test flakes. Any failure must be rerun in isolation and compared with the known flaky component tests before attribution, while all new tests must remain deterministic.