## Goal

Replace the narrow Nucleo-only completion fix with one maintained settled-publication protocol for every completion surface. Preserve the last committed popup while replacement work is pending, prohibit stale acceptance, publish complete replacements atomically, and eliminate the `/model` popup disappearance captured in `/home/philpax/Videos/Recordings/recording-20260905-164700.mp4`.

## Implementation Summary

Introduce a private `maki-ui` publication module that separates the universal display lifecycle from producer-specific freshness proofs:

- a generic committed/pending publication owner for complete render-affecting payloads;
- a Nucleo pattern adapter for `@` completion, the standalone file picker, and asynchronously ticked slash-command names;
- request-generation publication for command arguments;
- an immediate commit path for synchronous `ListPicker` and chat-search filtering.

Refactor the current uncommitted `components/coherent_completion.rs` work into this broader design rather than layering a second policy beside it. Convert slash-command matching from synchronous Nucleo draining to normal event-loop polling, and route both command-name and command-argument work through `CommandPalette::tick` and `CommandPalette::cadence`. Keep matching, ranking, source precedence, rendering, command lifecycle notifications, insertion syntax, and component-specific navigation in their existing owners.

The recording establishes the primary regression: around 2.121s and 2.571s `/model` argument candidates are visible; around 2.130–2.144s and 2.585–2.609s the entire popup disappears and exposes the animated splash; the replacement appears around 2.155s and 2.612s. The direct cause is `CommandPalette::sync_arguments` calling `reset_argument_state`, which clears committed rows before its asynchronous response arrives (`maki-ui/src/components/command.rs`).

Affected touch points:

- `maki-ui/src/components/coherent_completion.rs`, renamed or rewritten as the generic publication module.
- `maki-ui/src/components/mod.rs` module registration.
- `maki-ui/src/components/file_completion.rs` and `file_picker.rs` migration from the narrow helper.
- `maki-ui/src/components/command.rs` for command-name and argument publication, acceptance, polling, and cadence.
- `maki-ui/src/app/mod.rs`, `app/view.rs`, and `app/tests.rs` for unified polling/cadence and full-frame regressions.
- `maki-ui/src/components/list_picker.rs` and `search_modal.rs` for synchronous commits through the common protocol.
- Existing async `ListPicker` consumers (`model_picker.rs`, `mcp_picker.rs`, task refresh, Lua picker) are audited and retain their complete-vector replacement contracts.
- No public configuration, Lua API, command API, or persistence schema changes.

## Implementation Plan

### Phase 1: Build one settled-publication protocol

1. Replace `CoherentCompletion` with a small private generic publication owner whose committed value is the complete visible snapshot, not merely a pending bit. Use a shape equivalent to `Published<K, T>` with:
   - an optional committed `{ key, value }`;
   - a monotonically wrapping request generation;
   - an optional pending `{ generation, key }`;
   - explicit `begin`, current-generation `commit`, same-key `stream`, `commit_sync`, `clear`, and `cancel/invalidate` transitions;
   - read-only access to the committed value while pending;
   - `can_accept`, `is_pending`, and pending-cadence accessors;
   - decisions such as `Wait`, `Commit`, `Stream`, and `Clear` so owners apply selection/scroll effects exactly once.
2. Make the invariants structural:
   - `begin` never destroys the committed snapshot;
   - only the latest generation and expected semantic key may commit;
   - pending committed rows remain renderable but are never acceptable;
   - a replacement payload is built completely before one atomic assignment;
   - `stream` is legal only for the currently committed key and cannot overtake a newer pending generation;
   - authoritative empty/error/cancel paths clear explicitly rather than representing pending work as an empty committed list;
   - no-op edits do not begin a generation.
3. Define complete component payloads so all geometry-affecting result state commits together:
   - `@`: current `QueryIntent`, reference matches, file matches, combined matches, and coarse/materialized/final/truncation metadata;
   - file picker: matches and all count/truncation metadata;
   - command arguments: filtered argument rows and replacement range;
   - command names: filtered rows plus query/argument-count data needed to interpret and accept them;
   - `ListPicker`: filtered indices and match-highlight indices;
   - search: complete search-match vector.
   Selection, scroll, viewport caches, walkers, receivers, Nucleo instances, source vectors, and render code remain component-owned. `Commit` resets query-driven selection/scroll; `Stream` clamps without resetting.
4. Add a `PatternKey` adapter that clones the public one-column `Pattern::atoms`. It begins after Nucleo reparse and maps observations as follows:
   - unchanged or old-pattern snapshot: `Wait`;
   - first changed snapshot matching the latest pattern: `Commit`, regardless of `Status::running`;
   - later changed snapshot with the committed pattern and no newer pending request: `Stream`.
   Preserve the current rule that item counts, walker completion, active injectors, and `Status::running == false` are not coherence proofs.
5. For zero indexed items, allow an immediate commit where no asynchronous rematch can invalidate existing candidates, then stream later discovery. Preserve the current explicit-path synchronous commit. Do not force a newly started empty project walker to delay reference-only results.
6. Use request generation directly as the command-argument freshness key. Keep `CompletionSession` and its lower-level cancellation/request-ID validation unchanged; the UI protocol owns only visible publication and acceptance readiness.
7. Add deterministic protocol tests for retained committed values, stale key/generation rejection, rapid supersession, atomic commit, same-key streaming, stream rejection behind a newer request, synchronous commit, explicit clear, cancellation invalidating late results, readiness, and cadence. Preserve focused Nucleo tests for stale patterns, current-pattern commit while running, zero-item readiness, and later streaming.

### Phase 2: Refactor the file surfaces onto the generic owner

1. Migrate `FileCompletionMenu` from separate `ref_matches`, `pending_ref_matches`, `file_matches`, `matches`, and count fields into one committed `FileCompletionSnapshot` plus only the producer state needed to build the next snapshot.
2. In `sync_query`, compute pending Lua reference matches and the new intent without mutating the committed snapshot. Reparse Nucleo and begin a pattern generation when indexed files require asynchronous rematching. On immediate/explicit paths, build and synchronously commit the complete snapshot.
3. In `tick`, materialize files into a local replacement snapshot. On `Commit`, atomically install pending references, files, combined ranking, and counts, then reset selection/scroll. On `Stream`, rebuild the complete current-query snapshot and clamp existing selection. On `Wait`, do not mutate visible rows, counts, selection, scroll, or geometry.
4. Gate Enter/Tab through `can_accept`, preserving `Passthrough` while pending. Preserve explicit-path advancement, source ordering, file tie precedence, smart case/normalization behavior, the lowercase Nucleo retrieval safety rule, popup limits, visibility debounce, and walker/spinner behavior.
5. Migrate `FilePickerModal` to a committed `FilePickerSnapshot`. Query edits begin a pattern generation only when text changes. `tick` atomically commits or streams rows and count metadata; Enter is consumed while pending. Preserve empty-directory/error closure, walker behavior, truncation/materialization, ranking, dimensions, and scrolling.
6. Retain and strengthen the existing real-Nucleo controlled-injection tests. Compare complete `TestBackend` buffers, including styles, while waiting rather than only rectangle/row vectors. Assert current-pattern publication occurs while injection/walking remains active, then later same-pattern discovery streams.

### Phase 3: Centralize command-name publication and make it incremental

1. Replace the synchronous `CommandPalette::tick(query)` drain loop (`while status.running`) with nonblocking command-name producer state. Define a `CommandContextKey` containing the normalized command word, registry generation, and argument count, plus pending input/cursor/mode context needed to start argument completion after command settlement.
2. Split command synchronization into an ordered pipeline owned by `CommandPalette`, rather than leaving `App` to call two independent state machines:
   - `sync_input(input, cursor, mode)` parses command word and argument count, refreshes the registry snapshot if needed, and records the latest full input context;
   - if command word or registry generation changed, reparse Nucleo, begin a command-name generation, retain old rows as display-only, immediately invalidate/cancel argument authority, and do **not** issue an argument request against retained command rows;
   - `tick()` calls `nucleo.tick(0)`, routes status/snapshot pattern through the adapter, commits the latest command-name payload, resolves the selected current command, then starts/resynchronizes argument completion for the latest stored input context;
   - `cadence()` reports `PENDING` while either stage owes an answer.
3. Handle semantic changes that do not require a Nucleo rematch explicitly. When the command word and registry generation are unchanged but argument count changes, rematerialize eligibility synchronously from the already committed/current-pattern snapshot and `commit_sync` a new `CommandContextKey`; never begin a generation waiting for `Status::changed`. Cursor/mode/argument-token changes with the same resolved command bypass command-name matching and begin only a new argument request.
4. Couple every argument request key to `{ command_publication_generation, command_id, invoked_name, argument_index, argument_query/range, mode }`. A response may publish or emit lifecycle callbacks only if that complete key still matches the latest committed command context. When a command-name commit changes the selected/resolved command, cancel/invalidate any prior argument request and automatically resynchronize arguments from the stored latest input context.
5. Make command-row navigation an explicit pipeline transition. `move_up`/`move_down` (or their `SelectionChanged` handling inside `CommandPalette`) must invalidate argument acceptance and immediately start completion for the newly selected **committed** command using the stored latest input/cursor/mode context, even when input text and command pattern are unchanged. Do not rely on a later cadence tick or no-op `sync_input` call. Retained argument rows may remain visible but inert until the newly selected command's response commits.
6. Retain the previously committed command-name popup through stale snapshots and registry rebuilds. `ResolvedCommand` rows are owned, so old registry results remain safe to render; they are not safe to accept or to seed argument requests until the new generation commits.
7. Gate Enter/Tab execution and completion through command-name `can_accept`. Pending stale command rows may still be navigated, but cannot execute or rewrite input. Preserve the existing consumed behavior for unsafe acceptance.
8. Preserve command registry projection, aliases, argument-count eligibility, ranking, descriptions, selection clamping, and confirmation ownership across registry refresh.
9. Add component tests proving `sync_input` returns without draining, pending cadence is requested, stale-pattern frames are buffer-identical, only the latest rapid query commits, the commit produces one owed frame, pending rows cannot execute/complete or start argument work, registry refresh retains but disables old rows until a current snapshot commits, same-command-word argument-count changes commit synchronously, a command-name commit resynchronizes argument completion for the latest input, and command-row navigation immediately requests arguments for the newly selected committed command.

### Phase 4: Fix and centralize command-argument publication

1. Remove the independently called `sync_arguments` entry point from normal app input flow and make argument synchronization the second stage of `CommandPalette::sync_input`/`tick`. Replace eager `reset_argument_state` when that stage begins with a shared publication `begin`:
   - retain the committed `ArgumentSnapshot { items, range }` for rendering;
   - reset/cancel producer lifecycle state as required, but keep an inert visual copy;
   - replace only the pending receiver and complete argument request key;
   - do not start while command names are pending or from retained stale command rows;
   - do not reset visible selection/scroll until a current response commits.
2. Keep command completion-session lifecycle independent from visual retention. Starting a replacement may cancel/revert the prior `/theme` preview, but retained rows become display-only. Highlight, accept, and cancel notifications must never target a stale retained candidate.
3. Refactor `poll_arguments` to build a local filtered/sorted `ArgumentSnapshot`, then:
   - discard stale responses without cancelling a newer current session;
   - atomically commit a non-empty current result and reset selection/scroll;
   - treat an empty/unmatched/error/disconnected current result as an authoritative settled clear/fallback, removing retained rows once rather than during request startup;
   - return `Dirty::YES` only when visible committed state changes.
4. Gate argument Enter/Tab through `can_accept`, not the incidental presence of `argument_range`. Preserve exact-match Enter behavior, accepted-input behavior, replacement ranges, command fallback, and command execution semantics.
5. Fold command-name and argument polling into one `CommandPalette::tick()` and one `CommandPalette::cadence()`. Update `App::tick` to call that unified poller and `App::cadence` to include it, replacing the argument-only poll. No producer polling may move into `view`.
6. Replace `argument_completion_clears_old_rows_while_request_pending` with the intended invariant. Use `probed_event_handle` and a `/model`-equivalent async completion to assert the complete old popup remains visible through the pending interval, is non-acceptable, and is replaced in one committed frame. Add rapid-response tests covering stale non-empty and stale empty responses, current empty fallback, lifecycle safety, selection/scroll reset timing, and pending cadence.
7. Add an app-level full-frame scenario matching the recording: render settled model candidates, type another character, render before the response, assert the popup region and full frame remain stable except for the input/cursor cells, deliver the response, and assert one atomic replacement. Ensure the animated splash never becomes visible through the popup region.

### Phase 5: Route synchronous completion/search surfaces through immediate publication

1. Refactor `ListPicker::rebuild_filter` to build filtered indices and highlight indices locally and install them with `commit_sync`. Query edits remain synchronous and reset selection/scroll in the same update; complete external source replacements from model, MCP, task, theme, rewind, login, and Lua picker consumers retain current behavior.
2. Refactor `SearchModal::update_matches` to build a complete vector locally and `commit_sync` it. Keep search navigation and selection semantics local; use the common protocol only for atomic result publication, not autocomplete-specific insertion.
3. Route synchronously completed command-name setup/empty-input closure and explicit file completion through the same immediate commit/clear API. Remove direct component-local publication mutations that bypass the protocol.
4. Audit all completion-producing Lua paths. Keep Lua APIs returning complete candidate vectors; Rust remains the sole owner of filtering, freshness, publication, rendering, and acceptance. Do not add generation or rendering policy to Lua.
5. Add source-contract tests or focused code assertions only where they prevent policy duplication without becoming brittle: all asynchronous completion surfaces must hold the shared publication type, and retired eager-clear/direct-`status.changed` publication patterns must be absent. Prefer behavioral tests over broad source-text tests.

### Phase 6: Cleanup, validation, and review

1. Remove the old `CoherentCompletion`, duplicate `argument_generation` readiness logic, synchronous command Nucleo drain, eager pending clears, and direct acceptance checks that infer readiness from row/range presence.
2. Update local comments to describe the universal committed/pending protocol and producer-specific freshness. Avoid introducing a broad completion widget or coupling ranking/layout implementations.
3. Run formatting, compilation, focused tests, lint, full `maki-ui` tests, then repository-wide checks required by policy.
4. Run implementation review focused on generation/key transitions, command lifecycle correctness, Nucleo discovery semantics, stale acceptance, empty/error paths, event-loop cadence, and accidental ranking/layout changes. Fix or explicitly rebut all findings and repeat for critical issues.

## Acceptance Criteria

- **AC.1:** A single private settled-publication implementation owns committed-versus-pending lifecycle, generation supersession, acceptance readiness, atomic commit/clear, synchronous commit, same-query streaming, and pending cadence for all migrated surfaces; no parallel component-local publication policy remains.
- **AC.2:** During a changed `@` query with indexed files, every waiting frame retains the complete prior mixed file/reference snapshot and geometry, stale candidates cannot be accepted, and the first current-pattern snapshot atomically replaces it even while discovery continues.
- **AC.3:** The standalone file picker retains rows, count metadata, dimensions, selection, and scroll while pending, blocks stale Enter, atomically commits current-pattern results, and streams later discovery.
- **AC.4:** Slash-command name matching no longer drains Nucleo synchronously; it retains committed rows while asynchronously matching, blocks stale execution/completion and stale argument requests, publishes only the latest command/registry context, and wakes the event loop until settled.
- **AC.5:** Command-name and command-argument stages are ordered: argument requests use only a committed current command identity, command-name commits and command-row navigation resynchronize the latest input context, and argument-count changes with an unchanged command word settle without waiting for a nonexistent Nucleo change.
- **AC.6:** `/model` and all command-argument completion retain their committed popup through a replacement request, never expose the splash/underlay between result sets, reject stale acceptance/responses, and atomically commit or authoritatively clear only the latest response.
- **AC.7:** Command completion lifecycle notifications remain correct: retained stale rows cannot receive highlight/accept callbacks, a new query safely cancels/reverts prior preview state, and empty/error/cancel outcomes do not cancel a newer session.
- **AC.8:** `ListPicker` filtering publishes synchronously through the common immediate path while preserving progressive query filtering, score/source-order ranking, original-item selection, and external model-list refresh selection behavior.
- **AC.9:** Chat search publishes synchronously through the common immediate path while preserving score ordering, navigation, selection, and closure behavior.
- **AC.10:** Command matching preserves alias invocation/completion, ranking, descriptions, argument-count eligibility, and argument insertion/replacement ranges.
- **AC.11:** File completion preserves explicit-path behavior, file/reference source precedence, materialization/truncation limits, popup size limits, visibility debounce, and uppercase-query safety.
- **AC.12:** Pending asynchronous completion work contributes `Cadence::PENDING` when no spinner already wakes the loop; each commit/stream/clear owes exactly one frame, and settled or cancelled completion work returns to idle cadence.
- **AC.13:** Formatting, compilation, Clippy, all focused `maki-ui` tests, the full `maki-ui` suite, and required workspace checks pass without new failures.

## Test Strategy

| Acceptance criterion | Named validation |
|---|---|
| AC.1 | Publication unit tests `begin_retains_committed_value`, `latest_generation_wins`, `commit_replaces_atomically`, `stream_requires_committed_key`, `newer_pending_blocks_stream`, `commit_sync_is_ready`, `clear_is_authoritative`, and `cancel_rejects_late_result`; focused structural audit `all_completion_surfaces_use_shared_publication`. |
| AC.2 | Existing/refactored `file_completion` tests `query_refresh_keeps_mixed_snapshot_and_geometry_until_current_pattern`, `reference_only_query_publishes_immediately`, `file_completion_query_commit_survives_in_flight_injection`, `pending_query_rejects_stale_candidate_and_commits_latest_query`, plus new `pending_file_completion_frame_matches_committed_buffer`. |
| AC.3 | Existing/refactored `file_picker` tests `pending_query_consumes_enter_and_preserves_committed_state`, `file_picker_query_commit_survives_in_flight_injection`, `no_op_edits_preserve_ready_selection`, plus new `pending_file_picker_frame_matches_committed_buffer`. |
| AC.4 | `command` tests `sync_returns_without_draining_command_matcher`, `command_name_keeps_committed_frame_until_current_pattern`, `rapid_command_queries_commit_latest`, `pending_command_name_cannot_execute_or_complete`, `pending_command_name_does_not_start_arguments_for_retained_command`, `registry_refresh_retains_rows_until_commit`, and `command_name_commit_owes_one_frame`. |
| AC.5 | Command/app tests `command_change_waits_before_requesting_old_command_arguments`, `command_name_commit_resynchronizes_latest_argument_context`, `command_selection_change_resynchronizes_arguments_for_selected_command`, `argument_request_key_includes_committed_command_identity`, and `same_command_word_argument_count_commits_without_matcher_change`. |
| AC.6 | Replace app regression with `argument_completion_retains_committed_popup_while_request_pending`; add `model_argument_popup_never_exposes_underlay`, `pending_argument_cannot_accept_stale_row`, `rapid_argument_requests_commit_latest_non_empty_response`, `stale_empty_response_cannot_clear_current_popup`, and `current_empty_response_clears_atomically`. Use `probed_event_handle`, bounded deadlines, and full `TestBackend` buffers. |
| AC.7 | Command/app tests `retained_argument_does_not_highlight_stale_candidate`, `replacement_query_cancels_prior_preview_once`, `stale_response_does_not_cancel_current_session`, `ctrl_c_closes_palette_and_cancels_lifecycle`, `esc_closes_palette_and_cancels_lifecycle`, and `unmatched_completion_items_cancel_the_argument_session`; retain lower-layer supersession/timeout tests. |
| AC.8 | Add `query_filter_commits_synchronously`; retain `search_filters_progressively`, `search_change_resets_selection_to_top_result`, `fuzzy_search_uses_shared_matcher`, `filter_ranks_by_score_not_source_order`, `filter_ties_keep_source_order`, `enter_under_active_filter_selects_original_index`, `refresh_updates_items_and_preserves_search`, and `refresh_preserves_selection_with_active_search`. |
| AC.9 | Add `match_update_commits_synchronously`; retain `matching_finds_correct_segments`, `matches_sorted_by_score_descending`, `navigation_wraps_around`, `enter_selects_current_match`, and `enter_on_no_matches_closes`. |
| AC.10 | Retain `shared_ranking_orders_matches_over_registration_order`, `palette_projects_only_registry_snapshot`, `argument_parser_handles_multibyte_whitespace`, `argument_completion_tab_clears_the_popup`, and argument replacement app tests; add `aliases_match_and_complete_to_invoked_name`, `argument_count_filters_ineligible_commands`, and `argument_replacement_preserves_multibyte_range`. |
| AC.11 | Retain `explicit_completion_commits_without_pending_refresh`, `refresh_tracks_materialization_boundary`, `uppercase_file_query_does_not_panic`, `uppercase_ref_query_does_not_panic`, `host_and_guest_candidates_share_heuristic_order`, `file_refresh_uses_lexical_source_order_for_ties`, and both `pending_debounce_controls_visibility` tests; add `file_completion_popup_respects_height_limit` and `file_picker_popup_respects_height_limit`. |
| AC.12 | Publication unit `pending_and_settled_cadence`; command component `pending_command_work_requests_cadence`; app scenario `completion_pending_cadence_settles_after_single_commit_frame`; retain `a_matcher_mid_answer_keeps_the_loop_coming_back`, `settled_picker_owes_no_frame_and_does_not_animate`, and file-completion cadence regression `publication_state_preserves_pending_and_spinner_cadence`. |
| AC.13 | `cargo fmt --all -- --check`; `cargo check -p maki-ui --tests`; focused `cargo nextest run -p maki-ui` filters for publication/file/command/list/search/app regressions; `cargo clippy -p maki-ui --all-targets -- -D warnings`; `cargo nextest run -p maki-ui`; then `just check`, `just lint`, and `just test` if touched dependencies or repository policy require workspace validation. |

For asynchronous tests, use wall-clock deadlines with `yield_now`, channels/barriers for controlled producers, and no fixed sleeps or tick budgets. Full-frame tests must disable/freeze splash animation, compare cell symbols and styles, mask only deliberately changed input/cursor cells, and verify the popup region never contains splash/underlay cells during pending work. Never mask dynamic cells inside the popup region. At least one real Nucleo controlled-injection test remains for each incremental file surface, and command-name tests use actual Nucleo status/snapshot transitions.

### Nextest stabilization

The command-name matcher now settles through event-loop polling. Tests must not type a slash command and immediately assert command rows, press Enter, select a row, or request argument completion. Shared helpers such as `type_slash`, `type_and_submit`, and `open_tasks_picker` must poll `App::tick` until `CommandPalette::cadence` no longer reports `Cadence::PENDING`. The polling loop must use a wall-clock deadline and `yield_now`.

Tests that call `CommandPalette::sync` directly must apply the same settlement rule unless the test explicitly verifies pending behavior. Argument completion tests must settle command publication before they call `sync_arguments`, because pending retained command rows cannot start argument work.

Incremental completion tests must wait for their complete semantic result rather than the first selectable row. The mixed `@` source test waits until skill, subagent, and model candidates are all present. This requirement detects partial publication and prevents a project file from ending the wait before Lua candidates become visible.

Tests must assert settled output rather than transient pending state when the producer can answer during the initial nonblocking poll. The registry refresh regression verifies the replacement registry projection and confirmation result after settlement. It does not require `is_pending()` to remain observable.

Run the full crate with Nextest after focused tests:

```bash
cargo nextest run -p maki-ui
```

If the full run fails, rerun every failure with an exact Nextest expression. Repeat timing-sensitive isolated tests enough times to distinguish deterministic stale assumptions from scheduling-dependent failures. Do not classify a failure as a known flake without the isolated result and root cause. The implementation completed with `1513 tests run: 1513 passed, 0 skipped`.

## Review Strategy

Before handoff, run a `plan_reviewer` against this plan and resolve or explicitly rebut every finding; rerun after any critical/high correction.

After implementation and all automatable validation, dispatch a `general` review subagent focused on publication generation/key correctness, complete-payload atomicity, Nucleo `changed`/`running` semantics, command preview lifecycle, stale Enter/Tab behavior, empty/error/cancel transitions, event-loop polling/cadence, and regressions in ranking or geometry. Fix or explicitly rebut every finding, repeating review if critical issues remain.

## Documentation Strategy

No user-facing documentation is needed because completion syntax, controls, configuration, and intended results do not change. Update concise local comments around the shared protocol, Nucleo semantic-pattern adapter, and command lifecycle separation. Do not add a new architecture document unless implementation reveals a reusable contract that existing project guidance must record.

## Risks, Blockers, and Required Decisions

- The generic protocol must own complete visible payloads to make partial publication structurally difficult, but it must not become a giant widget abstraction. Rendering, ranking, producer handles, selection mechanics, and lifecycle callbacks stay outside.
- Command-name matching currently blocks in a synchronous drain loop. Moving it to event-loop ticks changes timing, so pending acceptance and cadence must land in the same phase to avoid introducing a new stale-execution or frozen-popup window.
- Command arguments have two generation systems: `CompletionSession` request IDs and the UI publication generation. Keep both. The lower layer protects producer/session correctness; the UI generation protects visible publication and future detached/stale responses.
- `/theme` completion has preview side effects. Retaining its rows visually must not retain authority to highlight or accept them; tests must prove callbacks apply only to the current committed generation.
- A current empty response is a settled result and should trigger the existing fallback/closure atomically. An old empty response must never clear a newer popup.
- Nucleo 0.5 has no query generation in `Status`, `running` includes later injection, `MultiPattern` is not equality-comparable, and injected/snapshot counts are not coherent frontiers. Continue comparing public one-column atom vectors and never use count equality or `running == false` as the incremental file coherence proof.
- Full-frame comparisons must freeze splash animation and may mask only input/cursor cells outside the popup region. Never mask splash or other underlay cells inside the popup region; that region is the regression oracle. Do not weaken tests to rectangle-only assertions, which missed the recorded disappearance.
- Synchronous `ListPicker` and search migration is for one publication boundary, not because they currently flicker. If the generic API makes those paths more complex rather than simpler, keep only `commit_sync` integration and do not add pending producer state.
- A full-suite failure is not treated as a known flake without an exact isolated rerun and a root-cause investigation. Async command tests must settle command publication before dependent actions. Incremental source tests must wait for their complete expected candidate set.
