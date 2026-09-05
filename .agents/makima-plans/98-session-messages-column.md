## Goal

Add a message-count column to the `/sessions` picker, showing the number of messages in each session while preserving the existing current/open/age status column and picker behavior.

## Implementation Summary

Extend the stored-session summary contract with a `message_count` field and populate it from persisted session data. The count is the number of main transcript `msg` records, excluding `sub_msg` records belonging to subagents. Thread the field through the Lua session API and live-session response, merge it into picker rows, and render it as a compact right-aligned column beside the existing status/age label.

The main touchpoints are `maki-storage/src/sessions.rs` for summary scanning and scan-cache persistence, `maki-ui/src/event_loop.rs` for live rows, `maki-lua/src/api/session.rs` for the public return-shape documentation, `plugins/sessions/sessions_helpers.lua` and `plugins/sessions/init.lua` for pure formatting and UI layout, and affected Rust/ Lua tests. No change is needed to message history storage or to picker ordering, filtering, selection, locking, or lifecycle behavior.

## Implementation Plan

1. **Define and expose the summary field.**
   - Add a serialized `message_count` field to `maki_storage::sessions::SessionSummary`, using the existing integer style for counts.
   - Update all Rust `SessionSummary` fixtures and serialized-field assertions, including `src/cmd/subcmd.rs`.
   - Update `maki-lua/src/api/session.rs` documentation for `list()` and `list_all()` to include `message_count` and state that it counts main transcript messages only, excluding subagent histories.

2. **Compute counts during storage scanning without changing scan-cache semantics.**
   - Extend `ScannedHeader` with `message_count`; because `ScanCacheEntry` already caches the complete scanned header behind `(size, mtime_ms)`, unchanged files must reuse the cached count. Change `load_scan_cache` to return `(cache, cache_needs_rewrite)`: an old/unversioned cache parses as empty with `cache_needs_rewrite = true`, and `scan_headers` initializes `dirty` from that flag so the new version is written even when there are no session entries.
   - For JSONL files, retain the existing header/version and final-meta behavior, and scan records on a cache miss or changed file with a lightweight tagged record representation. Count a line only when it is valid JSON with `t: "msg"` and a `d` field of any JSON value, including `null`; malformed JSONL, missing `t`, missing `d`, `sub_msg`, `out`, `header`, `meta`, and unknown records do not increment the count. Keep the count independent of the final metadata lookup.
   - For legacy JSON files, parse a scan-only representation of the existing legacy object: a valid `messages` JSON array contributes its array length, a missing `messages` field contributes `0`, and a non-array `messages` value or malformed whole document makes the legacy header unlisted, matching the existing `None` scan result. Individual array elements are counted as persisted main-message entries without attempting to deserialize their provider-specific shape. No subagent collection is counted.
   - Populate `SessionSummary.message_count` from the scanned header for both `list_in` and `list_all`. Use explicit scan-cache versioning: wrap the cache in a versioned structure (or otherwise reject the old unversioned shape), mark an old cache as dirty, rescan all files, and persist the new cache format. Never default a missing count to zero for an otherwise cache-valid session.
   - Keep the existing file signature invalidation and scan-cache pruning behavior. Do not alter sorting or lock-file recomputation.

3. **Thread counts through live-session responses.**
   - Add the focused runtime session’s main `state.session.messages().len()` as `message_count` in `maki-ui/src/event_loop.rs` `SessionRequest::Live` JSON. Update the `live()` return-shape documentation in `maki-lua/src/api/session.rs` to list this field and its main-transcript-only semantics.
   - Keep live rows authoritative when `Helpers.merge` combines live and stored rows, while ensuring stored-only rows retain their scanned count. The live count must exclude subagent chat/history data.
   - Add a private `live_session_row(i, &SessionRuntime) -> serde_json::Value` helper and make `handle_session_request(SessionRequest::Live, ...)` map every runtime through it. Add the named event-loop test `live_session_response_includes_main_message_count` at `maki-ui/src/event_loop.rs` tests, exercising that helper with a runtime containing main and subagent messages and asserting the exact response object. This fixes the test seam rather than leaving handler-vs-helper selection to execution.

4. **Render the column in the sessions plugin.**
   - Add pure helper behavior in `plugins/sessions/sessions_helpers.lua` for formatting the count and producing the count/status display data. Format counts as bare decimal values with no `messages` suffix. Preserve `current`, `open`, and age text as the final rightmost status. Count and status use `dim` when unselected and `selected` when selected.
   - Render a pinned header row with the labels `title`, `messages`, and `age`. Remove the empty spacer between the header and the first session row. Use the theme-defined `path` style for the header.
   - Extract `row_right_columns(title_width, icon_width, confirm_width, inner_width, count, status)` (or an equivalent pure helper) that returns the exact padding and ordered count/status segments. Make `init.lua` call this helper when constructing every row. Use fixed display widths for the count and status columns. Right-align both values within their columns, with one space between the columns. Include the row's two-cell leading prefix in the title-side width calculation. If content exceeds the available width, retain the minimum one-space separator and allow the existing row content to clip rather than overlap or panic.
   - Add a production-wired render representation helper, `row_spans`, and use it from `init.lua`; it must preserve the existing title spans and append the padded count column then the padded status column. The plugin spec test `picker_row_contains_count_then_status` asserts the exact segment order/text/styles, while `picker_row_right_columns_remain_separated_at_narrow_width` asserts the padding invariant. Add coverage for header styling, header placement, and fixed-column alignment. The title, confirmation text, icon, selection styles, spinner behavior, filtering, and existing status labels must remain intact. The plugin spec harness loads `plugins/sessions/tests/spec.lua` through `maki-lua/tests/spec.rs::plugin_spec`, so this is the concrete runner for these pure production-wired helpers.
   - Keep merge behavior explicit: live rows carry the live count, stored-only rows carry the stored count, and duplicate live/stored rows appear once.
   - Do not make message count affect recency sorting, fuzzy ranking, selection reconciliation, or refresh cadence.

5. **Add focused regressions and documentation consistency checks.**
   - In `maki-storage` tests, cover JSONL counts for zero and multiple main messages, mixed `msg`/`sub_msg`/`out`/`meta` records, count changes after a file invalidates the scan cache, reuse of an unchanged cached count, the exact legacy cases defined above, and versioned-cache invalidation/rewrite.
   - In `maki-ui` tests, add `live_session_response_includes_main_message_count` in the existing event-loop test module, exercising the specified `live_session_row` helper with a main history plus subagent history and asserting the exact response object.
   - In `plugins/sessions/tests/spec.lua`, cover `message_count_formatting` for bare zero/one/plural values, `selected_count_and_status_styles`, `merge_preserves_message_count`, and `live_rows_override_stored_message_count`. Add the production-wired render regressions `picker_row_contains_count_then_status`, `picker_row_right_columns_remain_separated_at_narrow_width`, and header styling/alignment/placement cases. Assert the fixed-width right alignment, the one-space separator, the `title`/`messages`/`age` labels, the theme-defined header style, and the absence of an empty row between the header and sessions. The tests must fail if `init.lua` stops using the helpers.
   - Add `maki-lua` API-boundary coverage, `session_list_results_include_message_count`, through the existing Lua test host, invoking both `maki.session.list()` and `maki.session.list_all()` against a deterministic storage fixture and asserting numeric `message_count` plus the existing fields. Add `generated_session_docs_contain_message_count_contract` in `maki-docgen/src` alongside the existing generation tests, asserting the generated `list`, `list_all`, and `live` return-shape text and main-transcript-only wording. Update generated Lua API documentation through the existing generator if output changes. Do not hand-edit generated documentation.

6. **Validate and review.**
   - Run focused checks/tests for `maki-storage`, `maki-ui`, and `maki-lua`, plus the repository's existing Lua plugin-spec runner used for `plugins/sessions/tests/spec.lua` (identify and invoke the same runner used by other plugin specs, without adding a new harness).
   - Run `just fmt-check`, `just gen-docs-check` when generated docs change, `just lint`, and the relevant workspace tests; run the full repository checks as practical.
   - Review the implementation against the explicit contract that counts are persisted main transcript records, not rendered bubbles, turns, tokens, or subagent messages. Resolve all implementation-review findings before completion.

## Acceptance Criteria

- **AC.1** Every stored session returned by `maki.session.list()` and `maki.session.list_all()` includes a numeric `message_count` field, and existing summary fields remain present. Verified at the Lua boundary by `maki-lua::session_list_results_include_message_count`, and at the serialized CLI/storage boundary by `src/cmd/subcmd.rs::sessions_json_pins_the_serialized_fields` plus the storage list tests.
- **AC.2** `message_count` equals the number of main `msg` records, including zero, and excludes `sub_msg`, tool-output, header, metadata, unknown, and malformed records. Verified by `jsonl_scan_counts_main_messages_only` and mixed-record storage tests.
- **AC.3** Changed session files produce refreshed counts while unchanged files reuse the scan-cache result, and old/missing cache fields do not produce an incorrect count. Verified by `scan_cache_refreshes_message_count`, `unchanged_scan_reuses_cached_message_count`, and `old_scan_cache_defaults_or_invalidates_message_count`.
- **AC.4** Legacy session files have the documented deterministic count behavior and do not count subagent data. Verified by `legacy_scan_message_count` and malformed/unsupported legacy scan cases.
- **AC.5** Live session rows expose the main transcript count and picker merging preserves the correct count for live-over-stored duplicates and stored-only sessions. Verified by `maki-ui::event_loop::tests::live_session_response_includes_main_message_count` and the `merge_preserves_message_count`/`live_rows_override_stored_message_count` Lua spec cases.
- **AC.6** The sessions picker row representation emits the exact `0 messages`/`1 message`/`N messages` count before the existing current/open/age status as separately styled right-aligned segments, and its layout calculation always leaves at least one separator without underflow or overlap. Verified by `message_count_formatting`, `selected_count_and_status_styles`, the production-wired `picker_row_contains_count_then_status`, and `picker_row_right_columns_remain_separated_at_narrow_width`; manual TUI validation must additionally confirm the rendered picker matches the row representation.
- **AC.7** Existing picker policies remain unchanged: fuzzy ordering/filtering and selection retention (`sessions_filter_orders_exact_prefix_and_fuzzy`, `sessions_filter_preserves_original_order_for_ties`, `sessions_filter_keeps_selected_id`, `sessions_filter_clamps_missing_selection`), merge behavior (`merge_live_wins_over_stored_duplicate`, `merge_stored_only_rows_become_idle`), status labels/age (`right_shows_open_label_for_open_sessions`, `right_shows_current_for_focused`, `right_shows_age_for_idle_rows`), and open eligibility (`can_open_blocks_open_sessions`) all pass; count rendering must not alter these outcomes.
- **AC.8** Public generated API documentation and repository validation are consistent. Verified by `maki-docgen` test `generated_session_docs_contain_message_count_contract`, which asserts the generated `list`, `list_all`, and `live` contracts and main-transcript-only wording, plus `just gen-docs-check`, `just fmt-check`, focused crate checks/tests, `just lint`, and relevant workspace tests.

## Test Strategy

| Acceptance criterion | Named test/check |
|---|---|
| AC.1 | `maki-lua::session_list_results_include_message_count`; `src/cmd/subcmd.rs::sessions_json_pins_the_serialized_fields`; `maki-storage` list tests `list_in_serializes_message_count` and `list_all_serializes_message_count` |
| AC.2 | `maki-storage::sessions::tests::{jsonl_scan_counts_main_messages_only, jsonl_scan_excludes_subagent_and_non_message_records}` |
| AC.3 | `scan_cache_refreshes_message_count`; `unchanged_scan_reuses_cached_message_count`; `old_scan_cache_is_invalidated_and_rewritten` |
| AC.4 | `legacy_scan_message_count`; `legacy_scan_missing_or_malformed_messages_is_unlisted` |
| AC.5 | `maki-ui::event_loop::tests::live_session_response_includes_main_message_count`; `plugins/sessions/tests/spec.lua::{merge_preserves_message_count, live_rows_override_stored_message_count}` |
| AC.6 | `plugins/sessions/tests/spec.lua::{message_count_formatting, selected_count_and_status_styles, picker_row_contains_count_then_status, picker_row_right_columns_remain_separated_at_narrow_width}`; manual TUI check of `/sessions` at normal and narrow supported terminal widths |
| AC.7 | Explicit existing regressions: `sessions_filter_orders_exact_prefix_and_fuzzy`, `sessions_filter_preserves_original_order_for_ties`, `sessions_filter_keeps_selected_id`, `sessions_filter_clamps_missing_selection`, `merge_live_wins_over_stored_duplicate`, `merge_stored_only_rows_become_idle`, `right_shows_open_label_for_open_sessions`, `right_shows_current_for_focused`, `right_shows_age_for_idle_rows`, and `can_open_blocks_open_sessions` |
| AC.8 | `maki-docgen/src` test `generated_session_docs_contain_message_count_contract`; `just gen-docs-check`; `just fmt-check`; `cargo check -p maki-storage --tests`; `cargo check -p maki-ui --tests`; `cargo check -p maki-lua --tests`; `just lint`; relevant `cargo nextest` targets |

The existing plugin spec harness is sufficient for the pure row representation and merge behavior once `row_spans` is extracted and production-wired. It cannot prove terminal painting, so manual TUI validation is an explicit supplemental check for the observable visual result, not a substitute for the executable row-representation tests.

## Review Strategy

Before handoff, run the `plan_reviewer` subagent against this plan and the issue requirement. Fix every critical or high finding in the plan and rerun review until none remain.

After implementation and automated checks, review the final diff against AC.1–AC.8, with special attention to JSONL scan cost, old scan-cache compatibility, legacy behavior, and right-column width arithmetic. Fix or explicitly rebut all findings; repeat review for any critical finding.

## Documentation Strategy

Update Rust API doc comments in `maki-lua/src/api/session.rs` to describe `message_count` and its main-transcript-only semantics. Regenerate checked-in API documentation with the existing `maki-docgen` workflow if output changes. No new standalone user guide is needed because this is a small additional column in an existing picker.

## Risks, Blockers, and Required Decisions

- Counting changed JSONL files requires a full record scan, unlike the existing header-plus-tail scan. The existing `(size, mtime_ms)` cache keeps repeated picker opens cheap; do not add a per-row full session load.
- The count is raw persisted main transcript records, not user/assistant turns or rendered message bubbles. This distinction must remain in API documentation and tests.
- Live in-memory sessions can be ahead of disk, so live rows should use their in-memory main-session length while stored rows use the scan result. This is intentional and limited to the picker display.
- Legacy files need an explicit fallback because their scan path historically reads only header fields. The executing agent must pin the behavior in a test and document it in the implementation; do not silently expose an unverified count.
- Adding a required serialized field changes public Lua and CLI JSON shapes, so all exact-field fixtures must be updated together.
