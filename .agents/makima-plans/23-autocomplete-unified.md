# Shared heuristic ranking for all autocomplete surfaces

## Goal

Use one shared matching and ranking implementation for `@` completion and command argument completion, including Lua-provided arguments. Preserve each surface's query parsing, insertion behavior, source lifecycle, stale-result handling, and UI ordering contracts while making textual ranking consistent.

## Implementation Summary

Move the generic Nucleo-backed match classification, highlight-index generation, ranking metadata, and comparator from `maki-ui/src/components/file_completion.rs` into the existing `maki-match` crate. Keep the API data-oriented so callers can rank their own item types and apply their own final label tie-break.

Refactor `@` completion to consume the shared API while retaining kind-prefix parsing, file/reference source ranks, asynchronous walking, deterministic file order, and combined-menu behavior. Refactor command argument completion to rank every `CommandArgumentItem` by `label`, including Lua results, while continuing to use `insertion` only for replacement and preserving provider lifecycle and generation behavior. Do not change Lua APIs, command parsing, argument cardinality, command-name ordering, or `@` syntax.

Affected files are `maki-match/src/lib.rs`, `maki-ui/src/components/file_completion.rs`, and `maki-ui/src/components/command.rs`, with tests in those modules and existing Lua integration tests where needed. No new dependency is expected because `maki-ui` already depends on `maki-match`.

## Implementation Plan

### Phase 1: Define the shared contract

1. Extend `maki-match/src/lib.rs` without changing legacy APIs. Add `CompletionMatch { indices: Vec<u32>, ranking: CompletionRanking }`, `CompletionRanking { quality_rank: u8, boundary_rank: u8, start_index: usize, gap_count: usize, span_length: usize, unmatched_suffix: usize, fuzzy_score: u32 }`, `CompletionMatchOptions { case_matching: CaseMatching, normalization: Normalization }`, `completion_match(query: &str, label: &str, options: CompletionMatchOptions) -> Option<CompletionMatch>`, and the comparator with explicit source-rank/source-order arguments.
2. Implement matching with Nucleo `Pattern::parse(query, case_matching, normalization)`. The `nucleo-matcher` 0.3.1 grammar (used through the workspace's `nucleo` 0.5.0 facade where applicable) is whitespace-separated atoms with escaped whitespace, plus `$`, `!`, `'`, and `^` prefixes at atom boundaries; it does not provide quoted-string syntax, so do not add or promise quoted-term behavior. All positive atoms must match; negative atoms may reject a candidate but contribute no highlights or positive metrics. For multiple positive atoms, collect each atom's indices, union/deduplicate/sort them, derive start/boundary/gaps/span/suffix from that union, and sum positive scores. A single atom has quality `0` exact, `1` prefix-contiguous, `2` non-prefix contiguous substring, or `3` gapped fuzzy; multi-atom matches use quality `3`. Empty queries return a neutral match with no highlights.
3. Define the index contract precisely: the new API returns ascending, deduplicated, zero-based Unicode codepoint offsets into the original `label`; existing `fuzzy_match` remains source-compatible and 1-based. Build Nucleo haystacks from the original string's `chars()` using the same `Utf32Str::Ascii`/`Utf32Str::Unicode` codepoint path already used by `maki-match`, never from a case-folded or normalized string. Treat Nucleo's returned indices as codepoint positions and derive all ranking metrics from those positions. Add regression tests for Unicode case-expansion inputs to ensure indices remain aligned.
4. Define comparator direction and tie-break scope: quality, boundary, start, gaps, span, and suffix ascending; fuzzy score descending; source rank ascending; source order ascending; caller label comparison last. Therefore command provider order precedes the label tie-break, while `@` source policy remains file rank `0`, reference rank `1`, then source order.
5. Add unit tests for each quality tier, every comparator direction, source tie-breaks, case modes, smart normalization, empty queries, repeated occurrences, CJK/emoji indices, Unicode case-expansion safety, escaped-whitespace terms, `$`/`!`/`'`/`^` atom prefixes, negative terms, and multi-term union/aggregation. Include concrete competing candidates for each ordering distinction and retain all legacy API tests.

### Phase 2: Migrate `@` completion

1. Replace the local matcher, ranking metadata, and comparator in `maki-ui/src/components/file_completion.rs` with `maki-match` calls. Keep `QueryIntent` and alias recognition local because kind-prefixed syntax is surface-specific.
2. Match the payload against the label with the kind prefix removed, then translate returned payload codepoint indices to full-label offsets for rendering. Preserve smart case and smart normalization.
3. Preserve explicit kind filtering, empty ordinary/kind queries, files-first behavior for a bare `@`, file source rank `0`, reference source rank `1`, reference collection order, and lexical ordering of materialized file paths before source order is assigned.
4. Preserve the asynchronous walker, `MAX_MATERIALIZED`, refresh behavior, selection clamping, insertion ranges, popup navigation, rendering, aliases, host/plugin candidates, and Unicode highlight behavior. Remove local ranking only after the existing tests pass against the shared implementation.

### Phase 3: Rank command arguments

1. Update `CommandPalette::poll_arguments` in `maki-ui/src/components/command.rs` to match and rank each returned `CommandArgumentItem` by `label`. Use `insertion` only for replacement and acceptance.
2. Match the current argument token from `PendingArguments::query`, not the complete `ctx.args` string. Preserve the current command-argument options: case-insensitive matching and smart normalization. Command argument completion remains single-token: `argument_at_cursor` extracts one whitespace-delimited token, so escaped whitespace and multi-term pattern semantics apply only to direct shared-matcher tests and `@` matching, not to command argument queries. Continue passing the unchanged `CommandArgumentContext` to providers, including full args, current arg, argument index, mode, session, and generation.
3. Sort with shared textual ranking, then the command surface's stable provider order. Do not apply `@` source ranks or alter command-name ordering.
4. Keep `pending_arguments`, cancellation, generation checks, stale-result retention, and `CommandArgumentLifecycle::{Highlight, Accept, Cancel}` ordering unchanged. Ranking must be a local transformation and must not change callback payloads or timing/state transitions.
5. Ensure model, theme, and Lua sources use the same ranking path. For Lua items whose label differs from insertion, match/display/highlight the label and insert the insertion value.
6. Add tests for exact/prefix/substring/fuzzy ordering, case-insensitive matching, equal-rank provider order, model/theme sources, Lua ranking, label/insertion distinction, lifecycle events, stale-result retention, and generation mismatch.

### Phase 4: Validate integration and regressions

1. Retain or update file-completion tests for quality ordering, closer spans, file/reference ties, aliases, explicit kind offsets, empty kind queries, Unicode, async refresh, selection clamping, acceptance, and host/plugin candidates.
2. Add integration assertions that equivalent labels receive equivalent textual ordering through both surfaces when configured with the same options, while allowing their documented source policies and query parsers to differ.
3. Run scoped checks first, then the repository checks specified by the `justfile`: `just check`, `just lint`, `just test`, `just fmt-check`, `just gen-docs-check`, and `just machete` as applicable. Record exact unrelated failures rather than bypassing checks.

## Acceptance Criteria

- **AC.1** The shared crate exposes one completion matcher and comparator while legacy `maki-match` APIs retain their existing behavior. Verification: `completion_match_returns_zero_based_indices`, `fuzzy_match_remains_one_based`, `completion_match_requires_all_positive_atoms`, `completion_match_excludes_negative_atom_indices`, `completion_match_unions_multi_atom_indices`, and `completion_ranking_comparator_orders_all_fields` in `maki-match`, plus `at_completion_uses_shared_matcher` and `command_arguments_use_shared_matcher` in `maki-ui`.
- **AC.2** `@` completion preserves quality ordering, metric tie-breaks, file/reference source policy, kind filtering, full-label highlight offsets, refresh, selection, insertion, aliases, and host/plugin behavior. Verification: the existing named file-completion tests for these behaviors pass, including `completion_quality_tiers_are_ordered`, `closer_match_beats_longer_suffix_match`, `files_win_equal_quality_ties`, `explicit_kind_query_ranks_payload_and_offsets_highlights`, `empty_kind_queries_do_not_highlight`, `file_refresh_rebuilds_with_heuristic_order`, `refresh_clamps_selection_after_reordering`, `refresh_then_accept_inserts_selected_item`, and `host_and_guest_candidates_share_heuristic_order`.
- **AC.3** Command argument completion orders exact, prefix, contiguous substring, and fuzzy matches using the shared textual ranking. Verification: `command_argument_quality_tiers_are_ordered` fails if any tier is not ordered correctly.
- **AC.4** Command argument matching remains case-insensitive, remains single-token, and preserves provider order for equal ranking keys. Verification: `command_argument_matching_is_case_insensitive`, `command_argument_equal_rank_preserves_provider_order`, `argument_at_cursor_selects_middle_argument`, `argument_at_cursor_handles_multibyte_text`, and the new `command_argument_equal_rank_uses_provider_order_before_label` test, which supplies distinct labels with identical shared ranking metadata and asserts source order. Escaped-whitespace, negative-atom, and multi-positive-atom behavior is covered only by shared matcher tests, not command argument completion.
- **AC.5** Lua arguments use the shared ranking while preserving label/display, insertion/replacement, lifecycle events, context, and generation. Verification: `lua_command_arguments_use_shared_ranking`, `lua_argument_label_and_insertion_remain_distinct`, `enter_accepts_argument_then_executes_completed_command`, `enter_on_exact_argument_match_notifies_accept`, `zero_match_keeps_lifecycle_until_recovery_and_close_cancels_once`, `delayed_argument_response_is_rejected_after_navigation_resync`, and `lua_commands_update_on_generation_change`.
- **AC.6** Model and theme argument sources also use the shared matcher without changing source collection or `nargs` behavior. Verification: `model_arguments_use_shared_ranking`, `theme_arguments_use_shared_ranking`, and existing `sync_respects_nargs` tests pass.
- **AC.7** Stale argument results remain generation-safe and old results remain visible until a valid result arrives. Verification: `stale_argument_items_stay_until_new_result_lands` and a generation-mismatch regression test pass.
- **AC.8** Highlight indices remain zero-based Unicode codepoint offsets aligned with displayed labels. The supported contract intentionally indexes every Unicode scalar value from the original `chars()` haystack, including combining marks, flag components, and emoji codepoints, rather than Nucleo's default first-codepoint-per-grapheme `Utf32Str::new` behavior. Verification: `completion_indices_cover_combining_marks`, `completion_indices_cover_flag_components`, `completion_indices_cover_emoji_codepoints`, `at_completion_preserves_unicode_offsets`, and `command_argument_preserves_unicode_offsets`.
- **AC.9** Existing command-name ordering, command parsing, popup navigation, file refresh, selection, and insertion behavior do not regress. Verification: `lua_command_overrides_builtin_of_same_name`, `sync_filters_on_first_word_only`, `sync_respects_nargs`, `navigation_wraps`, `file_refresh_rebuilds_with_heuristic_order`, `refresh_clamps_selection_after_reordering`, `refresh_then_accept_inserts_selected_item`, `at_completion_preserves_insertion_range`, and `command_arguments_use_shared_matcher`.
- **AC.10** All six repository validation commands apply to this workspace and must pass: `just check`, `just lint`, `just test`, `just fmt-check`, `just gen-docs-check`, and `just machete`. Unrelated pre-existing failures are recorded separately in the review report and do not satisfy this criterion.

## Test Strategy

Pure matcher and comparator semantics use unit tests in `maki-match/src/lib.rs`. File behavior uses the existing direct menu helpers and injected candidates in `file_completion.rs`; asynchronous tests remain limited to the existing refresh harness. Command behavior uses `command.rs` palette fixtures and source fixtures, with assertions separated for ordering, labels, indices, insertion values, lifecycle events, and generation. Lua behavior uses the existing Lua host/plugin integration harness. New tests named in AC.1–AC.8 must be added at the stated modules and must directly assert the behavior they cover; existing tests only cover the regressions they actually exercise. No new test infrastructure is expected because these harnesses already exercise the required observable behavior.

## Review Strategy

Before handoff, run `plan_reviewer` against this plan and fix or explicitly rebut every critical and high finding. If a finding identifies a missing test harness, add the required infrastructure, acceptance criteria, and tests before handoff.

After implementation and all automatable checks pass, follow repository review guidance if present; otherwise dispatch a `general` review agent to inspect the final diff against AC.1–AC.10. Fix or explicitly rebut every finding. If any critical finding remains, repeat review after fixes, adding a focused regression test for ranking or stale-result findings.

## Documentation Strategy

No user-facing documentation change is expected because autocomplete syntax and Lua APIs remain unchanged. Update generated documentation only if exposing the shared API triggers the repository's documentation checks. Do not modify `AGENTS.md` unless the implementation introduces a repository-wide architecture rule.

## Risks, Blockers, and Required Decisions

- The current command path uses Nucleo matching on the current argument token, while the shared contract supports parsed multi-term patterns. Preserve the current observable whitespace/query behavior and add tests before changing any parsing.
- Case options differ: `@` uses smart case; command arguments use ignore case. Options must remain explicit at each call site.
- `nucleo-matcher` 0.3.1's default `Utf32Str::new` indexes one codepoint per grapheme, but this repository's existing matcher intentionally uses a `chars()` haystack. Preserve that explicit scalar-value indexing contract for completion highlights, including combining marks, regional-indicator flag components, and emoji codepoints; do not describe this as arbitrary Unicode case expansion. Nucleo's simple case handling must still be tested against original-label indices.
- Lua labels and insertion values can differ. Any sorting/highlighting based on insertion is a regression.
- Provider lifecycle and generation state are asynchronous. The shared matcher must remain pure and local to result processing.
- Existing legacy `maki-match` indices are 1-based. Do not silently change them while introducing zero-based completion indices.
- Full validation may expose unrelated failures. Record exact failures and do not bypass checks or mix unrelated fixes into this change.
