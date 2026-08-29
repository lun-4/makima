### Goal

Unify Rust and Lua completion behavior around one canonical textual matcher and comparator, while preserving the deliberate policy differences of individual surfaces. Lua plugins should be able to request the same matching options and ordering semantics used by Rust, and the built-in `/sessions` picker should rank matches consistently instead of using match/no-match only.

### Implementation Summary

Extend the existing `maki-match` completion contract so it has a reusable textual-ranking comparator separate from the full candidate comparator. Keep `CompletionMatch` and its ranking metadata as the canonical Rust representation, and expose the same semantics through `maki.match.completion(query, label, opts?)` and `maki.match.compare(left, right)`.

The Lua adapter will translate Rust zero-based codepoint indices to Lua's existing one-based codepoint convention, parse stable string options into the existing Nucleo case/normalization enums, and expose a three-way comparison result. Callers use `function(a, b) return maki.match.compare(a, b) < 0 end` as the `table.sort` predicate. The comparator will compare textual ranking only; source rank, provider order, sections, and async retrieval remain caller-owned policies.

Update the Lua sessions plugin to sort matched rows using the shared comparator and preserve the pre-filter order as the final tie-break. Make retaining a selected session across ordinary query edits and refreshes explicit, while preserving the existing session-rank/freeze behavior, rendering, and lifecycle apart from the intended result ordering and selection-retention change.

Affected touch points:

- `maki-match/src/lib.rs`: ranking comparator extraction, tests, and public matcher contract.
- `maki-lua/src/api/match.rs`: Lua options, comparator binding, documentation, and binding tests.
- `plugins/sessions/init.lua` and `plugins/sessions/sessions_helpers.lua`: ranked filtering with stable original-order ties and testable pure filtering logic.
- `plugins/lib/tests/spec.lua` and `plugins/sessions/tests/spec.lua`: Lua-facing API and sessions-order regression tests.
- `site/docs/content/lua-api/_index.md` and any generated reference output: regenerated API documentation.

Non-goals: changing legacy `maki.match.fuzzy`, `fuzzy_resolve`, or `fuzzy_resolve_candidates`; changing direct command resolution ambiguity behavior; replacing Nucleo's coarse asynchronous retrieval; changing source priorities in `@` completion; or forcing search, MCP tool search, and all pickers into one global list policy.

### Implementation Plan

#### Phase 1: Make textual ranking a first-class shared contract

1. In `maki-match/src/lib.rs`, extract the comparison of `CompletionRanking` into a focused public function, such as `compare_completion_rankings(left, right) -> Ordering`. It must compare, in order:
   - `quality_rank` ascending;
   - `boundary_rank` ascending;
   - `start_index` ascending;
   - `gap_count` ascending;
   - `span_length` ascending;
   - `unmatched_suffix` ascending;
   - `fuzzy_score` descending.
2. Keep `compare_completion_matches` as the full Rust candidate comparator. Make it call the ranking comparator first, then apply its existing source-rank, source-order, and label tie-breaks. This preserves the existing Rust UI behavior while making the universal portion reusable by Lua.
3. Preserve `completion_match`, `completion_match_default`, `CompletionMatch`, `CompletionRanking`, and all legacy matcher/resolver APIs. Do not change the existing zero-based Rust completion indices or the legacy one-based `fuzzy_match` indices.
4. Add direct unit tests with competing `CompletionRanking` values for every comparator field and direction, including the descending raw fuzzy score. Add tests proving that equal textual ranks are equal before caller-owned source policy is applied, and that the full comparator still orders source rank, source order, and label after textual rank.

#### Phase 2: Expose configurable matching and canonical comparison to Lua

1. Change the Lua-facing completion signature to accept an optional third argument:

   ```lua
   maki.match.completion(query, label, opts?)
   ```

   Keep the existing two-argument form behavior exactly unchanged. The Rust binding parameter remains named `opts`; its generated `@param` entry must use `opts` with the repository's accepted optional-table type syntax, not `opts?` as a parameter name.
2. Parse `opts.case` as one of `"smart"`, `"ignore"`, or `"respect"`, defaulting to `"smart"`. Parse `opts.normalization` as `"smart"` or `"never"`, defaulting to `"smart"`. Convert these strings to `nucleo_matcher::pattern::{CaseMatching, Normalization}` without exposing Rust enum names to Lua.
3. Accept the optional options argument as a raw `LuaValue`/`Option<LuaValue>` at the binding boundary so the implementation can distinguish nil, a table, and invalid values. Treat a missing options value or nil option fields as defaults. Validate each present field explicitly and raise a clear Lua programmer-error naming `match.completion`, the field, and the offending type/value for a wrong type or unsupported string. Do not silently fall back for an explicitly invalid option.
4. Add `maki.match.compare(left, right)`, accepting match tables returned by `maki.match.completion` and returning an integer in the conventional `-1`, `0`, `1` form. It must compare only the `ranking` fields using the shared Rust ranking comparator. This raw three-way result is not itself a `table.sort` predicate: callers must use `function(a, b) return maki.match.compare(a, b) < 0 end`. Document that source priority/order and list grouping are intentionally not part of this helper and must be applied by the caller.
5. Keep `maki.match.fuzzy(query, text)` unchanged, including its result shape (`score` plus one-based `indices`), default behavior, and compatibility tests. Do not rename `fuzzy_score` in the completion result or add a duplicate top-level score.
6. Add Lua unit tests for:
   - default completion behavior and one-based codepoint indices;
   - `case = "smart"`, `"ignore"`, and `"respect"` differences;
   - `normalization = "smart"` versus `"never"`;
   - nil option fields and invalid outer/table field types and values;
   - comparator sign and equality for exact/prefix/substring/fuzzy results and structural tie-breaks;
   - comparison of results with equal textual rank;
   - sorting three results with `function(a, b) return maki.match.compare(a, b) < 0 end`, proving the documented wrapper produces the Rust order;
   - malformed comparison operands, including non-table operands, missing `ranking`, missing ranking fields, wrong field types, zero/invalid public `start_index`, and a legacy `maki.match.fuzzy` result.
7. Parse comparator operands as the result shape produced by `completion`: validate every ranking field as a non-negative integer in the expected range, convert the public one-based `ranking.start_index` back to Rust's zero-based `start_index`, and reject malformed values with errors naming `match.compare` and the offending field.
8. Add or update Lua plugin-spec coverage in `plugins/lib/tests/spec.lua` for the public options and comparator, including a Rust/Lua-parity case where the same query, label, and options produce hard-coded expected match and indices. Use explicit expected arrays for CJK, combining-mark, regional-indicator flag, and skin-tone emoji labels, and assert Lua's one-based values against Rust's zero-based values after conversion.

#### Phase 3: Use the same policy in the sessions plugin

1. Add a pure helper in `plugins/sessions/sessions_helpers.lua` for filtering and ordering a list of session rows. It should accept the rows, query, matcher function, and textual comparator function (or an equivalent dependency-injection shape), retain each match and original position, and sort with a boolean wrapper around the three-way comparator: `compare(left.match, right.match) < 0`; equal textual ranks must use the original row position as the stable tie-break. Make `apply_filter` in `plugins/sessions/init.lua` delegate its filtering and ordering to this helper, passing `maki.match.completion` and the `< 0` wrapper around `maki.match.compare`, so the production path uses the tested policy. This keeps the ordering policy testable without booting the full UI.
2. Extract a pure selection-reconciliation helper in `sessions_helpers.lua` that accepts the previous selected ID, previous visible position, and newly filtered rows, and returns the selected ID (or equivalent selected position). It must retain the selected ID when it still matches and otherwise clamp the previous visible position to the new result count. Call this helper from `init.lua` so the plugin-spec tests exercise the actual selection policy through the same helper used by production code. Because the existing plugin spec harness does not load `init.lua`, add a small Rust source-contract test using `include_str!("../../plugins/sessions/init.lua")` (or the correct relative path) to assert that production wiring calls the reconciliation helper and no longer unconditionally clears the selection on ordinary filter changes.
3. Make selection retention across ordinary query edits an explicit behavior change: remove the unconditional `board.sel_id = nil` reset in `filter_changed`, while preserving the existing reset behavior when opening a picker or otherwise intentionally starting a new selection. Keep `board.all` ordering, recency/rank assignment, refresh behavior, loading behavior, and session IDs unchanged. Define the same reconciliation path for query edits and refreshes so a selected ID is retained whenever it remains visible.
4. Store each match on the session row as today so rendering continues to use `Spans.match_spans` with one-based indices. Update the stale comment that says scores are ignored to describe the new textual ordering and stable tie-break.
5. Add sessions plugin tests covering exact/prefix/fuzzy ordering, original-order ties, selection retention by ID, selection fallback when the selected row disappears, empty queries, and one-based highlight indices. Use injected matcher/comparator functions for pure helper tests, and add a production-wiring assertion through the extracted reconciliation helper so removing the `init.lua` call or restoring the unconditional reset is detectable without timing or filesystem dependencies.

#### Phase 4: Verify Rust consumers and documentation boundaries

1. Confirm existing Rust completion callers continue to pass their explicit policies:
   - `@` completion keeps smart case/normalization, explicit kind parsing, files-vs-reference source rank, lexical file materialization order, and full-label highlight offsets.
   - command names and command arguments keep ignore-case/smart-normalization and provider-order tie-breaking.
   - file picker and generic list picker keep their existing grouping and source behavior.
2. Where helpful, replace direct ranking-field chains in Rust callers with `compare_completion_rankings` only when that does not alter source/grouping policy. Do not replace the full comparator where source rank/order/label are required.
3. Leave direct `/model` and `/theme` resolution on `fuzzy_resolve*`, search modal scoring on its local matcher, and MCP tool search on its token/name/description scoring. Add no compatibility shim that makes these silently choose a ranked result.
4. Expand the generated `maki.match` documentation from the source comments in `maki-lua/src/api/match.rs` to list:
   - the optional options table and allowed values;
   - the complete result shape and every ranking field;
   - one-based Unicode codepoint indices;
   - the distinction between textual ranking and source/list policy;
   - `maki.match.compare` return values and intended `table.sort` usage;
   - the legacy role of `maki.match.fuzzy`.
5. Regenerate documentation with the repository's existing generator rather than hand-editing generated output, and verify the generated diff is limited to the expected API documentation.

#### Phase 5: Validation and review

1. Run focused tests first: `cargo test -p maki-match -p maki-lua`, the relevant `maki-ui` tests, and the Lua plugin specs through the existing harness.
2. Run repository validation from `justfile`: `just check`, `just lint`, `just test`, `just fmt-check`, `just gen-docs-check`, and `just machete` as applicable. Record exact unrelated failures rather than changing unrelated code.
3. Review the final implementation against the separation of concerns: one canonical textual matcher/comparator, explicit Lua options, caller-owned source policy, stable sessions ties, unchanged legacy APIs, and complete docs.

### Acceptance Criteria

- **AC.1** Rust exposes one canonical textual ranking comparator whose ordering is quality, boundary, start, gaps, span, suffix ascending and fuzzy score descending. Verification: `completion_ranking_comparator_orders_each_field` and `completion_match_comparator_applies_source_policy_after_textual_rank` in `maki-match`.
- **AC.2** Existing Rust completion and legacy matching behavior remains compatible. Verification: `maki-match` tests `empty_query_matches_with_zero_score`, `no_match_returns_none`, `indices_are_1_based_codepoints`, `multi_term_order_is_independent`, `smart_case_uppercase_query_is_case_sensitive`, `smart_case_lowercase_query_is_case_insensitive`, `indices_are_codepoints_not_graphemes`, `repeated_terms_dedup_indices`, `completion_match_returns_zero_based_indices`, and `completion_match_requires_all_positive_atoms_and_excludes_negative_indices`, plus the named `maki-ui` completion regression tests in AC.8.
- **AC.3** Lua `maki.match.completion(query, label)` retains its current default result shape and indices. Verification: `completion_returns_one_based_indices_and_ranking` and `match_completion_default_matches_rust_contract` in `maki-lua`, plus the corresponding Lua plugin spec.
- **AC.4** Lua can select smart, ignore, or respect case matching and smart or never normalization, with invalid options rejected clearly. Verification: `completion_options_control_case_matching`, `completion_options_control_normalization`, and `completion_rejects_invalid_options` in `maki-lua`.
- **AC.5** Lua `maki.match.compare` produces the same textual ordering as Rust and does not apply hidden source policy. Verification: `compare_completion_results_orders_textual_rank`, `compare_completion_results_returns_zero_for_equal_rank`, and `match_compare_matches_rust_ordering` in `maki-lua`/Lua plugin specs.
- **AC.6** The `/sessions` picker uses textual ranking while preserving original order for equal textual ranks and retaining the selected session when possible. Verification: `sessions_filter_ranks_matches`, `sessions_filter_preserves_original_order_for_ties`, `sessions_filter_keeps_selected_id`, `sessions_filter_clamps_missing_selection`, and the one-based highlight test in `plugins/sessions/tests/spec.lua` exercise the pure helpers. A named Rust source-contract test, `maki-lua/tests/sessions_policy.rs::sessions_init_wires_filter_and_selection_reconciliation`, uses `include_str!("../../plugins/sessions/init.lua")` (or the correct relative path) to assert that `apply_filter` delegates to the filter helper, production code calls the reconciliation helper, and ordinary `filter_changed` no longer unconditionally clears `board.sel_id`. This Rust test is the production-wiring check because the Lua plugin spec does not load `init.lua`.
- **AC.7** Unicode highlighting remains aligned across Rust and Lua, with one-based Lua indices and zero-based Rust indices referring to the same original-label codepoints. Verification: existing `indices_are_codepoints_not_graphemes` and picker Unicode tests plus `completion_unicode_indices_match_between_rust_and_lua` with hard-coded expected arrays for CJK, combining marks, regional-indicator flags, and skin-tone emoji codepoints.
- **AC.8** Rust surface-specific policies remain intact: `@` source ranking and kind parsing, command provider order and insertion distinction, list-picker sections, file-picker ordering, and asynchronous retrieval/truncation. Verification: the existing `maki-ui` tests `completion_quality_tiers_are_ordered`, `explicit_kind_query_ranks_payload_and_offsets_highlights`, `file_refresh_rebuilds_with_heuristic_order`, `file_refresh_uses_lexical_source_order_for_ties`, `command_argument_equal_rank_uses_provider_order_before_label` (add this focused test if absent), `lua_argument_label_and_insertion_remain_distinct` (or the current equivalent), `filter_ranks_by_score_not_source_order`, `filter_section_groups_stay_contiguous`, and the file-picker materialization/truncation tests pass without changing their expected policy-level order.
- **AC.9** Legacy and intentionally separate scoring paths remain separate. Verification: `model_arg_ambiguous_flashes`, `theme_arg_fuzzy_unique_applies`, `matches_sorted_by_score_descending`, the MCP search exact/name/description ordering test (add a discriminating case if absent), and the `maki.match.fuzzy` plugin/spec tests pass; direct call-site review is an additional safeguard, not the sole test for this criterion. A discriminating ambiguity case must continue to reject a direct `/model` or `/theme` argument even when completion ranking would choose one candidate.
- **AC.10** Public Lua documentation describes the options, result fields, index convention, comparator, and policy boundary. Verification: add `generated_match_docs_contain_completion_contract` against the existing `maki-docgen` generation/rendering function, asserting the signatures, allowed option values, every ranking field, one-based codepoint wording, explicit `< 0` `table.sort` usage, source-policy boundary, and legacy fuzzy role; also run `just gen-docs-check` to verify generated output synchronization.
- **AC.11** Repository validation passes. Verification: `just check`, `just lint`, `just test`, `just fmt-check`, `just gen-docs-check`, and `just machete` pass, with unrelated pre-existing failures recorded separately.

### Test Strategy

| Acceptance criteria | Named tests/checks |
|---|---|
| AC.1 | `completion_ranking_comparator_orders_each_field`; `completion_match_comparator_applies_source_policy_after_textual_rank` |
| AC.2 | `maki-match::tests::{empty_query_matches_with_zero_score,no_match_returns_none,indices_are_1_based_codepoints,multi_term_order_is_independent,smart_case_uppercase_query_is_case_sensitive,smart_case_lowercase_query_is_case_insensitive,indices_are_codepoints_not_graphemes,repeated_terms_dedup_indices,completion_match_returns_zero_based_indices,completion_match_requires_all_positive_atoms_and_excludes_negative_indices}`; named `maki-ui` tests in AC.8 |
| AC.3 | `completion_returns_one_based_indices_and_ranking`; `match_completion_default_matches_rust_contract`; Lua `match_completion_default` spec |
| AC.4 | `completion_options_control_case_matching`; `completion_options_control_normalization`; `completion_rejects_invalid_options` |
| AC.5 | `compare_completion_results_orders_textual_rank`; `compare_completion_results_returns_zero_for_equal_rank`; Lua `match_compare_matches_rust_ordering` spec |
| AC.6 | `plugins/sessions/tests/spec.lua` pure-helper tests: `sessions_filter_ranks_matches`, `sessions_filter_preserves_original_order_for_ties`, `sessions_filter_keeps_selected_id`, `sessions_filter_clamps_missing_selection`, and one-based highlight coverage; `maki-lua/tests/sessions_policy.rs::sessions_init_wires_filter_and_selection_reconciliation` source-contract test using `include_str!("../../plugins/sessions/init.lua")` to assert filter-helper delegation, reconciliation-helper wiring, and removal of the ordinary-filter `board.sel_id = nil` reset |
| AC.7 | Existing `indices_are_codepoints_not_graphemes`, list/file Unicode tests; `completion_unicode_indices_match_between_rust_and_lua` with hard-coded expected arrays |
| AC.8 | `maki-ui/src/components/file_completion.rs::{completion_quality_tiers_are_ordered,explicit_kind_query_ranks_payload_and_offsets_highlights,file_refresh_rebuilds_with_heuristic_order}`; `maki-ui/src/components/file_picker.rs::{file_refresh_uses_lexical_source_order_for_ties,materialized_match_count_is_capped}` (or current equivalents); `maki-ui/src/components/command.rs::{command_argument_equal_rank_uses_provider_order_before_label,lua_argument_label_and_insertion_remain_distinct}` (add focused tests if absent); `maki-ui/src/components/list_picker.rs::{filter_ranks_by_score_not_source_order,filter_section_groups_stay_contiguous}` |
| AC.9 | `maki-ui/src/app/tests.rs::{model_arg_ambiguous_flashes,theme_arg_fuzzy_unique_applies}`; `maki-ui/src/components/search_modal.rs::matches_sorted_by_score_descending`; a discriminating MCP search exact/name/description ordering test (add if absent); `plugins/lib/tests/spec.lua` legacy `match_fuzzy_*` cases. Add a direct ambiguity case where completion ranking would prefer one `/model` or `/theme` candidate but legacy resolution must still reject the ambiguous argument. Static call-site review is an additional safeguard, not the sole verification. |
| AC.10 | `maki-docgen` semantic test `generated_match_docs_contain_completion_contract` against the existing generation/rendering function, plus `just gen-docs-check` |
| AC.11 | `just check`; `just lint`; `just test`; `just fmt-check`; `just gen-docs-check`; `just machete` |

Pure matcher/comparator tests belong in `maki-match/src/lib.rs`. Lua conversion and option parsing belong in `maki-lua/src/api/match.rs`. Lua-facing behavioral examples belong in `plugins/lib/tests/spec.lua`; sessions ordering is tested through an injectable pure helper because the plugin's UI entrypoint is not directly loaded by the plugin spec harness. Existing UI integration tests remain the regression layer for Rust surface policies and asynchronous behavior.

### Review Strategy

Before handoff, run the `plan-reviewer` subagent against this plan, the user request, and the inspected files. Fix every critical or high finding in the plan or explicitly rebut it before handoff.

After implementation and all automatable checks pass, dispatch a general-purpose review agent (or follow repository review guidance if added) to inspect the final diff against AC.1–AC.11. Resolve all critical and high findings, adding focused regression tests for any comparator, option-parsing, or sessions-order issue before repeating review.

### Documentation Strategy

Document the public contract in the Rust doc comments that feed `maki-docgen`, then regenerate `site/docs/content/lua-api/_index.md` and any related generated references with `just gen-docs`. Explain that `completion` exposes textual ranking while callers decide source priority, grouping, and stable order. Keep `maki.match.fuzzy` documented as the compatibility/raw-score API.

No repository-wide architecture document or `AGENTS.md` change is needed because this formalizes an existing `maki-match` boundary rather than introducing a new subsystem.

### Risks, Blockers, and Required Decisions

- Adding options and `maki.match.compare` expands the public Lua API. Preserve the two-argument completion form and reject only explicitly invalid options so existing plugins remain source-compatible.
- `CaseMatching` is `Respect`, `Ignore`, or `Smart`, while `Normalization` is `Never` or `Smart`; keep the Lua strings stable and do not expose dependency-specific enum names. The implementation must handle the dependency's non-exhaustive enums without assuming future variants.
- Lua `compare` should accept only the result shape produced by `completion`; malformed tables are programmer errors. Its documentation must make clear that it does not compare source rank or provider order.
- The sessions picker will intentionally change the order of filtered rows. Its existing frozen `board.all` order and stable tie-break preserve predictability, but exact/prefix matches will move above weaker matches. This is the agreed product behavior.
- Rust's Nucleo worker remains a coarse retrieval mechanism for asynchronous file/path lists. The final shared match can only rank materialized candidates, so the existing materialization/truncation boundary remains a product limitation.
- Do not change one-based legacy `fuzzy_match` indices or use the completion result's zero-based Rust representation directly in Lua/plugin rendering.
- Do not make direct `/model` or `/theme` commands silently resolve the highest-ranked fuzzy result; ambiguity is an intentional command-line safety behavior.
- Full validation may expose unrelated failures. Record them precisely and keep unrelated fixes out of this change.
