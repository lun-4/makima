# Heuristic ranking for `@` autocomplete suggestions

## Goal

Make `@` autocomplete surface the candidate that feels most like what the user meant. Rank exact and close textual matches above weak fuzzy matches, prefer files over tagged references only as a default tie-break, and honor explicit kind intent such as `@skill:` or `@model:` consistently for host and guest candidates.

## Implementation Summary

Change the shared `@` completion implementation in `maki-ui/src/components/file_completion.rs`. Keep Lua completion sources responsible only for supplying `ItemSpec` candidates. The UI will collect files and tagged references through the existing shared path, calculate comparable match metadata for both, and apply one deterministic heuristic sort after every refresh.

The current implementation stores only highlight indices and sorts by a two-tier prefix flag (`file_completion.rs:87-91`, `448-469`). Extend candidates with the ranking information needed to distinguish exact, prefix, contiguous substring, and fuzzy matches. Recompute scores for materialized file paths with the existing `Matcher`, because `Nucleo::snapshot().matched_items()` exposes matched items but not their scores. Keep the existing asynchronous walker and `MAX_MATERIALIZED` bound.

Recognize the existing full and short kind prefixes before fuzzy matching. Explicit kind intent filters candidates to the requested `CompletionItem.kind` and matches the remaining payload against the candidate's meaningful name while preserving label-relative highlight indices. Unrecognized prefixes remain ordinary free-text queries. Bare `@` remains files-first because it expresses no textual intent.

Scope boundaries:

- No Lua API change and no plugin-side ranking contract.
- No change to command argument completion in `command.rs` or `list_picker.rs`; its section/run semantics are a separate autocomplete surface.
- No dependency change unless the existing `nucleo` APIs prove insufficient during implementation.
- No change to insertion, expansion, popup navigation, or rendering beyond using the new highlight indices.

## Implementation Plan

### Phase 1 — Model query intent and candidate ranking

1. In `maki-ui/src/components/file_completion.rs`, define an internal query representation containing the normalized payload and an optional requested completion kind. Recognize:
   - `skill` and `skill:` plus `sk` and `sk:` → `skill`.
   - `subagent` and `subagent:` plus `su`, `su:`, `a`, and `a:` → `subagent`.
   - `model` and `model:` plus `m` and `m:` → `model`.
2. Prefix recognition is token-boundary based: a string is an alias only when it equals an alias or starts with `alias:`. Thus `skillfoo`, `modelish`, and `subagent-tools` remain ordinary free-text queries and can find labels containing those text sequences. Recognized aliases with empty payloads are explicit kind queries. Unrecognized strings, including an unrecognized `x:` form, remain ordinary queries. Preserve broad behavior for ambiguous `s:` by not assigning it a kind.
3. Extend `Candidate` with explicit ranking metadata: match quality, match start/boundary, matched span, gap count, fuzzy score, source kind, and stable source order. For files, assign source order from the path's canonical lexical ordering, not the asynchronous Nucleo result position; for references, retain their collected source index. Prefer an interpretable tuple over one opaque weighted integer.
4. Define the complete lexicographic ordering precisely. For ordinary non-empty queries, sort ascending by:
   - `quality_rank`: exact equality (0), prefix (1), contiguous substring (2), fuzzy-only (3);
   - `boundary_rank`: match beginning at label/path/kind boundary (0), otherwise (1);
   - `start_index`: earlier match start first;
   - `gap_count`: fewer gaps first;
   - `span_length`: shorter matched span first;
   - `unmatched_suffix`: shorter suffix after the final matched character first;
   - `fuzzy_score`: higher score first;
   - `source_rank`: files (0), tagged references (1);
   - `source_order`: references use collection order; files use a deterministic lexical path key (with the path as a final tie-break), so asynchronous Nucleo result order never affects equal-rank ordering.
   Explicit kind intent filters to the requested kind before this tuple and matches only the remaining payload. For empty queries, skip textual fields and use files-first followed by the same deterministic source-order rules. A prefix is a match beginning at label index zero; a boundary match is a nonzero match immediately after `/`, `-`, `_`, `.`, or `:` and remains in the prefix quality tier only when the prefix condition holds.

### Phase 2 — Implement shared matching and sorting

1. Refactor `highlight_indices`/`fuzzy_match` so matching returns highlight indices and ranking metadata. Use the existing case-insensitive lowercase contract and matcher APIs, preserving codepoint indices.
2. Classify non-empty candidates as exact, prefix, contiguous substring, or fuzzy. Use the same match operation that supplies highlights whenever possible. For each materialized file accepted by Nucleo, recompute the custom match against the full path. If it matches, use its metadata and indices. If it does not, retain the file only as a defensive fallback with `quality_rank = 4`, `boundary_rank = 1`, maximal start/gap/span/suffix values, zero fuzzy score, and no highlights, so it sorts after every successfully recomputed match. Empty queries match every materialized file/reference with neutral metadata and no highlights. Explicit kind filters are applied before files enter the combined list, so files are excluded for non-file kind requests.
3. For explicit kind queries, match the payload against the candidate label with the kind prefix removed, filter to the requested kind, and translate payload indices back to full-label codepoint offsets. Empty payloads match all candidates of that kind without highlights. No-colon forms are always kind intent only when the complete query equals `model`, `skill`, or `subagent`; `skillfoo`, `modelish`, and `subagent-tools` are ordinary free-text queries and must remain searchable.
4. Apply the same ranking construction to Lua references and materialized file paths. Retain the asynchronous walker and `MAX_MATERIALIZED` limit.
5. Replace `rebuild_combined`/`prefix_rank` with one stable combined sort. Rerun it after both `sync_query` and `refresh_file_matches`.
6. Preserve selection reset/clamping, popup navigation, insertion ranges, and completion rendering behavior.

### Phase 3 — Regression and behavior tests

Add focused unit tests in the existing `file_completion.rs` test module:

- exact, prefix, contiguous substring, and weak fuzzy tiers in the expected order regardless of source order;
- `github-iss` ranks `github-issue` before `github-issue-simple`;
- a clearly stronger tagged match beats a weak file fuzzy match, while comparable files remain ahead of tagged references;
- bare `@` and neutral queries remain files-first;
- fuzzy/shape tie-breaks are deterministic and equal keys preserve source order;
- case-insensitive matching and Unicode/codepoint highlights remain correct;
- `skill:`, `sk:`, `subagent:`, `su:`, `a:`, `model:`, and `m:` filter correctly, including empty payloads;
- ambiguous `s:` remains broad;
- unrecognized `x:` remains free text;
- explicit-kind payload ranking and full-label highlight offset translation;
- explicit non-file kind queries exclude files;
- no-colon exact aliases (`model`, `skill`, `subagent`) are kind intent, while concatenated strings such as `skillfoo`, `modelish`, and `subagent-tools` remain ordinary free-text queries;
- asynchronous file refresh applies the same ranking;
- host-collected and guest/plugin-collected candidates are combined and ranked identically through the shared menu path, using fixtures that label one candidate as host/file-origin and one as plugin-origin.

Reuse existing session helpers and direct file-match injection. Do not add filesystem or timing dependencies to ranking tests.

### Phase 4 — Validation, handoff, and review

1. Run `cargo check -p maki-ui --tests` or equivalent `just check` target.
2. Run `just lint`.
3. Run scoped `cargo nextest run -p maki-ui`, then `just test`/workspace tests.
4. Review the final diff against all acceptance criteria and verify every source feeding `FileCompletionMenu` receives the shared ranking policy.

## Acceptance Criteria

- **AC.1** Ordinary queries order exact above prefix, prefix above contiguous substring, and substring above weak fuzzy independent of source order. Test: `completion_quality_tiers_are_ordered`.
- **AC.2** Closer comparable matches win, including `github-issue` before `github-issue-simple` for `github-iss`. Test: `closer_match_beats_longer_suffix_match`.
- **AC.3** Kind is only a default preference: strong tagged matches beat weak file fuzzy matches, while comparable files win. Tests: `textual_quality_beats_default_file_preference`, `files_win_equal_quality_ties`.
- **AC.4** Bare `@` and neutral queries remain files-first; equal keys are deterministic. Tests: `bare_at_lists_files_before_refs`, `equal_rank_preserves_source_order`.
- **AC.5** Full and short explicit kind prefixes filter correctly, including empty payloads, while `s:` remains broad. Tests: `explicit_kind_prefixes_filter_candidates`, `ambiguous_kind_prefix_remains_broad`.
- **AC.6** Explicit kind queries rank the remaining payload and highlight the corresponding characters in the full label. Test: `explicit_kind_query_ranks_payload_and_offsets_highlights`.
- **AC.7** Unrecognized prefixes remain ordinary fuzzy queries. Test: `unrecognized_kind_prefix_is_free_text`.
- **AC.8** File refresh applies the same ranking without changing selection or insertion. Tests: `file_refresh_rebuilds_with_heuristic_order`, `refresh_clamps_selection_after_reordering`, and `refresh_then_accept_inserts_selected_item`.
- **AC.9** Host-collected and guest/plugin-collected completion candidates continue working without API changes and are ranked through the same shared menu path. Test: `host_and_guest_candidates_share_heuristic_order`, using a file-origin fixture and a plugin-origin `CompletionItem` in one menu; source availability remains covered by `plan_mode_hides_general_subagent`, `build_mode_hides_plan_reviewer_subagent`, and `skill_source_offers_builtin_skill` in `maki-lua/tests/completion_plugins.rs`.
- **AC.10** Case-insensitive matching and Unicode codepoint highlights remain correct. Test: `case_insensitive_ranking_and_codepoint_highlights`, asserting both ordering and full-label codepoint highlight offsets for a non-ASCII label.

## Test Strategy

Pure ranking logic belongs in `maki-ui/src/components/file_completion.rs` unit tests. Existing `maki-lua/tests/completion_plugins.rs` tests prove that host and guest sources still provide candidates; no new Lua harness is needed.

Every criterion has a named test above. Run `cargo check -p maki-ui --tests`, `just lint`, and `just test`. No test-infrastructure gap is known: direct menu helpers, injected file candidates, and Lua host completion tests already exist.

## Review Strategy

Plan review should check that all acceptance criteria have executable test coverage and that ranking semantics are explicit. During implementation, review the final diff against AC.1–AC.10 after automated checks. Any ranking-semantic finding should be resolved with a focused ordering test, not inspection alone.

## Documentation Strategy

No documentation change is required. This changes ordering in an existing `@` popup without changing the Lua API or workflow. Do not change `AGENTS.md` or add architecture docs unless implementation reveals a reusable matching contract beyond this menu.

## Risks, Blockers, and Required Decisions

- Nucleo snapshot items do not expose worker scores. Recompute metadata with the existing matcher for materialized paths and retain `MAX_MATERIALIZED` unless tests show a problem.
- Explicit-kind matching must translate payload indices back to full-label codepoint offsets; add the regression test before changing rendering.
- Asynchronous discovery can change the list. Keep sorting deterministic and selection clamped; never use thread timing as a tie-break.
- Preserve existing aliases (`a`, `m`, and short forms), but do not interpret ambiguous `s`/`s:` as one kind without a product decision.
- Command argument completion has separate section ordering and is outside this issue.
