### Goal

Prevent model-facing truncation metadata from silently becoming file content while preserving bounded read output and allowing safe mutations to source regions the model has actually observed. Provide a bounded recovery path for omitted bytes so files with long physical lines remain editable.

### Implementation Summary

Replace `FileReadTracker`’s mtime-only record with versioned read provenance. For each normalized path, retain freshness metadata, a fingerprint of the exact file content version, and merged source byte intervals shown losslessly to the model. The built-in `read` plugin will report those intervals from the final bounded output, and a new exact byte-chunk mode on the same tool will let callers accumulate coverage for omitted spans without returning unbounded content.

Before `write` or any `edit` variant commits, validate the actual `before` and proposed `after` snapshots. Use the existing workspace `similar` dependency to derive source- and destination-side change mappings, then reject any mutation that cannot conservatively prove all deleted/replaced source bytes were observed. Insert-only changes remain allowed because they preserve existing bytes. After a targeted edit, rebase prior coverage through unchanged spans and add only actual new result bytes; never treat the edit handler’s internal full-file reread as model-observed. A successful whole-file `write` may record the complete accepted content because that content was supplied by the model.

This avoids marker-string heuristics, permits legitimate literal `[line truncated]` text, allows safe edits outside truncated regions, and covers `edit`, `multiedit`, `edit_lines`, and `insert_lines` through their shared `apply_edit` path.

Relevant touch points are `maki-agent/src/tools/file_tracker.rs`, `maki-lua/src/api/util/ctx.rs`, `plugins/read/init.lua`, `plugins/read/read_helpers.lua`, `plugins/grep/init.lua`, `plugins/write/init.lua`, `plugins/edit/init.lua`, schema contract coverage in `maki-lua/tests/plugin_host.rs`, and dispatch-level coverage in `maki-lua/src/write_lock_regression.rs`. Existing output markers and output budgets remain unchanged. Grep remains freshness evidence but grants no lossless byte coverage because it returns incomplete snippets.

### Implementation Plan

#### Phase 1: Add versioned, interval-aware mutation provenance

1. Refactor `FileReadTracker` in `maki-agent/src/tools/file_tracker.rs` to store per-path state rather than only `SystemTime`:
   - Keep canonical path normalization and current stale-read behavior.
   - Store a deterministic fingerprint of the complete UTF-8 source snapshot supplied by a successful `read`, without retaining the full contents.
   - Store normalized, merged half-open byte intervals for source bytes shown losslessly to the model.
   - Accumulate intervals only when an observation has the same fingerprint; replace prior intervals when the content fingerprint changes and the observation is current.
   - Add a per-path provenance generation/observation lease: `read` captures the generation before filesystem I/O and may publish its snapshot only if no mutation commit advanced that generation. Mutation commits advance the generation. A read of V1 that finishes after an edit commits V2 must be discarded rather than replacing V2 provenance. External changes remain detected by snapshot fingerprints and the existing freshness policy.
   - Keep freshness separable from provenance so `stale_read_check = false` disables the external-mtime policy but not the truncation-safety invariant.
   - Treat missing filesystem mtime independently from provenance. Provenance must work with `InMemoryFs` and filesystems that expose no mtime through `std::fs`.

2. Introduce explicit tracker operations:
   - Record freshness-only evidence for tools such as grep without creating, widening, or erasing precise coverage for an unchanged tracked snapshot.
   - Record a complete content fingerprint plus one or more lossless source intervals for `read`.
   - Validate a proposed mutation from `before` to `after`, including fingerprint agreement and proof that no unseen source byte is discarded.
   - Commit a successful mutation by rebasing coverage to the `after` snapshot.
   - Record a successful whole-file write as completely known only when the entire accepted content came from the tool request.
   - Return distinct actionable errors for stale content, snapshot mismatch, and unseen changed bytes. The unseen-byte error should identify the first relevant source byte range and direct the caller to `read` byte-chunk mode.

3. Define mutation validation and coverage rebasing precisely:
   - Work in UTF-8-safe source units and convert all mappings to byte offsets before interval comparison.
   - Use `similar` to derive equal, delete, and insert spans. A replacement is a source-side delete adjacent to destination-side insertion.
   - A zero-length source-side change is an insertion and requires no prior source coverage. Every non-empty deleted/replaced source span, including any line terminator in that span, must be covered.
   - Treat equal-span boundary choices around repeated text conservatively. Start from the `similar` edit script, then verify each equal span against the maximal preceding and following unchanged anchors. For every changed region, enumerate every source occurrence between those anchors that can produce the same ordered unchanged suffix/prefix and destination text. Require the source-side deleted/replaced bytes to be covered for every candidate mapping; if candidate enumeration exceeds a small explicit safety bound or uniqueness cannot be proven, reject. Never accept solely because `similar` selected a covered occurrence.
   - Add repeated-text vectors for deletion, replacement, and `replace_all`: identical observed and unobserved occurrences must be rejected when any valid candidate consumes unseen bytes, while the same transformations succeed once every candidate source span is covered.
   - On successful targeted edits, map previously covered bytes through proven equal spans and discard coverage for deleted bytes. Mark destination bytes known only when the commit operation can map them verbatim to explicit string fields from the accepted tool request (`new_string` values); account explicitly for deterministic separators/newlines introduced by line tools, and do not infer model visibility from `diff_before`, `diff_after`, UI rendering, or the handler’s internal full-file reread. Bytes inserted by deterministic line-tool formatting may be marked only when their exact value and destination range are derived from the request contract; otherwise leave them uncovered.
   - On a pure insertion, preserve/rebase all prior coverage and mark only inserted bytes known.
   - Unit-test interval normalization, snapshot replacement/accumulation, UTF-8 offset mapping, insertion, replacement, deletion, disjoint changes, repeated text, and post-edit rebasing.

#### Phase 2: Make `read` emit precise provenance and add bounded recovery

1. Extend `plugins/read/read_helpers.lua` with a source-coordinate line representation. Each physical line must carry:
   - `start_byte`: first source byte.
   - `text_end_byte`: exclusive end of displayed line text.
   - `terminator_end_byte`: exclusive end after `\n` or `\r\n`.
   - The original text and whether the final line is unterminated.
   Define exact behavior for empty files, empty physical lines, a terminal newline, and a final unterminated line. CRLF display may continue to omit `\r`, but a terminator is covered only when the final output represents that complete line; when covered, both `\r` and `\n` belong to the source interval.

2. Refactor `plugins/read/init.lua` so final output and provenance are produced by the same bounded pass:
   - Preserve current line numbering and `[line truncated]` / `[file truncated]` rendering.
   - Represent each rendered fragment together with the source interval for only its verbatim bytes. Line-number prefixes, separators inserted for rendering, and synthetic markers have no source interval.
   - A fully emitted physical line covers its source text and complete terminator. A per-line-truncated line covers only the real prefix before the marker. An unterminated final line has no terminator bytes to cover.
   - Apply whole-output line/byte limits at rendered-fragment boundaries. Only fragments that survive into final `llm_output` grant coverage. Never infer final coverage from the pre-truncated intermediate string.
   - If the byte budget cannot fit a complete next rendered fragment, omit that fragment and append the existing file marker rather than granting partial source coverage that is not present in final output.
   - Record the complete source fingerprint and final surviving intervals after a successful read.

3. Add exact bounded byte-chunk mode to the existing `read` tool:
   - Make `offset`, `limit`, `byte_offset`, and `byte_limit` optional in the registered schema because the current schema validator does not support `oneOf`/`anyOf` conditional requirements.
   - In the handler, require exactly one complete pair: line mode requires both `offset` and `limit`; byte mode requires both `byte_offset` and `byte_limit`. Reject missing members, no mode, or mixed modes with stable useful errors. Existing valid line-mode calls retain identical behavior.
   - Interpret `byte_offset` as a zero-based source byte offset and `byte_limit` as the maximum source bytes requested. Reject negative/out-of-range offsets and non-character-boundary offsets. Cap the returned chunk by the existing whole-output byte budget and end at the greatest UTF-8 character boundary that fits.
   - Return explicit source byte range and total-size metadata around the verbatim chunk. Metadata is synthetic and grants no coverage; only chunk bytes do.
   - Repeated chunks for the same fingerprint accumulate coverage. This enables a later mutation of a formerly omitted span without exposing the whole file at once.
   - Update the read description and prompt hint so mutation errors explain the exact recovery call.

4. Update `maki-lua/tests/plugin_host.rs` schema/contract tests:
   - Replace assertions that `offset` and `limit` are schema-required with handler-level contract assertions.
   - Preserve successful existing line-mode parsing and execution.
   - Add byte-only success plus failures for no complete pair, one missing pair member, and mixed modes.
   - Verify generated tool schema/docs show all four mode fields as optional while documenting the exactly-one-pair rule.

5. Keep `plugins/grep/init.lua` as freshness-only evidence. Grep snippets may omit context, skip/truncate long lines, and cap output. A grep after a precise read must not erase valid provenance for the same unchanged snapshot, and grep alone must never authorize destructive mutation.

#### Phase 3: Validate every mutation at the common commit boundary

1. Update `maki-lua/src/api/util/ctx.rs` with handler-only context methods that follow the project’s `(value, err)` convention:
   - Record precise read provenance from a source snapshot and intervals.
   - Record freshness-only evidence.
   - Check a proposed mutation using `path`, `before`, and `after`.
   - Commit a successful targeted mutation by applying the validated mapping, without promoting internal reads.
   - Record a successful whole-file write snapshot as complete.

2. Update `plugins/edit/init.lua`’s shared `apply_edit` flow:
   - Preserve the existing stale check before reading and transformation.
   - After the complete current file is read and the transform produces `after`, validate provenance before `atomic_write`.
   - Keep validation, `atomic_write`, and provenance commit within the existing per-path dispatch lock.
   - Commit rebased coverage only after `atomic_write` succeeds.
   - Route `edit`, `multiedit`, `edit_lines`, and `insert_lines` through this single sequence.

3. Update `plugins/write/init.lua`:
   - Distinguish a new path from an existing file.
   - For an existing file, read the complete current content after the stale check, validate `before` against requested content, and call `atomic_write` only when provenance permits every removed/replaced source byte.
   - Preserve new-file creation because no prior source bytes exist.
   - Record complete requested content only after a successful write.
   - Keep check, read, validation, write, and commit inside the existing per-path dispatch lock.

4. Ensure `stale_read_check = false` affects only mtime freshness. Fingerprint mismatch and insufficient read coverage remain errors. A backing-content change must invalidate old coverage even if stale-mtime checking is disabled.

#### Phase 4: Add regression coverage and perform the marker audit

1. Expand `maki-agent/src/tools/file_tracker.rs` unit tests for mutation proof and rebasing while retaining all current stale-read tests. Include exact expected intervals and conservative repeated-content outcomes.

2. Expand `plugins/read/tests/spec.lua` with exact source-coordinate expectations for:
   - Empty input and empty physical lines.
   - LF and CRLF terminators.
   - Terminal newline versus final unterminated line.
   - Long ASCII and multibyte lines.
   - Per-line truncation prefixes.
   - Whole-output omission at a rendered-fragment boundary.

3. Add dispatch-level tests to `maki-lua/src/write_lock_regression.rs` using real bundled plugins, shared `ToolContext`, `tool_dispatch::run`, and `InMemoryFs`:
   - Update existing destructive-mutation fixtures in this module to prime existing files through a lossless dispatch of the real `read` tool with the same shared `ToolContext` before edit/write calls. Keep a separate `existing_file_without_provenance_is_rejected` regression so compatibility fixtures do not weaken the invariant.
   - `read_line_mode_records_only_final_rendered_ranges` constrains both per-line and whole-output budgets through real plugin options, then proves via subsequent allowed/rejected mutations that only fragments surviving final `llm_output` were recorded; include an omitted rendered fragment.
   - Long ASCII line output remains bounded and marked; a whole-file write based on the representation is rejected and bytes remain unchanged.
   - A long multibyte line truncates on a character boundary; destructive write and line replacement of its unseen suffix are rejected.
   - Literal `[line truncated]` and `[file truncated]` source content can be fully read and legitimately mutated.
   - A partial line read permits a replacement wholly inside covered bytes but rejects a replacement/deletion crossing into an unseen line.
   - `edit`, `multiedit`, and `edit_lines` reject unseen destructive changes; `insert_lines` succeeds when it only inserts.
   - A safe edit followed by an attempt to delete a still-unseen untouched region remains rejected, proving internal rereads and prior edits do not escalate coverage.
   - Multiple byte chunks accumulate coverage and eventually allow the formerly rejected mutation.
   - A full lossless read permits normal write and edit behavior.
   - Grep alone grants no destructive coverage and does not erase prior precise coverage for unchanged content.
   - Backing-content change invalidates old intervals with `stale_read_check` disabled.
   - New-file writes still succeed.

4. Use the existing instrumented backends in `maki-lua/src/write_lock_regression.rs` for commit-boundary regressions:
   - `failed_coverage_check_does_not_call_atomic_write` asserts the backend write method is never reached on rejection, not only that final bytes happen to match.
   - `successful_edit_preserves_unseen_coverage_boundary` performs a safe edit and then verifies a still-unseen destructive edit fails.
   - `validation_and_write_share_same_path_lock` parks the first mutation after validation but before completion and proves a same-path mutation cannot validate or write until the first releases.
   - `late_old_read_cannot_replace_committed_provenance` delays a V1 read across a successful V2 edit and proves the old observation lease is discarded, leaving the committed V2 fingerprint and rebased intervals authoritative.

5. Perform a repository-wide fixed-string audit for `[line truncated]` before completion and classify every occurrence. The planning audit found only intentional definitions, documentation, and tests (`maki-agent/src/tools/mod.rs`, `maki-lua/src/api/text.rs`, `plugins/lib/maki/tool_view.lua`, `plugins/lib/tests/spec.lua`, and generated Lua API docs), with no unexplained persisted marker. This is a required one-time implementation audit rather than a permanent automated acceptance criterion; record its result in the implementation summary and repair any unexplained occurrence.

### Acceptance Criteria

- **AC.1:** Existing line-mode `read` calls still enforce configured line/byte budgets and render current markers, while only final verbatim source fragments grant coverage.
- **AC.2:** A whole-file `write` that would delete or replace an unseen source byte for the current snapshot fails before any filesystem write and leaves the file unchanged.
- **AC.3:** `edit`, `multiedit`, and `edit_lines` reject actual before/after changes that cannot prove unseen source bytes are preserved, while changes confined to observed bytes succeed.
- **AC.4:** Pure insertions preserve existing provenance and remain possible when unrelated file regions are unseen.
- **AC.5:** Byte-chunk reads are bounded, UTF-8 safe, identify their exact source range, and accumulate coverage so omitted long-line regions can be recovered and safely mutated.
- **AC.6:** Literal truncation-marker text is ordinary source; no safety decision depends on searching mutation content for marker strings.
- **AC.7:** Long ASCII and multibyte lines cannot be silently shortened through whole-file writes or line replacements based on truncated output.
- **AC.8:** Provenance remains bound to the exact source snapshot, and neither handler-internal rereads nor successful targeted edits promote untouched unseen bytes to observed coverage.
- **AC.9:** Freshness configuration remains independent from provenance: disabling `stale_read_check` cannot authorize unseen or fingerprint-mismatched destructive changes.
- **AC.10:** Rejected mutations never call `atomic_write`, while accepted validation and write remain serialized under the existing same-path dispatch lock.
- **AC.11:** Existing stale-read behavior, new-file writes, successful full-read mutations, atomic writes, and existing same-path serialization behavior continue to pass.

### Test Strategy

| Acceptance criterion | Named regression coverage |
|---|---|
| AC.1 | `read_line_mode_records_only_final_rendered_ranges`; existing `truncate_line_cases` and `truncate_file_*` tests |
| AC.2 | `truncated_read_blocks_destructive_whole_file_write`; `failed_coverage_check_does_not_call_atomic_write` |
| AC.3 | `partial_read_allows_observed_edit_and_blocks_unseen_edits`; `all_edit_variants_validate_read_coverage`; tracker `repeated_unseen_occurrence_is_not_favorably_aligned` |
| AC.4 | `partial_read_allows_pure_insertion`; tracker `insertion_rebases_coverage_without_promoting_unseen_bytes` |
| AC.5 | `byte_chunks_are_utf8_safe_and_accumulate_coverage`; `read_modes_require_exactly_one_complete_pair`; `byte_chunk_rejects_invalid_boundaries` |
| AC.6 | `literal_truncation_markers_are_ordinary_source` |
| AC.7 | `long_ascii_line_cannot_be_written_back_truncated`; `long_multibyte_line_cannot_be_replaced_truncated` |
| AC.8 | `successful_edit_preserves_unseen_coverage_boundary`; tracker `targeted_edit_rebases_only_prior_and_new_coverage` |
| AC.9 | `snapshot_change_invalidates_coverage_with_stale_check_disabled`; `stale_check_disabled_does_not_disable_provenance` |
| AC.10 | `failed_coverage_check_does_not_call_atomic_write`; `validation_and_write_share_same_path_lock` |
| AC.11 | Existing `file_tracker` stale-read tests plus `full_read_allows_write_and_edit`, `write_new_file_succeeds_without_provenance`, `edit_and_write_handlers_use_atomic_write`, and the existing write-lock regression suite |

Run cheapest checks first while iterating:

1. `cargo test -p maki-agent tools::file_tracker`
2. `cargo test -p maki-lua spec::read_plugin_spec`
3. `cargo test -p maki-lua plugin_host` with filters for the read schema/mode contract tests
4. `cargo test -p maki-lua write_lock_regression`
5. `cargo check -p maki-agent --tests && cargo check -p maki-lua --tests`
6. `just check`
7. `just lint`
8. `just test`
9. `just gen-docs-check`

### Review Strategy

Before handoff, run the `plan-reviewer` and resolve or explicitly rebut every critical/high finding. Rerun review after corrections until no critical/high finding remains.

After implementation and automated tests, dispatch a `nat-code-reviewer` or follow more specific repository review guidance. Emphasize interval arithmetic, post-edit coverage rebasing, UTF-8/CRLF boundaries, repeated-text alignment conservatism, race freedom between validation and write, stale-check configuration semantics, and accidental authorization from grep or synthetic markers. Fix or explicitly rebut all findings and repeat review if any critical issue remains.

### Documentation Strategy

Update the read tool’s source description/schema and regenerate tool documentation rather than hand-editing generated pages. Document both valid call shapes:

- Line mode: `path`, `offset`, and `limit`.
- Byte mode: `path`, `byte_offset`, and `byte_limit`.

Explain that exactly one complete mode pair is required, byte offsets are zero-based UTF-8 boundaries, output remains bounded, and mutation errors report the chunk needed to recover unseen bytes. Run `just gen-docs-check` after generation.

The new context methods should remain handler-only capabilities. Update generated Lua API documentation only if the existing generator exposes them automatically; do not promote them as general plugin primitives without a separate API decision.

No standalone conceptual guide is needed because this is a mutation-safety invariant and a bounded extension of the existing `read` reference.

### Risks, Blockers, and Required Decisions

- **Conservative mapping:** Repeated content can yield several valid alignments. Correctness requires rejection whenever the implementation cannot prove unseen source bytes are preserved. The repeated observed/unobserved occurrence test pins this rule.
- **Coverage rebasing:** Targeted edits must preserve only mapped prior coverage plus actual new result bytes. Marking the complete post-edit file known would be a critical privilege escalation and is explicitly forbidden.
- **UTF-8 and line endings:** Intervals are source bytes, while diffs operate on UTF-8 text and line display strips CR. Exact LF, CRLF, terminal-newline, empty-line, and multibyte tests are required.
- **Schema limitation:** The current schema validator has no conditional union support. This plan intentionally uses optional fields plus handler-level exactly-one-mode validation and updates existing schema contract tests.
- **Race window:** Validation and write must use the same `before` snapshot under the existing path lock. External-process races remain governed by the existing stale check; this work must not weaken it.
- **Memory/CPU:** Store fingerprints and merged intervals, not whole snapshots. Diffing and rebasing happen only on mutation, where handlers already hold `before` and `after`; avoid unnecessary extra full-content clones.
- **Configuration semantics:** `stale_read_check = false` disables only mtime freshness, never provenance or fingerprint checks.
- **Grep semantics:** Grep remains freshness-only and cannot grant destructive mutation coverage.
- **One-time audit limitation:** The repository marker scan is an implementation audit, not an automated regression. Behavioral protection against future marker persistence is covered by AC.2, AC.3, AC.6, and AC.7.
- **Decision:** Correctness takes priority over minimum code. The selected design is range-aware, versioned provenance with bounded recovery rather than a coarse lossy flag or marker detection.