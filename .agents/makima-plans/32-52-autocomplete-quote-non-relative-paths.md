### Goal

Fix the complete `@` reference workflow described by GitHub issues #32 and #52: support punctuation-safe and quoted reference syntax, automatically quote completion insertions when required, close completion at unquoted whitespace, and browse explicit non-project file paths beginning with `~`, `/`, or `.` one directory at a time.

### Implementation Summary

Treat the two issues as one shared token contract across `maki-lua` parsing and `maki-ui` completion.

- In `maki-lua/src/api/completion.rs`, make `parse_at_tokens` understand unquoted trailing sentence punctuation and single- or double-quoted values. Parsed values exclude delimiters and stripped punctuation, while token ranges cover exactly the reference text that should be expanded or highlighted.
- In `maki-ui/src/components/file_completion.rs`, replace the popup’s independent “everything before the cursor since `@`” logic with quote-aware active-token parsing that agrees with the submit-time grammar. Unquoted whitespace ends completion; an unfinished quoted value remains active across spaces.
- Normalize final completion insertions in the UI so values containing whitespace or punctuation that the unquoted parser would trim are enclosed in quotes. Preserve any intentional whitespace after a plugin insertion as a delimiter outside the closing quote.
- Keep the existing project-relative `Nucleo` walker for ordinary file queries. For queries beginning with `~`, `/`, or `.`, resolve the requested parent from the session cwd and optional home, synchronously `read_dir` exactly that parent, and fuzzy-rank only its immediate children. Selecting a directory advances the active token and refreshes; selecting a file finalizes and closes it.

Affected contracts and files:

- `maki-lua/src/api/completion.rs`: canonical `AtToken` grammar, expansion ranges, `ItemSpec` insertion documentation, parser/expander tests.
- `maki-ui/src/components/file_completion.rs`: active token range/query extraction, insertion quoting, explicit-path discovery, candidate action metadata, popup lifecycle, unit tests.
- `maki-ui/src/app/mod.rs`: advance-versus-final selection handling and post-replacement synchronization.
- `maki-ui/src/components/input.rs`: parser-driven highlighting regressions and quoted file existence checks.
- `maki-ui/src/app/tests.rs`: end-to-end completion interaction regressions.
- `site/docs/content/references/_index.md` and generated Lua API documentation source/output as applicable: document punctuation, quoting, and explicit-path browsing.

Scope boundaries:

- Do not alter agent-side path resolution in `maki-agent/src/tools/mod.rs:283-300`.
- Do not change the full-screen picker or `spawn_file_walker` implementation in `maki-ui/src/components/file_picker.rs`.
- Keep escaping minimal and local to quoted `@` values: backslash escapes only the active quote delimiter and backslash itself; a backslash before any other character remains literal. Do not change slash-command argument completion or its separate `CommandArgumentItem` contract.
- Do not add dependencies.

### Implementation Plan

1. Define one explicit `@` token grammar in `maki-lua/src/api/completion.rs`.
   - Preserve `at_is_token_start`: `@` starts a reference only at the start of text or after whitespace.
   - Split parsing after `@` into an optional case-insensitive `prefix:` and a value.
   - Support quoted values in both file and tagged forms: `@"file with spaces.md"`, `@'file with spaces.md'`, `@skill:"bad skill name"`, and `@skill:'bad skill name'`.
   - Require the opening and closing delimiters to match. A closed quoted token may contain whitespace and the punctuation that would otherwise be stripped. Within a quoted value, `\\` decodes to one backslash and a backslash before the active quote delimiter decodes to that literal delimiter; before any other character, the backslash remains part of the value. Store the decoded inner value without quotes, but retain a byte range from `@` through the closing delimiter so expansion/highlighting replaces/styles the whole token. Treat an unterminated quote or dangling terminal escape as incomplete rather than silently consuming unrelated trailing prose at submit time.
   - For unquoted values, stop at the first whitespace and trim only the right-edge punctuation specified by issue #32: `, . ! ? ) ] } " '` and their full-width counterparts. Reduce the token range by the stripped UTF-8 byte length so punctuation remains outside expansion and highlighting. Do not strip punctuation from the middle of a value or from a quoted value.
   - Continue skipping empty tagged values such as `@skill:`. Ensure punctuation-only values that become empty do not produce a reference.
   - Keep unknown prefixes and file references verbatim during `expand_references`, but because their ranges now exclude trailing punctuation, preserve that punctuation in the surrounding output. Recognized expanders receive only the normalized unquoted/quoted value.

2. Make completion use the same quote-aware token boundaries.
   - Add or expose a focused helper contract from `maki-lua` for the active token under the cursor, or implement a UI helper directly against shared parser primitives if exporting a new parser API would add unnecessary surface. In either case, codify parity with table-driven tests shared across equivalent inputs.
   - Replace `maki-ui/src/components/file_completion.rs::at_token_range` behavior so an unquoted token ends at whitespace. Consequently, typing `@partial` followed by a space closes the earlier popup rather than continuing to match it.
   - Keep an unfinished quoted token active across whitespace until its matching delimiter is typed. Extract the completion query without the opening quote and optional tagged prefix syntax, while retaining enough quote state to construct the replacement correctly.
   - Ensure cursor-in-middle behavior, non-ASCII byte/character offsets, mid-word `@` rejection, and command-palette precedence continue to work.

3. Centralize safe final insertion formatting in `maki-ui/src/components/file_completion.rs`.
   - Parse each candidate’s existing insertion into a reference body plus any intentional trailing whitespace delimiter (for example the built-in model/subagent candidates).
   - Quote the value when it contains whitespace or any character that the unquoted parser would strip from the right edge. Preserve the candidate’s `@` prefix and tagged prefix, choose a deterministic delimiter (`"` by default, with `'` preferred when it avoids escaping), escape any remaining active delimiter and backslashes according to the quoted grammar, and put intentional trailing whitespace after the closing quote.
   - Do not double-quote already valid quoted insertions. Keep labels and fuzzy-match targets unchanged; normalization applies to replacement text only.
   - Apply the same helper to built-in file candidates and Lua-provided candidates so plugin authors do not each need to reproduce parser rules. Update the `ItemSpec` API documentation to state that `insertion` is a logical `@` insertion and the UI quotes its value when required.
   - For explicit-path directory advancement, keep the token syntactically open: preserve an opening quote without adding the closing delimiter while the user is still browsing. Add the closing quote only on final file/non-directory selection.

4. Add explicit-path resolution and depth-one discovery to `maki-ui/src/components/file_completion.rs`.
   - Retain session cwd and optional home context. Production obtains home from `maki_storage::paths::home()`; resolver/discovery helpers accept injected cwd/home for deterministic tests.
   - Classify only the path value’s first character `~`, `/`, or `.` as explicit mode, including when that value is inside an unfinished quote. Tagged queries remain non-file queries.
   - Resolve `~` and `~/…` from home; rooted/absolute syntax through `Path`; and `.`, `./…`, `..`, and `../…` from session cwd. On Unix, `/` is the filesystem root. On Windows, leading `/` is current-drive root-relative, matching `Path` and the existing runtime resolver contract. Derive one parent directory to read plus the unfinished final component. Bare context switches (`~`, `/`, `.`, `./`, `..`, `../`) list that directory’s immediate children; partial/deeper queries read only the represented parent and fuzzy-match the leaf.
   - Perform one synchronous `read_dir` call through a small injectable discovery function/trait. This seam must let a counting fake assert the number of reads and returned entries without relying on implementation inspection.
   - Skip unreadable entries and unsupported file types. Include files, directories, and usable symlinks consistently with the current picker; use following metadata where needed to identify symlinks to directories as descendable.
   - Preserve the user-facing namespace (`~/`, rooted/absolute, `./`, or `../`) in labels and replacements. Preserve separators already typed by the user; append `MAIN_SEPARATOR` only when generating the next directory boundary. Match the unfinished leaf with existing `completion_match` ranking, offset highlight indices by the displayed prefix, and retain deterministic lexical source-order tie breaking.
   - Explicit mode contains only filesystem children, not project-index or Lua reference candidates. Missing/unreadable/non-directory parents or absent home produce an active session with no candidates rather than a panic, stale matches, or forced closure.

5. Make project and explicit discovery lifecycles mutually exclusive and reversible.
   - Refactor `FileCompletionMenu::open` so an initially explicit query does not start `spawn_file_walker` at all. Represent project discovery as optional state rather than requiring a live `Nucleo`/walker in every session.
   - If an existing ordinary session switches into explicit mode, cancel/drop its project walker state before publishing explicit results. If the user deletes the explicit prefix and returns to an ordinary query, start a fresh project walker rooted at the retained session cwd.
   - Ensure project walker completion/tick events cannot overwrite explicit results. Clear project refresh counters/pending flags when entering explicit mode, and clear explicit candidates when leaving it.
   - Set popup visibility directly from explicit discovery state/results instead of relying on current `Nucleo` injector checks. An explicit query with candidates becomes visible immediately; an explicit query with no candidates remains an active but non-rendered session so further editing can recover. Adjust `tick` and `cadence` for optional project state without fake walking/matching activity.
   - Preserve existing debounce, materialization cap, asynchronous refresh, ranking, and crash flash behavior in ordinary project mode.

6. Represent directory advancement separately from final completion.
   - Add internal candidate/action metadata that explicitly identifies descendable explicit-path directories. Do not infer navigation from display text or a trailing separator alone.
   - Make Enter and Tab return an advance action for such directories and the existing final-select action for files and non-file references. Preserve navigation keys, Esc, modified cursor behavior, and passthrough behavior.
   - In `maki-ui/src/app/mod.rs`, handle advance by replacing the active token with the selected directory’s open replacement, updating cursor/range, synchronizing command and argument completion, and immediately re-running file completion without closing it.
   - Handle final selection by inserting the safely quoted final replacement, synchronizing command/argument completion, and closing the popup as today.

7. Add parser, expansion, and highlighting regressions for issue #32.
   - In `maki-lua/src/api/completion.rs`, add `#[test_case]` coverage for every ASCII/full-width trailing punctuation class, multiple trailing marks, punctuation in the middle, quoted file/tagged values with spaces and punctuation, both delimiter styles, mismatched/unterminated quotes, empty-after-trim values, UTF-8 ranges, and multiple references in one line.
   - Add expansion tests proving stripped punctuation remains after a recognized replacement, quoted values are passed unquoted to expanders, unknown/file tokens pass through with their original quoted source, and malformed quoted text remains unchanged.
   - In `maki-ui/src/components/input.rs`, assert highlighting covers quotes but excludes stripped punctuation, resolves quoted file values by existence, and leaves surrounding punctuation/raw text styled independently.
   - In `maki-ui/src/components/file_completion.rs`, table-test active-range parity: unquoted whitespace closes the token; unfinished quotes retain spaces; matching closing quotes finalize the token; cursor-middle and Unicode offsets remain correct.
   - Test insertion normalization for file and tagged candidates, trailing plugin delimiter preservation, both quote-delimiter choices, no double quoting, and punctuation/space-free insertions remaining byte-for-byte unchanged.

8. Add explicit-path and combined interaction regressions for issue #52.
   - Unit-test resolver/discovery using temp trees plus injected cwd/home for bare, partial, and deep `~`, rooted/absolute, `.`, and `..` forms; preserve typed namespaces, rank leaf names, offset highlights, include direct children only, and mark directories.
   - Use a counting fake discovery backend to assert one read per explicit refresh/advance and zero reads of grandchildren. Add a constructor/spawner seam to assert initially explicit sessions do not start a project walker, entering explicit mode cancels it, and returning to ordinary mode starts a fresh one. Add a race regression that enters explicit mode while the old project walker is still active, drains its completion/disconnect signals, and proves they neither overwrite explicit candidates nor close the popup before a fresh ordinary walker is created on transition back.
   - Test missing parent, file-as-parent, failed read, failed entry, and absent home as empty active states, including recovery after editing to a valid query.
   - In `maki-ui/src/app/tests.rs`, extend `completion_app` fixtures with sibling and external temp directories. Verify `@../…` and absolute completion find paths outside the project index.
   - Exercise Enter and Tab across directory advancement and file finalization. Assert each directory step keeps the popup active with a trailing separator, final file selection closes it, and command/argument completion synchronization still occurs.
   - Add a combined quoted-path scenario with spaces and trailing punctuation in directory/file names: completion navigates while the quote is open, final insertion closes the quote, `parse_at_tokens` returns the literal path value, and input highlighting recognizes the existing file.
   - Verify switching explicit → ordinary restores project matches and Lua candidates, while explicit mode never leaks those candidates.

9. Update canonical documentation.
   - Extend `site/docs/content/references/_index.md` syntax guidance with unquoted trailing-punctuation behavior, single/double quoted values, automatic completion quoting, unquoted-whitespace popup closure, and examples of `@~/…`, `@/…`, and `@../…` directory browsing.
   - Update source doc comments for `ItemSpec`, `parse_at_tokens`, and completion source examples so generated Lua API documentation describes the actual insertion contract. Regenerate docs with the repository command rather than editing generated text independently.

10. Validate from focused to workspace-wide gates.
   - Run formatting first (`cargo fmt --check` or the repository formatting target).
   - Run focused `maki-lua` completion tests, focused `maki-ui` file-completion/input/app tests, then `cargo check -p maki-lua --tests` and `cargo check -p maki-ui --tests`.
   - Run `just gen-docs-check` after regeneration.
   - Run `just lint` and `just test` before completion.

### Acceptance Criteria

- **AC.1:** Unquoted `@` references exclude the specified ASCII/full-width trailing sentence punctuation from their values and ranges, while punctuation in the middle remains literal and surrounding punctuation survives expansion unchanged.
- **AC.2:** Single- and double-quoted file/tagged references accept whitespace and normally stripped punctuation; parsed values exclude matching delimiters, ranges include them, and malformed/unterminated quotes are not misparsed as complete references.
- **AC.3:** Completion automatically emits a syntactically valid quoted insertion when a file or Lua candidate value requires it, preserves intentional trailing delimiters, and leaves safe insertions unchanged.
- **AC.4:** An unquoted whitespace ends the active completion token and closes the popup; an unfinished quoted token remains active across spaces, and parsing, completion range extraction, expansion, and highlighting agree on token boundaries.
- **AC.5:** Queries whose file value begins with `~`, `/`, or `.` discover matching immediate children outside the cwd project index for home, rooted/absolute, current-directory, and parent-directory contexts.
- **AC.6:** Each explicit refresh/advance performs exactly one parent-directory read, never recursively reads descendants, and does not start or retain the project background walker while explicit mode is active.
- **AC.7:** Explicit candidates preserve the typed path namespace, use existing fuzzy ranking/highlighting for the unfinished leaf, and visibly distinguish directories with a trailing platform separator.
- **AC.8:** Enter or Tab on an explicit directory advances one level and keeps a refreshed popup active; final file selection safely closes any quote, inserts the token, synchronizes other completion state, and closes the popup.
- **AC.9:** Missing/unreadable parents, failed entries, and unavailable home yield an empty recoverable explicit session without panic, stale project/Lua matches, or a walker race.
- **AC.10:** Ordinary project-relative completion, Lua candidates, ranking, navigation, insertion, expansion, and highlighting retain their prior behavior, including after switching back from explicit mode.
- **AC.11:** The references documentation and generated Lua API reference describe punctuation stripping, quoting, automatic insertion quoting, whitespace closure, and explicit-path browsing.

### Test Strategy

| Acceptance criterion | Named regression coverage |
|---|---|
| AC.1 | `parse_unquoted_trailing_punctuation` (`#[test_case]` for every ASCII/full-width mark), `parse_preserves_internal_punctuation`, and `expansion_preserves_surrounding_punctuation` in `maki-lua/src/api/completion.rs` |
| AC.2 | `parse_quoted_reference_cases`, `parse_rejects_incomplete_or_mismatched_quotes`, `quoted_expansion_receives_inner_value`, and quoted-file pass-through tests in `maki-lua/src/api/completion.rs` |
| AC.3 | `completion_replacement_quotes_unsafe_values` (`#[test_case]` for file/tagged/space/punctuation/delimiter cases), `completion_replacement_preserves_trailing_delimiter`, and `safe_replacement_is_unchanged` in `maki-ui/src/components/file_completion.rs` |
| AC.4 | `active_at_token_quote_and_whitespace_cases` in `file_completion.rs`; `at_token_spans_quoted_and_punctuated_cases` in `input.rs`; app regression `space_closes_unquoted_completion_but_not_quoted_completion` |
| AC.5 | `explicit_path_resolution_cases` with injected cwd/home plus app tests `parent_path_completion_finds_sibling` and `absolute_path_completion_finds_external_file` |
| AC.6 | `explicit_discovery_reads_parent_once` with a counting fake; `explicit_mode_project_walker_lifecycle` with an injected walker spawner/cancel observer; `explicit_mode_ignores_stale_walker_signals`; `explicit_discovery_is_depth_one` |
| AC.7 | `explicit_candidates_preserve_namespace_rank_leaf_and_offset_highlights` over home/root/current/parent forms, including cfg-gated Unix/Windows root semantics and typed-versus-generated separator behavior |
| AC.8 | `explicit_directory_selection_advances_then_file_selection_finishes` app scenario using a `#[test_case]` for Enter/Tab, plus `quoted_explicit_path_advances_then_closes_on_file` |
| AC.9 | `explicit_discovery_failures_are_recoverable` with injected read/entry failures, missing/non-directory parent, and absent home; `explicit_failure_clears_stale_candidates` |
| AC.10 | Existing completion/parser/input test suites plus `switching_from_explicit_to_project_restores_project_and_lua_matches`, existing `enter_inserts_file_verbatim`, and existing tagged-reference expansion tests |
| AC.11 | `just gen-docs-check` and a focused docs/source assertion if the doc generator already supports snapshot/content checks; otherwise the generated-doc consistency command is the executable regression check |

Use `tempfile::TempDir` and injected filesystem/walker seams. Do not depend on real root/home contents or portable permission denial. Simulate read failures through the discovery seam. Existing tests use short convergence loops with deadlines; do not add sleeps.

### Review Strategy

Before handoff, run a fresh `plan-reviewer` pass on this combined #32/#52 plan. Resolve or rebut every critical/high finding and repeat review until none remain.

After implementation and all automated checks, dispatch `nat-code-reviewer` to review correctness, parser edge cases, path/platform behavior, state races, plugin compatibility, readability, security, and performance. Fix or explicitly rebut all findings and repeat if any critical finding remains.

### Documentation Strategy

Documentation is required because #32 adds user-visible syntax and changes the completion-source insertion contract, while #52 adds a new browsing workflow. Update the canonical references page and the Rust doc comments that feed generated Lua API documentation, then regenerate/check generated docs. Do not create a second competing syntax page.

### Risks, Blockers, and Required Decisions

- Parser/completion divergence is the main risk. Keep punctuation classification and insertion quoting in shared helpers where crate boundaries permit; where they cannot share implementation, enforce parity with identical table-driven cases.
- Quoted directory browsing needs two states: an unfinished replacement during advancement and a closed replacement at final selection. Model this explicitly rather than trimming/adding quotes heuristically in `app/mod.rs`.
- Plugin insertions may intentionally end in whitespace. Separate that delimiter before quoting and restore it after the closing quote so model/subagent behavior remains compatible.
- Synchronous `read_dir` can block on a slow filesystem, but #52 explicitly requests bounded synchronous depth-one discovery. One call per refresh is enforced through an injectable counting test.
- The current menu unconditionally starts a walker and derives visibility from its injector. The implementation must make project discovery optional and explicit visibility first-class; otherwise explicit queries race or never render.
- Windows rooted-path and separator behavior differs from Unix. Use `Path`/`PathBuf`/`MAIN_SEPARATOR`, test platform-independent temp absolute paths, and avoid asserting real `/` contents.
- Embedded delimiters and backslashes must round-trip through the minimal quoted escape grammar. Prefer the alternate delimiter to reduce escaping, then escape the selected delimiter and backslashes; table-test values containing either delimiter, both delimiters, backslashes, and combinations so completion never emits an invalid token.
- No unresolved product decision or test-infrastructure blocker remains; the required discovery and walker seams are part of this implementation.