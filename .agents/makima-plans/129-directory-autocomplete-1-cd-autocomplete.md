## Goal

Implement issue #129: offer the existing prompt path autocomplete for builtin `/cd`, restricted to directories, resolving paths against the active session's working directory.

## Implementation Summary

Reuse `maki-ui/src/components/file_completion.rs` and its existing project walker/explicit filesystem discovery. Add a directory-command mode rather than another completion provider or walker. Integrate ownership, insertion, and rendering in `maki-ui/src/app/mod.rs`, `app/view.rs`, and `components/command.rs`; expose builtin identity through `command_runtime.rs`. Adjust builtin Cd arity in `maki-commands/src/spec.rs` to permit raw paths containing spaces.

Implemented against `c98ab8f7`. The final read-only code review approved the implementation after the fixes recorded below. No dependencies were added; the implementation uses existing nucleo, ignore, tempfile, and test-case dependencies.

Scope excludes a general command-completion API redesign, shell quoting, changes to `@` semantics, and filesystem platform support beyond the existing completion resolver.

## Implementation Plan

### 1. Add directory-only mode to the shared popup

- Add an explicit session completion mode distinguishing references from directory commands. Preserve existing callers/default reference behavior.
- Directory mode has no Lua candidates and treats query text literally, bypassing reference-kind parsing (`parse_query`), including names such as `model`, `skill`, and colon-containing names.
- Reuse project fuzzy discovery for empty/bare queries. Project walker entries already distinguish directories by their trailing platform separator (`components/file_picker.rs`, walker). Exclude the walker root, which is represented by a lone platform separator and must not be inserted as an absolute filesystem root. Filter borrowed directory entries before allocating paths, retaining at most 640 owned paths while counting all eligible matches. Reference mode reads the snapshot count directly and visits at most 640 entries. Preserve ordering, cancellation, stale-query protection, and truncation behavior.
- Reuse explicit filesystem discovery for queries starting with `~`, `/`, or `.`, as today. In directory mode also use explicit discovery for relative paths containing a separator, so `foo/` lists children after descent. Keep this new routing specific to directory mode.
- Explicit discovery includes only entries identified as directories by the existing resolver, retaining directory symlink support and existing empty/error handling. Do not change project ignore rules or symlink traversal.
- Share the discovery-selection predicate between opening and synchronization. Reopen when mode or cwd changes so stale roots/results cannot leak between contexts. Successful cwd mutation refreshes an active popup immediately; an accepted or dismissed popup stays closed.
- Directory mode: Tab advances the highlighted directory with a trailing separator and refreshes children; Enter accepts its raw path and closes the popup, without executing. A subsequent Enter executes via normal command dispatch. Escape closes the popup without changing input. With no selectable current result, Enter falls through to execution; Tab is consumed so command-name completion cannot reset the path. Existing reference-mode Enter/Tab behavior stays unchanged.

### 2. Integrate command context, keyboard ownership, and raw path insertion

- Add a small command-palette query exposing the selected resolved command as needed. Recognize only the exact command word followed by whitespace, not fuzzy `/c` or `/cdfoo` matches. `/cd` without a separator retains normal command completion; completing its name with Tab must open directory suggestions.
- Verify builtin identity using both `ResolvedCommand::producer_id()` against `StandardCommands::builtin_producer().id()` and `command.spec().name == "/cd"`. Expose a narrow helper from `command_runtime.rs`, which currently privately retains `_standard_commands`. Never hijack a plugin override named `/cd`.
- Add a pure directory-command argument-range helper for the first command line. Treat the entire trimmed remainder as one raw path, including internal spaces, Unicode, and `@`. Query only text through the cursor; replace the entire path span, preserving the command prefix and text outside it. Convert cursor character offsets to byte ranges consistently with the input buffer. Do not enable this context when the cursor is in the command name or on another line.
- In `sync_file_completion`, detect directory-command context before reference tokens and allow that context despite an active command palette. Continue suppressing it for overlays and streaming. Skip Lua item collection in directory mode.
- Give the directory popup keyboard priority over the command palette and render it instead of the slash palette. Directory argument ownership is independent of popup visibility: after dismissal or acceptance, Tab preserves the path and slash-command rows remain suppressed while the cursor is in the directory argument. Keep normal slash dispatch available on passthrough Enter and after dismissal/acceptance. Use a shared completion-key handler.
- Directory insertion uses raw path text, without `@`, shell quotes, or an added argument-delimiting space. Keep the trailing directory separator for descent. Ensure acceptance reads the mode before closing the session. Reference insertion continues using the existing replacement/advance helpers.
- Synchronize after command-name Tab completion, typing, paste, cursor moves, and directory advancement. Ctrl movement and normal input use a shared input-action handler. Focused main-chat mouse clicks also synchronize the cursor context. Newline keys in directory context edit the input and synchronize rather than accepting a directory. Do not reopen an accepted/dismissed popup immediately without another input/cursor synchronization event. When directory completion returns a navigation or Tab action, do not let it reach command-name completion.
- Change only builtin Cd's argument arity from OPTIONAL to ANY: dispatch currently counts whitespace-delimited words, but Cd's executor already consumes the entire trimmed remainder as one path and handles empty input as home. Preserve that raw-remainder execution contract; do not introduce quote decoding or a shell parser.

### 3. Add tests and documentation

- Extend existing inline test modules and `maki-ui/src/app/tests.rs`; use `#[test_case]` for parameterized cases. Existing `completion_app`, `write_completion_fixture`, `converge_completion`, `rendered`, and `rendered_rows` provide fixture, deterministic completion convergence, and render infrastructure. Reuse injectable resolver/walker dependencies for discovery edge cases and cap tests; avoid sleeps and global cwd/home mutation.
- Add the named tests below. Exercise real app key dispatch and rendering, not only isolated candidate filtering. Space-path coverage must reach normal command execution and assert session cwd.
- Update documentation as described below, then perform scoped checks/tests, wider lint/tests, and implementation review.

## Acceptance Criteria

- **AC.1:** `/cd ` and partial paths show only directories, including empty directories, with project fuzzy matching and explicit child discovery. Lua references/files never appear and files cannot consume the directory result cap.
- **AC.2:** Tab descends repeatedly, Enter accepts then executes on the next Enter, Escape preserves input, and no-match Tab does not overwrite the path. The visible popup and actual key handling agree.
- **AC.3:** Relative, parent-relative, absolute, and home-relative completion follows existing resolver behavior using session cwd. Internal-space and Unicode paths survive insertion and execute successfully; bare `/cd` still resolves home.
- **AC.4:** Command-name Tab, paste, cursor changes, context changes, and cwd changes synchronize correctly without stale selections. Only exact builtin `/cd` gets directory completion; custom overrides and reference completion keep their own behavior.

## Test Strategy

Tests to add (names may follow surrounding naming conventions, but preserve coverage):

- AC.1 → `cd_completion_filters_directories`: injected project/explicit fixtures containing files, empty/nonempty directories, reference-like names, and Lua candidates. `cd_directory_filter_precedes_cap`: enough higher-ranked files to exceed the cap while valid directories remain. `cd_explicit_directory_symlinks`: resolver or platform-gated symlink fixtures exclude regular/broken symlinks and include directory symlinks.
- AC.2 → `cd_completion_keyboard_and_render`: app scenario asserts directory popup contents and absence of slash rows, Tab child navigation, Enter acceptance without cwd change, next Enter changing cwd. `cd_completion_escape_preserves_input`; `cd_completion_no_matches_preserves_path`: missing paths and empty directories, checking Tab preservation and Enter dispatch/error behavior.
- AC.3 → `cd_completion_path_forms`: parameterized resolver/menu tests for bare, `./`, `../`, absolute, `~/`, and nested relative paths. `cd_completed_space_and_unicode_path_executes`: app scenario in temporary directories asserts exact input and resulting cwd. `cd_raw_path_dispatch`: command tests prove the entire space-containing remainder reaches Cd unchanged and empty input remains valid; extend agent Cd resolution tests with an injected/home-derived expected home rather than changing environment.
- AC.4 → `cd_completion_activation_and_ranges`: unit cases for command boundaries, cursor before/in/after the path, internal spaces, Unicode byte ranges, and second-line exclusion. `cd_completion_command_tab_and_paste`: app scenarios assert visible directory results. `cd_completion_context_and_cwd_switch`: ensure fresh results after switching roots or reference mode. `cd_completion_rejects_stale_selection`: controlled walker/query change test. `cd_completion_does_not_hijack_override`: register a custom `/cd`, assert its own command completion/dispatch behavior and no directory popup. `reference_completion_unchanged_after_cd`: app scenario switching back to `@` and asserting ordinary file/reference insertion and explicit-directory descent. Run existing model/theme command completion tests as additional regression coverage.

Use existing injected dependencies and convergence helpers; no missing test infrastructure is anticipated. Add small fixture helpers within existing test modules if necessary.

Run cheapest first:
1. `cargo check -p maki-ui -p maki-commands --tests` (include maki-agent if its test module changes).
2. `cargo nextest run -p maki-ui -p maki-commands` and targeted maki-agent tests if changed.
3. `cargo fmt --all -- --check`, `just lint`, `just test`, and `just gen-docs-check` after regeneration.
Record failures and distinguish pre-existing/environment failures from regressions.

## Review Strategy

Before handoff, request a read-only plan_reviewer review of this artifact and address findings. Fix or explicitly rebut any critical or high plan-review findings, then repeat plan_reviewer before submission until no critical or high findings remain. After implementation and automated tests, dispatch a general subagent to review the diff, emphasizing popup ownership, stale results, raw-space execution, producer identity, and unchanged `@` behavior. Fix or explicitly rebut all findings; repeat review for critical findings until resolved or blocked for operator decision.

## Documentation Strategy

`site/docs/content/commands/_index.md` is generated by `maki-docgen/src/gen_commands.rs`; do not hand-edit generated output. Per the final user decision, command help does not describe autocomplete keys or behavior. The builtin Cd summary in `maki-commands/src/spec.rs` is "Change working directory. Paths may contain spaces." Regenerate documentation with `just gen-docs`; no generator change is needed. Keep the completion-key/provider metadata truthful: directory completion is UI filesystem completion, not a registered async command-argument provider.

## Risks, Blockers, and Required Decisions

- No unresolved blocker. Existing test infrastructure supports component and app/render verification.
- Command arity counts whitespace words, so OPTIONAL rejects valid space paths and can hide `/cd` from the palette. ANY intentionally permits the whole raw remainder; this is not multiple path arguments. Leading/trailing filename whitespace remains unsupported because dispatch trims arguments. Shell quotes remain literal, matching the existing executor.
- Preserve existing `~`/`~/` completion behavior and the executor's broader leading-tilde expansion; do not expand this issue into tilde semantics or Windows-drive fixes.
- Project discovery respects existing ignore policies; explicit paths continue allowing navigation outside the project and into ignored locations. Filter before materialization without changing walker behavior for other users.
- Enter differs deliberately from reference directory descent: it accepts a directory for `/cd`, allowing users to execute it rather than being forced into its children. Guard this with app scenarios.
- Source locations were checked against the implementation rather than the initial research line numbers.

## Review Fixes and Verification

- Excluded the project walker root from directory candidates without changing reference-mode results.
- Bounded owned path materialization in directory mode and preserved bounded snapshot traversal in reference mode.
- Fixed stale completion after Ctrl cursor movement, focused input clicks, newline insertion, and successful cwd changes.
- Preserved directory argument ownership after Escape and Enter acceptance. Tab no longer reaches command-name replacement or erases the path, and slash rows remain hidden.
- Removed the unnecessary dead-code allowance on the reference-mode `open` method.
- Added component tests for root filtering, materialization counts and truncation, literal reference-like names, stale queries, explicit path forms, and directory symlinks. Added app tests for key/render ownership, repeated Tab descent, dismissal, no matches, command-name Tab, paste, plugin overrides, reference-mode restoration, raw Unicode/space/`@` paths, cwd changes, Ctrl movement, mouse clicks, newlines, and byte ranges.
- Raw-path execution coverage runs through normal app command dispatch. The Cd executor is unchanged; no new agent or command-crate test modules were added. The earlier per-crate test suggestions remain the proposed strategy rather than a claim that every named test was added.
- Final verification passed: `cargo check -p maki-ui -p maki-commands --tests`, scoped UI/commands tests (1,587), `just lint`, `just test` (4,847 workspace tests), `just gen-docs-check`, `cargo fmt --all -- --check`, and `git diff --check`.
- Final read-only code review: approve, with no remaining actionable findings. Review conclusions are based on static inspection; the build and test results above were run separately.
