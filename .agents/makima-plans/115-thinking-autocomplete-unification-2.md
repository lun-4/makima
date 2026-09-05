## Goal

Move `/thinking` command ownership entirely to the bundled Lua thinking plugin and remove the separate `/thinking-selector` command plus all Rust built-in thinking command dispatch/completion plumbing. `/thinking <setting>` sets and persists the setting, while bare `/thinking` opens the host-driven selector.

## Implementation Summary

The bundled plugin in `plugins/thinking/init.lua` becomes the sole command registration, with optional arity and Lua-native completion populated by `maki.session.thinking().options`. The existing Lua session API and UI event-loop handlers remain the boundary for reading options, validating settings, updating focused-session state, and persisting defaults.

Remove `/thinking` from `maki-commands` built-ins and delete its associated `BuiltinId`, operation, host-context variants, agent parsing, UI command-host handling, and Rust completion source. This intentionally means `/thinking` is TUI-only and unavailable when plugins are disabled; ACP/headless behavior is unchanged in practice because the former Rust command already required interactive UI capability. Numeric budgets remain accepted through `maki.session.set_thinking`, but finite completion and selector entries come only from host-provided options.

Affected areas are `plugins/thinking`, `maki-commands`, `maki-agent`, `maki-ui`, `maki-lua` tests, and generated command documentation. The canonical thinking domain, provider behavior, persistence representation, and non-command model/session APIs are out of scope and must remain unchanged.

## Implementation Plan

### Phase 1: Make the bundled Lua plugin the sole command owner

1. Update `plugins/thinking/init.lua` to register `/thinking`, remove `/thinking-selector`, use `nargs = "?"`, restore the `[effort]` argument hint and command description, and retain `tui_only = true`.
2. Keep one handler with two explicit paths:
   - Trimmed non-empty arguments call `maki.session.set_thinking({ mode = args, set_default = true })`.
   - Empty arguments call the existing interactive selector.
3. Add `completion.get_items` to the Lua command. It must call `maki.session.thinking()`, return no candidates on host error, unsupported models, or empty options, and map each host option in order to a completion item whose label and insertion equal that option. Completion must not flash errors while the user types and must not contain a fallback/hardcoded option ladder.
4. Continue sourcing selector rendering, initial selection, navigation, and selected values exclusively from `info.options`. Preserve the selector's explicit user-facing errors for unavailable thinking, unsupported models, and an empty host option list.

### Phase 2: Remove Rust command ownership

1. In `maki-commands/src/spec.rs`, remove `BuiltinId::Thinking`, `CompletionKey::Thinking`, `BuiltinOperation::SetThinking`, `HostContextRequest::ThinkingConfig`, `HostContextResponse::ThinkingConfig`, and the `/thinking` entry in `BUILTIN_COMMANDS`. Remove imports that existed only for those variants.
2. Remove the `maki_commands::ThinkingConfig` compatibility re-export from `maki-commands/src/lib.rs` if no remaining command API or test uses it. Keep canonical `maki-domain` types and all non-command consumers intact.
3. In `maki-agent/src/command.rs`, remove the headless unavailable-thinking context branch, local `parse_thinking`, built-in thinking dispatch, `StandardCompletions.thinking`, and the `CompletionKey::Thinking` registration branch. Update exhaustive matches and test host fixtures accordingly.
4. In `maki-ui/src/app/mod.rs`, remove command-host handling for thinking context and `BuiltinOperation::SetThinking`. Retain `App::set_thinking`, model thinking state, and all paths used by Lua session/model requests.
5. In `maki-ui/src/components/arg_completion.rs`, remove `ThinkingArgSource`, its domain imports, and its Rust completion test. In `maki-ui/src/command_runtime.rs`, remove construction and registration of that completion source.
6. Search for all removed variants/types to update exhaustive matches and prove no residual Rust `/thinking` command path remains. Add a regression scan over `maki-commands/src/spec.rs`, `maki-commands/src/lib.rs`, `maki-agent/src/command.rs`, `maki-ui/src/app/mod.rs`, `maki-ui/src/command_runtime.rs`, and `maki-ui/src/components/arg_completion.rs`; forbid `BuiltinId::Thinking`, `CompletionKey::Thinking`, `BuiltinOperation::SetThinking`, `HostContextRequest::ThinkingConfig`, `HostContextResponse::ThinkingConfig`, `ThinkingArgSource`, `parse_thinking`, and a built-in `name: "/thinking"`/equivalent metadata entry. Do not introduce a fallback built-in: with plugins disabled, `/thinking` should resolve as ordinary prompt text just like any unavailable command.

### Phase 3: Add Lua-owned behavior and completion coverage

1. Update the bundled-plugin command registration expectation in `maki-lua/tests/plugin_host.rs` to assert a single `/thinking` Lua command with optional arguments, `[effort]`, and TUI-only metadata, and assert `/thinking-selector` is absent.
2. Add a direct-argument scenario that executes the bundled `/thinking` handler with surrounding whitespace in its argument (for example `"  high  "`), services the resulting `UiAction::Session`, and asserts `SessionRequest::SetThinking { thinking: "high", set_default: true }`. Reply successfully and assert the command completes and emits the expected success flash.
3. Add a completion scenario using the existing Lua command completion bridge. Service `SessionRequest::GetThinking` with a deliberately distinctive/reordered options array and assert completion labels and insertions match it exactly and in order. Add unsupported, host-error, and empty-options cases that return no candidates, assert no numeric budget is invented, and assert these completion failures emit no `UiAction::Flash`.
4. Add a bare-command selector scenario using existing `UiAction::Session` and `UiAction::OpenWin` channels: return a distinctive/reordered host option list, inspect the initial rendered buffer, send navigation and Enter through `event_tx`, observe window closure, and assert the selected host value is sent through `SessionRequest::SetThinking` with `set_default = true`. This exercises the real bundled plugin without hardcoded options.
5. Add a target-capability scenario with the thinking plugin loaded: resolve `/thinking` for an interactive TUI target, and verify it is unresolved and dispatched as literal input for a non-interactive ACP/headless target. Separately verify standard command registration without plugins leaves `/thinking` unresolved for both target types.
6. Retain the existing Lua API response-shape test for `{ mode, supports_thinking, options }`, strengthening it only if needed by the new scenarios. No new generic harness should be added unless the existing `UiAction::Session`/`OpenWin` facilities cannot drive these tests.
7. Add minimal `maki-ui` event-loop test infrastructure that constructs a real event-loop instance with a focused runtime and temporary `StateDir`, then delivers `UiAction::Session { req: SessionRequest::SetThinking { ... } }` through the same action-dispatch entry used in production. Prove the harness reaches `handle_session_request` by awaiting the real reply channel. Add `set_thinking_session_request_updates_state_and_prefs` with `set_default = true`; assert the focused session's typed thinking state changes, the reply contains the resulting mode, the status feedback names that mode, and re-reading prefs from disk yields the same `default_thinking`. A helper-level unit test may supplement this scenario but must not replace delivery through the actual event-loop action and request match arms.

### Phase 4: Reconcile generated documentation

1. In `maki-docgen/src/gen_commands.rs`, remove the special filter that hides plugin `/thinking`, allowing the sole Lua registration to appear under bundled plugin commands. The built-in table must no longer contain `/thinking`.
2. Update command prose to state that bare `/thinking` opens the selector, an optional setting applies and persists the default, finite autocomplete comes from the host, and positive numeric budgets remain accepted even though they are not suggested.
3. Strengthen the docgen test to assert exactly one `/thinking` command row, located in the bundled-plugin section; no `/thinking-selector` row; and no `/thinking` built-in row.
4. Regenerate `site/docs/content/commands/_index.md` through the repository's docgen command. Update Lua API generated docs only if source API wording changes; do not manually maintain generated output independently of its source.

### Phase 5: Validate and review

1. Run formatting, then focused checks/tests for `maki-commands`, `maki-agent`, `maki-ui`, `maki-lua`, and `maki-docgen`.
2. Run `just check`, `just lint`, `just test`, `just gen-docs-check`, and `git diff --check`. If the known unrelated provider catalog test `providers::catalog::tests::model_is_free_uses_catalog_definition::free_opencode_model_is_free` still fails unchanged, report it separately with evidence rather than weakening this change's tests.
3. Dispatch a `general` implementation reviewer after automated validation. Fix or explicitly rebut findings, repeating review if any critical findings remain.

## Acceptance Criteria

- **AC.1**: The effective TUI command registry contains one bundled-plugin `/thinking` command with optional arity and no `/thinking-selector`; `BUILTIN_COMMANDS` contains no `/thinking`.
- **AC.2**: `/thinking <setting>` routes through Lua to `SessionRequest::SetThinking` with the exact trimmed setting and `set_default = true`, and successful completion produces the current success feedback.
- **AC.3**: Bare `/thinking` opens the selector, renders and navigates a host-supplied option order, and applies the selected host value with `set_default = true`.
- **AC.4**: `/thinking` autocomplete is Lua-owned, returns exactly the ordered finite options supplied by `maki.session.thinking()`, invents no numeric budgets or fallback values, and returns no items or flash messages for unsupported, failed, or empty host responses.
- **AC.5**: No Rust built-in thinking command behavior, operation, host context, or completion source remains. The real UI event-loop `SetThinking` path still updates focused-session state, returns and flashes the resulting mode, and persists `Prefs.default_thinking` when requested.
- **AC.6**: With the Lua plugin loaded, `/thinking` resolves only for an interactive TUI target and remains literal input for ACP/headless. With plugins disabled, it remains literal input for all targets, matching sole Lua ownership.
- **AC.7**: Generated command documentation contains exactly one `/thinking` row under bundled plugin commands, no `/thinking-selector`, and accurately describes bare selector, direct setting, persistence, completion, and numeric-budget behavior.
- **AC.8**: Workspace formatting, compilation, linting, relevant tests, generated-doc checks, and diff hygiene pass, apart from any explicitly evidenced pre-existing unrelated failure.

## Test Strategy

| Acceptance criterion | Regression test or check |
|---|---|
| AC.1 | Update `maki-lua/tests/plugin_host.rs` bundled command registry test to assert `/thinking` metadata and absence of `/thinking-selector`; add/update a `maki-commands` metadata test asserting `BUILTIN_COMMANDS` has no `/thinking`. |
| AC.2 | Add `thinking_command_argument_sets_default_through_session_request` in `maki-lua/tests/plugin_host.rs`, executing the real bundled plugin with `"  high  "` and asserting the exact trimmed `SessionRequest::SetThinking` plus success flash. |
| AC.3 | Add `bare_thinking_command_uses_host_options` in `maki-lua/tests/plugin_host.rs`, driving `UiAction::OpenWin.event_tx` and asserting rendered order/navigation and the resulting set request. |
| AC.4 | Add parameterized Lua completion tests `thinking_completion_uses_host_options` and `thinking_completion_is_empty_without_flashing_when_unavailable`, using a reordered list plus unsupported/error/empty cases, exact item assertions, and an assertion that the UI action channel receives no flash. |
| AC.5 | Add or update `maki-commands/tests/thinking_architecture.rs::thinking_command_is_lua_owned` to scan the six files and forbidden symbols listed in Phase 2.6; add minimal real event-loop action-dispatch harness plus `maki-ui` test `set_thinking_session_request_updates_state_and_prefs`, delivering `UiAction::Session` through production dispatch and asserting reply completion, focused state mutation, feedback, and prefs read back from temporary storage. |
| AC.6 | Add target-scoped registry tests `thinking_plugin_is_tui_only` with the plugin loaded and `thinking_is_unresolved_without_plugins` with only standard commands, asserting interactive versus non-interactive resolution and literal-input fallback directly. |
| AC.7 | Update `maki-docgen/src/gen_commands.rs::doc_projection_separates_builtins_and_bundled_plugins` to assert section placement, exactly one row, absence of `/thinking-selector`, and expected prose; run `just gen-docs-check`. |
| AC.8 | Run `cargo fmt --all --check`, focused crate tests/checks, `just check`, `just lint`, `just test`, `just gen-docs-check`, and `git diff --check`. |

The existing Lua test API exposes `UiAction::Session` reply channels and `UiAction::OpenWin` with a window event sender, so the command, completion, and selector behaviors can be exercised without adding a new framework. If implementation reveals a concrete missing seam, add the smallest test-only helper around these existing channels and test that helper as part of the same scenario.

## Review Strategy

Use the `plan_reviewer` before handoff and fix or explicitly rebut every finding. If any critical or high finding is returned, rerun plan review after resolving it. After implementation and automated testing, dispatch a `general` reviewer focused on sole command ownership, Lua coroutine/session interactions, completion failure behavior, and removal of stale Rust paths. Fix or explicitly rebut every implementation finding; after any critical finding, rerun review until no critical findings remain or execution is blocked for operator input.

## Documentation Strategy

Update the command generator and regenerate the checked-in command reference. The Lua session API contract already exposes `options`; adjust its source docs and regenerated Lua API reference only if needed to clarify that the ordered finite list drives both selector and completion. No architecture or `AGENTS.md` update is needed because this follows the existing bundled-plugin command model.

## Risks, Blockers, and Required Decisions

- Removing the built-in intentionally removes `/thinking` when plugins are disabled. This follows the request for entire Lua-side ownership; do not silently retain a Rust fallback.
- Lua completion performs a session round trip while typing. Existing completion timeout/cancellation handling must remain effective, and failures should produce an empty candidate set without flashing.
- The bare command parks in `win:recv`; tests must always close or confirm the window to avoid leaked command coroutines.
- Command removal must not remove `maki.session.thinking`, `maki.session.set_thinking`, `App::set_thinking`, event-loop `SessionRequest` handling, canonical domain types, provider translation, or persistence logic.
- Numeric budgets are intentionally parseable direct arguments but absent from the finite host option list, selector, and completion suggestions.
- No unresolved blocker remains. The user decision is explicit: Lua is the sole owner, not an override layered over a Rust fallback.
