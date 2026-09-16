## Goal

Fix the apparent freeze when opening the sessions picker without a query, and add behavioral regressions proving the picker initializes, handles closing keys, and can reopen.

## Implementation Summary

Restore the nil-safe query guard in `plugins/sessions/init.lua:453`. Add focused integration/scenario coverage in the existing `maki-lua/tests/plugin_host.rs`, using the bundled sessions plugin, its actual command/keybind handlers, and mocked UI/session channels. No new dependencies or production APIs are required.

The failure is an initialization exception, not a demonstrated lock deadlock. `/sessions` passes an absent optional query as nil; `open()` retains a focused window before `TextInput:insert_text(nil)` errors. The receive loop is never entered, and retained `board.win` prevents automatic closure. Commit `e1575c5e` removed the previously safe guard. Literal `/sessiosn` is not an alias and is outside this fix.

Scope excludes global Lua exception cleanup, command-dispatch refactoring, completion callback deadlocks, and changing text-input's string-only contract.

## Implementation Plan

### 1. Add focused picker scenarios using existing test infrastructure

Extend `maki-lua/tests/plugin_host.rs` with a small local helper for the sessions scenarios, not a general framework:

- Start `test_support::spawn_host_for_tests(&["sessions"])`, which uses the in-memory backend.
- Dispatch the direct command through `host.command_registry()`, a target with `TargetCapabilities::ALL`, and a minimal `CommandHost`, following `maki-lua/tests/typed_invocation_routes.rs:54–105`. Use actual registry dispatch so absent typed arguments follow the production `ExecuteCommand` path.
- Capture `UiAction::OpenWin` including `buf`, `event_tx`, and `cmd_rx`; retain the channels until the scenario closes the picker. Assert title and focus.
- Service `SessionRequest::Live` and `SessionRequest::List` with deterministic fixture responses. Distinguish and assert both requests, rather than accepting any session action as sufficient. Use a matching session and a nonmatching session for filter checks, with valid fixture IDs and complete row fields.
- Observe actual rendered content with a bounded predicate loop: consume `WinCommand::SetCursor`, then inspect `buf.take().text()` (the existing non-destructive snapshot API). `render()` commits `board.buf:set_lines` before SetCursor (`plugins/sessions/init.lua:182–183`), but queued cursor commands can belong to earlier or periodic renders. Continue until the expected content predicate holds, using one deadline. Include a matching stored-only fixture row; load completion requires that row and absence of the loading hint after List and the subsequent Live reply. After Esc, require the cleared input and both fixture titles, rather than assuming the next cursor command represents the key's render.
- Send `WinEvent::Key` through the captured event sender and require `WinCommand::Close`. Close and then dispatch a second opening on the same host, requiring a fresh window, session requests, and another successful closure. This detects retained stale `board` state.
- Use bounded channel waits with the existing `HOST_REPLY_TIMEOUT` and one deadline per wait loop, no sleeps. Ensure any helper UI pump can be explicitly stopped and joined on success or assertion failure; avoid detached blocked test workers. Do not call load barriers while a mocked session reply remains unanswered.
- Parameterize using `#[test_case]`, with shared constants for fixture titles and asserted messages.

Add named scenarios:

1. `sessions_command_initializes_closes_and_reopens`: omitted query and explicitly quoted empty query, each exercised with `esc` and `ctrl+c`. Require populated rendered rows, both load phases, Close, and successful reopening.
2. `sessions_command_query_filters_and_escape_clears`: dispatch a quoted nonempty query, assert the filter text and only matching session title appear. Send Esc and wait for the next render; assert the filter is cleared and both fixture titles appear. A second Esc must close the picker.
3. `sessions_keybind_initializes_and_closes`: resolve the bundled Ctrl+P entry from `host.keymap_reader().load().entries` using its key/modifiers/plugin, invoke `EventHandle::run_keybind_callback(id)`, and require initialization, rendered rows, and closure. Do not replace this with a hand-written call to `open()`.
4. Strengthen `session_picker_requested_routes_through_sessions_command` (`plugin_host.rs:5693`) using the shared scenario helper: retain its assertion that the autocmd emits `/sessions`, route that action through real registry dispatch, reply to the RunCommand roundtrip, then require session loading, rendered rows, and keyboard closure. Preserve `session_picker_requested_autocmd_does_not_wedge_host`.

First run the omitted-query regression against the unfixed plugin and confirm that it fails after OpenWin but before the first Live request. The prior investigation observed exactly this with a temporary extension of the old route test; a nonempty query passed. Existing tests currently pass because they assert only OpenWin and host liveness.

### 2. Fix the query guard

In `plugins/sessions/init.lua`, change `if query ~= "" then` back to `if query and query ~= "" then`. Keep `board.query = query or ""`, the optional typed schema, async loading, and keyboard behavior unchanged. Do not weaken `TextInput:insert_text` to silently accept nil or add unrelated lifecycle machinery.

### 3. Validate and review

Run the checks below cheapest first, then review the final diff. Confirm the new omitted-query scenarios fail with the old guard and pass with the fix. Leave no temporary mutations and do not commit unless asked.

## Acceptance Criteria

- **AC.1:** Argumentless `/sessions` and `/sessions ""` render the available sessions, complete both loading phases, close on Esc or Ctrl+C, and reopen on the same host. Verified by `sessions_command_initializes_closes_and_reopens`.
- **AC.2:** An explicit query is visible and filters the rows; Esc clears it and restores rows, and a second Esc closes the picker. Verified by `sessions_command_query_filters_and_escape_clears`.
- **AC.3:** The bundled Ctrl+P keybind opens an initialized, keyboard-closable picker without arguments. Verified by `sessions_keybind_initializes_and_closes`.
- **AC.4:** `SessionPickerRequested` still routes through `/sessions`, now reaching loaded rendered rows and keyboard closure rather than merely opening a window. Verified by the strengthened `session_picker_requested_routes_through_sessions_command`.

## Test Strategy

All four criteria use integration/scenario tests with the real bundled Lua plugin and observable window actions/buffer output. Pure helper tests or host-liveness assertions alone cannot catch this bug. The small channel-driving helper in step 1 supplies the needed scenario support; no missing external test infrastructure is required.

Explicit mapping: AC.1 → `sessions_command_initializes_closes_and_reopens`; AC.2 → `sessions_command_query_filters_and_escape_clears`; AC.3 → `sessions_keybind_initializes_and_closes`; AC.4 → `session_picker_requested_routes_through_sessions_command`.

Execution checks:

1. `cargo check -p maki-lua --tests`.
2. Run the new omitted-query case with the old guard and record the expected failure, then apply the fix.
3. `cargo test -p maki-lua --test plugin_host sessions_` and `cargo test -p maki-lua --test plugin_host session_picker_requested`.
4. `cargo nextest run -p maki-lua`.
5. `cargo clippy -p maki-lua --tests -- -D warnings`, `cargo fmt --all -- --check`, and `stylua --check plugins/sessions/init.lua`.

An optional interactive smoke check can confirm `/sessions`, Ctrl+P, and startup picker appearance/closing in a fresh process, but is not a substitute for the automated scenarios. Tests mock UI transport and storage responses rather than launching a real terminal or scanning user session files.

## Review Strategy

Before handoff, have a read-only `plan_reviewer` review this plan and resolve its findings. If any high or critical plan findings are returned, fix or explicitly rebut them and repeat plan review before handoff. After implementation and automated checks, dispatch a `general` review subagent to inspect the fix and tests, especially asynchronous ordering, timeout/cleanup behavior, real entry-point coverage, and whether assertions would catch the old nil bug. Fix or explicitly rebut all findings; repeat review after any critical findings until cleared or blocked.

## Documentation Strategy

No documentation changes needed: this restores existing advertised behavior without changing the command schema or public APIs. Existing user docs and repository architecture guidance remain accurate. Do not generate docs or create documentation files for this fix.

## Risks, Blockers, and Required Decisions

- A window-opening assertion alone passes despite the original crash. Tests must observe session requests, rendered content, and a Close command.
- The background List task can still be in flight when a close key is sent. For the core scenarios, finish both loading phases before closing; use channel/render milestones rather than timing assumptions.
- Esc clears a nonempty filter before closing, unlike Ctrl+C. The query scenario must explicitly preserve that behavior.
- Registry dispatch reports completion when the handler is enqueued, not when the picker finishes. Do not use dispatch completion as a readiness or cleanup signal.
- Broad protection against arbitrary future plugin exceptions remains out of scope; the concrete initialization bug is addressed with narrow regression coverage.
- No blockers or operator decisions remain for this scoped fix.
