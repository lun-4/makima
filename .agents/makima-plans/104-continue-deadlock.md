# Fix: `makima --continue` deadlocks (issue #104, sessions picker launched via sync autocmd)

## Goal

Make `makima --continue` (bare) open the sessions picker exactly like `/sessions` does: populated, keyboard-responsive, closable with ctrl-c/Esc. No more wedged plugin host, dead input, or forced SIGKILL.

## Implementation Summary

**Root cause** (from investigation): bare `--continue` sets `session_picker = true` in the TUI (`src/cmd/tui.rs:254-256`, `src/cmd/tui.rs:341`); the UI fires the `SessionPickerRequested` autocmd at startup (`maki-ui/src/event_loop.rs:725-726` → `maki-ui/src/app/mod.rs:2329`). Autocmd callbacks are dispatched **synchronously on the Lua thread** (`maki-lua/src/runtime.rs:3681` → `api/autocmd.rs` `dispatch` → `call_isolated` → `func.call`). The picker's `open()` (`plugins/sessions/init.lua:421`) requires an **async coroutine context**: it suspends on `maki.session.live()`/`list()` UI roundtrips (`maki-lua/src/api/session.rs:61`, `blocking` on `ui_json_roundtrip`) and then parks in `while board do board.win:recv(TICK_MS)` (`init.lua:478`). mlua 0.11 async functions called outside a coroutine/executor poll with a noop waker and cannot suspend, so the host thread wedges inside the dispatch. The window still renders (empty, because `open_win` is a sync fire-and-forget and the refresh/stored-scan never complete) and, holding keyboard focus, swallows ctrl-c/Esc forever (`maki-ui/src/components/lua_float.rs:313`).

**Why `/sessions` and `<C-p>` work**: they run the same `open()` through `Request::RunCommand`/`RunKeybindCallback`, which spawn **deadline-free async tasks** on the host executor (`maki-lua/src/runtime.rs:3521`, `runtime.rs:3781`; `TaskScope::detached` has `deadline: None`, `runtime.rs:946-948`).

**Fix**: keep the UI↔plugin contract (the documented `SessionPickerRequested` event, `site/docs/content/lua-api/_index.md:689`) and the UI fire site unchanged. Change the plugin's autocmd registration (`plugins/sessions/init.lua:541`) so the callback **defers into the command seam** instead of calling `open()` synchronously:

```lua
maki.api.create_autocmd("SessionPickerRequested", { callback = function()
  maki.async.run(function() maki.api.run_command("/sessions") end)
end })
```

- `maki.async.run` is a synchronous API (`maki-lua/src/api/async.rs:95`) that just queues a task and returns, so the sync autocmd dispatch completes immediately — no wedge (the callback never touches async APIs).
- The queued wrapper task calls `maki.api.run_command("/sessions")` (`api/tool.rs:894`), which roundtrips `UiAction::RunCommand` to the main loop, which dispatches the real `/sessions` command (`maki-ui/src/event_loop.rs:1160-1176` → `run_cmdline`) → `Request::ExecuteCommand` → deadline-free async task → `open()` runs as a proper coroutine. This is byte-for-byte the user-typing-`/sessions` path.
- The `maki.async.run` wrapper only lives as long as the `run_command` resolution roundtrip (milliseconds); the 60s `ASYNC_RUN_DEFAULT_DEADLINE` (`runtime.rs:117`) never touches the open picker's `recv` loop. This is why we cannot just do `maki.async.run(open)` — an open picker would be abandoned after 60s.

**Why not other options (rejected)**:
- UI-side `run_cmdline("/sessions", 0)` in Rust: hardcodes the plugin command name in maki-ui; the repo recently removed hard-coded command identities (`10f2eca5 refactor(ui): remove hard-coded command identities`). The event contract keeps the UI decoupled.
- Making autocmd dispatch async generally: breaks the documented and tested "every matching autocmd callback runs synchronously before this function returns" contract (`api/autocmd.rs:199-201`, tests in `tests/events_slots.rs`) and the `TurnEnd` GC + flash ordering in the host request loop. Out of scope.

**Touched files**: `plugins/sessions/init.lua` (fix), `maki-lua/tests/plugin_host.rs` (regression tests), `maki-lua/src/api/autocmd.rs` (docs note in the events doc comment; regenerated into `site/docs/content/lua-api/_index.md` via `just gen-docs`), `maki-lua/src/api/AGENTS.md` (seam guidance, one line).

## Implementation Plan

### Phase 1 — Regression tests first (both are mandatory; neither is `#[ignore]`d)

Two tests in `maki-lua/tests/plugin_host.rs`, using the existing `test_support::spawn_host_for_tests(&["sessions"])` harness (boots a real `PluginHost` pre-loaded with the sessions plugin). The host always boots with an internal `ui_action_tx` that nothing consumes by default (`runtime.rs:3279`), so unanswered roundtrips dangle exactly like a wedged UI — that is what reproduces the bug under a bare host.

**Test 1 — the wedge guard** (fails on pre-fix code; guards the reported deadlock):

```rust
#[test]
fn session_picker_requested_autocmd_does_not_wedge_host() {
    let (_handle, guard) = maki_lua::test_support::spawn_host_for_tests(&["sessions"]);
    // Firing the event must return promptly instead of suspending the dispatch.
    with_timeout(|| {
        guard
            .host()
            .load_source("fire", "maki.api.exec_autocmds('SessionPickerRequested')")
            .expect("firing SessionPickerRequested must not wedge the host");
    });
    // The host request loop must still serve work right after the fire.
    with_timeout(|| {
        guard
            .host()
            .load_source("probe", "local still_alive = true")
            .expect("host must stay responsive after SessionPickerRequested");
    });
}
```

**Test 2 — the seam is actually taken** (fails if the fix ever degrades to a silent no-op). The test owns the host's UI-action channel and answers it with a stub responder that **also plays the UI's role in command dispatch**, so the full post-fix chain runs end-to-end:

- `let ui_rx = guard.host().ui_action_rx();` (`loader.rs:565`).
- Spawn a stub responder thread that reads `ui_rx` until disconnect and handles:
  - `UiAction::Session { reply_tx, .. }` → `reply_tx.send(Ok(serde_json::json!([])))`;
  - `UiAction::OpenWin { config, .. }` → record `config.title` (an `Option<String>`; later assert `config.title.as_deref() == Some(" Sessions ")`);
  - `UiAction::RunCommand { cmdline, depth, reply_tx }` → record the `cmdline`, then **dispatch the command into the host the way the real UI does** so `open()` actually runs: call the host's `run_command_for_test("sessions", "/sessions", String::new(), depth)` on the test handle (see `loader.rs:770`; if the helper's signature differs, use `guard.host().command_registry()` (`loader.rs:549`) bound to the existing `FakeCommandHost` at `tests/plugin_host.rs:27` and invoke the resolved behavior), then `reply_tx.send(Ok(()))`.
  - Other variants are unreachable in this flow; `_ => {}`.
- Asserts, each within the deadline helper:
  1. A `RunCommand` with `cmdline == "/sessions"` was observed (routed through the command seam, not a direct call — this is what discriminates a silent no-op).
  2. An `OpenWin` with `config.title.as_deref() == Some(" Sessions ")` arrived (the picker opened end-to-end through the command task).
  3. After the fire, the host serves a fresh `load_source("probe", ...)` within the deadline (responsive).
- Recordings surface via a `flume::bounded(1)` channel or an `Arc<Mutex<Vec<_>>>` polled with `recv_timeout` — never a fixed sleep.

Notes for the implementer:
- **Timeout mechanics**: `load_source` blocks the calling thread synchronously on its reply (`loader.rs` `send_load` → `reply_rx.recv()`), so `smol::block_on(futures_lite::future::or(load, Timer::after(...)))` cannot time it out — while the Lua thread is wedged, the `Timer` future is never polled and the test just hangs until the runner's own timeout. Run every host roundtrip on a helper thread and wait on a `flume::bounded(1)` result channel with `recv_timeout(Duration::from_secs(5))`, panicking with a constant message (`TEST_WAKE_TIMEOUT_MSG` style, pattern at `maki-lua/src/runtime.rs:1189` and the `recv_timeout` use in `tests/plugin_host.rs`) on timeout — a deterministic panic beats a silent CI hang.
- **Validate both tests against pre-fix code first**: temporarily revert `init.lua:541` to `{ callback = open }`, run, confirm the pair fails (Test 1 hangs/timeouts in the wedge variant; Test 2's assertion 1 fails in the error variant, because pre-fix `open()` runs directly and never produces a `RunCommand`); then apply Phase 2 and confirm both pass. The two tests together guard both mlua failure variants, so no separate branch is needed. If neither test fails pre-fix, stop and re-examine the harness before shipping — never ship a vacuous test.

### Phase 2 — Apply the plugin fix

In `plugins/sessions/init.lua`, replace line 541:

```lua
maki.api.create_autocmd("SessionPickerRequested", { callback = open })
```

with a callback that defers into the command seam:

```lua
-- Autocmd callbacks run synchronously on the Lua thread; open() suspends
-- (ui roundtrips, win:recv), so it must run as a coroutine. Hop through the
-- /sessions command, which the host runs as a deadline-free async task. The
-- maki.async.run wrapper lives only as long as the run_command roundtrip,
-- so its abandon deadline never touches the open picker.
maki.api.create_autocmd("SessionPickerRequested", { callback = function()
  maki.async.run(function()
    local _, err = maki.api.run_command("/sessions")
    if err then
      maki.ui.flash(err)
    end
  end)
end })
```

The `open()` guard (`if board then return end`, `init.lua:422-424`) still prevents double-opens from `--continue` racing `<C-p>`/`/sessions`. No other plugin changes; `/sessions` (line 532) and `<C-p>` (line 542) registrations stay as-is.

### Phase 3 — Docs

1. The lua-api events list is **generated by maki-docgen**: `site/docs/content/lua-api/_index.md` is written from doc comments and checked byte-for-byte by `just gen-docs-check`; the event names live in the doc comment at `maki-lua/src/api/autocmd.rs:127-143` (the `"SessionPickerRequested"` entry is line 132). Add the note there — e.g. that the event now opens the `/sessions` picker asynchronously via the command seam, so the picker is not open synchronously when the event returns. Then run `just gen-docs` and confirm the note lands in `site/docs/content/lua-api/_index.md` (~line 689). Do **not** edit the generated file by hand: `just gen-docs` overwrites hand edits and `just gen-docs-check` fails on them.
2. `maki-lua/src/api/AGENTS.md`: one line under a "seams" note — autocmd callbacks run synchronously on the Lua thread and must never suspend (UI roundtrips, `win:recv`); defer suspending work via `maki.async.run` or a command/keymap handler.

### Phase 4 — Validation

- `just check` (or `cargo check -p maki-lua --tests -p maki-ui --tests`).
- `just lint` (`cargo clippy --all --tests -- -D warnings`).
- `cargo nextest run -p maki-lua -p maki-ui` (full workspace if time permits).
- Manual TUI pass (below).

## Acceptance Criteria

- **AC.1** — Regression tests in `maki-lua/tests/plugin_host.rs`: `session_picker_requested_autocmd_does_not_wedge_host` and `session_picker_requested_routes_through_sessions_command` (mandatory pair). At least one of the pair **fails on pre-fix code** (verified by temporarily reverting `init.lua:541` during Phase 1; in the wedge variant Test 1 hangs/timeouts, in the error variant Test 2's `RunCommand` assertion fails) and both pass post-fix; neither test is `#[ignore]`d or otherwise skipped in the final diff.
- **AC.2** — Manual TUI check (no automated TUI integration harness; see Test Strategy): `makima -c` (bare) opens a populated sessions picker; ctrl-c and Esc close it; Enter on a row focuses that session; `/sessions` and `<C-p>` still open the picker; `makima -c <id>` and `makima -l` are unaffected.
- **AC.3** — Workspace checks green: `just check`, `just lint`, `cargo nextest run -p maki-lua -p maki-ui`.
- **AC.4** — Docs: the `SessionPickerRequested` note is added to the source doc comment (`maki-lua/src/api/autocmd.rs` events list), `just gen-docs` is run, the note is present in `site/docs/content/lua-api/_index.md`, and `just gen-docs-check` passes.

## Test Strategy

| Criterion | Test | Layer |
|---|---|---|
| AC.1 | `session_picker_requested_autocmd_does_not_wedge_host` + `session_picker_requested_routes_through_sessions_command` (both new, mandatory) | integration, real `PluginHost` + real sessions plugin + stub UI responder (`ui_action_rx`) |
| AC.2 | manual TUI checklist (populated list, ctrl-c/Esc close, Enter focuses, `/sessions`/`<C-p>`) | manual — no event-loop + real-host + terminal integration harness exists (see Risks) |
| AC.3 | `just check` / `just lint` / `cargo nextest run -p maki-lua -p maki-ui` | CI-style |
| AC.4 | `just gen-docs` + `just gen-docs-check` + grep for the note in the generated page | docs |

Notes:
- `startup_session_picker_emits_plugin_request` (`maki-ui/src/app/tests.rs:4004`) is left untouched and is **not** an acceptance criterion: nothing in this change touches the UI fire site, so that pre-existing test passes with or without the fix and adds no discriminating power.
- Explicit gap: the full picker behavior (rendered rows, key routing, focus switching) needs the real TUI loop and is not automatable with current infrastructure — the repo has `App`-level render assertions (ratatui `TestBackend`), but no event-loop + real-host integration harness. AC.2 is therefore manual; the automatable proxy is AC.1: Test 1 guards the root-cause wedge, Test 2 guards the fix against degrading to a silent no-op. The Phase 1 execute-first-verify-fails step is what proves the pair actually guards the bug rather than passing vacuously.

## Review Strategy

- Plan-mode: this plan gets a `plan_reviewer` pass before `plan_submit`; findings fixed or rebutted.
- Post-implementation: follow repo norms — run the full workspace test suite and `just lint`, then dispatch a `general` subagent to review the diff (plugin change, test, docs) against AGENTS.md conventions (no comments bloat, KISS, test naming). Fix or explicitly rebut all findings; repeat until no critical findings remain.

## Documentation Strategy

Covered in Phase 3: the lua-api events doc comment (`maki-lua/src/api/autocmd.rs`) gets a one-line note that `SessionPickerRequested` now opens the picker asynchronously through the `/sessions` command (regenerated into `site/docs/content/lua-api/_index.md` via `just gen-docs`); `maki-lua/src/api/AGENTS.md` gets a one-line seam rule (autocmd callbacks must not suspend; defer via `maki.async.run` or commands). No user-facing CLI/docs changes needed — `--continue` behavior is unchanged in intent, only fixed.

## Risks, Blockers, and Required Decisions

- **mlua sync-call failure mode uncertainty (known, not a blocker)**: whether a suspended async fn inside sync `func.call` hangs the thread (wedge) or errors out the callback is mlua-version-specific; the reported symptoms match both variants ("empty picker + dead keys" occurs either way). The fix resolves both because the callback no longer touches async APIs synchronously. The Phase 1 pre-fix validation determines which variant the tests guard; if Test 1 passes pre-fix, the local mlua is the error variant — Tests 1+2 together plus AC.2 cover it, and that must be stated at handoff. Never ship a vacuous test.
- **`maki.async.run` wrapper abandonment (accepted)**: the wrapper task carries the 60s `ASYNC_RUN_DEFAULT_DEADLINE`; it completes in milliseconds when a UI is attached, and is abandoned with a logged warning after 60s in headless/dangling-UI contexts. It never outlives the roundtrip it waits on in the TUI. Expected, harmless.
- **No TUI integration harness (flagged gap)**: AC.2 is manual-only. The repo has `App`-level render assertions (ratatui `TestBackend`) but no event-loop + real-host + terminal harness; building one is out of scope for this fix. The automatable regression (AC.1: wedge guard + seam-routing guard) covers the root cause.
- **SIGTERM in the report**: this tree installs no signal handler (grep confirmed), so SIGTERM terminates the process even in the broken state. If the fork's `mistress` branch adds a graceful-shutdown path that joins the Lua host, that path would hang on the wedged host — worth verifying on the fork separately; out of scope here.
- **General footgun remains (documented, not fixed)**: other autocmd callbacks that suspend (e.g. a user plugin calling `maki.session.current()` in a `TurnEnd` callback) can still wedge the host. Broadly fixing autocmd dispatch is out of scope (breaks tested sync semantics); Phase 3 adds the seam guidance so plugin authors avoid it.