# Reject unknown slash commands via a required `//` escape (issue #106)

## Goal

Any input that starts with a single `/` and does not resolve to a registered
command is rejected everywhere (TUI promptbox, `maki sdk` / SDK stream mode,
ACP, headless `--print` runner) instead of being forwarded to the model as a
literal prompt. A literal message that starts with `/` is sent by prefixing it
with another slash: `//lmao` sends `/lmao`, `///lmao` sends `//lmao` (exactly
one leading slash stripped). `!`/`!!` shell prefix and `exit` keep current
handling; non-slash input stays literal.

## Implementation Summary

One source of truth, in `maki-commands` (frontend-neutral crate):

1. **`classify_input(text) -> SlashClass`** — a new public lexical classifier in
   `maki-commands/src/dispatch.rs` (re-exported from `lib.rs`). It is the only
   place that decides whether text is a command attempt, an escaped literal, or
   plain prose, and the only place the escape-strip value is computed:
   `trim_start()` then `//x` → `EscapedLiteral("/x")` (exactly one leading
   slash dropped), `/x` → `Command("/x")`, else `Plain`. Used by the dispatch
   core, the TUI promptbox, and the palette.
2. **`CommandRegistry::dispatch_input_at`** — the only implementation of the
   reject/escape *outcome*. It classifies once:
   - `EscapedLiteral` → `LiteralInput` carrying the classifier's stripped text
     (attachments preserved);
   - `Command` that fails `resolve_input_for` → `Dispatched(Failed(
     CommandError::UnknownCommand(name)))` (new name-carrying variant);
   - anything else → today's `LiteralInput(content)`.
   Every input surface consumes this single function (see below).

Consumer wiring (no policy re-implemented anywhere):

- **SDK** (`src/sdk_mode.rs`), **ACP** (`maki-acp/src/server.rs`), **headless
  print** (`src/print.rs`), and **nested command dispatch** already call
  `dispatch_input` and already route `Dispatched(Failed(..))` to an error (no
  agent turn) — they reject for free. Their `LiteralInput` arms ignore the
  carried content today (SDK rebuilds from the raw `prompt`; print returns the
  original `AgentInput`), so those two arms switch to building the input from
  the `CommandContent` carried by `LiteralInput` so the escape propagates. ACP
  already forwards the carried content verbatim.
- **TUI promptbox** (`handle_submit` in `maki-ui/src/app/mod.rs`): instead of
  implementing reject/escape, it classifies with the shared
  `classify_input` — `Plain` submits via the normal message flow; `Command`
  becomes a new `CommandRuntime::dispatch_input(...)` call
  (`maki-ui/src/command_runtime.rs`) that pipes the raw text into
  `CommandRegistry::dispatch_input` and relays the outcome through the existing
  `CommandEvent::Outcome` channel; the existing event-loop handler
  (`maki-ui/src/event_loop.rs`) already flashes `Failed`, submits `AgentTurn`,
  and swallows `Completed`. `EscapedLiteral` re-enters the normal message flow
  with the classifier's stripped value (a literal message, not a command, so
  it must not round-trip the command runtime; nothing is stripped in the TUI —
  the value comes from `classify_input`).
- **Palette** (`CommandPalette::sync`, `maki-ui/src/components/command.rs`)
  uses the same classifier so `//`-prefixed input never matches or executes a
  command (`//model` must send `/model`, not run `/model`).
- **Explicit API** `maki.api.run_command` nested dispatch
  (`maki-lua/src/api/tool.rs`) sees the new `Failed(UnknownCommand)` outcome
  where it previously saw `LiteralInput`; map that arm to its existing
  `Err("unknown command")`. The UI-side `run_cmdline`
  (`maki-ui/src/app/mod.rs:2027`) is left alone: it is the name-based explicit
  command executor (used by `maki.api.run_command` and tests), predates this
  change, already rejects unknown names at resolution, and deliberately does
  not adopt the input escape (the issue exempts it).

Result: the escape rule and the reject rule each live in exactly one place
(`classify_input` and `dispatch_input_at`); no surface decides
"unknown slash input → literal" locally.

`submit_prompt` (programmatic message path) is untouched so
`submit_prompt_never_interprets_text` (`/compact`) keeps passing; only the
typed-promptbox path changes.

Docs: `maki-docgen/src/gen_commands.rs` intro lines 64/104 describe the old
pass-through; update the generator and regenerate, plus hand-edit
`site/docs/content/acp/_index.md` and `site/docs/content/headless/_index.md`.

### Files (worktree layout; `maki` crate is at repo root, i.e. `src/`)

- `maki-commands/src/dispatch.rs` (shared classifier + core change)
- `maki-commands/src/lib.rs` (re-export classifier)
- `maki-commands/Cargo.toml` (`test-case` dev-dependency)
- `maki-commands/src/tests.rs`
- `src/sdk_mode.rs` (literal arm uses content; tests)
- `src/print.rs` (literal arm uses content; tests)
- `maki-acp/src/server.rs` (tests only)
- `maki-ui/src/app/mod.rs` (`handle_submit`)
- `maki-ui/src/command_runtime.rs` (new `dispatch_input`)
- `maki-ui/src/components/command.rs` (palette `sync`)
- `maki-ui/src/app/tests.rs`
- `maki-lua/src/api/tool.rs` (`run_command`)
- `maki-lua/tests/plugin_host.rs` (new test)
- `maki-docgen/src/gen_commands.rs`, `site/docs/content/commands/_index.md`,
  `site/docs/content/acp/_index.md`, `site/docs/content/headless/_index.md`

Non-goals: changing `maki.api.run_command` semantics beyond the new
`Failed(UnknownCommand)` arm; changing subagent-chat input (no slash commands
there); changing `run_cmdline`/`resolve_input_for` (explicit name-based
command API, predates the escape, already rejects at resolution — the issue
exempts it; it is intentionally not an input-parse surface).

## Implementation Plan

### Phase 1 — Shared classifier + core rejection/escape (`maki-commands`)

`maki-commands/src/dispatch.rs` — the single source of truth for the escape
rule. Add a public lexical classifier near `ParsedInput` (line ~165), and
re-export it from `maki-commands/src/lib.rs` (`pub use dispatch::{...,
SlashClass, classify_input}`):

```rust
pub enum SlashClass<'a> {
    /// Single-slash command attempt: `/foo` or ` /foo` (trimmed).
    Command(&'a str),
    /// `//foo` → literal `/foo`; `///foo` → literal `//foo`. Exactly one
    /// leading slash is stripped from the trimmed text.
    EscapedLiteral(&'a str),
    /// Not a command attempt.
    Plain,
}

pub fn classify_input(text: &str) -> SlashClass<'_> {
    let trimmed = text.trim_start();
    if trimmed.starts_with("//") {
        SlashClass::EscapedLiteral(&trimmed[1..])
    } else if trimmed.starts_with('/') {
        SlashClass::Command(trimmed)
    } else {
        SlashClass::Plain
    }
}
```

`dispatch_input_at` (~line 53) classifies once and branches:

```rust
let class = classify_input(&content.text);
if let SlashClass::EscapedLiteral(literal) = class {
    let attachments = content.attachments.clone();
    let text = Arc::from(literal);
    return Box::pin(async move {
        InputDispatch::LiteralInput(CommandContent { text, attachments })
    });
}
let Ok(resolved) = self.resolve_input_for(&target, &content.text) else {
    if matches!(class, SlashClass::Command(_)) {
        let name = Arc::from(
            ParsedInput::parse(content.text.trim_start())
                .map_or(content.text.trim_start(), |parsed| parsed.name),
        );
        return Box::pin(async move {
            InputDispatch::Dispatched(CommandOutcome::Failed(
                CommandError::UnknownCommand(name),
            ))
        });
    }
    return Box::pin(async move { InputDispatch::LiteralInput(content) });
};
// existing dispatch_resolved path unchanged
```

(`SlashClass` is `Copy` — borrows of `&str` — so `class` survives the early
return and is reused in the failure branch.)

Change the `CommandError` variant (~line 386-388):

```rust
#[error("unknown command {0}")]
UnknownCommand(Arc<str>),
```

`ParsedInput` is `pub(super)` in the same file (line 165) — reuse it for the
reported name (whitespace-truncated command word). Leading whitespace is
dropped on the escaped literal (mirrors `ParsedInput`/`classify_input` trim
semantics; input ` //lmao` sends `/lmao`). A bare `/` becomes
`Failed(UnknownCommand("/"))`. Names literally registered as `//foo` become
unreachable through `dispatch_input` — accepted, this is the point of the
escape.

Update tests in `maki-commands/src/tests.rs`:

- line ~280 (`projection_and_dispatch_share_capability_filter`):
  `dispatch_input(&portable, "/picker")` now
  `Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(_)))`;
  keep the `interactive` assert unchanged.
- line ~480 (foreign-target test): `dispatch_input(&foreign_target,
  "/local")` now `Dispatched(Failed(UnknownCommand(_)))`.
- add `classify_input` unit tests (pure fn, exhaustive — cheap):
  `/lmao`→Command("/lmao"), ` /lmao`→Command("/lmao"), `//lmao`→Escaped("/lmao"),
  `///lmao`→Escaped("//lmao"), `//`→Escaped("/"), prose→Plain, empty→Plain.

Add `#[test_case]` dispatch suite (per AGENTS.md). `maki-commands/Cargo.toml`
`[dev-dependencies]` currently has only `futures-lite` — add
`test-case = { workspace = true }` (the workspace root already declares
`test-case = "3"`, so workspace inheritance is enough). If that addition is
undesired, fall back to plain `#[test]` fns with a small table loop inside.

```rust
#[test_case("/lmao" => Failed(UnknownCommand(name)) if name == "/lmao"; ...)]
// plus: " /lmao" (leading ws) => Failed; "/" => Failed;
// "//lmao" => LiteralInput "/lmao"; "///lmao" => LiteralInput "//lmao";
// " //lmao" => LiteralInput "/lmao";
```

Write it as a single `#[test_case]` fn asserting the `InputDispatch` outcome
shape; include one case asserting attachments survive the escape
(`CommandContent { text: "//lmao", attachments: [...] }` →
`LiteralInput` with the same attachments). Existing registry/producer/target
helpers (`CommandRegistry::new()`, `create_producer`, `registration`,
`bind_target`, `Host`) already exist in the test module.

### Phase 2 — SDK stream mode (`src/sdk_mode.rs`)

Production, line ~877: switch the literal arm to consume the carried content:

```rust
InputDispatch::LiteralInput(content) => {
    let input = AgentInput {
        message: content.text.to_string(),
        mode: mode.agent_mode(&cwd),
        images: command_attachments::into_images(&content.attachments)?,
        preamble: Vec::new(),
        thinking: Default::default(),
        fast,
        workflow,
        prompt: None,
    };
    if handle.input_tx.send(input).is_err() { break; }
}
```

The `Dispatched(Failed(error))` arm already emits
`emit_command_result(&writer, &shared, true, error.to_string())` — no change.

Tests (same file, `sdk_dispatch_returns_outcomes_and_preserves_literal_input`
~line 1825):

- `/unknown literal` assert now `Dispatched(Failed(CommandError::UnknownCommand(_)))`;
  keep `ordinary literal` → `LiteralInput(_)`.
- add: `commands.dispatch_input("//unknown literal", &images)` →
  `LiteralInput(content)` with `content.text == "/unknown literal"` and the
  existing gif attachment intact; `///unknown literal` → `//unknown literal`.
- `sdk_unsupported_builtin_is_literal_input` (~line 1857): `/help` on the SDK
  target now `Dispatched(Failed(CommandError::UnknownCommand(_)))`; rename test
  `sdk_unsupported_builtin_is_rejected`.

Note: the run loop (`pub fn run`, stdin-driven) has no direct unit harness; the
dispatch-level tests above pin the new outcomes and the loop arm is the
unchanged `Failed` arm plus the content-based `LiteralInput` arm (mirrors the
ACP handler, which is covered at handler level — see Phase 4).

### Phase 3 — Headless print runner (`src/print.rs`)

Production, line ~213 in `drive_print`:

```rust
InputDispatch::LiteralInput(content) => AgentInput {
    message: content.text.to_string(),
    mode: literal.mode.clone(),
    images: command_attachments::into_images(&content.attachments)?,
    preamble: Vec::new(),
    thinking: Default::default(),
    fast: literal.fast,
    workflow: literal.workflow,
    prompt: None,
},
```

`Dispatched(Failed(error)) => return Err(error.into())` already rejects — no
change. (`drive_print` runs the `--print`/headless prompt path; this is the
"headless runner" surface from the issue.)

Tests (module at line ~520):

- `unknown_command_runs_literal_input` (~line 716): flips to assert
  `drive_print(..., input("/unknown literal"), ...)` returns `Err` (runner not
  invoked).
- add `escaped_slash_runs_literal_input`: `//unknown literal` (with a webp
  image) → runner receives message `/unknown literal` + image intact; and a
  `///unknown literal` case → `//unknown literal`. Use the existing
  `target(&registry)`, `CommandTurnMarker`, and closure-runner helpers.

### Phase 4 — ACP (`maki-acp/src/server.rs`)

No production change: `handle_prompt` (line ~682) already maps
`LiteralInput(content)` → `send_command_turn` (content-based, escape
propagates) and `Dispatched(Failed(error))` → `Err(command_error(error))`
(-32602, `error.to_string()`).

Tests:

- `unknown_slash_prompt_is_sent_to_agent_literal` (~line 1553): rename to
  `unknown_slash_prompt_is_rejected`; assert
  `handle_prompt("/does-not-exist value")` returns `Err` with
  `error.code == -32602` and `error.message == "unknown command /does-not-exist"`,
  `input_rx.is_empty()`.
- `unavailable_interactive_command_is_sent_to_agent_literal` (~line 1590):
  `/help` + image now rejected; assert `Err` with message `"unknown command
  /help"` and empty `input_rx`.
- add `escaped_slash_prompt_is_sent_literal`: `//does-not-exist value` (image)
  → `send_command_turn` fires with message `/does-not-exist value` and the
  image preserved (mirror `agent_turn_preserves_image_content` asserts).

### Phase 5 — TUI promptbox (`maki-ui`)

The TUI implements no reject/escape policy; it pipes input through
`dispatch_input` like every other surface.

`maki-ui/src/command_runtime.rs` — new `dispatch_input` mirroring
`dispatch_command` (~line 118): spawn `registry.dispatch_input(target, content)`
on the smol executor and relay the outcome through the existing
`CommandEvent::Outcome` channel (reuse `send_outcome`-style code):

```rust
pub(crate) fn dispatch_input(&self, target: &TargetHandle, content: CommandContent) {
    let future = self.registry.dispatch_input(target, content);
    let tx = self.event_tx.clone();
    let target_id = target.id();
    smol::spawn(async move {
        if let InputDispatch::Dispatched(outcome) = future.await {
            let _ = tx
                .send_async(CommandEvent::Outcome { target: target_id, outcome })
                .await;
        }
    })
    .detach();
}
```

`LiteralInput` here is unreachable (a `Command`-classified input never yields
literal after the Phase 1 core change); a `tracing::debug!` else-branch is
fine but not required.

The existing production event-loop handler (`maki-ui/src/event_loop.rs` ~939)
and the test drain (`execute_pending_commands`, `maki-ui/src/app/mod.rs`
~2097) already do the right thing with `CommandEvent::Outcome`: `Failed`
→ `flash(error.to_string())`, `AgentTurn` → `submit_command_turn`, `Completed`
→ nothing. No new outcome handling anywhere.

`maki-ui/src/components/command.rs`, `CommandPalette::sync` (~line 489) — use
the shared classifier so `//`-prefixed input never matches commands (import
`maki_commands::{classify_input, SlashClass}`). Rewrite the guard to operate
on the classifier's trimmed `Command` value:

```rust
let SlashClass::Command(trimmed) = classify_input(input) else {
    self.filtered.clear();
    self.current_arg_count = 0;
    return;
};
let stripped = &trimmed[1..]; // trimmed starts with exactly one '/'
// existing body unchanged (parts, cmd_word, arg_count, nucleo reparse, tick)
```

This makes `is_active()` false for `//...` and plain/escaped input, so
`handle_key` Enter passes through to the input box (`Sub`), never `Execute`.
One deliberate side effect: leading-whitespace command attempts (` /model`)
now match in the palette, consistent with `dispatch_input_at`'s trim semantics
(today they cleared because the raw input doesn't start with `/`; the new
`handle_submit` routes them to the registry either way, so palette and runtime
agree). (Verify `sync_arguments`, the cursor-move path in the same file, does
not independently reopen the palette for `//` input; mirror the guard there if
it does.)

`maki-ui/src/app/mod.rs`, `handle_submit` (~line 1552), after the `exit` and
`!`/`!!` shell-prefix handling (unchanged order — they short-circuit first, so
`!` never hits the slash path and `exit` still quits; import
`maki_commands::{classify_input, SlashClass}`):

```rust
match classify_input(&sub.text) {
    SlashClass::Plain => self.submit_or_queue(sub.into()),
    SlashClass::EscapedLiteral(literal) => {
        let mut sub = sub;
        sub.text = literal.to_owned(); // value computed by the shared classifier
        self.submit_or_queue(sub.into())
    }
    SlashClass::Command(_) => {
        let attachments = sub
            .images
            .iter()
            .map(|image| CommandAttachment {
                media_type: Arc::from(image.media_type.mime()),
                data: Arc::clone(&image.data),
            })
            .collect::<Arc<[_]>>();
        self.command_runtime.dispatch_input(
            &self.command_target,
            // raw text: dispatch_input_at owns trimming and all policy
            CommandContent { text: Arc::from(sub.text.as_str()), attachments },
        );
        vec![]
    }
}
```

Escaped literals re-enter the normal message flow with the classifier's value
(a literal must not round-trip the command runtime; the TUI contains no strip
logic — `handle_submit` only copies the classifier's value). The only `[1..]`
cut in the TUI is the palette's nucleo query derivation in `sync`, which runs
after the `SlashClass::Command` guard and never computes the escape value.
`Command` inputs go to the runtime asynchronously; the event loop
flashes `Failed` (the reject message) or submits `AgentTurn` on a later
iteration — no flash/submit code in `handle_submit`.

Tests (`maki-ui/src/app/tests.rs`):

- `slash_noncommand_sends_as_prompt` (~line 4017): rename; after
  `type_and_submit(&mut app, "/nonexistent")` the `Command` route returns
  immediately, so drain with `app.execute_pending_commands()` (private helper
  in `maki-ui/src/app/mod.rs`, accessible from the crate's test module) then
  assert `flash_text().is_some()` (contains "unknown command") and no
  `Action::SendMessage`.
- add `slash_escape_sends_literal_input`: `type_and_submit("//lmao")` →
  exactly one `Action::SendMessage(ai)` with `ai.message == "/lmao"`;
  `///lmao` → `"//lmao"` (synchronous, no drain needed). Use the existing
  `type_and_submit`/`test_app` helpers.
- add palette unit test in `maki-ui/src/components/command.rs` test module:
  `sync("//model")` leaves `is_active()` false (filtered empty) while
  `sync("/model")` is active. Use the existing `CommandPalette::new(registry,
  target)` test setup.

`submit_prompt` (`maki-ui/src/app/queue.rs`) is intentionally unchanged:
`submit_prompt_never_interprets_text` (`/compact`, `exit`, `!ls`, line ~856)
continues to assert literal submission for programmatic submits.

### Phase 6 — Lua `maki.api.run_command` (`maki-lua/src/api/tool.rs`)

`run_command` nested branch (~line 905): nested dispatch now yields
`Dispatched(Failed(CommandError::UnknownCommand(_)))` for unknown `/` cmdlines.
Preserve API rejection:

```rust
match result {
    InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(_))) => {
        Err("unknown command".to_owned())
    }
    InputDispatch::Dispatched(_) => Ok(()),
    InputDispatch::LiteralInput(_) => Err("unknown command".to_owned()),
}
```

(The UI-side `UiAction::RunCommand` → `run_cmdline` path already rejects at
resolve time; unchanged.)

Extract the match into a private helper `nested_dispatch_result(result:
InputDispatch) -> Result<(), String>` so the new arm is unit-testable: at the
Lua boundary the arm is observable-equivalent to the old `LiteralInput` arm
(both surface `Err("unknown command")`), so an API-level test cannot
distinguish them. Add `#[test_case]` unit tests in `maki-lua/src/api/tool.rs`
pinning all three outcomes directly with constructed `InputDispatch` values
(`Failed(UnknownCommand)` → `Err`, other `Dispatched` → `Ok`, `LiteralInput` →
`Err`).

Add a plugin_host test (`maki-lua/tests/plugin_host.rs`,
`nested_run_command_rejects_unknown_command`): register `/go` whose handler
calls `maki.api.run_command("/bogus")` and flashes `tostring(ok) .. "|" ..
tostring(err)`; assert a `UiAction::Flash` with `"nil|unknown command"`. Invoke
`/go` by dispatching through the registry (`registry.dispatch_input`), not
`run_command_for_test`: the latter runs the handler in the legacy scope
without a `command_invocation`, which takes the UI-roundtrip branch instead.
The flash assertion is a behavior-preservation pin (the old code flashed the
same string via the `LiteralInput` arm); the new arm is pinned by the
`nested_dispatch_result` unit tests above.

Also reword `maki.api.run_command`'s doc comment (it claimed "exactly as
typing it in the input would", now false for `//`-prefixed cmdlines): describe
it as the explicit name-based executor where the leading slash is optional,
extra leading slashes are stripped, and the `//` input escape does not apply.

### Phase 7 — Docs

- `maki-docgen/src/gen_commands.rs`:
  - line ~64: replace "Unknown and unavailable slash-prefixed text remains a
    model prompt." with a sentence stating unknown/unavailable slash input is
    rejected with an error, and prefixing `/` text with `/` sends it literally
    (`//lmao` → `/lmao`).
  - line ~104: replace "Invoking an unavailable command sends the complete
    input as ordinary model text." with the rejection + escape behavior.
  - regenerate: `just gen-docs` (produces `site/docs/content/commands/_index.md`).
- Hand-written, edit directly:
  - `site/docs/content/acp/_index.md` lines ~45/47: same rewording (unknown/
    unavailable slash input returns an error; `//` escape).
  - `site/docs/content/headless/_index.md` lines ~40/42: same rewording; also
    change "Commands that require TUI capabilities remain literal input in
    headless frontends." to reflect rejection (or escape).

Verification: `just gen-docs-check`, `just check`, `just lint`, `just test`.

## Acceptance Criteria

- **AC.1** — `dispatch_input` core: unknown `/` input yields
  `Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(name)))`;
  `//x` → `LiteralInput("/x")`; `///x` → `LiteralInput("//x")`; attachments
  preserved; leading-whitespace variants handled; `classify_input` returns the
  documented classes. → maki-commands tests.
- **AC.2** — TUI promptbox: typing `/lmao` flashes the error (via the
  event-loop `Failed` path) and starts no turn; `//lmao` sends `/lmao`;
  `///lmao` sends `//lmao`; `//model` never opens/executes the palette. →
  maki-ui tests (tests.rs, drained with `execute_pending_commands`, plus
  command.rs palette test).
- **AC.3** — SDK: dispatch-level reject for `/unknown literal` and `/help`;
  escape yields stripped text with attachments. → sdk_mode.rs tests.
- **AC.4** — ACP: `handle_prompt` returns the unknown-command error (no agent
  turn) for `/does-not-exist` and `/help`; `//does-not-exist value` becomes a
  literal turn with text `/does-not-exist value`. → server.rs tests.
- **AC.5** — Headless print: `drive_print` errors on `/unknown literal`
  (runner not invoked) and forwards stripped literal for `//...`. → print.rs
  tests.
- **AC.6** — `maki.api.run_command("/bogus")` inside a command handler still
  returns `(nil, "unknown command")`. → plugin_host.rs test.
- **AC.7** — Docs reflect rejection + escape; `just gen-docs-check` passes and
  generated/acp/headless pages no longer claim pass-through.
- **AC.8** — `just check`, `just lint` (-D warnings), and `just test` all pass
  on the workspace.

## Test Strategy

Mapping (tests named for each AC; AC.1–AC.5 fail before the change, AC.6–
AC.8 are regression pins / suite-wide gates that pass pre-change by design):

| AC | Tests |
|----|-------|
| AC.1 | new `#[test_case]` dispatch suite + `classify_input` unit tests + updated tests at `maki-commands/src/tests.rs:280` and `:480` |
| AC.2 | flipped `slash_noncommand_sends_as_prompt` (drains `execute_pending_commands`) + new `slash_escape_sends_literal_input` (`maki-ui/src/app/tests.rs`); new palette `sync` test (`maki-ui/src/components/command.rs`); `CommandRuntime::dispatch_input` covered through these |
| AC.3 | updated `sdk_dispatch_returns_outcomes_and_preserves_literal_input` + flipped `sdk_unsupported_builtin_is_literal_input` (`src/sdk_mode.rs`) |
| AC.4 | flipped `unknown_slash_prompt_is_sent_to_agent_literal` + `unavailable_interactive_command_is_sent_to_agent_literal` + new escape test (`maki-acp/src/server.rs`) |
| AC.5 | flipped `unknown_command_runs_literal_input` + new escape test (`src/print.rs`) |
| AC.6 | new plugin_host test (`maki-lua/tests/plugin_host.rs`) |
| AC.7 | `just gen-docs-check`; doc edits |
| AC.8 | `just check` / `just lint` / `just test` |

Known coverage gaps (flagged, not hidden):

- The SDK `run()` loop reads stdin and has no direct harness, so the loop's
  `LiteralInput` arm consuming the escaped content is not asserted
  end-to-end; it is pinned by the dispatch-level escape tests (AC.3) and
  mirrored by the ACP handler-level escape test (AC.4).
- `AC.7`'s hand-edited `acp/_index.md` and `headless/_index.md` are not
  machine-checked (`just gen-docs-check` only diffs the generated
  `commands/_index.md`); verifying they no longer claim pass-through relies
  on review of the edited pages.

All other surfaces are tested at the handler boundary that decides between
turn/error.

Existing tests that must keep passing unchanged: `submit_prompt_never_interprets_text`
(text cases `/compact`, `exit`, `!ls`), ACP `/model`/`/compact`/`/btw` error
tests, `run_cmdline_keeps_typed_input`, `portable_bare_*` tests.

## Review Strategy

After implementation, follow repo conventions (`just check`/`lint`/`test`),
then dispatch a `general` subagent to review the diff against this plan,
focusing on: all `LiteralInput`/`dispatch_input` call sites updated (entire
list from `grep LiteralInput`), palette `sync` escape guard, docs consistency,
no other surface still passes unknown `/` text to the model, and — the
centralization contract — no surface re-implements `//`-stripping or
unknown-slash rejection locally. In `maki-ui`, the only permitted slash-handling
sites are the two `classify_input` call sites (`handle_submit`, palette `sync`)
plus `run_cmdline`'s pre-existing normalization; grep `maki-ui` for
`starts_with("//")`, `strip_prefix("//")`, and `trim_start_matches('/')` to
confirm no new literal-strip logic appeared, and grep the workspace for
`unknown command` outside `maki-commands` (only `run_cmdline`'s quoted message
and the Lua `Err("unknown command")` are allowed). Fix or rebut findings;
re-review until no critical/high findings remain.

Performed (2026-09-04): a cooperative `general` review found no critical/high
issues and confirmed the centralization contract; an adversarial `general`
review then probed escape bypasses, split-brain trim semantics, TUI timing,
cross-surface drift, and regressions with a throwaway probe crate. Verdict: no
CRITICAL path from typed user input to the model on the four mandated
surfaces; `//x` → `/x` exactly-one-strip confirmed everywhere those surfaces
classify. Actions taken from findings:

- Fixed: nested-run_command test was observable-equivalent to the old code
  (tautology) → extracted `nested_dispatch_result` + direct unit tests
  (Phase 6); commands-page doc over-claimed global rejection while
  `maki.session.prompt` and subagent chat stay verbatim → added "Text sent
  programmatically (`maki.session.prompt`, subagent chat) is not parsed as a
  command" to `gen_commands.rs`; misleading `maki.api.run_command` doc →
  reworded.
- Rebutted (accepted as-is): invisible-character prefix (`\u{200B}/lmao` →
  `Plain`, pinned by a `classify_input` test case — "starts with `/`" is
  defined on `trim_start`ed text); `StaleTarget` misreported as
  `UnknownCommand` (plan's "does not resolve" design); whitespace-in-name
  (`/\u{00A0}lmao` → name `/`) and trailing-space drift (`//help  `) —
  pre-existing `ParsedInput`/TUI semantics; per-branch `run_command` error
  strings — planned two-message design; dead `map_or` fallback and
  `//  lmao` → `/  lmao` — plan-faithful.

## Documentation Strategy

User-visible behavior change → update the three doc pages listed in Phase 7.
Commands page is generator-owned (`maki-docgen`); ACP and headless pages are
hand-written. No AGENTS.md change needed.

## Risks, Blockers, and Required Decisions

- **`//`-named commands become unreachable** via `dispatch_input` and the
  palette. Pathological; accepted — it is the defined escape.
- **Escaped literals drop leading whitespace** (` //lmao` → `/lmao`), matching
  `ParsedInput` trim semantics. Deterministic; accepted.
- **Bare `/` is rejected** as `unknown command /` — consistent with "starts
  with a single `/` and does not resolve".
- **SDK loop coverage gap** (see Test Strategy) — accepted; mirrored by ACP
  handler test.
- **Error message is now unified**: all surfaces that go through
  `dispatch_input` (TUI promptbox, SDK, ACP, headless) surface the
  `CommandError` Display `unknown command /lmao`; the TUI's flash comes from
  the event-loop `Failed` arm, not from TUI code. The only other message
  shape is the explicit API's pre-existing quoted `unknown command '/x'`
  (`run_cmdline`), which is intentionally a separate surface.
- **`run_cmdline` does not adopt the escape**: `maki.api.run_command` /
  `UiAction::RunCommand` keep the existing name-based behavior
  (`trim_start_matches('/')` normalization, reject at resolve). This is the
  explicit command API the issue exempts; it is not an input-parse path.
  Deliberate boundary.
- **TUI Command-outcome timing**: `handle_submit` returns before the outcome
  is known; the flash (reject) or turn (rare `AgentTurn`) lands on a later
  event-loop tick. Tests drain events with `execute_pending_commands` to
  observe it. Matches the existing palette path's timing.
- **"Unknown" covers capability-unavailable commands**: `/help` on a target
  lacking TUI capabilities reports `unknown command /help` because
  `resolve_for` collapses both cases into `ResolutionError::UnknownCommand`.
  Matches the issue's "does not resolve to a registered command" phrasing.
- **Subagent chat is out of scope**: `handle_subagent_chat_key`
  (`maki-ui/src/app/mod.rs:~1528`) routes `/...` text straight to the
  subagent queue; it has no slash commands, so `/lmao` sent there still
  reaches the model as literal text. Documented non-goal. The commands-page
  intro now carves out programmatic surfaces ("Text sent programmatically
  (`maki.session.prompt`, subagent chat) is not parsed as a command") so the
  docs do not over-claim global rejection.
- **Invisible characters before the slash**: `\u{200B}/lmao` classifies as
  `Plain` because `trim_start()` only strips Unicode `White_Space`, so a
  ZWSP/LRM/BOM prefix means the text "starts with `/`" only visually and
  reaches the model verbatim (same pre-existing behavior in the TUI input
  box). Accepted as out of the issue's "starts with a single `/`" wording;
  pinned by a `classify_input` test case so the contract is explicit.
- No blockers. No operator decisions required beyond approving the plan.