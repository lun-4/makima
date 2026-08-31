# ACP command-effect projection handover

## Status

No implementation has started for the work described here. The investigation only read the current implementation and the ACP 0.13.8 schema. The next session should start from the merged default branch, `mistress`, rather than continue from `command-registry-v2`.

Zed is the tested ACP client. Protocol statements below describe the portable ACP v1 surface used by Maki. Client presentation details must still be verified against Zed.

The predecessor plan is `.agents/makima-plans/50-command-registry-acp-unification.md`. That plan implemented shared command discovery and dispatch, but its assumption that every selected builtin could be projected faithfully through ACP is no longer valid. It also contains two literal truncation-marker corruptions. The investigation and decisions needed for continuation are restated below, so the damaged clauses are not required. Issue #75 tracks the read-output corruption mechanism.

## Goal

Make ACP command execution preserve the meaning of each command and project all client-visible state that ACP can represent. Do not report a local command as complete when the client still displays stale state or when the command used different semantics from the TUI.

The correction is broader than adding notifications in `maki-acp`. The current command outcome collapses several persistent and transient effects into `Completed`. The implementation needs explicit ownership for persistent session options, separate execution for isolated turns, and typed progress or feedback for effects that are not options.

## Observed failures

Manual testing through Zed found these failures:

| Command | Observed behavior | Required behavior |
|---|---|---|
| `/cd` | The backend changes directory, but the client UI keeps the original directory. | Change the live backend directory, use it for later tool locations, and show the canonical directory in the transcript. Document that ACP cannot change Zed's session directory field. |
| `/new` and `/clear` | Model history is emptied without clearing the visible transcript or creating a new ACP session. | Do not advertise or execute these aliases through ACP. Session creation remains client-initiated. |
| `/model` | The model changes in the backend, but the ACP current-model control remains stale. Slash-command arguments have no searchable model completion. | Route slash selection and the ACP model selector through one setter. Publish the resulting full config-option snapshot. Use the ACP model config option for search and selection. |
| `/btw` | The command does not behave like the TUI side-question feature. | Run one isolated provider request over copied history, without tools or primary-history mutation, and stream the answer through the active ACP prompt. |
| `/yolo` | Permission behavior changes without a client indicator. | Expose and update a session option in ACP's `mode` category. |
| `/workflow` | Workflow behavior changes without a client indicator. | Expose and update a session option in ACP's `mode` category. |
| `/compact` | Compaction runs without visible progress or completion information. | Emit tool-style progress, complete the prompt after compaction, and persist the compacted history immediately. |
| `/automode` | The portable Lua command toggles Bash state but has no ACP selector or indicator. | Expose Bash auto mode as a plugin-owned session option in ACP's `mode` category. |

`/fast` was not in the original failure list, but it has the same architecture as YOLO and Workflow. It also belongs in the shared session-option work.

## Confirmed current architecture

### Command dispatch loses effects

`maki-commands/src/dispatch.rs` defines only these command outcomes:

```rust
pub enum CommandOutcome {
    Completed,
    AgentTurn(AgentTurn),
    Failed(CommandError),
}
```

`HostResponse` has the same `Completed` and `AgentTurn` split. A completed result carries no state delta, progress source, feedback text, or lifecycle meaning.

`maki-agent/src/command.rs` implements the portable host through `SessionCommandHost`. It mutates `SessionCommandState`, `PermissionManager`, the model channel, or `InteractiveControl`, then usually returns `HostResponse::Completed`. The state currently contains the model, Fast state, model list, working directory, and Workflow state. YOLO remains in `PermissionManager`.

`maki-acp/src/server.rs::handle_prompt` responds to every `CommandOutcome::Completed` with an immediate `PromptResponse(EndTurn)`. It emits no other update. This is why successful backend mutations leave the client stale.

This loss should not be fixed by adding a large generic `CommandEffect` union to `CommandOutcome`. Persistent options, transient feedback, progress events, and model turns have different ownership and lifecycle rules. They should remain separate abstractions.

### ACP already has a partial model selector

`maki-acp/src/methods.rs::model_config_option` builds a select option with ID `model` and category `SessionConfigOptionCategory::Model`.

`maki-acp/src/server.rs` includes that option in new-session and load-session responses. Model discovery also emits `ConfigOptionUpdate`. Direct `session/set_config_option` requests update the model and return a refreshed model option.

The missing confirmed behavior is the opposite direction: `/model` calls `SessionCommandHost::SetModel`, mutates `SessionCommandState`, and returns `Completed`. The ACP server does not emit a `ConfigOptionUpdate` for that path.

The model options are therefore not wholly absent from the wire. If Zed still shows no model selector during the next test, capture the initial response and discovery updates before changing the schema. Slash-command metadata supports an unstructured argument hint, not a searchable list of argument values. Search belongs in the ACP model config option.

### `/new` resets history inside the existing ACP session

`maki-agent/src/command.rs` maps builtin `/new` and alias `/clear` to `BuiltinOperation::ResetSession`. `SessionCommandHost` sends `InteractiveControl::Reset`.

`maki-agent/src/headless.rs::apply_interactive_control` replaces the model history with an empty list and records that empty history in the existing session store. It does not create a new `SessionRef`, replace the ACP `SessionId`, or ask the client to clear its transcript.

This behavior explains the manual result: future turns have empty model context, while Zed still shows prior messages under the same session.

### `/cd` stores the requested path instead of the canonical result

`maki-agent/src/headless.rs::apply_interactive_control` canonicalizes the requested path, verifies that it is a directory, updates the agent working directory, updates permission state, and persists the canonical path.

The control reply contains only `Result<(), String>`. After success, `SessionCommandHost` stores the original requested `PathBuf` in `SessionCommandState`. Relative paths and symlinked paths can therefore disagree with the path used by the headless agent.

`maki-acp/src/server.rs::start_event_pump` also captures the session startup directory by value. Later tool-call locations in `maki-acp/src/translate.rs` continue to resolve relative paths against that stale directory after `/cd`.

### `/compact` has no ACP translation and no immediate checkpoint

`InteractiveControl::Compact` calls `agent::compact` over the live history and event sender. Agent events include `AutoCompacting` and `CompactionDone`, but `maki-acp/src/server.rs::start_event_pump` has no translation for either event.

The headless session store records history after normal agent turns. The compact control path does not call `SessionStore::record_turn` after compaction. A successful manual compaction can remain only in memory until another turn triggers persistence.

### `/btw` uses primary-turn semantics in ACP

`SessionCommandHost` converts `BuiltinOperation::QuickQuestion` to a normal `AgentTurn`. `maki-acp/src/server.rs::send_command_turn` sends it through the primary interactive agent input channel with the current mode, tools, Fast state, Workflow state, and normal history mutation.

The TUI implementation in `maki-ui/src/app/btw.rs` has different semantics. It copies the shared history, closes dangling tool calls in the copy, appends a side-question reminder, supplies an empty tool list, calls the current provider directly, and streams into a modal. It does not append the question or answer to primary model history.

ACP cannot display a TUI modal. It can still preserve the model isolation contract and stream the answer as ACP agent-message chunks. The answer will remain visible in Zed's transcript.

### `/automode` state is private to the Bash plugin

`plugins/bash/bash_helpers.lua` stores `auto_mode_on` in module state. `plugins/bash/init.lua` initializes it from plugin options. The `/automode` handler toggles that module field and calls `maki.ui.flash`.

The command result contains neither the previous value nor the resulting value. ACP cannot read the initial state, set an explicit value, validate a selector request, or observe a later change. Inferring state from a completed Lua handler is not reliable.

## Confirmed ACP constraints

Maki currently depends on `agent-client-protocol-schema` 0.13.8 and advertises ACP protocol v1.

The portable v1 `SessionUpdate` variants include message chunks, thought chunks, tool calls and updates, plan updates, available-command updates, current session-mode updates, config-option updates, session-info updates, and usage updates.

The schema has no portable operation that performs any of these actions:

- Clear or retract previously rendered transcript messages.
- Replace the session ID of an active client session.
- Ask the client to create a new session from inside a slash-command prompt.
- Change the original working directory displayed by the client.

`SessionInfoUpdate` carries title, timestamps, and custom metadata. Custom metadata cannot be treated as a portable working-directory update because clients make no presentation or behavior guarantees for agent-defined metadata.

ACP session modes and config-option categories are separate mechanisms:

- Build, Plan, and custom agent modes use `SessionModeState`, `session/set_mode`, and `CurrentModeUpdate`.
- Model selection uses a session config option with category `model`.
- YOLO, Fast, Workflow, and Bash auto mode should use distinct session config options with category `mode`.

Several config options can share the `mode` category. Client layout is client-defined. Array order can affect which controls Zed presents most prominently.

`ConfigOptionUpdate` describes the full set of current options. Emit a complete ordered snapshot rather than a changed item in isolation.

## Confirmed decisions

### Do not expose `/new` or `/clear` through ACP

ACP session creation is client-initiated. A command running inside `session/prompt` cannot faithfully create and switch to a replacement client session.

Remove `/new` and `/clear` from ACP advertisement and ACP command dispatch. A manually typed unavailable command must not reset model history. It should follow the established unavailable-command policy, which currently forwards unknown or unavailable slash-prefixed text literally.

Do not add a Zed-specific transcript-clearing extension. Do not continue resetting hidden model context while leaving the transcript intact.

The capability model should distinguish session compaction from session replacement. The current broad `SessionControl` capability groups `/compact` and `/new` even though ACP can represent only the former. Prefer a capability or availability policy that expresses the semantic distinction instead of filtering command names in `maki-acp`.

### Keep slash commands and selectors over the same state

Keep `/model`, `/yolo`, `/fast`, `/workflow`, and `/automode`. Slash commands remain useful for portability and command-line use.

Each slash command and each ACP `session/set_config_option` request must call the same explicit setter. Toggling is a command-level convenience. The shared state operation accepts the requested value.

For example, `/yolo` reads the current value and calls `set("enabled")` or `set("disabled")`. The ACP selector calls the same setter with its selected value. The setter validates, changes the underlying state, and publishes the new snapshot after success.

### Use ACP config-option categories as follows

| Option | ACP category | Initial value source |
|---|---|---|
| Model | `model` | Current session model |
| YOLO | `mode` | Session `PermissionManager` |
| Fast | `mode` | `SessionCommandState` and current model capability |
| Workflow | `mode` | `SessionCommandState` |
| Bash auto mode | `mode` | Bash plugin option and live plugin state |

Use stable select values such as `enabled` and `disabled` unless the implementation deliberately enables and adopts ACP's unstable boolean-option feature. Select options work with the current dependency features and give Zed labels such as Enabled and Disabled.

A model change can disable Fast when the new model does not support it. That single setter operation must publish a snapshot that contains both the new model and the resulting Fast value.

### Give `/cd` visible but honest feedback

Keep `/cd` available through ACP. A successful command should report the canonical directory in the transcript, for example:

```text
Working directory: /canonical/path
```

The feedback is a frontend event. It must not become primary model history.

The implementation must also make the canonical directory the single live value used by command context and ACP tool-location translation. Documentation must state that ACP does not let Maki update Zed's displayed session directory.

### Show `/compact` as tool-style progress

Represent manual compaction with ACP `ToolCall` and `ToolCallUpdate` messages. Suggested presentation:

```text
Compact context    in progress
Compact context    completed
```

A failed compaction should mark the tool call failed and return a prompt error. A successful compaction should persist the compacted history before the prompt completes.

ACP does not replace Zed's visible transcript after compaction. The progress item states that model context was compacted. It must not imply that old client messages were removed.

### Preserve `/btw` isolation rather than TUI presentation

The ACP `/btw` contract is:

- Copy current model history.
- Close dangling tool calls in the copy.
- Append the side-question reminder and the user question to the copy.
- Include command image attachments.
- Use the current provider and model.
- Expose no tools.
- Run one provider turn.
- Stream text through ACP agent-message chunks for the active prompt.
- Append neither the question nor the answer to primary model history.
- Complete or fail the current ACP prompt when the isolated turn ends.

The answer remains in the client transcript because ACP has no modal or ephemeral-message primitive. Isolation applies to provider input, tools, and primary model history.

### Add a generic Lua session-option primitive for `/automode`

A Bash-specific ACP hook would fix one command but leave plugin-owned state unprojectable. The preferred direction is a generic Lua registration primitive that publishes a session option and returns a handle for reading and setting its value.

An illustrative Lua shape is:

```lua
local auto_mode = maki.api.register_session_option({
  id = "bash.auto_mode",
  name = "Bash auto mode",
  description = "Classify each bash command before execution",
  category = "mode",
  values = {
    { value = "disabled", name = "Disabled" },
    { value = "enabled", name = "Enabled" },
  },
  initial_value = opts.auto_mode and "enabled" or "disabled",
  on_change = function(value)
    bh.set_auto_mode(value == "enabled")
  end,
})
```

The `/automode` handler then calls the handle's explicit setter rather than writing module state directly. Registration validation is a programmer-error boundary. Runtime setter failures follow Maki's Lua `(value, err)` convention. A call with no value result returns `(true, nil)` on success.

This API shape is speculative. The implementation session must settle ownership, callback lifecycle, reload behavior, and rollback before fixing the public signature.

## Recommended architecture

### Session-option registry

Introduce a frontend-neutral per-session registry for persistent selectable state. A useful conceptual split is:

```text
SessionOptionDefinition
  id
  name
  description
  category
  ordered selectable values

SessionOptionState
  definition
  current value
  set(explicit value)

SessionOptionSnapshot
  ordered full set of definitions and current values
```

The registry needs these operations:

- Register or replace an option by stable ID.
- Remove plugin-owned options when their registration lifetime ends.
- Read an ordered full snapshot.
- Set a value explicitly and return a domain error.
- Notify watchers after a successful state or definition change.

The registry should publish only committed state. If a plugin callback fails, keep the old value and do not publish a misleading snapshot.

ACP adapts each snapshot to `SessionConfigOption`. New-session and load-session responses include the initial snapshot. A watcher emits `ConfigOptionUpdate` after slash-command changes, direct selector changes, model discovery, plugin registration, plugin removal, and dependent changes such as Fast being cleared by a model change.

The TUI does not need to copy ACP presentation. It must use the same underlying setters for slash commands and existing controls so frontend state cannot diverge.

### Separate effect channels

Keep these concepts separate:

```text
persistent session state  -> SessionOptionRegistry snapshots
transient command feedback -> typed frontend event
long-running local work    -> progress events
primary model turn         -> AgentTurn
isolated model turn        -> explicit isolated-turn request
```

Do not make `Completed` carry an unstructured list of arbitrary effects. A typed working-directory result or frontend event is reasonable because it describes one transient effect. Compaction progress belongs on the event stream. `/btw` needs a distinct execution request because its history and tool semantics differ from `AgentTurn`.

### Working-directory source of truth

Change the directory control reply to return the canonical `PathBuf`:

```text
InteractiveControl::ChangeDirectory
  request: PathBuf
  reply: Result<PathBuf, String>
```

After success, update the shared live directory with the returned path. The agent working directory, permission manager, session store, command context, ACP feedback, and tool-location translation must all read the same value.

The ACP event pump must not retain a startup-only copy. It can read a shared directory handle at each translation or receive a typed directory-change event that updates local pump state.

### Isolated-turn runtime

Factor the provider-level semantics used by `maki-ui/src/app/btw.rs` into a frontend-neutral agent service. The TUI can continue rendering a modal. ACP can translate the same stream to agent-message chunks.

The service should accept copied history, current provider and model, resolved system text, images, cancellation, and an output event sink. It should enforce an empty tool list and return no history mutation.

Do not emulate `/btw` by submitting a specially worded normal `AgentInput`. A prompt alone cannot guarantee that tools are absent or that the primary history remains unchanged.

## Open design questions

### Registry ownership and crate placement

`maki-commands` owns command metadata and dispatch but currently has no dependency on agent state or Lua. `maki-agent` owns the live session state and is the likely home for session-option runtime behavior. `maki-lua` must register plugin options through per-state app data. The implementation should decide whether definitions live in a small neutral crate or whether `maki-agent` owns the complete registry and exposes a narrow handle to `maki-lua`.

The choice must avoid a dependency cycle and must not make ACP types part of the core abstraction.

### Option lifetimes across plugin reloads

The command registry already tracks producer replacement and dynamic projection. Session options need equivalent ownership. A plugin reload can replace an option definition, preserve a compatible current value, reset from plugin configuration, or remove the option after failure.

The expected behavior is not settled. Decide and test it before exposing the Lua API. A registration guard tied to the Lua plugin generation is preferable to permanent global state.

### Persistence across session load

Current session state restores some values inconsistently. `RESTORED_FAST` is false, Workflow starts false, YOLO comes from process parameters, and Bash auto mode comes from plugin configuration. Model restore also needs review when the option registry becomes the source used by load responses.

Decide which options are session-persistent and which are process or plugin configuration. Do not accidentally claim persistence merely because ACP shows a selector.

### Fast availability

Fast can be enabled only for models that support it. Options include:

- Always expose Enabled and Disabled, then reject Enabled for unsupported models.
- Expose only Disabled when unsupported.
- Omit Fast while unsupported and add it after a compatible model selection.

The first option keeps layout stable but permits a rejected selection. The latter options make the valid set accurate but cause projection churn. Match current slash-command validation and test Zed's presentation before deciding.

### Option IDs and ordering

Stable IDs and order are public protocol behavior. A plausible initial order is Model, YOLO, Fast, Workflow, Bash auto mode. Model has its own category. The remaining order gives general agent controls priority over a tool-specific control.

Potential IDs include `model`, `yolo`, `fast`, `workflow`, and `bash.auto_mode`. Confirm naming rules and collision policy for third-party plugins.

### ACP update ordering

For a slash command that changes an option, choose and test an ordering between `ConfigOptionUpdate` and the final `PromptResponse`. The client should observe the new value by prompt completion.

For `/cd`, choose whether confirmation uses an agent-message chunk or another visible ACP representation. For `/compact`, define stable tool-call IDs and the exact sequence for start, success, failure, and prompt completion.

### `/btw` system text

The TUI builds a Build-mode system prompt from live prompt slots and publishes it through `btw_system`. The headless ACP runtime does not currently expose the same prepared value. The isolated-turn service needs a frontend-neutral way to obtain equivalent system text without coupling ACP to TUI state.

### TUI adoption scope

The underlying setters must be shared. A new generic TUI option selector is optional unless needed by the implementation. Do not delay ACP correctness by redesigning the TUI settings interface.

## Implementation sequence

1. Start a new branch from current `mistress`. Confirm that the command-registry work is present and that no projection implementation from this investigation exists.
2. Add regression tests that capture the current failures before changing architecture. Include `/new` not being ACP-dispatchable, `/model` updating the selector, and state options sharing setters.
3. Split command capabilities or availability so `/compact` remains portable while `/new` and `/clear` are omitted. Verify a manually typed `/new` cannot clear history.
4. Design and implement the per-session option registry with explicit setters for Model, YOLO, Fast, and Workflow. Make model-dependent Fast changes transactional and observable.
5. Adapt ACP session creation, loading, direct config setting, model discovery, and option watching to full ordered snapshots. Remove the model-only direct mutation path.
6. Add the Lua session-option registration primitive. Migrate Bash auto mode and `/automode` to it. Cover plugin registration, replacement, removal, callback failure, and reload.
7. Change `/cd` to return its canonical path. Share the live path with ACP translation and emit visible confirmation.
8. Add compaction progress translation and immediate session-store persistence. Verify failure and cancellation behavior.
9. Extract frontend-neutral isolated-turn execution from the TUI `/btw` path. Route ACP `/btw` through it and preserve images, cancellation, streaming, and history isolation.
10. Update ACP, command, Lua API, and headless documentation. State the client limitations for transcript clearing and displayed working-directory changes.
11. Run targeted crate tests during each step, then run the repository-wide checks from `AGENTS.md`.

The option-registry design should be reviewed before steps 4 through 6 become a public Lua API. The remaining command fixes can proceed after the core event and lifecycle boundaries are agreed.

## Acceptance tests

### Command availability

- ACP initial and dynamic command projections omit `/new` and `/clear` while retaining `/compact`.
- A literal `/new` sent through ACP does not mutate primary history or replace the session ID.
- TUI availability and behavior for `/new` and `/clear` remain unchanged.

### Session options

- New-session and load-session responses contain the full ordered option set with correct categories and current values.
- Direct `session/set_config_option` and slash commands call the same setter for Model, YOLO, Fast, Workflow, and Bash auto mode.
- `/model <spec>` emits a full config-option update with the selected model before prompt completion.
- A model change that disables Fast publishes both resulting values in one coherent snapshot.
- Invalid option IDs, invalid values, policy-rejected models, and unsupported Fast requests leave state unchanged and return useful errors.
- Model discovery preserves the current model and all non-model options while expanding the model values.
- Plugin option registration, replacement, removal, and failed callbacks publish only valid full snapshots.
- `/automode` selector changes affect Bash permission classification, and `/automode` slash changes update the selector.

### Working directory

- `/cd` stores and reports the canonical path.
- Later command context resolves from the new path.
- Later ACP tool-call locations resolve relative paths from the new path.
- Permission state and the persisted session directory use the same path.
- A failed `/cd` changes none of those values and reports the failure.
- ACP emits visible confirmation without appending it to primary model history.

### Compaction

- `/compact` emits a started tool call and a completed or failed update.
- The ACP prompt completes only after compaction finishes.
- Successful compaction persists the replaced history before prompt completion.
- A reloaded session uses the compacted history even when no normal turn followed compaction.
- Zed's old transcript remains visible, and the progress text does not claim otherwise.

### Side questions

- `/btw` supplies copied primary history and closes dangling tool calls only in the copy.
- The provider request has an empty tool list.
- The current provider, model, system text, and image attachments are used.
- Text streams through the active ACP prompt and the prompt completes on provider completion.
- Provider failure and cancellation terminate the ACP prompt cleanly.
- Primary history is byte-for-byte unchanged after success, failure, and cancellation.
- The next normal agent turn does not contain the `/btw` question or answer.

### Regression and client verification

- Existing registry precedence, custom commands, MCP prompts, image preservation, unknown slash forwarding, and stale-target protections continue to pass.
- Existing TUI behavior for model changes, toggles, `/cd`, `/compact`, `/new`, and `/btw` continues to pass.
- A recorded Zed test verifies option visibility, option updates after slash commands, compaction progress, `/cd` confirmation, and isolated `/btw` output.

## Likely code paths

Core command contracts and behavior:

- `maki-commands/src/dispatch.rs`
- `maki-commands/src/spec.rs`
- `maki-agent/src/command.rs`
- `maki-agent/src/headless.rs`
- `maki-agent/src/types.rs`
- `maki-agent/src/agent/compaction.rs`
- `maki-agent/src/agent/history.rs`

ACP projection and lifecycle:

- `maki-acp/src/server.rs`
- `maki-acp/src/methods.rs`
- `maki-acp/src/translate.rs`
- `maki-acp/src/lib.rs`

TUI semantics to preserve or extract:

- `maki-ui/src/app/btw.rs`
- `maki-ui/src/agent/agent_loop.rs`
- `maki-ui/src/app/mod.rs`

Lua option registration and Bash migration:

- `maki-lua/src/api/tool.rs`
- `maki-lua/src/api/util/command.rs`
- related Lua state app-data and plugin-publication code found during implementation
- `plugins/bash/init.lua`
- `plugins/bash/bash_helpers.lua`
- `plugins/bash/tests/spec.lua`

Documentation:

- `site/docs/content/acp/_index.md`
- `site/docs/content/commands/_index.md`
- generated Lua API and command documentation inputs

## Verification commands

Use the cheapest affected checks while iterating:

```text
cargo check -p maki-commands --tests
cargo check -p maki-agent --tests
cargo check -p maki-acp --tests
cargo check -p maki-lua --tests
cargo check -p maki-ui --tests
cargo nextest run -p maki-commands
cargo nextest run -p maki-agent
cargo nextest run -p maki-acp
cargo nextest run -p maki-lua
cargo nextest run -p maki-ui
just gen-docs-check
```

After targeted checks pass, run the repository checks required by `AGENTS.md`:

```text
just check
just lint
just test
```

Client behavior cannot be proved by Rust tests alone. Repeat the Zed scenarios and retain the relevant ACP messages or a concise test record.

## Non-goals

- Add a client-specific Zed extension for transcript clearing or directory changes.
- Upgrade ACP solely to avoid designing Maki's state ownership.
- Treat slash-command argument hints as structured autocomplete.
- Put ACP schema types in the command or agent core.
- Infer plugin state from flash text or completed command handlers.
- Append command feedback to primary model history.
- Replace every TUI settings control with a generic selector.
- Build a monolithic command-effect protocol that combines state, feedback, progress, and model turns.

## Primary risks

The largest design risk is creating two state owners. `SessionCommandState`, `PermissionManager`, plugin module state, ACP response builders, and the future registry must not each cache independent current values.

The largest protocol risk is claiming unsupported client effects. ACP can report a changed backend directory and compacted model context, but it cannot force Zed to replace its displayed directory or transcript.

The largest lifecycle risk is plugin reload. A stale Lua setter callback must not survive after its plugin generation is gone, and a failed replacement must not remove the last valid option state without an explicit policy.

The largest testing risk is asserting only JSON emission. Zed may rank or hide multiple `mode` options. Wire-level tests and one client-level verification are both required.
