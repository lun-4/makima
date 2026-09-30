+++
title = "Modes"
weight = 3
[extra]
group = "Concepts"
+++

# Modes

Makima ships with two agent modes: **build** and **plan**. A mode bundles a badge
in the input bar, a system-prompt snippet the model follows, and, optionally, a
write restriction and its own visible toolset. This page shows what the built-in
modes do, and how a Lua plugin defines a new mode or overrides a built-in one.

## The built-in modes

Tab toggles between them (build is the default).

- **build `[BUILD]`** - the default. Full toolset, no restrictions.
- **plan `[PLAN]`** - analyse and plan. The mode names one plan file as its
  intended write target, and the model gets a directive not to change other files.

Switching to plan mode allocates a plan file under `plans/`. Native write and edit tools can change only the designated plan file. The host checks normalized target paths, including arguments changed by input hooks. A custom mode can explicitly offer third-party local tools; tool origin does not determine whether a mode offers them. MCP execution and deferred MCP search remain unavailable in write-restricted modes, even with YOLO enabled.

A mode is not a sandbox for plugin effects. Lua filesystem writes require the plugin's `fs_write` permission and are not restricted to the plan path. Tool selection and native write-target checks do not constrain every effect of a selected plugin.

## Plan approval

The review form shows the current implementation model as a selectable row. The model-picker shortcut or Enter on that row opens the searchable model picker. Selecting a model stages it without changing the session model or saving a preference. Selecting the current model clears the staged selection. When a different model is staged, `Use current model` also clears it.

Escape in the picker returns to the form without changing its selection or parallel setting. Editing, refining, rewriting, or dismissing and reopening the plan preserves the staged model. Leaving Plan mode, resetting the form, or replacing the session clears it.

`Implement plan` keeps the current conversation. `Clear context and implement` prepares a fresh Build session with empty conversation history. Both paths require idle planning work and idle managed descendants. Busy approval reports a retry message without cancelling existing work. Implementation receives the captured approved plan text, so later file edits do not change its instructions.

While approval is preparing, form submission, editing, and model staging are locked. Escape cancels preparation and unlocks the form. Provider construction and prompt readiness finish before approval commits. A preparation failure leaves the original planning session, plan, history, and staged selection available for retry. Fresh-context approval prepares the new runtime and its first admission before replacing the planning runtime.

Successful approval commits the implementation model, normalized options, Build mode, and one implementation turn. The selected model remains the session model. Fresh-context approval displays the approved plan and implementation input in the new session. Cancellation or execution failure after successful approval does not restore the planning session or previous model. Saving follows [Configuration changes](#configuration-changes).

Cancellation releases the approval wait. It does not undo external prompt requests or authentication subprocesses that have already started.

## Configuration changes

Managed TUI roots and Lua agent sessions own their model, thinking, fast, workflow, and mode configuration. Changes apply in order to later admitted work. An active or already admitted turn retains its provider, settings, resolved mode definition, prompt inputs, and tool bindings. A mode registry change does not replace the definition captured for that turn. Selecting an undefined custom mode fails.

A managed setter succeeds when the actor commits the change. The actor publishes the committed configuration to the UI, provider slot, session metadata, and storage writer. Session saving runs asynchronously and does not block admission or the implementation turn. If saving fails, the committed configuration remains active and a warning reports that the configuration was applied but not saved. The storage writer retains the latest committed snapshot for retry. A successful retry clears the pending-save condition. A crash before saving can lose the latest configuration change. A save failure never rolls back an actor commit.

Headless, print, and ACP execution retain their existing frontend-owned initialization and configuration paths. They do not yet use the managed actor execution loop. YOLO and plugin options retain their existing owners; YOLO changes affect permission checks immediately.

## What a mode is

Under the hood a mode is a definition in a shared registry:

- **name** (`"build"`, `"plan"`, or a custom id) and a **label** for the badge.
- **system_prompt** - a snippet appended to the system prompt, like the plan
  directive. `{plan_path}` and the other prompt variables are filled in.
- **restrict_write_to** (optional) - names the only write target for bundled native write and edit tools. The host compares normalized path keys before allowing those tools to write. Lua filesystem writes are not restricted to this path; they require the plugin's `fs_write` permission.
- **tools** (optional) - when set, the model sees *only* this exact toolset for
  that mode. When absent, the mode inherits the default (build) set. This is how
  a tool like `plan_submit` exists only while you are in plan mode.

The built-in `build` and `plan` are pre-registered entries. Overriding one is
the same call as defining a new mode: it fully replaces the definition.

## Defining and overriding modes from Lua

The registry lives on the API as `maki.api.mode`. Define a mode or override a
built-in with `define`:

```lua
maki.api.mode.define({
  name = "audit",                 -- a new custom mode
  label = "[AUDIT]",
  system_prompt = [[You only review code. You never change it.]],
  restrict_write_to = "audit.md",
  tools = { "read", "grep", "glob", "write", "edit" },
})
```

Override the built-in plan mode the same way:

```lua
-- Replaces the built-in plan directive and toolset.
maki.api.mode.define({
  name = "plan",
  label = "[PLAN]",
  system_prompt = function(ctx)
    return "My stricter plan-mode directive, plan file: " .. (ctx.plan_path or "?")
  end,
  tools = { "read", "grep", "glob", "write", "edit", "plan_submit" },
})
```

`system_prompt` may be a string or a function of `{ cwd, plan_path }` returning
a string. Because a definition fully replaces the built-in, a partial override
(for example only `tools`, no `system_prompt`) drops the built-in directive;
supply both when you override.

Other methods:

```lua
maki.api.mode.get()          -- current mode id: "build", "plan", or a custom name
maki.api.mode.set("plan")    -- enter a mode; fails if it is not defined
maki.api.mode.list()         -- all modes as { name, label }
maki.api.mode.reset("plan")  -- drop a plugin override, restore the built-in
maki.api.mode.reset()        -- restore every built-in
```

Switching modes fires the autocmd `ModeChanged` with data `{ mode = "<id>" }`.

## Example: a plan-review workflow

The repositories ship two example plugins that put this together. They are
bundled and enabled by default; disable the ones you don't want from `init.lua`:

```lua
maki.setup({
  plugins = {
    mode_plan_override = { enabled = false },
    plan_submit_tool = { enabled = false },
  },
})
```

- `mode_plan_override` replaces the built-in `plan` mode with a verbatim clone
  of polytoken's plan directive (via the `plan` plugin override). It focuses
  the model on producing a reviewable artifact, restricts writes to the plan
  file, swaps the toolset to `read`, `grep`, `glob`, `webfetch`, `write`,
  `edit`, `plan_submit`, and `task`, and adds `/plan` and `/build` slash commands.
  The directive and the plan reviewer splice
  one shared plan specification, so both always see the exact same document.
- `plan_submit_tool` is a mode-scoped tool: it prints the finished plan inline
  as a display-only message (kept out of the model context) and surfaces the plan review form, with accept (hands off to implementation), refine (keep planning), or cancel. It only exists in plan mode because plan's toolset lists it. While `plan_submit` is in an active mode's toolset, the built-in auto-hooks that open the review form on a plan-file write are skipped; the model calls `plan_submit` explicitly when the plan is ready. The review form uses the same approval workflow as a completed plan-file write.

The built-in `task` tool grows a `plan_reviewer` subagent type when the plan
override is active: a read-only audit that verifies the plan follows the shared
plan specification, maps every acceptance criterion to a named test, and checks
test-infrastructure adequacy before answering `VERDICT: pass|fail`. It is only
spawnable inside plan mode, and `general` subagents are blocked there so plan
work stays read-only. A reviewer finding about a missing test harness is handled
by revising the plan to build the infrastructure, confirmed via the `question`
tool when the gap is out of scope.

With all three enabled, a typical loop is:

1. Switch to plan mode (`/plan`). The model drafts `plan.md` using the cloned
   directive and the reduced toolset.
2. The model calls `task` with `subagent_type = "plan_reviewer"` to audit the
   plan, then iterates until `VERDICT: pass`.
3. The model calls `plan_submit`; the plan prints inline and you accept, refine,
   or concede through the plan review form.
4. Switch to build mode (`/build`) to implement with the full toolset.

## Persistence and other surfaces

The active mode is persisted with the session, so a custom mode survives a
restart (it falls back to build with a warning if its plugin is not loaded
then). Custom modes also appear in the Agent Client Protocol session modes when
the ACP server is started from a live plugin host.