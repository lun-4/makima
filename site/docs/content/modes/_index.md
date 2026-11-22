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

Switching to plan mode allocates a plan file under `plans/`. Native write and edit tools can change only the designated plan file. The host checks normalized target paths, including arguments changed by input hooks. A custom mode can explicitly offer third-party local tools; tool origin does not determine whether a mode offers them.

Plan mode inherits the normal toolset, including command execution, `code_execution`, and MCP tools. The prompt permits read-only investigation, such as `gh issue view`, `git status`, and `git log`. Calls use the normal [permission flow](/docs/permissions/), including configured rules, auto mode, and YOLO. The prompt requires explicit human confirmation before side effects such as builds, package installation, or changes to local or remote state. A permission approval does not establish that a command is read-only.

A mode is not a sandbox for plugin effects. Lua filesystem writes require the plugin's `fs_write` permission and are not restricted to the plan path. Tool selection and native write-target checks do not constrain every effect of a selected plugin.

## Plan approval

When a plan is ready, the plan form opens below the chat. It has three actions:

- `Refine plan` hides the form so you can keep planning with the agent.
- `Implement plan` switches to build mode and implements the plan in the current conversation.
- `Clear context and implement` starts a fresh build session that holds only the plan, then implements it.

Space toggles parallel implementation, which asks the agent to split the work across subagents. Ctrl+O opens the plan in your editor. Esc hides the form, and Ctrl+T shows or hides it.

The `Implementation model` row shows the model that will do the work. It starts as the current session model. Press Enter on that row to open the [model picker](/docs/providers/#model-picker), where you can pick another model and set its thinking and fast options. Nothing changes for the session yet. Once you pick something, a `Use current model` row lets you go back. Options you leave alone follow whatever the agent uses at approval. Your pick stays while you refine or edit the plan, and goes away when you leave plan mode or the agent starts a new plan. The [keybindings](/docs/keybindings/#form) page lists every key in the form.

Approval needs planning work to be finished. If the agent or one of its subagents is still running, Makima asks you to try again later and leaves that work running. While approval is getting ready, the form is locked. Esc cancels it and gives you the form back as it was.

Approval is strict about the handoff turn's setup. If collecting prompt slots fails, say because a plugin stopped, the implementation cannot start and you get the form back. A turn you send yourself in that situation keeps going with default slots and a warning in the log, because planning should not lose its turn to a broken plugin.

After approval, the picked model and options become the session's settings in one step, and the agent starts implementing. If the model is not allowed or does not support an option you turned on, approval stops and you get the form back. The agent gets the plan text as it was at approval, so later edits to the file do not change the task. Cancelling from here stops the implementation like any other turn. It does not bring back the planning session or the old model.

## Configuration changes

The model, thinking, fast, workflow, and mode settings belong to each agent, so changing the viewed subagent leaves the main agent alone. You change them with `/model`, `/thinking`, `/fast`, `/workflow`, Tab, a plan approval, or a plugin. When you pick a model that does not support thinking or fast, those turn off.

A change never touches a turn that has already started. The running turn finishes with the model, settings, mode, and tools it started with. A message you send while the agent works usually joins the running turn, but not after a change: then it waits for the running turn to finish and starts its own turn with the new settings. SDK and ACP messages behave the same as typing in the TUI. Messages that were already in the queue keep the settings they were queued with. The same is true when a plugin redefines a mode: running and queued turns keep the old definition.

Makima saves the change with the session before it applies it. If the save fails, nothing changes and you see an error. A change a plugin requested is also dropped when the plugin reloads or its call ends before the save starts. Once the save has started, the change always applies.

The TUI, print, SDK, ACP and Lua agent sessions all follow these rules. One-shot print runs apply changes the same way but do not save the session. YOLO and plugin options belong to the session, not to one agent: they sit outside this, `Agent:set_option` sets them only through the root agent and refuses them on a subagent, and a YOLO change applies to permission checks right away.

## What a mode is

Under the hood a mode is a definition in a shared registry:

- **name** (`"build"`, `"plan"`, or a custom id) and a **label** for the badge.
- **system_prompt** - a snippet appended to the system prompt, like the plan
  directive. `{plan_path}` and the other prompt variables are filled in.
- **restrict_write_to** (optional) - names the only write target for bundled native write and edit tools. The host compares normalized path keys before allowing those tools to write. Lua filesystem writes are not restricted to this path; they require the plugin's `fs_write` permission.
- **tools** (optional) - when set, the model sees *only* this exact toolset for
  that mode. When absent, the mode inherits the default (build) set. This is how
  a custom mode can limit the tools available to the model. Tools can also enforce their own mode requirements, as `plan_submit` does.

The built-in `build` and `plan` are pre-registered entries. Overriding one is
the same call as defining a new mode: it fully replaces the definition.

## Defining and overriding modes from Lua

The registry lives on the API as `makima.api.mode`. Define a mode or override a
built-in with `define`:

```lua
makima.api.mode.define({
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
makima.api.mode.define({
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
local agent = ctx:agent()  -- in a command, keymap or tool handler
agent:config(ctx).mode       -- the agent's mode id: "build", "plan", or a custom name
agent:set_mode(ctx, "plan")  -- enter a mode; fails if it is not defined
makima.api.mode.list()         -- all modes as { name, label }
makima.api.mode.reset("plan")  -- drop a plugin override, restore the built-in
makima.api.mode.reset()        -- restore every built-in
```

Every committed config change of an agent fires the autocmd `ConfigChanged`, with the agent's mode in `ev.data.mode`. Compare it with the last value you saw to react to mode switches.

## Example: a plan-review workflow

The repositories ship two example plugins that put this together. They are
bundled and enabled by default; disable the ones you don't want from `init.lua`:

```lua
makima.setup({
  plugins = {
    mode_plan_override = { enabled = false },
    plan_submit_tool = { enabled = false },
  },
})
```

- `mode_plan_override` replaces the built-in `plan` directive with one based on polytoken's planning workflow. It focuses the model on producing a reviewable artifact, restricts native writes to the plan file, inherits the normal toolset, and adds `/plan` and `/build` slash commands. The directive and the plan reviewer splice one shared plan specification, so both always see the exact same document.
- `plan_submit_tool` prints the finished plan inline as a display-only message (kept out of the model context) and opens the [plan form](#plan-approval). Its handler rejects calls outside plan mode. While `plan_submit` is available in the active plan mode's toolset, the built-in auto-hooks that open the plan form on a plan-file write are skipped; the model calls `plan_submit` explicitly when the plan is ready. The form works the same way as after a plan-file write.

The built-in task tools grow a `plan_reviewer` subagent type when the plan
override is active: a read-only audit that verifies the plan follows the shared
plan specification, maps every acceptance criterion to a named test, and checks
test-infrastructure adequacy before answering `VERDICT: pass|fail`. It is only
spawnable inside plan mode, and `general` subagents are blocked there so plan
work stays read-only. A reviewer finding about a missing test harness is handled
by revising the plan to build the infrastructure, confirmed via the `question`
tool when the gap is out of scope.

With both enabled, a typical loop is:

1. Switch to plan mode (`/plan`). The model investigates with the normal toolset and drafts the designated plan file.
2. The model calls `task_spawn` with `subagent_type = "plan_reviewer"` to audit
   the plan, collects the verdict with `task_wait`, then iterates until
   `VERDICT: pass`.
3. The model calls `plan_submit`; the plan prints inline and you accept, refine,
   or concede through the plan review form.
4. Switch to build mode (`/build`) to implement with the full toolset.

## Persistence and other surfaces

The active mode is persisted with the session, so a custom mode survives a
restart (it falls back to build with a warning if its plugin is not loaded
then). Custom modes also appear in the Agent Client Protocol session modes when
the ACP server is started from a live plugin host.