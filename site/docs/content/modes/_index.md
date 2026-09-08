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
- **plan `[PLAN]`** - analyse and plan. Writes are locked to a single plan
  file, and the model gets a directive telling it never to touch anything else.

Switching to plan mode allocates a plan file under `plans/`. The `write` and
`edit` tools only allow edits to that file while in plan mode.

## What a mode is

Under the hood a mode is a definition in a shared registry:

- **name** (`"build"`, `"plan"`, or a custom id) and a **label** for the badge.
- **system_prompt** - a snippet appended to the system prompt, like the plan
  directive. `{plan_path}` and the other prompt variables are filled in.
- **restrict_write_to** (optional) - when set, every non-matching write is
  blocked, exactly like the plan-file-only rule.
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
  file, swaps the toolset to read tools plus `webfetch`,
  `write`/`edit`/`plan_submit`, and adds
  `/plan` and `/build` slash commands. The directive and the plan reviewer splice
  one shared plan specification, so both always see the exact same document.
- `plan_submit_tool` is a mode-scoped tool: it prints the finished plan inline
  as a **display-only** message (kept out of your context) and surfaces the plan
  review form, with **accept** (hands off to implementation), **refine** (keep
  planning), or **cancel**. It only exists in plan mode because plan's toolset
  lists it. While `plan_submit` is in an active mode's toolset, the built-in
   auto-hooks that open the review form on a plan-file write are skipped; the
   model calls `plan_submit` explicitly when the plan is ready. The review form
   shows the current implementation model as a selectable row. Press `Ctrl+M`
   or select that row to search for another model. A choice is staged without
   changing the active model or recents; Escape cancels it. Approving the plan
   applies the selected model and keeps it as the session model.

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