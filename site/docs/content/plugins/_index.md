+++
title = "Plugins"
weight = 23
[extra]
group = "Guides"
+++

# Writing makima plugins

Makima plugins are plain Lua files (Luau) that run inside makima. A plugin can
register tools the LLM calls, slash commands, keymaps, prompt hints, and
custom UI. Everything lives under the global `maki` table. The full API
reference is at the end of this document.

## Where plugin code goes

Plugins live in the makima config dir. There are two of them, same layout:

- `~/.config/makima/` - global, every project
- `<project>/.makima/` - this project only

```
init.lua        the only file makima runs; require()s plugins, calls maki.setup()
lua/<name>.lua  plugin modules, loaded by require("<name>")
plugin.toml     permission grants for every Lua file in the dir
```

Nothing under `lua/` loads on its own. A module name is its path under `lua/`
without the extension: `lua/browser.lua` is `require("browser")`,
`lua/acme/tools.lua` is `require("acme.tools")`. `require` is sandboxed to
that directory, you cannot reach files outside it.

## Creating a plugin

1. Write the code in `~/.config/makima/lua/<name>.lua`. The `maki` global is
   already there, nothing to import. For a project-only plugin use
   `<project>/.makima/` here and in every step below.

```lua
maki.api.register_tool({
  name = "hello",
  description = "Say hello to a name.",
  parameters = { type = "object", properties = { name = { type = "string" } }, required = { "name" } },
  handler = function(args)
    return { llm_output = "hello " .. args.name }
  end,
})
```

2. Load it from `~/.config/makima/init.lua`, creating that file if missing:

```lua
require("hello")
```

3. Grant the permissions it needs in `~/.config/makima/plugin.toml`, creating
   that file if missing. Without the file every gated call is denied.

```toml
[permissions]
fs_read = true
run = true
```

4. Run `/reload`, then read the log as described below, to see that it loaded
   and what it printed.

Leave `maki.api.register_options` to bundled plugins: maki rejects a
`plugins.<name>` table for a plugin it does not ship, and startup fails. Keep
settings in a local table, or export a `setup(opts)` function `init.lua` calls.

## Permissions and plugin.toml

Sensitive APIs are gated per plugin file, and a plugin without a
`plugin.toml` next to it gets nothing. The gates and the file format are
in [the reference](/docs/lua-api/#plugin-permissions).

## Development loop

`/reload` rebuilds plugins and config in place, no restart needed. Until it
runs, an edited plugin is still the old one.

To debug, add `maki.log.info|warn|error(...)` calls. They write to `maki.log`
in the dir `maki.env.logs_dir()` returns (Linux: `~/.local/logs/makima/`). The
log keeps `info` and above. Set `MAKI_LOG=debug` to also keep `maki.log.debug`,
or `MAKI_LOG=maki_lua=trace` to narrow it to one target. When a backtrace comes
out useless, start makima with `--no-jit`: plugins then run on the interpreter,
with full debug info.

## Conventions

- Fallible runtime calls return a `(value, err)` pair; check `err` before using `value`.
- Tool handlers report failures with `{ llm_output = "error: ...", is_error = true }`, not by raising.
- The model picks tools by reading `description`, so state precisely what the tool does and when to use it.
- Reusable helpers ship with makima; see "Shared helper modules" in the API reference.

## Agent context and lifetime

Tool handlers receive `handler(input, ctx)`. Slash-command handlers receive `handler(opts, ctx)`. Keymap callbacks receive `callback(nil, ctx)`. A command or keymap context can be nil when the host has no agent target. Callbacks may omit the second argument.

`ctx:agent()` resolves the invocation's agent. A tool context refers to the executing agent, not UI focus. A trusted command context pins its target. A keymap context is pinned to the agent selected when the key was pressed, so later focus changes do not retarget it. If the pinned agent closes, the context keeps its plugin authority: `maki.agent.root`, `list` and `get` still work, and only calls that address the closed agent, such as `ctx:agent()` or `spawn` under it, fail. `maki.ui.focused_agent()` resolves the agent that has UI focus at call time as a visibility-only reference; pass it to `maki.agent.get(ctx, ref)` for control. `agent:defer_close(ctx)` registers authorized host-held cleanup at callback exit, including cancellation. Context authority expires when its invocation ends or is cancelled, or when its plugin generation is revoked. Storing a context for a later handler does not extend that authority.

In a tool handler, `ctx:model()` reads the selection admitted for the executing turn. `agent:config(ctx).model` reads the agent's committed selection. These can differ after a configuration update while the turn is running. Both selections contain `spec`, `thinking`, and `fast`; descriptive model metadata is separate. `ctx:mode()` and `agent:config(ctx).mode` differ the same way.

`Agent` handles do not close their agents when garbage-collected. Children outlive the turn that spawned them; [Agent work](/docs/agent-work/#cancellation) defines which cancellations and closes reach them. `AgentSubscription` handles, from the `Agent:on_*` aliases for an agent-filtered autocmd, do not unsubscribe when garbage-collected. A subscription requires trusted authority; delegated tool handlers cannot register one. Call `subscription:close()` to revoke future deliveries and cancel active handlers. Plugin unload also revokes registrations. Delegated tool handlers cannot enter the trusted `maki.api.run_command()` route. They also cannot register commands, keymaps, autocmds, completion sources, expanders, usage callbacks, notify handlers, slots, timers, or deferred callbacks, because those later run with host authority. Mutators of the global mode registry, such as `maki.api.mode.define`, refuse them as well, and session mutators accept only the tool's own session. Callbacks a delegated tool fires, such as autocmds from `exec_autocmds`, run with the same delegated restrictions. These checks confine what a delegated invocation can register. They do not sandbox the plugin's own code, which other callbacks of the same plugin can still call.

`Agent:send(ctx, message, { when = ... })` is the one way to start a turn, and returns the ticket of the turn that carries the message. `when = "idle"` requires an idle agent, `"queue"` queues a distinct FIFO turn, and `"join"` uses the shared [message queue](/docs/agent-work/#messages-and-turns): it starts work or joins compatible work. Wait for the result with `ticket:wait(ctx, { timeout })`; a timeout stops the wait, not the turn.

### Focused-agent UI

UI agent actions keep the target selected when the action starts. `maki.session.*` retains session scope. See [Agent work](/docs/agent-work/#focused-controls) for shared input, working directories, closed targets, and pending-message controls.

### Roots in other frontends

Print, SDK and ACP roots are managed agents in the same graph as the TUI, so every `Agent` operation, including child spawn, works the same way there. Root model, mode and working-directory changes go through the session coordinator in all three. SDK and ACP persist them with the session; print applies them and saves nothing. Concurrent ACP root prompts are rejected.

The [Lua API reference](/docs/lua-api/) defines the method signatures and returned fields. The Context concepts page describes model context windows, not Lua invocation authority.

## A complete real example

The bundled `glob` tool, verbatim: schema, header and restore hooks, error
handling, LLM output truncation, collapsible UI view. It is a bundled plugin,
so it opens with `register_options`, which your own plugin skips:

```lua
local ToolView = require("maki.tool_view")
local shorten_path = require("maki.shorten_path")
local output_limits = require("maki.output_limits")

local NO_FILES_FOUND = "No files found"

local opts = maki.api.register_options(output_limits.extend({
  search_result_limit = { default = 100, min = 10, desc = "Max files returned per search." },
}))

local function glob_view_opts(ctx)
  local tol = ctx:tool_output_lines()
  return { max_lines = (tol and tol.other) or 3, keep = "head" }
end

maki.api.register_tool({
  name = "glob",
  kind = "search",
  description = [[Find files by glob pattern.

- Respects .gitignore.
- Returns absolute paths sorted by modification time (newest first).
- Prefer speculative parallel searches over sequential rounds of glob+grep.]],

  schema = {
    type = "object",
    properties = {
      pattern = { type = "string", description = "Glob pattern (e.g. **/*.rs, src/**/*.ts)", required = true },
      path = { type = "string", description = "Directory to search in (default: cwd)" },
    },
  },

  header = function(input)
    local buf = maki.ui.buf()
    local spans = { { shorten_path(input.pattern or ""), "tool" } }
    if input.path then
      spans[#spans + 1] = { " in ", "dim" }
      spans[#spans + 1] = { shorten_path(input.path), "path" }
    end
    buf:line(spans)
    return buf
  end,

  restore = function(_input, output, _is_error, ctx)
    return ToolView.restore(output, glob_view_opts(ctx))
  end,

  handler = function(input, ctx)
    local pattern = input.pattern
    if not pattern then
      return { llm_output = "error: pattern is required", is_error = true }
    end

    local limit = opts.search_result_limit
    local max_lines, max_bytes = output_limits.resolve(opts, ctx)
    local path, path_err = ctx:resolve_path(input.path or ".")
    if not path then
      return { llm_output = "error: " .. tostring(path_err), is_error = true }
    end

    local files, err = maki.fs.glob(pattern, {
      path = path,
      gitignore = true,
      sort = "mtime",
      limit = limit,
    })

    if not files then
      return { llm_output = "error: " .. err, is_error = true }
    end

    if #files == 0 then
      return { llm_output = NO_FILES_FOUND }
    end

    local lines = {}
    for i, f in ipairs(files) do
      lines[i] = shorten_path(f)
    end
    local text = table.concat(lines, "\n")
    local llm_output = text

    local buf = maki.ui.buf()
    local view = ToolView.new(buf, glob_view_opts(ctx))
    for _, line in ipairs(lines) do
      view:append(line)
    end
    view:finish()
    buf:on("click", function()
      view:toggle()
    end)

    return {
      llm_output = llm_output,
      output_limits = { max_lines = max_lines, max_bytes = max_bytes },
      body = buf,
    }
  end,
})
```

## Full API reference

Every module, function, and method is in the [Lua API reference](/docs/lua-api/).
The agent gets the same document on disk through the builtin
`maki-plugin-dev` skill, so asking it to write a plugin for you works
without pasting any of this.
