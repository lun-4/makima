+++
title = "Lua API"
weight = 10
[extra]
group = "Reference"
+++

# Lua API

Makima plugins are plain Lua files. Everything a plugin can touch lives under
one global table: `makima`. This reference documents every module, function,
and method. It is generated straight from the source code by `makima-docgen`.
For where plugin files live and how to load them, read the
[Plugins guide](/docs/plugins/) first.

The API tries to mirror Neovim as much as possible (`makima.fs`, `makima.uv`,
`makima.treesitter`, `makima.keymap`, `makima.base64`), signatures are kept identical
so code can be copy-pasted between the two without too many modifications.

Plugins run compiled to native code (Luau JIT). If you are debugging a
plugin and want full backtraces, start makima with `--no-jit`: it runs your
Lua on the interpreter with complete debug info instead.

A small plugin looks like this:

```lua
makima.api.register_command({
  name = "greet",
  description = "Say hello from Lua",
  tui_only = false,
  handler = function()
    makima.ui.flash("hello from a plugin!")
  end,
})
```

## How to read this reference

Signatures use Neovim notation: `{path}` is a required argument, `{opts?}`
is optional, and `{...}` is variadic.

One convention to remember: fallible runtime operations return a
`(value, err)` pair instead of throwing. Check `err` before using `value`:

```lua
local text, err = makima.fs.read("config.json")
if err then
  makima.log.error("read failed: " .. err)
  return
end
```

Lua errors are reserved for programmer mistakes, like passing a number where
a string belongs.

## Permissions and plugin.toml {#plugin-permissions}

Sensitive APIs are gated per plugin file; every gated function's entry in
this reference names the permission it needs. The permissions are: `fs_read`, `fs_write`, `net`, `run`, `env`.
A gated call without its permission raises
`permission denied: '<name>' not granted for this plugin`.

Grants come from a `plugin.toml` next to the Lua file (for
`~/.config/makima/init.lua` that is `~/.config/makima/plugin.toml`):

```toml
[permissions]
fs_read = true
fs_write = true
net = true
run = true
env = true
```

The rules:

- No `plugin.toml` at all: every permission is denied, and makima logs a
  warning at load time.
- `plugin.toml` exists: permissions default to granted; set a key to
  `false` to revoke it. An empty file grants everything.
- Invalid TOML, an unknown key under `[permissions]` or a value that is not
  a boolean: everything denied, with a warning in the log.

## Overview

| Module | What it is for |
| --- | --- |
| [`LuaCtx`](#LuaCtx) | Host-issued invocation context. |
| [`makima`](#makima) | The global entry point. |
| [`makima.api`](#makima-api) | Plugin registration. |
| [`makima.perf`](#makima-perf) | Performance instrumentation for splashes and the UI. |
| [`makima.store`](#makima-store) | Shared key-value store for plugin contributions. |
| [`makima.state`](#makima-state) | Journaled plugin slots, scoped to an agent or session. |
| [`makima.agent`](#makima-agent) | Subagent primitives for plugins that need to talk to an LLM. |
| [`makima.agent.Agent`](#makima-agent-Agent) | A nonowning agent handle. |
| [`makima.agent.AgentRef`](#makima-agent-AgentRef) | Visibility-only identity. |
| [`makima.agent.AgentSubscription`](#makima-agent-AgentSubscription) | An autocmd filtered to one agent. |
| [`makima.agent.AgentTurn`](#makima-agent-AgentTurn) | An exact-turn ticket with repeatable retained result reads. |
| [`makima.async`](#makima-async) | Tools for running things concurrently in Lua plugins. |
| [`makima.async.Semaphore`](#makima-async-Semaphore) | A counting semaphore for limiting how many tasks run at once. |
| [`makima.async.Permit`](#makima-async-Permit) | One slot in a semaphore, obtained from `Semaphore:acquire()`. |
| [`makima.base64`](#makima-base64) | Base64 encoding and decoding, modelled after `vim.base64`. |
| [`makima.crypto`](#makima-crypto) | Hashing on binary strings. |
| [`makima.env`](#makima-env) | Paths to makima's own directories (config, state, logs, cache), and whether |
| [`makima.fn`](#makima-fn) | Process and environment helpers, modeled after Neovim's `vim.fn` job |
| [`makima.fs`](#makima-fs) | File-system utilities, modelled after `vim.fs` and `vim.uv`. |
| [`makima.host`](#makima-host) | Actions on the host machine: opening a browser, reading the keyring. |
| [`makima.image`](#makima-image) | Small building blocks for working with images: probe metadata, decode |
| [`makima.image.Image`](#makima-image-Image) | A decoded image you can inspect, resize, and re-encode. |
| [`makima.interpreter`](#makima-interpreter) | Run Python code in a memory-safe, time-limited sandbox. |
| [`makima.json`](#makima-json) | JSON encoding, decoding, and schema validation. |
| [`makima.json.SchemaValidator`](#makima-json-SchemaValidator) | A compiled JSON Schema validator. |
| [`makima.keymap`](#makima-keymap) | Key mappings, modeled after `vim.keymap`. |
| [`makima.log`](#makima-log) | Structured logging for plugins. |
| [`makima.match`](#makima-match) | Fuzzy matching via nucleo, the same matcher makima's built-in pickers use. |
| [`makima.api.mode`](#makima-api-mode) | `makima.api.mode`: define, override and list agent modes. |
| [`makima.model`](#makima-model) | Model catalog reads that address no agent. |
| [`makima.net`](#makima-net) | HTTP clients: a guarded `request` for web content and a raw, streaming |
| [`makima.net.Response`](#makima-net-Response) | A streaming HTTP response from `makima.net.send`. |
| [`makima.session`](#makima-session) | Host session primitives. |
| [`makima.credentials`](#makima-credentials) | Credential stores for plugins. |
| [`makima.credentials.Store`](#makima-credentials-Store) | A credentials file: a provider's (its ctx's `credentials`) or a |
| [`makima.Ctx`](#makima-Ctx) | A plugin's authority. |
| [`makima.provider.LoginUi`](#makima-provider-LoginUi) | The `ui` a provider's `login(ctx, ui, config)` receives. |
| [`makima.usage`](#makima-usage) | Provider-side quota snapshots. |
| [`makima.text`](#makima-text) | Text transformation utilities. |
| [`makima.time`](#makima-time) | Wall-clock timestamps and relative-age formatting. |
| [`makima.timer`](#makima-timer) | Recurring callbacks on the runtime's timer pump. |
| [`makima.treesitter`](#makima-treesitter) | Tree-sitter parsing and query API. |
| [`makima.treesitter.language`](#makima-treesitter-language) | Language registry for tree-sitter grammars. |
| [`makima.treesitter.query`](#makima-treesitter-query) | Query compilation and lookup. |
| [`makima.treesitter.Query`](#makima-treesitter-Query) | A compiled tree-sitter query. |
| [`makima.treesitter.Tree`](#makima-treesitter-Tree) | A parsed syntax tree. |
| [`makima.treesitter.Node`](#makima-treesitter-Node) | A single node in a parsed syntax tree. |
| [`makima.treesitter.LanguageTree`](#makima-treesitter-LanguageTree) | Manages parsing of a source string for a single language. |
| [`makima.ui`](#makima-ui) | Functions for building interactive UI. |
| [`makima.ui.Win`](#makima-ui-Win) | Handle to a floating or split window. |
| [`makima.ui.Buf`](#makima-ui-Buf) | A content buffer that holds styled lines of text. |
| [`makima.uv`](#makima-uv) | System and environment utilities, modelled after `vim.uv`. |
| [`makima.yaml`](#makima-yaml) | YAML encoding and decoding. |

## LuaCtx {#LuaCtx}

Host-issued invocation context. Tool contexts have delegated authority over the executing agent and allowed descendants. Command/keymap/event contexts have trusted plugin authority over their pinned host target. Authority expires on invocation exit, cancellation or plugin-generation revocation; retaining userdata does not extend it. Unsupported capabilities return nil and an error.

---

### `LuaCtx:agent()` {#LuaCtx-agent}

```lua
LuaCtx:agent()
```

Resolve the pinned invocation subject, not current UI focus.

**Returns:** ([`Agent?`](#makima-agent-Agent), `string?`) Nonowning handle with no authority independent of this context.

---

### `LuaCtx:model()` {#LuaCtx-model}

```lua
LuaCtx:model()
```

Read the executing turn's admitted selection. This can differ from Agent:config(ctx).model, which reads committed configuration. Trusted command/event contexts without an executing turn return an unsupported-capability error.

**Returns:** (`table?`, `string?`) Complete normalized selection {spec, thinking, fast}; thinking is a setting string or budget integer. Disabled values are off and false.

---

### `LuaCtx:mode()` {#LuaCtx-mode}

```lua
LuaCtx:mode()
```

Read the executing turn's admitted mode. This can differ from Agent:config(ctx).mode, which reads committed configuration. Trusted command/event contexts without an executing turn return an unsupported-capability error.

**Returns:** (`string?`, `string?`) Registered mode name.


## makima {#makima}

The global entry point. Every API lives under this table.

---

### `makima.setup()` {#makima-setup}

```lua
makima.setup({config})
```

Apply your personal configuration. This is only available inside `init.lua` (not in plugins) and can be called at most once. The table accepts the same keys as the Configuration reference.

**Parameters:**

- `{config}` (`table`) Configuration table.

**Example:**

```lua
makima.setup({
model = "opus",
keymaps = false,
})
```

---

### `makima.split()` {#makima-split}

```lua
makima.split({s}, {sep}, {opts?})
```

Split {s} at each occurrence of {sep} and return the pieces as a
list. Mirrors Neovim's `vim.split`, so code using it can be copied
between Neovim and makima. {sep} is a Lua pattern unless `plain` is
set; an empty {sep} splits into single characters.

**Parameters:**

- `{s}` (`string`) String to split.
- `{sep}` (`string`) Separator: a Lua pattern, or literal text with `plain`.
- `{opts?}` (`table?`) Optional settings:
  - `plain` (`boolean?`) treat {sep} as literal text instead of a pattern.
  - `trimempty` (`boolean?`) drop empty pieces from the start and end of the result.

**Returns:** (`table`) List of split pieces.

**Example:**

```lua
makima.split("a,b,c", ",")                   -- { "a", "b", "c" }
makima.split("x*y*z", "*", { plain = true }) -- { "x", "y", "z" }
makima.split("\nhello\nworld\n", "\n", { trimempty = true }) -- { "hello", "world" }
```

---

### `makima.version()` {#makima-version}

```lua
makima.version()
```

Read the current makima version and, if the built-in update check found one, the newer version. Rust owns the update check; plugins only mirror it, so the version text and update notice belong to whoever draws them.

**Returns:** (`table`) { current = string, latest = string|nil, update_available = boolean }

**Example:**

```lua
local v = makima.version()
if v.update_available then
  print("run makima update to get v" .. v.latest)
end
```

---

### `makima.defer_fn()` {#makima-defer_fn}

```lua
makima.defer_fn({callback}, {ms})
```

Run {callback} after {ms} milliseconds, on the Lua thread and outside
any task scope. The timer does not hang off the caller's cancel token
or the 60 second `async.run` deadline, so the callback still fires
once the tool call that scheduled it is over. That is what a toast
needs to dismiss itself, and the difference from `makima.async.sleep`.

You get back a handle. Its `:stop()` cancels a callback that has not
fired yet, which is how you debounce: schedule, then stop and
reschedule on every new event. An error raised by the callback is
logged and dropped, since nobody is waiting for a result. Delegated tool
tasks, including their async children, cannot schedule deferred callbacks.

**Parameters:**

- `{callback}` (`function`) Called with no arguments.
- `{ms}` (`integer`) Delay in milliseconds. Zero fires on the next tick.

**Returns:** string|nil Error on failure.

**Example:**

```lua
-- A toast that dismisses itself 4 seconds later:
local buf = makima.ui.buf({ scratch = true })
buf:line("copied!")
local win = makima.ui.open_win(buf, { split = "right", width = 20, height = 3 })
makima.defer_fn(function() win:close() end, 4000)

-- Repaint only after the user has stopped typing for half a second:
local pending
local function repaint_soon()
  if pending then
    pending:stop()
  end
  pending = makima.defer_fn(repaint, 500)
end
```

---

### `makima.notify()` {#makima-notify}

```lua
makima.notify({msg}, {level?}, {opts?})
```

Show a one line notice. By default it goes to `makima.ui.flash`, with
`{opts.title}` in front of the message when you pass one. A run with
no UI, such as `makima --print` or the sdk, logs the notice instead of
dropping it.

There is one handler for the whole process. Once a plugin calls
`makima.set_notify_handler`, notices from every plugin go through it.
That is how a UI plugin turns flashes into stacked toasts without
any of the callers knowing about it.

{level} reaches the handler untouched, and the default ignores it.

**Parameters:**

- `{msg}` (`string`) Notice text.
- `{level?}` (`string?`) Optional. Severity name such as "info", "warn" or "error".
- `{opts?}` (`table?`) Optional. `title` (string) labels the notice. Free form otherwise.

**Example:**

```lua
makima.notify("saved!")
makima.notify("build failed", "error", { title = "make" })
```

---

### `makima.set_notify_handler()` {#makima-set_notify_handler}

```lua
makima.set_notify_handler({handler})
```

Install the handler that every `makima.notify` call in the process goes
through, in place of the default flash. Pass `nil` to put the default
back.

The handler runs on the Lua thread, so keep it short and hand real
work to `makima.async.run`. If it raises an error, the error is logged
and the notice falls back to `makima.ui.flash`, so the user still sees
it. Unloading the plugin that installed the handler also restores the
default.

**Parameters:**

- `{handler}` (`function|nil`) Handler `function(msg, level?, opts?)`, or nil.

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
local Toast = require("makima.toast")
makima.set_notify_handler(function(msg, level, opts)
  Toast.show(msg, { title = opts and opts.title, level = level })
end)
```


## makima.api {#makima-api}

Plugin registration. This is where you tell makima about your tools,
slash commands, and prompt contributions.

Most plugins only need `register_tool` and maybe `register_prompt_hint`.
Call these at the top level of your plugin file (during load).

```lua
makima.api.register_tool({ name = "greet", ... })
makima.api.register_prompt_hint({ slot = "tool_usage", content = "..." })
```

`@`-reference completion. Plugins register sources (popup candidates)
and expanders (submit-time token replacement) keyed by prefix. The
built-in `skill`, `task`, and `model` plugins provide the defaults; you
can add your own kinds the same way.

---

### `makima.api.register_tool()` {#makima-api-register_tool}

```lua
makima.api.register_tool({spec})
```

Register a new tool the agent can call. This is the main way plugins add
capabilities to the agent. The tool is queued during plugin load and
committed to the registry once the plugin finishes loading.

Your {spec} table must include a name, a description (the model reads it
to decide when to use the tool), a JSON Schema for the input, and a handler
function. The handler receives `(input, ctx)` and returns either a plain
string or a table with richer output fields.

**Parameters:**

- `{spec}` (`table`) Tool specification:
  - `name` (`string`) Required. ASCII identifier, up to 64 chars ([a-zA-Z_][a-zA-Z0-9_]*).
  - `description` (`string`) Required. Non-empty description shown to the model.
  - `schema` (`table`) Required. JSON Schema object describing the tool's input parameters.
  - `handler` (`function`) Required. Called with `(input, ctx)` when the tool is invoked.
    Must return a string or a table with any of these fields:
    - `llm_output` (`string`) Text sent to the model.
    - `is_error` (`boolean`) When true, the result is treated as an error.
    - `output_limits` (`table`) Bounds raw plain-text `llm_output` after the handler or `ctx:finish` returns.
    - `body` (`BufHandle`) Rich rendered body shown in the UI.
    - `header` (`BufHandle`) One-line header shown before the body.
    - `format` (`string`) "plain" (default) or "markdown".
    - `annotation` (`string`) Short label shown next to the tool call.
    - `written_path` (`string`) Path of a file written by the tool.
    - `diff_path` (`string`) Path for a diff output block.
    - `diff_before` (`string`) Before text of the diff.
    - `diff_after` (`string`) After text of the diff.
    - `image` (`table`) { media_type: string, data: string } base64 image.
    - `instructions` (`table`) Array of { path, content } blocks injected as context.
    - `state` (`any`) Serializable state forwarded to restore.
  - `audiences` (`string[]`) Which model audiences see the tool. Values: "main", "sub", "all". Default: all audiences.
  - `kind` (`string`) Optional grouping label (e.g. "filesystem").
  - `timeout` (`number`) Execution timeout in seconds. 0 or false disables. Default: inherits agent deadline.
  - `header` (`function`) Optional. Called before execution, returns a string or BufHandle for the one-line header.
  - `restore` (`function`) Optional. Called to re-render a previous tool result. Receives `(tool_name, input, output, ctx)`.
  - `start` (`function`) Optional. Called when the tool call starts, before the handler runs.
  - `describe` (`function`) Optional. Returns a custom description string for the current context.
  - `examples` (`table`) Optional. Array of example input objects for documentation.
  - `permission_scopes` (`string|function`) Field name in schema (string) or `function(input, ctx)` returning a list of path scopes that need write permission. `ctx.session_id` identifies the invocation session when available, and `ctx.options` maps its session option ids to their values.
  - `mutable_path` (`string|function`) Schema field name (type: string) for the primary path the tool writes, or `function(input, ctx)` returning the resolved target path (nil when the call does not mutate). `ctx.cwd` is the invocation session's working directory. When dispatched through the agent, tools declaring a `mutable_path` participate in same-process per-path mutation serialization: concurrent calls mutating the same normalized path run in non-overlapping order. Recursive same-path reentry from inside a locked mutable tool is unsupported and fails with `same-path mutation is already in progress`.
  - `start_annotation` (`string|table`) Schema field used to annotate the start header with a count (string) or timeout (`{ field, kind="timeout" }`).

  The handler can bound its model-facing output by returning `output_limits` with `llm_output`, or by passing the same reply table to `ctx:finish(reply)`. Options are `preview` ("head" or "head_tail"), `trailer` (a line that always comes last and is never cut), `lines_clipped` (the text already has long lines cut), and `max_lines` / `max_bytes` (per-tool limits; zero returns metadata only). The fixed output label is "output". Limits require plain-text `llm_output` and cannot be combined with markdown, image, diff, `state`, or `instructions` fields. Trailing newlines are removed before limiting, hashing, and saving. Longer output is saved to the session's offload store and replaced by a preview and footer; without a store it is cut instead.


  Job exit and cancellation hooks are synchronous callbacks and cannot yield, so pass the reply table with `output_limits` to `ctx:finish(reply)`. The host applies limits after receiving the reply and preserves `is_error`. With an output hook, dispatch filters the full output before limiting. Persistence retains its absolute deadline, including any per-tool timeout or deadline set with `ctx:set_deadline`. Cancellation or an expired deadline denies filtering without saving output. The body is saved without trailing newlines or the protected trailer. A started worker retains its owned text, options, and store even when its caller is dropped; session cleanup closes the store before removing its files.


**Example:**

```lua
makima.api.register_tool({
  name = "word_count",
  description = "Count words in a file.",
  kind = "read",
  schema = {
    properties = { path = { type = "string", description = "File path" } },
    required = { "path" },
  },
  handler = function(input)
    local f = io.open(input.path, "r")
    if not f then return { llm_output = "file not found", is_error = true } end
    local n = 0
    for _ in f:read("*a"):gmatch("%S+") do n = n + 1 end
    f:close()
    return tostring(n) .. " words"
  end,
})
```

---

### `makima.api.register_permission_rule()` {#makima-api-register_permission_rule}

```lua
makima.api.register_permission_rule({spec})
```

Declare an agent permission rule for a native tool. Use it to pre-allow
(or pre-deny) tool calls on paths your plugin owns, like a storage
directory outside the working dir, so the user is not prompted for them.

Rules live as long as the plugin is loaded: a reload replaces them, and a
reload that registers none clears the old ones. User config and session
deny rules always win over a plugin allow.

**Parameters:**

- `{spec}` (`table`) Rule specification:
  - `tool` (`string`) Required. Native tool name (e.g. "edit", "write").
    MCP tools and the "*" wildcard are not allowed.
  - `scope` (`string`) Required. Scope pattern the rule applies to, e.g.
    "/abs/dir/**" for a directory subtree.
  - `effect` (`string`) Optional. "allow" (default) or "deny".

**Example:**

```lua
makima.api.register_permission_rule({
  tool = "write",
  scope = notes_dir .. "/**",
})
```

---

### `makima.api.register_command()` {#makima-api-register_command}

```lua
makima.api.register_command({spec})
```

Register a slash-command that appears in the user input bar.

Slash commands let the user trigger plugin actions by typing `/name` in the
input. Use them for interactive workflows that do not need the model, such as
browsing memory files or toggling settings.

**Parameters:**

- `{spec}` (`table`) Command specification:
  - `name` (`string`) Required. The command name (e.g. "/hello"; a leading
    slash is added when missing).
  - `description` (`string`) Optional. Short description shown in the command palette.
  - `tui_only` (`boolean`) Required. If true, the command is available only in the interactive TUI.
  - `argument_hint` (`string`) Optional. Short hint describing the command arguments.
  - `arguments` (`array`) Optional. A dense typed positional schema. Each
    descriptor has a unique non-empty `name` and a
    `type` of `string`, `integer`, `enum`, `file`, or
    `directory`. Enum descriptors also require a
    non-empty `choices` array of distinct strings.
    `optional` and `variadic` are false by default.
    A required scalar consumes exactly one value. An
    optional scalar consumes zero or one value. A
    required variadic consumes one or more values.
    An optional variadic consumes zero or more values.
    Optional descriptors follow required descriptors.
    A variadic descriptor is last and consumes the
    remaining values. Typed input uses one quote-aware
    grammar. Unicode
    whitespace separates tokens outside quotes. Single
    and double quotes group text, adjacent quoted and
    unquoted fragments concatenate, and empty quotes
    produce an empty value. Backslashes are literal
    outside quotes and inside single quotes. Inside
    double quotes, only `\\"` and `\\\\` decode to a
    quote or backslash. Other backslashes stay literal.
    Shell expansion and operators are not supported.
    The command parser outer-trims the argument
    remainder before this grammar runs. Inner spaces,
    quotes, newlines inside quotes, and decoded values
    remain significant. Newlines separate tokens only
    outside quotes.
  - `arguments` (`table`) Use `{ raw = true }` for an unparsed argument
    remainder. Add `required = true` to reject empty
    input. Raw handlers receive `opts.args` only.
  - `handler` (`function`) Required. Called as `handler(opts, ctx)` after the
    command arguments pass validation. `ctx` is a trusted
    invocation-scoped LuaCtx, or nil if the host has no
    agent target. `ctx:agent()` resolves its
    pinned agent; later focus changes do not retarget it.
    Retaining ctx does not extend its authority beyond
    the handler. Existing one-argument handlers still work.
    The command finishes when the handler returns, and
    fails with the handler's error if it raises.
    `opts.args` is
    the outer-trimmed original argument remainder. It
    keeps inner whitespace and source quotes. For typed
    commands, `opts.fargs` is the decoded token array
    and `opts.values` is a name-keyed table. String,
    enum, file, and directory values are strings.
    Integer values are Lua integers. A missing optional
    scalar is `nil`. A missing optional variadic is an
    empty array. A variadic value is always an array.
    File and directory values retain their decoded,
    non-empty, NUL-free spelling and are not expanded
    or checked for existence by type validation. Raw
    commands do not set `opts.fargs` or `opts.values`.
    Typed integers are signed decimal values with no
    separators or alternate bases. The inclusive exact
    range is `-9007199254740991` to
    `9007199254740991`.
    Completion is declared on each typed descriptor
    with `completion = false`, `completion = "disabled"`,
    or a provider table. A provider table contains exactly
    one of `items` and `get_items`, and may set `mode` to
    `"replace"` or `"extend"` (`"replace"` is the default).
    Defaults are enum choices in the core and file or
    directory discovery in the TUI. String and integer
    defaults are empty. Provider callbacks receive the
    typed argument name, type, parsed preceding values,
    and the command completion context. Raw commands do
    not have argument completion providers.

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
-- Documentation and test example. It is not bundled and never copies files.
local recorded
makima.api.register_command({
  name = "/copy",
  description = "Record typed path arguments",
  tui_only = false,
  arguments = {
    { name = "source", type = "file" },
    { name = "destination", type = "directory" },
    { name = "policy", type = "enum", choices = { "skip", "overwrite" }, optional = true },
  },
  handler = function(opts)
    recorded = opts.values
  end,
})
-- `/copy "input file.txt" "build output" overwrite` records decoded values.

-- Raw commands preserve the complete argument remainder:
makima.api.register_command({
  name = "/hello",
  description = "Say hello",
  tui_only = false,
  arguments = { raw = true, required = true },
  handler = function(opts)
    recorded = opts.args
  end,
})
```

---

### `makima.api.register_prompt_hint()` {#makima-api-register_prompt_hint}

```lua
makima.api.register_prompt_hint({spec})
```

Add a piece of text to an aggregate prompt slot. Multiple plugins can each
contribute to the same slot, and all contributions are concatenated.

Good for things like tool usage guidelines or extra context that should
appear alongside other plugins' hints. If you need to own the whole slot
(e.g. identity or tone), use `set_prompt` instead.

Throws if you pass a singleton slot name.

**Parameters:**

- `{spec}` (`table`) Hint specification:
  - `slot` (`string`) Required. Aggregate slot name (e.g. "tool_usage", "general").
  - `content` (`string|function`) Required. Static text, or a `function()` that returns a string. Max 1 MiB.
  - `prompt` (`string|string[]`) Optional. Restrict to specific prompt ids (e.g. "system").

**Example:**

```lua
makima.api.register_prompt_hint({
  slot = "tool_usage",
  content = "- Prefer **grep** over reading entire files.",
})
```

---

### `makima.api.register_options()` {#makima-api-register_options}

```lua
makima.api.register_options({spec})
```

Declare the options your plugin accepts under `plugins.<name>` in
`makima.setup`, and get back what the user set merged with your defaults.
Call it once, at the top level of your plugin file.

An unknown key, a wrong type, or a value below `min` fails the plugin
load with a clear message, so users catch typos right away. Bad specs
fail the load too. The specs also feed the generated configuration docs.

**Parameters:**

- `{spec}` (`table`) Map of option name to a spec table:
  - `default` (`boolean|number|string`) Optional. Used when the user sets nothing. Its Lua type becomes the option type.
  - `type` (`string`) Required when there is no default: "boolean", "integer", "number", or "string".
  - `min` (`number`) Optional. Minimum accepted value, numeric options only.
  - `desc` (`string`) Required. One line shown in the configuration docs.

**Returns:** (`table`) Merged options: the user's value where set, otherwise the default, or nil when neither exists.

**Example:**

```lua
local opts = makima.api.register_options({
  timeout_secs = { default = 120, min = 5, desc = "Kill the command after this many seconds." },
  max_output_lines = { type = "integer", desc = "Override agent.max_output_lines for this tool." },
})
```

---

### `makima.api.set_prompt()` {#makima-api-set_prompt}

```lua
makima.api.set_prompt({spec})
```

Set a singleton prompt slot. Only one plugin owns each singleton slot at a
time, so calling this replaces any previous value from your plugin.

Use this for slots like "identity" or "tone" where a single coherent value
makes more sense than combining fragments. For aggregate slots like
"tool_usage", use `register_prompt_hint` instead.

Throws if you pass an aggregate slot name.

**Parameters:**

- `{spec}` (`table`) Spec fields mirror `register_prompt_hint`:
  - `slot` (`string`) Required. Singleton slot name (e.g. "identity", "tone").
  - `content` (`string|function`) Required. Static text or a `function()` returning a string. Max 1 MiB.
  - `prompt` (`string|string[]`) Optional. Restrict to specific prompt ids.

**Example:**

```lua
makima.api.set_prompt({
  slot = "tone",
  content = "Be concise. No filler words.",
})
```

---

### `makima.api.get_tools()` {#makima-api-get_tools}

```lua
makima.api.get_tools({opts?})
```

Return a list of all registered tools. Useful for building UI that shows
available tools or for checking which tools are enabled.

Each entry has the tool's name, schema, audiences, and an `enabled` flag.
Describe callbacks are not invoked (the static description is used).

**Parameters:**

- `{opts?}` (`table?`) Options:
  - `config` (`table`) Optional config table with a `disabled_tools` string[] field used to compute the `enabled` flag on each entry.

**Returns:** (`table[]`) Array of tool entries: { name, schema, audiences, kind?, enabled }.

**Example:**

```lua
local tools = makima.api.get_tools()
for _, t in ipairs(tools) do
  print(t.name, t.enabled)
end
```

---

### `makima.api.get_tool()` {#makima-api-get_tool}

```lua
makima.api.get_tool({name})
```

Look up a single tool by name. Returns its metadata table or nil if the
tool does not exist. For Lua-registered tools the returned table also
includes `header` and `restore` handle functions (wrapped so they never
throw).

**Parameters:**

- `{name}` (`string`) Exact tool name.

**Returns:** (`table|nil`) Tool entry with fields { name, schema, audiences, kind?, header?, restore? }, or nil if not found.

**Example:**

```lua
local t = makima.api.get_tool("bash")
if t then
  print("bash audiences:", table.concat(t.audiences, ", "))
end
```

---

### `makima.api.run_command()` {#makima-api-run_command}

```lua
makima.api.run_command({ctx?}, {cmdline})
```

Runs a slash command by name, as the explicit name-based executor: the
leading slash is optional, extra leading slashes are stripped, and the
name must resolve to a registered command (unknown names error, and the
`//` input escape does not apply to this API).
Works for built-ins, custom `/project:` and `/user:` commands, MCP
prompts, and commands other plugins registered.

Use it to alias a command you like under a name you prefer, instead of
reimplementing what it does. See `makima.ui.action` for the same idea
applied to keybound UI actions.

Pass the whole command line, arguments included: `"/cd ~/src"`. The
leading slash is optional. Names match exactly apart from case, so a typo
reports an error instead of running the closest command, and a cycle of
aliases stops with one too.

The call waits for the command's handler and returns its error when it
fails. A model turn, `/compact` or another long command it starts keeps
running after the call returns, so aliasing `/compact` does not block
your handler.

The command runs on the agent {ctx} is pinned to, through the frontend's
own command target for it, the same way in every frontend: a model turn
starts on that agent's session. Only `/btw` needs the TUI, because its
answer has nowhere else to show. Inside another command's handler the
nested command shares that handler's invocation, so one that needs more
than its handler -- a model turn, a custom Markdown command, `/compact`,
`/btw` -- reports an error instead of pretending it ran. Call it from a
keybinding or an autocmd if you need those. Only a trusted ctx (command,
keymap, autocmd) can run commands; delegated tool handlers cannot.

**Parameters:**

- `{ctx?}` ([`LuaCtx?`](#LuaCtx)) Trusted context naming the agent to run the command for. Required outside a command handler; a nested call shares its handler's invocation.
- `{cmdline}` (`string`) Command line, e.g. `"/new"` or `"/cd ~/src"`.

**Returns:** (`boolean|nil`, `string|nil`) `true` once the command ran, or nil and an error message if it is unknown, failed or cannot run here.

**Example:**

```lua
-- /resume as an alias for the built-in session picker:
makima.api.register_command({
  name = "/resume",
  description = "Alias for /sessions",
  tui_only = false,
  handler = function(_, ctx)
    local ok, err = makima.api.run_command(ctx, "/sessions")
    if not ok then
      makima.ui.flash("could not run /sessions: " .. err)
    end
  end,
})
```

---

### `makima.api.register_completion_source()` {#makima-api-register_completion_source}

```lua
makima.api.register_completion_source({prefix}, {spec})
```

Register a completion source for `prefix`. The source's `get_items(ctx)` is
called once when the `@` popup opens; `ctx` is `{ mode = "...", models = {...} }`.
Returns candidates as `{ label, kind, insertion, description? }`. `insertion`
is a logical `@` reference. The UI adds quotes when its value contains
whitespace or trailing sentence punctuation.

**Parameters:**

- `{prefix}` (`string`) The prefix this source owns (e.g. "skill"); labels typically carry the prefix (`skill:review`) so the fuzzy filter narrows by kind as the user types.
- `{spec}` (`table`) `{ get_items = function(ctx) -> { {label, kind, insertion, description?} } }`.

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
makima.api.register_completion_source("skill", {
  get_items = function(ctx)
    return { { label = "skill:review", kind = "skill", insertion = "@skill:review" } }
  end,
})
```

---

### `makima.api.register_expander()` {#makima-api-register_expander}

```lua
makima.api.register_expander({prefix}, {f})
```

Register a submit-time expander for `prefix`. Called with `{ value = "..." }`
(the decoded part after `prefix:`) for each `@prefix:value` token; quoted
values may contain whitespace and punctuation. Returns `(string, nil)` to
splice that string in place of the token, or `(nil, err)` to flash `err` and
abort the run. Unknown prefixes pass through verbatim, so register an
expander under every alias you accept (e.g. both `"skill"` and `"s"`).

**Parameters:**

- `{prefix}` (`string`) The prefix this expander owns (e.g. "skill" or "s").
- `{f}` (`function`) `{ value = string } -> (string, string|nil)`.

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
makima.api.register_expander("skill", function(ref)
  return "<skill:" .. ref.value .. ">", nil
end)
makima.api.register_expander("s", function(ref)
  return "<skill:" .. ref.value .. ">", nil
end)
```

---

### `makima.api.create_autocmd()` {#makima-api-create_autocmd}

```lua
makima.api.create_autocmd({event}, {opts})
```

Listen for one or more events. Returns an id you can pass to
`del_autocmd` later to remove the listener.

Every callback runs as its own task, called as `callback(ev, ctx)`: it
can await, and it never blocks the event that fired it. `ev` is
`{ id, event, match, data, agent }`.

Agent events fire for every agent, children included: `"TurnStart"`,
`"TurnEnd"`, `"AgentIdle"`, `"ConfigChanged"`, `"AgentClosed"`,
`"ToolStart"`, `"ToolDone"`, `"AutoCompacting"` and `"CompactionDone"`.
Their `ev.agent` is the agent's handle (nil once it has left the graph),
`ctx` is a trusted context pinned to it, and `ev.data` carries `agent_id`,
`session_id` and `parent_id` (nil for a root, so `if ev.data.parent_id then
return end` keeps a listener on roots). `"TurnStart"` adds `turn_id`,
`origin` and `selection`; `"TurnEnd"` carries the same fields as
`AgentTurn:result` plus `reason`, `cost`, `list_cost`, `context_size`,
`context_window` and `num_turns`; `"AgentIdle"` fires once work (a turn,
a compaction) settled, not after a config change alone, and adds `settled`;
`"ConfigChanged"` adds `generation`, `selection` and `mode`;
`"AgentClosed"` adds `lifecycle`. Tool events add `tool_id` and `tool`.
`"AutoCompacting"` adds `context_size` and `context_window`.
`"CompactionDone"` adds `status` (`"completed"`, `"failed"` or
`"cancelled"`), and after a compaction inside a turn also
`context_size_before`, `context_size_after` and `context_window`; a
compaction inside a turn that fails ends the turn, so it reports as a
failed `"TurnEnd"`.

Host events with no agent, where `ctx` is nil: `"SessionReset"`,
`"SessionFocusChanged"`, `"SessionCwdChanged"`, `"ProviderChanged"`,
`"SessionPickerRequested"`, `"SplashShown"`, `"SplashHidden"` and
`"StoreChanged"`. `"SessionReset"` and `"SessionFocusChanged"` carry
`data.session_id`: the session left behind, and the one now focused, which
also has `data.previous_session_id` except on initial startup.
`"SessionCwdChanged"` carries `data.cwd`, the session's new working
directory, after `/cd` moves it. `"StoreChanged"` carries `data.registry`
and, for registrations, `data.key`. Plugins can fire their own events with
`exec_autocmds`.

**Parameters:**

- `{event}` (`string|string[]`) Event name or list of names.
- `{opts}` (`table`) Options:
  - `callback` (`function`) called as `callback(ev, ctx)`.
  - `agent` (`Agent`) only fire for this agent's events.
  - `once` (`boolean`) remove the handler after it fires once (default false).
  - `pattern` (`string|string[]`) only fire when the pattern matches. `"*"` matches everything. Omit to match all.

**Returns:** (`integer|nil`, `string|nil`) Autocmd id. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
makima.api.create_autocmd("TurnEnd", {
  callback = function(ev, ctx)
    if ev.data.parent_id then
      return
    end
    makima.log.info("root turn ended: " .. ev.data.status)
  end,
})
```

---

### `makima.api.del_autocmd()` {#makima-api-del_autocmd}

```lua
makima.api.del_autocmd({id})
```

Remove a previously registered autocmd and cancel its running callbacks.
Does nothing if the {id} does not exist.

**Parameters:**

- `{id}` (`integer`) Id returned by `create_autocmd`.

**Example:**

```lua
makima.api.del_autocmd(id)
```

---

### `makima.api.exec_autocmds()` {#makima-api-exec_autocmds}

```lua
makima.api.exec_autocmds({event}, {opts?})
```

Fire one or more events manually. Each matching callback starts as its
own task; none has run yet when this function returns.

**Parameters:**

- `{event}` (`string|string[]`) Event name or list of names to fire.
- `{opts?}` (`table?`) Options:
  - `pattern` (`string`) passed to callbacks as `ev.match`.
  - `data` (`any`) arbitrary value passed as `ev.data`.

**Example:**

```lua
makima.api.exec_autocmds("MyEvent", {
  pattern = "init",
  data = { msg = "hello" },
})
```

---

### `makima.api.declare_slot()` {#makima-api-declare_slot}

```lua
makima.api.declare_slot({name}, {default})
```

Create a named extension point owned by your plugin. You provide a
{default} function, and other plugins can wrap it with layers using
`set_slot`. The returned callable runs the full chain: outermost
layer first, then inward, ending at {default}.

Throws if another plugin already owns a slot with the same {name}, or
if {name} starts with `"tool."`, which the host fires itself.

The chain is async: the default and every layer may park (`makima.fs.*`,
`makima.fn.jobwait`, `makima.agent.call_tool`, ...), and so does the
returned callable. Call it from a tool handler, a command, or an
autocmd, rather than from a `header` or `restore` function, which
cannot wait. The chain runs in your task, so cancelling the caller
cancels the layers it is waiting on.

**Parameters:**

- `{name}` (`string`) Unique slot name, e.g. `"myplugin.render"`.
- `{default}` (`function`) Default implementation, called when no layers wrap it.

**Returns:** (`function`) Callable that dispatches through all layers.

**Example:**

```lua
local render = makima.api.declare_slot("myplugin.render", function(text)
  return text:upper()
end)
print(render("hello")) -- HELLO
```

---

### `makima.api.set_slot()` {#makima-api-set_slot}

```lua
makima.api.set_slot({name}, {wrapper})
```

Add a layer around an existing (or future) slot. Layers wrap the
default from the outside in. Each layer receives `prev` as its
first argument. Call `prev(...)` to continue down the chain.
Calling `prev` more than once throws.

You can call this before the owner runs `declare_slot`. The layer
is queued and attached when the slot is declared.

A layer may park, and one that throws is skipped: the chain continues
as if it had returned `prev(...)` untouched, so a broken layer never
takes the seam down with it.

Layers wrap in registration order, so the last one registered runs
first and sees the value before the others do.

Makima fires two slots per tool itself: `tool.<name>.input` before
permissions look at the call, and `tool.<name>.output` on the text it
produced. Both take `function(prev, value, ctx)` and answer with a
table to replace the value, nothing to leave it alone, or
`nil, reason` to stop the call. Wrapping one costs the capability the
tool declares, and a tool declaring none costs every permission. See
[Hooks](/docs/hooks/).

**Parameters:**

- `{name}` (`string`) Slot name to wrap.
- `{wrapper}` (`function`) Layer: `function(prev, ...)`. Call `prev(...)` to continue.

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
makima.api.set_slot("myplugin.render", function(prev, text)
  return prev("[" .. text .. "]")
end)
```

---

### `makima.api.get_slots()` {#makima-api-get_slots}

```lua
makima.api.get_slots()
```

List all known slots and their current state. Useful for debugging
which plugins own or wrap each slot.

**Returns:** table<string, { owner: string?, has_default: boolean, layers: { plugin: string }[] }>

---

### `makima.api.register_session_option()` {#makima-api-register_session_option}

```lua
makima.api.register_session_option({spec})
```

Registers one selectable session option owned by the loading plugin.

The `id` must use the plugin namespace, such as `bash.auto_mode`. Global
`init.lua` uses `makima_init.global.*`, and project `init.lua` uses
`makima_init.project.*`. Core owns
unqualified ids. The definition supplies `name`, `description`, `category`,
an ordered non-empty `values` list, and `initial_value`. Set `persistent` to
retain each session's value across reloads.

An optional synchronous `validate(value)` callback returns `true` to accept
the candidate. It returns `false, message` or raises an error to reject it.
Rejection leaves every session and the active plugin generation unchanged.
Validation cannot yield or mutate session options.

Compatible plugin replacement retains current session values. Read a
value through `Agent:config(ctx).options[id]` and change it with
`Agent:set_option(ctx, id, value)`.

**Parameters:**

- `{spec}` (`table`) Session option definition and optional validation callback.

---

### `makima.api.get_session_options()` {#makima-api-get_session_options}

```lua
makima.api.get_session_options()
```

Lists the definitions of every option `Agent:set_option` accepts: the
built-in `workflow` and `yolo` toggles and every plugin's options. A
session's current values are in `Agent:config(ctx).options`.

**Returns:** table Array of `{id, name, description, category, values, owner, persistent}`, where `values` is an array of `{value, name}` and `owner` is "builtin" or the plugin name.

**Example:**

```lua
for _, option in ipairs(makima.api.get_session_options()) do
  print(option.id, option.name)
end
```

---

### `makima.api.register_provider()` {#makima-api-register_provider}

```lua
makima.api.register_provider({ctx?}, {spec?})
```

Registers a model provider under {ctx}, or under the plugin's root ctx
without one. Call it while the plugin loads, such as from a `setup(ctx)`
that `init.lua` calls. A provider id belongs to one plugin; registering
an id another plugin already owns fails and names both plugins.

{spec} fields:
  `id` (string) Required. The provider part of `provider/model` specs,
  made of ASCII letters, digits, `-`, `_` and `.`, not starting with `.`.
  `display_name` (string) Required.
  `models` (table) Model descriptors: `id` plus optional
  `context_window`, `max_output_tokens`, `pricing`, `pricing_schedule`
  (`{ windows = { { start, end } }, weekdays_only?, multiplier }`, whole
  UTC hours that scale `pricing` by `multiplier`), `supports_thinking`,
  `requires_thinking`, `supports_vision`, `supports_fast`,
  `supports_tool_examples`, `tier`, `free` (offered at no charge, marked
  in the model picker) and `provider_data`. The first descriptor with a
  tier is that tier's default. `supports_fast` may be `"pending"` while
  the provider cannot tell before its discovery answers: a saved or typed
  `/fast` then waits for the model prepared after discovery.
  `prepare` (function) Required. `prepare(ctx, model, config)` returns an
  instance for one model descriptor, optionally followed by an adjusted
  descriptor with the same id, or nil plus an error. `config` is the
  provider's `providers.toml` section as a table, read at each call.
  `discover` (function) Optional. `discover(ctx, config, refresh)`
  returns the descriptors the provider offers now. `refresh` is true when
  the user asked to bypass caches (`makima models --refresh`).
  `config` (table) Optional. `providers.toml` fields asked before login:
  `{ key, prompt, default?, secret?, optional?, choices? }`, where
  `choices` is a list of `{ id, label, detail? }`. Each answer is saved
  as a text field of the provider's section, such as `base_url`.
  `auth` (table) Login methods: `{ id, label, login }`.
  `login(ctx, ui, config)` gets a `makima.provider.LoginUi` and the
  provider's section with this login's answers applied, not yet saved.
  It returns a truthy value, optionally a table with `model` to choose
  the default model, or nil plus an error.
  `aliases` (table) Optional. `{ id, label, method }` names that log in
  with a preset method.
  `section` (string) Optional. The heading the provider is listed under
  in the login picker, after the providers without one.
  `auth_status` (function) Optional. `auth_status(ctx, config)` returns
  `{ state, detail? }`, where `state` is `saved`, `env`, `configured`,
  `missing` or `failed` (the check could not tell, shown as an error and
  treated as missing). It must work without credentials. Without it, the
  provider counts as logged in while its credentials file exists.
  `logout` (function) Optional. `logout(ctx)`. Without it, logout
  deletes the credentials file.
  `docs` (table) Optional. What the providers reference page says
  beyond models, login methods and config fields: `env` (the
  environment variables it reads), `endpoint` (its default endpoint) and
  `notes` (Markdown). Providers without it are left off that page.

Config is saved and the default model chosen only after `login`
succeeds and `auth_status` no longer reports `missing`.

Callbacks receive a provider `makima.Ctx` derived from {ctx}: `id`, `send`
(`makima.net.send` with the `[provider]` connect, low-speed and stream
timeouts as defaults) and `credentials` (the provider's
`makima.credentials.Store`); `makima.credentials.open` reaches {ctx}'s plugin
stores through it. Only the registering plugin's callbacks receive it, so
a provider id's credentials belong to the plugin that registered it. An instance has
`stream(ctx, request, emit)` and optionally `fetch_usage(ctx)`,
`refresh_auth(ctx)`, `reload_auth(ctx)`, `rotate_key(ctx)`,
`end_conversation(ctx, conversation_id, reason)`, `key_count` (a
number, or a function read at prepare and after each `reload_auth` or
`refresh_auth`, for a pool those can change) and
`request_delivery_marker`. `request` has `model`
(`id`, `max_output_tokens`, `context_window`), `system`, `messages`,
`tools`, `thinking`, `fast`, `session_id` and `invocation`
(`conversation_id`, `operation_id`, `attempt`, `purpose`, `agent_id`,
`previous`). `previous` is `{ id, relation, shared }` on every request of
a conversation started from another: `relation` is `continues` (after a
cancel or rewind) or `branches` (a fork, isolated turn or compaction
summary), and the first `shared` messages equal those of the last request
sent under `id`. `max_output_tokens` is what the model declares. `reason`
is `cancelled`, `closed`, `compacted` or `rewound`; the host never uses an
ended conversation id again, but a successor may name a `cancelled` or
`rewound` one in `previous`. `emit(event)` takes `{ type = "text_delta", text }`
and the other provider events, and returns false once the call is over.
`stream` returns `{ message, usage, stop_reason }`. A failure returns nil
plus an error: `{ kind = "config", message }` is a configuration error,
such as a missing key or setting, and is not retried; a table with
`status`, `message` and optional `retry_after` seconds is an API error,
and `overflow = true` on it says the request does not fit the model's
context, which the host answers by compacting instead of retrying;
anything else is a transport error. An error is retried only when it
names `retry`: `rate_limit`, `transient`, `connect`, `timeout` or `none`.
The bundled `http.classify` names it from the status or the
`makima.net.send` failure. A bare string and a raised error are not
retried. Every callback that returns nil plus an error follows these
rules.

The host links a successor to a conversation, and ends it with
`end_conversation`, only once a request under it was delivered: handed to
a server that may now hold state for it. `request_delivery_marker`
decides when that is. With `"auto"`, the default, a request is delivered
at its first accepted `emit`. With `"manual"` the provider decides alone,
and `emit` never delivers. In both, `ctx.mark_request_delivery()`, which
only `stream`'s ctx has, delivers the request at once and returns true;
once the call is over it returns false, and the provider must then not
send the request. A successful return always delivers; a manual provider
that never marked gets a warning in the log, and a failure without a mark
stays undelivered. Delivery holds across the retries of a request. A
provider whose server keeps state per conversation should mark right
before it sends the model request, since a failure or cancel before the
first event would otherwise leave that state behind.

**Parameters:**

- `{ctx?}` ([`makima.Ctx?`](#makima-Ctx)) The ctx to register under; omit it for the plugin's root ctx.
- `{spec?}` (`table`) Required. Provider definition (see above).

**Example:**

```lua
makima.api.register_provider({
  id = "echo",
  display_name = "Echo",
  models = { { id = "echo-1", tier = "strong" } },
  auth = {
    { id = "api_key", label = "API key", login = function(ctx, ui, config)
      local key, err = ui:input("API key", true)
      if not key then return nil, err end
      return ctx.credentials:write({ api_key = key })
    end },
  },
  prepare = function(ctx, model, config)
    return {
      stream = function(ctx, request, emit)
        emit({ type = "text_delta", text = "hi" })
        return {
          message = { role = "assistant", content = { { type = "text", text = "hi" } } },
          stop_reason = "end_turn",
        }
      end,
    }
  end,
})
```

---

### `makima.api.provider_config()` {#makima-api-provider_config}

```lua
makima.api.provider_config({id?})
```

Read `providers.toml`: the section of provider {id}, or every section by
provider id when {id} is nil. The file is read at each call, so a section
saved earlier in the session is seen. A malformed file is logged and
treated as empty because this API may run during an active session. A
section has the fields a provider's `config` argument has. Use it for a
setting another provider's section holds.

Requires the `env` [plugin permission](#plugin-permissions).

**Parameters:**

- `{id?}` (`string?`) Provider id.

**Returns:** (`table?`) The section, nil when there is none, or every section.

**Example:**

```lua
local opencode = makima.api.provider_config("opencode") or {}
for id, section in pairs(makima.api.provider_config()) do print(id, section.protocol) end
```

---

### `makima.api.startup_provider_config()` {#makima-api-startup_provider_config}

```lua
makima.api.startup_provider_config()
```

Read every `providers.toml` section while registering providers. Unlike
`provider_config`, malformed TOML is fatal so startup cannot silently
continue with an empty provider registry. This function is only available
while the calling plugin is loading.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** table Sections by provider id.

---

### `makima.api.save_provider_config()` {#makima-api-save_provider_config}

```lua
makima.api.save_provider_config({id}, {section})
```

Save {section} as provider {id}'s `providers.toml` section, replacing
any section it had. Fields the file does not know are dropped. Fails
without touching the file when it does not parse.

Requires the `fs_write` [plugin permission](#plugin-permissions).

**Parameters:**

- `{id}` (`string`) Provider id.
- `{section}` (`table`) Fields such as `protocol`, `base_url` and `api_key_env`.

**Returns:** (`boolean?`, `string?`) true, or nil plus an error.

**Example:**

```lua
makima.api.save_provider_config("my-llm", { protocol = "openai", base_url = "http://localhost:8000/v1" })
```


## makima.perf {#makima-perf}

Performance instrumentation for splashes and the UI. The host
measures splash renders; plugins read the timings and draw their own
readouts.

---

### `makima.perf.timings()` {#makima-perf-timings}

```lua
makima.perf.timings()
```

Read the splash render timings the host measured: how long the most
recent `splash.render` invocation took and how many renders completed in
the trailing second. The numbers come from the host that drives the
render, so they include the whole call (queue wait, warm-up, deadline)
and need no instrumentation inside the splash itself.

A still splash settles to `0 fps` once nothing animates, which is the
point: it proves the splash is not burning CPU.

**Returns:** (`table`) `{ render_ms = number, fps = number }`.

**Example:**

```lua
local t = makima.perf.timings()
```


## makima.store {#makima-store}

Shared key-value store for plugin contributions. A plugin registers
values under a (registry, key) pair: the same plugin may replace its
own value, a different plugin cannot claim an existing key, and
entries are removed automatically when their owning plugin unloads or
reloads.

Every change fires the `StoreChanged` autocmd event: `data =
{ registry, key }` on register, `data = { registry }` when an owner
unloads (one event per touched registry, no ordering guarantee).
Gather the initial state at load time and keep it fresh from events;
do not poll.

---

### `makima.store.register()` {#makima-store-register}

```lua
makima.store.register({registry}, {key}, {value})
```

Register a value in a store registry under a stable key. The same plugin
may replace its own value; a different plugin cannot claim an existing
key. Entries are removed automatically when their plugin unloads or
reloads.

The change fires the `StoreChanged` autocmd event with
`data = { registry, key }`, so consumers collect the initial state at
load time and then stay fresh from events instead of polling.

**Parameters:**

- `{registry}` (`string`) Registry name, e.g. `"splash"`.
- `{key}` (`string`) Stable entry identifier.
- `{value}` (`any`) Value exposed to registry consumers; tables may contain functions.

---

### `makima.store.collect()` {#makima-store-collect}

```lua
makima.store.collect({registry})
```

Collect the current entries of a store registry. Returns a fresh table
keyed by each entry's stable identifier; an unknown registry yields an
empty table.

**Parameters:**

- `{registry}` (`string`) Registry name.

**Returns:** table Map of entry key to registered value.


## makima.state {#makima-state}

Journaled plugin slots, scoped to an agent or session. Values are copied strict JSON data. Unloaded plugin values remain stored without running plugin code.

---

### `makima.state.register()` {#makima-state-register}

```lua
makima.state.register({name}, {spec})
```

Register a plugin-qualified slot during load. spec supplies scope (agent or session), positive version, and optional synchronous migrate(value, old_version). Registration is transactional with plugin load.

**Returns:** (`StateSlot`) Registered slot.

---

### `makima.state.null()` {#makima-state-null}

```lua
makima.state.null()
```

JSON null sentinel. Unlike nil it remains present in arrays and objects.


## makima.agent {#makima-agent}

Subagent primitives for plugins that need to talk to an LLM.

This module gives you the building blocks: resolve which model to use,
build a system prompt, list available tools, call a tool directly, or
spawn an agent with its own conversation history.

Policy like retries, validation, and concurrency lives in the calling
plugin, not here.

```lua
local tools = makima.agent.tools(ctx, { audience = "general_sub" })
local agent = assert(makima.agent.spawn(ctx, {
  system = "You are a helpful assistant.",
  tools = tools,
}))
local r = assert(assert(agent:send(ctx, "Hello!", { when = "idle" })):wait(ctx))
print(r.text)
agent:close(ctx)
```

---

### `makima.agent.resolve_model()` {#makima-agent-resolve_model}

```lua
makima.agent.resolve_model({ctx}, {opts?})
```

Look up the model that the current agent is using, or pick a cheaper one.
You might want a cheaper model for simple subtasks (summaries, classification)
without hard-coding a model name.

The returned table has the same fields as `makima.model.info`.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Agent context.
- `{opts?}` (`table?`) Optional fields:
  - `tier` (`string?`) target tier, one of `"weak"`, `"medium"`, `"strong"`.
    Clamped to the parent tier so you cannot escalate.
  - `spec` (`string?`) exact model spec string, e.g. `"claude-3-5-haiku-20241022"`.
    Takes precedence over `tier`.

**Returns:** (`table?`, `string?`) Model table on success, or `(nil, err)` on failure.

**Example:**

```lua
local model, err = makima.agent.resolve_model(ctx, { tier = "weak" })
if err then error(err) end
print(model.spec, model.tier)
```

---

### `makima.agent.system_prompt()` {#makima-agent-system_prompt}

```lua
makima.agent.system_prompt({ctx}, {opts})
```

Build a system prompt from a built-in template. Environment variables like
`{cwd}` are substituted automatically. Use this when you need a ready-made
prompt for a subagent session.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Agent context.
- `{opts}` (`table`) Required fields:
  - `prompt_id` (`string`) one of `"research"`, `"general"`, `"system"`.

  Optional fields:

  - `instructions` (`string|boolean?`) extra text appended to the prompt.
    `true` uses the invoking turn's recorded instructions.
    `false` or nil omits them.

**Returns:** (`string?`, `string?`) The assembled prompt string, or `(nil, err)` on failure.

**Example:**

```lua
local prompt, err = makima.agent.system_prompt(ctx, {
  prompt_id = "research",
  instructions = true,
})
if err then error(err) end
```

---

### `makima.agent.tools()` {#makima-agent-tools}

```lua
makima.agent.tools({ctx}, {opts})
```

Get the list of tool definitions for a given audience. Pass the result
straight into `makima.agent.spawn()` or use it to inspect what tools are
available.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Agent context.
- `{opts}` (`table`) Required fields:
  - `audience` (`string`) tool audience filter, e.g. `"general"`, `"subagent"`,
    `"general_sub"`.

  Optional fields:

  - `only` (`string[]?`) include only these tool names.
  - `except` (`string[]?`) exclude these tool names.
  - `workflow` (`boolean?`) use workflow-mode descriptions. Default: `false`.
  - `spec` (`string?`) evaluate capability exclusions against this model spec.

**Returns:** (`table?`, `string?`) Array of tool definition tables, or `(nil, err)` on failure.

**Example:**

```lua
local defs, err = makima.agent.tools(ctx, {
  audience = "general_sub",
  except = { "bash", "write" },
})
if err then error(err) end
print(#defs .. " tools available")
```

---

### `makima.agent.callable_tools()` {#makima-agent-callable_tools}

```lua
makima.agent.callable_tools({ctx})
```

Every tool name this context can dispatch: registry tools, MCP tools
(deferred ones included), host tools (ACP client tools, a subagent's
`structured_output`) and `tool_search`. Reach for it when you expose tools
inside a sandbox and need the names to bind. `makima.api.get_tools()` covers
the registry alone and has no view of the session.

The list already accounts for this session's audience, the config's
`disabled_tools` and the model's capabilities. Read `audiences` to layer
your own policy on top. A sandbox wants `interpreter`.

Each name shows up once, described by the tool a call would really reach, so
a host tool that shadows a registry name reports its own audience rather
than the shadowed one's.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Agent context.

**Returns:** (`table?`, `string?`) Array of `{ name, alias?, source, audiences, schema? }`,
  or `(nil, err)` on failure. `source` is one of `"native"`, `"local"`,
  `"mcp"`. `alias` is a safe identifier to bind, set only when `name` is not
  one (say `srv__get-docs`). Dispatch `name` in every case. `schema` comes
  with registry tools only.

**Example:**

```lua
local tools, err = makima.agent.callable_tools(ctx)
if err then error(err) end
for _, t in ipairs(tools) do
  print(t.source, t.alias or t.name)
end
```

---

### `makima.agent.call_tool()` {#makima-agent-call_tool}

```lua
makima.agent.call_tool({ctx}, {name}, {input}, {opts?})
```

Run a tool by name and wait for the result. This is how you call built-in
tools (like `read`, `bash`, `glob`) from Lua without going through the LLM.

Live events (streaming output, annotations, cumulative usage) are delivered
through optional callbacks while the tool runs.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Agent context.
- `{name}` (`string`) Tool name, e.g. `"bash"`, `"read"`.
- `{input}` (`table|any`) Tool input (JSON-serializable). Must match the tool's `input_schema`.
- `{opts?}` (`table?`) Optional fields:
  - `timeout` (`integer?`) deadline in seconds.
  - `on_live_buf` (`function?`) called with a `BufHandle` for each live buffer
    the tool publishes. Must not yield.
  - `on_annotation` (`function?`) called with an annotation string for each
    annotation event. Must not yield.
  - `on_usage` (`function?`) called with a formatted cumulative token usage
    string. Must not yield.

**Returns:** (`string?`, `string?`, `any`) Tool output text, or `(nil, err)` on
  failure. The third value is the tool's `state` (see `register_tool`),
  for callers that hand it back to the tool's `restore` later.

**Example:**

```lua
local out, err = makima.agent.call_tool(ctx, "bash", {
  command = "ls -la",
  timeout = 10,
})
if err then error(err) end
print(out)
```

---

### `makima.agent.permission_prompt()` {#makima-agent-permission_prompt}

```lua
makima.agent.permission_prompt({ctx}, {opts})
```

Run the normal interactive permission prompt for a tool call, without
dispatching the tool. This is how a plugin escalates a classifier deny or
failure to the same allow/deny/remember choices a user would see with
automode off.
Allow and deny rules apply exactly as they do for a plain tool call.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Agent context.
- `{opts}` (`table`) Required fields:
  - `tool` (`string`) tool key to prompt for, e.g. `"bash"`.
  - `scopes` (`string[]`) permission scopes to prompt for.

  Optional fields:

  - `force_prompt` (`boolean?`) prompt even for scopes an allow rule claims.

**Returns:** (`boolean?`, `string?`) `true` when the call is allowed, or
  `(nil, err)` with the permission-denied message when denied.

**Example:**

```lua
local ok, err = makima.agent.permission_prompt(ctx, {
  tool = "bash",
  scopes = { "echo hello" },
  force_prompt = false,
})
if not ok then return { llm_output = err, is_error = true } end
```

---

### `makima.agent.root()` {#makima-agent-root}

```lua
makima.agent.root({ctx})
```

Read the root agent of this context's graph.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** ([`AgentRef?`](#makima-agent-AgentRef), `string?`) Visibility-only root reference.

---

### `makima.agent.list()` {#makima-agent-list}

```lua
makima.agent.list({ctx})
```

List accessible resident and dormant agents in this context's graph.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`table?`, `string?`) Array of visibility-only AgentRef values. Visibility does not grant control.

---

### `makima.agent.get()` {#makima-agent-get}

```lua
makima.agent.get({ctx}, {reference})
```

Resolve an agent reference or stable ID with current control authority.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{reference}` (`AgentRef|string`) Visibility-only reference or stable agent ID.

**Returns:** ([`Agent?`](#makima-agent-Agent), `string?`) Nonowning handle or error.

---

### `makima.agent.spawn()` {#makima-agent-spawn}

```lua
makima.agent.spawn({ctx}, {opts})
```

Spawn a child with a copy of its parent's configuration.

Omitted tools inherit the parent's capability-filtered request tools and
cannot widen the parent's audience or mode policy. `tools = {}` excludes
all request tools. Explicit tools select the child's request definitions.
Requires an active managed turn or a trusted manager-backed parent.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Active agent context or trusted pinned parent.
- `{opts}` (`table`) Optional creation fields:
  - `model` (`string|table`) a spec or `{spec, thinking, fast}` selection, validated
    like `Agent:set_model`; omitted fields inherit, thinking is capped at the
    parent's level, and `Agent:model` results can be passed as is.
  - `system` (`string`) custom text appended after the admitted mode's prompt contribution; defaults to empty and does not inherit the parent's full system prompt.
  - `tools` (`array`) request definitions; omitted inherits permitted parent tools.
  - `local_tools` (`table`) name-keyed Lua tool specs with description, input_schema,
    handler(input), and optional capture_input for successful input capture.
  - `name` (`string`) display name. audience (string): defaults to the parent's audience;
    the child cannot widen the parent's capability ceiling.
  - `mcp` (`boolean`) defaults to true, subject to inherited restrictions.
  - `output_schema` (`table`) validated before creation; requires a valid final
    report each turn through the Rust structured-output reporting tool.
  - `silent` (`boolean`) defaults to false; suppresses parent relay and delivery.
  - `show` (`boolean|string`) post a clickable row for the child in its parent's chat;
    clicking it opens the child's chat once the child starts a turn.
    A string adds a description. Defaults to true for trusted parents and
    false otherwise. Silent children and children spawned by a tool call,
    which already have a tool row, cannot be shown.
  - `auto_deliver` (`boolean`) defaults to true; children notify their recorded
    parent once per settled turn. A wait in the parent's own turn returns
    the result directly and withdraws the queued notification.
  - `inherit_provider` (`boolean`) defaults to false; reuse the caller's provider
    and model, ignoring model.spec, when true.

  Any other option key is an error.


**Returns:** ([`Agent?`](#makima-agent-Agent), `string?`) Nonowning child handle. Garbage collection does not close it.


## makima.agent.Agent {#makima-agent-Agent}

A nonowning agent handle. Garbage collection never closes its actor.
Reads and effects require current invocation authority; identity is context-free.
A held managed handle retains authorized closed-status reads, not mutation rights.

---

### `Agent:id()` {#Agent-id}

```lua
Agent:id()
```

Read the stable agent ID.

**Returns:** (`string?`, `string?`) Agent ID.

---

### `Agent:is_live()` {#Agent-is_live}

```lua
Agent:is_live()
```

Check whether this agent currently has an open resident actor.

**Returns:** (`boolean?`, `string?`) Liveness without restoring the agent.

---

### `Agent:turn()` {#Agent-turn}

```lua
Agent:turn({ctx}, {turn_id})
```

Resolve an exact retained turn without starting or restoring the agent.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{turn_id}` (`string`) Stable turn ID.

**Returns:** ([`AgentTurn?`](#makima-agent-AgentTurn), `string?`) Exact-turn ticket or a retention error.

---

### `Agent:latest_turn()` {#Agent-latest_turn}

```lua
Agent:latest_turn({ctx})
```

Read the most recently admitted turn, including turns the runtime admitted
to deliver a child's result.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** ([`AgentTurn?`](#makima-agent-AgentTurn), `string?`) Exact-turn ticket, or nil without an error when no turn was admitted or it is no longer retained.

---

### `Agent:status()` {#Agent-status}

```lua
Agent:status({ctx})
```

Read lifecycle, active turn, queue size, and cumulative usage.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`table?`, `string?`) Fields: id (string), status ("idle", "working", or "closed", the vocabulary `makima.session` uses, which adds "needs_input"), queued (integer), active_turn (string or nil), input_tokens and output_tokens (cumulative integers). Authorized held managed handles retain a closed snapshot after graph removal.

---

### `Agent:config()` {#Agent-config}

```lua
Agent:config({ctx})
```

Read the committed configuration.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`table?`, `string?`) `{ version, model, mode, options }`. version is the committed config generation; model is the normalized selection `{spec, thinking, fast}` (thinking is a setting string or budget integer, disabled options are "off" and false); mode is the registered mode name; options maps every other session option id (workflow, yolo, plugin options such as `bash.auto_mode`) to its value. workflow is the agent's own; yolo and plugin options belong to the session, so a child reads its root's. The committed config is what the next turn admits; `ctx:model()` and `ctx:mode()` describe the turn already running.

---

### `Agent:available_models()` {#Agent-available_models}

```lua
Agent:available_models({ctx})
```

List authenticated, policy-filtered model spec strings without changing focus.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`table?`, `string?`) Array of available model spec strings.

---

### `Agent:transcript()` {#Agent-transcript}

```lua
Agent:transcript({ctx}, {opts})
```

Read a bounded committed transcript, optionally through a completed turn.
Omitted through_turn selects the latest history-bearing terminal turn; no
completed history returns an empty snapshot. Pending and unknown turns fail
explicitly. A boundary removed by compaction returns transcript_unavailable.
Snapshots are bounded history views, not an archive immune to compaction.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{opts}` (`table`) Required last_messages (1..1000) and max_bytes (2..1048576); optional through_turn. max_bytes bounds compact JSON of the messages array, not metadata.

**Returns:** (`table?`, `string?`) agent_id, through_turn, messages, epoch, total_messages, omitted_messages, omitted_tool_groups, compacted, bytes, truncated. Tool-call/response groups are indivisible; omitted_tool_groups counts omitted groups, not calls. compacted identifies a history containing a compaction summary, independently of bounded truncation.

---

### `Agent:ref()` {#Agent-ref}

```lua
Agent:ref()
```

Return another nonowning handle to this agent.

**Returns:** ([`AgentRef?`](#makima-agent-AgentRef), `string?`) Stable reference.

---

### `Agent:send()` {#Agent-send}

```lua
Agent:send({ctx}, {message}, {opts})
```

Send a user message and get the ticket of the turn that carries it.
`when` decides how the message meets other work: `"idle"` admits a turn
only when the agent has no active, preparing or queued work; `"join"`
joins compatible admitted work, reaching its next safe model request
without interrupting a provider request or tool, or starts a turn when
there is none; `"queue"` queues a distinct turn behind admitted work.
Wait for the result with `AgentTurn:wait`.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{message}` (`string`) User message (text only).
- `{opts}` (`table`) Required `when` ("idle", "join" or "queue"). `existing_only` (join only) joins compatible work or returns busy without starting a turn. `after_turn` (idle only) must match the latest terminal turn, including failure or cancellation.

**Returns:** ([`AgentTurn?`](#makima-agent-AgentTurn), `string?`) Exact-turn ticket, or an admission error. Bounded inbox rejection does not cancel accepted work.

---

### `Agent:set_model()` {#Agent-set_model}

```lua
Agent:set_model({ctx}, {patch})
```

Switch model through a reserved actor configuration operation.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{patch}` (`string|table`) Spec string or partial spec/thinking/fast selection.

**Returns:** (`table?`, `string?`) Complete normalized committed selection {spec, thinking, fast}, accepted as a subsequent patch subject to destination policy.

---

### `Agent:set_mode()` {#Agent-set_mode}

```lua
Agent:set_mode({ctx}, {name})
```

Switch mode through the actor configuration FIFO.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{name}` (`string`) Registered mode name.

**Returns:** (`string?`, `string?`) Committed mode name or error.

---

### `Agent:set_option()` {#Agent-set_option}

```lua
Agent:set_option({ctx}, {id}, {value})
```

Set one option: workflow, yolo or a plugin option. Model, thinking, fast
and mode have typed setters (`set_model`, `set_mode`) and are refused
here. workflow applies to this agent. yolo and plugin options belong to
the session, so they are set on its root and refused on a child. Plugin
options run their owner's validator first.
See [Configuration changes](/docs/modes/#configuration-changes).

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{id}` (`string`) Option id.
- `{value}` (`string`) Selectable value id.

**Returns:** (`boolean?`, `string?`) Success.

---

### `Agent:cancel()` {#Agent-cancel}

```lua
Agent:cancel({ctx})
```

Cancel existing work without closing the agent.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`boolean?`, `string?`) Success or error.

---

### `Agent:cancel_subtree()` {#Agent-cancel_subtree}

```lua
Agent:cancel_subtree({ctx})
```

Cancel existing work in this agent and its descendants.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`boolean?`, `string?`) Success or error.

---

### `Agent:close()` {#Agent-close}

```lua
Agent:close({ctx})
```

Permanently close this agent and its descendants.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`boolean?`, `string?`) Success or error.

---

### `Agent:retire()` {#Agent-retire}

```lua
Agent:retire({ctx})
```

Retire a quiescent leaf, preserving its retained results and transcript.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`boolean?`, `string?`) Success or a retirement error.

---

### `Agent:defer_close()` {#Agent-defer_close}

```lua
Agent:defer_close({ctx})
```

Close this agent at the end of the current host task, even on cancellation.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`boolean?`, `string?`) Cleanup registered.

---

### `Agent:on_turn_start()` {#Agent-on_turn_start}

```lua
Agent:on_turn_start({ctx}, {handler})
```

Alias for `create_autocmd("TurnStart", { agent = self, callback = handler })`.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Trusted invocation context.
- `{handler}` (`function`) Called as handler(ev, ctx), like any autocmd callback.

**Returns:** ([`AgentSubscription?`](#makima-agent-AgentSubscription), `string?`) Revocable subscription.

---

### `Agent:on_turn_end()` {#Agent-on_turn_end}

```lua
Agent:on_turn_end({ctx}, {handler})
```

Alias for `create_autocmd("TurnEnd", { agent = self, callback = handler })`.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Trusted invocation context.
- `{handler}` (`function`) Called as handler(ev, ctx), like any autocmd callback.

**Returns:** ([`AgentSubscription?`](#makima-agent-AgentSubscription), `string?`) Revocable subscription.

---

### `Agent:on_idle()` {#Agent-on_idle}

```lua
Agent:on_idle({ctx}, {handler})
```

Alias for `create_autocmd("AgentIdle", { agent = self, callback = handler })`.
Idle is an observation, not a reservation against new work. It fires once
work settled, so a handler that changes config does not wake itself.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Trusted invocation context.
- `{handler}` (`function`) Called as handler(ev, ctx), like any autocmd callback.

**Returns:** ([`AgentSubscription?`](#makima-agent-AgentSubscription), `string?`) Revocable subscription.

---

### `Agent:on_config_change()` {#Agent-on_config_change}

```lua
Agent:on_config_change({ctx}, {handler})
```

Alias for `create_autocmd("ConfigChanged", { agent = self, callback = handler })`.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Trusted invocation context.
- `{handler}` (`function`) Called as handler(ev, ctx), like any autocmd callback.

**Returns:** ([`AgentSubscription?`](#makima-agent-AgentSubscription), `string?`) Revocable subscription.

---

### `Agent:on_close()` {#Agent-on_close}

```lua
Agent:on_close({ctx}, {handler})
```

Alias for `create_autocmd("AgentClosed", { agent = self, callback = handler })`.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Trusted invocation context.
- `{handler}` (`function`) Called as handler(ev, ctx), like any autocmd callback.

**Returns:** ([`AgentSubscription?`](#makima-agent-AgentSubscription), `string?`) Revocable subscription.


## makima.agent.AgentRef {#makima-agent-AgentRef}

Visibility-only identity. Resolve with get(ctx, reference) to request control.

---

### `AgentRef:id()` {#AgentRef-id}

```lua
AgentRef:id()
```

Read a reference's stable identity.

**Returns:** (`string?`, `string?`) Agent ID.


## makima.agent.AgentSubscription {#makima-agent-AgentSubscription}

An autocmd filtered to one agent. Garbage collection does not
unsubscribe; close it, or unload the plugin.

---

### `AgentSubscription:close()` {#AgentSubscription-close}

```lua
AgentSubscription:close()
```

Same as `makima.api.del_autocmd` with this subscription's id: stops future
deliveries and cancels running callbacks.

**Returns:** (`boolean?`, `string?`) Success.


## makima.agent.AgentTurn {#makima-agent-AgentTurn}

An exact-turn ticket with repeatable retained result reads. Dropping it does not cancel work.
Settled results remain readable with current valid authority after lookup eviction
or agent close. Result fields: agent_id, turn_id, status ("completed", "failed",
or "cancelled"), text (last assistant message, including partial failure text),
duration_ms (immutable elapsed milliseconds for this turn), usage,
input_tokens, output_tokens (per-turn counters), output (captured array),
origin. usage contains input_tokens, output_tokens, cache_creation_input_tokens
and cache_read_input_tokens; top-level input_tokens includes cached tokens.
captured is the last output when present. Failed results include error,
user_message and retryable; cancelled results include cancellation_reason.
origin contains kind ("user", "plugin", or "internal"), plugin?,
plugin_generation?, source_agent?, source_turn?. It conveys provenance only.

---

### `AgentTurn:id()` {#AgentTurn-id}

```lua
AgentTurn:id()
```

Read the stable turn ID.

**Returns:** (`string?`, `string?`) Turn ID.

---

### `AgentTurn:agent_id()` {#AgentTurn-agent_id}

```lua
AgentTurn:agent_id()
```

Read the target agent ID.

**Returns:** (`string?`, `string?`) Agent ID.

---

### `AgentTurn:result()` {#AgentTurn-result}

```lua
AgentTurn:result({ctx})
```

Read a retained result without waiting or consuming it.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`table?`, `string?`) Nil without error while pending.

---

### `AgentTurn:wait()` {#AgentTurn-wait}

```lua
AgentTurn:wait({ctx}, {opts?})
```

Wait for this exact turn. Timeout stops waiting, not the agent. A wait that
returns a result in the recorded parent's own turn withdraws the queued
child-result delivery for that turn.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.
- `{opts?}` (`table?`) timeout is a nonnegative integer number of seconds. Zero returns nil, nil while pending; positive timeout returns nil and "agent turn wait timed out" without cancelling work.

**Returns:** (`table?`, `string?`) Per-turn result or error.

---

### `AgentTurn:cancel()` {#AgentTurn-cancel}

```lua
AgentTurn:cancel({ctx})
```

Cancel this admitted turn. For a managed turn, also close the child
subtrees it delegated. UI cancellation leaves those child subtrees open.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Invocation context.

**Returns:** (`boolean?`, `string?`) Success or error.


## makima.async {#makima-async}

Tools for running things concurrently in Lua plugins.

Use `run` to fire off background tasks, `gather` or `join` to run
several functions at once, and `semaphore` to limit concurrency.
The `await` and `wrap` helpers bridge callback-based APIs into
coroutine-friendly calls.

```lua
local results = makima.async.gather({
  function() return fetch("a.txt") end,
  function() return fetch("b.txt") end,
})
```

---

### `makima.async.run()` {#makima-async-run}

```lua
makima.async.run({fn}, {on_finish?})
```

Fire off a function as a new async task. It runs in the background and
you do not wait for it. If you need the result, pass an {on_finish}
callback.

A task started from a managed agent invocation inherits that exact turn's
authority. It may use the authority only while the originating turn remains
active; retained work fails closed after the turn ends.

**Parameters:**

- `{fn}` (`function`) Zero-argument function to execute.
- `{on_finish?}` (`function?`) Optional callback `function(err, result)`. Called once {fn} completes.

**Example:**

```lua
makima.async.run(function()
  local data = expensive_fetch()
  process(data)
end)
```

---

### `makima.async.await()` {#makima-async-await}

```lua
makima.async.await({argc}, {fn}, {...})
```

Turn a callback-based function into a normal call you can use in a coroutine. It calls `fn(..., callback)`, inserting the callback at position {argc}, then suspends your coroutine until the callback fires. You get back whatever the callback was called with.

**Parameters:**

- `{argc}` (`integer`) Total number of positional arguments {fn} expects (including the callback). Must be >= 1.
- `{fn}` (`function`) Callback-based function to call.
- `{...}` (`any`) Extra arguments forwarded to {fn} before the injected callback.

**Returns:** (`...`) Values passed by the caller to the injected callback.

**Example:**

```lua
local result = makima.async.await(2, http.get, url)
```

---

### `makima.async.wrap()` {#makima-async-wrap}

```lua
makima.async.wrap({argc}, {fn})
```

Create a coroutine-friendly wrapper around a callback-based function. The wrapper calls `makima.async.await` for you, so you can use the result like a normal function call.

**Parameters:**

- `{argc}` (`integer`) Callback position, forwarded to `makima.async.await`.
- `{fn}` (`function`) Callback-based function to wrap.

**Returns:** (`function`) Wrapped function you can call like a normal function.

**Example:**

```lua
local get = makima.async.wrap(2, http.get)
local body = get(url)
```

---

### `makima.async.join()` {#makima-async-join}

```lua
makima.async.join({max_jobs}, {fns})
```

Run all functions in {fns} with at most {max_jobs} going at once. Waits until every function has finished. Unlike `gather`, this does not return individual results.

**Parameters:**

- `{max_jobs}` (`integer`) Maximum number of functions running at the same time.
- `{fns}` (`table`) Array of zero-argument functions to execute.

**Example:**

```lua
makima.async.join(4, {
  function() process(files[1]) end,
  function() process(files[2]) end,
  function() process(files[3]) end,
})
```

---

### `makima.async.gather()` {#makima-async-gather}

```lua
makima.async.gather({fns})
```

Run all functions in {fns} at the same time and collect their results.
Unlike `join`, this gives you back the return value (or error) from each
function. The results are in the same order as the input.

Each entry in the result array has `ok` (boolean), and either `value`
(on success) or `err` (string, on failure).

**Parameters:**

- `{fns}` (`table`) Array of zero-argument functions.

**Returns:** (`table`) Array of result tables, one per function.

**Example:**

```lua
local results = makima.async.gather({
  function() return fetch("a.txt") end,
  function() return fetch("b.txt") end,
})
for i, r in ipairs(results) do
  if r.ok then print(r.value) else print("error: " .. r.err) end
end
```

---

### `makima.async.sleep()` {#makima-async-sleep}

```lua
makima.async.sleep({ms})
```

Suspend the calling task for {ms} milliseconds. Other tasks and the UI
keep running. Cancelling the task ends the sleep early with an error.

All plugins share one Lua thread. Code that runs for 5 seconds without
yielding is stopped with an error. `sleep(0)` lets every other ready
task run once, then returns. Call it periodically in long loops.

A repeating timer is a sleep loop inside `makima.async.run`. For a
one-shot timer that outlives the tool call, such as a toast that
dismisses itself, use `makima.defer_fn`.

**Parameters:**

- `{ms}` (`integer`) Milliseconds to wait. Negative values raise an error.

**Example:**

```lua
makima.async.run(function()
  while true do
    poll()
    makima.async.sleep(5000)
  end
end)

-- A long loop that keeps the rest of makima responsive:
for _, line in ipairs(lines) do
  process(line)
  makima.async.sleep(0)
end
```

---

### `makima.async.semaphore()` {#makima-async-semaphore}

```lua
makima.async.semaphore({n})
```

Create a counting semaphore that allows at most {n} concurrent permits.
Use this to limit how many tasks hit a resource at the same time.

**Parameters:**

- `{n}` (`integer`) Maximum number of concurrent permits. Values below 1 are clamped to 1.

**Returns:** ([`makima.async.Semaphore`](#makima-async-Semaphore)) A new semaphore.

**Example:**

```lua
local sem = makima.async.semaphore(5)
-- each task acquires a permit before doing work
local permit = sem:acquire()
do_work()
permit:release()
```

---

### `makima.async.on_cancel()` {#makima-async-on_cancel}

```lua
makima.async.on_cancel({fn})
```

Register {fn} to run as soon as the current task is cancelled or hits
its deadline, without waiting for whatever it is doing to finish. Use
it to paint the cancelled state: a handler waiting on children
(`gather`, `call_tool`) stays parked until they wind down, so anything
after the wait is too late to reach the screen.

The callback receives the reason (`"cancelled"` or `"timeout"`) and may
still call `ctx:finish`; the host prefers that reply over the generic
cancelled/timeout error. Mark it `is_error = true` and end it with a
marker, so the model knows the output it gets is cut short.

The callback runs outside your coroutine, so it must not yield. It
fires at most once, immediately if the task is already cancelled. An
error inside it is logged and never reaches your handler, and the
other hooks still run.

**Parameters:**

- `{fn}` (`function`) Function to run on cancel; receives the reason string.

**Example:**

```lua
makima.async.on_cancel(function(reason)
  view:append({ { reason, "tool_error" } })
  ctx:finish({ llm_output = partial .. "\n[cancelled; output is partial]", is_error = true })
end)
makima.async.gather(children)
```


## makima.async.Semaphore {#makima-async-Semaphore}

A counting semaphore for limiting how many tasks run at once.

Create one with `makima.async.semaphore(n)`, then call `:acquire()` to
get a permit before doing work. If the task is cancelled, the acquire
is cancelled too.

---

### `Semaphore:acquire()` {#Semaphore-acquire}

```lua
Semaphore:acquire()
```

Wait for a permit from the semaphore. Your coroutine suspends until a slot
opens up. If the owning task is cancelled, the acquire is cancelled too.

**Returns:** ([`makima.async.Permit`](#makima-async-Permit)) A permit handle. Call `:release()` when done, or let it be garbage collected.

**Example:**

```lua
local sem = makima.async.semaphore(3)
local permit = sem:acquire()
-- do work that needs the slot
permit:release()
```


## makima.async.Permit {#makima-async-Permit}

One slot in a semaphore, obtained from `Semaphore:acquire()`.

The slot is held until you call `:release()` or until the permit is
garbage collected. Releasing early lets other tasks acquire sooner.

---

### `Permit:release()` {#Permit-release}

```lua
Permit:release()
```

Give the permit back to the semaphore so another task can acquire it.
Throws if you already released this permit.


## makima.base64 {#makima-base64}

Base64 encoding and decoding, modelled after `vim.base64`.

Both functions accept strings and Luau buffers, so you can round-trip
binary data read with `makima.fs.read_bytes`.

```lua
local encoded = makima.base64.encode("hello")
local decoded = makima.base64.decode(encoded)
```

---

### `makima.base64.encode()` {#makima-base64-encode}

```lua
makima.base64.encode({data})
```

Encode {data} to standard Base64. Like `vim.base64.encode`.
Accepts both strings and Luau buffers.

**Parameters:**

- `{data}` (`string|buffer`) Data to encode.

**Returns:** (`string`) Base64-encoded string.

**Example:**

```lua
makima.base64.encode("hello") -- "aGVsbG8="
```

---

### `makima.base64.decode()` {#makima-base64-decode}

```lua
makima.base64.decode({str})
```

Decode a Base64-encoded {str} back to its original bytes. Like `vim.base64.decode`.
Throws if {str} is not valid Base64.

**Parameters:**

- `{str}` (`string|buffer`) Base64-encoded text.

**Returns:** (`string`) Decoded bytes as a string.

**Example:**

```lua
makima.base64.decode("aGVsbG8=") -- "hello"
```


## makima.crypto {#makima-crypto}

Hashing on binary strings. Digests come back as raw bytes, not hex.
Both functions accept strings and Luau buffers.

```lua
local signature = makima.crypto.hmac_sha256(key, makima.crypto.sha256(body))
```

---

### `makima.crypto.sha256()` {#makima-crypto-sha256}

```lua
makima.crypto.sha256({data})
```

SHA-256 digest of {data}, as 32 raw bytes.

**Parameters:**

- `{data}` (`string|buffer`) Bytes to hash.

**Returns:** (`string`) Raw 32-byte digest.

**Example:**

```lua
local digest = makima.crypto.sha256("abc")
```

---

### `makima.crypto.hmac_sha256()` {#makima-crypto-hmac_sha256}

```lua
makima.crypto.hmac_sha256({key}, {data})
```

HMAC-SHA256 of {data} keyed with {key}, as 32 raw bytes.

**Parameters:**

- `{key}` (`string|buffer`) Secret key, any length.
- `{data}` (`string|buffer`) Message to authenticate.

**Returns:** (`string`) Raw 32-byte MAC.

**Example:**

```lua
local mac = makima.crypto.hmac_sha256("secret", "message")
```


## makima.env {#makima-env}

Paths to makima's own directories (config, state, logs, cache), and whether
the project's `.makima` directory is off limits.

Use these to locate config files or persistent state without hard-coding paths.

```lua
local cfg = makima.env.config_dir()
```

---

### `makima.env.state_dir()` {#makima-env-state_dir}

```lua
makima.env.state_dir()
```

Return the directory where makima stores runtime state (sessions, auth tokens, etc.).
Typically something like `~/.local/state/makima`.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** (`string?`) State directory path, or nil if it cannot be determined.

**Example:**

```lua
local dir = makima.env.state_dir()
```

---

### `makima.env.config_dir()` {#makima-env-config_dir}

```lua
makima.env.config_dir()
```

Return the directory where makima looks for user configuration files.
Typically something like `~/.config/makima`.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** (`string?`) Config directory path, or nil if it cannot be determined.

**Example:**

```lua
local dir = makima.env.config_dir()
```

---

### `makima.env.logs_dir()` {#makima-env-logs_dir}

```lua
makima.env.logs_dir()
```

Return the directory where makima writes its log files (`makima.log`).
Typically something like `~/.local/logs/makima`.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** (`string?`) Logs directory path, or nil if it cannot be determined.

**Example:**

```lua
local dir = makima.env.logs_dir()
```

---

### `makima.env.cache_dir()` {#makima-env-cache_dir}

```lua
makima.env.cache_dir()
```

Return the directory where makima keeps caches it can rebuild, such as
downloaded catalogs. Typically something like `~/.cache/makima`.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** (`string?`) Cache directory path, or nil if it cannot be determined.

**Example:**

```lua
local dir = makima.env.cache_dir()
```

---

### `makima.env.no_project()` {#makima-env-no_project}

```lua
makima.env.no_project()
```

Whether makima was started with `--no-project`. A plugin that reads files
from the project's `.makima` directory should skip them when this is true.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** (`boolean`) `true` under `--no-project`.

**Example:**

```lua
if not makima.env.no_project() then
  scan(".makima/skills")
end
```


## makima.fn {#makima-fn}

Process and environment helpers, modeled after Neovim's `vim.fn` job
control. Use these to run shell commands, wait for output, and check
whether programs are installed.

```lua
local id = makima.fn.jobstart("git status", {
  on_exit = function(code) print("done: " .. code) end,
})
```

---

### `makima.fn.jobstart()` {#makima-fn-jobstart}

```lua
makima.fn.jobstart({cmd}, {opts?})
```

Run a shell command in the background. The command runs through
`bash -c` on Unix or `cmd /C` on Windows. You get back a job id
that you can pass to `jobstop` or `jobwait` to control the process.

Requires the `run` [plugin permission](#plugin-permissions).

**Parameters:**

- `{cmd}` (`string|string[]`) Shell command to run, or array of arguments.
- `{opts?}` (`table?`) Optional settings:
  - `cwd` (`string?`) working directory (tilde is expanded).
  - `env` (`table?`) extra environment variables, `{ VAR = "value" }`.
  - `on_stdout` (`function?`) called with `(job_id, line)` for each stdout line.
  - `on_stderr` (`function?`) called with `(job_id, line)` for each stderr line.
  - `on_exit` (`function?`) called with `(job_id, code)` when the process finishes.
  - `owner` (`string?`) job lifetime. `"task"` (default) ends the job with
    the current call. `"plugin"` keeps it alive until the plugin unloads
    or reloads.

**Returns:** (`integer`) Job id.

**Example:**

```lua
local id = makima.fn.jobstart("ls -la", {
  cwd = "~/projects",
  on_stdout = function(_, line) print(line) end,
  on_exit = function(_, code) print("exit: " .. code) end,
})
```

---

### `makima.fn.jobstop()` {#makima-fn-jobstop}

```lua
makima.fn.jobstop({job_id})
```

Kill a running job immediately (SIGKILL on Unix). Safe to call on
jobs that already exited or on unknown ids.

Requires the `run` [plugin permission](#plugin-permissions).

**Parameters:**

- `{job_id}` (`integer`) Job id returned by `jobstart`.

**Example:**

```lua
makima.fn.jobstop(id)
```

---

### `makima.fn.jobwait()` {#makima-fn-jobwait}

```lua
makima.fn.jobwait({job_id}, {timeout_ms?})
```

Wait for a job to finish and collect its output. Returns a result
table with `stdout`, `stderr`, and `exit_code`. Returns `nil` if the
job does not finish before the timeout.

While waiting, the job's `on_stdout`, `on_stderr`, and `on_exit`
callbacks fire as events arrive (like Neovim), so you can stream
output into a buffer while parked here.

Requires the `run` [plugin permission](#plugin-permissions).

**Parameters:**

- `{job_id}` (`integer`) Job id returned by `jobstart`.
- `{timeout_ms?}` (`integer?`) Maximum wait in milliseconds (default 30000).

**Returns:** (`table?`) `{ stdout, stderr, exit_code }`, or nil on timeout.

**Example:**

```lua
local id = makima.fn.jobstart("echo hello")
local result = makima.fn.jobwait(id, 5000)
if result then
  print(result.stdout)
end
```

---

### `makima.fn.executable()` {#makima-fn-executable}

```lua
makima.fn.executable({name})
```

Check whether {name} can be found on `$PATH` or is a path to a file,
and on Unix whether that file may be executed. Returns 1 when found, 0
otherwise (matches Neovim's `vim.fn.executable`).

Requires the `env` [plugin permission](#plugin-permissions).

**Parameters:**

- `{name}` (`string`) Program name (e.g. `"git"`) or path.

**Returns:** (`integer`) `1` if found, `0` otherwise.

**Example:**

```lua
if makima.fn.executable("rg") == 1 then
  -- use ripgrep
end
```

---

### `makima.fn.winsaveview()` {#makima-fn-winsaveview}

```lua
makima.fn.winsaveview()
```

Read the viewport of the focused chat transcript, like Neovim's
`vim.fn.winsaveview()`. The transcript is the only scrollable window
makima has, so there is no window argument.

`topline` is the 1-based transcript line at the top of the viewport, so
the last visible one is `math.min(topline + height - 1, line_count)`.
`auto_scroll` has no Vim counterpart: it is true while the transcript
follows streaming output.

**Returns:** (`table|nil`, `string|nil`) `{topline, line_count, height, auto_scroll}`, or nil and an error.

**Example:**

```lua
local view = makima.fn.winsaveview()
makima.fn.winrestview({ topline = view.topline + 1 })
```

---

### `makima.fn.winrestview()` {#makima-fn-winrestview}

```lua
makima.fn.winrestview({view})
```

Scroll the focused chat transcript so that the `topline` field of
{view} becomes the top visible line, like Neovim's
`vim.fn.winrestview()`. Out of range values are clamped. Other keys are
ignored, so a table straight from `winsaveview()` round-trips.

Scrolling away from the bottom unpins the transcript; landing back at
the bottom re-pins it so streaming output keeps following.

**Parameters:**

- `{view}` (`table`) View to restore. Only `topline` (1-based) is read.

**Returns:** (`boolean|nil`, `string|nil`) true on success, or nil and an error.

**Example:**

```lua
makima.fn.winrestview({ topline = 1 })
```


## makima.fs {#makima-fs}

File-system utilities, modelled after `vim.fs` and `vim.uv`.

Fallible operations return `(value, err)` pairs and never throw.
Paths support `~/` expansion. Relative paths resolve from the current working directory.

```lua
local text, err = makima.fs.read("init.lua")
if err then return end
```

---

### `makima.fs.read()` {#makima-fs-read}

```lua
makima.fs.read({path})
```

Read the entire file at {path} as a UTF-8 string.
If the file contains bytes that are not valid UTF-8, this function throws.
Use `read_bytes` for binary files.

Requires the `fs_read` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Absolute or relative file path. `~/` is expanded to the home directory.

**Returns:** (`string?`, `string?`) File contents, or nil plus an error message.

**Example:**

```lua
local text, err = makima.fs.read("config.toml")
if err then
  makima.log.warn("could not read config: " .. err)
  return
end
```

---

### `makima.fs.read_bytes()` {#makima-fs-read_bytes}

```lua
makima.fs.read_bytes({path})
```

Read the entire file at {path} as raw bytes, returned as a Luau buffer.
Useful for binary files or when you need to pass the data to `makima.base64.encode`.

Requires the `fs_read` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Absolute or relative file path. `~/` is expanded to the home directory.

**Returns:** (`buffer?`, `string?`) File bytes as a Luau buffer, or nil plus an error message.

**Example:**

```lua
local buf, err = makima.fs.read_bytes("image.png")
if err then return end
local encoded = makima.base64.encode(buf)
```

---

### `makima.fs.metadata()` {#makima-fs-metadata}

```lua
makima.fs.metadata({path})
```

Get metadata for the file or directory at {path}.
Returns a table with `size` (integer), `is_file` (boolean), `is_dir` (boolean),
and `mtime` (number, fractional seconds since the Unix epoch; absent when the
filesystem does not report a modification time).
If {path} does not exist, returns nil with no error.

Requires the `fs_read` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Absolute or relative path.

**Returns:** (`table?`, `string?`) Metadata table, nil if missing, or nil plus an error message.

**Example:**

```lua
local meta = makima.fs.metadata("src/main.rs")
if meta and meta.is_file then
  print("size: " .. meta.size)
end
```

---

### `makima.fs.dirname()` {#makima-fs-dirname}

```lua
makima.fs.dirname({path})
```

Return the parent directory of {path}. Like `vim.fs.dirname`.

**Parameters:**

- `{path}` (`string`) File path.

**Returns:** (`string?`) Parent directory, or nil if {path} has no parent.

**Example:**

```lua
makima.fs.dirname("/home/user/init.lua") -- "/home/user"
```

---

### `makima.fs.basename()` {#makima-fs-basename}

```lua
makima.fs.basename({path})
```

Return the final component (the file name) of {path}. Like `vim.fs.basename`.

**Parameters:**

- `{path}` (`string`) File path.

**Returns:** (`string?`) File name, or nil for paths like `/`.

**Example:**

```lua
makima.fs.basename("/home/user/init.lua") -- "init.lua"
```

---

### `makima.fs.joinpath()` {#makima-fs-joinpath}

```lua
makima.fs.joinpath({...})
```

Join one or more path segments into a single path. Like `vim.fs.joinpath`.

**Parameters:**

- `{...}` (`string`) One or more path segments to join.

**Returns:** (`string`) The joined path.

**Example:**

```lua
makima.fs.joinpath("src", "api", "fs.rs") -- "src/api/fs.rs"
```

---

### `makima.fs.normalize()` {#makima-fs-normalize}

```lua
makima.fs.normalize({path})
```

Clean up `.` and `..` segments and make {path} absolute. Like `vim.fs.normalize`.
This is purely string-based and does not touch the filesystem.

**Parameters:**

- `{path}` (`string`) Path to normalize. `~/` is expanded.

**Returns:** (`string`) Normalized absolute path.

**Example:**

```lua
makima.fs.normalize("src/../src/api") -- "/home/user/project/src/api"
```

---

### `makima.fs.abspath()` {#makima-fs-abspath}

```lua
makima.fs.abspath({path})
```

Make {path} absolute by prepending the current working directory when needed.
Unlike `normalize`, this does not resolve `.` or `..` segments.

**Parameters:**

- `{path}` (`string`) Relative or absolute path. `~/` is expanded.

**Returns:** (`string`) Absolute path.

**Example:**

```lua
makima.fs.abspath("src/main.rs") -- "/home/user/project/src/main.rs"
```

---

### `makima.fs.parents()` {#makima-fs-parents}

```lua
makima.fs.parents({path})
```

Return all ancestor directories of {path}, from the immediate parent up to the root.
Handy for walking up a directory tree.

**Parameters:**

- `{path}` (`string`) File or directory path.

**Returns:** (`string[]`) Array of ancestor directory paths.

**Example:**

```lua
local dirs = makima.fs.parents("/home/user/project/src")
-- { "/home/user/project", "/home/user", "/home", "/" }
```

---

### `makima.fs.root()` {#makima-fs-root}

```lua
makima.fs.root({source}, {marker})
```

Walk upward from {source} looking for a directory that contains one of the
{marker} files or directories. Like `vim.fs.root`. Useful for finding the
project root.

Requires the `fs_read` [plugin permission](#plugin-permissions).

**Parameters:**

- `{source}` (`string`) Starting file or directory path.
- `{marker}` (`string|string[]`) Marker filename(s) to look for, e.g. `".git"` or `{"package.json", ".git"}`.

**Returns:** (`string?`, `string?`) Root directory path, or nil when not found.

**Example:**

```lua
local root = makima.fs.root("src/main.rs", { ".git", "Cargo.toml" })
if root then print("project root: " .. root) end
```

---

### `makima.fs.relpath()` {#makima-fs-relpath}

```lua
makima.fs.relpath({base}, {target})
```

Compute a relative path from {base} to {target}.

**Parameters:**

- `{base}` (`string`) Base directory path.
- `{target}` (`string`) Target path.

**Returns:** (`string`) Relative path from {base} to {target}.

**Example:**

```lua
makima.fs.relpath("/home/user", "/home/user/project/src") -- "project/src"
```

---

### `makima.fs.ext()` {#makima-fs-ext}

```lua
makima.fs.ext({path})
```

Return the file extension of {path}, without the leading dot.

**Parameters:**

- `{path}` (`string`) File path.

**Returns:** (`string?`) Extension, or nil if the path has no extension.

**Example:**

```lua
makima.fs.ext("main.rs")   -- "rs"
makima.fs.ext("Makefile")  -- nil
```

---

### `makima.fs.dir()` {#makima-fs-dir}

```lua
makima.fs.dir({path}, {opts?})
```

List the contents of the directory at {path}.
Each entry is a two-element array `{name, type}` where type is one of
`"file"`, `"directory"`, `"link"`, or `"unknown"`. Follows symlinks.

Requires the `fs_read` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Directory path.
- `{opts?}` (`table?`) `depth` (integer, default 1): how many levels deep to recurse.

**Returns:** (`table?`, `string?`) Array of `{name, type}` entries, or nil plus an error message.

**Example:**

```lua
local entries, err = makima.fs.dir("src", { depth = 2 })
if err then return end
for _, e in ipairs(entries) do
  print(e[1], e[2]) -- "main.rs"  "file"
end
```

---

### `makima.fs.write()` {#makima-fs-write}

```lua
makima.fs.write({path}, {content})
```

Write {content} to the file at {path}, creating it if it does not exist
or overwriting it if it does.

Requires the `fs_write` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Destination file path. `~/` is expanded.
- `{content}` (`string|buffer`) Bytes to write.

**Returns:** (`true?`, `string?`) `true` on success, or nil plus an error message.

**Example:**

```lua
local ok, err = makima.fs.write("out.txt", "hello world")
if err then print("write failed: " .. err) end
```

---

### `makima.fs.atomic_write()` {#makima-fs-atomic_write}

```lua
makima.fs.atomic_write({path}, {content})
```

Atomically replace {path} with {content}. The parent directory must exist.
Readers observe either the old file or the complete new file.
Existing file permissions are preserved. On Unix, new files use mode 0600.

Requires the `fs_write` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Destination file path. `~/` is expanded.
- `{content}` (`string|buffer`) Bytes to write.

**Returns:** (`true?`, `string?`) `true` on success, or nil plus an error message.

**Example:**

```lua
local ok, err = makima.fs.atomic_write("state.json", encoded)
if err then print("atomic write failed: " .. err) end
```

---

### `makima.fs.rm()` {#makima-fs-rm}

```lua
makima.fs.rm({path}, {opts?})
```

Delete the file, symlink, or directory at {path}.
Pass `recursive = true` to remove a non-empty directory tree (like `rm -r`).
Unlike `vim.fs.rm`, this also removes an empty directory without `recursive`.
Symlinks are removed themselves, never followed.

Requires the `fs_write` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Path to the file or directory to remove.
- `{opts?}` (`table?`) `recursive` (boolean, default false): remove a directory and its contents recursively. `force` (boolean, default false): silently ignore a missing path.

**Returns:** (`true?`, `string?`) `true` on success, or nil plus an error message.

**Example:**

```lua
local ok, err = makima.fs.rm("temp.txt")
if err then print("rm failed: " .. err) end
makima.fs.rm("stale_dir", { recursive = true, force = true })
```

---

### `makima.fs.mkdir()` {#makima-fs-mkdir}

```lua
makima.fs.mkdir({path}, {opts?})
```

Create the directory at {path}. Set `parents = true` to create
intermediate directories, like `mkdir -p`.

Requires the `fs_write` [plugin permission](#plugin-permissions).

**Parameters:**

- `{path}` (`string`) Directory path to create.
- `{opts?}` (`table?`) `parents` (boolean, default false): create intermediate parent directories.

**Returns:** (`true?`, `string?`) `true` on success, or nil plus an error message.

**Example:**

```lua
makima.fs.mkdir("a/b/c", { parents = true })
```

---

### `makima.fs.glob()` {#makima-fs-glob}

```lua
makima.fs.glob({pattern}, {opts?})
```

Find files matching one or more glob patterns.
Respects `.gitignore` by default. Pass `sort = "mtime"` to get the most
recently modified files first.

Requires the `fs_read` [plugin permission](#plugin-permissions).

**Parameters:**

- `{pattern}` (`string|string[]`) Glob pattern or array of patterns.
- `{opts?}` (`table?`) `path` (string): search root. `limit` (integer): max results. `gitignore` (boolean, default true): respect .gitignore. `sort` (string): `"mtime"` sorts newest first.

**Returns:** (`string[]?`, `string?`) Array of absolute file paths, or nil plus an error message.

**Example:**

```lua
local files, err = makima.fs.glob("**/*.lua", { path = "plugins", limit = 10 })
if err then return end
for _, f in ipairs(files) do print(f) end
```

---

### `makima.fs.grep()` {#makima-fs-grep}

```lua
makima.fs.grep({pattern}, {opts?})
```

Search file contents for a regex {pattern}. Returns structured matches
grouped by file, similar to ripgrep output.

Each result entry has a `path` and a list of `groups`. Each group contains
`lines`, where every line has `line_nr`, `text`, and `is_match`.

Requires the `fs_read` [plugin permission](#plugin-permissions).

**Parameters:**

- `{pattern}` (`string`) Regular expression to search for.
- `{opts?}` (`table?`) `path` (string): search root. `include` (string): file glob filter (e.g. `"*.rs"`). `context_before` / `context_after` (integer): context lines around matches. `limit` (integer): max match groups. `max_line_bytes` (integer): skip lines longer than this.

**Returns:** (`table?`, `string?`) Array of `{path, groups}` tables, or nil plus an error message.

**Example:**

```lua
local hits, err = makima.fs.grep("TODO", { path = "src", include = "*.rs", limit = 5 })
if err then return end
for _, file in ipairs(hits) do
  for _, g in ipairs(file.groups) do
    for _, line in ipairs(g.lines) do
      if line.is_match then print(file.path .. ":" .. line.line_nr) end
    end
  end
end
```


## makima.host {#makima-host}

Actions on the host machine: opening a browser, reading the keyring.

```lua
makima.host.open("https://example.com")
```

---

### `makima.host.open()` {#makima-host-open}

```lua
makima.host.open({url})
```

Open {url} in the user's default browser. Only `http` and `https` URLs
are accepted.

Requires the `run` [plugin permission](#plugin-permissions).

**Parameters:**

- `{url}` (`string`) URL to open.

**Returns:** (`boolean?`, `string?`) true, or nil plus an error string.

**Example:**

```lua
local ok, err = makima.host.open("https://example.com/login")
```

---

### `makima.host.keyring_get()` {#makima-host-keyring_get}

```lua
makima.host.keyring_get({service}, {account})
```

Read a secret another program stored in the system keyring, such as the
token the GitHub CLI keeps there.

Requires the `env` [plugin permission](#plugin-permissions).

**Parameters:**

- `{service}` (`string`) Keyring service name, such as `gh:github.com`.
- `{account}` (`string`) Account name within the service; may be empty.

**Returns:** (`string?`, `string?`) The secret, nil when there is no such entry, or nil plus an error when the keyring cannot be read.

**Example:**

```lua
local token, err = makima.host.keyring_get("gh:github.com", "octocat")
```


## makima.image {#makima-image}

Small building blocks for working with images: probe metadata, decode
pixels, resize, and encode back to bytes. Plugins compose these freely.

Decoding is guarded against pixel-bomb attacks (50 MP limit).

```lua
local img = makima.image.decode(raw_bytes)
local small = img:resize(1024, 768)
local png = small:encode("png")
```

---

### `makima.image.probe()` {#makima-image-probe}

```lua
makima.image.probe({data})
```

Read image metadata (format, dimensions) from raw bytes without fully
decoding the pixels. Much faster than `decode` when you only need to
check the size or format.

Returns a table with `format` (string), `width` (integer), `height`
(integer), or `(nil, err)` if the bytes are not a recognized image.

**Parameters:**

- `{data}` (`string|buffer`) Raw image bytes.

**Returns:** (`table?`, `string?`) Info table, or `(nil, err)` on failure.

**Example:**

```lua
local info, err = makima.image.probe(raw_bytes)
if err then error(err) end
print(info.format, info.width, info.height)
```

---

### `makima.image.decode()` {#makima-image-decode}

```lua
makima.image.decode({data})
```

Decode raw image bytes into an Image handle you can resize and re-encode.
Images larger than 50 megapixels are rejected to prevent memory bombs.

**Parameters:**

- `{data}` (`string|buffer`) Raw image bytes.

**Returns:** ([`makima.image.Image?`](#makima-image-Image), `string?`) Decoded image, or `(nil, err)` on failure.

**Example:**

```lua
local img, err = makima.image.decode(raw_bytes)
if err then error(err) end
print(img:width() .. "x" .. img:height())
```


## makima.image.Image {#makima-image-Image}

A decoded image you can inspect, resize, and re-encode.

Get one from `makima.image.decode()`. The image data lives in memory
until the handle is garbage collected.

---

### `Image:width()` {#Image-width}

```lua
Image:width()
```

Get the width of the image in pixels.

**Returns:** (`integer`) Width in pixels.

---

### `Image:height()` {#Image-height}

```lua
Image:height()
```

Get the height of the image in pixels.

**Returns:** (`integer`) Height in pixels.

---

### `Image:resize()` {#Image-resize}

```lua
Image:resize({max_w}, {max_h})
```

Shrink the image to fit inside {max_w} x {max_h}, keeping the aspect
ratio. If the image already fits, it is returned as-is. Never upscales.

**Parameters:**

- `{max_w}` (`integer`) Maximum width in pixels. Must be positive.
- `{max_h}` (`integer`) Maximum height in pixels. Must be positive.

**Returns:** ([`makima.image.Image`](#makima-image-Image)) A new image handle (or the same one if no resize was needed).

**Example:**

```lua
local img = makima.image.decode(raw_bytes)
local small = img:resize(800, 600)
local encoded = small:encode("jpeg")
```

---

### `Image:encode()` {#Image-encode}

```lua
Image:encode({format})
```

Encode the image into raw bytes in the given format. Use this to prepare
images for sending over the network or writing to disk.

**Parameters:**

- `{format}` (`string`) Output format: `"png"`, `"jpeg"`, or `"jpg"`.

**Returns:** (`string`) Encoded image bytes.

**Example:**

```lua
local bytes = img:encode("png")
-- bytes is a Lua string containing the raw PNG data
```


## makima.interpreter {#makima-interpreter}

Run Python code in a memory-safe, time-limited sandbox.

The sandbox uses the monty interpreter. Python code can call back into
Lua-defined tools, and stdout is streamed line by line.

```lua
local r, err = makima.interpreter.run("print('hello')", {
  timeout = 10,
  max_memory_mb = 128,
  on_output = function(line) print(line) end,
})
```

---

### `makima.interpreter.run()` {#makima-interpreter-run}

```lua
makima.interpreter.run({code}, {opts})
```

Run Python code in a sandboxed interpreter with memory and time limits.
Stdout lines are streamed to your {on_output} callback as they are produced.
If the Python code calls tools, those calls are dispatched to the Lua
functions you provide in {opts}.tools.

The result table has optional fields: `stdout` (string, trimmed combined
output) and `output` (string, the final expression value). On error, the
table is empty and the second return value is the error message.

Requires the `run` [plugin permission](#plugin-permissions).

**Parameters:**

- `{code}` (`string`) Python source code to execute.
- `{opts}` (`table`) Required fields:
  - `timeout` (`integer`) execution time limit in seconds.
  - `max_memory_mb` (`integer`) memory limit in megabytes.
  - `on_output` (`function`) called with each stdout line (string) as it is
    produced. Must not yield.

  Optional fields:

  - `preamble` (`string?`) Python source (imports, helpers) compiled ahead of
    {code}. Tracebacks are rebased so line 1 is {code} line 1.
  - `tools` (`table?`) map of `name -> function` for tools the sandbox may call.
    Each function receives the tool input table and must return `(string)` or
    `(nil, err)`. Tool calls are batched and dispatched concurrently.

**Returns:** (`table`, `string?`) Result table, plus an error string on failure.

**Example:**

```lua
local result, err = makima.interpreter.run("print(2 + 2)", {
  timeout = 30,
  max_memory_mb = 256,
  on_output = function(line) print("py: " .. line) end,
})
if err then error(err) end
if result.stdout then print(result.stdout) end
```


## makima.json {#makima-json}

JSON encoding, decoding, and schema validation. Encode Lua
tables to JSON strings, decode JSON back into tables, and
optionally validate data against a JSON Schema.

```lua
local s = makima.json.encode({ ok = true })
local t = makima.json.decode(s)
```

---

### `makima.json.encode()` {#makima-json-encode}

```lua
makima.json.encode({value})
```

Turn a Lua value into a JSON string. Tables, strings, numbers,
booleans, and nil all work. Functions and userdata cannot be
serialized.

**Parameters:**

- `{value}` (`any`) Lua value to encode.

**Returns:** (`string?`, `string?`) JSON string, or nil plus an error.

**Example:**

```lua
local s, err = makima.json.encode({ name = "makima", version = 1 })
print(s) -- {"name":"makima","version":1}
```

---

### `makima.json.decode()` {#makima-json-decode}

```lua
makima.json.decode({str})
```

Parse a JSON string into a Lua value. Objects become tables and
arrays become 1-indexed sequences.

**Parameters:**

- `{str}` (`string`) JSON string to decode.

**Returns:** (`any?`, `string?`) Decoded value, or nil plus an error.

**Example:**

```lua
local t, err = makima.json.decode('{"x": 42}')
print(t.x) -- 42
```

---

### `makima.json.schema_validator()` {#makima-json-schema_validator}

```lua
makima.json.schema_validator({schema})
```

Compile a JSON Schema into a reusable validator object. Supports
draft-07, 2019-09, and 2020-12. Schema errors show up right away so
you catch mistakes before doing any real work.

**Parameters:**

- `{schema}` (`table`) JSON Schema as a Lua table.

**Returns:** ([`makima.json.SchemaValidator?`](#makima-json-SchemaValidator), `string?`) Validator, or nil plus an error.

**Example:**

```lua
local v, err = makima.json.schema_validator({
  type = "object",
  properties = { name = { type = "string" } },
  required = { "name" },
})
local errs = v:validate({ name = "makima" })
assert(errs == nil)
```


## makima.json.SchemaValidator {#makima-json-SchemaValidator}

A compiled JSON Schema validator. Create one with `makima.json.schema_validator()` and reuse it to validate many values without recompiling the schema each time.

---

### `SchemaValidator:validate()` {#SchemaValidator-validate}

```lua
SchemaValidator:validate({value})
```

Check {value} against the compiled schema. Returns nil when the value is valid. When validation fails, returns a list of human-readable error strings.

**Parameters:**

- `{value}` (`any`) The Lua value to validate.

**Returns:** (`table?`) Array of error strings, or nil if valid.

**Example:**

```lua
local errs = validator:validate({ name = 123 })
if errs then
for _, msg in ipairs(errs) do print(msg) end
end
```


## makima.keymap {#makima-keymap}

Key mappings, modeled after `vim.keymap`. If you have written a
Neovim keymap plugin before, this will feel familiar.

```lua
makima.keymap.set("n", "<C-t>", function()
  print("hello")
end, { desc = "Say hello" })
```

---

### `makima.keymap.set()` {#makima-keymap-set}

```lua
makima.keymap.set({mode}, {lhs}, {rhs}, {opts?})
```

Bind a key to a Lua function, just like `vim.keymap.set`. Only
normal mode (`"n"`) is supported right now. If {lhs} is already
mapped, the old binding is replaced and a warning is logged.

**Parameters:**

- `{mode}` (`string`) Mode letter. Currently only `"n"` is accepted.
- `{lhs}` (`string`) Key in Vim notation, e.g. `"<C-t>"`, `"<Space>"`, `"a"`.
- `{rhs}` (`function`) Called as `rhs(nil, ctx)` when the key is pressed.

  `ctx` is a trusted invocation-scoped LuaCtx, or nil if the host has no


  agent target. Resolve the callback's agent with `ctx:agent()`.


  Later focus changes do not retarget an already resolved context. Retaining


  the context does not extend its authority beyond the callback.

- `{opts?}` (`table?`) Options:
  - `desc` (`string`) short description shown in the keymap list.

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
makima.keymap.set("n", "<C-t>", function()
  print("toggle!")
end, { desc = "Toggle panel" })
```

---

### `makima.keymap.del()` {#makima-keymap-del}

```lua
makima.keymap.del({mode}, {lhs})
```

Remove the mapping for {lhs} in {mode}. Does nothing if no mapping
exists for that key.

**Parameters:**

- `{mode}` (`string`) Mode letter (reserved for future modes).
- `{lhs}` (`string`) Key to unmap, in Vim notation.

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error. Returns nil and an error when called from a delegated tool handler.

**Example:**

```lua
makima.keymap.del("n", "<C-t>")
```


## makima.log {#makima-log}

Structured logging for plugins.

Each call emits a tracing event tagged with the calling plugin's name.
Messages show up in makima's log output, which you can view with `makima --log`.

```lua
makima.log.info("ready")
makima.log.warn("something looks off")
```

---

### `makima.log.debug()` {#makima-log-debug}

```lua
makima.log.debug({msg})
```

Emit a DEBUG-level log message. Useful for development and troubleshooting.
The message is tagged with the plugin name automatically.

**Parameters:**

- `{msg}` (`string`) Message to log.

**Example:**

```lua
makima.log.debug("loaded " .. #items .. " items")
```

---

### `makima.log.info()` {#makima-log-info}

```lua
makima.log.info({msg})
```

Emit an INFO-level log message. Good for normal operational events.

**Parameters:**

- `{msg}` (`string`) Message to log.

**Example:**

```lua
makima.log.info("plugin initialized")
```

---

### `makima.log.warn()` {#makima-log-warn}

```lua
makima.log.warn({msg})
```

Emit a WARN-level log message. Use for recoverable problems.

**Parameters:**

- `{msg}` (`string`) Message to log.

**Example:**

```lua
makima.log.warn("config file missing, using defaults")
```

---

### `makima.log.error()` {#makima-log-error}

```lua
makima.log.error({msg})
```

Emit an ERROR-level log message. Use for failures that need attention.

**Parameters:**

- `{msg}` (`string`) Message to log.

**Example:**

```lua
makima.log.error("failed to connect to API")
```


## makima.match {#makima-match}

Fuzzy matching via nucleo, the same matcher makima's built-in pickers use.

Use it for type-ahead search over a plugin's own item list.

```lua
local m = makima.match.fuzzy("gh pr", "gh pr 441 review")
if m then
  print(m.score) -- matched codepoint offsets in m.indices
end
```

---

### `makima.match.fuzzy()` {#makima-match-fuzzy}

```lua
makima.match.fuzzy({query}, {text})
```

Compatibility/raw-score fuzzy matching for {query} against {text} with
nucleo, the same matcher makima's built-in pickers use. Every
whitespace-separated word in {query} must match somewhere in {text}; word
order does not matter. An empty or whitespace-only query matches everything.

**Parameters:**

- `{query}` (`string`) Search words, whitespace separated.
- `{text}` (`string`) Text to search in.

**Returns:** (`table|nil`) nil when no word matches. On a match: {score = number, indices = {…}} where indices are the 1-based codepoint offsets of the matched characters, ascending.

**Example:**

```lua
local m = makima.match.fuzzy("gh pr", "gh pr 441 review")
if m then
  print(m.score) -- matched codepoint offsets in m.indices
end
```

---

### `makima.match.completion()` {#makima-match-completion}

```lua
makima.match.completion({query}, {label}, {opts?})
```

Match {query} against a completion {label}, returning one-based Lua
indices and the textual ranking metadata used by built-in completion.

The optional {opts} table accepts `case = "smart" | "ignore" | "respect"`
and `normalization = "smart" | "never"`; omitted or nil fields use the
defaults. Indices and `ranking.start_index` are one-based Unicode codepoint
positions. The other ranking fields are non-negative integers. The result
shape is `{ indices, ranking = { quality_rank, boundary_rank, start_index,
gap_count, span_length, unmatched_suffix, fuzzy_score } }`.

**Parameters:**

- `{query}` (`string`) Search pattern.
- `{label}` (`string`) Completion label.
- `{opts?}` (`table?`) Optional matching options.

**Returns:** (`table|nil`) A match with one-based codepoint `indices` and a
`ranking` table, or nil when the label does not match.

---

### `makima.match.compare()` {#makima-match-compare}

```lua
makima.match.compare({left}, {right})
```

Compare the textual ranking of two results from {completion}.

Returns -1 when {left} sorts first, 0 for equal textual ranking, and 1
when {right} sorts first. Source priority, provider order, list grouping,
and other caller-owned policy are intentionally not compared. Use the
result as a boolean predicate for `table.sort`:
`function(a, b) return makima.match.compare(a, b) < 0 end`.

**Parameters:**

- `{left}` (`table`) Match result returned by `makima.match.completion`.
- `{right}` (`table`) Match result returned by `makima.match.completion`.

**Returns:** (`integer`) -1, 0, or 1.


## makima.api.mode {#makima-api-mode}

`makima.api.mode`: define, override and list agent modes. Built-in
`build` and `plan` are pre-registered, so overrides use the same call
as defining a custom mode. Switch an agent with `Agent:set_mode`.

---

### `makima.api.mode.define()` {#makima-api-mode-define}

```lua
makima.api.mode.define({opts})
```

Defines a new mode or fully overrides an existing one (built-ins included).

**Parameters:**

- `{opts}` (`table`) { name = "audit", label = "[AUDIT]", system_prompt = string|fn(ctx)->string, restrict_write_to = string, tools = string[] }

**Returns:** (`boolean`, `string|nil`) `true` on success, or nil and an error.

**Example:**

```lua
makima.api.mode.define({ name = "plan", label = "[PLAN]", tools = { "read", "write" } })
```

---

### `makima.api.mode.list()` {#makima-api-mode-list}

```lua
makima.api.mode.list()
```

Lists all known modes as `{ name, label }` pairs (built-ins first).

**Returns:** (`table|nil`, `string|nil`) Array of `{name, label}`, or nil and an error.

**Example:**

```lua
for _, m in ipairs(makima.api.mode.list()) do print(m.name) end
```

---

### `makima.api.mode.reset()` {#makima-api-mode-reset}

```lua
makima.api.mode.reset({name?})
```

Restores a built-in's default definition, dropping any plugin override.
With no argument, resets every built-in.

**Parameters:**

- `{name?}` (`string|nil`) Mode id to reset ("build" or "plan").

**Returns:** (`boolean`, `string|nil`) `true` on success, or nil and an error.

**Example:**

```lua
makima.api.mode.reset("plan")
```


## makima.model {#makima-model}

Model catalog reads that address no agent. `Agent:config`,
`Agent:set_model` and `Agent:available_models` act on an agent.

---

### `makima.model.info()` {#makima-model-info}

```lua
makima.model.info({spec})
```

Describes a model by spec without addressing any agent. Unknown specs
return an error.

**Parameters:**

- `{spec}` (`string`) Model spec, e.g. `"anthropic/claude-sonnet-4-20250514"`.

**Returns:** (`table|nil`, `string|nil`) `{spec, provider, id, tier, supports_thinking,
  supports_fast, context_window, max_output_tokens, pricing}`, or nil and an
  error. `max_output_tokens` is nil when unknown. `pricing` is
  `{input, output, cache_write, cache_read}` in USD per million tokens.

**Example:**

```lua
local info = assert(makima.model.info("anthropic/claude-sonnet-4-20250514"))
```

---

### `makima.model.thinking_levels()` {#makima-model-thinking_levels}

```lua
makima.model.thinking_levels()
```

Lists the thinking levels a selection accepts, from `off` to `max`. A
token budget is also accepted.

**Returns:** table Array of thinking level names.

**Example:**

```lua
local levels = makima.model.thinking_levels()
```


## makima.net {#makima-net}

HTTP clients: a guarded `request` for web content and a raw, streaming
`send` for trusted plugin code.

With `request`, all traffic goes over HTTPS (plain HTTP is upgraded),
private and metadata IP addresses are blocked to prevent SSRF,
including after a redirect, and failed requests (5xx) are retried.
Hosts listed in the `net.allowed_private_hosts` config option are
exempt from the block. `send` has none of these guards.

```lua
local res, err = makima.net.request("https://example.com")
if res then print(res.body) end
```

---

### `makima.net.request()` {#makima-net-request}

```lua
makima.net.request({url}, {opts?})
```

Make an HTTP request and return the response body. Plain `http://`
URLs are automatically upgraded to `https://`. Requests to private
or metadata IP addresses are blocked for safety, unless the host is
listed in `net.allowed_private_hosts`.

{opts} fields:
  `method` (string) HTTP verb (default `"GET"`).
  `headers` (table) Header name/value pairs.
  `body` (string|buffer) Request body, sent as is.
  `timeout` (integer) Timeout in seconds, max 120 (default 30).
  `max_bytes` (integer) Max response size in bytes (default 5 MB).
  `retry` (integer) Retries on 5xx errors (default 3).

The response table has three fields: `body` (string), `status`
(integer), and `content_type` (string). `body` holds the raw bytes,
so a binary download can go straight to `makima.fs.write`.

Requires the `net` [plugin permission](#plugin-permissions).

**Parameters:**

- `{url}` (`string`) URL starting with `http://` or `https://`.
- `{opts?}` (`table?`) Request options (see above).

**Returns:** (`table?`, `string?`) Response table, or nil plus an error string.

**Example:**

```lua
local res, err = makima.net.request("https://httpbin.org/get")
if err then
  print("failed: " .. err)
else
  print(res.status, res.body)
end
```

---

### `makima.net.send()` {#makima-net-send}

```lua
makima.net.send({request})
```

Send one HTTP request and return a streaming response. This is the raw
primitive for trusted plugin code, such as LLM providers: there is no
private address blocking, no https upgrade, no retry, no size cap, and
redirects are not followed (a 3xx comes back as is). Never pass it a URL
that came from the model without checking it first; use
`makima.net.request` for that.

The response arrives with the first body byte or when the transfer ends,
not at the end of the headers. A server that sends headers and then stalls
blocks `send` until its first byte or until `low_speed_timeout` fires.

A connection that is refused or a host name that does not resolve fails
with an error starting `could not connect`. A `read()` that times out
fails with an error containing `idle timeout`. The providers plugin's
`http.classify` reads these to name a provider error's retry class.

{request} fields:
  `url` (string) Required, `http://` or `https://`.
  `method` (string) HTTP verb (default `"GET"`).
  `headers` (table) Header name/value pairs. `User-Agent` defaults to `makima/<version>`.
  `body` (string|buffer) Request body, binary safe.
  `connect_timeout` (number) Seconds to connect (default 10).
  `low_speed_timeout` (number) Seconds without any received byte before
  the transfer fails (default 30).
  `idle_timeout` (number) Seconds a single `read()` may wait (default 300).
  `unix_socket` (string) Connect through this Unix domain socket instead of
  TCP. `url` still supplies the path and the Host header. Unix only.

Requires the `net` [plugin permission](#plugin-permissions).

**Parameters:**

- `{request}` (`table`) Request description (see above).

**Returns:** ([`makima.net.Response?`](#makima-net-Response), `string?`) Response handle, or nil plus an error string.

**Example:**

```lua
local res, err = makima.net.send({
  url = "https://api.example.com/v1/stream",
  method = "POST",
  headers = { ["content-type"] = "application/json" },
  body = makima.json.encode({ stream = true }),
})
if not res then return print(err) end
local parts = {}
while true do
  local chunk, read_err = res:read()
  if read_err then print(read_err) end
  if not chunk then break end
  table.insert(parts, chunk)
end
res:close()
```


## makima.net.Response {#makima-net-Response}

A streaming HTTP response from `makima.net.send`.

`status` is the integer status code. `headers` is a table keyed by
lowercase header name, with repeated headers joined by `", "`. Read
the body with `:read()`. Closing or dropping the handle, or the end of
the task that sent the request, closes the connection once the next
byte arrives from the server.

---

### `Response:read()` {#Response-read}

```lua
Response:read()
```

Read the next chunk of the body, up to 64 KiB. Returns nil at the end of
the stream, or nil plus an error when the read fails, when no byte arrives
within `idle_timeout`, or after `close()`.

**Returns:** (`string?`, `string?`) Binary chunk, nil at the end, or nil plus an error string.

**Example:**

```lua
local chunk, err = res:read()
```

---

### `Response:close()` {#Response-close}

```lua
Response:close()
```

Drop the body and close the connection. Safe to call more than once.
The socket itself closes when the next byte arrives from the server, or
when `low_speed_timeout` fires.

**Example:**

```lua
res:close()
```


## makima.session {#makima-session}

Host session primitives. The interactive UI can run several sessions
at once; these functions let plugins list, create, focus, rename, and
delete them. Session management returns `nil, "no interactive UI
attached"` without a UI. `notify` instead targets a live agent mailbox
directly, so it also works under ACP and SDK frontends. A tool handler
cannot create, focus or delete sessions, and the functions that change
a session (`set_title`) accept
only the session its own turn belongs to. Every function that reads or
changes one session takes it explicitly; `ctx:session_id()` names the
invocation's session.

---

### `makima.session.list()` {#makima-session-list}

```lua
makima.session.list()
```

Lists sessions stored for the current project. Answered from a
background scan, so a slow disk never blocks the UI. `open_elsewhere` is
true while another makima instance has the session open. `message_count`
counts main transcript messages only and excludes subagent histories.

**Returns:** (`table|nil`, `string|nil`) Array of `{id, title, updated_at, cwd, message_count, open_elsewhere}`, or nil and an error.

**Example:**

```lua
local stored, err = makima.session.list()
```

---

### `makima.session.list_all()` {#makima-session-list_all}

```lua
makima.session.list_all()
```

Lists stored sessions across every project directory, most recently
updated first. Answered from a background scan, so a slow disk never
blocks the UI. `open_elsewhere` is true while another makima instance has
the session open. `message_count` counts main transcript messages only
and excludes subagent histories.

**Returns:** (`table|nil`, `string|nil`) Array of `{id, title, updated_at, cwd, message_count, open_elsewhere}`, or nil and an error.

**Example:**

```lua
local stored, err = makima.session.list_all()
```

---

### `makima.session.live()` {#makima-session-live}

```lua
makima.session.live()
```

Lists the sessions currently running in this UI. Status is "working",
"needs_input", or "idle". A mailbox follow-up stays "working" without an
intermediate "idle" status.

`message_count` counts main transcript messages only and excludes subagent histories.

**Returns:** (`table|nil`, `string|nil`) Array of `{id, title, status, updated_at, message_count, focused}`, or nil and an error.

**Example:**

```lua
local live, err = makima.session.live()
```

---

### `makima.session.read()` {#makima-session-read}

```lua
makima.session.read({opts})
```

Snapshot of the session you name in `session`.

The returned table:
```lua
{
  id,
  title,             -- nil under headless drivers
  status = "idle" | "working" | "needs_input",
  updated_at,        -- unix seconds
  usage = {
    total = { input, output, cache_creation, cache_read, cost },
    models = { { model, input, output, cache_creation, cache_read, cost }, ... },
  },
}
```

`usage` includes subagent spend. Costs are the values recorded when each
turn was billed and stay nil when unpriced. `models` is sorted by total
tokens, descending, then spec; headless drivers do not split usage by
model and leave it empty. Model, mode and options are on the agent:
`Agent:config(ctx)`; per-turn context size and list price are on the
`TurnEnd` autocmd.

**Parameters:**

- `{opts}` (`table`) Required `session` (string) id.

**Returns:** (`table|nil`, `string|nil`) Snapshot table, or nil and an error.

**Example:**

```lua
local s = makima.session.read({ session = ctx:session_id() })
makima.ui.notify("spent " .. tostring(s.usage.total.cost))
```

---

### `makima.session.focus()` {#makima-session-focus}

```lua
makima.session.focus({id})
```

Switches the UI to the session with {id}. The session must belong to
the current directory and must not be open in another terminal.

**Parameters:**

- `{id}` (`string`) Session id, as returned by `list()` or `live()`.

**Returns:** (`boolean|nil`, `string|nil`) true on success, or nil and an error.

**Example:**

```lua
local _, err = makima.session.focus(id)
```

---

### `makima.session.delete()` {#makima-session-delete}

```lua
makima.session.delete({id})
```

Deletes a session and its stored history, cancelling it first if it
is running. The focused session cannot be deleted.

**Parameters:**

- `{id}` (`string`) Session id to delete.

**Returns:** (`boolean|nil`, `string|nil`) true on success, or nil and an error.

**Example:**

```lua
local _, err = makima.session.delete(id)
```

---

### `makima.session.new()` {#makima-session-new}

```lua
makima.session.new({opts?})
```

Starts a new session in the current project.

**Parameters:**

- `{opts?}` (`table?`) Optional fields: prompt (string) first user message

  to submit right away; focus (boolean) switch the UI to the new session.


**Returns:** (`string|nil`, `string|nil`) New session id, or nil and an error.

**Example:**

```lua
local id, err = makima.session.new({ prompt = "fix the tests", focus = true })
```

---

### `makima.session.notify()` {#makima-session-notify}

```lua
makima.session.notify({text}, {opts})
```

Reports {text} to a live session without creating a user turn. The
observation waits for the session's next agent run.

**Parameters:**

- `{text}` (`string`) What to report. Must not be blank.
- `{opts}` (`table`) Options:
  - `session` (`string`) id of a live session.
  - `wake` (`boolean`) start a TUI turn when it next becomes idle (default false).

**Returns:** (`boolean|nil`, `string|nil`) true, or nil and an error.

**Example:**

```lua
makima.session.notify("[monitor] deploy failed", { session = id, wake = true })
```

---

### `makima.session.set_title()` {#makima-session-set_title}

```lua
makima.session.set_title({opts})
```

Renames a session, live or stored.

**Parameters:**

- `{opts}` (`table`) Required fields: id (string) session to rename;
  - `title` (`string`) the new title.

**Returns:** (`boolean|nil`, `string|nil`) true on success, or nil and an error.

**Example:**

```lua
local _, err = makima.session.set_title({ id = id, title = "refactor" })
```

---

### `makima.session.set_default_thinking()` {#makima-session-set_default_thinking}

```lua
makima.session.set_default_thinking({mode})
```

Persists the default thinking setting for new sessions without changing
any live session. Every frontend starts a blank session on it, ahead of
`always_thinking`. `mode` is a thinking setting as `Agent:set_model`
accepts it. Print saves nothing, so there it returns an error and leaves
the saved default alone.

**Parameters:**

- `{mode}` (`string`) The thinking setting.

**Returns:** (`table|nil`, `string|nil`) `{mode}` with the normalized setting, or nil and an error.

**Example:**

```lua
makima.session.set_default_thinking("medium")
```


## makima.credentials {#makima-credentials}

Credential stores for plugins. A provider's own credentials are its
ctx's `credentials`; state a plugin shares across its providers, such
as one sign-in, goes in a plugin store.

---

### `makima.credentials.open()` {#makima-credentials-open}

```lua
makima.credentials.open({ctx}, {key})
```

Open plugin store {key} of {ctx}: a credentials file only that ctx and
the ctxs it was narrowed from reach. A provider ctx opens its plugin
ctx's stores.

**Parameters:**

- `{ctx}` ([`makima.Ctx`](#makima-Ctx)) The plugin's ctx, or a provider ctx.
- `{key}` (`string`) ASCII letters, digits, `-`, `_` or `.`, not starting with `.`.

**Returns:** ([`makima.credentials.Store`](#makima-credentials-Store)) The store.

**Example:**

```lua
local account = makima.credentials.open(ctx, "account")
local saved = account:read()
```


## makima.credentials.Store {#makima-credentials-Store}

A credentials file: a provider's (its ctx's `credentials`) or a
plugin's (`makima.credentials.open`). The value is whatever JSON table
is stored, such as `{ api_key }` or `{ access, refresh, expires }`.

---

### `Store:read()` {#Store-read}

```lua
Store:read()
```

Read the stored credentials.

**Returns:** (`table?`, `string?`) The stored table, nil when there is none or it does not parse, or nil plus an error.

**Example:**

```lua
local saved = ctx.credentials:read()
```

---

### `Store:write()` {#Store-write}

```lua
Store:write({value})
```

Replace the stored credentials with {value}. The write is atomic and the
file is readable only by the user.

**Parameters:**

- `{value}` (`table`) Any JSON-compatible table.

**Returns:** (`boolean?`, `string?`) true, or nil plus an error.

**Example:**

```lua
local ok, err = ctx.credentials:write({ api_key = key })
```

---

### `Store:delete()` {#Store-delete}

```lua
Store:delete()
```

Delete the stored credentials.

**Returns:** (`boolean?`, `string?`) true if a file was removed, false if there was none, or nil plus an error.

**Example:**

```lua
ctx.credentials:delete()
```

---

### `Store:refresh()` {#Store-refresh}

```lua
Store:refresh({fn})
```

Refresh the stored credentials as one transaction. {fn}`(current)` runs
with a cross-process lock held and `current` reread from disk after the
lock was taken. It returns a new table to store, nil to keep `current`
(another process already refreshed), or nil plus an error.

Concurrent refreshes of one store share a single call of {fn}. The call
runs on its own and is not cancelled with the caller, so a rotated token
that the server already issued is always written. During plugin load,
which nothing cancels, {fn} runs in the first caller instead.

**Parameters:**

- `{fn}` (`function`) `fn(current)` returning the table to store.

**Returns:** (`table?`, `any?`) The stored table after the refresh, or nil plus the error {fn} returned, unchanged.

**Example:**

```lua
local tokens, err = ctx.credentials:refresh(function(current)
  if current and current.expires > makima.time.now().secs * 1000 + 60000 then return nil end
  return exchange(current.refresh)
end)
```

---

### `Store:lock()` {#Store-lock}

```lua
Store:lock({fn})
```

Run {fn} with the store's cross-process lock held, the same
lock `refresh` takes, for work that must not overlap another process
changing these credentials. Unlike `refresh`, every call runs its own
{fn}, nothing is reread or written, and dropping the caller ends {fn}.

**Parameters:**

- `{fn}` (`function`) Called with no arguments.

**Returns:** any Whatever {fn} returns.

**Example:**

```lua
local result, err = ctx.credentials:lock(function()
  return run_login_script()
end)
```


## makima.Ctx {#makima-Ctx}

A plugin's authority. An entry chunk (a bundled plugin, or `init.lua`)
receives its root ctx as `...`; pass it on by reference, such as to
a module's `setup(ctx)`, and narrow it with `with_name`. APIs that
act for the plugin take it, such as `makima.credentials.open` and
`makima.api.register_provider`. Its fields cannot be assigned.

`send` is `makima.net.send` with default timeouts: from
`MAKIMA_NET_TIMEOUT_SECS` (default 30 seconds) on a plugin ctx, from
`[provider]` on a provider ctx. A provider ctx, which a provider's
callbacks receive, also has `id` (the provider id) and `credentials`
(the provider's `makima.credentials.Store`); a `stream` call's ctx also
has `mark_request_delivery`, described on `makima.api.register_provider`.

---

### `Ctx:with_name()` {#Ctx-with_name}

```lua
Ctx:with_name({name})
```

A child ctx whose plugin stores live under {name}, inside this ctx's.
It reaches neither this ctx's stores nor a sibling's. A provider ctx's
child is a plain plugin ctx.

**Parameters:**

- `{name}` (`string`) ASCII letters, digits, `-`, `_` or `.`, not starting with `.`.

**Returns:** ([`makima.Ctx`](#makima-Ctx)) The child ctx.

**Example:**

```lua
local ctx = ...
require("fountain.provider").setup(ctx:with_name("fountain"))
```


## makima.provider.LoginUi {#makima-provider-LoginUi}

The `ui` a provider's `login(ctx, ui, config)` receives.

The CLI or the TUI shows it. `input` and `choice` wait for the user. The notices `instructions`,
`device_code` and `open_browser` return at once. `run` waits for a program on the terminal.

---

### `LoginUi:input()` {#LoginUi-input}

```lua
LoginUi:input({prompt}, {secret?}, {default?})
```

Ask the user for text.

**Parameters:**

- `{prompt}` (`string`) What to ask for.
- `{secret?}` (`boolean?`) Hide the input, for keys and passwords.
- `{default?}` (`string?`) Text offered for editing.

**Returns:** (`string?`, `string?`) The entered text, or nil plus an error (`"cancelled"` when the user backed out).

**Example:**

```lua
local key, err = ui:input("API key", true)
```

---

### `LoginUi:choice()` {#LoginUi-choice}

```lua
LoginUi:choice({prompt}, {options})
```

Ask the user to choose one of {options}.

**Parameters:**

- `{prompt}` (`string`) What the choice is about.
- `{options}` (`table`) List of `{ id, label, detail? }`.

**Returns:** (`string?`, `string?`) The chosen id, or nil plus an error.

**Example:**

```lua
local plan = ui:choice("Plan", { { id = "standard", label = "Standard" } })
```

---

### `LoginUi:instructions()` {#LoginUi-instructions}

```lua
LoginUi:instructions({text})
```

Show {text} to the user. Returns at once.

**Parameters:**

- `{text}` (`string`) Instructions to show.

**Returns:** (`boolean?`, `string?`) true, or nil plus an error.

**Example:**

```lua
ui:instructions("Create a key in the dashboard first.")
```

---

### `LoginUi:device_code()` {#LoginUi-device_code}

```lua
LoginUi:device_code({url}, {code})
```

Show a device login: the user opens {url} and enters {code}. Returns at
once, so the provider can poll while the code is on screen.

**Parameters:**

- `{url}` (`string`) Verification URL.
- `{code}` (`string`) Code the user enters.

**Returns:** (`boolean?`, `string?`) true, or nil plus an error.

**Example:**

```lua
ui:device_code("https://example.com/device", "ABCD-1234")
```

---

### `LoginUi:open_browser()` {#LoginUi-open_browser}

```lua
LoginUi:open_browser({url})
```

Ask the frontend to open {url} in the user's browser. It also shows the
URL, in case the browser does not open. Returns at once.

**Parameters:**

- `{url}` (`string`) An `http` or `https` URL.

**Returns:** (`boolean?`, `string?`) true, or nil plus an error.

**Example:**

```lua
ui:open_browser("https://example.com/keys")
```

---

### `LoginUi:run()` {#LoginUi-run}

```lua
LoginUi:run({argv})
```

Run the program {argv} on the user's terminal, handing it input and
output until it exits, for a login that is an interactive program. Only
`makima auth login` has a terminal to hand over; the TUI fails this.

**Parameters:**

- `{argv}` (`table`) The program and its arguments.

**Returns:** (`boolean?`, `string?`) true once the program exited successfully, or nil plus an error.

**Example:**

```lua
local ok, err = ui:run({ "/home/me/.config/makima/providers/corp", "login" })
```


## makima.usage {#makima-usage}

Provider-side quota snapshots. `get` reads the Lua-thread mirror,
`fetch` asks the interactive UI to refresh it, and `on_change` observes
canonical publications. Without an interactive UI `get` stays nil,
`fetch` returns `nil, "no interactive UI attached"`, and callbacks are idle.

---

### `makima.usage.get()` {#makima-usage-get}

```lua
makima.usage.get()
```

Returns the latest provider quota snapshot without starting a network request.
Returns nil until the interactive UI publishes its first snapshot.

**Returns:** table|nil `{provider_id, provider, model, status}` or nil.

---

### `makima.usage.fetch()` {#makima-usage-fetch}

```lua
makima.usage.fetch({opts?})
```

Fetches the active provider account's quota. Ordinary fetches join an
in-flight request; a forced fetch queues one fresh follow-up.

**Parameters:**

- `{opts?}` (`table?`) Optional `{force = boolean}`.

**Returns:** table|nil, string|nil Fresh `{provider_id, provider, model, status}`, or nil and an error.

---

### `makima.usage.on_change()` {#makima-usage-on_change}

```lua
makima.usage.on_change({callback})
```

Registers a synchronous callback for provider quota snapshot changes.
Registrations belong to the plugin and are removed when it unloads.
Callback failures are logged and do not stop other callbacks.

**Parameters:**

- `{callback}` (`function`) Called with `{provider_id, provider, model, status}`.

**Returns:** (`function|nil`, `string|nil`) Idempotent unsubscribe callback. Returns nil and an error when called from a delegated tool handler.


## makima.text {#makima-text}

Text transformation utilities.

Helper functions for converting between text formats.

```lua
local md = makima.text.html_to_markdown(html)
local text = makima.text.to_utf8(bytes)
```

---

### `makima.text.html_to_markdown()` {#makima-text-html_to_markdown}

```lua
makima.text.html_to_markdown({html})
```

Convert an HTML string to Markdown.
Useful for cleaning up web content fetched with `makima.webfetch`.

**Parameters:**

- `{html}` (`string`) HTML source text.

**Returns:** (`string?`, `string?`) Markdown text on success, or nil plus an error message.

**Example:**

```lua
local md, err = makima.text.html_to_markdown("<h1>Hello</h1><p>world</p>")
if err then return end
print(md) -- "# Hello\n\nworld"
```

---

### `makima.text.to_utf8()` {#makima-text-to_utf8}

```lua
makima.text.to_utf8({s})
```

Return {s} as valid UTF-8, with every invalid byte sequence replaced by
U+FFFD. Lua strings are bytes, and `makima.net` and `makima.fs` hand them
over as-is, so decode before measuring or cutting a body as text. An
invalid byte can grow to 3 bytes, so a cut made before decoding can triple
in size.

**Parameters:**

- `{s}` (`string`) Bytes to decode.

**Returns:** (`string`) Valid UTF-8 text, `s` itself when it already is.

**Example:**

```lua
local resp = makima.net.request(url)
local text = makima.text.to_utf8(resp.body)
```

---

### `makima.text.truncate_line()` {#makima-text-truncate_line}

```lua
makima.text.truncate_line({text}, {max_bytes})
```

Truncate one line while preserving a UTF-8 boundary and ending it with
`[line truncated, +N bytes]`, where N is the number of bytes cut.

**Parameters:**

- `{text}` (`string`) The line to truncate.
- `{max_bytes}` (`integer`) Maximum bytes of the result, marker included; a cap smaller than the marker yields the marker alone.

**Returns:** string The truncated line.

---

### `makima.text.truncate_file()` {#makima-text-truncate_file}

```lua
makima.text.truncate_file({text}, {max_lines}, {max_bytes}, {remaining_lines})
```

Truncate file output by line and byte limits, adding `[file truncated]` when needed.

**Parameters:**

- `{text}` (`string`) The file output to truncate.
- `{max_lines}` (`integer`) Maximum lines to retain.
- `{max_bytes}` (`integer`) Maximum bytes to retain.
- `{remaining_lines}` (`integer?`) Number of source lines remaining after the output.

**Returns:** string The truncated file output.

---

### `makima.text.truncation_marker()` {#makima-text-truncation_marker}

```lua
makima.text.truncation_marker({line})
```

Name the tool output a line's trailing truncation marker came from, if any.
Mutation guards use it to reject markers pasted back as file content.

**Parameters:**

- `{line}` (`string`) One line, without its newline.

**Returns:** string? `"read"` for a read or grep line marker (current or legacy format), `"preview"` for a line cut in an offload preview, or nil.

---

### `makima.text.is_offload_notice()` {#makima-text-is_offload_notice}

```lua
makima.text.is_offload_notice({line})
```

Whether a line is the saved-output footer in a tool result.

**Parameters:**

- `{line}` (`string`) One line of tool output.

**Returns:** boolean


## makima.time {#makima-time}

Wall-clock timestamps and relative-age formatting.

`now` returns `{secs, nanosecs}` since the Unix epoch, the same clock
the host uses for session `updated_at` but with full nanosecond
precision. Timestamps are opaque objects you hand to `ago` — not raw
numbers to subtract — so callers get one consistent relative-age
format instead of reimplementing epoch math per plugin.

```lua
local t0 = makima.time.now()
-- work...
print(makima.time.ago(t0))
```

---

### `makima.time.now()` {#makima-time-now}

```lua
makima.time.now()
```

Return the current wall-clock time as a timestamp table.

The table is `{ secs = integer, nanosecs = integer }` where `secs` is
whole seconds since the Unix epoch (`1970-01-01T00:00:00Z`) and `nanosecs`
is the sub-second part, `0 .. 999_999_999`. Split like this, both fields
stay exact in a Lua number (each under 2^53), so the timestamp keeps full
nanosecond precision — the same clock the host uses for session
`updated_at`, just finer than its whole seconds.

Treat the returned table as opaque; pass it to `makima.time.ago` rather than
doing epoch math by hand. To wrap a stored whole-second value (like a
session's `updated_at`) use `makima.time.at`.

**Returns:** (`table`) `{secs = integer, nanosecs = integer}` timestamp.

**Example:**

```lua
local t0 = makima.time.now()
-- ...work...
print(makima.time.ago(t0))
```

---

### `makima.time.at()` {#makima-time-at}

```lua
makima.time.at({secs})
```

Wrap a whole-second Unix timestamp (e.g. a session's `updated_at`) into a
timestamp object readable by `makima.time.ago`.

**Parameters:**

- `{secs}` (`integer`) Whole seconds since the Unix epoch.

**Returns:** (`table`) `{secs = secs, nanosecs = 0}` timestamp.

**Example:**

```lua
print(makima.time.ago(makima.time.at(session.updated_at)))
```

---

### `makima.time.ago()` {#makima-time-ago}

```lua
makima.time.ago({instant}, {reference?})
```

Format {instant} as a live delta like `9s` or `4d 7h 23m 9s`.
{instant} is a timestamp from `makima.time.now` (or `makima.time.at`). If
{reference} is given, age is measured from it instead of the current time.
Fractions truncate to whole seconds, and higher zero units are omitted.
A timestamp in the future renders as `0s`.

**Parameters:**

- `{instant}` (`table`) Timestamp from `makima.time.now`/`makima.time.at`.
- `{reference?}` (`table?`) Timestamp to measure from; defaults to `makima.time.now`.

**Returns:** (`string`) Elapsed delta, e.g. `2m 9s`.

**Example:**

```lua
local t0 = makima.time.now()
-- ...work...
print(makima.time.ago(t0))
```


## makima.timer {#makima-timer}

Recurring callbacks on the runtime's timer pump.

Use `set` for anything that must happen every N seconds: demo loops,
periodic refreshes, watchdogs. Each fire runs as a fresh task, so
callbacks may sleep or do I/O, and fires land exactly on schedule
instead of being polled each frame. Timer fires do not inherit managed
agent authority from their registration task. Timers registered by a
plugin are dropped when the plugin is unloaded.

```lua
local id = makima.timer.set(5, function()
  print("five seconds gone")
end)
```

---

### `makima.timer.set()` {#makima-timer-set}

```lua
makima.timer.set({seconds}, {callback})
```

Schedule {callback} to run every {seconds} on the runtime's timer pump.

Each fire runs as a fresh task, so the callback may be async (`sleep`,
fs, ...) and fires exactly when due: no per-frame polling. Timer fires do
not inherit managed agent authority from the task that registered them.
The callback receives the timer's id as its first argument - use it with
`makima.timer.del` to stop the timer. Do not capture the returned id in the
callback instead: `local id = makima.timer.set(5, function()
makima.timer.del(id) end)` captures nil (a Luau value-capture quirk), which
is why the id is passed as an argument. A fire still running when the
next deadline arrives runs alongside it; every fire task carries the
standard 60s async deadline. Deadlines missed while the runtime was busy
coalesce into a single fire.

Registration is synchronous - nothing runs until the first deadline - so
calling this from `init.lua` or a command is safe. Delegated tool tasks,
including their async children, cannot register timers.

**Parameters:**

- `{seconds}` (`number`) Interval between fires, in seconds. Must be a finite number > 0 and < 1e9.
- `{callback}` (`function`) Called on each fire with the timer id. May be async.

**Returns:** string|nil Error on failure.

**Example:**

```lua
local runs = 0
makima.timer.set(1, function(id)
  runs = runs + 1
  if runs == 10 then
    makima.timer.del(id)
  end
end)
```

---

### `makima.timer.del()` {#makima-timer-del}

```lua
makima.timer.del({id})
```

Stop the timer {id}. Does nothing for an unknown id. A fire that already
started keeps running out.

**Parameters:**

- `{id}` (`integer`) Id returned by `makima.timer.set`.

**Example:**

```lua
local id = makima.timer.set(5, on_tick)
-- later
makima.timer.del(id)
```


## makima.treesitter {#makima-treesitter}

Tree-sitter parsing and query API.

Mirrors `vim.treesitter` from Neovim, so plugins can be shared between the two.
Start with `get_parser()` to parse source code, then use `get_node_text()` and
the `query` sub-module to extract information from the syntax tree.

```lua
local parser, err = makima.treesitter.get_parser(source, "lua")
local trees = parser:parse()
local root = trees[1]:root()
```

---

### `makima.treesitter.get_parser()` {#makima-treesitter-get_parser}

```lua
makima.treesitter.get_parser({source}, {lang})
```

Creates a `LanguageTree` for {source} using the grammar named {lang}.
This is the main entry point for parsing source code with tree-sitter.
Signature matches `vim.treesitter.get_parser()`, so Neovim plugins can be copy-pasted.

**Parameters:**

- `{source}` (`string`) Source text to parse.
- `{lang}` (`string`) Language name, e.g. `"rust"` or `"lua"`.

**Returns:** ([`LanguageTree|nil`](#makima-treesitter-LanguageTree), `string|nil`) Parser, or nil and an error message.

**Example:**

```lua
local parser, err = makima.treesitter.get_parser(src, "lua")
if err then print("error: " .. err) end
```

---

### `makima.treesitter.get_string_parser()` {#makima-treesitter-get_string_parser}

```lua
makima.treesitter.get_string_parser({source}, {lang})
```

Alias for `get_parser`. Use whichever name you prefer.

**Parameters:**

- `{source}` (`string`) Source text to parse.
- `{lang}` (`string`) Language name.

**Returns:** ([`LanguageTree|nil`](#makima-treesitter-LanguageTree), `string|nil`) Parser, or nil and an error message.

---

### `makima.treesitter.get_node_text()` {#makima-treesitter-get_node_text}

```lua
makima.treesitter.get_node_text({node}, {source})
```

Gets the text that {node} covers in {source}.
Useful when you have a captured node and need the actual source substring.

**Parameters:**

- `{node}` ([`Node`](#makima-treesitter-Node)) The node whose text you want.
- `{source}` (`string`) Original source text the tree was parsed from.

**Returns:** (`string`) Substring covered by the node.

**Example:**

```lua
local text = makima.treesitter.get_node_text(node, source)
print(text)
```

---

### `makima.treesitter.get_node_range()` {#makima-treesitter-get_node_range}

```lua
makima.treesitter.get_node_range({node})
```

Returns the range of {node} as four 0-based integers: start_row, start_col, end_row, end_col.

**Parameters:**

- `{node}` ([`Node`](#makima-treesitter-Node)) The node to query.

**Returns:** (`integer`, `integer`, `integer`, `integer`) start_row, start_col, end_row, end_col.

**Example:**

```lua
local sr, sc, er, ec = makima.treesitter.get_node_range(node)
```

---

### `makima.treesitter.get_range()` {#makima-treesitter-get_range}

```lua
makima.treesitter.get_range({node})
```

Returns a six-element table for {node}: `{start_row, start_col, start_byte, end_row, end_col, end_byte}`.
This gives you byte offsets in addition to row/column positions.

**Parameters:**

- `{node}` ([`Node`](#makima-treesitter-Node)) The node to query.

**Returns:** (`table`) Six-element array: start_row, start_col, start_byte, end_row, end_col, end_byte.

**Example:**

```lua
local r = makima.treesitter.get_range(node)
print("bytes: " .. r[3] .. "-" .. r[6])
```

---

### `makima.treesitter.is_ancestor()` {#makima-treesitter-is_ancestor}

```lua
makima.treesitter.is_ancestor({dest}, {source})
```

Checks whether {dest} is an ancestor of {source} (or the same node).
Walks up from {source} toward the root looking for {dest}.

**Parameters:**

- `{dest}` ([`Node`](#makima-treesitter-Node)) Potential ancestor node.
- `{source}` ([`Node`](#makima-treesitter-Node)) Node to check ancestry for.

**Returns:** (`boolean`)

---

### `makima.treesitter.is_in_node_range()` {#makima-treesitter-is_in_node_range}

```lua
makima.treesitter.is_in_node_range({node}, {line}, {col})
```

Checks whether the 0-based position ({line}, {col}) falls inside {node}.
Handy for cursor-position checks.

**Parameters:**

- `{node}` ([`Node`](#makima-treesitter-Node)) Node to test against.
- `{line}` (`integer`) 0-based line number.
- `{col}` (`integer`) 0-based column number.

**Returns:** (`boolean`)

---

### `makima.treesitter.node_contains()` {#makima-treesitter-node_contains}

```lua
makima.treesitter.node_contains({node}, {range})
```

Checks whether {node} fully contains the given {range}.

**Parameters:**

- `{node}` ([`Node`](#makima-treesitter-Node)) Node to test.
- `{range}` (`table`) Four-element array `{start_row, start_col, end_row, end_col}`.

**Returns:** (`boolean`)

---

### `makima.treesitter.get_node()` {#makima-treesitter-get_node}

```lua
makima.treesitter.get_node({opts?})
```

Placeholder for cursor-based node lookup (not yet implemented, always returns nil).

**Parameters:**

- `{opts?}` (`table?`) Options (currently unused).

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Always nil.


## makima.treesitter.language {#makima-treesitter-language}

Language registry for tree-sitter grammars.

Mirrors `vim.treesitter.language`. Use these functions to register grammars,
map filetypes to languages, and inspect available node types.

```lua
makima.treesitter.language.add("lua")
makima.treesitter.language.register("lua", "luau")
```

---

### `makima.treesitter.language.add()` {#makima-treesitter-language-add}

```lua
makima.treesitter.language.add({lang}, {opts?})
```

Registers {lang} for use with tree-sitter.
Call this to confirm a language grammar is available. Throws if {lang} is unknown.
Custom grammar paths are not yet supported.

**Parameters:**

- `{lang}` (`string`) Language name, e.g. `"rust"`.
- `{opts?}` (`table?`) Options table (the `path` key is not yet supported).

**Example:**

```lua
makima.treesitter.language.add("lua")
```

---

### `makima.treesitter.language.register()` {#makima-treesitter-language-register}

```lua
makima.treesitter.language.register({lang}, {filetype})
```

Associates {lang} with one or more filetypes, so you can look up the right
parser language for a given filetype later with `get_lang()`.

**Parameters:**

- `{lang}` (`string`) Language name.
- `{filetype}` (`string|table`) A single filetype string or an array of filetype strings.

**Example:**

```lua
makima.treesitter.language.register("typescript", { "ts", "tsx" })
```

---

### `makima.treesitter.language.get_lang()` {#makima-treesitter-language-get_lang}

```lua
makima.treesitter.language.get_lang({filetype})
```

Looks up the tree-sitter language name for {filetype}.
Returns the registered language, or falls back to {filetype} itself if
a grammar with that name exists. Returns nil when nothing matches.

**Parameters:**

- `{filetype}` (`string`) Filetype to look up, e.g. `"ts"`.

**Returns:** (`string|nil`) Language name, or nil.

**Example:**

```lua
local lang = makima.treesitter.language.get_lang("tsx")
if lang then print(lang) end -- "typescript"
```

---

### `makima.treesitter.language.get_filetypes()` {#makima-treesitter-language-get_filetypes}

```lua
makima.treesitter.language.get_filetypes({lang})
```

Returns all filetypes that have been registered for {lang}.

**Parameters:**

- `{lang}` (`string`) Language name.

**Returns:** (`table`) Array of filetype strings.

**Example:**

```lua
local fts = makima.treesitter.language.get_filetypes("typescript")
-- { "ts", "tsx" }
```

---

### `makima.treesitter.language.inspect()` {#makima-treesitter-language-inspect}

```lua
makima.treesitter.language.inspect({lang})
```

Returns metadata about the grammar for {lang}.
Useful for debugging or discovering which node types and fields a grammar defines.

**Parameters:**

- `{lang}` (`string`) Language name.

**Returns:** (`table`) Table with keys `abi_version` (integer), `node_types` (string[]), `fields` (string[]).

**Example:**

```lua
local info = makima.treesitter.language.inspect("lua")
print("ABI: " .. info.abi_version)
for _, nt in ipairs(info.node_types) do print(nt) end
```


## makima.treesitter.query {#makima-treesitter-query}

Query compilation and lookup.

Mirrors `vim.treesitter.query`. Use `parse()` to compile a tree-sitter
query string into a `Query` object you can run against parsed trees.

```lua
local q = makima.treesitter.query.parse("lua", "(string) @str")
```

---

### `makima.treesitter.query.parse()` {#makima-treesitter-query-parse}

```lua
makima.treesitter.query.parse({lang}, {query})
```

Compiles a tree-sitter query string for {lang}.
Throws if the language is unknown or the query has a syntax error.

**Parameters:**

- `{lang}` (`string`) Language name, e.g. `"lua"`.
- `{query}` (`string`) Tree-sitter S-expression query.

**Returns:** ([`Query`](#makima-treesitter-Query)) Compiled query object.

**Example:**

```lua
local q = makima.treesitter.query.parse("lua", "(identifier) @id")
```

---

### `makima.treesitter.query.get()` {#makima-treesitter-query-get}

```lua
makima.treesitter.query.get({lang}, {name})
```

Looks up a named built-in query for {lang} (not yet implemented, always returns nil).

**Parameters:**

- `{lang}` (`string`) Language name.
- `{name}` (`string`) Query name, e.g. `"highlights"`.

**Returns:** ([`Query|nil`](#makima-treesitter-Query)) Query object, or nil if not found.


## makima.treesitter.Query {#makima-treesitter-Query}

A compiled tree-sitter query.

Get one by calling `makima.treesitter.query.parse(lang, query_string)`.
Then use `:iter_captures()` or `:iter_matches()` to run it against a syntax tree.

```lua
local q = makima.treesitter.query.parse("lua", "(identifier) @id")
for idx, node, meta in q:iter_captures(root, source) do
  print(node:type())
end
```

---

### `Query:iter_captures()` {#Query-iter_captures}

```lua
Query:iter_captures({node}, {source}, {start_row?}, {stop_row?})
```

Iterates over every capture matched by this query. Each call to the returned iterator yields `(capture_index, node, metadata, match, active)`. Use this when you care about individual captures rather than whole pattern matches.

**Parameters:**

- `{node}` ([`Node`](#makima-treesitter-Node)) Root node to search within.
- `{source}` (`string`) Source text the tree was parsed from.
- `{start_row?}` (`integer`) Only match rows >= this value (0-based).
- `{stop_row?}` (`integer`) Only match rows < this value (0-based).

**Returns:** (`function`) Iterator yielding (integer, Node, table, table, integer).

**Example:**

```lua
local q = makima.treesitter.query.parse("lua", "(identifier) @id")
for idx, node, meta in q:iter_captures(root, source) do
  print(idx, node:type())
end
```

---

### `Query:iter_matches()` {#Query-iter_matches}

```lua
Query:iter_matches({node}, {source}, {start_row?}, {stop_row?})
```

Iterates over every full pattern match in this query. Each call to the returned iterator yields `(pattern_index, captures, metadata, active)` where captures is a table keyed by capture index. Use this when you need all captures for a pattern together.

**Parameters:**

- `{node}` ([`Node`](#makima-treesitter-Node)) Root node to search within.
- `{source}` (`string`) Source text the tree was parsed from.
- `{start_row?}` (`integer`) Only match rows >= this value (0-based).
- `{stop_row?}` (`integer`) Only match rows < this value (0-based).

**Returns:** (`function`) Iterator yielding (integer, table, table, integer).

**Example:**

```lua
local q = makima.treesitter.query.parse("lua", "(function_declaration name: (identifier) @name)"
)
for pat, captures, meta in q:iter_matches(root, source) do
  for cap_idx, nodes in pairs(captures) do
    print(nodes[1]:type())
  end
end
```


## makima.treesitter.Tree {#makima-treesitter-Tree}

A parsed syntax tree.

Obtained from `LanguageTree:parse()` or `LanguageTree:trees()`.
Call `:root()` to get the root node and start traversing.

```lua
local trees = parser:parse()
local root = trees[1]:root()
```

---

### `Tree:root()` {#Tree-root}

```lua
Tree:root()
```

Returns the root node of this tree. This is where you start walking
the syntax tree or running queries.

**Returns:** ([`Node`](#makima-treesitter-Node)) Root node.

**Example:**

```lua
local root = tree:root()
print(root:type()) -- e.g. "chunk" for Lua
```

---

### `Tree:copy()` {#Tree-copy}

```lua
Tree:copy()
```

Returns an independent copy of this tree.
Edits to the copy will not affect the original.

**Returns:** ([`Tree`](#makima-treesitter-Tree)) A new Tree with the same content.


## makima.treesitter.Node {#makima-treesitter-Node}

A single node in a parsed syntax tree.

Nodes are obtained from `Tree:root()`, navigation methods like `:child()`,
or from query captures. Each node knows its type, range, and children.

```lua
local root = tree:root()
print(root:type(), root:child_count())
for child, field in root:iter_children() do
  print(child:type(), field)
end
```

---

### `Node:type()` {#Node-type}

```lua
Node:type()
```

Returns the grammar type name for this node, like `"function_definition"` or `"identifier"`.

**Returns:** (`string`) Grammar type name.

---

### `Node:symbol()` {#Node-symbol}

```lua
Node:symbol()
```

Returns the numeric symbol id for this node's grammar type.
Two nodes with the same type always share the same symbol id.

**Returns:** (`integer`) Symbol id.

---

### `Node:id()` {#Node-id}

```lua
Node:id()
```

Returns a unique string identifier for this specific node in the tree.
Useful for deduplication or as a table key.

**Returns:** (`string`) Node identity string.

---

### `Node:range()` {#Node-range}

```lua
Node:range({include_bytes?})
```

Returns the range of this node as multiple return values.
Without {include_bytes}: `start_row, start_col, end_row, end_col`.
With {include_bytes} set to true: `start_row, start_col, start_byte, end_row, end_col, end_byte`.

**Parameters:**

- `{include_bytes?}` (`boolean`) When true, byte offsets are included in the return values.

**Returns:** (`integer`, `integer`, `integer`, `integer`) Four values, or six when include_bytes is true.

**Example:**

```lua
local sr, sc, er, ec = node:range()
local sr, sc, sb, er, ec, eb = node:range(true)
```

---

### `Node:start()` {#Node-start}

```lua
Node:start()
```

Returns the start position of this node: row, column, and byte offset (all 0-based).

**Returns:** (`integer`, `integer`, `integer`) start_row, start_col, start_byte.

---

### `Node:end_()` {#Node-end_}

```lua
Node:end_()
```

Returns the end position of this node: row, column, and byte offset (all 0-based).

**Returns:** (`integer`, `integer`, `integer`) end_row, end_col, end_byte.

---

### `Node:byte_length()` {#Node-byte_length}

```lua
Node:byte_length()
```

Returns how many bytes this node spans in the source text.

**Returns:** (`integer`) Byte length.

---

### `Node:child()` {#Node-child}

```lua
Node:child({index})
```

Returns the child at position {index} (0-based), including anonymous nodes like punctuation.
Returns nil if {index} is out of bounds.

**Parameters:**

- `{index}` (`integer`) 0-based child index.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Child node, or nil.

---

### `Node:named_child()` {#Node-named_child}

```lua
Node:named_child({index})
```

Returns the named child at position {index} (0-based), skipping anonymous nodes.
Returns nil if {index} is out of bounds.

**Parameters:**

- `{index}` (`integer`) 0-based named child index.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Named child node, or nil.

---

### `Node:child_count()` {#Node-child_count}

```lua
Node:child_count()
```

Returns the total number of children, including anonymous nodes.

**Returns:** (`integer`) Child count.

---

### `Node:named_child_count()` {#Node-named_child_count}

```lua
Node:named_child_count()
```

Returns the number of named children (skipping anonymous punctuation nodes).

**Returns:** (`integer`) Named child count.

---

### `Node:children()` {#Node-children}

```lua
Node:children()
```

Returns all children (named and anonymous) as a Lua table.

**Returns:** (`table`) Array of Node.

**Example:**

```lua
for _, child in ipairs(node:children()) do
  print(child:type())
end
```

---

### `Node:named_children()` {#Node-named_children}

```lua
Node:named_children()
```

Returns all named children as a Lua table, skipping anonymous nodes.

**Returns:** (`table`) Array of Node.

---

### `Node:iter_children()` {#Node-iter_children}

```lua
Node:iter_children()
```

Returns an iterator function that yields `(child, field_name)` for every child.
The field name is nil for children that are not assigned to a grammar field.

**Returns:** (`function`) Iterator yielding (Node, string|nil).

**Example:**

```lua
for child, field in node:iter_children() do
  if field then print(field .. ": " .. child:type()) end
end
```

---

### `Node:field()` {#Node-field}

```lua
Node:field({name})
```

Returns all children assigned to the grammar field {name} as a table.
For example, a function node might have a `"name"` or `"body"` field.

**Parameters:**

- `{name}` (`string`) Field name defined in the grammar.

**Returns:** (`table`) Array of Node.

**Example:**

```lua
local bodies = node:field("body")
```

---

### `Node:parent()` {#Node-parent}

```lua
Node:parent()
```

Returns the parent of this node, or nil if this is the root.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Parent node.

---

### `Node:next_sibling()` {#Node-next_sibling}

```lua
Node:next_sibling()
```

Returns the next sibling (named or anonymous), or nil if this is the last child.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Next sibling.

---

### `Node:prev_sibling()` {#Node-prev_sibling}

```lua
Node:prev_sibling()
```

Returns the previous sibling (named or anonymous), or nil if this is the first child.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Previous sibling.

---

### `Node:next_named_sibling()` {#Node-next_named_sibling}

```lua
Node:next_named_sibling()
```

Returns the next named sibling, skipping anonymous nodes. Returns nil at the end.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Next named sibling.

---

### `Node:prev_named_sibling()` {#Node-prev_named_sibling}

```lua
Node:prev_named_sibling()
```

Returns the previous named sibling, skipping anonymous nodes. Returns nil at the start.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Previous named sibling.

---

### `Node:child_with_descendant()` {#Node-child_with_descendant}

```lua
Node:child_with_descendant({descendant})
```

Finds the direct child of this node that contains {descendant}.
Returns nil if {descendant} is not actually inside this node.

**Parameters:**

- `{descendant}` ([`Node`](#makima-treesitter-Node)) A node that may be a descendant.

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Direct child containing the descendant.

---

### `Node:descendant_for_range()` {#Node-descendant_for_range}

```lua
Node:descendant_for_range({start_row}, {start_col}, {end_row}, {end_col})
```

Finds the smallest node inside this node that spans the given point range.
Includes both named and anonymous nodes.

**Parameters:**

- `{start_row}` (`integer`) Start row (0-based).
- `{start_col}` (`integer`) Start column (0-based).
- `{end_row}` (`integer`) End row (0-based).
- `{end_col}` (`integer`) End column (0-based).

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Smallest node covering the range, or nil.

---

### `Node:named_descendant_for_range()` {#Node-named_descendant_for_range}

```lua
Node:named_descendant_for_range({start_row}, {start_col}, {end_row}, {end_col})
```

Like `descendant_for_range`, but only considers named nodes.

**Parameters:**

- `{start_row}` (`integer`) Start row (0-based).
- `{start_col}` (`integer`) Start column (0-based).
- `{end_row}` (`integer`) End row (0-based).
- `{end_col}` (`integer`) End column (0-based).

**Returns:** ([`Node|nil`](#makima-treesitter-Node)) Smallest named node covering the range, or nil.

---

### `Node:named()` {#Node-named}

```lua
Node:named()
```

Returns true if this is a named node (not anonymous punctuation like `,` or `(`).

**Returns:** (`boolean`)

---

### `Node:extra()` {#Node-extra}

```lua
Node:extra()
```

Returns true if this node is an "extra" (like a comment) that can appear anywhere in the grammar.

**Returns:** (`boolean`)

---

### `Node:missing()` {#Node-missing}

```lua
Node:missing()
```

Returns true if this node is "missing", meaning it was inserted by the parser during error recovery.

**Returns:** (`boolean`)

---

### `Node:has_error()` {#Node-has_error}

```lua
Node:has_error()
```

Returns true if this node or any of its descendants contain a syntax error.

**Returns:** (`boolean`)

---

### `Node:has_changes()` {#Node-has_changes}

```lua
Node:has_changes()
```

Returns true if this node has been marked as changed since the last parse.

**Returns:** (`boolean`)

---

### `Node:equal()` {#Node-equal}

```lua
Node:equal({other})
```

Returns true if this node and {other} are the same node in the tree.

**Parameters:**

- `{other}` ([`Node`](#makima-treesitter-Node)) Node to compare against.

**Returns:** (`boolean`)

---

### `Node:sexpr()` {#Node-sexpr}

```lua
Node:sexpr()
```

Returns the S-expression (lisp-like) string for this node and its children.
Handy for debugging the tree structure.

**Returns:** (`string`) S-expression.

**Example:**

```lua
print(node:sexpr()) -- e.g. "(identifier)"
```

---

### `Node:tree()` {#Node-tree}

```lua
Node:tree()
```

Returns the Tree that this node belongs to.

**Returns:** ([`Tree`](#makima-treesitter-Tree)) The owning tree.


## makima.treesitter.LanguageTree {#makima-treesitter-LanguageTree}

Manages parsing of a source string for a single language.

Obtained from `makima.treesitter.get_parser()` or `makima.treesitter.get_string_parser()`.
Call `:parse()` to get the syntax tree, then use `:root()` on the tree to start walking nodes.

```lua
local parser, err = makima.treesitter.get_parser(source, "lua")
if not err then
  local trees = parser:parse()
  local root = trees[1]:root()
end
```

---

### `LanguageTree:parse()` {#LanguageTree-parse}

```lua
LanguageTree:parse({range?})
```

Parses the source and returns a table containing the resulting Tree.
The tree is cached, so calling this again is cheap.

**Parameters:**

- `{range?}` (`table`) Unused. Accepted for API compatibility.

**Returns:** (`table`) Array with one Tree element.

**Example:**

```lua
local trees = parser:parse()
local root = trees[1]:root()
```

---

### `LanguageTree:lang()` {#LanguageTree-lang}

```lua
LanguageTree:lang()
```

Returns the language name this parser was created with.

**Returns:** (`string`) Language name, e.g. `"lua"`.

---

### `LanguageTree:children()` {#LanguageTree-children}

```lua
LanguageTree:children()
```

Returns child LanguageTrees for injected languages.
Not yet implemented, always returns an empty table.

**Returns:** (`table`) Empty table.

---

### `LanguageTree:trees()` {#LanguageTree-trees}

```lua
LanguageTree:trees()
```

Returns all parsed trees as a table (at most one for now).
Returns an empty table if `parse()` has not been called yet.

**Returns:** (`table`) Array of Tree.

---

### `LanguageTree:source()` {#LanguageTree-source}

```lua
LanguageTree:source()
```

Returns the source string this parser was created with.

**Returns:** (`string`) The original source text.

---

### `LanguageTree:is_valid()` {#LanguageTree-is_valid}

```lua
LanguageTree:is_valid({exclude_children?}, {range?})
```

Checks whether the parse tree is still valid.
Not yet implemented, always returns true.

**Parameters:**

- `{exclude_children?}` (`boolean`) Unused.
- `{range?}` (`table`) Unused.

**Returns:** (`boolean`) Always true.

---

### `LanguageTree:for_each_tree()` {#LanguageTree-for_each_tree}

```lua
LanguageTree:for_each_tree({fn})
```

Calls {fn} with `(tree, nil)` for the parsed tree.
Triggers a parse if the tree has not been parsed yet.

**Parameters:**

- `{fn}` (`function`) Callback receiving `(Tree, nil)`.

**Example:**

```lua
parser:for_each_tree(function(tree, _)
  print(tree:root():type())
end)
```

---

### `LanguageTree:included_regions()` {#LanguageTree-included_regions}

```lua
LanguageTree:included_regions()
```

Returns the regions this parser covers.
Not yet implemented, always returns a table with one empty region.

**Returns:** (`table`) Array with one empty table.

---

### `LanguageTree:contains()` {#LanguageTree-contains}

```lua
LanguageTree:contains({range})
```

Checks whether this parser covers the given {range}.
Not yet implemented, always returns true.

**Parameters:**

- `{range}` (`table`) Range to check (currently unused).

**Returns:** (`boolean`) Always true.

---

### `LanguageTree:destroy()` {#LanguageTree-destroy}

```lua
LanguageTree:destroy()
```

Drops the cached parse tree and frees its memory.
After calling this, the next `parse()` will re-parse from scratch.


## makima.ui {#makima-ui}

Functions for building interactive UI. Create buffers to hold
content, open floating or split windows to display them, highlight
code, render markdown, and show status hints.

```lua
local buf = makima.ui.buf()
buf:line("hello from my plugin!")
local win = makima.ui.open_win(buf, { title = "Greeting", width = "50%", height = 5 })
```

---

### `makima.ui.buf()` {#makima-ui-buf}

```lua
makima.ui.buf({opts?})
```

Creates a new buffer for building UI content. The first buffer you
create in a task becomes the "live" buffer, streamed to the UI while
your tool runs. Pass `{ scratch = true }` to opt out: a toast or an
interactive prompt raised from inside a tool needs its own window
without stealing the live stream.

**Parameters:**

- `{opts?}` (`table?`) Optional. `scratch` (boolean, default false): never claim the live pane.

**Returns:** ([`Buf`](#makima-ui-Buf)) Buffer handle.

**Example:**

```lua
local buf = makima.ui.buf()
buf:line("hello world")
local toast = makima.ui.buf({ scratch = true })
toast:line("copied!")
```

---

### `makima.ui.theme_color()` {#makima-ui-theme_color}

```lua
makima.ui.theme_color({name})
```

Looks up a semantic color from the current theme. Use this to keep
your plugin's colors consistent with the rest of the UI.

**Parameters:**

- `{name}` (`string`) Semantic color name, e.g. "accent" or "background".

**Returns:** (`string|nil`) "#rrggbb" hex color, or nil if the name is unknown.

**Example:**

```lua
local accent = makima.ui.theme_color("accent")
if accent then
  buf:line({ { "note", { fg = accent, bold = true } } })
end
```

---

### `makima.ui.format_time()` {#makima-ui-format_time}

```lua
makima.ui.format_time({epoch_ms}, {style?})
```

Formats a Unix timestamp as local time using the configured clock.

**Parameters:**

- `{epoch_ms}` (`integer`) Unix timestamp in milliseconds.
- `{style?}` (`string?`) Output style: `time`, `weekday`, or `date`. Defaults to `time`.

**Returns:** (`string`) Formatted local time.

---

### `makima.ui.highlight()` {#makima-ui-highlight}

```lua
makima.ui.highlight({code}, {lang}, {opts?})
```

Syntax-highlights a chunk of source code. Returns a table of styled
lines that you can feed into a buffer. Each line is a list of
`{text, style}` spans where style is a `{fg, bold?, italic?, underline?}` table.

**Parameters:**

- `{code}` (`string`) Source text to highlight.
- `{lang}` (`string`) Language identifier, e.g. "rust", "python".
- `{opts?}` (`table?`) Options. Fields:
  - `independent` (`boolean`) highlight each line without cross-line context. Default false.
  - `prefix` (`string`) prepend to the source before highlighting (affects token context). Default "".

**Returns:** (`table`) Lines: `{ { {text, style}, ... }, ... }`. Each style is `{fg, bold?, italic?, underline?}`.

**Example:**

```lua
local lines = makima.ui.highlight("fn main() {}", "rust")
for _, spans in ipairs(lines) do
  buf:line(spans)
end
```

---

### `makima.ui.markdown()` {#makima-ui-markdown}

```lua
makima.ui.markdown({text}, {width})
```

Renders Markdown into styled lines ready to display in a buffer.
Each span's style is either a named string ("bold", "heading",
"inline_code", etc.) or a `{fg, bold?, italic?, underline?}` table
for syntax-highlighted code blocks.

**Parameters:**

- `{text}` (`string`) Markdown source.
- `{width}` (`integer`) Wrap width in columns.

**Returns:** (`table`) Lines: `{ { {text, style}, ... }, ... }`.

**Example:**

```lua
local size = makima.ui.terminal_size()
local lines = makima.ui.markdown("# Hello\n\nSome **bold** text.", size.cols)
for _, spans in ipairs(lines) do
  buf:line(spans)
end
```

---

### `makima.ui.humantime()` {#makima-ui-humantime}

```lua
makima.ui.humantime({secs})
```

Formats a number of seconds into a short, human-friendly string.
Useful for displaying elapsed time in status messages.

**Parameters:**

- `{secs}` (`integer`) Duration in seconds.

**Returns:** (`string`) Human-readable duration, e.g. "1m30s".

**Example:**

```lua
makima.ui.humantime(90)   -- "1m30s"
makima.ui.humantime(3661) -- "1h1m1s"
```

---

### `makima.ui.terminal_size()` {#makima-ui-terminal_size}

```lua
makima.ui.terminal_size()
```

Returns the current terminal size. Handy for sizing floating windows
or wrapping text to fit the screen.

**Returns:** (`table`) `{cols, rows}`, terminal width and height in characters.

**Example:**

```lua
local size = makima.ui.terminal_size()
local half_width = math.floor(size.cols / 2)
```

---

### `makima.ui.display_width()` {#makima-ui-display_width}

```lua
makima.ui.display_width({text})
```

Returns the display width of a string in terminal cells, matching
how `ratatui` measures text.

**Parameters:**

- `{text}` (`string`) The text to measure.

**Returns:** (`integer`) Number of display cells the text occupies.

**Example:**

```lua
local w = makima.ui.display_width("hello")
```

---

### `makima.ui.truncate_text()` {#makima-ui-truncate_text}

```lua
makima.ui.truncate_text({text}, {max_width})
```

Splits a string at a display-cell boundary.

**Parameters:**

- `{text}` (`string`) The text to split.
- `{max_width}` (`integer`) Maximum display cells for the head.

**Returns:** (`table`) `{head = string, tail = string}`.

**Example:**

```lua
local t = makima.ui.truncate_text("hello world", 5)
-- t.head == "hello", t.tail == " world"
```

---

### `makima.ui.focused_agent()` {#makima-ui-focused_agent}

```lua
makima.ui.focused_agent()
```

Resolve the agent that has UI focus now. Later focus changes do not
retarget the returned reference. Pass it to `makima.agent.get(ctx, ref)` for
control; this call grants no authority. Delegated tool handlers cannot read
UI focus.

**Returns:** ([`AgentRef?`](#makima-agent-AgentRef), `string?`) Visibility-only reference, or nil and an error when no agent has focus.

**Example:**

```lua
local focused, err = makima.ui.focused_agent()
if focused then
  local agent = assert(makima.agent.get(ctx, focused))
  local model = assert(agent:config(ctx)).model
end
```

---

### `makima.ui.post()` {#makima-ui-post}

```lua
makima.ui.post({ctx}, {text}, {opts?})
```

Show {text} as a row in the chat of the agent {ctx} is pinned to. A post
is for the user only: it never reaches the model, and later focus changes
do not move it. It is saved with the session and comes back on restore.
Trusted commands, keymaps and callbacks can post; delegated tool handlers
cannot, since their tool row already speaks for them.

`makima.agent.spawn` posts a row for each child it shows; see its `show`
option.

**Parameters:**

- `{ctx}` ([`LuaCtx`](#LuaCtx)) Trusted invocation context.
- `{text}` (`string`) Row text. Must not be blank.
- `{opts?}` (`table?`) Optional fields:
  - `level` (`string?`) "info" (default), "warn" or "error".
  - `agent` (`Agent?`) a child spawned from Lua that the post is about. The row
    is underlined; clicking it opens the child's chat. If no unique chat is
    available, a status message explains why it cannot open. Keeps that child's
    transcript when this post survives a rewind.

  Any other option key is an error.


**Returns:** (`boolean?`, `string?`) True once sent, or nil and an error.

**Example:**

```lua
local ok, err = makima.ui.post(ctx, "review found 3 issues", {
  level = "warn",
  agent = reviewer,
})
```

---

### `makima.ui.flash()` {#makima-ui-flash}

```lua
makima.ui.flash({msg})
```

Shows a brief message in the status bar. The message disappears
after a short time. Good for confirming an action like "copied!"
or showing a transient warning.

**Parameters:**

- `{msg}` (`string`) Message text.

**Example:**

```lua
makima.ui.flash("Copied to clipboard!")
```

---

### `makima.ui.action()` {#makima-ui-action}

```lua
makima.ui.action({name})
```

Runs a built-in UI action by name, exactly as its default keybinding
would. Handy when a default key never reaches makima because tmux or
your terminal grabs it first: bind a new key with `makima.keymap.set`
and call this from it.

Valid names: `"file_picker"`, `"search"`, `"tasks"`, `"help"`,
`"plan_toggle"`, `"plan_editor"`, `"plan_submit"`, `"edit_input"`,
`"pop_queue"`, `"prev_chat"`, `"next_chat"`.

For slash commands rather than keybound actions, see
`makima.api.run_command`.

**Parameters:**

- `{name}` (`string`) Action name, e.g. `"file_picker"`.

**Returns:** (`boolean|nil`, `string|nil`) `true` on success, or nil and an error message for an unknown name.

**Example:**

```lua
-- Open the built-in file picker with Ctrl+Q instead of Ctrl+S:
makima.keymap.set("n", "<C-q>", function()
  makima.ui.action("file_picker")
end)
```

---

### `makima.ui.open_editor()` {#makima-ui-open_editor}

```lua
makima.ui.open_editor({path})
```

Opens {path} in the user's `$EDITOR` (e.g. vim, nano) and waits for
it to close. This suspends the TUI while the editor is running.
Returns the editor's exit code so you can check if the user saved.

**Parameters:**

- `{path}` (`string`) File to open.

**Returns:** (`integer`) Editor exit code, or -1 if the action could not be dispatched.

**Example:**

```lua
local code = makima.ui.open_editor("/tmp/scratch.lua")
if code == 0 then
  makima.ui.flash("File saved")
end
```

---

### `makima.ui.open_list_picker()` {#makima-ui-open_list_picker}

```lua
makima.ui.open_list_picker({items}, {opts?})
```

Opens a fuzzy-filtering list picker dialog and blocks until the user
decides. Items are ranked by match score with matched characters
highlighted, exactly like the built-in pickers. Filter edits keep the
current row instead of jumping to the best match.

An item's optional `age` field is a timestamp from `makima.time.now` or
`makima.time.at`, not an elapsed duration. The host renders it as a live
delta (for example `9s` or `4d 7h 23m 9s`) and refreshes it about once
per second while the dialog is open. `detail` and `age` can coexist;
age follows the detail.

**Parameters:**

- `{items}` (`table`) List of items: plain strings, or `{ label, detail?, section?, section_detail?, age? }` tables.
- `{opts?}` (`table?`) Options:
  - `title` (`string`) dialog title.
  - `footer` (`table`) key-hint pairs for the footer border. Each entry is `{key, label}`.
  - `cursor` (`integer`) initial 1-based item index. Default 1.
  - `submit_keys` (`table`) extra key names that confirm the selection, in the "ctrl+o" notation; enter always confirms.
  - `notify_initial` (`boolean`) fire `on_change` for the initial item. Default false.
  - `timeout_ms` (`integer`) idle window; when it elapses `on_timeout` fires, and keeps firing while the dialog stays idle. Zero or omitted disables the window.
  - `on_change` (`function`) `on_change(item, index)` with the original item and its 1-based index, fired when the selection moves.
  - `on_timeout` (`function`) `on_timeout()` when the idle window elapses.

**Returns:** (`table`) `{ type = "choice", index = n }` with the 1-based index of the chosen item, `{ type = "delete", index = n }` when the user presses Ctrl+D twice on an item (the first press flashes "Press Ctrl+D again to delete"), or `{ type = "close" }` when dismissed with Esc or Ctrl+C, or when the UI channel drops mid-pick.

**Example:**

```lua
local result = makima.ui.open_list_picker({ "alpha", "beta" }, { title = "Pick" })
if result.type == "choice" then
  makima.ui.flash("Picked " .. result.index)
end
```

---

### `makima.ui.open_win()` {#makima-ui-open_win}

```lua
makima.ui.open_win({buf}, {opts})
```

Opens a floating or split window that displays the contents of {buf}.
Returns a Win handle you can use to receive events, update layout,
and close the window when you are done.

**Parameters:**

- `{buf}` ([`Buf`](#makima-ui-Buf)) Buffer to display.
- `{opts}` (`table`) Float configuration. Fields:
  - `width` (`integer|string`) window width. Integer for absolute columns; "N%" for percent of terminal width. Default "60%".
  - `height` (`integer|string`) window height. Integer for absolute rows; "N%" for percent of terminal height. Default "70%".
  - `row` (`integer?`) row offset from the anchor corner. Negative values move up.
  - `col` (`integer?`) column offset from the anchor corner.
  - `anchor` (`string`) corner the (row, col) offset is relative to. One of "NW" (default), "NE", "SW", "SE".
  - `border` (`string`) border style. One of "rounded" (default), "single", "double", "none".
  - `title` (`string`) text shown in the top border. Default "".
  - `title_pos` (`string`) title alignment. One of "left" (default), "center", "right".
  - `footer` (`table`) key-hint pairs shown in the bottom border. Each entry is {key, label}.
  - `zindex` (`integer`) stacking order. Default 50.
  - `cursor_line` (`boolean`) highlight the focused row. Default false.
  - `reserved_top` (`integer`) rows reserved at the top of the content area. Default 0.
  - `reserved_bottom` (`integer`) rows reserved at the bottom of the content area. Default 0.
  - `split` (`string`) dock the window to an edge instead of floating. One of "above", "below", "left", "right", "panel", or "" (floating, default).
  - `order` (`integer`) paint order among split windows at the same edge. Default 50.
  - `focus` (`boolean`) whether the window takes keyboard focus on open. Default true.
  - `visible` (`boolean`) whether the window is initially visible. Default true.
  - `needs_input` (`boolean`) whether the window means the session needs user input. Default false.

**Returns:** ([`Win`](#makima-ui-Win)) Window handle.

**Example:**

```lua
local buf = makima.ui.buf()
buf:line("Pick an option:")
local win = makima.ui.open_win(buf, {
  title = "Menu",
  width = "50%",
  height = 10,
  cursor_line = true,
  footer = { { "q", "quit" }, { "Enter", "select" } },
})
```

---

### `makima.ui.set_status_content()` {#makima-ui-set_status_content}

```lua
makima.ui.set_status_content({spans})
```

Sets styled status content for your plugin. Each span is {text, style_name} or {text, style_name, on_click}. An empty table clears your plugin's content. Only your own content is affected. Clickable entries run their callback when clicked in the status bar.

**Parameters:**

- `{spans}` (`table`) Sequence of {text, style_name} or {text, style_name, on_click} entries.

**Example:**

```lua
makima.ui.set_status_content({ {"connected", "success"}, {" readonly", "muted"} })
-- clickable entry:
makima.ui.set_status_content({ {"refresh", "accent", function() makima.ui.flash("refreshing") end} })
```

---

### `makima.ui.set_status_hint()` {#makima-ui-set_status_hint}

```lua
makima.ui.set_status_hint({spans})
```

Shows key hints in the status bar for your plugin. Each hint is a {key, label} pair. Pass nil to clear your plugin's hints. Only your own hints are affected, other plugins keep theirs.

**Parameters:**

- `{spans}` (`table|nil`) Sequence of {key, label} pairs, e.g. `{{"q", "quit"}, {"j", "down"}}`. Pass nil to remove the plugin's hints.

**Example:**

```lua
makima.ui.set_status_hint({ {"q", "quit"}, {"j", "down"} })
-- later, clear them:
makima.ui.set_status_hint(nil)
```

---

### `makima.ui.set_window_title()` {#makima-ui-set_window_title}

```lua
makima.ui.set_window_title({title})
```

Sets the terminal emulator's window title. Pass an empty string to
clear it.

The title passes through tmux, GNU screen, and zellij untouched, and
control characters are stripped, so model text cannot inject escape
sequences into the terminal. On exit makima hands the title back to the
shell, on terminals that support the title stack.

**Parameters:**

- `{title}` (`string`) New window title, e.g. `"● 3/5 tests"`.

**Example:**

```lua
makima.ui.set_window_title("makima: " .. session_name)
-- Give the title back to the shell:
makima.ui.set_window_title("")
```


## makima.ui.Win {#makima-ui-Win}

Handle to a floating or split window. You get one from
`makima.ui.open_win()`. Use `recv()` in a loop to handle keyboard
input, and call `close()` when done.

Fields: `width`, `height` (initial content dimensions in columns/rows),
`visible` (current visibility).

```lua
local win = makima.ui.open_win(buf, { title = "Demo" })
while true do
  local ev = win:recv()
  if not ev or ev.key == "q" then break end
end
win:close()
```

---

### `Win:recv()` {#Win-recv}

```lua
Win:recv({timeout_ms?})
```

Waits for the next event from this window. Call this in a loop to build an interactive UI. Returns nil once the window is closed or the channel disconnects. Pass {timeout_ms} to also get `{type="timeout"}` events so your plugin can animate while idle.

Event tables by type:
- `{type="key", key}` -- keypress. Key is a string like "q", "j", or "esc".
- `{type="resize", width, height}` -- terminal was resized.
- `{type="paste", text}` -- bracketed paste.
- `{type="click", row, col}` -- mouse click in the content; row is 1-based, 0 means the border/title/footer.
- `{type="close"}` -- window was closed externally.
- `{type="timeout"}` -- no event arrived within {timeout_ms}.

**Parameters:**

- `{timeout_ms?}` (`integer`) Max milliseconds to wait before a timeout event is returned.

**Returns:** (`table|nil`) Event table, or nil if the window has closed.

**Example:**

```lua
while true do
  local ev = win:recv()
  if not ev or ev.key == "q" then break end
  if ev.type == "key" and ev.key == "j" then
    -- move cursor down
  end
end
win:close()
```

---

### `Win:set_config()` {#Win-set_config}

```lua
Win:set_config({opts})
```

Updates the window layout on the fly. Only the fields you include in
{opts} are changed, everything else stays the same.

**Parameters:**

- `{opts}` (`table`) Partial float config. Accepted fields:
  - `title` (`string`) border title text.
  - `title_pos` (`string`) title alignment, "left", "center", or "right".
  - `footer` (`table`) key-hint pairs `{{key, label}, ...}` shown in the bottom border.
  - `border` (`string`) "rounded", "single", "double", or "none".
  - `anchor` (`string`) corner origin, "NW", "NE", "SW", or "SE".
  - `width` (`integer|string`) new width; integer or "N%".
  - `height` (`integer|string`) new height; integer or "N%".
  - `zindex` (`integer`) stacking order.
  - `cursor_line` (`boolean`) highlight the focused row.
  - `reserved_top` (`integer`) rows reserved at the top of the content area.
  - `split` (`string`) edge docking, "above", "below", "left", "right", "panel", or "".
  - `order` (`integer`) paint order among split windows.
  - `needs_input` (`boolean`) whether the window means the session needs user input.

**Example:**

```lua
win:set_config({ title = "Updated!", width = "80%" })
```

---

### `Win:set_cursor()` {#Win-set_cursor}

```lua
Win:set_cursor({row})
```

Moves the highlighted cursor line to {row} (1-indexed). Only has a
visible effect when the window was opened with `cursor_line = true`.

**Parameters:**

- `{row}` (`integer`) Target row, 1-indexed.

**Example:**

```lua
win:set_cursor(3) -- highlight the third line
```

---

### `Win:close()` {#Win-close}

```lua
Win:close()
```

Closes the window and frees its resources. Safe to call more than
once. The window also closes automatically when the handle is
garbage collected.

**Example:**

```lua
win:close()
```

---

### `Win:is_open()` {#Win-is_open}

```lua
Win:is_open()
```

Returns true if the window is still alive (not closed). Useful for
checking before sending commands.

**Returns:** (`boolean`) true if open.

**Example:**

```lua
if win:is_open() then
  win:set_config({ title = "still here" })
end
```

---

### `Win:show()` {#Win-show}

```lua
Win:show()
```

Makes the window visible again after it was hidden with `hide()`.

**Example:**

```lua
win:show()
```

---

### `Win:hide()` {#Win-hide}

```lua
Win:hide()
```

Hides the window without closing it. The window keeps its state
and buffer contents. Call `show()` to bring it back.

**Example:**

```lua
win:hide()
-- do some work...
win:show()
```

---

### `Win:is_visible()` {#Win-is_visible}

```lua
Win:is_visible()
```

Returns true if the window is both open and visible (not hidden).

**Returns:** (`boolean`) true if visible.


## makima.ui.Buf {#makima-ui-Buf}

A content buffer that holds styled lines of text. Create one with
`makima.ui.buf()` and pass it to `makima.ui.open_win()` to show it in
a floating or split window.

```lua
local buf = makima.ui.buf()
buf:line("hello")
buf:line({ { "world", "bold" } })
```

---

### `Buf:line()` {#Buf-line}

```lua
Buf:line({line})
```

Appends a single line to the end of the buffer. You can pass a
plain string for unstyled text, or a table of `{text, style?}` spans
for rich content. Style can be a named string like "bold" or
"keyword", or an inline table `{fg?, bg?, bold?, italic?, underline?, dim?, strikethrough?, reversed?}`
with "#rrggbb" color strings.

**Parameters:**

- `{line}` (`string|table`) Plain string, or a sequence of spans: `{ {text, style?}, ... }`.

**Example:**

```lua
buf:line("plain text")
buf:line({ { "ERROR", { fg = "#ff0000", bold = true } }, { " something broke" } })
```

---

### `Buf:lines()` {#Buf-lines}

```lua
Buf:lines({lines})
```

Appends several lines at once. Each entry uses the same format as
`buf:line()`, so you can mix plain strings and styled spans.

**Parameters:**

- `{lines}` (`table`) Sequence of line values, each the same format accepted by `buf:line`.

**Example:**

```lua
buf:lines({
  "first line",
  { { "styled ", "bold" }, { "second line" } },
  "third line",
})
```

---

### `Buf:set_lines()` {#Buf-set_lines}

```lua
Buf:set_lines({lines})
```

Replaces every line in the buffer with {lines}. Use this when you
want to redraw the whole buffer, for example after the user toggles
a view.

**Parameters:**

- `{lines}` (`table`) Sequence of line values, each the same format accepted by `buf:line`.

**Example:**

```lua
buf:set_lines({ "new content", "replaces everything" })
```

---

### `Buf:len()` {#Buf-len}

```lua
Buf:len()
```

Returns how many lines the buffer currently holds.

**Returns:** (`integer`) Line count.

**Example:**

```lua
if buf:len() == 0 then
  buf:line("(empty)")
end
```

---

### `Buf:get_lines()` {#Buf-get_lines}

```lua
Buf:get_lines()
```

Returns all lines in the buffer as a Lua table. Each line is a
sequence of `{text, style?}` spans, the same format `buf:line()`
accepts. Useful for reading back content, copying it to another
buffer, or round-tripping through `set_lines()`.

**Returns:** (`table`) Sequence of lines.

**Example:**

```lua
local lines = buf:get_lines()
buf:set_lines(lines) -- round-trip
```

---

### `Buf:on()` {#Buf-on}

```lua
Buf:on({event}, {callback})
```

Registers an event handler on the buffer.

Supported events:
- "click": fires when the user clicks a line. The handler receives
  a click-event table whose `row` is 1-based, with `row = 0` for clicks
  on chrome or outside the content. Transcript buffers deliver clicks
  here; float windows deliver them from `win:recv()` as a `click` event
  that also carries `col`.
- "change": fires synchronously after every mutation (`line`,
  `lines`, `set_lines`). Must not yield.

Calling `on()` again for the same event replaces the previous handler.

**Parameters:**

- `{event}` (`string`) Event name: "click" or "change".
- `{callback}` (`function`) Handler function. For "click", receives a click-event table. For "change", receives no arguments.

**Example:**

```lua
buf:on("click", function(ev)
  makima.ui.flash("Clicked row " .. ev.row)
end)
```

---

### `Buf:click()` {#Buf-click}

```lua
Buf:click({ev})
```

Programmatically fires the buffer's click handler with event {ev}.
Does nothing if no click handler is registered. Useful for testing
or simulating user interaction from code.

**Parameters:**

- `{ev}` (`table`) Click event table passed to the handler.

**Example:**

```lua
buf:click({ row = 1 })
```

---

### `Buf:blit()` {#Buf-blit}

```lua
Buf:blit({fb}, {width}, {height}, {opts?})
```

Replaces the whole buffer with a pixel frame drawn as `"▀"` cells.
Each cell's foreground is the top pixel and its background the
bottom one, so one text line fits two pixel rows. When {height} is
odd the last line leaves its background unset and the terminal
default shows through.

{fb} is a Luau `buffer` of raw pixel bytes in row-major order,
top-left origin. Its size must be exactly
`width * height * bytes_per_pixel` for the chosen format, otherwise
the call throws. A mismatch usually means a wrong width or format,
and an early error beats hunting down a garbled frame.

Formats: "rgb" is the default at 3 bytes per pixel. "rgba" and
"bgra" take 4 bytes per pixel and ignore the 4th byte. "bgra" is
what a little-endian `uint32` holding `0xRRGGBB` looks like in
memory, the layout doomgeneric uses for its framebuffer.

`char` swaps the `"▀"` glyph for another one column wide string,
e.g. `"█"` when only the foreground color should show. The
foreground still comes from the top pixel and the background from
the bottom one, whatever the glyph.

**Parameters:**

- `{fb}` (`buffer`) Raw pixel bytes.
- `{width}` (`integer`) Frame width in pixels, > 0.
- `{height}` (`integer`) Frame height in pixels, > 0.
- `{opts?}` (`table|nil`) Options: `format` = "rgb"|"rgba"|"bgra", `char` = one column wide string.

**Example:**

```lua
local fb = buffer.create(160 * 100 * 3)
buffer.writeu8(fb, (y * 160 + x) * 3, 255) -- red channel
buf:blit(fb, 160, 100)
buf:blit(fb32, 160, 100, { format = "bgra", char = "█" })
```


## makima.uv {#makima-uv}

System and environment utilities, modelled after `vim.uv`.

Provides access to the working directory, home directory, and environment
variables. None of these functions throw.

```lua
local home = makima.uv.os_homedir()
```

---

### `makima.uv.cwd()` {#makima-uv-cwd}

```lua
makima.uv.cwd()
```

Return the current working directory as an absolute path. Like `vim.uv.cwd`.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** (`string?`) Current working directory, or nil if it cannot be determined.

**Example:**

```lua
local cwd = makima.uv.cwd()
if cwd then print("working in: " .. cwd) end
```

---

### `makima.uv.os_homedir()` {#makima-uv-os_homedir}

```lua
makima.uv.os_homedir()
```

Return the current user's home directory. Like `vim.uv.os_homedir`.

Requires the `env` [plugin permission](#plugin-permissions).

**Returns:** (`string?`) Home directory path, or nil if it cannot be determined.

**Example:**

```lua
local home = makima.uv.os_homedir() -- e.g. "/home/user"
```

---

### `makima.uv.os_getenv()` {#makima-uv-os_getenv}

```lua
makima.uv.os_getenv({name})
```

Look up the environment variable {name}. Like `vim.uv.os_getenv`.
Returns nil when the variable is not set.

Requires the `env` [plugin permission](#plugin-permissions).

**Parameters:**

- `{name}` (`string`) Name of the environment variable.

**Returns:** (`string?`) Variable value, or nil if not set.

**Example:**

```lua
local editor = makima.uv.os_getenv("EDITOR") or "vi"
```


## makima.yaml {#makima-yaml}

YAML encoding and decoding. Works the same way as `makima.json`,
but for YAML formatted strings.

```lua
local t = makima.yaml.decode("greeting: hello")
print(t.greeting)
```

---

### `makima.yaml.encode()` {#makima-yaml-encode}

```lua
makima.yaml.encode({value})
```

Turn a Lua value into a YAML string. Most Lua types work, but
circular references will return an error.

**Parameters:**

- `{value}` (`any`) Lua value to encode.

**Returns:** (`string?`, `string?`) YAML string, or nil plus an error.

**Example:**

```lua
local s, err = makima.yaml.encode({ name = "makima", tags = { "ai", "agent" } })
print(s)
```

---

### `makima.yaml.decode()` {#makima-yaml-decode}

```lua
makima.yaml.decode({str})
```

Parse a YAML string into a Lua value. Mappings become tables and
sequences become 1-indexed arrays.

**Parameters:**

- `{str}` (`string`) YAML string to decode.

**Returns:** (`any?`, `string?`) Decoded value, or nil plus an error.

**Example:**

```lua
local t, err = makima.yaml.decode("name: makima\nversion: 1")
print(t.name) -- makima
```


## Shared helper modules

These ship inside makima; `require` them from any plugin. Small modules are
shown as full source, larger ones as their public interface.

### `require("makima.color")`

```lua
local M = {}

function M.lerp(from, to, t)
  local fr, fg, fb = from:match("#(%x%x)(%x%x)(%x%x)")
  local tr, tg, tb = to:match("#(%x%x)(%x%x)(%x%x)")
  if not fr or not tr then
    return from
  end
  fr, fg, fb = tonumber(fr, 16), tonumber(fg, 16), tonumber(fb, 16)
  tr, tg, tb = tonumber(tr, 16), tonumber(tg, 16), tonumber(tb, 16)
  local r = math.floor(fr + (tr - fr) * t + 0.5)
  local g = math.floor(fg + (tg - fg) * t + 0.5)
  local b = math.floor(fb + (tb - fb) * t + 0.5)
  return string.format("#%02x%02x%02x", r, g, b)
end

function M.dim(color, factor)
  local background = makima.ui.theme_color("background")
  if not background then
    return color
  end
  return M.lerp(color, background, factor)
end

return M
```

### `require("makima.dir_listing")`

```lua
-- Shared directory listing for index and list plugins.
-- Lists entries, filters instruction files, sorts dirs before files, and
-- renders the listing so every caller shows a directory the same way.
function M.list(path, ctx)
function M.view(text, ctx)
```

### `require("makima.fuzzy_replace")`

```lua
M.NO_MATCH = "old_string not found in file"
M.MULTIPLE_MATCHES = "old_string matches multiple locations; add surrounding context to make it unique"
M.EMPTY_OLD_STRING = "old_string must not be empty"

-- Replace {old_string} with {new_string} in {content}, tolerating small
-- whitespace and indentation drift. Returns the new content, or nil plus
-- one of the error constants above. With {max_line_bytes}, fuzzy matches
-- must cover long lines in full.
function M.replace(content, old_string, new_string, replace_all, max_line_bytes)
```

### `require("makima.list_picker")`

```lua
-- Draws the filter query and its blank spacer into {lines}, pins that height on
-- {win} and returns it, which is also the first scrollable line. Drawing and
-- pinning belong together: a query that wraps, or one pasted with a newline,
-- makes the header taller than a picker would guess, and a reserved_top guessed
-- elsewhere then mis-scrolls the list.
function ListPicker.render_header(win, lines, input, prefix, inner)

-- Open a fuzzy-filter picker in a floating window and block until the user
-- decides. {items} is a list of strings or { label, detail?, age? } tables,
-- where `age` is a timestamp from `makima.time.now` or `makima.time.at` and
-- refreshes once per displayed second while the dialog is open.
-- {opts}:
-- title, footer, cursor (initial index), submit_keys (extra submit keys
-- besides enter), action_keys (keys that close the picker and report
-- themselves, like { "R" } for a refresh binding. Use uppercase keys, since
-- lowercase ones keep feeding the filter). Returns
-- { type = "choice"|"delete", index }, { type = "key", key, index? } or
-- { type = "close" }.
function ListPicker.open(items, opts)
ListPicker.split_words = split_words
ListPicker.matches = matches
ListPicker.highlight_spans = highlight_spans
```

### `require("makima.long_lines")`

```lua
-- Write-time guard against lossy write-back of truncated tool output.
--
-- read and grep cut lines longer than agent.max_line_bytes, so the model may
-- only ever have seen a prefix of them. These checks compare content, not
-- read history: they hold after resume, in subagents, and whatever the model
-- last read. Lines are compared exactly, and every protected line consumes
-- its own occurrence, so two identical long lines cannot collapse into one.
M.LONG_LINE_CHANGED = "line %d is %d bytes, longer than agent.max_line_bytes (%d), so it may have been shown "

--- Every long line of `before` must survive unchanged somewhere in `after`.
function M.check_write(before, after, max_line_bytes)

--- Every long line in content's [start_line, end_line] must survive in
--- `new_string`. An empty `new_string` is an explicit deletion and passes.
function M.check_replace_lines(content, start_line, end_line, new_string, max_line_bytes)

--- Reject lines of `after` that end in a truncation marker unless an
--- identical line is still available in `before`. Removing a marker passes.
function M.check_markers(before, after)
```

### `require("makima.output_limits")`

```lua
-- Shared per-tool output limit options, so the tools that support them
-- cannot drift apart.

local DEFAULT_MAX_OUTPUT_LINES = 2000
local DEFAULT_MAX_OUTPUT_BYTES = 50 * 1024
local DEFAULT_MAX_LINE_BYTES = 1000

local M = {}

M.DEFAULT_MAX_LINE_BYTES = DEFAULT_MAX_LINE_BYTES
M.specs = {
  max_output_lines = { type = "integer", desc = "Override `agent.max_output_lines` for this tool." },
  max_output_bytes = { type = "integer", desc = "Override `agent.max_output_bytes` for this tool." },
}

function M.line_bytes(ctx)
  return ctx:config("max_line_bytes", DEFAULT_MAX_LINE_BYTES)
end

function M.extend(spec)
  for name, s in pairs(M.specs) do
    spec[name] = s
  end
  return spec
end

--- Returns max_lines, max_bytes: tool override when set, agent-wide otherwise.
function M.resolve(opts, ctx)
  return opts.max_output_lines or ctx:config("max_output_lines", DEFAULT_MAX_OUTPUT_LINES),
    opts.max_output_bytes or ctx:config("max_output_bytes", DEFAULT_MAX_OUTPUT_BYTES)
end

return M
```

### `require("makima.partial")`

```lua
-- When a tool is cut short, it still hands back what it printed. The marker
-- tells the model that output is real but unfinished. One home for the
-- wording and the painting, so every tool says it the same way.

--- Close {view} once and return raw output with a deferred marker trailer.
--- {reason} is a cancel-hook reason ("cancelled" | "timeout") and {started_at}
--- the `os.time()` the tool began at.
function M.cut(view, out, reason, timeout_secs, started_at, limits)
```

### `require("makima.plan_spec")`

```lua
-- Shared plan specification, spliced verbatim into the plan-mode directive and
-- the review prompt so both always see the exact same document.
--
-- Ported from polytoken's default plan specification (plan_spec_default.md);
-- keep in sync with upstream.
```

### `require("makima.scroll")`

```lua
-- Relative scrolling on top of makima.fn.winsaveview / winrestview.
-- Positive {delta} scrolls down, negative up. Returns (true, nil) or (nil, err).
local function scroll(delta)
  local view, err = makima.fn.winsaveview()
  if not view then
    return nil, err
  end
  return makima.fn.winrestview({ topline = view.topline + delta })
end

return scroll
```

### `require("makima.shorten_path")`

```lua
local function normalize_sep(s)
  return s:gsub("\\", "/")
end

local function shorten_path(path)
  local p = normalize_sep(path)
  local cwd = makima.uv.cwd()
  if cwd then
    cwd = normalize_sep(cwd)
    if p:sub(1, #cwd + 1) == cwd .. "/" then
      local rel = p:sub(#cwd + 2)
      return rel == "" and "." or rel
    end
  end
  local home = makima.uv.os_homedir()
  if home then
    home = normalize_sep(home)
    if p:sub(1, #home + 1) == home .. "/" then
      local rel = p:sub(#home + 2)
      return rel == "" and "~" or "~/" .. rel
    end
  end
  return path
end

return shorten_path
```

### `require("makima.spans")`

```lua
-- Spans: styled slices of a label for fuzzy-match highlighting.
--
-- `match_spans` takes 1-based CODEPOINT indices (as returned by
-- `makima.match.fuzzy`) and slices the label by bytes, so multi-byte titles
-- (CJK, emoji) get whole-codepoint spans. Adjacent indices merge into one
-- span.

-- indices: 1-based codepoint offsets, ascending. No indices yields a single
-- {text, base_style} span.
function Spans.match_spans(text, indices, base_style, match_style)
```

### `require("makima.test_helpers")`

```lua
-- Shared test helpers for Lua plugin specs.
--
-- Provides a lightweight test harness: `case` wraps each block in pcall so a
-- single failure does not abort the rest of the suite. Failures are collected
-- and surfaced by `report()` at the end.
function M.case(name, fn)
function M.eq(actual, expected, msg)
function M.has(s, substr, msg)
function M.mktmpdir(prefix)
function M.rmtree(dir)

-- A scripted `makima.net.send` response that yields {chunks} in order. A
-- `read_error` is returned once the chunks run out instead of the end.
function M.response(chunks, opts)

-- A provider `ctx` whose `send` records each request and returns the next of
-- {responses}, and whose credentials live in memory. `refresh` and `lock`
-- run their function at once, without a lock.
function M.provider_ctx(responses, saved)

-- An `emit` for provider streams, plus the list it records into.
function M.recorder()

-- Runs {fn} with `makima.uv.os_getenv` reading from {env} instead.
function M.with_env(env, fn)

-- Compares JSON-like values structurally.
function M.same(actual, expected, path)
function M.report()
```

### `require("makima.text_input")`

```lua
-- TextInput: multi-line editable buffer with a byte-offset cursor.
--
-- Invariants enforced everywhere:
--   * `line` is 1-based and indexes a line that always exists.
--   * `col` is a byte offset inside `lines[line]`, always on a UTF-8 codepoint
--     boundary, so `lines[line]:sub(1, col)` is a complete UTF-8 prefix.
--   * No line ever contains a literal newline; newlines split into rows.
--
-- Parents OWN their keys. `handle_key` returns one of R.IGNORED / R.MOVED /
-- R.CHANGED. Parent dispatchers must filter their own keys (esc, ctrl+c,
-- submit keys, etc.) BEFORE forwarding, because `handle_key` claims any key
-- it can interpret. `ctrl+a` is bound to move-home; if a parent wants it for
-- "select all" it must intercept first.
--
-- IGNORED is returned when the buffer literally cannot act (backspace at
-- (1, 0), right at end of buffer, etc.). Parents can use that signal to fall
-- through to their own logic.
--
-- Parity cases live in plugins/lib/tests/spec.lua (TRACE_CASES). Add one
-- whenever you change handle_key semantics.
TextInput.Result = R
function TextInput.new()
function TextInput:value()
function TextInput:is_empty()
function TextInput:line_count()
function TextInput:clear()

-- Returns the codepoint right before the cursor as a string, or nil at the
-- start of a line. Lets callers peek backwards (e.g. "is the previous char
-- a backslash?") without touching internal indices.
function TextInput:char_before_cursor()
function TextInput:insert_text(text)
function TextInput:insert_char(c)
function TextInput:insert_space()
function TextInput:split_line()
function TextInput:remove_char()
function TextInput:delete_char()
function TextInput:remove_word_before()
function TextInput:delete_word_after()
function TextInput:kill_to_end_of_line()
function TextInput:move_left()
function TextInput:move_right()
function TextInput:move_up()
function TextInput:move_down()
function TextInput:move_home()
function TextInput:move_end()
function TextInput:move_word_left()
function TextInput:move_word_right()
function TextInput:handle_key(key)

-- Wrap lines to {width} with {prefix} before the first row. Returns
-- { lines = styled lines, cursor_row = 1-based row holding the cursor }.
function TextInput:render(prefix, prefix_width, width)

-- Click coordinates are 1-based display cells in the rendered input.
function TextInput:click(rendered, row, col)
```

### `require("makima.toast")`

```lua
-- Corner toast notifications built on floating windows. `makima.ui.flash` gives
-- you one line in the status area. A toast stays up long enough to read, can
-- carry a title, and stacks under the toasts already on screen.

-- Show {text} as a toast, up to 5 lines of it. {opts}: title (string),
-- timeout_secs (integer, default 4). Returns right away and the toast
-- dismisses itself when the time is up.
function Toast.show(text, opts)
```

### `require("makima.tool_view")`

```lua
-- The shared truncate/expand body that tool plugins render through.
--
-- Click handlers get `ev.row`, a 1-based line in this buf; 0 means the
-- click landed outside it (the header). The handler lives on the buf
-- itself, so any wrapper of the same buf (a batch child's foreign handle)
-- reaches the same toggle. Expansion is never stored: the UI records
-- clicked rows and replays them through `restore` in order, so `toggle`
-- stays a pure flag flip + re-render, deterministic across replays.
-- Async highlighting goes through `makima.async.run`; during restore the
-- runtime runs those tasks inline before snapshotting.

-- Right aligned, so the content column stays put when a number gains a digit.
-- Formats the number alone, callers add their own separator.
function ToolView.line_nr_fmt(max_line_nr)

-- opts: max_lines (default 80) shown while collapsed, keep "head"|"tail"
-- (default "tail"), max_expand_lines (default 2000) kept for expansion,
-- max_line_bytes (optional) per-line byte cap applied at render time.
function ToolView.new(buf, opts)
function ToolView:set_header(lines)
function ToolView:clear()
function ToolView:append(line)
function ToolView:append_text(text)

-- Append {content} with line numbers, then syntax-highlight it for {ext}
-- asynchronously. Returns false when {content} is empty.
function ToolView:set_highlight(content, ext)

-- Content rows on screen, for callers with their own per-row click targets. A
-- single hidden line is drawn as itself instead of a notice, so it counts as
-- content too. Rows line up with `all_lines` under keep = "head"; keep = "tail"
-- prints its notice first and shifts them.
function ToolView:visible_count()
function ToolView:toggle()
function ToolView:flush()
function ToolView:update_line(all_idx, line)

-- Call once after the last append so the collapsed notice renders.
function ToolView:finish()
function ToolView.restore_lines(lines, opts)

-- Rebuild a collapsed view from a tool's saved llm_output, click-to-toggle
-- wired. For `restore` hooks.
function ToolView.restore(output, opts)

-- Same, for tools whose live output goes through markdown (`format =
-- "markdown"`); {opts.width} is the wrap width. Errors stay plain, as they do
-- live.
function ToolView.restore_markdown(output, is_error, opts)
```

