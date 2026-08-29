+++
title = "Permissions"
weight = 6
[extra]
group = "Reference"
+++

# Permissions

Makima uses a permission system to decide what each tool is allowed to do and when to ask you first.

Rules come from four layers, combined for resolution:

1. **Session rules**, set during the current session (in-memory only)
2. **Config rules**, loaded from TOML permission files
3. **Builtin rules**, the hardcoded defaults
4. **Plugin rules**, declared by plugins via [`maki.api.register_permission_rule`](/lua-api/#maki-api-register_permission_rule)

Any matching deny blocks the tool. No exceptions, so a config deny always beats a plugin allow.

## Check Flow

For every tool call, each scope resolves like this:

```
tool call
    │
deny rule matches?  ── yes ──►  blocked. no exceptions
    │ no
allow rule matches? ── yes ──►  runs
    │ no
YOLO active?        ── yes ──►  runs
    │ no
plan file write?    ── yes ──►  runs
    │ no
    ▼
default: prompt / allow / deny
```

Deny rules are checked across all layers before anything else, so a deny cannot be bypassed by YOLO or the plan-file auto-allow. In plan mode, writes to any path other than the plan file are rejected before this flow, and MCP tools are blocked entirely. `default` resolves per-tool first, then global; the built-in default is `"prompt"`.

## Builtin Defaults

File-write tools are pre-allowed inside the project working directory (cwd at session start, canonicalized). Paths outside that tree still need a prompt or an explicit allow rule:

| Tool | Scope | Notes |
|------|-------|-------|
| `write` | `<cwd>/**` | Outside cwd requires permission |
| `edit` | `<cwd>/**` | Outside cwd requires permission |
| `multiedit` | `<cwd>/**` | Outside cwd requires permission |
| `edit_lines` | `<cwd>/**` | Same, when the opt-in tool is enabled |
| `insert_lines` | `<cwd>/**` | Same, when the opt-in tool is enabled |
| `task` | `*` | Subagent spawning always allowed |

The memory plugin uses a plugin rule to pre-allow the file-write tools inside its notes directory (under makima's state dir), so the agent can edit memory notes directly without a prompt.

These tools have no builtin allow rule, so they prompt (or follow your `default`) every time unless you add rules:

- `bash` - Shell commands (scopes come from tree-sitter parsing)
- `websearch` - Web search queries
- `webfetch` - URL fetching

Tools that never declare permission scopes (for example `read`, `glob`, `grep`, `index`, `memory`, `skill`, `todo_write`) **skip** the permission manager entirely. They always run. If you need to block one of them, turn the plugin off in `init.lua` (`plugins.read = { enabled = false }`) rather than using `permissions.toml`.

Container tools like `batch` and `code_execution` prompt for each inner tool individually.

## TOML Configuration

There are two permission files:

- **Global**: `~/.config/makima/permissions.toml`
- **Project**: `.makima/permissions.toml` (takes precedence over global)

```toml
default = "deny"

[bash]
allow = [
    "cargo *",
    "git *",
]
deny = [
    "rm -rf *",
    "sudo *",
]

[read]
default = "allow"

[mcp.deepwiki]
allow = ["search", "fetch"]

[mcp.github]
deny = ["admin_delete"]
```

Each tool gets its own section with `allow` and `deny` arrays. Values are glob-like scope patterns.

> **Note:** In MCP server sections (`[mcp.*]`), the boolean forms `allow = true` and `deny = true` are deprecated and ignored. Use `default = "allow"` or `default = "deny"` instead. For native tool sections (e.g. `[bash]`), `allow = true` still works.

### The `default` key

Controls what happens when no allow or deny rule matches. Can be `"prompt"` (built-in default), `"deny"`, or `"allow"`. Set it globally or per-tool:

```toml
default = "deny"

[bash]
default = "prompt"
allow = ["cargo *"]
```

Here everything is denied by default, except `bash` which still prompts, and `cargo *` commands which are allowed.

Project files **cannot** set `default = "allow"` (top-level, per-tool, or MCP). That value is ignored so a project cannot grant itself full access. Project **allow lists** still work. Put `default = "allow"` only in the global file.

## Scope Patterns

| Pattern | Matches |
|---------|--------|
| `*` or `**` | Any value (full wildcard) |
| `prefix*` | Values starting with prefix |
| `cmd *` | Bare `cmd` or `cmd` plus args (`pwd *` matches `pwd` and `pwd -L`, not `pwdx`) |
| `dir/**` | `dir` itself or anything under it (path-aware on Windows and Unix) |
| `exact` | Exact match only |

## MCP Tool Permissions

MCP tools use natural TOML nesting. Server names are table keys under `[mcp]`, tool names are array values:

```toml
# Global permissions.toml (default = "allow" is ignored in project files)
[mcp.deepwiki]
allow = ["search", "fetch"]

[mcp.github]
deny = ["admin_delete"]

[mcp.lean-lsp]
default = "allow"               # allow all tools on this server (global only)
```

Tool names must match `^[a-zA-Z0-9_-]{1,64}$` (no dots, max 64 chars). Server names cannot contain dots.

## Permission Prompts

When a gated tool needs permission, Makima asks you.

| Key | Action |
|-----|--------|
| `y` | Allow once (immediate) |
| `s` | Allow for this session (confirm with `Enter` or `y`; any other key cancels) |
| `a` | Always allow for this project (confirm; saved to `.makima/permissions.toml`) |
| `A` | Always allow globally (confirm; saved to `~/.config/makima/permissions.toml`) |
| `n` | Open deny guidance editor (type optional guidance, then `Enter` to deny once; `Esc` cancels) |
| `d` | Deny always for this project (confirm) |
| `D` | Deny always globally (confirm) |

Session and always-allow / always-deny choices need a second key (`Enter` or `y`) so a fat-finger does not rewrite your rules. Deny-once with `n` lets you type a short reason the agent will see.

### Scope Generalization

When you pick "always allow" (or always deny for MCP), the saved scope is generalized so it stays useful beyond that one call:

- **bash**: `cargo test --all` becomes `cargo *`
- **write / edit / multiedit / edit_lines / insert_lines**: `/path/to/file.rs` becomes `/path/to/**`
- **MCP tools**: always `*` (per-tool, so allowing `deepwiki.search` will not cover `deepwiki.fetch`)
- **webfetch / websearch** (and anything else gated): the exact URL or query string is stored as-is

For MCP tools, both allow and deny decisions generalize to `*` (the entire tool). MCP inputs are opaque JSON with no meaningful scope pattern. Denying a single MCP invocation denies that tool until you revoke the rule.

## Auto Mode (bash)

Auto mode puts a separate-context classifier model in front of **every** bash command. A silent, throwaway session (empty history, its own model unless `auto_model` pins one) reviews the command against a safety policy and returns accept or deny with a one-line reason. Approved commands run as usual. A clean deny asks you for permission first (unless YOLO is on, see below): the normal allow / deny / remember prompt appears and you decide. Auto mode only skips the prompt when the classifier fails (spawn, timeout, or a malformed verdict): the command is denied fail-closed with the classifier error.

The classifier sees only the command and working directory in a silent throwaway session. Its turn is never relayed into your main conversation, so nothing about the main run leaks into its decision and its reasoning does not clutter the thread. The default system prompt is the codex-guardian policy (bundled, `auto_classifier_prompt.lua`).

Enable it like YOLO:

```lua
-- ~/.config/makima/init.lua
maki.setup({
    plugins = {
        bash = {
            auto_mode = true,
        },
    },
})
```

Or toggle it live with `/automode`, or start the session with `--automode`. The live `/automode` toggle wins while running; a config or `--automode` seed is only the starting state.

The tri-state verdict is strict:

- **approve** (`approved: true`) runs the command. The tool view shows an `auto-mode: allowed` line so you can see the classifier let it through.
- **deny** (`approved: false`) asks you for permission first: the same allow / deny / remember prompt as any other gated tool. The command then runs exactly as it would with auto mode off: allow once, allow for the session, or always-allow all work, and your existing permission rules still apply. With YOLO on there is no prompt and the deny rejects the command with the classifier's reason instead.
- **error** (classifier failure, timeout, or a non-boolean verdict) denies the command fail-closed with the classifier error. No user prompt is involved, and the command never auto-runs.

Things to watch:

- A deny asks instead of rejecting, so a stricter classifier means more prompts, not more silent rejections. If the classifier is too strict, tune the prompt or disable auto mode.
- Permission rules (allow once, allow for the session, always-allow, explicit denies) only enter through the deny prompt. approve and error verdicts are the classifier's call alone, so a session allow will not rescue a command when the classifier errors.
- YOLO flips the deny behavior to the old one: reject outright with the classifier's reason, no prompt. This is YOLO's "no prompts, gates stay authoritative" reading, consistent with explicit deny rules still applying under YOLO.
- `auto_model` is optional. Unset, the classifier runs on the same model as the current session; set it to pin a dedicated classifier model (a cheap one keeps the gate fast).

The `/automode` toggle only flips `auto_mode`; it does not change the deny-to-prompt behavior.

## YOLO Mode

To skip prompts on gated tools, toggle YOLO with `/yolo`, or run with `--yolo`. Explicit deny rules still apply. Tools that never declare permission scopes are unaffected (they never prompted).

To start in YOLO mode every time:

```lua
-- ~/.config/makima/init.lua
maki.setup({
    always_yolo = true,
})
```

## Bash Command Parsing

Bash commands get parsed with tree-sitter to extract individual commands. Something like `cd /tmp && cargo test` is checked as two separate commands.

Some constructs are too complex to analyze statically, so they always trigger a prompt:

- Command substitution: `$(...)`, backticks
- Process substitution: `<(...)`, `>(...)`
- Subshells: `(...)`
- Arithmetic expansion: `$((...))`

Brace groups `{ ... }` and control flow (`if`, `for`, …) are segmented when possible; they do not by themselves force a prompt the way substitutions do.

## Plugin Permissions

Lua plugins have a separate, unrelated gate. A `plugin.toml` manifest next to the Lua file controls which gated `maki.*` APIs it may call. No manifest means every gated call is denied, including for your own `init.lua`. The [Lua API reference](/lua-api/#plugin-permissions) documents the manifest and lists every permission.

## Session Persistence

When you save a session, its permission rules are saved too. Loading the session restores them.
