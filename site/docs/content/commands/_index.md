+++
title = "Commands"
weight = 8
[extra]
group = "Reference"
+++

# Commands

Type `/` in the TUI input box to open the command palette. A leading slash command is also recognized in `--print`, SDK stream mode, and ACP. Command names use exact ASCII-insensitive matching. Unknown and unavailable slash-prefixed text remains a model prompt. A known available command with invalid arguments returns an error instead of becoming a prompt.

The active registry combines built-ins, custom Markdown commands, MCP prompts, and Lua commands. Lua commands have the highest collision priority, followed by MCP prompts, custom commands, and built-ins. Each frontend advertises its capabilities, and the registry omits commands that require unavailable capabilities. The Lua `tui_only` field maps to the interactive-TUI capability. Registrations can change when plugins reload or MCP servers reconnect. The palette and protocol command lists show the current target-scoped winners. Root CLI subcommands such as `maki auth` are separate from slash commands.

## Built-in commands

| Command | Description | Arguments | TUI-only |
|---------|-------------|-----------|----------|
| `/tasks` | Browse and search tasks |  | yes |
| `/compact` | Summarize and compact conversation history |  | no |
| `/new` | Start a new session |  | no |
| `/clear` | Alias for `/new` |  | no |
| `/help` | Show keybindings |  | yes |
| `/usage` | Show token usage breakdown |  | yes |
| `/queue` | Remove items from queue |  | yes |
| `/model` | Switch model | <model> | no |
| `/theme` | Switch color theme | <theme> | yes |
| `/mcp` | Configure MCP servers |  | yes |
| `/login` | Authenticate with an LLM provider |  | yes |
| `/cd` | Change working directory | <path> | no |
| `/btw` | Ask a quick question (no tools, no history pollution) | <question> | no |
| `/yolo` | Toggle YOLO mode (skip all permission prompts) |  | no |
| `/thinking` | Toggle extended thinking (off, adaptive, effort level, or budget) | <mode> | yes |
| `/fast` | Toggle Anthropic fast mode (Opus only) |  | no |
| `/workflow` | Toggle workflow mode (task callable inside code_execution) |  | no |
| `/exit` | Exit the application |  | no |
| `/reload` | Reload plugins and config |  | no |

The portable built-ins are `/compact`, `/new` (and `/clear`), `/model`, `/cd`, `/btw`, `/yolo`, `/fast`, and `/workflow`. ACP advertises these built-ins plus custom, MCP, and portable Lua commands. Commands that require TUI capabilities are omitted from ACP. Invoking an unavailable command sends the complete input as ordinary model text.

## Bundled plugin commands

Bundled Lua plugins register these commands at startup. Plugin commands have higher collision priority than built-ins, so a bundled plugin can replace a built-in implementation for the targets it supports.

| Command | Description | Arguments | TUI-only |
|---------|-------------|-----------|----------|
| `/automode` | Toggle bash auto mode (classifier gates every bash command) |  | no |
| `/build` | Switch to build mode (full tool access) |  | no |
| `/memory` | View, edit, and delete memory files |  | yes |
| `/plan` | Switch to plan mode (analyse and write only the plan file) |  | no |
| `/rename` | Rename the current session | <title> | yes |
| `/sessions` | Browse and switch sessions | [query] | yes |
| `/splash` | Preview and select a splash renderer | [splash] | yes |
| `/splash-fps` | Toggle the splash fps overlay: live fps and per-frame render time. |  | yes |
| `/thinking` | Set thinking effort (bare opens a selector) | [effort] | yes |

## Command arguments

`/model` and `/theme` also accept an argument. While you type it, the palette lists the possible values (model specs, theme names), and submitting resolves the argument without opening the picker:

- **`/model <spec>`**: a full `provider/id` spec is used as-is, even if it is not in the discovered list. A fragment is fuzzy-matched against the discovered specs, and a unique match switches to it. Zero or multiple matches flash a note and keep the current model.
- **`/theme <name>`**: the exact name is applied and persisted. A fragment that matches one theme name (like `toky` for `tokyonight`) is resolved the same way; unknown or ambiguous names flash a note and leave the current theme. With no argument, the picker opens and previews each theme as you navigate.

## Sessions

Sessions run concurrently. `/new` starts a fresh session while the old one keeps working in the background, and `/sessions` shows the live status of each (working, needs input, idle) so you can jump between them. The picker lists this directory's sessions; a session open in another terminal is greyed out and cannot be opened from here. When a background session finishes or needs input, Makima flashes a note in the status bar. `/rename` renames the current session; in the session picker, `Ctrl+N` / `Ctrl+R` / `Ctrl+D` create, rename, and delete.

## Modes and toggles

- **`/yolo`**: skip permission prompts for this session (deny rules still apply). Config: `always_yolo = true`.
- **`/thinking`**: extended thinking. Optional arg: `off`, `adaptive`, an effort level (`minimal` … `max`), or a token budget number. Config: `always_thinking`.
- **`/fast`**: Anthropic fast mode (Opus only; ignored on other models). Config: `always_fast = true`.
- **`/workflow`**: let `code_execution` call the `task` tool (and other workflow-only tools) from inside the Python sandbox. Config: `always_workflow = true`.
- **Plan / build**: not a slash command. Press `Tab` in the input to toggle plan mode (plan-file writes only).
- **`/reload`**: rebuild plugins and config without leaving the app.
- **`/btw`**: one-shot side question with no tools and no history pollution.
- **`/memory`**: open the memory file picker (view / edit / delete). See the `memory` tool under [Tools](/docs/tools/).

## Custom commands

You can define your own slash commands as Markdown files. Empty files are skipped.

### Discovery and priority

Later sources override earlier ones when the command **name** matches (the stem of the file, or `name` in frontmatter):

1. User config: `~/.config/makima/commands/` (and legacy `~/.makima/commands/` if present)
2. User third-party: `~/.claude/commands/`
3. Project dirs, walking from the current working directory up to the nearest `.git` root. At each level: `.makima/commands/`, then `.claude/commands/`

Because the walk goes cwd → … → git root, a command at the **repository root overrides** the same name found only under a nested cwd. Project commands override user commands. Palette names are `/project:<name>` or `/user:<name>` depending on which scope won.

Skip all of the above with `--no-commands` (see [CLI](/docs/cli/)).

### Metadata

You can add optional metadata at the top of the file between `---` lines to set `name`, `description`, and `argument-hint`:

```markdown
---
description: Review code for issues
argument-hint: <file>
---
Review $ARGUMENTS and suggest improvements.
```

### Arguments

Use `$ARGUMENTS` in the command body. It gets replaced with whatever you type after the command name. The command is treated as accepting args if the body contains `$ARGUMENTS` or you set `argument-hint`.

For example, `/project:review main.rs` replaces `$ARGUMENTS` with `main.rs`.

## Aliasing commands

Prefer a different name for a command? `maki.api.run_command` runs any slash command exactly as typing it would, so an alias is a one-line handler in your `init.lua` instead of a reimplementation.

```lua
-- ~/.config/makima/init.lua
local aliases = {
    { name = "/clear", target = "/new", description = "Alias for /new" },
    { name = "/resume", target = "/sessions", description = "Alias for /sessions" },
}

for _, alias in ipairs(aliases) do
    maki.api.register_command({
        name = alias.name,
        description = alias.description,
        handler = function()
            local ok, err = maki.api.run_command(alias.target)
            if not ok then
                maki.ui.flash("could not run " .. alias.target .. ": " .. err)
            end
        end,
    })
end
```

Both names stay in the palette: aliasing adds a name, it does not rename or hide the original. It works for any command listed above, plus plugin commands and MCP prompts. See [`maki.api.run_command`](/docs/lua-api/#maki-api-run_command) for matching and error handling, or [`maki.ui.action`](/docs/lua-api/#maki-ui-action) to bind a key instead of a name.

Related: [CLI](/docs/cli/) for shell flags and subcommands, [Skills](/docs/skills/) for on-demand playbooks.