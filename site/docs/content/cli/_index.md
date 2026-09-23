+++
title = "CLI"
weight = 11
[extra]
group = "Reference"
+++

# CLI

`makima` without a subcommand starts the TUI. Subcommands cover auth, models, MCP OAuth, updates, and a few debug helpers. Many flags only apply to one of three run paths: **TUI**, one-shot **`--print`**, or **SDK** (`--print --input-format stream-json`).

```bash
makima [OPTIONS] [PROMPT]
makima <COMMAND>
```

If you pass a prompt (or pipe stdin) without `--print`, the TUI still opens and that text is the first message. With `--print`, Makima runs non-interactively and exits when done.

## Flags by run path

| Flag | TUI | `--print` | SDK (`stream-json`) |
|------|-----|-----------|---------------------|
| `-m` / `--model` | yes | yes | yes |
| `--yolo` | yes | yes | yes (or `--permission-mode bypassPermissions`) |
| `--no-plugins` / `--no-commands` / `--no-rtk` / `--no-jit` | yes | yes | yes |
| `--allowed-tools` / `--disallowed-tools` | yes | yes | yes |
| `-c` / `--continue [ID]` | yes (ID, or picker with no ID) | no (ID ignored; bare flag errors) | yes (ID only) |
| `-l` / `--last` | yes | no (always new session) | yes |
| `--exit-on-done` | yes | n/a (always exits) | n/a |
| `--image` | no (use Ctrl+V paste) | yes | via wire protocol |
| `--verbose`, `--output-format` | no | yes | stream only |
| `--system-prompt`, `--append-system-prompt` | yes | yes | yes |
| `--max-turns`, `--session-id`, `--fork-session` | no | no | yes |
| `--permission-mode` | no | no | yes |
| `--include-partial-messages` | no | no | yes |

### Shared flags (detail)

| Flag | Description |
|------|-------------|
| `-p`, `--print` | Non-interactive run. See [Headless Mode](/docs/headless/) |
| `--image <PATH>` | Attach an image in `--print` mode (repeatable). Paths must be png, jpeg, gif, or webp |
| `-m`, `--model <SPEC>` | Model as `provider/model-id`. Fallback: last used → `provider.default_model` in config → auto-detect from available providers |
| `--verbose` | Full turn-by-turn messages in `--print` output |
| `-c`, `--continue [ID]` | Continue a specific session by ID (TUI / SDK). With no ID (TUI only), opens the session picker with this directory's sessions. A following positional is taken as the ID, so run `--continue` alone to open the picker. Resuming a session stored under another directory, or one open in another terminal, is an error. In `--print` mode an ID is ignored (print mode always starts a new session) and a bare flag is an error |
| `-l`, `--last` | Continue the most recent session in this directory (TUI / SDK only). Errors the same way if it is open in another terminal |
| `--output-format <text\|json\|stream-json>` | Output shape for `--print` (default `text`) |
| `--input-format <text\|stream-json>` | With `--print`, `stream-json` enters SDK mode |
| `--no-commands` | Skip custom commands from `.makima/commands`, `.claude/commands`, etc. |
| `--no-rtk` | Disable [rtk](https://github.com/rtk-ai/rtk) command rewriting |
| `--no-plugins` | Skip user `init.lua` (global and project); keep the Lua host and builtin plugins so tools and the default keymap still load |
| `--no-jit` | Run plugin Lua on the interpreter with full debug info |
| `--yolo` | Skip permission prompts on gated tools (alias: `--dangerously-skip-permissions`). Deny rules still apply |
| `--exit-on-done` | Exit when the agent finishes (TUI automation wrappers) |
| `--allowed-tools <LIST>` | Comma-separated allow list (PascalCase or snake_case) |
| `--disallowed-tools <LIST>` | Comma-separated deny list |
| `--session-id <ID>` | Session id for SDK mode |
| `--fork-session` | Load a session's history under a new id (SDK) |
| `--max-turns <N>` | Cap agent turns (SDK) |
| `--system-prompt <TEXT>` | Replace the system prompt entirely |
| `--append-system-prompt <TEXT>` | Append to the built-in system prompt |
| `--permission-mode <MODE>` | SDK: `default`, `acceptEdits`, `plan`, or `bypassPermissions` |
| `--include-partial-messages` | Stream partial deltas in SDK mode |

### Tool name lists

`--allowed-tools` / `--disallowed-tools` accept Claude Code PascalCase (`Read,Edit,Bash`) or snake_case (`read,edit,bash`). Makima lowercases PascalCase to snake_case and checks the result against the built-in tool names, so `CodeExecution` works but `MultiEdit` errors: it normalizes to `multi_edit`, and the tool is called `multiedit`. Write `multiedit` or `edit_lines` as-is. Unknown names error out with the list of valid names. The opt-in edit tools (`edit_lines`, `insert_lines`, `multiedit`) are always valid names here, even while disabled; listing one does nothing until you enable the tool in config.

### Permission modes (SDK)

| Mode | Effect |
|------|--------|
| `default` | Normal permission prompts |
| `acceptEdits` | Accepted for Claude Code compatibility; currently same as `default` |
| `plan` | Agent mode plan with plan file `./plan.md` under cwd |
| `bypassPermissions` | Same as `--yolo` for the SDK path |

If both `--yolo` and `--permission-mode` are set, the explicit mode wins. Unknown mode names warn and fall back to `default`.

Several other Claude Code flags are accepted and ignored so existing scripts keep parsing. Makima prints a warning when you pass one of them.

## Subcommands

### `makima auth`

```bash
makima auth login [provider]   # interactive picker if omitted
makima auth logout <provider>
makima auth status
```

`login` stores credentials under the state directory and can write plan / base URL choices into `providers.toml` (see [Configuration](/docs/configuration/#directory-layout) for the platform path). OpenAI and Copilot have dedicated flows; other providers prompt for a key (and a plan when the provider has more than one). Custom providers can be created from the interactive picker.

`status` shows each provider as configured (key on disk), env-only, or missing.

### `makima models`

```bash
makima models
makima models --refresh    # refetch the models.dev catalog
```

One spec per line, warnings on stderr. Built-in and script providers are listed live, catalog-backed providers from the models.dev cache, which expires after 24 hours and supplies model pricing and context windows. `--refresh` refetches it. When the refetch fails, the cached catalog stays in place, the list still prints, and the command exits non-zero.

### `makima sessions`

```bash
makima sessions --json
```

Lists every stored session across all directories, most recently updated first. The command is `--json`-only and prints a JSON array; each entry carries the id, title, `updated_at` (epoch seconds), `cwd`, and `open_elsewhere` (true while another terminal has the session open). The ID is what you pass to `makima -c <ID>` from the same directory.

### `makima mcp`

```bash
makima mcp auth <server>     # OAuth for an HTTP MCP server
makima mcp logout <server>   # drop stored tokens
```

Server names come from your [MCP config](/docs/mcp/). On a machine without a browser, `auth` prints a URL you open elsewhere and paste back.

### `makima update` / `makima rollback`

```bash
makima update            # install latest release
makima update -y         # skip confirmation
makima update --no-color
makima rollback          # previous version
```

Uses the same install locations as the install scripts.

### `makima acp`

```bash
makima acp
makima acp -m anthropic/claude-sonnet-4-6
makima acp --yolo
makima --no-jit acp
```

Starts an [ACP](/docs/acp/) server on stdio for editors like Zed. Subcommand flags are only `-m` / `--model` and `--yolo`. Global flags like `--no-jit` must come before the subcommand.

### `makima index`

```bash
makima index path/to/file.rs
```

Runs the `index` tool on a file and prints the skeleton, so you can see what the agent will get before a session. Builtin plugins always load here; `--no-plugins` only skips user `init.lua`.

### `makima prompt`

```bash
makima prompt                  # rendered system prompt (default: system variant)
makima prompt research
makima prompt general
makima prompt --plan           # system prompt + plan-mode reminder (system only)
makima prompt --tools          # tool definitions as JSON
makima prompt --tools --names  # tool names only, one per line
```

Debug helper for inspecting the prompt and tool surface the agent sees. `--plan` is rejected on non-system variants.

### `makima migrate`

```bash
makima migrate xdg
```

Moves data from `~/.makima/` into platform directories. Safe to re-run. See [Configuration](/docs/configuration/#directory-layout).

## Everyday examples

```bash
# TUI on a project
cd ~/code/my-app && makima

# One-shot with YOLO and a model pin
makima -p --yolo -m anthropic/claude-sonnet-4-6 "summarize the architecture"

# Resume yesterday's session
makima -l

# List stored sessions
makima sessions --json

# Pick a session to resume
makima -c

# List models, then log in
makima models
makima auth login

# Inspect tools without starting a session
makima prompt --tools --names
```

For JSON / stream-json output, stdin prompts, and SDK wire mode, see [Headless Mode](/docs/headless/).
