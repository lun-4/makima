+++
title = "Headless Mode"
weight = 21
[extra]
group = "Guides"
+++

# Headless Mode

Run Makima non-interactively with `--print` / `-p`. Useful for scripts, CI, and automation.

```bash
makima "explain this codebase" --print
```

Pipe via stdin:

```bash
echo "list all TODO comments" | makima -p
```

## Output Formats

| Format | Description |
|--------|-------------|
| `text` | Raw response only (default) |
| `json` | Single JSON object with metadata |
| `stream-json` | JSONL stream, one event per line |

```bash
makima "fix the tests" --print --output-format json
```

JSON output includes `type`, `subtype`, `is_error`, `duration_ms`, `num_turns`, `result`, `stop_reason`, `session_id`, `total_cost_usd`, and `usage`.

Add `--verbose` to include full turn-by-turn messages in the output.

## Slash commands

A prompt whose first token is an available slash command runs that command before Maki creates model input. Custom Markdown commands, MCP prompts, and Lua commands can produce a normal agent turn. A side-effect-only command succeeds with empty output. A known available command with invalid arguments exits with an error. Unknown and unavailable slash-prefixed text remains a model prompt.

SDK stream mode and ACP project the built-ins supported by their target capabilities. `/model` accepts an explicit model spec. One-shot print mode supports `/exit`. Commands that require TUI capabilities remain literal input in headless frontends. Local commands reject image attachments, while agent-turn commands preserve them.

See [Commands](/docs/commands/) for matching, collision priority, and the generated command list.

## Claude Code Compatibility

Makima's `--print` is a drop-in replacement for Claude Code:

```bash
# Before
claude "fix the bug" --print --output-format json

# After
makima "fix the bug" --print --output-format json
```

Same JSON fields, same `--output-format` options, same `--verbose` behavior. Scripts that parse Claude Code output work unchanged.

## SDK / Stream Mode

For tools like Conductor, Windsurf, or custom orchestrators that speak the Claude Code SDK wire protocol, use `--input-format stream-json`:

```bash
makima --print --input-format stream-json
```

This enters a bidirectional NDJSON loop over stdio instead of the one-shot print path:

```
your orchestrator                     makima --print --input-format stream-json
        │                                             │
        │  {"type":"user",...}            (stdin)     │
        ├─────────────────────────────────────────────►
        │                                             │
        ◄─────────────────────────────────────────────┤
        │  system / assistant / stream_event / result │
        │  one JSON object per line       (stdout)    │
```

Inbound messages (`user`, `control_request`, `control_response`, `control_cancel_request`) drive the agent; outbound messages match the Claude Code SDK shape. The initialization and control projections list the active slash commands. A `system/commands_update` event reports registry changes after plugin reloads or MCP updates. Under the hood it reuses the same driver as the TUI and ACP server, so sessions, tools, and permissions all work the same way.

SDK-only flags (`--max-turns`, `--session-id`, `--fork-session`, `--permission-mode`, `--include-partial-messages`, ...) are listed in the [CLI flag matrix](/docs/cli/#flags-by-run-path). `--system-prompt` / `--append-system-prompt` apply in every run path.

Two caveats:

- One-shot `--print` always starts a **new** session in **build** mode. Plan mode and session resume need the SDK path (or the TUI).
- The plan file for SDK `--permission-mode plan` is `./plan.md` under cwd, not the state-dir `plans/<slug>.md` files the TUI uses.

### Quick example

```bash
echo '{"type":"user","message":{"content":"explain this repo"}}' \
  | makima --print --input-format stream-json --max-turns 3
```

## Examples

Pipe compiler errors back for a fix:

```bash
cargo build 2>&1 | makima "Fix these compiler errors." --print --yolo
```

Generate a changelog from recent commits:

```bash
git log --oneline v1.2.0..HEAD | makima "Write a user-facing \
  changelog grouped by: Added, Changed, Fixed. Skip chores." --print
```

Automated PR summaries in CI:

```bash
SUMMARY=$(git diff main..HEAD | makima "Write a 2-3 sentence \
  summary of this change for a PR description." --print)
gh pr edit --body "$SUMMARY"
```

Migrate an API across many files:

```bash
grep -rl 'old_api_call' src/ | while read file; do
  makima "In $file, migrate old_api_call() to new_api_call(). \
    Keep behavior identical." -p --yolo --allowed-tools Read,Edit
done
```

Cost tracking:

```bash
makima "refactor the database layer" -p --output-format json | jq '.total_cost_usd'
```
