+++
title = "ACP"
weight = 22
[extra]
group = "Guides"
+++

# ACP (Agent Client Protocol)

Run Makima inside your editor. `makima acp` starts an [ACP](https://agentclientprotocol.com/) server over stdio, so any ACP-capable editor (like [Zed](https://zed.dev/)) can drive Makima as its coding agent.

```bash
makima acp
```

## Zed setup

Add Makima as a custom agent in Zed's `settings.json`:

```json
"agent_servers": {
  "Makima": {
    "default_config_options": {
      "model": "deepseek/deepseek-v4-flash"
    },
    "type": "custom",
    "command": "makima",
    "args": ["acp"],
    "env": {}
  }
}
```

The `model` value is a `provider/model-id` spec, same format as `makima --model`.

## What works

- Sessions persist. Loading a session replays the full conversation in the editor. A session that is open in another terminal cannot be loaded.
- Session options include Model, YOLO, Fast, Workflow, and available plugin options such as Bash auto mode. New and loaded sessions return the complete ordered option list. Option updates also contain the complete list.
- Model switching uses the editor's model selector. All configured providers appear. Providers such as OpenRouter add discovered models in the background. A model change and any required Fast disablement appear as one update.
- Modes switch between build mode and plan mode. Build mode has full tool access. Plan mode limits writes to the plan file.
- Tool permission prompts appear in the editor. The user can allow or reject the operation once or always.
- The `question` tool becomes an ACP elicitation form. If the client does not support elicitation, Makima removes the tool and the model asks in plain text.
- Tool progress streams during execution, including sub-agents and batched calls.
- Prompts can include images and editor-attached files.
- The editor receives the target-scoped command list. The list updates when plugin or MCP registrations change. An available leading slash command runs before model input. Unknown and unavailable slash-prefixed text remains a model prompt. Agent-turn commands preserve images and text resources. Local commands reject non-text content.
- `/compact` reports a `Compact context` tool call and saves the compacted model history before it reports success.
- `/btw` runs one isolated model request with copied history and no tools. Its question and response do not change the primary model history.
- `/cd` reports the canonical working directory as an agent message. Later relative tool locations use that directory.

ACP maps the target-scoped central registry to ACP command metadata. It does not implement or inspect individual command names. Commands that require TUI capabilities are omitted from ACP's advertised command list. `/new` and `/clear` require TUI session replacement, so ACP does not advertise or execute them. Typing either command forwards the complete prompt as ordinary model input. Custom Markdown commands, MCP prompts, and portable Lua commands use the same registry and collision rules as the TUI. See [Commands](/docs/commands/).

ACP cannot remove messages from the editor's visible transcript or change the editor's displayed session directory. `/compact` changes only the model context. `/cd` changes Makima's canonical working directory and reports it in the transcript, but the editor can continue to display its original directory field.

Authentication, providers, and permissions come from your normal Makima config. Set up [providers](/docs/providers/) first and ACP sessions just work.

```bash
makima acp
makima acp -m anthropic/claude-sonnet-4-6
makima acp --yolo
makima --no-jit acp
```

`makima acp` only takes `-m` / `--model` and `--yolo` as subcommand flags. Global flags like `--no-jit` must come before the subcommand (`makima --no-jit acp`, not `makima acp --no-jit`).

Plan mode in ACP uses the same state-directory plan files as the TUI (`…/plans/<slug>.md`), not the SDK's `./plan.md`.

A session open in another terminal is locked by it: loading fails until that instance exits, which releases the lock automatically (or a few seconds after it crashes).
