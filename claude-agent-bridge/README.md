# Claude Agent bridge

Serves Maki's `claude-agent` provider. It runs your installed Claude Code through the Claude Agent SDK and listens on a Unix socket that the Lua provider (`plugins/providers/providers/claude_agent.lua`) talks to. The design is in `claude-agent-bridge-design.md` at the repository root.

```sh
cd claude-agent-bridge
npm install --omit=optional   # the SDK's bundled Claude Code is not needed
node src/main.ts              # Node 24 or later runs the TypeScript directly
```

Then log in once with `maki auth login claude-agent` (it runs `claude auth login`) and pick a model such as `claude-agent/sonnet`.

By default the bridge lives in `$XDG_STATE_HOME/makima/claude-agent` (`~/.local/state/makima/claude-agent`):

```text
bridge.sock      the socket, readable only by you
conversations/   one record per conversation, so it resumes after a restart
workdir/         where Claude Code runs; its transcripts go under ~/.claude/projects
```

`--state`, `--socket` and `--claude` move them. A provider pointed at another socket reads `socket` from the `[claude-agent]` section of `providers.toml`.

## Layout

```text
src/main.ts           arguments, startup
src/server.ts         HTTP routes, SSE frames, heartbeats
src/conversations.ts  one entry per Maki conversation: lineage, steps, lifecycle
src/session.ts        one Claude Code process (`query`), its turns and interrupts
src/convert.ts        Maki history to Claude content, SDK messages to Maki events
src/tools.ts          Maki's tools as an in-process MCP server, waiting handlers
src/options.ts        the SDK options that keep Claude Code out of Maki's way
src/records.ts        what is saved per conversation
src/protocol.ts       the request, events and error tables Maki uses
```

## Checks

```sh
npm run check   # tsc
npm test        # node --test, with a scripted stand-in for Claude Code
```
