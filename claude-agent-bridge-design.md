# Claude Agent Bridge design

Status: first version implemented on the #44 branch, in `claude-agent-bridge/` and `plugins/providers/providers/claude_agent.lua`. It builds on the Lua provider API of #44 (`.agents/makima-plans/44-providers-lua.md`), including two additions made for it: [conversation lineage](.agents/makima-plans/44-providers-lua.md#conversation-lineage) and [retry classes](.agents/makima-plans/44-providers-lua.md#retry-classes). Where the build differs from the first draft of this document, the text below says what was built.

## Goal

Add a Claude Agent provider to Makima using Anthropic's unmodified TypeScript Agent SDK and the user's installed Claude Code executable. Makima keeps its agent loop, tools, permissions, user interface and canonical conversation history. A separate TypeScript bridge owns SDK sessions and translates between the SDK and a local HTTP API that a Lua provider calls.

The integration follows OpenCode's Claude Agent provider ([PR #58782](https://github.com/anomalyco/opencode/pull/58782)). It does not change or reimplement the Agent SDK. The bridge is application code against the SDK's public API.

The bridge is also the smoke test for the provider API: it is the most stateful provider Makima would have, so anything the API is missing shows up here first.

## Compliance

The OpenCode PR author [states](https://github.com/anomalyco/opencode/pull/58782#issuecomment-6527771235) that OpenCode received Anthropic's permission, which shows that FOSS coding agents such as Makima and OpenCode can use the Agent SDK for coding agent purposes. The SDK's [branding guidelines](https://code.claude.com/docs/en/agent-sdk/overview#branding-guidelines) allow the name "Claude Agent" and not "Claude Code", so the provider id is `claude-agent`.

## Architecture

```text
Makima agent loop and tool dispatcher
        |
        | provider request, events and response (makima-providers)
        v
Lua provider `claude-agent` (plugins/providers)
        |
        | HTTP and SSE over a Unix socket (ctx.send)
        v
TypeScript Agent Bridge
        |
        | public SDK API and an in-process MCP server
        v
Claude Agent SDK -> Claude Code executable
```

Makima sends a model request to the Lua provider. The provider forwards it to the bridge and turns the bridge's SSE events into Makima provider events. The bridge starts or resumes a Claude SDK session and offers Makima's tools to Claude through an SDK MCP server.

When Claude calls a tool, the MCP handler waits. The bridge ends the step with a normal assistant response holding the tool call. Makima applies its permissions and runs the tool, and its next request carries the result. The bridge hands the result to the waiting handler, and the same SDK turn continues.

The bridge never runs Makima tools. Claude Code's built-in tools are off. If the bridge cannot offer Makima's tools, the request fails; it never falls back to Claude Code's tools.

### Ownership

| Concern | Owner |
| --- | --- |
| User interface, agent loop, tool dispatch and permissions | Makima |
| Canonical history, model selection and compaction | Makima |
| Translation between Makima provider calls and the bridge API | Lua provider |
| SDK sessions, MCP handlers, history synchronization | Bridge |
| Claude runtime and model communication | Claude Agent SDK and Claude Code |

The bridge holds conversation data and can start SDK sessions, but cannot run Makima tools. Every tool call it returns still goes through Makima's dispatch and permission checks.

## What OpenCode does

OpenCode's provider lives in `packages/core/src/plugin/provider/claude-agent` (about 2,400 lines, checked at `fe04656465`) with a spec in `specs/v2/claude-agent-provider.md`. The parts the bridge takes over:

- **Tools.** One SDK MCP server named `opencode` offers every tool as `mcp__opencode__<name>`, with names over the 64-character limit shortened and hashed (`tools/names.ts`). `tools: []` turns the built-in tools off and `allowedTools: ["mcp__opencode__*"]` with `permissionMode: "dontAsk"` refuses anything else. Tools are marked not read-only, so Claude calls them one at a time.
- **Parking.** A `PreToolUse` hook records Claude's tool-use id, which the MCP handler cannot see. The handler then waits on a promise keyed by that id (`tools/pending.ts`). Because the hook holds one id at a time, tool calls run serially. A `PostToolUse` hook adds PDFs and images that an MCP result cannot carry.
- **Step boundary.** A step ends at `message_stop` of an assistant message that called an offered tool (`claude/session.ts`). The SDK turn stays open. Calls to tools that were not offered get Claude Code's own error and do not end the step.
- **Processes.** One long-lived `claude` process per chat, stopped after 5 minutes idle, never while a tool is waiting. A change to the system prompt, tools, connection settings or output limit restarts it; model and effort change on the running process (`claude/sessions.ts`).
- **History.** OpenCode's history is the source of truth. Claude cannot load history it did not write, so history it missed is sent as one user message wrapped in `<conversation-checkpoint>` (`sync/catch-up.ts`). After an edit or undo it forks Claude's session at the last point both histories agree (`claude/fork.ts`, using the SDK's `forkSession` with `upToMessageId`), or starts fresh. After an interrupt it keeps Claude's partial reply.
- **Side calls.** Compaction and title generation fork the chat's Claude session and keep its tools and system prompt, so Claude reuses the prompt cache (8,577 newly cached tokens fell to 32 in their test).
- **Isolation.** `settingSources: []`, `settings: { disableAllHooks: true }`, `strictMcpConfig`, `skills: []`, `verbatimPrompts`, summarized adaptive thinking, and environment variables that turn off CLAUDE.md files, auto memory, auto compaction, away summaries, tool search, the advisor tool and the non-streaming and refusal fallbacks. `MAX_MCP_OUTPUT_TOKENS` is raised so the host's own truncation applies. `CLAUDE_CODE_SIMPLE`, `CLAUDE_CODE_EFFORT_LEVEL` and `MCP_TOOL_TIMEOUT` are cleared from the user's environment (`claude/query-options.ts`). It needs Claude Code 2.1.248 or later.
- **Usage.** Claude Code reports one response's usage several times while streaming and again as a turn total. OpenCode keeps the highest count per response id and takes context size from the last response's input (`language-model/usage.ts`).
- **Retries.** Claude Code retries failed requests itself, so OpenCode does not retry this provider.

OpenCode gets the identity it needs from its own chat session id and a fork header. Makima's provider API gives the bridge the same through `invocation` and `invocation.previous`.

## The provider API the bridge uses

All of this exists on the #44 branch.

- **Registration.** `makima.api.register_provider` with `models`, `discover` (forwards to the bridge's model list), `auth` (a login method that runs `claude auth login` through `ui:run`, which only `makima auth login` supports), `auth_status` (the bridge's status) and `prepare`. Documented on `register_provider` in `makima-lua/src/api/provider.rs`.
- **Request.** `stream(ctx, request, emit)` gets `model` (`id`, `max_output_tokens`, `context_window`), `system`, `messages`, `tools`, `thinking` (`{ kind = "off" | "adaptive" }`, `{ kind = "effort", level }` or `{ kind = "budget", tokens }`), `fast`, `session_id` and `invocation` (`conversation_id`, `operation_id`, `attempt`, `purpose` of `agent`, `compaction` or `isolated`, and `agent_id`). `(operation_id, attempt)` names one stream attempt. Messages use Makima's content blocks: `text`, `thinking`, `redacted_thinking`, `tool_use`, `tool_result` (string `content`, optional `is_error`) and `image`.
- **`invocation.previous`** (`id`, `relation` of `continues` or `branches`, `shared`) on every request of a conversation started from another one. See [conversation lineage](.agents/makima-plans/44-providers-lua.md#conversation-lineage).
- **Events.** `emit` takes `text_delta`, `thinking_delta`, `thinking_block_end`, `tool_use_start` (`id`, `name`) and `prompt_progress`, and returns false once the call is over. Tool arguments arrive only in the final message.
- **Response.** `stream` returns `{ message, usage, stop_reason }`, with usage as `input_tokens`, `output_tokens`, `cache_creation_input_tokens` and `cache_read_input_tokens`.
- **Errors.** nil plus an error table: `kind = "config"` for what only the user can fix, `status` and `message` (optional `retry_after`) for an API error, `overflow = true` when the request does not fit the context.
- **`retry`** on the error table (`rate_limit`, `transient`, `connect`, `timeout`, `none`). An error without it is not retried. See [retry classes](.agents/makima-plans/44-providers-lua.md#retry-classes).
- **Conversation end.** The optional instance method `end_conversation(ctx, conversation_id, reason)`, with `cancelled`, `closed`, `compacted` or `rewound`. The host calls it detached after every stream for that id is dropped, including when a turn is cancelled while Makima waits on a tool after a tool-use response.
- **Transport.** `ctx.send` is `makima.net.send` with the `[provider]` timeouts as defaults. It takes `unix_socket`, has no byte cap on streamed bodies, no private-address guard and follows no redirects. The shared `sse` module in `plugins/providers/providers/` parses the step stream.
- **Process.** `makima.fn.jobstart` can start the bridge, as executable providers start their scripts.

## Conversation identity and lineage

The bridge keys its state by `conversation_id`. Each conversation has its own SDK session record, process, waiting tool calls and lock.

| Request | Bridge action |
| --- | --- |
| Known `conversation_id` | Continue its SDK session. If tools are waiting, the request must carry their results. |
| New id, no `previous` | Start a fresh SDK session. History before the last user message goes in as a checkpoint. |
| New id, `previous.relation = "continues"` | Take over `previous.id`'s SDK session if the bridge still has it, and catch up from message `shared`. After a cancel the old turn was already interrupted and drained. |
| New id, `previous.relation = "branches"` | Fork `previous.id`'s SDK session at the last Claude message within the first `shared` messages, then catch up from there. Without a usable fork point, start fresh. |
| `previous.id` unknown to the bridge | Treat as no `previous`. |
| `previous.id` still running | `end_conversation` is detached, so a successor can arrive first. Interrupt and drain `previous.id` as for `cancelled`, then continue or fork it. |

Child agents have their own conversation ids and start fresh. Isolated turns and compaction summaries branch from the conversation they copy. Today compaction rewrites old messages, so its `shared` is small and the fork rarely pays off; a compaction that only appends its request (a non-goal of #44, tracked separately) would make it pay off for native providers too. Isolated turns send no tools while the main conversation does, and a different tool set needs a new process, so they also start fresh until they keep the main conversation's tools.

To find fork points, the bridge records for each conversation the SDK session id, how many Makima messages the SDK has seen, and the Claude message uuid that ends each Makima message it wrote or received. Because the host says exactly how many messages are shared, the bridge does not hash histories the way OpenCode does.

The SDK session id stays inside the bridge. It is never interchangeable with any Makima id.

## Concurrency

The bridge serves many conversations at once. A waiting tool or permission prompt in one does not block another. Steps within one conversation are serialized; a second concurrent step for the same conversation gets a conflict. Tool correlation is serial, as in OpenCode.

The bridge limits running processes, queued requests, message sizes and kept state. A conversation whose tools are waiting for Makima is never idle. Abandoned conversations are cleaned up by a bounded orphan policy.

## HTTP API

The bridge listens on a Unix socket in Makima's state directory, created with user-only permissions. The socket's file permissions authenticate the client, so there is no bearer token and no TCP listener. All requests and responses are JSON except the SSE step stream.

| Endpoint | Purpose |
| --- | --- |
| `GET /v1/status` | Protocol version, Claude Code version, login and readiness, for `auth_status`. |
| `GET /v1/models` | Model descriptors Claude reports, for `discover`. |
| `POST /v1/steps` | Start or continue one model step; the response is SSE. |
| `POST /v1/conversations/{conversation_id}/end` | `end_conversation`, with the reason in the body. |

There is no cancel endpoint: dropping the stream cancels the step, and the bridge treats a disconnect before the terminal frame as an interrupt.

### Step request

The Lua provider posts the request it received, unchanged apart from encoding:

```json
{
  "protocol_version": 1,
  "request": {
    "model": { "id": "claude-sonnet-4-5", "max_output_tokens": 64000, "context_window": 200000 },
    "system": "Makima system instructions",
    "messages": [{ "role": "user", "content": [{ "type": "text", "text": "Inspect the project." }] }],
    "tools": [{ "name": "read", "description": "Read a file.", "input_schema": { "type": "object" } }],
    "thinking": { "kind": "adaptive" },
    "fast": false,
    "session_id": "makima-session-1",
    "invocation": {
      "conversation_id": "c-1",
      "operation_id": "o-1",
      "attempt": 0,
      "purpose": "agent",
      "agent_id": "a-1",
      "previous": { "id": "c-0", "relation": "continues", "shared": 12 }
    }
  }
}
```

The bridge validates tool names, schemas and model settings and rejects what it cannot honour rather than changing its meaning. A change to tools, system prompt or connection settings restarts the conversation's process; model and effort apply to the running one.

### Step response

`text/event-stream`. Each `event` frame holds one Makima provider event. Exactly one terminal frame ends the stream: `done` with the full `{ message, usage, stop_reason }`, or `error` with a Makima error table. EOF without a terminal frame is a failure. SSE comments carry heartbeats while Claude Code is slow or retrying, so the stream stays inside the provider idle timeout.

```text
event: event
data: {"type":"text_delta","text":"I will inspect the project."}

event: event
data: {"type":"tool_use_start","id":"toolu_123","name":"read"}

event: done
data: {"message":{"role":"assistant","content":[{"type":"text","text":"I will inspect the project."},{"type":"tool_use","id":"toolu_123","name":"read","input":{"path":"Cargo.toml"}}]},"usage":{"input_tokens":100,"output_tokens":30,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},"stop_reason":"tool_use"}
```

A tool-use response leaves its handlers waiting after the stream closes. Usage follows OpenCode: the highest count per response id, never a sum of repeated updates.

### Tool-result continuation

The next request carries Makima's full history. The bridge takes the `tool_result` blocks after the last assistant message, matches each to a waiting call by `tool_use_id`, and rejects missing, duplicate or unknown results before resolving any handler. Image blocks that follow a `tool_result` in the same user message are folded into that call's MCP result. A handler is resolved once.

Tool names are encoded MCP-safe with a reversible mapping, and only offered names are routed back to Makima.

A continuation the bridge refuses fails that step with `retry = "none"`, and the bridge stops the conversation's process. The next step starts again from the last reply Claude finished, so one bad request does not leave the conversation stuck.

### Errors and retries

Before the SSE headers the bridge answers with an HTTP status and a JSON error table; after them with a terminal `error` frame. The Lua provider returns the table as is.

The bridge sets `retry` on every error. Anything after the SDK session advanced is `none`, since Claude Code has already retried and a second attempt could duplicate a step. A failure before the bridge touched the SDK, such as a refused socket connection, keeps the class `http.classify` gives it. Context overflow is `overflow = true`; login and setup problems are `kind = "config"`.

A request whose `(operation_id, attempt)` the bridge already started gets a conflict instead of a second SDK turn.

## Cancellation and lifecycle

A disconnect before the terminal frame interrupts the SDK turn. The bridge waits for the SDK iterator to drain before clearing waiting handlers and accepting the next step, as OpenCode does. Late events from a cancelled step are dropped.

`end_conversation` maps to:

| Reason | Bridge action |
| --- | --- |
| `cancelled` | Interrupt and drain if still running, then keep the session for a grace period in case a successor `continues` it. |
| `rewound` | Keep the session for a grace period; a successor continues or forks it at `shared`. |
| `compacted`, `closed` | Stop the process and discard the session. |

The host does not end conversations at exit and resumes them later, so a conversation's SDK session survives a bridge restart: the bridge saves a record per conversation and resumes the SDK's transcript. Waiting handlers do not survive a restart. Claude's transcript then ends at a tool call nothing will answer, so the bridge forks it at the last reply Claude finished and sends the rest of Makima's history as a checkpoint. Processes stop after 5 minutes idle; a conversation with waiting tools stops after an hour without an answer.

Forks use `resume` with `forkSession` and `resumeSessionAt`. Each saved fork point names the SDK session it is in, so a fork of a fork can still go back to a point in the first session.

### Bridge process

The first version is started by hand and found at its socket path. A managed bridge, started by the provider with `makima.fn.jobstart` on first use, follows once the protocol is stable.

## Claude Code settings

The bridge passes every setting explicitly and reads none of the user's Claude configuration, following OpenCode's list above: no settings files, hooks, MCP servers, skills, CLAUDE.md, auto memory, auto compaction, away summaries, tool search, advisor tool, non-streaming or refusal fallback, and the three user variables cleared. Makima's own compaction, subagents and plan mode apply. Disabling these is not a sandbox; the executable is the one the user chose, by path or `PATH`.

## Lua provider sketch

```text
register_provider({ id = "claude-agent", ... })
  auth: one method that runs `claude auth login` through ui:run
  auth_status(ctx): GET /v1/status
  discover(ctx): GET /v1/models
  prepare(ctx, model, config): socket path from config, returns the instance

instance.stream(ctx, request, emit):
  POST /v1/steps with the request
  parse SSE, forward `event` frames to emit, stop when emit returns false
  return the `done` payload, or the `error` table
instance.end_conversation(ctx, id, reason):
  POST /v1/conversations/{id}/end
instance.fetch_usage(ctx):
  the latest rate-limit information the bridge saw, if any
```

## Validation

Tests cover:

- Two agents sharing a Makima session without sharing SDK state.
- Isolated turns and compaction running while the main conversation waits on a tool.
- A tool call, Makima's permission decision, the result and the resumed SDK turn.
- Several tool calls in sequence, unknown tools, missing and duplicate results.
- Failed tool registration with no fallback to Claude Code's tools.
- Interrupting a stream, cancelling while a tool waits, late events and a bridge restart.
- Each lineage case: cancel then continue, rewind then continue at `shared`, fork, isolated turn, compaction summary, resume before the first request under a new id.
- Repeated `(operation_id, attempt)` and ambiguous failures returning `retry = "none"`.
- SSE chunk boundaries, heartbeats, terminal frames and `emit` returning false.
- Usage without double counting.

The bridge's tests (`npm test`, `just bridge-check`) stand a scripted `Session` in for Claude Code, below the SDK. A fake executable that replays recorded sessions through the real SDK, as OpenCode does, and opt-in tests against an installed `claude` are still to come; the first version was checked by hand against Claude Code 2.1.318 for a tool step and its continuation, cancel and continue, a fork at a boundary, and resume after a bridge restart. The Lua provider has a spec in `plugins/providers/tests/claude_agent.lua` with a scripted `ctx`.

## Decided

- Lineage: `invocation.previous` with `continues` or `branches` and an exact `shared` count from the host, rather than a second stable id, keeping ids across cancels, or the bridge hashing histories.
- Compaction summaries and isolated turns branch through the same field.
- Retries: the provider names the retry class on each error; the host only applies budgets.
- Rich tool results: the bridge folds image blocks into MCP results; Makima's `tool_result` stays a string.
- Transport: a Unix socket with user-only permissions instead of a loopback port and token.
- Cancellation: the stream's disconnect and `end_conversation`; no cancel endpoint.
- System prompt: Makima's `request.system` alone, replacing Claude Code's. Every SDK session runs in one bridge directory (`<state>/workdir`), since Makima's tools resolve paths themselves.
- A bridge that is not running is a config error with the command to start it, not a transport failure to retry.

## Open

1. A managed bridge process: when to start it, how to find a stale socket, and how Makima checks the bridge's version.
2. How long to keep a `cancelled` or `rewound` session for a successor.
3. Which SDK settings Makima exposes as provider config (base URL, API key, headers, output limit), following OpenCode's mapping.
4. Cost: subscription logins report $0; API key and cloud logins need pricing for the model Claude resolves.
5. Keeping the main conversation's tools in isolated turns, so they can fork instead of starting fresh.
6. `fetch_usage`: the rate-limit information Claude Code reports is not passed on yet.
7. Context windows: models are described as 200,000 tokens, or 1,000,000 for a `[1m]` model, rather than asked of Claude Code per model as OpenCode does.
