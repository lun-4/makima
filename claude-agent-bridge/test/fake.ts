import type { Options, SDKMessage } from "@anthropic-ai/claude-agent-sdk"
import type { ClaudeBlock } from "../src/convert.ts"
import { Conversations } from "../src/conversations.ts"
import type { Limits } from "../src/conversations.ts"
import type { Block, Message, StepRequest, Tool } from "../src/protocol.ts"
import { memoryRecords } from "../src/records.ts"
import type { Effort, Probe, ResultMessage, Session } from "../src/session.ts"
import { PREFIX } from "../src/tools.ts"

export const SESSION = "11111111-1111-4111-8111-111111111111"
export const READ: Tool = { name: "read", description: "Read a file.", input_schema: { type: "object" } }
export const LOGGED_IN: Probe = { version: "2.1.318", account: { email: "a@example.com" }, models: [] }

let uuids = 0
const uuid = () => `uuid-${++uuids}`

function message(value: object): SDKMessage {
  return { session_id: SESSION, parent_tool_use_id: null, uuid: uuid(), ...value } as unknown as SDKMessage
}

export const sdk = {
  init: (servers: { name: string; status: string }[] = [{ name: "maki", status: "connected" }]) =>
    message({ type: "system", subtype: "init", mcp_servers: servers }),
  start: (id: string, usage = { input_tokens: 10, output_tokens: 1 }) =>
    message({ type: "stream_event", event: { type: "message_start", message: { id, usage } } }),
  blockStart: (index: number, block: object) =>
    message({ type: "stream_event", event: { type: "content_block_start", index, content_block: block } }),
  delta: (index: number, delta: object) => message({ type: "stream_event", event: { type: "content_block_delta", index, delta } }),
  blockStop: (index: number) => message({ type: "stream_event", event: { type: "content_block_stop", index } }),
  stop: () => message({ type: "stream_event", event: { type: "message_stop" } }),
  assistant: (id: string, content: object[], extra: object = {}) =>
    message({ type: "assistant", message: { id, content, usage: { input_tokens: 10, output_tokens: 5 } }, ...extra }),
  result: (extra: object = {}) =>
    message({
      type: "result",
      subtype: "success",
      is_error: false,
      stop_reason: "end_turn",
      result: "",
      usage: { input_tokens: 10, output_tokens: 5 },
      ...extra,
    }),
}

// A whole reply that streams {text} and ends the turn.
export function textTurn(text: string, id = "msg-text"): SDKMessage[] {
  return [
    sdk.start(id),
    sdk.blockStart(0, { type: "text", text: "" }),
    sdk.delta(0, { type: "text_delta", text }),
    sdk.blockStop(0),
    sdk.assistant(id, [{ type: "text", text }]),
    sdk.stop(),
    sdk.result(),
  ]
}

// A reply that calls Maki's `read`, up to the end of that response.
export function toolTurn(callId: string, id = "msg-tool"): SDKMessage[] {
  return [
    sdk.init(),
    sdk.start(id),
    sdk.blockStart(0, { type: "tool_use", id: callId, name: `${PREFIX}read` }),
    sdk.blockStop(0),
    sdk.assistant(id, [{ type: "tool_use", id: callId, name: `${PREFIX}read`, input: { path: "a" } }]),
    sdk.stop(),
  ]
}

// Stands in for Claude Code: each `begin` or continuation takes the next
// scripted batch of SDK messages.
export class FakeSession implements Session {
  readonly options: Options
  readonly sent: { content: ClaudeBlock[]; next: boolean }[] = []
  readonly models: string[] = []
  readonly efforts: (Effort | undefined)[] = []
  readonly script: SDKMessage[][]
  ready: Promise<Probe> = Promise.resolve(LOGGED_IN)
  ended = false
  running = false
  interrupts = 0
  stops = 0
  private queue: SDKMessage[] = []

  constructor(options: Options, script: SDKMessage[][]) {
    this.options = options
    this.script = script
  }

  private next(): void {
    this.queue.push(...(this.script.shift() ?? []))
  }

  begin(content: ClaudeBlock[]): void {
    this.sent.push({ content, next: false })
    this.running = true
    this.next()
  }

  addUser(content: ClaudeBlock[]): void {
    this.sent.push({ content, next: true })
  }

  async readStep(endsStep: (message: SDKMessage) => boolean, signal: AbortSignal): Promise<ResultMessage | "tools"> {
    if (this.queue.length === 0 && this.running) this.next()
    while (true) {
      signal.throwIfAborted()
      const message = this.queue.shift()
      if (!message) {
        await new Promise((resolve) => signal.addEventListener("abort", resolve, { once: true }))
        continue
      }
      if (endsStep(message)) return "tools"
      if (message.type === "result") {
        this.running = false
        return message
      }
    }
  }

  async interrupt(): Promise<void> {
    if (!this.running) return
    this.interrupts++
    this.running = false
    this.queue = []
  }

  async stop(): Promise<void> {
    this.stops++
    await this.interrupt()
    this.ended = true
  }

  async setModel(model: string): Promise<void> {
    this.models.push(model)
  }

  async setEffort(effort: Effort | undefined): Promise<void> {
    this.efforts.push(effort)
  }
}

export const FAST_LIMITS: Limits = { idleMs: 60_000, orphanMs: 60_000, graceMs: 60_000, readyMs: 1_000, maxProcesses: 2 }

// A registry whose sessions play {scripts} in start order.
export function harness(scripts: SDKMessage[][][] = [], probe: Probe = LOGGED_IN, limits: Limits = FAST_LIMITS) {
  const sessions: FakeSession[] = []
  const records = memoryRecords()
  const conversations = new Conversations(
    { executable: "claude", workdir: "/tmp" },
    (options) => {
      const session = new FakeSession(options, scripts.shift() ?? [])
      session.ready = Promise.resolve(probe)
      sessions.push(session)
      return session
    },
    records,
    limits,
  )
  const events: unknown[] = []
  let opened = 0
  const sink = {
    open: () => void opened++,
    event: (event: unknown) => void events.push(event),
  }
  return {
    sessions,
    records,
    conversations,
    events,
    opened: () => opened,
    step: (request: StepRequest, signal = new AbortController().signal) => conversations.step(request, sink, signal),
  }
}

export const user = (...content: Block[]): Message => ({ role: "user", content })
export const assistant = (...content: Block[]): Message => ({ role: "assistant", content })
export const text = (value: string): Block => ({ type: "text", text: value })
export const result = (id: string, content: string): Block => ({ type: "tool_result", tool_use_id: id, content })

let operations = 0

export function request(
  id: string,
  messages: Message[],
  extra: Partial<StepRequest["invocation"]> = {},
  tools: Tool[] = [READ],
): StepRequest {
  return {
    model: { id: "sonnet" },
    system: "Be brief.",
    messages,
    tools,
    thinking: { kind: "adaptive" },
    invocation: { conversation_id: id, operation_id: `op-${++operations}`, attempt: 0, purpose: "agent", ...extra },
  }
}
