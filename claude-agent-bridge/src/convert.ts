import type { SDKAssistantMessageError, SDKMessage } from "@anthropic-ai/claude-agent-sdk"
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js"
import { BridgeError, invalid } from "./protocol.ts"
import type { Block, ErrorTable, Event, Message, StepResult, StopReason, Tool, Usage } from "./protocol.ts"
import { originalName } from "./tools.ts"

export type ClaudeBlock = { type: "text"; text: string } | { type: "image"; source: { type: "base64"; media_type: string; data: string } }

const CHECKPOINT_OPEN =
  "<conversation-checkpoint>\nThe following is conversation history from Makima that you have not seen. Treat it as prior conversation, not a new request. Continue with the user messages after this checkpoint."
const CHECKPOINT_CLOSE = "</conversation-checkpoint>"
const RESUME_TEXT = "Continue from where the checkpoint ends."
const STOP_REASONS: Record<string, StopReason> = { end_turn: "end_turn", tool_use: "tool_use", max_tokens: "max_tokens" }
const OVERFLOW_PATTERNS = [/prompt is too long/i, /context (window|length)/i, /exceeds? the maximum/i]
const CONFIG_ERRORS: readonly SDKAssistantMessageError[] = [
  "authentication_failed",
  "oauth_org_not_allowed",
  "account_on_hold",
  "verification_required",
  "billing_error",
  "cloud_credential_error",
  "model_not_found",
]
const LOGIN_HINT = " Run `claude auth login` in a terminal."

function image(block: Extract<Block, { type: "image" }>): ClaudeBlock {
  return { type: "image", source: { type: "base64", media_type: block.source.media_type, data: block.source.data } }
}

// {block} as Claude content. Thinking is Claude's own and never replayed;
// tool calls and results Claude did not make become tagged text.
function convertBlock(block: Block): ClaudeBlock | undefined {
  switch (block.type) {
    case "text":
      return block.text === "" ? undefined : { type: "text", text: block.text }
    case "image":
      return image(block)
    case "tool_use":
      return { type: "text", text: `<tool-call>\n${JSON.stringify({ id: block.id, name: block.name, input: block.input })}\n</tool-call>` }
    case "tool_result": {
      const result = { id: block.tool_use_id, is_error: block.is_error || undefined, content: block.content }
      return { type: "text", text: `<tool-result>\n${JSON.stringify(result)}\n</tool-result>` }
    }
    case "thinking":
    case "redacted_thinking":
      return undefined
  }
}

function convertAll(blocks: readonly Block[]): ClaudeBlock[] {
  return blocks.flatMap((block) => convertBlock(block) ?? [])
}

function isHistory(message: Message): boolean {
  return message.role === "assistant" || message.content.some((block) => block.type === "tool_result")
}

// Claude content for {messages} it has not seen. Claude cannot be handed
// earlier turns, so everything up to the last reply or tool result goes in
// as a tagged checkpoint, and the user messages after it as they are.
export function catchUp(messages: readonly Message[]): ClaudeBlock[] {
  const end = messages.findLastIndex(isHistory) + 1
  const blocks: ClaudeBlock[] = []
  if (end > 0) {
    blocks.push({ type: "text", text: CHECKPOINT_OPEN })
    for (const message of messages.slice(0, end)) {
      blocks.push({ type: "text", text: `<${message.role}>` }, ...convertAll(message.content), { type: "text", text: `</${message.role}>` })
    }
    blocks.push({ type: "text", text: CHECKPOINT_CLOSE })
  }
  for (const message of messages.slice(end)) blocks.push(...convertAll(message.content))
  if (end > 0 && end === messages.length) blocks.push({ type: "text", text: RESUME_TEXT })
  return blocks
}

export interface Continuation {
  readonly results: ReadonlyMap<string, CallToolResult>
  // Content after the results, for Claude to read once the tools return.
  readonly extra: ClaudeBlock[]
}

// Matches the tool results in {message}, and the {later} messages, to the
// calls Claude is waiting on. Images that follow a result join it. Fails
// on a missing, duplicate or unknown result before anything is resolved.
export function continuation(message: Message | undefined, later: readonly Message[], waiting: readonly string[]): Continuation {
  if (message?.role !== "user") throw invalid("Claude is waiting for tool results, but the next message is not from the user")
  const results = new Map<string, CallToolResult>()
  const extra: ClaudeBlock[] = []
  let current: CallToolResult | undefined
  for (const block of message.content) {
    if (block.type === "tool_result") {
      if (results.has(block.tool_use_id)) throw invalid(`duplicate result for tool call ${block.tool_use_id}`)
      if (!waiting.includes(block.tool_use_id)) throw invalid(`result for unknown tool call ${block.tool_use_id}`)
      current = { isError: block.is_error === true, content: [{ type: "text", text: block.content }] }
      results.set(block.tool_use_id, current)
    } else if (block.type === "image" && current) {
      current.content.push({ type: "image", mimeType: block.source.media_type, data: block.source.data })
    } else {
      current = undefined
      const converted = convertBlock(block)
      if (converted) extra.push(converted)
    }
  }
  const missing = waiting.filter((id) => !results.has(id))
  if (missing.length > 0) throw invalid(`missing results for tool calls ${missing.join(", ")}`)
  if (later.length > 0) extra.push(...catchUp(later))
  return { results, extra }
}

interface Counts {
  input: number
  output: number
  write: number
  read: number
}

interface RawUsage {
  input_tokens?: number | null
  output_tokens?: number | null
  cache_creation_input_tokens?: number | null
  cache_read_input_tokens?: number | null
}

// Claude Code reports a response's usage several times while it streams, so
// each count keeps its highest value per response. Context is the last
// response's input; output adds up over the step's responses.
export class UsageTracker {
  private readonly responses = new Map<string, Counts>()
  private last: string | undefined

  update(id: string, usage: RawUsage | undefined): void {
    if (!usage) return
    let counts = this.responses.get(id)
    if (!counts) {
      counts = { input: 0, output: 0, write: 0, read: 0 }
      this.responses.set(id, counts)
      this.last = id
    }
    counts.input = Math.max(counts.input, usage.input_tokens ?? 0)
    counts.output = Math.max(counts.output, usage.output_tokens ?? 0)
    counts.write = Math.max(counts.write, usage.cache_creation_input_tokens ?? 0)
    counts.read = Math.max(counts.read, usage.cache_read_input_tokens ?? 0)
  }

  // A turn's result sums every model call of the SDK turn, which spans all
  // the Makima steps between tool calls and re-reads the context in each. It
  // stands in only when the step saw no usage and the turn made one call.
  turnTotal(usage: RawUsage, calls: number): void {
    if (this.responses.size === 0 && calls === 1) this.update("turn", usage)
  }

  snapshot(): Usage {
    const last = this.last === undefined ? undefined : this.responses.get(this.last)
    let output = 0
    for (const counts of this.responses.values()) output += counts.output
    return {
      input_tokens: last?.input ?? 0,
      output_tokens: output,
      cache_creation_input_tokens: last?.write ?? 0,
      cache_read_input_tokens: last?.read ?? 0,
    }
  }
}

type StreamEvent = Extract<SDKMessage, { type: "stream_event" }>["event"]

// Turns one step's SDK messages into Makima events, the reply and its usage.
export class StepTranslator {
  private readonly tools: readonly Tool[]
  private readonly usage = new UsageTracker()
  private readonly streamed = new Set<string>()
  private readonly thinkingBlocks = new Set<number>()
  private readonly content: Block[] = []
  private responseId = ""
  private errorCode: SDKAssistantMessageError | undefined
  private rateLimitResetsAt: number | undefined
  readonly toolCalls: { id: string; claudeName: string }[] = []
  lastAssistantUuid: string | undefined
  sessionId: string | undefined

  constructor(tools: readonly Tool[]) {
    this.tools = tools
  }

  // The Makima events for {message}. Messages from Claude Code's own
  // subagents (a parent tool-use id) are not part of the step.
  translate(message: SDKMessage): Event[] {
    if ("session_id" in message && typeof message.session_id === "string") this.sessionId = message.session_id
    if (message.type === "rate_limit_event") {
      const info = message.rate_limit_info
      this.rateLimitResetsAt = info.status === "rejected" ? info.resetsAt : undefined
      return []
    }
    if (message.type === "stream_event" && message.parent_tool_use_id === null) return this.streamEvent(message.event)
    if (message.type === "assistant" && message.parent_tool_use_id === null) return this.assistant(message)
    if (message.type === "result") this.usage.turnTotal(message.usage, message.num_turns)
    return []
  }

  private streamEvent(event: StreamEvent): Event[] {
    switch (event.type) {
      case "message_start":
        this.responseId = event.message.id
        this.streamed.add(this.responseId)
        this.usage.update(this.responseId, event.message.usage)
        return []
      case "message_delta":
        this.usage.update(this.responseId, event.usage)
        return []
      case "content_block_start": {
        const block = event.content_block
        if (block.type === "thinking") this.thinkingBlocks.add(event.index)
        if (block.type !== "tool_use") return []
        const name = originalName(block.name, this.tools)
        return name ? [{ type: "tool_use_start", id: block.id, name }] : []
      }
      case "content_block_delta": {
        const delta = event.delta
        if (delta.type === "text_delta" && delta.text) return [{ type: "text_delta", text: delta.text }]
        if (delta.type === "thinking_delta" && delta.thinking) return [{ type: "thinking_delta", text: delta.thinking }]
        return []
      }
      case "content_block_stop":
        if (!this.thinkingBlocks.delete(event.index)) return []
        return [{ type: "thinking_block_end" }]
      default:
        return []
    }
  }

  private assistant(message: Extract<SDKMessage, { type: "assistant" }>): Event[] {
    if (message.error) {
      this.errorCode = message.error
      return []
    }
    this.lastAssistantUuid = message.uuid
    this.usage.update(message.message.id, message.message.usage)
    // Without stream events for this response, its full message is all there is.
    const events: Event[] = []
    const unstreamed = !this.streamed.has(message.message.id)
    for (const block of message.message.content) {
      if (block.type === "text") {
        this.content.push({ type: "text", text: block.text })
        if (unstreamed && block.text) events.push({ type: "text_delta", text: block.text })
      } else if (block.type === "thinking") {
        this.content.push({ type: "thinking", thinking: block.thinking, signature: block.signature })
        if (unstreamed && block.thinking) events.push({ type: "thinking_delta", text: block.thinking }, { type: "thinking_block_end" })
      } else if (block.type === "redacted_thinking") {
        this.content.push({ type: "redacted_thinking", data: block.data })
      } else if (block.type === "tool_use") {
        const name = originalName(block.name, this.tools)
        if (!name) continue
        this.content.push({ type: "tool_use", id: block.id, name, input: block.input })
        this.toolCalls.push({ id: block.id, claudeName: block.name })
        if (unstreamed) events.push({ type: "tool_use_start", id: block.id, name })
      }
    }
    return events
  }

  // Whether {message} ends a response that called a Makima tool, which ends
  // the Makima step while the SDK turn waits for the results.
  endsToolStep(message: SDKMessage): boolean {
    return (
      this.toolCalls.length > 0 &&
      message.type === "stream_event" &&
      message.parent_tool_use_id === null &&
      message.event.type === "message_stop"
    )
  }

  result(stopReason: StopReason): StepResult {
    return { message: { role: "assistant", content: this.content }, usage: this.usage.snapshot(), stop_reason: stopReason }
  }

  // The step's result for the turn's final {message}, or the Makima error it ends with.
  finish(message: Extract<SDKMessage, { type: "result" }>): StepResult {
    const status = message.subtype === "success" ? (message.api_error_status ?? undefined) : undefined
    const failed = message.subtype !== "success" || message.is_error || (status !== undefined && status >= 400)
    if (!failed) return this.result(STOP_REASONS[message.stop_reason ?? ""] ?? "end_turn")
    const detail = message.subtype === "success" ? message.result : message.errors.join("\n")
    throw new BridgeError(this.error(detail, status))
  }

  // Claude Code has already retried by the time a turn fails, and a second
  // attempt could repeat a step, so nothing here is retried.
  private error(detail: string, status: number | undefined): ErrorTable {
    const code = this.errorCode
    if (code && CONFIG_ERRORS.includes(code)) {
      const hint = code === "authentication_failed" ? LOGIN_HINT : ""
      return { kind: "config", message: `Claude Code: ${detail || code}.${hint}` }
    }
    if (OVERFLOW_PATTERNS.some((pattern) => pattern.test(detail))) {
      return { status: status ?? 400, message: detail, overflow: true, retry: "none" }
    }
    let message = detail || (status ? `Claude Code could not finish its reply (HTTP ${status})` : "Claude Code could not finish its reply")
    if (code === "rate_limit" && this.rateLimitResetsAt) {
      message += ` Resets at ${new Date(this.rateLimitResetsAt * 1000).toISOString()}.`
    }
    const rateLimited = code === "rate_limit" ? 429 : undefined
    return { status: status ?? rateLimited, message, retry: "none" }
  }
}
