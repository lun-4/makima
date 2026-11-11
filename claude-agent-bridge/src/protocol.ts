// What the Lua provider posts and receives. Field names follow Makima's provider
// API (`register_provider` in makima-lua/src/api/provider.rs).

export const PROTOCOL_VERSION = 1

export type Block =
  | { type: "text"; text: string }
  | { type: "thinking"; thinking: string; signature?: string }
  | { type: "redacted_thinking"; data: string }
  | { type: "tool_use"; id: string; name: string; input: unknown }
  | { type: "tool_result"; tool_use_id: string; content: string; is_error?: boolean }
  | { type: "image"; source: { media_type: string; data: string } }

export interface Message {
  role: "user" | "assistant"
  content: Block[]
}

export interface Tool {
  name: string
  description: string
  input_schema: unknown
}

export type Thinking = { kind: "off" } | { kind: "adaptive" } | { kind: "effort"; level: string } | { kind: "budget"; tokens: number }

export type Relation = "continues" | "branches"

export interface Previous {
  id: string
  relation: Relation
  shared: number
}

export interface Invocation {
  conversation_id: string
  operation_id: string
  attempt: number
  purpose: "agent" | "compaction" | "isolated"
  agent_id?: string
  previous?: Previous
}

export interface StepRequest {
  model: { id: string; max_output_tokens?: number; context_window?: number }
  system: string
  messages: Message[]
  tools: Tool[]
  thinking: Thinking
  fast?: boolean
  session_id?: string
  invocation: Invocation
}

export type Event =
  | { type: "text_delta"; text: string }
  | { type: "thinking_delta"; text: string }
  | { type: "thinking_block_end" }
  | { type: "tool_use_start"; id: string; name: string }

export interface Usage {
  input_tokens: number
  output_tokens: number
  cache_creation_input_tokens: number
  cache_read_input_tokens: number
}

export type StopReason = "end_turn" | "tool_use" | "max_tokens"

export interface StepResult {
  message: { role: "assistant"; content: Block[] }
  usage: Usage
  stop_reason: StopReason
}

export type Retry = "rate_limit" | "transient" | "connect" | "timeout" | "none"

export type EndReason = "cancelled" | "closed" | "compacted" | "rewound"
export const END_REASONS: readonly EndReason[] = ["cancelled", "closed", "compacted", "rewound"]

// A Makima provider error table. `httpStatus` is only how the bridge answers
// before the event stream starts; it never reaches Makima.
export interface ErrorTable {
  message: string
  kind?: "config"
  status?: number
  overflow?: boolean
  retry?: Retry
}

export class BridgeError extends Error {
  readonly table: ErrorTable
  readonly httpStatus: number

  constructor(table: ErrorTable, httpStatus = 500) {
    super(table.message)
    this.table = table
    this.httpStatus = httpStatus
  }
}

export function invalid(message: string): BridgeError {
  return new BridgeError({ message, retry: "none" }, 400)
}

export function conflict(message: string): BridgeError {
  return new BridgeError({ message, retry: "none" }, 409)
}

export function configError(message: string): BridgeError {
  return new BridgeError({ message, kind: "config" }, 503)
}

export function failure(message: string): BridgeError {
  return new BridgeError({ message, retry: "none" })
}

export function toErrorTable(error: unknown): ErrorTable {
  if (error instanceof BridgeError) return error.table
  return { message: error instanceof Error ? error.message : String(error), retry: "none" }
}

// Lua encodes an empty table as `{}`, so an empty list may arrive as an object.
function list<T>(value: unknown, field: string): T[] {
  if (Array.isArray(value)) return value as T[]
  if (value !== null && typeof value === "object" && Object.keys(value).length === 0) return []
  throw invalid(`${field} must be a list`)
}

function text(value: unknown, field: string): string {
  if (typeof value !== "string" || value === "") throw invalid(`${field} must be a non-empty string`)
  return value
}

function block(value: unknown): Block {
  const raw = (value ?? {}) as Record<string, unknown>
  switch (raw.type) {
    case "text":
      return { type: "text", text: String(raw.text ?? "") }
    case "thinking":
      return { type: "thinking", thinking: String(raw.thinking ?? "") }
    case "redacted_thinking":
      return { type: "redacted_thinking", data: String(raw.data ?? "") }
    case "tool_use":
      return { type: "tool_use", id: text(raw.id, "tool_use.id"), name: text(raw.name, "tool_use.name"), input: raw.input ?? {} }
    case "tool_result":
      return {
        type: "tool_result",
        tool_use_id: text(raw.tool_use_id, "tool_result.tool_use_id"),
        content: String(raw.content ?? ""),
        is_error: raw.is_error === true,
      }
    case "image": {
      const source = (raw.source ?? {}) as Record<string, unknown>
      return { type: "image", source: { media_type: text(source.media_type, "image.media_type"), data: text(source.data, "image.data") } }
    }
    default:
      throw invalid(`unsupported content block ${JSON.stringify(raw.type)}`)
  }
}

function message(value: unknown): Message {
  const raw = (value ?? {}) as Record<string, unknown>
  if (raw.role !== "user" && raw.role !== "assistant") throw invalid("message role must be user or assistant")
  return { role: raw.role, content: list(raw.content, "message.content").map(block) }
}

function previous(value: unknown): Previous | undefined {
  if (value === undefined || value === null) return undefined
  const raw = value as Record<string, unknown>
  if (raw.relation !== "continues" && raw.relation !== "branches") throw invalid("previous.relation must be continues or branches")
  if (!Number.isSafeInteger(raw.shared) || (raw.shared as number) < 0) throw invalid("previous.shared must be a count")
  return { id: text(raw.id, "previous.id"), relation: raw.relation, shared: raw.shared as number }
}

function thinking(value: unknown): Thinking {
  const raw = (value ?? { kind: "adaptive" }) as Record<string, unknown>
  switch (raw.kind) {
    case "off":
    case "adaptive":
      return { kind: raw.kind }
    case "effort":
      return { kind: "effort", level: text(raw.level, "thinking.level") }
    case "budget":
      if (!Number.isSafeInteger(raw.tokens)) throw invalid("thinking.tokens must be a count")
      return { kind: "budget", tokens: raw.tokens as number }
    default:
      throw invalid(`unsupported thinking kind ${JSON.stringify(raw.kind)}`)
  }
}

export function parseStep(body: unknown): StepRequest {
  const envelope = (body ?? {}) as Record<string, unknown>
  if (envelope.protocol_version !== PROTOCOL_VERSION) {
    throw invalid(`unsupported protocol version ${JSON.stringify(envelope.protocol_version)}; this bridge speaks ${PROTOCOL_VERSION}`)
  }
  const raw = (envelope.request ?? {}) as Record<string, unknown>
  const model = (raw.model ?? {}) as Record<string, unknown>
  const invocation = (raw.invocation ?? {}) as Record<string, unknown>
  const purpose = invocation.purpose
  if (purpose !== "agent" && purpose !== "compaction" && purpose !== "isolated") throw invalid("invocation.purpose is unknown")
  const messages = list(raw.messages, "messages").map(message)
  if (messages.at(-1)?.role !== "user") throw invalid("the last message must be from the user")
  return {
    model: {
      id: text(model.id, "model.id"),
      max_output_tokens: typeof model.max_output_tokens === "number" ? model.max_output_tokens : undefined,
      context_window: typeof model.context_window === "number" ? model.context_window : undefined,
    },
    system: typeof raw.system === "string" ? raw.system : "",
    messages,
    tools: list<Record<string, unknown>>(raw.tools, "tools").map((tool) => ({
      name: text(tool.name, "tool.name"),
      description: typeof tool.description === "string" ? tool.description : "",
      input_schema: tool.input_schema ?? { type: "object" },
    })),
    thinking: thinking(raw.thinking),
    fast: raw.fast === true,
    session_id: typeof raw.session_id === "string" ? raw.session_id : undefined,
    invocation: {
      conversation_id: text(invocation.conversation_id, "invocation.conversation_id"),
      operation_id: text(invocation.operation_id, "invocation.operation_id"),
      attempt: Number.isSafeInteger(invocation.attempt) ? (invocation.attempt as number) : 0,
      purpose,
      agent_id: typeof invocation.agent_id === "string" ? invocation.agent_id : undefined,
      previous: previous(invocation.previous),
    },
  }
}
