import { createHash } from "node:crypto"
import { Server } from "@modelcontextprotocol/sdk/server/index.js"
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js"
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js"
import type { HookCallback, McpSdkServerConfigWithInstance, Options } from "@anthropic-ai/claude-agent-sdk"
import type { Tool } from "./protocol.ts"

export const SERVER_NAME = "maki"
export const PREFIX = `mcp__${SERVER_NAME}__`
const NAME_LIMIT = 64
const HASH_LENGTH = 10
// Claude Code cuts each MCP tool description at this many characters.
const DESCRIPTION_LIMIT = 2048
const MAX_RESULT_CHARS = 500_000
export const CANCELLED_TEXT = "Cancelled by the user."

// The MCP name Claude sees for {name}. Names that do not fit MCP's limits are
// cut and get a hash, so two long names never collide.
export function shortName(name: string): string {
  const limit = NAME_LIMIT - PREFIX.length
  if (name.length <= limit && /^[a-zA-Z0-9_-]+$/.test(name)) return name
  const hash = createHash("sha256").update(name).digest("hex").slice(0, HASH_LENGTH)
  return `${name.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, limit - HASH_LENGTH - 1)}_${hash}`
}

// The Maki name of a tool Claude called, or undefined for one Maki did not offer.
export function originalName(claudeName: string, tools: readonly Tool[]): string | undefined {
  return tools.find((tool) => PREFIX + shortName(tool.name) === claudeName)?.name
}

interface Call {
  readonly name: string
  readonly result: PromiseWithResolvers<CallToolResult>
  resolved: boolean
}

// MCP handlers waiting for Maki's results. The handler cannot see Claude's
// tool-use id, so the PreToolUse hook names it just before the call. Tools
// are marked not read-only, so Claude Code runs them one at a time and the
// hook and handler always pair up.
export class PendingTools {
  private readonly calls = new Map<string, Call>()
  private next: string | undefined

  expect(id: string, name: string): Call {
    const saved = this.calls.get(id)
    if (saved) {
      if (saved.name !== name) throw new Error(`tool call ${id} changed its tool`)
      return saved
    }
    const call = { name, result: Promise.withResolvers<CallToolResult>(), resolved: false }
    this.calls.set(id, call)
    return call
  }

  before(id: string, name: string): void {
    this.expect(id, name)
    this.next = id
  }

  async call(name: string): Promise<CallToolResult> {
    const id = this.next
    this.next = undefined
    if (!id) throw new Error("Claude called a Maki tool without a PreToolUse id")
    const result = await this.expect(id, name).result.promise
    this.calls.delete(id)
    return result
  }

  resolve(id: string, result: CallToolResult): void {
    const call = this.calls.get(id)
    if (!call || call.resolved) throw new Error(`no waiting tool call ${id}`)
    call.resolved = true
    call.result.resolve(result)
  }

  // Only after an interrupt has drained the turn: resolving a handler earlier
  // lets Claude make another model call.
  clear(): void {
    for (const call of this.calls.values()) {
      if (!call.resolved) call.result.resolve({ isError: true, content: [{ type: "text", text: CANCELLED_TEXT }] })
    }
    this.calls.clear()
    this.next = undefined
  }
}

export function hooks(pending: PendingTools): Options["hooks"] {
  const before: HookCallback = async (input) => {
    if (input.hook_event_name === "PreToolUse" && input.tool_name.startsWith(PREFIX)) {
      pending.before(input.tool_use_id, input.tool_name)
    }
    return {}
  }
  return { PreToolUse: [{ hooks: [before] }] }
}

export function server(tools: readonly Tool[], pending: PendingTools): McpSdkServerConfigWithInstance {
  const mcp = new Server({ name: SERVER_NAME, version: "1.0.0" }, { capabilities: { tools: {} } })
  const definitions = tools.map((tool) => ({
    name: shortName(tool.name),
    description: tool.description.slice(0, DESCRIPTION_LIMIT),
    inputSchema: tool.input_schema as { type: "object" },
    annotations: { readOnlyHint: false },
    _meta: { "anthropic/alwaysLoad": true, "anthropic/maxResultSizeChars": MAX_RESULT_CHARS },
  }))
  mcp.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: definitions }))
  mcp.setRequestHandler(CallToolRequestSchema, async (request) => {
    const name = PREFIX + request.params.name
    if (!originalName(name, tools)) throw new Error(`Claude called a tool Maki did not offer: ${name}`)
    return pending.call(name)
  })
  // The SDK only calls `connect` on the instance, which the low-level server
  // has; it takes raw JSON schemas where `McpServer` wants zod.
  return { type: "sdk", name: SERVER_NAME, instance: mcp as unknown as McpSdkServerConfigWithInstance["instance"] }
}
