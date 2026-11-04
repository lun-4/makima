import { createHash, randomUUID } from "node:crypto"
import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk"
import { catchUp, continuation, StepTranslator } from "./convert.ts"
import type { ClaudeBlock, Continuation } from "./convert.ts"
import { effort, launchKey, sessionOptions } from "./options.ts"
import type { Environment } from "./options.ts"
import { BridgeError, configError, conflict, failure } from "./protocol.ts"
import type { EndReason, Event, Message, StepRequest, StepResult } from "./protocol.ts"
import { forkPoint, freshRecord } from "./records.ts"
import type { Record, Records } from "./records.ts"
import type { Effort, Probe, Session, StartSession } from "./session.ts"
import { PendingTools } from "./tools.ts"

export interface Limits {
  // A process stops after this long without a step, unless tools are waiting.
  readonly idleMs: number
  // A conversation whose tools nobody answers is stopped after this long.
  readonly orphanMs: number
  // How long a cancelled or rewound conversation waits for a successor.
  readonly graceMs: number
  readonly readyMs: number
  readonly maxProcesses: number
}

export const DEFAULT_LIMITS: Limits = {
  idleMs: 5 * 60_000,
  orphanMs: 60 * 60_000,
  graceMs: 10 * 60_000,
  readyMs: 30_000,
  maxProcesses: 8,
}

const MAX_OPERATIONS = 10_000
const TOOL_SERVER_MISSING = "The Maki tool server did not connect to Claude Code"
const NOT_LOGGED_IN = "Claude Code is not logged in. Run `claude auth login` in a terminal."

// Where Maki's step goes while it runs.
export interface StepSink {
  // The step is accepted; later failures arrive as the stream's error frame.
  open(): void
  event(event: Event): void
}

interface Live {
  readonly session: Session
  readonly pending: PendingTools
  readonly launch: string
  model: string
  effort: Effort | undefined
}

interface Conversation {
  readonly id: string
  readonly persist: boolean
  record: Record
  live?: Live
  step?: Promise<unknown>
  timer?: NodeJS.Timeout
  lastUsed: number
}

type Plan = { content: ClaudeBlock[] } | { continuation: Continuation }

// Claude Code reports a token source only in some releases; an account email
// means a login, and a cloud provider brings its own credentials.
export function isLoggedIn(probe: Probe): boolean {
  const account = probe.account
  if (account.tokenSource !== undefined && account.tokenSource !== "none") return true
  if (account.apiProvider !== undefined && account.apiProvider !== "firstParty") return true
  return account.apiKeySource !== undefined || account.email !== undefined
}

function launchHash(request: StepRequest): string {
  return createHash("sha256").update(launchKey(request)).digest("hex")
}

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

// The messages after {record.seen} that Claude has not seen. After an
// interrupt Claude's transcript already holds its partial reply and the
// cancelled tool results, so Maki's copies of them are left out.
export function unseen(record: Record, messages: readonly Message[]): Message[] {
  let rest = messages.slice(record.seen)
  const interrupted = record.interrupted
  if (!interrupted) return rest
  const partial = rest[0]
  if (partial?.role === "assistant" && !partial.content.some((block) => block.type === "tool_use")) rest = rest.slice(1)
  const cancelled = new Set(interrupted.tools)
  return rest
    .map((message) => ({
      ...message,
      content: message.content.filter((block) => block.type !== "tool_result" || !cancelled.has(block.tool_use_id)),
    }))
    .filter((message) => message.content.length > 0)
}

// A conversation that starts again from the last reply Claude finished, for
// when its transcript ends at a tool call nothing will answer.
function restartedRecord(record: Record): Record {
  const point = record.boundaries.at(-1)
  if (!point) return freshRecord()
  return { fork: { session: point.session, at: point.uuid }, seen: point.count, boundaries: [...record.boundaries] }
}

export class Conversations {
  private readonly environment: Environment
  private readonly start: StartSession
  private readonly records: Records
  private readonly limits: Limits
  private readonly active = new Map<string, Conversation>()
  private readonly operations = new Set<string>()

  constructor(environment: Environment, start: StartSession, records: Records, limits: Limits = DEFAULT_LIMITS) {
    this.environment = environment
    this.start = start
    this.records = records
    this.limits = limits
  }

  get size(): number {
    return this.active.size
  }

  live(id: string): boolean {
    return this.active.get(id)?.live !== undefined
  }

  // Runs one Maki step. Fails before `sink.open()` for a request the bridge
  // refuses, and after it for a step that went wrong.
  async step(request: StepRequest, sink: StepSink, signal: AbortSignal): Promise<StepResult> {
    const { conversation_id: id, operation_id, attempt } = request.invocation
    const operation = `${operation_id}/${attempt}`
    if (this.operations.has(operation)) throw conflict(`step ${operation} was already started`)
    let conversation = this.active.get(id)
    if (conversation?.step) throw conflict(`conversation ${id} is already running a step`)
    this.remember(operation)
    const adopting = !conversation
    if (!conversation) {
      conversation = { id, persist: request.invocation.purpose === "agent", record: freshRecord(), lastUsed: Date.now() }
      this.active.set(id, conversation)
    }
    const current = conversation
    clearTimeout(current.timer)
    const step = (async () => {
      if (adopting) await this.adopt(current, request)
      return this.run(current, request, sink, signal)
    })()
    current.step = step
    try {
      return await step
    } finally {
      current.step = undefined
      current.lastUsed = Date.now()
      this.arm(current)
    }
  }

  async end(id: string, reason: EndReason): Promise<void> {
    const conversation = this.active.get(id)
    if (!conversation) {
      if (reason === "compacted" || reason === "closed") await this.records.remove(id)
      return
    }
    await conversation.step?.catch(() => {})
    if (reason === "compacted" || reason === "closed") return this.discard(conversation)
    await this.settle(conversation)
    await this.save(conversation)
    clearTimeout(conversation.timer)
    conversation.timer = setTimeout(() => void this.discard(conversation), this.limits.graceMs)
    conversation.timer.unref()
  }

  async close(): Promise<void> {
    await Promise.allSettled([...this.active.values()].map((conversation) => this.stopLive(conversation)))
    for (const conversation of this.active.values()) clearTimeout(conversation.timer)
    this.active.clear()
  }

  private remember(operation: string): void {
    this.operations.add(operation)
    if (this.operations.size <= MAX_OPERATIONS) return
    const oldest = this.operations.values().next().value
    if (oldest !== undefined) this.operations.delete(oldest)
  }

  // Sets up a conversation the bridge has not seen: one it saved before a
  // restart, one that takes over its predecessor's SDK session, one forked
  // from its source, or a fresh one.
  private async adopt(conversation: Conversation, request: StepRequest): Promise<void> {
    const saved = await this.records.load(conversation.id)
    if (saved) {
      conversation.record = saved
      return
    }
    const previous = request.invocation.previous
    if (!previous) return
    const source = this.active.get(previous.id)
    // `end_conversation` is detached, so the predecessor may still be draining.
    await source?.step?.catch(() => {})
    const record = source?.record ?? (await this.records.load(previous.id))
    if (!record) return
    if (previous.relation === "continues" && previous.shared >= record.seen) {
      if (source) {
        await this.settle(source)
        clearTimeout(source.timer)
        conversation.live = source.live
        source.live = undefined
        this.active.delete(previous.id)
      }
      conversation.record = record
      await this.records.remove(previous.id)
      return
    }
    const point = forkPoint(record, previous.shared)
    // A different tool set needs a new process, and a forked transcript would
    // hold calls to tools the new one does not offer.
    if (!point || record.launch !== launchHash(request)) return
    conversation.record = {
      fork: { session: point.session, at: point.uuid },
      seen: point.count,
      boundaries: record.boundaries.filter((boundary) => boundary.count <= point.count),
    }
  }

  private async run(conversation: Conversation, request: StepRequest, sink: StepSink, signal: AbortSignal): Promise<StepResult> {
    const messages = request.messages
    const launch = launchKey(request)
    const live = conversation.live
    if (conversation.record.waiting && (!live || live.session.ended || live.launch !== launch)) {
      await this.stopLive(conversation)
      conversation.record = restartedRecord(conversation.record)
    }
    if (messages.length < conversation.record.seen) {
      await this.stopLive(conversation)
      conversation.record = freshRecord()
    }
    const plan = await this.plan(conversation, messages)
    sink.open()
    const opened = await this.open(conversation, request, launch)
    const translator = new StepTranslator(request.tools)
    const record = conversation.record
    if ("continuation" in plan) {
      const { results, extra } = plan.continuation
      if (extra.length > 0) opened.session.addUser(extra, randomUUID())
      for (const [id, result] of results) opened.pending.resolve(id, result)
      record.waiting = undefined
    } else {
      opened.session.begin(plan.content, randomUUID())
    }
    record.seen = messages.length
    record.interrupted = undefined
    record.launch = launchHash(request)
    try {
      const end = await opened.session.readStep((message) => {
        this.checkToolServer(message, request)
        for (const event of translator.translate(message)) sink.event(event)
        return translator.endsToolStep(message)
      }, signal)
      this.noteSession(conversation, translator)
      if (end === "tools") {
        for (const call of translator.toolCalls) opened.pending.expect(call.id, call.claudeName)
        record.waiting = translator.toolCalls.map((call) => call.id)
        record.seen += 1
        return translator.result("tool_use")
      }
      const result = translator.finish(end)
      record.seen += 1
      const session = record.session
      if (conversation.persist && session && translator.lastAssistantUuid) {
        record.boundaries.push({ count: record.seen, session, uuid: translator.lastAssistantUuid })
      }
      return result
    } catch (error) {
      this.noteSession(conversation, translator)
      if (opened.session.running) {
        await opened.session.interrupt().catch(() => {})
        opened.pending.clear()
        record.interrupted = { tools: [] }
      }
      throw error
    } finally {
      await this.save(conversation)
    }
  }

  private async plan(conversation: Conversation, messages: readonly Message[]): Promise<Plan> {
    const record = conversation.record
    if (!record.waiting) return { content: catchUp(unseen(record, messages)) }
    try {
      return { continuation: continuation(messages[record.seen], messages.slice(record.seen + 1), record.waiting) }
    } catch (error) {
      // This step fails; the next one starts again from Claude's last finished reply.
      await this.stopLive(conversation)
      conversation.record = restartedRecord(record)
      await this.save(conversation)
      throw error
    }
  }

  private checkToolServer(message: SDKMessage, request: StepRequest): void {
    if (message.type !== "system" || message.subtype !== "init" || request.tools.length === 0) return
    const connected = message.mcp_servers.some((server) => server.name === "maki" && server.status === "connected")
    if (!connected) throw failure(TOOL_SERVER_MISSING)
  }

  private noteSession(conversation: Conversation, translator: StepTranslator): void {
    const id = translator.sessionId
    if (!id || conversation.record.session === id) return
    conversation.record.session = id
    conversation.record.fork = undefined
  }

  private async open(conversation: Conversation, request: StepRequest, launch: string): Promise<Live> {
    const current = conversation.live
    if (current && (current.session.ended || current.launch !== launch)) await this.stopLive(conversation)
    let live = conversation.live
    if (!live) {
      await this.makeRoom()
      const pending = new PendingTools()
      const options = sessionOptions(this.environment, request, conversation.record, pending, conversation.persist)
      live = { session: this.start(options), pending, launch, model: request.model.id, effort: effort(request) }
      conversation.live = live
      let probe: Probe
      try {
        probe = await withTimeout(
          live.session.ready,
          this.limits.readyMs,
          `claude did not start within ${this.limits.readyMs / 1000} seconds`,
        )
      } catch (error) {
        await this.stopLive(conversation)
        throw configError(`Claude Code could not start: ${error instanceof Error ? error.message : String(error)}`)
      }
      if (!isLoggedIn(probe)) {
        await this.stopLive(conversation)
        throw configError(NOT_LOGGED_IN)
      }
    }
    if (live.model !== request.model.id) {
      await live.session.setModel(request.model.id)
      live.model = request.model.id
    }
    const wanted = effort(request)
    if (live.effort !== wanted) {
      await live.session.setEffort(wanted)
      live.effort = wanted
    }
    return live
  }

  // Stops the least recently used process that has nothing running.
  private async makeRoom(): Promise<void> {
    const running = [...this.active.values()].filter((conversation) => conversation.live)
    if (running.length < this.limits.maxProcesses) return
    const idle = running
      .filter((conversation) => !conversation.step && !conversation.record.waiting)
      .sort((a, b) => a.lastUsed - b.lastUsed)[0]
    if (!idle) throw new BridgeError({ message: `all ${this.limits.maxProcesses} Claude Code processes are busy`, retry: "none" }, 503)
    await this.stopLive(idle)
  }

  // Interrupts a running turn, including one waiting for tool results.
  private async settle(conversation: Conversation): Promise<void> {
    const live = conversation.live
    const record = conversation.record
    if (live?.session.running) {
      await live.session.interrupt()
      live.pending.clear()
      record.interrupted = { tools: record.waiting ?? [] }
      record.waiting = undefined
    } else if (record.waiting) {
      conversation.record = restartedRecord(record)
    }
  }

  private async stopLive(conversation: Conversation): Promise<void> {
    const live = conversation.live
    if (!live) return
    conversation.live = undefined
    try {
      await live.session.stop()
    } finally {
      live.pending.clear()
    }
  }

  private async discard(conversation: Conversation): Promise<void> {
    clearTimeout(conversation.timer)
    if (this.active.get(conversation.id) === conversation) this.active.delete(conversation.id)
    await this.stopLive(conversation)
    await this.records.remove(conversation.id)
  }

  private async save(conversation: Conversation): Promise<void> {
    if (conversation.persist) await this.records.save(conversation.id, conversation.record)
  }

  // A finished step leaves its process running for the next one. One that
  // waits for tools stays until Maki answers or gives up.
  private arm(conversation: Conversation): void {
    clearTimeout(conversation.timer)
    if (!conversation.live) return
    const waiting = conversation.record.waiting !== undefined
    const delay = waiting ? this.limits.orphanMs : this.limits.idleMs
    conversation.timer = setTimeout(() => {
      if (conversation.step) return
      void (async () => {
        if (waiting) await this.settle(conversation)
        await this.stopLive(conversation)
        await this.save(conversation)
      })()
    }, delay)
    conversation.timer.unref()
  }
}
