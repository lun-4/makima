import { query } from "@anthropic-ai/claude-agent-sdk"
import type { AccountInfo, ModelInfo, Options, SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk"
import { planUsage } from "./convert.ts"
import type { ClaudeBlock } from "./convert.ts"
import type { PlanUsage } from "./protocol.ts"

const USAGE_METHOD = "usage_EXPERIMENTAL_MAY_CHANGE_DO_NOT_RELY_ON_THIS_API_YET"

export type ResultMessage = Extract<SDKMessage, { type: "result" }>
export type Effort = NonNullable<Options["effort"]>

export interface Probe {
  readonly version: string | undefined
  readonly account: AccountInfo
  readonly models: readonly ModelInfo[]
  // Context windows by model value, for the models Claude Code reported one for.
  readonly contextWindows?: Readonly<{ [model: string]: number }>
}

// One Claude Code process. The bridge talks to it only through this, so tests
// can stand in for Claude Code.
export interface Session {
  readonly ready: Promise<Probe>
  readonly ended: boolean
  readonly running: boolean
  begin(content: ClaudeBlock[], uuid: string): void
  // Content Claude reads once its waiting tools return, within the same turn.
  addUser(content: ClaudeBlock[], uuid: string): void
  // Reads the running turn until {endsStep} says a message ends the step, or
  // until the turn's result. Rejects once {signal} aborts.
  readStep(onMessage: (message: SDKMessage) => boolean, signal: AbortSignal): Promise<ResultMessage | "tools">
  // Stops the running turn and waits for Claude Code to finish writing it.
  interrupt(): Promise<void>
  stop(): Promise<void>
  setModel(model: string): Promise<void>
  setEffort(effort: Effort | undefined): Promise<void>
}

export type StartSession = (options: Options) => Session

// Older releases ignore `verbatimPrompts`, and could read user text as
// Claude Code commands.
const MINIMUM_VERSION = [2, 1, 248]

export function versionError(version: string | undefined): string | undefined {
  if (!version) return "Claude Code did not report its version. Run `claude update`."
  const parts = version.split(/[-+]/)[0]!.split(".").map(Number)
  for (let i = 0; i < MINIMUM_VERSION.length; i++) {
    const have = parts[i] ?? 0
    const need = MINIMUM_VERSION[i]!
    if (have !== need)
      return have > need
        ? undefined
        : `Claude Code ${version} is too old; ${MINIMUM_VERSION.join(".")} or later is required. Run \`claude update\`.`
  }
  return undefined
}

class InputQueue implements AsyncIterable<SDKUserMessage> {
  private readonly items: SDKUserMessage[] = []
  private wake: (() => void) | undefined
  private closed = false

  push(item: SDKUserMessage): void {
    this.items.push(item)
    this.wake?.()
  }

  close(): void {
    this.closed = true
    this.wake?.()
  }

  async *[Symbol.asyncIterator](): AsyncIterator<SDKUserMessage> {
    while (true) {
      const item = this.items.shift()
      if (item) {
        yield item
        continue
      }
      if (this.closed) return
      await new Promise<void>((resolve) => {
        this.wake = resolve
      })
      this.wake = undefined
    }
  }
}

interface Turn {
  readonly messages: SDKMessage[]
  readonly drained: PromiseWithResolvers<void>
  wake: (() => void) | undefined
  finished: boolean
}

function userMessage(content: ClaudeBlock[], uuid: string, priority?: "next"): SDKUserMessage {
  const message = { role: "user", content } as SDKUserMessage["message"]
  return { type: "user", uuid: uuid as SDKUserMessage["uuid"], message, parent_tool_use_id: null, priority }
}

export const startSession: StartSession = (options) => {
  const input = new InputQueue()
  const sdk = query({ prompt: input, options })
  let turn: Turn | undefined
  let ended = false
  let failure: unknown
  let stopping: Promise<void> | undefined

  const ready = sdk.initializationResult().then((init) => {
    const error = versionError(init.claude_code_version)
    if (error) throw new Error(error)
    return { version: init.claude_code_version, account: init.account, models: init.models }
  })
  ready.catch(() => {})

  const finishTurn = () => {
    if (!turn || turn.finished) return
    turn.finished = true
    turn.drained.resolve()
  }

  // The only reader of the SDK iterator, so a cancelled step still drains.
  const pump = (async () => {
    try {
      for await (const message of sdk) {
        if (turn && !turn.finished) {
          turn.messages.push(message)
          turn.wake?.()
        }
        if (message.type === "result") finishTurn()
      }
    } catch (error) {
      failure = error
    } finally {
      ended = true
      const active = turn
      finishTurn()
      active?.wake?.()
    }
  })()

  const interrupt = async () => {
    const active = turn
    if (!active || active.finished || ended) return
    await sdk.interrupt()
    // interrupt() resolves before the result and the transcript flush.
    await active.drained.promise
  }

  return {
    ready,
    get ended() {
      return ended
    },
    get running() {
      return !!turn && !turn.finished
    },
    begin(content, uuid) {
      if (ended || stopping) throw failure ?? new Error("Claude Code has stopped")
      if (turn && !turn.finished) throw new Error("Claude Code is still finishing the previous turn")
      turn = { messages: [], drained: Promise.withResolvers(), wake: undefined, finished: false }
      input.push(userMessage(content, uuid))
    },
    addUser(content, uuid) {
      input.push(userMessage(content, uuid, "next"))
    },
    async readStep(endsStep, signal) {
      const active = turn
      if (!active) throw new Error("no Claude turn to read")
      const aborted = () => active.wake?.()
      signal.addEventListener("abort", aborted)
      try {
        while (true) {
          signal.throwIfAborted()
          const message = active.messages.shift()
          if (!message) {
            if (active.finished) throw failure ?? new Error("Claude Code stopped before finishing its reply")
            await new Promise<void>((resolve) => {
              active.wake = resolve
            })
            active.wake = undefined
            continue
          }
          if (endsStep(message)) return "tools"
          if (message.type === "result") return message
        }
      } finally {
        signal.removeEventListener("abort", aborted)
      }
    },
    interrupt,
    stop() {
      stopping ??= (async () => {
        try {
          if (!ended) await interrupt().catch(() => {})
          input.close()
          await pump
        } finally {
          sdk.close()
        }
      })()
      return stopping
    },
    setModel: (model) => sdk.setModel(model),
    setEffort: (effort) => sdk.applyFlagSettings({ effortLevel: effort ?? null } as never),
  }
}

// Each model's context window, from Claude Code's own accounting. A model it
// cannot answer for is left out.
async function contextWindows(sdk: ReturnType<typeof query>, models: readonly ModelInfo[]): Promise<{ [model: string]: number }> {
  const windows: { [model: string]: number } = {}
  for (const model of models) {
    try {
      await sdk.setModel(model.value)
      const usage = await sdk.getContextUsage({ detail: "summary" })
      if (Number.isSafeInteger(usage.rawMaxTokens) && usage.rawMaxTokens > 0) windows[model.value] = usage.rawMaxTokens
    } catch {
      continue
    }
  }
  return windows
}

type Query = ReturnType<typeof query>
type Init = Awaited<ReturnType<Query["initializationResult"]>>

// Starts Claude Code without a prompt and runs {use} on it, all within
// {timeoutMs}. No model call is made.
async function idleClaude<T>(options: Options, timeoutMs: number, use: (sdk: Query, init: Init) => Promise<T>): Promise<T> {
  const stop = Promise.withResolvers<void>()
  async function* idle(): AsyncGenerator<SDKUserMessage> {
    await stop.promise
  }
  const sdk = query({ prompt: idle(), options: { ...options, persistSession: false } })
  try {
    const timeout = new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`claude did not answer within ${timeoutMs / 1000} seconds`)), timeoutMs).unref(),
    )
    return await Promise.race([sdk.initializationResult().then((init) => use(sdk, init)), timeout])
  } finally {
    stop.resolve()
    sdk.close()
  }
}

// Claude Code's version, login and models, and with {withContextWindows}
// their context windows.
export function probe(options: Options, timeoutMs: number, withContextWindows: boolean): Promise<Probe> {
  return idleClaude(options, timeoutMs, async (sdk, init) => ({
    version: init.claude_code_version,
    account: init.account,
    models: init.models,
    contextWindows: withContextWindows ? await contextWindows(sdk, init.models) : undefined,
  }))
}

// The login's plan usage, or undefined when this SDK has no usage call. The
// call is experimental, so the bridge checks for it rather than relying on it.
export function usage(options: Options, timeoutMs: number): Promise<PlanUsage | undefined> {
  return idleClaude(options, timeoutMs, async (sdk) => {
    const fetch = (sdk as Partial<Query>)[USAGE_METHOD]
    if (typeof fetch !== "function") return undefined
    return planUsage(await fetch.call(sdk, { skipBehaviors: true }))
  })
}
