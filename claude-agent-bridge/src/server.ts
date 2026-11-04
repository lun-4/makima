import { chmod, mkdir, rm } from "node:fs/promises"
import { createConnection } from "node:net"
import { createServer } from "node:http"
import type { IncomingMessage, Server, ServerResponse } from "node:http"
import { dirname } from "node:path"
import type { Conversations } from "./conversations.ts"
import { isLoggedIn } from "./conversations.ts"
import { END_REASONS, invalid, parseStep, PROTOCOL_VERSION, toErrorTable } from "./protocol.ts"
import type { EndReason, ErrorTable } from "./protocol.ts"
import { BridgeError } from "./protocol.ts"
import type { Probe } from "./session.ts"

const MAX_BODY_BYTES = 64 * 1024 * 1024
// Inside Maki's provider idle timeout while Claude Code is slow or retrying.
const HEARTBEAT_MS = 15_000
const PROBE_TTL_MS = 60_000
const END_PATH = /^\/v1\/conversations\/([^/]+)\/end$/

// {contextWindows} asks each model's window too, which takes seconds.
export type ProbeClaude = (contextWindows: boolean) => Promise<Probe>

async function readBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of request) {
    size += (chunk as Buffer).length
    if (size > MAX_BODY_BYTES) throw invalid(`request body exceeds ${MAX_BODY_BYTES} bytes`)
    chunks.push(chunk as Buffer)
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"))
  } catch {
    throw invalid("request body is not JSON")
  }
}

function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" })
  response.end(JSON.stringify(body))
}

function frame(response: ServerResponse, event: string, data: unknown): void {
  response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
}

// Caches a probe briefly, since `auth_status` and `discover` ask often and
// each probe starts Claude Code.
function cachedProbe(probe: () => Promise<Probe>): () => Promise<Probe> {
  let cached: { at: number; result: Promise<Probe> } | undefined
  return () => {
    if (!cached || Date.now() - cached.at > PROBE_TTL_MS) {
      const result = probe()
      cached = { at: Date.now(), result }
      result.catch(() => {
        cached = undefined
      })
    }
    return cached.result
  }
}

export function handler(conversations: Conversations, probeClaude: ProbeClaude) {
  const probe = cachedProbe(() => probeClaude(false))
  const probeModels = cachedProbe(() => probeClaude(true))

  async function status(response: ServerResponse): Promise<void> {
    try {
      const result = await probe()
      json(response, 200, {
        protocol_version: PROTOCOL_VERSION,
        claude_code_version: result.version,
        logged_in: isLoggedIn(result),
        account: { email: result.account.email, subscription: result.account.subscriptionType, api_provider: result.account.apiProvider },
        ready: true,
      })
    } catch (error) {
      json(response, 200, { protocol_version: PROTOCOL_VERSION, ready: false, error: toErrorTable(error).message })
    }
  }

  async function models(response: ServerResponse): Promise<void> {
    const result = await probeModels()
    json(response, 200, {
      models: result.models.map((model) => ({
        value: model.value,
        resolved_model: model.resolvedModel,
        context_window: result.contextWindows?.[model.value],
        display_name: model.displayName,
        description: model.description,
        supports_adaptive_thinking: model.supportsAdaptiveThinking,
        supported_effort_levels: model.supportedEffortLevels,
      })),
    })
  }

  async function step(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const parsed = parseStep(await readBody(request))
    const abort = new AbortController()
    let finished = false
    let heartbeat: NodeJS.Timeout | undefined
    response.on("close", () => {
      if (!finished) abort.abort(new Error("Maki closed the step"))
    })
    const sink = {
      open() {
        response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" })
        heartbeat = setInterval(() => response.write(": heartbeat\n\n"), HEARTBEAT_MS)
      },
      event(event: unknown) {
        if (!abort.signal.aborted) frame(response, "event", event)
      },
    }
    try {
      const result = await conversations.step(parsed, sink, abort.signal)
      finished = true
      frame(response, "done", result)
    } catch (error) {
      finished = true
      if (abort.signal.aborted) return
      if (!response.headersSent) throw error
      frame(response, "error", toErrorTable(error))
    } finally {
      clearInterval(heartbeat)
      if (!response.writableEnded) response.end()
    }
  }

  async function end(id: string, request: IncomingMessage, response: ServerResponse): Promise<void> {
    const body = (await readBody(request)) as { reason?: unknown }
    if (!END_REASONS.includes(body.reason as EndReason)) throw invalid(`unknown reason ${JSON.stringify(body.reason)}`)
    await conversations.end(id, body.reason as EndReason)
    json(response, 200, {})
  }

  return async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    const url = new URL(request.url ?? "/", "http://bridge")
    try {
      const ending = END_PATH.exec(url.pathname)
      if (request.method === "GET" && url.pathname === "/v1/status") return await status(response)
      if (request.method === "GET" && url.pathname === "/v1/models") return await models(response)
      if (request.method === "POST" && url.pathname === "/v1/steps") return await step(request, response)
      if (request.method === "POST" && ending) return await end(decodeURIComponent(ending[1]!), request, response)
      json(response, 404, { message: `no route ${request.method} ${url.pathname}`, retry: "none" } satisfies ErrorTable)
    } catch (error) {
      if (response.headersSent) {
        response.end()
        return
      }
      json(response, error instanceof BridgeError ? error.httpStatus : 500, toErrorTable(error))
    }
  }
}

function socketAlive(path: string): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = createConnection(path)
    socket.once("connect", () => {
      socket.destroy()
      resolve(true)
    })
    socket.once("error", () => resolve(false))
  })
}

// Listens on {path}, readable only by this user. A socket left by a bridge
// that died is replaced; a live one means another bridge is running.
export async function listen(path: string, handle: ReturnType<typeof handler>): Promise<Server> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 })
  if (await socketAlive(path)) throw new Error(`another bridge is already listening on ${path}`)
  await rm(path, { force: true })
  const server = createServer((request, response) => void handle(request, response))
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(path, resolve)
  })
  await chmod(path, 0o600)
  return server
}
