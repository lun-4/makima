import assert from "node:assert/strict"
import { request as httpRequest } from "node:http"
import { mkdtemp, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { after, before, mock, test } from "node:test"
import type { Server } from "node:http"
import type { PlanUsage } from "../src/protocol.ts"
import { cached, handler, listen } from "../src/server.ts"
import { harness, LOGGED_IN, request, text, textTurn, user } from "./fake.ts"

const PLAN: PlanUsage = { plan: "pro", available: true, rate_limits: { five_hour: { utilization: 24, resets_at: "2026-11-15T20:19:59Z" } } }
const USAGE_FAILED = "claude did not answer"

let dir: string
let server: Server
let socket: string
let usageCalls = 0
let nextUsage: () => Promise<PlanUsage | undefined> = async () => PLAN
const h = harness([[textTurn("hello")]])

function fetchUsage(): Promise<PlanUsage | undefined> {
  usageCalls++
  return nextUsage()
}

before(async () => {
  dir = await mkdtemp(join(tmpdir(), "bridge-"))
  socket = join(dir, "state", "bridge.sock")
  const probe = { ...LOGGED_IN, account: { ...LOGGED_IN.account, subscriptionType: "max", apiProvider: "firstParty" as const } }
  server = await listen(
    socket,
    handler(h.conversations, async () => probe, fetchUsage),
  )
})

after(async () => {
  server.close()
  await rm(dir, { recursive: true, force: true })
})

function call(method: string, path: string, body?: unknown): Promise<{ status: number; type: string; text: string }> {
  return new Promise((resolve, reject) => {
    const req = httpRequest({ socketPath: socket, method, path, headers: { "content-type": "application/json" } }, (res) => {
      let text = ""
      res.setEncoding("utf8")
      res.on("data", (chunk: string) => (text += chunk))
      res.on("end", () => resolve({ status: res.statusCode ?? 0, type: String(res.headers["content-type"]), text }))
    })
    req.on("error", reject)
    req.end(body === undefined ? undefined : JSON.stringify(body))
  })
}

test("the socket is private to the user", async () => {
  assert.equal((await stat(socket)).mode & 0o777, 0o600)
})

test("a second bridge on the same socket refuses to start", async () => {
  await assert.rejects(
    listen(
      socket,
      handler(h.conversations, async () => LOGGED_IN, fetchUsage),
    ),
    /already listening/,
  )
})

test("a step streams events and ends with one done frame", async () => {
  const response = await call("POST", "/v1/steps", { protocol_version: 1, request: request("c", [user(text("hi"))]) })
  assert.equal(response.status, 200)
  assert.equal(response.type, "text/event-stream")
  const frames = response.text.trim().split("\n\n")
  assert.equal(frames[0], 'event: event\ndata: {"type":"text_delta","text":"hello"}')
  assert.match(frames.at(-1)!, /^event: done\ndata: \{"message"/)
  assert.equal(frames.length, 2)
})

test("a refused step answers with a status and an error table", async () => {
  const response = await call("POST", "/v1/steps", { protocol_version: 9, request: {} })
  assert.equal(response.status, 400)
  assert.equal(JSON.parse(response.text).retry, "none")
})

test("status reports the login", async () => {
  const response = await call("GET", "/v1/status")
  assert.deepEqual(JSON.parse(response.text), {
    protocol_version: 1,
    claude_code_version: "2.1.318",
    logged_in: true,
    account: { email: "a@example.com", subscription: "max", api_provider: "firstParty" },
    ready: true,
  })
})

test("models include the account used for discovery", async () => {
  const response = await call("GET", "/v1/models")
  assert.deepEqual(JSON.parse(response.text).account, { subscription: "max", api_provider: "firstParty" })
})

test("ending a conversation takes a known reason only", async () => {
  assert.equal((await call("POST", "/v1/conversations/c/end", { reason: "closed" })).status, 200)
  assert.equal((await call("POST", "/v1/conversations/c/end", { reason: "bored" })).status, 400)
})

test("usage is cached once fetched, and a failed fetch is not", async () => {
  nextUsage = async () => {
    throw new Error(USAGE_FAILED)
  }
  const failed = await call("GET", "/v1/usage")
  assert.equal(failed.status, 500)
  assert.equal(JSON.parse(failed.text).message, USAGE_FAILED)
  nextUsage = async () => PLAN
  assert.deepEqual(JSON.parse((await call("GET", "/v1/usage")).text), { usage: PLAN })
  assert.deepEqual(JSON.parse((await call("GET", "/v1/usage")).text), { usage: PLAN })
  assert.equal(usageCalls, 2)
})

test("a late failure keeps the newer cached answer", async () => {
  mock.timers.enable({ apis: ["Date"] })
  try {
    const first = Promise.withResolvers<string>()
    const loads = [first.promise, Promise.resolve("fresh")]
    let calls = 0
    const load = cached(1_000, () => loads[calls++]!)
    void load().catch(() => {})
    mock.timers.tick(1_001)
    assert.equal(await load(), "fresh")
    first.reject(new Error(USAGE_FAILED))
    await first.promise.catch(() => {})
    assert.equal(await load(), "fresh")
    assert.equal(calls, 2)
  } finally {
    mock.timers.reset()
  }
})
