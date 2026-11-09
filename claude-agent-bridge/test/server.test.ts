import assert from "node:assert/strict"
import { request as httpRequest } from "node:http"
import { mkdtemp, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { after, before, test } from "node:test"
import type { Server } from "node:http"
import { handler, listen } from "../src/server.ts"
import { harness, LOGGED_IN, request, text, textTurn, user } from "./fake.ts"

let dir: string
let server: Server
let socket: string
const h = harness([[textTurn("hello")]])

before(async () => {
  dir = await mkdtemp(join(tmpdir(), "bridge-"))
  socket = join(dir, "state", "bridge.sock")
  server = await listen(
    socket,
    handler(h.conversations, async () => LOGGED_IN),
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
      handler(h.conversations, async () => LOGGED_IN),
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
    account: { email: "a@example.com" },
    ready: true,
  })
})

test("ending a conversation takes a known reason only", async () => {
  assert.equal((await call("POST", "/v1/conversations/c/end", { reason: "closed" })).status, 200)
  assert.equal((await call("POST", "/v1/conversations/c/end", { reason: "bored" })).status, 400)
})
