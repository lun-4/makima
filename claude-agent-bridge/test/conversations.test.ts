import assert from "node:assert/strict"
import { test } from "node:test"
import { BridgeError } from "../src/protocol.ts"
import { SERVER_NAME } from "../src/tools.ts"
import type { StepRequest } from "../src/protocol.ts"
import { assistant, harness, READ, request, result, sdk, SESSION, text, textTurn, toolTurn, user } from "./fake.ts"

const GO = user(text("go"))
const CALL = assistant({ type: "tool_use", id: "t1", name: "read", input: { path: "a" } })
const CHECKPOINT = /^<conversation-checkpoint>/

async function rejects(promise: Promise<unknown>, check: (error: BridgeError) => void): Promise<void> {
  await assert.rejects(promise, (error: unknown) => {
    assert.ok(error instanceof BridgeError, String(error))
    check(error)
    return true
  })
}

function texts(content: { type: string; text?: string }[]): (string | undefined)[] {
  return content.map((block) => block.text)
}

test("a tool step parks the turn and the next request continues it", async () => {
  const h = harness([[toolTurn("t1"), textTurn("done")]])
  const first = await h.step(request("c", [GO]))
  assert.equal(first.stop_reason, "tool_use")
  assert.deepEqual(first.message.content, [{ type: "tool_use", id: "t1", name: "read", input: { path: "a" } }])
  assert.deepEqual(h.events, [{ type: "tool_use_start", id: "t1", name: "read" }])

  const second = await h.step(request("c", [GO, CALL, user(result("t1", "file"))]))
  assert.equal(second.stop_reason, "end_turn")
  assert.equal(h.sessions.length, 1)
  assert.equal(h.sessions[0]!.sent.length, 1, "the results resume the turn without a new prompt")
  const saved = h.records.saved.get("c")!
  assert.equal(saved.seen, 4)
  assert.equal(saved.waiting, undefined)
  assert.deepEqual(
    saved.boundaries.map((boundary) => boundary.count),
    [4],
  )
})

test("text after the tool results reaches the waiting turn", async () => {
  const h = harness([[toolTurn("t1"), textTurn("done")]])
  await h.step(request("c", [GO]))
  await h.step(request("c", [GO, CALL, user(result("t1", "file"), text("also check b"))]))
  assert.deepEqual(h.sessions[0]!.sent[1], { content: [{ type: "text", text: "also check b" }], next: true })
})

test("a wrong continuation fails once, then the conversation starts over", async () => {
  const h = harness([[toolTurn("t1")], [textTurn("fresh")]])
  await h.step(request("c", [GO]))
  await rejects(h.step(request("c", [GO, CALL, user(result("t9", "file"))])), (error) => {
    assert.equal(error.httpStatus, 400)
    assert.equal(error.table.retry, "none")
  })
  assert.equal(h.sessions[0]!.stops, 1)
  const retried = await h.step(request("c", [GO, CALL, user(result("t9", "file")), user(text("again"))]))
  assert.equal(retried.stop_reason, "end_turn")
  assert.match(texts(h.sessions[1]!.sent[0]!.content)[0]!, CHECKPOINT)
  assert.ok(h.sessions[1]!.options.sessionId, "no finished reply to fork from, so a new session")
})

test("waiting tools without their process restart from the history", async () => {
  const h = harness([[toolTurn("t1")], [textTurn("fresh")]])
  await h.step(request("c", [GO]))
  await h.conversations.close()
  await h.step(request("c", [GO, CALL, user(result("t1", "file"))]))
  assert.match(texts(h.sessions[1]!.sent[0]!.content)[0]!, CHECKPOINT)
})

test("a repeated operation and a second concurrent step are conflicts", async () => {
  const h = harness([[[sdk.start("m")]]])
  const first = request("c", [GO])
  const running = h.step(first, new AbortController().signal).catch(() => {})
  await rejects(h.step(first), (error) => assert.equal(error.httpStatus, 409))
  await rejects(h.step(request("c", [GO])), (error) => assert.equal(error.httpStatus, 409))
  await h.conversations.close()
  void running
})

test("a successor that continues takes over the process", async () => {
  const h = harness([[textTurn("hi"), textTurn("again")]])
  await h.step(request("c1", [GO]))
  await h.step(request("c2", [GO, assistant(text("hi")), user(text("more"))], { previous: { id: "c1", relation: "continues", shared: 2 } }))
  assert.equal(h.sessions.length, 1)
  assert.deepEqual(texts(h.sessions[0]!.sent[1]!.content), ["more"])
  assert.equal(h.records.saved.has("c1"), false)
  assert.equal(h.records.saved.get("c2")!.seen, 4)
})

test("after a cancel, Claude keeps its partial reply and Makima's copy is left out", async () => {
  const h = harness([[[sdk.start("m")], textTurn("ok")]])
  const abort = new AbortController()
  const running = h.step(request("c1", [GO]), abort.signal)
  await new Promise((resolve) => setImmediate(resolve))
  abort.abort()
  await assert.rejects(running)
  assert.equal(h.sessions[0]!.interrupts, 1)
  await h.conversations.end("c1", "cancelled")
  const partial = assistant(text("half a rep"))
  await h.step(request("c2", [GO, partial, user(text("go on"))], { previous: { id: "c1", relation: "continues", shared: 1 } }))
  assert.deepEqual(texts(h.sessions[0]!.sent[1]!.content), ["go on"])
})

test("a branch forks the source at its last finished reply", async () => {
  const h = harness([[textTurn("hi")], [textTurn("aside")]])
  await h.step(request("c1", [GO]))
  const boundary = h.records.saved.get("c1")!.boundaries[0]!
  await h.step(request("c2", [GO, assistant(text("hi")), user(text("btw"))], { previous: { id: "c1", relation: "branches", shared: 2 } }))
  const options = h.sessions[1]!.options
  assert.equal(options.resume, SESSION)
  assert.equal(options.resumeSessionAt, boundary.uuid)
  assert.equal(options.forkSession, true)
  assert.deepEqual(texts(h.sessions[1]!.sent[0]!.content), ["btw"])
  assert.ok(h.records.saved.has("c1"), "a branch leaves its source")
})

test("a branch with other tools starts fresh with the shared history", async () => {
  const h = harness([[textTurn("hi")], [textTurn("aside")]])
  await h.step(request("c1", [GO]))
  const aside = request(
    "c2",
    [GO, assistant(text("hi")), user(text("btw"))],
    { previous: { id: "c1", relation: "branches", shared: 2 } },
    [],
  )
  await h.step(aside)
  assert.ok(h.sessions[1]!.options.sessionId)
  assert.match(texts(h.sessions[1]!.sent[0]!.content)[0]!, CHECKPOINT)
})

test("closing a conversation stops its process and forgets it", async () => {
  const h = harness([[textTurn("hi")]])
  await h.step(request("c", [GO]))
  await h.conversations.end("c", "closed")
  assert.equal(h.sessions[0]!.stops, 1)
  assert.equal(h.records.saved.has("c"), false)
  assert.equal(h.conversations.size, 0)
})

test("model and effort change on the running process", async () => {
  const h = harness([[textTurn("a"), textTurn("b")]])
  await h.step(request("c", [GO]))
  const next: StepRequest = {
    ...request("c", [GO, assistant(text("a")), user(text("b"))]),
    model: { id: "opus" },
    thinking: { kind: "effort", level: "high" },
  }
  await h.step(next)
  assert.equal(h.sessions.length, 1)
  assert.deepEqual(h.sessions[0]!.models, ["opus"])
  assert.deepEqual(h.sessions[0]!.efforts, ["high"])
})

test("a logged-out Claude Code is a config error", async () => {
  const h = harness([[textTurn("hi")]], { version: "2.1.318", account: {}, models: [] })
  await rejects(h.step(request("c", [GO])), (error) => assert.equal(error.table.kind, "config"))
})

test("a tool server that did not connect fails the step", async () => {
  const turn = toolTurn("t1")
  turn[0] = sdk.init([{ name: SERVER_NAME, status: "failed" }])
  const h = harness([[turn]])
  await rejects(h.step(request("c", [GO], {}, [READ])), (error) => assert.equal(error.table.retry, "none"))
  assert.equal(h.opened(), 1)
})

test("compaction and isolated conversations leave no record", async () => {
  const h = harness([[textTurn("summary")]])
  await h.step(request("c", [GO], { purpose: "compaction" }))
  assert.equal(h.records.saved.size, 0)
})
