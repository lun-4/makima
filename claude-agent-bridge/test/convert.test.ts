import assert from "node:assert/strict"
import { test } from "node:test"
import type { SDKMessage } from "@anthropic-ai/claude-agent-sdk"
import { catchUp, continuation, StepTranslator } from "../src/convert.ts"
import { unseen } from "../src/conversations.ts"
import { BridgeError } from "../src/protocol.ts"
import type { Block } from "../src/protocol.ts"
import { PendingTools, PREFIX, shortName, CANCELLED_TEXT } from "../src/tools.ts"
import { assistant, READ, result, sdk, text, user } from "./fake.ts"

const PICTURE: Block = { type: "image", source: { media_type: "image/png", data: "AAAA" } }
const CALL: Block = { type: "tool_use", id: "t1", name: "read", input: { path: "a" } }

test("history Claude has not seen becomes a checkpoint before the new prompt", () => {
  const blocks = catchUp([
    user(text("hi")),
    assistant({ type: "thinking", thinking: "hmm" }, CALL),
    user(result("t1", "file")),
    user(text("next")),
  ])
  assert.deepEqual(
    blocks.map((block) => (block.type === "text" ? block.text : block.type)),
    [
      blocks[0]!.type === "text" ? blocks[0]!.text : "",
      "<user>",
      "hi",
      "</user>",
      "<assistant>",
      '<tool-call>\n{"id":"t1","name":"read","input":{"path":"a"}}\n</tool-call>',
      "</assistant>",
      "<user>",
      '<tool-result>\n{"id":"t1","content":"file"}\n</tool-result>',
      "</user>",
      "</conversation-checkpoint>",
      "next",
    ],
  )
})

test("a history that ends on tool results asks Claude to go on", () => {
  const blocks = catchUp([user(text("hi")), assistant(CALL), user(result("t1", "file"))])
  assert.deepEqual(blocks.slice(-2), [
    { type: "text", text: "</conversation-checkpoint>" },
    { type: "text", text: "Continue from where the checkpoint ends." },
  ])
})

test("user messages alone go in as they are", () => {
  assert.deepEqual(catchUp([user(text("a")), user(text("b"), PICTURE)]), [
    { type: "text", text: "a" },
    { type: "text", text: "b" },
    { type: "image", source: { type: "base64", media_type: "image/png", data: "AAAA" } },
  ])
})

test("images after a tool result join it", () => {
  const { results, extra } = continuation(user(result("t1", "shot"), PICTURE, text("look")), [], ["t1"])
  assert.deepEqual(results.get("t1"), {
    isError: false,
    content: [
      { type: "text", text: "shot" },
      { type: "image", mimeType: "image/png", data: "AAAA" },
    ],
  })
  assert.deepEqual(extra, [{ type: "text", text: "look" }])
})

for (const [name, blocks, message] of [
  ["missing", [result("t1", "a")], /missing results for tool calls t2/],
  ["duplicate", [result("t1", "a"), result("t1", "b"), result("t2", "c")], /duplicate result/],
  ["unknown", [result("t1", "a"), result("t2", "b"), result("t3", "c")], /unknown tool call t3/],
] as const) {
  test(`a ${name} tool result is refused`, () => {
    assert.throws(
      () => continuation(user(...blocks), [], ["t1", "t2"]),
      (error: unknown) => error instanceof BridgeError && message.test(error.message),
    )
  })
}

test("after an interrupt the partial reply and cancelled results are Claude's", () => {
  const messages = [user(text("go")), assistant(text("half")), user(result("t1", "cancelled"), text("again"))]
  assert.deepEqual(unseen({ seen: 1, boundaries: [], interrupted: { tools: ["t1"] } }, messages), [user(text("again"))])
  assert.deepEqual(unseen({ seen: 1, boundaries: [] }, messages), messages.slice(1))
})

function run(messages: SDKMessage[]) {
  const translator = new StepTranslator([READ])
  const events = messages.flatMap((message) => translator.translate(message))
  return { translator, events }
}

test("stream events become Maki events", () => {
  const { events } = run([
    sdk.start("m"),
    sdk.blockStart(0, { type: "thinking", thinking: "" }),
    sdk.delta(0, { type: "thinking_delta", thinking: "plan" }),
    sdk.blockStop(0),
    sdk.blockStart(1, { type: "text", text: "" }),
    sdk.delta(1, { type: "text_delta", text: "hi" }),
    sdk.blockStop(1),
    sdk.blockStart(2, { type: "tool_use", id: "t1", name: `${PREFIX}read` }),
    sdk.blockStart(3, { type: "tool_use", id: "t2", name: "Bash" }),
  ])
  assert.deepEqual(events, [
    { type: "thinking_delta", text: "plan" },
    { type: "thinking_block_end" },
    { type: "text_delta", text: "hi" },
    { type: "tool_use_start", id: "t1", name: "read" },
  ])
})

test("a reply that was not streamed is emitted whole", () => {
  const { events, translator } = run([sdk.assistant("m", [{ type: "text", text: "whole" }])])
  assert.deepEqual(events, [{ type: "text_delta", text: "whole" }])
  assert.deepEqual(translator.result("end_turn").message.content, [{ type: "text", text: "whole" }])
})

test("the turn total never inflates a step that saw its own usage", () => {
  const { translator } = run([sdk.start("last", { input_tokens: 5, output_tokens: 1 })])
  const total = { input_tokens: 50, output_tokens: 90, cache_read_input_tokens: 1_200_000, cache_creation_input_tokens: 0 }
  translator.translate(sdk.result({ usage: total, num_turns: 10 }))
  assert.deepEqual(translator.result("end_turn").usage, {
    input_tokens: 5,
    output_tokens: 1,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
  })
})

test("a step with no usage of its own takes a one-call turn's total", () => {
  const { translator } = run([])
  translator.translate(sdk.result({ usage: { input_tokens: 7, output_tokens: 3 }, num_turns: 1 }))
  assert.equal(translator.result("end_turn").usage.input_tokens, 7)
})

test("usage keeps each response's highest count and the last context", () => {
  const { translator } = run([
    sdk.start("a", { input_tokens: 100, output_tokens: 1 }),
    sdk.assistant("a", [], {}),
    sdk.start("b", { input_tokens: 300, output_tokens: 1 }),
  ])
  translator.translate({
    ...sdk.start("b"),
    event: { type: "message_delta", usage: { output_tokens: 7 }, delta: {} },
  } as unknown as SDKMessage)
  assert.deepEqual(translator.result("end_turn").usage, {
    input_tokens: 300,
    output_tokens: 12,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
  })
})

for (const [name, setup, final, expected] of [
  ["overflow", [], { is_error: true, result: "Prompt is too long" }, { overflow: true, retry: "none" }],
  [
    "login",
    [sdk.assistant("m", [], { error: "authentication_failed" })],
    { is_error: true, result: "Invalid API key" },
    { kind: "config" },
  ],
  ["rate limit", [sdk.assistant("m", [], { error: "rate_limit" })], { is_error: true, result: "" }, { status: 429, retry: "none" }],
  ["server", [], { is_error: true, result: "", api_error_status: 529 }, { status: 529, retry: "none" }],
] as const) {
  test(`a failed turn maps to a Maki error: ${name}`, () => {
    const { translator } = run([...setup])
    const end = sdk.result(final) as Extract<SDKMessage, { type: "result" }>
    assert.throws(
      () => translator.finish(end),
      (error: unknown) =>
        error instanceof BridgeError &&
        Object.entries(expected).every(([key, value]) => (error.table as unknown as Record<string, unknown>)[key] === value),
    )
  })
}

test("long tool names fit MCP and stay distinct", () => {
  const long = "x".repeat(80)
  const first = shortName(`${long}a`)
  const second = shortName(`${long}b`)
  assert.ok((PREFIX + first).length <= 64)
  assert.notEqual(first, second)
  assert.equal(shortName("read"), "read")
})

test("a result can arrive before Claude calls the handler", async () => {
  const pending = new PendingTools()
  pending.expect("t1", `${PREFIX}read`)
  pending.resolve("t1", { content: [{ type: "text", text: "early" }] })
  pending.before("t1", `${PREFIX}read`)
  assert.deepEqual(await pending.call(`${PREFIX}read`), { content: [{ type: "text", text: "early" }] })
})

test("clearing answers every waiting handler as cancelled", async () => {
  const pending = new PendingTools()
  pending.before("t1", `${PREFIX}read`)
  const waiting = pending.call(`${PREFIX}read`)
  pending.clear()
  assert.deepEqual(await waiting, { isError: true, content: [{ type: "text", text: CANCELLED_TEXT }] })
})
