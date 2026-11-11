local eventstream = require("providers.bedrock_eventstream")
local t = require("makima.test_helpers")

local THROTTLED = "Rate exceeded"
local INT_HEADER = 4

local function uint(value, bytes)
  local out = {}
  for i = bytes, 1, -1 do
    out[i] = string.char(value % 256)
    value = math.floor(value / 256)
  end
  return table.concat(out)
end

local function frame(headers, payload)
  local encoded = { "\5:skip" .. string.char(INT_HEADER) .. uint(1, 4) }
  for name, value in pairs(headers) do
    encoded[#encoded + 1] = string.char(#name) .. name .. "\7" .. uint(#value, 2) .. value
  end
  local header_bytes = table.concat(encoded)
  local total = 12 + #header_bytes + #payload + 4
  return uint(total, 4) .. uint(#header_bytes, 4) .. uint(0, 4) .. header_bytes .. payload .. uint(0, 4)
end

local function chunk(event)
  local bytes = makima.base64.encode(assert(makima.json.encode(event)))
  return frame({ [":event-type"] = "chunk" }, assert(makima.json.encode({ bytes = bytes })))
end

-- Splits {wire} into reads of {size} bytes, so frames straddle reads.
local function reads(wire, size)
  local chunks = {}
  for i = 1, #wire, size do
    chunks[#chunks + 1] = wire:sub(i, i + size - 1)
  end
  return chunks
end

t.case("chunks_feed_the_anthropic_decoder", function()
  local wire = table.concat({
    chunk({ type = "message_start", message = { usage = { input_tokens = 4 } } }),
    chunk({ type = "content_block_start", index = 0, content_block = { type = "text", text = "" } }),
    chunk({ type = "content_block_delta", index = 0, delta = { type = "text_delta", text = "Hi" } }),
    frame({ [":event-type"] = "other" }, "ignored"),
    chunk({ type = "message_delta", delta = { stop_reason = "end_turn" }, usage = { output_tokens = 1 } }),
  })
  for _, size in ipairs({ 1, 7, #wire }) do
    local emit, events = t.recorder()
    local result = assert(eventstream.stream(t.response(reads(wire, size)), emit))
    t.same(events, { { type = "text_delta", text = "Hi" } })
    t.same(result, {
      message = { role = "assistant", content = { { type = "text", text = "Hi" } } },
      usage = { input_tokens = 4, output_tokens = 1, cache_creation_input_tokens = 0, cache_read_input_tokens = 0 },
      stop_reason = "end_turn",
    })
  end
end)

t.case("exception_frame_is_an_api_error", function()
  local cases = {
    throttlingException = { 429, "rate_limit" },
    ThrottlingException = { 429, "rate_limit" },
    modelStreamErrorException = { 500, "transient" },
    modelTimeoutException = { 500, "transient" },
  }
  for name, case in pairs(cases) do
    local wire = frame({ [":message-type"] = "exception", [":exception-type"] = name }, THROTTLED)
    local _, err = eventstream.stream(t.response({ wire }), t.recorder())
    t.same(err, { status = case[1], message = THROTTLED, retry = case[2] }, name)
  end
end)

t.case("truncated_frame_fails", function()
  local wire = chunk({ type = "message_stop" })
  local _, err = eventstream.stream(t.response({ wire:sub(1, #wire - 3) }), t.recorder())
  t.eq(err, "Bedrock EventStream: truncated frame")
end)

t.report()
