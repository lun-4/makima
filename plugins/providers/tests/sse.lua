local sse = require("providers.sse")
local t = require("maki.test_helpers")

local READ_ERROR = "connection reset"

local function collect(chunks, opts)
  local next_event = sse.events(t.response(chunks, opts))
  local events = {}
  while true do
    local event, err = next_event()
    if not event then
      return events, err
    end
    events[#events + 1] = event
  end
end

t.case("every_chunk_split_gives_the_same_events", function()
  local wire = "\239\187\191event: first\r\ndata: a\r\ndata:b\r\n\r\n: comment\rdata: c\r\revent: last\ndata: d\n\n"
  local expected = {
    { event = "first", data = "a\nb" },
    { event = "message", data = "c" },
    { event = "last", data = "d" },
  }
  for split = 0, #wire do
    local chunks = {}
    for _, part in ipairs({ wire:sub(1, split), wire:sub(split + 1) }) do
      if part ~= "" then
        chunks[#chunks + 1] = part
      end
    end
    t.same(collect(chunks), expected, "split at " .. split)
  end
end)

t.case("byte_at_a_time", function()
  local wire = "data: one\r\n\r\ndata: two\n\n"
  local chunks = {}
  for i = 1, #wire do
    chunks[i] = wire:sub(i, i)
  end
  t.same(collect(chunks), { { event = "message", data = "one" }, { event = "message", data = "two" } })
end)

t.case("event_without_data_is_skipped_and_type_resets", function()
  t.same(collect({ "event: ignored\n\ndata: x\n\n" }), { { event = "message", data = "x" } })
end)

t.case("open_event_dispatched_at_end", function()
  t.same(collect({ "data: partial" }), { { event = "message", data = "partial" } })
  t.same(collect({ "data: a\ndata: b\n" }), { { event = "message", data = "a\nb" } })
end)

t.case("read_error_ends_the_stream", function()
  local events, err = collect({ "data: done\n\ndata: lost" }, { read_error = READ_ERROR })
  t.same(events, { { event = "message", data = "done" } })
  t.eq(err, READ_ERROR)
end)

t.report()
