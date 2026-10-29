local ToolView = require("maki.tool_view")

local failures = {}

local function case(name, fn)
  local ok, err = pcall(fn)
  if not ok then
    table.insert(failures, name .. ": " .. tostring(err))
  end
end

local function eq(actual, expected, msg)
  if actual ~= expected then
    error((msg or "") .. "\nexpected: " .. tostring(expected) .. "\n  actual: " .. tostring(actual))
  end
end

local SSE = require("maki.sse")

local function sse_body(chunks, failure)
  local index = 0
  return {
    read = function()
      index = index + 1
      if chunks[index] then
        return chunks[index]
      end
      if failure then
        return nil, failure
      end
      return ""
    end,
  }
end

case("sse_chunk_boundaries_and_multiline", function()
  local wire = ": comment\r\nevent: delta\r\ndata: café\r\ndata:  two\r\n\r\ndata\n\n"
  for split = 1, #wire do
    local next_event = SSE.new(sse_body({ wire:sub(1, split), wire:sub(split + 1) }), 256)
    local first = next_event()
    eq(first.event, "delta")
    eq(first.data, "café\n two")
    local second = next_event()
    eq(second.event, "message")
    eq(second.data, "")
    eq(next_event(), nil)
  end
end)

case("sse_eof_and_field_rules", function()
  local next_event = SSE.new(sse_body({ "event: ignored\n\n: comment\rdata: one\rdata:two\revent: custom\r\r" }), 256)
  local event = next_event()
  eq(event.event, "custom")
  eq(event.data, "one\ntwo")
  eq(next_event(), nil)
  eq(next_event(), nil)
end)

case("sse_incomplete_events_are_discarded", function()
  for _, wire in ipairs({ "data: partial", "data: partial\n", "data: partial\r" }) do
    local next_event = SSE.new(sse_body({ wire }), 256)
    eq(next_event(), nil)
    eq(next_event(), nil)
  end
end)

case("sse_bom_and_cr_boundaries", function()
  local wire = "\239\187\191data: hello\r\r"
  for split = 1, #wire do
    local next_event = SSE.new(sse_body({ wire:sub(1, split), wire:sub(split + 1) }, "unexpected read"), 256)
    local event, err = next_event()
    eq(err, nil)
    eq(event.data, "hello")
  end
end)

case("sse_heartbeat_and_line_limit", function()
  local next_event = SSE.new(sse_body({ string.rep(": ping\n", 100) .. "data: ok\n\n" }), 16)
  eq(next_event().data, "ok")
  next_event = SSE.new(sse_body({ "\r", string.rep("x", 20) }), 16)
  local event, err = next_event()
  eq(event, nil)
  eq(err, "SSE event exceeds max_event_bytes")
end)

case("sse_rejects_invalid_size_limits", function()
  for _, limit in ipairs({ 0, -1, 0.5, math.huge, 0 / 0 }) do
    eq(pcall(SSE.new, sse_body({}), limit), false)
  end
end)

case("sse_error_and_size_limit", function()
  local next_event = SSE.new(sse_body({ "data: incomplete" }, "connection lost"), 256)
  local event, err = next_event()
  eq(event, nil)
  eq(err, "connection lost")
  event, err = next_event()
  eq(event, nil)
  eq(err, "connection lost")
  next_event = SSE.new(sse_body({ "data: ", "123456789" }), 10)
  event, err = next_event()
  eq(event, nil)
  eq(err, "SSE event exceeds max_event_bytes")
end)

local JsonResponse = require("maki.json_response")

case("json_response_chunked_and_exact_limit", function()
  local value, err = JsonResponse.read(sse_body({ '{"value":', "false}" }), 15)
  eq(err, nil)
  eq(value.value, false)
  value, err = JsonResponse.read(sse_body({ "false" }), 5)
  eq(err, nil)
  eq(value, false)
end)

case("json_response_null_and_zero_limit", function()
  local value, err = JsonResponse.read(sse_body({ "null" }), 4)
  eq(value, nil)
  eq(err, nil)
  value, err = JsonResponse.read(sse_body({ "null" }), 0)
  eq(value, nil)
  eq(err.kind, "api")
  eq(err.message, "JSON response exceeds max_bytes")
  value, err = JsonResponse.read(sse_body({}), 0)
  eq(value, nil)
  eq(err.kind, "api")
  assert(err.message:find("invalid JSON response:", 1, true))
end)

case("json_response_preserves_null_when_requested", function()
  local value, err =
    JsonResponse.read(sse_body({ '{"items":[null,', 'false,null],"missing":null}' }), 100, { preserve_null = true })
  eq(err, nil)
  eq(value.items[1], maki.json.null())
  eq(value.items[2], false)
  eq(value.items[3], maki.json.null())
  eq(value.missing, maki.json.null())
  value, err = JsonResponse.read(sse_body({ "null" }), 4, { preserve_null = true })
  eq(err, nil)
  eq(value, maki.json.null())
end)

case("json_response_rejects_invalid_limits", function()
  for _, limit in ipairs({ -1, 0.5, math.huge, 0 / 0, "1" }) do
    local ok = pcall(JsonResponse.read, sse_body({}), limit)
    eq(ok, false)
  end
end)

case("json_response_rejects_oversize_before_next_read", function()
  local reads = 0
  local body = {
    read = function()
      reads = reads + 1
      return "1234"
    end,
  }
  local value, err = JsonResponse.read(body, 3)
  eq(value, nil)
  eq(err.kind, "api")
  eq(err.message, "JSON response exceeds max_bytes")
  eq(reads, 1)
end)

case("json_response_read_and_decode_errors", function()
  local value, err = JsonResponse.read(sse_body({ "{" }, "connection lost"), 20)
  eq(value, nil)
  eq(err.kind, "network")
  eq(err.message, "connection lost")
  value, err = JsonResponse.read(sse_body({ "{" }), 20)
  eq(value, nil)
  eq(err.kind, "api")
  assert(err.message:find("invalid JSON response:", 1, true))
  local original = { kind = "closed", message = "closed" }
  value, err = JsonResponse.read(sse_body({}, original), 0)
  eq(value, nil)
  eq(err, original)
  value, err = JsonResponse.read({
    read = function()
      return nil
    end,
  }, 20)
  eq(value, nil)
  eq(err.kind, "network")
  eq(err.message, "response body read failed")
end)

-- Mock buf that records set_lines calls
local function mock_buf()
  local b = { lines = nil, call_count = 0 }
  function b:set_lines(lines)
    self.lines = lines
    self.call_count = self.call_count + 1
  end
  function b:on() end
  return b
end

case("truncate_within_limits_unchanged", function()
  eq(maki.text.truncate_file("hello", 100, 1000, nil), "hello")
  eq(maki.text.truncate_file("a\nb\nc", 3, 1000, nil), "a\nb\nc")
  eq(maki.text.truncate_file("", 100, 1000, nil), "")
end)

case("truncate_exceeds_line_limit", function()
  local result = maki.text.truncate_file("aaa\nbbb\nccc\nddd", 2, 1000, nil)
  assert(result:find("aaa", 1, true), "should keep first line")
  assert(result:find("bbb", 1, true), "should keep second line")
  assert(not result:find("ccc", 1, true), "should drop third line")
  assert(result:find("%[file truncated%]"), "should have truncation marker")
end)

case("truncate_exceeds_byte_limit", function()
  local text = string.rep("x", 200)
  local result = maki.text.truncate_file(text, 1000, 50, nil)
  assert(#result < #text, "should be shorter")
  assert(result:find("%[file truncated"), "should have truncation marker")
end)

case("truncate_byte_limit_mid_line", function()
  local text = "short\n" .. string.rep("x", 100)
  local result = maki.text.truncate_file(text, 1000, 20, nil)
  assert(result:find("short"), "should keep first line")
  assert(not result:find(string.rep("x", 100)), "should drop long line")
  assert(result:find("%[file truncated"), "should have truncation marker")
end)

case("truncate_trailing_newlines_counted", function()
  local result = maki.text.truncate_file("a\n\n\n\n\n", 2, 1000, nil)
  assert(result:find("%[file truncated"), "trailing newlines should count as lines")
end)

-- maki.match.fuzzy tests (mirror of the maki-lua unit tests)

case("match_fuzzy_empty_query_matches_all", function()
  local m = maki.match.fuzzy("", "hello world")
  assert(m, "empty query must match")
  eq(m.score, 0)
  eq(#m.indices, 0)
  local w = maki.match.fuzzy("   ", "hello world")
  assert(w, "whitespace query must match")
  eq(w.score, 0)
  eq(#w.indices, 0)
end)

case("match_fuzzy_no_match_returns_nil", function()
  eq(maki.match.fuzzy("xyz", "hello world"), nil)
  eq(maki.match.fuzzy("hello xyz", "hello world"), nil)
end)

case("match_fuzzy_returns_1based_codepoint_indices", function()
  local m = maki.match.fuzzy("ap", "apple")
  assert(m, "expected match")
  eq(#m.indices, 2)
  eq(m.indices[1], 1)
  eq(m.indices[2], 2)
end)

case("match_fuzzy_multi_term_order_independent", function()
  assert(maki.match.fuzzy("441 review", "review gh pr 441"), "order must not matter")
end)

case("match_fuzzy_smart_case", function()
  eq(maki.match.fuzzy("APPLE", "apple pie"), nil)
  assert(maki.match.fuzzy("apple", "Apple pie"), "lowercase query is case-insensitive")
end)

case("match_fuzzy_cjk_codepoint_indices", function()
  local m = maki.match.fuzzy("好世", "你好世界")
  assert(m, "expected CJK match")
  eq(#m.indices, 2)
  eq(m.indices[1], 2)
  eq(m.indices[2], 3)
end)

case("match_fuzzy_indices_are_codepoints_not_graphemes", function()
  local m = maki.match.fuzzy("a", "👍🏽abc")
  assert(m, "expected match")
  eq(#m.indices, 1)
  eq(m.indices[1], 3, "codepoint position, not grapheme position")
end)

case("match_fuzzy_contiguous_beats_scattered", function()
  local tight = maki.match.fuzzy("app", "apple")
  local loose = maki.match.fuzzy("app", "axpyp")
  assert(tight and loose, "both must match")
  assert(tight.score > loose.score, "contiguous should score higher")
end)

case("match_fuzzy_repeated_terms_dedup_indices", function()
  local m = maki.match.fuzzy("a a", "banana")
  assert(m, "expected match")
  eq(#m.indices, 1)
  eq(m.indices[1], 2)
end)

case("match_fuzzy_non_string_args_error", function()
  local ok, err = pcall(maki.match.fuzzy, 123, "hello")
  assert(not ok, "number query should error")
  assert(
    tostring(err):find("match.fuzzy: query: expected string, got integer", 1, true),
    "error names the query arg: " .. tostring(err)
  )
  local ok2, err2 = pcall(maki.match.fuzzy, "hello", {})
  assert(not ok2, "table text should error")
  assert(
    tostring(err2):find("match.fuzzy: text: expected string, got table", 1, true),
    "error names the text arg: " .. tostring(err2)
  )
end)

case("match_completion_default_matches_rust_contract", function()
  local m = maki.match.completion("ap", "apple")
  assert(m, "expected completion match")
  eq(m.indices[1], 1)
  eq(m.indices[2], 2)
  eq(m.ranking.quality_rank, 1)
  eq(m.ranking.boundary_rank, 0)
  eq(m.ranking.start_index, 1)
  eq(m.ranking.gap_count, 0)
  eq(m.ranking.span_length, 2)
  eq(m.ranking.unmatched_suffix, 3)
  assert(m.ranking.fuzzy_score >= 0)
end)

case("match_completion_options_and_compare", function()
  assert(maki.match.completion("APPLE", "apple", { case = "ignore" }))
  assert(maki.match.completion("APPLE", "apple", { case = "smart" }) == nil)
  assert(maki.match.completion("a", "A", { case = "respect" }) == nil)
  assert(maki.match.completion("e", "bë", { normalization = "smart" }))
  assert(maki.match.completion("e", "bë", { normalization = "never" }) == nil)

  local exact = maki.match.completion("app", "app")
  local prefix = maki.match.completion("app", "apple")
  local substring = maki.match.completion("app", "xapp")
  assert(maki.match.compare(exact, prefix) < 0)
  assert(maki.match.compare(prefix, exact) > 0)
  assert(maki.match.compare(exact, exact) == 0)
  local results = { substring, exact, prefix }
  table.sort(results, function(a, b)
    return maki.match.compare(a, b) < 0
  end)
  assert(results[1] == exact and results[2] == prefix and results[3] == substring)
end)

case("match_completion_unicode_indices_match_rust_contract", function()
  local cases = {
    { query = "好世", label = "你好世界", expected = { 2, 3 } },
    { query = "e", label = "éclair", expected = { 1 } },
    { query = "b", label = "🇺🇸ab", expected = { 4 } },
    { query = "a", label = "👍🏽abc", expected = { 3 } },
  }
  for _, c in ipairs(cases) do
    local m = maki.match.completion(c.query, c.label)
    assert(m, c.label .. " should match")
    eq(#m.indices, #c.expected, c.label .. " index count")
    for i, expected in ipairs(c.expected) do
      eq(m.indices[i], expected, c.label .. " index " .. i)
    end
  end
end)

case("match_compare_rejects_malformed_operands", function()
  local cases = {
    { 1, {}, "match.compare: left" },
    { {}, {}, "left.ranking" },
    { { ranking = {} }, {}, "left.ranking.quality_rank" },
    { { ranking = { quality_rank = 1 } }, {}, "left.ranking.boundary_rank" },
    {
      { ranking = { quality_rank = 1, boundary_rank = 0, start_index = 0 } },
      {},
      "left.ranking.start_index",
    },
    { { score = 1, indices = {} }, {}, "left.ranking" },
  }
  for _, c in ipairs(cases) do
    local ok, err = pcall(maki.match.compare, c[1], c[2])
    assert(not ok, "malformed operand should error")
    assert(tostring(err):find(c[3], 1, true), tostring(err))
  end
end)

-- ToolView tests

case("tool_view_tail_keeps_last_n", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  for i = 1, 5 do
    view:append("line" .. i)
  end
  eq(#buf.lines, 4) -- 3 ring lines + 1 notice
  eq(buf.lines[1][1][1], "... (2 lines) (click to expand)")
  eq(buf.lines[2], "line3")
  eq(buf.lines[3], "line4")
  eq(buf.lines[4], "line5")
end)

case("tool_view_head_keeps_first_n", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "head" })
  for i = 1, 5 do
    view:append("line" .. i)
  end
  view:finish()
  eq(#buf.lines, 4) -- 3 ring lines + 1 notice
  eq(buf.lines[1], "line1")
  eq(buf.lines[2], "line2")
  eq(buf.lines[3], "line3")
  eq(buf.lines[4][1][1], "... (2 lines) (click to expand)")
end)

case("tool_view_header_appears_first", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 5 })
  view:set_header({ "cmd", { { "---", "dim" } } })
  view:append("output1")
  eq(buf.lines[1], "cmd")
  eq(buf.lines[2][1][1], "---")
  eq(buf.lines[3], "output1")
end)

case("tool_view_ring_wraparound", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  for i = 1, 10 do
    view:append("line" .. i)
  end
  eq(view.skipped, 7)
  eq(buf.lines[1][1][1], "... (7 lines) (click to expand)")
  eq(buf.lines[2], "line8")
  eq(buf.lines[3], "line9")
  eq(buf.lines[4], "line10")
end)

case("tool_view_finish_flushes_head_skipped", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 2, keep = "head" })
  for i = 1, 5 do
    view:append("line" .. i)
  end
  local count_before = buf.call_count
  view:finish()
  assert(buf.call_count > count_before, "finish should flush when head has skipped lines")
  eq(buf.lines[3][1][1], "... (3 lines) (click to expand)")
end)

case("tool_view_no_truncation_within_limit", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 10, keep = "tail" })
  for i = 1, 5 do
    view:append("line" .. i)
  end
  eq(#buf.lines, 5)
  eq(view.skipped, 0)
end)

case("tool_view_toggle_expands_all_lines", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  for i = 1, 10 do
    view:append("line" .. i)
  end
  eq(#buf.lines, 4) -- 3 visible + hidden notice
  view:toggle()
  eq(#buf.lines, 10) -- 10 data lines
  eq(buf.lines[1], "line1")
  eq(buf.lines[10], "line10")
end)

case("tool_view_toggle_twice_collapses_back", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  for i = 1, 10 do
    view:append("line" .. i)
  end
  view:toggle()
  view:toggle()
  eq(#buf.lines, 4)
  eq(buf.lines[1][1][1], "... (7 lines) (click to expand)")
  eq(buf.lines[2], "line8")
end)

case("tool_view_toggle_head_mode_expands", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 2, keep = "head" })
  for i = 1, 5 do
    view:append("line" .. i)
  end
  view:finish()
  eq(buf.lines[3][1][1], "... (3 lines) (click to expand)")
  view:toggle()
  eq(buf.lines[1], "line1")
  eq(buf.lines[5], "line5")
end)

case("tool_view_expand_cap_overflow_shows_omitted", function()
  local buf = mock_buf()
  local cap = 20
  local view = ToolView.new(buf, { max_lines = 2, keep = "tail", max_expand_lines = cap })
  for i = 1, cap + 5 do
    view:append("line" .. i)
  end
  eq(view.all_skipped, 5)
  view:toggle()
  eq(buf.lines[1], "line1")
  eq(buf.lines[cap], "line" .. cap)
  eq(buf.lines[cap + 1][1][1], "5 lines omitted")
end)

case("tool_view_no_collapse_link_when_within_max", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 10, keep = "tail" })
  for i = 1, 5 do
    view:append("line" .. i)
  end
  view:toggle()
  for _, line in ipairs(buf.lines) do
    if type(line) == "table" and line[1] and line[1][1] == "click to collapse" then
      error("should not show collapse link when lines <= max")
    end
  end
end)

case("tool_view_clear_resets_data_but_keeps_expanded", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  for i = 1, 10 do
    view:append("line" .. i)
  end
  view:toggle()
  eq(view.expanded, true)
  view:clear()
  eq(#view.all_lines, 0)
  eq(view.all_skipped, 0)
  eq(view.ring_count, 0)
  eq(view.skipped, 0)
end)

case("tool_view_header_preserved_after_toggle", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  view:set_header({ "$ echo hello", { { "---", "dim" } } })
  for i = 1, 10 do
    view:append("line" .. i)
  end
  view:toggle()
  eq(buf.lines[1], "$ echo hello")
  eq(buf.lines[2][1][1], "---")
  eq(buf.lines[3], "line1")
  eq(buf.lines[12], "line10")
end)

case("tool_view_no_truncate_single_line", function()
  for _, mode in ipairs({ "tail", "head" }) do
    local buf = mock_buf()
    local view = ToolView.new(buf, { max_lines = 3, keep = mode })
    for i = 1, 4 do
      view:append("line" .. i)
    end
    if mode == "head" then
      view:finish()
    end
    eq(#buf.lines, 4, mode .. ": should inline the single skipped line")
    eq(buf.lines[1], "line1", mode)
    eq(buf.lines[4], "line4", mode)
  end
end)

case("tool_view_append_after_toggle_still_works", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  for i = 1, 5 do
    view:append("line" .. i)
  end
  view:toggle()
  view:append("line6")
  eq(view.all_lines[6], "line6")
end)

case("tool_view_max_line_bytes_truncates_string_line", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail", max_line_bytes = 10 })
  view:append(string.rep("a", 20))
  eq(buf.lines[1], string.rep("a", 10) .. "[line truncated]")
  assert(buf.lines[1]:find("%[line truncated%]"), "truncated line should be marked")
end)

case("tool_view_max_line_bytes_truncates_span_line", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail", max_line_bytes = 12 })
  view:append({ { "hello", "dim" }, " ", { "worldoverflow", "error" } })
  eq(buf.lines[1][1][1], "hello")
  eq(buf.lines[1][1][2], "dim")
  eq(buf.lines[1][3][2], "error")
  assert(buf.lines[1][3][1]:find("%[line truncated%]"), "span should be marked truncated")
end)

case("tool_view_max_line_bytes_utf8_safe", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail", max_line_bytes = 10 })
  view:append("éééééééééé")
  eq(buf.lines[1], string.rep("é", 5) .. "[line truncated]")
end)

case("tool_view_max_line_bytes_default_off", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "tail" })
  local long = string.rep("a", 100)
  view:append(long)
  eq(buf.lines[1], long)
end)

local RESTORE_OPTS = { max_lines = 10, keep = "head", width = 80 }
local RESTORE_OUTPUT = "**bold** line\nsecond line"
local RENDERED = "rendered by markdown"

local function all_text(lines)
  local out = {}
  for _, line in ipairs(lines) do
    if type(line) == "string" then
      out[#out + 1] = line
    else
      for _, span in ipairs(line) do
        out[#out + 1] = span[1]
      end
    end
  end
  return table.concat(out, "\n")
end

-- Restores {output} with {markdown} standing in for the real renderer; returns
-- the text that reached the buf and how many times the renderer ran.
local function restore_markdown(output, is_error, markdown)
  local buf = mock_buf()
  local original = { buf = maki.ui.buf, markdown = maki.ui.markdown }
  local calls = 0
  maki.ui.buf = function()
    return buf
  end
  maki.ui.markdown = function(...)
    calls = calls + 1
    return markdown(...)
  end
  local ok, err = pcall(ToolView.restore_markdown, output, is_error, RESTORE_OPTS)
  maki.ui.buf, maki.ui.markdown = original.buf, original.markdown
  assert(ok, tostring(err))
  return all_text(buf.lines), calls
end

local function markdown_marker()
  return { { { RENDERED, "md" } } }
end

case("tool_view_restore_markdown_renders_markdown", function()
  local text, calls = restore_markdown(RESTORE_OUTPUT, false, markdown_marker)
  eq(calls, 1, "output must go through markdown rendering")
  eq(text, RENDERED, "the rendered lines are what reaches the buf")
end)

case("tool_view_restore_markdown_keeps_errors_plain", function()
  local text, calls = restore_markdown(RESTORE_OUTPUT, true, markdown_marker)
  eq(calls, 0, "error output must never reach the markdown renderer")
  eq(text, RESTORE_OUTPUT, "error output must render verbatim")
end)

case("tool_view_restore_markdown_falls_back_when_rendering_fails", function()
  local text = restore_markdown(RESTORE_OUTPUT, false, function()
    error("renderer blew up")
  end)
  eq(text, RESTORE_OUTPUT, "a failing renderer must not swallow the output")
end)

local TextInput = require("maki.text_input")

case("text_input_insert_and_value", function()
  local input = TextInput.new()
  input:handle_key("h")
  input:handle_key("i")
  eq(input:value(), "hi")
  eq(input.col, 2)
end)

case("text_input_backspace_at_start_noop", function()
  local input = TextInput.new()
  input:handle_key("backspace")
  eq(input:value(), "")
  eq(input.col, 0)
end)

case("text_input_backspace_deletes", function()
  local input = TextInput.new()
  input:handle_key("a")
  input:handle_key("b")
  input:handle_key("c")
  input:handle_key("backspace")
  eq(input:value(), "ab")
  eq(input.col, 2)
end)

case("text_input_shift_backspace_deletes", function()
  local input = TextInput.new()
  input:handle_key("a")
  input:handle_key("b")
  input:handle_key("shift+backspace")
  eq(input:value(), "a")
  eq(input.col, 1)
end)

case("text_input_cursor_movement", function()
  local input = TextInput.new()
  input:handle_key("a")
  input:handle_key("b")
  input:handle_key("c")
  input:handle_key("left")
  eq(input.col, 2)
  input:handle_key("left")
  eq(input.col, 1)
  input:handle_key("left")
  eq(input.col, 0)
  input:handle_key("left")
  eq(input.col, 0)
  input:handle_key("right")
  eq(input.col, 1)
  input:handle_key("end")
  eq(input.col, 3)
  input:handle_key("home")
  eq(input.col, 0)
end)

case("text_input_delete_word", function()
  local input = TextInput.new()
  for c in ("hello world"):gmatch(".") do
    input:handle_key(c)
  end
  eq(input:value(), "hello world")
  input:handle_key("ctrl+w")
  eq(input:value(), "hello ", "ctrl+w eats the last word in one press")
  input:handle_key("ctrl+w")
  eq(input:value(), "", "second ctrl+w eats remaining trailing space and word")
end)

local R = TextInput.Result

case("text_input_unknown_key_returns_ignored", function()
  local input = TextInput.new()
  eq(input:handle_key("ctrl+z"), R.IGNORED)
  eq(input:handle_key("f1"), R.IGNORED)
end)

case("text_input_multibyte_key_inserts_single_codepoint", function()
  local input = TextInput.new()
  eq(input:handle_key("你"), R.CHANGED, "single CJK codepoint key is inserted, not ignored by byte length")
  eq(input:value(), "你")
  input:handle_key("好")
  eq(input:value(), "你好")
  input:handle_key("left")
  eq(input:char_before_cursor(), "你")
end)

case("text_input_render_format", function()
  local input = TextInput.new()
  input:handle_key("a")
  input:handle_key("b")
  input:handle_key("left")
  local r = input:render("> ")
  eq(#r.lines, 1)
  eq(r.cursor_row, 1)
  local spans = r.lines[1]
  eq(#spans, 4)
  eq(spans[1][1], "> ")
  eq(spans[1][2], "dim")
  eq(spans[2][1], "a")
  eq(spans[2][2], "")
  eq(spans[3][1], "b")
  eq(spans[3][2], "cursor")
  eq(spans[4][1], "")
  eq(spans[4][2], "")
end)

case("text_input_is_empty", function()
  local input = TextInput.new()
  eq(input:is_empty(), true)
  input:handle_key("x")
  eq(input:is_empty(), false)
end)

case("text_input_utf8_insert_navigate_delete_render", function()
  local input = TextInput.new()
  input:insert_text("héllo — wörld")
  eq(input:value(), "héllo — wörld", "paste preserves multibyte text")
  eq(input:char_before_cursor(), "d", "char_before_cursor over single-byte")

  input = TextInput.new()
  input:insert_text("aé")
  eq(input.col, 3, "cursor at end of 'aé' (1 + 2 bytes)")
  input:handle_key("left")
  eq(input.col, 1, "left jumps over whole codepoint")
  eq(input:char_before_cursor(), "a")
  input:handle_key("right")
  eq(input.col, 3, "right jumps over whole codepoint")
  input:handle_key("backspace")
  eq(input:value(), "a", "backspace removes whole codepoint")

  input = TextInput.new()
  input:insert_text("aé")
  input:handle_key("left")
  local spans = input:render("> ").lines[1]
  eq(spans[2][1], "a", "text before cursor")
  eq(spans[3][1], "é", "cursor span is whole codepoint")
  eq(spans[3][2], "cursor")
end)

case("text_input_insert_text_table_driven", function()
  local cases = {
    { "foo\nbar\nbaz", 3, "foo\nbar\nbaz", 3, 3 },
    { "a\n", 2, "a\n", 2, 0 },
    { "\nx", 2, "\nx", 2, 1 },
    { "\n\n\n", 4, "\n\n\n", 4, 0 },
    { "é\nö\n世界", 3, "é\nö\n世界", 3, #"世界" },
  }
  for i, c in ipairs(cases) do
    local input = TextInput.new()
    input:insert_text(c[1])
    eq(input:line_count(), c[2], "case " .. i .. ": line_count")
    eq(input:value(), c[3], "case " .. i .. ": value")
    eq(input.line, c[4], "case " .. i .. ": line")
    eq(input.col, c[5], "case " .. i .. ": col")
  end
end)

case("text_input_newline_table_driven", function()
  local cases = {
    { { "left", "left" }, "hel\nlo" },
    { { "home" }, "\nhello" },
    { { "end" }, "hello\n" },
  }
  for i, c in ipairs(cases) do
    local input = TextInput.new()
    input:insert_text("hello")
    for _, k in ipairs(c[1]) do
      input:handle_key(k)
    end
    input:handle_key("newline")
    eq(input:value(), c[2], "case " .. i)
    eq(input:line_count(), 2, "case " .. i .. ": two lines")
    eq(input.line, 2, "case " .. i .. ": cursor on new line")
    eq(input.col, 0, "case " .. i .. ": cursor at col 0")
  end
end)

case("text_input_up_down_navigation_clamps_and_no_ops", function()
  local input = TextInput.new()
  input:insert_text("abc\nlonger_line")
  eq(input.line, 2)
  eq(input.col, 11, "cursor at end of longer line")
  input:handle_key("up")
  eq(input.line, 1)
  eq(input.col, 3, "col clamps to short line length")
  input:handle_key("up")
  eq(input.line, 1, "up at line 1 is a no-op")
  input:handle_key("down")
  input:handle_key("down")
  eq(input.line, 2, "down at last line is a no-op")
end)

case("text_input_cursor_wraps_across_line_boundaries", function()
  local input = TextInput.new()
  input:insert_text("abc\nxy")
  input:handle_key("home")
  input:handle_key("left")
  eq(input.line, 1)
  eq(input.col, 3, "left at col 0 lands at end of previous line")
  input:handle_key("right")
  eq(input.line, 2)
  eq(input.col, 0, "right at end of non-last line goes to start of next")
end)

case("text_input_backspace_joins_lines_table_driven", function()
  local cases = {
    { "foo\nbar", { "home" }, "foobar", 1, 3 },
    { "\nabc", { "home" }, "abc", 1, 0 },
    { "a\nb", { "end", "backspace" }, "a", 1, 1 },
  }
  for i, c in ipairs(cases) do
    local input = TextInput.new()
    input:insert_text(c[1])
    for _, k in ipairs(c[2]) do
      input:handle_key(k)
    end
    input:handle_key("backspace")
    eq(input:value(), c[3], "case " .. i .. ": value")
    eq(input.line, c[4], "case " .. i .. ": line")
    eq(input.col, c[5], "case " .. i .. ": col")
    eq(input:line_count(), 1, "case " .. i .. ": joined to one line")
  end
end)

case("text_input_empty_input_movement_is_noop_and_char_before_cursor_is_nil", function()
  local input = TextInput.new()
  for _, k in ipairs({ "left", "right", "up", "down", "backspace" }) do
    input:handle_key(k)
  end
  eq(input:value(), "")
  eq(input:line_count(), 1)
  eq(input.line, 1)
  eq(input.col, 0)
  eq(input:char_before_cursor(), nil, "no char before cursor at start of empty input")

  input = TextInput.new()
  input:insert_text("abc\ndef")
  input:handle_key("home")
  eq(input:char_before_cursor(), nil, "no char before cursor at col 0 on non-first line")
end)

case("text_input_ctrl_w_consumes_trailing_spaces_then_word", function()
  local input = TextInput.new()
  input:insert_text("hello world  ")
  input:handle_key("ctrl+w")
  eq(input:value(), "hello ", "single ctrl+w eats trailing spaces AND the word")
  input:handle_key("ctrl+w")
  eq(input:value(), "", "second ctrl+w eats what is left")
end)

case("text_input_render_multiline_padding_and_cursor", function()
  local input = TextInput.new()
  input:insert_text("line1\nline2")
  local prefix = "> "
  local r = input:render(prefix, #prefix)
  eq(#r.lines, 2, "one render row per logical line")
  eq(r.cursor_row, 2, "cursor on second logical line")
  eq(r.lines[1][1][1], prefix, "first row uses the prefix")
  eq(r.lines[2][1][1], string.rep(" ", #prefix), "continuation rows use blank padding")
  eq(r.lines[1][2][1], "line1", "non-cursor row renders text in one span")
  local saw_cursor
  for _, span in ipairs(r.lines[2]) do
    if span[2] == "cursor" then
      saw_cursor = true
    end
  end
  assert(saw_cursor, "cursor span must appear on the row holding the cursor")
end)

local function span_text(row)
  local parts = {}
  for _, span in ipairs(row) do
    parts[#parts + 1] = span[1]
  end
  return table.concat(parts)
end

local function find_cursor_char(row)
  for _, span in ipairs(row) do
    if span[2] == "cursor" then
      return span[1]
    end
  end
end

case("text_input_render_wraps_long_line", function()
  local input = TextInput.new()
  input:insert_text("abcdefghij")
  local r = input:render("> ", 2, 8)
  eq(#r.lines, 2, "10 chars at usable=6 produces 2 visual rows")
  eq(r.cursor_row, 2, "cursor on last visual row")
  eq(find_cursor_char(r.lines[2]), " ", "cursor at end is a space")
end)

case("text_input_render_wrap_cursor_mid_line", function()
  local input = TextInput.new()
  input:insert_text("abcdefghij")
  for _ = 1, 5 do
    input:handle_key("left")
  end
  local r = input:render("> ", 2, 8)
  eq(#r.lines, 2, "still 2 visual rows")
  eq(r.cursor_row, 1, "cursor in first chunk")
  eq(find_cursor_char(r.lines[1]), "f", "cursor on 'f'")
end)

case("text_input_render_wrap_multiline", function()
  local input = TextInput.new()
  input:insert_text("abcdefghij\n1234567890")
  local r = input:render("> ", 2, 8)
  eq(#r.lines, 4, "each logical line wraps into 2 visual rows")
  eq(r.cursor_row, 4, "cursor on last visual row of second logical line")
end)

case("text_input_render_degenerate_width", function()
  local input = TextInput.new()
  input:insert_text("abc")
  local r = input:render("", 0, 1)
  eq(#r.lines, 3, "usable=1 means one char per visual row")
  eq(r.cursor_row, 3, "cursor on last row")
end)

case("text_input_render_empty_input_with_width", function()
  local input = TextInput.new()
  local r = input:render("> ", 2, 10)
  eq(#r.lines, 1, "empty input still produces one row")
  eq(r.cursor_row, 1, "cursor on that single row")
  eq(find_cursor_char(r.lines[1]), " ", "cursor is a space on empty input")
end)

case("text_input_render_wrap_utf8_multibyte_at_boundary", function()
  local input = TextInput.new()
  input:insert_text("aaéé")
  local r = input:render("", 0, 3)
  eq(#r.lines, 2, "4 codepoints at usable=3 wraps into 2 rows")
  eq(span_text(r.lines[1]), "aaé", "first chunk has 3 codepoints")
  local second = span_text(r.lines[2])
  assert(second:find("é"), "second chunk contains the remaining é")
  eq(r.cursor_row, 2, "cursor on second row")
end)

case("text_input_render_wrap_cursor_at_exact_chunk_boundary", function()
  local input = TextInput.new()
  input:insert_text("abcdef")
  for _ = 1, 3 do
    input:handle_key("left")
  end
  local r = input:render("", 0, 3)
  eq(#r.lines, 2, "6 chars at usable=3 -> 2 rows")
  eq(r.cursor_row, 2, "cursor col=3 lands in second chunk")
  eq(find_cursor_char(r.lines[2]), "d", "cursor char is 'd'")
end)

case("text_input_render_exact_fit_no_extra_row", function()
  local input = TextInput.new()
  input:insert_text("abcdef")
  local r = input:render(">>", 2, 8)
  eq(#r.lines, 1, "6 chars exactly fills usable=6, no extra row")
  eq(r.cursor_row, 1)
end)

case("text_input_render_empty_lines_in_multiline_with_width", function()
  local input = TextInput.new()
  input:insert_text("ab\n\ncd")
  local r = input:render("> ", 2, 10)
  eq(#r.lines, 3, "three logical lines produce three visual rows")
  eq(r.cursor_row, 3, "cursor on last line")
end)

case("text_input_render_cursor_at_start_with_wrapping", function()
  local input = TextInput.new()
  input:insert_text("abcdef")
  input:handle_key("home")
  local r = input:render("", 0, 3)
  eq(r.cursor_row, 1, "cursor at col=0 is in the first chunk")
  eq(find_cursor_char(r.lines[1]), "a", "cursor on first char 'a'")
end)

case("text_input_render_prefix_width_override", function()
  local input = TextInput.new()
  input:insert_text("abcdefgh")
  local r = input:render("X", 4, 8)
  eq(#r.lines, 2, "usable = 8-4 = 4, 8 chars wraps into 2 rows")
  local first = span_text(r.lines[1])
  assert(first:sub(1, 1) == "X", "first row starts with actual prefix 'X'")
  local second = span_text(r.lines[2])
  assert(second:sub(1, 4) == "    ", "continuation uses prefix_width=4 spaces of padding")
end)

case("text_input_invariants_hold_under_random_sequence", function()
  TextInput._debug = true
  local input = TextInput.new()
  local keys = {
    "a",
    "b",
    "c",
    "x",
    "é",
    "space",
    "newline",
    "left",
    "right",
    "up",
    "down",
    "home",
    "end",
    "backspace",
    "delete",
    "ctrl+w",
    "ctrl+left",
    "ctrl+right",
    "ctrl+a",
    "ctrl+k",
    "alt+d",
    "alt+b",
    "alt+f",
  }
  math.randomseed(0xC0FFEE)
  for _ = 1, 2000 do
    local k = keys[math.random(#keys)]
    if k == "é" then
      input:insert_text("é")
    else
      input:handle_key(k)
    end
  end
  TextInput._debug = false
end)

-- Inline parity cases. Each case sets up an initial value/cursor, applies a
-- sequence of keys, and asserts final value+cursor. Add a case here whenever
-- you change handle_key semantics. Lives in Lua now that there is no second
-- implementation to cross-check against; if a Rust TextBuffer comes back,
-- promote this back to a shared golden file.
local TRACE_CASES = {
  {
    name = "plain_insert",
    initial = "",
    cur = { 1, 0 },
    keys = { "h", "i" },
    final_value = "hi",
    final_cur = { 1, 2 },
  },
  {
    name = "backspace_deletes_char",
    initial = "abc",
    cur = { 1, 3 },
    keys = { "backspace" },
    final_value = "ab",
    final_cur = { 1, 2 },
  },
  {
    name = "delete_at_end_joins_lines",
    initial = "ab\ncd",
    cur = { 1, 2 },
    keys = { "delete" },
    final_value = "abcd",
    final_cur = { 1, 2 },
  },
  {
    name = "backspace_at_line_start_joins",
    initial = "ab\ncd",
    cur = { 2, 0 },
    keys = { "backspace" },
    final_value = "abcd",
    final_cur = { 1, 2 },
  },
  {
    name = "left_then_right_round_trips",
    initial = "abc",
    cur = { 1, 2 },
    keys = { "left", "right" },
    final_value = "abc",
    final_cur = { 1, 2 },
  },
  {
    name = "right_wraps_to_next_line",
    initial = "ab\ncd",
    cur = { 1, 2 },
    keys = { "right" },
    final_value = "ab\ncd",
    final_cur = { 2, 0 },
  },
  {
    name = "left_wraps_to_prev_line",
    initial = "ab\ncd",
    cur = { 2, 0 },
    keys = { "left" },
    final_value = "ab\ncd",
    final_cur = { 1, 2 },
  },
  {
    name = "home_jumps_to_col_zero",
    initial = "hello",
    cur = { 1, 5 },
    keys = { "home" },
    final_value = "hello",
    final_cur = { 1, 0 },
  },
  {
    name = "end_jumps_to_line_length",
    initial = "hello",
    cur = { 1, 0 },
    keys = { "end" },
    final_value = "hello",
    final_cur = { 1, 5 },
  },
  {
    name = "up_clamps_to_short_line",
    initial = "abc\nlonger_line",
    cur = { 2, 11 },
    keys = { "up" },
    final_value = "abc\nlonger_line",
    final_cur = { 1, 3 },
  },
  {
    name = "down_moves_to_next_line",
    initial = "ab\ncd",
    cur = { 1, 0 },
    keys = { "down" },
    final_value = "ab\ncd",
    final_cur = { 2, 0 },
  },
  {
    name = "ctrl_left_jumps_word",
    initial = "hello world",
    cur = { 1, 11 },
    keys = { "ctrl+left" },
    final_value = "hello world",
    final_cur = { 1, 6 },
  },
  {
    name = "ctrl_left_twice_lands_at_zero",
    initial = "hello world",
    cur = { 1, 11 },
    keys = { "ctrl+left", "ctrl+left" },
    final_value = "hello world",
    final_cur = { 1, 0 },
  },
  {
    name = "ctrl_right_jumps_word",
    initial = "hello world",
    cur = { 1, 0 },
    keys = { "ctrl+right" },
    final_value = "hello world",
    final_cur = { 1, 5 },
  },
  {
    name = "ctrl_right_eats_leading_spaces_then_word",
    initial = "hello  ",
    cur = { 1, 0 },
    keys = { "ctrl+right" },
    final_value = "hello  ",
    final_cur = { 1, 5 },
  },
  {
    name = "ctrl_left_eats_leading_spaces_then_word",
    initial = "  hello",
    cur = { 1, 7 },
    keys = { "ctrl+left" },
    final_value = "  hello",
    final_cur = { 1, 2 },
  },
  {
    name = "ctrl_w_eats_trailing_spaces_and_word",
    initial = "hello world  ",
    cur = { 1, 13 },
    keys = { "ctrl+w" },
    final_value = "hello ",
    final_cur = { 1, 6 },
  },
  {
    name = "ctrl_w_twice_clears_input",
    initial = "hello world",
    cur = { 1, 11 },
    keys = { "ctrl+w", "ctrl+w" },
    final_value = "",
    final_cur = { 1, 0 },
  },
  {
    name = "ctrl_w_at_line_start_joins",
    initial = "ab\ncd",
    cur = { 2, 0 },
    keys = { "ctrl+w" },
    final_value = "abcd",
    final_cur = { 1, 2 },
  },
  {
    name = "ctrl_delete_eats_word_after",
    initial = "hello world",
    cur = { 1, 0 },
    keys = { "ctrl+delete" },
    final_value = " world",
    final_cur = { 1, 0 },
  },
  {
    name = "alt_d_eats_word_after_space",
    initial = "hello world",
    cur = { 1, 6 },
    keys = { "alt+d" },
    final_value = "hello ",
    final_cur = { 1, 6 },
  },
  {
    name = "ctrl_delete_at_line_end_joins",
    initial = "ab\ncd",
    cur = { 1, 2 },
    keys = { "ctrl+delete" },
    final_value = "abcd",
    final_cur = { 1, 2 },
  },
  {
    name = "ctrl_k_truncates_line",
    initial = "hello world",
    cur = { 1, 5 },
    keys = { "ctrl+k" },
    final_value = "hello",
    final_cur = { 1, 5 },
  },
  {
    name = "ctrl_k_at_line_end_joins",
    initial = "ab\ncd",
    cur = { 1, 2 },
    keys = { "ctrl+k" },
    final_value = "ab\ncd",
    final_cur = { 1, 2 },
  },
  {
    name = "ctrl_a_moves_home",
    initial = "hello",
    cur = { 1, 5 },
    keys = { "ctrl+a" },
    final_value = "hello",
    final_cur = { 1, 0 },
  },
  {
    name = "alt_b_aliases_ctrl_left",
    initial = "hello world",
    cur = { 1, 11 },
    keys = { "alt+b" },
    final_value = "hello world",
    final_cur = { 1, 6 },
  },
  {
    name = "alt_f_aliases_ctrl_right",
    initial = "hello world",
    cur = { 1, 0 },
    keys = { "alt+f" },
    final_value = "hello world",
    final_cur = { 1, 5 },
  },
  {
    name = "newline_splits_line",
    initial = "abcd",
    cur = { 1, 2 },
    keys = { "newline" },
    final_value = "ab\ncd",
    final_cur = { 2, 0 },
  },
  {
    name = "space_inserts_a_space",
    initial = "abcd",
    cur = { 1, 2 },
    keys = { "space" },
    final_value = "ab cd",
    final_cur = { 1, 3 },
  },
  {
    name = "utf8_left_over_multibyte",
    initial = "aé",
    cur = { 1, 3 },
    keys = { "left" },
    final_value = "aé",
    final_cur = { 1, 1 },
  },
  {
    name = "utf8_backspace_removes_codepoint",
    initial = "aé",
    cur = { 1, 3 },
    keys = { "backspace" },
    final_value = "a",
    final_cur = { 1, 1 },
  },
  {
    name = "utf8_ctrl_w_eats_multibyte_word",
    initial = "hello wörld",
    cur = { 1, 12 },
    keys = { "ctrl+w" },
    final_value = "hello ",
    final_cur = { 1, 6 },
  },
  {
    name = "tab_is_whitespace_for_ctrl_w",
    initial = "hello\tworld",
    cur = { 1, 11 },
    keys = { "ctrl+w" },
    final_value = "hello\t",
    final_cur = { 1, 6 },
  },
  {
    name = "ignored_backspace_at_buffer_start",
    initial = "",
    cur = { 1, 0 },
    keys = { "backspace" },
    final_value = "",
    final_cur = { 1, 0 },
    results = { R.IGNORED },
  },
  {
    name = "ignored_left_at_buffer_start",
    initial = "abc",
    cur = { 1, 0 },
    keys = { "left" },
    final_value = "abc",
    final_cur = { 1, 0 },
    results = { R.IGNORED },
  },
  {
    name = "ignored_right_at_buffer_end",
    initial = "abc",
    cur = { 1, 3 },
    keys = { "right" },
    final_value = "abc",
    final_cur = { 1, 3 },
    results = { R.IGNORED },
  },
  {
    name = "ignored_up_on_first_line",
    initial = "abc",
    cur = { 1, 1 },
    keys = { "up" },
    final_value = "abc",
    final_cur = { 1, 1 },
    results = { R.IGNORED },
  },
  {
    name = "ignored_down_on_last_line",
    initial = "abc",
    cur = { 1, 1 },
    keys = { "down" },
    final_value = "abc",
    final_cur = { 1, 1 },
    results = { R.IGNORED },
  },
  {
    name = "ignored_ctrl_w_at_buffer_start",
    initial = "abc",
    cur = { 1, 0 },
    keys = { "ctrl+w" },
    final_value = "abc",
    final_cur = { 1, 0 },
    results = { R.IGNORED },
  },
  {
    name = "ignored_delete_at_buffer_end",
    initial = "abc",
    cur = { 1, 3 },
    keys = { "delete" },
    final_value = "abc",
    final_cur = { 1, 3 },
    results = { R.IGNORED },
  },
}

case("text_input_trace_cases", function()
  for _, c in ipairs(TRACE_CASES) do
    local input = TextInput.new()
    input:insert_text(c.initial)
    input.line, input.col = c.cur[1], c.cur[2]
    local got = {}
    for _, k in ipairs(c.keys) do
      got[#got + 1] = input:handle_key(k)
    end
    eq(input:value(), c.final_value, c.name .. ": value")
    eq(input.line, c.final_cur[1], c.name .. ": cursor line")
    eq(input.col, c.final_cur[2], c.name .. ": cursor col")
    if c.results then
      for i, want in ipairs(c.results) do
        eq(got[i], want, c.name .. ": key " .. i .. " result")
      end
    end
  end
end)

case("set_highlight_number_width_scales", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 200 })
  local lines = {}
  for i = 1, 100 do
    lines[i] = "x"
  end
  local content = table.concat(lines, "\n")
  local ok = view:set_highlight(content, "txt")
  eq(ok, true)
  eq(view.ring_count, 100)
  local first_nr = buf.lines[1][1][1]
  local last_nr = buf.lines[100][1][1]
  eq(first_nr, "  1 ", "3-digit width for 100 lines, right-aligned")
  eq(last_nr, "100 ", "line 100 should fill the width")
  eq(buf.lines[1][1][2], "line_nr")
end)

case("set_highlight_empty_content_returns_false", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3 })
  eq(view:set_highlight("", "txt"), false)
  eq(view:set_highlight("\n", "txt"), false)
  eq(buf.lines, nil, "nothing flushed for empty content")
end)

case("set_highlight_toggle_keeps_lines_and_collapses_back", function()
  local buf = mock_buf()
  local view = ToolView.new(buf, { max_lines = 3, keep = "head" })
  eq(view:set_highlight("a\nb\nc\nd\ne", "txt"), true)

  view:toggle()
  eq(view.expanded, true)
  eq(#buf.lines, 5, "expanded renders every line")
  eq(buf.lines[1][2][1], "a")
  eq(buf.lines[5][2][1], "e")

  view:toggle()
  eq(view.expanded, false)
  eq(buf.lines[3][2][1], "c", "collapsed shows the head window")
  eq(buf.lines[4][1][1], "... (2 lines) (click to expand)")
end)

local Spans = require("maki.spans")

case("match_spans_ascii", function()
  local spans = Spans.match_spans("alphabet", { 1, 4, 8 }, "item", "match")
  eq(#spans, 5)
  eq(spans[1][1], "a")
  eq(spans[1][2], "match")
  eq(spans[2][1], "lp")
  eq(spans[2][2], "item")
  eq(spans[3][1], "h")
  eq(spans[3][2], "match")
  eq(spans[4][1], "abe")
  eq(spans[4][2], "item")
  eq(spans[5][1], "t")
  eq(spans[5][2], "match")
end)

case("match_spans_cjk", function()
  local spans = Spans.match_spans("你好世界", { 2, 3 }, "item", "match")
  eq(#spans, 3, "adjacent CJK codepoints merge, bytes line up")
  eq(spans[1][1], "你")
  eq(spans[1][2], "item")
  eq(spans[2][1], "好世")
  eq(spans[2][2], "match")
  eq(spans[3][1], "界")
  eq(spans[3][2], "item")
end)

case("match_spans_no_indices_single_span", function()
  local spans = Spans.match_spans("hello", {}, "item", "match")
  eq(#spans, 1)
  eq(spans[1][1], "hello")
  eq(spans[1][2], "item")
end)

case("match_spans_adjacent", function()
  local spans = Spans.match_spans("review pr 441", { 8, 9, 11, 12, 13 }, "item", "match")
  eq(#spans, 4)
  eq(spans[1][1], "review ")
  eq(spans[1][2], "item")
  eq(spans[2][1], "pr")
  eq(spans[2][2], "match")
  eq(spans[3][1], " ")
  eq(spans[3][2], "item")
  eq(spans[4][1], "441")
  eq(spans[4][2], "match")
end)

local SPEC_HEADINGS = {
  "## Goal",
  "## Implementation Summary",
  "## Implementation Plan",
  "## Acceptance Criteria",
  "## Test Strategy",
  "## Review Strategy",
  "## Documentation Strategy",
  "## Risks, Blockers, and Required Decisions",
}

case("plan_spec_is_shared_source_with_all_headings", function()
  local spec = require("maki.plan_spec")
  assert(type(spec) == "string" and #spec > 0, "maki.plan_spec must be a non-empty string")
  for _, heading in ipairs(SPEC_HEADINGS) do
    eq(spec:find(heading, 1, true) ~= nil, true, "plan spec missing heading: " .. heading)
  end
end)

local long_lines = require("maki.long_lines")
local LL_MAX = 10
local LONG = string.rep("L", 20)
local OTHER_LONG = string.rep("M", 20)

local function long_line_err(line_nr, size)
  return string.format(long_lines.LONG_LINE_CHANGED, line_nr, size, LL_MAX)
end

local function marker_err(line_nr)
  return string.format(long_lines.TRUNCATED_MARKER_ADDED.read, line_nr)
end

case("check_write_keeps_unchanged_long_line", function()
  eq(long_lines.check_write("a\n" .. LONG .. "\nb", "x\n" .. LONG .. "\ny", LL_MAX), nil)
end)

case("check_write_allows_moved_long_line", function()
  eq(long_lines.check_write(LONG .. "\na\n", "a\n" .. LONG .. "\n", LL_MAX), nil)
end)

case("check_write_rejects_altered_long_line", function()
  eq(long_lines.check_write("a\n" .. LONG, "a\n" .. LONG:sub(1, 15), LL_MAX), long_line_err(2, 20))
end)

case("check_write_duplicate_long_line_collapse_rejected", function()
  eq(long_lines.check_write(LONG .. "\n" .. LONG, LONG, LL_MAX), long_line_err(2, 20))
end)

case("check_write_stripped_prefix_of_duplicate_rejected", function()
  eq(long_lines.check_write(LONG .. "\n" .. LONG, LONG .. "\n" .. LONG:sub(1, 12), LL_MAX), long_line_err(2, 20))
end)

case("check_write_ignores_short_lines", function()
  eq(long_lines.check_write("short\nlines", "", LL_MAX), nil)
end)

case("check_write_crlf_matches_lf", function()
  eq(long_lines.check_write(LONG .. "\r\nb\r\n", LONG .. "\nb\n", LL_MAX), nil)
end)

case("check_write_multibyte_counts_bytes", function()
  local wide = string.rep("é", 6)
  eq(long_lines.check_write(wide, "", LL_MAX), long_line_err(1, 12))
end)

case("check_replace_lines_allows_deletion", function()
  eq(long_lines.check_replace_lines("a\n" .. LONG .. "\nb", 2, 2, "", LL_MAX), nil)
end)

case("check_replace_lines_rejects_dropped_long_line", function()
  eq(long_lines.check_replace_lines("a\n" .. LONG .. "\nb", 1, 3, "a\nb", LL_MAX), long_line_err(2, 20))
end)

case("check_replace_lines_keeps_long_line", function()
  eq(long_lines.check_replace_lines("a\n" .. LONG .. "\nb", 1, 3, "A\n" .. LONG .. "\nB", LL_MAX), nil)
end)

case("check_replace_lines_only_checks_range", function()
  eq(long_lines.check_replace_lines(LONG .. "\na\nb", 2, 3, "x", LL_MAX), nil)
end)

case("check_replace_lines_duplicate_collapse_rejected", function()
  eq(long_lines.check_replace_lines(LONG .. "\n" .. LONG, 1, 2, LONG, LL_MAX), long_line_err(2, 20))
end)

local MARKED = "abc" .. "[line truncated, +42 bytes]"
local LEGACY_MARKED = "abc[line truncated]"

case("check_markers_rejects_added_marker", function()
  eq(long_lines.check_markers("a", "a\n" .. MARKED), marker_err(2))
end)

case("check_markers_rejects_added_legacy_marker", function()
  eq(long_lines.check_markers("", LEGACY_MARKED), marker_err(1))
end)

case("check_markers_keeps_existing_marker_line", function()
  eq(long_lines.check_markers(MARKED .. "\nold", MARKED .. "\nnew"), nil)
end)

case("check_markers_duplicate_rejected", function()
  eq(long_lines.check_markers(MARKED, MARKED .. "\n" .. MARKED), marker_err(2))
end)

case("check_markers_changed_marker_line_rejected", function()
  eq(long_lines.check_markers(MARKED, "xyz[line truncated, +42 bytes]"), marker_err(1))
end)

case("check_markers_removing_marker_allowed", function()
  eq(long_lines.check_markers(MARKED, "abc"), nil)
end)

case("check_markers_rejects_preview_cut_marker", function()
  eq(
    long_lines.check_markers("", "abc[line cut: first 3 of 900 bytes]"),
    string.format(long_lines.TRUNCATED_MARKER_ADDED.preview, 1)
  )
end)

case("check_markers_rejects_marker_with_trailing_space", function()
  eq(long_lines.check_markers("", MARKED .. "  "), marker_err(1))
end)

case("check_markers_rejects_unicode_trailing_whitespace", function()
  local suffixes = {
    utf8.char(0xA0),
    utf8.char(0x2003),
    utf8.char(0x202F),
    " \t" .. utf8.char(0xA0, 0x2003, 0x202F) .. "\t ",
  }
  for _, suffix in ipairs(suffixes) do
    eq(long_lines.check_markers("", MARKED .. suffix), marker_err(1))
  end
end)

case("check_markers_preserves_unicode_suffixed_literals_and_rejects_duplicates", function()
  local suffix = " \t" .. utf8.char(0xA0, 0x2003, 0x202F) .. "\t "
  local legacy = LEGACY_MARKED .. suffix
  local preview = "abc[line cut: first 3 of 900 bytes]" .. suffix
  local preview_err = string.format(long_lines.TRUNCATED_MARKER_ADDED.preview, 1)
  eq(long_lines.check_markers(MARKED .. suffix, MARKED .. suffix), nil)
  eq(long_lines.check_markers(legacy .. "\nold", legacy .. "\nnew"), nil)
  eq(long_lines.check_markers(MARKED .. suffix, MARKED .. suffix .. "\n" .. MARKED .. suffix), marker_err(2))
  eq(long_lines.check_markers("", legacy), marker_err(1))
  eq(long_lines.check_markers("", preview), preview_err)
  eq(
    long_lines.check_markers(preview, preview .. "\n" .. preview),
    string.format(long_lines.TRUNCATED_MARKER_ADDED.preview, 2)
  )
  eq(long_lines.check_markers("", "abc[line truncated, +oops bytes]" .. suffix), nil)
  eq(long_lines.check_markers("", "abc[ordinary brackets]" .. suffix), nil)
end)

case("check_markers_ignores_marker_mid_line", function()
  eq(long_lines.check_markers("", "x [line truncated] y"), nil)
end)

case("check_markers_error_names_category", function()
  local err = long_lines.check_markers("", MARKED)
  eq(err, marker_err(1))
  assert(err:find("read or grep output", 1, true), err)
end)

case("check_write_other_long_line_unaffected", function()
  eq(long_lines.check_write(LONG .. "\n" .. OTHER_LONG, OTHER_LONG .. "\n" .. LONG, LL_MAX), nil)
end)

case("synthetic_quota_priority_and_clamping", function()
  local synthetic = require("maki.synthetic")
  local usage = assert(
    synthetic.parse_quotas(
      [[{"subscription":{"requests":1,"limit":10},"rollingFiveHourLimit":{"remaining":25,"max":100,"nextTickAt":"reset"},"weeklyTokenLimit":{"percentRemaining":-10}}]],
      function(value)
        return value == "reset" and 123 or nil
      end
    )
  )
  eq(usage.limits[1].percentage, 75)
  eq(usage.limits[1].reset_at, 123)
  eq(usage.limits[2].percentage, 100)
  local legacy = assert(synthetic.parse_quotas([[{"subscription":{"requests":1,"limit":4}}]]))
  eq(legacy.limits[1].percentage, 25)
  local invalid = assert(synthetic.parse_quotas([[{"subscription":{"requests":1,"limit":0}}]]))
  eq(invalid.limits[1].percentage, nil)
  local result, err = synthetic.parse_quotas([[{}]])
  eq(result, nil)
  eq(err.kind, "config")
end)

case("synthetic_rejects_malformed_lanes", function()
  local synthetic = require("maki.synthetic")
  for _, body in ipairs({
    [[{"rollingFiveHourLimit":{"remaining":1}}]],
    [[{"subscription":{"requests":"1","limit":4}}]],
    [[{"weeklyTokenLimit":{"percentRemaining":false}}]],
    [[{"rollingFiveHourLimit":{"remaining":1,"max":4},"subscription":{"requests":1}}]],
    [[{"weeklyTokenLimit":{"percentRemaining":10,"nextRegenAt":42}}]],
    [[{"rollingFiveHourLimit":false}]],
  }) do
    local result, err = synthetic.parse_quotas(body)
    eq(result, nil)
    eq(err.kind, "api")
  end
end)

case("synthetic_quota_boundaries_and_resets", function()
  local synthetic = require("maki.synthetic")
  local usage = assert(
    synthetic.parse_quotas(
      [[{"rollingFiveHourLimit":{"remaining":200,"max":100,"nextTickAt":"bad"},"weeklyTokenLimit":{"percentRemaining":33.5,"nextRegenAt":"week"}}]],
      function(value)
        return value == "week" and 456 or nil
      end
    )
  )
  eq(#usage.limits, 2)
  eq(usage.limits[1].kind.type, "hours")
  eq(usage.limits[1].kind.hours, 5)
  eq(usage.limits[1].percentage, 0)
  eq(usage.limits[1].reset_at, nil)
  eq(usage.limits[2].kind.type, "weekly")
  eq(usage.limits[2].percentage, 67)
  eq(usage.limits[2].reset_at, 456)
  eq(#usage.by_model_today, 0)

  local weekly = assert(synthetic.parse_quotas([[{"weeklyTokenLimit":{"percentRemaining":150}}]]))
  eq(#weekly.limits, 1)
  eq(weekly.limits[1].percentage, 0)
  local legacy =
    assert(synthetic.parse_quotas([[{"subscription":{"requests":3,"limit":2,"renewsAt":"reset"}}]], function()
      return 789
    end))
  eq(legacy.limits[1].percentage, 100)
  eq(legacy.limits[1].reset_at, 789)
end)

case("synthetic_quota_invalid_json_and_shapes", function()
  local synthetic = require("maki.synthetic")
  for _, body in ipairs({
    "{",
    "true",
    "42",
    [[{"subscription":[]}]],
    [[{"weeklyTokenLimit":{"percentRemaining":1,"nextRegenAt":false}}]],
  }) do
    local result, err = synthetic.parse_quotas(body)
    eq(result, nil)
    eq(err.kind, "api")
    assert(type(err.message) == "string" and #err.message > 0)
  end
end)

case("synthetic_effort_policy", function()
  local synthetic = require("maki.synthetic")
  for _, test in ipairs({
    { { mode = "off" }, nil },
    { { mode = "adaptive" }, "medium" },
    { { mode = "effort", level = "xhigh" }, "high" },
    { { mode = "effort", level = "max" }, "high" },
    { { mode = "budget", tokens = 6554 }, "low" },
    { { mode = "budget", tokens = 65536 }, "high" },
  }) do
    local body = {}
    assert(synthetic.encode_thinking(body, test[1], 65536))
    eq(body.reasoning_effort, test[2])
  end
end)

case("synthetic_key_rotation_and_header_override", function()
  local synthetic = require("maki.synthetic")
  local instance = assert(synthetic.prepare(nil, { id = "fixture" }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = " first, , second ",
    headers = { authorization = "gateway" },
  }))
  eq(instance.key_count, 2)
  local captured = {}
  local ctx = {
    send = function(_, request)
      captured[#captured + 1] = request.headers
      return nil, { kind = "network", message = "fixture" }
    end,
  }
  local request = {
    model = { id = "fixture", max_output_tokens = 100 },
    messages = {},
    tools = {},
    system = "",
    thinking = { mode = "off" },
  }
  instance.stream(ctx, request, function() end)
  assert(instance.rotate_key())
  instance.stream(ctx, request, function() end)
  eq(captured[1].authorization, "gateway")
  eq(captured[2].authorization, "gateway")
  eq(captured[1].Authorization, nil)
  local direct = assert(synthetic.prepare(nil, { id = "fixture" }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = "first, second",
  }))
  direct.stream(ctx, request, function() end)
  assert(direct.rotate_key())
  direct.stream(ctx, request, function() end)
  eq(captured[3].Authorization, "Bearer first")
  eq(captured[4].Authorization, "Bearer second")
end)

case("synthetic_stored_credentials_and_logout", function()
  local synthetic = require("maki.synthetic")
  local saved
  local store = {
    read = function(_, key)
      eq(key, "api_key")
      return saved
    end,
    write = function(_, key, value)
      eq(key, "api_key")
      saved = value
      return true
    end,
    delete = function(_, key)
      eq(key, "api_key")
      saved = nil
      return true
    end,
  }
  local ctx = { credentials = store }
  local config = { api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY", api_key = "config-key" }
  eq(synthetic.auth_status(ctx, config).source, "config")
  assert(synthetic.login(ctx, {
    input = function(_, _, secret, optional)
      assert(secret and not optional)
      return " saved-key "
    end,
  }))
  eq(synthetic.auth_status(ctx, config).source, "stored")
  local instance = assert(synthetic.prepare(ctx, { id = "fixture" }, config))
  local authorization
  instance.stream({
    send = function(_, request)
      authorization = request.headers.Authorization
      return nil, { kind = "network", message = "fixture" }
    end,
  }, {
    model = { id = "fixture", max_output_tokens = 100 },
    messages = {},
    tools = {},
    system = "",
    thinking = { mode = "off" },
  }, function() end)
  eq(authorization, "Bearer saved-key")
  assert(synthetic.logout(ctx))
  eq(synthetic.auth_status(ctx, config).source, "config")
  eq(synthetic.auth_status(ctx, { api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY" }).configured, false)
end)

case("synthetic_discovers_models_with_configured_auth", function()
  local synthetic = require("maki.synthetic")
  local sent, closed = nil, false
  local chunks = { [[{"data":[{"id":"z"},{"id":"a","context_length":123,"input_modalities":["image"]}]}]], "" }
  local models = assert(synthetic.discover({
    send = function(_, request)
      sent = request
      return {
        status = 200,
        read = function()
          return table.remove(chunks, 1)
        end,
        close = function()
          closed = true
        end,
      }
    end,
  }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = "first, second",
    base_url = "https://fixture.invalid/v1/",
    headers = { authorization = "gateway" },
  }))
  eq(sent.url, "https://fixture.invalid/v1/models")
  eq(sent.method, "GET")
  eq(sent.headers.authorization, "gateway")
  eq(sent.headers.Authorization, nil)
  eq(models[1].id, "a")
  eq(models[1].context_window, 123)
  eq(models[1].vision, true)
  eq(models[2].id, "z")
  assert(closed)
end)

case("synthetic_discovery_closes_http_errors", function()
  local synthetic = require("maki.synthetic")
  local closed = false
  local result, err = synthetic.discover({
    send = function()
      return {
        status = 401,
        close = function()
          closed = true
        end,
      }
    end,
  }, { api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY", api_key = "key" })
  eq(result, nil)
  eq(err.status, 401)
  assert(closed)
end)

case("synthetic_fetch_usage_closes_response", function()
  local synthetic = require("maki.synthetic")
  local instance = assert(synthetic.prepare(nil, { id = "fixture" }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = "fixture-key",
  }))
  local sent, closed = nil, false
  local chunks = {
    [[{"subscription":{"requests":1,"limit":4,"renewsAt":"2025-01-01T01:00:00+01:00"},"weeklyTokenLimit":{"percentRemaining":50,"nextRegenAt":"invalid"}}]],
    "",
  }
  local usage = assert(instance.fetch_usage({
    send = function(_, request)
      sent = request
      return {
        status = 200,
        read = function()
          return table.remove(chunks, 1)
        end,
        close = function()
          closed = true
        end,
      }
    end,
  }))
  eq(sent.method, "GET")
  eq(sent.url, "https://api.synthetic.new/v2/quotas")
  eq(sent.headers.Authorization, "Bearer fixture-key")
  eq(usage.limits[1].percentage, 25)
  eq(usage.limits[1].reset_at, 1735689600000)
  eq(usage.limits[2].reset_at, nil)
  assert(closed)
end)

case("synthetic_stream_uses_provider_context", function()
  local synthetic = require("maki.synthetic")
  local instance = assert(synthetic.prepare(nil, { id = "fixture" }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = "fixture-key",
    base_url = "https://fixture.invalid/v1/",
  }))
  local sent, closed, events = nil, false, {}
  local chunks = {
    'data: {"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}\n\n',
    "data: [DONE]\n\n",
    "",
  }
  local ctx = {
    send = function(_, request)
      sent = request
      return {
        status = 200,
        read = function()
          return table.remove(chunks, 1)
        end,
        close = function()
          closed = true
          return true
        end,
      }
    end,
  }
  local result = assert(instance.stream(ctx, {
    model = { id = "fixture", max_output_tokens = 1000 },
    messages = {},
    tools = {},
    system = "system",
    thinking = { mode = "off" },
  }, function(event)
    events[#events + 1] = event
    return true
  end))
  eq(sent.url, "https://fixture.invalid/v1/chat/completions")
  eq(sent.headers.Authorization, "Bearer fixture-key")
  eq(assert(maki.json.decode(sent.body)).max_completion_tokens, 1000)
  eq(events[1].text, "answer")
  eq(result.message.content[1].text, "answer")
  assert(closed)
end)

case("synthetic_api_error_does_not_expose_response_body", function()
  local synthetic = require("maki.synthetic")
  local instance = assert(synthetic.prepare(nil, { id = "fixture" }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = "fixture-key",
  }))
  local closed = false
  local result, err = instance.stream({
    send = function()
      return {
        status = 401,
        read = function()
          return "fixture-key"
        end,
        close = function()
          closed = true
        end,
      }
    end,
  }, {
    model = { id = "fixture" },
    messages = {},
    tools = {},
    thinking = { mode = "off" },
  }, function()
    return true
  end)
  eq(result, nil)
  eq(err.kind, "api")
  eq(err.status, 401)
  eq(err.message, "Synthetic request failed")
  assert(closed)
end)

case("synthetic_stream_error_does_not_expose_response_body", function()
  local synthetic = require("maki.synthetic")
  local instance = assert(synthetic.prepare(nil, { id = "fixture" }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = "fixture-key",
  }))
  local result, err = instance.stream({
    send = function()
      return {
        status = 200,
        read = function()
          return 'data: {"error":{"code":401,"message":"fixture-key"}}\n\n'
        end,
        close = function() end,
      }
    end,
  }, {
    model = { id = "fixture" },
    messages = {},
    tools = {},
    thinking = { mode = "off" },
  }, function()
    return true
  end)
  eq(result, nil)
  eq(err.kind, "api")
  eq(err.status, 401)
  eq(err.message, "Synthetic request failed")
end)

case("synthetic_stream_preserves_transport_errors", function()
  local synthetic = require("maki.synthetic")
  local instance = assert(synthetic.prepare(nil, { id = "fixture" }, {
    api_key_env = "MAKI_TEST_SYNTHETIC_MISSING_KEY",
    api_key = "fixture-key",
  }))
  local failure = { kind = "network", message = "transport failed" }
  local result, err = instance.stream({
    send = function()
      return nil, failure
    end,
  }, {
    model = { id = "fixture" },
    messages = {},
    tools = {},
    thinking = { mode = "off" },
  }, function()
    return true
  end)
  eq(result, nil)
  eq(err, failure)
end)

if #failures > 0 then
  error(#failures .. " case(s) failed:\n\n" .. table.concat(failures, "\n\n"))
end
