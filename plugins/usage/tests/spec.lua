local h = require("usage_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq
local has = th.has

local function text(line)
  local parts = {}
  for _, span in ipairs(line) do
    parts[#parts + 1] = span[1]
  end
  return table.concat(parts)
end

local function ready(limits)
  return { state = "ready", usage = { plan = "pro", limits = limits } }
end

case("format_tokens_is_compact", function()
  eq(h.format_tokens(999), "999")
  eq(h.format_tokens(1250), "1.2k")
  eq(h.format_tokens(12500), "12k")
  eq(h.format_tokens(1250000), "1.2m")
end)

case("kind_normalizes_serde_enum_shapes", function()
  local label, short = h.kind({ Hours = 5 })
  eq(label, "5-hour usage")
  eq(short, "5h")
  label, short = h.kind({ Weekly = { model = "Sonnet" } })
  eq(label, "Current week (Sonnet)")
  eq(short, "w")
  label, short = h.kind("Credits")
  eq(label, "Usage credits")
  eq(short, "cr")
end)

case("normalize_accepts_snapshot_tagged_and_direct_ready_states", function()
  local tag, value = h.normalize({ status = "ready", limits = {} })
  eq(tag, "ready")
  eq(type(value), "table")
  tag, value = h.normalize({ Ready = { limits = {} } })
  eq(tag, "ready")
  eq(type(value), "table")
  tag, value = h.normalize({ limits = { { kind = "Credits" } } })
  eq(tag, "ready")
  eq(#value.limits, 1)
end)

case("compact_status_skips_non_ready_and_percentless_lanes", function()
  eq(#h.status_content({ state = "loading" }), 0)
  local spans = h.status_content({
    provider = "Anthropic",
    status = "ready",
    limits = {
      { window = { kind = "hours", value = 5 }, percentage = 30 },
      { window = { kind = "credits" }, detail = "$2 spent" },
      { window = { kind = "weekly", model = nil }, percentage = 90 },
    },
  })
  eq(#spans, 3)
  eq(spans[1][1], "5h30%")
  eq(spans[1][2], "#8a84ce")
  eq(spans[3][1], "w90%")
  eq(spans[3][2], "#ee616c")
end)

case("status_gradient_has_exact_low_mid_high_colors", function()
  local spans = h.status_content(ready({
    { window = { kind = "hours", value = 1 }, percentage = 0 },
    { window = { kind = "hours", value = 1 }, percentage = 50 },
    { window = { kind = "hours", value = 1 }, percentage = 100 },
  }))
  eq(spans[1][2], "#5896ff")
  eq(spans[3][2], "#ab79ad")
  eq(spans[5][2], "#ff5c5c")
end)

case("reset_text_routes_absolute_dates_by_style", function()
  local original = maki.ui.format_time
  local calls = {}
  maki.ui.format_time = function(epoch_ms, style)
    calls[#calls + 1] = { epoch_ms, style }
    return style
  end
  local ok, result = pcall(function()
    local weekday = h.reset_text(2 * 24 * 60 * 60 * 1000, 0)
    local date = h.reset_text(8 * 24 * 60 * 60 * 1000, 0)
    return { weekday, date }
  end)
  maki.ui.format_time = original
  if not ok then
    error(result)
  end
  eq(result[1], "Resets weekday")
  eq(result[2], "Resets date")
  eq(calls[1][2], "weekday")
  eq(calls[2][2], "date")
end)

case("close_keys_and_modal_height_are_pure", function()
  eq(h.is_close_key("q"), true)
  eq(h.is_close_key("esc"), true)
  eq(h.is_close_key("ctrl+c"), true)
  eq(h.is_close_key("ctrl+r"), false)
  eq(h.modal_height(100, 20), 12)
  eq(h.modal_height(3, 100), 3)
  eq(h.modal_height(0, 1), 1)
  eq(h.modal_window_height(12), 14)
  eq(h.modal_window_height(3), 5)
end)

case("modal_lines_include_quota_session_and_recorded_costs", function()
  local lines = h.lines({
    provider = "Anthropic",
    status = "ready",
    limits = {
      { window = { kind = "hours", value = 5 }, percentage = 30, reset_at_ms = 1060000 },
    },
  }, {
    total = {
      input = 1000,
      output = 200,
      cache_read = 300,
      cache_creation = 400,
      cost = 1.23456,
    },
    models = {
      { model = "provider/model", input = 10, output = 20, cache_read = 30, cache_creation = 40, cost = 0.25 },
      { model = "provider/unpriced", input = 1, output = 2, cache_read = 3, cache_creation = 4 },
    },
  }, 1000)
  local all = {}
  for _, line in ipairs(lines) do
    all[#all + 1] = text(line)
  end
  local rendered = table.concat(all, "\n")
  has(rendered, "Anthropic quota")
  has(rendered, "5-hour usage  30% used  Resets in 0 hr 1 min")
  has(rendered, "all models")
  has(rendered, "cache-read     300")
  has(rendered, "cache-create     400")
  has(rendered, "$1.235")
  has(rendered, "—")
  has(rendered, "provider/model")
  has(rendered, "$0.250")
end)

case("quota_states_render_plain_feedback", function()
  has(text(h.lines({ state = "unsupported" }, {})[2]), "no usage endpoint")
  has(text(h.lines({ state = "error", value = "boom" }, {})[2]), "boom")
  has(text(h.lines({ status = "error", error = "failed" }, {})[2]), "failed")
end)

case("scroll_and_viewport_clamp_after_resize", function()
  eq(h.scroll(0, "down", 10, 4), 1)
  eq(h.scroll(1, "pagedown", 10, 4), 4)
  eq(h.scroll(4, "end", 10, 4), 6)
  eq(h.clamp_scroll(6, 10, 8), 2)
  local visible, scroll = h.viewport({ "a", "b", "c", "d" }, 3, 3)
  eq(scroll, 1)
  eq(#visible, 3)
  eq(visible[1], "b")
end)

th.report()
