local sh = require("sessions_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

local NOW = os.time()
local MINUTE = 60
local HOUR = 3600

local function stored_row(overrides)
  local s = { id = "a1", title = "stored", updated_at = NOW, cwd = "/here" }
  for k, v in pairs(overrides or {}) do
    s[k] = v
  end
  return s
end

local function live_row(overrides)
  local s = stored_row(overrides)
  s.status = "working"
  s.focused = true
  return s
end

case("merge_live_wins_over_stored_duplicate", function()
  local live = { live_row({ id = "a1", open_elsewhere = true }) }
  local stored = { stored_row({ id = "a1" }), stored_row({ id = "a2", open_elsewhere = true }) }
  local all = sh.merge(live, stored)
  eq(#all, 2)
  eq(all[1].id, "a1")
  eq(all[1].live, true, "live row wins and stays marked live")
  eq(all[1].open_elsewhere, true, "open_elsewhere rides through the merge")
  eq(all[2].id, "a2")
  eq(all[2].open_elsewhere, true)
end)

case("merge_stored_only_rows_become_idle", function()
  local all = sh.merge({}, { stored_row({ id = "a1", open_elsewhere = true }) })
  eq(#all, 1)
  eq(all[1].status, "idle")
  eq(all[1].focused, false)
  eq(all[1].live, nil)
  eq(all[1].open_elsewhere, true)
end)

case("merge_empty_inputs", function()
  eq(#sh.merge({}, {}), 0)
end)

case("row_style_item_by_default", function()
  eq(sh.row_style(stored_row(), false), "item")
end)

case("row_style_greys_open_elsewhere", function()
  eq(sh.row_style(stored_row({ open_elsewhere = true }), false), "dim")
end)

case("row_style_selected_wins_over_dim", function()
  eq(sh.row_style(stored_row({ open_elsewhere = true }), true), "selected")
end)

case("right_shows_open_label_for_open_sessions", function()
  local text, style = sh.right(stored_row({ open_elsewhere = true }), false)
  eq(text, "open")
  eq(style, "dim")
end)

case("right_selected_open_row_keeps_label_selection_style", function()
  local text, style = sh.right(stored_row({ open_elsewhere = true }), true)
  eq(text, "open")
  eq(style, "selected")
end)

case("right_shows_current_for_focused", function()
  local text, style = sh.right(live_row({ id = "a1" }), false)
  eq(text, "current")
  eq(style, "accent")
end)

case("right_selected_focused_wins_over_accent", function()
  local text, style = sh.right(live_row({ id = "a1" }), true)
  eq(text, "current")
  eq(style, "selected")
end)

case("right_shows_age_for_idle_rows", function()
  local text, style = sh.right(stored_row({}), false)
  eq(text, "just now")
  eq(style, "dim")
end)

case("can_open_blocks_open_sessions", function()
  eq(sh.can_open(stored_row({ open_elsewhere = true })), false)
  eq(sh.can_open(stored_row({})), true)
  eq(sh.can_open(live_row({ id = "a1" })), true)
end)

local function match(rank, indices)
  return { indices = indices or {}, ranking = rank }
end

local function rank(quality, start_index)
  return {
    quality_rank = quality,
    boundary_rank = 0,
    start_index = start_index or 1,
    gap_count = 0,
    span_length = 1,
    unmatched_suffix = 0,
    fuzzy_score = 0,
  }
end

case("sessions_filter_ranks_matches", function()
  local rows = {
    stored_row({ id = "weak", title = "weak" }),
    stored_row({ id = "exact", title = "exact" }),
  }
  local ranks = { weak = match(rank(3, 2), { 2 }), exact = match(rank(0), { 1 }) }
  local filtered = sh.filter_rows(rows, "query", function(_, title)
    return ranks[title]
  end, function(left, right)
    return left.ranking.quality_rank - right.ranking.quality_rank
  end)
  eq(filtered[1].id, "exact")
  eq(filtered[2].id, "weak")
  eq(filtered[1]._match.indices[1], 1)
end)

case("sessions_filter_preserves_original_order_for_ties", function()
  local rows = {
    stored_row({ id = "first", title = "first" }),
    stored_row({ id = "second", title = "second" }),
  }
  local filtered = sh.filter_rows(rows, "query", function()
    return match(rank(2, 1), { 1 })
  end, function()
    return 0
  end)
  eq(filtered[1].id, "first")
  eq(filtered[2].id, "second")
end)

case("sessions_filter_orders_exact_prefix_and_fuzzy", function()
  local rows = {
    stored_row({ id = "fuzzy", title = "xapyp" }),
    stored_row({ id = "prefix", title = "apple" }),
    stored_row({ id = "exact", title = "app" }),
  }
  local filtered = sh.filter_rows(rows, "app", maki.match.completion, function(left, right)
    return maki.match.compare(left, right)
  end)
  eq(filtered[1].id, "exact")
  eq(filtered[2].id, "prefix")
  eq(filtered[3].id, "fuzzy")
end)

case("sessions_filter_empty_query_keeps_all_and_highlights_one_based", function()
  local rows = {
    stored_row({ id = "first", title = "你好" }),
    stored_row({ id = "second", title = "👍🏽abc" }),
  }
  local filtered = sh.filter_rows(rows, "a", function(_, title)
    if title == "你好" then
      return nil
    end
    return match(rank(3, 3), { 3 })
  end, function()
    return 0
  end)
  eq(#filtered, 1)
  eq(filtered[1].id, "second")
  eq(filtered[1]._match.indices[1], 3, "rendering consumes one-based codepoint indices")

  local all = sh.filter_rows(rows, "", function()
    return match(rank(4), {})
  end, function()
    return 0
  end)
  eq(#all, 2)
end)

case("sessions_filter_keeps_selected_id", function()
  local rows = { stored_row({ id = "a1" }), stored_row({ id = "a2" }) }
  eq(sh.reconcile_selection("a2", 1, rows), "a2")
end)

case("sessions_filter_clamps_missing_selection", function()
  local rows = { stored_row({ id = "a1" }), stored_row({ id = "a2" }) }
  eq(sh.reconcile_selection("gone", 9, rows), "a2")
  eq(sh.reconcile_selection("gone", 0, rows), "a1")
  eq(sh.reconcile_selection("gone", 1, {}), nil)
end)

case("age_buckets", function()
  eq(sh.age(NOW), "just now")
  eq(sh.age(NOW - 5 * MINUTE), "5m ago")
  eq(sh.age(NOW - 3 * HOUR), "3h ago")
  eq(sh.age(NOW - 50 * HOUR), "2d ago")
  eq(sh.age(NOW - 30 * 24 * HOUR), "1mo ago")
  eq(sh.age(NOW + HOUR), "just now", "future timestamps clamp to now")
end)

case("message_count_formatting", function()
  eq(sh.format_count(0), "0")
  eq(sh.format_count(1), "1")
  eq(sh.format_count(12), "12")
  eq(sh.format_count(nil), "0")
end)

case("picker_row_contains_count_then_status", function()
  local spans = sh.row_spans({ { "title", "item" } }, "42", "dim", 5, "1d ago", "selected")
  local texts = {}
  for _, span in ipairs(spans) do
    texts[#texts + 1] = span[1]
  end
  eq(table.concat(texts), "title" .. string.rep(" ", 18) .. "42" .. string.rep(" ", 5) .. "1d ago")
  eq(spans[2][2], "dim", "pad shares the count style")
  eq(spans[3][2], "dim", "count keeps the selection-driven style")
  eq(spans[4][1], "42")
  eq(spans[5][1], " ", "one space separates the count and status columns")
  eq(spans[6][2], "selected", "status padding keeps its own style")
  eq(spans[7][2], "selected")
  eq(#spans, 7)
end)

case("picker_row_right_columns_remain_separated_at_narrow_width", function()
  local count_text, padding = sh.row_right_columns(9, 80, 5)
  eq(count_text, "5")
  eq(padding, 45, "a short title pushes the columns to the right edge")
  local _, edge = sh.row_right_columns(80 - 27, 80, 5)
  eq(edge, 1, "the budget edge leaves exactly one separator")
  local _, clamped = sh.row_right_columns(200, 80, 5)
  eq(clamped, 1, "the separator never underflows below one space")
end)

case("picker_row_clips_long_titles_before_the_columns", function()
  local inner, icon_width, confirm_width = 60, 2, 0
  local budget = sh.title_width_budget(confirm_width, inner)
  eq(budget, inner - 1 - 26)
  local title = string.rep("x", 100)
  local clipped = sh.clip_spans({ { "  ", "item" }, { "● ", "accent" }, { title, "item" } }, budget)
  local left = sh.spans_width(clipped)
  local count_text, padding = sh.row_right_columns(left, inner, 7)
  local row = sh.row_spans(clipped, count_text, "dim", padding, "1d ago", "dim")
  eq(sh.spans_width(row), inner, "the clipped row fills the width exactly, columns intact")
  eq(padding, 1)
  eq(utf8.len(clipped[3][1]), budget - 4, "only the title tail is cut")
end)

case("picker_header_shares_row_columns_and_theme_style", function()
  local inner = 80
  local header = sh.header_spans(inner)
  eq(sh.spans_width(header), inner)
  for _, span in ipairs(header) do
    eq(span[2], "path")
  end
  eq(header[1][1], "    title")
  eq(header[4][1], "messages")
  eq(header[7][1], "age")

  local count_text, padding = sh.row_right_columns(9, inner, 5)
  local row = sh.row_spans(
    { { "  ", "item" }, { "  ", "dim" }, { "title", "item" } },
    count_text,
    "dim",
    padding,
    "1d ago",
    "dim"
  )
  eq(sh.spans_width(row), inner)
  -- Both label the same 15-cell count column and 10-cell age column, so the
  -- width up to and including the count label must match.
  local function width_through_count(spans, count_index)
    local total = 0
    for i, span in ipairs(spans) do
      total = total + (utf8.len(span[1]) or #span[1])
      if i == count_index then
        return total
      end
    end
  end
  eq(width_through_count(header, 4), width_through_count(row, 6))
end)

case("merge_preserves_message_count", function()
  local all = sh.merge({}, { stored_row({ id = "a1", message_count = 7 }) })
  eq(all[1].message_count, 7, "stored-only rows keep their scanned count")
  eq(all[1].status, "idle")
end)

case("live_rows_override_stored_message_count", function()
  local all = sh.merge({ live_row({ id = "a1", message_count = 3 }) }, { stored_row({ id = "a1", message_count = 7 }) })
  eq(all[1].message_count, 3, "the live copy beats the stored count")
  eq(#all, 1)
end)

th.report()
