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

th.report()
