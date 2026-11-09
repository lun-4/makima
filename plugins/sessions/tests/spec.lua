local sh = require("sessions_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

local NOW = os.time()
local NANOS_PER_SEC = 1000000000
local NANOS_PER_MS = 1000000
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
  local text, style = sh.right(stored_row({}), false, maki.time.at(NOW))
  eq(text, "0s")
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
  local reference = maki.time.at(NOW)
  eq(sh.age(NOW, reference), "0s")
  eq(sh.age(NOW - 5 * MINUTE, reference), "5m 0s")
  eq(sh.age(NOW - 3 * HOUR, reference), "3h 0s")
  eq(sh.age(NOW - 50 * HOUR, reference), "2d 2h 0s")
  eq(sh.age(NOW - 30 * 24 * HOUR, reference), "30d 0s")
  eq(sh.age(NOW + HOUR, reference), "0s", "future timestamps clamp to now")
end)

case("message_count_formatting", function()
  eq(sh.format_count(0), "0")
  eq(sh.format_count(1), "1")
  eq(sh.format_count(12), "12")
  eq(sh.format_count(nil), "0")
end)

case("picker_row_contains_count_then_status", function()
  local columns = sh.layout(80, { "1d 0s" }, { "42" })
  local count_text, padding = sh.row_right_columns(5, 80, 42, columns)
  local spans = sh.row_spans({ { "title", "item" } }, count_text, "dim", padding, "1d 0s", "selected", columns)
  local texts = {}
  for _, span in ipairs(spans) do
    texts[#texts + 1] = span[1]
  end
  eq(table.concat(texts), "title" .. string.rep(" ", padding + columns.count_width - 2) .. "42" .. " " .. "1d 0s")
  eq(spans[2][2], "dim", "pad shares the count style")
  eq(spans[3][2], "dim", "count keeps the selection-driven style")
  eq(spans[4][1], "42")
  eq(spans[5][1], " ", "one space separates the count and status columns")
  eq(spans[6][2], "selected", "status padding keeps its own style")
  eq(spans[7][2], "selected")
  eq(#spans, 7)
end)

case("picker_row_right_columns_remain_separated_at_narrow_width", function()
  local columns = sh.layout(80, { "1d 0s" }, { "5" })
  local count_text, padding = sh.row_right_columns(9, 80, 5, columns)
  eq(count_text, "5")
  eq(padding, 80 - 9 - columns.right_width, "a short title pushes the columns to the right edge")
  local _, edge = sh.row_right_columns(80 - columns.right_width, 80, 5, columns)
  eq(edge, 1, "the budget edge leaves exactly one separator")
  local _, clamped = sh.row_right_columns(200, 80, 5, columns)
  eq(clamped, 1, "the separator never underflows below one space")
end)

case("picker_row_clips_long_titles_before_the_columns", function()
  local inner, confirm_width = 60, 0
  local columns = sh.layout(inner, { "1d 0s" }, { "7" })
  local budget = sh.title_width_budget(confirm_width, inner, columns)
  eq(budget, inner - columns.right_width - 1)
  local title = string.rep("x", 100)
  local clipped = sh.clip_spans({ { "  ", "item" }, { "● ", "accent" }, { title, "item" } }, budget)
  local left = sh.spans_width(clipped)
  local count_text, padding = sh.row_right_columns(left, inner, 7, columns)
  local row = sh.row_spans(clipped, count_text, "dim", padding, "1d 0s", "dim", columns)
  eq(sh.spans_width(row), inner, "the clipped row fills the width exactly, columns intact")
  eq(padding, 1)
  eq(utf8.len(clipped[3][1]), budget - 4, "only the title tail is cut")
end)

case("picker_row_with_clipped_confirmation_fills_the_width_exactly", function()
  local inner = 40
  local columns = sh.layout(inner, { "1d 0s" }, { "7" })
  local room = sh.confirm_width_budget(inner, columns)
  local confirm = sh.clip_spans({ { string.rep("x", 100), "error" } }, room)
  local confirm_width = sh.spans_width(confirm)
  local budget = sh.title_width_budget(confirm_width, inner, columns)
  local title = sh.clip_spans({ { "  ", "item" }, { "● ", "accent" }, { "title", "item" } }, budget)
  for _, span in ipairs(confirm) do
    title[#title + 1] = span
  end
  local count_text, padding = sh.row_right_columns(sh.spans_width(title), inner, 7, columns)
  local row = sh.row_spans(title, count_text, "dim", padding, "1d 0s", "dim", columns)
  eq(sh.spans_width(row), inner, "a confirmation clipped to its budget keeps the columns on their header cells")
end)

case("confirm_layout_keeps_the_full_hint_beside_a_wide_age", function()
  local inner, label, hint = 38, "4d 7h 23m 9s", "  Ctrl+D again to delete"
  local columns = sh.layout(inner, { label }, { "123" })
  assert(sh.confirm_width_budget(inner, columns) < utf8.len(hint), "the shared layout would clip the hint")
  local confirming = sh.confirm_layout(columns)
  eq(confirming.status_width, columns.status_width, "the age keeps its column")
  assert(sh.confirm_width_budget(inner, confirming) >= utf8.len(hint), "the confirmation lends the count cell")
end)

case("picker_header_shares_row_columns_and_theme_style", function()
  local inner = 80
  local columns = sh.layout(inner, { "1d 0s" }, { "5" })
  local header = sh.header_spans(inner, columns)
  eq(sh.spans_width(header), inner)
  for _, span in ipairs(header) do
    eq(span[2], "path")
  end
  eq(header[1][1], "    title")
  eq(header[4][1], "messages")
  eq(header[7][1], "age")

  local count_text, padding = sh.row_right_columns(9, inner, 5, columns)
  local row = sh.row_spans(
    { { "  ", "item" }, { "  ", "dim" }, { "title", "item" } },
    count_text,
    "dim",
    padding,
    "1d 0s",
    "dim",
    columns
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

local function picker(events, query, opts)
  opts = opts or {}
  local originals = {
    buf = maki.ui.buf,
    open_win = maki.ui.open_win,
    flash = maki.ui.flash,
    command = maki.api.register_command,
    autocmd = maki.api.create_autocmd,
    keymap = maki.keymap.set,
    run = maki.async.run,
    live = maki.session.live,
    list = maki.session.list,
    focus = maki.session.focus,
    title = maki.session.set_title,
    delete = maki.session.delete,
    now = maki.time.now,
    completion = maki.match.completion,
  }
  local command, lines, focused, renamed, closed, deleted
  local clock = { secs = NOW, nanosecs = 0 }
  local async_queue = {}
  local snapshots, snapshot_counts, timeouts = {}, {}, {}
  local autocmds = opts.autocmds or {}
  local live_calls, list_calls, match_calls = 0, 0, 0
  local function advance_clock(ms)
    clock.nanosecs = clock.nanosecs + ms * NANOS_PER_MS
    while clock.nanosecs >= NANOS_PER_SEC do
      clock.nanosecs = clock.nanosecs - NANOS_PER_SEC
      clock.secs = clock.secs + 1
    end
  end
  maki.time.now = function()
    return { secs = clock.secs, nanosecs = clock.nanosecs }
  end
  maki.match.completion = function(...)
    match_calls = match_calls + 1
    return originals.completion(...)
  end
  maki.api.register_command = function(spec)
    if spec.name == "/sessions" then
      command = spec.handler
    end
  end
  maki.api.create_autocmd = function(name, spec)
    autocmds[name] = spec.callback
  end
  maki.keymap.set = function() end
  maki.async.run = function(fn)
    if opts.defer_async then
      async_queue[#async_queue + 1] = fn
    else
      fn()
    end
  end
  local function session_live()
    live_calls = live_calls + 1
    return {}
  end
  local function session_list()
    list_calls = list_calls + 1
    return opts.rows or { stored_row({ id = "one", title = "first" }), stored_row({ id = "two", title = "second" }) }
  end
  maki.session.live = session_live
  maki.session.list = session_list
  maki.session.focus = function(id)
    focused = id
    return true
  end
  maki.session.set_title = function(spec)
    renamed = spec
    return true
  end
  maki.session.delete = function(id)
    deleted = id
    return true
  end
  maki.ui.flash = function() end
  maki.ui.buf = function()
    return {
      set_lines = function(_, value)
        lines = value
        snapshots[#snapshots + 1] = value
        snapshot_counts[#snapshot_counts + 1] = {
          live = live_calls,
          list = list_calls,
          match = match_calls,
        }
      end,
    }
  end
  local step = 0
  maki.ui.open_win = function()
    return {
      width = opts.width or 80,
      height = 20,
      set_config = function() end,
      set_cursor = function() end,
      close = function()
        closed = true
      end,
      recv = function(_, timeout)
        timeouts[#timeouts + 1] = timeout
        step = step + 1
        local event = events[step]
        if type(event) == "function" then
          return event(lines)
        end
        if type(event) == "table" and event.advance_ms then
          advance_clock(event.advance_ms)
        end
        return event or { type = "close" }
      end,
    }
  end
  local ok, err = pcall(function()
    load_plugin()
    command({ values = { query = query } })
  end)
  maki.ui.buf, maki.ui.open_win, maki.ui.flash = originals.buf, originals.open_win, originals.flash
  maki.api.register_command, maki.api.create_autocmd = originals.command, originals.autocmd
  maki.keymap.set, maki.async.run = originals.keymap, originals.run
  maki.session.live, maki.session.list, maki.session.focus = originals.live, originals.list, originals.focus
  maki.session.set_title, maki.session.delete = originals.title, originals.delete
  maki.time.now, maki.match.completion = originals.now, originals.completion
  assert(ok, tostring(err))
  return focused,
    renamed,
    closed,
    deleted,
    {
      clock = clock,
      snapshots = snapshots,
      counts = snapshot_counts,
      timeouts = timeouts,
      flush_async = function()
        local saved_list, saved_live = maki.session.list, maki.session.live
        maki.session.list, maki.session.live = session_list, session_live
        local queued = async_queue
        async_queue = {}
        for _, fn in ipairs(queued) do
          fn()
        end
        maki.session.list, maki.session.live = saved_list, saved_live
      end,
    }
end

local function all_text(lines)
  local parts = {}
  for _, line in ipairs(lines or {}) do
    for _, span in ipairs(line) do
      parts[#parts + 1] = span[1]
    end
  end
  return table.concat(parts)
end

local function row_with(lines, title)
  for row, line in ipairs(lines) do
    for _, span in ipairs(line) do
      if span[1] == title then
        return row
      end
    end
  end
  error("missing session row: " .. title)
end

case("click_opens_the_session_under_the_pointer", function()
  local focused, _, closed = picker({
    { type = "click", row = 0, col = 2 },
    { type = "click", row = 2, col = 2 },
    function(lines)
      return { type = "click", row = row_with(lines, "second"), col = 3 }
    end,
  })
  eq(focused, "two")
  eq(closed, true)
end)

case("rename_caret_click_edits_without_opening_a_session", function()
  local focused, renamed, closed = picker({
    { type = "key", key = "ctrl+r" },
    { type = "click", row = 1, col = 9 },
    { type = "key", key = "X" },
    { type = "key", key = "enter" },
    { type = "key", key = "ctrl+c" },
  }, "first")
  eq(focused, nil)
  eq(renamed.id, "one")
  eq(renamed.title, "Xfirst")
  eq(closed, true)
end)

case("clicking_delete_confirmation_deletes_instead_of_opening", function()
  local focused, _, _, deleted = picker({
    { type = "key", key = "ctrl+d" },
    function(lines)
      for row, line in ipairs(lines) do
        local col = 1
        for _, span in ipairs(line) do
          if span[1]:find("again to delete", 1, true) then
            return { type = "click", row = row, col = col + 2 }
          end
          col = col + maki.ui.display_width(span[1])
        end
      end
      error("missing delete confirmation")
    end,
    { type = "key", key = "ctrl+c" },
  }, "first")
  eq(focused, nil)
  eq(deleted, "one")
end)

case("right_preserves_current_and_open_labels", function()
  local reference = maki.time.at(NOW)
  local current, current_style = sh.right(live_row({ id = "a1" }), false, reference)
  eq(current, "current")
  eq(current_style, "accent")
  local open, open_style = sh.right(stored_row({ open_elsewhere = true }), false, reference)
  eq(open, "open")
  eq(open_style, "dim")
end)

case("sessions_compound_age_column_layout", function()
  local label = "4d 7h 23m 9s"
  local function row_for(inner)
    local columns = sh.layout(inner, { label }, { "123" })
    local budget = sh.title_width_budget(0, inner, columns)
    local title = sh.clip_spans({ { "  ", "item" }, { "● ", "accent" }, { "title", "item" } }, budget)
    local count_text, padding = sh.row_right_columns(sh.spans_width(title), inner, 123, columns)
    local row = sh.row_spans(title, count_text, "dim", padding, label, "dim", columns)
    return columns, row
  end

  local wide, wide_row = row_for(80)
  eq(sh.spans_width(sh.header_spans(80, wide)), 80)
  eq(wide.status_width, utf8.len(label))
  eq(wide.show_count, true)
  eq(sh.spans_width(wide_row), 80)
  assert(all_text({ wide_row }):find(label, 1, true), "ordinary width keeps the full compound age")

  local narrow, narrow_row = row_for(28)
  eq(sh.spans_width(sh.header_spans(28, narrow)), 28)
  eq(narrow.show_count, false, "the count is withdrawn before the age is elided")
  eq(narrow.status_width, utf8.len(label))
  eq(sh.spans_width(narrow_row), 28)
  assert(all_text({ narrow_row }):find(label, 1, true), "the full age survives while the count does not")

  local elided, elided_row = row_for(18)
  eq(sh.spans_width(sh.header_spans(18, elided)), 18)
  eq(elided.status_width, 10)
  eq(sh.spans_width(elided_row), 18)
  assert(all_text({ elided_row }):find("…7h 23m 9s", 1, true), "higher units elide first")
end)

case("sessions_delete_hit_columns_follow_age_layout", function()
  local long_ago = NOW - (4 * 24 * HOUR + 7 * HOUR + 23 * MINUTE + 9)
  local focused, _, _, deleted = picker(
    {
      { type = "key", key = "ctrl+d" },
      function(lines)
        for row, line in ipairs(lines) do
          local col = 1
          for _, span in ipairs(line) do
            if span[1]:find("again to delete", 1, true) then
              return { type = "click", row = row, col = col + 2 }
            end
            col = col + maki.ui.display_width(span[1])
          end
        end
        error("missing delete confirmation: " .. all_text(lines))
      end,
      { type = "key", key = "ctrl+c" },
    },
    "first",
    {
      width = 60,
      rows = { stored_row({ id = "one", title = "first", updated_at = long_ago }) },
    }
  )
  eq(focused, nil)
  eq(deleted, "one", "the confirmation click target follows the age column layout")
end)

case("age_renders_on_first_second_boundary", function()
  local _, _, _, _, probe = picker({
    { type = "timeout", advance_ms = 999 },
    { type = "timeout", advance_ms = 1 },
    { type = "close" },
  })
  eq(#probe.snapshots, 3, "999ms does not redraw; the next 1ms crosses the boundary")
  eq(probe.timeouts[1], 100, "the receive timeout stays within the status poll bound")
  assert(all_text(probe.snapshots[2]):find("0s", 1, true))
  assert(all_text(probe.snapshots[3]):find("1s", 1, true))
end)

case("continuous_events_do_not_starve_age_render", function()
  local events = {}
  for _ = 1, 10 do
    events[#events + 1] = { type = "click", row = 0, col = 0, advance_ms = 100 }
  end
  events[#events + 1] = { type = "close" }
  local _, _, _, _, probe = picker(events)
  eq(#probe.snapshots, 3, "harmless events still allow the second-boundary redraw")
  assert(all_text(probe.snapshots[3]):find("1s", 1, true))
end)

case("status_changes_refresh_after_non_timeout_events", function()
  local autocmds = {}
  local _, _, _, _, probe = picker({
    function()
      autocmds.SessionStatusChanged({ data = { session_id = "x", status = "working", focused = true } })
      return { type = "click", row = 0, col = 0 }
    end,
    { type = "close" },
  }, nil, { autocmds = autocmds })
  eq(#probe.snapshots, 3, "a status change arriving with input still refreshes the list")
end)

case("age_only_render_does_not_scan_or_filter_sessions", function()
  local _, _, _, _, probe = picker({
    { type = "timeout", advance_ms = 1000 },
    { type = "close" },
  })
  eq(#probe.snapshots, 3)
  eq(probe.counts[3].live, probe.counts[2].live, "age refresh does not read live sessions")
  eq(probe.counts[3].list, probe.counts[2].list, "age refresh does not rescan stored sessions")
  eq(probe.counts[3].match, probe.counts[2].match, "age refresh does not refilter the list")
end)

case("close_and_reopen_isolate_age_refresh", function()
  local _, _, _, _, first = picker({ { type = "close" } }, nil, { defer_async = true })
  local before = #first.snapshots
  first.flush_async()
  eq(#first.snapshots, before, "a stored scan landing after close must not write")

  local _, _, _, _, second = picker({
    { type = "timeout", advance_ms = 1000 },
    { type = "close" },
  })
  assert(all_text(second.snapshots[#second.snapshots]):find("1s", 1, true))
end)

th.report()
