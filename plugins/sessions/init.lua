-- The /sessions picker: one flat list of every session in this directory,
-- live or stored. Live ones get a colored icon, and row order is frozen
-- while the picker is open so rows never jump around under the cursor
-- while background agents keep working. A session open in another terminal
-- is greyed out and cannot be opened from here.

local TextInput = require("maki.text_input")
local Spans = require("maki.spans")
local Helpers = require("sessions_helpers")

local FILTER_PREFIX = "❯ "
local RENAME_PREFIX = "Rename: "
local CONFIRM_HINT = "  Ctrl+D again to delete"
local DELETE_FOCUSED_HINT = "Cannot delete the current session"
local RENAME_USAGE = "Usage: /rename <title>"
local EMPTY_HINT = "  No sessions yet. Press Ctrl+N to start one."
local NO_MATCHES_HINT = "  No matches"
local LOADING_HINT = "  Loading sessions…"
local OPEN_ELSEWHERE_MSG = "session is open in another terminal"
local TICK_MS = 100
local AGE_TICKS = 10
-- Placeholder only: the host swaps "spinner:*"-styled spans for the live
-- animated frame, so working rows spin without this plugin redrawing.
local WORKING_ICON = "· "
local FILTER_KEYS = {
  { "Enter", "open" },
  { "Ctrl+N", "new" },
  { "Ctrl+R", "rename" },
  { "Ctrl+D", "delete" },
}
local RENAME_KEYS = {
  { "Enter", "save" },
  { "Esc", "cancel" },
}

local board = nil

local function icon_of(s)
  if s.status == "needs_input" then
    return "◆ ", "warning"
  end
  if s.status == "working" then
    return WORKING_ICON, "accent", true
  end
  if s.focused then
    return "● ", "accent"
  end
  if s.live then
    return "○ ", "accent"
  end
  return "  ", "dim"
end

-- Current session first, then most recently opened.
local function by_recency(a, b)
  if a.focused ~= b.focused then
    return a.focused
  end
  return (a.updated_at or 0) > (b.updated_at or 0)
end

-- Rows keep their rank for the picker's lifetime; new ones enter above
-- existing ones so nothing already on screen moves. Within one batch
-- (notably the first full refresh) recency decides.
local function assign_ranks(fresh)
  table.sort(fresh, by_recency)
  local base = board.min_rank - #fresh
  for i, s in ipairs(fresh) do
    board.rank[s.id] = base + i
  end
  board.min_rank = base
end

local function update_footer()
  if board.rename then
    board.win:set_config({ footer = RENAME_KEYS })
    return
  end
  local footer = {}
  if board.counts.needs_input > 0 then
    footer[#footer + 1] = { "◆ " .. board.counts.needs_input, "needs input" }
  end
  if board.counts.working > 0 then
    footer[#footer + 1] = { "● " .. board.counts.working, "working" }
  end
  for _, f in ipairs(FILTER_KEYS) do
    footer[#footer + 1] = f
  end
  board.win:set_config({ footer = footer })
end

local function sel_index()
  for i, s in ipairs(board.items) do
    if s.id == board.sel_id then
      return i
    end
  end
  return nil
end

local function find_stored(id)
  for i, st in ipairs(board.stored or {}) do
    if st.id == id then
      return i
    end
  end
  return nil
end

local function apply_filter()
  local prev_pos = sel_index() or 1
  local query = board.input:value()
  board.items = Helpers.filter_rows(board.all, query, maki.match.completion, function(left, right)
    return maki.match.compare(left, right)
  end)
  board.sel_id = Helpers.reconcile_selection(board.sel_id, prev_pos, board.items)
end

local function filter_changed()
  board.confirm = nil
  apply_filter()
end

local function dispw(s)
  return utf8.len(s) or #s
end

local function render()
  local lines = {}
  local inner = board.width - 4
  local input = board.rename and board.rename.input or board.input
  local prefix = board.rename and RENAME_PREFIX or FILTER_PREFIX
  for _, ln in ipairs(input:render(prefix, dispw(prefix), inner).lines) do
    lines[#lines + 1] = ln
  end
  lines[#lines + 1] = Helpers.header_spans(inner)
  -- The query and header stay pinned while the list scrolls.
  if #lines ~= board.reserved then
    board.reserved = #lines
    board.win:set_config({ reserved_top = board.reserved })
  end
  local cursor_line = board.reserved
  for i, s in ipairs(board.items) do
    local selected = s.id == board.sel_id
    local icon, icon_style, spinning = icon_of(s)
    local base = Helpers.row_style(s, selected)
    local right, right_style = Helpers.right(s, selected)
    if selected then
      icon_style = "selected"
    end
    -- Prefix after selection so a working row keeps animating host-side
    -- on the selection background.
    if spinning then
      icon_style = "spinner:" .. icon_style
    end
    local confirm_width = board.confirm == s.id and dispw(CONFIRM_HINT) or 0
    local title_spans = { { "  ", base }, { icon, icon_style } }
    for _, sp in
      ipairs(
        Spans.match_spans(s.title, s._match and s._match.indices or {}, base, selected and "match_selected" or "match")
      )
    do
      title_spans[#title_spans + 1] = sp
    end
    title_spans = Helpers.clip_spans(title_spans, Helpers.title_width_budget(confirm_width, inner))
    if board.confirm == s.id then
      title_spans[#title_spans + 1] = { CONFIRM_HINT, selected and "match_selected" or "error" }
    end
    local count_text, pad = Helpers.row_right_columns(Helpers.spans_width(title_spans), inner, s.message_count)
    local count_style = selected and "selected" or "dim"
    local line = Helpers.row_spans(title_spans, count_text, count_style, pad, right, right_style)
    lines[#lines + 1] = line
    if selected then
      cursor_line = #lines
    end
  end
  if board.loading then
    lines[#lines + 1] = { { LOADING_HINT, "dim" } }
  elseif #board.items == 0 then
    lines[#lines + 1] = { { #board.all == 0 and EMPTY_HINT or NO_MATCHES_HINT, "dim" } }
  end
  board.buf:set_lines(lines)
  board.win:set_cursor(cursor_line)
end

-- Rebuilds the list from live runtimes and the stored snapshot, then
-- renders. Live runtimes win over their stored copies; stored-only sessions
-- are idle. Until the background scan lands (`board.stored`) only live
-- sessions are shown. `live()` suspends this coroutine, and the picker may
-- close or another refresh may finish meanwhile, so bail out unless this
-- board is still current.
local function refresh()
  local this_board = board
  local live, live_err = maki.session.live()
  if board ~= this_board then
    return
  end
  if live_err then
    maki.ui.flash(live_err)
    render()
    return
  end
  local all = Helpers.merge(live, board.stored or {})
  board.counts = { needs_input = 0, working = 0 }
  for _, s in ipairs(all) do
    if board.counts[s.status] then
      board.counts[s.status] = board.counts[s.status] + 1
    end
  end
  if board.loading then
    table.sort(all, by_recency)
  else
    local fresh = {}
    for _, s in ipairs(all) do
      if not board.rank[s.id] then
        fresh[#fresh + 1] = s
      end
    end
    if #fresh > 0 then
      assign_ranks(fresh)
    end
    table.sort(all, function(a, b)
      if a.focused ~= b.focused then
        return a.focused
      end
      return board.rank[a.id] < board.rank[b.id]
    end)
  end
  board.all = all
  apply_filter()
  update_footer()
  render()
end

local function close()
  if board then
    board.win:close()
    board = nil
  end
end

local function selected()
  local idx = sel_index()
  return idx and board.items[idx] or nil
end

local function set_sel(i)
  board.sel_id = board.items[i] and board.items[i].id or nil
  board.confirm = nil
  render()
end

local function move_sel(delta, wrap)
  local n = #board.items
  if n == 0 then
    return
  end
  local cur = sel_index() or 1
  if wrap then
    set_sel((cur - 1 + delta) % n + 1)
  else
    set_sel(math.min(math.max(cur + delta, 1), n))
  end
end

local function page_size()
  return math.max(board.height - board.reserved - 1, 1)
end

local function open_selected()
  local s = selected()
  if not s then
    return
  end
  if not s.focused then
    if not Helpers.can_open(s) then
      maki.ui.flash(OPEN_ELSEWHERE_MSG)
      return
    end
    local _, err = maki.session.focus(s.id)
    if err then
      maki.ui.flash(err)
      return
    end
  end
  close()
end

local function open_blank()
  local _, err = maki.session.new({ focus = true })
  if err then
    maki.ui.flash(err)
    return
  end
  close()
end

local function delete_selected()
  local s = selected()
  if not s then
    return
  end
  if s.focused then
    maki.ui.flash(DELETE_FOCUSED_HINT)
    return
  end
  if board.confirm ~= s.id then
    board.confirm = s.id
    render()
    return
  end
  board.confirm = nil
  local _, err = maki.session.delete(s.id)
  if err then
    maki.ui.flash(err)
    return
  end
  board.deleted[s.id] = true
  local si = find_stored(s.id)
  if si then
    table.remove(board.stored, si)
  end
  refresh()
end

local function start_rename()
  local s = selected()
  if not s then
    return
  end
  local input = TextInput.new()
  input:insert_text(s.title)
  board.rename = { id = s.id, input = input }
  board.confirm = nil
  update_footer()
  render()
end

local function stop_rename()
  board.rename = nil
  update_footer()
  render()
end

local function commit_rename()
  local title = board.rename.input:value():match("^%s*(.-)%s*$")
  local id = board.rename.id
  stop_rename()
  if title == "" then
    return
  end
  local _, err = maki.session.set_title({ id = id, title = title })
  if err then
    maki.ui.flash(err)
  else
    local si = find_stored(id)
    if si then
      board.stored[si].title = title
    end
  end
  refresh()
end

local function handle_rename_key(key)
  if key == "esc" then
    stop_rename()
  elseif key == "enter" then
    commit_rename()
  elseif key ~= "up" and key ~= "down" then
    if board.rename.input:handle_key(key) ~= "ignored" then
      render()
    end
  end
end

local function handle_key(key)
  if key == "ctrl+c" then
    close()
  elseif board.rename then
    handle_rename_key(key)
  elseif key == "esc" then
    if board.confirm then
      board.confirm = nil
      render()
    elseif not board.input:is_empty() then
      board.input:clear()
      filter_changed()
      render()
    else
      close()
    end
  elseif key == "up" then
    move_sel(-1, true)
  elseif key == "down" then
    move_sel(1, true)
  elseif key == "pageup" then
    move_sel(-page_size())
  elseif key == "pagedown" then
    move_sel(page_size())
  elseif key == "enter" then
    open_selected()
  elseif key == "ctrl+n" then
    open_blank()
  elseif key == "ctrl+r" then
    start_rename()
  elseif key == "ctrl+d" then
    delete_selected()
  else
    local r = board.input:handle_key(key)
    if r ~= "ignored" then
      if r == "changed" then
        filter_changed()
      else
        board.confirm = nil
      end
      render()
    end
  end
end

local function open()
  if board then
    return
  end
  local buf = maki.ui.buf()
  local win = maki.ui.open_win(buf, {
    title = " Sessions ",
    width = "70%",
    height = "70%",
    border = "rounded",
    reserved_top = 2,
    focus = true,
    footer = FILTER_KEYS,
  })
  board = {
    win = win,
    buf = buf,
    width = win.width,
    height = win.height,
    reserved = 2,
    input = TextInput.new(),
    all = {},
    items = {},
    rank = {},
    deleted = {},
    min_rank = 0,
    counts = { needs_input = 0, working = 0 },
    sel_id = nil,
    frame = 0,
    loading = true,
  }
  -- Two-phase load: live sessions are cheap, so they show up and take keys
  -- right away; the stored scan can be slow, so a background task merges it
  -- in once it lands.
  refresh()
  local this_board = board
  maki.async.run(function()
    local stored, err = maki.session.list()
    if board ~= this_board then
      return
    end
    if err then
      maki.ui.flash(err)
      stored = {}
    end
    -- A delete may have landed while the scan was in flight; never let the
    -- stale snapshot resurrect that session as a ghost row.
    local kept = {}
    for _, st in ipairs(stored) do
      if not board.deleted[st.id] then
        kept[#kept + 1] = st
      end
    end
    board.stored = kept
    board.loading = false
    refresh()
  end)
  while board do
    local ev = board.win:recv(TICK_MS)
    if not ev or ev.type == "close" then
      board = nil
    elseif ev.type == "timeout" then
      board.frame = board.frame + 1
      if board.dirty then
        board.dirty = false
        refresh()
      elseif board.frame % AGE_TICKS == 0 then
        render()
      end
    elseif ev.type == "key" then
      handle_key(ev.key)
    elseif ev.type == "paste" then
      local input = board.rename and board.rename.input or board.input
      input:insert_text(ev.text)
      if not board.rename then
        filter_changed()
      else
        board.confirm = nil
      end
      render()
    elseif ev.type == "resize" then
      board.width = ev.width
      board.height = ev.height
      render()
    end
  end
end

-- A background agent flipping state deserves a heads-up even with the picker
-- closed, so the flash names the session and where to find it. Autocmds run
-- synchronously while refresh needs an async roundtrip, so the dirty flag
-- defers it to the next tick of the recv loop.
local last_status = {}
maki.api.create_autocmd("SessionStatusChanged", {
  callback = function(ev)
    local d = ev.data or {}
    local prev = last_status[d.session_id]
    last_status[d.session_id] = d.status
    if not d.focused then
      if d.status == "needs_input" then
        maki.ui.flash("◆ " .. d.title .. " needs input · /sessions")
      elseif d.status == "idle" and prev == "working" then
        maki.ui.flash("✓ " .. d.title .. " finished · /sessions")
      end
    end
    if board then
      board.dirty = true
    end
  end,
})

maki.api.register_command({
  name = "/sessions",
  description = "Browse and switch sessions",
  argument_hint = "[query]",
  nargs = "?",
  tui_only = true,
  handler = open,
})

-- Autocmd callbacks run synchronously on the Lua thread; open() suspends
-- (ui roundtrips, win:recv), so it must run as a coroutine. Hop through the
-- /sessions command, which the host runs as a deadline-free async task. The
-- maki.async.run wrapper lives only as long as the run_command roundtrip,
-- so its abandon deadline never touches the open picker.
maki.api.create_autocmd("SessionPickerRequested", {
  callback = function()
    maki.async.run(function()
      local _, err = maki.api.run_command("/sessions")
      if err then
        maki.ui.flash(err)
      end
    end)
  end,
})
maki.keymap.set("n", "<C-p>", open, { desc = "Browse sessions" })

maki.api.register_command({
  name = "/rename",
  description = "Rename the current session",
  argument_hint = "<title>",
  nargs = "+",
  tui_only = true,
  handler = function(opts)
    local title = opts.args:match("^%s*(.-)%s*$")
    if title == "" then
      maki.ui.flash(RENAME_USAGE)
      return
    end
    local id, err = maki.session.current()
    if err then
      maki.ui.flash(err)
      return
    end
    local _, set_err = maki.session.set_title({ id = id, title = title })
    maki.ui.flash(set_err or ('Renamed to "' .. title .. '"'))
  end,
})
