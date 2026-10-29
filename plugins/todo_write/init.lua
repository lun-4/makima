local NAMESPACE = "todo_write.todos"
local focused, focused_id, focused_session
local refresh_generation = 0
local panel_items = {}
local visibility = {}
local panels = {}
local buf, win

local STATUS_MARKERS = {
  completed = { "[✓]", "todo_completed" },
  in_progress = { "[•]", "todo_in_progress" },
  pending = { "[ ]", "todo_pending" },
  cancelled = { "[x]", "todo_cancelled" },
}
local PRIORITIES = { high = true, medium = true, low = true }

local DESCRIPTION = [[Create or update a structured todo list to track tasks.

**Use after EACH completed step!**

- Send the complete list each time (replace-all semantics).
- Send an empty list to clear todos.
- Use ONLY for multi-step work (3+ steps).
- Skip for trivial tasks.]]

local function empty_list()
  return maki.json.decode("[]")
end

local function validate(items)
  if type(items) ~= "table" then
    return nil, "todos must be a dense array"
  end
  local count = 0
  for key in pairs(items) do
    if type(key) ~= "number" or key % 1 ~= 0 or key < 1 or key > #items then
      return nil, "todos must be a dense array"
    end
    count = count + 1
  end
  if count ~= #items then
    return nil, "todos must be a dense array"
  end
  local result = empty_list()
  for index, item in ipairs(items) do
    if type(item) ~= "table" then
      return nil, string.format("todo %d must be an object", index)
    end
    if type(item.content) ~= "string" then
      return nil, string.format("todo %d content must be a string", index)
    end
    if not STATUS_MARKERS[item.status] then
      return nil, string.format("todo %d has an invalid status", index)
    end
    if item.priority ~= nil and not PRIORITIES[item.priority] then
      return nil, string.format("todo %d has an invalid priority", index)
    end
    result[index] = { content = item.content, status = item.status, priority = item.priority }
  end
  return result
end

local function read_items(ctx)
  local items, err = ctx:history_get(NAMESPACE)
  if err then
    return nil, err
  end
  if items == nil then
    return empty_list()
  end
  return validate(items)
end

local function count_done(items)
  local n = 0
  for _, item in ipairs(items) do
    if item.status == "completed" then
      n = n + 1
    end
  end
  return n
end

local function update_hint(items)
  maki.ui.set_status_hint({
    { string.format(" %d/%d ", count_done(items), #items), "foreground" },
    { "Ctrl+T", "keybind_key" },
    { " ", "" },
  })
end

local function ensure_win(visible)
  local panel = panels[focused_session or ""]
  if panel and panel.win:is_open() then
    buf, win = panel.buf, panel.win
    return
  end
  buf = maki.ui.buf()
  win = maki.ui.open_win(buf, {
    split = "panel",
    height = 4,
    order = 10,
    title = " Todos ",
    border = "rounded",
    focus = false,
    visible = visible,
    footer = {
      { "Ctrl+T", "to hide" },
    },
  })
  panels[focused_session or ""] = { buf = buf, win = win }
end

local function build_lines(items)
  local lines = {}
  for _, item in ipairs(items) do
    local marker = STATUS_MARKERS[item.status]
    lines[#lines + 1] = {
      { marker[1] .. " " .. item.content, marker[2] },
    }
  end
  return lines
end

local function card(items)
  local body = maki.ui.buf()
  body:set_lines(build_lines(items))
  return body
end

local function result(items)
  return { llm_output = maki.json.encode(items), body = card(items) }
end

local function failure(err)
  return { llm_output = err, is_error = true }
end

local function hide_panel()
  if win and win:is_open() then
    win:hide()
  end
  maki.ui.set_status_hint(nil)
end

local function sync_panel()
  if #panel_items == 0 then
    hide_panel()
    return
  end
  local visible = visibility[focused_id] == true
  ensure_win(visible)
  buf:set_lines(build_lines(panel_items))
  win:set_config({ height = #panel_items + 2 })
  if visible then
    win:show()
    maki.ui.set_status_hint(nil)
  else
    win:hide()
    update_hint(panel_items)
  end
end

local function refresh(ctx, pop)
  refresh_generation = refresh_generation + 1
  local generation, agent = refresh_generation, focused
  if not agent then
    panel_items = {}
    hide_panel()
    return
  end
  local items, err = agent:history_get(ctx, NAMESPACE)
  if generation ~= refresh_generation then
    return
  end
  if not err then
    if items == nil then
      items = empty_list()
    end
    items, err = validate(items)
  end
  panel_items = not err and items or {}
  if #panel_items == 0 then
    visibility[focused_id] = nil
  elseif pop and visibility[focused_id] == nil then
    visibility[focused_id] = true
  end
  sync_panel()
end

maki.api.register_prompt_hint({
  slot = "tool_usage",
  content = "- Use todo_write for multi-step tasks (3+ steps); update **after EACH step** (done + next in_progress), never batched at the end. Use todo_read to check the current list.",
})

maki.api.register_tool({
  name = "todo_write",
  description = DESCRIPTION,
  schema = {
    type = "object",
    required = { "todos" },
    properties = {
      todos = {
        type = "array",
        description = "The complete updated todo list, or an empty array to clear it",
        items = {
          type = "object",
          required = { "content", "status" },
          properties = {
            content = { type = "string", description = "Task description" },
            status = {
              type = "string",
              enum = { "pending", "in_progress", "completed", "cancelled" },
            },
            priority = {
              type = "string",
              enum = { "high", "medium", "low" },
            },
          },
        },
      },
    },
  },
  audiences = { "main", "research_sub", "general_sub" },

  header = function(input)
    return string.format("%d todos", type(input.todos) == "table" and #input.todos or 0)
  end,

  restore = function(input)
    local items = validate(input.todos)
    return items and card(items) or nil
  end,

  handler = function(input, ctx)
    local items, err = validate(input.todos)
    if err then
      return failure(err)
    end
    local ok
    ok, err = ctx:history_set(NAMESPACE, items)
    if not ok then
      return failure(err)
    end
    return result(items)
  end,
})

maki.api.register_tool({
  name = "todo_read",
  description = "Read the current todo list for this agent.",
  schema = { type = "object", properties = {} },
  audiences = { "main", "research_sub", "general_sub" },
  header = "Read todos",
  restore = function(_, output)
    local items = validate(maki.json.decode(output))
    return items and card(items) or nil
  end,
  handler = function(_, ctx)
    local items, err = read_items(ctx)
    if err then
      return failure(err)
    end
    return result(items)
  end,
})

local function toggle()
  if not focused_id or #panel_items == 0 then
    return
  end
  visibility[focused_id] = not visibility[focused_id]
  sync_panel()
end

maki.keymap.set("n", "<C-t>", toggle, { desc = "Toggle todo panel" })

maki.api.create_autocmd("AgentFocusChanged", {
  callback = function(ev, ctx)
    hide_panel()
    focused = ev.agent
    focused_id = ev.data and ev.data.agent_id
    focused_session = ev.data and ev.data.session_id
    local panel = panels[focused_session or ""]
    buf, win = panel and panel.buf, panel and panel.win
    panel_items = {}
    refresh(ctx, false)
  end,
})

maki.api.create_autocmd("HistoryStateChanged", {
  callback = function(ev, ctx)
    local data = ev.data or {}
    if focused and data.agent_id == focused_id and (data.namespace == nil or data.namespace == NAMESPACE) then
      refresh(ctx, true)
    end
  end,
})
