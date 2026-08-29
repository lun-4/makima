local REGISTRY = "splash"
local FALLBACK_NAME = "default"
local STATE_DIR = "splashes"
local STATE_FILE = "selection.json"

local renderers = {}
local dirty = true
local candidate
local committed = { name = FALLBACK_NAME }
local previous_committed
local last_render
local rollback_queued = false
local startup

local plugin_opts = maki.api.register_options({
  splash = {
    type = "string",
    desc = "Splash to select at startup, e.g. 'aurora' or 'default'. Wins over the persisted selection, re-wins on every boot, and updates the persisted selection to its value.",
  },
})
if type(plugin_opts.splash) == "string" then
  startup = plugin_opts.splash:lower()
end

local function refresh_renderers()
  if not dirty then
    return
  end
  dirty = false
  renderers = maki.store.collect(REGISTRY)
  -- Discard cached renderers so a reloaded or unloaded plugin does not keep
  -- a function from an old instance alive through the picker.
  committed.renderer = nil
  committed.validated = nil
  if candidate then
    candidate.renderer = nil
    candidate.validated = nil
  end
  if previous_committed then
    previous_committed.renderer = nil
    previous_committed.validated = nil
  end
end

local function resolve(selection)
  refresh_renderers()
  if type(selection.renderer) == "function" then
    return true
  end
  local entry = renderers[selection.name]
  if type(entry) ~= "table" or type(entry.renderer) ~= "function" then
    return nil, "unknown splash: " .. selection.name
  end
  selection.renderer = entry.renderer
  selection.validated = nil
  return true
end

local function items()
  refresh_renderers()
  local out = {}
  for name, entry in pairs(renderers) do
    if type(entry) == "table" and type(entry.renderer) == "function" then
      out[#out + 1] = {
        label = entry.label or name,
        detail = entry.description or "",
        name = name,
      }
    end
  end
  table.sort(out, function(a, b)
    return a.label < b.label
  end)
  return out
end

local function item_index(list, name)
  for index, item in ipairs(list) do
    if item.name == name then
      return index
    end
  end
  return 1
end

local function invoke(selection, input)
  local resolved, err = resolve(selection)
  if not resolved then
    return nil, err
  end
  -- The fallback draws the chain below the picker (user set_slot layers, then
  -- the slot default), so wrapping the default keeps working.
  if selection.name == FALLBACK_NAME then
    return pcall(input.prev, input.w, input.h, input.t, input.fade)
  end
  return pcall(selection.renderer, input.w, input.h, input.t, input.fade)
end

local function validate(selection)
  if selection.validated or selection.name == FALLBACK_NAME then
    return true
  end
  local ok, frame = invoke(selection, last_render or { w = 80, h = 24, t = 0, fade = 1 })
  if not ok then
    return nil, frame
  end
  selection.validated = true
  return true
end

local function state_dir()
  local dir = maki.env.state_dir()
  if dir then
    return maki.fs.joinpath(dir, STATE_DIR)
  end
  return nil
end

local function state_path()
  local dir = state_dir()
  if dir then
    return maki.fs.joinpath(dir, STATE_FILE)
  end
  return nil
end

local function load_preference()
  local path = state_path()
  if not path then
    return nil
  end
  local ok, content = pcall(maki.fs.read, path)
  if not content then
    return nil, not ok
  end
  local decoded, value = pcall(maki.json.decode, content)
  if decoded and type(value) == "table" and type(value.name) == "string" then
    return value.name, false
  end
  return nil, true
end

local function save_preference(name)
  local dir = state_dir()
  if not dir then
    return true
  end
  local ok, err = maki.fs.mkdir(dir, { parents = true })
  if not ok and err then
    return nil, err
  end
  return maki.fs.atomic_write(maki.fs.joinpath(dir, STATE_FILE), maki.json.encode({ name = name }))
end

local function queue_rollback(failed_persisted)
  if rollback_queued or not failed_persisted then
    return
  end
  rollback_queued = true
  maki.async.run(function()
    local ok, err = save_preference(committed.name)
    rollback_queued = false
    if not ok then
      maki.ui.flash("splash rollback failed: " .. tostring(err))
    end
  end)
end

local function stage(name)
  local selection = { name = name }
  local resolved, err = resolve(selection)
  if not resolved then
    candidate = nil
    return nil, err
  end
  candidate = selection
  return selection
end

local function cancel()
  candidate = nil
end

local function commit(selection, persist)
  selection = selection or candidate
  if not selection then
    return nil, "no splash selected"
  end
  local valid, validation_err = validate(selection)
  if not valid then
    cancel()
    return nil, validation_err
  end
  if persist then
    local saved, save_err = save_preference(selection.name)
    if not saved then
      cancel()
      return nil, save_err
    end
    selection.persisted = true
  end
  previous_committed, committed, candidate = committed, selection, nil
  rollback_queued = false
  return true
end

local function rollback()
  local failed = committed
  candidate = nil
  if previous_committed then
    committed, previous_committed = previous_committed, nil
  else
    committed = { name = FALLBACK_NAME }
  end
  queue_rollback(failed.persisted)
end

local function render(prev, w, h, t, fade)
  local input = { prev = prev, w = w, h = h, t = t, fade = fade }
  last_render = input
  -- A startup selection still pending here never became resolvable, so the
  -- name is a typo or its plugin never loaded. Report once on the first frame.
  if startup then
    local pending = startup
    startup = nil
    maki.async.run(function()
      maki.ui.flash("unknown splash: " .. pending)
    end)
  end
  if candidate then
    local pending = candidate
    local ok, frame = invoke(pending, input)
    if ok then
      pending.validated = true
      return frame
    end
    if candidate == pending then
      candidate = nil
    end
  end

  local ok, frame = invoke(committed, input)
  if ok then
    return frame
  end
  rollback()
  local restored, restored_frame = invoke(committed, input)
  if restored then
    return restored_frame
  end
  local failed = committed
  committed = { name = FALLBACK_NAME }
  queue_rollback(failed.persisted)
  return prev(w, h, t, fade)
end

local function select_and_commit(name, persist)
  if not candidate and committed.name == name then
    return true
  end
  local selection, err = stage(name)
  if not selection then
    return nil, err
  end
  return commit(selection, persist)
end

local function apply_startup()
  if not startup then
    return
  end
  local pending = startup
  if committed.name == pending then
    startup = nil
    return
  end
  local selection, err = stage(pending)
  if not selection then
    return
  end
  startup = nil
  maki.async.run(function()
    local ok, err = commit(selection, true)
    if not ok then
      maki.ui.flash("splash selection failed: " .. tostring(err))
    end
  end)
end

local function command(opts)
  local name = opts.fargs[1]
  if name then
    local ok, err = select_and_commit(name:lower(), true)
    if not ok then
      maki.ui.flash("splash selection failed: " .. tostring(err))
    end
    return
  end

  local picker_items = items()
  local result = maki.ui.open_list_picker(picker_items, {
    title = "Splash picker",
    cursor = item_index(picker_items, committed.name),
    timeout_ms = 100,
    notify_initial = true,
    on_change = function(item)
      local _, err = stage(item.name)
      if err then
        maki.ui.flash("splash preview failed: " .. tostring(err))
      end
    end,
  })
  if result and result.type == "choice" then
    local item = picker_items[result.index]
    local selection = candidate
    if not selection or selection.name ~= item.name then
      selection = stage(item.name)
    end
    local ok, err = commit(selection, true)
    if not ok then
      maki.ui.flash("splash selection failed: " .. tostring(err))
    end
  else
    cancel()
  end
end

maki.api.set_slot("splash.render", render)

maki.api.create_autocmd("StoreChanged", {
  callback = function(ev)
    if ev.data and ev.data.registry == REGISTRY then
      dirty = true
      apply_startup()
    end
  end,
})

-- Programmatic selection: any Lua context (config, keymap, tool) can fire
-- this. data = { name = <splash name>, persist = <boolean, default true> }.
maki.api.create_autocmd("SplashSelect", {
  callback = function(ev)
    local data = ev.data
    if type(data) ~= "table" or type(data.name) ~= "string" then
      maki.ui.flash("SplashSelect requires data.name (string)")
      return
    end
    maki.async.run(function()
      local ok, err = select_and_commit(data.name:lower(), data.persist ~= false)
      if not ok then
        maki.ui.flash("splash selection failed: " .. tostring(err))
      end
    end)
  end,
})

maki.api.register_command({
  name = "/splash",
  description = "Preview and select a splash renderer",
  argument_hint = "[splash]",
  nargs = "?",
  tui_only = true,
  completion = {
    get_items = function()
      local out = {}
      for _, item in ipairs(items()) do
        out[#out + 1] = {
          label = item.label,
          insertion = item.name,
          description = item.detail,
        }
      end
      return out
    end,
    on_highlight = function(_, item)
      local _, err = stage(item.insertion)
      if err then
        maki.ui.flash("splash preview failed: " .. tostring(err))
      end
    end,
    on_accept = function(_, item)
      local selection = candidate
      if not selection or selection.name ~= item.insertion then
        selection = stage(item.insertion)
      end
      local ok, err = commit(selection, true)
      if not ok then
        maki.ui.flash("splash selection failed: " .. tostring(err))
      end
    end,
    on_cancel = cancel,
  },
  handler = command,
})

-- Load time only reads the preference into `committed`; the first frame
-- resolves it. An unknown name fails then and rolls back to the fallback,
-- repairing the file.
local saved, invalid_saved = load_preference()
if saved then
  committed = { name = saved, persisted = true }
end
if invalid_saved then
  maki.async.run(function()
    local ok, err = save_preference(FALLBACK_NAME)
    if not ok then
      maki.ui.flash("splash rollback failed: " .. tostring(err))
    end
  end)
end
apply_startup()

return { render = render }
