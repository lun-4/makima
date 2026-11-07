-- Shared test helpers for Lua plugin specs.
--
-- Provides a lightweight test harness: `case` wraps each block in pcall so a
-- single failure does not abort the rest of the suite. Failures are collected
-- and surfaced by `report()` at the end.

local M = {}

local failures = {}

function M.case(name, fn)
  local ok, err = pcall(fn)
  if not ok then
    table.insert(failures, name .. ": " .. tostring(err))
  end
end

function M.eq(actual, expected, msg)
  if actual ~= expected then
    error((msg or "") .. "\nexpected: " .. tostring(expected) .. "\n  actual: " .. tostring(actual))
  end
end

function M.has(s, substr, msg)
  if not s:find(substr, 1, true) then
    error((msg or "") .. "\nexpected to contain: " .. tostring(substr) .. "\n  actual: " .. tostring(s))
  end
end

local _tmpdir_counter = 0

function M.mktmpdir(prefix)
  _tmpdir_counter = _tmpdir_counter + 1
  local name = "/tmp/maki_"
    .. (prefix or "test")
    .. "_"
    .. tostring(os.clock()):gsub("%.", "")
    .. "_"
    .. _tmpdir_counter
  maki.fs.mkdir(name)
  return name
end

function M.rmtree(dir)
  maki.fs.rm(dir, { recursive = true })
end

-- A scripted `maki.net.send` response that yields {chunks} in order. A
-- `read_error` is returned once the chunks run out instead of the end.
function M.response(chunks, opts)
  opts = opts or {}
  local index = 0
  local response = { status = opts.status or 200, headers = opts.headers or {}, closed = false }
  function response:read()
    index += 1
    if chunks[index] then
      return chunks[index]
    end
    return nil, opts.read_error
  end
  function response:close()
    self.closed = true
  end
  return response
end

-- A provider `ctx` whose `send` records each request and returns the next of
-- {responses}, and whose credentials live in memory. `refresh` and `lock`
-- run their function at once, without a lock.
function M.provider_ctx(responses, saved)
  local ctx = { id = "test", requests = {}, saved = saved }
  ctx.send = function(request)
    ctx.requests[#ctx.requests + 1] = request
    return table.remove(responses, 1)
  end
  ctx.credentials = {
    read = function()
      return ctx.saved
    end,
    write = function(_, value)
      ctx.saved = value
      return true
    end,
    delete = function()
      local existed = ctx.saved ~= nil
      ctx.saved = nil
      return existed
    end,
    refresh = function(_, fn)
      local value, err = fn(ctx.saved)
      if value == nil and err ~= nil then
        return nil, err
      end
      if value ~= nil then
        ctx.saved = value
      end
      return ctx.saved
    end,
    lock = function(_, fn)
      return fn()
    end,
  }
  -- `delivered_after` is how many requests went out before the first
  -- `ctx.mark_request_delivery()`; set `retired` to make it return false.
  ctx.mark_request_delivery = function()
    if ctx.retired then
      return false
    end
    ctx.delivered_after = ctx.delivered_after or #ctx.requests
    return true
  end
  return ctx
end

-- An `emit` for provider streams, plus the list it records into.
function M.recorder()
  local events = {}
  return function(event)
    events[#events + 1] = event
    return true
  end, events
end

-- Runs {fn} with `maki.uv.os_getenv` reading from {env} instead.
function M.with_env(env, fn)
  local getenv = maki.uv.os_getenv
  maki.uv.os_getenv = function(name)
    return env[name]
  end
  local ok, err = pcall(fn)
  maki.uv.os_getenv = getenv
  if not ok then
    error(err, 0)
  end
end

-- Compares JSON-like values structurally.
function M.same(actual, expected, path)
  path = path or "value"
  if type(expected) ~= "table" or type(actual) ~= "table" then
    M.eq(actual, expected, path)
    return
  end
  for key, value in pairs(expected) do
    M.same(actual[key], value, path .. "." .. tostring(key))
  end
  for key in pairs(actual) do
    if expected[key] == nil then
      error(path .. ": unexpected key " .. tostring(key))
    end
  end
end

function M.report()
  if #failures > 0 then
    error(#failures .. " case(s) failed:\n\n" .. table.concat(failures, "\n\n"))
  end
end

return M
