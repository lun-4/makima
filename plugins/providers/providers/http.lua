-- HTTP plumbing every provider shares: API key lookup, configured headers,
-- error responses and buffered bodies.

local M = {}

local MAX_ERROR_BYTES = 64 * 1024
local MAX_JSON_BYTES = 16 * 1024 * 1024
local RETRY_AFTER_HEADER = "retry-after"

local function trim(text)
  return text:match("^%s*(.-)%s*$")
end

-- Keys in precedence order: {env_var} (or `config.api_key_env`) as a
-- comma-separated pool, then the saved `api_key`, then `config.api_key`. An
-- empty variable counts as unset. Returns the keys and their source (`env`,
-- `saved` or `configured`, which are also `auth_status` states), or nil plus
-- an error that says how to log in.
function M.api_keys(ctx, config, env_var)
  env_var = config.api_key_env or env_var
  if env_var then
    local keys = {}
    for part in (maki.uv.os_getenv(env_var) or ""):gmatch("[^,]+") do
      local key = trim(part)
      if key ~= "" then
        keys[#keys + 1] = key
      end
    end
    if #keys > 0 then
      return keys, "env"
    end
  end
  local saved, err = ctx.credentials:read()
  if err then
    return nil, err
  end
  if saved and type(saved.api_key) == "string" and saved.api_key ~= "" then
    return { saved.api_key }, "saved"
  end
  if type(config.api_key) == "string" and config.api_key ~= "" then
    return { config.api_key }, "configured"
  end
  local missing = env_var and env_var .. " not set and no saved credentials" or "no saved credentials"
  return nil, string.format("%s for '%s'; run `maki auth login %s`", missing, ctx.id, ctx.id)
end

-- Round-robin over {keys}, for an instance's `key_count` and `rotate_key`.
function M.key_pool(keys)
  local index = 1
  return {
    count = #keys,
    current = function()
      return keys[index]
    end,
    rotate = function()
      if #keys < 2 then
        return false
      end
      index = index % #keys + 1
      return true
    end,
  }
end

-- Expands `${VAR}` from the environment, like `maki_config::expand_env`. An
-- unset or empty variable returns nil plus its name. An unterminated `${`
-- stays literal.
function M.expand_env(value)
  local parts, pos = {}, 1
  while true do
    local start = value:find("${", pos, true)
    local finish = start and value:find("}", start + 2, true)
    if not finish then
      break
    end
    local name = value:sub(start + 2, finish - 1)
    local replacement = maki.uv.os_getenv(name)
    if not replacement or replacement == "" then
      return nil, name
    end
    parts[#parts + 1] = value:sub(pos, start - 1)
    parts[#parts + 1] = replacement
    pos = finish + 1
  end
  parts[#parts + 1] = value:sub(pos)
  return table.concat(parts)
end

-- Sets {name}, replacing any header that differs only in case.
function M.set_header(headers, name, value)
  local lower = name:lower()
  for existing in pairs(headers) do
    if existing:lower() == lower then
      headers[existing] = nil
    end
  end
  headers[name] = value
end

-- A copy of {base} with the `providers.toml` headers of provider {id}
-- expanded and set over it, or nil plus an error naming the missing variable.
function M.headers(id, base, configured)
  local result = table.clone(base)
  for name, value in pairs(configured or {}) do
    local expanded, missing = M.expand_env(value)
    if not expanded then
      return nil,
        string.format("provider '%s' header '%s': environment variable '%s' is unset or empty", id, name, missing)
    end
    M.set_header(result, name, expanded)
  end
  return result
end

-- Seconds from a `Retry-After` header in delay-seconds form, if positive.
function M.retry_after(response)
  local value = response.headers[RETRY_AFTER_HEADER]
  local secs = value and tonumber(value:match("^%s*(%d+)%s*$"))
  if secs and secs > 0 then
    return secs
  end
end

-- Closes a failed {response} and returns the stream error for it: the status,
-- the start of the body as the message, and `retry_after`.
function M.error(response)
  local chunks, size = {}, 0
  while size < MAX_ERROR_BYTES do
    local chunk = response:read()
    if not chunk then
      break
    end
    chunks[#chunks + 1] = chunk
    size += #chunk
  end
  response:close()
  local message = table.concat(chunks):sub(1, MAX_ERROR_BYTES)
  if trim(message) == "" then
    message = "HTTP " .. response.status
  end
  return { status = response.status, message = message, retry_after = M.retry_after(response) }
end

-- `ctx.send(request)`, with a non-2xx response turned into nil plus `M.error`.
-- A transport failure is nil plus its message, which the host retries.
function M.send(ctx, request)
  local response, err = ctx.send(request)
  if not response then
    return nil, err
  end
  if response.status < 200 or response.status >= 300 then
    return nil, M.error(response)
  end
  return response
end

-- Reads the rest of the body, or nil plus an error once it passes {max_bytes}.
function M.read_body(response, max_bytes)
  local chunks, size = {}, 0
  while true do
    local chunk, err = response:read()
    if err then
      return nil, err
    end
    if not chunk then
      return table.concat(chunks)
    end
    size += #chunk
    if size > max_bytes then
      return nil, "response body exceeds " .. max_bytes .. " bytes"
    end
    chunks[#chunks + 1] = chunk
  end
end

-- Reads, closes and decodes a JSON {response}. A JSON null is nil without an
-- error.
function M.read_json(response, max_bytes)
  local body, err = M.read_body(response, max_bytes or MAX_JSON_BYTES)
  response:close()
  if not body then
    return nil, err
  end
  local value, decode_err = maki.json.decode(body)
  if decode_err then
    return nil, "invalid JSON response: " .. decode_err
  end
  return value
end

return M
