-- HTTP plumbing every provider shares: API key lookup, configured headers,
-- error responses and buffered bodies.

local codec = require("providers.codec")

local M = {}

local MAX_ERROR_BYTES = 64 * 1024
local MAX_JSON_BYTES = 16 * 1024 * 1024
local ERROR_BODY_TRUNCATED = "\n[response body truncated]"
local RETRY_AFTER_HEADER = "retry-after"
local CONTENT_TYPE = "Content-Type"
local JSON_TYPE = "application/json"
-- How `maki.net.send` reports a connection never made and a stalled read.
local CONNECT_FAILED = "could not connect"
local IDLE_TIMEOUT = "idle timeout"

M.CONFIG_KIND = "config"

local function trim(text)
  return text:match("^%s*(.-)%s*$")
end

-- A failure the host does not retry, for missing keys, settings or
-- credentials that only the user can fix.
function M.config_error(message)
  return { kind = M.CONFIG_KIND, message = message }
end

-- The text of an error, whether a string or a table.
function M.message(err)
  return type(err) == "table" and err.message or err
end

-- {err} with the retry class the host reads. A table that names one, and a
-- config error, stay as they are. A status names it through
-- `codec.status_retry`; anything else is a transport failure, classed by
-- the `maki.net.send` text it carries, even inside a provider's own words:
-- a connection never made, a stalled read, or else a transient failure.
function M.classify(err)
  if type(err) ~= "table" then
    err = { message = tostring(err) }
  end
  if err.retry or err.kind == M.CONFIG_KIND then
    return err
  end
  local classified = table.clone(err)
  if err.status then
    classified.retry = codec.status_retry(err.status)
  else
    local message = err.message or ""
    if message:find(CONNECT_FAILED, 1, true) then
      classified.retry = "connect"
    elseif message:find(IDLE_TIMEOUT, 1, true) then
      classified.retry = "timeout"
    else
      classified.retry = "transient"
    end
  end
  return classified
end

-- Passes every `stream` failure of {instance} through `M.classify`, in
-- place, so a provider retries like the native ones did without classing
-- each error itself.
function M.classify_stream(instance)
  local stream = instance.stream
  instance.stream = function(ctx, request, emit)
    local result, err = stream(ctx, request, emit)
    if result == nil and err ~= nil then
      return nil, M.classify(err)
    end
    return result, err
  end
end

local function env_keys(env_var)
  local keys = {}
  for part in (maki.uv.os_getenv(env_var) or ""):gmatch("[^,]+") do
    local key = trim(part)
    if key ~= "" then
      keys[#keys + 1] = key
    end
  end
  return #keys > 0 and keys or nil
end

-- Read a comma-separated key pool from {env_var}, with no credential fallback.
function M.env_api_keys(ctx, env_var)
  local keys = env_var and env_keys(env_var)
  if keys then
    return keys, "env"
  end
  local missing = env_var and env_var .. " not set" or "no API key environment variable"
  return nil, M.config_error(string.format("%s for '%s'", missing, ctx.id))
end

-- Keys in precedence order: {env_var} (or `config.api_key_env`) as a
-- comma-separated pool, then the saved `api_key`, then `config.api_key`. An
-- empty variable counts as unset. Returns the keys and their source (`env`,
-- `saved` or `configured`, which are also `auth_status` states), or nil plus
-- a config error that says how to log in.
function M.api_keys(ctx, config, env_var)
  env_var = config.api_key_env or env_var
  local keys = env_var and env_keys(env_var)
  if keys then
    return keys, "env"
  end
  local saved, err = ctx.credentials:read()
  if saved and type(saved.api_key) == "string" and saved.api_key ~= "" then
    return { saved.api_key }, "saved"
  end
  if type(config.api_key) == "string" and config.api_key ~= "" then
    return { config.api_key }, "configured"
  end
  if err then
    return nil, err
  end
  local missing = env_var and env_var .. " not set and no saved credentials" or "no saved credentials"
  return nil, M.config_error(string.format("%s for '%s'; run `makima auth login %s`", missing, ctx.id, ctx.id))
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
-- expanded and set over it, or nil plus a config error naming the missing
-- variable.
function M.headers(id, base, configured)
  local result = table.clone(base)
  for name, value in pairs(configured or {}) do
    local expanded, missing = M.expand_env(value)
    if not expanded then
      return nil,
        M.config_error(
          string.format("provider '%s' header '%s': environment variable '%s' is unset or empty", id, name, missing)
        )
    end
    M.set_header(result, name, expanded)
  end
  return result
end

-- Percent-encodes everything but RFC 3986 unreserved characters.
function M.urlencode(text)
  return (text:gsub("[^%w%-._~]", function(char)
    return string.format("%%%02X", char:byte())
  end))
end

-- An `application/x-www-form-urlencoded` body from ordered `{ name, value }`
-- pairs.
function M.form(fields)
  local parts = {}
  for _, field in ipairs(fields) do
    parts[#parts + 1] = field[1] .. "=" .. M.urlencode(field[2])
  end
  return table.concat(parts, "&")
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
  local chunks, size, truncated = {}, 0, false
  while true do
    local chunk = response:read()
    if not chunk then
      break
    end
    local remaining = MAX_ERROR_BYTES - size
    if #chunk > remaining then
      chunks[#chunks + 1] = chunk:sub(1, remaining)
      truncated = true
      break
    end
    chunks[#chunks + 1] = chunk
    size = size + #chunk
  end
  if not truncated and response:read() then
    truncated = true
  end
  response:close()
  local message = table.concat(chunks)
  if truncated then
    message = message:sub(1, MAX_ERROR_BYTES - #ERROR_BODY_TRUNCATED) .. ERROR_BODY_TRUNCATED
  end
  if trim(message) == "" then
    message = "HTTP " .. response.status
  end
  return codec.api_error(response.status, message, M.retry_after(response))
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

-- POSTs {body} as JSON to {url} and decodes the reply with the codec
-- {stream}`(response, emit)`, closing the response either way.
function M.post_stream(ctx, url, headers, body, stream, emit)
  headers = table.clone(headers)
  M.set_header(headers, CONTENT_TYPE, JSON_TYPE)
  local response, err =
    M.send(ctx, { url = url, method = "POST", headers = headers, body = assert(maki.json.encode(body)) })
  if not response then
    return nil, err
  end
  local result, stream_err = stream(response, emit)
  response:close()
  return result, stream_err
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
    size = size + #chunk
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
