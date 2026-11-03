-- Pieces every streaming codec shares: JSON array tagging, error frames,
-- token counts, usage tables and minted tool ids.

local M = {}

-- JSON arrays carry mlua's array metatable, which hides behind
-- `__metatable = false`.
local ARRAY_MARK = getmetatable(maki.json.decode("[]"))
local UNMAPPED_STATUS = 400
local EMPTY_FRAME_MESSAGE = "provider sent an error frame with no detail"
local TOOL_ID_HEX_BYTES = 4
local ERROR_STATUS = {
  overloaded_error = 529,
  server_is_overloaded = 529,
  service_unavailable_error = 503,
  provider_overloaded = 503,
  provider_unavailable = 502,
  api_error = 500,
  server_error = 500,
  rate_limit_error = 429,
  rate_limit_exceeded = 429,
  tokens = 429,
  request_too_large = 413,
  not_found_error = 404,
  permission_error = 403,
  billing_error = 402,
  insufficient_quota = 402,
  authentication_error = 401,
  invalid_api_key = 401,
}

local tool_id_seed
local tool_id_count = 0

-- An empty table that `maki.json.encode` writes as `[]`.
function M.array()
  return maki.json.decode("[]")
end

function M.is_array(value)
  return type(value) == "table" and getmetatable(value) == ARRAY_MARK
end

-- A JSON number as a non-negative integer; anything else counts as 0.
function M.count(value)
  return type(value) == "number" and math.max(0, math.floor(value)) or 0
end

function M.usage()
  return { input_tokens = 0, output_tokens = 0, cache_creation_input_tokens = 0, cache_read_input_tokens = 0 }
end

local function http_status(code)
  local status = tonumber(code)
  if status and status % 1 == 0 and status >= 100 and status < 600 then
    return status
  end
end

-- The HTTP status an error code or type stands for, if it is a known one.
function M.error_status(code)
  return ERROR_STATUS[code]
end

-- The stream error for an `error` object sent inside an event stream.
function M.frame_error(detail)
  detail = type(detail) == "table" and detail or {}
  local metadata = type(detail.metadata) == "table" and detail.metadata or {}
  local message = detail.message
  if type(message) ~= "string" or message:match("^%s*$") then
    message = EMPTY_FRAME_MESSAGE
  end
  return {
    status = ERROR_STATUS[detail.code]
      or http_status(detail.code)
      or ERROR_STATUS[detail.type]
      or ERROR_STATUS[metadata.error_type]
      or UNMAPPED_STATUS,
    message = message,
  }
end

-- Decodes streamed tool arguments. Anything that is not JSON becomes `{}`.
function M.tool_input(arguments)
  local input = maki.json.decode(arguments)
  if input == nil then
    return {}
  end
  return input
end

-- An id for a tool call the provider left unnamed. The seed differs per run,
-- so a resumed session never mints an id its transcript already holds.
function M.tool_id(prefix)
  if not tool_id_seed then
    local now = maki.time.now()
    tool_id_seed = now.secs .. "." .. now.nanosecs
  end
  tool_id_count += 1
  local digest = maki.crypto.sha256(tool_id_seed .. ":" .. tool_id_count):sub(1, TOOL_ID_HEX_BYTES)
  return prefix .. digest:gsub(".", function(byte)
    return string.format("%02x", string.byte(byte))
  end)
end

return M
