-- Pieces every streaming codec shares: JSON array tagging, error frames,
-- token counts, usage tables and minted tool ids.

local M = {}

-- JSON arrays carry mlua's array metatable, which hides behind
-- `__metatable = false`.
local ARRAY_MARK = getmetatable(makima.json.decode("[]"))
local UNMAPPED_STATUS = 400
local TOO_MANY_REQUESTS = 429
local FIRST_SERVER_ERROR = 500
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

-- Request fields that cap the output. A refusal naming one is about the cap
-- we sent, which compacting the session cannot fix.
local OUTPUT_CAP_FIELDS = { "max_tokens", "max_completion_tokens", "max_output_tokens" }
local OVERFLOW_SCOPES = { "context", "token", "prompt", "input" }
local OVERFLOW_WORDS = { "exceeds", "exceeded", "too long", "too many", "maximum" }
-- `10000 tokens (6000 in the messages, 4000 in the completion)`.
local COMPLETION_HALF = "(%d+)%s* in the completion"
-- `input length and max_tokens exceed context limit: 199773 + 8192 > 200000`.
local SUM_OVER_LIMIT = "%d%s* %+ %s*%d.- > %s*%d"
local BAD_REQUEST = 400
local TOO_LARGE = 413

local tool_id_seed
local tool_id_count = 0

-- An empty table that `makima.json.encode` writes as `[]`.
function M.array()
  return makima.json.decode("[]")
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

local function contains_any(text, words)
  for _, word in ipairs(words) do
    if text:find(word, 1, true) then
      return true
    end
  end
  return false
end

-- The prompt plus the output budget does not fit. Servers that enforce
-- `prompt + max_tokens <= window` name `max_tokens` here, so this is checked
-- before the output cap.
local function over_budget(message)
  if message:find(SUM_OVER_LIMIT) then
    return true
  end
  local completion = tonumber(message:match(COMPLETION_HALF))
  return completion ~= nil and completion > 0
end

-- Whether the server refused the request because it does not fit the
-- model's context, which the host answers by compacting the session.
function M.overflows(status, message)
  if status ~= BAD_REQUEST and status ~= TOO_LARGE then
    return false
  end
  local lower = message:lower()
  if over_budget(lower) or status == TOO_LARGE then
    return true
  end
  if contains_any(lower, OUTPUT_CAP_FIELDS) then
    return false
  end
  return contains_any(lower, OVERFLOW_SCOPES) and contains_any(lower, OVERFLOW_WORDS)
end

-- The retry class a response status names: a rate limit or a server error.
-- Any other status is not retried.
function M.status_retry(status)
  if status == TOO_MANY_REQUESTS then
    return "rate_limit"
  elseif status >= FIRST_SERVER_ERROR then
    return "transient"
  end
end

-- The error table the host reads as an API error, with `overflow = true`
-- when the request does not fit the model's context, and the retry class of
-- its status.
function M.api_error(status, message, retry_after)
  return {
    status = status,
    message = message,
    retry_after = retry_after,
    overflow = M.overflows(status, message) or nil,
    retry = M.status_retry(status),
  }
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
  return M.api_error(
    ERROR_STATUS[detail.code]
      or http_status(detail.code)
      or ERROR_STATUS[detail.type]
      or ERROR_STATUS[metadata.error_type]
      or UNMAPPED_STATUS,
    message
  )
end

-- Decodes streamed tool arguments. Anything that is not JSON becomes `{}`.
function M.tool_input(arguments)
  local input = makima.json.decode(arguments)
  if input == nil then
    return {}
  end
  return input
end

-- An id for a tool call the provider left unnamed. The seed differs per run,
-- so a resumed session never mints an id its transcript already holds.
function M.tool_id(prefix)
  if not tool_id_seed then
    local now = makima.time.now()
    tool_id_seed = now.secs .. "." .. now.nanosecs
  end
  tool_id_count = tool_id_count + 1
  local digest = makima.crypto.sha256(tool_id_seed .. ":" .. tool_id_count):sub(1, TOOL_ID_HEX_BYTES)
  return prefix .. digest:gsub(".", function(byte)
    return string.format("%02x", string.byte(byte))
  end)
end

return M
