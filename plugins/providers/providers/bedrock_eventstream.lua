-- AWS EventStream framing for Bedrock's InvokeModelWithResponseStream. The
-- frame CRCs are not checked; TLS already guards the bytes in transit.

local anthropic = require("providers.anthropic")
local codec = require("providers.codec")

local M = {}

local PRELUDE_BYTES = 12
local MIN_FRAME_BYTES = 16
local TRAILER_BYTES = 4
local STRING_HEADER = 7
local BYTES_HEADER = 6
local FIXED_HEADER_BYTES = { [0] = 0, [1] = 0, [2] = 1, [3] = 2, [4] = 4, [5] = 8, [8] = 8, [9] = 16 }
local UNKNOWN_EXCEPTION_STATUS = 500
local EXCEPTION_STATUS = {
  throttlingexception = 429,
  validationexception = 400,
  modeltimeoutexception = 500,
  serviceunavailableexception = 503,
  internalserverexception = 500,
}
local INVALID = "Bedrock EventStream: "

local function uint(bytes, offset, count)
  local value = 0
  for index = offset, offset + count - 1 do
    value = value * 256 + string.byte(bytes, index)
  end
  return value
end

-- String headers by name. Other header types are skipped.
local function parse_headers(bytes)
  local headers, pos = {}, 1
  while pos <= #bytes do
    local name_length = string.byte(bytes, pos)
    local name = bytes:sub(pos + 1, pos + name_length)
    local kind = string.byte(bytes, pos + 1 + name_length)
    pos = pos + 2 + name_length
    local length = FIXED_HEADER_BYTES[kind]
    if (kind == STRING_HEADER or kind == BYTES_HEADER) and pos + 1 <= #bytes then
      length = uint(bytes, pos, 2)
      pos = pos + 2
    end
    if not length or pos + length - 1 > #bytes then
      return nil, INVALID .. "malformed header"
    end
    if kind == STRING_HEADER then
      headers[name] = bytes:sub(pos, pos + length - 1)
    end
    pos = pos + length
  end
  return headers
end

-- Returns `next()`, which yields `{ headers, payload }` per frame, nil at the
-- end of the body, or nil plus an error. The caller closes the response.
function M.frames(response)
  local pending, offset = "", 1

  local function take(count)
    local parts, size = {}, 0
    while size < count do
      if offset > #pending then
        local chunk, err = response:read()
        if err then
          return nil, err
        end
        if not chunk then
          if size == 0 then
            return ""
          end
          return nil, INVALID .. "truncated frame"
        end
        pending, offset = chunk, 1
      end
      local length = math.min(count - size, #pending - offset + 1)
      parts[#parts + 1] = pending:sub(offset, offset + length - 1)
      offset = offset + length
      size = size + length
    end
    return table.concat(parts)
  end

  return function()
    local prelude, err = take(PRELUDE_BYTES)
    if not prelude or prelude == "" then
      return nil, err
    end
    local total, header_length = uint(prelude, 1, 4), uint(prelude, 5, 4)
    if total < MIN_FRAME_BYTES or header_length > total - MIN_FRAME_BYTES then
      return nil, INVALID .. "invalid frame lengths"
    end
    local rest, rest_err = take(total - PRELUDE_BYTES)
    if not rest or #rest < total - PRELUDE_BYTES then
      return nil, rest_err or INVALID .. "truncated frame"
    end
    local headers, header_err = parse_headers(rest:sub(1, header_length))
    if not headers then
      return nil, header_err
    end
    return { headers = headers, payload = rest:sub(header_length + 1, #rest - TRAILER_BYTES) }
  end
end

-- The Messages event inside a `chunk` frame: `{ bytes }` holding base64 JSON.
local function chunk_event(payload)
  local envelope = makima.json.decode(payload)
  if type(envelope) ~= "table" then
    return nil, INVALID .. "invalid chunk"
  end
  local value = envelope
  if type(envelope.bytes) == "string" then
    value = makima.json.decode(makima.base64.decode(envelope.bytes))
  end
  if type(value) ~= "table" then
    return nil, INVALID .. "invalid chunk payload"
  end
  return type(value.type) == "string" and value.type or "", value
end

-- Returns `next()` in the shape `anthropic.decode` reads: an event type and
-- its payload. An exception frame ends the stream with an API error.
function M.events(response)
  local next_frame = M.frames(response)
  return function()
    while true do
      local frame, err = next_frame()
      if not frame then
        return nil, err
      end
      local exception = frame.headers[":exception-type"]
      if exception then
        return nil,
          codec.api_error(
            EXCEPTION_STATUS[exception:lower()] or UNKNOWN_EXCEPTION_STATUS,
            utf8.len(frame.payload) and frame.payload or exception
          )
      end
      if frame.headers[":event-type"] == "chunk" and frame.payload ~= "" then
        return chunk_event(frame.payload)
      end
    end
  end
end

-- Decodes a Bedrock Anthropic stream into `{ message, usage, stop_reason }`.
function M.stream(response, emit)
  return anthropic.decode(M.events(response), emit)
end

return M
