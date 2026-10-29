local M = {}

-- Reads one event at a time. The caller owns and closes the response body.
-- An incomplete event at EOF is discarded; an empty data field is an event.
function M.new(body, max_event_bytes)
  assert(
    type(max_event_bytes) == "number"
      and max_event_bytes > 0
      and max_event_bytes < math.huge
      and max_event_bytes % 1 == 0,
    "max_event_bytes must be a positive integer"
  )
  local buffer = ""
  local data = {}
  local event_type
  local size = 0
  local eof = false
  local skip_lf = false
  local first_line = true
  local failed

  local function dispatch()
    local event
    if #data > 0 then
      event = { event = event_type or "message", data = table.concat(data, "\n") }
    end
    data = {}
    event_type = nil
    size = 0
    return event
  end

  return function()
    if failed then
      return nil, failed
    end
    while true do
      if skip_lf and #buffer > 0 then
        if buffer:sub(1, 1) == "\n" then
          buffer = buffer:sub(2)
        end
        skip_lf = false
      end
      local pos = buffer:find("[\r\n]")
      if pos then
        local line = buffer:sub(1, pos - 1)
        skip_lf = buffer:sub(pos, pos) == "\r"
        buffer = buffer:sub(pos + 1)
        if first_line then
          first_line = false
          if line:sub(1, 3) == "\239\187\191" then
            line = line:sub(4)
          end
        end
        if #line + 1 > max_event_bytes then
          failed = "SSE event exceeds max_event_bytes"
          return nil, failed
        end
        if line == "" then
          local event = dispatch()
          if event then
            return event
          end
        elseif line:sub(1, 1) ~= ":" then
          local colon = line:find(":", 1, true)
          local field = colon and line:sub(1, colon - 1) or line
          local value = colon and line:sub(colon + 1) or ""
          if value:sub(1, 1) == " " then
            value = value:sub(2)
          end
          if field == "data" or field == "event" then
            size = size + #line + 1
            if size > max_event_bytes then
              failed = "SSE event exceeds max_event_bytes"
              return nil, failed
            end
            if field == "data" then
              data[#data + 1] = value
            else
              event_type = value ~= "" and value or nil
            end
          end
        end
      elseif eof then
        buffer = ""
        data = {}
        return nil
      else
        local chunk, err = body:read()
        if not chunk then
          failed = err or "SSE body read failed"
          return nil, failed
        end
        if chunk == "" then
          eof = true
        else
          buffer = buffer .. chunk
          if not buffer:find("[\r\n]") and #buffer > max_event_bytes then
            failed = "SSE event exceeds max_event_bytes"
            return nil, failed
          end
        end
      end
    end
  end
end

return M
