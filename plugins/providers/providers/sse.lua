-- Server-sent events over a `maki.net.send` response.

local M = {}

local BOM = "\239\187\191"
local DEFAULT_EVENT = "message"

-- Returns `next()`, which yields `{ event, data }` per event, nil at the end
-- of the body, or nil plus the read error. The caller closes the response.
-- An event still open when the body ends is dispatched.
function M.events(response)
  local buffer, pos = "", 1
  local partial = {}
  local data, event_type = {}, nil
  local eof, first_line, skip_lf = false, true, false

  local function dispatch()
    local event
    if #data > 0 then
      event = { event = event_type or DEFAULT_EVENT, data = table.concat(data, "\n") }
    end
    data, event_type = {}, nil
    return event
  end

  local function process(line)
    if first_line then
      first_line = false
      if line:sub(1, #BOM) == BOM then
        line = line:sub(#BOM + 1)
      end
    end
    if line == "" then
      return dispatch()
    end
    local colon = line:find(":", 1, true)
    if colon == 1 then
      return
    end
    local field = colon and line:sub(1, colon - 1) or line
    local value = colon and line:sub(colon + (line:sub(colon + 1, colon + 1) == " " and 2 or 1)) or ""
    if field == "data" then
      data[#data + 1] = value
    elseif field == "event" then
      event_type = value ~= "" and value or nil
    end
  end

  -- The next complete line, or nil once the buffer needs another read.
  local function next_line()
    if skip_lf and pos <= #buffer then
      if buffer:sub(pos, pos) == "\n" then
        pos = pos + 1
      end
      skip_lf = false
    end
    local stop = buffer:find("[\r\n]", pos)
    if not stop then
      return nil
    end
    local line = buffer:sub(pos, stop - 1)
    if #partial > 0 then
      partial[#partial + 1] = line
      line = table.concat(partial)
      partial = {}
    end
    skip_lf = buffer:sub(stop, stop) == "\r"
    pos = stop + 1
    return line
  end

  return function()
    while true do
      local line = next_line()
      if line then
        local event = process(line)
        if event then
          return event
        end
      elseif eof then
        return nil
      else
        if pos <= #buffer then
          partial[#partial + 1] = buffer:sub(pos)
        end
        local chunk, err = response:read()
        if err then
          return nil, err
        end
        if chunk then
          buffer, pos = chunk, 1
        else
          eof = true
          buffer, pos = "", 1
          if #partial > 0 then
            process(table.concat(partial))
            partial = {}
          end
          local event = dispatch()
          if event then
            return event
          end
        end
      end
    end
  end
end

return M
