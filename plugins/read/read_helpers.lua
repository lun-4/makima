local M = {}

function M.source_lines(content)
  local lines = {}
  local pos = 1
  while pos <= #content do
    local nl = content:find("\n", pos, true)
    local raw_end = nl and nl - 1 or #content
    local text_end = raw_end
    if nl and content:sub(raw_end, raw_end) == "\r" then
      text_end = raw_end - 1
    end
    lines[#lines + 1] = {
      text = content:sub(pos, text_end),
      start_byte = pos - 1,
      text_end_byte = text_end,
      terminator_end_byte = nl or text_end,
      terminated = nl ~= nil,
    }
    pos = nl and nl + 1 or #content + 1
  end
  return lines
end

function M.split_lines(content)
  local lines = {}
  for i, line in ipairs(M.source_lines(content)) do
    lines[i] = line.text
  end
  return lines
end

function M.utf8_prefix(text, max_bytes)
  if #text <= max_bytes then
    return text, #text
  end
  local boundary = max_bytes + 1
  while boundary > 1 and text:byte(boundary) and text:byte(boundary) >= 0x80 and text:byte(boundary) < 0xC0 do
    boundary = boundary - 1
  end
  return text:sub(1, boundary - 1), boundary - 1
end

return M
