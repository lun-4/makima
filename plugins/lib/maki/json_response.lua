local M = {}

-- The caller owns the response body and must close it, including on failure.
-- By default JSON null decodes to nil; check the error rather than the value.
-- Set opts.preserve_null to retain nulls as JsonNull values.
function M.read(body, max_bytes, opts)
  assert(
    type(max_bytes) == "number" and max_bytes >= 0 and max_bytes < math.huge and max_bytes % 1 == 0,
    "max_bytes must be a non-negative integer"
  )
  local chunks = {}
  local size = 0
  while true do
    local chunk, err = body:read()
    if err then
      if type(err) == "table" then
        return nil, err
      end
      return nil, { kind = "network", message = tostring(err) }
    end
    if chunk == nil then
      return nil, { kind = "network", message = "response body read failed" }
    end
    if chunk == "" then
      break
    end
    size = size + #chunk
    if size > max_bytes then
      return nil, { kind = "api", message = "JSON response exceeds max_bytes" }
    end
    chunks[#chunks + 1] = chunk
  end
  local value, err = maki.json.decode(table.concat(chunks), opts)
  if err then
    return nil, { kind = "api", message = "invalid JSON response: " .. tostring(err) }
  end
  return value, nil
end

return M
