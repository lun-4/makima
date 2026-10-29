local messages = require("maki.anthropic")
local sse = require("maki.sse")
local M = {}

local profiles = {
  { "claude-haiku-4-5", "weak", true, 64000, 1, 5, 1.25, 0.1 },
  { "claude-sonnet-4-5", "medium", false, 64000, 3, 15, 3.75, 0.3 },
  { "claude-sonnet-4-6", "medium", false, 64000, 3, 15, 3.75, 0.3 },
  { "claude-sonnet-5", "medium", true, 128000, 2, 10, 2.5, 0.2 },
  { "claude-sonnet-4", "medium", false, 64000, 3, 15, 3.75, 0.3 },
  { "claude-opus-4-5", "strong", false, 64000, 5, 25, 6.25, 0.5 },
  { "claude-opus-4-6", "strong", false, 128000, 5, 25, 6.25, 0.5 },
  { "claude-opus-4-7", "strong", false, 128000, 5, 25, 6.25, 0.5 },
  { "claude-opus-4-8", "strong", false, 128000, 5, 25, 6.25, 0.5, true },
  { "claude-opus-5", "strong", true, 128000, 5, 25, 6.25, 0.5, true },
  { "claude-fable-5", "strong", false, 128000, 10, 50, 12.5, 1 },
  { "claude-opus-4-0", "strong", false, 32000, 15, 75, 18.75, 1.5 },
  { "claude-opus-4-1", "strong", false, 32000, 15, 75, 18.75, 1.5 },
}
M.models = {}
for _, p in ipairs(profiles) do
  M.models[#M.models + 1] = {
    id = p[1],
    prefixes = { p[1] },
    tier = p[2],
    default = p[3],
    family = "claude",
    context_window = 200000,
    max_output_tokens = p[4],
    vision = true,
    fast = p[9] or false,
    pricing = {
      input = p[5],
      output = p[6],
      cache_write = p[7],
      cache_read = p[8],
      fast = p[9] and { input = 10, output = 50 } or nil,
    },
    thinking = { mode = "adaptive", levels = { "minimal", "low", "medium", "high", "xhigh", "max" } },
  }
end

function M.stream(response, emit)
  local next_event = sse.new(response, 1024 * 1024)
  local content, arguments = {}, {}
  local usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }
  local stop_reason
  while true do
    local event, err = next_event()
    if not event then
      if err then
        return nil, type(err) == "table" and err or { kind = "network", message = tostring(err) }
      end
      break
    end
    local value = maki.json.decode(event.data)
    if type(value) ~= "table" then
      value = {}
    end
    local kind = event.event
    if kind == "message_start" and type(value.message) == "table" and type(value.message.usage) == "table" then
      local u = value.message.usage
      usage = {
        input = u.input_tokens or 0,
        output = u.output_tokens or 0,
        cache_creation = u.cache_creation_input_tokens or 0,
        cache_read = u.cache_read_input_tokens or 0,
      }
    elseif kind == "content_block_start" and type(value.index) == "number" and type(value.content_block) == "table" then
      local b = value.content_block
      local block
      if b.type == "text" then
        block = { type = "text", text = "" }
      elseif b.type == "thinking" then
        block = { type = "thinking", thinking = "" }
      elseif b.type == "redacted_thinking" then
        block = { type = b.type, data = b.data }
      elseif b.type == "tool_use" then
        block = { type = b.type, id = b.id, name = b.name, input = maki.json.null() }
        arguments[value.index] = {}
        local ok, emit_err = emit({ type = "tool_use_start", id = b.id, name = b.name })
        if not ok then
          return nil, emit_err
        end
      end
      if block then
        content[value.index + 1] = block
      end
    elseif kind == "content_block_delta" and type(value.index) == "number" and type(value.delta) == "table" then
      local b, d = content[value.index + 1], value.delta
      if d.type == "text_delta" or d.type == "thinking_delta" then
        local field = d.type == "text_delta" and "text" or "thinking"
        local text = d[field]
        if type(text) == "string" and text ~= "" then
          if b and b[field] then
            b[field] = b[field] .. text
          end
          local ok, emit_err = emit({ type = d.type, text = text })
          if not ok then
            return nil, emit_err
          end
        end
      elseif d.type == "signature_delta" and b and b.type == "thinking" then
        b.signature = d.signature
      elseif d.type == "input_json_delta" and arguments[value.index] then
        arguments[value.index][#arguments[value.index] + 1] = d.partial_json or ""
      end
    elseif kind == "content_block_stop" and type(value.index) == "number" then
      local b = content[value.index + 1]
      if b and b.type == "tool_use" then
        b.input = maki.json.decode(table.concat(arguments[value.index] or {}), { preserve_null = true }) or {}
      end
    elseif kind == "message_delta" then
      if type(value.usage) == "table" then
        usage.output = value.usage.output_tokens or 0
      end
      if type(value.delta) == "table" and type(value.delta.stop_reason) == "string" then
        stop_reason = ({ end_turn = "end_turn", tool_use = "tool_use", max_tokens = "max_tokens" })[value.delta.stop_reason]
          or "end_turn"
      end
    elseif kind == "error" then
      return nil, { kind = "api", status = 400, message = "Anthropic stream failed" }
    elseif kind == "message_stop" then
      break
    end
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }
end

function M.prepare(_, model, config)
  config = config or {}
  local key = maki.uv.os_getenv(config.api_key_env or "ANTHROPIC_API_KEY")
  if not key or key == "" then
    key = config.api_key
  end
  if not key or key == "" then
    return nil, { kind = "config", message = "Anthropic API key is not configured" }
  end
  local keys = {}
  for part in key:gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "Anthropic API key is not configured" }
  end
  local index = 1
  local base = maki.uv.os_getenv("ANTHROPIC_BASE_URL")
  if not base or base == "" then
    base = config.base_url or "https://api.anthropic.com"
  end
  base = base:match("^[^?#]*"):gsub("/+$", ""):gsub("/v1/messages$", "")
  return {
    model = model,
    key_count = #keys,
    rotate_key = function()
      if #keys < 2 then
        return false
      end
      index = index % #keys + 1
      return true
    end,
    stream = function(ctx, request, emit)
      local body, err = messages.build_request(request)
      if not body then
        return nil, { kind = "config", message = tostring(err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local headers =
        { ["x-api-key"] = keys[index], ["anthropic-version"] = "2023-06-01", ["content-type"] = "application/json" }
      headers["anthropic-beta"] = messages.beta_header(request)
      for name, value in pairs(config.headers or {}) do
        for existing in pairs(headers) do
          if existing:lower() == name:lower() then
            headers[existing] = nil
          end
        end
        headers[name] = value
      end
      local response, send_err =
        ctx:send({ url = base .. "/v1/messages", method = "POST", headers = headers, body = encoded })
      if not response then
        return nil, type(send_err) == "table" and send_err or { kind = "network", message = "Anthropic request failed" }
      end
      if response.status ~= 200 then
        response:close()
        return nil, { kind = "api", status = response.status, message = "Anthropic request failed" }
      end
      local result, stream_err = M.stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

function M.register()
  maki.api.register_provider({ id = "anthropic", display_name = "Anthropic", models = M.models, prepare = M.prepare })
end

return M
