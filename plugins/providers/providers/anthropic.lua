-- Anthropic Messages: request encoding and stream decoding, shared by the
-- Anthropic API and Bedrock.

local codec = require("providers.codec")
local sse = require("providers.sse")
local thinking = require("providers.thinking")

local M = {}

M.LONG_CONTEXT_BETA = "context-1m-2025-08-07"

local FALLBACK_MAX_TOKENS = 32000
local EMPTY_TEXT = "(empty)"
local EPHEMERAL = { type = "ephemeral" }
local CACHED_TAIL_MESSAGES = 2
local LONG_CONTEXT_SUFFIX = "%-1m$"
local FAST_BETA = "fast-mode-2026-02-01"
local UNPARSED_ERROR_STATUS = 400
local OPUS = "opus"
local ADAPTIVE_SINCE = { 5, 0 }
local ADAPTIVE_SINCE_OPUS = { 4, 7 }
local STOP_REASONS = { end_turn = "end_turn", tool_use = "tool_use", max_tokens = "max_tokens" }

local function wire_block(block)
  if block.type == "text" then
    return { type = "text", text = block.text }
  elseif block.type == "thinking" then
    return { type = "thinking", thinking = block.thinking, signature = block.signature }
  elseif block.type == "redacted_thinking" then
    return { type = "redacted_thinking", data = block.data }
  elseif block.type == "tool_use" then
    return { type = "tool_use", id = block.id, name = block.name, input = block.input }
  elseif block.type == "tool_result" then
    return {
      type = "tool_result",
      tool_use_id = block.tool_use_id,
      content = block.content,
      is_error = block.is_error or nil,
    }
  elseif block.type == "image" then
    return {
      type = "image",
      source = { type = "base64", media_type = block.source.media_type, data = block.source.data },
    }
  end
end

-- Blank text blocks are dropped, an empty message says so, and the last
-- non-thinking block of the final two messages is marked for caching.
function M.convert_messages(messages)
  local result = {}
  for index, message in ipairs(messages) do
    local content = {}
    for _, block in ipairs(message.content) do
      if block.type ~= "text" or not block.text:match("^%s*$") then
        content[#content + 1] = wire_block(block)
      end
    end
    if #content == 0 then
      content[1] = { type = "text", text = EMPTY_TEXT }
    end
    if index + CACHED_TAIL_MESSAGES > #messages then
      for i = #content, 1, -1 do
        if content[i].type ~= "thinking" and content[i].type ~= "redacted_thinking" then
          content[i].cache_control = EPHEMERAL
          break
        end
      end
    end
    result[#result + 1] = { role = message.role, content = content }
  end
  return result
end

function M.convert_tools(tools)
  local result = codec.array()
  for _, tool in ipairs(tools or {}) do
    result[#result + 1] = table.clone(tool)
  end
  if #result > 0 then
    result[#result].cache_control = EPHEMERAL
  end
  return result
end

local function at_least(version, minimum)
  return version[1] > minimum[1] or (version[1] == minimum[1] and version[2] >= minimum[2])
end

-- Claude 5 models, and Opus from 4.7, take adaptive thinking and reject
-- `type = "enabled"`. Legacy ids put the version first, and gateway ids keep
-- a vendor prefix.
function M.requires_adaptive(model_id)
  local bare = model_id:match("([^/]+)$")
  local family, major, minor = bare:match("^claude%-(%a+)[%-%.](%d+)[%-%.]?(%d*)")
  if not family then
    return false
  end
  local version = { tonumber(major), tonumber(minor) or 0 }
  return at_least(version, family == OPUS and ADAPTIVE_SINCE_OPUS or ADAPTIVE_SINCE)
end

local function apply_thinking(body, request)
  local setting = request.thinking
  local max = thinking.max_budget(request.model)
  if M.requires_adaptive(request.model.id) then
    if setting.kind == "off" then
      return
    end
    -- These models omit thinking text unless asked for the summary.
    body.thinking = { type = "adaptive", display = "summarized" }
    local effort = thinking.effort(setting, thinking.dialects.anthropic_adaptive, max)
    if effort then
      body.output_config = { effort = effort }
    end
    return
  end
  local budget = thinking.budget(setting, max)
  if budget == "adaptive" then
    body.thinking = { type = "adaptive" }
  elseif budget ~= "off" then
    body.thinking = { type = "enabled", budget_tokens = budget }
  end
end

-- The body fields the Anthropic API and Bedrock share. {system_prefix} goes
-- before the system prompt as its own block.
function M.build_body(request, system_prefix)
  local system = {}
  if system_prefix then
    system[1] = { type = "text", text = system_prefix }
  end
  system[#system + 1] = { type = "text", text = request.system or "", cache_control = EPHEMERAL }
  local body = {
    max_tokens = request.model.max_output_tokens or FALLBACK_MAX_TOKENS,
    system = system,
    messages = M.convert_messages(request.messages),
    tools = M.convert_tools(request.tools),
  }
  apply_thinking(body, request)
  return body
end

-- A `-1m` model id asks for the long context window: the API takes the id
-- without the suffix plus `LONG_CONTEXT_BETA`. Returns the bare id and whether
-- the suffix was there.
function M.strip_long_context(model_id)
  local bare, count = model_id:gsub(LONG_CONTEXT_SUFFIX, "")
  return bare, count > 0
end

-- The Anthropic API body.
function M.build_request(request, system_prefix)
  local body = M.build_body(request, system_prefix)
  body.model = M.strip_long_context(request.model.id)
  body.stream = true
  if request.fast then
    body.speed = "fast"
  end
  return body
end

function M.beta_header(request)
  local betas = {}
  if request.fast then
    betas[#betas + 1] = FAST_BETA
  end
  if select(2, M.strip_long_context(request.model.id)) then
    betas[#betas + 1] = M.LONG_CONTEXT_BETA
  end
  return #betas > 0 and table.concat(betas, ",") or nil
end

local function parse_usage(u)
  local usage = codec.usage()
  usage.input_tokens = codec.count(u.input_tokens)
  usage.output_tokens = codec.count(u.output_tokens)
  usage.cache_creation_input_tokens = codec.count(u.cache_creation_input_tokens)
  usage.cache_read_input_tokens = codec.count(u.cache_read_input_tokens)
  return usage
end

local function new_block(start)
  if start.type == "text" then
    return { type = "text" }, {}
  elseif start.type == "thinking" then
    return { type = "thinking" }, {}
  elseif start.type == "redacted_thinking" then
    return { type = "redacted_thinking", data = start.data }
  elseif start.type == "tool_use" and type(start.id) == "string" and type(start.name) == "string" then
    return { type = "tool_use", id = start.id, name = start.name }, {}
  end
end

-- Decodes Messages events into `{ message, usage, stop_reason }`, emitting
-- deltas as they arrive. `next_event()` returns an event type and its decoded
-- payload, nil at the end, or nil plus an error. The SSE and the Bedrock
-- EventStream transports both feed it.
function M.decode(next_event, emit)
  local content, by_index, parts = {}, {}, {}
  local usage, stop_reason = codec.usage(), nil
  while true do
    local kind, value = next_event()
    if not kind then
      if value then
        return nil, value
      end
      break
    end
    value = type(value) == "table" and value or {}
    local block = by_index[value.index]
    local delta = type(value.delta) == "table" and value.delta or {}
    if kind == "message_start" and type(value.message) == "table" and type(value.message.usage) == "table" then
      usage = parse_usage(value.message.usage)
    elseif kind == "content_block_start" and value.index ~= nil and type(value.content_block) == "table" then
      local buffer
      block, buffer = new_block(value.content_block)
      if block then
        content[#content + 1] = block
        by_index[value.index] = block
        parts[block] = buffer
        if block.type == "tool_use" then
          emit({ type = "tool_use_start", id = block.id, name = block.name })
        end
      end
    elseif kind == "content_block_delta" and (delta.type == "text_delta" or delta.type == "thinking_delta") then
      local field = delta.type == "text_delta" and "text" or "thinking"
      local text = delta[field]
      if type(text) == "string" and text ~= "" then
        if block and block.type == field then
          table.insert(parts[block], text)
        end
        emit({ type = delta.type, text = text })
      end
    elseif kind == "content_block_delta" and delta.type == "signature_delta" and block and block.type == "thinking" then
      block.signature = delta.signature
    elseif
      kind == "content_block_delta"
      and delta.type == "input_json_delta"
      and block
      and block.type == "tool_use"
    then
      table.insert(parts[block], type(delta.partial_json) == "string" and delta.partial_json or "")
    elseif kind == "message_delta" then
      if type(value.usage) == "table" then
        usage.output_tokens = codec.count(value.usage.output_tokens)
      end
      if type(delta.stop_reason) == "string" then
        stop_reason = STOP_REASONS[delta.stop_reason] or "end_turn"
      end
    elseif kind == "error" then
      return nil, codec.frame_error(value.error)
    elseif kind == "message_stop" then
      break
    end
  end
  for _, finished in ipairs(content) do
    local buffer = parts[finished]
    if finished.type == "text" then
      finished.text = table.concat(buffer)
    elseif finished.type == "thinking" then
      finished.thinking = table.concat(buffer)
    elseif finished.type == "tool_use" then
      finished.input = codec.tool_input(table.concat(buffer))
    end
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }
end

-- Decodes an SSE {response}. An `error` event whose payload is not JSON
-- becomes an API error carrying the raw payload.
function M.stream(response, emit)
  local next_sse = sse.events(response)
  return M.decode(function()
    local event, err = next_sse()
    if not event then
      return nil, err
    end
    local value, decode_err = maki.json.decode(event.data)
    if event.event == "error" and (decode_err or type(value) ~= "table" or type(value.error) ~= "table") then
      return nil, codec.api_error(UNPARSED_ERROR_STATUS, event.data)
    end
    if event.event ~= "message" then
      return event.event, value
    end
    return type(value) == "table" and value.type or "", value
  end, emit)
end

return M
