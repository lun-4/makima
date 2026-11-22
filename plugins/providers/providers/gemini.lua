-- Gemini generateContent: request encoding and stream decoding, shared by
-- the Gemini API and Vertex.

local codec = require("providers.codec")
local sse = require("providers.sse")
local thinking = require("providers.thinking")

local M = {}

local FLASH_MAX_THINKING = 24576
local PRO_MAX_THINKING = 32768
local UNKNOWN_TOOL = "unknown"
local TOOL_ID_PREFIX = "call_"

-- Every function name a tool result needs, keyed by tool use id.
local function tool_names(messages)
  local names = {}
  for _, message in ipairs(messages) do
    for _, block in ipairs(message.content) do
      if block.type == "tool_use" then
        names[block.id] = block.name
      end
    end
  end
  return names
end

-- A tool result as a `functionResponse.response` object: JSON objects as
-- they are, anything else under `result`, errors wrapped in `error`.
local function tool_response(block)
  local value = makima.json.decode(block.content)
  if value == nil then
    value = block.content
  end
  if type(value) ~= "table" or codec.is_array(value) then
    value = { result = value }
  end
  if block.is_error then
    value = { error = value }
  end
  return value
end

-- Images next to tool results go in a following user turn, since Gemini
-- wants a function response turn to hold only function responses.
function M.convert_messages(messages)
  local names = tool_names(messages)
  local result = {}
  for _, message in ipairs(messages) do
    local parts, images = {}, {}
    local has_results = false
    for _, block in ipairs(message.content) do
      has_results = has_results or block.type == "tool_result"
    end
    for _, block in ipairs(message.content) do
      if block.type == "text" then
        parts[#parts + 1] = { text = block.text }
      elseif block.type == "thinking" then
        parts[#parts + 1] = { text = block.thinking, thought = true, thoughtSignature = block.signature }
      elseif block.type == "tool_use" then
        parts[#parts + 1] = {
          functionCall = { name = block.name, args = block.input },
          thoughtSignature = block.thought_signature,
        }
      elseif block.type == "tool_result" then
        parts[#parts + 1] = {
          functionResponse = { name = names[block.tool_use_id] or UNKNOWN_TOOL, response = tool_response(block) },
        }
      elseif block.type == "image" then
        local part = { inlineData = { mimeType = block.source.media_type, data = block.source.data } }
        table.insert(has_results and images or parts, part)
      end
    end
    if #parts > 0 then
      result[#result + 1] = { role = message.role == "assistant" and "model" or "user", parts = parts }
    end
    if #images > 0 then
      result[#result + 1] = { role = "user", parts = images }
    end
  end
  return result
end

-- A deep copy of a JSON schema without `additionalProperties`, which Gemini
-- rejects.
local function strip_additional_properties(value)
  if type(value) ~= "table" then
    return value
  end
  local copy = codec.is_array(value) and codec.array() or {}
  for key, item in pairs(value) do
    if key ~= "additionalProperties" then
      copy[key] = strip_additional_properties(item)
    end
  end
  return copy
end

-- Gemini rejects an object schema without properties, so such tools are
-- declared without parameters.
function M.convert_tools(tools)
  local result = {}
  for _, tool in ipairs(tools or {}) do
    if type(tool.name) == "string" then
      local declaration =
        { name = tool.name, description = type(tool.description) == "string" and tool.description or "" }
      local schema = tool.input_schema
      if
        type(schema) == "table"
        and type(schema.properties) == "table"
        and not codec.is_array(schema.properties)
        and next(schema.properties) ~= nil
      then
        declaration.parameters = strip_additional_properties(schema)
      end
      result[#result + 1] = declaration
    end
  end
  return result
end

-- The thinking budget cap: the model family's documented limit, lowered to
-- what the model's output window allows.
function M.max_thinking(model)
  local cap = model.id:find("flash", 1, true) and FLASH_MAX_THINKING or PRO_MAX_THINKING
  local max = thinking.max_budget(model)
  return max and math.min(max, cap) or cap
end

function M.build_request(request)
  local body = { contents = M.convert_messages(request.messages) }
  if request.system and request.system ~= "" then
    body.systemInstruction = { parts = { { text = request.system } } }
  end
  local config = {}
  local budget = thinking.budget(request.thinking, M.max_thinking(request.model))
  if budget == "adaptive" then
    config.thinkingConfig = { includeThoughts = true }
  elseif budget ~= "off" then
    config.thinkingConfig = { thinkingBudget = budget, includeThoughts = true }
  end
  config.maxOutputTokens = request.model.max_output_tokens
  if next(config) then
    body.generationConfig = config
  end
  local tools = M.convert_tools(request.tools)
  if #tools > 0 then
    body.tools = { { functionDeclarations = tools } }
  end
  return body
end

local function parse_usage(meta)
  local cached = codec.count(meta.cachedContentTokenCount)
  local usage = codec.usage()
  usage.input_tokens = math.max(0, codec.count(meta.promptTokenCount) - cached)
  usage.output_tokens = codec.count(meta.candidatesTokenCount) + codec.count(meta.thoughtsTokenCount)
  usage.cache_read_input_tokens = cached
  return usage
end

-- Decodes the stream into `{ message, usage, stop_reason }`, emitting deltas
-- as they arrive. Returns nil plus an error for read failures.
function M.stream(response, emit)
  local next_event = sse.events(response)
  local content, parts = {}, {}
  local usage, stop_reason = codec.usage(), nil
  local thinking_open = false

  local function close_thinking()
    if thinking_open then
      thinking_open = false
      emit({ type = "thinking_block_end" })
    end
  end

  local function add_call(part, call)
    close_thinking()
    local id = codec.tool_id(TOOL_ID_PREFIX .. call.name .. "_")
    emit({ type = "tool_use_start", id = id, name = call.name })
    content[#content + 1] = {
      type = "tool_use",
      id = id,
      name = call.name,
      input = call.args or {},
      thought_signature = call.thoughtSignature or part.thoughtSignature,
    }
    stop_reason = "tool_use"
  end

  local function add_text(part)
    local thought = part.thought == true
    if part.text ~= "" then
      if thought then
        thinking_open = true
      else
        close_thinking()
      end
      emit({ type = thought and "thinking_delta" or "text_delta", text = part.text })
    elseif not thought then
      return
    end
    local kind = thought and "thinking" or "text"
    local last = content[#content]
    if not last or last.type ~= kind then
      last = { type = kind }
      content[#content + 1] = last
      parts[last] = {}
    end
    table.insert(parts[last], part.text)
    if thought and part.thoughtSignature ~= nil then
      last.signature = part.thoughtSignature
    end
  end

  while true do
    local event, err = next_event()
    if not event then
      if err then
        return nil, err
      end
      break
    end
    local chunk = makima.json.decode(event.data)
    chunk = type(chunk) == "table" and chunk or {}
    if type(chunk.usageMetadata) == "table" then
      usage = parse_usage(chunk.usageMetadata)
    end
    for _, candidate in ipairs(type(chunk.candidates) == "table" and chunk.candidates or {}) do
      if type(candidate) == "table" then
        if candidate.finishReason then
          stop_reason = candidate.finishReason == "MAX_TOKENS" and "max_tokens" or "end_turn"
        end
        local candidate_content = type(candidate.content) == "table" and candidate.content or {}
        for _, part in ipairs(type(candidate_content.parts) == "table" and candidate_content.parts or {}) do
          if
            type(part) == "table"
            and type(part.functionCall) == "table"
            and type(part.functionCall.name) == "string"
          then
            add_call(part, part.functionCall)
          elseif type(part) == "table" and type(part.text) == "string" then
            add_text(part)
          end
        end
      end
    end
  end
  close_thinking()
  for _, block in ipairs(content) do
    if parts[block] then
      block[block.type] = table.concat(parts[block])
    end
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }
end

return M
