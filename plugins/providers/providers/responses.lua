-- OpenAI Responses: request encoding and stream decoding.

local chat_completions = require("providers.chat_completions")
local codec = require("providers.codec")
local sse = require("providers.sse")
local thinking = require("providers.thinking")

local M = {}

local SUMMARY = "auto"
local UNKNOWN_ERROR_STATUS = 500
local FAILED_MESSAGE = "response generation failed"

local function data_url(source)
  return "data:" .. source.media_type .. ";base64," .. source.data
end

function M.convert_input(messages)
  local input = codec.array()
  for _, message in ipairs(messages) do
    if message.role == "user" then
      for _, block in ipairs(message.content) do
        local part
        if block.type == "text" then
          part = { type = "input_text", text = block.text }
        elseif block.type == "image" then
          part = { type = "input_image", image_url = data_url(block.source) }
        elseif block.type == "tool_result" then
          input[#input + 1] = { type = "function_call_output", call_id = block.tool_use_id, output = block.content }
        end
        if part then
          input[#input + 1] = { type = "message", role = "user", content = { part } }
        end
      end
    elseif message.role == "assistant" then
      local text, calls = {}, {}
      for _, block in ipairs(message.content) do
        if block.type == "text" then
          text[#text + 1] = block.text
        elseif block.type == "tool_use" then
          calls[#calls + 1] = {
            type = "function_call",
            call_id = block.id,
            name = block.name,
            arguments = assert(makima.json.encode(block.input)),
          }
        end
      end
      if #text > 0 then
        input[#input + 1] = {
          type = "message",
          role = "assistant",
          content = { { type = "output_text", text = table.concat(text) } },
        }
      end
      for _, call in ipairs(calls) do
        input[#input + 1] = call
      end
    end
  end
  return input
end

function M.convert_tools(tools)
  local out = {}
  for _, tool in ipairs(chat_completions.convert_tools(tools)) do
    local func = tool["function"]
    out[#out + 1] = {
      type = "function",
      name = func.name,
      description = func.description,
      parameters = func.parameters,
      strict = false,
    }
  end
  return out
end

-- The `reasoning` object for {setting} in {dialect}: the effort, plus a
-- summary request whenever thinking is on. Nil when there is nothing to say.
function M.reasoning(setting, dialect, max)
  local effort = thinking.effort(setting, dialect, max)
  local summary = setting.kind ~= "off" and SUMMARY or nil
  if effort or summary then
    return { effort = effort, summary = summary }
  end
end

-- {reasoning} is the provider's encoded `reasoning` object, if any.
function M.build_request(request, reasoning)
  local body = {
    model = request.model.id,
    instructions = request.system or "",
    input = M.convert_input(request.messages),
    stream = true,
    store = false,
    reasoning = reasoning,
  }
  local tools = M.convert_tools(request.tools)
  if #tools > 0 then
    body.tools = tools
  end
  return body
end

-- Function call arguments as a JSON text: a string as is, an object encoded.
local function arguments(value)
  if type(value) == "string" then
    return value
  elseif type(value) == "table" and not codec.is_array(value) then
    return makima.json.encode(value) or ""
  end
  return ""
end

local function parse_usage(u)
  local details = type(u.input_tokens_details) == "table" and u.input_tokens_details or {}
  local cached = codec.count(details.cached_tokens)
  local usage = codec.usage()
  usage.input_tokens = math.max(0, codec.count(u.input_tokens) - cached)
  usage.output_tokens = codec.count(u.output_tokens)
  usage.cache_read_input_tokens = cached
  return usage
end

-- Decodes the stream into `{ message, usage, stop_reason }`, emitting deltas
-- as they arrive. Returns nil plus an error for error events and read failures.
function M.stream(response, emit)
  local next_event = sse.events(response)
  local text, thoughts, current, calls = {}, {}, {}, {}
  local usage, stop_reason = codec.usage(), nil
  local leading = true

  local function find(index)
    if type(index) ~= "number" then
      return calls[#calls]
    end
    for _, call in ipairs(calls) do
      if call.index == index then
        return call
      end
    end
  end

  local function start(call)
    if call.name ~= "" and not call.started then
      call.started = true
      emit({ type = "tool_use_start", id = call.id, name = call.name })
    end
  end

  local function output_item(kind, data)
    local item = data.item
    local id = type(item.call_id) == "string" and item.call_id or ""
    local name = type(item.name) == "string" and item.name or ""
    local done = kind == "response.output_item.done"
    local call = done and find(data.output_index)
    if not call then
      local index = data.output_index
      if done or type(index) ~= "number" then
        index = #calls
      end
      call = { index = index, id = id, name = name, arguments = {} }
      calls[#calls + 1] = call
    else
      call.id = call.id ~= "" and call.id or id
      call.name = call.name ~= "" and call.name or name
    end
    local complete = done and arguments(item.arguments) or ""
    if complete ~= "" then
      call.arguments = { complete }
    end
    start(call)
  end

  while true do
    local event, err = next_event()
    if not event then
      if err then
        return nil, err
      end
      break
    end
    local data = makima.json.decode(event.data)
    local valid = data ~= nil
    data = type(data) == "table" and data or {}
    local kind = event.event == "message" and data.type or event.event
    local delta = type(data.delta) == "string" and data.delta or ""
    if kind == "error" then
      if type(data.error) == "table" then
        return nil, codec.frame_error(data.error)
      end
      local message = type(data.message) == "string" and data.message or "unknown error"
      return nil, codec.api_error(UNKNOWN_ERROR_STATUS, message)
    elseif kind == "response.failed" and valid then
      local detail = type(data.response) == "table" and type(data.response.error) == "table" and data.response.error
        or {}
      return nil,
        codec.api_error(
          codec.error_status(detail.code) or UNKNOWN_ERROR_STATUS,
          type(detail.message) == "string" and detail.message or FAILED_MESSAGE
        )
    elseif kind == "response.output_text.delta" and delta ~= "" then
      if leading then
        leading = false
        delta = delta:gsub("^%s+", "")
      end
      if delta ~= "" then
        emit({ type = "text_delta", text = delta })
        text[#text + 1] = delta
      end
    elseif
      (kind == "response.reasoning_text.delta" or kind == "response.reasoning_summary_text.delta") and delta ~= ""
    then
      emit({ type = "thinking_delta", text = delta })
      current[#current + 1] = delta
    elseif kind == "response.reasoning_summary_part.added" and #current > 0 then
      thoughts[#thoughts + 1] = table.concat(current)
      current = {}
      emit({ type = "thinking_block_end" })
    elseif kind == "response.in_progress" and type(data.prompt_progress) == "table" then
      local progress = data.prompt_progress
      emit({
        type = "prompt_progress",
        processed = codec.count(progress.processed),
        total = codec.count(progress.total),
        cache = codec.count(progress.cache),
      })
    elseif
      (kind == "response.output_item.added" or kind == "response.output_item.done")
      and type(data.item) == "table"
      and data.item.type == "function_call"
    then
      output_item(kind, data)
    elseif kind == "response.function_call_arguments.delta" then
      local call = find(data.output_index)
      if call then
        call.arguments[#call.arguments + 1] = arguments(data.delta)
      end
    elseif valid and (kind == "response.completed" or kind == "response.incomplete") then
      local result = type(data.response) == "table" and data.response or {}
      if type(result.usage) == "table" then
        usage = parse_usage(result.usage)
      end
      if kind == "response.incomplete" or result.status == "incomplete" then
        stop_reason = "max_tokens"
      elseif (type(result.status) ~= "string" or result.status == "completed") and #calls > 0 then
        stop_reason = "tool_use"
      else
        stop_reason = "end_turn"
      end
      break
    end
  end

  if #current > 0 then
    thoughts[#thoughts + 1] = table.concat(current)
  end
  local content = {}
  for _, thought in ipairs(thoughts) do
    content[#content + 1] = { type = "thinking", thinking = thought }
  end
  if #text > 0 then
    content[#content + 1] = { type = "text", text = table.concat(text) }
  end
  for _, call in ipairs(calls) do
    content[#content + 1] =
      { type = "tool_use", id = call.id, name = call.name, input = codec.tool_input(table.concat(call.arguments)) }
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }
end

return M
