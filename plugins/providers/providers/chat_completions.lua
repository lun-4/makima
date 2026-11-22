-- OpenAI Chat Completions: request encoding and stream decoding.

local codec = require("providers.codec")
local sse = require("providers.sse")

local M = {}

local DONE = "[DONE]"
local TOOL_ID_PREFIX = "call_"
local UNKNOWN_TOOL = "makima_unknown_tool"
local STOP_REASONS = { stop = "end_turn", tool_calls = "tool_use", length = "max_tokens" }

local function data_url(source)
  return "data:" .. source.media_type .. ";base64," .. source.data
end

function M.convert_messages(messages, system)
  local out = { { role = "system", content = system or "" } }
  for _, message in ipairs(messages) do
    if message.role == "user" then
      local text, images = {}, {}
      for _, block in ipairs(message.content) do
        if block.type == "text" then
          text[#text + 1] = block.text
        elseif block.type == "image" then
          images[#images + 1] = { type = "image_url", image_url = { url = data_url(block.source) } }
        elseif block.type == "tool_result" then
          out[#out + 1] = { role = "tool", tool_call_id = block.tool_use_id, content = block.content }
        end
      end
      if #images > 0 then
        if #text > 0 then
          images[#images + 1] = { type = "text", text = table.concat(text, "\n") }
        end
        out[#out + 1] = { role = "user", content = images }
      elseif #text > 0 then
        out[#out + 1] = { role = "user", content = table.concat(text, "\n") }
      end
    elseif message.role == "assistant" then
      local text, reasoning, calls = {}, {}, {}
      for _, block in ipairs(message.content) do
        if block.type == "text" then
          text[#text + 1] = block.text
        elseif block.type == "thinking" then
          reasoning[#reasoning + 1] = block.thinking
        elseif block.type == "tool_use" then
          calls[#calls + 1] = {
            id = block.id,
            type = "function",
            ["function"] = { name = block.name, arguments = assert(makima.json.encode(block.input)) },
          }
        end
      end
      local content, thought = table.concat(text), table.concat(reasoning)
      if content ~= "" or thought ~= "" or #calls > 0 then
        out[#out + 1] = {
          role = "assistant",
          content = content,
          reasoning_content = thought ~= "" and thought or nil,
          tool_calls = #calls > 0 and calls or nil,
        }
      end
    end
  end
  return out
end

-- Schemas that are not JSON objects become an empty object schema.
function M.convert_tools(tools)
  local out = {}
  for _, tool in ipairs(tools or {}) do
    if type(tool.name) == "string" then
      local schema = tool.input_schema
      if type(schema) ~= "table" or codec.is_array(schema) then
        schema = { type = "object", properties = {} }
      end
      out[#out + 1] = {
        type = "function",
        ["function"] = {
          name = tool.name,
          description = type(tool.description) == "string" and tool.description or "",
          parameters = schema,
        },
      }
    end
  end
  return out
end

-- {options}: `max_tokens_field` names the output limit field, if the API
-- takes one; `include_stream_usage` asks for usage in the final chunk.
function M.build_request(request, options)
  local body = {
    model = request.model.id,
    messages = M.convert_messages(request.messages, request.system),
    stream = true,
  }
  if options.max_tokens_field and request.model.max_output_tokens then
    body[options.max_tokens_field] = request.model.max_output_tokens
  end
  if options.include_stream_usage then
    body.stream_options = { include_usage = true }
  end
  local tools = M.convert_tools(request.tools)
  if #tools > 0 then
    body.tools = tools
  end
  return body
end

local function parse_usage(u)
  local details = type(u.prompt_tokens_details) == "table" and u.prompt_tokens_details or {}
  local cached = math.max(codec.count(details.cached_tokens), codec.count(u.prompt_cache_hit_tokens))
  local usage = codec.usage()
  usage.input_tokens = math.max(0, codec.count(u.prompt_tokens) - cached)
  usage.output_tokens = codec.count(u.completion_tokens)
  usage.cache_read_input_tokens = cached
  return usage
end

-- Decodes the stream into `{ message, usage, stop_reason }`, emitting deltas
-- as they arrive. Returns nil plus an error for error frames and read failures.
function M.stream(response, emit)
  local next_event = sse.events(response)
  local text, reasoning, calls, call_order = {}, {}, {}, {}
  local usage, stop_reason = codec.usage(), nil
  local leading = true

  local function add_text(value)
    if type(value) ~= "string" or value == "" then
      return
    end
    if leading then
      leading = false
      value = value:gsub("^%s+", "")
    end
    if value ~= "" then
      emit({ type = "text_delta", text = value })
      text[#text + 1] = value
    end
  end

  local function add_reasoning(value)
    if type(value) == "string" and value ~= "" then
      emit({ type = "thinking_delta", text = value })
      reasoning[#reasoning + 1] = value
    end
  end

  local function add_tool_call(call, index)
    local acc = calls[index]
    if not acc then
      acc = { id = codec.tool_id(TOOL_ID_PREFIX), name = "", arguments = {} }
      calls[index] = acc
      call_order[#call_order + 1] = index
    end
    if type(call.id) == "string" and call.id ~= "" then
      acc.id = call.id
    end
    local func = type(call["function"]) == "table" and call["function"] or {}
    if type(func.name) == "string" and func.name ~= "" then
      acc.name = func.name
    end
    if type(func.arguments) == "string" then
      acc.arguments[#acc.arguments + 1] = func.arguments
    end
    if not acc.started and acc.name ~= "" then
      acc.started = true
      emit({ type = "tool_use_start", id = acc.id, name = acc.name })
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
    if event.data:match("^%s*(.-)%s*$") == DONE then
      break
    end
    local chunk = makima.json.decode(event.data)
    chunk = type(chunk) == "table" and chunk or {}
    if type(chunk.error) == "table" then
      return nil, codec.frame_error(chunk.error)
    end
    if type(chunk.usage) == "table" then
      usage = parse_usage(chunk.usage)
    end
    local choice = type(chunk.choices) == "table" and chunk.choices[1]
    if type(choice) == "table" then
      if choice.finish_reason then
        stop_reason = STOP_REASONS[choice.finish_reason] or "end_turn"
      end
      local delta = choice.delta or choice.message
      if type(delta) == "table" then
        local thought = delta.reasoning_content
        add_reasoning((type(thought) == "string" and thought ~= "") and thought or delta.reasoning)
        if type(delta.content) == "table" then
          for _, part in ipairs(delta.content) do
            if type(part) == "table" and part.type == "text" then
              add_text(part.text)
            elseif type(part) == "table" and part.type == "thinking" and type(part.thinking) == "table" then
              for _, item in ipairs(part.thinking) do
                add_reasoning(type(item) == "table" and item.text or item)
              end
            end
          end
        else
          add_text(delta.content)
        end
        for position, call in ipairs(type(delta.tool_calls) == "table" and delta.tool_calls or {}) do
          local index = call.index
          if index == nil and delta == choice.message then
            index = position - 1
          end
          if type(index) == "number" then
            add_tool_call(call, index)
          end
        end
      end
    end
  end

  local content = {}
  if #reasoning > 0 then
    content[#content + 1] = { type = "thinking", thinking = table.concat(reasoning) }
  end
  if #text > 0 then
    content[#content + 1] = { type = "text", text = table.concat(text) }
  end
  table.sort(call_order)
  for _, index in ipairs(call_order) do
    local call = calls[index]
    content[#content + 1] = {
      type = "tool_use",
      id = call.id,
      name = call.name ~= "" and call.name or UNKNOWN_TOOL,
      input = codec.tool_input(table.concat(call.arguments)),
    }
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }
end

return M
