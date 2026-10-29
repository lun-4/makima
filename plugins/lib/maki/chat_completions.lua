local M = {}

function M.convert_messages(messages, system)
  local out = { { role = "system", content = system or "" } }
  for _, message in ipairs(messages) do
    if message.role == "user" then
      local text, images = {}, {}
      for _, block in ipairs(message.content) do
        if block.type == "text" then
          text[#text + 1] = block.text
        elseif block.type == "image" then
          images[#images + 1] = {
            type = "image_url",
            image_url = { url = "data:" .. block.source.media_type .. ";base64," .. block.source.data },
          }
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
          local arguments, err = maki.json.encode(block.input)
          if not arguments then
            return nil, err
          end
          calls[#calls + 1] = {
            id = block.id,
            type = "function",
            ["function"] = { name = block.name, arguments = arguments },
          }
        end
      end
      local content, thought = table.concat(text), table.concat(reasoning)
      if content ~= "" or thought ~= "" or #calls > 0 then
        local converted = { role = "assistant", content = content }
        if thought ~= "" then
          converted.reasoning_content = thought
        end
        if #calls > 0 then
          converted.tool_calls = calls
        end
        out[#out + 1] = converted
      end
    end
  end
  return out, nil
end

function M.convert_tools(tools)
  local out = {}
  if type(tools) ~= "table" then
    return out, nil
  end
  for _, tool in ipairs(tools) do
    if type(tool) == "table" and tool.name ~= nil and tool.description ~= nil then
      local schema = tool.input_schema
      local encoded, err = maki.json.encode(schema)
      if not encoded then
        return nil, err
      end
      if encoded:sub(1, 1) ~= "{" then
        schema = { type = "object", properties = {} }
      end
      out[#out + 1] = {
        type = "function",
        ["function"] = { name = tool.name, description = tool.description, parameters = schema },
      }
    end
  end
  return out, nil
end

function M.build_request(request, options)
  local messages, err = M.convert_messages(request.messages, request.system)
  if not messages then
    return nil, err
  end
  local tools, tools_err = M.convert_tools(request.tools)
  if not tools then
    return nil, tools_err
  end
  local body = { model = request.model.id, messages = messages, stream = true }
  if request.model.max_output_tokens ~= nil then
    body[options.max_tokens_field] = request.model.max_output_tokens
  end
  if options.include_stream_usage then
    body.stream_options = { include_usage = true }
  end
  if #tools > 0 then
    body.tools = tools
  end
  return body, nil
end

local MAX_TOOL_CALLS = 512
local ERROR_STATUS = {
  overloaded_error = 529,
  server_is_overloaded = 529,
  service_unavailable_error = 503,
  provider_overloaded = 503,
  provider_unavailable = 502,
  api_error = 500,
  server_error = 500,
  rate_limit_error = 429,
  rate_limit_exceeded = 429,
  tokens = 429,
  request_too_large = 413,
  not_found_error = 404,
  permission_error = 403,
  billing_error = 402,
  insufficient_quota = 402,
  authentication_error = 401,
  invalid_api_key = 401,
}
local next_id = 0
local id_tag = tostring({}):gsub("[^%w]", "")

function M.stream(body, emit, max_event_bytes)
  local next_event = require("maki.sse").new(body, max_event_bytes or 1024 * 1024)
  local text, reasoning, calls = {}, {}, {}
  local usage = { input = 0, output = 0, cache_read = 0, cache_creation = 0 }
  local stop_reason, first_content = nil, true

  local function send(kind, value)
    if type(value) ~= "string" or value == "" then
      return true
    end
    if kind == "text_delta" and first_content then
      first_content = false
      value = value:gsub("^%s+", "")
      if value == "" then
        return true
      end
    end
    local ok, err = emit({ type = kind, text = value })
    if not ok then
      return nil, err
    end
    local target = kind == "text_delta" and text or reasoning
    target[#target + 1] = value
    return true
  end

  while true do
    local event, read_err = next_event()
    if not event then
      if read_err then
        if type(read_err) == "table" then
          return nil, read_err
        end
        local kind = read_err == "response is closed" and "closed" or "network"
        return nil, { kind = kind, message = tostring(read_err) }
      end
      break
    end
    if event.data == "[DONE]" then
      break
    end
    local chunk = maki.json.decode(event.data)
    if type(chunk) ~= "table" then
      chunk = {}
    end
    if type(chunk.error) == "table" then
      local detail = chunk.error
      local code = detail.code
      local numeric = tonumber(code)
      if not numeric or numeric % 1 ~= 0 or numeric < 100 or numeric >= 600 then
        numeric = nil
      end
      local metadata = type(detail.metadata) == "table" and detail.metadata or {}
      local status = ERROR_STATUS[code]
        or numeric
        or ERROR_STATUS[detail.type]
        or ERROR_STATUS[metadata.error_type]
        or 400
      local message = detail.message
      if type(message) ~= "string" or message:match("^%s*$") then
        message = "provider sent an error frame with no detail"
      end
      return nil, { kind = "api", status = status, message = message }
    end
    if type(chunk.usage) == "table" then
      local u = chunk.usage
      local details = type(u.prompt_tokens_details) == "table" and u.prompt_tokens_details or {}
      local cached = math.max(tonumber(details.cached_tokens) or 0, tonumber(u.prompt_cache_hit_tokens) or 0)
      usage = {
        input = math.max(0, (tonumber(u.prompt_tokens) or 0) - cached),
        output = tonumber(u.completion_tokens) or 0,
        cache_read = cached,
        cache_creation = 0,
      }
    end
    local choice = type(chunk.choices) == "table" and chunk.choices[1]
    if type(choice) == "table" then
      if choice.finish_reason then
        stop_reason = ({ stop = "end_turn", tool_calls = "tool_use", length = "max_tokens" })[choice.finish_reason]
          or "end_turn"
      end
      local delta = choice.delta or choice.message
      if type(delta) == "table" then
        local thought = delta.reasoning_content
        if type(thought) ~= "string" or thought == "" then
          thought = delta.reasoning
        end
        local ok, err = send("thinking_delta", thought)
        if not ok then
          return nil, err
        end
        if type(delta.content) == "string" then
          ok, err = send("text_delta", delta.content)
          if not ok then
            return nil, err
          end
        elseif type(delta.content) == "table" then
          for _, part in ipairs(delta.content) do
            if part.type == "text" then
              ok, err = send("text_delta", part.text)
              if not ok then
                return nil, err
              end
            elseif part.type == "thinking" and type(part.thinking) == "table" then
              for _, item in ipairs(part.thinking) do
                ok, err = send("thinking_delta", type(item) == "table" and item.text or item)
                if not ok then
                  return nil, err
                end
              end
            end
          end
        end
        if type(delta.tool_calls) == "table" then
          for position, call in ipairs(delta.tool_calls) do
            local index = call.index
            if index == nil and delta == choice.message then
              index = position - 1
            end
            if type(index) == "number" and index >= 0 and index < MAX_TOOL_CALLS and index % 1 == 0 then
              local acc = calls[index]
              if not acc then
                next_id = next_id + 1
                acc = { id = "maki_unnamed_" .. id_tag .. "_" .. next_id, name = "", arguments = {} }
                calls[index] = acc
              end
              if type(call.id) == "string" and call.id ~= "" then
                acc.id = call.id
              end
              local func = call["function"]
              if type(func) == "table" then
                if type(func.name) == "string" and func.name ~= "" then
                  acc.name = acc.name .. func.name
                end
                if type(func.arguments) == "string" then
                  acc.arguments[#acc.arguments + 1] = func.arguments
                end
              end
              if not acc.started and acc.name ~= "" then
                ok, err = emit({ type = "tool_use_start", id = acc.id, name = acc.name })
                if not ok then
                  return nil, err
                end
                acc.started = true
              end
            end
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
  for index = 0, MAX_TOOL_CALLS - 1 do
    local call = calls[index]
    if call then
      local input = maki.json.decode(table.concat(call.arguments), { preserve_null = true })
      if input == nil then
        return nil, { kind = "api", message = "Provider returned invalid JSON tool arguments" }
      end
      content[#content + 1] =
        { type = "tool_use", id = call.id, name = call.name ~= "" and call.name or "maki_unknown_tool", input = input }
    end
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }, nil
end

return M
