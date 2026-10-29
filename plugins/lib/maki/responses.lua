local M = {}

function M.convert_input(messages)
  local input = assert(maki.json.decode("[]"))
  for _, message in ipairs(messages) do
    if message.role == "user" then
      for _, block in ipairs(message.content) do
        local part
        if block.type == "text" then
          part = { type = "input_text", text = block.text }
        elseif block.type == "image" then
          part =
            { type = "input_image", image_url = "data:" .. block.source.media_type .. ";base64," .. block.source.data }
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
          local arguments, err = maki.json.encode(block.input)
          if not arguments then
            return nil, err
          end
          calls[#calls + 1] = { type = "function_call", call_id = block.id, name = block.name, arguments = arguments }
        end
      end
      if #text > 0 then
        input[#input + 1] =
          { type = "message", role = "assistant", content = { { type = "output_text", text = table.concat(text) } } }
      end
      for _, call in ipairs(calls) do
        input[#input + 1] = call
      end
    end
  end
  return input
end

function M.convert_tools(tools)
  local converted, err = require("maki.chat_completions").convert_tools(tools)
  if not converted then
    return nil, err
  end
  local out = {}
  for _, tool in ipairs(converted) do
    local f = tool["function"]
    out[#out + 1] =
      { type = "function", name = f.name, description = f.description, parameters = f.parameters, strict = false }
  end
  return out
end

-- options.reasoning contains the provider's already resolved effort and summary.
function M.build_request(request, options)
  local input, err = M.convert_input(request.messages)
  if not input then
    return nil, err
  end
  local tools, tool_err = M.convert_tools(request.tools)
  if not tools then
    return nil, tool_err
  end
  local body =
    { model = request.model.id, instructions = request.system or "", input = input, stream = true, store = false }
  if #tools > 0 then
    body.tools = tools
  end
  if options and options.reasoning then
    body.reasoning = options.reasoning
  end
  return body
end

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

local function u32(value)
  if type(value) ~= "number" or value < 0 or value % 1 ~= 0 or value >= 18446744073709551616 then
    return 0
  end
  return value % 4294967296
end

function M.stream(body, emit, max_event_bytes)
  local next_event = require("maki.sse").new(body, max_event_bytes or 1024 * 1024)
  local text, thoughts, current, calls = {}, {}, {}, {}
  local usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }
  local stop, first = nil, true
  local function send(event)
    local ok, err = emit(event)
    if not ok then
      return nil, err
    end
    return true
  end
  local function arguments(value)
    if type(value) == "string" then
      return value
    end
    if type(value) == "table" then
      local encoded = maki.json.encode(value)
      if encoded and encoded:sub(1, 1) == "{" then
        return encoded
      end
    end
    return ""
  end
  local function find(index)
    if type(index) ~= "number" or index < 0 or index % 1 ~= 0 then
      return calls[#calls]
    end
    for _, call in ipairs(calls) do
      if call.index == index then
        return call
      end
    end
  end
  while true do
    local event, err = next_event()
    if not event then
      if err then
        if type(err) == "table" then
          return nil, err
        end
        return nil, { kind = err == "response is closed" and "closed" or "network", message = tostring(err) }
      end
      break
    end
    local data = maki.json.decode(event.data, { preserve_null = true })
    local valid = data ~= nil
    if type(data) ~= "table" then
      data = {}
    end
    local kind = event.event == "message" and data.type or event.event
    if kind == "error" or (kind == "response.failed" and valid) then
      local response = type(data.response) == "table" and data.response or {}
      local detail = kind == "error" and data.error or response.error
      detail = type(detail) == "table" and detail or {}
      local message = type(detail.message) == "string" and detail.message or nil
      if kind == "error" then
        if
          type(data.error) == "table"
          and (detail.type == nil or type(detail.type) == "string")
          and (detail.message == nil or type(detail.message) == "string")
          and (detail.metadata == nil or type(detail.metadata) == "table")
        then
          local code = detail.code
          local numeric = (type(code) == "number" or type(code) == "string") and tonumber(code) or nil
          if not numeric or numeric % 1 ~= 0 or numeric < 100 or numeric >= 600 then
            numeric = nil
          end
          local metadata = detail.metadata or {}
          local status = ERROR_STATUS[code]
            or numeric
            or ERROR_STATUS[detail.type]
            or ERROR_STATUS[metadata.error_type]
            or 400
          if not message or message:match("^%s*$") then
            message = "provider sent an error frame with no detail"
          end
          return nil, { kind = "api", status = status, message = message }
        end
        message = (type(data.message) == "string" and data.message) or "unknown error"
        return nil, { kind = "api", status = 500, message = message }
      end
      return nil,
        { kind = "api", status = ERROR_STATUS[detail.code] or 500, message = message or "response generation failed" }
    elseif kind == "response.output_text.delta" and type(data.delta) == "string" and data.delta ~= "" then
      local delta = data.delta
      if first then
        first = false
        delta = delta:gsub("^%s+", "")
      end
      if delta ~= "" then
        local ok, send_err = send({ type = "text_delta", text = delta })
        if not ok then
          return nil, send_err
        end
        text[#text + 1] = delta
      end
    elseif
      (kind == "response.reasoning_text.delta" or kind == "response.reasoning_summary_text.delta")
      and type(data.delta) == "string"
      and data.delta ~= ""
    then
      local ok, send_err = send({ type = "thinking_delta", text = data.delta })
      if not ok then
        return nil, send_err
      end
      current[#current + 1] = data.delta
    elseif kind == "response.reasoning_summary_part.added" and #current > 0 then
      thoughts[#thoughts + 1] = table.concat(current)
      current = {}
      local ok, send_err = send({ type = "thinking_block_end" })
      if not ok then
        return nil, send_err
      end
    elseif kind == "response.in_progress" and data.prompt_progress ~= nil then
      local p = type(data.prompt_progress) == "table" and data.prompt_progress or {}
      local ok, send_err =
        send({ type = "prompt_progress", processed = u32(p.processed), total = u32(p.total), cache = u32(p.cache) })
      if not ok then
        return nil, send_err
      end
    elseif
      (kind == "response.output_item.added" or kind == "response.output_item.done")
      and type(data.item) == "table"
      and data.item.type == "function_call"
    then
      local item = data.item
      local id = type(item.call_id) == "string" and item.call_id or ""
      local name = type(item.name) == "string" and item.name or ""
      local call = kind == "response.output_item.done" and find(data.output_index) or nil
      if not call then
        local index = data.output_index
        if kind == "response.output_item.done" or type(index) ~= "number" or index < 0 or index % 1 ~= 0 then
          index = #calls
        end
        call = { index = index, id = id, name = name, arguments = "" }
        calls[#calls + 1] = call
        if kind == "response.output_item.done" then
          call.arguments = arguments(item.arguments)
        end
        if name ~= "" then
          local ok, send_err = send({ type = "tool_use_start", id = id, name = name })
          if not ok then
            return nil, send_err
          end
        end
      else
        local start = call.name == "" and name ~= ""
        if call.id == "" then
          call.id = id
        end
        if call.name == "" then
          call.name = name
        end
        local args = arguments(item.arguments)
        if args ~= "" then
          call.arguments = args
        end
        if start then
          local ok, send_err = send({ type = "tool_use_start", id = call.id, name = call.name })
          if not ok then
            return nil, send_err
          end
        end
      end
    elseif kind == "response.function_call_arguments.delta" then
      local call = find(data.output_index)
      if call then
        call.arguments = call.arguments .. arguments(data.delta)
      end
    elseif valid and (kind == "response.completed" or kind == "response.incomplete") then
      local response = type(data.response) == "table" and data.response or {}
      if type(response.usage) == "table" then
        local u = response.usage
        local details = type(u.input_tokens_details) == "table" and u.input_tokens_details or {}
        local cached = u32(details.cached_tokens)
        usage = {
          input = math.max(0, u32(u.input_tokens) - cached),
          output = u32(u.output_tokens),
          cache_read = cached,
          cache_creation = 0,
        }
      end
      if kind == "response.incomplete" or response.status == "incomplete" then
        stop = "max_tokens"
      elseif (type(response.status) ~= "string" or response.status == "completed") and #calls > 0 then
        stop = "tool_use"
      else
        stop = "end_turn"
      end
      break
    end
  end
  local content = {}
  if #current > 0 then
    thoughts[#thoughts + 1] = table.concat(current)
  end
  for _, thought in ipairs(thoughts) do
    content[#content + 1] = { type = "thinking", thinking = thought }
  end
  if #text > 0 then
    content[#content + 1] = { type = "text", text = table.concat(text) }
  end
  for _, call in ipairs(calls) do
    local input = maki.json.decode(call.arguments, { preserve_null = true })
    if input == nil then
      input = {}
    end
    content[#content + 1] = { type = "tool_use", id = call.id, name = call.name, input = input }
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop }
end

return M
