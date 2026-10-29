local M = {}

local FALLBACK_MAX_TOKENS = 32000
local FALLBACK_THINKING = 32768
local MIN_THINKING = 1024
local percentages = { minimal = 10, low = 20, medium = 40, high = 60, xhigh = 80, max = 100 }

local function array()
  return assert(maki.json.decode("[]"))
end

local function blank(text)
  for _, codepoint in utf8.codes(text) do
    if
      not (
        (codepoint >= 0x09 and codepoint <= 0x0d)
        or codepoint == 0x20
        or codepoint == 0x85
        or codepoint == 0xa0
        or codepoint == 0x1680
        or (codepoint >= 0x2000 and codepoint <= 0x200a)
        or codepoint == 0x2028
        or codepoint == 0x2029
        or codepoint == 0x202f
        or codepoint == 0x205f
        or codepoint == 0x3000
      )
    then
      return false
    end
  end
  return true
end

local function copy(value)
  local result = {}
  for key, item in pairs(value) do
    result[key] = item
  end
  return result
end

function M.convert_messages(messages)
  local result = array()
  for index, message in ipairs(messages) do
    local content = array()
    for _, block in ipairs(message.content) do
      if block.type ~= "text" or not blank(block.text) then
        local wire = copy(block)
        if wire.type == "image" then
          wire.source = copy(block.source)
          wire.source.type = "base64"
        elseif wire.type == "tool_result" and not wire.is_error then
          wire.is_error = nil
        end
        content[#content + 1] = wire
      end
    end
    if #content == 0 then
      content[1] = { type = "text", text = "(empty)" }
    end
    if index + 2 > #messages then
      for i = #content, 1, -1 do
        if content[i].type ~= "thinking" and content[i].type ~= "redacted_thinking" then
          content[i].cache_control = { type = "ephemeral" }
          break
        end
      end
    end
    result[#result + 1] = { role = message.role, content = content }
  end
  return result
end

function M.convert_tools(tools)
  if tools == nil then
    return maki.json.null()
  end
  local encoded, err = maki.json.encode(tools)
  if not encoded then
    return nil, err
  end
  if encoded:sub(1, 1) ~= "[" then
    return tools
  end
  local result = array()
  for _, tool in ipairs(tools) do
    result[#result + 1] = type(tool) == "table" and copy(tool) or tool
  end
  if #result > 0 then
    result[#result].cache_control = { type = "ephemeral" }
  end
  return result
end

local function apply_thinking(body, model, thinking)
  if not thinking or thinking.mode == "off" then
    return
  end
  local bare = model.id:match("([^/]+)$") or model.id
  local family, version = bare:match("^claude%-([^%.%-]+)[%.%-](.+)$")
  local major, minor
  if version then
    local first, rest = version:match("^([^%.%-]+)[%.%-]?(.*)$")
    major = first and first:match("^%d+$") and tonumber(first)
    local second = rest:match("^([^%.%-]+)")
    minor = second and second:match("^%d+$") and tonumber(second) or 0
  end
  local adaptive = family
    and not tonumber(family)
    and major
    and (major >= 5 or (family == "opus" and (major > 4 or (major == 4 and minor >= 7))))
  local maximum = model.max_output_tokens and math.max(MIN_THINKING, math.floor(model.max_output_tokens / 2))
  if adaptive then
    body.thinking = { type = "adaptive", display = "summarized" }
    local level = thinking.level
    if thinking.mode == "budget" then
      local percent = math.floor(thinking.tokens * 100 / (maximum or FALLBACK_THINKING))
      level = percent <= 10 and "minimal"
        or percent <= 20 and "low"
        or percent <= 40 and "medium"
        or percent <= 60 and "high"
        or percent <= 80 and "xhigh"
        or "max"
    end
    if level then
      local percent = percentages[level]
      body.output_config = { effort = percent < 40 and "low" or percent < 60 and "medium" or "high" }
    end
  elseif thinking.mode == "adaptive" then
    body.thinking = { type = "adaptive" }
  else
    local tokens = thinking.tokens
    if thinking.mode == "effort" then
      local ceiling = maximum or FALLBACK_THINKING
      tokens = math.min(
        ceiling,
        math.max(math.min(MIN_THINKING, ceiling), math.floor(ceiling * percentages[thinking.level] / 100))
      )
    else
      tokens = math.max(MIN_THINKING, tokens)
      if maximum then
        tokens = math.min(maximum, tokens)
      end
    end
    body.thinking = { type = "enabled", budget_tokens = tokens }
  end
end

function M.build_shared_body(request, options)
  options = options or {}
  local system = array()
  if options.system_prefix ~= nil then
    system[#system + 1] = { type = "text", text = options.system_prefix }
  end
  system[#system + 1] = { type = "text", text = request.system or "", cache_control = { type = "ephemeral" } }
  local tools, err = M.convert_tools(request.tools)
  if tools == nil then
    return nil, err
  end
  local body = {
    max_tokens = request.model.max_output_tokens or FALLBACK_MAX_TOKENS,
    system = system,
    messages = M.convert_messages(request.messages),
    tools = tools,
  }
  apply_thinking(body, request.model, request.thinking)
  return body
end

function M.beta_header(request)
  local betas = {}
  if request.fast and request.model.fast then
    betas[#betas + 1] = "fast-mode-2026-02-01"
  end
  if request.model.id:match("%-1m$") then
    betas[#betas + 1] = "context-1m-2025-08-07"
  end
  if #betas > 0 then
    return table.concat(betas, ",")
  end
end

function M.build_request(request, options)
  local body, err = M.build_shared_body(request, options)
  if not body then
    return nil, err
  end
  body.model = request.model.id:gsub("%-1m$", "")
  body.stream = true
  if request.fast and request.model.fast then
    body.speed = "fast"
  end
  return body
end

return M
