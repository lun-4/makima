local M = {}

local CODEX_URL = "https://chatgpt.com/backend-api/codex/models?client_version=0.153.4"
local PLATFORM_URL = "https://api.openai.com/v1"
local MAX_RESPONSE_BYTES = 4 * 1024 * 1024
local plan_models = {
  "gpt-6.1-sol",
  "gpt-6.1-terra",
  "gpt-6-astra",
  "gpt-6-sol",
  "gpt-6-luna",
  "gpt-5.6-luna",
  "gpt-5.6-terra",
  "gpt-5.6-sol",
  "gpt-5.5",
  "gpt-5.4",
  "gpt-5.4-mini",
  "gpt-5.3-codex",
  "gpt-5.2-codex",
  "gpt-5.1-codex-mini",
  "gpt-5.1-codex-max",
  "gpt-5.1-codex",
}
local levels = { "minimal", "low", "medium", "high", "xhigh", "max" }

function M.plan_context_window(id)
  if type(id) ~= "string" then
    return nil
  end
  local known = id == "gpt-5.2" or id:find("-codex", 1, true) ~= nil
  for _, model in ipairs(plan_models) do
    if model == id then
      known = true
      break
    end
  end
  if not known then
    return nil
  end
  if
    id == "gpt-6.1-sol"
    or id == "gpt-6.1-terra"
    or (id:find("-codex", 1, true) and (id:find("gpt-6.1-sol", 1, true) == 1 or id:find("gpt-6.1-terra", 1, true) == 1))
  then
    return 500000
  end
  if id:find("gpt-5.6-", 1, true) == 1 and not id:find("-codex", 1, true) then
    return 372000
  end
  return 272000
end

local function positive_integer(value)
  if type(value) == "number" and value > 0 and value <= 4294967295 and value % 1 == 0 then
    return value
  end
end

local function has(list, value)
  if type(list) ~= "table" then
    return false
  end
  for _, item in ipairs(list) do
    if item == value then
      return true
    end
  end
  return false
end

function M.parse_codex(value, account_id)
  if type(value) ~= "table" or type(value.models) ~= "table" then
    return nil, { kind = "api", message = "Invalid Codex models response" }
  end
  local result = {}
  for _, entry in ipairs(value.models) do
    if type(entry) == "table" and entry.visibility == "list" and type(entry.slug) == "string" and entry.slug ~= "" then
      local supported, off = {}, false
      if type(entry.supported_reasoning_levels) == "table" then
        for _, level in ipairs(entry.supported_reasoning_levels) do
          if type(level) == "table" then
            if level.effort == "none" then
              off = true
            end
            supported[level.effort or ""] = true
          end
        end
      end
      local ordered = {}
      for _, level in ipairs(levels) do
        if supported[level] then
          ordered[#ordered + 1] = level
        end
      end
      local fast = false
      if type(entry.service_tiers) == "table" then
        for _, tier in ipairs(entry.service_tiers) do
          if type(tier) == "table" and tier.id == "priority" then
            fast = true
          end
        end
      end
      local model = {
        id = entry.slug,
        context_window = positive_integer(entry.context_window),
        thinking = #ordered > 0 and { mode = "effort", levels = ordered, required = not off } or { mode = "none" },
        fast = fast and type(account_id) == "string" and account_id ~= "",
        provider_data = { account_id = account_id, default_reasoning_level = entry.default_reasoning_level, off = off },
      }
      if type(entry.input_modalities) == "table" and #entry.input_modalities > 0 then
        model.vision = has(entry.input_modalities, "image")
      end
      result[#result + 1] = model
    end
  end
  if #result == 0 then
    return nil,
      {
        kind = "config",
        message = "Codex models response listed no visible models; the endpoint schema likely changed",
      }
  end
  return result
end

function M.parse_platform(value)
  if type(value) ~= "table" or type(value.data) ~= "table" then
    return nil, { kind = "api", message = "Invalid OpenAI models response" }
  end
  local result = {}
  for _, entry in ipairs(value.data) do
    if type(entry) == "table" and type(entry.id) == "string" and entry.id ~= "" then
      local model = {
        id = entry.id,
        context_window = positive_integer(entry.context_length)
          or positive_integer(entry.max_model_len)
          or positive_integer(entry.max_context_length),
        max_output_tokens = positive_integer(entry.max_tokens) or positive_integer(entry.max_output_length),
      }
      if type(entry.input_modalities) == "table" then
        model.vision = has(entry.input_modalities, "image")
      end
      local pricing = entry.pricing
      if type(pricing) == "table" and type(pricing.prompt) == "string" and type(pricing.completion) == "string" then
        local input, output = tonumber(pricing.prompt), tonumber(pricing.completion)
        if input and output then
          model.pricing = {
            input = input,
            output = output,
            cache_write = tonumber(pricing.cache_creation) or 0,
            cache_read = tonumber(pricing.cache_read) or 0,
          }
        end
      end
      result[#result + 1] = model
    end
  end
  table.sort(result, function(a, b)
    return a.id < b.id
  end)
  return result
end

function M.discover(ctx, auth)
  local codex = auth.kind == "codex"
  local url = codex and CODEX_URL or (auth.base_url or PLATFORM_URL):gsub("/+$", "") .. "/models"
  local response, err = ctx:send({ url = url, method = "GET", headers = auth.headers or {} })
  local value
  if response then
    if response.status == 200 then
      value, err = require("maki.json_response").read(response, MAX_RESPONSE_BYTES)
    else
      err = { kind = "api", status = response.status, message = "OpenAI model discovery failed" }
    end
    response:close()
  elseif type(err) ~= "table" then
    err = { kind = "network", message = "OpenAI model discovery failed" }
  end
  if value then
    if codex then
      value, err = M.parse_codex(value, auth.account_id)
    else
      value, err = M.parse_platform(value)
    end
  end
  if value then
    return value
  end
  if not codex then
    return nil, err
  end
  local fallback = {}
  for _, id in ipairs(plan_models) do
    fallback[#fallback + 1] = { id = id }
  end
  return fallback
end

return M
