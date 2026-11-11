local compat = require("providers.openai_compat")
local thinking = require("providers.thinking")

local M = {}

local REFERER = "https://makima.ln4.net"
local APP_TITLE = "Makima"
local CHAT_API = "chat"
-- Requesty's curated routing policies, with short stable ids that spread
-- across upstreams. Listed before the raw `<vendor>/<model>` catalog.
local MANAGED_PATH = "/models/managed"
local UNREADABLE = "requesty: unreadable field %q on model %q, ignoring it"
local MANAGED_ONLY = "requesty: full catalog unavailable, listing managed models only: %s"
local CATALOG_ONLY = "requesty: managed models unavailable, listing full catalog only: %s"

-- A missing field is Requesty not saying; a field in a shape we cannot read
-- is upstream drift, logged and dropped without losing the model.
local function reader(row)
  return function(name, valid)
    local value = row[name]
    if value == nil then
      return nil
    end
    if valid(value) then
      return value
    end
    makima.log.warn(string.format(UNREADABLE, name, row.id))
    return nil
  end
end

local function is_number(value)
  return type(value) == "number"
end

local function is_boolean(value)
  return type(value) == "boolean"
end

function M.parse_model(row)
  if type(row.id) ~= "string" or (type(row.api) == "string" and row.api ~= CHAT_API) then
    return nil
  end
  local read = reader(row)
  -- `0` is Requesty's "unknown"; kept, it would go out as `max_tokens: 0`.
  local function limit(name)
    local value = compat.limit(read(name, is_number))
    return value and value > 0 and value or nil
  end
  local function price(name)
    local value = read(name, is_number)
    return value and value * compat.PER_MILLION
  end
  local input, output = price("input_price"), price("output_price")
  return {
    id = row.id,
    context_window = limit("context_window"),
    max_output_tokens = limit("max_output_tokens"),
    -- Half a price is no price: it would read as free.
    pricing = (input and output) and {
      input = input,
      output = output,
      cache_write = price("caching_price") or 0,
      cache_read = price("cached_price") or 0,
    } or nil,
    supports_thinking = read("supports_reasoning", is_boolean) == true,
    supports_vision = read("supports_vision", is_boolean) == true,
  }
end

-- Managed policies first, then the catalog, without repeating an id.
function M.merge(managed, catalog)
  local merged, seen = {}, {}
  for _, list in ipairs({ managed, catalog }) do
    for _, model in ipairs(list) do
      if not seen[model.id] then
        seen[model.id] = true
        merged[#merged + 1] = model
      end
    end
  end
  return merged
end

local function listing(ctx, client, path)
  local body, err = client.get_json(ctx, client.base .. path)
  if err then
    return nil, err
  end
  return compat.parse_listing(body, M.parse_model)
end

local function outcome(result)
  if result.ok then
    return result.value[1], result.value[2]
  end
  return nil, result.err
end

-- Both listings at once, since the picker waits for the slowest provider.
function M.list_models(ctx, client)
  local results = makima.async.gather({
    function()
      return { listing(ctx, client, MANAGED_PATH) }
    end,
    function()
      return { listing(ctx, client, compat.MODELS_PATH) }
    end,
  })
  local managed, managed_err = outcome(results[1])
  local catalog, catalog_err = outcome(results[2])
  if managed and catalog then
    return M.merge(managed, catalog)
  elseif managed then
    makima.log.warn(string.format(MANAGED_ONLY, tostring(catalog_err)))
    return managed
  elseif catalog then
    makima.log.warn(string.format(CATALOG_ONLY, tostring(managed_err)))
    return catalog
  end
  return nil, managed_err
end

M.def = {
  id = "requesty",
  display_name = "Requesty",
  env = "REQUESTY_API_KEY",
  base_url = "https://router.requesty.ai/v1",
  notes = "Requesty routes across many models with one key, including managed routing policies (`requesty/claude-sonnet-4-5`) and the full `<vendor>/<model>` catalog (`requesty/openai/gpt-4o-mini`). Get a key at [app.requesty.ai/api-keys](https://app.requesty.ai/api-keys). Set `REQUESTY_BASE_URL=https://router.eu.requesty.ai/v1` to keep all traffic in the EU.",
  login_url = "https://app.requesty.ai/api-keys",
  model = "openai/gpt-5.5",
  include_stream_usage = true,
  defaults = {
    context_window = 200000,
    max_output_tokens = 128000,
    supports_thinking = true,
    supports_vision = false,
  },
  list_models = M.list_models,
  encode = function(body, request, model)
    -- Requesty only adds Anthropic cache breakpoints when asked.
    body.requesty = { auto_cache = true }
    if model.supports_thinking then
      body.reasoning_effort =
        thinking.effort(request.thinking, thinking.dialects.prefer_high, thinking.max_budget(request.model))
    end
    return { ["HTTP-Referer"] = REFERER, ["X-Title"] = APP_TITLE }
  end,
}

return M
