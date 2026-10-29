local models = require("maki.openai_models")

assert(models.plan_context_window("gpt-6.1-sol-codex") == 500000)
assert(models.plan_context_window("gpt-5.6-luna") == 372000)
assert(models.plan_context_window("gpt-5.6-luna-preview") == nil)
assert(models.plan_context_window("future-codex") == 272000)

local parsed = assert(models.parse_codex({
  models = {
    { slug = "hidden", visibility = "hide" },
    {
      slug = "model",
      visibility = "list",
      context_window = 1000,
      supported_reasoning_levels = {
        { effort = "high" },
        { effort = "none" },
        { effort = "low" },
        { effort = "high" },
      },
      default_reasoning_level = "low",
      input_modalities = { "text", "image" },
      service_tiers = { { id = "priority" } },
    },
  },
}, "account"))
assert(#parsed == 1 and parsed[1].id == "model" and parsed[1].context_window == 1000)
assert(parsed[1].thinking.levels[1] == "low" and parsed[1].thinking.levels[2] == "high")
assert(not parsed[1].thinking.required and parsed[1].vision and parsed[1].fast)
assert(parsed[1].provider_data.account_id == "account" and parsed[1].provider_data.default_reasoning_level == "low")
assert(
  not assert(
    models.parse_codex({ models = { { slug = "m", visibility = "list", service_tiers = { { id = "priority" } } } } })
  )[1].fast
)
local result, err = models.parse_codex({ models = {} })
assert(not result and err.kind == "config")

parsed = assert(models.parse_platform({
  data = {
    {
      id = "z",
      max_model_len = 4096,
      max_output_length = 512,
      input_modalities = { "text" },
      pricing = { prompt = "1", completion = "2" },
    },
    { id = "a" },
    {},
  },
}))
assert(#parsed == 2 and parsed[1].id == "a" and parsed[2].id == "z")
assert(parsed[2].context_window == 4096 and parsed[2].max_output_tokens == 512 and parsed[2].vision == false)
assert(parsed[2].pricing.input == 1 and parsed[2].pricing.output == 2)

local closed, request = false, nil
local ctx = {
  send = function(_, opts)
    request = opts
    return {
      status = 200,
      read = (function()
        local done = false
        return function()
          if done then
            return ""
          end
          done = true
          return [[{"models":[{"slug":"m","visibility":"list"}]}]]
        end
      end)(),
      close = function()
        closed = true
      end,
    }
  end,
}
parsed =
  assert(models.discover(ctx, { kind = "codex", headers = { Authorization = "Bearer token" }, account_id = "a" }))
assert(closed and parsed[1].id == "m" and request.url:find("client_version=", 1, true))
ctx.send = function()
  return nil, { kind = "network", message = "failed" }
end
assert(#assert(models.discover(ctx, { kind = "codex" })) > 0)
result, err = models.discover(ctx, { kind = "platform" })
assert(not result and err.kind == "network")
