-- `@model:` completion source and expander. Pairs `@model:spec` candidates
-- with the available-models list the UI passes through `ctx.models`, and
-- rewrites `@model:spec` (and the `@m:` alias) into a `<model:spec>` intent token
-- at submit. Standalone `@model:` with no subagent has no effect: use `/model`
-- to switch the session model.

maki.api.register_completion_source("model", {
  get_items = function(ctx)
    local models = ctx.models or {}
    local items = {}
    for _, spec in ipairs(models) do
      items[#items + 1] = {
        label = "model:" .. spec,
        kind = "model",
        insertion = "@model:" .. spec .. " ",
      }
    end
    return items
  end,
})

local function expand_model(ref)
  return "<model:" .. ref.value .. ">", nil
end

maki.api.register_expander("model", expand_model)
maki.api.register_expander("m", expand_model)

-- `after_instructions` is a system-only slot, so this teaches the main agent
-- what the token means without costing subagent prompts any tokens.
maki.api.register_prompt_hint({
  slot = "after_instructions",
  content = "A `<model:spec>` token in a user message (typed as `@model:spec`) requests that model for an accompanying subagent reference: pass the spec to task_spawn's `model` input when its schema offers one. A `<model:spec>` with no subagent has no effect; the /model command switches the session model.",
})
