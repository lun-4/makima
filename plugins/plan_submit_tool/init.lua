-- Mode-scoped `plan_submit` tool: prints the plan inline and shows the review
-- form for accept / refine / cancel. Requires plan mode; registered to load only
-- when plan's toolset includes it (see plugins/mode_plan_override).
-- Enable with:
--   [plugins.plan_submit_tool]
--   enabled = true

local function handler(_input, ctx)
  if ctx:mode() ~= "plan" then
    return {
      llm_output = "plan_submit is only available in plan mode",
      is_error = true,
    }
  end

  local ok, err = maki.ui.action("plan_submit")
  if not ok then
    return { llm_output = "error: " .. err, is_error = true }
  end
  return { llm_output = "Plan submitted for review." }
end

maki.api.register_tool({
  name = "plan_submit",
  description = [[Submit the finished plan for user review in the interactive UI.

Prints the plan inline as a display-only message (hidden from your context) and
shows the plan review form. The user can accept (hands off to implementation in
the current session), refine (keep planning), or cancel; accept and refine flow
through the plan form. No floating window is opened. Call it only once the plan
is complete and you are ready to begin implementation. Requires plan mode.]],
  kind = "execute",
  schema = {
    type = "object",
    properties = {},
  },
  handler = handler,
})
