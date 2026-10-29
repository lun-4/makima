local mistral = require("maki.mistral")
local messages = {
  { role = "assistant", content = "answer", reasoning_content = "thought" },
  { role = "assistant", content = "", reasoning_content = "only" },
  { role = "assistant", content = "unchanged" },
}
mistral.convert_messages(messages)
assert(messages[1].reasoning_content == nil)
assert(messages[1].content[1].thinking[1].text == "thought")
assert(messages[1].content[2].text == "answer")
assert(#messages[2].content == 1 and messages[2].content[1].thinking[1].text == "only")
assert(messages[3].content == "unchanged")

local request = {
  model = { id = "mistral-medium-latest", thinking = { mode = "adaptive" } },
  messages = {},
  thinking = { mode = "effort", level = "low" },
}
local body = assert(mistral.build_request(request))
assert(body.reasoning_effort == "high" and body.stream_options.include_usage)
request.thinking = { mode = "off" }
assert(mistral.build_request(request).reasoning_effort == nil)
request.model.id = "ministral-14b-latest"
request.thinking = { mode = "adaptive" }
assert(mistral.build_request(request).reasoning_effort == nil)
