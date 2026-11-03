local compat = require("providers.openai_compat")
local local_server = require("providers.local_server")

maki.api.register_provider(require("providers.anthropic_provider").provider())
maki.api.register_provider(require("providers.openai").provider())
maki.api.register_provider(require("providers.copilot").provider())
maki.api.register_provider(require("providers.google").provider())
maki.api.register_provider(require("providers.vertex").provider())

for _, name in ipairs({ "synthetic", "mistral", "zai", "deepseek", "openrouter", "requesty", "regolo", "tensorx" }) do
  maki.api.register_provider(compat.provider(require("providers." .. name).def))
end

for _, name in ipairs({ "ollama", "llama_cpp" }) do
  maki.api.register_provider(local_server.provider(require("providers." .. name).def))
end
