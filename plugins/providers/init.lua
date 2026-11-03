local aperture = require("providers.aperture")
local catalog = require("providers.catalog")
local compat = require("providers.openai_compat")
local local_server = require("providers.local_server")
local opencode = require("providers.opencode")

local registered = {}

local function register(spec)
  maki.api.register_provider(spec)
  registered[spec.id] = true
end

register(require("providers.anthropic_provider").provider())
register(require("providers.openai").provider())
register(require("providers.copilot").provider())
register(require("providers.google").provider())
register(require("providers.vertex").provider())
register(aperture.provider())

for _, name in ipairs({ "synthetic", "mistral", "zai", "deepseek", "openrouter", "requesty", "regolo", "tensorx" }) do
  register(compat.provider(require("providers." .. name).def))
end

for _, name in ipairs({ "ollama", "llama_cpp" }) do
  register(local_server.provider(require("providers." .. name).def))
end

-- Seeded from the cache only, so startup never waits on the network.
local cache = catalog.new(catalog.cache_dir())
cache:load()
register(opencode.provider(cache, opencode.ZEN))
register(opencode.provider(cache, opencode.GO))
for _, id in ipairs(cache:servable(registered)) do
  register(catalog.provider(cache, id))
end
