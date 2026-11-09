local aperture = require("providers.aperture")
local catalog = require("providers.catalog")
local compat = require("providers.openai_compat")
local custom = require("providers.custom")
local executable = require("providers.executable")
local http = require("providers.http")
local local_server = require("providers.local_server")
local opencode = require("providers.opencode")
local routes = require("providers.routes")

local registered = {}

-- Every provider registered here has its stream errors classed for retry.
local function register(spec)
  local prepare = spec.prepare
  spec.prepare = function(...)
    local results = table.pack(prepare(...))
    if type(results[1]) == "table" and results[1].stream then
      http.classify_stream(results[1])
    end
    return table.unpack(results, 1, results.n)
  end
  maki.api.register_provider(spec)
  registered[spec.id] = true
end

-- Providers the user defines: a bad one is logged and left out instead of
-- taking every provider down with it.
local function register_user(id, source, make_spec)
  local ok, err = pcall(function()
    register(make_spec())
  end)
  if not ok then
    maki.log.warn(string.format("%s: provider %s left out: %s", source, id, tostring(err)))
  end
end

local function register_compat(names)
  for _, name in ipairs(names) do
    register(compat.provider(require("providers." .. name).def))
  end
end

-- Registration order is auto-detect order: with no model chosen, the first
-- provider that has credentials supplies the default model.
register(require("providers.anthropic_provider").provider())
register(require("providers.openai").provider())
register(require("providers.copilot").provider())
register_compat({ "zai", "synthetic", "deepseek", "regolo" })
register(require("providers.google").provider())
register(require("providers.vertex").provider())
register(aperture.provider())
register_compat({ "mistral", "openrouter", "requesty", "tensorx" })

for _, name in ipairs({ "ollama", "llama_cpp" }) do
  register(local_server.provider(require("providers." .. name).def))
end
local claude_agent = require("providers.claude_agent")
register(claude_agent.provider(claude_agent.cache_path()))

-- Seeded from the cache only, so startup never waits on the network.
local cache = catalog.new(catalog.cache_dir())
cache:load()
register(opencode.provider(cache, opencode.ZEN))
register(opencode.provider(cache, opencode.GO))

-- The providers a script can name as its base.
local bases = table.clone(routes.ROUTES)
bases.aperture = aperture.delegated()
bases.opencode = opencode.delegated(cache)
register(custom.creator(executable.notes(bases) .. "\n\n" .. catalog.NOTES))

-- Sections with a protocol, then scripts, each win over a models.dev
-- provider of the same id but never over a built-in one. A built-in
-- provider's section may name a protocol of its own.
local sections = maki.api.startup_provider_config()
local ids = {}
for id, section in pairs(sections) do
  if section.protocol then
    ids[#ids + 1] = id
  end
end
table.sort(ids)
for _, id in ipairs(ids) do
  if not registered[id] then
    register_user(id, "providers.toml", function()
      return custom.provider(id, sections[id])
    end)
  end
end

local config_dir, state_dir = maki.env.config_dir(), maki.env.state_dir()
if config_dir then
  local cache_path = state_dir and maki.fs.joinpath(state_dir, executable.CACHE_FILE)
  for _, script in ipairs(executable.scan(maki.fs.joinpath(config_dir, executable.DIR), cache_path, bases)) do
    if registered[script.slug] then
      maki.log.warn(string.format("%s: provider id %s is already taken, skipping", script.path, script.slug))
    else
      register_user(script.slug, script.path, function()
        return executable.provider(script, bases[script.info.base])
      end)
    end
  end
end

for _, id in ipairs(cache:servable(registered)) do
  register(catalog.provider(cache, id))
end
