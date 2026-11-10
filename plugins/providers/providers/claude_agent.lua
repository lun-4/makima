-- Claude Agent: Claude through the user's own Claude Code login. The
-- TypeScript bridge in `claude-agent-bridge/` runs the Claude Agent SDK and
-- listens on a Unix socket; this provider forwards each request to it
-- unchanged and turns its event stream back into provider events. Makima keeps
-- the agent loop, tools and permissions: the bridge offers Makima's tools to
-- Claude and ends a step when Claude calls one.

local http = require("providers.http")
local sse = require("providers.sse")

local M = {}

M.ID = "claude-agent"
M.PROTOCOL_VERSION = 1
M.STATE_DIR = "claude-agent"
M.SOCKET_FILE = "bridge.sock"
M.DEFAULT_CONTEXT = 200000
M.LONG_CONTEXT = 1000000
M.CACHE_FILE = "claude-agent-models.json"

local BASE_URL = "http://claude-agent"
local STATUS_PATH = "/v1/status"
local MODELS_PATH = "/v1/models"
local STEPS_PATH = "/v1/steps"
local END_PATH = "/v1/conversations/%s/end"
local CONTENT_TYPE = { ["Content-Type"] = "application/json" }
local LONG_CONTEXT_SUFFIX = "[1m]"
-- Claude Code's own pick, which changes under the user; Makima names a model.
local CLAUDE_DEFAULT = "default"
local TIERS = { { "opus", "strong" }, { "sonnet", "medium" }, { "haiku", "weak" } }
local NO_SOCKET = "no state directory for the Claude Agent bridge socket; set `socket` in providers.toml"
local BRIDGE_DOWN = "the Claude Agent bridge is not running at %s; start it with `node claude-agent-bridge/src/main.ts`"
local NOT_LOGGED_IN = "Claude Code is not logged in; run `makima auth login claude-agent`"
local NO_RESULT = "the Claude Agent bridge closed the step without a result"
local CANCELLED = "cancelled"
local CONNECT_FAILED = "could not connect"
local LOGIN_COMMAND = { "claude", "auth", "login" }

local function alias(id, tier)
  return { id = id, tier = tier, context_window = M.DEFAULT_CONTEXT, supports_thinking = true, supports_vision = true }
end

-- Claude Code's aliases, for when no discovery was ever cached.
M.MODELS = { alias("opus", "strong"), alias("sonnet", "medium"), alias("haiku", "weak") }

-- Where discovery is cached, or nil where the host has no cache directory.
function M.cache_path()
  local dir = makima.env.cache_dir()
  return dir and makima.fs.joinpath(dir, M.CACHE_FILE)
end

-- The models the last discovery cached at {path}, else `M.MODELS`. The host
-- resolves a saved model, and checks its `/thinking` level, against these
-- before discovery answers.
function M.cached_models(path)
  local text = path and makima.fs.metadata(path) and makima.fs.read(path)
  local models = text and makima.json.decode(text)
  if type(models) == "table" and #models > 0 then
    return models
  end
  return M.MODELS
end

-- `socket` from providers.toml, else `bridge.sock` in the bridge's
-- directory under Makima's state directory, where the bridge listens by
-- default.
function M.socket(config)
  if type(config.socket) == "string" and config.socket ~= "" then
    return config.socket
  end
  local state = makima.env.state_dir()
  return state and makima.fs.joinpath(state, M.STATE_DIR, M.SOCKET_FILE)
end

local function send(ctx, socket, method, path, body)
  return ctx.send({
    url = BASE_URL .. path,
    unix_socket = socket,
    method = method,
    headers = body and CONTENT_TYPE or nil,
    body = body and assert(makima.json.encode(body)) or nil,
  })
end

-- A refused connection means the bridge is not running, which only the
-- user can fix. Other transport failures keep their message for
-- `http.classify`.
local function unreachable(socket, err)
  if tostring(err):find(CONNECT_FAILED, 1, true) then
    return http.config_error(BRIDGE_DOWN:format(socket))
  end
  return err
end

-- The bridge answers a refused request with its error table as JSON.
local function refused(response)
  local status = response.status
  local body = http.read_json(response)
  if type(body) == "table" and type(body.message) == "string" then
    return body
  end
  return { status = status, message = "HTTP " .. status, retry = "none" }
end

local function get_json(ctx, socket, path)
  local response, err = send(ctx, socket, "GET", path)
  if not response then
    return nil, unreachable(socket, err)
  end
  if response.status ~= 200 then
    return nil, refused(response)
  end
  return http.read_json(response)
end

-- A descriptor for a model Claude Code reports, or nil for its default.
-- Claude Code lists the newest model of each family only as an alias
-- (`opus`), so the model is named by the id the alias resolves to and keeps
-- it until the user picks another. Only aliases get a tier, so each tier's
-- default is the newest model of its family. The context window is the one
-- Claude Code reports, else a guess from the `[1m]` suffix.
function M.describe(row)
  local value = row.value
  if type(value) ~= "string" or value == CLAUDE_DEFAULT then
    return nil
  end
  local resolved = type(row.resolved_model) == "string" and row.resolved_model ~= "" and row.resolved_model or nil
  local id = resolved or value
  local tier
  if resolved and resolved ~= value then
    for _, entry in ipairs(TIERS) do
      if value:find(entry[1], 1, true) then
        tier = entry[2]
        break
      end
    end
  end
  local long = id:sub(-#LONG_CONTEXT_SUFFIX) == LONG_CONTEXT_SUFFIX
  local reported = type(row.context_window) == "number" and row.context_window > 0 and row.context_window or nil
  return {
    id = id,
    tier = not long and tier or nil,
    context_window = reported or (long and M.LONG_CONTEXT or M.DEFAULT_CONTEXT),
    supports_thinking = row.supports_adaptive_thinking ~= false,
    supports_vision = true,
  }
end

-- Reads the step's event stream: events go to {emit}, and the one terminal
-- frame is the result or the bridge's error table. The step may have
-- advanced Claude's session by the time the stream breaks, so a broken
-- stream is not retried.
function M.read_step(response, emit)
  local next_event = sse.events(response)
  while true do
    local event, read_err = next_event()
    if not event then
      response:close()
      return nil, { message = read_err or NO_RESULT, retry = "none" }
    end
    local data, decode_err = makima.json.decode(event.data)
    if decode_err then
      response:close()
      return nil, { message = "invalid bridge frame: " .. decode_err, retry = "none" }
    end
    if event.event == "event" then
      if not emit(data) then
        response:close()
        return nil, { message = CANCELLED, retry = "none" }
      end
    elseif event.event == "done" then
      response:close()
      return data
    elseif event.event == "error" then
      response:close()
      return nil, data
    end
  end
end

-- {cache} is where discovery is kept between runs, nil to keep none.
function M.provider(cache)
  local spec = {
    id = M.ID,
    display_name = "Claude Agent",
    models = M.cached_models(cache),
    auth = {
      {
        id = "claude",
        label = "Claude Code login",
        login = function(_, ui)
          return ui:run(LOGIN_COMMAND)
        end,
      },
    },
    docs = {
      endpoint = "unix:<state dir>/claude-agent/bridge.sock",
      notes = "Uses your Claude Code login through the Claude Agent SDK. Start the bridge first: `npm install --omit=optional` and then `node src/main.ts` in `claude-agent-bridge/` (Node 24 or later). It runs your installed `claude` with its own tools, settings, hooks, MCP servers and CLAUDE.md files turned off, and offers Claude Makima's tools instead, so Makima's permissions apply to every call. `socket` in the `[claude-agent]` section of `providers.toml` points at a bridge listening elsewhere. Claude Code retries failed requests itself, so Makima does not retry this provider.",
    },
  }

  function spec.auth_status(ctx, config)
    local socket = M.socket(config)
    if not socket then
      return { state = "missing", detail = NO_SOCKET }
    end
    local status = get_json(ctx, socket, STATUS_PATH)
    if type(status) ~= "table" then
      return { state = "missing", detail = BRIDGE_DOWN:format(socket) }
    elseif not status.ready then
      return { state = "failed", detail = status.error }
    elseif not status.logged_in then
      return { state = "missing", detail = NOT_LOGGED_IN }
    end
    local account = type(status.account) == "table" and status.account or {}
    return { state = "configured", detail = account.email or account.subscription }
  end

  function spec.discover(ctx, config)
    local socket = M.socket(config)
    if not socket then
      return M.cached_models(cache)
    end
    local body, err = get_json(ctx, socket, MODELS_PATH)
    if not body then
      -- Discovery is best effort: a bridge that is down, or a Claude Code
      -- that is not logged in, only matters to a step. `prepare` reports it
      -- then, and `auth_status` reports it from the login picker, so the
      -- cached models stay listed without a warning at startup.
      makima.log.debug("claude-agent: model discovery failed: " .. http.message(err))
      return M.cached_models(cache)
    end
    local models, seen = {}, {}
    for _, row in ipairs(type(body.models) == "table" and body.models or {}) do
      local model = M.describe(row)
      if model and not seen[model.id] then
        seen[model.id] = true
        models[#models + 1] = model
      end
    end
    if cache and #models > 0 then
      local _, write_err = makima.fs.atomic_write(cache, assert(makima.json.encode(models)))
      if write_err then
        makima.log.warn("claude-agent: model cache not written: " .. write_err)
      end
    end
    return models
  end

  function spec.prepare(_, _, config)
    local socket = M.socket(config)
    if not socket then
      return nil, http.config_error(NO_SOCKET)
    end
    -- The bridge takes the conversation over, and may move its
    -- predecessor's Claude session to it, as soon as a step arrives: before
    -- it answers, and even for a step it then refuses.
    local instance = { request_delivery_marker = "manual" }

    function instance.stream(ctx, request, emit)
      if not ctx.mark_request_delivery() then
        return nil, { message = CANCELLED, retry = "none" }
      end
      local response, err =
        send(ctx, socket, "POST", STEPS_PATH, { protocol_version = M.PROTOCOL_VERSION, request = request })
      if not response then
        return nil, unreachable(socket, err)
      end
      if response.status ~= 200 then
        return nil, refused(response)
      end
      return M.read_step(response, emit)
    end

    function instance.end_conversation(ctx, conversation_id, reason)
      local path = END_PATH:format(http.urlencode(conversation_id))
      local response, err = send(ctx, socket, "POST", path, { reason = reason })
      if not response then
        makima.log.warn(string.format("claude-agent: ending %s failed: %s", conversation_id, http.message(err)))
        return
      end
      response:close()
    end

    return instance
  end

  return spec
end

return M
