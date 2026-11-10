-- Gemini through Vertex AI, authenticated with Application Default
-- Credentials: an `authorized_user` file from `gcloud auth
-- application-default login`, or the metadata server of the machine makima
-- runs on. Service-account and other credential files are rejected.

local compat = require("providers.openai_compat")
local gemini = require("providers.gemini")
local http = require("providers.http")

local M = {}

M.PROJECT_ERROR =
  "Vertex AI requires GOOGLE_CLOUD_PROJECT, GCLOUD_PROJECT, providers.vertex.project, or an ADC quota project"
M.LOGIN_INSTRUCTIONS =
  "Run `gcloud auth application-default login`, then set GOOGLE_CLOUD_PROJECT to the Vertex AI project. Optionally set GOOGLE_CLOUD_LOCATION."
M.METADATA_TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
M.OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
M.CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
M.INVALID_ADC = "invalid Vertex ADC file"
M.INVALID_TOKEN = "invalid Vertex token response"
M.METADATA_UNAVAILABLE = "no Application Default Credentials found and the GCP metadata service is unavailable: "

local DEFAULT_LOCATION = "global"
local GLOBAL_HOST = "https://aiplatform.googleapis.com"
local REGIONAL_HOST = "https://%s-aiplatform.googleapis.com"
local STREAM_PATH = "%s/v1/projects/%s/locations/%s/publishers/google/models/%s:streamGenerateContent?alt=sse"
local AUTHORIZED_USER = "authorized_user"
local UNSUPPORTED_ADC =
  "Vertex ADC credential type '%s' is not supported; use `gcloud auth application-default login` or an attached service account"
local ADC_FILE = "gcloud/application_default_credentials.json"
local UNIX_CONFIG_DIR = "/.config/"
local WINDOWS = "Windows_NT"
local FORM_TYPE = "application/x-www-form-urlencoded"
local JSON_TYPE = "application/json"
local REFRESH_MARGIN_SECS = 60
local OK = 200
local UNAUTHORIZED = 401
local CONTEXT_WINDOW = 1048576
local GEMINI_3_8_FLASH_CONTEXT_WINDOW = 500000
local SCHEMA_TYPES = { string = true, number = true, integer = true, boolean = true, array = true, object = true }

local function row(id, tier, input, output, cache_write, cache_read, extra)
  local result = {
    id = id,
    tier = tier,
    context_window = CONTEXT_WINDOW,
    pricing = { input = input, output = output, cache_write = cache_write, cache_read = cache_read },
  }
  for key, value in pairs(extra or {}) do
    result[key] = value
  end
  return result
end

M.def = {
  id = "vertex",
  display_name = "Google Vertex AI",
  defaults = {
    context_window = 1000000,
    max_output_tokens = 65536,
    supports_thinking = true,
    supports_vision = true,
    supports_tool_examples = false,
  },
  models = {
    row("gemini-2.5-pro", "strong", 1.25, 10.0, 0.375, 0.125),
    row("gemini-2.5-flash", "medium", 0.3, 2.5, 0.0833333333333333, 0.03),
    row("gemini-2.5-flash-lite", "weak", 0.1, 0.4, 0.0833333333333333, 0.01),
    row("gemini-3-flash-preview", "medium", 0.5, 3.0, 0.0833333333333333, 0.05),
    row("gemini-3.1-pro-preview", "medium", 2.0, 12.0, 0.375, 0.2),
    row("gemini-3.1-flash-lite", "weak", 0.25, 1.5, 0.0833333333333333, 0.025),
    row("gemini-3.5-flash", "medium", 1.5, 9.0, 0.0833333333333333, 0.15),
    row("gemini-3.5-flash-lite", "weak", 0.3, 2.5, 0.0833333333333333, 0.03),
    row("gemini-3.6-flash", "medium", 0.75, 3.75, 0.0416666666666667, 0.075, { aliases = { "gemini-3.7-flash" } }),
    -- Capped below 1M: requests past about 500k tokens fail with HTTP 417.
    row(
      "gemini-3.8-flash",
      "medium",
      0.75,
      3.75,
      0.0416666666666667,
      0.075,
      { context_window = GEMINI_3_8_FLASH_CONTEXT_WINDOW }
    ),
  },
}

local function env(name)
  return makima.uv.os_getenv(name)
end

local function nonempty(value)
  return type(value) == "string" and value ~= "" and value or nil
end

-- The credentials of an `authorized_user` ADC file, or nil plus an error.
function M.parse_adc(content)
  local file = makima.json.decode(content)
  if type(file) ~= "table" or type(file.type) ~= "string" then
    return nil, M.INVALID_ADC
  end
  if file.type ~= AUTHORIZED_USER then
    return nil, http.config_error(string.format(UNSUPPORTED_ADC, file.type))
  end
  if
    type(file.client_id) ~= "string"
    or type(file.client_secret) ~= "string"
    or type(file.refresh_token) ~= "string"
  then
    return nil, M.INVALID_ADC
  end
  return {
    client_id = file.client_id,
    client_secret = file.client_secret,
    refresh_token = file.refresh_token,
    token_uri = type(file.token_uri) == "string" and file.token_uri or M.OAUTH_TOKEN_URL,
    quota_project = type(file.quota_project_id) == "string" and file.quota_project_id or nil,
  }
end

local function default_adc_path()
  if env("OS") == WINDOWS then
    local appdata = env("APPDATA")
    return appdata and appdata .. "/" .. ADC_FILE
  end
  local home = env("HOME")
  return home and home .. UNIX_CONFIG_DIR .. ADC_FILE
end

-- The file `GOOGLE_APPLICATION_CREDENTIALS` names, which must exist, then
-- gcloud's default file, then `{ metadata = true }` for the metadata server.
function M.source()
  local path = env("GOOGLE_APPLICATION_CREDENTIALS")
  if path then
    if not makima.fs.metadata(path) then
      return nil, http.config_error("GOOGLE_APPLICATION_CREDENTIALS does not exist: " .. path)
    end
  else
    path = default_adc_path()
    if path and not makima.fs.metadata(path) then
      path = nil
    end
  end
  if not path then
    return { metadata = true }
  end
  local content, err = makima.fs.read(path)
  if not content then
    return nil, string.format("read %s: %s", path, tostring(err))
  end
  return M.parse_adc(content)
end

-- The credential source, project and location, or nil plus an error. Reads
-- the ADC file but never the network.
function M.settings(config)
  local source, err = M.source()
  if not source then
    return nil, err
  end
  local project = nonempty(env("GOOGLE_CLOUD_PROJECT"))
    or nonempty(env("GCLOUD_PROJECT"))
    or nonempty(config.project)
    or nonempty(source.quota_project)
  if not project then
    return nil, http.config_error(M.PROJECT_ERROR)
  end
  local location = nonempty(env("GOOGLE_CLOUD_LOCATION")) or nonempty(config.location) or DEFAULT_LOCATION
  return { source = source, project = project, location = location }
end

function M.stream_url(settings, model_id)
  local host = settings.location == DEFAULT_LOCATION and GLOBAL_HOST or REGIONAL_HOST:format(settings.location)
  return STREAM_PATH:format(
    host,
    http.urlencode(settings.project),
    http.urlencode(settings.location),
    http.urlencode(model_id)
  )
end

-- Vertex wants OpenAPI's uppercase type names in tool schemas.
local function uppercase_schema_types(value)
  if type(value) ~= "table" then
    return
  end
  if SCHEMA_TYPES[value.type] then
    value.type = value.type:upper()
  end
  for _, item in pairs(value) do
    uppercase_schema_types(item)
  end
end

function M.build_request(request)
  local body = gemini.build_request(request)
  uppercase_schema_types(body.tools)
  return body
end

local function token_response(ctx, source)
  if not source.metadata then
    return http.send(ctx, {
      url = source.token_uri,
      method = "POST",
      headers = { ["Content-Type"] = FORM_TYPE },
      body = http.form({
        { "client_id", source.client_id },
        { "client_secret", source.client_secret },
        { "refresh_token", source.refresh_token },
        { "grant_type", "refresh_token" },
        { "scope", M.CLOUD_PLATFORM_SCOPE },
      }),
    })
  end
  local response, err = ctx.send({ url = M.METADATA_TOKEN_URL, headers = { ["Metadata-Flavor"] = "Google" } })
  if not response then
    return nil, http.config_error(M.METADATA_UNAVAILABLE .. tostring(err))
  end
  if response.status ~= OK then
    return nil, http.error(response)
  end
  return response
end

-- A fresh access token as `{ value, expires_at }` (epoch seconds).
local function fetch_token(ctx, source)
  local response, err = token_response(ctx, source)
  if not response then
    return nil, err
  end
  local body, read_err = http.read_json(response)
  if read_err then
    return nil, read_err
  end
  if type(body) ~= "table" or type(body.access_token) ~= "string" or type(body.expires_in) ~= "number" then
    return nil, M.INVALID_TOKEN
  end
  return { value = body.access_token, expires_at = makima.time.now().secs + body.expires_in }
end

local function prepare(_, info, config)
  local settings, err = M.settings(config)
  if not settings then
    return nil, err
  end
  local cached

  -- Sends with the cached token while it has a minute left, else a new one.
  local function send(ctx, url, body, force_refresh)
    if force_refresh or not cached or cached.expires_at <= makima.time.now().secs + REFRESH_MARGIN_SECS then
      local token, token_err = fetch_token(ctx, settings.source)
      if not token then
        return nil, token_err
      end
      cached = token
    end
    return ctx.send({
      url = url,
      method = "POST",
      headers = {
        ["Content-Type"] = JSON_TYPE,
        Authorization = "Bearer " .. cached.value,
        ["x-goog-user-project"] = settings.source.quota_project or settings.project,
      },
      body = body,
    })
  end

  return {
    reload_auth = function()
      cached = nil
    end,
    -- A 401 before the stream starts retries once with a new token.
    stream = function(ctx, request, emit)
      local url = M.stream_url(settings, request.model.id)
      local body = assert(makima.json.encode(M.build_request(request)))
      local response, send_err = send(ctx, url, body, false)
      if response and response.status == UNAUTHORIZED then
        response:close()
        response, send_err = send(ctx, url, body, true)
      end
      if not response then
        return nil, send_err
      end
      if response.status ~= OK then
        return nil, http.error(response)
      end
      local result, stream_err = gemini.stream(response, emit)
      response:close()
      return result, stream_err
    end,
  },
    compat.describe(M.def, info)
end

local function login(_, ui)
  local _, err = ui:instructions(M.LOGIN_INSTRUCTIONS)
  if err then
    return nil, err
  end
  return true
end

local function auth_status(_, config)
  local settings, err = M.settings(config)
  if not settings then
    return { state = "missing", detail = http.message(err) }
  end
  local via = settings.source.metadata and "metadata server" or "ADC"
  return { state = "env", detail = string.format("via %s, project %s", via, settings.project) }
end

function M.provider()
  return {
    id = M.def.id,
    display_name = M.def.display_name,
    docs = {
      env = { "GOOGLE_CLOUD_PROJECT", "GOOGLE_CLOUD_LOCATION", "GOOGLE_APPLICATION_CREDENTIALS" },
      endpoint = GLOBAL_HOST,
      notes = "Authenticates with Application Default Credentials: an `authorized_user` file from `gcloud auth application-default login`, or the metadata server of the machine Makima runs on. Service account files are rejected. The project comes from `GOOGLE_CLOUD_PROJECT` or `project` in `providers.toml`, the location from `GOOGLE_CLOUD_LOCATION` or `location` (default `global`).",
    },
    models = compat.descriptors(M.def),
    auth = { { id = "adc", label = "Application Default Credentials", login = login } },
    auth_status = auth_status,
    prepare = prepare,
  }
end

return M
