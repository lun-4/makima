local json_response = require("maki.json_response")

local M = {}
local CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
local DEVICE_CODE_URL = "https://auth.openai.com/api/accounts/deviceauth/usercode"
local DEVICE_TOKEN_URL = "https://auth.openai.com/api/accounts/deviceauth/token"
local TOKEN_URL = "https://auth.openai.com/oauth/token"
local REDIRECT_URI = "https://auth.openai.com/deviceauth/callback"
local MAX_RESPONSE_BYTES = 1024 * 1024
local DEFAULT_EXPIRY = 3600
local POLL_MARGIN = 3

M.verification_url = "https://auth.openai.com/codex/device"

local function nonempty(value)
  return type(value) == "string" and value ~= ""
end

local function form(value)
  return (value:gsub("[^A-Za-z0-9%-._~]", function(char)
    return string.format("%%%02X", string.byte(char))
  end))
end

local function post(ctx, url, content_type, body, pending)
  local response, err =
    ctx:send({ url = url, method = "POST", headers = { ["Content-Type"] = content_type }, body = body })
  if not response then
    return nil, type(err) == "table" and err or { kind = "network", message = "OpenAI authentication request failed" }
  end
  if response.status ~= 200 then
    local status = response.status
    response:close()
    if pending and (status == 403 or status == 404) then
      return nil, { kind = "pending", status = status, message = "Authorization pending" }
    end
    return nil, { kind = "api", status = status, message = "OpenAI authentication request failed" }
  end
  local value, read_err = json_response.read(response, MAX_RESPONSE_BYTES)
  response:close()
  if read_err then
    return nil, read_err
  end
  if type(value) ~= "table" then
    return nil, { kind = "api", message = "Invalid OpenAI authentication response" }
  end
  return value
end

function M.account_id(token)
  if type(token) ~= "string" then
    return nil
  end
  local payload = token:match("^[^.]+%.([^.]+)%.[^.]+$")
  if not payload or #payload % 4 == 1 then
    return nil
  end
  payload = payload:gsub("-", "+"):gsub("_", "/")
  payload = payload .. string.rep("=", (4 - #payload % 4) % 4)
  local ok, decoded = pcall(maki.base64.decode, payload)
  if not ok then
    return nil
  end
  local claims = maki.json.decode(decoded)
  if type(claims) ~= "table" then
    return nil
  end
  if nonempty(claims.chatgpt_account_id) then
    return claims.chatgpt_account_id
  end
  local auth = claims["https://api.openai.com/auth"]
  if type(auth) == "table" and nonempty(auth.chatgpt_account_id) then
    return auth.chatgpt_account_id
  end
  local org = type(claims.organizations) == "table" and claims.organizations[1]
  if type(org) == "table" and nonempty(org.id) then
    return org.id
  end
end

local function tokens(value, now_ms)
  if
    not nonempty(value.access_token)
    or not nonempty(value.refresh_token)
    or (value.expires_in ~= nil and (type(value.expires_in) ~= "number" or value.expires_in < 0 or value.expires_in >= math.huge))
    or type(now_ms) ~= "number"
    or now_ms < 0
    or now_ms >= math.huge
  then
    return nil, { kind = "api", message = "Invalid OpenAI token response" }
  end
  return {
    access = value.access_token,
    refresh = value.refresh_token,
    expires = now_ms + (value.expires_in or DEFAULT_EXPIRY) * 1000,
    account_id = M.account_id(value.id_token) or M.account_id(value.access_token),
  }
end

function M.start(ctx)
  local body, encode_err = maki.json.encode({ client_id = CLIENT_ID })
  if not body then
    return nil, { kind = "config", message = tostring(encode_err) }
  end
  local value, err = post(ctx, DEVICE_CODE_URL, "application/json", body)
  if not value then
    return nil, err
  end
  if not nonempty(value.device_auth_id) or not nonempty(value.user_code) then
    return nil, { kind = "api", message = "Invalid OpenAI device authorization response" }
  end
  local interval = tonumber(value.interval) or 5
  if interval < 1 or interval >= math.huge then
    interval = 5
  end
  return {
    device_auth_id = value.device_auth_id,
    user_code = value.user_code,
    interval = interval + POLL_MARGIN,
    verification_url = M.verification_url,
  }
end

function M.poll(ctx, session)
  local body, err = maki.json.encode({ device_auth_id = session.device_auth_id, user_code = session.user_code })
  if not body then
    return nil, { kind = "config", message = tostring(err) }
  end
  local value, request_err = post(ctx, DEVICE_TOKEN_URL, "application/json", body, true)
  if not value then
    return nil, request_err
  end
  if not nonempty(value.authorization_code) or not nonempty(value.code_verifier) then
    return nil, { kind = "api", message = "Invalid OpenAI device token response" }
  end
  return value
end

function M.exchange(ctx, device_token, now_ms)
  if not nonempty(device_token.authorization_code) or not nonempty(device_token.code_verifier) then
    return nil, { kind = "config", message = "Invalid OpenAI device token" }
  end
  local body = "grant_type=authorization_code&code="
    .. form(device_token.authorization_code)
    .. "&redirect_uri="
    .. form(REDIRECT_URI)
    .. "&client_id="
    .. form(CLIENT_ID)
    .. "&code_verifier="
    .. form(device_token.code_verifier)
  local value, err = post(ctx, TOKEN_URL, "application/x-www-form-urlencoded", body)
  if not value then
    return nil, err
  end
  return tokens(value, now_ms)
end

function M.refresh(ctx, current, now_ms)
  if type(current) ~= "table" or not nonempty(current.refresh) then
    return nil, { kind = "config", message = "OpenAI refresh token is missing" }
  end
  local body = "grant_type=refresh_token&refresh_token=" .. form(current.refresh) .. "&client_id=" .. form(CLIENT_ID)
  local value, err = post(ctx, TOKEN_URL, "application/x-www-form-urlencoded", body)
  if not value then
    return nil, err
  end
  return tokens(value, now_ms)
end

return M
