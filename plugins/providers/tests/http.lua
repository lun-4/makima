local http = require("providers.http")
local t = require("maki.test_helpers")

local KEY_ENV = "TEST_API_KEY"
local HEADER_ERROR = "provider 'test' header 'X-Gateway': environment variable 'GATEWAY' is unset or empty"
local MISSING_KEY = "TEST_API_KEY not set and no saved credentials for 'test'; run `makima auth login test`"

t.case("api_keys_env_pool_wins", function()
  t.with_env({ [KEY_ENV] = " a , ,b " }, function()
    local ctx = t.provider_ctx({}, { api_key = "saved" })
    local keys, source = http.api_keys(ctx, { api_key = "configured" }, KEY_ENV)
    t.same(keys, { "a", "b" })
    t.eq(source, "env")
  end)
end)

t.case("api_keys_blank_env_falls_back_to_saved", function()
  t.with_env({ [KEY_ENV] = " , " }, function()
    local keys, source = http.api_keys(t.provider_ctx({}, { api_key = "saved" }), {}, KEY_ENV)
    t.same(keys, { "saved" })
    t.eq(source, "saved")
  end)
end)

t.case("api_keys_unreadable_saved_falls_back_to_config", function()
  local ctx = t.provider_ctx({})
  ctx.credentials.read = function()
    return nil, "keyring locked"
  end
  local keys, source = http.api_keys(ctx, { api_key = "configured" }, KEY_ENV)
  t.same(keys, { "configured" })
  t.eq(source, "configured")
end)

t.case("api_keys_config_is_one_literal_key", function()
  t.with_env({}, function()
    local keys, source = http.api_keys(t.provider_ctx({}), { api_key = "a,b" }, KEY_ENV)
    t.same(keys, { "a,b" })
    t.eq(source, "configured")
  end)
end)

t.case("api_keys_config_names_env_var", function()
  t.with_env({ OTHER_KEY = "other" }, function()
    local keys = http.api_keys(t.provider_ctx({}), { api_key_env = "OTHER_KEY" }, KEY_ENV)
    t.same(keys, { "other" })
  end)
end)

t.case("api_keys_missing", function()
  t.with_env({}, function()
    local keys, err = http.api_keys(t.provider_ctx({}), {}, KEY_ENV)
    t.eq(keys, nil)
    t.same(err, http.config_error(MISSING_KEY))
  end)
end)

t.case("key_pool_rotates", function()
  local pool = http.key_pool({ "a", "b" })
  t.eq(pool.count, 2)
  t.eq(pool.current(), "a")
  t.eq(pool.rotate(), true)
  t.eq(pool.current(), "b")
  pool.rotate()
  t.eq(pool.current(), "a")
  t.eq(http.key_pool({ "a" }).rotate(), false)
end)

t.case("expand_env_matches_config_semantics", function()
  t.with_env({ A = "1", B = "2", EMPTY = "" }, function()
    t.eq(http.expand_env("x${A}y${B}z"), "x1y2z")
    t.eq(http.expand_env("plain"), "plain")
    t.eq(http.expand_env("${A}${unterminated"), "1${unterminated")
    t.eq(select(2, http.expand_env("${EMPTY}")), "EMPTY")
    t.eq(select(2, http.expand_env("ok ${UNSET}")), "UNSET")
  end)
end)

t.case("headers_replace_case_insensitively", function()
  t.with_env({ TOKEN = "secret" }, function()
    local base = { Authorization = "Bearer key", ["content-type"] = "application/json" }
    local headers = assert(http.headers("test", base, { authorization = "Token ${TOKEN}", ["X-Extra"] = "1" }))
    t.same(headers, { authorization = "Token secret", ["content-type"] = "application/json", ["X-Extra"] = "1" })
    t.eq(base.Authorization, "Bearer key")
  end)
end)

t.case("headers_name_missing_variable", function()
  t.with_env({}, function()
    local headers, err = http.headers("test", {}, { ["X-Gateway"] = "${GATEWAY}" })
    t.eq(headers, nil)
    t.same(err, http.config_error(HEADER_ERROR))
  end)
end)

t.case("non_2xx_becomes_api_error", function()
  local failed = t.response({ "rate ", "limited" }, { status = 429, headers = { ["retry-after"] = " 7 " } })
  local ctx = t.provider_ctx({ failed })
  local response, err = http.send(ctx, { url = "https://example.invalid" })
  t.eq(response, nil)
  t.same(err, { status = 429, message = "rate limited", retry_after = 7, retry = "rate_limit" })
  t.eq(failed.closed, true)
end)

t.case("error_without_body_names_status", function()
  local err = http.error(t.response({}, { status = 503, headers = { ["retry-after"] = "0" } }))
  t.same(err, { status = 503, message = "HTTP 503", retry = "transient" })
end)

t.case("error_marks_truncated_bodies", function()
  local response = t.response({ string.rep("x", 64 * 1024), "tail" }, { status = 400 })
  local err = http.error(response)
  t.eq(#err.message, 64 * 1024)
  t.eq(err.message:sub(-26), "\n[response body truncated]")
  t.eq(response.closed, true)
end)

t.case("send_passes_transport_errors_through", function()
  local ctx = {
    send = function()
      return nil, "connection refused"
    end,
  }
  t.same({ http.send(ctx, { url = "https://example.invalid" }) }, { nil, "connection refused" })
end)

local CONNECT_FAILED = "token request: could not connect: Connection refused"
local IDLE_TIMEOUT = "idle timeout waiting for response body"
local RESET = "connection reset"

t.case("classify_names_a_retry_class", function()
  for _, case in ipairs({
    { CONNECT_FAILED, "connect" },
    { { message = IDLE_TIMEOUT }, "timeout" },
    { RESET, "transient" },
    { { status = 429, message = RESET }, "rate_limit" },
    { { status = 502, message = RESET }, "transient" },
    { { status = 401, message = RESET }, nil },
  }) do
    t.eq(http.classify(case[1]).retry, case[2], http.message(case[1]))
  end
end)

t.case("classify_keeps_a_named_class_and_config_errors", function()
  local named = { status = 500, message = RESET, retry = "none" }
  t.same(http.classify(named), named)
  t.same(http.classify(http.config_error(RESET)), http.config_error(RESET))
end)

t.case("classify_stream_classes_only_failures", function()
  local result, failing = { message = {} }, false
  local instance = {
    stream = function()
      if failing then
        return nil, RESET
      end
      return result
    end,
  }
  http.classify_stream(instance)
  t.eq(instance.stream(), result)
  failing = true
  local _, err = instance.stream()
  t.same(err, { message = RESET, retry = "transient" })
end)

t.case("read_json_decodes_and_caps", function()
  local response = t.response({ '{"a":', "1}" })
  t.same(http.read_json(response), { a = 1 })
  t.eq(response.closed, true)
  local value, err = http.read_json(t.response({ "[1,2,3]" }), 4)
  t.eq(value, nil)
  t.eq(err, "response body exceeds 4 bytes")
  t.eq(select(2, http.read_json(t.response({}, { read_error = "reset" }))), "reset")
end)

t.report()
