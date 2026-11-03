local sigv4 = require("providers.sigv4")
local t = require("maki.test_helpers")

local HOST = "bedrock-runtime.us-east-1.amazonaws.com"
local MODEL_URL = "https://" .. HOST .. "/model/anthropic.claude-3-5-sonnet-20241022-v2%3A0/invoke-with-response-stream"
local TIMESTAMP = "20240101T000000Z"
-- Pinned by the native signer against the doubly encoded `%253A` path.
local AUTHORIZATION = "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/bedrock/aws4_request, "
  .. "SignedHeaders=content-type;host;x-amz-date, "
  .. "Signature=ff69b69725fa6fa2de10b57605b9fca403f357980e082d138d1772abe10fbea2"

local function input(session_token)
  return {
    method = "POST",
    url = MODEL_URL,
    headers = { { "content-type", "application/json" }, { "host", HOST } },
    body = "{}",
    access_key = "AKID",
    secret_key = "SECRET",
    session_token = session_token,
    region = "us-east-1",
    service = "bedrock",
    timestamp = TIMESTAMP,
  }
end

t.case("matches_pinned_signature", function()
  t.same(sigv4.sign(input()), { { "Authorization", AUTHORIZATION }, { "x-amz-date", TIMESTAMP } })
end)

t.case("session_token_is_signed_and_returned", function()
  local signed = sigv4.sign(input("TOKEN"))
  t.has(signed[1][2], "SignedHeaders=content-type;host;x-amz-date;x-amz-security-token")
  t.same(signed[3], { "x-amz-security-token", "TOKEN" })
end)

t.report()
