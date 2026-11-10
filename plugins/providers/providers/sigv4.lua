-- AWS Signature Version 4 for one request.

local M = {}

local ALGORITHM = "AWS4-HMAC-SHA256"
local DATE_HEADER = "x-amz-date"
local TOKEN_HEADER = "x-amz-security-token"

local function hex(bytes)
  return (bytes:gsub(".", function(byte)
    return string.format("%02x", string.byte(byte))
  end))
end

local function hex_sha256(bytes)
  return hex(makima.crypto.sha256(bytes))
end

-- Bedrock model ids hold ':', which URLs already spell as '%3A'. SigV4 wants
-- the path encoded again, so '%' itself becomes '%25'.
local function encode_path(path)
  return (path:gsub("[^%w%-_%.~/]", function(byte)
    return string.format("%%%02X", string.byte(byte))
  end))
end

-- Signs {input}: `method`, `url`, `headers` (a list of `{ name, value }` with
-- lowercase names, including `host`), `body`, `access_key`, `secret_key`,
-- optional `session_token`, `region`, `service` and `timestamp`
-- (`YYYYMMDDTHHMMSSZ`). Returns the `{ name, value }` headers to add.
function M.sign(input)
  local suffix = input.url:match("^https?://[^/?]+(.*)$")
  local path, query = suffix:match("^([^?]*)%??(.*)$")
  if path == "" then
    path = "/"
  end
  local headers = table.clone(input.headers)
  headers[#headers + 1] = { DATE_HEADER, input.timestamp }
  if input.session_token then
    headers[#headers + 1] = { TOKEN_HEADER, input.session_token }
  end
  table.sort(headers, function(left, right)
    return left[1] < right[1]
  end)
  local canonical, names = {}, {}
  for _, header in ipairs(headers) do
    canonical[#canonical + 1] = header[1] .. ":" .. header[2] .. "\n"
    names[#names + 1] = header[1]
  end
  local signed_headers = table.concat(names, ";")
  local canonical_request = table.concat({
    input.method,
    encode_path(path),
    query,
    table.concat(canonical),
    signed_headers,
    hex_sha256(input.body),
  }, "\n")
  local date = input.timestamp:sub(1, 8)
  local scope = table.concat({ date, input.region, input.service, "aws4_request" }, "/")
  local string_to_sign = table.concat({ ALGORITHM, input.timestamp, scope, hex_sha256(canonical_request) }, "\n")
  local key = makima.crypto.hmac_sha256("AWS4" .. input.secret_key, date)
  key = makima.crypto.hmac_sha256(key, input.region)
  key = makima.crypto.hmac_sha256(key, input.service)
  key = makima.crypto.hmac_sha256(key, "aws4_request")
  local signature = hex(makima.crypto.hmac_sha256(key, string_to_sign))
  local result = {
    {
      "Authorization",
      string.format(
        "%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
        ALGORITHM,
        input.access_key,
        scope,
        signed_headers,
        signature
      ),
    },
    { DATE_HEADER, input.timestamp },
  }
  if input.session_token then
    result[#result + 1] = { TOKEN_HEADER, input.session_token }
  end
  return result
end

return M
