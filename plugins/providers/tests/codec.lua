local codec = require("providers.codec")
local t = require("makima.test_helpers")

-- Each refusal is a documented server message; the comment names its source.
local OVERFLOW_CASES = {
  -- llama.cpp server-context.cpp
  { 400, "request (268914 tokens) exceeds the available context size (262144 tokens)", true },
  -- OpenAI error codes guide
  { 400, "Input exceeds context limit", true },
  { 400, "This model's maximum context length is 8192 tokens. However, you requested 9850 tokens", true },
  -- Gemini troubleshooting guide
  { 400, "The input token count exceeds the maximum number of tokens allowed", true },
  { 400, "Request contains too many tokens. Please reduce the input size.", true },
  { 400, "Your input context is too long.", true },
  -- Ollama API errors
  { 400, "context length exceeded", true },
  -- Anthropic errors, and any 413 (RFC 9110)
  { 413, "prompt is too long", true },
  { 413, "Payload too large", true },
  -- DeepSeek, Mistral, OpenRouter
  { 400, "This model's maximum context length is 131072 tokens. However, you requested 168754 tokens", true },
  {
    400,
    "Prompt contains 321774 tokens and 0 draft tokens, too large for model with 262144 maximum context length",
    true,
  },
  {
    400,
    "This endpoint's maximum context length is 200000 tokens. However, you requested about 5028244 tokens",
    true,
  },
  -- Bedrock validation exceptions
  { 400, "Input is too long for requested model.", true },
  { 400, "Input is too long for the model", true },
  -- The prompt plus the output budget, named even when max_tokens is quoted
  { 400, "input length and `max_tokens` exceed context limit: 199773 + 8192 > 200000", true },
  { 400, "you requested 10000 tokens (6000 in the messages, 4000 in the completion)", true },
  -- Not about the context
  { 400, "Rate limit exceeded", false },
  { 400, "Invalid API key", false },
  { 500, "Internal server error", false },
  { 400, "The output is too long", false },
  -- A cap we sent, which compacting cannot fix
  { 400, "Invalid 'max_tokens': integer above maximum value. Expected a value <= 32768", false },
  { 400, "max_completion_tokens is too large: 100000", false },
  { 400, "max_output_tokens exceeds the model maximum", false },
}

t.case("overflow_follows_the_server_refusal", function()
  for _, case in ipairs(OVERFLOW_CASES) do
    local status, message, expected = case[1], case[2], case[3]
    t.eq(codec.overflows(status, message), expected, message)
    t.eq(codec.api_error(status, message).overflow, expected or nil, message)
  end
end)

t.case("frame_errors_carry_overflow", function()
  local err = codec.frame_error({ type = "request_too_large", message = "prompt is too long" })
  t.eq(err.status, 413)
  t.eq(err.overflow, true)
end)
