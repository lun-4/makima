--!strict
-- Luau types for `makima.api.register_provider`, as documentation for plugin
-- authors. Nothing checks them against the host; generating them from the
-- Rust types is tracked in #47.

export type Json = any

export type Pricing = {
  input: number,
  output: number,
  cache_write: number,
  cache_read: number,
  fast: { input: number, output: number }?,
}

-- Scales `pricing` inside whole-hour UTC windows (`start` inclusive, `end`
-- exclusive); `multiplier` must exceed 1.
export type PricingSchedule = {
  windows: { { start: number, ["end"]: number } },
  weekdays_only: boolean?,
  multiplier: number,
}

-- Absent fields stay unknown.
export type ModelDescriptor = {
  id: string,
  context_window: number?,
  max_output_tokens: number?,
  pricing: Pricing?,
  pricing_schedule: PricingSchedule?,
  supports_thinking: boolean?,
  requires_thinking: boolean?,
  supports_vision: boolean?,
  -- "pending" while discovery has not said yet; the host keeps a /fast wish.
  supports_fast: (boolean | "pending")?,
  supports_tool_examples: boolean?,
  tier: ("weak" | "medium" | "strong")?,
  free: boolean?,
  provider_data: Json?,
}

export type ContentBlock =
  { type: "text", text: string }
  | { type: "thinking", thinking: string, signature: string? }
  | { type: "redacted_thinking", data: string }
  | { type: "tool_use", id: string, name: string, input: Json, thought_signature: string? }
  | { type: "tool_result", tool_use_id: string, content: string, is_error: boolean? }
  | { type: "image", source: { media_type: string, data: string } }

export type Message = { role: "user" | "assistant", content: { ContentBlock } }

export type Effort = "minimal" | "low" | "medium" | "high" | "xhigh" | "max"

export type Thinking =
  { kind: "off" }
  | { kind: "adaptive" }
  | { kind: "effort", level: Effort }
  | { kind: "budget", tokens: number }

export type Invocation = {
  conversation_id: string,
  operation_id: string,
  attempt: number,
  purpose: "agent" | "compaction" | "isolated",
  agent_id: string?,
}

-- `tools` holds `{ name, description, input_schema, input_examples? }`.
-- `model.max_output_tokens` is the declared cap; `output_tokens` trims it to
-- this request's turn budget.
export type Request = {
  model: {
    id: string,
    output_tokens: number?,
    max_output_tokens: number?,
    context_window: number,
  },
  system: string,
  messages: { Message },
  tools: { Json },
  thinking: Thinking,
  fast: boolean,
  session_id: string?,
  invocation: Invocation,
}

export type Event =
  { type: "text_delta", text: string }
  | { type: "thinking_delta", text: string }
  | { type: "thinking_block_end" }
  | { type: "tool_use_start", id: string, name: string }
  | { type: "prompt_progress", processed: number, total: number, cache: number }

-- Returns false once the call is over.
export type Emit = (event: Event) -> boolean

export type Usage = {
  input_tokens: number?,
  output_tokens: number?,
  cache_creation_input_tokens: number?,
  cache_read_input_tokens: number?,
}

export type StreamResult = {
  message: { role: "assistant", content: { ContentBlock } },
  usage: Usage?,
  stop_reason: ("end_turn" | "tool_use" | "max_tokens")?,
}

-- Returned as `nil, err`. A table with `kind = "config"` is a configuration
-- error (missing key, setting or credentials file) the host does not retry;
-- a table with a status is an API error; anything else is a transport error
-- the host retries. A raised error is not retried.
export type Error =
  string
  | { kind: "config", message: string }
  | { status: number, message: string, retry_after: number? }

export type Response = {
  status: number,
  headers: { [string]: string },
  read: (self: Response) -> (string?, string?),
  close: (self: Response) -> (),
}

export type HttpRequest = {
  url: string,
  method: string?,
  headers: { [string]: string }?,
  body: string?,
  connect_timeout: number?,
  low_speed_timeout: number?,
  idle_timeout: number?,
  unix_socket: string?,
}

-- `makima.credentials.Store`: a provider ctx's `credentials`, or
-- `makima.credentials.open(ctx, key)` for a plugin store.
export type Store = {
  read: (self: Store) -> (Json?, string?),
  write: (self: Store, value: Json) -> (boolean?, string?),
  delete: (self: Store) -> (boolean?, string?),
  refresh: (self: Store, fn: (current: Json?) -> (Json?, string?)) -> (Json?, string?),
  lock: <T...>(self: Store, fn: () -> T...) -> T...,
}

-- `makima.Ctx` as a provider's callbacks receive it. Its fields cannot be
-- assigned; `with_name` narrows it to a plugin ctx.
export type Context = {
  id: string,
  send: (request: HttpRequest) -> (Response?, string?),
  credentials: Store,
  with_name: (self: Context, name: string) -> any,
}

-- The ctx `stream` gets, which can also mark the request delivered.
export type StreamContext = Context & {
  mark_request_delivery: () -> boolean,
}

export type UsageLimit = { kind: Json, percentage: number?, reset_at: number?, detail: string? }

export type Instance = {
  stream: (ctx: StreamContext, request: Request, emit: Emit) -> (StreamResult?, Error?),
  request_delivery_marker: ("auto" | "manual")?,
  fetch_usage: ((ctx: Context) -> ({ plan: string?, limits: { UsageLimit } }?, Error?))?,
  refresh_auth: ((ctx: Context) -> (boolean?, Error?))?,
  reload_auth: ((ctx: Context) -> (boolean?, Error?))?,
  rotate_key: ((ctx: Context) -> boolean)?,
  key_count: (number | (ctx: Context) -> number)?,
  end_conversation: ((
    ctx: Context,
    conversation_id: string,
    reason: "cancelled" | "closed" | "compacted" | "rewound"
  ) -> ())?,
}

export type Choice = { id: string, label: string, detail: string? }

export type LoginUi = {
  input: (self: LoginUi, prompt: string, secret: boolean?, default: string?) -> (string?, string?),
  choice: (self: LoginUi, prompt: string, options: { Choice }) -> (string?, string?),
  instructions: (self: LoginUi, text: string) -> (boolean?, string?),
  device_code: (self: LoginUi, url: string, code: string) -> (boolean?, string?),
  open_browser: (self: LoginUi, url: string) -> (boolean?, string?),
}

-- `config` is the provider's `providers.toml` section.
export type AuthMethod = {
  id: string,
  label: string,
  login: (ctx: Context, ui: LoginUi, config: { [string]: Json }) -> ({ model: string? } | boolean | nil, string?),
}

export type ConfigField = {
  key: string,
  prompt: string,
  default: string?,
  secret: boolean?,
  optional: boolean?,
  choices: { Choice }?,
}

export type Provider = {
  id: string,
  display_name: string,
  models: { ModelDescriptor }?,
  auth: { AuthMethod }?,
  aliases: { { id: string, label: string, method: string } }?,
  config: { ConfigField }?,
  section: string?,
  docs: { env: { string }?, endpoint: string?, notes: string? }?,
  auth_status: ((
    ctx: Context,
    config: { [string]: Json }
  ) -> {
    state: "saved" | "env" | "configured" | "missing",
    detail: string?,
  })?,
  logout: ((ctx: Context) -> ())?,
  discover: ((ctx: Context, config: { [string]: Json }, refresh: boolean) -> ({ ModelDescriptor }?, Error?))?,
  prepare: (
    ctx: Context,
    model: ModelDescriptor,
    config: { [string]: Json }
  ) -> (Instance?, ModelDescriptor | Error | nil),
}

return nil
