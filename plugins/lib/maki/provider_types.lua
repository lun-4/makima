--!strict

-- Provider DTOs and host services are provisional until the provider host is implemented.
-- Prices are dollars per million tokens; reset_at is epoch milliseconds.
-- Success pairs carry a value and no error, failures carry an error and no value.

-- The opaque sentinel returned by maki.json.null().
export type JsonNull = userdata
export type JsonValue = boolean | number | string | JsonNull | { JsonValue } | { [string]: JsonValue }
export type Effort = "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
export type ThinkingPreference =
  { mode: "off" }
  | { mode: "adaptive" }
  | { mode: "effort", level: Effort }
  | { mode: "budget", tokens: number }
export type ThinkingCapabilities =
  { mode: "none" }
  | { mode: "adaptive", levels: { Effort }, required: boolean? }
  | { mode: "effort", levels: { Effort }, required: boolean? }
  | { mode: "budget", min_tokens: number, max_tokens: number, required: boolean? }

export type FastPricing = { input: number, output: number }
export type Pricing = {
  input: number,
  output: number,
  cache_write: number,
  cache_read: number,
  fast: FastPricing?,
}
export type Tier = "weak" | "medium" | "strong" | "compaction"
export type ModelFamily = "claude" | "generic" | "gemini" | "glm" | "gpt" | "synthetic"
export type Model = {
  id: string,
  context_window: number?,
  max_output_tokens: number?,
  thinking: ThinkingCapabilities?,
  vision: boolean?,
  fast: boolean?,
  tool_examples: boolean?,
  pricing: Pricing?,
  provider_data: JsonValue?,
  prefixes: { string }?,
  tier: Tier?,
  family: ModelFamily?,
  default: boolean?,
}
export type PreparedModel = {
  id: string,
  provider: string,
  context_window: number,
  max_output_tokens: number?,
  turn_output_tokens: number?,
  thinking: ThinkingCapabilities?,
  vision: boolean?,
  fast: boolean?,
  tool_examples: boolean?,
  pricing: Pricing?,
  provider_data: JsonValue?,
}

export type ImageSource = { media_type: "image/png" | "image/jpeg" | "image/gif" | "image/webp", data: string }
export type ContentBlock =
  { type: "text", text: string }
  | { type: "thinking", thinking: string, signature: string? }
  | { type: "redacted_thinking", data: string }
  | { type: "tool_use", id: string, name: string, input: JsonValue, thought_signature: string? }
  | { type: "tool_result", tool_use_id: string, content: string, is_error: boolean? }
  | { type: "image", source: ImageSource }
export type Message = {
  role: "user" | "assistant",
  content: { ContentBlock },
  display_text: string?,
  kind: ("turn" | "observation")?,
  observation_source: { kind: "child_result", agent_id: string, turn_id: string }?,
}
export type ToolExample = { input: JsonValue, output: JsonValue? }
export type Tool = { name: string, description: string, input_schema: JsonValue, examples: { ToolExample }? }
export type ProviderRequest = {
  model: PreparedModel,
  system: string?,
  messages: { Message },
  tools: { Tool },
  session_id: string?,
  thinking: ThinkingPreference,
  fast: boolean,
}
export type TokenUsage = { input: number, output: number, cache_creation: number, cache_read: number }
export type ProviderResponse = {
  message: Message,
  usage: TokenUsage,
  stop_reason: ("end_turn" | "tool_use" | "max_tokens")?,
}
export type ProviderEvent =
  { type: "text_delta", text: string }
  | { type: "thinking_delta", text: string }
  | { type: "thinking_block_end" }
  | { type: "tool_use_start", id: string, name: string }
  | { type: "prompt_progress", processed: number, total: number, cache: number }
export type Overflow = { kind: "prompt" } | { kind: "budget", prompt: number?, limit: number? }
export type ProviderError =
  { kind: "api", message: string, status: number?, retry_after_ms: number?, overflow: Overflow? }
  | { kind: "config", message: string }
  | { kind: "network", message: string, retry_after_ms: number? }
  | { kind: "cancelled", message: string }
  | { kind: "closed", message: string }

export type UsageWindow =
  { type: "hours", hours: number }
  | { type: "days", days: number }
  | { type: "monthly" }
  | { type: "weekly", model: string? }
  | { type: "credits" }
  | { type: "subscription" }
  | { type: "other", label: string }
export type UsageLimit = { kind: UsageWindow, percentage: number?, reset_at: number?, detail: string? }
export type ModelUsageRow = {
  model: string,
  input_tokens: number,
  output_tokens: number,
  total_tokens: number,
  spend_microdollars: number,
}
export type ProviderUsage = { plan: string?, limits: { UsageLimit }, by_model_today: { ModelUsageRow }? }

export type OverrideFields = {
  context_window: number?,
  max_output_tokens: number?,
  supports_thinking: boolean?,
  supports_vision: boolean?,
  base: string?,
  path_prefix: string?,
}
export type ProviderOverride = {
  context_window: number?,
  max_output_tokens: number?,
  supports_thinking: boolean?,
  supports_vision: boolean?,
  base: string?,
  path_prefix: string?,
  models: { [string]: OverrideFields }?,
}
export type ConfigModel = {
  id: string,
  tier: Tier,
  context_window: number?,
  max_output_tokens: number?,
  supports_tool_examples: boolean?,
  supports_thinking: boolean?,
  requires_thinking: boolean?,
  supports_vision: boolean?,
  pricing_input: number?,
  pricing_output: number?,
  pricing_cache_write: number?,
  pricing_cache_read: number?,
  pricing_fast_input: number?,
  pricing_fast_output: number?,
}
export type ProviderConfig = {
  display_name: string?,
  protocol: ("openai" | "openai-responses" | "anthropic" | "google")?,
  base_url: string?,
  plan: string?,
  api_key_env: string?,
  api_key: string?,
  default_model: string?,
  project: string?,
  location: string?,
  discover_models: boolean?,
  headers: { [string]: string }?,
  enable_free_models: boolean?,
  overrides: { [string]: ProviderOverride }?,
  models: { ConfigModel }?,
  extensions: { [string]: JsonValue }?,
}
export type PricingWindow = { start: number, ["end"]: number }
export type PricingSchedule = { windows: { PricingWindow }, weekdays_only: boolean, multiplier: number }
export type ConfigField = {
  name: string,
  ty: string?,
  min: number?,
  description: string?,
  required: boolean?,
  default: JsonValue?,
}
export type ProviderDocs = { description: string?, login_url: string?, config: { ConfigField }? }

export type HttpRequest = { method: string, url: string, headers: { [string]: string }?, body: string? }
-- read returns an empty string at EOF; nil with an error indicates failure.
export type HttpResponse = {
  status: number,
  headers: { [string]: string },
  read: (self: HttpResponse) -> (string?, ProviderError?),
  close: (self: HttpResponse) -> (boolean?, ProviderError?),
}
export type CredentialRefresh<T> = (current: T?) -> (T?, ProviderError?)
export type CredentialStore = {
  read: (self: CredentialStore, key: string) -> (JsonValue?, ProviderError?),
  write: (self: CredentialStore, key: string, value: JsonValue) -> (boolean?, ProviderError?),
  delete: (self: CredentialStore, key: string) -> (boolean?, ProviderError?),
  refresh: (self: CredentialStore, key: string, callback: CredentialRefresh<JsonValue>) -> (JsonValue?, ProviderError?),
}
export type ProviderContext = {
  send: (self: ProviderContext, request: HttpRequest) -> (HttpResponse?, ProviderError?),
  credentials: CredentialStore,
}
export type AuthContext = { credentials: CredentialStore }
export type AuthChoice = { id: string, label: string }
export type AuthOutcome = { type: "success" } | { type: "cancelled" }
export type AuthInteraction = {
  input: (self: AuthInteraction, prompt: string, secret: boolean, optional: boolean) -> (string?, ProviderError?),
  choice: (self: AuthInteraction, prompt: string, choices: { AuthChoice }) -> (string?, ProviderError?),
  instructions: (self: AuthInteraction, text: string) -> (boolean?, ProviderError?),
  device_challenge: (self: AuthInteraction, url: string, code: string) -> (boolean?, ProviderError?),
  open_browser: (self: AuthInteraction, url: string) -> (boolean?, ProviderError?),
}
export type AuthStatus = { configured: boolean, source: string?, method: string?, expires_at: number? }
export type Emit = (event: ProviderEvent) -> (boolean?, ProviderError?)
export type Stream = (ctx: ProviderContext, request: ProviderRequest, emit: Emit) -> (ProviderResponse?, ProviderError?)
export type AuthHook = (ctx: ProviderContext) -> (boolean?, ProviderError?)
export type RotateKey = (ctx: ProviderContext) -> (boolean?, ProviderError?)
export type FetchUsage = (ctx: ProviderContext) -> (ProviderUsage?, ProviderError?)
export type ProviderInstance = {
  model: Model,
  key_count: number?,
  stream: Stream,
  refresh_auth: AuthHook?,
  reload_auth: AuthHook?,
  rotate_key: RotateKey?,
  fetch_usage: FetchUsage?,
}
export type Login = (ctx: AuthContext, ui: AuthInteraction) -> (boolean?, ProviderError?)
export type AuthMethod = { id: string, label: string, login: Login }
export type AuthStatusHook = (ctx: AuthContext, config: ProviderConfig) -> (AuthStatus?, ProviderError?)
export type Logout = (ctx: AuthContext, config: ProviderConfig) -> (boolean?, ProviderError?)
export type Discover = (ctx: ProviderContext, config: ProviderConfig) -> ({ Model }?, ProviderError?)
export type Prepare = (
  ctx: ProviderContext,
  model: Model,
  config: ProviderConfig
) -> (ProviderInstance?, ProviderError?)
export type Provider = {
  id: string,
  display_name: string,
  models: { Model },
  auth: { AuthMethod }?,
  auth_status: AuthStatusHook?,
  logout: Logout?,
  discover: Discover?,
  prepare: Prepare,
  family: ModelFamily?,
  supports_thinking: boolean?,
  accepts_arbitrary_models: boolean?,
  fallback_max_output: number?,
  fallback_context_window: number?,
  pricing_schedule: PricingSchedule?,
  config_schema: { ConfigField }?,
  docs: ProviderDocs?,
}

return table.freeze({})
