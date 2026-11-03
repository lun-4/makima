+++
title = "Providers"
weight = 5
[extra]
group = "Reference"
+++

# Providers

Makima talks to LLM providers over their HTTP APIs. Models are split into three tiers: **weak** (cheap and fast), **medium** (balanced), and **strong** (highest capability, highest cost). There is also a **compaction** tier for choosing a dedicated model to summarize context when the conversation grows long.

Open the model picker with `/model` and press `!`, `@`, `#`, or `$` on any row to assign it to strong, medium, weak, or compaction. Press the same key again to remove the assignment. Your overrides are saved to `~/.local/state/makima/model-tiers` and apply across sessions.

## Auth Reloading

Makima re-reads auth from storage and environment variables each time a new agent spawns (`/new`, retry, session load). If you run `makima auth login` in another terminal or change an env var, the next session picks it up without a restart.

You can set multiple API keys in one env var (`ANTHROPIC_API_KEY=sk-1,sk-2,sk-3`) and they rotate automatically on rate-limit or auth errors.

## Base URL Overrides

Every provider honors a `<SLUG>_BASE_URL` env var (`anthropic` -> `ANTHROPIC_BASE_URL`, `llama-cpp` -> `LLAMA_CPP_BASE_URL`). Set it to the origin of a proxy or a compatible endpoint and Makima appends the API paths itself:

```sh
ANTHROPIC_BASE_URL=https://my-proxy.internal makima
```

It wins over `providers.toml` and built-in defaults. `ANTHROPIC_BASE_URL` and `OPENAI_BASE_URL` are the same names the official SDKs use, so an existing proxy setup carries over as is. One exception: `OPENAI_BASE_URL` only redirects the platform API, never the ChatGPT Coding Plan backend.

You can also set `base_url` for a built-in provider in `~/.config/makima/providers.toml`. It overrides the built-in default and loses to the env var above:

```toml
[openai]
base_url = "http://xxxx:1234/v1"
```

The built-in provider still owns the slug, so `protocol`, `api_key_env`, `discover_models` and `models` are ignored with a warning. Use a custom slug if you need those.

## Built-in Providers

### OpenAI

- **Env var**: `OPENAI_API_KEY` (also supports OAuth device flow)
- **API**: `https://api.openai.com/v1`

| Tier | Models | Pricing (in/out per 1M tokens) | Context |
|------|--------|-------------------------------|---------|
| Weak | gpt-6-luna | $0.10 / $0.50 | 272K ctx / 128K out |
| Weak | **gpt-5.6-luna** (default) | $1.00 / $6.00 | 372K ctx / 128K out |
| Weak | gpt-5.4-nano | $0.20 / $1.25 | 400K ctx / 128K out |
| Weak | gpt-5.4-mini | $0.75 / $4.50 | 400K ctx / 128K out |
| Weak | gpt-4.1-nano | $0.10 / $0.40 | 1047K ctx / 32K out |
| Medium | gpt-6.1-sol | $2.00 / $10.00 | 500K ctx / 128K out |
| Medium | gpt-6.1-terra | $1.00 / $5.00 | 500K ctx / 128K out |
| Medium | gpt-6-sol | $2.00 / $10.00 | 272K ctx / 128K out |
| Medium | **gpt-5.6-terra** (default) | $2.50 / $15.00 | 372K ctx / 128K out |
| Medium | gpt-4.1-mini | $0.40 / $1.60 | 1047K ctx / 32K out |
| Medium | gpt-4.1 | $2.00 / $8.00 | 1047K ctx / 32K out |
| Medium | o4-mini | $1.10 / $4.40 | 200K ctx / 100K out |
| Medium | gpt-5.1-codex-mini | $0.25 / $2.00 | 400K ctx / 128K out |
| Strong | gpt-6-astra | $10.00 / $50.00 | 272K ctx / 128K out |
| Strong | **gpt-5.6-sol** (default) | $5.00 / $30.00 | 372K ctx / 128K out |
| Strong | gpt-5.5 | $5.00 / $30.00 | 1050K ctx / 128K out |
| Strong | gpt-5.4 | $2.50 / $15.00 | 1050K ctx / 128K out |
| Strong | o3 | $2.00 / $8.00 | 200K ctx / 100K out |
| Strong | gpt-5.3-codex | $1.75 / $14.00 | 400K ctx / 128K out |
| Strong | gpt-5.2-codex | $1.75 / $14.00 | 400K ctx / 128K out |
| Strong | gpt-5.1-codex-max | $1.25 / $10.00 | 400K ctx / 128K out |
| Strong | gpt-5.1-codex | $1.25 / $10.00 | 400K ctx / 128K out |

Defaults: gpt-5.6-luna (weak), gpt-5.6-terra (medium), gpt-5.6-sol (strong)

#### Auth

You can use OpenAI two ways.

**API key** — Set `OPENAI_API_KEY` or pick `OpenAI (API key)` in `/login` and paste a key from [platform.openai.com/api-keys](https://platform.openai.com/api-keys).

**ChatGPT subscription** — Use OpenAI Codex through your ChatGPT login instead. Run `makima auth login openai` or pick `OpenAI Codex (ChatGPT login)` in `/login`. Makima guides you through the OAuth device flow at [auth.openai.com/codex](https://auth.openai.com/codex) and stores your OAuth tokens, which take precedence over the API key.

With ChatGPT OAuth (`makima auth login openai`) the model list comes from the Codex backend's own `/models` endpoint, so a model your plan gains shows up without a Makima update, with the context window and reasoning levels the backend declares for it. The table above is the offline fallback. The endpoint hides models newer than the Codex CLI version Makima reports, so a brand new release can lag until that version is bumped.

### Google

- **Env var**: `GEMINI_API_KEY`
- **API**: `https://generativelanguage.googleapis.com/v1beta`
- **Features**: Native Gemini API with thinking support

| Tier | Models | Pricing (in/out per 1M tokens) | Context |
|------|--------|-------------------------------|---------|
| Weak | **gemini-2.0-flash-lite** (default) | $0.07 / $0.30 | 1048K ctx / 65K out |
| Medium | **gemini-2.5-flash** (default) | $0.15 / $0.60 | 1048K ctx / 65K out |
| Strong | **gemini-2.5-pro** (default) | $1.25 / $5.00 | 1048K ctx / 65K out |

Defaults: gemini-2.5-pro (strong), gemini-2.5-flash (medium), gemini-2.0-flash-lite (weak)

### Google Vertex AI

- **Env var**: `GOOGLE_CLOUD_PROJECT` plus Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS` or `gcloud auth application-default login`); optional `GOOGLE_CLOUD_LOCATION`
- **API**: `https://aiplatform.googleapis.com/v1`
- **Features**: Native Vertex AI Gemini API using Application Default Credentials

| Tier | Models | Pricing (in/out per 1M tokens) | Context |
|------|--------|-------------------------------|---------|
| Weak | **gemini-2.5-flash-lite** (default) | $0.10 / $0.40 | 1048K ctx / 65K out |
| Weak | gemini-3.1-flash-lite | $0.25 / $1.50 | 1048K ctx / 65K out |
| Weak | gemini-3.5-flash-lite | $0.30 / $2.50 | 1048K ctx / 65K out |
| Medium | **gemini-2.5-flash** (default) | $0.30 / $2.50 | 1048K ctx / 65K out |
| Medium | gemini-3-flash-preview | $0.50 / $3.00 | 1048K ctx / 65K out |
| Medium | gemini-3.1-pro-preview | $2.00 / $12.00 | 1048K ctx / 65K out |
| Medium | gemini-3.5-flash | $1.50 / $9.00 | 1048K ctx / 65K out |
| Medium | gemini-3.6-flash, gemini-3.7-flash | $0.75 / $3.75 | 1048K ctx / 65K out |
| Medium | gemini-3.8-flash | $0.75 / $3.75 | 500K ctx / 65K out |
| Strong | **gemini-2.5-pro** (default) | $1.25 / $10.00 | 1048K ctx / 65K out |

Defaults: gemini-2.5-pro (strong), gemini-2.5-flash (medium), gemini-2.5-flash-lite (weak)

### Copilot

- **Env var**: `GH_COPILOT_TOKEN` (or run `makima auth login copilot` to import a token from gh CLI, the Copilot client, or the system keyring)
- **API**: `https://api.githubcopilot.com (or GraphQL-discovered Copilot API endpoint)`
- **Features**: Native Copilot Chat HTTP API with model endpoint discovery

| Tier | Models | Pricing (in/out per 1M tokens) | Context |
|------|--------|-------------------------------|---------|
| Weak | gpt-5-mini | $0.25 / $2.00 | 200K ctx / 100K out |
| Weak | gpt-5.4-mini | $0.75 / $4.50 | 200K ctx / 100K out |
| Weak | gpt-5.4-nano | $0.20 / $1.25 | 200K ctx / 100K out |
| Weak | claude-haiku-4.5 | $1.00 / $5.00 | 200K ctx / 64K out |
| Weak | gemini-3.5-flash | $1.50 / $9.00 | 200K ctx / 65K out |
| Weak | mai-code-1-flash-picker | $0.75 / $4.50 | 200K ctx / 100K out |
| Weak | **gpt-5.6-luna** (default) | $0.20 / $1.20 | 200K ctx / 100K out |
| Medium | gemini-3.6-flash | $0.75 / $3.75 | 200K ctx / 65K out |
| Medium | gemini-3.7-flash | $0.75 / $3.75 | 200K ctx / 65K out |
| Medium | claude-sonnet-4.5, claude-sonnet-4.6 | $3.00 / $15.00 | 200K ctx / 64K out |
| Medium | claude-sonnet-5 | $2.00 / $10.00 | 200K ctx / 100K out |
| Medium | kimi-k2.7-code | $0.95 / $4.00 | 200K ctx / 100K out |
| Medium | gemini-3.1-pro-preview | $2.00 / $12.00 | 200K ctx / 65K out |
| Medium | **gpt-5.6-terra** (default) | $2.00 / $12.00 | 200K ctx / 100K out |
| Medium | grok-4.5 | $2.00 / $6.00 | 200K ctx / 100K out |
| Medium | grok-4.6 | $2.00 / $6.00 | 200K ctx / 100K out |
| Strong | gpt-5.5 | $5.00 / $30.00 | 200K ctx / 100K out |
| Strong | kimi-k3 | $3.00 / $15.00 | 200K ctx / 100K out |
| Strong | gpt-5.4 | $2.50 / $15.00 | 200K ctx / 100K out |
| Strong | gpt-5.6-sol | $5.00 / $30.00 | 200K ctx / 100K out |
| Strong | gpt-5.3-codex | $1.75 / $14.00 | 200K ctx / 100K out |
| Strong | **claude-opus-5, claude-opus-4.8, claude-opus-4.7, claude-opus-4.6, claude-opus-4.5** (default) | $5.00 / $25.00 | 200K ctx / 64K out |
| Strong | claude-opus-4.8-fast, claude-fable-5 | $10.00 / $50.00 | 200K ctx / 100K out |

Defaults: gpt-5.6-luna (weak), gpt-5.6-terra (medium), claude-opus-5 (strong)

### Ollama

- **Env var**: `OLLAMA_HOST` for local/remote (e.g. `http://localhost:11434`), `OLLAMA_API_KEY` for auth
- **API**: `http://localhost:11434/v1`
- **Features**: Local or remote inference via OLLAMA_HOST, cloud fallback via OLLAMA_API_KEY

This provider talks the OpenAI-compatible `/v1` API, so it also works with llama.cpp's server, LocalAI, or anything else that speaks the same protocol. Just point `OLLAMA_HOST` to the right address (e.g. `http://localhost:8080` for llama.cpp).

### LlamaCpp

- **Env var**: `LLAMA_CPP_API_KEY`
- **API**: `http://localhost:8080/v1`
- **Features**: Local or remote inference via LLAMA_CPP_HOST, set optional key via LLAMA_CPP_API_KEY

Connects to any OpenAI-compatible `/v1` endpoint. Point `LLAMA_CPP_HOST` to your server address (defaults to `http://localhost:8080`).

### Opencode Zen

- **Env var**: `OPENCODE_API_KEY`
- **API**: `https://opencode.ai/zen/v1`
- **Features**: Dynamically discovered models via [models.dev](https://models.dev/) + all the models provided by Opencode Zen API

No hardcoded model catalog. Use any model ID supported by this provider.

By default Makima hides free models from the Opencode catalog. To list free models (they use a public fallback, no API key needed), add this to `~/.config/makima/providers.toml`:

```toml
[opencode]
enable_free_models = true
```

The default is `false`.

### Aperture

- **Env var**: `APERTURE_HOST` (e.g. `https://your-host.tailnet.ts.net`)
- **API**: `Aperture gateway (set APERTURE_HOST)`
- **Features**: Tailscale Aperture LLM gateway; set APERTURE_HOST or configure in providers.toml

Aperture discovers models from your gateway. Set `APERTURE_HOST` to your Tailscale Aperture endpoint (e.g. `https://your-host.tailnet.ts.net`). No API key needed, Tailscale handles auth.

### Opencode Go

- **Env var**: `OPENCODE_API_KEY`
- **API**: `https://opencode.ai/zen/go/v1`
- **Features**: Dynamically discovered models via [models.dev](https://models.dev/) + all the models provided by Opencode Go API

No hardcoded model catalog. Use any model ID supported by this provider. An API key is required.


## Model Identifiers

Models are referenced as `provider/model_id`:

```
anthropic/claude-sonnet-4-6
openai/gpt-4.1
zai/glm-4.7
```

If the model name is unique across providers, the prefix can be omitted.

### Models newer than your Maki version

The tables above list the models Maki curates. Any other id a provider accepts works too: type it into `/model` or pass it to `--model`. The picker also lists what the provider's own model endpoint reports, so same-day releases are selectable there.

For an id no table covers, rates, context window, vision and thinking support come from [models.dev](https://models.dev/), refreshed daily (`maki models --refresh` forces it). Maki reads each field on its own, so a row that lists a price but no context window still leaves the window to the sources below.

Sources rank by how sure they are to describe the exact model you asked for:

1. What the provider's own model endpoint reported this session.
2. A curated row for that id, including its dated snapshots. `claude-sonnet-4-5-20250929` reads the `claude-sonnet-4-5` row.
3. models.dev.
4. A curated row for a close relative, reached by shared prefix. `glm-5.4` falls back to `glm-5` here, and takes its family and tier from it either way.
5. The provider's defaults, with no cost estimate.

A curated row is checked against the provider's own pricing page, so it wins for the id it names. For a relative it loses to models.dev, because a rate nobody checked against the id you typed is only a guess.

New models start at the **medium** tier until you assign one in the picker.

## providers.toml

`providers.toml` lives in the config directory (`~/.config/makima/providers.toml` on Linux/macOS, `%APPDATA%\makima\providers.toml` on Windows). It is the file for provider overrides and custom HTTP providers. Two jobs:

1. Tweak a built-in (pick a plan, change its base URL, set `enable_free_models` for Opencode).
2. Declare a custom provider that speaks OpenAI, Anthropic, or Google wire format.

```toml
# Point a built-in at a proxy. Env vars still win over this file.
[anthropic]
base_url = "https://my-proxy.internal"

# Full custom provider. Slug becomes the `provider/` prefix in model specs.
[my-proxy]
display_name = "My Proxy"
protocol = "openai"            # openai | openai-responses | anthropic | google
base_url = "https://llm.example.com/v1"
api_key_env = "MY_PROXY_API_KEY"
default_model = "my-proxy/fast-v1"
discover_models = true         # also list models via the provider's /models endpoint

[[my-proxy.models]]
id = "fast-v1"
tier = "weak"
context_window = 128000
max_output_tokens = 16384
pricing_input = 0.5
pricing_output = 1.5

[[my-proxy.models]]
id = "smart-v1"
tier = "strong"
context_window = 200000
max_output_tokens = 32000
supports_thinking = true
supports_vision = false
```

### Provider fields

| Field | Type | Notes |
|-------|------|-------|
| `display_name` | string | Shown in pickers and auth status |
| `protocol` | string | `openai`, `openai-responses`, `anthropic`, or `google`. Required for custom slugs |
| `base_url` | string | Origin of the API. Makima appends the protocol paths |
| `plan` | string | Built-in plan key (see Plans below). Sets base URL and default model |
| `api_key_env` | string | Env var that holds the key. Defaults to `<SLUG>_API_KEY` |
| `api_key` | string | Inline key (prefer the env var or `makima auth login`) |
| `default_model` | string | Used after login when no model is saved yet |
| `discover_models` | bool | When true, also probe the provider's model list endpoint (default false) |
| `enable_free_models` | bool | Opencode only. Show free catalog models (default false) |
| `models` | array | Declared models for custom providers (see below) |
| `overrides` | table | Aperture only. Per-upstream model overrides (see below) |

### Model fields

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `id` | string | required | Model id. Spec becomes `{slug}/{id}` |
| `tier` | string | `medium` | `weak`, `medium`, `strong`, or `compaction` |
| `context_window` | u32 | protocol default | Tokens of context |
| `max_output_tokens` | u32 | protocol default | Max completion tokens |
| `supports_tool_examples` | bool | protocol default | |
| `supports_thinking` | bool | protocol default | |
| `requires_thinking` | bool | false | For APIs that reject requests with thinking disabled. Implies `supports_thinking` and raises thinking to minimal effort when off (including compaction) |
| `supports_vision` | bool | protocol default | When false, image input and `view_image` are off |
| `pricing_input` / `pricing_output` | f64 | 0 | USD per 1M tokens |
| `pricing_cache_write` / `pricing_cache_read` | f64 | 0 | USD per 1M tokens |
| `pricing_fast_input` / `pricing_fast_output` | f64 | unset | Fast-mode pricing when the provider supports it |

Custom slugs must not reuse a built-in provider name. A bad TOML parse exits with code 2 at startup so a typo cannot silently empty the registry.

You can also create a custom provider interactively with `makima auth login` and choosing the custom option. That writes a starter entry to this file.

### Aperture overrides

Aperture proxies upstream providers, exposing each model as `aperture/<upstream>/<model>`. Overrides keyed by upstream provider id live under `[aperture.overrides]`:

```toml
[aperture.overrides.llmserver]
base = "llama-cpp"
context_window = 131072
max_output_tokens = 16384

[aperture.overrides.llmserver.models."qwen-3.6"]
context_window = 262144
supports_vision = true
```

Provider-level fields apply to every model from that upstream; per-model entries under `models` win field by field. Fields: `context_window`, `max_output_tokens`, `supports_thinking`, `supports_vision`, `base` (remaps an opaque vendor to a native provider; e.g. `llama-cpp`, `google`, `anthropic`), and `path_prefix`. Model ids containing dots must be quoted (`"qwen3.6"`) since TOML treats a bare dotted key as a nested table.

Makima sends `/v1` (or `/v1beta` for Gemini routes, nothing for Anthropic and Z.AI), and Aperture appends that path to the upstream's base url. If an upstream base url already carries its own path, set `path_prefix = ""` for it to avoid a doubled path. Z.AI defaults to no prefix since its API path has no `/v1` segment; point the upstream base url at the full API root (e.g. `https://api.z.ai/api/paas/v4`).

### Plans

No built-in currently ships more than one plan.

## Dynamic Providers

To add a custom provider or proxy, drop an executable script into the config `providers/` directory (`~/.config/makima/providers/` on Linux/macOS, `%APPDATA%\makima\providers\` on Windows). The script must handle these subcommands:

| Subcommand | Timeout | What it does |
|------------|---------|--------|
| `info` | 5s | Return JSON with `display_name`, `base` provider, `has_auth` |
| `models` | 5s | Return JSON array of model entries (optional) |
| `resolve` | 30s | Return auth JSON (`base_url`, `headers`) |
| `login` | interactive | OAuth or credential flow |
| `logout` | interactive | Clear credentials |
| `refresh` | 30s | Refresh auth tokens |

`resolve` is called each time a new agent spawns, so scripts should read tokens from disk instead of caching them in memory. That way auth changes from other processes get picked up.

The `base` field specifies which built-in provider to inherit the model catalog from. Valid values: `anthropic`, `openai`, `google`, `copilot`, `ollama`, `llama-cpp`, `opencode`, `aperture`.

If your provider serves models not in the base catalog, add a `models` subcommand returning:

```json
[{"id": "my-model-v2", "tier": "strong", "context_window": 200000, "max_output_tokens": 16384}]
```

Only `id` is required. Optional fields: `tier` (default `medium`), `context_window` (128K), `max_output_tokens` (16K), `pricing` (`{input, output, cache_write, cache_read}`, all per 1M tokens), `supports_tool_examples` (defaults to the base provider's setting), `supports_thinking` (defaults to the base provider's setting), `requires_thinking` (default false; for APIs that reject requests with thinking off, raises it to minimal effort and implies `supports_thinking`), `supports_vision` (defaults to the base provider's setting; when false, image input and the `view_image` tool are disabled). The first model listed per tier is used for sub-agents. Without this subcommand, the base provider's models are used.

A `llama-cpp` model can replace Makima's token-budget mapping with its native thinking fields. Each thinking mode maps to a JSON fragment merged into the request body:

```json
[{
  "id": "reasoning-model",
  "supports_thinking": true,
  "thinking_fields": {
    "off": {"reasoning_effort": "none"},
    "adaptive": {"reasoning_effort": "medium"},
    "low": {"reasoning_effort": "low"},
    "medium": {"reasoning_effort": "medium"},
    "xhigh": {"reasoning_effort": "xhigh"}
  }
}]
```

`off` is used when thinking is off, `adaptive` when thinking is on without a chosen level. Any other key is an effort level, one of `minimal`, `low`, `medium`, `high`, `xhigh`, `max`. The levels you declare are the ones the model accepts: whatever you ask for snaps into them, downwards first, so a level the model never advertised is never sent. Every part is optional.

Fragments are merged into the body, so nesting works too. A template toggle is just a fragment:

```json
"thinking_fields": {
  "off": {"chat_template_kwargs": {"enable_thinking": false}},
  "adaptive": {"chat_template_kwargs": {"enable_thinking": true}}
}
```

Named modes send only these fields, no token budget. An explicit `/thinking <budget>` snaps into the levels you declared; a model that declares none gets the `adaptive` fragment plus `thinking_budget_tokens`. Any mode you left undeclared falls back to the usual `thinking_budget_tokens` mapping, so no request ever ends up saying nothing. Models without `thinking_fields` keep the existing llama.cpp behavior.

Dynamic provider models are namespaced as `{slug}/{model_id}` (e.g. `myproxy/claude-sonnet-4-6`).

### Script Name Rules

- Must start with a letter or digit
- Only letters, digits, underscores, and hyphens after that
- Can't reuse a built-in provider's slug
- Must be executable
