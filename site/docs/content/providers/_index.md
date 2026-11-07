+++
title = "Providers"
weight = 5
[extra]
group = "Reference"
+++

# Providers

Makima talks to LLM providers over their HTTP APIs. Every provider is registered by a Lua plugin; the built-in ones come from the bundled `providers` plugin. Models are split into three tiers: **weak** (cheap and fast), **medium** (balanced), and **strong** (highest capability, highest cost). There is also a **compaction** tier for choosing a dedicated model to summarize context when the conversation grows long.

## Model picker

`/model` without arguments and Ctrl+M open the model picker. The plan form's `Implementation model` row opens the same picker.

Type to filter the list and use Up and Down to move. Tab moves to the options the highlighted model supports, such as thinking and fast mode. Each option starts at `Use current`, which keeps the agent's value. Type a value, or cycle through the choices with Up and Down for thinking or Space, Left and Right for fast. Enter applies the model and its options together, and Esc cancels everything.

Options the highlighted model does not support are hidden. If you set one and then move to a model without it, it is left out, and it comes back when you move to a model that has it.

Press `!`, `@`, `#`, or `$` on any row of the model list to assign it to strong, medium, weak, or compaction. Press the same key again to remove the assignment. Your overrides are saved to `~/.local/state/makima/model-tiers` and apply across sessions.

## Auth Reloading

Makima re-reads auth from storage and environment variables each time a new agent spawns (`/new`, retry, session load). If you run `makima auth login` in another terminal or change an env var, the next session picks it up without a restart.

An API key variable can hold several keys separated by commas (`sk-1,sk-2,sk-3`). They rotate on rate-limit and auth errors.

## providers.toml

`providers.toml` lives in the config directory (`~/.config/makima/providers.toml` on Linux and macOS, `%APPDATA%\makima\providers.toml` on Windows). Each section is named after a provider id and holds that provider's settings. `makima auth login` and `/login` write the answers they ask for there once the login works, and `makima auth logout` removes the section.

Most hosted providers read these fields:

| Field | Notes |
|-------|-------|
| `base_url` | Origin of a proxy or compatible endpoint; Makima appends the API paths |
| `api_key_env` | Env var that holds the key, instead of the one listed below |
| `api_key` | Inline key (prefer the env var or `makima auth login`) |
| `headers` | Extra headers sent with every request; values expand `${VAR}` from the environment, and a same-name header replaces the built-in auth header |

A provider that lists a `<ID>_BASE_URL` variable below honors it over `base_url`, so a proxy can be set for one run:

```sh
<ID>_BASE_URL=https://my-proxy.internal makima
```

Local servers list a host variable instead, and the `base_url` saved by `makima auth login` wins over it. Provider-specific fields are described in each provider's section.

## Built-in Providers

### Anthropic

- **Id**: `anthropic`
- **Env vars**: `ANTHROPIC_API_KEY`, `ANTHROPIC_BASE_URL`
- **API**: `https://api.anthropic.com`
- **Login**: `makima auth login anthropic` (API key)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **claude-haiku-4-5** (default) | $1.00 / $5.00 | 200K ctx / 64K out |
| medium | **claude-sonnet-5** (default) | $2.00 / $10.00 | 200K ctx / 128K out |
| medium | claude-sonnet-4-6 | $3.00 / $15.00 | 200K ctx / 64K out |
| medium | claude-sonnet-4-5 | $3.00 / $15.00 | 200K ctx / 64K out |
| medium | claude-sonnet-4 | $3.00 / $15.00 | 200K ctx / 64K out |
| strong | **claude-opus-5** (default) | $5.00 / $25.00 | 200K ctx / 128K out |
| strong | claude-opus-4-8 | $5.00 / $25.00 | 200K ctx / 128K out |
| strong | claude-opus-4-7 | $5.00 / $25.00 | 200K ctx / 128K out |
| strong | claude-opus-4-6 | $5.00 / $25.00 | 200K ctx / 128K out |
| strong | claude-opus-4-5 | $5.00 / $25.00 | 200K ctx / 64K out |
| strong | claude-fable-5 | $10.00 / $50.00 | 200K ctx / 128K out |
| strong | claude-opus-4-0 | $15.00 / $75.00 | 200K ctx / 32K out |
| strong | claude-opus-4-1 | $15.00 / $75.00 | 200K ctx / 32K out |

Add `-1m` to any Claude model, like `claude-sonnet-4-6-1m`, to use the 1M token context window.

#### Amazon Bedrock

If you already use Claude through AWS Bedrock, set `CLAUDE_CODE_USE_BEDROCK=1` and every Anthropic request goes through Bedrock instead, with the same models and features.

You need `AWS_REGION` and one of these for auth:

| Method | Env vars |
|--------|----------|
| IAM credentials | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_SESSION_TOKEN`) |
| Credentials file | `AWS_PROFILE` (defaults to `default`), reads `~/.aws/credentials` |
| Bearer token | `AWS_BEARER_TOKEN_BEDROCK` |
| Container credentials | `AWS_CONTAINER_CREDENTIALS_FULL_URI` |
| Gateway proxy | `CLAUDE_CODE_SKIP_BEDROCK_AUTH=1` + `ANTHROPIC_BEDROCK_BASE_URL` (skips signing, for a proxy that handles auth) |

`ANTHROPIC_MODEL` overrides the model and `ANTHROPIC_BEDROCK_BASE_URL` the endpoint. These names match Claude Code, so a Bedrock setup from there works here as is.

### OpenAI

- **Id**: `openai`
- **Env vars**: `OPENAI_API_KEY`, `OPENAI_BASE_URL`
- **API**: `https://api.openai.com/v1`
- **Login**: `makima auth login openai` (API key, ChatGPT login)
- **Login alias**: `makima auth login openai-codex`, OpenAI Codex (ChatGPT login)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **gpt-5.6-luna** (default) | $1.00 / $6.00 | 372K ctx / 128K out |
| weak | gpt-6-luna | $0.10 / $0.50 | 272K ctx / 128K out |
| weak | gpt-5.4-nano | $0.20 / $1.25 | 400K ctx / 128K out |
| weak | gpt-5.4-mini | $0.75 / $4.50 | 400K ctx / 128K out |
| weak | gpt-4.1-nano | $0.10 / $0.40 | 1047K ctx / 32K out |
| medium | **gpt-5.6-terra** (default) | $2.50 / $15.00 | 372K ctx / 128K out |
| medium | gpt-6.1-sol | $2.00 / $10.00 | 500K ctx / 128K out |
| medium | gpt-6.1-terra | $1.00 / $5.00 | 500K ctx / 128K out |
| medium | gpt-6-sol | $2.00 / $10.00 | 272K ctx / 128K out |
| medium | gpt-4.1-mini | $0.40 / $1.60 | 1047K ctx / 32K out |
| medium | gpt-4.1 | $2.00 / $8.00 | 1047K ctx / 32K out |
| medium | o4-mini | $1.10 / $4.40 | 200K ctx / 100K out |
| medium | gpt-5.1-codex-mini | $0.25 / $2.00 | 400K ctx / 128K out |
| strong | **gpt-5.6-sol** (default) | $5.00 / $30.00 | 372K ctx / 128K out |
| strong | gpt-6-astra | $10.00 / $50.00 | 272K ctx / 128K out |
| strong | gpt-5.5 | $5.00 / $30.00 | 1050K ctx / 128K out |
| strong | gpt-5.4 | $2.50 / $15.00 | 1050K ctx / 128K out |
| strong | o3 | $2.00 / $8.00 | 200K ctx / 100K out |
| strong | gpt-5.3-codex | $1.75 / $14.00 | 400K ctx / 128K out |
| strong | gpt-5.2-codex | $1.75 / $14.00 | 400K ctx / 128K out |
| strong | gpt-5.1-codex-max | $1.25 / $10.00 | 400K ctx / 128K out |
| strong | gpt-5.1-codex | $1.25 / $10.00 | 400K ctx / 128K out |

#### Auth

You can use OpenAI two ways.

**API key**: set `OPENAI_API_KEY`, or pick the API key method in `/login` and paste a key from [platform.openai.com/api-keys](https://platform.openai.com/api-keys).

**ChatGPT subscription**: use Codex through your ChatGPT login. Run `makima auth login openai-codex`, or pick the ChatGPT login method in `/login`. Makima walks you through the device flow at [auth.openai.com/codex](https://auth.openai.com/codex) and saves the tokens, which win over the API key. `OPENAI_BASE_URL` only redirects the API key path, never the ChatGPT backend.

With a ChatGPT login the model list comes from the Codex backend's own `/models`, so a model your plan gains shows up without a Makima update, with the context window and reasoning levels the backend declares. The table above is the offline fallback. The backend hides models newer than the Codex CLI version Makima reports, so a brand new release can lag until that version is bumped.

### Copilot

- **Id**: `copilot`
- **Env vars**: `GH_COPILOT_TOKEN`, `COPILOT_GITHUB_TOKEN`
- **API**: `https://api.githubcopilot.com`
- **Login**: `makima auth login copilot` (Import GitHub token)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **gpt-5.6-luna** (default) | $0.20 / $1.20 | 200K ctx / 100K out |
| weak | gpt-5-mini | $0.25 / $2.00 | 200K ctx / 100K out |
| weak | gpt-5.4-mini | $0.75 / $4.50 | 200K ctx / 100K out |
| weak | gpt-5.4-nano | $0.20 / $1.25 | 200K ctx / 100K out |
| weak | claude-haiku-4.5 | $1.00 / $5.00 | 200K ctx / 64K out |
| weak | gemini-3.5-flash | $1.50 / $9.00 | 200K ctx / 65K out |
| weak | mai-code-1-flash-picker | $0.75 / $4.50 | 200K ctx / 100K out |
| medium | **gpt-5.6-terra** (default) | $2.00 / $12.00 | 200K ctx / 100K out |
| medium | gemini-3.6-flash | $0.75 / $3.75 | 200K ctx / 65K out |
| medium | gemini-3.7-flash | $0.75 / $3.75 | 200K ctx / 65K out |
| medium | claude-sonnet-4.5 | $3.00 / $15.00 | 200K ctx / 64K out |
| medium | claude-sonnet-5 | $2.00 / $10.00 | 200K ctx / 100K out |
| medium | kimi-k2.7-code | $0.95 / $4.00 | 200K ctx / 100K out |
| medium | gemini-3.1-pro-preview | $2.00 / $12.00 | 200K ctx / 65K out |
| medium | grok-4.5 | $2.00 / $6.00 | 200K ctx / 100K out |
| medium | grok-4.6 | $2.00 / $6.00 | 200K ctx / 100K out |
| medium | claude-sonnet-4.6 | $3.00 / $15.00 | 200K ctx / 64K out |
| strong | **claude-opus-5** (default) | $5.00 / $25.00 | 200K ctx / 64K out |
| strong | gpt-5.5 | $5.00 / $30.00 | 200K ctx / 100K out |
| strong | kimi-k3 | $3.00 / $15.00 | 200K ctx / 100K out |
| strong | gpt-5.4 | $2.50 / $15.00 | 200K ctx / 100K out |
| strong | gpt-5.6-sol | $5.00 / $30.00 | 200K ctx / 100K out |
| strong | gpt-5.3-codex | $1.75 / $14.00 | 200K ctx / 100K out |
| strong | claude-opus-4.8-fast | $10.00 / $50.00 | 200K ctx / 100K out |
| strong | claude-opus-4.8 | $5.00 / $25.00 | 200K ctx / 64K out |
| strong | claude-opus-4.7 | $5.00 / $25.00 | 200K ctx / 64K out |
| strong | claude-opus-4.6 | $5.00 / $25.00 | 200K ctx / 64K out |
| strong | claude-opus-4.5 | $5.00 / $25.00 | 200K ctx / 64K out |
| strong | claude-fable-5 | $10.00 / $50.00 | 200K ctx / 100K out |

`makima auth login copilot` imports a token from the gh CLI, the Copilot client or the system keyring. Each model streams through the API its endpoint names: Messages, Responses or Chat Completions.

### Z.AI

- **Id**: `zai`
- **Env vars**: `ZHIPU_API_KEY`, `ZAI_BASE_URL`
- **API**: `https://api.z.ai/api/paas/v4`
- **Login**: `makima auth login zai` (API key)
- **`plan`** in `providers.toml`: Plan: `standard`, `coding`

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **glm-4.7-flash** (default) | $0.00 / $0.00 | 200K ctx / 131K out |
| weak | glm-5.3-flash | $0.15 / $0.50 | 1000K ctx / 131K out |
| weak | glm-4.5-flash | $0.00 / $0.00 | 131K ctx / 98K out |
| weak | glm-4.5-air | $0.20 / $1.10 | 131K ctx / 98K out |
| medium | **glm-4.7** (default) | $0.60 / $2.20 | 200K ctx / 131K out |
| medium | glm-4.5 | $0.60 / $2.20 | 131K ctx / 98K out |
| medium | glm-4.6 | $0.60 / $2.20 | 200K ctx / 131K out |
| strong | **glm-5-code** (default) | $1.20 / $5.00 | 200K ctx / 131K out |
| strong | glm-5.3 | $1.40 / $4.40 | 1000K ctx / 131K out |
| strong | glm-5.2 | $1.00 / $3.20 | 1000K ctx / 131K out |
| strong | glm-5.1 | $1.00 / $3.20 | 200K ctx / 131K out |
| strong | glm-5 | $1.00 / $3.20 | 200K ctx / 131K out |

### Synthetic

- **Id**: `synthetic`
- **Env vars**: `SYNTHETIC_API_KEY`, `SYNTHETIC_BASE_URL`
- **API**: `https://api.synthetic.new/openai/v1`
- **Login**: `makima auth login synthetic` (API key)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **hf:zai-org/GLM-4.7-Flash** (default) | $0.10 / $0.50 | 200K ctx / 131K out |
| medium | **hf:deepseek-ai/DeepSeek-V3.2** (default) | $0.56 / $1.68 | 200K ctx / 131K out |
| strong | **hf:moonshotai/Kimi-K2.5** (default) | $0.45 / $3.40 | 200K ctx / 131K out |

### DeepSeek

- **Id**: `deepseek`
- **Env vars**: `DEEPSEEK_API_KEY`, `DEEPSEEK_BASE_URL`
- **API**: `https://api.deepseek.com`
- **Login**: `makima auth login deepseek` (API key)
- **Peak pricing**: the prices below are off-peak; each turn is billed as it happens, at 2x during 01:00-04:00, 06:00-10:00 UTC, Mon-Fri

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| medium | **deepseek-v4-flash** (default) | $0.22 / $0.66 | 1000K ctx / 384K out |
| strong | **deepseek-v4-pro** (default) | $0.66 / $1.98 | 1000K ctx / 384K out |

### Regolo

- **Id**: `regolo`
- **Env vars**: `REGOLO_API_KEY`, `REGOLO_BASE_URL`
- **API**: `https://api.regolo.ai/v1`
- **Login**: `makima auth login regolo` (API key)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **qwen3.5-9b** (default) | $0.07 / $0.35 | 80K ctx / 80K out |
| medium | **qwen3-coder-next** (default) | $0.50 / $2.00 | 120K ctx / 120K out |
| strong | **qwen3.5-122b** (default) | $1.00 / $4.20 | 120K ctx / 120K out |

### Google

- **Id**: `google`
- **Env vars**: `GEMINI_API_KEY`, `GOOGLE_BASE_URL`
- **API**: `https://generativelanguage.googleapis.com/v1beta`
- **Login**: `makima auth login google` (API key)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **gemini-2.0-flash-lite** (default) | $0.07 / $0.30 | 1048K ctx / 65K out |
| medium | **gemini-2.5-flash** (default) | $0.15 / $0.60 | 1048K ctx / 65K out |
| strong | **gemini-2.5-pro** (default) | $1.25 / $5.00 | 1048K ctx / 65K out |

### Google Vertex AI

- **Id**: `vertex`
- **Env vars**: `GOOGLE_CLOUD_PROJECT`, `GOOGLE_CLOUD_LOCATION`, `GOOGLE_APPLICATION_CREDENTIALS`
- **API**: `https://aiplatform.googleapis.com`
- **Login**: `makima auth login vertex` (Application Default Credentials)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **gemini-2.5-flash-lite** (default) | $0.10 / $0.40 | 1048K ctx / 65K out |
| weak | gemini-3.1-flash-lite | $0.25 / $1.50 | 1048K ctx / 65K out |
| weak | gemini-3.5-flash-lite | $0.30 / $2.50 | 1048K ctx / 65K out |
| medium | **gemini-2.5-flash** (default) | $0.30 / $2.50 | 1048K ctx / 65K out |
| medium | gemini-3-flash-preview | $0.50 / $3.00 | 1048K ctx / 65K out |
| medium | gemini-3.1-pro-preview | $2.00 / $12.00 | 1048K ctx / 65K out |
| medium | gemini-3.5-flash | $1.50 / $9.00 | 1048K ctx / 65K out |
| medium | gemini-3.6-flash | $0.75 / $3.75 | 1048K ctx / 65K out |
| medium | gemini-3.8-flash | $0.75 / $3.75 | 500K ctx / 65K out |
| medium | gemini-3.7-flash | $0.75 / $3.75 | 1048K ctx / 65K out |
| strong | **gemini-2.5-pro** (default) | $1.25 / $10.00 | 1048K ctx / 65K out |

Authenticates with Application Default Credentials: an `authorized_user` file from `gcloud auth application-default login`, or the metadata server of the machine Makima runs on. Service account files are rejected. The project comes from `GOOGLE_CLOUD_PROJECT` or `project` in `providers.toml`, the location from `GOOGLE_CLOUD_LOCATION` or `location` (default `global`).

### Aperture

- **Id**: `aperture`
- **Env vars**: `APERTURE_HOST`
- **Login**: `makima auth login aperture` (API key)
- **`base_url`** in `providers.toml`: Aperture host URL

No curated models. The provider lists them once it is configured.

Aperture discovers models from your Tailscale Aperture gateway at `APERTURE_HOST` (like `https://your-host.tailnet.ts.net`), or at the `base_url` saved by `makima auth login aperture`. Tailscale identifies you, so no key is needed. Each model is `aperture/<upstream>/<model>`, and a model whose upstream is a built-in provider streams through that provider's own request code.

Overrides keyed by upstream id live under `[aperture.overrides]` in `providers.toml`:

```toml
[aperture.overrides.llmserver]
base = "llama-cpp"
context_window = 131072
max_output_tokens = 16384

[aperture.overrides.llmserver.models."qwen-3.6"]
context_window = 262144
supports_vision = true
```

Upstream fields apply to every model from that upstream; entries under `models` win field by field. Fields: `context_window`, `max_output_tokens`, `supports_thinking`, `supports_vision`, `base` (routes an upstream through a built-in provider, like `llama-cpp`, `google` or `anthropic`) and `path_prefix`. Quote model ids that contain dots, since TOML reads a bare dotted key as nested tables.

Makima sends `/v1` (or `/v1beta` for Gemini routes, nothing for Anthropic and Z.AI) and Aperture appends it to the upstream's base URL. If an upstream base URL already carries its own path, set `path_prefix = ""` for it.

### Mistral

- **Id**: `mistral`
- **Env vars**: `MISTRAL_API_KEY`, `MISTRAL_BASE_URL`
- **API**: `https://api.mistral.ai/v1`
- **Login**: `makima auth login mistral` (API key)
- **`plan`** in `providers.toml`: Plan: `standard`, `coding`

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **ministral-14b-latest** (default) | $0.20 / $0.20 | 262K ctx |
| weak | ministral-14b-2512 | $0.20 / $0.20 | 262K ctx |
| medium | **mistral-small-latest** (default) | $0.15 / $0.60 | 262K ctx |
| medium | mistral-small-2603 | $0.15 / $0.60 | 262K ctx |
| strong | **mistral-medium-latest** (default) | $1.50 / $7.50 | 262K ctx |
| strong | glm-5-2 | $1.40 / $4.40 | 1000K ctx |
| strong | mistral-medium-3.5 | $1.50 / $7.50 | 262K ctx |
| strong | mistral-medium-3-5 | $1.50 / $7.50 | 262K ctx |
| strong | mistral-medium-2604 | $1.50 / $7.50 | 262K ctx |
| strong | zai-glm-5-2 | $1.40 / $4.40 | 1000K ctx |

### OpenRouter

- **Id**: `openrouter`
- **Env vars**: `OPENROUTER_API_KEY`, `OPENROUTER_BASE_URL`
- **API**: `https://openrouter.ai/api/v1`
- **Login**: `makima auth login openrouter` (API key)

No curated models. The provider lists them once it is configured.

OpenRouter serves models from many providers behind one key. Browse them at [openrouter.ai/models](https://openrouter.ai/models) and use any id directly, like `openrouter/anthropic/claude-sonnet-4`.

### Requesty

- **Id**: `requesty`
- **Env vars**: `REQUESTY_API_KEY`, `REQUESTY_BASE_URL`
- **API**: `https://router.requesty.ai/v1`
- **Login**: `makima auth login requesty` (API key)

No curated models. The provider lists them once it is configured.

Requesty routes across many models with one key, including managed routing policies (`requesty/claude-sonnet-4-5`) and the full `<vendor>/<model>` catalog (`requesty/openai/gpt-4o-mini`). Get a key at [app.requesty.ai/api-keys](https://app.requesty.ai/api-keys). Set `REQUESTY_BASE_URL=https://router.eu.requesty.ai/v1` to keep all traffic in the EU.

### TensorX

- **Id**: `tensorx`
- **Env vars**: `TENSORX_API_KEY`, `TENSORX_BASE_URL`
- **API**: `https://api.tensorx.ai/v1`
- **Login**: `makima auth login tensorx` (API key)

No curated models. The provider lists them once it is configured.

### Ollama

- **Id**: `ollama`
- **Env vars**: `OLLAMA_HOST`, `OLLAMA_API_KEY`
- **API**: `http://localhost:11434/v1`
- **Login**: `makima auth login ollama` (API key)
- **`base_url`** in `providers.toml`: Ollama host URL

No curated models. The provider lists them once it is configured.

Talks to the server at the `base_url` saved by `makima auth login ollama`, else at `OLLAMA_HOST`, or to Ollama Cloud when only `OLLAMA_API_KEY` is set. Models come from the server, with the context window and vision support it reports for each.

### LlamaCpp

- **Id**: `llama-cpp`
- **Env vars**: `LLAMA_CPP_HOST`, `LLAMA_CPP_API_KEY`
- **API**: `http://localhost:8080/v1`
- **Login**: `makima auth login llama-cpp` (API key)
- **`base_url`** in `providers.toml`: LlamaCpp host URL

No curated models. The provider lists them once it is configured.

Talks to the server at the `base_url` saved by `makima auth login llama-cpp`, else at `LLAMA_CPP_HOST`. Models come from the server: a router lists many, a single-model server one. The key is optional.

### Claude Agent

- **Id**: `claude-agent`
- **API**: `unix:<state dir>/claude-agent/bridge.sock`
- **Login**: `makima auth login claude-agent` (Claude Code login)

| Tier | Model | Pricing (in/out per 1M tokens) | Limits |
|------|-------|-------------------------------|--------|
| weak | **haiku** (default) |  | 200K ctx |
| medium | **sonnet** (default) |  | 200K ctx |
| strong | **opus** (default) |  | 200K ctx |

Uses your Claude Code login through the Claude Agent SDK. Start the bridge first: `npm install --omit=optional` and then `node src/main.ts` in `claude-agent-bridge/` (Node 24 or later). It runs your installed `claude` with its own tools, settings, hooks, MCP servers and CLAUDE.md files turned off, and offers Claude Maki's tools instead, so Maki's permissions apply to every call. `socket` in the `[claude-agent]` section of `providers.toml` points at a bridge listening elsewhere. Claude Code retries failed requests itself, so Maki does not retry this provider.

### Opencode Zen

- **Id**: `opencode`
- **Env vars**: `OPENCODE_API_KEY`
- **Login**: `makima auth login opencode` (API key)

No curated models. The provider lists them once it is configured.

Models come from the provider's models.dev catalog entry. Free models stay hidden until you turn them on in `~/.config/makima/providers.toml`; they then work without a key through the public key:

```toml
[opencode]
enable_free_models = true
```

Zen and Go both read it from `[opencode]`.

Zen also takes `<catalog provider>/<model>` ids and sends them to that provider. A delegated model reads the catalog provider's API key from its environment variable. Saved credentials are provider-scoped: `makima auth login nvidia`, for example, configures the direct NVIDIA provider and does not configure `opencode/nvidia/<model>`. Set the delegated provider's environment variable separately. It can contain the same API key.

### Opencode Go

- **Id**: `opencode-go`
- **Env vars**: `OPENCODE_API_KEY`
- **Login**: `makima auth login opencode-go` (API key)

No curated models. The provider lists them once it is configured.

Models come from the provider's models.dev catalog entry. Free models stay hidden until you turn them on in `~/.config/makima/providers.toml`; they then work without a key through the public key:

```toml
[opencode]
enable_free_models = true
```

Zen and Go both read it from `[opencode]`.

### Custom provider

- **Id**: `custom`
- **Login**: `makima auth login custom` (Create)

Run `makima auth login custom` to add a provider of your own, or write its section in `providers.toml` (`~/.config/makima/providers.toml` on Linux and macOS, `%APPDATA%\makima\providers.toml` on Windows). A section with a `protocol` is a provider named after the section, used as the `provider/` part of its model specs:

```toml
[my-proxy]
display_name = "My Proxy"
protocol = "openai"            # openai | openai-responses | anthropic | google
base_url = "https://llm.example.com/v1"
api_key_env = "MY_PROXY_API_KEY"
discover_models = true         # also list the server's models

[[my-proxy.models]]
id = "fast-v1"
tier = "weak"
context_window = 128000
max_output_tokens = 16384
pricing_input = 0.5
pricing_output = 1.5
```

| Field | Notes |
|-------|-------|
| `display_name` | Shown in pickers and auth status |
| `protocol` | `openai`, `openai-responses`, `anthropic` or `google` |
| `base_url` | Origin of the API. `<ID>_BASE_URL` wins over it |
| `api_key_env` | Env var that holds the key, by default `<ID>_API_KEY` |
| `api_key` | Inline key (prefer the env var or `makima auth login <id>`) |
| `headers` | Extra headers sent with every request; values expand `${VAR}` |
| `discover_models` | Also list the server's models (default false) |
| `models` | Declared models, which win over discovered metadata field by field |

Model fields: `id` (required), `tier` (`weak`, `medium`, `strong` or `compaction`, default `medium`), `context_window`, `max_output_tokens`, `supports_tool_examples`, `supports_thinking`, `requires_thinking` (for APIs that refuse thinking off; raises it to minimal effort, compaction included), `supports_vision` (false turns off image input and `view_image`), and `pricing_input`, `pricing_output`, `pricing_cache_write`, `pricing_cache_read`, `pricing_fast_input`, `pricing_fast_output` in USD per 1M tokens.

A section never takes over a built-in provider's id. A section that fails to register is logged and left out, and a file that does not parse stops Makima at startup with exit code 2.

#### Executable providers

An executable file in the config `providers/` directory (`~/.config/makima/providers/` on Linux and macOS, `%APPDATA%\makima\providers\` on Windows) is a provider named after the file. Makima runs it with one subcommand and reads JSON from its output:

| Subcommand | Timeout | What it returns |
|------------|---------|-----------------|
| `info` | 5s | `display_name`, `base`, `has_auth` and an optional `system_prefix` |
| `models` | 5s | An optional list of models |
| `resolve` | 30s | Auth for each new agent: `headers` and an optional `base_url` |
| `refresh` | 30s | The same, after the server rejects the credentials |
| `reload` | 30s | The same, after a login elsewhere |
| `login` | none | Only with `has_auth`. Runs on your terminal, so use `makima auth login <name>`; `/login` in the TUI cannot |
| `logout` | 30s | Only with `has_auth`. Runs on `makima auth logout <name>` |

`resolve` runs each time an agent starts, so read tokens from disk instead of caching them, and a login in another process is picked up. `info` and `models` change only with the script, so their output is cached by the file's size and modification time.

`base` names the provider whose protocol the script speaks and whose curated models it inherits: one of `anthropic`, `aperture`, `copilot`, `deepseek`, `google`, `llama-cpp`, `mistral`, `ollama`, `openai`, `opencode`, `openrouter`, `regolo`, `requesty`, `synthetic`, `tensorx`, `zai`. A `models` list replaces them:

```json
[{"id": "my-model-v2", "tier": "strong", "context_window": 200000, "max_output_tokens": 16384}]
```

Only `id` is required. Optional fields: `tier` (default `medium`), `context_window` (default 128K), `max_output_tokens` (default 16K), `pricing` (`{input, output, cache_write, cache_read}` per 1M tokens), and `supports_tool_examples`, `supports_thinking`, `requires_thinking` and `supports_vision`, which default to the base's. A `llama-cpp` based model can also declare `thinking_fields`: the JSON merged into the request body for `off`, `adaptive` and each effort level it accepts.

Script models are `<file name>/<model id>`. The name starts with a letter or digit, continues with letters, digits, `_` and `-`, and cannot take an id that is already registered.

#### Providers from models.dev

Every provider in the [models.dev](https://models.dev) catalog that speaks Chat Completions or Anthropic Messages at a published base URL becomes a provider of its own, under its models.dev id. A built-in provider, a `providers.toml` section or a script with the same id wins. The login picker lists them under "Providers from Models.dev". Each takes its key from the environment variables models.dev names for it, or from `makima auth login <id>`, and shows its models once it has one.

The catalog is cached in the cache directory (`models-dev/`) and fetched again once it is a day old, or by `makima models --refresh`. Startup reads only the cache and never waits on the network, so a provider new to the catalog appears from the next start.

## Model Identifiers

Models are referenced as `provider/model_id`:

```
anthropic/claude-sonnet-5
openai/gpt-5.6-terra
copilot/gpt-5.6-terra
```

### Models newer than your Makima version

The tables above list the models each provider curates. Any other id a provider accepts works too: type it into `/model` or pass it to `--model`. The picker also lists what the provider's own model endpoint reports, so same-day releases are selectable there.

A model's rates, limits and capabilities come from its provider: what the provider's model endpoint reported this session, else its curated row. Each provider decides how a row matches an id; most match dated snapshots of a row by prefix. An id nothing describes gets a 128K context window, no cost estimate, and no thinking, vision or fast mode.

New models start at the **medium** tier until you assign one in the picker.
