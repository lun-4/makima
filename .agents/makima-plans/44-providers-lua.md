# Move providers to Lua (#44)

## Goal

The host does not know how to interact with any provider. Every built-in provider is implemented in one bundled Lua plugin, `plugins/providers/`. Rust keeps provider-neutral types, a registry, an adapter from the `Provider` trait to Lua, and generic primitives: streaming HTTP, hashing, credential storage, user interaction and browser opening.

The first attempt is the branch `luna.44-provider-api` at `4b6ac423`. It moved the protocol and auth code to Lua correctly, but added about 100k lines, most of them in Rust host machinery and tests that did not need to exist. A ten-area review of that branch produced the findings listed under [Review findings and their resolution](#review-findings-and-their-resolution). This plan replaces the plans `44-provider-api.md`, `44-provider-api-completion.md` and `45-provider-migration-handoff.md` on that branch, none of which are carried over.

The work lands as one branch with clean commits. The branch as a whole is an atomic replacement: native and Lua providers coexist in intermediate commits, and the native code is gone by the end. Every commit builds and passes `just ci`.

## Size budget

The budget is a review gate, not a hard limit. A commit that exceeds its share needs a reason in its message.

| Area | Budget (added lines) |
|---|---|
| Rust host, non-test | 3k to 4k |
| Rust host tests | 2k |
| Lua providers, codecs and shared helpers | 9k to 10k |
| Lua provider tests | 5k to 6k |
| Docs and plan | 1k |

The native providers in `maki-providers/src/providers/` are 22k lines and are deleted. The expected net change for the branch is close to zero or negative.

## Non-goals

- An HTTP server or OAuth localhost callback. Current OAuth flows use device codes. If a server is added later, it is a general host listener that can also serve a future ACP streamable-HTTP endpoint, as lun-4 requested in the issue thread.
- Isolation between plugins. User plugins already receive every permission by default and can read any file, so bundled provider code gets no special authority and user plugins get no extra restriction.
- Live reload of providers. The registry is built once at startup. A reload rebuilds the plugin host and the registry together.
- Luau static type checking, and tests that compare Luau annotations with Rust types.
- New providers or changes to wire behaviour, except the regressions fixed below.

## Design

### Trust model

Bundled and user plugins are equal. A plugin that calls `register_provider` owns that provider id. A second registration of an existing id is an error that names both plugins. There are no source proofs, registration origins, endpoint grants or restricted environments. Cloud metadata endpoints for Vertex and Bedrock are ordinary HTTP destinations that the provider code requests directly.

### Plugin layout

`plugins/providers/` is an ordinary bundled plugin. Its `init.lua` registers every built-in provider, the models.dev catalog providers, and providers from `providers.toml` (custom and executable). Shared codecs and helpers live in modules inside the plugin. Rust holds no list of provider ids. The loader has no special case for the plugin.

A single built-in provider cannot be disabled. A provider without credentials or configuration is unused, so a switch adds configuration surface without a current need. The first attempt shipped a `plugins.<provider>.enabled` switch that the loader silently ignored; no such option exists here.

`plugins/providers/types.lua` holds Luau type annotations for the provider table, model descriptors and instances, as documentation for plugin authors. Nothing tests it against the Rust types. Generating it from the Rust types is tracked in #47.

### Registration

```lua
maki.api.register_provider({
    id = "synthetic",
    display_name = "Synthetic",
    models = { ... },        -- curated model descriptors
    auth = { ... },          -- login methods: { id, label, login }
    auth_status = fn,        -- optional; callable without credentials
    logout = fn,             -- optional
    discover = fn,           -- optional; async model discovery
    prepare = fn,            -- returns a provider instance for one model
    config = { ... },        -- optional; providers.toml fields and prompts
})
```

Rust decodes the table once into a `ProviderSpec` struct with `mlua`'s serde support. Callback fields are kept as `Function` handles. That one decode is the only validation. Docgen renders from the same struct, so docs and runtime cannot disagree. `maki.model.validate` is not added unless a plugin needs it.

A model descriptor carries id, context window, max output tokens, thinking capabilities, vision, fast support, tool examples, pricing and an opaque `provider_data` JSON value. Absent fields stay unknown. The Rust `ModelInfo` type holds this directly; `Model` does not keep a second copy of the same metadata.

A provider instance returned by `prepare` has `stream(ctx, request, emit)` and optionally `fetch_usage`, `refresh_auth`, `reload_auth`, `rotate_key` and `key_count`. State lives in Lua closures.

### Registry and the Provider trait

`ProviderRegistry` maps provider ids to their spec and current model list. It is built after the plugin host loads, and an `Arc` of it is passed explicitly to every place that resolves models, including the callers of `Model::from_spec` today. There is no process-wide registry. Model resolution reads the registry and never calls Lua. Discovery results replace a provider's model list in one write.

The existing `Provider` trait stays as the boundary that `maki-agent` and the frontends use. `LuaProvider` in `maki-lua` implements it by sending requests to the Lua runtime thread. Two trait changes are needed: `keys()` returns a borrowed key pool that Lua cannot provide, so it becomes `key_count()` and an async `rotate_key()`; and `adjust_model` is removed because `prepare` returns the adjusted model.

`LuaProvider` uses one generic helper to invoke an instance method and await its result. Each trait method is a call to that helper. Dropping the Rust future cancels the Lua call and closes the event emitter, so late emissions are discarded.

### Request conversion

The message history is converted from Rust types to Lua tables once per request with `mlua` serde, without a JSON intermediate. The first attempt converted the full history to JSON and then to Lua twice per turn on the shared Lua thread. The existing benchmark target measures conversion for large histories before the branch is merged.

### HTTP

`ctx:send(request)` sends a request and returns a response handle once the headers arrive. The handle exposes status and headers, `read()` for the next body chunk with an explicit end-of-stream value, and an idempotent `close()`. Cancellation closes the connection. Rules:

- One pooled `isahc` client for provider traffic, with DNS pinning per request.
- No byte cap on streamed bodies. Buffered helpers keep a cap.
- No automatic retry of provider requests. Retry policy is the provider's, in Lua.
- The User-Agent is `maki/<version>`, and the `Expect: 100-continue` fix from `7f75a8bd` applies.
- Redirect validation and credential stripping across authorities are shared with `maki.net.request`.

Stock `isahc` is used. The first attempt vendored a fork to publish responses at headers rather than at the first body byte. The native providers streamed correctly on stock `isahc`, so the first HTTP commit confirms that behaviour with a test. If stock `isahc` turns out to be insufficient, the fix goes upstream and the workspace pins a git revision.

Hash and HMAC primitives (`maki.crypto.sha256`, `maki.crypto.hmac_sha256`) operate on binary strings, for SigV4 in Lua.

### Credentials

`ctx.credentials` reads, writes and deletes credentials for the calling provider's id, using the existing auth file formats in `maki-storage`. `ctx.credentials:refresh(fn)` takes the existing cross-process lock, rereads the stored value, calls `fn` if a refresh is still needed, and writes the result atomically. After the token request is sent, the write runs to completion even if the turn is cancelled, because a server that rotates refresh tokens revokes the token family when the new token is lost. Concurrent refreshes of the same provider share one in-flight call.

### Authentication

One Rust function, `drive_login`, runs the whole login flow for both `maki auth login` and the TUI `/login`. It covers provider and plan selection, config prompts, method choice, the provider's `login` callback, the success check, saving config, and choosing a default model. Config is saved only after a successful login, so a cancelled login leaves nothing behind. The frontends only implement `InteractionService`:

- `input(prompt, secret)`
- `choice(prompt, options)`
- `instructions(text)`
- `device_code(url, code)`
- `open_browser(url)`, which also shows the URL in case the browser does not open

The CLI implements it on the terminal and the TUI implements it in the login picker. One `auth_status` function serves `maki auth status` and the TUI, with one error policy: a provider whose status check fails is reported as failed and the listing continues. `maki.host.open(url)` is a general Lua API, and login uses it.

Provider-specific aliases such as `openai-codex` and protocol lists move to the Lua spec, so the frontends contain no provider names.

### Thinking

Rust keeps the provider-neutral `ThinkingConfig`. Each model descriptor declares its thinking capabilities. A shared Lua resolver maps the requested setting to the model's capabilities, and each provider encodes the result. The resolver's result is used as returned, and any adjustment is reported to the frontend.

### Docgen

Docgen starts the real plugin host with no user configuration, credentials or network, reads the registry, and renders from `ProviderSpec`. There is no second Lua loader.

### Tests

Provider behaviour tests are Lua specs under `plugins/providers/tests/`, run by the existing spec runner with a scripted `ctx` (fake `send`, credentials and clock). Shared codecs are tested once per codec. Provider tests cover what is specific to that provider: request construction, auth precedence, header expansion, discovery, pricing and quota.

Rust tests cover the host only: streaming HTTP against a local server, `ProviderSpec` decoding, `LuaProvider` dispatch and cancellation, credential refresh under contention and cancellation, `drive_login` with a scripted `InteractionService`, and one architecture test that fails if a built-in provider id or endpoint appears in Rust source.

The following test patterns from the first attempt are not used: tests that re-execute themselves as child processes, regular expressions over Lua source, JSON snapshots of provider metadata, tests of the test harness, and `#[cfg(test)]` branches in production paths.

## What is taken from the first attempt

Files are copied from `luna.44-provider-api` at `4b6ac423` with `git show` or `git checkout 4b6ac423 -- <path>` and then adapted. Commits are not cherry-picked.

- Codecs: `chat_completions`, `responses`, `anthropic`, `gemini`, `sse`, Bedrock EventStream and SigV4.
- Auth flows: Codex device flow and refresh, Copilot credential import, Vertex ADC, Bedrock credentials.
- `catalog.lua`, the executable provider adapter, and the per-provider modules.
- Lua codec and provider specs, trimmed to the rules above.
- `maki-lua/src/api/net.rs` from the first attempt as reference for redirect and pinning code. Its structure is not reused.

During the port, credential lookup, `${VAR}` header expansion, Retry-After parsing and error-body reading move into one shared helper module, and the eight OpenAI-compatible providers share one factory. `opencode.lua` uses the catalog cache instead of downloading models.dev itself.

Fixes on the first attempt that are unrelated to providers become separate commits at the start of the branch, after checking that they still apply to `mistress`: the session-lock heartbeat race in child approval, and the test fixes in `66d31803`, `627e3461` and `88f189ae`.

## Commit sequence

1. This plan.
2. Unrelated fixes taken from the first attempt, one commit each.
3. `RefreshProvider` carries its session index, so a login that finishes in an unfocused tab refreshes that tab. This bug predates the branch.
4. Host primitives: streaming `ctx:send`, crypto primitives, `maki.host.open`.
5. Host provider API: `register_provider`, `ProviderSpec`, `ProviderRegistry`, `LuaProvider`, the `Provider` trait changes, and the registry wired into model resolution. Native providers still resolve through `ProviderKind` when the registry has no entry.
6. Host auth: `ctx.credentials`, the refresh transaction, `InteractionService`, `drive_login` and `auth_status`, used by both frontends for registered providers.
7. `plugins/providers/` with the shared helpers, codecs and spec runner.
8. Provider batches, each porting its providers and deleting their native code:
   1. Synthetic and the OpenAI-compatible factory (Mistral, Z.AI, DeepSeek, OpenRouter, Requesty, Regolo, TensorX).
   2. Anthropic and Bedrock.
   3. OpenAI with Codex, and Copilot.
   4. Google and Vertex.
   5. Ollama and llama.cpp.
   6. Aperture, the models.dev catalog and OpenCode.
   7. Custom and executable providers from `providers.toml`.
9. Removal of `ProviderKind`, the static manifests and model tables, `maki-providers/src/providers/`, and dependencies that are no longer used. Docgen moves to the registry. The architecture test is added.
10. Documentation: generated provider and Lua API pages, the hand-written CLI and permissions pages following the tone rules in `AGENTS.md`, and a short section in `AGENTS.md` on adding a provider.

## Review findings and their resolution

| Finding on the first attempt | Resolution |
|---|---|
| 5 MiB cap on every streamed response | HTTP: no cap on streamed bodies; test in commit 4 |
| Fake Chrome User-Agent and lost `Expect` fix on provider traffic | HTTP rules; commit 4 |
| Vendored `isahc` fork, 18k lines | Stock `isahc`; behaviour confirmed by test in commit 4 |
| New TCP and TLS connection per request | One pooled client |
| `${VAR}` header expansion missing in Synthetic and Mistral; Copilot ignores configured headers; TensorX duplicate Content-Type | Shared helper; one spec per provider |
| Optional provider plugins silently ignored by the loader | One plugin, no manifest, no disable switch |
| Provider ids hard-coded in five Rust lists, `opencode` checks, env scanning for `*_API_KEY`, metadata IPs in Rust | Registration in Lua only; architecture test in commit 9 |
| `Protocol` enum and provider-only fields in `ProviderDef`, `ModelFamily`, error-string sniffing per provider | Removed in commit 9; provider config fields come from the spec |
| Source proofs, endpoint grants and sandboxed core environments that a fully permissioned plugin bypasses | Removed by the trust model |
| Reload machinery, publication authority and the latent reload deadlock | No live reload; the registry is built once |
| Provider-call plumbing copied seven times in `runtime.rs` | One generic invoke helper in `LuaProvider` |
| Three validators that disagree, hand-written table walks | One serde decode into `ProviderSpec` |
| Second Lua loader for docgen | Docgen uses the real host |
| Dead code in `maki-providers` (`auth.rs`, discovery cache, test-only variants, unused helpers) | Not recreated; commit 9 deletes the native remainder |
| `Model` holds metadata twice | `ModelInfo` holds it once |
| CLI and TUI login flows written twice; TUI saves config before login | `drive_login`; config saved after success |
| Four `auth_status` implementations with different error policies | One `auth_status` |
| Login in an unfocused tab refreshes the focused tab | Commit 3 |
| `openai-codex` alias and protocol lists in frontends | Moved to the Lua spec |
| Browser opening only inside login | `maki.host.open` |
| Cancellation during OAuth refresh can drop a rotated token | Refresh write completes after the request is sent |
| Full history converted twice per turn through JSON | One serde conversion; benchmark before merge |
| Quadratic string building in Anthropic, Gemini and Responses codecs, and in SSE line splitting | Fixed during the port with buffer tables |
| Thinking resolver result overwritten | Fixed during the port; resolver result used as returned |
| Bedrock frames decoded, re-encoded as SSE and decoded again | The Anthropic decoder accepts decoded events |
| Error classification table copied three times; Retry-After and error-body reading copied per provider | Shared helper |
| Two models.dev loaders | `opencode.lua` uses the catalog cache |
| About 10k lines of provider-specific Rust tests, self-re-executing tests, source regexes, metadata snapshots | Test rules above |
| Provider docs rewritten as internal-contract prose, examples lost | Commit 10 |
| 20 stale plan files and a build-box IP committed | Not carried over |
