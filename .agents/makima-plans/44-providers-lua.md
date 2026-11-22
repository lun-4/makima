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
- A compaction that keeps the prompt cache. Today the summary request strips thinking, images and old tool results and so changes the prefix. Asking for the summary as one user message appended to the unchanged history would let providers reuse their cache; that is a separate redesign of compaction.

## Design

### Trust model

Bundled and user plugins are equal. A plugin that calls `register_provider` owns that provider id. A second registration of an existing id is an error that names both plugins. There are no source proofs, registration origins, endpoint grants or restricted environments. Cloud metadata endpoints for Vertex and Bedrock are ordinary HTTP destinations that the provider code requests directly.

### Plugin layout

`plugins/providers/` is an ordinary bundled plugin. Its `init.lua` registers every built-in provider, the models.dev catalog providers, and providers from `providers.toml` (custom and executable). Shared codecs and helpers live in modules inside the plugin. Rust holds no list of provider ids. The loader has no special case for the plugin.

A single built-in provider cannot be disabled. A provider without credentials or configuration is unused, so a switch adds configuration surface without a current need. The first attempt shipped a `plugins.<provider>.enabled` switch that the loader silently ignored; no such option exists here.

`init.lua` registers in a fixed order: the built-in providers, then `providers.toml` sections with a `protocol`, then executable scripts, then the models.dev catalog providers. A later source never takes an id an earlier one holds, and a user-defined provider that fails to register is logged and left out. The plugin reads `providers.toml` through `maki.api.provider_config(id?)`, and custom provider creation saves through `maki.api.save_provider_config(id, section)`; both were added for this plugin and are general APIs.

The models.dev catalog is cached as an index of providers plus one file per provider. Startup reads only the index and never the network, so a provider new to the catalog registers from the next start. Discovery refreshes the cache once it is a day old or when the user asks, and `opencode.lua` shares the same cache.

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
    aliases = { ... },       -- optional; { id, label, method } login shortcuts
    section = "...",         -- optional; heading in the login picker
    docs = { ... },          -- optional; env, endpoint, notes for docgen
})
```

`aliases` replaces the frontend alias lists (`openai-codex` is an alias of `openai` with the ChatGPT method preset). `section` groups the catalog providers in the login picker. `docs` is described under Docgen.

Rust decodes the table once into a `ProviderSpec` struct with `mlua`'s serde support. Callback fields are kept as `Function` handles. That one decode is the only validation. Docgen renders from the same struct, so docs and runtime cannot disagree. `maki.model.validate` is not added unless a plugin needs it.

A model descriptor carries id, context window, max output tokens, thinking capabilities, vision, fast support, tool examples, pricing and an opaque `provider_data` JSON value. Absent fields stay unknown. The Rust `ModelInfo` type holds this directly; `Model` does not keep a second copy of the same metadata.

A provider instance returned by `prepare` has `stream(ctx, request, emit)` and optionally `fetch_usage`, `refresh_auth`, `reload_auth`, `rotate_key` and `key_count`. State lives in Lua closures.

Every callback receives `ctx = { id, send, credentials }`, where `send` is the registering plugin's `maki.net.send`. A callback that fails returns nil plus an error. A table with `kind = "config"` is a configuration error (missing key, setting or login) and is never retried; the shared helper builds it with `http.config_error`. A table with `status` is an API error, and anything else is a transport error. An API error with `overflow = true` is a context overflow, which the host answers by compacting; the shared `codec.api_error` reads that from the server's refusal, so Rust holds no knowledge of provider error wording. Whether a failure is retried is named by the error itself, see [Retry classes](#retry-classes). The config kind was added during the port, because configuration problems were otherwise retried with backoff, unlike in the native providers.

### Registry and the Provider trait

`ProviderRegistry` maps provider ids to their spec and current model list. It is built after the plugin host loads, and an `Arc` of it is passed explicitly to every place that resolves models, including the callers of `Model::from_spec` today. There is no process-wide registry. Model resolution reads the registry and never calls Lua. Discovery results replace a provider's model list in one write.

With no saved model and no `default_model`, the frontend picks the strong default, else the medium one, of the first provider in `auth_status` order that reports credentials. That order is registration order with sectioned providers last, which replaces the native `PROVIDER_PRIORITY` list.

The existing `Provider` trait stays as the boundary that `maki-agent` and the frontends use. `LuaProvider` in `maki-lua` implements it by sending requests to the Lua runtime thread. Two trait changes are needed: `keys()` returns a borrowed key pool that Lua cannot provide, so it becomes `key_count()` and an async `rotate_key()`; and `adjust_model` is removed because `prepare` returns the adjusted model.

`LuaProvider` uses one generic helper to invoke an instance method and await its result. Each trait method is a call to that helper. Dropping the Rust future cancels the Lua call and closes the event emitter, so late emissions are discarded.

### Invocation identity and conversation lifecycle

`session_id` keeps its current meaning. Every stream call also carries an `Invocation` with `conversation_id`, `operation_id`, `attempt`, `purpose` (agent, compaction or isolated) and an optional `agent_id`.

A `conversation_id` belongs to one agent and outlives a single turn. A forked or child agent gets its own id at once. Sharing an id until the histories diverge is rejected, because concurrent forks could then address and cancel the same remote state. When compaction replaces the history, the old id is ended and a new one starts; the summary request uses a separate fresh id. Rewind starts a new id and ends the old one. A cancelled turn ends the current id and the next turn uses a new one. Isolated turns and compaction summaries each have their own id.

An `operation_id` is generated for each logical request, which is one call of the retry loop. `attempt` starts at 0 and goes up by one per retry. It is set before each stream call, so `(operation_id, attempt)` names one stream attempt.

`end_conversation(conversation_id, reason)` has this contract:

- Only the host calls it. It means the host will never send another request with that id, and every stream for that id has already been dropped. The provider may discard all state for the id. Ids are never reused.
- Providers treat repeated calls, and calls for ids they never saw, as no-ops. The host aims at one call per id but does not guarantee it.
- It is a best-effort notification. It runs detached, so it never delays a cancel, a close or the next turn. Each call is bounded by a 10 second timeout. Errors and timeouts are logged at warn level and are not shown to the user or retried. Delivery at process exit is not guaranteed, so remote services keep their own orphan cleanup.
- The reason is `cancelled` (a turn was cancelled and the agent continues later under a new id), `closed` (the history is discarded for good: `/new`, a finished isolated turn, a finished compaction summary), `compacted` (the history was replaced by a summary) or `rewound` (the history was cut back by rewind).
- Exiting an agent, including at shutdown, does not end its conversation. The id is persisted with the session and a resume continues it, so a remote service relies on its own orphan cleanup for sessions that are never resumed.

In Lua this is an optional instance method `end_conversation(ctx, conversation_id, reason)`. Its return values are ignored, and a raised error is logged like any other failure. Native providers ignore the invocation and do not implement `end_conversation`.

### Conversation lineage

A provider that keeps remote state per conversation, such as the Claude Agent bridge (`claude-agent-bridge-design.md`), needs to know when a new conversation id carries on from an old one. Without that, every cancel, rewind or fork makes it rebuild its remote session from the full history, which loses the remote transcript and its prompt cache.

Every request under a conversation that the host started from another one carries `invocation.previous`:

```lua
previous = {
  id = "...",            -- the conversation this one starts from
  relation = "continues", -- or "branches"
  shared = 12,           -- leading messages equal to the last request sent under `id`
}
```

- `continues`: the same agent goes on after its previous conversation ended. A cancelled turn and a rewind continue.
- `branches`: a second history starts from a copy of another one, which may still be in use. A forked session, an isolated turn and a compaction summary branch from the conversation they copy.
- `shared` counts the leading messages of this request that are equal to the same messages of the last request sent under `previous.id`. The host records the length of the last request it sent under each conversation and takes the smaller of that and the prefix it kept: rewind keeps its cut, a fork and an isolated turn keep the whole copy, a cancelled turn keeps everything. Compaction's summary request rewrites old messages, so it keeps only the leading messages the rewrite left equal, often none. An undercount is always safe, so a host that has not recorded a request yet uses what it has.
- A conversation that never sent a request passes its own `previous` on: its successor names the conversation that did, with `shared` capped by both cuts, and `branches` if either step branched. Ids that never reached the provider are never named.
- `/new`, a plan approval that clears the context, a rewind that keeps no messages, and the conversation that follows a compaction carry no `previous`: their history does not start from the old one. They end the old id as `closed` (`compacted` for compaction).
- Child agents start fresh, with no `previous`.

`previous` is a fixed property of the conversation: every request under the id carries the same value, so a provider that lost its state between two requests can still link them. A provider that already knows the id ignores it. `previous` and the length of the last request are saved with the session together with the conversation id, so a resume or a fork before the next request still links the two. Child agent histories save only their id.

The host calls `end_conversation` for `previous.id` before the first request under the new id, but the call is detached, so that request can reach the provider first. A conversation ended as `cancelled` or `rewound` may still be named by a successor's `previous`, so a provider that wants to continue it keeps its state for a bounded time instead of discarding it at once. A provider without remote state ignores `previous`.

### Retry classes

The provider decides whether a failure is retried; the host only applies budgets. An error table names its class with `retry`:

| `retry` | Budget | Typical source |
|---|---|---|
| `rate_limit` | unbounded with `retry_after`, `max_retries` without | 429 |
| `transient` | unbounded | 5xx, a dropped stream, a malformed stream |
| `connect` | `max_retries` | refused connection, unknown host |
| `timeout` | `max_timeout_retries` | idle read timeout |
| `none` | not retried | everything else |

An error without `retry`, a bare string and a raised Lua error are not retried. An `overflow = true` error and a `kind = "config"` error are never retried whatever `retry` says. Key rotation on 401, 403 and 429 is independent of `retry` and needs a provider with more than one key.

Classification lives in Lua only. `http.classify(err)` is the one classifier: a table with a status gets its class from the status, and a string or a table without a status gets it from the `maki.net.send` failure text (`could not connect` is `connect`, `idle timeout` is `timeout`, anything else is `transient`), which is part of `maki.net.send`'s documented contract. A table that already names `retry` is left alone. `codec.api_error` sets the class from the status, and `init.lua` passes every built-in provider's `stream` errors through `http.classify`, so the bundled providers retry exactly as the native ones did. A user provider that wants retries names `retry` or calls `http.classify`.

`AgentError` carries the class the provider named, and `retry_kind()` returns it.

### Request conversion

The message history is converted from Rust types to Lua tables once per request with `mlua` serde, without a JSON intermediate. The first attempt converted the full history to JSON and then to Lua twice per turn on the shared Lua thread. `cargo bench -p maki-lua --bench provider_perf` measures it, and `provider_wire` times a whole request against a loopback server, at the branch base too. Host conversions run with the Lua memory limit lifted, because mlua guards every allocation with a protected call while a limit is set. On the build box a request with 1,000 messages takes 7 ms end to end, against 2.8 ms for the native providers; the host's conversion is 2.3 ms of it.

### HTTP

`maki.net.send(request)` sends a request and returns a response handle. The handle exposes status and headers, `read()` for the next body chunk with an explicit end-of-stream value, and an idempotent `close()`. Dropping or closing the handle aborts the transfer. Rules:

- One pooled `isahc` client for provider traffic. There is no private-address blocking and no DNS pinning: `send` serves trusted plugin code, and `maki.net.request` keeps its guard for model-supplied URLs.
- No byte cap on streamed bodies. Buffered helpers keep a cap.
- No automatic retry of provider requests. Retry policy is the provider's, in Lua.
- The User-Agent is `maki/<version>`, and the `Expect: 100-continue` fix from `7f75a8bd` applies.
- Redirects are not followed. A 3xx response is returned to the caller.

`send` also takes a `unix_socket` path. A local service can then listen on a socket protected by file permissions, which a loopback TCP port cannot offer.

Stock `isahc` is used. It completes a response at the first body byte or at transfer end, not at the end of the headers. Streaming providers see no difference, because SSE and error responses send body bytes immediately. The transport stays behind the `send` API. Replacing smol and isahc with tokio and reqwest is tracked in #186.

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

In the code `InteractionService` is a single `interact(InteractionRequest)` method over these cases, and `input` also takes a default to edit. One case was added: `run(argv)` hands the terminal to a program until it exits, for executable providers whose login is an interactive script. Only `maki auth login` can hand over its terminal; the TUI fails it with a message pointing there.

The CLI implements it on the terminal and the TUI implements it in the login picker. One `auth_status` function serves `maki auth status` and the TUI, with one error policy: a provider whose status check fails is reported as failed and the listing continues. `maki.host.open(url)` is a general Lua API, and login uses it. `maki.host.keyring_get(service, account)` was added for the Copilot token import, which reads the token the GitHub CLI keeps in the system keyring.

Provider-specific aliases such as `openai-codex` and protocol lists move to the Lua spec, so the frontends contain no provider names.

### Thinking

Rust keeps the provider-neutral `ThinkingConfig`. Each model descriptor declares its thinking capabilities. A shared Lua resolver maps the requested setting to the model's capabilities, and each provider encodes the result. The resolver's result is used as returned, and any adjustment is reported to the frontend.

### Docgen

Docgen starts the real plugin host with no user configuration, credentials or network, reads the registry, and renders from `ProviderSpec`. There is no second Lua loader.

What the page says beyond models, login methods and config fields comes from the spec's optional `docs` table: `env` (the variables the provider reads), `endpoint` and `notes` (Markdown). A provider without `docs` is left off the page, which keeps the dozens of catalog providers out of it; one static section describes them instead.

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
4. Host primitives: streaming `maki.net.send`, crypto primitives, `maki.host.open`.
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
11. Conversation lineage: `invocation.previous` set at every place that starts a conversation from another, saved with the session, a plan approval that clears the context ending as `closed`, and a host test per relation (cancel, rewind, fork, isolated turn, compaction summary, `/new`, a conversation that never sent a request).
12. Retry classes: the `retry` field read by the host, which no longer derives a class; `http.classify` and `codec.api_error` in Lua, applied to every built-in provider's `stream` errors. A host test per class and Lua specs for the classifier.

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
