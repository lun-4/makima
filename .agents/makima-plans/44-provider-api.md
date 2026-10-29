## Goal

Move every existing provider implementation and provider-specific policy into one bundled Lua `providers` plugin, retaining provider-neutral Rust agent orchestration and generic host primitives. Preserve existing configuration, credentials, model specifications, frontend behavior, and HTTP contracts through a fixture-backed incremental migration.

## Implementation Summary

The dependency direction remains `maki-lua -> maki-agent -> maki-providers`. Keep canonical conversation/result types, generic registry/factory contracts, validation, and accounting in `maki-providers`; implement the Lua adapter in `maki-lua`. Do not rename the crate merely because its concrete implementations disappear.

Bundle `plugins/providers/` with per-provider modules and shared Lua modules for Chat Completions, Responses, Anthropic, Gemini, SSE, auth, thinking conversion, catalogs, and routed/executable compatibility. Rust must contain no built-in provider endpoint, protocol encoder/parser, provider dispatch enum, credential precedence, thinking dialect, or provider-specific quota/pricing rule after cutover. Generic HTTP, crypto, compression, secure storage, locking, retry scheduling, cancellation, and accounting remain Rust responsibilities.

Agreed API shape:

- `maki.api.register_provider({id, display_name, models, auth, discover, prepare, ...metadata})` stages an offline registration. `models` is a curated Lua table; `discover(ctx, config)` asynchronously enriches/adds models; `prepare(ctx, model, config)` returns a resolved model and stateful instance.
- Auth methods carry stable IDs, labels, and `login(ctx, interaction)` callbacks. Provider-level `auth_status(ctx, config)` and `logout(ctx, config)` are callable without preparation or valid credentials. CLI and TUI invoke the same auth service. Login is never implicitly invoked by preparation. Status returns secret-free credential source/method/configured/expiry information, not an assertion of remotely valid access. Logout cancels pending auth/refresh work before removing app-owned credentials and invalidating account caches; it cannot remove environment or external imported credentials. Preserve current generic configuration deletion and executable logout delegation, with provider callbacks supplying any specialized cleanup.
- Instances implement `stream(ctx, request, emit)` and optionally `fetch_usage`, `refresh_auth`, `reload_auth`, and `rotate_key`; `key_count` is captured at preparation. State lives in Lua closures.
- Model descriptors expose thinking modes/levels/budget bounds, vision, fast support, limits, pricing and opaque JSON-compatible `provider_data`. Resolve capabilities per model/route/account, even where Lua populates them from shared provider defaults.
- Use Luau annotations now, but do not add full plugin static typechecking in this issue. Host validation is mandatory; `maki.model.validate` is an optional helper using the same validator.
- Runtime failures use `(value, err)` with structured provider errors. Programmer errors throw. Documentation uses a restricted offline registration loader, not live discovery.

Primary touch points: `maki-providers/src/{provider,manifest,model,model_registry,types,error,pricing,selection}.rs`, `maki-providers/src/providers/`, `maki-config/src/providers.rs`, `maki-storage/src/auth.rs`, `maki-agent/src/agent/{streaming,run}.rs`, provider preparation in agent/headless/actor paths, `maki-lua/src/{runtime,loader,plugin_permissions}.rs`, `maki-lua/src/api/`, `maki-ui/src/components/login_picker.rs`, UI provider installation/usage/discovery paths, `src/cmd/{subcmd,tui,acp}.rs`, print/SDK startup, `maki-docgen/src/gen_providers.rs`, and `tests/frontend_parity.rs`.

Non-goals: a general HTTP server or ACP HTTP transport, new provider products, full Luau analysis, unrelated UI redesign, live-credential CI, or changing known wire quirks without explicit regression cases.

## Implementation Plan

### Phase 1: Freeze observable contracts before migration

1. Extract/extend the fake-upstream pattern in `tests/frontend_parity.rs:1060` into a reusable test harness. Existing binary-level tests at `:155` and `:282` assert actual request thinking/messages/tools. Preserve those assertions. The catalog test `maki-providers/src/providers/catalog.rs:1163` asserts method/path/session header/model but directly constructs a private Rust provider; retain its expectations while changing its preparation entry point later.
2. Build a deterministic HTTP contract harness supporting captured method/path/query/headers/body, gzip/zstd request decoding, scripted statuses/headers/JSON/SSE/binary bodies, multiple sequential requests, explicit chunk release and disconnect signals, bounded shutdown, and injected clock advancement. Never use sleeps or live credentials. Compare JSON semantically, preserve significant ordering, redact fixture secrets, and assert significant headers case-insensitively.
3. Add injectable network endpoint resolution below URL construction for fixed auth/quota/cloud endpoints. Tests must assert the original logical URL/authority while routing to an isolated loopback server; do not add production provider-specific URL override knobs or weaken SSRF policy. Prefer a per-host transport connector/resolver seam shared by old/new adapters. Keep ordinary base-URL override tests separate so URL construction remains covered. Isolate config/env/storage per test.
4. Add a frontend-independent provider driver accepting the normalized request and returning events/final response. Initially back it with existing Rust implementations. Cover every built-in slug and special route with named fixtures before porting it; move existing request-builder/parser expectations into these fixtures where useful. Add a narrow process driver for scripted executable providers and credential refresh races.
5. Build scripted interaction/browser/credential-store/keyring/clock seams. Prove the harness itself with `http_contract_harness_captures_and_releases_chunks`, `auth_harness_advances_clock_and_cancels`, and `logical_endpoint_routing_preserves_request_authority`. Test timeout/cancel/EOF without wall-clock scheduling assumptions.
6. Record current quirks explicitly: Responses omits output caps; some builders use declared rather than turn caps; Anthropic signature-delta and EOF semantics; Google error-envelope limitations; TensorX discovery endpoint behavior. Preserve them in parity fixtures unless changed as a separately named intentional correction. Do not claim that existing behavior proves all advertised semantics.

### Phase 2: Define validated contracts and host-scoped registries

1. Introduce owned descriptor/request/result/error DTOs in `maki-providers`. Convert static borrowed manifest/model fields and `ModelBatch.provider` to owned/shared values; never leak Lua data to obtain static lifetimes. Replace `ModelInfo.provider_info: Arc<dyn Any>` with JSON-compatible provider-owned data. Include all existing quota fields, including `by_model_today`.
2. Preserve normalized history: text, images, tool uses/results, thinking signatures, redacted thinking, tool thought signatures, observation metadata, separate system text, tool examples, session identity, thinking/fast options, declared and per-turn output limits. Events and final result remain separate: events cannot reconstruct tool arguments, signatures, usage or stop reason (`types.rs:459`, `:865`). Define explicit tagged event/error schemas rather than reusing externally tagged serialization accidentally.
3. Validate descriptors at registration, discovery publication and prepared-model return; validate events/results at dispatch. Reject malformed shapes, duplicate IDs/levels, empty identities, nonintegral/nonpositive limits, nonfinite/negative prices, inconsistent thinking profiles and invalid final roles. Preserve unknown versus false/zero. Error paths identify plugin/provider/model/field. Publication is atomic: invalid batches never partially replace good metadata.
4. Expose optional `maki.model.validate` using that validator. Implement the shared exported Luau type module specified below, covering model tables and every hook/context, not just annotating selected examples. All bundled catalogs, registrations and callbacks use those types with explicit parameter/result annotations and `--!strict` where compatible. Do not install or require a static analyzer yet; annotations express the contract, while host validators enforce runtime boundaries. No claim of compile-time guarantees until the analyzer is introduced.
5. Implement a `ProviderRegistry` per plugin host with immutable snapshots and a generic async factory handle. Synchronous model resolution reads one snapshot for an entire operation and never invokes Lua, scripts or network. Thread registry handles through selection, actor rediscovery, completion, pricing, UI and frontend paths; remove process-global provider-host lookup. Preserve exact/dated/prefix/source precedence and user model-policy/tier overrides using normalized data supplied by Lua.
6. Move provider-specific adjustments to preparation/discovery; remove synchronous `Provider::adjust_model`. Replace borrowed `keys()` with synchronous captured `key_count()` and async `rotate_key() -> Result<bool, AgentError>`. Rotation is awaited and cancellation-aware. A reload changing pool cardinality replaces the prepared instance so retry counts remain coherent.

### Shared Luau interface contract: Model and hooks

Add a canonical importable type module under `plugins/lib/maki/provider_types.lua`. Runtime may return an empty frozen table; exported type aliases are its purpose. Providers use `local Types = require("maki.provider_types")`, then `type Model = Types.Model`, etc. Do not duplicate structural aliases per provider. This module is shipped and exercised by the restricted docgen loader too.

Use explicit tagged unions for thinking preferences, thinking capability variants, content blocks, events, errors and auth outcomes. Do not use `any` to hide an incomplete boundary. Define recursive `JsonValue` for provider-private metadata and tool inputs, distinguish absent versus JSON null, and retain typed named records for fields the host interprets. Derive/maintain the aliases alongside Rust DTOs with schema-key/discriminant parity tests, including optional fields; annotations are not the runtime schema.

The core public shape is below. Supporting types named here must also be fully defined in the module, with typed methods on all contexts (no untyped ctx/ui bags). Exact aliases for existing monetary/token fields must preserve current units and ranges; Rust validators enforce integers/finiteness beyond Luau's number type.

```lua
export type Effort = "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
export type ThinkingPreference =
    { mode: "off" }
    | { mode: "adaptive" }
    | { mode: "effort", level: Effort }
    | { mode: "budget", tokens: number }

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
}

export type Emit = (event: ProviderEvent) -> (boolean?, ProviderError?)
export type Stream = (
    ctx: ProviderContext, request: ProviderRequest, emit: Emit
) -> (ProviderResponse?, ProviderError?)
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

export type Login = (
    ctx: AuthContext, ui: AuthInteraction
) -> (boolean?, ProviderError?)
export type AuthMethod = { id: string, label: string, login: Login }
export type AuthStatusHook = (
    ctx: AuthContext, config: ProviderConfig
) -> (AuthStatus?, ProviderError?)
export type Logout = (
    ctx: AuthContext, config: ProviderConfig
) -> (boolean?, ProviderError?)
export type Discover = (
    ctx: ProviderContext, config: ProviderConfig
) -> ({ Model }?, ProviderError?)
export type Prepare = (
    ctx: ProviderContext, model: Model, config: ProviderConfig
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
}
```

Define descriptor extensions for existing offline prefix aliases/default/tier/pricing-schedule/provider-config-schema/docs fields explicitly rather than losing them because the compact core omits them. Separate `Model` descriptor from typed prepared-request model fields (provider identity, effective output limits and resolved capabilities) where necessary; do not require those runtime fields in curated catalogs. ProviderConfig is the typed compatibility configuration record plus explicitly JSON-valued provider extensions, not an unrestricted arbitrary table. AuthContext and ProviderContext expose scoped, typed host services; AuthInteraction has typed input/choice/instructions/device-challenge/browser operations. Refresh credential callbacks and transactions get named generic signatures so provider-specific credential records remain typed without entering host event metadata.

Success pairs: boolean false from rotation means no alternate key and is not an error; ordinary successful no-value operations return true. Emit returns a pair so revoked/closed sinks are explicit; every codec must propagate a failed emission. Errors carry a tagged kind and applicable status/retry/overflow fields. Document invariants of optional pair values; Luau aliases alone cannot prove mutual exclusivity.

Example annotated usage (helpers are typed imports, not implicit globals):

```lua
local Types = require("maki.provider_types")
type Model = Types.Model
type Provider = Types.Provider
type Prepare = Types.Prepare

local models: { Model } = require("synthetic.models")
local prepare: Prepare = prepare_synthetic
local provider: Provider = {
    id = "synthetic",
    display_name = "Synthetic",
    models = models,
    prepare = prepare,
}
maki.api.register_provider(provider)
```

Add annotations to both callback definitions and exported records; do not leave untyped hook parameters in the final implementation just because earlier shape examples are abbreviated. Add `provider_luau_type_surface_complete` to statically assert exported aliases/hook annotations and runtime/DTO schema-key correspondence, plus `annotated_provider_contract_examples_load` to load annotated model/login/discover/prepare/stream/refresh/logout examples in real Luau. These are contract-presence/parity and execution checks, not a replacement or claim of full Luau type analysis. Future analyzer adoption should require no redesign of these public types.

### Phase 3: Generic host I/O, credential and interaction APIs

1. Add a low-level HTTP response-handle API (final name `maki.net.send`) returning status/headers before body consumption, bounded raw-byte `read()` with explicit EOF, and idempotent `close()`. Keep existing buffered `maki.net.request` behavior compatible. Lua helpers implement bounded JSON reads and SSE parsing over handles.
2. Share existing DNS pinning, redirect validation and cross-authority credential stripping. Use no transport retries for provider requests, preserving connect/idle/overall timeout distinctions and long inference streams. Cancel/drop/revocation closes bodies and connections. Enforce one active reader and bounded buffering.
3. Mint internal endpoint capabilities only from a host-verified embedded provider source at bootstrap. Bind them to source identity, instance and generation, with narrowly enumerated destination/purpose grants. Neither manifest permissions (even holds_all), provider slug, user-supplied IDs nor external plugin registration can mint them. Check generation on every use and revoke on unload. Metadata grants cannot be used to send unrelated provider credentials. Do not silently inherit web-fetch HTTPS upgrading or a blanket private-network bypass. Support user-configured local/provider endpoints over HTTP through narrowly scoped provider endpoint grants. Cloud metadata access for Vertex/Bedrock uses explicit internal endpoint capabilities available only to the trusted provider code requesting them; untrusted plugin network policy remains unchanged. Test both allowed local providers and denied arbitrary private destinations, including redirect/rebinding attempts.
4. Expose only missing generic primitives for hash/HMAC, encoding, request gzip/zstd compression, secure random, URL/form encoding, time parsing and cancellable waits. Lua owns SigV4, binary EventStream interpretation, endpoint compression eligibility and protocol framing. Reuse existing workspace dependencies before adding any.
   - For SigV4, expose `maki.crypto.sha256(bytes)` and `maki.crypto.hmac_sha256(key, bytes)` returning raw digest bytes as binary-safe Lua strings. These deterministic primitives need no runtime-error pair for valid byte-string arguments; wrong argument types are programmer errors. Use mlua byte-string access, never UTF-8 conversion. Keep hexadecimal encoding in a shared Lua helper, not a new crypto package.
   - No new third-party packages are needed for SigV4: workspace `sha2` and `hmac` already exist (`Cargo.toml:113–114`). Add workspace dependencies to `maki-lua`, then remove their `maki-providers` dependency entries when its signing code is deleted. Do not introduce an AWS SDK or host SigV4 API.
   - Lua derives the signing key via HMAC over date, region, service and `aws4_request`, hashes the payload/canonical request, and constructs the Authorization header. Serialize the body once and sign/send exactly those bytes; prohibit subsequent implicit body compression or path/header rewriting that changes signed material. Compression, if selected, must happen before signing.
   - Reuse existing Bedrock signing fixtures, particularly the double-encoded model path at `maki-providers/src/providers/anthropic/bedrock.rs:762`. Add `crypto_sha256_known_answers`, `crypto_hmac_sha256_known_answers`, `crypto_binary_strings_roundtrip` (including NUL/non-UTF8 and chained HMAC keys), and `sigv4_signs_exact_sent_bytes` to cover both primitives and the real Lua-to-HTTP boundary.
5. Expose scoped credential reads/writes/deletes and a cancellation-safe refresh transaction that locks, reloads after lock acquisition, invokes Lua refresh if needed, and persists atomically with existing permissions. Preserve existing auth-file schemas initially; avoid a new mandatory envelope migration. Failed persistence must not masquerade as logout. Use shared keyed single-flight for refresh and distinguish transient failure from revoked credentials.
6. Provide permission-controlled generic keyring access/import sources and reuse fs/subprocess APIs for Copilot, ADC, AWS and executable providers. Keep provider-specific file locations, precedence and keyring entry naming in Lua. Provider diagnostics and interaction events must never include secrets.
7. Implement generic interaction operations for input (secret/optional), choice, instructions, device challenge and browser opening with visible URL fallback. Interactive executable login uses an explicit terminal-handoff capability in CLI; TUI reports/invokes the same supported compatibility flow without unsafe inherited terminal access. Cancellation is a distinct terminal result, never success, and stale attempts cannot commit credentials.
8. Do not add a server: current Codex login is device flow. Future callback support must build on generic constrained listener primitives rather than coupling MCP or ACP protocols to provider auth.

### Phase 4: Lua dispatch, bootstrap and lifecycle

1. Implement `LuaProvider` and factory in `maki-lua`, holding runtime sender, instance ID and plugin generation, never calling mlua from agent/network threads. Dispatch each operation as a scoped yielding task. Support concurrent sessions and reentrant Lua child-agent calls.
2. Extend transactional plugin registration with provider ownership. Reject duplicate slugs deterministically, preserve old registrations on failed replacement, revoke old sinks/callbacks and cancel work on unload/replacement. Keep core providers available independently of optional tool-plugin disabling. Allow third-party providers with new IDs, not silent core overrides.
3. Dropping a Rust operation future immediately closes its event gate and signals cancellation; late Lua emissions are rejected, pending replies complete on shutdown, and streams release resources. No detached provider task may outlive generation authority.
4. Bootstrap each TUI/print/SDK/ACP/auth/models command by allocating registry, spawning Lua, registering bundled providers offline, loading configuration/custom/executable definitions, completing registration barrier, then resolving/ preparing models and starting discovery. Authentication-only commands use this same provider registry rather than a separate dispatch table.
5. Empty/no-credential state remains usable for login/configuration without inventing an Anthropic/default concrete provider. Full host reload builds a new host/registry and prepares replacements before atomic session installation; old handles remain tied to their old VM until cancelled/drained. Do not retarget adapters across VMs.
6. Integrate usage provider/auth identity invalidation with existing `maki-ui/src/provider_usage.rs` and Lua usage plugin. Auth mutation, instance replacement and account discovery must invalidate stale quota/fast eligibility before publication.

### Phase 5: Migrate Synthetic and OpenAI/Codex as proving cases

1. Build shared Lua Chat codec: full existing message/tool/image conversion, reasoning variants, fragmented tool arguments, stable generated missing IDs, limits on accumulator indexes, usage/cache normalization, SSE errors and final-response construction. SSE parsing handles byte/UTF-8 boundaries, CRLF, multiline data, comments and termination.
2. Port Synthetic offline models, API-key precedence/pool/headers, standard effort mapping, model discovery, and quota fetching. Preserve v3 rolling-lane priority over legacy subscription, remaining-to-used conversion, rounding/clamping, optional reset timestamps and empty-recognized-response errors (`synthetic.rs:232`).
3. Port Responses codec and OpenAI platform dispatch, keeping provider slug `openai` and auth methods `api_key`/`chatgpt`; `openai-codex` remains an existing frontend compatibility alias, not a new model slug. Preserve current protocol choice for known/discovered plan models versus other Chat models.
4. Port Codex JSON device request/poll/code-verifier exchange, account extraction, refresh, endpoint/header precedence, usage and account-scoped model discovery. Preserve server poll intervals; cancellation never saves or reports success. Share CLI/TUI workflow. Bind fast eligibility and discovered capabilities to account/auth generation and invalidate on credentials change. Auth resends must be explicitly gated before any delivered events.
5. Run identical upstream contracts against Rust and Lua drivers while both exist. Keep binary frontend-parity tests running against normal provider preparation; do not substitute a canned provider at that boundary.

### Phase 6: Migrate the remaining provider shapes

Use the same contracts; retain provider-specific logic in Lua composition, not host slug branches.

- Anthropic: cached system/tools/message blocks, beta/fast/long-context policy, adaptive/budget thinking, signatures/redacted thinking and ordered final blocks.
- Bedrock: all existing bearer/skip/env/container/profile modes, credential expiry, SigV4 over exact serialized bytes, body/URL differences and binary EventStream exception mapping, reusing Anthropic event logic.
- Google/Vertex: name-based function result lookup, schema transforms, images, signatures, usage; Vertex authorized-user/metadata ADC, token caching, project/location and pre-stream 401 refresh.
- Copilot: credential imports and enterprise hosts, GraphQL endpoint discovery/cache invalidation, per-model Messages/Responses/Chat choice, discovered reasoning and AI-credit pricing.
- Aperture: gateway overrides, per-route protocol composition, URL prefixes and wire IDs, inherited model adjustments and generic fallback behavior.
- Mistral: structured thinking, high-only effort, affinity and discovery filters. Z.AI: GLM mapping, quota and billing-error normalization. DeepSeek: toggles/reasoning padding/R1 exception and balance. OpenRouter: reasoning metadata/cache/session/attribution. Requesty: parallel managed/full discovery with partial success and pricing. Regolo: metadata joins and daily activity/per-model usage. TensorX: discovered parameter flags, thinking templates, unknown output maximum.
- Ollama/llama.cpp: optional credentials, host handling, cloud fallback only where existing; Chat/Responses selection and progress; Ollama per-model shows and llama.cpp props/router/single/legacy modes.
- OpenCode/catalog: models.dev cache/TTL, arbitrary supported catalog slugs, protocol/auth selection, free gating/public fallback, prefix/session behavior and Go usage.
- Custom TOML/executable providers: preserve protocols, declared/discovered metadata precedence, scripts' info/models/resolve/login/logout/refresh/reload, system prefixes, shared refresh generations, missing-refreshed-URL preservation and no auth replay after events.

### Provider shape catalog and implementation examples

These are composition patterns inside the single bundled Lua plugin, not a Rust enum or a requirement that every provider use a configurable mega-factory. Only registration and instance contracts cross the host boundary. Local helper names below illustrate responsibilities; implement ordinary Lua modules with typed signatures, explicit `(value, err)` propagation and lifecycle/cancellation semantics from preceding phases. The snippets omit unrelated registration fields for readability. They are design examples, not complete executable implementations.

#### Shape A: Shared Chat codec plus provider policy

Use a stateful key pool and shared Chat codec; provider code constructs or transforms the body and interprets discovery/quota responses. This is the simplest starting point, but shared streaming does not imply identical auth, discovery or quotas.

```lua
local chat = require("protocols.chat_completions")
local thinking = require("thinking")
local models = require("synthetic.models")

local function prepare(ctx, model: Model, config): (ProviderInstance?, ProviderError?)
    local auth, err = resolve_api_keys(ctx, config)
    if not auth then return nil, err end

    return {
        model = model,
        key_count = auth:key_count(),
        rotate_key = function() return auth:rotate() end,
        reload_auth = function(ctx) return auth:reload(ctx) end,
        fetch_usage = function(ctx) return quotas.fetch(ctx, auth:snapshot()) end,
        stream = function(ctx, request: ProviderRequest, emit)
            local resolved, resolve_err = thinking.resolve(request.options.thinking, model.thinking)
            if not resolved then return nil, resolve_err end
            local body = chat.build_request(request)
            encode_synthetic_thinking(body, resolved.effective)
            return chat.stream(ctx, {
                endpoint = resolve_chat_endpoint(config),
                headers = auth:snapshot_headers(),
                body = body,
            }, emit)
        end,
    }, nil
end

maki.api.register_provider({
    id = "synthetic",
    display_name = "Synthetic",
    models = models,
    auth = api_key_methods,
    auth_status = api_key_status,
    logout = api_key_logout,
    discover = discover_synthetic_models,
    prepare = prepare,
})
```

`Model`, `ProviderInstance`, `ProviderRequest` and `ProviderError` refer to imported exported Luau types. Final implementation provides concrete types for other parameters too; full static analysis is deferred, runtime validation is not. `model.thinking` is already normalized from curated/default/discovered capability data. The call retains requested/effective metadata for frontend reporting even though the example shows only body encoding.

Mapping: Synthetic is the reference implementation. Regolo shares its standard-effort Chat stream shape but not its discovery/usage logic. Mistral, Z.AI, DeepSeek, OpenRouter, Requesty and TensorX are variants of this shape with nontrivial model/body/error/discovery policy. Keep those functions in named provider modules rather than stuffing provider-name switches into `chat`.

#### Shape B: Native protocol implementation

A provider owns its native wire vocabulary and composes reusable protocol functions. It is not forced through Chat messages or effort strings.

```lua
local messages = require("protocols.anthropic")

local function stream(ctx, request: ProviderRequest, emit)
    local body = messages.build_request(request)
    encode_anthropic_thinking(body, request)
    local headers = anthropic_headers(auth:snapshot(), request.model)
    return messages.stream(ctx, {
        endpoint = endpoint,
        headers = headers,
        body = body,
    }, emit)
end
```

Mapping: Anthropic direct uses Messages; Google direct uses Gemini. Both own schema/history/signature/usage behavior in their Lua protocol modules. Anthropic's offline model profiles distinguish adaptive from legacy budget encoding; Google's conversion must resolve tool-result names from earlier tool uses and preserve thought signatures. These are two implementations of the pattern, not a single universal native JSON encoder.

#### Shape C: Stateful authenticated provider with per-model protocol choice

Authentication, account-scoped discovery and routing make preparation more than a static URL/header resolver. Keep the account cache tied to credential generation. Login methods are independent of preparation.

```lua
local function prepare(ctx, model: Model, config)
    local account, err = resolve_openai_account(ctx, config)
    if not account then return nil, err end
    local route = choose_openai_route(model, account)

    return {
        model = model,
        refresh_auth = function(ctx) return account:refresh(ctx) end,
        reload_auth = function(ctx) return account:reload(ctx) end,
        fetch_usage = function(ctx) return account:fetch_usage(ctx) end,
        stream = function(ctx, request, emit)
            return route.stream(ctx, account, request, emit)
        end,
    }, nil
end

maki.api.register_provider({
    id = "openai",
    models = require("openai.models"),
    auth = {
        { id = "api_key", label = "API key", login = login_api_key },
        { id = "chatgpt", label = "ChatGPT login", login = codex_auth.login },
    },
    auth_status = openai_auth_status,
    logout = openai_logout,
    discover = discover_openai_models,
    prepare = prepare,
})
```

`route.stream` handles its body/header policy and invokes shared Responses or Chat codecs. Preserve current known/discovered-plan-model selection; do not assume auth method alone determines protocol. The account wrapper invalidates/reprepares routing/capabilities on relevant changes; a captured route cannot silently survive an account transition. API-key instances additionally expose rotation where the existing implementation supports it.

Mapping: OpenAI platform/Codex is the reference case. Copilot is a specialized member: imports credentials, discovers its GraphQL API endpoint and chooses Messages/Responses/Chat from each model's supported endpoints, with Copilot headers and reasoning/pricing interpretation. Do not shoehorn its login/import into an OAuth-only account helper.

#### Shape D: Credential/transport specialization around a native codec

Native content can be reused even when request framing, signing or credentials are completely different. These providers deserve dedicated modules, not another Chat configuration record.

```lua
local function stream_bedrock(ctx, request, emit)
    local credentials, err = aws_credentials:current(ctx)
    if not credentials then return nil, err end
    local body = anthropic.build_bedrock_request(request)
    local wire = encode_json(body)
    local signed, sign_err = sign_bedrock_request(credentials, request.model, wire)
    if not signed then return nil, sign_err end
    local response, send_err = http.send(ctx, signed)
    if not response then return nil, send_err end
    return bedrock_events.consume(response, anthropic.new_accumulator(), emit)
end
```

This is the SigV4 branch; Bedrock bearer/skip-auth branches use the same body/event path without signing. Lua signs exact serialized bytes and decodes AWS binary EventStream into Anthropic events. Rust provides raw transport, hashing and HMAC, not Bedrock understanding.

Mapping: Bedrock is the specialized binary/signed case under the existing Anthropic selection, not a new user-visible provider slug. Vertex is a separate specialization around Gemini: ADC/user/metadata credentials, expiring bearer token cache, project/location/quota headers, uppercase schema types and pre-stream 401 refresh. Vertex does not need Bedrock frame or signing helpers.

#### Shape E: Local endpoint with rich discovery

Streaming can reuse a shared codec while local discovery and configuration remain specialized. No interactive login is required when a host is already configured and auth is optional.

```lua
local function discover_llama(ctx, config)
    local endpoint, err = resolve_local_endpoint(ctx, config)
    if not endpoint then return nil, err end
    return llama_models.discover(ctx, endpoint)
end

local function prepare_llama(ctx, model, config)
    local endpoint, err = resolve_local_endpoint(ctx, config)
    if not endpoint then return nil, err end
    local codec = choose_local_protocol(config)
    return {
        model = model,
        stream = function(ctx, request, emit)
            return codec.stream_local(ctx, endpoint, request, emit)
        end,
    }, nil
end
```

Mapping: llama.cpp uses props/models router/single/legacy discovery and local thinking fields; Ollama uses parallel show queries, optional cloud fallback and its distinct thinking behavior. Both may select Chat or Responses/progress; that choice stays in Lua and must not erase their differences. Endpoint grants remain host-validated.

#### Shape F: Composite gateway/delegating provider

A gateway routes one public model identity into a native implementation with gateway auth and path/model overrides. Delegation is local Lua composition, not registering fake child providers with the host or recursively obtaining another provider's credentials.

```lua
local function stream_aperture(ctx, request, emit)
    local route = resolve_gateway_route(request.model, overrides)
    local delegated = rewrite_gateway_request(request, route)
    local endpoint = gateway_endpoint(route, gateway_auth:snapshot())
    return route.implementation.stream_with_endpoint(ctx, delegated, endpoint, emit)
end
```

Mapping: Aperture is the primary composite case. It supports native route inheritance, vendor-prefixed model IDs with Google's bare-ID exception, explicit base/path overrides and a generic fallback that retains its current behavior. OpenCode can also delegate to native Lua implementations, but its catalog/free-model policy belongs to Shape G; use both patterns rather than inventing a universal gateway class.

#### Shape G: Metadata/config-driven provider factory

A factory converts catalog/config records into concrete descriptors/instances using shared codecs. The factory is Lua and can create arbitrary provider slugs. Rust only validates resulting registrations and normalized metadata.

```lua
local function instance_from_definition(ctx, model, definition)
    local auth, err = resolve_definition_auth(ctx, definition)
    if not auth then return nil, err end
    local implementation = protocols[definition.protocol]
    return implementation.prepare(ctx, model, definition, auth)
end
```

Mapping: custom TOML supports Chat, Responses, Anthropic and Google with explicit declared/discovered precedence. models.dev catalog providers select supported package/auth families and preserve unknown fields, cache TTL and model gating. OpenCode Zen/Go adds public fallback/free gating, native delegation/model prefixes, session headers and Go-only quota policy. Catalog or TOML protocol values are Lua factory inputs, not a fixed host dispatch enum. Keep their existing serialized config spellings compatible.

#### Shape H: Executable-provider compatibility adapter

This is intentionally specialized: the external process supplies credentials/metadata while an existing native Lua implementation handles generation. It is not a new LLM wire protocol.

```lua
local function prepare_script(ctx, model, config)
    local script = executable_provider(config)
    local resolved, err = script:resolve(ctx)
    if not resolved then return nil, err end
    return prepare_with_script_auth(ctx, model, {
        implementation = resolve_base_implementation(config),
        credentials = resolved,
        refresh = function(ctx) return script:refresh(ctx) end,
        reload = function(ctx) return script:reload(ctx) end,
    })
end
```

Mapping: every existing executable/dynamic provider uses this adapter. Preserve info/models/resolve/login/logout/refresh/reload contracts, interactive terminal ownership, system prefix, cross-instance single-flight and missing-refreshed-URL semantics. Refresh-and-replay is gated before any output and cannot be reduced to an unconditional codec retry.

#### Complete ownership map

| Existing provider/variant | Primary shape | Required specialization rather than generic defaults |
|---|---|---|
| Synthetic | A | Legacy/v3/weekly quota parsing |
| Regolo | A | Model/group joins, key/activity/daily per-model usage |
| Mistral | A | Structured reasoning, high-only effort, affinity/filtering |
| Z.AI | A | GLM thinking, billing classification and quotas |
| DeepSeek | A | Toggle/padding/R1 exception, balance currencies |
| OpenRouter | A | Discovery-driven effort requirements, caching/session/attribution |
| Requesty | A | Managed/full catalog join, partial failure and auto-cache |
| TensorX | A | Supported-parameter flags/template fields and output-cap exception |
| Anthropic direct | B | Cache/beta/long-context/fast and signed thinking |
| Google direct | B | Gemini tool/schema/signature conversion |
| OpenAI API and Codex login mode | C | Chat/Responses selection, device refresh/account-bound discovery |
| Copilot | C | GraphQL discovery/import plus three protocols |
| Anthropic Bedrock mode | D | AWS credentials/SigV4/binary framing |
| Vertex | D | ADC/project/location/token cache around Gemini |
| Ollama | E | show discovery/cloud fallback/distinct thinking |
| llama.cpp | E | props/router/single/legacy discovery/local thinking |
| Aperture | F | Route inheritance, auth/path/model overrides |
| OpenCode Zen/Go | G + F | Catalog/free/public policy plus native delegation/session/Go quotas |
| Arbitrary supported catalog providers | G | Package-specific protocol/auth and models.dev metadata |
| Custom TOML providers | G | Four codecs, explicit declarations and optional discovery |
| Executable/dynamic providers | H + chosen base | Script lifecycle and safe credential refresh delegation |

These are all current built-in ProviderKind entries plus catalog/custom/dynamic families and Bedrock mode. The fixture inventory below must test the distinctions, not merely one representative per shape. Add `provider_shape_inventory_complete` to compare every bundled registration/family and specialized mode with this catalog's executable fixture inventory; shape membership does not excuse missing provider-specific fixtures. Use examples to guide module boundaries, not as an extra public inheritance hierarchy.

### Required fixture inventory and migration gate

Maintain an executable fixture manifest keyed by provider/route/operation. `provider_fixture_inventory_complete` compares registrations and the required cases below against implemented fixture IDs; a provider cannot lose its Rust implementation while any required case lacks passing Rust/Lua parity (or a separately recorded intentional-correction expectation). Security/new API correctness tests are additional, not replacements for parity.

| Provider/group | Required route/auth/discovery/usage cases |
|---|---|
| Synthetic | env/saved/config key precedence and rotation/header overrides; Chat thinking Off/Adaptive/all levels; generic model listing; quota v2, v3 precedence, weekly, invalid limits/timestamps and empty response |
| OpenAI/Codex | API-key Chat and Responses; OAuth device pending/success/denied/timeout/cancel/exchange; OAuth precedence/account header/fixed backend versus API base override; expiry/401 refresh and failure; account-scoped model efforts/fast eligibility/static fallback; quota supported OAuth versus unsupported API key; logout/status for both methods |
| Anthropic | env/saved keys/header override/rotation; direct Messages cache/beta/long-context/fast; legacy budget versus adaptive thinking; signatures/redacted blocks; OAuth-eligible quota and unsupported key usage |
| Bedrock | env SigV4, profile credentials, container expiry refresh/auth header, bearer, skip-auth gateway; body/URL/signing; EventStream chunks and 429/400/500 exceptions |
| Google/Vertex | Gemini key and signature/tool/image conversion; Vertex authorized_user ADC refresh, metadata token, rejected service-account file, project/location/quota headers and 401 retry; discovery success/failure |
| Copilot | env/saved/file/gh/keyring import precedence and enterprise hosts; GraphQL success/fallback; discovered Messages/Responses/Chat routes and fallback endpoint guessing; filtering/pricing/effort; reload invalidation; unsupported usage; logout/status |
| Aperture | native Anthropic/Google/OpenAI-compatible routes and generic fallback; provider/model override precedence; default/custom path prefixes; prefixed/bare wire IDs; model adjustment and discovery pricing |
| Mistral/Zai/DeepSeek | each Chat request plus keys/rotation; Mistral structured thinking/affinity/nonthinking model filter; Zai billing reclassification/quota; DeepSeek reasoning padding/R1 exception/balance currencies |
| OpenRouter/Requesty | OpenRouter required/default reasoning/cache/attribution/session/pricing; Requesty managed/full success, each partial failure, both failure, duplicate precedence/unknown limits/pricing/auto-cache |
| Regolo/TensorX | Regolo metadata join failure fallback, key info/activity/daily by-model usage; TensorX parameter flags/template fallback/no output cap/static discovery URL parity |
| Ollama/llama.cpp | local no-key/key/header/host paths; Chat and Responses/progress; Ollama cloud fallback and per-model show partial failure; llama props router/single/legacy modes/vision/filter/local thinking |
| OpenCode/catalog | OpenCode Zen/Go routing/prefix/session; public fallback free enabled/disabled/paid filtering; models.dev Chat and Anthropic families/cache TTL; Go quota versus unsupported other slugs |
| Custom/executable | all four custom protocols, declared-versus-discovered overrides/discovery off; executable info/models/resolve/login/logout/refresh/reload success/error/timeout; system prefix, refresh single-flight, absent refreshed URL, cancellation and no auth replay after events |

All stream routes additionally run shared text/thinking/tool/signature/usage/stop/error/chunk-split cases applicable to their codec. All auth sources run status/logout without constructing a provider; use `auth_status_logout_contract` for API-key, OAuth and executable methods, including env/external-source visibility, no refresh resurrection after logout and documented config-removal semantics. Add `internal_endpoint_capability_cannot_be_spoofed` with fully permissioned third-party plugins, spoofed core IDs and revoked generations.

### Phase 7: Thinking capabilities and encoding

1. Publish thinking choices on every resolved model from curated definitions plus genuine discovery. Shared provider defaults are internal Lua data; do not infer model-specific levels from a plain model list. Preserve current Synthetic levels minimal/low/medium/high and its adaptive-to-medium mapping.
2. Share a pure Lua resolver taking requested intent, model profile and constraints, returning effective mode plus exact/adjusted/unsupported metadata. Preserve Off, Adaptive, effort and explicit budget as distinct intents. Snap once using the existing ordering (supported value, else greatest lower, else minimum); budget/effort conversions are marked approximate. Do not claim equivalent compute across providers.
3. Each provider encoder consumes that resolved result. Native-adaptive Anthropic sends adaptive thinking and no explicit effort for Adaptive; explicit effort adds output_config.effort. Other models use their own toggles/budgets/effort locations. Off by omission is only advertised as true opt-out where known; unknown behavior remains unknown.
4. Keep the user's persisted preference separate from effective per-request choice. Surface a compact adjustment/unsupported diagnostic in frontend events rather than silently dropping an explicit setting; unsupported enabled thinking on a known nonthinking model retains the existing clamp-to-Off behavior but reports the adjustment. Strict errors are used for malformed profiles, not ordinary model switches. Include parent/subagent budget constraints before one final resolution so multiple snaps do not compound.
5. Test actual final wire fields for supported, snapped, required-thinking, Off, Adaptive and budget selections, not merely resolver output. Any deliberate change from historical wire behavior is named/documented separately from migration parity.

### Phase 8: Cutover, docs, cleanup

1. Remove concrete Rust providers, protocol builders/parsers, ProviderKind, inventory built-in metadata, static manifests and factory branches only after their contracts pass via Lua. Remove provider-specific config validation/defaults, CLI auth dispatch and TUI synthetic Codex/Vertex/Copilot branches. Config storage/parsing remains generic; builtin validation comes from registry schemas while old providers.toml fields retain meaning.
2. Remove Rust provider knowledge in thinking helpers, pricing schedules, token/capability heuristics and docgen. Retain generic arithmetic and neutral estimation. Feed any provider-specific estimation/vision/tool-example behavior through normalized capabilities, not model-name checks. Audit all consumers across workspace, including compaction/title/subagent paths.
3. Build restricted docgen registration mode in `maki-lua`: only embedded provider modules, deterministic module loading, no user plugins/env credentials/network/process/browser/storage, execution limits, and the same DTO validation. Load offline registrations only; never invoke discovery/preparation/auth. `maki-docgen/src/gen_providers.rs` renders snapshot metadata and provider-supplied reference notes.
4. Keep Luau annotations out of source files statically extracted by the current tree-sitter-lua command-doc pipeline, or extend the explicitly tested extraction boundary without changing its direct-inline-command contract. Provider metadata generation evaluates restricted Lua and does not statically interpret annotated model expressions.
5. Add architecture regression tests for absence of Rust concrete provider owners/known endpoint tables and for independent host registries. Remove obsolete dependencies; update Cargo.lock only where needed. Never commit credentials or local .ssh state.
6. Generate docs and validate through remote CI. Cheap formatting may run locally; real Rust builds/tests and docgen execute on the remote build box per AGENTS.local.md (`.ssh/remote-ci.sh`). Iterate scoped check/tests remotely before full `just ci`.

## Acceptance Criteria

- **AC.1:** HTTP contract harness deterministically captures logical/actual requests and controls streamed responses/cancellation; named harness tests pass without sleeps or credentials.
- **AC.2:** Invalid model/provider/event/result data is rejected with field paths, optional validation matches boundary validation, and failed publication preserves the previous registry snapshot.
- **AC.3:** Existing provider upstream contracts pass through real bundled Lua implementations, covering every listed provider and protocol/auth/usage variant without changing established expected requests unless an intentional correction is separately recorded.
- **AC.4:** CLI explicit login, CLI selection and TUI use the same plugin auth methods; device cancellation cannot commit; credential refresh preserves locking, reload-after-lock and atomic permissions.
- **AC.5:** Requests stream incrementally, expose headers before body, obey cancellation/timeout and security policy, and never receive automatic transport POST retries.
- **AC.6:** Concurrent/reentrant provider calls, host isolation, generation revocation and shutdown behave correctly; every frontend resolves models only after offline registration and works without optional tool plugins.
- **AC.7:** Model thinking metadata drives selection/validation and outgoing fields; requested/effective adjustments are observable, signatures/tool arguments/token accounting survive Lua roundtrips, and stale account quota/fast metadata is rejected.
- **AC.8:** Existing config/model specs/auth files/custom/catalog/executable flows continue to work through Lua; synchronous resolution is offline, coherent and host-scoped.
- **AC.9:** Generated provider documentation matches offline validated Lua metadata without I/O, annotation-bearing catalogs load, and docs drift checks pass.
- **AC.10:** No concrete built-in provider policy remains in Rust and full remote CI succeeds.

## Test Strategy

Build missing seams in Phase 1, not as follow-up work. Keep unit tests for pure transforms, real-host integration tests for Lua dispatch/HTTP/auth/storage, render/scenario tests for observable frontend behavior, and process tests for executable/refresh boundaries.

Named coverage mapping (parameterize with test_case where appropriate):

| Criteria | Tests to add or extend |
|---|---|
| AC.1 | `http_contract_harness_captures_and_releases_chunks`, `auth_harness_advances_clock_and_cancels`, `logical_endpoint_routing_preserves_request_authority` |
| AC.2 | `provider_luau_type_surface_complete`, `annotated_provider_contract_examples_load`, `provider_descriptor_validation_cases`, `model_validation_helper_matches_registration`, `invalid_discovery_preserves_snapshot`, `invalid_provider_event_and_final_result_rejected` |
| AC.3 | `provider_shape_inventory_complete`, `provider_fixture_inventory_complete`, `provider_http_contract` parameterized by all Phase 6 slugs/routes and Synthetic/Codex, `provider_stream_contract` parameterized by four codecs plus Bedrock, `provider_discovery_and_usage_contract`; retain `opencode_go_stream_sends_session_header` expectations through Lua |
| AC.4 | `auth_status_logout_contract`, `cli_tui_auth_method_parity`, `login_picker_auth_scenarios` (render labels/code/masking/error/cancel), `codex_device_auth_transcript`, `cancelled_attempt_never_commits`, `refresh_rotation_single_flight` (process/barrier), `credential_legacy_roundtrip_and_permissions`, `refresh_failure_classification` |
| AC.5 | `internal_endpoint_capability_cannot_be_spoofed`, `stream_headers_and_chunks_arrive_before_eof`, `stream_cancel_and_generation_close_connection`, `stream_idle_timeout_with_fake_clock`, `stream_post_is_not_retried`, `provider_local_endpoint_grant_is_scoped`, `stream_redirect_and_dns_rebinding_security` |
| AC.6 | `provider_reentrant_child_request`, `concurrent_provider_calls_remain_isolated`, `provider_failed_reload_preserves_previous`, `provider_generation_shutdown`, `provider_bootstrap_barrier` parameterized TUI/print/SDK/ACP/auth/models, `core_providers_load_without_optional_plugins`, `empty_registry_allows_configuration` |
| AC.7 | `thinking_wire_contract` parameterized provider/model/mode, `thinking_adjustment_visible_in_frontends`, extend existing `first_request_carries_session_thinking` and `scenario_reaches_the_provider_alike`, `provider_signatures_and_tools_roundtrip`, `provider_usage_normalization`, `account_change_invalidates_fast_and_quota` |
| AC.8 | `legacy_provider_configuration_contract`, `custom_and_catalog_resolution_contract`, `executable_provider_lifecycle_contract`, `model_registry_snapshot_resolution` including two-host isolation and snapshot precedence, `new_session_reloads_external_credentials` |
| AC.9 | `provider_docs_match_offline_registry`, `provider_doc_loader_denies_side_effects`, `annotated_provider_catalog_loads_without_static_analyzer`, `provider_metadata_changes_update_docs`, plus remote `just gen-docs-check` |
| AC.10 | `provider_migration_has_no_rust_policy_owners` (workspace source/dependency architecture guard), remote `.ssh/remote-ci.sh` running just ci |

Capture request fixtures against old implementations before deletion; run identical fixture assertions through both drivers during migration. Protocol fixture tests include chunk splits, malformed input, error statuses, Retry-After, cached-token accounting, thinking signatures, tool arguments, stop reasons and completion before physical EOF. Full-stack tests drive actual prepared Lua providers, not canned Provider substitutes. Existing frontend tests keep invoking the real binary. No live-provider test infrastructure is required for these contracts; optional manual live smoke tests may supplement but never replace deterministic tests.

## Review Strategy

Before handoff, use a read-only plan_reviewer to audit this artifact against the request and test coverage. Fix/rebut all findings; repeat if high/critical findings occur. After implementation and automatable checks, dispatch an independent general reviewer to audit migration completeness, protocol compatibility, auth/security, lifecycle races, and fixture independence. Fix/rebut findings and repeat for critical findings until clear or blocked for operator decision.

## Documentation Strategy

Update generated provider, Lua API, plugin and auth/command references through the existing docgen workflow. Document normalized model/thinking/instance/error tables, validation helper, response-handle lifetime/cancellation/security, auth interaction, offline bootstrap and metadata-only docs loading. Update existing architecture guidance in AGENTS.md to explain Lua ownership and scoped registry/factory seams. Update existing hand-written CLI/headless/permissions docs where auth or private-endpoint behavior needs explanation, linking canonical references rather than duplicating policy. Explain requested/effective thinking adjustments and approximate cross-provider budgets without implying equal reasoning compute. Full static plugin typechecking is explicitly future work; annotations alone are not a validation guarantee.

## Risks, Blockers, and Required Decisions

No operator decisions remain blocking: one bundled plugin, annotations now/runtime validation, optional helper, offline tables plus live discovery, Lua codecs/policy, shared auth and HTTP contract-first migration were discussed and accepted.

- This is a broad multi-crate migration. Keep phases independently testable and avoid deleting Rust fixtures before equivalent Lua contracts pass.
- Current tests cover selected real HTTP paths, not every provider. Phase 1 explicitly supplies the missing deterministic network/auth/process harness and fixture coverage.
- Existing protocol quirks are compatibility baselines, not proof upstream behavior is ideal. Record exceptions and separate intentional corrections from migration work.
- Model/account discovery and async refresh can race provider replacement; generation-bound snapshots and contract tests are mandatory.
- Network policy differs between current provider HTTP and web-fetch Lua HTTP. Scoped endpoint grants must preserve local/cloud credential functionality without weakening untrusted plugin restrictions.
- Moving codecs to Lua adds allocations and cross-thread events. Batch only where observable streaming remains prompt; profile fixture-backed large histories and stream throughput before cutover, without adding a separate optimization project.
- Annotation syntax and current tree-sitter-lua docs extraction are not interchangeable. Keep metadata evaluation distinct and cover annotated catalogs explicitly.
- Secure storage, external credential import and executable interactive login span process/platform boundaries. Keep provider policy in Lua but security-sensitive mechanisms generic and testable; cross-platform permission checks must use platform-appropriate assertions.
- Full static typechecking and live upstream API conformance remain outside this issue. Runtime validators and deterministic contract tests are included, not waived.
