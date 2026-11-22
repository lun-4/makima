# Plugin ctx: first step toward capabilities (#44)

Branch: `philpax.44-provider-api-v2`. Replaces `44-provider-credentials-no-ctx.md`.

## Goal

A plugin gets a ctx: a host-made userdata that stands for the plugin's authority. Its entry chunk receives it, passes it on by reference, narrows it by name, and hands it to APIs that act for the plugin. In this branch the ctx carries three things:

- a namespace for plugin credential stores, which `maki.credentials.open(ctx, key)` opens;
- `send`, network access with timeouts from `MAKIMA_NET_TIMEOUT_SECS`, usable at load;
- the authority under which `maki.api.register_provider(ctx, spec)` registers a provider.

Provider callbacks get a provider ctx that the host derives from the registering plugin ctx: the plugin ctx plus the provider's id, its `send` with the `[provider]` timeouts, and its provider-id credentials.

The first user is the Token Fountain plugin (`~/git/token-fountain-makima-provider/`): one gzai sign-in, stored in a plugin store, serves a `fountain` provider and one `tf-<slug>` provider per project.

This replaces the `maki.api.provider_credentials(id)` lookup currently on the branch (uncommitted), which found credentials by name and checked ownership: ACL-shaped. Here authority moves by reference: whoever holds a ctx may use it.

## Design

### Root ctx

Every plugin entry chunk receives its root ctx as the chunk's `...`:

```lua
-- ~/.config/makima/init.lua
local ctx = ...
require("fountain.provider").setup(ctx:with_name("fountain"))
```

- Bundled plugins (`load_source`, `runtime.rs` near `func.call_async::<()>(())`) and `init.lua` (`run_init_lua`) both pass it. Bundled plugins that ignore `...` are unaffected.
- The root ctx's namespace is the plugin name. The project `init.lua` gets the same root as the global one (`maki_init.global`), so a plugin sees the same stores from either config. A trusted project's `init.lua` can therefore open the global config's stores; project config only runs after folder trust, and trusted config can already read files, so this adds no exposure. Provider id ownership keeps using the calling plugin (`maki_init.project` stays distinct there), because unload clears specs by plugin.
- There is no `maki.current_context()`. A global getter would let code skip the narrowed ctx it was handed and take the root.
- Modules receive a ctx through an explicit call such as `setup(ctx)`, never through `require`: modules are cached, so a ctx passed by the first `require` would silently apply to every later one.

### Ctx userdata

`maki.Ctx`, host-made and immutable. A plugin cannot forge, edit or widen one.

| Member | Plugin ctx | Provider ctx |
|---|---|---|
| `ctx:with_name(name)` | child ctx whose stores live under `name` | same, from its plugin ctx |
| `ctx.send(request)` | `maki.net.send` with `MAKIMA_NET_TIMEOUT_SECS` defaults | `maki.net.send` with the `[provider]` timeouts |
| `ctx.id` | absent | provider id |
| `ctx.credentials` | absent | the provider-id store, `<state>/auth/<id>.json` as today |

- `with_name` narrows only: a child cannot reach its parent's or a sibling's stores. Names follow the provider id rules (`valid_id`: ASCII letters, digits, `-`, `_`, `.`, not starting with `.`).
- Only the host extends a ctx, as it does when it derives a provider ctx from a plugin ctx by adding `send` with the `[provider]` timeouts, `id` and `credentials`. Plugins only narrow.
- `send` stays a field (`ctx.send(...)`), as every bundled provider and `providers.http` call it today. Request fields still override its defaults. It still goes through the registering plugin's `net` permission from `plugin.toml`; moving that check to the moment a ctx is granted is future work.
- Provider ctxs are built per call as today (`provider.rs` `fn ctx`), now as userdata. `maki.test_helpers` keeps faking provider ctxs as tables; providers only read fields, so specs keep working.

### `MAKIMA_NET_TIMEOUT_SECS`

The plugin ctx exists before config does (`maki.setup` runs inside `init.lua`), so its `send` takes its timeouts from the environment: `MAKIMA_NET_TIMEOUT_SECS`, default 30, used as the connect, low-speed and idle default. Read when a root ctx is made. An unparsable or zero value logs a warning and uses the default.

### Plugin credential stores

```lua
local account = maki.credentials.open(ctx, "account")
account:read()  account:write(value)  account:delete()  account:refresh(fn)  account:lock(fn)
```

- `ctx` is a plugin ctx or a provider ctx; a provider ctx opens its plugin ctx's namespace. Anything else raises an error.
- Path: `<state>/auth/plugins/<root>/<name>/.../<key>.json`, its lock beside it. Keys follow the provider id rules. Nothing outside `plugins/` is reachable, so a plugin store can never be a provider's file.
- The class `maki.provider.Credentials` becomes `maki.credentials.Store`. Provider ctxs' `credentials` is a `Store` too; its methods and semantics are unchanged.
- `RefreshFlights` and the cross-process lock key on the store's location instead of the provider id.
- The host's default `auth_status` and logout keep looking at provider-id stores only. A provider whose sign-in lives in a plugin store supplies both.

### `register_provider(ctx, spec)`

```lua
maki.api.register_provider(ctx, spec)   -- provider ctxs derive from ctx
maki.api.register_provider(spec)        -- unchanged; registers under the root ctx
```

The first API that takes a ctx. The spec keeps its plugin ctx, and every callback's provider ctx derives from it. The bundled `providers` plugin keeps the one-argument form.

### Refresh during load

Carried over from the previous plan: `Store:refresh` runs its function inline while a plugin is loading. A load holds the request loop that runs refresh jobs, so a queued job would wait for the load that waits for it.

### Removed

`maki.api.provider_credentials`, `NOT_REGISTERED_ERR`, `ProviderStore::owns` and their tests.

## Changes

| Area | Change |
|---|---|
| `maki-lua/src/ctx.rs` (new) | `Ctx` userdata (`Plugin`, `Provider`), `PluginCtx` (namespace, the plugin's `maki.net.send`, timed `send`), `with_name`, read-only fields, `MAKIMA_NET_TIMEOUT_SECS` parsing, the timeout-defaults wrapper moved from `provider.rs` |
| `maki-lua/src/api/mod.rs` | `create_maki_global` takes the root name, builds the root ctx, adds `maki.credentials`, returns the table and the root |
| `maki-lua/src/runtime.rs` | entry chunks get their root ctx as `...`; `init.lua` (global and project) gets the `maki_init.global` root |
| `maki-lua/src/api/provider.rs` | `register_provider(ctx?, spec)`; the spec keeps its `PluginCtx` instead of the raw `send` |
| `maki-lua/src/provider.rs` | callback ctx is a provider `Ctx` derived from the spec's ctx |
| `maki-lua/src/credentials.rs` | `Store` (renamed) addresses a credentials file by name; `maki.credentials.open`; inline refresh at load |
| `maki-storage/src/auth.rs` | `PLUGIN_STORES_DIR`; the credential functions take a name (provider id or plugin store path) |
| `maki-lua/src/docs_render.rs` | Plugins guide section "The plugin ctx", which also documents `MAKIMA_NET_TIMEOUT_SECS` |
| `plugins/providers/types.lua` | `Credentials` → `Store`; `Context` notes |
| `site/docs/` | `just gen-docs` (Lua API, Plugins) |

`register_provider`'s generated signature reads `({ctx?}, {spec?})`: the docs generator marks optional parameters by their Rust `Option` type, and the one-argument form puts the spec first, so both are `Option`; the `@param` text says `spec` is required.

## Tests

`maki-lua/tests/provider_credentials.rs` grows into ctx tests; `#[test_case]` where cases share a shape.

- An entry chunk receives a root ctx; `with_name` children open separate stores, and a child cannot open its parent's store.
- `maki.credentials.open` with a plugin ctx, with a provider ctx (same namespace as its plugin ctx), and with a non-ctx value (error).
- Store names and keys that break the id rules are refused.
- `register_provider(ctx, spec)`: a callback's provider ctx opens the same plugin store as `ctx`; `ctx.id`, `ctx.credentials` still address the provider-id file.
- The one-argument `register_provider` still works (the existing tests cover it).
- At load: `ctx.send` reaches the test server, and `Store:refresh` whose function sends returns and writes (bounded, so a regression fails instead of hanging).
- `MAKIMA_NET_TIMEOUT_SECS`: a plugin ctx's `send` against a server that never answers gives up after the configured time.
- A ctx field cannot be assigned.

## Future (not in this branch)

- **Capability-required APIs.** Other APIs take a ctx the way `register_provider` and `maki.credentials.open` do, and their global forms go away for packages.
- **Network as a capability.** A ctx's `send` is granted with the ctx, from the plugin's grants, instead of checked against `plugin.toml` on each call. Narrowing: `ctx:narrow({ net = { "*.gzai.org" } })`. Per-plugin overrides of timeouts, keyed by the ctx's name.
- **Packages.** A package manager loads each package with its own root ctx built from its own manifest, so grants and stores stop being shared by everything `init.lua` loads, and `plugin.toml` stops covering a whole config dir.
- **Ambient globals.** Capabilities confine only once packages run without `maki.net`, `maki.fs` and the other globals. Until then the globals keep the `plugin.toml` checks.
- **Revocation.** A ctx carries its plugin generation (`PluginAuthority`) and refuses work once that plugin unloads. Today `/reload` rebuilds the whole host, so no ctx outlives its plugin.
- **One ctx type.** The tool `LuaCtx` (`api/util/ctx.rs`, `Caps`) and `Ctx` merge, and the trusted and delegated authority rules become capabilities of it.
- **Test ctxs.** Once host APIs require a real ctx, `maki.test_helpers` needs a host-made test ctx instead of tables.

## Checks

`just fmt`, `cargo check -p maki-lua --tests`, `cargo nextest run -p maki-lua`, `just gen-docs-check`, then `.ssh/remote-ci.sh` (its `pylint` step currently fails on the box's newer `ty` in unrelated harbor files; run tests and `gen-docs-check` directly there if it still does).
