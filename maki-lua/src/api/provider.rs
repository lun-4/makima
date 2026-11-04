use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_config::providers::{ProviderDef, ProvidersConfig};
use maki_lua_macro::{lua_fn, lua_table};
use maki_providers::login::{AuthMethod, AuthSpec, ConfigField, LoginAlias};
use maki_providers::{ModelInfo, ProviderDocs};
use mlua::{DeserializeOptions, Function, Lua, LuaSerdeExt, Result as LuaResult, Table, Value};
use serde::Deserialize;
use serde::de::{Deserializer, Error, Visitor};

use crate::api::util::pair::{Pair, try_pair};
use crate::plugin_permissions::PluginPermissions;
use crate::provider::serialize_options;
use crate::runtime::require_plugin_load;

const ID_PUNCTUATION: &str = "-_.";
const BAD_ID_ERR: &str =
    "must be non-empty ASCII letters, digits, '-', '_' or '.', and not start with '.'";
const BAD_ALIAS_ERR: &str = "must be a valid provider id other than the provider's own";

pub(crate) struct ProviderSpec {
    pub(crate) plugin: Arc<str>,
    pub(crate) id: Arc<str>,
    pub(crate) display_name: Arc<str>,
    pub(crate) models: Vec<ModelInfo>,
    pub(crate) prepare: Function,
    pub(crate) discover: Option<Function>,
    pub(crate) auth: AuthSpec,
    /// The `login` of each of `auth.methods`, in the same order.
    pub(crate) logins: Vec<Function>,
    pub(crate) auth_status: Option<Function>,
    pub(crate) logout: Option<Function>,
    pub(crate) docs: Option<ProviderDocs>,
    /// The registering plugin's `maki.net.send`, carrying its permissions.
    pub(crate) send: Function,
}

pub(crate) type PendingProviders = Arc<Mutex<Vec<ProviderSpec>>>;

/// Specs of every successfully loaded plugin.
#[derive(Default)]
pub(crate) struct ProviderStore(Vec<ProviderSpec>);

impl ProviderStore {
    pub(crate) fn specs(&self) -> &[ProviderSpec] {
        &self.0
    }

    pub(crate) fn clear_plugin(&mut self, plugin: &str) {
        self.0.retain(|spec| *spec.plugin != *plugin);
    }

    fn foreign_owner(&self, id: &str, plugin: &str) -> Option<Arc<str>> {
        self.0
            .iter()
            .find(|spec| *spec.id == *id && *spec.plugin != *plugin)
            .map(|spec| Arc::clone(&spec.plugin))
    }
}

/// Replaces {plugin}'s committed specs with the ones its load registered.
pub(crate) fn commit_pending(lua: &Lua, plugin: &str, pending: &PendingProviders) {
    let specs = std::mem::take(&mut *lock(pending));
    if let Some(mut store) = lua.app_data_mut::<ProviderStore>() {
        store.clear_plugin(plugin);
        store.0.extend(specs);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Serde skips function values in tables, so a present callback never reaches
/// this; anything that does is a wrong type.
#[derive(Default)]
struct Callback;

impl<'de> Deserialize<'de> for Callback {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CallbackVisitor;
        impl Visitor<'_> for CallbackVisitor {
            type Value = Callback;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a function")
            }
            fn visit_unit<E: Error>(self) -> Result<Callback, E> {
                Ok(Callback)
            }
        }
        deserializer.deserialize_any(CallbackVisitor)
    }
}

/// The data fields of a spec. The callback fields are listed only so that
/// `deny_unknown_fields` accepts them; the callbacks themselves are read from
/// the table as functions.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpecFields {
    id: String,
    display_name: String,
    #[serde(default)]
    models: Vec<ModelInfo>,
    #[serde(default, rename = "prepare")]
    _prepare: Callback,
    #[serde(default, rename = "discover")]
    _discover: Callback,
    #[serde(default)]
    auth: Vec<AuthMethod>,
    #[serde(default)]
    aliases: Vec<LoginAlias>,
    #[serde(default)]
    config: Vec<ConfigField>,
    #[serde(default)]
    section: Option<String>,
    #[serde(default)]
    docs: Option<ProviderDocs>,
    #[serde(default, rename = "auth_status")]
    _auth_status: Callback,
    #[serde(default, rename = "logout")]
    _logout: Callback,
}

/// The id names the provider's credentials file, so it must be a plain file name.
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && !id.starts_with('.')
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || ID_PUNCTUATION.contains(c))
}

/// Checks what serde cannot: references between methods and aliases.
fn validate_auth(id: &str, auth: &AuthSpec) -> Result<(), String> {
    let methods: Vec<&str> = auth.methods.iter().map(|method| &*method.id).collect();
    for (i, method) in methods.iter().enumerate() {
        if methods[..i].contains(method) {
            return Err(format!("auth method {method:?} is listed twice"));
        }
    }
    for alias in &auth.aliases {
        if !valid_id(&alias.id) || alias.id == id {
            return Err(format!("alias {:?} {BAD_ALIAS_ERR}", alias.id));
        }
        if !methods.contains(&&*alias.method) {
            return Err(format!(
                "alias {:?} names unknown auth method {:?}",
                alias.id, alias.method
            ));
        }
    }
    Ok(())
}

fn spec_error(message: impl fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("register_provider: {message}"))
}

/// Registers a model provider. Call it at the top level during plugin load.
/// A provider id belongs to one plugin; registering an id another plugin
/// already owns fails and names both plugins.
///
/// {spec} fields:
///   `id` (string) Required. The provider part of `provider/model` specs,
///   made of ASCII letters, digits, `-`, `_` and `.`, not starting with `.`.
///   `display_name` (string) Required.
///   `models` (table) Model descriptors: `id` plus optional
///   `context_window`, `max_output_tokens`, `pricing`, `pricing_schedule`
///   (`{ windows = { { start, end } }, weekdays_only?, multiplier }`, whole
///   UTC hours that scale `pricing` by `multiplier`), `supports_thinking`,
///   `requires_thinking`, `supports_vision`, `supports_fast`,
///   `supports_tool_examples`, `tier`, `free` (offered at no charge, marked
///   in the model picker) and `provider_data`. The first descriptor with a
///   tier is that tier's default. `supports_fast` may be `"pending"` while
///   the provider cannot tell before its discovery answers: a saved or typed
///   `/fast` then waits for the model prepared after discovery.
///   `prepare` (function) Required. `prepare(ctx, model, config)` returns an
///   instance for one model descriptor, optionally followed by an adjusted
///   descriptor with the same id, or nil plus an error. `config` is the
///   provider's `providers.toml` section as a table, read at each call.
///   `discover` (function) Optional. `discover(ctx, config, refresh)`
///   returns the descriptors the provider offers now. `refresh` is true when
///   the user asked to bypass caches (`maki models --refresh`).
///   `config` (table) Optional. `providers.toml` fields asked before login:
///   `{ key, prompt, default?, secret?, optional?, choices? }`, where
///   `choices` is a list of `{ id, label, detail? }`. Each answer is saved
///   as a text field of the provider's section, such as `base_url`.
///   `auth` (table) Login methods: `{ id, label, login }`.
///   `login(ctx, ui, config)` gets a `maki.provider.LoginUi` and the
///   provider's section with this login's answers applied, not yet saved.
///   It returns a truthy value, optionally a table with `model` to choose
///   the default model, or nil plus an error.
///   `aliases` (table) Optional. `{ id, label, method }` names that log in
///   with a preset method.
///   `section` (string) Optional. The heading the provider is listed under
///   in the login picker, after the providers without one.
///   `auth_status` (function) Optional. `auth_status(ctx, config)` returns
///   `{ state, detail? }`, where `state` is `saved`, `env`, `configured`,
///   `missing` or `failed` (the check could not tell, shown as an error and
///   treated as missing). It must work without credentials. Without it, the
///   provider counts as logged in while its credentials file exists.
///   `logout` (function) Optional. `logout(ctx)`. Without it, logout
///   deletes the credentials file.
///   `docs` (table) Optional. What the providers reference page says
///   beyond models, login methods and config fields: `env` (the
///   environment variables it reads), `endpoint` (its default endpoint) and
///   `notes` (Markdown). Providers without it are left off that page.
///
/// Config is saved and the default model chosen only after `login`
/// succeeds and `auth_status` no longer reports `missing`.
///
/// `ctx` is `{ id, send, credentials }`, where `send` is `maki.net.send`
/// with the `[provider]` connect, low-speed and stream timeouts as defaults, and
/// `credentials` is the provider's `maki.provider.Credentials`. Only the
/// registering plugin's callbacks receive `ctx`, so a provider id's
/// credentials belong to the plugin that registered it. An instance has
/// `stream(ctx, request, emit)` and optionally `fetch_usage(ctx)`,
/// `refresh_auth(ctx)`, `reload_auth(ctx)`, `rotate_key(ctx)`,
/// `end_conversation(ctx, conversation_id, reason)` and `key_count` (a
/// number, or a function read at prepare and after each `reload_auth` or
/// `refresh_auth`, for a pool those can change). `request` has `model`
/// (`id`, `max_output_tokens`, `context_window`), `system`, `messages`,
/// `tools`, `thinking`, `fast`, `session_id` and `invocation`
/// (`conversation_id`, `operation_id`, `attempt`, `purpose`, `agent_id`).
/// `max_output_tokens` is what the model declares. `reason` is `cancelled`,
/// `closed`, `compacted` or `rewound`; the host never uses an ended
/// conversation id again. `emit(event)` takes `{ type = "text_delta", text }`
/// and the other provider events, and returns false once the call is over.
/// `stream` returns `{ message, usage, stop_reason }`. A failure returns nil
/// plus an error: `{ kind = "config", message }` is a configuration error,
/// such as a missing key or setting, and is not retried; a table with
/// `status`, `message` and optional `retry_after` seconds is an API error,
/// and `overflow = true` on it says the request does not fit the model's
/// context, which the host answers by compacting instead of retrying;
/// anything else is a retryable transport error. A raised error is not
/// retried. Every callback that returns nil plus an error follows these
/// rules.
///
/// @param spec table Provider definition (see above).
/// @return
/// @example
/// maki.api.register_provider({
///   id = "echo",
///   display_name = "Echo",
///   models = { { id = "echo-1", tier = "strong" } },
///   auth = {
///     { id = "api_key", label = "API key", login = function(ctx, ui, config)
///       local key, err = ui:input("API key", true)
///       if not key then return nil, err end
///       return ctx.credentials:write({ api_key = key })
///     end },
///   },
///   prepare = function(ctx, model, config)
///     return {
///       stream = function(ctx, request, emit)
///         emit({ type = "text_delta", text = "hi" })
///         return {
///           message = { role = "assistant", content = { { type = "text", text = "hi" } } },
///           stop_reason = "end_turn",
///         }
///       end,
///     }
///   end,
/// })
#[lua_fn]
fn register_provider(
    lua: &Lua,
    #[ctx] pending: PendingProviders,
    #[ctx] plugin: Arc<str>,
    #[ctx] send: Function,
    spec: Table,
) -> LuaResult<()> {
    require_plugin_load(lua, &plugin, "register_provider")?;
    let fields: SpecFields = lua
        .from_value_with(
            Value::Table(spec.clone()),
            DeserializeOptions::new().deny_unsupported_types(false),
        )
        .map_err(spec_error)?;
    if !valid_id(&fields.id) {
        return Err(spec_error(format!("id {:?} {BAD_ID_ERR}", fields.id)));
    }
    let prepare = spec
        .get::<Option<Function>>("prepare")?
        .ok_or_else(|| spec_error(format!("provider {:?}: prepare is required", fields.id)))?;
    let discover = spec.get::<Option<Function>>("discover")?;
    let auth = AuthSpec {
        methods: fields.auth,
        aliases: fields.aliases,
        config: fields.config,
        section: fields.section,
    };
    validate_auth(&fields.id, &auth).map_err(spec_error)?;
    let logins = match spec.get::<Option<Table>>("auth")? {
        Some(methods) => methods
            .sequence_values::<Table>()
            .zip(&auth.methods)
            .map(|(method, fields)| {
                method?.get::<Option<Function>>("login")?.ok_or_else(|| {
                    spec_error(format!("auth method {:?}: login is required", fields.id))
                })
            })
            .collect::<LuaResult<_>>()?,
        None => Vec::new(),
    };

    let mut pending = lock(&pending);
    let owner = lua
        .app_data_ref::<ProviderStore>()
        .and_then(|store| store.foreign_owner(&fields.id, &plugin))
        .or_else(|| {
            pending
                .iter()
                .find(|spec| *spec.id == *fields.id)
                .map(|spec| Arc::clone(&spec.plugin))
        });
    if let Some(owner) = owner {
        return Err(spec_error(format!(
            "provider {:?} from plugin {plugin:?} is already registered by plugin {owner:?}",
            fields.id
        )));
    }
    pending.push(ProviderSpec {
        plugin,
        id: fields.id.into(),
        display_name: fields.display_name.into(),
        models: fields.models,
        prepare,
        discover,
        auth,
        logins,
        auth_status: spec.get("auth_status")?,
        logout: spec.get("logout")?,
        docs: fields.docs,
        send,
    });
    Ok(())
}

/// Read `providers.toml`: the section of provider {id}, or every section by
/// provider id when {id} is nil. The file is read at each call, so a section
/// saved earlier in the session is seen. A section has the fields a
/// provider's `config` argument has. Use it for a setting another provider's
/// section holds, or to register providers the file defines.
///
/// @param id string? Provider id.
/// @return (table?) The section, nil when there is none, or every section.
/// @example
/// local opencode = maki.api.provider_config("opencode") or {}
/// for id, section in pairs(maki.api.provider_config()) do print(id, section.protocol) end
#[lua_fn(guard = Env)]
fn provider_config(lua: &Lua, id: Option<String>) -> LuaResult<Value> {
    let config = ProvidersConfig::load_or_default();
    match id {
        Some(id) => config.get(&id).map_or(Ok(Value::Nil), |def| {
            lua.to_value_with(def, serialize_options())
        }),
        None => lua.to_value_with(&config.providers, serialize_options()),
    }
}

/// Save {section} as provider {id}'s `providers.toml` section, replacing
/// any section it had. Fields the file does not know are dropped. Fails
/// without touching the file when it does not parse.
///
/// @param id string Provider id.
/// @param section table Fields such as `protocol`, `base_url` and `api_key_env`.
/// @return (boolean?, string?) true, or nil plus an error.
/// @example
/// maki.api.save_provider_config("my-llm", { protocol = "openai", base_url = "http://localhost:8000/v1" })
#[lua_fn(guard = FsWrite)]
fn save_provider_config(lua: &Lua, id: String, section: Value) -> LuaResult<Pair<bool>> {
    let def: ProviderDef = try_pair!(lua.from_value(section));
    let mut config = try_pair!(ProvidersConfig::try_load());
    config.upsert(id, def);
    try_pair!(config.save());
    Ok((Some(true), None))
}

lua_table! {
    extend "maki.api" => pub(crate) fn add_provider_fn(
        pending: PendingProviders,
        plugin: Arc<str>,
        send: Function,
        perms: &PluginPermissions
    ), DOCS [
        register_provider(pending, plugin, send),
        provider_config(perms),
        save_provider_config(perms),
    ]
}
