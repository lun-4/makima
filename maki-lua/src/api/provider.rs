use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_lua_macro::{lua_fn, lua_table};
use maki_providers::ModelInfo;
use mlua::{DeserializeOptions, Function, Lua, LuaSerdeExt, Result as LuaResult, Table, Value};
use serde::Deserialize;
use serde::de::{Deserializer, Error, Visitor};

use crate::runtime::require_plugin_load;

pub(crate) struct ProviderSpec {
    pub(crate) plugin: Arc<str>,
    pub(crate) id: Arc<str>,
    pub(crate) display_name: Arc<str>,
    pub(crate) models: Vec<ModelInfo>,
    pub(crate) prepare: Function,
    pub(crate) discover: Option<Function>,
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
}

fn spec_error(message: impl fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("register_provider: {message}"))
}

/// Registers a model provider. Call it at the top level during plugin load.
/// A provider id belongs to one plugin; registering an id another plugin
/// already owns fails and names both plugins.
///
/// {spec} fields:
///   `id` (string) Required. The provider part of `provider/model` specs.
///   `display_name` (string) Required.
///   `models` (table) Model descriptors: `id` plus optional
///   `context_window`, `max_output_tokens`, `pricing`, `supports_thinking`,
///   `requires_thinking`, `supports_vision`, `supports_fast`,
///   `supports_tool_examples`, `tier` and `provider_data`. The first
///   descriptor with a tier is that tier's default.
///   `prepare` (function) Required. `prepare(ctx, model)` returns an instance
///   for one model descriptor, optionally followed by an adjusted descriptor
///   with the same id, or nil plus an error.
///   `discover` (function) Optional. `discover(ctx)` returns the descriptors
///   the provider offers now.
///
/// `ctx` is `{ id, send }`, where `send` is `maki.net.send`. An instance has
/// `stream(ctx, request, emit)` and optionally `fetch_usage(ctx)`,
/// `refresh_auth(ctx)`, `reload_auth(ctx)`, `rotate_key(ctx)`,
/// `end_conversation(ctx, conversation_id, reason)` and `key_count` (a
/// number, or a function read once at prepare). `request` has `model`
/// (`id`, `output_tokens`, `thinking_ceiling`, `context_window`), `system`,
/// `messages`, `tools`, `thinking`, `fast`, `session_id` and `invocation`
/// (`conversation_id`, `operation_id`, `attempt`, `purpose`, `agent_id`).
/// `reason` is `cancelled`, `closed`, `compacted` or `rewound`; the host
/// never uses an ended conversation id again. `emit(event)` takes
/// `{ type = "text_delta", text }` and the other provider events, and
/// returns false once the call is over. `stream` returns
/// `{ message, usage, stop_reason }`. A failure returns nil plus an error:
/// a table with `status`, `message` and optional `retry_after` seconds is
/// an API error, anything else a retryable transport error. A raised error
/// is not retried.
///
/// @param spec table Provider definition (see above).
/// @return
/// @example
/// maki.api.register_provider({
///   id = "echo",
///   display_name = "Echo",
///   models = { { id = "echo-1", tier = "strong" } },
///   prepare = function(ctx, model)
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
    if fields.id.is_empty() || fields.id.contains('/') {
        return Err(spec_error(format!(
            "id {:?} must be non-empty and contain no '/'",
            fields.id
        )));
    }
    let prepare = spec
        .get::<Option<Function>>("prepare")?
        .ok_or_else(|| spec_error(format!("provider {:?}: prepare is required", fields.id)))?;
    let discover = spec.get::<Option<Function>>("discover")?;

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
        send,
    });
    Ok(())
}

lua_table! {
    extend "maki.api" => pub(crate) fn add_provider_fn(
        pending: PendingProviders,
        plugin: Arc<str>,
        send: Function
    ), DOCS [register_provider(pending, plugin, send)]
}
