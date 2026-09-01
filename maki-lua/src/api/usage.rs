use std::collections::HashMap;
use std::sync::Arc;

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Function, Lua, Result as LuaResult, Table, Value};

use crate::api::util::command::{ProviderUsageSnapshot, UiAction, ui_roundtrip};
use crate::api::util::convert::json_to_lua;
use crate::api::util::dispatch::call_isolated;
use crate::api::util::pair::{Pair, try_pair};

const CALLBACK_SEAM: &str = "maki.usage.on_change";

#[derive(Default)]
pub(crate) struct UsageMirror {
    snapshot: Option<ProviderUsageSnapshot>,
    callbacks: HashMap<Arc<str>, Vec<Function>>,
}

impl UsageMirror {
    pub(crate) fn clear_plugin(&mut self, plugin: &str) {
        self.callbacks.remove(plugin);
    }
}

fn snapshot_to_lua(lua: &Lua, snapshot: &ProviderUsageSnapshot) -> LuaResult<Value> {
    let value = serde_json::to_value(snapshot).map_err(mlua::Error::external)?;
    json_to_lua(lua, &value)
}

pub(crate) fn publish(lua: &Lua, snapshot: ProviderUsageSnapshot) {
    let callbacks: Vec<(Arc<str>, Function)> = {
        let Some(mut mirror) = lua.app_data_mut::<UsageMirror>() else {
            return;
        };
        if mirror.snapshot.as_ref() == Some(&snapshot) {
            return;
        }
        mirror.snapshot = Some(snapshot.clone());
        mirror
            .callbacks
            .iter()
            .flat_map(|(plugin, callbacks)| {
                callbacks
                    .iter()
                    .cloned()
                    .map(|callback| (Arc::clone(plugin), callback))
            })
            .collect()
    };
    let value = match snapshot_to_lua(lua, &snapshot) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "failed to convert provider usage snapshot");
            return;
        }
    };
    for (plugin, callback) in callbacks {
        call_isolated::<()>(lua, &callback, value.clone(), CALLBACK_SEAM, &plugin);
    }
}

/// Returns the latest provider quota snapshot without starting a network request.
/// Returns nil until the interactive UI publishes its first snapshot.
///
/// @return table|nil `{provider_id, provider, model, status}` or nil.
#[lua_fn]
fn get(lua: &Lua) -> LuaResult<Value> {
    let snapshot = lua
        .app_data_ref::<UsageMirror>()
        .and_then(|mirror| mirror.snapshot.clone());
    match snapshot {
        Some(snapshot) => snapshot_to_lua(lua, &snapshot),
        None => Ok(Value::Nil),
    }
}

/// Fetches the active provider account's quota. Ordinary fetches join an
/// in-flight request; a forced fetch queues one fresh follow-up.
///
/// @param opts table? Optional `{force = boolean}`.
/// @return table|nil, string|nil Fresh `{provider_id, provider, model, status}`, or nil and an error.
#[lua_fn]
async fn fetch(
    _lua: Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
    opts: Option<Table>,
) -> LuaResult<Pair<Value>> {
    let force = match opts {
        Some(opts) => opts.get::<Option<bool>>("force")?.unwrap_or(false),
        None => false,
    };
    let snapshot = try_pair!(
        ui_roundtrip(tx.as_ref(), |reply_tx| UiAction::UsageFetch {
            force,
            reply_tx
        })
        .await
    );
    let snapshot = try_pair!(snapshot);
    Ok((Some(snapshot_to_lua(&_lua, &snapshot)?), None))
}

/// Registers a synchronous callback for provider quota snapshot changes.
/// Registrations belong to the plugin and are removed when it unloads.
/// Callback failures are logged and do not stop other callbacks.
///
/// @param callback function Called with `{provider_id, provider, model, status}`.
/// @return function Idempotent unsubscribe callback.
#[lua_fn]
fn on_change(lua: &Lua, #[ctx] plugin: Arc<str>, callback: Function) -> LuaResult<Function> {
    let mut mirror = lua
        .app_data_mut::<UsageMirror>()
        .ok_or_else(|| mlua::Error::runtime("usage mirror not initialized"))?;
    mirror
        .callbacks
        .entry(Arc::clone(&plugin))
        .or_default()
        .push(callback.clone());
    let key = lua.create_registry_value(callback)?;
    let plugin_key = plugin;
    lua.create_function(move |lua, ()| {
        let Some(mut mirror) = lua.app_data_mut::<UsageMirror>() else {
            return Ok(());
        };
        if let Some(callbacks) = mirror.callbacks.get_mut(&plugin_key) {
            callbacks.retain(|callback| callback != &lua.registry_value::<Function>(&key).unwrap());
        }
        Ok(())
    })
}

lua_table! {
    /// Provider-side quota snapshots. `get` reads the Lua-thread mirror,
    /// `fetch` asks the interactive UI to refresh it, and `on_change` observes
    /// canonical publications. Without an interactive UI `get` stays nil,
    /// `fetch` returns `nil, "no interactive UI attached"`, and callbacks are idle.
    "maki.usage" => pub(crate) fn create_usage_table(
        tx: Option<flume::Sender<UiAction>>,
        plugin: Arc<str>,
    ), DOCS [get, fetch(tx), on_change(plugin)]
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::api::util::command::{NO_UI_ERR, ProviderUsageLimit, ProviderUsageWindow};

    fn snapshot() -> ProviderUsageSnapshot {
        ProviderUsageSnapshot {
            provider_id: "anthropic".into(),
            provider: "Anthropic".into(),
            model: "claude-sonnet-4-5".into(),
            status: "ready".into(),
            limits: vec![ProviderUsageLimit {
                window: ProviderUsageWindow::Hours { value: 5 },
                percentage: Some(42),
                reset_at_ms: None,
                detail: None,
            }],
            plan: Some("pro".into()),
            error: None,
        }
    }

    fn lua_with_usage(tx: Option<flume::Sender<UiAction>>, plugin: &str) -> Lua {
        let lua = Lua::new();
        lua.set_app_data(UsageMirror::default());
        let table = create_usage_table(&lua, tx, Arc::from(plugin)).unwrap();
        lua.globals().set("usage", table).unwrap();
        lua
    }

    #[test]
    fn snapshot_serialization_has_exact_top_level_schema() {
        let value = serde_json::to_value(snapshot()).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(
            object
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            [
                "limits",
                "model",
                "plan",
                "provider",
                "provider_id",
                "status"
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        );
        assert_eq!(value["status"], json!("ready"));
        assert_eq!(value["plan"], json!("pro"));
        assert_eq!(value["limits"][0]["window"]["kind"], json!("hours"));
        assert_eq!(value["limits"][0]["window"]["value"], json!(5));
        assert_eq!(
            serde_json::from_value::<ProviderUsageSnapshot>(value).unwrap(),
            snapshot()
        );
    }

    #[test]
    fn publish_updates_mirror_before_isolated_callbacks() {
        let lua = lua_with_usage(None, "bad");
        lua.load("usage.on_change(function() error('boom') end)")
            .exec()
            .unwrap();
        let good = create_usage_table(&lua, None, Arc::from("good")).unwrap();
        lua.globals().set("usage_good", good).unwrap();
        lua.load(
            "seen = false; usage_good.on_change(function(s)\n                local current = usage.get()\n                seen = current.provider_id == s.provider_id and current.status == s.status\n            end)",
        )
        .exec()
        .unwrap();

        publish(&lua, snapshot());

        assert!(lua.globals().get::<bool>("seen").unwrap());
        let value: Table = lua.load("return usage.get()").eval().unwrap();
        assert_eq!(value.get::<String>("provider_id").unwrap(), "anthropic");
        assert_eq!(value.get::<String>("status").unwrap(), "ready");
    }

    #[test]
    fn clear_plugin_removes_only_owned_callbacks() {
        let lua = lua_with_usage(None, "one");
        lua.load("one = 0; usage.on_change(function() one += 1 end)")
            .exec()
            .unwrap();
        let two = create_usage_table(&lua, None, Arc::from("two")).unwrap();
        lua.globals().set("usage_two", two).unwrap();
        lua.load("two = 0; usage_two.on_change(function() two += 1 end)")
            .exec()
            .unwrap();
        lua.app_data_mut::<UsageMirror>()
            .unwrap()
            .clear_plugin("one");

        publish(&lua, snapshot());

        assert_eq!(lua.globals().get::<u32>("one").unwrap(), 0);
        assert_eq!(lua.globals().get::<u32>("two").unwrap(), 1);
    }

    #[test]
    fn fetch_forwards_force_and_returns_typed_snapshot() {
        let (tx, rx) = flume::unbounded();
        let lua = lua_with_usage(Some(tx), "test");
        let expected = snapshot();
        let reply = expected.clone();
        let checker = std::thread::spawn(move || {
            let UiAction::UsageFetch { force, reply_tx } = rx.recv().unwrap() else {
                panic!("expected usage fetch");
            };
            assert!(force);
            reply_tx.send(Ok(reply)).unwrap();
        });

        let (value, error): (Table, Option<String>) = smol::block_on(
            lua.load("return usage.fetch({ force = true })")
                .eval_async(),
        )
        .unwrap();
        checker.join().unwrap();

        assert_eq!(error, None);
        assert_eq!(value.get::<String>("provider").unwrap(), expected.provider);
    }

    #[test]
    fn headless_get_is_nil_and_fetch_returns_no_ui_error() {
        let lua = lua_with_usage(None, "test");
        assert!(
            lua.load("return usage.get()")
                .eval::<Value>()
                .unwrap()
                .is_nil()
        );
        let (value, error): (Value, Option<String>) =
            smol::block_on(lua.load("return usage.fetch()").eval_async()).unwrap();
        assert!(value.is_nil());
        assert_eq!(error.as_deref(), Some(NO_UI_ERR));
    }
}
