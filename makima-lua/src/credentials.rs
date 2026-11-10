//! `makima.credentials.Store`: credentials stored for a provider (its ctx's
//! `credentials`) or for a plugin (`makima.credentials.open`), and the shared,
//! cross-process refresh transaction.

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Shared;
use makima_agent::cancel::CancelToken;
use makima_lua_macro::{lua_class, lua_fn, lua_table};
use makima_storage::StateDir;
use makima_storage::auth::{delete_credentials, load_credentials, lock_tokens, save_credentials};
use mlua::{
    Function, Lua, LuaSerdeExt, MultiValue, Result as LuaResult, UserDataRef, Value as LuaValue,
};
use serde_json::Value;
use smol::future::yield_now;
use smol::unblock;
use tracing::warn;

use crate::api::env::host_state_dir;
use crate::api::util::pair::{Pair, pair, try_pair};
use crate::ctx::Ctx;
use crate::provider::{HOST_STOPPED, ProviderJob, serialize_options};
use crate::runtime::{LoadingPlugin, Request, RequestTx, strip_traceback};

const EMPTY_REFRESH: &str = "refresh returned nil with no stored credentials";

/// The error is what the refresh function returned, a string or a table such
/// as `{ status, message, retry_after }`, handed back to every caller as is.
type RefreshResult = Result<Option<Value>, Value>;
type Flight = Shared<oneshot::Receiver<RefreshResult>>;

/// In-flight refreshes by store name. Only the Lua thread touches it.
#[derive(Default)]
pub(crate) struct RefreshFlights(HashMap<Arc<str>, Flight>);

/// One credentials file, named relative to the auth dir: a provider id, or a
/// plugin store under `plugins/`.
pub(crate) struct Store {
    name: Arc<str>,
}

impl Store {
    pub(crate) fn new(name: Arc<str>) -> Self {
        Self { name }
    }
}

fn location(lua: &Lua, this: &Store) -> Result<(StateDir, Arc<str>), String> {
    Ok((host_state_dir(lua)?, Arc::clone(&this.name)))
}

/// Read the stored credentials.
///
/// @return (table?, string?) The stored table, nil when there is none or it does not parse, or nil plus an error.
/// @example
/// local saved = ctx.credentials:read()
#[lua_fn]
async fn read(lua: Lua, this: UserDataRef<Store>) -> LuaResult<Pair<LuaValue>> {
    let (dir, name) = try_pair!(location(&lua, &this));
    match unblock(move || load_credentials(&dir, &name)).await {
        Some(value) => Ok((Some(lua.to_value_with(&value, serialize_options())?), None)),
        None => Ok((None, None)),
    }
}

/// Replace the stored credentials with {value}. The write is atomic and the
/// file is readable only by the user.
///
/// @param value table Any JSON-compatible table.
/// @return (boolean?, string?) true, or nil plus an error.
/// @example
/// local ok, err = ctx.credentials:write({ api_key = key })
#[lua_fn]
async fn write(lua: Lua, this: UserDataRef<Store>, value: LuaValue) -> LuaResult<Pair<bool>> {
    let value: Value = lua.from_value(value)?;
    let (dir, name) = try_pair!(location(&lua, &this));
    Ok(pair(
        unblock(move || save_credentials(&dir, &name, &value).map(|()| true)).await,
    ))
}

/// Delete the stored credentials.
///
/// @return (boolean?, string?) true if a file was removed, false if there was none, or nil plus an error.
/// @example
/// ctx.credentials:delete()
#[lua_fn]
async fn delete(lua: Lua, this: UserDataRef<Store>) -> LuaResult<Pair<bool>> {
    let (dir, name) = try_pair!(location(&lua, &this));
    Ok(pair(unblock(move || delete_credentials(&dir, &name)).await))
}

/// Refresh the stored credentials as one transaction. {fn}`(current)` runs
/// with a cross-process lock held and `current` reread from disk after the
/// lock was taken. It returns a new table to store, nil to keep `current`
/// (another process already refreshed), or nil plus an error.
///
/// Concurrent refreshes of one store share a single call of {fn}. The call
/// runs on its own and is not cancelled with the caller, so a rotated token
/// that the server already issued is always written. During plugin load,
/// which nothing cancels, {fn} runs in the first caller instead.
///
/// @param fn function `fn(current)` returning the table to store.
/// @return (table?, any?) The stored table after the refresh, or nil plus the error {fn} returned, unchanged.
/// @example
/// local tokens, err = ctx.credentials:refresh(function(current)
///   if current and current.expires > makima.time.now().secs * 1000 + 60000 then return nil end
///   return exchange(current.refresh)
/// end)
#[lua_fn]
async fn refresh(
    lua: Lua,
    this: UserDataRef<Store>,
    r#fn: Function,
) -> LuaResult<(LuaValue, LuaValue)> {
    let name = Arc::clone(&this.name);
    drop(this);
    let existing = lua
        .app_data_ref::<RefreshFlights>()
        .and_then(|flights| flights.0.get(&name).cloned());
    let result = match existing {
        Some(flight) => flight.await.unwrap_or_else(|_| Err(HOST_STOPPED.into())),
        None if lua.app_data_ref::<LoadingPlugin>().is_some() => {
            refresh_in_load(&lua, name, r#fn).await
        }
        None => match start_refresh(&lua, name, r#fn) {
            Ok(flight) => flight.await.unwrap_or_else(|_| Err(HOST_STOPPED.into())),
            Err(error) => Err(error.into()),
        },
    };
    let to_lua = |value: &Value| lua.to_value_with(value, serialize_options());
    match result {
        Ok(Some(value)) => Ok((to_lua(&value)?, LuaValue::Nil)),
        Ok(None) => Ok((LuaValue::Nil, LuaValue::Nil)),
        Err(error) => Ok((LuaValue::Nil, to_lua(&error)?)),
    }
}

/// Run {fn} with the store's cross-process lock held, the same
/// lock `refresh` takes, for work that must not overlap another process
/// changing these credentials. Unlike `refresh`, every call runs its own
/// {fn}, nothing is reread or written, and dropping the caller ends {fn}.
///
/// @param fn function Called with no arguments.
/// @return any Whatever {fn} returns.
/// @example
/// local result, err = ctx.credentials:lock(function()
///   return run_login_script()
/// end)
#[lua_fn]
async fn lock(lua: Lua, this: UserDataRef<Store>, r#fn: Function) -> LuaResult<MultiValue> {
    let (dir, name) = location(&lua, &this).map_err(mlua::Error::runtime)?;
    drop(this);
    let lock_name = Arc::clone(&name);
    let lock = unblock(move || lock_tokens(&dir, &lock_name)).await;
    if lock.is_none() {
        warn!(store = %name, "credential lock not acquired, running unlocked");
    }
    let values = r#fn.call_async::<MultiValue>(()).await;
    drop(lock);
    values
}

/// A load holds the request loop that runs refresh jobs, so a job queued now
/// would wait for the load that waits for it. The first caller runs
/// {refresh} itself instead, and concurrent callers wait on its flight as
/// they would on a job's. Nothing cancels a load midway, so the rotated
/// token is still always written.
async fn refresh_in_load(lua: &Lua, name: Arc<str>, refresh: Function) -> RefreshResult {
    let (reply, flight) = oneshot::channel();
    if let Some(mut flights) = lua.app_data_mut::<RefreshFlights>() {
        flights.0.insert(Arc::clone(&name), flight.shared());
    }
    // The refresh can finish within one poll when its file work is quick, so
    // yield once: callers started alongside this one, such as by
    // `makima.async.gather`, then find the flight instead of running {refresh}.
    yield_now().await;
    let result = run_refresh(lua, &name, refresh).await;
    if let Some(mut flights) = lua.app_data_mut::<RefreshFlights>() {
        flights.0.remove(&name);
    }
    let _ = reply.send(result.clone());
    result
}

/// Runs the refresh as its own uncancellable job, so dropping the caller
/// cannot lose a token the server already rotated. Nothing bounds the whole
/// run for the same reason: the token request is bounded by `ctx.send`'s
/// timeouts instead.
fn start_refresh(lua: &Lua, name: Arc<str>, refresh: Function) -> Result<Flight, String> {
    let requests = lua
        .app_data_ref::<RequestTx>()
        .map(|tx| tx.0.clone())
        .ok_or(HOST_STOPPED)?;
    let (reply, flight) = oneshot::channel();
    let flight = flight.shared();
    let job_name = Arc::clone(&name);
    let job: ProviderJob = Box::new(move |lua| {
        Box::pin(async move {
            let result = run_refresh(&lua, &job_name, refresh).await;
            if let Some(mut flights) = lua.app_data_mut::<RefreshFlights>() {
                flights.0.remove(&job_name);
            }
            let _ = reply.send(result);
        })
    });
    requests
        .send(Request::Provider {
            job,
            cancel: CancelToken::none(),
        })
        .map_err(|_| HOST_STOPPED)?;
    if let Some(mut flights) = lua.app_data_mut::<RefreshFlights>() {
        flights.0.insert(name, flight.clone());
    }
    Ok(flight)
}

async fn run_refresh(lua: &Lua, name: &Arc<str>, refresh: Function) -> RefreshResult {
    let dir = host_state_dir(lua)?;
    let (lock_dir, lock_name) = (dir.clone(), Arc::clone(name));
    let lock = unblock(move || lock_tokens(&lock_dir, &lock_name)).await;
    if lock.is_none() {
        warn!(store = %name, "credential lock not acquired, refreshing unlocked");
    }
    let (read_dir, read_name) = (dir.clone(), Arc::clone(name));
    let current = unblock(move || load_credentials(&read_dir, &read_name)).await;
    let current_lua = match &current {
        Some(value) => lua.to_value_with(value, serialize_options()),
        None => Ok(LuaValue::Nil),
    }
    .map_err(|error| error.to_string())?;
    let values = refresh
        .call_async::<MultiValue>(current_lua)
        .await
        .map_err(|error| strip_traceback(&error))?;
    let refreshed: Value = match (values.front(), values.get(1)) {
        (None | Some(LuaValue::Nil), Some(error)) if !error.is_nil() => {
            return Err(lua
                .from_value(error.clone())
                .unwrap_or_else(|_| error.type_name().into()));
        }
        (None | Some(LuaValue::Nil), _) => {
            return current.map_or_else(|| Err(EMPTY_REFRESH.into()), |value| Ok(Some(value)));
        }
        (Some(value), _) => lua
            .from_value(value.clone())
            .map_err(|error| error.to_string())?,
    };
    let (save_name, saved) = (Arc::clone(name), refreshed.clone());
    // The server may already have revoked the old token, so a failed save
    // still hands the fresh one to the caller.
    if let Err(error) = unblock(move || save_credentials(&dir, &save_name, &saved)).await {
        warn!(store = %name, %error, "refreshed credentials not saved");
    }
    drop(lock);
    Ok(Some(refreshed))
}

/// Open plugin store {key} of {ctx}: a credentials file only that ctx and
/// the ctxs it was narrowed from reach. A provider ctx opens its plugin
/// ctx's stores.
///
/// @param ctx makima.Ctx The plugin's ctx, or a provider ctx.
/// @param key string ASCII letters, digits, `-`, `_` or `.`, not starting with `.`.
/// @return (makima.credentials.Store) The store.
/// @example
/// local account = makima.credentials.open(ctx, "account")
/// local saved = account:read()
#[lua_fn]
fn open(_lua: &Lua, ctx: UserDataRef<Ctx>, key: String) -> LuaResult<Store> {
    ctx.plugin()
        .store_name(&key)
        .map(Store::new)
        .map_err(mlua::Error::runtime)
}

lua_class! {
    /// A credentials file: a provider's (its ctx's `credentials`) or a
    /// plugin's (`makima.credentials.open`). The value is whatever JSON table
    /// is stored, such as `{ api_key }` or `{ access, refresh, expires }`.
    "makima.credentials.Store" => Store, STORE_DOCS [read, write, delete, refresh, lock]
}

lua_table! {
    /// Credential stores for plugins. A provider's own credentials are its
    /// ctx's `credentials`; state a plugin shares across its providers, such
    /// as one sign-in, goes in a plugin store.
    "makima.credentials" => pub(crate) fn create_credentials_table(), DOCS [open]
}
