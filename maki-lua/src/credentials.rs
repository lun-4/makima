//! `ctx.credentials`: a Lua provider's stored credentials and the shared,
//! cross-process refresh transaction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Shared;
use futures_lite::future;
use maki_agent::cancel::CancelToken;
use maki_lua_macro::{lua_class, lua_fn};
use maki_storage::StateDir;
use maki_storage::auth::{
    delete_provider_credentials, load_credentials, lock_tokens, save_credentials,
};
use mlua::{
    Function, Lua, LuaSerdeExt, MultiValue, Result as LuaResult, UserDataRef, Value as LuaValue,
};
use serde_json::Value;
use smol::{Timer, unblock};
use tracing::warn;

use crate::api::env::host_state_dir;
use crate::api::util::pair::{Pair, err_pair, pair, try_pair};
use crate::provider::{HOST_STOPPED, ProviderJob, serialize_options};
use crate::runtime::{Request, RequestTx, strip_traceback};

/// Bounds a refresh whose Lua function never returns, which would otherwise
/// block every later refresh of that provider.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(120);
const REFRESH_TIMEOUT_ERR: &str = "credential refresh timed out";

type RefreshResult = Result<Option<Value>, String>;
type Flight = Shared<oneshot::Receiver<RefreshResult>>;

/// In-flight refreshes by provider id. Only the Lua thread touches it.
#[derive(Default)]
pub(crate) struct RefreshFlights(HashMap<Arc<str>, Flight>);

/// Always addresses the credentials of the provider whose callback received it.
pub(crate) struct Credentials {
    pub(crate) id: Arc<str>,
}

fn location(lua: &Lua, this: &Credentials) -> Result<(StateDir, Arc<str>), String> {
    Ok((host_state_dir(lua)?, Arc::clone(&this.id)))
}

/// Read the stored credentials.
///
/// @return (table?, string?) The stored table, nil when there is none or it does not parse, or nil plus an error.
/// @example
/// local saved = ctx.credentials:read()
#[lua_fn]
async fn read(lua: Lua, this: UserDataRef<Credentials>) -> LuaResult<Pair<LuaValue>> {
    let (dir, id) = try_pair!(location(&lua, &this));
    match unblock(move || load_credentials(&dir, &id)).await {
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
async fn write(lua: Lua, this: UserDataRef<Credentials>, value: LuaValue) -> LuaResult<Pair<bool>> {
    let value: Value = lua.from_value(value)?;
    let (dir, id) = try_pair!(location(&lua, &this));
    Ok(pair(
        unblock(move || save_credentials(&dir, &id, &value).map(|()| true)).await,
    ))
}

/// Delete the stored credentials.
///
/// @return (boolean?, string?) true if a file was removed, false if there was none, or nil plus an error.
/// @example
/// ctx.credentials:delete()
#[lua_fn]
async fn delete(lua: Lua, this: UserDataRef<Credentials>) -> LuaResult<Pair<bool>> {
    let (dir, id) = try_pair!(location(&lua, &this));
    Ok(pair(
        unblock(move || delete_provider_credentials(&dir, &id)).await,
    ))
}

/// Refresh the stored credentials as one transaction. {fn}`(current)` runs
/// with a cross-process lock held and `current` reread from disk after the
/// lock was taken. It returns a new table to store, nil to keep `current`
/// (another process already refreshed), or nil plus an error.
///
/// Concurrent refreshes of one provider share a single call of {fn}. The
/// call runs on its own and is not cancelled with the caller, so a rotated
/// token that the server already issued is always written.
///
/// @param fn function `fn(current)` returning the table to store.
/// @return (table?, string?) The stored table after the refresh, or nil plus an error.
/// @example
/// local tokens, err = ctx.credentials:refresh(function(current)
///   if current and current.expires > maki.time.now().secs * 1000 + 60000 then return nil end
///   return exchange(current.refresh)
/// end)
#[lua_fn]
async fn refresh(
    lua: Lua,
    this: UserDataRef<Credentials>,
    r#fn: Function,
) -> LuaResult<Pair<LuaValue>> {
    let id = Arc::clone(&this.id);
    drop(this);
    let existing = lua
        .app_data_ref::<RefreshFlights>()
        .and_then(|flights| flights.0.get(&id).cloned());
    let flight = match existing {
        Some(flight) => flight,
        None => try_pair!(start_refresh(&lua, id, r#fn)),
    };
    match flight.await.unwrap_or_else(|_| Err(HOST_STOPPED.into())) {
        Ok(Some(value)) => Ok((Some(lua.to_value_with(&value, serialize_options())?), None)),
        Ok(None) => Ok((None, None)),
        Err(error) => Ok(err_pair(error)),
    }
}

/// Runs the refresh as its own uncancellable job, so dropping the caller
/// cannot lose a token the server already rotated.
fn start_refresh(lua: &Lua, id: Arc<str>, refresh: Function) -> Result<Flight, String> {
    let requests = lua
        .app_data_ref::<RequestTx>()
        .map(|tx| tx.0.clone())
        .ok_or(HOST_STOPPED)?;
    let (reply, flight) = oneshot::channel();
    let flight = flight.shared();
    let job_id = Arc::clone(&id);
    let job: ProviderJob = Box::new(move |lua| {
        Box::pin(async move {
            let timeout = async {
                Timer::after(REFRESH_TIMEOUT).await;
                Err(REFRESH_TIMEOUT_ERR.to_owned())
            };
            let result = future::or(run_refresh(&lua, &job_id, refresh), timeout).await;
            if let Some(mut flights) = lua.app_data_mut::<RefreshFlights>() {
                flights.0.remove(&job_id);
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
        flights.0.insert(id, flight.clone());
    }
    Ok(flight)
}

async fn run_refresh(lua: &Lua, id: &Arc<str>, refresh: Function) -> RefreshResult {
    let dir = host_state_dir(lua)?;
    let (lock_dir, lock_id) = (dir.clone(), Arc::clone(id));
    let lock = unblock(move || lock_tokens(&lock_dir, &lock_id)).await;
    if lock.is_none() {
        warn!(provider = %id, "credential lock not acquired, refreshing unlocked");
    }
    let (read_dir, read_id) = (dir.clone(), Arc::clone(id));
    let current = unblock(move || load_credentials(&read_dir, &read_id)).await;
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
            return Err(error
                .to_string()
                .unwrap_or_else(|_| error.type_name().to_owned()));
        }
        (None | Some(LuaValue::Nil), _) => return Ok(current),
        (Some(value), _) => lua
            .from_value(value.clone())
            .map_err(|error| error.to_string())?,
    };
    let (save_id, saved) = (Arc::clone(id), refreshed.clone());
    // The server may already have revoked the old token, so a failed save
    // still hands the fresh one to the caller.
    if let Err(error) = unblock(move || save_credentials(&dir, &save_id, &saved)).await {
        warn!(provider = %id, %error, "refreshed credentials not saved");
    }
    drop(lock);
    Ok(Some(refreshed))
}

lua_class! {
    /// The calling provider's stored credentials, `ctx.credentials`.
    ///
    /// It always addresses that provider's own credentials file. The value is
    /// whatever JSON table the provider stores, such as `{ api_key }` or
    /// `{ access, refresh, expires }`.
    "maki.provider.Credentials" => Credentials, CREDENTIALS_DOCS [read, write, delete, refresh]
}
