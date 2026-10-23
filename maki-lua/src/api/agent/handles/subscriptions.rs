use std::sync::Arc;

use maki_lua_macro::{lua_class, lua_fn};
use mlua::{Function, Lua, UserDataRef};

use super::LuaAgent;
use super::authority::authorize;
use crate::agent_autocmd::{
    AGENT_CLOSED_EVENT, AGENT_IDLE_EVENT, CONFIG_CHANGED_EVENT, TURN_END_EVENT, TURN_START_EVENT,
};
use crate::agent_errors::TRUSTED_SUBSCRIPTION_REQUIRED;
use crate::api::autocmd::{AutocmdEntry, AutocmdStore, next_autocmd_id};
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{Pair, try_pair};
use crate::runtime::current_plugin;

const NO_AUTOCMD_STORE: &str = "autocmd store not initialized";

pub(super) struct LuaAgentSubscription {
    id: u64,
}

fn subscribe(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    handler: Function,
    event: &str,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    try_pair!(ctx.trusted(lua).map_err(|_| TRUSTED_SUBSCRIPTION_REQUIRED));
    try_pair!(authorize(lua, ctx, agent));
    let (plugin, _) = try_pair!(current_plugin(lua).ok_or("no active plugin identity"));
    let entry = AutocmdEntry::new(
        next_autocmd_id(),
        handler,
        Arc::clone(&plugin),
        false,
        None,
        Some(agent.listen(lua)),
    );
    let id = entry.id;
    try_pair!(lua.app_data_mut::<AutocmdStore>().ok_or(NO_AUTOCMD_STORE))
        .register(event.into(), entry);
    Ok((Some(LuaAgentSubscription { id }), None))
}

/// Alias for `create_autocmd("TurnStart", { agent = self, callback = handler })`.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(ev, ctx), like any autocmd callback.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_start")]
fn on_turn_start(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, TURN_START_EVENT)
}

/// Alias for `create_autocmd("TurnEnd", { agent = self, callback = handler })`.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(ev, ctx), like any autocmd callback.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_end")]
fn on_turn_end(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, TURN_END_EVENT)
}

/// Alias for `create_autocmd("AgentIdle", { agent = self, callback = handler })`.
/// Idle is an observation, not a reservation against new work. It fires once
/// work settled, so a handler that changes config does not wake itself.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(ev, ctx), like any autocmd callback.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_idle")]
fn on_idle(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, AGENT_IDLE_EVENT)
}

/// Alias for `create_autocmd("ConfigChanged", { agent = self, callback = handler })`.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(ev, ctx), like any autocmd callback.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_config_change")]
fn on_config_change(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, CONFIG_CHANGED_EVENT)
}

/// Alias for `create_autocmd("AgentClosed", { agent = self, callback = handler })`.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(ev, ctx), like any autocmd callback.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_close")]
fn on_close(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, AGENT_CLOSED_EVENT)
}

/// Same as `maki.api.del_autocmd` with this subscription's id: stops future
/// deliveries and cancels running callbacks.
/// @return (boolean?, string?) Success.
#[lua_fn(name = "close")]
fn subscription_close(lua: &Lua, this: &LuaAgentSubscription) -> mlua::Result<Pair<bool>> {
    if let Some(mut store) = lua.app_data_mut::<AutocmdStore>() {
        store.remove(this.id);
    }
    Ok((Some(true), None))
}

lua_class! {
    /// An autocmd filtered to one agent. Garbage collection does not
    /// unsubscribe; close it, or unload the plugin.
    "maki.agent.AgentSubscription" => LuaAgentSubscription, SUBSCRIPTION_DOCS [subscription_close]
}
