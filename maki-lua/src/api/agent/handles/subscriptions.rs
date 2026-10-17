use futures_lite::future::race;
use maki_agent::{ActorEvent, AgentId};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{Function, Lua, UserDataRef};
use serde_json::{Value as JsonValue, json};

use super::authority::{authorize, validate_host_root};
use super::projection::{origin_json, result_json, selection_json};
use super::{AgentRuntime, LuaAgent};
use crate::agent_errors::{NO_ORCHESTRATION_SERVICES, TRUSTED_SUBSCRIPTION_REQUIRED};
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::host_root::UNSUPPORTED_OPERATION_ERROR;
use crate::orchestration::{
    self, AgentEventRequest, TrustedTarget, close_event_callback, event_callback,
    register_event_callback,
};
use crate::runtime::{Request, RequestTx, current_plugin};

pub(super) struct LuaAgentSubscription {
    id: u64,
    stop: flume::Sender<()>,
}

#[derive(Clone, Copy)]
enum EventKind {
    Start,
    End,
    Idle,
    Config,
    Close,
}

fn event_payload(kind: EventKind, event: ActorEvent, target: AgentId) -> Option<JsonValue> {
    let payload = match (kind, event) {
        (
            EventKind::Start,
            ActorEvent::Start {
                agent_id,
                turn_id,
                provenance,
                config,
            },
        ) => json!({
            "agent_id": agent_id.to_string(),
            "turn_id": turn_id.to_string(),
            "origin": origin_json(&provenance),
            "selection": config.map(|commit| selection_json(&commit.config)),
        }),
        (EventKind::End, ActorEvent::End(result)) => result_json(&result),
        (EventKind::Idle, ActorEvent::Idle { agent_id }) => {
            json!({ "agent_id": agent_id.to_string() })
        }
        (EventKind::Config, ActorEvent::Config(commit)) => json!({
            "agent_id": target.to_string(),
            "generation": commit.generation,
            "selection": selection_json(&commit.config),
            "mode": commit.config.mode.id().key(),
        }),
        (
            EventKind::Close,
            ActorEvent::Close {
                agent_id,
                lifecycle,
            },
        ) => json!({
            "agent_id": agent_id.to_string(),
            "lifecycle": format!("{lifecycle:?}").to_lowercase(),
        }),
        _ => return None,
    };
    Some(payload)
}

fn subscribe(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    handler: Function,
    kind: EventKind,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    if matches!(agent.manager, AgentRuntime::HostRoot { .. }) {
        try_pair!(validate_host_root(lua, ctx, agent));
        return Ok(err_pair(UNSUPPORTED_OPERATION_ERROR));
    }
    try_pair!(ctx.trusted(lua).map_err(|_| TRUSTED_SUBSCRIPTION_REQUIRED));
    try_pair!(authorize(lua, ctx, agent));
    let (plugin, _) = try_pair!(current_plugin(lua).ok_or("no active plugin identity"));
    let target = try_pair!(agent.manager.lookup(agent.id));
    let service = try_pair!(orchestration::services(lua).ok_or(NO_ORCHESTRATION_SERVICES));
    let template = try_pair!(service.template(&target));
    let tx = try_pair!(
        lua.app_data_ref::<RequestTx>()
            .map(|tx| tx.0.clone())
            .ok_or("agent event dispatch is unavailable")
    );
    let actor = try_pair!(target.actor());
    let events = actor.subscribe_leased_events();
    let id = register_event_callback(lua, plugin, handler)?;
    let cancel = try_pair!(
        event_callback(lua, id)
            .map(|callback| callback.2)
            .ok_or("event registration expired")
    );
    let (stop, stopped) = flume::bounded(1);
    let keep_subscription_open = stop.clone();
    smol::spawn(async move {
        let _keep_subscription_open = keep_subscription_open;
        loop {
            let next = async { events.recv_async().await.ok() };
            let stopping = async {
                let _ = cancel.race(stopped.recv_async()).await;
                None
            };
            let Some(event) = race(next, stopping).await else {
                break;
            };
            let (event, lease) = event.into_parts();
            let Some(payload) = event_payload(kind, event, target.id()) else {
                continue;
            };
            let request = AgentEventRequest {
                callback_id: id,
                payload,
                lease,
                target: TrustedTarget {
                    target: target.clone(),
                    template: template.clone(),
                },
            };
            if tx.send(Request::AgentEvent(Box::new(request))).is_err() {
                break;
            }
        }
    })
    .detach();
    Ok((Some(LuaAgentSubscription { id, stop }), None))
}

/// Subscribe to future turn admissions. Handlers may overlap.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(payload, ctx) with a fresh trusted scoped context. Payload fields: agent_id, turn_id, origin, selection (normalized {spec, thinking, fast}, when available). origin contains kind ("user", "plugin", or "internal"), plugin?, plugin_generation?, source_agent?, source_turn?. Provenance does not grant authority.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_start")]
fn on_turn_start(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, EventKind::Start)
}

/// Subscribe to exact settled turn results. Handlers may overlap.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(result, ctx). result has the same fields as AgentTurn:result and AgentTurn:wait. Each invocation has a fresh trusted scoped context.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_end")]
fn on_turn_end(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, EventKind::End)
}

/// Subscribe to transitions to no pending turn work.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler({agent_id}, ctx) with a fresh trusted scoped context. Idle is an observation, not a reservation against new work.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_idle")]
fn on_idle(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, EventKind::Idle)
}

/// Subscribe to committed model or mode changes.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler(payload, ctx). Payload fields: agent_id, generation (committed integer), selection (normalized {spec, thinking, fast}), mode (registered name). The context is fresh and trusted.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_config_change")]
fn on_config_change(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, EventKind::Config)
}

/// Subscribe to permanent lifecycle closure.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Called as handler({agent_id, lifecycle}, ctx). lifecycle is "closed" or "shutdown". The context is fresh and trusted.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_close")]
fn on_close(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, EventKind::Close)
}

/// Revoke future deliveries and cancel active handlers.
/// @return (boolean?, string?) Success.
#[lua_fn(name = "close")]
fn subscription_close(lua: &Lua, this: &LuaAgentSubscription) -> mlua::Result<Pair<bool>> {
    close_event_callback(lua, this.id);
    let _ = this.stop.try_send(());
    Ok((Some(true), None))
}

lua_class! {
    /// A revocable subscription. Garbage collection does not unsubscribe.
    /// Close explicitly to cancel callbacks; plugin unload also revokes registration.
    "maki.agent.AgentSubscription" => LuaAgentSubscription, SUBSCRIPTION_DOCS [subscription_close]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flume::TryRecvError;

    use super::*;

    #[test]
    fn subscription_gc_preserves_registration_until_registry_close() {
        let lua = Lua::new();
        crate::orchestration::install_plugin_authority(&lua, Arc::from("gc-owner"));
        let handler = lua.create_function(|_, ()| Ok(())).unwrap();
        let id = register_event_callback(&lua, Arc::from("gc-owner"), handler).unwrap();
        let cancel = event_callback(&lua, id).unwrap().2;
        let (stop, stopped) = flume::bounded(1);
        let _keep_subscription_open = stop.clone();
        lua.globals()
            .set("subscription", LuaAgentSubscription { id, stop })
            .unwrap();
        lua.load("subscription = nil").exec().unwrap();
        lua.gc_collect().unwrap();
        assert_eq!(stopped.try_recv(), Err(TryRecvError::Empty));
        assert!(!cancel.is_cancelled());
        assert!(event_callback(&lua, id).is_some());
        close_event_callback(&lua, id);
        assert!(cancel.is_cancelled());
        assert!(event_callback(&lua, id).is_none());
        lua.gc_collect().unwrap();
    }
}
