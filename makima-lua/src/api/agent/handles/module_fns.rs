use std::sync::Arc;

use makima_agent::actor::ActorLifecycle;
use makima_agent::{ActorEvent, AgentId, AgentManagerHandle};
use makima_lua_macro::lua_fn;
use mlua::{Lua, Table, UserDataRef, Value as LuaValue};

use super::authority::{authorize, context_graph, lookup, observe, scope};
use super::{AgentOwners, LuaAgent, LuaAgentRef};
use crate::agent_errors::{DELEGATED_FOCUS, FOREIGN_AGENT_REFERENCE, NO_FOCUSED_AGENT};
use crate::api::agent::{LuaSession, build_session};
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::orchestration::resolve_target;
use crate::runtime::{current_plugin, in_delegated_tool};

/// The agent of an explicit invocation context, for `ctx:agent()`.
pub(crate) fn current(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgent>> {
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
    try_pair!(manager.reference(id).validate_live());
    try_pair!(scope(lua, &ctx, id));
    Ok((Some(try_pair!(lookup(manager, id, lua))), None))
}

/// Resolves the agent that has UI focus now, pinned against later focus changes.
pub(crate) fn focused_agent_ref(lua: &Lua) -> Result<LuaAgentRef, String> {
    if in_delegated_tool(lua) {
        return Err(DELEGATED_FOCUS.into());
    }
    let held = resolve_target(lua, None)?.ok_or(NO_FOCUSED_AGENT)?.target;
    Ok(LuaAgentRef {
        manager: held.manager(),
        id: held.id(),
        held: Some(held),
    })
}

/// Read the root agent of this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (AgentRef?, string?) Visibility-only root reference.
#[lua_fn]
fn root(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgentRef>> {
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    let id = try_pair!(manager.root_id());
    let reference = manager.reference(id);
    let agent = try_pair!(lookup(manager.clone(), id, lua));
    if ctx.trusted(lua).is_ok() {
        try_pair!(observe(lua, &ctx, &agent));
    }
    Ok((
        Some(LuaAgentRef {
            held: Some(reference),
            manager,
            id,
        }),
        None,
    ))
}

/// Resolve an agent reference or durable agent ID with current control authority.
/// Resolution does not reactivate a dormant agent. Saved IDs can be resolved in
/// the restored session graph.
/// @param ctx LuaCtx Invocation context.
/// @param reference AgentRef|string Visibility-only reference or stable agent ID.
/// @return (Agent?, string?) Nonowning handle or error.
#[lua_fn]
fn get(lua: &Lua, ctx: UserDataRef<LuaCtx>, reference: LuaValue) -> mlua::Result<Pair<LuaAgent>> {
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    let (manager, id) = match reference {
        LuaValue::String(id) => (manager, try_pair!(id.to_str()?.parse::<AgentId>())),
        LuaValue::UserData(reference) => {
            let reference = reference.borrow::<LuaAgentRef>()?;
            if ctx.trusted(lua).is_err() && !manager.same_manager(&reference.manager) {
                return Ok(err_pair(FOREIGN_AGENT_REFERENCE));
            }
            (reference.manager.clone(), reference.id)
        }
        value => {
            return Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: "AgentRef or string".to_owned(),
                message: None,
            });
        }
    };
    let agent = try_pair!(lookup(manager, id, lua));
    if ctx.trusted(lua).is_ok() {
        try_pair!(observe(lua, &ctx, &agent));
    } else {
        try_pair!(authorize(lua, &ctx, &agent));
    }
    Ok((Some(agent), None))
}

fn is_self_or_ancestor(
    manager: &AgentManagerHandle,
    mut agent: AgentId,
    candidate: AgentId,
) -> Result<bool, String> {
    loop {
        if agent == candidate {
            return Ok(true);
        }
        match manager
            .node(agent)
            .map_err(|error| error.to_string())?
            .parent_id
        {
            Some(parent) => agent = parent,
            None => return Ok(false),
        }
    }
}

/// List accessible resident and dormant agents in this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Array of visibility-only AgentRef values. Visibility does not grant control.
#[lua_fn]
fn list(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
    let trusted = ctx.trusted(lua).ok();
    if trusted.is_none() {
        try_pair!(scope(lua, &ctx, id));
    }
    let table = lua.create_table()?;
    for node in manager.snapshot() {
        if trusted.is_some() {
            let agent = try_pair!(lookup(manager.clone(), node.agent_id, lua));
            if observe(lua, &ctx, &agent).is_err() {
                continue;
            }
        } else if !try_pair!(is_self_or_ancestor(&manager, id, node.agent_id))
            && scope(lua, &ctx, node.agent_id).is_err()
        {
            continue;
        }
        table.push(LuaAgentRef {
            held: Some(manager.reference(node.agent_id)),
            manager: manager.clone(),
            id: node.agent_id,
        })?;
    }
    Ok((Some(table), None))
}

/// Spawn a child with a copy of its parent's configuration.
///
/// Omitted tools inherit the parent's capability-filtered request tools and
/// cannot widen the parent's audience or mode policy. `tools = {}` excludes
/// all request tools. Explicit tools select the child's request definitions.
/// Requires an active managed turn or a trusted manager-backed parent.
///
/// @param ctx LuaCtx Active agent context or trusted pinned parent.
/// @param opts table Optional creation fields:
///   model (string|table): a spec or `{spec, thinking, fast}` selection, validated
///     like `Agent:set_model`; omitted fields inherit, thinking is capped at the
///     parent's level, and `Agent:config(ctx).model` results can be passed as is.
///   system (string): custom text appended after the admitted mode's prompt contribution; defaults to empty and does not inherit the parent's full system prompt.
///   tools (array): request definitions; omitted inherits permitted parent tools.
///   local_tools (table): name-keyed Lua tool specs with description, input_schema,
///     handler(input), and optional capture_input for successful input capture.
///   name (string): display name. audience (string): defaults to the parent's audience;
///     the child cannot widen the parent's capability ceiling.
///   mcp (boolean): defaults to true, subject to inherited restrictions.
///   output_schema (table): validated before creation; requires a valid final
///     report each turn through the Rust structured-output reporting tool.
///   silent (boolean): defaults to false; suppresses parent relay and delivery.
///   show (boolean|string): post a clickable row for the child in its parent's chat;
///     clicking it opens the child's chat once the child starts a turn.
///     A string adds a description. Defaults to true for trusted parents and
///     false otherwise. Silent children and children spawned by a tool call,
///     which already have a tool row, cannot be shown.
///   auto_deliver (boolean): defaults to true; children notify their recorded
///     parent once per settled turn. A wait in the parent's own turn returns
///     the result directly and withdraws the queued notification.
///   inherit_provider (boolean): defaults to false; reuse the caller's provider
///     and model, ignoring model.spec, when true.
///   Any other option key is an error.
/// @return (Agent?, string?) Nonowning child handle. Garbage collection does not close it.
#[lua_fn]
async fn spawn(lua: Lua, ctx: UserDataRef<LuaCtx>, opts: Table) -> mlua::Result<Pair<LuaAgent>> {
    let (_, parent) = try_pair!(context_graph(&lua, &ctx));
    try_pair!(scope(&lua, &ctx, parent));
    let owner = ctx
        .trusted(&lua)
        .ok()
        .map(|trusted| Arc::clone(&trusted.plugin))
        .or_else(|| current_plugin(&lua).map(|(plugin, _)| plugin));
    let (session, error) = build_session(lua.clone(), ctx, opts).await?;
    let Some(session) = session else {
        return Ok((None, error));
    };
    let id = session.agent_id;
    let agent = try_pair!(lookup(session.agent.manager(), id, &lua));
    retain_session(&lua, session, owner);
    Ok((Some(agent), None))
}

pub(in crate::api::agent) fn retain_session(
    lua: &Lua,
    session: LuaSession,
    owner: Option<Arc<str>>,
) {
    let id = session.agent_id;
    let actor = Arc::clone(&session.actor);
    let events = actor.subscribe_events();
    let identity = actor.identity();
    let existing = lua
        .app_data_ref::<AgentOwners>()
        .map(|owners| owners.clone());
    let owners = existing.unwrap_or_else(|| {
        let owners = AgentOwners::default();
        lua.set_app_data(owners.clone());
        owners
    });
    owners.insert(session, owner);
    let snapshot = actor.snapshot();
    if snapshot.lifecycle != ActorLifecycle::Open && snapshot.active_turn.is_none() {
        owners.retire_incarnation(id, &identity);
        return;
    }
    smol::spawn(async move {
        while let Ok(event) = events.recv_async().await {
            if matches!(event, ActorEvent::Close { .. }) {
                owners.retire_incarnation(id, &identity);
                break;
            }
        }
    })
    .detach();
}
