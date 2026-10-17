use std::collections::HashMap;
use std::sync::Arc;

use maki_agent::actor::ActorLifecycle;
use maki_agent::{ActorEvent, AgentId, AgentManagerHandle};
use maki_lua_macro::lua_fn;
use mlua::{Lua, Table, UserDataRef};

use super::authority::{
    authorize, context_graph, host_agent, lookup, scope, task_authority, validate_host_root,
    validate_unmanaged,
};
use super::{AgentOwners, AgentRuntime, AgentSessions, LuaAgent, LuaAgentRef, session_state};
use crate::agent_errors::{
    DELEGATED_FOCUS, FOREIGN_AGENT_REFERENCE, NO_FOCUSED_AGENT, NO_PLUGIN_AUTHORITY,
    UNMANAGED_AUTHORITY_INACTIVE,
};
use crate::api::agent::{SessionControl, build_session};
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::host_root::UNSUPPORTED_OPERATION_ERROR;
use crate::orchestration::resolve_target;
use crate::runtime::{TaskHandle, current_plugin, in_delegated_tool, lock_cell};

/// Read the agent associated with an explicit invocation context.
/// @param ctx LuaCtx Invocation context.
/// @return (Agent?, string?) Nonowning agent handle.
#[lua_fn]
pub(crate) fn current(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgent>> {
    if let Some(services) = try_pair!(ctx.host_root(lua)) {
        return Ok((Some(host_agent(services)), None));
    }
    if let Some(context) = ctx.agent().filter(|agent| agent.managed_turn.is_none()) {
        let session = lua.app_data_ref::<AgentSessions>().and_then(|sessions| {
            sessions
                .lock()
                .get(&ctx.subject?)
                .filter(|session| {
                    session.state.params.get().is_some_and(|params| {
                        context.execution_agent_id == Some(session.agent_id)
                            || Arc::ptr_eq(&params.ledger, &context.ledger)
                    })
                })
                .map(|session| {
                    (
                        session.agent_id,
                        session.actor.clone(),
                        session.state.clone(),
                    )
                })
        });
        let (id, actor, state) = try_pair!(session.ok_or("execution subject is unavailable"));
        let (plugin, generation) = try_pair!(current_plugin(lua).ok_or(NO_PLUGIN_AUTHORITY));
        let runtime = AgentRuntime::unmanaged(
            lua,
            actor,
            state.clone(),
            context.subagent_cancels.clone(),
            plugin,
            generation,
        );
        let agent = LuaAgent::without_graph(runtime, Some(id), Some(state));
        try_pair!(authorize(lua, &ctx, &agent));
        return Ok((Some(agent), None));
    }
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
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
        manager: AgentRuntime::Managed(held.manager()),
        id: Some(held.id()),
        held: Some(held),
    })
}

/// Read the root agent of this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (AgentRef?, string?) Visibility-only root reference.
#[lua_fn]
fn root(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgentRef>> {
    if let Some(services) = try_pair!(ctx.host_root(lua)) {
        let reference = LuaAgentRef {
            manager: AgentRuntime::host(services),
            id: None,
            held: None,
        };
        return Ok((Some(reference), None));
    }
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    let id = try_pair!(manager.root_id());
    if let Ok(trusted) = ctx.trusted(lua) {
        try_pair!(trusted.validate_target(lua, &try_pair!(manager.lookup(id))));
    }
    Ok((
        Some(LuaAgentRef {
            held: Some(try_pair!(manager.lookup(id))),
            manager: AgentRuntime::Managed(manager),
            id: Some(id),
        }),
        None,
    ))
}

/// Resolve an AgentRef with current control authority. String IDs are not accepted.
/// @param ctx LuaCtx Invocation context.
/// @param reference AgentRef Visibility-only reference.
/// @return (Agent?, string?) Nonowning handle or error.
#[lua_fn]
fn get(
    lua: &Lua,
    ctx: UserDataRef<LuaCtx>,
    reference: UserDataRef<LuaAgentRef>,
) -> mlua::Result<Pair<LuaAgent>> {
    match reference.manager {
        AgentRuntime::HostRoot { .. } => {
            let agent = LuaAgent::without_graph(reference.manager.clone(), None, None);
            try_pair!(validate_host_root(lua, &ctx, &agent));
            return Ok((Some(agent), None));
        }
        AgentRuntime::Unmanaged { .. } => {
            let id = try_pair!(reference.id.ok_or(UNSUPPORTED_OPERATION_ERROR));
            let state = session_state(lua, id);
            let agent = LuaAgent::without_graph(reference.manager.clone(), reference.id, state);
            try_pair!(authorize(lua, &ctx, &agent));
            return Ok((Some(agent), None));
        }
        AgentRuntime::Managed(_) => {}
    }
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    if ctx.trusted(lua).is_err() && !manager.same_manager(try_pair!(reference.manager.managed())) {
        return Ok(err_pair(FOREIGN_AGENT_REFERENCE));
    }
    let agent = try_pair!(lookup(
        try_pair!(reference.manager.managed()).clone(),
        try_pair!(reference.id.ok_or(UNSUPPORTED_OPERATION_ERROR)),
        lua
    ));
    try_pair!(authorize(lua, &ctx, &agent));
    Ok((Some(agent), None))
}

/// Whether `candidate` is `agent` or one of its ancestors.
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

/// List accessible live agents in this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Array of visibility-only AgentRef values. Visibility does not grant control. Non-TUI hosts include their root and accessible live plugin-owned standalone children.
#[lua_fn]
fn list(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    if let Some(services) = try_pair!(ctx.host_root(lua)) {
        let table = lua.create_table()?;
        table.push(LuaAgentRef {
            manager: AgentRuntime::host(services),
            id: None,
            held: None,
        })?;
        if let Some(sessions) = lua.app_data_ref::<AgentSessions>() {
            for session in sessions.lock().values() {
                let SessionControl::Unmanaged { parent_cancels, .. } = &session.control else {
                    continue;
                };
                let Some((plugin, generation)) = current_plugin(lua) else {
                    continue;
                };
                if lua
                    .app_data_ref::<AgentOwners>()
                    .is_none_or(|owners| owners.0.get(&session.agent_id) != Some(&plugin))
                {
                    continue;
                }
                let runtime = AgentRuntime::unmanaged(
                    lua,
                    session.actor.clone(),
                    session.state.clone(),
                    parent_cancels.clone(),
                    plugin,
                    generation,
                );
                if validate_unmanaged(lua, &ctx, &runtime).is_ok()
                    && session.actor.snapshot().lifecycle == ActorLifecycle::Open
                {
                    table.push(LuaAgentRef {
                        manager: runtime,
                        id: Some(session.agent_id),
                        held: None,
                    })?;
                }
            }
        }
        return Ok((Some(table), None));
    }
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
    let trusted = ctx.trusted(lua).ok();
    if trusted.is_none() {
        try_pair!(scope(lua, &ctx, id));
    }
    let table = lua.create_table()?;
    for node in manager.snapshot() {
        if let Some(trusted) = trusted {
            let Ok(target) = manager.lookup(node.agent_id) else {
                continue;
            };
            if trusted.validate_target(lua, &target).is_err() {
                continue;
            }
        } else if !try_pair!(is_self_or_ancestor(&manager, id, node.agent_id))
            && scope(lua, &ctx, node.agent_id).is_err()
        {
            continue;
        }
        table.push(LuaAgentRef {
            held: Some(try_pair!(manager.lookup(node.agent_id))),
            manager: AgentRuntime::Managed(manager.clone()),
            id: Some(node.agent_id),
        })?;
    }
    Ok((Some(table), None))
}

/// Spawn a child with a copy of its parent's configuration.
///
/// Omitted tools inherit the parent's capability-filtered request tools and
/// cannot widen the parent's audience or mode policy. `tools = {}` excludes
/// all request tools. Explicit tools select the child's request definitions.
/// Unlike spawn, the compatibility Session API defaults to no request tools.
/// Unmanaged children use the current host task's authority, not a managed graph.
///
/// @param ctx LuaCtx Active agent context or trusted pinned parent.
/// @param opts table Optional creation fields:
///   model (string|table): a spec or `{spec, thinking, fast}` selection, validated
///     like `Agent:set_model`; omitted fields inherit, thinking is capped at the
///     parent's level, and `Agent:model` results can be passed as is.
///   system (string): custom text appended after the admitted mode's prompt contribution; defaults to empty and does not inherit the parent's full system prompt.
///   tools (array): request definitions; omitted inherits permitted parent tools.
///   local_tools (table): name-keyed Lua tool specs with description, input_schema,
///     handler(input), and optional capture_input for successful input capture.
///   name (string): display name. audience (string): defaults to "general_sub";
///     the child cannot widen the parent's capability ceiling.
///   mcp (boolean): defaults to true, subject to inherited restrictions.
///   output_schema (table): validated before creation; requires a valid final
///     report each turn through the Rust structured-output reporting tool.
///   silent (boolean): defaults to false; suppresses parent relay and delivery.
///   show (boolean|string): post a row for the child in its parent's chat;
///     a string adds a description. Defaults to true for trusted parents and
///     false otherwise. Silent children and children spawned by a tool call,
///     which already have its row, cannot be shown.
///   auto_deliver (boolean): defaults to true; managed children notify their
///     recorded parent once per settled turn. A wait in the
///     parent's own turn returns the result directly and withdraws the
///     queued notification.
///   semaphore (maki.async.Semaphore): unmanaged concurrency limit; managed
///     children use their manager's capacity instead.
///   inherit_provider (boolean): defaults to false; reuse the caller's provider
///     and model, ignoring model.spec, when true.
///   Any other option key is an error.
/// @return (Agent?, string?) Nonowning child handle. Garbage collection does not close it.
#[lua_fn]
async fn spawn(lua: Lua, ctx: UserDataRef<LuaCtx>, opts: Table) -> mlua::Result<Pair<LuaAgent>> {
    try_pair!(ctx.validate_origin(&lua));
    let task = lua
        .app_data_ref::<TaskHandle>()
        .map(|task| Arc::downgrade(&task));
    if let Ok(host) = ctx.trusted_host_root(&lua) {
        try_pair!(ctx.host_root(&lua));
        if host.template.is_none() {
            return Ok(err_pair(UNSUPPORTED_OPERATION_ERROR));
        }
    } else if ctx.trusted(&lua).is_ok()
        || ctx
            .agent()
            .is_some_and(|agent| agent.managed_turn.is_some())
    {
        let (_, id) = try_pair!(context_graph(&lua, &ctx));
        try_pair!(scope(&lua, &ctx, id));
    } else {
        let context = try_pair!(ctx.agent().ok_or_else(|| ctx.cap_err("orchestration")));
        let handle = try_pair!(task_authority(&lua));
        let cell = lock_cell(&handle);
        if !cell.is_live() || context.cancel.is_cancelled() || cell.managed_turn.is_some() {
            return Ok(err_pair(UNMANAGED_AUTHORITY_INACTIVE));
        }
    }
    let owner = ctx
        .trusted(&lua)
        .ok()
        .map(|trusted| Arc::clone(&trusted.plugin))
        .or_else(|| current_plugin(&lua).map(|(plugin, _)| plugin));
    let (session, error) = build_session(lua.clone(), ctx, opts, true).await?;
    let Some(session) = session else {
        return Ok((None, error));
    };
    let id = session.agent_id;
    let state = Arc::clone(&session.state);
    let agent = match &session.control {
        SessionControl::Managed { agent, .. } => {
            let mut handle = try_pair!(lookup(agent.manager(), id, &lua));
            handle.state = Some(state);
            handle
        }
        SessionControl::Unmanaged { parent_cancels, .. } => {
            let task = try_pair!(task.ok_or("no captured task authority"));
            let captured = try_pair!(task.upgrade().ok_or("unmanaged owner expired"));
            let active = try_pair!(task_authority(&lua));
            let cell = lock_cell(&active);
            if !Arc::ptr_eq(&active, &captured) || !cell.is_live() || cell.managed_turn.is_some() {
                return Ok(err_pair("unmanaged owner expired during preparation"));
            }
            drop(cell);
            let (plugin, generation) = try_pair!(current_plugin(&lua).ok_or(NO_PLUGIN_AUTHORITY));
            let runtime = AgentRuntime::unmanaged(
                &lua,
                Arc::clone(&session.actor),
                Arc::clone(&state),
                Arc::clone(parent_cancels),
                plugin,
                generation,
            );
            LuaAgent::without_graph(runtime, Some(id), Some(state))
        }
    };
    if let Some(owner) = owner {
        let live_ids = lua
            .app_data_ref::<AgentSessions>()
            .map(|sessions| sessions.lock().keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        if let Some(mut owners) = lua.app_data_mut::<AgentOwners>() {
            owners.0.retain(|id, _| live_ids.contains(id));
            owners.0.insert(id, owner);
        } else {
            lua.set_app_data(AgentOwners(HashMap::from([(id, owner)])));
        }
    }
    let actor = Arc::clone(&session.actor);
    let events = actor.subscribe_events();
    let existing = lua
        .app_data_ref::<AgentSessions>()
        .map(|sessions| sessions.clone());
    let sessions = existing.unwrap_or_else(|| {
        let sessions = AgentSessions::default();
        lua.set_app_data(sessions.clone());
        sessions
    });
    sessions.lock().insert(id, session);
    let snapshot = actor.snapshot();
    if snapshot.lifecycle != ActorLifecycle::Open && snapshot.active_turn.is_none() {
        sessions.retire(id);
        return Ok((Some(agent), None));
    }
    smol::spawn(async move {
        while let Ok(event) = events.recv_async().await {
            if matches!(event, ActorEvent::Close { .. }) {
                sessions.retire(id);
                break;
            }
        }
    })
    .detach();
    Ok((Some(agent), None))
}
