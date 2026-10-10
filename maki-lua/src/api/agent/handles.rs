use super::{
    LuaActorState, LuaSession, SessionControl, build_session, derive_result, model_to_lua_table,
};
use crate::api::util::{
    convert::json_to_lua,
    ctx::LuaCtx,
    pair::{Pair, err_pair, try_pair},
};
use maki_agent::{
    AgentActorHandle, AgentId, AgentInput, AgentManagerHandle, AgentNodeSnapshot, AgentRef,
    CurrentManagedTurn, TurnTicket,
    actor::{ActorLifecycle, ActorStatus},
    cancel::CancelMap,
};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{Function, Lua, Table, UserDataRef};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

#[derive(Clone)]
enum AgentRuntime {
    Managed(AgentManagerHandle),
    Unmanaged {
        actor: Arc<AgentActorHandle>,
        parent: Arc<CancelMap<String>>,
        task: Weak<Mutex<crate::runtime::TaskCell>>,
    },
}

impl AgentRuntime {
    fn managed(&self) -> Result<&AgentManagerHandle, String> {
        match self {
            Self::Managed(manager) => Ok(manager),
            Self::Unmanaged { .. } => Err("operation requires a managed graph".into()),
        }
    }

    fn actor(&self, id: AgentId) -> Result<AgentActorHandle, String> {
        match self {
            Self::Managed(manager) => manager.actor(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } if actor.agent_id() == id => Ok(actor.as_ref().clone()),
            Self::Unmanaged { .. } => Err("agent belongs to another runtime".into()),
        }
    }

    fn lookup(&self, id: AgentId) -> Result<AgentRef, String> {
        self.managed()?
            .lookup(id)
            .map_err(|error| error.to_string())
    }

    fn node(&self, id: AgentId) -> Result<AgentNodeSnapshot, String> {
        self.managed()?.node(id).map_err(|error| error.to_string())
    }

    fn close_subtree(&self, id: AgentId) -> Result<(), String> {
        match self {
            Self::Managed(manager) => manager.close_subtree(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } => {
                actor.close();
                Ok(())
            }
        }
    }

    fn cancel_agent(&self, id: AgentId) -> Result<(), String> {
        match self {
            Self::Managed(manager) => manager.cancel_agent(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } => {
                actor.cancel_existing();
                Ok(())
            }
        }
    }

    fn cancel_subtree(&self, id: AgentId) -> Result<(), String> {
        match self {
            Self::Managed(manager) => manager
                .cancel_subtree(id)
                .map_err(|error| error.to_string()),
            Self::Unmanaged { .. } => self.cancel_agent(id),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LuaAgent {
    manager: AgentRuntime,
    id: AgentId,
    state: Option<Arc<LuaActorState>>,
}

#[derive(Clone)]
pub(super) struct LuaAgentRef {
    manager: AgentRuntime,
    id: AgentId,
}

#[derive(Clone)]
struct LuaAgentTurn {
    agent: LuaAgent,
    owner: Option<AgentRef>,
    ticket: TurnTicket,
}

#[derive(Clone, Default)]
struct AgentSessions(Arc<Mutex<HashMap<AgentId, LuaSession>>>);

#[derive(Default)]
struct AgentOwners(HashMap<AgentId, Arc<str>>);

pub(crate) fn close_plugin_agents(lua: &Lua, plugin: &str) {
    let ids = if let Some(mut owners) = lua.app_data_mut::<AgentOwners>() {
        let ids: Vec<_> = owners
            .0
            .iter()
            .filter(|(_, owner)| owner.as_ref() == plugin)
            .map(|(id, _)| *id)
            .collect();
        for id in &ids {
            owners.0.remove(id);
        }
        ids
    } else {
        return;
    };
    let retired = lua.app_data_ref::<AgentSessions>().map(|sessions| {
        let mut sessions = sessions.0.lock().unwrap();
        ids.into_iter()
            .filter_map(|id| sessions.remove(&id))
            .collect::<Vec<_>>()
    });
    drop(retired);
}

fn retire_closed(lua: &Lua) {
    let Some(sessions) = lua.app_data_ref::<AgentSessions>() else {
        return;
    };
    let retired = {
        let mut sessions = sessions.0.lock().unwrap();
        let ids: Vec<_> = sessions
            .iter()
            .filter(|(_, session)| session.actor.snapshot().lifecycle != ActorLifecycle::Open)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| sessions.remove(&id))
            .collect::<Vec<_>>()
    };
    drop(retired);
}

fn validate_unmanaged(lua: &Lua, ctx: &LuaCtx, runtime: &AgentRuntime) -> Result<(), String> {
    ctx.validate_origin(lua)?;
    let AgentRuntime::Unmanaged { parent, task, .. } = runtime else {
        return Err("not an unmanaged agent".into());
    };
    let context = ctx.agent().ok_or_else(|| ctx.cap_err("orchestration"))?;
    let active = lua
        .app_data_ref::<crate::runtime::TaskHandle>()
        .map(|task| Arc::clone(&task))
        .ok_or("no active task authority")?;
    let captured = task.upgrade().ok_or("unmanaged owner expired")?;
    let cell = crate::runtime::lock_cell(&active);
    if !Arc::ptr_eq(&active, &captured)
        || !cell.scope_alive
        || cell.cancel.is_cancelled()
        || context.cancel.is_cancelled()
        || context.managed_turn.is_some()
        || cell.managed_turn.is_some()
        || !Arc::ptr_eq(parent, &context.subagent_cancels)
    {
        return Err("unmanaged agent authority is not active in this invocation".into());
    }
    Ok(())
}

fn context_graph(lua: &Lua, ctx: &LuaCtx) -> Result<(AgentManagerHandle, AgentId), String> {
    ctx.validate_origin(lua)?;
    if let Ok(trusted) = ctx.trusted(lua) {
        return Ok((trusted.target.manager(), trusted.target.id()));
    }
    let current = ctx
        .agent()
        .and_then(|agent| agent.managed_turn.as_ref())
        .ok_or_else(|| "current/root/list require a managed graph".to_owned())?;
    let active = crate::runtime::current_managed_turn(lua)
        .ok_or("managed agent authority is not active in this invocation")?;
    if !current.same_authority(&active) {
        return Err("managed agent authority is not active in this invocation".into());
    }
    current
        .validate_active()
        .map_err(|error| error.to_string())?;
    Ok((current.manager(), current.agent_id()))
}

fn scope(lua: &Lua, ctx: &LuaCtx, target: AgentId) -> Result<Option<CurrentManagedTurn>, String> {
    let (manager, _) = context_graph(lua, ctx)?;
    manager.node(target).map_err(|error| error.to_string())?;
    let expected = ctx.agent().and_then(|agent| agent.managed_turn.as_ref());
    let active = crate::runtime::current_managed_turn(lua);
    match (expected, active) {
        (Some(expected), Some(active)) if expected.same_authority(&active) => {
            active
                .validate_active()
                .map_err(|error| error.to_string())?;
            if target != active.agent_id() {
                active
                    .validate_descendant(target)
                    .map_err(|error| error.to_string())?;
            }
            Ok(Some(active))
        }
        (None, None) if ctx.trusted(lua).is_ok() => {
            let trusted = ctx.trusted(lua)?;
            let target = manager.lookup(target).map_err(|error| error.to_string())?;
            trusted.validate_target(lua, &target)?;
            Ok(None)
        }
        _ => Err("managed agent authority is not active in this invocation".into()),
    }
}

fn authorize(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if matches!(agent.manager, AgentRuntime::Unmanaged { .. }) {
        validate_unmanaged(lua, ctx, &agent.manager)?;
        return Ok(None);
    }
    if let Ok(trusted) = ctx.trusted(lua) {
        let target = agent
            .manager
            .lookup(agent.id)
            .map_err(|error| error.to_string())?;
        trusted.validate_target(lua, &target)?;
        return Ok(None);
    }
    retire_closed(lua);
    let (manager, _) = context_graph(lua, ctx)?;
    if !manager.same_manager(agent.manager.managed()?) {
        return Err("agent belongs to another runtime".into());
    }
    scope(lua, ctx, agent.id)
}

fn destination_template(lua: &Lua, agent: &LuaAgent) -> Result<super::AgentContext, String> {
    if let Some(template) = agent.state.as_ref().and_then(|state| state.template.get()) {
        let mut template = template.clone();
        if let Some(config) = agent
            .manager
            .actor(agent.id)
            .map_err(|error| error.to_string())?
            .effective_config()
        {
            template.model = Arc::new(config.model.clone());
            template.provider = Arc::clone(&config.provider);
            template.opts = maki_providers::RequestOptions {
                thinking: config.thinking,
                fast: config.fast,
            };
            template.workflow = config.workflow;
            template.mode = config.mode.clone();
            template.mode_def = config.mode_def.clone().map(Arc::new);
        }
        return Ok(super::AgentContext::from(&template));
    }
    let target = agent
        .manager
        .lookup(agent.id)
        .map_err(|error| error.to_string())?;
    let service = lua
        .app_data_ref::<crate::orchestration::OrchestrationServicesSlot>()
        .and_then(|slot| slot.0.clone())
        .ok_or("destination template is unavailable")?;
    Ok(super::AgentContext::from(&service.template(&target)?))
}

fn observe(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if matches!(agent.manager, AgentRuntime::Unmanaged { .. }) {
        validate_unmanaged(lua, ctx, &agent.manager)?;
        return Ok(None);
    }
    if let Ok(trusted) = ctx.trusted(lua) {
        trusted.validate_host(lua)?;
        let service = trusted.services(lua)?;
        let node = agent
            .manager
            .node(agent.id)
            .map_err(|error| error.to_string())?;
        let root = agent
            .manager
            .lookup(node.root_id)
            .map_err(|error| error.to_string())?;
        service.validate_target(&root)?;
        return Ok(None);
    }
    retire_closed(lua);
    let (manager, _) = context_graph(lua, ctx)?;
    if !manager.same_manager(agent.manager.managed()?) {
        return Err("agent belongs to another runtime".into());
    }
    let current = crate::runtime::current_managed_turn(lua).ok_or("managed authority expired")?;
    let mut target = agent.id;
    loop {
        if target == current.agent_id() {
            return Ok(Some(current));
        }
        let parent = manager
            .node(target)
            .map_err(|error| error.to_string())?
            .parent_id;
        target = parent.ok_or("agent is not a descendant of the caller")?;
    }
}

fn lookup(manager: AgentManagerHandle, id: AgentId, lua: &Lua) -> Result<LuaAgent, String> {
    manager.actor(id).map_err(|error| error.to_string())?;
    let state = lua.app_data_ref::<AgentSessions>().and_then(|sessions| {
        sessions
            .0
            .lock()
            .unwrap()
            .get(&id)
            .map(|session| Arc::clone(&session.state))
    });
    Ok(LuaAgent {
        manager: AgentRuntime::Managed(manager),
        id,
        state,
    })
}

/// Read the agent associated with an explicit invocation context.
/// @param ctx LuaCtx Invocation context.
/// @return (Agent?, string?) Nonowning agent handle.
#[lua_fn]
pub(super) fn current(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgent>> {
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
    try_pair!(scope(lua, &ctx, id));
    Ok((Some(try_pair!(lookup(manager, id, lua))), None))
}

/// Read the root agent of this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (Agent?, string?) Root agent, subject to caller authority.
#[lua_fn]
pub(super) fn root(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<LuaAgentRef>> {
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    let id = try_pair!(manager.root_id());
    if let Ok(trusted) = ctx.trusted(lua) {
        try_pair!(trusted.validate_target(lua, &try_pair!(manager.lookup(id))));
    }
    Ok((
        Some(LuaAgentRef {
            manager: AgentRuntime::Managed(manager),
            id,
        }),
        None,
    ))
}

/// Look up a live agent by ID in this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @param reference AgentRef Visibility-only reference.
/// @return (Agent?, string?) Nonowning handle or error.
#[lua_fn]
pub(super) fn get(
    lua: &Lua,
    ctx: UserDataRef<LuaCtx>,
    reference: UserDataRef<LuaAgentRef>,
) -> mlua::Result<Pair<LuaAgent>> {
    if matches!(reference.manager, AgentRuntime::Unmanaged { .. }) {
        let agent = LuaAgent {
            manager: reference.manager.clone(),
            id: reference.id,
            state: lua.app_data_ref::<AgentSessions>().and_then(|sessions| {
                sessions
                    .0
                    .lock()
                    .unwrap()
                    .get(&reference.id)
                    .map(|session| Arc::clone(&session.state))
            }),
        };
        try_pair!(authorize(lua, &ctx, &agent));
        return Ok((Some(agent), None));
    }
    let (manager, _) = try_pair!(context_graph(lua, &ctx));
    if ctx.trusted(lua).is_err() && !manager.same_manager(try_pair!(reference.manager.managed())) {
        return Ok(err_pair("agent reference belongs to another runtime"));
    }
    let agent = try_pair!(lookup(
        try_pair!(reference.manager.managed()).clone(),
        reference.id,
        lua
    ));
    try_pair!(authorize(lua, &ctx, &agent));
    Ok((Some(agent), None))
}

/// List accessible live agents in this context's graph.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Array of nonowning handles.
#[lua_fn]
pub(super) fn list(lua: &Lua, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    let (manager, id) = try_pair!(context_graph(lua, &ctx));
    try_pair!(scope(lua, &ctx, id));
    let table = lua.create_table()?;
    for node in manager.snapshot() {
        if ctx.trusted(lua).is_err() {
            let mut caller = id;
            let mut ancestor = false;
            loop {
                if caller == node.agent_id {
                    ancestor = true;
                    break;
                }
                let parent = try_pair!(manager.node(caller)).parent_id;
                let Some(parent) = parent else {
                    break;
                };
                caller = parent;
            }
            if !ancestor && scope(lua, &ctx, node.agent_id).is_err() {
                continue;
            }
        }
        if let Ok(trusted) = ctx.trusted(lua) {
            let Ok(target) = manager.lookup(node.agent_id) else {
                continue;
            };
            if trusted.validate_target(lua, &target).is_err() {
                continue;
            }
        }
        table.push(LuaAgentRef {
            manager: AgentRuntime::Managed(manager.clone()),
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
/// Unlike spawn, the compatibility Session API defaults to no request tools.
/// Unmanaged children use the current host task's authority, not a managed graph.
///
/// @param ctx LuaCtx Active agent context or trusted pinned parent.
/// @param opts table Spawn options; tools is an optional array of request definitions.
/// @return (Agent?, string?) Nonowning child handle. Garbage collection does not close it.
#[lua_fn]
pub(super) async fn spawn(
    lua: Lua,
    ctx: UserDataRef<LuaCtx>,
    opts: Table,
) -> mlua::Result<Pair<LuaAgent>> {
    try_pair!(ctx.validate_origin(&lua));
    let task = lua
        .app_data_ref::<crate::runtime::TaskHandle>()
        .map(|task| Arc::downgrade(&task));
    if ctx.trusted(&lua).is_ok()
        || ctx
            .agent()
            .is_some_and(|agent| agent.managed_turn.is_some())
    {
        let (_, id) = try_pair!(context_graph(&lua, &ctx));
        try_pair!(scope(&lua, &ctx, id));
    } else {
        let context = try_pair!(ctx.agent().ok_or_else(|| ctx.cap_err("orchestration")));
        let handle = try_pair!(
            lua.app_data_ref::<crate::runtime::TaskHandle>()
                .map(|task| Arc::clone(&task))
                .ok_or("no active task authority")
        );
        let cell = crate::runtime::lock_cell(&handle);
        if !cell.scope_alive
            || cell.cancel.is_cancelled()
            || context.cancel.is_cancelled()
            || cell.managed_turn.is_some()
        {
            return Ok(err_pair(
                "unmanaged agent authority is not active in this invocation",
            ));
        }
    }
    let owner = ctx
        .trusted(&lua)
        .ok()
        .map(|trusted| Arc::clone(&trusted.plugin))
        .or_else(|| crate::runtime::current_plugin(&lua).map(|(plugin, _)| plugin));
    let (session, error) = build_session(lua.clone(), ctx, opts, true).await?;
    let Some(session) = session else {
        return Ok((None, error));
    };
    let manager = match &session.control {
        SessionControl::Managed { agent, .. } => AgentRuntime::Managed(agent.manager()),
        SessionControl::Unmanaged { parent_cancels, .. } => {
            let task = try_pair!(task.ok_or("no captured task authority"));
            let captured = try_pair!(task.upgrade().ok_or("unmanaged owner expired"));
            let active = try_pair!(
                lua.app_data_ref::<crate::runtime::TaskHandle>()
                    .map(|task| Arc::clone(&task))
                    .ok_or("no active task authority")
            );
            let cell = crate::runtime::lock_cell(&active);
            if !Arc::ptr_eq(&active, &captured)
                || !cell.scope_alive
                || cell.cancel.is_cancelled()
                || cell.managed_turn.is_some()
            {
                return Ok(err_pair("unmanaged owner expired during preparation"));
            }
            AgentRuntime::Unmanaged {
                actor: Arc::clone(&session.actor),
                parent: Arc::clone(parent_cancels),
                task,
            }
        }
    };
    let agent = LuaAgent {
        manager,
        id: session.agent_id,
        state: Some(Arc::clone(&session.state)),
    };
    if let Some(owner) = owner {
        let live_ids = lua
            .app_data_ref::<AgentSessions>()
            .map(|sessions| {
                sessions
                    .0
                    .lock()
                    .unwrap()
                    .keys()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(mut owners) = lua.app_data_mut::<AgentOwners>() {
            owners.0.retain(|id, _| live_ids.contains(id));
        }
        if lua.app_data_ref::<AgentOwners>().is_none() {
            lua.set_app_data(AgentOwners::default());
        }
        lua.app_data_mut::<AgentOwners>()
            .unwrap()
            .0
            .insert(agent.id, owner);
    }
    let actor = Arc::clone(&session.actor);
    let events = actor.subscribe_events();
    if lua.app_data_ref::<AgentSessions>().is_none() {
        lua.set_app_data(AgentSessions::default());
    }
    lua.app_data_ref::<AgentSessions>()
        .unwrap()
        .0
        .lock()
        .unwrap()
        .insert(agent.id, session);
    let sessions = lua.app_data_ref::<AgentSessions>().unwrap().clone();
    let id = agent.id;
    if actor.snapshot().lifecycle != ActorLifecycle::Open {
        let retired = sessions.0.lock().unwrap().remove(&id);
        drop(retired);
        return Ok((Some(agent), None));
    }
    smol::spawn(async move {
        while let Ok(event) = events.recv_async().await {
            if matches!(event, maki_agent::ActorEvent::Close { .. }) {
                let retired = sessions.0.lock().unwrap().remove(&id);
                drop(retired);
                break;
            }
        }
    })
    .detach();
    Ok((Some(agent), None))
}

/// Read the stable agent ID.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "id")]
fn agent_id(_lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<String>> {
    Ok((Some(this.id.to_string()), None))
}

/// Read lifecycle, active turn, queue size, and cumulative usage.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Current actor snapshot.
#[lua_fn(name = "status")]
fn agent_status(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let snapshot = try_pair!(this.manager.actor(this.id)).snapshot();
    let table = lua.create_table()?;
    table.set("id", this.id.to_string())?;
    table.set(
        "status",
        match snapshot.lifecycle {
            ActorLifecycle::Closed | ActorLifecycle::Shutdown => "closed",
            ActorLifecycle::Open => match snapshot.status {
                ActorStatus::Idle => "idle",
                _ => "running",
            },
        },
    )?;
    table.set("queued", snapshot.queued)?;
    table.set("active_turn", snapshot.active_turn.map(|id| id.to_string()))?;
    table.set("input_tokens", snapshot.cumulative_usage.total_input())?;
    table.set("output_tokens", snapshot.cumulative_usage.output)?;
    Ok((Some(table), None))
}

/// Read the actor's committed model configuration.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Model descriptor.
#[lua_fn(name = "model")]
fn agent_model(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let actor = try_pair!(this.manager.actor(this.id));
    let config = try_pair!(
        actor
            .effective_config()
            .ok_or("agent configuration is unavailable")
    );
    let selection = model_to_lua_table(lua, &config.model)?;
    selection.set(
        "thinking",
        json_to_lua(
            lua,
            &serde_json::to_value(config.thinking).map_err(mlua::Error::external)?,
        )?,
    )?;
    selection.set("fast", config.fast)?;
    Ok((Some(selection), None))
}

/// List authenticated host model descriptors without changing focus.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Available model catalog.
#[lua_fn(name = "available_models")]
fn agent_available_models(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<mlua::Value>> {
    try_pair!(authorize(lua, &ctx, this));
    let service = try_pair!(
        lua.app_data_ref::<crate::orchestration::OrchestrationServicesSlot>()
            .and_then(|slot| slot.0.clone())
            .ok_or("host model catalog is unavailable")
    );
    let target = try_pair!(this.manager.lookup(this.id));
    let models = try_pair!(service.available_models(&target));
    let table = lua.create_table()?;
    for model in models {
        table.push(model)?;
    }
    Ok((Some(mlua::Value::Table(table)), None))
}

/// Read a bounded committed transcript, optionally through a completed turn.
/// @param ctx LuaCtx Invocation context.
/// @param opts table Required positive last_messages and max_bytes; optional through_turn.
/// @return (table?, string?) Messages and truncation metadata.
#[lua_fn(name = "transcript")]
fn agent_transcript(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    opts: Table,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let through_turn = opts.get::<Option<String>>("through_turn")?;
    let through_turn = try_pair!(
        through_turn
            .map(|id| id.parse::<maki_agent::TurnId>())
            .transpose()
    );
    let actor = try_pair!(this.manager.actor(this.id));
    let snapshot = try_pair!(actor.transcript(maki_agent::TranscriptRequest {
        through_turn,
        last_messages: opts.get::<usize>("last_messages")?,
        max_bytes: opts.get::<usize>("max_bytes")?,
    }));
    let table = lua.create_table()?;
    table.set("messages", json_to_lua(lua, &snapshot.messages)?)?;
    table.set(
        "through_turn",
        snapshot.through_turn.map(|id| id.to_string()),
    )?;
    table.set("epoch", snapshot.epoch)?;
    table.set("total_messages", snapshot.total_messages)?;
    table.set("omitted_messages", snapshot.omitted_messages)?;
    table.set("bytes", snapshot.bytes)?;
    table.set("truncated", snapshot.truncated)?;
    Ok((Some(table), None))
}

/// Return another nonowning handle to this agent.
/// @return (AgentRef?, string?) Stable reference.
#[lua_fn(name = "ref")]
fn agent_ref(lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<LuaAgentRef>> {
    let _ = lua;
    Ok((
        Some(LuaAgentRef {
            manager: this.manager.clone(),
            id: this.id,
        }),
        None,
    ))
}

fn admit(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    message: String,
    idle: bool,
    after_turn: Option<maki_agent::TurnId>,
) -> Result<LuaAgentTurn, String> {
    authorize(lua, ctx, agent)?;
    let actor = agent
        .manager
        .actor(agent.id)
        .map_err(|error| error.to_string())?;
    let config = actor
        .effective_config()
        .ok_or("agent configuration is unavailable")?;
    let input = AgentInput {
        message,
        mode: config.mode.clone(),
        thinking: config.thinking,
        fast: config.fast,
        workflow: config.workflow,
        images: Vec::new(),
        preamble: Vec::new(),
        prompt: None,
        cancel: None,
        lease_committer: None,
    };
    let provenance = if let Ok(trusted) = ctx.trusted(lua) {
        maki_agent::TurnProvenance {
            origin: maki_agent::TurnOrigin::Plugin,
            plugin: Some(trusted.plugin.to_string()),
            plugin_generation: Some(trusted.generation),
            ..Default::default()
        }
    } else {
        let current = crate::runtime::current_managed_turn(lua);
        maki_agent::TurnProvenance {
            origin: maki_agent::TurnOrigin::Plugin,
            plugin: crate::runtime::current_plugin(lua).map(|plugin| plugin.0.to_string()),
            plugin_generation: crate::runtime::current_plugin(lua).map(|plugin| plugin.1),
            source_agent: current.as_ref().map(CurrentManagedTurn::agent_id),
            source_turn: current.as_ref().map(CurrentManagedTurn::turn_id),
        }
    };
    let ticket = actor
        .admit_turn_with_options(
            input,
            None,
            String::new(),
            maki_agent::TurnAdmissionOptions {
                idle_only: idle,
                after_turn,
                provenance,
            },
        )
        .map_err(|error| error.to_string())?;
    Ok(LuaAgentTurn {
        agent: agent.clone(),
        owner: match &agent.manager {
            AgentRuntime::Managed(manager) => Some(
                manager
                    .lookup(agent.id)
                    .map_err(|error| error.to_string())?,
            ),
            AgentRuntime::Unmanaged { .. } => None,
        },
        ticket,
    })
}

/// Admit a turn only when this agent and its descendants are idle.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message.
/// @param opts table? Optional after_turn ID required to match the latest completed turn.
/// @return (AgentTurn?, string?) Exact-turn ticket.
#[lua_fn(name = "send")]
fn agent_send(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    message: String,
    opts: Option<Table>,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    let after_turn = opts
        .map(|opts| opts.get::<Option<String>>("after_turn"))
        .transpose()?
        .flatten();
    let after_turn = try_pair!(
        after_turn
            .map(|id| id.parse::<maki_agent::TurnId>())
            .transpose()
    );
    Ok((
        Some(try_pair!(admit(lua, this, &ctx, message, true, after_turn))),
        None,
    ))
}

/// Queue a turn behind already admitted work.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message.
/// @return (AgentTurn?, string?) Exact-turn ticket.
#[lua_fn(name = "enqueue")]
fn agent_enqueue(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    message: String,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    Ok((
        Some(try_pair!(admit(lua, this, &ctx, message, false, None))),
        None,
    ))
}

/// Send an idle-only turn and wait for its exact result.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message.
/// @param opts table? Optional timeout and after_turn.
/// @return (table?, string?) Per-turn result.
#[lua_fn(name = "prompt")]
async fn agent_prompt(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    message: String,
    opts: Option<Table>,
) -> mlua::Result<Pair<Table>> {
    let after_turn = opts
        .as_ref()
        .map(|opts| opts.get::<Option<String>>("after_turn"))
        .transpose()?
        .flatten();
    let after_turn = try_pair!(
        after_turn
            .map(|id| id.parse::<maki_agent::TurnId>())
            .transpose()
    );
    let turn = try_pair!(admit(&lua, &this, &ctx, message, true, after_turn));
    drop(this);
    let userdata = lua.create_userdata(turn)?;
    ticket_wait(lua, userdata.borrow::<LuaAgentTurn>()?, ctx, opts).await
}

/// Switch model through a reserved actor configuration operation.
/// @param ctx LuaCtx Invocation context.
/// @param patch string|table Spec string or partial spec/thinking/fast selection.
/// @return (table?, string?) Committed model descriptor.
#[lua_fn(name = "set_model")]
async fn agent_set_model(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    patch: mlua::Value,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(&lua, &ctx, &this));
    let patch = match patch {
        mlua::Value::String(spec) => {
            let table = lua.create_table()?;
            table.set("spec", spec)?;
            table
        }
        mlua::Value::Table(table) => table,
        _ => return Ok(err_pair("model patch must be a spec string or table")),
    };
    for key in patch.clone().pairs::<String, mlua::Value>() {
        let (key, _) = key?;
        if !matches!(key.as_str(), "spec" | "thinking" | "fast") {
            return Ok(err_pair(format!("unknown model patch key: {key}")));
        }
    }
    let spec = patch.get::<Option<String>>("spec")?;
    let thinking = match patch.get::<Option<mlua::Value>>("thinking")? {
        None => None,
        Some(value) => {
            let setting = match value {
                mlua::Value::String(value) => value.to_str()?.to_owned(),
                mlua::Value::Integer(value) => value.to_string(),
                _ => return Ok(err_pair("thinking must be a string or integer")),
            };
            Some(maki_providers::ThinkingConfig::from(try_pair!(
                maki_storage::sessions::StoredThinking::parse_setting(&setting)
            )))
        }
    };
    let fast = patch.get::<Option<bool>>("fast")?;
    let actor = try_pair!(this.manager.actor(this.id));
    let reservation = try_pair!(actor.reserve_config_update());
    let template = try_pair!(destination_template(&lua, &this));
    let prepare = lua
        .app_data_ref::<super::SessionProviderPreparer>()
        .map(|prepare| Arc::clone(&prepare));
    let model = if spec.is_some() {
        let (model, provider) =
            try_pair!(super::build_session_provider(&spec, false, &template, prepare).await);
        Some(maki_agent::actor::PreparedModel { model, provider })
    } else {
        None
    };
    try_pair!(authorize(&lua, &ctx, &this));
    try_pair!(
        reservation.resolve(Ok(maki_agent::actor::ConfigChange::Patch(
            maki_agent::actor::ConfigPatch {
                model,
                thinking,
                fast,
                ..Default::default()
            }
        )))
    );
    drop(this);
    drop(ctx);
    let commit = try_pair!(reservation.wait().await);
    let selection = lua.create_table()?;
    selection.set("spec", commit.config.model.spec())?;
    selection.set(
        "thinking",
        serde_json::to_value(commit.config.thinking)
            .map_err(mlua::Error::external)
            .and_then(|value| json_to_lua(&lua, &value))?,
    )?;
    selection.set("fast", commit.config.fast)?;
    Ok((Some(selection), None))
}

/// Switch mode through the actor configuration FIFO.
/// @param ctx LuaCtx Invocation context.
/// @param name string Registered mode name.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "set_mode")]
async fn agent_set_mode(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    name: String,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(&lua, &ctx, &this));
    let template = try_pair!(destination_template(&lua, &this));
    if template.restrict_write_to().is_some() {
        return Ok(err_pair("restricted agents cannot change mode"));
    }
    let definition = try_pair!(
        template
            .modes
            .get_by_key(&name)
            .ok_or_else(|| format!("unknown mode: {name}"))
    );
    let actor = try_pair!(this.manager.actor(this.id));
    let reservation = try_pair!(actor.reserve_config_update());
    let mode = match name.as_str() {
        "build" => maki_agent::AgentMode::Build,
        "plan" => {
            let path = template.mode.plan_path().map(|path| path.to_path_buf());
            let path = match path {
                Some(path) => path,
                None => {
                    if let Some(prepare) = lua
                        .app_data_ref::<super::PlanPathPreparer>()
                        .map(|prepare| Arc::clone(&prepare))
                        .or_else(|| {
                            lua.app_data_ref::<crate::orchestration::OrchestrationServicesSlot>()
                                .and_then(|slot| {
                                    slot.0
                                        .as_ref()
                                        .and_then(|service| service.plan_path_preparer())
                                })
                        })
                    {
                        try_pair!(prepare())
                    } else {
                        let state_dir = lua
                            .app_data_ref::<crate::api::env::StateDirOverride>()
                            .map(|dir| maki_storage::StateDir::from_path(dir.0.clone()));
                        try_pair!(
                            smol::unblock(move || {
                                let dir = state_dir
                                    .map(Ok)
                                    .unwrap_or_else(maki_storage::StateDir::resolve)?;
                                maki_storage::plans::new_plan_path(&dir)
                            })
                            .await
                        )
                    }
                }
            };
            try_pair!(authorize(&lua, &ctx, &this));
            maki_agent::AgentMode::Plan(path)
        }
        _ => maki_agent::AgentMode::Custom(maki_agent::ModeId::parse(&name)),
    };
    try_pair!(
        reservation.resolve(Ok(maki_agent::actor::ConfigChange::Mode {
            mode: mode.clone(),
            mode_def: Some(definition)
        }))
    );
    drop(this);
    drop(ctx);
    try_pair!(reservation.wait().await);
    Ok((Some(true), None))
}

/// Cancel existing work without closing the agent.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel")]
fn agent_cancel(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    try_pair!(this.manager.cancel_agent(this.id));
    Ok((Some(true), None))
}

/// Cancel existing work in this agent and its descendants.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel_subtree")]
fn agent_cancel_subtree(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    try_pair!(this.manager.cancel_subtree(this.id));
    Ok((Some(true), None))
}

/// Permanently close this agent and its descendants.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "close")]
fn agent_close(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    try_pair!(this.manager.close_subtree(this.id));
    retire_closed(lua);
    Ok((Some(true), None))
}

/// Read the stable turn ID.
/// @return (string?, string?) Turn ID.
#[lua_fn(name = "id")]
fn turn_id(_lua: &Lua, this: &LuaAgentTurn) -> mlua::Result<Pair<String>> {
    Ok((Some(this.ticket.turn_id().to_string()), None))
}

/// Read the target agent ID.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "agent_id")]
fn turn_agent_id(lua: &Lua, this: &LuaAgentTurn) -> mlua::Result<Pair<String>> {
    let _ = lua;
    Ok((Some(this.agent.id.to_string()), None))
}

fn turn_result(
    lua: &Lua,
    _agent: &LuaAgent,
    ticket: &TurnTicket,
    outcome: &maki_agent::TurnOutcome,
) -> mlua::Result<Pair<Table>> {
    let retained = ticket.peek_result();
    let mut result = derive_result(outcome);
    if let Some(retained) = &retained {
        result.text = retained.text.clone();
        result.captured = retained.output.last().cloned();
    }
    let table = lua.create_table()?;
    table.set(
        "origin",
        json_to_lua(lua, &origin_json(ticket.provenance()))?,
    )?;
    table.set("agent_id", outcome.agent_id().to_string())?;
    table.set("turn_id", outcome.turn_id().to_string())?;
    table.set("text", result.text)?;
    table.set("input_tokens", outcome.usage().total_input())?;
    table.set("output_tokens", outcome.usage().output)?;
    table.set(
        "usage",
        json_to_lua(
            lua,
            &serde_json::to_value(outcome.usage()).map_err(mlua::Error::external)?,
        )?,
    )?;
    table.set("error", result.error)?;
    table.set(
        "status",
        match outcome {
            maki_agent::TurnOutcome::Completed { .. } => "completed",
            maki_agent::TurnOutcome::Failed { .. } => "failed",
            maki_agent::TurnOutcome::Cancelled { reason, .. } => {
                table.set("cancellation_reason", reason.to_string())?;
                "cancelled"
            }
        },
    )?;
    if let Some(captured) = result.captured {
        table.set("captured", json_to_lua(lua, &captured)?)?;
    }
    if let Some(retained) = retained {
        table.set(
            "output",
            json_to_lua(lua, &serde_json::Value::Array(retained.output))?,
        )?;
    }
    Ok((Some(table), None))
}

/// Read a retained result without waiting or consuming it.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Nil without error while pending.
#[lua_fn(name = "result")]
fn ticket_result(
    lua: &Lua,
    this: &LuaAgentTurn,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<Table>> {
    try_pair!(observe(lua, &ctx, &this.agent));
    if let Some(owner) = &this.owner {
        try_pair!(owner.turn_ticket(this.ticket.turn_id()));
    }
    match this.ticket.peek() {
        Some(outcome) => turn_result(lua, &this.agent, &this.ticket, &outcome),
        None => Ok((None, None)),
    }
}

/// Wait for this exact turn. Timeout stops waiting, not the agent.
/// @param ctx LuaCtx Invocation context.
/// @param opts table? Optional timeout in seconds.
/// @return (table?, string?) Per-turn result or error.
#[lua_fn(name = "wait")]
async fn ticket_wait(
    lua: Lua,
    this: UserDataRef<LuaAgentTurn>,
    ctx: UserDataRef<LuaCtx>,
    opts: Option<Table>,
) -> mlua::Result<Pair<Table>> {
    let active = try_pair!(observe(&lua, &ctx, &this.agent));
    if let Some(owner) = &this.owner {
        try_pair!(owner.turn_ticket(this.ticket.turn_id()));
    }
    let agent = this.agent.clone();
    let ticket = this.ticket.clone();
    let retained_ticket = ticket.clone();
    let timeout = opts
        .map(|opts| opts.get::<Option<u64>>("timeout"))
        .transpose()?
        .flatten();
    if let Some(outcome) = ticket.peek() {
        return turn_result(&lua, &agent, &ticket, &outcome);
    }
    if timeout == Some(0) {
        return Ok((None, None));
    }
    drop(this);
    drop(ctx);
    let outcome = if let Some(active) = active {
        if active.agent_id() == agent.id {
            return Ok(err_pair("cannot wait for a turn on the calling agent"));
        }
        let actor = try_pair!(agent.manager.actor(agent.id));
        let wait = try_pair!(active.lease().observe_descendant(
            &active,
            agent.id,
            &actor,
            ticket,
            timeout.map(Duration::from_secs)
        ));
        try_pair!(wait.wait().await.map_err(|error| format!("{error:?}")))
    } else {
        match timeout {
            Some(seconds) => try_pair!(
                futures_lite::future::race(async { Ok(ticket.wait().await) }, async {
                    smol::Timer::after(Duration::from_secs(seconds)).await;
                    Err("agent turn wait timed out")
                })
                .await
            ),
            None => ticket.wait().await,
        }
    };
    turn_result(&lua, &agent, &retained_ticket, &outcome)
}

/// Cancel only this admitted turn.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Success or error.
#[lua_fn(name = "cancel")]
fn ticket_cancel(
    lua: &Lua,
    this: &LuaAgentTurn,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, &this.agent));
    try_pair!(
        try_pair!(this.agent.manager.actor(this.agent.id)).cancel_turn(this.ticket.turn_id())
    );
    Ok((Some(true), None))
}

struct LuaAgentSubscription {
    id: u64,
    stop: flume::Sender<()>,
    request_tx: flume::Sender<crate::runtime::Request>,
}

impl Drop for LuaAgentSubscription {
    fn drop(&mut self) {
        let _ = self.stop.try_send(());
        let _ = self
            .request_tx
            .try_send(crate::runtime::Request::CloseAgentEventCallback { id: self.id });
    }
}

fn subscribe(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    handler: Function,
    kind: &'static str,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    try_pair!(authorize(lua, ctx, agent));
    let plugin = try_pair!(crate::runtime::current_plugin(lua).ok_or("no active plugin identity"));
    let target = try_pair!(agent.manager.lookup(agent.id));
    let service = try_pair!(
        lua.app_data_ref::<crate::orchestration::OrchestrationServicesSlot>()
            .and_then(|slot| slot.0.clone())
            .ok_or("no host orchestration services")
    );
    let template = try_pair!(service.template(&target));
    let tx = try_pair!(
        lua.app_data_ref::<crate::runtime::RequestTx>()
            .map(|tx| tx.0.clone())
            .ok_or("agent event dispatch is unavailable")
    );
    let actor = try_pair!(target.actor());
    let events = actor.subscribe_events();
    let id = crate::orchestration::register_event_callback(lua, plugin.0, handler)?;
    let cancel = try_pair!(
        crate::orchestration::event_callback(lua, id)
            .map(|callback| callback.2)
            .ok_or("event registration expired")
    );
    let (stop, stopped) = flume::bounded(1);
    let request_tx = tx.clone();
    smol::spawn(async move {
        loop {
            let event = futures_lite::future::race(async { events.recv_async().await.ok() }, async { let _ = cancel.race(stopped.recv_async()).await; None }).await;
            let Some(event) = event else { break; };
            let payload = match (kind, event) {
                ("start", maki_agent::ActorEvent::Start { agent_id, turn_id, provenance, config }) => serde_json::json!({
                    "agent_id": agent_id.to_string(), "turn_id": turn_id.to_string(),
                    "origin": origin_json(&provenance), "selection": config.map(|commit| selection_json(&commit.config)),
                }),
                ("end", maki_agent::ActorEvent::End(result)) => result_json(&result),
                ("idle", maki_agent::ActorEvent::Idle { agent_id }) => serde_json::json!({"agent_id": agent_id.to_string()}),
                ("config", maki_agent::ActorEvent::Config(commit)) => serde_json::json!({"agent_id": target.id().to_string(), "generation": commit.generation, "selection": selection_json(&commit.config)}),
                ("close", maki_agent::ActorEvent::Close { agent_id, lifecycle }) => serde_json::json!({"agent_id": agent_id.to_string(), "lifecycle": format!("{lifecycle:?}").to_lowercase()}),
                _ => continue,
            };
            if tx.send(crate::runtime::Request::AgentEvent(Box::new(crate::orchestration::AgentEventRequest { callback_id: id, payload, target: crate::orchestration::TrustedTarget { target: target.clone(), template: template.clone() } }))).is_err() { break; }
        }
    }).detach();
    Ok((
        Some(LuaAgentSubscription {
            id,
            stop,
            request_tx,
        }),
        None,
    ))
}

fn selection_json(config: &maki_agent::EffectiveAgentConfig) -> serde_json::Value {
    serde_json::json!({"spec": config.model.spec(), "thinking": config.thinking, "fast": config.fast})
}

fn origin_json(origin: &maki_agent::TurnProvenance) -> serde_json::Value {
    serde_json::json!({
        "kind": match origin.origin { maki_agent::TurnOrigin::Internal => "internal", maki_agent::TurnOrigin::User => "user", maki_agent::TurnOrigin::Plugin => "plugin" },
        "plugin": origin.plugin, "plugin_generation": origin.plugin_generation,
        "source_agent": origin.source_agent.map(|id| id.to_string()), "source_turn": origin.source_turn.map(|id| id.to_string()),
    })
}

fn result_json(result: &maki_agent::TurnResult) -> serde_json::Value {
    let outcome = &result.outcome;
    let mut value = serde_json::json!({"agent_id": outcome.agent_id().to_string(), "turn_id": outcome.turn_id().to_string(), "text": result.text, "usage": outcome.usage(), "output": result.output, "origin": origin_json(&result.provenance)});
    let status = match outcome {
        maki_agent::TurnOutcome::Completed { .. } => "completed",
        maki_agent::TurnOutcome::Failed { failure, .. } => {
            value["error"] = failure.diagnostic.clone().into();
            "failed"
        }
        maki_agent::TurnOutcome::Cancelled { reason, .. } => {
            value["cancellation_reason"] = reason.to_string().into();
            "cancelled"
        }
    };
    value["status"] = status.into();
    value
}

/// Subscribe to future turn admissions. Handlers may overlap.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Receives immutable event payload and a fresh scoped ctx.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_start")]
fn on_turn_start(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "start")
}

/// Subscribe to exact settled turn results. Handlers may overlap.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Receives result payload and a fresh scoped ctx.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_turn_end")]
fn on_turn_end(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "end")
}

/// Subscribe to transitions to no pending turn work.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Receives agent identity and a fresh scoped ctx.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_idle")]
fn on_idle(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "idle")
}

/// Subscribe to committed model or mode changes.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Receives selection/generation and a fresh scoped ctx.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_config_change")]
fn on_config_change(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "config")
}

/// Subscribe to permanent lifecycle closure.
/// @param ctx LuaCtx Trusted invocation context.
/// @param handler function Receives lifecycle and a fresh scoped ctx.
/// @return (AgentSubscription?, string?) Revocable subscription.
#[lua_fn(name = "on_close")]
fn on_close(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    handler: Function,
) -> mlua::Result<Pair<LuaAgentSubscription>> {
    subscribe(lua, this, &ctx, handler, "close")
}

/// Revoke future deliveries and cancel active handlers.
/// @return (boolean?, string?) Success.
#[lua_fn(name = "close")]
fn subscription_close(lua: &Lua, this: &LuaAgentSubscription) -> mlua::Result<Pair<bool>> {
    crate::orchestration::close_event_callback(lua, this.id);
    let _ = this.stop.try_send(());
    Ok((Some(true), None))
}

/// Close this agent at the end of the current host task, even on cancellation.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Cleanup registered.
#[lua_fn(name = "defer_close")]
pub(crate) fn agent_defer_close(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    let manager = this.manager.clone();
    let id = this.id;
    let sessions = lua
        .app_data_ref::<AgentSessions>()
        .map(|sessions| sessions.clone());
    try_pair!(crate::runtime::register_defer_close(
        lua,
        Box::new(move || {
            let _ = manager.close_subtree(id);
            if let Some(sessions) = sessions {
                let retired = {
                    let mut sessions = sessions.0.lock().unwrap();
                    let ids: Vec<_> = sessions
                        .iter()
                        .filter(|(_, session)| {
                            session.actor.snapshot().lifecycle != ActorLifecycle::Open
                        })
                        .map(|(id, _)| *id)
                        .collect();
                    ids.into_iter()
                        .filter_map(|id| sessions.remove(&id))
                        .collect::<Vec<_>>()
                };
                drop(retired);
            }
        })
    ));
    Ok((Some(true), None))
}

lua_class! {
    /// A revocable subscription. Close explicitly to cancel active callbacks.
    "maki.agent.AgentSubscription" => LuaAgentSubscription, SUBSCRIPTION_DOCS [subscription_close]
}

/// Read a reference's stable identity.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "id")]
fn reference_id(_lua: &Lua, this: &LuaAgentRef) -> mlua::Result<Pair<String>> {
    Ok((Some(this.id.to_string()), None))
}

lua_class! {
    /// Visibility-only identity. Resolve with get(ctx, reference) to request control.
    "maki.agent.AgentRef" => LuaAgentRef, REF_DOCS [reference_id]
}

fn agent_internal_methods<M: mlua::UserDataMethods<LuaAgent>>(methods: &mut M) {
    methods.add_method("_maki_task_owner", |_, this, ()| {
        Ok(super::LuaTaskOwner {
            owner_id: this.state.as_ref().and_then(|state| state.parent_agent_id),
        })
    });
    methods.add_method("_maki_session_id", |_, this, ()| {
        Ok(this
            .state
            .as_ref()
            .map(|state| state.ui_id.clone())
            .unwrap_or_else(|| this.id.to_string()))
    });
}

lua_class! {
    /// A nonowning live agent handle. Garbage collection never closes its actor.
    "maki.agent.Agent" => LuaAgent, AGENT_DOCS [agent_id, agent_status, agent_model, agent_available_models, agent_transcript, agent_ref, agent_send, agent_enqueue, agent_prompt, agent_set_model, agent_set_mode, agent_cancel, agent_cancel_subtree, agent_close, agent_defer_close, on_turn_start, on_turn_end, on_idle, on_config_change, on_close]
    extra agent_internal_methods
}
lua_class! {
    /// A nonowning exact-turn ticket with repeatable result reads.
    "maki.agent.AgentTurn" => LuaAgentTurn, TURN_DOCS [turn_id, turn_agent_id, ticket_result, ticket_wait, ticket_cancel]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::agent::{
        LuaActorBackend,
        structured_output::{StructuredOutput, TOOL_NAME},
        tests::{StreamOnceProvider, canned_reply_with_usage, session_with_provider},
    };
    use crate::runtime::TaskScope;
    use maki_agent::{AgentLimits, tools::test_support::stub_ctx};
    use maki_providers::TokenUsage;

    const REPLY: &str = "retained reply";

    struct TestServices(crate::orchestration::TrustedTarget);

    impl crate::orchestration::OrchestrationServices for TestServices {
        fn resolve_target(
            &self,
            _: Option<&maki_commands::CommandInvocation>,
        ) -> Result<Option<crate::orchestration::TrustedTarget>, String> {
            Ok(Some(self.0.clone()))
        }

        fn template(
            &self,
            target: &maki_agent::AgentRef,
        ) -> Result<maki_agent::tools::ToolContext, String> {
            if target.same_manager(&self.0.target) {
                Ok(self.0.template.clone())
            } else {
                Err("wrong test manager".into())
            }
        }
    }

    #[test]
    fn subscription_gc_schedules_registry_cleanup_without_lua_reentry() {
        let lua = Lua::new();
        let handler = lua.create_function(|_, ()| Ok(())).unwrap();
        let id =
            crate::orchestration::register_event_callback(&lua, Arc::from("gc-owner"), handler)
                .unwrap();
        let cancel = crate::orchestration::event_callback(&lua, id).unwrap().2;
        let (request_tx, requests) = flume::unbounded();
        let (stop, stopped) = flume::bounded(1);
        lua.globals()
            .set(
                "subscription",
                LuaAgentSubscription {
                    id,
                    stop,
                    request_tx,
                },
            )
            .unwrap();
        lua.load("subscription = nil").exec().unwrap();
        lua.gc_collect().unwrap();
        assert_eq!(stopped.try_recv(), Ok(()));
        assert!(!cancel.is_cancelled());
        assert!(crate::orchestration::event_callback(&lua, id).is_some());
        smol::block_on(async {
            let request = requests.recv_async().await.unwrap();
            let crate::runtime::Request::CloseAgentEventCallback { id: closed } = request else {
                panic!("unexpected cleanup request");
            };
            assert_eq!(closed, id);
            crate::orchestration::close_event_callback(&lua, closed);
        });
        assert!(cancel.is_cancelled());
        assert!(crate::orchestration::event_callback(&lua, id).is_none());
        lua.gc_collect().unwrap();
    }

    #[test]
    fn spawn_inherits_defaults_while_legacy_session_keeps_empty_tools() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = TaskScope::detached(&lua);
            let mut context = stub_ctx(&maki_agent::AgentMode::Build);
            context.workflow = true;
            let mut expected = context.registry.definitions(
                &maki_agent::template::env_vars(),
                &maki_agent::tools::DescriptionContext {
                    filter: &context.tool_filter,
                    audience: context.audience,
                    workflow: context.workflow,
                    mcp: context.mcp.is_some(),
                },
                context.model.supports_tool_examples(),
            );
            expected.as_array_mut().unwrap().truncate(1);
            let structured = StructuredOutput::compile(serde_json::json!({
                "type": "object", "properties": {}
            }))
            .unwrap();
            let mut parent_definitions = expected.clone();
            parent_definitions
                .as_array_mut()
                .unwrap()
                .push(structured.definition());
            context.local_tools = Arc::new(HashMap::from([(
                TOOL_NAME.to_owned(),
                structured.local_tool(),
            )]));
            context.turn_bindings = Arc::new(maki_agent::tools::TurnToolBindings::capture(
                &context.registry,
                &context.local_tools,
                context.mcp.as_ref(),
            ));
            context.request_tools = Some(maki_agent::tools::RequestTools::assembled(
                parent_definitions,
                &context.config,
                &context.model,
            ));
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(scope.handle());
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", super::super::create_agent_table(&lua).unwrap())
                .unwrap();
            let child: mlua::AnyUserData = scope
                .scope_future(
                    lua.load(
                        r#"
                return assert(agent_api.spawn(ctx, { inherit_provider = true, silent = true }))
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            let child = child.borrow::<LuaAgent>().unwrap();
            let state = child.state.as_ref().unwrap();
            let template = state.template.get().unwrap();
            assert_eq!(template.audience, context.audience);
            assert!(
                child
                    .manager
                    .actor(child.id)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .workflow
            );
            assert_eq!(state.tools.definitions(), &expected);
            assert!(!state.local_tools.contains_key(TOOL_NAME));
            assert!(state.structured.is_none());
            assert_eq!(
                template.request_tools.as_ref().unwrap().definitions(),
                state.tools.definitions()
            );
            let empty: mlua::AnyUserData = scope.scope_future(lua.load(r#"
                return assert(agent_api.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
            "#).eval_async()).await.unwrap();
            let empty = empty.borrow::<LuaAgent>().unwrap();
            assert!(
                empty
                    .state
                    .as_ref()
                    .unwrap()
                    .tools
                    .definitions()
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            empty.manager.close_subtree(empty.id).unwrap();
            let legacy: mlua::AnyUserData = scope
                .scope_future(
                    lua.load(
                        r#"
                return assert(agent_api.session(ctx, { inherit_provider = true, silent = true }))
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            let legacy = legacy.borrow::<LuaSession>().unwrap();
            assert!(
                legacy
                    .state
                    .tools
                    .definitions()
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
            child.manager.close_subtree(child.id).unwrap();
        });
    }

    #[test]
    fn unmanaged_spawn_uses_actor_tickets_and_rejects_foreign_task_authority() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = TaskScope::detached(&lua);
            let mut context = stub_ctx(&maki_agent::AgentMode::Build);
            context.provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, TokenUsage::default()),
            ]));
            context.request_tools = Some(maki_agent::tools::RequestTools::default());
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(scope.handle());
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", super::super::create_agent_table(&lua).unwrap())
                .unwrap();
            let result: String = scope
                .scope_future(
                    lua.load(
                        r#"
                local child, err = agent_api.spawn(ctx, { inherit_provider = true, silent = true })
                assert(child, err)
                retained_child = child
                local reference = assert(child:ref())
                assert(agent_api.get(ctx, reference):id() == child:id())
                local ticket = assert(child:send(ctx, "standalone request"))
                local result = assert(ticket:wait(ctx))
                assert(ticket:result(ctx).text == result.text)
                assert(child:transcript(ctx, { last_messages = 100, max_bytes = 65536 }))
                local current, current_err = agent_api.current(ctx)
                assert(current == nil and current_err ~= nil)
                return result.text
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            assert_eq!(result, REPLY);
            let other = TaskScope::detached(&lua);
            let denied: bool = other
                .scope_future(
                    lua.load(
                        r#"
                local status, err = retained_child:status(ctx)
                assert(status == nil and err ~= nil)
                local child, spawn_err = agent_api.spawn(ctx, { inherit_provider = true, silent = true })
                return child == nil and spawn_err ~= nil
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            assert!(denied);
            drop(other);
            let closed: bool = scope
                .scope_future(lua.load("return retained_child:close(ctx)").eval_async())
                .await
                .unwrap();
            assert!(closed);
        });
    }

    #[test]
    fn plugin_revocation_closes_only_owned_children() {
        const OWNER: &str = "agent-owner";
        const OTHER_OWNER: &str = "other-owner";
        let provider = Arc::new(StreamOnceProvider::new_replies(Vec::new()));
        let (actor, _, session, _) = session_with_provider(provider.clone(), None, None);
        let (other_actor, _, other_session, _) = session_with_provider(provider, None, None);
        let lua = Lua::new();
        lua.set_app_data(AgentOwners(HashMap::from([
            (session.agent_id, Arc::from(OWNER)),
            (other_session.agent_id, Arc::from(OTHER_OWNER)),
        ])));
        lua.set_app_data(AgentSessions(Arc::new(Mutex::new(HashMap::from([
            (session.agent_id, session),
            (other_session.agent_id, other_session),
        ])))));
        crate::orchestration::revoke_plugin(&lua, OWNER);
        assert_ne!(actor.snapshot().lifecycle, ActorLifecycle::Open);
        assert_eq!(other_actor.snapshot().lifecycle, ActorLifecycle::Open);
        assert_eq!(
            lua.app_data_ref::<AgentSessions>()
                .unwrap()
                .0
                .lock()
                .unwrap()
                .len(),
            1
        );
        crate::orchestration::revoke_plugin(&lua, OWNER);
        assert_eq!(other_actor.snapshot().lifecycle, ActorLifecycle::Open);
    }

    #[test]
    fn lua_agent_ticket_reads_are_repeatable_and_gc_is_nonowning() {
        smol::block_on(async {
            let usage = TokenUsage {
                input: 13,
                output: 7,
                ..Default::default()
            };
            let provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, usage),
            ]));
            let (_standalone, state, legacy, _events) = session_with_provider(provider, None, None);
            let params = state.params.get().unwrap();
            let config = maki_agent::EffectiveAgentConfig::new(
                maki_agent::RunSettings {
                    model: params.model.clone(),
                    provider: Arc::clone(&params.provider),
                    thinking: state.opts.thinking,
                    fast: state.opts.fast,
                    workflow: false,
                },
                maki_agent::AgentMode::Build,
            )
            .with_mode_def(Some(params.modes.current(&maki_agent::AgentMode::Build)));
            let mut managed_fixture = None;
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let target = manager
                .create_root_with_config(Some(config), Vec::new(), None, |agent_id| {
                    let (actor, root_state, session, events) =
                        super::super::tests::session_with_provider_for_agent(
                            Arc::clone(&params.provider),
                            agent_id,
                        );
                    assert_eq!(root_state.params.get().unwrap().agent_id, agent_id);
                    managed_fixture = Some((actor, Arc::clone(&root_state), session, events));
                    Ok::<_, String>(Box::new(LuaActorBackend::new(root_state)))
                })
                .unwrap();
            let lua = Lua::new();
            let scope = TaskScope::detached(&lua);
            let mut template = stub_ctx(&maki_agent::AgentMode::Build);
            template.provider = Arc::clone(&params.provider);
            let trusted_target = crate::orchestration::TrustedTarget {
                target: target.clone(),
                template,
            };
            lua.set_app_data(crate::orchestration::OrchestrationServicesSlot(Some(
                Arc::new(TestServices(trusted_target.clone())),
            )));
            let ctx = LuaCtx::trusted_context(&lua, trusted_target, Arc::from("agent-test"));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set(
                    "agent",
                    LuaAgent {
                        manager: AgentRuntime::Managed(manager.clone()),
                        id: target.id(),
                        state: Some(Arc::clone(&managed_fixture.as_ref().unwrap().1)),
                    },
                )
                .unwrap();
            let result: Table = lua
                .load(
                    r#"
                local ticket, err = agent:send(ctx, "hello")
                assert(ticket, err)
                local result, wait_err = ticket:wait(ctx)
                assert(result, wait_err)
                assert(result.status == "completed", result.error or result.status)
                assert(ticket:id(ctx) ~= agent:id(ctx))
                local again = ticket:result(ctx)
                assert(again.text == result.text)
                assert(again.input_tokens == result.input_tokens)
                local history = agent:transcript(ctx, { last_messages = 100, max_bytes = 65536 })
                assert(history, "transcript unavailable")
                local found_user, found_assistant = false, false
                for _, message in ipairs(history.messages) do
                    for _, block in ipairs(message.content or {}) do
                        if block.type == "text" then
                            found_user = found_user or (message.role == "user" and block.text == "hello")
                            found_assistant = found_assistant or (message.role == "assistant" and block.text == result.text)
                        end
                    end
                end
                assert(found_user, "transcript missing admitted user message")
                local diagnostic = "result.text=" .. tostring(result.text)
                for index, message in ipairs(history.messages) do
                    diagnostic = diagnostic .. " message[" .. index .. "].role=" .. tostring(message.role)
                    for _, block in ipairs(message.content or {}) do
                        diagnostic = diagnostic .. " block.type=" .. tostring(block.type) .. " block.text=" .. tostring(block.text)
                    end
                end
                assert(found_assistant, "transcript missing retained assistant message: " .. diagnostic)
                ticket = nil
                agent = nil
                collectgarbage("collect")
                return result
            "#,
                )
                .eval_async()
                .await
                .unwrap();
            assert_eq!(result.get::<String>("text").unwrap(), REPLY);
            assert_eq!(
                result.get::<u32>("input_tokens").unwrap(),
                usage.total_input()
            );
            assert_eq!(
                target.actor().unwrap().snapshot().lifecycle,
                ActorLifecycle::Open
            );
            drop(scope);
            manager.shutdown(Duration::from_secs(1)).await;
            drop(legacy);
        });
    }
}
