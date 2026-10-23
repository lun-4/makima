use std::sync::Arc;

use maki_agent::actor::{
    ActorError, ActorLifecycle, ActorStatus, ConfigChange, ConfigPatch, PreparedModel,
};
use maki_agent::session_coordinator::SessionCoordinatorHandle;
use maki_agent::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, MODEL_OPTION_ID, SessionOptionError,
    THINKING_OPTION_ID, WORKFLOW_OPTION_ID,
};
use maki_agent::{
    AgentInput, AgentRef, CurrentManagedTurn, TranscriptRequest, TurnAdmissionOptions, TurnId,
    TurnOrigin, TurnProvenance,
};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{Lua, Table, UserDataMethods, UserDataRef, Value as LuaValue};

use super::authority::{authorize, destination_template, observe, retire_closed};
use super::projection::selection_table;
use super::subscriptions::{
    on_close__doc, on_close__register, on_config_change__doc, on_config_change__register,
    on_idle__doc, on_idle__register, on_turn_end__doc, on_turn_end__register, on_turn_start__doc,
    on_turn_start__register,
};
use super::{AgentOwners, LuaAgent, LuaAgentRef, LuaAgentTurn};
use crate::agent_errors::{
    AGENT_BUSY, AGENT_CONFIG_UNAVAILABLE, NO_ORCHESTRATION_SERVICES, NO_PLUGIN_AUTHORITY,
    RESTRICTED_MODE_CHANGE, SESSION_LEVEL_OPTION, STALE_TURN, TYPED_OPTION_SETTER,
};
use crate::api::agent::{
    LuaTaskOwner, SessionProviderPreparer, build_session_provider, validate_required_output_mode,
};
use crate::api::env::host_state_dir;
use crate::api::session_option::{ensure_validation_not_in_progress, validate_option_value};
use crate::api::util::convert::json_to_lua;
use crate::api::util::ctx::{AgentContext, LuaCtx};
use crate::api::util::pair::{Pair, err_pair, try_pair};
use crate::api::util::selection::selection_from_lua;
use crate::orchestration::{self, plugin_authority};
use crate::runtime::{
    current_managed_turn, current_plugin, register_defer_close, validate_plugin_effect,
};
use crate::session_snapshot::{STATUS_CLOSED, STATUS_IDLE, STATUS_WORKING};

const MODE_KEY: &str = "mode";
const SEND_WHEN_REQUIRED: &str = "send: opts.when must be \"idle\", \"join\" or \"queue\"";
const EXISTING_ONLY_NEEDS_JOIN: &str = "send: existing_only needs when = \"join\"";
const AFTER_TURN_NEEDS_IDLE: &str = "send: after_turn needs when = \"idle\"";
const NO_SESSION_OPTIONS: &str = "agent belongs to no session with options";
const TYPED_OPTIONS: [&str; 3] = [MODEL_OPTION_ID, THINKING_OPTION_ID, FAST_OPTION_ID];

/// Read the stable agent ID.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "id")]
fn agent_id(_lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<String>> {
    Ok((Some(this.identity()), None))
}

/// Read lifecycle, active turn, queue size, and cumulative usage.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Fields: id (string), status ("idle", "working", or "closed", the vocabulary `maki.session` uses, which adds "needs_input"), queued (integer), active_turn (string or nil), input_tokens and output_tokens (cumulative integers). Authorized held managed handles retain a closed snapshot after graph removal.
#[lua_fn(name = "status")]
fn agent_status(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    try_pair!(observe(lua, &ctx, this));
    let table = lua.create_table()?;
    table.set("id", this.identity())?;
    let snapshot = if let Some(held) = &this.held {
        try_pair!(
            try_pair!(held.snapshot())
                .actor
                .ok_or("agent snapshot is unavailable")
        )
    } else {
        try_pair!(this.manager.actor(this.id)).snapshot()
    };
    let status = match snapshot.lifecycle {
        ActorLifecycle::Closed | ActorLifecycle::Shutdown => STATUS_CLOSED,
        ActorLifecycle::Open => match snapshot.status {
            ActorStatus::Idle => STATUS_IDLE,
            _ => STATUS_WORKING,
        },
    };
    table.set("status", status)?;
    table.set("queued", snapshot.queued)?;
    table.set("active_turn", snapshot.active_turn.map(|id| id.to_string()))?;
    table.set("input_tokens", snapshot.cumulative_usage.total_input())?;
    table.set("output_tokens", snapshot.cumulative_usage.output)?;
    Ok((Some(table), None))
}

/// The coordinator of the session {agent} works in; a child shares its
/// root's.
fn session_coordinator(lua: &Lua, agent: &LuaAgent) -> Result<SessionCoordinatorHandle, String> {
    let template = destination_template(lua, agent)?;
    let session = template.session_id.as_ref().ok_or(NO_SESSION_OPTIONS)?;
    SessionCoordinatorHandle::resolve(session.id()).map_err(|error| error.to_string())
}

/// Read the committed configuration.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) `{ version, model, mode, options }`. version is the committed config generation; model is the normalized selection `{spec, thinking, fast}` (thinking is a setting string or budget integer, disabled options are "off" and false); mode is the registered mode name; options maps every other session option id (workflow, yolo, plugin options such as `bash.auto_mode`) to its value. workflow is the agent's own; yolo and plugin options belong to the session, so a child reads its root's. The committed config is what the next turn admits; `ctx:model()` and `ctx:mode()` describe the turn already running.
#[lua_fn(name = "config")]
fn agent_config(lua: &Lua, this: &LuaAgent, ctx: UserDataRef<LuaCtx>) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let actor = try_pair!(this.manager.actor(this.id));
    let commit = try_pair!(actor.config_snapshot().ok_or(AGENT_CONFIG_UNAVAILABLE));
    let options = lua.create_table()?;
    if let Ok(coordinator) = session_coordinator(lua, this) {
        for state in coordinator.read().options().options.iter() {
            let id = state.definition.id.as_ref();
            if !TYPED_OPTIONS.contains(&id) {
                options.set(id, state.current_value.as_ref())?;
            }
        }
    }
    options.set(
        WORKFLOW_OPTION_ID,
        toggle_value(commit.config.settings.workflow),
    )?;
    let config = lua.create_table()?;
    config.set("version", commit.generation)?;
    config.set("model", selection_table(lua, &commit.config)?)?;
    config.set("mode", commit.config.mode.id().key())?;
    config.set("options", options)?;
    Ok((Some(config), None))
}

/// Set one option: workflow, yolo or a plugin option. Model, thinking, fast
/// and mode have typed setters (`set_model`, `set_mode`) and are refused
/// here. workflow applies to this agent. yolo and plugin options belong to
/// the session, so they are set on its root and refused on a child. Plugin
/// options run their owner's validator first.
/// See [Configuration changes](/docs/modes/#configuration-changes).
/// @param ctx LuaCtx Invocation context.
/// @param id string Option id.
/// @param value string Selectable value id.
/// @return (boolean?, string?) Success.
#[lua_fn(name = "set_option")]
async fn agent_set_option(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    id: String,
    value: String,
) -> mlua::Result<Pair<bool>> {
    if TYPED_OPTIONS.contains(&id.as_str()) || id == MODE_KEY {
        return Ok(err_pair(TYPED_OPTION_SETTER));
    }
    try_pair!(authorize(&lua, &ctx, &this));
    try_pair!(validate_plugin_effect(&lua));
    try_pair!(ensure_validation_not_in_progress(&lua));
    let target = try_pair!(this.manager.lookup(this.id));
    if try_pair!(target.snapshot()).parent_id.is_some() {
        if id != WORKFLOW_OPTION_ID {
            return Ok(err_pair(SESSION_LEVEL_OPTION));
        }
        try_pair!(set_child_workflow(&lua, this, ctx, &target, value).await);
        return Ok((Some(true), None));
    }
    let coordinator = try_pair!(session_coordinator(&lua, &this));
    drop(this);
    drop(ctx);
    let snapshot = coordinator.read().options();
    if let Some(option) = snapshot
        .options
        .iter()
        .find(|option| option.definition.id.as_ref() == id)
    {
        try_pair!(validate_option_value(&lua, option, &value));
    }
    try_pair!(
        coordinator
            .set_option_if_version(id, value, Some(snapshot.version))
            .await
    );
    Ok((Some(true), None))
}

fn toggle_value(enabled: bool) -> &'static str {
    if enabled {
        ENABLED_VALUE
    } else {
        DISABLED_VALUE
    }
}

/// A child's workflow is its own config, committed through its config FIFO
/// like `set_model`.
async fn set_child_workflow(
    lua: &Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    target: &AgentRef,
    value: String,
) -> Result<(), String> {
    let enabled = match value.as_str() {
        ENABLED_VALUE => true,
        DISABLED_VALUE => false,
        _ => {
            return Err(SessionOptionError::InvalidValue {
                id: WORKFLOW_OPTION_ID.into(),
                value: value.into(),
            }
            .to_string());
        }
    };
    let services = orchestration::services(lua).ok_or(NO_ORCHESTRATION_SERVICES)?;
    services.validate_target(target)?;
    orchestration::commit_config_change(&*services, target, async |publication, _| {
        let claim = ctx.agent_publication_claim(lua, target, &publication)?;
        authorize(lua, &ctx, &this)?;
        drop(this);
        drop(ctx);
        let patch = ConfigPatch {
            workflow: Some(enabled),
            ..Default::default()
        };
        Ok((claim, ConfigChange::Patch(patch)))
    })
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// List authenticated, policy-filtered model spec strings without changing focus.
/// @param ctx LuaCtx Invocation context.
/// @return (table?, string?) Array of available model spec strings.
#[lua_fn(name = "available_models")]
fn agent_available_models(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let service =
        try_pair!(orchestration::services(lua).ok_or("host model catalog is unavailable"));
    let target = try_pair!(this.manager.lookup(this.id));
    let models = try_pair!(service.available_models(&target));
    Ok((Some(lua.create_sequence_from(models)?), None))
}

/// Read a bounded committed transcript, optionally through a completed turn.
/// Omitted through_turn selects the latest history-bearing terminal turn; no
/// completed history returns an empty snapshot. Pending and unknown turns fail
/// explicitly. A boundary removed by compaction returns transcript_unavailable.
/// Snapshots are bounded history views, not an archive immune to compaction.
/// @param ctx LuaCtx Invocation context.
/// @param opts table Required last_messages (1..1000) and max_bytes (2..1048576); optional through_turn. max_bytes bounds compact JSON of the messages array, not metadata.
/// @return (table?, string?) agent_id, through_turn, messages, epoch, total_messages, omitted_messages, omitted_tool_groups, compacted, bytes, truncated. Tool-call/response groups are indivisible; omitted_tool_groups counts omitted groups, not calls. compacted identifies a history containing a compaction summary, independently of bounded truncation.
#[lua_fn(name = "transcript")]
fn agent_transcript(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    opts: Table,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(lua, &ctx, this));
    let through_turn = opts.get::<Option<String>>("through_turn")?;
    let through_turn = try_pair!(through_turn.map(|id| id.parse::<TurnId>()).transpose());
    let actor = try_pair!(this.manager.actor(this.id));
    let snapshot = try_pair!(actor.transcript(TranscriptRequest {
        through_turn,
        last_messages: opts.get::<usize>("last_messages")?,
        max_bytes: opts.get::<usize>("max_bytes")?,
    }));
    let table = lua.create_table()?;
    table.set("agent_id", this.identity())?;
    table.set("messages", json_to_lua(lua, &snapshot.messages)?)?;
    table.set(
        "through_turn",
        snapshot.through_turn.map(|id| id.to_string()),
    )?;
    table.set("epoch", snapshot.epoch)?;
    table.set("total_messages", snapshot.total_messages)?;
    table.set("omitted_messages", snapshot.omitted_messages)?;
    table.set("omitted_tool_groups", snapshot.omitted_tool_groups)?;
    table.set("compacted", snapshot.compacted)?;
    table.set("bytes", snapshot.bytes)?;
    table.set("truncated", snapshot.truncated)?;
    Ok((Some(table), None))
}

/// Return another nonowning handle to this agent.
/// @return (AgentRef?, string?) Stable reference.
#[lua_fn(name = "ref")]
fn agent_ref(_lua: &Lua, this: &LuaAgent) -> mlua::Result<Pair<LuaAgentRef>> {
    Ok((Some(this.reference()), None))
}

fn admit(
    lua: &Lua,
    agent: &LuaAgent,
    ctx: &LuaCtx,
    message: String,
    options: TurnAdmissionOptions,
    submit: bool,
) -> Result<LuaAgentTurn, String> {
    authorize(lua, ctx, agent)?;
    let actor = agent
        .manager
        .actor(agent.id)
        .map_err(|error| error.to_string())?;
    actor.effective_config().ok_or(AGENT_CONFIG_UNAVAILABLE)?;
    let input = AgentInput {
        message,
        images: Vec::new(),
        preamble: Vec::new(),
        prompt: None,
    };
    let provenance = if let Ok(trusted) = ctx.trusted(lua) {
        TurnProvenance {
            origin: TurnOrigin::Plugin,
            plugin: Some(trusted.plugin.to_string()),
            plugin_generation: Some(trusted.generation),
            ..Default::default()
        }
    } else {
        let current = current_managed_turn(lua);
        let plugin = current_plugin(lua);
        TurnProvenance {
            origin: TurnOrigin::Plugin,
            plugin: plugin.as_ref().map(|plugin| plugin.0.to_string()),
            plugin_generation: plugin.map(|plugin| plugin.1),
            source_agent: current.as_ref().map(CurrentManagedTurn::agent_id),
            source_turn: current.as_ref().map(CurrentManagedTurn::turn_id),
        }
    };
    let options = TurnAdmissionOptions {
        provenance,
        ..options
    };
    let ticket = if submit {
        actor.submit(input, None, String::new(), options)
    } else {
        actor.admit_turn_with_options(input, None, String::new(), options)
    }
    .map_err(|error| match error {
        ActorError::Busy => AGENT_BUSY.to_owned(),
        ActorError::StaleTurn => STALE_TURN.to_owned(),
        other => other.to_string(),
    })?;
    Ok(LuaAgentTurn {
        agent: agent.clone(),
        ticket,
    })
}

/// When a turn a plugin sends starts.
enum When {
    /// Only when the agent has no active, preparing or queued work.
    Idle,
    /// Join compatible admitted work, or start a turn when there is none.
    Join,
    /// A distinct FIFO turn behind already admitted work.
    Queue,
}

/// Reads `send`'s options; a malformed table is a programmer error.
fn send_options(opts: &Table) -> mlua::Result<(When, TurnAdmissionOptions)> {
    let when = match opts.get::<Option<String>>("when")?.as_deref() {
        Some("idle") => When::Idle,
        Some("join") => When::Join,
        Some("queue") => When::Queue,
        _ => return Err(mlua::Error::runtime(SEND_WHEN_REQUIRED)),
    };
    let existing_only = opts.get::<Option<bool>>("existing_only")?;
    let after_turn = opts
        .get::<Option<String>>("after_turn")?
        .map(|id| id.parse::<TurnId>().map_err(mlua::Error::runtime))
        .transpose()?;
    if existing_only.is_some() && !matches!(when, When::Join) {
        return Err(mlua::Error::runtime(EXISTING_ONLY_NEEDS_JOIN));
    }
    if after_turn.is_some() && !matches!(when, When::Idle) {
        return Err(mlua::Error::runtime(AFTER_TURN_NEEDS_IDLE));
    }
    let options = TurnAdmissionOptions {
        idle_only: matches!(when, When::Idle),
        existing_only: existing_only.unwrap_or_default(),
        after_turn,
        ..Default::default()
    };
    Ok((when, options))
}

/// Send a user message and get the ticket of the turn that carries it.
/// `when` decides how the message meets other work: `"idle"` admits a turn
/// only when the agent has no active, preparing or queued work; `"join"`
/// joins compatible admitted work, reaching its next safe model request
/// without interrupting a provider request or tool, or starts a turn when
/// there is none; `"queue"` queues a distinct turn behind admitted work.
/// Wait for the result with `AgentTurn:wait`.
/// @param ctx LuaCtx Invocation context.
/// @param message string User message (text only).
/// @param opts table Required `when` ("idle", "join" or "queue"). `existing_only` (join only) joins compatible work or returns busy without starting a turn. `after_turn` (idle only) must match the latest terminal turn, including failure or cancellation.
/// @return (AgentTurn?, string?) Exact-turn ticket, or an admission error. Bounded inbox rejection does not cancel accepted work.
#[lua_fn(name = "send")]
fn agent_send(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
    message: String,
    opts: Table,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    let (when, options) = send_options(&opts)?;
    let turn = try_pair!(admit(
        lua,
        this,
        &ctx,
        message,
        options,
        matches!(when, When::Join),
    ));
    Ok((Some(turn), None))
}

/// Read the most recently admitted turn, including turns the runtime admitted
/// to deliver a child's result.
/// @param ctx LuaCtx Invocation context.
/// @return (AgentTurn?, string?) Exact-turn ticket, or nil without an error when no turn was admitted or it is no longer retained.
#[lua_fn(name = "latest_turn")]
fn agent_latest_turn(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<LuaAgentTurn>> {
    try_pair!(observe(lua, &ctx, this));
    let actor = try_pair!(this.manager.actor(this.id));
    Ok((
        actor.latest_ticket().map(|ticket| LuaAgentTurn {
            agent: this.clone(),
            ticket,
        }),
        None,
    ))
}

/// Switch model through a reserved actor configuration operation.
/// @param ctx LuaCtx Invocation context.
/// @param patch string|table Spec string or partial spec/thinking/fast selection.
/// @return (table?, string?) Complete normalized committed selection {spec, thinking, fast}, accepted as a subsequent patch subject to destination policy.
#[lua_fn(name = "set_model")]
async fn agent_set_model(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    patch: LuaValue,
) -> mlua::Result<Pair<Table>> {
    try_pair!(authorize(&lua, &ctx, &this));
    let selection = try_pair!(selection_from_lua(&lua, patch));
    let spec = selection.spec.map(|spec| spec.to_string());
    let template = try_pair!(destination_template(&lua, &this));
    let target = try_pair!(this.manager.lookup(this.id));
    let services = try_pair!(orchestration::services(&lua).ok_or(NO_ORCHESTRATION_SERVICES));
    try_pair!(services.validate_target(&target));
    let commit = orchestration::commit_config_change(
        &*services,
        &target,
        async |publication, predecessor| {
            let claim = ctx.agent_publication_claim(&lua, &target, &publication)?;
            let mut captured = template.to_tool_context();
            captured.apply_effective_config(&predecessor);
            let template = AgentContext::from(&captured);
            let prepare = lua
                .app_data_ref::<SessionProviderPreparer>()
                .map(|prepare| Arc::clone(&prepare));
            let model = match spec {
                Some(_) => {
                    let (model, provider) =
                        build_session_provider(&spec, false, &template, prepare).await?;
                    Some(PreparedModel { model, provider })
                }
                None => None,
            };
            authorize(&lua, &ctx, &this)?;
            drop(this);
            drop(ctx);
            let patch = ConfigPatch {
                model,
                thinking: selection.thinking,
                fast: selection.fast,
                ..Default::default()
            };
            Ok((claim, ConfigChange::Patch(patch)))
        },
    )
    .await;
    let commit = try_pair!(commit);
    Ok((Some(selection_table(&lua, &commit.config)?), None))
}

fn mode_is_restricted(context: &AgentContext) -> bool {
    context.restrict_write_to().is_some()
        || context
            .mode_def
            .as_ref()
            .is_some_and(|mode| mode.tools.is_some())
}

/// Switch mode through the actor configuration FIFO.
/// @param ctx LuaCtx Invocation context.
/// @param name string Registered mode name.
/// @return (string?, string?) Committed mode name or error.
#[lua_fn(name = "set_mode")]
async fn agent_set_mode(
    lua: Lua,
    this: UserDataRef<LuaAgent>,
    ctx: UserDataRef<LuaCtx>,
    name: String,
) -> mlua::Result<Pair<String>> {
    try_pair!(authorize(&lua, &ctx, &this));
    let target = try_pair!(this.manager.lookup(this.id));
    let services = try_pair!(orchestration::services(&lua).ok_or(NO_ORCHESTRATION_SERVICES));
    try_pair!(services.validate_target(&target));
    let template = try_pair!(destination_template(&lua, &this));
    if ctx.trusted(&lua).is_err()
        && (mode_is_restricted(&template) || ctx.agent().is_some_and(mode_is_restricted))
    {
        return Ok(err_pair(RESTRICTED_MODE_CHANGE));
    }
    let structured = this
        .state
        .as_ref()
        .is_some_and(|state| state.structured.is_some());
    let state_dir = try_pair!(host_state_dir(&lua));
    let ticket = try_pair!(orchestration::reserve_config_change(&*services, &target));
    let commit = ticket
        .commit_mode(
            &target,
            &template.modes,
            &name,
            &state_dir,
            None,
            async |publication, definition| {
                validate_required_output_mode(structured, Some(definition))?;
                let claim = ctx.agent_publication_claim(&lua, &target, &publication)?;
                authorize(&lua, &ctx, &this)?;
                drop(this);
                drop(ctx);
                Ok(claim)
            },
        )
        .await;
    let commit = try_pair!(commit);
    Ok((Some(commit.config.mode.id().key().to_string()), None))
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

/// Close this agent at the end of the current host task, even on cancellation.
/// @param ctx LuaCtx Invocation context.
/// @return (boolean?, string?) Cleanup registered.
#[lua_fn(name = "defer_close")]
fn agent_defer_close(
    lua: &Lua,
    this: &LuaAgent,
    ctx: UserDataRef<LuaCtx>,
) -> mlua::Result<Pair<bool>> {
    try_pair!(authorize(lua, &ctx, this));
    let manager = this.manager.clone();
    let id = this.id;
    let sessions = lua
        .app_data_ref::<AgentOwners>()
        .map(|sessions| sessions.clone());
    try_pair!(register_defer_close(
        lua,
        Box::new(move || {
            let _ = manager.close_subtree(id);
            if let Some(sessions) = sessions {
                sessions.retire_settled();
            }
        })
    ));
    Ok((Some(true), None))
}

// Bundled task plugin internals, not public API. `_maki_session_id` reads the
// task/UI identity. `_maki_task_owner` returns a host-stamped guard whose
// `_maki_can_control` checks plugin generation, host and parent branch, and whose
// `_maki_retired` reports a closed child. The guard authorizes no effect.
fn agent_internal_methods<M: UserDataMethods<LuaAgent>>(methods: &mut M) {
    methods.add_method("_maki_task_owner", |lua, this, ctx: UserDataRef<LuaCtx>| {
        authorize(lua, &ctx, this).map_err(mlua::Error::runtime)?;
        let (plugin, generation) =
            current_plugin(lua).ok_or_else(|| mlua::Error::runtime(NO_PLUGIN_AUTHORITY))?;
        Ok(LuaTaskOwner {
            owner_id: this.state.as_ref().and_then(|state| state.parent_agent_id),
            owner: plugin_authority(lua, plugin.clone()),
            plugin,
            generation,
            target: this
                .manager
                .lookup(this.id)
                .map_err(mlua::Error::external)?,
        })
    });
    methods.add_method("_maki_session_id", |_, this, ()| {
        Ok(this
            .state
            .as_ref()
            .map(|state| state.ui_id.clone())
            .unwrap_or_else(|| this.identity()))
    });
}

lua_class! {
    /// A nonowning agent handle. Garbage collection never closes its actor.
    /// Reads and effects require current invocation authority; identity is context-free.
    /// A held managed handle retains authorized closed-status reads, not mutation rights.
    "maki.agent.Agent" => LuaAgent, AGENT_DOCS [
        agent_id, agent_latest_turn, agent_status, agent_config, agent_available_models,
        agent_transcript, agent_ref, agent_send, agent_set_model,
        agent_set_mode, agent_set_option, agent_cancel, agent_cancel_subtree, agent_close, agent_defer_close,
        on_turn_start, on_turn_end, on_idle, on_config_change, on_close,
    ]
    extra agent_internal_methods
}
