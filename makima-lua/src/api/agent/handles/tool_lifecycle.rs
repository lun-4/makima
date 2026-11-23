use std::sync::Arc;

use makima_agent::actor::{ToolIdleFence, ToolTransition};
use makima_agent::tools::{ToolRegistry, ToolSource};
use makima_agent::{AgentId, AgentManagerHandle, CancelToken, TurnId};
use makima_storage::id::MakimaId;
use makima_storage::plugin_session::{PluginRecord, PluginSession};
use mlua::{Function, Lua, LuaSerdeExt, Table, UserData, UserDataMethods, UserDataRef};

use super::LuaAgent;
use crate::api::session_option::ensure_validation_not_in_progress;
use crate::api::util::command::{UiAction, ui_send};
use crate::api::util::ctx::LuaCtx;
use crate::api::util::pair::{err_pair, try_pair};
use crate::runtime::{active_task_id, current_plugin, in_delegated_tool, validate_plugin_effect};

struct RecordAccess {
    session: PluginSession,
    owner: Arc<str>,
    task: Option<u64>,
    cancel: CancelToken,
}

impl RecordAccess {
    fn validate(&self, lua: &Lua) -> Result<(), String> {
        validate_plugin_effect(lua)?;
        if self.cancel.is_cancelled()
            || self.task != active_task_id(lua)
            || !current_plugin(lua).is_some_and(|(plugin, _)| plugin == self.owner)
        {
            return Err("session record access has expired".into());
        }
        Ok(())
    }
}

impl UserData for RecordAccess {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("read", |lua, this, key: String| {
            try_pair!(this.validate(lua));
            let record = try_pair!(this.session.read(&this.owner, &key));
            Ok((
                record.map(|record| lua.to_value(&record)).transpose()?,
                None,
            ))
        });
        methods.add_method("write", |lua, this, (key, record): (String, Table)| {
            try_pair!(this.validate(lua));
            let record: PluginRecord = lua.from_value(mlua::Value::Table(record))?;
            try_pair!(this.session.write(&this.owner, &key, &record));
            Ok((Some(true), None))
        });
    }
}

struct Reservation<'a> {
    manager: &'a AgentManagerHandle,
    agent: AgentId,
    name: &'a str,
    transition: ToolTransition,
    _cancel: makima_agent::CancelTrigger,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        // Do not touch Lua: on an error, leave the attachment disabled.
        self.manager
            .abandon_tool_transition(self.agent, self.name, &self.transition);
    }
}

struct IdleWake {
    fence: ToolIdleFence,
    agent: AgentId,
    manager: AgentManagerHandle,
    after: TurnId,
    owner: Arc<str>,
    generation: u64,
    revoke: CancelToken,
}
impl UserData for IdleWake {}

pub(crate) fn register_methods<M: UserDataMethods<LuaAgent>>(methods: &mut M) {
    methods.add_method(
        "tool_transition",
        |lua, agent, (ctx, name, reduce): (UserDataRef<LuaCtx>, String, Function)| {
            if in_delegated_tool(lua) {
                return Ok(err_pair("tool transitions require trusted authority"));
            }
            try_pair!(ensure_validation_not_in_progress(lua));
            let trusted = try_pair!(ctx.trusted(lua));
            if trusted.target.id() != agent.id
                || !trusted.target.manager().same_manager(&agent.manager)
            {
                return Ok(err_pair("tool transitions require the exact pinned root"));
            }
            try_pair!(trusted.validate_target(lua, &trusted.target));
            let registry = try_pair!(
                lua.app_data_ref::<Arc<ToolRegistry>>()
                    .ok_or("tool registry is unavailable")
            );
            let entry = try_pair!(registry.get(&name).ok_or("tool is not registered"));
            if !matches!(&entry.source, ToolSource::Lua { plugin } if *plugin == trusted.plugin) {
                return Ok(err_pair("tool transitions require the owning plugin"));
            }
            drop(registry);
            let session = try_pair!(
                agent
                    .manager
                    .plugin_session()
                    .ok_or("session has no live sidecar lease")
            );
            if trusted.template.session_id.as_ref().map(|id| id.id()) != Some(session.id()) {
                return Ok(err_pair("plugin session does not match the pinned root"));
            }
            let transition = try_pair!(agent.manager.begin_tool_transition(
                agent.id,
                entry,
                trusted.plugin_revocation()
            ));
            let (cancel, token) = CancelToken::new();
            let reservation = Reservation {
                manager: &agent.manager,
                agent: agent.id,
                name: &name,
                transition,
                _cancel: cancel,
            };
            let transition = &reservation.transition;
            let boundary = lua.create_table()?;
            boundary.set("facts", lua.to_value(&transition.facts)?)?;
            boundary.set("latest", transition.latest.map(|id| id.to_string()))?;
            boundary.set("plan", transition.plan)?;
            boundary.set("faulted", transition.faulted)?;
            boundary.set("closing", transition.closing)?;
            boundary.set(
                "record",
                RecordAccess {
                    session,
                    owner: trusted.plugin.clone(),
                    task: active_task_id(lua),
                    cancel: token,
                },
            )?;
            // A reducer must not yield across the transition reservation. In
            // particular no other Lua command can interleave with record publication.
            let active: Option<Table> = try_pair!(
                smol::block_on(futures_lite::future::poll_once(reduce.call_async(boundary)))
                    .unwrap_or_else(|| Err(mlua::Error::runtime("tool reducer cannot yield")))
            );
            let active = active
                .map(|active| -> mlua::Result<_> {
                    let parse = |key| {
                        active
                            .get::<String>(key)?
                            .parse::<MakimaId>()
                            .map_err(mlua::Error::runtime)
                    };
                    Ok((parse("revision")?, parse("scope")?))
                })
                .transpose()?;
            try_pair!(trusted.validate_target(lua, &trusted.target));
            let fence = try_pair!(
                agent
                    .manager
                    .finish_tool_transition(agent.id, &name, transition, active)
            );
            let reply = lua.create_table()?;
            if let (Some(fence), Some(after)) = (fence, transition.latest) {
                reply.set(
                    "wake",
                    IdleWake {
                        fence,
                        agent: agent.id,
                        manager: agent.manager.clone(),
                        after,
                        owner: trusted.plugin.clone(),
                        generation: trusted.generation,
                        revoke: trusted.plugin_revocation().clone(),
                    },
                )?;
            }
            Ok((Some(reply), None))
        },
    );
    methods.add_method(
        "wake_with_tool",
        |lua, agent, (ctx, wake, message): (UserDataRef<LuaCtx>, UserDataRef<IdleWake>, String)| {
            let trusted = try_pair!(ctx.trusted(lua));
            if in_delegated_tool(lua)
                || wake.revoke.is_cancelled()
                || wake.agent != agent.id
                || !wake.manager.same_manager(&agent.manager)
                || trusted.target.id() != agent.id
                || !trusted.target.manager().same_manager(&agent.manager)
                || trusted.plugin != wake.owner
                || trusted.generation != wake.generation
            {
                return Ok(err_pair("tool wake has expired"));
            }
            try_pair!(trusted.validate_target(lua, &trusted.target));
            let sender = lua.app_data_ref::<flume::Sender<UiAction>>();
            try_pair!(ui_send(
                sender.as_deref(),
                UiAction::ToolWake {
                    root: trusted.target.clone(),
                    message,
                    fence: wake.fence.clone(),
                    after: wake.after,
                    plugin: wake.owner.clone(),
                    generation: wake.generation,
                }
            ));
            Ok((Some(true), None))
        },
    );
    methods.add_method("new_revision_id", |_, _, ()| {
        Ok(MakimaId::generate().to_string())
    });
}
