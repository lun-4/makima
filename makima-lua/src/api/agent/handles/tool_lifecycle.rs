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
use crate::docs::{FnDoc, ParamDoc};
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

#[allow(non_upper_case_globals)]
pub(super) const tool_transition__doc: FnDoc = FnDoc {
    name: "tool_transition",
    args: "{ctx}, {name}, {reduce}",
    desc: "Reduce actor-ordered facts for your opt-in tool on a pinned TUI root with a live session lease. The synchronous reducer must not yield. It receives {facts, latest, plan, faulted, closing, record}; facts are exposed {turn, scope}, settled {turn, outcome}, or stopped {reason}. latest is the latest settled turn ID. record:read(key) and record:write(key, table) are bounded, lease-checked sidecar operations valid only inside the reducer. A missing record returns nil without an error. Return {revision, scope} to publish an active revision, or nil to disarm and detach. This does not attach a detached tool. A failed reducer disarms the attachment.",
    params: &[
        ParamDoc {
            name: "{ctx}",
            ty: "LuaCtx",
            desc: "Trusted invocation pinned to this exact root.",
        },
        ParamDoc {
            name: "{name}",
            ty: "string",
            desc: "An opt-in tool owned by the invoking plugin, with a lifecycle controller.",
        },
        ParamDoc {
            name: "{reduce}",
            ty: "function",
            desc: "Synchronous reducer. Use opaque IDs from new_revision_id() for activation revisions and counter scopes.",
        },
    ],
    returns: "(table?, string?) On success, may carry an opaque wake fence in wake when the revision is active and there is a settled turn.",
    example: "",
    guard: None,
};

#[allow(non_upper_case_globals)]
pub(super) const wake_with_tool__doc: FnDoc = FnDoc {
    name: "wake_with_tool",
    args: "{ctx}, {wake}, {message}",
    desc: "Ask the TUI to admit an idle turn for a lifecycle-controlled attachment. The host first gives ordinary input and child delivery priority, then checks the exact settled turn, attachment, policy, and history fence. Busy or stale wakes are dropped, not queued. A successful return means only that the TUI received the request for consideration.",
    params: &[
        ParamDoc {
            name: "{ctx}",
            ty: "LuaCtx",
            desc: "Trusted invocation pinned to the same root.",
        },
        ParamDoc {
            name: "{wake}",
            ty: "userdata",
            desc: "Fence returned by tool_transition.",
        },
        ParamDoc {
            name: "{message}",
            ty: "string",
            desc: "User-role text for the new turn.",
        },
    ],
    returns: "(boolean?, string?) Success or a routing error.",
    example: "",
    guard: None,
};

#[allow(non_upper_case_globals)]
pub(super) const new_revision_id__doc: FnDoc = FnDoc {
    name: "new_revision_id",
    args: "",
    desc: "Generate a fresh opaque ID for a lifecycle controller revision or scope.",
    params: &[],
    returns: "(string) Opaque ID.",
    example: "",
    guard: None,
};

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
