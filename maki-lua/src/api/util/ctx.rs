use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use maki_agent::agent::LoadedInstructions;
use maki_agent::cancel::CancelToken;
use maki_agent::session_coordinator::ModelMutationValidator;
use maki_agent::tools::{FileReadTracker, QuestionMode, ToolAudience, ToolContext, ToolLive};
use maki_agent::{AgentEvent, AgentId};
use maki_config::{AgentConfig, ToolOutputLines};
use maki_storage::id::SessionRef;
use mlua::{LuaSerdeExt, MultiValue, UserData, UserDataMethods, Value as LuaValue};

use crate::agent_errors::{INVOCATION_EXPIRED, OTHER_RUNTIME};
use crate::api::tool::ToolCallReply;
use crate::api::ui::buf::BufHandle;
use crate::api::util::convert::json_to_lua;
use crate::api::util::pair::{Pair, try_pair};
use crate::host_root::{HostRootRef, HostRootServices};
use crate::orchestration::{HostRootServicesSlot, TrustedHostRootContext};
use crate::runtime::{active_task, lock_cell};

const DEADLINE_ALREADY_SET_MSG: &str = "ctx:set_deadline() already called";

fn send_live_buf(lua: &mlua::Lua, buf: &mlua::AnyUserData) -> mlua::Result<()> {
    let shared = buf.borrow::<BufHandle>().map(|h| Arc::clone(&h.buf))?;
    let task = active_task(lua);
    let (live, sink) = {
        let mut cell = lock_cell(&task);
        cell.root_buf = Some(Arc::clone(&shared));
        (cell.live.clone(), cell.live_sink.clone())
    };
    if let Some(live) = live {
        let _ = live.event_tx.send(maki_agent::AgentEvent::LiveToolBuf {
            id: live.tool_use_id.clone(),
            body: Arc::clone(&shared),
        });
    }
    if let Some(sink) = sink {
        let _ = sink.send(ToolLive::Buf(shared));
    }
    Ok(())
}

/// Captured snapshot of the parent `ToolContext`. Per-call state that must
/// not leak across sibling calls (instructions, output lines, local tools) is
/// reset so child calls start clean. The deadline is preserved: a nested
/// `call_tool` must cap itself to the parent's deadline, never extend it.
#[derive(Clone)]
pub(crate) struct AgentContext(ToolContext);

impl From<&ToolContext> for AgentContext {
    fn from(ctx: &ToolContext) -> Self {
        let mut c = ctx.clone();
        c.loaded_instructions = LoadedInstructions::new();
        c.tool_output_lines = ToolOutputLines::default();
        Self(c)
    }
}

impl Deref for AgentContext {
    type Target = ToolContext;
    fn deref(&self) -> &ToolContext {
        &self.0
    }
}

impl AgentContext {
    /// Drops `tool_use_id` so an inner tool never emits UI events under the
    /// outer call's id, and `live_sink` so a grandchild never streams into
    /// a sink meant for its parent.
    pub(crate) fn to_tool_context(&self) -> ToolContext {
        let mut c = self.0.clone();
        c.tool_use_id = None;
        c.live_sink = None;
        c
    }
}

/// One ctx type for handler, `start`, and restore invocations. Each kind's
/// capabilities live in its `Caps` variant, so a capability exists exactly
/// when its data does. Methods a kind lacks return `(nil, err)` instead of
/// not existing, so callers can probe without pcall.
pub(crate) struct LuaCtx {
    caps: Caps,
    origin: Option<crate::runtime::TaskHandle>,
    host_services: Option<Arc<HostRootServices>>,
    pub(crate) subject: Option<AgentId>,
    pub(crate) cancel: CancelToken,
    tool_output_lines: ToolOutputLines,
    pub(crate) finish_tx: Option<flume::Sender<ToolCallReply>>,
}

enum Caps {
    TrustedHostRoot {
        authority: Box<TrustedHostRootContext>,
    },
    Trusted {
        authority: Box<crate::orchestration::TrustedContext>,
        agent: Box<AgentContext>,
    },
    Handler {
        agent: Box<AgentContext>,
        /// Kept apart from `agent`, which resets its copy so child calls
        /// start with a clean instruction set.
        loaded_instructions: LoadedInstructions,
    },
    /// `start` runs before permission checks: it reads config and publishes
    /// previews, but dispatching tools is structurally impossible.
    Start {
        config: AgentConfig,
        workflow: bool,
        audience: ToolAudience,
        session_id: Option<SessionRef>,
    },
    Restore {
        state: Option<serde_json::Value>,
    },
}

impl LuaCtx {
    fn new(ctx: &ToolContext, caps: Caps) -> Self {
        Self {
            caps,
            origin: None,
            host_services: None,
            subject: None,
            cancel: ctx.cancel.clone(),
            tool_output_lines: ctx.tool_output_lines,
            finish_tx: None,
        }
    }

    pub(crate) fn pin_host_root(&mut self, lua: &mlua::Lua) {
        self.host_services = lua
            .app_data_ref::<HostRootServicesSlot>()
            .filter(|slot| {
                self.agent().is_some_and(|agent| {
                    agent.managed_turn.is_none()
                        && slot.services.as_ref().is_some_and(|services| {
                            agent.execution_agent_id == Some(services.reference().id())
                        })
                })
            })
            .and_then(|slot| slot.services.clone());
        self.subject = self
            .host_services
            .as_ref()
            .map(|services| services.reference().id());
        self.pin_subject(lua);
    }

    pub(crate) fn bind_origin(&mut self, task: &crate::runtime::TaskHandle) {
        self.origin = Some(task.clone());
        if let Some(agent) = self.agent() {
            lock_cell(task).agent_parent = Some(agent.subagent_cancels.clone());
        }
    }

    pub(crate) fn delegated_tool(&self) -> crate::runtime::DelegatedTool {
        crate::runtime::DelegatedTool {
            session: self.session_id().flatten().map(SessionRef::id),
        }
    }

    pub(crate) fn validate_origin(&self, lua: &mlua::Lua) -> Result<(), String> {
        if matches!(self.caps, Caps::Trusted { .. }) {
            return self.trusted(lua).map(|_| ());
        }
        if let Caps::TrustedHostRoot { authority } = &self.caps {
            return authority.validate(lua);
        }
        validate_task_origin(lua, self.origin.as_ref(), false)
    }

    pub(crate) fn handler(ctx: &ToolContext) -> Self {
        Self::new(
            ctx,
            Caps::Handler {
                agent: Box::new(AgentContext::from(ctx)),
                loaded_instructions: ctx.loaded_instructions.clone(),
            },
        )
    }

    pub(crate) fn start(ctx: &ToolContext) -> Self {
        Self::new(
            ctx,
            Caps::Start {
                config: ctx.config.clone(),
                workflow: ctx.workflow,
                audience: ctx.audience,
                session_id: ctx.session_id.clone(),
            },
        )
    }

    pub(crate) fn restore(
        tool_output_lines: ToolOutputLines,
        state: Option<serde_json::Value>,
    ) -> Self {
        Self {
            caps: Caps::Restore { state },
            origin: None,
            host_services: None,
            subject: None,
            cancel: CancelToken::none(),
            tool_output_lines,
            finish_tx: None,
        }
    }

    pub(crate) fn trusted_context(
        lua: &mlua::Lua,
        target: crate::orchestration::TrustedTarget,
        plugin: Arc<str>,
    ) -> Self {
        let agent = Box::new(AgentContext::from(&target.template));
        let cancel = lock_cell(&active_task(lua)).cancel.clone();
        let task = active_task(lua);
        let owner = lock_cell(&task)
            .plugin_owner
            .clone()
            .unwrap_or_else(|| crate::orchestration::plugin_authority(lua, plugin.clone()));
        let authority = Box::new(crate::orchestration::TrustedContext::new(
            lua,
            target,
            &task,
            plugin,
            owner.clone(),
        ));
        {
            let mut cell = lock_cell(&task);
            cell.plugin_origin = Some((authority.plugin.clone(), authority.generation));
            cell.plugin_owner = Some(owner);
        }
        Self {
            caps: Caps::Trusted { authority, agent },
            origin: None,
            host_services: None,
            subject: None,
            cancel,
            tool_output_lines: ToolOutputLines::default(),
            finish_tx: None,
        }
    }

    pub(crate) fn trusted(
        &self,
        lua: &mlua::Lua,
    ) -> Result<&crate::orchestration::TrustedContext, String> {
        let Caps::Trusted { authority, .. } = &self.caps else {
            return Err(self.cap_err("orchestration"));
        };
        authority.validate(lua)?;
        Ok(authority)
    }

    pub(crate) fn trusted_host_root_context(
        lua: &mlua::Lua,
        plugin: Arc<str>,
    ) -> Result<Option<Self>, String> {
        crate::runtime::validate_plugin_effect(lua)?;
        let task = active_task(lua);
        {
            let cell = lock_cell(&task);
            if cell.delegated_tool.is_some() {
                return Err("delegated tools cannot mint host root authority".into());
            }
            if cell.managed_turn.is_some() {
                return Ok(None);
            }
        }
        let owner = lock_cell(&task)
            .plugin_owner
            .clone()
            .unwrap_or_else(|| crate::orchestration::plugin_authority(lua, plugin.clone()));
        let Some(authority) =
            TrustedHostRootContext::new(lua, &task, plugin.clone(), owner.clone())?
        else {
            return Ok(None);
        };
        let mut cell = lock_cell(&task);
        if cell
            .plugin_origin
            .as_ref()
            .is_some_and(|(captured, generation)| {
                captured != &plugin || *generation != authority.generation
            })
        {
            return Err("plugin generation expired before host root admission".into());
        }
        cell.plugin_origin = Some((authority.plugin.clone(), authority.generation));
        cell.plugin_owner = Some(owner);
        Ok(Some(Self {
            caps: Caps::TrustedHostRoot {
                authority: Box::new(authority),
            },
            origin: None,
            host_services: None,
            subject: None,
            cancel: cell.cancel.clone(),
            tool_output_lines: ToolOutputLines::default(),
            finish_tx: None,
        }))
    }

    pub(crate) fn trusted_host_root(
        &self,
        lua: &mlua::Lua,
    ) -> Result<&TrustedHostRootContext, String> {
        let Caps::TrustedHostRoot { authority } = &self.caps else {
            return Err(self.cap_err("host root orchestration"));
        };
        authority.validate(lua)?;
        Ok(authority)
    }

    pub(crate) fn host_root(
        &self,
        lua: &mlua::Lua,
    ) -> Result<Option<Arc<HostRootServices>>, String> {
        self.validate_origin(lua)?;
        if crate::runtime::current_managed_turn(lua).is_some() {
            return Ok(None);
        }
        match &self.caps {
            Caps::TrustedHostRoot { authority } => return Ok(Some(authority.services.clone())),
            Caps::Handler { agent, .. } if agent.managed_turn.is_none() => {}
            _ => return Ok(None),
        }
        let Some(services) = &self.host_services else {
            return Ok(None);
        };
        if self.subject != Some(services.reference().id()) {
            return Err("host root execution subject mismatch".into());
        }
        let current = lua
            .app_data_ref::<HostRootServicesSlot>()
            .and_then(|slot| slot.services.clone())
            .ok_or("host root services expired")?;
        if !Arc::ptr_eq(services, &current) {
            return Err("host root context does not belong to this host scope".into());
        }
        services
            .validate(&services.reference())
            .map_err(|error| error.to_string())?;
        Ok(Some(services.clone()))
    }

    pub(crate) fn host_model_validator(
        &self,
        lua: &mlua::Lua,
        services: &Arc<HostRootServices>,
        reference: &HostRootRef,
    ) -> Result<ModelMutationValidator, String> {
        self.validate_origin(lua)?;
        let bound = self
            .host_root(lua)?
            .ok_or("host root authority is unavailable")?;
        if !Arc::ptr_eq(&bound, services) {
            return Err(OTHER_RUNTIME.into());
        }
        services
            .validate(reference)
            .map_err(|error| error.to_string())?;
        let task = active_task(lua);
        let plugin_origin = lock_cell(&task).plugin_origin.clone();
        let owner = lock_cell(&task).plugin_owner.clone();
        let task = Arc::downgrade(&task);
        let cancel = self.cancel.clone();
        let services = services.clone();
        let reference = reference.clone();
        Ok(Arc::new(move || {
            let task = task
                .upgrade()
                .ok_or_else(|| Arc::<str>::from(INVOCATION_EXPIRED))?;
            let cell = lock_cell(&task);
            if !cell.is_live()
                || cancel.is_cancelled()
                || cell.plugin_origin != plugin_origin
                || owner.as_ref().is_some_and(|owner| !owner.is_alive())
            {
                return Err(Arc::from(INVOCATION_EXPIRED));
            }
            services
                .validate(&reference)
                .map_err(|error| Arc::from(error.to_string()))
        }))
    }

    /// Restricted dispatch capability; trusted orchestration uses `trusted`.
    pub(crate) fn agent(&self) -> Option<&AgentContext> {
        match &self.caps {
            Caps::Handler { agent, .. } => Some(agent),
            _ => None,
        }
    }

    fn config(&self) -> Option<&AgentConfig> {
        match &self.caps {
            Caps::Handler { agent, .. } | Caps::Trusted { agent, .. } => Some(&agent.config),
            Caps::Start { config, .. } => Some(config),
            Caps::Restore { .. } | Caps::TrustedHostRoot { .. } => None,
        }
    }

    fn workflow(&self) -> Option<bool> {
        match &self.caps {
            Caps::Handler { agent, .. } | Caps::Trusted { agent, .. } => Some(agent.workflow),
            Caps::Start { workflow, .. } => Some(*workflow),
            Caps::Restore { .. } | Caps::TrustedHostRoot { .. } => None,
        }
    }

    fn audience(&self) -> Option<ToolAudience> {
        match &self.caps {
            Caps::Handler { agent, .. } | Caps::Trusted { agent, .. } => Some(agent.audience),
            Caps::Start { audience, .. } => Some(*audience),
            Caps::Restore { .. } | Caps::TrustedHostRoot { .. } => None,
        }
    }

    /// Outer `None` means the kind has no session at all, inner `None`
    /// means this run has one but it is not tied to a session.
    fn session_id(&self) -> Option<Option<&SessionRef>> {
        match &self.caps {
            Caps::Handler { agent, .. } | Caps::Trusted { agent, .. } => {
                Some(agent.session_id.as_ref())
            }
            Caps::Start { session_id, .. } => Some(session_id.as_ref()),
            Caps::Restore { .. } | Caps::TrustedHostRoot { .. } => None,
        }
    }

    fn file_tracker(&self) -> Option<&FileReadTracker> {
        self.agent().map(|a| &*a.file_tracker)
    }

    fn loaded_instructions(&self) -> Option<&LoadedInstructions> {
        match &self.caps {
            Caps::Handler {
                loaded_instructions,
                ..
            } => Some(loaded_instructions),
            _ => None,
        }
    }

    fn state(&self) -> Option<&serde_json::Value> {
        match &self.caps {
            Caps::Restore { state } => state.as_ref(),
            _ => None,
        }
    }

    fn kind(&self) -> &'static str {
        match self.caps {
            Caps::Trusted { .. } => "trusted",
            Caps::TrustedHostRoot { .. } => "trusted host root",
            Caps::Handler { .. } => "handler",
            Caps::Start { .. } => "start",
            Caps::Restore { .. } => "restore",
        }
    }

    pub(crate) fn cap_err(&self, method: &str) -> String {
        format!("{method} not available in {} ctx", self.kind())
    }

    fn cap_err_pair<T>(&self, method: &str) -> Pair<T> {
        (None, Some(self.cap_err(method)))
    }
}

impl UserData for LuaCtx {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("model", |lua, this, ()| {
            if let Err(error) = this.validate_origin(lua) {
                return Ok((None, Some(error)));
            }
            let Some(agent) = this.agent() else {
                return Ok(this.cap_err_pair("model"));
            };
            let selection =
                maki_providers::CompleteModelSelection::from_model(&agent.model, agent.opts);
            Ok((Some(lua.to_value(&selection)?), None))
        });
        methods.add_method("mode", |lua, this, ()| {
            try_pair!(this.validate_origin(lua));
            let Some(agent) = this.agent() else {
                return Ok(this.cap_err_pair("mode"));
            };
            Ok((Some(agent.mode.id().key().to_string()), None))
        });
        methods.add_method("cancelled", |_, this, ()| Ok(this.cancel.is_cancelled()));
        methods.add_function(
            "defer_close",
            |lua, (ctx, agent): (mlua::AnyUserData, mlua::AnyUserData)| {
                let ctx = ctx.borrow::<LuaCtx>()?;
                try_pair!(ctx.validate_origin(lua));
                let agent = agent.borrow::<crate::api::agent::LuaAgent>()?;
                crate::api::agent::agent_defer_close(lua, &agent, ctx)
            },
        );

        methods.add_method("workflow", |lua, this, ()| {
            let Some(workflow) = this.workflow() else {
                return Ok(this.cap_err_pair("workflow"));
            };
            try_pair!(this.validate_origin(lua));
            Ok((Some(workflow), None))
        });

        methods.add_method("audience", |lua, this, ()| {
            let Some(audience) = this.audience() else {
                return Ok(this.cap_err_pair("audience"));
            };
            try_pair!(this.validate_origin(lua));
            Ok((Some(audience.name().unwrap_or("main").to_string()), None))
        });

        methods.add_function("agent", |lua, ctx: mlua::UserDataRef<LuaCtx>| {
            try_pair!(ctx.validate_origin(lua));
            crate::api::agent::current(lua, ctx)
        });

        methods.add_method("_maki_managed", |lua, this, ()| {
            if this.validate_origin(lua).is_err() {
                return Ok(false);
            }
            let Some(expected) = this.agent().and_then(|agent| agent.managed_turn.as_ref()) else {
                return Ok(false);
            };
            Ok(crate::runtime::current_managed_turn(lua)
                .is_some_and(|active| expected.same_authority(&active)))
        });

        // The session that called this tool, which under concurrent
        // sessions is not always the focused one `maki.session.current()`
        // reports. Nil without an error when the run has no session, as in
        // the `maki index` one-shot.
        methods.add_method("session_id", |lua, this, ()| {
            let Some(session_id) = this.session_id() else {
                return Ok(this.cap_err_pair("session_id"));
            };
            try_pair!(this.validate_origin(lua));
            let Some(session_id) = session_id else {
                return Ok((None, None));
            };
            Ok((Some(session_id.id().to_string()), None))
        });

        methods.add_method("resolve_path", |lua, this, path: String| {
            let Some(agent) = this.agent() else {
                return Ok(this.cap_err_pair("resolve_path"));
            };
            try_pair!(this.validate_origin(lua));
            match agent.resolve_path(&path) {
                Ok(path) => Ok((Some(path), None)),
                Err(error) => Ok((None, Some(error))),
            }
        });

        methods.add_method("live_buf", |lua, this, buf: mlua::AnyUserData| {
            if matches!(this.caps, Caps::Restore { .. }) {
                return Ok(this.cap_err_pair("live_buf"));
            }
            try_pair!(this.validate_origin(lua));
            send_live_buf(lua, &buf)?;
            Ok((Some(true), None))
        });

        methods.add_method("config", |lua, this, args: MultiValue| {
            let Some(config) = this.config() else {
                return Ok(this.cap_err_pair("config"));
            };
            try_pair!(this.validate_origin(lua));
            let config_val = lua.to_value(config)?;
            if args.is_empty() {
                return Ok((Some(config_val), None));
            }
            let key: String = lua.from_value(args[0].clone())?;
            let default = args.get(1).cloned().unwrap_or(LuaValue::Nil);
            let val = match config_val {
                LuaValue::Table(ref tbl) => {
                    let val = tbl.raw_get::<LuaValue>(key.as_str())?;
                    if matches!(val, LuaValue::Nil) {
                        default
                    } else {
                        val
                    }
                }
                _ => default,
            };
            Ok((Some(val), None))
        });

        methods.add_method("tool_output_lines", |lua, this, ()| {
            lua.to_value(&this.tool_output_lines)
        });

        methods.add_method("state", |lua, this, ()| match this.state() {
            Some(v) => json_to_lua(lua, v),
            None => Ok(LuaValue::Nil),
        });

        methods.add_method("set_deadline", |lua, this, secs: u64| {
            if !matches!(this.caps, Caps::Handler { .. }) {
                return Ok(this.cap_err_pair("set_deadline"));
            }
            try_pair!(this.validate_origin(lua));
            let handle = active_task(lua);
            let cell = handle.lock().unwrap_or_else(|e| e.into_inner());
            if cell.deadline_secs.get().is_some() {
                return Err(mlua::Error::runtime(DEADLINE_ALREADY_SET_MSG));
            }
            cell.deadline_secs.set(Some(secs));
            let deadline = Instant::now() + Duration::from_secs(secs);
            cell.deadline.set(Some(
                cell.deadline
                    .get()
                    .map_or(deadline, |current| current.min(deadline)),
            ));
            cell.deadline_changed.notify(usize::MAX);
            Ok((Some(true), None))
        });

        methods.add_method("capture_read", |lua, this, path: String| {
            let Some(agent) = this.agent() else {
                return Ok(this.cap_err_pair("capture_read"));
            };
            try_pair!(this.validate_origin(lua));
            let tracker = Arc::clone(&agent.file_tracker);
            let mut snapshot = tracker.capture_read(Path::new(&path));
            let origin = this.origin.clone();
            let commit = lua.create_function_mut(move |lua, ()| {
                try_pair!(validate_task_origin(lua, origin.as_ref(), false));
                if let Some(snapshot) = snapshot.take() {
                    tracker.commit_read(snapshot);
                }
                Ok((Some(true), None))
            })?;
            Ok((Some(commit), None))
        });

        methods.add_method("record_read", |lua, this, path: String| {
            let Some(tracker) = this.file_tracker() else {
                return Ok(this.cap_err_pair("record_read"));
            };
            try_pair!(this.validate_origin(lua));
            tracker.record_read(Path::new(&path));
            Ok((Some(true), None))
        });

        // The matching check before a write is run by the dispatcher under the
        // file write lock for every tool declaring `mutable_path`. Kept as a
        // compatibility shim returning (Some(true), None).
        methods.add_method("check_before_edit", |lua, this, _path: String| {
            if this.agent().is_none() {
                return Ok(this.cap_err_pair("check_before_edit"));
            }
            try_pair!(this.validate_origin(lua));
            Ok((Some(true), None))
        });

        methods.add_async_method(
            "find_instructions",
            |lua, this, dir_path: String| async move {
                let Some(loaded) = this.loaded_instructions().cloned() else {
                    return Ok(this.cap_err_pair("find_instructions"));
                };
                let Some(cwd) = this.agent().map(|agent| agent.cwd.clone()) else {
                    return Ok(this.cap_err_pair("find_instructions"));
                };
                try_pair!(this.validate_origin(&lua));
                let origin = this.origin.clone();
                // Nothing may hold the ctx borrow across the wait: a cancel
                // hook firing meanwhile needs `ctx:finish`, which takes it
                // mutably.
                drop(this);
                let results = smol::unblock(move || {
                    let abs = resolve_abs_with_cwd(dir_path, &cwd);
                    maki_agent::find_subdirectory_instructions(&abs, &cwd, &loaded)
                })
                .await;
                try_pair!(validate_task_origin(&lua, origin.as_ref(), false));
                let tbl = lua.create_table()?;
                for (i, (path, content)) in results.into_iter().enumerate() {
                    let entry = lua.create_table()?;
                    entry.set("path", path)?;
                    entry.set("content", content)?;
                    tbl.set(i + 1, entry)?;
                }
                Ok((Some(tbl), None))
            },
        );

        methods.add_method("is_instruction_file", |_, _, name: String| {
            Ok(maki_agent::is_instruction_file(&name))
        });

        methods.add_method("question_mode", |lua, this, ()| {
            let Some(agent) = this.agent() else {
                return Ok(this.cap_err_pair("question_mode"));
            };
            try_pair!(this.validate_origin(lua));
            let mode = match agent.question_mode {
                QuestionMode::Tui => "tui",
                QuestionMode::Elicitation => "elicitation",
                QuestionMode::Headless => "headless",
            };
            Ok((Some(lua.create_string(mode)?), None))
        });

        // Asks the host for structured input (ACP `elicitation/create` form).
        // Only available when the run was spawned with elicitation support;
        // otherwise answers `(nil, err)` so the caller can fail gracefully.
        methods.add_async_method("ask", |lua, this, questions: mlua::Table| async move {
            let Some(agent) = this.agent().cloned() else {
                return Ok(this.cap_err_pair("ask"));
            };
            try_pair!(this.validate_origin(&lua));
            let origin = this.origin.clone();
            drop(this);
            if agent.question_mode != QuestionMode::Elicitation {
                return Ok((None, Some("no elicitation-capable host".to_string())));
            }
            let questions_value =
                match lua.from_value::<serde_json::Value>(LuaValue::Table(questions)) {
                    Ok(v) => v,
                    Err(_) => return Ok((None, Some("invalid questions".to_string()))),
                };
            if let Err(e) = agent.event_tx.send(AgentEvent::Question {
                id: agent.tool_use_id.clone().unwrap_or_default(),
                questions: questions_value,
            }) {
                return Ok((None, Some(e.to_string())));
            }
            let Some(rx) = &agent.user_response_rx else {
                return Ok((None, Some("no user response channel".to_string())));
            };
            let guard = rx.lock().await;
            try_pair!(validate_task_origin(&lua, origin.as_ref(), false));
            let answer = agent.cancel.race(guard.recv_async()).await;
            drop(guard);
            try_pair!(validate_task_origin(&lua, origin.as_ref(), false));
            match answer {
                Ok(Ok(raw)) => match serde_json::from_str::<serde_json::Value>(&raw) {
                    Ok(payload) => Ok((Some(lua.to_value(&payload)?), None)),
                    Err(e) => Ok((None, Some(format!("invalid question response: {e}")))),
                },
                Ok(Err(_)) => Ok((None, Some("question channel closed".to_string()))),
                Err(_) => Ok((None, Some("cancelled".to_string()))),
            }
        });

        methods.add_method_mut("finish", |lua, this, val: LuaValue| {
            let Some(agent) = this.agent() else {
                return Ok(this.cap_err_pair("finish"));
            };
            // Cancellation hooks may finish, but only in their original live scope.
            try_pair!(validate_task_origin(lua, this.origin.as_ref(), true));
            let reply = ToolCallReply::from_lua_value_with_config(lua, &val, &agent.config);
            let tx = this
                .finish_tx
                .take()
                .ok_or_else(|| mlua::Error::runtime("ctx:finish() already called"))?;

            if let Some(buf) = crate::api::ui::buf::buf_from_reply(&val) {
                lock_cell(&active_task(lua)).root_buf = Some(buf);
            }
            let _ = tx.send(reply);
            Ok((Some(true), None))
        });
    }
}

fn validate_task_origin(
    lua: &mlua::Lua,
    origin: Option<&crate::runtime::TaskHandle>,
    cleanup: bool,
) -> Result<(), String> {
    let origin = origin.ok_or("context is not bound to an invocation")?;
    let current = crate::runtime::active_task_id(lua);
    let cell = lock_cell(origin);
    if !cell.scope_alive || (!cleanup && !cell.is_live()) {
        return Err(INVOCATION_EXPIRED.into());
    }
    if !cleanup {
        cell.validate_plugin_effect()?;
    }
    if current != Some(cell.id) {
        return Err("context belongs to another invocation".into());
    }
    Ok(())
}

fn resolve_abs_with_cwd(path: String, cwd: &Path) -> PathBuf {
    if Path::new(&path).is_absolute() {
        path.into()
    } else {
        cwd.join(&path)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use std::thread;

    use maki_agent::AgentMode;
    use maki_agent::tools::Deadline;
    use maki_agent::tools::test_support::stub_ctx_with;
    use maki_agent::tools::{LocalTool, ToolAudience};

    use super::*;

    #[test]
    fn unmanaged_handler_origin_rejects_sibling_and_expired_scope() {
        let lua = mlua::Lua::new();
        let scope = crate::runtime::TaskScope::detached(&lua);
        let mut ctx = LuaCtx::handler(&populated_ctx());
        ctx.bind_origin(scope.handle());
        assert!(ctx.validate_origin(&lua).is_ok());
        let sibling = crate::runtime::TaskScope::detached(&lua);
        assert_eq!(
            ctx.validate_origin(&lua).unwrap_err(),
            "context belongs to another invocation"
        );
        drop(sibling);
        assert!(ctx.validate_origin(&lua).is_ok());
        drop(scope);
        assert_eq!(ctx.validate_origin(&lua).unwrap_err(), INVOCATION_EXPIRED);
    }

    #[test]
    fn host_root_context_rejects_service_replacement_retirement_and_scope_exit() {
        let lua = mlua::Lua::new();
        crate::orchestration::install_plugin_authority(&lua, std::sync::Arc::from("owner"));
        let scope = crate::runtime::TaskScope::detached(&lua);
        let template = populated_ctx();
        let services = Arc::new(HostRootServices::fixed(
            maki_agent::AgentId::generate(),
            &template.model,
            maki_providers::RequestOptions::default(),
            Arc::new(maki_config::ModelPolicy::default()),
        ));
        lua.set_app_data(HostRootServicesSlot {
            services: Some(services.clone()),
            template: Some(template.clone()),
        });
        let ctx = LuaCtx::trusted_host_root_context(&lua, Arc::from("owner"))
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(
            &ctx.host_root(&lua).unwrap().unwrap(),
            &services
        ));
        assert!(ctx.trusted_host_root(&lua).unwrap().template.is_some());
        assert!(ctx.agent().is_none());
        lua.set_app_data(HostRootServicesSlot::default());
        assert!(ctx.host_root(&lua).is_err());
        lua.set_app_data(HostRootServicesSlot {
            services: Some(services.clone()),
            template: None,
        });
        services.retire();
        assert!(ctx.host_root(&lua).is_err());
        drop(scope);
        assert!(ctx.host_root(&lua).is_err());
    }

    #[test]
    fn captured_host_model_validator_rejects_revoked_plugin_and_expired_task() {
        let lua = mlua::Lua::new();
        crate::orchestration::install_plugin_authority(&lua, std::sync::Arc::from("owner"));
        let scope = crate::runtime::TaskScope::detached(&lua);
        let template = populated_ctx();
        let services = Arc::new(HostRootServices::fixed(
            maki_agent::AgentId::generate(),
            &template.model,
            maki_providers::RequestOptions::default(),
            Arc::new(maki_config::ModelPolicy::default()),
        ));
        lua.set_app_data(HostRootServicesSlot {
            services: Some(services.clone()),
            template: Some(template),
        });
        let ctx = LuaCtx::trusted_host_root_context(&lua, Arc::from("owner"))
            .unwrap()
            .unwrap();
        let validator = ctx
            .host_model_validator(&lua, &services, &services.reference())
            .unwrap();
        let check = validator.clone();
        assert!(thread::spawn(move || check()).join().unwrap().is_ok());
        lock_cell(scope.handle())
            .deadline
            .set(Some(Instant::now() - Duration::from_secs(1)));
        assert!(validator().is_err());
        lock_cell(scope.handle()).deadline.set(None);
        assert!(validator().is_ok());
        crate::orchestration::revoke_plugin(&lua, "owner");
        assert!(validator().is_err());
        drop(scope);
        assert!(validator().is_err());
    }

    #[test]
    fn tool_host_model_validator_binds_live_plugin_authority() {
        let lua = mlua::Lua::new();
        let owner = crate::orchestration::install_plugin_authority(&lua, Arc::from("owner"));
        let scope = crate::runtime::TaskScope::detached(&lua);
        let mut template = populated_ctx();
        let id = maki_agent::AgentId::generate();
        template.execution_agent_id = Some(id);
        let services = Arc::new(HostRootServices::fixed(
            id,
            &template.model,
            maki_providers::RequestOptions::default(),
            Arc::new(maki_config::ModelPolicy::default()),
        ));
        lua.set_app_data(HostRootServicesSlot {
            services: Some(services.clone()),
            template: None,
        });
        crate::runtime::bind_plugin_owner(&lua, Arc::from("owner"), owner).unwrap();
        let mut ctx = LuaCtx::handler(&template);
        ctx.pin_host_root(&lua);
        ctx.bind_origin(scope.handle());
        let validator = ctx
            .host_model_validator(&lua, &services, &services.reference())
            .unwrap();
        assert!(validator().is_ok());
        crate::orchestration::revoke_plugin(&lua, "owner");
        assert!(validator().is_err());
    }

    #[test]
    fn tool_root_is_pinned_by_execution_id_without_template_and_rejects_replacement() {
        let lua = mlua::Lua::new();
        let scope = crate::runtime::TaskScope::detached(&lua);
        let mut template = populated_ctx();
        let id = maki_agent::AgentId::generate();
        template.execution_agent_id = Some(id);
        let services = Arc::new(HostRootServices::fixed(
            id,
            &template.model,
            maki_providers::RequestOptions::default(),
            Arc::new(maki_config::ModelPolicy::default()),
        ));
        lua.set_app_data(HostRootServicesSlot {
            services: Some(services.clone()),
            template: None,
        });
        let mut root = LuaCtx::handler(&template);
        root.pin_host_root(&lua);
        root.bind_origin(scope.handle());
        assert_eq!(root.subject, Some(services.reference().id()));
        assert!(Arc::ptr_eq(
            &root.host_root(&lua).unwrap().unwrap(),
            &services
        ));
        let mut child_template = template.clone();
        child_template.ledger = maki_agent::RunLedger::child(&template.ledger);
        child_template.execution_agent_id = Some(maki_agent::AgentId::generate());
        let mut child = LuaCtx::handler(&child_template);
        child.pin_host_root(&lua);
        child.bind_origin(scope.handle());
        assert!(child.host_root(&lua).unwrap().is_none());
        let replacement = Arc::new(HostRootServices::fixed(
            services.reference().id(),
            &template.model,
            maki_providers::RequestOptions::default(),
            Arc::new(maki_config::ModelPolicy::default()),
        ));
        lua.set_app_data(HostRootServicesSlot {
            services: Some(replacement),
            template: Some(template),
        });
        assert!(root.host_root(&lua).is_err());
        assert!(child.host_root(&lua).unwrap().is_none());
    }

    #[test]
    fn host_root_mint_rejects_stale_generation_and_delegated_tools() {
        let lua = mlua::Lua::new();
        crate::orchestration::install_plugin_authority(&lua, std::sync::Arc::from("owner"));
        let scope = crate::runtime::TaskScope::detached(&lua);
        let template = populated_ctx();
        lua.set_app_data(HostRootServicesSlot {
            services: Some(Arc::new(HostRootServices::fixed(
                maki_agent::AgentId::generate(),
                &template.model,
                maki_providers::RequestOptions::default(),
                Arc::new(maki_config::ModelPolicy::default()),
            ))),
            template: Some(template),
        });
        let plugin: Arc<str> = Arc::from("owner");
        assert!(
            LuaCtx::trusted_host_root_context(&lua, plugin.clone())
                .unwrap()
                .is_some()
        );
        let captured = lock_cell(scope.handle()).plugin_origin.clone();
        crate::orchestration::revoke_plugin(&lua, &plugin);
        assert!(LuaCtx::trusted_host_root_context(&lua, plugin.clone()).is_err());
        assert_eq!(lock_cell(scope.handle()).plugin_origin, captured);
        lock_cell(scope.handle()).plugin_origin = None;
        lock_cell(scope.handle()).delegated_tool = Some(Default::default());
        assert!(LuaCtx::trusted_host_root_context(&lua, plugin).is_err());
        assert!(lock_cell(scope.handle()).plugin_origin.is_none());
    }

    const FOREIGN_CONTEXT_MSG: &str = "context belongs to another invocation";
    const EXPIRED_CONTEXT_MSG: &str = INVOCATION_EXPIRED;
    const READ_PATH: &str = "/missing/ctx-lifetime-test";

    #[test]
    fn scoped_methods_and_read_commit_reject_foreign_and_expired_contexts() {
        let lua = mlua::Lua::new();
        let scope = crate::runtime::TaskScope::detached(&lua);
        let mut ctx = LuaCtx::handler(&populated_ctx());
        ctx.bind_origin(scope.handle());
        lua.globals().set("ctx", ctx).unwrap();
        lua.globals().set("read_path", READ_PATH).unwrap();
        lua.load("commit = assert(ctx:capture_read(read_path))")
            .exec()
            .unwrap();
        let methods = lua
            .load(
                r#"
                return function(expected)
                    for _, call in ipairs({
                        function() return ctx:workflow() end,
                        function() return ctx:audience() end,
                        function() return ctx:session_id() end,
                        function() return ctx:config() end,
                        function() return ctx:resolve_path(read_path) end,
                        function() return ctx:set_deadline(1) end,
                        function() return ctx:capture_read(read_path) end,
                        function() return ctx:record_read(read_path) end,
                        function() return ctx:check_before_edit(read_path) end,
                        function() return ctx:question_mode() end,
                        commit,
                    }) do
                        local value, err = call()
                        assert(value == nil and err == expected, tostring(err))
                    end
                end
                "#,
            )
            .eval::<mlua::Function>()
            .unwrap();
        let sibling = crate::runtime::TaskScope::detached(&lua);
        methods.call::<()>(FOREIGN_CONTEXT_MSG).unwrap();
        assert!(lock_cell(sibling.handle()).deadline_secs.get().is_none());
        drop(sibling);
        lua.load("assert(commit())").exec().unwrap();
        drop(scope);
        methods.call::<()>(EXPIRED_CONTEXT_MSG).unwrap();
    }

    #[test]
    fn finish_allows_deadline_cleanup_but_rejects_foreign_and_closed_scopes() {
        let lua = mlua::Lua::new();
        let scope = crate::runtime::TaskScope::detached(&lua);
        let mut ctx = LuaCtx::handler(&populated_ctx());
        ctx.bind_origin(scope.handle());
        let (tx, rx) = flume::bounded(1);
        ctx.finish_tx = Some(tx);
        lua.globals().set("ctx", ctx).unwrap();
        let sibling = crate::runtime::TaskScope::detached(&lua);
        let (_, err): Pair<bool> = lua.load("return ctx:finish('done')").eval().unwrap();
        assert_eq!(err.as_deref(), Some(FOREIGN_CONTEXT_MSG));
        assert!(rx.is_empty());
        drop(sibling);
        let (trigger, cancel) = CancelToken::new();
        lock_cell(scope.handle()).cancel = cancel;
        trigger.cancel();
        lock_cell(scope.handle())
            .deadline
            .set(Some(Instant::now() - Duration::from_secs(1)));
        let (value, err): Pair<bool> = lua.load("return ctx:finish('done')").eval().unwrap();
        assert_eq!((value, err), (Some(true), None));
        assert!(rx.try_recv().is_ok());
        drop(scope);
        let (_, err): Pair<bool> = lua.load("return ctx:finish('done')").eval().unwrap();
        assert_eq!(err.as_deref(), Some(EXPIRED_CONTEXT_MSG));
    }

    #[test]
    fn restore_presentation_methods_remain_scope_independent() {
        let lua = mlua::Lua::new();
        lua.globals()
            .set(
                "ctx",
                LuaCtx::restore(
                    ToolOutputLines::default(),
                    Some(serde_json::json!({"saved": true})),
                ),
            )
            .unwrap();
        lua.load(
            "assert(ctx:state().saved); assert(ctx:tool_output_lines()); assert(not ctx:cancelled()); assert(ctx:is_instruction_file('AGENTS.md'))",
        )
        .exec()
        .unwrap();
    }

    const TOOL_USE_ID: &str = "tu-1";
    const INSTRUCTION_PATH: &str = "/tmp/nested/AGENTS.md";
    const LOCAL_TOOL_NAME: &str = "sess_tool";
    /// Arbitrary ids are rejected: `SessionRef` parses base58 or a uuid.
    const SESSION_ID: &str = "01965087-4c71-7f00-8000-000000000000";

    fn session_ref() -> SessionRef {
        SESSION_ID.parse().expect("valid session id")
    }

    fn populated_ctx() -> ToolContext {
        let mut ctx = stub_ctx_with(&AgentMode::Build, None, Some(TOOL_USE_ID));
        ctx.session_id = Some(session_ref());
        ctx.deadline = Deadline::after(Duration::from_secs(60));
        ctx.tool_output_lines = ToolOutputLines {
            bash: 999,
            ..ToolOutputLines::default()
        };
        assert!(
            !ctx.loaded_instructions
                .contains_or_insert(PathBuf::from(INSTRUCTION_PATH))
        );
        let mut tools: HashMap<String, LocalTool> = HashMap::new();
        tools.insert(
            LOCAL_TOOL_NAME.into(),
            maki_agent::tools::local_tool(ToolAudience::MODEL, |_, _| {
                Box::pin(async { Ok(String::new()) })
            }),
        );
        ctx.local_tools = Arc::new(tools);
        ctx.live_sink = Some(flume::unbounded().0);
        ctx
    }

    #[test]
    fn agent_context_keeps_tool_use_id_and_resets_per_call_state() {
        let agent = AgentContext::from(&populated_ctx());
        assert_eq!(agent.tool_use_id.as_deref(), Some(TOOL_USE_ID));
        assert_eq!(
            agent.session_id,
            Some(session_ref()),
            "the session owns the whole run, so it is not per-call state"
        );
        assert!(
            matches!(agent.deadline, Deadline::At(_)),
            "the parent deadline must be inherited, not discarded"
        );
        assert_eq!(agent.tool_output_lines, ToolOutputLines::default());
        assert!(
            agent.local_tools.contains_key(LOCAL_TOOL_NAME),
            "a nested call must route names exactly like the model's own call"
        );
        assert!(
            !agent
                .loaded_instructions
                .contains_or_insert(PathBuf::from(INSTRUCTION_PATH)),
            "loaded_instructions must be a fresh set, not a shared clone"
        );
    }

    #[test]
    fn agent_context_to_tool_context_drops_tool_use_id_and_sink() {
        let agent = AgentContext::from(&populated_ctx());
        assert!(
            agent.live_sink.is_some(),
            "the sink set by the caller must survive into AgentContext"
        );
        let inner = agent.to_tool_context();
        assert_eq!(inner.tool_use_id, None);
        assert!(inner.live_sink.is_none(), "sink must not be inherited");
        assert_eq!(agent.tool_use_id.as_deref(), Some(TOOL_USE_ID));
        assert_eq!(
            inner.session_id,
            Some(session_ref()),
            "a dispatched child runs in the same session, unlike tool_use_id"
        );
    }

    #[test]
    fn session_id_reaches_handler_and_start_but_not_restore() {
        let ctx = populated_ctx();
        assert_eq!(
            LuaCtx::handler(&ctx).session_id(),
            Some(Some(&session_ref()))
        );
        assert_eq!(LuaCtx::start(&ctx).session_id(), Some(Some(&session_ref())));
        assert_eq!(
            LuaCtx::restore(ToolOutputLines::default(), None).session_id(),
            None,
            "restore has no ToolContext to take a session from"
        );
    }

    #[test]
    fn session_id_absent_is_distinct_from_kind_lacking_it() {
        let mut ctx = populated_ctx();
        ctx.session_id = None;
        assert_eq!(
            LuaCtx::handler(&ctx).session_id(),
            Some(None),
            "a sessionless run still has the capability, so lua sees nil without an error"
        );
    }

    #[test]
    fn handler_ctx_keeps_parent_instruction_set() {
        let ctx = LuaCtx::handler(&populated_ctx());
        assert!(
            ctx.loaded_instructions()
                .expect("handler has instructions")
                .contains_or_insert(PathBuf::from(INSTRUCTION_PATH)),
            "handler must share the parent's set; AgentContext resets its own copy"
        );
    }
}
