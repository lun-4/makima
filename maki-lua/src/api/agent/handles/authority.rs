use std::sync::Arc;

use maki_agent::{AgentId, AgentManagerHandle, CurrentManagedTurn};
use mlua::Lua;

use super::{AgentRuntime, AgentSessions, LuaAgent, session_state};
use crate::agent_errors::{
    MANAGED_AUTHORITY_INACTIVE, NO_TASK_AUTHORITY, NOT_DESCENDANT_OF_CALLER,
    OTHER_PLUGIN_GENERATION, OTHER_RUNTIME, UNMANAGED_AUTHORITY_INACTIVE,
};
use crate::api::util::ctx::{AgentContext, LuaCtx};
use crate::host_root::{HostRootServices, UNSUPPORTED_OPERATION_ERROR};
use crate::orchestration::{self, holds_plugin_grant};
use crate::runtime::{TaskHandle, current_managed_turn, lock_cell};

pub(super) fn retire_closed(lua: &Lua) {
    if let Some(sessions) = lua.app_data_ref::<AgentSessions>() {
        sessions.retire_settled();
    }
}

pub(super) fn task_authority(lua: &Lua) -> Result<TaskHandle, &'static str> {
    lua.app_data_ref::<TaskHandle>()
        .map(|task| Arc::clone(&task))
        .ok_or(NO_TASK_AUTHORITY)
}

pub(super) fn validate_unmanaged(
    lua: &Lua,
    ctx: &LuaCtx,
    runtime: &AgentRuntime,
) -> Result<(), String> {
    ctx.validate_origin(lua)?;
    ctx.host_root(lua)?;
    let AgentRuntime::Unmanaged {
        parent,
        plugin,
        generation,
        owner,
        ..
    } = runtime
    else {
        return Err("not an unmanaged agent".into());
    };
    let template = ctx
        .trusted_host_root(lua)
        .ok()
        .and_then(|host| host.template.as_ref())
        .map(AgentContext::from);
    let context = ctx
        .agent()
        .or(template.as_ref())
        .ok_or_else(|| ctx.cap_err("orchestration"))?;
    let active = task_authority(lua)?;
    if !holds_plugin_grant(lua, plugin, *generation, owner) {
        return Err(OTHER_PLUGIN_GENERATION.into());
    }
    let cell = lock_cell(&active);
    if !cell.is_live()
        || context.cancel.is_cancelled()
        || context.managed_turn.is_some()
        || cell.managed_turn.is_some()
        || !Arc::ptr_eq(parent, &context.subagent_cancels)
    {
        return Err(UNMANAGED_AUTHORITY_INACTIVE.into());
    }
    Ok(())
}

pub(super) fn host_agent(services: Arc<HostRootServices>) -> LuaAgent {
    LuaAgent::without_graph(AgentRuntime::host(services), None, None)
}

pub(super) fn validate_host_root(lua: &Lua, ctx: &LuaCtx, agent: &LuaAgent) -> Result<(), String> {
    let AgentRuntime::HostRoot {
        services,
        reference,
    } = &agent.manager
    else {
        return Err(UNSUPPORTED_OPERATION_ERROR.into());
    };
    let current = ctx
        .host_root(lua)?
        .ok_or("host root authority is unavailable")?;
    if !Arc::ptr_eq(services, &current) {
        return Err(OTHER_RUNTIME.into());
    }
    services
        .validate(reference)
        .map_err(|error| error.to_string())
}

/// Validates handles that live outside a managed graph; `None` for managed handles.
fn validate_outside_graph(lua: &Lua, ctx: &LuaCtx, agent: &LuaAgent) -> Option<Result<(), String>> {
    match agent.manager {
        AgentRuntime::HostRoot { .. } => Some(validate_host_root(lua, ctx, agent)),
        AgentRuntime::Unmanaged { .. } => Some(validate_unmanaged(lua, ctx, &agent.manager)),
        AgentRuntime::Managed(_) => None,
    }
}

pub(super) fn context_graph(
    lua: &Lua,
    ctx: &LuaCtx,
) -> Result<(AgentManagerHandle, AgentId), String> {
    ctx.validate_origin(lua)?;
    if let Ok(trusted) = ctx.trusted(lua) {
        return Ok((trusted.target.manager(), trusted.target.id()));
    }
    let current = ctx
        .agent()
        .and_then(|agent| agent.managed_turn.as_ref())
        .ok_or_else(|| "current/root/list require a managed graph".to_owned())?;
    let active = current_managed_turn(lua).ok_or(MANAGED_AUTHORITY_INACTIVE)?;
    if !current.same_authority(&active) {
        return Err(MANAGED_AUTHORITY_INACTIVE.into());
    }
    current
        .validate_active()
        .map_err(|error| error.to_string())?;
    Ok((current.manager(), current.agent_id()))
}

pub(super) fn scope(
    lua: &Lua,
    ctx: &LuaCtx,
    target: AgentId,
) -> Result<Option<CurrentManagedTurn>, String> {
    let (manager, _) = context_graph(lua, ctx)?;
    manager.node(target).map_err(|error| error.to_string())?;
    let expected = ctx.agent().and_then(|agent| agent.managed_turn.as_ref());
    let active = current_managed_turn(lua);
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
        _ => Err(MANAGED_AUTHORITY_INACTIVE.into()),
    }
}

pub(super) fn authorize(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if let Some(result) = validate_outside_graph(lua, ctx, agent) {
        return result.map(|()| None);
    }
    if let Ok(trusted) = ctx.trusted(lua) {
        let target = agent.manager.lookup(agent.id)?;
        trusted.validate_target(lua, &target)?;
        return Ok(None);
    }
    retire_closed(lua);
    let (manager, _) = context_graph(lua, ctx)?;
    if !manager.same_manager(agent.manager.managed()?) {
        return Err(OTHER_RUNTIME.into());
    }
    scope(lua, ctx, agent.id.ok_or(UNSUPPORTED_OPERATION_ERROR)?)
}

pub(super) fn observe(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if let Some(result) = validate_outside_graph(lua, ctx, agent) {
        return result.map(|()| None);
    }
    if let Ok(trusted) = ctx.trusted(lua) {
        let root = agent
            .root
            .as_ref()
            .ok_or("managed root identity is unavailable")?;
        if !root.manager().same_manager(agent.manager.managed()?) {
            return Err(OTHER_RUNTIME.into());
        }
        if !trusted.target.manager().same_manager(&root.manager()) {
            return Err(OTHER_RUNTIME.into());
        }
        let subject = trusted
            .target
            .snapshot()
            .map_err(|error| error.to_string())?;
        if subject.root_id != root.id() {
            return Err("agent belongs to another graph".into());
        }
        return Ok(None);
    }
    retire_closed(lua);
    let (manager, _) = context_graph(lua, ctx)?;
    if !manager.same_manager(agent.manager.managed()?) {
        return Err(OTHER_RUNTIME.into());
    }
    let current = current_managed_turn(lua).ok_or("managed authority expired")?;
    if !agent.ancestors.contains(&current.agent_id()) {
        return Err(NOT_DESCENDANT_OF_CALLER.into());
    }
    Ok(Some(current))
}

pub(super) fn destination_template(lua: &Lua, agent: &LuaAgent) -> Result<AgentContext, String> {
    if let Some(template) = agent.state.as_ref().and_then(|state| state.template.get()) {
        let mut template = template.clone();
        if let Some(config) = agent.manager.actor(agent.id)?.effective_config() {
            template.apply_effective_config(&config);
        }
        return Ok(AgentContext::from(&template));
    }
    let target = agent.manager.lookup(agent.id)?;
    let service = orchestration::services(lua).ok_or("destination template is unavailable")?;
    Ok(AgentContext::from(&service.template(&target)?))
}

pub(super) fn lookup(
    manager: AgentManagerHandle,
    id: AgentId,
    lua: &Lua,
) -> Result<LuaAgent, String> {
    let held = manager.lookup(id).map_err(|error| error.to_string())?;
    let node = held.snapshot().map_err(|error| error.to_string())?;
    let root = manager
        .lookup(node.root_id)
        .map_err(|error| error.to_string())?;
    let mut ancestors = vec![id];
    let mut parent = node.parent_id;
    while let Some(id) = parent {
        ancestors.push(id);
        parent = manager
            .node(id)
            .map_err(|error| error.to_string())?
            .parent_id;
    }
    Ok(LuaAgent {
        manager: AgentRuntime::Managed(manager),
        id: Some(id),
        state: session_state(lua, id),
        held: Some(held),
        root: Some(root),
        ancestors,
    })
}

impl LuaCtx {
    pub(crate) fn pin_subject(&mut self, lua: &Lua) {
        if self.subject.is_some() {
            return;
        }
        self.subject = self.agent().and_then(|context| context.execution_agent_id);
        if self.subject.is_some() {
            return;
        }
        self.subject = self.agent().and_then(|context| {
            let sessions = lua.app_data_ref::<AgentSessions>()?;
            sessions
                .lock()
                .values()
                .find(|session| {
                    session
                        .state
                        .params
                        .get()
                        .is_some_and(|params| Arc::ptr_eq(&params.ledger, &context.ledger))
                })
                .map(|session| session.agent_id)
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use futures_lite::future::race;
    use maki_agent::{
        ActorBackend, ActorEvent, AgentActorHandle, AgentInput, AgentMode, BackendResult,
        ControlWork, History, TurnContext, WorkKind,
    };
    use maki_providers::TokenUsage;
    use smol::Timer;

    use super::*;
    use crate::api::agent::LuaActorBackend;
    use crate::api::agent::handles::tests::{REPLY, RETIRE_TIMEOUT};
    use crate::api::agent::tests::{
        StreamOnceProvider, canned_reply_with_usage, session_with_provider,
    };

    struct SettlementGate {
        backend: LuaActorBackend,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl ActorBackend for SettlementGate {
        fn run_turn<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            input: AgentInput,
            work: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async move {
                let result = self.backend.run_turn(history, context, input, work).await;
                self.entered.send(()).unwrap();
                self.release.recv_async().await.unwrap();
                result
            })
        }

        fn run_control<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            control: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            self.backend.run_control(history, context, control)
        }

        fn run_compact<'a>(
            &'a mut self,
            history: &'a mut History,
            context: TurnContext,
            instructions: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            self.backend.run_compact(history, context, instructions)
        }
    }

    #[test]
    fn active_close_keeps_session_until_billed_end_before_close() {
        smol::block_on(async {
            let usage = TokenUsage {
                input: 13,
                output: 7,
                ..Default::default()
            };
            let provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, usage),
            ]));
            let (original, state, mut session, _) = session_with_provider(provider, None, None);
            original.close();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let (actor, runner) = AgentActorHandle::spawn(
                session.agent_id,
                Vec::new(),
                None,
                Box::new(SettlementGate {
                    backend: LuaActorBackend::new(Arc::clone(&state)),
                    entered: entered_tx,
                    release: release_rx,
                }),
            );
            runner.detach();
            session.actor = Arc::new(actor.clone());
            let lua = Lua::new();
            let id = session.agent_id;
            let sessions = AgentSessions(Arc::new(Mutex::new(HashMap::from([(id, session)]))));
            lua.set_app_data(sessions.clone());
            let events = actor.subscribe_events();
            let ticket = actor
                .admit_turn(
                    AgentInput {
                        message: REPLY.into(),
                        mode: AgentMode::Build,
                        images: Vec::new(),
                        preamble: Vec::new(),
                        thinking: Default::default(),
                        fast: false,
                        workflow: false,
                        prompt: None,
                        cancel: None,
                        lease_committer: None,
                    },
                    None,
                    String::new(),
                )
                .unwrap();
            race(
                async {
                    entered_rx.recv_async().await.unwrap();
                    actor.close();
                    assert!(actor.snapshot().active_turn.is_some());
                    retire_closed(&lua);
                    assert!(sessions.lock().contains_key(&id));
                    assert_eq!(actor.snapshot().cumulative_usage, TokenUsage::default());
                    release_tx.send(()).unwrap();
                    let mut ended = false;
                    loop {
                        match events.recv_async().await.unwrap() {
                            ActorEvent::End(result) => {
                                assert_eq!(result.outcome.usage(), usage);
                                assert_eq!(actor.snapshot().cumulative_usage, usage);
                                ended = true;
                            }
                            ActorEvent::Close { .. } => {
                                assert!(ended);
                                break;
                            }
                            _ => {}
                        }
                    }
                },
                async {
                    Timer::after(RETIRE_TIMEOUT).await;
                    panic!("active close did not settle");
                },
            )
            .await;
            ticket.wait().await;
            assert!(actor.snapshot().active_turn.is_none());
            assert_eq!(actor.snapshot().cumulative_usage, usage);
            retire_closed(&lua);
            assert!(!sessions.lock().contains_key(&id));
        });
    }
}
