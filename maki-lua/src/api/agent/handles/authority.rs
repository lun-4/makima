use maki_agent::{AgentId, AgentManagerHandle, CurrentManagedTurn};
use mlua::Lua;

use super::{AgentOwners, LuaAgent, session_state};
use crate::agent_errors::{MANAGED_AUTHORITY_INACTIVE, NOT_DESCENDANT_OF_CALLER, OTHER_RUNTIME};
use crate::api::util::ctx::{AgentContext, LuaCtx};
use crate::orchestration;
use crate::runtime::current_managed_turn;

pub(super) fn retire_closed(lua: &Lua) {
    if let Some(sessions) = lua.app_data_ref::<AgentOwners>() {
        sessions.retire_settled();
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
    if !manager.same_manager(&agent.manager) {
        return Err(OTHER_RUNTIME.into());
    }
    scope(lua, ctx, agent.id)
}

pub(super) fn observe(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if let Ok(trusted) = ctx.trusted(lua) {
        let root = agent
            .root
            .as_ref()
            .ok_or("managed root identity is unavailable")?;
        if !root.manager().same_manager(&agent.manager) {
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
    if !manager.same_manager(&agent.manager) {
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
        if let Some(config) = agent
            .manager
            .actor(agent.id)
            .map_err(|error| error.to_string())?
            .effective_config()
        {
            template.apply_effective_config(&config);
        }
        return Ok(AgentContext::from(&template));
    }
    let target = agent
        .manager
        .lookup(agent.id)
        .map_err(|error| error.to_string())?;
    let service = orchestration::services(lua).ok_or("destination template is unavailable")?;
    Ok(AgentContext::from(&service.template(&target)?))
}

pub(crate) fn lookup(
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
        manager,
        id,
        state: session_state(lua, id),
        held: Some(held),
        root: Some(root),
        ancestors,
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;

    use futures_lite::future::race;
    use maki_agent::actor::{
        ActorLifecycle, AdmissionPreparation, AgentContextSnapshot, EffectiveAgentConfig,
    };
    use maki_agent::agent::RunSettings;
    use maki_agent::tools::test_support::stub_ctx;
    use maki_agent::{
        ActorBackend, ActorEvent, AgentInput, AgentLimits, AgentManagerHandle, AgentMode,
        BackendResult, ControlWork, History, TurnContext, WorkKind,
    };
    use maki_providers::TokenUsage;
    use maki_providers::provider::Provider;
    use smol::Timer;
    use std::sync::Arc;

    use crate::api::agent::LuaActorBackend;
    use crate::api::agent::handles::tests::{REPLY, RETIRE_TIMEOUT};
    use crate::api::agent::tests::{
        StreamOnceProvider, canned_reply_with_usage, session_with_provider,
        session_with_provider_for_agent,
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

        fn admission_preparation(&self) -> Option<AdmissionPreparation> {
            self.backend.admission_preparation()
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
            let provider: Arc<dyn Provider> = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, usage),
            ]));
            let (original, _, _session, _) = session_with_provider(Arc::clone(&provider), None);
            original.close();
            let (entered_tx, entered_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let mut context = stub_ctx(&AgentMode::Build);
            context.provider = Arc::clone(&provider);
            let config = EffectiveAgentConfig::new(
                RunSettings {
                    model: (*context.model).clone(),
                    provider: Arc::clone(&context.provider),
                    thinking: context.opts.thinking,
                    fast: context.opts.fast,
                    workflow: false,
                },
                context.mode.clone(),
            )
            .with_mode_def(Some(context.modes.current(&context.mode)))
            .with_context(AgentContextSnapshot::new(context.cwd.clone()));
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let mut root_fixture = None;
            let target = manager
                .create_root_with_config(Some(config), Vec::new(), None, |agent_id| {
                    let (_, root_state, fixture, _) =
                        session_with_provider_for_agent(Arc::clone(&context.provider), agent_id);
                    root_fixture = Some(fixture);
                    Ok::<_, String>(Box::new(SettlementGate {
                        backend: LuaActorBackend::new(root_state),
                        entered: entered_tx,
                        release: release_rx,
                    }))
                })
                .unwrap();
            let _root_fixture = root_fixture.unwrap();
            let actor = target.actor().unwrap();
            let id = target.id();
            let events = actor.subscribe_events();
            let ticket = actor
                .admit_turn(
                    AgentInput {
                        message: REPLY.into(),
                        images: Vec::new(),
                        preamble: Vec::new(),
                        prompt: None,
                    },
                    None,
                    String::new(),
                )
                .unwrap();
            race(
                async {
                    entered_rx.recv_async().await.unwrap();
                    target.close_subtree().unwrap();
                    assert_ne!(actor.snapshot().lifecycle, ActorLifecycle::Open);
                    assert!(
                        actor.snapshot().active_turn.is_some(),
                        "a close during a running turn must not terminalize it"
                    );
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
                                assert!(ended, "the billed end must land before the close");
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
            let settled = actor.snapshot();
            assert!(settled.active_turn.is_none());
            assert_eq!(settled.cumulative_usage, usage);
            assert_eq!(settled.lifecycle, ActorLifecycle::Closed);
            // `manager.node(id)` is not durable: the manager reaps a closed,
            // drained node from its graph. The reference retains the terminal
            // snapshot, so it still reports the same agent after reaping.
            assert_eq!(target.snapshot().unwrap().agent_id, id);
            assert!(
                manager.lookup(id).is_err(),
                "a closed agent is no longer a live reference"
            );
            manager.shutdown(RETIRE_TIMEOUT).await;
        });
    }
}
