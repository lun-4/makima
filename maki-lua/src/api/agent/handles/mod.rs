mod agent_methods;
mod authority;
mod module_fns;
mod projection;
mod subscriptions;
mod tickets;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use maki_agent::actor::ActorLifecycle;
use maki_agent::cancel::CancelMap;
use maki_agent::{AgentActorHandle, AgentId, AgentManagerHandle, AgentRef, TurnTicket};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::Lua;

use crate::agent_errors::OTHER_RUNTIME;
use crate::api::agent::{LuaActorState, LuaSession, SessionOwners};
use crate::api::util::pair::Pair;
use crate::host_root::{HostRootRef, HostRootServices, UNSUPPORTED_OPERATION_ERROR};
use crate::orchestration::{PluginAuthority, plugin_authority};

pub(crate) use agent_methods::{AGENT_DOCS, agent_defer_close};
pub(crate) use module_fns::{current, focused_agent_ref};
pub(super) use module_fns::{
    current__doc, current__register, get__doc, get__register, list__doc, list__register, root__doc,
    root__register, spawn__doc, spawn__register,
};
pub(crate) use subscriptions::SUBSCRIPTION_DOCS;
pub(crate) use tickets::TURN_DOCS;

#[derive(Clone)]
enum AgentRuntime {
    HostRoot {
        services: Arc<HostRootServices>,
        reference: HostRootRef,
    },
    Managed(AgentManagerHandle),
    Unmanaged {
        actor: Arc<AgentActorHandle>,
        state: Arc<LuaActorState>,
        parent: Arc<CancelMap<String>>,
        plugin: Arc<str>,
        generation: u64,
        owner: Arc<PluginAuthority>,
    },
}

impl AgentRuntime {
    fn host(services: Arc<HostRootServices>) -> Self {
        Self::HostRoot {
            reference: services.reference(),
            services,
        }
    }

    fn unmanaged(
        lua: &Lua,
        actor: Arc<AgentActorHandle>,
        state: Arc<LuaActorState>,
        parent: Arc<CancelMap<String>>,
        plugin: Arc<str>,
        generation: u64,
    ) -> Self {
        Self::Unmanaged {
            actor,
            state,
            parent,
            owner: plugin_authority(lua, plugin.clone()),
            plugin,
            generation,
        }
    }

    fn identity(&self, id: Option<AgentId>) -> String {
        match self {
            Self::HostRoot { reference, .. } => reference.id().to_string(),
            _ => id.map(|id| id.to_string()).unwrap_or_default(),
        }
    }

    fn managed(&self) -> Result<&AgentManagerHandle, String> {
        match self {
            Self::Managed(manager) => Ok(manager),
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Unmanaged { .. } => Err("operation requires a managed graph".into()),
        }
    }

    fn actor(&self, id: Option<AgentId>) -> Result<AgentActorHandle, String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::Managed(manager) => manager.actor(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } if actor.agent_id() == id => Ok(actor.as_ref().clone()),
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Unmanaged { .. } => Err(OTHER_RUNTIME.into()),
        }
    }

    fn lookup(&self, id: Option<AgentId>) -> Result<AgentRef, String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        self.managed()?
            .lookup(id)
            .map_err(|error| error.to_string())
    }

    fn close_subtree(&self, id: Option<AgentId>) -> Result<(), String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Managed(manager) => manager.close_subtree(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, state, .. } => {
                state.close_with(actor);
                Ok(())
            }
        }
    }

    fn cancel_agent(&self, id: Option<AgentId>) -> Result<(), String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Managed(manager) => manager.cancel_agent(id).map_err(|error| error.to_string()),
            Self::Unmanaged { actor, .. } => {
                actor.cancel_existing();
                Ok(())
            }
        }
    }

    fn cancel_subtree(&self, id: Option<AgentId>) -> Result<(), String> {
        let id = id.ok_or(UNSUPPORTED_OPERATION_ERROR)?;
        match self {
            Self::Managed(manager) => manager
                .cancel_subtree(id)
                .map_err(|error| error.to_string()),
            Self::HostRoot { .. } => Err(UNSUPPORTED_OPERATION_ERROR.into()),
            Self::Unmanaged { actor, state, .. } => {
                state.cancel_subtree(actor);
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct LuaAgent {
    manager: AgentRuntime,
    id: Option<AgentId>,
    state: Option<Arc<LuaActorState>>,
    held: Option<AgentRef>,
    root: Option<AgentRef>,
    ancestors: Vec<AgentId>,
}

impl LuaAgent {
    /// The id the UI keys this agent's transcript by; `None` for host roots.
    pub(crate) fn transcript_id(&self) -> Option<&str> {
        self.state.as_ref().map(|state| state.ui_id.as_str())
    }

    /// A handle without managed graph identity: host roots and unmanaged children.
    fn without_graph(
        manager: AgentRuntime,
        id: Option<AgentId>,
        state: Option<Arc<LuaActorState>>,
    ) -> Self {
        Self {
            manager,
            id,
            state,
            held: None,
            root: None,
            ancestors: Vec::new(),
        }
    }

    fn identity(&self) -> String {
        self.manager.identity(self.id)
    }

    fn reference(&self) -> LuaAgentRef {
        LuaAgentRef {
            manager: self.manager.clone(),
            id: self.id,
            held: self.held.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct LuaAgentRef {
    manager: AgentRuntime,
    id: Option<AgentId>,
    #[expect(dead_code, reason = "keeps the node's terminal snapshot slot alive")]
    held: Option<AgentRef>,
}

#[derive(Clone)]
struct LuaAgentTurn {
    agent: LuaAgent,
    ticket: TurnTicket,
}

#[derive(Clone, Default)]
struct AgentSessions(Arc<Mutex<HashMap<AgentId, LuaSession>>>);

impl AgentSessions {
    fn lock(&self) -> MutexGuard<'_, HashMap<AgentId, LuaSession>> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Sessions are dropped after the lock is released.
    fn retire(&self, id: AgentId) {
        let retired = self.lock().remove(&id);
        drop(retired);
    }

    fn retire_settled(&self) {
        let retired: Vec<_> = self
            .lock()
            .extract_if(|_, session| session_settled_closed(session))
            .collect();
        drop(retired);
    }
}

#[derive(Default)]
struct AgentOwners(HashMap<AgentId, Arc<str>>);

pub(super) fn close_plugin_agents(lua: &Lua, plugin: &str) {
    let ids: Vec<_> = {
        let Some(mut owners) = lua.app_data_mut::<AgentOwners>() else {
            return;
        };
        owners
            .0
            .extract_if(|_, owner| owner.as_ref() == plugin)
            .map(|(id, _)| id)
            .collect()
    };
    let retired = lua.app_data_ref::<AgentSessions>().map(|sessions| {
        let mut sessions = sessions.lock();
        ids.into_iter()
            .filter_map(|id| sessions.remove(&id))
            .collect::<Vec<_>>()
    });
    drop(retired);
}

fn session_settled_closed(session: &LuaSession) -> bool {
    let snapshot = session.actor.snapshot();
    snapshot.lifecycle != ActorLifecycle::Open && snapshot.active_turn.is_none()
}

pub(super) fn session_state(lua: &Lua, id: AgentId) -> Option<Arc<LuaActorState>> {
    lua.app_data_ref::<AgentSessions>()
        .and_then(|sessions| {
            sessions
                .lock()
                .get(&id)
                .map(|session| Arc::clone(&session.state))
        })
        .or_else(|| {
            lua.app_data_ref::<SessionOwners>().and_then(|owners| {
                owners
                    .0
                    .iter()
                    .find(|owner| owner.agent.as_ref().is_some_and(|agent| agent.id() == id))
                    .and_then(|owner| owner.state.upgrade())
            })
        })
}

/// Read a reference's stable identity.
/// @return (string?, string?) Agent ID.
#[lua_fn(name = "id")]
fn reference_id(_lua: &Lua, this: &LuaAgentRef) -> mlua::Result<Pair<String>> {
    Ok((Some(this.manager.identity(this.id)), None))
}

lua_class! {
    /// Visibility-only identity. Resolve with get(ctx, reference) to request control.
    "maki.agent.AgentRef" => LuaAgentRef, REF_DOCS [reference_id]
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_lite::future::race;
    use maki_agent::tools::test_support::{stub_ctx, stub_ctx_with};
    use maki_agent::tools::{DescriptionContext, RequestTools, ToolContext, TurnToolBindings};
    use maki_agent::{
        AgentEvent, AgentLimits, AgentMode, EffectiveAgentConfig, EventSender, Post, PostLevel,
        RunSettings,
    };
    use maki_config::ModelPolicy;
    use maki_providers::{RequestOptions, TokenUsage};
    use mlua::{AnyUserData, Table};
    use smol::Timer;

    use super::authority::validate_unmanaged;
    use super::*;
    use crate::agent_errors::OTHER_PLUGIN_GENERATION;
    use crate::api::agent::structured_output::{StructuredOutput, TOOL_NAME};
    use crate::api::agent::tests::{
        StreamOnceProvider, canned_reply_with_usage, session_with_provider,
        session_with_provider_for_agent,
    };
    use crate::api::agent::{LuaActorBackend, create_agent_table};
    use crate::api::util::ctx::LuaCtx;
    use crate::orchestration::{
        HostRootServicesSlot, OrchestrationServices, OrchestrationServicesSlot, TrustedTarget,
        revoke_plugin,
    };
    use crate::runtime::{TaskScope, lock_cell};

    pub(super) const REPLY: &str = "retained reply";
    pub(super) const RETIRE_TIMEOUT: Duration = Duration::from_secs(5);
    const PLUGIN: &str = "agent-test";
    const FOREIGN_PLUGIN: &str = "foreign-agent-test";

    fn plugin_scope(lua: &Lua, plugin: &str) -> TaskScope {
        let scope = TaskScope::detached(lua);
        let plugin: Arc<str> = Arc::from(plugin);
        let current = plugin_authority(lua, plugin.clone());
        let owner = if current.is_alive() {
            current
        } else {
            crate::orchestration::install_plugin_authority(lua, plugin.clone())
        };
        lock_cell(scope.handle()).plugin_origin = Some((plugin, owner.generation()));
        scope
    }

    struct TestServices(TrustedTarget);

    impl OrchestrationServices for TestServices {
        fn resolve_target(
            &self,
            _: Option<&maki_commands::CommandInvocation>,
        ) -> Result<Option<TrustedTarget>, String> {
            Ok(Some(self.0.clone()))
        }

        fn register_target(&self, target: TrustedTarget) -> Result<(), String> {
            if target.target.same_manager(&self.0.target) {
                Ok(())
            } else {
                Err("wrong test manager".into())
            }
        }

        fn template(&self, target: &AgentRef) -> Result<ToolContext, String> {
            if target.same_manager(&self.0.target) {
                Ok(self.0.template.clone())
            } else {
                Err("wrong test manager".into())
            }
        }
    }

    #[test]
    fn standalone_current_returns_execution_child_not_installed_root() {
        let lua = Lua::new();
        crate::orchestration::install_plugin_authority(&lua, Arc::from("child-tool"));
        let scope = TaskScope::detached(&lua);
        let provider = Arc::new(StreamOnceProvider::new_replies(Vec::new()));
        let (actor, state, session, _) = session_with_provider(provider, None, None);
        let id = session.agent_id;
        let mut context = stub_ctx(&AgentMode::Build);
        let services = Arc::new(HostRootServices::fixed(
            AgentId::generate(),
            &context.model,
            RequestOptions::default(),
            Arc::new(ModelPolicy::default()),
        ));
        lua.set_app_data(HostRootServicesSlot {
            services: Some(services),
            template: Some(context.clone()),
        });
        context.ledger = state.params.get().unwrap().ledger.clone();
        lua.set_app_data(AgentSessions(Arc::new(Mutex::new(HashMap::from([(
            id, session,
        )])))));
        let mut ctx = LuaCtx::handler(&context);
        ctx.pin_host_root(&lua);
        ctx.bind_origin(scope.handle());
        let plugin: Arc<str> = Arc::from("child-tool");
        lock_cell(scope.handle()).plugin_origin = Some((plugin, 0));
        lock_cell(scope.handle()).delegated_tool = Some(Default::default());
        let userdata = lua.create_userdata(ctx).unwrap();
        let (child, error) = current(&lua, userdata.borrow().unwrap()).unwrap();
        assert!(error.is_none(), "{error:?}");
        let child = child.unwrap();
        assert_eq!(child.id, Some(id));
        assert!(matches!(child.manager, AgentRuntime::Unmanaged { .. }));
        actor.close();
    }

    #[test]
    fn spawn_inherits_defaults_while_legacy_session_keeps_empty_tools() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&AgentMode::Build);
            context.workflow = true;
            let mut expected = context.registry.definitions(
                &maki_agent::template::env_vars(),
                &DescriptionContext {
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
            context.turn_bindings = Arc::new(TurnToolBindings::capture(
                &context.registry,
                &context.local_tools,
                context.mcp.as_ref(),
            ));
            context.request_tools = Some(RequestTools::assembled(
                parent_definitions,
                &context.config,
                &context.model,
            ));
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(scope.handle());
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            let child: AnyUserData = scope
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
            const SPAWN_WITHOUT_TOOLS: &str = r#"
                return assert(agent_api.spawn(ctx, {
                    inherit_provider = true, silent = true, tools = {},
                }))
            "#;
            let empty: AnyUserData = scope
                .scope_future(lua.load(SPAWN_WITHOUT_TOOLS).eval_async())
                .await
                .unwrap();
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
            let legacy: AnyUserData = scope
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
    fn unmanaged_spawn_uses_actor_tickets_and_rejects_foreign_plugin_and_runtime() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&AgentMode::Build);
            context.provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, TokenUsage::default()),
            ]));
            context.request_tools = Some(RequestTools::default());
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(scope.handle());
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
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
            drop(scope);
            let fresh = plugin_scope(&lua, PLUGIN);
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(fresh.handle());
            lua.globals().set("ctx", ctx).unwrap();
            let retained: AnyUserData = lua.globals().get("retained_child").unwrap();
            let child = retained.borrow::<LuaAgent>().unwrap();
            let ctx: AnyUserData = lua.globals().get("ctx").unwrap();
            assert!(validate_unmanaged(&lua, &ctx.borrow().unwrap(), &child.manager).is_ok());
            {
                let foreign = plugin_scope(&lua, FOREIGN_PLUGIN);
                let mut ctx = LuaCtx::handler(&context);
                ctx.bind_origin(foreign.handle());
                assert_eq!(
                    validate_unmanaged(&lua, &ctx, &child.manager).unwrap_err(),
                    OTHER_PLUGIN_GENERATION
                );
            }
            {
                let other_lua = Lua::new();
                let foreign = plugin_scope(&other_lua, PLUGIN);
                let mut ctx = LuaCtx::handler(&context);
                ctx.bind_origin(foreign.handle());
                assert_eq!(
                    validate_unmanaged(&other_lua, &ctx, &child.manager).unwrap_err(),
                    OTHER_PLUGIN_GENERATION
                );
            }
            drop(child);
            let closed: bool = fresh
                .scope_future(lua.load("return retained_child:close(ctx)").eval_async())
                .await
                .unwrap();
            assert!(closed);
        });
    }

    #[test_case::test_case(false; "cancel_keeps_descendants_open")]
    #[test_case::test_case(true; "close_permanently_closes_descendants")]
    fn standalone_nested_subtree_lifecycle(close: bool) {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&AgentMode::Build);
            context.provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, TokenUsage::default()),
                canned_reply_with_usage(REPLY, TokenUsage::default()),
            ]));
            context.request_tools = Some(RequestTools::default());
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            let bind = |name: &str, context: &ToolContext| {
                let mut ctx = LuaCtx::handler(context);
                ctx.bind_origin(scope.handle());
                lua.globals().set(name, ctx).unwrap();
            };
            bind("ctx", &context);
            scope.scope_future(async {
                lua.load(r#"
                    parent = assert(agent_api.spawn(ctx, { inherit_provider = true, silent = true }))
                    sibling = assert(agent_api.spawn(ctx, { inherit_provider = true, silent = true }))
                "#).exec_async().await.unwrap();
                let parent: AnyUserData = lua.globals().get("parent").unwrap();
                let parent = parent.borrow::<LuaAgent>().unwrap().clone();
                let mut child_context = context.clone();
                let params = parent.state.as_ref().unwrap().params.get().unwrap();
                child_context.subagent_cancels = params.subagent_cancels.clone();
                child_context.ledger = params.ledger.clone();
                bind("child_ctx", &child_context);
                lua.load(r#"
                    child = assert(agent_api.spawn(child_ctx, { inherit_provider = true, silent = true }))
                "#).exec_async().await.unwrap();
                let child: AnyUserData = lua.globals().get("child").unwrap();
                let child = child.borrow::<LuaAgent>().unwrap().clone();
                let mut leaf_context = child_context.clone();
                let params = child.state.as_ref().unwrap().params.get().unwrap();
                leaf_context.subagent_cancels = params.subagent_cancels.clone();
                leaf_context.ledger = params.ledger.clone();
                bind("leaf_ctx", &leaf_context);
                let semaphore = Arc::new(async_lock::Semaphore::new(1));
                let permit = semaphore.acquire().await;
                lua.globals().set("gate", crate::api::r#async::LuaSemaphore { sem: semaphore.clone() }).unwrap();
                lua.load(r#"
                    leaf = assert(agent_api.spawn(leaf_ctx, {
                        inherit_provider = true, silent = true, semaphore = gate,
                    }))
                    active = assert(leaf:send(leaf_ctx, "active"))
                    queued = assert(leaf:enqueue(leaf_ctx, "queued"))
                "#).exec_async().await.unwrap();
                let leaf: AnyUserData = lua.globals().get("leaf").unwrap();
                let leaf = leaf.borrow::<LuaAgent>().unwrap().clone();
                let state = leaf.state.as_ref().unwrap();
                race(async {
                    while !state.execution_started.load(std::sync::atomic::Ordering::Acquire) {
                        smol::future::yield_now().await;
                    }
                }, async {
                    Timer::after(RETIRE_TIMEOUT).await;
                    panic!("leaf did not enter its turn");
                }).await;
                lua.globals().set("close_subtree", close).unwrap();
                lua.load(r#"
                    assert(parent:status(ctx).status == "idle")
                    assert(parent:cancel(ctx))
                    assert(leaf:status(leaf_ctx).status == "running")
                    if close_subtree then
                        assert(parent:close(ctx))
                    else
                        assert(parent:cancel_subtree(ctx))
                    end
                    assert(assert(active:wait(leaf_ctx, { timeout = 5 })).status == "cancelled")
                    assert(assert(queued:wait(leaf_ctx, { timeout = 5 })).status == "cancelled")
                    assert(sibling:status(ctx).status == "idle")
                "#).exec_async().await.unwrap();
                for agent in [&parent, &child, &leaf] {
                    assert_eq!(agent.manager.actor(agent.id).unwrap().snapshot().lifecycle,
                        if close { ActorLifecycle::Closed } else { ActorLifecycle::Open });
                }
                if !close {
                    drop(permit);
                    let text: String = lua.load(r#"
                        return assert(leaf:prompt(leaf_ctx, "after cancellation", { timeout = 5 })).text
                    "#).eval_async().await.unwrap();
                    assert_eq!(text, REPLY);
                }
                lua.load("assert(parent:close(ctx)); assert(sibling:close(ctx))")
                    .exec_async().await.unwrap();
                lua.globals().set("parent", mlua::Value::Nil).unwrap();
                drop(parent);
                lua.gc_collect().unwrap();
                // A retained parent-local map must still reject late child registration.
                lua.load(r#"
                    late = assert(agent_api.spawn(child_ctx, { inherit_provider = true, silent = true }))
                    assert(late:status(child_ctx).status == "closed")
                "#).exec_async().await.unwrap();
            }).await;
        });
    }

    const CHILD_NAME: &str = "reviewer";
    const POST_TEXT: &str = "review found issues";

    /// Runs {chunk} with `ctx` pinned to a trusted root and `agent_api` and
    /// `ui` in scope. Returns the chunk's string result and the posts the
    /// root's chat received.
    fn run_trusted(chunk: &str) -> (String, Vec<Post>) {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let (event_tx, event_rx) = flume::unbounded();
            let context = stub_ctx_with(
                &AgentMode::Build,
                Some(&EventSender::new(event_tx, 0)),
                None,
            );
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let mut root_fixture = None;
            let root = manager
                .create_root_with_config(None, Vec::new(), None, |id| {
                    let (_, state, session, _) =
                        session_with_provider_for_agent(Arc::clone(&context.provider), id);
                    root_fixture = Some(session);
                    Ok::<_, String>(Box::new(LuaActorBackend::new(state)))
                })
                .unwrap();
            let trusted_target = TrustedTarget {
                target: root,
                template: context,
            };
            lua.set_app_data(OrchestrationServicesSlot(Some(Arc::new(TestServices(
                trusted_target.clone(),
            )))));
            let ctx = LuaCtx::trusted_context(&lua, trusted_target, Arc::from(PLUGIN));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            lua.globals()
                .set(
                    "ui",
                    crate::api::ui::create_ui_table(&lua, None, Arc::from(PLUGIN), None).unwrap(),
                )
                .unwrap();
            let result: String = scope
                .scope_future(lua.load(chunk).eval_async())
                .await
                .unwrap();
            let posts = event_rx
                .drain()
                .filter_map(|envelope| match envelope.event {
                    AgentEvent::Post(post) => Some(post),
                    _ => None,
                })
                .collect();
            manager.shutdown(RETIRE_TIMEOUT).await;
            drop(root_fixture);
            (result, posts)
        })
    }

    #[test_case::test_case("nil", Some(CHILD_NAME) ; "shown_by_default")]
    #[test_case::test_case("'the pending diff'", Some("reviewer: the pending diff") ; "with_description")]
    #[test_case::test_case("false", None ; "hidden")]
    fn spawn_posts_a_row_for_shown_children(show: &str, expected: Option<&str>) {
        let (transcript, posts) = run_trusted(&format!(
            r#"
            local child = assert(agent_api.spawn(ctx, {{
                inherit_provider = true, tools = {{}}, name = "{CHILD_NAME}", show = {show},
            }}))
            local id = child:_maki_session_id()
            assert(child:close(ctx))
            return id
        "#
        ));
        let expected: Vec<_> = expected
            .map(|text| Post {
                text: text.into(),
                level: PostLevel::Info,
                agent: Some(transcript),
            })
            .into_iter()
            .collect();
        assert_eq!(posts, expected);
    }

    #[test]
    fn showing_a_silent_child_is_an_error() {
        let (error, posts) = run_trusted(
            r#"
            local child, err = agent_api.spawn(ctx, {
                inherit_provider = true, tools = {}, silent = true, show = true,
            })
            assert(child == nil)
            return err
        "#,
        );
        assert_eq!(error, crate::agent_errors::SHOW_SILENT_AGENT);
        assert!(posts.is_empty());
    }

    #[test_case::test_case("nil", None ; "hidden_by_default")]
    #[test_case::test_case("true", Some(crate::agent_errors::SHOW_TOOL_AGENT) ; "explicit_show_refused")]
    fn tool_spawned_children_keep_their_tool_row(show: &str, expected: Option<&str>) {
        const TOOL_USE_ID: &str = "call_review";
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let (event_tx, event_rx) = flume::unbounded();
            let context = stub_ctx_with(
                &AgentMode::Build,
                Some(&EventSender::new(event_tx, 0)),
                Some(TOOL_USE_ID),
            );
            let mut ctx = LuaCtx::handler(&context);
            ctx.bind_origin(scope.handle());
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            let error: Option<String> = scope
                .scope_future(
                    lua.load(format!(
                        r#"
                local child, err = agent_api.spawn(ctx, {{
                    inherit_provider = true, tools = {{}}, show = {show},
                }})
                if child then assert(child:close(ctx)) end
                return err
            "#
                    ))
                    .eval_async(),
                )
                .await
                .unwrap();
            assert_eq!(error.as_deref(), expected);
            assert!(
                !event_rx
                    .drain()
                    .any(|envelope| matches!(envelope.event, AgentEvent::Post(_)))
            );
        });
    }

    #[test]
    fn post_links_a_child_at_the_requested_level() {
        let (transcript, posts) = run_trusted(&format!(
            r#"
            local child = assert(agent_api.spawn(ctx, {{
                inherit_provider = true, tools = {{}}, show = false,
            }}))
            assert(ui.post(ctx, "{POST_TEXT}", {{ level = "error", agent = child }}))
            local id = child:_maki_session_id()
            assert(child:close(ctx))
            return id
        "#
        ));
        assert_eq!(
            posts,
            [Post {
                text: POST_TEXT.into(),
                level: PostLevel::Error,
                agent: Some(transcript),
            }]
        );
    }

    #[test_case::test_case("'  '", "nil", crate::agent_errors::BLANK_POST ; "blank_text")]
    #[test_case::test_case("'text'", "{ level = 'fatal' }", crate::agent_errors::UNKNOWN_POST_LEVEL ; "unknown_level")]
    #[test_case::test_case("'text'", "{ colour = 'red' }", crate::agent_errors::UNKNOWN_POST_OPTION ; "unknown_option")]
    fn invalid_posts_return_errors(text: &str, opts: &str, expected: &str) {
        let (error, posts) = run_trusted(&format!(
            r#"
            local ok, err = ui.post(ctx, {text}, {opts})
            assert(ok == nil)
            return err
        "#
        ));
        assert!(error.starts_with(expected), "{error}");
        assert!(posts.is_empty());
    }

    #[test]
    fn abandoned_defer_close_scope_removes_retained_session_without_gc() {
        smol::block_on(async {
            let lua = Lua::new();
            crate::orchestration::install_plugin_authority(&lua, Arc::from("agent-test"));
            let scope = TaskScope::detached(&lua);
            let context = stub_ctx(&AgentMode::Build);
            let config = EffectiveAgentConfig::new(
                RunSettings {
                    model: (*context.model).clone(),
                    provider: Arc::clone(&context.provider),
                    thinking: context.opts.thinking,
                    fast: context.opts.fast,
                    workflow: false,
                },
                context.mode.clone(),
            );
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let mut root_fixture = None;
            let target = manager
                .create_root_with_config(Some(config), Vec::new(), None, |id| {
                    let (_, state, session, _) =
                        session_with_provider_for_agent(Arc::clone(&context.provider), id);
                    root_fixture = Some(session);
                    Ok::<_, String>(Box::new(LuaActorBackend::new(state)))
                })
                .unwrap();
            let trusted_target = TrustedTarget {
                target,
                template: context,
            };
            lua.set_app_data(OrchestrationServicesSlot(Some(Arc::new(TestServices(
                trusted_target.clone(),
            )))));
            let ctx = LuaCtx::trusted_context(&lua, trusted_target, Arc::from("agent-test"));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            let child: AnyUserData = scope
                .scope_future(
                    lua.load(
                        r#"
                local child = assert(agent_api.spawn(ctx, {
                    inherit_provider = true, silent = true, tools = {},
                }))
                assert(child:defer_close(ctx))
                return child
            "#,
                    )
                    .eval_async(),
                )
                .await
                .unwrap();
            let child = child.borrow::<LuaAgent>().unwrap();
            let actor = child.manager.lookup(child.id).unwrap().actor().unwrap();
            assert_eq!(actor.snapshot().lifecycle, ActorLifecycle::Open);
            let sessions = lua.app_data_ref::<AgentSessions>().unwrap().clone();
            assert!(sessions.lock().contains_key(&child.id.unwrap()));
            drop(scope);
            assert_ne!(actor.snapshot().lifecycle, ActorLifecycle::Open);
            race(
                async {
                    while sessions.lock().contains_key(&child.id.unwrap()) {
                        smol::future::yield_now().await;
                    }
                },
                async {
                    Timer::after(RETIRE_TIMEOUT).await;
                    panic!("deferred session ownership was not released");
                },
            )
            .await;
            assert_eq!(actor.snapshot().lifecycle, ActorLifecycle::Closed);
            assert!(actor.snapshot().active_turn.is_none());
            manager.shutdown(RETIRE_TIMEOUT).await;
            drop(root_fixture);
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
        revoke_plugin(&lua, OWNER);
        assert_ne!(actor.snapshot().lifecycle, ActorLifecycle::Open);
        assert_eq!(other_actor.snapshot().lifecycle, ActorLifecycle::Open);
        assert_eq!(lua.app_data_ref::<AgentSessions>().unwrap().lock().len(), 1);
        revoke_plugin(&lua, OWNER);
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
            let config = EffectiveAgentConfig::new(
                RunSettings {
                    model: params.model.clone(),
                    provider: Arc::clone(&params.provider),
                    thinking: state.opts.thinking,
                    fast: state.opts.fast,
                    workflow: false,
                },
                AgentMode::Build,
            )
            .with_mode_def(Some(params.modes.current(&AgentMode::Build)));
            let mut managed_fixture = None;
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let target = manager
                .create_root_with_config(Some(config), Vec::new(), None, |agent_id| {
                    let (actor, root_state, session, events) =
                        session_with_provider_for_agent(Arc::clone(&params.provider), agent_id);
                    assert_eq!(root_state.params.get().unwrap().agent_id, agent_id);
                    managed_fixture = Some((actor, Arc::clone(&root_state), session, events));
                    Ok::<_, String>(Box::new(LuaActorBackend::new(root_state)))
                })
                .unwrap();
            let lua = Lua::new();
            crate::orchestration::install_plugin_authority(&lua, Arc::from("agent-test"));
            let scope = TaskScope::detached(&lua);
            let mut template = stub_ctx(&AgentMode::Build);
            template.provider = Arc::clone(&params.provider);
            let trusted_target = TrustedTarget {
                target: target.clone(),
                template,
            };
            lua.set_app_data(OrchestrationServicesSlot(Some(Arc::new(TestServices(
                trusted_target.clone(),
            )))));
            let ctx = LuaCtx::trusted_context(&lua, trusted_target, Arc::from("agent-test"));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set(
                    "agent",
                    LuaAgent {
                        manager: AgentRuntime::Managed(manager.clone()),
                        id: Some(target.id()),
                        state: Some(Arc::clone(&managed_fixture.as_ref().unwrap().1)),
                        held: Some(target.clone()),
                        root: Some(manager.root().unwrap()),
                        ancestors: vec![target.id()],
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
