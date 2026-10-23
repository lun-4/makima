mod agent_methods;
mod authority;
mod module_fns;
pub(super) mod projection;
mod subscriptions;
mod tickets;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use maki_agent::actor::ActorLifecycle;
use maki_agent::{AgentActorHandle, AgentId, AgentManagerHandle, AgentRef, TurnTicket};
use maki_lua_macro::{lua_class, lua_fn};
use mlua::Lua;

use crate::api::agent::{LuaActorState, LuaSession};
use crate::api::util::pair::Pair;

pub(crate) use agent_methods::AGENT_DOCS;
pub(crate) use authority::lookup as lookup_agent;
pub(crate) use module_fns::{current, focused_agent_ref};
pub(super) use module_fns::{
    get__doc, get__register, list__doc, list__register, root__doc, root__register, spawn__doc,
    spawn__register,
};
pub(crate) use projection::{origin_json, result_json, selection_json};
pub(crate) use subscriptions::SUBSCRIPTION_DOCS;
pub(crate) use tickets::TURN_DOCS;

#[derive(Clone)]
pub(crate) struct LuaAgent {
    manager: AgentManagerHandle,
    id: AgentId,
    state: Option<Arc<LuaActorState>>,
    held: Option<AgentRef>,
    root: Option<AgentRef>,
    ancestors: Vec<AgentId>,
}

impl LuaAgent {
    /// The autocmd filter for this agent's events, making sure its graph
    /// feeds this host.
    pub(crate) fn listen(&self, lua: &Lua) -> crate::api::autocmd::AgentFilter {
        crate::agent_autocmd::observe(lua, &self.manager);
        crate::api::autocmd::AgentFilter::new(&self.manager, self.id)
    }

    /// The id the UI keys this agent's transcript by.
    pub(crate) fn transcript_id(&self) -> Option<&str> {
        self.state.as_ref().map(|state| state.ui_id.as_str())
    }

    fn identity(&self) -> String {
        self.id.to_string()
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
    manager: AgentManagerHandle,
    id: AgentId,
    #[expect(dead_code, reason = "keeps the node's terminal snapshot slot alive")]
    held: Option<AgentRef>,
}

#[derive(Clone)]
struct LuaAgentTurn {
    agent: LuaAgent,
    ticket: TurnTicket,
}

#[derive(Clone, Default)]
struct AgentOwners(Arc<Mutex<HashMap<AgentId, AgentOwner>>>);

struct AgentOwner {
    plugin: Option<Arc<str>>,
    actor: Weak<AgentActorHandle>,
    state: Weak<LuaActorState>,
    /// Dropping the session closes the child and emits its ordered close
    /// notification, so the record keeps it alive until it is retired.
    hold: Option<LuaSession>,
}

impl AgentOwner {
    /// Take the hold so a caller can drop it outside the registry lock.
    fn take_hold(&mut self) -> Option<LuaSession> {
        self.hold.take()
    }
}
impl AgentOwners {
    fn lock(&self) -> MutexGuard<'_, HashMap<AgentId, AgentOwner>> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Record a spawned child, keeping its session alive until it is retired.
    fn insert(&self, session: LuaSession, plugin: Option<Arc<str>>) {
        let id = session.agent_id;
        let owner = AgentOwner {
            plugin,
            actor: Arc::downgrade(&session.actor),
            state: Arc::downgrade(&session.state),
            hold: Some(session),
        };
        self.lock().insert(id, owner);
    }

    /// Records are dropped after the lock is released, since dropping an
    /// active session re-enters Lua.
    fn retire(&self, id: AgentId) {
        let retired = self.lock().get_mut(&id).and_then(AgentOwner::take_hold);
        drop(retired);
    }

    fn retire_settled(&self) {
        let retired: Vec<_> = self
            .lock()
            .extract_if(|_, owner| owner.settled_closed())
            .collect();
        drop(retired);
    }

    fn state(&self, id: AgentId) -> Option<Arc<LuaActorState>> {
        self.lock().get(&id).and_then(|owner| owner.state.upgrade())
    }
}

impl AgentOwner {
    fn settled_closed(&self) -> bool {
        let Some(actor) = self.actor.upgrade() else {
            return true;
        };
        let snapshot = actor.snapshot();
        snapshot.lifecycle != ActorLifecycle::Open && snapshot.active_turn.is_none()
    }
}

pub(super) fn close_plugin_agents(lua: &Lua, plugin: &str) {
    let Some(owners) = lua
        .app_data_ref::<AgentOwners>()
        .map(|owners| owners.clone())
    else {
        return;
    };
    let retired: Vec<_> = owners
        .lock()
        .extract_if(|_, owner| owner.plugin.as_deref() == Some(plugin))
        .map(|(_, owner)| owner)
        .collect();
    drop(retired);
}

pub(super) fn session_state(lua: &Lua, id: AgentId) -> Option<Arc<LuaActorState>> {
    lua.app_data_ref::<AgentOwners>()
        .and_then(|owners| owners.state(id))
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use futures_lite::future::race;
    use maki_agent::tools::test_support::{stub_ctx, stub_ctx_with};
    use maki_agent::tools::{DescriptionContext, RequestTools, ToolContext, TurnToolBindings};
    use maki_agent::{
        AgentEvent, AgentLimits, AgentMode, EffectiveAgentConfig, EventSender, Post, PostLevel,
        RunSettings,
    };
    use maki_commands::{
        CommandError, CommandFuture, CommandHost, HostRequest, HostResponse, TargetCapabilities,
    };
    use maki_providers::TokenUsage;
    use mlua::{AnyUserData, Table};
    use smol::Timer;
    use test_case::test_case;

    use super::*;
    use crate::UiAction;
    use crate::agent_errors::{
        BLANK_POST, SHOW_SILENT_AGENT, SHOW_TOOL_AGENT, UNKNOWN_POST_LEVEL, UNKNOWN_POST_OPTION,
    };
    use crate::api::agent::structured_output::{StructuredOutput, TOOL_NAME};
    use crate::api::agent::tests::{
        HangingProvider, StreamOnceProvider, canned_reply_with_usage, session_with_provider,
        session_with_provider_for_agent,
    };
    use crate::api::agent::{LuaActorBackend, create_agent_table};
    use crate::api::ui::create_ui_table;
    use crate::api::util::ctx::LuaCtx;
    use crate::orchestration::{
        OrchestrationServices, OrchestrationServicesSlot, TrustedTarget, revoke_plugin,
    };
    use crate::runtime::{TaskScope, lock_cell};
    use crate::test_support::spawn_host_for_tests;

    pub(super) const REPLY: &str = "retained reply";
    pub(super) const RETIRE_TIMEOUT: Duration = Duration::from_secs(5);
    const PLUGIN: &str = "agent-test";
    const EDITOR_PATH: &str = "plan.md";

    struct EditorCommandHost;

    impl CommandHost for EditorCommandHost {
        fn request(&self, _: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
            Box::pin(async { Err(CommandError::Producer("unexpected host request".into())) })
        }
    }

    fn plugin_scope(lua: &Lua, plugin: &str) -> TaskScope {
        let scope = TaskScope::detached(lua);
        let plugin: Arc<str> = Arc::from(plugin);
        let current = crate::orchestration::plugin_authority(lua, plugin.clone());
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

        fn register_child_target(&self, target: TrustedTarget, _: String) -> Result<(), String> {
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
    fn spawn_inherits_defaults_while_a_bare_root_cannot_spawn() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&AgentMode::Build);
            context.workflow = true;
            let mut expected = context.registry.definitions(
                &maki_agent::template::env_vars_for(&context.cwd),
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
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            let root = install_managed_root(&lua, &context);
            let spawn_child = lua.load(
                r#"
                return assert(agent_api.spawn(ctx, { inherit_provider = true, silent = true }))
            "#,
            );
            let child: AnyUserData = scope.scope_future(spawn_child.eval_async()).await.unwrap();
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
            child.manager.close_subtree(child.id).unwrap();
            root.shutdown();
        });
    }

    #[test]
    fn managed_child_uses_actor_tickets_and_transcript() {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&AgentMode::Build);
            context.provider = Arc::new(StreamOnceProvider::new_replies(vec![
                canned_reply_with_usage(REPLY, TokenUsage::default()),
            ]));
            context.request_tools = Some(RequestTools::default());
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            let root = install_managed_root(&lua, &context);
            let result: String = scope
                .scope_future(
                    lua.load(
                        r#"
                local child, err = agent_api.spawn(ctx, { inherit_provider = true, silent = true })
                assert(child, err)
                retained_child = child
                local reference = assert(child:ref())
                assert(agent_api.get(ctx, reference):id() == child:id())
                local ticket = assert(child:send(ctx, "standalone request", { when = "idle" }))
                local result = assert(ticket:wait(ctx))
                assert(ticket:result(ctx).text == result.text)
                assert(child:transcript(ctx, { last_messages = 100, max_bytes = 65536 }))
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
            let retained: AnyUserData = lua.globals().get("retained_child").unwrap();
            let child = retained.borrow::<LuaAgent>().unwrap();
            drop(child);
            // A retained handle must still close with managed authority from a
            // later plugin scope.
            root.trusted_ctx(&lua);
            let closed: bool = fresh
                .scope_future(lua.load("return retained_child:close(ctx)").eval_async())
                .await
                .unwrap();
            assert!(closed);
            root.shutdown();
        });
    }

    /// A minimal admitted turn input for exercising manager/actor state.
    fn turn_input() -> maki_agent::AgentInput {
        maki_agent::AgentInput {
            message: "turn".into(),
            images: Vec::new(),
            preamble: Vec::new(),
            prompt: None,
        }
    }

    #[test_case(false; "cancel_keeps_descendants_open")]
    #[test_case(true; "close_permanently_closes_descendants")]
    fn standalone_nested_subtree_lifecycle(close: bool) {
        smol::block_on(async {
            let lua = Lua::new();
            // The trusted context below needs an active task scope.
            let _scope = plugin_scope(&lua, PLUGIN);
            let mut context = stub_ctx(&AgentMode::Build);
            // A parked provider keeps the leaf's turn running for the whole
            // test, so cancellation is observable rather than already finished.
            context.provider = Arc::new(HangingProvider);
            let root = install_managed_root_with(
                &lua,
                &context,
                AgentLimits {
                    // One running turn at a time, so the leaf parks with its
                    // turn active and a queued admission behind it.
                    max_concurrent_agent_turns: 1,
                    ..AgentLimits::default()
                },
            );
            let manager = root.manager.clone();
            let config = maki_agent::actor::EffectiveAgentConfig::new(
                maki_agent::RunSettings {
                    provider: Arc::clone(&context.provider),
                    model: context.model.as_ref().clone(),
                    fast: false,
                    workflow: false,
                    thinking: Default::default(),
                },
                AgentMode::Build,
            );
            let mut fixtures = Vec::new();
            let mut spawn = |parent: &AgentRef| {
                let config = config.clone();
                manager
                    .spawn_child_trusted_with_config(
                        parent,
                        config,
                        maki_agent::AgentMetadata::default(),
                        Vec::new(),
                        None,
                        |id| {
                            let (_, state, fixture, _) =
                                session_with_provider_for_agent(Arc::clone(&context.provider), id);
                            fixtures.push(fixture);
                            Ok::<_, String>(Box::new(LuaActorBackend::new(state)))
                        },
                    )
                    .unwrap()
            };
            let parent = spawn(&root.target.target);
            let sibling = spawn(&root.target.target);
            let child = spawn(&parent);
            let leaf = spawn(&child);
            let parent_actor = parent.actor().unwrap();
            let child_actor = child.actor().unwrap();
            let leaf_actor = leaf.actor().unwrap();
            let open =
                |actor: &AgentActorHandle| actor.snapshot().lifecycle == ActorLifecycle::Open;
            for actor in [&parent_actor, &child_actor, &leaf_actor] {
                assert!(open(actor), "every node starts open");
            }
            let active = leaf_actor
                .admit_turn(turn_input(), None, "active".into())
                .unwrap();
            let queued = leaf_actor
                .admit_turn(turn_input(), None, "queued".into())
                .unwrap();
            // Cancelling the parent must spare its descendants.
            assert!(parent.cancel().is_ok());
            let outcome = manager
                .node(leaf.id())
                .expect("leaf is a manager node")
                .graph_lifecycle;
            assert_eq!(
                outcome,
                maki_agent::manager::GraphLifecycle::Live,
                "cancelling an ancestor keeps descendants live"
            );
            if close {
                parent.close_subtree().unwrap();
                assert!(leaf_actor.snapshot().lifecycle != ActorLifecycle::Open);
            } else {
                manager.cancel_subtree(parent.id()).unwrap();
            }
            for actor in [&parent_actor, &child_actor, &leaf_actor] {
                assert_eq!(
                    actor.snapshot().lifecycle,
                    if close {
                        ActorLifecycle::Closed
                    } else {
                        ActorLifecycle::Open
                    },
                    "cancel leaves the subtree open; close closes it"
                );
            }
            assert!(sibling.close_subtree().is_ok());
            drop((active, queued, fixtures));
            root.shutdown();
        });
    }

    #[test_case("/editor", false ; "direct_focus_changes_after_dispatch")]
    #[test_case("/editor", true ; "direct_target_closes_after_dispatch")]
    #[test_case("/edit", false ; "nested_focus_changes_after_dispatch")]
    #[test_case("/edit", true ; "nested_target_closes_after_dispatch")]
    fn editor_request_retains_the_invocation_agent(cmdline: &'static str, close_target: bool) {
        smol::block_on(async {
            let context = stub_ctx(&AgentMode::Build);
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let replacement_manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let mut fixtures = Vec::new();
            let mut create = |manager: &AgentManagerHandle| {
                manager
                    .create_root_with_config(None, Vec::new(), None, |id| {
                        let (_, state, session, _) =
                            session_with_provider_for_agent(Arc::clone(&context.provider), id);
                        fixtures.push(session);
                        Ok::<_, String>(Box::new(LuaActorBackend::new(state)))
                    })
                    .unwrap()
            };
            let original = create(&manager);
            let replacement = create(&replacement_manager);
            let (handle, host) = spawn_host_for_tests(&[]);
            handle.install_orchestration_services(Arc::new(TestServices(TrustedTarget {
                target: original.clone(),
                template: context.clone(),
            })));
            host.host()
                .load_source(
                    PLUGIN,
                    r#"
                    maki.api.register_command({
                        name = "/editor", tui_only = false,
                        handler = function(_, ctx)
                            local agent = assert(ctx:agent())
                            local code = maki.ui.open_editor("plan.md")
                            maki.ui.flash(agent:id() .. "|" .. tostring(code))
                        end,
                    })
                    maki.api.register_command({
                        name = "/edit", tui_only = false,
                        handler = function(_, ctx) assert(maki.api.run_command(ctx, "/editor")) end,
                    })
                    "#,
                )
                .unwrap();
            let commands = host.host().command_registry();
            let target = commands.bind_target(TargetCapabilities::ALL, Arc::new(EditorCommandHost));
            let dispatch =
                smol::spawn(async move { commands.dispatch_input(&target, cmdline.into()).await });
            let actions = host.host().ui_action_rx();
            let UiAction::OpenEditor {
                target,
                path,
                reply_tx,
            } = actions.recv_timeout(RETIRE_TIMEOUT).unwrap()
            else {
                panic!("expected editor request");
            };
            handle.install_orchestration_services(Arc::new(TestServices(TrustedTarget {
                target: replacement.clone(),
                template: context,
            })));
            host.host().wait_for_worker_barrier_for_test();
            if close_target {
                manager.close_subtree(original.id()).unwrap();
            }
            let target = target.unwrap();
            assert_eq!(target.id(), original.id());
            assert_ne!(target.id(), replacement.id());
            assert_eq!(target.validate_live().is_err(), close_target);
            assert_eq!(path, PathBuf::from(EDITOR_PATH));
            let code = if close_target { -1 } else { 0 };
            reply_tx.send(code).unwrap();
            let UiAction::Flash(message) = actions.recv_timeout(RETIRE_TIMEOUT).unwrap() else {
                panic!("expected editor result");
            };
            assert_eq!(message, format!("{}|{code}", original.id()));
            dispatch.await;
            manager.shutdown(RETIRE_TIMEOUT).await;
            replacement_manager.shutdown(RETIRE_TIMEOUT).await;
            drop(fixtures);
        });
    }

    const CHILD_NAME: &str = "reviewer";
    const POST_TEXT: &str = "review found issues";

    /// A managed root installed in a Lua state, with the pieces a test needs to
    /// drive it and shut it down.
    struct ManagedRoot {
        manager: AgentManagerHandle,
        target: TrustedTarget,
        fixture: Option<crate::api::agent::tests::BackendFixture>,
        events: flume::Receiver<maki_agent::Envelope>,
    }

    impl ManagedRoot {
        /// A trusted context for the same root, for code that runs outside the
        /// first scope but still needs managed authority.
        fn trusted_ctx(&self, lua: &Lua) -> AnyUserData {
            let ctx = LuaCtx::trusted_context(lua, self.target.clone(), Arc::from(PLUGIN));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals().get("ctx").unwrap()
        }

        fn shutdown(self) {
            // The manager must go down before the fixture: dropping the fixture
            // first would terminalize turns the manager still tracks.
            let Self {
                manager,
                fixture,
                events,
                ..
            } = self;
            drop(events);
            smol::block_on(manager.shutdown(RETIRE_TIMEOUT));
            drop(fixture);
        }
    }

    /// Installs a managed root whose trusted context becomes the global `ctx`.
    /// Children spawned through it are real manager nodes, which is the only
    /// way `maki.agent.spawn` authorises a parent.
    fn install_managed_root(lua: &Lua, context: &ToolContext) -> ManagedRoot {
        install_managed_root_with(lua, context, AgentLimits::default())
    }

    fn install_managed_root_with(
        lua: &Lua,
        context: &ToolContext,
        limits: AgentLimits,
    ) -> ManagedRoot {
        let (event_tx, event_rx) = flume::unbounded();
        let mut rooted = context.clone();
        rooted.event_tx = EventSender::new(event_tx);
        let manager = AgentManagerHandle::new(limits).unwrap();
        let mut fixture = None;
        let root = manager
            .create_root_with_config(None, Vec::new(), None, |id| {
                let (_, state, session, _) =
                    session_with_provider_for_agent(Arc::clone(&rooted.provider), id);
                fixture = Some(session);
                Ok::<_, String>(Box::new(LuaActorBackend::new(state)))
            })
            .unwrap();
        let trusted_target = TrustedTarget {
            target: root,
            template: rooted,
        };
        lua.set_app_data(OrchestrationServicesSlot(Some(Arc::new(TestServices(
            trusted_target.clone(),
        )))));
        let ctx = LuaCtx::trusted_context(lua, trusted_target.clone(), Arc::from(PLUGIN));
        lua.globals().set("ctx", ctx).unwrap();
        ManagedRoot {
            manager,
            target: trusted_target,
            fixture,
            events: event_rx,
        }
    }

    /// Runs {chunk} with `ctx` pinned to a trusted root and `agent_api` and
    /// `ui` in scope. Returns the chunk's string result and the posts the
    /// root's chat received.
    fn run_trusted(chunk: &str) -> (String, Vec<Post>) {
        run_trusted_with(chunk, None)
    }

    /// Like [`run_trusted`], with the root's template carrying `tool_use_id`
    /// so children spawned from its handler count as tool-spawned.
    fn run_trusted_with(chunk: &str, tool_use_id: Option<&str>) -> (String, Vec<Post>) {
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let context = stub_ctx_with(&AgentMode::Build, None, tool_use_id);
            let root = install_managed_root(&lua, &context);
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            lua.globals()
                .set(
                    "ui",
                    create_ui_table(&lua, None, Arc::from(PLUGIN), None).unwrap(),
                )
                .unwrap();
            let result: String = scope
                .scope_future(lua.load(chunk).eval_async())
                .await
                .unwrap();
            let posts = root
                .events
                .drain()
                .filter_map(|envelope| match envelope.event {
                    AgentEvent::Post(post) => Some(post),
                    _ => None,
                })
                .collect();
            root.shutdown();
            (result, posts)
        })
    }

    #[test_case("nil", Some(CHILD_NAME) ; "shown_by_default")]
    #[test_case("'the pending diff'", Some("reviewer: the pending diff") ; "with_description")]
    #[test_case("false", None ; "hidden")]
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
        assert_eq!(error, SHOW_SILENT_AGENT);
        assert!(posts.is_empty());
    }

    #[test_case("nil", None ; "hidden_by_default")]
    #[test_case("true", Some(SHOW_TOOL_AGENT) ; "explicit_show_refused")]
    fn tool_spawned_children_keep_their_tool_row(show: &str, expected: Option<&str>) {
        const TOOL_USE_ID: &str = "call_review";
        let (error, posts) = run_trusted_with(
            &format!(
                r#"
            local child, err = agent_api.spawn(ctx, {{
                inherit_provider = true, tools = {{}}, show = {show},
            }})
            if child then assert(child:close(ctx)) end
            return err or ""
        "#
            ),
            Some(TOOL_USE_ID),
        );
        assert_eq!(error, expected.unwrap_or_default());
        assert!(
            posts.is_empty(),
            "a child spawned by a tool call already has a tool row"
        );
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

    #[test_case("'  '", "nil", BLANK_POST ; "blank_text")]
    #[test_case("'text'", "{ level = 'fatal' }", UNKNOWN_POST_LEVEL ; "unknown_level")]
    #[test_case("'text'", "{ colour = 'red' }", UNKNOWN_POST_OPTION ; "unknown_option")]
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
            let sessions = lua.app_data_ref::<AgentOwners>().unwrap().clone();
            assert!(sessions.lock().contains_key(&child.id));
            drop(scope);
            assert_ne!(actor.snapshot().lifecycle, ActorLifecycle::Open);
            race(
                async {
                    while sessions.lock().contains_key(&child.id) {
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
        const OTHER_OWNER: &str = "other-owner";
        smol::block_on(async {
            let lua = Lua::new();
            let scope = plugin_scope(&lua, PLUGIN);
            let root = install_managed_root(&lua, &stub_ctx(&AgentMode::Build));
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            let spawn = |ctx: &str| {
                lua.load(format!(
                    "return assert(agent_api.spawn({ctx}, {{ inherit_provider = true, silent = true, tools = {{}} }}))"
                ))
                .eval_async::<AnyUserData>()
            };
            let owned = scope.scope_future(spawn("ctx")).await.unwrap();
            let other_scope = plugin_scope(&lua, OTHER_OWNER);
            let other_ctx =
                LuaCtx::trusted_context(&lua, root.target.clone(), Arc::from(OTHER_OWNER));
            lua.globals().set("other_ctx", other_ctx).unwrap();
            let other = other_scope.scope_future(spawn("other_ctx")).await.unwrap();
            let actor = |handle: &AnyUserData| {
                let agent = handle.borrow::<LuaAgent>().unwrap();
                agent.manager.lookup(agent.id).unwrap().actor().unwrap()
            };
            let (owned, other) = (actor(&owned), actor(&other));
            revoke_plugin(&lua, PLUGIN);
            assert_ne!(owned.snapshot().lifecycle, ActorLifecycle::Open);
            assert_eq!(other.snapshot().lifecycle, ActorLifecycle::Open);
            assert_eq!(
                lua.app_data_ref::<AgentOwners>().unwrap().lock().len(),
                1,
                "revocation drops only the revoked plugin's ownership record"
            );
            revoke_plugin(&lua, PLUGIN);
            assert_eq!(other.snapshot().lifecycle, ActorLifecycle::Open);
            drop((scope, other_scope));
            root.shutdown();
        });
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
            let (_standalone, state, legacy, _events) = session_with_provider(provider, None);
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
                        manager: manager.clone(),
                        id: target.id(),
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
                local ticket, err = agent:send(ctx, "hello", { when = "idle" })
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
