use makima_agent::{AgentId, AgentManagerHandle, AgentRef, CurrentManagedTurn};
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

fn trusted_target(lua: &Lua, ctx: &LuaCtx, target: &AgentRef) -> Result<(), String> {
    let trusted = ctx.trusted(lua)?;
    trusted.validate_incarnation()?;
    observe_trusted_target(lua, ctx, target)
}

fn observe_trusted_target(lua: &Lua, ctx: &LuaCtx, target: &AgentRef) -> Result<(), String> {
    let trusted = ctx.trusted(lua)?;
    if !trusted.target.same_manager(target) {
        return orchestration::services(lua)
            .ok_or(crate::agent_errors::NO_ORCHESTRATION_SERVICES)?
            .validate_target(target);
    }
    let subject = trusted
        .target
        .snapshot()
        .map_err(|error| error.to_string())?;
    let target = target.snapshot().map_err(|error| error.to_string())?;
    if subject.root_id != target.root_id {
        return Err("agent belongs to another graph".into());
    }
    Ok(())
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
            trusted_target(lua, ctx, &manager.reference(target))?;
            Ok(None)
        }
        _ => Err(MANAGED_AUTHORITY_INACTIVE.into()),
    }
}

pub(in crate::api::agent) fn authorize(
    lua: &Lua,
    ctx: &LuaCtx,
    agent: &LuaAgent,
) -> Result<Option<CurrentManagedTurn>, String> {
    if ctx.trusted(lua).is_ok() {
        trusted_target(lua, ctx, &agent.manager.reference(agent.id))?;
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
    if ctx.trusted(lua).is_ok() {
        observe_trusted_target(lua, ctx, &agent.manager.reference(agent.id))?;
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
    if let Some(template) =
        session_state(lua, agent.id).and_then(|state| state.template.get().cloned())
    {
        let mut template = template;
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
    _lua: &Lua,
) -> Result<LuaAgent, String> {
    let held = manager.reference(id);
    let node = held.snapshot().map_err(|error| error.to_string())?;
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
        transcript_key: manager
            .journal()
            .agent(id)
            .map(|record| record.transcript_key)
            .or(node.metadata.transcript_key),
        manager,
        id,
        held: Some(held),
        ancestors,
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;

    use futures_lite::future::race;
    use makima_agent::actor::{
        ActorHistory, ActorLifecycle, AdmissionPreparation, AgentContextSnapshot,
        EffectiveAgentConfig, PendingQueueEntry,
    };
    use makima_agent::agent::RunSettings;
    use makima_agent::journal::{JournalChange, JournalCommit, SessionJournal};
    use makima_agent::manager::ResidentAgent;
    use makima_agent::tools::test_support::stub_ctx;
    use makima_agent::{
        ActorBackend, ActorEvent, AgentId, AgentInput, AgentLimits, AgentManagerHandle, AgentMode,
        BackendResult, ControlWork, HistorySnapshot, ManagerError, TurnCancellationReason,
        TurnContext, TurnId, TurnOutcome, TurnProvenance, TurnResult, WorkKind,
    };
    use makima_commands::{
        CommandArguments, CommandBehavior, CommandDocs, CommandError, CommandFuture, CommandHost,
        CommandInvocation, CommandOutcome, CommandRegistry, CommandSpec, HostRequest, HostResponse,
        ProducerPrecedence, Registration, TargetCapabilities,
    };
    use makima_domain::TokenUsage as StoredTokenUsage;
    use makima_providers::provider::Provider;
    use makima_providers::{Message, TokenUsage};
    use makima_storage::id::MakimaId;
    use makima_storage::session_state::{
        SessionStateView, StateChange, StoredAgentLifecycle, StoredAgentRecord,
    };
    use mlua::Lua;
    use smol::Timer;
    use std::sync::Arc;
    use test_case::test_case;

    use crate::api::agent::handles::tests::{REPLY, RETIRE_TIMEOUT};
    use crate::api::agent::tests::{
        StreamOnceProvider, canned_reply_with_usage, session_with_provider,
        session_with_provider_for_agent,
    };
    use crate::api::agent::{LuaActorBackend, create_agent_table};
    use crate::api::util::ctx::LuaCtx;
    use crate::orchestration::{
        OrchestrationServices, OrchestrationServicesSlot, PinnedCommandTarget, TrustedTarget,
        command_runner, install_plugin_authority, revoke_plugin,
    };
    use crate::runtime::TaskScope;

    const QUEUED_COMPACT_COMMAND: &str = "/queued-compact";
    const COMPACT_INSTRUCTIONS: &str = "retain the authority boundary";

    struct InactiveBackend;

    impl ActorBackend for InactiveBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("authority test must not execute a turn") })
        }

        fn run_control<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("authority test must not execute a control") })
        }

        fn run_compact<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("authority test must not compact") })
        }
    }

    fn inactive_target() -> (TrustedTarget, EffectiveAgentConfig) {
        let template = stub_ctx(&AgentMode::Build);
        let config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::clone(&template.provider),
                model: (*template.model).clone(),
                thinking: template.opts.thinking,
                fast: template.opts.fast,
                workflow: false,
            },
            AgentMode::Build,
        )
        .with_mode_def(Some(template.modes.current(&AgentMode::Build)));
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let target = manager
            .create_root_with_config(
                Some(config.clone()),
                vec![Message::user(COMPACT_INSTRUCTIONS.into())],
                None,
                |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(InactiveBackend)),
            )
            .unwrap();
        (TrustedTarget { target, template }, config)
    }

    struct GatedCompact {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl CommandBehavior for GatedCompact {
        fn execute(
            &self,
            invocation: CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            assert!(
                invocation
                    .admission_context::<PinnedCommandTarget>()
                    .is_some()
            );
            let entered = self.entered.clone();
            let release = self.release.clone();
            Box::pin(async move {
                entered.send(()).unwrap();
                release.recv_async().await.unwrap();
                Ok(CommandOutcome::ManualCompaction(Some(
                    COMPACT_INSTRUCTIONS.into(),
                )))
            })
        }
    }

    struct UnusedCommandHost;

    impl CommandHost for UnusedCommandHost {
        fn request(&self, _: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
            unreachable!("queued command must execute its registered handler")
        }
    }

    #[test]
    fn queued_command_cannot_compact_replacement_but_fresh_pin_can() {
        smol::block_on(async {
            let (target, config) = inactive_target();
            let agent = &target.target;
            let manager = agent.manager();
            let pin = PinnedCommandTarget::pin(target.clone()).unwrap();
            let old = pin.incarnation();
            let registry = CommandRegistry::new();
            let command_target =
                registry.bind_target(TargetCapabilities::NONE, Arc::new(UnusedCommandHost));
            let (entered_tx, entered) = flume::bounded(1);
            let (release, release_rx) = flume::bounded(1);
            let producer = registry.create_producer(ProducerPrecedence::Application);
            producer
                .replace(vec![Registration {
                    spec: CommandSpec {
                        name: Arc::from(QUEUED_COMPACT_COMMAND),
                        aliases: Arc::from([]),
                        arguments: CommandArguments::Raw { required: false },
                        docs: CommandDocs {
                            summary: Arc::from(COMPACT_INSTRUCTIONS),
                            argument_hint: None,
                        },
                        required_capabilities: TargetCapabilities::NONE,
                    },
                    behavior: Arc::new(GatedCompact {
                        entered: entered_tx,
                        release: release_rx,
                    }),
                    argument_completions: Vec::new(),
                }])
                .unwrap();
            let run = command_runner(
                registry,
                command_target,
                Arc::new(|_, outcome, pin| {
                    Box::pin(async move {
                        let CommandOutcome::ManualCompaction(instructions) = outcome else {
                            panic!("queued command must produce manual compaction");
                        };
                        let actor = pin.actor()?;
                        let run_id = actor.reserve_presentation(actor.presentation_id());
                        actor
                            .push_compact(run_id, instructions)
                            .map_err(|error| error.to_string())
                    })
                }),
            );
            let queued = smol::spawn(run(QUEUED_COMPACT_COMMAND.into(), 0, pin));
            race(entered.recv_async(), async {
                Timer::after(RETIRE_TIMEOUT).await;
                panic!("queued command handler did not enter");
            })
            .await
            .unwrap();
            manager.detach_incarnation(&old).unwrap();
            let (_start, start_rx) = flume::bounded(1);
            let replacement = manager
                .ensure_resident_with(agent.id(), |_| async {
                    Ok(ResidentAgent {
                        backend: Box::new(InactiveBackend),
                        config,
                        shared_messages: None,
                        start: Some(start_rx),
                        initialize: None,
                    })
                })
                .await
                .unwrap();
            let actor = replacement.actor().unwrap();
            let history = actor.committed_history();
            let presentation = actor.presentation_id();
            assert!(!history.is_empty());
            let before = manager.journal().checkpoint_snapshot().unwrap();
            release.send(()).unwrap();
            let result = race(queued, async {
                Timer::after(RETIRE_TIMEOUT).await;
                panic!("queued command handler did not resume");
            })
            .await;
            assert_eq!(
                result.unwrap_err(),
                ManagerError::StaleIncarnation(agent.id()).to_string()
            );
            replacement.validate_live().unwrap();
            assert!(actor.pending_queue().is_empty());
            assert_eq!(actor.presentation_id(), presentation);
            assert_eq!(actor.committed_history(), history);
            let after = manager.journal().checkpoint_snapshot().unwrap();
            assert_eq!(after.revision, before.revision);
            let before_history = before.histories.get(&agent.id()).unwrap();
            let after_history = after.histories.get(&agent.id()).unwrap();
            assert_eq!(after_history.epoch, before_history.epoch);
            assert_eq!(after_history.compacted, before_history.compacted);
            assert_eq!(after_history.messages, before_history.messages);

            let fresh = PinnedCommandTarget::pin(target.clone()).unwrap();
            release.send(()).unwrap();
            race(run(QUEUED_COMPACT_COMMAND.into(), 0, fresh), async {
                Timer::after(RETIRE_TIMEOUT).await;
                panic!("fresh command did not finish");
            })
            .await
            .unwrap();
            let pending = actor.pending_queue();
            let [
                PendingQueueEntry::Compact {
                    operation_id,
                    instructions,
                    ..
                },
            ] = pending.as_slice()
            else {
                panic!("fresh command must queue exactly one compact");
            };
            assert_eq!(instructions.as_deref(), Some(COMPACT_INSTRUCTIONS));
            assert!(actor.remove_pending_compact(*operation_id).is_some());
            assert!(manager.shutdown(RETIRE_TIMEOUT).await.timed_out.is_empty());
        });
    }

    #[test]
    fn suspended_foreign_plugin_callback_cannot_close_replacement_but_fresh_invocation_can() {
        const OWNER: &str = "agent-owner";
        const OBSERVER: &str = "independent-observer";
        smol::block_on(async {
            let (target, config) = inactive_target();
            let agent = &target.target;
            let manager = agent.manager();
            let old = agent.incarnation().unwrap();
            let lua = Lua::new();
            lua.set_app_data(OrchestrationServicesSlot(Some(Arc::new(NoResidentTargets))));
            install_plugin_authority(&lua, Arc::from(OWNER));
            let observer = install_plugin_authority(&lua, Arc::from(OBSERVER));
            let scope = TaskScope::detached(&lua);
            let ctx = LuaCtx::trusted_context(&lua, target.clone(), Arc::from(OBSERVER));
            lua.globals().set("ctx", ctx).unwrap();
            lua.globals()
                .set("agent_api", create_agent_table(&lua).unwrap())
                .unwrap();
            lua.globals()
                .set(
                    "expected_error",
                    ManagerError::StaleIncarnation(agent.id()).to_string(),
                )
                .unwrap();
            let (release, wait) = flume::bounded(1);
            lua.globals()
                .set(
                    "wait_for_replacement",
                    lua.create_async_function(move |_, ()| {
                        let wait = wait.clone();
                        async move { wait.recv_async().await.map_err(mlua::Error::external) }
                    })
                    .unwrap(),
                )
                .unwrap();
            let mut callback = Box::pin(
                lua.load(
                    r#"
                durable = assert(agent_api.get(ctx, assert(agent_api.root(ctx))))
                wait_for_replacement()
                local closed, err = durable:close(ctx)
                assert(closed == nil and err == expected_error, err)
            "#,
                )
                .exec_async(),
            );
            assert!(
                futures_lite::future::poll_once(callback.as_mut())
                    .await
                    .is_none()
            );
            revoke_plugin(&lua, OWNER);
            assert!(observer.is_alive());
            manager.detach_incarnation(&old).unwrap();
            let replacement = manager
                .ensure_resident_with(agent.id(), |_| async {
                    Ok(ResidentAgent {
                        backend: Box::new(InactiveBackend),
                        config,
                        shared_messages: None,
                        start: None,
                        initialize: None,
                    })
                })
                .await
                .unwrap();
            let before = manager.journal().checkpoint_snapshot().unwrap();
            release.send(()).unwrap();
            race(callback, async {
                Timer::after(RETIRE_TIMEOUT).await;
                panic!("suspended callback did not resume");
            })
            .await
            .unwrap();
            replacement.validate_live().unwrap();
            assert_eq!(
                manager.journal().checkpoint_snapshot().unwrap().revision,
                before.revision
            );
            drop(scope);
            let _fresh = TaskScope::detached(&lua);
            let ctx = LuaCtx::trusted_context(&lua, target.clone(), Arc::from(OBSERVER));
            lua.globals().set("ctx", ctx).unwrap();
            lua.load("assert(durable:close(ctx))").exec().unwrap();
            assert!(!manager.is_live(agent.id()));
            assert!(manager.shutdown(RETIRE_TIMEOUT).await.timed_out.is_empty());
        });
    }

    struct NoResidentTargets;

    impl OrchestrationServices for NoResidentTargets {
        fn resolve_target(
            &self,
            _: Option<&makima_commands::CommandInvocation>,
        ) -> Result<Option<TrustedTarget>, String> {
            Ok(None)
        }
    }

    #[test_case(StoredAgentLifecycle::Open, "idle"; "dormant")]
    #[test_case(StoredAgentLifecycle::Closed, "closed"; "closed")]
    #[test_case(StoredAgentLifecycle::Retired, "closed"; "retired")]
    fn dormant_handles_resolve_without_runtime_capabilities(
        lifecycle: StoredAgentLifecycle,
        status: &str,
    ) {
        const TRANSCRIPT_KEY: &str = "retained-child";
        const INPUT_TOKENS: u32 = 13;
        const OUTPUT_TOKENS: u32 = 7;
        let root = AgentId::generate();
        let child = AgentId::generate();
        let turn = TurnId::generate();
        let usage = TokenUsage {
            input: INPUT_TOKENS,
            output: OUTPUT_TOKENS,
            ..Default::default()
        };
        let record = |id, parent, depth, transcript_key: &str, lifecycle| StoredAgentRecord {
            id,
            parent,
            depth,
            transcript_key: transcript_key.to_owned(),
            lifecycle,
            ancestry_unknown: false,
            label: None,
            spawned_by_tool_use_id: None,
            spec: None,
            restorable: Ok(()),
            usage: StoredTokenUsage {
                input: usage.input,
                output: usage.output,
                cache_creation: usage.cache_creation,
                cache_read: usage.cache_read,
                cost: None,
            },
            created_seq: 0,
            updated_seq: 0,
            legacy: false,
        };
        let result = TurnResult {
            outcome: TurnOutcome::cancelled(
                child,
                turn,
                usage,
                1,
                TurnCancellationReason::Interrupted,
            ),
            text: REPLY.to_owned(),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        };
        let journal = Arc::new(SessionJournal::default());
        journal
            .commit(JournalCommit {
                agents: vec![
                    record(root, None, 0, "", StoredAgentLifecycle::Open),
                    record(child, Some(root), 1, TRANSCRIPT_KEY, lifecycle),
                ],
                changes: vec![JournalChange {
                    agent: Some(child),
                    generation: 0,
                    change: StateChange::Turn {
                        turn,
                        result: result.to_stored(MakimaId::generate(), None).unwrap(),
                    },
                }],
                histories: [
                    (root, HistorySnapshot::new(Vec::new())),
                    (child, HistorySnapshot::new(Vec::new())),
                ]
                .into(),
                ..Default::default()
            })
            .unwrap();
        let snapshot = journal.checkpoint_snapshot().unwrap();
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .unwrap();
        let manager = AgentManagerHandle::restore_graph(
            AgentLimits::default(),
            journal,
            view,
            snapshot.histories,
        )
        .unwrap();
        let lua = Lua::new();
        lua.set_app_data(OrchestrationServicesSlot(Some(Arc::new(NoResidentTargets))));
        install_plugin_authority(&lua, Arc::from(TRANSCRIPT_KEY));
        let _scope = TaskScope::detached(&lua);
        let ctx = LuaCtx::trusted_context(
            &lua,
            TrustedTarget {
                target: manager.reference(root),
                template: stub_ctx(&AgentMode::Build),
            },
            Arc::from(TRANSCRIPT_KEY),
        );
        lua.globals().set("ctx", ctx).unwrap();
        lua.globals()
            .set("agent_api", create_agent_table(&lua).unwrap())
            .unwrap();
        lua.globals().set("child_id", child.to_string()).unwrap();
        lua.globals().set("turn_id", turn.to_string()).unwrap();
        lua.globals().set("expected_status", status).unwrap();
        smol::block_on(
            lua.load(format!(
                r#"
            local child = assert(agent_api.get(ctx, child_id))
            assert(child:id() == child_id)
            assert(child:_makima_session_id() == {TRANSCRIPT_KEY:?})
            assert(not child:is_live())
            local status = assert(child:status(ctx))
            assert(not status.live and status.status == expected_status)
            assert(status.input_tokens == {INPUT_TOKENS})
            assert(status.output_tokens == {OUTPUT_TOKENS})
            assert(status.active_turn == nil and status.queued == 0)
            assert(assert(agent_api.get(ctx, assert(child:ref()))):id() == child_id)
            assert(#assert(agent_api.list(ctx)) == 2)
            assert(assert(agent_api.root(ctx)):id() ~= child_id)
            local ticket = assert(child:turn(ctx, turn_id))
            assert(ticket:id() == turn_id and ticket:agent_id() == child_id)
            assert(assert(ticket:result(ctx)).text == {REPLY:?})
            assert(assert(ticket:wait(ctx, {{ timeout = 0 }})).text == {REPLY:?})
        "#
            ))
            .exec_async(),
        )
        .unwrap();
        assert!(manager.actor(child).is_err());
    }

    struct SettlementGate {
        backend: LuaActorBackend,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl ActorBackend for SettlementGate {
        fn run_turn<'a>(
            &'a mut self,
            history: ActorHistory<'a>,
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
            history: ActorHistory<'a>,
            context: TurnContext,
            control: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            self.backend.run_control(history, context, control)
        }

        fn run_compact<'a>(
            &'a mut self,
            history: ActorHistory<'a>,
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
            assert_eq!(target.snapshot().unwrap().agent_id, id);
            assert_eq!(manager.lookup(id).unwrap().id(), id);
            assert!(!manager.is_live(id));
            manager.shutdown(RETIRE_TIMEOUT).await;
        });
    }
}
