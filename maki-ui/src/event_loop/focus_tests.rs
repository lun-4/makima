use std::sync::Arc;

use maki_agent::actor::{ConfigChange, ConfigPatch, PendingQueueEntry, PreparedModel};
use maki_agent::{AgentError, ThinkingConfig};
use maki_commands::{CommandError, CommandOutcome, HostContextRequest, HostRequest};
use maki_config::SessionDefaults;
use maki_providers::model::Model;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    Effort, Message, ModelInfo, ProviderEvent, RequestOptions, StopReason, StreamResponse,
    TokenUsage,
};
use maki_storage::id::SessionRef;
use maki_storage::sessions::{Effort as StoredEffort, StoredThinking};
use test_case::test_case;

use super::tests::{select_child_chat, with_event_loop};
use super::{
    AppSession, CLOSED_TARGET_ERR, CommandEvent, EventLoop, PreparedProvider, SESSION_OP_TIMEOUT,
    SessionRequest, UiAction,
};
use crate::app::shell::ShellEvent;
use crate::chat::Chat;
use crate::components::Action;

const RESTORED_CHAT: &str = "restored task";
const PROMPT: &str = "session prompt";
const SHELL_OUTPUT: &str = "shell output";
const EXPANDER_PLUGIN: &str = "session_prompt_expanders";
const EXPANDER_FAILURE: &str = "reference expansion rejected";
const EXPANDED_PROMPT: &str = "expanded task";
const OVERSIZED_PROMPT_BYTES: usize = 300 * 1024;

fn install_expanders(event_loop: &mut EventLoop<'_>) -> maki_lua::PluginHost {
    let host = maki_lua::PluginHost::with_session_provider_preparer(
        Arc::clone(maki_agent::tools::ToolRegistry::global_arc()),
        event_loop.ctx.command_runtime.registry.clone(),
        false,
        Arc::new(maki_lua::test_support::InMemoryFs::new()),
        Some(event_loop.ctx.storage.path().to_path_buf()),
        None,
        maki_lua::ui_sink::HostUi::Frontend,
    )
    .unwrap();
    host.load_source(
        EXPANDER_PLUGIN,
        &format!(
            r#"
        maki.api.register_expander("ok", function(_) return "{EXPANDED_PROMPT}", nil end)
        maki.api.register_expander("reject", function(_) return nil, "{EXPANDER_FAILURE}" end)
    "#
        ),
    )
    .unwrap();
    event_loop.ctx.lua_event_handle = host.event_handle();
    event_loop.sessions[0].app.lua_event_handle = host.event_handle();
    host
}

fn select_restored_chat(event_loop: &mut EventLoop<'_>) {
    let runtime = Arc::clone(&event_loop.ctx.command_runtime);
    let app = &mut event_loop.sessions[0].app;
    app.command_runtime = Arc::clone(&runtime);
    app.command_target = runtime.bind_target();
    event_loop.sync_orchestration();
    assert!(
        runtime
            .target_agent(&event_loop.sessions[0].app.command_target)
            .is_some()
    );
    let app = &mut event_loop.sessions[0].app;
    let mut chat = Chat::new(
        RESTORED_CHAT.into(),
        app.ui_config.clone(),
        app.lua_event_handle.clone(),
        Arc::clone(&app.theme_provider),
    );
    chat.subagent_id = Some(RESTORED_CHAT.into());
    app.chats.push(chat);
    app.active_chat = 1;
    event_loop.sync_orchestration();
    assert!(
        runtime
            .target_agent(&event_loop.sessions[0].app.command_target)
            .is_none()
    );
    assert!(event_loop.selected_target(0).is_none());
}

fn dispatch_slash(event_loop: &mut EventLoop<'_>, command: &str) -> CommandOutcome {
    let runtime = Arc::clone(&event_loop.ctx.command_runtime);
    runtime.dispatch_input(&event_loop.sessions[0].app.command_target, command.into());
    loop {
        let event = runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap();
        if let CommandEvent::Outcome { outcome, .. } = event {
            return outcome;
        }
        event_loop.handle_command(event);
    }
}

#[test_case("/compact"; "compact")]
#[test_case("/cd /"; "directory")]
#[test_case("/fast"; "fast")]
#[test_case("/workflow"; "workflow")]
#[test_case("/model"; "model_picker")]
#[test_case("/model anthropic/claude-sonnet-4-6"; "model_selection")]
fn closed_agent_rejects_without_root_fallback(command: &str) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = event_loop.sessions[0].handles.history.load_full();
        let _guard = actor.prepare_idle().unwrap();
        runtime.dispatch_input(&event_loop.sessions[0].app.command_target, command.into());
        loop {
            let event = runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap();
            let finished = matches!(event, CommandEvent::Outcome { .. });
            if let CommandEvent::Outcome { outcome, .. } = &event {
                assert!(matches!(
                    outcome,
                    CommandOutcome::Failed(CommandError::StaleTarget)
                ));
            }
            event_loop.handle_command(event);
            if finished {
                break;
            }
        }
        assert_eq!(
            event_loop.sessions[0].app.status_bar.flash_text(),
            Some(CommandError::StaleTarget.to_string().as_str())
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[0].handles.history.load_full()
        ));
        assert!(actor.pending_queue().is_empty());
    });
}

#[test]
fn live_compact_commands_preserve_guidance_and_busy_order() {
    const FIRST: &str = "retain implementation details";
    const SECOND: &str = "retain test results";
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        event_loop.sessions[0].app.active_chat = 0;
        event_loop.sync_orchestration();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let _guard = actor.prepare_idle().unwrap();
        assert!(matches!(
            dispatch_slash(event_loop, &format!("/compact {FIRST}")),
            CommandOutcome::Completed
        ));
        assert!(matches!(
            dispatch_slash(event_loop, &format!("/compact {SECOND}")),
            CommandOutcome::Completed
        ));
        let pending = actor.pending_queue();
        assert_eq!(pending.len(), 2);
        let [
            PendingQueueEntry::Compact {
                operation_id: first,
                instructions: Some(first_text),
                ..
            },
            PendingQueueEntry::Compact {
                operation_id: second,
                instructions: Some(second_text),
                ..
            },
        ] = pending.as_slice()
        else {
            panic!("expected two queued compacts");
        };
        assert_eq!(first_text, FIRST);
        assert_eq!(second_text, SECOND);
        assert_ne!(first, second);
        assert!(actor.remove_pending_compact(*second).is_some());
        assert!(
            matches!(actor.pending_queue().as_slice(), [PendingQueueEntry::Compact { instructions: Some(text), .. }] if text == FIRST)
        );
    });
}

#[test_case("/fast"; "fast")]
#[test_case("/workflow"; "workflow")]
#[test_case("/cd /"; "directory")]
fn live_slash_controls_commit_on_the_admitted_actor(command: &str) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        event_loop.sessions[0].app.active_chat = 0;
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        if command == "/fast" {
            let config = actor.effective_config().unwrap();
            let mut model = config.model.clone();
            model.supports_fast_override = Some(maki_providers::model::FastSupport::Supported);
            let ticket = actor.reserve_prepared_operation(None).unwrap();
            smol::block_on(
                ticket.commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        model,
                        provider: Arc::clone(&config.settings.provider),
                    }),
                    ..Default::default()
                }))),
            )
            .unwrap();
        }
        event_loop.sync_orchestration();
        let previous = actor.effective_config().unwrap();
        let commits = actor.subscribe_config_commits();
        assert!(matches!(
            dispatch_slash(event_loop, command),
            CommandOutcome::Completed
        ));
        let commit = commits.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
        match command {
            "/fast" => assert_eq!(commit.config.fast, !previous.fast),
            "/workflow" => assert_eq!(commit.config.workflow, !previous.workflow),
            _ => assert_eq!(commit.config.context.cwd(), std::path::Path::new("/")),
        }
    });
}

#[test_case(HostContextRequest::WorkingDirectory; "directory")]
#[test_case(HostContextRequest::FastModeSupported; "fast")]
#[test_case(HostContextRequest::ModelSpecs; "models")]
fn restored_context_queries_reject_missing_admission(request: HostContextRequest) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let (reply, response) = flume::bounded(1);
        event_loop.handle_command(CommandEvent::Host {
            target: event_loop.sessions[0].app.command_target.id(),
            request: HostRequest::Context(request),
            admission: None,
            reply,
        });
        assert!(matches!(
            response.recv().unwrap(),
            Err(CommandError::StaleTarget)
        ));
    });
}

#[test_case(Action::Compact(None); "compact")]
#[test_case(Action::ChangeDirectory("/".into()); "directory")]
#[test_case(Action::ChangeMode("build".into()); "mode")]
fn restored_actions_report_closed_target(action: Action) {
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let asynchronous = matches!(action, Action::ChangeMode(_));
        event_loop.handle_action(0, action);
        if asynchronous {
            let event = event_loop
                .internal_rx
                .recv_timeout(SESSION_OP_TIMEOUT)
                .unwrap();
            event_loop.handle_internal(event);
        }
        assert!(
            event_loop.sessions[0]
                .app
                .status_bar
                .flash_text()
                .unwrap()
                .contains(CLOSED_TARGET_ERR)
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(actor.pending_queue().is_empty());
    });
}

#[test_case(false; "agent_turn")]
#[test_case(true; "isolated_turn")]
fn restored_custom_turn_commands_have_no_root_authority(isolated: bool) {
    struct CustomTurn(bool);
    impl maki_commands::CommandBehavior for CustomTurn {
        fn execute(
            &self,
            _: maki_commands::CommandInvocation,
        ) -> maki_commands::CommandFuture<Result<CommandOutcome, CommandError>> {
            let content = PROMPT.into();
            let outcome = if self.0 {
                CommandOutcome::IsolatedTurn(maki_commands::IsolatedTurn { content })
            } else {
                CommandOutcome::AgentTurn(maki_commands::AgentTurn {
                    content,
                    prompt: None,
                })
            };
            Box::pin(async move { Ok(outcome) })
        }
    }
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let producer = runtime
            .registry
            .create_producer(maki_commands::ProducerPrecedence::Application);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/restored-turn"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Raw { required: false },
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from(PROMPT),
                        argument_hint: None,
                    },
                    required_capabilities: maki_commands::TargetCapabilities::NONE,
                },
                behavior: Arc::new(CustomTurn(isolated)),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let history = event_loop.sessions[0].handles.history.load_full();
        let (manager, _) = event_loop.sessions[0].handles.manager_and_root();
        let actors = manager.snapshot().len();
        runtime.dispatch_input(
            &event_loop.sessions[0].app.command_target,
            "/restored-turn".into(),
        );
        event_loop.handle_command(runtime.recv_timeout_for_test(SESSION_OP_TIMEOUT).unwrap());
        assert_eq!(
            event_loop.sessions[0].app.status_bar.flash_text(),
            Some(CommandError::StaleTarget.to_string().as_str())
        );
        assert!(event_loop.sessions[0].handles.queue.is_empty());
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[0].handles.history.load_full()
        ));
        assert_eq!(manager.snapshot().len(), actors);
    });
}

#[test_case(true; "restored_child")]
#[test_case(false; "live_child_pinned_across_focus_switch")]
fn registered_lua_mode_set_uses_its_invocation_agent(restored: bool) {
    const SPAWNED: &str = "mode child spawned";
    const DONE: &str = "public mode setter completed";
    with_event_loop(|event_loop| {
        select_restored_chat(event_loop);
        let host = install_expanders(event_loop);
        host.event_handle()
            .install_orchestration_services(event_loop.orchestration.clone());
        host.load_source(
            "public_mode_command",
            &format!(
                r#"
            maki.api.register_command({{
                name = "/spawn-mode-child", tui_only = false,
                handler = function(_, ctx)
                    assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true }}))
                    maki.ui.flash("{SPAWNED}")
                end,
            }})
            maki.api.register_command({{
                name = "/set-public-mode", tui_only = false,
                handler = function(_, ctx)
                    assert((ctx ~= nil) == {live})
                    if ctx then
                        local agent = assert(ctx:agent())
                        assert(agent:set_mode(ctx, "plan"))
                    end
                    maki.ui.flash("{DONE}")
                end,
            }})
        "#,
                live = !restored
            ),
        )
        .unwrap();
        let command_runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let root_actor = manager.actor(root).unwrap();
        let original = root_actor.effective_config().unwrap();
        let child = if restored {
            None
        } else {
            event_loop.sessions[0].app.active_chat = 0;
            event_loop.sync_orchestration();
            smol::block_on(command_runtime.registry.dispatch_input(
                &event_loop.sessions[0].app.command_target,
                "/spawn-mode-child".into(),
            ));
            assert!(
                matches!(host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(), UiAction::Flash(text) if text == SPAWNED)
            );
            let child = manager
                .snapshot()
                .into_iter()
                .find(|node| node.parent_id == Some(root))
                .unwrap()
                .agent_id;
            select_child_chat(event_loop, child, RESTORED_CHAT);
            Some(child)
        };
        let registry = command_runtime.registry.clone();
        let target = event_loop.sessions[0].app.command_target.clone();
        let command = smol::spawn(async move {
            registry
                .dispatch_input(&target, "/set-public-mode".into())
                .await
        });
        assert!(
            matches!(host.ui_action_rx().recv_timeout(SESSION_OP_TIMEOUT).unwrap(), UiAction::Flash(text) if text == DONE)
        );
        smol::block_on(command);
        assert!(Arc::ptr_eq(
            &original,
            &root_actor.effective_config().unwrap()
        ));
        if let Some(child) = child {
            assert!(
                manager
                    .actor(child)
                    .unwrap()
                    .effective_config()
                    .unwrap()
                    .mode
                    .plan_path()
                    .is_some()
            );
        }
    });
}

#[test_case("   "; "blank")]
#[test_case("@reject:task"; "expander_failure")]
#[test_case("oversized"; "oversized")]
fn rejected_new_prompt_never_publishes_a_runtime(prompt: &str) {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let original = event_loop.sessions[0].id();
        let prompt = if prompt == "oversized" {
            "x".repeat(OVERSIZED_PROMPT_BYTES)
        } else {
            prompt.into()
        };
        let expected = match prompt.as_str() {
            "   " => crate::app::EMPTY_PROMPT_ERR.to_owned(),
            "@reject:task" => EXPANDER_FAILURE.to_owned(),
            _ => maki_agent::actor::ActorError::InstructionOverflow.to_string(),
        };
        let (reply, response) = flume::bounded(1);
        event_loop.handle_session_request(
            SessionRequest::New {
                prompt: Some(prompt),
                focus: true,
            },
            reply,
        );
        let error = response
            .recv_timeout(SESSION_OP_TIMEOUT)
            .unwrap()
            .unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(event_loop.sessions.len(), 1);
        assert_eq!(event_loop.sessions[event_loop.focused].id(), original);
    });
}

#[test]
fn session_prompt_expands_once_and_busy_submission_is_queued() {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let _guard = actor.prepare_idle().unwrap();
        assert_eq!(
            event_loop.submit_text(0, "@ok:task".into()).unwrap(),
            serde_json::json!("started")
        );
        assert_eq!(actor.pending_submissions()[0].text, EXPANDED_PROMPT);
        assert_eq!(
            event_loop.submit_text(0, PROMPT.into()).unwrap(),
            serde_json::json!("queued")
        );
        assert_eq!(actor.pending_submissions().len(), 2);
    });
}

#[test_case(false; "expander_failure")]
#[test_case(true; "admission_failure")]
fn rejected_session_prompt_preserves_shell_context(oversized: bool) {
    with_event_loop(|event_loop| {
        let _host = install_expanders(event_loop);
        let id = event_loop.sessions[0].app.shell.reserve_for(None);
        event_loop.sessions[0]
            .app
            .handle_shell_event(ShellEvent::Done {
                id,
                command: PROMPT.into(),
                output: SHELL_OUTPUT.into(),
                is_error: false,
                visible: true,
            });
        let prompt = if oversized {
            "x".repeat(OVERSIZED_PROMPT_BYTES)
        } else {
            "@reject:task".into()
        };
        let error = event_loop.submit_text(0, prompt).unwrap_err();
        if !oversized {
            assert_eq!(error, EXPANDER_FAILURE);
        }
        assert!(
            !event_loop.sessions[0]
                .app
                .shell
                .results_for(None)
                .is_empty()
        );
        assert!(event_loop.sessions[0].handles.queue.is_empty());
    });
}

const GLM_4: &str = "ollama/glm-4";
const GLM_5: &str = "ollama/glm-5";
const OTHER_PROVIDER: &str = "zai/glm-5";
const UNREACHABLE_OLLAMA_HOST: &str = "http://127.0.0.1:1";

#[test_case(&[GLM_5, OTHER_PROVIDER], "ollama", Ok(GLM_5) ; "provider_section_switches")]
#[test_case(&[GLM_5], "glm", Ok(GLM_5) ; "unique_fragment_switches")]
#[test_case(&[GLM_4, GLM_5], "glm", Err("command failed: ambiguous model: glm") ; "ambiguous_fragment_fails")]
#[test_case(&[GLM_5], "xyz", Err("command failed: no model matches: xyz") ; "unknown_fragment_fails")]
fn model_argument_resolves_against_the_catalog(
    catalog: &[&str],
    argument: &str,
    expected: Result<&str, &str>,
) {
    // SAFETY: nextest runs each test in its own process.
    unsafe { std::env::set_var("OLLAMA_HOST", UNREACHABLE_OLLAMA_HOST) };
    with_event_loop(|event_loop| {
        event_loop.ctx.catalog.specs().store(Some(Arc::new(
            catalog.iter().map(|spec| spec.to_string()).collect(),
        )));
        event_loop.sync_orchestration();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let commits = actor.subscribe_config_commits();
        let outcome = dispatch_slash(event_loop, &format!("/model {argument}"));
        match expected {
            Ok(spec) => {
                assert!(matches!(outcome, CommandOutcome::Completed), "{outcome:?}");
                let commit = commits.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
                assert_eq!(commit.config.model.spec(), spec);
            }
            Err(message) => {
                let CommandOutcome::Failed(error) = outcome else {
                    panic!("expected a failure, got {outcome:?}");
                };
                assert_eq!(error.to_string(), message);
            }
        }
    });
}

const THINKING_MODEL: &str = "anthropic/claude-opus-4-6";
const PLAN_HEADING: &str = "# Plan Mode";
const PLAN_COMMAND_PLUGIN: &str = "plan_mode_command";

/// Records the system prompt and options of every request, then ends the turn.
struct RecordingProvider(flume::Sender<(String, RequestOptions)>);

impl Provider for RecordingProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [Message],
        system: &'a str,
        _: &'a serde_json::Value,
        _: &'a flume::Sender<ProviderEvent>,
        options: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        let _ = self.0.send((system.to_owned(), options));
        Box::pin(async {
            Ok(StreamResponse {
                message: Message::user(String::new()),
                usage: TokenUsage::default(),
                stop_reason: Some(StopReason::EndTurn),
            })
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn recording_provider() -> (Arc<dyn Provider>, flume::Receiver<(String, RequestOptions)>) {
    let (requests, recorded) = flume::unbounded();
    (Arc::new(RecordingProvider(requests)), recorded)
}

/// The TUI's half of `tests/frontend_parity.rs`: a blank session sends
/// `always_thinking` like every headless frontend does.
#[test]
fn blank_session_sends_always_thinking_to_the_provider() {
    with_event_loop(|event_loop| {
        event_loop.ctx.session_defaults = SessionDefaults {
            thinking: Some(StoredThinking::Effort {
                level: StoredEffort::High,
            }),
            ..SessionDefaults::default()
        };
        let (provider, requests) = recording_provider();
        let model = Model::from_spec(THINKING_MODEL).unwrap();
        let runtime = event_loop
            .ctx
            .spawn_runtime_with_provider(
                AppSession::new(THINKING_MODEL, &event_loop.session_cwd),
                Some(PreparedProvider {
                    model: model.clone(),
                    provider,
                }),
            )
            .unwrap();
        let index = event_loop.push_runtime(runtime);
        event_loop.submit_text(index, PROMPT.into()).unwrap();
        let (_, options) = requests.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
        assert_eq!(
            options,
            RequestOptions {
                thinking: ThinkingConfig::Effort(Effort::High),
                fast: false,
            }
            .clamped(&model)
        );
    });
}

/// The TUI's half of `tests/frontend_parity.rs`: a Lua command that sets
/// plan mode changes what the next turn sends.
#[test]
fn lua_mode_command_reaches_the_provider() {
    with_event_loop(|event_loop| {
        let runtime = Arc::clone(&event_loop.ctx.command_runtime);
        let app = &mut event_loop.sessions[0].app;
        app.command_runtime = Arc::clone(&runtime);
        app.command_target = runtime.bind_target();
        let host = install_expanders(event_loop);
        host.event_handle()
            .install_orchestration_services(event_loop.orchestration.clone());
        host.load_source(
            PLAN_COMMAND_PLUGIN,
            r#"
            maki.api.register_command({
                name = "/plan-now", tui_only = false,
                handler = function(_, ctx)
                    assert(assert(ctx:agent()):set_mode(ctx, "plan"))
                end,
            })
        "#,
        )
        .unwrap();
        event_loop.sync_orchestration();
        let outcome = dispatch_slash(event_loop, "/plan-now");
        assert!(matches!(outcome, CommandOutcome::Completed), "{outcome:?}");

        let (provider, requests) = recording_provider();
        let (manager, root) = event_loop.sessions[0].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let model = actor.effective_config().unwrap().model.clone();
        smol::block_on(
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel { model, provider }),
                    ..ConfigPatch::default()
                }))),
        )
        .unwrap();
        event_loop.submit_text(0, PROMPT.into()).unwrap();
        let (system, _) = requests.recv_timeout(SESSION_OP_TIMEOUT).unwrap();
        assert!(system.contains(PLAN_HEADING), "{system}");
    });
}
