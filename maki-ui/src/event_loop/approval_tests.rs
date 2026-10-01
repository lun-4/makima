use crossterm::event::{KeyCode, KeyEvent};
use maki_agent::actor::PreparedModel;
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::provider::BoxFuture as ApprovalFuture;
use maki_providers::{
    ContentBlock as ApprovalContentBlock, RequestOptions as ApprovalOptions, Role as ApprovalRole,
    StopReason as ApprovalStopReason, StreamResponse as ApprovalResponse,
};
use maki_storage::checkpoint::{CheckpointRequest, CheckpointVersion, CheckpointWriter};
#[cfg(unix)]
use std::io::Write;
use std::path::Path;
#[cfg(unix)]
use std::process::Command;
use std::sync::atomic::AtomicUsize;

const APPROVAL_SOURCE_MODEL: &str = "anthropic/claude-opus-4-6";
const APPROVAL_TARGET_MODEL: &str = "openai/gpt-5";
const APPROVAL_HISTORY: &str = "existing planning conversation";
const APPROVAL_PLAN: &str = "# Implementation\n\nImplement the regression tests.\n";
const APPROVAL_RESPONSE: &str = "implementation finished";
const APPROVAL_PROVIDER_FAILURE: &str = "approval provider preparation failed";
const APPROVAL_WAIT: Duration = Duration::from_secs(10);
const APPROVAL_CHILD_PROMPT: &str = "gated managed child";
const APPROVAL_RELEASED_LOCK: &str = "released";
const APPROVAL_CHILD_TOOL: &str = r#"
    local sessions = {}
    maki.api.register_tool({
        name = "approval_child", description = "start test child", kind = "read",
        schema = { type = "object", properties = {} },
        audiences = { "main" },
        handler = function(_, ctx)
            local child, err = maki.agent.session(ctx, { name = "approval child", system = "child", tools = maki.json.decode('[]'), inherit_provider = true, auto_deliver = false })
            if err then return { llm_output = err, is_error = true } end
            sessions[#sessions + 1] = child
            local ok, send_err = child:send("gated managed child")
            if send_err then return { llm_output = send_err, is_error = true } end
            return "child started"
        end,
    })
"#;
const APPROVAL_AUTOCMD: &str = r#"
    local starts = 0
    maki.api.create_autocmd("TurnStart", { callback = function()
        starts = starts + 1
        maki.api.register_command({ name = "/approval-start-" .. tostring(starts), description = "observed", tui_only = false, handler = function() end })
    end })
    local modes = 0
    maki.api.create_autocmd("ModeChanged", { callback = function()
        modes = modes + 1
        maki.api.register_command({ name = "/approval-mode-" .. tostring(modes), description = "observed", tui_only = false, handler = function() end })
    end })
"#;

type ApprovalRequest = (String, ApprovalOptions, Vec<Message>, String);

struct ApprovalRecordingProvider {
    requests: flume::Sender<ApprovalRequest>,
    release: Option<flume::Receiver<()>>,
}

impl Provider for ApprovalRecordingProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        _tools: &'a serde_json::Value,
        _events: &'a flume::Sender<maki_providers::ProviderEvent>,
        options: ApprovalOptions,
        _session: Option<&'a SessionRef>,
    ) -> ApprovalFuture<'a, Result<ApprovalResponse, maki_providers::AgentError>> {
        Box::pin(async move {
            self.requests
                .send((model.spec(), options, messages.to_vec(), system.into()))
                .unwrap();
            if let Some(release) = &self.release {
                release.recv_async().await.unwrap();
            }
            Ok(ApprovalResponse {
                message: Message {
                    role: ApprovalRole::Assistant,
                    content: vec![ApprovalContentBlock::Text {
                        text: APPROVAL_RESPONSE.into(),
                    }],
                    ..Message::user(String::new())
                },
                usage: TokenUsage::default(),
                stop_reason: Some(ApprovalStopReason::EndTurn),
            })
        })
    }

    fn list_models(
        &self,
    ) -> ApprovalFuture<'_, Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>>
    {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct ApprovalChildProvider {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
    root_requests: AtomicUsize,
    requests: flume::Sender<Vec<Message>>,
}

impl Provider for ApprovalChildProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        messages: &'a [Message],
        _system: &'a str,
        _tools: &'a serde_json::Value,
        _events: &'a flume::Sender<maki_providers::ProviderEvent>,
        _options: ApprovalOptions,
        _session: Option<&'a SessionRef>,
    ) -> ApprovalFuture<'a, Result<ApprovalResponse, maki_providers::AgentError>> {
        Box::pin(async move {
            self.requests.send(messages.to_vec()).unwrap();
            let child = messages
                .iter()
                .any(|message| message.user_text() == Some(APPROVAL_CHILD_PROMPT));
            let content = if child {
                self.entered.send(()).unwrap();
                self.release.recv_async().await.unwrap();
                ApprovalContentBlock::Text {
                    text: APPROVAL_RESPONSE.into(),
                }
            } else if self.root_requests.fetch_add(1, Ordering::SeqCst) == 0 {
                ApprovalContentBlock::ToolUse {
                    id: "approval-child-call".into(),
                    name: "approval_child".into(),
                    input: serde_json::json!({}),
                    thought_signature: None,
                }
            } else {
                ApprovalContentBlock::Text {
                    text: APPROVAL_RESPONSE.into(),
                }
            };
            Ok(ApprovalResponse {
                message: Message {
                    role: ApprovalRole::Assistant,
                    content: vec![content],
                    ..Message::user(String::new())
                },
                usage: TokenUsage::default(),
                stop_reason: Some(if child || self.root_requests.load(Ordering::SeqCst) > 1 {
                    ApprovalStopReason::EndTurn
                } else {
                    ApprovalStopReason::ToolUse
                }),
            })
        })
    }

    fn list_models(
        &self,
    ) -> ApprovalFuture<'_, Result<Vec<maki_providers::ModelInfo>, maki_providers::AgentError>>
    {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[test]
fn approval_rejects_active_managed_child_without_cancelling_it() {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) =
            approval_setup_with_registry(event_loop, Arc::clone(ToolRegistry::global_arc()));
        host.load_source("approval-child", APPROVAL_CHILD_TOOL)
            .unwrap();
        event_loop.sessions[index]
            .app
            .permissions
            .set_session_yolo(Some(true));
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let (record, recorded) = flume::unbounded();
        let provider = Arc::new(ApprovalChildProvider {
            entered,
            release: gate,
            root_requests: AtomicUsize::new(0),
            requests: record,
        });
        let setter = actor.reserve_config_update().unwrap();
        setter
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    model: Some(PreparedModel {
                        model: Model::from_spec(APPROVAL_SOURCE_MODEL).unwrap(),
                        provider: provider.clone(),
                    }),
                    ..Default::default()
                },
            )))
            .unwrap();
        smol::block_on(setter.wait()).unwrap();
        event_loop
            .submit_text(index, APPROVAL_HISTORY.into())
            .unwrap();
        approval_pump_until(event_loop, |_| !entry.is_empty());
        assert!(
            !entry.is_empty(),
            "child did not run after {} root requests; history: {}",
            provider.root_requests.load(Ordering::SeqCst),
            serde_json::to_string(&recorded.drain().collect::<Vec<_>>()).unwrap()
        );
        entry.recv_timeout(APPROVAL_WAIT).unwrap();
        approval_pump_until(event_loop, |event_loop| {
            actor.snapshot().status == maki_agent::actor::ActorStatus::Idle
                && event_loop.sessions[index].app.status == Status::Idle
        });
        let children: Vec<_> = manager
            .snapshot()
            .into_iter()
            .filter(|node| node.parent_id == Some(root))
            .collect();
        assert_eq!(children.len(), 1);
        let child = manager.actor(children[0].agent_id).unwrap();
        let child_before = child.snapshot();
        assert!(matches!(
            child_before.status,
            maki_agent::actor::ActorStatus::Running(_)
        ));
        let history = event_loop.sessions[index].coordinator.read().history();
        let recent = maki_storage::model::read_recents(&event_loop.ctx.storage);
        let saved_model = maki_storage::model::read_model(&event_loop.ctx.storage);
        let run = event_loop.sessions[index].app.run_id;
        let config = actor.effective_config().unwrap();
        approval_dispatch(event_loop, index, &path, true);
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert_eq!(event_loop.sessions[index].app.run_id, run);
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert_eq!(child.snapshot().status, child_before.status);
        assert_eq!(child.snapshot().active_turn, child_before.active_turn);
        assert_eq!(child.snapshot().lifecycle, child_before.lifecycle);
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[index].coordinator.read().history()
        ));
        assert_eq!(
            maki_storage::model::read_recents(&event_loop.ctx.storage),
            recent
        );
        assert_eq!(
            maki_storage::model::read_model(&event_loop.ctx.storage),
            saved_model
        );
        assert_eq!(
            event_loop.sessions[index].app.state.mode,
            crate::app::mode::Mode::Plan
        );
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert!(requests.is_empty());
        release.send(()).unwrap();
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
                && child.snapshot().status == maki_agent::actor::ActorStatus::Idle
        });
        assert!(child.snapshot().latest.is_some());
        shutdown_manager(&manager);
    });
}

fn approval_setup(
    event_loop: &mut EventLoop<'_>,
) -> (usize, PathBuf, flume::Receiver<ApprovalRequest>, PluginHost) {
    approval_setup_with_registry(event_loop, Arc::new(ToolRegistry::new()))
}

fn approval_setup_with_registry(
    event_loop: &mut EventLoop<'_>,
    registry: Arc<ToolRegistry>,
) -> (usize, PathBuf, flume::Receiver<ApprovalRequest>, PluginHost) {
    let host = PluginHost::with_command_registry(
        registry,
        event_loop.ctx.command_runtime.registry.clone(),
        false,
    )
    .unwrap();
    host.load_source("approval-observer", APPROVAL_AUTOCMD)
        .unwrap();
    event_loop.ctx.lua_event_handle = host.event_handle();
    let path = PathBuf::from(&event_loop.session_cwd).join("approval-plan.md");
    std::fs::write(&path, APPROVAL_PLAN).unwrap();
    let (requests, receiver) = flume::unbounded();
    let mut session = AppSession::new(APPROVAL_SOURCE_MODEL, &event_loop.session_cwd);
    session.meta.mode = Some(maki_storage::sessions::StoredMode::Plan);
    session.meta.plan_path = Some(path.display().to_string());
    session.meta.plan_written = true;
    session.meta.thinking = Some(DomainThinkingConfig::Off.into());
    session.meta.fast = true;
    session.push_message(Message::user(APPROVAL_HISTORY.into()));
    let runtime = event_loop
        .ctx
        .spawn_runtime_with_provider(
            session,
            Some(PreparedProvider {
                model: Model::from_spec(APPROVAL_SOURCE_MODEL).unwrap(),
                provider: Arc::new(ApprovalRecordingProvider {
                    requests: requests.clone(),
                    release: None,
                }),
            }),
        )
        .unwrap();
    let index = event_loop.sessions.len();
    event_loop.push_runtime(runtime);
    event_loop.sessions[index]
        .app
        .plan_form
        .set_implementation_model(APPROVAL_TARGET_MODEL.into(), APPROVAL_SOURCE_MODEL);
    event_loop.ctx.prepare_provider = Arc::new(move |model, _| {
        Ok(PreparedModel {
            model: model.clone(),
            provider: Arc::new(ApprovalRecordingProvider {
                requests: requests.clone(),
                release: None,
            }),
        })
    });
    (index, path, receiver, host)
}

fn approval_dispatch(event_loop: &mut EventLoop<'_>, index: usize, path: &Path, fresh: bool) {
    if !event_loop.sessions[index].app.plan_form.parallel() {
        event_loop.sessions[index]
            .app
            .plan_form
            .handle_key(KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE));
    }
    event_loop.sessions[index].app.plan_approval_pending = true;
    event_loop.dispatch(
        index,
        vec![Action::ApprovePlan {
            clear_context: fresh,
            model: Some(APPROVAL_TARGET_MODEL.into()),
            parallel: true,
            path: path.to_path_buf(),
        }],
    );
}

fn approval_ready(event_loop: &mut EventLoop<'_>) -> InternalEvent {
    let deadline = Instant::now() + APPROVAL_WAIT;
    loop {
        let event = event_loop
            .internal_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        if matches!(event, InternalEvent::PlanApprovalReady { .. }) {
            return event;
        }
        event_loop.handle_internal(event);
    }
}

fn approval_pump_until(
    event_loop: &mut EventLoop<'_>,
    mut done: impl FnMut(&EventLoop<'_>) -> bool,
) {
    let deadline = Instant::now() + APPROVAL_WAIT;
    loop {
        while let Ok(event) = event_loop.internal_rx.try_recv() {
            event_loop.handle_internal(event);
        }
        for index in 0..event_loop.sessions.len() {
            while let Ok(envelope) = event_loop.sessions[index].handles.agent_rx.try_recv() {
                assert_eq!(envelope.run_id, event_loop.sessions[index].app.run_id);
                event_loop
                    .handle_wake(Wake::Agent(index, Box::new(envelope)))
                    .unwrap();
            }
        }
        let _ = event_loop.tick();
        if done(event_loop) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "approval event processing timed out"
        );
        std::thread::yield_now();
    }
}

fn approval_assert_preserved(event_loop: &EventLoop<'_>, index: usize, path: &Path, run_id: u64) {
    let app = &event_loop.sessions[index].app;
    assert_eq!(app.state.model.spec(), APPROVAL_SOURCE_MODEL);
    assert_eq!(app.state.mode, crate::app::mode::Mode::Plan);
    assert_eq!(app.state.plan.path(), Some(path));
    assert_eq!(
        app.plan_form.implementation_model(),
        Some(APPROVAL_TARGET_MODEL)
    );
    assert_eq!(app.run_id, run_id);
    assert!(
        app.state
            .session
            .messages()
            .iter()
            .any(|message| message.user_text() == Some(APPROVAL_HISTORY))
    );
    assert!(!app.plan_approval_pending);
    assert_eq!(app.state.session.messages().len(), 1);
    smol::block_on(app.lua_event_handle.collect_prompt_slots_async());
    let commands = event_loop
        .ctx
        .command_runtime
        .registry
        .snapshot_for(&app.command_target)
        .unwrap();
    assert!(
        commands
            .commands()
            .iter()
            .all(|command| !command.spec().name.starts_with("/approval-start-"))
    );
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_dispatch_uses_selected_provider_and_persists_completion(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = approval_setup(event_loop);
        let original_id = event_loop.sessions[index].id();
        let original_run = event_loop.sessions[index].app.run_id;
        let prefs =
            serde_json::to_value(maki_storage::sessions::read_prefs(&event_loop.ctx.storage))
                .unwrap();
        let stored_model = maki_storage::model::read_model(&event_loop.ctx.storage);
        let recents = maki_storage::model::read_recents(&event_loop.ctx.storage);
        approval_dispatch(event_loop, index, &path, fresh);
        approval_dispatch(event_loop, index, &path, fresh);
        let ready = approval_ready(event_loop);
        assert!(requests.is_empty());
        assert_eq!(event_loop.sessions[index].id(), original_id);
        assert_eq!(
            maki_storage::model::read_recents(&event_loop.ctx.storage),
            recents
        );
        assert_eq!(
            maki_storage::model::read_model(&event_loop.ctx.storage),
            stored_model
        );
        assert_eq!(
            serde_json::to_value(maki_storage::sessions::read_prefs(&event_loop.ctx.storage))
                .unwrap(),
            prefs
        );
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            APPROVAL_SOURCE_MODEL
        );
        smol::block_on(host.event_handle().collect_prompt_slots_async());
        assert!(
            host.command_registry()
                .snapshot_for(&event_loop.sessions[index].app.command_target)
                .unwrap()
                .commands()
                .iter()
                .all(|command| !command.spec().name.starts_with("/approval-start-"))
        );
        event_loop.handle_internal(ready);
        approval_pump_until(event_loop, |event_loop| {
            let app = &event_loop.sessions[index].app;
            app.run_id == original_run.wrapping_add(1)
                && app.status == Status::Idle
                && !app.plan_approval_pending
        });
        smol::block_on(host.event_handle().collect_prompt_slots_async());
        let commands = host
            .command_registry()
            .snapshot_for(&event_loop.sessions[index].app.command_target)
            .unwrap();
        let starts: Vec<_> = commands
            .commands()
            .iter()
            .filter(|command| command.spec().name.starts_with("/approval-start-"))
            .collect();
        assert_eq!(starts.len(), 1, "TurnStart observations: {commands:?}");
        assert_eq!(starts[0].spec().name.as_ref(), "/approval-start-1");
        assert_eq!(
            maki_storage::model::read_recents(&event_loop.ctx.storage)
                .first()
                .map(String::as_str),
            Some(APPROVAL_TARGET_MODEL)
        );
        assert_eq!(
            maki_storage::model::read_model(&event_loop.ctx.storage).as_deref(),
            Some(APPROVAL_TARGET_MODEL)
        );
        assert_eq!(
            serde_json::to_value(maki_storage::sessions::read_prefs(&event_loop.ctx.storage))
                .unwrap(),
            prefs
        );
        let (model, options, messages, _) = requests.recv_timeout(APPROVAL_WAIT).unwrap();
        assert_eq!(model, APPROVAL_TARGET_MODEL);
        assert_eq!(
            options,
            ApprovalOptions {
                thinking: DomainThinkingConfig::Off,
                fast: true
            }
            .clamped(&Model::from_spec(APPROVAL_TARGET_MODEL).unwrap())
        );
        assert_eq!(
            messages
                .iter()
                .any(|message| message.user_text() == Some(APPROVAL_HISTORY)),
            !fresh
        );
        let instruction = messages
            .iter()
            .filter_map(Message::user_text)
            .next_back()
            .unwrap();
        assert!(instruction.contains(&path.display().to_string()));
        assert!(instruction.contains("batch+task"));
        assert!(requests.is_empty());
        let runtime = &mut event_loop.sessions[index];
        assert_eq!(runtime.id() != original_id, fresh);
        assert_eq!(runtime.app.state.mode, crate::app::mode::Mode::Build);
        assert_eq!(runtime.app.state.model.spec(), APPROVAL_TARGET_MODEL);
        assert_eq!(runtime.app.plan_form.implementation_model(), None);
        let (manager, root) = runtime.handles.manager_and_root();
        assert_eq!(
            manager
                .actor(root)
                .unwrap()
                .effective_config()
                .unwrap()
                .mode,
            maki_agent::AgentMode::Build
        );
        runtime.app.checkpoint_now();
        smol::block_on(event_loop.ctx.storage_writer.checkpoint(CheckpointRequest {
            session_id: runtime.id(),
            version: CheckpointVersion {
                revision: runtime.app.state.session.revision(),
                epoch: 1,
            },
            snapshot: Arc::clone(&runtime.app.state.session),
        }))
        .unwrap();
        let restored = AppSession::load(runtime.id(), &event_loop.ctx.storage).unwrap();
        assert_eq!(restored.model, APPROVAL_TARGET_MODEL);
        assert_eq!(
            restored.meta.mode,
            Some(maki_storage::sessions::StoredMode::Build)
        );
        assert_eq!(restored.meta.thinking, Some(options.thinking.into()));
        assert_eq!(restored.meta.fast, options.fast);
        assert_eq!(
            restored
                .messages()
                .iter()
                .any(|message| message.user_text() == Some(APPROVAL_HISTORY)),
            !fresh
        );
        assert!(restored.messages().iter().any(|message| message.content.iter().any(|block| matches!(block, ApprovalContentBlock::Text { text } if text == APPROVAL_RESPONSE))));
        assert!(requests.is_empty());
    });
}

fn approval_final_ready(
    event_loop: &mut EventLoop<'_>,
    fresh: bool,
    requests: &flume::Receiver<ApprovalRequest>,
) -> InternalEvent {
    loop {
        let ready = approval_ready(event_loop);
        assert!(requests.is_empty());
        match &ready {
            InternalEvent::PlanApprovalReady {
                result: Ok(prepared),
                ..
            } if prepared.content_verified && (!fresh || prepared.candidate.is_some()) => {
                return ready;
            }
            InternalEvent::PlanApprovalReady {
                result: Err(error), ..
            } => panic!("approval preparation failed: {error}"),
            _ => event_loop.handle_internal(ready),
        }
    }
}

fn approval_mode_events(event_loop: &EventLoop<'_>, index: usize) -> usize {
    let app = &event_loop.sessions[index].app;
    smol::block_on(app.lua_event_handle.collect_prompt_slots_async());
    event_loop
        .ctx
        .command_runtime
        .registry
        .snapshot_for(&app.command_target)
        .unwrap()
        .commands()
        .iter()
        .filter(|command| command.spec().name.starts_with("/approval-mode-"))
        .count()
}

#[test]
fn approval_mode_transition_emits_once_and_config_only_emits_none() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let before = approval_mode_events(event_loop, index);
        approval_dispatch(event_loop, index, &path, false);
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == crate::app::mode::Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(APPROVAL_WAIT).unwrap();
        let _ = event_loop.tick();
        assert_eq!(approval_mode_events(event_loop, index), before + 1);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let setter = actor.reserve_config_update().unwrap();
        setter
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    fast: Some(false),
                    ..Default::default()
                },
            )))
            .unwrap();
        let commit = smol::block_on(setter.wait()).unwrap();
        event_loop.sessions[index].project_config(&commit);
        let _ = event_loop.tick();
        assert_eq!(approval_mode_events(event_loop, index), before + 1);
    });
}

#[test]
fn approval_fresh_forgets_retired_cache_but_preserves_saved_source() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let source = event_loop.sessions[index].id();
        event_loop.sessions[index].app.checkpoint_now();
        let snapshot = Arc::clone(&event_loop.sessions[index].app.state.session);
        smol::block_on(event_loop.ctx.storage_writer.checkpoint(CheckpointRequest {
            session_id: source,
            version: CheckpointVersion {
                revision: snapshot.revision(),
                epoch: 1,
            },
            snapshot,
        }))
        .unwrap();
        assert!(
            event_loop
                .ctx
                .storage_writer
                .latest_snapshot(source)
                .is_some()
        );
        approval_dispatch(event_loop, index, &path, true);
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == crate::app::mode::Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(APPROVAL_WAIT).unwrap();
        assert!(
            event_loop
                .ctx
                .storage_writer
                .latest_snapshot(source)
                .is_none()
        );
        let restored = AppSession::load(source, &event_loop.ctx.storage).unwrap();
        assert!(
            restored
                .messages()
                .iter()
                .any(|message| message.user_text() == Some(APPROVAL_HISTORY))
        );
        assert_eq!(restored.model, APPROVAL_SOURCE_MODEL);
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_postcommit_cancel_keeps_selected_build_runtime(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let source_id = event_loop.sessions[index].id();
        let (record, recorded) = flume::unbounded();
        let (release, gate) = flume::bounded(1);
        event_loop.ctx.prepare_provider = Arc::new(move |model, _| {
            Ok(PreparedModel {
                model,
                provider: Arc::new(ApprovalRecordingProvider {
                    requests: record.clone(),
                    release: Some(gate.clone()),
                }),
            })
        });
        approval_dispatch(event_loop, index, &path, fresh);
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == crate::app::mode::Mode::Build
                && !recorded.is_empty()
        });
        let (model, _, _, _) = recorded.recv_timeout(APPROVAL_WAIT).unwrap();
        assert_eq!(model, APPROVAL_TARGET_MODEL);
        let id = event_loop.sessions[index].id();
        let identity = event_loop.sessions[index].handles.identity();
        assert_eq!(id != source_id, fresh);
        let run = event_loop.sessions[index].app.run_id;
        event_loop.dispatch(index, vec![Action::CancelAgent { run_id: run }]);
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
        });
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(Arc::ptr_eq(
            &identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_eq!(
            event_loop.sessions[index].app.state.mode,
            crate::app::mode::Mode::Build
        );
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            APPROVAL_TARGET_MODEL
        );
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert!(event_loop.sessions[index].app.state.plan.path().is_none());
        assert!(recorded.is_empty());
        assert!(requests.is_empty());
        drop(release);
    });
}

#[test]
fn approval_fresh_candidate_cancel_releases_source_setter() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let original_id = event_loop.sessions[index].id();
        let original_identity = event_loop.sessions[index].handles.identity();
        let run = event_loop.sessions[index].app.run_id;
        approval_dispatch(event_loop, index, &path, true);
        let ready = approval_final_ready(event_loop, true, &requests);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let before = actor.effective_config().unwrap();
        let successor = actor.reserve_config_update().unwrap();
        successor
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    fast: Some(false),
                    ..Default::default()
                },
            )))
            .unwrap();
        assert!(Arc::ptr_eq(&before, &actor.effective_config().unwrap()));
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        event_loop.handle_internal(ready);
        smol::block_on(successor.wait()).unwrap();
        approval_assert_preserved(event_loop, index, &path, run);
        assert_eq!(event_loop.sessions[index].id(), original_id);
        assert!(Arc::ptr_eq(
            &original_identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert!(!actor.effective_config().unwrap().fast);
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_runtime_identity_rejects_stale_completion(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        approval_dispatch(event_loop, index, &path, fresh);
        let mut ready = approval_ready(event_loop);
        if let InternalEvent::PlanApprovalReady { runtime, .. } = &mut ready {
            *runtime = Arc::new(());
        }
        event_loop.handle_internal(ready);
        assert!(event_loop.sessions[index].app.plan_approval_pending);
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        approval_assert_preserved(event_loop, index, &path, run);
        assert!(requests.is_empty());
    });
}

struct ApprovalLockDisposal {
    path: PathBuf,
    disposed: flume::Sender<io::Result<String>>,
}

impl Drop for ApprovalLockDisposal {
    fn drop(&mut self) {
        let _ = self.disposed.send(std::fs::read_to_string(&self.path));
    }
}

#[test]
fn approval_fresh_cancel_releases_source_before_target_lock_returns() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let source = event_loop.sessions[index].id();
        let identity = event_loop.sessions[index].handles.identity();
        let run = event_loop.sessions[index].app.run_id;
        let targets = event_loop.ctx.command_runtime.registry.target_count();
        let recents = maki_storage::model::read_recents(&event_loop.ctx.storage);
        let saved_model = maki_storage::model::read_model(&event_loop.ctx.storage);
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let (disposed, disposal) = flume::bounded(1);
        let sessions_dir = event_loop.ctx.sessions_dir.clone();
        event_loop.ctx.lock_prepare_gate = Some(Arc::new(move |target| {
            let guard = ApprovalLockDisposal {
                path: session_lock::lock_path(&sessions_dir, &target),
                disposed: disposed.clone(),
            };
            entered.send(target).unwrap();
            gate.recv_timeout(APPROVAL_WAIT).unwrap();
            Box::new(guard)
        }));
        approval_dispatch(event_loop, index, &path, true);
        let ready = approval_ready(event_loop);
        event_loop.handle_internal(ready);
        let target = entry.recv_timeout(APPROVAL_WAIT).unwrap();
        assert!(disposal.is_empty());
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let before = actor.effective_config().unwrap();
        let successor = actor.reserve_config_update().unwrap();
        successor
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    fast: Some(false),
                    ..Default::default()
                },
            )))
            .unwrap();
        assert!(Arc::ptr_eq(&before, &actor.effective_config().unwrap()));
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        smol::block_on(bounded_session_op(
            async { successor.wait().await.map_err(|error| error.to_string()) },
            APPROVAL_WAIT,
        ))
        .unwrap();
        assert!(!actor.effective_config().unwrap().fast);
        assert!(disposal.is_empty());
        assert!(!session_lock::lock_path(&event_loop.sessions_dir, &target).exists());
        let cancelled = approval_ready(event_loop);
        event_loop.handle_internal(cancelled);
        approval_assert_preserved(event_loop, index, &path, run);
        release.send(()).unwrap();
        assert_eq!(
            disposal.recv_timeout(APPROVAL_WAIT).unwrap().unwrap(),
            APPROVAL_RELEASED_LOCK
        );
        event_loop.ctx.lock_prepare_gate = None;
        assert_eq!(event_loop.sessions[index].id(), source);
        assert!(Arc::ptr_eq(
            &identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_eq!(
            event_loop.ctx.command_runtime.registry.target_count(),
            targets
        );
        assert_eq!(
            maki_storage::model::read_recents(&event_loop.ctx.storage),
            recents
        );
        assert_eq!(
            maki_storage::model::read_model(&event_loop.ctx.storage),
            saved_model
        );
        assert!(SessionCoordinatorHandle::resolve(target).is_err());
        assert!(SessionMailbox::notify(target, APPROVAL_RESPONSE.into(), false).is_err());
        assert!(
            event_loop
                .ctx
                .storage_writer
                .latest_snapshot(target)
                .is_none()
        );
        assert!(AppSession::load(target, &event_loop.ctx.storage).is_err());
        assert!(
            !event_loop
                .sessions_dir
                .join(format!("{target}.jsonl"))
                .exists()
        );
        let target_lock = session_lock::claim(&event_loop.sessions_dir, &target)
            .unwrap()
            .unwrap();
        target_lock.release().unwrap();
        assert!(
            session_lock::claim(&event_loop.sessions_dir, &source)
                .unwrap()
                .is_none()
        );
        assert!(requests.is_empty());
    });
}

#[test]
fn approval_fresh_target_lock_failure_preserves_runtime_and_storage() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let id = event_loop.sessions[index].id();
        let identity = event_loop.sessions[index].handles.identity();
        let targets = event_loop.ctx.command_runtime.registry.target_count();
        let run = event_loop.sessions[index].app.run_id;
        approval_dispatch(event_loop, index, &path, true);
        let ready = approval_ready(event_loop);
        let blocked = PathBuf::from(&event_loop.session_cwd).join("approval-lock-file");
        std::fs::write(&blocked, []).unwrap();
        let sessions = std::mem::replace(&mut event_loop.ctx.sessions_dir, blocked);
        event_loop.handle_internal(ready);
        approval_pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        event_loop.ctx.sessions_dir = sessions;
        approval_assert_preserved(event_loop, index, &path, run);
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(Arc::ptr_eq(
            &identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_eq!(
            event_loop.ctx.command_runtime.registry.target_count(),
            targets
        );
        assert!(requests.is_empty());
    });
}

#[test]
fn approval_fresh_activation_failure_releases_candidate_resources() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let source_id = event_loop.sessions[index].id();
        let source_identity = event_loop.sessions[index].handles.identity();
        let targets = event_loop.ctx.command_runtime.registry.target_count();
        let run = event_loop.sessions[index].app.run_id;
        approval_dispatch(event_loop, index, &path, true);
        let ready = approval_final_ready(event_loop, true, &requests);
        let InternalEvent::PlanApprovalReady {
            result: Ok(prepared),
            ..
        } = &ready
        else {
            panic!("expected prepared candidate");
        };
        let candidate = prepared.candidate.as_ref().unwrap();
        let target = candidate.app.session_id();
        let (manager, root) = candidate.handles.manager_and_root();
        let ticket = prepared
            .candidate_commit
            .as_ref()
            .unwrap()
            .ticket
            .as_ref()
            .unwrap()
            .clone();
        let duplicate = prepare_coordinator(
            &event_loop.ctx.coordinator_deps(),
            &candidate.app.app.state.session,
            Vec::new(),
            event_loop.ctx.available_model_specs(),
            &candidate.model_slot,
            &candidate.handles,
            &candidate.app.app.permissions,
            candidate.app.app.state.thinking,
        )
        .unwrap()
        .activate()
        .unwrap();
        event_loop.handle_internal(ready);
        approval_assert_preserved(event_loop, index, &path, run);
        assert_eq!(event_loop.sessions[index].id(), source_id);
        assert!(Arc::ptr_eq(
            &source_identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_eq!(
            event_loop.ctx.command_runtime.registry.target_count(),
            targets
        );
        let deadline = Instant::now() + APPROVAL_WAIT;
        while ticket.peek().is_none() || !manager.runner_finished(root).unwrap() {
            assert!(
                Instant::now() < deadline,
                "failed candidate runner did not shut down"
            );
            std::thread::yield_now();
        }
        assert!(
            !event_loop
                .sessions_dir
                .join(format!("{target}.jsonl"))
                .exists()
        );
        assert!(
            event_loop
                .ctx
                .storage_writer
                .latest_snapshot(target)
                .is_none()
        );
        assert!(AppSession::load(target, &event_loop.ctx.storage).is_err());
        let candidate_lock = session_lock::claim(&event_loop.sessions_dir, &target)
            .unwrap()
            .unwrap();
        candidate_lock.release().unwrap();
        assert!(
            session_lock::claim(&event_loop.sessions_dir, &source_id)
                .unwrap()
                .is_none()
        );
        assert!(requests.is_empty());
        duplicate.retire();
        assert!(SessionCoordinatorHandle::resolve(target).is_err());
        assert!(SessionMailbox::notify(target, APPROVAL_RESPONSE.into(), false).is_err());
    });
}

#[cfg(unix)]
#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_cancel_releases_setter_before_final_read_returns(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        approval_dispatch(event_loop, index, &path, fresh);
        let mut ready = approval_ready(event_loop);
        if fresh {
            event_loop.handle_internal(ready);
            ready = approval_ready(event_loop);
        }
        std::fs::remove_file(&path).unwrap();
        assert!(
            Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let writer_path = path.clone();
        let writer = std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(writer_path)
                .unwrap();
            entered.send(()).unwrap();
            gate.recv().unwrap();
            file.write_all(APPROVAL_PLAN.as_bytes()).unwrap();
        });
        event_loop.handle_internal(ready);
        entry.recv_timeout(APPROVAL_WAIT).unwrap();
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let successor = actor.reserve_config_update().unwrap();
        successor
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    fast: Some(false),
                    ..Default::default()
                },
            )))
            .unwrap();
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        smol::block_on(bounded_session_op(
            async { successor.wait().await.map_err(|error| error.to_string()) },
            APPROVAL_WAIT,
        ))
        .unwrap();
        assert!(!actor.effective_config().unwrap().fast);
        assert!(requests.is_empty());
        release.send(()).unwrap();
        writer.join().unwrap();
        let cancelled = approval_ready(event_loop);
        event_loop.handle_internal(cancelled);
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, APPROVAL_PLAN).unwrap();
        assert_eq!(
            event_loop.sessions[index].app.state.mode,
            crate::app::mode::Mode::Plan
        );
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_final_event_preserves_immutable_plan_input(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        approval_dispatch(event_loop, index, &path, fresh);
        let ready = approval_final_ready(event_loop, fresh, &requests);
        const MUTATED: &str = "# Late edit\nDo not implement this changed revision.\n";
        std::fs::write(&path, MUTATED).unwrap();
        event_loop.handle_internal(ready);
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == crate::app::mode::Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        let (_, _, messages, system) = requests.recv_timeout(APPROVAL_WAIT).unwrap();
        let input = format!(
            "{}\n{}",
            system,
            messages
                .iter()
                .filter_map(Message::user_text)
                .collect::<Vec<_>>()
                .join("\n")
        );
        assert!(
            input.contains(APPROVAL_PLAN.trim()),
            "approved content missing from implementation input"
        );
        assert!(!input.contains(MUTATED.trim()));
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_inflight_heartbeat_does_not_abort(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        start_runtime_heartbeat_with(
            &mut event_loop.sessions[index],
            &event_loop.internal_tx,
            move |lease| {
                entered.send(()).unwrap();
                gate.recv().unwrap();
                (lease, Ok(session_lock::LockBeat::Held))
            },
        );
        entry.recv_timeout(APPROVAL_WAIT).unwrap();
        approval_dispatch(event_loop, index, &path, fresh);
        let ready = approval_final_ready(event_loop, fresh, &requests);
        event_loop.handle_internal(ready);
        assert!(
            event_loop.sessions[index].app.plan_approval_pending
                || event_loop.sessions[index].app.state.mode == crate::app::mode::Mode::Build
        );
        release.send(()).unwrap();
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == crate::app::mode::Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(APPROVAL_WAIT).unwrap();
        assert!(requests.is_empty());
    });
}

#[test_case(false; "directory")]
#[test_case(true; "history")]
fn approval_fresh_source_lease_defers_mutation_until_cancel(history: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let cwd = event_loop.sessions[index].coordinator.read().cwd();
        let original = event_loop.sessions[index].coordinator.read().history();
        let changed = cwd.join("approved-directory");
        std::fs::create_dir(&changed).unwrap();
        approval_dispatch(event_loop, index, &path, true);
        let ready = approval_final_ready(event_loop, true, &requests);
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let mutation = async {
            if history {
                coordinator
                    .replace_history(vec![Message::user(APPROVAL_RESPONSE.into())])
                    .await
                    .map(|_| ())
            } else {
                coordinator
                    .change_directory(changed.clone())
                    .await
                    .map(|_| ())
            }
        };
        let mut mutation = Box::pin(mutation);
        assert!(smol::block_on(futures_lite::future::poll_once(mutation.as_mut())).is_none());
        assert_eq!(coordinator.read().cwd(), cwd);
        assert!(Arc::ptr_eq(&original, &coordinator.read().history()));
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        event_loop.handle_internal(ready);
        smol::block_on(bounded_session_op(
            async { mutation.await.map_err(|error| error.to_string()) },
            APPROVAL_WAIT,
        ))
        .unwrap();
        if history {
            assert_eq!(
                coordinator.read().history()[0].user_text(),
                Some(APPROVAL_RESPONSE)
            );
            assert_eq!(coordinator.read().cwd(), cwd);
        } else {
            assert_eq!(coordinator.read().cwd(), changed);
            assert!(Arc::ptr_eq(&original, &coordinator.read().history()));
        }
        assert!(requests.is_empty());
        assert_eq!(
            event_loop.sessions[index].app.state.mode,
            crate::app::mode::Mode::Plan
        );
    });
}

#[test]
fn approval_fresh_uses_finalized_history_and_captured_absolute_path() {
    with_event_loop(|event_loop| {
        let (index, original_path, requests, _host) = approval_setup(event_loop);
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let cwd = coordinator.read().cwd().join("source-directory");
        std::fs::create_dir(&cwd).unwrap();
        smol::block_on(coordinator.change_directory(cwd.clone())).unwrap();
        let lease = smol::block_on(coordinator.acquire_lease()).unwrap();
        let committer = lease.committer().unwrap();
        smol::block_on(committer.commit_history(vec![Message::user(APPROVAL_RESPONSE.into())]))
            .unwrap();
        drop(lease);
        let relative = PathBuf::from("relative-plan.md");
        let absolute = cwd.join(&relative);
        std::fs::rename(original_path, &absolute).unwrap();
        Arc::make_mut(&mut event_loop.sessions[index].app.state.session).cwd =
            cwd.display().to_string();
        event_loop.sessions[index].app.state.plan =
            crate::app::mode::PlanState::Ready(relative.clone());
        approval_dispatch(event_loop, index, &relative, true);
        let ready = approval_ready(event_loop);
        if let InternalEvent::PlanApprovalReady {
            result: Ok(prepared),
            ..
        } = &ready
        {
            assert_eq!(prepared.source.snapshot().cwd(), cwd);
            assert_eq!(
                prepared.source.snapshot().history()[0].user_text(),
                Some(APPROVAL_RESPONSE)
            );
            assert_eq!(prepared.plan.path, absolute);
        } else {
            panic!("fresh source capture failed");
        }
        event_loop.handle_internal(ready);
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == crate::app::mode::Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        let (_, _, messages, _) = requests.recv_timeout(APPROVAL_WAIT).unwrap();
        assert!(
            !messages
                .iter()
                .any(|message| message.user_text() == Some(APPROVAL_HISTORY)
                    || message.user_text() == Some(APPROVAL_RESPONSE))
        );
        assert!(
            messages
                .iter()
                .filter_map(Message::user_text)
                .any(|text| text.contains(&absolute.display().to_string()))
        );
        assert_eq!(event_loop.sessions[index].coordinator.read().cwd(), cwd);
        assert!(requests.is_empty());
    });
}

#[test]
fn approval_fresh_stale_cwd_rejects_relative_path() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let cwd = coordinator.read().cwd().join("changed-directory");
        std::fs::create_dir(&cwd).unwrap();
        smol::block_on(coordinator.change_directory(cwd)).unwrap();
        let relative = PathBuf::from(path.file_name().unwrap());
        event_loop.sessions[index].app.state.plan =
            crate::app::mode::PlanState::Ready(relative.clone());
        approval_dispatch(event_loop, index, &relative, true);
        approval_pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        approval_assert_preserved(event_loop, index, &relative, run);
        assert!(requests.is_empty());
    });
}

#[test]
fn approval_fresh_permission_change_before_activation_preserves_source() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        approval_dispatch(event_loop, index, &path, true);
        let ready = approval_final_ready(event_loop, true, &requests);
        event_loop.sessions[index].app.permissions.toggle_yolo();
        event_loop.handle_internal(ready);
        approval_assert_preserved(event_loop, index, &path, run);
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_provider_preparation_failure_preserves_plan(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        event_loop.ctx.prepare_provider = Arc::new(|_, _| Err(APPROVAL_PROVIDER_FAILURE.into()));
        approval_dispatch(event_loop, index, &path, fresh);
        approval_pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        approval_assert_preserved(event_loop, index, &path, run);
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(requests.is_empty());
    });
}

#[test]
fn approval_cancel_during_provider_preparation_ignores_late_result() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let (completed, completion) = flume::bounded(1);
        let (record, late_requests) = flume::unbounded();
        event_loop.ctx.prepare_provider = Arc::new(move |model, _| {
            entered.send(()).unwrap();
            gate.recv().unwrap();
            completed.send(()).unwrap();
            Ok(PreparedModel {
                model,
                provider: Arc::new(ApprovalRecordingProvider {
                    requests: record.clone(),
                    release: None,
                }),
            })
        });
        approval_dispatch(event_loop, index, &path, false);
        entry.recv_timeout(APPROVAL_WAIT).unwrap();
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        let cancelled = approval_ready(event_loop);
        event_loop.handle_internal(cancelled);
        release.send(()).unwrap();
        completion.recv_timeout(APPROVAL_WAIT).unwrap();
        approval_assert_preserved(event_loop, index, &path, run);
        assert!(requests.is_empty());
        assert!(late_requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_cancel_before_ready_preserves_plan(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        approval_dispatch(event_loop, index, &path, fresh);
        let ready = approval_ready(event_loop);
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        event_loop.handle_internal(ready);
        let _ = event_loop.tick();
        approval_assert_preserved(event_loop, index, &path, run);
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_save_failure_retains_committed_selection(fresh: bool) {
    with_event_loop(|event_loop| {
        let (warnings, warning_rx) = flume::unbounded();
        event_loop.ctx.storage_writer =
            Arc::new(StorageWriter::new(event_loop.ctx.storage.clone(), warnings));
        let (index, path, requests, _host) = approval_setup(event_loop);
        approval_dispatch(event_loop, index, &path, fresh);
        approval_pump_until(event_loop, |event_loop| {
            let app = &event_loop.sessions[index].app;
            app.state.mode == crate::app::mode::Mode::Build && app.status == Status::Idle
        });
        requests.recv_timeout(APPROVAL_WAIT).unwrap();
        let id = event_loop.sessions[index].id();
        let runtime = &event_loop.sessions[index];
        smol::block_on(event_loop.ctx.storage_writer.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: runtime.app.state.session.revision(),
                epoch: 1,
            },
            snapshot: Arc::clone(&runtime.app.state.session),
        }))
        .unwrap();
        let log = event_loop.sessions_dir.join(format!("{id}.jsonl"));
        let moved = log.with_extension("approval-backup");
        std::fs::rename(&log, &moved).unwrap();
        std::fs::create_dir(&log).unwrap();
        event_loop.sessions[index].app.state.session = Arc::new({
            let mut session = (*event_loop.sessions[index].app.state.session).clone();
            session.set_title(APPROVAL_RESPONSE.into());
            session
        });
        event_loop.sessions[index].app.checkpoint_now();
        let warning = warning_rx.recv_timeout(APPROVAL_WAIT).unwrap();
        assert!(warning.starts_with("Session save failed"), "{warning}");
        assert_eq!(event_loop.sessions[index].id(), id);
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            APPROVAL_TARGET_MODEL
        );
        assert_eq!(
            event_loop.sessions[index].app.state.mode,
            crate::app::mode::Mode::Build
        );
        std::fs::remove_dir(&log).unwrap();
        std::fs::rename(&moved, &log).unwrap();
        event_loop.sessions[index].app.checkpoint_now();
        let runtime = &event_loop.sessions[index];
        smol::block_on(event_loop.ctx.storage_writer.checkpoint(CheckpointRequest {
            session_id: id,
            version: CheckpointVersion {
                revision: runtime.app.state.session.revision(),
                epoch: 1,
            },
            snapshot: Arc::clone(&runtime.app.state.session),
        }))
        .unwrap();
        assert_eq!(
            AppSession::load(id, &event_loop.ctx.storage).unwrap().model,
            APPROVAL_TARGET_MODEL
        );
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_without_override_uses_actor_predecessor(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let (record, recorded) = flume::unbounded();
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let predecessor = actor.reserve_config_update().unwrap();
        predecessor
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    model: Some(PreparedModel {
                        model: Model::from_spec(APPROVAL_TARGET_MODEL).unwrap(),
                        provider: Arc::new(ApprovalRecordingProvider {
                            requests: record,
                            release: None,
                        }),
                    }),
                    thinking: Some(DomainThinkingConfig::Off),
                    fast: Some(false),
                    ..Default::default()
                },
            )))
            .unwrap();
        smol::block_on(predecessor.wait()).unwrap();
        drop(smol::block_on(event_loop.sessions[index].coordinator.acquire_lease()).unwrap());
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            APPROVAL_SOURCE_MODEL
        );
        event_loop.ctx.prepare_provider =
            Arc::new(|_, _| panic!("no-override approval must reuse actor provider"));
        event_loop.sessions[index].app.plan_form.use_current_model();
        event_loop.sessions[index].app.plan_approval_pending = true;
        event_loop.dispatch(
            index,
            vec![Action::ApprovePlan {
                clear_context: fresh,
                model: None,
                parallel: false,
                path,
            }],
        );
        let ready = approval_ready(event_loop);
        if let InternalEvent::PlanApprovalReady {
            result: Err(error), ..
        } = &ready
        {
            panic!("no-override preparation failed: {error}");
        }
        event_loop.handle_internal(ready);
        approval_pump_until(event_loop, |event_loop| {
            let app = &event_loop.sessions[index].app;
            !app.plan_approval_pending && app.status == Status::Idle
        });
        let (model, options, _, _) = recorded.recv_timeout(APPROVAL_WAIT).unwrap();
        assert_eq!(model, APPROVAL_TARGET_MODEL);
        assert_eq!(
            options,
            ApprovalOptions {
                thinking: DomainThinkingConfig::Off,
                fast: false
            }
        );
        assert!(recorded.is_empty());
        assert!(requests.is_empty());
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            APPROVAL_TARGET_MODEL
        );
    });
}

#[test]
fn approval_busy_rejection_preserves_active_turn() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let (active_requests, active_rx) = flume::unbounded();
        let (release, gate) = flume::bounded(1);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let change = actor.reserve_config_update().unwrap();
        change
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    model: Some(PreparedModel {
                        model: Model::from_spec(APPROVAL_SOURCE_MODEL).unwrap(),
                        provider: Arc::new(ApprovalRecordingProvider {
                            requests: active_requests,
                            release: Some(gate),
                        }),
                    }),
                    ..Default::default()
                },
            )))
            .unwrap();
        smol::block_on(change.wait()).unwrap();
        event_loop
            .submit_text(index, APPROVAL_HISTORY.into())
            .unwrap();
        active_rx.recv_timeout(APPROVAL_WAIT).unwrap();
        let run = event_loop.sessions[index].app.run_id;
        let config = actor.effective_config().unwrap();
        approval_dispatch(event_loop, index, &path, false);
        assert_eq!(event_loop.sessions[index].app.run_id, run);
        assert_eq!(event_loop.sessions[index].app.status, Status::Streaming);
        assert_eq!(
            event_loop.sessions[index].app.state.mode,
            crate::app::mode::Mode::Plan
        );
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert_eq!(
            event_loop.sessions[index]
                .app
                .plan_form
                .implementation_model(),
            Some(APPROVAL_TARGET_MODEL)
        );
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(requests.is_empty());
        release.send(()).unwrap();
        approval_pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
        });
        assert!(active_rx.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn approval_changed_plan_before_commit_preserves_session(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = approval_setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        approval_dispatch(event_loop, index, &path, fresh);
        let ready = approval_ready(event_loop);
        std::fs::write(&path, "# Changed plan\nDo not approve the old contents.\n").unwrap();
        event_loop.handle_internal(ready);
        approval_pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        approval_assert_preserved(event_loop, index, &path, run);
        assert!(requests.is_empty());
    });
}
