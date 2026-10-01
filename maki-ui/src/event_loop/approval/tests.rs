use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyEvent};
use maki_agent::actor::{
    ActorStatus, AgentActorHandle, ConfigChange, ConfigCommit, ConfigPatch, ConfigUpdateTicket,
    PreparedModel,
};
use maki_agent::manager::ManagerError;
use maki_agent::session_coordinator::{SessionCoordinatorError, SessionCoordinatorHandle};
use maki_agent::tools::ToolRegistry;
use maki_agent::{AgentMode, SessionMailbox};
use maki_domain::ThinkingConfig as DomainThinkingConfig;
use maki_lua::PluginHost;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role,
    StopReason, StreamResponse, TokenUsage,
};
use maki_storage::checkpoint::{CheckpointRequest, CheckpointVersion, CheckpointWriter};
use maki_storage::id::{MakiId, SessionRef};
use maki_storage::model::{read_model, read_recents};
use maki_storage::session_lock;
use maki_storage::sessions::{StoredMode, read_prefs};
use test_case::test_case;

use super::super::tests::{shutdown_manager, with_event_loop};
use super::super::{
    EventLoop, InternalEvent, LOCK_LOST_MSG, PreparedProvider, Wake, prepare_coordinator,
    start_runtime_heartbeat_with,
};
use super::ApprovalStep;
use crate::AppSession;
use crate::app::mode::{Mode, PlanState};
use crate::components::keybindings::key;
use crate::components::{self, Action, Status};
use crate::plan_approval::{
    APPROVAL_BUSY, APPROVAL_CHANGED, IMPLEMENT_PARALLEL_HINT, idle_error_message,
};
use crate::storage_writer::{SAVE_FAILED_PREFIX, StorageWriter};

const SOURCE_MODEL: &str = "anthropic/claude-opus-4-6";
const TARGET_MODEL: &str = "openai/gpt-5";
const HISTORY: &str = "existing planning conversation";
const PLAN: &str = "# Implementation\n\nImplement the regression tests.\n";
const RESPONSE: &str = "implementation finished";
const PROVIDER_FAILURE: &str = "approval provider preparation failed";
const WAIT: Duration = Duration::from_secs(10);
const CHECKPOINT_EPOCH: u64 = 1;
const CHILD_PROMPT: &str = "gated managed child";
const RESET_COMMAND: &str = "/approval-reset";
const START_COMMAND_PREFIX: &str = "/approval-start-";
const MODE_COMMAND_PREFIX: &str = "/approval-mode-";
const CHILD_TOOL_NAME: &str = "approval_child";
const CHILD_CALL_ID: &str = "approval-child-call";
const CHILD_PLUGIN: &str = "approval-child";
const OBSERVER_PLUGIN: &str = "approval-observer";
const PLAN_FILE: &str = "approval-plan.md";
const RELATIVE_PLAN_FILE: &str = "relative-plan.md";
const MUTATED_PLAN: &str = "# Late edit\nDo not implement this changed revision.\n";
const MOVED_CWD: &str = "moved-cwd";
const SESSION_LOG_EXTENSION: &str = "jsonl";
const SESSION_LOG_BACKUP_EXTENSION: &str = "approval-backup";
const DRAFT: &str = "draft typed before approval";
const CLEAR_AND_IMPLEMENT_ROW: usize = 1;
const IMPLEMENT_ROW: usize = 2;
const USE_CURRENT_ROW: usize = 4;

type RecordedRequest = (String, RequestOptions, Vec<Message>, String);

fn child_tool() -> String {
    format!(
        r#"
    local sessions = {{}}
    maki.api.register_tool({{
        name = "{CHILD_TOOL_NAME}", description = "start test child", kind = "read",
        schema = {{ type = "object", properties = {{}} }},
        audiences = {{ "main" }},
        handler = function(_, ctx)
            local child, err = maki.agent.session(ctx, {{ name = "approval child", system = "child", tools = maki.json.decode('[]'), inherit_provider = true, auto_deliver = false }})
            if err then return {{ llm_output = err, is_error = true }} end
            sessions[#sessions + 1] = child
            local ok, send_err = child:send("{CHILD_PROMPT}")
            if send_err then return {{ llm_output = send_err, is_error = true }} end
            return "child started"
        end,
    }})
"#
    )
}

fn observer() -> String {
    format!(
        r#"
    local starts = 0
    maki.api.create_autocmd("TurnStart", {{ callback = function()
        starts = starts + 1
        maki.api.register_command({{ name = "{START_COMMAND_PREFIX}" .. tostring(starts), description = "observed", tui_only = false, handler = function() end }})
    end }})
    local modes = 0
    maki.api.create_autocmd("ModeChanged", {{ callback = function()
        modes = modes + 1
        maki.api.register_command({{ name = "{MODE_COMMAND_PREFIX}" .. tostring(modes), description = "observed", tui_only = false, handler = function() end }})
    end }})
    maki.api.create_autocmd("SessionReset", {{ callback = function(ev)
        maki.api.register_command({{ name = "{RESET_COMMAND}", description = ev.data.session_id, tui_only = false, handler = function() end }})
    end }})
"#
    )
}

struct RecordingProvider {
    requests: flume::Sender<RecordedRequest>,
    release: Option<flume::Receiver<()>>,
}

impl Provider for RecordingProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        _tools: &'a serde_json::Value,
        _events: &'a flume::Sender<ProviderEvent>,
        options: RequestOptions,
        _session: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.requests
                .send((model.spec(), options, messages.to_vec(), system.into()))
                .unwrap();
            if let Some(release) = &self.release {
                release.recv_async().await.unwrap();
            }
            Ok(StreamResponse {
                message: Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: RESPONSE.into(),
                    }],
                    ..Message::user(String::new())
                },
                usage: TokenUsage::default(),
                stop_reason: Some(StopReason::EndTurn),
            })
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct ChildProvider {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
    root_requests: AtomicUsize,
}

impl Provider for ChildProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        messages: &'a [Message],
        _system: &'a str,
        _tools: &'a serde_json::Value,
        _events: &'a flume::Sender<ProviderEvent>,
        _options: RequestOptions,
        _session: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let child = messages
                .iter()
                .any(|message| message.user_text() == Some(CHILD_PROMPT));
            let content = if child {
                self.entered.send(()).unwrap();
                self.release.recv_async().await.unwrap();
                ContentBlock::Text {
                    text: RESPONSE.into(),
                }
            } else if self.root_requests.fetch_add(1, Ordering::SeqCst) == 0 {
                ContentBlock::ToolUse {
                    id: CHILD_CALL_ID.into(),
                    name: CHILD_TOOL_NAME.into(),
                    input: serde_json::json!({}),
                    thought_signature: None,
                }
            } else {
                ContentBlock::Text {
                    text: RESPONSE.into(),
                }
            };
            Ok(StreamResponse {
                message: Message {
                    role: Role::Assistant,
                    content: vec![content],
                    ..Message::user(String::new())
                },
                usage: TokenUsage::default(),
                stop_reason: Some(if child || self.root_requests.load(Ordering::SeqCst) > 1 {
                    StopReason::EndTurn
                } else {
                    StopReason::ToolUse
                }),
            })
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[test]
fn rejects_active_managed_child_without_cancelling_it() {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) =
            setup_with_registry(event_loop, Arc::clone(ToolRegistry::global_arc()));
        host.load_source(CHILD_PLUGIN, &child_tool()).unwrap();
        event_loop.sessions[index]
            .app
            .permissions
            .set_session_yolo(Some(true));
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let provider = Arc::new(ChildProvider {
            entered,
            release: gate,
            root_requests: AtomicUsize::new(0),
        });
        apply_patch(&actor, model_patch(SOURCE_MODEL, provider));
        event_loop.submit_text(index, HISTORY.into()).unwrap();
        pump_until(event_loop, |_| !entry.is_empty());
        entry.recv_timeout(WAIT).unwrap();
        pump_until(event_loop, |event_loop| {
            actor.snapshot().status == ActorStatus::Idle
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
        assert!(matches!(child_before.status, ActorStatus::Running(_)));
        let history = event_loop.sessions[index].coordinator.read().history();
        let recent = read_recents(&event_loop.ctx.storage);
        let saved_model = read_model(&event_loop.ctx.storage);
        let run = event_loop.sessions[index].app.run_id;
        let config = actor.effective_config().unwrap();
        approve(event_loop, index, true);
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert_eq!(
            event_loop.sessions[index].app.status_bar.flash_text(),
            Some(APPROVAL_BUSY)
        );
        assert_eq!(event_loop.sessions[index].app.run_id, run);
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert_eq!(child.snapshot().status, child_before.status);
        assert_eq!(child.snapshot().active_turn, child_before.active_turn);
        assert_eq!(child.snapshot().lifecycle, child_before.lifecycle);
        assert!(Arc::ptr_eq(
            &history,
            &event_loop.sessions[index].coordinator.read().history()
        ));
        assert_eq!(read_recents(&event_loop.ctx.storage), recent);
        assert_eq!(read_model(&event_loop.ctx.storage), saved_model);
        assert_eq!(event_loop.sessions[index].app.state.mode, Mode::Plan);
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert!(requests.is_empty());
        release.send(()).unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
                && child.snapshot().status == ActorStatus::Idle
        });
        assert!(child.snapshot().latest.is_some());
        shutdown_manager(&manager);
    });
}

fn setup(
    event_loop: &mut EventLoop<'_>,
) -> (usize, PathBuf, flume::Receiver<RecordedRequest>, PluginHost) {
    setup_with_registry(event_loop, Arc::new(ToolRegistry::new()))
}

fn setup_with_registry(
    event_loop: &mut EventLoop<'_>,
    registry: Arc<ToolRegistry>,
) -> (usize, PathBuf, flume::Receiver<RecordedRequest>, PluginHost) {
    let host = PluginHost::with_command_registry(
        registry,
        event_loop.ctx.command_runtime.registry.clone(),
        false,
    )
    .unwrap();
    host.load_source(OBSERVER_PLUGIN, &observer()).unwrap();
    event_loop.ctx.lua_event_handle = host.event_handle();
    let path = PathBuf::from(&event_loop.session_cwd).join(PLAN_FILE);
    fs::write(&path, PLAN).unwrap();
    let (requests, receiver) = flume::unbounded();
    let mut session = AppSession::new(SOURCE_MODEL, &event_loop.session_cwd);
    session.meta.mode = Some(StoredMode::Plan);
    session.meta.plan_path = Some(path.display().to_string());
    session.meta.plan_written = true;
    session.meta.thinking = Some(DomainThinkingConfig::Off.into());
    session.meta.fast = true;
    session.push_message(Message::user(HISTORY.into()));
    let runtime = event_loop
        .ctx
        .spawn_runtime_with_provider(
            session,
            Some(PreparedProvider {
                model: Model::from_spec(SOURCE_MODEL).unwrap(),
                provider: Arc::new(RecordingProvider {
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
        .set_implementation_model(TARGET_MODEL.into(), SOURCE_MODEL);
    event_loop.ctx.prepare_provider = Arc::new(move |model, _| {
        Ok(PreparedModel {
            model: model.clone(),
            provider: Arc::new(RecordingProvider {
                requests: requests.clone(),
                release: None,
            }),
        })
    });
    (index, path, receiver, host)
}

fn press(event_loop: &mut EventLoop<'_>, key_event: KeyEvent) {
    event_loop.handle_input(Event::Key(key_event));
}

/// Reopening the form resets its selection to the first row.
fn choose_form_row(event_loop: &mut EventLoop<'_>, index: usize, row: usize, parallel: bool) {
    event_loop.set_focused(index);
    if event_loop.sessions[index].app.plan_form.is_visible() {
        press(event_loop, key::PLAN_TOGGLE.to_key_event());
    }
    press(event_loop, key::PLAN_TOGGLE.to_key_event());
    if event_loop.sessions[index].app.plan_form.parallel() != parallel {
        press(event_loop, components::key(KeyCode::Char(' ')));
    }
    for _ in 0..row {
        press(event_loop, components::key(KeyCode::Down));
    }
    press(event_loop, components::key(KeyCode::Enter));
}

fn approve(event_loop: &mut EventLoop<'_>, index: usize, fresh: bool) {
    let row = if fresh {
        CLEAR_AND_IMPLEMENT_ROW
    } else {
        IMPLEMENT_ROW
    };
    choose_form_row(event_loop, index, row, true);
}

fn next_ready(event_loop: &mut EventLoop<'_>) -> InternalEvent {
    let deadline = Instant::now() + WAIT;
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

fn pump_until(event_loop: &mut EventLoop<'_>, mut done: impl FnMut(&EventLoop<'_>) -> bool) {
    let deadline = Instant::now() + WAIT;
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
        thread::yield_now();
    }
}

fn queue_patch(actor: &AgentActorHandle, patch: ConfigPatch) -> ConfigUpdateTicket {
    let ticket = actor.reserve_config_update().unwrap();
    ticket.resolve(Ok(ConfigChange::Patch(patch))).unwrap();
    ticket
}

fn apply_patch(actor: &AgentActorHandle, patch: ConfigPatch) -> ConfigCommit {
    smol::block_on(queue_patch(actor, patch).wait()).unwrap()
}

fn model_patch(spec: &str, provider: Arc<dyn Provider>) -> ConfigPatch {
    ConfigPatch {
        model: Some(PreparedModel {
            model: Model::from_spec(spec).unwrap(),
            provider,
        }),
        ..Default::default()
    }
}

fn disable_fast() -> ConfigPatch {
    ConfigPatch {
        fast: Some(false),
        ..Default::default()
    }
}

fn session_log(event_loop: &EventLoop<'_>, id: MakiId) -> PathBuf {
    event_loop
        .sessions_dir
        .join(format!("{id}.{SESSION_LOG_EXTENSION}"))
}

fn checkpoint(event_loop: &EventLoop<'_>, index: usize) {
    let runtime = &event_loop.sessions[index];
    smol::block_on(event_loop.ctx.storage_writer.checkpoint(CheckpointRequest {
        session_id: runtime.id(),
        version: CheckpointVersion {
            revision: runtime.app.state.session.revision(),
            epoch: CHECKPOINT_EPOCH,
        },
        snapshot: Arc::clone(&runtime.app.state.session),
    }))
    .unwrap();
}

fn assert_preserved(
    event_loop: &EventLoop<'_>,
    index: usize,
    path: &Path,
    run_id: u64,
    flash: Option<&str>,
) {
    let app = &event_loop.sessions[index].app;
    assert_eq!(app.status_bar.flash_text(), flash);
    assert_eq!(app.state.model.spec(), SOURCE_MODEL);
    assert_eq!(app.state.mode, Mode::Plan);
    assert_eq!(app.state.plan.path(), Some(path));
    assert_eq!(app.plan_form.implementation_model(), Some(TARGET_MODEL));
    assert_eq!(app.run_id, run_id);
    assert!(
        app.state
            .session
            .messages()
            .iter()
            .any(|message| message.user_text() == Some(HISTORY))
    );
    assert!(!app.plan_approval_pending);
    assert_eq!(app.state.session.messages().len(), 1);
    assert!(observed(event_loop, index, START_COMMAND_PREFIX).is_empty());
}

/// Names and descriptions of the commands the observer plugin registered
/// under `prefix`.
fn observed(event_loop: &EventLoop<'_>, index: usize, prefix: &str) -> Vec<(String, String)> {
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
        .map(|command| command.spec())
        .filter(|spec| spec.name.starts_with(prefix))
        .map(|spec| (spec.name.to_string(), spec.docs.summary.to_string()))
        .collect()
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn dispatch_uses_selected_provider_and_persists_completion(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let original_id = event_loop.sessions[index].id();
        let original_run = event_loop.sessions[index].app.run_id;
        let prefs = serde_json::to_value(read_prefs(&event_loop.ctx.storage)).unwrap();
        let stored_model = read_model(&event_loop.ctx.storage);
        let recents = read_recents(&event_loop.ctx.storage);
        approve(event_loop, index, fresh);
        approve(event_loop, index, fresh);
        let ready = next_ready(event_loop);
        assert!(requests.is_empty());
        assert_eq!(event_loop.sessions[index].id(), original_id);
        assert_eq!(read_recents(&event_loop.ctx.storage), recents);
        assert_eq!(read_model(&event_loop.ctx.storage), stored_model);
        assert_eq!(
            serde_json::to_value(read_prefs(&event_loop.ctx.storage)).unwrap(),
            prefs
        );
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            SOURCE_MODEL
        );
        assert!(observed(event_loop, index, START_COMMAND_PREFIX).is_empty());
        event_loop.handle_internal(ready);
        pump_until(event_loop, |event_loop| {
            let app = &event_loop.sessions[index].app;
            app.run_id == original_run.wrapping_add(1)
                && app.status == Status::Idle
                && !app.plan_approval_pending
        });
        let starts: Vec<_> = observed(event_loop, index, START_COMMAND_PREFIX)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(starts, [format!("{START_COMMAND_PREFIX}1")]);
        assert_eq!(
            read_recents(&event_loop.ctx.storage)
                .first()
                .map(String::as_str),
            Some(TARGET_MODEL)
        );
        assert_eq!(
            read_model(&event_loop.ctx.storage).as_deref(),
            Some(TARGET_MODEL)
        );
        assert_eq!(
            serde_json::to_value(read_prefs(&event_loop.ctx.storage)).unwrap(),
            prefs
        );
        let (model, options, messages, _) = requests.recv_timeout(WAIT).unwrap();
        assert_eq!(model, TARGET_MODEL);
        assert_eq!(
            options,
            RequestOptions {
                thinking: DomainThinkingConfig::Off,
                fast: true
            }
            .clamped(&Model::from_spec(TARGET_MODEL).unwrap())
        );
        assert_eq!(
            messages
                .iter()
                .any(|message| message.user_text() == Some(HISTORY)),
            !fresh
        );
        let instruction = messages
            .iter()
            .filter_map(Message::user_text)
            .next_back()
            .unwrap();
        assert!(instruction.contains(&path.display().to_string()));
        assert!(instruction.contains(IMPLEMENT_PARALLEL_HINT));
        assert!(requests.is_empty());
        let runtime = &mut event_loop.sessions[index];
        assert_eq!(runtime.id() != original_id, fresh);
        assert_eq!(runtime.app.state.mode, Mode::Build);
        assert_eq!(runtime.app.state.model.spec(), TARGET_MODEL);
        assert_eq!(runtime.app.plan_form.implementation_model(), None);
        let (manager, root) = runtime.handles.manager_and_root();
        assert_eq!(
            manager
                .actor(root)
                .unwrap()
                .effective_config()
                .unwrap()
                .mode,
            AgentMode::Build
        );
        runtime.app.checkpoint_now();
        let id = runtime.id();
        checkpoint(event_loop, index);
        let restored = AppSession::load(id, &event_loop.ctx.storage).unwrap();
        assert_eq!(restored.model, TARGET_MODEL);
        assert_eq!(restored.meta.mode, Some(StoredMode::Build));
        assert_eq!(restored.meta.thinking, Some(options.thinking.into()));
        assert_eq!(restored.meta.fast, options.fast);
        assert_eq!(
            restored
                .messages()
                .iter()
                .any(|message| message.user_text() == Some(HISTORY)),
            !fresh
        );
        assert!(restored.messages().iter().any(|message| {
            message
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::Text { text } if text == RESPONSE))
        }));
        assert!(requests.is_empty());
    });
}

fn final_ready(
    event_loop: &mut EventLoop<'_>,
    fresh: bool,
    requests: &flume::Receiver<RecordedRequest>,
) -> InternalEvent {
    loop {
        let ready = next_ready(event_loop);
        assert!(requests.is_empty());
        match &ready {
            InternalEvent::PlanApprovalReady {
                result: Ok(step), ..
            } if !fresh || matches!(step, ApprovalStep::Fresh(_)) => {
                return ready;
            }
            InternalEvent::PlanApprovalReady {
                result: Err(error), ..
            } => panic!("approval preparation failed: {error}"),
            _ => event_loop.handle_internal(ready),
        }
    }
}

#[test]
fn mode_transition_emits_once_and_config_only_emits_none() {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let before = observed(event_loop, index, MODE_COMMAND_PREFIX).len();
        approve(event_loop, index, false);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(WAIT).unwrap();
        let _ = event_loop.tick();
        assert_eq!(
            observed(event_loop, index, MODE_COMMAND_PREFIX).len(),
            before + 1
        );
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let commit = apply_patch(&actor, disable_fast());
        event_loop.sessions[index].project_config(&commit);
        let _ = event_loop.tick();
        assert_eq!(
            observed(event_loop, index, MODE_COMMAND_PREFIX).len(),
            before + 1
        );
    });
}

#[test]
fn fresh_forgets_retired_cache_but_preserves_saved_source() {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let source = event_loop.sessions[index].id();
        event_loop.sessions[index].app.checkpoint_now();
        checkpoint(event_loop, index);
        assert!(
            event_loop
                .ctx
                .storage_writer
                .latest_snapshot(source)
                .is_some()
        );
        approve(event_loop, index, true);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(WAIT).unwrap();
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
                .any(|message| message.user_text() == Some(HISTORY))
        );
        assert_eq!(restored.model, SOURCE_MODEL);
    });
}

#[test]
fn fresh_retires_outgoing_session_like_a_reset() {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let source = event_loop.sessions[index].id();
        event_loop.sessions[index]
            .app
            .input_box
            .set_input(DRAFT.into());
        approve(event_loop, index, true);
        let ready = final_ready(event_loop, true, &requests);
        event_loop.handle_internal(ready);
        session_lock::claim(&event_loop.sessions_dir, &source)
            .unwrap()
            .expect("outgoing lock is released at the swap")
            .release()
            .unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(WAIT).unwrap();
        assert_eq!(
            observed(event_loop, index, RESET_COMMAND),
            [(RESET_COMMAND.to_string(), source.to_string())]
        );
        pump_until(event_loop, |event_loop| {
            AppSession::load(source, &event_loop.ctx.storage)
                .ok()
                .and_then(|session| session.meta.input_draft)
                .as_deref()
                == Some(DRAFT)
        });
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn postcommit_cancel_keeps_selected_build_runtime(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let source_id = event_loop.sessions[index].id();
        let (record, recorded) = flume::unbounded();
        let (release, gate) = flume::bounded(1);
        event_loop.ctx.prepare_provider = Arc::new(move |model, _| {
            Ok(PreparedModel {
                model,
                provider: Arc::new(RecordingProvider {
                    requests: record.clone(),
                    release: Some(gate.clone()),
                }),
            })
        });
        approve(event_loop, index, fresh);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == Mode::Build && !recorded.is_empty()
        });
        let (model, _, _, _) = recorded.recv_timeout(WAIT).unwrap();
        assert_eq!(model, TARGET_MODEL);
        let id = event_loop.sessions[index].id();
        let identity = event_loop.sessions[index].handles.identity();
        assert_eq!(id != source_id, fresh);
        let run = event_loop.sessions[index].app.run_id;
        event_loop.dispatch(index, vec![Action::CancelAgent { run_id: run }]);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
        });
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(Arc::ptr_eq(
            &identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_eq!(event_loop.sessions[index].app.state.mode, Mode::Build);
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            TARGET_MODEL
        );
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert!(event_loop.sessions[index].app.state.plan.path().is_none());
        assert!(recorded.is_empty());
        assert!(requests.is_empty());
        drop(release);
    });
}

#[test]
fn fresh_activation_failure_releases_candidate_resources() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let source_id = event_loop.sessions[index].id();
        let source_identity = event_loop.sessions[index].handles.identity();
        let targets = event_loop.ctx.command_runtime.registry.target_count();
        let run = event_loop.sessions[index].app.run_id;
        approve(event_loop, index, true);
        let ready = final_ready(event_loop, true, &requests);
        let InternalEvent::PlanApprovalReady {
            result: Ok(step), ..
        } = &ready
        else {
            panic!("expected prepared candidate");
        };
        let ApprovalStep::Fresh(fresh) = step else {
            panic!("expected fresh step");
        };
        let candidate = &fresh.candidate;
        let target = candidate.app.session_id();
        let (manager, root) = candidate.handles.manager_and_root();
        let ticket = fresh.ticket.clone();
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
        assert_preserved(
            event_loop,
            index,
            &path,
            run,
            Some(&SessionCoordinatorError::DuplicateSession(target).to_string()),
        );
        assert_eq!(event_loop.sessions[index].id(), source_id);
        assert!(Arc::ptr_eq(
            &source_identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_eq!(
            event_loop.ctx.command_runtime.registry.target_count(),
            targets
        );
        pump_until(event_loop, |_| {
            ticket.peek().is_some() && manager.runner_finished(root).unwrap()
        });
        assert!(!session_log(event_loop, target).exists());
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
        assert!(SessionMailbox::notify(target, RESPONSE.into(), false).is_err());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn captured_plan_survives_later_edits(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        approve(event_loop, index, fresh);
        let ready = next_ready(event_loop);
        fs::write(&path, MUTATED_PLAN).unwrap();
        event_loop.handle_internal(ready);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        let (_, _, messages, system) = requests.recv_timeout(WAIT).unwrap();
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
            input.contains(PLAN.trim()),
            "approved content missing from implementation input"
        );
        assert!(!input.contains(MUTATED_PLAN.trim()));
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn inflight_heartbeat_does_not_abort(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
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
        entry.recv_timeout(WAIT).unwrap();
        approve(event_loop, index, fresh);
        let ready = final_ready(event_loop, fresh, &requests);
        event_loop.handle_internal(ready);
        assert!(
            event_loop.sessions[index].app.plan_approval_pending
                || event_loop.sessions[index].app.state.mode == Mode::Build
        );
        release.send(()).unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(WAIT).unwrap();
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn lock_lost_while_waiting_keeps_lock_lost_message(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        start_runtime_heartbeat_with(
            &mut event_loop.sessions[index],
            &event_loop.internal_tx,
            move |lease| {
                entered.send(()).unwrap();
                gate.recv().unwrap();
                (lease, Ok(session_lock::LockBeat::Lost))
            },
        );
        entry.recv_timeout(WAIT).unwrap();
        approve(event_loop, index, fresh);
        let ready = final_ready(event_loop, fresh, &requests);
        event_loop.handle_internal(ready);
        assert!(event_loop.sessions[index].app.plan_approval_pending);
        release.send(()).unwrap();
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        assert_eq!(
            event_loop.sessions[index].app.status_bar.flash_text(),
            Some(LOCK_LOST_MSG)
        );
        assert!(requests.is_empty());
    });
}

#[test]
fn unavailable_agent_graph_is_not_reported_busy() {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let (manager, _) = event_loop.sessions[index].handles.manager_and_root();
        shutdown_manager(&manager);
        approve(event_loop, index, false);
        let expected = idle_error_message(ManagerError::GraphShutdown);
        assert_ne!(expected, APPROVAL_BUSY);
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert_eq!(
            event_loop.sessions[index].app.status_bar.flash_text(),
            Some(expected.as_str())
        );
        assert!(requests.is_empty());
    });
}

#[test]
fn fresh_uses_finalized_history_and_captured_absolute_path() {
    with_event_loop(|event_loop| {
        let (index, original_path, requests, _host) = setup(event_loop);
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let cwd = coordinator.read().cwd().join(MOVED_CWD);
        fs::create_dir(&cwd).unwrap();
        smol::block_on(coordinator.change_directory(cwd.clone())).unwrap();
        let lease = smol::block_on(coordinator.acquire_lease()).unwrap();
        let committer = lease.committer().unwrap();
        smol::block_on(committer.commit_history(vec![Message::user(RESPONSE.into())])).unwrap();
        drop(lease);
        let relative = PathBuf::from(RELATIVE_PLAN_FILE);
        let absolute = cwd.join(&relative);
        fs::rename(original_path, &absolute).unwrap();
        Arc::make_mut(&mut event_loop.sessions[index].app.state.session).cwd =
            cwd.display().to_string();
        event_loop.sessions[index].app.state.plan = PlanState::Ready(relative.clone());
        approve(event_loop, index, true);
        let ready = next_ready(event_loop);
        if let InternalEvent::PlanApprovalReady {
            result: Ok(step), ..
        } = &ready
            && let ApprovalStep::Captured(prepared) = step
        {
            assert_eq!(prepared.source.snapshot().cwd(), cwd);
            assert_eq!(
                prepared.source.snapshot().history()[0].user_text(),
                Some(RESPONSE)
            );
            assert_eq!(prepared.plan.path, absolute);
        } else {
            panic!("fresh source capture failed");
        }
        event_loop.handle_internal(ready);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.state.mode == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        let (_, _, messages, _) = requests.recv_timeout(WAIT).unwrap();
        assert!(
            !messages
                .iter()
                .any(|message| message.user_text() == Some(HISTORY)
                    || message.user_text() == Some(RESPONSE))
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
fn fresh_stale_cwd_rejects_relative_path() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let cwd = coordinator.read().cwd().join(MOVED_CWD);
        fs::create_dir(&cwd).unwrap();
        smol::block_on(coordinator.change_directory(cwd)).unwrap();
        let relative = PathBuf::from(path.file_name().unwrap());
        event_loop.sessions[index].app.state.plan = PlanState::Ready(relative.clone());
        approve(event_loop, index, true);
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        assert_preserved(event_loop, index, &relative, run, Some(APPROVAL_CHANGED));
        assert!(requests.is_empty());
    });
}

#[test]
fn fresh_permission_change_before_activation_preserves_source() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        approve(event_loop, index, true);
        let ready = final_ready(event_loop, true, &requests);
        event_loop.sessions[index].app.permissions.toggle_yolo();
        event_loop.handle_internal(ready);
        assert_preserved(event_loop, index, &path, run, Some(APPROVAL_CHANGED));
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn provider_preparation_failure_preserves_plan(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        event_loop.ctx.prepare_provider = Arc::new(|_, _| Err(PROVIDER_FAILURE.into()));
        approve(event_loop, index, fresh);
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        assert_preserved(event_loop, index, &path, run, Some(PROVIDER_FAILURE));
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(requests.is_empty());
    });
}

#[test]
fn cancel_during_provider_preparation_ignores_late_result() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
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
                provider: Arc::new(RecordingProvider {
                    requests: record.clone(),
                    release: None,
                }),
            })
        });
        approve(event_loop, index, false);
        entry.recv_timeout(WAIT).unwrap();
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        let cancelled = next_ready(event_loop);
        event_loop.handle_internal(cancelled);
        assert_preserved(event_loop, index, &path, run, None);
        release.send(()).unwrap();
        completion.recv_timeout(WAIT).unwrap();
        event_loop.submit_text(index, RESPONSE.into()).unwrap();
        let (model, _, _, _) = requests.recv_timeout(WAIT).unwrap();
        assert_eq!(model, SOURCE_MODEL);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
        });
        assert!(requests.is_empty());
        assert!(late_requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn cancel_before_ready_preserves_plan(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        approve(event_loop, index, fresh);
        let ready = next_ready(event_loop);
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        event_loop.handle_internal(ready);
        let _ = event_loop.tick();
        assert_preserved(event_loop, index, &path, run, None);
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn save_failure_retains_committed_selection(fresh: bool) {
    with_event_loop(|event_loop| {
        let (warnings, warning_rx) = flume::unbounded();
        event_loop.ctx.storage_writer =
            Arc::new(StorageWriter::new(event_loop.ctx.storage.clone(), warnings));
        let (index, _, requests, _host) = setup(event_loop);
        approve(event_loop, index, fresh);
        pump_until(event_loop, |event_loop| {
            let app = &event_loop.sessions[index].app;
            app.state.mode == Mode::Build && app.status == Status::Idle
        });
        requests.recv_timeout(WAIT).unwrap();
        let id = event_loop.sessions[index].id();
        checkpoint(event_loop, index);
        let log = session_log(event_loop, id);
        let moved = log.with_extension(SESSION_LOG_BACKUP_EXTENSION);
        fs::rename(&log, &moved).unwrap();
        fs::create_dir(&log).unwrap();
        event_loop.sessions[index].app.state.session = Arc::new({
            let mut session = (*event_loop.sessions[index].app.state.session).clone();
            session.set_title(RESPONSE.into());
            session
        });
        event_loop.sessions[index].app.checkpoint_now();
        let warning = warning_rx.recv_timeout(WAIT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
        assert_eq!(event_loop.sessions[index].id(), id);
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            TARGET_MODEL
        );
        assert_eq!(event_loop.sessions[index].app.state.mode, Mode::Build);
        fs::remove_dir(&log).unwrap();
        fs::rename(&moved, &log).unwrap();
        event_loop.sessions[index].app.checkpoint_now();
        checkpoint(event_loop, index);
        assert_eq!(
            AppSession::load(id, &event_loop.ctx.storage).unwrap().model,
            TARGET_MODEL
        );
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn without_override_uses_actor_predecessor(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let (record, recorded) = flume::unbounded();
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        apply_patch(
            &actor,
            ConfigPatch {
                thinking: Some(DomainThinkingConfig::Off),
                fast: Some(false),
                ..model_patch(
                    TARGET_MODEL,
                    Arc::new(RecordingProvider {
                        requests: record,
                        release: None,
                    }),
                )
            },
        );
        drop(smol::block_on(event_loop.sessions[index].coordinator.acquire_lease()).unwrap());
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            SOURCE_MODEL
        );
        event_loop.ctx.prepare_provider =
            Arc::new(|_, _| panic!("no-override approval must reuse actor provider"));
        choose_form_row(event_loop, index, USE_CURRENT_ROW, false);
        assert_eq!(
            event_loop.sessions[index]
                .app
                .plan_form
                .implementation_model(),
            None
        );
        approve(event_loop, index, fresh);
        let ready = next_ready(event_loop);
        if let InternalEvent::PlanApprovalReady {
            result: Err(error), ..
        } = &ready
        {
            panic!("no-override preparation failed: {error}");
        }
        event_loop.handle_internal(ready);
        pump_until(event_loop, |event_loop| {
            let app = &event_loop.sessions[index].app;
            !app.plan_approval_pending && app.status == Status::Idle
        });
        let (model, options, _, _) = recorded.recv_timeout(WAIT).unwrap();
        assert_eq!(model, TARGET_MODEL);
        assert_eq!(
            options,
            RequestOptions {
                thinking: DomainThinkingConfig::Off,
                fast: false
            }
        );
        assert!(recorded.is_empty());
        assert!(requests.is_empty());
        assert_eq!(
            event_loop.sessions[index].app.state.model.spec(),
            TARGET_MODEL
        );
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn busy_rejection_preserves_active_turn(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let (active_requests, active_rx) = flume::unbounded();
        let (release, gate) = flume::bounded(1);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        apply_patch(
            &actor,
            model_patch(
                SOURCE_MODEL,
                Arc::new(RecordingProvider {
                    requests: active_requests,
                    release: Some(gate),
                }),
            ),
        );
        event_loop.submit_text(index, HISTORY.into()).unwrap();
        active_rx.recv_timeout(WAIT).unwrap();
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        let config = actor.effective_config().unwrap();
        approve(event_loop, index, fresh);
        assert_eq!(event_loop.sessions[index].id(), id);
        assert_eq!(event_loop.sessions[index].app.run_id, run);
        assert_eq!(event_loop.sessions[index].app.status, Status::Streaming);
        assert_eq!(event_loop.sessions[index].app.state.mode, Mode::Plan);
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert_eq!(
            event_loop.sessions[index]
                .app
                .plan_form
                .implementation_model(),
            Some(TARGET_MODEL)
        );
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert_eq!(
            event_loop.sessions[index].app.status_bar.flash_text(),
            Some(APPROVAL_BUSY)
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(requests.is_empty());
        release.send(()).unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
        });
        assert!(active_rx.is_empty());
    });
}
