use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crossterm::event::{Event, KeyCode, KeyEvent};
use maki_agent::actor::{
    ActorEvent, ActorStatus, AgentActorHandle, ConfigChange, ConfigCommit, ConfigPatch,
    PreparedModel,
};
use maki_agent::config_change::ConfigChangeError;
use maki_agent::manager::ManagerError;
use maki_agent::tools::ToolRegistry;
use maki_agent::{AgentInput, AgentMode};
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
    EventLoop, InternalEvent, LOCK_LOST_MSG, PreparedProvider, Wake, reserve_config_change,
    start_runtime_heartbeat_with,
};
use super::ApprovalStep;
use crate::AppSession;
use crate::app::mode::{Mode, PlanState};
use crate::components::keybindings::key;
use crate::components::{self, Action, Status};
use crate::plan_approval::{
    APPROVAL_BUSY, APPROVAL_CHANGED, IMPLEMENT_PARALLEL_HINT, idle_error_message, read_plan,
};
use crate::storage_writer::{SAVE_FAILED_PREFIX, StorageWriter};

const SOURCE_MODEL: &str = "anthropic/claude-opus-4-6";
const TARGET_MODEL: &str = "openai/gpt-5";
const HISTORY: &str = "existing planning conversation";
const PLAN: &str = "# Implementation\n\nImplement the regression tests.\n";
const RESPONSE: &str = "implementation finished";
const PROVIDER_FAILURE: &str = "approval provider preparation failed";
const WAIT: Duration = Duration::from_secs(10);
const CHILD_COMMAND: &str = "/approval-child";
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

fn child_tool(auto_deliver: bool) -> String {
    format!(
        r#"
    local sessions = {{}}
    maki.api.register_tool({{
        name = "{CHILD_TOOL_NAME}", description = "start test child", kind = "read",
        schema = {{ type = "object", properties = {{}} }},
        audiences = {{ "main" }},
        handler = function(_, ctx)
            local child, err = maki.agent.spawn(ctx, {{ name = "approval child", system = "child", tools = maki.json.decode('[]'), auto_deliver = {auto_deliver} }})
            if err then return {{ llm_output = err, is_error = true }} end
            sessions[#sessions + 1] = child
            local ticket, send_err = assert(child:send(ctx, "{CHILD_PROMPT}", {{ when = "idle" }})):wait(ctx)
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
    local modes, mode = 0, "plan"
    maki.api.create_autocmd("ConfigChanged", {{ callback = function(ev)
        if ev.data.parent_id or ev.data.mode == mode then
            return
        end
        mode = ev.data.mode
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
        system: &'a str,
        _tools: &'a serde_json::Value,
        _events: &'a flume::Sender<ProviderEvent>,
        _options: RequestOptions,
        _session: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let child = system == "child"
                || messages.iter().filter_map(Message::user_text).next_back() == Some(CHILD_PROMPT);
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

#[test_case(None; "active_child_rejected")]
#[test_case(Some(false); "late_child_result_after_abort")]
#[test_case(Some(true); "late_child_result_after_drop")]
fn rejects_active_managed_child_without_cancelling_it(drop_preparation: Option<bool>) {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) =
            setup_with_registry(event_loop, Arc::clone(ToolRegistry::global_arc()));
        host.load_source(
            CHILD_PLUGIN,
            &format!(
                r#"
            local sessions = {{}}
            maki.api.register_command({{
                name = "{CHILD_COMMAND}", tui_only = false,
                handler = function(_, ctx)
                    local child, err = maki.agent.spawn(ctx, {{
                        name = "approval child", system = "child",
                        tools = maki.json.decode('[]'), inherit_provider = true,
                        auto_deliver = {auto_deliver},
                    }})
                    assert(child, err)
                    sessions[#sessions + 1] = child
                    local ticket, send_err = assert(child:send(ctx, "{CHILD_PROMPT}", {{ when = "idle" }})):wait(ctx)
                    assert(ticket, send_err)
                end,
            }})
        "#,
                auto_deliver = drop_preparation.is_some()
            ),
        )
        .unwrap();
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
            root_requests: AtomicUsize::new(1),
        });
        apply_patch(&actor, model_patch(SOURCE_MODEL, provider.clone()));
        event_loop.set_focused(index);
        event_loop.sync_orchestration();
        let registry = event_loop.ctx.command_runtime.registry.clone();
        let target = event_loop.sessions[index].app.command_target.clone();
        smol::spawn(registry.dispatch_input(&target, CHILD_COMMAND.into())).detach();
        entry.recv_timeout(WAIT).unwrap();
        pump_until(event_loop, |_| actor.snapshot().status == ActorStatus::Idle);
        assert_eq!(event_loop.sessions[index].app.status, Status::Idle);
        let children: Vec<_> = manager
            .snapshot()
            .into_iter()
            .filter(|node| node.parent_id == Some(root))
            .collect();
        assert_eq!(children.len(), 1);
        let child = manager.actor(children[0].agent_id).unwrap();
        if let Some(drop_preparation) = drop_preparation {
            let initial_calls = provider.root_requests.load(Ordering::SeqCst);
            release.send(()).unwrap();
            // An Idle status shows before a turn's outcome is published, and
            // approval refuses a subtree with a publication still pending.
            pump_until(event_loop, |event_loop| {
                provider.root_requests.load(Ordering::SeqCst) == initial_calls + 1
                    && manager.prepare_idle_subtree(root).is_ok()
                    && event_loop.sessions[index].handles.agent_rx.is_empty()
                    && event_loop.sessions[index].app.status == Status::Idle
            });
            approve(event_loop, index, true);
            let prepared = next_ready(event_loop);
            let InternalEvent::PlanApprovalReady {
                result: Ok(ApprovalStep::Prepared(source)),
                ..
            } = &prepared
            else {
                panic!("late-child source was not captured");
            };
            let frozen = Arc::clone(&source.source.history.messages);
            let calls = provider.root_requests.load(Ordering::SeqCst);
            let ticket = child
                .admit_turn(
                    AgentInput::new(CHILD_PROMPT.into(), Vec::new()),
                    None,
                    CHILD_PROMPT.into(),
                )
                .unwrap();
            entry.recv_timeout(WAIT).unwrap();
            release.send(()).unwrap();
            smol::block_on(ticket.wait());
            pump_until(event_loop, |_| child.snapshot().status == ActorStatus::Idle);
            assert_eq!(provider.root_requests.load(Ordering::SeqCst), calls);
            let source = actor.prepare_idle();
            assert!(source.is_err(), "approval source guard was released early");
            let InternalEvent::PlanApprovalReady {
                result: Ok(ApprovalStep::Prepared(source)),
                ..
            } = &prepared
            else {
                unreachable!();
            };
            assert!(Arc::ptr_eq(
                &frozen,
                &source.idle.source_snapshot().unwrap().history.messages
            ));
            if drop_preparation {
                drop(prepared);
                event_loop.sessions[index].end_plan_approval();
            } else {
                event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
                event_loop.handle_internal(prepared);
            }
            pump_until(event_loop, |event_loop| {
                provider.root_requests.load(Ordering::SeqCst) > calls
                    && actor.prepare_idle().is_ok()
                    && event_loop.sessions[index].app.status == Status::Idle
            });
            assert_eq!(provider.root_requests.load(Ordering::SeqCst), calls + 1);
            assert_eq!(event_loop.sessions[index].app.root_mode(), Mode::Plan);
            assert!(requests.is_empty());
            shutdown_manager(&manager);
            return;
        }
        let child_before = child.snapshot();
        assert!(matches!(child_before.status, ActorStatus::Running(_)));
        let history = actor.committed_history();
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
        assert!(Arc::ptr_eq(&history, &actor.committed_history()));
        assert_eq!(read_recents(&event_loop.ctx.storage), recent);
        assert_eq!(read_model(&event_loop.ctx.storage), saved_model);
        assert_eq!(event_loop.sessions[index].app.root_mode(), Mode::Plan);
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
        assert!(matches!(
            child.snapshot().latest,
            Some(maki_agent::TurnOutcome::Completed { turn_id, .. })
                if Some(turn_id) == child_before.active_turn
        ));
        shutdown_manager(&manager);
    });
}

#[test_case(false; "keep_context")]
#[test_case(true; "clear_context")]
fn historical_child_approval_never_falls_back_to_root(clear_context: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = actor.committed_history();
        let run_id = event_loop.sessions[index].app.run_id;
        let app = &mut event_loop.sessions[index].app;
        let mut child = crate::chat::Chat::new(
            CHILD_PROMPT.into(),
            app.ui_config.clone(),
            app.lua_event_handle.clone(),
            Arc::clone(&app.theme_provider),
        );
        child.agent_id = Some(maki_agent::AgentId::generate());
        child.subagent_id = Some(CHILD_PROMPT.into());
        app.chats.push(child);
        app.active_chat = 1;
        event_loop.prepare_plan_approval(index, clear_context, None, false, path.clone());
        assert!(event_loop.sessions[index].pending_approval.is_none());
        assert_eq!(event_loop.sessions[index].app.run_id, run_id);
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(&history, &actor.committed_history()));
        assert!(requests.is_empty());
        shutdown_manager(&manager);
    });
}

#[test_case(false, false, false, false, false; "keep_context")]
#[test_case(true, false, false, false, false; "clear_context")]
#[test_case(true, true, false, false, false; "checkpoint_failure")]
#[test_case(true, false, true, false, false; "checkpoint_deadline")]
#[test_case(true, true, true, false, false; "checkpoint_deadline_with_late_failure")]
#[test_case(true, false, true, true, false; "checkpoint_escape")]
#[test_case(true, false, true, false, true; "checkpoint_lock_loss")]
#[test_case(false, false, false, false, true; "keep_context_lock_loss")]
fn managed_child_approval_is_pinned(
    clear_context: bool,
    fail_checkpoint: bool,
    delay_checkpoint: bool,
    escape: bool,
    lose_lock: bool,
) {
    const CHECKPOINT_FAILURE: &str = "child approval checkpoint failed";
    const CHECKPOINT_FLASH: &str = "history checkpoint failed: child approval checkpoint failed";
    with_event_loop(|event_loop| {
        let (index, path, requests, host) =
            setup_with_registry(event_loop, Arc::clone(ToolRegistry::global_arc()));
        host.load_source(CHILD_PLUGIN, &child_tool(false)).unwrap();
        event_loop.sessions[index]
            .app
            .permissions
            .set_session_yolo(Some(true));
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let root_actor = manager.actor(root).unwrap();
        let mode = AgentMode::Build;
        let reservation = reserve_config_change(
            &manager.lookup(root).unwrap(),
            &event_loop.sessions[index].coordinator,
            None,
        )
        .unwrap();
        smol::block_on(reservation.commit_change(ConfigChange::Mode {
            mode: mode.clone(),
            mode_def: Some(host.event_handle().mode_registry().current(&mode)),
        }))
        .unwrap();
        let (entered, entry) = flume::bounded(1);
        let (release, gate) = flume::bounded(1);
        apply_patch(
            &root_actor,
            model_patch(
                SOURCE_MODEL,
                Arc::new(ChildProvider {
                    entered,
                    release: gate,
                    root_requests: AtomicUsize::new(0),
                }),
            ),
        );
        event_loop.submit_text(index, HISTORY.into()).unwrap();
        let entry_deadline = Instant::now() + WAIT;
        pump_until(event_loop, |event_loop| {
            if let Some(maki_agent::TurnOutcome::Failed { failure, .. }) =
                root_actor.snapshot().latest
            {
                panic!(
                    "root failed before pinned child start: {}",
                    failure.diagnostic
                );
            }
            let history = root_actor.committed_history();
            for message in history.iter() {
                for block in &message.content {
                    if let ContentBlock::ToolResult {
                        is_error: true,
                        content,
                        ..
                    } = block
                    {
                        panic!("pinned child tool failed before provider start: {content:?}");
                    }
                }
            }
            assert!(
                !entry.is_empty() || Instant::now() < entry_deadline,
                "pinned child provider not entered: root={:?}, app={:?}, graph={:?}, history={history:?}",
                root_actor.snapshot(),
                event_loop.sessions[index].app.status,
                manager.snapshot()
            );
            !entry.is_empty()
        });
        entry.recv_timeout(WAIT).unwrap();
        let child_id = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root))
            .unwrap()
            .agent_id;
        let child = manager.actor(child_id).unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index]
                .app
                .chats
                .iter()
                .any(|chat| chat.agent_id == Some(child_id))
        });
        let committed_messages = child.committed_message_count();
        assert!(committed_messages > 0);
        let child_chat = event_loop.sessions[index]
            .app
            .chats
            .iter()
            .find(|chat| chat.agent_id == Some(child_id))
            .unwrap();
        let child_key = child_chat.subagent_id.clone().unwrap();
        let info = maki_agent::SubagentInfo {
            agent_id: child_id,
            parent_agent_id: Some(root),
            parent_tool_use_id: child_key.clone(),
            name: CHILD_TOOL_NAME.into(),
            prompt: None,
            model: None,
            opts: None,
            answer_tx: None,
            cancel: None,
        };
        event_loop.handle_agent(
            index,
            Box::new(maki_agent::Envelope {
                event: maki_agent::AgentEvent::Post(maki_domain::Post {
                    text: CHILD_PROMPT.into(),
                    level: maki_domain::PostLevel::Info,
                    agent: None,
                }),
                subagent: Some(info),
                identity: None,
            }),
        );
        assert_eq!(
            event_loop.sessions[index]
                .app
                .state
                .session
                .posts()
                .iter()
                .find(|post| post.chat.as_deref() == Some(child_key.as_str()))
                .unwrap()
                .after_message,
            committed_messages,
        );
        let child_events = child.subscribe_events();
        release.send(()).unwrap();
        let idle_deadline = Instant::now() + WAIT;
        loop {
            let event = child_events
                .recv_timeout(idle_deadline.saturating_duration_since(Instant::now()))
                .expect("pinned child setup did not become idle");
            if matches!(event, ActorEvent::Idle { agent_id, .. } if agent_id == child_id) {
                break;
            }
        }
        let settled_deadline = Instant::now() + WAIT;
        pump_until(event_loop, |event_loop| {
            let settled = child.snapshot().status == ActorStatus::Idle
                && root_actor.snapshot().status == ActorStatus::Idle
                && event_loop.sessions[index].app.status == Status::Idle;
            assert!(
                settled || Instant::now() < settled_deadline,
                "pinned child setup did not settle: root={:?}, child={:?}, app={:?}",
                root_actor.snapshot(),
                child.snapshot(),
                event_loop.sessions[index].app.status
            );
            settled
        });
        let mode = AgentMode::Plan(path.clone());
        let ticket = child.reserve_prepared_operation(None).unwrap();
        smol::block_on(ticket.commit_when_ready(Some(ConfigChange::Mode {
            mode: mode.clone(),
            mode_def: Some(host.event_handle().mode_registry().current(&mode)),
        })))
        .unwrap();
        let config = child.effective_config().unwrap();
        let child_epoch = child.history_boundaries().unwrap()[0].epoch;
        assert!(
            event_loop.sessions[index]
                .app
                .state
                .session
                .subagents()
                .iter()
                .any(|stored| stored.tool_use_id == child_key),
            "selected child metadata missing from app snapshot"
        );
        event_loop.sessions[index].app.checkpoint_now();
        let root_config = root_actor.effective_config().unwrap();
        let root_history = root_actor.committed_history();
        let session = event_loop.sessions[index].id();
        let root_run = event_loop.sessions[index].app.run_id;
        let child_chat = event_loop.sessions[index]
            .app
            .chats
            .iter()
            .position(|chat| chat.agent_id == Some(child_id))
            .unwrap();
        event_loop.sessions[index].app.active_chat = child_chat;
        event_loop.sessions[index].app.sync_focused_queue();
        event_loop.sync_orchestration();
        event_loop.prepare_plan_approval(
            index,
            clear_context,
            Some(TARGET_MODEL.into()),
            false,
            path.clone(),
        );
        assert!(
            event_loop.sessions[index].pending_approval.is_some(),
            "child approval rejected before preparation: {:?}",
            event_loop.sessions[index].app.status_bar.flash_text()
        );
        let mut ready = next_ready(event_loop);
        while let Ok(envelope) = event_loop.sessions[index].handles.agent_rx.try_recv() {
            event_loop.handle_agent(index, Box::new(envelope));
        }
        let (checkpoint_release, checkpoint_gate) = flume::bounded(1);
        let (checkpoint_entered, checkpoint_entry) = flume::bounded(1);
        if fail_checkpoint || delay_checkpoint {
            let InternalEvent::PlanApprovalReady {
                result: Ok(ApprovalStep::Prepared(prepared)),
                ..
            } = &mut ready
            else {
                panic!("child preparation failed");
            };
            let publish = std::mem::replace(
                &mut prepared.transaction.publish,
                Box::new(|_, _| panic!("publication callback was not restored")),
            );
            prepared.transaction.publish = Box::new(move |snapshot, publication| {
                Box::pin(async move {
                    if delay_checkpoint {
                        checkpoint_entered.send(()).unwrap();
                        publication.cancelled().await;
                        checkpoint_gate
                            .recv_async()
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    if fail_checkpoint {
                        Err(CHECKPOINT_FAILURE.into())
                    } else {
                        publish(snapshot, publication).await
                    }
                })
            });
        }
        event_loop.sessions[index].app.active_chat = 0;
        event_loop.sessions[index].app.sync_focused_queue();
        if lose_lock && !clear_context {
            let InternalEvent::PlanApprovalReady {
                result: Ok(ApprovalStep::Prepared(prepared)),
                ..
            } = ready
            else {
                panic!("child preparation failed");
            };
            let publication = prepared.transaction.publication.clone();
            event_loop.sessions[index]
                .pending_approval
                .as_mut()
                .unwrap()
                .committing = true;
            super::super::mark_runtime_lock_lost(&mut event_loop.sessions[index]);
            assert!(publication.is_cancelled());
            let result = smol::block_on(prepared.idle.commit_prepared_transaction_held(
                prepared.operation,
                None,
                prepared.transaction,
            ));
            assert!(result.is_err());
            event_loop.complete_plan_approval(index, Err(LOCK_LOST_MSG.into()));
            assert!(event_loop.sessions[index].pending_approval.is_none());
            drop(child.prepare_idle().expect(LOCK_LOST_MSG));
            assert!(Arc::ptr_eq(&config, &child.effective_config().unwrap()));
            assert_eq!(child.history_boundaries().unwrap()[0].epoch, child_epoch);
            assert!(requests.is_empty());
            shutdown_manager(&manager);
            return;
        }
        if delay_checkpoint {
            let InternalEvent::PlanApprovalReady {
                result: Ok(ApprovalStep::Prepared(prepared)),
                ..
            } = ready
            else {
                panic!("child preparation failed");
            };
            let (expire, expiration) = flume::bounded(1);
            event_loop.commit_plan_approval_with_deadline(index, *prepared, async move {
                expiration.recv_async().await.unwrap();
            });
            checkpoint_entry.recv_timeout(WAIT).unwrap();
            if escape || lose_lock {
                if lose_lock {
                    super::super::mark_runtime_lock_lost(&mut event_loop.sessions[index]);
                } else {
                    event_loop.sessions[index].end_plan_approval();
                }
            } else {
                expire.send(()).unwrap();
            }
            checkpoint_release.send(()).unwrap();
            let deadline = next_ready(event_loop);
            assert!(matches!(
                &deadline,
                InternalEvent::PlanApprovalReady {
                    result: Err(error),
                    ..
                } if escape || lose_lock || error == super::APPROVAL_CHECKPOINT_TIMEOUT
            ));
            event_loop.handle_internal(deadline);
            assert!(event_loop.sessions[index].pending_approval.is_none());
            assert!(!event_loop.sessions[index].app.plan_approval_pending);
            drop(
                child
                    .prepare_idle()
                    .expect("deadline must release actor reservation"),
            );
            assert!(Arc::ptr_eq(&config, &child.effective_config().unwrap()));
            assert_eq!(child.history_boundaries().unwrap()[0].epoch, child_epoch);
            assert!(requests.is_empty());
            drop(checkpoint_release);
            event_loop.sessions[index].app.active_chat = child_chat;
            event_loop.sessions[index].app.sync_focused_queue();
            assert!(!event_loop.sessions[index].app.plan_approval_pending);
            if lose_lock {
                assert_eq!(
                    event_loop.sessions[index].app.status_bar.flash_text(),
                    Some(LOCK_LOST_MSG)
                );
                shutdown_manager(&manager);
                return;
            }
            event_loop.sessions[index].app.plan_approval_pending = true;
            event_loop.sessions[index]
                .app
                .plan_form
                .set_implementation_model(TARGET_MODEL.into(), SOURCE_MODEL);
            event_loop.prepare_plan_approval(
                index,
                clear_context,
                Some(TARGET_MODEL.into()),
                false,
                path.clone(),
            );
            assert!(event_loop.sessions[index].pending_approval.is_some());
            let retry = next_ready(event_loop);
            event_loop.handle_internal(retry);
        } else {
            event_loop.handle_internal(ready);
        }
        assert!(
            event_loop.sessions[index]
                .pending_approval
                .as_ref()
                .is_some_and(|pending| pending.committing),
            "child commit did not start: {:?}",
            event_loop.sessions[index].app.status_bar.flash_text(),
        );
        assert!(event_loop.sessions[index].pending_approval.is_some());
        let committed = next_ready(event_loop);
        let fail_checkpoint = fail_checkpoint && !delay_checkpoint;
        if !fail_checkpoint {
            if let InternalEvent::PlanApprovalReady {
                result: Err(error), ..
            } = &committed
            {
                panic!("child commit failed: {error}");
            }
            event_loop.sessions[index].end_plan_approval();
            assert!(event_loop.sessions[index].pending_approval.is_some());
            assert!(requests.is_empty());
            assert!(matches!(child.snapshot().status, ActorStatus::Idle));
            while let Ok(envelope) = event_loop.sessions[index].handles.agent_rx.try_recv() {
                assert!(!matches!(
                    envelope.event,
                    maki_agent::AgentEvent::TextDelta { .. }
                        | maki_agent::AgentEvent::TurnComplete(_)
                        | maki_agent::AgentEvent::ToolStart(_)
                ));
                event_loop.handle_agent(index, Box::new(envelope));
            }
            assert!(requests.is_empty());
        }
        event_loop.handle_internal(committed);
        assert!(event_loop.sessions[index].pending_approval.is_none());
        if fail_checkpoint {
            let idle = child
                .prepare_idle()
                .expect("failed checkpoint must release actor reservation");
            drop(idle);
            assert!(Arc::ptr_eq(&config, &child.effective_config().unwrap()));
            assert_eq!(child.history_boundaries().unwrap()[0].epoch, child_epoch);
            assert!(requests.is_empty());
            assert_eq!(
                event_loop.sessions[index].app.status_bar.flash_text(),
                Some(CHECKPOINT_FLASH)
            );
        } else {
            assert_eq!(
                event_loop.sessions[index].app.chats[child_chat].last_message_text(),
                read_plan(path.clone(), false).unwrap().message,
            );
            pump_until(event_loop, |_| !requests.is_empty());
            let (model, _, messages, _) = requests.recv_timeout(WAIT).unwrap();
            assert_eq!(model, TARGET_MODEL);
            assert_eq!(
                messages
                    .iter()
                    .any(|message| message.user_text() == Some(CHILD_PROMPT)),
                !clear_context
            );
            assert_eq!(child.effective_config().unwrap().mode, AgentMode::Build);
        }
        assert!(Arc::ptr_eq(
            &root_config,
            &root_actor.effective_config().unwrap()
        ));
        assert!(Arc::ptr_eq(&root_history, &root_actor.committed_history()));
        assert_eq!(event_loop.sessions[index].id(), session);
        assert_eq!(event_loop.sessions[index].app.run_id, root_run);
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
        maki_lua::ui_sink::HostUi::Frontend,
    )
    .unwrap();
    host.event_handle()
        .install_orchestration_services(event_loop.orchestration.clone());
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

fn apply_patch(actor: &AgentActorHandle, patch: ConfigPatch) -> ConfigCommit {
    let ticket = actor.reserve_prepared_operation(None).unwrap();
    smol::block_on(ticket.commit_when_ready(Some(ConfigChange::Patch(patch))))
        .unwrap()
        .config
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

fn checkpoint(event_loop: &mut EventLoop<'_>, index: usize) {
    event_loop.sessions[index].app.checkpoint_now();
    let runtime = &event_loop.sessions[index];
    smol::block_on(event_loop.ctx.storage_writer.checkpoint(CheckpointRequest {
        session_id: runtime.id(),
        version: CheckpointVersion {
            revision: runtime.app.state.session.revision(),
            epoch: runtime.app.state.session.history_snapshot().epoch,
        },
        snapshot: Arc::clone(&runtime.app.state.session),
    }))
    .unwrap();
}

fn assert_preserved(
    event_loop: &EventLoop<'_>,
    host: &PluginHost,
    index: usize,
    path: &Path,
    run_id: u64,
    flash: Option<&str>,
) {
    let app = &event_loop.sessions[index].app;
    assert_eq!(app.status_bar.flash_text(), flash);
    assert_eq!(app.root_config().model.spec(), SOURCE_MODEL);
    assert_eq!(app.root_mode(), Mode::Plan);
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
    assert!(observed(event_loop, host, index, START_COMMAND_PREFIX).is_empty());
}

/// Names and descriptions of the commands the observer plugin registered
/// under `prefix`.
fn observed(
    event_loop: &EventLoop<'_>,
    host: &PluginHost,
    index: usize,
    prefix: &str,
) -> Vec<(String, String)> {
    let app = &event_loop.sessions[index].app;
    host.drain_tasks_for_test();
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

#[test_case(false, false; "existing_context")]
#[test_case(true, false; "fresh_context")]
#[test_case(false, true; "after_config_only_commit")]
fn dispatch_uses_selected_provider_and_persists_completion(fresh: bool, prior_config: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = setup(event_loop);
        if prior_config {
            let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
            let operation = manager
                .actor(root)
                .unwrap()
                .reserve_prepared_operation(None)
                .unwrap();
            operation.resolve(None).unwrap();
            smol::block_on(operation.wait_ready()).unwrap();
            operation.commit().unwrap();
        }
        let original_id = event_loop.sessions[index].id();
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
            event_loop.sessions[index].app.root_config().model.spec(),
            SOURCE_MODEL
        );
        assert!(observed(event_loop, &host, index, START_COMMAND_PREFIX).is_empty());
        event_loop.handle_internal(ready);
        pump_until(event_loop, |event_loop| {
            let runtime = &event_loop.sessions[index];
            let (manager, root) = runtime.handles.manager_and_root();
            let actor = manager.actor(root).unwrap();
            runtime.app.status == Status::Idle
                && !runtime.app.plan_approval_pending
                && actor.snapshot().status == ActorStatus::Idle
                && matches!(
                    actor.snapshot().latest,
                    Some(maki_agent::TurnOutcome::Completed { .. })
                )
                && actor.effective_config().is_some_and(|config| {
                    config.mode == AgentMode::Build && config.model.spec() == TARGET_MODEL
                })
                && !requests.is_empty()
        });
        let starts: Vec<_> = observed(event_loop, &host, index, START_COMMAND_PREFIX)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(starts, [format!("{START_COMMAND_PREFIX}1")]);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        assert_eq!(
            event_loop.sessions[index].app.run_id,
            manager.actor(root).unwrap().presentation_id()
        );
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
        assert_eq!(runtime.id(), original_id);
        assert_eq!(runtime.app.root_mode(), Mode::Build);
        assert_eq!(runtime.app.root_config().model.spec(), TARGET_MODEL);
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
    requests: &flume::Receiver<RecordedRequest>,
) -> InternalEvent {
    loop {
        let ready = next_ready(event_loop);
        assert!(requests.is_empty());
        match &ready {
            InternalEvent::PlanApprovalReady {
                result: Ok(ApprovalStep::Prepared(_)),
                ..
            } => {
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
        let (index, _, requests, host) = setup(event_loop);
        let before = observed(event_loop, &host, index, MODE_COMMAND_PREFIX).len();
        approve(event_loop, index, false);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.root_mode() == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
                && !requests.is_empty()
        });
        requests.recv_timeout(WAIT).unwrap();
        let _ = event_loop.tick();
        assert_eq!(
            observed(event_loop, &host, index, MODE_COMMAND_PREFIX).len(),
            before + 1
        );
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let commit = apply_patch(&actor, disable_fast());
        event_loop.sessions[index].project_config(&commit);
        let _ = event_loop.tick();
        assert_eq!(
            observed(event_loop, &host, index, MODE_COMMAND_PREFIX).len(),
            before + 1
        );
    });
}

#[test]
fn clear_context_preserves_runtime_and_session_state() {
    with_event_loop(|event_loop| {
        let (index, _, requests, host) = setup(event_loop);
        let source = event_loop.sessions[index].id();
        let identity = event_loop.sessions[index].handles.identity();
        let command_target = event_loop.sessions[index].app.command_target.id();
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let options = coordinator.read().options();
        let mailbox = coordinator.mailbox().unwrap();
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let actor_identity = actor.identity();
        let usage = event_loop.sessions[index].app.state.session.token_usage;
        event_loop.sessions[index]
            .app
            .input_box
            .set_input(DRAFT.into());
        event_loop.sessions[index].app.checkpoint_now();
        checkpoint(event_loop, index);
        approve(event_loop, index, true);
        let ready = final_ready(event_loop, &requests);
        event_loop.handle_internal(ready);
        assert!(requests.is_empty());
        let committed = next_ready(event_loop);
        assert!(requests.is_empty());
        event_loop.handle_internal(committed);
        assert!(
            session_lock::claim(&event_loop.sessions_dir, &source)
                .unwrap()
                .is_none()
        );
        assert_eq!(event_loop.sessions[index].id(), source);
        assert!(Arc::ptr_eq(
            &identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert!(Arc::ptr_eq(
            &actor_identity,
            &manager.actor(root).unwrap().identity()
        ));
        assert_eq!(
            event_loop.sessions[index].app.command_target.id(),
            command_target
        );
        assert_eq!(
            event_loop.sessions[index].app.input_box.buffer.value(),
            DRAFT
        );
        assert_eq!(
            event_loop.sessions[index].app.state.session.token_usage,
            usage
        );
        assert!(coordinator.mailbox().is_ok());
        drop(mailbox);
        let committed_options = coordinator.read().options();
        assert!(committed_options.version > options.version);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.root_mode() == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
        });
        requests.recv_timeout(WAIT).unwrap();
        assert!(observed(event_loop, &host, index, RESET_COMMAND).is_empty());
        assert!(
            event_loop
                .ctx
                .storage_writer
                .latest_snapshot(source)
                .is_some()
        );
        event_loop.sessions[index].app.checkpoint_now();
        checkpoint(event_loop, index);
        let restored = AppSession::load(source, &event_loop.ctx.storage).unwrap();
        assert_eq!(restored.meta.input_draft.as_deref(), Some(DRAFT));
        assert!(
            !restored
                .messages()
                .iter()
                .any(|message| message.user_text() == Some(HISTORY))
        );
        assert_eq!(restored.model, TARGET_MODEL);
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
            event_loop.sessions[index].app.root_mode() == Mode::Build && !recorded.is_empty()
        });
        let (model, _, _, _) = recorded.recv_timeout(WAIT).unwrap();
        assert_eq!(model, TARGET_MODEL);
        let id = event_loop.sessions[index].id();
        let identity = event_loop.sessions[index].handles.identity();
        assert_eq!(id, source_id);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let turn = actor.snapshot().active_turn.unwrap();
        press(event_loop, key::QUIT.to_key_event());
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
        });
        assert!(matches!(
            actor.snapshot().latest,
            Some(maki_agent::TurnOutcome::Cancelled { turn_id, .. }) if turn_id == turn
        ));
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(Arc::ptr_eq(
            &identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_eq!(event_loop.sessions[index].app.root_mode(), Mode::Build);
        assert_eq!(
            event_loop.sessions[index].app.root_config().model.spec(),
            TARGET_MODEL
        );
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert!(event_loop.sessions[index].app.state.plan.path().is_none());
        assert!(recorded.is_empty());
        assert!(requests.is_empty());
        drop(release);
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
            event_loop.sessions[index].app.root_mode() == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
                && !requests.is_empty()
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
        let ready = final_ready(event_loop, &requests);
        event_loop.handle_internal(ready);
        assert!(
            event_loop.sessions[index].app.plan_approval_pending
                || event_loop.sessions[index].app.root_mode() == Mode::Build
        );
        release.send(()).unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.root_mode() == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
                && !requests.is_empty()
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
        let ready = final_ready(event_loop, &requests);
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
fn clear_uses_actor_source_and_captured_absolute_path() {
    with_event_loop(|event_loop| {
        let (index, original_path, requests, _host) = setup(event_loop);
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let cwd = coordinator.read().cwd().unwrap().join(MOVED_CWD);
        fs::create_dir(&cwd).unwrap();
        smol::block_on(coordinator.change_directory(cwd.clone())).unwrap();
        let relative = PathBuf::from(RELATIVE_PLAN_FILE);
        let absolute = cwd.join(&relative);
        fs::rename(original_path, &absolute).unwrap();
        Arc::make_mut(&mut event_loop.sessions[index].app.state.session).cwd =
            cwd.display().to_string();
        event_loop.sessions[index].app.state.plan = PlanState::Ready(relative.clone());
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let mode = AgentMode::Plan(relative);
        let reservation =
            reserve_config_change(&manager.lookup(root).unwrap(), &coordinator, None).unwrap();
        smol::block_on(
            reservation.commit_change(ConfigChange::Mode {
                mode: mode.clone(),
                mode_def: Some(
                    event_loop
                        .ctx
                        .lua_event_handle
                        .mode_registry()
                        .current(&mode),
                ),
            }),
        )
        .unwrap();
        approve(event_loop, index, true);
        let ready = next_ready(event_loop);
        if let InternalEvent::PlanApprovalReady {
            result: Ok(step), ..
        } = &ready
            && let ApprovalStep::Prepared(prepared) = step
        {
            assert_eq!(prepared.source.config.config.context.cwd(), cwd);
            assert_eq!(
                prepared.source.history.messages[0].user_text(),
                Some(HISTORY)
            );
            assert_eq!(prepared.plan.path, absolute);
        } else {
            panic!("approval source capture failed");
        }
        event_loop.handle_internal(ready);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.root_mode() == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
                && !requests.is_empty()
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
        assert_eq!(
            event_loop.sessions[index].coordinator.read().cwd().unwrap(),
            cwd
        );
        assert!(requests.is_empty());
    });
}

#[test]
fn stale_plan_path_rejects_approval() {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        let coordinator = event_loop.sessions[index].coordinator.clone();
        let cwd = coordinator.read().cwd().unwrap().join(MOVED_CWD);
        fs::create_dir(&cwd).unwrap();
        smol::block_on(coordinator.change_directory(cwd)).unwrap();
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let source = manager.actor(root).unwrap().config_snapshot().unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index]
                .projected_config
                .as_ref()
                .map(|commit| commit.generation)
                == Some(source.generation)
        });
        let relative = PathBuf::from(path.file_name().unwrap());
        event_loop.set_focused(index);
        press(event_loop, key::PLAN_TOGGLE.to_key_event());
        event_loop.sessions[index].app.state.plan = PlanState::Ready(relative.clone());
        event_loop.dispatch(
            index,
            vec![Action::ApprovePlan {
                clear_context: true,
                model: Some(TARGET_MODEL.into()),
                parallel: true,
                path: relative.clone(),
            }],
        );
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        assert_preserved(
            event_loop,
            &host,
            index,
            &relative,
            run,
            Some(APPROVAL_CHANGED),
        );
        assert_eq!(
            manager
                .actor(root)
                .unwrap()
                .effective_config()
                .unwrap()
                .mode,
            AgentMode::Plan(path),
        );
        assert!(requests.is_empty());
    });
}

#[test]
fn permission_change_before_publication_preserves_source() {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = setup(event_loop);
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        approve(event_loop, index, true);
        let ready = final_ready(event_loop, &requests);
        event_loop.sessions[index].app.permissions.toggle_yolo();
        event_loop.handle_internal(ready);
        assert_preserved(event_loop, &host, index, &path, run, Some(APPROVAL_CHANGED));
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn provider_preparation_failure_preserves_plan(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = setup(event_loop);
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        event_loop.ctx.prepare_provider = Arc::new(|_, _| Err(PROVIDER_FAILURE.into()));
        approve(event_loop, index, fresh);
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        let failure = ConfigChangeError::ModelPreparation(PROVIDER_FAILURE.into()).to_string();
        assert_preserved(event_loop, &host, index, &path, run, Some(&failure));
        assert_eq!(event_loop.sessions[index].id(), id);
        assert!(requests.is_empty());
    });
}

#[test]
fn cancel_during_provider_preparation_ignores_late_result() {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = setup(event_loop);
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
        assert_preserved(event_loop, &host, index, &path, run, None);
        release.send(()).unwrap();
        completion.recv_timeout(WAIT).unwrap();
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        event_loop.submit_text(index, RESPONSE.into()).unwrap();
        let (model, _, _, _) = requests.recv_timeout(WAIT).unwrap();
        assert_eq!(model, SOURCE_MODEL);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
                && matches!(
                    actor.snapshot().latest,
                    Some(maki_agent::TurnOutcome::Completed { .. })
                )
        });
        assert!(requests.is_empty());
        assert!(late_requests.is_empty());
    });
}

#[test_case(false; "existing_context")]
#[test_case(true; "fresh_context")]
fn cancel_before_ready_preserves_plan(fresh: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = setup(event_loop);
        let run = event_loop.sessions[index].app.run_id;
        approve(event_loop, index, fresh);
        let ready = next_ready(event_loop);
        event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
        event_loop.handle_internal(ready);
        let _ = event_loop.tick();
        assert_preserved(event_loop, &host, index, &path, run, None);
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
            app.root_mode() == Mode::Build && app.status == Status::Idle && !requests.is_empty()
        });
        requests.recv_timeout(WAIT).unwrap();
        let id = event_loop.sessions[index].id();
        checkpoint(event_loop, index);
        let log = session_log(event_loop, id);
        let moved = log.with_extension(SESSION_LOG_BACKUP_EXTENSION);
        fs::rename(&log, &moved).unwrap();
        let (deleted, deletion) = flume::bounded(1);
        event_loop.ctx.storage_writer.delete(id, move |result| {
            deleted.send(result).unwrap();
        });
        deletion.recv_timeout(WAIT).unwrap().unwrap();
        fs::create_dir(&log).unwrap();
        event_loop.sessions[index]
            .app
            .input_box
            .set_input(DRAFT.into());
        event_loop.sessions[index].app.checkpoint_now();
        let warning = warning_rx.recv_timeout(WAIT).unwrap();
        assert!(warning.starts_with(SAVE_FAILED_PREFIX), "{warning}");
        assert_eq!(event_loop.sessions[index].id(), id);
        assert_eq!(
            event_loop.sessions[index].app.root_config().model.spec(),
            TARGET_MODEL
        );
        assert_eq!(event_loop.sessions[index].app.root_mode(), Mode::Build);
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
        choose_form_row(event_loop, index, USE_CURRENT_ROW, false);
        assert_eq!(
            event_loop.sessions[index]
                .app
                .plan_form
                .implementation_model(),
            None
        );
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
        event_loop.ctx.prepare_provider =
            Arc::new(|_, _| panic!("no-override approval must reuse actor provider"));
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
            if !app.plan_approval_pending {
                assert_eq!(
                    app.root_mode(),
                    Mode::Build,
                    "{:?}",
                    app.status_bar.flash_text()
                );
            }
            !app.plan_approval_pending
                && app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
                && matches!(
                    actor.snapshot().latest,
                    Some(maki_agent::TurnOutcome::Completed { .. })
                )
                && !recorded.is_empty()
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
            event_loop.sessions[index].app.root_config().model.spec(),
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
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Streaming
        });
        let active = actor.snapshot();
        assert!(matches!(active.status, ActorStatus::Running(_)));
        let id = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        let config = actor.effective_config().unwrap();
        approve(event_loop, index, fresh);
        assert_eq!(event_loop.sessions[index].id(), id);
        assert_eq!(event_loop.sessions[index].app.run_id, run);
        assert_eq!(event_loop.sessions[index].app.status, Status::Streaming);
        assert_eq!(event_loop.sessions[index].app.root_mode(), Mode::Plan);
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
        assert_eq!(actor.snapshot().status, active.status);
        assert_eq!(actor.snapshot().active_turn, active.active_turn);
        assert!(requests.is_empty());
        release.send(()).unwrap();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
        });
        assert!(active_rx.is_empty());
    });
}
