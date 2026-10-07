//! Non-interactive (headless) mode: `makima --print -p "prompt"`.
//!
//! Wire format intentionally matches Claude Code so existing scripts work
//! unchanged. Keep `PrintResult` fields a strict subset of theirs. `StreamJson`
//! is JSONL with the same shape, `Text` prints the raw response only.
//!
//! We adopt new fields when Claude Code adds them but never invent our own.
//! Check their docs before changing anything here.

use std::future::Future;
use std::io::{self, Read};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use color_eyre::Result;
use color_eyre::eyre::{Context, eyre};
use futures::FutureExt;
use maki_agent::command::{self, StandardCommands, StandardCompletions};
use maki_agent::headless::{HeadlessHandle, HeadlessParams};
use maki_agent::permissions::PluginRuleStore;
use maki_agent::tools::QUESTION_TOOL_NAME;
use maki_agent::tools::offload::OffloadCleanup;
use maki_agent::{
    AgentConfig, AgentEvent, AgentInput, AgentMode, DoneReason, Envelope, ImageSource,
    ModeRegistry, PermissionsConfig, TurnOutcome,
};
use maki_commands::{
    BuiltinOperation, CommandContent, CommandError, CommandFuture, CommandHost, CommandOutcome,
    CommandRegistry, HostRequest, HostResponse, InputDispatch, TargetCapabilities,
    TargetCapability,
};
use maki_config::{ModelPolicy, ProjectConfig, SessionDefaults};
use maki_lua::EventHandle;
use maki_lua::session_snapshot::{HeadlessMeta, HeadlessSnapshot, MODE_BUILD};
use maki_providers::model::Model;
use maki_providers::{TokenUsage, add_cost};
use maki_storage::id::{MakiId, SessionRef};
use serde::Serialize;
use serde_json::Value;

use crate::command_attachments;

const AGENT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

// Fails fast: silently dropping an image the caller explicitly attached
// would be worse than erroring.
fn load_images(paths: &[PathBuf]) -> Result<Vec<ImageSource>> {
    paths
        .iter()
        .map(|path| {
            let media_type = maki_ui::image::media_type_for(path)
                .ok_or_else(|| eyre!("unsupported image type: {}", path.display()))?;
            maki_ui::image::load_file_image(path, media_type)
                .map_err(|e| eyre!("failed to load image: {e}"))
        })
        .collect()
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    StreamJson,
}

#[derive(Serialize)]
struct PrintResult {
    #[serde(rename = "type")]
    result_type: &'static str,
    subtype: &'static str,
    is_error: bool,
    duration_ms: u128,
    num_turns: u32,
    result: String,
    stop_reason: Option<DoneReason>,
    session_id: SessionRef,
    total_cost_usd: f64,
    usage: TokenUsage,
}

#[derive(Serialize)]
struct InitEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    subtype: &'static str,
    cwd: &'a str,
    session_id: &'a SessionRef,
    tools: &'a [String],
    model: &'a str,
}

#[derive(Serialize)]
struct AssistantEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    message: AssistantMessage<'a>,
    session_id: &'a SessionRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_tool_use_id: Option<&'a str>,
}

#[derive(Serialize)]
struct AssistantMessage<'a> {
    model: &'a str,
    role: &'static str,
    content: &'a Value,
    usage: &'a TokenUsage,
}

#[derive(Serialize)]
struct UserEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    message: UserMessage<'a>,
    session_id: &'a SessionRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_tool_use_id: Option<&'a str>,
}

#[derive(Serialize)]
struct UserMessage<'a> {
    role: &'static str,
    content: &'a Value,
}

#[derive(Serialize)]
struct RetryEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    subtype: &'static str,
    attempt: u32,
    retry_delay_ms: u64,
    error: &'a str,
    session_id: &'a SessionRef,
}

enum VerboseOutput {
    StreamJson,
    Json(Vec<Value>),
}

trait PrintRunner {
    fn run(self, input: AgentInput) -> Result<()>;
}

impl<F> PrintRunner for F
where
    F: FnOnce(AgentInput) -> Result<()>,
{
    fn run(self, input: AgentInput) -> Result<()> {
        self(input)
    }
}

#[derive(Default)]
struct CommandTurnMarker;

struct PrintCommandHost;

fn print_capabilities() -> TargetCapabilities {
    TargetCapabilities::from_slice(&[
        TargetCapability::AgentTurns,
        TargetCapability::ApplicationLifecycle,
    ])
}

impl CommandHost for PrintCommandHost {
    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        Box::pin(async move {
            match request {
                HostRequest::Context(_) => Ok(HostResponse::Context(
                    maki_commands::HostContextResponse::Unavailable,
                )),
                HostRequest::Builtin(BuiltinOperation::Exit) => Ok(HostResponse::Completed),
                HostRequest::Builtin(BuiltinOperation::QuickQuestion {
                    question,
                    attachments,
                }) => Ok(HostResponse::AgentTurn(maki_commands::AgentTurn {
                    content: CommandContent {
                        text: question,
                        attachments,
                    },
                    prompt: None,
                })),
                HostRequest::Builtin(operation) => Err(CommandError::Producer(Arc::from(format!(
                    "unsupported command operation: {operation:?}"
                )))),
            }
        })
    }
}

fn drive_print(
    registry: &CommandRegistry,
    target: &maki_commands::TargetHandle,
    _marker: &CommandTurnMarker,
    literal: AgentInput,
    runner: impl PrintRunner,
) -> Result<()> {
    let content = CommandContent {
        text: Arc::from(literal.message.as_str()),
        attachments: command_attachments::from_images(&literal.images),
    };
    let input = match smol::block_on(registry.dispatch_input(target, content)) {
        InputDispatch::Dispatched(CommandOutcome::AgentTurn(turn)) => {
            let defaults = SessionDefaults {
                fast: literal.fast,
                workflow: literal.workflow,
                thinking: Some(literal.thinking.into()),
            };
            command_attachments::agent_input(turn, literal.mode.clone(), defaults)?
        }
        InputDispatch::Dispatched(
            CommandOutcome::Completed | CommandOutcome::FrontendFeedback(_),
        ) => {
            return Ok(());
        }
        InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(_)) => {
            return Err(color_eyre::eyre::eyre!(
                "isolated turns are unavailable in print mode"
            ));
        }
        InputDispatch::Dispatched(CommandOutcome::ManualCompaction(_)) => {
            return Err(color_eyre::eyre::eyre!(
                "manual compaction is unavailable in print mode"
            ));
        }
        InputDispatch::Dispatched(CommandOutcome::Failed(error)) => return Err(error.into()),
        InputDispatch::LiteralInput(content) => AgentInput {
            message: content.text.to_string(),
            mode: literal.mode.clone(),
            images: command_attachments::into_images(&content.attachments)?,
            preamble: Vec::new(),
            thinking: Default::default(),
            fast: literal.fast,
            workflow: literal.workflow,
            prompt: None,
            cancel: None,
            lease_committer: None,
        },
    };
    runner.run(input)
}

impl VerboseOutput {
    fn emit(&mut self, value: &impl Serialize) -> Result<()> {
        match self {
            Self::StreamJson => println!("{}", serde_json::to_string(value)?),
            Self::Json(events) => events.push(serde_json::to_value(value)?),
        }
        Ok(())
    }
}

fn run_and_settle<T>(
    task: smol::Task<()>,
    teardown: impl Future<Output = std::thread::Result<()>>,
    cleanup: Option<OffloadCleanup>,
    shutdown: impl Future<Output = ()>,
    body: impl FnOnce() -> Result<T>,
) -> Result<T> {
    // Body state is never reused after unwinding. Settlement only borrows the
    // task, and the cleanup controller stays outside every unwind boundary.
    let body_result = catch_unwind(AssertUnwindSafe(body));
    let mut task = task;
    let settlement = smol::block_on(
        AssertUnwindSafe(async {
            futures_lite::future::or(&mut task, shutdown).await;
        })
        .catch_unwind(),
    );
    drop(task);
    let teardown = smol::block_on(teardown);
    let settlement = match (settlement, teardown) {
        (Err(panic), teardown) => {
            if teardown.is_err() {
                tracing::error!(
                    "headless future destruction also panicked after agent settlement panic"
                );
            }
            Err(panic)
        }
        (Ok(()), teardown) => teardown,
    };
    smol::block_on(async {
        if let Some(cleanup) = cleanup {
            cleanup.request();
            let _ = cleanup.wait().await;
        }
    });

    match (body_result, settlement) {
        (Err(body_panic), settlement) => {
            if settlement.is_err() {
                tracing::error!("agent settlement also panicked after print body panic");
            }
            resume_unwind(body_panic)
        }
        (Ok(_), Err(settlement_panic)) => resume_unwind(settlement_panic),
        (Ok(result), Ok(())) => result,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    model: &Model,
    prompt_arg: Option<String>,
    image_paths: Vec<PathBuf>,
    format: OutputFormat,
    verbose: bool,
    config: AgentConfig,
    permissions_config: PermissionsConfig,
    timeouts: maki_providers::Timeouts,
    lua_handle: EventHandle,
    defaults: SessionDefaults,
    model_policy: Arc<ModelPolicy>,
    system_prompt_override: Option<String>,
    append_system_prompt: Option<String>,
    plugin_rules: Arc<PluginRuleStore>,
    commands: &[command::CustomCommand],
    command_registry: CommandRegistry,
    modes: Arc<ModeRegistry>,
    project_config: ProjectConfig,
) -> Result<()> {
    let prompt = match prompt_arg {
        Some(p) => p,
        None => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf).context("read stdin")?;
            buf
        }
    };

    let images = load_images(&image_paths)?;
    let literal = AgentInput::from_defaults(prompt, AgentMode::Build, images, defaults);
    let _standard_commands =
        StandardCommands::register(&command_registry, commands, StandardCompletions::default())?;
    let target = command_registry.bind_target(print_capabilities(), Arc::new(PrintCommandHost));
    let command_turn_marker = CommandTurnMarker;

    let prompt_slots = lua_handle.collect_prompt_slots();
    let session_options = lua_handle.session_option_catalog();
    let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
    let (mcp_handle, mcp_config_errors) = smol::block_on(async {
        let (handle, errors) = maki_agent::mcp::start_with_commands(
            &cwd,
            project_config.clone(),
            command_registry.clone(),
        )
        .await;
        if let Some(handle) = &handle {
            handle.ready().await;
        }
        (handle, errors)
    });
    if !mcp_config_errors.is_empty() {
        eprintln!("MCP config error: {mcp_config_errors}");
    }

    let terminal_result_emitted = std::cell::Cell::new(false);
    let state_dir = maki_storage::paths::state_dir().ok();
    let runner = |input: AgentInput| {
        let handle = maki_agent::headless::spawn(HeadlessParams {
            model: model.clone(),
            config,
            permissions_config,
            timeouts,
            input,
            prompt_slots,
            excluded_tools: vec![QUESTION_TOOL_NAME],
            mcp_handle,
            initial_wd: cwd,
            model_policy,
            system_prompt_override,
            append_system_prompt,
            plugin_rules: Arc::clone(&plugin_rules),
            project_config,
            modes: Arc::clone(&modes),
            session_options: session_options.clone(),
            state_dir: state_dir.clone(),
        })
        .map_err(|error| eyre!("register print session coordinator: {error}"))?;

        let HeadlessHandle {
            event_rx,
            tool_names,
            session_id,
            cwd,
            task,
            offload_cleanup,
            teardown,
        } = handle;
        let start = Instant::now();
        let (mut result, verbose_out) = run_and_settle(
            task,
            teardown.wait(),
            offload_cleanup,
            async {
                smol::Timer::after(AGENT_SHUTDOWN_TIMEOUT).await;
            },
            || {
                let mut verbose_out = match format {
                    OutputFormat::StreamJson => Some(VerboseOutput::StreamJson),
                    _ if verbose => Some(VerboseOutput::Json(Vec::new())),
                    _ => None,
                };

                if let Some(out) = &mut verbose_out {
                    out.emit(&InitEvent {
                        event_type: "system",
                        subtype: "init",
                        cwd: &cwd,
                        session_id: &session_id,
                        tools: &tool_names,
                        model: &model.id,
                    })?;
                }

                let mut result_text = String::new();
                let mut is_error = false;
                let mut num_turns: u32 = 0;
                let mut usage = TokenUsage::default();
                // Summed as the turns land: rates move mid-run, and only a turn knows the
                // rate it paid.
                let mut cost = None;
                let mut stop_reason: Option<DoneReason> = None;

                let snapshot = HeadlessSnapshot::default();
                snapshot.install(
                    &lua_handle,
                    HeadlessMeta {
                        id: session_id.to_string(),
                        cwd: cwd.clone(),
                        model: model.spec(),
                        fast: false,
                        thinking: String::new(),
                    },
                    // `makima --print` always runs the agent in build mode
                    // (`headless::spawn` hardcodes `AgentMode::Build`).
                    || MODE_BUILD,
                );

                while let Ok(envelope) = smol::block_on(event_rx.recv_async()) {
                    if matches!(envelope.event, AgentEvent::StreamClosed) {
                        break;
                    }
                    snapshot.observe(&envelope);
                    maki_lua::agent_autocmd::dispatch(
                        &lua_handle,
                        &session_id,
                        &envelope,
                        envelope.subagent.is_some(),
                    );
                    let Envelope {
                        ref event,
                        ref subagent,
                        ..
                    } = envelope;
                    let parent_tool_use_id =
                        subagent.as_ref().map(|s| s.parent_tool_use_id.as_str());

                    match event {
                        AgentEvent::TextDelta { text } => {
                            if parent_tool_use_id.is_none() {
                                result_text.push_str(text);
                            }
                        }
                        AgentEvent::ThinkingDelta { .. } | AgentEvent::ThinkingBlockEnd => {}
                        AgentEvent::ToolPending { .. }
                        | AgentEvent::ToolStart(_)
                        | AgentEvent::ToolExecutionStart { .. }
                        | AgentEvent::ToolOutput { .. }
                        | AgentEvent::ToolDone(_)
                        | AgentEvent::QueueItemConsumed { .. }
                        | AgentEvent::ModelSwitched { .. }
                        | AgentEvent::QueueDrained
                        | AgentEvent::AutoCompacting { .. }
                        | AgentEvent::CompactionDone { .. }
                        | AgentEvent::AuthRequired
                        | AgentEvent::PermissionRequest { .. }
                        | AgentEvent::Question { .. }
                        | AgentEvent::SubagentHistory { .. }
                        | AgentEvent::SubagentClosed
                        | AgentEvent::ToolSnapshot { .. }
                        | AgentEvent::ToolHeaderSnapshot { .. }
                        | AgentEvent::LiveToolBuf { .. }
                        | AgentEvent::Nudge
                        | AgentEvent::PromptProgress { .. }
                        | AgentEvent::StreamClosed => {}
                        AgentEvent::Retry {
                            attempt,
                            message,
                            delay_ms,
                        } => {
                            if let Some(out) = &mut verbose_out {
                                out.emit(&RetryEvent {
                                    event_type: "system",
                                    subtype: "api_retry",
                                    attempt: *attempt,
                                    retry_delay_ms: *delay_ms,
                                    error: message,
                                    session_id: &session_id,
                                })?;
                            }
                        }
                        AgentEvent::TurnComplete(tc) => {
                            add_cost(&mut cost, tc.cost);
                            if let Some(out) = &mut verbose_out {
                                let content_value = serde_json::to_value(&tc.message.content)?;
                                out.emit(&AssistantEvent {
                                    event_type: "assistant",
                                    message: AssistantMessage {
                                        model: &tc.model,
                                        role: "assistant",
                                        content: &content_value,
                                        usage: &tc.usage,
                                    },
                                    session_id: &session_id,
                                    parent_tool_use_id,
                                })?;
                            }
                        }
                        AgentEvent::ToolResultsSubmitted { message } => {
                            if let Some(out) = &mut verbose_out {
                                let content_value = serde_json::to_value(&message.content)?;
                                out.emit(&UserEvent {
                                    event_type: "user",
                                    message: UserMessage {
                                        role: "user",
                                        content: &content_value,
                                    },
                                    session_id: &session_id,
                                    parent_tool_use_id,
                                })?;
                            }
                        }
                        AgentEvent::TurnOutcome(outcome) => {
                            num_turns = outcome.num_turns();
                            usage = outcome.usage();
                            match outcome {
                                TurnOutcome::Completed { reason, .. } => {
                                    stop_reason = Some(*reason)
                                }
                                TurnOutcome::Failed { failure, .. } => {
                                    is_error = true;
                                    result_text = failure.user_message.clone();
                                }
                                TurnOutcome::Cancelled { .. } => {
                                    is_error = true;
                                }
                            }
                            break;
                        }
                        AgentEvent::ControlComplete { .. } => break,
                        AgentEvent::ControlError { message } => {
                            is_error = true;
                            result_text = message.clone();
                            break;
                        }
                    }
                }
                Ok((
                    PrintResult {
                        result_type: "result",
                        subtype: if is_error { "error" } else { "success" },
                        is_error,
                        duration_ms: 0,
                        num_turns,
                        result: result_text,
                        stop_reason,
                        session_id,
                        // Zero on an unpriced model, which is what its turns reported too.
                        total_cost_usd: cost.unwrap_or_default(),
                        usage,
                    },
                    verbose_out,
                ))
            },
        )?;
        result.duration_ms = start.elapsed().as_millis();
        match format {
            OutputFormat::Text => print!("{}", result.result),
            OutputFormat::Json | OutputFormat::StreamJson => {
                match verbose_out {
                    Some(VerboseOutput::Json(mut events)) => {
                        events.push(serde_json::to_value(&result)?);
                        println!("{}", serde_json::to_string(&events)?);
                    }
                    _ => println!("{}", serde_json::to_string(&result)?),
                }
                terminal_result_emitted.set(true);
            }
        }
        if result.is_error {
            // The error text is already in the emitted result; a detailed
            // error report here would print it twice.
            Err(eyre!("agent run failed"))
        } else {
            Ok(())
        }
    };
    let outcome = drive_print(
        &command_registry,
        &target,
        &command_turn_marker,
        literal,
        runner,
    );
    if let (Err(error), true, false) = (
        &outcome,
        matches!(format, OutputFormat::Json | OutputFormat::StreamJson),
        terminal_result_emitted.get(),
    ) {
        let result = PrintResult {
            result_type: "result",
            subtype: "error",
            is_error: true,
            duration_ms: 0,
            num_turns: 0,
            result: error.to_string(),
            stop_reason: None,
            session_id: SessionRef::from_id(MakiId::generate()),
            total_cost_usd: 0.0,
            usage: TokenUsage::default(),
        };
        println!("{}", serde_json::to_string(&result)?);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_agent::tools::offload::{OffloadBackend, OffloadSnapshot, OffloadStore};
    use maki_providers::TokenUsage;
    use serde::Serializer;
    use std::io::Write;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use test_case::test_case;

    const GATE_TIMEOUT: Duration = Duration::from_secs(10);
    const BODY_PANIC: &str = "print body panic";
    const TASK_PANIC: &str = "agent task panic";
    const DESTRUCTOR_PANIC: &str = "pending provider resource destructor panic";
    const SHUTDOWN_PANIC: &str = "shutdown future panic";
    const SERIALIZER_ERROR: &str = "print serializer failed";
    const OUTPUT_ERROR: &str = "print output failed";
    const CLEANUP_ERROR: &str = "offload removal failed";

    struct GatedRemoval {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        removed: Arc<AtomicBool>,
        fail: bool,
    }

    impl OffloadBackend for GatedRemoval {
        fn matches(&self, _: &str, _: &[u8]) -> io::Result<bool> {
            Ok(false)
        }

        fn create_new(&self, _: &str, _: &[u8]) -> io::Result<bool> {
            Ok(true)
        }

        fn snapshot(&self) -> io::Result<OffloadSnapshot> {
            Ok(OffloadSnapshot {
                names: Vec::new(),
                total_bytes: 0,
            })
        }

        fn remove_all(&self) -> io::Result<()> {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(GATE_TIMEOUT).unwrap();
            if self.fail {
                Err(io::Error::other(CLEANUP_ERROR))
            } else {
                self.removed.store(true, Ordering::Release);
                Ok(())
            }
        }

        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/print-offload-test").join(name)
        }
    }

    struct ReleaseRemoval(flume::Sender<()>);

    impl Drop for ReleaseRemoval {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    fn gated_cleanup(
        fail: bool,
    ) -> (
        OffloadCleanup,
        flume::Receiver<()>,
        ReleaseRemoval,
        Arc<AtomicBool>,
    ) {
        let (entered, received) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let removed = Arc::new(AtomicBool::new(false));
        let store = Arc::new(OffloadStore::new(Box::new(GatedRemoval {
            entered,
            release: released,
            removed: Arc::clone(&removed),
            fail,
        })));
        (
            OffloadCleanup::new(store),
            received,
            ReleaseRemoval(release),
            removed,
        )
    }

    struct FailingSerializer;

    impl Serialize for FailingSerializer {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom(SERIALIZER_ERROR))
        }
    }

    struct FailingOutput;

    impl Write for FailingOutput {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other(OUTPUT_ERROR))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    enum BodyExit {
        Success,
        SerializerError,
        OutputError,
        Panic,
    }

    #[derive(Clone)]
    enum TaskExit {
        Success,
        Timeout,
        Panic,
        ShutdownPanic,
    }

    #[test_case(BodyExit::Success, TaskExit::Success ; "success")]
    #[test_case(BodyExit::SerializerError, TaskExit::Success ; "serializer error")]
    #[test_case(BodyExit::OutputError, TaskExit::Success ; "output error")]
    #[test_case(BodyExit::Success, TaskExit::Timeout ; "timeout")]
    #[test_case(BodyExit::Panic, TaskExit::Success ; "body panic")]
    #[test_case(BodyExit::Success, TaskExit::Panic ; "task panic")]
    #[test_case(BodyExit::Panic, TaskExit::Panic ; "dual panic")]
    #[test_case(BodyExit::Success, TaskExit::ShutdownPanic ; "shutdown panic")]
    fn print_settlement_drains_removal_before_return(body_exit: BodyExit, task_exit: TaskExit) {
        let (cleanup, entered, release, removed) = gated_cleanup(false);
        let (task_dropped, dropped) = flume::bounded(1);
        let (teardown_done, teardown_rx) = flume::bounded(1);
        struct MarkTaskDropped {
            dropped: flume::Sender<()>,
            completed: flume::Sender<()>,
        }
        impl Drop for MarkTaskDropped {
            fn drop(&mut self) {
                let _ = self.dropped.send(());
                let _ = self.completed.send(());
            }
        }
        let marker = MarkTaskDropped {
            dropped: task_dropped,
            completed: teardown_done,
        };
        let task_behavior = task_exit.clone();
        let task = smol::spawn(async move {
            let _marker = marker;
            match task_behavior {
                TaskExit::Success => (),
                TaskExit::Panic => panic!("{TASK_PANIC}"),
                TaskExit::Timeout | TaskExit::ShutdownPanic => std::future::pending().await,
            }
        });
        let (completed, completion) = flume::bounded(1);
        let shutdown_behavior = task_exit.clone();
        let body_behavior = body_exit.clone();
        let thread = std::thread::spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_and_settle(
                    task,
                    async {
                        teardown_rx.recv_async().await.unwrap();
                        Ok(())
                    },
                    Some(cleanup),
                    async move {
                        match shutdown_behavior {
                            TaskExit::Timeout => (),
                            TaskExit::ShutdownPanic => panic!("{SHUTDOWN_PANIC}"),
                            TaskExit::Success | TaskExit::Panic => std::future::pending().await,
                        }
                    },
                    || match body_behavior {
                        BodyExit::Success => Ok(()),
                        BodyExit::SerializerError => {
                            VerboseOutput::Json(Vec::new()).emit(&FailingSerializer)
                        }
                        BodyExit::OutputError => {
                            FailingOutput.write_all(b"result")?;
                            Ok(())
                        }
                        BodyExit::Panic => panic!("{BODY_PANIC}"),
                    },
                )
            }));
            dropped
                .try_recv()
                .expect("task destruction must finish at return boundary");
            completed.send(()).unwrap();
            result
        });
        entered.recv_timeout(GATE_TIMEOUT).unwrap();
        assert!(
            completion.try_recv().is_err(),
            "return boundary must wait for removal"
        );
        assert!(!removed.load(Ordering::Acquire));
        drop(release);
        let result = thread.join().unwrap();
        assert!(removed.load(Ordering::Acquire));
        let expected_panic = match (&body_exit, &task_exit) {
            (BodyExit::Panic, _) => Some(BODY_PANIC),
            (_, TaskExit::Panic) => Some(TASK_PANIC),
            (_, TaskExit::ShutdownPanic) => Some(SHUTDOWN_PANIC),
            _ => None,
        };
        if let Some(expected) = expected_panic {
            let panic = result.expect_err("panic must escape after cleanup");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied());
            assert_eq!(message, Some(expected));
        } else {
            match body_exit {
                BodyExit::Success => result.unwrap().unwrap(),
                BodyExit::SerializerError => assert!(
                    result
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains(SERIALIZER_ERROR)
                ),
                BodyExit::OutputError => assert!(
                    result
                        .unwrap()
                        .unwrap_err()
                        .to_string()
                        .contains(OUTPUT_ERROR)
                ),
                BodyExit::Panic => unreachable!(),
            }
        }
    }

    #[test]
    fn print_settlement_reports_cleanup_failure() {
        let (cleanup, entered, release, removed) = gated_cleanup(true);
        let observer = cleanup.clone();
        let thread = std::thread::spawn(move || {
            run_and_settle(
                smol::spawn(async {}),
                async { Ok(()) },
                Some(cleanup),
                std::future::pending(),
                || Ok(()),
            )
        });
        entered.recv_timeout(GATE_TIMEOUT).unwrap();
        drop(release);
        thread.join().unwrap().unwrap();
        let error = smol::block_on(observer.wait()).unwrap_err();
        assert_eq!(error.to_string(), CLEANUP_ERROR);
        assert!(!removed.load(Ordering::Acquire));
    }

    const PRINT_RESULT_FIELDS: &[&str] = &[
        "type",
        "subtype",
        "is_error",
        "num_turns",
        "result",
        "stop_reason",
        "session_id",
        "total_cost_usd",
        "usage",
        "duration_ms",
    ];
    const INIT_EVENT_FIELDS: &[&str] = &["type", "subtype", "cwd", "session_id", "tools", "model"];
    const RETRY_EVENT_FIELDS: &[&str] = &[
        "type",
        "subtype",
        "attempt",
        "retry_delay_ms",
        "error",
        "session_id",
    ];

    fn input(message: &str) -> AgentInput {
        AgentInput {
            message: message.into(),
            mode: AgentMode::Build,
            images: Vec::new(),
            preamble: Vec::new(),
            thinking: Default::default(),
            fast: false,
            workflow: false,
            prompt: None,
            cancel: None,
            lease_committer: None,
        }
    }

    struct RecordingPrintProvider(flume::Sender<(String, String, bool)>);

    impl maki_providers::provider::Provider for RecordingPrintProvider {
        fn stream_message<'a>(
            &'a self,
            model: &'a Model,
            _: &'a [maki_providers::Message],
            system: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<maki_providers::ProviderEvent>,
            options: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_agent::AgentError>,
        > {
            Box::pin(async move {
                self.0
                    .send((model.spec(), system.to_owned(), options.fast))
                    .unwrap();
                Ok(maki_providers::StreamResponse {
                    message: maki_providers::Message {
                        role: maki_providers::Role::Assistant,
                        content: vec![maki_providers::ContentBlock::Text {
                            text: "done".into(),
                        }],
                        ..Default::default()
                    },
                    usage: TokenUsage::default(),
                    stop_reason: Some(maki_providers::StopReason::EndTurn),
                })
            })
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, maki_agent::AgentError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct PendingResource {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        dropped: Arc<AtomicBool>,
        panic: bool,
    }

    impl Drop for PendingResource {
        fn drop(&mut self) {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(GATE_TIMEOUT).unwrap();
            self.dropped.store(true, Ordering::Release);
            if self.panic {
                panic!("{DESTRUCTOR_PANIC}");
            }
        }
    }

    struct PendingPrintProvider {
        polled: flume::Sender<()>,
        resource: Mutex<Option<PendingResource>>,
    }

    impl maki_providers::provider::Provider for PendingPrintProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            _: &'a [maki_providers::Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<maki_providers::ProviderEvent>,
            _: maki_providers::RequestOptions,
            _: Option<&'a SessionRef>,
        ) -> maki_providers::provider::BoxFuture<
            'a,
            Result<maki_providers::StreamResponse, maki_agent::AgentError>,
        > {
            let resource = self.resource.lock().unwrap().take().unwrap();
            Box::pin(async move {
                let _resource = resource;
                self.polled.send_async(()).await.unwrap();
                std::future::pending().await
            })
        }

        fn list_models(
            &self,
        ) -> maki_providers::provider::BoxFuture<
            '_,
            Result<Vec<maki_providers::ModelInfo>, maki_agent::AgentError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[test_case(false, false ; "pending destruction acknowledged before timeout return")]
    #[test_case(true, false ; "pending destructor panic resumes after removal")]
    #[test_case(true, true ; "body panic wins over pending destructor panic")]
    fn print_waits_for_actual_headless_future_destruction(
        destructor_panic: bool,
        body_panic: bool,
    ) {
        let (polled, polled_rx) = flume::bounded(1);
        let (destroying, destroying_rx) = flume::bounded(1);
        let (release_drop, release_drop_rx) = flume::bounded(1);
        let release_drop = ReleaseRemoval(release_drop);
        let dropped = Arc::new(AtomicBool::new(false));
        let cwd = std::env::temp_dir();
        let handle = maki_agent::headless::spawn_with_provider(
            HeadlessParams {
                model: Model::from_spec("anthropic/claude-opus-4-8").unwrap(),
                config: AgentConfig::default(),
                permissions_config: PermissionsConfig::default(),
                timeouts: Default::default(),
                input: input("pending provider request"),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: cwd.clone(),
                system_prompt_override: None,
                append_system_prompt: None,
                model_policy: Arc::default(),
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(&cwd),
                modes: Arc::default(),
                session_options: Default::default(),
                state_dir: None,
            },
            Arc::new(PendingPrintProvider {
                polled,
                resource: Mutex::new(Some(PendingResource {
                    entered: destroying,
                    release: release_drop_rx,
                    dropped: Arc::clone(&dropped),
                    panic: destructor_panic,
                })),
            }),
        )
        .unwrap();
        polled_rx.recv_timeout(GATE_TIMEOUT).unwrap();
        let (cleanup, removing, release_removal, removed) = gated_cleanup(false);
        let (completed, returned) = flume::bounded(1);
        let at_boundary = Arc::clone(&dropped);
        let thread = std::thread::spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_and_settle(
                    handle.task,
                    handle.teardown.wait(),
                    Some(cleanup),
                    async {},
                    || {
                        if body_panic {
                            panic!("{BODY_PANIC}");
                        }
                        Ok(())
                    },
                )
            }));
            assert!(
                at_boundary.load(Ordering::Acquire),
                "actual destruction must finish before return or panic"
            );
            completed.send(()).unwrap();
            result
        });
        destroying_rx.recv_timeout(GATE_TIMEOUT).unwrap();
        assert!(
            removing.try_recv().is_err(),
            "cleanup must follow actual destruction"
        );
        assert!(returned.try_recv().is_err());
        assert!(!dropped.load(Ordering::Acquire));
        drop(release_drop);
        removing.recv_timeout(GATE_TIMEOUT).unwrap();
        assert!(
            returned.try_recv().is_err(),
            "outer catch must wait for removal"
        );
        assert!(!removed.load(Ordering::Acquire));
        drop(release_removal);
        returned.recv_timeout(GATE_TIMEOUT).unwrap();
        let result = thread.join().unwrap();
        assert!(removed.load(Ordering::Acquire));
        if destructor_panic || body_panic {
            let panic = result.expect_err("panic must remain a panic");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied());
            assert_eq!(
                message,
                Some(if body_panic {
                    BODY_PANIC
                } else {
                    DESTRUCTOR_PANIC
                })
            );
        } else {
            result.unwrap().unwrap();
        }
    }

    /// The provider's report is a rendezvous, so the run can't reach its
    /// cleanup before the saved output below exists.
    #[test]
    fn print_run_removes_offload_dir() {
        let state = tempfile::tempdir().unwrap();
        let cwd = std::env::temp_dir();
        let (requests, received) = flume::bounded(0);
        let handle = maki_agent::headless::spawn_with_provider(
            HeadlessParams {
                model: Model::from_spec("anthropic/claude-opus-4-8").unwrap(),
                config: AgentConfig::default(),
                permissions_config: PermissionsConfig::default(),
                timeouts: Default::default(),
                input: input("print request"),
                prompt_slots: Default::default(),
                excluded_tools: Vec::new(),
                mcp_handle: None,
                initial_wd: cwd.clone(),
                system_prompt_override: None,
                append_system_prompt: None,
                model_policy: Arc::default(),
                plugin_rules: Arc::default(),
                project_config: ProjectConfig::for_project(&cwd),
                modes: Arc::default(),
                session_options: Default::default(),
                state_dir: Some(state.path().to_path_buf()),
            },
            Arc::new(RecordingPrintProvider(requests)),
        )
        .unwrap();
        let dir =
            maki_agent::tools::offload::offload_dir_for(state.path(), Some(&handle.session_id))
                .unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("saved.txt"), "output").unwrap();

        received.recv().unwrap();
        smol::block_on(handle.task);
        smol::block_on(handle.teardown.wait()).unwrap();
        assert!(!dir.exists(), "print mode must remove its offload dir");
    }

    #[test]
    fn print_turn_uses_initialized_config() {
        const SPEC: &str = "anthropic/claude-opus-4-8";
        const SYSTEM: &str = "print initialized system";
        let registry = CommandRegistry::new();
        let target = target(&registry);
        let (requests, received) = flume::unbounded();
        let mut literal = input("print request");
        literal.fast = true;
        drive_print(&registry, &target, &CommandTurnMarker, literal, |input| {
            let cwd = std::env::temp_dir();
            let handle = maki_agent::headless::spawn_with_provider(
                HeadlessParams {
                    model: Model::from_spec(SPEC).unwrap(),
                    config: AgentConfig::default(),
                    permissions_config: PermissionsConfig::default(),
                    timeouts: Default::default(),
                    input,
                    prompt_slots: Default::default(),
                    excluded_tools: Vec::new(),
                    mcp_handle: None,
                    initial_wd: cwd.clone(),
                    system_prompt_override: Some(SYSTEM.into()),
                    append_system_prompt: None,
                    model_policy: Arc::default(),
                    plugin_rules: Arc::default(),
                    project_config: ProjectConfig::for_project(&cwd),
                    modes: Arc::default(),
                    session_options: Default::default(),
                    state_dir: None,
                },
                Arc::new(RecordingPrintProvider(requests)),
            )?;
            smol::block_on(handle.task);
            smol::block_on(handle.teardown.wait()).unwrap();
            Ok(())
        })
        .unwrap();
        assert_eq!(
            received.try_recv().unwrap(),
            (SPEC.into(), SYSTEM.into(), true)
        );
        assert!(received.try_recv().is_err());
    }

    fn target(registry: &CommandRegistry) -> maki_commands::TargetHandle {
        registry.bind_target(print_capabilities(), Arc::new(PrintCommandHost))
    }

    struct ReplaceAttachment;

    impl maki_commands::CommandBehavior for ReplaceAttachment {
        fn execute(
            &self,
            invocation: maki_commands::CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            Box::pin(async move {
                let Some(attachment) = invocation.content.attachments.first() else {
                    return Err(CommandError::Producer(Arc::from(
                        "missing command attachment",
                    )));
                };
                if attachment.media_type.as_ref() != "image/png"
                    || attachment.data.as_ref() != "AAAA"
                {
                    return Err(CommandError::Producer(Arc::from(
                        "command attachment changed before dispatch",
                    )));
                }
                Ok(CommandOutcome::AgentTurn(maki_commands::AgentTurn {
                    content: CommandContent {
                        text: Arc::from("inspected"),
                        attachments: Arc::from([maki_commands::CommandAttachment {
                            media_type: Arc::from("image/jpeg"),
                            data: Arc::from("BBBB"),
                        }]),
                    },
                    prompt: None,
                }))
            })
        }
    }

    #[test]
    fn command_behavior_owns_attachment_policy() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
        producer
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/inspect"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Positional(Arc::from([])),
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("inspect"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::from_capability(
                        TargetCapability::AgentTurns,
                    ),
                },
                behavior: Arc::new(ReplaceAttachment),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input("/inspect");
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Png,
            Arc::from("AAAA"),
        ));
        let mut received = None;

        drive_print(&registry, &target, &sink, literal, |input| {
            received = Some(input);
            Ok(())
        })
        .unwrap();

        let received = received.unwrap();
        assert_eq!(received.message, "inspected");
        assert_eq!(received.images.len(), 1);
        assert_eq!(
            received.images[0].media_type,
            maki_agent::ImageMediaType::Jpeg
        );
        assert_eq!(received.images[0].data.as_ref(), "BBBB");
    }

    #[test]
    fn quick_question_runs_as_agent_turn() {
        let registry = CommandRegistry::new();
        let _commands =
            StandardCommands::register(&registry, &[], StandardCompletions::default()).unwrap();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input("/btw explain this");
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Png,
            Arc::from("AAAA"),
        ));
        let mut received = None;

        drive_print(&registry, &target, &sink, literal, |input| {
            received = Some(input);
            Ok(())
        })
        .unwrap();

        let received = received.unwrap();
        assert_eq!(received.message, "explain this");
        assert_eq!(received.images.len(), 1);
        assert_eq!(
            received.images[0].media_type,
            maki_agent::ImageMediaType::Png
        );
        assert_eq!(received.images[0].data.as_ref(), "AAAA");
    }

    #[test]
    fn completed_command_skips_runner() {
        struct Completed;
        impl maki_commands::CommandBehavior for Completed {
            fn execute(
                &self,
                _invocation: maki_commands::CommandInvocation,
            ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
                Box::pin(async { Ok(CommandOutcome::Completed) })
            }
        }
        let registry = CommandRegistry::new();
        registry
            .create_producer(maki_commands::ProducerPrecedence::Plugin)
            .replace(vec![maki_commands::Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from("/done"),
                    aliases: Arc::from([]),
                    arguments: maki_commands::CommandArguments::Positional(Arc::from([])),
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from("done"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(Completed),
                argument_completions: Vec::new(),
            }])
            .unwrap();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut ran = false;
        drive_print(&registry, &target, &sink, input("/done"), |_| {
            ran = true;
            Ok(())
        })
        .unwrap();
        assert!(!ran);
    }

    #[test]
    fn unknown_command_rejects_and_skips_runner() {
        let registry = CommandRegistry::new();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input("/unknown literal");
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Webp,
            Arc::from("AAAA"),
        ));
        let mut ran = false;
        let _ = drive_print(&registry, &target, &sink, literal, |_input: AgentInput| {
            ran = true;
            Ok(())
        })
        .expect_err("unknown slash command must be rejected");
        assert!(!ran);
    }

    #[test_case("//unknown literal", "/unknown literal"; "escaped literal strips one slash")]
    #[test_case("///unknown literal", "//unknown literal"; "triple slash strips one slash")]
    fn escaped_slash_runs_literal_input(message: &str, expected: &str) {
        let registry = CommandRegistry::new();
        let target = target(&registry);
        let sink = CommandTurnMarker;
        let mut literal = input(message);
        literal.images.push(ImageSource::new(
            maki_agent::ImageMediaType::Webp,
            Arc::from("AAAA"),
        ));
        let mut received = None;
        drive_print(&registry, &target, &sink, literal, |input: AgentInput| {
            received = Some(input);
            Ok(())
        })
        .unwrap();
        let received = received.unwrap();
        assert_eq!(received.message, expected);
        assert_eq!(received.images.len(), 1);
        assert_eq!(
            received.images[0].media_type,
            maki_agent::ImageMediaType::Webp
        );
        assert_eq!(received.images[0].data.as_ref(), "AAAA");
    }

    #[test]
    fn wire_format_required_fields() {
        let result = PrintResult {
            result_type: "result",
            subtype: "success",
            is_error: false,
            duration_ms: 1234,
            num_turns: 2,
            result: "done".into(),
            stop_reason: Some(DoneReason::EndTurn),
            session_id: SessionRef::generate(),
            total_cost_usd: 0.003,
            usage: TokenUsage::default(),
        };
        let json: Value = serde_json::to_value(&result).unwrap();
        for field in PRINT_RESULT_FIELDS {
            assert!(json.get(field).is_some(), "PrintResult missing: {field}");
        }

        let sid = SessionRef::generate();
        let init = InitEvent {
            event_type: "system",
            subtype: "init",
            cwd: "/tmp",
            session_id: &sid,
            tools: &["bash".into(), "read".into()],
            model: "test-model",
        };
        let json: Value = serde_json::to_value(&init).unwrap();
        for field in INIT_EVENT_FIELDS {
            assert!(json.get(field).is_some(), "InitEvent missing: {field}");
        }

        let retry = RetryEvent {
            event_type: "system",
            subtype: "api_retry",
            attempt: 2,
            retry_delay_ms: 3000,
            error: "rate_limit",
            session_id: &sid,
        };
        let json: Value = serde_json::to_value(&retry).unwrap();
        for field in RETRY_EVENT_FIELDS {
            assert!(json.get(field).is_some(), "RetryEvent missing: {field}");
        }
    }
}
