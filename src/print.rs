//! Non-interactive (headless) mode: `makima "prompt" --print`.
//!
//! Wire format intentionally matches Claude Code so existing scripts work
//! unchanged. Keep `PrintResult` fields a strict subset of theirs. `StreamJson`
//! is JSONL with the same shape, `Text` prints the raw response only.
//!
//! We adopt new fields when Claude Code adds them but never invent our own.
//! Check their docs before changing anything here.

use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use color_eyre::Result;
use color_eyre::eyre::{Context, eyre};
use maki_agent::command::{self, PromptSink};
use maki_agent::headless::{HeadlessHandle, HeadlessParams};
use maki_agent::permissions::PluginRuleStore;
use maki_agent::tools::QUESTION_TOOL_NAME;
use maki_agent::{
    AgentConfig, AgentEvent, AgentInput, AgentMode, DoneReason, Envelope, ImageSource,
    McpPromptRef, McpPromptRequest, McpPromptSink, ModeRegistry, PermissionsConfig,
};
use maki_commands::{
    ArgumentArity, BUILTIN_COMMANDS, CommandBehavior, CommandClassification, CommandDocs,
    CommandError, CommandFuture, CommandInvocation, CommandRegistry, CommandSpec, InputDispatch,
    ProducerPrecedence, Registration,
};
use maki_config::ModelPolicy;
use maki_lua::EventHandle;
use maki_providers::model::Model;
use maki_providers::{TokenUsage, add_cost};
use maki_storage::id::{MakiId, SessionRef};
use serde::Serialize;
use serde_json::Value;

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

struct CommandInputSink {
    input: std::sync::Mutex<Option<AgentInput>>,
    mode: AgentMode,
    images: Vec<ImageSource>,
    fast: bool,
    workflow: bool,
}

impl CommandInputSink {
    fn input(&self, message: String, prompt: Option<Box<McpPromptRef>>) -> AgentInput {
        AgentInput {
            message,
            mode: self.mode.clone(),
            images: self.images.clone(),
            preamble: Vec::new(),
            thinking: Default::default(),
            fast: self.fast,
            workflow: self.workflow,
            prompt,
        }
    }

    fn set(&self, input: AgentInput, invocation: CommandInvocation) {
        *self.input.lock().unwrap_or_else(|error| error.into_inner()) = Some(input);
        invocation
            .lifecycle
            .transition(CommandClassification::AgentTurnAccepted);
    }

    fn take(&self) -> Option<AgentInput> {
        self.input
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }
}

impl PromptSink for CommandInputSink {
    fn submit(
        &self,
        prompt: String,
        invocation: CommandInvocation,
    ) -> CommandFuture<Result<(), CommandError>> {
        self.set(self.input(prompt, None), invocation);
        Box::pin(async { Ok(()) })
    }
}

impl McpPromptSink for CommandInputSink {
    fn submit(
        &self,
        invocation: CommandInvocation,
        prompt: McpPromptRequest,
    ) -> CommandFuture<Result<(), CommandError>> {
        let prompt_ref = McpPromptRef {
            qualified_name: prompt.qualified_name,
            arguments: prompt.arguments,
        };
        self.set(
            self.input(prompt.display_text, Some(Box::new(prompt_ref))),
            invocation,
        );
        Box::pin(async { Ok(()) })
    }
}

struct CompletedBuiltin;

impl CommandBehavior for CompletedBuiltin {
    fn execute(&self, invocation: CommandInvocation) -> CommandFuture<Result<(), CommandError>> {
        invocation
            .lifecycle
            .transition(CommandClassification::Completed);
        Box::pin(async { Ok(()) })
    }
}

struct UnsupportedBuiltin(Arc<str>);

impl CommandBehavior for UnsupportedBuiltin {
    fn execute(&self, _invocation: CommandInvocation) -> CommandFuture<Result<(), CommandError>> {
        let error = CommandError::UnsupportedFrontend(Arc::clone(&self.0));
        Box::pin(async move { Err(error) })
    }
}

fn register_print_commands(
    registry: &CommandRegistry,
    commands: &[command::CustomCommand],
    sink: Arc<CommandInputSink>,
) -> Result<()> {
    let builtin = registry.create_producer(ProducerPrecedence::Builtin);
    builtin.replace(
        BUILTIN_COMMANDS
            .iter()
            .map(|command| Registration {
                spec: command.spec(),
                behavior: if command.name == "/exit" {
                    Arc::new(CompletedBuiltin)
                } else {
                    Arc::new(UnsupportedBuiltin(Arc::from(command.name)))
                },
                completion: None,
            })
            .collect(),
    )?;
    let application = registry.create_producer(ProducerPrecedence::Application);
    command::register_commands(&application, commands, sink)?;
    Ok(())
}

fn drive_print(
    registry: &CommandRegistry,
    sink: &CommandInputSink,
    literal: AgentInput,
    runner: impl PrintRunner,
) -> Result<()> {
    if !literal.images.is_empty() && registry.resolves_input(&literal.message) {
        return Err(eyre!("slash commands cannot include images"));
    }
    let target = registry.create_target();
    let input = match smol::block_on(registry.dispatch_input(&literal.message, 0, target))? {
        InputDispatch::Dispatched(dispatch) => match smol::block_on(dispatch.classification()) {
            CommandClassification::AgentTurnAccepted => sink
                .take()
                .ok_or_else(|| eyre!("command accepted an agent turn without input"))?,
            CommandClassification::Completed => return Ok(()),
            CommandClassification::Failed(error) => return Err(error.into()),
        },
        InputDispatch::NotCommand | InputDispatch::UnknownCommandInput => literal,
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
    fast: bool,
    workflow: bool,
    model_policy: Arc<ModelPolicy>,
    system_prompt_override: Option<String>,
    append_system_prompt: Option<String>,
    plugin_rules: Arc<PluginRuleStore>,
    commands: &[command::CustomCommand],
    command_registry: CommandRegistry,
    modes: Arc<ModeRegistry>,
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
    let literal = AgentInput {
        message: prompt,
        mode: AgentMode::Build,
        images,
        preamble: Vec::new(),
        thinking: Default::default(),
        fast,
        workflow,
        prompt: None,
    };
    let sink = Arc::new(CommandInputSink {
        input: std::sync::Mutex::new(None),
        mode: AgentMode::Build,
        images: literal.images.clone(),
        fast,
        workflow,
    });
    register_print_commands(&command_registry, commands, Arc::clone(&sink))?;

    let prompt_slots = lua_handle.collect_prompt_slots();
    let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
    let (mcp_handle, mcp_config_errors) = smol::block_on(maki_agent::mcp::start_with_commands(
        &cwd,
        command_registry.clone(),
        Arc::clone(&sink) as Arc<dyn McpPromptSink>,
    ));
    if !mcp_config_errors.is_empty() {
        eprintln!("MCP config error: {mcp_config_errors}");
    }

    let terminal_result_emitted = std::cell::Cell::new(false);
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
            fast,
            workflow,
            model_policy,
            system_prompt_override,
            append_system_prompt,
            plugin_rules: Arc::clone(&plugin_rules),
            modes: Arc::clone(&modes),
        });

        let HeadlessHandle {
            event_rx,
            tool_names,
            session_id,
            cwd,
            task,
        } = handle;
        let start = Instant::now();

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

        while let Ok(envelope) = smol::block_on(event_rx.recv_async()) {
            let Envelope {
                ref event,
                ref subagent,
                ..
            } = envelope;
            let parent_tool_use_id = subagent.as_ref().map(|s| s.parent_tool_use_id.as_str());

            match event {
                AgentEvent::TextDelta { text } => {
                    if parent_tool_use_id.is_none() {
                        result_text.push_str(text);
                    }
                }
                AgentEvent::ThinkingDelta { .. } => {}
                AgentEvent::ToolPending { .. }
                | AgentEvent::ToolStart(_)
                | AgentEvent::ToolOutput { .. }
                | AgentEvent::ToolDone(_)
                | AgentEvent::QueueItemConsumed { .. }
                | AgentEvent::QueueDrained
                | AgentEvent::AutoCompacting
                | AgentEvent::CompactionDone
                | AgentEvent::AuthRequired
                | AgentEvent::PermissionRequest { .. }
                | AgentEvent::Question { .. }
                | AgentEvent::SubagentHistory { .. }
                | AgentEvent::ToolSnapshot { .. }
                | AgentEvent::ToolHeaderSnapshot { .. }
                | AgentEvent::LiveToolBuf { .. }
                | AgentEvent::Nudge
                | AgentEvent::PromptProgress { .. } => {}
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
                AgentEvent::Done {
                    usage: u,
                    num_turns: turns,
                    reason,
                } => {
                    num_turns = *turns;
                    usage = *u;
                    stop_reason = Some(*reason);
                    break;
                }
                AgentEvent::Error { message } => {
                    is_error = true;
                    result_text = message.clone();
                    break;
                }
            }
        }
        smol::block_on(async {
            futures_lite::future::or(task, async {
                smol::Timer::after(AGENT_SHUTDOWN_TIMEOUT).await;
            })
            .await;
        });

        let duration_ms = start.elapsed().as_millis();
        // Zero on an unpriced model, which is what its turns reported too.
        let total_cost_usd = cost.unwrap_or_default();
        let run_error = is_error.then(|| result_text.clone());

        match format {
            OutputFormat::Text => {
                print!("{result_text}");
            }
            OutputFormat::Json | OutputFormat::StreamJson => {
                let result = PrintResult {
                    result_type: "result",
                    subtype: if is_error { "error" } else { "success" },
                    is_error,
                    duration_ms,
                    num_turns,
                    result: result_text,
                    stop_reason,
                    session_id,
                    total_cost_usd,
                    usage,
                };
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

        match run_error {
            // The error text is already in the emitted result; a detailed
            // error report here would print it twice.
            Some(_) => Err(eyre!("agent run failed")),
            None => Ok(()),
        }
    };
    let outcome = drive_print(&command_registry, &sink, literal, runner);
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
    use maki_agent::command::{CommandScope, CustomCommand};
    use maki_agent::tools::ToolRegistry;
    use maki_lua::PluginHost;
    use maki_providers::TokenUsage;

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
        }
    }

    fn sink() -> Arc<CommandInputSink> {
        Arc::new(CommandInputSink {
            input: std::sync::Mutex::new(None),
            mode: AgentMode::Build,
            images: Vec::new(),
            fast: false,
            workflow: false,
        })
    }

    #[test]
    fn nested_lua_agent_turn_accepted_runs_rendered_prompt() {
        let registry = CommandRegistry::new();
        let host = PluginHost::with_command_registry(
            Arc::new(ToolRegistry::new()),
            registry.clone(),
            true,
        )
        .unwrap();
        host.load_source(
            "nested",
            r#"
            maki.api.register_command({
                name = "/outer",
                tui_only = false,
                handler = function()
                    local ok, err = maki.api.run_command("/project:inner nested")
                    if not ok then error(err) end
                end,
            })
            "#,
        )
        .unwrap();
        let sink = sink();
        register_print_commands(
            &registry,
            &[CustomCommand {
                name: "inner".into(),
                description: "inner prompt".into(),
                content: "rendered $ARGUMENTS".into(),
                scope: CommandScope::Project,
                accepts_args: true,
            }],
            Arc::clone(&sink),
        )
        .unwrap();
        let mut received = None;
        drive_print(&registry, &sink, input("/outer"), |input: AgentInput| {
            received = Some(input.message);
            Ok(())
        })
        .unwrap();
        assert_eq!(received.as_deref(), Some("rendered nested"));
    }

    #[test]
    fn completed_command_skips_runner() {
        struct Completed;
        impl CommandBehavior for Completed {
            fn execute(
                &self,
                invocation: CommandInvocation,
            ) -> CommandFuture<Result<(), CommandError>> {
                invocation
                    .lifecycle
                    .transition(CommandClassification::Completed);
                Box::pin(async { Ok(()) })
            }
        }
        let registry = CommandRegistry::new();
        registry
            .create_producer(ProducerPrecedence::Plugin)
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/done"),
                    aliases: Arc::from([]),
                    arguments: ArgumentArity::NONE,
                    docs: CommandDocs {
                        summary: Arc::from("done"),
                        argument_hint: None,
                    },
                    tui_only: false,
                },
                behavior: Arc::new(Completed),
                completion: None,
            }])
            .unwrap();
        let sink = sink();
        let mut ran = false;
        drive_print(&registry, &sink, input("/done"), |_| {
            ran = true;
            Ok(())
        })
        .unwrap();
        assert!(!ran);
    }

    #[test]
    fn exit_command_completes_without_runner() {
        let registry = CommandRegistry::new();
        let sink = sink();
        register_print_commands(&registry, &[], Arc::clone(&sink)).unwrap();
        let mut ran = false;

        drive_print(&registry, &sink, input("/exit"), |_| {
            ran = true;
            Ok(())
        })
        .unwrap();

        assert!(!ran);
    }

    #[test]
    fn unknown_command_runs_literal_input() {
        let registry = CommandRegistry::new();
        let sink = sink();
        let mut received = None;
        drive_print(
            &registry,
            &sink,
            input("/unknown literal"),
            |input: AgentInput| {
                received = Some(input.message);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(received.as_deref(), Some("/unknown literal"));
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
