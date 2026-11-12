use super::shell::ShellEvent;
use super::*;
use crate::chat::{CANCELLED_TEXT, DONE_TEXT};
use crate::click::{ClickTarget, PickerId, ScrollOwner, Surface};
use crate::components::btw_modal::BtwEvent;
use crate::components::command::ParsedCommand;
use crate::components::file_picker::EMPTY_DIR_MSG;
use crate::components::keybindings::{KeybindContext, key as kb};
use crate::components::{Action, ExitRequest, TEST_MODEL_PROVIDER, buffer_text, key, test_model};
use crate::model_capabilities::test_support::seeded;
use crate::repaint::expect::{OWED, QUIET};
use crate::selection::{SelectableZone, SelectionState, SelectionZone};
use arc_swap::ArcSwap;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEventKind};
use makima_agent::actor::HistoryChange;
use makima_agent::agent::estimate_message_tokens;
use makima_agent::command::{CommandScope, CustomCommand};
use makima_agent::permissions::PermissionManager;
use makima_agent::tools::{TASK_SPAWN_TOOL_NAME, ToolRegistry, cli_tool_ctx};
use makima_agent::{
    ActorBackend, AgentId, AgentInput, AgentManagerHandle, AgentMode, AgentRef, BackendResult,
    DoneReason, EffectiveAgentConfig, History, HistorySnapshot, ImageMediaType, McpConfigErrors,
    McpServerInfo, McpServerStatus, McpSnapshot, McpSnapshotReader, ModeDefSpec, Post, PostLevel,
    ToolDoneEvent, ToolOutput, ToolStartEvent, TurnCompleteEvent, TurnContext, TurnFailure,
    TurnFailureKind, TurnId, TurnOutcome,
};
use makima_commands::ModelSelection;
use makima_config::{PermissionsConfig, UiConfig};
use makima_lua::test_support::{HintWriterHandle, hint_writer_pair};
use makima_lua::{BuiltinAction, CommandArgumentItem, HintReader, KeymapReader};
use makima_providers::model::FastSupport;
use makima_providers::test_util::offline_ids;
use makima_providers::{
    ContentBlock, Effort, Message, ModelCapabilities, Role, ThinkingConfig, ThinkingSupport,
    TokenUsage,
};
use makima_storage::sessions::{StoredMode, StoredPost, StoredSubagent};
use ratatui::layout::Rect;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use test_case::test_case;

const WRITER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
const SETTLE_POLL: Duration = Duration::from_millis(1);
const TASK_ID: &str = "task1";
const SUB_TOOL_ID: &str = "sub_t1";
const TOOL_OUTPUT_LINE: &str = "hello from the subagent";
const LATE_MODEL_SPEC: &str = "ollama/glm-5";
const MODEL_SPEC_GLM4: &str = "ollama/glm-4";
const MODEL_SPEC_GPT9: &str = "openai/gpt-9";
const HINT_PLUGIN: &str = "statusline";
const HINT_TEXT: &str = "2/4 staged";
const HINT_STYLE: &str = "fg";
const RETRY_MESSAGE: &str = "overloaded";
const RETRY_DELAY: Duration = Duration::from_secs(5);
const MISSING_DIR: &str = "gone";
const WALK_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_IMAGE_DATA: &str = "dGVzdA==";
const IMAGE_PATH_PASTE: &str = "file:///tmp/nonexistent.png";
const LOCAL_COMMAND_ATTACHMENTS_ERROR: &str =
    "command failed: local commands cannot include non-text content";
const REJECTED_COMMAND_SHELL_COMMAND: &str = "pwd";
const REJECTED_COMMAND_SHELL_OUTPUT: &str = "command shell cwd";
const RUNNING_TURN_TEXT: &str = "running turn";
const FOLDED_TEXT: &str = "folded into the running turn";
const PLAN_OPTIONS_MODEL: &str = "anthropic/claude-opus-4-6";
const PLAN_OPTIONS_OTHER: &str = "anthropic/claude-sonnet-4-6";
const PLAN_OPTIONS_LONG_MODEL: &str =
    "custom/a-long-implementation-model-identity-that-wraps-across-narrow-terminal-widths";
const PLAN_OPTIONS_PATH: &str = "model-options-plan.md";
const PLAN_OPTIONS_CHILD_PATH: &str = "child-model-options-plan.md";
const PLAN_OPTIONS_REPLACEMENT_PATH: &str = "replacement-model-options-plan.md";

fn set_zone(app: &mut App, zone: SelectionZone, area: Rect) {
    app.zones.push(SelectableZone { area, zone });
}

fn build_app(dir: StateDir, writer: Arc<StorageWriter>) -> App {
    build_app_with_registry(dir, writer, makima_commands::CommandRegistry::new())
}

fn build_app_with_handle(
    dir: StateDir,
    writer: Arc<StorageWriter>,
    handle: makima_lua::EventHandle,
) -> App {
    build_app_with_full(
        dir,
        writer,
        makima_commands::CommandRegistry::new(),
        handle,
        UiConfig::default(),
    )
}

fn build_app_with_registry(
    dir: StateDir,
    writer: Arc<StorageWriter>,
    registry: makima_commands::CommandRegistry,
) -> App {
    build_app_with_full(
        dir,
        writer,
        registry,
        makima_lua::EventHandle::disconnected_for_test(),
        UiConfig::default(),
    )
}

#[derive(Clone)]
struct TestLuaCompletion {
    handle: makima_lua::EventHandle,
    plugin: Arc<str>,
}

impl makima_commands::CommandCompletion for TestLuaCompletion {
    fn complete(
        &self,
        context: makima_commands::CompletionContext,
        _cancellation: makima_commands::CancellationToken,
    ) -> makima_commands::CommandFuture<
        Result<Vec<makima_commands::CompletionItem>, makima_commands::CompletionError>,
    > {
        let (_, cancel) = makima_agent::CancelToken::new();
        let command_generation = self
            .handle
            .command_generation_for_test(&self.plugin, &context.invoked_name);
        let context = makima_lua::CommandArgumentContext {
            command: Arc::clone(&context.invoked_name),
            plugin: Arc::clone(&self.plugin),
            args: context.arguments.to_string(),
            arg: context.argument.to_string(),
            index: context.argument_index,
            mode: context.mode.to_string(),
            session: 1,
            generation: 0,
            command_generation,
            argument_name: context.argument_name,
            argument_kind: context
                .argument_kind
                .as_ref()
                .map(|kind| kind.type_name().to_owned()),
            preceding_arguments: context.preceding_arguments.clone(),
        };
        let Some(rx) = self.handle.collect_command_argument_items(context, cancel) else {
            return Box::pin(async { Ok(Vec::new()) });
        };
        Box::pin(async move {
            Ok(rx
                .recv_async()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|item| makima_commands::CompletionItem {
                    label: Arc::from(item.label),
                    insertion: Arc::from(item.insertion),
                    description: item.description.map(Arc::from),
                })
                .collect())
        })
    }

    fn lifecycle(
        &self,
        context: &makima_commands::CompletionContext,
        event: &makima_commands::CompletionLifecycleEvent,
        _cancellation: &makima_commands::CancellationToken,
    ) -> Result<(), makima_commands::CompletionError> {
        let (event, item) = match event {
            makima_commands::CompletionLifecycleEvent::Highlight(item) => (
                makima_lua::CommandArgumentLifecycle::Highlight,
                Some(makima_lua::CommandArgumentItem {
                    label: item.label.to_string(),
                    insertion: item.insertion.to_string(),
                    description: item.description.as_ref().map(ToString::to_string),
                }),
            ),
            makima_commands::CompletionLifecycleEvent::Accept(item) => (
                makima_lua::CommandArgumentLifecycle::Accept,
                Some(makima_lua::CommandArgumentItem {
                    label: item.label.to_string(),
                    insertion: item.insertion.to_string(),
                    description: item.description.as_ref().map(ToString::to_string),
                }),
            ),
            makima_commands::CompletionLifecycleEvent::Cancel => {
                (makima_lua::CommandArgumentLifecycle::Cancel, None)
            }
        };
        self.handle.command_argument_lifecycle(
            makima_lua::CommandArgumentContext {
                command: Arc::clone(&context.invoked_name),
                plugin: Arc::clone(&self.plugin),
                args: context.arguments.to_string(),
                arg: context.argument.to_string(),
                index: context.argument_index,
                mode: context.mode.to_string(),
                session: 1,
                generation: 0,
                command_generation: self
                    .handle
                    .command_generation_for_test(&self.plugin, &context.invoked_name),
                argument_name: context.argument_name.clone(),
                argument_kind: context
                    .argument_kind
                    .as_ref()
                    .map(|kind| kind.type_name().to_owned()),
                preceding_arguments: context.preceding_arguments.clone(),
            },
            event,
            item,
            makima_agent::CancelToken::none(),
        );
        Ok(())
    }
}

struct TestLuaCommand {
    handle: makima_lua::EventHandle,
    name: Arc<str>,
    plugin: Arc<str>,
    max_args: Option<usize>,
    completion: bool,
}

fn test_command_arguments(
    max_args: Option<usize>,
    completion: bool,
) -> makima_commands::CommandArguments {
    match (max_args, completion) {
        (Some(0), _) => makima_commands::CommandArguments::Positional(Arc::from([])),
        (_, true) => makima_commands::CommandArguments::Positional(Arc::from([
            makima_commands::PositionalArgument {
                name: Arc::from("arg"),
                kind: makima_commands::ArgumentKind::String,
                optional: true,
                variadic: max_args.is_none(),
                completion: makima_commands::CompletionPolicy::Replace,
            },
        ])),
        (Some(1), false) => makima_commands::CommandArguments::Positional(Arc::from([
            makima_commands::PositionalArgument::optional(
                "arg",
                makima_commands::ArgumentKind::String,
            ),
        ])),
        (None, false) => makima_commands::CommandArguments::Raw { required: false },
        (Some(_), false) => makima_commands::CommandArguments::Positional(Arc::from([])),
    }
}

fn register_test_lua_command(
    registry: &makima_commands::CommandRegistry,
    command: TestLuaCommand,
) -> makima_commands::Producer {
    let producer = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
    let completion = command.completion.then(|| {
        Arc::new(TestLuaCompletion {
            handle: command.handle.clone(),
            plugin: Arc::clone(&command.plugin),
        }) as Arc<dyn makima_commands::CommandCompletion>
    });
    producer
        .replace(vec![makima_commands::Registration {
            spec: makima_commands::CommandSpec {
                name: Arc::clone(&command.name),
                aliases: Arc::from([]),
                arguments: test_command_arguments(command.max_args, command.completion),
                docs: makima_commands::CommandDocs {
                    summary: Arc::from("Lua test command"),
                    argument_hint: None,
                },
                required_capabilities: makima_commands::TargetCapabilities::default(),
            },
            behavior: command
                .handle
                .command_behavior_for_test(&command.plugin, &command.name),
            argument_completions: if command.completion {
                vec![completion]
            } else if command.max_args == Some(1) {
                vec![None]
            } else {
                Vec::new()
            },
        }])
        .unwrap();
    producer
}

fn lua_registry(
    command: TestLuaCommand,
) -> (makima_commands::CommandRegistry, makima_commands::Producer) {
    let registry = makima_commands::CommandRegistry::new();
    let producer = register_test_lua_command(&registry, command);
    (registry, producer)
}

fn lua_registry_with_commands(
    commands: Vec<TestLuaCommand>,
) -> (makima_commands::CommandRegistry, makima_commands::Producer) {
    let registry = makima_commands::CommandRegistry::new();
    let producer = registry.create_producer(makima_commands::ProducerPrecedence::Plugin);
    let registrations = commands
        .into_iter()
        .map(|command| {
            let completion = command.completion.then(|| {
                Arc::new(TestLuaCompletion {
                    handle: command.handle.clone(),
                    plugin: Arc::clone(&command.plugin),
                }) as Arc<dyn makima_commands::CommandCompletion>
            });
            makima_commands::Registration {
                spec: makima_commands::CommandSpec {
                    name: Arc::clone(&command.name),
                    aliases: Arc::from([]),
                    arguments: test_command_arguments(command.max_args, command.completion),
                    docs: makima_commands::CommandDocs {
                        summary: Arc::from("Lua test command"),
                        argument_hint: None,
                    },
                    required_capabilities: makima_commands::TargetCapabilities::default(),
                },
                behavior: command
                    .handle
                    .command_behavior_for_test(&command.plugin, &command.name),
                argument_completions: if command.completion {
                    vec![completion]
                } else if command.max_args == Some(1) {
                    vec![None]
                } else {
                    Vec::new()
                },
            }
        })
        .collect();
    producer.replace(registrations).unwrap();
    (registry, producer)
}

fn build_app_with_full(
    dir: StateDir,
    writer: Arc<StorageWriter>,
    registry: makima_commands::CommandRegistry,
    handle: makima_lua::EventHandle,
    ui: UiConfig,
) -> App {
    let model = test_model();
    let command_runtime = Arc::new(crate::command_runtime::CommandRuntime::new_for_test(
        &[],
        registry,
        Arc::new(crate::components::arg_completion::ModelArgSource::new(
            Arc::new(ArcSwapOption::empty()),
            seeded(&[], &[]),
        )),
        Arc::new(crate::components::arg_completion::ThemeArgSource::new(
            Arc::new(crate::theme::InMemoryThemesProvider::bundled()),
        )),
    ));
    App::new(
        &model,
        AppSession::new("test-model", "/tmp/test"),
        dir,
        Arc::new(ArcSwapOption::empty()),
        McpSnapshotReader::empty(),
        McpConfigErrors::new(PathBuf::new()),
        KeymapReader::empty(),
        HintReader::empty(),
        makima_lua::StatusContentReader::empty(),
        writer,
        ui,
        100,
        Arc::new(PermissionManager::new(
            PermissionsConfig {
                rules: vec![],
                ..Default::default()
            },
            PathBuf::from("/tmp"),
            makima_config::ProjectConfig::for_project(Path::new("/tmp")),
            Arc::default(),
        )),
        handle,
        Arc::new(makima_config::ModelPolicy::default()),
        Arc::new(crate::theme::InMemoryThemesProvider::bundled()),
        command_runtime,
    )
}

fn test_writer(dir: StateDir) -> StorageWriter {
    StorageWriter::new(dir, flume::unbounded().0)
}

fn app_with_custom_commands(commands: &[CustomCommand]) -> App {
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let model = test_model();
    let handle = makima_lua::EventHandle::disconnected_for_test();
    let command_runtime = Arc::new(crate::command_runtime::CommandRuntime::new_for_test(
        commands,
        makima_commands::CommandRegistry::new(),
        Arc::new(crate::components::arg_completion::ModelArgSource::new(
            Arc::new(ArcSwapOption::empty()),
            seeded(&[], &[]),
        )),
        Arc::new(crate::components::arg_completion::ThemeArgSource::new(
            Arc::new(crate::theme::InMemoryThemesProvider::bundled()),
        )),
    ));
    let mut app = App::new(
        &model,
        AppSession::new("test-model", "/tmp/test"),
        dir,
        Arc::new(ArcSwapOption::empty()),
        McpSnapshotReader::empty(),
        McpConfigErrors::new(PathBuf::new()),
        KeymapReader::empty(),
        HintReader::empty(),
        makima_lua::StatusContentReader::empty(),
        writer,
        UiConfig::default(),
        100,
        Arc::new(PermissionManager::new(
            PermissionsConfig {
                rules: vec![],
                ..Default::default()
            },
            PathBuf::from("/tmp"),
            makima_config::ProjectConfig::for_project(Path::new("/tmp")),
            Arc::default(),
        )),
        handle,
        Arc::new(ModelPolicy::default()),
        Arc::new(crate::theme::InMemoryThemesProvider::bundled()),
        command_runtime,
    );
    bind_root(&mut app, parked_root());
    app
}

pub(crate) fn test_app() -> App {
    app_with_root(parked_root())
}

/// The root's runner never starts, so queued work stays put for assertions.
fn parked_root() -> AgentRef {
    let (start_tx, start_rx) = flume::bounded(1);
    AgentManagerHandle::new(Default::default())
        .unwrap()
        .create_root_deferred_with_config(
            Some(root_config()),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(ParkedBackend(start_tx)) as Box<dyn ActorBackend>),
            start_rx,
        )
        .unwrap()
}

/// A root whose first turn is running and never finishes.
fn running_root_app() -> (App, flume::Receiver<AgentInput>) {
    let (tx, rx) = flume::unbounded();
    let root = AgentManagerHandle::new(Default::default())
        .unwrap()
        .create_root_with_config(Some(root_config()), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(FocusedInputBackend(tx)) as Box<dyn ActorBackend>)
        })
        .unwrap();
    let actor = root.actor().unwrap();
    let mut app = app_with_root(root);
    let run_id = actor.reserve_presentation(app.run_id);
    actor
        .submit(
            AgentInput::new(RUNNING_TURN_TEXT.into(), Vec::new()),
            None,
            String::new(),
            makima_agent::actor::TurnAdmissionOptions {
                already_displayed: true,
                displayed_presentation: Some(run_id),
                ..Default::default()
            },
        )
        .unwrap();
    app.record_run_start(run_id);
    rx.recv_timeout(SETTLE_TIMEOUT).unwrap();
    (app, rx)
}

/// A host turn can start before the app hears of it. Typed input that folds
/// into that turn must follow its presentation, not the one reserved for a
/// fresh start, or the turn's output is fenced off as stale.
#[test]
fn starting_submit_follows_the_turn_it_folds_into() {
    let (tx, rx) = flume::unbounded();
    let root = AgentManagerHandle::new(Default::default())
        .unwrap()
        .create_root_with_config(Some(root_config()), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(FocusedInputBackend(tx)) as Box<dyn ActorBackend>)
        })
        .unwrap();
    let mut app = app_with_root(root.clone());
    let message = |text: &str| QueuedMessage {
        text: text.into(),
        images: Vec::new(),
    };
    let (_, host) = root
        .actor()
        .unwrap()
        .submit_presented(
            app.build_agent_input(&message(RUNNING_TURN_TEXT)),
            None,
            String::new(),
            Default::default(),
        )
        .unwrap();
    rx.recv_timeout(SETTLE_TIMEOUT).unwrap();
    assert_ne!(app.status, Status::Streaming);
    let typed = app.build_agent_input(&message(FOLDED_TEXT));
    assert!(
        app.admit_user_turn(&root, typed, Some(FOLDED_TEXT.into()))
            .unwrap()
    );
    assert_eq!(app.run_id, host);
}

fn app_with_root(root: AgentRef) -> App {
    let dir = StateDir::from_path(env::temp_dir());
    let mut app = build_app(dir.clone(), Arc::new(test_writer(dir)));
    bind_root(&mut app, root);
    app
}

fn root_config() -> EffectiveAgentConfig {
    EffectiveAgentConfig::new(
        makima_agent::RunSettings {
            provider: cli_tool_ctx(std::env::current_dir().unwrap()).provider,
            model: test_model(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        },
        AgentMode::Build,
    )
    .with_mode_def(makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build))
}

fn root_actor(app: &App) -> makima_agent::actor::AgentActorHandle {
    app.queue.root_actor().unwrap().clone()
}

fn root_submissions(app: &App) -> Vec<String> {
    root_actor(app)
        .pending_submissions()
        .into_iter()
        .map(|entry| entry.text)
        .collect()
}

fn commit_root_config(app: &App, change: makima_agent::actor::ConfigChange) {
    let actor = root_actor(app);
    smol::block_on(
        actor
            .reserve_prepared_operation(None)
            .unwrap()
            .commit_when_ready(Some(change)),
    )
    .unwrap();
}

/// Commits `mode` on the root and follows it the way the event loop does.
fn set_root_mode(app: &mut App, mode: Mode) {
    let mode_def = mode.def(&app.lua_event_handle.mode_registry());
    let agent_mode = match mode {
        Mode::Build => AgentMode::Build,
        Mode::Plan => {
            if app.state.plan.path().is_none() {
                app.state.plan = PlanState::Drafting(
                    makima_storage::plans::new_plan_path(&app.storage).unwrap(),
                );
            }
            AgentMode::Plan(app.state.plan.path().unwrap().to_path_buf())
        }
        Mode::Custom(name) => AgentMode::Custom(makima_agent::ModeId::Custom(name)),
    };
    let previous = app.root_config().mode.clone();
    commit_root_config(
        app,
        makima_agent::actor::ConfigChange::Mode {
            mode: agent_mode.clone(),
            mode_def: Some(mode_def),
        },
    );
    app.project_root_mode(&previous, &agent_mode);
}

fn commit_root_model(app: &App, model: Model) {
    commit_root_config(
        app,
        makima_agent::actor::ConfigChange::Patch(makima_agent::actor::ConfigPatch {
            model: Some(makima_agent::actor::PreparedModel {
                provider: cli_tool_ctx(std::env::current_dir().unwrap()).provider,
                model,
            }),
            ..Default::default()
        }),
    );
}

pub(crate) fn bind_root(app: &mut App, root: AgentRef) {
    app.queue
        .set_shared(crate::agent::shared_queue::actor_queue(Arc::new(
            root.actor().unwrap(),
        )));
    app.chats[0].agent_id = Some(root.id());
    bind_actor_target(app, root);
    app.sync_focused_queue();
}

/// Owns the runner's start sender so the deferred runner stays parked, and
/// with it every queued submission, for as long as the actor lives.
struct ParkedBackend(#[allow(dead_code)] flume::Sender<()>);

impl ActorBackend for ParkedBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: AgentInput,
        _: makima_agent::WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(std::future::pending())
    }

    fn run_control<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: &'a makima_agent::ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(std::future::pending())
    }

    fn run_compact<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

pub(crate) fn actor_presentation_app(target: AgentRef, run_id: u64) -> App {
    let mut app = app_with_root(target);
    app.run_id = run_id;
    app
}

pub(crate) fn assert_actor_response(app: &mut App, response: &str) {
    assert_eq!(app.main_chat().last_message_text(), response);
}

pub(crate) fn assert_actor_presentation(app: &mut App, run_id: u64, response: &str, text: &str) {
    assert_eq!(app.run_id, run_id);
    assert!(
        !app.main_chat().streaming_text_is_empty(),
        "missing streamed response: {response}"
    );
    assert!((0..app.main_chat().message_count()).any(|index| {
        app.main_chat()
            .message_at(index)
            .is_some_and(|message| message.text == text)
    }));
    assert_eq!(app.status, Status::Streaming);
}

/// A `test_app` past its idle splash, whose drifting starfield would mask
/// every other cadence.
fn app_without_splash() -> App {
    let mut app = streaming_app();
    app.update(agent_msg(AgentEvent::TextDelta { text: "hi".into() }));
    app.update(done_event());
    app
}

/// Hands back the slot providers publish their model lists into, since the app
/// keeps no handle to it once the picker owns it.
fn app_with_model_slot() -> (App, Arc<ArcSwapOption<Vec<String>>>) {
    let mut app = test_app();
    let models = install_model_slot(&mut app);
    (app, models)
}

fn install_model_slot(app: &mut App) -> Arc<ArcSwapOption<Vec<String>>> {
    let models = Arc::new(ArcSwapOption::empty());
    let options = ModelCapabilities {
        thinking: ThinkingSupport::Yes,
        fast: true,
    };
    let resolved = [
        PLAN_OPTIONS_MODEL,
        PLAN_OPTIONS_OTHER,
        PLAN_OPTIONS_LONG_MODEL,
        LATE_MODEL_SPEC,
    ]
    .map(|spec| (spec, options));
    app.model_picker = ModelPicker::new(
        Arc::clone(&models),
        Arc::new(offline_ids(&[
            TEST_MODEL_PROVIDER,
            "ollama",
            "openai",
            "zai",
        ])),
        seeded(&resolved, &[]),
    );
    app.available_models = Arc::clone(&models);
    models
}

/// Hands back the end a plugin publishes hints through. That is the Lua thread
/// in production, and this test here. Seeding the watch from the new reader is
/// what `App::new` does, and skipping it would make the first poll report the
/// swap itself.
fn app_with_hints() -> (App, HintWriterHandle) {
    let (writer, reader) = hint_writer_pair();
    let mut app = test_app();
    app.hints = Watch::seeded(reader.load_full());
    app.hint_reader = reader;
    (app, writer)
}

fn tempdir_app() -> (TempDir, StateDir, Arc<StorageWriter>, App) {
    let tmp = TempDir::new().unwrap();
    let dir = StateDir::from_path(tmp.path().to_path_buf());
    let writer = Arc::new(test_writer(dir.clone()));
    let mut app = build_app(dir.clone(), Arc::clone(&writer));
    bind_root(&mut app, parked_root());
    (tmp, dir, writer, app)
}

fn mouse_event(kind: MouseEventKind, column: u16, row: u16) -> Msg {
    Msg::Mouse(MouseEvent {
        kind,
        column,
        row,
        modifiers: KeyModifiers::NONE,
    })
}

fn agent_msg(event: AgentEvent) -> Msg {
    agent_msg_with_run_id(event, 1)
}

/// Actor work stamped with the given presentation. Tests use the presentation
/// number as their run id, matching what admission now owns.
fn agent_msg_with_run_id(event: AgentEvent, presentation: u64) -> Msg {
    Msg::Agent(Box::new(Envelope {
        event,
        subagent: None,
        identity: Some(work_identity(presentation)),
    }))
}

pub(crate) fn work_identity(presentation: u64) -> makima_agent::ActorWorkIdentity {
    makima_agent::ActorWorkIdentity::new(
        AgentId::generate(),
        makima_agent::ActorWorkKind::Control {
            operation_id: presentation,
        },
        presentation,
    )
}

fn done() -> AgentEvent {
    AgentEvent::TurnOutcome(TurnOutcome::completed(
        AgentId::generate(),
        TurnId::generate(),
        TokenUsage::default(),
        1,
        DoneReason::EndTurn,
    ))
}

fn done_event() -> Msg {
    agent_msg(done())
}

fn subagent_info(parent_id: &str, name: &str) -> SubagentInfo {
    subagent_info_with_tx(parent_id, name, None)
}

fn subagent_info_with_tx(
    parent_id: &str,
    name: &str,
    answer_tx: Option<flume::Sender<String>>,
) -> SubagentInfo {
    subagent_info_full(parent_id, name, answer_tx, None)
}

thread_local! {
    static SUBAGENT_IDS: RefCell<HashMap<String, AgentId>> = RefCell::new(HashMap::new());
}

fn subagent_info_full(
    parent_id: &str,
    name: &str,
    answer_tx: Option<flume::Sender<String>>,
    input_tx: Option<flume::Sender<String>>,
) -> SubagentInfo {
    let agent_id = SUBAGENT_IDS.with(|agent_ids| {
        *agent_ids
            .borrow_mut()
            .entry(parent_id.to_owned())
            .or_insert_with(AgentId::generate)
    });
    subagent_info_for_agent(agent_id, parent_id, name, answer_tx, input_tx)
}

fn subagent_info_for_agent(
    agent_id: AgentId,
    parent_id: &str,
    name: &str,
    answer_tx: Option<flume::Sender<String>>,
    _input_tx: Option<flume::Sender<String>>,
) -> SubagentInfo {
    SubagentInfo {
        agent_id,
        parent_agent_id: None,
        parent_tool_use_id: parent_id.into(),
        name: name.into(),
        prompt: None,
        model: None,
        opts: None,
        answer_tx,
        cancel: None,
    }
}

const POST_TEXT: &str = "review found issues";
/// No run uses it, so a post must get past the stale-run filter.
const UNRELATED_RUN_ID: u64 = 99;

fn post_msg(subagent: Option<SubagentInfo>) -> (Msg, Post) {
    let post = Post {
        text: POST_TEXT.into(),
        level: PostLevel::Error,
        agent: None,
    };
    let msg = Msg::Agent(Box::new(Envelope {
        event: AgentEvent::Post(post.clone()),
        subagent,
        identity: Some(work_identity(UNRELATED_RUN_ID)),
    }));
    (msg, post)
}

fn assert_last_is_post(chat: &Chat) {
    assert_eq!(chat.last_message_text(), POST_TEXT);
    assert_eq!(
        chat.last_message_role(),
        Some(&DisplayRole::Post(PostLevel::Error))
    );
}

#[test]
fn main_chat_post_is_recorded_and_restored() {
    let mut app = test_app();
    let (msg, post) = post_msg(None);
    app.update(msg);
    assert_last_is_post(&app.chats[0]);
    assert_eq!(
        app.state.session.posts(),
        [StoredPost {
            chat: None,
            after_message: 0,
            post,
        }]
    );

    app.reset_ui_chrome();
    app.restore_display();
    assert_last_is_post(&app.chats[0]);
}

#[test_case(false, false, false, false ; "live_link")]
#[test_case(true, false, false, false ; "restored_link")]
#[test_case(false, true, false, false ; "missing_target")]
#[test_case(false, false, true, false ; "drag_selects_text")]
#[test_case(false, false, false, true ; "ambiguous_target")]
fn linked_post_opens_its_agent(restored: bool, missing: bool, drag: bool, ambiguous: bool) {
    let mut app = test_app();
    app.resolve_or_create_chat(&subagent_info(TASK_ID, "reviewer"));
    if ambiguous {
        app.resolve_or_create_chat(&subagent_info_for_agent(
            AgentId::generate(),
            TASK_ID,
            "other reviewer",
            None,
            None,
        ));
    }
    app.push_post(
        None,
        Post {
            text: POST_TEXT.into(),
            level: PostLevel::Warn,
            agent: Some(if missing { SUB_TOOL_ID } else { TASK_ID }.into()),
        },
        None,
    );
    if restored {
        app.reset_ui_chrome();
        app.restore_display();
    }
    render_app(&mut app, 100, 40);
    let area = app.msg_area();
    let row = (area.y..area.bottom())
        .find(|&row| app.chats[0].post_agent_at(row, area).is_some())
        .expect("linked post is visible");
    let column = area.x;
    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Left),
        column,
        row,
    ));
    let end_column = if drag { column + 5 } else { column };
    if drag {
        app.update(mouse_event(
            MouseEventKind::Drag(MouseButton::Left),
            end_column,
            row,
        ));
    }
    app.update(mouse_event(
        MouseEventKind::Up(MouseButton::Left),
        end_column,
        row,
    ));
    assert_eq!(
        app.active_chat,
        usize::from(!missing && !drag && !ambiguous)
    );
    if missing || ambiguous {
        assert_eq!(app.status_bar.flash_text(), Some(FLASH_NO_AGENT_CHAT));
    }
    if drag {
        assert!(matches!(
            app.selection_state,
            Some(SelectionState::PendingCopy { .. })
        ));
    }
}

#[test]
fn subagent_post_lands_in_the_child_chat() {
    let mut app = test_app();
    let (msg, post) = post_msg(Some(subagent_info(TASK_ID, "reviewer")));
    app.update(msg);
    let chat_idx = app.chat_index[TASK_ID];
    assert_ne!(chat_idx, 0);
    assert_last_is_post(&app.chats[chat_idx]);
    assert_eq!(
        app.state.session.posts(),
        [StoredPost {
            chat: Some(TASK_ID.into()),
            after_message: 0,
            post,
        }]
    );
}

fn subagent_msg(event: AgentEvent, parent_id: &str, name: Option<&str>) -> Msg {
    subagent_msg_with_run_id(event, parent_id, name, 1)
}

fn subagent_msg_with_run_id(
    event: AgentEvent,
    parent_id: &str,
    name: Option<&str>,
    presentation: u64,
) -> Msg {
    Msg::Agent(Box::new(Envelope {
        event,
        subagent: Some(subagent_info(parent_id, name.unwrap_or("Agent"))),
        identity: Some(work_identity(presentation)),
    }))
}

fn subagent_msg_with_prompt(
    event: AgentEvent,
    parent_id: &str,
    name: Option<&str>,
    prompt: Option<&str>,
) -> Msg {
    let mut info = subagent_info(parent_id, name.unwrap_or("Agent"));
    info.prompt = prompt.map(String::from);
    Msg::Agent(Box::new(Envelope {
        event,
        subagent: Some(info),
        identity: Some(work_identity(1)),
    }))
}

fn subagent_msg_with_model(event: AgentEvent, parent_id: &str, name: &str, model: &str) -> Msg {
    let mut info = subagent_info(parent_id, name);
    info.model = Some(model.into());
    Msg::Agent(Box::new(Envelope {
        event,
        subagent: Some(info),
        identity: Some(work_identity(1)),
    }))
}

fn tool_start(id: &str, tool: &str) -> AgentEvent {
    AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: id.into(),
        tool: tool.into(),
        summary: id.into(),
        annotation: None,
        input: None,
        raw_input: None,
        output: None,
        render_header: None,
    }))
}

fn turn_complete(usage: TokenUsage, model: &str, cost: Option<f64>) -> AgentEvent {
    AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
        message: Default::default(),
        usage,
        model: model.into(),
        cost,
        context_size: None,
        context_window: 0,
    }))
}

fn tool_results_submitted() -> AgentEvent {
    AgentEvent::ToolResultsSubmitted {
        message: Box::new(Message::user(String::new())),
    }
}

#[test]
fn typing_and_submit() {
    let mut app = test_app();
    app.update(Msg::Key(key(KeyCode::Char('h'))));
    app.update(Msg::Key(key(KeyCode::Char('i'))));

    assert!(app.update(Msg::Key(key(KeyCode::Enter))).is_empty());
    assert_eq!(root_submissions(&app), ["hi"]);
    assert_eq!(app.status, Status::Streaming);
    // Regression check: the bubble has to be on screen the same frame we
    // submit, otherwise it briefly sits one row too high before snapping down.
    assert_eq!(
        app.main_chat().last_message_role(),
        Some(&DisplayRole::User),
    );
    assert_eq!(app.main_chat().last_message_text(), "hi");
}

#[test]
fn mailbox_wake_starts_without_an_empty_user_bubble() {
    let mut app = test_app();
    let actions = app.start_mailbox_run(vec![Message::observation("failed".into())]);

    assert!(matches!(
        &actions[..],
        [Action::SendMessage(input)]
            if input.message.is_empty()
                && input.preamble.len() == 1
                && input.preamble[0].is_observation()
    ));
    assert_eq!(app.status, Status::Streaming);
    assert!(app.main_chat().segment_search_texts().is_empty());
}

fn with_text(app: &mut App) {
    app.update(Msg::Key(key(KeyCode::Char('h'))));
    app.update(Msg::Key(key(KeyCode::Char('i'))));
}

fn with_image(app: &mut App) {
    let img = ImageSource::new(ImageMediaType::Png, Arc::from(TEST_IMAGE_DATA));
    app.input_box.attach_image(img);
}

#[test_case(with_text as fn(&mut App)  ; "clears_text")]
#[test_case(with_image as fn(&mut App) ; "clears_image")]
fn ctrl_c_clears_nonempty_input(setup: fn(&mut App)) {
    let mut app = test_app();
    setup(&mut app);
    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));
    assert!(actions.is_empty());
    assert_eq!(app.exit_request, ExitRequest::None);
    assert!(app.input_box.is_empty());
}

#[test]
fn ctrl_c_quits_when_input_empty() {
    let mut app = test_app();
    app.status = Status::Idle;
    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));
    assert_eq!(app.exit_request, ExitRequest::Success);
    assert!(matches!(actions.as_slice(), [Action::ManualExit]));
}

#[test_case(done(), ExitRequest::Success ; "done_exits_success")]
#[test_case(AgentEvent::ControlError { message: "boom".into() }, ExitRequest::Error ; "error_exits_error")]
fn exit_on_done_flag_triggers_exit(event: AgentEvent, expected: ExitRequest) {
    let mut app = test_app();
    app.exit_on_done = true;
    app.status = Status::Streaming;
    app.run_id = 1;
    app.update(agent_msg(event));
    assert_eq!(app.exit_request, expected);
}

#[test]
fn standalone_compaction_returns_to_idle_without_ending_the_session() {
    let mut app = test_app();
    app.exit_on_done = true;
    app.status = Status::Streaming;
    app.run_id = 1;
    app.update(agent_msg(AgentEvent::ControlComplete {
        usage: TokenUsage::default(),
    }));
    assert_eq!(app.exit_request, ExitRequest::None);
    assert_eq!(app.status, Status::Idle);
}

#[test]
fn shift_tab_toggles_mode_without_mutating_state() {
    let mut app = test_app();
    assert_eq!(app.root_mode(), Mode::Build);

    assert!(app.update(Msg::Key(key(KeyCode::Tab))).is_empty());
    assert_eq!(app.root_mode(), Mode::Build);

    let actions = app.update(Msg::Key(key(KeyCode::BackTab)));
    assert!(matches!(&actions[..], [Action::ChangeMode(id)] if id == "plan"));
    assert_eq!(app.root_mode(), Mode::Build);
}

#[test_case(ToolOutput::Plain("wrote 100 bytes to /tmp/plans/test.md".into()), Some("/tmp/plans/test.md".into()), true  ; "write_matching")]
#[test_case(ToolOutput::Diff { path: "/tmp/plans/test.md".into(), before: String::new(), after: String::new(), summary: String::new() }, None, true  ; "edit_matching")]
#[test_case(ToolOutput::Plain("wrote 100 bytes to /tmp/other.rs".into()), Some("/tmp/other.rs".into()), false ; "write_non_matching")]
fn tool_done_transitions_plan_to_ready(
    output: ToolOutput,
    written_path: Option<String>,
    expect_ready: bool,
) {
    let mut app = test_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting(PathBuf::from("/tmp/plans/test.md"));
    app.status = Status::Streaming;
    app.run_id = 1;

    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t1".into(),
        tool: "write".into(),
        output,
        is_error: false,
        annotation: None,
        written_path,
    }))));

    assert_eq!(app.state.plan.is_ready(), expect_ready);
}

#[test]
fn altgr_chars_not_swallowed_by_ctrl_handler() {
    let mut app = test_app();
    let altgr_backslash = KeyEvent {
        code: KeyCode::Char('\\'),
        modifiers: KeyModifiers::CONTROL | KeyModifiers::ALT,
        kind: crossterm::event::KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    };
    app.update(Msg::Key(key(KeyCode::Char('h'))));
    app.update(Msg::Key(key(KeyCode::Char('i'))));
    app.update(Msg::Key(altgr_backslash));
    assert_eq!(app.input_box.buffer.value(), "hi\\");
}

#[test_case(Status::Idle      ; "idle")]
#[test_case(Status::Streaming ; "streaming")]
fn paste_works_regardless_of_status(status: Status) {
    let mut app = test_app();
    app.status = status;
    app.update(Msg::Paste("pasted".into()));
    assert_eq!(app.input_box.buffer.value(), "pasted");
}

#[test]
fn ordinary_paste_synchronizes_argument_completion() {
    let dir = StateDir::from_path(env::temp_dir());
    let handle = makima_lua::EventHandle::disconnected_for_test();
    let (registry, _producer) = lua_registry(TestLuaCommand {
        handle,
        name: Arc::from("/deploy"),
        plugin: Arc::from("deploy"),
        max_args: Some(1),
        completion: true,
    });
    let mut app = build_app_with_registry(dir.clone(), Arc::new(test_writer(dir)), registry);
    let generation = app.command_palette.argument_generation();

    app.update(Msg::Paste("/deploy staging".into()));
    settle_command_palette(&mut app);

    assert!(app.command_palette.argument_generation() > generation);
}

#[test_case("a\rb\rc",       "a\nb\nc"       ; "bare_cr")]
#[test_case("a\r\nb\r\nc",   "a\nb\nc"       ; "crlf")]
#[test_case("a\r\nb\rc\nd",  "a\nb\nc\nd"    ; "mixed")]
fn paste_normalizes_line_endings(input: &str, expected: &str) {
    let mut app = test_app();
    app.update(Msg::Paste(input.into()));
    assert_eq!(app.input_box.buffer.value(), expected);
}

#[test]
fn paste_file_path_triggers_image_load() {
    let mut app = test_app();
    app.update(Msg::Paste(IMAGE_PATH_PASTE.into()));
    assert!(!app.image_paste_rx.is_empty());
    assert_eq!(app.input_box.buffer.value(), "");
}

#[test]
fn submit_during_streaming_queues_message() {
    let mut app = test_app();
    app.update(Msg::Key(key(KeyCode::Char('a'))));
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.status, Status::Streaming);

    app.update(Msg::Key(key(KeyCode::Char('b'))));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(actions.is_empty());
    assert_eq!(app.queue.len(), 1);
}

#[test_case(COMPLETION_PREVIEW_CHARS - 1, false, false; "short")]
#[test_case(COMPLETION_PREVIEW_CHARS, false, false; "at_limit")]
#[test_case(COMPLETION_PREVIEW_CHARS + 1, false, true; "ui_truncated")]
#[test_case(10, true, true; "delivery_truncated")]
fn typed_completion_notices_render_once_and_skip_withdrawn(
    chars: usize,
    delivery_truncated: bool,
    truncated: bool,
) {
    use makima_agent::actor::{
        ActorEvent, ChildDelivery, DeliveryEvent, DeliveryStatus, TurnResult,
    };
    const LABEL: &str = "research child";
    const TEXT: &str = "界";
    let mut app = test_app();
    let delivery = Arc::new(ChildDelivery {
        label: Some(LABEL.into()),
        result: TurnResult {
            outcome: TurnOutcome::completed(
                AgentId::generate(),
                TurnId::generate(),
                Default::default(),
                1,
                DoneReason::EndTurn,
            ),
            text: TEXT.repeat(chars),
            output: if delivery_truncated {
                vec![serde_json::json!({
                    "truncated": true,
                    "full_result": {"lookup": "retained turn result"},
                })]
            } else {
                Vec::new()
            },
            provenance: Default::default(),
            duration_ms: 0,
        },
    });
    app.handle_actor_event(ActorEvent::Delivery(DeliveryEvent {
        delivery: Arc::clone(&delivery),
        status: DeliveryStatus::Discarded(makima_agent::TurnCancellationReason::User),
    }));
    assert_eq!(app.main_chat().message_count(), 0);
    let agent_id = delivery.result.outcome.agent_id();
    app.handle_actor_event(ActorEvent::Delivery(DeliveryEvent {
        delivery,
        status: DeliveryStatus::Executed {
            parent_turn_id: TurnId::generate(),
        },
    }));
    let chat = app.main_chat();
    assert_eq!(chat.message_count(), 1);
    let notice = chat.message_at(0).unwrap();
    assert_eq!(notice.role, DisplayRole::Done);
    assert!(notice.text.contains(LABEL));
    let marker = if truncated { COMPLETION_TRUNCATED } else { "" };
    let preview = TEXT.repeat(chars.min(COMPLETION_PREVIEW_CHARS));
    assert_eq!(notice.result_agent, Some(agent_id));
    assert!(
        notice
            .result_preview
            .as_ref()
            .unwrap()
            .starts_with(&format!("[Result from {LABEL}] {preview}{marker}"))
    );
    assert_eq!(
        notice.text.contains("Full result lookup:"),
        delivery_truncated
    );
}

#[test_case(0, false, false, false; "first_child")]
#[test_case(1, false, false, false; "second_child_same_label")]
#[test_case(0, true, false, false; "drag_selects_without_navigation")]
#[test_case(0, false, true, false; "child_unavailable_after_delivery")]
#[test_case(0, false, false, true; "delivery_preview_opens_full_transcript")]
fn completion_notice_navigates_to_child(
    target: usize,
    drag: bool,
    unavailable: bool,
    delivery_preview: bool,
) {
    use makima_agent::actor::{
        ActorEvent, ChildDelivery, DeliveryEvent, DeliveryStatus, TurnResult,
    };
    const LABEL: &str = "research child";
    const TAIL: &str = "full result tail";
    let mut app = app_without_splash();
    let mut children = Vec::new();
    for _ in 0..2 {
        let agent_id = AgentId::generate();
        let info = subagent_info_for_agent(agent_id, TASK_ID, LABEL, None, None);
        let index = app.resolve_or_create_chat(&info);
        let text = format!("{}{TAIL}", "界".repeat(COMPLETION_PREVIEW_CHARS + 1));
        app.chats[index].push(DisplayMessage::new(DisplayRole::Assistant, text.clone()));
        app.handle_actor_event(ActorEvent::Delivery(DeliveryEvent {
            delivery: Arc::new(ChildDelivery {
                label: Some(LABEL.into()),
                result: TurnResult {
                    outcome: TurnOutcome::completed(
                        agent_id,
                        TurnId::generate(),
                        Default::default(),
                        1,
                        DoneReason::EndTurn,
                    ),
                    text: if delivery_preview { LABEL.into() } else { text },
                    output: if delivery_preview {
                        vec![serde_json::json!({
                            "truncated": true,
                            "full_result": {"agent_id": agent_id},
                            "offload": {"path": IMAGE_PATH_PASTE, "complete": true},
                        })]
                    } else {
                        Vec::new()
                    },
                    provenance: Default::default(),
                    duration_ms: 0,
                },
            }),
            status: DeliveryStatus::Executed {
                parent_turn_id: TurnId::generate(),
            },
        }));
        children.push((agent_id, index));
    }
    render_app(&mut app, 80, 60);
    let (agent_id, index) = children[target];
    let notice = (0..app.chats[0].message_count())
        .filter_map(|index| app.chats[0].message_at(index))
        .find(|message| message.result_agent == Some(agent_id))
        .unwrap();
    assert!(
        notice
            .result_preview
            .as_ref()
            .unwrap()
            .contains(COMPLETION_TRUNCATED)
    );
    assert!(notice.text.contains("[View child transcript]"));
    if delivery_preview {
        assert!(notice.text.contains(IMAGE_PATH_PASTE));
    }
    let area = app.msg_area();
    let row = (area.y..area.bottom())
        .find(|&row| {
            app.chats[0]
                .message_at_row(row, area)
                .is_some_and(|message| message.result_agent == Some(agent_id))
        })
        .unwrap();
    if !drag {
        for expanded in [true, false] {
            app.update(mouse_event(
                MouseEventKind::Down(MouseButton::Left),
                area.x,
                row,
            ));
            app.update(mouse_event(
                MouseEventKind::Up(MouseButton::Left),
                area.x,
                row,
            ));
            let notice = (0..app.chats[0].message_count())
                .filter_map(|index| app.chats[0].message_at(index))
                .find(|message| message.result_agent == Some(agent_id))
                .unwrap();
            assert_eq!(notice.result_expanded, expanded);
            assert_eq!(app.active_chat, 0);
            render_app(&mut app, 80, 60);
        }
    }
    if unavailable {
        app.live_chat_index.remove(&agent_id);
    }
    if !drag {
        let action_row = (area.y..area.bottom())
            .find(|&row| app.chats[0].result_navigation_at(row, area) == Some(agent_id))
            .unwrap();
        app.update(mouse_event(
            MouseEventKind::Down(MouseButton::Left),
            area.x,
            action_row,
        ));
        app.update(mouse_event(
            MouseEventKind::Up(MouseButton::Left),
            area.x,
            action_row,
        ));
    } else {
        app.update(mouse_event(
            MouseEventKind::Down(MouseButton::Left),
            area.x,
            row,
        ));
        if drag {
            app.update(mouse_event(
                MouseEventKind::Drag(MouseButton::Left),
                area.x + 3,
                row,
            ));
        }
        app.update(mouse_event(
            MouseEventKind::Up(MouseButton::Left),
            area.x + if drag { 3 } else { 0 },
            row,
        ));
    }
    if drag || unavailable {
        assert_eq!(app.active_chat, 0);
    } else {
        assert_eq!(app.active_chat, index);
        assert!(app.chats[index].last_message_text().ends_with(TAIL));
    }
    if drag {
        assert!(matches!(
            app.selection_state,
            Some(SelectionState::PendingCopy { .. })
        ));
    }
    if unavailable {
        assert_eq!(
            app.status_bar.flash_text(),
            Some(format!("Child transcript unavailable for {agent_id}").as_str())
        );
    }
}

#[test_case("valid", true; "unique_mapping")]
#[test_case("ambiguous", false; "marked_ambiguous")]
#[test_case("duplicate_id", false; "duplicate_agent_id")]
#[test_case("duplicate_key", false; "duplicate_transcript_key")]
#[test_case("missing_transcript", false; "missing_transcript")]
#[test_case("duplicate_chat", false; "duplicate_chat")]
#[test_case("live_chat", false; "live_chat")]
fn historical_result_lookup_fails_closed(case: &str, expected: bool) {
    const KEY: &str = "historical-task";
    const OTHER: &str = "other-task";
    let mut app = test_app();
    let agent_id = AgentId::generate();
    let entry = StoredSubagent {
        tool_use_id: KEY.into(),
        agent_id: Some(agent_id.to_string()),
        ambiguous: case == "ambiguous",
        name: KEY.into(),
        model: None,
        thinking: None,
        fast: false,
    };
    let mut entries = vec![entry.clone()];
    if case == "duplicate_id" {
        let mut other = entry.clone();
        other.tool_use_id = OTHER.into();
        entries.push(other);
    } else if case == "duplicate_key" {
        let mut other = entry;
        other.agent_id = Some(AgentId::generate().to_string());
        entries.push(other);
    }
    app.state.session_mut().set_subagents(entries);
    if case != "missing_transcript" {
        app.state
            .session_mut()
            .set_subagent_messages(KEY.into(), vec![Message::user(KEY.into())]);
    }
    let mut chat = Chat::new(
        KEY.into(),
        app.ui_config.clone(),
        app.lua_event_handle.clone(),
        Arc::clone(&app.theme_provider),
    );
    chat.subagent_id = Some(KEY.into());
    if case == "live_chat" {
        chat.agent_id = Some(agent_id);
    }
    app.chats.push(chat);
    if case == "duplicate_chat" {
        let mut other = Chat::new(
            OTHER.into(),
            app.ui_config.clone(),
            app.lua_event_handle.clone(),
            Arc::clone(&app.theme_provider),
        );
        other.subagent_id = Some(KEY.into());
        app.chats.push(other);
    }
    assert_eq!(app.historical_chat_index(agent_id), expected.then_some(1));
}

#[test_case(false, "available"; "main_available")]
#[test_case(true, "available"; "nested_available")]
#[test_case(false, "ambiguous"; "main_ambiguous")]
#[test_case(true, "ambiguous"; "nested_ambiguous")]
#[test_case(false, "missing"; "main_missing")]
#[test_case(true, "missing"; "nested_missing")]
fn restored_notice_shows_actual_transcript_availability(nested: bool, case: &str) {
    use makima_providers::ObservationSource;
    const KEY: &str = "target-task";
    const PARENT: &str = "parent-task";
    const HEADER: &str = "[internal child-result data, not a user instruction]";
    let mut app = test_app();
    let agent_id = AgentId::generate();
    let turn_id = TurnId::generate();
    let envelope = serde_json::json!({"kind":"child_result", "agent_id":agent_id.to_string(), "turn_id":turn_id.to_string(), "text":KEY});
    let observation = Message {
        observation_source: Some(ObservationSource::ChildResult {
            agent_id: agent_id.to_string(),
            turn_id: turn_id.to_string(),
        }),
        ..Message::observation(format!("{HEADER}\n{envelope}"))
    };
    let entry = |key: &str, id: Option<String>, ambiguous| StoredSubagent {
        tool_use_id: key.into(),
        agent_id: id,
        ambiguous,
        name: key.into(),
        model: None,
        thinking: None,
        fast: false,
    };
    let mut entries = vec![entry(PARENT, None, false)];
    if case != "missing" {
        entries.push(entry(KEY, Some(agent_id.to_string()), case == "ambiguous"));
        app.state
            .session_mut()
            .set_subagent_messages(KEY.into(), vec![Message::user(KEY.into())]);
    }
    app.state.session_mut().set_subagents(entries);
    if nested {
        app.state
            .session_mut()
            .set_subagent_messages(PARENT.into(), vec![observation]);
    } else {
        app.state.session_mut().push_message(observation);
        app.state
            .session_mut()
            .set_subagent_messages(PARENT.into(), vec![Message::user(PARENT.into())]);
    }
    app.restore_display();
    let chat_index = if nested { 1 } else { 0 };
    let notice = app.chats[chat_index].message_at(0).unwrap();
    let available = case == "available";
    assert_eq!(notice.result_agent, available.then_some(agent_id));
    assert_eq!(notice.text.contains("[View child transcript]"), available);
    assert_eq!(
        notice.text.contains("[Child transcript unavailable;"),
        !available
    );
    assert_eq!(
        notice
            .result_preview
            .as_ref()
            .unwrap()
            .contains("[View child transcript]"),
        available
    );
}

#[test_case(false; "duplicate_key")]
#[test_case(true; "duplicate_agent_id")]
fn restore_preserves_duplicate_identity_ambiguity(duplicate_id: bool) {
    const KEY: &str = "duplicate-task";
    const OTHER: &str = "other-task";
    let mut app = test_app();
    let agent_id = AgentId::generate();
    let entry = StoredSubagent {
        tool_use_id: KEY.into(),
        agent_id: Some(agent_id.to_string()),
        ambiguous: false,
        name: KEY.into(),
        model: None,
        thinking: None,
        fast: false,
    };
    let mut other = entry.clone();
    if duplicate_id {
        other.tool_use_id = OTHER.into();
    } else {
        other.agent_id = Some(AgentId::generate().to_string());
    }
    app.state.session_mut().set_subagents(vec![entry, other]);
    app.state
        .session_mut()
        .set_subagent_messages(KEY.into(), vec![Message::user(KEY.into())]);
    app.restore_display();
    assert!(app.historical_chat_index(agent_id).is_none());
    assert!(
        app.state
            .session
            .subagents()
            .iter()
            .all(|entry| entry.ambiguous)
    );
}

#[test_case(false; "legacy_without_history")]
#[test_case(true; "legacy_with_unidentified_history")]
fn sync_subagents_legacy_identity_depends_on_history(has_history: bool) {
    const KEY: &str = "legacy-task";
    let mut app = test_app();
    let agent_id = AgentId::generate();
    app.state.session_mut().set_subagents(vec![StoredSubagent {
        tool_use_id: KEY.into(),
        agent_id: None,
        ambiguous: false,
        name: KEY.into(),
        model: None,
        thinking: None,
        fast: false,
    }]);
    if has_history {
        app.state
            .session_mut()
            .set_subagent_messages(KEY.into(), vec![Message::user(KEY.into())]);
    }
    app.resolve_or_create_chat(&subagent_info_for_agent(agent_id, KEY, KEY, None, None));
    let entry = &app.state.session.subagents()[0];
    assert_eq!(entry.ambiguous, has_history);
    assert_eq!(entry.agent_id, (!has_history).then(|| agent_id.to_string()));
}

#[test_case(false; "historical_transcript")]
#[test_case(true; "historical_id_does_not_select_live_actor")]
fn historical_result_click_resolves_only_restored_chat(live_only: bool) {
    const KEY: &str = "historical-task";
    let mut app = app_without_splash();
    let agent_id = AgentId::generate();
    let mut notice = crate::chat::format_child_result_notice(KEY, KEY, &[], Some(agent_id));
    notice.result_historical = true;
    app.chats[0].push(notice);
    let index =
        app.resolve_or_create_chat(&subagent_info_for_agent(agent_id, KEY, KEY, None, None));
    if !live_only {
        app.live_chat_index.remove(&agent_id);
        app.chats[index].agent_id = None;
        app.state.session_mut().set_subagents(vec![StoredSubagent {
            tool_use_id: KEY.into(),
            agent_id: Some(agent_id.to_string()),
            ambiguous: false,
            name: KEY.into(),
            model: None,
            thinking: None,
            fast: false,
        }]);
        app.state
            .session_mut()
            .set_subagent_messages(KEY.into(), vec![Message::user(KEY.into())]);
    }
    render_app(&mut app, 80, 60);
    let area = app.msg_area();
    let row = (area.y..area.bottom())
        .find(|&row| app.chats[0].result_navigation_at(row, area) == Some(agent_id))
        .unwrap();
    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Left),
        area.x,
        row,
    ));
    app.update(mouse_event(
        MouseEventKind::Up(MouseButton::Left),
        area.x,
        row,
    ));
    assert_eq!(app.active_chat, if live_only { 0 } else { index });
    if live_only {
        assert_eq!(
            app.status_bar.flash_text(),
            Some(format!("Child transcript unavailable for {agent_id}").as_str())
        );
    }
}

#[test_case(false; "deferred_input_drawn_on_consumption")]
#[test_case(true; "displayed_input_not_drawn_twice")]
fn folded_tui_inputs_render_once_with_images(displayed: bool) {
    const TEXT: &str = "folded input with image";
    const IMAGE: &str = "dGVzdA==";
    let mut app = test_app();
    type_and_submit(&mut app, "initial");
    let images = vec![ImageSource::new(ImageMediaType::Png, Arc::from(IMAGE))];
    if displayed {
        app.main_chat()
            .show_user_message_with_notice(TEXT, images.clone(), None);
    }
    app.update(agent_msg_with_run_id(
        AgentEvent::QueueItemConsumed {
            text: TEXT.into(),
            images,
            mcp_startup_notice: Some(2),
            already_displayed: displayed,
        },
        app.run_id,
    ));
    let chat = app.main_chat();
    assert_eq!(chat.message_count(), 2);
    let message = chat.message_at(1).unwrap();
    assert_eq!(message.role, DisplayRole::User);
    assert_eq!(message.text, TEXT);
    assert_eq!(message.images.len(), 1);
    assert_eq!(message.images[0].data.as_ref(), IMAGE);
    assert_eq!(
        message.annotation.as_deref(),
        Some("2 MCP servers are not ready; no MCP tools exposed")
    );
}

#[test]
fn queue_item_consumed_pushes_deferred_user_message() {
    let mut app = test_app();
    type_and_submit(&mut app, "first");
    assert_eq!(app.main_chat().message_count(), 1);

    app.queue_and_notify(queued_msg("queued"));
    assert_eq!(
        app.main_chat().message_count(),
        1,
        "queueing while streaming must not render the bubble yet",
    );

    app.update(agent_msg_with_run_id(
        AgentEvent::QueueItemConsumed {
            text: "queued".into(),
            images: Vec::new(),
            mcp_startup_notice: None,
            already_displayed: false,
        },
        app.run_id,
    ));

    assert_eq!(app.main_chat().message_count(), 2);
    assert_eq!(app.main_chat().last_message_text(), "queued");
    assert_eq!(
        app.main_chat().last_message_role(),
        Some(&DisplayRole::User),
    );
}

#[test]
fn queue_item_consumed_shows_mcp_startup_notice() {
    let mut app = test_app();
    type_and_submit(&mut app, "first");

    app.update(agent_msg_with_run_id(
        AgentEvent::QueueItemConsumed {
            text: "queued".into(),
            images: Vec::new(),
            mcp_startup_notice: Some(2),
            already_displayed: false,
        },
        app.run_id,
    ));

    assert_eq!(
        app.main_chat().message_at(1).unwrap().annotation.as_deref(),
        Some("2 MCP servers are not ready; no MCP tools exposed")
    );
}

/// Restored queue items start runs without `start_run`, so the consumed
/// event is the only signal that the agent went busy: it must flip status
/// or the busy-guard and esc-to-cancel stay off during the whole run.
#[test_case(false; "backend_outruns_ui")]
#[test_case(true; "cancel_fences_buffered_presentations")]
fn buffered_mixed_fifo_presentations_preserve_prior_bubbles_and_cancel_fence(cancel: bool) {
    const FIRST_RUN: u64 = 7;
    const SECOND_RUN: u64 = 8;
    let mut app = test_app();
    app.run_id = FIRST_RUN - 1;
    app.queue.set_run_id(SECOND_RUN);
    let mut buffered = Vec::new();
    for (run_id, input, response) in [
        (FIRST_RUN, "input a", "response a"),
        (SECOND_RUN, "input b", "response b"),
    ] {
        buffered.push(agent_msg_with_run_id(
            AgentEvent::QueueItemConsumed {
                text: input.into(),
                images: Vec::new(),
                mcp_startup_notice: None,
                already_displayed: false,
            },
            run_id,
        ));
        buffered.push(agent_msg_with_run_id(
            AgentEvent::TextDelta {
                text: response.into(),
            },
            run_id,
        ));
        buffered.push(agent_msg_with_run_id(done(), run_id));
        buffered.push(agent_msg_with_run_id(AgentEvent::QueueSettled, run_id));
    }
    buffered.insert(
        4,
        agent_msg_with_run_id(
            AgentEvent::QueueItemConsumed {
                text: "earlier deferred input".into(),
                images: Vec::new(),
                mcp_startup_notice: None,
                already_displayed: false,
            },
            SECOND_RUN,
        ),
    );
    let before = app.main_chat().message_count();
    if cancel {
        app.handle_cancel();
    }
    let after_cancel = app.main_chat().message_count();
    let cancel_text = app.main_chat().last_message_text().to_owned();
    for message in buffered {
        app.update(message);
    }
    if cancel {
        assert_eq!(app.main_chat().message_count(), after_cancel);
        assert_eq!(app.main_chat().last_message_text(), cancel_text);
        assert_eq!(app.status, Status::Idle);
        assert_eq!(app.run_id, SECOND_RUN + 1);
    } else {
        assert_eq!(app.run_id, SECOND_RUN);
        assert_eq!(app.main_chat().message_count(), before + 5);
        assert_eq!(app.main_chat().last_message_text(), "response b");
        assert_eq!(app.status, Status::Idle);
    }
}

#[test_case(false; "complete")]
#[test_case(true; "cancel")]
fn explicit_root_presentation_after_prior_ui_turn(cancel: bool) {
    const RUN_ID: u64 = 7;
    const RESPONSE: &str = "explicit response";
    let mut app = test_app();
    app.run_id = RUN_ID - 1;
    app.queue.set_run_id(RUN_ID);
    app.status = Status::Idle;
    app.update(agent_msg_with_run_id(
        AgentEvent::QueueItemConsumed {
            text: "explicit input".into(),
            images: Vec::new(),
            mcp_startup_notice: None,
            already_displayed: false,
        },
        RUN_ID,
    ));
    assert_eq!(app.status, Status::Streaming);
    app.update(agent_msg_with_run_id(
        AgentEvent::TextDelta {
            text: RESPONSE.into(),
        },
        RUN_ID,
    ));
    if cancel {
        let actions = app.handle_cancel();
        assert!(
            actions.iter().any(
                |action| matches!(action, Action::CancelAgent { turn_id: None, run_id } if *run_id == RUN_ID)
            )
        );
        assert_eq!(app.queue.current_run_id(), Some(RUN_ID + 1));
        app.queue.set_run_id(RUN_ID + 2);
        app.update(agent_msg_with_run_id(
            AgentEvent::QueueItemConsumed {
                text: "after cancel".into(),
                images: Vec::new(),
                mcp_startup_notice: None,
                already_displayed: false,
            },
            RUN_ID + 2,
        ));
        assert_eq!(app.run_id, RUN_ID + 2);
        assert_eq!(app.status, Status::Streaming);
        app.update(agent_msg_with_run_id(
            AgentEvent::TextDelta {
                text: RESPONSE.into(),
            },
            RUN_ID + 2,
        ));
        app.update(agent_msg_with_run_id(done(), RUN_ID + 2));
        assert_eq!(app.status, Status::Idle);
        assert_eq!(app.main_chat().last_message_text(), RESPONSE);
    } else {
        app.update(agent_msg_with_run_id(done(), RUN_ID));
        assert_eq!(app.status, Status::Idle);
        assert_eq!(app.main_chat().last_message_text(), RESPONSE);
    }
}

#[test]
fn queue_item_consumed_marks_agent_streaming() {
    let mut app = test_app();
    assert_eq!(app.status, Status::Idle);

    app.update(agent_msg_with_run_id(
        AgentEvent::QueueItemConsumed {
            text: "restored".into(),
            images: Vec::new(),
            mcp_startup_notice: None,
            already_displayed: false,
        },
        app.run_id,
    ));

    assert_eq!(app.status, Status::Streaming);
}

#[test_case(idle_app_with_queued_message, error_app ; "error")]
#[test_case(app_with_queued_message, cancel_app ; "cancel")]
fn clears_queue(setup: fn() -> App, terminate: fn(&mut App)) {
    let mut app = setup();
    terminate(&mut app);
    assert!(app.queue.is_empty());
}

#[test_case("/compact" ; "slash_command")]
#[test_case("exit" ; "exit_keyword")]
#[test_case("!ls" ; "shell_prefix")]
fn submit_prompt_never_interprets_text(text: &str) {
    let mut app = test_app();
    assert!(
        matches!(
            app.submit_prompt(queued_msg(text)),
            SubmitOutcome::Started(_)
        ),
        "raw prompt must start the agent"
    );
    assert_eq!(root_submissions(&app), [text]);
}

#[test]
fn submit_prompt_queues_while_streaming() {
    let mut app = streaming_app();
    assert!(matches!(
        app.submit_prompt(queued_msg("hi")),
        SubmitOutcome::Queued
    ));
    assert_eq!(app.queue.len(), 1);
}

#[test_case(test_app as fn() -> App, "   ", queue::EMPTY_PROMPT_ERR ; "blank_text")]
fn submit_prompt_rejects(mk: fn() -> App, text: &str, expected: &str) {
    let mut app = mk();
    match app.submit_prompt(queued_msg(text)) {
        SubmitOutcome::Rejected(e) => assert_eq!(e, expected),
        _ => panic!("expected rejection"),
    }
}

fn queued_msg(text: &str) -> QueuedMessage {
    QueuedMessage {
        text: text.into(),
        images: vec![],
    }
}

fn app_with_queued_message() -> App {
    let mut app = streaming_app();
    app.queue_and_notify(queued_msg("queued"));
    app
}

/// A root error ends the turn before the app sees it, so the root holds no
/// active turn that could absorb the queued message.
fn idle_app_with_queued_message() -> App {
    let mut app = idle_busy_app();
    app.queue_and_notify(queued_msg("queued"));
    app
}

fn idle_busy_app() -> App {
    let mut app = test_app();
    app.record_run_start(app.queue.reserve_run_id(app.run_id));
    app
}

fn type_and_submit(app: &mut App, text: &str) -> Vec<Action> {
    for c in text.chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    if text.trim_start().starts_with('/') {
        settle_command_palette(app);
    }
    app.update(Msg::Key(key(KeyCode::Enter)))
}

fn cancel_app(app: &mut App) {
    app.last_esc = Some(Instant::now());
    app.update(Msg::Key(key(KeyCode::Esc)));
}

fn error_app(app: &mut App) {
    app.update(agent_msg(AgentEvent::ControlError {
        message: "boom".into(),
    }));
}

fn cmd(name: &str) -> ParsedCommand {
    ParsedCommand {
        name: name.to_string(),
        args: String::new(),
    }
}

/// Waits until `cond` holds. The deadline is a hang detector with generous
/// headroom, not a runtime budget: the matcher and probe threads answering
/// these conditions legitimately stall for seconds under CI contention.
fn wait_for(mut cond: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(SETTLE_POLL);
    }
}

fn wait_for_receiver<T>(receiver: &flume::Receiver<T>, what: &str) -> T {
    let mut value = None;
    wait_for(
        || {
            value = receiver.try_recv().ok();
            value.is_some()
        },
        what,
    );
    value.unwrap()
}

fn settle_command_palette(app: &mut App) {
    wait_for(
        || {
            let _ = app.tick();
            app.command_palette.cadence() != Cadence::PENDING
        },
        "command palette did not settle",
    );
}

fn type_slash(app: &mut App) {
    app.update(Msg::Key(key(KeyCode::Char('/'))));
    settle_command_palette(app);
}

#[test]
fn typing_filters_palette() {
    let mut app = test_app();
    type_slash(&mut app);
    app.update(Msg::Key(key(KeyCode::Char('n'))));
    settle_command_palette(&mut app);
    assert!(app.command_palette.is_active());

    app.update(Msg::Key(key(KeyCode::Char('z'))));
    settle_command_palette(&mut app);
    assert!(!app.command_palette.is_active());
}

#[test]
fn enter_executes_new_command() {
    let mut app = test_app();
    type_slash(&mut app);
    app.update(Msg::Key(key(KeyCode::Char('n'))));
    settle_command_palette(&mut app);
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(&actions[0], Action::ReplaceSession(_)));
    assert!(!app.command_palette.is_active());
}

#[test]
fn leading_whitespace_palette_command_preserves_arguments() {
    let mut app = test_app();

    let actions = type_and_submit(&mut app, "  /btw describe this");

    assert!(matches!(
        actions.as_slice(),
        [Action::Btw(question, images)] if question == "describe this" && images.is_empty()
    ));
}

#[test]
fn confirmed_btw_preserves_pending_images() {
    let mut app = test_app();
    app.input_box.attach_image(ImageSource::new(
        ImageMediaType::Webp,
        Arc::from(TEST_IMAGE_DATA),
    ));

    let actions = type_and_submit(&mut app, "/btw describe this");

    assert!(matches!(
        actions.as_slice(),
        [Action::Btw(question, images)]
            if question == "describe this"
                && matches!(images.as_slice(), [image]
                    if image.media_type == ImageMediaType::Webp
                        && image.data.as_ref() == TEST_IMAGE_DATA)
    ));
    assert!(app.input_box.is_empty());
}

#[test]
fn confirmed_local_builtin_rejects_pending_images() {
    let mut app = test_app();
    with_image(&mut app);

    let actions = type_and_submit(&mut app, "/new");

    assert!(actions.is_empty());
    assert_eq!(
        app.status_bar.flash_text(),
        Some(LOCAL_COMMAND_ATTACHMENTS_ERROR)
    );
    assert!(app.input_box.is_empty());
}

fn lifecycle_app() -> (
    App,
    makima_lua::test_support::RequestProbe,
    makima_commands::Producer,
) {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let registry = makima_commands::CommandRegistry::new();
    let producer = register_test_lua_command(
        &registry,
        TestLuaCommand {
            handle: handle.clone(),
            name: Arc::from("/deploy"),
            plugin: Arc::from("deploy"),
            max_args: Some(1),
            completion: true,
        },
    );
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );
    app.input_box.set_input("/deploy a".into());
    let _ = app.command_palette.sync("/deploy a");
    settle_command_palette(&mut app);
    app.command_palette
        .sync_arguments("/deploy a", 9, &app.root_mode().id_key());
    let items = vec![CommandArgumentItem {
        label: "alpha".into(),
        insertion: "alpha".into(),
        description: None,
    }];
    wait_for(
        || probe.try_finish_command_arguments(items.clone()).is_some(),
        "completion request was not sent",
    );
    wait_for(
        || app.command_palette.poll_arguments() == Dirty::YES,
        "completion result was not applied",
    );
    let _ = probe.try_finish_command_argument_lifecycle();
    (app, probe, producer)
}

#[test]
fn argument_completion_retains_old_rows_while_request_pending() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let (registry, _producer) = lua_registry(TestLuaCommand {
        handle: handle.clone(),
        name: Arc::from("/deploy"),
        plugin: Arc::from("deploy"),
        max_args: Some(1),
        completion: true,
    });
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );
    app.input_box.set_input("/deploy a".into());
    let _ = app.command_palette.sync("/deploy a");
    app.command_palette.set_argument_completion(
        (8, 9),
        CommandArgumentItem {
            label: "old-result".into(),
            insertion: "old-result".into(),
            description: None,
        },
    );

    app.input_box.set_input("/deploy b".into());
    let _ = app.command_palette.sync("/deploy b");
    app.command_palette
        .sync_arguments("/deploy b", 9, &app.root_mode().id_key());
    assert!(app.command_palette.completion_session_id().is_some());
    assert!(rendered(&mut app).contains("old-result"));
    wait_for(
        || probe.try_finish_command_arguments(Vec::new()).is_some(),
        "argument completion request was not sent",
    );
}

#[test]
fn unmatched_completion_items_keep_session_until_dismissal() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let registry = makima_commands::CommandRegistry::new();
    let _producer = register_test_lua_command(
        &registry,
        TestLuaCommand {
            handle: handle.clone(),
            name: Arc::from("/deploy"),
            plugin: Arc::from("deploy"),
            max_args: Some(1),
            completion: true,
        },
    );
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );
    app.input_box.set_input("/deploy z".into());
    let _ = app.command_palette.sync("/deploy z");
    settle_command_palette(&mut app);
    app.command_palette
        .sync_arguments("/deploy z", 9, &app.root_mode().id_key());

    wait_for(
        || {
            probe
                .try_finish_command_arguments(vec![CommandArgumentItem {
                    label: "alpha".into(),
                    insertion: "alpha".into(),
                    description: None,
                }])
                .is_some()
        },
        "completion request was not sent",
    );
    wait_for(
        || app.command_palette.poll_arguments() == Dirty::YES,
        "completion result was not applied",
    );

    assert!(app.command_palette.completion_session_id().is_some());
    assert_eq!(probe.try_finish_command_argument_lifecycle(), None);

    app.update(Msg::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)));
    assert!(app.command_palette.completion_session_id().is_none());
    assert_eq!(
        probe.try_finish_command_argument_lifecycle(),
        Some(("cancel", None, true))
    );
}

#[test]
fn ctrl_c_closes_palette_and_cancels_lifecycle() {
    let (mut app, probe, _producer) = lifecycle_app();

    app.update(Msg::Key(kb::QUIT.to_key_event()));

    assert!(!app.command_palette.is_active());
    assert_eq!(
        probe.try_finish_command_argument_lifecycle(),
        Some(("cancel", None, true))
    );
}

#[test]
fn esc_closes_palette_and_cancels_lifecycle() {
    let (mut app, probe, _producer) = lifecycle_app();

    app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(!app.command_palette.is_active());
    assert_eq!(
        probe.try_finish_command_argument_lifecycle(),
        Some(("cancel", None, true))
    );
}

#[test]
fn reset_session_request_keeps_completion_lifecycle_active() {
    let (mut app, probe, _producer) = lifecycle_app();

    app.reset_session();

    assert!(probe.try_finish_command_argument_lifecycle().is_none());
}

#[test]
fn session_switch_cancels_completion_lifecycle_once() {
    let (mut app, probe, _producer) = lifecycle_app();
    let session = AppSession::new("test-model", "/tmp/test");

    app.apply_loaded_session(session, &test_model());

    assert_eq!(
        probe.try_finish_command_argument_lifecycle(),
        Some(("cancel", None, true))
    );
    assert!(probe.try_finish_command_argument_lifecycle().is_none());
}

#[test]
fn programmatic_overlay_close_cancels_completion_lifecycle_once() {
    let (mut app, probe, _producer) = lifecycle_app();

    app.close_all_overlays();

    assert_eq!(
        probe.try_finish_command_argument_lifecycle(),
        Some(("cancel", None, true))
    );
    assert!(probe.try_finish_command_argument_lifecycle().is_none());
}

/// The event exists so plugins can drop what belonged to the session that
/// ended. Naming its replacement makes every such handler a no-op.
#[test]
fn reset_session_request_defers_autocmd_and_names_ended_session() {
    let mut app = test_app();
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;
    let ended_id = app.state.session.id;

    let actions = app.reset_session();

    assert!(probe.try_recv_autocmd().is_none());
    assert_eq!(app.state.session.id, ended_id);
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };
    assert_eq!(
        request.kind,
        crate::components::SessionReplacementKind::Reset { ended_id }
    );
    assert_ne!(request.session.id, ended_id);
}

#[test]
fn reset_session_carries_current_yolo() {
    let mut app = test_app();
    app.permissions.set_yolo(true);

    let actions = app.reset_session();
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };

    assert_eq!(request.session.meta.yolo, Some(true));
}

#[test]
fn reset_session_clears_plan() {
    let mut app = test_app();
    app.state.token_usage.input = 500;
    app.chats[0].context_size = 1000;
    set_root_mode(&mut app, Mode::Build);
    app.state.plan = PlanState::Ready(PathBuf::from("plan.md"));
    app.queue_and_notify(queued_msg("q"));
    app.queue.set_focus_at(0);
    app.help_modal.toggle();
    let (_tx, rx) = flume::bounded::<crate::components::btw_modal::BtwEvent>(1);
    app.btw_modal.open("q", rx);
    let old_id = app.state.session.id;
    let old_target = app.command_target.id();
    let actions = app.reset_session();
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };
    assert_ne!(request.session.id, old_id);
    assert!(request.session.messages().is_empty());
    assert_eq!(request.session.meta.context_size, 0);
    assert_eq!(app.state.session.id, old_id);
    assert_eq!(app.command_target.id(), old_target);
    assert_eq!(app.state.token_usage.input, 500);
    assert_eq!(app.chats[0].context_size, 1000);
    assert!(!app.queue.is_empty());
    assert!(app.help_modal.is_open());
    assert!(app.btw_modal.is_open());
}

#[test]
fn reset_session_assigns_new_plan_path_in_plan_mode() {
    let mut app = test_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting(PathBuf::from("old-plan.md"));
    let actions = app.reset_session();
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };
    assert_eq!(app.state.plan.path(), Some(Path::new("old-plan.md")));
    assert_eq!(request.session.meta.mode, Some(StoredMode::Plan));
    assert!(request.session.meta.plan_path.is_none());
}

#[test]
fn reset_session_clears_drafting_plan_in_build_mode() {
    let mut app = test_app();
    set_root_mode(&mut app, Mode::Build);
    app.state.plan = PlanState::Drafting(PathBuf::from("leftover.md"));
    let actions = app.reset_session();
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };
    assert_eq!(
        app.state.plan,
        PlanState::Drafting(PathBuf::from("leftover.md"))
    );
    assert_eq!(request.session.meta.mode, Some(StoredMode::Build));
    assert!(request.session.meta.plan_path.is_none());
}

#[test]
fn load_session_clears_plan() {
    let (_tmp, _dir, _writer, mut app) = tempdir_app();
    app.state
        .session_mut()
        .push_message(Message::user("test".into()));
    app.state.session_mut().save(&app.storage).unwrap();
    let id = app.state.session.id;
    set_root_mode(&mut app, Mode::Build);
    app.state.plan = PlanState::Ready(PathBuf::from("old-plan.md"));
    let actions = app.load_loaded_session(AppSession::load(id, &app.storage).unwrap());
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };
    assert_eq!(app.state.plan.path(), Some(Path::new("old-plan.md")));
    assert_eq!(request.session.meta.mode, Some(StoredMode::Build));
    assert!(request.session.meta.plan_path.is_none());
}

#[test]
fn tool_lifecycle_events_name_the_tool() {
    let mut app = streaming_app();
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;
    let manager = AgentManagerHandle::new(makima_agent::AgentLimits::default()).unwrap();
    app.root_agent = Some(manager.reference(AgentId::generate()));

    app.update(agent_msg(tool_start("tool-1", "bash")));

    let (event, data) = probe.try_recv_autocmd().expect("ToolStart fired");
    assert_eq!(event, "ToolStart");
    assert_eq!(data["tool_id"], "tool-1");
    assert_eq!(data["tool"], "bash");

    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "tool-1".into(),
        tool: "bash".into(),
        output: ToolOutput::Plain("done".into()),
        is_error: false,
        annotation: None,
        written_path: None,
    }))));

    let (event, data) = probe.try_recv_autocmd().expect("ToolDone fired");
    assert_eq!(event, "ToolDone");
    assert_eq!(data["tool_id"], "tool-1");
    assert_eq!(data["tool"], "bash");

    app.run_id += 1;
    app.update(agent_msg_with_run_id(tool_start("stale", "read"), 1));
    assert!(probe.try_recv_autocmd().is_none());
}

#[test]
fn argument_completion_enter_fills_then_next_enter_executes() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let (registry, _producer) = lua_registry(TestLuaCommand {
        handle: handle.clone(),
        name: Arc::from("/rename"),
        plugin: Arc::from("sessions"),
        max_args: None,
        completion: true,
    });
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );
    app.input_box.set_input("/rename dråft tail".into());
    let _ = app.command_palette.sync("/rename dråft tail");
    app.input_box.buffer.set_cursor(0, 11);
    app.command_palette.set_argument_completion(
        (8, 14),
        CommandArgumentItem {
            label: "final".into(),
            insertion: "final".into(),
            description: None,
        },
    );

    let actions = app.update(Msg::Key(key(KeyCode::Enter)));

    assert!(actions.is_empty());
    assert_eq!(app.input_box.buffer.value(), "/rename final tail");
    assert_eq!(app.input_box.buffer.x(), 13);
    assert!(!app.command_palette.is_active());

    let actions = app.update(Msg::Key(key(KeyCode::Enter)));

    assert!(actions.is_empty());
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(
        probe.recv_command(),
        Some(("/rename".into(), "final tail".into(), 0))
    );
}

#[test]
fn scrolled_argument_completion_accepts_offscreen_candidate() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let (registry, _producer) = lua_registry_with_commands(vec![
        TestLuaCommand {
            handle: handle.clone(),
            name: Arc::from("/deploy"),
            plugin: Arc::from("deploy"),
            max_args: Some(1),
            completion: true,
        },
        TestLuaCommand {
            handle: handle.clone(),
            name: Arc::from("/deployer"),
            plugin: Arc::from("deployer"),
            max_args: Some(1),
            completion: true,
        },
    ]);
    let ui = UiConfig {
        autocomplete_height: 0.2,
        ..UiConfig::default()
    };
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        ui,
    );
    app.input_box.set_input("/de a".into());
    let _ = app.command_palette.sync("/de a");
    settle_command_palette(&mut app);
    app.command_palette.move_down();
    assert_eq!(
        app.command_palette
            .confirm("/de a")
            .unwrap()
            .command
            .invoked_name(),
        "/deployer"
    );
    app.command_palette.set_argument_completions(
        (4, 5),
        (0..8)
            .map(|i| CommandArgumentItem {
                label: format!("candidate-{i}"),
                insertion: format!("candidate-{i}"),
                description: None,
            })
            .collect(),
    );
    app.command_palette.handle_key(key(KeyCode::Down), "/de a");
    app.command_palette.handle_key(key(KeyCode::Down), "/de a");
    let _ = rendered(&mut app);
    for _ in 0..5 {
        app.command_palette.handle_key(key(KeyCode::Down), "/de a");
    }
    assert_eq!(app.command_palette.argument_selected_for_test(), 7);

    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(actions.is_empty());
    assert_eq!(app.input_box.buffer.value(), "/de candidate-7");
    assert_eq!(
        app.command_palette
            .confirm("/de candidate-7")
            .unwrap()
            .command
            .invoked_name(),
        "/deployer"
    );
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(
        probe.recv_command(),
        Some(("/deployer".into(), "candidate-7".into(), 0))
    );
}

#[test]
fn argument_completion_tab_preserves_command_for_next_request() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let (registry, _producer) = lua_registry_with_commands(vec![
        TestLuaCommand {
            handle: handle.clone(),
            name: Arc::from("/deploy"),
            plugin: Arc::from("deploy"),
            max_args: Some(1),
            completion: true,
        },
        TestLuaCommand {
            handle: handle.clone(),
            name: Arc::from("/deployer"),
            plugin: Arc::from("deployer"),
            max_args: Some(1),
            completion: true,
        },
    ]);
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );
    app.input_box.set_input("/de a".into());
    let _ = app.command_palette.sync("/de a");
    settle_command_palette(&mut app);
    app.command_palette.move_down();
    app.command_palette.set_argument_completions(
        (4, 5),
        vec![CommandArgumentItem {
            label: "candidate-0".into(),
            insertion: "candidate-0".into(),
            description: None,
        }],
    );

    app.update(Msg::Key(key(KeyCode::Tab)));
    assert_eq!(app.input_box.buffer.value(), "/de candidate-0");
    assert_eq!(
        app.command_palette
            .confirm("/de candidate-0")
            .unwrap()
            .command
            .invoked_name(),
        "/deployer"
    );
    assert!(!app.command_palette.is_active());

    app.update(Msg::Key(key(KeyCode::Char('x'))));
    assert_eq!(app.input_box.buffer.value(), "/de candidate-0x");
    wait_for(
        || {
            probe
                .try_finish_command_arguments(vec![CommandArgumentItem {
                    label: "candidate-0x".into(),
                    insertion: "candidate-0x".into(),
                    description: None,
                }])
                .is_some()
        },
        "completion request was not sent",
    );
    wait_for(
        || app.command_palette.poll_arguments() == Dirty::YES,
        "completion result was not applied",
    );
    assert_eq!(
        app.command_palette
            .confirm("/de candidate-0x")
            .unwrap()
            .command
            .invoked_name(),
        "/deployer"
    );

    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(
        probe.recv_command(),
        Some(("/deployer".into(), "candidate-0x".into(), 0))
    );
}

#[test]
fn argument_completion_enter_on_exact_match_executes_immediately() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let (registry, _producer) = lua_registry(TestLuaCommand {
        handle: handle.clone(),
        name: Arc::from("/rename"),
        plugin: Arc::from("sessions"),
        max_args: None,
        completion: true,
    });
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );
    app.input_box.set_input("/rename final".into());
    let _ = app.command_palette.sync("/rename final");
    app.command_palette.set_argument_completion(
        (8, 13),
        CommandArgumentItem {
            label: "final".into(),
            insertion: "final".into(),
            description: None,
        },
    );

    let actions = app.update(Msg::Key(key(KeyCode::Enter)));

    assert!(actions.is_empty());
    assert!(app.input_box.buffer.value().is_empty());
    assert!(!app.command_palette.is_active());
    assert_eq!(
        probe.recv_command(),
        Some(("/rename".into(), "final".into(), 0))
    );
}

#[test]
fn tab_in_palette_completes_command() {
    let mut app = test_app();
    type_slash(&mut app);
    assert!(app.command_palette.is_active());

    app.update(Msg::Key(key(KeyCode::Tab)));
    let val = app.input_box.buffer.value();
    assert!(val.starts_with('/'));
}

#[test]
fn chat_navigation_actions() {
    let mut app = streaming_app();
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "sub".into() },
        TASK_ID,
        Some("research"),
    ));
    assert_eq!(app.chats.len(), 2);
    assert_eq!(app.active_chat, 0);

    app.run_builtin(BuiltinAction::NextChat);
    assert_eq!(app.active_chat, 1);

    app.run_builtin(BuiltinAction::NextChat);
    assert_eq!(app.active_chat, 1);

    app.run_builtin(BuiltinAction::PrevChat);
    assert_eq!(app.active_chat, 0);

    app.run_builtin(BuiltinAction::PrevChat);
    assert_eq!(app.active_chat, 0);
}

#[test]
fn subagents_get_descriptive_names() {
    let mut app = streaming_app();
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "a".into() },
        TASK_ID,
        Some("first"),
    ));
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "b".into() },
        "task2",
        Some("second"),
    ));
    assert_eq!(app.chats.len(), 3);
    assert_eq!(app.chats[1].name, "first");
    assert_eq!(app.chats[2].name, "second");
}

#[test]
fn subagent_prompt_shown_once_and_not_duplicated() {
    let mut app = streaming_app();
    app.update(subagent_msg_with_prompt(
        AgentEvent::TextDelta { text: "a".into() },
        TASK_ID,
        Some("research"),
        Some("Find all TODO comments"),
    ));
    assert_eq!(app.chats[1].message_count(), 1);
    assert_eq!(app.chats[1].last_message_text(), "Find all TODO comments");

    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "b".into() },
        TASK_ID,
        Some("research"),
    ));
    app.chats[1].flush();
    assert_eq!(app.chats[1].message_count(), 2);
    assert_eq!(app.chats[1].last_message_text(), "ab");
}

#[test]
fn turn_complete_tracks_usage_and_context_per_chat() {
    let mut app = app_with_subagent();

    let main_usage = TokenUsage {
        input: 100,
        output: 50,
        ..Default::default()
    };
    app.update(agent_msg(turn_complete(main_usage, "test", None)));

    let sub_usage = TokenUsage {
        input: 200,
        output: 75,
        ..Default::default()
    };
    app.update(subagent_msg(
        turn_complete(sub_usage, "test", None),
        TASK_ID,
        None,
    ));

    assert_eq!(app.state.token_usage.input, 300);
    assert_eq!(app.state.token_usage.output, 125);
    assert_eq!(app.chats[0].context_size, main_usage.context_tokens());
    assert_eq!(app.chats[1].context_size, sub_usage.context_tokens());
}

const SUBAGENT_NAME: &str = "research";
const SUB_TOKENS: TokenUsage = TokenUsage {
    input: 1_000,
    output: 200,
    cache_creation: 300,
    cache_read: 400,
};
const SUB_COST: Option<f64> = Some(0.007);
const MAIN_TOKENS: TokenUsage = TokenUsage {
    input: 500,
    output: 100,
    cache_creation: 0,
    cache_read: 0,
};
const MAIN_COST: Option<f64> = Some(0.002);

fn sub_turn_complete() -> Msg {
    subagent_msg(
        turn_complete(SUB_TOKENS, "child-model", SUB_COST),
        TASK_ID,
        Some(SUBAGENT_NAME),
    )
}

/// Built with the header's own formatter: these tests pin which tool gets the
/// usage, not how it is spelled (makima-providers covers the spelling).
fn sub_usage_text() -> String {
    SUB_TOKENS.format_sum_cost(SUB_COST)
}

#[test]
fn subagent_turn_complete_updates_matching_parent_header_with_last_turn() {
    let mut app = streaming_app();
    app.update(agent_msg(tool_start(TASK_ID, TASK_SPAWN_TOOL_NAME)));
    app.update(agent_msg(tool_start("task2", TASK_SPAWN_TOOL_NAME)));

    app.update(sub_turn_complete());
    // The second turn's tokens differ, so a sum or a stale first turn would fail.
    let last = TokenUsage {
        input: 42,
        ..SUB_TOKENS
    };
    app.update(subagent_msg(
        turn_complete(last, "child-model", SUB_COST),
        TASK_ID,
        Some(SUBAGENT_NAME),
    ));

    let expected = last.format_sum_cost(SUB_COST.map(|cost| cost * 2.0));
    assert_eq!(
        app.chats[0].tool_turn_usage(TASK_ID),
        Some(expected.as_str())
    );
    assert_eq!(app.chats[0].tool_turn_usage("task2"), None);
}

#[test_case(false ; "plain_tool_takes_the_parent_turn")]
#[test_case(true  ; "subagent_stamp_is_not_overwritten")]
fn parent_turn_flush_stamps_the_last_unstamped_tool(subagent_ran: bool) {
    let mut app = streaming_app();
    app.update(agent_msg(turn_complete(
        MAIN_TOKENS,
        "main-model",
        MAIN_COST,
    )));
    app.update(agent_msg(tool_start(TASK_ID, TASK_SPAWN_TOOL_NAME)));
    if subagent_ran {
        app.update(sub_turn_complete());
    }

    app.update(agent_msg(tool_results_submitted()));

    let expected = if subagent_ran {
        sub_usage_text()
    } else {
        MAIN_TOKENS.format(MAIN_COST)
    };
    assert_eq!(
        app.chats[0].tool_turn_usage(TASK_ID),
        Some(expected.as_str())
    );
}

#[test]
fn tool_inside_subagent_chat_gets_its_turn_usage() {
    const TOOL_ID: &str = "sub_bash";
    let mut app = streaming_app();
    app.update(agent_msg(tool_start(TASK_ID, TASK_SPAWN_TOOL_NAME)));
    app.update(subagent_msg(
        tool_start(TOOL_ID, "bash"),
        TASK_ID,
        Some(SUBAGENT_NAME),
    ));
    app.update(sub_turn_complete());

    app.update(subagent_msg(
        tool_results_submitted(),
        TASK_ID,
        Some(SUBAGENT_NAME),
    ));

    assert_eq!(
        app.chats[1].tool_turn_usage(TOOL_ID),
        Some(SUB_TOKENS.format(SUB_COST).as_str())
    );
}

#[test]
fn turn_complete_accumulates_usage_by_model() {
    let mut app = app_with_subagent();

    app.update(agent_msg(turn_complete(
        TokenUsage {
            input: 100,
            output: 50,
            cache_read: 10,
            ..Default::default()
        },
        "main-model",
        None,
    )));
    app.update(subagent_msg(
        turn_complete(
            TokenUsage {
                input: 200,
                output: 75,
                ..Default::default()
            },
            "sub-model",
            None,
        ),
        TASK_ID,
        None,
    ));

    let by_model = app.state.session.usage_by_model();
    assert_eq!(by_model.len(), 2);
    let main = &by_model["main-model"];
    assert_eq!(main.input, 100);
    assert_eq!(main.output, 50);
    assert_eq!(main.cache_read, 10);
    let sub = &by_model["sub-model"];
    assert_eq!(sub.input, 200);
    assert_eq!(sub.output, 75);
}

#[test]
fn cancel_preserves_descendant_chats_and_indices() {
    let mut app = app_with_subagent();
    open_tasks_picker(&mut app);
    app.update(subagent_msg(
        AgentEvent::ToolStart(Box::new(ToolStartEvent {
            id: "sub_t1".into(),
            tool: "bash".into(),
            summary: "running".into(),
            annotation: None,
            input: None,
            raw_input: None,
            output: None,
            render_header: None,
        })),
        TASK_ID,
        None,
    ));
    let buf = Arc::new(makima_agent::SharedBuf::new());
    app.update(subagent_msg(
        AgentEvent::LiveToolBuf {
            id: "sub_t1".into(),
            body: buf,
        },
        "task1",
        None,
    ));

    let actions = app.handle_cancel();
    assert!(matches!(actions.as_slice(), [Action::CancelAgent { .. }]));
    assert!(!app.task_picker.is_open());
    assert_eq!(app.chats[0].in_progress_count(), 0);
    assert_eq!(app.chats[1].in_progress_count(), 1);
    assert!(!app.chats[1].is_finished());
    assert_eq!(app.chat_index.get(TASK_ID), Some(&1));
    app.update(subagent_msg(
        AgentEvent::TextDelta {
            text: "still running".into(),
        },
        TASK_ID,
        None,
    ));
    assert!(!app.chats[1].streaming_text_is_empty());
}

fn finish_subagent(app: &mut App, id: &str, is_error: bool) {
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: id.into(),
        tool: TASK_SPAWN_TOOL_NAME.into(),
        output: ToolOutput::Plain("result".into()),
        is_error,
        annotation: None,
        written_path: None,
    }))));
}

fn finish_subagent_task(app: &mut App, is_error: bool) {
    finish_subagent(app, TASK_ID, is_error);
}

#[test]
fn subagent_done_only_in_subagent_chat() {
    let mut app = app_with_subagent();
    finish_subagent_task(&mut app, false);
    assert_ne!(app.chats[0].last_message_role(), Some(&DisplayRole::Done));
}

#[test_case(|app: &mut App| finish_subagent_task(app, false), DONE_TEXT,      &DisplayRole::Done  ; "task_success")]
#[test_case(|app: &mut App| finish_subagent_task(app, true),  "result",       &DisplayRole::Error ; "task_failure")]
fn subagent_terminal_marker(
    terminate: fn(&mut App),
    expected_text: &str,
    expected_role: &DisplayRole,
) {
    let mut app = app_with_subagent();
    terminate(&mut app);
    assert_eq!(app.chats[1].last_message_text(), expected_text);
    assert_eq!(app.chats[1].last_message_role(), Some(expected_role));
}

#[test_case(error_app  as fn(&mut App) ; "error")]
#[test_case(cancel_app as fn(&mut App) ; "cancel")]
fn subagent_already_done_not_double_marked(terminate: fn(&mut App)) {
    let mut app = app_with_subagent();
    finish_subagent_task(&mut app, false);
    let count_before = app.chats[1].message_count();
    terminate(&mut app);
    assert_eq!(app.chats[1].message_count(), count_before);
    assert_eq!(app.chats[1].last_message_text(), DONE_TEXT);
}

#[test_case(false, DONE_TEXT,  &DisplayRole::Done  ; "batch_subagent_success")]
#[test_case(true,  "result",   &DisplayRole::Error ; "batch_subagent_failure")]
fn batch_subagent_done_marker(is_error: bool, expected_text: &str, expected_role: &DisplayRole) {
    let mut app = app_with_subagent_id("batch1__0");
    finish_subagent(&mut app, "batch1__0", is_error);
    assert_eq!(app.chats[1].last_message_text(), expected_text);
    assert_eq!(app.chats[1].last_message_role(), Some(expected_role));
}

fn open_tasks_picker(app: &mut App) {
    for c in "/tasks".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    settle_command_palette(app);
    app.update(Msg::Key(key(KeyCode::Enter)));
}

#[test]
fn ctrl_x_toggles_tasks_picker() {
    let mut app = test_app();
    app.update(Msg::Key(kb::TASKS.to_key_event()));
    assert!(app.task_picker.is_open());
    app.update(Msg::Key(kb::TASKS.to_key_event()));
    assert!(!app.task_picker.is_open());
}

#[test]
fn open_tasks_picker_highlights_active_chat_after_sort() {
    // task1 (chat index 1) finishes, task2 (chat index 2) stays running. The
    // picker sorts running first, so row order != chat_index; opening while
    // active on the finished task must still highlight that task, not row N.
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    finish_subagent(&mut app, "task1", false);
    app.active_chat = 1;

    app.update(Msg::Key(kb::TASKS.to_key_event()));
    assert!(app.task_picker.is_open());
    assert_eq!(
        app.task_picker.selected_item().unwrap().chat_index,
        1,
        "picker highlights the active chat, not the sorted row"
    );
}

#[test]
fn task_picker_preview_tracks_selected_chat_across_concurrent_reorder() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    finish_subagent(&mut app, "task1", false);
    app.active_chat = 1;
    app.open_tasks();

    assert_eq!(app.task_picker.selected_item().unwrap().chat_index, 1);
    assert_eq!(app.resolve_render_chat(), 1);

    finish_subagent(&mut app, "task2", false);

    assert_eq!(app.task_picker.selected_item().unwrap().chat_index, 1);
    assert_eq!(app.resolve_render_chat(), 1);
}

#[test]
fn task_age_uses_monotonic_start() {
    let now = Instant::now();
    let clock = ClockSample {
        monotonic: now,
        unix: Duration::ZERO,
    };
    let entry = |started_at| TaskEntry {
        name: "task".into(),
        finished: Some(true),
        chat_index: 1,
        identity: TaskIdentity::Unidentified(1),
        parent_chat_index: None,
        depth: 0,
        snippet: String::new(),
        context: String::new(),
        started_at,
    };

    assert_eq!(
        entry(Some(now)).age(&clock).map(makima_lua::format_ago),
        Some("0s".into())
    );
    assert_eq!(
        entry(Some(now - Duration::from_secs(5 * 60)))
            .age(&clock)
            .map(makima_lua::format_ago),
        Some("5m 0s".into())
    );
    assert_eq!(
        entry(Some(now - Duration::from_secs(2 * 60 * 60)))
            .age(&clock)
            .map(makima_lua::format_ago),
        Some("2h 0s".into())
    );
    assert_eq!(
        entry(Some(now - Duration::from_secs(3 * 24 * 60 * 60)))
            .age(&clock)
            .map(makima_lua::format_ago),
        Some("3d 0s".into())
    );
    assert_eq!(entry(None).age(&clock), None);
}

fn picker_rows(picker: &mut ListPicker<TaskEntry>, now: &ClockSample) -> Vec<String> {
    let area = Rect::new(0, 0, 80, 24);
    let backend = ratatui::backend::TestBackend::new(area.width, area.height);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| {
            picker.view_at(frame, area, now);
        })
        .unwrap();
    let buf = terminal.backend().buffer();
    (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| {
                    buf.cell(ratatui::layout::Position::new(x, y))
                        .map(|c| c.symbol())
                        .unwrap_or(" ")
                })
                .collect()
        })
        .collect()
}

#[test]
fn task_picker_finished_ages_advance_without_row_refresh() {
    let mut app = app_with_subagent();
    finish_subagent_task(&mut app, false);
    app.open_tasks();
    let started = app.chats[1].started_at().unwrap();
    let first = ClockSample {
        monotonic: started + Duration::from_secs(5),
        unix: Duration::ZERO,
    };
    let second = ClockSample {
        monotonic: first.monotonic + crate::repaint::AGE_FRAME,
        unix: Duration::ZERO,
    };

    let rows = picker_rows(&mut app.task_picker, &first);
    assert!(rows.iter().any(|row| row.contains("5s")));
    let selected = app.task_picker.selected_index();
    let count = app.task_picker.items().unwrap().len();
    let selected_chat = app
        .task_picker
        .selected_item()
        .map(|entry| entry.chat_index);

    let rows = picker_rows(&mut app.task_picker, &second);
    assert!(rows.iter().any(|row| row.contains("6s")));
    assert_eq!(app.task_picker.selected_index(), selected);
    assert_eq!(app.task_picker.items().unwrap().len(), count);
    assert_eq!(
        app.task_picker
            .selected_item()
            .map(|entry| entry.chat_index),
        selected_chat
    );

    let cadence = app.task_picker.cadence();
    assert_eq!(cadence.frame(), Some(crate::repaint::AGE_FRAME));
    assert!(
        cadence.moves(),
        "a finished-only task list still owes age frames"
    );
    app.task_picker.close();
    assert_eq!(app.task_picker.cadence(), Cadence::IDLE);
}

#[test]
fn task_picker_preserves_missing_and_completed_age_semantics() {
    let mut app = app_with_subagent();
    finish_subagent_task(&mut app, false);
    let started = app.chats[1].started_at().unwrap();
    let clock = ClockSample {
        monotonic: started + Duration::from_secs(5),
        unix: Duration::ZERO,
    };
    let entries = app.task_entries();
    let sub = entries.iter().find(|entry| entry.chat_index == 1).unwrap();
    assert_eq!(sub.age(&clock), Some(Duration::from_secs(5)));
    assert!(entries[0].age(&clock).is_none(), "the main chat has no age");
    let later = ClockSample {
        monotonic: clock.monotonic + Duration::from_secs(1),
        unix: Duration::ZERO,
    };
    assert_eq!(
        sub.age(&later),
        Some(Duration::from_secs(6)),
        "completed tasks keep aging instead of freezing their runtime"
    );
}

#[test]
fn task_entries_sorted_running_first() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    finish_subagent(&mut app, "task1", false);

    let entries = app.task_entries();
    assert_eq!(entries[0].chat_index, 0, "main chat stays first");
    assert!(
        !entries[1].is_finished(),
        "running subagent before finished"
    );
    assert!(entries[1].is_spinning());
    assert!(entries[2].is_finished(), "finished subagent last");
}

#[test]
fn task_entries_sort_alive_then_most_recent() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "z".into() },
        "task3",
        Some("test"),
    ));

    finish_subagent(&mut app, "task3", false);

    let entries = app.task_entries();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.chat_index)
            .collect::<Vec<_>>(),
        vec![0, 2, 1, 3]
    );
}

#[test]
fn task_entry_shows_context_and_age() {
    let mut app = app_with_subagent();
    app.chats[1].context_size = 5000;
    let entries = app.task_entries();

    let clock = ClockSample {
        monotonic: Instant::now(),
        unix: Duration::ZERO,
    };
    let sub = entries.iter().find(|e| e.chat_index == 1).unwrap();
    assert_eq!(sub.context_str(), Some("5.0k"));
    assert!(sub.age(&clock).is_some());

    let main = &entries[0];
    assert_eq!(main.context_str(), None);
    assert_eq!(main.age(&clock), None);
}

#[test]
fn finished_subagent_entry_is_finished_flag() {
    let mut app = app_with_subagent();
    let entries = app.task_entries();
    assert!(!entries[0].is_finished(), "main chat is not finished");
    assert!(
        !entries[1].is_finished(),
        "running subagent is not finished"
    );

    finish_subagent_task(&mut app, false);
    let done_entries = app.task_entries();
    let done = done_entries.iter().find(|e| e.chat_index == 1).unwrap();
    assert!(done.is_finished());
}

fn render_app(app: &mut App, width: u16, height: u16) {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal.draw(|frame| app.view(frame)).unwrap();
}

fn hit_center(app: &App, target: ClickTarget) -> (u16, u16) {
    let rect = app.hits.rect_of(target).expect("target is registered");
    (rect.x + rect.width / 2, rect.y + rect.height / 2)
}

fn click_target(app: &mut App, target: ClickTarget) -> Vec<Action> {
    render_app(app, 100, 40);
    let (x, y) = hit_center(app, target);
    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), x, y));
    app.update(mouse_event(MouseEventKind::Up(MouseButton::Left), x, y))
}

#[derive(Clone, Copy, Debug)]
enum ScrollbarFixture {
    Model,
    Task,
    Input,
    Search,
    Help,
    Btw,
    Split,
    Float,
    Transcript,
}

const SCROLLBAR_ROWS: usize = 80;
const PINNED_TOP: &str = "pinned header";
const PINNED_BOTTOM: &str = "pinned footer";

fn scrollbar_fixture(fixture: ScrollbarFixture) -> (App, ScrollOwner) {
    let mut app = app_without_splash();
    let owner = match fixture {
        ScrollbarFixture::Model => {
            app = app_with_tall_model_picker();
            ScrollOwner::Picker(PickerId::Model)
        }
        ScrollbarFixture::Task => {
            for i in 0..SCROLLBAR_ROWS {
                app = with_subagent(app, &format!("scroll-task-{i}"));
            }
            app.open_tasks();
            ScrollOwner::Picker(PickerId::Task)
        }
        ScrollbarFixture::Input => {
            app.update(Msg::Paste(
                (0..SCROLLBAR_ROWS)
                    .map(|i| format!("draft row {i}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
            ScrollOwner::Input
        }
        ScrollbarFixture::Search => {
            app = middle_scroll_transcript();
            open_search(&mut app);
            app.update(Msg::Paste("transcript".into()));
            ScrollOwner::Picker(PickerId::Search)
        }
        ScrollbarFixture::Help => {
            app.help_modal.toggle();
            ScrollOwner::Help
        }
        ScrollbarFixture::Btw => {
            app.btw_modal = crate::components::btw_modal::BtwModal::new(0);
            let (tx, rx) = flume::bounded(2);
            app.btw_modal.open("scroll regression", rx);
            tx.send(BtwEvent::TextDelta(
                (0..SCROLLBAR_ROWS)
                    .map(|i| format!("answer row {i}\n\n"))
                    .collect(),
            ))
            .unwrap();
            tx.send(BtwEvent::Done).unwrap();
            let _ = app.btw_modal.poll();
            ScrollOwner::Btw
        }
        ScrollbarFixture::Split | ScrollbarFixture::Float => {
            open_scrollbar_window(
                &mut app,
                if matches!(fixture, ScrollbarFixture::Split) {
                    makima_lua::Split::Above
                } else {
                    makima_lua::Split::None
                },
            );
            ScrollOwner::Float(0)
        }
        ScrollbarFixture::Transcript => {
            app = middle_scroll_transcript();
            ScrollOwner::Messages
        }
    };
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(app.hits.rect_of(ClickTarget::Scrollbar(owner)).is_some());
    (app, owner)
}

fn open_scrollbar_window(app: &mut App, split: makima_lua::Split) {
    let buf = Arc::new(makima_agent::SharedBuf::new());
    buf.append(makima_agent::SnapshotLine::plain(PINNED_TOP.into()));
    for i in 0..SCROLLBAR_ROWS {
        buf.append(makima_agent::SnapshotLine::plain(format!("window row {i}")));
    }
    buf.append(makima_agent::SnapshotLine::plain(PINNED_BOTTOM.into()));
    let config = makima_lua::FloatConfig {
        width: makima_lua::Dimension::Abs(40),
        height: makima_lua::Dimension::Abs(10),
        reserved_top: 1,
        reserved_bottom: 1,
        split,
        ..makima_lua::FloatConfig::default()
    };
    let (event_tx, _event_rx) = flume::bounded(8);
    let (_cmd_tx, cmd_rx) = flume::bounded(8);
    app.float_mgr.open(buf, config, false, event_tx, cmd_rx);
}

fn scrollbar_mouse_frame(app: &mut App, kind: MouseEventKind, x: u16, y: u16) {
    assert!(app.update(mouse_event(kind, x, y)).is_empty());
    render_app(app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(app.selection_state.is_none());
    assert!(app.pending_click.is_none());
}

fn scrollbar_selection(app: &App) -> (Option<usize>, Option<usize>, usize, usize) {
    (
        app.task_picker.selected_index(),
        app.search_modal.current_segment_index(),
        app.input_box.buffer.cursor_byte_offset(),
        app.active_chat,
    )
}

#[test_case(ScrollbarFixture::Model; "model")]
#[test_case(ScrollbarFixture::Task; "task_list_picker")]
#[test_case(ScrollbarFixture::Input; "input")]
#[test_case(ScrollbarFixture::Search; "search")]
#[test_case(ScrollbarFixture::Help; "help")]
#[test_case(ScrollbarFixture::Btw; "btw")]
#[test_case(ScrollbarFixture::Split; "passive_split_pinned")]
#[test_case(ScrollbarFixture::Float; "passive_float_pinned")]
#[test_case(ScrollbarFixture::Transcript; "transcript")]
fn scrollbar_drag_survives_each_render(fixture: ScrollbarFixture) {
    let (mut app, owner) = scrollbar_fixture(fixture);
    let selection = scrollbar_selection(&app);
    app.scroll_owner_to(owner, u16::MAX);
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let bottom = app.scroll_line(owner);
    assert!(bottom > 0);
    app.scroll_owner_to(owner, bottom / 3);
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let initial = app.scroll_line(owner);
    let rect = app.hits.rect_of(ClickTarget::Scrollbar(owner)).unwrap();
    let content = app.scroll_content_len(owner).unwrap();
    let (_, thumb_start) = crate::components::scrollbar::thumb(rect.height, content, initial);
    let thumb_y = rect.y + thumb_start;
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Down(MouseButton::Left),
        rect.x,
        thumb_y,
    );
    assert_eq!(app.scroll_line(owner), initial);
    assert!(app.scroll_drag.is_some());
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Drag(MouseButton::Left),
        rect.x,
        thumb_y,
    );
    assert_eq!(app.scroll_line(owner), initial);
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Up(MouseButton::Left),
        rect.x,
        thumb_y,
    );
    assert_eq!(app.scroll_line(owner), initial);
    assert!(app.scroll_drag.is_none());

    app.scroll_owner_to(owner, 0);
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Down(MouseButton::Left),
        rect.x,
        rect.y,
    );
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Drag(MouseButton::Left),
        rect.x,
        rect.bottom() - 1,
    );
    assert_eq!(app.scroll_line(owner), bottom);
    assert!(app.scroll_drag.is_some());
    scrollbar_mouse_frame(&mut app, MouseEventKind::Up(MouseButton::Left), 0, 0);
    assert_eq!(app.scroll_line(owner), 0);
    assert!(app.scroll_drag.is_none());
    assert_eq!(scrollbar_selection(&app), selection);

    if matches!(fixture, ScrollbarFixture::Split | ScrollbarFixture::Float) {
        let rows = rendered_area(&mut app);
        assert!(rows.iter().any(|row| row.contains(PINNED_TOP)));
        assert!(rows.iter().any(|row| row.contains(PINNED_BOTTOM)));
        assert!(!app.float_mgr.is_focused());
    }
    if matches!(fixture, ScrollbarFixture::Model) {
        assert!(matches!(
            app.model_picker.handle_key(key(KeyCode::Enter)),
            crate::components::model_picker::ModelPickerAction::Select(selection)
                if selection.spec.as_deref() == Some("anthropic/model-0")
        ));
    }
}

#[test_case(ScrollbarFixture::Model; "model")]
#[test_case(ScrollbarFixture::Task; "task_list_picker")]
#[test_case(ScrollbarFixture::Input; "input")]
#[test_case(ScrollbarFixture::Search; "search")]
#[test_case(ScrollbarFixture::Help; "help")]
#[test_case(ScrollbarFixture::Btw; "btw")]
#[test_case(ScrollbarFixture::Split; "passive_split")]
#[test_case(ScrollbarFixture::Float; "passive_float")]
#[test_case(ScrollbarFixture::Transcript; "transcript")]
fn scrollbar_track_jumps_and_release_uses_final_coordinates(fixture: ScrollbarFixture) {
    let (mut app, owner) = scrollbar_fixture(fixture);
    app.scroll_owner_to(owner, u16::MAX);
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let bottom = app.scroll_line(owner);
    let selection = scrollbar_selection(&app);
    app.scroll_owner_to(owner, 0);
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let rect = app.hits.rect_of(ClickTarget::Scrollbar(owner)).unwrap();
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Down(MouseButton::Left),
        rect.x,
        rect.bottom() - 1,
    );
    assert_eq!(app.scroll_line(owner), bottom);
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Drag(MouseButton::Left),
        rect.x,
        rect.y,
    );
    assert_eq!(app.scroll_line(owner), 0);
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Up(MouseButton::Left),
        RENDER_AREA.width - 1,
        RENDER_AREA.height - 1,
    );
    assert_eq!(app.scroll_line(owner), bottom);
    assert!(app.scroll_drag.is_none());
    assert_eq!(scrollbar_selection(&app), selection);
}

#[test_case(0; "resize")]
#[test_case(1; "upper_overlay")]
#[test_case(2; "content_change")]
fn scrollbar_render_invalidates_stale_drag(change: u8) {
    let (mut app, owner) = scrollbar_fixture(ScrollbarFixture::Input);
    app.scroll_owner_to(owner, 0);
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let rect = app.hits.rect_of(ClickTarget::Scrollbar(owner)).unwrap();
    scrollbar_mouse_frame(
        &mut app,
        MouseEventKind::Down(MouseButton::Left),
        rect.x,
        rect.y,
    );
    assert!(app.scroll_drag.is_some());
    match change {
        0 => render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height + 8),
        1 => {
            app.help_modal.toggle();
            render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
        }
        _ => {
            app.input_box.buffer.clear();
            render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
        }
    }
    assert!(app.scroll_drag.is_none());
    let offset = app.scroll_line(owner);
    let height = RENDER_AREA.height + if change == 0 { 8 } else { 0 };
    if change == 1 {
        let (_, help) = app.hits.modal().unwrap();
        assert!(!help.contains(ratatui::layout::Position::new(rect.x, rect.bottom())));
    }
    assert!(
        app.update(mouse_event(
            MouseEventKind::Drag(MouseButton::Left),
            rect.x,
            rect.bottom()
        ))
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, height);
    assert!(
        app.update(mouse_event(
            MouseEventKind::Up(MouseButton::Left),
            rect.x,
            rect.bottom()
        ))
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, height);
    assert_eq!(app.scroll_line(owner), offset);
    assert!(app.selection_state.is_none());
    if change == 1 {
        assert!(app.help_modal.is_open());
        assert_eq!(
            app.hits.modal().map(|(surface, _)| surface),
            Some(Surface::Help)
        );
    }
}

#[test_case(0; "permission")]
#[test_case(1; "question")]
#[test_case(2; "upper_help_modal")]
fn rendered_upper_surface_blocks_covered_picker_and_wheel(surface: u8) {
    let mut app = app_with_tall_model_picker();
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let covered_rows: Vec<_> = (0..40)
        .filter_map(|index| {
            let target = ClickTarget::PickerRow {
                picker: PickerId::Model,
                index,
            };
            app.hits.rect_of(target).map(|rect| (target, rect))
        })
        .collect();
    match surface {
        0 => {
            app.permission_prompt.open(
                "scroll-permission".into(),
                makima_config::ToolKey::native(PERM_TOOL),
                vec![PERM_SCOPE.into()],
                None,
            );
            app.active_input = Some(InputKind::Permission);
        }
        1 => {
            let buf = Arc::new(makima_agent::SharedBuf::new());
            let (event_tx, _event_rx) = flume::bounded(8);
            let (_cmd_tx, cmd_rx) = flume::bounded(8);
            app.float_mgr
                .open(buf, below_input_config(16), true, event_tx, cmd_rx);
            app.active_input = Some(InputKind::Question);
        }
        _ => app.help_modal.toggle(),
    }
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let upper = match surface {
        0 => Surface::Permission,
        1 => Surface::Float(0),
        _ => Surface::Help,
    };
    let upper_rect = app.hits.rect_of(ClickTarget::Surface(upper)).unwrap();
    let (covered, pos) = covered_rows
        .iter()
        .flat_map(|&(target, rect)| {
            (rect.y..rect.bottom()).flat_map(move |y| {
                (rect.x..rect.right()).map(move |x| (target, ratatui::layout::Position::new(x, y)))
            })
        })
        .find(|&(_, pos)| {
            upper_rect.contains(pos)
                && (app.hits.target_at(pos) == Some(ClickTarget::Surface(upper))
                    || (surface == 1
                        && app.hits.target_at(pos) == Some(ClickTarget::WindowContent(0))))
        })
        .expect("upper surface has an opaque blank area over a picker row");
    let picker_offset = app.scroll_line(ScrollOwner::Picker(PickerId::Model));
    let chat_offset = app.chats[0].scroll_top();
    assert_eq!(app.hits.surface_at(pos), Some(upper));
    assert_ne!(app.hits.target_at(pos), Some(covered));
    assert!(
        app.update(mouse_event(
            MouseEventKind::Down(MouseButton::Left),
            pos.x,
            pos.y
        ))
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(
        app.update(mouse_event(
            MouseEventKind::Up(MouseButton::Left),
            pos.x,
            pos.y
        ))
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(app.model_picker.is_open());
    assert!(
        app.update(Msg::Scroll {
            column: pos.x,
            row: pos.y,
            delta: -4
        })
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert_eq!(
        app.scroll_line(ScrollOwner::Picker(PickerId::Model)),
        picker_offset
    );
    assert_eq!(app.chats[0].scroll_top(), chat_offset);
    assert!(
        app.update(Msg::Scroll {
            column: 0,
            row: 0,
            delta: -4
        })
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert_eq!(
        app.scroll_line(ScrollOwner::Picker(PickerId::Model)),
        picker_offset
    );
    assert_eq!(app.chats[0].scroll_top(), chat_offset);
}

#[test_case(0; "help")]
#[test_case(1; "model_picker")]
#[test_case(2; "focused_float")]
fn wheel_scrolls_exposed_transcript_without_changing_modal(surface: u8) {
    let mut app = middle_scroll_transcript();
    match surface {
        0 => app.help_modal.toggle(),
        1 => {
            app.run_builtin(BuiltinAction::ModelPicker);
        }
        _ => {
            let buf = Arc::new(makima_agent::SharedBuf::new());
            let (event_tx, _event_rx) = flume::bounded(8);
            let (_cmd_tx, cmd_rx) = flume::bounded(8);
            app.float_mgr.open(
                buf,
                makima_lua::FloatConfig {
                    width: makima_lua::Dimension::Abs(40),
                    height: makima_lua::Dimension::Abs(10),
                    ..makima_lua::FloatConfig::default()
                },
                true,
                event_tx,
                cmd_rx,
            );
        }
    }
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let modal = app.hits.modal();
    assert!(modal.is_some());
    let area = app.zones.find(SelectionZone::Messages).unwrap().area;
    let pos = (area.y..area.bottom())
        .flat_map(|y| (area.x..area.right()).map(move |x| ratatui::layout::Position::new(x, y)))
        .find(|&pos| app.hits.scroll_surface_at(pos).is_none())
        .unwrap();
    let before = app.chats[0].scroll_top();
    assert!(before > 0);
    assert!(
        app.update(Msg::Scroll {
            column: pos.x,
            row: pos.y,
            delta: 4
        })
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(app.chats[0].scroll_top() < before);
    assert_eq!(app.hits.modal(), modal);
    assert_eq!(app.active_chat, 0);
    assert!(app.selection_state.is_none());
}

#[test_case(ScrollbarFixture::Model; "lower_picker")]
#[test_case(ScrollbarFixture::Input; "input")]
#[test_case(ScrollbarFixture::Split; "passive_split")]
fn wheel_scrolls_exposed_lower_surface_without_click_activation(fixture: ScrollbarFixture) {
    let (mut app, owner) = scrollbar_fixture(fixture);
    app.scroll_owner_to(owner, 0);
    app.help_modal.toggle();
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    let help = app.hits.modal().unwrap();
    assert_eq!(help.0, Surface::Help);
    let pos = if owner == ScrollOwner::Input {
        let area = app.zones.find(SelectionZone::Input).unwrap().area;
        (area.y..area.bottom())
            .flat_map(|y| (area.x..area.right()).map(move |x| ratatui::layout::Position::new(x, y)))
            .find(|&pos| app.hits.scroll_surface_at(pos).is_none())
    } else {
        let lower = match owner {
            ScrollOwner::Picker(picker) => Surface::Picker(picker),
            ScrollOwner::Float(id) => Surface::Float(id),
            _ => unreachable!(),
        };
        (0..RENDER_AREA.height)
            .flat_map(|y| (0..RENDER_AREA.width).map(move |x| ratatui::layout::Position::new(x, y)))
            .find(|&pos| app.hits.scroll_surface_at(pos) == Some(lower))
    }
    .expect("lower surface remains exposed beside help");
    let help_offset = app.help_modal.scroll_line();
    let before = app.scroll_line(owner);
    let selection = scrollbar_selection(&app);
    assert!(app.hits.target_at(pos).is_none());
    assert!(
        app.update(Msg::Scroll {
            column: pos.x,
            row: pos.y,
            delta: -4
        })
        .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(app.scroll_line(owner) > before);
    assert_eq!(app.help_modal.scroll_line(), help_offset);
    assert_eq!(app.hits.modal(), Some(help));
    assert_eq!(scrollbar_selection(&app), selection);
}

#[test]
fn rendered_outside_click_dismisses_only_upper_modal() {
    let mut app = app_with_tall_model_picker();
    app.help_modal.toggle();
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert_eq!(
        app.hits.modal().map(|(surface, _)| surface),
        Some(Surface::Help)
    );
    assert!(
        app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 0, 0))
            .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(
        app.update(mouse_event(MouseEventKind::Up(MouseButton::Left), 0, 0))
            .is_empty()
    );
    render_app(&mut app, RENDER_AREA.width, RENDER_AREA.height);
    assert!(!app.help_modal.is_open());
    assert!(app.model_picker.is_open());
    assert_eq!(
        app.hits.modal().map(|(surface, _)| surface),
        Some(Surface::Picker(PickerId::Model))
    );
    assert!(app.selection_state.is_none());
}

#[test_case(false, false; "normal_current_choice")]
#[test_case(false, true; "normal_to_yolo")]
#[test_case(true, true; "yolo_current_choice")]
#[test_case(true, false; "yolo_to_normal")]
fn permission_chip_menu_sets_explicit_value(current: bool, enabled: bool) {
    let mut app = test_app();
    app.permissions.set_yolo(current);
    let rows = rendered_area(&mut app);
    let chip = if current { "[yolo]" } else { "[normal]" };
    assert!(rows.last().unwrap().contains(chip));
    assert!(click_target(&mut app, ClickTarget::PermissionChip).is_empty());
    assert_eq!(app.permissions.is_yolo(), current);
    assert!(app.menu.is_open());
    assert!(click_target(&mut app, ClickTarget::MenuRow(u16::from(enabled))).is_empty());
    assert_eq!(app.permissions.is_yolo(), enabled);
    assert!(!app.menu.is_open());
}

#[test]
fn click_mode_chip_opens_menu() {
    let mut app = test_app();
    click_target(&mut app, ClickTarget::ModeChip);
    assert!(app.menu.is_open());
}

#[test]
fn menu_mode_row_emits_change_mode() {
    let mut app = test_app();
    click_target(&mut app, ClickTarget::ModeChip);
    render_app(&mut app, 100, 40);
    let actions = click_target(&mut app, ClickTarget::MenuRow(1));
    assert!(matches!(actions.as_slice(), [Action::ChangeMode(_)]));
}

#[test]
fn click_model_chip_opens_model_picker() {
    let mut app = test_app();
    let actions = click_target(&mut app, ClickTarget::ModelChip);
    assert!(matches!(actions.as_slice(), [Action::RefreshModels]));
    assert!(app.model_picker.is_open());
}

#[test]
fn click_agent_badge_opens_tasks() {
    let mut app = test_app();
    click_target(&mut app, ClickTarget::AgentChip);
    assert!(app.task_picker.is_open());
}

#[test]
fn outside_click_closes_model_picker() {
    let mut app = test_app();
    app.run_builtin(makima_lua::BuiltinAction::ModelPicker);
    render_app(&mut app, 100, 40);
    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 0, 0));
    app.update(mouse_event(MouseEventKind::Up(MouseButton::Left), 0, 0));
    assert!(!app.model_picker.is_open());
}

#[test]
fn model_footer_enter_does_not_answer_active_permission_confirmation() {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![
        MODEL_SPEC_GLM4.into(),
        MODEL_SPEC_GPT9.into(),
    ])));
    app.run_builtin(BuiltinAction::ModelPicker);
    let (answer_tx, answer_rx) = flume::bounded(1);
    app.answer_tx = Some(answer_tx);
    app.permission_prompt.open(
        "footer-permission".into(),
        makima_config::ToolKey::native(PERM_TOOL),
        vec![PERM_SCOPE.into()],
        None,
    );
    app.active_input = Some(InputKind::Permission);
    assert!(app.update(Msg::Key(key(KeyCode::Char('a')))).is_empty());
    render_app(&mut app, TEST_AREA.width, TEST_AREA.height);
    let target = ClickTarget::KeyHint(key(KeyCode::Enter));
    let (x, y) = hit_center(&app, target);
    let pos = ratatui::layout::Position::new(x, y);
    assert_eq!(app.hits.target_at(pos), Some(target));
    assert_eq!(
        app.hits.surface_at(pos),
        Some(Surface::Picker(PickerId::Model))
    );
    assert!(
        app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), x, y))
            .is_empty()
    );
    render_app(&mut app, TEST_AREA.width, TEST_AREA.height);
    let actions = app.update(mouse_event(MouseEventKind::Up(MouseButton::Left), x, y));
    render_app(&mut app, TEST_AREA.width, TEST_AREA.height);
    assert!(matches!(
        actions.as_slice(),
        [Action::SelectModel(_) | Action::SelectPinnedModel { .. }]
    ));
    assert!(!app.model_picker.is_open());
    assert!(app.permission_active());
    assert!(app.permission_prompt.is_open());
    assert!(answer_rx.try_recv().is_err());
    assert!(matches!(
        app.permission_prompt.handle_key(key(KeyCode::Enter)),
        Some(makima_agent::permissions::PermissionAnswer::AllowAlwaysLocal)
    ));
}

#[test]
fn click_permission_hint_answers() {
    let mut app = test_app();
    app.permission_prompt.open(
        "req".into(),
        makima_config::ToolKey::native("bash"),
        vec!["execute".into()],
        None,
    );
    app.active_input = Some(crate::app::InputKind::Permission);
    render_app(&mut app, 100, 40);
    click_target(&mut app, ClickTarget::PermissionHint { row: 0, cell: 0 });
    assert!(!app.permission_prompt.is_open());
}

fn rendered_rows(app: &mut App, width: u16, height: u16) -> Vec<String> {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal.draw(|frame| app.view(frame)).unwrap();
    let buf = terminal.backend().buffer();
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| {
                    buf.cell(ratatui::layout::Position::new(x, y))
                        .map(|c| c.symbol())
                        .unwrap_or(" ")
                })
                .collect()
        })
        .collect()
}

#[test]
fn focused_running_subagent_draws_status_bar_spinner_while_main_is_idle() {
    const SPINNER_GLYPHS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

    let mut app = app_with_subagent();
    app.status = Status::Idle;
    app.active_chat = 1;
    assert!(!app.chats[1].is_finished());
    assert!(
        app.cadence().moves(),
        "the footer spinner must keep repainting"
    );

    let rows = rendered_rows(&mut app, 80, 24);
    let status = rows.last().expect("status row");
    assert!(
        status.chars().any(|ch| SPINNER_GLYPHS.contains(ch)),
        "focused running subagent must animate the status bar: {status}"
    );
}

#[test]
fn top_bar_is_always_one_row() {
    let area = Rect::new(0, 0, 80, 24);
    // Persistent even with no subagents.
    assert_eq!(test_app().top_bar_rect(area).height, 1);

    // Stays one row regardless of how many subagents are running.
    let mut app = app_with_subagent();
    assert_eq!(app.top_bar_rect(area).height, 1);
    for i in 2..=6 {
        app.update(subagent_msg(
            AgentEvent::TextDelta { text: "x".into() },
            &format!("task{i}"),
            Some("name"),
        ));
    }
    assert_eq!(app.top_bar_rect(area).height, 1, "top bar stays one row");
}

#[test]
fn top_bar_shows_active_chat_running_count_and_cwd() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    finish_subagent(&mut app, "task2", false);

    let bar_h = app.top_bar_rect(Rect::new(0, 0, 120, 24)).height;
    let rows = rendered_rows(&mut app, 120, 24);
    let bar: String = rows
        .iter()
        .take(bar_h as usize)
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    // Active chat badge for the main chat.
    assert!(bar.contains("[Main]"), "active badge: {bar}");
    // One running subagent besides the main chat.
    assert!(bar.contains("1 tasks"), "running count: {bar}");
    assert!(bar.contains(kb::TASKS.label), "ctrl-x hint: {bar}");
    // The right side may truncate the cwd, but the branch should remain visible.
    let branch = app
        .status_bar
        .cwd_branch()
        .rsplit_once(':')
        .map_or_else(|| app.status_bar.cwd_branch(), |(_, branch)| branch);
    assert!(bar.contains(branch), "branch shown: {bar}");
    // No per-subagent rows in the bar.
    assert!(!bar.contains("research"), "no name rows: {bar}");

    // A second running subagent bumps the count.
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "z".into() },
        "task3",
        Some("third"),
    ));
    let rows = rendered_area(&mut app);
    let bar: String = rows
        .iter()
        .take(bar_h as usize)
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(bar.contains("2 tasks"), "two running: {bar}");
}

#[test]
fn top_bar_shows_task_hint_when_no_subagents_are_running() {
    let mut app = test_app();
    let rows = rendered_area(&mut app);
    let bar: String = rows.first().map(|s| s.as_str()).unwrap_or("").to_string();
    assert!(bar.contains("[Main]"), "badge always present: {bar}");
    assert!(
        bar.contains("to see tasks"),
        "task hint always present: {bar}"
    );
    assert!(bar.contains(kb::TASKS.label), "ctrl-x hint: {bar}");
}

#[test]
fn top_bar_shows_subagent_badge_when_tabbed_into_subagent() {
    let mut app = app_with_subagent();
    app.active_chat = 1;
    let rows = rendered_area(&mut app);
    let bar: String = rows.first().map(|s| s.as_str()).unwrap_or("").to_string();
    assert!(bar.contains("↳"), "subagent badge: {bar}");
    assert!(bar.contains("research"), "subagent name: {bar}");
    // The task hint includes all non-main subagents, including the active one.
    assert!(bar.contains("1 tasks"), "running count: {bar}");
}

#[test]
fn top_bar_hint_excludes_finished_subagents() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    finish_subagent(&mut app, "task1", false);
    // task2 still running, task1 finished: from Main, one more running.
    let rows = rendered_area(&mut app);
    let bar: String = rows.first().map(|s| s.as_str()).unwrap_or("").to_string();
    assert!(bar.contains("1 tasks"), "only running counted: {bar}");
}

#[test_case(false; "restored_identity")]
#[test_case(true; "missing_identity")]
fn restored_task_keyboard_submission_never_reaches_main(missing_identity: bool) {
    const INPUT: &str = "hi this is a test";
    const TASK: &str = "restored-task";
    let mut app = test_app();
    app.state.session_mut().set_subagents(vec![StoredSubagent {
        tool_use_id: TASK.into(),
        agent_id: None,
        ambiguous: false,
        name: "old task".into(),
        model: None,
        thinking: None,
        fast: false,
    }]);
    app.state
        .session_mut()
        .set_subagent_messages(TASK.into(), vec![Message::user("prior work".into())]);
    app.restore_display();
    if missing_identity {
        app.chats[1].subagent_id = None;
    }
    app.update(Msg::Key(kb::TASKS.to_key_event()));
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.active_chat, 1);
    assert!(app.active_subagent_closed());
    let main_messages = app.chats[0].message_count();
    for character in INPUT.chars() {
        assert!(
            app.update(Msg::Key(key(KeyCode::Char(character))))
                .is_empty()
        );
    }
    assert!(app.update(Msg::Key(key(KeyCode::Enter))).is_empty());
    assert_eq!(app.status, Status::Idle);
    assert_eq!(app.chats[0].message_count(), main_messages);
    assert!(app.queue.root_text_messages().is_empty());
    assert!(matches!(
        app.submit_prompt(queued_msg(INPUT)),
        SubmitOutcome::Rejected(error) if error == queue::NO_SUBAGENT_ERR
    ));
    assert!(
        app.handle_submit(Submission {
            text: "!pwd".into(),
            images: Vec::new(),
        })
        .is_empty()
    );
}

#[test]
fn restored_subagents_have_no_ago_and_are_finished() {
    let mut app = test_app();
    let id = "restored-sub".to_string();
    app.state.session_mut().set_subagents(vec![StoredSubagent {
        tool_use_id: id.clone(),
        agent_id: None,
        ambiguous: false,
        name: "old task".into(),
        model: None,
        thinking: None,
        fast: false,
    }]);
    app.state
        .session_mut()
        .set_subagent_messages(id.clone(), vec![Message::user("hi".into())]);
    app.restore_display();

    assert_eq!(app.chats.len(), 2);
    assert!(app.chats[1].is_finished());
    assert_eq!(app.chats[1].started_at(), None);
    assert_eq!(app.chats[1].subagent_id.as_deref(), Some(id.as_str()));

    let entries = app.task_entries();
    let sub = entries.iter().find(|e| e.chat_index == 1).unwrap();
    assert!(sub.is_finished());
    assert_eq!(
        sub.age(&ClockSample {
            monotonic: Instant::now(),
            unix: Duration::ZERO,
        }),
        None
    );
    assert_eq!(sub.depth(), 0);
    assert_eq!(sub.parent_chat_index, None);

    let root = AgentId::generate();
    let child = AgentId::generate();
    app.chats[0].agent_id = Some(root);
    add_picker_agent(&mut app, child, root, "child");
    add_picker_agent(&mut app, AgentId::generate(), child, "grandchild");
    let entries = app.task_entries();
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.depth()))
            .collect::<Vec<_>>(),
        [
            ("Main", 0),
            ("child", 1),
            ("grandchild", 2),
            ("old task", 0)
        ]
    );
}

fn streaming_app() -> App {
    running_root_app().0
}

fn app_with_subagent_id(id: &str) -> App {
    with_subagent(streaming_app(), id)
}

fn with_subagent(mut app: App, id: &str) -> App {
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "x".into() },
        id,
        Some("research"),
    ));
    app
}

fn app_with_subagent() -> App {
    app_with_subagent_id(TASK_ID)
}

fn add_picker_agent(app: &mut App, agent_id: AgentId, parent_id: AgentId, name: &str) {
    let mut info = subagent_info_for_agent(agent_id, name, name, None, None);
    info.parent_agent_id = Some(parent_id);
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta { text: "x".into() },
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));
}

#[test_case(false, false; "parent_first")]
#[test_case(true, false; "parent_arrives_late")]
#[test_case(true, true; "parent_arrives_late_filtered")]
fn task_entries_preserve_hierarchy_with_unknown_ancestry(
    parent_arrives_late: bool,
    filtered: bool,
) {
    let mut app = app_with_subagent_id("unknown");
    let root = app.chats[0].agent_id.unwrap();
    let child = AgentId::generate();
    let grandchild = AgentId::generate();
    if parent_arrives_late {
        add_picker_agent(&mut app, grandchild, child, "grandchild");
        let entries = app.task_entries();
        let orphan = entries
            .iter()
            .find(|entry| entry.name == "grandchild")
            .unwrap();
        assert_eq!(orphan.depth(), 0);
        app.open_tasks();
        if filtered {
            app.task_picker.handle_paste("grandchild");
        }
        app.task_picker
            .select_item_by(|entry| entry.name == "grandchild");
    }
    add_picker_agent(&mut app, child, root, "child");
    if !parent_arrives_late {
        add_picker_agent(&mut app, grandchild, child, "grandchild");
    }
    let child_index = app.live_chat_index[&child];
    app.chats[child_index].mark_finished(DisplayRole::Assistant, DONE_TEXT);
    add_picker_agent(&mut app, AgentId::generate(), root, "sibling");

    let entries = app.task_entries();
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.name.as_str(), entry.depth()))
            .collect::<Vec<_>>(),
        [
            ("Main", 0),
            ("sibling", 1),
            ("child", 1),
            ("grandchild", 2),
            ("research", 0)
        ]
    );
    assert_eq!(app.chats[child_index].parent_agent_id, Some(root));
    if parent_arrives_late {
        let selected = app.task_picker.selected_item().unwrap();
        assert_eq!(selected.name, "grandchild");
        assert_eq!(selected.depth(), 2);
    }
}

#[test_case(false; "unfiltered")]
#[test_case(true; "filtered")]
fn task_picker_inserts_descendants_in_subtree_preserving_selection(filtered: bool) {
    let mut app = streaming_app();
    let root = app.chats[0].agent_id.unwrap();
    let child = AgentId::generate();
    let grandchild = AgentId::generate();
    add_picker_agent(&mut app, child, root, "branch");
    add_picker_agent(&mut app, AgentId::generate(), root, "branch sibling");
    app.open_tasks();
    if filtered {
        app.update(Msg::Key(key(KeyCode::Char('b'))));
    }
    app.task_picker
        .select_item_by(|entry| entry.name == "branch");
    add_picker_agent(&mut app, grandchild, child, "branch leaf");

    assert_eq!(app.task_picker.selected_item().unwrap().name, "branch");
    app.update(Msg::Key(key(KeyCode::Down)));
    assert_eq!(app.task_picker.selected_item().unwrap().name, "branch leaf");
    assert_eq!(app.task_picker.selected_item().unwrap().depth(), 2);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.active_chat, app.live_chat_index[&grandchild]);
}

#[test]
fn task_picker_nested_siblings_sort_on_open_and_stay_stable_during_refresh() {
    let mut app = streaming_app();
    let root = app.chats[0].agent_id.unwrap();
    let parent = AgentId::generate();
    let older = AgentId::generate();
    let newer = AgentId::generate();
    add_picker_agent(&mut app, parent, root, "parent");
    add_picker_agent(&mut app, older, parent, "older");
    add_picker_agent(&mut app, newer, parent, "newer");
    app.chats[app.live_chat_index[&newer]].mark_finished(DisplayRole::Assistant, DONE_TEXT);
    app.open_tasks();
    assert_eq!(app.task_picker.item(2).unwrap().name, "older");
    app.task_picker
        .select_item_by(|entry| entry.name == "newer");
    add_picker_agent(&mut app, AgentId::generate(), older, "new leaf");
    assert_eq!(app.task_picker.selected_item().unwrap().name, "newer");
    assert_eq!(app.task_picker.item(3).unwrap().name, "new leaf");

    app.chats[app.live_chat_index[&older]].mark_finished(DisplayRole::Assistant, DONE_TEXT);
    app.sync_task_picker();
    assert_eq!(app.task_picker.item(2).unwrap().name, "older");
    app.task_picker.close();
    app.open_tasks();
    assert_eq!(app.task_picker.item(2).unwrap().name, "newer");
    assert_eq!(app.task_picker.item(3).unwrap().name, "older");
}

#[test]
fn filtered_task_picker_keeps_ancestors_in_tree_order() {
    let mut app = streaming_app();
    let root = app.chats[0].agent_id.unwrap();
    let child = AgentId::generate();
    add_picker_agent(&mut app, child, root, "branch");
    add_picker_agent(&mut app, AgentId::generate(), child, "leaf");
    add_picker_agent(&mut app, AgentId::generate(), root, "leaf sibling");
    app.open_tasks();
    app.task_picker.handle_paste("leaf");
    assert_eq!(
        app.task_picker.selected_item().unwrap().name,
        "leaf sibling"
    );
    app.task_picker.select_item_by(|entry| entry.name == "Main");

    for name in ["Main", "leaf sibling", "branch", "leaf"] {
        assert_eq!(app.task_picker.selected_item().unwrap().name, name);
        app.update(Msg::Key(key(KeyCode::Down)));
    }
}

#[test]
fn open_task_picker_refreshes_after_tool_done() {
    let mut app = app_with_subagent();
    open_tasks_picker(&mut app);
    assert!(
        app.task_picker
            .selected_item()
            .is_some_and(|entry| entry.chat_index == 0)
    );

    finish_subagent_task(&mut app, false);

    assert!(app.task_picker.is_open());
    assert_eq!(app.task_picker.item(1).unwrap().finished, Some(true));
}

#[test]
fn open_task_picker_appends_new_child_without_changing_selection() {
    let mut app = app_with_subagent();
    open_tasks_picker(&mut app);
    app.update(Msg::Key(key(KeyCode::Down)));

    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "new".into() },
        "task2",
        Some("build"),
    ));

    assert_eq!(app.task_picker.item(1).unwrap().name, "research");
    assert_eq!(app.task_picker.item(2).unwrap().name, "build");
    assert_eq!(app.task_picker.selected_item().unwrap().name, "research");
}

#[test]
fn filtered_task_picker_enter_selects_entry_chat() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    open_tasks_picker(&mut app);
    app.update(Msg::Key(key(KeyCode::Char('b'))));

    assert_eq!(app.task_picker.selected_item().unwrap().chat_index, 2);
    app.update(Msg::Key(key(KeyCode::Enter)));

    assert!(!app.task_picker.is_open());
    assert_eq!(app.active_chat, 2);
}

#[test]
fn filtered_task_picker_refresh_preserves_selected_chat_identity() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    open_tasks_picker(&mut app);
    app.update(Msg::Key(key(KeyCode::Char('b'))));
    assert_eq!(app.task_picker.selected_item().unwrap().chat_index, 2);

    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "z".into() },
        "task3",
        Some("benchmark"),
    ));

    assert!(app.task_picker.is_open());
    assert_eq!(app.task_picker.selected_item().unwrap().chat_index, 2);
}

#[test]
fn filtered_task_picker_appends_higher_ranked_match_until_query_changes() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    open_tasks_picker(&mut app);
    app.update(Msg::Key(key(KeyCode::Char('b'))));
    assert_eq!(app.task_picker.selected_item().unwrap().name, "build");

    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "z".into() },
        "task3",
        Some("b"),
    ));
    assert_eq!(app.task_picker.selected_item().unwrap().name, "build");
    app.update(Msg::Key(key(KeyCode::Down)));
    assert_eq!(app.task_picker.selected_item().unwrap().name, "b");

    app.update(Msg::Key(key(KeyCode::Backspace)));
    app.update(Msg::Key(key(KeyCode::Char('b'))));
    assert_eq!(app.task_picker.selected_item().unwrap().name, "b");
    app.update(Msg::Key(key(KeyCode::Down)));
    assert_eq!(app.task_picker.selected_item().unwrap().name, "build");
}

#[test]
fn task_picker_reopen_restores_default_status_sorting() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    open_tasks_picker(&mut app);
    let first_task = app.task_picker.item(1).unwrap().chat_index;
    app.chats[first_task].mark_finished(DisplayRole::Assistant, "done");
    app.sync_task_picker();
    assert_eq!(app.task_picker.item(1).unwrap().chat_index, first_task);

    app.task_picker.close();
    open_tasks_picker(&mut app);
    assert_eq!(app.task_picker.item(1).unwrap().finished, Some(false));
    assert_eq!(app.task_picker.item(2).unwrap().chat_index, first_task);
}

#[test]
fn task_picker_refresh_keeps_existing_order_when_status_changes() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    open_tasks_picker(&mut app);
    let initial: Vec<_> = (0..3)
        .map(|index| app.task_picker.item(index).unwrap().name.clone())
        .collect();

    finish_subagent(&mut app, "task1", false);
    finish_subagent(&mut app, "task2", false);

    let refreshed: Vec<_> = (0..3)
        .map(|index| app.task_picker.item(index).unwrap().name.clone())
        .collect();
    assert_eq!(refreshed, initial);
}

#[test]
fn task_picker_refresh_removes_missing_rows_and_preserves_selection_by_identity() {
    let mut app = app_with_subagent_id("task1");
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    open_tasks_picker(&mut app);
    app.task_picker
        .select_item_by(|entry| entry.name == "build");
    let removed_index = app.chat_index.remove("task1").unwrap();
    app.chats[removed_index].subagent_id = None;
    app.chats[removed_index].agent_id = None;
    app.sync_task_picker();

    let names: Vec<_> = (0..2)
        .map(|index| app.task_picker.item(index).unwrap().name.clone())
        .collect();
    assert_eq!(names, ["Main", "build"]);
    assert_eq!(app.task_picker.selected_item().unwrap().name, "build");
}

#[test]
fn open_task_picker_refreshes_after_subagent_history() {
    let mut app = app_with_subagent_id("session-abc");
    open_tasks_picker(&mut app);

    app.update(agent_msg(AgentEvent::SubagentHistory {
        history_epoch: 0,
        conversation_id: None,
        tool_use_id: "session-abc".into(),
        messages: vec![],
    }));

    assert!(app.task_picker.is_open());
    assert_eq!(app.task_picker.item(1).unwrap().finished, Some(true));
}

#[test]
fn subagent_history_stores_the_child_conversation_id() {
    const CHILD_KEY: &str = "session-abc";
    const CHILD_CONVERSATION: &str = "child-conversation";
    let mut app = app_with_subagent_id(CHILD_KEY);
    app.update(agent_msg(AgentEvent::SubagentHistory {
        history_epoch: 0,
        conversation_id: Some(CHILD_CONVERSATION.into()),
        tool_use_id: CHILD_KEY.into(),
        messages: vec![Message::user("child".into())],
    }));

    let stored = app.state.session.subagent_history_snapshot(CHILD_KEY);
    assert_eq!(
        stored
            .and_then(|history| history.conversation)
            .map(|conversation| conversation.id)
            .as_deref(),
        Some(CHILD_CONVERSATION)
    );
}

#[test]
fn closed_task_picker_stays_closed_after_lifecycle_events() {
    let mut app = app_with_subagent();
    finish_subagent_task(&mut app, false);
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "new".into() },
        "task2",
        Some("build"),
    ));
    assert!(!app.task_picker.is_open());
}

#[test]
fn picker_escape_restores_chat() {
    let mut app = app_with_subagent();
    assert_eq!(app.active_chat, 0);

    open_tasks_picker(&mut app);
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(!app.task_picker.is_open());
    assert_eq!(app.active_chat, 0);
}

#[test]
fn picker_enter_stays_at_navigated() {
    let mut app = app_with_subagent();

    open_tasks_picker(&mut app);
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Enter)));

    assert!(!app.task_picker.is_open());
    assert_eq!(app.active_chat, 1);
}

const OVERLAY_BLOCKED_KEYS: &[KeyEvent] = &[
    KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL),
    kb::SCROLL_HALF_UP.to_key_event(),
    kb::SCROLL_HALF_DOWN.to_key_event(),
    kb::HELP.to_key_event(),
];

fn open_help(app: &mut App) {
    app.help_modal.toggle();
}

fn open_search(app: &mut App) {
    app.search_modal.open(0, true);
}

fn focus_queue(app: &mut App) {
    app.status = Status::Streaming;
    app.run_id = 1;
    app.queue_and_notify(queued_msg("q"));
    app.queue.set_focus_at(0);
}

#[test_case(open_tasks_picker as fn(&mut App) ; "task_picker")]
#[test_case(open_help                         ; "help_modal")]
#[test_case(open_search                       ; "search_modal")]
#[test_case(focus_queue                       ; "queue_focus")]
fn overlay_blocks_ctrl_shortcuts(setup: fn(&mut App)) {
    let mut app = app_with_subagent();
    setup(&mut app);
    let before = app.active_chat;
    let scroll_before = app.chats[app.active_chat].scroll_top();

    for k in OVERLAY_BLOCKED_KEYS {
        app.update(Msg::Key(*k));
    }

    assert_eq!(
        app.active_chat, before,
        "active_chat changed through overlay"
    );
    assert_eq!(
        app.chats[app.active_chat].scroll_top(),
        scroll_before,
        "scroll changed through overlay"
    );
}

const COMPACT_GUIDANCE: &str = "keep the failing test names";
const COMPACT_WITH_GUIDANCE: &str = "/compact keep the failing test names";

#[test]
fn cancel_clears_pending_input() {
    let mut app = streaming_app();
    app.pending_input = PendingInput::AuthRetry { agent_id: None };
    cancel_app(&mut app);
    assert_eq!(app.pending_input, PendingInput::None);
}

#[test]
fn scroll_disables_auto_scroll() {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 80, 20));
    app.active_chat().enable_auto_scroll();

    app.update(Msg::Scroll {
        column: 10,
        row: 10,
        delta: 3,
    });
    assert!(!app.chats[0].auto_scroll());
}

#[test]
fn scroll_outside_msg_area_ignored() {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 80, 20));
    app.active_chat().enable_auto_scroll();

    app.update(Msg::Scroll {
        column: 10,
        row: 25,
        delta: 3,
    });
    assert!(app.chats[0].auto_scroll());
}

#[test]
fn scroll_shortcuts_toggle_auto_scroll() {
    let mut app = test_app();
    app.active_chat().enable_auto_scroll();
    app.update(Msg::Key(kb::SCROLL_TOP.to_key_event()));
    assert!(!app.chats[0].auto_scroll());
    app.update(Msg::Key(kb::SCROLL_BOTTOM.to_key_event()));
    assert!(app.chats[0].auto_scroll());
}

#[test]
fn mouse_drag_updates_selection() {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 80, 20));
    app.active_chat().scroll_to_top();

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 5, 5));
    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 20, 10));

    let state = app.selection_state.as_ref().unwrap();
    let (_, end) = state.sel().normalized();
    assert_eq!(end.row, 10);
    assert_eq!(end.col, 20);
}

#[test]
fn mouse_drag_clamps_to_area() {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 80, 20));
    app.active_chat().scroll_to_top();

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 5, 5));
    app.update(mouse_event(
        MouseEventKind::Drag(MouseButton::Left),
        100,
        50,
    ));

    let state = app.selection_state.as_ref().unwrap();
    let (_, end) = state.sel().normalized();
    assert_eq!(end.col, 79);
    assert_eq!(end.row, 19, "clamped to area bottom");
    assert!(
        app.selection_state.as_ref().unwrap().is_edge_scrolling(),
        "outside area triggers edge scroll"
    );
}

#[test_case(Rect::new(0, 2, 80, 20), (10, 12), (10, 1),  Some(EDGE_SCROLL_LINES)  ; "top_edge")]
#[test_case(Rect::new(0, 2, 80, 20), (10, 10), (10, 22), Some(-EDGE_SCROLL_LINES) ; "bottom_edge")]
#[test_case(Rect::new(0, 2, 80, 20), (10, 10), (20, 15), None                     ; "middle_no_scroll")]
fn edge_scroll_direction(zone: Rect, down: (u16, u16), drag: (u16, u16), expected: Option<i32>) {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, zone);
    app.active_chat().scroll_to_top();

    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Left),
        down.0,
        down.1,
    ));
    app.update(mouse_event(
        MouseEventKind::Drag(MouseButton::Left),
        drag.0,
        drag.1,
    ));

    let state = app.selection_state.as_ref().unwrap();
    let edge_dir = match state {
        SelectionState::Dragging { edge_scroll, .. } => edge_scroll.as_ref().map(|es| es.dir),
        _ => None,
    };
    assert_eq!(edge_dir, expected);
}

#[test]
fn mouse_up_clears_edge_scroll() {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 2, 80, 20));
    app.active_chat().scroll_to_top();

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 10, 10));
    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 10, 1));
    assert!(app.selection_state.as_ref().unwrap().is_edge_scrolling());

    app.update(mouse_event(MouseEventKind::Up(MouseButton::Left), 10, 1));
    let state = app.selection_state.as_ref().unwrap();
    assert!(state.is_pending_copy());
}

#[test]
fn double_esc_cancels_flushes_and_fails_tools() {
    let mut app = streaming_app();
    app.update(agent_msg(AgentEvent::TextDelta {
        text: "partial".into(),
    }));
    app.update(agent_msg(AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: "t1".into(),
        tool: "bash".into(),
        summary: "running".into(),
        annotation: None,
        input: None,
        raw_input: None,
        output: None,
        render_header: None,
    }))));

    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(actions.is_empty());

    app.last_esc = Some(Instant::now());
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(matches!(&actions[0], Action::CancelAgent { .. }));
    assert_eq!(app.status, Status::Idle);
    assert_eq!(app.chats[0].in_progress_count(), 0);
}

#[test]
fn double_esc_idle_opens_rewind_picker() {
    let mut app = test_app();
    bind_history_actor(&mut app, vec![Message::user("hello".into())]);

    app.last_esc = Some(Instant::now());
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(app.rewind_picker.is_open());
}

#[test]
fn double_esc_idle_no_user_turns_flashes_error() {
    let mut app = test_app();
    app.last_esc = Some(Instant::now());
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(!app.rewind_picker.is_open());
}

#[test]
fn ctrl_c_while_streaming_cancels_instead_of_quitting() {
    let mut app = streaming_app();
    let cancelled_run = app.run_id;

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));
    assert!(matches!(
        &actions[0],
        Action::CancelAgent { turn_id: Some(_), run_id } if *run_id == cancelled_run
    ));
    assert!(app.run_id > cancelled_run);
    assert_eq!(app.status, Status::Idle);
    assert_ne!(app.exit_request, ExitRequest::Success);
}

/// The whole point of issue 778: a settled session paints nothing at all. Any
/// poller that starts reporting a change on every tick trips this.
#[test]
fn settled_app_owes_no_frame_and_does_not_animate() {
    let mut app = app_without_splash();

    assert_eq!(app.cadence(), Cadence::IDLE);
    assert_eq!(app.tick(), Dirty::NO, "{QUIET}");
}

/// Nothing wakes the loop when a background thread drops an answer into a
/// shared slot, so `tick` has to go and look. The tick that first sees it is
/// also the only one allowed to claim a frame: `tick` runs on every turn of the
/// loop, so a poller that keeps saying yes never lets it sleep again.
#[track_caller]
fn assert_owes_one_frame(app: &mut App, arrival: impl FnOnce()) {
    assert_eq!(app.tick(), Dirty::NO, "{QUIET}");
    arrival();
    assert_eq!(app.tick(), Dirty::YES, "{OWED}");
    assert_eq!(app.tick(), Dirty::NO, "{QUIET}");
}

/// Providers publish their model list into a shared slot that wakes nothing,
/// so an open picker keeps showing the stale list until the user happens to
/// press a key.
#[test]
fn model_list_arriving_in_the_background_owes_a_frame() {
    let (mut app, models) = app_with_model_slot();
    app.execute_command(cmd("/model"), 0);
    assert!(app.model_picker.is_open());

    assert_owes_one_frame(&mut app, || {
        models.store(Some(Arc::new(vec![LATE_MODEL_SPEC.into()])));
    });
}

fn late_model_selection(thinking: ThinkingConfig, fast: bool) -> ModelSelection {
    ModelSelection {
        spec: Some(Arc::from(LATE_MODEL_SPEC)),
        thinking: Some(thinking),
        fast: Some(fast),
    }
}

/// `/model` with a spec and options, in brace or trailing `key=value` form,
/// or with options alone, emits one `SelectModel` carrying the whole
/// selection and leaves the picker closed. A spec absent from the
/// discovered list is still accepted.
#[test_case(
    format!("{LATE_MODEL_SPEC}{{thinking=high,fast=true}}"),
    late_model_selection(ThinkingConfig::Effort(Effort::High), true)
    ; "brace_options"
)]
#[test_case(
    format!("{LATE_MODEL_SPEC} thinking=high fast=true"),
    late_model_selection(ThinkingConfig::Effort(Effort::High), true)
    ; "trailing_options"
)]
#[test_case(
    "thinking=off fast=false".into(),
    ModelSelection { spec: None, thinking: Some(ThinkingConfig::Off), fast: Some(false) }
    ; "options_only"
)]
fn model_structured_selection_emits_one_atomic_action(args: String, expected: ModelSelection) {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![LATE_MODEL_SPEC.into()])));
    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args,
        },
        0,
    );
    assert!(
        matches!(actions.as_slice(), [Action::SelectModel(selection)] if selection == &expected)
    );
    assert!(!app.model_picker.is_open());
}

#[test_case("fast=true fast=false" ; "duplicate_option")]
#[test_case("fast=on" ; "invalid_fast")]
#[test_case("thinking=0" ; "zero_budget")]
#[test_case("unknown=value" ; "unknown_option")]
fn model_invalid_selection_emits_no_model_change(args: &str) {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![LATE_MODEL_SPEC.into()])));
    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: args.into(),
        },
        0,
    );
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::SelectModel(_) | Action::ChangeModel(_)))
    );
}

#[test]
fn model_arg_spec_emits_change_model() {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![LATE_MODEL_SPEC.into()])));

    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: LATE_MODEL_SPEC.into(),
        },
        0,
    );
    assert!(matches!(&actions[..], [Action::ChangeModel(spec)] if spec == LATE_MODEL_SPEC));
    assert!(!app.model_picker.is_open());

    // A spec not in the discovered list still emits ChangeModel.
    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: MODEL_SPEC_GPT9.into(),
        },
        0,
    );
    assert!(matches!(&actions[..], [Action::ChangeModel(spec)] if spec == MODEL_SPEC_GPT9));
    assert!(!app.model_picker.is_open());
}

#[test]
fn malformed_model_arg_flashes_invalid_model() {
    let (mut app, _models) = app_with_model_slot();
    let before = app.root_config().model.spec();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: "anthropic/".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert_eq!(app.root_config().model.spec(), before);
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        format!(
            "command failed: {}",
            makima_providers::ModelError::InvalidFormat
        )
    );
}

/// `/model` with no argument still opens the picker and refreshes the list.
#[test]
fn model_no_arg_opens_picker_and_refreshes() {
    let (mut app, _models) = app_with_model_slot();
    let actions = app.execute_command(cmd("/model"), 0);
    assert!(app.model_picker.is_open());
    assert!(matches!(&actions[..], [Action::RefreshModels]));
}

/// `/theme <name>` (exact) applies and persists the theme on a tempdir-backed
/// disk provider: generation bumps, the current name updates, the pick is
/// readable back from the `StateDir`, and the flash names the theme.
#[test]
fn theme_arg_exact_applies_and_persists() {
    let _guard = crate::theme::theme_test_guard();
    let (tmp, dir, _writer, mut app) = tempdir_app();
    app.theme_provider = Arc::new(crate::theme::DiskThemesProvider::new(
        Some(dir.clone()),
        tmp.path().to_path_buf(),
    ));
    let gen_before = app.theme_provider.generation();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/theme".into(),
            args: "zenburn".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert!(!app.theme_picker.is_open());
    assert!(app.theme_provider.generation() > gen_before);
    assert_eq!(app.theme_provider.current_theme_name(), "zenburn");
    assert_eq!(
        makima_storage::theme::read_theme_name(&dir).as_deref(),
        Some("zenburn")
    );
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        format!("{THEME_APPLIED_PREFIX}: zenburn")
    );
}

/// `/theme <fragment>` that fuzzy-resolves to a unique name applies it
/// (in-memory provider, no disk write).
#[test]
fn theme_arg_fuzzy_unique_applies() {
    let _guard = crate::theme::theme_test_guard();
    let mut app = test_app();
    let gen_before = app.theme_provider.generation();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/theme".into(),
            args: "toky".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert!(!app.theme_picker.is_open());
    assert!(app.theme_provider.generation() > gen_before);
    assert_eq!(app.theme_provider.current_theme_name(), "tokyonight");
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        format!("{THEME_APPLIED_PREFIX}: tokyonight")
    );
}

/// `/theme <name>` that is not in the catalog flashes `load`'s unknown-theme
/// error and changes nothing.
#[test]
fn theme_arg_unknown_flashes() {
    let _guard = crate::theme::theme_test_guard();
    let mut app = test_app();
    let gen_before = app.theme_provider.generation();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/theme".into(),
            args: "nonexistent".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert!(!app.theme_picker.is_open());
    assert_eq!(app.theme_provider.generation(), gen_before);
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        "command failed: theme is unknown or ambiguous: nonexistent"
    );
}

/// `/theme <fragment>` with 2+ fuzzy matches flashes and changes nothing.
#[test]
fn theme_arg_ambiguous_flashes() {
    let _guard = crate::theme::theme_test_guard();
    let mut app = test_app();
    let gen_before = app.theme_provider.generation();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/theme".into(),
            args: "catppuccin".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert!(!app.theme_picker.is_open());
    assert_eq!(app.theme_provider.generation(), gen_before);
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        "command failed: theme is unknown or ambiguous: catppuccin"
    );
}

/// Tool output streams into a subagent's chat while the parent chat is the one
/// on screen. Draining only the active chat would lose it, and the task picker
/// and a later switch would show nothing.
#[test]
fn tick_drains_live_bufs_of_background_chats() {
    let mut app = test_app();
    app.run_id = 1;
    app.update(agent_msg(tool_start(TASK_ID, TASK_SPAWN_TOOL_NAME)));
    app.update(subagent_msg(tool_start(SUB_TOOL_ID, "bash"), TASK_ID, None));
    let buf = Arc::new(makima_agent::SharedBuf::new());
    app.update(subagent_msg(
        AgentEvent::LiveToolBuf {
            id: SUB_TOOL_ID.into(),
            body: Arc::clone(&buf),
        },
        TASK_ID,
        None,
    ));
    assert_eq!(app.active_chat, 0, "the subagent's chat is the hidden one");

    assert_owes_one_frame(&mut app, || {
        buf.append(makima_agent::SnapshotLine::plain(TOOL_OUTPUT_LINE.into()));
    });
}

/// A plugin publishes hints from the Lua thread, and the loop never hears back
/// from that thread. The footer they draw in is on screen the whole time, so a
/// publish nobody polled for shows up on some later, unrelated keypress, or
/// never.
#[test]
fn status_hints_published_by_a_plugin_reach_the_screen() {
    let (mut app, plugin) = app_with_hints();
    plugin.publish(vec![(
        Arc::from(HINT_PLUGIN),
        vec![(HINT_TEXT.into(), HINT_STYLE.into())],
    )]);

    assert!(
        !rendered(&mut app).contains(HINT_TEXT),
        "a hint no poller has seen must not be on screen"
    );
    assert_eq!(app.tick(), Dirty::YES, "{OWED}");
    assert!(rendered(&mut app).contains(HINT_TEXT));

    plugin.publish(vec![]);
    assert_eq!(app.tick(), Dirty::YES, "{OWED}");
    assert!(!rendered(&mut app).contains(HINT_TEXT));
}

const MIDDLE_ORIGIN: u16 = 10;
const MIDDLE_COLUMN: u16 = 4;
const MIDDLE_TEST_AREA: Rect = Rect::new(0, 0, 80, 24);

fn activate_middle_scroll(app: &mut App) {
    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Middle),
        MIDDLE_COLUMN,
        MIDDLE_ORIGIN,
    ));
}

fn middle_scroll_transcript() -> App {
    let mut app = app_without_splash();
    for i in 0..100 {
        app.active_chat().push(DisplayMessage::new(
            DisplayRole::User,
            format!("transcript row {i}"),
        ));
    }
    rendered(&mut app);
    app
}

#[test_case(SelectionZone::Messages, true; "messages")]
#[test_case(SelectionZone::Input, false; "input")]
#[test_case(SelectionZone::Overlay, false; "overlay")]
fn middle_scroll_activation_targets(zone: SelectionZone, expected: bool) {
    let mut app = app_without_splash();
    set_zone(&mut app, zone, MIDDLE_TEST_AREA);
    activate_middle_scroll(&mut app);
    assert_eq!(app.middle_scroll.is_some(), expected);
    app.update(mouse_event(
        MouseEventKind::Up(MouseButton::Middle),
        MIDDLE_COLUMN,
        MIDDLE_ORIGIN,
    ));
    assert_eq!(app.middle_scroll.is_some(), expected);
    activate_middle_scroll(&mut app);
    assert!(app.middle_scroll.is_none());
}

#[test_case(-10, 100, 5; "up")]
#[test_case(10, 100, -5; "down")]
#[test_case(5, 100, -1; "moderate_speed")]
#[test_case(17, 100, -12; "cap_threshold")]
#[test_case(-1, 100, 0; "upper_dead_zone")]
#[test_case(1, 100, 0; "lower_dead_zone")]
#[test_case(0, 100, 0; "anchor")]
#[test_case(31, 100, -12; "speed_cap")]
#[test_case(100, 10000, -12; "stall_clamp")]
#[test_case(6, 25, 0; "fractional_step")]
fn middle_scroll_motion_rates(displacement: i32, millis: u64, expected: i32) {
    let mut app = app_without_splash();
    set_zone(&mut app, SelectionZone::Messages, MIDDLE_TEST_AREA);
    activate_middle_scroll(&mut app);
    let now = Instant::now();
    let state = app.middle_scroll.as_mut().unwrap();
    state.move_to((i32::from(MIDDLE_ORIGIN) + displacement) as u16, now);
    assert_eq!(state.delta(now + Duration::from_millis(millis)), expected);
}

#[test_case(2, -2; "fine_control")]
#[test_case(5, -16; "moderate_speed")]
#[test_case(10, -54; "fast_travel")]
#[test_case(17, -120; "capped_speed")]
fn middle_scroll_power_curve(displacement: u16, expected: i32) {
    const TICK_COUNT: u32 = 10;
    const TICK_INTERVAL: Duration = Duration::from_millis(100);
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    let now = Instant::now();
    let state = app.middle_scroll.as_mut().unwrap();
    state.move_to(MIDDLE_ORIGIN + displacement, now);
    let delta: i32 = (1..=TICK_COUNT)
        .map(|step| state.delta(now + TICK_INTERVAL * step))
        .sum();
    assert_eq!(delta, expected);
}

#[test]
fn middle_scroll_fractional_direction_and_horizontal_motion() {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    let now = Instant::now();
    let state = app.middle_scroll.as_mut().unwrap();
    state.move_to(MIDDLE_ORIGIN + 6, now);
    assert_eq!(state.delta(now + Duration::from_millis(25)), 0);
    assert_eq!(state.delta(now + Duration::from_millis(50)), -1);
    state.move_to(MIDDLE_ORIGIN - 6, now + Duration::from_millis(50));
    assert_eq!(state.delta(now + Duration::from_millis(75)), 0);
    state.move_to(MIDDLE_ORIGIN, now + Duration::from_millis(75));
    state.move_to(MIDDLE_ORIGIN - 6, now + Duration::from_millis(100));
    assert_eq!(state.delta(now + Duration::from_millis(125)), 0);
    app.update(mouse_event(MouseEventKind::Moved, u16::MAX, MIDDLE_ORIGIN));
    let state = app.middle_scroll.as_mut().unwrap();
    assert_eq!(state.origin.x, MIDDLE_COLUMN);
    assert_eq!(state.delta(now + Duration::from_secs(1)), 0);
}

#[test_case(0; "outside")]
#[test_case(1; "modal")]
#[test_case(2; "dragging")]
#[test_case(3; "pending_copy")]
fn middle_scroll_activation_rejected(cause: u8) {
    let mut app = middle_scroll_transcript();
    match cause {
        0 => app.zones = ZoneRegistry::new(),
        1 => open_help(&mut app),
        2 => {
            app.update(mouse_event(
                MouseEventKind::Down(MouseButton::Left),
                MIDDLE_COLUMN,
                MIDDLE_ORIGIN,
            ));
        }
        _ => make_pending_copy(&mut app),
    }
    activate_middle_scroll(&mut app);
    assert!(app.middle_scroll.is_none());
}

#[test]
fn middle_scroll_stationary_ticks_render() {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    let now = Instant::now();
    app.middle_scroll.as_mut().unwrap().move_to(0, now);
    let before = rendered(&mut app);
    for step in 1..=4 {
        assert_eq!(
            app.tick_middle_scroll_at(now + Duration::from_millis(step * 100)),
            Dirty::YES
        );
    }
    assert_ne!(rendered(&mut app), before);
}

#[test]
fn middle_scroll_anchor_render() {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    let rows = rendered_rows(&mut app, 80, 24);
    assert_eq!(
        rows[usize::from(MIDDLE_ORIGIN)]
            .chars()
            .nth(usize::from(MIDDLE_COLUMN)),
        Some('+')
    );
    activate_middle_scroll(&mut app);
    let rows = rendered_rows(&mut app, 80, 24);
    assert_ne!(
        rows[usize::from(MIDDLE_ORIGIN)]
            .chars()
            .nth(usize::from(MIDDLE_COLUMN)),
        Some('+')
    );
}

#[test_case(0; "escape")]
#[test_case(1; "ordinary_key")]
#[test_case(2; "release")]
#[test_case(3; "paste")]
#[test_case(4; "wheel")]
#[test_case(5; "left_button")]
#[test_case(6; "right_button")]
fn middle_scroll_input_cancellation(input: u8) {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    let msg = match input {
        0 => Msg::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
        1 => Msg::Key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE)),
        2 => Msg::Key(KeyEvent::new_with_kind(
            KeyCode::Esc,
            KeyModifiers::NONE,
            KeyEventKind::Release,
        )),
        3 => Msg::Paste("paste".into()),
        4 => Msg::Scroll {
            column: MIDDLE_COLUMN,
            row: MIDDLE_ORIGIN,
            delta: 1,
        },
        5 => mouse_event(
            MouseEventKind::Down(MouseButton::Left),
            MIDDLE_COLUMN,
            MIDDLE_ORIGIN,
        ),
        _ => mouse_event(
            MouseEventKind::Down(MouseButton::Right),
            MIDDLE_COLUMN,
            MIDDLE_ORIGIN,
        ),
    };
    let actions = app.update(msg);
    assert_eq!(app.middle_scroll.is_some(), input == 2);
    if input == 0 {
        assert!(actions.is_empty());
        assert!(app.last_esc.is_none());
    }
    if input == 1 {
        assert_eq!(app.input_box.buffer.value(), "x");
    }
    if input == 3 {
        assert_eq!(app.input_box.buffer.value(), "paste");
    }
    if input == 5 {
        assert!(app.selection_state.is_some());
    }
}

#[test_case(0; "overlay")]
#[test_case(1; "geometry")]
#[test_case(2; "chat_identity")]
#[test_case(3; "reset")]
#[test_case(4; "preview")]
fn middle_scroll_target_invalidation(cause: u8) {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    match cause {
        0 => open_help(&mut app),
        1 => {
            app.zones = ZoneRegistry::new();
            set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 40, 10));
        }
        2 => app.chats[0].subagent_id = Some(TASK_ID.into()),
        3 => app.reset_ui_chrome(),
        _ => app.task_picker.open(vec![], TASK_ID),
    }
    let _ = app.tick_middle_scroll_at(Instant::now());
    assert!(app.middle_scroll.is_none());
    app.close_all_overlays();
    assert_eq!(
        app.tick_middle_scroll_at(Instant::now() + Duration::from_secs(1)),
        Dirty::NO
    );
}

#[test]
fn middle_scroll_boundaries_and_reversal() {
    let mut app = middle_scroll_transcript();
    let bottom = app.chats[0].scroll_top();
    activate_middle_scroll(&mut app);
    let now = Instant::now();
    app.middle_scroll.as_mut().unwrap().move_to(100, now);
    assert_eq!(
        app.tick_middle_scroll_at(now + Duration::from_millis(100)),
        Dirty::NO
    );
    app.middle_scroll
        .as_mut()
        .unwrap()
        .move_to(0, now + Duration::from_millis(100));
    assert_eq!(
        app.tick_middle_scroll_at(now + Duration::from_millis(200)),
        Dirty::YES
    );
    assert_eq!(app.chats[0].scroll_top(), bottom - 5);
    app.chats[0].scroll_to_top();
    assert_eq!(
        app.tick_middle_scroll_at(now + Duration::from_millis(300)),
        Dirty::NO
    );
    app.middle_scroll
        .as_mut()
        .unwrap()
        .move_to(20, now + Duration::from_millis(300));
    assert_eq!(
        app.tick_middle_scroll_at(now + Duration::from_millis(400)),
        Dirty::YES
    );
    assert_eq!(app.chats[0].scroll_top(), 5);
}

#[test_case(false; "above_bottom")]
#[test_case(true; "at_bottom")]
fn middle_scroll_streaming_follow_render(at_bottom: bool) {
    let mut app = middle_scroll_transcript();
    if !at_bottom {
        app.chats[0].scroll(10);
    }
    activate_middle_scroll(&mut app);
    let before = app.chats[0].scroll_top();
    app.chats[0].push(DisplayMessage::new(DisplayRole::User, "new output".into()));
    rendered(&mut app);
    assert_eq!(app.chats[0].scroll_top(), before);
    let _ = app.cancel_middle_scroll();
    rendered(&mut app);
    assert_eq!(app.chats[0].scroll_top(), before);
    app.chats[0].scroll(-i32::from(u16::MAX));
    rendered(&mut app);
    assert!(app.chats[0].auto_scroll());
    let bottom = app.chats[0].scroll_top();
    app.chats[0].push(DisplayMessage::new(
        DisplayRole::User,
        "later output".into(),
    ));
    rendered(&mut app);
    assert!(app.chats[0].scroll_top() > bottom);
}

#[test]
fn middle_scroll_async_overlay_prevents_final_motion() {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    let now = Instant::now();
    app.middle_scroll.as_mut().unwrap().move_to(0, now);
    let before = app.chats[0].scroll_top();
    app.last_input = Some(now);
    assert!(app.begin_input_demand(bash_perm_demand(PERM_SCOPE)));
    app.last_input = Some(now - IDLE_AGE);
    let _ = app.tick_at(now + Duration::from_millis(100));
    assert!(app.permission_active());
    assert!(app.middle_scroll.is_none());
    assert_eq!(app.chats[0].scroll_top(), before);
}

#[test]
fn middle_scroll_chat_switch_and_render_resize() {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    app.update(subagent_msg(
        AgentEvent::TextDelta {
            text: TASK_ID.into(),
        },
        TASK_ID,
        Some(TASK_ID),
    ));
    app.active_chat = 1;
    let _ = app.tick_middle_scroll_at(Instant::now());
    assert!(app.middle_scroll.is_none());
    app.active_chat = 0;
    assert_eq!(app.tick_middle_scroll_at(Instant::now()), Dirty::NO);
    activate_middle_scroll(&mut app);
    rendered_rows(&mut app, 60, 20);
    assert!(app.middle_scroll.is_none());
}

#[test]
fn middle_scroll_selection_copy_render() {
    let mut app = middle_scroll_transcript();
    activate_middle_scroll(&mut app);
    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Left),
        MIDDLE_COLUMN,
        MIDDLE_ORIGIN,
    ));
    app.update(mouse_event(
        MouseEventKind::Drag(MouseButton::Left),
        MIDDLE_COLUMN + 8,
        MIDDLE_ORIGIN,
    ));
    app.update(mouse_event(
        MouseEventKind::Up(MouseButton::Left),
        MIDDLE_COLUMN + 8,
        MIDDLE_ORIGIN,
    ));
    assert!(
        app.selection_state
            .as_ref()
            .is_some_and(SelectionState::is_pending_copy)
    );
    activate_middle_scroll(&mut app);
    assert!(app.middle_scroll.is_none());
    let rows = rendered_rows(&mut app, 80, 24);
    assert_ne!(
        rows[usize::from(MIDDLE_ORIGIN)]
            .chars()
            .nth(usize::from(MIDDLE_COLUMN)),
        Some('+')
    );
}

#[test]
fn middle_scroll_cadence_restores_baseline() {
    let mut app = app_without_splash();
    set_zone(&mut app, SelectionZone::Messages, MIDDLE_TEST_AREA);
    let baseline = app.cadence();
    activate_middle_scroll(&mut app);
    assert_eq!(
        app.cadence(),
        Cadence::any([baseline, Cadence::after(mouse::MIDDLE_SCROLL_INTERVAL)])
    );
    assert_eq!(app.cancel_middle_scroll(), Dirty::YES);
    assert_eq!(app.cancel_middle_scroll(), Dirty::NO);
    assert_eq!(app.cadence(), baseline);
}

fn rendered(app: &mut App) -> String {
    let backend = ratatui::backend::TestBackend::new(80, 24);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal.draw(|frame| app.view(frame)).unwrap();
    buffer_text(terminal.backend().buffer())
}

/// When the picker gives up on a directory it cannot list, the flash is the
/// only trace the user gets. Forwarding it moved from `view` into `tick`, and
/// dropping that hop closes the picker with no explanation at all. The loop
/// ends the moment the walker thread answers; the deadline only turns a
/// missing hop into a failure instead of a hang.
#[test]
fn tick_forwards_the_file_picker_flash_to_the_status_bar() {
    let tmp = TempDir::new().unwrap();
    let mut app = test_app();
    app.file_picker
        .open(&tmp.path().join(MISSING_DIR).to_string_lossy());

    let deadline = Instant::now() + WALK_TIMEOUT;
    while app.status_bar.flash_text().is_none() {
        assert!(Instant::now() < deadline, "the picker never flashed");
        let _ = app.tick();
        std::thread::yield_now();
    }

    assert_eq!(app.status_bar.flash_text(), Some(EMPTY_DIR_MSG));
    assert!(!app.file_picker.is_open());
}

/// A waiting tool draws a spinner, which changes once per `SPINNER_FRAME`.
/// Claiming `SMOOTH` here paints five identical frames for every visible one,
/// for as long as the tool runs.
#[test]
fn waiting_tool_animates_at_the_spinner_rate() {
    let mut app = app_without_splash();
    app.update(agent_msg(tool_start("t1", "bash")));

    assert_eq!(app.cadence(), Cadence::SPINNER);
}

/// The bar spins for a whole streaming turn, again while a restore is in
/// flight, and once more for a retry countdown. The old `is_animating` only
/// knew about the restore, so the other two froze mid turn.
#[test_case(Status::Streaming, false, false => Cadence::SPINNER ; "streaming_turn")]
#[test_case(Status::Idle, true, false => Cadence::SPINNER ; "restoring_session")]
#[test_case(Status::Idle, false, true => Cadence::SPINNER ; "retry_countdown")]
#[test_case(Status::Idle, false, false => Cadence::IDLE ; "nothing_in_flight")]
fn status_bar_motion_reaches_app_cadence(
    status: Status,
    restoring: bool,
    retrying: bool,
) -> Cadence {
    let mut app = app_without_splash();
    app.status = status;
    app.restoring.store(restoring, Ordering::Relaxed);
    if retrying {
        app.chats[0].retry_info = Some(RetryInfo {
            attempt: 1,
            message: RETRY_MESSAGE.into(),
            deadline: Instant::now() + RETRY_DELAY,
        });
    }
    app.cadence()
}

/// `App::cadence` asks `overlays()` as a group, so a moving overlay only
/// reaches the loop through that fold.
#[test]
fn open_overlay_motion_reaches_app_cadence() {
    let mut app = app_without_splash();
    assert_eq!(app.cadence(), Cadence::IDLE);

    let (event_tx, _event_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (_cmd_tx, cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    app.float_mgr.open(
        Arc::new(makima_agent::SharedBuf::new()),
        makima_lua::FloatConfig::default(),
        true,
        event_tx,
        cmd_rx,
    );
    assert_eq!(
        app.cadence(),
        Cadence::SPINNER,
        "an open float's spinners only turn if the app keeps painting"
    );

    app.close_all_overlays();
    assert_eq!(app.cadence(), Cadence::IDLE);
}

#[test]
fn edge_scroll_makes_app_animating() {
    let mut app = app_without_splash();
    assert_eq!(app.cadence(), Cadence::IDLE);
    let zone = Rect::new(0, 2, 80, 20);
    set_zone(&mut app, SelectionZone::Messages, zone);
    app.active_chat().scroll_to_top();
    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 10, 10));
    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 10, 1));
    assert_eq!(
        app.cadence(),
        Cadence::SMOOTH,
        "an edge-scrolling drag advances on a timer, with no events to wake us"
    );
}

#[test]
fn empty_click_clears_selection() {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 80, 20));

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 5, 5));
    app.update(mouse_event(MouseEventKind::Up(MouseButton::Left), 5, 5));
    assert!(app.selection_state.is_none());
}

fn make_pending_copy(app: &mut App) {
    set_zone(app, SelectionZone::Messages, Rect::new(0, 0, 80, 20));
    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 5, 5));
    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 10, 10));
    app.update(mouse_event(MouseEventKind::Up(MouseButton::Left), 10, 10));
}

const DRAG_ROW: u16 = 5;
const DRAG_COL: u16 = 5;
const SCROLL_LINES: i32 = 3;
const DRAG_ZONE_AREA: Rect = Rect {
    x: 0,
    y: 0,
    width: 80,
    height: 10,
};
const OTHER_ZONE_AREA: Rect = Rect {
    x: 0,
    y: DRAG_ZONE_AREA.height,
    width: 80,
    height: 10,
};

fn send_key(app: &mut App) {
    app.update(Msg::Key(key(KeyCode::Char('a'))));
}

fn send_scroll_outside_drag_zone(app: &mut App) {
    app.update(Msg::Scroll {
        column: DRAG_COL,
        row: OTHER_ZONE_AREA.y + 1,
        delta: SCROLL_LINES,
    });
}

#[test_case(send_key as fn(&mut App) ; "key")]
#[test_case(send_scroll_outside_drag_zone as fn(&mut App) ; "scroll_outside_drag_zone")]
fn interrupt_clears_dragging_but_preserves_pending_copy(interrupt: fn(&mut App)) {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, DRAG_ZONE_AREA);
    set_zone(&mut app, SelectionZone::Input, OTHER_ZONE_AREA);
    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Left),
        DRAG_COL,
        DRAG_ROW,
    ));
    interrupt(&mut app);
    assert!(app.selection_state.is_none(), "clears dragging");

    make_pending_copy(&mut app);
    interrupt(&mut app);
    assert!(
        app.selection_state.as_ref().unwrap().is_pending_copy(),
        "preserves pending copy"
    );
}

#[test]
fn scroll_preserves_dragging_and_updates_cursor() {
    let mut app = test_app();
    for i in 0..50 {
        app.active_chat()
            .push(DisplayMessage::new(DisplayRole::User, format!("line {i}")));
    }

    let area = Rect::new(0, 0, 80, 20);
    set_zone(&mut app, SelectionZone::Messages, area);

    let backend = ratatui::backend::TestBackend::new(area.width, area.height);
    let mut terminal = ratatui::Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| {
            app.active_chat().view(frame, area, false, true);
        })
        .unwrap();

    let max_scroll = app.active_chat().scroll_top();
    assert!(
        max_scroll > 0,
        "scroll_top should be non-zero after rendering scrollable content"
    );

    app.update(Msg::Scroll {
        column: DRAG_COL,
        row: DRAG_ROW,
        delta: SCROLL_LINES,
    });
    let scroll_before = app.active_chat().scroll_top();
    assert!(
        scroll_before < max_scroll,
        "scroll up should move scroll_top away from max_scroll"
    );

    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Left),
        DRAG_COL,
        DRAG_ROW,
    ));

    app.update(Msg::Scroll {
        column: DRAG_COL,
        row: DRAG_ROW,
        delta: -SCROLL_LINES,
    });

    assert!(
        matches!(
            app.selection_state.as_ref().unwrap(),
            SelectionState::Dragging { .. }
        ),
        "scroll keeps dragging"
    );

    let (start, end) = app.selection_state.as_ref().unwrap().sel().normalized();
    let anchor_row = scroll_before as u32 + DRAG_ROW as u32;
    assert_eq!(start.row, anchor_row, "anchor keeps its doc row");
    assert_eq!(
        end.row,
        anchor_row + SCROLL_LINES as u32,
        "cursor re-projects by the scrolled lines"
    );
    assert_eq!(start.col, DRAG_COL, "anchor column is unchanged");
    assert_eq!(end.col, DRAG_COL, "cursor column is unchanged");

    make_pending_copy(&mut app);
    app.update(Msg::Scroll {
        column: DRAG_COL,
        row: DRAG_ROW,
        delta: -SCROLL_LINES,
    });
    assert!(
        app.selection_state.as_ref().unwrap().is_pending_copy(),
        "scroll preserves pending copy"
    );
}

#[test]
fn new_mouse_down_replaces_pending_copy_with_dragging() {
    let mut app = test_app();
    make_pending_copy(&mut app);

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 15, 15));
    assert!(matches!(
        app.selection_state.as_ref().unwrap(),
        SelectionState::Dragging { .. }
    ));
}

#[test]
fn pending_copy_ignores_drag_and_tick() {
    let mut app = test_app();
    make_pending_copy(&mut app);

    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 50, 50));
    assert!(app.selection_state.as_ref().unwrap().is_pending_copy());

    let _ = app.tick_edge_scroll();
    assert!(app.selection_state.as_ref().unwrap().is_pending_copy());
}

#[test]
fn pending_copy_not_animating() {
    let mut app = app_without_splash();
    make_pending_copy(&mut app);
    assert_eq!(app.cadence(), Cadence::IDLE);
}

#[test]
fn edge_scroll_direction_switches_on_drag_reversal() {
    let mut app = test_app();
    let zone = Rect::new(0, 5, 80, 10);
    set_zone(&mut app, SelectionZone::Messages, zone);
    app.active_chat().scroll_to_top();

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 10, 8));
    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 10, 4));

    if let Some(SelectionState::Dragging { edge_scroll, .. }) = &app.selection_state {
        assert!(
            edge_scroll.as_ref().unwrap().dir > 0,
            "scrolling up (positive dir)"
        );
    } else {
        panic!("expected Dragging");
    }

    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 10, 16));
    if let Some(SelectionState::Dragging { edge_scroll, .. }) = &app.selection_state {
        assert!(
            edge_scroll.as_ref().unwrap().dir < 0,
            "scrolling down after reversal"
        );
    } else {
        panic!("expected Dragging");
    }
}

#[test]
fn drag_back_into_area_clears_edge_scroll() {
    let mut app = test_app();
    let zone = Rect::new(0, 5, 80, 10);
    set_zone(&mut app, SelectionZone::Messages, zone);
    app.active_chat().scroll_to_top();

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 10, 8));
    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 10, 4));
    assert!(app.selection_state.as_ref().unwrap().is_edge_scrolling());

    app.update(mouse_event(MouseEventKind::Drag(MouseButton::Left), 10, 10));
    assert!(
        !app.selection_state.as_ref().unwrap().is_edge_scrolling(),
        "dragging back into area must stop edge scroll"
    );
}

#[test]
fn mouse_down_outside_all_zones_ignored() {
    let mut app = test_app();
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 40, 10));

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 50, 15));
    assert!(
        app.selection_state.is_none(),
        "click outside zones must not create selection"
    );
}

#[test_case(true  ; "non_empty")]
#[test_case(false ; "empty")]
fn queue_command_sets_focus(has_queue: bool) {
    let mut app = if has_queue {
        app_with_queued_message()
    } else {
        test_app()
    };
    app.execute_command(cmd("/queue"), 0);
    assert_eq!(app.queue.focus().is_some(), has_queue);
}

#[test]
fn queue_boundary_clamps() {
    let mut app = app_with_queued_message();
    app.queue_and_notify(queued_msg("second"));
    app.queue.set_focus_at(0);
    app.update(Msg::Key(key(KeyCode::Up)));
    assert_eq!(app.queue.focus(), Some(0), "up at top clamps");
    app.queue.set_focus_at(1);
    app.update(Msg::Key(key(KeyCode::Down)));
    assert_eq!(app.queue.focus(), Some(1), "down at bottom clamps");
}

#[test]
fn queue_enter_removes_selected() {
    let mut app = app_with_queued_message();
    app.queue_and_notify(queued_msg("second"));
    app.queue.set_focus_at(0);

    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.queue.len(), 1);
    assert_eq!(app.queue.panel_entries()[0].text, "second");
    assert_eq!(app.queue.focus(), Some(0));
}

#[test]
fn queue_enter_deletes_last_unfocuses() {
    let mut app = app_with_queued_message();
    app.queue.set_focus_at(0);

    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.queue.is_empty());
    assert!(app.queue.focus().is_none());
}

#[test]
fn queue_esc_unfocuses_without_removing() {
    let mut app = app_with_queued_message();
    app.queue.set_focus_at(0);

    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(app.queue.focus().is_none());
    assert_eq!(app.queue.len(), 1);
}

#[test]
fn ctrl_q_pops_newest_visible_item() {
    let mut app = app_with_queued_message();
    app.queue_and_notify(queued_msg("second"));
    app.queue_and_notify(queued_msg("third"));
    app.queue.set_focus_at(1);

    app.update(Msg::Key(kb::POP_QUEUE.to_key_event()));

    assert_eq!(app.queue.len(), 2);
    assert_eq!(
        app.queue
            .panel_entries()
            .iter()
            .map(|entry| entry.text.as_ref())
            .collect::<Vec<_>>(),
        ["queued", "second"]
    );
    assert_eq!(
        app.queue.focus(),
        Some(1),
        "focus follows the retained newest item"
    );
}

#[test_case(app_with_queued_message, cancel_app ; "cancel")]
#[test_case(idle_app_with_queued_message, error_app ; "error")]
fn clears_queue_focus_on_terminate(setup: fn() -> App, terminate: fn(&mut App)) {
    let mut app = setup();
    app.queue.set_focus_at(0);
    terminate(&mut app);
    assert!(app.queue.focus().is_none());
}

#[test]
fn stale_events_ignored_after_run_id_increment() {
    let mut app = streaming_app();

    cancel_app(&mut app);
    let current_run = app.run_id;
    type_and_submit(&mut app, "new prompt");
    let active_run = app.run_id;

    app.update(agent_msg_with_run_id(
        AgentEvent::TextDelta {
            text: "stale text".into(),
        },
        current_run,
    ));
    assert_eq!(app.chats[0].last_message_text(), "new prompt");

    app.update(agent_msg_with_run_id(
        AgentEvent::TextDelta {
            text: "new text".into(),
        },
        active_run,
    ));
    app.chats[0].flush();
    assert_eq!(app.chats[0].last_message_text(), "new text");
}

#[test]
fn stale_done_does_not_drain_queue() {
    let mut app = streaming_app();

    cancel_app(&mut app);
    app.queue_and_notify(queued_msg("next"));

    app.update(agent_msg_with_run_id(done(), 1));
    assert_eq!(app.queue.len(), 1);
    assert_eq!(app.status, Status::Idle);
}

#[test]
fn mouse_down_in_input_creates_input_zone_selection() {
    let mut app = test_app();
    let input = Rect::new(0, 15, 80, 5);
    set_zone(&mut app, SelectionZone::Messages, Rect::new(0, 0, 80, 15));
    set_zone(&mut app, SelectionZone::Input, input);

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 10, 16));
    let state = app.selection_state.as_ref().unwrap();
    assert_eq!(state.sel().zone, SelectionZone::Input);
    assert_eq!(state.sel().area, input);
}

#[test]
fn resolve_or_create_chat_sets_model_id_and_annotation() {
    let mut app = streaming_app();
    app.update(agent_msg(AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: TASK_ID.into(),
        tool: TASK_SPAWN_TOOL_NAME.into(),
        summary: "research".into(),
        annotation: None,
        input: None,
        raw_input: None,
        output: None,
        render_header: None,
    }))));

    app.update(subagent_msg_with_model(
        AgentEvent::TextDelta { text: "hi".into() },
        TASK_ID,
        "research",
        "anthropic/claude-sonnet-4-20250514",
    ));

    assert_eq!(app.chats.len(), 2);
    assert_eq!(
        app.chats[1].model_id.as_deref(),
        Some("anthropic/claude-sonnet-4-20250514")
    );
}

#[test]
fn help_toggles_modal() {
    let mut app = test_app();
    assert!(!app.help_modal.is_open());
    app.update(Msg::Key(kb::HELP.to_key_event()));
    assert!(app.help_modal.is_open());
    app.execute_command(cmd("/help"), 0);
    assert!(!app.help_modal.is_open());
}

#[test]
fn help_modal_consumes_keys_and_esc_closes() {
    let mut app = test_app();
    app.update(Msg::Key(kb::HELP.to_key_event()));

    app.update(Msg::Key(key(KeyCode::Char('h'))));
    app.update(Msg::Key(key(KeyCode::Char('i'))));
    assert_eq!(app.input_box.buffer.value(), "");

    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(!app.help_modal.is_open());
}

#[test_case(
    |_: &mut App| {},
    &[KeybindContext::General, KeybindContext::Editing],
    &[KeybindContext::Streaming]
    ; "idle"
)]
#[test_case(
    |app: &mut App| { *app = streaming_app(); },
    &[KeybindContext::General, KeybindContext::Streaming, KeybindContext::Editing],
    &[]
    ; "streaming"
)]
#[test_case(
    |app: &mut App| { set_root_mode(app, Mode::Plan); app.state.plan = PlanState::Drafting("test-plan.md".into()); app.plan_form.on_plan_ready(); },
    &[KeybindContext::FormInput],
    &[KeybindContext::Editing]
    ; "plan_form"
)]
#[test_case(
    |app: &mut App| { app.status = Status::Streaming; app.run_id = 1; app.queue_and_notify(queued_msg("q")); app.queue.set_focus_at(0); },
    &[KeybindContext::QueueFocus],
    &[KeybindContext::Editing]
    ; "queue_focus"
)]
#[test_case(
    |app: &mut App| { open_tasks_picker(app); },
    &[KeybindContext::TaskPicker],
    &[KeybindContext::Editing]
    ; "task_picker"
)]
#[test_case(
    |app: &mut App| {
        bind_history_actor(app, vec![Message::user("test".into())]);
        app.open_rewind_picker();
    },
    &[KeybindContext::RewindPicker],
    &[KeybindContext::Editing]
    ; "rewind_picker"
)]
fn active_contexts(setup: fn(&mut App), expected: &[KeybindContext], absent: &[KeybindContext]) {
    let mut app = test_app();
    setup(&mut app);
    let contexts = app.active_keybind_contexts();
    for ctx in expected {
        assert!(contexts.contains(ctx), "{ctx:?} should be present");
    }
    for ctx in absent {
        assert!(!contexts.contains(ctx), "{ctx:?} should be absent");
    }
}

#[test]
fn submit_exit_quits() {
    let mut app = test_app();
    let actions = app.handle_submit(Submission {
        text: "exit".into(),
        images: vec![],
    });
    assert_eq!(app.exit_request, ExitRequest::Success);
    assert!(matches!(actions.as_slice(), [Action::ManualExit]));
}

#[test]
fn session_has_content_covers_each_branch() {
    let mut session = AppSession::new("test-model", "/tmp/test");
    assert!(!session_has_content(&session));

    session.meta.input_draft = Some("draft".into());
    assert!(session_has_content(&session));
    session.meta.input_draft = None;

    session.meta.queued_messages = vec!["queued".into()];
    assert!(session_has_content(&session));
    session.meta.queued_messages.clear();

    session.meta.mode = Some(StoredMode::Plan);
    assert!(session_has_content(&session));
    session.meta.mode = Some(StoredMode::Build);

    session.push_message(Message::user("hello".into()));
    assert!(session_has_content(&session));
}

#[test]
fn checkpoint_syncs_ephemeral_content_into_meta() {
    let mut app = test_app();
    app.checkpoint();
    assert!(!session_has_content(&app.state.session));

    app.update(Msg::Key(key(KeyCode::Char('x'))));
    app.checkpoint();
    assert!(session_has_content(&app.state.session));

    app.update(Msg::Key(key(KeyCode::Backspace)));
    app.checkpoint();
    assert!(app.state.session.meta.input_draft.is_none());
    assert!(!session_has_content(&app.state.session));

    set_root_mode(&mut app, Mode::from_id("plan"));
    app.checkpoint();
    assert_eq!(app.state.session.meta.mode, Some(StoredMode::Plan));
    assert!(session_has_content(&app.state.session));

    let mut queued = app_with_queued_message();
    queued.checkpoint();
    let session = &queued.state.session;
    assert!(session.messages().is_empty());
    assert!(session.meta.input_draft.is_none());
    assert_eq!(session.meta.mode, Some(StoredMode::Build));
    assert_eq!(session.meta.queued_messages, vec!["queued".to_string()]);
    assert!(session_has_content(session));
}

#[test]
fn checkpoint_persists_observations_without_using_them_as_title() {
    let mut app = test_app();
    let initial_title = app.state.session.title.clone();
    let _history = attach_live_history(
        &mut app,
        vec![
            Message::observation("build failed".into()),
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "I will fix it".into(),
                }],
                ..Default::default()
            },
        ],
    );

    app.checkpoint();

    assert_eq!(app.state.session.messages().len(), 2);
    assert!(app.state.session.messages()[0].is_observation());
    assert_eq!(app.state.session.title, initial_title);
}

fn drain_writer(app: App, writer: Arc<StorageWriter>) {
    drop(app);
    Arc::try_unwrap(writer)
        .ok()
        .expect("app must hold the only other writer reference")
        .shutdown(WRITER_DRAIN_TIMEOUT);
}

#[test]
fn reload_persists_session_with_content_to_disk() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    app.state
        .session_mut()
        .push_message(Message::user("hello".into()));
    let actions = app.execute_command(cmd("/reload"), 0);
    assert_eq!(app.exit_request, ExitRequest::Reload);
    assert!(matches!(actions.as_slice(), [Action::ManualExit]));
    app.checkpoint();
    let id = app.state.session.id;
    drain_writer(app, writer);

    assert_eq!(AppSession::load(id, &dir).unwrap().messages().len(), 1);
}

#[test]
fn reload_leaves_empty_session_unpersisted_on_disk() {
    let (tmp, _dir, writer, mut app) = tempdir_app();
    app.execute_command(cmd("/reload"), 0);
    drain_writer(app, writer);

    let sessions_dir = tmp.path().join(makima_storage::sessions::SESSIONS_DIR);
    let entries = std::fs::read_dir(&sessions_dir)
        .map(|d| d.count())
        .unwrap_or(0);
    assert_eq!(entries, 0);
}

#[test]
fn restore_resumed_session_flushes_queued_messages_and_round_trips() {
    let mut app = test_app();
    app.state.session_mut().meta.queued_messages = vec!["q1".into(), "q2".into()];

    app.restore_resumed_session();
    assert_eq!(app.queue.text_messages(), ["q1", "q2"]);

    app.checkpoint();
    assert_eq!(app.state.session.meta.queued_messages, ["q1", "q2"]);
}

#[test]
fn apply_loaded_session_defers_queued_messages_until_flush() {
    let mut app = test_app();
    let mut session = AppSession::new("test-model", "/tmp/test");
    session.meta.queued_messages = vec!["deferred".into()];
    session.push_message(Message::user("hello".into()));

    let model = app.root_config().model.clone();
    app.apply_loaded_session(session, &model);

    assert!(app.queue.is_empty());
    assert_eq!(app.state.session.meta.queued_messages, ["deferred"]);
}

#[test]
fn loaded_session_restores_yolo() {
    let mut app = test_app();
    app.permissions.set_yolo(false);
    let mut session = AppSession::new("test-model", "/tmp/test");
    session.meta.yolo = Some(true);

    let model = app.root_config().model.clone();
    app.apply_loaded_session(session, &model);

    assert!(app.permissions.is_yolo());
}

#[test]
fn yolo_toggle() {
    let mut app = test_app();
    assert!(!app.permissions.is_yolo());
    app.execute_command(cmd("/yolo"), 0);
    assert!(app.permissions.is_yolo());
    let flash = app.status_bar.flash_text().unwrap();
    assert!(flash.contains("enabled"), "flash={flash:?}");
    app.execute_command(cmd("/yolo"), 0);
    assert!(!app.permissions.is_yolo());
    let flash = app.status_bar.flash_text().unwrap();
    assert!(flash.contains("disabled"), "flash={flash:?}");
}

#[test]
fn typed_slash_command_executes() {
    let mut app = test_app();
    let actions = type_and_submit(&mut app, "/help");
    assert!(actions.is_empty());
    assert!(app.help_modal.is_open());
}

const LUA_COMMAND_RAN: &str = "lua command with args must reach the plugin";
const LUA_COMMAND_NOT_SENT: &str = "lua command with args must not reach the model";

/// The palette hides a Lua command once the typed words pass its derived
/// positional bound, and a hidden command falls through to `handle_submit`,
/// so a multiword raw command must still be routed to its plugin.
#[test]
fn typed_lua_command_with_args_executes() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let (registry, _producer) = lua_registry(TestLuaCommand {
        handle: handle.clone(),
        name: Arc::from("/rename"),
        plugin: Arc::from("sessions"),
        max_args: None,
        completion: false,
    });
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );

    let actions = type_and_submit(&mut app, "/rename my title");

    assert!(actions.is_empty(), "{LUA_COMMAND_NOT_SENT}");
    assert!(probe.try_recv().is_some(), "{LUA_COMMAND_RAN}");
}

const COPY_TYPED_PLUGIN: &str = r#"
    local function destination_items(ctx)
        return {
            { label = "destination:" .. ctx.values.source, insertion = "destination folder/" },
        }
    end

    makima.api.register_command({
        name = "/copy",
        tui_only = false,
        arguments = {
            { name = "source", type = "file" },
            { name = "destination", type = "directory", completion = {
                mode = "replace",
                get_items = destination_items,
            } },
            { name = "policy", type = "enum", choices = { "skip", "overwrite" }, optional = true },
        },
        handler = function(opts)
            local policy = opts.values.policy or ""
            makima.ui.flash(opts.values.source .. "|" .. opts.values.destination .. "|" .. policy)
        end,
    })

    makima.api.register_command({
        name = "/copy-default",
        tui_only = false,
        arguments = {
            { name = "source", type = "file" },
            { name = "destination", type = "directory" },
        },
        handler = function(opts)
            makima.ui.flash("default|" .. opts.values.source .. "|" .. opts.values.destination)
        end,
    })

    makima.api.register_command({
        name = "/copy-disabled",
        tui_only = false,
        arguments = {
            { name = "source", type = "file" },
            { name = "destination", type = "directory", completion = false },
        },
        handler = function(opts)
            makima.ui.flash("disabled|" .. opts.values.source .. "|" .. opts.values.destination)
        end,
    })

    makima.api.register_command({
        name = "/copy-replace",
        tui_only = false,
        arguments = {
            { name = "source", type = "file" },
            { name = "destination", type = "directory", completion = {
                mode = "replace",
                items = { { label = "replacement", insertion = "replacement folder/" } },
            } },
        },
        handler = function(opts)
            makima.ui.flash("replace|" .. opts.values.source .. "|" .. opts.values.destination)
        end,
    })

    makima.api.register_command({
        name = "/copy-extend",
        tui_only = false,
        arguments = {
            { name = "source", type = "file" },
            { name = "destination", type = "directory", completion = {
                mode = "extend",
                items = { { label = "custom", insertion = "custom/" } },
            } },
        },
        handler = function(opts)
            makima.ui.flash("extend|" .. opts.values.source .. "|" .. opts.values.destination)
        end,
    })
    "#;

struct CopyTuiFixture {
    _host: makima_lua::test_support::PluginHostGuard,
    actions: flume::Receiver<makima_lua::UiAction>,
    app: App,
    root: TempDir,
}

fn copy_tui_fixture() -> CopyTuiFixture {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("destination folder")).unwrap();
    std::fs::create_dir_all(root.path().join("other directory")).unwrap();
    std::fs::write(root.path().join("source file.txt"), b"source bytes").unwrap();
    std::fs::write(
        root.path().join("destination folder/existing.txt"),
        b"existing bytes",
    )
    .unwrap();

    let (handle, host) = makima_lua::test_support::spawn_host_for_tests(&[]);
    host.host()
        .load_source("copy_fixture", COPY_TYPED_PLUGIN)
        .unwrap();
    let registry = host.host().command_registry();
    let dir = StateDir::from_path(env::temp_dir());
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );
    Arc::get_mut(&mut app.state.session)
        .unwrap()
        .set_cwd(root.path().to_string_lossy().into_owned());
    app.command_palette
        .set_cwd(Arc::from(root.path().to_string_lossy().as_ref()));

    let actions = host.host().ui_action_rx();
    CopyTuiFixture {
        _host: host,
        actions,
        app,
        root,
    }
}

/// Every provider publish bumps the completion revision and `accept` refuses
/// a stale candidate, so Enter only lands once the request has finished, not
/// as soon as the visible labels look right.
fn poll_settled_copy_completion(app: &mut App, mut ready: impl FnMut(&App) -> bool, what: &str) {
    wait_for(
        || {
            let _ = app.tick();
            app.command_palette.cadence() != Cadence::PENDING && ready(app)
        },
        what,
    );
}

fn poll_copy_completion(app: &mut App) {
    poll_settled_copy_completion(
        app,
        |app| app.command_palette.has_argument_selectable(),
        "copy completion popup never offered a selectable item",
    );
}

fn copy_candidate_labels(app: &App) -> Vec<String> {
    app.command_palette
        .argument_match_items()
        .into_iter()
        .map(|item| item.label.to_string())
        .collect()
}

fn poll_copy_candidates(app: &mut App, expected: &[&str]) {
    poll_settled_copy_completion(
        app,
        |app| {
            copy_candidate_labels(app)
                .iter()
                .map(String::as_str)
                .eq(expected.iter().copied())
        },
        &format!("copy completion candidates did not settle on {expected:?}"),
    );
}

fn copy_type(app: &mut App, text: &str) {
    for character in text.chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
}

fn copy_tree(root: &Path) -> Vec<(String, Option<Vec<u8>>)> {
    fn visit(root: &Path, path: &Path, entries: &mut Vec<(String, Option<Vec<u8>>)>) {
        let mut children = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        children.sort();
        for child in children {
            let relative = child
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            if child.is_dir() {
                entries.push((relative, None));
                visit(root, &child, entries);
            } else {
                entries.push((relative, Some(std::fs::read(child).unwrap())));
            }
        }
    }

    let mut entries = Vec::new();
    visit(root, root, &mut entries);
    entries
}

#[test_case(None; "omitted_optional_policy")]
#[test_case(Some("overwrite"); "selected_optional_policy")]
fn copy_typed_command_scenario(policy: Option<&str>) {
    let mut fixture = copy_tui_fixture();
    let before_tree = copy_tree(fixture.root.path());

    fixture
        .app
        .update(Msg::Paste("/copy \"source file.txt\" dest".into()));
    poll_copy_completion(&mut fixture.app);
    let rendered = rendered(&mut fixture.app);
    assert!(
        rendered.contains("destination:source file.txt"),
        "{rendered}"
    );
    assert_eq!(
        copy_candidate_labels(&fixture.app),
        vec!["destination:source file.txt"]
    );

    fixture.app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(
        fixture.app.input_box.buffer.value(),
        "/copy \"source file.txt\" \"destination folder/\""
    );
    assert!(!fixture.app.command_palette.is_active());

    match policy {
        Some("overwrite") => {
            fixture.app.update(Msg::Key(key(KeyCode::Char(' '))));
            poll_copy_completion(&mut fixture.app);
            assert_eq!(
                copy_candidate_labels(&fixture.app),
                vec!["skip", "overwrite"]
            );
            fixture.app.update(Msg::Key(key(KeyCode::Down)));
            fixture.app.update(Msg::Key(key(KeyCode::Enter)));
            assert_eq!(
                fixture.app.input_box.buffer.value(),
                "/copy \"source file.txt\" \"destination folder/\" overwrite"
            );
            assert!(!fixture.app.command_palette.is_active());
        }
        None => assert_eq!(
            fixture.app.input_box.buffer.value(),
            "/copy \"source file.txt\" \"destination folder/\""
        ),
        other => panic!("unsupported copy policy: {other:?}"),
    }

    fixture.app.update(Msg::Key(key(KeyCode::Enter)));
    let message = wait_for_receiver(&fixture.actions, "copy handler did not record invocation");
    let expected_flash = match policy {
        Some(policy) => format!("source file.txt|destination folder/|{policy}"),
        None => "source file.txt|destination folder/|".to_owned(),
    };
    assert!(matches!(message, makima_lua::UiAction::Flash(value) if value == expected_flash));
    assert_eq!(copy_tree(fixture.root.path()), before_tree);
}

#[test_case("/copy-default", vec!["destination folder/", "other directory/"], "default|source file.txt|destination folder/" ; "default")]
#[test_case("/copy-disabled", Vec::<&str>::new(), "disabled|source file.txt|destination folder/" ; "disabled")]
#[test_case("/copy-replace", vec!["replacement"], "replace|source file.txt|replacement folder/" ; "replace")]
#[test_case("/copy-extend", vec!["destination folder/", "other directory/", "custom"], "extend|source file.txt|destination folder/" ; "extend")]
fn copy_typed_completion_destination_policy(
    command: &str,
    expected_candidates: Vec<&str>,
    expected_flash: &str,
) {
    let mut fixture = copy_tui_fixture();
    let before_tree = copy_tree(fixture.root.path());
    fixture
        .app
        .update(Msg::Paste(format!("{command} \"source file.txt\" ")));
    if expected_candidates.is_empty() {
        assert!(
            fixture
                .app
                .command_palette
                .argument_match_items()
                .is_empty()
        );
    } else {
        poll_copy_candidates(&mut fixture.app, &expected_candidates);
        fixture.app.update(Msg::Key(key(KeyCode::Enter)));
    }

    if expected_candidates.is_empty() {
        copy_type(&mut fixture.app, "\"destination folder/\"");
    }
    settle_command_palette(&mut fixture.app);
    fixture.app.update(Msg::Key(key(KeyCode::Enter)));

    let message = wait_for_receiver(
        &fixture.actions,
        "copy policy handler did not record invocation",
    );
    assert!(matches!(message, makima_lua::UiAction::Flash(value) if value == expected_flash));
    assert_eq!(copy_tree(fixture.root.path()), before_tree);
}

const RUN_CMDLINE_REJECTED: &str = "a rejected cmdline must not run anything";
const MAX_COMMAND_DEPTH_ERROR: &str = "maximum command recursion depth exceeded";

#[test_case("/new" ; "plain")]
#[test_case("/NEW" ; "uppercase")]
#[test_case("  /new  " ; "surrounding_whitespace")]
#[test_case("new" ; "missing_slash")]
fn run_cmdline_executes_builtin(cmdline: &str) {
    let mut app = test_app();

    let actions = app.run_cmdline(cmdline, 0).unwrap();

    assert!(matches!(&actions[..], [Action::ReplaceSession(_)]));
}

#[test]
fn run_cmdline_splits_args_off_the_name() {
    let mut app = test_app();

    let actions = app.run_cmdline("/btw what is rust?", 0).unwrap();

    assert!(
        matches!(&actions[..], [Action::Btw(q, images)] if q == "what is rust?" && images.is_empty())
    );
}

#[test]
fn direct_tasks_command_opens_picker() {
    let mut app = test_app();

    let actions = app.execute_command(cmd("/tasks"), 0);

    assert!(actions.is_empty());
    assert!(app.task_picker.is_open());
}

#[test]
fn direct_login_command_opens_picker() {
    let mut app = test_app();

    let actions = app.execute_command(cmd("/login"), 0);

    assert!(actions.is_empty());
    assert!(app.login_picker.is_open());
}

#[test]
fn direct_exit_command_requests_successful_exit() {
    let mut app = test_app();

    let actions = app.execute_command(cmd("/exit"), 0);

    assert_eq!(app.exit_request, ExitRequest::Success);
    assert!(matches!(actions.as_slice(), [Action::ManualExit]));
}

#[test]
fn direct_custom_command_renders_args_and_starts_run() {
    let mut app = app_with_custom_commands(&[CustomCommand {
        name: "review".into(),
        description: "Code review".into(),
        content: "Review $ARGUMENTS".into(),
        scope: CommandScope::Project,
        accepts_args: true,
        argument_hint: None,
    }]);

    let (target, rx) = spawn_input_child(&mut app, TASK_ID, "reviewer");
    app.active_chat = 1;
    bind_actor_target(&app, target);
    let before = app.chats[1].message_count();
    let actions = app.execute_command(
        ParsedCommand {
            name: "/project:review".into(),
            args: "src/lib.rs".into(),
        },
        0,
    );

    assert!(actions.is_empty());
    let input = received_input(&rx);
    assert_eq!(input.message, "Review src/lib.rs");
    assert_eq!(app.chats[1].message_count(), before);
    assert!(app.queue.root_text_messages().is_empty());
}

#[test]
fn run_cmdline_dispatches_custom_command() {
    let mut app = app_with_custom_commands(&[CustomCommand {
        name: "review".into(),
        description: "Code review".into(),
        content: "Review $ARGUMENTS".into(),
        scope: CommandScope::Project,
        accepts_args: true,
        argument_hint: None,
    }]);

    let (target, rx) = spawn_input_child(&mut app, TASK_ID, "reviewer");
    app.active_chat = 1;
    bind_actor_target(&app, target);
    let before = app.chats[1].message_count();
    let actions = app.run_cmdline("/project:review src/lib.rs", 0).unwrap();

    assert!(actions.is_empty());
    let input = received_input(&rx);
    assert_eq!(input.message, "Review src/lib.rs");
    assert_eq!(app.chats[1].message_count(), before);
    assert!(app.queue.root_text_messages().is_empty());
}

/// Only the typed path clears the input, so a keybind or autocmd reaching for
/// `run_command` cannot eat a half-written message.
#[test]
fn run_cmdline_keeps_typed_input() {
    let mut app = test_app();
    app.input_box.set_input("half written".into());

    app.run_cmdline("/help", 0).unwrap();

    assert_eq!(app.input_box.buffer.value(), "half written");
}

#[test]
fn run_cmdline_unknown_name_errors_without_dispatching() {
    let mut app = test_app();
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;

    let Err(err) = app.run_cmdline("/nope", 0) else {
        panic!("{RUN_CMDLINE_REJECTED}");
    };

    assert!(err.contains("/nope"), "err={err:?}");
    assert!(probe.recv_command().is_none(), "{RUN_CMDLINE_REJECTED}");
}

#[test]
fn run_cmdline_uses_registry_depth_limit() {
    let mut app = test_app();

    assert!(
        app.run_cmdline("/new", (makima_commands::MAX_COMMAND_DEPTH + 1) as u8)
            .is_ok(),
        "resolution succeeds before the command outcome arrives"
    );
    assert_eq!(app.status_bar.flash_text(), Some(MAX_COMMAND_DEPTH_ERROR));
    assert!(
        app.run_cmdline("/new", makima_commands::MAX_COMMAND_DEPTH as u8)
            .is_ok(),
        "the cap itself must still run"
    );
}

/// A Lua command reached through an alias carries the hop count onward, or a
/// cycle of Lua aliases would never trip the cap. It goes out spelled as
/// registered, since only that spelling dispatches.
#[test]
fn run_cmdline_forwards_depth_to_lua_command() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let (registry, _producer) = lua_registry(TestLuaCommand {
        handle: handle.clone(),
        name: Arc::from("/Sessions"),
        plugin: Arc::from("sessions"),
        max_args: Some(0),
        completion: false,
    });
    let mut app = build_app_with_full(
        dir.clone(),
        Arc::new(test_writer(dir)),
        registry,
        handle,
        UiConfig::default(),
    );

    app.run_cmdline("/sessions", 3).unwrap();

    assert_eq!(
        probe.recv_command(),
        Some(("/Sessions".to_string(), String::new(), 3))
    );
}

#[test]
fn startup_session_picker_emits_plugin_request() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let app = build_app_with_handle(dir.clone(), Arc::new(test_writer(dir)), handle);

    app.open_startup_session_picker();

    assert_eq!(
        probe.try_recv_autocmd(),
        Some(("SessionPickerRequested".to_owned(), serde_json::json!({})))
    );
}

#[test]
fn slash_noncommand_is_rejected_and_flashes() {
    let mut app = test_app();
    let actions = type_and_submit(&mut app, "/nonexistent");
    assert!(actions.is_empty());
    app.execute_pending_commands();
    assert!(
        app.status_bar
            .flash_text()
            .is_some_and(|text| text.contains("unknown command"))
    );
    assert!(!actions.iter().any(|a| matches!(a, Action::SendMessage(..))));
}

#[test_case("//lmao", "/lmao" ; "escaped literal strips one slash")]
#[test_case("///lmao", "//lmao" ; "triple slash strips one slash")]
fn slash_escape_sends_literal_input(text: &str, expected: &str) {
    let mut app = test_app();
    assert!(type_and_submit(&mut app, text).is_empty());
    assert_eq!(root_submissions(&app), [expected]);
}

fn bind_history_actor(app: &mut App, messages: Vec<Message>) -> AgentRef {
    let manager = AgentManagerHandle::new(Default::default()).unwrap();
    let (tx, _) = flume::unbounded();
    let shared = Arc::new(ArcSwap::from_pointee(HistorySnapshot::new(
        messages.clone(),
    )));
    let template = cli_tool_ctx(std::env::current_dir().unwrap());
    let config = EffectiveAgentConfig::new(
        makima_agent::RunSettings {
            provider: template.provider,
            model: (*template.model).clone(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        },
        AgentMode::Build,
    );
    let target = manager
        .create_root_with_config(
            Some(config),
            messages.clone(),
            Some(Arc::clone(&shared)),
            |_| Ok::<_, String>(Box::new(FocusedInputBackend(tx)) as Box<dyn ActorBackend>),
        )
        .unwrap();
    app.chats[0].agent_id = Some(target.id());
    app.shared_history = Some(shared);
    app.state.session_mut().replace_messages(messages);
    bind_actor_target(app, target.clone());
    target
}

#[test_case(false ; "main")]
#[test_case(true ; "child")]
fn focused_retry_tracks_only_target_until_request_progress(child: bool) {
    let mut app = streaming_app();
    let target = if child {
        let (target, _) = spawn_input_child(&mut app, TASK_ID, "research");
        app.active_chat = 1;
        bind_actor_target(&app, target.clone());
        Some(target)
    } else {
        None
    };
    let info = target
        .as_ref()
        .map(|target| subagent_info_for_agent(target.id(), TASK_ID, "research", None, None));
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::Retry {
            attempt: 1,
            message: RETRY_MESSAGE.into(),
            delay_ms: RETRY_DELAY.as_millis() as u64,
        },
        subagent: info.clone(),
        identity: Some(work_identity(app.run_id)),
    })));
    assert_eq!(app.focused_retry().unwrap().message, RETRY_MESSAGE);
    if child {
        assert!(app.chats[0].retry_info.is_none());
    }
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta {
            text: "request resumed".into(),
        },
        subagent: info,
        identity: Some(work_identity(app.run_id)),
    })));
    assert!(app.focused_retry().is_none());
}

#[test]
fn focused_plan_state_is_isolated() {
    let mut app = test_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting("root-plan.md".into());
    app.plan_form.on_plan_ready();
    let (target, _) = spawn_input_child_with_mode(
        &mut app,
        TASK_ID,
        "planner",
        AgentMode::Plan("child-plan.md".into()),
    );
    app.active_chat = 1;
    bind_actor_target(&app, target);
    app.sync_focused_queue();
    assert!(!app.plan_form.is_visible());
    assert_eq!(
        app.focused_plan_path(),
        Some(PathBuf::from("child-plan.md"))
    );
    app.plan_form.on_plan_ready();
    app.plan_approval_pending = true;
    app.update(Msg::Paste("not a draft".into()));
    assert!(app.input_box.buffer.value().is_empty());
    app.active_chat = 0;
    app.sync_focused_queue();
    assert!(app.plan_form.is_visible());
    assert!(!app.plan_approval_pending);
    assert_eq!(app.focused_plan_path(), Some(PathBuf::from("root-plan.md")));
    app.active_chat = 1;
    app.sync_focused_queue();
    assert!(app.plan_form.is_visible());
    assert!(app.plan_approval_pending);
    app.plan_approval_pending = false;
    let child_id = app.selected_agent_id().unwrap();
    let actions = app.implement_plan(false);
    assert!(matches!(actions.as_slice(), [Action::ApprovePlan {
        clear_context: false,
        selection,
        parallel: _,
        path,
    }] if selection.is_empty() && path == Path::new("child-plan.md")));
    assert_eq!(
        app.command_runtime
            .target_agent(&app.command_target)
            .unwrap()
            .id(),
        child_id
    );
    assert!(app.plan_approval_pending);
    assert_eq!(app.state.plan.path(), Some(Path::new("root-plan.md")));
    app.active_chat = 0;
    app.sync_focused_queue();
    assert!(app.plan_form.is_visible());
    assert!(!app.plan_approval_pending);
    app.active_chat = 1;
    app.sync_focused_queue();
    assert!(app.plan_approval_pending);
    assert_eq!(app.focused_mode(), Mode::Plan);
    assert_eq!(
        app.focused_model_spec(),
        app.focused_config().unwrap().model.spec()
    );
}

#[test]
fn root_error_recovery_does_not_follow_child_queue_focus() {
    let (mut app, rx) = app_with_subagent_actor(TASK_ID);
    app.queue_and_notify(queued_msg("root pending"));
    type_and_submit(&mut app, "child active");
    received_input(&rx);
    type_and_submit(&mut app, "child pending");
    app.queue.set_focus_at(0);
    assert_eq!(app.queue.focus(), Some(0));
    assert_eq!(app.build_meta().queued_messages, ["root pending"]);
    app.update(agent_msg(AgentEvent::ControlError {
        message: "root failure".into(),
    }));
    assert_eq!(app.recoverable_queue, ["root pending"]);
    assert_eq!(app.queue.text_messages(), ["child pending"]);
    assert_eq!(app.queue.focus(), Some(0));
    assert_eq!(app.build_meta().queued_messages, ["root pending"]);
}

#[test]
fn rejected_focused_submission_retains_pending_shell_results() {
    const SHELL_COMMAND: &str = "pwd";
    const SHELL_OUTPUT: &str = "shell cwd";
    let (mut app, input_rx) = app_with_subagent_actor(TASK_ID);
    let target = app.selected_agent_id().unwrap();
    let id = app.shell.reserve_for(Some(target));
    app.handle_shell_event(ShellEvent::Done {
        id,
        command: SHELL_COMMAND.into(),
        output: SHELL_OUTPUT.into(),
        is_error: false,
        visible: true,
    });
    let before = serde_json::to_value(app.shell.results_for(Some(target))).unwrap();
    let message_count = app.chats[app.active_chat].message_count();
    assert!(!app.shell.results_for(Some(target)).is_empty());
    let result = app.submit_prompt(queued_msg(&"x".repeat(256 * 1024 + 1)));
    assert!(matches!(result, super::queue::SubmitOutcome::Rejected(_)));
    assert_eq!(app.chats[app.active_chat].message_count(), message_count);
    assert!(app.queue.text_messages().is_empty());
    assert!(input_rx.is_empty());
    assert_eq!(
        serde_json::to_value(app.shell.results_for(Some(target))).unwrap(),
        before
    );
}

#[test_case(false; "child")]
#[test_case(true; "main")]
fn rejected_command_turn_retains_pending_shell_results(root: bool) {
    let (mut app, _rx) = app_with_subagent_actor(TASK_ID);
    let target = app
        .command_runtime
        .target_agent(&app.command_target)
        .unwrap();
    if root {
        app.chats[0].agent_id = Some(target.id());
    }
    for scope in [Some(target.id()), None]
        .into_iter()
        .take(if root { 2 } else { 1 })
    {
        let id = app.shell.reserve_for(scope);
        app.handle_shell_event(ShellEvent::Done {
            id,
            command: REJECTED_COMMAND_SHELL_COMMAND.into(),
            output: REJECTED_COMMAND_SHELL_OUTPUT.into(),
            is_error: false,
            visible: true,
        });
    }
    let target_before = serde_json::to_value(app.shell.results_for(Some(target.id()))).unwrap();
    let root_before = serde_json::to_value(app.shell.results_for(None)).unwrap();
    let admission = makima_lua::orchestration::TrustedTarget {
        target: target.clone(),
        template: cli_tool_ctx(std::env::current_dir().unwrap()),
    };
    let turn = makima_commands::AgentTurn {
        content: makima_commands::CommandContent {
            text: "x".repeat(256 * 1024 + 1).into(),
            ..Default::default()
        },
        prompt: None,
    };
    app.submit_command_turn(turn, Some(&admission));
    assert_eq!(
        serde_json::to_value(app.shell.results_for(Some(target.id()))).unwrap(),
        target_before
    );
    assert_eq!(
        serde_json::to_value(app.shell.results_for(None)).unwrap(),
        root_before
    );
    assert!(app.status_bar.flash_text().is_some());
}

#[test]
fn focused_queue_removes_the_selected_compact_with_a_shared_run_id() {
    const FIRST: &str = "first compact";
    const SECOND: &str = "second compact";
    let (mut app, rx) = app_with_subagent_actor(TASK_ID);
    type_and_submit(&mut app, "active");
    received_input(&rx);
    let actor = app
        .command_runtime
        .target_agent(&app.command_target)
        .unwrap()
        .actor()
        .unwrap();
    actor.push_compact(app.run_id, Some(FIRST.into())).unwrap();
    actor.push_compact(app.run_id, Some(SECOND.into())).unwrap();
    app.sync_focused_queue();
    app.queue.set_focus_at(1);
    app.queue.remove_focused();
    let entries = actor.pending_queue();
    assert_eq!(entries.len(), 1);
    assert!(
        matches!(&entries[0], makima_agent::actor::PendingQueueEntry::Compact { instructions: Some(text), .. } if text == FIRST)
    );
}

#[test]
fn focused_queue_preserves_compact_and_stale_removal_identity() {
    let (mut app, rx) = app_with_subagent_actor(TASK_ID);
    type_and_submit(&mut app, "active");
    received_input(&rx);
    type_and_submit(&mut app, "first queued");
    let actor = app
        .command_runtime
        .target_agent(&app.command_target)
        .unwrap()
        .actor()
        .unwrap();
    actor
        .push_compact(1, Some(COMPACT_GUIDANCE.into()))
        .unwrap();
    type_and_submit(&mut app, "after compact");
    app.sync_focused_queue();
    assert_eq!(app.queue.len(), 3);
    assert_eq!(app.queue.panel_entries()[1].text, COMPACT_WITH_GUIDANCE);
    app.queue.set_focus_at(0);
    let first = actor
        .pending_submissions()
        .into_iter()
        .find(|entry| entry.text == "first queued")
        .unwrap();
    actor.remove_submission(first.id).unwrap();
    assert_eq!(app.queue.focus(), None);
    app.queue.remove_focused();
    assert_eq!(app.queue.len(), 2);
    assert_eq!(app.queue.panel_entries()[0].text, COMPACT_WITH_GUIDANCE);
    app.queue.set_focus_at(0);
    app.queue.remove_focused();
    assert_eq!(app.queue.len(), 1);
    assert_eq!(app.queue.panel_entries()[0].text, "after compact");
}

fn apply_rewind_action(app: &mut App, actions: Vec<Action>) {
    let mut actions = actions.into_iter();
    let Action::RewindAgent { target, boundary } = actions.next().expect("rewind action") else {
        panic!("expected actor rewind");
    };
    assert!(actions.next().is_none());
    let actor = target.actor().unwrap();
    let guard = actor.prepare_idle().unwrap();
    let (draft, replacement) = actor.rewind_snapshot(&boundary).unwrap();
    let operation = guard.reserve_prepared_operation(None).unwrap();
    let transaction = makima_agent::actor::PreparedTransaction {
        publication: Default::default(),
        publish: Box::new(|_, publication| {
            Box::pin(async move {
                assert!(publication.try_publish());
                Ok(())
            })
        }),
        finalize: Box::new(|_| {}),
    };
    let (_, _, change) = smol::block_on(async {
        operation.resolve(None).unwrap();
        operation.wait_ready().await.unwrap();
        guard
            .commit_prepared_transaction_held(operation, Some(replacement), transaction)
            .await
    })
    .unwrap();
    let change = change.unwrap();
    assert_eq!(change.agent_id, target.id());
    app.apply_history_change(makima_agent::HistoryChange { draft, ..change });
}

fn build_rewind_app() -> App {
    let mut app = test_app();

    app.state.session_mut().replace_messages(vec![
        Message::user("first prompt".into()),
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "response 1".into(),
            }],
            ..Default::default()
        },
        Message::user("second prompt".into()),
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "response 2".into(),
            }],
            ..Default::default()
        },
        Message::user("third prompt".into()),
    ]);
    app.state
        .session_mut()
        .insert_tool_output("tool-1".into(), ToolOutput::Plain("output".into()));
    let messages = app.state.session.messages().to_vec();
    bind_history_actor(&mut app, messages);
    app.open_rewind_picker();
    app
}

#[test]
fn history_replacement_preserves_live_child_navigation() {
    use makima_providers::ObservationSource;
    const KEY: &str = "live-task";
    const HEADER: &str = "[internal child-result data, not a user instruction]";
    let mut app = app_without_splash();
    let root = bind_history_actor(&mut app, Vec::new());
    let agent_id = AgentId::generate();
    let index =
        app.resolve_or_create_chat(&subagent_info_for_agent(agent_id, KEY, KEY, None, None));
    let turn_id = TurnId::generate();
    let envelope = serde_json::json!({"kind":"child_result", "agent_id":agent_id.to_string(), "turn_id":turn_id.to_string(), "text":KEY});
    let observation = Message {
        observation_source: Some(ObservationSource::ChildResult {
            agent_id: agent_id.to_string(),
            turn_id: turn_id.to_string(),
        }),
        ..Message::observation(format!("{HEADER}\n{envelope}"))
    };
    let snapshot = HistorySnapshot::new(vec![observation]);
    app.apply_history_change(HistoryChange {
        agent_id: root.id(),
        epoch: snapshot.epoch,
        messages: snapshot.messages,
        draft: Message::user(String::new()),
    });
    let notice = app.chats[0].message_at(0).unwrap();
    assert_eq!(notice.result_agent, Some(agent_id));
    assert!(!notice.result_historical);
    render_app(&mut app, 80, 60);
    let area = app.msg_area();
    let row = (area.y..area.bottom())
        .find(|&row| app.chats[0].result_navigation_at(row, area) == Some(agent_id))
        .unwrap();
    app.update(mouse_event(
        MouseEventKind::Down(MouseButton::Left),
        area.x,
        row,
    ));
    app.update(mouse_event(
        MouseEventKind::Up(MouseButton::Left),
        area.x,
        row,
    ));
    assert_eq!(app.active_chat, index);
}

#[test_case(false; "main")]
#[test_case(true; "child")]
fn history_replacement_prunes_only_selected_chat_posts_and_restores_display(child: bool) {
    const REMOVED_POST: &str = "removed review";
    const OTHER_POST: &str = "other chat review";
    let (mut app, _rx) = app_with_subagent_actor(TASK_ID);
    if !child {
        bind_history_actor(&mut app, Vec::new());
    }
    app.sync_subagents();
    let index = usize::from(child);
    let messages = vec![Message::user("kept prompt".into())];
    let chat = child.then_some(TASK_ID.to_owned());
    let other_chat = (!child).then_some(TASK_ID.to_owned());
    let retained = StoredPost {
        chat: chat.clone(),
        after_message: 1,
        post: Post {
            text: POST_TEXT.into(),
            level: PostLevel::Error,
            agent: None,
        },
    };
    let other = StoredPost {
        chat: other_chat,
        after_message: 2,
        post: Post {
            text: OTHER_POST.into(),
            level: PostLevel::Info,
            agent: None,
        },
    };
    for post in [
        retained.clone(),
        StoredPost {
            chat,
            after_message: 2,
            post: Post {
                text: REMOVED_POST.into(),
                level: PostLevel::Warn,
                agent: None,
            },
        },
        other.clone(),
    ] {
        app.state.session_mut().push_post(post);
    }
    let snapshot = HistorySnapshot::new(messages);
    app.apply_history_change(HistoryChange {
        agent_id: app.chats[index].agent_id.unwrap(),
        epoch: snapshot.epoch,
        messages: snapshot.messages,
        draft: Message::user("next prompt".into()),
    });
    assert_eq!(app.state.session.posts(), [retained, other]);
    assert_last_is_post(&app.chats[index]);
    let displayed: Vec<_> = (0..app.chats[index].message_count())
        .map(|offset| app.chats[index].message_at(offset).unwrap().text.clone())
        .collect();
    assert!(!displayed.iter().any(|text| text == REMOVED_POST));
    let tmp = TempDir::new().unwrap();
    let dir = StateDir::from_path(tmp.path().to_path_buf());
    let id = app.state.session.id;
    app.state.session_mut().save(&dir).unwrap();
    let restored = AppSession::load(id, &dir).unwrap();
    assert_eq!(restored.posts(), app.state.session.posts());
    app.state.session = Arc::new(restored);
    app.reset_ui_chrome();
    app.restore_display();
    assert!((0..app.chats[index].message_count()).any(|offset| {
        let message = app.chats[index].message_at(offset).unwrap();
        message.text == POST_TEXT && message.role == DisplayRole::Post(PostLevel::Error)
    }));
    assert!(
        !(0..app.chats[index].message_count())
            .any(|offset| { app.chats[index].message_at(offset).unwrap().text == REMOVED_POST })
    );
}

#[test]
fn rewind_to_middle_truncates_and_populates_input() {
    let mut app = build_rewind_app();
    let old_run_id = app.run_id;
    let entry = crate::components::rewind_picker::RewindEntry {
        turn_index: 2,
        prompt_preview: "2: second".into(),
    };
    let actions = app.rewind_to(entry);

    assert_eq!(app.state.session.messages().len(), 5);
    assert!(app.state.session.tool_outputs().contains_key("tool-1"));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.run_id, old_run_id);
    let agent_id = app.selected_agent_id();
    apply_rewind_action(&mut app, actions);
    assert_eq!(app.selected_agent_id(), agent_id);
    assert_eq!(app.state.session.messages().len(), 2);
    assert_eq!(app.input_box.buffer.value(), "second prompt");
    assert_eq!(app.run_id, old_run_id);
}

#[test]
fn rewind_to_first_turn_clears_everything() {
    let mut app = build_rewind_app();
    let baseline = 100_000 - estimate_message_tokens(app.state.session.messages());
    app.state.context_size = 100_000;
    app.state.token_usage.input = 500;
    app.state.token_usage.output = 200;
    let entry = crate::components::rewind_picker::RewindEntry {
        turn_index: 0,
        prompt_preview: "1: first".into(),
    };
    let actions = app.rewind_to(entry);

    assert!(!app.state.session.messages().is_empty());
    assert!(app.state.session.tool_outputs().contains_key("tool-1"));
    assert_eq!(app.state.token_usage.input, 500);
    assert_eq!(app.state.token_usage.output, 200);
    assert_eq!(app.state.context_size, 100_000);
    apply_rewind_action(&mut app, actions);
    assert!(app.state.session.messages().is_empty());
    assert!(!app.state.session.tool_outputs().contains_key("tool-1"));
    assert_eq!(app.state.context_size, baseline);
    assert_eq!(app.input_box.buffer.value(), "first prompt");
    assert_eq!(app.state.token_usage.input, 500);
    assert_eq!(app.state.token_usage.output, 200);
}

#[test_case(Duration::ZERO,          true  ; "keeps_fresh_error")]
#[test_case(Duration::from_secs(60), false ; "clears_stale_error")]
fn tick_error_expiry(age: Duration, expect_error: bool) {
    let mut app = test_app();
    app.status = Status::Error {
        message: "fail".into(),
        since: Instant::now() - age,
    };
    let _ = app.tick_error_expiry();
    assert_eq!(matches!(app.status, Status::Error { .. }), expect_error);
}

#[test]
fn retry_clears_in_progress_tools() {
    let mut app = streaming_app();
    app.update(agent_msg(AgentEvent::ToolPending {
        id: "t1".into(),
        name: "bash".into(),
    }));
    assert_eq!(app.chats[0].in_progress_count(), 1);

    app.update(agent_msg(AgentEvent::Retry {
        attempt: 1,
        message: "overloaded".into(),
        delay_ms: 1000,
    }));
    assert_eq!(app.chats[0].in_progress_count(), 0);
    assert!(app.chats[0].retry_info.is_some());
}

#[test]
fn retry_clears_subagent_in_progress_tools() {
    let mut app = streaming_app();
    app.update(subagent_msg(
        AgentEvent::ToolPending {
            id: "st1".into(),
            name: "bash".into(),
        },
        TASK_ID,
        Some("research"),
    ));
    assert_eq!(app.chats.len(), 2);
    assert_eq!(app.chats[1].in_progress_count(), 1);

    app.update(subagent_msg(
        AgentEvent::Retry {
            attempt: 1,
            message: "overloaded".into(),
            delay_ms: 1000,
        },
        TASK_ID,
        Some("research"),
    ));
    assert_eq!(app.chats[1].in_progress_count(), 0);
    assert!(app.chats[0].retry_info.is_none());
}

fn auth_retry_enter(app: &mut App) -> Vec<Action> {
    app.update(Msg::Key(key(KeyCode::Enter)))
}

fn auth_retry_type_then_enter(app: &mut App) -> Vec<Action> {
    type_and_submit(app, "ignored")
}

#[test_case(auth_retry_enter          ; "bare_enter")]
#[test_case(auth_retry_type_then_enter ; "typed_text_then_enter")]
fn auth_retry_sends_empty_answer(submit: fn(&mut App) -> Vec<Action>) {
    let mut app = streaming_app();
    let (tx, rx) = flume::unbounded();
    app.answer_tx = Some(tx);

    app.update(agent_msg(AgentEvent::AuthRequired));
    assert!(matches!(
        app.pending_input,
        PendingInput::AuthRetry { agent_id: None }
    ));

    let actions = submit(&mut app);
    assert!(actions.is_empty());
    assert_eq!(app.pending_input, PendingInput::None);
    assert_eq!(rx.try_recv().unwrap(), "");
}

fn app_with_subagent_tx(id: &str) -> (App, flume::Receiver<String>, flume::Receiver<String>) {
    let (sub_tx, sub_rx) = flume::unbounded();
    let (main_tx, main_rx) = flume::unbounded();
    let mut app = streaming_app();
    app.answer_tx = Some(main_tx);
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta { text: "x".into() },
        subagent: Some(subagent_info_with_tx(id, "research", Some(sub_tx))),
        identity: Some(work_identity(1)),
    })));
    (app, sub_rx, main_rx)
}

#[test]
fn auth_required_in_subagent_shows_in_both_chats() {
    let mut app = app_with_subagent_id("sub1");
    app.update(subagent_msg(
        AgentEvent::AuthRequired,
        "sub1",
        Some("research"),
    ));

    assert_eq!(app.chats[1].last_message_text(), AUTH_EXPIRED_MSG);
    assert_eq!(app.chats[0].last_message_text(), AUTH_EXPIRED_MSG);
    assert!(matches!(
        app.pending_input,
        PendingInput::AuthRetry { agent_id: Some(_) }
    ));
}

#[test]
fn auth_retry_in_subagent_routes_to_subagent_channel() {
    let (mut app, sub_rx, main_rx) = app_with_subagent_tx("sub1");
    app.update(subagent_msg(
        AgentEvent::AuthRequired,
        "sub1",
        Some("research"),
    ));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));

    assert!(actions.is_empty());
    assert_eq!(app.pending_input, PendingInput::None);
    assert_eq!(sub_rx.try_recv().unwrap(), "");
    assert!(main_rx.try_recv().is_err());
}

#[test]
fn cancel_clears_subagent_auth_retry() {
    let (mut app, sub_rx, _main_rx) = app_with_subagent_tx("sub1");
    app.update(subagent_msg(
        AgentEvent::AuthRequired,
        "sub1",
        Some("research"),
    ));

    cancel_app(&mut app);

    assert_eq!(app.pending_input, PendingInput::None);
    assert!(sub_rx.try_recv().is_err());
}

#[test]
fn stale_auth_required_after_cancel_is_dropped() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 2;
    let count_before = app.chats[0].message_count();
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::AuthRequired,
        subagent: None,
        identity: Some(work_identity(1)),
    })));
    assert_eq!(app.pending_input, PendingInput::None);
    assert_eq!(app.chats[0].message_count(), count_before);
}

#[test]
fn send_to_agent_unknown_subagent_falls_back_to_main() {
    let (main_tx, main_rx) = flume::unbounded();
    let mut app = streaming_app();
    app.answer_tx = Some(main_tx);

    app.pending_input = PendingInput::AuthRetry {
        agent_id: Some(AgentId::generate()),
    };
    app.update(Msg::Key(key(KeyCode::Enter)));

    assert_eq!(main_rx.try_recv().unwrap(), "");
    assert_eq!(app.pending_input, PendingInput::None);
}

/// Output that mutates a segment already on screen, rather than appending a
/// new one, still has to be searchable. A `!` shell command does exactly this
/// and never sets `Status::Streaming`, so the status is no guide to staleness.
#[test]
fn search_reaches_output_that_lands_in_an_existing_segment() {
    const LATE_TEXT: &str = "zzarrived";

    let mut app = test_app();
    app.run_id = 1;
    app.update(agent_msg(tool_start("tool-1", "bash")));
    rendered(&mut app);
    app.update(Msg::Key(kb::SEARCH.to_key_event()));

    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "tool-1".into(),
        tool: "bash".into(),
        output: ToolOutput::Plain(LATE_TEXT.into()),
        is_error: false,
        annotation: None,
        written_path: None,
    }))));
    rendered(&mut app);
    for c in LATE_TEXT.chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }

    assert!(
        app.search_modal.current_segment_index().is_some(),
        "search must see output that landed in a segment opened before it"
    );
}

/// The messages that close a turn out land after the status has already left
/// `Streaming`, so both statuses have to reach a freshly appended segment.
#[test_case(Status::Streaming ; "mid_turn")]
#[test_case(Status::Idle      ; "after_the_turn_ended")]
fn search_reaches_output_that_lands_while_the_modal_is_open(status: Status) {
    // Nothing else in the transcript holds this string, so a hit can only come
    // from a corpus rebuilt after the modal opened.
    const LATE_TEXT: &str = "zzarrived";

    let mut app = test_app();
    app.status = status;
    app.update(Msg::Key(kb::SEARCH.to_key_event()));

    app.active_chat().push(DisplayMessage::new(
        DisplayRole::Assistant,
        LATE_TEXT.into(),
    ));
    rendered(&mut app);
    for c in LATE_TEXT.chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }

    assert!(
        app.search_modal.current_segment_index().is_some(),
        "search must see the message that arrived after the modal opened"
    );
}

#[test_case(42, false ; "restores_scroll_position")]
#[test_case(0,  true  ; "restores_auto_scroll")]
fn search_escape_restores_scroll(scroll_top: u16, auto_scroll: bool) {
    let mut app = test_app();
    app.active_chat().restore_scroll(scroll_top, auto_scroll);

    app.update(Msg::Key(kb::SEARCH.to_key_event()));
    app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(!app.search_modal.is_open());
    assert_eq!(app.active_chat().scroll_top(), scroll_top);
    assert_eq!(app.active_chat().auto_scroll(), auto_scroll);
}

#[test]
fn mcp_command_opens_picker() {
    let mut app = test_app();
    app.execute_command(cmd("/mcp"), 0);
    assert!(app.mcp_picker.is_open());
}

#[test]
fn mcp_toggle_dispatches_action() {
    let mut app = test_app();
    app.mcp_picker = McpPicker::new(
        McpSnapshotReader::from_snapshot(McpSnapshot {
            infos: vec![McpServerInfo {
                name: "test-srv".into(),
                transport_kind: "stdio",
                tool_count: 2,
                prompt_count: 0,
                status: McpServerStatus::Running,
                config_path: PathBuf::from("/tmp/config.toml"),
                url: None,
                oauth: None,
            }],
            prompts: vec![],
            pids: vec![],
            generation: 0,
        }),
        McpConfigErrors::new(PathBuf::new()),
    );
    app.execute_command(cmd("/mcp"), 0);

    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(
        &actions[0],
        Action::ToggleMcp(name, false) if name == "test-srv"
    ));
}

#[test_case(
    |app: &mut App| { set_root_mode(app, Mode::Plan); app.state.plan = PlanState::Drafting("test-plan.md".into()); app.plan_form.on_plan_ready(); },
    ""
    ; "consumed_by_plan_form"
)]
#[test_case(
    |app: &mut App| { open_tasks_picker(app); },
    ""
    ; "routed_to_open_picker"
)]
#[test_case(
    |app: &mut App| { app.update(Msg::Key(kb::SEARCH.to_key_event())); },
    ""
    ; "routed_to_search_modal"
)]
#[test_case(
    |_: &mut App| {},
    "pasted"
    ; "falls_through_to_input"
)]
fn paste_routing(setup: fn(&mut App), expected_input: &str) {
    let mut app = test_app();
    setup(&mut app);
    app.update(Msg::Paste("pasted".into()));
    assert_eq!(app.input_box.buffer.value(), expected_input);
}

#[test_case(PlanState::None,                                       true  ; "no_plan")]
#[test_case(PlanState::Drafting(PathBuf::from("/tmp/plan.md")),     false ; "plan_drafting")]
#[test_case(PlanState::Ready(PathBuf::from("/tmp/plan.md")),       false ; "plan_ready")]
fn open_editor(plan: PlanState, expect_flash: bool) {
    let mut app = test_app();
    let plan_path = plan.path().map(Path::to_path_buf);
    app.state.plan = plan;
    let actions = app.update(Msg::Key(kb::OPEN_EDITOR.to_key_event()));
    if expect_flash {
        assert!(actions.is_empty());
        assert_eq!(app.status_bar.flash_text().unwrap(), FLASH_NO_PLAN);
        assert!(!app.plan_form.is_visible());
    } else {
        let expected = plan_path.unwrap();
        assert!(matches!(&actions[..], [Action::OpenEditor(p)] if p == &expected));
        assert!(!app.plan_form.is_visible());
    }
}

#[test]
fn alt_o_opens_editor_for_input() {
    let mut app = test_app();
    app.input_box.buffer.insert_text("hello");
    let actions = app.update(Msg::Key(kb::EDIT_INPUT.to_key_event()));
    assert!(matches!(&actions[..], [Action::EditInputInEditor]));
}

#[test]
fn btw_empty_is_rejected_by_registry() {
    let mut app = test_app();
    let actions = app.execute_command(
        ParsedCommand {
            name: "/btw".into(),
            args: String::new(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        "invalid typed arguments for /btw: raw arguments are required"
    );
}

#[test]
fn btw_with_question_returns_action() {
    let mut app = test_app();
    let actions = app.execute_command(
        ParsedCommand {
            name: "/btw".into(),
            args: "what is rust?".into(),
        },
        0,
    );
    assert!(
        matches!(&actions[..], [Action::Btw(q, images)] if q == "what is rust?" && images.is_empty())
    );
}

#[test]
fn btw_modal_key_routing_and_animation() {
    let mut app = test_app();
    let (tx, rx) = flume::bounded(1);
    app.btw_modal.open("test", rx);

    // A pending stream is data, drained by `poll`. Only the typewriter
    // revealing the answer moves on its own.
    assert!(app.btw_modal.is_streaming());
    assert_eq!(app.btw_modal.cadence(), Cadence::IDLE);
    tx.send(BtwEvent::TextDelta("hi".into())).unwrap();
    assert_eq!(app.btw_modal.poll(), Dirty::YES);
    assert_eq!(app.btw_modal.cadence(), Cadence::SMOOTH);

    let actions = app.update(Msg::Key(key(KeyCode::Char('x'))));
    assert!(actions.is_empty());
    assert!(app.btw_modal.is_open());
    assert_eq!(app.input_box.buffer.value(), "");

    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(actions.is_empty());
    assert!(!app.btw_modal.is_open());
    assert_eq!(app.btw_modal.cadence(), Cadence::IDLE);
}

#[test]
fn overlay_zone_click_gating() {
    let mut app = test_app();
    let msg = Rect::new(0, 0, 80, 15);
    let overlay = Rect::new(10, 3, 60, 10);
    set_zone(&mut app, SelectionZone::Messages, msg);
    set_zone(&mut app, SelectionZone::Overlay, overlay);
    app.hits.surface(crate::click::Surface::Help, overlay, true);
    app.help_modal.toggle();

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 5, 1));
    assert!(app.selection_state.is_none());

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 20, 5));
    let state = app.selection_state.as_ref().unwrap();
    assert_eq!(state.sel().zone, SelectionZone::Overlay);
}

fn streaming_app_with_history() -> App {
    with_history(streaming_app())
}

fn with_history(mut app: App) -> App {
    let history = vec![
        Message::user("hello".into()),
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: "world".into(),
            }],
            ..Default::default()
        },
    ];
    app.shared_history = Some(Arc::new(ArcSwap::from_pointee(
        makima_agent::HistorySnapshot::new(history),
    )));
    app
}

/// The stale event is dropped, yet the cancelled turn still reaches disk: the
/// next frame's checkpoint syncs the mirror whatever event arrived.
#[test_case(done() ; "stale_done")]
#[test_case(
    AgentEvent::ControlError { message: "timeout".into() } ; "stale_error"
)]
fn checkpoint_after_cancel_persists_the_cancelled_turn(event: AgentEvent) {
    let mut app = streaming_app_with_history();
    let old_run_id = app.run_id;
    cancel_app(&mut app);
    assert_ne!(app.run_id, old_run_id);
    assert!(app.state.session.messages().is_empty());

    app.update(agent_msg_with_run_id(event, old_run_id));
    app.checkpoint();
    assert_eq!(app.state.session.messages().len(), 2);
}

#[test]
fn parent_done_preserves_unresolved_children_and_reconciles_own_tools() {
    let mut app = streaming_app_with_history();
    app.update(agent_msg(AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: "task1".into(),
        tool: TASK_SPAWN_TOOL_NAME.into(),
        summary: "research".into(),
        annotation: None,
        input: None,
        raw_input: None,
        output: None,
        render_header: None,
    }))));
    app.update(subagent_msg(
        AgentEvent::ToolStart(Box::new(ToolStartEvent {
            id: "child-tool".into(),
            tool: "read".into(),
            summary: "reading".into(),
            annotation: None,
            input: None,
            raw_input: None,
            output: None,
            render_header: None,
        })),
        "task1",
        Some("research"),
    ));
    let buf = Arc::new(makima_agent::SharedBuf::new());
    app.update(subagent_msg(
        AgentEvent::LiveToolBuf {
            id: "child-tool".into(),
            body: buf,
        },
        "task1",
        None,
    ));

    app.update(done_event());

    assert!(!app.chats[1].is_finished());
    assert_eq!(app.chats[0].in_progress_count(), 0);
    assert_eq!(app.chats[1].in_progress_count(), 1);
    assert!(
        app.chats[0]
            .last_message_text()
            .contains(MISSING_TOOL_COMPLETION)
    );
    app.checkpoint();
    assert_eq!(app.state.session.subagents().len(), 1);
    assert_eq!(app.state.session.messages().len(), 2);
    assert!(app.state.session.tool_outputs().is_empty());
    assert_eq!(app.chat_index.get("task1"), Some(&1));
}

#[test]
fn parent_error_preserves_running_and_completed_children() {
    let mut app = streaming_app_with_history();
    app.update(subagent_msg_with_model(
        AgentEvent::TextDelta { text: "one".into() },
        "task1",
        "first",
        "model-a",
    ));
    finish_subagent(&mut app, "task1", false);
    app.update(subagent_msg_with_model(
        AgentEvent::TextDelta { text: "two".into() },
        "task2",
        "second",
        "model-b",
    ));
    app.update(subagent_msg_with_model(
        AgentEvent::TextDelta {
            text: "three".into(),
        },
        "task3",
        "third",
        "model-c",
    ));
    finish_subagent(&mut app, "task3", false);
    open_tasks_picker(&mut app);

    app.update(agent_msg(AgentEvent::ControlError {
        message: "boom".into(),
    }));

    assert!(app.task_picker.is_open());
    let running = app
        .task_picker
        .items()
        .unwrap()
        .iter()
        .find(|entry| entry.chat_index == 2)
        .unwrap();
    assert_eq!(running.finished, Some(false));
    app.checkpoint();
    let saved: Vec<_> = app
        .state
        .session
        .subagents()
        .iter()
        .map(|subagent| {
            (
                subagent.tool_use_id.as_str(),
                subagent.name.as_str(),
                subagent.model.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        saved,
        vec![
            ("task1", "first", Some("model-a")),
            ("task2", "second", Some("model-b")),
            ("task3", "third", Some("model-c")),
        ]
    );
}

#[test]
fn reserved_shell_survives_parent_done_until_shell_done() {
    let mut app = streaming_app_with_history();
    let id = app.shell.reserve_for(None);

    app.update(done_event());
    assert!(app.shell.active_ids().contains(&id));

    app.handle_shell_event(shell::ShellEvent::Start {
        id: id.clone(),
        command: "true".into(),
    });
    assert_eq!(app.chats[0].in_progress_count(), 1);
    app.handle_shell_event(shell::ShellEvent::Done {
        id: id.clone(),
        command: "true".into(),
        output: String::new(),
        is_error: false,
        visible: false,
    });
    assert_eq!(app.chats[0].in_progress_count(), 0);
    assert!(!app.shell.active_ids().contains(&id));
}

#[test]
fn agent_error_preserves_shell_and_child_tools() {
    let mut app = streaming_app_with_history();
    let shell_id = app.shell.reserve_for(None);
    app.handle_shell_event(shell::ShellEvent::Start {
        id: shell_id.clone(),
        command: "true".into(),
    });
    app.update(agent_msg(AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: "agent-tool".into(),
        tool: "read".into(),
        summary: "reading".into(),
        annotation: None,
        input: None,
        raw_input: None,
        output: None,
        render_header: None,
    }))));
    app.update(subagent_msg(
        AgentEvent::ToolStart(Box::new(ToolStartEvent {
            id: "child-tool".into(),
            tool: "read".into(),
            summary: "reading".into(),
            annotation: None,
            input: None,
            raw_input: None,
            output: None,
            render_header: None,
        })),
        "task1",
        Some("research"),
    ));

    app.update(agent_msg(AgentEvent::ControlError {
        message: "provider overloaded".into(),
    }));

    assert_eq!(app.chats[0].in_progress_count(), 1);
    assert_eq!(app.chats[1].in_progress_count(), 1);
    assert!(!app.chats[1].is_finished());

    app.handle_shell_event(shell::ShellEvent::Done {
        id: shell_id.clone(),
        command: "true".into(),
        output: String::new(),
        is_error: false,
        visible: false,
    });
    assert_eq!(app.chats[0].in_progress_count(), 0);
    assert!(!app.shell.active_ids().contains(&shell_id));
}

#[test]
fn parent_done_preserves_shell_and_same_id_child_tool() {
    let mut app = streaming_app_with_history();
    let id = app.shell.reserve_for(None);
    app.handle_shell_event(shell::ShellEvent::Start {
        id: id.clone(),
        command: "true".into(),
    });
    app.update(subagent_msg(
        AgentEvent::ToolStart(Box::new(ToolStartEvent {
            id: id.clone(),
            tool: "read".into(),
            summary: "reading".into(),
            annotation: None,
            input: None,
            raw_input: None,
            output: None,
            render_header: None,
        })),
        "task1",
        Some("research"),
    ));

    app.update(done_event());

    assert_eq!(app.chats[0].in_progress_count(), 1);
    assert_eq!(app.chats[1].in_progress_count(), 1);
    assert!(!app.chats[1].is_finished());
}

#[test]
fn error_event_matching_run_id_saves_session_and_queued_messages() {
    let mut app = with_history(idle_busy_app());
    app.queue_and_notify(queued_msg("next"));

    app.update(agent_msg(AgentEvent::ControlError {
        message: "boom".into(),
    }));
    app.checkpoint();

    assert_eq!(app.state.session.messages().len(), 2);
    assert_eq!(app.state.session.meta.queued_messages, ["next"]);
    assert!(app.queue.is_empty());

    assert_eq!(app.state.session.meta.queued_messages, ["next"]);

    type_and_submit(&mut app, "replacement");
    app.checkpoint();
    assert!(app.state.session.meta.queued_messages.is_empty());
}

#[test_case(false; "success")]
#[test_case(true; "failure")]
fn standalone_compaction_preserves_error_recovery_text_in_checkpoint(failed: bool) {
    const PENDING: &str = "pending user message";
    const SUMMARY: &str = "compacted conversation";
    const FAILURE: &str = "root failure";
    let mut app = with_history(idle_busy_app());
    app.queue_and_notify(queued_msg(PENDING));
    app.update(agent_msg_with_run_id(
        AgentEvent::ControlError {
            message: FAILURE.into(),
        },
        app.run_id,
    ));
    assert!(app.holds_recovery_text());
    app.checkpoint();
    assert_eq!(app.state.session.meta.queued_messages, [PENDING]);

    let presentation = app.queue.reserve_run_id(app.run_id);
    let identity = makima_agent::ActorWorkIdentity::new(
        AgentId::generate(),
        makima_agent::ActorWorkKind::Compact {
            operation_id: presentation,
        },
        presentation,
    );
    for event in [
        AgentEvent::ModelSwitched {
            spec: test_model().spec(),
            context_window: test_model().context_window,
        },
        AgentEvent::TextDelta {
            text: SUMMARY.into(),
        },
        if failed {
            AgentEvent::ControlError {
                message: FAILURE.into(),
            }
        } else {
            AgentEvent::ControlComplete {
                usage: TokenUsage::default(),
            }
        },
    ] {
        app.update(Msg::Agent(Box::new(Envelope {
            event,
            subagent: None,
            identity: Some(identity),
        })));
        assert!(app.holds_recovery_text());
        app.checkpoint();
        assert_eq!(app.state.session.meta.queued_messages, [PENDING]);
    }
    assert_eq!(app.run_id, presentation);
    if failed {
        assert!(matches!(&app.status, Status::Error { message, .. } if message == FAILURE));
    } else {
        assert_eq!(app.status, Status::Idle);
        assert_actor_response(&mut app, SUMMARY);
    }
}

#[test]
fn flush_restored_queue_drops_recovery_snapshot() {
    let mut app = with_history(idle_busy_app());
    app.queue_and_notify(queued_msg("next"));
    app.update(agent_msg(AgentEvent::ControlError {
        message: "boom".into(),
    }));
    app.checkpoint();
    assert_eq!(app.state.session.meta.queued_messages, ["next"]);

    app.flush_restored_queue();
    app.checkpoint();
    assert_eq!(app.state.session.meta.queued_messages, ["next"]);
    assert!(app.recoverable_queue.is_empty());

    app.queue.clear_root_user_work();
    app.checkpoint();
    assert!(app.state.session.meta.queued_messages.is_empty());
}

// --- Plan form integration tests ---

fn model_selection(
    spec: Option<&str>,
    thinking: Option<ThinkingConfig>,
    fast: Option<bool>,
) -> ModelSelection {
    ModelSelection {
        spec: spec.map(Arc::from),
        thinking,
        fast,
    }
}

fn configure_model_options(
    target: &AgentRef,
    spec: &str,
    thinking: ThinkingConfig,
    fast: bool,
    supported: bool,
) {
    let mut model = Model::from_spec(spec).unwrap();
    model.thinking_support = Some(if supported {
        ThinkingSupport::Yes
    } else {
        ThinkingSupport::No
    });
    model.fast_support = Some(if supported {
        FastSupport::Supported
    } else {
        FastSupport::Unsupported
    });
    smol::block_on(
        target
            .actor()
            .unwrap()
            .reserve_prepared_operation(None)
            .unwrap()
            .commit_when_ready(Some(makima_agent::actor::ConfigChange::Patch(
                makima_agent::actor::ConfigPatch {
                    model: Some(makima_agent::actor::PreparedModel {
                        provider: cli_tool_ctx(std::env::current_dir().unwrap()).provider,
                        model,
                    }),
                    thinking: Some(thinking),
                    fast: Some(fast),
                    ..Default::default()
                },
            ))),
    )
    .unwrap();
}

fn commit_target_plan_path(app: &mut App, target: &AgentRef, path: &str, root: bool) {
    let actor = target.actor().unwrap();
    let previous = target.effective_config().unwrap().unwrap().mode.clone();
    let mode = AgentMode::Plan(path.into());
    smol::block_on(
        actor
            .reserve_prepared_operation(None)
            .unwrap()
            .commit_when_ready(Some(makima_agent::actor::ConfigChange::Mode {
                mode: mode.clone(),
                mode_def: makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Plan),
            })),
    )
    .unwrap();
    if root {
        app.project_root_mode(&previous, &mode);
    }
}

/// Focuses chat `index` and binds commands to `target`, the way switching
/// chats does.
fn focus_chat(app: &mut App, index: usize, target: &AgentRef) {
    app.active_chat = index;
    bind_actor_target(app, target.clone());
    app.sync_focused_queue();
}

fn sync_child_plan(app: &mut App, child: &AgentRef) {
    app.sync_child_plan_paths(|id| {
        assert_eq!(id, child.id());
        child
            .effective_config()
            .unwrap()
            .unwrap()
            .mode
            .plan_path()
            .map(Path::to_path_buf)
    });
}

struct StagedForms {
    child: AgentRef,
    _child_input: flume::Receiver<AgentInput>,
    root_stage: ModelSelection,
    child_stage: ModelSelection,
}

/// Stages different selections, each with parallel on, on the root's plan
/// form and on a planning child's, and leaves the child focused.
fn stage_root_and_child_forms(app: &mut App) -> StagedForms {
    app.sync_focused_queue();
    let root_stage = model_selection(None, Some(ThinkingConfig::Off), Some(false));
    app.plan_form
        .set_implementation_selection(root_stage.clone());
    app.update(Msg::Key(key(KeyCode::Char(' '))));
    let (child, child_input) = spawn_input_child(app, TASK_ID, SUBAGENT_NAME);
    configure_model_options(
        &child,
        PLAN_OPTIONS_OTHER,
        ThinkingConfig::Effort(Effort::High),
        true,
        true,
    );
    commit_target_plan_path(app, &child, PLAN_OPTIONS_CHILD_PATH, false);
    focus_chat(app, 1, &child);
    app.plan_form.on_plan_ready();
    let child_stage = model_selection(None, Some(ThinkingConfig::Adaptive), Some(true));
    app.plan_form
        .set_implementation_selection(child_stage.clone());
    app.update(Msg::Key(key(KeyCode::Char(' '))));
    StagedForms {
        child,
        _child_input: child_input,
        root_stage,
        child_stage,
    }
}

fn model_options_app(child: bool, plan: bool) -> (TempDir, App, AgentRef) {
    let (tmp, _dir, _writer, mut app) = tempdir_app();
    let root = app
        .command_runtime
        .target_agent(&app.command_target)
        .unwrap();
    configure_model_options(&root, PLAN_OPTIONS_MODEL, ThinkingConfig::Off, false, true);
    if plan {
        set_root_mode(&mut app, Mode::Plan);
        app.state.plan = PlanState::Ready(PLAN_OPTIONS_PATH.into());
        app.plan_form.on_plan_ready();
    }
    let target = if child {
        let (target, _rx) = spawn_input_child_with_mode(
            &mut app,
            TASK_ID,
            SUBAGENT_NAME,
            if plan {
                AgentMode::Plan(PLAN_OPTIONS_CHILD_PATH.into())
            } else {
                AgentMode::Build
            },
        );
        configure_model_options(
            &target,
            PLAN_OPTIONS_OTHER,
            ThinkingConfig::Effort(Effort::High),
            true,
            true,
        );
        focus_chat(&mut app, 1, &target);
        if plan {
            app.plan_form.on_plan_ready();
        }
        target
    } else {
        root
    };
    let models = install_model_slot(&mut app);
    models.store(Some(Arc::new(vec![
        PLAN_OPTIONS_MODEL.into(),
        PLAN_OPTIONS_OTHER.into(),
    ])));
    (tmp, app, target)
}

fn edit_picker_options(app: &mut App, thinking: &str, fast: bool) {
    assert!(app.update(Msg::Key(key(KeyCode::Tab))).is_empty());
    assert!(app.update(Msg::Paste(thinking.into())).is_empty());
    assert!(app.update(Msg::Key(key(KeyCode::Tab))).is_empty());
    assert!(
        app.update(Msg::Key(key(if fast {
            KeyCode::Right
        } else {
            KeyCode::Left
        })))
        .is_empty()
    );
}

fn model_persistence(app: &App) -> serde_json::Value {
    serde_json::json!({
        "model": makima_storage::model::read_model(&app.storage),
        "recents": makima_storage::model::read_recents(&app.storage),
        "prefs": makima_storage::sessions::read_prefs(&app.storage),
    })
}

fn plan_app() -> App {
    let mut app = streaming_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting(PathBuf::from("test-plan.md"));
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t1".into(),
        tool: "write".into(),
        output: ToolOutput::Plain("wrote 42 bytes to test-plan.md".into()),
        is_error: false,
        annotation: None,
        written_path: Some("test-plan.md".into()),
    }))));
    app.update(done_event());
    app
}

#[test_case(Mode::Plan,  true  ; "plan_mode_tooldone_opens_form")]
#[test_case(Mode::Build, false ; "build_mode_tooldone_no_form")]
fn tool_done_write_opens_plan_form(mode: Mode, expect_form: bool) {
    let mut app = streaming_app();
    set_root_mode(&mut app, mode);
    app.state.plan = PlanState::Drafting(PathBuf::from("/tmp/plans/test.md"));
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t1".into(),
        tool: "write".into(),
        output: ToolOutput::Plain("wrote 42 bytes to /tmp/plans/test.md".into()),
        is_error: false,
        annotation: None,
        written_path: Some("/tmp/plans/test.md".into()),
    }))));
    assert_eq!(app.plan_form.is_visible(), expect_form);
    if expect_form {
        assert!(app.state.plan.is_ready());
    }
}

#[test]
fn done_event_does_not_open_plan_form() {
    let mut app = streaming_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Ready(PathBuf::from("test-plan.md"));
    app.update(done_event());
    assert!(!app.plan_form.is_visible());
}

#[test]
fn re_edit_keeps_plan_form_visible() {
    let mut app = plan_app();
    assert!(app.state.plan.is_ready());
    assert!(app.plan_form.is_visible());

    // Agent edits the plan again (second write to same path) — idempotent, stays Ready
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t2".into(),
        tool: "write".into(),
        output: ToolOutput::Plain("wrote 50 bytes to test-plan.md".into()),
        is_error: false,
        annotation: None,
        written_path: Some("test-plan.md".into()),
    }))));
    assert!(matches!(app.state.plan, PlanState::Ready(_)));
    assert!(app.plan_form.is_visible());
}

#[test]
fn plan_submit_builtin_shows_form_marks_ready_pushes_message() {
    let dir = TempDir::new().unwrap();
    let plan_path = dir.path().join("plan.md");
    const PLAN_BODY: &str = "# My Plan\n\n- Step 1";
    std::fs::write(&plan_path, PLAN_BODY).unwrap();

    let mut app = test_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting(plan_path);

    let actions = app.run_builtin(BuiltinAction::PlanSubmit);
    assert!(actions.is_empty());
    assert!(app.plan_form.is_visible());
    assert!(app.state.plan.is_ready());
    assert_eq!(app.main_chat().last_message_text(), PLAN_BODY);
    assert!(
        !app.state
            .session
            .messages()
            .iter()
            .any(|m| m.content.iter().any(|c| matches!(
                c,
                ContentBlock::Text { text } if text.contains(PLAN_BODY)
            ))),
        "plan body must not enter the model session context (display-only)"
    );
}

#[test]
fn plan_submit_builtin_empty_file_does_not_show_form() {
    let dir = TempDir::new().unwrap();
    let plan_path = dir.path().join("plan.md");
    std::fs::write(&plan_path, "").unwrap();

    let mut app = test_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting(plan_path);

    assert!(app.run_builtin(BuiltinAction::PlanSubmit).is_empty());
    assert!(!app.plan_form.is_visible());
    assert!(!app.state.plan.is_ready());
    assert!(app.status_bar.flash_text().is_some());
}

#[test_case(None, true, &[], &[], false; "inherited_registered")]
#[test_case(None, false, &[], &[], true; "inherited_absent")]
#[test_case(None, true, &["write"], &[], true; "inherited_allowlist_excluded")]
#[test_case(None, true, &[], &[PLAN_SUBMIT_TOOL], true; "inherited_disabled")]
#[test_case(Some(vec![PLAN_SUBMIT_TOOL]), true, &[], &[], false; "explicit_registered")]
#[test_case(Some(vec![PLAN_SUBMIT_TOOL]), false, &[], &[], true; "explicit_absent")]
#[test_case(Some(vec![PLAN_SUBMIT_TOOL]), true, &[], &[PLAN_SUBMIT_TOOL], true; "explicit_disabled")]
#[test_case(Some(vec!["write"]), true, &[], &[], true; "explicit_excluded")]
#[test_case(Some(vec![]), true, &[], &[], true; "explicit_empty")]
fn plan_submit_availability_gates_auto_open(
    tools: Option<Vec<&str>>,
    registered: bool,
    allowed_tools: &[&str],
    disabled_tools: &[&str],
    expect_form: bool,
) {
    let host = makima_lua::PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    if registered {
        host.load_source(
            "plan_submit_tool",
            include_str!("../../../plugins/plan_submit_tool/init.lua"),
        )
        .unwrap();
    }
    host.mode_registry()
        .define(ModeDefSpec {
            name: "plan".into(),
            tools: tools.map(|tools| tools.into_iter().map(String::from).collect()),
            ..Default::default()
        })
        .unwrap();

    let mut app = streaming_app();
    app.tool_filter = ToolFilter::from_config(
        &AgentConfig {
            allowed_tools: allowed_tools.iter().map(|name| (*name).into()).collect(),
            disabled_tools: disabled_tools.iter().map(|name| (*name).into()).collect(),
            ..Default::default()
        },
        &app.root_config().model,
        &[],
    );
    app.lua_event_handle = host.event_handle();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting(PathBuf::from("/tmp/plans/test.md"));

    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t1".into(),
        tool: "write".into(),
        output: ToolOutput::Plain("wrote 42 bytes to /tmp/plans/test.md".into()),
        is_error: false,
        annotation: None,
        written_path: Some("/tmp/plans/test.md".into()),
    }))));
    assert_eq!(app.plan_form.is_visible(), expect_form);
    assert_eq!(app.state.plan.is_ready(), expect_form);
}

#[test_case(1, true ; "clear_and_implement")]
#[test_case(2, false ; "implement_keeps_context")]
fn plan_form_menu_options(downs: usize, has_new_session: bool) {
    let mut app = plan_app();
    assert!(app.plan_form.is_visible());

    for _ in 0..downs {
        app.update(Msg::Key(key(KeyCode::Down)));
    }
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.plan_form.is_visible());
    assert_eq!(app.root_mode(), Mode::Plan);
    assert!(app.state.plan.is_ready());
    assert!(app.plan_approval_pending);
    assert!(matches!(actions.as_slice(), [Action::ApprovePlan {
        clear_context, selection, parallel: false, path,
    }] if *clear_context == has_new_session && selection.is_empty() && path == Path::new("test-plan.md")));
}

#[test]
fn plan_form_implement_toggled_parallel() {
    let mut app = plan_app();
    app.update(Msg::Key(key(KeyCode::Char(' '))));
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Down)));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(
        actions.as_slice(),
        [Action::ApprovePlan {
            clear_context: false,
            parallel: true,
            ..
        }]
    ));
}

#[test_case(false, false, "high", ThinkingConfig::Effort(Effort::High), true ; "root_session_enabled")]
#[test_case(false, true, "high", ThinkingConfig::Effort(Effort::High), true ; "root_plan_enabled")]
#[test_case(true, false, "off", ThinkingConfig::Off, false ; "child_session_disabled")]
#[test_case(true, true, "off", ThinkingConfig::Off, false ; "child_plan_disabled")]
fn shared_model_picker_applies_or_stages_same_selection(
    child: bool,
    plan: bool,
    value: &str,
    thinking: ThinkingConfig,
    fast: bool,
) {
    let (_tmp, mut app, target) = model_options_app(child, plan);
    let config = target.effective_config().unwrap().unwrap();
    let original = config.model_selection();
    let root = app.root_config().model_selection();
    let persisted = model_persistence(&app);
    assert!(matches!(
        app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()))
            .as_slice(),
        [Action::RefreshModels]
    ));
    edit_picker_options(&mut app, value, fast);
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    let selection = model_selection(None, Some(thinking), Some(fast));
    if plan {
        assert!(actions.is_empty());
        let staged = model_selection(None, Some(thinking), Some(fast));
        assert_eq!(app.plan_form.implementation_selection(), &staged);
        assert!(matches!(
            app.implement_plan(false).as_slice(),
            [Action::ApprovePlan { selection, .. }] if selection == &staged
        ));
    } else {
        assert!(matches!(
            actions.as_slice(),
            [Action::SelectPinnedModel { target: selected_target, selection: selected }]
                if selected_target.id() == target.id() && selected_target.same_manager(&target) && selected == &selection
        ));
        assert!(app.plan_form.implementation_selection().is_empty());
    }
    assert!(!app.model_picker.is_open());
    assert!(app.model_picker_purpose == ModelPickerPurpose::Session);
    assert_eq!(
        target
            .effective_config()
            .unwrap()
            .unwrap()
            .model_selection(),
        original
    );
    assert_eq!(app.root_config().model_selection(), root);
    assert_eq!(model_persistence(&app), persisted);
}

#[test_case(false, key(KeyCode::Esc) ; "session_escape")]
#[test_case(false, kb::QUIT.to_key_event() ; "session_ctrl_c")]
#[test_case(true, key(KeyCode::Esc) ; "plan_escape")]
#[test_case(true, kb::QUIT.to_key_event() ; "plan_ctrl_c")]
fn shared_model_picker_cancel_preserves_actor_and_storage(plan: bool, cancel: KeyEvent) {
    let (_tmp, mut app, target) = model_options_app(false, plan);
    let original = target
        .effective_config()
        .unwrap()
        .unwrap()
        .model_selection();
    let persisted = model_persistence(&app);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    edit_picker_options(&mut app, "adaptive", true);
    assert!(app.update(Msg::Key(cancel)).is_empty());
    assert_eq!(
        target
            .effective_config()
            .unwrap()
            .unwrap()
            .model_selection(),
        original
    );
    assert_eq!(model_persistence(&app), persisted);
    assert!(app.plan_form.implementation_selection().is_empty());
    assert!(!app.model_picker.is_open());
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    if plan {
        assert!(actions.is_empty());
        assert!(app.plan_form.implementation_selection().is_empty());
    } else {
        assert!(matches!(
            actions.as_slice(),
            [Action::SelectPinnedModel { selection, .. }]
                if selection.is_empty()
        ));
    }
}

#[test_case(false, key(KeyCode::Esc) ; "options_escape")]
#[test_case(false, kb::QUIT.to_key_event() ; "options_ctrl_c")]
#[test_case(true, key(KeyCode::Esc) ; "undiscovered_escape")]
#[test_case(true, kb::QUIT.to_key_event() ; "undiscovered_ctrl_c")]
fn plan_picker_cancel_retains_staged_options(undiscovered: bool, cancel: KeyEvent) {
    let (_tmp, mut app, target) = model_options_app(false, true);
    let staged = model_selection(
        undiscovered.then_some(PLAN_OPTIONS_LONG_MODEL),
        Some(ThinkingConfig::Off),
        Some(false),
    );
    let config = target.effective_config().unwrap().unwrap();
    let original = config.model_selection();
    app.plan_form.set_implementation_selection(staged.clone());
    let persisted = model_persistence(&app);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    app.update(Msg::Key(key(KeyCode::Tab)));
    app.update(Msg::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::CONTROL,
    )));
    app.update(Msg::Paste("adaptive".into()));
    app.update(Msg::Key(key(KeyCode::Tab)));
    app.update(Msg::Key(key(KeyCode::Right)));
    assert!(app.update(Msg::Key(cancel)).is_empty());
    assert_eq!(app.plan_form.implementation_selection(), &staged);
    assert_eq!(
        target
            .effective_config()
            .unwrap()
            .unwrap()
            .model_selection(),
        original
    );
    assert_eq!(model_persistence(&app), persisted);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    assert_eq!(
        app.model_picker.highlighted_spec(),
        Some(staged.spec.as_deref().unwrap_or(&original.spec))
    );
    assert!(app.update(Msg::Key(key(KeyCode::Enter))).is_empty());
    assert_eq!(app.plan_form.implementation_selection(), &staged);
}

#[test]
fn plan_picker_target_change_discards_draft() {
    let (_tmp, mut app, root) = model_options_app(false, true);
    let StagedForms {
        child,
        root_stage,
        child_stage,
        ..
    } = stage_root_and_child_forms(&mut app);
    focus_chat(&mut app, 0, &root);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    edit_picker_options(&mut app, "high", true);
    focus_chat(&mut app, 1, &child);
    assert!(!app.model_picker.is_open());
    assert_eq!(app.plan_form.implementation_selection(), &child_stage);
    focus_chat(&mut app, 0, &root);
    assert_eq!(app.plan_form.implementation_selection(), &root_stage);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    assert!(app.update(Msg::Key(key(KeyCode::Enter))).is_empty());
    assert_eq!(app.plan_form.implementation_selection(), &root_stage);
}

#[test]
fn child_plan_stage_survives_focus_before_command_target_rebind() {
    let (_tmp, mut app, root) = model_options_app(false, true);
    let StagedForms {
        child,
        root_stage,
        child_stage,
        ..
    } = stage_root_and_child_forms(&mut app);
    app.plan_approval_pending = true;
    focus_chat(&mut app, 0, &root);
    assert_eq!(app.plan_form.implementation_selection(), &root_stage);
    app.active_chat = 1;
    app.sync_focused_queue();
    assert!(app.focused_config().is_none());
    assert_eq!(app.plan_form.implementation_selection(), &child_stage);
    assert!(app.plan_form.is_visible());
    assert!(app.plan_form.parallel());
    assert!(app.plan_approval_pending);
    bind_actor_target(&app, child.clone());
    sync_child_plan(&mut app, &child);
    app.sync_focused_queue();
    assert_eq!(app.plan_form.implementation_selection(), &child_stage);
    assert!(app.plan_form.parallel());
    assert!(app.plan_approval_pending);
}

#[test_case(false, false ; "root_active")]
#[test_case(false, true ; "root_saved")]
#[test_case(true, false ; "child_active")]
#[test_case(true, true ; "child_saved")]
fn replaced_plan_path_discards_only_owning_form_stage(child_owner: bool, saved: bool) {
    let (_tmp, mut app, root) = model_options_app(false, true);
    let StagedForms {
        child,
        root_stage,
        child_stage,
        ..
    } = stage_root_and_child_forms(&mut app);
    let owner_index = usize::from(child_owner);
    let other_index = usize::from(!child_owner);
    let owner = if child_owner { &child } else { &root };
    let other = if child_owner { &root } else { &child };
    let owner_stage = if child_owner {
        &child_stage
    } else {
        &root_stage
    };
    let other_stage = if child_owner {
        &root_stage
    } else {
        &child_stage
    };
    focus_chat(&mut app, owner_index, owner);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    edit_picker_options(&mut app, "high", true);
    app.plan_approval_pending = true;
    if saved {
        focus_chat(&mut app, other_index, other);
        assert!(!app.model_picker.is_open());
        app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
        assert!(app.model_picker.is_open());
    }
    commit_target_plan_path(&mut app, owner, PLAN_OPTIONS_REPLACEMENT_PATH, !child_owner);
    if child_owner {
        sync_child_plan(&mut app, &child);
    }
    app.sync_focused_queue();
    assert!(app.plan_form.parallel());
    if saved {
        assert!(app.model_picker.is_open());
        assert_eq!(app.plan_form.implementation_selection(), other_stage);
        assert!(!app.plan_approval_pending);
        app.update(Msg::Key(key(KeyCode::Esc)));
        let (form, pending) = app
            .plan_forms
            .get(&child_owner.then_some(child.id()))
            .unwrap();
        assert!(form.implementation_selection().is_empty());
        assert!(!form.is_visible());
        assert!(form.parallel());
        assert!(!pending);
        focus_chat(&mut app, owner_index, owner);
    } else {
        assert!(!app.model_picker.is_open());
    }
    assert!(app.plan_form.implementation_selection().is_empty());
    assert!(!app.plan_form.is_visible());
    assert!(app.plan_form.parallel());
    assert!(!app.plan_approval_pending);
    assert_eq!(
        app.focused_plan_path(),
        Some(PLAN_OPTIONS_REPLACEMENT_PATH.into())
    );
    app.plan_form.on_plan_ready();
    assert!(app.plan_form.is_visible());
    assert!(app.plan_form.implementation_selection().is_empty());
    focus_chat(&mut app, other_index, other);
    assert_eq!(app.plan_form.implementation_selection(), other_stage);
    assert_ne!(app.plan_form.implementation_selection(), owner_stage);
    assert!(app.plan_form.is_visible());
    assert!(app.plan_form.parallel());
}

#[test]
fn ordinary_model_picker_keeps_owner_after_focus_change() {
    let (_tmp, mut app, child) = model_options_app(true, false);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    edit_picker_options(&mut app, "off", false);
    app.active_chat = 0;
    app.sync_focused_queue();
    assert!(app.model_picker.is_open());
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(
        actions.as_slice(),
        [Action::SelectPinnedModel { target, selection }]
            if target.id() == child.id() && target.same_manager(&child)
                && selection == &model_selection(None, Some(ThinkingConfig::Off), Some(false))
    ));
    assert_eq!(
        app.root_config().model_selection().spec.as_ref(),
        PLAN_OPTIONS_MODEL
    );
}

#[test_case(false, false ; "plan_retired")]
#[test_case(true, false ; "child_plan_retired")]
#[test_case(true, true ; "child_closed")]
fn plan_picker_retirement_discards_open_draft(child: bool, closed: bool) {
    let (_tmp, mut app, target) = model_options_app(child, true);
    let staged = model_selection(None, Some(ThinkingConfig::Off), Some(false));
    app.plan_form.set_implementation_selection(staged);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    edit_picker_options(&mut app, "adaptive", true);
    if closed {
        target.close_subtree().unwrap();
        assert!(target.validate_live().is_err());
        app.update(Msg::Agent(Box::new(Envelope {
            event: AgentEvent::SubagentClosed,
            subagent: Some(subagent_info_for_agent(
                target.id(),
                TASK_ID,
                SUBAGENT_NAME,
                None,
                None,
            )),
            identity: Some(work_identity(1)),
        })));
        app.sync_focused_plan_form();
    } else if child {
        let mode_def = ModeDefSpec {
            name: "review".into(),
            restrict_write_to: Some(PLAN_OPTIONS_CHILD_PATH.into()),
            ..Default::default()
        }
        .build()
        .unwrap();
        smol::block_on(
            target
                .actor()
                .unwrap()
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(makima_agent::actor::ConfigChange::Mode {
                    mode: AgentMode::Custom(mode_def.id.clone()),
                    mode_def: Some(mode_def),
                })),
        )
        .unwrap();
        app.sync_focused_plan_form();
    } else {
        set_root_mode(&mut app, Mode::Build);
    }
    assert!(!app.model_picker.is_open());
    assert!(app.model_picker_purpose == ModelPickerPurpose::Session);
    assert!(app.plan_form.implementation_selection().is_empty());
    assert!(!app.plan_form.is_visible());
}

#[test_case(false ; "shortcut")]
#[test_case(true ; "model_row")]
fn plan_model_picker_stages_without_switching(row: bool) {
    // `test_app` shares `$TMPDIR` as its state dir with every other test
    // process, which rewrite the model, recents and prefs this test pins.
    let (_tmp, _dir, _writer, mut app) = tempdir_app();
    let models = install_model_slot(&mut app);
    const MODEL: &str = "ollama/glm-5";
    models.store(Some(Arc::new(vec![MODEL.into()])));
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Ready(PathBuf::from("test-plan.md"));
    app.plan_form.on_plan_ready();
    let original = app.root_config().model.spec();
    let persisted_model = makima_storage::model::read_model(&app.storage);
    let recents = makima_storage::model::read_recents(&app.storage);
    let prefs = serde_json::to_value(makima_storage::sessions::read_prefs(&app.storage)).unwrap();
    let actions = if row {
        for _ in 0..3 {
            app.update(Msg::Key(key(KeyCode::Down)));
        }
        app.update(Msg::Key(key(KeyCode::Enter)))
    } else {
        app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()))
    };
    assert!(matches!(actions.as_slice(), [Action::RefreshModels]));
    assert!(
        app.active_keybind_contexts()
            .contains(&KeybindContext::ModelPicker)
    );
    app.route_paste(MODEL.split_once('/').unwrap().1);
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(actions.is_empty());
    assert_eq!(
        app.plan_form.implementation_selection(),
        &model_selection(Some(MODEL), None, None)
    );
    assert_eq!(app.root_config().model.spec(), original);
    assert_eq!(
        makima_storage::model::read_model(&app.storage),
        persisted_model
    );
    assert_eq!(makima_storage::model::read_recents(&app.storage), recents);
    assert_eq!(
        serde_json::to_value(makima_storage::sessions::read_prefs(&app.storage)).unwrap(),
        prefs
    );
    assert!(!app.plan_approval_pending);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert_eq!(
        app.plan_form.implementation_selection(),
        &model_selection(Some(MODEL), None, None)
    );
    assert!(app.model_picker_purpose == ModelPickerPurpose::Session);
}

#[test_case(false ; "root")]
#[test_case(true ; "child")]
fn plan_model_chip_stages_without_switching(child: bool) {
    let (_tmp, mut app, target) = model_options_app(child, true);
    let original = target
        .effective_config()
        .unwrap()
        .unwrap()
        .model_selection();
    let root = app.root_config().model_selection();
    let persisted = model_persistence(&app);
    assert!(matches!(
        click_target(&mut app, ClickTarget::ModelChip).as_slice(),
        [Action::RefreshModels]
    ));
    assert!(app.model_picker.is_open());
    assert!(app.model_picker_purpose == ModelPickerPurpose::PlanImplementation);
    let selected = if child {
        PLAN_OPTIONS_MODEL
    } else {
        PLAN_OPTIONS_OTHER
    };
    app.route_paste(selected.split_once('/').unwrap().1);
    assert_eq!(app.model_picker.highlighted_spec(), Some(selected));
    assert!(click_target(&mut app, ClickTarget::KeyHint(key(KeyCode::Enter))).is_empty());
    assert_eq!(
        app.plan_form.implementation_selection(),
        &model_selection(Some(selected), None, None)
    );
    assert!(!app.model_picker.is_open());
    assert!(app.plan_form.is_visible());
    assert_eq!(
        target
            .effective_config()
            .unwrap()
            .unwrap()
            .model_selection(),
        original
    );
    assert_eq!(app.root_config().model_selection(), root);
    assert_eq!(model_persistence(&app), persisted);
}

#[test]
fn open_session_model_picker_keeps_keys_when_plan_form_appears() {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![LATE_MODEL_SPEC.into()])));
    set_root_mode(&mut app, Mode::Plan);
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    app.state.plan.mark_ready();
    app.plan_form.on_plan_ready();
    let contexts = app.active_keybind_contexts();
    assert!(contexts.contains(&KeybindContext::ModelPicker));
    assert!(!contexts.contains(&KeybindContext::FormInput));
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    assert!(app.model_picker_purpose == ModelPickerPurpose::Session);
    app.update(Msg::Paste(
        LATE_MODEL_SPEC.split_once('/').unwrap().1.into(),
    ));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(
        actions.as_slice(),
        [Action::SelectPinnedModel { selection, .. }]
            if selection == &model_selection(Some(LATE_MODEL_SPEC), None, None)
    ));
    assert!(app.plan_form.implementation_selection().is_empty());
    assert!(app.plan_form.is_visible());
}

enum BlockingSurface {
    Permission,
    Question,
    Help,
    Btw,
    Float,
}

#[test_case(BlockingSurface::Permission ; "permission")]
#[test_case(BlockingSurface::Question ; "question")]
#[test_case(BlockingSurface::Help ; "help")]
#[test_case(BlockingSurface::Btw ; "btw")]
#[test_case(BlockingSurface::Float ; "float")]
fn plan_picker_yields_to_blocking_overlay(surface: BlockingSurface) {
    const MODEL: &str = "zai/glm-5";
    let (_tmp, mut app, _target) = model_options_app(false, true);
    assert_ne!(app.root_config().model.spec(), MODEL);
    let models = install_model_slot(&mut app);
    models.store(Some(Arc::new(vec![MODEL.into()])));
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Ready(PathBuf::from("test-plan.md"));
    app.plan_form.on_plan_ready();
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    app.route_paste(MODEL.split_once('/').unwrap().1);
    assert_eq!(app.model_picker.highlighted_spec(), Some(MODEL));
    let (_btw_tx, btw_rx) = flume::bounded(1);
    let (event_tx, event_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (_cmd_tx, cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    let forwards_to_float = matches!(surface, BlockingSurface::Question | BlockingSurface::Float);
    match surface {
        BlockingSurface::Permission => {
            app.permission_prompt.open(
                "permission".into(),
                makima_config::ToolKey::native("bash"),
                vec!["execute".into()],
                None,
            );
            app.active_input = Some(InputKind::Permission);
        }
        BlockingSurface::Help => app.help_modal.toggle(),
        BlockingSurface::Btw => app.btw_modal.open("question", btw_rx),
        BlockingSurface::Question | BlockingSurface::Float => {
            let question = matches!(surface, BlockingSurface::Question);
            let config = makima_lua::FloatConfig {
                needs_input: question,
                ..makima_lua::FloatConfig::default()
            };
            app.float_mgr.open(
                Arc::new(makima_agent::SharedBuf::new()),
                config,
                true,
                event_tx,
                cmd_rx,
            );
            if question {
                app.active_input = Some(InputKind::Question);
            }
        }
    }
    let contexts = app.active_keybind_contexts();
    assert!(!contexts.contains(&KeybindContext::ModelPicker));
    assert!(!contexts.contains(&KeybindContext::FormInput));
    assert!(app.update(Msg::Key(key(KeyCode::Char('x')))).is_empty());
    app.route_paste("no-such-model");
    assert!(app.model_picker.is_open());
    assert!(app.plan_form.implementation_selection().is_empty());
    if forwards_to_float {
        assert!(event_rx.try_recv().is_ok());
    }
    app.help_modal.close();
    app.btw_modal.close();
    app.permission_prompt.close();
    app.float_mgr.close_all();
    app.active_input = None;
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(
        app.plan_form.implementation_selection(),
        &model_selection(Some(MODEL), None, None)
    );
}

#[test_case(false ; "help")]
#[test_case(true ; "btw")]
fn plan_form_yields_escape_to_blocking_modal(btw: bool) {
    let mut app = plan_app();
    let (_tx, rx) = flume::bounded(1);
    if btw {
        app.btw_modal.open("question", rx);
    } else {
        app.help_modal.toggle();
    }
    app.route_paste("blocked");
    assert!(app.update(Msg::Key(key(KeyCode::Esc))).is_empty());
    assert!(app.plan_form.is_visible());
    assert!(!app.help_modal.is_open());
    assert!(!app.btw_modal.is_open());
}

#[test_case("build" ; "build")]
#[test_case("custom" ; "custom")]
fn plan_mode_exit_clears_staging_and_picker(mode: &str) {
    let mut app = plan_app();
    app.plan_form
        .set_implementation_selection(model_selection(Some(LATE_MODEL_SPEC), None, None));
    app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
    set_root_mode(&mut app, Mode::from_id(mode));
    assert!(app.plan_form.implementation_selection().is_empty());
    assert!(!app.model_picker.is_open());
    assert!(app.model_picker_purpose == ModelPickerPurpose::Session);
}

#[test_case(false ; "session_root_fallback")]
#[test_case(true ; "plan_root_fallback")]
fn model_picker_without_target_initializes_from_root(plan: bool) {
    let (_tmp, _dir, _writer, mut app) = tempdir_app();
    let models = install_model_slot(&mut app);
    let root_model = app.root_config().model.spec();
    models.store(Some(Arc::new(vec![
        MODEL_SPEC_GLM4.into(),
        root_model.clone(),
    ])));
    app.open_model_picker(if plan {
        ModelPickerPurpose::PlanImplementation
    } else {
        ModelPickerPurpose::Session
    });
    assert!(matches!(
        app.model_picker.handle_key(key(KeyCode::Enter)),
        ModelPickerAction::Select(selection)
            if selection.is_empty()
    ));
}

#[test]
fn normal_model_picker_still_requests_model_change() {
    const MODEL: &str = "zai/glm-5";
    let (_tmp, mut app, _target) = model_options_app(false, false);
    assert_ne!(app.root_config().model.spec(), MODEL);
    let models = install_model_slot(&mut app);
    models.store(Some(Arc::new(vec![MODEL.into()])));
    app.run_builtin(BuiltinAction::ModelPicker);
    app.route_paste(MODEL.split_once('/').unwrap().1);
    assert_eq!(app.model_picker.highlighted_spec(), Some(MODEL));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(
        actions.as_slice(),
        [Action::SelectPinnedModel { selection, .. }]
            if selection == &model_selection(Some(MODEL), None, None)
    ));
    assert!(app.plan_form.implementation_selection().is_empty());
}

#[test]
fn plan_approval_pending_blocks_edits_and_requests_cancellation() {
    let mut app = plan_app();
    app.plan_form
        .set_implementation_selection(model_selection(Some(LATE_MODEL_SPEC), None, None));
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.plan_approval_pending);
    assert!(
        app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()))
            .is_empty()
    );
    assert!(!app.model_picker.is_open());
    assert!(app.update(Msg::Key(key(KeyCode::Enter))).is_empty());
    app.route_paste("blocked");
    assert!(matches!(
        app.update(Msg::Key(key(KeyCode::Esc))).as_slice(),
        [Action::CancelPlanApproval]
    ));
    assert!(app.plan_approval_pending);
    assert_eq!(
        app.plan_form.implementation_selection(),
        &model_selection(Some(LATE_MODEL_SPEC), None, None)
    );
    assert!(app.state.plan.is_ready());
}

fn rendered_plan_rows(app: &mut App) -> Vec<String> {
    let rows = rendered_rows(app, 100, 40);
    let area = app
        .hits
        .rect_of(ClickTarget::Surface(Surface::Plan))
        .unwrap();
    rows[usize::from(area.y)..usize::from(area.bottom())].to_vec()
}

#[test]
fn pending_plan_footer_blocks_space_and_requests_cancellation() {
    let mut app = plan_app();
    let staged = model_selection(Some(LATE_MODEL_SPEC), None, None);
    app.plan_form.set_implementation_selection(staged.clone());
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.plan_approval_pending);
    let before = rendered_plan_rows(&mut app);
    assert!(click_target(&mut app, ClickTarget::KeyHint(key(KeyCode::Char(' ')))).is_empty());
    assert!(!app.plan_form.parallel());
    assert_eq!(rendered_plan_rows(&mut app), before);
    assert!(click_target(&mut app, ClickTarget::ModelChip).is_empty());
    assert!(!app.model_picker.is_open());
    assert!(matches!(
        click_target(&mut app, ClickTarget::KeyHint(key(KeyCode::Esc))).as_slice(),
        [Action::CancelPlanApproval]
    ));
    assert!(app.plan_approval_pending);
    assert!(app.plan_form.is_visible());
    assert!(app.state.plan.is_ready());
    assert_eq!(app.plan_form.implementation_selection(), &staged);
}

#[test_case(0 ; "refine")]
#[test_case(2 ; "implement")]
#[test_case(3 ; "model")]
#[test_case(4 ; "use_current")]
fn pending_plan_row_cannot_change_selection_or_submit(row: u16) {
    let mut app = plan_app();
    app.plan_form
        .set_implementation_selection(model_selection(Some(LATE_MODEL_SPEC), None, None));
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.plan_approval_pending);
    let before = rendered_plan_rows(&mut app);
    assert!(click_target(&mut app, ClickTarget::PlanRow(row)).is_empty());
    assert_eq!(rendered_plan_rows(&mut app), before);
    assert!(app.plan_approval_pending);
    assert!(app.plan_form.is_visible());
    assert!(!app.model_picker.is_open());
}

#[test_case(false ; "plan_model_picker")]
#[test_case(true ; "approval_pending")]
fn image_path_paste_follows_plan_ownership(approval_pending: bool) {
    let mut app = plan_app();
    if approval_pending {
        app.update(Msg::Key(key(KeyCode::Down)));
        app.update(Msg::Key(key(KeyCode::Enter)));
        assert!(app.plan_approval_pending);
    } else {
        app.update(Msg::Key(kb::MODEL_PICKER.to_key_event()));
        assert!(app.model_picker.is_open());
    }
    app.update(Msg::Paste(IMAGE_PATH_PASTE.into()));
    assert!(app.image_paste_rx.is_empty());
    assert_eq!(app.input_box.buffer.value(), "");
}

#[test]
fn staged_model_matching_session_model_approves_without_override() {
    let mut app = plan_app();
    app.plan_form
        .set_implementation_selection(model_selection(Some(LATE_MODEL_SPEC), None, None));
    commit_root_model(&app, Model::from_spec(LATE_MODEL_SPEC).unwrap());
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Down)));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(
        actions.as_slice(),
        [Action::ApprovePlan { selection, .. }] if selection.is_empty()
    ));
}

#[test]
fn admitted_plan_run_uses_exact_run_id_without_submitting() {
    let (_tmp, mut app, target) = model_options_app(false, true);
    app.plan_approval_pending = true;
    let pending_submissions = root_submissions(&app);
    app.record_plan_implementation(
        42,
        "plan".into(),
        PLAN_OPTIONS_PATH.into(),
        "implement".into(),
    );
    app.finish_child_plan_approval(target.id());
    assert_eq!(root_submissions(&app), pending_submissions);
    assert_eq!(app.run_id, 42);
    assert_eq!(app.status, Status::Streaming);
    assert!(!app.plan_approval_pending);
    assert!(!app.plan_form.is_visible());
    assert_eq!(app.state.plan, PlanState::None);
}

#[test]
fn plan_form_open_editor() {
    let mut app = plan_app();

    let actions = app.update(Msg::Key(kb::OPEN_EDITOR.to_key_event()));
    assert!(app.plan_form.is_visible());
    assert!(matches!(&actions[..], [Action::OpenEditor(p)] if p == Path::new("test-plan.md")));
}

fn rewrite_plan(app: &mut App) {
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t2".into(),
        tool: "write".into(),
        output: ToolOutput::Plain("wrote 99 bytes to test-plan.md".into()),
        is_error: false,
        annotation: None,
        written_path: Some("test-plan.md".into()),
    }))));
}

fn dismiss_plan_esc(app: &mut App) {
    app.update(Msg::Key(key(KeyCode::Esc)));
}

#[test]
fn rewrite_does_not_reopen_after_dismiss() {
    let mut app = plan_app();
    assert!(app.plan_form.is_visible());

    dismiss_plan_esc(&mut app);
    assert!(!app.plan_form.is_visible());
    assert!(app.state.plan.is_ready());

    rewrite_plan(&mut app);
    assert!(!app.plan_form.is_visible());
    assert!(app.state.plan.is_ready());
}

#[test]
fn ctrl_t_toggles_plan_form_in_plan_mode() {
    let mut app = plan_app();
    assert!(app.plan_form.is_visible());

    app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event()));
    assert!(!app.plan_form.is_visible());

    app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event()));
    assert!(app.plan_form.is_visible());
}

#[test]
fn ctrl_t_noop_when_plan_not_ready() {
    let mut app = test_app();
    set_root_mode(&mut app, Mode::Plan);
    app.state.plan = PlanState::Drafting(PathBuf::from("test-plan.md"));
    assert!(!app.plan_form.is_visible());

    app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event()));
    assert!(!app.plan_form.is_visible());
}

fn install_override(
    app: &mut App,
    key: KeyCode,
    modifiers: KeyModifiers,
) -> makima_lua::test_support::RequestProbe {
    app.keymap_reader =
        makima_lua::test_support::keymap_reader_with(vec![makima_lua::KeymapEntry {
            key,
            modifiers,
            desc: "plugin override".into(),
            plugin: Arc::from("test-plugin"),
            id: OVERRIDE_ID,
        }]);
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;
    probe
}

const OVERRIDE_ID: u64 = 1;
const OVERRIDE_DISPATCHED: &str = "override callback must be dispatched";
const OVERRIDE_NOT_DISPATCHED: &str = "override callback must not be dispatched";

#[test]
fn override_shadows_builtin_ctrl_when_no_overlay_open() {
    let mut app = test_app();
    let _probe = install_override(&mut app, kb::HELP.code, kb::HELP.modifiers);

    let actions = app.update(Msg::Key(kb::HELP.to_key_event()));

    assert!(
        matches!(actions[..], [Action::RunKeybind(OVERRIDE_ID)]),
        "{OVERRIDE_DISPATCHED}"
    );
    assert!(
        !app.help_modal.is_open(),
        "override must consume the key before the built-in HELP handler runs"
    );
}

#[test]
fn override_shadows_quit_builtin() {
    let mut app = test_app();
    app.status = Status::Idle;
    let _probe = install_override(&mut app, kb::QUIT.code, kb::QUIT.modifiers);

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));

    assert!(
        matches!(actions[..], [Action::RunKeybind(OVERRIDE_ID)]),
        "{OVERRIDE_DISPATCHED}"
    );
    assert_eq!(
        app.exit_request,
        ExitRequest::None,
        "override must consume Ctrl+C before the built-in quit handler runs"
    );
}

#[test]
fn override_shadows_shift_tab_mode_toggle() {
    let mut app = test_app();
    let initial_mode = app.root_mode().clone();
    let _probe = install_override(&mut app, KeyCode::Tab, KeyModifiers::SHIFT);

    let actions = app.update(Msg::Key(key(KeyCode::BackTab)));

    assert!(
        matches!(actions[..], [Action::RunKeybind(OVERRIDE_ID)]),
        "{OVERRIDE_DISPATCHED}"
    );
    assert_eq!(
        app.root_mode(),
        initial_mode,
        "override must consume Shift+Tab before the built-in mode toggle runs"
    );
}

#[test]
fn override_shadows_esc_builtin() {
    let mut app = test_app();
    let _probe = install_override(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    let actions = app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(
        matches!(actions[..], [Action::RunKeybind(OVERRIDE_ID)]),
        "{OVERRIDE_DISPATCHED}"
    );
    assert!(
        app.last_esc.is_none(),
        "override must consume Esc before the built-in esc handler runs"
    );
}

#[cfg(unix)]
#[test]
fn override_does_not_shadow_suspend() {
    let mut app = test_app();
    let _probe = install_override(&mut app, kb::SUSPEND.code, kb::SUSPEND.modifiers);

    let actions = app.update(Msg::Key(kb::SUSPEND.to_key_event()));

    assert!(
        actions.iter().any(|a| matches!(a, Action::Suspend)),
        "suspend is non-remappable: override must not shadow Ctrl+Z"
    );
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::RunKeybind(_))),
        "{OVERRIDE_NOT_DISPATCHED}"
    );
}

#[test]
fn builtin_runs_when_no_override() {
    let mut app = test_app();

    app.update(Msg::Key(kb::HELP.to_key_event()));

    assert!(app.help_modal.is_open());
}

#[test]
fn plan_toggle_beats_override_when_open_and_after_dismiss() {
    let mut app = plan_app();
    let _probe = install_override(&mut app, kb::PLAN_TOGGLE.code, kb::PLAN_TOGGLE.modifiers);
    assert!(app.plan_form.is_visible());

    let mut actions = app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event()));
    assert!(
        !app.plan_form.is_visible(),
        "open plan form must consume Ctrl+T before the override"
    );

    actions.extend(app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event())));
    assert!(
        app.plan_form.is_visible(),
        "Ctrl+T must reopen the dismissed plan form despite the override"
    );
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::RunKeybind(_))),
        "{OVERRIDE_NOT_DISPATCHED}"
    );
}

#[test]
fn streaming_cancel_wins_over_quit_override() {
    let mut app = streaming_app();
    let _probe = install_override(&mut app, kb::QUIT.code, kb::QUIT.modifiers);

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));

    assert!(
        matches!(&actions[0], Action::CancelAgent { .. }),
        "built-in cancel must win while streaming even when Ctrl+C is overridden"
    );
    assert_eq!(app.status, Status::Idle);
    assert_eq!(app.exit_request, ExitRequest::None);
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::RunKeybind(_))),
        "{OVERRIDE_NOT_DISPATCHED}"
    );
}

#[test]
fn dead_host_override_falls_back_to_builtin() {
    let mut app = test_app();
    let _probe = install_override(&mut app, kb::HELP.code, kb::HELP.modifiers);
    app.lua_event_handle = makima_lua::EventHandle::disconnected_for_test();

    app.update(Msg::Key(kb::HELP.to_key_event()));

    assert!(
        app.help_modal.is_open(),
        "dead lua host must fall back to the built-in HELP handler"
    );
}

#[test]
fn streaming_cancel_wins_over_esc_override() {
    let mut app = streaming_app();
    app.status_bar.flash_duration = Duration::from_secs(3600);
    app.last_esc = Some(Instant::now());
    let _probe = install_override(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    let actions = app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(
        matches!(&actions[0], Action::CancelAgent { .. }),
        "built-in cancel must win while streaming even when Esc is overridden"
    );
    assert_eq!(app.status, Status::Idle);
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, Action::RunKeybind(_))),
        "{OVERRIDE_NOT_DISPATCHED}"
    );
}

#[test]
fn reset_session_request_keeps_plan_form_until_commit() {
    let mut app = plan_app();
    assert!(app.plan_form.is_visible());

    app.reset_session();
    assert!(app.plan_form.is_visible());
}

#[test]
fn ctrl_c_closes_overlay_instead_of_quitting() {
    let mut app = test_app();
    app.help_modal.toggle();
    assert!(app.help_modal.is_open());

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));
    assert_eq!(app.exit_request, ExitRequest::None);
    assert!(!app.help_modal.is_open());
    assert!(actions.is_empty());
}

#[test]
fn bash_prefix_overrides_mode() {
    let mut app = test_app();

    app.input_box.set_input("! ls".into());
    assert_eq!(&*app.mode_label().0, "[BASH]");

    app.update(Msg::Key(key(KeyCode::BackTab)));
    assert_eq!(
        app.root_mode(),
        Mode::Build,
        "shift+tab must not toggle while bash prefix present"
    );

    app.input_box.set_input("ls".into());
    assert_eq!(&*app.mode_label().0, "[BUILD]");
}

#[test]
fn model_switch_updates_active_run_status() {
    const SWITCHED_MODEL_SPEC: &str = "anthropic/claude-opus-4-8";
    const INITIAL_CONTEXT_WINDOW: u32 = 123_456;

    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_context_window = Some(INITIAL_CONTEXT_WINDOW);

    let expected_context_window = crate::components::TEST_CONTEXT_WINDOW;
    app.update(agent_msg_with_run_id(
        AgentEvent::ModelSwitched {
            spec: SWITCHED_MODEL_SPEC.into(),
            context_window: expected_context_window,
        },
        app.run_id,
    ));

    let status = rendered_rows(&mut app, 120, 24).pop().expect("status row");

    assert!(status.contains(SWITCHED_MODEL_SPEC));
    assert!(status.contains(&makima_providers::format_tokens(expected_context_window)));
    assert!(!status.contains(&makima_providers::format_tokens(INITIAL_CONTEXT_WINDOW)));
}

/// Workflow sessions have synthetic ids that no ToolDone matches, so
/// SubagentHistory is what finishes their chat.
#[test]
fn subagent_history_finishes_workflow_chat() {
    let mut app = streaming_app();
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "sub".into() },
        "session-abc",
        Some("researcher"),
    ));
    assert_eq!(app.chats.len(), 2);
    assert!(!app.chats[1].is_finished());

    app.update(agent_msg_with_run_id(
        AgentEvent::SubagentHistory {
            history_epoch: 0,
            conversation_id: None,
            tool_use_id: "session-abc".into(),
            messages: vec![],
        },
        1,
    ));
    assert!(app.chats[1].is_finished());
    assert_eq!(app.chats[1].last_message_text(), DONE_TEXT);
}

#[test]
fn stamped_child_tool_done_does_not_finish_the_child_turn() {
    let mut app = streaming_app();
    let info = subagent_info(TASK_ID, "worker");
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::ToolStart(Box::new(ToolStartEvent {
            id: "child-tool".into(),
            tool: "read".into(),
            summary: "reading".into(),
            annotation: None,
            input: None,
            raw_input: None,
            output: None,
            render_header: None,
        })),
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::ToolDone(Box::new(ToolDoneEvent {
            id: "child-tool".into(),
            tool: "read".into(),
            output: ToolOutput::Plain("read complete".into()),
            is_error: false,
            annotation: None,
            written_path: None,
        })),
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));

    assert_eq!(app.chats.len(), 2);
    assert!(!app.chats[1].is_finished());
    assert_eq!(app.chats[1].in_progress_count(), 0);

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TurnOutcome(TurnOutcome::completed(
            info.agent_id,
            TurnId::generate(),
            TokenUsage::default(),
            1,
            DoneReason::EndTurn,
        )),
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));
    assert!(app.chats[1].is_finished());
    assert_eq!(app.chats[1].last_message_text(), DONE_TEXT);
}

#[test]
fn outer_task_done_does_not_duplicate_stamped_child_completion() {
    let mut app = streaming_app();
    let info = subagent_info(TASK_ID, "worker");
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta {
            text: "answer".into(),
        },
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TurnOutcome(TurnOutcome::completed(
            info.agent_id,
            TurnId::generate(),
            TokenUsage::default(),
            1,
            DoneReason::EndTurn,
        )),
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));
    let count_after_outcome = app.chats[1].message_count();
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: TASK_ID.into(),
        tool: TASK_SPAWN_TOOL_NAME.into(),
        output: ToolOutput::Plain("answer".into()),
        is_error: false,
        annotation: None,
        written_path: None,
    }))));

    assert_eq!(app.chats[1].message_count(), count_after_outcome);
    assert_eq!(app.chats[1].last_message_text(), DONE_TEXT);
}

#[test]
fn reusable_subagent_processes_distinct_turn_outcomes() {
    const FAILURE_MESSAGE: &str = "second turn failed";
    let mut app = streaming_app();
    let info = subagent_info(TASK_ID, "worker");
    let envelope = |outcome| {
        Msg::Agent(Box::new(Envelope {
            event: AgentEvent::TurnOutcome(outcome),
            subagent: Some(info.clone()),
            identity: Some(work_identity(1)),
        }))
    };
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta {
            text: "first".into(),
        },
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));

    app.update(envelope(TurnOutcome::completed(
        AgentId::generate(),
        TurnId::generate(),
        TokenUsage::default(),
        1,
        DoneReason::EndTurn,
    )));
    app.run_id = 2;
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta {
            text: "second turn".into(),
        },
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    assert!(!app.chats[1].is_finished());
    assert!(app.task_entries()[1].is_spinning());
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentHistory {
            history_epoch: 0,
            conversation_id: None,
            tool_use_id: TASK_ID.into(),
            messages: vec![Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "recovered reply".into(),
                }],
                ..Default::default()
            }],
        },
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    assert!(app.queue.text_messages().is_empty());
    app.update(envelope(TurnOutcome::failed(
        AgentId::generate(),
        TurnId::generate(),
        TokenUsage::default(),
        1,
        TurnFailure {
            kind: TurnFailureKind::Provider,
            diagnostic: FAILURE_MESSAGE.into(),
            user_message: FAILURE_MESSAGE.into(),
            retryable: false,
        },
    )));

    assert_eq!(app.chats[1].last_message_role(), Some(&DisplayRole::Error));
    assert_eq!(app.chats[1].last_message_text(), FAILURE_MESSAGE);
}

#[test]
fn main_turn_completion_keeps_async_subagent_cancellable_and_reusable() {
    const FOLLOW_UP: &str = "follow up";
    let (mut app, input_rx) = app_with_subagent_actor(TASK_ID);
    app.update(done_event());
    assert!(!app.chats[1].is_finished());
    app.handle_cancel();
    type_and_submit(&mut app, FOLLOW_UP);
    assert_eq!(received_input(&input_rx).message, FOLLOW_UP);
}

#[test]
fn stamped_child_failure_wins_over_prior_history_snapshot() {
    let mut app = streaming_app();
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "sub".into() },
        TASK_ID,
        Some("worker"),
    ));
    app.update(subagent_msg_with_run_id(
        AgentEvent::SubagentHistory {
            history_epoch: 0,
            conversation_id: None,
            tool_use_id: TASK_ID.into(),
            messages: vec![],
        },
        TASK_ID,
        Some("worker"),
        1,
    ));
    assert!(!app.chats[1].is_finished());

    let failure = TurnFailure {
        kind: TurnFailureKind::Provider,
        diagnostic: "provider detail".into(),
        user_message: "provider unavailable".into(),
        retryable: false,
    };
    app.update(subagent_msg_with_run_id(
        AgentEvent::TurnOutcome(TurnOutcome::failed(
            AgentId::generate(),
            TurnId::generate(),
            TokenUsage::default(),
            1,
            failure,
        )),
        TASK_ID,
        Some("worker"),
        1,
    ));
    assert_eq!(app.chats[1].last_message_role(), Some(&DisplayRole::Error));
    assert_eq!(app.chats[1].last_message_text(), "provider unavailable");
}

#[test]
fn failed_subagent_is_presented_without_ui_delivery() {
    const FAILURE_MESSAGE: &str = "provider unavailable";

    let mut app = streaming_app();
    let (input_tx, _input_rx) = flume::unbounded();
    let info = subagent_info_full(TASK_ID, "worker", None, Some(input_tx));
    let outcome = TurnOutcome::failed(
        info.agent_id,
        TurnId::generate(),
        TokenUsage::default(),
        1,
        TurnFailure {
            kind: TurnFailureKind::Provider,
            diagnostic: FAILURE_MESSAGE.into(),
            user_message: FAILURE_MESSAGE.into(),
            retryable: false,
        },
    );

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TurnOutcome(outcome),
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert_eq!(app.chats[1].last_message_role(), Some(&DisplayRole::Error));
    assert_eq!(app.chats[1].last_message_text(), FAILURE_MESSAGE);
    assert!(app.queue.text_messages().is_empty());
}

#[test]
fn agent_error_creates_synthetic_tool_done_with_message() {
    let mut app = streaming_app();

    app.update(agent_msg(AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: "t1".into(),
        tool: "bash".into(),
        summary: "echo hello".into(),
        annotation: None,
        input: None,
        raw_input: None,
        output: None,
        render_header: None,
    }))));
    assert_eq!(app.main_chat().in_progress_count(), 1);

    let error_msg = "Provider is overloaded";
    app.update(agent_msg(AgentEvent::ControlError {
        message: error_msg.into(),
    }));

    assert_eq!(app.main_chat().in_progress_count(), 0);
    let text = app.main_chat().last_message_text();
    assert!(
        text.contains(error_msg),
        "tool output should contain error: {text}"
    );
}

#[test]
fn ctrl_c_denies_permission_prompt() {
    let mut app = test_app();
    app.permission_prompt.open(
        "id".into(),
        makima_config::ToolKey::native("bash"),
        vec!["execute".into()],
        None,
    );
    app.active_input = Some(InputKind::Permission);
    assert!(app.permission_prompt.is_open());

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));
    assert_eq!(app.exit_request, ExitRequest::None);
    assert!(!app.permission_prompt.is_open());
    assert!(actions.is_empty());
}

const TEST_AREA: Rect = Rect {
    x: 0,
    y: 0,
    width: 80,
    height: 40,
};
const SPLIT_EXTENT: u16 = 8;

fn open_split_window(app: &mut App, dir: makima_lua::Split) {
    let buf = Arc::new(makima_agent::SharedBuf::new());
    let config = makima_lua::FloatConfig {
        width: makima_lua::Dimension::Abs(SPLIT_EXTENT),
        height: makima_lua::Dimension::Abs(SPLIT_EXTENT),
        border: makima_lua::Border::None,
        split: dir,
        ..makima_lua::FloatConfig::default()
    };
    let (event_tx, _event_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (_cmd_tx, cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    app.float_mgr.open(buf, config, true, event_tx, cmd_rx);
}

#[test]
fn attention_float_marks_app_as_awaiting_input_until_close() {
    let mut app = test_app();
    let buf = Arc::new(makima_agent::SharedBuf::new());
    let config = makima_lua::FloatConfig {
        needs_input: true,
        split: makima_lua::Split::Below,
        height: makima_lua::Dimension::Abs(4),
        ..makima_lua::FloatConfig::default()
    };
    let (event_tx, _event_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (cmd_tx, cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);

    let active = app.handle_open_win(buf, config, true, event_tx, cmd_rx);
    assert!(active, "an idle input demand activates immediately");
    assert!(app.question_active());
    assert!(app.awaiting_input());

    cmd_tx.send(makima_lua::WinCommand::Close).unwrap();
    let _ = app.float_mgr.tick();
    app.reconcile_active();
    assert!(!app.awaiting_input());
}

#[test]
fn below_split_reserves_bottom_and_suppresses_input() {
    let mut app = test_app();
    let (msg_before, _b, _s, input_before, splits_before) = app.layout_geometry(TEST_AREA);
    assert!(
        splits_before.rect(makima_lua::Split::Below).is_none(),
        "no split open yet"
    );
    assert!(input_before.height > 0, "input box visible before split");

    open_split_window(&mut app, makima_lua::Split::Below);
    let (msg_after, _bottom, _s, input_after, splits_after) = app.layout_geometry(TEST_AREA);

    let band = splits_after
        .rect(makima_lua::Split::Below)
        .expect("below split should reserve a bottom band");
    assert_eq!(
        band.height, SPLIT_EXTENT,
        "below band reserves the requested rows",
    );
    assert!(
        msg_after.height < msg_before.height,
        "chat must shrink to make room for the below split",
    );
    assert_eq!(
        input_after.height, 0,
        "input box is suppressed under a below split"
    );
}

/// `carve` already tests the per-direction geometry; this pins the app wiring:
/// a split shrinks the chat while the full-width status bar stays put. Below is
/// tested separately since it also hides the input box.
#[test_case(makima_lua::Split::Above ; "above")]
#[test_case(makima_lua::Split::Left ; "left")]
#[test_case(makima_lua::Split::Right ; "right")]
fn non_below_split_reserves_band_and_keeps_status_full_width(dir: makima_lua::Split) {
    let mut app = test_app();
    let (msg_before, _b, _s, _i, _sp) = app.layout_geometry(TEST_AREA);

    open_split_window(&mut app, dir);
    let (msg_after, _bottom, status_after, _input, splits) = app.layout_geometry(TEST_AREA);

    assert!(splits.rect(dir).is_some(), "split must reserve a band");
    assert!(
        msg_after.area() < msg_before.area(),
        "chat must shrink to make room for the split",
    );
    assert_eq!(
        status_after.width, TEST_AREA.width,
        "status bar stays full width regardless of the split",
    );
}

#[test]
fn closing_split_restores_layout() {
    let mut app = test_app();
    let before = app.layout_geometry(TEST_AREA);

    open_split_window(&mut app, makima_lua::Split::Below);
    app.float_mgr.close_all();

    let after = app.layout_geometry(TEST_AREA);
    assert_eq!(after, before, "closing the split restores the layout");
}

#[test]
fn cancel_preserves_panel_window_and_closes_transient() {
    let mut app = test_app();

    let panel_buf = Arc::new(makima_agent::SharedBuf::new());
    let panel_config = makima_lua::FloatConfig {
        split: makima_lua::Split::Panel,
        height: makima_lua::Dimension::Abs(SPLIT_EXTENT),
        ..makima_lua::FloatConfig::default()
    };
    let (panel_tx, _panel_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (_panel_cmd_tx, panel_cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    app.float_mgr
        .open(panel_buf, panel_config, false, panel_tx, panel_cmd_rx);
    assert_eq!(app.float_mgr.panel_reqs().len(), 1, "panel dock is open");

    let modal_buf = Arc::new(makima_agent::SharedBuf::new());
    let modal_config = makima_lua::FloatConfig {
        needs_input: true,
        ..makima_lua::FloatConfig::default()
    };
    let (modal_tx, _modal_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (_modal_cmd_tx, modal_cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    app.float_mgr
        .open(modal_buf, modal_config, true, modal_tx, modal_cmd_rx);
    assert!(app.float_mgr.is_open(), "transient float is focused");

    app.handle_cancel();

    assert_eq!(
        app.float_mgr.panel_reqs().len(),
        1,
        "panel dock survives cancel and stays visible",
    );
    assert!(
        !app.float_mgr.is_focused(),
        "transient focused float is closed by cancel",
    );
}

#[test]
fn permission_prompt_takes_bottom_precedence_over_below_split() {
    let mut app = test_app();
    open_split_window(&mut app, makima_lua::Split::Below);
    open_split_window(&mut app, makima_lua::Split::Left);
    open_split_window(&mut app, makima_lua::Split::Above);
    app.permission_prompt.open(
        "perm-1".into(),
        makima_config::ToolKey::native("bash"),
        vec!["ls".into()],
        None,
    );
    app.active_input = Some(InputKind::Permission);

    let (_msg, _bottom, _status, _input, splits) = app.layout_geometry(TEST_AREA);
    assert!(
        splits.rect(makima_lua::Split::Below).is_none(),
        "below split must yield the bottom area to an open permission prompt",
    );
    assert!(
        splits.rect(makima_lua::Split::Left).is_some(),
        "the prompt must leave a left split untouched",
    );
    assert!(
        splits.rect(makima_lua::Split::Above).is_some(),
        "the prompt must leave an above split untouched",
    );
}

fn app_with_active_subagent() -> App {
    let mut app = app_with_subagent();
    app.run_builtin(BuiltinAction::NextChat);
    assert_eq!(app.active_chat, 1);
    app
}

#[test_case(false; "main")]
#[test_case(true; "child")]
fn main_child_input_interaction_parity(child: bool) {
    let mut app = app_with_subagent();
    app.active_chat = usize::from(child);
    app.update(Msg::Paste("shared draft".into()));
    app.update(Msg::Key(key(KeyCode::Left)));
    app.update(Msg::Key(key(KeyCode::Char('!'))));
    assert_eq!(app.input_box.buffer.value(), "shared draf!t");
    assert!(app.run_builtin(BuiltinAction::Search).is_empty());
    assert!(app.search_modal.is_open());
}

#[test]
fn shared_draft_survives_focus_switch() {
    let mut app = app_with_subagent();
    app.update(Msg::Paste("shared draft".into()));
    app.run_builtin(BuiltinAction::NextChat);
    assert_eq!(app.input_box.buffer.value(), "shared draft");
    app.run_builtin(BuiltinAction::PrevChat);
    assert_eq!(app.input_box.buffer.value(), "shared draft");
}

#[test_case(false; "idle_root")]
#[test_case(true; "busy_root")]
fn focused_agent_status_independent_of_root(root_busy: bool) {
    let mut app = app_with_active_subagent();
    app.status = if root_busy {
        Status::Streaming
    } else {
        Status::Idle
    };
    assert!(app.focused_streaming());
    app.chats[1].mark_finished(DisplayRole::Done, DONE_TEXT);
    assert!(!app.focused_streaming());
}

#[test_case(40, 12; "small")]
#[test_case(100, 35; "large")]
fn focused_agent_render_layout_matrix(width: u16, height: u16) {
    let mut app = app_with_subagent();
    app.input_box.set_input("shared\nmultiline".into());
    let area = Rect::new(0, 0, width, height);
    let main = app.layout_geometry(area);
    app.active_chat = 1;
    let child = app.layout_geometry(area);
    assert_eq!(
        (main.0, main.1, main.2, main.3),
        (child.0, child.1, child.2, child.3)
    );
    let rows = rendered_rows(&mut app, width, height);
    assert!(rows.iter().any(|row| row.contains("shared")));
}

#[derive(Clone, Copy, Debug)]
enum RenderSurface {
    Draft,
    Panel,
    Permission,
    Question,
    Plan,
}

#[test_case(80, 24 ; "compact")]
#[test_case(140, 40 ; "large")]
fn normalized_focused_render_matrix(width: u16, height: u16) {
    for surface in [
        RenderSurface::Draft,
        RenderSurface::Panel,
        RenderSurface::Permission,
        RenderSurface::Question,
        RenderSurface::Plan,
    ] {
        for root_busy in [false, true] {
            for selected_busy in [false, true] {
                for closed in [false, true] {
                    if closed && selected_busy {
                        continue;
                    }
                    let mut panels = Vec::new();
                    for child in [false, true] {
                        let mut app = test_app();
                        let mode = if matches!(surface, RenderSurface::Plan) {
                            makima_agent::AgentMode::Plan("matrix-plan.md".into())
                        } else {
                            makima_agent::AgentMode::Build
                        };
                        let (target, rx) =
                            spawn_input_child_with_mode(&mut app, TASK_ID, "matrix-agent", mode);
                        let config = target.effective_config().unwrap().unwrap();
                        commit_root_model(&app, config.model.clone());
                        app.state.session_mut().model = config.model.spec();
                        if child {
                            bind_actor_target(&app, target);
                        }
                        app.status = if if child { root_busy } else { selected_busy } {
                            Status::Streaming
                        } else {
                            Status::Idle
                        };
                        app.active_chat = usize::from(child);
                        if !selected_busy {
                            app.chats[1].mark_finished(DisplayRole::Done, DONE_TEXT);
                        }
                        if closed {
                            let id = app.chats[1].agent_id.unwrap();
                            app.subagent_channels.get_mut(&id).unwrap().closed = true;
                        }
                        app.sync_focused_queue();
                        if child && selected_busy {
                            type_and_submit(&mut app, "active");
                            received_input(&rx);
                        }
                        app.input_box.set_input("shared draft\nsecond line".into());
                        with_image(&mut app);
                        app.last_input = None;
                        match surface {
                            RenderSurface::Draft => {}
                            RenderSurface::Panel => {
                                let (tx, _rx) = flume::unbounded();
                                let (_cmd_tx, cmd_rx) = flume::unbounded();
                                app.float_mgr.open(
                                    Arc::new(makima_agent::SharedBuf::new()),
                                    makima_lua::FloatConfig {
                                        split: makima_lua::Split::Panel,
                                        height: makima_lua::Dimension::Abs(3),
                                        ..Default::default()
                                    },
                                    false,
                                    tx,
                                    cmd_rx,
                                );
                            }
                            RenderSurface::Permission => {
                                let mut demand = bash_perm_demand("matrix-permission");
                                demand.perm.as_mut().unwrap().agent_id = app.selected_agent_id();
                                assert!(!app.begin_input_demand(demand));
                                assert!(app.permission_active());
                            }
                            RenderSurface::Question => {
                                let (active, _events, _commands) =
                                    open_question_win(&mut app, true);
                                assert!(active);
                                assert!(app.question_active());
                            }
                            RenderSurface::Plan => {
                                set_root_mode(&mut app, Mode::Plan);
                                app.state.plan = PlanState::Drafting("matrix-plan.md".into());
                                app.plan_form.on_plan_ready();
                                assert!(app.plan_form_active());
                            }
                        }
                        let rows = rendered_rows(&mut app, width, height);
                        let geometry = app.layout_geometry(Rect::new(0, 0, width, height));
                        let bottom = geometry.1;
                        let start = geometry
                            .4
                            .rect(makima_lua::Split::Below)
                            .map_or(bottom.y, |below| bottom.y.min(below.y));
                        let panel: Vec<_> = rows
                            .into_iter()
                            .skip(start as usize)
                            .take(geometry.2.y.saturating_sub(start) as usize)
                            .map(|row| {
                                let row = if row.contains("[subtask] ") {
                                    let mut normalized = row.replace("[subtask] ", "");
                                    if let Some(border) = normalized.rfind('│') {
                                        normalized.insert_str(border, "          ");
                                    }
                                    normalized
                                } else { row };
                                row.replace(
                                    "DESPAWNED · sends to this subagent · TAB mode · ESC cancel",
                                    &"─".repeat("DESPAWNED · sends to this subagent · TAB mode · ESC cancel".chars().count()),
                                )
                                .replace("sends to this subagent · TAB mode · ESC cancel", &"─".repeat("sends to this subagent · TAB mode · ESC cancel".chars().count()))
                                .trim_end()
                                .to_owned()
                            })
                            .collect();
                        assert!(!panel.is_empty());
                        panels.push(panel);
                    }
                    assert_eq!(
                        panels[0], panels[1],
                        "{surface:?}: root_busy={root_busy}, selected_busy={selected_busy}, closed={closed}"
                    );
                }
            }
        }
    }
}

#[test_case(false, false ; "both_idle")]
#[test_case(false, true ; "child_busy")]
#[test_case(true, false ; "root_busy")]
#[test_case(true, true ; "both_busy")]
fn focused_status_render_does_not_leak_root_state(root_busy: bool, child_busy: bool) {
    const ROOT_ERROR: &str = "root-only error";
    let mut app = with_subagent(
        if root_busy {
            streaming_app()
        } else {
            test_app()
        },
        TASK_ID,
    );
    app.status = if root_busy {
        Status::Streaming
    } else {
        Status::error(ROOT_ERROR.into())
    };
    app.run_model = Some("old-root-model".into());
    app.chats[1].model_id = Some("child-model".into());
    if !child_busy {
        app.chats[1].mark_finished(DisplayRole::Done, DONE_TEXT);
    }
    let main = rendered_rows(&mut app, 160, 30).pop().unwrap();
    assert_eq!(main.contains(ROOT_ERROR), !root_busy);
    assert_eq!(main.contains("[switch queued]"), root_busy);
    app.active_chat = 1;
    let child = rendered_rows(&mut app, 160, 30).pop().unwrap();
    assert!(!child.contains(ROOT_ERROR));
    assert!(!child.contains("[switch queued]"));
    assert!(child.contains("child-model"));
    assert_eq!(app.focused_streaming(), child_busy);
}

#[test]
fn viewed_agent_cancel_is_local() {
    let mut app = app_with_subagent();
    let child_count = app.chats[1].message_count();
    let agent_id = app.chats[1].agent_id.unwrap();
    app.handle_cancel();
    assert_eq!(app.chats[1].message_count(), child_count);
    assert!(!app.chats[1].is_finished());
    assert!(app.subagent_channels.contains_key(&agent_id));
}

#[test]
fn child_consumption_renders_user_bubble() {
    let mut app = app_with_subagent();
    app.update(subagent_msg(
        AgentEvent::QueueItemConsumed {
            text: "consumed child input".into(),
            images: Vec::new(),
            mcp_startup_notice: None,
            already_displayed: false,
        },
        TASK_ID,
        Some("research"),
    ));
    assert_eq!(app.chats[1].last_message_text(), "consumed child input");
    assert_eq!(app.chats[1].last_message_role(), Some(&DisplayRole::User));
}

#[test]
fn shell_results_stay_with_original_agent_after_focus_switch() {
    let mut app = app_with_active_subagent();
    let target = app.selected_agent_id();
    let id = app.shell.reserve_for(target);
    app.active_chat = 0;
    app.handle_shell_event(shell::ShellEvent::Start {
        id: id.clone(),
        command: "true".into(),
    });
    assert_eq!(app.chats[1].in_progress_count(), 1);
    app.handle_shell_event(shell::ShellEvent::Done {
        id,
        command: "true".into(),
        output: "output".into(),
        is_error: false,
        visible: true,
    });
    assert!(app.shell.drain_results().is_empty());
    assert_eq!(app.shell.drain_results_for(target).len(), 1);
}

#[test]
fn double_esc_in_subagent_cancels_subagent() {
    let mut app = app_with_active_subagent();
    app.last_esc = Some(Instant::now());
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(actions.is_empty());
    assert!(!app.chats[1].is_finished());
}

#[test]
fn single_or_stale_esc_in_subagent_flashes() {
    let mut app = app_with_active_subagent();
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(actions.is_empty());
    assert_eq!(app.status_bar.flash_text().unwrap(), FLASH_CANCEL);

    app.last_esc = Some(Instant::now().checked_sub(Duration::from_secs(10)).unwrap());
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(actions.is_empty());
    assert!(!app.chats[1].is_finished());
}

#[test]
fn esc_in_main_chat_with_active_subagent_no_cancel() {
    let mut app = app_with_subagent();
    assert_eq!(app.active_chat, 0);
    app.last_esc = Some(Instant::now());
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], Action::CancelAgent { .. }));
    assert!(!matches!(&actions[0], Action::CancelSubagent { .. }));
}

#[test]
fn cancel_subagent_retains_channel() {
    let (mut app, _sub_rx, _main_rx) = app_with_subagent_tx(TASK_ID);
    assert!(!app.subagent_channels.is_empty());
    app.run_builtin(BuiltinAction::NextChat);
    assert_eq!(app.active_chat, 1);
    app.last_esc = Some(Instant::now());
    let agent_id = app.chats[app.active_chat].agent_id.unwrap();
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(app.subagent_channels.contains_key(&agent_id));
}

#[test]
fn multiple_subagents_cancel_one_other_unaffected() {
    let mut app = app_with_subagent_id(TASK_ID);
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "y".into() },
        "task2",
        Some("build"),
    ));
    assert_eq!(app.chats.len(), 3);

    app.active_chat = *app.chat_index.get("task2").unwrap();
    app.last_esc = Some(Instant::now());
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(actions.is_empty());
    let task1_idx = *app.chat_index.get(TASK_ID).unwrap();
    assert!(!app.chats[task1_idx].is_finished());
    assert!(!app.chats[app.active_chat].is_finished());
}

#[test]
fn double_esc_in_finished_subagent_noop() {
    let mut app = app_with_active_subagent();
    finish_subagent_task(&mut app, false);
    app.last_esc = Some(Instant::now());
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(actions.is_empty());
}

#[test]
fn subagent_cancel_then_navigate_back_main_unaffected() {
    let mut app = app_with_active_subagent();
    app.last_esc = Some(Instant::now());
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(!app.chats[1].is_finished());

    app.run_builtin(BuiltinAction::PrevChat);
    assert_eq!(app.active_chat, 0);
    assert_eq!(app.status, Status::Streaming);
    assert!(!app.chats[0].is_finished());
}

// -- Every frame checkpoints: one way in for a history, one trigger to save --

/// Long enough that a waiting change is still waiting when the assert runs, on
/// any machine, so none of these tests depend on the wall clock.
const SOFT_DELAY_HELD: Duration = Duration::from_secs(3600);
const MID_BATCH_RESULT: &str = "file contents";
const TYPED_DRAFT: &str = "hi";
const UNSENT_DRAFT: &str = "half typed thought";
const LIVE_AGENT_TEXT: &str = "live agent turn";
const STORED_SESSION_TEXT: &str = "other session talk";
const TOOL_IDS: [&str; 2] = ["tool-a", "tool-b"];
const FINISHED_TASK_ID: &str = "task-finished";
const UNFINISHED_TASK_ID: &str = "task-unfinished";

fn tool_use_msg(id: &str) -> Message {
    Message {
        role: Role::Assistant,
        content: vec![ContentBlock::tool_use(id, "read", serde_json::json!({}))],
        ..Default::default()
    }
}

fn tool_result_msg(id: &str, text: &str) -> Message {
    Message {
        role: Role::User,
        content: vec![ContentBlock::ToolResult {
            tool_use_id: id.into(),
            content: text.into(),
            is_error: false,
        }],
        display_text: Some(String::new()),
        ..Default::default()
    }
}

fn tool_text(id: &str) -> String {
    format!("output of {id}")
}

fn attach_live_history(app: &mut App, messages: Vec<Message>) -> History {
    let mirror: makima_agent::SharedMessages =
        Arc::new(ArcSwap::from_pointee(HistorySnapshot::default()));
    let history = History::new(messages).with_mirror(Arc::clone(&mirror));
    app.shared_history = Some(mirror);
    history
}

/// Types [`TYPED_DRAFT`] one key per frame and hands back the stamp of the
/// write the first key caused. The soft delay never elapses, so every key after
/// the first is still waiting when the caller looks.
fn type_draft_leaving_last_key_waiting(app: &mut App) -> Sent {
    let mut keys = TYPED_DRAFT.chars();
    app.update(Msg::Key(key(KeyCode::Char(keys.next().unwrap()))));
    app.checkpoint();
    let first = app
        .last_sent
        .clone()
        .expect("the first keystroke puts the session on disk");

    for c in keys {
        app.update(Msg::Key(key(KeyCode::Char(c))));
        app.checkpoint_with(SOFT_DELAY_HELD);
    }
    first
}

/// Checkpointing mid-batch used to freeze the tools as failed forever. The
/// synthetic closing message made the snapshot as long as the real results that
/// followed, so the append cursor never saw them.
#[test]
fn mid_batch_checkpoint_does_not_shadow_the_real_tool_results() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    let mut history = attach_live_history(
        &mut app,
        vec![Message::user("go".into()), tool_use_msg("t1")],
    );
    app.checkpoint();

    history.push(tool_result_msg("t1", MID_BATCH_RESULT));
    app.checkpoint();

    let id = app.state.session.id;
    drain_writer(app, writer);

    let loaded = AppSession::load(id, &dir).unwrap();
    assert_eq!(loaded.messages().len(), 3);
    let [
        ContentBlock::ToolResult {
            content, is_error, ..
        },
    ] = &loaded.messages()[2].content[..]
    else {
        panic!("expected one real tool result: {:?}", loaded.messages()[2]);
    };
    assert_eq!((content.as_str(), *is_error), (MID_BATCH_RESULT, false));
}

#[test]
fn checkpoint_after_rewind_persists_the_truncated_history() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    let target = bind_history_actor(
        &mut app,
        vec![
            Message::user("first prompt".into()),
            Message::user("second prompt".into()),
        ],
    );
    app.open_rewind_picker();
    app.checkpoint();

    let entry = crate::components::rewind_picker::RewindEntry {
        turn_index: 1,
        prompt_preview: "2: second".into(),
    };
    let actions = app.rewind_to(entry);
    assert!(
        app.shared_history.is_some(),
        "live mirror remains installed"
    );
    apply_rewind_action(&mut app, actions);
    assert_eq!(app.selected_agent_id(), Some(target.id()));
    assert_eq!(
        app.shared_history.as_ref().unwrap().load().messages.len(),
        1
    );
    app.checkpoint_now();
    let id = app.state.session.id;
    drain_writer(app, writer);
    assert_eq!(AppSession::load(id, &dir).unwrap().messages().len(), 1);
}

#[test]
fn reset_session_never_writes_the_old_conversation_under_the_new_id() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    let _live = attach_live_history(&mut app, vec![Message::user("old talk".into())]);
    app.checkpoint();
    let old_id = app.state.session.id;

    let actions = app.reset_session();
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };
    let new_id = request.session.id;
    assert_ne!(new_id, old_id);
    assert_eq!(app.state.session.id, old_id);

    drain_writer(app, writer);
    assert_eq!(AppSession::load(old_id, &dir).unwrap().messages().len(), 1);
    assert!(AppSession::load(new_id, &dir).is_err());
}

/// Two traps in one switch. `install_local_history` has to drop the mirror
/// handle, or the old agent's messages land under the freshly loaded id. And
/// `revision` is `#[serde(skip)]`, so the loaded session starts back at zero and
/// can collide with the revision already sent for the one it replaced, which
/// only keying `last_sent` by id survives.
#[test]
fn load_session_persists_the_new_session_and_leaks_no_history_into_it() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    let mut stored = AppSession::new("test-model", "/tmp/test");
    stored.push_message(Message::user(STORED_SESSION_TEXT.into()));
    stored.save(&dir).unwrap();

    let _live = attach_live_history(&mut app, vec![Message::user(LIVE_AGENT_TEXT.into())]);
    app.input_box.set_input(UNSENT_DRAFT.into());
    app.checkpoint();
    let (live_id, sent_revision) = (app.state.session.id, app.state.session.revision());

    let actions = app.load_loaded_session(AppSession::load(stored.id, &dir).unwrap());
    let Action::ReplaceSession(request) = &actions[0] else {
        panic!("expected replacement request");
    };
    assert_eq!(app.state.session.id, live_id);
    assert_eq!(app.state.session.revision(), sent_revision);
    assert_eq!(request.session.id, stored.id);
    assert_eq!(request.session.messages().len(), 1);
    assert_eq!(
        request.session.messages()[0].user_text(),
        Some(STORED_SESSION_TEXT)
    );

    drain_writer(app, writer);
    let previous = AppSession::load(live_id, &dir).unwrap();
    assert_eq!(previous.meta.input_draft.as_deref(), Some(UNSENT_DRAFT));
    assert_eq!(previous.messages()[0].user_text(), Some(LIVE_AGENT_TEXT));
}

#[test]
fn idle_checkpoint_changes_nothing() {
    let mut app = test_app();
    app.state
        .session_mut()
        .push_message(Message::user("hello".into()));
    app.checkpoint();
    let (revision, updated_at) = (app.state.session.revision(), app.state.session.updated_at);

    app.checkpoint();
    app.checkpoint();

    assert_eq!(app.state.session.revision(), revision);
    assert_eq!(app.state.session.updated_at, updated_at);
}

/// Issue #675: a crash between a keystroke and submit threw the draft away,
/// because nothing was written until the turn ended. The first key lands within
/// a frame now, and the keys behind it ride along on a later write rather than
/// each costing an `fsync`.
#[test]
fn first_draft_keystroke_lands_and_the_rest_coalesce() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    let first = type_draft_leaving_last_key_waiting(&mut app);
    assert_eq!(
        app.last_sent.as_ref(),
        Some(&first),
        "a keystroke on its own waits instead of costing a write",
    );

    app.checkpoint_with(Duration::ZERO);
    assert_ne!(
        app.last_sent.as_ref(),
        Some(&first),
        "and lands once the delay is up"
    );

    let id = app.state.session.id;
    drain_writer(app, writer);
    let saved = AppSession::load(id, &dir).unwrap();
    assert_eq!(saved.meta.input_draft.as_deref(), Some(TYPED_DRAFT));
    assert!(saved.messages().is_empty());
}

#[test]
fn a_content_change_writes_the_waiting_draft_with_it() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    type_draft_leaving_last_key_waiting(&mut app);

    app.state
        .session_mut()
        .push_message(Message::user(LIVE_AGENT_TEXT.into()));
    app.checkpoint_with(SOFT_DELAY_HELD);

    let id = app.state.session.id;
    drain_writer(app, writer);
    let saved = AppSession::load(id, &dir).unwrap();
    assert_eq!(saved.messages().len(), 1, "content never waits");
    assert_eq!(saved.meta.input_draft.as_deref(), Some(TYPED_DRAFT));
}

#[test]
fn shutdown_writes_a_draft_that_is_still_waiting() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    type_draft_leaving_last_key_waiting(&mut app);

    app.checkpoint_now();

    let id = app.state.session.id;
    drain_writer(app, writer);
    let saved = AppSession::load(id, &dir).unwrap();
    assert_eq!(saved.meta.input_draft.as_deref(), Some(TYPED_DRAFT));
}

/// Submitting empties the draft a frame before the agent mirrors the prompt
/// back. Delete the session in that gap and the user loses the one they were
/// just starting.
#[test]
fn submitting_the_draft_keeps_the_session_on_disk() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    type_draft_leaving_last_key_waiting(&mut app);
    let id = app.state.session.id;

    app.update(Msg::Key(key(KeyCode::Enter)));
    app.checkpoint();
    assert!(!app.has_content(), "the submit window is what this covers");

    drain_writer(app, writer);
    assert!(AppSession::load(id, &dir).is_ok());
}

/// The draft put the session on disk, and deleting it leaves nothing worth
/// keeping. Without the delete the file survives with the abandoned draft in
/// it, and the picker offers an empty session to resume.
#[test]
fn deleting_the_draft_takes_the_session_off_disk() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    type_draft_leaving_last_key_waiting(&mut app);
    let id = app.state.session.id;

    for _ in TYPED_DRAFT.chars() {
        app.update(Msg::Key(key(KeyCode::Backspace)));
    }
    app.checkpoint();
    assert!(app.last_sent.is_none(), "nothing is on disk to stamp");

    drain_writer(app, writer);
    assert!(AppSession::load(id, &dir).is_err());
}

/// The second result goes through the append cursor the first one opened, so a
/// stale cursor would quietly drop or duplicate it.
#[test]
fn two_tool_results_checkpointed_separately_both_reach_disk() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    app.state
        .session_mut()
        .push_message(Message::user("prompt".into()));
    app.status = Status::Streaming;
    app.run_id = 1;

    for tool_id in TOOL_IDS {
        app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
            id: tool_id.into(),
            tool: "bash".into(),
            output: ToolOutput::Plain(tool_text(tool_id).into()),
            is_error: false,
            annotation: None,
            written_path: None,
        }))));
        app.checkpoint();
    }

    let id = app.state.session.id;
    drain_writer(app, writer);
    let loaded = AppSession::load(id, &dir).unwrap();
    for tool_id in TOOL_IDS {
        match loaded.tool_outputs().get(tool_id).map(Arc::as_ref) {
            Some(ToolOutput::Plain(out)) => assert_eq!(out.text, tool_text(tool_id)),
            other => panic!("missing plain output for {tool_id}: {other:?}"),
        }
    }
}

/// The `Done` path clears `chat_index` right after pruning it, so nothing can
/// rebuild the tabs later. Only the `sync_subagents` call inside
/// `retain_resolved_subagents` carries the survivors over.
#[test]
fn turn_end_keeps_running_and_finished_subagents() {
    let mut app = streaming_app();
    for (task_id, name) in [
        (FINISHED_TASK_ID, "finished child"),
        (UNFINISHED_TASK_ID, "open child"),
    ] {
        app.update(subagent_msg(
            AgentEvent::TextDelta { text: "x".into() },
            task_id,
            Some(name),
        ));
    }
    finish_subagent(&mut app, FINISHED_TASK_ID, false);
    assert_eq!(app.state.session.subagents().len(), 2);

    app.update(done_event());
    assert_eq!(app.chat_index.len(), 2);
    assert!(!app.chats[2].is_finished());
    app.checkpoint();

    let ids: Vec<_> = app
        .state
        .session
        .subagents()
        .iter()
        .map(|sa| sa.tool_use_id.as_str())
        .collect();
    assert_eq!(ids, [FINISHED_TASK_ID, UNFINISHED_TASK_ID]);
}

#[test]
fn run_builtin_file_picker_opens_modal() {
    let mut app = test_app();
    assert!(app.run_builtin(BuiltinAction::FilePicker).is_empty());
    assert!(app.file_picker.is_open());
}

// --- `@` completion popup (files + skills) ---------------------------------

/// An app whose cwd points at a fresh temp dir, so walk results are
/// deterministic no matter the machine. The `EventHandle` is backed by an
/// in-memory completion store so `@` sources/expanders work without a plugin
/// host; the store handle is returned so tests can seed it.
fn completion_app() -> (TempDir, App, Arc<makima_lua::TestCompletionBackend>) {
    let tmp = TempDir::new().unwrap();
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, backend) = makima_lua::test_support::event_handle_with_completion();
    let mut app = build_app_with_handle(dir.clone(), Arc::new(test_writer(dir)), handle);
    std::sync::Arc::get_mut(&mut app.state.session)
        .unwrap()
        .set_cwd(tmp.path().to_string_lossy().into_owned());
    (tmp, app, backend)
}

fn write_completion_fixture(cwd: &Path, rel: &str, content: &str) {
    let path = cwd.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Seed a `skill` source + expander into the test backend (skills now come
/// from a plugin completion source, not the filesystem).
fn seed_skill(backend: &makima_lua::TestCompletionBackend, name: &str) {
    backend.register_source(
        "skill",
        vec![makima_lua::ItemSpec {
            label: format!("skill:{name}"),
            kind: "skill".into(),
            insertion: format!("@skill:{name}"),
            description: None,
        }],
    );
    backend.register_expander("skill", move |v| Ok(format!("<skill:{v}>")));
    backend.register_expander("s", move |v| Ok(format!("<skill:{v}>")));
}

/// Lets the completion popup's walker finish and nucleo converge, waiting until
/// the popup is actually offering a selectable item.
fn converge_completion(app: &mut App) {
    let reference = app.input_box.buffer.value().starts_with('@');
    wait_for(
        || {
            let _ = app.file_completion.tick();
            let _ = app.command_palette.poll_arguments();
            if reference {
                app.file_completion.has_selectable()
            } else {
                app.command_palette.has_argument_selectable()
            }
        },
        "completion popup never offered a selectable item",
    );
}

#[test]
fn typing_at_opens_popup() {
    let (_tmp, mut app, _backend) = completion_app();
    app.update(Msg::Key(key(KeyCode::Char('@'))));
    assert!(app.file_completion.is_active());
    assert_eq!(app.input_box.buffer.value(), "@");
}

#[test]
fn no_token_no_popup() {
    let (_tmp, mut app, _backend) = completion_app();
    app.update(Msg::Key(key(KeyCode::Char('h'))));
    app.update(Msg::Key(key(KeyCode::Char('i'))));
    assert!(!app.file_completion.is_active());
}

#[test]
fn esc_closes_leaves_text() {
    let (_tmp, mut app, _backend) = completion_app();
    app.update(Msg::Key(key(KeyCode::Char('@'))));
    assert!(app.file_completion.is_active());
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(!app.file_completion.is_active());
    assert_eq!(app.input_box.buffer.value(), "@");
}

#[test]
fn enter_inserts_skill() {
    let (_tmp, mut app, backend) = completion_app();
    seed_skill(&backend, "review");
    for c in "@skill:rev".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "@skill:review");
    assert!(!app.file_completion.is_active());
}

#[test_case(KeyModifiers::ALT ; "alt_left")]
#[test_case(KeyModifiers::CONTROL ; "ctrl_left")]
fn word_motion_left_with_completion_open_reaches_input(mods: KeyModifiers) {
    let (_tmp, mut app, backend) = completion_app();
    seed_skill(&backend, "review");
    for c in "@skill:rev".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(KeyEvent::new(KeyCode::Left, mods)));
    assert_eq!(app.input_box.buffer.x(), 0);
}

#[test]
fn at_completion_insertion_synchronizes_argument_completion() {
    let (_tmp, mut app, _backend) = completion_app();
    let producer = app
        .command_runtime
        .registry
        .create_producer(makima_commands::ProducerPrecedence::Plugin);
    producer
        .replace(vec![makima_commands::Registration {
            spec: makima_commands::CommandSpec {
                name: Arc::from("/deploy"),
                aliases: Arc::from([]),
                arguments: makima_commands::CommandArguments::Positional(Arc::from([
                    makima_commands::PositionalArgument::optional(
                        "arg",
                        makima_commands::ArgumentKind::String,
                    )
                    .with_completion(makima_commands::CompletionPolicy::Replace),
                ])),
                docs: makima_commands::CommandDocs {
                    summary: Arc::from("Deploy"),
                    argument_hint: None,
                },
                required_capabilities: makima_commands::TargetCapabilities::default(),
            },
            behavior: makima_lua::EventHandle::disconnected_for_test()
                .command_behavior_for_test("deploy", "/deploy"),
            argument_completions: vec![Some(Arc::new(TestLuaCompletion {
                handle: makima_lua::EventHandle::disconnected_for_test(),
                plugin: Arc::from("deploy"),
            })
                as Arc<dyn makima_commands::CommandCompletion>)],
        }])
        .unwrap();
    app.command_palette = CommandPalette::new(
        app.command_runtime.registry.clone(),
        app.command_target.clone(),
    );
    app.input_box.set_input("/deploy @rev".into());
    let _ = app.command_palette.sync("/deploy @rev");
    settle_command_palette(&mut app);
    let value = app.input_box.buffer.value();
    app.sync_command_arguments(&value, app.input_box.buffer.cursor_byte_offset());
    app.file_completion
        .open(&app.state.session.cwd, Vec::new(), "rev", (8, 12));
    let generation = app.command_palette.argument_generation();

    app.insert_completion(CompletionItem {
        label: "skill:review".into(),
        kind: "skill".into(),
        insertion: "@skill:review".into(),
        description: None,
    });

    assert_eq!(app.input_box.buffer.value(), "/deploy @skill:review");
    assert!(app.command_palette.argument_generation() > generation);
}

#[test]
fn at_completion_layout_ignores_autocomplete_height() {
    let (_tmp, mut app, backend) = completion_app();
    backend.register_source(
        "skill",
        (0..8)
            .map(|i| makima_lua::ItemSpec {
                label: format!("skill:item-{i}"),
                kind: "skill".into(),
                insertion: format!("@skill:item-{i}"),
                description: None,
            })
            .collect(),
    );
    for c in "@skill:".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);

    app.ui_config.autocomplete_height = 0.2;
    let low_fraction = rendered_rows(&mut app, 80, 24);
    app.ui_config.autocomplete_height = 0.9;
    let high_fraction = rendered_rows(&mut app, 80, 24);

    assert_eq!(low_fraction, high_fraction);
}

#[test]
fn skills_complete_without_prefix() {
    let (_tmp, mut app, backend) = completion_app();
    seed_skill(&backend, "review");
    for c in "@rev".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "@skill:review");
}

#[test]
fn enter_inserts_file_verbatim() {
    let (tmp, mut app, _backend) = completion_app();
    write_completion_fixture(tmp.path(), "docs/read me.md", "content");
    for c in "@read".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "@\"docs/read me.md\"");
}

#[test]
fn space_closes_unquoted_completion_but_not_quoted_completion() {
    let (_tmp, mut app, _backend) = completion_app();
    for character in "@partial ".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    assert!(!app.file_completion.is_active());

    app.input_box.set_input(String::new());
    for character in "@\"partial path".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    assert!(app.file_completion.is_active());
}

#[test]
fn parent_path_completion_advances_then_finishes() {
    let (_tmp, mut app, _backend) = completion_app();
    let parent = tempfile::tempdir().unwrap();
    let project = parent.path().join("project");
    let sibling = parent.path().join("sibling");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::create_dir_all(&sibling).unwrap();
    std::fs::write(sibling.join("outside.txt"), b"outside").unwrap();
    std::sync::Arc::get_mut(&mut app.state.session)
        .unwrap()
        .set_cwd(project.to_string_lossy().into_owned());

    for character in "@../sib".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.file_completion.is_active());
    assert_eq!(
        app.input_box.buffer.value(),
        format!("@../sibling{}", std::path::MAIN_SEPARATOR)
    );

    for character in "out".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(
        app.input_box.buffer.value(),
        format!("@../sibling{}outside.txt", std::path::MAIN_SEPARATOR)
    );
    assert!(!app.file_completion.is_active());
}

#[test]
fn absolute_path_completion_finds_external_file() {
    let (_tmp, mut app, _backend) = completion_app();
    let external = tempfile::tempdir().unwrap();
    let file = external.path().join("external-note.txt");
    std::fs::write(&file, b"external").unwrap();
    let query = format!(
        "@{}{}external",
        external.path().display(),
        std::path::MAIN_SEPARATOR
    );

    for character in query.chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));

    assert_eq!(app.input_box.buffer.value(), format!("@{}", file.display()));
    assert!(!app.file_completion.is_active());
}

#[test]
fn quoted_explicit_path_advances_then_closes_on_file() {
    let (_tmp, mut app, _backend) = completion_app();
    let parent = tempfile::tempdir().unwrap();
    let project = parent.path().join("project");
    let directory = parent.path().join("release notes");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("final report?.md"), b"report").unwrap();
    Arc::get_mut(&mut app.state.session)
        .unwrap()
        .set_cwd(project.to_string_lossy().into_owned());

    for character in "@\"../release".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(
        app.input_box.buffer.value(),
        format!("@\"../release notes{}", std::path::MAIN_SEPARATOR)
    );
    assert!(app.file_completion.is_active());

    for character in "final".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Tab)));
    assert_eq!(
        app.input_box.buffer.value(),
        format!(
            "@\"../release notes{}final report?.md\"",
            std::path::MAIN_SEPARATOR
        )
    );
    assert!(!app.file_completion.is_active());
}

#[test]
fn popup_closes_when_token_removed() {
    let (_tmp, mut app, _backend) = completion_app();
    app.update(Msg::Key(key(KeyCode::Char('@'))));
    app.update(Msg::Key(key(KeyCode::Char('x'))));
    assert!(app.file_completion.is_active());
    // Backspace the `x`, then the `@`: the token is gone and the popup closes.
    app.update(Msg::Key(key(KeyCode::Backspace)));
    assert_eq!(app.input_box.buffer.value(), "@");
    assert!(app.file_completion.is_active());
    app.update(Msg::Key(key(KeyCode::Backspace)));
    assert_eq!(app.input_box.buffer.value(), "");
    assert!(!app.file_completion.is_active());
}

#[test]
fn cd_completion_filters_directories_and_accepts_before_execution() {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    std::fs::write(tmp.path().join("alpha.txt"), b"file").unwrap();

    for character in "/cd al".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    let items = app.command_palette.argument_match_items();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].label,
        format!("alpha{}", std::path::MAIN_SEPARATOR).into()
    );

    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "/cd alpha/");
    assert!(!app.command_palette.is_active());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());

    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test_case("release notes", "release" ; "spaces")]
#[test_case("日本語 @release notes", "\"日本語 @rel" ; "unicode_spaces_and_at")]
fn cd_completion_keeps_paths_raw(directory: &str, query: &str) {
    let (tmp, mut app, backend) = completion_app();
    seed_skill(&backend, "release");
    let path = tmp.path().join(directory);
    std::fs::create_dir(&path).unwrap();

    app.update(Msg::Paste(format!("/cd {query}")));
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    let expected = if directory.chars().any(char::is_whitespace) {
        format!("/cd \"{directory}/\"")
    } else {
        format!("/cd {directory}/")
    };
    assert_eq!(app.input_box.buffer.value(), expected);
    assert!(!app.command_palette.is_active());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());

    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test]
fn quoted_directory_descent_keeps_cursor_inside_quotes() {
    let (tmp, mut app, _backend) = completion_app();
    let parent = tmp.path().join("release notes");
    let child = parent.join("draft copy");
    std::fs::create_dir_all(&child).unwrap();

    app.update(Msg::Paste("/cd release".into()));
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Tab)));

    let descended = format!("/cd \"release notes{}\"", std::path::MAIN_SEPARATOR);
    assert_eq!(app.input_box.buffer.value(), descended);
    assert_eq!(
        app.input_box.buffer.cursor_byte_offset(),
        descended.len() - 1
    );

    app.update(Msg::Paste("draft copy".into()));
    assert_eq!(
        app.input_box.buffer.value(),
        format!(
            "/cd \"release notes{}draft copy\"",
            std::path::MAIN_SEPARATOR
        )
    );
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    app.update(Msg::Key(key(KeyCode::Enter)));

    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test_case(KeyCode::Right, 1 ; "right")]
#[test_case(KeyCode::Down, 2 ; "down")]
#[test_case(KeyCode::Left, 0 ; "left")]
#[test_case(KeyCode::Up, 0 ; "up")]
fn completion_grid_arrows_match_for_typed_paths_and_at(
    key_code: KeyCode,
    expected_selection: usize,
) {
    let mut selections = Vec::new();
    for input in ["/cd ", "@"] {
        let (tmp, mut app, _backend) = completion_app();
        for index in 0..7 {
            std::fs::create_dir(tmp.path().join(format!("entry-{index}"))).unwrap();
        }
        app.update(Msg::Paste(input.into()));
        converge_completion(&mut app);
        let deadline = Instant::now() + WALK_TIMEOUT;
        loop {
            let _ = app.file_completion.tick();
            let _ = app.command_palette.poll_arguments();
            let count = if input.starts_with("/cd") {
                app.command_palette.argument_match_items().len()
            } else {
                app.file_completion.match_items().len()
            };
            if count >= 7 {
                break;
            }
            assert!(Instant::now() < deadline, "completion rows did not settle");
            std::thread::yield_now();
        }
        let before = (
            app.input_box.buffer.value(),
            app.input_box.buffer.cursor_byte_offset(),
        );
        let _ = rendered(&mut app);
        app.update(Msg::Key(key(key_code)));
        selections.push(if input.starts_with("/cd") {
            app.command_palette.argument_selected_for_test()
        } else {
            app.file_completion.selected_for_test()
        });
        assert_eq!(
            (
                app.input_box.buffer.value(),
                app.input_box.buffer.cursor_byte_offset()
            ),
            before,
            "{key_code:?} must not move the prompt cursor"
        );
    }
    assert_eq!(selections, vec![expected_selection; 2]);
}

#[test]
fn cd_completion_tab_descends_and_renders_instead_of_slash_rows() {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir_all(tmp.path().join("alpha/bravo/charlie")).unwrap();
    std::fs::write(tmp.path().join("alpha.txt"), b"file").unwrap();
    for character in "/cd al".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    let screen = rendered(&mut app);
    assert!(screen.contains("alpha/"));
    assert!(!screen.contains("alpha.txt"));
    assert_eq!(screen.matches("/cd").count(), 1);

    for (input, child) in [("/cd alpha/", "bravo/"), ("/cd alpha/bravo/", "charlie/")] {
        app.update(Msg::Key(key(KeyCode::Tab)));
        assert_eq!(app.input_box.buffer.value(), input);
        assert!(app.command_palette.is_active());
        assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
        converge_completion(&mut app);
        let screen = rendered(&mut app);
        assert!(screen.contains(child));
        assert_eq!(screen.matches("/cd").count(), 1);
    }
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "/cd alpha/bravo/charlie/");
    assert!(!app.command_palette.is_active());
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test]
fn cd_completion_escape_preserves_partial_for_execution() {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir_all(tmp.path().join("alpha/child")).unwrap();
    app.update(Msg::Paste("/cd ./alpha".into()));
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(!app.command_palette.is_active());
    assert_eq!(app.input_box.buffer.value(), "/cd ./alpha");
    let _ = app.tick();
    assert!(!app.command_palette.is_active());
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test_case(KeyCode::Esc, "/cd ./alpha"; "dismissed")]
#[test_case(KeyCode::Enter, "/cd ./alpha/"; "accepted")]
fn cd_completion_closed_popup_preserves_argument_ownership(close: KeyCode, expected: &str) {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    app.update(Msg::Paste("/cd ./alpha".into()));
    converge_completion(&mut app);
    app.update(Msg::Key(key(close)));
    assert!(!app.command_palette.is_active());
    assert_eq!(rendered(&mut app).matches("/cd").count(), 1);
    let mode = app.root_mode().clone();
    app.update(Msg::Key(key(KeyCode::Tab)));
    assert_eq!(app.input_box.buffer.value(), expected);
    if close == KeyCode::Esc {
        assert_eq!(app.root_mode(), mode);
    }
    assert!(!app.command_palette.is_active());
    assert_eq!(rendered(&mut app).matches("/cd").count(), 1);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test_case("./missing" ; "no_match")]
#[test_case("./empty/" ; "empty_directory")]
fn cd_completion_no_match_tab_preserves_input_enter_submits(query: &str) {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("empty")).unwrap();
    let input = format!("/cd {query}");
    app.update(Msg::Paste(input.clone()));
    settle_command_palette(&mut app);
    assert!(app.command_palette.is_active());
    assert!(!app.command_palette.has_argument_selectable());
    let mode = app.root_mode().clone();
    app.update(Msg::Key(key(KeyCode::Tab)));
    assert_eq!(app.input_box.buffer.value(), input);
    assert_eq!(app.root_mode(), mode);
    assert!(app.command_palette.is_active());
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert!(!app.command_palette.is_active());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test_case(false ; "command_name_tab")]
#[test_case(true ; "paste")]
fn cd_completion_opens_after_command_name_tab_or_paste(paste: bool) {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    if paste {
        app.update(Msg::Paste("/cd ".into()));
    } else {
        for character in "/cd".chars() {
            app.update(Msg::Key(key(KeyCode::Char(character))));
        }
        assert!(!app.file_completion.is_active());
        app.update(Msg::Key(key(KeyCode::Tab)));
    }
    assert_eq!(app.input_box.buffer.value(), "/cd ");
    assert!(app.command_palette.is_active());
    assert!(app.command_palette.has_argument_selectable() || app.command_palette.is_active());
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Tab)));
    assert_eq!(app.input_box.buffer.value(), "/cd alpha/");
}

#[test]
fn cd_slash_completion_uses_child_cwd_before_argument_dispatch() {
    let (root, mut app, _backend) = completion_app();
    let child = TempDir::new().unwrap();
    std::fs::create_dir(root.path().join("alpha-root")).unwrap();
    std::fs::create_dir(child.path().join("alpha-child")).unwrap();
    let (target, _) = spawn_input_child(&mut app, TASK_ID, "research");
    let actor = target.actor().unwrap();
    let update = actor.reserve_prepared_operation(None).unwrap();
    smol::block_on(update.commit_when_ready(Some(
        makima_agent::actor::ConfigChange::WorkingDirectory(child.path().to_path_buf()),
    )))
    .unwrap();
    app.active_chat = 1;
    bind_actor_target(&app, target);
    app.sync_focused_queue();
    for character in "/cd ./al".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    let labels: Vec<_> = app
        .command_palette
        .argument_match_items()
        .iter()
        .map(|item| item.label.to_string())
        .collect();
    assert_eq!(labels, ["./alpha-child/"]);
    app.update(Msg::Key(key(KeyCode::Tab)));
    assert_eq!(app.input_box.buffer.value(), "/cd ./alpha-child/");
    assert_eq!(app.state.session.cwd, root.path().to_string_lossy());
}

#[test]
fn cd_completion_does_not_hijack_plugin_override() {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let _producer = register_test_lua_command(
        &app.command_runtime.registry,
        TestLuaCommand {
            handle,
            name: Arc::from("/cd"),
            plugin: Arc::from("test"),
            max_args: None,
            completion: false,
        },
    );
    app.update(Msg::Paste("/cd al".into()));
    settle_command_palette(&mut app);
    assert!(!app.file_completion.is_active());
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(actions.is_empty(), "{LUA_COMMAND_NOT_SENT}");
    assert!(probe.try_recv().is_some(), "{LUA_COMMAND_RAN}");
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test]
fn cd_completion_switches_back_to_reference_sources() {
    let (tmp, mut app, backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    seed_skill(&backend, "review");
    app.update(Msg::Paste("/cd ./al".into()));
    converge_completion(&mut app);
    app.input_box.set_input(String::new());
    app.update(Msg::Paste("@skill:rev".into()));
    assert!(app.file_completion.is_active());
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "@skill:review");
    assert!(!app.file_completion.is_active());
}

#[test_case(false ; "active")]
#[test_case(true ; "dismissed")]
fn cd_completion_cwd_change_refreshes_typed_popup(dismissed: bool) {
    let (tmp, mut app, _backend) = completion_app();
    let next = TempDir::new().unwrap();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    std::fs::create_dir(next.path().join("alpine")).unwrap();
    let input = "/cd ./al";
    app.update(Msg::Paste(input.into()));
    converge_completion(&mut app);
    assert_eq!(
        app.command_palette.argument_match_items()[0].label,
        "./alpha/".into()
    );
    if dismissed {
        app.update(Msg::Key(key(KeyCode::Esc)));
    }

    app.apply_directory_change(next.path().canonicalize().unwrap());
    if !dismissed {
        converge_completion(&mut app);
    }

    assert_eq!(
        app.state.session.cwd,
        next.path().canonicalize().unwrap().to_string_lossy()
    );
    assert_eq!(app.command_palette.is_active(), !dismissed);
    assert_eq!(app.input_box.buffer.value(), input);
    if !dismissed {
        assert_eq!(
            app.command_palette.argument_match_items()[0].label,
            "./alpine/".into()
        );
    }
}

#[test_case(false ; "active")]
#[test_case(true ; "dismissed")]
fn cd_completion_cwd_change_refreshes_reference_popup(dismissed: bool) {
    let (tmp, mut app, _backend) = completion_app();
    let next = TempDir::new().unwrap();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    std::fs::create_dir(next.path().join("alpine")).unwrap();
    let input = "@./al";
    app.update(Msg::Paste(input.into()));
    converge_completion(&mut app);
    assert_eq!(app.file_completion.match_items()[0].label, "./alpha/");
    if dismissed {
        app.update(Msg::Key(key(KeyCode::Esc)));
    }

    app.apply_directory_change(next.path().canonicalize().unwrap());

    assert_eq!(
        app.state.session.cwd,
        next.path().canonicalize().unwrap().to_string_lossy()
    );
    assert_eq!(app.file_completion.is_active(), !dismissed);
    assert_eq!(app.input_box.buffer.value(), input);
    if !dismissed {
        assert!(!app.file_completion.needs_reopen(&app.state.session.cwd));
        assert_eq!(app.file_completion.match_items()[0].label, "./alpine/");
    }
    let _ = app.tick();
    assert_eq!(app.file_completion.is_active(), !dismissed);
}

#[test]
fn cd_completion_ctrl_a_enter_executes_original_partial() {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    app.update(Msg::Paste("/cd ./al".into()));
    converge_completion(&mut app);
    app.update(Msg::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::CONTROL,
    )));
    assert_eq!(app.input_box.buffer.x(), 0);
    assert!(!app.file_completion.is_active());
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test]
fn cd_completion_click_command_name_closes_popup_before_enter() {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    app.update(Msg::Paste("/cd ./al".into()));
    converge_completion(&mut app);
    let rows = rendered_rows(&mut app, 80, 24);
    let area = app.zones.find(SelectionZone::Input).unwrap().area;
    let row = area.y;
    let column = rows[usize::from(row)]
        .chars()
        .collect::<Vec<_>>()
        .windows("/cd".len())
        .position(|window| window == ['/', 'c', 'd'])
        .unwrap() as u16;
    for kind in [
        MouseEventKind::Down(MouseButton::Left),
        MouseEventKind::Up(MouseButton::Left),
    ] {
        app.update(mouse_event(kind, column, row));
    }
    assert_eq!(app.input_box.buffer.cursor_byte_offset(), 0);
    assert_eq!(app.input_box.buffer.value(), "/cd ./al");
    assert!(!app.file_completion.is_active());
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test]
fn cd_completion_ctrl_left_requeries_before_accepting() {
    let (tmp, mut app, _backend) = completion_app();
    for directory in ["release apple", "release notes"] {
        std::fs::create_dir(tmp.path().join(directory)).unwrap();
    }
    app.update(Msg::Paste("/cd \"./release no".into()));
    converge_completion(&mut app);
    assert_eq!(
        app.command_palette.argument_match_items()[0].label,
        "./release notes/".into()
    );
    app.update(Msg::Key(KeyEvent::new(
        KeyCode::Left,
        KeyModifiers::CONTROL,
    )));
    assert_eq!(
        app.input_box.buffer.cursor_byte_offset(),
        "/cd \"./release ".len()
    );
    converge_completion(&mut app);
    let items = app.command_palette.argument_match_items();
    assert_eq!(items.len(), 2);
    let quoted = items
        .iter()
        .find(|item| item.label.as_ref() == "./release apple/")
        .expect("quoted requery must offer release apple");
    assert_eq!(quoted.label.as_ref(), "./release apple/");
    app.command_palette.select_for_test(quoted);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "/cd \"./release apple/\"");
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(app.input_box.buffer.value().is_empty());
    assert_eq!(app.state.session.cwd, tmp.path().to_string_lossy());
}

#[test]
fn cd_completion_cursor_in_whitespace_before_path_does_not_panic() {
    let (_tmp, mut app, _backend) = completion_app();
    let input = "/cd  ./alpha";
    app.input_box.set_input(input.into());
    app.input_box.buffer.set_cursor_byte_offset(4);
    let _ = app.command_palette.sync(input);
    app.sync_command_arguments(input, 4);

    assert!(app.typed_path_completion_context().is_some());
    app.sync_file_completion();
    assert!(!app.file_completion.is_active());
}

#[test_case("/cd ", "  " ; "trailing_spaces")]
#[test_case("/cd \t ", " \t\nkeep @skill:review" ; "whitespace_and_following_line")]
fn cd_completion_preserves_text_outside_path(prefix: &str, remainder: &str) {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("日本語 notes")).unwrap();
    let partial = format!("{prefix}./日本");
    let input = format!("{partial}{remainder}");
    app.input_box.set_input(input.clone());
    app.input_box.buffer.set_cursor_byte_offset(partial.len());
    let _ = app.command_palette.sync(&input);
    app.sync_command_arguments(&input, partial.len());
    app.sync_file_completion();
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(
        app.input_box.buffer.value(),
        format!("{prefix}\"./日本語 notes/\"{remainder}")
    );
    assert!(!app.file_completion.is_active());
}

#[test_case(KeyCode::Enter, KeyModifiers::SHIFT ; "shift_enter")]
#[test_case(KeyCode::Char('j'), KeyModifiers::CONTROL ; "ctrl_j")]
fn cd_completion_newline_closes_popup_at_new_cursor(code: KeyCode, modifiers: KeyModifiers) {
    let (tmp, mut app, _backend) = completion_app();
    std::fs::create_dir(tmp.path().join("alpha")).unwrap();
    app.update(Msg::Paste("/cd ./al".into()));
    converge_completion(&mut app);
    app.update(Msg::Key(KeyEvent::new(code, modifiers)));
    assert_eq!(app.input_box.buffer.value(), "/cd ./al\n");
    assert_eq!(app.input_box.buffer.y(), 1);
    assert!(!app.file_completion.is_active());
}

#[test]
fn command_palette_takes_precedence() {
    let (_tmp, mut app, _backend) = completion_app();
    // `/model ` takes an argument, so the palette stays matched while an
    // `@` token is added to that argument space.
    for c in "/model ".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    assert!(app.command_palette.is_active());
    app.update(Msg::Key(key(KeyCode::Char('@'))));
    assert!(
        app.command_palette.is_active(),
        "palette stays matched on /model"
    );
    assert!(
        !app.file_completion.is_active(),
        "@ popup suppressed while palette is up"
    );
}

// --- `@` reference completion: subagents and models --------------------------

fn completion_match_items(app: &App) -> Vec<CompletionItem> {
    app.file_completion.match_items()
}

fn subagent_match_names(app: &App) -> Vec<String> {
    let mut names: Vec<_> = completion_match_items(app)
        .into_iter()
        .filter(|i| i.kind == "subagent")
        .map(|i| {
            i.label
                .strip_prefix("subagent:")
                .map(|s| s.to_string())
                .unwrap_or(i.label)
        })
        .collect();
    names.sort();
    names
}

/// Seed the `subagent` source with the types valid for `mode` (the task
/// plugin's mode filtering, mirrored here): `general` is plan-blocked,
/// `plan_reviewer` is build-blocked.
fn seed_subagents(backend: &makima_lua::TestCompletionBackend, mode: &str) {
    let all = [
        ("research", "Read-only search and summarize"),
        ("general", "Can modify files"),
        ("plan_reviewer", "Read-only plan audit (plan mode)"),
    ];
    let items: Vec<_> = all
        .iter()
        .filter(|(name, _)| {
            if mode == "plan" {
                *name != "general"
            } else {
                *name != "plan_reviewer"
            }
        })
        .map(|(name, desc)| makima_lua::ItemSpec {
            label: format!("subagent:{name}"),
            kind: "subagent".into(),
            insertion: format!("@subagent:{name} "),
            description: Some((*desc).into()),
        })
        .collect();
    backend.register_source("subagent", items);
    backend.register_expander("subagent", |v| Ok(format!("<subagent:{v}>")));
    backend.register_expander("a", |v| Ok(format!("<subagent:{v}>")));
}

#[test]
fn at_a_prefix_lists_subagents() {
    let (_tmp, mut app, backend) = completion_app();
    seed_subagents(&backend, "build");
    for c in "@a:".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    assert_eq!(
        subagent_match_names(&app),
        vec!["general".to_string(), "research".to_string()]
    );
}

#[test]
fn at_subagent_prefix_lists_subagents() {
    let (_tmp, mut app, backend) = completion_app();
    seed_subagents(&backend, "build");
    for c in "@subagent:".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    assert_eq!(
        subagent_match_names(&app),
        vec!["general".to_string(), "research".to_string()]
    );
}

#[test]
fn mode_switch_closes_completion_source_snapshot() {
    let (_tmp, mut app, backend) = completion_app();
    bind_root(&mut app, parked_root());
    seed_subagents(&backend, "build");
    for character in "@a:".chars() {
        app.update(Msg::Key(key(KeyCode::Char(character))));
    }
    converge_completion(&mut app);
    assert!(app.file_completion.is_active());

    set_root_mode(&mut app, Mode::from_id("plan"));
    assert!(!app.file_completion.is_active());
}

#[test]
fn at_a_prefix_in_plan_mode_hides_general() {
    let (_tmp, mut app, backend) = completion_app();
    bind_root(&mut app, parked_root());
    set_root_mode(&mut app, Mode::from_id("plan"));
    seed_subagents(&backend, "plan");
    for c in "@a:".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    let names = subagent_match_names(&app);
    assert!(!names.contains(&"general".to_string()));
    assert!(names.contains(&"plan_reviewer".to_string()));
    assert!(names.contains(&"research".to_string()));
}

fn seed_models(backend: &makima_lua::TestCompletionBackend, models: &[&str]) {
    let items: Vec<_> = models
        .iter()
        .map(|spec| makima_lua::ItemSpec {
            label: format!("model:{spec}"),
            kind: "model".into(),
            insertion: format!("@model:{spec} "),
            description: None,
        })
        .collect();
    backend.register_source("model", items);
    backend.register_expander("model", |v| Ok(format!("<model:{v}>")));
    backend.register_expander("m", |v| Ok(format!("<model:{v}>")));
}

#[test]
fn at_m_prefix_lists_models() {
    let (_tmp, mut app, backend) = completion_app();
    app.available_models.store(Some(Arc::new(vec![
        "ollama/glm-5".into(),
        "anthropic/claude".into(),
    ])));
    seed_models(&backend, &["ollama/glm-5", "anthropic/claude"]);
    for c in "@m:".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    let specs: Vec<String> = completion_match_items(&app)
        .into_iter()
        .filter(|i| i.kind == "model")
        .map(|i| {
            i.label
                .strip_prefix("model:")
                .map(|s| s.to_string())
                .unwrap_or(i.label)
        })
        .collect();
    assert_eq!(
        specs,
        vec!["ollama/glm-5".to_string(), "anthropic/claude".to_string()]
    );
}

#[test]
fn enter_inserts_subagent_reference_with_trailing_space() {
    let (_tmp, mut app, backend) = completion_app();
    seed_subagents(&backend, "build");
    for c in "@a:res".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "@subagent:research ");
}

#[test]
fn mixed_list_includes_skills_subagents_and_models() {
    let (_tmp, mut app, backend) = completion_app();
    seed_skill(&backend, "review");
    seed_subagents(&backend, "build");
    app.available_models
        .store(Some(Arc::new(vec!["ollama/glm-5".into()])));
    seed_models(&backend, &["ollama/glm-5"]);
    app.update(Msg::Key(key(KeyCode::Char('@'))));
    let deadline = Instant::now() + Duration::from_secs(5);
    let items = loop {
        let _ = app.file_completion.tick();
        let items = completion_match_items(&app);
        if ["skill", "subagent", "model"]
            .into_iter()
            .all(|kind| items.iter().any(|item| item.kind == kind))
        {
            break items;
        }
        assert!(
            Instant::now() < deadline,
            "mixed completion sources did not settle: {:?}",
            items
                .iter()
                .map(|item| (&item.kind, &item.label))
                .collect::<Vec<_>>()
        );
        std::thread::yield_now();
    };
    assert!(items.iter().any(|i| i.kind == "skill"));
    assert!(items.iter().any(|i| i.kind == "subagent"));
    assert!(items.iter().any(|i| i.kind == "model"));
}

// --- submit-time `@` expansion ------------------------------------------------

#[test]
fn submit_expands_subagent_reference_in_place() {
    let (_tmp, app, backend) = completion_app();
    seed_subagents(&backend, "build");
    let input = app.build_agent_input(&queued_msg_with(
        "@subagent:research review this package",
        &app,
    ));
    assert_eq!(input.message, "<subagent:research> review this package");
}

#[test]
fn submit_expands_subagent_model_and_skill_in_place() {
    let (_tmp, app, backend) = completion_app();
    seed_subagents(&backend, "build");
    seed_models(&backend, &["weak"]);
    seed_skill(&backend, "pdf");
    // `@m:weak` and `@skill:pdf` both expand; token order in the message is kept.
    let msg = expand_for_test(&app, "@subagent:general @m:weak @skill:pdf fix the report");
    assert_eq!(
        msg,
        "<subagent:general> <model:weak> <skill:pdf> fix the report"
    );
}

#[test]
fn submit_standalone_model_expands_in_place_no_action() {
    let (_tmp, mut app, backend) = completion_app();
    bind_root(&mut app, parked_root());
    seed_models(&backend, &["ollama/glm-5"]);
    // No ChangeModel: the `@model` path no longer switches the session model.
    assert!(matches!(
        app.submit_prompt(queued_msg("@model:ollama/glm-5 fix the bug")),
        SubmitOutcome::Started(actions) if actions.is_empty()
    ));
    assert_eq!(root_submissions(&app), ["<model:ollama/glm-5> fix the bug"]);
}

#[test]
fn unrecognized_references_pass_through_at_submit() {
    let (_tmp, app, _backend) = completion_app();
    let input = app.build_agent_input(&queued_msg_with("foo@bar @nothing:whatever fix it", &app));
    assert_eq!(input.message, "foo@bar @nothing:whatever fix it");
}

/// Run a message through the same `@`-expansion `submit_prompt` uses, without
/// starting a run.
fn expand_for_test(app: &App, text: &str) -> String {
    app.lua_event_handle
        .expand_references(text)
        .expect("expander rejected a recognized token")
}

/// `build_agent_input` reads already-expanded text; this applies the
/// expansion first so the assertion sees the rewritten message.
fn queued_msg_with(text: &str, app: &App) -> QueuedMessage {
    QueuedMessage {
        text: expand_for_test(app, text),
        images: vec![],
    }
}

// --- Async subagent routing (AC.8, AC.9, AC.10) ------------------------------

struct PromptTurnBehavior;

impl makima_commands::CommandBehavior for PromptTurnBehavior {
    fn execute(
        &self,
        invocation: makima_commands::CommandInvocation,
    ) -> makima_commands::CommandFuture<
        Result<makima_commands::CommandOutcome, makima_commands::CommandError>,
    > {
        Box::pin(async move {
            Ok(makima_commands::CommandOutcome::AgentTurn(
                makima_commands::AgentTurn {
                    content: invocation.content,
                    prompt: Some(makima_commands::PromptReference {
                        qualified_name: Arc::from("fixture/review"),
                        arguments: Arc::from([(Arc::from("subject"), invocation.arguments)]),
                    }),
                },
            ))
        })
    }
}

#[test]
fn child_mcp_command_preserves_full_input_while_root_busy() {
    let (mut app, rx) = app_with_subagent_actor(TASK_ID);
    let child = app.selected_agent_id().unwrap();
    app.queue_and_notify(queued_msg("root pending"));
    let producer = app
        .command_runtime
        .registry
        .create_producer(makima_commands::ProducerPrecedence::Plugin);
    producer
        .replace(vec![makima_commands::Registration {
            spec: makima_commands::CommandSpec {
                name: Arc::from("/fixture-review"),
                aliases: Arc::from([]),
                arguments: test_command_arguments(Some(1), false),
                docs: makima_commands::CommandDocs {
                    summary: Arc::from("MCP prompt fixture"),
                    argument_hint: None,
                },
                required_capabilities: Default::default(),
            },
            behavior: Arc::new(PromptTurnBehavior),
            argument_completions: vec![None],
        }])
        .unwrap();
    app.handle_submit(Submission {
        text: "/fixture-review subject".into(),
        images: vec![ImageSource::new(
            ImageMediaType::Png,
            Arc::from(TEST_IMAGE_DATA),
        )],
    });
    assert!(app.execute_pending_commands().is_empty());
    let input = received_input(&rx);
    assert_eq!(input.images.len(), 1);
    assert_eq!(input.images[0].data.as_ref(), TEST_IMAGE_DATA);
    assert_eq!(
        input.prompt.as_ref().unwrap().qualified_name,
        "fixture/review"
    );
    assert_eq!(
        input
            .prompt
            .as_ref()
            .unwrap()
            .arguments
            .get("subject")
            .map(String::as_str),
        Some("subject")
    );
    assert_eq!(app.selected_agent_id(), Some(child));
    assert_eq!(app.status, Status::Streaming);
    assert_eq!(app.queue.root_text_messages(), ["root pending"]);
}

struct FocusedInputBackend(flume::Sender<AgentInput>);

impl ActorBackend for FocusedInputBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        context: TurnContext,
        input: AgentInput,
        _: makima_agent::WorkKind,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(inbox) = &context.inbox {
                inbox.take_initial_submission();
            }
            self.0.send_async(input).await.unwrap();
            std::future::pending().await
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: &'a makima_agent::ControlWork,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

fn bind_actor_target(app: &App, target: AgentRef) {
    app.command_runtime.sync_admissions(HashMap::from([(
        app.command_target.id(),
        makima_lua::orchestration::TrustedTarget {
            target,
            template: cli_tool_ctx(std::env::current_dir().unwrap()),
        },
    )]));
}

fn spawn_input_child(
    app: &mut App,
    id: &str,
    name: &str,
) -> (AgentRef, flume::Receiver<AgentInput>) {
    spawn_input_child_with_mode(app, id, name, AgentMode::Build)
}

fn spawn_input_child_with_mode(
    app: &mut App,
    id: &str,
    name: &str,
    mode: AgentMode,
) -> (AgentRef, flume::Receiver<AgentInput>) {
    let template = cli_tool_ctx(std::env::current_dir().unwrap());
    let config = EffectiveAgentConfig::new(
        makima_agent::RunSettings {
            provider: template.provider,
            model: (*template.model).clone(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        },
        mode,
    );
    let manager = AgentManagerHandle::new(Default::default()).unwrap();
    let (unused_tx, _) = flume::unbounded();
    let root = manager
        .create_root_with_config(Some(config.clone()), Vec::new(), None, |_| {
            Ok::<_, String>(
                Box::new(FocusedInputBackend(unused_tx)) as Box<dyn makima_agent::ActorBackend>
            )
        })
        .unwrap();
    let (tx, rx) = flume::unbounded();
    let target = manager
        .spawn_child_trusted_with_config(
            &root,
            config,
            Default::default(),
            Vec::new(),
            None,
            |_| {
                Ok::<_, String>(
                    Box::new(FocusedInputBackend(tx)) as Box<dyn makima_agent::ActorBackend>
                )
            },
        )
        .unwrap();
    target
        .actor()
        .unwrap()
        .configure_parent_delivery(false)
        .unwrap();
    let info = subagent_info_for_agent(target.id(), id, name, None, None);
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta { text: "x".into() },
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));
    (target, rx)
}

fn app_with_subagent_actor(id: &str) -> (App, flume::Receiver<AgentInput>) {
    let mut app = streaming_app();
    let (target, rx) = spawn_input_child(&mut app, id, "research");
    app.active_chat = 1;
    bind_actor_target(&app, target);
    (app, rx)
}

fn received_input(rx: &flume::Receiver<AgentInput>) -> AgentInput {
    smol::block_on(async {
        futures_lite::future::race(rx.recv_async(), async {
            smol::Timer::after(SETTLE_TIMEOUT).await;
            panic!("actor did not enter submitted input")
        })
        .await
        .unwrap()
    })
}

#[test]
fn duplicate_child_labels_route_by_agent_id() {
    let mut app = streaming_app();
    let (first, first_rx) = spawn_input_child(&mut app, "duplicate", "first");
    let (second, second_rx) = spawn_input_child(&mut app, "duplicate", "second");
    let first_index = app.live_chat_index[&first.id()];
    let second_index = app.live_chat_index[&second.id()];
    assert_ne!(first_index, second_index);
    app.active_chat = second_index;
    bind_actor_target(&app, second.clone());
    assert!(matches!(
        app.submit_prompt(queued_msg("continue")),
        SubmitOutcome::Queued
    ));
    assert_eq!(received_input(&second_rx).message, "continue");
    assert!(first_rx.try_recv().is_err());
    app.handle_cancel();
    assert!(first.validate_live().is_ok());
    assert!(second.validate_live().is_ok());
}

#[test]
fn tasks_selection_routes_keyboard_submission_to_child() {
    const INPUT: &str = "continue child work";
    let mut app = streaming_app();
    let (target, input_rx) = spawn_input_child(&mut app, TASK_ID, "research");
    app.update(Msg::Key(kb::TASKS.to_key_event()));
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.selected_agent_id(), Some(target.id()));
    bind_actor_target(&app, target);
    for character in INPUT.chars() {
        assert!(
            app.update(Msg::Key(key(KeyCode::Char(character))))
                .is_empty()
        );
    }
    assert!(app.update(Msg::Key(key(KeyCode::Enter))).is_empty());
    assert_eq!(received_input(&input_rx).message, INPUT);
    assert!(app.queue.root_text_messages().is_empty());
}

#[test]
fn submit_in_subagent_chat_routes_to_subagent_queue() {
    let (mut app, input_rx) = app_with_subagent_actor(TASK_ID);
    let outcome = app.submit_prompt(queued_msg("do more"));
    assert!(matches!(outcome, SubmitOutcome::Queued));
    assert_eq!(
        received_input(&input_rx).message,
        "do more",
        "message must go to the subagent queue"
    );
    assert_ne!(app.chats[1].last_message_text(), "do more");
    assert!(app.queue.text_messages().is_empty());
}

#[test_case(false; "main")]
#[test_case(true; "child")]
fn shared_submission_preserves_images_and_preamble(child: bool) {
    const IMAGE_BYTES: usize = 300 * 1024;
    const SHELL_COMMAND: &str = "pwd";
    const SHELL_OUTPUT: &str = "attachment cwd";
    let (mut app, input_rx) = if child {
        app_with_subagent_actor(TASK_ID)
    } else {
        let (tx, rx) = flume::unbounded();
        let root = AgentManagerHandle::new(Default::default())
            .unwrap()
            .create_root_with_config(None, Vec::new(), None, |_| {
                Ok::<_, String>(
                    Box::new(FocusedInputBackend(tx)) as Box<dyn makima_agent::ActorBackend>
                )
            })
            .unwrap();
        (app_with_root(root), rx)
    };
    let target = app.selected_agent_id().unwrap();
    let id = app.shell.reserve_for(Some(target));
    app.handle_shell_event(ShellEvent::Done {
        id,
        command: SHELL_COMMAND.into(),
        output: SHELL_OUTPUT.into(),
        is_error: false,
        visible: true,
    });
    let expected_preamble = serde_json::to_value(app.shell.results_for(Some(target))).unwrap();
    let data: Arc<str> = Arc::from("A".repeat(IMAGE_BYTES));
    let mut msg = queued_msg("hi");
    msg.images
        .push(ImageSource::new(ImageMediaType::Png, Arc::clone(&data)));
    let outcome = app.submit_prompt(msg);
    if child {
        assert!(matches!(outcome, SubmitOutcome::Queued));
    } else {
        assert!(matches!(outcome, SubmitOutcome::Started(_)));
    }
    let input = received_input(&input_rx);
    assert_eq!(input.images.len(), 1);
    assert_eq!(input.images[0].data, data);
    assert_eq!(
        serde_json::to_value(input.preamble).unwrap(),
        expected_preamble
    );
    assert!(app.shell.results_for(Some(target)).is_empty());
}

#[test]
fn submit_after_subagent_completion_routes_to_reusable_child() {
    let (mut app, input_rx) = app_with_subagent_actor(TASK_ID);
    let agent_id = app.chats[app.active_chat].agent_id.unwrap();
    let input_tx = None;
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TurnOutcome(TurnOutcome::completed(
            agent_id,
            TurnId::generate(),
            TokenUsage::default(),
            1,
            DoneReason::EndTurn,
        )),
        subagent: Some(subagent_info_for_agent(
            agent_id, TASK_ID, "research", None, input_tx,
        )),
        identity: Some(work_identity(1)),
    })));

    assert!(app.chats[app.active_chat].is_finished());
    assert!(matches!(
        app.submit_prompt(queued_msg("continue")),
        SubmitOutcome::Queued
    ));
    assert_eq!(received_input(&input_rx).message, "continue");
}

#[test]
fn subagent_closed_marks_despawned_and_rejects_input() {
    const LATE_FAILURE: &str = "late failure";

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let chat_idx = app.active_chat;
    let agent_id = app.chats[chat_idx].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    app.open_tasks();
    assert!(!app.active_subagent_closed());
    assert!(app.task_picker.item(chat_idx).unwrap().is_spinning());
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    assert!(app.active_subagent_closed());
    assert!(app.chats[chat_idx].is_finished());
    assert!(app.task_picker.item(chat_idx).unwrap().is_finished());
    assert!(!app.task_picker.item(chat_idx).unwrap().is_spinning());
    assert_eq!(app.chats[chat_idx].last_message_text(), CANCELLED_TEXT);

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TurnOutcome(TurnOutcome::failed(
            agent_id,
            TurnId::generate(),
            TokenUsage::default(),
            1,
            TurnFailure {
                kind: TurnFailureKind::Provider,
                diagnostic: LATE_FAILURE.into(),
                user_message: LATE_FAILURE.into(),
                retryable: false,
            },
        )),
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));
    assert!(app.chats[chat_idx].is_finished());
    assert_eq!(app.chats[chat_idx].last_message_text(), CANCELLED_TEXT);

    match app.submit_prompt(queued_msg("poke")) {
        SubmitOutcome::Rejected(e) => assert_eq!(e, queue::NO_SUBAGENT_ERR),
        _ => panic!("closed subagent must reject, not start a main turn"),
    }
    assert!(
        app.queue.text_messages().is_empty(),
        "nothing may reach the main queue"
    );
}

#[test]
fn subagent_closed_replaces_active_permission_with_live_root_request() {
    const CHILD_PERMISSION_ID: &str = "child-permission";
    const ROOT_PERMISSION_ID: &str = "root-permission";

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let agent_id = app.chats[app.active_chat].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    let permission = |id: &str| AgentEvent::PermissionRequest {
        id: id.into(),
        tool: makima_config::ToolKey::native(PERM_TOOL),
        scopes: vec![PERM_SCOPE.into()],
    };

    app.update(Msg::Agent(Box::new(Envelope {
        event: permission(CHILD_PERMISSION_ID),
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    app.pending_input = PendingInput::AuthRetry {
        agent_id: Some(agent_id),
    };
    app.update(agent_msg(permission(ROOT_PERMISSION_ID)));
    assert_eq!(app.permission_prompt.agent_id(), Some(agent_id));
    assert_eq!(app.input_queue.len(), 1);

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert!(app.permission_active());
    assert_eq!(app.permission_prompt.agent_id(), None);
    assert!(app.input_queue.is_empty());
    assert_eq!(app.pending_input, PendingInput::None);
}

#[test]
fn subagent_closed_prunes_deferred_permission_before_live_root_request() {
    const CHILD_PERMISSION_ID: &str = "deferred-child-permission";
    const ROOT_PERMISSION_ID: &str = "deferred-root-permission";

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let agent_id = app.chats[app.active_chat].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    let permission = |id: &str| AgentEvent::PermissionRequest {
        id: id.into(),
        tool: makima_config::ToolKey::native(PERM_TOOL),
        scopes: vec![PERM_SCOPE.into()],
    };
    app.last_input = Some(Instant::now());
    app.update(Msg::Agent(Box::new(Envelope {
        event: permission(CHILD_PERMISSION_ID),
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    app.update(agent_msg(permission(ROOT_PERMISSION_ID)));
    assert_eq!(app.input_queue.len(), 2);
    assert!(!app.permission_prompt.is_open());
    app.last_input = None;

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert!(app.permission_active());
    assert_eq!(app.permission_prompt.agent_id(), None);
    assert!(app.input_queue.is_empty());
}

#[test_case(false, DONE_TEXT, &DisplayRole::Done ; "completed")]
#[test_case(true, "failed", &DisplayRole::Error ; "failed")]
fn subagent_closed_preserves_terminal_status(
    failed: bool,
    expected_text: &str,
    expected_role: &DisplayRole,
) {
    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let chat_idx = app.active_chat;
    let agent_id = app.chats[chat_idx].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    if failed {
        app.chats[chat_idx].mark_failed(expected_text);
    } else {
        app.chats[chat_idx].mark_finished(DisplayRole::Done, expected_text);
    }

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert!(app.chats[chat_idx].is_finished());
    assert_eq!(app.chats[chat_idx].last_message_text(), expected_text);
    assert_eq!(app.chats[chat_idx].last_message_role(), Some(expected_role));
}

#[test]
fn final_history_after_subagent_closed_only_persists_messages() {
    const FINAL_REPLY: &str = "final reply";

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let chat_idx = app.active_chat;
    let chat_count = app.chats.len();
    let agent_id = app.chats[chat_idx].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    let messages = vec![Message {
        role: Role::Assistant,
        content: vec![ContentBlock::Text {
            text: FINAL_REPLY.into(),
        }],
        ..Default::default()
    }];
    app.chats[chat_idx].mark_finished(DisplayRole::Done, DONE_TEXT);

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentHistory {
            history_epoch: 0,
            conversation_id: None,
            tool_use_id: TASK_ID.into(),
            messages: messages.clone(),
        },
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert_eq!(
        serde_json::to_value(app.state.session.subagent_messages()[TASK_ID].as_ref()).unwrap(),
        serde_json::to_value(&messages).unwrap()
    );
    assert_eq!(app.chats.len(), chat_count);
    assert_eq!(app.live_chat_index.get(&agent_id), Some(&chat_idx));
    assert!(app.chats[chat_idx].is_finished());
    assert!(app.active_subagent_closed());
    assert!(app.queue.text_messages().is_empty());
}

#[test]
fn turn_complete_after_subagent_closed_only_accounts_usage() {
    const MODEL: &str = "delayed-model";
    const COST: f64 = 0.125;

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let chat_idx = app.active_chat;
    let chat_count = app.chats.len();
    let agent_id = app.chats[chat_idx].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    let usage = TokenUsage {
        input: 100,
        output: 25,
        cache_creation: 10,
        cache_read: 5,
    };
    app.chats[chat_idx].mark_finished(DisplayRole::Done, DONE_TEXT);

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    app.update(Msg::Agent(Box::new(Envelope {
        event: turn_complete(usage, MODEL, Some(COST)),
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert_eq!(app.state.token_usage, usage);
    assert_eq!(app.chats[chat_idx].cost, Some(COST));
    assert_eq!(app.chats.len(), chat_count);
    assert!(app.chats[chat_idx].is_finished());
    assert!(app.active_subagent_closed());
    assert!(app.queue.text_messages().is_empty());

    let tmp = TempDir::new().unwrap();
    let dir = StateDir::from_path(tmp.path().to_path_buf());
    let session_id = app.state.session.id;
    app.state.session_mut().save(&dir).unwrap();
    let persisted = AppSession::load(session_id, &dir).unwrap();
    assert_eq!(persisted.usage_by_model().len(), 1);
    assert_eq!(persisted.usage_by_model()[MODEL], usage.billed(Some(COST)));
}

#[test]
fn turn_complete_after_root_cancel_preserves_child() {
    const MODEL: &str = "cancelled-child-model";
    const COST: f64 = 0.25;

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let chat_idx = app.active_chat;
    let agent_id = app.chats[chat_idx].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    let usage = TokenUsage {
        input: 80,
        output: 20,
        cache_creation: 4,
        cache_read: 2,
    };

    app.active_chat = 0;
    app.handle_cancel();
    assert!(app.live_chat_index.contains_key(&agent_id));
    app.update(Msg::Agent(Box::new(Envelope {
        event: turn_complete(usage, MODEL, Some(COST)),
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert_eq!(app.state.token_usage, usage);
    assert_eq!(
        app.state.session.usage_by_model()[MODEL],
        usage.billed(Some(COST))
    );
    assert!(app.live_chat_index.contains_key(&agent_id));
    assert!(!app.chats[chat_idx].is_finished());
    assert!(app.queue.text_messages().is_empty());
}

#[test_case::test_case(false; "live_child")]
#[test_case::test_case(true; "closed_child")]
fn reconstructed_subagent_history_cannot_resurrect_rewound_history(closed: bool) {
    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let agent_id = app.chats[app.active_chat].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    let earlier = HistorySnapshot::new(vec![Message::user(TASK_ID.into())]);
    let latest = HistorySnapshot::new(Vec::new());
    assert!(
        app.state
            .session_mut()
            .adopt_subagent_history(TASK_ID.into(), &earlier)
    );
    app.apply_history_change(HistoryChange {
        agent_id,
        epoch: latest.epoch,
        messages: Arc::clone(&latest.messages),
        draft: Message::user(String::new()),
    });
    if closed {
        app.update(Msg::Agent(Box::new(Envelope {
            event: AgentEvent::SubagentClosed,
            subagent: Some(info.clone()),
            identity: Some(work_identity(1)),
        })));
    }
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentHistory {
            tool_use_id: TASK_ID.into(),
            history_epoch: earlier.epoch,
            conversation_id: None,
            messages: earlier.messages.as_ref().clone(),
        },
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));
    let projected = app
        .state
        .session
        .subagent_history_snapshot(TASK_ID)
        .unwrap();
    assert_eq!(projected.epoch, latest.epoch);
    assert!(projected.messages.is_empty());
}

#[test]
fn progress_after_subagent_closed_does_not_reopen_chat() {
    const LATE_PROGRESS: &str = "late progress";

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let chat_idx = app.active_chat;
    let agent_id = app.chats[chat_idx].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    app.chats[chat_idx].mark_finished(DisplayRole::Done, DONE_TEXT);

    for event in [
        AgentEvent::SubagentClosed,
        AgentEvent::TextDelta {
            text: LATE_PROGRESS.into(),
        },
    ] {
        app.update(Msg::Agent(Box::new(Envelope {
            event,
            subagent: Some(info.clone()),
            identity: Some(work_identity(1)),
        })));
    }

    app.chats[chat_idx].flush();
    assert!(app.chats[chat_idx].is_finished());
    assert_eq!(app.chats[chat_idx].last_message_text(), DONE_TEXT);
}

#[test]
fn delayed_subagent_closed_after_root_cancel_does_not_recreate_chat() {
    const IN_PROGRESS: &str = "in progress";

    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let agent_id = app.chats[app.active_chat].agent_id.unwrap();
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta {
            text: IN_PROGRESS.into(),
        },
        subagent: Some(info.clone()),
        identity: Some(work_identity(1)),
    })));
    assert!(!app.chats[app.active_chat].is_finished());

    let child_index = app.active_chat;
    app.active_chat = 0;
    app.handle_cancel();
    assert!(!app.chats[child_index].is_finished());
    assert_eq!(app.live_chat_index.get(&agent_id), Some(&child_index));
    let chat_count = app.chats.len();
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert_eq!(app.chats.len(), chat_count);
    assert_eq!(app.live_chat_index.get(&agent_id), Some(&child_index));
    assert!(app.chats[child_index].is_finished());
    assert!(app.subagent_channels.get(&agent_id).unwrap().closed);
}

#[test]
fn subagent_closed_prunes_only_its_presentation_state() {
    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let agent_id = app.chats[app.active_chat].agent_id.unwrap();
    let other_agent_id = AgentId::generate();
    let turn_id = TurnId::generate();
    let other_turn_id = TurnId::generate();
    app.stamped_subagent_outcomes.insert((agent_id, turn_id));
    app.stamped_subagent_outcomes
        .insert((other_agent_id, other_turn_id));
    let info = subagent_info_for_agent(agent_id, TASK_ID, "research", None, None);

    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentClosed,
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));

    assert!(
        !app.stamped_subagent_outcomes
            .iter()
            .any(|(stamped_agent_id, _)| *stamped_agent_id == agent_id)
    );
    assert!(
        app.stamped_subagent_outcomes
            .contains(&(other_agent_id, other_turn_id))
    );
}

#[test]
fn typing_in_subagent_chat_edits_input_and_submits_to_subagent() {
    let (mut app, input_rx) = app_with_subagent_actor(TASK_ID);
    // Typing reaches the shared input box on a focused subagent tab.
    app.update(Msg::Key(key(KeyCode::Char('h'))));
    app.update(Msg::Key(key(KeyCode::Char('i'))));
    assert_eq!(app.input_box.buffer.value(), "hi");
    // Enter submits to the subagent's driver queue, not the main agent.
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(received_input(&input_rx).message, "hi");
}

#[test]
fn paste_in_subagent_chat_edits_input_and_submits_to_subagent() {
    const PASTED: &str = "first\nsecond";

    let (mut app, input_rx) = app_with_subagent_actor(TASK_ID);
    app.update(Msg::Paste("first\r\nsecond".into()));
    assert_eq!(app.input_box.buffer.value(), PASTED);

    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(received_input(&input_rx).message, PASTED);
    assert!(app.chats[0].message_count() == 0);
}

#[test]
fn subagent_history_updates_storage_without_queuing_parent_work() {
    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let message = |text: &str| Message {
        role: Role::Assistant,
        content: vec![ContentBlock::Text { text: text.into() }],
        ..Default::default()
    };
    let history_event = |messages| {
        subagent_msg(
            AgentEvent::SubagentHistory {
                history_epoch: 0,
                conversation_id: None,
                tool_use_id: TASK_ID.to_string(),
                messages,
            },
            TASK_ID,
            None,
        )
    };
    let first_messages = vec![message("the answer")];
    app.update(history_event(first_messages.clone()));
    app.update(history_event(first_messages));

    assert!(app.queue.text_messages().is_empty());

    let changed_messages = vec![message("revised answer")];
    app.update(history_event(changed_messages.clone()));
    app.update(history_event(changed_messages.clone()));
    assert!(app.queue.text_messages().is_empty());

    let mut grown_messages = changed_messages;
    grown_messages.push(message("the follow-up"));
    app.update(history_event(grown_messages.clone()));
    assert!(app.queue.text_messages().is_empty());
    assert_eq!(
        serde_json::to_value(app.state.session.subagent_messages()[TASK_ID].as_ref()).unwrap(),
        serde_json::to_value(&grown_messages).unwrap()
    );
}

#[test]
fn grandchild_history_is_visible_but_not_promoted_to_root() {
    let mut app = streaming_app();
    let parent_id = AgentId::generate();
    let grandchild_id = AgentId::generate();
    let (input_tx, _input_rx) = flume::unbounded();
    let mut grandchild = subagent_info_for_agent(
        grandchild_id,
        "grandchild-task",
        "grandchild",
        None,
        Some(input_tx),
    );
    grandchild.parent_agent_id = Some(parent_id);
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::SubagentHistory {
            history_epoch: 0,
            conversation_id: None,
            tool_use_id: "grandchild-task".into(),
            messages: vec![Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "nested answer".into(),
                }],
                ..Default::default()
            }],
        },
        subagent: Some(grandchild),
        identity: Some(work_identity(1)),
    })));

    assert_eq!(
        app.chats.len(),
        2,
        "grandchild remains visible as a flat row"
    );
    assert!(app.queue.text_messages().is_empty());
}

#[test]
fn task_picker_rows_show_snippet() {
    let (mut app, _input_rx) = app_with_subagent_actor(TASK_ID);
    let info = subagent_info_for_agent(
        app.selected_agent_id().unwrap(),
        TASK_ID,
        "research",
        None,
        None,
    );
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta {
            text: "progress line".into(),
        },
        subagent: Some(info),
        identity: Some(work_identity(1)),
    })));
    app.chats[1].flush();
    app.open_tasks();
    let entry = app.task_picker.item(1).expect("subagent row");
    assert!(
        entry.snippet.contains("progress line"),
        "snippet: {}",
        entry.snippet
    );
}

const BELL_TOOL_NAME: &str = "bash";

fn permission_request_event() -> AgentEvent {
    AgentEvent::PermissionRequest {
        id: "perm-1".into(),
        tool: makima_config::ToolKey::native(BELL_TOOL_NAME),
        scopes: vec!["execute".into()],
    }
}

#[test]
fn turn_complete_emits_bell() {
    let mut app = streaming_app();
    let actions = app.update(done_event());
    assert!(
        actions.iter().any(|a| matches!(a, Action::Bell)),
        "default config should bell on turn complete"
    );
}

#[test]
fn turn_complete_bell_disabled() {
    let mut app = test_app();
    app.ui_config.bell.turn_complete = false;
    app.status = Status::Streaming;
    app.run_id = 1;
    let actions = app.update(done_event());
    assert!(
        !actions.iter().any(|a| matches!(a, Action::Bell)),
        "disabled turn_complete should not bell"
    );
}

#[test]
fn permission_request_emits_bell() {
    let mut app = streaming_app();
    let actions = app.update(agent_msg(permission_request_event()));
    assert!(
        actions.iter().all(|a| !matches!(a, Action::Bell)),
        "bell is routed via pending_bell, not Action::Bell"
    );
    assert!(
        app.take_pending_bell(),
        "default config should bell on permission request"
    );
}

#[test]
fn permission_request_bell_disabled() {
    let mut app = test_app();
    app.ui_config.bell.permission = false;
    app.status = Status::Streaming;
    app.run_id = 1;
    let actions = app.update(agent_msg(permission_request_event()));
    assert!(
        !actions.iter().any(|a| matches!(a, Action::Bell)),
        "disabled permission should not bell"
    );
    assert!(
        !app.take_pending_bell(),
        "disabled permission sets no pending bell"
    );
}

// ---- home-screen splash via the Lua plugin ----

#[test]
fn test_idle_splash_pulls_lua_frame() {
    let (handle, _guard) = makima_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let mut app = build_app_with_handle(dir, writer, handle);
    rendered(&mut app); // assigns the splash area in view
    // Cadence is SMOOTH at startup so ticks keep pulling; the first frame or
    // two can miss the pull timeout while the Lua JIT warms up, so loop.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let all = loop {
        let _ = app.tick();
        let Some(frame) = app.main_chat().splash_frame() else {
            assert!(
                std::time::Instant::now() < deadline,
                "no splash frame pulled"
            );
            continue;
        };
        let all: String = frame.rows.iter().map(|r| r.glyphs.as_str()).collect();
        if all.contains("makima") {
            break all;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "idle splash missed the logo"
        );
    };
    assert!(
        all.contains("makima"),
        "idle splash renders the bundled logo"
    );
}

#[test]
fn slow_splash_renderer_does_not_block_tick_or_input() {
    // Wall-clock spin (os.time): bounded even under CPU starvation, unlike
    // os.clock whose CPU-time wait stretches with load and hogs a core. The
    // render is many times MAX_TICK, so a tick that waits on it trips loudly.
    const RENDER_WALL_SECS: u64 = 2;
    const MAX_TICK: Duration = Duration::from_millis(300);

    let (handle, guard) = makima_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
    guard
        .host()
        .load_source(
            "slow_splash",
            &format!(
                r##"
makima.api.set_slot("splash.render", function(prev, w, h, t, fade)
  local deadline = os.time() + {RENDER_WALL_SECS}
  while os.time() < deadline do end
  return {{ {{ {{ glyphs = string.rep("x", w), style = "#ffffff" }} }} }}
end)
"##
            ),
        )
        .unwrap();
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let mut app = build_app_with_handle(dir, writer, handle);
    rendered(&mut app);

    let started = std::time::Instant::now();
    let _ = app.tick();
    assert!(
        started.elapsed() < MAX_TICK,
        "tick waited for splash render"
    );

    let started = std::time::Instant::now();
    let _ = app.tick();
    assert!(started.elapsed() < MAX_TICK, "pending splash blocked tick");
    app.update(Msg::Key(key(KeyCode::Char('x'))));
    assert_eq!(app.input_box.buffer.value(), "x");
}

#[test]
fn test_splash_lifecycle_events() {
    let (handle, probe) = makima_lua::test_support::probed_event_handle();
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let mut app = build_app_with_handle(dir, writer, handle);

    let _ = app.tick();
    let ev = probe.try_recv_autocmd();
    assert_eq!(
        ev.as_ref().map(|(e, _)| e.as_str()),
        Some("SplashShown"),
        "startup: {ev:?}"
    );

    app.status = Status::Streaming;
    app.run_id = 1;
    app.update(agent_msg(AgentEvent::TextDelta { text: "hi".into() }));
    let _ = app.tick();
    let ev = probe.try_recv_autocmd();
    assert_eq!(
        ev.as_ref().map(|(e, _)| e.as_str()),
        Some("SplashHidden"),
        "on message: {ev:?}"
    );
}

#[test]
fn test_splash_survives_reset_to_empty_session() {
    // Regression: a reset (or a switch to an empty session) swaps in a fresh
    // chat with a fresh splash clock and `splash_shown: false`. The reported
    // bug blanked the splash ~1s later; hold it up past the 1.6s entry fade
    // and fail the moment a frame goes missing.
    let (handle, _guard) = makima_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let mut app = build_app_with_handle(dir, writer, handle.clone());
    rendered(&mut app); // assigns the splash area in view
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let _ = app.tick();
        let Some(frame) = app.main_chat().splash_frame() else {
            assert!(
                std::time::Instant::now() < deadline,
                "no splash frame pulled"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
            continue;
        };
        let all: String = frame.rows.iter().map(|r| r.glyphs.as_str()).collect();
        if all.contains("makima") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "idle splash missed the logo"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // The event loop fires these around a session switch; include them so the
    // Lua side sees the same traffic as production.
    handle.fire_autocmd(
        "SessionFocusChanged",
        serde_json::json!({ "session_id": app.state.session.id }),
    );
    handle.fire_autocmd(
        "SessionStatusChanged",
        serde_json::json!({ "session_id": app.state.session.id, "status": "idle", "focused": true }),
    );
    app.reset_session();
    handle.fire_autocmd(
        "SessionFocusChanged",
        serde_json::json!({ "session_id": app.state.session.id }),
    );
    handle.fire_autocmd(
        "SessionStatusChanged",
        serde_json::json!({ "session_id": app.state.session.id, "status": "idle", "focused": true }),
    );
    let reset_at = std::time::Instant::now();
    let mut timeline = Vec::new();
    loop {
        rendered(&mut app);
        let _ = app.tick();
        let chat = app.main_chat();
        let frame = chat.splash_frame();
        let rows = frame.map(|f| f.rows.len()).unwrap_or(0);
        timeline.push(format!(
            "{:4.1}s rows={} suppressed={} shown={}",
            reset_at.elapsed().as_secs_f32(),
            rows,
            chat.splash_pull_suppressed(),
            chat.splash_shown_flag()
        ));
        let all: String = frame
            .map(|f| f.rows.iter().map(|r| r.glyphs.as_str()).collect())
            .unwrap_or_default();
        assert!(
            all.contains("makima"),
            "splash vanished {}s after the reset to an empty session\n{timeline:?}",
            reset_at.elapsed().as_secs_f32()
        );
        if reset_at.elapsed() > std::time::Duration::from_millis(2200) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[test]
fn test_splash_off_settles_idle() {
    // A still splash only ever animates during its entry fade; after that the
    // cadence is IDLE and no frame is owed. Use a disconnected handle so this
    // test is about the cadence/tick, not host behavior (and immune to the
    // parallel version-change test that bumps the shared update global).
    let (handle, _guard) = (makima_lua::EventHandle::disconnected_for_test(), ());
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let ui = UiConfig {
        splash_animation: false,
        ..Default::default()
    };
    let mut app = build_app_with_full(
        dir,
        writer,
        makima_commands::CommandRegistry::new(),
        handle,
        ui,
    );
    rendered(&mut app); // start the entry fade
    app.main_chat().advance_splash_past_fade();
    let _ = app.tick(); // settle: fade over, still splash is IDLE
    assert_eq!(
        app.tick(),
        Dirty::NO,
        "settled still splash owes no repaint"
    );
}

#[test]
fn test_splash_still_repulls_once_on_version_change() {
    let (handle, _guard) = makima_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let ui = UiConfig {
        splash_animation: false,
        ..Default::default()
    };
    let mut app = build_app_with_full(
        dir,
        writer,
        makima_commands::CommandRegistry::new(),
        handle.clone(),
        ui,
    );
    // Warm the Lua JIT so the still-splash pull and the forced repull below
    // fit inside the pull timeout even under parallel test load.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while handle.splash_frame(80, 20, 0.0, 1.0).is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "splash never warmed up"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    rendered(&mut app);
    let _ = app.tick(); // consume SplashShown, pull the fading still frame
    app.main_chat().advance_splash_past_fade();
    let _ = app.tick(); // settle to IDLE, no further pulls

    assert!(
        crate::update::set_latest_for_test("9.9.9"),
        "changed untouched by prior tests"
    );
    // A newer version forces a repull; a single pull can still come back
    // `Unknown` under load (the force stays armed), so converge on the
    // notice appearing rather than expecting the very first frame.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let _ = app.tick();
        let frame = app.main_chat().splash_frame();
        let all: String = frame
            .map(|f| f.rows.iter().map(|r| r.glyphs.as_str()).collect())
            .unwrap_or_default();
        if all.contains("run makima update to get v9.9.9") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "notice never reached the settled still splash: {all}"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

#[test]
fn run_builtin_model_picker_opens_and_refreshes() {
    let mut app = test_app();
    let actions = app.run_builtin(BuiltinAction::ModelPicker);
    assert!(app.model_picker.is_open());
    assert!(matches!(&actions[..], [Action::RefreshModels]));
}

#[test]
fn alt_m_opens_model_picker() {
    let mut app = test_app();
    let key = KeyEvent {
        code: KeyCode::Char('m'),
        modifiers: KeyModifiers::CONTROL,
        kind: crossterm::event::KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    };
    app.update(Msg::Key(key));
    assert!(app.model_picker.is_open());
}

// ---- defer + z-order input-demand UI (plan 8) ----

const PERM_TOOL: &str = "bash";
const PERM_SCOPE: &str = "execute";
const QUESTION_TEXT: &str = "QUESTIONLINE";

fn perm_demand(id: &str, tool: makima_config::ToolKey, scopes: Vec<String>) -> InputDemand {
    InputDemand {
        kind: InputKind::Permission,
        blocked_by_modal: false,
        hold_until_submit: false,
        perm: Some(PermissionPayload {
            id: id.into(),
            tool,
            scopes,
            agent_id: None,
        }),
    }
}

fn bash_perm_demand(id: &str) -> InputDemand {
    perm_demand(
        id,
        makima_config::ToolKey::native(PERM_TOOL),
        vec![PERM_SCOPE.into()],
    )
}

fn below_input_config(height: u16) -> makima_lua::FloatConfig {
    makima_lua::FloatConfig {
        needs_input: true,
        split: makima_lua::Split::Below,
        height: makima_lua::Dimension::Abs(height),
        ..makima_lua::FloatConfig::default()
    }
}

/// Opens a below+needs_input question window through the shared `handle_open_win`
/// path. Returns whether it went active, plus the window's event/command channels.
fn open_question_win(
    app: &mut App,
    focus: bool,
) -> (
    bool,
    flume::Receiver<makima_lua::WinEvent>,
    flume::Sender<makima_lua::WinCommand>,
) {
    let buf = Arc::new(makima_agent::SharedBuf::new());
    let (event_tx, event_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (cmd_tx, cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    let active = app.handle_open_win(buf, below_input_config(4), focus, event_tx, cmd_rx);
    (active, event_rx, cmd_tx)
}

fn app_with_tall_model_picker() -> App {
    let (mut app, models) = app_with_model_slot();
    let names: Vec<String> = (0..40).map(|i| format!("anthropic/model-{i}")).collect();
    let resolved: Vec<_> = names
        .iter()
        .map(|spec| (spec.as_str(), ModelCapabilities::NONE))
        .collect();
    app.model_picker = ModelPicker::new(
        Arc::clone(&models),
        Arc::new(offline_ids(&[TEST_MODEL_PROVIDER])),
        seeded(&resolved, &[]),
    );
    models.store(Some(Arc::new(names)));
    app.run_builtin(BuiltinAction::ModelPicker);
    assert!(app.model_picker.is_open());
    assert!(app.model_picker.select(0));
    app
}

const RENDER_AREA: Rect = Rect {
    x: 0,
    y: 0,
    width: 80,
    height: 24,
};
/// `last_input` age that puts the app past the 2s deferral window (idle).
const IDLE_AGE: Duration = Duration::from_secs(3);

fn rendered_area(app: &mut App) -> Vec<String> {
    rendered_rows(app, RENDER_AREA.width, RENDER_AREA.height)
}

#[test]
fn permission_deferred_while_typing() {
    let mut app = test_app();
    app.last_input = Some(Instant::now());
    let defer = app.begin_input_demand(bash_perm_demand("perm"));
    assert!(defer, "demand queued while typing");
    assert_eq!(app.input_queue.len(), 1);
    assert!(!app.permission_active());
    assert!(
        !app.permission_prompt.is_open(),
        "queued prompt is not opened (no overlay side effects)"
    );
    app.update(Msg::Key(key(KeyCode::Char('y'))));
    assert!(
        !app.permission_prompt.is_open(),
        "deferred prompt is not answered by typing"
    );
    assert_eq!(app.input_queue.len(), 1, "deferred demand survives the key");
}

#[test]
fn permission_promotes_after_idle() {
    let mut app = test_app();
    app.last_input = Some(Instant::now());
    assert!(app.begin_input_demand(bash_perm_demand("perm")));
    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(
        app.input_queue.is_empty(),
        "queue drained on idle promotion"
    );
    assert!(app.permission_active(), "permission promoted and opened");
    app.update(Msg::Key(key(KeyCode::Char('y'))));
    assert!(
        !app.permission_prompt.is_open(),
        "y answers the active prompt"
    );
}

#[test]
fn permission_promotes_on_modal_close() {
    let mut app = test_app();
    app.run_builtin(BuiltinAction::ModelPicker);
    assert!(app.has_blocking_modal());
    app.last_input = Some(Instant::now());
    let mut demand = bash_perm_demand("perm");
    demand.blocked_by_modal = true;
    assert!(app.begin_input_demand(demand));
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(
        app.input_queue.is_empty(),
        "demand promoted when the modal closed"
    );
    assert!(app.permission_active());
}

#[test]
fn permission_immediate_when_idle() {
    let mut app = test_app();
    app.last_input = None;
    let defer = app.begin_input_demand(bash_perm_demand("perm"));
    assert!(!defer, "idle demand activates immediately");
    assert!(app.input_queue.is_empty());
    assert!(app.permission_active());
    app.update(Msg::Key(key(KeyCode::Char('y'))));
    assert!(
        !app.permission_prompt.is_open(),
        "y answers the active prompt"
    );
}

#[test]
fn permission_drawn_on_top_of_model_picker() {
    let mut app = app_with_tall_model_picker();
    app.permission_prompt.open(
        "perm".into(),
        makima_config::ToolKey::native(PERM_TOOL),
        vec![PERM_SCOPE.into()],
        None,
    );
    app.active_input = Some(InputKind::Permission);
    assert!(app.permission_active());

    let (_msg, bottom, _status, _input, _splits) = app.layout_geometry(RENDER_AREA);
    assert!(
        bottom.height > 0,
        "active permission reserves the bottom area"
    );

    let both_rows = rendered_area(&mut app);
    let both_bottom: String =
        both_rows[bottom.y as usize..(bottom.y + bottom.height) as usize].join("");
    assert!(
        both_bottom.contains("Permission Required"),
        "permission painted on top in the bottom area"
    );
    assert!(
        both_bottom.contains("defer"),
        "Alt+M defer hint is shown on the active prompt"
    );

    app.permission_prompt.close();
    app.active_input = None;
    let picker_rows = rendered_area(&mut app);
    let picker_bottom: String =
        picker_rows[bottom.y as usize..(bottom.y + bottom.height) as usize].join("");
    assert!(
        picker_bottom.contains("model-"),
        "picker content occupies the overlap without the prompt"
    );
    assert!(
        !both_bottom.contains("model-"),
        "permission painted over the picker in the overlap"
    );
}

#[test]
fn permission_hidden_while_deferred() {
    let mut app = test_app();
    let (_msg, _bottom, _status, baseline_input, _splits) = app.layout_geometry(RENDER_AREA);
    app.last_input = Some(Instant::now());
    app.begin_input_demand(bash_perm_demand("perm"));
    let text = rendered(&mut app);
    assert!(
        !text.contains("Permission Required"),
        "deferred prompt is not drawn"
    );
    let (_m, _b, _s, input, _sp) = app.layout_geometry(RENDER_AREA);
    assert!(input.height > 0, "input box visible");
    assert_eq!(
        input.height, baseline_input.height,
        "deferred prompt reserves no bottom area"
    );
}

#[test]
fn question_float_deferred_then_promoted() {
    let mut app = test_app();
    app.last_input = Some(Instant::now());
    let (active, event_rx, _cmd_tx) = open_question_win(&mut app, true);
    assert!(!active, "queued while busy");
    assert!(
        !app.float_mgr.is_focused(),
        "a deferred question float takes no focus"
    );
    assert_eq!(app.input_queue.len(), 1);
    assert!(!app.question_active());

    app.update(Msg::Key(key(KeyCode::Char('y'))));
    assert!(
        event_rx.try_recv().is_err(),
        "deferred float does not receive keys"
    );

    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(
        app.float_mgr.is_focused(),
        "focus_input_window ran on promotion"
    );
    assert!(app.question_active());

    app.update(Msg::Key(key(KeyCode::Char('y'))));
    assert!(event_rx.try_recv().is_ok(), "promoted float receives keys");
}

#[test]
fn question_deferred_behind_focused_popup_waits_for_idle() {
    let mut app = test_app();
    // A focused centered popup (e.g. the sessions board): modal, owns focus.
    let popup_buf = Arc::new(makima_agent::SharedBuf::new());
    let popup_config = makima_lua::FloatConfig {
        split: makima_lua::Split::None,
        height: makima_lua::Dimension::Abs(4),
        ..makima_lua::FloatConfig::default()
    };
    let (popup_tx, _popup_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (_popup_cmd_tx, popup_cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    app.float_mgr
        .open(popup_buf, popup_config, true, popup_tx, popup_cmd_rx);
    assert!(app.float_mgr.is_focused(), "popup is focused");
    assert!(
        app.has_blocking_modal(),
        "focused popup is a blocking modal"
    );

    // Question arrives while the user is busy: it is queued behind the modal.
    app.last_input = Some(Instant::now());
    let (active, _event_rx, _cmd_tx) = open_question_win(&mut app, true);
    assert!(!active, "queued while busy behind a modal");
    assert!(
        app.float_mgr.is_focused(),
        "the in-use popup keeps its focus, not defocused"
    );
    assert!(
        app.has_blocking_modal(),
        "modal still present while deferred"
    );
    assert!(!app.question_active(), "question is not active yet");
    assert_eq!(app.input_queue.len(), 1);

    // Still busy (under 2s): promotion must NOT fire via the modal-close clause,
    // because the modal is still open. It waits for idle.
    let _ = app.tick();
    assert!(
        !app.question_active(),
        "no immediate promotion while busy behind an open modal"
    );
    assert_eq!(app.input_queue.len(), 1);

    // Idle: now it promotes (popup still open, but the user is idle).
    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(app.question_active(), "promotes after idle");
    assert_eq!(app.input_queue.len(), 0);
}

#[test]
fn question_drawn_on_top_when_active() {
    let mut app = app_with_tall_model_picker();
    let buf = Arc::new(makima_agent::SharedBuf::new());
    buf.append(makima_agent::SnapshotLine::plain(QUESTION_TEXT.into()));
    let config = below_input_config(8);
    let (event_tx, _event_rx) = flume::bounded::<makima_lua::WinEvent>(8);
    let (_cmd_tx, cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
    app.float_mgr.open(buf, config, true, event_tx, cmd_rx);
    app.active_input = Some(InputKind::Question);
    assert!(app.question_active());

    let (_msg, _bottom, _status, _input, splits) = app.layout_geometry(RENDER_AREA);
    let below = splits
        .rect(makima_lua::Split::Below)
        .expect("active question split reserves space");

    let both_rows = rendered_area(&mut app);
    let both_below: String =
        both_rows[below.y as usize..(below.y + below.height) as usize].join("");
    assert!(
        both_below.contains(QUESTION_TEXT),
        "question painted on top in the below split"
    );
    assert!(
        both_below.contains("defer"),
        "Alt+M defer hint is shown on the active question float"
    );

    app.float_mgr.close_all();
    app.active_input = None;
    let picker_rows = rendered_area(&mut app);
    let picker_below: String =
        picker_rows[below.y as usize..(below.y + below.height) as usize].join("");
    let overwritten_row = (below.y..below.y + below.height).find(|&row| {
        let row = row as usize;
        !picker_rows[row].trim().is_empty() && picker_rows[row] != both_rows[row]
    });
    assert!(
        overwritten_row.is_some(),
        "question replaces nonempty picker content in the overlap"
    );
    assert!(
        !picker_below.contains(QUESTION_TEXT),
        "question content disappears without the question float"
    );
}

#[test]
fn bell_deferred_until_idle_promotion() {
    let mut app = streaming_app();
    app.last_input = Some(Instant::now());
    let actions = app.update(agent_msg(permission_request_event()));
    assert!(
        actions.iter().all(|a| !matches!(a, Action::Bell)),
        "no Action::Bell on arrival while deferred"
    );
    assert!(
        !app.take_pending_bell(),
        "no bell on arrival while deferred"
    );
    assert_eq!(app.input_queue.len(), 1);
    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(app.permission_active());
    assert!(app.take_pending_bell(), "bell fires on idle promotion");
}

#[test]
fn bell_deferred_until_modal_close_promotion() {
    let mut app = test_app();
    app.run_builtin(BuiltinAction::ModelPicker);
    app.last_input = Some(Instant::now());
    app.status = Status::Streaming;
    app.run_id = 1;
    let actions = app.update(agent_msg(permission_request_event()));
    assert!(
        actions.iter().all(|a| !matches!(a, Action::Bell)),
        "no Action::Bell on arrival behind a modal"
    );
    assert!(
        !app.take_pending_bell(),
        "no bell on arrival while deferred behind a modal"
    );
    assert_eq!(app.input_queue.len(), 1);
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(app.permission_active(), "promoted when the modal closed");
    assert!(
        app.take_pending_bell(),
        "bell fires on modal-close promotion"
    );
}

#[test]
fn active_surface_survives_typing_ticks() {
    let mut app = test_app();
    let (active, _event_rx, _cmd_tx) = open_question_win(&mut app, true);
    assert!(active, "idle question activates immediately");
    assert!(app.question_active());
    app.last_input = Some(Instant::now());
    for _ in 0..3 {
        app.update(Msg::Key(key(KeyCode::Char('a'))));
        let _ = app.tick();
    }
    assert!(
        app.question_active(),
        "active surface is not re-deferred by typing ticks"
    );
    assert!(app.float_mgr.is_focused(), "focus retained across ticks");
    assert!(
        app.input_queue.is_empty(),
        "active surface is not re-queued"
    );
}

#[test]
fn non_input_window_not_deferred() {
    fn open_non_input(split: makima_lua::Split, focus: bool) -> App {
        let mut app = test_app();
        app.last_input = Some(Instant::now());
        // Plan mode with a ready plan: `transition_plan(InteractivePrompt)`
        // would flip it back to drafting, so a ready plan afterwards proves the
        // non-input path never invokes it.
        set_root_mode(&mut app, Mode::Plan);
        app.state.plan = PlanState::Ready(PathBuf::from("test-plan.md"));
        let buf = Arc::new(makima_agent::SharedBuf::new());
        let config = makima_lua::FloatConfig {
            split,
            height: makima_lua::Dimension::Abs(4),
            ..makima_lua::FloatConfig::default()
        };
        let (event_tx, _event_rx) = flume::bounded::<makima_lua::WinEvent>(8);
        let (_cmd_tx, cmd_rx) = flume::bounded::<makima_lua::WinCommand>(8);
        let active = app.handle_open_win(buf, config, focus, event_tx, cmd_rx);
        assert!(active, "non-input window is never deferred");
        assert!(app.input_queue.is_empty(), "queue untouched");
        assert!(!app.take_pending_bell(), "no bell for a non-input window");
        assert!(
            app.state.plan.is_ready(),
            "non-input window does not trigger transition_plan"
        );
        app
    }

    let below = open_non_input(makima_lua::Split::Below, true);
    assert!(
        below.float_mgr.is_focused(),
        "tool-output below split keeps focus"
    );
    let panel = open_non_input(makima_lua::Split::Panel, false);
    assert!(
        !panel.float_mgr.is_focused(),
        "panel with focus=false stays unfocused"
    );
    let popup = open_non_input(makima_lua::Split::None, true);
    assert!(popup.float_mgr.is_focused(), "centered popup keeps focus");
}

#[test]
fn second_demand_enqueues_not_zombie() {
    let mut app = test_app();
    app.last_input = Some(Instant::now());
    app.begin_input_demand(bash_perm_demand("p1"));
    let (active, _event_rx, _cmd_tx) = open_question_win(&mut app, true);
    assert!(!active, "second demand queues while the first is queued");
    assert_eq!(app.input_queue.len(), 2);
    assert!(matches!(app.input_queue[0].kind, InputKind::Permission));
    assert!(matches!(app.input_queue[1].kind, InputKind::Question));
    assert!(!app.permission_active());
    assert!(!app.question_active());
    assert!(
        !app.float_mgr.is_focused(),
        "queued question float is not focused"
    );
    assert!(!app.permission_prompt.is_open());
    assert!(
        !app.take_pending_bell(),
        "no zombie bell on the second arrival"
    );

    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(app.permission_active(), "head permission promotes first");
    assert_eq!(app.input_queue.len(), 1, "question still queued");

    app.update(Msg::Key(key(KeyCode::Char('y'))));
    assert!(!app.permission_active(), "answering closes the permission");
    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(
        app.question_active(),
        "question promotes after the permission resolves"
    );
    assert!(app.input_queue.is_empty());
}

#[test]
fn queued_permission_no_overlay_side_effects() {
    let mut app = test_app();
    app.last_input = Some(Instant::now());
    app.begin_input_demand(bash_perm_demand("p"));
    assert!(
        !app.any_overlay_open(),
        "queued prompt does not count as an overlay"
    );
    assert!(!app.permission_prompt.is_open());
    assert!(!app.has_modal_overlay(), "no modal blocks input clicks");
    let (_msg, _bottom, _status, input, _splits) = app.layout_geometry(RENDER_AREA);
    assert!(input.height > 0, "input box present");
    let _ = rendered(&mut app);
    let zone = app
        .zone_at(input.y + 1, input.x + 1)
        .expect("input position has a zone");
    assert_eq!(
        zone.zone,
        SelectionZone::Input,
        "Input zone is not shadowed by an Overlay while a permission is queued"
    );
}

// ---- manual Alt+M defer (plan 9) ----

fn alt_m() -> KeyEvent {
    KeyEvent {
        code: KeyCode::Char('m'),
        modifiers: KeyModifiers::ALT,
        kind: crossterm::event::KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    }
}

#[test]
fn alt_m_when_nothing_active_is_noop() {
    let mut app = test_app();
    app.last_input = None;
    let actions = app.update(Msg::Key(alt_m()));
    assert!(actions.is_empty(), "Alt+M is consumed without side effects");
    assert!(app.input_queue.is_empty());
    assert!(!app.permission_active());
    assert!(!app.question_active());
}

#[test]
fn alt_m_defers_active_permission_until_submit() {
    let mut app = test_app();
    app.last_input = None;
    assert!(
        !app.begin_input_demand(bash_perm_demand("perm")),
        "idle demand activates immediately"
    );
    assert!(app.permission_active());

    app.update(Msg::Key(alt_m()));
    assert!(!app.permission_active(), "Alt+M hides the active prompt");
    assert!(!app.permission_prompt.is_open(), "prompt closed on defer");
    assert_eq!(app.input_queue.len(), 1, "deferred demand is queued");
    assert!(
        app.input_queue[0].hold_until_submit,
        "manual defer holds until submit"
    );

    // The 2s idle timer must NOT release a manual hold.
    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(!app.permission_active(), "hold ignores the idle timer");
    assert!(!app.permission_prompt.is_open(), "prompt still hidden");
    assert_eq!(app.input_queue.len(), 1);

    // Typing in the freed input box must not answer the deferred prompt.
    app.update(Msg::Key(key(KeyCode::Char('h'))));
    assert!(
        !app.permission_prompt.is_open(),
        "typing does not misanswer"
    );

    // Submitting the focused input box releases the hold and re-promotes.
    let actions = app.handle_submit(Submission::empty());
    assert!(actions.is_empty(), "empty submit yields nothing");
    assert!(app.submit_released, "submit arms the release");
    let _ = app.promote_deferred_if_ready();
    assert!(app.input_queue.is_empty());
    assert!(app.permission_active(), "prompt re-promotes after submit");
}

#[test]
fn alt_m_defers_active_question_until_submit() {
    let mut app = test_app();
    app.last_input = None;
    let (active, _event_rx, _cmd_tx) = open_question_win(&mut app, true);
    assert!(active, "idle question activates immediately");
    assert!(app.question_active());
    assert!(app.float_mgr.is_focused());

    app.update(Msg::Key(alt_m()));
    assert!(!app.question_active(), "Alt+M defers the question float");
    assert!(!app.float_mgr.is_focused(), "float releases focus on defer");
    assert_eq!(app.input_queue.len(), 1);
    assert!(app.input_queue[0].hold_until_submit);

    // The 2s idle timer must NOT release a manual hold either.
    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(!app.question_active(), "hold ignores the idle timer");
    assert!(!app.float_mgr.is_focused(), "float stays unfocused");
    assert_eq!(app.input_queue.len(), 1);

    let _ = app.handle_submit(Submission::empty());
    let _ = app.promote_deferred_if_ready();
    assert!(app.input_queue.is_empty());
    assert!(app.question_active(), "float re-promotes after submit");
    assert!(
        app.float_mgr.is_focused(),
        "focus restored on question promotion"
    );
}

#[test]
fn alt_m_toggles_back_to_the_held_permission() {
    let mut app = test_app();
    app.last_input = None;
    assert!(!app.begin_input_demand(bash_perm_demand("perm")));
    assert!(app.permission_active());

    app.update(Msg::Key(alt_m()));
    assert!(!app.permission_active());
    assert!(app.held_input_pending(), "held demand reports pending");

    let actions = app.update(Msg::Key(alt_m()));
    assert!(actions.is_empty(), "toggle yields no agent actions");
    assert!(
        app.permission_active(),
        "second Alt+M restores the held prompt"
    );
    assert!(app.input_queue.is_empty());
    assert!(!app.held_input_pending());
}

#[test]
fn alt_m_toggle_ignores_auto_deferred_head() {
    // An auto-deferral (typing window) at the head promotes via the idle
    // timer; Alt+M must not force it, nor reach past FIFO order.
    let mut app = test_app();
    app.last_input = Some(Instant::now());
    assert!(app.begin_input_demand(bash_perm_demand("perm")));
    assert!(!app.held_input_pending());

    app.update(Msg::Key(alt_m()));
    assert!(
        !app.permission_active(),
        "auto-deferred head is not promoted by Alt+M"
    );
    assert_eq!(app.input_queue.len(), 1);

    app.last_input = Some(Instant::now() - IDLE_AGE);
    let _ = app.tick();
    assert!(app.permission_active(), "idle timer still releases it");
}

#[test]
fn defer_hint_pins_above_status_bar_until_restored() {
    let hint_row = |rows: &[String]| rows[RENDER_AREA.height as usize - 2].clone();

    let mut app = test_app();
    app.last_input = None;
    assert!(!app.begin_input_demand(bash_perm_demand("perm")));
    let baseline = rendered_area(&mut app);
    assert!(
        !hint_row(&baseline).contains("Undefer"),
        "no hint while the prompt is active"
    );

    app.update(Msg::Key(alt_m()));
    let deferred = rendered_area(&mut app);
    let row = hint_row(&deferred);
    assert!(
        row.contains("Undefer pending model input"),
        "hint appears once the demand is deferred"
    );
    assert!(
        row.starts_with('('),
        "the hint sits flush with the left edge: {row:?}"
    );

    app.update(Msg::Key(alt_m()));
    let restored = rendered_area(&mut app);
    assert!(
        !hint_row(&restored).contains("Undefer"),
        "hint clears once the demand is restored"
    );
}

#[test]
fn plan_ready_fires_once_per_draft() {
    let mut app = test_app();
    set_root_mode(&mut app, Mode::Plan);
    let tmp = tempfile::tempdir().unwrap();
    let state_dir = StateDir::from_path(tmp.path().to_path_buf());
    app.state.plan = PlanState::Drafting(makima_storage::plans::new_plan_path(&state_dir).unwrap());
    assert!(!app.state.plan.is_ready());

    // First WriteDone marks plan ready
    app.transition_plan(PlanTrigger::WriteDone);
    assert!(app.state.plan.is_ready());

    // Duplicate WriteDone while still ready does not transition again
    app.transition_plan(PlanTrigger::WriteDone);
    assert!(app.state.plan.is_ready());

    // Interactive prompt marks plan drafting again
    app.transition_plan(PlanTrigger::InteractivePrompt);
    assert!(!app.state.plan.is_ready());

    // Subsequent WriteDone marks plan ready for the new draft
    app.transition_plan(PlanTrigger::WriteDone);
    assert!(app.state.plan.is_ready());
}

#[test]
fn ctrl_b_asks_the_session_to_move_the_running_call() {
    let mut app = test_app();
    let actions = app.update(Msg::Key(kb::BACKGROUND.to_key_event()));
    assert!(matches!(
        actions.as_slice(),
        [Action::MoveToBackground { .. }]
    ));
}

#[test]
fn a_running_tool_hints_the_background_key_until_it_moves() {
    const HINT: &str = "Ctrl+B to background";
    let mut app = streaming_app();
    app.update(agent_msg(tool_start("call_hint", "bash")));
    assert!(
        rendered(&mut app).contains(HINT),
        "a running call must name the key that moves it"
    );

    app.update(agent_msg(AgentEvent::ToolMoved {
        id: "call_hint".into(),
        tool: "bash".into(),
        summary: "call_hint".into(),
    }));
    assert!(
        !rendered(&mut app).contains(HINT),
        "a moved call must drop the hint"
    );
}

#[test]
fn a_moved_call_appears_in_tasks_until_it_finishes() {
    let mut app = test_app();
    app.update(agent_msg(AgentEvent::ToolMoved {
        id: "call_moved".into(),
        tool: "bash".into(),
        summary: "cargo test".into(),
    }));

    let entry = app
        .task_entries()
        .into_iter()
        .find(|entry| matches!(&entry.identity, TaskIdentity::Background(id) if id == "call_moved"))
        .expect("a moved call is a task");
    assert_eq!(entry.name, "cargo test");
    assert_eq!(entry.finished, Some(false), "a running call spins");

    app.update(agent_msg(AgentEvent::BackgroundDone(Box::new(
        ToolDoneEvent {
            id: "call_moved".into(),
            tool: "bash".into(),
            output: ToolOutput::Plain("done".into()),
            is_error: false,
            annotation: None,
            written_path: None,
        },
    ))));

    let entry = app
        .task_entries()
        .into_iter()
        .find(|entry| matches!(&entry.identity, TaskIdentity::Background(id) if id == "call_moved"))
        .expect("a finished call stays listed");
    assert_eq!(entry.finished, Some(true));
    assert_eq!(
        app.state
            .session
            .tool_outputs()
            .get("call_moved")
            .map(|o| o.as_text()),
        Some("done".into())
    );
}

#[test]
fn a_lost_background_call_reads_as_failed() {
    let mut app = test_app();
    app.update(agent_msg(AgentEvent::ToolMoved {
        id: "call_lost".into(),
        tool: "bash".into(),
        summary: "cargo test".into(),
    }));
    app.update(agent_msg(AgentEvent::BackgroundLost {
        id: "call_lost".into(),
        tool: "bash".into(),
        reason: "worker died".into(),
    }));

    let entry = app
        .task_entries()
        .into_iter()
        .find(|entry| matches!(&entry.identity, TaskIdentity::Background(id) if id == "call_lost"))
        .expect("a lost call stays listed");
    assert_eq!(entry.finished, Some(true));
    assert_eq!(entry.context, "failed");
    let output = app
        .state
        .session
        .tool_outputs()
        .get("call_lost")
        .map(|o| o.as_text())
        .expect("the row keeps a lost notice");
    assert!(output.contains("lost"), "{output}");
}

#[test]
fn selecting_a_background_task_row_renders_its_chat() {
    let mut app = test_app();
    app.update(agent_msg(AgentEvent::ToolMoved {
        id: "call_moved".into(),
        tool: "bash".into(),
        summary: "cargo test".into(),
    }));
    app.open_tasks();
    let index = app
        .task_picker
        .items()
        .unwrap()
        .iter()
        .position(|entry| matches!(&entry.identity, TaskIdentity::Background(_)))
        .expect("the moved call is listed");
    app.task_picker.select(index);
    assert_eq!(app.resolve_render_chat(), 0);
}
