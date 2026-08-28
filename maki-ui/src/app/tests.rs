use super::*;
use crate::agent::shared_queue;
use crate::chat::{CANCELLED_TEXT, DONE_TEXT, ERROR_TEXT};
use crate::components::btw_modal::BtwEvent;
use crate::components::command::ParsedCommand;
use crate::components::file_picker::EMPTY_DIR_MSG;
use crate::components::keybindings::{KeybindContext, key as kb};
use crate::components::{ExitRequest, buffer_text, key, test_model};
use crate::repaint::expect::{OWED, QUIET};
use crate::selection::{SelectableZone, SelectionState, SelectionZone};
use arc_swap::ArcSwap;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEventKind};
use maki_agent::command::{CommandScope, CustomCommand};
use maki_agent::permissions::PermissionManager;
use maki_agent::{
    DoneReason, ImageMediaType, McpConfigErrors, McpServerInfo, McpServerStatus, McpSnapshot,
    McpSnapshotReader, ModeDefSpec, ToolDoneEvent, ToolOutput, ToolStartEvent, TurnCompleteEvent,
};
use maki_config::{PermissionsConfig, UiConfig};
use maki_lua::test_support::{HintWriterHandle, hint_writer_pair};
use maki_lua::{BuiltinAction, CommandArgumentItem, HintReader, KeymapReader};
use maki_providers::{
    ContentBlock, Effort, Message, ProviderUsage, Role, TokenUsage, UsageLimit, UsageWindow,
};
use maki_storage::sessions::{StoredMode, StoredSubagent, StoredThinking};
use ratatui::layout::Rect;
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use test_case::test_case;

const WRITER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const TASK_ID: &str = "task1";
const SUB_TOOL_ID: &str = "sub_t1";
const TOOL_OUTPUT_LINE: &str = "hello from the subagent";
const LATE_MODEL_SPEC: &str = "zai/glm-5";
const MODEL_SPEC_GLM4: &str = "zai/glm-4";
const MODEL_SPEC_OPUS: &str = "anthropic/claude-opus-4-5";
const MODEL_SPEC_GPT9: &str = "openai/gpt-9";
const MODEL_SPEC_CLAUDE: &str = "anthropic/claude-sonnet-4-20250514";
const HINT_PLUGIN: &str = "statusline";
const HINT_TEXT: &str = "2/4 staged";
const HINT_STYLE: &str = "fg";
const RETRY_MESSAGE: &str = "overloaded";
const RETRY_DELAY: Duration = Duration::from_secs(5);
const MISSING_DIR: &str = "gone";
const WALK_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_IMAGE_DATA: &str = "dGVzdA==";
const LOCAL_COMMAND_ATTACHMENTS_ERROR: &str =
    "command failed: local commands cannot include non-text content";
const FAST_UNSUPPORTED_COMMAND_ERROR: &str =
    "command failed: Fast mode requires an Anthropic Opus 4.6+ model (API only)";

fn set_zone(app: &mut App, zone: SelectionZone, area: Rect) {
    app.zones.push(SelectableZone { area, zone });
}

fn build_app(dir: StateDir, writer: Arc<StorageWriter>) -> App {
    build_app_with_registry(dir, writer, maki_commands::CommandRegistry::new())
}

fn build_app_with_handle(
    dir: StateDir,
    writer: Arc<StorageWriter>,
    handle: maki_lua::EventHandle,
) -> App {
    build_app_with_full(
        dir,
        writer,
        maki_commands::CommandRegistry::new(),
        handle,
        UiConfig::default(),
    )
}

fn build_app_with_registry(
    dir: StateDir,
    writer: Arc<StorageWriter>,
    registry: maki_commands::CommandRegistry,
) -> App {
    build_app_with_full(
        dir,
        writer,
        registry,
        maki_lua::EventHandle::disconnected_for_test(),
        UiConfig::default(),
    )
}

#[derive(Clone)]
struct TestLuaBehavior {
    handle: maki_lua::EventHandle,
    plugin: Arc<str>,
    name: Arc<str>,
}

impl maki_commands::CommandBehavior for TestLuaBehavior {
    fn execute(
        &self,
        invocation: maki_commands::CommandInvocation,
    ) -> maki_commands::CommandFuture<
        Result<maki_commands::CommandOutcome, maki_commands::CommandError>,
    > {
        self.handle.run_command(
            Arc::clone(&self.plugin),
            Arc::clone(&self.name),
            invocation.arguments.to_string(),
            invocation.depth as u8,
        );
        Box::pin(async { Ok(maki_commands::CommandOutcome::Completed) })
    }
}

#[derive(Clone)]
struct TestLuaCompletion {
    handle: maki_lua::EventHandle,
    plugin: Arc<str>,
}

impl maki_commands::CommandCompletion for TestLuaCompletion {
    fn complete(
        &self,
        context: maki_commands::CompletionContext,
        _cancellation: maki_commands::CancellationToken,
    ) -> maki_commands::CommandFuture<
        Result<Vec<maki_commands::CompletionItem>, maki_commands::CompletionError>,
    > {
        let (_, cancel) = maki_agent::CancelToken::new();
        let context = maki_lua::CommandArgumentContext {
            command: context.invoked_name,
            plugin: Arc::clone(&self.plugin),
            args: context.arguments.to_string(),
            arg: context.argument.to_string(),
            index: context.argument_index,
            mode: context.mode.to_string(),
            session: 1,
            generation: 0,
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
                .map(|item| maki_commands::CompletionItem {
                    label: Arc::from(item.label),
                    insertion: Arc::from(item.insertion),
                    description: item.description.map(Arc::from),
                })
                .collect())
        })
    }

    fn lifecycle(
        &self,
        context: &maki_commands::CompletionContext,
        event: &maki_commands::CompletionLifecycleEvent,
        _cancellation: &maki_commands::CancellationToken,
    ) -> Result<(), maki_commands::CompletionError> {
        let (event, item) = match event {
            maki_commands::CompletionLifecycleEvent::Highlight(item) => (
                maki_lua::CommandArgumentLifecycle::Highlight,
                Some(maki_lua::CommandArgumentItem {
                    label: item.label.to_string(),
                    insertion: item.insertion.to_string(),
                    description: item.description.as_ref().map(ToString::to_string),
                }),
            ),
            maki_commands::CompletionLifecycleEvent::Accept(item) => (
                maki_lua::CommandArgumentLifecycle::Accept,
                Some(maki_lua::CommandArgumentItem {
                    label: item.label.to_string(),
                    insertion: item.insertion.to_string(),
                    description: item.description.as_ref().map(ToString::to_string),
                }),
            ),
            maki_commands::CompletionLifecycleEvent::Cancel => {
                (maki_lua::CommandArgumentLifecycle::Cancel, None)
            }
        };
        self.handle.command_argument_lifecycle(
            maki_lua::CommandArgumentContext {
                command: Arc::clone(&context.invoked_name),
                plugin: Arc::clone(&self.plugin),
                args: context.arguments.to_string(),
                arg: context.argument.to_string(),
                index: context.argument_index,
                mode: context.mode.to_string(),
                session: 1,
                generation: 0,
            },
            event,
            item,
            maki_agent::CancelToken::none(),
        );
        Ok(())
    }
}

struct TestLuaCommand {
    handle: maki_lua::EventHandle,
    name: Arc<str>,
    plugin: Arc<str>,
    max_args: Option<usize>,
    completion: bool,
}

fn register_test_lua_command(
    registry: &maki_commands::CommandRegistry,
    command: TestLuaCommand,
) -> maki_commands::Producer {
    let producer = registry.create_producer(maki_commands::ProducerPrecedence::Plugin);
    let completion = command.completion.then(|| {
        Arc::new(TestLuaCompletion {
            handle: command.handle.clone(),
            plugin: Arc::clone(&command.plugin),
        }) as Arc<dyn maki_commands::CommandCompletion>
    });
    producer
        .replace(vec![maki_commands::Registration {
            spec: maki_commands::CommandSpec {
                name: Arc::clone(&command.name),
                aliases: Arc::from([]),
                arguments: command
                    .max_args
                    .map(|max| maki_commands::ArgumentArity::bounded(0, max))
                    .unwrap_or_else(|| maki_commands::ArgumentArity::unbounded(0)),
                docs: maki_commands::CommandDocs {
                    summary: Arc::from("Lua test command"),
                    argument_hint: None,
                },
                required_capabilities: maki_commands::TargetCapabilities::default(),
            },
            behavior: Arc::new(TestLuaBehavior {
                handle: command.handle,
                plugin: command.plugin,
                name: command.name,
            }),
            completion,
        }])
        .unwrap();
    producer
}

fn lua_registry(
    command: TestLuaCommand,
) -> (maki_commands::CommandRegistry, maki_commands::Producer) {
    let registry = maki_commands::CommandRegistry::new();
    let producer = register_test_lua_command(&registry, command);
    (registry, producer)
}

fn build_app_with_full(
    dir: StateDir,
    writer: Arc<StorageWriter>,
    registry: maki_commands::CommandRegistry,
    handle: maki_lua::EventHandle,
    ui: UiConfig,
) -> App {
    let model = test_model();
    let command_runtime = Arc::new(crate::command_runtime::CommandRuntime::new_for_test(
        &[],
        registry,
        Arc::new(crate::components::arg_completion::ModelArgSource::new(
            Arc::new(ArcSwapOption::empty()),
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
        writer,
        ui,
        100,
        Arc::new(PermissionManager::new(
            PermissionsConfig {
                rules: vec![],
                ..Default::default()
            },
            PathBuf::from("/tmp"),
            Arc::default(),
        )),
        handle,
        Arc::new(maki_config::ModelPolicy::default()),
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
    let handle = maki_lua::EventHandle::disconnected_for_test();
    let command_runtime = Arc::new(crate::command_runtime::CommandRuntime::new_for_test(
        commands,
        maki_commands::CommandRegistry::new(),
        Arc::new(crate::components::arg_completion::ModelArgSource::new(
            Arc::new(ArcSwapOption::empty()),
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
        writer,
        UiConfig::default(),
        100,
        Arc::new(PermissionManager::new(
            PermissionsConfig {
                rules: vec![],
                ..Default::default()
            },
            PathBuf::from("/tmp"),
            Arc::default(),
        )),
        handle,
        Arc::new(ModelPolicy::default()),
        Arc::new(crate::theme::InMemoryThemesProvider::bundled()),
        command_runtime,
    );
    let (shared_queue, _rx) = shared_queue::queue();
    app.queue.set_shared(shared_queue);
    app
}

pub(crate) fn test_app() -> App {
    let dir = StateDir::from_path(env::temp_dir());
    let mut app = build_app(dir.clone(), Arc::new(test_writer(dir)));
    let (shared_queue, _rx) = shared_queue::queue();
    app.queue.set_shared(shared_queue);
    app
}

/// A `test_app` past its idle splash, whose drifting starfield would mask
/// every other cadence.
fn app_without_splash() -> App {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.update(agent_msg(AgentEvent::TextDelta { text: "hi".into() }));
    app.update(done_event());
    app
}

/// Hands back the slot providers publish their model lists into, since the app
/// keeps no handle to it once the picker owns it.
fn app_with_model_slot() -> (App, Arc<ArcSwapOption<Vec<String>>>) {
    let models = Arc::new(ArcSwapOption::empty());
    let mut app = test_app();
    app.model_picker = ModelPicker::new(Arc::clone(&models));
    app.available_models = Arc::clone(&models);
    (app, models)
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
    let app = build_app(dir.clone(), Arc::clone(&writer));
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

fn agent_msg_with_run_id(event: AgentEvent, run_id: u64) -> Msg {
    Msg::Agent(Box::new(Envelope {
        event,
        subagent: None,
        run_id,
    }))
}

fn done() -> AgentEvent {
    AgentEvent::Done {
        usage: TokenUsage::default(),
        num_turns: 1,
        reason: DoneReason::EndTurn,
    }
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

fn subagent_info_full(
    parent_id: &str,
    name: &str,
    answer_tx: Option<flume::Sender<String>>,
    input_tx: Option<flume::Sender<String>>,
) -> SubagentInfo {
    SubagentInfo {
        parent_tool_use_id: parent_id.into(),
        name: name.into(),
        prompt: None,
        model: None,
        answer_tx,
        input_tx,
    }
}

fn subagent_msg(event: AgentEvent, parent_id: &str, name: Option<&str>) -> Msg {
    subagent_msg_with_run_id(event, parent_id, name, 1)
}

fn subagent_msg_with_run_id(
    event: AgentEvent,
    parent_id: &str,
    name: Option<&str>,
    run_id: u64,
) -> Msg {
    Msg::Agent(Box::new(Envelope {
        event,
        subagent: Some(subagent_info(parent_id, name.unwrap_or("Agent"))),
        run_id,
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
        run_id: 1,
    }))
}

fn subagent_msg_with_model(event: AgentEvent, parent_id: &str, name: &str, model: &str) -> Msg {
    let mut info = subagent_info(parent_id, name);
    info.model = Some(model.into());
    Msg::Agent(Box::new(Envelope {
        event,
        subagent: Some(info),
        run_id: 1,
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

    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(&actions[0], Action::SendMessage(s) if s.message == "hi"));
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
#[test_case(AgentEvent::Error { message: "boom".into() }, ExitRequest::Error ; "error_exits_error")]
fn exit_on_done_flag_triggers_exit(event: AgentEvent, expected: ExitRequest) {
    let mut app = test_app();
    app.exit_on_done = true;
    app.status = Status::Streaming;
    app.run_id = 1;
    app.update(agent_msg(event));
    assert_eq!(app.exit_request, expected);
}

#[test]
fn toggle_mode_state_machine() {
    let tab = |app: &mut App| app.update(Msg::Key(key(KeyCode::Tab)));

    let mut app = test_app();
    assert_eq!(app.state.mode, Mode::Build);

    tab(&mut app);
    assert_eq!(app.state.mode, Mode::Plan);
    let first_path = app.state.plan.path().unwrap().to_path_buf();
    assert!(first_path.to_str().unwrap().contains("plans"));

    tab(&mut app);
    assert_eq!(app.state.mode, Mode::Build);
    assert!(!app.state.plan.is_ready());

    tab(&mut app);
    assert_eq!(app.state.mode, Mode::Plan);
    assert_eq!(app.state.plan.path().unwrap(), first_path);

    app.state.plan.mark_ready();
    tab(&mut app);
    assert_eq!(app.state.mode, Mode::Build);
    assert!(app.state.plan.is_ready());
    assert_eq!(app.state.plan.path().unwrap(), first_path);

    app.state.mode = Mode::Build;
    app.status = Status::Streaming;
    app.run_id = 1;
    tab(&mut app);
    assert_eq!(app.state.mode, Mode::Plan);
    assert_eq!(app.state.plan.path().unwrap(), first_path);
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
    app.state.mode = Mode::Plan;
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
    let handle = maki_lua::EventHandle::disconnected_for_test();
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
    app.update(Msg::Paste("file:///tmp/nonexistent.png".into()));
    assert!(!app.image_paste_rx.is_empty());
    assert_eq!(app.input_box.buffer.value(), "");
}

#[test]
fn submit_during_streaming_queues_message() {
    let mut app = test_app();
    app.update(Msg::Key(key(KeyCode::Char('a'))));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(&actions[0], Action::SendMessage(_)));
    assert_eq!(app.status, Status::Streaming);

    app.update(Msg::Key(key(KeyCode::Char('b'))));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(actions.is_empty());
    assert_eq!(app.queue.len(), 1);
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
            image_count: 0,
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

/// Restored queue items start runs without `start_run`, so the consumed
/// event is the only signal that the agent went busy: it must flip status
/// or the busy-guard and esc-to-cancel stay off during the whole run.
#[test]
fn queue_item_consumed_marks_agent_streaming() {
    let mut app = test_app();
    assert_eq!(app.status, Status::Idle);

    app.update(agent_msg_with_run_id(
        AgentEvent::QueueItemConsumed {
            text: "restored".into(),
            image_count: 0,
        },
        app.run_id,
    ));

    assert_eq!(app.status, Status::Streaming);
}

#[test_case(error_app as fn(&mut App) ; "error")]
#[test_case(cancel_app as fn(&mut App) ; "cancel")]
fn clears_queue(terminate: fn(&mut App)) {
    let mut app = app_with_queued_message();
    terminate(&mut app);
    assert!(app.queue.is_empty());
}

#[test_case("/compact" ; "slash_command")]
#[test_case("exit" ; "exit_keyword")]
#[test_case("!ls" ; "shell_prefix")]
fn submit_prompt_never_interprets_text(text: &str) {
    let mut app = test_app();
    match app.submit_prompt(queued_msg(text)) {
        SubmitOutcome::Started(actions) => {
            assert!(matches!(&actions[0], Action::SendMessage(_)))
        }
        _ => panic!("raw prompt must start the agent"),
    }
}

#[test]
fn submit_prompt_queues_while_streaming() {
    let mut app = test_app();
    app.status = Status::Streaming;
    assert!(matches!(
        app.submit_prompt(queued_msg("hi")),
        SubmitOutcome::Queued
    ));
    assert_eq!(app.queue.len(), 1);
}

#[test_case(test_app as fn() -> App, "   ", queue::EMPTY_PROMPT_ERR ; "blank_text")]
#[test_case(streaming_app_without_queue, "hi", queue::NO_QUEUE_ERR ; "streaming_without_shared_queue")]
fn submit_prompt_rejects(mk: fn() -> App, text: &str, expected: &str) {
    let mut app = mk();
    match app.submit_prompt(queued_msg(text)) {
        SubmitOutcome::Rejected(e) => assert_eq!(e, expected),
        _ => panic!("expected rejection"),
    }
}

fn streaming_app_without_queue() -> App {
    let dir = StateDir::from_path(env::temp_dir());
    let mut app = build_app(dir.clone(), Arc::new(test_writer(dir)));
    app.status = Status::Streaming;
    app
}

fn queued_msg(text: &str) -> QueuedMessage {
    QueuedMessage {
        text: text.into(),
        images: vec![],
    }
}

fn app_with_queued_message() -> App {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.queue_and_notify(queued_msg("queued"));
    app
}

fn type_and_submit(app: &mut App, text: &str) -> Vec<Action> {
    for c in text.chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    app.update(Msg::Key(key(KeyCode::Enter)))
}

fn cancel_app(app: &mut App) {
    app.last_esc = Some(Instant::now());
    app.update(Msg::Key(key(KeyCode::Esc)));
}

fn error_app(app: &mut App) {
    app.update(agent_msg(AgentEvent::Error {
        message: "boom".into(),
    }));
}

fn cmd(name: &str) -> ParsedCommand {
    ParsedCommand {
        name: name.to_string(),
        args: String::new(),
    }
}

fn type_slash(app: &mut App) {
    app.update(Msg::Key(key(KeyCode::Char('/'))));
}

#[test]
fn typing_filters_palette() {
    let mut app = test_app();
    type_slash(&mut app);
    app.update(Msg::Key(key(KeyCode::Char('n'))));
    assert!(app.command_palette.is_active());

    app.update(Msg::Key(key(KeyCode::Char('z'))));
    assert!(!app.command_palette.is_active());
}

#[test]
fn enter_executes_new_command() {
    let mut app = test_app();
    type_slash(&mut app);
    app.update(Msg::Key(key(KeyCode::Char('n'))));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(matches!(&actions[0], Action::NewSession));
    assert!(!app.command_palette.is_active());
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
    maki_lua::test_support::RequestProbe,
    maki_commands::Producer,
) {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
    let registry = maki_commands::CommandRegistry::new();
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
    app.command_palette.sync("/deploy a");
    app.command_palette
        .sync_arguments("/deploy a", 9, &app.state.mode.id_key());
    let items = vec![CommandArgumentItem {
        label: "alpha".into(),
        insertion: "alpha".into(),
        description: None,
    }];
    for _ in 0..1000 {
        if probe.try_finish_command_arguments(items.clone()).is_some() {
            break;
        }
        std::thread::yield_now();
    }
    let _ = app.command_palette.poll_arguments();
    let _ = probe.try_finish_command_argument_lifecycle();
    (app, probe, producer)
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
fn reset_session_cancels_completion_lifecycle_once() {
    let (mut app, probe, _producer) = lifecycle_app();

    app.reset_session();

    assert_eq!(
        probe.try_finish_command_argument_lifecycle(),
        Some(("cancel", None, true))
    );
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
fn session_reset_names_the_session_that_ended() {
    let mut app = test_app();
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;
    let ended = app.state.session.id.to_string();

    app.reset_session();

    let (event, data) = probe.try_recv_autocmd().expect("SessionReset fired");
    assert_eq!(event, "SessionReset");
    assert_eq!(data["session_id"], serde_json::json!(ended));
    assert_ne!(
        app.state.session.id.to_string(),
        ended,
        "reset must have installed a different session, or this proves nothing"
    );
}

#[test]
fn reset_session_clears_plan() {
    let mut app = test_app();
    app.state.token_usage.input = 500;
    app.chats[0].context_size = 1000;
    app.state.mode = Mode::Build;
    app.state.plan = PlanState::Ready(PathBuf::from("plan.md"));
    app.queue_and_notify(queued_msg("q"));
    app.queue.set_focus_at(0);
    app.help_modal.toggle();
    let (_tx, rx) = flume::bounded::<crate::components::btw_modal::BtwEvent>(1);
    app.btw_modal.open("q", rx);
    let actions = app.reset_session();
    assert!(matches!(&actions[0], Action::NewSession));
    assert_eq!(app.status, Status::Idle);
    assert_eq!(app.state.token_usage.input, 0);
    assert_eq!(app.chats[0].context_size, 0);
    assert_eq!(app.state.mode, Mode::Build);
    assert_eq!(app.state.plan, PlanState::None);
    assert!(app.queue.is_empty());
    assert!(app.recoverable_queue.is_empty());
    assert_eq!(app.chats.len(), 1);
    assert_eq!(app.chats[0].name, "Main");
    assert_eq!(app.active_chat, 0);
    assert!(app.chat_index.is_empty());
    assert!(app.queue.focus().is_none());
    assert!(!app.help_modal.is_open());
    assert!(!app.btw_modal.is_open());
}

#[test]
fn replacing_session_rotates_command_target() {
    let mut app = test_app();
    let reset_target = app.command_target.id();

    app.reset_session();
    assert_ne!(app.command_target.id(), reset_target);

    let load_target = app.command_target.id();
    app.apply_loaded_session(AppSession::new("test-model", "/tmp/test"), &test_model());
    assert_ne!(app.command_target.id(), load_target);
}

#[test]
fn reset_session_assigns_new_plan_path_in_plan_mode() {
    let mut app = test_app();
    app.state.mode = Mode::Plan;
    app.state.plan = PlanState::Drafting(PathBuf::from("old-plan.md"));
    app.reset_session();
    assert_eq!(app.state.mode, Mode::Plan);
    assert!(app.state.plan.path().is_some());
    assert_ne!(app.state.plan.path(), Some(Path::new("old-plan.md")));
}

#[test]
fn reset_session_clears_drafting_plan_in_build_mode() {
    let mut app = test_app();
    app.state.mode = Mode::Build;
    app.state.plan = PlanState::Drafting(PathBuf::from("leftover.md"));
    app.reset_session();
    assert_eq!(app.state.mode, Mode::Build);
    assert_eq!(app.state.plan, PlanState::None);
}

#[test]
fn load_session_clears_plan() {
    let (_tmp, _dir, _writer, mut app) = tempdir_app();
    app.state
        .session_mut()
        .push_message(Message::user("test".into()));
    app.state.session_mut().save(&app.storage).unwrap();
    let id = app.state.session.id;
    app.state.mode = Mode::Build;
    app.state.plan = PlanState::Ready(PathBuf::from("old-plan.md"));
    app.load_loaded_session(AppSession::load(id, &app.storage).unwrap());
    assert_eq!(app.state.mode, Mode::Build);
    assert_eq!(app.state.plan.path(), None);
}

#[test]
fn tool_lifecycle_events_name_the_session_and_tool() {
    let mut app = streaming_app();
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;
    let session_id = app.state.session.id.to_string();

    app.update(agent_msg(tool_start("tool-1", "bash")));

    let (event, data) = probe.try_recv_autocmd().expect("ToolStart fired");
    assert_eq!(event, "ToolStart");
    assert_eq!(data["session_id"], serde_json::json!(session_id));
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
    assert_eq!(data["session_id"], serde_json::json!(session_id));
    assert_eq!(data["tool_id"], "tool-1");
    assert_eq!(data["tool"], "bash");

    app.run_id += 1;
    app.update(agent_msg_with_run_id(tool_start("stale", "read"), 1));
    assert!(probe.try_recv_autocmd().is_none());
}

#[test]
fn argument_completion_enter_fills_then_next_enter_executes() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
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
    app.command_palette.sync("/rename dråft tail");
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
        probe.try_recv_command(),
        Some(("/rename".into(), "final tail".into(), 0))
    );
}

#[test]
fn argument_completion_enter_on_exact_match_executes_immediately() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
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
    app.command_palette.sync("/rename final");
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
        probe.try_recv_command(),
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
/// usage, not how it is spelled (maki-providers covers the spelling).
fn sub_usage_text() -> String {
    SUB_TOKENS.format_sum_cost(SUB_COST)
}

#[test]
fn subagent_turn_complete_updates_matching_parent_header_with_last_turn() {
    let mut app = streaming_app();
    app.update(agent_msg(tool_start(TASK_ID, "task")));
    app.update(agent_msg(tool_start("task2", "task")));

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
    app.update(agent_msg(tool_start(TASK_ID, "task")));
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
    app.update(agent_msg(tool_start(TASK_ID, "task")));
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
fn cancel_resets_all_chats_and_indices() {
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
    let buf = Arc::new(maki_agent::SharedBuf::new());
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
    assert_eq!(app.chats[1].in_progress_count(), 0);
    assert!(app.chats[1].is_finished());
    assert!(app.chat_index.is_empty());
    assert_eq!(app.cadence(), Cadence::IDLE);
}

fn finish_subagent(app: &mut App, id: &str, is_error: bool) {
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: id.into(),
        tool: "task".into(),
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
#[test_case(|app: &mut App| finish_subagent_task(app, true),  ERROR_TEXT,     &DisplayRole::Error ; "task_failure")]
#[test_case(cancel_app as fn(&mut App),                       CANCELLED_TEXT, &DisplayRole::Error ; "cancel")]
#[test_case(error_app  as fn(&mut App),                       ERROR_TEXT,     &DisplayRole::Error ; "main_error")]
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
#[test_case(true,  ERROR_TEXT, &DisplayRole::Error ; "batch_subagent_failure")]
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
fn ago_formats_relative_start_time() {
    let now = Instant::now();
    assert_eq!(ago(now), "just now");
    assert_eq!(ago(now - Duration::from_secs(5 * 60)), "5min ago");
    assert_eq!(ago(now - Duration::from_secs(2 * 60 * 60)), "2h ago");
    assert_eq!(ago(now - Duration::from_secs(3 * 24 * 60 * 60)), "3d ago");
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
fn task_entry_shows_context_and_ago() {
    let mut app = app_with_subagent();
    app.chats[1].context_size = 5000;
    let entries = app.task_entries();

    let sub = entries.iter().find(|e| e.chat_index == 1).unwrap();
    assert_eq!(sub.context_str(), Some("5.0k"));
    assert!(sub.ago().is_some_and(|s| !s.is_empty()));

    let main = &entries[0];
    assert_eq!(main.context_str(), None);
    assert_eq!(main.ago(), None);
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
fn usage_readout_draws_in_status_bar() {
    let mut app = test_app();
    let usage = ProviderUsage {
        plan: None,
        limits: vec![
            UsageLimit {
                kind: UsageWindow::Hours(5),
                percentage: Some(30),
                reset_at: None,
                detail: None,
            },
            UsageLimit {
                kind: UsageWindow::Weekly { model: None },
                percentage: Some(50),
                reset_at: None,
                detail: None,
            },
        ],
    };
    app.usage_slot
        .store(Some(Arc::new(UsageFetchState::Ready(usage))));
    let rows = rendered_rows(&mut app, 80, 24);
    let text = rows.join("\n");
    assert!(
        text.contains("5h30% w50%"),
        "usage readout missing from rendered bottom region:\n{text}"
    );
}

#[test]
fn usage_readout_blank_for_non_ready_states() {
    let app = test_app();
    app.usage_slot
        .store(Some(Arc::new(UsageFetchState::Loading)));
    assert!(
        app.usage_readout().is_none(),
        "Loading must not paint a readout"
    );
    app.usage_slot
        .store(Some(Arc::new(UsageFetchState::Unsupported)));
    assert!(
        app.usage_readout().is_none(),
        "Unsupported must not paint a readout"
    );
    app.usage_slot
        .store(Some(Arc::new(UsageFetchState::Error("boom".into()))));
    assert!(
        app.usage_readout().is_none(),
        "Error must not paint a readout"
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

#[test]
fn restored_subagents_have_no_ago_and_are_finished() {
    let mut app = test_app();
    let id = "restored-sub".to_string();
    app.state.session_mut().set_subagents(vec![StoredSubagent {
        tool_use_id: id.clone(),
        name: "old task".into(),
        model: None,
    }]);
    app.state
        .session_mut()
        .set_subagent_messages(id.clone(), vec![Message::user("hi".into())]);
    app.restore_display();

    assert_eq!(app.chats.len(), 2);
    assert!(app.chats[1].is_finished());
    assert_eq!(app.chats[1].started_at(), None);

    let entries = app.task_entries();
    let sub = entries.iter().find(|e| e.chat_index == 1).unwrap();
    assert!(sub.is_finished());
    assert_eq!(sub.ago(), None);
}

fn streaming_app() -> App {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app
}

fn app_with_subagent_id(id: &str) -> App {
    let mut app = streaming_app();
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
fn open_task_picker_inserts_new_child_without_changing_selection() {
    let mut app = app_with_subagent();
    open_tasks_picker(&mut app);
    app.update(Msg::Key(key(KeyCode::Down)));

    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "new".into() },
        "task2",
        Some("build"),
    ));

    assert_eq!(app.task_picker.item(1).unwrap().name, "build");
    assert_eq!(app.task_picker.item(2).unwrap().name, "research");
    assert_eq!(app.task_picker.selected_item().unwrap().chat_index, 1);
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
fn open_task_picker_refreshes_after_subagent_history() {
    let mut app = app_with_subagent_id("session-abc");
    open_tasks_picker(&mut app);

    app.update(agent_msg(AgentEvent::SubagentHistory {
        tool_use_id: "session-abc".into(),
        messages: vec![],
    }));

    assert!(app.task_picker.is_open());
    assert_eq!(app.task_picker.item(1).unwrap().finished, Some(true));
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

#[test]
fn compact_command_sets_streaming() {
    let mut app = test_app();
    let actions = app.execute_command(cmd("/compact"), 0);
    assert!(matches!(&actions[0], Action::Compact));
    assert_eq!(app.status, Status::Streaming);
}

#[test]
fn compact_during_streaming_queues_item() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;

    let actions = app.execute_command(cmd("/compact"), 0);
    assert!(actions.is_empty());
    assert_eq!(app.queue.len(), 1);
    assert_eq!(app.queue.panel_entries()[0].text, "/compact");
}

#[test]
fn cancel_clears_pending_input() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.pending_input = PendingInput::AuthRetry { subagent_id: None };
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    type_and_submit(&mut app, "hello");
    app.status = Status::Idle;
    app.run_id = 1;
    app.state
        .session_mut()
        .push_message(Message::user("hello".into()));

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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));
    assert!(matches!(&actions[0], Action::CancelAgent { .. }));
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

/// `/usage` spawns a detached fetch that stores its answer with nothing
/// listening, so an unpolled modal sits on `Loading` until the user presses
/// some unrelated key.
#[test]
fn usage_quota_arriving_in_the_background_owes_a_frame() {
    let mut app = test_app();
    app.execute_command(cmd("/usage"), 0);
    let slot = Arc::clone(&app.usage_slot);

    assert_owes_one_frame(&mut app, || {
        slot.store(Some(Arc::new(UsageFetchState::Loading)));
    });
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

/// `/model <provider/id>` emits `ChangeModel` for the spec without the picker,
/// even when the spec is absent from the discovered list (explicit specs
/// bypass the list).
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

/// `/model <fragment>` that fuzzy-resolves to a unique spec emits
/// `ChangeModel(resolved)` without the picker.
#[test]
fn model_arg_fuzzy_unique_emits_change_model() {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![
        LATE_MODEL_SPEC.into(),
        MODEL_SPEC_OPUS.into(),
    ])));

    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: "glm".into(),
        },
        0,
    );
    assert!(matches!(&actions[..], [Action::ChangeModel(spec)] if spec == LATE_MODEL_SPEC));
    assert!(!app.model_picker.is_open());
}

/// `/model <fragment>` with 2+ fuzzy matches flashes, emits nothing, and leaves
/// the session model unchanged with the picker closed.
#[test]
fn model_arg_ambiguous_flashes() {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![
        MODEL_SPEC_GLM4.into(),
        LATE_MODEL_SPEC.into(),
    ])));
    let before = app.state.model.spec();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: "glm".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert!(!app.model_picker.is_open());
    assert_eq!(app.state.model.spec(), before);
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        "command failed: ambiguous model: glm"
    );
}

/// `/model <fragment>` with zero fuzzy matches flashes, emits nothing, and
/// leaves the session model unchanged with the picker closed.
#[test]
fn model_arg_no_match_flashes() {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![LATE_MODEL_SPEC.into()])));
    let before = app.state.model.spec();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: "xyz".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert!(!app.model_picker.is_open());
    assert_eq!(app.state.model.spec(), before);
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        "command failed: no model matches: xyz"
    );
}

#[test]
fn model_arg_matches_provider_section() {
    let (mut app, models) = app_with_model_slot();
    models.store(Some(Arc::new(vec![
        MODEL_SPEC_CLAUDE.into(),
        LATE_MODEL_SPEC.into(),
    ])));

    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: "anthropic".into(),
        },
        0,
    );
    assert!(matches!(&actions[..], [Action::ChangeModel(spec)] if spec == MODEL_SPEC_CLAUDE));
}

#[test]
fn malformed_model_arg_flashes_invalid_model() {
    let (mut app, _models) = app_with_model_slot();
    let before = app.state.model.spec();

    let actions = app.execute_command(
        ParsedCommand {
            name: "/model".into(),
            args: "anthropic/".into(),
        },
        0,
    );
    assert!(actions.is_empty());
    assert_eq!(app.state.model.spec(), before);
    assert_eq!(
        app.status_bar.flash_text().unwrap(),
        "command failed: model must be in 'provider/model' format (e.g. anthropic/claude-sonnet-4-20250514)"
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
        maki_storage::theme::read_theme_name(&dir).as_deref(),
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
    app.update(agent_msg(tool_start(TASK_ID, "task")));
    app.update(subagent_msg(tool_start(SUB_TOOL_ID, "bash"), TASK_ID, None));
    let buf = Arc::new(maki_agent::SharedBuf::new());
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
        buf.append(maki_agent::SnapshotLine::plain(TOOL_OUTPUT_LINE.into()));
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
        app.retry_info = Some(RetryInfo {
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

    let (event_tx, _event_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (_cmd_tx, cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
    app.float_mgr.open(
        Arc::new(maki_agent::SharedBuf::new()),
        maki_lua::FloatConfig::default(),
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
            app.active_chat().view(frame, area, false);
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
fn ctrl_q_pops_front() {
    let mut app = app_with_queued_message();
    app.queue_and_notify(queued_msg("second"));
    app.update(Msg::Key(kb::POP_QUEUE.to_key_event()));
    assert_eq!(app.queue.len(), 1);
    assert_eq!(app.queue.panel_entries()[0].text, "second");
    assert!(app.queue.focus().is_none(), "unfocused stays unfocused");

    app.queue_and_notify(queued_msg("third"));
    app.queue.set_focus_at(1);
    app.update(Msg::Key(kb::POP_QUEUE.to_key_event()));
    assert_eq!(
        app.queue.focus(),
        Some(0),
        "focus adjusted when item removed"
    );
}

#[test_case(cancel_app as fn(&mut App) ; "cancel")]
#[test_case(error_app as fn(&mut App)  ; "error")]
fn clears_queue_focus_on_terminate(terminate: fn(&mut App)) {
    let mut app = app_with_queued_message();
    app.queue.set_focus_at(0);
    terminate(&mut app);
    assert!(app.queue.focus().is_none());
}

#[test]
fn stale_events_ignored_after_run_id_increment() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;

    cancel_app(&mut app);
    let current_run = app.run_id;
    let actions = type_and_submit(&mut app, "new prompt");
    assert!(matches!(&actions[0], Action::SendMessage(i) if i.message == "new prompt"));
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;

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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.update(agent_msg(AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: TASK_ID.into(),
        tool: "task".into(),
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
    |app: &mut App| { app.status = Status::Streaming; },
    &[KeybindContext::General, KeybindContext::Streaming, KeybindContext::Editing],
    &[]
    ; "streaming"
)]
#[test_case(
    |app: &mut App| { app.state.mode = Mode::Plan; app.plan_form.on_plan_ready(); },
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
        app.state.session_mut().push_message(Message::user("test".into()));
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

    app.update(Msg::Key(key(KeyCode::Tab)));
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

    let sessions_dir = tmp.path().join(maki_storage::sessions::SESSIONS_DIR);
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
fn apply_loaded_session_defers_queued_messages_until_respawn() {
    let mut app = test_app();
    let mut session = AppSession::new("test-model", "/tmp/test");
    session.meta.queued_messages = vec!["deferred".into()];
    session.push_message(Message::user("hello".into()));

    let model = app.state.model.clone();
    app.apply_loaded_session(session, &model);

    assert!(app.queue.is_empty());
    assert_eq!(app.state.session.meta.queued_messages, ["deferred"]);
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
fn usage_command_toggles_modal() {
    let mut app = test_app();
    assert!(!app.usage_modal.is_open());
    let open_actions = app.execute_command(cmd("/usage"), 0);
    assert!(app.usage_modal.is_open());
    assert!(
        open_actions
            .iter()
            .any(|a| matches!(a, Action::RefreshUsage)),
        "opening should request a quota refresh"
    );
    let close_actions = app.execute_command(cmd("/usage"), 0);
    assert!(!app.usage_modal.is_open());
    assert!(
        !close_actions
            .iter()
            .any(|a| matches!(a, Action::RefreshUsage)),
        "closing should not trigger a refresh"
    );
}

#[test]
fn ctrl_r_refreshes_usage_while_modal_open() {
    let mut app = test_app();
    app.execute_command(cmd("/usage"), 0);
    assert!(app.usage_modal.is_open());

    let actions = app.update(Msg::Key(kb::REFRESH.to_key_event()));
    assert!(
        actions.iter().any(|a| matches!(a, Action::RefreshUsage)),
        "Ctrl+R should emit RefreshUsage"
    );
    assert!(app.usage_modal.is_open(), "modal should stay open");
}

#[test]
fn cd_command_behavior() {
    let mut app = test_app();
    app.execute_command(
        ParsedCommand {
            name: "/cd".into(),
            args: "/tmp".into(),
        },
        0,
    );
    let flash = app.status_bar.flash_text().unwrap();
    assert!(flash.starts_with("cd /tmp"), "flash={flash:?}");
    // Use `canonicalize_clean` (resolves symlinks like the OS does) rather
    // than `absolute` which preserves symlinks. On macOS `/tmp` is a symlink
    // to `/private/tmp`; production `cmd_cd` reads back `current_dir()` which
    // returns the resolved form, so the test expectation must match.
    let resolved = maki_storage::paths::canonicalize_clean(Path::new("/tmp"));
    assert_eq!(app.state.session.cwd, resolved.to_string_lossy());

    app.execute_command(
        ParsedCommand {
            name: "/cd".into(),
            args: "/nonexistent_path_12345".into(),
        },
        0,
    );
    let flash = app.status_bar.flash_text().unwrap();
    assert!(flash.starts_with("cd: "), "error flash={flash:?}");
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

/// The palette hides a lua command once the typed words pass its `max_args`,
/// and a hidden command falls through to `handle_submit`, so a multi word
/// `nargs` command must still be routed to its plugin.
#[test]
fn typed_lua_command_with_args_executes() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
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

const RUN_CMDLINE_REJECTED: &str = "a rejected cmdline must not run anything";
const MAX_COMMAND_DEPTH_ERROR: &str = "maximum command recursion depth exceeded";

#[test_case("/new" ; "plain")]
#[test_case("/NEW" ; "uppercase")]
#[test_case("  /new  " ; "surrounding_whitespace")]
#[test_case("new" ; "missing_slash")]
fn run_cmdline_executes_builtin(cmdline: &str) {
    let mut app = test_app();

    let actions = app.run_cmdline(cmdline, 0).unwrap();

    assert!(matches!(&actions[..], [Action::NewSession]));
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

    let actions = app.execute_command(
        ParsedCommand {
            name: "/project:review".into(),
            args: "src/lib.rs".into(),
        },
        0,
    );

    assert!(matches!(
        actions.as_slice(),
        [Action::SendMessage(input)] if input.message == "Review src/lib.rs"
    ));
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

    let actions = app.run_cmdline("/project:review src/lib.rs", 0).unwrap();

    assert!(matches!(
        actions.as_slice(),
        [Action::SendMessage(input)] if input.message == "Review src/lib.rs"
    ));
}

/// Only the typed path clears the input, so a keybind or autocmd reaching for
/// `run_command` cannot eat a half-written message.
#[test]
fn run_cmdline_keeps_typed_input() {
    let mut app = test_app();
    app.input_box.set_input("half written".into());

    app.run_cmdline("/usage", 0).unwrap();

    assert_eq!(app.input_box.buffer.value(), "half written");
}

#[test]
fn run_cmdline_unknown_name_errors_without_dispatching() {
    let mut app = test_app();
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;

    let Err(err) = app.run_cmdline("/nope", 0) else {
        panic!("{RUN_CMDLINE_REJECTED}");
    };

    assert!(err.contains("/nope"), "err={err:?}");
    assert!(probe.try_recv_command().is_none(), "{RUN_CMDLINE_REJECTED}");
}

#[test]
fn run_cmdline_uses_registry_depth_limit() {
    let mut app = test_app();

    assert!(
        app.run_cmdline("/new", (maki_commands::MAX_COMMAND_DEPTH + 1) as u8)
            .is_ok(),
        "resolution succeeds before the command outcome arrives"
    );
    assert_eq!(app.status_bar.flash_text(), Some(MAX_COMMAND_DEPTH_ERROR));
    assert!(
        app.run_cmdline("/new", maki_commands::MAX_COMMAND_DEPTH as u8)
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
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
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
        probe.try_recv_command(),
        Some(("/Sessions".to_string(), String::new(), 3))
    );
}

#[test]
fn startup_session_picker_emits_plugin_request() {
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
    let app = build_app_with_handle(dir.clone(), Arc::new(test_writer(dir)), handle);

    app.open_startup_session_picker();

    assert_eq!(
        probe.try_recv_autocmd(),
        Some(("SessionPickerRequested".to_owned(), serde_json::json!({})))
    );
}

#[test]
fn slash_noncommand_sends_as_prompt() {
    let mut app = test_app();
    let actions = type_and_submit(&mut app, "/nonexistent");
    assert!(app.status_bar.flash_text().is_none());
    assert!(actions.iter().any(|a| matches!(a, Action::SendMessage(..))));
}

fn build_rewind_app() -> App {
    let mut app = test_app();

    app.state.session_mut().replace_messages(vec![
        Message::user("first prompt".into()),
        Message {
            role: Role::Assistant,
            content: vec![
                ContentBlock::Text {
                    text: "response 1".into(),
                },
                ContentBlock::tool_use("tool-1", "bash", serde_json::json!({})),
            ],
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
    app
}

#[test]
fn rewind_to_middle_truncates_and_populates_input() {
    let mut app = build_rewind_app();
    let old_run_id = app.run_id;
    let entry = crate::components::rewind_picker::RewindEntry {
        turn_index: 2,
        prompt_preview: "2: second".into(),
        prompt_text: "second prompt".into(),
    };
    let actions = app.rewind_to(entry);

    assert_eq!(app.state.session.messages().len(), 2);
    assert!(app.state.session.tool_outputs().contains_key("tool-1"));
    assert_eq!(app.input_box.buffer.value(), "second prompt");
    assert_eq!(app.run_id, old_run_id);
    let Action::LoadSession(ref loaded) = actions[0] else {
        panic!("expected LoadSession");
    };
    assert_eq!(loaded.messages.len(), 2);
}

#[test]
fn rewind_to_first_turn_clears_everything() {
    let mut app = build_rewind_app();
    app.state.context_size = 100_000;
    app.state.token_usage.input = 500;
    app.state.token_usage.output = 200;
    let entry = crate::components::rewind_picker::RewindEntry {
        turn_index: 0,
        prompt_preview: "1: first".into(),
        prompt_text: "first prompt".into(),
    };
    let actions = app.rewind_to(entry);

    assert!(app.state.session.messages().is_empty());
    assert!(!app.state.session.tool_outputs().contains_key("tool-1"));
    assert_eq!(app.state.token_usage.input, 500);
    assert_eq!(app.state.token_usage.output, 200);
    assert_eq!(app.state.context_size, 0);
    assert_eq!(app.chats[0].context_size, 0);
    assert!(matches!(&actions[0], Action::LoadSession(_)));
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    assert!(app.retry_info.is_some());
}

#[test]
fn retry_clears_subagent_in_progress_tools() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    assert!(app.retry_info.is_none());
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    let (tx, rx) = flume::unbounded();
    app.answer_tx = Some(tx);

    app.update(agent_msg(AgentEvent::AuthRequired));
    assert!(matches!(
        app.pending_input,
        PendingInput::AuthRetry { subagent_id: None }
    ));

    let actions = submit(&mut app);
    assert!(actions.is_empty());
    assert_eq!(app.pending_input, PendingInput::None);
    assert_eq!(rx.try_recv().unwrap(), "");
}

fn app_with_subagent_tx(id: &str) -> (App, flume::Receiver<String>, flume::Receiver<String>) {
    let (sub_tx, sub_rx) = flume::unbounded();
    let (main_tx, main_rx) = flume::unbounded();
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.answer_tx = Some(main_tx);
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta { text: "x".into() },
        subagent: Some(subagent_info_with_tx(id, "research", Some(sub_tx))),
        run_id: 1,
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
        PendingInput::AuthRetry { subagent_id: Some(ref id) } if id == "sub1"
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
        run_id: 1,
    })));
    assert_eq!(app.pending_input, PendingInput::None);
    assert_eq!(app.chats[0].message_count(), count_before);
}

#[test]
fn send_to_agent_unknown_subagent_falls_back_to_main() {
    let (main_tx, main_rx) = flume::unbounded();
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.answer_tx = Some(main_tx);

    app.pending_input = PendingInput::AuthRetry {
        subagent_id: Some("nonexistent".into()),
    };
    app.update(Msg::Key(key(KeyCode::Enter)));

    assert_eq!(main_rx.try_recv().unwrap(), "");
    assert_eq!(app.pending_input, PendingInput::None);
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
    |app: &mut App| { app.state.mode = Mode::Plan; app.plan_form.on_plan_ready(); },
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
        "invalid arguments for /btw: expected 1 or more"
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
    app.help_modal.toggle();

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 5, 1));
    assert!(app.selection_state.is_none());

    app.update(mouse_event(MouseEventKind::Down(MouseButton::Left), 20, 5));
    let state = app.selection_state.as_ref().unwrap();
    assert_eq!(state.sel().zone, SelectionZone::Overlay);
}

fn streaming_app_with_history() -> App {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
        maki_agent::HistorySnapshot::new(history),
    )));
    app
}

/// The stale event is dropped, yet the cancelled turn still reaches disk: the
/// next frame's checkpoint syncs the mirror whatever event arrived.
#[test_case(done() ; "stale_done")]
#[test_case(
    AgentEvent::Error { message: "timeout".into() } ; "stale_error"
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
fn parent_done_reconciles_unresolved_children_and_tools() {
    let mut app = streaming_app_with_history();
    app.update(agent_msg(AgentEvent::ToolStart(Box::new(ToolStartEvent {
        id: "task1".into(),
        tool: "task".into(),
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
    let buf = Arc::new(maki_agent::SharedBuf::new());
    app.update(subagent_msg(
        AgentEvent::LiveToolBuf {
            id: "child-tool".into(),
            body: buf,
        },
        "task1",
        None,
    ));

    app.update(done_event());

    assert!(app.chats[1].is_finished());
    assert_eq!(app.chats[0].in_progress_count(), 0);
    assert_eq!(app.chats[1].in_progress_count(), 0);
    assert!(
        app.chats[0]
            .last_message_text()
            .contains(MISSING_TOOL_COMPLETION)
    );
    app.checkpoint();
    assert!(app.state.session.subagents().is_empty());
    assert_eq!(app.state.session.messages().len(), 2);
    assert!(app.state.session.tool_outputs().is_empty());
    assert_eq!(app.cadence(), Cadence::IDLE);
}

#[test]
fn parent_error_refreshes_picker_and_persists_only_completed_children() {
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

    app.update(agent_msg(AgentEvent::Error {
        message: "boom".into(),
    }));

    assert!(app.task_picker.is_open());
    assert_eq!(app.task_picker.item(2).unwrap().finished, Some(true));
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
            ("task3", "third", Some("model-c")),
        ]
    );
}

#[test]
fn reserved_shell_survives_parent_done_until_shell_done() {
    let mut app = streaming_app_with_history();
    let id = app.shell.reserve_id();

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
fn active_shell_survives_agent_error_while_agent_and_child_tools_fail() {
    let mut app = streaming_app_with_history();
    let shell_id = app.shell.reserve_id();
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

    app.update(agent_msg(AgentEvent::Error {
        message: "provider overloaded".into(),
    }));

    assert_eq!(app.chats[0].in_progress_count(), 1);
    assert_eq!(app.chats[1].in_progress_count(), 0);
    assert!(app.chats[1].is_finished());

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
fn main_shell_exclusion_does_not_protect_same_id_in_child_chat() {
    let mut app = streaming_app_with_history();
    let id = app.shell.reserve_id();
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
    assert_eq!(app.chats[1].in_progress_count(), 0);
    assert!(app.chats[1].is_finished());
}

#[test]
fn error_event_matching_run_id_saves_session_and_queued_messages() {
    let mut app = streaming_app_with_history();
    app.queue_and_notify(queued_msg("next"));

    app.update(agent_msg(AgentEvent::Error {
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

#[test]
fn flush_restored_queue_drops_recovery_snapshot() {
    let mut app = streaming_app_with_history();
    app.queue_and_notify(queued_msg("next"));
    app.update(agent_msg(AgentEvent::Error {
        message: "boom".into(),
    }));
    app.checkpoint();
    assert_eq!(app.state.session.meta.queued_messages, ["next"]);

    app.flush_restored_queue();
    app.checkpoint();
    assert_eq!(app.state.session.meta.queued_messages, ["next"]);
    assert!(app.recoverable_queue.is_empty());

    app.queue.clear();
    app.checkpoint();
    assert!(app.state.session.meta.queued_messages.is_empty());
}

// --- Plan form integration tests ---

fn implement_msg(parallel: bool) -> String {
    if parallel {
        format!("{IMPLEMENT_MSG_PREFIX} at `test-plan.md`. {IMPLEMENT_PARALLEL_HINT}")
    } else {
        format!("{IMPLEMENT_MSG_PREFIX} at `test-plan.md`.")
    }
}

fn plan_app() -> App {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.state.mode = Mode::Plan;
    app.state.plan = PlanState::Drafting(PathBuf::from("test-plan.md"));
    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t1".into(),
        tool: "write".into(),
        output: ToolOutput::Plain("wrote 42 bytes to test-plan.md".into()),
        is_error: false,
        annotation: None,
        written_path: Some("test-plan.md".into()),
    }))));
    app
}

#[test_case(Mode::Plan,  true  ; "plan_mode_tooldone_opens_form")]
#[test_case(Mode::Build, false ; "build_mode_tooldone_no_form")]
fn tool_done_write_opens_plan_form(mode: Mode, expect_form: bool) {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.state.mode = mode;
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.state.mode = Mode::Plan;
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
    app.state.mode = Mode::Plan;
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
    app.state.mode = Mode::Plan;
    app.state.plan = PlanState::Drafting(plan_path);

    assert!(app.run_builtin(BuiltinAction::PlanSubmit).is_empty());
    assert!(!app.plan_form.is_visible());
    assert!(!app.state.plan.is_ready());
    assert!(app.status_bar.flash_text().is_some());
}

#[test]
fn plan_submit_mode_disables_auto_open() {
    let registry = maki_agent::ModeRegistry::builtin();
    registry
        .define(ModeDefSpec {
            name: "plan".into(),
            tools: Some(vec!["plan_submit".into()]),
            ..Default::default()
        })
        .unwrap();

    let mut app = test_app();
    app.lua_event_handle =
        maki_lua::EventHandle::disconnected_for_test_with_modes(Arc::new(registry));
    app.status = Status::Streaming;
    app.run_id = 1;
    app.state.mode = Mode::Plan;
    app.state.plan = PlanState::Drafting(PathBuf::from("/tmp/plans/test.md"));

    app.update(agent_msg(AgentEvent::ToolDone(Box::new(ToolDoneEvent {
        id: "t1".into(),
        tool: "write".into(),
        output: ToolOutput::Plain("wrote 42 bytes to /tmp/plans/test.md".into()),
        is_error: false,
        annotation: None,
        written_path: Some("/tmp/plans/test.md".into()),
    }))));
    assert!(!app.plan_form.is_visible());
    assert!(!app.state.plan.is_ready());
}

#[test_case(1, Mode::Build, true,  true  ; "clear_and_implement")]
#[test_case(2, Mode::Build, false, true  ; "implement_keeps_context")]
fn plan_form_menu_options(
    downs: usize,
    expected_mode: Mode,
    has_new_session: bool,
    has_send_message: bool,
) {
    let mut app = plan_app();
    assert!(app.plan_form.is_visible());

    for _ in 0..downs {
        app.update(Msg::Key(key(KeyCode::Down)));
    }
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    assert!(!app.plan_form.is_visible());
    assert_eq!(app.state.mode, expected_mode);
    assert_eq!(app.state.plan, PlanState::None);
    assert_eq!(
        actions.iter().any(|a| matches!(a, Action::NewSession)),
        has_new_session
    );
    let expected_msg = implement_msg(PlanForm::new().parallel());
    assert_eq!(
        actions
            .iter()
            .any(|a| matches!(a, Action::SendMessage(i) if i.message == expected_msg)),
        has_send_message
    );
}

#[test]
fn plan_form_implement_toggled_parallel() {
    let mut app = plan_app();
    app.update(Msg::Key(key(KeyCode::Char(' '))));
    app.update(Msg::Key(key(KeyCode::Down)));
    app.update(Msg::Key(key(KeyCode::Down)));
    let actions = app.update(Msg::Key(key(KeyCode::Enter)));
    let expected_msg = implement_msg(!PlanForm::new().parallel());
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::SendMessage(i) if i.message == expected_msg))
    );
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
    app.state.mode = Mode::Plan;
    app.state.plan = PlanState::Drafting(PathBuf::from("test-plan.md"));
    assert!(!app.plan_form.is_visible());

    app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event()));
    assert!(!app.plan_form.is_visible());
}

fn install_override(
    app: &mut App,
    key: KeyCode,
    modifiers: KeyModifiers,
) -> maki_lua::test_support::RequestProbe {
    app.keymap_reader = maki_lua::test_support::keymap_reader_with(vec![maki_lua::KeymapEntry {
        key,
        modifiers,
        desc: "plugin override".into(),
        plugin: Arc::from("test-plugin"),
        id: 1,
    }]);
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
    app.lua_event_handle = handle;
    probe
}

const OVERRIDE_DISPATCHED: &str = "override callback must be dispatched";
const OVERRIDE_NOT_DISPATCHED: &str = "override callback must not be dispatched";

#[test]
fn override_shadows_builtin_ctrl_when_no_overlay_open() {
    let mut app = test_app();
    let probe = install_override(&mut app, kb::HELP.code, kb::HELP.modifiers);

    let actions = app.update(Msg::Key(kb::HELP.to_key_event()));

    assert!(actions.is_empty());
    assert!(probe.try_recv().is_some(), "{OVERRIDE_DISPATCHED}");
    assert!(
        !app.help_modal.is_open(),
        "override must consume the key before the built-in HELP handler runs"
    );
}

#[test]
fn override_shadows_quit_builtin() {
    let mut app = test_app();
    app.status = Status::Idle;
    let probe = install_override(&mut app, kb::QUIT.code, kb::QUIT.modifiers);

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));

    assert!(actions.is_empty());
    assert!(probe.try_recv().is_some(), "{OVERRIDE_DISPATCHED}");
    assert_eq!(
        app.exit_request,
        ExitRequest::None,
        "override must consume Ctrl+C before the built-in quit handler runs"
    );
}

#[test]
fn override_shadows_tab_mode_toggle() {
    let mut app = test_app();
    let initial_mode = app.state.mode.clone();
    let probe = install_override(&mut app, KeyCode::Tab, KeyModifiers::NONE);

    let actions = app.update(Msg::Key(key(KeyCode::Tab)));

    assert!(actions.is_empty());
    assert!(probe.try_recv().is_some(), "{OVERRIDE_DISPATCHED}");
    assert_eq!(
        app.state.mode, initial_mode,
        "override must consume Tab before the built-in mode toggle runs"
    );
}

#[test]
fn override_shadows_esc_builtin() {
    let mut app = test_app();
    let probe = install_override(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    let actions = app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(actions.is_empty());
    assert!(probe.try_recv().is_some(), "{OVERRIDE_DISPATCHED}");
    assert!(
        app.last_esc.is_none(),
        "override must consume Esc before the built-in esc handler runs"
    );
}

#[cfg(unix)]
#[test]
fn override_does_not_shadow_suspend() {
    let mut app = test_app();
    let probe = install_override(&mut app, kb::SUSPEND.code, kb::SUSPEND.modifiers);

    let actions = app.update(Msg::Key(kb::SUSPEND.to_key_event()));

    assert!(
        actions.iter().any(|a| matches!(a, Action::Suspend)),
        "suspend is non-remappable: override must not shadow Ctrl+Z"
    );
    assert!(probe.try_recv().is_none(), "{OVERRIDE_NOT_DISPATCHED}");
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
    let probe = install_override(&mut app, kb::PLAN_TOGGLE.code, kb::PLAN_TOGGLE.modifiers);
    assert!(app.plan_form.is_visible());

    app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event()));
    assert!(
        !app.plan_form.is_visible(),
        "open plan form must consume Ctrl+T before the override"
    );

    app.update(Msg::Key(kb::PLAN_TOGGLE.to_key_event()));
    assert!(
        app.plan_form.is_visible(),
        "Ctrl+T must reopen the dismissed plan form despite the override"
    );
    assert!(probe.try_recv().is_none(), "{OVERRIDE_NOT_DISPATCHED}");
}

#[test]
fn streaming_cancel_wins_over_quit_override() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    let probe = install_override(&mut app, kb::QUIT.code, kb::QUIT.modifiers);

    let actions = app.update(Msg::Key(kb::QUIT.to_key_event()));

    assert!(
        matches!(&actions[0], Action::CancelAgent { .. }),
        "built-in cancel must win while streaming even when Ctrl+C is overridden"
    );
    assert_eq!(app.status, Status::Idle);
    assert_eq!(app.exit_request, ExitRequest::None);
    assert!(probe.try_recv().is_none(), "{OVERRIDE_NOT_DISPATCHED}");
}

#[test]
fn dead_host_override_falls_back_to_builtin() {
    let mut app = test_app();
    let _probe = install_override(&mut app, kb::HELP.code, kb::HELP.modifiers);
    app.lua_event_handle = maki_lua::EventHandle::disconnected_for_test();

    app.update(Msg::Key(kb::HELP.to_key_event()));

    assert!(
        app.help_modal.is_open(),
        "dead lua host must fall back to the built-in HELP handler"
    );
}

#[test]
fn streaming_cancel_wins_over_esc_override() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.status_bar.flash_duration = Duration::from_secs(3600);
    app.last_esc = Some(Instant::now());
    let probe = install_override(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    let actions = app.update(Msg::Key(key(KeyCode::Esc)));

    assert!(
        matches!(&actions[0], Action::CancelAgent { .. }),
        "built-in cancel must win while streaming even when Esc is overridden"
    );
    assert_eq!(app.status, Status::Idle);
    assert!(probe.try_recv().is_none(), "{OVERRIDE_NOT_DISPATCHED}");
}

#[test]
fn reset_session_closes_plan_form() {
    let mut app = plan_app();
    assert!(app.plan_form.is_visible());

    app.reset_session();
    assert!(!app.plan_form.is_visible());
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

    app.update(Msg::Key(key(KeyCode::Tab)));
    assert_eq!(
        app.state.mode,
        Mode::Build,
        "tab must not toggle while bash prefix present"
    );

    app.input_box.set_input("ls".into());
    assert_eq!(&*app.mode_label().0, "[BUILD]");
}

#[test]
fn thinking_toggle_cycles_off_adaptive() {
    let mut app = test_app();
    assert_eq!(app.state.thinking, ThinkingConfig::Off);

    app.execute_command(cmd("/thinking"), 0);
    assert_eq!(app.state.thinking, ThinkingConfig::Adaptive);

    app.execute_command(cmd("/thinking"), 0);
    assert_eq!(app.state.thinking, ThinkingConfig::Off);
}

#[test]
fn thinking_explicit_args() {
    let mut app = test_app();

    app.execute_command(
        ParsedCommand {
            name: "/thinking".into(),
            args: "8192".into(),
        },
        0,
    );
    assert_eq!(app.state.thinking, ThinkingConfig::Budget(8192));

    app.execute_command(
        ParsedCommand {
            name: "/thinking".into(),
            args: "high".into(),
        },
        0,
    );
    assert_eq!(app.state.thinking, ThinkingConfig::Effort(Effort::High));
}

#[test]
fn thinking_unsupported_model_flashes_error() {
    let mut app = test_app();
    app.state.model.thinking_override = Some(maki_providers::ThinkingSupport::No);

    app.execute_command(cmd("/thinking"), 0);
    assert_eq!(app.state.thinking, ThinkingConfig::Off);
    assert!(app.status_bar.flash_text().is_some());
}

#[test]
fn thinking_restored_from_session_meta() {
    let tmp = TempDir::new().unwrap();
    let storage = StateDir::from_path(tmp.path().to_path_buf());
    let mut session = AppSession::new("test-model", "/tmp/test");
    session.meta.thinking = Some(StoredThinking::Budget { tokens: 4096 });

    let state = SessionState::from_session(
        session,
        &test_model(),
        &storage,
        &maki_config::ModelPolicy::default(),
    );
    assert_eq!(state.thinking, ThinkingConfig::Budget(4096));
}

fn set_opus_model(app: &mut App) {
    app.state.model = maki_providers::Model::from_spec("anthropic/claude-opus-4-8").unwrap();
}

#[test]
fn fast_toggle_on_off_on_opus() {
    let mut app = test_app();
    set_opus_model(&mut app);
    assert!(!app.state.fast);

    app.execute_command(cmd("/fast"), 0);
    assert!(app.state.fast);
    assert_eq!(app.status_bar.flash_text(), Some(FAST_ON_MSG));

    app.execute_command(cmd("/fast"), 0);
    assert!(!app.state.fast);
    assert_eq!(app.status_bar.flash_text(), Some(FAST_OFF_MSG));
}

#[test]
fn workflow_toggle_flows_into_agent_input() {
    let mut app = test_app();
    let msg = QueuedMessage {
        text: "hi".into(),
        images: Vec::new(),
    };
    assert!(!app.build_agent_input(&msg).workflow);

    app.execute_command(cmd("/workflow"), 0);
    assert!(app.build_agent_input(&msg).workflow);
    assert_eq!(app.status_bar.flash_text(), Some(WORKFLOW_ON_MSG));

    app.execute_command(cmd("/workflow"), 0);
    assert!(!app.build_agent_input(&msg).workflow);
    assert_eq!(app.status_bar.flash_text(), Some(WORKFLOW_OFF_MSG));
}

/// Workflow sessions have synthetic ids that no ToolDone matches, so
/// SubagentHistory is what finishes their chat.
#[test]
fn subagent_history_finishes_workflow_chat() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
    app.update(subagent_msg(
        AgentEvent::TextDelta { text: "sub".into() },
        "session-abc",
        Some("researcher"),
    ));
    assert_eq!(app.chats.len(), 2);
    assert!(!app.chats[1].is_finished());

    app.update(agent_msg_with_run_id(
        AgentEvent::SubagentHistory {
            tool_use_id: "session-abc".into(),
            messages: vec![],
        },
        1,
    ));
    assert!(app.chats[1].is_finished());
    assert_eq!(app.chats[1].last_message_text(), DONE_TEXT);
}

#[test_case("anthropic/claude-sonnet-4-5" ; "non_opus_anthropic")]
#[test_case("openai/gpt-5.5" ; "non_anthropic")]
fn fast_flashes_error_on_ineligible_model(spec: &str) {
    let mut app = test_app();
    app.state.model = maki_providers::Model::from_spec(spec).unwrap();

    app.execute_command(cmd("/fast"), 0);
    assert!(!app.state.fast);
    assert_eq!(
        app.status_bar.flash_text(),
        Some(FAST_UNSUPPORTED_COMMAND_ERROR)
    );
}

#[test]
fn fast_restored_from_session_meta() {
    let tmp = TempDir::new().unwrap();
    let storage = StateDir::from_path(tmp.path().to_path_buf());
    let mut session = AppSession::new("anthropic/claude-opus-4-8", "/tmp/test");
    session.meta.fast = true;

    let state = SessionState::from_session(
        session,
        &test_model(),
        &storage,
        &maki_config::ModelPolicy::default(),
    );
    assert!(state.fast);
}

#[test]
fn fast_normalized_off_when_restored_onto_ineligible_model() {
    let tmp = TempDir::new().unwrap();
    let storage = StateDir::from_path(tmp.path().to_path_buf());
    // Saved as fast=true, but sonnet cannot do fast mode, so restoring must drop
    // it to false or the UI would show a phantom [fast] badge.
    let mut session = AppSession::new("anthropic/claude-sonnet-4-5", "/tmp/test");
    session.meta.fast = true;

    let state = SessionState::from_session(
        session,
        &test_model(),
        &storage,
        &maki_config::ModelPolicy::default(),
    );
    assert!(!state.fast);
}

#[test]
fn update_model_to_ineligible_resets_fast() {
    let mut app = test_app();
    set_opus_model(&mut app);
    app.state.fast = true;

    let sonnet = maki_providers::Model::from_spec("anthropic/claude-sonnet-4-5").unwrap();
    app.state.update_model(&sonnet);
    assert!(!app.state.fast);
}

#[test]
fn agent_error_creates_synthetic_tool_done_with_message() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;

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
    app.update(agent_msg(AgentEvent::Error {
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
        maki_config::ToolKey::native("bash"),
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

fn open_split_window(app: &mut App, dir: maki_lua::Split) {
    let buf = Arc::new(maki_agent::SharedBuf::new());
    let config = maki_lua::FloatConfig {
        width: maki_lua::Dimension::Abs(SPLIT_EXTENT),
        height: maki_lua::Dimension::Abs(SPLIT_EXTENT),
        border: maki_lua::Border::None,
        split: dir,
        ..maki_lua::FloatConfig::default()
    };
    let (event_tx, _event_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (_cmd_tx, cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
    app.float_mgr.open(buf, config, true, event_tx, cmd_rx);
}

#[test]
fn attention_float_marks_app_as_awaiting_input_until_close() {
    let mut app = test_app();
    let buf = Arc::new(maki_agent::SharedBuf::new());
    let config = maki_lua::FloatConfig {
        needs_input: true,
        split: maki_lua::Split::Below,
        height: maki_lua::Dimension::Abs(4),
        ..maki_lua::FloatConfig::default()
    };
    let (event_tx, _event_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (cmd_tx, cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);

    let active = app.handle_open_win(buf, config, true, event_tx, cmd_rx);
    assert!(active, "an idle input demand activates immediately");
    assert!(app.question_active());
    assert!(app.awaiting_input());

    cmd_tx.send(maki_lua::WinCommand::Close).unwrap();
    let _ = app.float_mgr.tick();
    app.reconcile_active();
    assert!(!app.awaiting_input());
}

#[test]
fn below_split_reserves_bottom_and_suppresses_input() {
    let mut app = test_app();
    let (msg_before, _b, _s, input_before, splits_before) = app.layout_geometry(TEST_AREA);
    assert!(
        splits_before.rect(maki_lua::Split::Below).is_none(),
        "no split open yet"
    );
    assert!(input_before.height > 0, "input box visible before split");

    open_split_window(&mut app, maki_lua::Split::Below);
    let (msg_after, _bottom, _s, input_after, splits_after) = app.layout_geometry(TEST_AREA);

    let band = splits_after
        .rect(maki_lua::Split::Below)
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
#[test_case(maki_lua::Split::Above ; "above")]
#[test_case(maki_lua::Split::Left ; "left")]
#[test_case(maki_lua::Split::Right ; "right")]
fn non_below_split_reserves_band_and_keeps_status_full_width(dir: maki_lua::Split) {
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

    open_split_window(&mut app, maki_lua::Split::Below);
    app.float_mgr.close_all();

    let after = app.layout_geometry(TEST_AREA);
    assert_eq!(after, before, "closing the split restores the layout");
}

#[test]
fn cancel_preserves_panel_window_and_closes_transient() {
    let mut app = test_app();

    let panel_buf = Arc::new(maki_agent::SharedBuf::new());
    let panel_config = maki_lua::FloatConfig {
        split: maki_lua::Split::Panel,
        height: maki_lua::Dimension::Abs(SPLIT_EXTENT),
        ..maki_lua::FloatConfig::default()
    };
    let (panel_tx, _panel_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (_panel_cmd_tx, panel_cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
    app.float_mgr
        .open(panel_buf, panel_config, false, panel_tx, panel_cmd_rx);
    assert_eq!(app.float_mgr.panel_reqs().len(), 1, "panel dock is open");

    let modal_buf = Arc::new(maki_agent::SharedBuf::new());
    let modal_config = maki_lua::FloatConfig {
        needs_input: true,
        ..maki_lua::FloatConfig::default()
    };
    let (modal_tx, _modal_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (_modal_cmd_tx, modal_cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
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
    open_split_window(&mut app, maki_lua::Split::Below);
    open_split_window(&mut app, maki_lua::Split::Left);
    open_split_window(&mut app, maki_lua::Split::Above);
    app.permission_prompt.open(
        "perm-1".into(),
        maki_config::ToolKey::native("bash"),
        vec!["ls".into()],
        None,
    );
    app.active_input = Some(InputKind::Permission);

    let (_msg, _bottom, _status, _input, splits) = app.layout_geometry(TEST_AREA);
    assert!(
        splits.rect(maki_lua::Split::Below).is_none(),
        "below split must yield the bottom area to an open permission prompt",
    );
    assert!(
        splits.rect(maki_lua::Split::Left).is_some(),
        "the prompt must leave a left split untouched",
    );
    assert!(
        splits.rect(maki_lua::Split::Above).is_some(),
        "the prompt must leave an above split untouched",
    );
}

fn app_with_active_subagent() -> App {
    let mut app = app_with_subagent();
    app.run_builtin(BuiltinAction::NextChat);
    assert_eq!(app.active_chat, 1);
    app
}

#[test]
fn double_esc_in_subagent_cancels_subagent() {
    let mut app = app_with_active_subagent();
    app.last_esc = Some(Instant::now());
    let actions = app.update(Msg::Key(key(KeyCode::Esc)));
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        Action::CancelSubagent { tool_use_id } if tool_use_id == TASK_ID
    ));
    assert!(app.chats[1].is_finished());
    assert_eq!(app.chats[1].last_message_text(), CANCELLED_TEXT);
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
fn cancel_subagent_removes_answer_sender() {
    let (mut app, _sub_rx, _main_rx) = app_with_subagent_tx(TASK_ID);
    assert!(!app.subagent_channels.is_empty());
    app.run_builtin(BuiltinAction::NextChat);
    assert_eq!(app.active_chat, 1);
    app.last_esc = Some(Instant::now());
    app.update(Msg::Key(key(KeyCode::Esc)));
    assert!(!app.subagent_channels.contains_key(TASK_ID));
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

    assert_eq!(actions.len(), 1);
    assert!(matches!(
        &actions[0],
        Action::CancelSubagent { tool_use_id } if tool_use_id == "task2"
    ));
    let task1_idx = *app.chat_index.get(TASK_ID).unwrap();
    assert!(!app.chats[task1_idx].is_finished());
    assert!(app.chats[app.active_chat].is_finished());
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
    assert!(app.chats[1].is_finished());

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
const SWITCHED_DRAFT: &str = "draft typed after switching";
const BUMP_TITLE: &str = "title bump ";
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

fn attach_live_history(app: &mut App, messages: Vec<Message>) -> maki_agent::History {
    let mirror: maki_agent::SharedMessages =
        Arc::new(ArcSwap::from_pointee(maki_agent::HistorySnapshot::default()));
    let history = maki_agent::History::new(messages).with_mirror(Arc::clone(&mirror));
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

/// In the window between a rewind and the agent respawn, syncing from the
/// mirror would bring back the messages that were just dropped.
#[test]
fn checkpoint_after_rewind_persists_the_truncated_history() {
    let (_tmp, dir, writer, mut app) = tempdir_app();
    let _live = attach_live_history(
        &mut app,
        vec![
            Message::user("first prompt".into()),
            Message::user("second prompt".into()),
        ],
    );
    app.checkpoint();

    let entry = crate::components::rewind_picker::RewindEntry {
        turn_index: 1,
        prompt_preview: "2: second".into(),
        prompt_text: "second prompt".into(),
    };
    app.rewind_to(entry);
    assert!(app.shared_history.is_none(), "mirror handle is dropped");
    app.checkpoint();

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

    app.reset_session();
    app.checkpoint();
    let new_id = app.state.session.id;
    assert_ne!(new_id, old_id);

    drain_writer(app, writer);
    assert_eq!(AppSession::load(old_id, &dir).unwrap().messages().len(), 1);
    assert!(
        AppSession::load(new_id, &dir).is_err(),
        "an empty session has no content to persist",
    );
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

    app.load_loaded_session(AppSession::load(stored.id, &dir).unwrap());
    assert_eq!(app.state.session.id, stored.id);
    // Walk the loaded session up to the revision already sent for the live one,
    // so the checkpoint below lands on the exact collision.
    let session = app.state.session_mut();
    while session.revision() + 1 < sent_revision {
        session.set_title(format!("{BUMP_TITLE}{}", session.revision()));
    }
    app.input_box.set_input(SWITCHED_DRAFT.into());
    app.checkpoint();
    assert_eq!(
        app.state.session.revision(),
        sent_revision,
        "both sessions must sit at the same revision for this to test anything"
    );

    drain_writer(app, writer);
    let loaded = AppSession::load(stored.id, &dir).unwrap();
    assert_eq!(loaded.meta.input_draft.as_deref(), Some(SWITCHED_DRAFT));
    assert_eq!(loaded.messages().len(), 1);
    assert_eq!(loaded.messages()[0].user_text(), Some(STORED_SESSION_TEXT));
    let previous = AppSession::load(live_id, &dir).unwrap();
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
fn turn_end_keeps_only_the_subagents_that_finished() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    assert!(app.chat_index.is_empty());
    app.checkpoint();

    let ids: Vec<_> = app
        .state
        .session
        .subagents()
        .iter()
        .map(|sa| sa.tool_use_id.as_str())
        .collect();
    assert_eq!(ids, [FINISHED_TASK_ID]);
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
fn completion_app() -> (TempDir, App, Arc<maki_lua::TestCompletionBackend>) {
    let tmp = TempDir::new().unwrap();
    let dir = StateDir::from_path(env::temp_dir());
    let (handle, backend) = maki_lua::test_support::event_handle_with_completion();
    let mut app = build_app_with_handle(dir.clone(), Arc::new(test_writer(dir)), handle);
    let (shared_queue, _rx) = shared_queue::queue();
    app.queue.set_shared(shared_queue);
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
fn seed_skill(backend: &maki_lua::TestCompletionBackend, name: &str) {
    backend.register_source(
        "skill",
        vec![maki_lua::ItemSpec {
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
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let _ = app.file_completion.tick();
        if app.file_completion.has_selectable() {
            return;
        }
        std::thread::yield_now();
    }
    panic!("@-completion popup never offered a selectable item");
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
        .create_producer(maki_commands::ProducerPrecedence::Plugin);
    producer
        .replace(vec![maki_commands::Registration {
            spec: maki_commands::CommandSpec {
                name: Arc::from("/deploy"),
                aliases: Arc::from([]),
                arguments: maki_commands::ArgumentArity::bounded(0, 1),
                docs: maki_commands::CommandDocs {
                    summary: Arc::from("Deploy"),
                    argument_hint: None,
                },
                required_capabilities: maki_commands::TargetCapabilities::default(),
            },
            behavior: Arc::new(TestLuaBehavior {
                handle: maki_lua::EventHandle::disconnected_for_test(),
                plugin: Arc::from("deploy"),
                name: Arc::from("/deploy"),
            }),
            completion: Some(Arc::new(TestLuaCompletion {
                handle: maki_lua::EventHandle::disconnected_for_test(),
                plugin: Arc::from("deploy"),
            })),
        }])
        .unwrap();
    app.command_palette = CommandPalette::new(
        app.command_runtime.registry.clone(),
        app.command_target.clone(),
    );
    app.input_box.set_input("/deploy @rev".into());
    app.command_palette.sync("/deploy @rev");
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
    assert_eq!(app.input_box.buffer.value(), "@docs/read me.md");
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
fn command_palette_takes_precedence() {
    let (_tmp, mut app, _backend) = completion_app();
    // `/thinking ` takes an argument, so the palette stays matched while an
    // `@` token is added to that argument space.
    for c in "/thinking ".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    assert!(app.command_palette.is_active());
    app.update(Msg::Key(key(KeyCode::Char('@'))));
    assert!(
        app.command_palette.is_active(),
        "palette stays matched on /thinking"
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
    completion_match_items(app)
        .into_iter()
        .filter(|i| i.kind == "subagent")
        .map(|i| {
            i.label
                .strip_prefix("subagent:")
                .map(|s| s.to_string())
                .unwrap_or(i.label)
        })
        .collect()
}

/// Seed the `subagent` source with the types valid for `mode` (the task
/// plugin's mode filtering, mirrored here): `general` is plan-blocked,
/// `plan_reviewer` is build-blocked.
fn seed_subagents(backend: &maki_lua::TestCompletionBackend, mode: &str) {
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
        .map(|(name, desc)| maki_lua::ItemSpec {
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
        vec!["research".to_string(), "general".to_string()]
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
        vec!["research".to_string(), "general".to_string()]
    );
}

#[test]
fn at_a_prefix_in_plan_mode_hides_general() {
    let (_tmp, mut app, backend) = completion_app();
    app.set_mode_id("plan".into());
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

fn seed_models(backend: &maki_lua::TestCompletionBackend, models: &[&str]) {
    let items: Vec<_> = models
        .iter()
        .map(|spec| maki_lua::ItemSpec {
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
        "zai/glm-5".into(),
        "anthropic/claude".into(),
    ])));
    seed_models(&backend, &["zai/glm-5", "anthropic/claude"]);
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
        vec!["zai/glm-5".to_string(), "anthropic/claude".to_string()]
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
fn enter_inserts_model_reference_with_trailing_space() {
    let (_tmp, mut app, backend) = completion_app();
    app.available_models
        .store(Some(Arc::new(vec!["zai/glm-5".into()])));
    seed_models(&backend, &["zai/glm-5"]);
    for c in "@m:glm".chars() {
        app.update(Msg::Key(key(KeyCode::Char(c))));
    }
    converge_completion(&mut app);
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(app.input_box.buffer.value(), "@model:zai/glm-5 ");
}

#[test]
fn mixed_list_includes_skills_subagents_and_models() {
    let (_tmp, mut app, backend) = completion_app();
    seed_skill(&backend, "review");
    seed_subagents(&backend, "build");
    app.available_models
        .store(Some(Arc::new(vec!["zai/glm-5".into()])));
    seed_models(&backend, &["zai/glm-5"]);
    app.update(Msg::Key(key(KeyCode::Char('@'))));
    converge_completion(&mut app);
    let items = completion_match_items(&app);
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
    seed_models(&backend, &["zai/glm-5"]);
    let outcome = app.submit_prompt(queued_msg("@model:zai/glm-5 fix the bug"));
    let actions = match outcome {
        SubmitOutcome::Started(a) => a,
        _ => panic!("expected Started"),
    };
    // No ChangeModel: the `@model` path no longer switches the session model.
    assert!(
        !actions.iter().any(|a| matches!(a, Action::ChangeModel(_))),
        "@model must not emit ChangeModel"
    );
    let message = actions
        .into_iter()
        .find_map(|a| match a {
            Action::SendMessage(inp) => Some(inp.message),
            _ => None,
        })
        .expect("SendMessage action");
    assert_eq!(message, "<model:zai/glm-5> fix the bug");
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

/// A subagent tab whose `SubagentInfo` carries both an answer and an input
/// channel, as an async session does.
fn app_with_subagent_input_tx(id: &str) -> (App, flume::Receiver<String>) {
    let (input_tx, input_rx) = flume::unbounded();
    let mut app = streaming_app();
    let info = subagent_info_full(id, "research", None, Some(input_tx));
    app.update(Msg::Agent(Box::new(Envelope {
        event: AgentEvent::TextDelta { text: "x".into() },
        subagent: Some(info),
        run_id: 1,
    })));
    app.active_chat = 1;
    (app, input_rx)
}

#[test]
fn submit_in_subagent_chat_routes_to_subagent_queue() {
    let (mut app, input_rx) = app_with_subagent_input_tx(TASK_ID);
    let outcome = app.submit_prompt(queued_msg("do more"));
    assert!(matches!(outcome, SubmitOutcome::Queued));
    assert_eq!(
        input_rx.try_recv().unwrap(),
        "do more",
        "message must go to the subagent queue"
    );
    // The subagent's chat shows the message; the main queue is untouched.
    assert_eq!(app.chats[1].last_message_text(), "do more");
    assert!(app.queue.text_messages().is_empty());
}

#[test]
fn submit_in_subagent_chat_with_images_is_rejected() {
    let (mut app, _input_rx) = app_with_subagent_input_tx(TASK_ID);
    let mut msg = queued_msg("hi");
    msg.images
        .push(ImageSource::new(ImageMediaType::Png, Arc::from("dGVzdA==")));
    assert!(matches!(app.submit_prompt(msg), SubmitOutcome::Rejected(_)));
}

#[test]
fn submit_in_finished_subagent_chat_is_rejected() {
    let (mut app, _input_rx) = app_with_subagent_input_tx(TASK_ID);
    // The subagent's driver channel is gone: it finished. Focus stays on it.
    app.subagent_channels.remove(TASK_ID);
    match app.submit_prompt(queued_msg("poke")) {
        SubmitOutcome::Rejected(e) => assert_eq!(e, queue::NO_SUBAGENT_ERR),
        _ => panic!("finished subagent must reject, not start a main turn"),
    }
    assert!(
        app.queue.text_messages().is_empty(),
        "nothing may reach the main queue"
    );
}

#[test]
fn typing_in_subagent_chat_edits_input_and_submits_to_subagent() {
    let (mut app, input_rx) = app_with_subagent_input_tx(TASK_ID);
    // Typing reaches the shared input box on a focused subagent tab.
    app.update(Msg::Key(key(KeyCode::Char('h'))));
    app.update(Msg::Key(key(KeyCode::Char('i'))));
    assert_eq!(app.input_box.buffer.value(), "hi");
    // Enter submits to the subagent's driver queue, not the main agent.
    app.update(Msg::Key(key(KeyCode::Enter)));
    assert_eq!(input_rx.try_recv().unwrap(), "hi");
}

#[test]
fn subagent_completion_queues_reply_to_main() {
    let (mut app, _input_rx) = app_with_subagent_input_tx(TASK_ID);
    // Terminal completion flushes the subagent's history; the driver surfaces
    // the assistant reply in the history messages.
    let messages = vec![Message {
        role: Role::Assistant,
        content: vec![ContentBlock::Text {
            text: "the answer".into(),
        }],
        ..Default::default()
    }];
    app.update(subagent_msg(
        AgentEvent::SubagentHistory {
            tool_use_id: TASK_ID.to_string(),
            messages,
        },
        TASK_ID,
        None,
    ));
    let expected = format!("{SUBAGENT_REPLY_HEADER}{TASK_ID}{SUBAGENT_REPLY_SUFFIX}the answer");
    assert_eq!(app.queue.text_messages(), [expected]);
}

#[test]
fn task_picker_rows_show_snippet() {
    let (mut app, _input_rx) = app_with_subagent_input_tx(TASK_ID);
    app.update(subagent_msg(
        AgentEvent::TextDelta {
            text: "progress line".into(),
        },
        TASK_ID,
        None,
    ));
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
        tool: maki_config::ToolKey::native(BELL_TOOL_NAME),
        scopes: vec!["execute".into()],
    }
}

#[test]
fn turn_complete_emits_bell() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    let (handle, _guard) = maki_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
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
    const RENDER_SECS: f64 = 0.3;
    const MAX_TICK: Duration = Duration::from_millis(50);

    let (handle, guard) = maki_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
    guard
        .host()
        .load_source(
            "slow_splash",
            &format!(
                r##"
maki.api.set_slot("splash.render", function(prev, w, h, t, fade)
  local started = os.clock()
  while os.clock() - started < {RENDER_SECS} do end
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
    let (handle, probe) = maki_lua::test_support::probed_event_handle();
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
    let (handle, _guard) = maki_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
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
    let (handle, _guard) = (maki_lua::EventHandle::disconnected_for_test(), ());
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let ui = UiConfig {
        splash_animation: false,
        ..Default::default()
    };
    let mut app = build_app_with_full(
        dir,
        writer,
        maki_commands::CommandRegistry::new(),
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
    let (handle, _guard) = maki_lua::test_support::spawn_host_for_tests(&["splashes_default"]);
    let dir = StateDir::from_path(env::temp_dir());
    let writer = Arc::new(test_writer(dir.clone()));
    let ui = UiConfig {
        splash_animation: false,
        ..Default::default()
    };
    let mut app = build_app_with_full(
        dir,
        writer,
        maki_commands::CommandRegistry::new(),
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

fn perm_demand(id: &str, tool: maki_config::ToolKey, scopes: Vec<String>) -> InputDemand {
    InputDemand {
        kind: InputKind::Permission,
        blocked_by_modal: false,
        hold_until_submit: false,
        perm: Some(PermissionPayload {
            id: id.into(),
            tool,
            scopes,
            subagent_id: None,
        }),
    }
}

fn bash_perm_demand(id: &str) -> InputDemand {
    perm_demand(
        id,
        maki_config::ToolKey::native(PERM_TOOL),
        vec![PERM_SCOPE.into()],
    )
}

fn below_input_config(height: u16) -> maki_lua::FloatConfig {
    maki_lua::FloatConfig {
        needs_input: true,
        split: maki_lua::Split::Below,
        height: maki_lua::Dimension::Abs(height),
        ..maki_lua::FloatConfig::default()
    }
}

/// Opens a below+needs_input question window through the shared `handle_open_win`
/// path. Returns whether it went active, plus the window's event/command channels.
fn open_question_win(
    app: &mut App,
    focus: bool,
) -> (
    bool,
    flume::Receiver<maki_lua::WinEvent>,
    flume::Sender<maki_lua::WinCommand>,
) {
    let buf = Arc::new(maki_agent::SharedBuf::new());
    let (event_tx, event_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (cmd_tx, cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
    let active = app.handle_open_win(buf, below_input_config(4), focus, event_tx, cmd_rx);
    (active, event_rx, cmd_tx)
}

fn app_with_tall_model_picker() -> App {
    let (mut app, models) = app_with_model_slot();
    let names: Vec<String> = (0..40).map(|i| format!("anthropic/model-{i}")).collect();
    models.store(Some(Arc::new(names)));
    app.run_builtin(BuiltinAction::ModelPicker);
    assert!(app.model_picker.is_open());
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
        maki_config::ToolKey::native(PERM_TOOL),
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
    let popup_buf = Arc::new(maki_agent::SharedBuf::new());
    let popup_config = maki_lua::FloatConfig {
        split: maki_lua::Split::None,
        height: maki_lua::Dimension::Abs(4),
        ..maki_lua::FloatConfig::default()
    };
    let (popup_tx, _popup_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (_popup_cmd_tx, popup_cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
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
    let buf = Arc::new(maki_agent::SharedBuf::new());
    buf.append(maki_agent::SnapshotLine::plain(QUESTION_TEXT.into()));
    let config = below_input_config(8);
    let (event_tx, _event_rx) = flume::bounded::<maki_lua::WinEvent>(8);
    let (_cmd_tx, cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
    app.float_mgr.open(buf, config, true, event_tx, cmd_rx);
    app.active_input = Some(InputKind::Question);
    assert!(app.question_active());

    let (_msg, _bottom, _status, _input, splits) = app.layout_geometry(RENDER_AREA);
    let below = splits
        .rect(maki_lua::Split::Below)
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
    assert!(
        picker_below.contains("model-"),
        "picker content occupies the overlap without the question"
    );
    assert!(
        !both_below.contains("model-"),
        "question painted over the picker in the overlap"
    );
}

#[test]
fn bell_deferred_until_idle_promotion() {
    let mut app = test_app();
    app.status = Status::Streaming;
    app.run_id = 1;
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
    fn open_non_input(split: maki_lua::Split, focus: bool) -> App {
        let mut app = test_app();
        app.last_input = Some(Instant::now());
        // Plan mode with a ready plan: `transition_plan(InteractivePrompt)`
        // would flip it back to drafting, so a ready plan afterwards proves the
        // non-input path never invokes it.
        app.state.mode = Mode::Plan;
        app.state.plan = PlanState::Ready(PathBuf::from("test-plan.md"));
        let buf = Arc::new(maki_agent::SharedBuf::new());
        let config = maki_lua::FloatConfig {
            split,
            height: maki_lua::Dimension::Abs(4),
            ..maki_lua::FloatConfig::default()
        };
        let (event_tx, _event_rx) = flume::bounded::<maki_lua::WinEvent>(8);
        let (_cmd_tx, cmd_rx) = flume::bounded::<maki_lua::WinCommand>(8);
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

    let below = open_non_input(maki_lua::Split::Below, true);
    assert!(
        below.float_mgr.is_focused(),
        "tool-output below split keeps focus"
    );
    let panel = open_non_input(maki_lua::Split::Panel, false);
    assert!(
        !panel.float_mgr.is_focused(),
        "panel with focus=false stays unfocused"
    );
    let popup = open_non_input(maki_lua::Split::None, true);
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
