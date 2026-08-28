use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use maki_commands::{
    AgentTurn, ArgumentArity, BUILTIN_COMMANDS, BuiltinOperation, CommandBehavior,
    CommandCompletion, CommandContent, CommandError, CommandFuture, CommandInvocation,
    CommandOutcome, CommandRegistry, CompletionKey, HostContextRequest, HostContextResponse,
    HostRequest, HostResponse, Producer, ProducerPrecedence, Registration, RegistrationError,
    TargetCapabilities, TargetCapability, ThinkingConfig,
};
use maki_config::ModelPolicy;
use maki_match::{MatchCandidate, Resolution, fuzzy_resolve, fuzzy_resolve_candidates};
use maki_providers::Model;

use crate::headless::InteractiveControl;
use crate::permissions::PermissionManager;
use serde::Deserialize;
use tracing::{debug, warn};

const PROJECT_COMMAND_DIRS: &[&str] = &[".makima/commands", ".claude/commands"];
const GLOBAL_THIRD_PARTY_COMMAND_DIRS: &[&str] = &[".claude/commands"];
const ARGUMENTS_PLACEHOLDER: &str = "$ARGUMENTS";
const STANDARD_COMMANDS_ALREADY_REGISTERED: &str = "standard commands are already registered";
const LOCAL_COMMAND_ATTACHMENTS: &str = "local commands cannot include non-text content";
const NONINTERACTIVE_MODEL_USAGE: &str = "Usage: /model <model>";

pub fn portable_capabilities() -> TargetCapabilities {
    TargetCapabilities::from_slice(&[
        TargetCapability::AgentTurns,
        TargetCapability::ModelSelection,
        TargetCapability::SessionControl,
        TargetCapability::WorkingDirectory,
        TargetCapability::PermissionToggles,
        TargetCapability::ConfigToggles,
    ])
}

pub struct SessionCommandState {
    pub current_model: Mutex<String>,
    model_specs: Mutex<Arc<[Arc<str>]>>,
    cwd: Mutex<PathBuf>,
    fast: AtomicBool,
    workflow: AtomicBool,
}

impl SessionCommandState {
    pub fn new(
        current_model: String,
        model_specs: Arc<[Arc<str>]>,
        cwd: PathBuf,
        fast: bool,
        workflow: bool,
    ) -> Self {
        Self {
            current_model: Mutex::new(current_model),
            model_specs: Mutex::new(model_specs),
            cwd: Mutex::new(cwd),
            fast: AtomicBool::new(fast),
            workflow: AtomicBool::new(workflow),
        }
    }

    pub fn set_model_specs(&self, specs: impl IntoIterator<Item = String>) {
        *self
            .model_specs
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            specs.into_iter().map(Arc::from).collect::<Vec<_>>().into();
    }

    pub fn fast(&self) -> bool {
        self.fast.load(Ordering::Relaxed)
    }

    pub fn workflow(&self) -> bool {
        self.workflow.load(Ordering::Relaxed)
    }
}

pub struct SessionCommandHost {
    model_policy: Arc<ModelPolicy>,
    model_tx: flume::Sender<Model>,
    control_tx: flume::Sender<InteractiveControl>,
    state: Arc<SessionCommandState>,
    permissions: Arc<PermissionManager>,
}

impl SessionCommandHost {
    pub fn new(
        model_policy: Arc<ModelPolicy>,
        model_tx: flume::Sender<Model>,
        control_tx: flume::Sender<InteractiveControl>,
        state: Arc<SessionCommandState>,
        permissions: Arc<PermissionManager>,
    ) -> Self {
        Self {
            model_policy,
            model_tx,
            control_tx,
            state,
            permissions,
        }
    }

    fn control(
        &self,
        make: impl FnOnce(flume::Sender<Result<(), String>>) -> InteractiveControl + Send + 'static,
    ) -> CommandFuture<Result<HostResponse, CommandError>> {
        let control_tx = self.control_tx.clone();
        Box::pin(async move {
            let (reply, response) = flume::bounded(1);
            control_tx
                .send(make(reply))
                .map_err(|_| CommandError::StaleTarget)?;
            response
                .recv_async()
                .await
                .map_err(|_| CommandError::StaleTarget)?
                .map(|()| HostResponse::Completed)
                .map_err(|error| CommandError::Producer(Arc::from(error)))
        })
    }
}

impl maki_commands::CommandHost for SessionCommandHost {
    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        let operation = match request {
            HostRequest::Context(HostContextRequest::ModelSpecs) => {
                let specs = self
                    .state
                    .model_specs
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                return Box::pin(async move {
                    Ok(HostResponse::Context(HostContextResponse::Values(specs)))
                });
            }
            HostRequest::Context(HostContextRequest::ThemeNames) => {
                return Box::pin(async {
                    Ok(HostResponse::Context(HostContextResponse::Unavailable))
                });
            }
            HostRequest::Context(HostContextRequest::WorkingDirectory) => {
                let cwd = self
                    .state
                    .cwd
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone();
                return Box::pin(async move {
                    Ok(HostResponse::Context(
                        HostContextResponse::WorkingDirectory(cwd),
                    ))
                });
            }
            HostRequest::Context(HostContextRequest::ThinkingConfig) => {
                return Box::pin(async {
                    Ok(HostResponse::Context(HostContextResponse::Unavailable))
                });
            }
            HostRequest::Builtin(operation) => operation,
        };
        match operation {
            maki_commands::BuiltinOperation::Compact => self.control(InteractiveControl::Compact),
            maki_commands::BuiltinOperation::ResetSession => {
                self.control(InteractiveControl::Reset)
            }
            maki_commands::BuiltinOperation::ToggleYolo => {
                self.permissions.toggle_yolo();
                Box::pin(async { Ok(HostResponse::Completed) })
            }
            maki_commands::BuiltinOperation::ToggleFast => {
                self.state.fast.fetch_xor(true, Ordering::Relaxed);
                Box::pin(async { Ok(HostResponse::Completed) })
            }
            maki_commands::BuiltinOperation::ToggleWorkflow => {
                self.state.workflow.fetch_xor(true, Ordering::Relaxed);
                Box::pin(async { Ok(HostResponse::Completed) })
            }
            maki_commands::BuiltinOperation::ChangeDirectory { path } => {
                let future = self.control({
                    let path = path.clone();
                    move |reply| InteractiveControl::ChangeDirectory { path, reply }
                });
                let state = Arc::clone(&self.state);
                Box::pin(async move {
                    let response = future.await?;
                    *state.cwd.lock().unwrap_or_else(|error| error.into_inner()) = path;
                    Ok(response)
                })
            }
            maki_commands::BuiltinOperation::SetModel { spec } => {
                let model_policy = Arc::clone(&self.model_policy);
                let model_tx = self.model_tx.clone();
                let state = Arc::clone(&self.state);
                Box::pin(async move {
                    if !model_policy.allows(&spec) {
                        return Err(CommandError::Producer(Arc::from(
                            "model is not allowed by policy",
                        )));
                    }
                    let model = Model::from_spec(&spec)
                        .map_err(|error| CommandError::Producer(Arc::from(error.to_string())))?;
                    model_tx.send(model).map_err(|_| {
                        CommandError::Producer(Arc::from("session ended before model change"))
                    })?;
                    *state
                        .current_model
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = spec.to_string();
                    Ok(HostResponse::Completed)
                })
            }
            maki_commands::BuiltinOperation::QuickQuestion {
                question,
                attachments,
            } => Box::pin(async move {
                Ok(HostResponse::AgentTurn(AgentTurn {
                    content: CommandContent {
                        text: question,
                        attachments,
                    },
                    prompt: None,
                }))
            }),
            operation => Box::pin(async move {
                Err(CommandError::Producer(Arc::from(format!(
                    "unsupported builtin operation: {operation:?}"
                ))))
            }),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct Frontmatter {
    name: Option<String>,
    description: Option<String>,
    #[serde(rename = "argument-hint")]
    argument_hint: Option<String>,
}

pub(crate) fn find_project_ancestor_dirs(cwd: &Path) -> impl Iterator<Item = PathBuf> {
    let mut current = Some(cwd.to_path_buf());

    std::iter::from_fn(move || {
        let dir = current.take()?;
        if !dir.join(".git").exists() {
            current = dir.parent().map(Path::to_path_buf);
        }
        Some(dir)
    })
}

fn parse_frontmatter(content: &str) -> (Frontmatter, &str) {
    let content = content.trim_start();

    let Some(rest) = content.strip_prefix("---") else {
        return (Frontmatter::default(), content);
    };

    let Some(end) = rest.find("\n---") else {
        return (Frontmatter::default(), content);
    };

    let yaml = &rest[1..end + 1];
    let body = rest[end + 4..].trim();

    let fm = serde_yaml::from_str(yaml).unwrap_or_default();
    (fm, body)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandScope {
    Project,
    User,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomCommand {
    pub name: String,
    pub description: String,
    pub content: String,
    pub scope: CommandScope,
    pub accepts_args: bool,
    pub argument_hint: Option<String>,
}

#[derive(Clone)]
struct CustomCommandBehavior {
    command: CustomCommand,
}

impl CommandBehavior for CustomCommandBehavior {
    fn execute(
        &self,
        invocation: CommandInvocation,
    ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
        let content = CommandContent {
            text: Arc::from(self.command.render(&invocation.arguments)),
            attachments: invocation.content.attachments.clone(),
        };
        Box::pin(async move {
            Ok(CommandOutcome::AgentTurn(AgentTurn {
                content,
                prompt: None,
            }))
        })
    }
}

struct BuiltinBehavior {
    id: maki_commands::BuiltinId,
}

async fn host_context(
    invocation: &CommandInvocation,
    request: HostContextRequest,
) -> Result<HostContextResponse, CommandError> {
    match invocation
        .host_request(HostRequest::Context(request))
        .await?
    {
        HostResponse::Context(response) => Ok(response),
        response => Err(CommandError::Producer(Arc::from(format!(
            "invalid host context response: {response:?}"
        )))),
    }
}

fn resolve_model(argument: &str, specs: &[Arc<str>]) -> Result<Arc<str>, CommandError> {
    if argument.contains('/') {
        Model::from_spec(argument)
            .map_err(|error| CommandError::Producer(Arc::from(error.to_string())))?;
        return Ok(Arc::from(argument));
    }
    let candidates = specs
        .iter()
        .map(|spec| {
            let (provider, model_id) = spec.split_once('/').unwrap_or(("", spec));
            MatchCandidate {
                value: spec,
                fields: vec![spec, provider, model_id],
            }
        })
        .collect::<Vec<_>>();
    match fuzzy_resolve_candidates(argument, &candidates) {
        Resolution::Unique(index) => Ok(Arc::clone(&specs[index])),
        Resolution::NoMatch => Err(CommandError::Producer(Arc::from(format!(
            "no model matches: {argument}"
        )))),
        Resolution::Ambiguous => Err(CommandError::Producer(Arc::from(format!(
            "ambiguous model: {argument}"
        )))),
    }
}

fn parse_thinking(input: &str, current: ThinkingConfig) -> Result<ThinkingConfig, CommandError> {
    match input.trim().to_ascii_lowercase().as_str() {
        "" | "toggle" => Ok(if current == ThinkingConfig::Off {
            ThinkingConfig::Adaptive
        } else {
            ThinkingConfig::Off
        }),
        "off" | "false" => Ok(ThinkingConfig::Off),
        "on" | "true" | "adaptive" => Ok(ThinkingConfig::Adaptive),
        "minimal" => Ok(ThinkingConfig::Minimal),
        "low" => Ok(ThinkingConfig::Low),
        "medium" => Ok(ThinkingConfig::Medium),
        "high" => Ok(ThinkingConfig::High),
        "xhigh" => Ok(ThinkingConfig::XHigh),
        "max" => Ok(ThinkingConfig::Max),
        budget => budget
            .parse::<u32>()
            .ok()
            .filter(|budget| *budget > 0)
            .map(ThinkingConfig::Budget)
            .ok_or_else(|| CommandError::Producer(Arc::from("invalid thinking mode"))),
    }
}

impl CommandBehavior for BuiltinBehavior {
    fn execute(
        &self,
        invocation: CommandInvocation,
    ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
        if self.id != maki_commands::BuiltinId::Btw && !invocation.content.attachments.is_empty() {
            return Box::pin(async {
                Err(CommandError::Producer(Arc::from(LOCAL_COMMAND_ATTACHMENTS)))
            });
        }
        let arguments = invocation.arguments.trim().to_owned();
        let id = self.id;
        Box::pin(async move {
            let operation = match id {
                maki_commands::BuiltinId::Tasks => BuiltinOperation::OpenTasks,
                maki_commands::BuiltinId::Compact => BuiltinOperation::Compact,
                maki_commands::BuiltinId::New => BuiltinOperation::ResetSession,
                maki_commands::BuiltinId::Help => BuiltinOperation::ToggleHelp,
                maki_commands::BuiltinId::Usage => BuiltinOperation::ToggleUsage,
                maki_commands::BuiltinId::Queue => BuiltinOperation::FocusQueue,
                maki_commands::BuiltinId::Model if arguments.is_empty() => {
                    if !invocation.target_supports(TargetCapability::InteractiveUi) {
                        return Err(CommandError::Producer(Arc::from(
                            NONINTERACTIVE_MODEL_USAGE,
                        )));
                    }
                    BuiltinOperation::OpenModelPicker
                }
                maki_commands::BuiltinId::Model => {
                    let specs = if arguments.contains('/') {
                        Arc::from([])
                    } else {
                        let HostContextResponse::Values(specs) =
                            host_context(&invocation, HostContextRequest::ModelSpecs).await?
                        else {
                            return Err(CommandError::Producer(Arc::from(
                                "model resolution is unavailable",
                            )));
                        };
                        specs
                    };
                    BuiltinOperation::SetModel {
                        spec: resolve_model(&arguments, &specs)?,
                    }
                }
                maki_commands::BuiltinId::Theme if arguments.is_empty() => {
                    BuiltinOperation::OpenThemePicker
                }
                maki_commands::BuiltinId::Theme => {
                    let HostContextResponse::Values(names) =
                        host_context(&invocation, HostContextRequest::ThemeNames).await?
                    else {
                        return Err(CommandError::Producer(Arc::from(
                            "theme resolution is unavailable",
                        )));
                    };
                    let Resolution::Unique(index) = fuzzy_resolve(&arguments, &names) else {
                        return Err(CommandError::Producer(Arc::from(format!(
                            "theme is unknown or ambiguous: {arguments}"
                        ))));
                    };
                    BuiltinOperation::SetTheme {
                        name: Arc::clone(&names[index]),
                    }
                }
                maki_commands::BuiltinId::Mcp => BuiltinOperation::OpenMcpPicker,
                maki_commands::BuiltinId::Login => BuiltinOperation::OpenLoginPicker,
                maki_commands::BuiltinId::Cd => {
                    let HostContextResponse::WorkingDirectory(cwd) =
                        host_context(&invocation, HostContextRequest::WorkingDirectory).await?
                    else {
                        return Err(CommandError::Producer(Arc::from(
                            "working-directory resolution is unavailable",
                        )));
                    };
                    let path = if arguments.is_empty() {
                        maki_storage::paths::home().unwrap_or_default()
                    } else if let Some(rest) = arguments.strip_prefix('~') {
                        let home = maki_storage::paths::home().unwrap_or_default();
                        if rest.is_empty() {
                            home
                        } else {
                            home.join(rest.trim_start_matches('/'))
                        }
                    } else {
                        let path = PathBuf::from(&arguments);
                        if path.is_relative() {
                            cwd.join(path)
                        } else {
                            path
                        }
                    };
                    BuiltinOperation::ChangeDirectory { path }
                }
                maki_commands::BuiltinId::Btw => BuiltinOperation::QuickQuestion {
                    question: Arc::from(arguments),
                    attachments: invocation.content.attachments.clone(),
                },
                maki_commands::BuiltinId::Yolo => BuiltinOperation::ToggleYolo,
                maki_commands::BuiltinId::Thinking => {
                    let HostContextResponse::ThinkingConfig(current) =
                        host_context(&invocation, HostContextRequest::ThinkingConfig).await?
                    else {
                        return Err(CommandError::Producer(Arc::from(
                            "thinking configuration is unavailable",
                        )));
                    };
                    BuiltinOperation::SetThinking {
                        config: parse_thinking(&arguments, current)?,
                    }
                }
                maki_commands::BuiltinId::Fast => BuiltinOperation::ToggleFast,
                maki_commands::BuiltinId::Workflow => BuiltinOperation::ToggleWorkflow,
                maki_commands::BuiltinId::Exit => BuiltinOperation::Exit,
                maki_commands::BuiltinId::Reload => BuiltinOperation::Reload,
            };
            match invocation
                .host_request(HostRequest::Builtin(operation))
                .await?
            {
                HostResponse::Completed => Ok(CommandOutcome::Completed),
                HostResponse::AgentTurn(turn) => Ok(CommandOutcome::AgentTurn(turn)),
                HostResponse::Context(_) => Err(CommandError::Producer(Arc::from(
                    "invalid builtin host response",
                ))),
            }
        })
    }
}

#[derive(Default)]
pub struct StandardCompletions {
    pub model: Option<Arc<dyn CommandCompletion>>,
    pub theme: Option<Arc<dyn CommandCompletion>>,
}

pub struct StandardCommands {
    builtin: Producer,
    application: Producer,
    registry: CommandRegistry,
}

impl Drop for StandardCommands {
    fn drop(&mut self) {
        self.builtin.remove();
        self.application.remove();
        self.registry.release_standard_commands();
    }
}

impl StandardCommands {
    pub fn register(
        registry: &CommandRegistry,
        commands: &[CustomCommand],
        completions: StandardCompletions,
    ) -> Result<Self, CommandError> {
        if !registry.claim_standard_commands() {
            return Err(CommandError::Producer(Arc::from(
                STANDARD_COMMANDS_ALREADY_REGISTERED,
            )));
        }
        let builtin = registry.create_producer(ProducerPrecedence::Builtin);
        builtin
            .replace(
                BUILTIN_COMMANDS
                    .iter()
                    .map(|command| Registration {
                        spec: command.spec(),
                        behavior: Arc::new(BuiltinBehavior { id: command.id }),
                        completion: match command.completion {
                            Some(CompletionKey::Model) => completions.model.clone(),
                            Some(CompletionKey::Theme) => completions.theme.clone(),
                            None => None,
                        },
                    })
                    .collect(),
            )
            .inspect_err(|_| registry.release_standard_commands())
            .map_err(registration_error)?;
        let application = registry.create_producer(ProducerPrecedence::Application);
        if let Err(error) = register_commands(&application, commands) {
            builtin.remove();
            registry.release_standard_commands();
            return Err(registration_error(error));
        }
        Ok(Self {
            builtin,
            application,
            registry: registry.clone(),
        })
    }

    pub fn register_custom_commands(
        &self,
        commands: &[CustomCommand],
    ) -> Result<(), RegistrationError> {
        register_commands(&self.application, commands)
    }

    pub fn builtin_producer(&self) -> &Producer {
        &self.builtin
    }

    pub fn application_producer(&self) -> &Producer {
        &self.application
    }
}

fn registration_error(error: RegistrationError) -> CommandError {
    CommandError::Producer(Arc::from(error.to_string()))
}

pub fn register_commands(
    producer: &Producer,
    commands: &[CustomCommand],
) -> Result<(), RegistrationError> {
    producer.replace(
        commands
            .iter()
            .cloned()
            .map(|command| Registration {
                spec: maki_commands::CommandSpec {
                    name: Arc::from(command.display_name()),
                    aliases: Arc::from([]),
                    arguments: if command.has_args() {
                        ArgumentArity::ANY
                    } else {
                        ArgumentArity::NONE
                    },
                    docs: maki_commands::CommandDocs {
                        summary: Arc::from(command.description.clone()),
                        argument_hint: command.argument_hint.clone().map(Arc::from),
                    },
                    required_capabilities: Default::default(),
                },
                behavior: Arc::new(CustomCommandBehavior { command }),
                completion: None,
            })
            .collect(),
    )
}

impl CustomCommand {
    pub fn display_name(&self) -> String {
        let prefix = match self.scope {
            CommandScope::Project => "/project",
            CommandScope::User => "/user",
        };
        format!("{prefix}:{}", self.name)
    }

    pub fn has_args(&self) -> bool {
        self.accepts_args
    }

    pub fn render(&self, args: &str) -> String {
        self.content.replace(ARGUMENTS_PLACEHOLDER, args)
    }
}

pub fn discover_commands(cwd: &Path) -> Vec<CustomCommand> {
    discover_commands_inner(
        cwd,
        maki_storage::paths::home().as_deref(),
        maki_storage::paths::config_dir().ok().as_deref(),
    )
}

fn discover_commands_inner(
    cwd: &Path,
    home: Option<&Path>,
    xdg_config: Option<&Path>,
) -> Vec<CustomCommand> {
    let mut commands: HashMap<String, CustomCommand> = HashMap::new();

    for dir in maki_storage::paths::user_config_dirs(home, xdg_config, "commands") {
        scan_command_dir(&dir, CommandScope::User, &mut commands);
    }
    if let Some(home) = home {
        for dir in GLOBAL_THIRD_PARTY_COMMAND_DIRS {
            scan_command_dir(&home.join(dir), CommandScope::User, &mut commands);
        }
    }

    for dir in find_project_ancestor_dirs(cwd) {
        for cmd_dir in PROJECT_COMMAND_DIRS {
            scan_command_dir(&dir.join(cmd_dir), CommandScope::Project, &mut commands);
        }
    }

    let mut result: Vec<_> = commands.into_values().collect();
    result.sort_by(|a, b| a.name.cmp(&b.name));
    debug!(count = result.len(), "commands discovered");
    result
}

fn scan_command_dir(
    dir: &Path,
    scope: CommandScope,
    commands: &mut HashMap<String, CustomCommand>,
) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(ext) = path.extension() else {
            continue;
        };
        if ext != "md" {
            continue;
        }
        if let Ok(content) = fs::read_to_string(&path)
            && let Some(cmd) = parse_command(&content, &path, scope.clone())
            && let Some(existing) = commands.insert(cmd.name.clone(), cmd)
        {
            debug!(
                command = existing.name,
                path = ?path,
                "command overridden by later priority"
            );
        }
    }
}

fn parse_command(content: &str, path: &Path, scope: CommandScope) -> Option<CustomCommand> {
    let name_from_file = path.file_stem()?.to_string_lossy().into_owned();
    let (fm, body) = parse_frontmatter(content);

    if body.is_empty() {
        let name = fm.name.as_deref().unwrap_or(&name_from_file);
        warn!(command = name, path = ?path, "command file has no content, skipping");
        return None;
    }

    let accepts_args = fm.argument_hint.is_some() || body.contains(ARGUMENTS_PLACEHOLDER);

    Some(CustomCommand {
        name: fm.name.unwrap_or(name_from_file),
        description: fm.description.unwrap_or_default(),
        content: body.to_string(),
        scope,
        accepts_args,
        argument_hint: fm.argument_hint,
    })
}

#[cfg(test)]
mod tests {
    use maki_commands::CommandHost;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;

    #[test_case(
        "---\nname: review\ndescription: Code review\nargument-hint: <file>\n---\nReview $ARGUMENTS",
        "review", "Code review", true
        ; "full_frontmatter"
    )]
    #[test_case(
        "Review $ARGUMENTS",
        "test-cmd", "", true
        ; "body_placeholder_without_hint"
    )]
    #[test_case(
        "Just do things",
        "test-cmd", "", false
        ; "no_frontmatter_uses_filename"
    )]
    #[test_case(
        "---\ndescription: Quick fix\n---\nFix the code",
        "test-cmd", "Quick fix", false
        ; "no_args_placeholder"
    )]
    fn parse_command_fields(
        content: &str,
        expected_name: &str,
        expected_desc: &str,
        expected_has_args: bool,
    ) {
        let path = PathBuf::from("/fake/test-cmd.md");
        let cmd = parse_command(content, &path, CommandScope::Project).unwrap();
        assert_eq!(cmd.name, expected_name);
        assert_eq!(cmd.description, expected_desc);
        assert_eq!(cmd.has_args(), expected_has_args);
        assert_eq!(
            cmd.argument_hint.as_deref(),
            (content.contains("argument-hint")).then_some("<file>")
        );
    }

    struct FakeCommandHost;

    #[derive(Default)]
    struct RecordingCommandHost(Mutex<Vec<maki_commands::BuiltinOperation>>);

    impl maki_commands::CommandHost for RecordingCommandHost {
        fn request(
            &self,
            request: maki_commands::HostRequest,
        ) -> CommandFuture<Result<maki_commands::HostResponse, CommandError>> {
            match request {
                HostRequest::Builtin(operation) => {
                    self.0
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .push(operation);
                    Box::pin(async { Ok(maki_commands::HostResponse::Completed) })
                }
                HostRequest::Context(request) => Box::pin(async move {
                    let response = match request {
                        HostContextRequest::ModelSpecs => {
                            HostContextResponse::Values(Arc::from([Arc::from("openai/gpt-5")]))
                        }
                        HostContextRequest::ThemeNames => {
                            HostContextResponse::Values(Arc::from([Arc::from("dark")]))
                        }
                        HostContextRequest::WorkingDirectory => {
                            HostContextResponse::WorkingDirectory(PathBuf::from("/project"))
                        }
                        HostContextRequest::ThinkingConfig => {
                            HostContextResponse::ThinkingConfig(ThinkingConfig::Off)
                        }
                    };
                    Ok(HostResponse::Context(response))
                }),
            }
        }
    }

    impl maki_commands::CommandHost for FakeCommandHost {
        fn request(
            &self,
            _request: maki_commands::HostRequest,
        ) -> CommandFuture<Result<maki_commands::HostResponse, CommandError>> {
            Box::pin(async { Ok(maki_commands::HostResponse::Completed) })
        }
    }

    fn target(registry: &maki_commands::CommandRegistry) -> maki_commands::TargetHandle {
        registry.bind_target(
            maki_commands::TargetCapabilities::default(),
            Arc::new(FakeCommandHost),
        )
    }

    fn custom_command(content: &str) -> CustomCommand {
        CustomCommand {
            name: "review".into(),
            description: "Code review".into(),
            content: content.into(),
            scope: CommandScope::Project,
            accepts_args: true,
            argument_hint: None,
        }
    }

    #[test]
    fn frontmatter_argument_hint_is_published() {
        let path = PathBuf::from("/fake/review.md");
        let command = parse_command(
            "---\nargument-hint: <file>\n---\nReview $ARGUMENTS",
            &path,
            CommandScope::Project,
        )
        .unwrap();
        let registry = maki_commands::CommandRegistry::new();
        let producer = registry.create_producer(maki_commands::ProducerPrecedence::Application);
        register_commands(&producer, &[command]).unwrap();
        let target = target(&registry);
        assert_eq!(
            registry
                .resolve_for(&target, "/project:review")
                .unwrap()
                .spec()
                .docs
                .argument_hint
                .as_deref(),
            Some("<file>")
        );
    }

    #[test]
    fn custom_command_dispatch_preserves_markdown_attachments() {
        let registry = maki_commands::CommandRegistry::new();
        let producer = registry.create_producer(maki_commands::ProducerPrecedence::Application);
        register_commands(&producer, &[custom_command("Review $ARGUMENTS")]).unwrap();
        let target = target(&registry);
        let attachments = Arc::from([maki_commands::CommandAttachment {
            media_type: Arc::from("text/markdown"),
            data: Arc::from("# Context"),
        }]);

        let result = smol::block_on(registry.dispatch_input(
            &target,
            maki_commands::CommandContent {
                text: Arc::from("/project:review src/lib.rs"),
                attachments: Arc::clone(&attachments),
            },
        ));

        let maki_commands::InputDispatch::Dispatched(maki_commands::CommandOutcome::AgentTurn(
            turn,
        )) = result
        else {
            panic!("custom command did not produce an agent turn");
        };
        assert_eq!(turn.content.text.as_ref(), "Review src/lib.rs");
        assert_eq!(turn.content.attachments, attachments);
    }

    #[test]
    fn builtin_arguments_are_centrally_interpreted() {
        let registry = maki_commands::CommandRegistry::new();
        let _commands =
            StandardCommands::register(&registry, &[], StandardCompletions::default()).unwrap();
        let host = Arc::new(RecordingCommandHost::default());
        let target = registry.bind_target(TargetCapabilities::ALL, host.clone());

        for input in [
            "/model",
            "/model openai/gpt-5",
            "/theme",
            "/theme dark",
            "/btw explain this",
        ] {
            let result = smol::block_on(registry.dispatch_input(&target, input.into()));
            assert!(matches!(
                result,
                maki_commands::InputDispatch::Dispatched(CommandOutcome::Completed)
            ));
        }

        assert_eq!(
            *host.0.lock().unwrap_or_else(|error| error.into_inner()),
            vec![
                maki_commands::BuiltinOperation::OpenModelPicker,
                maki_commands::BuiltinOperation::SetModel {
                    spec: Arc::from("openai/gpt-5"),
                },
                maki_commands::BuiltinOperation::OpenThemePicker,
                maki_commands::BuiltinOperation::SetTheme {
                    name: Arc::from("dark"),
                },
                maki_commands::BuiltinOperation::QuickQuestion {
                    question: Arc::from("explain this"),
                    attachments: Arc::from([]),
                },
            ]
        );

        let portable_host = Arc::new(RecordingCommandHost::default());
        let portable = registry.bind_target(portable_capabilities(), portable_host.clone());
        assert!(matches!(
            smol::block_on(registry.dispatch_input(&portable, "/model".into())),
            maki_commands::InputDispatch::Dispatched(CommandOutcome::Failed(
                CommandError::Producer(message)
            )) if message.as_ref() == NONINTERACTIVE_MODEL_USAGE
        ));
        assert!(
            portable_host
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty()
        );
    }

    #[test]
    fn session_host_dispatches_portable_state_operations() {
        let (model_tx, model_rx) = flume::unbounded();
        let (control_tx, control_rx) = flume::unbounded();
        let state = Arc::new(SessionCommandState::new(
            OFFLINE_MODEL.into(),
            Arc::from([]),
            PathBuf::from("/project"),
            false,
            false,
        ));
        let permissions = Arc::new(PermissionManager::new(
            maki_config::PermissionsConfig::default(),
            PathBuf::from("/project"),
            Arc::default(),
        ));
        let host = SessionCommandHost::new(
            Arc::new(ModelPolicy::default()),
            model_tx,
            control_tx,
            Arc::clone(&state),
            Arc::clone(&permissions),
        );
        let request = |operation| HostRequest::Builtin(operation);
        assert!(matches!(
            smol::block_on(host.request(request(maki_commands::BuiltinOperation::ToggleYolo))),
            Ok(HostResponse::Completed)
        ));
        assert!(permissions.is_yolo());
        assert!(matches!(
            smol::block_on(host.request(request(maki_commands::BuiltinOperation::ToggleFast))),
            Ok(HostResponse::Completed)
        ));
        assert!(state.fast());
        assert!(matches!(
            smol::block_on(host.request(request(maki_commands::BuiltinOperation::ToggleWorkflow))),
            Ok(HostResponse::Completed)
        ));
        assert!(state.workflow());

        let quick = smol::block_on(host.request(HostRequest::Builtin(
            maki_commands::BuiltinOperation::QuickQuestion {
                question: Arc::from("explain this"),
                attachments: Arc::from([]),
            },
        )));
        assert!(matches!(
            quick,
            Ok(HostResponse::AgentTurn(AgentTurn { content, .. }))
                if content.text.as_ref() == "explain this"
        ));

        let compact = host.request(request(maki_commands::BuiltinOperation::Compact));
        let compact_task = smol::spawn(compact);
        let InteractiveControl::Compact(reply) = control_rx.recv().unwrap() else {
            panic!("expected compact control");
        };
        reply.send(Ok(())).unwrap();
        assert!(matches!(
            smol::block_on(compact_task),
            Ok(HostResponse::Completed)
        ));

        let directory = host.request(request(maki_commands::BuiltinOperation::ChangeDirectory {
            path: PathBuf::from("/tmp"),
        }));
        let directory_task = smol::spawn(directory);
        let InteractiveControl::ChangeDirectory { path, reply } = control_rx.recv().unwrap() else {
            panic!("expected change-directory control");
        };
        assert_eq!(path, PathBuf::from("/tmp"));
        reply.send(Ok(())).unwrap();
        assert!(matches!(
            smol::block_on(directory_task),
            Ok(HostResponse::Completed)
        ));

        let model = Model::from_spec(OFFLINE_MODEL).unwrap();
        let result = smol::block_on(host.request(HostRequest::Builtin(
            maki_commands::BuiltinOperation::SetModel {
                spec: Arc::from(OFFLINE_MODEL),
            },
        )));
        assert!(matches!(result, Ok(HostResponse::Completed)));
        assert_eq!(model_rx.recv().unwrap().spec(), model.spec());
    }

    const OFFLINE_MODEL: &str = "openai/gpt-5";

    #[test]
    fn local_builtin_rejects_attachments() {
        let registry = maki_commands::CommandRegistry::new();
        let commands =
            StandardCommands::register(&registry, &[], StandardCompletions::default()).unwrap();
        let target = registry.bind_target(portable_capabilities(), Arc::new(FakeCommandHost));
        let content = maki_commands::CommandContent {
            text: Arc::from("/compact"),
            attachments: Arc::from([maki_commands::CommandAttachment {
                media_type: Arc::from("image/png"),
                data: Arc::from("AAAA"),
            }]),
        };

        let result = smol::block_on(registry.dispatch_input(&target, content));

        assert!(matches!(
            result,
            maki_commands::InputDispatch::Dispatched(CommandOutcome::Failed(
                CommandError::Producer(message)
            )) if message.as_ref() == LOCAL_COMMAND_ATTACHMENTS
        ));
        drop(commands);
    }

    #[test]
    fn standard_commands_register_only_once() {
        let registry = maki_commands::CommandRegistry::new();
        let first =
            StandardCommands::register(&registry, &[], StandardCompletions::default()).unwrap();
        let second = StandardCommands::register(&registry, &[], StandardCompletions::default());
        assert!(matches!(
            second,
            Err(CommandError::Producer(message)) if message.as_ref() == STANDARD_COMMANDS_ALREADY_REGISTERED
        ));
        drop(first);
        assert!(
            registry
                .presented_commands(&target(&registry))
                .unwrap()
                .is_empty()
        );
        let third = StandardCommands::register(&registry, &[], StandardCompletions::default());
        assert!(third.is_ok());
    }

    #[test]
    fn parse_command_empty_body_returns_none() {
        let path = PathBuf::from("/fake/empty.md");
        assert!(parse_command("---\nname: empty\n---\n   \n", &path, CommandScope::User).is_none());
    }

    #[test_case(CommandScope::Project, "/project:review" ; "project_scope")]
    #[test_case(CommandScope::User, "/user:review" ; "user_scope")]
    fn display_name_prefix(scope: CommandScope, expected: &str) {
        let cmd = CustomCommand {
            name: "review".into(),
            description: String::new(),
            content: "body".into(),
            scope,
            accepts_args: false,
            argument_hint: None,
        };
        assert_eq!(cmd.display_name(), expected);
    }

    #[test]
    fn discover_project_overrides_global() {
        let project = TempDir::new().unwrap();
        let cmd_dir = project.path().join(".makima/commands");
        fs::create_dir_all(&cmd_dir).unwrap();
        fs::write(
            cmd_dir.join("overlap.md"),
            "---\ndescription: Project version\n---\nProject content",
        )
        .unwrap();

        let global = TempDir::new().unwrap();
        let global_cmd_dir = global.path().join(".makima/commands");
        fs::create_dir_all(&global_cmd_dir).unwrap();
        fs::write(
            global_cmd_dir.join("overlap.md"),
            "---\ndescription: Global version\n---\nGlobal content",
        )
        .unwrap();

        let commands = discover_commands_inner(project.path(), Some(global.path()), None);
        let overlap: Vec<_> = commands.iter().filter(|c| c.name == "overlap").collect();
        assert_eq!(overlap.len(), 1);
        assert_eq!(overlap[0].description, "Project version");
        assert_eq!(overlap[0].scope, CommandScope::Project);
    }

    #[test]
    fn discover_supports_both_dir_sources() {
        let dir = TempDir::new().unwrap();

        for (cmd_dir, filename) in [
            (".makima/commands", "a-cmd.md"),
            (".claude/commands", "b-cmd.md"),
        ] {
            let path = dir.path().join(cmd_dir);
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join(filename), "Content").unwrap();
        }

        let commands = discover_commands_inner(dir.path(), None, None);
        let names: Vec<_> = commands.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"a-cmd"));
        assert!(names.contains(&"b-cmd"));
    }

    #[test]
    fn discover_ignores_non_md_files() {
        let dir = TempDir::new().unwrap();
        let cmd_dir = dir.path().join(".makima/commands");
        fs::create_dir_all(&cmd_dir).unwrap();
        fs::write(cmd_dir.join("valid.md"), "Content").unwrap();
        fs::write(cmd_dir.join("invalid.txt"), "Content").unwrap();

        let commands = discover_commands_inner(dir.path(), None, None);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].name, "valid");
    }

    #[test_case(
        "---\n: invalid: yaml: [[\n---\nBody",
        None, "Body"
        ; "invalid_yaml_falls_back"
    )]
    #[test_case(
        "---\nname: oops\nThis never closes",
        None, "---\nname: oops\nThis never closes"
        ; "no_closing_delimiter"
    )]
    #[test_case(
        "  \n---\nname: trimmed\n---\nBody",
        Some("trimmed"), "Body"
        ; "leading_whitespace"
    )]
    fn parse_frontmatter_edge_cases(input: &str, expected_name: Option<&str>, expected_body: &str) {
        let (fm, body) = parse_frontmatter(input);
        assert_eq!(fm.name.as_deref(), expected_name);
        assert_eq!(body, expected_body);
    }

    #[test]
    fn find_project_ancestor_dirs_stops_at_git() {
        let tmp = TempDir::new().unwrap();
        let deep = tmp.path().join("a/b/c");
        fs::create_dir_all(&deep).unwrap();
        fs::create_dir_all(tmp.path().join("a/.git")).unwrap();

        let dirs: Vec<_> = find_project_ancestor_dirs(&deep).collect();
        let dir_strs: Vec<_> = dirs
            .iter()
            .map(|d| d.to_string_lossy().into_owned())
            .collect();

        assert!(dir_strs.contains(&deep.to_string_lossy().into_owned()));
        assert!(dir_strs.contains(&tmp.path().join("a/b").to_string_lossy().into_owned()));
        assert!(dir_strs.contains(&tmp.path().join("a").to_string_lossy().into_owned()));
        assert!(
            !dir_strs.contains(&tmp.path().to_string_lossy().into_owned()),
            "should not traverse past .git"
        );
    }
}
