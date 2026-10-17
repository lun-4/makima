pub use maki_domain::ModelSelection;
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use crate::arguments::{
    CommandArguments, CompletionPolicy, StaticArgumentKind, StaticCommandArguments,
    StaticPositionalArgument,
};
use crate::completion::CommandCompletion;
use crate::dispatch::{CommandAttachment, CommandBehavior};

pub type CommandFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub const MAX_COMMAND_DEPTH: usize = 8;
pub const COMPACT_COMMAND_NAME: &str = "/compact";
pub const MODEL_COMMAND_NAME: &str = "/model";
pub(crate) const MALFORMED_MODEL_OPTIONS: &str = "malformed model options";
pub(crate) const MIXED_QUOTED_MODEL_SYNTAX: &str = "mixed quoted model syntax";
pub(crate) const MODEL_OPTION_KEY_VALUE: &str = "expected model option key=value";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TargetCapability {
    AgentTurns,
    ModelSelection,
    HistoryCompaction,
    SessionReplacement,
    WorkingDirectory,
    PermissionToggles,
    ConfigToggles,
    InteractiveUi,
    ApplicationLifecycle,
    Reload,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TargetCapabilities(u16);

impl TargetCapabilities {
    pub const NONE: Self = Self(0);
    pub const ALL: Self = Self::from_slice(&[
        TargetCapability::AgentTurns,
        TargetCapability::ModelSelection,
        TargetCapability::HistoryCompaction,
        TargetCapability::SessionReplacement,
        TargetCapability::WorkingDirectory,
        TargetCapability::PermissionToggles,
        TargetCapability::ConfigToggles,
        TargetCapability::InteractiveUi,
        TargetCapability::ApplicationLifecycle,
        TargetCapability::Reload,
    ]);

    pub const fn from_capability(capability: TargetCapability) -> Self {
        Self(1 << capability as u8)
    }

    pub const fn from_slice(capabilities: &[TargetCapability]) -> Self {
        let mut bits = 0;
        let mut index = 0;
        while index < capabilities.len() {
            bits |= 1 << capabilities[index] as u8;
            index += 1;
        }
        Self(bits)
    }

    pub const fn contains(self, capability: TargetCapability) -> bool {
        self.0 & Self::from_capability(capability).0 != 0
    }

    pub const fn contains_all(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuiltinId {
    Tasks,
    Compact,
    New,
    Help,
    Queue,
    Model,
    Theme,
    Mcp,
    Login,
    Cd,
    Btw,
    Yolo,
    Fast,
    Workflow,
    Exit,
    Reload,
    Trust,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompletionKey {
    Model,
    Theme,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinOperation {
    OpenTasks,
    Compact(Option<String>),
    ResetSession,
    ToggleHelp,
    FocusQueue,
    OpenModelPicker,
    SetModel {
        spec: Arc<str>,
    },
    SelectModel(ModelSelection),
    OpenThemePicker,
    SetTheme {
        name: Arc<str>,
    },
    OpenMcpPicker,
    OpenLoginPicker,
    ChangeDirectory {
        path: PathBuf,
    },
    QuickQuestion {
        question: Arc<str>,
        attachments: Arc<[CommandAttachment]>,
    },
    ToggleYolo,
    ToggleFast,
    ToggleWorkflow,
    Exit,
    Reload,
    Trust,
}

pub fn parse_model_selection(arguments: &str) -> Result<ModelSelection, crate::CommandError> {
    let invalid = |message: String| crate::CommandError::Producer(Arc::from(message));
    let mut selection = ModelSelection::default();
    // Quotes and escapes make a token literal; option values may still be quoted.
    let tokens = crate::lex_strict(arguments).map_err(|error| invalid(error.to_string()))?;
    let mut tokens = tokens.into_iter();
    while let Some(token) = tokens.next() {
        let mut end = token.range.end;
        let token_raw = &arguments[token.range.clone()];
        let literal = token_raw.starts_with(['\'', '"', '\\']);
        if literal && token_raw.contains(['{', '}']) {
            if token_raw.starts_with('\\') {
                return Err(invalid(MIXED_QUOTED_MODEL_SYNTAX.into()));
            }
            let quote = token_raw.chars().next().unwrap();
            let mut escaped = false;
            let close = token_raw.char_indices().skip(1).find_map(|(index, ch)| {
                if escaped {
                    escaped = false;
                    None
                } else if ch == '\\' {
                    escaped = true;
                    None
                } else {
                    (ch == quote).then_some(index)
                }
            });
            if close.is_none_or(|close| close + quote.len_utf8() != token_raw.len()) {
                return Err(invalid(MIXED_QUOTED_MODEL_SYNTAX.into()));
            }
        }
        if !literal && arguments[token.range.clone()].contains('{') {
            while !arguments[token.range.start..end].ends_with('}') {
                let Some(next) = tokens.next() else {
                    return Err(invalid(MALFORMED_MODEL_OPTIONS.into()));
                };
                end = next.range.end;
            }
        }
        let raw = &arguments[token.range.start..end];
        if raw.starts_with(['\'', '"', '\\']) {
            selection
                .set_spec(token.value)
                .map_err(|error| invalid(error.to_string()))?;
        } else if let Some(open) = raw.find('{') {
            if !raw.ends_with('}') || raw[..open].contains('}') {
                return Err(invalid(MALFORMED_MODEL_OPTIONS.into()));
            }
            let spec = &raw[..open];
            if spec.contains(['\'', '"', '\\']) {
                return Err(invalid(MIXED_QUOTED_MODEL_SYNTAX.into()));
            }
            let options = &raw[open + 1..raw.len() - 1];
            if options.is_empty() || options.contains(['{', '}']) {
                return Err(invalid(MALFORMED_MODEL_OPTIONS.into()));
            }
            selection
                .set_spec(spec)
                .map_err(|error| invalid(error.to_string()))?;
            for option in options.split(',') {
                let tokens =
                    crate::lex_strict(option).map_err(|error| invalid(error.to_string()))?;
                let [token] = tokens.as_slice() else {
                    return Err(invalid(MALFORMED_MODEL_OPTIONS.into()));
                };
                let Some((key, value)) = token.value.split_once('=') else {
                    return Err(invalid(MODEL_OPTION_KEY_VALUE.into()));
                };
                selection
                    .set_option(key, value)
                    .map_err(|error| invalid(error.to_string()))?;
            }
        } else if raw.contains('}') {
            return Err(invalid(MALFORMED_MODEL_OPTIONS.into()));
        } else if let Some((key, value)) = token.value.split_once('=') {
            selection
                .set_option(key, value)
                .map_err(|error| invalid(error.to_string()))?;
        } else {
            selection
                .set_spec(token.value)
                .map_err(|error| invalid(error.to_string()))?;
        }
    }
    Ok(selection)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextRequest {
    ModelSpecs,
    ThemeNames,
    WorkingDirectory,
    FastModeSupported,
    SessionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextResponse {
    Values(Arc<[Arc<str>]>),
    WorkingDirectory(PathBuf),
    FastModeSupported(bool),
    SessionId(Arc<str>),
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticArgumentCompletion {
    pub key: CompletionKey,
    pub policy: CompletionPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinDefinition {
    pub id: BuiltinId,
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub description: &'static str,
    pub arguments: StaticCommandArguments,
    pub argument_completions: &'static [Option<StaticArgumentCompletion>],
    pub argument_hint: Option<&'static str>,
    pub required_capabilities: TargetCapabilities,
}

const INTERACTIVE: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::InteractiveUi);
const COMPACTION: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::HistoryCompaction);
const SESSION_REPLACEMENT: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::SessionReplacement);
const MODEL: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::ModelSelection);
const CWD: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::WorkingDirectory);
const AGENT: TargetCapabilities = TargetCapabilities::from_capability(TargetCapability::AgentTurns);
const PERMISSIONS: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::PermissionToggles);
const CONFIG: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::ConfigToggles);
const LIFECYCLE: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::ApplicationLifecycle);
const RELOAD: TargetCapabilities = TargetCapabilities::from_capability(TargetCapability::Reload);

macro_rules! builtin {
    ($id:ident, $name:expr, $aliases:expr, $description:expr, typed $arguments:expr, $completions:expr, $hint:expr, $caps:expr $(,)?) => {
        BuiltinDefinition {
            id: BuiltinId::$id,
            name: $name,
            aliases: $aliases,
            description: $description,
            arguments: StaticCommandArguments::Positional($arguments),
            argument_completions: $completions,
            argument_hint: $hint,
            required_capabilities: $caps,
        }
    };
    ($id:ident, $name:expr, $aliases:expr, $description:expr, raw $required:expr, $hint:expr, $caps:expr $(,)?) => {
        BuiltinDefinition {
            id: BuiltinId::$id,
            name: $name,
            aliases: $aliases,
            description: $description,
            arguments: StaticCommandArguments::Raw {
                required: $required,
            },
            argument_completions: &[],
            argument_hint: $hint,
            required_capabilities: $caps,
        }
    };
}

const CD_ARGUMENTS: &[StaticPositionalArgument] = &[StaticPositionalArgument {
    name: "path",
    kind: StaticArgumentKind::Directory,
    optional: true,
    variadic: false,
}];
const MODEL_ARGUMENTS: &[StaticPositionalArgument] = &[StaticPositionalArgument {
    name: "model",
    kind: StaticArgumentKind::String,
    optional: true,
    variadic: true,
}];
const THEME_ARGUMENTS: &[StaticPositionalArgument] = &[StaticPositionalArgument {
    name: "theme",
    kind: StaticArgumentKind::String,
    optional: true,
    variadic: false,
}];
const MODEL_COMPLETIONS: &[Option<StaticArgumentCompletion>] = &[Some(StaticArgumentCompletion {
    key: CompletionKey::Model,
    policy: CompletionPolicy::Replace,
})];
const THEME_COMPLETIONS: &[Option<StaticArgumentCompletion>] = &[Some(StaticArgumentCompletion {
    key: CompletionKey::Theme,
    policy: CompletionPolicy::Replace,
})];
const NO_ARGUMENT_COMPLETIONS: &[Option<StaticArgumentCompletion>] = &[];
const CD_COMPLETIONS: &[Option<StaticArgumentCompletion>] = &[None];

pub const BUILTIN_COMMANDS: &[BuiltinDefinition] = &[
    builtin!(
        Tasks,
        "/tasks",
        &[],
        "Browse and search tasks",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        INTERACTIVE,
    ),
    builtin!(
        Compact,
        COMPACT_COMMAND_NAME,
        &[],
        "Summarize and compact conversation history",
        raw false,
        Some("<instructions>"),
        COMPACTION,
    ),
    builtin!(
        New,
        "/new",
        &["/clear"],
        "Start a new session",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        SESSION_REPLACEMENT,
    ),
    builtin!(
        Help,
        "/help",
        &[],
        "Show keybindings",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        INTERACTIVE,
    ),
    builtin!(
        Queue,
        "/queue",
        &[],
        "Remove items from queue",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        INTERACTIVE,
    ),
    builtin!(
        Model,
        MODEL_COMMAND_NAME,
        &[],
        "Switch model",
        typed MODEL_ARGUMENTS,
        MODEL_COMPLETIONS,
        Some("[spec] [thinking=value] [fast=true|false]"),
        MODEL,
    ),
    builtin!(
        Theme,
        "/theme",
        &[],
        "Switch color theme",
        typed THEME_ARGUMENTS,
        THEME_COMPLETIONS,
        Some("<theme>"),
        INTERACTIVE,
    ),
    builtin!(
        Mcp,
        "/mcp",
        &[],
        "Configure MCP servers",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        INTERACTIVE,
    ),
    builtin!(
        Login,
        "/login",
        &[],
        "Authenticate with an LLM provider",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        INTERACTIVE,
    ),
    builtin!(
        Cd,
        "/cd",
        &[],
        "Change working directory. Quote paths containing spaces.",
        typed CD_ARGUMENTS,
        CD_COMPLETIONS,
        None,
        CWD,
    ),
    builtin!(
        Btw,
        "/btw",
        &[],
        "Ask a quick question (no tools, no history pollution)",
        raw true,
        Some("<question>"),
        AGENT,
    ),
    builtin!(
        Yolo,
        "/yolo",
        &[],
        "Toggle YOLO mode (skip all permission prompts)",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        PERMISSIONS,
    ),
    builtin!(
        Fast,
        "/fast",
        &[],
        "Toggle fast mode (Anthropic Opus or Codex subscription models)",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        CONFIG,
    ),
    builtin!(
        Workflow,
        "/workflow",
        &[],
        "Toggle workflow mode (task tools callable inside code_execution)",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        CONFIG,
    ),
    builtin!(
        Exit,
        "/exit",
        &[],
        "Exit the application",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        LIFECYCLE,
    ),
    builtin!(
        Reload,
        "/reload",
        &[],
        "Reload plugins and config",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        RELOAD,
    ),
    builtin!(
        Trust,
        "/trust",
        &[],
        "Trust this folder and load its shared project config",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        RELOAD,
    ),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: Arc<str>,
    pub aliases: Arc<[Arc<str>]>,
    pub arguments: CommandArguments,
    pub docs: CommandDocs,
    pub required_capabilities: TargetCapabilities,
}

impl CommandSpec {
    pub fn argument_hint(&self) -> Option<Arc<str>> {
        self.docs
            .argument_hint
            .clone()
            .or_else(|| self.arguments.usage_hint())
    }
}

impl BuiltinDefinition {
    pub fn spec(&self) -> CommandSpec {
        let arguments = match self.arguments {
            StaticCommandArguments::Raw { required } => CommandArguments::Raw { required },
            StaticCommandArguments::Positional(arguments) => {
                let mut arguments: Vec<crate::arguments::PositionalArgument> =
                    arguments.iter().copied().map(Into::into).collect();
                for (argument, completion) in arguments.iter_mut().zip(self.argument_completions) {
                    if let Some(completion) = completion {
                        argument.completion = completion.policy;
                    }
                }
                CommandArguments::Positional(arguments.into())
            }
        };
        CommandSpec {
            name: Arc::from(self.name),
            aliases: self.aliases.iter().copied().map(Arc::from).collect(),
            arguments,
            docs: CommandDocs {
                summary: Arc::from(self.description),
                argument_hint: self.argument_hint.map(Arc::from),
            },
            required_capabilities: self.required_capabilities,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandDocs {
    pub summary: Arc<str>,
    pub argument_hint: Option<Arc<str>>,
}

pub struct Registration {
    pub spec: CommandSpec,
    pub behavior: Arc<dyn CommandBehavior>,
    pub argument_completions: Vec<Option<Arc<dyn CommandCompletion>>>,
}

impl Registration {
    pub fn with_argument_completion(
        mut self,
        name: impl Into<Arc<str>>,
        provider: Arc<dyn CommandCompletion>,
    ) -> Self {
        let name = name.into();
        if let Some(arguments) = self.spec.arguments.positional()
            && let Some(index) = arguments.iter().position(|argument| argument.name == name)
        {
            if self.argument_completions.len() != arguments.len() {
                self.argument_completions
                    .resize_with(arguments.len(), || None);
            }
            self.argument_completions[index] = Some(provider);
        }
        self
    }
}

impl fmt::Debug for Registration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registration")
            .field("spec", &self.spec)
            .field("behavior", &"dyn CommandBehavior")
            .field(
                "argument_completions",
                &self
                    .argument_completions
                    .iter()
                    .map(|provider| provider.as_ref().map(|_| "dyn CommandCompletion"))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(pub(super) RegistryId, u64);

        impl $name {
            pub(crate) const fn new(registry_id: RegistryId, value: u64) -> Self {
                Self(registry_id, value)
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct RegistryId(pub(super) u64);

opaque_id!(ProducerId);
opaque_id!(CommandId);
opaque_id!(CompletionSessionId);
opaque_id!(InvocationTargetId);
