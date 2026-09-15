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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TargetCapability {
    AgentTurns,
    ModelSelection,
    SessionControl,
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
    pub const ALL: Self = Self((1 << 9) - 1);

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompletionKey {
    Model,
    Theme,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinOperation {
    OpenTasks,
    Compact,
    ResetSession,
    ToggleHelp,
    FocusQueue,
    OpenModelPicker,
    SetModel {
        spec: Arc<str>,
    },
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextRequest {
    ModelSpecs,
    ThemeNames,
    WorkingDirectory,
    FastModeSupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextResponse {
    Values(Arc<[Arc<str>]>),
    WorkingDirectory(PathBuf),
    FastModeSupported(bool),
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
const SESSION: TargetCapabilities =
    TargetCapabilities::from_capability(TargetCapability::SessionControl);
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
    variadic: false,
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
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        SESSION,
    ),
    builtin!(
        New,
        "/new",
        &["/clear"],
        "Start a new session",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        SESSION,
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
        "/model",
        &[],
        "Switch model",
        typed MODEL_ARGUMENTS,
        MODEL_COMPLETIONS,
        Some("<model>"),
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
        "Toggle Anthropic fast mode (Opus only)",
        typed & [],
        NO_ARGUMENT_COMPLETIONS,
        None,
        CONFIG,
    ),
    builtin!(
        Workflow,
        "/workflow",
        &[],
        "Toggle workflow mode (task callable inside code_execution)",
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
