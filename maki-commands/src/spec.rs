use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

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
    Usage,
    Queue,
    Model,
    Theme,
    Mcp,
    Login,
    Cd,
    Btw,
    Yolo,
    Thinking,
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
    ToggleUsage,
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
    SetThinking {
        config: ThinkingConfig,
    },
    ToggleFast,
    ToggleWorkflow,
    Exit,
    Reload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingConfig {
    Off,
    Adaptive,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
    Budget(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextRequest {
    ModelSpecs,
    ThemeNames,
    WorkingDirectory,
    ThinkingConfig,
    FastModeSupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostContextResponse {
    Values(Arc<[Arc<str>]>),
    WorkingDirectory(PathBuf),
    ThinkingConfig(ThinkingConfig),
    FastModeSupported(bool),
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinDefinition {
    pub id: BuiltinId,
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub description: &'static str,
    pub arguments: ArgumentArity,
    pub argument_hint: Option<&'static str>,
    pub required_capabilities: TargetCapabilities,
    pub completion: Option<CompletionKey>,
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
    ($id:ident, $name:expr, $aliases:expr, $description:literal, $arguments:expr, $hint:expr, $caps:expr, $completion:expr $(,)?) => {
        BuiltinDefinition {
            id: BuiltinId::$id,
            name: $name,
            aliases: $aliases,
            description: $description,
            arguments: $arguments,
            argument_hint: $hint,
            required_capabilities: $caps,
            completion: $completion,
        }
    };
}

pub const BUILTIN_COMMANDS: &[BuiltinDefinition] = &[
    builtin!(
        Tasks,
        "/tasks",
        &[],
        "Browse and search tasks",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Compact,
        COMPACT_COMMAND_NAME,
        &[],
        "Summarize and compact conversation history",
        ArgumentArity::NONE,
        None,
        SESSION,
        None,
    ),
    builtin!(
        New,
        "/new",
        &["/clear"],
        "Start a new session",
        ArgumentArity::NONE,
        None,
        SESSION,
        None,
    ),
    builtin!(
        Help,
        "/help",
        &[],
        "Show keybindings",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Usage,
        "/usage",
        &[],
        "Show token usage breakdown",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Queue,
        "/queue",
        &[],
        "Remove items from queue",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Model,
        "/model",
        &[],
        "Switch model",
        ArgumentArity::OPTIONAL,
        Some("<model>"),
        MODEL,
        Some(CompletionKey::Model),
    ),
    builtin!(
        Theme,
        "/theme",
        &[],
        "Switch color theme",
        ArgumentArity::OPTIONAL,
        Some("<theme>"),
        INTERACTIVE,
        Some(CompletionKey::Theme),
    ),
    builtin!(
        Mcp,
        "/mcp",
        &[],
        "Configure MCP servers",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Login,
        "/login",
        &[],
        "Authenticate with an LLM provider",
        ArgumentArity::NONE,
        None,
        INTERACTIVE,
        None,
    ),
    builtin!(
        Cd,
        "/cd",
        &[],
        "Change working directory",
        ArgumentArity::OPTIONAL,
        Some("<path>"),
        CWD,
        None,
    ),
    builtin!(
        Btw,
        "/btw",
        &[],
        "Ask a quick question (no tools, no history pollution)",
        ArgumentArity::ONE_OR_MORE,
        Some("<question>"),
        AGENT,
        None,
    ),
    builtin!(
        Yolo,
        "/yolo",
        &[],
        "Toggle YOLO mode (skip all permission prompts)",
        ArgumentArity::NONE,
        None,
        PERMISSIONS,
        None,
    ),
    builtin!(
        Thinking,
        "/thinking",
        &[],
        "Toggle extended thinking (off, adaptive, effort level, or budget)",
        ArgumentArity::OPTIONAL,
        Some("<mode>"),
        INTERACTIVE,
        None,
    ),
    builtin!(
        Fast,
        "/fast",
        &[],
        "Toggle Anthropic fast mode (Opus only)",
        ArgumentArity::NONE,
        None,
        CONFIG,
        None,
    ),
    builtin!(
        Workflow,
        "/workflow",
        &[],
        "Toggle workflow mode (task callable inside code_execution)",
        ArgumentArity::NONE,
        None,
        CONFIG,
        None,
    ),
    builtin!(
        Exit,
        "/exit",
        &[],
        "Exit the application",
        ArgumentArity::NONE,
        None,
        LIFECYCLE,
        None,
    ),
    builtin!(
        Reload,
        "/reload",
        &[],
        "Reload plugins and config",
        ArgumentArity::NONE,
        None,
        RELOAD,
        None,
    ),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub name: Arc<str>,
    pub aliases: Arc<[Arc<str>]>,
    pub arguments: ArgumentArity,
    pub docs: CommandDocs,
    pub required_capabilities: TargetCapabilities,
}

impl BuiltinDefinition {
    pub fn spec(&self) -> CommandSpec {
        CommandSpec {
            name: Arc::from(self.name),
            aliases: self.aliases.iter().copied().map(Arc::from).collect(),
            arguments: self.arguments,
            docs: CommandDocs {
                summary: Arc::from(self.description),
                argument_hint: self.argument_hint.map(Arc::from),
            },
            required_capabilities: self.required_capabilities,
        }
    }
}

/// Argument count bounds. Arguments are counted by splitting the raw
/// remainder on whitespace, with no shell-like quoting: `/cd "my dir"` counts
/// as two arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgumentArity {
    pub min: usize,
    pub max: Option<usize>,
}

impl ArgumentArity {
    pub const NONE: Self = Self::exactly(0);
    pub const OPTIONAL: Self = Self::bounded(0, 1);
    pub const ONE: Self = Self::exactly(1);
    pub const ANY: Self = Self::unbounded(0);
    pub const ONE_OR_MORE: Self = Self::unbounded(1);

    pub const fn exactly(count: usize) -> Self {
        Self {
            min: count,
            max: Some(count),
        }
    }

    pub const fn bounded(min: usize, max: usize) -> Self {
        Self {
            min,
            max: Some(max),
        }
    }

    pub const fn unbounded(min: usize) -> Self {
        Self { min, max: None }
    }

    pub fn accepts(self, count: usize) -> bool {
        count >= self.min && self.max.is_none_or(|max| count <= max)
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
    pub completion: Option<Arc<dyn CommandCompletion>>,
}

impl fmt::Debug for Registration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Registration")
            .field("spec", &self.spec)
            .field("behavior", &"dyn CommandBehavior")
            .field(
                "completion",
                &self.completion.as_ref().map(|_| "dyn CommandCompletion"),
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
