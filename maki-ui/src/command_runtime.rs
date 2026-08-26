use std::sync::Arc;

use crate::components::arg_completion::{ModelArgSource, ThemeArgSource};
use maki_agent::{
    McpPromptRequest, McpPromptSink,
    command::{self, PromptSink},
};
use maki_commands::{
    ArgumentArity, BUILTIN_COMMANDS, CommandBehavior, CommandClassification, CommandCompletion,
    CommandDocs, CommandError, CommandFuture, CommandInvocation, CommandRegistry, CommandSpec,
    InvocationLifecycle, InvocationTargetId, ProducerPrecedence, Registration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuiltinRoute {
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

impl BuiltinRoute {
    fn from_name(name: &str) -> Self {
        match name {
            "/tasks" => Self::Tasks,
            "/compact" => Self::Compact,
            "/new" => Self::New,
            "/help" => Self::Help,
            "/usage" => Self::Usage,
            "/queue" => Self::Queue,
            "/model" => Self::Model,
            "/theme" => Self::Theme,
            "/mcp" => Self::Mcp,
            "/login" => Self::Login,
            "/cd" => Self::Cd,
            "/btw" => Self::Btw,
            "/yolo" => Self::Yolo,
            "/thinking" => Self::Thinking,
            "/fast" => Self::Fast,
            "/workflow" => Self::Workflow,
            "/exit" => Self::Exit,
            "/reload" => Self::Reload,
            _ => unreachable!("builtin metadata and route enum diverged"),
        }
    }
}

#[derive(Clone)]
pub(crate) enum CommandRoute {
    Builtin(BuiltinRoute),
    Prompt(String),
    Mcp(McpPromptRequest),
}

pub(crate) struct RoutedCommand {
    pub target: InvocationTargetId,
    pub route: CommandRoute,
    pub arguments: String,
    pub depth: usize,
    pub lifecycle: InvocationLifecycle,
}

#[derive(Clone)]
pub(crate) struct McpPromptRouteSink {
    command_tx: flume::Sender<RoutedCommand>,
}

impl McpPromptSink for McpPromptRouteSink {
    fn submit(
        &self,
        invocation: CommandInvocation,
        prompt: McpPromptRequest,
    ) -> CommandFuture<Result<(), CommandError>> {
        let result = self.command_tx.send(RoutedCommand {
            target: invocation.target_id,
            route: CommandRoute::Mcp(prompt),
            arguments: invocation.arguments.to_string(),
            depth: invocation.depth,
            lifecycle: invocation.lifecycle,
        });
        Box::pin(async move { result.map_err(|_| CommandError::StaleTarget) })
    }
}

#[derive(Clone)]
struct PromptRouteSink {
    command_tx: flume::Sender<RoutedCommand>,
}

impl PromptSink for PromptRouteSink {
    fn submit(
        &self,
        prompt: String,
        invocation: CommandInvocation,
    ) -> CommandFuture<Result<(), CommandError>> {
        let result = self.command_tx.send(RoutedCommand {
            target: invocation.target_id,
            route: CommandRoute::Prompt(prompt),
            arguments: String::new(),
            depth: invocation.depth,
            lifecycle: invocation.lifecycle,
        });
        Box::pin(async move { result.map_err(|_| CommandError::StaleTarget) })
    }
}

struct RouteBehavior {
    route: CommandRoute,
    command_tx: flume::Sender<RoutedCommand>,
}

impl CommandBehavior for RouteBehavior {
    fn execute(&self, invocation: CommandInvocation) -> CommandFuture<Result<(), CommandError>> {
        let result = self.command_tx.send(RoutedCommand {
            target: invocation.target_id,
            route: self.route.clone(),
            arguments: invocation.arguments.to_string(),
            depth: invocation.depth,
            lifecycle: invocation.lifecycle,
        });
        Box::pin(async move { result.map_err(|_| CommandError::StaleTarget) })
    }
}

pub(crate) struct CommandRuntime {
    pub registry: CommandRegistry,
    command_tx: flume::Sender<RoutedCommand>,
    theme_completion: Arc<ThemeArgSource>,
    #[cfg(test)]
    command_rx: flume::Receiver<RoutedCommand>,
}

impl CommandRuntime {
    #[cfg(test)]
    pub fn new_for_test(
        custom_commands: &[command::CustomCommand],
        registry: CommandRegistry,
        model_completion: Arc<ModelArgSource>,
        theme_completion: Arc<ThemeArgSource>,
    ) -> Self {
        Self::new(
            custom_commands,
            registry,
            model_completion,
            theme_completion,
        )
        .0
    }

    pub fn new(
        custom_commands: &[command::CustomCommand],
        registry: CommandRegistry,
        model_completion: Arc<ModelArgSource>,
        theme_completion: Arc<ThemeArgSource>,
    ) -> (Self, flume::Receiver<RoutedCommand>, Arc<dyn McpPromptSink>) {
        let (command_tx, command_rx) = flume::unbounded();
        let builtin = registry.create_producer(ProducerPrecedence::Builtin);
        builtin
            .replace(
                BUILTIN_COMMANDS
                    .iter()
                    .map(|command| {
                        let route = CommandRoute::Builtin(BuiltinRoute::from_name(command.name));
                        Registration {
                            spec: CommandSpec {
                                name: Arc::from(command.name),
                                aliases: command.aliases.iter().copied().map(Arc::from).collect(),
                                arguments: if command.max_args == usize::MAX {
                                    ArgumentArity::unbounded(0)
                                } else {
                                    ArgumentArity::bounded(0, command.max_args)
                                },
                                docs: CommandDocs {
                                    summary: Arc::from(command.description),
                                    argument_hint: None,
                                },
                            },
                            behavior: Arc::new(RouteBehavior {
                                route: route.clone(),
                                command_tx: command_tx.clone(),
                            }),
                            completion: match route {
                                CommandRoute::Builtin(BuiltinRoute::Model) => {
                                    Some(Arc::clone(&model_completion) as Arc<dyn CommandCompletion>)
                                }
                                CommandRoute::Builtin(BuiltinRoute::Theme) => {
                                    Some(Arc::clone(&theme_completion) as Arc<dyn CommandCompletion>)
                                }
                                _ => None,
                            },
                        }
                    })
                    .collect(),
            )
            .expect("static builtin registrations are valid");
        let application = registry.create_producer(ProducerPrecedence::Application);
        command::register_commands(
            &application,
            custom_commands,
            Arc::new(PromptRouteSink {
                command_tx: command_tx.clone(),
            }),
        )
        .expect("custom command metadata is valid");
        let runtime = Self {
            registry,
            command_tx,
            theme_completion,
            #[cfg(test)]
            command_rx: command_rx.clone(),
        };
        let mcp_prompt_sink = Arc::new(McpPromptRouteSink {
            command_tx: runtime.command_tx.clone(),
        });
        (runtime, command_rx, mcp_prompt_sink)
    }

    #[cfg(test)]
    pub fn try_recv_for_test(&self) -> Option<RoutedCommand> {
        self.command_rx.try_recv().ok()
    }

    pub(crate) fn finish_theme_preview(&self, target: InvocationTargetId, commit: bool) {
        self.theme_completion.finish(target, commit);
    }
}

pub(crate) fn complete(command: &RoutedCommand, classification: CommandClassification) {
    command.lifecycle.transition(classification);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_swap::ArcSwapOption;
    use maki_commands::{BUILTIN_COMMANDS, CommandClassification, CommandError, InputDispatch};

    use super::{CommandRuntime, RoutedCommand};
    use crate::{
        components::arg_completion::{ModelArgSource, ThemeArgSource},
        theme::ThemesProvider,
    };

    fn runtime() -> (CommandRuntime, flume::Receiver<RoutedCommand>) {
        runtime_with_themes(Arc::new(crate::theme::InMemoryThemesProvider::bundled()))
    }

    fn runtime_with_themes(
        provider: Arc<crate::theme::InMemoryThemesProvider>,
    ) -> (CommandRuntime, flume::Receiver<RoutedCommand>) {
        let (runtime, rx, _) = CommandRuntime::new(
            &[],
            maki_commands::CommandRegistry::new(),
            Arc::new(ModelArgSource::new(Arc::new(ArcSwapOption::empty()))),
            Arc::new(ThemeArgSource::new(provider)),
        );
        (runtime, rx)
    }

    #[test]
    fn routed_command_preserves_target_and_terminal_lifecycle() {
        let (runtime, rx) = runtime();
        let target = runtime.registry.create_target();
        let InputDispatch::Dispatched(dispatch) =
            smol::block_on(runtime.registry.dispatch_input("/help", 0, target)).unwrap()
        else {
            panic!("expected dispatch");
        };
        let routed = rx.recv().unwrap();
        assert_eq!(routed.target, target);
        super::complete(&routed, CommandClassification::Completed);
        assert_eq!(
            smol::block_on(dispatch.classification()),
            CommandClassification::Completed
        );
    }

    fn theme_context(
        runtime: &CommandRuntime,
        target: maki_commands::InvocationTargetId,
        theme: &str,
    ) -> (
        maki_commands::CompletionSession,
        maki_commands::CompletionCandidate,
    ) {
        let session = runtime
            .registry
            .open_completion(runtime.registry.resolve("/theme").unwrap(), target)
            .unwrap();
        let maki_commands::CompletionResult::Items(mut items) = smol::block_on(session.complete(
            Arc::from(theme),
            Arc::from(theme),
            0,
            Arc::from("build"),
        )) else {
            panic!("expected theme items");
        };
        let candidate = items
            .drain(..)
            .find(|item| item.item().insertion.as_ref() == theme)
            .unwrap();
        (session, candidate)
    }

    #[test]
    fn overlapping_theme_previews_restore_deterministically() {
        let _guard = crate::theme::theme_test_guard();
        let provider = Arc::new(crate::theme::InMemoryThemesProvider::bundled());
        let baseline = provider.current_theme_name();
        let (runtime, _) = runtime_with_themes(Arc::clone(&provider));
        let first_target = runtime.registry.create_target();
        let second_target = runtime.registry.create_target();
        let (first, first_item) = theme_context(&runtime, first_target, "dracula");
        let (second, second_item) = theme_context(&runtime, second_target, "tokyonight");

        first.highlight(&first_item).unwrap();
        second.highlight(&second_item).unwrap();
        first.cancel().unwrap();
        assert_eq!(
            **crate::theme::current(),
            provider.load("tokyonight").unwrap()
        );
        second.cancel().unwrap();
        assert_eq!(**crate::theme::current(), provider.load(&baseline).unwrap());
    }

    #[test]
    fn dropped_target_is_classified_stale() {
        let (runtime, rx) = runtime();
        let target = runtime.registry.create_target();
        let InputDispatch::Dispatched(dispatch) =
            smol::block_on(runtime.registry.dispatch_input("/help", 0, target)).unwrap()
        else {
            panic!("expected dispatch");
        };
        let routed = rx.recv().unwrap();
        routed
            .lifecycle
            .transition(CommandClassification::Failed(CommandError::StaleTarget));
        assert_eq!(
            smol::block_on(dispatch.classification()),
            CommandClassification::Failed(CommandError::StaleTarget)
        );
    }
    /// Every built-in in the shared metadata must map to a TUI route: a
    /// missing arm here panics at startup instead of failing this test, so
    /// pin the pairing explicitly.
    #[test]
    fn every_builtin_command_maps_to_a_route() {
        for command in BUILTIN_COMMANDS {
            let _route = super::BuiltinRoute::from_name(command.name);
        }
        assert_eq!(BUILTIN_COMMANDS.len(), 18);
    }
}
