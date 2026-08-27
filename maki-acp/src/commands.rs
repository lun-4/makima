use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use maki_agent::{
    McpPromptRequest, McpPromptSink,
    command::{self, PromptSink},
};
use maki_commands::{
    BUILTIN_COMMANDS, CommandBehavior, CommandClassification, CommandError, CommandFuture,
    CommandInvocation, CommandRegistry, InvocationLifecycle, InvocationTargetId, Producer,
    ProducerId, ProducerPrecedence, Registration,
};

#[derive(Clone)]
pub enum CommandRoute {
    Prompt(String),
    Mcp(McpPromptRequest),
    Model(String),
    Builtin { name: Arc<str>, arguments: String },
}

pub struct RoutedCommand {
    pub target: InvocationTargetId,
    pub route: CommandRoute,
    pub lifecycle: InvocationLifecycle,
}

#[derive(Clone)]
pub struct CommandDispatcher {
    registry: CommandRegistry,
    mailboxes: Arc<Mutex<HashMap<InvocationTargetId, flume::Sender<RoutedCommand>>>>,
    builtin_producer: ProducerId,
}

pub struct CommandMailbox {
    target: InvocationTargetId,
    rx: flume::Receiver<RoutedCommand>,
    mailboxes: Arc<Mutex<HashMap<InvocationTargetId, flume::Sender<RoutedCommand>>>>,
}

impl CommandMailbox {
    pub fn target(&self) -> InvocationTargetId {
        self.target
    }

    pub fn receiver(&self) -> &flume::Receiver<RoutedCommand> {
        &self.rx
    }
}

impl Drop for CommandMailbox {
    fn drop(&mut self) {
        self.mailboxes.lock().unwrap().remove(&self.target);
    }
}

impl CommandDispatcher {
    pub fn new(registry: CommandRegistry, custom_commands: &[command::CustomCommand]) -> Self {
        let builtin = registry.create_producer(ProducerPrecedence::Builtin);
        let dispatcher = Self {
            registry,
            mailboxes: Arc::default(),
            builtin_producer: builtin.id(),
        };
        dispatcher.register_builtins(&builtin);
        let producer = dispatcher
            .registry
            .create_producer(ProducerPrecedence::Application);
        command::register_commands(&producer, custom_commands, Arc::new(dispatcher.clone()))
            .expect("custom command metadata is valid");
        dispatcher
    }

    pub fn registry(&self) -> CommandRegistry {
        self.registry.clone()
    }

    pub fn create_mailbox(&self) -> CommandMailbox {
        let target = self.registry.create_target();
        let (tx, rx) = flume::unbounded();
        self.mailboxes.lock().unwrap().insert(target, tx);
        CommandMailbox {
            target,
            rx,
            mailboxes: Arc::clone(&self.mailboxes),
        }
    }

    pub fn projection(&self) -> maki_commands::RegistrySnapshot {
        self.registry.snapshot()
    }

    pub fn builtin_producer(&self) -> ProducerId {
        self.builtin_producer
    }

    pub fn mcp_sink(&self) -> Arc<dyn McpPromptSink> {
        Arc::new(self.clone())
    }

    fn register_builtins(&self, producer: &Producer) {
        producer
            .replace(
                BUILTIN_COMMANDS
                    .iter()
                    .map(|command| {
                        let name: Arc<str> = Arc::from(command.name);
                        let route = if command.name == "/model" {
                            BuiltinRoute::Model
                        } else {
                            BuiltinRoute::Builtin(Arc::clone(&name))
                        };
                        Registration {
                            spec: command.spec(),
                            behavior: Arc::new(BuiltinBehavior {
                                route,
                                mailboxes: Arc::clone(&self.mailboxes),
                            }),
                            completion: None,
                        }
                    })
                    .collect(),
            )
            .expect("static builtin registrations are valid");
    }
}

#[derive(Clone)]
enum BuiltinRoute {
    Model,
    Builtin(Arc<str>),
}

struct BuiltinBehavior {
    route: BuiltinRoute,
    mailboxes: Arc<Mutex<HashMap<InvocationTargetId, flume::Sender<RoutedCommand>>>>,
}

impl CommandBehavior for BuiltinBehavior {
    fn execute(&self, invocation: CommandInvocation) -> CommandFuture<Result<(), CommandError>> {
        let route = match &self.route {
            BuiltinRoute::Model => CommandRoute::Model(invocation.arguments.to_string()),
            BuiltinRoute::Builtin(name) => CommandRoute::Builtin {
                name: Arc::clone(name),
                arguments: invocation.arguments.to_string(),
            },
        };
        route_command(&self.mailboxes, invocation, route)
    }
}

impl PromptSink for CommandDispatcher {
    fn submit(
        &self,
        prompt: String,
        invocation: CommandInvocation,
    ) -> CommandFuture<Result<(), CommandError>> {
        route_agent_turn(&self.mailboxes, invocation, CommandRoute::Prompt(prompt))
    }
}

impl McpPromptSink for CommandDispatcher {
    fn submit(
        &self,
        invocation: CommandInvocation,
        prompt: McpPromptRequest,
    ) -> CommandFuture<Result<(), CommandError>> {
        route_agent_turn(&self.mailboxes, invocation, CommandRoute::Mcp(prompt))
    }
}

fn route_command(
    mailboxes: &Arc<Mutex<HashMap<InvocationTargetId, flume::Sender<RoutedCommand>>>>,
    invocation: CommandInvocation,
    route: CommandRoute,
) -> CommandFuture<Result<(), CommandError>> {
    let tx = mailboxes
        .lock()
        .unwrap()
        .get(&invocation.target_id)
        .cloned();
    Box::pin(async move {
        tx.ok_or(CommandError::StaleTarget)?
            .send(RoutedCommand {
                target: invocation.target_id,
                route,
                lifecycle: invocation.lifecycle,
            })
            .map_err(|_| CommandError::StaleTarget)
    })
}

/// Routes a command whose effect is an agent turn and classifies the
/// invocation synchronously, so a frontend awaiting the classification never
/// observes a premature terminal state while the producing handler runs on.
fn route_agent_turn(
    mailboxes: &Arc<Mutex<HashMap<InvocationTargetId, flume::Sender<RoutedCommand>>>>,
    invocation: CommandInvocation,
    route: CommandRoute,
) -> CommandFuture<Result<(), CommandError>> {
    let lifecycle = invocation.lifecycle.clone();
    let routed = route_command(mailboxes, invocation, route);
    Box::pin(async move {
        routed.await?;
        lifecycle.transition(CommandClassification::AgentTurnAccepted);
        Ok(())
    })
}

pub fn complete(command: &RoutedCommand, classification: CommandClassification) {
    command.lifecycle.transition(classification);
}

#[cfg(test)]
mod tests {
    use maki_commands::InputDispatch;

    use super::*;

    #[test]
    fn concurrent_targets_receive_only_their_commands_and_stale_target_fails() {
        let dispatcher = CommandDispatcher::new(CommandRegistry::new(), &[]);
        let first = dispatcher.create_mailbox();
        let second = dispatcher.create_mailbox();
        let registry = dispatcher.registry();

        let InputDispatch::Dispatched(first_dispatch) =
            smol::block_on(registry.dispatch_input("/model first", 0, first.target())).unwrap()
        else {
            panic!("known command did not dispatch");
        };
        let InputDispatch::Dispatched(second_dispatch) =
            smol::block_on(registry.dispatch_input("/model second", 0, second.target())).unwrap()
        else {
            panic!("known command did not dispatch");
        };
        let first_command = first.receiver().recv().unwrap();
        let second_command = second.receiver().recv().unwrap();
        assert!(matches!(first_command.route, CommandRoute::Model(ref value) if value == "first"));
        assert!(
            matches!(second_command.route, CommandRoute::Model(ref value) if value == "second")
        );
        assert!(first.receiver().is_empty());
        assert!(second.receiver().is_empty());
        complete(&first_command, CommandClassification::Completed);
        complete(&second_command, CommandClassification::Completed);
        assert_eq!(
            smol::block_on(first_dispatch.classification()),
            CommandClassification::Completed
        );
        assert_eq!(
            smol::block_on(second_dispatch.classification()),
            CommandClassification::Completed
        );

        let stale_target = first.target();
        drop(first);
        let error =
            smol::block_on(registry.dispatch_input("/model stale", 0, stale_target)).unwrap_err();
        assert_eq!(error, CommandError::StaleTarget);
        assert!(second.receiver().is_empty());
    }

    #[test]
    fn prompt_submit_classifies_agent_turn_before_the_handler_finishes() {
        let dispatcher = CommandDispatcher::new(CommandRegistry::new(), &[]);
        let mailbox = dispatcher.create_mailbox();
        let producer = dispatcher
            .registry()
            .create_producer(ProducerPrecedence::Application);
        command::register_commands(
            &producer,
            &[command::CustomCommand {
                name: "greet".into(),
                description: "greets".into(),
                content: "hello $ARGUMENTS".into(),
                scope: maki_agent::command::CommandScope::Project,
                accepts_args: true,
            }],
            Arc::new(dispatcher.clone()) as Arc<dyn command::PromptSink>,
        )
        .unwrap();

        let InputDispatch::Dispatched(dispatch) = smol::block_on(
            dispatcher
                .registry()
                .dispatch_input("/project:greet world", 0, mailbox.target()),
        )
        .unwrap() else {
            panic!("custom command did not dispatch");
        };

        // The sink classifies the invocation synchronously, so a producing
        // handler that keeps running cannot be mistaken for a completed
        // command, and the routed prompt is already queued.
        assert_eq!(
            smol::block_on(futures_lite::future::poll_once(dispatch.classification())).unwrap(),
            CommandClassification::AgentTurnAccepted
        );
        let routed = smol::block_on(mailbox.receiver().recv_async()).unwrap();
        let CommandRoute::Prompt(prompt) = routed.route else {
            panic!("expected a prompt route");
        };
        assert_eq!(prompt, "hello world");
    }
}
