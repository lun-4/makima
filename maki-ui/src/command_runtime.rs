use std::sync::Arc;

use crate::components::arg_completion::{ModelArgSource, ThemeArgSource};
use maki_agent::command::{self, StandardCommands, StandardCompletions};
use maki_commands::{
    CommandContent, CommandError, CommandFuture, CommandHost, CommandOutcome, CommandRegistry,
    HostRequest, HostResponse, ResolvedCommand, TargetCapabilities, TargetHandle,
};

pub(crate) enum CommandEvent {
    Host {
        target: maki_commands::InvocationTargetId,
        request: HostRequest,
        reply: flume::Sender<Result<HostResponse, CommandError>>,
    },
    Outcome {
        target: maki_commands::InvocationTargetId,
        outcome: CommandOutcome,
    },
}

struct UiCommandHost {
    target: std::sync::OnceLock<maki_commands::InvocationTargetId>,
    tx: flume::Sender<CommandEvent>,
}

impl CommandHost for UiCommandHost {
    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        let Some(target) = self.target.get().copied() else {
            return Box::pin(async { Err(CommandError::StaleTarget) });
        };
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = flume::bounded(1);
            tx.send_async(CommandEvent::Host {
                target,
                request,
                reply,
            })
            .await
            .map_err(|_| CommandError::StaleTarget)?;
            rx.recv_async()
                .await
                .unwrap_or(Err(CommandError::StaleTarget))
        })
    }
}

pub(crate) struct CommandRuntime {
    pub registry: CommandRegistry,
    event_tx: flume::Sender<CommandEvent>,
    #[cfg(test)]
    event_rx: flume::Receiver<CommandEvent>,
    theme_completion: Arc<ThemeArgSource>,
    _standard_commands: StandardCommands,
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
    ) -> (Self, flume::Receiver<CommandEvent>) {
        let (event_tx, event_rx) = flume::unbounded();
        let standard_commands = StandardCommands::register(
            &registry,
            custom_commands,
            StandardCompletions {
                model: Some(model_completion),
                theme: Some(Arc::clone(&theme_completion) as Arc<_>),
            },
        )
        .expect("static and configured command metadata is valid");
        (
            Self {
                registry,
                event_tx,
                #[cfg(test)]
                event_rx: event_rx.clone(),
                theme_completion,
                _standard_commands: standard_commands,
            },
            event_rx,
        )
    }

    pub(crate) fn bind_target(&self) -> TargetHandle {
        let host = Arc::new(UiCommandHost {
            target: std::sync::OnceLock::new(),
            tx: self.event_tx.clone(),
        });
        let target = self
            .registry
            .bind_target(TargetCapabilities::ALL, host.clone());
        host.target
            .set(target.id())
            .expect("new command host target is unset");
        target
    }

    pub(crate) fn dispatch_command(
        &self,
        target: &TargetHandle,
        command: ResolvedCommand,
        arguments: Arc<str>,
        content: CommandContent,
        depth: usize,
    ) {
        let future = self
            .registry
            .dispatch_command_with_depth(target, command, arguments, content, depth);
        self.send_outcome(target.id(), future);
    }

    fn send_outcome(
        &self,
        target: maki_commands::InvocationTargetId,
        future: CommandFuture<CommandOutcome>,
    ) {
        let tx = self.event_tx.clone();
        smol::spawn(async move {
            let outcome = future.await;
            let _ = tx
                .send_async(CommandEvent::Outcome { target, outcome })
                .await;
        })
        .detach();
    }

    #[cfg(test)]
    pub(crate) fn recv_for_test(&self) -> Option<CommandEvent> {
        self.event_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .ok()
    }

    pub(crate) fn finish_theme_preview(
        &self,
        target: maki_commands::InvocationTargetId,
        commit: bool,
    ) {
        self.theme_completion.finish(target, commit);
    }
}
