use maki_lua::orchestration::{self, CommandRunner, TrustedTarget};
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::components::arg_completion::{ModelArgSource, PathArgSource, ThemeArgSource};
use maki_agent::command::{self, StandardCommands, StandardCompletions};
use maki_commands::{
    CommandContent, CommandError, CommandFuture, CommandHost, CommandOutcome, CommandRegistry,
    HostRequest, HostResponse, PreparedTarget, ResolvedCommand, TargetCapabilities, TargetHandle,
};

pub(crate) const CLOSED_TARGET_ERR: &str = "the selected agent has closed; switch to a live chat";

pub(crate) enum CommandEvent {
    Host {
        target: maki_commands::InvocationTargetId,
        request: HostRequest,
        admission: Option<Box<TrustedTarget>>,
        reply: flume::Sender<Result<HostResponse, CommandError>>,
    },
    Outcome {
        target: maki_commands::InvocationTargetId,
        outcome: CommandOutcome,
        admission: Option<Box<TrustedTarget>>,
    },
}

struct UiCommandHost {
    target: std::sync::OnceLock<maki_commands::InvocationTargetId>,
    tx: flume::Sender<CommandEvent>,
    admissions: Arc<Mutex<HashMap<maki_commands::InvocationTargetId, TrustedTarget>>>,
}

pub(crate) struct PreparedCommandTarget {
    target: PreparedTarget,
    host: Arc<UiCommandHost>,
}

impl PreparedCommandTarget {
    pub(crate) fn handle(&self) -> &TargetHandle {
        self.target.handle()
    }

    pub(crate) fn snapshot(&self) -> maki_commands::RegistrySnapshot {
        self.target.snapshot()
    }

    pub(crate) fn activate(self) -> TargetHandle {
        let target = self.target.activate();
        self.host
            .target
            .set(target.id())
            .expect("new command host target is unset");
        target
    }
}

impl CommandHost for UiCommandHost {
    fn admission_context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        let target = self.target.get()?;
        self.admissions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(target)
            .cloned()
            .map(|target| Arc::new(target) as Arc<dyn Any + Send + Sync>)
    }
    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        self.request_with_admission(request, self.admission_context())
    }

    fn request_with_admission(
        &self,
        request: HostRequest,
        admission: Option<Arc<dyn Any + Send + Sync>>,
    ) -> CommandFuture<Result<HostResponse, CommandError>> {
        let admission = admission.and_then(|value| value.downcast_ref::<TrustedTarget>().cloned());
        let Some(target) = self.target.get().copied() else {
            return Box::pin(async { Err(CommandError::StaleTarget) });
        };
        let tx = self.tx.clone();
        Box::pin(async move {
            let (reply, rx) = flume::bounded(1);
            tx.send_async(CommandEvent::Host {
                target,
                request,
                admission: admission.map(Box::new),
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
    admissions: Arc<Mutex<HashMap<maki_commands::InvocationTargetId, TrustedTarget>>>,
    event_tx: flume::Sender<CommandEvent>,
    #[cfg(test)]
    event_rx: flume::Receiver<CommandEvent>,
    theme_completion: Arc<ThemeArgSource>,
    pub(crate) path_completion: Arc<PathArgSource>,
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
        let admissions = Arc::default();
        let model_completion = Arc::new(model_completion.with_targets(Arc::clone(&admissions)));
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
                admissions,
                event_tx,
                #[cfg(test)]
                event_rx: event_rx.clone(),
                path_completion: Arc::new(PathArgSource::new(maki_storage::paths::home())),
                theme_completion,
                _standard_commands: standard_commands,
            },
            event_rx,
        )
    }

    pub(crate) fn sync_admissions(
        &self,
        targets: HashMap<maki_commands::InvocationTargetId, TrustedTarget>,
    ) {
        *self
            .admissions
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = targets;
    }

    fn target_admission(&self, target: &TargetHandle) -> Option<TrustedTarget> {
        self.admissions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&target.id())
            .cloned()
    }

    pub(crate) fn target_agent(&self, target: &TargetHandle) -> Option<maki_agent::AgentRef> {
        self.target_admission(target)
            .map(|admission| admission.target)
    }

    pub(crate) fn prepare_target(&self) -> PreparedCommandTarget {
        let host = Arc::new(UiCommandHost {
            target: std::sync::OnceLock::new(),
            tx: self.event_tx.clone(),
            admissions: self.admissions.clone(),
        });
        PreparedCommandTarget {
            target: self
                .registry
                .prepare_target(TargetCapabilities::ALL, host.clone()),
            host,
        }
    }

    #[cfg(test)]
    pub(crate) fn bind_target(&self) -> TargetHandle {
        self.prepare_target().activate()
    }

    pub(crate) fn dispatch_command(
        &self,
        target: &TargetHandle,
        command: ResolvedCommand,
        arguments: Arc<str>,
        content: CommandContent,
        depth: usize,
    ) {
        let admission = self.target_admission(target);
        let future = if let Some(admission) = &admission {
            self.registry.dispatch_command_with_admission(
                target,
                command,
                arguments,
                content,
                depth,
                Arc::new(admission.clone()),
            )
        } else {
            self.registry
                .dispatch_command_with_depth(target, command, arguments, content, depth)
        };
        self.send_outcome(target.id(), admission, future);
    }

    /// The plugin command runner for {target}: the shared runner, whose
    /// unfinished outcomes reach the event loop like a typed command's.
    pub(crate) fn command_runner(&self, target: &TargetHandle) -> CommandRunner {
        let tx = self.event_tx.clone();
        let id = target.id();
        orchestration::command_runner(
            self.registry.clone(),
            target.clone(),
            Arc::new(move |_, outcome, admission| {
                let tx = tx.clone();
                Box::pin(async move {
                    tx.send_async(CommandEvent::Outcome {
                        target: id,
                        outcome,
                        admission: Some(Box::new(admission)),
                    })
                    .await
                    .map_err(|_| CLOSED_TARGET_ERR.to_owned())
                })
            }),
        )
    }

    pub(crate) fn dispatch_input(&self, target: &TargetHandle, content: CommandContent) {
        let admission = self.target_admission(target);
        let future = self.registry.dispatch_input(target, content);
        self.send_dispatched(target.id(), admission, future);
    }

    fn send_dispatched(
        &self,
        target: maki_commands::InvocationTargetId,
        admission: Option<TrustedTarget>,
        future: CommandFuture<maki_commands::InputDispatch>,
    ) {
        let tx = self.event_tx.clone();
        smol::spawn(async move {
            let maki_commands::InputDispatch::Dispatched(outcome) = future.await else {
                return;
            };
            let _ = tx
                .send_async(CommandEvent::Outcome {
                    target,
                    outcome,
                    admission: admission.map(Box::new),
                })
                .await;
        })
        .detach();
    }

    fn send_outcome(
        &self,
        target: maki_commands::InvocationTargetId,
        admission: Option<TrustedTarget>,
        future: CommandFuture<CommandOutcome>,
    ) {
        let tx = self.event_tx.clone();
        smol::spawn(async move {
            let outcome = future.await;
            let _ = tx
                .send_async(CommandEvent::Outcome {
                    target,
                    outcome,
                    admission: admission.map(Box::new),
                })
                .await;
        })
        .detach();
    }

    #[cfg(test)]
    pub(crate) fn recv_timeout_for_test(
        &self,
        timeout: std::time::Duration,
    ) -> Option<CommandEvent> {
        self.event_rx.recv_timeout(timeout).ok()
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

#[cfg(test)]
mod tests {
    use super::{
        Arc, CommandContent, CommandEvent, CommandOutcome, CommandRegistry, CommandRuntime,
        HashMap, ModelArgSource, ThemeArgSource, TrustedTarget, command,
    };
    use arc_swap::ArcSwapOption;
    use maki_agent::{
        ActorBackend, AgentInput, AgentManagerHandle, BackendResult, ControlWork, History,
        TurnContext, WorkKind,
    };
    use maki_lua::orchestration;
    use std::{future::Future, pin::Pin, time::Duration};
    use test_case::test_case;

    const COMMAND: &str = "/user:pinned";
    const CONTENT: &str = "captured command";

    struct IdleBackend;

    impl ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(std::future::pending())
        }

        fn run_control<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::ControlDone })
        }

        fn run_compact<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { BackendResult::CompactDone })
        }
    }

    fn pinned_runtime() -> CommandRuntime {
        CommandRuntime::new_for_test(
            &[command::CustomCommand {
                name: "pinned".into(),
                description: CONTENT.into(),
                content: CONTENT.into(),
                scope: command::CommandScope::User,
                accepts_args: false,
                argument_hint: None,
            }],
            CommandRegistry::new(),
            Arc::new(ModelArgSource::new(Arc::new(ArcSwapOption::empty()))),
            Arc::new(ThemeArgSource::new(Arc::new(
                crate::theme::InMemoryThemesProvider::bundled(),
            ))),
        )
    }

    #[test_case(false; "live_target")]
    #[test_case(true; "closed_target")]
    fn plugin_run_command_hands_its_turn_to_the_loop(close: bool) {
        let runtime = pinned_runtime();
        let handle = runtime.bind_target();
        let root = AgentManagerHandle::new(Default::default())
            .unwrap()
            .create_root(Vec::new(), None, Box::new(IdleBackend))
            .unwrap();
        if close {
            root.close_subtree().unwrap();
        }
        let admission = TrustedTarget {
            target: root.clone(),
            template: maki_agent::tools::cli_tool_ctx(std::env::current_dir().unwrap()),
        };
        let result = smol::block_on(runtime.command_runner(&handle)(
            COMMAND.into(),
            0,
            admission,
        ));
        if close {
            assert_eq!(result, Err(orchestration::CLOSED_TARGET_ERR.to_owned()));
            assert!(runtime.recv_timeout_for_test(Duration::ZERO).is_none());
            return;
        }
        assert_eq!(result, Ok(()));
        let Some(CommandEvent::Outcome {
            outcome: CommandOutcome::AgentTurn(turn),
            admission: Some(admission),
            ..
        }) = runtime.recv_timeout_for_test(Duration::ZERO)
        else {
            panic!("the command's turn must reach the loop");
        };
        assert_eq!(turn.content.text.as_ref(), CONTENT);
        assert_eq!(admission.target.id(), root.id());
        root.close_subtree().unwrap();
    }

    #[test_case(false, false; "input_focus_switch")]
    #[test_case(true, false; "resolved_focus_switch")]
    #[test_case(false, true; "input_closed_original")]
    #[test_case(true, true; "resolved_closed_original")]
    fn outcome_keeps_original_admission(resolved: bool, close: bool) {
        let runtime = pinned_runtime();
        let handle = runtime.bind_target();
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let first = manager
            .create_root(Vec::new(), None, Box::new(IdleBackend))
            .unwrap();
        let second = AgentManagerHandle::new(Default::default())
            .unwrap()
            .create_root(Vec::new(), None, Box::new(IdleBackend))
            .unwrap();
        let admission = TrustedTarget {
            target: first.clone(),
            template: maki_agent::tools::cli_tool_ctx(std::env::current_dir().unwrap()),
        };
        runtime.sync_admissions(HashMap::from([(handle.id(), admission)]));
        if resolved {
            let command = runtime.registry.resolve_for(&handle, COMMAND).unwrap();
            runtime.dispatch_command(
                &handle,
                command,
                Arc::from(""),
                CommandContent::default(),
                0,
            );
        } else {
            runtime.dispatch_input(&handle, COMMAND.into());
        }
        runtime.sync_admissions(HashMap::from([(
            handle.id(),
            TrustedTarget {
                target: second.clone(),
                template: maki_agent::tools::cli_tool_ctx(std::env::current_dir().unwrap()),
            },
        )]));
        if close {
            first.close_subtree().unwrap();
        }
        let CommandEvent::Outcome {
            outcome,
            admission: Some(admission),
            ..
        } = runtime.recv_for_test().unwrap()
        else {
            panic!("expected pinned outcome");
        };
        assert!(
            matches!(outcome, CommandOutcome::AgentTurn(turn) if turn.content.text.as_ref() == CONTENT)
        );
        assert_eq!(admission.target.id(), first.id());
        assert_ne!(admission.target.id(), second.id());
        assert_eq!(admission.target.validate_live().is_err(), close);
        assert!(second.actor().unwrap().latest_ticket().is_none());
        if !close {
            first.close_subtree().unwrap();
        }
        second.close_subtree().unwrap();
    }
}
