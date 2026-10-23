use arc_swap::ArcSwapOption;
use maki_agent::config_change::ConfigChangeError;
use maki_agent::session_coordinator::ConfigMutation;
use maki_agent::{
    AgentRef,
    agent::LoadedInstructions,
    cancel::CancelToken,
    tools::{Deadline, ToolContext, TurnToolBindings},
};
use maki_commands::CommandInvocation;
use maki_providers::Message;
use maki_storage::checkpoint::CheckpointPublication;
use maki_storage::sessions::HistorySnapshot;
use std::sync::{Arc, Mutex, MutexGuard};

use super::{
    CommandRunner, ConfigMutationReserver, OrchestrationServices, TrustedTarget, TurnCompletion,
    TurnCompletionFuture, default_config_mutation,
};

const CHILD_HISTORY_KEY_UNAVAILABLE: &str = "child agent history key is unavailable";
const FOREIGN_PLUGIN_HOST_AGENT: &str = "agent does not belong to this plugin host";
const NO_COMMAND_RUNNER: &str = "this frontend runs no commands for the agent";

struct RootTarget {
    target: TrustedTarget,
    config_mutations: Option<ConfigMutationReserver>,
    commands: Option<CommandRunner>,
}

struct ChildTarget {
    target: TrustedTarget,
    history_key: String,
}

#[derive(Default)]
struct Targets {
    roots: Vec<RootTarget>,
    children: Vec<ChildTarget>,
    selected: Option<AgentRef>,
}

pub struct AgentOrchestration {
    targets: Mutex<Targets>,
    models: Arc<ArcSwapOption<Vec<String>>>,
    completion: Option<TurnCompletion>,
}

fn same_agent(left: &AgentRef, right: &AgentRef) -> bool {
    left.same_manager(right) && left.id() == right.id()
}

impl AgentOrchestration {
    pub fn new(models: Arc<ArcSwapOption<Vec<String>>>) -> Self {
        Self {
            targets: Mutex::new(Targets::default()),
            models,
            completion: None,
        }
    }

    pub fn bind_root(models: Arc<ArcSwapOption<Vec<String>>>, root: TrustedTarget) -> Self {
        let target = root.target.clone();
        let services = Self::new(models);
        services.register_root(root, None);
        services.select(Some(target));
        services
    }

    pub fn with_completion(mut self, completion: TurnCompletion) -> Self {
        self.completion = Some(completion);
        self
    }

    fn lock(&self) -> MutexGuard<'_, Targets> {
        self.targets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub fn register_root(
        &self,
        target: TrustedTarget,
        config_mutations: Option<ConfigMutationReserver>,
    ) {
        let mut targets = self.lock();
        targets.roots.retain(|entry| {
            entry.target.target.validate_live().is_ok()
                && !entry.target.target.same_manager(&target.target)
        });
        targets.roots.push(RootTarget {
            target,
            config_mutations,
            commands: None,
        });
    }

    /// Runs plugin-requested commands for {root}'s graph through {runner},
    /// the frontend's own command target.
    pub fn bind_commands(&self, root: &AgentRef, runner: CommandRunner) {
        if let Some(entry) = self
            .lock()
            .roots
            .iter_mut()
            .find(|entry| same_agent(&entry.target.target, root))
        {
            entry.commands = Some(runner);
        }
    }

    pub fn history_key(&self, target: &AgentRef) -> Result<String, String> {
        self.validate_target(target)?;
        self.lock()
            .children
            .iter()
            .find(|entry| same_agent(&entry.target.target, target))
            .map(|entry| entry.history_key.clone())
            .ok_or_else(|| CHILD_HISTORY_KEY_UNAVAILABLE.into())
    }

    pub(super) fn roots(&self) -> Vec<TrustedTarget> {
        self.lock()
            .roots
            .iter()
            .map(|entry| entry.target.clone())
            .collect()
    }

    pub fn select(&self, selected: Option<AgentRef>) {
        let mut targets = self.lock();
        targets.selected = selected;
        targets
            .roots
            .retain(|entry| entry.target.target.validate_live().is_ok());
        let roots = targets
            .roots
            .iter()
            .map(|entry| entry.target.target.clone())
            .collect::<Vec<_>>();
        targets.children.retain(|entry| {
            entry.target.target.validate_live().is_ok()
                && roots
                    .iter()
                    .any(|root| root.same_manager(&entry.target.target))
        });
    }
}

impl OrchestrationServices for AgentOrchestration {
    fn resolve_target(
        &self,
        invocation: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        if let Some(invocation) = invocation {
            let Some(pinned) = invocation.admission_context::<TrustedTarget>() else {
                return Ok(None);
            };
            if pinned.target.validate_live().is_err() {
                return Ok(None);
            }
            self.validate_target(&pinned.target)?;
            return Ok(Some(pinned.clone()));
        }
        let target = self.lock().selected.clone();
        target
            .map(|target| {
                self.template(&target)
                    .map(|template| TrustedTarget { target, template })
            })
            .transpose()
    }

    fn register_child_target(
        &self,
        target: TrustedTarget,
        history_key: String,
    ) -> Result<(), String> {
        self.validate_target(&target.target)?;
        let mut targets = self.lock();
        if targets
            .roots
            .iter()
            .any(|root| same_agent(&root.target.target, &target.target))
        {
            return Err("root template registration is owned by the host".into());
        }
        targets.children.retain(|entry| {
            entry.target.target.validate_live().is_ok()
                && !same_agent(&entry.target.target, &target.target)
        });
        targets.children.push(ChildTarget {
            target,
            history_key,
        });
        Ok(())
    }

    fn template(&self, target: &AgentRef) -> Result<ToolContext, String> {
        target.validate_live().map_err(|error| error.to_string())?;
        let targets = self.lock();
        let root = targets
            .roots
            .iter()
            .find(|entry| {
                entry.target.target.same_manager(target)
                    && entry.target.target.validate_live().is_ok()
            })
            .ok_or(FOREIGN_PLUGIN_HOST_AGENT)?;
        let source = if root.target.target.id() == target.id() {
            &root.target
        } else {
            targets
                .children
                .iter()
                .find(|entry| same_agent(&entry.target.target, target))
                .map(|entry| &entry.target)
                .ok_or("child agent template is unavailable")?
        };
        let mut template = source.template.clone();
        let config = target
            .effective_config()
            .map_err(|error| error.to_string())?
            .ok_or("agent configuration is unavailable")?;
        template.apply_effective_config(&config);
        template.cwd = config.context.cwd().to_path_buf();
        template.managed_turn = None;
        template.tool_use_id = None;
        template.live_sink = None;
        template.cancel = CancelToken::none();
        template.deadline = Deadline::None;
        template.loaded_instructions = LoadedInstructions::new();
        template.pending_output_limits = None;
        template.turn_bindings = Arc::new(TurnToolBindings::default());
        Ok(template)
    }

    fn validate_target(&self, target: &AgentRef) -> Result<(), String> {
        target.validate_live().map_err(|error| error.to_string())?;
        if self.lock().roots.iter().any(|entry| {
            entry.target.target.same_manager(target) && entry.target.target.validate_live().is_ok()
        }) {
            Ok(())
        } else {
            Err(FOREIGN_PLUGIN_HOST_AGENT.into())
        }
    }

    fn reserve_config_mutation(
        &self,
        target: &AgentRef,
        publication: CheckpointPublication,
    ) -> Result<ConfigMutation, ConfigChangeError> {
        self.validate_target(target)?;
        let reserve = self
            .lock()
            .roots
            .iter()
            .find(|root| root.target.target.same_manager(target))
            .and_then(|root| root.config_mutations.clone());
        match reserve {
            Some(reserve) => reserve(target, publication),
            None => default_config_mutation(target, publication),
        }
    }

    fn complete_turn(
        &self,
        target: &AgentRef,
        history: HistorySnapshot<Message>,
    ) -> TurnCompletionFuture {
        if let Err(error) = self.validate_target(target) {
            return Box::pin(async move { Err(error) });
        }
        if let Some(completion) = &self.completion {
            return completion(target, history);
        }
        Box::pin(async { Ok(()) })
    }

    fn command_runner(&self, target: &AgentRef) -> Result<CommandRunner, String> {
        self.validate_target(target)?;
        self.lock()
            .roots
            .iter()
            .find(|root| root.target.target.same_manager(target))
            .and_then(|root| root.commands.clone())
            .ok_or_else(|| NO_COMMAND_RUNNER.into())
    }

    fn available_models(&self, target: &AgentRef) -> Result<Vec<String>, String> {
        let template = self.template(target)?;
        let models = self
            .models
            .load_full()
            .ok_or("host model catalog is unavailable")?;
        Ok(models
            .iter()
            .filter(|spec| template.model_policy.allows(spec))
            .cloned()
            .collect())
    }
}
