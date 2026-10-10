use arc_swap::ArcSwapOption;
use maki_agent::{
    AgentRef,
    agent::LoadedInstructions,
    cancel::CancelToken,
    tools::{Deadline, ToolContext, TurnToolBindings},
};
use maki_commands::{CommandInvocation, InvocationTargetId};
use maki_lua::orchestration::{OrchestrationServices, TrustedTarget};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Targets {
    roots: Vec<TrustedTarget>,
    children: Vec<TrustedTarget>,
    commands: HashMap<InvocationTargetId, AgentRef>,
    focused: Option<AgentRef>,
}

pub(crate) struct AgentOrchestration {
    targets: Mutex<Targets>,
    models: Arc<ArcSwapOption<Vec<String>>>,
}

impl AgentOrchestration {
    pub(crate) fn new(models: Arc<ArcSwapOption<Vec<String>>>) -> Self {
        Self {
            targets: Mutex::new(Targets::default()),
            models,
        }
    }

    pub(crate) fn register_root(&self, target: TrustedTarget) {
        let mut targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        targets.roots.retain(|entry| {
            entry.target.validate_live().is_ok() && !entry.target.same_manager(&target.target)
        });
        targets.roots.push(target);
    }

    pub(crate) fn select(
        &self,
        commands: HashMap<InvocationTargetId, AgentRef>,
        focused: Option<AgentRef>,
    ) {
        let mut targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        targets.commands = commands;
        targets.focused = focused;
        targets
            .roots
            .retain(|entry| entry.target.validate_live().is_ok());
        let roots = targets
            .roots
            .iter()
            .map(|entry| entry.target.clone())
            .collect::<Vec<_>>();
        targets.children.retain(|entry| {
            entry.target.validate_live().is_ok()
                && roots.iter().any(|root| root.same_manager(&entry.target))
        });
    }
}

impl OrchestrationServices for AgentOrchestration {
    fn resolve_target(
        &self,
        invocation: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        if let Some(invocation) = invocation {
            let pinned = invocation
                .admission_context::<TrustedTarget>()
                .ok_or("command agent admission context is unavailable")?;
            self.validate_target(&pinned.target)?;
            return Ok(Some(pinned.clone()));
        }
        let target = {
            let targets = self
                .targets
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            targets.focused.clone()
        };
        target
            .map(|target| {
                self.template(&target)
                    .map(|template| TrustedTarget { target, template })
            })
            .transpose()
    }

    fn register_target(&self, target: TrustedTarget) -> Result<(), String> {
        self.validate_target(&target.target)?;
        let mut targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !targets.roots.iter().any(|root| {
            root.target.same_manager(&target.target) && root.target.validate_live().is_ok()
        }) {
            return Err("agent does not belong to this plugin host".into());
        }
        if targets.roots.iter().any(|root| {
            root.target.same_manager(&target.target) && root.target.id() == target.target.id()
        }) {
            return Err("root template registration is owned by the host".into());
        }
        targets.children.retain(|entry| {
            entry.target.validate_live().is_ok()
                && !(entry.target.same_manager(&target.target)
                    && entry.target.id() == target.target.id())
        });
        targets.children.push(target);
        Ok(())
    }

    fn template(&self, target: &AgentRef) -> Result<ToolContext, String> {
        target.validate_live().map_err(|error| error.to_string())?;
        let targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let root = targets
            .roots
            .iter()
            .find(|entry| entry.target.same_manager(target) && entry.target.validate_live().is_ok())
            .ok_or("agent does not belong to this plugin host")?;
        let source = if root.target.id() == target.id() {
            root
        } else {
            targets
                .children
                .iter()
                .find(|entry| entry.target.same_manager(target) && entry.target.id() == target.id())
                .ok_or("child agent template is unavailable")?
        };
        let mut template = source.template.clone();
        let config = target
            .effective_config()
            .map_err(|error| error.to_string())?
            .ok_or("agent configuration is unavailable")?;
        template.provider = Arc::clone(&config.settings.provider);
        template.model = Arc::new(config.settings.model.clone());
        template.opts.thinking = config.settings.thinking;
        template.opts.fast = config.settings.fast;
        template.workflow = config.settings.workflow;
        template.mode = config.mode.clone();
        template.mode_def = config.mode_def.clone().map(Arc::new);
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
        let targets = self
            .targets
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if targets
            .roots
            .iter()
            .any(|entry| entry.target.same_manager(target) && entry.target.validate_live().is_ok())
        {
            Ok(())
        } else {
            Err("agent does not belong to this plugin host".into())
        }
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
