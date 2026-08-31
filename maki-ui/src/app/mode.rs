use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::agent::QueuedMessage;
use crate::components::Status;
use crate::theme;
use maki_agent::{AgentInput, AgentMode, ModeDef, ModeId, ModeRegistry};
use maki_storage::StateDir;
use maki_storage::plans;
use ratatui::style::{Color, Modifier, Style};

use super::App;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mode {
    Build,
    Plan,
    Custom(Arc<str>),
}

pub(crate) enum PlanTrigger {
    WriteDone,
    InteractivePrompt,
}

impl Mode {
    pub(crate) fn def(&self, registry: &ModeRegistry) -> ModeDef {
        match self {
            Self::Build => registry
                .get(&ModeId::Build)
                .unwrap_or_else(|| maki_agent::ModeDef::default_for(ModeId::Build)),
            Self::Plan => registry
                .get(&ModeId::Plan)
                .unwrap_or_else(|| maki_agent::ModeDef::default_for(ModeId::Plan)),
            Self::Custom(name) => registry
                .get(&ModeId::Custom(Arc::clone(name)))
                .unwrap_or_else(|| maki_agent::ModeDef::default_for(ModeId::Build)),
        }
    }

    pub(crate) fn label(&self, registry: &ModeRegistry) -> Cow<'static, str> {
        match self {
            Self::Build => "[BUILD]".into(),
            Self::Plan => "[PLAN]".into(),
            Self::Custom(name) => {
                let label = self.def(registry).label;
                if label.is_empty() {
                    format!("[{}]", name.to_ascii_uppercase()).into()
                } else {
                    Cow::Owned(label.to_string())
                }
            }
        }
    }

    pub(crate) fn color(&self) -> Color {
        match self {
            Self::Build => theme::current().mode_build,
            Self::Plan => theme::current().mode_plan,
            Self::Custom(_) => theme::current().mode_custom,
        }
    }

    pub(crate) fn id_key(&self) -> String {
        match self {
            Self::Build => "build".to_owned(),
            Self::Plan => "plan".to_owned(),
            Self::Custom(name) => name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum PlanState {
    #[default]
    None,
    Drafting(PathBuf),
    Ready(PathBuf),
}

impl PlanState {
    pub(crate) fn path(&self) -> Option<&Path> {
        match self {
            Self::None => Option::None,
            Self::Drafting(p) | Self::Ready(p) => Some(p),
        }
    }

    pub(crate) fn mark_ready(&mut self) {
        if let Self::Drafting(p) = self {
            *self = Self::Ready(std::mem::take(p));
        }
    }

    pub(crate) fn mark_drafting(&mut self) {
        if let Self::Ready(p) = self {
            *self = Self::Drafting(std::mem::take(p));
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    pub(crate) fn allocate_path(&mut self, storage: &StateDir) {
        if matches!(self, Self::None) {
            *self = Self::Drafting(
                plans::new_plan_path(storage).unwrap_or_else(|_| PathBuf::from("plans/plan.md")),
            );
        }
    }
}

impl App {
    pub(crate) fn transition_plan(&mut self, trigger: PlanTrigger) {
        if self.state.mode != Mode::Plan {
            return;
        }
        match trigger {
            PlanTrigger::WriteDone => {
                if self.state.plan.is_ready() {
                    return;
                }
                self.state.plan.mark_ready();
                self.plan_form.on_plan_ready();
            }
            PlanTrigger::InteractivePrompt => {
                if self.state.plan.is_ready() {
                    self.state.plan.mark_drafting();
                    self.plan_form.on_plan_drafting();
                }
            }
        }
    }

    pub(super) fn enter_plan(&mut self) {
        self.state.plan.allocate_path(&self.storage);
        self.state.mode = Mode::Plan;
    }

    /// Cycles to the next registered mode; the default two-mode case keeps
    /// Tab toggling build<->plan.
    pub(super) fn toggle_mode(&mut self) -> Vec<super::Action> {
        let registry = self.lua_event_handle.mode_registry();
        let modes = registry.list();
        if modes.is_empty() {
            return vec![];
        }
        let current = self.state.mode.id_key();
        let idx = modes.iter().position(|d| d.id.key() == current);
        let next = match idx {
            Some(i) => &modes[(i + 1) % modes.len()],
            None => &modes[0],
        };
        self.set_mode_id(next.id.key().to_owned());
        vec![]
    }

    /// Applies a mode switch, guarding plan-mode invariants (plan path).
    pub(crate) fn set_mode_id(&mut self, id: String) {
        match id.as_str() {
            "build" => {
                self.state.mode = Mode::Build;
            }
            "plan" => {
                self.state.plan.allocate_path(&self.storage);
                self.state.mode = Mode::Plan;
            }
            name => {
                self.state.mode = Mode::Custom(Arc::from(name));
            }
        }
        self.emit_mode_changed(&id);
    }

    fn emit_mode_changed(&self, id: &str) {
        let data = serde_json::json!({ "mode": id });
        self.lua_event_handle.fire_autocmd("ModeChanged", data);
    }

    pub(super) fn agent_mode(&self) -> AgentMode {
        match &self.state.mode {
            Mode::Plan => match self.state.plan.path() {
                Some(p) => AgentMode::Plan(p.to_path_buf()),
                None => {
                    debug_assert!(false, "Plan mode without path - invariant violated");
                    AgentMode::Build
                }
            },
            Mode::Build => AgentMode::Build,
            Mode::Custom(name) => AgentMode::Custom(ModeId::Custom(Arc::clone(name))),
        }
    }

    pub(crate) fn build_agent_input(&self, msg: &QueuedMessage) -> AgentInput {
        // `msg.text` is already `@`-expanded at submit time (`submit_prompt`);
        // no second expansion here.
        AgentInput {
            message: msg.text.clone(),
            mode: self.agent_mode(),
            images: msg.images.clone(),
            preamble: Vec::new(),
            thinking: self.state.thinking,
            fast: self.state.fast,
            workflow: self.state.workflow,
            prompt: None,
            lease_committer: None,
        }
    }

    pub(super) fn mode_label(&self) -> (Cow<'static, str>, Style) {
        let label: Cow<'static, str> = if self.is_bash_input() {
            "[BASH]".into()
        } else {
            self.state
                .mode
                .label(&self.lua_event_handle.mode_registry())
        };
        let style = Style::new()
            .fg(self.effective_mode_color())
            .add_modifier(Modifier::BOLD);
        (label, style)
    }

    pub(crate) fn is_bash_input(&self) -> bool {
        self.input_box
            .buffer
            .lines()
            .first()
            .is_some_and(|l| l.starts_with('!'))
    }

    pub(super) fn effective_mode_color(&self) -> Color {
        if self.is_bash_input() {
            theme::current().mode_bash
        } else {
            self.state.mode.color()
        }
    }

    pub(super) fn separator_style(&self) -> Style {
        if self.status == Status::Streaming {
            theme::current().input_border
        } else {
            Style::new().fg(self.effective_mode_color())
        }
    }
}
