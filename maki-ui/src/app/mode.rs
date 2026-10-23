use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::agent::QueuedMessage;
use crate::theme;
use maki_agent::{AgentInput, AgentMode, ModeDef, ModeId, ModeRegistry};
use ratatui::style::{Color, Modifier, Style};

use super::{App, ModelPickerPurpose};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Mode {
    Build,
    Plan,
    Custom(Arc<str>),
}

impl From<&AgentMode> for Mode {
    fn from(mode: &AgentMode) -> Self {
        match mode.id() {
            ModeId::Build => Self::Build,
            ModeId::Plan => Self::Plan,
            ModeId::Custom(name) => Self::Custom(name),
        }
    }
}

pub(crate) enum PlanTrigger {
    WriteDone,
    InteractivePrompt,
}

impl Mode {
    #[cfg(test)]
    pub(crate) fn from_id(id: &str) -> Self {
        match id {
            "build" => Self::Build,
            "plan" => Self::Plan,
            name => Self::Custom(Arc::from(name)),
        }
    }

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
}

impl App {
    pub(crate) fn transition_plan(&mut self, trigger: PlanTrigger) {
        if self.root_mode() != Mode::Plan {
            return;
        }
        match trigger {
            PlanTrigger::WriteDone => {
                if self.state.plan.is_ready() {
                    return;
                }
                self.state.plan.mark_ready();
                self.plan_form.on_plan_ready();
                if let Some(path) = self.state.plan.path().map(|p| p.display().to_string()) {
                    self.fire_session_autocmd("PlanReady", serde_json::json!({ "path": path }));
                }
            }
            PlanTrigger::InteractivePrompt => {
                if self.state.plan.is_ready() {
                    self.state.plan.mark_drafting();
                    self.plan_form.on_plan_drafting();
                }
            }
        }
    }

    /// Cycles to the next registered mode; the default two-mode case keeps
    /// Tab toggling build<->plan.
    pub(super) fn toggle_mode(&mut self) -> Vec<super::Action> {
        let registry = self.lua_event_handle.mode_registry();
        let modes = registry.list();
        if modes.is_empty() {
            return vec![];
        }
        let current = self.focused_mode().id_key();
        let idx = modes.iter().position(|d| d.id.key() == current);
        let next = match idx {
            Some(i) => &modes[(i + 1) % modes.len()],
            None => &modes[0],
        };
        vec![super::Action::ChangeMode(next.id.key().to_owned())]
    }

    /// Follows a committed root mode change: leaving plan mode retires the
    /// plan form, entering it adopts the plan path the agent writes to.
    pub(crate) fn project_root_mode(&mut self, previous: &AgentMode, mode: &AgentMode) {
        if previous.id() == mode.id() && previous.plan_path() == mode.plan_path() {
            return;
        }
        match mode.plan_path() {
            Some(path) if self.state.plan.path() != Some(path) => {
                self.state.plan = PlanState::Drafting(path.to_path_buf());
            }
            Some(_) => {}
            None if matches!(previous, AgentMode::Plan(_)) => {
                self.state.plan = PlanState::None;
                self.plan_form.reset();
                self.plan_approval_pending = false;
                if self.model_picker_purpose == ModelPickerPurpose::PlanImplementation {
                    self.close_model_picker();
                }
            }
            None => {}
        }
        self.file_completion.close();
    }

    pub(crate) fn build_agent_input(&self, msg: &QueuedMessage) -> AgentInput {
        // `msg.text` is already `@`-expanded at submit time (`submit_prompt`);
        // no second expansion here.
        AgentInput::new(msg.text.clone(), msg.images.clone())
    }

    pub(super) fn mode_label(&self) -> (Cow<'static, str>, Style) {
        let label: Cow<'static, str> = if self.is_bash_input() {
            "[BASH]".into()
        } else if let Some(config) = self.focused_config() {
            config
                .mode_def
                .as_ref()
                .map_or_else(
                    || config.mode.id().key().to_owned(),
                    |mode| mode.label.to_string(),
                )
                .into()
        } else {
            self.focused_mode()
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
            self.focused_mode().color()
        }
    }

    pub(super) fn separator_style(&self) -> Style {
        if self.focused_streaming() {
            theme::current().input_border
        } else {
            Style::new().fg(self.effective_mode_color())
        }
    }
}
