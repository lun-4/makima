use super::App;
use crate::components::Action;
use crate::components::list_picker::{PickerAction, PickerItem};
use crate::repaint::Dirty;
use crossterm::event::KeyEvent;
use makima_agent::rewind::{RewindGuard, RewindTargetId};

pub(super) const REWIND_WARNING: &str =
    "Only conversation context changes. Files and running tasks stay unchanged.";
pub(super) const PLANNING_WARNING: &str =
    "Back to planning session. Files changed during implementation stay changed.";
const NO_REWIND_SESSION: &str = "Rewind is unavailable before the session starts";

#[derive(Clone)]
pub(crate) struct RewindEntry {
    pub index: usize,
    pub label: String,
    pub section: String,
    pub planning: bool,
}

impl PickerItem for RewindEntry {
    fn label(&self) -> &str {
        &self.label
    }

    fn section(&self) -> Option<&str> {
        Some(&self.section)
    }

    fn detail(&self) -> Option<&str> {
        self.planning.then_some(PLANNING_WARNING)
    }
}

impl App {
    pub(crate) fn show_rewind(&mut self, guard: RewindGuard) {
        let entries = guard
            .targets()
            .iter()
            .enumerate()
            .map(|(index, target)| {
                let planning = target.id == RewindTargetId::PlanningSession;
                RewindEntry {
                    index,
                    label: target
                        .prompt
                        .as_ref()
                        .and_then(|prompt| prompt.user_text())
                        .unwrap_or("Back to planning session")
                        .to_owned(),
                    section: if planning {
                        "Planning"
                    } else if target.cut_off {
                        "Rewound"
                    } else {
                        "Current context"
                    }
                    .into(),
                    planning,
                }
            })
            .collect();
        self.rewind_guard = Some(guard);
        self.rewind_picker.open(entries, REWIND_WARNING);
        self.pending_dirty |= Dirty::YES;
    }

    pub(super) fn open_rewind(&mut self) -> Vec<Action> {
        if self.coordinator.is_none() {
            self.flash(NO_REWIND_SESSION.into());
            return vec![];
        }
        self.rewind_pending = true;
        vec![Action::BeginRewind]
    }

    pub(super) fn handle_rewind_key(&mut self, key: KeyEvent) -> Vec<Action> {
        match self.rewind_picker.handle_key(key) {
            PickerAction::Select(entry) => {
                self.rewind_pending = true;
                vec![Action::CommitRewind(entry.index)]
            }
            PickerAction::Close => {
                self.rewind_guard = None;
                vec![]
            }
            _ => vec![],
        }
    }
}
