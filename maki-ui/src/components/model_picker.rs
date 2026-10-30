use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::text::{Line, Span};

use maki_domain::{
    MODEL_OPTION_SPECS, ModelCapabilities, ModelOptionAvailability, ModelOptionSpec,
    ModelOptionValueKind, ModelSelection,
};
use maki_providers::dynamic;
use maki_providers::model_registry;
use maki_providers::provider::ProviderKind;
use maki_providers::{CompleteModelSelection, ModelTier};

use crate::components::Overlay;
use crate::components::keybindings::key;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem};
use crate::repaint::{Cadence, Dirty, Watch};
use crate::text_buffer::{EditResult, TextBuffer};
use crate::theme;

const TITLE: &str = " Models ";
const RECENT_SECTION: &str = "Recent";
const FREE_PREFIX: &str = "Free · ";
const PICKER_WIDTH_PERCENT: u32 = 65;
const PICKER_HEIGHT_PERCENT: u32 = 80;
const FOOTER_RESERVED_ROWS: u32 = 4;
const COMPACT_HINT_WIDTH: usize = 40;
const CONFLICT_HINT_WIDTH: usize = 60;

fn footer_line() -> Line<'static> {
    let t = theme::current();
    Line::from(vec![
        Span::styled("  !", t.keybind_key),
        Span::styled(" strong", t.tool_dim),
        Span::styled("  @", t.keybind_key),
        Span::styled(" medium", t.tool_dim),
        Span::styled("  #", t.keybind_key),
        Span::styled(" weak", t.tool_dim),
        Span::styled("  $", t.keybind_key),
        Span::styled(" compaction", t.tool_dim),
    ])
}

fn tier_for_shortcut(key: KeyEvent) -> Option<ModelTier> {
    let digit = match (key.code, key.modifiers.contains(KeyModifiers::SHIFT)) {
        // Kitty protocol: Shift+digit reported with base key + SHIFT modifier
        (KeyCode::Char(c @ '1'..='4'), true) => c,
        // Legacy terminals: Shift+digit reported as the resulting character
        (KeyCode::Char('!' | '¡'), false) => '1', // US, ES
        (KeyCode::Char('@' | '"' | '™'), false) => '2', // US, UK/DE
        (KeyCode::Char('#' | '§' | '£'), false) => '3', // US, DE, UK
        (KeyCode::Char('$' | '€' | '¤'), false) => '4', // US, EU, Nordic
        _ => return None,
    };
    match digit {
        '1' => Some(ModelTier::Strong),
        '2' => Some(ModelTier::Medium),
        '3' => Some(ModelTier::Weak),
        '4' => Some(ModelTier::Compaction),
        _ => None,
    }
}

pub enum ModelPickerAction {
    Consumed,
    Select(ModelSelection),
    AssignTier(String, ModelTier),
    UnassignTier(String, ModelTier),
    Close,
}

struct ModelEntry {
    spec: String,
    id: String,
    provider_display: String,
    suffix: Option<String>,
    tier: String,
    override_tiers: Vec<ModelTier>,
}

impl PickerItem for ModelEntry {
    fn label(&self) -> &str {
        &self.id
    }

    fn suffix(&self) -> Option<&str> {
        self.suffix.as_deref()
    }

    fn detail(&self) -> Option<&str> {
        Some(&self.tier)
    }

    fn section(&self) -> Option<&str> {
        Some(self.provider_display.as_str())
    }

    fn is_highlighted(&self) -> bool {
        !self.override_tiers.is_empty()
    }
}

pub struct ModelPicker {
    picker: ListPicker<ModelEntry>,
    models: Arc<ArcSwapOption<Vec<String>>>,
    available: Watch<Vec<String>>,
    recents: Vec<String>,
    current_spec: String,
    initial_spec: String,
    current: ModelSelection,
    capabilities: ModelCapabilities,
    specs: &'static [ModelOptionSpec],
    editors: Vec<TextBuffer>,
    focus: Option<usize>,
    error: Option<(usize, String)>,
    needs_rebuild: bool,
    /// User-moved entry to restore on refresh: `(was_recent, spec)`.
    anchor: Option<(bool, String)>,
}

impl ModelPicker {
    pub fn new(models: Arc<ArcSwapOption<Vec<String>>>) -> Self {
        Self {
            picker: ListPicker::new().with_footer_builder(footer_line),
            models,
            available: Watch::default(),
            recents: Vec::new(),
            current_spec: String::new(),
            initial_spec: String::new(),
            current: ModelSelection::default(),
            capabilities: ModelCapabilities::default(),
            specs: MODEL_OPTION_SPECS,
            editors: Vec::new(),
            focus: None,
            error: None,
            needs_rebuild: false,
            anchor: None,
        }
    }

    pub fn set_recents(&mut self, recents: Vec<String>) {
        self.recents = recents;
        self.needs_rebuild = true;
    }

    pub fn open(
        &mut self,
        current: CompleteModelSelection,
        capabilities: ModelCapabilities,
        draft: ModelSelection,
    ) {
        self.picker.close();
        self.current_spec = current.spec.to_string();
        self.initial_spec = draft.spec.as_deref().unwrap_or(&current.spec).to_owned();
        self.capabilities = if self.initial_spec == self.current_spec {
            capabilities
        } else {
            ModelCapabilities::default()
        };
        self.current = current.into();
        self.editors = self
            .specs
            .iter()
            .map(|spec| {
                let mut editor = TextBuffer::new(spec.read(&draft).unwrap_or_default());
                editor.move_to_end();
                editor
            })
            .collect();
        self.focus = None;
        self.error = None;
        self.anchor = None;
        self.needs_rebuild = false;
        self.available = Watch::seeded(self.models.load_full());
        let entries = self.load_entries();
        self.picker.open(entries, TITLE);
        self.preselect_initial_model();
    }

    pub fn update_current(&mut self, current: CompleteModelSelection) -> Dirty {
        if !self.is_open() {
            return Dirty::NO;
        }
        let current: ModelSelection = current.into();
        if self.current == current {
            return Dirty::NO;
        }
        let identity = current.spec.as_deref().unwrap_or_default();
        self.needs_rebuild |= self.current_spec != identity;
        self.current_spec = identity.to_owned();
        self.current = current;
        Dirty::YES
    }

    pub fn highlighted_spec(&self) -> Option<&str> {
        self.picker.selected_item().map(|entry| entry.spec.as_str())
    }

    pub fn set_capabilities(&mut self, spec: &str, capabilities: ModelCapabilities) -> Dirty {
        if self.highlighted_spec() != Some(spec) || self.capabilities == capabilities {
            return Dirty::NO;
        }
        self.capabilities = capabilities;
        self.reconcile_focus();
        Dirty::YES
    }

    /// Poll discovery outside rendering so an unchanged picker stays idle.
    pub fn refresh(&mut self) -> Dirty {
        if !self.picker.is_open() {
            return Dirty::NO;
        }
        let arrived = self.available.poll(self.models.load_full());
        if arrived == Dirty::NO && !self.needs_rebuild {
            return Dirty::NO;
        }
        self.needs_rebuild = false;
        let before = self.highlighted_spec().map(str::to_owned);
        let entries = self.load_entries();
        self.picker.replace_items(entries);
        if let Some((was_recent, spec)) = &self.anchor {
            let spec = spec.clone();
            if !self
                .picker
                .select_item_by(|e| e.spec == spec && e.suffix().is_some() == *was_recent)
            {
                self.picker.select_item_by(|e| e.spec == spec);
            }
        } else {
            self.preselect_initial_model();
        }
        if self.highlighted_spec() != before.as_deref() {
            self.invalidate_capabilities();
        }
        Dirty::YES
    }

    fn load_entries(&self) -> Vec<ModelEntry> {
        let specs = self.available.get();
        let mut entries = Vec::new();
        for spec in &self.recents {
            if specs.is_none_or(|specs| specs.contains(spec))
                && let Some(mut entry) = parse_model_entry(spec)
            {
                entry.suffix = Some(std::mem::take(&mut entry.provider_display));
                entry.provider_display = RECENT_SECTION.to_string();
                entries.push(entry);
            }
        }
        let mut full: Vec<ModelEntry> = specs
            .map(|specs| {
                specs
                    .iter()
                    .filter_map(|spec| parse_model_entry(spec))
                    .collect()
            })
            .unwrap_or_default();
        full.sort_by(|a, b| {
            a.provider_display
                .cmp(&b.provider_display)
                .then_with(|| a.id.cmp(&b.id))
        });
        entries.extend(full);
        for spec in [
            Some(self.current_spec.as_str()),
            Some(self.initial_spec.as_str()),
            self.highlighted_spec(),
        ]
        .into_iter()
        .flatten()
        .filter(|spec| !spec.is_empty())
        {
            if !entries.iter().any(|entry| entry.spec == spec) {
                entries.push(fallback_model_entry(spec));
            }
        }
        entries
    }

    fn preselect_initial_model(&mut self) {
        if !self
            .picker
            .select_item_by(|entry| entry.spec == self.initial_spec && entry.suffix().is_none())
        {
            self.picker
                .select_item_by(|entry| entry.spec == self.initial_spec);
        }
    }

    fn visible_options(&self) -> Vec<usize> {
        self.specs
            .iter()
            .enumerate()
            .filter_map(|(index, spec)| {
                matches!(
                    spec.availability(&self.capabilities),
                    ModelOptionAvailability::Available { .. }
                )
                .then_some(index)
            })
            .collect()
    }

    fn conflicts(&self) -> Vec<(usize, &'static str)> {
        self.specs
            .iter()
            .zip(&self.editors)
            .enumerate()
            .filter_map(|(index, (spec, editor))| {
                if editor.value().is_empty() {
                    return None;
                }
                match spec.availability(&self.capabilities) {
                    ModelOptionAvailability::Available { .. } => None,
                    ModelOptionAvailability::Pending { reason }
                    | ModelOptionAvailability::Unavailable { reason } => Some((index, reason)),
                }
            })
            .collect()
    }

    fn reconcile_focus(&mut self) {
        if self
            .focus
            .is_some_and(|index| !self.visible_options().contains(&index))
        {
            self.focus = None;
        }
    }

    fn invalidate_capabilities(&mut self) {
        self.capabilities = ModelCapabilities::default();
        self.reconcile_focus();
    }

    pub fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    pub fn close(&mut self) {
        self.picker.close();
        self.editors.clear();
        self.focus = None;
        self.error = None;
        self.capabilities = ModelCapabilities::default();
    }

    pub fn contains(&self, pos: Position) -> bool {
        self.picker.contains(pos)
    }

    pub fn scroll(&mut self, delta: i32) {
        self.picker.scroll(delta);
    }

    fn track_anchor<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        let before = self
            .picker
            .selected_item()
            .map(|entry| (entry.suffix().is_some(), entry.spec.clone()));
        let result = f(self);
        let after = self
            .picker
            .selected_item()
            .map(|entry| (entry.suffix().is_some(), entry.spec.clone()));
        if before != after && self.is_open() {
            if before.as_ref().map(|(_, spec)| spec) != after.as_ref().map(|(_, spec)| spec) {
                self.invalidate_capabilities();
            }
            self.anchor = after;
        }
        result
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        if !self.is_open() {
            return false;
        }
        if let Some(index) = self.focus {
            if self.specs[index].value_kind == ModelOptionValueKind::FreeInput {
                let text: String = text
                    .chars()
                    .filter(|character| !character.is_control())
                    .collect();
                self.editors[index].insert_text(&text);
                self.error = None;
            }
            true
        } else {
            self.track_anchor(|picker| picker.picker.handle_paste(text))
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> ModelPickerAction {
        self.track_anchor(|picker| picker.handle_key_inner(key))
    }

    fn move_focus(&mut self, backward: bool) {
        let visible = self.visible_options();
        self.focus = if backward {
            match self.focus {
                None => visible.last().copied(),
                Some(index) => visible
                    .iter()
                    .position(|&row| row == index)
                    .and_then(|position| position.checked_sub(1).map(|position| visible[position])),
            }
        } else {
            match self.focus {
                None => visible.first().copied(),
                Some(index) => visible
                    .iter()
                    .position(|&row| row == index)
                    .and_then(|position| visible.get(position + 1).copied()),
            }
        };
    }

    fn cycle_option(&mut self, index: usize, backward: bool) {
        let spec = &self.specs[index];
        let value = self.editors[index].value();
        let position = if value.is_empty() {
            Some(0)
        } else {
            spec.presets
                .iter()
                .position(|preset| preset.value == value)
                .map(|position| position + 1)
        };
        let count = spec.presets.len() + 1;
        let next = match (position, backward) {
            (Some(position), true) => (position + count - 1) % count,
            (Some(position), false) => (position + 1) % count,
            (None, true) => count - 1,
            (None, false) => 0,
        };
        self.editors[index] = TextBuffer::new(if next == 0 {
            String::new()
        } else {
            spec.presets[next - 1].value.to_owned()
        });
        self.editors[index].move_to_end();
        self.error = None;
    }

    fn confirm(&mut self) -> ModelPickerAction {
        let Some(identity) = self.highlighted_spec() else {
            return ModelPickerAction::Consumed;
        };
        let identity = (identity != self.current_spec).then(|| Arc::from(identity));
        if !self.conflicts().is_empty() {
            return ModelPickerAction::Consumed;
        }
        let mut selection = ModelSelection {
            spec: identity,
            ..ModelSelection::default()
        };
        for (index, (spec, editor)) in self.specs.iter().zip(&self.editors).enumerate() {
            let value = editor.value();
            if let Err(error) = spec.replace(
                &mut selection,
                (!value.is_empty()).then_some(value.as_str()),
            ) {
                self.error = Some((index, error.to_string()));
                self.focus = Some(index);
                return ModelPickerAction::Consumed;
            }
        }
        self.close();
        ModelPickerAction::Select(selection)
    }

    fn handle_key_inner(&mut self, event: KeyEvent) -> ModelPickerAction {
        if !self.is_open() {
            return ModelPickerAction::Close;
        }
        if event.code == KeyCode::Esc || key::QUIT.matches(event) {
            self.close();
            return ModelPickerAction::Close;
        }
        if key::REFRESH.matches(event) {
            for (index, _) in self.conflicts() {
                self.editors[index].clear();
                if self
                    .error
                    .as_ref()
                    .is_some_and(|(field, _)| *field == index)
                {
                    self.error = None;
                }
            }
            return ModelPickerAction::Consumed;
        }
        match event.code {
            KeyCode::Enter => return self.confirm(),
            KeyCode::Tab | KeyCode::BackTab => {
                self.move_focus(
                    event.code == KeyCode::BackTab || event.modifiers.contains(KeyModifiers::SHIFT),
                );
                return ModelPickerAction::Consumed;
            }
            _ => {}
        }
        if let Some(index) = self.focus {
            match (&self.specs[index].value_kind, event.code) {
                (ModelOptionValueKind::FreeInput, KeyCode::Up | KeyCode::Down)
                    if event.modifiers.is_empty() =>
                {
                    self.cycle_option(index, event.code == KeyCode::Up);
                }
                (
                    ModelOptionValueKind::Choice,
                    KeyCode::Char(' ') | KeyCode::Left | KeyCode::Right,
                ) if event.modifiers.is_empty() => {
                    self.cycle_option(index, event.code == KeyCode::Left);
                }
                (ModelOptionValueKind::FreeInput, _)
                    if self.editors[index].handle_key(event) == EditResult::Changed =>
                {
                    self.error = None;
                }
                _ => {}
            }
            return ModelPickerAction::Consumed;
        }
        if let Some(tier) = tier_for_shortcut(event)
            && let Some(entry) = self.picker.selected_item()
        {
            let spec = entry.spec.clone();
            self.needs_rebuild = true;
            return if entry.override_tiers.contains(&tier) {
                ModelPickerAction::UnassignTier(spec, tier)
            } else {
                ModelPickerAction::AssignTier(spec, tier)
            };
        }
        match self.picker.handle_key(event) {
            PickerAction::Close => ModelPickerAction::Close,
            PickerAction::Consumed
            | PickerAction::Select(_)
            | PickerAction::Toggle(..)
            | PickerAction::Delete(..) => ModelPickerAction::Consumed,
        }
    }

    fn option_line(&self, index: usize) -> Line<'static> {
        let t = theme::current();
        let spec = &self.specs[index];
        let value = self.editors[index].value();
        let focused = self.focus == Some(index);
        let mut spans = vec![
            Span::styled(if focused { " > " } else { "   " }, t.keybind_key),
            Span::styled(format!("{}: ", spec.label), t.keybind_key),
        ];
        if value.is_empty() {
            let current = spec.read(&self.current).unwrap_or_default();
            spans.push(Span::styled(format!("Use current ({current})"), t.tool_dim));
        } else if focused && spec.value_kind == ModelOptionValueKind::FreeInput {
            let cursor = self.editors[index].x();
            let mut characters = value.chars();
            let before: String = characters.by_ref().take(cursor).collect();
            let at = characters.next().unwrap_or(' ');
            spans.push(Span::raw(before));
            spans.push(Span::styled(at.to_string(), t.cursor));
            spans.push(Span::raw(characters.collect::<String>()));
        } else {
            spans.push(Span::raw(value));
        }
        if let ModelOptionAvailability::Available { note: Some(note) } =
            spec.availability(&self.capabilities)
        {
            spans.push(Span::styled(format!(" ({note})"), t.tool_dim));
        }
        Line::from(spans)
    }

    fn footer_lines(&self, area: Rect) -> Vec<Line<'static>> {
        let t = theme::current();
        let visible = self.visible_options();
        let conflicts = self.conflicts();
        let mut messages = Vec::new();
        for &(index, reason) in &conflicts {
            messages.push(Line::from(Span::styled(
                format!(
                    " {}={}: {reason}",
                    self.specs[index].label,
                    self.editors[index].value()
                ),
                t.error,
            )));
        }
        if let Some((index, error)) = &self.error {
            messages.push(Line::from(Span::styled(
                format!(" {}: {error}", self.specs[*index].label),
                t.error,
            )));
        }
        let mut pending = Vec::new();
        for spec in self.specs {
            if let ModelOptionAvailability::Pending { reason } =
                spec.availability(&self.capabilities)
                && !pending.contains(&reason)
            {
                pending.push(reason);
            }
        }
        for reason in pending {
            messages.push(Line::from(Span::styled(format!(" {reason}"), t.tool_dim)));
        }
        let width = (u32::from(area.width) * PICKER_WIDTH_PERCENT / 100).saturating_sub(2) as usize;
        let hint_text = if !conflicts.is_empty() {
            if width < CONFLICT_HINT_WIDTH {
                " Enter Esc Ctrl+R reset"
            } else {
                " Enter confirm  Esc cancel  Ctrl+R reset conflicts  Tab options"
            }
        } else if width < COMPACT_HINT_WIDTH {
            " Enter confirm Esc cancel Tab"
        } else {
            " Enter confirm  Esc cancel  Tab options"
        };
        let hint = Line::from(Span::styled(hint_text, t.keybind_key));
        let budget = (u32::from(area.height) * PICKER_HEIGHT_PERCENT / 100)
            .saturating_sub(FOOTER_RESERVED_ROWS)
            .max(1) as usize;
        let focused_rows = usize::from(self.focus.is_some() && conflicts.is_empty());
        let message_count = messages.len().min(budget.saturating_sub(1 + focused_rows));
        let option_count = visible.len().min(budget.saturating_sub(message_count + 1));
        let focus_position = self
            .focus
            .and_then(|index| visible.iter().position(|&row| row == index))
            .unwrap_or(0);
        let start = focus_position
            .saturating_add(1)
            .saturating_sub(option_count);
        let mut lines: Vec<Line<'static>> = visible
            .iter()
            .skip(start)
            .take(option_count)
            .map(|&index| self.option_line(index))
            .collect();
        lines.extend(messages.into_iter().take(message_count));
        if lines.len() + 1 < budget {
            lines.insert(
                0,
                Line::from(Span::styled(
                    format!(
                        " Selected: {}",
                        self.highlighted_spec().unwrap_or("No matches")
                    ),
                    t.tool_dim,
                )),
            );
        }
        if self.focus.is_some() && lines.len() + 1 < budget {
            lines.push(Line::from(Span::styled(
                " ↑/↓ presets  Space/←/→ choices  empty: Use current",
                t.tool_dim,
            )));
        } else if self.focus.is_none() && lines.len() + 1 < budget {
            lines.push(footer_line());
        }
        lines.push(hint);
        lines
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        let footer = self.footer_lines(area);
        self.picker.view_with_footer_lines(frame, area, footer)
    }
}

impl Overlay for ModelPicker {
    fn is_open(&self) -> bool {
        self.is_open()
    }

    fn close(&mut self) {
        self.close()
    }

    fn cadence(&self) -> Cadence {
        self.picker.cadence()
    }
}

fn fallback_model_entry(spec: &str) -> ModelEntry {
    let (provider, id) = spec.split_once('/').unwrap_or(("Model", spec));
    let override_tiers = model_registry::override_tiers(spec);
    ModelEntry {
        spec: spec.to_owned(),
        id: id.to_owned(),
        provider_display: provider.to_owned(),
        suffix: None,
        tier: format_tier_label("", &override_tiers, false),
        override_tiers,
    }
}

fn format_tier_label(tier: &str, override_tiers: &[ModelTier], free: bool) -> String {
    let tier = if override_tiers.is_empty() {
        tier.to_owned()
    } else {
        override_tiers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("/")
    };
    if free {
        format!("{FREE_PREFIX}{tier}")
    } else {
        tier
    }
}

fn parse_model_entry(spec: &str) -> Option<ModelEntry> {
    let (provider_str, model_id) = spec.split_once('/')?;

    let provider_display = if let Ok(kind) = provider_str.parse::<ProviderKind>() {
        kind.display_name().to_string()
    } else if let Some(name) = dynamic::display_name(provider_str) {
        name.to_string()
    } else if let Some(info) = maki_providers::catalog_provider_if_available(provider_str) {
        info.display_name.clone()
    } else if let Some(builtin) = maki_config::providers::builtin_provider(provider_str) {
        builtin.display_name.to_string()
    } else {
        let config = maki_config::providers::ProvidersConfig::load();
        config.get(provider_str)?;
        maki_config::providers::resolve_display_name(provider_str, config.get(provider_str))
    };

    let override_tiers = model_registry::override_tiers(spec);
    let (tier, free) = match maki_providers::Model::from_spec(spec) {
        Ok(m) => (m.tier.to_string(), m.is_free()),
        Err(_) => (String::new(), false),
    };
    let tier = format_tier_label(&tier, &override_tiers, free);
    let id = model_id.to_string();
    Some(ModelEntry {
        spec: spec.to_string(),
        id,
        provider_display,
        suffix: None,
        tier,
        override_tiers,
    })
}

#[cfg(test)]
mod tests {
    use super::{FREE_PREFIX, ModelPicker, ModelPickerAction, RECENT_SECTION, parse_model_entry};
    use crate::components::key;
    use crate::components::keybindings::key as kb;
    use crate::repaint::Dirty;
    use arc_swap::ArcSwapOption;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use maki_domain::{
        FastCapability, MODEL_CAPABILITIES_UNRESOLVED, MODEL_FAST_PENDING, MODEL_THINKING_REQUIRED,
        MODEL_THINKING_UNSUPPORTED, ModelCapabilities, ModelOptionAvailability, ModelOptionSpec,
        ModelOptionValueKind, ModelSelection, ThinkingCapability,
    };
    use maki_providers::{
        CompleteModelSelection, Effort, ModelTier, ThinkingConfig, model_registry,
    };
    use maki_storage::StateDir;
    use ratatui::{Terminal, backend::TestBackend};
    use std::sync::{Arc, Mutex, MutexGuard};
    use tempfile::TempDir;
    use test_case::test_case;

    const CURRENT: &str = "anthropic/claude-sonnet-4-20250514";
    const OTHER: &str = "anthropic/claude-opus-4-6-20260101";
    const THIRD: &str = "zai/glm-5";
    const MISSING: &str = "custom/undiscovered";
    const FALLBACK: &str = "llama-cpp/default";
    const INVALID: &str = "not-a-value";
    const SAME_SIZED_LIST: &str = "a republished list of the same length is still a new list";
    const EXTRA_LABEL: &str = "Injected effort";

    fn models(specs: &[&str]) -> Arc<ArcSwapOption<Vec<String>>> {
        Arc::new(ArcSwapOption::from(Some(Arc::new(
            specs.iter().map(|spec| (*spec).to_owned()).collect(),
        ))))
    }

    fn test_models() -> Arc<ArcSwapOption<Vec<String>>> {
        models(&[CURRENT, OTHER, THIRD])
    }

    static MODEL_REGISTRY_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct ModelRegistryTestGuard {
        _lock: MutexGuard<'static, ()>,
        _temp_dir: TempDir,
        state_dir: StateDir,
        previous_strong: Option<String>,
    }

    impl ModelRegistryTestGuard {
        fn new() -> Self {
            let lock = MODEL_REGISTRY_TEST_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let previous_strong =
                model_registry::spec_for_tier_any(ModelTier::Strong).filter(|previous| {
                    model_registry::override_tiers(previous).contains(&ModelTier::Strong)
                });
            let temp_dir = tempfile::tempdir().unwrap();
            let state_dir = StateDir::from_path(temp_dir.path().to_path_buf());
            Self {
                _lock: lock,
                _temp_dir: temp_dir,
                state_dir,
                previous_strong,
            }
        }
    }

    impl Drop for ModelRegistryTestGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.previous_strong.take() {
                model_registry::set_and_persist(previous, ModelTier::Strong, &self.state_dir);
            } else {
                model_registry::unset_and_persist(FALLBACK, ModelTier::Strong, &self.state_dir);
            }
        }
    }

    fn current(spec: &str) -> CompleteModelSelection {
        CompleteModelSelection {
            spec: Arc::from(spec),
            thinking: ThinkingConfig::Off,
            fast: false,
        }
    }

    fn capabilities(thinking: ThinkingCapability, fast: FastCapability) -> ModelCapabilities {
        ModelCapabilities {
            resolved: true,
            thinking,
            fast,
        }
    }

    fn supported() -> ModelCapabilities {
        capabilities(ThinkingCapability::Supported, FastCapability::Supported)
    }

    fn open(picker: &mut ModelPicker, spec: &str) {
        picker.open(current(spec), supported(), ModelSelection::default());
    }

    fn confirm(picker: &mut ModelPicker) -> ModelSelection {
        match picker.handle_key(key(KeyCode::Enter)) {
            ModelPickerAction::Select(selection) => selection,
            _ => panic!("expected a structured selection"),
        }
    }

    fn render(picker: &mut ModelPicker, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        terminal
            .backend()
            .buffer()
            .content()
            .chunks(width as usize)
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn a_same_sized_model_list_owes_a_frame() {
        let models = models(&[CURRENT]);
        let mut picker = ModelPicker::new(Arc::clone(&models));
        open(&mut picker, CURRENT);
        assert_eq!(picker.refresh(), Dirty::NO);
        models.store(Some(Arc::new(vec![THIRD.to_owned()])));
        assert_eq!(picker.refresh(), Dirty::YES, "{SAME_SIZED_LIST}");
        assert!(
            picker
                .picker
                .items()
                .unwrap()
                .iter()
                .any(|entry| entry.spec == THIRD)
        );
        assert_eq!(picker.highlighted_spec(), Some(CURRENT));
    }

    #[test_case(key(KeyCode::Esc); "esc")]
    #[test_case(kb::QUIT.to_key_event(); "ctrl_c")]
    fn model_picker_cancel_discards_draft(cancel: KeyEvent) {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("high");
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Right));
        assert!(matches!(
            picker.handle_key(cancel),
            ModelPickerAction::Close
        ));
        assert!(!picker.is_open());
        open(&mut picker, CURRENT);
        let selection = confirm(&mut picker);
        assert_eq!(selection, ModelSelection::default());
    }

    #[test_case("high", ThinkingConfig::Effort(Effort::High); "effort")]
    #[test_case("adaptive", ThinkingConfig::Adaptive; "adaptive")]
    #[test_case("2048", ThinkingConfig::Budget(2048); "budget")]
    #[test_case("off", ThinkingConfig::Off; "explicit_off")]
    fn model_picker_confirms_structured_selection(value: &str, expected: ThinkingConfig) {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(value);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Left));
        let selection = confirm(&mut picker);
        assert_eq!(selection.spec, None);
        assert_eq!(selection.thinking, Some(expected));
        assert_eq!(selection.fast, Some(false));
        assert!(!picker.is_open());
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Close
        ));
    }

    #[test_case(false; "ordinary_open")]
    #[test_case(true; "explicit_current_draft")]
    fn model_picker_same_identity_keeps_only_explicit_option_overrides(staged_identity: bool) {
        let mut picker = ModelPicker::new(test_models());
        let baseline = CompleteModelSelection {
            spec: Arc::from(CURRENT),
            thinking: ThinkingConfig::Effort(Effort::High),
            fast: true,
        };
        let draft = ModelSelection {
            spec: staged_identity.then(|| Arc::from(CURRENT)),
            ..ModelSelection::default()
        };
        picker.open(baseline, supported(), draft);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("high");
        assert_eq!(
            confirm(&mut picker),
            ModelSelection {
                thinking: Some(ThinkingConfig::Effort(Effort::High)),
                ..ModelSelection::default()
            }
        );
    }

    #[test_case(false; "omitted_options")]
    #[test_case(true; "explicit_option")]
    fn model_picker_current_update_preserves_highlight_and_drafts(explicit: bool) {
        let mut picker = ModelPicker::new(models(&[CURRENT]));
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        if explicit {
            picker.handle_paste("off");
        }
        let baseline = CompleteModelSelection {
            spec: Arc::from(OTHER),
            thinking: ThinkingConfig::Effort(Effort::High),
            fast: true,
        };
        assert_eq!(picker.update_current(baseline.clone()), Dirty::YES);
        assert_eq!(picker.update_current(baseline), Dirty::NO);
        assert_eq!(picker.highlighted_spec(), Some(CURRENT));
        assert_eq!(picker.initial_spec, CURRENT);
        assert_eq!(picker.focus, Some(0));
        assert_eq!(picker.editors[0].value(), if explicit { "off" } else { "" });
        let text = render(&mut picker, 120, 24);
        assert!(text.contains("Fast: Use current (true)"));
        if !explicit {
            assert!(text.contains("Thinking: Use current (high)"));
        }
        assert_eq!(picker.refresh(), Dirty::YES);
        assert_eq!(picker.highlighted_spec(), Some(CURRENT));
        assert!(
            picker
                .picker
                .items()
                .unwrap()
                .iter()
                .any(|entry| entry.spec == OTHER)
        );
        assert_eq!(
            confirm(&mut picker),
            ModelSelection {
                spec: Some(Arc::from(CURRENT)),
                thinking: explicit.then_some(ThinkingConfig::Off),
                ..ModelSelection::default()
            }
        );
    }

    #[test]
    fn model_picker_current_update_omits_new_current_highlight() {
        let mut picker = ModelPicker::new(test_models());
        let draft = ModelSelection {
            spec: Some(Arc::from(OTHER)),
            fast: Some(false),
            ..ModelSelection::default()
        };
        picker.open(current(CURRENT), supported(), draft);
        let _ = picker.set_capabilities(OTHER, supported());
        assert_eq!(picker.update_current(current(OTHER)), Dirty::YES);
        assert_eq!(picker.initial_spec, OTHER);
        assert_eq!(picker.highlighted_spec(), Some(OTHER));
        assert_eq!(
            confirm(&mut picker),
            ModelSelection {
                fast: Some(false),
                ..ModelSelection::default()
            }
        );
    }

    #[test_case(INVALID; "syntax")]
    #[test_case("0"; "zero_budget")]
    fn model_picker_invalid_option_keeps_draft_open(value: &str) {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_paste("sonnet");
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(value);
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert!(picker.is_open());
        assert_eq!(picker.highlighted_spec(), Some(CURRENT));
        assert_eq!(picker.editors[0].value(), value);
        assert!(picker.error.is_some());
        assert!(render(&mut picker, 120, 24).contains(value));
        picker.handle_key(key(KeyCode::Home));
        picker.handle_key(kb::KILL_LINE.to_key_event());
        picker.handle_paste("high");
        assert_eq!(
            confirm(&mut picker).thinking,
            Some(ThinkingConfig::Effort(Effort::High))
        );
    }

    #[test]
    fn model_picker_focus_routes_keys_and_paste() {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.focus, Some(0));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('!'))),
            ModelPickerAction::Consumed
        ));
        assert_eq!(picker.editors[0].value(), "!");
        picker.handle_key(key(KeyCode::Backspace));
        picker.handle_paste("2048\n");
        assert_eq!(picker.editors[0].value(), "2048");
        picker.handle_key(key(KeyCode::Down));
        assert_eq!(picker.editors[0].value(), "");
        picker.handle_key(key(KeyCode::Down));
        assert!(!picker.editors[0].value().is_empty());
        picker.handle_key(key(KeyCode::Up));
        assert_eq!(picker.editors[0].value(), "");
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("ignored");
        assert_eq!(picker.editors[1].value(), "");
        picker.handle_key(key(KeyCode::Right));
        assert_eq!(picker.editors[1].value(), "true");
        picker.handle_key(key(KeyCode::Char(' ')));
        assert_eq!(picker.editors[1].value(), "false");
        picker.handle_key(key(KeyCode::Right));
        assert_eq!(picker.editors[1].value(), "");
        picker.handle_key(key(KeyCode::BackTab));
        assert_eq!(picker.focus, Some(0));
        picker.handle_key(key(KeyCode::BackTab));
        assert_eq!(picker.focus, None);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.focus, None);
        assert_eq!(picker.highlighted_spec(), Some(CURRENT));
    }

    #[test_case(ThinkingCapability::Unsupported, FastCapability::Unsupported, false, false; "neither")]
    #[test_case(ThinkingCapability::Supported, FastCapability::Unsupported, true, false; "free_input")]
    #[test_case(ThinkingCapability::Unsupported, FastCapability::Supported, false, true; "choice")]
    #[test_case(ThinkingCapability::Supported, FastCapability::Supported, true, true; "both")]
    #[test_case(ThinkingCapability::Required, FastCapability::Pending, true, false; "required_pending")]
    fn model_picker_hides_unsupported_options(
        thinking: ThinkingCapability,
        fast: FastCapability,
        show_thinking: bool,
        show_fast: bool,
    ) {
        let mut picker = ModelPicker::new(test_models());
        let required = thinking == ThinkingCapability::Required;
        picker.open(
            current(CURRENT),
            capabilities(thinking, fast),
            ModelSelection::default(),
        );
        let text = render(&mut picker, 160, 24);
        assert_eq!(text.contains("Thinking:"), show_thinking);
        assert_eq!(text.contains("Fast:"), show_fast);
        assert_eq!(text.contains(MODEL_THINKING_REQUIRED), required);
        let visible = picker.visible_options();
        for &index in &visible {
            picker.handle_key(key(KeyCode::Tab));
            assert_eq!(picker.focus, Some(index));
        }
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.focus, None);
    }

    #[test]
    fn model_picker_pending_is_not_editable_and_model_only_can_confirm() {
        let mut picker = ModelPicker::new(models(&[]));
        picker.open(
            current(CURRENT),
            ModelCapabilities::default(),
            ModelSelection::default(),
        );
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.focus, None);
        let text = render(&mut picker, 120, 24);
        assert!(!text.contains("Thinking:"));
        assert!(!text.contains("Fast:"));
        assert!(text.contains(MODEL_CAPABILITIES_UNRESOLVED));
        assert_eq!(confirm(&mut picker), ModelSelection::default());
    }

    #[test_case("off"; "explicit_disabled")]
    #[test_case("high"; "enabled")]
    #[test_case(INVALID; "invalid_raw")]
    fn model_picker_unavailable_overrides_require_explicit_reset(value: &str) {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(value);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Left));
        let _ = picker.set_capabilities(
            CURRENT,
            capabilities(ThinkingCapability::Unsupported, FastCapability::Supported),
        );
        assert_eq!(picker.focus, Some(1));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert_eq!(picker.editors[0].value(), value);
        let text = render(&mut picker, 160, 24);
        assert!(!text.contains("Thinking:"));
        assert!(text.contains(&format!("Thinking={value}")));
        assert!(text.contains(MODEL_THINKING_UNSUPPORTED));
        assert!(text.contains("Ctrl+R"));
        let _ = picker.set_capabilities(CURRENT, supported());
        assert_eq!(picker.editors[0].value(), value);
        let _ = picker.set_capabilities(
            CURRENT,
            capabilities(ThinkingCapability::Unsupported, FastCapability::Supported),
        );
        picker.handle_key(kb::REFRESH.to_key_event());
        assert_eq!(picker.editors[0].value(), "");
        assert_eq!(picker.editors[1].value(), "false");
        let selection = confirm(&mut picker);
        assert_eq!(selection.thinking, None);
        assert_eq!(selection.fast, Some(false));
    }

    #[test]
    fn pending_override_remains_until_supported_or_reset() {
        let mut picker = ModelPicker::new(test_models());
        let draft = ModelSelection {
            fast: Some(false),
            ..ModelSelection::default()
        };
        picker.open(
            current(CURRENT),
            capabilities(ThinkingCapability::Supported, FastCapability::Pending),
            draft,
        );
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert!(render(&mut picker, 160, 24).contains(MODEL_FAST_PENDING));
        let _ = picker.set_capabilities(CURRENT, supported());
        assert_eq!(confirm(&mut picker).fast, Some(false));
    }

    #[test]
    fn model_picker_capability_refresh_preserves_drafts() {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("high");
        let _ = picker.set_capabilities(
            CURRENT,
            capabilities(ThinkingCapability::Unsupported, FastCapability::Supported),
        );
        assert_eq!(picker.focus, None);
        assert_eq!(picker.editors[0].value(), "high");
        picker.handle_key(key(KeyCode::Down));
        let highlighted = picker.highlighted_spec().unwrap().to_owned();
        assert_ne!(highlighted, CURRENT);
        assert!(picker.visible_options().is_empty());
        assert_eq!(picker.set_capabilities(CURRENT, supported()), Dirty::NO);
        assert!(picker.visible_options().is_empty());
        assert_eq!(
            picker.set_capabilities(&highlighted, supported()),
            Dirty::YES
        );
        assert_eq!(picker.editors[0].value(), "high");
        picker.close();
        assert_eq!(
            picker.set_capabilities(&highlighted, supported()),
            Dirty::NO
        );
        open(&mut picker, CURRENT);
        assert_eq!(picker.editors[0].value(), "");
    }

    #[test]
    fn highlighted_capabilities_override_current_model_controls() {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Down));
        let highlighted = picker.highlighted_spec().unwrap().to_owned();
        assert_ne!(highlighted, CURRENT);
        let _ = picker.set_capabilities(
            &highlighted,
            capabilities(ThinkingCapability::Unsupported, FastCapability::Unsupported),
        );
        let text = render(&mut picker, 120, 24);
        assert!(!text.contains("Thinking:"));
        assert!(!text.contains("Fast:"));
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.focus, None);
        assert_eq!(picker.set_capabilities(CURRENT, supported()), Dirty::NO);
        assert!(picker.visible_options().is_empty());
        assert_eq!(
            confirm(&mut picker).spec.as_deref(),
            Some(highlighted.as_str())
        );
    }

    #[test]
    fn reset_without_conflicts_keeps_supported_drafts() {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("off");
        picker.handle_key(kb::REFRESH.to_key_event());
        assert_eq!(confirm(&mut picker).thinking, Some(ThinkingConfig::Off));
    }

    #[test]
    fn model_picker_refresh_preserves_option_draft() {
        let models = test_models();
        let mut picker = ModelPicker::new(Arc::clone(&models));
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("high");
        models.store(Some(Arc::new(vec![CURRENT.into(), THIRD.into()])));
        assert_eq!(picker.refresh(), Dirty::YES);
        assert_eq!(picker.focus, Some(0));
        assert_eq!(picker.editors[0].value(), "high");
        assert_eq!(picker.highlighted_spec(), Some(CURRENT));
        assert_eq!(picker.refresh(), Dirty::NO);
    }

    #[test]
    fn staged_and_highlighted_undiscovered_identity_survive_refresh() {
        let models = test_models();
        let mut picker = ModelPicker::new(Arc::clone(&models));
        let draft = ModelSelection {
            spec: Some(Arc::from(MISSING)),
            fast: Some(false),
            ..ModelSelection::default()
        };
        picker.open(current(CURRENT), supported(), draft);
        assert_eq!(picker.highlighted_spec(), Some(MISSING));
        assert!(picker.visible_options().is_empty());
        let _ = picker.set_capabilities(MISSING, supported());
        models.store(Some(Arc::new(vec![THIRD.into()])));
        let _ = picker.refresh();
        assert_eq!(picker.highlighted_spec(), Some(MISSING));
        assert_eq!(
            picker
                .picker
                .items()
                .unwrap()
                .iter()
                .filter(|entry| entry.spec == MISSING)
                .count(),
            1
        );
        assert_eq!(confirm(&mut picker).fast, Some(false));
        open(&mut picker, CURRENT);
        picker.handle_paste("glm");
        assert_eq!(picker.highlighted_spec(), Some(THIRD));
        models.store(Some(Arc::new(vec![CURRENT.into()])));
        let _ = picker.refresh();
        assert_eq!(picker.highlighted_spec(), Some(THIRD));
    }

    #[test]
    fn current_model_option_edits_need_no_discovery() {
        let mut picker = ModelPicker::new(models(&[]));
        open(&mut picker, MISSING);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("off");
        let selection = confirm(&mut picker);
        assert_eq!(selection.spec, None);
        assert_eq!(selection.thinking, Some(ThinkingConfig::Off));
    }

    #[test]
    fn unmatched_search_does_not_confirm_from_option_focus() {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_paste("unmatched-search");
        picker.handle_key(key(KeyCode::Tab));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert!(picker.is_open());
        assert_eq!(picker.highlighted_spec(), None);
    }

    #[test]
    fn refresh_updates_items_and_preserves_search() {
        let models = models(&[CURRENT]);
        let mut picker = ModelPicker::new(Arc::clone(&models));
        open(&mut picker, CURRENT);
        picker.handle_paste("opus");
        models.store(Some(Arc::new(vec![CURRENT.into(), OTHER.into()])));
        let _ = picker.refresh();
        assert_eq!(confirm(&mut picker).spec.as_deref(), Some(OTHER));
    }

    #[test_case(key(KeyCode::Char('!')), ModelTier::Strong; "legacy_strong")]
    #[test_case(key(KeyCode::Char('$')), ModelTier::Compaction; "legacy_compaction")]
    #[test_case(key(KeyCode::Char('€')), ModelTier::Compaction; "euro_compaction")]
    #[test_case(KeyEvent::new(KeyCode::Char('1'), KeyModifiers::SHIFT), ModelTier::Strong; "kitty_strong")]
    #[test_case(KeyEvent::new(KeyCode::Char('4'), KeyModifiers::SHIFT), ModelTier::Compaction; "kitty_compaction")]
    fn tier_shortcut_assigns_and_keeps_picker_open(event: KeyEvent, expected: ModelTier) {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        assert!(
            matches!(picker.handle_key(event), ModelPickerAction::AssignTier(spec, tier) if spec == CURRENT && tier == expected)
        );
        assert!(picker.is_open());
    }

    #[test]
    fn fallback_model_tier_shortcut_can_unassign_after_refresh() {
        let _registry_guard = ModelRegistryTestGuard::new();
        let models = models(&[CURRENT]);
        let mut picker = ModelPicker::new(Arc::clone(&models));
        let draft = ModelSelection {
            spec: Some(Arc::from(FALLBACK)),
            fast: Some(false),
            ..ModelSelection::default()
        };
        picker.open(current(FALLBACK), supported(), draft);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("high");
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.highlighted_spec(), Some(FALLBACK));
        assert_eq!(picker.focus, None);
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('!'))),
            ModelPickerAction::AssignTier(spec, ModelTier::Strong) if spec == FALLBACK
        ));
        model_registry::set_and_persist(
            FALLBACK.to_owned(),
            ModelTier::Strong,
            &_registry_guard.state_dir,
        );
        assert_eq!(picker.refresh(), Dirty::YES);
        assert_eq!(picker.highlighted_spec(), Some(FALLBACK));
        assert_eq!(picker.initial_spec, FALLBACK);
        assert_eq!(picker.focus, None);
        assert_eq!(picker.editors[0].value(), "high");
        assert_eq!(picker.editors[1].value(), "false");
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('!'))),
            ModelPickerAction::UnassignTier(spec, ModelTier::Strong) if spec == FALLBACK
        ));
        assert!(picker.is_open());
        assert_eq!(picker.highlighted_spec(), Some(FALLBACK));
        assert_eq!(picker.initial_spec, FALLBACK);
        assert_eq!(picker.focus, None);
        assert_eq!(picker.editors[0].value(), "high");
        assert_eq!(picker.editors[1].value(), "false");
        assert!(
            !models
                .load()
                .as_ref()
                .unwrap()
                .contains(&FALLBACK.to_owned())
        );
    }

    #[test]
    fn refresh_preserves_selection_for_current_model() {
        let models = models(&[]);
        let mut picker = ModelPicker::new(Arc::clone(&models));
        open(&mut picker, OTHER);
        models.store(Some(Arc::new(vec![
            CURRENT.into(),
            OTHER.into(),
            THIRD.into(),
        ])));
        let _ = picker.refresh();
        assert_eq!(picker.highlighted_spec(), Some(OTHER));
        assert_eq!(confirm(&mut picker), ModelSelection::default());
    }

    #[test_case(false; "provider_anchor")]
    #[test_case(true; "recent_anchor")]
    fn refresh_preserves_recent_and_provider_anchors(recent: bool) {
        let models = test_models();
        let mut picker = ModelPicker::new(Arc::clone(&models));
        picker.set_recents(vec![THIRD.into(), CURRENT.into()]);
        open(&mut picker, CURRENT);
        picker.track_anchor(|picker| {
            picker
                .picker
                .select_item_by(|entry| entry.spec == THIRD && entry.suffix.is_some() == recent);
        });
        models.store(Some(Arc::new(vec![CURRENT.into(), THIRD.into()])));
        let _ = picker.refresh();
        let entry = picker.picker.selected_item().unwrap();
        assert_eq!(entry.spec, THIRD);
        assert_eq!(entry.suffix.is_some(), recent);
    }

    #[test]
    fn refresh_after_collapse_anchors_to_provider_entry() {
        let models = test_models();
        let mut picker = ModelPicker::new(Arc::clone(&models));
        picker.set_recents(vec![THIRD.into(), CURRENT.into()]);
        open(&mut picker, CURRENT);
        models.store(None);
        let _ = picker.refresh();
        assert_eq!(
            picker.picker.selected_item().unwrap().provider_display,
            RECENT_SECTION
        );
        models.store(Some(Arc::new(vec![
            CURRENT.into(),
            OTHER.into(),
            THIRD.into(),
        ])));
        let _ = picker.refresh();
        assert_eq!(
            picker.picker.selected_item().unwrap().provider_display,
            "Anthropic"
        );
    }

    #[test]
    fn reopen_preselects_current_model_in_provider_section() {
        let mut picker = ModelPicker::new(test_models());
        picker.set_recents(vec![THIRD.into(), CURRENT.into()]);
        open(&mut picker, CURRENT);
        picker.picker.select(0);
        assert_eq!(confirm(&mut picker).spec.as_deref(), Some(THIRD));
        open(&mut picker, THIRD);
        assert_eq!(
            picker.picker.selected_item().unwrap().provider_display,
            "Z.AI"
        );
    }

    #[test]
    fn parse_model_entry_preserves_provider_tier_and_price() {
        let entry = parse_model_entry(CURRENT).unwrap();
        assert_eq!(entry.id, "claude-sonnet-4-20250514");
        assert_eq!(entry.provider_display, "Anthropic");
        assert!(!entry.tier.is_empty());
        assert!(!entry.tier.starts_with(FREE_PREFIX));
        assert!(parse_model_entry("no-slash").is_none());
    }

    #[test_case(120, 24; "wide")]
    #[test_case(80, 24; "normal")]
    #[test_case(50, 16; "narrow")]
    #[test_case(80, 10; "short")]
    fn model_picker_renders_options_and_hints(width: u16, height: u16) {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("off");
        let text = render(&mut picker, width, height);
        assert!(text.contains("Thinking: off"));
        assert!(text.contains("Fast: Use current"));
        assert!(text.contains("Enter"));
        assert!(text.contains("Esc"));
    }

    fn injected_specs(count: usize) -> &'static [ModelOptionSpec] {
        let production = &maki_domain::MODEL_OPTION_SPECS[0];
        let specs = (0..count)
            .map(|index| ModelOptionSpec {
                key: Box::leak(format!("injected_{index}").into_boxed_str()),
                label: if count == 1 {
                    EXTRA_LABEL
                } else {
                    Box::leak(format!("Injected {index}").into_boxed_str())
                },
                description: production.description,
                presets: production.presets,
                value_kind: ModelOptionValueKind::FreeInput,
                read: production.read,
                replace: production.replace,
                availability: |capabilities| {
                    if capabilities.resolved {
                        ModelOptionAvailability::Available { note: None }
                    } else {
                        ModelOptionAvailability::Unavailable {
                            reason: MODEL_CAPABILITIES_UNRESOLVED,
                        }
                    }
                },
            })
            .collect::<Vec<_>>();
        Box::leak(specs.into_boxed_slice())
    }

    #[test]
    fn model_option_editor_projects_supplied_specs() {
        let mut picker = ModelPicker::new(test_models());
        picker.specs = injected_specs(1);
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(INVALID);
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert!(render(&mut picker, 120, 24).contains(EXTRA_LABEL));
        picker.handle_key(key(KeyCode::Home));
        picker.handle_key(kb::KILL_LINE.to_key_event());
        picker.handle_paste("high");
        assert!(render(&mut picker, 120, 24).contains("Injected effort: high"));
        let _ = picker.set_capabilities(CURRENT, ModelCapabilities::default());
        assert_eq!(picker.focus, None);
        let text = render(&mut picker, 120, 24);
        assert!(!text.contains("Injected effort:"));
        assert!(text.contains("Injected effort=high"));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        let _ = picker.set_capabilities(CURRENT, supported());
        assert_eq!(
            confirm(&mut picker).thinking,
            Some(ThinkingConfig::Effort(Effort::High))
        );
    }

    #[test_case(80, 10; "short")]
    #[test_case(50, 12; "narrow")]
    fn short_footer_keeps_conflict_and_reset_hints_visible(width: u16, height: u16) {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(INVALID);
        let _ = picker.set_capabilities(
            CURRENT,
            capabilities(ThinkingCapability::Unsupported, FastCapability::Supported),
        );
        let text = render(&mut picker, width, height);
        assert!(text.contains("Thinking="));
        assert!(!text.contains("Thinking:"));
        assert!(text.contains("Ctrl+R"));
        assert!(text.contains("Enter"));
        assert!(text.contains("Esc"));
    }

    #[test]
    fn focused_field_and_validation_error_fit_short_footer() {
        let mut picker = ModelPicker::new(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(INVALID);
        picker.handle_key(key(KeyCode::Enter));
        let text = render(&mut picker, 80, 10);
        assert!(text.contains("Thinking: not-a-value"));
        assert!(text.contains("unknown thinking value"));
        assert!(text.contains("Enter"));
        assert!(text.contains("Esc"));
    }

    #[test]
    fn short_footer_keeps_focused_injected_row_visible() {
        let mut picker = ModelPicker::new(test_models());
        picker.specs = injected_specs(12);
        open(&mut picker, CURRENT);
        for _ in 0..12 {
            picker.handle_key(key(KeyCode::Tab));
        }
        let text = render(&mut picker, 80, 10);
        assert!(text.contains("Injected 11: Use current"));
        assert!(!text.contains("Injected 0:"));
        assert!(text.contains("Enter"));
        assert!(text.contains("Esc"));
    }
}
