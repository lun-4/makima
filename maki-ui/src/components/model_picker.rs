use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::text::{Line, Span};

use maki_providers::model_registry;
use maki_providers::{
    CompleteModelSelection, ModelCapabilities, ModelOption, ModelSelection, ModelTier,
    ProviderRegistry,
};

use crate::click::{ClickTarget, HitTarget, PickerId};
use crate::components::Overlay;
use crate::components::keybindings::key;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem};
use crate::model_capabilities::ModelCapabilityCache;
use crate::repaint::{Cadence, Dirty, Watch};
use crate::text_buffer::{EditResult, TextBuffer};
use crate::theme;

const TITLE: &str = " Models ";
const RECENT_SECTION: &str = "Recent";
const FREE_PREFIX: &str = "Free · ";
const USE_CURRENT: &str = "Use current";
const LOADING_OPTIONS: &str = "   Loading model options…";
const OPTION_HINT: &str = "   ↑/↓ or Space/←/→ cycle values, empty uses current";
const CONFIRM_HINTS: [(&str, KeyCode); 3] = [
    ("  Enter confirm", KeyCode::Enter),
    ("  Esc cancel", KeyCode::Esc),
    ("  Tab options", KeyCode::Tab),
];

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
    providers: Arc<ProviderRegistry>,
    capabilities: Arc<ModelCapabilityCache>,
    draft: Option<Draft>,
    needs_rebuild: bool,
    /// User-moved entry to restore on refresh: `(was_recent, spec)`.
    anchor: Option<(bool, String)>,
}

/// What the open picker edits. Each option keeps its own editor; an empty
/// one means "use current". Editors of options the highlighted model lacks
/// are hidden and left out of the selection, but keep their text for when
/// the highlight moves back to a model that has them.
struct Draft {
    current: ModelSelection,
    current_capabilities: ModelCapabilities,
    initial_spec: String,
    editors: [TextBuffer; ModelOption::ALL.len()],
    focus: Option<ModelOption>,
    error: Option<(ModelOption, String)>,
}

impl Draft {
    fn editor(&self, option: ModelOption) -> &TextBuffer {
        &self.editors[option as usize]
    }

    fn editor_mut(&mut self, option: ModelOption) -> &mut TextBuffer {
        &mut self.editors[option as usize]
    }

    fn has_overrides(&self) -> bool {
        self.editors.iter().any(|editor| !editor.value().is_empty())
    }
}

impl ModelPicker {
    pub fn new(
        models: Arc<ArcSwapOption<Vec<String>>>,
        providers: Arc<ProviderRegistry>,
        capabilities: Arc<ModelCapabilityCache>,
    ) -> Self {
        Self {
            picker: ListPicker::new().with_picker(PickerId::Model),
            models,
            available: Watch::default(),
            recents: Vec::new(),
            providers,
            capabilities,
            draft: None,
            needs_rebuild: false,
            anchor: None,
        }
    }

    pub fn set_recents(&mut self, recents: Vec<String>) {
        self.recents = recents;
        self.needs_rebuild = true;
    }

    /// Opens on `draft`'s model, or the current one, with `draft`'s option
    /// overrides already filled in.
    pub fn open(
        &mut self,
        current: CompleteModelSelection,
        current_capabilities: ModelCapabilities,
        draft: &ModelSelection,
    ) {
        self.draft = Some(Draft {
            initial_spec: draft.spec.as_deref().unwrap_or(&current.spec).to_owned(),
            current: current.into(),
            current_capabilities,
            editors: ModelOption::ALL.map(|option| {
                let mut editor = TextBuffer::new(option.get(draft).unwrap_or_default());
                editor.move_to_end();
                editor
            }),
            focus: None,
            error: None,
        });
        self.anchor = None;
        self.needs_rebuild = false;
        self.available = Watch::seeded(self.models.load_full());
        let entries = self.load_entries();
        self.picker.open(entries, TITLE);
        self.preselect_initial_model();
    }

    /// Polls discovery outside rendering so an unchanged picker stays idle.
    pub fn refresh(&mut self) -> Dirty {
        if !self.picker.is_open() {
            return Dirty::NO;
        }
        let arrived = self.available.poll(self.models.load_full());
        if arrived == Dirty::NO && !self.needs_rebuild {
            return Dirty::NO;
        }
        self.needs_rebuild = false;
        let follow_selection = self.picker.selection_visible();
        let scroll_line = self.picker.scroll_line();
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
        if !follow_selection {
            self.picker.scroll_to_line(scroll_line);
        }
        Dirty::YES
    }

    fn load_entries(&self) -> Vec<ModelEntry> {
        let specs = self.available.get();
        let mut entries = Vec::new();
        for spec in &self.recents {
            if specs.is_none_or(|specs| specs.contains(spec))
                && let Some(mut entry) = parse_model_entry(spec, &self.providers)
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
                    .filter_map(|spec| parse_model_entry(spec, &self.providers))
                    .collect()
            })
            .unwrap_or_default();
        full.sort_by(|a, b| {
            a.provider_display
                .cmp(&b.provider_display)
                .then_with(|| a.id.cmp(&b.id))
        });
        entries.extend(full);
        let draft = self.draft.as_ref();
        let pinned = [
            draft.and_then(|draft| draft.current.spec.as_deref()),
            draft.map(|draft| draft.initial_spec.as_str()),
            self.highlighted_spec(),
        ];
        for spec in pinned.into_iter().flatten() {
            if !entries.iter().any(|entry| entry.spec == spec) {
                entries.push(fallback_model_entry(spec));
            }
        }
        entries
    }

    fn preselect_initial_model(&mut self) {
        let Some(initial) = self.draft.as_ref().map(|draft| draft.initial_spec.clone()) else {
            return;
        };
        if !self
            .picker
            .select_item_by(|entry| entry.spec == initial && entry.suffix().is_none())
        {
            self.picker.select_item_by(|entry| entry.spec == initial);
        }
    }

    pub(crate) fn highlighted_spec(&self) -> Option<&str> {
        self.picker.selected_item().map(|entry| entry.spec.as_str())
    }

    /// Capabilities of the highlighted model; `None` while they resolve.
    fn highlighted_capabilities(&self) -> Option<ModelCapabilities> {
        let draft = self.draft.as_ref()?;
        let spec = self.highlighted_spec()?;
        if draft.current.spec.as_deref() == Some(spec) {
            Some(draft.current_capabilities)
        } else {
            self.capabilities.get(spec)
        }
    }

    fn visible_options(&self) -> Vec<ModelOption> {
        self.highlighted_capabilities()
            .map(|capabilities| {
                ModelOption::ALL
                    .into_iter()
                    .filter(|option| capabilities.supports(*option))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The focused option, if the highlighted model still has it.
    fn focus(&self) -> Option<ModelOption> {
        self.draft
            .as_ref()?
            .focus
            .filter(|option| self.visible_options().contains(option))
    }

    pub fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    pub fn close(&mut self) {
        self.picker.close();
        self.draft = None;
    }

    pub fn select(&mut self, index: usize) -> bool {
        let selected = self.track_anchor(|p| p.picker.select(index));
        if selected && let Some(draft) = self.draft.as_mut() {
            draft.focus = None;
        }
        selected
    }

    pub fn click_input(&mut self, pos: Position) -> bool {
        let clicked = self.picker.click_input(pos);
        if clicked && let Some(draft) = self.draft.as_mut() {
            draft.focus = None;
        }
        clicked
    }

    pub fn scroll(&mut self, delta: i32) {
        self.picker.scroll(delta);
    }

    fn track_anchor<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        let entry = |picker: &Self| {
            picker
                .picker
                .selected_item()
                .map(|entry| (entry.suffix().is_some(), entry.spec.clone()))
        };
        let before = entry(self);
        let result = f(self);
        let after = entry(self);
        if before != after && self.is_open() {
            self.anchor = after;
        }
        result
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        if !self.is_open() {
            return false;
        }
        match self.focus() {
            Some(option) => {
                if option.accepts_free_input()
                    && let Some(draft) = self.draft.as_mut()
                {
                    let text: String = text
                        .chars()
                        .filter(|character| !character.is_control())
                        .collect();
                    draft.editor_mut(option).insert_text(&text);
                    draft.error = None;
                }
                true
            }
            None => self.track_anchor(|picker| picker.picker.handle_paste(text)),
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> ModelPickerAction {
        self.track_anchor(|picker| picker.handle_key_inner(key))
    }

    fn move_focus(&mut self, backward: bool) {
        let visible = self.visible_options();
        let position = self
            .focus()
            .and_then(|focus| visible.iter().position(|option| *option == focus));
        let next = match (position, backward) {
            (None, false) => visible.first().copied(),
            (None, true) => visible.last().copied(),
            (Some(position), false) => visible.get(position + 1).copied(),
            (Some(position), true) => position.checked_sub(1).map(|position| visible[position]),
        };
        if let Some(draft) = self.draft.as_mut() {
            draft.focus = next;
        }
    }

    /// Steps through "use current" (empty) followed by the presets.
    fn cycle_option(&mut self, option: ModelOption, backward: bool) {
        let Some(draft) = self.draft.as_mut() else {
            return;
        };
        let value = draft.editor(option).value();
        let presets = option.presets();
        let count = presets.len() + 1;
        let position = if value.is_empty() {
            Some(0)
        } else {
            presets
                .iter()
                .position(|preset| *preset == value)
                .map(|position| position + 1)
        };
        let next = match (position, backward) {
            (Some(position), false) => (position + 1) % count,
            (Some(position), true) => (position + count - 1) % count,
            (None, false) => 0,
            (None, true) => count - 1,
        };
        let value = next.checked_sub(1).map_or("", |index| presets[index]);
        let editor = draft.editor_mut(option);
        *editor = TextBuffer::new(value.to_owned());
        editor.move_to_end();
        draft.error = None;
    }

    fn confirm(&mut self) -> ModelPickerAction {
        let capabilities = self.highlighted_capabilities();
        let Some(spec) = self.highlighted_spec().map(Arc::<str>::from) else {
            return ModelPickerAction::Consumed;
        };
        let Some(draft) = self.draft.as_mut() else {
            return ModelPickerAction::Consumed;
        };
        let Some(capabilities) =
            capabilities.or_else(|| (!draft.has_overrides()).then_some(ModelCapabilities::NONE))
        else {
            return ModelPickerAction::Consumed;
        };
        let mut selection = ModelSelection {
            spec: (draft.current.spec.as_ref() != Some(&spec)).then_some(spec),
            ..ModelSelection::default()
        };
        for option in ModelOption::ALL
            .into_iter()
            .filter(|option| capabilities.supports(*option))
        {
            let value = draft.editor(option).value();
            if let Err(error) = option.set(&mut selection, (!value.is_empty()).then_some(&*value)) {
                draft.error = Some((option, error.to_string()));
                draft.focus = Some(option);
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
        if let Some(option) = self.focus() {
            self.handle_option_key(option, event);
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

    fn handle_option_key(&mut self, option: ModelOption, event: KeyEvent) {
        let cycles = if option.accepts_free_input() {
            matches!(event.code, KeyCode::Up | KeyCode::Down)
        } else {
            matches!(
                event.code,
                KeyCode::Char(' ') | KeyCode::Left | KeyCode::Right
            )
        };
        if cycles && event.modifiers.is_empty() {
            self.cycle_option(option, matches!(event.code, KeyCode::Up | KeyCode::Left));
        } else if option.accepts_free_input()
            && let Some(draft) = self.draft.as_mut()
            && draft.editor_mut(option).handle_key(event) == EditResult::Changed
        {
            draft.error = None;
        }
    }

    fn option_line(
        &self,
        draft: &Draft,
        option: ModelOption,
        capabilities: ModelCapabilities,
    ) -> Line<'static> {
        let t = theme::current();
        let value = draft.editor(option).value();
        let focused = self.focus() == Some(option);
        let mut spans = vec![
            Span::styled(if focused { " > " } else { "   " }, t.keybind_key),
            Span::styled(format!("{}: ", option.label()), t.keybind_key),
        ];
        if value.is_empty() {
            let current = option.get(&draft.current).unwrap_or_default();
            spans.push(Span::styled(
                format!("{USE_CURRENT} ({current})"),
                t.tool_dim,
            ));
        } else if focused && option.accepts_free_input() {
            let mut characters = value.chars();
            let before: String = characters.by_ref().take(draft.editor(option).x()).collect();
            let at = characters.next().unwrap_or(' ');
            spans.push(Span::raw(before));
            spans.push(Span::styled(at.to_string(), t.cursor));
            spans.push(Span::raw(characters.collect::<String>()));
        } else {
            spans.push(Span::raw(value));
        }
        if let Some(note) = capabilities.note(option) {
            spans.push(Span::styled(format!(" ({note})"), t.tool_dim));
        }
        Line::from(spans)
    }

    fn footer_lines(&self) -> Vec<Line<'static>> {
        let Some(draft) = self.draft.as_ref() else {
            return Vec::new();
        };
        let t = theme::current();
        let capabilities = self.highlighted_capabilities();
        let mut lines: Vec<Line<'static>> = match capabilities {
            Some(capabilities) => self
                .visible_options()
                .into_iter()
                .map(|option| self.option_line(draft, option, capabilities))
                .collect(),
            None => vec![Line::from(Span::styled(LOADING_OPTIONS, t.tool_dim))],
        };
        if let Some((option, error)) = &draft.error {
            lines.push(Line::from(Span::styled(
                format!(" {}: {error}", option.label()),
                t.error,
            )));
        }
        lines.push(match self.focus() {
            Some(_) => Line::from(Span::styled(OPTION_HINT, t.tool_dim)),
            None => footer_line(),
        });
        lines.push(Line::from(
            CONFIRM_HINTS
                .iter()
                .map(|(text, _)| Span::styled(*text, t.keybind_key))
                .collect::<Vec<_>>(),
        ));
        lines
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        let footer = self.footer_lines();
        self.picker.view_with_footer_lines(frame, area, footer)
    }

    pub fn hits(&self) -> Vec<HitTarget> {
        let mut hits = self.picker.hits();
        if let Some(area) = self.picker.footer_area() {
            let lines = self.footer_lines();
            let mut add_hint = |x, row, width, code| {
                let rect = Rect::new(x, area.y.saturating_add(row), width, 1).intersection(area);
                if !rect.is_empty() {
                    hits.push(HitTarget {
                        rect,
                        target: ClickTarget::KeyHint(KeyEvent::new(code, KeyModifiers::NONE)),
                    });
                }
            };
            if self.focus().is_none() {
                let keys = [
                    KeyCode::Char('!'),
                    KeyCode::Char('@'),
                    KeyCode::Char('#'),
                    KeyCode::Char('$'),
                ];
                let line = footer_line();
                let mut x = area.x;
                for (spans, code) in line.spans.as_chunks::<2>().0.iter().zip(keys) {
                    let width = spans.iter().map(Span::width).sum::<usize>() as u16;
                    add_hint(x, (lines.len() - 2) as u16, width, code);
                    x = x.saturating_add(width);
                }
            }
            let mut x = area.x;
            for (text, code) in CONFIRM_HINTS {
                let width = Span::raw(text).width() as u16;
                add_hint(x, (lines.len() - 1) as u16, width, code);
                x = x.saturating_add(width);
            }
        }
        hits
    }

    pub fn scroll_line(&self) -> Option<u16> {
        self.is_open().then(|| self.picker.scroll_line())
    }

    pub fn content_len(&self) -> Option<u16> {
        self.is_open().then(|| self.picker.content_len()).flatten()
    }

    pub fn scroll_to_line(&mut self, line: u16) {
        self.picker.scroll_to_line(line);
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

fn parse_model_entry(spec: &str, providers: &ProviderRegistry) -> Option<ModelEntry> {
    let (provider_str, model_id) = spec.split_once('/')?;
    let provider_display = providers.display_name(provider_str)?.to_string();

    let override_tiers = model_registry::override_tiers(spec);
    let (tier, free) = match providers.model(spec) {
        Ok(m) => (m.tier.to_string(), m.free),
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
    use super::{
        FREE_PREFIX, LOADING_OPTIONS, ModelPicker, ModelPickerAction, RECENT_SECTION,
        parse_model_entry,
    };
    use crate::click::ClickTarget;
    use crate::components::key;
    use crate::components::keybindings::key as kb;
    use crate::model_capabilities::ModelCapabilityCache;
    use crate::model_capabilities::test_support::{THINKING_ONLY, seeded};
    use crate::repaint::Dirty;
    use arc_swap::ArcSwapOption;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use maki_providers::test_util::offline_ids;
    use maki_providers::{
        CompleteModelSelection, Effort, ModelCapabilities, ModelOption, ModelSelection, ModelTier,
        ProviderRegistry, ThinkingConfig, ThinkingSupport, model_registry,
    };
    use maki_storage::StateDir;
    use ratatui::{Terminal, backend::TestBackend, layout::Position};
    use std::sync::{Arc, Mutex, MutexGuard};
    use tempfile::TempDir;
    use test_case::test_case;

    const ANTHROPIC: &str = "anthropic";
    const ZAI: &str = "zai";
    const CURRENT: &str = "anthropic/claude-sonnet-4-20250514";
    const OTHER: &str = "anthropic/claude-opus-4-6-20260101";
    const THIRD: &str = "zai/glm-5";
    const UNRESOLVED: &str = "openai/gpt-5";
    const FALLBACK: &str = "llama-cpp/default";
    const INVALID: &str = "not-a-value";
    const SAME_SIZED_LIST: &str = "a republished list of the same length is still a new list";
    const ALL_OPTIONS: ModelCapabilities = ModelCapabilities {
        thinking: ThinkingSupport::Yes,
        fast: true,
    };

    #[test_case(false; "wheel")]
    #[test_case(true; "absolute")]
    fn refresh_preserves_manual_viewport(absolute: bool) {
        let models = Arc::new(ArcSwapOption::empty());
        let specs: Vec<String> = (0..40).map(|i| format!("zai/model-{i:02}")).collect();
        models.store(Some(Arc::new(specs.clone())));
        let mut picker = picker(Arc::clone(&models));
        open(&mut picker, &specs[0]);
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        if absolute {
            picker.scroll_to_line(20);
        } else {
            picker.scroll(-20);
        }
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        let offset = picker.scroll_line();
        assert!(offset.unwrap() > 0);
        models.store(Some(Arc::new(specs)));
        assert_eq!(picker.refresh(), Dirty::YES);
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        assert_eq!(picker.scroll_line(), offset);
        assert_eq!(picker.picker.selected_item().unwrap().spec, "zai/model-00");
        picker.handle_key(key(KeyCode::Down));
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        assert!(picker.scroll_line() < offset);
    }

    #[test_case(80; "wide")]
    #[test_case(30; "clipped")]
    fn rendered_footer_emits_clipped_key_hints(width: u16) {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        let mut terminal = Terminal::new(TestBackend::new(width, 24)).unwrap();
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        let area = picker.picker.footer_area().unwrap();
        let hints: Vec<_> = picker
            .hits()
            .into_iter()
            .filter(|hit| matches!(hit.target, ClickTarget::KeyHint(_)))
            .collect();
        assert!(hints.len() >= 2);
        assert!(
            hints
                .iter()
                .all(|hit| hit.rect.intersection(area) == hit.rect)
        );
        assert_eq!(
            hints[0].target,
            ClickTarget::KeyHint(key(KeyCode::Char('!')))
        );
        let confirm = hints
            .iter()
            .find(|hit| hit.target == ClickTarget::KeyHint(key(KeyCode::Enter)))
            .unwrap();
        assert_eq!(confirm.rect.y, area.bottom() - 1);
        assert_eq!(hints[0].rect.y, confirm.rect.y - 1);
    }

    #[test_case(6, false; "clipped_tiers")]
    #[test_case(6, true; "clipped_focused")]
    #[test_case(24, true; "focused")]
    fn footer_hits_only_cover_visible_hints(height: u16, focused: bool) {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        if focused {
            picker.handle_key(key(KeyCode::Tab));
        }
        render(&mut picker, 80, height);
        let area = picker.picker.footer_area().unwrap();
        let lines = picker.footer_lines();
        let hints: Vec<_> = picker
            .hits()
            .into_iter()
            .filter(|hit| matches!(hit.target, ClickTarget::KeyHint(_)))
            .collect();
        for hit in &hints {
            assert_eq!(hit.rect.intersection(area), hit.rect);
            let row = usize::from(hit.rect.y - area.y);
            assert!(row >= lines.len() - 2);
            if focused {
                assert_eq!(row, lines.len() - 1);
            }
        }
        if usize::from(area.height) < lines.len() - 1 {
            assert!(hints.is_empty());
        }
    }

    #[test_case(false; "row")]
    #[test_case(true; "search")]
    fn mouse_selection_returns_focus_to_model_list(search: bool) {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        render(&mut picker, 80, 24);
        if search {
            let area = picker
                .hits()
                .into_iter()
                .find(|hit| matches!(hit.target, ClickTarget::PickerInput(_)))
                .unwrap()
                .rect;
            assert!(picker.click_input(Position::new(area.x, area.y)));
        } else {
            assert!(picker.select(picker.picker.selected_index().unwrap()));
        }
        assert_eq!(picker.focus(), None);
    }

    fn models(specs: &[&str]) -> Arc<ArcSwapOption<Vec<String>>> {
        Arc::new(ArcSwapOption::from(Some(Arc::new(
            specs.iter().map(|spec| (*spec).to_owned()).collect(),
        ))))
    }

    fn test_models() -> Arc<ArcSwapOption<Vec<String>>> {
        models(&[CURRENT, OTHER, THIRD])
    }

    /// `OTHER` supports everything, `THIRD` only thinking, `UNRESOLVED` never resolves.
    fn cache() -> Arc<ModelCapabilityCache> {
        seeded(
            &[(OTHER, ALL_OPTIONS), (THIRD, THINKING_ONLY)],
            &[UNRESOLVED],
        )
    }

    fn test_providers() -> Arc<ProviderRegistry> {
        Arc::new(offline_ids(&[ANTHROPIC, ZAI, "openai", "llama-cpp"]))
    }

    fn picker(models: Arc<ArcSwapOption<Vec<String>>>) -> ModelPicker {
        ModelPicker::new(models, test_providers(), cache())
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
            let previous_strong = model_registry::tier_override(ModelTier::Strong);
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

    fn open(picker: &mut ModelPicker, spec: &str) {
        picker.open(current(spec), ALL_OPTIONS, &ModelSelection::default());
    }

    fn editor(picker: &ModelPicker, option: ModelOption) -> String {
        picker.draft.as_ref().unwrap().editor(option).value()
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

    fn highlight(picker: &mut ModelPicker, spec: &str) {
        picker.track_anchor(|picker| {
            picker
                .picker
                .select_item_by(|entry| entry.spec == spec && entry.suffix.is_none())
        });
        assert_eq!(picker.highlighted_spec(), Some(spec));
    }

    #[test]
    fn a_same_sized_model_list_owes_a_frame() {
        let models = models(&[CURRENT]);
        let mut picker = picker(Arc::clone(&models));
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
    fn cancel_discards_draft(cancel: KeyEvent) {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("high");
        assert!(matches!(
            picker.handle_key(cancel),
            ModelPickerAction::Close
        ));
        assert!(!picker.is_open());
        open(&mut picker, CURRENT);
        assert_eq!(confirm(&mut picker), ModelSelection::default());
    }

    #[test_case("high", ThinkingConfig::Effort(Effort::High); "effort")]
    #[test_case("2048", ThinkingConfig::Budget(2048); "budget")]
    #[test_case("off", ThinkingConfig::Off; "explicit_off")]
    fn confirms_structured_selection(value: &str, expected: ThinkingConfig) {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(value);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Left));
        assert_eq!(
            confirm(&mut picker),
            ModelSelection {
                spec: None,
                thinking: Some(expected),
                fast: Some(false),
            }
        );
        assert!(!picker.is_open());
    }

    #[test]
    fn staged_current_identity_is_not_an_override() {
        let mut picker = picker(test_models());
        let draft = ModelSelection {
            spec: Some(Arc::from(CURRENT)),
            fast: Some(true),
            ..ModelSelection::default()
        };
        picker.open(current(CURRENT), ALL_OPTIONS, &draft);
        assert_eq!(
            confirm(&mut picker),
            ModelSelection {
                fast: Some(true),
                ..ModelSelection::default()
            }
        );
    }

    #[test_case(INVALID; "syntax")]
    #[test_case("0"; "zero_budget")]
    fn invalid_option_keeps_draft_open(value: &str) {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste(value);
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert!(picker.is_open());
        assert_eq!(editor(&picker, ModelOption::Thinking), value);
        assert!(render(&mut picker, 120, 24).contains("Thinking: "));
        picker.handle_key(key(KeyCode::Home));
        picker.handle_key(kb::KILL_LINE.to_key_event());
        picker.handle_paste("high");
        assert_eq!(
            confirm(&mut picker).thinking,
            Some(ThinkingConfig::Effort(Effort::High))
        );
    }

    #[test]
    fn focus_routes_keys_and_paste() {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.focus(), Some(ModelOption::Thinking));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('!'))),
            ModelPickerAction::Consumed
        ));
        assert_eq!(editor(&picker, ModelOption::Thinking), "!");
        picker.handle_key(key(KeyCode::Backspace));
        picker.handle_paste("2048\n");
        assert_eq!(editor(&picker, ModelOption::Thinking), "2048");
        picker.handle_key(key(KeyCode::Down));
        assert_eq!(editor(&picker, ModelOption::Thinking), "");
        picker.handle_key(key(KeyCode::Up));
        assert_eq!(
            editor(&picker, ModelOption::Thinking),
            *ModelOption::Thinking.presets().last().unwrap()
        );
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("ignored");
        assert_eq!(editor(&picker, ModelOption::Fast), "");
        picker.handle_key(key(KeyCode::Right));
        assert_eq!(editor(&picker, ModelOption::Fast), "true");
        picker.handle_key(key(KeyCode::Char(' ')));
        assert_eq!(editor(&picker, ModelOption::Fast), "false");
        picker.handle_key(key(KeyCode::Right));
        assert_eq!(editor(&picker, ModelOption::Fast), "");
        picker.handle_key(key(KeyCode::Tab));
        assert_eq!(picker.focus(), None);
        picker.handle_key(key(KeyCode::BackTab));
        assert_eq!(picker.focus(), Some(ModelOption::Fast));
    }

    #[test]
    fn hidden_options_keep_drafts_but_are_not_submitted() {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_key(key(KeyCode::Right));
        highlight(&mut picker, THIRD);
        assert_eq!(picker.visible_options(), [ModelOption::Thinking]);
        assert_eq!(picker.focus(), None);
        assert!(!render(&mut picker, 120, 24).contains("Fast:"));
        highlight(&mut picker, OTHER);
        assert_eq!(editor(&picker, ModelOption::Fast), "true");
        highlight(&mut picker, THIRD);
        assert_eq!(
            confirm(&mut picker),
            ModelSelection {
                spec: Some(Arc::from(THIRD)),
                ..ModelSelection::default()
            }
        );
    }

    #[test]
    fn unresolved_capabilities_hold_back_only_drafts() {
        let mut picker = picker(models(&[CURRENT, UNRESOLVED]));
        open(&mut picker, CURRENT);
        highlight(&mut picker, UNRESOLVED);
        assert!(render(&mut picker, 120, 24).contains(LOADING_OPTIONS.trim()));
        assert_eq!(confirm(&mut picker).spec.as_deref(), Some(UNRESOLVED));

        let draft = ModelSelection {
            spec: Some(Arc::from(UNRESOLVED)),
            thinking: Some(ThinkingConfig::Off),
            ..ModelSelection::default()
        };
        picker.open(current(CURRENT), ALL_OPTIONS, &draft);
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert!(picker.is_open());
    }

    #[test]
    fn required_thinking_shows_its_note() {
        let mut picker = picker(test_models());
        let required = ModelCapabilities {
            thinking: ThinkingSupport::Required,
            fast: false,
        };
        picker.open(current(CURRENT), required, &ModelSelection::default());
        assert!(render(&mut picker, 120, 24).contains("off uses minimal effort"));
    }

    #[test]
    fn refresh_preserves_option_draft() {
        let models = test_models();
        let mut picker = picker(Arc::clone(&models));
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("high");
        models.store(Some(Arc::new(vec![CURRENT.into(), THIRD.into()])));
        assert_eq!(picker.refresh(), Dirty::YES);
        assert_eq!(picker.focus(), Some(ModelOption::Thinking));
        assert_eq!(editor(&picker, ModelOption::Thinking), "high");
        assert_eq!(picker.highlighted_spec(), Some(CURRENT));
        assert_eq!(picker.refresh(), Dirty::NO);
    }

    #[test]
    fn staged_undiscovered_identity_survives_refresh() {
        let models = test_models();
        let mut picker = picker(Arc::clone(&models));
        let draft = ModelSelection {
            spec: Some(Arc::from(UNRESOLVED)),
            ..ModelSelection::default()
        };
        picker.open(current(CURRENT), ALL_OPTIONS, &draft);
        assert_eq!(picker.highlighted_spec(), Some(UNRESOLVED));
        models.store(Some(Arc::new(vec![THIRD.into()])));
        let _ = picker.refresh();
        assert_eq!(picker.highlighted_spec(), Some(UNRESOLVED));
        assert_eq!(confirm(&mut picker).spec.as_deref(), Some(UNRESOLVED));
    }

    #[test]
    fn unmatched_search_does_not_confirm() {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_paste("unmatched-search");
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Consumed
        ));
        assert!(picker.is_open());
    }

    #[test]
    fn refresh_updates_items_and_preserves_search() {
        let models = models(&[CURRENT]);
        let mut picker = picker(Arc::clone(&models));
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
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        assert!(
            matches!(picker.handle_key(event), ModelPickerAction::AssignTier(spec, tier) if spec == CURRENT && tier == expected)
        );
        assert!(picker.is_open());
    }

    #[test]
    fn tier_shortcuts_type_into_focused_option() {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('$'))),
            ModelPickerAction::Consumed
        ));
        assert_eq!(editor(&picker, ModelOption::Thinking), "$");
    }

    #[test]
    fn fallback_model_tier_shortcut_can_unassign_after_refresh() {
        let registry_guard = ModelRegistryTestGuard::new();
        let models = models(&[CURRENT]);
        let mut picker = picker(Arc::clone(&models));
        open(&mut picker, FALLBACK);
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('!'))),
            ModelPickerAction::AssignTier(spec, ModelTier::Strong) if spec == FALLBACK
        ));
        model_registry::set_and_persist(
            FALLBACK.to_owned(),
            ModelTier::Strong,
            &registry_guard.state_dir,
        );
        assert_eq!(picker.refresh(), Dirty::YES);
        assert_eq!(picker.highlighted_spec(), Some(FALLBACK));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('!'))),
            ModelPickerAction::UnassignTier(spec, ModelTier::Strong) if spec == FALLBACK
        ));
        assert!(picker.is_open());
    }

    #[test]
    fn refresh_preserves_selection_for_current_model() {
        let models = models(&[]);
        let mut picker = picker(Arc::clone(&models));
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
        let mut picker = picker(Arc::clone(&models));
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
        let mut picker = picker(Arc::clone(&models));
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
            ANTHROPIC
        );
    }

    #[test]
    fn reopen_preselects_current_model_in_provider_section() {
        let mut picker = picker(test_models());
        picker.set_recents(vec![THIRD.into(), CURRENT.into()]);
        open(&mut picker, CURRENT);
        picker.picker.select(0);
        assert_eq!(confirm(&mut picker).spec.as_deref(), Some(THIRD));
        open(&mut picker, THIRD);
        assert_eq!(picker.picker.selected_item().unwrap().provider_display, ZAI);
    }

    #[test]
    fn parse_model_entry_preserves_provider_tier_and_price() {
        let entry = parse_model_entry(CURRENT, &test_providers()).unwrap();
        assert_eq!(entry.id, "claude-sonnet-4-20250514");
        assert_eq!(entry.provider_display, ANTHROPIC);
        assert!(!entry.tier.is_empty());
        assert!(!entry.tier.starts_with(FREE_PREFIX));
        assert!(parse_model_entry("no-slash", &ProviderRegistry::default()).is_none());
    }

    #[test_case(120, 24; "wide")]
    #[test_case(50, 16; "narrow")]
    fn renders_options_and_hints(width: u16, height: u16) {
        let mut picker = picker(test_models());
        open(&mut picker, CURRENT);
        picker.handle_key(key(KeyCode::Tab));
        picker.handle_paste("off");
        let text = render(&mut picker, width, height);
        assert!(text.contains("Thinking: off"));
        assert!(text.contains("Fast: Use current"));
        assert!(text.contains("Enter"));
    }
}
