use crate::click::{HitTarget, PickerId};
use crate::components::Overlay;
use crate::components::list_picker::{ListPicker, PickerAction};
use crate::repaint::Cadence;
use crate::theme::{ThemesProvider, apply_theme};

use std::sync::Arc;

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::{Position, Rect};

const TITLE: &str = " Themes ";
const MAX_VISIBLE: u16 = 15;

pub enum ThemePickerAction {
    Consumed,
    Closed,
}

pub struct ThemePicker {
    picker: ListPicker<String>,
    provider: Arc<dyn ThemesProvider>,
    original_theme_name: Option<String>,
}

impl ThemePicker {
    pub fn new(provider: Arc<dyn ThemesProvider>) -> Self {
        Self {
            picker: ListPicker::new()
                .with_picker(PickerId::Theme)
                .with_max_visible(MAX_VISIBLE),
            provider,
            original_theme_name: None,
        }
    }

    pub fn open(&mut self) {
        let current_name = self.provider.current_theme_name();
        let entries = self.provider.names();
        let current_idx = entries
            .iter()
            .position(|name| *name == current_name)
            .unwrap_or(0);
        self.original_theme_name = Some(current_name);
        self.picker.open(entries, TITLE);
        self.picker.select(current_idx);
    }

    pub fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    pub fn close(&mut self) {
        self.picker.close();
        self.original_theme_name = None;
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> ThemePickerAction {
        match self.picker.handle_key(key) {
            PickerAction::Consumed => {
                self.apply_preview();
                ThemePickerAction::Consumed
            }
            PickerAction::Select(name) => {
                apply_theme(self.provider.as_ref(), &name);
                self.provider.persist(&name);
                self.original_theme_name = None;
                ThemePickerAction::Closed
            }
            PickerAction::Close => {
                self.restore_original();
                self.original_theme_name = None;
                ThemePickerAction::Closed
            }
            PickerAction::Toggle(..) | PickerAction::Delete(..) => ThemePickerAction::Consumed,
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        self.picker.view(frame, area)
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        let consumed = self.picker.handle_paste(text);
        if consumed {
            self.apply_preview();
        }
        consumed
    }

    fn apply_preview(&self) {
        if let Some(name) = self.picker.selected_item() {
            apply_theme(self.provider.as_ref(), name);
        }
    }

    fn restore_original(&self) {
        if let Some(ref name) = self.original_theme_name {
            apply_theme(self.provider.as_ref(), name);
        }
    }
    pub fn click_input(&mut self, pos: Position) -> bool {
        self.picker.click_input(pos)
    }

    pub fn select(&mut self, index: usize) -> bool {
        self.picker.select(index)
    }

    pub fn scroll(&mut self, delta: i32) {
        self.picker.scroll(delta);
    }

    pub fn hits(&self) -> Vec<HitTarget> {
        if self.is_open() {
            self.picker.hits()
        } else {
            Vec::new()
        }
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

impl Overlay for ThemePicker {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::key;
    use crate::components::keybindings::key as kb;
    use crate::theme::Theme;
    use crate::theme::{InMemoryThemesProvider, theme_test_guard};
    use crossterm::event::KeyCode;
    use std::sync::Mutex;
    use test_case::test_case;

    struct RecordingProvider {
        inner: InMemoryThemesProvider,
        events: Mutex<Vec<(&'static str, String)>>,
    }

    impl ThemesProvider for RecordingProvider {
        fn names(&self) -> Vec<String> {
            self.inner.names()
        }
        fn load(&self, name: &str) -> Result<Theme, String> {
            self.inner.load(name)
        }
        fn current_name(&self) -> Option<String> {
            self.inner.current_name()
        }
        fn persisted_name(&self) -> Option<String> {
            self.inner.persisted_name()
        }
        fn select(&self, name: &str) {
            self.events
                .lock()
                .unwrap()
                .push(("select", name.to_owned()));
            self.inner.select(name);
        }
        fn persist(&self, name: &str) {
            self.events
                .lock()
                .unwrap()
                .push(("persist", name.to_owned()));
            self.inner.persist(name);
        }
        fn install(&self, name: &str) -> Result<(), String> {
            self.inner.install(name)
        }
        fn generation(&self) -> u64 {
            self.inner.generation()
        }
    }

    #[test]
    fn mouse_selection_is_applied_before_confirmation_persists() {
        let provider = Arc::new(RecordingProvider {
            inner: InMemoryThemesProvider::bundled(),
            events: Mutex::new(Vec::new()),
        });
        let mut picker = ThemePicker::new(provider.clone());
        picker.open();
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        let original = provider.current_theme_name();
        let next = provider
            .names()
            .into_iter()
            .position(|name| name != original)
            .unwrap();
        assert!(picker.select(next));
        let selected = picker.picker.selected_item().unwrap().clone();
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            ThemePickerAction::Closed
        ));
        assert_eq!(
            *provider.events.lock().unwrap(),
            vec![("select", selected.clone()), ("persist", selected.clone())]
        );
        assert_eq!(provider.current_name(), Some(selected.clone()));
        assert_eq!(provider.persisted_name(), Some(selected));
    }

    #[test]
    fn enter_closes() {
        let _guard = theme_test_guard();
        let mut p = ThemePicker::new(Arc::new(InMemoryThemesProvider::bundled()));
        p.open();
        let action = p.handle_key(key(KeyCode::Enter));
        assert!(matches!(action, ThemePickerAction::Closed));
        assert!(!p.is_open());
    }

    #[test_case(key(KeyCode::Esc) ; "escape_restores_and_closes")]
    #[test_case(kb::QUIT.to_key_event() ; "ctrl_c_restores_and_closes")]
    fn cancel_restores(cancel_key: crossterm::event::KeyEvent) {
        let _guard = theme_test_guard();
        let mut p = ThemePicker::new(Arc::new(InMemoryThemesProvider::bundled()));
        p.open();
        p.handle_key(key(KeyCode::Down));
        let action = p.handle_key(cancel_key);
        assert!(matches!(action, ThemePickerAction::Closed));
        assert!(!p.is_open());
    }
}
