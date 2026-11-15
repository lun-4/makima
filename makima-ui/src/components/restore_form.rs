use crate::click::{ClickTarget, HitTarget};
use crate::components::form::selected_prefix;
use crate::components::modal::{CHROME_LINES, Modal};
use crate::components::plan_form::wrapped_key_hits;
use crate::components::scrollbar::saturating_u16;
use crate::components::{ModalScroll, Overlay, hint_line};
use crate::theme;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
pub use makima_storage::restore::RestoreChoice;
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use std::path::PathBuf;

const TITLE: &str = " Restore ";
const WIDTH_PERCENT: u16 = 80;
const MAX_HEIGHT_PERCENT: u16 = 80;
const SHELL_NOTE: &str = "Shell commands are not reverted.";
const FOOTER_HINTS: &[(&str, &str)] = &[
    ("Enter", "restore"),
    ("Esc", "cancel"),
    ("PgUp/PgDn", "scroll preview"),
];
const CHOICES: &[(RestoreChoice, char, &str)] = &[
    (RestoreChoice::ConversationOnly, '1', "Conversation"),
    (
        RestoreChoice::ConversationAndFiles,
        '2',
        "Conversation and files",
    ),
    (RestoreChoice::FilesOnly, '3', "Files only"),
];

#[derive(Debug, Clone, Default)]
pub struct RestorePreview {
    pub affected_paths: Vec<PathBuf>,
    pub shell_count: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RestoreFormAction {
    Consumed,
    Confirm(RestoreChoice),
    Close,
}

pub struct RestoreForm {
    preview: Option<RestorePreview>,
    selected: RestoreChoice,
    scroll: ModalScroll,
}

impl Default for RestoreForm {
    fn default() -> Self {
        Self::new()
    }
}

impl RestoreForm {
    pub fn new() -> Self {
        Self {
            preview: None,
            selected: RestoreChoice::ConversationOnly,
            scroll: ModalScroll::new_top(),
        }
    }

    pub fn open(&mut self, preview: RestorePreview, default_choice: RestoreChoice) {
        self.preview = Some(preview);
        self.selected = default_choice;
        self.scroll = ModalScroll::new_top();
    }

    pub fn close(&mut self) {
        self.preview = None;
        self.scroll = ModalScroll::new_top();
    }

    pub fn is_open(&self) -> bool {
        self.preview.is_some()
    }

    pub fn scroll(&mut self, delta: i32) {
        self.scroll.scroll(delta);
    }

    pub fn handle_key(&mut self, key_event: KeyEvent) -> RestoreFormAction {
        if !self.is_open() {
            return RestoreFormAction::Consumed;
        }
        match key_event.code {
            KeyCode::Esc => {
                self.close();
                return RestoreFormAction::Close;
            }
            KeyCode::Enter => {
                let choice = self.selected.clone();
                self.close();
                return RestoreFormAction::Confirm(choice);
            }
            KeyCode::Up | KeyCode::Left | KeyCode::Down | KeyCode::Right => {
                let index = CHOICES
                    .iter()
                    .position(|(choice, _, _)| *choice == self.selected)
                    .unwrap_or_default();
                let step = if matches!(key_event.code, KeyCode::Up | KeyCode::Left) {
                    CHOICES.len() - 1
                } else {
                    1
                };
                self.selected = CHOICES[(index + step) % CHOICES.len()].0.clone();
                self.scroll.scroll_to(0);
            }
            KeyCode::Char(number @ '1'..='3') => {
                if let Some((choice, _, _)) = CHOICES.iter().find(|(_, key, _)| *key == number) {
                    self.selected = choice.clone();
                    self.scroll.scroll_to(0);
                }
            }
            KeyCode::PageUp => {
                self.scroll
                    .handle_key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL));
            }
            KeyCode::PageDown => {
                self.scroll
                    .handle_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL));
            }
            KeyCode::Home => self.scroll.scroll_to(0),
            KeyCode::End => self.scroll.scroll_to(u16::MAX),
            _ => {
                self.scroll.handle_key(key_event);
            }
        }
        RestoreFormAction::Consumed
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> (Rect, Vec<HitTarget>) {
        let Some(preview) = self.preview.as_ref() else {
            return (Rect::default(), Vec::new());
        };
        let t = theme::current();
        let mut lines: Vec<Line<'static>> = CHOICES
            .iter()
            .map(|(choice, number, label)| {
                let (prefix, style) = selected_prefix(&t, *choice == self.selected);
                Line::from(vec![
                    Span::styled(prefix, style),
                    Span::styled(format!("{number}. {label}"), style),
                ])
            })
            .collect();
        lines.push(Line::default());
        lines.push(Line::from(format!(
            "Shell commands: {}",
            preview.shell_count
        )));
        lines.push(Line::styled(SHELL_NOTE, t.tool_dim));
        lines.push(Line::default());
        lines.push(Line::from(format!(
            "Affected files: {}",
            preview.affected_paths.len()
        )));
        lines.extend(
            preview
                .affected_paths
                .iter()
                .map(|path| Line::from(format!("  {}", path.display()))),
        );
        let footer = hint_line(FOOTER_HINTS);
        let [width_area] = Layout::horizontal([Constraint::Percentage(WIDTH_PERCENT)])
            .flex(Flex::Center)
            .areas(area);
        let width = width_area.width.saturating_sub(CHROME_LINES).max(1);
        let content = Paragraph::new(lines.clone()).wrap(Wrap { trim: false });
        let footer_widget = Paragraph::new(footer.clone()).wrap(Wrap { trim: false });
        let height = saturating_u16(content.line_count(width))
            .saturating_add(saturating_u16(footer_widget.line_count(width)));
        let (outer, inner) = Modal {
            title: TITLE,
            width_percent: WIDTH_PERCENT,
            max_height_percent: MAX_HEIGHT_PERCENT,
        }
        .render(
            frame,
            area,
            height.min(area.height.saturating_sub(CHROME_LINES)),
        );
        if inner.is_empty() {
            return (outer, Vec::new());
        }
        let footer_height = saturating_u16(footer_widget.line_count(inner.width))
            .min(inner.height.saturating_sub(1));
        let viewport = Rect {
            height: inner.height.saturating_sub(footer_height),
            ..inner
        };
        let footer_area = Rect {
            y: viewport.bottom(),
            height: footer_height,
            ..inner
        };
        self.scroll.update_dimensions(
            saturating_u16(content.line_count(inner.width)),
            viewport.height,
        );
        frame.render_widget(content.scroll((self.scroll.offset(), 0)), viewport);
        frame.render_widget(footer_widget, footer_area);

        let mut hits = Vec::new();
        let mut row = 0i32;
        for (line, (_, number, _)) in lines.iter().zip(CHOICES) {
            let rows = saturating_u16(
                Paragraph::new(line.clone())
                    .wrap(Wrap { trim: false })
                    .line_count(inner.width),
            );
            let top = i32::from(viewport.y) + row - i32::from(self.scroll.offset());
            let bottom = (top + i32::from(rows)).min(i32::from(viewport.bottom()));
            let top = top.max(i32::from(viewport.y));
            if bottom > top {
                hits.push(HitTarget {
                    rect: Rect::new(
                        viewport.x,
                        top as u16,
                        viewport.width,
                        (bottom - top) as u16,
                    ),
                    target: ClickTarget::KeyHint(KeyEvent::new(
                        KeyCode::Char(*number),
                        KeyModifiers::NONE,
                    )),
                });
            }
            row += i32::from(rows);
        }
        let mut offset = 0;
        let ranges: Vec<_> = FOOTER_HINTS
            .iter()
            .zip([KeyCode::Enter, KeyCode::Esc])
            .map(|((key, description), code)| {
                let start = offset + 2;
                offset = start + key.len() + 1 + description.len();
                (start..offset, KeyEvent::new(code, KeyModifiers::NONE))
            })
            .collect();
        hits.extend(wrapped_key_hits(&footer, &ranges, footer_area));
        (outer, hits)
    }
}

impl Overlay for RestoreForm {
    fn is_open(&self) -> bool {
        self.is_open()
    }

    fn close(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::{RestoreChoice, RestoreForm, RestoreFormAction, RestorePreview, SHELL_NOTE};
    use crate::click::{ClickTarget, HitTarget};
    use crate::components::{Overlay, buffer_text, key};
    use crossterm::event::KeyCode;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Rect;
    use std::path::PathBuf;
    use test_case::test_case;

    const PATH: &str = "src/restore.rs";
    const SHELL_COUNT: usize = 7;
    const WIDTH: u16 = 100;
    const HEIGHT: u16 = 30;

    fn preview() -> RestorePreview {
        RestorePreview {
            affected_paths: vec![PathBuf::from(PATH)],
            shell_count: SHELL_COUNT,
        }
    }

    fn render(form: &mut RestoreForm, width: u16, height: u16) -> (String, Rect, Vec<HitTarget>) {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        let mut outer = Rect::default();
        let mut hits = Vec::new();
        terminal
            .draw(|frame| {
                (outer, hits) = form.view(frame, frame.area());
            })
            .unwrap();
        (buffer_text(terminal.backend().buffer()), outer, hits)
    }

    #[test_case(RestoreChoice::ConversationOnly)]
    #[test_case(RestoreChoice::ConversationAndFiles)]
    #[test_case(RestoreChoice::FilesOnly)]
    fn confirms_explicit_default_and_reopens(choice: RestoreChoice) {
        let mut form = RestoreForm::new();
        assert!(!form.is_open());
        form.open(preview(), choice.clone());
        assert!(form.is_open());
        assert_eq!(
            form.handle_key(key(KeyCode::Enter)),
            RestoreFormAction::Confirm(choice)
        );
        assert!(!form.is_open());
        form.open(preview(), RestoreChoice::ConversationOnly);
        assert_eq!(
            form.handle_key(key(KeyCode::Enter)),
            RestoreFormAction::Confirm(RestoreChoice::ConversationOnly)
        );
    }

    #[test_case(KeyCode::Up, RestoreChoice::FilesOnly)]
    #[test_case(KeyCode::Left, RestoreChoice::FilesOnly)]
    #[test_case(KeyCode::Down, RestoreChoice::ConversationAndFiles)]
    #[test_case(KeyCode::Right, RestoreChoice::ConversationAndFiles)]
    #[test_case(KeyCode::Char('1'), RestoreChoice::ConversationOnly)]
    #[test_case(KeyCode::Char('2'), RestoreChoice::ConversationAndFiles)]
    #[test_case(KeyCode::Char('3'), RestoreChoice::FilesOnly)]
    #[test_case(KeyCode::Tab, RestoreChoice::ConversationOnly)]
    #[test_case(KeyCode::Char('x'), RestoreChoice::ConversationOnly)]
    fn selection_never_confirms_early(code: KeyCode, expected: RestoreChoice) {
        let mut form = RestoreForm::new();
        form.open(preview(), RestoreChoice::ConversationOnly);
        assert_eq!(form.handle_key(key(code)), RestoreFormAction::Consumed);
        assert!(form.is_open());
        assert_eq!(
            form.handle_key(key(KeyCode::Enter)),
            RestoreFormAction::Confirm(expected)
        );
    }

    #[test]
    fn arrows_cycle_and_cancel_clears_preview() {
        let mut form = RestoreForm::new();
        form.open(preview(), RestoreChoice::FilesOnly);
        assert_eq!(
            form.handle_key(key(KeyCode::Down)),
            RestoreFormAction::Consumed
        );
        assert_eq!(form.selected, RestoreChoice::ConversationOnly);
        assert_eq!(form.handle_key(key(KeyCode::Esc)), RestoreFormAction::Close);
        assert!(!form.is_open());
        assert_eq!(render(&mut form, WIDTH, HEIGHT).1, Rect::default());
        form.open(preview(), RestoreChoice::ConversationAndFiles);
        Overlay::close(&mut form);
        assert!(!Overlay::is_open(&form));
    }

    #[test]
    fn preview_renders_paths_shell_count_and_warning() {
        let mut form = RestoreForm::new();
        form.open(preview(), RestoreChoice::ConversationAndFiles);
        let (text, _, hits) = render(&mut form, WIDTH, HEIGHT);
        for expected in [
            PATH,
            SHELL_NOTE,
            &format!("Shell commands: {SHELL_COUNT}"),
            "Affected files: 1",
            "Conversation and files",
            "Files only",
        ] {
            assert!(text.contains(expected), "missing {expected}");
        }
        for code in [
            KeyCode::Char('1'),
            KeyCode::Char('2'),
            KeyCode::Char('3'),
            KeyCode::Enter,
            KeyCode::Esc,
        ] {
            assert!(
                hits.iter()
                    .any(|hit| hit.target == ClickTarget::KeyHint(key(code)))
            );
        }
    }

    #[test_case(12, 8)]
    #[test_case(35, 15)]
    #[test_case(WIDTH, HEIGHT)]
    fn hits_stay_inside_modal_and_do_not_leak_after_scroll(width: u16, height: u16) {
        let mut form = RestoreForm::new();
        let mut preview = preview();
        preview.affected_paths = (0..30)
            .map(|index| PathBuf::from(format!("directory/{index}/{PATH}")))
            .collect();
        form.open(preview, RestoreChoice::ConversationOnly);
        let (_, outer, hits) = render(&mut form, width, height);
        let inner = Rect::new(
            outer.x + 1,
            outer.y + 1,
            outer.width.saturating_sub(2),
            outer.height.saturating_sub(2),
        );
        for hit in hits {
            assert!(!hit.rect.is_empty());
            assert_eq!(hit.rect.intersection(inner), hit.rect);
        }
        form.handle_key(key(KeyCode::End));
        let (_, _, hits) = render(&mut form, width, height);
        assert!(hits.iter().all(|hit| !matches!(
            hit.target,
            ClickTarget::KeyHint(event) if matches!(event.code, KeyCode::Char('1'..='3'))
        )));
    }

    #[test]
    fn scrolling_reaches_every_wrapped_path_and_keeps_confirmation_visible() {
        const PATH_COUNT: usize = 20;
        const LONG_DIRECTORY: &str = "long_directory_name/";
        const END_MARKER: &str = "last_path_marker";
        let paths: Vec<_> = (0..PATH_COUNT)
            .map(|index| {
                PathBuf::from(format!(
                    "path_{index:02}_{LONG_DIRECTORY}{LONG_DIRECTORY}{END_MARKER}"
                ))
            })
            .collect();
        let mut form = RestoreForm::new();
        form.open(
            RestorePreview {
                affected_paths: paths,
                shell_count: SHELL_COUNT,
            },
            RestoreChoice::FilesOnly,
        );
        let mut seen = String::new();
        loop {
            let (text, _, hits) = render(&mut form, 55, 20);
            seen.push_str(&text);
            for code in [KeyCode::Enter, KeyCode::Esc] {
                assert!(
                    hits.iter()
                        .any(|hit| hit.target == ClickTarget::KeyHint(key(code)))
                );
            }
            let offset = form.scroll.offset();
            form.scroll(-1);
            if form.scroll.offset() == offset {
                break;
            }
        }
        for index in 0..PATH_COUNT {
            assert!(seen.contains(&format!("path_{index:02}_")));
        }
        assert!(seen.contains(END_MARKER));
        let bottom = form.scroll.offset();
        assert_eq!(
            form.handle_key(key(KeyCode::PageUp)),
            RestoreFormAction::Consumed
        );
        assert!(form.scroll.offset() < bottom);
        form.handle_key(key(KeyCode::PageDown));
        assert_eq!(form.scroll.offset(), bottom);
        form.handle_key(key(KeyCode::Home));
        let (top, _, _) = render(&mut form, 55, 20);
        assert!(top.contains("Conversation"));
    }
}
