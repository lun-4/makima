use std::borrow::Cow;

use crate::click::{ClickTarget, HitTarget};
use crate::components::keybindings::key;
use crate::theme;

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Paragraph};

const ELLIPSIS: &str = "...";
const QUEUE_LABEL: &str = " Queue ";
const FOCUSED_HINT: &str = " - Enter to delete";

pub struct QueueEntry<'a> {
    pub text: Cow<'a, str>,
    pub color: ratatui::style::Color,
}

pub fn height(queue_len: usize) -> u16 {
    if queue_len == 0 {
        0
    } else {
        queue_len as u16 + 2
    }
}

pub fn view(
    frame: &mut Frame,
    area: Rect,
    entries: &[QueueEntry],
    focus: Option<usize>,
) -> Vec<HitTarget> {
    if entries.is_empty() {
        return Vec::new();
    }
    let content_width = area.width.saturating_sub(2) as usize;
    let lines: Vec<Line> = entries
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let flat = entry.text.replace('\n', " ");
            let (style, hint_parts) = if focus == Some(i) {
                (theme::current().queue_delete, ("", FOCUSED_HINT, ""))
            } else if i == 0 {
                (
                    Style::new().fg(entry.color),
                    (" - ", key::POP_QUEUE.label, " to delete"),
                )
            } else {
                (Style::new().fg(entry.color), ("", "", ""))
            };
            truncate_line(&flat, content_width, style, hint_parts)
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(if focus.is_some() {
            theme::current().queue_delete
        } else {
            theme::current().panel_border
        })
        .title_top(Line::from(QUEUE_LABEL).left_aligned())
        .title_style(theme::current().panel_title);

    let paragraph = Paragraph::new(lines)
        .style(Style::new().fg(theme::current().foreground))
        .block(block);

    frame.render_widget(paragraph, area);
    entries
        .iter()
        .enumerate()
        .map(|(i, _)| HitTarget {
            rect: Rect::new(
                area.x + 1,
                area.y + 1 + i as u16,
                area.width.saturating_sub(2),
                1,
            ),
            target: ClickTarget::QueueRow(i),
        })
        .collect()
}

fn truncate_line(
    text: &str,
    max_width: usize,
    style: Style,
    hint: (&'static str, &'static str, &'static str),
) -> Line<'static> {
    let hint_style = theme::current().tool_dim;
    let hint_len = hint.0.len() + hint.1.len() + hint.2.len();
    let available = max_width.saturating_sub(hint_len);

    let (text_span, ellipsis) = if text.len() <= available {
        (Span::styled(text.to_string(), style), None)
    } else {
        let truncated_len = text.floor_char_boundary(available.saturating_sub(ELLIPSIS.len()));
        (
            Span::styled(text[..truncated_len].to_string(), style),
            Some(Span::styled(ELLIPSIS, hint_style)),
        )
    };

    let mut spans = vec![text_span];
    spans.extend(ellipsis);
    if hint_len > 0 {
        spans.push(Span::styled(hint.0, hint_style));
        spans.push(Span::styled(hint.1, hint_style));
        spans.push(Span::styled(hint.2, hint_style));
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_case::test_case;

    #[test]
    fn height_includes_borders() {
        assert_eq!(height(0), 0);
        assert_eq!(height(1), 3);
        assert_eq!(height(3), 5);
    }

    const HINT: (&str, &str, &str) = (" - hint", "", "");
    const NO_HINT: (&str, &str, &str) = ("", "", "");
    fn style() -> Style {
        Style::new().fg(theme::current().foreground)
    }
    fn span_texts<'a>(line: &'a Line<'a>) -> Vec<&'a str> {
        line.spans
            .iter()
            .map(|s| s.content.as_ref())
            .filter(|s| !s.is_empty())
            .collect()
    }

    const HINT_STR: &str = " - hint";
    #[test_case("hello", 10, NO_HINT, &["hello"]                                          ; "no_hint_short")]
    #[test_case("abcdefghij", 7, NO_HINT, &["abcd", ELLIPSIS]                             ; "no_hint_truncated")]
    #[test_case("abcde", 5, NO_HINT, &["abcde"]                                           ; "no_hint_exact_width")]
    #[test_case("abcdef", 2, NO_HINT, &[ELLIPSIS]                                     ; "no_hint_tiny_width")]
    #[test_case("●abc", 5, NO_HINT, &[ELLIPSIS]                                       ; "no_hint_multibyte_narrow")]
    #[test_case("●●●", 8, NO_HINT, &["●", ELLIPSIS]                                      ; "no_hint_multibyte_fits_one")]
    #[test_case("hello", 20, HINT, &["hello", HINT_STR]                                   ; "hint_short")]
    #[test_case("abcdefghijklmnopqrstuvwxyz", 18, HINT, &["abcdefgh", ELLIPSIS, HINT_STR]  ; "hint_truncated")]
    #[test_case("ab", 9, HINT, &["ab", HINT_STR]                                          ; "hint_exact_fit")]
    fn truncate_line_cases(
        input: &str,
        width: usize,
        hint: (&'static str, &'static str, &'static str),
        expected: &[&str],
    ) {
        assert_eq!(
            span_texts(&truncate_line(input, width, style(), hint)),
            expected
        );
    }
}
