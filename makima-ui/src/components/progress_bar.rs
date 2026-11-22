use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

const BAR_CHAR: &str = "━";
const UNFILLED_COLOR: Color = Color::DarkGray;

pub struct ProgressBarConfig<'a> {
    pub ratio: f64,
    pub style: Style,
    pub cache_ratio: f64,
    pub cache_style: Style,
    pub label: Option<&'a str>,
    pub label_style: Option<Style>,
    pub bar_width: u16,
}

pub fn render(frame: &mut Frame, area: Rect, config: &ProgressBarConfig<'_>) {
    if area.is_empty() {
        return;
    }

    let ratio = config.ratio.clamp(0.0, 1.0);
    let cache_ratio = config.cache_ratio.clamp(0.0, 1.0);
    let width = config.bar_width as usize;
    let filled = (ratio * width as f64).round() as usize;
    let cache_filled = (cache_ratio * width as f64).round() as usize;
    let cache_filled = cache_filled.min(filled);

    let mut spans = Vec::with_capacity(width);

    if let Some(label) = config.label {
        let style = config.label_style.unwrap_or_default();
        spans.push(Span::styled(label, style));
    }

    for i in 0..width {
        let style = if i < cache_filled {
            config.cache_style
        } else if i < filled {
            config.style
        } else {
            Style::new().fg(UNFILLED_COLOR)
        };
        spans.push(Span::styled(BAR_CHAR, style));
    }

    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use test_case::test_case;

    const CACHE_STYLE: Style = Style::new().fg(Color::Green);

    fn render_gauge(ratio: f64, width: u16) -> Terminal<TestBackend> {
        render_gauge_with_cache(ratio, 0.0, width)
    }

    fn render_gauge_with_cache(ratio: f64, cache_ratio: f64, width: u16) -> Terminal<TestBackend> {
        let backend = TestBackend::new(width, 1);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                render(
                    f,
                    Rect::new(0, 0, width, 1),
                    &ProgressBarConfig {
                        ratio,
                        style: Style::default(),
                        cache_ratio,
                        cache_style: CACHE_STYLE,
                        label: None,
                        label_style: None,
                        bar_width: width,
                    },
                );
            })
            .unwrap();
        terminal
    }

    fn filled_count(terminal: &Terminal<TestBackend>) -> usize {
        let buf = terminal.backend().buffer();
        (0..buf.area.width)
            .filter(|&x| buf.cell((x, 0)).is_some_and(|c| c.fg != UNFILLED_COLOR))
            .count()
    }

    fn cache_cell_count(terminal: &Terminal<TestBackend>) -> usize {
        let buf = terminal.backend().buffer();
        (0..buf.area.width)
            .filter(|&x| buf.cell((x, 0)).is_some_and(|c| c.fg == Color::Green))
            .count()
    }

    #[test_case(0.0, 20, 0  ; "ratio_zero")]
    #[test_case(0.5, 20, 10 ; "ratio_half")]
    #[test_case(1.0, 20, 20 ; "ratio_full")]
    fn render_filled_cell_count(ratio: f64, width: u16, expected_filled: usize) {
        let terminal = render_gauge(ratio, width);
        assert_eq!(
            filled_count(&terminal),
            expected_filled,
            "expected {expected_filled} filled cells for ratio {ratio}"
        );
    }

    #[test_case(1.5 ; "ratio_over_one_clamped")]
    #[test_case(-0.5 ; "ratio_negative_clamped")]
    fn render_clamps_ratio(ratio: f64) {
        let terminal = render_gauge(ratio, 20);
        let filled = filled_count(&terminal);
        assert!(
            (0..=20).contains(&filled),
            "clamped ratio should produce 0..=20 filled cells, got {filled}"
        );
    }

    #[test]
    fn render_zero_width_is_empty() {
        let backend = TestBackend::new(0, 1);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                render(
                    f,
                    Rect::new(0, 0, 0, 1),
                    &ProgressBarConfig {
                        ratio: 0.5,
                        style: Style::default(),
                        cache_ratio: 0.0,
                        cache_style: CACHE_STYLE,
                        label: None,
                        label_style: None,
                        bar_width: 0,
                    },
                );
            })
            .unwrap();
        assert_eq!(filled_count(&terminal), 0);
    }

    #[test]
    fn render_with_label_preserves_label_width() {
        let width = 30;
        let backend = TestBackend::new(width, 1);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                render(
                    f,
                    Rect::new(0, 0, width, 1),
                    &ProgressBarConfig {
                        ratio: 0.5,
                        style: Style::default(),
                        cache_ratio: 0.0,
                        cache_style: CACHE_STYLE,
                        label: Some(" PP:"),
                        label_style: None,
                        bar_width: width,
                    },
                );
            })
            .unwrap();
        let buf = terminal.backend().buffer();
        let label_cells = (0..width)
            .take_while(|&x| buf.cell((x, 0)).is_some_and(|c| c.symbol() != BAR_CHAR))
            .count();
        assert_eq!(label_cells, 4, "label 'PP:' should occupy 4 cells");
    }

    #[test_case(0.5, 0.0, 0, 10  ; "no_cache")]
    #[test_case(0.5, 0.5, 10, 10 ; "cache_equals_progress")]
    #[test_case(0.5, 0.25, 5, 10 ; "cache_half_of_progress")]
    #[test_case(0.5, 1.0, 10, 10 ; "cache_exceeds_progress_clamped")]
    fn render_cache_segment(
        ratio: f64,
        cache_ratio: f64,
        expected_cache: usize,
        expected_filled: usize,
    ) {
        let terminal = render_gauge_with_cache(ratio, cache_ratio, 20);
        assert_eq!(cache_cell_count(&terminal), expected_cache);
        assert_eq!(filled_count(&terminal), expected_filled);
    }
}
