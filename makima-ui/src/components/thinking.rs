use crate::markdown::{paint_semantic, should_truncate};
use makima_markdown::render::{Line as RLine, Renderer, code_block_start};
use ratatui::style::Style;
use ratatui::text::Line;
use std::hash::{DefaultHasher, Hash, Hasher};

pub(crate) struct LiveTail {
    start: usize,
    len: usize,
    hash: u64,
    renderer: Renderer,
}

impl Default for LiveTail {
    fn default() -> Self {
        Self {
            start: 0,
            len: 0,
            hash: hash_text(""),
            renderer: Renderer::streaming_wrapped(),
        }
    }
}

fn hash_text(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

impl LiveTail {
    fn render(&mut self, text: &str, start: usize, width: u16) -> Vec<RLine> {
        let text = &text[start..];
        if start != self.start
            || text
                .get(..self.len)
                .is_none_or(|prefix| hash_text(prefix) != self.hash)
        {
            *self = Self {
                start,
                ..Self::default()
            };
        }
        self.len = text.len();
        self.hash = hash_text(text);
        self.renderer.render(text, width)
    }
}

pub(crate) fn render_tail(
    text: &str,
    rows: usize,
    width: u16,
    text_style: Style,
    mut live: Option<&mut LiveTail>,
) -> Option<Vec<Line<'static>>> {
    let mut source_lines = rows.saturating_add(1);
    loop {
        let start = tail_start(text, source_lines);
        let semantic = match live.as_deref_mut() {
            Some(live) => live.render(text, start, width),
            None => Renderer::new().render(&text[start..], width),
        };
        let mut lines = paint_semantic(&semantic, "", text_style, Style::default());
        while lines.last().is_some_and(is_blank) {
            lines.pop();
        }
        let hidden = lines.len().saturating_sub(rows);
        if start == 0
            || (should_truncate(hidden) && lines.get(hidden).is_none_or(|line| !is_blank(line)))
        {
            return should_truncate(hidden).then(|| lines.split_off(hidden));
        }
        source_lines = source_lines.saturating_mul(2);
    }
}

fn tail_start(text: &str, min_lines: usize) -> usize {
    let Some((newline, _)) = text.rmatch_indices('\n').nth(min_lines.saturating_sub(1)) else {
        return 0;
    };
    let start = newline + 1;
    let code_start = code_block_start(text, start);
    if code_start != start || !text[start..].trim_start().starts_with('|') {
        return code_start;
    }
    let mut table_start = start;
    for line in text[..start].split_inclusive('\n').rev() {
        if !line.trim_start().starts_with('|') {
            break;
        }
        table_start -= line.len();
    }
    code_block_start(text, table_start)
}

fn is_blank(line: &Line<'_>) -> bool {
    line.spans.iter().all(|span| span.content.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const ROWS: usize = 3;
    const WIDTH: u16 = 24;
    const MANY: usize = 40;

    fn steps(count: usize) -> String {
        (1..=count)
            .map(|n| format!("- step {n:02}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test_case("word ".repeat(MANY); "wrapped_line")]
    #[test_case("word\n".repeat(MANY); "many_lines")]
    #[test_case(steps(MANY); "list")]
    #[test_case(format!("intro\n```rust\n{}```\nafter", "let a = 1;\n".repeat(MANY)); "closed_fence")]
    #[test_case(format!("intro\n````rust\n{}```\nmore", "let a = 1;\n".repeat(MANY)); "open_fence")]
    #[test_case(format!("| head | value |\n| --- | --- |\n{}", "| row | value |\n".repeat(MANY)); "table")]
    #[test_case("| h |\n|---|\n| v |\na\nb\nc\n\nd".to_owned(); "text_after_table")]
    fn tail_matches_full_render(text: String) {
        let mut live = LiveTail::default();
        for end in text.char_indices().map(|(end, _)| end).chain([text.len()]) {
            let prefix = &text[..end];
            let mut full = paint_semantic(
                &Renderer::new().render(prefix, WIDTH),
                "",
                Style::default(),
                Style::default(),
            );
            while full.last().is_some_and(is_blank) {
                full.pop();
            }
            let hidden = full.len().saturating_sub(ROWS);
            let expected = should_truncate(hidden).then(|| &full[hidden..]);
            assert_eq!(
                render_tail(prefix, ROWS, WIDTH, Style::default(), None).as_deref(),
                expected,
                "prefix: {prefix:?}"
            );
            assert_eq!(
                render_tail(prefix, ROWS, WIDTH, Style::default(), Some(&mut live)).as_deref(),
                expected,
                "live prefix: {prefix:?}"
            );
        }
    }

    #[test_case(4 => false; "too_little_hidden")]
    #[test_case(5 => true; "enough_hidden")]
    fn threshold(count: usize) -> bool {
        render_tail(&steps(count), ROWS, WIDTH, Style::default(), None).is_some()
    }

    #[test]
    fn reused_live_tail_resets_on_replacement() {
        let mut live = LiveTail::default();
        let first = format!("```rust\n{}", "let n = 1;\n".repeat(MANY));
        let second = format!("```python\n{}", "print(2)\n".repeat(MANY));
        render_tail(&first, ROWS, WIDTH, Style::default(), Some(&mut live));
        assert_eq!(
            render_tail(&second, ROWS, WIDTH, Style::default(), Some(&mut live)),
            render_tail(&second, ROWS, WIDTH, Style::default(), None)
        );
    }
}
