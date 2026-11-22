use super::segment::SegmentCache;
use crate::selection::{self, LineBreaks, ScreenSelection, Selection};

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::{Paragraph, Widget, Wrap};

/// Rows re-rendered per pass of the copy path. A `Cell` is 40 bytes, so at a
/// usual terminal width this holds the scratch buffer near a megabyte however
/// tall the selected segment is.
const COPY_CHUNK_ROWS: u16 = 256;

/// Copies the segments the selection spans. The streaming tail lives past the
/// cache, so text still arriving is not copyable.
pub(super) fn extract_selection_text(
    cache: &SegmentCache,
    viewport_width: u16,
    sel: &Selection,
    msg_area: Rect,
) -> String {
    let (doc_start, doc_end) = sel.normalized();
    let width = viewport_width;

    let heights: Vec<u16> = cache.segments().iter().map(|s| s.height(width)).collect();

    let mut out = String::new();
    let mut doc_row: u32 = 0;

    for (i, &h) in heights.iter().enumerate() {
        let seg_start = doc_row;
        let seg_end = doc_row + h as u32;
        doc_row = seg_end;

        if seg_end <= doc_start.row || seg_start > doc_end.row {
            continue;
        }

        if !out.is_empty() {
            out.push('\n');
        }

        let Some(seg) = cache.get(i) else { continue };

        if seg.lines().is_empty() {
            continue;
        }

        let drawn = seg.drawn_height(width);
        let rel_start = (doc_start.row.saturating_sub(seg_start) as u16).min(drawn);
        let rel_end = if doc_end.row + 1 >= seg_end {
            drawn
        } else {
            ((doc_end.row + 1 - seg_start) as u16).min(drawn)
        };

        let start_col = if seg_start > doc_start.row {
            0
        } else {
            doc_start.col.saturating_sub(msg_area.x)
        };
        let end_col = if seg_end < doc_end.row + 1 {
            width.saturating_sub(1)
        } else {
            doc_end.col.saturating_sub(msg_area.x)
        };

        let ss = ScreenSelection {
            start_row: rel_start,
            start_col,
            end_row: rel_end.saturating_sub(1),
            end_col,
        };

        // Only the selected rows are re-rendered, a chunk at a time. The
        // cursor measures the segment once for all the chunks it hands out.
        let mut carry = selection::RowCarry::default();
        let mut walk = seg.rows_from(rel_start, width);
        while let Some((lines, chunk)) = walk.next_chunk(COPY_CHUNK_ROWS) {
            if chunk.start >= rel_end {
                break;
            }
            // The buffer sits at the rows it draws, so `ss` keeps talking in
            // segment rows across every chunk.
            let area = Rect::new(0, chunk.start, width, chunk.end - chunk.start);
            // A double width grapheme landing on the last column makes ratatui
            // write one past the area it wrapped to, so the scratch buffer
            // holds a spare column for that write. `area` keeps the real width,
            // which is what decides the wrap and the columns copied.
            let mut tmp = Buffer::empty(Rect {
                width: width.saturating_add(1),
                ..area
            });
            Paragraph::new(lines.to_vec())
                .wrap(Wrap { trim: false })
                .render(area, &mut tmp);

            // The chunk runs to a line boundary, so it can reach past the
            // selection on either side. `append_rows` clips to the rows this
            // buffer actually holds.
            selection::append_rows(
                &tmp,
                area,
                &ss,
                rel_start..rel_end,
                &mut out,
                &LineBreaks::from_lines(lines, width),
                &mut carry,
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::segment::Segment;
    use super::{COPY_CHUNK_ROWS, SegmentCache, extract_selection_text};
    use crate::selection::{Selection, SelectionZone};
    use ratatui::layout::Rect;
    use ratatui::text::Line;

    /// What dragging the mouse over one whole segment copies.
    fn copy_whole_segment(lines: Vec<Line<'static>>, width: u16) -> String {
        let mut cache = SegmentCache::new();
        cache.push(Segment::with_lines(lines, None));
        let last_row = cache
            .get(0)
            .expect("segment")
            .height(width)
            .saturating_sub(1);
        let area = Rect::new(0, 0, width, last_row.saturating_add(1));

        let mut sel = Selection::start(0, 0, area, SelectionZone::Messages, 0);
        sel.update(last_row, width, 0);
        extract_selection_text(&cache, width, &sel, area)
    }

    /// A stale segment can hold a line far wider than the terminal now is, and
    /// at two columns ratatui writes a double width grapheme one cell past the
    /// area it wrapped to. The scrape used to hand that write a buffer exactly
    /// as wide as the area, so releasing the mouse took the whole UI down.
    ///
    /// The last grapheme is the one ratatui shoved over the edge, so it is off
    /// screen and copying it back would not match what the user sees.
    #[test]
    fn copying_into_two_columns_gives_back_what_is_on_screen() {
        assert_eq!(
            copy_whole_segment(vec![Line::from("a\u{4f60}\u{597d}")], 2),
            "a\u{4f60}"
        );
    }

    /// Blank rows are held back until content follows them, and the first row
    /// copied never gets a newline in front. Both live in state that only
    /// survives if it is carried across a chunk boundary, and with this many
    /// lead lines the boundary falls right between the two blanks.
    #[test]
    fn blank_rows_survive_a_chunk_boundary() {
        const WIDTH: u16 = 40;
        const LEAD: usize = COPY_CHUNK_ROWS as usize - 1;

        let mut lines: Vec<Line<'static>> =
            (0..LEAD).map(|i| Line::from(format!("l{i}"))).collect();
        lines.push(Line::default());
        lines.push(Line::default());
        lines.push(Line::from("tail"));

        let body = (0..LEAD)
            .map(|i| format!("l{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            copy_whole_segment(lines, WIDTH),
            format!("{body}\n\n\ntail")
        );
    }
}
