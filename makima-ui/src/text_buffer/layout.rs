//! How the input's lines break into rows on screen. Drawing, the caret, clicks
//! and copying a selection all ask here, so they never disagree on where a row
//! starts.
//!
//! The row count of every line is kept for one width and caught up from the
//! buffer's change log, so a keystroke only recounts the line it touched.
//! Running totals over those counts find the line under any row without
//! walking the lines above it, so a frame only wraps the rows it draws.

use std::ops::Range;

use unicode_width::UnicodeWidthChar;

use super::TextBuffer;

/// A drawn row: chars {chars} of line {line}, the {index}th row that line
/// wraps into.
pub struct Row {
    pub line: usize,
    pub index: u32,
    pub chars: Range<usize>,
}

#[derive(Default)]
pub struct Layout {
    /// Never 0 once built, so a default layout always rebuilds on its first
    /// sync.
    width: usize,
    version: u64,
    generation: u64,
    /// Rows each line takes as drawn, the cursor's extra row included.
    counts: Vec<u32>,
    sums: RowSums,
    cursor_line: usize,
    /// A caret past a completely full last row gets a row of its own, so the
    /// count of {cursor_line} carries one more row than its text wraps into.
    cursor_extra_row: bool,
    focused: bool,
}

impl Layout {
    /// Brings the counts up to date with {buffer} at {width} text columns.
    /// Cheap when nothing moved, so every reader calls it first.
    pub fn sync(&mut self, buffer: &TextBuffer, width: usize, focused: bool) {
        let width = width.max(1);
        if width == self.width
            && buffer.version() == self.version
            && buffer.generation() == self.generation
            && buffer.y() == self.cursor_line
            && focused == self.focused
        {
            return;
        }
        // Taken off while the counts still match the old text, because the
        // splice below can move or drop the line it sits on.
        self.set_cursor_extra_row(false);
        if width != self.width || self.apply_changes(buffer).is_none() {
            self.rebuild(buffer, width);
        }
        self.cursor_line = buffer.y();
        self.focused = focused;
        self.set_cursor_extra_row(focused && last_row_full(buffer.line(self.cursor_line), width));
        debug_assert_eq!(
            self.counts.len(),
            buffer.line_count(),
            "a count for every line"
        );
    }

    pub fn total_rows(&self) -> u32 {
        self.sums.before(self.counts.len())
    }

    /// The rows each line draws as, the cursor's extra row included.
    pub fn heights(&self) -> impl Iterator<Item = u32> + '_ {
        self.counts.iter().copied()
    }

    pub fn line_row_count(&self, line: usize) -> u32 {
        self.counts[line]
    }

    /// The row the caret is drawn on, counted from the top of the buffer.
    pub fn cursor_row(&self, buffer: &TextBuffer) -> u32 {
        let y = buffer.y();
        let caret = buffer.x();
        let in_line = self
            .rows_of(buffer, y)
            .iter()
            .rposition(|row| row.start <= caret)
            .unwrap_or(0);
        self.sums.before(y) + in_line as u32
    }

    /// The drawn rows from row {first} down, wrapped only as they are pulled.
    pub fn rows_from<'a>(
        &'a self,
        buffer: &'a TextBuffer,
        first: u32,
    ) -> impl Iterator<Item = Row> + 'a {
        let (line, skip) = self.sums.locate(first).unwrap_or((self.counts.len(), 0));
        (line..self.counts.len())
            .flat_map(move |y| {
                self.rows_of(buffer, y)
                    .into_iter()
                    .enumerate()
                    .map(move |(index, chars)| Row {
                        line: y,
                        index: index as u32,
                        chars,
                    })
            })
            .skip(skip as usize)
    }

    fn rows_of(&self, buffer: &TextBuffer, y: usize) -> Vec<Range<usize>> {
        debug_assert!(
            self.version == buffer.version() && self.cursor_line == buffer.y(),
            "layout read before sync"
        );
        let line = buffer.line(y);
        let mut rows = Vec::new();
        wrap(line, self.width, |chars| rows.push(chars));
        if self.cursor_extra_row && y == self.cursor_line {
            let len = line.chars().count();
            rows.push(len..len);
        }
        rows
    }

    /// Every change since the last sync fits in one line span, from its lowest
    /// first to its highest last, so only the lines in that span get
    /// recounted. The lines below it are the same lines in the old and the new
    /// text, so counting them up from the bottom finds where the span ended in
    /// the old counts, without ever knowing what the changes removed. `None`
    /// when the log does not reach back far enough.
    ///
    /// When no line came or went, each recount is one Fenwick update.
    /// Otherwise the counts shift and the sums are built again, one pass over
    /// plain integers.
    fn apply_changes(&mut self, buffer: &TextBuffer) -> Option<()> {
        if buffer.version() < self.version || buffer.generation() != self.generation {
            return None;
        }
        if buffer.version() == self.version {
            return Some(());
        }
        let changes: Vec<super::LineChange> = buffer.changes_since(self.version)?.collect();
        let below = changes.iter().map(|change| change.below).min()?;
        let first = changes.iter().map(|change| change.first).min()?;
        let old_last = self.counts.len().checked_sub(below + 1)?;
        if first > self.counts.len() {
            return None;
        }
        let last = buffer.line_count() - 1 - below;
        let fresh = (first..=last).map(|y| count_rows(buffer.line(y), self.width));
        if old_last == last {
            for (y, rows) in (first..).zip(fresh) {
                self.sums.add(y, rows as i32 - self.counts[y] as i32);
                self.counts[y] = rows;
            }
        } else {
            self.counts.splice(first..=old_last, fresh);
            self.sums = RowSums::new(&self.counts);
        }
        self.version = buffer.version();
        Some(())
    }

    fn rebuild(&mut self, buffer: &TextBuffer, width: usize) {
        self.width = width;
        self.version = buffer.version();
        self.generation = buffer.generation();
        self.counts = buffer
            .lines()
            .iter()
            .map(|line| count_rows(line, width))
            .collect();
        self.sums = RowSums::new(&self.counts);
    }

    fn set_cursor_extra_row(&mut self, on: bool) {
        if on == self.cursor_extra_row {
            return;
        }
        self.cursor_extra_row = on;
        let delta = if on { 1 } else { -1 };
        self.counts[self.cursor_line] = self.counts[self.cursor_line].wrapping_add_signed(delta);
        self.sums.add(self.cursor_line, delta);
    }
}

/// Greedy wrapping, where a char that no longer fits moves to the next row and
/// leaves the gap behind. Dividing the columns by the width looks close enough
/// for plain text, but it cannot see those gaps, so with wide chars it lands on
/// the wrong row.
///
/// Calls {emit} with the char range of every row, and returns whether the last
/// row is full, where a caret after it needs a row of its own.
fn wrap(line: &str, width: usize, mut emit: impl FnMut(Range<usize>)) -> bool {
    let mut row_start = 0;
    let mut row_col = 0;
    let mut len = 0;
    for (i, c) in line.chars().enumerate() {
        let w = c.width().unwrap_or(1);
        if row_col + w > width && row_col > 0 {
            emit(row_start..i);
            row_start = i;
            row_col = 0;
        }
        row_col += w;
        len = i + 1;
    }
    emit(row_start..len);
    row_col >= width
}

/// The one bit at `i`'s lowest set position, for walking a Fenwick tree.
fn lowest_bit(i: usize) -> usize {
    i & i.wrapping_neg()
}

fn count_rows(line: &str, width: usize) -> u32 {
    let mut rows = 0;
    wrap(line, width, |_| rows += 1);
    rows
}

fn last_row_full(line: &str, width: usize) -> bool {
    wrap(line, width, |_| {})
}

/// Running row totals as a Fenwick tree, so both the row a line starts on and
/// the line a row falls in cost `O(log lines)` and a recounted line is one
/// `O(log lines)` update. Slot `i` holds the sum of the `i & -i` counts that end
/// at line `i - 1`, and slot 0 stays empty so that math works out.
struct RowSums(Vec<u32>);

impl Default for RowSums {
    fn default() -> Self {
        Self(vec![0])
    }
}

impl RowSums {
    fn new(counts: &[u32]) -> Self {
        let mut tree = Vec::with_capacity(counts.len() + 1);
        tree.push(0);
        tree.extend_from_slice(counts);
        for i in 1..tree.len() {
            let parent = i + lowest_bit(i);
            if parent < tree.len() {
                tree[parent] += tree[i];
            }
        }
        Self(tree)
    }

    fn add(&mut self, line: usize, delta: i32) {
        let mut i = line + 1;
        while i < self.0.len() {
            self.0[i] = self.0[i].wrapping_add_signed(delta);
            i += lowest_bit(i);
        }
    }

    /// Rows above {line}.
    fn before(&self, line: usize) -> u32 {
        let mut sum = 0;
        let mut i = line;
        while i > 0 {
            sum += self.0[i];
            i -= lowest_bit(i);
        }
        sum
    }

    /// The line {row} falls in and how many of that line's rows come before
    /// it. Nothing is past the last row.
    fn locate(&self, row: u32) -> Option<(usize, u32)> {
        let len = self.0.len() - 1;
        let mut line = 0;
        let mut rest = row;
        let mut step = (len + 1).next_power_of_two() / 2;
        while step > 0 {
            let next = line + step;
            if next <= len && self.0[next] <= rest {
                line = next;
                rest -= self.0[next];
            }
            step /= 2;
        }
        (line < len).then_some((line, rest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const WIDTH: usize = 8;
    /// At {WIDTH} the second line wraps into four rows, and the last line ends
    /// on a full row, so the caret starts out on a row of its own.
    const START: &str = "short\nlong enough to wrap twice\n\nend\nwide 一二三四五";

    type Observed = (Vec<u32>, u32, Vec<(usize, u32, Range<usize>)>);

    /// Everything a reader gets out of a layout. Looking up every row on its
    /// own goes through the running totals, so a stale total shows up even
    /// when the per-line counts are right.
    fn observe(layout: &Layout, buffer: &TextBuffer) -> Observed {
        let rows = (0..layout.total_rows())
            .filter_map(|first| layout.rows_from(buffer, first).next())
            .map(|row| (row.line, row.index, row.chars))
            .collect();
        (layout.heights().collect(), layout.cursor_row(buffer), rows)
    }

    fn synced(buffer: &TextBuffer, width: usize) -> Layout {
        let mut layout = Layout::default();
        layout.sync(buffer, width, true);
        layout
    }

    #[test_case(|b| { b.set_cursor(0, 5); b.insert_text("wrap"); }, WIDTH ; "typing_that_wraps_a_line")]
    #[test_case(|b| b.add_line(), WIDTH ; "enter_on_a_full_row")]
    #[test_case(|b| b.move_up(), WIDTH ; "caret_leaving_a_full_row")]
    #[test_case(|b| b.insert_text("one\ntwo three four five\n"), WIDTH ; "multi_line_paste")]
    #[test_case(|b| { b.set_cursor(0, 2); b.delete_char(); b.delete_char(); }, WIDTH ; "deleting_across_lines")]
    #[test_case(|b| { b.set_cursor(0, 0); b.insert_text("wrapped "); }, WIDTH + 3 ; "width_change")]
    #[test_case(|b| { b.set_cursor(4, 0); b.insert_text("top\nlines\n"); }, WIDTH ; "edit_low_then_paste_above_it")]
    #[test_case(|b| { b.clear(); b.insert_text("brand\nnew value"); }, WIDTH ; "whole_value_swap")]
    #[test_case(|b| { b.set_cursor(1, 0); b.remove_char(); b.move_to_end(); }, WIDTH ; "deleting_a_line_break")]
    fn catching_up_matches_a_fresh_build(edit: fn(&mut TextBuffer), width: usize) {
        let mut buffer = TextBuffer::new(START.into());
        buffer.move_to_end();
        let mut layout = synced(&buffer, WIDTH);

        edit(&mut buffer);
        layout.sync(&buffer, width, true);

        assert_eq!(
            observe(&layout, &buffer),
            observe(&synced(&buffer, width), &buffer)
        );
    }

    #[test]
    fn a_reader_that_fell_further_behind_than_the_log_rebuilds() {
        let mut buffer = TextBuffer::new(START.into());
        buffer.move_to_end();
        let mut layout = synced(&buffer, WIDTH);
        for _ in 0..=super::super::CHANGE_LOG {
            buffer.push_char('x');
        }

        layout.sync(&buffer, WIDTH, true);

        assert_eq!(
            observe(&layout, &buffer),
            observe(&synced(&buffer, WIDTH), &buffer)
        );
    }

    /// Two buffers start their versions at zero, so a replaced input must not
    /// be read through the counts of the one it replaced.
    #[test]
    fn a_replaced_buffer_with_the_same_version_is_rebuilt() {
        let replaced = {
            let mut buffer = TextBuffer::new("one\ntwo".into());
            buffer.move_to_end();
            buffer
        };
        let mut layout = synced(&replaced, WIDTH);

        let mut fresh = TextBuffer::new("a much longer single line".into());
        fresh.move_to_end();
        layout.sync(&fresh, WIDTH, true);

        assert_eq!(
            observe(&layout, &fresh),
            observe(&synced(&fresh, WIDTH), &fresh)
        );
    }

    #[test]
    fn the_cursor_line_gets_a_row_of_its_own_only_while_focused() {
        let mut buffer = TextBuffer::new("exactly8".into());
        buffer.move_to_end();
        let mut layout = Layout::default();
        layout.sync(&buffer, WIDTH, true);
        assert_eq!(layout.total_rows(), 2);
        layout.sync(&buffer, WIDTH, false);
        assert_eq!(layout.total_rows(), 1);
    }

    #[test]
    fn row_sums_match_a_plain_running_total() {
        let counts: [u32; _] = [0, 2, 1, 0, 3, 0];
        let sums = RowSums::new(&counts);
        let mut expected = counts
            .iter()
            .enumerate()
            .flat_map(|(line, &rows)| (0..rows).map(move |skip| (line, skip)));
        for row in 0..=counts.iter().sum::<u32>() {
            assert_eq!(sums.locate(row), expected.next());
        }
        for line in 0..=counts.len() {
            assert_eq!(sums.before(line), counts[..line].iter().sum::<u32>());
        }
    }
}
