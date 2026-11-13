use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use makima_ui::bench_support::{Layout, Row, TextBuffer};
use unicode_width::UnicodeWidthChar;

const WIDTH: usize = 100;
const VISIBLE_ROWS: usize = 20;
const SIZES: [usize; 3] = [1_000, 10_000, 50_000];

const PROSE: &str =
    "the agent loop pulls a message off the channel and hands it to the provider, then waits";
const LONG_PROSE: &str = " and the rest of the paragraph wraps onto the next row of the input box, which is what makes the old full-buffer pass expensive";

fn buffer(lines: usize) -> TextBuffer {
    let mut text = String::with_capacity(lines * PROSE.len());
    for i in 0..lines {
        text.push_str(PROSE);
        if i % 5 == 0 {
            text.push_str(LONG_PROSE);
        }
        text.push('\n');
    }
    let mut buffer = TextBuffer::new(text);
    buffer.move_to_end();
    buffer
}

/// The per-frame cost before the layout cached row counts: every line wrapped
/// and measured, and the whole value joined just to ask whether it is empty.
/// Kept here so the two numbers are measured by the same bench.
fn full_layout(buffer: &TextBuffer) -> usize {
    let value = buffer.value();
    let mut rows = 0;
    for line in buffer.lines() {
        let widths: Vec<usize> = line.chars().map(|c| c.width().unwrap_or(1)).collect();
        let mut ranges = 0;
        let mut row_col = 0;
        for &w in &widths {
            if row_col + w > WIDTH && row_col > 0 {
                ranges += 1;
                row_col = 0;
            }
            row_col += w;
        }
        rows += ranges + 1;
    }
    rows + value.len()
}

fn first_visible(layout: &Layout) -> u32 {
    layout.total_rows().saturating_sub(VISIBLE_ROWS as u32)
}

/// A frame with the viewport parked at the end of the buffer, where typing
/// leaves it.
fn frame(layout: &mut Layout, buffer: &TextBuffer) -> usize {
    layout.sync(buffer, WIDTH, true);
    let rows: Vec<Row> = layout
        .rows_from(buffer, first_visible(layout))
        .take(VISIBLE_ROWS)
        .collect();
    rows.len()
}

fn bench_input(c: &mut Criterion) {
    for lines in SIZES {
        let end = buffer(lines);
        let mut layout = Layout::default();
        layout.sync(&end, WIDTH, true);

        c.bench_function(&format!("full_layout_before/{lines}"), |b| {
            b.iter(|| full_layout(black_box(&end)))
        });
        c.bench_function(&format!("frame_after/{lines}"), |b| {
            b.iter(|| frame(black_box(&mut layout), black_box(&end)))
        });
        c.bench_function(&format!("keystroke_after/{lines}"), |b| {
            b.iter_batched_ref(
                || {
                    let buffer = buffer(lines);
                    let mut layout = Layout::default();
                    layout.sync(&buffer, WIDTH, true);
                    (buffer, layout)
                },
                |(buffer, layout)| {
                    buffer.push_char(black_box('x'));
                    frame(black_box(layout), black_box(buffer))
                },
                BatchSize::LargeInput,
            )
        });
    }
}

criterion_group!(benches, bench_input);
criterion_main!(benches);
