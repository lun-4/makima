use std::sync::atomic::{AtomicBool, Ordering};

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::widgets::{Scrollbar, ScrollbarOrientation, ScrollbarState};

pub const SCROLLBAR_THUMB: &str = "\u{2590}";

static ENABLED: AtomicBool = AtomicBool::new(true);

pub(super) fn saturating_u16(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}

pub fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug)]
pub struct ScrollbarHit {
    /// The one-column rect that was actually drawn.
    pub area: Rect,
    pub content_len: u16,
}

/// The thumb geometry ratatui derives for a bar drawn over `height` rows with
/// `total` content lines. Returns `(length, start)`.
fn thumb_geometry(height: u16, total: u16, position: u16) -> (u16, u16) {
    if height == 0 {
        return (0, 0);
    }
    let height = u64::from(height);
    let total = u64::from(total.max(1));
    let position = u64::from(position);
    let rounding_divide =
        |numerator: u64, denominator: u64| (numerator + denominator / 2) / denominator;
    let thumb_len = rounding_divide(height * height, total).clamp(1, height);
    let thumb_start =
        rounding_divide(position * height, total).clamp(0, height.saturating_sub(thumb_len));
    (thumb_len as u16, thumb_start as u16)
}

pub fn thumb(height: u16, total: u16, position: u16) -> (u16, u16) {
    thumb_geometry(height, total, position)
}

/// Maps a pointer row inside the bar to the scroll position whose thumb
/// covers it, given the pointer's offset within the thumb (`grab`).
pub fn position_at(height: u16, total: u16, grab: u16, y: u16, current: u16) -> u16 {
    if height == 0 {
        return current;
    }
    let (thumb_len, thumb_start) = thumb_geometry(height, total, current);
    let travel = height.saturating_sub(thumb_len);
    if travel == 0 {
        return current;
    }
    let max_scroll = total.max(1).saturating_sub(height);
    let offset = i64::from(y) - i64::from(grab);
    if offset == i64::from(thumb_start) {
        return current.min(max_scroll);
    }
    let numerator = offset * i64::from(max_scroll);
    let rounded = if numerator >= 0 {
        (numerator + i64::from(travel) / 2) / i64::from(travel)
    } else {
        (numerator - i64::from(travel) / 2) / i64::from(travel)
    };
    rounded.clamp(0, i64::from(max_scroll)) as u16
}

/// The offset to grab within the thumb for a press at `y` rows into the bar.
pub fn grab_at(height: u16, total: u16, position: u16, y: u16) -> u16 {
    let (thumb_len, thumb_start) = thumb_geometry(height, total, position);
    if (thumb_start..thumb_start + thumb_len).contains(&y) {
        y.saturating_sub(thumb_start)
    } else {
        thumb_len / 2
    }
}

pub fn render_vertical_scrollbar(
    frame: &mut Frame,
    area: Rect,
    content_len: u16,
    position: u16,
) -> Option<ScrollbarHit> {
    if !ENABLED.load(Ordering::Relaxed) || area.width == 0 || area.height == 0 {
        return None;
    }
    let max_scroll = content_len.saturating_sub(area.height);
    let mut state = ScrollbarState::default()
        .content_length(max_scroll as usize + 1)
        .position(position as usize);

    let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .thumb_symbol(SCROLLBAR_THUMB)
        // ListPicker renders highlighted rows over the scrollbar track; resetting
        // the thumb style keeps its color stable instead of inheriting row bg.
        .thumb_style(Style::new().fg(Color::Reset).bg(Color::Reset))
        .track_symbol(None)
        .begin_symbol(None)
        .end_symbol(None);

    frame.render_stateful_widget(scrollbar, area, &mut state);
    Some(ScrollbarHit {
        area: Rect::new(area.right().saturating_sub(1), area.y, 1, area.height),
        content_len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use test_case::test_case;

    #[test_case(0, 0; "zero")]
    #[test_case(u16::MAX as usize, u16::MAX; "maximum")]
    #[test_case(u16::MAX as usize + 1, u16::MAX; "over_maximum")]
    #[test_case(usize::MAX, u16::MAX; "usize_maximum")]
    fn range_saturates(value: usize, expected: u16) {
        assert_eq!(saturating_u16(value), expected);
    }

    #[test_case(0, u16::MAX, u16::MAX, (0, 0); "zero_height")]
    #[test_case(u16::MAX, 0, 0, (u16::MAX, 0); "zero_total")]
    #[test_case(u16::MAX, u16::MAX, u16::MAX, (u16::MAX, 0); "maximum_height")]
    #[test_case(2, u16::MAX, u16::MAX, (1, 1); "maximum_position")]
    fn boundary_geometry(height: u16, total: u16, position: u16, expected: (u16, u16)) {
        assert_eq!(thumb(height, total, position), expected);
        let (_, start) = expected;
        let grab = grab_at(height, total, position, start);
        let recovered = position_at(height, total, grab, start, position);
        let expected_position = if height == 0 || expected.0 == height {
            position
        } else {
            position.min(total.saturating_sub(height))
        };
        assert_eq!(recovered, expected_position);
    }

    fn drawn_thumb(height: u16, total: u16, position: u16) -> Vec<u16> {
        let backend = TestBackend::new(1, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = frame.area();
                render_vertical_scrollbar(frame, area, total, position);
            })
            .unwrap();
        let buf = terminal.backend().buffer();
        (0..height)
            .filter(|&y| {
                buf.cell(ratatui::layout::Position::new(0, y))
                    .is_some_and(|c| c.symbol() == SCROLLBAR_THUMB)
            })
            .collect()
    }

    #[test]
    fn thumb_matches_ratatui_render() {
        for &(height, total) in &[(10u16, 100u16), (7, 13), (20, 21), (4, 4), (1, 9)] {
            let max_scroll = total.saturating_sub(height);
            for position in [0, max_scroll / 2, max_scroll] {
                let (len, start) = thumb(height, total, position);
                let expected: Vec<u16> = (start..start + len).collect();
                assert_eq!(drawn_thumb(height, total, position), expected);
            }
        }
    }

    #[test]
    fn thumb_and_position_round_trip() {
        for &(height, total) in &[(10u16, 100u16), (7, 13), (20, 21), (5, 40)] {
            let max_scroll = total.saturating_sub(height);
            let step = max_scroll / (height - 1).max(1) + 1;
            for position in [0, max_scroll / 3, max_scroll / 2, max_scroll] {
                let (_, start) = thumb(height, total, position);
                let y = start;
                let grab = grab_at(height, total, position, y);
                let recovered = position_at(height, total, grab, y, position);
                assert!(
                    recovered.abs_diff(position) <= step,
                    "{recovered} vs {position}"
                );
            }
        }
    }

    #[test]
    fn position_at_reaches_both_ends() {
        let (height, total) = (10u16, 100u16);
        let max_scroll = total - height;
        let (len, _) = thumb(height, total, 0);
        let grab = grab_at(height, total, 0, 0);
        assert_eq!(grab, 0);
        let travel = height - len;
        assert_eq!(position_at(height, total, grab, 0, 0), 0);
        assert_eq!(
            position_at(height, total, grab, travel + grab, 0),
            max_scroll
        );
    }

    #[test]
    fn disabled_scrollbar_registers_no_hit() {
        set_enabled(false);
        let mut hit = None;
        let backend = TestBackend::new(4, 4);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = frame.area();
                hit = render_vertical_scrollbar(frame, area, 10, 0);
            })
            .unwrap();
        assert!(hit.is_none());
        set_enabled(true);
    }
}
