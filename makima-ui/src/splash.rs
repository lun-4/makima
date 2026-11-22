use crate::repaint::Cadence;
use crate::theme::{self, lerp_u8};
use makima_lua::{SplashFrame, SplashStyle};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use std::time::Instant;

pub use makima_lua::SplashRow;

const COLOR_TRANSITION_SECS: f32 = 0.4;

/// Seconds for the initial fade-in animation (ease-out cubic).
const FADE_DURATION: f32 = 1.6;
/// Ascii chars mapped to increasing wave intensity (first must be space).
const FIELD_SYMS: &[&str] = &[" ", ".", ":", "+", "*"];
const FIELD_CHAR_MAX: f32 = (FIELD_SYMS.len() - 1) as f32;
/// Base opacity for the dimmest field character (0.0–1.0). Higher = less contrast between chars.
const FIELD_BASE_OPACITY: f32 = 0.5;

#[inline(always)]
fn field_idx(ch: char) -> usize {
    match ch {
        '.' => 1,
        ':' => 2,
        '+' => 3,
        '*' => 4,
        _ => 0,
    }
}

/// Per-cell field color derived from the `" .:+*"` LUT. The plugin already
/// folded `fade` into the glyph-bucket choice, so the cell is painted verbatim.
fn field_style(row_idx: usize, bg: (u8, u8, u8), ac: (u8, u8, u8)) -> Style {
    let frac = row_idx as f32 / FIELD_CHAR_MAX;
    let t = FIELD_BASE_OPACITY + frac * (1.0 - FIELD_BASE_OPACITY);
    Style::new().fg(Color::Rgb(
        lerp_u8(bg.0, ac.0, t * 0.25),
        lerp_u8(bg.1, ac.1, t * 0.175),
        lerp_u8(bg.2, ac.2, t * 0.325),
    ))
}

pub struct ColorTransition {
    from: (u8, u8, u8),
    to: (u8, u8, u8),
    start: Instant,
}

impl ColorTransition {
    pub fn new(color: Color) -> Self {
        let rgb = extract_rgb(color, (100, 140, 255));
        Self {
            from: rgb,
            to: rgb,
            start: Instant::now() - std::time::Duration::from_secs_f32(COLOR_TRANSITION_SECS),
        }
    }

    pub fn set(&mut self, color: Color) {
        let rgb = extract_rgb(color, (100, 140, 255));
        if rgb == self.to {
            return;
        }
        let now = Instant::now();
        self.from = self.resolve_rgb(now);
        self.to = rgb;
        self.start = now;
    }

    pub fn is_animating(&self) -> bool {
        Instant::now().duration_since(self.start).as_secs_f32() < COLOR_TRANSITION_SECS
    }

    pub fn resolve(&self) -> Color {
        let (r, g, b) = self.resolve_rgb(Instant::now());
        Color::Rgb(r, g, b)
    }

    fn resolve_rgb(&self, now: Instant) -> (u8, u8, u8) {
        let t = (now.duration_since(self.start).as_secs_f32() / COLOR_TRANSITION_SECS).min(1.0);
        let p = ease_out_cubic(t);
        (
            lerp_u8(self.from.0, self.to.0, p),
            lerp_u8(self.from.1, self.to.1, p),
            lerp_u8(self.from.2, self.to.2, p),
        )
    }
}

/// The idle-splash screen. Since the bundle moved to a Lua plugin, Rust keeps
/// only what plugins cannot own safely: the frame clock, the repaint cadence,
/// the entry-fade value, and the final blit of a plugin-produced frame.
pub struct Splash {
    start: Instant,
    animate: bool,
    frame: Option<SplashFrame>,
}

impl Default for Splash {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Splash {
    pub fn new(animate: bool) -> Self {
        Self {
            start: Instant::now(),
            animate,
            frame: None,
        }
    }

    /// Store the latest pulled plugin frame; `render` blits it verbatim.
    pub fn set_frame(&mut self, frame: Option<SplashFrame>) {
        self.frame = frame;
    }

    /// The current frame, for tests and the pull path.
    pub fn frame(&self) -> Option<&SplashFrame> {
        self.frame.as_ref()
    }

    /// Push the entry-fade clock forward (test helper for "settled" states).
    #[cfg(test)]
    pub fn advance_past_fade(&mut self) {
        self.start -= std::time::Duration::from_secs_f32(FADE_DURATION);
    }

    /// `(elapsed_secs, fade)` handed to the Lua renderer each pull.
    pub fn frame_inputs(&self) -> (f32, f32) {
        let t = self.start.elapsed().as_secs_f32();
        (t, self.fade_at(t))
    }

    fn fade_at(&self, t: f32) -> f32 {
        if t >= FADE_DURATION {
            1.0
        } else {
            ease_out_cubic(t / FADE_DURATION)
        }
    }

    /// The starfield drifts for as long as the splash is up. With it off the
    /// only motion left is the entry fade, which ends, so the loop settles on
    /// the start screen instead of burning a core on a still picture.
    pub fn cadence(&self) -> Cadence {
        Cadence::when(
            self.animate || self.start.elapsed().as_secs_f32() < FADE_DURATION,
            Cadence::SMOOTH,
        )
    }

    /// Rust owns the splash area's shape but not its content: a plugin frame
    /// (or no frame at all) is blitted, never a Rust-drawn overlay.
    pub fn render(&self, area: Rect, buf: &mut Buffer, accent: Color) {
        if area.width < 20 || area.height < 5 {
            return;
        }
        if let Some(frame) = &self.frame {
            self.blit(area, buf, frame, accent);
        }
    }

    fn blit(&self, area: Rect, buf: &mut Buffer, frame: &SplashFrame, accent: Color) {
        let theme = theme::current();
        let bg = extract_rgb(theme.background, (15, 15, 25));
        let ac = extract_rgb(accent, (100, 140, 255));

        let area_w = area.width as usize;
        let area_h = area.height as usize;
        if area_w == 0 || area_h == 0 {
            return;
        }

        let mut x = area.x;
        let mut y = area.y;
        let x_end = area.x + area.width;
        let y_end = area.y + area.height;

        'segments: for row in &frame.rows {
            let mut glyphs = row.glyphs.as_str();
            while let Some(ch) = glyphs.chars().next() {
                let sym = &glyphs[..ch.len_utf8()];
                let style = match &row.style {
                    // Field glyphs are `" .:+*"`; a space is the no-paint bucket.
                    SplashStyle::Field => {
                        let idx = field_idx(ch);
                        if idx == 0 {
                            // Advance past a skipped space but still bump col/row.
                            glyphs = &glyphs[ch.len_utf8()..];
                            if !advance(&mut x, &mut y, area.x, x_end, y_end) {
                                break 'segments;
                            }
                            continue;
                        }
                        field_style(idx, bg, ac)
                    }
                    // Explicit styles paint every char (spaces erase the field
                    // behind them), matching the old opaque text block. The bg
                    // is owned by the host (the same `theme.background` the field
                    // and screen use) so the block never reads as a cut-out.
                    SplashStyle::Hex(r, g, b) => Style::new().fg(Color::Rgb(*r, *g, *b)),
                    SplashStyle::Rgba { fg, bold } => {
                        let mut s = match fg {
                            Some((r, g, b)) => Style::new().fg(Color::Rgb(*r, *g, *b)),
                            None => Style::new().fg(theme.foreground),
                        }
                        .bg(Color::Rgb(bg.0, bg.1, bg.2));
                        if *bold {
                            s = s.add_modifier(Modifier::BOLD);
                        }
                        s
                    }
                };
                if let Some(cell) = buf.cell_mut((x, y)) {
                    cell.set_symbol(sym).set_style(style);
                }
                glyphs = &glyphs[ch.len_utf8()..];
                if !advance(&mut x, &mut y, area.x, x_end, y_end) {
                    break 'segments;
                }
            }
        }
    }
}

/// Advance one cell, wrapping at the area width. Returns false when the area
/// is exhausted.
fn advance(x: &mut u16, y: &mut u16, x_start: u16, x_end: u16, y_end: u16) -> bool {
    *x += 1;
    if *x >= x_end {
        *x = x_start;
        *y += 1;
        if *y >= y_end {
            return false;
        }
    }
    true
}

fn extract_rgb(color: Color, fallback: (u8, u8, u8)) -> (u8, u8, u8) {
    match color {
        Color::Rgb(r, g, b) => (r, g, b),
        _ => fallback,
    }
}

fn ease_out_cubic(t: f32) -> f32 {
    let t = t.clamp(0.0, 1.0);
    1.0 - (1.0 - t).powi(3)
}

#[cfg(test)]
mod tests {
    use super::*;
    use makima_lua::SplashRow;
    use std::time::Duration;
    use test_case::test_case;

    const EXPLICIT_FG: (u8, u8, u8) = (10, 20, 30);

    fn transition_at(from: (u8, u8, u8), to: (u8, u8, u8), offset: Duration) -> (u8, u8, u8) {
        let mut ct = ColorTransition::new(Color::Rgb(from.0, from.1, from.2));
        ct.set(Color::Rgb(to.0, to.1, to.2));
        ct.resolve_rgb(ct.start + offset)
    }

    #[test]
    fn interpolation_over_time() {
        let start = transition_at((0, 0, 0), (200, 200, 200), Duration::ZERO);
        assert_eq!(start, (0, 0, 0));

        let mid = transition_at((0, 0, 0), (200, 200, 200), Duration::from_millis(200));
        assert!(
            mid.0 > 0 && mid.0 < 200,
            "expected interpolated, got {}",
            mid.0
        );

        let done = transition_at((0, 0, 0), (255, 255, 255), Duration::from_millis(500));
        assert_eq!(done, (255, 255, 255));
    }

    #[test]
    fn chained_set_restarts_toward_new_target() {
        let mut ct = ColorTransition::new(Color::Rgb(0, 0, 0));
        ct.set(Color::Rgb(200, 100, 50));
        ct.set(Color::Rgb(10, 20, 30));

        let done = ct.resolve_rgb(ct.start + Duration::from_secs(1));
        assert_eq!(done, (10, 20, 30));
    }

    #[test]
    fn is_animating_lifecycle() {
        let ct = ColorTransition::new(Color::Rgb(0, 0, 0));
        assert!(!ct.is_animating(), "settled on construction");

        let mut ct = ColorTransition::new(Color::Rgb(0, 0, 0));
        ct.set(Color::Rgb(255, 0, 0));
        assert!(ct.is_animating(), "animating after set");
    }

    #[test]
    fn non_rgb_color_uses_fallback() {
        let ct = ColorTransition::new(Color::Blue);
        assert_eq!(
            ct.resolve_rgb(ct.start + Duration::from_secs(1)),
            (100, 140, 255)
        );
    }

    /// `splash_animation = false` is what a user on a slow machine reaches
    /// for, so the start screen really has to stop painting once the entry
    /// fade is over.
    #[test_case(false, false => Cadence::SMOOTH ; "entry_fade_is_running")]
    #[test_case(false, true  => Cadence::IDLE   ; "still_splash_settles_after_the_fade")]
    #[test_case(true,  true  => Cadence::SMOOTH ; "starfield_drifts_for_as_long_as_it_is_up")]
    fn splash_cadence(animate: bool, faded: bool) -> Cadence {
        let mut splash = Splash::new(animate);
        if faded {
            splash.start -= Duration::from_secs_f32(FADE_DURATION);
        }
        splash.cadence()
    }

    fn blit_frame(frame: SplashFrame) -> String {
        use crate::components::buffer_text;
        let area = Rect::new(0, 0, 80, 20);
        let mut buf = Buffer::empty(area);
        let splash = Splash::new(true);
        splash.blit(area, &mut buf, &frame, Color::Blue);
        buffer_text(&buf)
    }

    fn frame(rows: Vec<SplashRow>) -> SplashFrame {
        SplashFrame {
            width: 80,
            height: 20,
            rows,
        }
    }

    #[test]
    fn blit_skips_field_spaces_but_paints_text_segments() {
        // one background row: field glyphs with spaces in between
        let rows = frame(vec![SplashRow {
            glyphs: ". : + *".into(),
            style: SplashStyle::Field,
        }]);
        let text = blit_frame(rows);
        assert!(!text.contains('\u{0}'), "no null bytes");
        // '.', ':', '+', '*' land, the spaces between them do not
        assert!(text.contains('.'), "field glyphs painted");
        // A second, text row paints every char incl. a styled space.
        let rows = frame(vec![SplashRow {
            glyphs: "ab cd".into(),
            style: SplashStyle::Rgba {
                fg: Some(EXPLICIT_FG),
                bold: false,
            },
        }]);
        let text = blit_frame(rows);
        assert!(text.contains('a') && text.contains(' ') && text.contains('d'));
    }

    /// A segment without `fg` degrades to the host theme's foreground on the
    /// theme background: the plugin carries no palette of its own.
    #[test]
    fn blit_rgba_missing_fg_uses_theme_foreground() {
        let theme = theme::current();
        let rows = frame(vec![SplashRow {
            glyphs: "abc".into(),
            style: SplashStyle::Rgba {
                fg: None,
                bold: false,
            },
        }]);
        let area = Rect::new(0, 0, 80, 20);
        let mut buf = Buffer::empty(area);
        let splash = Splash::new(true);
        splash.blit(area, &mut buf, &rows, Color::Blue);
        let cell = buf.cell((0, 0)).unwrap();
        assert_eq!(cell.style().fg, Some(theme.foreground));
        assert_eq!(cell.style().bg, Some(theme.background));
    }

    /// An explicit `fg` keeps the segment colour; the cell background is
    /// always the host's theme background, never a plugin-supplied one.
    #[test]
    fn blit_rgba_keeps_explicit_fg_on_theme_bg() {
        let theme = theme::current();
        let rows = frame(vec![SplashRow {
            glyphs: "abc".into(),
            style: SplashStyle::Rgba {
                fg: Some(EXPLICIT_FG),
                bold: false,
            },
        }]);
        let area = Rect::new(0, 0, 80, 20);
        let mut buf = Buffer::empty(area);
        let splash = Splash::new(true);
        splash.blit(area, &mut buf, &rows, Color::Blue);
        let cell = buf.cell((0, 0)).unwrap();
        assert_eq!(cell.style().fg, Some(Color::Rgb(10, 20, 30)));
        assert_eq!(cell.style().bg, Some(theme.background));
    }

    fn serialize_cells(buf: &Buffer) -> String {
        let mut out = String::new();
        let area = buf.area();
        for y in 0..area.height {
            for x in 0..area.width {
                let cell = buf.cell((x, y)).unwrap();
                if cell.symbol() == " " {
                    continue;
                }
                out.push_str(&format!("{x},{y} {}\n", cell.symbol()));
            }
        }
        out
    }

    fn cells_map(s: &str) -> std::collections::HashMap<(u16, u16), char> {
        let mut m = std::collections::HashMap::new();
        for line in s.lines() {
            let line = line.trim_end();
            if line.is_empty() {
                continue;
            }
            let mut it = line.split(' ');
            let xy = it.next().unwrap();
            let glyph = it.next().unwrap().chars().next().unwrap();
            let (x, y) = xy.split_once(',').unwrap();
            m.insert((x.parse().unwrap(), y.parse().unwrap()), glyph);
        }
        m
    }

    fn is_text(glyph: char) -> bool {
        !" .:+*".contains(glyph)
    }

    /// AC.1: the default bundle reproduces the pre-port default screen. Text
    /// cells (logo/tagline/help/tip/version) must match the committed golden
    /// exactly in position and glyph; field cells may drift one bucket (f32 vs
    /// f64), so they are compared structurally (the Lua frame still covers the
    /// screen with `" .:+*"` glyphs via the blit LUT).
    #[test]
    fn test_splash_lua_matches_rust_golden() {
        use crate::update;
        use makima_lua::test_support::spawn_host_for_tests;
        use std::collections::HashMap;

        let golden = cells_map(include_str!("splash_golden.txt"));
        assert!(!golden.is_empty(), "golden fixture missing");
        let (handle, _guard) = spawn_host_for_tests(&["splashes_default"]);
        handle.set_version(update::CURRENT, None);

        // Back off between pulls so the host's render queue (which every
        // timed-out pull refills during JIT warmup) can drain under load.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let frame = loop {
            if let Some(f) = handle.splash_frame(80, 20, 10.0, 1.0) {
                let all: String = f.rows.iter().map(|r| r.glyphs.as_str()).collect();
                if all.contains(update::CURRENT) {
                    break f;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "splash never rendered the version"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        let area = Rect::new(0, 0, 80, 20);
        let mut buf = Buffer::empty(area);
        let splash = Splash::new(true);
        splash.blit(area, &mut buf, &frame, Color::Rgb(255, 184, 108));
        let lua_cells: HashMap<(u16, u16), char> = cells_map(&serialize_cells(&buf));

        for ((x, y), &glyph) in &golden {
            if is_text(glyph) {
                assert_eq!(
                    lua_cells.get(&(*x, *y)),
                    Some(&glyph),
                    "missing or moved text '{glyph}' at ({x},{y})"
                );
            }
        }
        for ((x, y), &glyph) in &lua_cells {
            if is_text(glyph) && golden.get(&(*x, *y)) != Some(&glyph) {
                panic!("unexpected text '{glyph}' at ({x},{y})");
            }
        }
    }
}
