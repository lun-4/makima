use std::collections::VecDeque;
use std::time::{Duration, Instant};

use mlua::{Lua, Result as LuaResult, Table, Value};

use crate::docs::FnDoc;

/// Max time the UI waits for a splash frame from the host. The pull is
/// best-effort: a dead or bogged-down renderer never freezes a frame. It
/// blocks at most this long only when the host is slow (a fast host replies
/// immediately), so a generous value keeps a load-starved Lua host from
/// failing a frame without slowing the common case.
pub const SPLASH_PULL_TIMEOUT: Duration = Duration::from_millis(100);

/// Style of one splash segment. `Field` keeps the fixed `" .:+*"` LUT so the
/// bundled starfield is one segment per background row with no per-cell hex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplashStyle {
    Field,
    Hex(u8, u8, u8),
    Rgba {
        fg: (u8, u8, u8),
        bg: (u8, u8, u8),
        bold: bool,
    },
}

#[derive(Debug, Clone)]
pub struct SplashRow {
    pub glyphs: String,
    pub style: SplashStyle,
}

/// A completed splash area. `rows` is the flattened, per-row list of segments
/// (row 0's segments then row 1's and so on), where each row's glyph strings
/// concatenate to `width` after truncation. The blitter walks them left to
/// right, wrapping at `width`, so it never needs explicit row boundaries.
#[derive(Debug, Clone)]
pub struct SplashFrame {
    pub width: u16,
    pub height: u16,
    pub rows: Vec<SplashRow>,
}

impl SplashFrame {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Version/update information the Rust host pushes to Lua and plugins read
/// via `maki.version()`. Cached on the Lua thread (the request handler and
/// every plugin share it), so a plain struct in `app_data` suffices.
#[derive(Debug, Clone, Default)]
pub struct VersionInfo {
    pub current: String,
    pub latest: Option<String>,
}

/// Splash render timings the host measures and plugins read via
/// `maki.perf.timings()`. Cached on the Lua thread like [`VersionInfo`]: the
/// frame request handler records, plugins read, no locking needed.
#[derive(Debug, Clone, Default)]
pub struct PerfInfo {
    /// Duration of the most recent `splash.render` invocation, in ms.
    pub render_ms: f32,
    /// Completion timestamps of recent renders; [`Self::fps`] counts the
    /// ones inside the trailing second.
    renders: VecDeque<Instant>,
}

impl PerfInfo {
    /// Register one completed render. The elapsed time becomes `render_ms`;
    /// the completion timestamp feeds the fps window.
    pub fn record_render(&mut self, render_elapsed: Duration) {
        let now = Instant::now();
        self.render_ms = render_elapsed.as_secs_f32() * 1000.0;
        self.renders.push_back(now);
        while self
            .renders
            .front()
            .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(1))
        {
            self.renders.pop_front();
        }
    }

    /// Renders completed in the trailing second.
    pub fn fps(&self) -> f32 {
        let cutoff = Instant::now() - Duration::from_secs(1);
        self.renders.iter().filter(|t| **t > cutoff).count() as f32
    }
}

fn parse_hex(s: &str) -> Option<(u8, u8, u8)> {
    let s = s.strip_prefix('#')?;
    if s.len() != 6 {
        return None;
    }
    Some((
        u8::from_str_radix(&s[0..2], 16).ok()?,
        u8::from_str_radix(&s[2..4], 16).ok()?,
        u8::from_str_radix(&s[4..6], 16).ok()?,
    ))
}

fn parse_style(value: Value) -> LuaResult<SplashStyle> {
    match value {
        Value::String(s) => {
            let s = s.to_string_lossy();
            if s == "field" {
                return Ok(SplashStyle::Field);
            }
            parse_hex(s.as_ref())
                .map(|(r, g, b)| SplashStyle::Hex(r, g, b))
                .ok_or_else(|| mlua::Error::runtime(format!("unknown splash style '{s}'")))
        }
        Value::Table(t) => {
            let fg = t
                .get::<Option<String>>("fg")?
                .ok_or_else(|| mlua::Error::runtime("splash style table missing fg"))?;
            let bg = t
                .get::<Option<String>>("bg")?
                .ok_or_else(|| mlua::Error::runtime("splash style table missing bg"))?;
            let bold: bool = t.get::<bool>("bold").unwrap_or(false);
            let fg = parse_hex(&fg)
                .ok_or_else(|| mlua::Error::runtime(format!("bad splash fg color '{fg}'")))?;
            let bg = parse_hex(&bg)
                .ok_or_else(|| mlua::Error::runtime(format!("bad splash bg color '{bg}'")))?;
            Ok(SplashStyle::Rgba { fg, bg, bold })
        }
        _ => Err(mlua::Error::runtime(
            "splash segment style must be a string or a {fg,bg,bold} table",
        )),
    }
}

fn parse_segment(value: Value) -> LuaResult<SplashRow> {
    let Value::Table(t) = value else {
        return Err(mlua::Error::runtime(
            "each splash row must be an array of {glyphs, style} segments",
        ));
    };
    let glyphs = t
        .get::<Option<String>>("glyphs")?
        .ok_or_else(|| mlua::Error::runtime("splash segment missing glyphs"))?;
    let style_value = t
        .get::<Option<Value>>("style")?
        .ok_or_else(|| mlua::Error::runtime("splash segment missing style"))?;
    let style = parse_style(style_value)?;
    Ok(SplashRow { glyphs, style })
}

/// Convert a `splash.render` Lua return (a table of rows) into a `SplashFrame`.
/// Row glyph strings are truncated so each row never exceeds `width`; missing
/// rows or short rows are left as-is (the blitter clips and skips spaces).
pub(crate) fn frame_from_lua(value: Value, width: u16, height: u16) -> LuaResult<SplashFrame> {
    let Value::Table(rows_table) = value else {
        return Err(mlua::Error::runtime("splash.render must return a table"));
    };
    let mut rows: Vec<SplashRow> = Vec::new();
    for i in 1..=height {
        let row: Option<Value> = rows_table.get(i as i64)?;
        let Some(row) = row else { continue };
        let Value::Table(row_table) = row else {
            return Err(mlua::Error::runtime("each splash row must be a table"));
        };
        let mut row_len = 0usize;
        for seg in row_table.sequence_values::<Value>() {
            let mut row = parse_segment(seg?)?;
            row_len += row.glyphs.chars().count();
            if row_len > width as usize {
                while row_len > width as usize {
                    row.glyphs.pop();
                    row_len -= 1;
                }
            }
            rows.push(row);
            if row_len >= width as usize {
                break;
            }
        }
    }
    Ok(SplashFrame {
        width,
        height,
        rows,
    })
}

/// Guard-free read-only `maki.version()` returning `{ current, latest,
/// update_available }`. The host owns the update check and pushes the result
/// via [`Request::SetVersion`]; plugins only mirror it.
pub(crate) fn register_version_api(lua: &Lua, maki: &Table) -> LuaResult<()> {
    let f = lua.create_function(|lua, ()| {
        let info = lua
            .app_data_ref::<VersionInfo>()
            .as_deref()
            .cloned()
            .unwrap_or_default();
        let t = lua.create_table()?;
        t.set("current", info.current.as_str())?;
        t.set("latest", info.latest.as_deref())?;
        t.set("update_available", info.latest.is_some())?;
        Ok(t)
    })?;
    maki.set("version", f)?;
    Ok(())
}

/// Outcome of a host pull. The UI treats `Missing` (the renderer answered that
/// it has nothing) as a dead renderer to suppress, while `Unknown` (the reply
/// was still computing when the timeout hit, or the handle is disconnected)
/// means "retry": a cold Lua JIT routinely misses the first frame or two but
/// warms up within the entry fade.
#[derive(Debug, Clone)]
pub enum SplashPull {
    Frame(SplashFrame),
    Missing,
    Unknown,
}

impl SplashPull {
    pub fn frame(self) -> Option<SplashFrame> {
        match self {
            SplashPull::Frame(f) => Some(f),
            _ => None,
        }
    }
}

/// Documentation for `maki.version()`, which is registered by hand (see
/// `register_version_api`) so it cannot carry a `#[lua_fn]` doc block.
#[allow(non_upper_case_globals)]
pub(crate) const version__doc: FnDoc = FnDoc {
    name: "version",
    args: "",
    desc: "Read the current makima version and, if the built-in update check found one, the newer version. Rust owns the update check; plugins only mirror it, so the version text and update notice belong to whoever draws them.",
    params: &[],
    returns: "(table) { current = string, latest = string|nil, update_available = boolean }",
    example: "local v = maki.version()\nif v.update_available then\n  print(\"run makima update to get v\" .. v.latest)\nend",
    guard: None,
};
