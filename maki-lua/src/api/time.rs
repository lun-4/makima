//! Wall-clock timestamps and relative-age formatting.
//!
//! `now()` returns a `{secs, nanosecs}` timestamp — the same wall clock the
//! host and storage layer use for session `updated_at`, but subdivided so a
//! plugin never hits Lua's double precision ceiling. Times are passed to the
//! maki time APIs as opaque objects instead of raw numbers, so callers lean
//! on `ago` (and `at` to wrap stored whole-second values) instead of hand
//! rolling epoch arithmetic.

use std::fmt::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Table, Value};

const NANOS_PER_SEC: u128 = 1_000_000_000;
const MILLIS_PER_SEC: u128 = 1_000;
const SECS_PER_MIN: u64 = 60;
const SECS_PER_HOUR: u64 = 60 * SECS_PER_MIN;
const SECS_PER_DAY: u64 = 24 * SECS_PER_HOUR;
const U64_RANGE_END: f64 = 18_446_744_073_709_551_616.0;
const TIMESTAMP_FIELD_ERR: &str =
    "timestamp requires non-negative integer secs and nanosecs 0..1e9";

#[derive(Clone, Copy)]
struct Timestamp {
    secs: u64,
    nanosecs: u32,
}

fn system_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
}

fn now_timestamp() -> Timestamp {
    let d = system_now();
    Timestamp {
        secs: d.as_secs(),
        nanosecs: d.subsec_nanos(),
    }
}

fn ts_to_table(lua: &Lua, ts: Timestamp) -> LuaResult<Table> {
    let t = lua.create_table()?;
    t.set("secs", ts.secs)?;
    t.set("nanosecs", ts.nanosecs)?;
    Ok(t)
}

fn timestamp_field(t: &Table, field: &str) -> LuaResult<u64> {
    match t.get::<Value>(field)? {
        Value::Integer(value) if value >= 0 => Ok(value as u64),
        Value::Number(value)
            if value.is_finite()
                && (0.0..U64_RANGE_END).contains(&value)
                && value.fract() == 0.0 =>
        {
            Ok(value as u64)
        }
        _ => Err(mlua::Error::runtime(TIMESTAMP_FIELD_ERR)),
    }
}

/// Decode the opaque Lua timestamp table into a Unix-epoch [`Duration`].
pub(crate) fn duration_from_table(t: &Table) -> LuaResult<Duration> {
    let secs = timestamp_field(t, "secs")?;
    let nanosecs = timestamp_field(t, "nanosecs")?;
    if nanosecs >= NANOS_PER_SEC as u64 {
        return Err(mlua::Error::runtime(TIMESTAMP_FIELD_ERR));
    }
    Duration::from_secs(secs)
        .checked_add(Duration::from_nanos(nanosecs))
        .ok_or_else(|| mlua::Error::runtime(TIMESTAMP_FIELD_ERR))
}

/// Return the current wall-clock time as a timestamp table.
///
/// The table is `{ secs = integer, nanosecs = integer }` where `secs` is
/// whole seconds since the Unix epoch (`1970-01-01T00:00:00Z`) and `nanosecs`
/// is the sub-second part, `0 .. 999_999_999`. Split like this, both fields
/// stay exact in a Lua number (each under 2^53), so the timestamp keeps full
/// nanosecond precision — the same clock the host uses for session
/// `updated_at`, just finer than its whole seconds.
///
/// Treat the returned table as opaque; pass it to `maki.time.ago` rather than
/// doing epoch math by hand. To wrap a stored whole-second value (like a
/// session's `updated_at`) use `maki.time.at`.
///
/// @return (table) `{secs = integer, nanosecs = integer}` timestamp.
/// @example
/// local t0 = maki.time.now()
/// -- ...work...
/// print(maki.time.ago(t0))
#[lua_fn]
fn now(lua: &Lua) -> LuaResult<Table> {
    ts_to_table(lua, now_timestamp())
}

/// Wrap a whole-second Unix timestamp (e.g. a session's `updated_at`) into a
/// timestamp object readable by `maki.time.ago`.
///
/// @param secs integer Whole seconds since the Unix epoch.
/// @return (table) `{secs = secs, nanosecs = 0}` timestamp.
/// @example
/// print(maki.time.ago(maki.time.at(session.updated_at)))
#[lua_fn]
fn at(lua: &Lua, secs: u64) -> LuaResult<Table> {
    ts_to_table(lua, Timestamp { secs, nanosecs: 0 })
}

/// Format {instant} as a live delta like `9s` or `4d 7h 23m 9s`.
/// {instant} is a timestamp from `maki.time.now` (or `maki.time.at`). If
/// {reference} is given, age is measured from it instead of the current time.
/// Fractions truncate to whole seconds, and higher zero units are omitted.
/// A timestamp in the future renders as `0s`.
///
/// @param instant table Timestamp from `maki.time.now`/`maki.time.at`.
/// @param reference table? Timestamp to measure from; defaults to `maki.time.now`.
/// @return (string) Elapsed delta, e.g. `2m 9s`.
/// @example
/// local t0 = maki.time.now()
/// -- ...work...
/// print(maki.time.ago(t0))
#[lua_fn]
fn ago(_lua: &Lua, instant: Table, reference: Option<Table>) -> LuaResult<String> {
    let from = duration_from_table(&instant)?;
    let to = match reference {
        Some(t) => duration_from_table(&t)?,
        None => system_now(),
    };
    Ok(format_ago(to.saturating_sub(from)))
}

fn push_unit(out: &mut String, value: u64, unit: char) {
    if value == 0 {
        return;
    }
    if !out.is_empty() {
        out.push(' ');
    }
    let _ = write!(out, "{value}{unit}");
}

/// Shared elapsed-time formatter. Consumed by `maki.time.ago` for plugins and
/// by makima's own pickers. Fractions truncate to whole seconds, and higher
/// zero units stay omitted.
pub fn format_ago(elapsed: Duration) -> String {
    let secs = (elapsed.as_millis() / MILLIS_PER_SEC) as u64;
    let mut out = String::with_capacity(24);
    push_unit(&mut out, secs / SECS_PER_DAY, 'd');
    push_unit(&mut out, secs % SECS_PER_DAY / SECS_PER_HOUR, 'h');
    push_unit(&mut out, secs % SECS_PER_HOUR / SECS_PER_MIN, 'm');
    if !out.is_empty() {
        out.push(' ');
    }
    let _ = write!(out, "{}s", secs % SECS_PER_MIN);
    out
}

lua_table! {
    /// Wall-clock timestamps and relative-age formatting.
    ///
    /// `now` returns `{secs, nanosecs}` since the Unix epoch, the same clock
    /// the host uses for session `updated_at` but with full nanosecond
    /// precision. Timestamps are opaque objects you hand to `ago` — not raw
    /// numbers to subtract — so callers get one consistent relative-age
    /// format instead of reimplementing epoch math per plugin.
    ///
    /// ```lua
    /// local t0 = maki.time.now()
    /// -- work...
    /// print(maki.time.ago(t0))
    /// ```
    "maki.time" => pub(crate) fn create_time_table(), DOCS [
        now, at, ago,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const ONE_SEC_MS: u64 = 1_000;
    const ONE_MIN_MS: u64 = 60 * ONE_SEC_MS;
    const ONE_HOUR_MS: u64 = 60 * ONE_MIN_MS;
    const ONE_DAY_MS: u64 = 24 * ONE_HOUR_MS;
    const EPOCH_SECS: u64 = 1_700_000_000;

    #[test_case(0 => "0s" ; "zero")]
    #[test_case(9_400 => "9s" ; "subsecond truncated")]
    #[test_case(ONE_MIN_MS - 1 => "59s" ; "just under a minute")]
    #[test_case(ONE_MIN_MS => "1m 0s" ; "one minute")]
    #[test_case(ONE_MIN_MS + 5_500 => "1m 5s" ; "minute and seconds")]
    #[test_case(ONE_HOUR_MS - 1 => "59m 59s" ; "just under an hour")]
    #[test_case(ONE_HOUR_MS => "1h 0s" ; "one hour")]
    #[test_case(ONE_HOUR_MS + ONE_MIN_MS + 1_200 => "1h 1m 1s" ; "hour minute seconds")]
    #[test_case(ONE_DAY_MS => "1d 0s" ; "one day")]
    #[test_case(4 * ONE_DAY_MS + 7 * ONE_HOUR_MS + 23 * ONE_MIN_MS + 9_400 => "4d 7h 23m 9s" ; "compound")]
    #[test_case(30 * ONE_DAY_MS => "30d 0s" ; "many days")]
    fn formats_compound_elapsed_seconds(ms: u64) -> String {
        format_ago(Duration::from_millis(ms))
    }

    #[test]
    fn format_truncates_subsecond() {
        assert_eq!(format_ago(Duration::from_micros(9_499_999)), "9s");
    }

    fn ts_tbl(lua: &Lua, secs: u64, nanosecs: u32) -> Table {
        let t = lua.create_table().unwrap();
        t.set("secs", secs).unwrap();
        t.set("nanosecs", nanosecs).unwrap();
        t
    }

    fn call_ago(lua: &Lua, from: Table, to: Table) -> String {
        let t = create_time_table(lua).unwrap();
        let ago: mlua::Function = t.get("ago").unwrap();
        ago.call((from, to)).unwrap()
    }

    #[test]
    fn ago_preserves_fractional_reference_and_clamps_future() {
        let lua = Lua::new();
        assert_eq!(
            call_ago(
                &lua,
                ts_tbl(&lua, EPOCH_SECS - 60, 0),
                ts_tbl(&lua, EPOCH_SECS, 500_000_000),
            ),
            "1m 0s"
        );
        assert_eq!(
            call_ago(
                &lua,
                ts_tbl(&lua, EPOCH_SECS - 60, 500_000_000),
                ts_tbl(&lua, EPOCH_SECS, 0),
            ),
            "59s"
        );
        assert_eq!(
            call_ago(
                &lua,
                ts_tbl(&lua, EPOCH_SECS + ONE_HOUR_MS / 1_000, 0),
                ts_tbl(&lua, EPOCH_SECS, 0),
            ),
            "0s"
        );
    }

    #[test]
    fn timestamp_decoder_rejects_invalid_fields() {
        let lua = Lua::new();
        let into_duration = |t: &Table| duration_from_table(t).map(|d| d.as_nanos());

        let bad_range = ts_tbl(&lua, EPOCH_SECS, NANOS_PER_SEC as u32);
        assert!(into_duration(&bad_range).is_err());

        let missing = lua.create_table().unwrap();
        assert!(into_duration(&missing).is_err());

        let negative = lua.create_table().unwrap();
        negative.set("secs", -1).unwrap();
        negative.set("nanosecs", 0).unwrap();
        assert!(into_duration(&negative).is_err());

        let fractional = lua.create_table().unwrap();
        fractional.set("secs", 1.5).unwrap();
        fractional.set("nanosecs", 0).unwrap();
        assert!(into_duration(&fractional).is_err());

        let oversized = lua.create_table().unwrap();
        oversized.set("secs", 1e20).unwrap();
        oversized.set("nanosecs", 0).unwrap();
        assert!(into_duration(&oversized).is_err());

        let err = create_time_table(&lua)
            .unwrap()
            .get::<mlua::Function>("ago")
            .unwrap()
            .call::<String>((bad_range, ts_tbl(&lua, EPOCH_SECS, 0)))
            .unwrap_err();
        assert!(err.to_string().contains(TIMESTAMP_FIELD_ERR));
    }

    #[test]
    fn now_is_wall_clock_timestamp() {
        let lua = Lua::new();
        let now: mlua::Function = create_time_table(&lua).unwrap().get("now").unwrap();
        let read = |t: Table| -> (u64, u32) {
            let secs: u64 = t.get("secs").unwrap();
            let nanosecs: u32 = t.get("nanosecs").unwrap();
            (secs, nanosecs)
        };
        let (secs, nanosecs) = read(now.call::<Table>(()).unwrap());
        assert!(secs > 1_700_000_000, "should be contemporary epoch seconds");
        assert!(nanosecs < NANOS_PER_SEC as u32);
        let (secs2, nanosecs2) = read(now.call::<Table>(()).unwrap());
        assert!(secs2 > secs || (secs2 == secs && nanosecs2 >= nanosecs));
    }

    #[test]
    fn at_wraps_whole_seconds() {
        let lua = Lua::new();
        let at: mlua::Function = create_time_table(&lua).unwrap().get("at").unwrap();
        let t = at.call::<Table>(EPOCH_SECS).unwrap();
        assert_eq!(t.get::<u64>("secs").unwrap(), EPOCH_SECS);
        assert_eq!(t.get::<u32>("nanosecs").unwrap(), 0);
    }
}
