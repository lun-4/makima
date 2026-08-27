//! Wall-clock timestamps and relative-age formatting.
//!
//! `now()` returns a `{secs, nanosecs}` timestamp — the same wall clock the
//! host and storage layer use for session `updated_at`, but subdivided so a
//! plugin never hits Lua's double precision ceiling. Times are passed to the
//! maki time APIs as opaque objects instead of raw numbers, so callers lean
//! on `ago` (and `at` to wrap stored whole-second values) instead of hand
//! rolling epoch arithmetic.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Table};

const NANOS_PER_SEC: u128 = 1_000_000_000;
const FIELD_RANGE_ERR: &str = "timestamp secs must be an integer; nanosecs an integer 0..1e9";

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

fn ts_from_table(t: &Table) -> LuaResult<Timestamp> {
    let secs = t.get::<u64>("secs")?;
    let nanosecs = t.get::<u32>("nanosecs")?;
    if nanosecs >= NANOS_PER_SEC as u32 {
        return Err(mlua::Error::runtime(FIELD_RANGE_ERR));
    }
    Ok(Timestamp { secs, nanosecs })
}

fn elapsed_ns(from: Timestamp, to: Timestamp) -> u128 {
    let from = from.secs as u128 * NANOS_PER_SEC + from.nanosecs as u128;
    let to = to.secs as u128 * NANOS_PER_SEC + to.nanosecs as u128;
    to.saturating_sub(from)
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

/// Format {instant} as a relative age like `3h ago`, or `just now` for less
/// than a minute. {instant} is a timestamp from `maki.time.now` (or
/// `maki.time.at`). If {reference} is given, age is measured from it instead
/// of the current time.
///
/// A timestamp in the future renders as `just now`.
///
/// @param instant table Timestamp from `maki.time.now`/`maki.time.at`.
/// @param reference table? Timestamp to measure from; defaults to `maki.time.now`.
/// @return (string) Relative age, e.g. `42min ago`.
/// @example
/// local t0 = maki.time.now()
/// -- ...work...
/// print(maki.time.ago(t0))
#[lua_fn]
fn ago(_lua: &Lua, instant: Table, reference: Option<Table>) -> LuaResult<String> {
    let from = ts_from_table(&instant)?;
    let to = match reference {
        Some(t) => ts_from_table(&t)?,
        None => now_timestamp(),
    };
    Ok(format_ago((elapsed_ns(from, to) / NANOS_PER_SEC) as u64))
}

/// Shared relative-age formatter. Consumed by `maki.time.ago` for plugins and
/// by makima's own pickers (they feed it `Instant::elapsed().as_secs()`, the
/// same whole-second duration).
pub fn format_ago(secs: u64) -> String {
    if secs < 60 {
        return "just now".into();
    }
    let mins = secs / 60;
    if mins < 60 {
        return format!("{mins}min ago");
    }
    let hrs = mins / 60;
    if hrs < 24 {
        return format!("{hrs}h ago");
    }
    format!("{}d ago", hrs / 24)
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

    const ONE_MIN: u64 = 60;
    const ONE_HOUR: u64 = 3600;
    const ONE_DAY: u64 = 86400;

    #[test_case(0 => "just now" ; "zero")]
    #[test_case(ONE_MIN - 1 => "just now" ; "under a minute")]
    #[test_case(ONE_MIN => "1min ago" ; "one minute")]
    #[test_case(5 * ONE_MIN => "5min ago" ; "five minutes")]
    #[test_case(ONE_HOUR - 1 => "59min ago" ; "just under an hour")]
    #[test_case(ONE_HOUR => "1h ago" ; "one hour")]
    #[test_case(2 * ONE_HOUR => "2h ago" ; "two hours")]
    #[test_case(ONE_DAY - 1 => "23h ago" ; "just under a day")]
    #[test_case(ONE_DAY => "1d ago" ; "one day")]
    #[test_case(3 * ONE_DAY => "3d ago" ; "three days")]
    #[test_case(30 * ONE_DAY => "30d ago" ; "a month of days")]
    fn formats_elapsed(secs: u64) -> String {
        format_ago(secs)
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
    fn ago_measures_reference_to_instant_in_seconds() {
        let lua = Lua::new();
        let epoch = system_now().as_secs();
        assert_eq!(
            call_ago(&lua, ts_tbl(&lua, epoch - 300, 0), ts_tbl(&lua, epoch, 0)),
            "5min ago"
        );
        assert_eq!(
            call_ago(&lua, ts_tbl(&lua, epoch - 7200, 0), ts_tbl(&lua, epoch, 0)),
            "2h ago"
        );
    }

    #[test]
    fn ago_counts_sub_second_nanosecs() {
        let lua = Lua::new();
        // 59.5 seconds apart -> under a minute once truncated to whole secs.
        let secs = system_now().as_secs();
        let out = call_ago(
            &lua,
            ts_tbl(&lua, secs - 59, 500_000_000),
            ts_tbl(&lua, secs, 0),
        );
        assert_eq!(out, "just now");
    }

    #[test]
    fn ago_clamps_future_and_rejects_bad_range() {
        let lua = Lua::new();
        let secs = system_now().as_secs();
        // Future instant -> "just now".
        assert_eq!(
            call_ago(&lua, ts_tbl(&lua, secs + ONE_DAY, 0), ts_tbl(&lua, secs, 0)),
            "just now"
        );
        let bad = ts_tbl(&lua, secs, NANOS_PER_SEC as u32);
        let err = create_time_table(&lua)
            .unwrap()
            .get::<mlua::Function>("ago")
            .unwrap()
            .call::<String>((bad, ts_tbl(&lua, secs, 0)))
            .unwrap_err();
        assert!(err.to_string().contains(FIELD_RANGE_ERR));
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
        // A second call must not move backwards in real time.
        let (secs2, nanosecs2) = read(now.call::<Table>(()).unwrap());
        assert!(secs2 > secs || (secs2 == secs && nanosecs2 >= nanosecs));
    }

    #[test]
    fn at_wraps_whole_seconds() {
        let lua = Lua::new();
        let at: mlua::Function = create_time_table(&lua).unwrap().get("at").unwrap();
        let t = at.call::<Table>(1_780_000_000u64).unwrap();
        assert_eq!(t.get::<u64>("secs").unwrap(), 1_780_000_000);
        assert_eq!(t.get::<u32>("nanosecs").unwrap(), 0);
    }
}
