//! `maki.perf`: performance instrumentation for the interactive UI. The host
//! measures splash renders; plugins read the numbers and draw their own
//! readouts (the bundled `perf` plugin turns one on from Lua).

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Table};

/// Read the splash render timings the host measured: how long the most
/// recent `splash.render` invocation took and how many renders completed in
/// the trailing second. The numbers come from the host that drives the
/// render, so they include the whole call (queue wait, warm-up, deadline)
/// and need no instrumentation inside the splash itself.
///
/// A still splash settles to `0 fps` once nothing animates, which is the
/// point: it proves the splash is not burning CPU.
///
/// @return (table) `{ render_ms = number, fps = number }`.
/// @example
/// local t = maki.perf.timings()
#[lua_fn]
fn timings(lua: &Lua) -> LuaResult<Table> {
    let perf = lua
        .app_data_ref::<crate::splash::PerfInfo>()
        .as_deref()
        .cloned()
        .unwrap_or_default();
    let t = lua.create_table()?;
    t.set("render_ms", perf.render_ms)?;
    t.set("fps", perf.fps())?;
    Ok(t)
}

lua_table! {
    /// Performance instrumentation for splashes and the UI. The host
    /// measures splash renders; plugins read the timings and draw their own
    /// readouts.
    "maki.perf" => pub(crate) fn create_perf_table(), DOCS [timings]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splash::PerfInfo;
    use std::time::Duration;

    fn timings(lua: &Lua) -> Table {
        let perf = create_perf_table(lua).unwrap();
        let call: mlua::Function = perf.get("timings").unwrap();
        call.call(()).unwrap()
    }

    #[test]
    fn timings_without_host_records_zeros() {
        let lua = Lua::new();
        let t = timings(&lua);
        assert_eq!(t.get::<f32>("render_ms").unwrap(), 0.0);
        assert_eq!(t.get::<f32>("fps").unwrap(), 0.0);
    }

    #[test]
    fn timings_roundtrip_host_recorded_renders() {
        let lua = Lua::new();
        let mut perf = PerfInfo::default();
        for _ in 0..3 {
            perf.record_render(Duration::from_millis(7));
        }
        lua.set_app_data(perf);
        let t = timings(&lua);
        assert_eq!(t.get::<f32>("render_ms").unwrap(), 7.0);
        assert_eq!(t.get::<f32>("fps").unwrap(), 3.0);
    }
}
