//! `maki.perf`: opt-in performance instrumentation for the interactive UI.
//! Nothing here runs unless a plugin calls it; every helper is a readout or
//! overlay that the plugin turns on and off itself.

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult};

use crate::api::util::command::{UiAction, ui_send};
use crate::api::util::pair::{Pair, try_pair};

/// Turns on the splash fps overlay: a live readout drawn into the top of
/// the input bar showing the current splash's frame rate and the UI-side
/// round-trip time per frame (from request to arrived frame, smoothed).
///
/// The numbers come from actual frame arrivals, so a renderer that claims
/// smooth animation but delivers few frames shows up as low fps, and a slow
/// `splash.render` shows up as a high round-trip. A still splash settles to
/// `0 fps` once nothing animates, which is the point: it proves the splash is
/// not burning CPU. The overlay also refreshes the readout about four times
/// a second while enabled, so disable it once you are done measuring.
///
/// @return (boolean|nil, string|nil) `true` once the UI accepted the toggle,
/// or nil and an error without an interactive UI attached.
/// @example
/// maki.perf.enable_splash_fps_overlay()
#[lua_fn]
fn enable_splash_fps_overlay(
    _lua: &Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
) -> LuaResult<Pair<bool>> {
    try_pair!(ui_send(
        tx.as_ref(),
        UiAction::SplashFpsOverlay { enabled: true }
    ));
    Ok((Some(true), None))
}

/// Turns the splash fps overlay back off, hiding the fps readout and
/// stopping its periodic refreshes.
///
/// @return (boolean|nil, string|nil) `true` once the UI accepted the toggle,
/// or nil and an error without an interactive UI attached.
/// @example
/// maki.perf.disable_splash_fps_overlay()
#[lua_fn]
fn disable_splash_fps_overlay(
    _lua: &Lua,
    #[ctx] tx: Option<flume::Sender<UiAction>>,
) -> LuaResult<Pair<bool>> {
    try_pair!(ui_send(
        tx.as_ref(),
        UiAction::SplashFpsOverlay { enabled: false }
    ));
    Ok((Some(true), None))
}

lua_table! {
    /// Performance readouts for splashes and the UI. Each function turns an
    /// opt-in instrument on and off; none of them run on their own.
    "maki.perf" => pub(crate) fn create_perf_table(tx: Option<flume::Sender<UiAction>>),
    DOCS [enable_splash_fps_overlay(tx), disable_splash_fps_overlay(tx)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::command::NO_UI_ERR;
    use mlua::Value;

    fn perf_table(tx: Option<flume::Sender<UiAction>>) -> Lua {
        let lua = Lua::new();
        let t = create_perf_table(&lua, tx).unwrap();
        lua.globals().set("perf", t).unwrap();
        lua
    }

    #[test]
    fn splash_fps_overlay_without_ui_returns_error_pair() {
        let (val, err): (Value, Option<String>) = perf_table(None)
            .load("return perf.enable_splash_fps_overlay()")
            .eval()
            .unwrap();
        assert!(val.is_nil());
        assert_eq!(err.as_deref(), Some(NO_UI_ERR));
    }

    #[test]
    fn splash_fps_overlay_roundtrips_toggle_through_ui_channel() {
        let (tx, rx) = flume::unbounded::<UiAction>();
        let lua = perf_table(Some(tx));
        lua.load("perf.enable_splash_fps_overlay()").exec().unwrap();
        lua.load("perf.disable_splash_fps_overlay()")
            .exec()
            .unwrap();
        let (a, b) = (rx.recv().unwrap(), rx.recv().unwrap());
        assert!(matches!(
            (a, b),
            (
                UiAction::SplashFpsOverlay { enabled: true },
                UiAction::SplashFpsOverlay { enabled: false }
            )
        ));
    }
}
