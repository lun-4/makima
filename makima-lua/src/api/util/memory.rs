use mlua::Lua;

/// Runs {convert} with the Lua memory limit lifted. While a limit is set,
/// mlua guards every table and string it creates with a protected call,
/// which makes building a large value about two and a half times slower.
/// Only for the host's own conversions of data already in memory: they
/// cannot run away, and no Lua code runs inside them.
pub(crate) fn unlimited<T>(lua: &Lua, convert: impl FnOnce() -> T) -> T {
    let _restore = Lifted::new(lua);
    convert()
}

/// Puts the previous limit back when dropped, a panic included.
struct Lifted<'a> {
    lua: &'a Lua,
    limit: Option<usize>,
}

impl<'a> Lifted<'a> {
    fn new(lua: &'a Lua) -> Self {
        Self {
            lua,
            limit: lua.set_memory_limit(0).ok(),
        }
    }
}

impl Drop for Lifted<'_> {
    fn drop(&mut self) {
        if let Some(limit) = self.limit {
            let _ = self.lua.set_memory_limit(limit);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADROOM: usize = 1024 * 1024;
    const LARGE: usize = 4 * HEADROOM;

    fn tight(lua: &Lua) {
        lua.set_memory_limit(lua.used_memory() + HEADROOM).unwrap();
    }

    /// Luau interns strings, so each call needs bytes no live string has.
    fn allocate(lua: &Lua, fill: u8) -> bool {
        lua.create_string(vec![fill; LARGE]).is_ok()
    }

    #[test]
    fn limit_is_lifted_inside_and_restored_after() {
        let lua = Lua::new();
        tight(&lua);
        assert!(!allocate(&lua, 1));
        assert!(unlimited(&lua, || allocate(&lua, 2)));
        assert!(!allocate(&lua, 3));
    }

    #[test]
    fn limit_is_restored_after_a_panic() {
        let lua = Lua::new();
        tight(&lua);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unlimited(&lua, || panic!("conversion failed"))
        }));
        assert!(panicked.is_err());
        assert!(!allocate(&lua, 1));
    }
}
