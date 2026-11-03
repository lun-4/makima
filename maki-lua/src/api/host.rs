//! Actions on the machine running maki, outside the terminal.

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult};
use smol::unblock;

use crate::api::net::parse_http_url;
use crate::api::util::pair::{Pair, try_pair};
use crate::plugin_permissions::PluginPermissions;

/// Open {url} in the user's default browser. Only `http` and `https` URLs
/// are accepted.
///
/// @param url string URL to open.
/// @return (boolean?, string?) true, or nil plus an error string.
/// @example
/// local ok, err = maki.host.open("https://example.com/login")
#[lua_fn(guard = Run)]
async fn open(_lua: Lua, url: String) -> LuaResult<Pair<bool>> {
    try_pair!(open_url(&url).await);
    Ok((Some(true), None))
}

pub(crate) async fn open_url(url: &str) -> Result<(), String> {
    let url = parse_http_url(url)?;
    unblock(move || ::open::that_detached(url.as_str()))
        .await
        .map_err(|e| format!("cannot open browser: {e}"))
}

lua_table! {
    /// Actions on the host machine, such as opening a browser for a login.
    ///
    /// ```lua
    /// maki.host.open("https://example.com")
    /// ```
    "maki.host" => pub(crate) fn create_host_table(perms: &PluginPermissions), DOCS [
        open(perms),
    ]
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;
    use crate::api::net::NON_HTTP_URL_ERR;

    #[test_case("file:///etc/passwd" ; "file_scheme")]
    #[test_case("javascript:alert(1)" ; "javascript_scheme")]
    fn non_http_url_is_rejected(url: &str) {
        let lua = Lua::new();
        let host = create_host_table(&lua, &PluginPermissions::trusted()).unwrap();
        let open: mlua::Function = host.get("open").unwrap();
        let (ok, err): (Option<bool>, Option<String>) =
            smol::block_on(open.call_async(url)).unwrap();
        assert_eq!(ok, None);
        assert!(err.is_some_and(|e| e.contains(NON_HTTP_URL_ERR)));
    }
}
