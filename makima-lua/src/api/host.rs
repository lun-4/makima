//! Actions on the machine running makima, outside the terminal.

use keyring::{Entry, Error as KeyringError};
use makima_lua_macro::{lua_fn, lua_table};
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
/// local ok, err = makima.host.open("https://example.com/login")
#[lua_fn(guard = Run)]
async fn open(_lua: Lua, url: String) -> LuaResult<Pair<bool>> {
    try_pair!(open_url(&url).await);
    Ok((Some(true), None))
}

/// Read a secret another program stored in the system keyring, such as the
/// token the GitHub CLI keeps there.
///
/// @param service string Keyring service name, such as `gh:github.com`.
/// @param account string Account name within the service; may be empty.
/// @return (string?, string?) The secret, nil when there is no such entry, or nil plus an error when the keyring cannot be read.
/// @example
/// local token, err = makima.host.keyring_get("gh:github.com", "octocat")
#[lua_fn(guard = Env)]
async fn keyring_get(_lua: Lua, service: String, account: String) -> LuaResult<Pair<String>> {
    let Ok(entry) = Entry::new(&service, &account) else {
        return Ok((None, None));
    };
    Ok(match unblock(move || entry.get_password()).await {
        Ok(secret) => (Some(secret), None),
        Err(KeyringError::NoEntry) => (None, None),
        Err(error) => (None, Some(error.to_string())),
    })
}

pub(crate) async fn open_url(url: &str) -> Result<(), String> {
    let url = parse_http_url(url)?;
    unblock(move || ::open::that_detached(url.as_str()))
        .await
        .map_err(|e| format!("cannot open browser: {e}"))
}

lua_table! {
    /// Actions on the host machine: opening a browser, reading the keyring.
    ///
    /// ```lua
    /// makima.host.open("https://example.com")
    /// ```
    "makima.host" => pub(crate) fn create_host_table(perms: &PluginPermissions), DOCS [
        open(perms),
        keyring_get(perms),
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
