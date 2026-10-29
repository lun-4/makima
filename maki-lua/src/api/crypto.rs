use hmac::{Hmac, Mac};
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, String as LuaString};
use sha2::{Digest, Sha256};

/// Hash {data} with SHA-256.
///
/// @param data string Input bytes.
/// @return (string) Raw 32-byte digest.
#[lua_fn]
fn sha256(lua: &Lua, data: LuaString) -> LuaResult<LuaString> {
    lua.create_string(Sha256::digest(data.as_bytes().as_ref()))
}

/// Compute HMAC-SHA-256 of {data} with {key}.
///
/// @param key string Key bytes.
/// @param data string Input bytes.
/// @return (string) Raw 32-byte digest.
#[lua_fn]
fn hmac_sha256(lua: &Lua, key: LuaString, data: LuaString) -> LuaResult<LuaString> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes().as_ref()).map_err(mlua::Error::external)?;
    mac.update(data.as_bytes().as_ref());
    lua.create_string(mac.finalize().into_bytes())
}

lua_table! {
    /// Binary-safe SHA-256 and HMAC-SHA-256 primitives.
    "maki.crypto" => pub(crate) fn create_crypto_table(), DOCS [
        sha256, hmac_sha256,
    ]
}

#[cfg(test)]
mod tests {
    use mlua::{Function, Lua, String as LuaString};
    use test_case::test_case;

    use super::create_crypto_table;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test_case("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ; "empty")]
    #[test_case("abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" ; "abc")]
    fn crypto_sha256_known_answers(input: &str, expected: &str) {
        let lua = Lua::new();
        let table = create_crypto_table(&lua).unwrap();
        let hash: Function = table.get("sha256").unwrap();
        let digest: LuaString = hash.call(input).unwrap();
        assert_eq!(hex(&digest.as_bytes()), expected);
    }

    #[test_case("", "", "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad" ; "empty")]
    #[test_case("key", "The quick brown fox jumps over the lazy dog", "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8" ; "standard")]
    fn crypto_hmac_sha256_known_answers(key: &str, input: &str, expected: &str) {
        let lua = Lua::new();
        let table = create_crypto_table(&lua).unwrap();
        let hmac: Function = table.get("hmac_sha256").unwrap();
        let digest: LuaString = hmac.call((key, input)).unwrap();
        assert_eq!(hex(&digest.as_bytes()), expected);
    }

    #[test]
    fn crypto_binary_strings_roundtrip() {
        let lua = Lua::new();
        let maki = lua.create_table().unwrap();
        maki.set("crypto", create_crypto_table(&lua).unwrap())
            .unwrap();
        lua.globals().set("maki", maki).unwrap();
        let (hash, first, chained): (LuaString, LuaString, LuaString) = lua
            .load(
                r#"
            local data = string.char(0, 255, 128, 65)
            local first = maki.crypto.hmac_sha256(data, data)
            return maki.crypto.sha256(data), first, maki.crypto.hmac_sha256(first, data)
        "#,
            )
            .eval()
            .unwrap();
        assert_eq!(
            hex(&hash.as_bytes()),
            "3507b01e644277ad3cd10dadd6e33cb801151e62e3cb899a67409ef701d6079c"
        );
        assert_eq!(
            hex(&first.as_bytes()),
            "f07edca8f70fa647c709fa4c874d48f87bb70baebb4061eca4fdea264a10094b"
        );
        assert_eq!(
            hex(&chained.as_bytes()),
            "da2dd5f11772df4217baae63005316c8f9ec408e14a3c280341b7d3c9c769db3"
        );
    }
}
