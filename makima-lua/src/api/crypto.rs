//! Hash primitives on binary strings, enough for request signing (SigV4) in Lua.

use hmac::{Hmac, Mac};
use makima_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, String as LuaString, Value as LuaValue};
use sha2::{Digest, Sha256};

use crate::api::util::convert::bytes_arg;

/// SHA-256 digest of {data}, as 32 raw bytes.
///
/// @param data string|buffer Bytes to hash.
/// @return (string) Raw 32-byte digest.
/// @example
/// local digest = makima.crypto.sha256("abc")
#[lua_fn]
fn sha256(lua: &Lua, data: LuaValue) -> LuaResult<LuaString> {
    let bytes = bytes_arg(&data, "crypto.sha256")?;
    lua.create_string(Sha256::digest(bytes))
}

/// HMAC-SHA256 of {data} keyed with {key}, as 32 raw bytes.
///
/// @param key string|buffer Secret key, any length.
/// @param data string|buffer Message to authenticate.
/// @return (string) Raw 32-byte MAC.
/// @example
/// local mac = makima.crypto.hmac_sha256("secret", "message")
#[lua_fn]
fn hmac_sha256(lua: &Lua, key: LuaValue, data: LuaValue) -> LuaResult<LuaString> {
    let key = bytes_arg(&key, "crypto.hmac_sha256 key")?;
    let data = bytes_arg(&data, "crypto.hmac_sha256 data")?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key)
        .map_err(|e| mlua::Error::runtime(format!("crypto.hmac_sha256: {e}")))?;
    mac.update(&data);
    lua.create_string(mac.finalize().into_bytes())
}

lua_table! {
    /// Hashing on binary strings. Digests come back as raw bytes, not hex.
    /// Both functions accept strings and Luau buffers.
    ///
    /// ```lua
    /// local signature = makima.crypto.hmac_sha256(key, makima.crypto.sha256(body))
    /// ```
    "makima.crypto" => pub(crate) fn create_crypto_table(), DOCS [
        sha256, hmac_sha256,
    ]
}

#[cfg(test)]
mod tests {
    use mlua::Function;
    use test_case::test_case;

    use super::*;

    const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    const SHA256_EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const RFC4231_CASE_1: &str = "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
    const RFC4231_CASE_2: &str = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843";
    const BINARY_HMAC: &str = "b47f47d3cc3460563383a5081d951ed96838e47e11b0edac7245339d0d76a82f";

    fn function(lua: &Lua, name: &str) -> Function {
        create_crypto_table(lua).unwrap().get(name).unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test_case(b"abc", SHA256_ABC ; "abc")]
    #[test_case(b"", SHA256_EMPTY ; "empty")]
    fn sha256_digest(data: &[u8], expected: &str) {
        let lua = Lua::new();
        let digest: LuaString = function(&lua, "sha256")
            .call(lua.create_string(data).unwrap())
            .unwrap();
        assert_eq!(hex(&digest.as_bytes()), expected);
    }

    #[test_case(&[0x0b; 20], b"Hi There", RFC4231_CASE_1 ; "rfc4231_case_1")]
    #[test_case(b"Jefe", b"what do ya want for nothing?", RFC4231_CASE_2 ; "rfc4231_case_2")]
    #[test_case(b"\0key\xff", b"da\0ta\x80\xfe", BINARY_HMAC ; "binary_key_and_data")]
    fn hmac_sha256_mac(key: &[u8], data: &[u8], expected: &str) {
        let lua = Lua::new();
        let mac: LuaString = function(&lua, "hmac_sha256")
            .call((
                lua.create_string(key).unwrap(),
                lua.create_buffer(data).unwrap(),
            ))
            .unwrap();
        assert_eq!(hex(&mac.as_bytes()), expected);
    }

    #[test]
    fn non_bytes_argument_throws() {
        let lua = Lua::new();
        assert!(function(&lua, "sha256").call::<LuaString>(42).is_err());
    }
}
