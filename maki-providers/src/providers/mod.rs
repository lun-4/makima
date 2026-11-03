use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use isahc::http::request::Builder;
use tracing::debug;

use crate::AgentError;

pub(crate) mod anthropic;
pub(crate) mod google;
pub(crate) mod openai;

const AUTHORIZATION_HEADER: &str = "authorization";

fn bearer_value(api_key: &str) -> String {
    format!("Bearer {api_key}")
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub connect: Duration,
    pub low_speed: Duration,
    pub stream: Duration,
    pub retry: crate::retry::RetryPolicy,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            low_speed: Duration::from_secs(30),
            stream: Duration::from_secs(300),
            retry: crate::retry::RetryPolicy::default(),
        }
    }
}

impl From<&maki_config::ProviderConfig> for Timeouts {
    fn from(config: &maki_config::ProviderConfig) -> Self {
        Self {
            connect: config.connect_timeout,
            low_speed: config.low_speed_timeout,
            stream: config.stream_timeout,
            retry: crate::retry::RetryPolicy::from(config),
        }
    }
}

#[derive(Clone)]
pub struct ResolvedAuth {
    pub base_url: Option<String>,
    pub headers: Vec<(String, String)>,
    /// Header names that came from `[<slug>.headers]`. They win over anything
    /// the provider sets afterwards, so a key rotation cannot drop a gateway
    /// credential that replaced the built-in auth header.
    config_headers: Vec<String>,
}

impl ResolvedAuth {
    /// The only way to build auth, so every provider picks up
    /// `[<slug>.headers]` from `providers.toml`. Skipping it would silently
    /// ignore the user's config, which is why there is no slug-less
    /// constructor outside of tests.
    pub fn new(slug: &str, headers: Vec<(String, String)>) -> Result<Self, AgentError> {
        let mut auth = Self {
            base_url: None,
            headers,
            config_headers: Vec::new(),
        };
        if let Some(def) = maki_config::providers::ProvidersConfig::load().get(slug) {
            auth.apply_config_headers(slug, &def.headers)?;
        }
        Ok(auth)
    }

    /// Fold `[<slug>.headers]` in, expanding `${VAR}` from the environment. An
    /// unset or empty variable fails the whole provider (see
    /// `maki_config::expand_env`), matching the MCP path.
    fn apply_config_headers(
        &mut self,
        slug: &str,
        headers: &BTreeMap<String, String>,
    ) -> Result<(), AgentError> {
        for (name, value) in headers {
            let expanded = maki_config::expand_env(value).map_err(|var| {
                AgentError::Config {
                    message: format!(
                        "provider '{slug}' header '{name}': environment variable '{var}' is unset or empty"
                    ),
                }
            })?;
            self.set_header(name, expanded);
            self.config_headers.push(name.clone());
        }
        Ok(())
    }

    pub fn bearer(slug: &str, api_key: &str) -> Result<Self, AgentError> {
        Self::new(
            slug,
            vec![(AUTHORIZATION_HEADER.into(), bearer_value(api_key))],
        )
    }

    pub fn with_base_url(mut self, base_url: Option<String>) -> Self {
        self.base_url = base_url;
        self
    }

    /// Set the header carrying the API key, unless `[<slug>.headers]` already
    /// owns that name: the config value is the one the gateway expects.
    fn set_key_header(&mut self, name: &str, value: String) {
        if self
            .config_headers
            .iter()
            .any(|configured| configured.eq_ignore_ascii_case(name))
        {
            return;
        }
        self.set_header(name, value);
    }

    /// Replace a same-name header (case-insensitive) instead of appending a
    /// second one: `Builder::header` appends, so a configured `Authorization`
    /// next to the built-in bearer would send two credentials.
    pub(crate) fn set_header(&mut self, name: &str, value: String) {
        match self
            .headers
            .iter_mut()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
        {
            Some(slot) => slot.1 = value,
            None => self.headers.push((name.to_string(), value)),
        }
    }

    /// Apply all auth headers to an HTTP request builder.
    pub fn configure_request(&self, builder: Builder) -> Builder {
        self.headers.iter().fold(builder, |b, (key, value)| {
            b.header(key.as_str(), value.as_str())
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(base_url: Option<String>, headers: Vec<(String, String)>) -> Self {
        Self {
            base_url,
            headers,
            config_headers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KeyPool {
    keys: Arc<Vec<String>>,
    index: Arc<AtomicUsize>,
}

impl KeyPool {
    pub fn from_env(env_var: &str) -> Result<Self, AgentError> {
        let raw = std::env::var(env_var).map_err(|_| AgentError::Config {
            message: format!("{env_var} not set"),
        })?;
        let keys: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if keys.is_empty() {
            return Err(AgentError::Config {
                message: format!("{env_var} is empty"),
            });
        }
        Ok(Self {
            keys: Arc::new(keys),
            index: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn resolve(slug: &str, env_var: &str) -> Result<Self, AgentError> {
        if let Ok(pool) = Self::from_env(env_var) {
            debug!(slug, keys = pool.len(), "resolved API key from env");
            return Ok(pool);
        }
        if let Some(key) = Self::key_from_file(slug) {
            debug!(slug, "resolved API key from saved credentials");
            return Ok(Self::from_keys(vec![key]));
        }
        if let Some(key) = Self::key_from_config(slug) {
            debug!(slug, "resolved API key from providers.toml");
            return Ok(Self::from_keys(vec![key]));
        }
        Err(AgentError::Config {
            message: format!(
                "{env_var} not set and no saved credentials for '{slug}' — run `maki auth login {slug}`"
            ),
        })
    }

    fn key_from_file(slug: &str) -> Option<String> {
        let dir = maki_storage::StateDir::resolve().ok()?;
        maki_storage::auth::load_provider_credentials(&dir, slug).map(|c| c.api_key)
    }

    fn key_from_config(slug: &str) -> Option<String> {
        maki_config::providers::ProvidersConfig::load()
            .get(slug)
            .and_then(|d| d.api_key.clone())
    }

    pub fn from_keys(keys: Vec<String>) -> Self {
        Self {
            keys: Arc::new(keys),
            index: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn current(&self) -> &str {
        &self.keys[self.index.load(Ordering::Relaxed) % self.keys.len()]
    }

    pub fn rotate(&self) -> bool {
        if self.keys.len() <= 1 {
            return false;
        }
        self.index.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Rotate to the next key and refresh only the header carrying it, so the
    /// resolved `base_url` and any `[<slug>.headers]` survive the rotation.
    pub fn rotate_key_header(
        &self,
        auth: &Mutex<ResolvedAuth>,
        name: &str,
        build: impl FnOnce(&str) -> String,
    ) -> bool {
        if !self.rotate() {
            return false;
        }
        auth.lock()
            .unwrap()
            .set_key_header(name, build(self.current()));
        true
    }

    pub fn rotate_bearer(&self, auth: &Mutex<ResolvedAuth>) -> bool {
        self.rotate_key_header(auth, AUTHORIZATION_HEADER, bearer_value)
    }

    pub fn rotate_auth(
        &self,
        auth: &Mutex<ResolvedAuth>,
        build: impl FnOnce(&str) -> ResolvedAuth,
    ) -> bool {
        if !self.rotate() {
            return false;
        }
        *auth.lock().unwrap() = build(self.current());
        true
    }

    pub fn rotate_headers(
        &self,
        auth: &Mutex<ResolvedAuth>,
        build: impl FnOnce(&str) -> Vec<(String, String)>,
    ) -> bool {
        if !self.rotate() {
            return false;
        }
        auth.lock().unwrap().headers = build(self.current());
        true
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Where a provider's key lands in its auth headers. Two shapes cover every
/// provider we have, and an enum keeps "how a key becomes a header" in one
/// place instead of one closure per provider.
#[derive(Clone, Copy)]
pub enum KeyHeader {
    /// `Authorization: Bearer <key>`.
    Bearer,
    /// The key verbatim, in a provider specific header (`x-api-key`,
    /// `x-goog-api-key`).
    Raw(&'static str),
}

impl KeyHeader {
    fn name(self) -> &'static str {
        match self {
            Self::Bearer => AUTHORIZATION_HEADER,
            Self::Raw(name) => name,
        }
    }

    fn value(self, key: &str) -> String {
        match self {
            Self::Bearer => bearer_value(key),
            Self::Raw(_) => key.to_string(),
        }
    }
}

/// A provider's keys and the auth they are written into.
pub struct KeyRotation<'a> {
    pool: &'a KeyPool,
    auth: &'a Mutex<ResolvedAuth>,
    header: KeyHeader,
}

impl<'a> KeyRotation<'a> {
    pub fn new(pool: &'a KeyPool, auth: &'a Mutex<ResolvedAuth>, header: KeyHeader) -> Self {
        Self { pool, auth, header }
    }

    /// How many keys a walk can try before it is back where it started.
    pub fn key_count(&self) -> usize {
        self.pool.len()
    }

    /// Advance to the next key and refresh only the header carrying it, so the
    /// resolved `base_url` and any `[<slug>.headers]` survive the rotation.
    pub fn rotate(&self) -> bool {
        if !self.pool.rotate() {
            return false;
        }
        self.auth
            .lock()
            .unwrap()
            .set_key_header(self.header.name(), self.header.value(self.pool.current()));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_pool_single_key_current() {
        let pool = KeyPool::from_keys(vec!["sk-1".into()]);
        assert_eq!(pool.current(), "sk-1");
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn key_pool_single_key_rotate_returns_false() {
        let pool = KeyPool::from_keys(vec!["sk-1".into()]);
        assert!(!pool.rotate());
        assert_eq!(pool.current(), "sk-1");
    }

    #[test]
    fn key_pool_multi_key_rotates() {
        let pool = KeyPool::from_keys(vec!["sk-1".into(), "sk-2".into(), "sk-3".into()]);
        assert_eq!(pool.current(), "sk-1");
        assert!(pool.rotate());
        assert_eq!(pool.current(), "sk-2");
        assert!(pool.rotate());
        assert_eq!(pool.current(), "sk-3");
    }

    #[test]
    fn key_pool_wraps_around() {
        let pool = KeyPool::from_keys(vec!["a".into(), "b".into()]);
        pool.rotate();
        pool.rotate();
        assert_eq!(pool.current(), "a");
    }

    #[test]
    fn resolve_from_env() {
        let env_var = format!("MAKI_TEST_KEY_{}", fastrand::u32(..));
        unsafe { std::env::set_var(&env_var, "from-env") };
        let pool = KeyPool::resolve("test_slug", &env_var).unwrap();
        unsafe { std::env::remove_var(&env_var) };
        assert_eq!(pool.current(), "from-env");
    }

    #[test]
    fn resolve_env_supports_comma_separated() {
        let env_var = format!("MAKI_TEST_MULTI_{}", fastrand::u32(..));
        unsafe { std::env::set_var(&env_var, "sk-1, sk-2, sk-3") };
        let pool = KeyPool::resolve("test_slug", &env_var).unwrap();
        unsafe { std::env::remove_var(&env_var) };
        assert_eq!(pool.current(), "sk-1");
        assert!(pool.rotate());
        assert_eq!(pool.current(), "sk-2");
    }

    #[test]
    fn resolve_returns_error_when_nothing_found() {
        let slug = format!("test_resolve_none_{}", fastrand::u32(..));
        let env_var = format!("MAKI_TEST_KEY_NONE_{}", fastrand::u32(..));
        let result = KeyPool::resolve(&slug, &env_var);
        assert!(result.is_err());
        let msg = format!("{result:?}");
        assert!(msg.contains(&env_var) || msg.contains(&slug));
    }

    const TEST_SLUG: &str = "gateway";
    const GATEWAY_HEADER: &str = "CF-Access-Client-Id";
    const GATEWAY_ID: &str = "client-id";
    const GATEWAY_URL: &str = "https://gw.internal/v1";
    const GATEWAY_CRED: &str = "Basic gateway-cred";

    fn config_headers(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn header_value(auth: &ResolvedAuth, name: &str) -> Option<String> {
        auth.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    }

    fn test_bearer(key: &str) -> ResolvedAuth {
        ResolvedAuth::for_test(None, vec![(AUTHORIZATION_HEADER.into(), bearer_value(key))])
    }

    #[test]
    fn config_headers_append_unknown_and_replace_same_name() {
        let mut auth = test_bearer("sk-1");
        auth.apply_config_headers(
            TEST_SLUG,
            &config_headers(&[
                (GATEWAY_HEADER, GATEWAY_ID),
                // Case differs from the built-in header on purpose: appending
                // instead of replacing would send two credentials.
                ("Authorization", "Basic other"),
            ]),
        )
        .unwrap();
        assert_eq!(auth.headers.len(), 2);
        assert_eq!(
            header_value(&auth, AUTHORIZATION_HEADER).as_deref(),
            Some("Basic other")
        );
        assert_eq!(
            header_value(&auth, GATEWAY_HEADER).as_deref(),
            Some(GATEWAY_ID)
        );
    }

    #[test]
    fn config_headers_unset_var_names_slug_header_and_var() {
        let var = format!("MAKI_TEST_GATEWAY_UNSET_{}", fastrand::u32(..));
        let mut auth = test_bearer("sk-1");
        let err = auth
            .apply_config_headers(
                TEST_SLUG,
                &config_headers(&[(GATEWAY_HEADER, &format!("${{{var}}}"))]),
            )
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains(TEST_SLUG), "got: {msg}");
        assert!(msg.contains(GATEWAY_HEADER), "got: {msg}");
        assert!(msg.contains(&var), "got: {msg}");
    }

    #[test]
    fn rotate_bearer_keeps_base_url_and_config_headers() {
        let pool = KeyPool::from_keys(vec!["sk-1".into(), "sk-2".into()]);
        let mut auth = test_bearer(pool.current());
        auth.base_url = Some(GATEWAY_URL.into());
        auth.apply_config_headers(TEST_SLUG, &config_headers(&[(GATEWAY_HEADER, GATEWAY_ID)]))
            .unwrap();

        let auth = Mutex::new(auth);
        assert!(pool.rotate_bearer(&auth));

        let auth = auth.lock().unwrap();
        assert_eq!(auth.base_url.as_deref(), Some(GATEWAY_URL));
        assert_eq!(
            header_value(&auth, AUTHORIZATION_HEADER).as_deref(),
            Some("Bearer sk-2")
        );
        assert_eq!(
            header_value(&auth, GATEWAY_HEADER).as_deref(),
            Some(GATEWAY_ID)
        );
    }

    #[test]
    fn rotate_bearer_keeps_a_configured_auth_header() {
        let pool = KeyPool::from_keys(vec!["sk-1".into(), "sk-2".into()]);
        let mut auth = test_bearer(pool.current());
        auth.apply_config_headers(
            TEST_SLUG,
            &config_headers(&[("Authorization", GATEWAY_CRED)]),
        )
        .unwrap();

        let auth = Mutex::new(auth);
        assert!(pool.rotate_bearer(&auth));

        // The gateway credential replaced the built-in bearer, so rotating the
        // key must not put `Bearer sk-2` back and lock the user out.
        let auth = auth.lock().unwrap();
        assert_eq!(auth.headers.len(), 1);
        assert_eq!(
            header_value(&auth, AUTHORIZATION_HEADER).as_deref(),
            Some(GATEWAY_CRED)
        );
    }
}
