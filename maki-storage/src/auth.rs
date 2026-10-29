use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fs4::{FileExt, TryLockError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{StateDir, StorageError, atomic_write_permissions};

const AUTH_DIR: &str = "auth";
const AUTH_FILE_MODE: u32 = 0o600;
const REFRESH_BUFFER_SECS: u64 = 60;
const LOCK_SUFFIX: &str = ".lock";
const LOCK_WAIT: Duration = Duration::from_secs(30);
const LOCK_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access: String,
    pub refresh: String,
    pub expires: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
}

impl OAuthTokens {
    pub fn is_expired(&self) -> bool {
        now_millis() + REFRESH_BUFFER_SECS * 1000 >= self.expires
    }

    pub fn is_hard_expired(&self) -> bool {
        now_millis() >= self.expires
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct McpAuthData {
    pub server_url: String,
    pub tokens: Option<OAuthTokens>,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub client_secret_expires_at: Option<u64>,
    #[serde(default)]
    pub redirect_uri: Option<String>,
    /// Token endpoint pinned at interactive auth. Silent refresh reuses it
    /// instead of trusting fresh discovery, so a later-compromised server
    /// cannot redirect the refresh token and client secret elsewhere.
    #[serde(default)]
    pub token_endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCredentials {
    pub api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

impl ProviderCredentials {
    pub fn masked_api_key(&self) -> String {
        if self.api_key.len() > 8 {
            format!(
                "{}...{}",
                &self.api_key[..4],
                &self.api_key[self.api_key.len() - 4..]
            )
        } else {
            "****".to_string()
        }
    }
}

#[cfg(test)]
thread_local! {
    static LOCK_BLOCKED: std::cell::RefCell<Option<std::sync::mpsc::Sender<bool>>> = const { std::cell::RefCell::new(None) };
}

/// Acquire the cross-process lock with a bounded wait.
pub fn lock_exclusive(path: &Path) -> Option<File> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => {
            fs::create_dir_all(path.parent()?).ok()?;
            OpenOptions::new()
                .write(true)
                .truncate(false)
                .create(true)
                .open(path)
                .ok()?
        }
    };
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match FileExt::try_lock(&file) {
            Ok(()) => return Some(file),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                #[cfg(test)]
                LOCK_BLOCKED.with(|sender| {
                    if let Some(sender) = sender.borrow_mut().take() {
                        let _ = sender.send(true);
                    }
                });
                sleep(LOCK_POLL);
            }
            Err(_) => return None,
        }
    }
}

/// Every write replaces the token file, so the lock sits beside it under a name
/// no loader reads.
pub fn lock_tokens(dir: &StateDir, provider: &str) -> Option<File> {
    lock_exclusive(&token_lock_path(dir, provider))
}

fn token_lock_path(dir: &StateDir, provider: &str) -> PathBuf {
    auth_path(dir, &format!("{provider}{LOCK_SUFFIX}"))
}

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("invalid credential scope")]
    InvalidScope,
    #[error("credential lock timed out")]
    LockTimeout,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("credential refresh failed: {0}")]
    Refresh(String),
    #[error("credentials revoked")]
    Revoked,
    #[error("credential file contains a different credential type")]
    Conflict,
    #[error("invalid credential value")]
    InvalidValue,
    #[error("credential lifecycle generation is stale")]
    StaleGeneration,
    #[error("credential lifecycle generation exhausted")]
    GenerationExhausted,
}

/// A storage namespace, not an authorization capability. Callers must bind it to
/// a host-verified provider identity before exposing it to a plugin.
#[derive(Clone)]
pub struct CredentialScope {
    dir: StateDir,
    name: String,
    legacy_kind: Option<LegacyCredentialKind>,
}

#[derive(Clone, Copy)]
pub enum LegacyCredentialKind {
    OAuth,
    ApiKey,
}

/// Provider-scoped fencing for operations that may finish after logout.
/// This is a storage namespace, not an authorization capability.
#[derive(Clone)]
pub struct CredentialLifecycle {
    dir: StateDir,
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialGeneration {
    dir: PathBuf,
    name: String,
    value: u64,
}

pub enum RefreshOutcome {
    Unchanged,
    Updated(serde_json::Value),
    Deleted,
    Revoked,
}

impl CredentialLifecycle {
    pub fn new(dir: StateDir, provider: &str) -> Result<Self, CredentialError> {
        CredentialScope::new(dir.clone(), provider)?;
        Ok(Self {
            dir,
            name: format!("lifecycle_{}_{provider}", provider.len()),
        })
    }

    fn path(&self) -> PathBuf {
        auth_path(&self.dir, &format!("{}.generation", self.name))
    }

    fn read_generation(&self) -> Result<u64, CredentialError> {
        match fs::read(self.path()) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(StorageError::from)
                .map_err(Into::into),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(StorageError::from(error).into()),
        }
    }

    pub async fn snapshot(&self) -> Result<CredentialGeneration, CredentialError> {
        let _lock = lock_credentials(&self.dir, &self.name).await?;
        Ok(CredentialGeneration {
            dir: self.dir.path().to_owned(),
            name: self.name.clone(),
            value: self.read_generation()?,
        })
    }

    /// Fence earlier operations before deleting credentials. A failed delete must
    /// still be reported; the generation advance alone is not a completed logout.
    pub async fn invalidate(&self) -> Result<CredentialGeneration, CredentialError> {
        let _lock = lock_credentials(&self.dir, &self.name).await?;
        let value = self
            .read_generation()?
            .checked_add(1)
            .ok_or(CredentialError::GenerationExhausted)?;
        save_auth(&self.path(), &value)?;
        Ok(CredentialGeneration {
            dir: self.dir.path().to_owned(),
            name: self.name.clone(),
            value,
        })
    }

    /// Runs the callback without locks, then checks the generation and original
    /// credential under lifecycle-then-credential locks before committing.
    pub async fn refresh<F, Fut>(
        &self,
        generation: &CredentialGeneration,
        scope: &CredentialScope,
        refresh: F,
    ) -> Result<Option<serde_json::Value>, CredentialError>
    where
        F: FnOnce(Option<serde_json::Value>) -> Fut,
        Fut: Future<Output = Result<RefreshOutcome, CredentialError>>,
    {
        if generation.dir != self.dir.path()
            || generation.name != self.name
            || scope.dir.path() != self.dir.path()
            || scope.name == self.name
        {
            return Err(CredentialError::InvalidScope);
        }
        let single_flight = format!("refresh_{}_{}", scope.name.len(), scope.name);
        let _single_flight = lock_credentials(&self.dir, &single_flight).await?;
        let current = {
            let _lifecycle = lock_credentials(&self.dir, &self.name).await?;
            if self.read_generation()? != generation.value {
                return Err(CredentialError::StaleGeneration);
            }
            let _credential = scope.lock().await?;
            scope.read()?
        };
        let outcome = refresh(current.clone()).await?;
        let _lifecycle = lock_credentials(&self.dir, &self.name).await?;
        if self.read_generation()? != generation.value {
            return Err(CredentialError::StaleGeneration);
        }
        let _credential = scope.lock().await?;
        if scope.read()? != current {
            return Err(CredentialError::Conflict);
        }
        match outcome {
            RefreshOutcome::Unchanged => Ok(current),
            RefreshOutcome::Updated(value) => {
                scope.write_unlocked(&value)?;
                Ok(Some(value))
            }
            RefreshOutcome::Deleted => {
                scope.delete_unlocked()?;
                Ok(None)
            }
            RefreshOutcome::Revoked => Err(CredentialError::Revoked),
        }
    }
}

impl CredentialScope {
    pub fn new(dir: StateDir, name: &str) -> Result<Self, CredentialError> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        {
            return Err(CredentialError::InvalidScope);
        }
        Ok(Self {
            dir,
            name: name.to_owned(),
            legacy_kind: None,
        })
    }

    pub fn legacy(
        dir: StateDir,
        name: &str,
        kind: LegacyCredentialKind,
    ) -> Result<Self, CredentialError> {
        let mut scope = Self::new(dir, name)?;
        scope.legacy_kind = Some(kind);
        Ok(scope)
    }

    fn matches_kind(&self, value: &serde_json::Value) -> bool {
        match self.legacy_kind {
            Some(kind) => {
                let oauth = serde_json::from_value::<OAuthTokens>(value.clone()).is_ok();
                let api_key = serde_json::from_value::<ProviderCredentials>(value.clone()).is_ok();
                match kind {
                    LegacyCredentialKind::OAuth => oauth && !api_key,
                    LegacyCredentialKind::ApiKey => api_key && !oauth,
                }
            }
            None => true,
        }
    }

    fn read_raw(&self) -> Result<Option<serde_json::Value>, CredentialError> {
        match fs::read(self.path()) {
            Ok(data) => Ok(Some(
                serde_json::from_slice(&data).map_err(StorageError::from)?,
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StorageError::from(error).into()),
        }
    }

    fn path(&self) -> PathBuf {
        auth_path(&self.dir, &self.name)
    }

    pub fn read(&self) -> Result<Option<serde_json::Value>, CredentialError> {
        Ok(self.read_raw()?.filter(|value| self.matches_kind(value)))
    }

    pub async fn write(&self, value: &serde_json::Value) -> Result<(), CredentialError> {
        let _lock = self.lock().await?;
        self.write_unlocked(value)
    }

    fn write_unlocked(&self, value: &serde_json::Value) -> Result<(), CredentialError> {
        if !self.matches_kind(value) {
            return Err(CredentialError::InvalidValue);
        }
        if self
            .read_raw()?
            .is_some_and(|current| !self.matches_kind(&current))
        {
            return Err(CredentialError::Conflict);
        }
        save_auth(&self.path(), value)?;
        Ok(())
    }

    pub async fn delete(&self) -> Result<bool, CredentialError> {
        let _lock = self.lock().await?;
        self.delete_unlocked()
    }

    fn delete_unlocked(&self) -> Result<bool, CredentialError> {
        if self
            .read_raw()?
            .is_some_and(|current| !self.matches_kind(&current))
        {
            return Ok(false);
        }
        match fs::remove_file(self.path()) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(StorageError::from(error).into()),
        }
    }

    async fn lock(&self) -> Result<File, CredentialError> {
        lock_credentials(&self.dir, &self.name).await
    }

    /// The callback receives the state reloaded under the cross-process lock.
    /// Dropping the future releases the lock without committing its result.
    pub async fn refresh<F, Fut>(
        &self,
        refresh: F,
    ) -> Result<Option<serde_json::Value>, CredentialError>
    where
        F: FnOnce(Option<serde_json::Value>) -> Fut,
        Fut: Future<Output = Result<RefreshOutcome, CredentialError>>,
    {
        let _lock = self.lock().await?;
        let current = self.read()?;
        match refresh(current.clone()).await? {
            RefreshOutcome::Unchanged => Ok(current),
            RefreshOutcome::Updated(value) => {
                self.write_unlocked(&value)?;
                Ok(Some(value))
            }
            RefreshOutcome::Deleted => {
                self.delete_unlocked()?;
                Ok(None)
            }
            RefreshOutcome::Revoked => Err(CredentialError::Revoked),
        }
    }
}

pub async fn lock_credentials(dir: &StateDir, name: &str) -> Result<File, CredentialError> {
    let path = token_lock_path(dir, name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(StorageError::from)?;
    }
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(StorageError::from)?,
        Err(error) => return Err(StorageError::from(error).into()),
    };
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match FileExt::try_lock(&file) {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                async_io::Timer::after(LOCK_POLL).await;
            }
            Err(TryLockError::WouldBlock) => return Err(CredentialError::LockTimeout),
            Err(TryLockError::Error(error)) => return Err(StorageError::from(error).into()),
        }
    }
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn auth_path(dir: &StateDir, filename: &str) -> PathBuf {
    dir.path().join(AUTH_DIR).join(format!("{filename}.json"))
}

fn load_auth<T: DeserializeOwned>(path: &Path) -> Option<T> {
    fs::read_to_string(path)
        .ok()
        .and_then(|d| serde_json::from_str(&d).ok())
}

fn save_auth(path: &Path, data: &impl Serialize) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(data)?;
    atomic_write_permissions(path, json.as_bytes(), AUTH_FILE_MODE)?;
    debug!(path = %path.display(), "auth data saved");
    Ok(())
}

fn delete_auth(path: &Path) -> Result<bool, StorageError> {
    if path.exists() {
        fs::remove_file(path)?;
        return Ok(true);
    }
    Ok(false)
}

pub fn load_tokens(dir: &StateDir, provider: &str) -> Option<OAuthTokens> {
    load_auth(&auth_path(dir, provider))
}

pub fn save_tokens(
    dir: &StateDir,
    provider: &str,
    tokens: &OAuthTokens,
) -> Result<(), StorageError> {
    let _lock = require_token_lock(dir, provider)?;
    save_tokens_unlocked(dir, provider, tokens)
}

/// Caller must hold the lock returned by `lock_tokens`.
pub fn save_tokens_unlocked(
    dir: &StateDir,
    provider: &str,
    tokens: &OAuthTokens,
) -> Result<(), StorageError> {
    save_auth(&auth_path(dir, provider), tokens)
}

fn require_token_lock(dir: &StateDir, name: &str) -> Result<File, StorageError> {
    lock_tokens(dir, name)
        .ok_or_else(|| std::io::Error::other("credential lock unavailable").into())
}

pub fn delete_tokens(dir: &StateDir, provider: &str) -> Result<bool, StorageError> {
    let _lock = require_token_lock(dir, provider)?;
    delete_tokens_unlocked(dir, provider)
}

/// Caller must hold the lock returned by `lock_tokens`.
pub fn delete_tokens_unlocked(dir: &StateDir, provider: &str) -> Result<bool, StorageError> {
    delete_auth(&auth_path(dir, provider))
}

pub fn load_mcp_auth(dir: &StateDir, server_name: &str, expected_url: &str) -> Option<McpAuthData> {
    let data: McpAuthData = load_auth(&auth_path(dir, &format!("mcp-{server_name}")))?;
    if data.server_url != expected_url {
        return None;
    }
    if let Some(expires_at) = data.client_secret_expires_at
        && now_millis() / 1000 >= expires_at
    {
        return None;
    }
    Some(data)
}

pub fn save_mcp_auth(
    dir: &StateDir,
    server_name: &str,
    data: &McpAuthData,
) -> Result<(), StorageError> {
    let _lock = require_token_lock(dir, &format!("mcp-{server_name}"))?;
    save_mcp_auth_unlocked(dir, server_name, data)
}

/// Caller must hold the lock for the MCP credential name.
pub fn save_mcp_auth_unlocked(
    dir: &StateDir,
    server_name: &str,
    data: &McpAuthData,
) -> Result<(), StorageError> {
    save_auth(&auth_path(dir, &format!("mcp-{server_name}")), data)
}

pub fn delete_mcp_auth(dir: &StateDir, server_name: &str) -> Result<bool, StorageError> {
    let _lock = require_token_lock(dir, &format!("mcp-{server_name}"))?;
    delete_auth(&auth_path(dir, &format!("mcp-{server_name}")))
}

pub fn load_provider_credentials(dir: &StateDir, slug: &str) -> Option<ProviderCredentials> {
    load_auth(&auth_path(dir, slug))
}

pub fn save_provider_credentials(
    dir: &StateDir,
    slug: &str,
    creds: &ProviderCredentials,
) -> Result<(), StorageError> {
    let _lock = require_token_lock(dir, slug)?;
    save_auth(&auth_path(dir, slug), creds)
}

pub fn delete_provider_credentials(dir: &StateDir, slug: &str) -> Result<bool, StorageError> {
    let _lock = require_token_lock(dir, slug)?;
    delete_auth(&auth_path(dir, slug))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;
    use test_case::test_case;

    const TEST_URL: &str = "https://mcp.example.com";

    #[test_case("../escape"; "traversal")]
    #[test_case(""; "empty")]
    #[test_case("a/b"; "separator")]
    fn credential_scope_rejects_invalid_names(name: &str) {
        let tmp = TempDir::new().unwrap();
        assert!(matches!(
            CredentialScope::new(StateDir::from_path(tmp.path().into()), name),
            Err(CredentialError::InvalidScope)
        ));
    }

    #[test]
    fn legacy_credential_views_preserve_other_type() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().into());
        let oauth =
            CredentialScope::legacy(dir.clone(), "openai", LegacyCredentialKind::OAuth).unwrap();
        let key =
            CredentialScope::legacy(dir.clone(), "openai", LegacyCredentialKind::ApiKey).unwrap();
        let tokens = OAuthTokens {
            access: "access".into(),
            refresh: "refresh".into(),
            expires: 42,
            account_id: Some("account".into()),
        };
        save_tokens(&dir, "openai", &tokens).unwrap();
        assert_eq!(oauth.read().unwrap().unwrap()["access"], "access");
        assert!(key.read().unwrap().is_none());
        smol::block_on(async {
            assert!(!key.delete().await.unwrap());
            assert!(matches!(
                key.write(&serde_json::json!({"api_key": "key"})).await,
                Err(CredentialError::Conflict)
            ));
            oauth
                .refresh(|_| async {
                    Ok(RefreshOutcome::Updated(
                        serde_json::json!({"access":"new", "refresh":"rotated", "expires":100}),
                    ))
                })
                .await
                .unwrap();
            assert!(matches!(
                oauth.write(&serde_json::json!({"api_key": "key"})).await,
                Err(CredentialError::InvalidValue)
            ));
        });
        assert_eq!(load_tokens(&dir, "openai").unwrap().access, "new");
        delete_tokens(&dir, "openai").unwrap();
        save_provider_credentials(
            &dir,
            "openai",
            &ProviderCredentials {
                api_key: "key".into(),
                host: None,
            },
        )
        .unwrap();
        assert!(oauth.read().unwrap().is_none());
        assert_eq!(key.read().unwrap().unwrap()["api_key"], "key");
        assert!(!smol::block_on(oauth.delete()).unwrap());
        assert_eq!(
            load_provider_credentials(&dir, "openai").unwrap().api_key,
            "key"
        );
    }

    #[test]
    fn credential_refresh_reloads_and_preserves_on_failure() {
        let tmp = TempDir::new().unwrap();
        let scope =
            CredentialScope::new(StateDir::from_path(tmp.path().into()), "provider").unwrap();
        let value = serde_json::json!({"access": "original"});
        smol::block_on(async {
            scope.write(&value).await.unwrap();
            let result = scope
                .refresh(|current| async move {
                    assert_eq!(current, Some(value));
                    Err(CredentialError::Refresh("temporary".into()))
                })
                .await;
            assert!(matches!(result, Err(CredentialError::Refresh(_))));
            assert_eq!(scope.read().unwrap().unwrap()["access"], "original");
            let result = scope
                .refresh(|_| async {
                    Ok(RefreshOutcome::Updated(
                        serde_json::json!({"access": "rotated"}),
                    ))
                })
                .await
                .unwrap();
            assert_eq!(result.unwrap()["access"], "rotated");
        });
    }

    #[test]
    fn credential_refresh_deletes_under_lock() {
        let tmp = TempDir::new().unwrap();
        let scope =
            CredentialScope::new(StateDir::from_path(tmp.path().into()), "provider").unwrap();
        smol::block_on(async {
            scope
                .write(&serde_json::json!({"access": "secret"}))
                .await
                .unwrap();
            assert!(
                scope
                    .refresh(|current| async move {
                        assert!(current.is_some());
                        Ok(RefreshOutcome::Deleted)
                    })
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        assert!(scope.read().unwrap().is_none());
        assert!(!scope.path().exists());
    }

    #[test]
    fn cancelled_credential_refresh_does_not_commit() {
        let tmp = TempDir::new().unwrap();
        let scope =
            CredentialScope::new(StateDir::from_path(tmp.path().into()), "provider").unwrap();
        smol::block_on(async {
            scope
                .write(&serde_json::json!({"access": "original"}))
                .await
                .unwrap();
            let future = scope.refresh(|_| async {
                std::future::pending::<()>().await;
                Ok(RefreshOutcome::Updated(
                    serde_json::json!({"access": "new"}),
                ))
            });
            let mut future = Box::pin(future);
            assert!(futures_poll_once(future.as_mut()).await.is_none());
            drop(future);
        });
        assert_eq!(scope.read().unwrap().unwrap()["access"], "original");
    }

    #[cfg(unix)]
    #[test]
    fn credential_mutations_accept_read_only_legacy_lock_file() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().into());
        drop(lock_tokens(&dir, "provider").unwrap());
        fs::set_permissions(
            token_lock_path(&dir, "provider"),
            fs::Permissions::from_mode(0o400),
        )
        .unwrap();
        let scope = CredentialScope::new(dir, "provider").unwrap();
        smol::block_on(scope.write(&serde_json::json!({"access": "token"}))).unwrap();
    }

    #[test]
    fn credential_mutations_wait_for_legacy_lock() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().into());
        let scope = CredentialScope::new(dir.clone(), "provider").unwrap();
        let legacy_lock = lock_tokens(&dir, "provider").unwrap();
        smol::block_on(async {
            let value = serde_json::json!({"access": "new"});
            let mut write = Box::pin(scope.write(&value));
            let mut delete = Box::pin(scope.delete());
            assert!(futures_poll_once(write.as_mut()).await.is_none());
            assert!(futures_poll_once(delete.as_mut()).await.is_none());
            assert!(scope.read().unwrap().is_none());
            drop(legacy_lock);
            write.await.unwrap();
            assert_eq!(scope.read().unwrap(), Some(value));
            assert!(delete.await.unwrap());
            assert!(scope.read().unwrap().is_none());
        });
    }

    #[test_case("provider", false; "token_save")]
    #[test_case("provider", true; "provider_delete")]
    #[test_case("mcp-server", false; "mcp_save")]
    #[test_case("mcp-server", true; "mcp_delete")]
    fn legacy_mutations_serialize_with_scoped_refresh(name: &str, delete: bool) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().into());
        let scope = CredentialScope::new(dir.clone(), name).unwrap();
        let original = serde_json::json!({"access": "original"});
        smol::block_on(scope.write(&original)).unwrap();
        let lock = lock_tokens(&dir, name).unwrap();
        let name = name.to_owned();
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            LOCK_BLOCKED.with(|sender| *sender.borrow_mut() = Some(event_tx.clone()));
            if name == "mcp-server" {
                if delete {
                    delete_mcp_auth(&dir, "server").unwrap();
                } else {
                    save_mcp_auth(&dir, "server", &test_mcp_data()).unwrap();
                }
            } else if delete {
                delete_provider_credentials(&dir, &name).unwrap();
            } else {
                save_tokens(
                    &dir,
                    &name,
                    &OAuthTokens {
                        access: "legacy".into(),
                        refresh: "refresh".into(),
                        expires: 0,
                        account_id: None,
                    },
                )
                .unwrap();
            }
            event_tx.send(false).unwrap();
        });
        assert!(
            event_rx.recv().unwrap(),
            "mutation completed without waiting for the lock"
        );
        assert_eq!(scope.read().unwrap(), Some(original));
        scope
            .write_unlocked(&serde_json::json!({"access": "rotated"}))
            .unwrap();
        drop(lock);
        worker.join().unwrap();
        let result = scope.read().unwrap();
        if delete {
            assert!(result.is_none());
        } else if scope.name == "mcp-server" {
            assert_eq!(result.unwrap()["client_id"], "client-123");
        } else {
            assert_eq!(result.unwrap()["access"], "legacy");
        }
    }

    async fn futures_poll_once<F: Future>(mut future: std::pin::Pin<&mut F>) -> Option<F::Output> {
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match future.as_mut().poll(cx) {
                std::task::Poll::Ready(value) => Some(value),
                std::task::Poll::Pending => None,
            })
        })
        .await
    }

    fn test_mcp_data() -> McpAuthData {
        McpAuthData {
            server_url: TEST_URL.into(),
            tokens: None,
            client_id: "client-123".into(),
            client_secret: None,
            client_secret_expires_at: None,
            redirect_uri: None,
            token_endpoint: None,
        }
    }

    #[test_case(0,                              true  ; "epoch_is_expired")]
    #[test_case(now_millis() + 3_600_000,       false ; "future_is_valid")]
    fn token_expiry(expires: u64, expected: bool) {
        let tokens = OAuthTokens {
            access: "a".into(),
            refresh: "r".into(),
            expires,
            account_id: None,
        };
        assert_eq!(tokens.is_expired(), expected);
    }

    #[test]
    fn save_load_delete_round_trip() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let tokens = OAuthTokens {
            access: "access_tok".into(),
            refresh: "refresh_tok".into(),
            expires: 9999999999,
            account_id: None,
        };
        save_tokens(&dir, "anthropic", &tokens).unwrap();

        let loaded = load_tokens(&dir, "anthropic").unwrap();
        assert_eq!(loaded.access, "access_tok");
        assert_eq!(loaded.refresh, "refresh_tok");
        assert_eq!(loaded.expires, 9999999999);

        #[cfg(unix)]
        {
            let metadata = fs::metadata(auth_path(&dir, "anthropic")).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, AUTH_FILE_MODE);
        }

        assert!(delete_tokens(&dir, "anthropic").unwrap());
        assert!(load_tokens(&dir, "anthropic").is_none());
        assert!(!delete_tokens(&dir, "anthropic").unwrap());
    }

    #[test]
    fn mcp_auth_round_trip() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let data = McpAuthData {
            tokens: Some(OAuthTokens {
                access: "acc".into(),
                refresh: "ref".into(),
                expires: 9999999999,
                account_id: None,
            }),
            ..test_mcp_data()
        };
        save_mcp_auth(&dir, "srv", &data).unwrap();
        let loaded = load_mcp_auth(&dir, "srv", TEST_URL).unwrap();
        assert_eq!(loaded.client_id, "client-123");
        assert_eq!(loaded.tokens.unwrap().access, "acc");
    }

    #[test_case(
        test_mcp_data(),
        "https://other.example.com"
        ; "url_mismatch"
    )]
    #[test_case(
        McpAuthData {
            client_secret: Some("s".into()),
            client_secret_expires_at: Some(1),
            ..test_mcp_data()
        },
        TEST_URL
        ; "expired_client_secret"
    )]
    fn mcp_auth_load_returns_none(data: McpAuthData, lookup_url: &str) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        save_mcp_auth(&dir, "srv", &data).unwrap();
        assert!(load_mcp_auth(&dir, "srv", lookup_url).is_none());
    }
}
