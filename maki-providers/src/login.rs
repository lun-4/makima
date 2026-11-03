//! The login flow every frontend drives, and auth status and logout for
//! registered providers. Frontends only implement `InteractionService`.

use std::sync::Arc;

use futures::future::join_all;
use maki_config::providers::ProvidersConfig;
use maki_storage::StateDir;
use maki_storage::auth::{delete_provider_credentials, load_credentials, save_credentials};
use maki_storage::model::persist_model;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use crate::error::AgentError;
use crate::model::ModelTier;
use crate::provider::BoxFuture;
use crate::registry::{Entry, ProviderRegistry};

const PROVIDER_PROMPT: &str = "Provider";
const METHOD_PROMPT: &str = "Login method";
const NO_METHODS_ERR: &str = "has no login methods";
const NO_CREDENTIALS_ERR: &str = "reports no credentials after login";

#[derive(Debug, Clone, PartialEq)]
pub enum InteractionRequest {
    /// Answered with the entered text. {default} is offered for editing.
    Input {
        prompt: String,
        secret: bool,
        default: Option<String>,
    },
    /// Answered with the chosen option's id.
    Choice {
        prompt: String,
        options: Vec<ChoiceOption>,
    },
    /// The notices below are answered at once, without waiting for the user.
    Instructions(String),
    DeviceCode {
        url: String,
        code: String,
    },
    /// The frontend opens the URL in a browser and shows it, in case the
    /// browser does not open.
    OpenBrowser(String),
}

pub trait InteractionService: Send + Sync {
    /// The user's answer, empty for a notice. A user who backs out is
    /// `AgentError::Cancelled`.
    fn interact(&self, request: InteractionRequest) -> BoxFuture<'_, Result<String, AgentError>>;
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChoiceOption {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub section: Option<String>,
}

#[derive(Clone, Default)]
pub struct AuthSpec {
    pub methods: Vec<AuthMethod>,
    pub aliases: Vec<LoginAlias>,
    pub config: Vec<ConfigField>,
}

/// Decoded from a table that also holds the `login` function.
#[derive(Clone, Deserialize)]
pub struct AuthMethod {
    pub id: String,
    pub label: String,
}

/// A name that logs in to a provider with a preset method.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginAlias {
    pub id: String,
    pub label: String,
    pub method: String,
}

/// A `providers.toml` field prompted before login.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigField {
    pub key: String,
    pub prompt: String,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub secret: bool,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub choices: Vec<ChoiceOption>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthState {
    Saved,
    Env,
    Configured,
    Missing,
    Failed,
}

impl AuthState {
    pub fn label(self) -> Option<&'static str> {
        match self {
            Self::Saved => Some("saved"),
            Self::Env => Some("env"),
            Self::Configured => Some("configured"),
            Self::Failed => Some("error"),
            Self::Missing => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthStatus {
    pub state: AuthState,
    #[serde(default)]
    pub detail: Option<String>,
}

pub struct ProviderStatus {
    pub id: Arc<str>,
    pub display_name: Arc<str>,
    pub status: AuthStatus,
}

pub struct LoginRequest {
    /// A provider id or alias; `None` asks the user.
    pub provider: Option<String>,
    /// Native providers the frontend can still log in to; removed with them in commit 9.
    pub native: Vec<ChoiceOption>,
}

#[derive(Debug, PartialEq)]
pub enum LoginOutcome {
    LoggedIn {
        provider: Arc<str>,
        display_name: Arc<str>,
        model: Option<String>,
    },
    /// A provider the registry does not know; the frontend runs its native flow.
    Native(String),
}

fn config_error(message: impl Into<String>) -> AgentError {
    AgentError::Config {
        message: message.into(),
    }
}

/// Selects a provider, asks its config fields and login method, and logs in.
/// Config is saved and the default model chosen only after the provider
/// reports credentials; a failed login also restores the credentials file.
pub async fn drive_login(
    registry: &ProviderRegistry,
    ui: Arc<dyn InteractionService>,
    storage: &StateDir,
    request: LoginRequest,
) -> Result<LoginOutcome, AgentError> {
    let name = match request.provider {
        Some(name) => name,
        None => {
            let options = provider_options(registry, storage, request.native).await;
            ui.interact(InteractionRequest::Choice {
                prompt: PROVIDER_PROMPT.into(),
                options,
            })
            .await?
        }
    };
    let Some((id, entry, preset)) = registry.login_target(&name) else {
        return Ok(LoginOutcome::Native(name));
    };
    let current = ProvidersConfig::try_load()
        .map_err(config_error)?
        .get(id)
        .cloned()
        .unwrap_or_default();
    let answers = ask_config(&*ui, &entry.auth.config, &serde_json::to_value(&current)?).await?;
    let pending = current.with_answers(&answers).map_err(config_error)?;
    let method = match (preset, entry.auth.methods.as_slice()) {
        (Some(method), _) => method.to_owned(),
        (None, []) => {
            return Err(config_error(format!(
                "{} {NO_METHODS_ERR}",
                entry.display_name
            )));
        }
        (None, [only]) => only.id.clone(),
        (None, methods) => {
            let options = methods
                .iter()
                .map(|method| ChoiceOption {
                    id: method.id.clone(),
                    label: method.label.clone(),
                    ..ChoiceOption::default()
                })
                .collect();
            ui.interact(InteractionRequest::Choice {
                prompt: METHOD_PROMPT.into(),
                options,
            })
            .await?
        }
    };

    let config = serde_json::to_value(&pending)?;
    let snapshot = load_credentials(storage, id);
    let model = match checked_login(entry, id, storage, method, config, ui).await {
        Ok(model) => model,
        Err(error) => {
            restore(storage, id, snapshot);
            return Err(error);
        }
    };
    if !answers.is_empty() {
        let mut providers = ProvidersConfig::try_load().map_err(config_error)?;
        providers.upsert(id.to_string(), pending);
        providers.save()?;
    }
    let model = match model {
        Some(model) => Some(format!("{id}/{model}")),
        None => registry.spec_for_tier(id, ModelTier::Strong),
    };
    if let Some(spec) = &model {
        persist_model(storage, spec);
    }
    Ok(LoginOutcome::LoggedIn {
        provider: Arc::clone(id),
        display_name: Arc::clone(&entry.display_name),
        model,
    })
}

async fn provider_options(
    registry: &ProviderRegistry,
    storage: &StateDir,
    native: Vec<ChoiceOption>,
) -> Vec<ChoiceOption> {
    let aliases = registry
        .entries()
        .flat_map(|(_, entry)| &entry.auth.aliases)
        .map(|alias| ChoiceOption {
            id: alias.id.clone(),
            label: alias.label.clone(),
            ..ChoiceOption::default()
        });
    auth_status(registry, storage)
        .await
        .into_iter()
        .map(|provider| ChoiceOption {
            id: provider.id.to_string(),
            label: provider.display_name.to_string(),
            detail: provider.status.state.label().map(str::to_owned),
            section: None,
        })
        .chain(aliases)
        .chain(native)
        .collect()
}

/// Answers that differ from {current}. An empty answer keeps the current
/// value; a required field without one asks again.
async fn ask_config(
    ui: &dyn InteractionService,
    fields: &[ConfigField],
    current: &Value,
) -> Result<Vec<(String, String)>, AgentError> {
    let mut answers = Vec::new();
    for field in fields {
        let current = current.get(&field.key).and_then(Value::as_str);
        loop {
            let request = if field.choices.is_empty() {
                InteractionRequest::Input {
                    prompt: field.prompt.clone(),
                    secret: field.secret,
                    default: current.map(str::to_owned).or_else(|| field.default.clone()),
                }
            } else {
                InteractionRequest::Choice {
                    prompt: field.prompt.clone(),
                    options: field.choices.clone(),
                }
            };
            let answer = ui.interact(request).await?;
            if answer.is_empty() && !field.optional && current.is_none() {
                continue;
            }
            if !answer.is_empty() && current != Some(answer.as_str()) {
                answers.push((field.key.clone(), answer));
            }
            break;
        }
    }
    Ok(answers)
}

async fn checked_login(
    entry: &Entry,
    id: &str,
    storage: &StateDir,
    method: String,
    config: Value,
    ui: Arc<dyn InteractionService>,
) -> Result<Option<String>, AgentError> {
    let model = entry.backend.login(method, config.clone(), ui).await?;
    let status = status_of(entry, id, storage, config).await?;
    if matches!(status.state, AuthState::Missing | AuthState::Failed) {
        return Err(config_error(format!(
            "{} {NO_CREDENTIALS_ERR}",
            entry.display_name
        )));
    }
    Ok(model)
}

/// A provider without its own check counts as logged in while its
/// credentials file exists.
async fn status_of(
    entry: &Entry,
    id: &str,
    storage: &StateDir,
    config: Value,
) -> Result<AuthStatus, AgentError> {
    match entry.backend.auth_status(config) {
        Some(status) => status.await,
        None => Ok(AuthStatus {
            state: match load_credentials(storage, id) {
                Some(_) => AuthState::Saved,
                None => AuthState::Missing,
            },
            detail: None,
        }),
    }
}

fn restore(storage: &StateDir, id: &str, snapshot: Option<Value>) {
    let restored = match snapshot {
        Some(value) => save_credentials(storage, id, &value),
        None => delete_provider_credentials(storage, id).map(drop),
    };
    if let Err(error) = restored {
        warn!(provider = id, %error, "credentials not restored after failed login");
    }
}

/// Every registered provider's status, in registry order. A failing check
/// is reported as `Failed` and the listing continues.
pub async fn auth_status(registry: &ProviderRegistry, storage: &StateDir) -> Vec<ProviderStatus> {
    let providers = ProvidersConfig::load_or_default();
    join_all(registry.entries().map(|(id, entry)| {
        let section = providers.get(id).cloned().unwrap_or_default();
        async move {
            let status = match serde_json::to_value(section) {
                Ok(config) => status_of(entry, id, storage, config).await,
                Err(error) => Err(error.into()),
            }
            .unwrap_or_else(|error| {
                warn!(provider = %id, %error, "auth status check failed");
                AuthStatus {
                    state: AuthState::Failed,
                    detail: Some(error.to_string()),
                }
            });
            ProviderStatus {
                id: Arc::clone(id),
                display_name: Arc::clone(&entry.display_name),
                status,
            }
        }
    }))
    .await
}

/// Logs out of a registered provider or alias and removes its
/// `providers.toml` section. `None` for a name the registry does not know.
pub async fn logout(
    registry: &ProviderRegistry,
    storage: &StateDir,
    provider: &str,
) -> Result<Option<Arc<str>>, AgentError> {
    let Some((id, entry, _)) = registry.login_target(provider) else {
        return Ok(None);
    };
    match entry.backend.logout() {
        Some(logout) => logout.await?,
        None => {
            delete_provider_credentials(storage, id)
                .map_err(|error| config_error(error.to_string()))?;
        }
    }
    let mut providers = ProvidersConfig::try_load().map_err(config_error)?;
    if providers.remove(id) {
        providers.save()?;
    }
    Ok(Some(Arc::clone(&entry.display_name)))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use maki_config::providers::ProviderDef;
    use maki_storage::model::read_model;
    use serde_json::json;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;
    use crate::model::{Model, ModelInfo};
    use crate::providers::Timeouts;
    use crate::registry::{PreparedProvider, ProviderBackend, RegisteredProvider};

    const PROVIDER: &str = "fake-login";
    const FAILING: &str = "a-failing";
    const DISPLAY_NAME: &str = "Fake Login";
    const ALIAS: &str = "fake-device";
    const ALIAS_LABEL: &str = "Fake (device)";
    const KEY_METHOD: &str = "api_key";
    const DEVICE_METHOD: &str = "device";
    const BASE_URL_KEY: &str = "base_url";
    const BASE_URL_PROMPT: &str = "Host URL";
    const DEFAULT_URL: &str = "http://localhost:8080";
    const ANSWERED_URL: &str = "http://example.test";
    const KEY_PROMPT: &str = "API key";
    const KEY: &str = "sk-new";
    const OLD_KEY: &str = "sk-old";
    const STRONG_MODEL: &str = "strong-1";
    const CHOSEN_MODEL: &str = "chosen-1";
    const NATIVE: &str = "native-provider";
    const STATUS_ERR: &str = "status exploded";
    const LOGIN_ERR: &str = "login exploded";

    #[derive(Default)]
    struct ScriptedInteraction {
        answers: Mutex<VecDeque<String>>,
        asked: Mutex<Vec<InteractionRequest>>,
    }

    impl ScriptedInteraction {
        fn new(answers: &[&str]) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(answers.iter().map(|a| (*a).to_owned()).collect()),
                asked: Mutex::default(),
            })
        }

        fn asked(&self) -> Vec<InteractionRequest> {
            self.asked.lock().unwrap().clone()
        }
    }

    /// Runs out of answers the way a user backs out.
    impl InteractionService for ScriptedInteraction {
        fn interact(
            &self,
            request: InteractionRequest,
        ) -> BoxFuture<'_, Result<String, AgentError>> {
            self.asked.lock().unwrap().push(request);
            let answer = self.answers.lock().unwrap().pop_front();
            Box::pin(async move { answer.ok_or(AgentError::Cancelled) })
        }
    }

    #[derive(Clone, Copy)]
    enum Status {
        CredentialsFile,
        Missing,
        Fails,
    }

    struct FakeAuthBackend {
        storage: StateDir,
        status: Status,
        login_fails: bool,
        model: Option<&'static str>,
        method: Arc<Mutex<Option<String>>>,
    }

    impl FakeAuthBackend {
        fn new(storage: &StateDir, status: Status) -> Self {
            Self {
                storage: storage.clone(),
                status,
                login_fails: false,
                model: None,
                method: Arc::default(),
            }
        }
    }

    impl ProviderBackend for FakeAuthBackend {
        fn prepare(
            &self,
            _model: Model,
            _info: ModelInfo,
        ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>> {
            Box::pin(async { Err(config_error("not used")) })
        }

        fn discover(&self) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
            None
        }

        /// Writes the key it is given before failing, so a restore is visible.
        fn login(
            &self,
            method: String,
            _config: Value,
            ui: Arc<dyn InteractionService>,
        ) -> BoxFuture<'static, Result<Option<String>, AgentError>> {
            *self.method.lock().unwrap() = Some(method);
            let (storage, fails, model) = (self.storage.clone(), self.login_fails, self.model);
            Box::pin(async move {
                let key = ui
                    .interact(InteractionRequest::Input {
                        prompt: KEY_PROMPT.into(),
                        secret: true,
                        default: None,
                    })
                    .await?;
                save_credentials(&storage, PROVIDER, &json!({ "api_key": key })).unwrap();
                match fails {
                    true => Err(config_error(LOGIN_ERR)),
                    false => Ok(model.map(str::to_owned)),
                }
            })
        }

        fn auth_status(
            &self,
            _config: Value,
        ) -> Option<BoxFuture<'static, Result<AuthStatus, AgentError>>> {
            let status = match self.status {
                Status::CredentialsFile => return None,
                Status::Missing => Ok(AuthStatus {
                    state: AuthState::Missing,
                    detail: None,
                }),
                Status::Fails => Err(config_error(STATUS_ERR)),
            };
            Some(Box::pin(async move { status }))
        }

        fn logout(&self) -> Option<BoxFuture<'static, Result<(), AgentError>>> {
            None
        }
    }

    struct Fixture {
        _dir: TempDir,
        storage: StateDir,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = TempDir::new().unwrap();
            maki_storage::paths::init_at(dir.path().to_path_buf()).unwrap();
            let storage = StateDir::from_path(dir.path().to_path_buf());
            Self { _dir: dir, storage }
        }

        fn registry(&self, backend: FakeAuthBackend, methods: &[&str]) -> ProviderRegistry {
            ProviderRegistry::new(
                vec![registered(PROVIDER, backend, methods)],
                Timeouts::default(),
            )
        }

        fn login(
            &self,
            registry: &ProviderRegistry,
            ui: Arc<ScriptedInteraction>,
            provider: Option<&str>,
        ) -> Result<LoginOutcome, AgentError> {
            smol::block_on(drive_login(
                registry,
                ui,
                &self.storage,
                LoginRequest {
                    provider: provider.map(str::to_owned),
                    native: vec![option(NATIVE)],
                },
            ))
        }

        fn saved_key(&self) -> Option<Value> {
            load_credentials(&self.storage, PROVIDER).map(|saved| saved["api_key"].clone())
        }
    }

    fn option(id: &str) -> ChoiceOption {
        ChoiceOption {
            id: id.into(),
            label: id.into(),
            ..ChoiceOption::default()
        }
    }

    fn registered(id: &str, backend: FakeAuthBackend, methods: &[&str]) -> RegisteredProvider {
        RegisteredProvider {
            id: id.into(),
            display_name: DISPLAY_NAME.into(),
            models: vec![ModelInfo {
                tier: Some(ModelTier::Strong),
                ..ModelInfo::id_only(STRONG_MODEL.into())
            }],
            backend: Arc::new(backend),
            auth: AuthSpec {
                methods: methods
                    .iter()
                    .map(|id| AuthMethod {
                        id: (*id).into(),
                        label: (*id).into(),
                    })
                    .collect(),
                aliases: vec![LoginAlias {
                    id: ALIAS.into(),
                    label: ALIAS_LABEL.into(),
                    method: DEVICE_METHOD.into(),
                }],
                config: vec![ConfigField {
                    key: BASE_URL_KEY.into(),
                    prompt: BASE_URL_PROMPT.into(),
                    default: Some(DEFAULT_URL.into()),
                    secret: false,
                    optional: true,
                    choices: Vec::new(),
                }],
            },
        }
    }

    fn url_prompt(default: &str) -> InteractionRequest {
        InteractionRequest::Input {
            prompt: BASE_URL_PROMPT.into(),
            secret: false,
            default: Some(default.into()),
        }
    }

    fn key_prompt() -> InteractionRequest {
        InteractionRequest::Input {
            prompt: KEY_PROMPT.into(),
            secret: true,
            default: None,
        }
    }

    fn save_section(base_url: &str) {
        let mut providers = ProvidersConfig::default();
        let def = ProviderDef::default()
            .with_answers(&[(BASE_URL_KEY.into(), base_url.into())])
            .unwrap();
        providers.upsert(PROVIDER.into(), def);
        providers.save().unwrap();
    }

    fn saved_url() -> Option<String> {
        ProvidersConfig::try_load()
            .unwrap()
            .get(PROVIDER)
            .and_then(|def| def.base_url.clone())
    }

    fn logged_in(model: &str) -> LoginOutcome {
        LoginOutcome::LoggedIn {
            provider: PROVIDER.into(),
            display_name: DISPLAY_NAME.into(),
            model: Some(format!("{PROVIDER}/{model}")),
        }
    }

    #[test]
    fn explicit_provider_prompts_config_then_logs_in() {
        let fixture = Fixture::new();
        let registry = fixture.registry(
            FakeAuthBackend::new(&fixture.storage, Status::CredentialsFile),
            &[KEY_METHOD],
        );
        let ui = ScriptedInteraction::new(&[ANSWERED_URL, KEY]);

        let outcome = fixture.login(&registry, Arc::clone(&ui), Some(PROVIDER));

        assert_eq!(outcome.unwrap(), logged_in(STRONG_MODEL));
        assert_eq!(ui.asked(), vec![url_prompt(DEFAULT_URL), key_prompt()]);
        assert_eq!(saved_url().as_deref(), Some(ANSWERED_URL));
        assert_eq!(fixture.saved_key(), Some(json!(KEY)));
        assert_eq!(
            read_model(&fixture.storage),
            Some(format!("{PROVIDER}/{STRONG_MODEL}"))
        );
    }

    #[test]
    fn saved_value_is_the_default_and_method_is_chosen() {
        let fixture = Fixture::new();
        save_section(ANSWERED_URL);
        let backend = FakeAuthBackend {
            model: Some(CHOSEN_MODEL),
            ..FakeAuthBackend::new(&fixture.storage, Status::CredentialsFile)
        };
        let registry = fixture.registry(backend, &[KEY_METHOD, DEVICE_METHOD]);
        let ui = ScriptedInteraction::new(&["", KEY_METHOD, KEY]);

        let outcome = fixture.login(&registry, Arc::clone(&ui), Some(PROVIDER));

        assert_eq!(outcome.unwrap(), logged_in(CHOSEN_MODEL));
        let method_choice = InteractionRequest::Choice {
            prompt: METHOD_PROMPT.into(),
            options: vec![option(KEY_METHOD), option(DEVICE_METHOD)],
        };
        assert_eq!(
            ui.asked(),
            vec![url_prompt(ANSWERED_URL), method_choice, key_prompt()]
        );
        assert_eq!(saved_url().as_deref(), Some(ANSWERED_URL));
        assert_eq!(
            read_model(&fixture.storage),
            Some(format!("{PROVIDER}/{CHOSEN_MODEL}"))
        );
    }

    #[test]
    fn alias_presets_method() {
        let fixture = Fixture::new();
        let backend = FakeAuthBackend::new(&fixture.storage, Status::CredentialsFile);
        let method = Arc::clone(&backend.method);
        let registry = fixture.registry(backend, &[KEY_METHOD, DEVICE_METHOD]);
        let ui = ScriptedInteraction::new(&[ANSWERED_URL, KEY]);

        fixture
            .login(&registry, Arc::clone(&ui), Some(ALIAS))
            .unwrap();

        assert_eq!(ui.asked(), vec![url_prompt(DEFAULT_URL), key_prompt()]);
        assert_eq!(method.lock().unwrap().as_deref(), Some(DEVICE_METHOD));
    }

    #[test]
    fn provider_selection_lists_status_aliases_and_natives() {
        let fixture = Fixture::new();
        save_credentials(&fixture.storage, PROVIDER, &json!({ "api_key": OLD_KEY })).unwrap();
        let registry = fixture.registry(
            FakeAuthBackend::new(&fixture.storage, Status::CredentialsFile),
            &[KEY_METHOD],
        );
        let ui = ScriptedInteraction::new(&[NATIVE]);

        let outcome = fixture.login(&registry, Arc::clone(&ui), None);

        assert_eq!(outcome.unwrap(), LoginOutcome::Native(NATIVE.into()));
        let provider = ChoiceOption {
            id: PROVIDER.into(),
            label: DISPLAY_NAME.into(),
            detail: Some(AuthState::Saved.label().unwrap().into()),
            section: None,
        };
        let alias = ChoiceOption {
            id: ALIAS.into(),
            label: ALIAS_LABEL.into(),
            ..ChoiceOption::default()
        };
        assert_eq!(
            ui.asked(),
            vec![InteractionRequest::Choice {
                prompt: PROVIDER_PROMPT.into(),
                options: vec![provider, alias, option(NATIVE)],
            }]
        );
    }

    #[test_case(true, Status::CredentialsFile, &[ANSWERED_URL, KEY], false ; "login_fails")]
    #[test_case(false, Status::Missing, &[ANSWERED_URL, KEY], false ; "no_credentials_after_login")]
    #[test_case(false, Status::CredentialsFile, &[ANSWERED_URL], true ; "cancelled")]
    fn failed_login_leaves_config_and_credentials_unchanged(
        login_fails: bool,
        status: Status,
        answers: &[&str],
        cancelled: bool,
    ) {
        let fixture = Fixture::new();
        save_credentials(&fixture.storage, PROVIDER, &json!({ "api_key": OLD_KEY })).unwrap();
        let backend = FakeAuthBackend {
            login_fails,
            ..FakeAuthBackend::new(&fixture.storage, status)
        };
        let registry = fixture.registry(backend, &[KEY_METHOD]);

        let error = fixture
            .login(&registry, ScriptedInteraction::new(answers), Some(PROVIDER))
            .unwrap_err();

        assert_eq!(matches!(error, AgentError::Cancelled), cancelled, "{error}");
        assert_eq!(saved_url(), None);
        assert_eq!(fixture.saved_key(), Some(json!(OLD_KEY)));
        assert_eq!(read_model(&fixture.storage), None);
    }

    #[test]
    fn auth_status_reports_failure_and_continues() {
        let fixture = Fixture::new();
        save_credentials(&fixture.storage, PROVIDER, &json!({ "api_key": OLD_KEY })).unwrap();
        let registry = ProviderRegistry::new(
            vec![
                registered(
                    FAILING,
                    FakeAuthBackend::new(&fixture.storage, Status::Fails),
                    &[],
                ),
                registered(
                    PROVIDER,
                    FakeAuthBackend::new(&fixture.storage, Status::CredentialsFile),
                    &[],
                ),
            ],
            Timeouts::default(),
        );

        let statuses = smol::block_on(auth_status(&registry, &fixture.storage));

        let states: Vec<_> = statuses
            .iter()
            .map(|provider| (&*provider.id, provider.status.state))
            .collect();
        assert_eq!(
            states,
            vec![(FAILING, AuthState::Failed), (PROVIDER, AuthState::Saved)]
        );
        assert_eq!(statuses[0].status.detail.as_deref(), Some(STATUS_ERR));
    }

    #[test]
    fn logout_removes_credentials_and_section() {
        let fixture = Fixture::new();
        save_credentials(&fixture.storage, PROVIDER, &json!({ "api_key": OLD_KEY })).unwrap();
        save_section(ANSWERED_URL);
        let registry = fixture.registry(
            FakeAuthBackend::new(&fixture.storage, Status::CredentialsFile),
            &[KEY_METHOD],
        );

        let name = smol::block_on(logout(&registry, &fixture.storage, ALIAS)).unwrap();
        let unknown = smol::block_on(logout(&registry, &fixture.storage, NATIVE)).unwrap();

        assert_eq!(name.as_deref(), Some(DISPLAY_NAME));
        assert_eq!(unknown, None);
        assert_eq!(fixture.saved_key(), None);
        assert_eq!(saved_url(), None);
    }
}
