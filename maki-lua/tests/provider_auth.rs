use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use flume::{Receiver, Sender};
use futures_lite::future;
use maki_agent::tools::ToolRegistry;
use maki_config::providers::ProvidersConfig;
use maki_lua::PluginHost;
use maki_lua::test_support::InMemoryFs;
use maki_lua::ui_sink::HostUi;
use maki_providers::login::{
    AuthState, InteractionRequest, InteractionService, LoginOutcome, LoginRequest, auth_status,
    drive_login, logout,
};
use maki_providers::provider::BoxFuture;
use maki_providers::{AgentError, ProviderRegistry, Timeouts};
use maki_storage::StateDir;
use maki_storage::auth::{
    ProviderCredentials, load_provider_credentials, save_provider_credentials,
};
use tempfile::TempDir;

const PLUGIN: &str = "fake_plugin";
const PROVIDER: &str = "fake";
const DISPLAY_NAME: &str = "Fake";
const OTHER: &str = "other";
const SWALLOW_ALIAS: &str = "fake-swallow";
const KEY_METHOD: &str = "api_key";
const KEY_PROMPT: &str = "API key";
const KEY: &str = "sk-from-login";
const ANSWERED_URL: &str = "http://example.test";
const RETURNED_MODEL: &str = "fake-2";
const NO_URL: &str = "none";
const BOUND: Duration = Duration::from_secs(10);

const SOURCE: &str = r#"
    maki.api.register_provider({
        id = "fake",
        display_name = "Fake",
        models = { { id = "fake-1", tier = "strong" } },
        prepare = function() return {} end,
        config = {
            { key = "base_url", prompt = "Host URL", default = "http://localhost:8080", optional = true },
        },
        auth = {
            { id = "api_key", label = "API key", login = function(ctx, ui, config)
                local key, err = ui:input("API key", true)
                if not key then return nil, err end
                assert(ctx.credentials:write({ api_key = key, host = config.base_url }))
                return { model = "fake-2" }
            end },
            { id = "swallow", label = "Ignores errors", login = function(ctx, ui)
                ui:input("API key", true)
                return true
            end },
        },
        aliases = { { id = "fake-swallow", label = "Fake (ignores errors)", method = "swallow" } },
    })
    maki.api.register_provider({
        id = "other",
        display_name = "Other",
        prepare = function() return {} end,
        auth = { { id = "api_key", label = "API key", login = function() return true end } },
        auth_status = function(ctx, config)
            return { state = "configured", detail = config.base_url or "none" }
        end,
    })
"#;

/// Asks go to `asked`; answers come from `answers`, and a closed answer
/// channel is a user who backed out.
struct ChannelInteraction {
    asked: Sender<InteractionRequest>,
    answers: Receiver<String>,
}

impl InteractionService for ChannelInteraction {
    fn interact(&self, request: InteractionRequest) -> BoxFuture<'_, Result<String, AgentError>> {
        let _ = self.asked.send(request);
        Box::pin(async move {
            self.answers
                .recv_async()
                .await
                .map_err(|_| AgentError::Cancelled)
        })
    }
}

struct Fixture {
    _host: PluginHost,
    registry: Arc<ProviderRegistry>,
    storage: StateDir,
    _state: TempDir,
}

impl Fixture {
    fn new() -> Self {
        let state = TempDir::new().unwrap();
        maki_storage::paths::init_at(state.path().to_path_buf()).unwrap();
        let host = PluginHost::with_session_provider_preparer(
            Arc::new(ToolRegistry::new()),
            Default::default(),
            true,
            Arc::new(InMemoryFs::new()),
            Some(state.path().to_path_buf()),
            None,
            HostUi::Frontend,
        )
        .unwrap();
        host.load_source(PLUGIN, SOURCE).unwrap();
        Self {
            registry: host.provider_registry(Timeouts::default()).unwrap(),
            _host: host,
            storage: StateDir::from_path(state.path().to_path_buf()),
            _state: state,
        }
    }

    /// Starts a login on its own thread, answering from {answers}.
    fn start_login(
        &self,
        provider: &str,
        answers: Receiver<String>,
    ) -> (
        Receiver<InteractionRequest>,
        thread::JoinHandle<Result<LoginOutcome, AgentError>>,
    ) {
        let (asked, asked_rx) = flume::unbounded();
        let ui = Arc::new(ChannelInteraction { asked, answers });
        let registry = Arc::clone(&self.registry);
        let storage = self.storage.clone();
        let request = LoginRequest {
            provider: Some(provider.into()),
            native: Vec::new(),
        };
        let login = thread::spawn(move || {
            smol::block_on(bounded(drive_login(&registry, ui, &storage, request)))
        });
        (asked_rx, login)
    }

    fn states(&self) -> Vec<(String, AuthState, Option<String>)> {
        smol::block_on(bounded(auth_status(&self.registry, &self.storage)))
            .into_iter()
            .map(|provider| {
                (
                    provider.id.to_string(),
                    provider.status.state,
                    provider.status.detail,
                )
            })
            .collect()
    }
}

async fn bounded<T>(fut: impl Future<Output = T>) -> T {
    future::or(fut, async {
        smol::Timer::after(BOUND).await;
        panic!("timed out after {BOUND:?}");
    })
    .await
}

fn scripted(answers: &[&str]) -> (Sender<String>, Receiver<String>) {
    let (tx, rx) = flume::unbounded();
    for answer in answers {
        tx.send((*answer).to_owned()).unwrap();
    }
    (tx, rx)
}

fn saved_section_exists() -> bool {
    ProvidersConfig::try_load().unwrap().get(PROVIDER).is_some()
}

#[test]
fn drive_login_through_lua_provider() {
    let fixture = Fixture::new();
    let (answers, answers_rx) = scripted(&[ANSWERED_URL, KEY_METHOD, KEY]);

    let (asked, login) = fixture.start_login(PROVIDER, answers_rx);
    let outcome = login.join().unwrap().unwrap();
    drop(answers);

    assert_eq!(
        outcome,
        LoginOutcome::LoggedIn {
            provider: PROVIDER.into(),
            display_name: DISPLAY_NAME.into(),
            model: Some(format!("{PROVIDER}/{RETURNED_MODEL}")),
        }
    );
    assert_eq!(asked.len(), 3);
    let saved = load_provider_credentials(&fixture.storage, PROVIDER).unwrap();
    assert_eq!(saved.api_key, KEY);
    assert_eq!(saved.host.as_deref(), Some(ANSWERED_URL));
    let section = ProvidersConfig::try_load().unwrap();
    assert_eq!(
        section.get(PROVIDER).unwrap().base_url.as_deref(),
        Some(ANSWERED_URL)
    );
}

#[test]
fn login_suspends_on_interaction() {
    let fixture = Fixture::new();
    let (answers, answers_rx) = scripted(&[ANSWERED_URL, KEY_METHOD]);

    let (asked, login) = fixture.start_login(PROVIDER, answers_rx);
    let key_prompt = InteractionRequest::Input {
        prompt: KEY_PROMPT.into(),
        secret: true,
        default: None,
    };
    let asks: Vec<_> = (0..3).map(|_| asked.recv_timeout(BOUND).unwrap()).collect();
    assert_eq!(asks.last(), Some(&key_prompt));

    let other = fixture
        .states()
        .into_iter()
        .find(|(id, _, _)| id == OTHER)
        .unwrap();
    assert_eq!(
        other,
        (OTHER.into(), AuthState::Configured, Some(NO_URL.into()))
    );

    answers.send(KEY.into()).unwrap();
    login.join().unwrap().unwrap();
    let saved = load_provider_credentials(&fixture.storage, PROVIDER).unwrap();
    assert_eq!(saved.api_key, KEY);
}

#[test]
fn cancelled_interaction_maps_to_cancelled() {
    let fixture = Fixture::new();
    let (answers, answers_rx) = scripted(&[ANSWERED_URL]);
    drop(answers);

    let (_asked, login) = fixture.start_login(SWALLOW_ALIAS, answers_rx);

    assert!(matches!(login.join().unwrap(), Err(AgentError::Cancelled)));
    assert!(!saved_section_exists());
}

#[test]
fn default_auth_status_and_logout_use_credentials_file() {
    let fixture = Fixture::new();
    let credentials = ProviderCredentials {
        api_key: KEY.into(),
        host: None,
    };
    save_provider_credentials(&fixture.storage, PROVIDER, &credentials).unwrap();
    let state_of = |id: &str| {
        fixture
            .states()
            .into_iter()
            .find(|(state_id, _, _)| state_id == id)
            .map(|(_, state, _)| state)
    };
    assert_eq!(state_of(PROVIDER), Some(AuthState::Saved));

    let name = smol::block_on(bounded(logout(
        &fixture.registry,
        &fixture.storage,
        PROVIDER,
    )));

    assert_eq!(name.unwrap().as_deref(), Some(DISPLAY_NAME));
    assert!(load_provider_credentials(&fixture.storage, PROVIDER).is_none());
    assert_eq!(state_of(PROVIDER), Some(AuthState::Missing));
}
