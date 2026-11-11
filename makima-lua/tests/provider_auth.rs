use std::fs;
use std::future::Future;
use std::net::TcpListener;
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use flume::{Receiver, Sender};
use futures_lite::future;
use makima_agent::tools::ToolRegistry;
use makima_config::providers::ProvidersConfig;
use makima_lua::PluginHost;
use makima_lua::test_support::InMemoryFs;
use makima_lua::ui_sink::HostUi;
use makima_providers::login::{
    AuthState, InteractionRequest, InteractionService, LoginOutcome, LoginRequest, auth_status,
    drive_login, logout,
};
use makima_providers::provider::BoxFuture;
use makima_providers::{
    AgentError, Invocation, InvocationPurpose, Message, ProviderRegistry, RequestDelivery,
    RequestOptions, RetryKind,
};
use makima_storage::StateDir;
use makima_storage::auth::{load_credentials, save_credentials};
use serde_json::json;
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
const BUNDLED_PLUGIN: &str = "providers";
const BUNDLED_SOURCE: &str = include_str!("../../plugins/providers/init.lua");
const PLAN_PROVIDER: &str = "zai";
const PLAN_PROVIDER_NAME: &str = "Z.AI";
const PLAN: &str = "coding";
const PLAN_MODEL: &str = "zai/glm-5-code";
const BROWSER_NOTICE: &str = "";
const BUNDLED_KEY: &str = "zk-bundled-login-key";
const MASKED_KEY: &str = "key: zk-b...-key, coding";
const PLAN_PROVIDER_KEY_ENV: &str = "ZHIPU_API_KEY";
const PLAN_LOGIN_URL: &str = "https://z.ai/manage-apikey/apikey-list";
const LOCAL_MODEL: &str = "llama-cpp/local-model";
const LOCAL_HOST_ENV: &str = "LLAMA_CPP_HOST";
const LOOPBACK: &str = "127.0.0.1:0";
const CONVERSATION: &str = "conversation";
const MALFORMED_PROVIDER: &str = "broken-custom";
const VALID_CUSTOM_PROVIDER: &str = "valid-custom";
const VALID_CUSTOM_MODEL: &str = "valid-custom/model";
const MALFORMED_CONFIG: &str = "[broken\n";
const PROVIDER_CONFIG_FATAL_CHILD: &str = "provider_config_fatal_child";
const BAD_CONFIG_EXIT_CODE: i32 = 2;
const AUTO_DETECT_ORDER: &[&str] = &[
    "anthropic",
    "openai",
    "copilot",
    "zai",
    "synthetic",
    "deepseek",
    "regolo",
];

const SOURCE: &str = r#"
    makima.api.register_provider({
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
    makima.api.register_provider({
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
        Self::with_plugin(PLUGIN, SOURCE)
    }

    fn with_plugin(plugin: &str, source: &str) -> Self {
        Self::with_plugin_state(plugin, source, TempDir::new().unwrap())
    }

    fn with_plugin_state(plugin: &str, source: &str, state: TempDir) -> Self {
        makima_storage::paths::init_at(state.path().to_path_buf()).unwrap();
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
        host.load_source(plugin, source).unwrap();
        Self {
            registry: host
                .provider_registry(makima_providers::Timeouts::default())
                .unwrap(),
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
        };
        let login = thread::spawn(move || {
            smol::block_on(bounded(drive_login(
                &registry,
                ui,
                &storage,
                &makima_config::ModelPolicy::default(),
                request,
            )))
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
        LoginOutcome {
            provider: PROVIDER.into(),
            display_name: DISPLAY_NAME.into(),
            model: Some(format!("{PROVIDER}/{RETURNED_MODEL}")),
        }
    );
    assert_eq!(asked.len(), 3);
    assert_eq!(
        load_credentials(&fixture.storage, PROVIDER),
        Some(json!({ "api_key": KEY, "host": ANSWERED_URL }))
    );
    let section = ProvidersConfig::try_load().unwrap();
    assert_eq!(
        section.get(PROVIDER).unwrap().0["base_url"].as_str(),
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
    assert_eq!(
        load_credentials(&fixture.storage, PROVIDER).unwrap()["api_key"],
        KEY
    );
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
    save_credentials(&fixture.storage, PROVIDER, &json!({ "api_key": KEY })).unwrap();
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
    assert!(load_credentials(&fixture.storage, PROVIDER).is_none());
    assert_eq!(state_of(PROVIDER), Some(AuthState::Missing));
}

#[test]
fn bundled_providers_keep_auto_detect_order() {
    let fixture = Fixture::with_plugin(BUNDLED_PLUGIN, BUNDLED_SOURCE);
    let ids: Vec<_> = fixture.states().into_iter().map(|(id, ..)| id).collect();
    assert_eq!(ids[..AUTO_DETECT_ORDER.len()], *AUTO_DETECT_ORDER);
}

#[test]
fn malformed_provider_section_is_skipped_without_aborting_bundled_plugin() {
    let state = TempDir::new().unwrap();
    fs::write(
        state.path().join("providers.toml"),
        format!(
            "[{MALFORMED_PROVIDER}]\nprotocol = \"opneai\"\n\n[{VALID_CUSTOM_PROVIDER}]\nprotocol = \"openai\"\nbase_url = \"http://example.test/v1\"\n\n[[{VALID_CUSTOM_PROVIDER}.models]]\nid = \"model\"\n"
        ),
    )
    .unwrap();

    let fixture = Fixture::with_plugin_state(BUNDLED_PLUGIN, BUNDLED_SOURCE, state);

    let ids: Vec<_> = fixture
        .registry
        .listings()
        .map(|provider| provider.id)
        .collect();
    assert!(!ids.contains(&MALFORMED_PROVIDER));
    assert!(ids.contains(&VALID_CUSTOM_PROVIDER));
    assert!(fixture.registry.model(VALID_CUSTOM_MODEL).is_ok());
}

#[test]
fn malformed_provider_config_remains_fatal_during_bundled_registration() {
    if std::env::var_os(PROVIDER_CONFIG_FATAL_CHILD).is_some() {
        let state = TempDir::new().unwrap();
        fs::write(state.path().join("providers.toml"), MALFORMED_CONFIG).unwrap();
        let _fixture = Fixture::with_plugin_state(BUNDLED_PLUGIN, BUNDLED_SOURCE, state);
        panic!("malformed providers.toml did not exit");
    }

    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("malformed_provider_config_remains_fatal_during_bundled_registration")
        .arg("--nocapture")
        .env(PROVIDER_CONFIG_FATAL_CHILD, "1")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(BAD_CONFIG_EXIT_CODE));
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid"));
}

/// The bundled plugin classes every provider's stream errors, so a local
/// server that is not running draws from the bounded connect budget.
#[test]
fn bundled_stream_errors_carry_a_retry_class() {
    let listener = TcpListener::bind(LOOPBACK).unwrap();
    let host = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    // SAFETY: nextest runs each test in its own process.
    unsafe { std::env::set_var(LOCAL_HOST_ENV, host) };
    let fixture = Fixture::with_plugin(BUNDLED_PLUGIN, BUNDLED_SOURCE);
    let model = fixture.registry.model(LOCAL_MODEL).unwrap();
    let prepared = smol::block_on(bounded(fixture.registry.prepare(model))).unwrap();
    let (events, _events_rx) = flume::unbounded();
    let invocation = Invocation {
        conversation_id: CONVERSATION.into(),
        operation_id: CONVERSATION.into(),
        attempt: 0,
        purpose: InvocationPurpose::Agent,
        agent_id: None,
        previous: None,
    };
    let error = smol::block_on(bounded(prepared.provider.stream_message(
        &prepared.model,
        &[Message::user(CONVERSATION.into())],
        "",
        &json!([]),
        &events,
        RequestOptions::default(),
        None,
        &invocation,
        RequestDelivery::untracked(),
    )))
    .unwrap_err();
    assert_eq!(error.retry_kind(), Some(RetryKind::Connect), "{error:?}");
}

#[test]
fn bundled_provider_logs_in_with_its_plan() {
    // SAFETY: nextest runs each test in its own process.
    unsafe { std::env::remove_var(PLAN_PROVIDER_KEY_ENV) };
    let fixture = Fixture::with_plugin(BUNDLED_PLUGIN, BUNDLED_SOURCE);
    let (answers, answers_rx) = scripted(&[PLAN, BROWSER_NOTICE, BUNDLED_KEY]);

    let (asked, login) = fixture.start_login(PLAN_PROVIDER, answers_rx);
    let outcome = login.join().unwrap().unwrap();
    drop(answers);

    assert_eq!(
        outcome,
        LoginOutcome {
            provider: PLAN_PROVIDER.into(),
            display_name: PLAN_PROVIDER_NAME.into(),
            model: Some(PLAN_MODEL.into()),
        }
    );
    let asks: Vec<_> = asked.drain().collect();
    assert!(matches!(asks[0], InteractionRequest::Choice { .. }));
    assert_eq!(
        asks[1],
        InteractionRequest::OpenBrowser(PLAN_LOGIN_URL.into())
    );
    assert!(matches!(
        asks[2],
        InteractionRequest::Input { secret: true, .. }
    ));
    assert_eq!(
        load_credentials(&fixture.storage, PLAN_PROVIDER).unwrap()["api_key"],
        BUNDLED_KEY
    );
    let config = ProvidersConfig::try_load().unwrap();
    assert_eq!(
        config.get(PLAN_PROVIDER).unwrap().0["plan"].as_str(),
        Some(PLAN)
    );
    let status = fixture
        .states()
        .into_iter()
        .find(|(id, _, _)| id == PLAN_PROVIDER)
        .unwrap();
    assert_eq!(
        status,
        (
            PLAN_PROVIDER.into(),
            AuthState::Saved,
            Some(MASKED_KEY.into())
        )
    );
}
