use super::{EventLoop, tests::with_event_loop_providers};
use crate::components::{Action, key, model_picker::ModelPickerAction};
use crossterm::event::KeyCode;
use makima_config::ModelPolicy;
use makima_providers::login::{AuthSpec, AuthStatus, InteractionService};
use makima_providers::provider::{self, BoxFuture};
use makima_providers::{
    AgentError, Model, ModelInfo, PreparedProvider, ProviderBackend, ProviderRegistry,
    RegisteredProvider,
};
use makima_providers::{
    CompleteModelSelection, ModelCapabilities, ModelSelection, ThinkingConfig, ThinkingSupport,
};
use ratatui::{Terminal, backend::TestBackend};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(5);
const PROVIDER: &str = "gateway";
const DISPLAY_NAME: &str = "Gateway";
const RECENT: &str = "gateway/discovery-recent";
const CATALOG: &str = "gateway/discovery-catalog";
const FAILURE_STATUS: u16 = 503;
const FAILURE_MESSAGE: &str = "listing unavailable";

/// Discovery that waits for the test to release it, then fails.
struct SlowFailingBackend {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
}

impl ProviderBackend for SlowFailingBackend {
    fn prepare(
        &self,
        model: Model,
        _info: ModelInfo,
    ) -> BoxFuture<'static, Result<PreparedProvider, AgentError>> {
        Box::pin(async move {
            Ok(PreparedProvider {
                model,
                provider: provider::unconfigured(),
            })
        })
    }

    fn discover(
        &self,
        _refresh: bool,
    ) -> Option<BoxFuture<'static, Result<Vec<ModelInfo>, AgentError>>> {
        let (entered, release) = (self.entered.clone(), self.release.clone());
        Some(Box::pin(async move {
            entered.send_async(()).await.unwrap();
            let _ = release.recv_async().await;
            Err(AgentError::Api {
                status: FAILURE_STATUS,
                message: FAILURE_MESSAGE.into(),
                retry_after: None,
                overflow: false,
                retry: None,
            })
        }))
    }

    fn login(
        &self,
        _method: String,
        _config: Value,
        _ui: Arc<dyn InteractionService>,
    ) -> BoxFuture<'static, Result<Option<String>, AgentError>> {
        Box::pin(async { Ok(None) })
    }

    fn auth_status(
        &self,
        _config: Value,
    ) -> Option<BoxFuture<'static, Result<AuthStatus, AgentError>>> {
        None
    }

    fn logout(&self) -> Option<BoxFuture<'static, Result<(), AgentError>>> {
        None
    }
}

fn current_selection(spec: &str) -> CompleteModelSelection {
    CompleteModelSelection {
        spec: Arc::from(spec),
        thinking: ThinkingConfig::Off,
        fast: false,
    }
}

fn open_with_draft(event_loop: &mut EventLoop<'_>, current: &str, draft: ModelSelection) {
    event_loop.sessions[0].app.model_picker.open(
        current_selection(current),
        ModelCapabilities {
            thinking: ThinkingSupport::Yes,
            fast: true,
        },
        &draft,
    );
}

fn open(event_loop: &mut EventLoop<'_>, spec: &str) {
    open_with_draft(event_loop, spec, ModelSelection::default());
}

fn picker_text(event_loop: &mut EventLoop<'_>) -> String {
    let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
    terminal
        .draw(|frame| {
            event_loop.sessions[0]
                .app
                .model_picker
                .view(frame, frame.area());
        })
        .unwrap();
    terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}

fn assert_picker_usable(event_loop: &mut EventLoop<'_>) {
    open(event_loop, RECENT);
    let text = picker_text(event_loop);
    assert!(text.contains("Recent"), "{text}");
    assert!(text.contains(DISPLAY_NAME), "{text}");
    assert!(text.contains("discovery-recent"), "{text}");
    assert!(text.contains("discovery-catalog"), "{text}");
    open(event_loop, RECENT);
    assert!(matches!(
        event_loop.sessions[0]
            .app
            .model_picker
            .handle_key(key(KeyCode::Enter)),
        ModelPickerAction::Select(selection) if selection == ModelSelection::default()
    ));

    open_with_draft(
        event_loop,
        RECENT,
        ModelSelection {
            spec: Some(Arc::from(CATALOG)),
            ..ModelSelection::default()
        },
    );
    assert!(matches!(
        event_loop.sessions[0]
            .app
            .model_picker
            .handle_key(key(KeyCode::Enter)),
        ModelPickerAction::Select(selection) if selection.spec.as_deref() == Some(CATALOG)
    ));
}

#[test]
fn refresh_keeps_catalog_and_recents_through_slow_provider_failure() {
    let (entered_tx, entered_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let providers = Arc::new(ProviderRegistry::new(vec![RegisteredProvider {
        id: PROVIDER.into(),
        display_name: DISPLAY_NAME.into(),
        models: Vec::new(),
        backend: Arc::new(SlowFailingBackend {
            entered: entered_tx,
            release: release_rx,
        }),
        auth: AuthSpec::default(),
        docs: None,
    }]));

    with_event_loop_providers(providers, |event_loop| {
        let policy = Arc::new(ModelPolicy::new(&[RECENT.into(), CATALOG.into()], &[]).unwrap());
        event_loop.ctx.model_policy = Arc::clone(&policy);
        event_loop.sessions[0]
            .handles
            .orchestration_template
            .model_policy = Arc::clone(&policy);
        let catalog = Arc::new(vec![RECENT.into(), CATALOG.into()]);
        event_loop
            .ctx
            .catalog
            .specs()
            .store(Some(Arc::clone(&catalog)));
        event_loop.sync_orchestration();
        event_loop.sessions[0]
            .app
            .model_picker
            .set_recents(vec![RECENT.into()]);
        assert_picker_usable(event_loop);
        open(event_loop, RECENT);

        event_loop.handle_action(0, Action::RefreshModels);
        entered_rx.recv_timeout(TIMEOUT).unwrap();
        assert!(Arc::ptr_eq(
            &event_loop.ctx.catalog.specs().load_full().unwrap(),
            &catalog
        ));
        event_loop.sync_model_discovery();
        let text = picker_text(event_loop);
        assert!(text.contains("Recent"), "{text}");
        assert!(text.contains("discovery-recent"), "{text}");
        assert!(text.contains("discovery-catalog"), "{text}");
        assert!(matches!(
            event_loop.sessions[0]
                .app
                .model_picker
                .handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Select(selection) if selection == ModelSelection::default()
        ));
        assert_picker_usable(event_loop);
        open(event_loop, RECENT);

        event_loop.sessions[0]
            .handles
            .orchestration_template
            .model_policy = Arc::new(ModelPolicy::new(&[], &[RECENT.into()]).unwrap());
        event_loop.sync_orchestration();
        let text = picker_text(event_loop);
        assert!(text.contains("discovery-recent"), "{text}");
        assert!(!text.contains("Recent"), "{text}");
        assert!(text.contains("discovery-catalog"), "{text}");
        assert!(matches!(
            event_loop.sessions[0]
                .app
                .model_picker
                .handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Select(selection) if selection == ModelSelection::default()
        ));
        event_loop.sessions[0]
            .handles
            .orchestration_template
            .model_policy = policy;
        event_loop.sync_orchestration();
        assert_picker_usable(event_loop);

        release_tx.send(()).unwrap();
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let warning = event_loop
                .warn_rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            if warning.starts_with(&format!("{DISPLAY_NAME}:")) {
                assert!(warning.contains(FAILURE_MESSAGE), "{warning}");
                break;
            }
        }
        event_loop.sync_model_discovery();
        assert!(Arc::ptr_eq(
            &event_loop.ctx.catalog.specs().load_full().unwrap(),
            &catalog
        ));
        assert_picker_usable(event_loop);

        event_loop
            .ctx
            .catalog
            .specs()
            .store(Some(Arc::new(Vec::new())));
        event_loop.sync_model_discovery();
        open(event_loop, RECENT);
        let text = picker_text(event_loop);
        assert!(text.contains("discovery-recent"), "{text}");
        assert!(!text.contains("discovery-catalog"), "{text}");
        assert!(!text.contains("Recent"), "{text}");
        assert!(matches!(
            event_loop.sessions[0]
                .app
                .model_picker
                .handle_key(key(KeyCode::Enter)),
            ModelPickerAction::Select(selection) if selection == ModelSelection::default()
        ));
    });
}
