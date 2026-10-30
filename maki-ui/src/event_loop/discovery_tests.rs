use super::{EventLoop, tests::with_event_loop};
use crate::components::{Action, key, model_picker::ModelPickerAction};
use crossterm::event::KeyCode;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use maki_config::{
    ModelPolicy,
    providers::{ProviderDef, ProvidersConfig},
};
use maki_domain::{FastCapability, ModelCapabilities, ModelSelection, ThinkingCapability};
use maki_providers::{CompleteModelSelection, ThinkingConfig};
use maki_storage::paths;
use ratatui::{Terminal, backend::TestBackend};
use smol::Async;
use std::{
    net::TcpListener,
    sync::Arc,
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(5);
const RECENT: &str = "mistral/discovery-recent";
const CATALOG: &str = "mistral/discovery-catalog";
const FAILURE_RESPONSE: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

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
            resolved: true,
            thinking: ThinkingCapability::Supported,
            fast: FastCapability::Supported,
        },
        draft,
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
    assert!(text.contains("Mistral"), "{text}");
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
    let root = tempfile::tempdir().unwrap();
    paths::init_at(root.path().to_path_buf()).unwrap();
    std::fs::write(root.path().join("models-dev-catalog.json"), "{}").unwrap();
    let listener = Async::new(TcpListener::bind(("127.0.0.1", 0)).unwrap()).unwrap();
    let mut providers = ProvidersConfig::default();
    providers.upsert(
        "mistral".into(),
        ProviderDef {
            api_key: Some("discovery-test-key".into()),
            base_url: Some(format!(
                "http://{}",
                listener.get_ref().local_addr().unwrap()
            )),
            ..ProviderDef::default()
        },
    );
    providers.save().unwrap();
    let (entered_tx, entered_rx) = flume::bounded(1);
    let (release_tx, release_rx) = flume::bounded(1);
    let server = smol::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut byte = [0];
        while !request.ends_with(b"\r\n\r\n") {
            assert_eq!(stream.read(&mut byte).await.unwrap(), 1);
            request.push(byte[0]);
        }
        assert!(request.starts_with(b"GET /models HTTP/1.1\r\n"));
        entered_tx.send_async(()).await.unwrap();
        if release_rx.recv_async().await.is_ok() {
            stream.write_all(FAILURE_RESPONSE).await.unwrap();
            stream.flush().await.unwrap();
        }
    });

    with_event_loop(|event_loop| {
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
            if warning.starts_with("Mistral:") {
                assert!(warning.contains("503"), "{warning}");
                assert!(warning.contains("using static fallback"), "{warning}");
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
    smol::block_on(server);
}
