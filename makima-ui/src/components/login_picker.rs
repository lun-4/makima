use std::io;
use std::iter;
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use flume::{Receiver, Sender};
use ratatui::Frame;
use ratatui::layout::{Position, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Wrap;
use smol::Task;
use unicode_width::UnicodeWidthStr;

use makima_config::ModelPolicy;
use makima_providers::login::{
    ChoiceOption, InteractionRequest, InteractionService, LoginOutcome, LoginRequest, drive_login,
};
use makima_providers::provider::BoxFuture;
use makima_providers::{AgentError, ProviderRegistry};
use makima_storage::StateDir;

use crate::click::{ClickTarget, HitTarget, PickerId};
use crate::components::Overlay;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem, click_search_buffer};
use crate::components::modal::Modal;
use crate::repaint::Dirty;
use crate::text_buffer::TextBuffer;
use crate::theme;

const TITLE: &str = " Login ";
const WAITING_MESSAGE: &str = "Waiting... (Esc to cancel)";
const SECRET_MASK: &str = "*";
const NO_TERMINAL_ERR: &str =
    "this login runs a program on the terminal; run `makima auth login` in a shell";

struct ChoiceItem(ChoiceOption);

impl PickerItem for ChoiceItem {
    fn label(&self) -> &str {
        &self.0.label
    }

    fn section(&self) -> Option<&str> {
        self.0.section.as_deref()
    }

    fn detail(&self) -> Option<&str> {
        self.0.detail.as_deref()
    }
}

enum LoginEvent {
    Prompt(InteractionRequest, Sender<String>),
    Finished(Result<LoginOutcome, AgentError>),
}

/// Hands each request to the picker, which answers from the UI thread.
struct TuiInteraction(Sender<LoginEvent>);

impl InteractionService for TuiInteraction {
    fn interact(&self, request: InteractionRequest) -> BoxFuture<'_, Result<String, AgentError>> {
        if matches!(request, InteractionRequest::Terminal(_)) {
            return Box::pin(async {
                Err(AgentError::Config {
                    message: NO_TERMINAL_ERR.into(),
                })
            });
        }
        let (reply, answer) = flume::bounded(1);
        let sent = self.0.send(LoginEvent::Prompt(request, reply));
        Box::pin(async move {
            sent.map_err(|_| AgentError::Cancelled)?;
            answer.recv_async().await.map_err(|_| AgentError::Cancelled)
        })
    }
}

/// Dropping it cancels the login.
struct LoginTask {
    _task: Task<()>,
    events: Receiver<LoginEvent>,
}

enum Step {
    Closed,
    /// The login driver is busy off the UI thread.
    Waiting,
    Choice {
        picker: Box<ListPicker<ChoiceItem>>,
        reply: Sender<String>,
    },
    Input {
        input: TextBuffer,
        prompt: String,
        secret: bool,
        reply: Sender<String>,
    },
    Done {
        message: String,
    },
}

enum StepAction {
    None,
    Wait,
    Close,
}

type Browser = Box<dyn Fn(&str) -> io::Result<()> + Send>;

pub struct LoginPicker {
    step: Step,
    providers: Arc<ProviderRegistry>,
    storage: StateDir,
    model_policy: Arc<ModelPolicy>,
    login: Option<LoginTask>,
    /// Notices from the provider's login, shown until the picker closes.
    notes: Vec<String>,
    /// Opens a URL in the user's browser; tests swap in one that records it.
    browser: Browser,
    /// Where the last frame drew the text input, for caret clicks.
    input_area: Rect,
}

fn open_in_browser(url: &str) -> io::Result<()> {
    open::that_detached(url)
}

impl LoginPicker {
    fn open_browser(&self, url: &str) {
        if let Err(error) = (self.browser)(url) {
            tracing::warn!(%error, url, "failed to open browser");
        }
    }

    pub fn new(
        providers: Arc<ProviderRegistry>,
        storage: StateDir,
        model_policy: Arc<ModelPolicy>,
    ) -> Self {
        Self {
            step: Step::Closed,
            providers,
            storage,
            model_policy,
            login: None,
            notes: Vec::new(),
            browser: Box::new(open_in_browser),
            input_area: Rect::default(),
        }
    }

    /// Starts the shared login driver in the background; its questions
    /// arrive through `poll`.
    pub fn open(&mut self) {
        let (events_tx, events) = flume::unbounded();
        let providers = Arc::clone(&self.providers);
        let storage = self.storage.clone();
        let model_policy = Arc::clone(&self.model_policy);
        let request = LoginRequest { provider: None };
        let task = smol::spawn(async move {
            let ui = Arc::new(TuiInteraction(events_tx.clone()));
            let outcome = drive_login(&providers, ui, &storage, &model_policy, request).await;
            let _ = events_tx.send(LoginEvent::Finished(outcome));
        });
        self.login = Some(LoginTask {
            _task: task,
            events,
        });
        self.notes.clear();
        self.step = Step::Waiting;
    }

    pub fn poll(&mut self) -> (Dirty, Option<LoginPickerAction>) {
        let mut dirty = Dirty::NO;
        let mut action = None;
        while let Some(event) = self.login.as_ref().and_then(|l| l.events.try_recv().ok()) {
            dirty = Dirty::YES;
            action = self.handle_event(event).or(action);
        }
        (dirty, action)
    }

    fn handle_event(&mut self, event: LoginEvent) -> Option<LoginPickerAction> {
        match event {
            LoginEvent::Prompt(request, reply) => {
                let note = match request {
                    InteractionRequest::Input {
                        prompt,
                        secret,
                        default,
                    } => {
                        self.step = Step::Input {
                            input: TextBuffer::new(default.unwrap_or_default()),
                            prompt,
                            secret,
                            reply,
                        };
                        return None;
                    }
                    InteractionRequest::Choice { prompt, options } => {
                        let mut picker = ListPicker::new().with_picker(PickerId::Login);
                        if !self.notes.is_empty() {
                            picker = picker.with_footer_line(Line::from(self.notes.join(" · ")));
                        }
                        picker.open(
                            options.into_iter().map(ChoiceItem).collect(),
                            format!(" {prompt} "),
                        );
                        self.step = Step::Choice {
                            picker: Box::new(picker),
                            reply,
                        };
                        return None;
                    }
                    InteractionRequest::Instructions(text) => text,
                    InteractionRequest::DeviceCode { url, code } => {
                        format!("Open {url} and enter code {code}")
                    }
                    InteractionRequest::OpenBrowser(url) => {
                        self.open_browser(&url);
                        format!("Opening {url}")
                    }
                    InteractionRequest::Terminal(_) => unreachable!("answered by TuiInteraction"),
                };
                self.notes.push(note);
                let _ = reply.send(String::new());
                None
            }
            LoginEvent::Finished(Ok(LoginOutcome {
                provider,
                display_name,
                model,
            })) => {
                self.step = Step::Done {
                    message: format!("Authenticated: {display_name}"),
                };
                Some(match model {
                    Some(model_spec) => LoginPickerAction::Authenticated { model_spec },
                    None => LoginPickerAction::Configured {
                        slug: provider.to_string(),
                    },
                })
            }
            LoginEvent::Finished(Err(AgentError::Cancelled)) => {
                Some(self.transition(StepAction::Close))
            }
            LoginEvent::Finished(Err(error)) => {
                self.step = Step::Done {
                    message: format!("Error: {error}"),
                };
                Some(LoginPickerAction::Consumed)
            }
        }
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        match &mut self.step {
            Step::Input { input, .. } => {
                input.insert_text(text);
                true
            }
            _ => false,
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> LoginPickerAction {
        let action = match &mut self.step {
            Step::Closed => return LoginPickerAction::Consumed,
            Step::Waiting => match key.code {
                KeyCode::Esc => StepAction::Close,
                _ => return LoginPickerAction::Consumed,
            },
            Step::Choice { picker, reply } => match picker.handle_key(key) {
                PickerAction::Select(item) => {
                    let _ = reply.send(item.0.id);
                    StepAction::Wait
                }
                PickerAction::Close => StepAction::Close,
                PickerAction::Consumed | PickerAction::Toggle(..) | PickerAction::Delete(..) => {
                    return LoginPickerAction::Consumed;
                }
            },
            Step::Input { input, reply, .. } => match key.code {
                KeyCode::Enter => {
                    let _ = reply.send(input.value().trim().to_owned());
                    StepAction::Wait
                }
                KeyCode::Esc => StepAction::Close,
                _ => {
                    input.handle_key(key);
                    return LoginPickerAction::Consumed;
                }
            },
            Step::Done { .. } => {
                if matches!(key.code, KeyCode::Enter | KeyCode::Esc) {
                    StepAction::Close
                } else {
                    StepAction::None
                }
            }
        };

        self.transition(action)
    }

    fn transition(&mut self, action: StepAction) -> LoginPickerAction {
        match action {
            StepAction::None => LoginPickerAction::Consumed,
            StepAction::Wait => {
                self.step = Step::Waiting;
                LoginPickerAction::Consumed
            }
            StepAction::Close => {
                self.close();
                LoginPickerAction::Close
            }
        }
    }

    fn choice_picker(&self) -> Option<&ListPicker<ChoiceItem>> {
        match &self.step {
            Step::Choice { picker, .. } => Some(picker),
            _ => None,
        }
    }

    fn choice_picker_mut(&mut self) -> Option<&mut ListPicker<ChoiceItem>> {
        match &mut self.step {
            Step::Choice { picker, .. } => Some(picker),
            _ => None,
        }
    }

    pub fn scroll(&mut self, delta: i32) {
        if let Some(picker) = self.choice_picker_mut() {
            picker.scroll(delta);
        }
    }

    pub fn click_input(&mut self, pos: Position) -> bool {
        let area = self.input_area;
        match &mut self.step {
            Step::Choice { picker, .. } => picker.click_input(pos),
            Step::Input { input, .. } => {
                click_search_buffer(input, area, pos, super::CHEVRON.width() as u16)
            }
            _ => false,
        }
    }

    pub fn click_row(&mut self, index: usize) -> bool {
        self.choice_picker_mut()
            .is_some_and(|picker| picker.select(index))
    }

    pub fn hits(&self) -> Vec<HitTarget> {
        match &self.step {
            Step::Choice { picker, .. } => picker.hits(),
            Step::Input { .. } => vec![HitTarget {
                rect: self.input_area,
                target: ClickTarget::PickerInput(PickerId::Login),
            }],
            _ => Vec::new(),
        }
    }

    pub fn scroll_line(&self) -> Option<u16> {
        self.choice_picker().map(ListPicker::scroll_line)
    }

    pub fn content_len(&self) -> Option<u16> {
        self.choice_picker().and_then(ListPicker::content_len)
    }

    pub fn scroll_to_line(&mut self, line: u16) {
        if let Some(picker) = self.choice_picker_mut() {
            picker.scroll_to_line(line);
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        self.input_area = Rect::default();
        match &mut self.step {
            Step::Closed => Rect::default(),
            Step::Choice { picker, .. } => picker.view(frame, area),
            Step::Waiting => {
                let lines = note_lines(&self.notes)
                    .chain(iter::once(placeholder_line(WAITING_MESSAGE)))
                    .collect();
                notes_modal(frame, area, lines).0
            }
            Step::Input {
                input,
                prompt,
                secret,
                ..
            } => {
                let above: Vec<_> = note_lines(&self.notes)
                    .chain(iter::once(placeholder_line(prompt)))
                    .collect();
                let lines = above
                    .iter()
                    .cloned()
                    .chain(iter::once(input_line_with_cursor(input, *secret)))
                    .collect();
                let (popup, inner) = notes_modal(frame, area, lines);
                let row = crate::wrap::total_rows(&above, inner.width);
                if row < inner.height {
                    self.input_area = Rect::new(inner.x, inner.y + row, inner.width, 1);
                }
                popup
            }
            Step::Done { message } => {
                let lines = iter::once(Line::from(message.clone()))
                    .chain(note_lines(&self.notes))
                    .collect();
                notes_modal(frame, area, lines).0
            }
        }
    }
}

impl Overlay for LoginPicker {
    fn is_open(&self) -> bool {
        !matches!(self.step, Step::Closed)
    }

    fn close(&mut self) {
        self.login = None;
        self.step = Step::Closed;
    }
}

fn note_lines(notes: &[String]) -> impl Iterator<Item = Line<'static>> + '_ {
    notes.iter().map(|note| Line::from(note.clone()))
}

fn placeholder_line(text: &str) -> Line<'static> {
    Line::from(Span::styled(
        text.to_owned(),
        theme::current().input_placeholder,
    ))
}

/// Returns the popup and the area its lines are drawn in.
fn notes_modal(frame: &mut Frame, area: Rect, lines: Vec<Line<'static>>) -> (Rect, Rect) {
    let modal = Modal {
        title: TITLE,
        width_percent: 65,
        max_height_percent: 45,
    };
    let height = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let (popup, inner) = modal.render(frame, area, height);
    frame.render_widget(
        ratatui::widgets::Paragraph::new(lines)
            .style(Style::new().bg(theme::current().background))
            .wrap(Wrap { trim: true }),
        inner,
    );
    (popup, inner)
}

fn input_line_with_cursor(input: &TextBuffer, secret: bool) -> Line<'static> {
    let t = theme::current();
    let value = match secret {
        true => SECRET_MASK.repeat(input.value().chars().count()),
        false => input.value(),
    };
    let cursor_byte = TextBuffer::char_to_byte(&value, input.x());
    let (before, rest) = value.split_at(cursor_byte);
    let mut chars = rest.chars();
    let cursor_char = chars.next().unwrap_or(' ');
    let after = chars.as_str();
    Line::from(vec![
        super::chevron_span(),
        Span::raw(before.to_string()),
        Span::styled(cursor_char.to_string(), t.cursor),
        Span::raw(after.to_string()),
    ])
}

pub enum LoginPickerAction {
    Consumed,
    Close,
    Authenticated { model_spec: String },
    Configured { slug: String },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crossterm::event::KeyModifiers;
    use makima_agent::tools::ToolRegistry;
    use makima_config::providers::ProvidersConfig;
    use makima_lua::PluginHost;
    use makima_lua::test_support::InMemoryFs;
    use makima_lua::ui_sink::HostUi;
    use makima_storage::auth::load_credentials;
    use makima_storage::model::read_model;
    use serde_json::json;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;
    use crate::components::key;

    const PROVIDER: &str = "deepseek";
    const DISPLAY_NAME: &str = "Fake DeepSeek";
    const DEFAULT_URL: &str = "http://localhost:8080";
    const ANSWERED_URL: &str = "http://example.test";
    const URL_PROMPT: &str = "Host URL";
    const KEY_PROMPT: &str = "API key";
    const KEY: &str = "sk-from-tui";
    const MODEL_SPEC: &str = "deepseek/fake-2";
    const BOUND: Duration = Duration::from_secs(10);
    const BROWSER_PROVIDER: &str = "browser-login";
    const BROWSER_URL: &str = "https://keys.example.test/new";
    const BROWSER_KEY: &str = "sk-from-browser";

    const SOURCE: &str = r#"
        makima.api.register_provider({
            id = "deepseek",
            display_name = "Fake DeepSeek",
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
            },
        })
        makima.api.register_provider({
            id = "browser-login",
            display_name = "Browser Login",
            prepare = function() return {} end,
            auth = {
                { id = "browser", label = "Browser", login = function(ctx, ui)
                    assert(ui:open_browser("https://keys.example.test/new"))
                    assert(ctx.credentials:write({ api_key = "sk-from-browser" }))
                    return true
                end },
            },
        })
    "#;

    struct Fixture {
        _host: PluginHost,
        picker: LoginPicker,
        storage: StateDir,
        opened: Receiver<String>,
        _state: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let state = TempDir::new().unwrap();
            makima_storage::paths::init_shared_for_test();
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
            host.load_source("fake_plugin", SOURCE).unwrap();
            let registry = host
                .provider_registry(makima_providers::Timeouts::default())
                .unwrap();
            let storage = StateDir::from_path(state.path().to_path_buf());
            let (opened_tx, opened) = flume::unbounded();
            Self {
                picker: LoginPicker {
                    browser: Box::new(move |url| {
                        let _ = opened_tx.send(url.to_owned());
                        Ok(())
                    }),
                    ..LoginPicker::new(registry, storage.clone(), Arc::new(ModelPolicy::default()))
                },
                _host: host,
                storage,
                opened,
                _state: state,
            }
        }

        /// Handles the login's next event, waiting a bounded time for it.
        fn next_event(&mut self) -> Option<LoginPickerAction> {
            let login = self.picker.login.as_ref().expect("login running");
            let event = login.events.recv_timeout(BOUND).expect("login event");
            self.picker.handle_event(event)
        }

        fn choose(&mut self, id: &str) {
            let Step::Choice { picker, .. } = &mut self.picker.step else {
                panic!("expected a choice");
            };
            assert!(picker.select_item_by(|item| item.0.id == id));
            self.picker.handle_key(key(KeyCode::Enter));
        }

        fn input(&self) -> (String, String, bool) {
            match &self.picker.step {
                Step::Input {
                    input,
                    prompt,
                    secret,
                    ..
                } => (prompt.clone(), input.value(), *secret),
                _ => panic!("expected an input step"),
            }
        }
    }

    #[test_case(0, 0; "prefix")]
    #[test_case(3, 1; "wide_character")]
    #[test_case(20, 3; "past_end")]
    fn text_entry_click_moves_caret(column: u16, expected: usize) {
        let state = TempDir::new().unwrap();
        let mut picker = LoginPicker::new(
            Arc::new(ProviderRegistry::default()),
            StateDir::from_path(state.path().to_path_buf()),
            Arc::new(ModelPolicy::default()),
        );
        picker.step = Step::Input {
            input: TextBuffer::new("a界z".into()),
            prompt: KEY_PROMPT.into(),
            secret: false,
            reply: flume::unbounded().0,
        };
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        let rect = picker
            .hits()
            .into_iter()
            .find(|hit| hit.target == ClickTarget::PickerInput(PickerId::Login))
            .unwrap()
            .rect;
        assert!(picker.click_input(Position::new(rect.x + column, rect.y)));
        assert!(!picker.click_input(Position::new(rect.x, rect.y - 1)));
        let Step::Input { input, .. } = &picker.step else {
            unreachable!()
        };
        assert_eq!(input.x(), expected);
        picker.close();
        assert!(!picker.click_input(Position::new(rect.x, rect.y)));
        assert!(picker.hits().is_empty());
    }

    fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    /// The same outcome as the driver's own test: only the frontend differs.
    #[test]
    fn tui_login_matches_driver_outcome() {
        let mut fixture = Fixture::new();
        fixture.picker.open();

        assert!(fixture.next_event().is_none());
        let Step::Choice { picker, .. } = &mut fixture.picker.step else {
            panic!("expected the provider choice");
        };
        assert!(picker.select_item_by(|item| item.0.id == PROVIDER));
        fixture.picker.handle_key(key(KeyCode::Enter));

        assert!(fixture.next_event().is_none());
        assert_eq!(
            fixture.input(),
            (URL_PROMPT.into(), DEFAULT_URL.into(), false)
        );
        fixture.picker.handle_key(ctrl('a'));
        fixture.picker.handle_key(ctrl('k'));
        fixture.picker.handle_paste(ANSWERED_URL);
        fixture.picker.handle_key(key(KeyCode::Enter));

        assert!(fixture.next_event().is_none());
        assert_eq!(fixture.input(), (KEY_PROMPT.into(), String::new(), true));
        fixture.picker.handle_paste(KEY);
        fixture.picker.handle_key(key(KeyCode::Enter));

        let action = fixture.next_event();
        assert!(matches!(
            action,
            Some(LoginPickerAction::Authenticated { model_spec }) if model_spec == MODEL_SPEC
        ));
        assert!(
            matches!(&fixture.picker.step, Step::Done { message } if message.contains(DISPLAY_NAME))
        );
        assert_eq!(
            load_credentials(&fixture.storage, PROVIDER),
            Some(json!({ "api_key": KEY, "host": ANSWERED_URL }))
        );
        let section = ProvidersConfig::try_load().unwrap();
        assert_eq!(
            section.get(PROVIDER).unwrap().0["base_url"].as_str(),
            Some(ANSWERED_URL)
        );
        assert_eq!(read_model(&fixture.storage).as_deref(), Some(MODEL_SPEC));
    }

    #[test]
    fn login_opens_the_url_the_provider_named() {
        let mut fixture = Fixture::new();
        fixture.picker.open();
        assert!(fixture.next_event().is_none());
        fixture.choose(BROWSER_PROVIDER);

        assert!(fixture.next_event().is_none());
        assert_eq!(fixture.opened.try_recv().as_deref(), Ok(BROWSER_URL));
        assert!(
            fixture
                .picker
                .notes
                .iter()
                .any(|note| note.contains(BROWSER_URL))
        );
        assert!(matches!(
            fixture.next_event(),
            Some(LoginPickerAction::Configured { slug }) if slug == BROWSER_PROVIDER
        ));
        assert_eq!(
            load_credentials(&fixture.storage, BROWSER_PROVIDER),
            Some(json!({ "api_key": BROWSER_KEY }))
        );
    }

    #[test]
    fn escape_on_prompt_cancels_login() {
        let mut fixture = Fixture::new();
        fixture.picker.open();
        assert!(fixture.next_event().is_none());
        let Step::Choice { picker, .. } = &mut fixture.picker.step else {
            panic!("expected the provider choice");
        };
        assert!(picker.select_item_by(|item| item.0.id == PROVIDER));
        fixture.picker.handle_key(key(KeyCode::Enter));
        assert!(fixture.next_event().is_none());

        assert!(matches!(
            fixture.picker.handle_key(key(KeyCode::Esc)),
            LoginPickerAction::Close
        ));
        assert!(!fixture.picker.is_open());
        assert!(load_credentials(&fixture.storage, PROVIDER).is_none());
    }
}
