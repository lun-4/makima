use std::env;
use std::io;
use std::iter;
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use flume::{Receiver, Sender};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Wrap;
use smol::Task;

use maki_config::providers::{self, Protocol, ProviderDef, ProvidersConfig, slugify};
use maki_providers::login::{
    AuthState, ChoiceOption, InteractionRequest, InteractionService, LoginOutcome, LoginRequest,
    drive_login,
};
use maki_providers::provider::BoxFuture;
use maki_providers::{AgentError, ProviderRegistry};
use maki_storage::StateDir;
use maki_storage::auth::{
    ProviderCredentials, load_provider_credentials, save_provider_credentials,
};
use maki_storage::model::persist_model;

use crate::components::Overlay;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem};
use crate::components::modal::Modal;
use crate::repaint::Dirty;
use crate::text_buffer::TextBuffer;
use crate::theme;

const TITLE: &str = " Login ";
pub const CUSTOM_SLUG: &str = "custom";
const CUSTOM_LABEL: &str = "Custom provider...";
const WAITING_MESSAGE: &str = "Waiting... (Esc to cancel)";
const SECRET_MASK: &str = "*";

const PROTOCOLS: &[(&str, &str)] = &[
    ("openai", "OpenAI-compatible"),
    ("anthropic", "Anthropic"),
    ("google", "Google Gemini"),
];

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

struct PlanItem {
    key: String,
    display_name: String,
    base_url: String,
}

impl PickerItem for PlanItem {
    fn label(&self) -> &str {
        &self.display_name
    }

    fn detail(&self) -> Option<&str> {
        Some(&self.base_url)
    }
}

struct ProtocolItem(&'static str, &'static str);

impl PickerItem for ProtocolItem {
    fn label(&self) -> &str {
        self.1
    }
}

struct CustomInfo {
    base_url: String,
    protocol: String,
}

enum Step {
    Closed,
    /// The login driver is busy off the UI thread.
    Waiting,
    Choice {
        picker: ListPicker<ChoiceItem>,
        reply: Sender<String>,
    },
    Input {
        input: TextBuffer,
        prompt: String,
        secret: bool,
        reply: Sender<String>,
    },
    PickPlan {
        picker: ListPicker<PlanItem>,
        slug: String,
    },
    CustomName {
        input: TextBuffer,
    },
    CustomProtocol {
        picker: ListPicker<ProtocolItem>,
        slug: String,
    },
    CustomUrl {
        input: TextBuffer,
        slug: String,
        protocol: String,
    },
    BuiltinUrl {
        input: TextBuffer,
        slug: String,
        display_name: String,
    },
    EnterKey {
        input: TextBuffer,
        slug: String,
        plan: Option<String>,
        display_name: String,
        custom: Option<CustomInfo>,
        builtin_url: Option<String>,
        api_key_optional: bool,
    },
    Done {
        message: String,
    },
}

enum StepAction {
    None,
    Wait,
    GoEnterKey {
        slug: String,
        plan: Option<String>,
        display_name: String,
        custom: Option<CustomInfo>,
        builtin_url: Option<String>,
        api_key_optional: bool,
    },
    GoPickPlan {
        slug: String,
    },
    GoCustomName,
    GoCustomProtocol {
        slug: String,
    },
    GoCustomUrl {
        slug: String,
        protocol: String,
    },
    GoBuiltinUrl {
        slug: String,
        display_name: String,
    },
    GoDone {
        message: String,
        model_spec: Option<String>,
        slug: String,
    },
    Back,
    Close,
}

pub struct LoginPicker {
    step: Step,
    providers: Arc<ProviderRegistry>,
    storage: StateDir,
    /// The native rows of the current login, which name native steps.
    native: Vec<ChoiceOption>,
    login: Option<LoginTask>,
    /// Notices from the provider's login, shown until the picker closes.
    notes: Vec<String>,
    /// Opens a URL in the user's browser; tests swap in one that does not.
    browser: fn(&str) -> io::Result<()>,
}

/// Native providers a frontend logs in to itself, skipping any the registry
/// now owns. Goes away with the native providers in commit 9.
pub fn native_login_choices(storage: &StateDir, registry: &ProviderRegistry) -> Vec<ChoiceOption> {
    let config = ProvidersConfig::load();
    let builtins = providers::all_builtins().into_iter().map(|b| {
        let has_key = load_provider_credentials(storage, b.slug).is_some();
        let has_env = env::var(b.default_api_key_env).is_ok();
        let configured = config.get(b.slug).is_some_and(|d| d.base_url.is_some());
        native_choice(
            b.slug,
            b.display_name,
            found_state(has_key, has_env, configured),
        )
    });
    let custom = config
        .providers
        .iter()
        .filter(|(slug, _)| providers::builtin_provider(slug).is_none())
        .map(|(slug, def)| {
            let has_key = load_provider_credentials(storage, slug).is_some();
            let has_env = def
                .api_key_env
                .as_deref()
                .is_some_and(|e| env::var(e).is_ok());
            let label = def.display_name.as_deref().unwrap_or(slug);
            native_choice(slug, label, found_state(has_key, has_env, false))
        });
    let custom_choice = native_choice(CUSTOM_SLUG, CUSTOM_LABEL, AuthState::Missing);
    builtins
        .chain(custom)
        .chain(iter::once(custom_choice))
        .filter(|choice| !registry.contains(&choice.id))
        .collect()
}

fn found_state(has_key: bool, has_env: bool, configured: bool) -> AuthState {
    match () {
        _ if has_key => AuthState::Saved,
        _ if has_env => AuthState::Env,
        _ if configured => AuthState::Configured,
        _ => AuthState::Missing,
    }
}

fn native_choice(id: &str, label: &str, state: AuthState) -> ChoiceOption {
    ChoiceOption {
        id: id.into(),
        label: label.into(),
        detail: state.label().map(str::to_owned),
        section: None,
    }
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

    pub fn new(providers: Arc<ProviderRegistry>, storage: StateDir) -> Self {
        Self {
            step: Step::Closed,
            providers,
            storage,
            native: Vec::new(),
            login: None,
            notes: Vec::new(),
            browser: open_in_browser,
        }
    }

    /// Starts the shared login driver in the background; its questions
    /// arrive through `poll`.
    pub fn open(&mut self) {
        self.native = native_login_choices(&self.storage, &self.providers);
        let (events_tx, events) = flume::unbounded();
        let providers = Arc::clone(&self.providers);
        let storage = self.storage.clone();
        let request = LoginRequest {
            provider: None,
            native: self.native.clone(),
        };
        let task = smol::spawn(async move {
            let ui = Arc::new(TuiInteraction(events_tx.clone()));
            let outcome = drive_login(&providers, ui, &storage, request).await;
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
                        let mut picker = ListPicker::new();
                        picker.open(
                            options.into_iter().map(ChoiceItem).collect(),
                            format!(" {prompt} "),
                        );
                        self.step = Step::Choice { picker, reply };
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
                };
                self.notes.push(note);
                let _ = reply.send(String::new());
                None
            }
            LoginEvent::Finished(Ok(LoginOutcome::LoggedIn {
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
            LoginEvent::Finished(Ok(LoginOutcome::Native(slug))) => Some(self.select_native(slug)),
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

    fn select_native(&mut self, slug: String) -> LoginPickerAction {
        let action = match slug.as_str() {
            CUSTOM_SLUG => StepAction::GoCustomName,
            _ => {
                let config = providers::ProvidersConfig::load();
                let def = config.get(&slug);
                let has_plans = providers::builtin_provider(&slug)
                    .and_then(|b| b.plans)
                    .is_some_and(|p| p.len() > 1);
                if has_plans {
                    StepAction::GoPickPlan { slug }
                } else {
                    let display_name =
                        if providers::builtin_provider(&slug).is_some() || def.is_some() {
                            providers::resolve_display_name(&slug, def)
                        } else {
                            self.native
                                .iter()
                                .find(|choice| choice.id == slug)
                                .map_or_else(|| slug.clone(), |choice| choice.label.clone())
                        };
                    let needs_url = providers::builtin_provider(&slug).is_some_and(|b| b.needs_url);
                    if needs_url {
                        StepAction::GoBuiltinUrl { slug, display_name }
                    } else {
                        StepAction::GoEnterKey {
                            slug,
                            plan: None,
                            display_name,
                            custom: None,
                            builtin_url: None,
                            api_key_optional: false,
                        }
                    }
                }
            }
        };
        self.transition(action)
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        match &mut self.step {
            Step::Input { input, .. }
            | Step::EnterKey { input, .. }
            | Step::CustomName { input }
            | Step::CustomUrl { input, .. }
            | Step::BuiltinUrl { input, .. } => {
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
                PickerAction::Close => StepAction::Wait,
                PickerAction::Consumed | PickerAction::Toggle(..) | PickerAction::Delete(..) => {
                    return LoginPickerAction::Consumed;
                }
            },
            Step::Input { input, reply, .. } => match key.code {
                KeyCode::Enter => {
                    let _ = reply.send(input.value().trim().to_owned());
                    StepAction::Wait
                }
                KeyCode::Esc => StepAction::Wait,
                _ => {
                    input.handle_key(key);
                    return LoginPickerAction::Consumed;
                }
            },
            Step::PickPlan { picker, slug } => match picker.handle_key(key) {
                PickerAction::Select(item) => {
                    let config = providers::ProvidersConfig::load();
                    StepAction::GoEnterKey {
                        slug: slug.clone(),
                        plan: Some(item.key.clone()),
                        display_name: providers::resolve_display_name(slug, config.get(slug)),
                        custom: None,
                        builtin_url: None,
                        api_key_optional: false,
                    }
                }
                PickerAction::Close => StepAction::Back,
                PickerAction::Consumed | PickerAction::Toggle(..) | PickerAction::Delete(..) => {
                    return LoginPickerAction::Consumed;
                }
            },
            Step::CustomName { input } => match key.code {
                KeyCode::Enter => {
                    let name = input.value().trim().to_string();
                    let slug = slugify(&name);
                    if slug.is_empty() {
                        return LoginPickerAction::Consumed;
                    }
                    StepAction::GoCustomProtocol { slug }
                }
                KeyCode::Esc => StepAction::Back,
                _ => {
                    input.handle_key(key);
                    return LoginPickerAction::Consumed;
                }
            },
            Step::CustomProtocol { picker, slug } => match picker.handle_key(key) {
                PickerAction::Select(item) => StepAction::GoCustomUrl {
                    slug: slug.clone(),
                    protocol: item.0.to_string(),
                },
                PickerAction::Close => StepAction::Back,
                PickerAction::Consumed | PickerAction::Toggle(..) | PickerAction::Delete(..) => {
                    return LoginPickerAction::Consumed;
                }
            },
            Step::CustomUrl {
                input,
                slug,
                protocol,
            } => match key.code {
                KeyCode::Enter => {
                    let base_url = input.value().trim().to_string();
                    if base_url.is_empty() {
                        return LoginPickerAction::Consumed;
                    }
                    StepAction::GoEnterKey {
                        slug: slug.clone(),
                        plan: None,
                        display_name: format!("Custom ({})", slug),
                        custom: Some(CustomInfo {
                            base_url,
                            protocol: protocol.clone(),
                        }),
                        builtin_url: None,
                        api_key_optional: false,
                    }
                }
                KeyCode::Esc => StepAction::Back,
                _ => {
                    input.handle_key(key);
                    return LoginPickerAction::Consumed;
                }
            },
            Step::EnterKey {
                input,
                slug,
                plan,
                display_name,
                custom,
                builtin_url,
                api_key_optional,
            } => match key.code {
                KeyCode::Enter => {
                    let api_key = input.value().trim().to_string();
                    if api_key.is_empty() && !*api_key_optional {
                        return LoginPickerAction::Consumed;
                    }
                    let slug_c = slug.clone();
                    let plan_c = plan.clone();
                    let dn_c = display_name.clone();
                    let storage = self.storage.clone();
                    let custom_c = custom.take();
                    let builtin_url_c = builtin_url.take();

                    let has_key = !api_key.is_empty();
                    if has_key {
                        let creds = ProviderCredentials {
                            api_key,
                            host: None,
                        };
                        if let Err(e) = save_provider_credentials(&storage, &slug_c, &creds) {
                            return self.transition(StepAction::GoDone {
                                message: format!("Error: {e}"),
                                model_spec: None,
                                slug: slug_c.clone(),
                            });
                        }
                    }

                    let mut config = ProvidersConfig::load();

                    if let Some(info) = &custom_c {
                        let api_key_env =
                            format!("{}_API_KEY", slug_c.to_uppercase().replace('-', "_"));
                        let provider_def = ProviderDef {
                            display_name: Some(dn_c.clone()),
                            protocol: Some(
                                info.protocol
                                    .parse::<Protocol>()
                                    .unwrap_or(Protocol::Openai),
                            ),
                            base_url: Some(info.base_url.clone()),
                            api_key_env: Some(api_key_env),
                            discover_models: true,
                            ..Default::default()
                        };
                        config.upsert(slug_c.clone(), provider_def);
                        if let Err(e) = config.save() {
                            return self.transition(StepAction::GoDone {
                                message: format!("Error saving config: {e}"),
                                model_spec: None,
                                slug: slug_c.clone(),
                            });
                        }
                    } else {
                        let needs_url =
                            providers::builtin_provider(&slug_c).is_some_and(|b| b.needs_url);
                        if plan_c.is_some() || builtin_url_c.is_some() || needs_url {
                            let mut def = config.get(&slug_c).cloned().unwrap_or_default();
                            def.plan = plan_c.clone();
                            if let Some(url) = builtin_url_c {
                                def.base_url = Some(url);
                            }
                            config.upsert(slug_c.clone(), def);
                            if let Err(e) = config.save() {
                                return self.transition(StepAction::GoDone {
                                    message: format!("Error saving config: {e}"),
                                    model_spec: None,
                                    slug: slug_c.clone(),
                                });
                            }
                        }
                    }

                    let needs_url =
                        providers::builtin_provider(&slug_c).is_some_and(|b| b.needs_url);
                    let default_model = if needs_url {
                        None
                    } else {
                        providers::resolve_default_model(&slug_c, config.get(&slug_c))
                    };
                    if let Some(model) = &default_model {
                        persist_model(&storage, model);
                    }

                    let verb = if has_key {
                        "Authenticated"
                    } else {
                        "Configured"
                    };
                    let msg = format!(
                        "{}: {}{}",
                        verb,
                        dn_c,
                        plan_c
                            .as_deref()
                            .map(|p| format!(" ({p})"))
                            .unwrap_or_default()
                    );
                    StepAction::GoDone {
                        message: msg,
                        model_spec: default_model,
                        slug: slug_c.clone(),
                    }
                }
                KeyCode::Esc => StepAction::Back,
                _ => {
                    input.handle_key(key);
                    return LoginPickerAction::Consumed;
                }
            },
            Step::BuiltinUrl {
                input,
                slug,
                display_name,
            } => match key.code {
                KeyCode::Enter => {
                    let mut base_url = input.value().trim().to_string();
                    if base_url.is_empty() {
                        base_url = providers::resolve_base_url(slug, None).unwrap_or_default();
                    }
                    StepAction::GoEnterKey {
                        slug: slug.clone(),
                        plan: None,
                        display_name: display_name.clone(),
                        custom: None,
                        builtin_url: Some(base_url),
                        api_key_optional: true,
                    }
                }
                KeyCode::Esc => StepAction::Back,
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
            StepAction::GoEnterKey {
                slug,
                plan,
                display_name,
                custom,
                builtin_url,
                api_key_optional,
            } => {
                if custom.is_none()
                    && let Some(url) = providers::resolve_login_url(&slug, plan.as_deref())
                {
                    self.open_browser(&url);
                }
                self.step = Step::EnterKey {
                    input: TextBuffer::new(String::new()),
                    slug,
                    plan,
                    display_name,
                    custom,
                    builtin_url,
                    api_key_optional,
                };
                LoginPickerAction::Consumed
            }
            StepAction::GoBuiltinUrl { slug, display_name } => {
                let config = providers::ProvidersConfig::load();
                let default =
                    providers::resolve_base_url(&slug, config.get(&slug)).unwrap_or_default();
                self.step = Step::BuiltinUrl {
                    input: TextBuffer::new(default),
                    slug,
                    display_name,
                };
                LoginPickerAction::Consumed
            }
            StepAction::GoPickPlan { slug } => {
                let builtin = providers::builtin_provider(&slug);
                let plans = builtin.and_then(|b| b.plans).unwrap_or(&[]);
                let plan_items: Vec<PlanItem> = plans
                    .iter()
                    .map(|(key, plan)| PlanItem {
                        key: key.to_string(),
                        display_name: plan.display_name.to_string(),
                        base_url: plan.base_url.to_string(),
                    })
                    .collect();
                let display = providers::resolve_display_name(&slug, None);
                let mut plan_picker = ListPicker::new();
                plan_picker.open(plan_items, format!(" {display} plan "));
                self.step = Step::PickPlan {
                    picker: plan_picker,
                    slug,
                };
                LoginPickerAction::Consumed
            }
            StepAction::GoCustomName => {
                self.step = Step::CustomName {
                    input: TextBuffer::new(String::new()),
                };
                LoginPickerAction::Consumed
            }
            StepAction::GoCustomProtocol { slug } => {
                let items: Vec<ProtocolItem> = PROTOCOLS
                    .iter()
                    .map(|(name, display)| ProtocolItem(name, display))
                    .collect();
                let mut picker = ListPicker::new();
                picker.open(items, " Protocol ");
                self.step = Step::CustomProtocol { picker, slug };
                LoginPickerAction::Consumed
            }
            StepAction::GoCustomUrl { slug, protocol } => {
                self.step = Step::CustomUrl {
                    input: TextBuffer::new(String::new()),
                    slug,
                    protocol,
                };
                LoginPickerAction::Consumed
            }
            StepAction::GoDone {
                message,
                model_spec,
                slug,
            } => {
                self.step = Step::Done { message };
                if let Some(spec) = model_spec {
                    LoginPickerAction::Authenticated { model_spec: spec }
                } else {
                    LoginPickerAction::Configured { slug }
                }
            }
            StepAction::Wait => {
                self.step = Step::Waiting;
                LoginPickerAction::Consumed
            }
            StepAction::Back => {
                self.open();
                LoginPickerAction::Consumed
            }
            StepAction::Close => {
                self.close();
                LoginPickerAction::Close
            }
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        match &mut self.step {
            Step::Closed => Rect::default(),
            Step::Choice { picker, .. } => picker.view(frame, area),
            Step::Waiting => {
                let lines = note_lines(&self.notes)
                    .chain(iter::once(placeholder_line(WAITING_MESSAGE)))
                    .collect();
                notes_modal(frame, area, lines)
            }
            Step::Input {
                input,
                prompt,
                secret,
                ..
            } => {
                let lines = note_lines(&self.notes)
                    .chain([
                        placeholder_line(prompt),
                        input_line_with_cursor(input, *secret),
                    ])
                    .collect();
                notes_modal(frame, area, lines)
            }
            Step::PickPlan { picker, .. } => picker.view(frame, area),
            Step::CustomName { input } => {
                let modal = Modal {
                    title: " Provider name ",
                    width_percent: 65,
                    max_height_percent: 40,
                };
                let (popup, inner) = modal.render(frame, area, 2);
                let t = theme::current();
                let hint = Span::styled("Enter provider name, then Enter", t.input_placeholder);
                let input_line = input_line_with_cursor(input, false);
                frame.render_widget(
                    ratatui::widgets::Paragraph::new(vec![Line::from(hint), input_line])
                        .style(Style::new().bg(t.background))
                        .wrap(Wrap { trim: true }),
                    inner,
                );
                popup
            }
            Step::CustomProtocol { picker, .. } => picker.view(frame, area),
            Step::CustomUrl { input, .. } => {
                let modal = Modal {
                    title: " Base URL ",
                    width_percent: 65,
                    max_height_percent: 40,
                };
                let (popup, inner) = modal.render(frame, area, 2);
                let t = theme::current();
                let hint = Span::styled("Enter base URL, then Enter", t.input_placeholder);
                let input_line = input_line_with_cursor(input, false);
                frame.render_widget(
                    ratatui::widgets::Paragraph::new(vec![Line::from(hint), input_line])
                        .style(Style::new().bg(t.background))
                        .wrap(Wrap { trim: true }),
                    inner,
                );
                popup
            }
            Step::BuiltinUrl {
                input,
                display_name,
                ..
            } => {
                let modal = Modal {
                    title: &format!(" {display_name} "),
                    width_percent: 65,
                    max_height_percent: 40,
                };
                let (popup, inner) = modal.render(frame, area, 2);
                let t = theme::current();
                let hint = Span::styled("Edit host URL, or Enter to confirm", t.input_placeholder);
                let input_line = input_line_with_cursor(input, false);
                frame.render_widget(
                    ratatui::widgets::Paragraph::new(vec![Line::from(hint), input_line])
                        .style(Style::new().bg(t.background))
                        .wrap(Wrap { trim: true }),
                    inner,
                );
                popup
            }
            Step::EnterKey {
                input,
                display_name,
                api_key_optional,
                ..
            } => {
                let modal = Modal {
                    title: &format!(" {display_name} "),
                    width_percent: 65,
                    max_height_percent: 40,
                };
                let (popup, inner) = modal.render(frame, area, 2);
                let t = theme::current();
                let hint_text = if *api_key_optional {
                    "Paste API key, or Enter to skip"
                } else {
                    "Paste API key, then Enter"
                };
                let hint = Span::styled(hint_text, t.input_placeholder);
                let input_line = input_line_with_cursor(input, false);
                frame.render_widget(
                    ratatui::widgets::Paragraph::new(vec![Line::from(hint), input_line])
                        .style(Style::new().bg(t.background))
                        .wrap(Wrap { trim: true }),
                    inner,
                );
                popup
            }
            Step::Done { message } => {
                let lines = iter::once(Line::from(message.clone()))
                    .chain(note_lines(&self.notes))
                    .collect();
                notes_modal(frame, area, lines)
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

fn notes_modal(frame: &mut Frame, area: Rect, lines: Vec<Line<'static>>) -> Rect {
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
    popup
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
    use maki_agent::tools::ToolRegistry;
    use maki_config::providers::ProvidersConfig;
    use maki_lua::PluginHost;
    use maki_lua::test_support::InMemoryFs;
    use maki_lua::ui_sink::HostUi;
    use maki_providers::Timeouts;
    use maki_storage::model::read_model;
    use tempfile::TempDir;

    use super::*;
    use crate::components::key;

    /// A builtin id, so the registered provider shadows the native one.
    const PROVIDER: &str = "deepseek";
    const DISPLAY_NAME: &str = "Fake DeepSeek";
    const DEFAULT_URL: &str = "http://localhost:8080";
    const ANSWERED_URL: &str = "http://example.test";
    const URL_PROMPT: &str = "Host URL";
    const KEY_PROMPT: &str = "API key";
    const KEY: &str = "sk-from-tui";
    const MODEL_SPEC: &str = "deepseek/fake-2";
    const BOUND: Duration = Duration::from_secs(10);

    const SOURCE: &str = r#"
        maki.api.register_provider({
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
    "#;

    struct Fixture {
        _host: PluginHost,
        picker: LoginPicker,
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
            host.load_source("fake_plugin", SOURCE).unwrap();
            let registry = host.provider_registry(Timeouts::default()).unwrap();
            let storage = StateDir::from_path(state.path().to_path_buf());
            Self {
                picker: LoginPicker {
                    browser: no_browser,
                    ..LoginPicker::new(registry, storage.clone())
                },
                _host: host,
                storage,
                _state: state,
            }
        }

        /// Handles the login's next event, waiting a bounded time for it.
        fn next_event(&mut self) -> Option<LoginPickerAction> {
            let login = self.picker.login.as_ref().expect("login running");
            let event = login.events.recv_timeout(BOUND).expect("login event");
            self.picker.handle_event(event)
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

    fn no_browser(_: &str) -> io::Result<()> {
        Ok(())
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
        let saved = load_provider_credentials(&fixture.storage, PROVIDER).unwrap();
        assert_eq!(saved.api_key, KEY);
        assert_eq!(saved.host.as_deref(), Some(ANSWERED_URL));
        let section = ProvidersConfig::try_load().unwrap();
        assert_eq!(
            section.get(PROVIDER).unwrap().base_url.as_deref(),
            Some(ANSWERED_URL)
        );
        assert_eq!(read_model(&fixture.storage).as_deref(), Some(MODEL_SPEC));
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

        fixture.picker.handle_key(key(KeyCode::Esc));
        assert!(matches!(fixture.picker.step, Step::Waiting));

        assert!(matches!(
            fixture.next_event(),
            Some(LoginPickerAction::Close)
        ));
        assert!(!fixture.picker.is_open());
        assert!(load_provider_credentials(&fixture.storage, PROVIDER).is_none());
    }

    #[test]
    fn native_choices_skip_registered() {
        let fixture = Fixture::new();
        let ids: Vec<String> = native_login_choices(&fixture.storage, &fixture.picker.providers)
            .into_iter()
            .map(|choice| choice.id)
            .collect();
        assert!(ids.iter().any(|id| id == CUSTOM_SLUG));
        assert!(!ids.iter().any(|id| id == PROVIDER));
    }
}
