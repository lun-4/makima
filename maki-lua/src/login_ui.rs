//! The `ui` a Lua provider's `login` receives, backed by the frontend's
//! `InteractionService`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use maki_lua_macro::{lua_class, lua_fn};
use maki_providers::AgentError;
use maki_providers::login::{ChoiceOption, InteractionRequest, InteractionService};
use mlua::{Lua, LuaSerdeExt, Result as LuaResult, UserDataRef, Value as LuaValue};

use crate::api::net::parse_http_url;
use crate::api::util::pair::{Pair, pair, try_pair};

pub(crate) struct LoginUi {
    pub(crate) service: Arc<dyn InteractionService>,
    /// Set once the user backs out, so a login that swallows the error still
    /// ends as cancelled.
    pub(crate) cancelled: Arc<AtomicBool>,
}

async fn ask(this: UserDataRef<LoginUi>, request: InteractionRequest) -> Pair<String> {
    let service = Arc::clone(&this.service);
    let cancelled = Arc::clone(&this.cancelled);
    drop(this);
    let answer = service.interact(request).await;
    if matches!(answer, Err(AgentError::Cancelled)) {
        cancelled.store(true, Ordering::SeqCst);
    }
    pair(answer)
}

async fn notify(this: UserDataRef<LoginUi>, request: InteractionRequest) -> Pair<bool> {
    let (answer, error) = ask(this, request).await;
    (answer.map(|_| true), error)
}

/// Ask the user for text.
///
/// @param prompt string What to ask for.
/// @param secret boolean? Hide the input, for keys and passwords.
/// @param default string? Text offered for editing.
/// @return (string?, string?) The entered text, or nil plus an error (`"cancelled"` when the user backed out).
/// @example
/// local key, err = ui:input("API key", true)
#[lua_fn]
async fn input(
    _lua: Lua,
    this: UserDataRef<LoginUi>,
    prompt: String,
    secret: Option<bool>,
    default: Option<String>,
) -> LuaResult<Pair<String>> {
    let request = InteractionRequest::Input {
        prompt,
        secret: secret.unwrap_or_default(),
        default,
    };
    Ok(ask(this, request).await)
}

/// Ask the user to choose one of {options}.
///
/// @param prompt string What the choice is about.
/// @param options table List of `{ id, label, detail? }`.
/// @return (string?, string?) The chosen id, or nil plus an error.
/// @example
/// local plan = ui:choice("Plan", { { id = "standard", label = "Standard" } })
#[lua_fn]
async fn choice(
    lua: Lua,
    this: UserDataRef<LoginUi>,
    prompt: String,
    options: LuaValue,
) -> LuaResult<Pair<String>> {
    let options: Vec<ChoiceOption> = lua.from_value(options)?;
    Ok(ask(this, InteractionRequest::Choice { prompt, options }).await)
}

/// Show {text} to the user. Returns at once.
///
/// @param text string Instructions to show.
/// @return (boolean?, string?) true, or nil plus an error.
/// @example
/// ui:instructions("Create a key in the dashboard first.")
#[lua_fn]
async fn instructions(
    _lua: Lua,
    this: UserDataRef<LoginUi>,
    text: String,
) -> LuaResult<Pair<bool>> {
    Ok(notify(this, InteractionRequest::Instructions(text)).await)
}

/// Show a device login: the user opens {url} and enters {code}. Returns at
/// once, so the provider can poll while the code is on screen.
///
/// @param url string Verification URL.
/// @param code string Code the user enters.
/// @return (boolean?, string?) true, or nil plus an error.
/// @example
/// ui:device_code("https://example.com/device", "ABCD-1234")
#[lua_fn]
async fn device_code(
    _lua: Lua,
    this: UserDataRef<LoginUi>,
    url: String,
    code: String,
) -> LuaResult<Pair<bool>> {
    Ok(notify(this, InteractionRequest::DeviceCode { url, code }).await)
}

/// Ask the frontend to open {url} in the user's browser. It also shows the
/// URL, in case the browser does not open. Returns at once.
///
/// @param url string An `http` or `https` URL.
/// @return (boolean?, string?) true, or nil plus an error.
/// @example
/// ui:open_browser("https://example.com/keys")
#[lua_fn]
async fn open_browser(_lua: Lua, this: UserDataRef<LoginUi>, url: String) -> LuaResult<Pair<bool>> {
    try_pair!(parse_http_url(&url));
    Ok(notify(this, InteractionRequest::OpenBrowser(url)).await)
}

/// Run the program {argv} on the user's terminal, handing it input and
/// output until it exits, for a login that is an interactive program. Only
/// `makima auth login` has a terminal to hand over; the TUI fails this.
///
/// @param argv table The program and its arguments.
/// @return (boolean?, string?) true once the program exited successfully, or nil plus an error.
/// @example
/// local ok, err = ui:run({ "/home/me/.config/makima/providers/corp", "login" })
#[lua_fn]
async fn run(_lua: Lua, this: UserDataRef<LoginUi>, argv: Vec<String>) -> LuaResult<Pair<bool>> {
    Ok(notify(this, InteractionRequest::Terminal(argv)).await)
}

lua_class! {
    /// The `ui` a provider's `login(ctx, ui, config)` receives.
    ///
    /// The CLI or the TUI shows it. `input` and `choice` wait for the user. The notices `instructions`,
    /// `device_code` and `open_browser` return at once. `run` waits for a program on the terminal.
    "maki.provider.LoginUi" => LoginUi, LOGIN_UI_DOCS [input, choice, instructions, device_code, open_browser, run]
}
