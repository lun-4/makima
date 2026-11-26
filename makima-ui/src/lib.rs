//! Single-threaded ratatui event loop; the agent runs on smol tasks in a separate thread.
//! `RuntimeChannels` bundles all flume channels to the agent. `dispatch()` processes
//! `Action`s returned by `App::update()`. Scroll and drag events are coalesced from
//! the queue to avoid jank.

pub mod animation;
pub mod app;
pub mod chat;
mod click;
mod clipboard;
mod clock;
mod color_compat;
mod command_runtime;
mod components;
pub use components::keybindings;
mod highlight;
pub use highlight::highlight_ansi;
pub mod image;
mod markdown;
mod model_capabilities;
pub use markdown::text_to_lines;
pub mod provider_usage;
mod render_worker;
pub mod repaint;
mod selection;
pub mod splash;
mod text_buffer;
mod theme;
pub use theme::BUNDLED_THEMES;
pub(crate) mod terminal_image;
mod trust_card;
pub use trust_card::ask_trust;
pub mod update;
pub mod wrap;

mod agent;
mod event_loop;
mod input;
mod plan_approval;
mod terminal;
mod terminal_da;

use std::time::Instant;

use color_eyre::Result;
use makima_agent::ToolOutput;
use makima_providers::Message;
use makima_providers::TokenUsage;
use makima_storage::id::MakimaId;

pub type AppSession = makima_storage::sessions::Session<Message, TokenUsage, ToolOutput>;

pub use event_loop::{EventLoopParams, StartupSessions};

pub enum RunOutcome {
    Exit {
        session: Option<(MakimaId, String)>,
        code: i32,
    },
    Reload {
        tabs: Vec<MakimaId>,
        focused: usize,
    },
}

pub fn run(params: EventLoopParams, initial_prompt: Option<String>) -> Result<RunOutcome> {
    let report = {
        let (_guard, mut terminal) = terminal::TerminalGuard::init()?;
        color_compat::init();
        let el = event_loop::EventLoop::new(&mut terminal, params)?;
        el.run(initial_prompt)?
    };
    Ok(match report.exit {
        components::ExitRequest::Reload => RunOutcome::Reload {
            tabs: report.tabs.iter().map(|session| session.id).collect(),
            focused: report.focused,
        },
        exit => {
            let session = report
                .tabs
                .get(report.focused)
                .filter(|s| app::session_has_content(s))
                .map(|s| (s.id, s.title().to_owned()));
            let started = Instant::now();
            drop(report);
            tracing::info!(
                elapsed_ms = started.elapsed().as_millis() as u64,
                "session buffers dropped"
            );
            RunOutcome::Exit {
                session,
                code: exit.code(),
            }
        }
    })
}
