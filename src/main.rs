mod cli;
mod cmd;
mod print;
mod project_trust;
mod sdk_mode;
mod setup;
mod update;

#[cfg(test)]
mod architecture_tests;

mod command_attachments {
    use std::sync::Arc;

    use color_eyre::Result;
    use color_eyre::eyre::eyre;
    use maki_agent::AgentInput;
    use maki_commands::{AgentTurn, CommandAttachment};
    use maki_providers::ImageSource;

    pub(crate) fn from_images(images: &[ImageSource]) -> Arc<[CommandAttachment]> {
        images
            .iter()
            .map(|image| CommandAttachment {
                media_type: Arc::from(image.media_type.mime()),
                data: Arc::clone(&image.data),
            })
            .collect::<Vec<_>>()
            .into()
    }

    pub(crate) fn into_images(attachments: &[CommandAttachment]) -> Result<Vec<ImageSource>> {
        maki_agent::command::command_images(attachments).map_err(|error| eyre!(error))
    }

    pub(crate) fn agent_input(turn: AgentTurn) -> Result<AgentInput> {
        maki_agent::command::command_input(turn).map_err(|error| eyre!(error))
    }
}

use clap::Parser;

use cli::Cli;

fn main() {
    // First, before anything can ask for a path: until this runs the state,
    // config, cache and log directories resolve to nothing, which is what
    // keeps a test binary out of the directories a real run reads and writes.
    maki_storage::paths::init().expect("storage paths must be initialized exactly once");
    color_eyre::install().ok();
    if let Err(e) = cmd::dispatch(Cli::parse()) {
        print_error(&e);
        std::process::exit(1);
    }
}

fn print_error(e: &color_eyre::Report) {
    const RED: &str = "\x1b[31m";
    const BOLD_RED: &str = "\x1b[1;31m";
    const DIM: &str = "\x1b[2m";
    const RESET: &str = "\x1b[0m";

    eprintln!();
    eprintln!("{BOLD_RED}✖ {e}{RESET}");
    let causes: Vec<_> = e.chain().skip(1).collect();
    let last = causes.len().saturating_sub(1);
    for (i, cause) in causes.iter().enumerate() {
        let branch = if i == last { "└─" } else { "├─" };
        eprintln!("{DIM}{branch}{RESET} {RED}{cause}{RESET}");
    }
    eprintln!();
}
