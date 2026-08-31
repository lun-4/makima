mod cli;
mod cmd;
mod print;
mod sdk_mode;
mod setup;
mod update;

#[cfg(test)]
mod architecture_tests;

mod command_attachments {
    use std::sync::Arc;

    use color_eyre::Result;
    use color_eyre::eyre::eyre;
    use maki_agent::{AgentInput, AgentMode, McpPromptRef};
    use maki_commands::{AgentTurn, CommandAttachment};
    use maki_providers::{ImageMediaType, ImageSource};

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
        attachments
            .iter()
            .map(|attachment| {
                let media_type =
                    ImageMediaType::from_mime(&attachment.media_type).ok_or_else(|| {
                        eyre!(
                            "unsupported command attachment media type: {}",
                            attachment.media_type
                        )
                    })?;
                Ok(ImageSource::new(media_type, Arc::clone(&attachment.data)))
            })
            .collect()
    }

    pub(crate) fn agent_input(
        turn: AgentTurn,
        mode: AgentMode,
        fast: bool,
        workflow: bool,
    ) -> Result<AgentInput> {
        Ok(AgentInput {
            message: turn.content.text.to_string(),
            mode,
            images: into_images(&turn.content.attachments)?,
            preamble: Vec::new(),
            thinking: Default::default(),
            fast,
            workflow,
            prompt: turn.prompt.map(|prompt| {
                Box::new(McpPromptRef {
                    qualified_name: prompt.qualified_name.to_string(),
                    arguments: prompt
                        .arguments
                        .iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                })
            }),
            lease_committer: None,
        })
    }
}

use clap::Parser;

use cli::Cli;

fn main() {
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
