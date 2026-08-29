use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use maki_commands::{
    CancellationToken, CommandCompletion, CommandFuture, CompletionContext, CompletionError,
    CompletionItem, CompletionLifecycleEvent, CompletionSessionId, InvocationTargetId,
};

use crate::theme::{ThemesProvider, apply_theme};

pub(crate) struct ModelArgSource {
    models: Arc<ArcSwapOption<Vec<String>>>,
}

impl ModelArgSource {
    pub(crate) fn new(models: Arc<ArcSwapOption<Vec<String>>>) -> Self {
        Self { models }
    }
}

impl CommandCompletion for ModelArgSource {
    fn complete(
        &self,
        _context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        let items = self.models.load_full().map_or_else(Vec::new, |specs| {
            specs
                .iter()
                .map(|spec| CompletionItem {
                    label: Arc::from(spec.as_str()),
                    insertion: Arc::from(spec.as_str()),
                    description: None,
                })
                .collect()
        });
        Box::pin(async move { Ok(items) })
    }
}

pub(crate) struct ThemeArgSource {
    provider: Arc<dyn ThemesProvider>,
    previews: Mutex<Vec<ThemePreview>>,
}

struct ThemePreview {
    session: CompletionSessionId,
    target: InvocationTargetId,
    original: String,
    selected: String,
    accepted: bool,
}

impl ThemeArgSource {
    pub(crate) fn new(provider: Arc<dyn ThemesProvider>) -> Self {
        Self {
            provider,
            previews: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn finish(&self, target: InvocationTargetId, commit: bool) {
        let mut previews = self
            .previews
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(index) = previews
            .iter()
            .rposition(|preview| preview.target == target && preview.accepted)
        else {
            return;
        };
        remove_preview(self.provider.as_ref(), &mut previews, index, commit);
    }
}

fn remove_preview(
    provider: &dyn ThemesProvider,
    previews: &mut Vec<ThemePreview>,
    index: usize,
    commit: bool,
) {
    let was_owner = index + 1 == previews.len();
    let removed = previews.remove(index);
    let restored = if commit {
        removed.selected
    } else {
        removed.original
    };
    if let Some(next) = previews.get_mut(index) {
        next.original = restored.clone();
    }
    if was_owner {
        let theme = previews
            .last()
            .map_or(restored.as_str(), |preview| preview.selected.as_str());
        apply_theme(provider, theme);
    }
}

impl CommandCompletion for ThemeArgSource {
    fn complete(
        &self,
        _context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        let items = self
            .provider
            .names()
            .into_iter()
            .map(|name| CompletionItem {
                label: Arc::from(name.as_str()),
                insertion: Arc::from(name),
                description: None,
            })
            .collect();
        Box::pin(async move { Ok(items) })
    }

    fn lifecycle(
        &self,
        context: &CompletionContext,
        event: &CompletionLifecycleEvent,
        _cancellation: &CancellationToken,
    ) -> Result<(), CompletionError> {
        let mut previews = self
            .previews
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match event {
            CompletionLifecycleEvent::Highlight(item) => {
                let index = previews
                    .iter()
                    .position(|preview| preview.session == context.session_id)
                    .unwrap_or_else(|| {
                        previews.push(ThemePreview {
                            session: context.session_id,
                            target: context.target_id,
                            original: self.provider.current_theme_name(),
                            selected: item.insertion.to_string(),
                            accepted: false,
                        });
                        previews.len() - 1
                    });
                previews[index].selected = item.insertion.to_string();
                if index + 1 == previews.len() {
                    apply_theme(self.provider.as_ref(), &item.insertion);
                }
            }
            CompletionLifecycleEvent::Accept(item) => {
                if let Some(preview) = previews
                    .iter_mut()
                    .find(|preview| preview.session == context.session_id)
                {
                    preview.selected = item.insertion.to_string();
                    preview.accepted = true;
                }
            }
            CompletionLifecycleEvent::Cancel => {
                if let Some(index) = previews
                    .iter()
                    .position(|preview| preview.session == context.session_id)
                {
                    remove_preview(self.provider.as_ref(), &mut previews, index, false);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use maki_commands::{
        ArgumentArity, CommandBehavior, CommandDocs, CommandError, CommandFuture,
        CommandInvocation, CommandOutcome, CommandRegistry, CommandSpec, CompletionResult,
        HostResponse, ProducerPrecedence, Registration, TargetCapabilities,
    };

    use crate::theme::InMemoryThemesProvider;

    use super::*;

    struct NoBehavior;

    impl CommandBehavior for NoBehavior {
        fn execute(
            &self,
            _invocation: CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            Box::pin(async { Ok(CommandOutcome::Completed) })
        }
    }

    impl maki_commands::CommandHost for NoBehavior {
        fn request(
            &self,
            _request: maki_commands::HostRequest,
        ) -> CommandFuture<Result<HostResponse, CommandError>> {
            Box::pin(async { Ok(HostResponse::Completed) })
        }
    }

    fn theme_fixture() -> (Arc<ThemeArgSource>, CommandRegistry) {
        let provider = Arc::new(InMemoryThemesProvider::bundled());
        let source = Arc::new(ThemeArgSource::new(provider));
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        producer
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/theme"),
                    aliases: Vec::new().into(),
                    arguments: ArgumentArity::unbounded(0),
                    docs: CommandDocs {
                        summary: Arc::from("test"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(NoBehavior),
                completion: Some(source.clone()),
            }])
            .unwrap();
        (source, registry)
    }

    fn accepted_preview(
        registry: &CommandRegistry,
        _source: &ThemeArgSource,
        theme: &str,
    ) -> InvocationTargetId {
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(NoBehavior));
        let command = registry.resolve_for(&target, "/theme").unwrap();
        let session = registry.open_completion(command, target.id()).unwrap();

        let result =
            smol::block_on(session.complete(Arc::from(""), Arc::from(""), 0, Arc::from("insert")));
        let CompletionResult::Items(candidates) = result else {
            panic!("expected completion items");
        };
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.item().insertion.as_ref() == theme)
            .unwrap_or_else(|| panic!("{theme} not in completion items"));
        session.highlight(candidate).unwrap();
        session.accept(candidate.clone()).unwrap();
        target.id()
    }

    const BASE_THEME: &str = "dracula";
    const SELECTED_THEME: &str = "tokyonight";

    #[test]
    fn bare_theme_invocation_reverts_stale_accepted_preview() {
        let (source, registry) = theme_fixture();
        source.provider.select(BASE_THEME);
        let target = accepted_preview(&registry, &source, SELECTED_THEME);

        // Opening the picker (empty arguments) must not commit the abandoned
        // selection.
        source.finish(target, false);
        assert_eq!(source.provider.current_theme_name(), BASE_THEME);
    }

    #[test]
    fn executing_a_selection_commits_its_preview() {
        let (source, registry) = theme_fixture();
        source.provider.select(BASE_THEME);
        let target = accepted_preview(&registry, &source, SELECTED_THEME);

        source.finish(target, true);
        assert_eq!(source.provider.current_theme_name(), SELECTED_THEME);
    }
}
