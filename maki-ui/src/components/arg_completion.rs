#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::components::file_completion::PathDiscovery;
use crate::theme::{ThemesProvider, apply_theme};
use arc_swap::ArcSwapOption;
use maki_commands::{
    CancellationToken, CommandCompletion, CommandFuture, CompletionContext, CompletionError,
    CompletionItem, CompletionItemNavigation, CompletionLifecycleEvent, CompletionPublisher,
    CompletionSessionId, InvocationTargetId, MAX_COMPLETION_CANDIDATES,
};

const PATH_COMPLETION_BATCH_SIZE: usize = 64;

pub(crate) struct ModelArgSource {
    models: Arc<ArcSwapOption<Vec<String>>>,
}

impl ModelArgSource {
    pub(crate) fn new(models: Arc<ArcSwapOption<Vec<String>>>) -> Self {
        Self { models }
    }
}

pub(crate) struct PathArgSource {
    discovery: PathDiscovery,
}

impl PathArgSource {
    pub(crate) fn new(home: Option<PathBuf>) -> Self {
        Self {
            discovery: PathDiscovery::new(home),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_discovery(discovery: PathDiscovery) -> Self {
        Self { discovery }
    }

    #[cfg(test)]
    pub(crate) fn typed_items(
        &self,
        cwd: &str,
        value: &str,
        directory_only: bool,
    ) -> std::io::Result<Vec<(String, bool)>> {
        self.discovery
            .typed_candidates(Path::new(cwd), value, directory_only)
    }
}

impl CommandCompletion for PathArgSource {
    fn complete(
        &self,
        context: CompletionContext,
        cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        let cwd = PathBuf::from(context.cwd.as_ref());
        let discovery = self.discovery.clone();
        let query = context.argument.to_string();
        let directory_only = matches!(
            context.argument_kind,
            Some(maki_commands::ArgumentKind::Directory)
        );
        Box::pin(async move {
            smol::unblock(move || {
                let mut items = Vec::new();
                discovery
                    .visit_typed_candidates(
                        &cwd,
                        &query,
                        directory_only,
                        &|| cancellation.is_cancelled(),
                        &mut |candidate| {
                            if items.len() == MAX_COMPLETION_CANDIDATES {
                                return false;
                            }
                            items.push(path_completion_item(candidate));
                            true
                        },
                    )
                    .map_err(|_| CompletionError::Unavailable)?;
                Ok(items)
            })
            .await
        })
    }

    fn navigation(
        &self,
        _context: &CompletionContext,
        item: &CompletionItem,
    ) -> CompletionItemNavigation {
        if item.insertion.ends_with(std::path::MAIN_SEPARATOR) {
            CompletionItemNavigation::Directory
        } else {
            CompletionItemNavigation::Terminal
        }
    }

    fn complete_incremental(
        &self,
        context: CompletionContext,
        cancellation: CancellationToken,
        publisher: CompletionPublisher,
    ) -> CommandFuture<Result<(), CompletionError>> {
        let cwd = PathBuf::from(context.cwd.as_ref());
        let discovery = self.discovery.clone();
        let query = context.argument.to_string();
        let directory_only = matches!(
            context.argument_kind,
            Some(maki_commands::ArgumentKind::Directory)
        );
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            smol::unblock(move || {
                let mut items = Vec::new();
                discovery
                    .visit_typed_candidates(
                        &cwd,
                        &query,
                        directory_only,
                        &|| cancellation.is_cancelled(),
                        &mut |candidate| {
                            if items.len() == MAX_COMPLETION_CANDIDATES {
                                return false;
                            }
                            items.push(path_completion_item(candidate));
                            if items.len().is_multiple_of(PATH_COMPLETION_BATCH_SIZE) {
                                items.sort_by(|left, right| left.insertion.cmp(&right.insertion));
                                if publisher.publish(items.clone()).is_err() {
                                    return false;
                                }
                            }
                            true
                        },
                    )
                    .map_err(|_| CompletionError::Unavailable)?;
                if !cancellation.is_cancelled() {
                    items.sort_by(|left, right| left.insertion.cmp(&right.insertion));
                    publisher.finish_with(items)?;
                }
                Ok(())
            })
            .await
        })
    }
}

fn path_completion_item((mut insertion, is_directory): (String, bool)) -> CompletionItem {
    if is_directory {
        insertion.push(std::path::MAIN_SEPARATOR);
    }
    CompletionItem {
        label: Arc::from(insertion.as_str()),
        insertion: Arc::from(insertion),
        description: None,
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
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;

    use maki_commands::{
        CommandArguments, CommandBehavior, CommandDocs, CommandError, CommandFuture,
        CommandInvocation, CommandOutcome, CommandRegistry, CommandSpec, CompletionInput,
        CompletionResult, CompletionSnapshotSink, HostResponse, ProducerPrecedence, Registration,
        TargetCapabilities,
    };
    use test_case::test_case;

    use crate::components::file_completion::{FileCandidate, FileResolver, PathDiscovery};
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
                    arguments: CommandArguments::Positional(Arc::from([
                        maki_commands::PositionalArgument::optional(
                            "theme",
                            maki_commands::ArgumentKind::String,
                        )
                        .with_completion(maki_commands::CompletionPolicy::Replace),
                    ])),
                    docs: CommandDocs {
                        summary: Arc::from("test"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(NoBehavior),
                argument_completions: vec![Some(source.clone())],
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

    struct FixtureResolver {
        reads: std::sync::Mutex<Vec<PathBuf>>,
        entries: Vec<FileCandidate>,
    }

    impl FileResolver for FixtureResolver {
        fn read_dir(&self, path: &Path) -> std::io::Result<Vec<FileCandidate>> {
            self.reads.lock().unwrap().push(path.to_path_buf());
            Ok(self.entries.clone())
        }
    }

    fn fixture_source(
        entries: Vec<FileCandidate>,
        home: Option<PathBuf>,
    ) -> (Arc<FixtureResolver>, PathArgSource) {
        let resolver = Arc::new(FixtureResolver {
            reads: std::sync::Mutex::new(Vec::new()),
            entries,
        });
        let source = PathArgSource::with_discovery(PathDiscovery::with_resolver(
            Arc::clone(&resolver) as Arc<dyn FileResolver>,
            home,
        ));
        (resolver, source)
    }

    fn file(name: &str) -> FileCandidate {
        FileCandidate {
            path: name.into(),
            is_directory: false,
        }
    }

    fn directory(name: &str) -> FileCandidate {
        FileCandidate {
            path: name.into(),
            is_directory: true,
        }
    }

    fn session_fixture(
        source: Arc<PathArgSource>,
        cwd: &str,
        kind: maki_commands::ArgumentKind,
    ) -> maki_commands::CompletionSession {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Application);
        producer
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/cd"),
                    aliases: Vec::new().into(),
                    arguments: maki_commands::CommandArguments::Positional(Arc::from([
                        maki_commands::PositionalArgument::optional("path", kind),
                    ])),
                    docs: CommandDocs {
                        summary: Arc::from("Change working directory."),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(NoBehavior),
                argument_completions: vec![None],
            }])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(NoBehavior));
        let command = registry.resolve_for(&target, "/cd").unwrap();
        let defaults = maki_commands::CompletionProviders::default()
            .with(
                maki_commands::CompletionKind::Directory,
                Arc::clone(&source) as Arc<dyn maki_commands::CommandCompletion>,
            )
            .with(
                maki_commands::CompletionKind::File,
                source as Arc<dyn maki_commands::CommandCompletion>,
            );
        registry
            .open_completion_with_defaults(command, target.id(), defaults, Arc::from(cwd))
            .unwrap()
    }

    fn candidates(
        session: &maki_commands::CompletionSession,
        argument: &str,
    ) -> Vec<CompletionItem> {
        match smol::block_on(session.complete(
            Arc::from(argument),
            Arc::from(argument),
            0,
            Arc::from("default"),
        )) {
            maki_commands::CompletionResult::Items(items) => items
                .into_iter()
                .map(|candidate| candidate.item().clone())
                .collect(),
            other => panic!("unexpected completion result: {other:?}"),
        }
    }

    #[test]
    fn typed_path_prefix_filters_relative_and_marks_directories() {
        let tmp = Arc::new(tempfile::tempdir().unwrap());
        let cwd = tmp.path().to_string_lossy().to_string();
        std::fs::create_dir_all(tmp.path().join("release apple")).unwrap();
        std::fs::write(tmp.path().join("release.txt"), b"x").unwrap();
        let source = Arc::new(PathArgSource::new(None));
        let session = session_fixture(source, &cwd, maki_commands::ArgumentKind::Directory);

        let mut items = candidates(&session, "./release ");
        let names: Vec<&str> = items
            .iter_mut()
            .map(|item| item.insertion.as_ref())
            .collect();
        assert!(names.contains(&"./release apple/"));
        assert!(!names.contains(&"./release.txt"));
    }

    #[cfg(unix)]
    #[test]
    fn typed_file_with_trailing_backslash_has_terminal_navigation() {
        let (resolver, source) = fixture_source(vec![file("report\\")], None);
        let session = session_fixture(
            source.into(),
            "/workspace/project",
            maki_commands::ArgumentKind::File,
        );

        let result = smol::block_on(session.complete(
            Arc::from("report"),
            Arc::from("report"),
            0,
            Arc::from("default"),
        ));
        let maki_commands::CompletionResult::Items(items) = result else {
            panic!("unexpected completion result: {result:?}");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(
            resolver.reads.lock().unwrap().as_slice(),
            &[PathBuf::from("/workspace/project")]
        );
        assert_eq!(items[0].item().insertion.as_ref(), "report\\");
        assert_eq!(items[0].navigation(), CompletionItemNavigation::Terminal);
    }

    #[test]
    fn typed_path_uses_shared_resolver_and_excludes_files_for_directories() {
        let cwd = PathBuf::from("/workspace/project");
        let (resolver, source) = fixture_source(
            vec![
                file("alpha.txt"),
                file("archive"),
                directory("archive"),
                directory("beta"),
            ],
            None,
        );
        let session = session_fixture(
            source.into(),
            "/workspace/project",
            maki_commands::ArgumentKind::Directory,
        );

        let mut items = candidates(&session, "ar");
        let names: Vec<&str> = items
            .iter_mut()
            .map(|item| item.insertion.as_ref())
            .collect();
        assert_eq!(names, vec!["archive/"]);
        assert_eq!(resolver.reads.lock().unwrap().as_slice(), &[cwd]);

        let _ = candidates(&session, "other");
        assert!(resolver.reads.lock().unwrap().len() == 2);
    }

    #[test]
    fn typed_path_home_and_absolute_namespaces_use_shared_resolution() {
        let home = PathBuf::from("/home/tester");
        let resolver = Arc::new(FixtureResolver {
            reads: std::sync::Mutex::new(Vec::new()),
            entries: vec![file("notes.txt")],
        });
        let source = Arc::new(PathArgSource::with_discovery(PathDiscovery::with_resolver(
            Arc::clone(&resolver) as Arc<dyn FileResolver>,
            Some(home.clone()),
        )));

        let items = source
            .typed_items("/workspace/project", "~/not", false)
            .expect("home discovery must not fail");
        assert_eq!(
            items[0].0, "~/notes.txt",
            "tilde namespace must be preserved"
        );
        assert_eq!(resolver.reads.lock().unwrap().as_slice(), &[home]);

        let items = source
            .typed_items("/workspace/project", "/tmp/not", false)
            .expect("absolute discovery must not fail");
        assert_eq!(items[0].0, "/tmp/notes.txt");
        assert_eq!(
            resolver.reads.lock().unwrap().last().unwrap(),
            &PathBuf::from("/tmp")
        );
    }

    #[test]
    fn typed_path_provider_stops_at_materialization_limit() {
        let entries = (0..MAX_COMPLETION_CANDIDATES + 100)
            .map(|index| directory(&format!("entry-{index}")))
            .collect();
        let (_resolver, source) = fixture_source(entries, None);
        let session = session_fixture(
            source.into(),
            "/workspace/project",
            maki_commands::ArgumentKind::Directory,
        );

        let items = candidates(&session, "");

        assert_eq!(items.len(), MAX_COMPLETION_CANDIDATES);
    }

    struct GatedResolver {
        visited: AtomicUsize,
        reached_batch: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        directories: bool,
    }

    impl FileResolver for GatedResolver {
        fn read_dir(&self, _path: &Path) -> std::io::Result<Vec<FileCandidate>> {
            unreachable!()
        }

        fn visit_dir(
            &self,
            _path: &Path,
            visitor: &mut dyn FnMut(FileCandidate) -> bool,
        ) -> std::io::Result<()> {
            for index in 0..MAX_COMPLETION_CANDIDATES {
                if index == PATH_COMPLETION_BATCH_SIZE {
                    self.reached_batch.send(()).unwrap();
                    self.release.lock().unwrap().recv().unwrap();
                }
                self.visited.fetch_add(1, Ordering::Relaxed);
                let candidate = if self.directories {
                    directory(&format!("entry-{index}"))
                } else {
                    file(&format!("entry-{index}"))
                };
                if !visitor(candidate) {
                    break;
                }
            }
            Ok(())
        }
    }

    #[test_case("", true, true; "matching_entries")]
    #[test_case("no-match", true, false; "nonmatching_prefix")]
    #[test_case("", false, false; "directory_only_rejects_files")]
    fn typed_path_stops_after_cancellation(query: &str, directories: bool, expects_snapshot: bool) {
        let (reached_tx, reached_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let resolver = Arc::new(GatedResolver {
            visited: AtomicUsize::new(0),
            reached_batch: reached_tx,
            release: Mutex::new(release_rx),
            directories,
        });
        let source = Arc::new(PathArgSource::with_discovery(PathDiscovery::with_resolver(
            Arc::clone(&resolver) as Arc<dyn FileResolver>,
            None,
        )));
        let session = session_fixture(
            source,
            "/workspace/project",
            maki_commands::ArgumentKind::Directory,
        );
        let (snapshots_tx, snapshots_rx) = mpsc::channel();
        let query: Arc<str> = Arc::from(query);
        let worker = {
            let session = session.clone();
            thread::spawn(move || {
                smol::block_on(session.complete_input_with_sink(
                    CompletionInput {
                        arguments: Arc::clone(&query),
                        argument: query,
                        argument_index: 0,
                        argument_range: Some(0..0),
                        mode: Arc::from("default"),
                    },
                    Some(CompletionSnapshotSink::new(move |snapshot| {
                        snapshots_tx.send(snapshot).unwrap();
                    })),
                ))
            })
        };

        reached_rx.recv().unwrap();
        if expects_snapshot {
            let first = snapshots_rx.recv().unwrap();
            assert_eq!(first.candidates.len(), PATH_COMPLETION_BATCH_SIZE);
        } else {
            assert!(snapshots_rx.try_recv().is_err());
        }
        session.cancel().unwrap();
        release_tx.send(()).unwrap();

        assert!(matches!(
            worker.join().unwrap(),
            CompletionResult::Cancelled
        ));
        assert_eq!(
            resolver.visited.load(Ordering::Relaxed),
            PATH_COMPLETION_BATCH_SIZE + 1
        );
    }

    #[test]
    fn cancelled_typed_path_request_returns_empty_without_io() {
        let (resolver, source) = fixture_source(vec![file("alpha.txt")], None);
        let session = session_fixture(
            source.into(),
            "/workspace/project",
            maki_commands::ArgumentKind::Directory,
        );
        session.cancel().unwrap();

        let result = smol::block_on(session.complete(
            Arc::from("al"),
            Arc::from("al"),
            0,
            Arc::from("default"),
        ));
        assert!(matches!(
            result,
            maki_commands::CompletionResult::Cancelled
                | maki_commands::CompletionResult::Failed
                | maki_commands::CompletionResult::Stale
        ));
        assert!(resolver.reads.lock().unwrap().is_empty());
    }
}
