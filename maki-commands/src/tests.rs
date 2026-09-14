use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::*;
use crate::completion::{CompletionInvalidation, CompletionPublisher, CompletionSource};
use test_case::test_case;

const LOCK_RELEASE_TIMEOUT: Duration = Duration::from_secs(1);

struct OutcomeBehavior(CommandOutcome);
struct CountingBehavior(Arc<AtomicU64>);

impl CommandBehavior for OutcomeBehavior {
    fn execute(
        &self,
        _invocation: CommandInvocation,
    ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
        let outcome = self.0.clone();
        Box::pin(async move { Ok(outcome) })
    }
}

impl CommandBehavior for CountingBehavior {
    fn execute(
        &self,
        _invocation: CommandInvocation,
    ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(CommandOutcome::Completed) })
    }
}

struct Host;

#[test]
fn prepared_target_is_stale_until_activation() {
    let registry = CommandRegistry::new();
    let prepared = registry.prepare_target(TargetCapabilities::NONE, Arc::new(Host));
    let id = prepared.handle().id();

    assert!(matches!(
        registry.snapshot_for(prepared.handle()),
        Err(CommandError::StaleTarget)
    ));

    let target = prepared.activate();
    assert_eq!(target.id(), id);
    assert!(registry.snapshot_for(&target).is_ok());
}

#[test]
fn dropped_prepared_target_is_never_published() {
    let registry = CommandRegistry::new();
    let prepared = registry.prepare_target(TargetCapabilities::NONE, Arc::new(Host));
    let handle = prepared.handle().clone();
    drop(prepared);

    assert!(matches!(
        registry.snapshot_for(&handle),
        Err(CommandError::StaleTarget)
    ));
}

#[derive(Default)]
struct CompletionProbe {
    completions: AtomicU64,
    events: Mutex<Vec<CompletionLifecycleEvent>>,
    reenter: Option<CommandRegistry>,
}

struct BlockingCompletion {
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    events: Mutex<Vec<CompletionLifecycleEvent>>,
}

struct SnapshotProvider {
    started: mpsc::SyncSender<CompletionPublisher>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    events: Arc<Mutex<Vec<CompletionLifecycleEvent>>>,
    navigation: Arc<Mutex<CompletionItemNavigation>>,
    preview: Mutex<Option<CompletionItem>>,
}

struct ReleaseSender(Option<mpsc::SyncSender<()>>);

impl ReleaseSender {
    fn send(&mut self) {
        self.0.take().unwrap().send(()).unwrap();
    }
}

struct ContextProbe {
    context: Mutex<Option<CompletionContext>>,
}

type VariadicCompletionCall = (Arc<str>, Arc<[ParsedArgument]>);

struct VariadicCompletionProbe {
    calls: Arc<Mutex<Vec<VariadicCompletionCall>>>,
}

impl CommandCompletion for VariadicCompletionProbe {
    fn complete(
        &self,
        context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        self.calls
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push((Arc::clone(&context.argument), context.preceding_arguments));
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct FirstRequestBlockingCompletion {
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

struct ReentrantRegistryWaker {
    registry: CommandRegistry,
    woke: mpsc::SyncSender<()>,
}

struct CompletionSessionWaker {
    session: CompletionSession,
    result: mpsc::SyncSender<CompletionResult>,
}

impl Wake for ReentrantRegistryWaker {
    fn wake(self: Arc<Self>) {
        drop(self.registry.subscribe());
        self.woke.send(()).unwrap();
    }
}

impl Wake for CompletionSessionWaker {
    fn wake(self: Arc<Self>) {
        let result = futures_lite::future::block_on(self.session.complete(
            Arc::from(""),
            Arc::from(""),
            0,
            Arc::from("insert"),
        ));
        self.result.send(result).unwrap();
    }
}

impl CommandCompletion for CompletionProbe {
    fn complete(
        &self,
        _context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        self.completions.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Ok(vec![CompletionItem {
                label: Arc::from("candidate"),
                insertion: Arc::from("candidate"),
                description: None,
            }])
        })
    }

    fn lifecycle(
        &self,
        _context: &CompletionContext,
        event: &CompletionLifecycleEvent,
        _cancellation: &CancellationToken,
    ) -> Result<(), CompletionError> {
        if let Some(registry) = &self.reenter {
            drop(registry.create_producer(ProducerPrecedence::Plugin));
        }
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(event.clone());
        Ok(())
    }
}

impl CommandCompletion for SnapshotProvider {
    fn complete(
        &self,
        _context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn complete_incremental(
        &self,
        _context: CompletionContext,
        _cancellation: CancellationToken,
        publisher: CompletionPublisher,
    ) -> CommandFuture<Result<(), CompletionError>> {
        let started = self.started.clone();
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            started.send(publisher).unwrap();
            release
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv()
                .unwrap();
            Ok(())
        })
    }

    fn navigation(
        &self,
        _context: &CompletionContext,
        _item: &CompletionItem,
    ) -> CompletionItemNavigation {
        *self
            .navigation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn lifecycle(
        &self,
        _context: &CompletionContext,
        event: &CompletionLifecycleEvent,
        _cancellation: &CancellationToken,
    ) -> Result<(), CompletionError> {
        *self.preview.lock().unwrap() = match event {
            CompletionLifecycleEvent::Highlight(item) => Some(item.clone()),
            _ => None,
        };
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(event.clone());
        Ok(())
    }
}

impl CommandCompletion for ContextProbe {
    fn complete(
        &self,
        context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        *self.context.lock().unwrap() = Some(context);
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl CommandCompletion for FirstRequestBlockingCompletion {
    fn complete(
        &self,
        context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        if context.generation == 0 {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv()
                .unwrap();
        }
        Box::pin(async {
            Ok(vec![CompletionItem {
                label: Arc::from("candidate"),
                insertion: Arc::from("candidate"),
                description: None,
            }])
        })
    }
}

impl CommandCompletion for BlockingCompletion {
    fn complete(
        &self,
        _context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .recv()
            .unwrap();
        Box::pin(async {
            Ok(vec![CompletionItem {
                label: Arc::from("candidate"),
                insertion: Arc::from("candidate"),
                description: None,
            }])
        })
    }

    fn lifecycle(
        &self,
        _context: &CompletionContext,
        event: &CompletionLifecycleEvent,
        _cancellation: &CancellationToken,
    ) -> Result<(), CompletionError> {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(event.clone());
        Ok(())
    }
}

impl CommandHost for Host {
    fn request(&self, _request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        Box::pin(async { Ok(HostResponse::Completed) })
    }
}

fn registration(name: &str, capabilities: TargetCapabilities) -> Registration {
    registration_with(
        name,
        &[],
        Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
        capabilities,
    )
}

fn registration_with(
    name: &str,
    aliases: &[&str],
    behavior: Arc<dyn CommandBehavior>,
    capabilities: TargetCapabilities,
) -> Registration {
    Registration {
        spec: CommandSpec {
            name: Arc::from(name),
            aliases: aliases.iter().copied().map(Arc::from).collect(),
            arguments: CommandArguments::Raw { required: false },
            docs: CommandDocs {
                summary: Arc::from("test command"),
                argument_hint: None,
            },
            required_capabilities: capabilities,
        },
        behavior,
        argument_completions: Vec::new(),
    }
}

fn completion_registration(completion: Arc<dyn CommandCompletion>) -> Registration {
    positional_registration_with_completion(
        "/complete",
        Arc::from([PositionalArgument::required("value", ArgumentKind::String)
            .with_completion(CompletionPolicy::Replace)]),
        Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
        Some(completion),
    )
}

fn positional_registration(
    name: &str,
    arguments: Arc<[PositionalArgument]>,
    behavior: Arc<dyn CommandBehavior>,
) -> Registration {
    positional_registration_with_completion(name, arguments, behavior, None)
}

fn positional_registration_with_completion(
    name: &str,
    arguments: Arc<[PositionalArgument]>,
    behavior: Arc<dyn CommandBehavior>,
    completion: Option<Arc<dyn CommandCompletion>>,
) -> Registration {
    Registration {
        spec: CommandSpec {
            name: Arc::from(name),
            aliases: Arc::from([]),
            arguments: CommandArguments::Positional(Arc::clone(&arguments)),
            docs: CommandDocs {
                summary: Arc::from("test command"),
                argument_hint: None,
            },
            required_capabilities: TargetCapabilities::NONE,
        },
        behavior,
        argument_completions: vec![completion; arguments.len()],
    }
}

fn completion_session(
    registry: &CommandRegistry,
    producer: &Producer,
    completion: Arc<dyn CommandCompletion>,
) -> CompletionSession {
    producer
        .replace(vec![completion_registration(completion)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let command = registry.resolve_for(&target, "/complete").unwrap();
    registry.open_completion(command, target.id()).unwrap()
}

fn complete_once(session: &CompletionSession) -> CompletionCandidate {
    let CompletionResult::Items(mut items) = futures_lite::future::block_on(session.complete(
        Arc::from(""),
        Arc::from(""),
        0,
        Arc::from("insert"),
    )) else {
        panic!("completion did not return items");
    };
    items.pop().unwrap()
}

fn completion_item(value: &str) -> CompletionItem {
    CompletionItem {
        label: Arc::from(value),
        insertion: Arc::from(value),
        description: None,
    }
}

type SnapshotFixture = (
    Arc<SnapshotProvider>,
    mpsc::Receiver<CompletionPublisher>,
    ReleaseSender,
    Arc<Mutex<Vec<CompletionLifecycleEvent>>>,
);

fn gated_snapshot_provider(navigation: CompletionItemNavigation) -> SnapshotFixture {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let events = Arc::new(Mutex::new(Vec::new()));
    let release = Arc::new(Mutex::new(release_rx));
    (
        Arc::new(SnapshotProvider {
            started: started_tx,
            release,
            events: Arc::clone(&events),
            navigation: Arc::new(Mutex::new(navigation)),
            preview: Mutex::new(None),
        }),
        started_rx,
        ReleaseSender(Some(release_tx)),
        events,
    )
}

fn snapshot_session(
    provider: Arc<dyn CommandCompletion>,
    policy: CompletionPolicy,
    defaults: CompletionProviders,
) -> (CommandRegistry, CompletionSession) {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let mut argument = PositionalArgument::required("value", ArgumentKind::String);
    argument.completion = policy;
    producer
        .replace(vec![
            positional_registration(
                "/snapshot",
                Arc::from([argument]),
                Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
            )
            .with_argument_completion("value", provider),
        ])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let command = registry.resolve_for(&target, "/snapshot").unwrap();
    let session = registry
        .open_completion_with_defaults(command, target.id(), defaults, Arc::from(""))
        .unwrap();
    (registry, session)
}

fn start_snapshot(session: &CompletionSession) -> thread::JoinHandle<CompletionResult> {
    let session = session.clone();
    thread::spawn(move || {
        futures_lite::future::block_on(session.complete_input_with_sink(
            CompletionInput {
                arguments: Arc::from(""),
                argument: Arc::from(""),
                argument_index: 0,
                argument_range: Some(0..0),
                mode: Arc::from("insert"),
            },
            None,
        ))
    })
}

#[test]
fn completion_snapshot_preserves_composed_candidates_for_consumer_filtering() {
    let (provider, started, mut release, _events) =
        gated_snapshot_provider(CompletionItemNavigation::Terminal);
    let (_registry, session) = snapshot_session(
        provider,
        CompletionPolicy::Replace,
        CompletionProviders::default(),
    );
    let worker = start_snapshot(&session);
    let publisher = started.recv().unwrap();
    let item_count = MAX_COMPLETION_CANDIDATES + 100;
    let items = (0..item_count)
        .map(|index| completion_item(&format!("item-{index}")))
        .collect();

    let snapshot = publisher.publish(items).unwrap();

    assert_eq!(snapshot.candidates.len(), item_count);
    publisher.finish().unwrap();
    release.send();
    assert!(
        matches!(worker.join().unwrap(), CompletionResult::Items(items) if items.len() == item_count)
    );
}

#[test]
fn completion_snapshot_replacement_rejects_removed_value_before_validate_highlight_and_accept() {
    let (provider, started, mut release, events) =
        gated_snapshot_provider(CompletionItemNavigation::Terminal);
    let (_registry, session) = snapshot_session(
        provider,
        CompletionPolicy::Replace,
        CompletionProviders::default(),
    );
    let worker = start_snapshot(&session);
    let publisher = started.recv().unwrap();
    let first = publisher.publish(vec![completion_item("old")]).unwrap();
    let old = first.candidates[0].clone();
    let second = publisher.publish(Vec::new()).unwrap();
    assert!(second.candidates.is_empty());
    assert_eq!(session.validate(&old), Err(CompletionError::StaleRequest));
    assert_eq!(session.highlight(&old), Err(CompletionError::StaleRequest));
    assert_eq!(session.accept(old), Err(CompletionError::StaleRequest));
    publisher.finish().unwrap();
    release.send();
    assert!(matches!(worker.join().unwrap(), CompletionResult::Items(items) if items.is_empty()));
    assert!(events.lock().unwrap().is_empty());
}

#[test]
fn completion_snapshot_replacement_rejects_changed_navigation_before_highlight_and_accept() {
    let (provider, started, mut release, events) =
        gated_snapshot_provider(CompletionItemNavigation::Terminal);
    let navigation = Arc::clone(&provider.navigation);
    let (_registry, session) = snapshot_session(
        provider,
        CompletionPolicy::Replace,
        CompletionProviders::default(),
    );
    let worker = start_snapshot(&session);
    let publisher = started.recv().unwrap();
    let first = publisher.publish(vec![completion_item("path")]).unwrap();
    let old = first.candidates[0].clone();
    *navigation.lock().unwrap() = CompletionItemNavigation::Directory;
    let second = publisher.publish(vec![completion_item("path")]).unwrap();
    let fresh = second.candidates[0].clone();
    assert_eq!(fresh.navigation(), CompletionItemNavigation::Directory);
    assert_eq!(session.validate(&old), Err(CompletionError::StaleRequest));
    assert_eq!(session.highlight(&old), Err(CompletionError::StaleRequest));
    assert_eq!(session.accept(old), Err(CompletionError::StaleRequest));
    session.validate(&fresh).unwrap();
    session.highlight(&fresh).unwrap();
    publisher.finish().unwrap();
    release.send();
    assert!(
        matches!(worker.join().unwrap(), CompletionResult::Items(items) if items[0].navigation() == CompletionItemNavigation::Directory)
    );
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [CompletionLifecycleEvent::Highlight(completion_item("path"))]
    );
}

#[test]
fn completion_snapshot_replacement_rejects_old_custom_duplicate_provenance() {
    let (custom, custom_started, mut custom_release, custom_events) =
        gated_snapshot_provider(CompletionItemNavigation::Terminal);
    let (default, default_started, mut default_release, _default_events) =
        gated_snapshot_provider(CompletionItemNavigation::Directory);
    let defaults = CompletionProviders::default().with(CompletionKind::String, default);
    let (_registry, session) = snapshot_session(custom, CompletionPolicy::Extend, defaults);
    let worker = start_snapshot(&session);
    let default_publisher = default_started.recv().unwrap();
    let default_snapshot = default_publisher
        .publish(vec![completion_item("same")])
        .unwrap();
    let old = default_snapshot.candidates[0].clone();
    assert_eq!(old.source(), &CompletionSource::KindDefault);
    default_publisher.finish().unwrap();
    default_release.send();
    let custom_publisher = custom_started.recv().unwrap();
    let second = custom_publisher
        .publish(vec![completion_item("same")])
        .unwrap();
    let fresh = second.candidates[0].clone();
    assert!(matches!(fresh.source(), CompletionSource::Argument(_)));
    assert_eq!(session.validate(&old), Err(CompletionError::StaleRequest));
    assert_eq!(session.highlight(&old), Err(CompletionError::StaleRequest));
    assert_eq!(session.accept(old), Err(CompletionError::StaleRequest));
    session.validate(&fresh).unwrap();
    session.highlight(&fresh).unwrap();
    custom_publisher.finish().unwrap();
    custom_release.send();
    assert!(
        matches!(worker.join().unwrap(), CompletionResult::Items(items) if items[0].source() == fresh.source())
    );
    assert_eq!(
        custom_events.lock().unwrap().as_slice(),
        [CompletionLifecycleEvent::Highlight(completion_item("same"))]
    );
}

#[test]
fn finished_custom_provider_is_cancelled_when_default_candidate_wins() {
    let (custom, custom_started, mut custom_release, custom_events) =
        gated_snapshot_provider(CompletionItemNavigation::Terminal);
    let custom_preview = Arc::clone(&custom);
    let (default, default_started, mut default_release, default_events) =
        gated_snapshot_provider(CompletionItemNavigation::Terminal);
    let defaults = CompletionProviders::default().with(CompletionKind::String, default);
    let (_registry, session) = snapshot_session(custom, CompletionPolicy::Extend, defaults);
    let worker = start_snapshot(&session);
    let default_publisher = default_started.recv().unwrap();
    default_publisher
        .publish(vec![completion_item("default")])
        .unwrap();
    default_publisher.finish().unwrap();
    default_release.send();
    let custom_publisher = custom_started.recv().unwrap();
    custom_publisher
        .publish(vec![completion_item("custom")])
        .unwrap();
    custom_publisher.finish().unwrap();
    custom_release.send();
    let CompletionResult::Items(candidates) = worker.join().unwrap() else {
        panic!("finished providers must retain their candidates");
    };
    let custom_candidate = candidates
        .iter()
        .find(|candidate| candidate.item().insertion.as_ref() == "custom")
        .unwrap();
    let default_candidate = candidates
        .iter()
        .find(|candidate| candidate.item().insertion.as_ref() == "default")
        .unwrap();
    assert_eq!(default_candidate.source(), &CompletionSource::KindDefault);
    session.highlight(custom_candidate).unwrap();
    assert_eq!(
        *custom_preview.preview.lock().unwrap(),
        Some(completion_item("custom"))
    );
    session.accept(default_candidate.clone()).unwrap();
    session.cancel().unwrap();
    assert!(custom_preview.preview.lock().unwrap().is_none());
    assert_eq!(
        custom_events.lock().unwrap().as_slice(),
        [
            CompletionLifecycleEvent::Highlight(completion_item("custom")),
            CompletionLifecycleEvent::Cancel,
        ]
    );
    assert_eq!(
        default_events.lock().unwrap().as_slice(),
        [CompletionLifecycleEvent::Accept(completion_item("default"))]
    );
}

#[test]
fn resolve_input_uses_shared_parser_and_preserves_arguments() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Plugin);
    producer
        .replace(vec![registration("/test", TargetCapabilities::NONE)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

    let resolved = registry
        .resolve_input_for(&target, "  /TeSt alpha  beta ")
        .unwrap();

    assert_eq!(resolved.command.invoked_name(), "/TeSt");
    assert_eq!(resolved.arguments.as_ref(), "alpha  beta");
    assert!(matches!(
        registry.resolve_input_for(&target, "literal input"),
        Err(ResolutionError::UnknownCommand(name)) if name.as_ref() == "literal input"
    ));
}

#[test]
fn typed_dispatch_parses_quoted_arguments_before_behavior() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let executions = Arc::new(AtomicU64::new(0));
    producer
        .replace(vec![positional_registration(
            "/typed",
            Arc::from([
                PositionalArgument::required("message", ArgumentKind::String),
                PositionalArgument::required("count", ArgumentKind::Integer),
            ]),
            Arc::new(CountingBehavior(Arc::clone(&executions))),
        )])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

    assert!(matches!(
        futures_lite::future::block_on(
            registry.dispatch_input(&target, "/typed \"hello world\" 3".into(),)
        ),
        InputDispatch::Dispatched(CommandOutcome::Completed)
    ));
    assert_eq!(executions.load(Ordering::Relaxed), 1);
}

#[test]
fn completion_policy_composes_default_and_provider_items() {
    assert_eq!(
        CompletionPolicy::Disabled.compose(vec![1, 2], vec![3]),
        Vec::<i32>::new()
    );
    assert_eq!(
        CompletionPolicy::Replace.compose(vec![1, 2], vec![3]),
        vec![3]
    );
    assert_eq!(
        CompletionPolicy::Extend.compose(vec![1, 2], vec![3]),
        vec![1, 2, 3]
    );
    assert_eq!(
        CompletionPolicy::Default.compose(vec![1], Vec::<i32>::new()),
        vec![1]
    );
}

#[test]
fn variadic_replace_provider_registers_and_repeats_for_each_slot() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(VariadicCompletionProbe {
        calls: Arc::clone(&calls),
    });
    let mut paths = PositionalArgument::required("paths", ArgumentKind::Directory);
    paths.variadic = true;
    paths.completion = CompletionPolicy::Replace;
    producer
        .replace(vec![
            positional_registration(
                "/paths",
                Arc::from([paths]),
                Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
            )
            .with_argument_completion("paths", provider),
        ])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let command = registry.resolve_for(&target, "/paths").unwrap();
    let session = registry.open_completion(command, target.id()).unwrap();

    for (argument, range) in [("one", 0..3), ("two", 4..7), ("one", 0..3)] {
        let _ = futures_lite::future::block_on(session.complete_input(CompletionInput {
            arguments: Arc::from("one two three "),
            argument: Arc::from(argument),
            argument_index: 0,
            argument_range: Some(range),
            mode: Arc::from("insert"),
        }));
    }

    let calls = calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|(argument, _)| argument.as_ref())
            .collect::<Vec<_>>(),
        ["one", "two", "one"]
    );
    assert!(calls[0].1.is_empty());
    assert_eq!(
        calls[1]
            .1
            .iter()
            .flat_map(|argument| argument.values.iter())
            .map(|value| match value {
                ArgumentValue::Directory(path) => path.to_string_lossy(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>(),
        ["one"]
    );
    assert!(calls[2].1.is_empty());
}

#[test]
fn completion_context_exposes_argument_semantics_and_legacy_input_stays_usable() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let probe = Arc::new(ContextProbe {
        context: Mutex::new(None),
    });
    producer
        .replace(vec![
            positional_registration(
                "/typed",
                Arc::from([
                    PositionalArgument::required(
                        "mode",
                        ArgumentKind::enum_with_default(
                            Arc::from([Arc::from("fast"), Arc::from("safe")]),
                            "safe",
                        ),
                    ),
                    PositionalArgument::optional("path", ArgumentKind::Directory)
                        .with_completion(CompletionPolicy::Replace),
                ]),
                Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
            )
            .with_argument_completion("path", probe.clone()),
        ])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let command = registry.resolve_for(&target, "/typed").unwrap();
    let session = registry.open_completion(command, target.id()).unwrap();

    let _ = futures_lite::future::block_on(session.complete_input(CompletionInput {
        arguments: Arc::from("fast"),
        argument: Arc::from(""),
        argument_index: 1,
        argument_range: Some(0..0),
        mode: Arc::from("insert"),
    }));
    let context = probe.context.lock().unwrap().clone();
    assert_eq!(
        context
            .as_ref()
            .and_then(|value| value.argument_name.as_deref()),
        Some("path")
    );
    assert_eq!(
        context.as_ref().map(|value| value.completion_policy),
        Some(CompletionPolicy::Replace)
    );
    assert_eq!(
        context.as_ref().and_then(|value| value.next_argument_index),
        None
    );
    assert_eq!(
        context.as_ref().map(|value| value.navigation),
        Some(CompletionNavigation::Close)
    );
}

#[test]
fn invalid_typed_input_prevents_behavior_execution() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let executions = Arc::new(AtomicU64::new(0));
    producer
        .replace(vec![positional_registration(
            "/typed",
            Arc::from([PositionalArgument::required(
                "mode",
                ArgumentKind::Enum(Arc::from([Arc::from("fast"), Arc::from("safe")])),
            )]),
            Arc::new(CountingBehavior(Arc::clone(&executions))),
        )])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

    let outcome =
        futures_lite::future::block_on(registry.dispatch_input(&target, "/typed slow".into()));

    assert!(matches!(
        outcome,
        InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::TypedArguments { .. }))
    ));
    assert_eq!(executions.load(Ordering::Relaxed), 0);
}

#[test]
fn builtin_argument_descriptors_carry_completion_metadata() {
    let model = BUILTIN_COMMANDS
        .iter()
        .find(|command| command.id == BuiltinId::Model)
        .unwrap()
        .spec();
    let theme = BUILTIN_COMMANDS
        .iter()
        .find(|command| command.id == BuiltinId::Theme)
        .unwrap()
        .spec();
    assert!(matches!(model.arguments, CommandArguments::Positional(_)));
    assert_eq!(
        model
            .arguments
            .positional()
            .unwrap()
            .first()
            .unwrap()
            .completion,
        CompletionPolicy::Replace
    );
    assert_eq!(
        theme
            .arguments
            .positional()
            .unwrap()
            .first()
            .unwrap()
            .completion,
        CompletionPolicy::Replace
    );

    let btw = BUILTIN_COMMANDS
        .iter()
        .find(|command| command.id == BuiltinId::Btw)
        .unwrap()
        .spec();
    assert_eq!(btw.arguments, CommandArguments::Raw { required: true });
}

#[test]
fn typed_usage_hint_fallback_preserves_explicit_hints() {
    let typed = CommandSpec {
        name: Arc::from("/typed"),
        aliases: Arc::from([]),
        arguments: CommandArguments::Positional(Arc::from([
            PositionalArgument::required("source", ArgumentKind::File),
            PositionalArgument::optional("paths", ArgumentKind::Directory),
        ])),
        docs: CommandDocs {
            summary: Arc::from("typed"),
            argument_hint: None,
        },
        required_capabilities: TargetCapabilities::NONE,
    };
    assert_eq!(typed.argument_hint().as_deref(), Some("<source> [paths]"));

    let explicit = CommandSpec {
        docs: CommandDocs {
            argument_hint: Some(Arc::from("<custom>")),
            ..typed.docs.clone()
        },
        ..typed
    };
    assert_eq!(explicit.argument_hint().as_deref(), Some("<custom>"));
    assert_eq!(
        BUILTIN_COMMANDS
            .iter()
            .find(|command| command.id == BuiltinId::Cd)
            .unwrap()
            .spec()
            .argument_hint()
            .as_deref(),
        Some("[path]")
    );
}

#[test]
fn positional_schema_validation_is_atomic() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    producer
        .replace(vec![registration("/old", TargetCapabilities::NONE)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let generation = registry.snapshot_for(&target).unwrap().generation();
    let invalid = positional_registration(
        "/new",
        Arc::from([
            PositionalArgument::required("value", ArgumentKind::String),
            PositionalArgument::required("value", ArgumentKind::Integer),
        ]),
        Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
    );

    assert!(matches!(
        producer.replace(vec![invalid]),
        Err(RegistrationError::InvalidArgumentSchema(name)) if name.as_ref() == "value"
    ));

    let snapshot = registry.snapshot_for(&target).unwrap();
    assert_eq!(snapshot.generation(), generation);
    assert_eq!(snapshot.commands().len(), 1);
    assert_eq!(snapshot.commands()[0].spec().name.as_ref(), "/old");
    assert!(registry.resolve_for(&target, "/new").is_err());
}

#[test]
fn projection_and_dispatch_share_capability_filter() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Builtin);
    let required = TargetCapabilities::from_capability(TargetCapability::InteractiveUi);
    producer
        .replace(vec![registration("/picker", required)])
        .unwrap();
    let portable = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let interactive = registry.bind_target(required, Arc::new(Host));

    assert!(registry.resolve_for(&portable, "/picker").is_err());
    assert!(registry.presented_commands(&portable).unwrap().is_empty());
    assert!(matches!(
        futures_lite::future::block_on(registry.dispatch_input(&portable, "/picker".into())),
        InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(_)))
    ));
    assert_eq!(registry.presented_commands(&interactive).unwrap().len(), 1);
    assert!(matches!(
        futures_lite::future::block_on(registry.dispatch_input(&interactive, "/picker".into())),
        InputDispatch::Dispatched(CommandOutcome::Completed)
    ));
}

#[test]
fn portable_override_wins_over_restricted_builtin() {
    let registry = CommandRegistry::new();
    let builtin = registry.create_producer(ProducerPrecedence::Builtin);
    builtin
        .replace(vec![registration(
            "/shared",
            TargetCapabilities::from_capability(TargetCapability::InteractiveUi),
        )])
        .unwrap();
    let plugin = registry.create_producer(ProducerPrecedence::Plugin);
    plugin
        .replace(vec![registration("/shared", TargetCapabilities::NONE)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

    let resolved = registry.resolve_for(&target, "/shared").unwrap();
    assert_eq!(resolved.producer_id(), plugin.id());
    assert!(matches!(
        futures_lite::future::block_on(registry.dispatch_input(&target, "/shared".into())),
        InputDispatch::Dispatched(CommandOutcome::Completed)
    ));
}

#[test]
fn foreign_target_is_rejected() {
    let registry = CommandRegistry::new();
    let foreign_registry = CommandRegistry::new();
    let foreign = foreign_registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

    assert!(matches!(
        registry.snapshot_for(&foreign),
        Err(CommandError::StaleTarget)
    ));
}

#[test]
fn replaced_resolved_command_is_stale() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    producer
        .replace(vec![registration("/old", TargetCapabilities::NONE)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let resolved = registry.resolve_for(&target, "/old").unwrap();
    producer
        .replace(vec![registration("/new", TargetCapabilities::NONE)])
        .unwrap();

    let outcome = futures_lite::future::block_on(registry.dispatch_command(
        &target,
        resolved,
        Arc::from(""),
        "/old".into(),
    ));

    assert!(matches!(
        outcome,
        CommandOutcome::Failed(CommandError::StaleCommand)
    ));
}

#[test]
fn producer_replacement_is_atomic_on_validation_failure() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    producer
        .replace(vec![registration("/old", TargetCapabilities::NONE)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let generation = registry.snapshot_for(&target).unwrap().generation();
    let invalid = positional_registration(
        "/invalid",
        Arc::from([
            PositionalArgument::optional("first", ArgumentKind::String),
            PositionalArgument::required("second", ArgumentKind::String),
        ]),
        Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
    );

    assert!(matches!(
        producer.replace(vec![registration("/new", TargetCapabilities::NONE), invalid]),
        Err(RegistrationError::InvalidArgumentOrder(name)) if name.as_ref() == "second"
    ));

    let snapshot = registry.snapshot_for(&target).unwrap();
    assert_eq!(snapshot.generation(), generation);
    assert_eq!(snapshot.commands().len(), 1);
    assert_eq!(snapshot.commands()[0].spec().name.as_ref(), "/old");
    assert!(registry.resolve_for(&target, "/old").is_ok());
    assert!(registry.resolve_for(&target, "/new").is_err());
}

#[test]
fn winner_selection_is_deterministic_and_shared_with_projection() {
    let precedence_registry = CommandRegistry::new();
    let precedence_target =
        precedence_registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    for precedence in [
        ProducerPrecedence::Builtin,
        ProducerPrecedence::Application,
        ProducerPrecedence::Mcp,
        ProducerPrecedence::Plugin,
    ] {
        let producer = precedence_registry.create_producer(precedence);
        producer
            .replace(vec![registration("/precedence", TargetCapabilities::NONE)])
            .unwrap();
        assert_eq!(
            precedence_registry
                .resolve_for(&precedence_target, "/precedence")
                .unwrap()
                .producer_id(),
            producer.id()
        );
    }

    let registry = CommandRegistry::new();
    let alias = registry.create_producer(ProducerPrecedence::Plugin);
    alias
        .replace(vec![registration_with(
            "/alias-owner",
            &["/shared"],
            Arc::new(OutcomeBehavior(CommandOutcome::Completed)),
            TargetCapabilities::NONE,
        )])
        .unwrap();
    let first = registry.create_producer(ProducerPrecedence::Plugin);
    first
        .replace(vec![registration("/shared", TargetCapabilities::NONE)])
        .unwrap();
    let second = registry.create_producer(ProducerPrecedence::Plugin);
    second
        .replace(vec![registration("/shared", TargetCapabilities::NONE)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));

    let winner = registry.resolve_for(&target, "/shared").unwrap();
    assert_eq!(winner.producer_id(), first.id());
    let projected = registry
        .snapshot_for(&target)
        .unwrap()
        .commands()
        .iter()
        .find(|command| command.invoked_name() == "/shared")
        .unwrap()
        .clone();
    assert_eq!(projected.producer_id(), winner.producer_id());

    assert!(first.remove());
    assert_eq!(
        registry
            .resolve_for(&target, "/shared")
            .unwrap()
            .producer_id(),
        second.id()
    );
    assert!(second.remove());
    assert_eq!(
        registry
            .resolve_for(&target, "/shared")
            .unwrap()
            .producer_id(),
        alias.id()
    );
}

#[test]
fn dispatch_rejects_foreign_command_and_target_without_execution() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let executions = Arc::new(AtomicU64::new(0));
    producer
        .replace(vec![registration_with(
            "/local",
            &[],
            Arc::new(CountingBehavior(Arc::clone(&executions))),
            TargetCapabilities::NONE,
        )])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let command = registry.resolve_for(&target, "/local").unwrap();
    let foreign_registry = CommandRegistry::new();
    let foreign_producer = foreign_registry.create_producer(ProducerPrecedence::Application);
    foreign_producer
        .replace(vec![registration("/foreign", TargetCapabilities::NONE)])
        .unwrap();
    let foreign_target = foreign_registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let foreign_command = foreign_registry
        .resolve_for(&foreign_target, "/foreign")
        .unwrap();

    assert!(matches!(
        futures_lite::future::block_on(registry.dispatch_input(&foreign_target, "/local".into())),
        InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::StaleTarget))
    ));
    assert!(matches!(
        futures_lite::future::block_on(registry.dispatch_command(
            &foreign_target,
            command,
            Arc::from(""),
            "/local".into(),
        )),
        CommandOutcome::Failed(CommandError::StaleTarget)
    ));
    assert!(matches!(
        futures_lite::future::block_on(registry.dispatch_command(
            &target,
            foreign_command,
            Arc::from(""),
            "/foreign".into(),
        )),
        CommandOutcome::Failed(CommandError::StaleCommand)
    ));
    assert_eq!(executions.load(Ordering::Relaxed), 0);
}

#[test]
fn subscription_reports_final_generation() {
    let registry = CommandRegistry::new();
    let subscription = registry.subscribe();
    let initial = subscription.generation();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    producer
        .replace(vec![registration("/first", TargetCapabilities::NONE)])
        .unwrap();
    producer
        .replace(vec![registration("/second", TargetCapabilities::NONE)])
        .unwrap();

    let generation = futures_lite::future::block_on(subscription.changed(initial));
    assert_eq!(generation, subscription.generation());
    assert!(generation > initial);
}

#[test]
fn subscriber_waker_reenters_registry_after_lock_release() {
    let registry = CommandRegistry::new();
    let subscription = registry.subscribe();
    let initial = subscription.generation();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let (woke_tx, woke_rx) = mpsc::sync_channel(1);
    let mut changed = subscription.changed(initial);
    let waker = Waker::from(Arc::new(ReentrantRegistryWaker {
        registry: registry.clone(),
        woke: woke_tx,
    }));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(changed.as_mut().poll(&mut context), Poll::Pending));
    let (done_tx, done_rx) = mpsc::sync_channel(1);

    let worker = thread::spawn(move || {
        let result = producer.replace(vec![registration("/changed", TargetCapabilities::NONE)]);
        done_tx.send(result).unwrap();
    });

    woke_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();
    done_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap().unwrap();
    worker.join().unwrap();
    assert!(matches!(
        changed.as_mut().poll(&mut context),
        Poll::Ready(_)
    ));
}

#[test]
fn replacement_publishes_only_after_sessions_are_stale() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let session = completion_session(&registry, &producer, Arc::new(CompletionProbe::default()));
    let command = session.command().clone();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let initial = registry.snapshot_for(&target).unwrap();
    let session_lock = session
        .owner
        .core
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let producer_for_thread = producer.clone();
    let worker = thread::spawn(move || producer_for_thread.replace(Vec::new()));

    let deadline = std::time::Instant::now() + LOCK_RELEASE_TIMEOUT;
    loop {
        let state = registry
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.producers[0].generation % 2 == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replacement did not enter its invalidation phase"
        );
        drop(state);
        thread::yield_now();
    }
    let snapshot = registry.snapshot_for(&target).unwrap();
    assert_eq!(snapshot.generation(), initial.generation());
    assert_eq!(snapshot.commands()[0].command_id(), command.command_id());
    let late_subscription = registry.subscribe();
    assert_eq!(late_subscription.generation(), initial.generation());
    assert!(matches!(
        registry.open_completion(command, target.id()),
        Err(CompletionError::StaleCommand)
    ));

    drop(session_lock);
    worker.join().unwrap().unwrap();
    assert!(late_subscription.generation() > initial.generation());
    assert!(
        registry
            .snapshot_for(&target)
            .unwrap()
            .commands()
            .is_empty()
    );
}

#[test]
fn subscriber_observes_stale_session_at_new_generation() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let session = completion_session(&registry, &producer, Arc::new(CompletionProbe::default()));
    let subscription = registry.subscribe();
    let initial = subscription.generation();
    let mut changed = subscription.changed(initial);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let waker = Waker::from(Arc::new(CompletionSessionWaker {
        session,
        result: result_tx,
    }));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(changed.as_mut().poll(&mut context), Poll::Pending));

    producer.replace(Vec::new()).unwrap();

    assert_eq!(
        result_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap(),
        CompletionResult::Stale
    );
    assert!(matches!(
        changed.as_mut().poll(&mut context),
        Poll::Ready(generation) if generation > initial
    ));
}

#[test]
fn completion_invalidation_detaches_without_locking_sessions() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let session = completion_session(&registry, &producer, Arc::new(CompletionProbe::default()));
    let session_lock = session
        .owner
        .core
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let registry_for_thread = registry.clone();
    let producer_id = producer.id();
    let (detached_tx, detached_rx) = mpsc::sync_channel(1);

    let worker = thread::spawn(move || {
        let invalidations = registry_for_thread
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .invalidate_completion_sessions(producer_id);
        detached_tx.send(invalidations).unwrap();
    });

    let invalidations = detached_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();
    drop(session_lock);
    worker.join().unwrap();
    for callback in invalidations
        .into_iter()
        .filter_map(CompletionInvalidation::prepare)
    {
        callback.call().unwrap();
    }
    assert!(matches!(
        futures_lite::future::block_on(session.complete(
            Arc::from(""),
            Arc::from(""),
            0,
            Arc::from("insert"),
        )),
        CompletionResult::Stale
    ));
}

#[test]
fn producer_replacement_cancels_completion_once_and_rejects_reuse() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let probe = Arc::new(CompletionProbe::default());
    let session = completion_session(&registry, &producer, probe.clone());
    let candidate = complete_once(&session);

    producer.replace(Vec::new()).unwrap();

    assert!(matches!(
        futures_lite::future::block_on(session.complete(
            Arc::from(""),
            Arc::from(""),
            0,
            Arc::from("insert"),
        )),
        CompletionResult::Stale
    ));
    assert!(matches!(
        session.highlight(&candidate),
        Err(CompletionError::StaleSession)
    ));
    assert!(matches!(
        session.accept(candidate),
        Err(CompletionError::StaleSession)
    ));
    assert_eq!(probe.completions.load(Ordering::Relaxed), 1);
    assert_eq!(
        *probe
            .events
            .lock()
            .unwrap_or_else(|error| error.into_inner()),
        [CompletionLifecycleEvent::Cancel]
    );
    session.cancel().unwrap();
}

#[test]
fn producer_removal_cancels_completion_outside_registry_lock() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let probe = Arc::new(CompletionProbe {
        reenter: Some(registry.clone()),
        ..CompletionProbe::default()
    });
    let session = completion_session(&registry, &producer, probe.clone());
    drop(complete_once(&session));

    assert!(producer.remove());

    assert_eq!(
        *probe
            .events
            .lock()
            .unwrap_or_else(|error| error.into_inner()),
        [CompletionLifecycleEvent::Cancel]
    );
    assert!(!producer.remove());
}

#[test]
fn superseded_completion_cannot_return_items() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let session = completion_session(
        &registry,
        &producer,
        Arc::new(FirstRequestBlockingCompletion {
            entered: entered_tx,
            release: Mutex::new(release_rx),
        }),
    );
    let first_session = session.clone();
    let first = thread::spawn(move || {
        futures_lite::future::block_on(first_session.complete(
            Arc::from("a"),
            Arc::from("a"),
            0,
            Arc::from("insert"),
        ))
    });
    entered_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();

    let second = futures_lite::future::block_on(session.complete(
        Arc::from("ab"),
        Arc::from("ab"),
        0,
        Arc::from("insert"),
    ));
    release_tx.send(()).unwrap();

    assert!(matches!(second, CompletionResult::Items(_)));
    assert_eq!(first.join().unwrap(), CompletionResult::Cancelled);
}

#[test]
fn invalidating_in_flight_completion_returns_cancelled_then_stale() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let completion = Arc::new(BlockingCompletion {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        events: Mutex::new(Vec::new()),
    });
    let session = completion_session(&registry, &producer, completion.clone());
    let session_for_thread = session.clone();
    let worker = thread::spawn(move || {
        futures_lite::future::block_on(session_for_thread.complete(
            Arc::from(""),
            Arc::from(""),
            0,
            Arc::from("insert"),
        ))
    });
    entered_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();

    producer.replace(Vec::new()).unwrap();
    release_tx.send(()).unwrap();

    assert_eq!(worker.join().unwrap(), CompletionResult::Cancelled);
    assert!(matches!(
        futures_lite::future::block_on(session.complete(
            Arc::from(""),
            Arc::from(""),
            0,
            Arc::from("insert"),
        )),
        CompletionResult::Stale
    ));
    assert_eq!(
        *completion
            .events
            .lock()
            .unwrap_or_else(|error| error.into_inner()),
        [CompletionLifecycleEvent::Cancel]
    );
}

#[test]
fn final_session_owner_drop_cancels_once() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let probe = Arc::new(CompletionProbe::default());
    let session = completion_session(&registry, &producer, probe.clone());
    let clone = session.clone();
    complete_once(&session);

    drop(session);
    assert!(
        probe
            .events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
    );
    drop(clone);

    assert_eq!(
        *probe
            .events
            .lock()
            .unwrap_or_else(|error| error.into_inner()),
        vec![CompletionLifecycleEvent::Cancel]
    );
}

#[test]
fn cancelled_completion_session_reopens_with_fresh_id() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let completion = Arc::new(CompletionProbe::default());
    let session = completion_session(&registry, &producer, completion.clone());
    let first = session.id();
    session.cancel().unwrap();

    let reopened = completion_session(&registry, &producer, completion);
    let second = reopened.id();

    assert_ne!(first, second);
}

#[test]
fn final_owner_drop_cancels_in_flight_completion() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let completion = Arc::new(BlockingCompletion {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        events: Mutex::new(Vec::new()),
    });
    let session = completion_session(&registry, &producer, completion.clone());
    let future = session.complete(Arc::from(""), Arc::from(""), 0, Arc::from("insert"));
    let worker = thread::spawn(move || futures_lite::future::block_on(future));
    entered_rx.recv_timeout(LOCK_RELEASE_TIMEOUT).unwrap();

    drop(session);
    release_tx.send(()).unwrap();

    assert_eq!(worker.join().unwrap(), CompletionResult::Cancelled);
    assert_eq!(
        *completion
            .events
            .lock()
            .unwrap_or_else(|error| error.into_inner()),
        vec![CompletionLifecycleEvent::Cancel]
    );
}

#[test]
fn accepting_completion_is_terminal_and_fires_once() {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    let probe = Arc::new(CompletionProbe::default());
    let session = completion_session(&registry, &producer, probe.clone());
    let candidate = complete_once(&session);
    let duplicate = candidate.clone();

    session.accept(candidate).unwrap();

    assert!(matches!(
        session.accept(duplicate),
        Err(CompletionError::StaleSession)
    ));
    assert!(matches!(
        futures_lite::future::block_on(session.complete(
            Arc::from(""),
            Arc::from(""),
            0,
            Arc::from("insert"),
        )),
        CompletionResult::Stale
    ));
    assert!(matches!(
        probe
            .events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_slice(),
        [CompletionLifecycleEvent::Accept(item)] if item.insertion.as_ref() == "candidate"
    ));
}

#[test_case("/lmao" => SlashClass::Command("/lmao") ; "single slash is a command")]
#[test_case(" /lmao" => SlashClass::Command("/lmao") ; "leading whitespace is trimmed")]
#[test_case("//lmao" => SlashClass::EscapedLiteral("/lmao") ; "double slash strips one slash")]
#[test_case("///lmao" => SlashClass::EscapedLiteral("//lmao") ; "triple slash strips one slash")]
#[test_case("//" => SlashClass::EscapedLiteral("/") ; "bare double slash")]
#[test_case("prose" => SlashClass::Plain ; "prose is plain")]
#[test_case("" => SlashClass::Plain ; "empty is plain")]
#[test_case("\u{200B}/lmao" => SlashClass::Plain ; "invisible prefix is not trimmed")]
fn classify_input_cases(input: &str) -> SlashClass<'_> {
    classify_input(input)
}

#[test_case("/lmao" => matches InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(name))) if name.as_ref() == "/lmao" ; "unknown slash is rejected with its name")]
#[test_case(" /lmao" => matches InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(name))) if name.as_ref() == "/lmao" ; "leading whitespace rejected with trimmed name")]
#[test_case("/" => matches InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(name))) if name.as_ref() == "/" ; "bare slash is rejected")]
#[test_case("//lmao" => matches InputDispatch::LiteralInput(content) if content.text.as_ref() == "/lmao" ; "escaped literal strips exactly one slash")]
#[test_case("///lmao" => matches InputDispatch::LiteralInput(content) if content.text.as_ref() == "//lmao" ; "triple slash strips exactly one slash")]
#[test_case(" //lmao" => matches InputDispatch::LiteralInput(content) if content.text.as_ref() == "/lmao" ; "leading whitespace escaped literal is trimmed")]
#[test_case("literal input" => matches InputDispatch::LiteralInput(content) if content.text.as_ref() == "literal input" ; "prose stays literal")]
#[test_case("/real" => matches InputDispatch::Dispatched(CommandOutcome::Completed) ; "registered command still dispatches")]
#[test_case("//real" => matches InputDispatch::LiteralInput(content) if content.text.as_ref() == "/real" ; "escaped registered command stays literal")]
fn dispatch_input_classifies_and_rejects(text: &str) -> InputDispatch {
    let registry = CommandRegistry::new();
    let producer = registry.create_producer(ProducerPrecedence::Application);
    producer
        .replace(vec![registration("/real", TargetCapabilities::NONE)])
        .unwrap();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    futures_lite::future::block_on(registry.dispatch_input(&target, text.into()))
}

#[test]
fn escaped_literal_preserves_attachments() {
    let registry = CommandRegistry::new();
    let target = registry.bind_target(TargetCapabilities::NONE, Arc::new(Host));
    let attachments = Arc::from([CommandAttachment {
        media_type: Arc::from("image/png"),
        data: Arc::from("bytes"),
    }]);
    let content = CommandContent {
        text: Arc::from("//lmao"),
        attachments,
    };

    match futures_lite::future::block_on(registry.dispatch_input(&target, content)) {
        InputDispatch::LiteralInput(literal) => {
            assert_eq!(literal.text.as_ref(), "/lmao");
            assert_eq!(literal.attachments.len(), 1);
            assert_eq!(literal.attachments[0].media_type.as_ref(), "image/png");
            assert_eq!(literal.attachments[0].data.as_ref(), "bytes");
        }
        other => panic!("expected LiteralInput, got {other:?}"),
    }
}
