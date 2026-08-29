use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use super::*;
use crate::completion::CompletionInvalidation;

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
            arguments: ArgumentArity::ANY,
            docs: CommandDocs {
                summary: Arc::from("test command"),
                argument_hint: None,
            },
            required_capabilities: capabilities,
        },
        behavior,
        completion: None,
    }
}

fn completion_registration(completion: Arc<dyn CommandCompletion>) -> Registration {
    Registration {
        completion: Some(completion),
        ..registration("/complete", TargetCapabilities::NONE)
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
        InputDispatch::LiteralInput(_)
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
    let mut invalid = registration("/invalid", TargetCapabilities::NONE);
    invalid.spec.arguments = ArgumentArity::bounded(2, 1);

    assert!(matches!(
        producer.replace(vec![
            registration("/new", TargetCapabilities::NONE),
            invalid
        ]),
        Err(RegistrationError::InvalidArgumentArity { min: 2, max: 1 })
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
        InputDispatch::LiteralInput(_)
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
