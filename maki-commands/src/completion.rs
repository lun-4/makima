use std::collections::HashSet;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use thiserror::Error;

use crate::arguments::{
    ArgumentKind, CommandArguments, CompletionEdit, CompletionPolicy, ParsedArgument, QuoteStyle,
    encode_completion_value, parse_completion_prefix_arguments,
};
use crate::completion_providers::CompletionProviders;
use crate::dispatch::ResolvedCommand;
use crate::registry::{RegistryInner, normalize};
use crate::spec::{CommandFuture, CommandId, CompletionSessionId, InvocationTargetId, ProducerId};

pub const MAX_COMPLETION_CANDIDATES: usize = 640;

pub(super) struct CompletionSessionCore {
    pub(super) id: CompletionSessionId,
    pub(super) producer_id: ProducerId,
    registry: Weak<RegistryInner>,
    pub(super) state: Mutex<CompletionSessionState>,
}

pub(super) struct CompletionSessionOwner {
    pub(super) core: Arc<CompletionSessionCore>,
}

pub(super) struct CompletionSessionState {
    command: ResolvedCommand,
    argument_completions: Vec<Option<Arc<dyn CommandCompletion>>>,
    defaults: CompletionProviders,
    target_id: InvocationTargetId,
    cwd: Arc<str>,
    next_request: u64,
    current_request: Option<CurrentCompletionRequest>,
    revision: u64,
    closed: bool,
}

struct CurrentCompletionRequest {
    id: u64,
    context: CompletionContext,
    cancellation: CancellationToken,
    providers: Vec<RequestProvider>,
    snapshot: Option<CompletionSnapshot>,
    sink: Option<CompletionSnapshotSink>,
}

#[derive(Clone)]
struct RequestProvider {
    provider: Arc<dyn CommandCompletion>,
    source: CompletionSource,
    items: Vec<CompletionItem>,
    finished: bool,
    terminated: bool,
}

pub(super) struct CompletionCallback {
    providers: Vec<(Arc<dyn CommandCompletion>, CompletionLifecycleEvent)>,
    context: CompletionContext,
    cancellation: CancellationToken,
}

pub(super) struct CompletionInvalidation {
    pub(super) session: Arc<CompletionSessionCore>,
}

impl CompletionCallback {
    pub(super) fn call(self) -> Result<(), CompletionError> {
        let mut result = Ok(());
        for (provider, event) in self.providers {
            if let Err(error) = provider.lifecycle(&self.context, &event, &self.cancellation) {
                result = Err(error);
            }
        }
        result
    }
}

impl CompletionInvalidation {
    pub(super) fn prepare(self) -> Option<CompletionCallback> {
        self.session
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close()
    }
}

impl CompletionSessionState {
    fn close(&mut self) -> Option<CompletionCallback> {
        if self.closed {
            return None;
        }
        self.closed = true;
        let current = self.current_request.take()?;
        current.cancellation.cancel();
        let providers = current
            .providers
            .into_iter()
            .filter_map(|mut entry| {
                if entry.terminated {
                    None
                } else {
                    entry.terminated = true;
                    Some((entry.provider, CompletionLifecycleEvent::Cancel))
                }
            })
            .collect();
        Some(CompletionCallback {
            providers,
            context: current.context,
            cancellation: current.cancellation,
        })
    }

    fn cancel_current(&mut self) -> Option<CompletionCallback> {
        let current = self.current_request.take()?;
        current.cancellation.cancel();
        let providers = current
            .providers
            .into_iter()
            .filter_map(|mut entry| {
                if entry.terminated {
                    None
                } else {
                    entry.terminated = true;
                    Some((entry.provider, CompletionLifecycleEvent::Cancel))
                }
            })
            .collect();
        Some(CompletionCallback {
            providers,
            context: current.context,
            cancellation: current.cancellation,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionNavigation {
    Stay,
    NextArgument,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionContext {
    pub command_id: CommandId,
    pub canonical_name: Arc<str>,
    pub invoked_name: Arc<str>,
    pub arguments: Arc<str>,
    pub argument: Arc<str>,
    pub argument_index: usize,
    pub argument_name: Option<Arc<str>>,
    pub argument_kind: Option<ArgumentKind>,
    pub argument_range: Option<Range<usize>>,
    pub preceding_arguments: Arc<[ParsedArgument]>,
    pub completion_policy: CompletionPolicy,
    pub enum_default: Option<Arc<str>>,
    pub next_argument_index: Option<usize>,
    pub navigation: CompletionNavigation,
    pub mode: Arc<str>,
    pub target_id: InvocationTargetId,
    pub cwd: Arc<str>,
    pub session_id: CompletionSessionId,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionItem {
    pub label: Arc<str>,
    pub insertion: Arc<str>,
    pub description: Option<Arc<str>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionItemNavigation {
    Terminal,
    Directory,
}

struct EnumCompletion;

impl CommandCompletion for EnumCompletion {
    fn complete(
        &self,
        context: CompletionContext,
        _cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
        let items = context
            .argument_kind
            .as_ref()
            .and_then(ArgumentKind::enum_choices)
            .map(|choices| {
                choices
                    .iter()
                    .map(|choice| CompletionItem {
                        label: Arc::clone(choice),
                        insertion: Arc::clone(choice),
                        description: None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        Box::pin(async move { Ok(items) })
    }
}

fn enum_completion() -> Arc<dyn CommandCompletion> {
    Arc::new(EnumCompletion)
}

impl CompletionItem {
    pub fn edit(&self, range: Range<usize>, quote_style: QuoteStyle) -> CompletionEdit {
        encode_completion_value(&self.insertion, range, quote_style)
    }
}

type CompletionArgumentMetadata = (
    Option<Arc<str>>,
    Option<ArgumentKind>,
    Arc<[ParsedArgument]>,
    CompletionPolicy,
    Option<Arc<str>>,
    Option<usize>,
    CompletionNavigation,
);

fn completion_argument_metadata(
    command: &ResolvedCommand,
    arguments: &str,
    argument_index: usize,
    argument_range: Option<&Range<usize>>,
) -> CompletionArgumentMetadata {
    let CommandArguments::Positional(schema) = &command.spec().arguments else {
        return (
            None,
            None,
            Arc::from([]),
            CompletionPolicy::Default,
            None,
            None,
            CompletionNavigation::Stay,
        );
    };
    let descriptor = schema
        .get(argument_index)
        .or_else(|| schema.last().filter(|argument| argument.variadic));
    let preceding_arguments =
        parse_completion_prefix_arguments(arguments, schema, argument_index, argument_range);
    let next_argument_index = (argument_index + 1 < schema.len()).then_some(argument_index + 1);
    let navigation = if next_argument_index.is_some() {
        CompletionNavigation::NextArgument
    } else {
        CompletionNavigation::Close
    };
    (
        descriptor.map(|argument| Arc::clone(&argument.name)),
        descriptor.map(|argument| argument.kind.clone()),
        preceding_arguments,
        descriptor.map_or(CompletionPolicy::Default, |argument| argument.completion),
        descriptor.and_then(|argument| argument.kind.default_value().map(Arc::from)),
        next_argument_index,
        navigation,
    )
}

pub trait CommandCompletion: Send + Sync + 'static {
    fn complete_incremental(
        &self,
        context: CompletionContext,
        cancellation: CancellationToken,
        publisher: CompletionPublisher,
    ) -> CommandFuture<Result<(), CompletionError>> {
        let future = self.complete(context, cancellation);
        Box::pin(async move {
            publisher.publish(future.await?)?;
            publisher.finish()?;
            Ok(())
        })
    }

    fn complete(
        &self,
        context: CompletionContext,
        cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>>;

    fn navigation(
        &self,
        _context: &CompletionContext,
        _item: &CompletionItem,
    ) -> CompletionItemNavigation {
        CompletionItemNavigation::Terminal
    }

    fn lifecycle(
        &self,
        _context: &CompletionContext,
        _event: &CompletionLifecycleEvent,
        _cancellation: &CancellationToken,
    ) -> Result<(), CompletionError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionCandidate {
    item: CompletionItem,
    session_id: CompletionSessionId,
    request_id: u64,
    revision: u64,
    provider_index: usize,
    source: CompletionSource,
    navigation: CompletionItemNavigation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionSource {
    KindDefault,
    Argument(Arc<str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionSnapshot {
    pub session_id: CompletionSessionId,
    pub generation: u64,
    pub revision: u64,
    pub finished: bool,
    pub candidates: Vec<CompletionCandidate>,
}

/// Receives replacement snapshots for one completion request.
#[derive(Clone)]
pub struct CompletionSnapshotSink {
    callback: Arc<dyn Fn(CompletionSnapshot) + Send + Sync>,
}

impl CompletionSnapshotSink {
    pub fn new(callback: impl Fn(CompletionSnapshot) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    fn publish(&self, snapshot: CompletionSnapshot) {
        (self.callback)(snapshot);
    }
}

/// Publishes a replacement snapshot for one provider in one request.
#[derive(Clone)]
pub struct CompletionPublisher {
    core: Weak<CompletionSessionCore>,
    request_id: u64,
    provider_index: usize,
}

impl CompletionPublisher {
    pub fn publish(
        &self,
        items: Vec<CompletionItem>,
    ) -> Result<CompletionSnapshot, CompletionError> {
        let core = self.core.upgrade().ok_or(CompletionError::StaleSession)?;
        let (snapshot, sink) = {
            let mut state = core.state.lock().unwrap_or_else(|error| error.into_inner());
            let current = current_request(&mut state, self.request_id)?;
            let entry = current
                .providers
                .get_mut(self.provider_index)
                .ok_or(CompletionError::StaleRequest)?;
            if entry.finished || entry.terminated {
                return Err(CompletionError::StaleRequest);
            }
            entry.items = items;
            let snapshot = build_snapshot(&core, &mut state, self.request_id, false)?;
            let sink = state
                .current_request
                .as_ref()
                .and_then(|request| request.sink.clone());
            (snapshot, sink)
        };
        if let Some(sink) = sink {
            sink.publish(snapshot.clone());
        }
        Ok(snapshot)
    }

    pub fn finish(&self) -> Result<CompletionSnapshot, CompletionError> {
        self.finish_inner(None)
    }

    pub fn finish_with(
        &self,
        items: Vec<CompletionItem>,
    ) -> Result<CompletionSnapshot, CompletionError> {
        self.finish_inner(Some(items))
    }

    fn finish_inner(
        &self,
        items: Option<Vec<CompletionItem>>,
    ) -> Result<CompletionSnapshot, CompletionError> {
        let core = self.core.upgrade().ok_or(CompletionError::StaleSession)?;
        let (snapshot, sink) = {
            let mut state = core.state.lock().unwrap_or_else(|error| error.into_inner());
            let current = current_request(&mut state, self.request_id)?;
            let entry = current
                .providers
                .get_mut(self.provider_index)
                .ok_or(CompletionError::StaleRequest)?;
            if entry.terminated || entry.finished {
                return Err(CompletionError::StaleRequest);
            }
            if let Some(items) = items {
                entry.items = items;
            }
            entry.finished = true;
            let finished = current.providers.iter().all(|provider| provider.finished);
            let snapshot = build_snapshot(&core, &mut state, self.request_id, finished)?;
            let sink = state
                .current_request
                .as_ref()
                .and_then(|request| request.sink.clone());
            (snapshot, sink)
        };
        if let Some(sink) = sink {
            sink.publish(snapshot.clone());
        }
        Ok(snapshot)
    }
}

fn current_request(
    state: &mut CompletionSessionState,
    request_id: u64,
) -> Result<&mut CurrentCompletionRequest, CompletionError> {
    if state.closed {
        return Err(CompletionError::StaleSession);
    }
    let current = state
        .current_request
        .as_mut()
        .ok_or(CompletionError::StaleRequest)?;
    if current.id != request_id || current.cancellation.is_cancelled() {
        return Err(CompletionError::StaleRequest);
    }
    Ok(current)
}

fn build_snapshot(
    core: &Arc<CompletionSessionCore>,
    state: &mut CompletionSessionState,
    request_id: u64,
    finished: bool,
) -> Result<CompletionSnapshot, CompletionError> {
    let (context, providers) = {
        let current = current_request(state, request_id)?;
        (current.context.clone(), current.providers.clone())
    };
    state.revision = state.revision.wrapping_add(1);
    let revision = state.revision;
    let items = compose_items(&context, &providers);
    let candidates = items
        .into_iter()
        .map(|(provider_index, source, item)| {
            let navigation = providers[provider_index]
                .provider
                .navigation(&context, &item);
            CompletionCandidate {
                item,
                session_id: core.id,
                request_id,
                revision,
                provider_index,
                navigation,
                source,
            }
        })
        .collect();
    let snapshot = CompletionSnapshot {
        session_id: core.id,
        generation: request_id,
        revision,
        finished,
        candidates,
    };
    current_request(state, request_id)?.snapshot = Some(snapshot.clone());
    Ok(snapshot)
}

fn compose_items(
    context: &CompletionContext,
    providers: &[RequestProvider],
) -> Vec<(usize, CompletionSource, CompletionItem)> {
    let mut defaults = Vec::new();
    let mut custom = Vec::new();
    for (index, provider) in providers.iter().enumerate() {
        for item in &provider.items {
            if matches!(provider.source, CompletionSource::KindDefault) {
                defaults.push((index, provider.source.clone(), item.clone()));
            } else {
                custom.push((index, provider.source.clone(), item.clone()));
            }
        }
    }
    let custom_insertions = custom
        .iter()
        .map(|(_, _, item)| item.insertion.clone())
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    let add = |result: &mut Vec<(usize, CompletionSource, CompletionItem)>,
               seen: &mut HashSet<Arc<str>>,
               item: (usize, CompletionSource, CompletionItem)| {
        if seen.insert(item.2.insertion.clone()) {
            result.push(item);
        }
    };
    match context.completion_policy {
        CompletionPolicy::Disabled => {}
        CompletionPolicy::Replace | CompletionPolicy::Default if !custom.is_empty() => {
            for item in custom {
                add(&mut result, &mut seen, item);
            }
        }
        CompletionPolicy::Replace | CompletionPolicy::Default => {
            for item in defaults {
                add(&mut result, &mut seen, item);
            }
        }
        CompletionPolicy::Extend => {
            for item in defaults {
                if !custom_insertions.contains(&item.2.insertion) {
                    add(&mut result, &mut seen, item);
                }
            }
            for item in custom {
                add(&mut result, &mut seen, item);
            }
        }
    }
    result.truncate(MAX_COMPLETION_CANDIDATES);
    result
}

impl CompletionCandidate {
    pub fn item(&self) -> &CompletionItem {
        &self.item
    }

    pub fn navigation(&self) -> CompletionItemNavigation {
        self.navigation
    }

    pub fn source(&self) -> &CompletionSource {
        &self.source
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionLifecycleEvent {
    Highlight(CompletionItem),
    Accept(CompletionItem),
    Cancel,
}

#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionInput {
    pub arguments: Arc<str>,
    pub argument: Arc<str>,
    pub argument_index: usize,
    pub argument_range: Option<Range<usize>>,
    pub mode: Arc<str>,
}

#[derive(Clone)]
pub struct CompletionSession {
    pub(super) command: ResolvedCommand,
    pub(super) target_id: InvocationTargetId,
    pub(super) owner: Arc<CompletionSessionOwner>,
}

impl CompletionSession {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        id: CompletionSessionId,
        producer_id: ProducerId,
        registry: Weak<RegistryInner>,
        command: ResolvedCommand,
        argument_completions: Vec<Option<Arc<dyn CommandCompletion>>>,
        defaults: CompletionProviders,
        target_id: InvocationTargetId,
        cwd: Arc<str>,
    ) -> Self {
        let state = CompletionSessionState {
            command: command.clone(),
            argument_completions,
            defaults,
            target_id,
            cwd,
            next_request: 0,
            current_request: None,
            revision: 0,
            closed: false,
        };
        let core = Arc::new(CompletionSessionCore {
            id,
            producer_id,
            registry,
            state: Mutex::new(state),
        });
        Self {
            command,
            target_id,
            owner: Arc::new(CompletionSessionOwner { core }),
        }
    }

    pub(super) fn weak_core(&self) -> Weak<CompletionSessionCore> {
        Arc::downgrade(&self.owner.core)
    }

    pub fn id(&self) -> CompletionSessionId {
        self.owner.core.id
    }

    pub fn command(&self) -> &ResolvedCommand {
        &self.command
    }

    pub fn target_id(&self) -> InvocationTargetId {
        self.target_id
    }

    pub fn complete(
        &self,
        arguments: Arc<str>,
        argument: Arc<str>,
        argument_index: usize,
        mode: Arc<str>,
    ) -> CommandFuture<CompletionResult> {
        self.complete_input(CompletionInput {
            arguments,
            argument,
            argument_index,
            argument_range: None,
            mode,
        })
    }

    pub fn complete_input(&self, input: CompletionInput) -> CommandFuture<CompletionResult> {
        self.complete_input_with_sink(input, None)
    }

    pub fn complete_input_with_sink(
        &self,
        input: CompletionInput,
        sink: Option<CompletionSnapshotSink>,
    ) -> CommandFuture<CompletionResult> {
        let core = Arc::clone(&self.owner.core);
        let (context, cancellation, request_id, providers, old_callback) = {
            let mut state = core.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return Box::pin(async { CompletionResult::Stale });
            }
            let old_callback = state.cancel_current();
            let request_id = state.next_request;
            state.next_request = state.next_request.wrapping_add(1);
            let (
                argument_name,
                argument_kind,
                preceding_arguments,
                completion_policy,
                enum_default,
                next_argument_index,
                navigation,
            ) = completion_argument_metadata(
                &state.command,
                &input.arguments,
                input.argument_index,
                input.argument_range.as_ref(),
            );
            let context = CompletionContext {
                command_id: state.command.command_id(),
                canonical_name: Arc::clone(&state.command.spec().name),
                invoked_name: Arc::from(state.command.invoked_name()),
                arguments: input.arguments,
                argument: input.argument,
                argument_index: input.argument_index,
                argument_name,
                argument_kind,
                argument_range: input.argument_range,
                preceding_arguments,
                completion_policy,
                enum_default,
                next_argument_index,
                navigation,
                mode: input.mode,
                target_id: state.target_id,
                cwd: Arc::clone(&state.cwd),
                session_id: core.id,
                generation: request_id,
            };
            let providers = providers_for(&state, &context);
            let cancellation = CancellationToken::default();
            state.current_request = Some(CurrentCompletionRequest {
                id: request_id,
                context: context.clone(),
                cancellation: cancellation.clone(),
                providers: providers.clone(),
                snapshot: None,
                sink,
            });
            (context, cancellation, request_id, providers, old_callback)
        };
        if let Some(callback) = old_callback {
            let _ = callback.call();
        }
        Box::pin(async move {
            for (provider_index, entry) in providers.into_iter().enumerate() {
                if cancellation.is_cancelled() {
                    return CompletionResult::Cancelled;
                }
                let publisher = CompletionPublisher {
                    core: Arc::downgrade(&core),
                    request_id,
                    provider_index,
                };
                match entry
                    .provider
                    .complete_incremental(context.clone(), cancellation.clone(), publisher.clone())
                    .await
                {
                    Ok(()) => {
                        let needs_finish = {
                            let state =
                                core.state.lock().unwrap_or_else(|error| error.into_inner());
                            state.current_request.as_ref().is_some_and(|current| {
                                current.id == request_id
                                    && current
                                        .providers
                                        .get(provider_index)
                                        .is_some_and(|provider| !provider.finished)
                            })
                        };
                        if needs_finish {
                            let _ = publisher.finish();
                        }
                    }
                    Err(CompletionError::Unavailable) => {
                        let _ = publisher.finish();
                    }
                    Err(
                        CompletionError::StaleCommand
                        | CompletionError::StaleSession
                        | CompletionError::StaleRequest,
                    ) => {
                        return if cancellation.is_cancelled() {
                            CompletionResult::Cancelled
                        } else {
                            CompletionResult::Stale
                        };
                    }
                }
            }
            let state = core.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed
                || state
                    .current_request
                    .as_ref()
                    .is_none_or(|current| current.id != request_id)
            {
                return CompletionResult::Cancelled;
            }
            CompletionResult::Items(
                state
                    .current_request
                    .as_ref()
                    .and_then(|request| request.snapshot.as_ref())
                    .map(|snapshot| snapshot.candidates.clone())
                    .unwrap_or_default(),
            )
        })
    }

    pub fn validate(&self, candidate: &CompletionCandidate) -> Result<(), CompletionError> {
        let core = &self.owner.core;
        let registry = core
            .registry
            .upgrade()
            .ok_or(CompletionError::StaleCommand)?;
        let (command_registry_id, command_id, producer_id, target_id, invoked_name) = {
            let state = core.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed || candidate.session_id != core.id {
                return Err(CompletionError::StaleSession);
            }
            let current = state
                .current_request
                .as_ref()
                .ok_or(CompletionError::StaleRequest)?;
            if current.context.target_id != state.target_id
                || current.id != candidate.request_id
                || state.revision != candidate.revision
            {
                return Err(CompletionError::StaleRequest);
            }
            let provider = current
                .providers
                .get(candidate.provider_index)
                .ok_or(CompletionError::StaleRequest)?;
            if provider.items.iter().all(|item| item != &candidate.item) {
                return Err(CompletionError::StaleRequest);
            }
            (
                state.command.registry_id,
                state.command.command_id(),
                core.producer_id,
                state.target_id,
                normalize(state.command.invoked_name()),
            )
        };
        if command_registry_id != registry.id || target_id.0 != registry.id {
            return Err(CompletionError::StaleCommand);
        }
        let registry_state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(winner) = registry_state.winners.get(&invoked_name) else {
            return Err(CompletionError::StaleCommand);
        };
        if winner.record.command_id != command_id || winner.record.producer_id != producer_id {
            return Err(CompletionError::StaleCommand);
        }
        Ok(())
    }

    pub fn highlight(&self, candidate: &CompletionCandidate) -> Result<(), CompletionError> {
        self.lifecycle(
            candidate,
            CompletionLifecycleEvent::Highlight(candidate.item.clone()),
        )
    }

    pub fn accept(&self, candidate: CompletionCandidate) -> Result<(), CompletionError> {
        let callback = {
            let mut state = self
                .owner
                .core
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.closed || candidate.session_id != self.owner.core.id {
                return Err(CompletionError::StaleSession);
            }
            let mut current = state
                .current_request
                .take()
                .ok_or(CompletionError::StaleRequest)?;
            if current.id != candidate.request_id || state.revision != candidate.revision {
                state.current_request = Some(current);
                return Err(CompletionError::StaleRequest);
            }
            let selected = current
                .providers
                .get_mut(candidate.provider_index)
                .ok_or(CompletionError::StaleRequest)?;
            if selected.items.iter().all(|item| item != &candidate.item) {
                state.current_request = Some(current);
                return Err(CompletionError::StaleRequest);
            }
            state.closed = true;
            current.cancellation.cancel();
            let providers = current
                .providers
                .into_iter()
                .enumerate()
                .filter_map(|(index, mut entry)| {
                    if entry.terminated {
                        return None;
                    }
                    entry.terminated = true;
                    let event = if index == candidate.provider_index {
                        CompletionLifecycleEvent::Accept(candidate.item.clone())
                    } else {
                        CompletionLifecycleEvent::Cancel
                    };
                    Some((entry.provider, event))
                })
                .collect();
            CompletionCallback {
                providers,
                context: current.context,
                cancellation: current.cancellation,
            }
        };
        callback.call()
    }

    pub fn cancel(&self) -> Result<(), CompletionError> {
        let callback = self
            .owner
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        callback.map_or(Ok(()), CompletionCallback::call)
    }

    fn lifecycle(
        &self,
        candidate: &CompletionCandidate,
        event: CompletionLifecycleEvent,
    ) -> Result<(), CompletionError> {
        let callback = {
            let state = self
                .owner
                .core
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.closed || candidate.session_id != self.owner.core.id {
                return Err(CompletionError::StaleSession);
            }
            let current = state
                .current_request
                .as_ref()
                .ok_or(CompletionError::StaleRequest)?;
            if current.id != candidate.request_id || state.revision != candidate.revision {
                return Err(CompletionError::StaleRequest);
            }
            let provider = current
                .providers
                .get(candidate.provider_index)
                .ok_or(CompletionError::StaleRequest)?;
            if provider.items.iter().all(|item| item != &candidate.item) {
                return Err(CompletionError::StaleRequest);
            }
            CompletionCallback {
                providers: vec![(Arc::clone(&provider.provider), event)],
                context: current.context.clone(),
                cancellation: current.cancellation.clone(),
            }
        };
        callback.call()
    }
}

fn providers_for(
    state: &CompletionSessionState,
    context: &CompletionContext,
) -> Vec<RequestProvider> {
    if context.completion_policy == CompletionPolicy::Disabled {
        return Vec::new();
    }

    let custom = context.argument_name.as_deref().and_then(|name| {
        let descriptor_index = state
            .command
            .spec()
            .arguments
            .positional()?
            .iter()
            .position(|argument| argument.name.as_ref() == name)?;
        state
            .argument_completions
            .get(descriptor_index)
            .and_then(Option::as_ref)
            .map(|provider| {
                (
                    Arc::clone(provider),
                    CompletionSource::Argument(Arc::from(name)),
                )
            })
    });
    let default = context
        .argument_kind
        .as_ref()
        .and_then(|kind| state.defaults.get(kind))
        .or_else(|| {
            context
                .argument_kind
                .as_ref()
                .and_then(|kind| kind.enum_choices().map(|_| enum_completion()))
        })
        .map(|provider| (provider, CompletionSource::KindDefault));
    let mut providers = Vec::new();
    let mut add = |provider: Option<(Arc<dyn CommandCompletion>, CompletionSource)>| {
        if let Some((provider, source)) = provider {
            providers.push(RequestProvider {
                provider,
                source,
                items: Vec::new(),
                finished: false,
                terminated: false,
            });
        }
    };
    match context.completion_policy {
        CompletionPolicy::Replace => add(custom),
        CompletionPolicy::Default => {
            add(custom);
            add(default);
        }
        CompletionPolicy::Extend => {
            add(default);
            add(custom);
        }
        CompletionPolicy::Disabled => unreachable!("disabled completion returned early"),
    }
    providers
}

impl Drop for CompletionSessionOwner {
    fn drop(&mut self) {
        let callback = self
            .core
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        if let Some(callback) = callback {
            let _ = callback.call();
        }
    }
}

impl Drop for CompletionSessionCore {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .completion_sessions
                .remove(&self.id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionResult {
    Items(Vec<CompletionCandidate>),
    Stale,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CompletionError {
    #[error("completion is unavailable")]
    Unavailable,
    #[error("the resolved command is no longer registered")]
    StaleCommand,
    #[error("the completion session is closed")]
    StaleSession,
    #[error("the completion request is stale")]
    StaleRequest,
}
