use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use thiserror::Error;

use crate::dispatch::ResolvedCommand;
use crate::registry::RegistryInner;
use crate::spec::{CommandFuture, CommandId, CompletionSessionId, InvocationTargetId, ProducerId};

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
    provider: Arc<dyn CommandCompletion>,
    target_id: InvocationTargetId,
    next_request: u64,
    current_request: Option<CurrentCompletionRequest>,
    closed: bool,
}

struct CurrentCompletionRequest {
    id: u64,
    context: CompletionContext,
    cancellation: CancellationToken,
}

pub(super) struct CompletionCallback {
    provider: Arc<dyn CommandCompletion>,
    context: CompletionContext,
    event: CompletionLifecycleEvent,
    cancellation: CancellationToken,
}

pub(super) struct CompletionInvalidation {
    pub(super) session: Arc<CompletionSessionCore>,
}

impl CompletionCallback {
    pub(super) fn call(self) -> Result<(), CompletionError> {
        self.provider
            .lifecycle(&self.context, &self.event, &self.cancellation)
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
        Some(CompletionCallback {
            provider: Arc::clone(&self.provider),
            context: current.context,
            event: CompletionLifecycleEvent::Cancel,
            cancellation: current.cancellation,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionContext {
    pub command_id: CommandId,
    pub canonical_name: Arc<str>,
    pub invoked_name: Arc<str>,
    pub arguments: Arc<str>,
    pub argument: Arc<str>,
    pub argument_index: usize,
    pub mode: Arc<str>,
    pub target_id: InvocationTargetId,
    pub session_id: CompletionSessionId,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionItem {
    pub label: Arc<str>,
    pub insertion: Arc<str>,
    pub description: Option<Arc<str>>,
}

pub trait CommandCompletion: Send + Sync + 'static {
    fn complete(
        &self,
        context: CompletionContext,
        cancellation: CancellationToken,
    ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>>;

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
}

impl CompletionCandidate {
    pub fn item(&self) -> &CompletionItem {
        &self.item
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

#[derive(Clone)]
pub struct CompletionSession {
    pub(super) command: ResolvedCommand,
    pub(super) target_id: InvocationTargetId,
    pub(super) owner: Arc<CompletionSessionOwner>,
}

impl CompletionSession {
    pub(super) fn new(
        id: CompletionSessionId,
        producer_id: ProducerId,
        registry: Weak<RegistryInner>,
        command: ResolvedCommand,
        provider: Arc<dyn CommandCompletion>,
        target_id: InvocationTargetId,
    ) -> Self {
        let core = Arc::new(CompletionSessionCore {
            id,
            producer_id,
            registry,
            state: Mutex::new(CompletionSessionState {
                command: command.clone(),
                provider,
                target_id,
                next_request: 0,
                current_request: None,
                closed: false,
            }),
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
        let core = Arc::clone(&self.owner.core);
        Box::pin(async move {
            let (provider, context, cancellation, request_id) = {
                let mut state = core.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.closed {
                    return CompletionResult::Stale;
                }
                let request_id = state.next_request;
                state.next_request += 1;
                if let Some(current) = state.current_request.take() {
                    current.cancellation.cancel();
                }
                let cancellation = CancellationToken::default();
                let context = CompletionContext {
                    command_id: state.command.command_id(),
                    canonical_name: Arc::clone(&state.command.spec().name),
                    invoked_name: Arc::from(state.command.invoked_name()),
                    arguments,
                    argument,
                    argument_index,
                    mode,
                    target_id: state.target_id,
                    session_id: core.id,
                    generation: request_id,
                };
                state.current_request = Some(CurrentCompletionRequest {
                    id: request_id,
                    context: context.clone(),
                    cancellation: cancellation.clone(),
                });
                (
                    Arc::clone(&state.provider),
                    context,
                    cancellation,
                    request_id,
                )
            };
            match provider.complete(context, cancellation).await {
                Ok(items) => {
                    let state = core.state.lock().unwrap_or_else(|error| error.into_inner());
                    if state.closed
                        || state
                            .current_request
                            .as_ref()
                            .is_none_or(|current| current.id != request_id)
                    {
                        return CompletionResult::Cancelled;
                    }
                    let candidates = items
                        .into_iter()
                        .map(|item| CompletionCandidate {
                            item,
                            session_id: core.id,
                            request_id,
                        })
                        .collect();
                    CompletionResult::Items(candidates)
                }
                Err(
                    CompletionError::StaleCommand
                    | CompletionError::StaleSession
                    | CompletionError::StaleRequest,
                ) => CompletionResult::Stale,
                Err(CompletionError::Unavailable) => CompletionResult::Failed,
            }
        })
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
            let current = state
                .current_request
                .take()
                .ok_or(CompletionError::StaleRequest)?;
            if current.id != candidate.request_id {
                state.current_request = Some(current);
                return Err(CompletionError::StaleRequest);
            }
            state.closed = true;
            CompletionCallback {
                provider: Arc::clone(&state.provider),
                context: current.context,
                event: CompletionLifecycleEvent::Accept(candidate.item),
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
            if current.id != candidate.request_id {
                return Err(CompletionError::StaleRequest);
            }
            CompletionCallback {
                provider: Arc::clone(&state.provider),
                context: current.context.clone(),
                event,
                cancellation: current.cancellation.clone(),
            }
        };
        callback.call()
    }
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
