use std::collections::{HashMap, HashSet};
use std::future::poll_fn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use crate::completion::{
    CompletionError, CompletionInvalidation, CompletionSession, CompletionSessionCore,
};
use crate::dispatch::{
    CommandError, CommandHost, ParsedInput, RegistrationError, ResolutionError, ResolvedCommand,
    ResolvedInput,
};
use crate::spec::{
    CommandFuture, CommandId, CompletionSessionId, InvocationTargetId, ProducerId, Registration,
    RegistryId, TargetCapabilities,
};

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct RegistrationRecord {
    pub(super) producer_id: ProducerId,
    pub(super) command_id: CommandId,
    pub(super) registration: Registration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProducerPrecedence {
    Plugin,
    Mcp,
    Application,
    Builtin,
}

#[derive(Clone)]
pub struct CommandRegistry(pub(super) Arc<RegistryInner>);

pub(super) struct RegistryInner {
    pub(super) id: RegistryId,
    publication: Mutex<()>,
    pub(super) state: Mutex<RegistryState>,
}

pub(super) struct RegistryState {
    next_id: u64,
    generation: u64,
    pub(super) producers: Vec<ProducerSlot>,
    pub(super) winners: HashMap<String, Winner>,
    projection: Arc<[ResolvedCommand]>,
    targets: HashMap<InvocationTargetId, TargetRecord>,
    subscribers: Vec<Weak<SubscriptionCore>>,
    standard_commands_registered: bool,
    pub(super) completion_sessions: HashMap<CompletionSessionId, Weak<CompletionSessionCore>>,
}

pub(super) struct TargetRecord {
    pub(super) capabilities: TargetCapabilities,
    pub(super) host: Arc<dyn CommandHost>,
}

struct TargetCore {
    id: InvocationTargetId,
    registry: Weak<RegistryInner>,
}

#[derive(Clone)]
pub struct TargetHandle(Arc<TargetCore>);

struct SubscriptionCore {
    generation: AtomicU64,
    waker: Mutex<Option<Waker>>,
}

#[derive(Clone)]
pub struct RegistrySubscription(Arc<SubscriptionCore>);

pub(super) struct ProducerSlot {
    id: ProducerId,
    precedence: ProducerPrecedence,
    creation_order: u64,
    records: Vec<Arc<RegistrationRecord>>,
    pub(super) generation: u64,
}

#[derive(Clone)]
pub(super) struct Winner {
    pub(super) record: Arc<RegistrationRecord>,
    canonical: bool,
    precedence: ProducerPrecedence,
    creation_order: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentedCommand {
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub argument_hint: Option<Arc<str>>,
}

impl From<&ResolvedCommand> for PresentedCommand {
    fn from(command: &ResolvedCommand) -> Self {
        Self {
            name: Arc::from(command.invoked_name()),
            description: Arc::clone(&command.spec().docs.summary),
            argument_hint: command.spec().docs.argument_hint.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegistrySnapshot {
    generation: u64,
    commands: Arc<[ResolvedCommand]>,
}

impl RegistrySnapshot {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn commands(&self) -> &[ResolvedCommand] {
        &self.commands
    }
}

#[derive(Clone)]
pub struct Producer {
    registry: Weak<RegistryInner>,
    id: ProducerId,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self(Arc::new(RegistryInner {
            id: RegistryId(NEXT_REGISTRY_ID.fetch_add(1, Ordering::Relaxed)),
            publication: Mutex::new(()),
            state: Mutex::new(RegistryState {
                next_id: 1,
                generation: 0,
                producers: Vec::new(),
                winners: HashMap::new(),
                projection: Arc::from([]),
                targets: HashMap::new(),
                subscribers: Vec::new(),
                standard_commands_registered: false,
                completion_sessions: HashMap::new(),
            }),
        }))
    }

    pub fn create_producer(&self, precedence: ProducerPrecedence) -> Producer {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let id = ProducerId::new(self.0.id, state.take_id());
        let creation_order = state.next_id;
        state.producers.push(ProducerSlot {
            id,
            precedence,
            creation_order,
            records: Vec::new(),
            generation: 0,
        });
        Producer {
            registry: Arc::downgrade(&self.0),
            id,
        }
    }

    pub fn bind_target(
        &self,
        capabilities: TargetCapabilities,
        host: Arc<dyn CommandHost>,
    ) -> TargetHandle {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let id = InvocationTargetId::new(self.0.id, state.take_id());
        state
            .targets
            .insert(id, TargetRecord { capabilities, host });
        TargetHandle(Arc::new(TargetCore {
            id,
            registry: Arc::downgrade(&self.0),
        }))
    }

    pub fn claim_standard_commands(&self) -> bool {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.standard_commands_registered {
            return false;
        }
        state.standard_commands_registered = true;
        true
    }

    pub fn release_standard_commands(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .standard_commands_registered = false;
    }

    pub fn subscribe(&self) -> RegistrySubscription {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let core = Arc::new(SubscriptionCore {
            generation: AtomicU64::new(state.generation),
            waker: Mutex::new(None),
        });
        state.subscribers.push(Arc::downgrade(&core));
        RegistrySubscription(core)
    }

    pub fn open_completion(
        &self,
        command: ResolvedCommand,
        target_id: InvocationTargetId,
    ) -> Result<CompletionSession, CompletionError> {
        if command.registry_id != self.0.id || target_id.0 != self.0.id {
            return Err(CompletionError::StaleCommand);
        }
        let provider = command.completion().ok_or(CompletionError::Unavailable)?;
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state
            .producers
            .iter()
            .find(|producer| {
                producer.id == command.producer_id()
                    && producer.generation.is_multiple_of(2)
                    && producer
                        .records
                        .iter()
                        .any(|record| record.command_id == command.command_id())
            })
            .ok_or(CompletionError::StaleCommand)?;
        let id = CompletionSessionId::new(self.0.id, state.take_id());
        let session = CompletionSession::new(
            id,
            command.producer_id(),
            Arc::downgrade(&self.0),
            command,
            provider,
            target_id,
        );
        state.completion_sessions.insert(id, session.weak_core());
        Ok(session)
    }

    pub fn resolve_for(
        &self,
        target: &TargetHandle,
        spelling: &str,
    ) -> Result<ResolvedCommand, ResolutionError> {
        let normalized = normalize(spelling);
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let capabilities =
            target_capabilities(&state, self.0.id, target).ok_or(ResolutionError::StaleTarget)?;
        state
            .winners
            .get(&normalized)
            .filter(|winner| {
                capabilities.contains_all(winner.record.registration.spec.required_capabilities)
            })
            .map(|winner| ResolvedCommand {
                registry_id: self.0.id,
                record: Arc::clone(&winner.record),
                invoked_name: Arc::from(spelling),
            })
            .ok_or_else(|| ResolutionError::UnknownCommand(Arc::from(spelling)))
    }

    pub fn snapshot_for(&self, target: &TargetHandle) -> Result<RegistrySnapshot, CommandError> {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let capabilities =
            target_capabilities(&state, self.0.id, target).ok_or(CommandError::StaleTarget)?;
        let commands = state
            .projection
            .iter()
            .filter(|command| capabilities.contains_all(command.spec().required_capabilities))
            .cloned()
            .collect();
        Ok(RegistrySnapshot {
            generation: state.generation,
            commands,
        })
    }

    pub fn presented_commands(
        &self,
        target: &TargetHandle,
    ) -> Result<Arc<[PresentedCommand]>, CommandError> {
        Ok(self
            .snapshot_for(target)?
            .commands()
            .iter()
            .map(PresentedCommand::from)
            .collect())
    }

    pub fn resolves_input_for(&self, target: &TargetHandle, input: &str) -> bool {
        self.resolve_input_for(target, input).is_ok()
    }

    pub fn resolve_input_for(
        &self,
        target: &TargetHandle,
        input: &str,
    ) -> Result<ResolvedInput, ResolutionError> {
        let parsed = ParsedInput::parse(input)
            .ok_or_else(|| ResolutionError::UnknownCommand(Arc::from(input.trim())))?;
        Ok(ResolvedInput {
            command: self.resolve_for(target, parsed.name)?,
            arguments: Arc::from(parsed.arguments),
        })
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TargetHandle {
    pub fn id(&self) -> InvocationTargetId {
        self.0.id
    }
}

impl Drop for TargetCore {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .targets
            .remove(&self.id);
    }
}

impl RegistrySubscription {
    pub fn generation(&self) -> u64 {
        self.0.generation.load(Ordering::Acquire)
    }

    pub fn changed(&self, generation: u64) -> CommandFuture<u64> {
        let subscription = self.clone();
        Box::pin(poll_fn(move |context| {
            let current = subscription.generation();
            if current != generation {
                return Poll::Ready(current);
            }
            *subscription
                .0
                .waker
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(context.waker().clone());
            let current = subscription.generation();
            if current != generation {
                Poll::Ready(current)
            } else {
                Poll::Pending
            }
        }))
    }
}

pub(super) fn target_record<'a>(
    state: &'a RegistryState,
    registry_id: RegistryId,
    target: &TargetHandle,
) -> Option<&'a TargetRecord> {
    (target.0.id.0 == registry_id)
        .then(|| state.targets.get(&target.0.id))
        .flatten()
}

fn target_capabilities(
    state: &RegistryState,
    registry_id: RegistryId,
    target: &TargetHandle,
) -> Option<TargetCapabilities> {
    target_record(state, registry_id, target).map(|record| record.capabilities)
}

impl Producer {
    pub fn id(&self) -> ProducerId {
        self.id
    }

    pub fn replace(&self, registrations: Vec<Registration>) -> Result<(), RegistrationError> {
        let validated = validate_registrations(registrations)?;
        let registry = self
            .registry
            .upgrade()
            .ok_or(RegistrationError::StaleProducer)?;
        let publication = registry
            .publication
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let position = state
            .producers
            .iter()
            .position(|producer| producer.id == self.id)
            .ok_or(RegistrationError::StaleProducer)?;
        let records = validated
            .into_iter()
            .map(|registration| {
                Arc::new(RegistrationRecord {
                    producer_id: self.id,
                    command_id: CommandId::new(registry.id, state.take_id()),
                    registration,
                })
            })
            .collect();
        state.producers[position].generation += 1;
        let invalidations = state.invalidate_completion_sessions(self.id);
        drop(state);
        let callbacks = invalidations
            .into_iter()
            .filter_map(CompletionInvalidation::prepare)
            .collect::<Vec<_>>();
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.producers[position].records = records;
        state.producers[position].generation += 1;
        state.generation += 1;
        state.rebuild();
        let wakers = state.take_subscriber_wakers();
        drop(state);
        drop(publication);
        for callback in callbacks {
            let _ = callback.call();
        }
        for waker in wakers {
            waker.wake();
        }
        Ok(())
    }

    pub fn remove(&self) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let publication = registry
            .publication
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(position) = state
            .producers
            .iter()
            .position(|producer| producer.id == self.id)
        else {
            return false;
        };
        state.producers[position].generation += 1;
        let invalidations = state.invalidate_completion_sessions(self.id);
        drop(state);
        let callbacks = invalidations
            .into_iter()
            .filter_map(CompletionInvalidation::prepare)
            .collect::<Vec<_>>();
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.producers.remove(position);
        state.generation += 1;
        state.rebuild();
        let wakers = state.take_subscriber_wakers();
        drop(state);
        drop(publication);
        for callback in callbacks {
            let _ = callback.call();
        }
        for waker in wakers {
            waker.wake();
        }
        true
    }
}

impl RegistryState {
    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    pub(super) fn invalidate_completion_sessions(
        &mut self,
        producer_id: ProducerId,
    ) -> Vec<CompletionInvalidation> {
        let sessions = self
            .completion_sessions
            .iter()
            .filter_map(|(id, session)| session.upgrade().map(|session| (*id, session)))
            .filter(|(_, session)| session.producer_id == producer_id)
            .collect::<Vec<_>>();
        sessions
            .into_iter()
            .map(|(id, session)| {
                self.completion_sessions.remove(&id);
                CompletionInvalidation { session }
            })
            .collect()
    }

    fn take_subscriber_wakers(&mut self) -> Vec<Waker> {
        let mut wakers = Vec::new();
        self.subscribers.retain(|subscriber| {
            let Some(subscriber) = subscriber.upgrade() else {
                return false;
            };
            subscriber
                .generation
                .store(self.generation, Ordering::Release);
            if let Some(waker) = subscriber
                .waker
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                wakers.push(waker);
            }
            true
        });
        wakers
    }

    fn rebuild(&mut self) {
        let mut winners = HashMap::new();
        for producer in &self.producers {
            for record in &producer.records {
                let spec = &record.registration.spec;
                for (spelling, canonical) in std::iter::once((&spec.name, true))
                    .chain(spec.aliases.iter().map(|alias| (alias, false)))
                {
                    insert_winner(
                        &mut winners,
                        record,
                        spelling,
                        canonical,
                        producer.precedence,
                        producer.creation_order,
                    );
                }
            }
        }
        let mut producers = self.producers.iter().collect::<Vec<_>>();
        producers.sort_by_key(|producer| (producer.precedence, producer.creation_order));
        let mut projection = Vec::new();
        for producer in producers {
            for record in &producer.records {
                for spelling in std::iter::once(&record.registration.spec.name)
                    .chain(record.registration.spec.aliases.iter())
                {
                    if winners
                        .get(&normalize(spelling))
                        .is_some_and(|winner| winner.record.command_id == record.command_id)
                    {
                        projection.push(ResolvedCommand {
                            registry_id: record.command_id.0,
                            record: Arc::clone(record),
                            invoked_name: Arc::clone(spelling),
                        });
                    }
                }
            }
        }
        self.winners = winners;
        self.projection = projection.into();
    }
}

fn insert_winner(
    winners: &mut HashMap<String, Winner>,
    record: &Arc<RegistrationRecord>,
    spelling: &Arc<str>,
    canonical: bool,
    precedence: ProducerPrecedence,
    creation_order: u64,
) {
    let candidate = Winner {
        record: Arc::clone(record),
        canonical,
        precedence,
        creation_order,
    };
    let key = normalize(spelling);
    match winners.get(&key) {
        Some(current) if !candidate.precedes(current) => {}
        _ => {
            winners.insert(key, candidate);
        }
    }
}

impl Winner {
    fn precedes(&self, other: &Self) -> bool {
        (self.precedence, !self.canonical, self.creation_order)
            < (other.precedence, !other.canonical, other.creation_order)
    }
}

pub fn validate_registrations(
    registrations: Vec<Registration>,
) -> Result<Vec<Registration>, RegistrationError> {
    let mut spellings = HashSet::new();
    for registration in &registrations {
        if registration
            .spec
            .arguments
            .max
            .is_some_and(|max| registration.spec.arguments.min > max)
        {
            return Err(RegistrationError::InvalidArgumentArity {
                min: registration.spec.arguments.min,
                max: registration.spec.arguments.max.unwrap_or_default(),
            });
        }
        for (spelling, alias) in std::iter::once((&registration.spec.name, false))
            .chain(registration.spec.aliases.iter().map(|alias| (alias, true)))
        {
            if validate_spelling(spelling).is_err() {
                return Err(if alias {
                    RegistrationError::InvalidAlias(Arc::clone(spelling))
                } else {
                    RegistrationError::InvalidName(Arc::clone(spelling))
                });
            }
            if !spellings.insert(normalize(spelling)) {
                return Err(RegistrationError::DuplicateSpelling(Arc::clone(spelling)));
            }
        }
    }
    Ok(registrations)
}

fn validate_spelling(spelling: &str) -> Result<(), ()> {
    if spelling.len() > 1 && spelling.starts_with('/') && !spelling.chars().any(char::is_whitespace)
    {
        Ok(())
    } else {
        Err(())
    }
}

pub(super) fn normalize(spelling: &str) -> String {
    spelling.to_ascii_lowercase()
}
