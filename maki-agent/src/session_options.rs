use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use event_listener::{Event, EventListener};
use thiserror::Error;

pub const MODEL_OPTION_ID: &str = "model";
pub const YOLO_OPTION_ID: &str = "yolo";
pub const FAST_OPTION_ID: &str = "fast";
pub const WORKFLOW_OPTION_ID: &str = "workflow";
pub const ENABLED_VALUE: &str = "enabled";
pub const DISABLED_VALUE: &str = "disabled";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SessionOptionOwner {
    Builtin,
    Plugin { plugin: Arc<str>, generation: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOptionCategory {
    Model,
    Mode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOptionValue {
    pub value: Arc<str>,
    pub name: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOptionDefinition {
    pub id: Arc<str>,
    pub owner: SessionOptionOwner,
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub category: SessionOptionCategory,
    pub values: Arc<[SessionOptionValue]>,
    pub initial_value: Arc<str>,
    pub persistent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOptionState {
    pub definition: SessionOptionDefinition,
    pub current_value: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOptionsSnapshot {
    pub version: u64,
    pub options: Arc<[SessionOptionState]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionOptionError {
    #[error("unknown session option: {0}")]
    UnknownId(Arc<str>),
    #[error("invalid value {value:?} for session option {id}")]
    InvalidValue { id: Arc<str>, value: Arc<str> },
    #[error("session option owner is unavailable: {0}")]
    UnavailableOwner(Arc<str>),
    #[error("session option was replaced by a newer plugin generation: {0}")]
    StaleHandle(Arc<str>),
    #[error("session option rejected by policy: {0}")]
    PolicyRejected(Arc<str>),
    #[error("Fast mode is unsupported by the current model")]
    FastUnsupported,
    #[error("session option callback failed: {0}")]
    CallbackFailed(Arc<str>),
    #[error("invalid session option definition: {0}")]
    InvalidDefinition(Arc<str>),
}

#[derive(Clone)]
struct State {
    version: u64,
    definitions: Vec<SessionOptionDefinition>,
    values: HashMap<Arc<str>, Arc<str>>,
}

#[derive(Clone)]
pub(crate) struct SessionOptionsCandidate {
    base_version: u64,
    state: State,
}

pub struct SessionOptions {
    state: Mutex<State>,
    changed: Event,
}

pub struct SessionOptionsSubscription {
    options: Arc<SessionOptions>,
    observed_version: u64,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SessionOptionDefinition {
    pub fn validate(&self) -> Result<(), SessionOptionError> {
        if !valid_id(&self.id, &self.owner) {
            return Err(SessionOptionError::InvalidDefinition(Arc::from(
                "option ID does not match its owner",
            )));
        }
        if self.values.is_empty() {
            return Err(SessionOptionError::InvalidDefinition(Arc::from(
                "option values must not be empty",
            )));
        }
        let mut seen = BTreeMap::new();
        for value in self.values.iter() {
            if value.value.is_empty() || seen.insert(value.value.as_ref(), ()).is_some() {
                return Err(SessionOptionError::InvalidDefinition(Arc::from(
                    "option values must be non-empty and unique",
                )));
            }
        }
        if !self.accepts(&self.initial_value) {
            return Err(SessionOptionError::InvalidDefinition(Arc::from(
                "initial value is not selectable",
            )));
        }
        Ok(())
    }

    pub fn accepts(&self, value: &str) -> bool {
        self.values
            .iter()
            .any(|candidate| candidate.value.as_ref() == value)
    }
}

impl SessionOptions {
    pub fn new(
        definitions: Vec<SessionOptionDefinition>,
        persisted: &BTreeMap<String, String>,
    ) -> Result<Arc<Self>, SessionOptionError> {
        let mut values = HashMap::with_capacity(definitions.len());
        let mut ids = BTreeMap::new();
        for definition in &definitions {
            definition.validate()?;
            if ids.insert(definition.id.as_ref(), ()).is_some() {
                return Err(SessionOptionError::InvalidDefinition(Arc::from(
                    "option IDs must be unique",
                )));
            }
            let value = persisted
                .get(definition.id.as_ref())
                .filter(|value| definition.accepts(value))
                .map(|value| Arc::from(value.as_str()))
                .unwrap_or_else(|| Arc::clone(&definition.initial_value));
            values.insert(Arc::clone(&definition.id), value);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                version: 1,
                definitions,
                values,
            }),
            changed: Event::new(),
        }))
    }

    pub fn snapshot(&self) -> SessionOptionsSnapshot {
        let state = lock(&self.state);
        snapshot(&state)
    }

    pub(crate) fn prepare_set(
        &self,
        id: &str,
        value: &str,
    ) -> Result<Option<SessionOptionsCandidate>, SessionOptionError> {
        self.prepare_set_values(&[(id, value)])
    }

    pub(crate) fn prepare_set_values(
        &self,
        values: &[(&str, &str)],
    ) -> Result<Option<SessionOptionsCandidate>, SessionOptionError> {
        let state = lock(&self.state);
        let mut resolved = Vec::with_capacity(values.len());
        for (id, value) in values {
            let definition = state
                .definitions
                .iter()
                .find(|definition| definition.id.as_ref() == *id)
                .ok_or_else(|| SessionOptionError::UnknownId(Arc::from(*id)))?;
            if !definition.accepts(value) {
                return Err(SessionOptionError::InvalidValue {
                    id: Arc::from(*id),
                    value: Arc::from(*value),
                });
            }
            resolved.push((Arc::clone(&definition.id), Arc::from(*value)));
        }
        if resolved
            .iter()
            .all(|(id, value)| state.values.get(id).is_some_and(|current| current == value))
        {
            return Ok(None);
        }
        let mut candidate = state.clone();
        candidate.values.extend(resolved);
        candidate.version += 1;
        Ok(Some(SessionOptionsCandidate {
            base_version: state.version,
            state: candidate,
        }))
    }

    pub(crate) fn prepare_replace_plugin(
        &self,
        plugin: &str,
        definitions: Vec<SessionOptionDefinition>,
    ) -> Result<Option<SessionOptionsCandidate>, SessionOptionError> {
        for definition in &definitions {
            definition.validate()?;
            if !matches!(
                &definition.owner,
                SessionOptionOwner::Plugin { plugin: owner, .. } if owner.as_ref() == plugin
            ) {
                return Err(SessionOptionError::InvalidDefinition(Arc::from(
                    "plugin definition owner does not match replacement owner",
                )));
            }
        }
        let mut candidate_ids = BTreeMap::new();
        for definition in &definitions {
            if candidate_ids.insert(definition.id.as_ref(), ()).is_some() {
                return Err(SessionOptionError::InvalidDefinition(Arc::from(
                    "option IDs must be unique",
                )));
            }
        }

        let state = lock(&self.state);
        for definition in &definitions {
            if state.definitions.iter().any(|current| {
                current.id == definition.id
                    && !matches!(
                        &current.owner,
                        SessionOptionOwner::Plugin { plugin: owner, .. }
                            if owner.as_ref() == plugin
                    )
            }) {
                return Err(SessionOptionError::UnavailableOwner(Arc::clone(
                    &definition.id,
                )));
            }
        }

        let insertion = state
            .definitions
            .iter()
            .position(|definition| {
                matches!(
                    &definition.owner,
                    SessionOptionOwner::Plugin { plugin: owner, .. } if owner.as_ref() == plugin
                )
            })
            .unwrap_or(state.definitions.len());
        let mut next_definitions = state
            .definitions
            .iter()
            .filter(|definition| {
                !matches!(
                    &definition.owner,
                    SessionOptionOwner::Plugin { plugin: owner, .. } if owner.as_ref() == plugin
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        next_definitions.splice(insertion..insertion, definitions);

        let mut next_values = HashMap::with_capacity(next_definitions.len());
        for definition in &next_definitions {
            let value = state
                .values
                .get(&definition.id)
                .filter(|value| definition.accepts(value))
                .cloned()
                .unwrap_or_else(|| Arc::clone(&definition.initial_value));
            next_values.insert(Arc::clone(&definition.id), value);
        }
        if state.definitions == next_definitions && state.values == next_values {
            return Ok(None);
        }
        Ok(Some(SessionOptionsCandidate {
            base_version: state.version,
            state: State {
                version: state.version + 1,
                definitions: next_definitions,
                values: next_values,
            },
        }))
    }

    pub(crate) fn commit_batch(
        candidates: Vec<(Arc<SessionOptions>, SessionOptionsCandidate)>,
    ) -> Result<Vec<SessionOptionsSnapshot>, SessionOptionError> {
        let mut guards = candidates
            .iter()
            .map(|(options, _)| lock(&options.state))
            .collect::<Vec<_>>();
        for (guard, (_, candidate)) in guards.iter().zip(&candidates) {
            if guard.version != candidate.base_version {
                return Err(SessionOptionError::StaleHandle(Arc::from(
                    "session option snapshot changed during commit",
                )));
            }
        }
        let changed = candidates
            .iter()
            .map(|(options, _)| Arc::clone(options))
            .collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(candidates.len());
        for (guard, (_, candidate)) in guards.iter_mut().zip(&candidates) {
            **guard = candidate.state.clone();
            snapshots.push(snapshot(guard));
        }
        drop(guards);
        for options in changed {
            options.changed.notify(usize::MAX);
        }
        Ok(snapshots)
    }

    pub(crate) fn prepare_replace_definition(
        &self,
        definition: SessionOptionDefinition,
    ) -> Result<Option<SessionOptionsCandidate>, SessionOptionError> {
        definition.validate()?;
        let state = lock(&self.state);
        let index = state
            .definitions
            .iter()
            .position(|current| current.id == definition.id)
            .ok_or_else(|| SessionOptionError::UnknownId(Arc::clone(&definition.id)))?;
        if state.definitions[index].owner != definition.owner {
            return Err(SessionOptionError::UnavailableOwner(Arc::clone(
                &definition.id,
            )));
        }
        let current_value = state
            .values
            .get(&definition.id)
            .cloned()
            .unwrap_or_else(|| Arc::clone(&definition.initial_value));
        let value = if definition.accepts(&current_value) {
            current_value
        } else {
            Arc::clone(&definition.initial_value)
        };
        if state.definitions[index] == definition
            && state
                .values
                .get(&definition.id)
                .is_some_and(|current| current == &value)
        {
            return Ok(None);
        }
        let mut candidate = state.clone();
        candidate.definitions[index] = definition;
        candidate
            .values
            .insert(Arc::clone(&candidate.definitions[index].id), value);
        candidate.version += 1;
        Ok(Some(SessionOptionsCandidate {
            base_version: state.version,
            state: candidate,
        }))
    }

    pub(crate) fn unchanged_candidate(&self) -> SessionOptionsCandidate {
        let state = lock(&self.state);
        SessionOptionsCandidate {
            base_version: state.version,
            state: state.clone(),
        }
    }

    pub(crate) fn candidate_snapshot(
        candidate: &SessionOptionsCandidate,
    ) -> SessionOptionsSnapshot {
        snapshot(&candidate.state)
    }

    pub(crate) fn commit(
        &self,
        candidate: SessionOptionsCandidate,
    ) -> Result<SessionOptionsSnapshot, SessionOptionError> {
        let mut state = lock(&self.state);
        if state.version != candidate.base_version {
            return Err(SessionOptionError::StaleHandle(Arc::from(
                "session option snapshot changed during commit",
            )));
        }
        *state = candidate.state;
        let snapshot = snapshot(&state);
        drop(state);
        self.changed.notify(usize::MAX);
        Ok(snapshot)
    }

    pub fn set(&self, id: &str, value: &str) -> Result<SessionOptionsSnapshot, SessionOptionError> {
        let Some(candidate) = self.prepare_set(id, value)? else {
            return Ok(self.snapshot());
        };
        self.commit(candidate)
    }

    pub fn persistent_values(&self) -> BTreeMap<String, String> {
        let state = lock(&self.state);
        state
            .definitions
            .iter()
            .filter(|definition| definition.persistent)
            .filter_map(|definition| {
                state
                    .values
                    .get(&definition.id)
                    .map(|value| (definition.id.to_string(), value.to_string()))
            })
            .collect()
    }

    pub fn subscribe(self: &Arc<Self>) -> SessionOptionsSubscription {
        SessionOptionsSubscription {
            options: Arc::clone(self),
            observed_version: self.snapshot().version,
        }
    }
}

impl SessionOptionsSubscription {
    pub async fn changed(&mut self) -> SessionOptionsSnapshot {
        loop {
            let listener: EventListener = self.options.changed.listen();
            let snapshot = self.options.snapshot();
            if snapshot.version != self.observed_version {
                self.observed_version = snapshot.version;
                return snapshot;
            }
            listener.await;
        }
    }
}

fn snapshot(state: &State) -> SessionOptionsSnapshot {
    let options = state
        .definitions
        .iter()
        .map(|definition| SessionOptionState {
            definition: definition.clone(),
            current_value: state
                .values
                .get(&definition.id)
                .cloned()
                .unwrap_or_else(|| Arc::clone(&definition.initial_value)),
        })
        .collect::<Vec<_>>()
        .into();
    SessionOptionsSnapshot {
        version: state.version,
        options,
    }
}

fn valid_id(id: &str, owner: &SessionOptionOwner) -> bool {
    if id.is_empty()
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
        })
    {
        return false;
    }
    match owner {
        SessionOptionOwner::Builtin => !id.contains('.'),
        SessionOptionOwner::Plugin { plugin, .. } => id
            .strip_prefix(plugin.as_ref())
            .is_some_and(|suffix| suffix.starts_with('.') && suffix.len() > 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(id: &str, initial: &str) -> SessionOptionDefinition {
        SessionOptionDefinition {
            id: Arc::from(id),
            owner: SessionOptionOwner::Builtin,
            name: Arc::from(id),
            description: Arc::from("test option"),
            category: SessionOptionCategory::Mode,
            values: Arc::from([
                SessionOptionValue {
                    value: Arc::from(DISABLED_VALUE),
                    name: Arc::from("Disabled"),
                },
                SessionOptionValue {
                    value: Arc::from(ENABLED_VALUE),
                    name: Arc::from("Enabled"),
                },
            ]),
            initial_value: Arc::from(initial),
            persistent: true,
        }
    }

    #[test]
    fn session_option_values_are_isolated_per_session() {
        let definitions = vec![definition(YOLO_OPTION_ID, DISABLED_VALUE)];
        let first = SessionOptions::new(definitions.clone(), &BTreeMap::new()).unwrap();
        let second = SessionOptions::new(definitions, &BTreeMap::new()).unwrap();

        first.set(YOLO_OPTION_ID, ENABLED_VALUE).unwrap();

        assert_eq!(
            first.snapshot().options[0].current_value.as_ref(),
            ENABLED_VALUE
        );
        assert_eq!(
            second.snapshot().options[0].current_value.as_ref(),
            DISABLED_VALUE
        );
    }

    #[test]
    fn snapshot_order_is_stable() {
        let definitions = vec![
            definition(MODEL_OPTION_ID, DISABLED_VALUE),
            definition(YOLO_OPTION_ID, DISABLED_VALUE),
            definition(FAST_OPTION_ID, DISABLED_VALUE),
            definition(WORKFLOW_OPTION_ID, DISABLED_VALUE),
        ];
        let options = SessionOptions::new(definitions, &BTreeMap::new()).unwrap();

        assert_eq!(
            options
                .snapshot()
                .options
                .iter()
                .map(|option| option.definition.id.as_ref())
                .collect::<Vec<_>>(),
            [
                MODEL_OPTION_ID,
                YOLO_OPTION_ID,
                FAST_OPTION_ID,
                WORKFLOW_OPTION_ID,
            ]
        );
    }

    #[test]
    fn failed_option_set_is_atomic() {
        let options = SessionOptions::new(
            vec![definition(FAST_OPTION_ID, DISABLED_VALUE)],
            &BTreeMap::new(),
        )
        .unwrap();
        let before = options.snapshot();

        assert!(matches!(
            options.set(FAST_OPTION_ID, "maybe"),
            Err(SessionOptionError::InvalidValue { .. })
        ));
        assert_eq!(options.snapshot(), before);
    }

    #[test]
    fn persisted_value_is_used_only_when_still_selectable() {
        let mut persisted = BTreeMap::new();
        persisted.insert(YOLO_OPTION_ID.into(), ENABLED_VALUE.into());
        persisted.insert(FAST_OPTION_ID.into(), "removed".into());
        let options = SessionOptions::new(
            vec![
                definition(YOLO_OPTION_ID, DISABLED_VALUE),
                definition(FAST_OPTION_ID, DISABLED_VALUE),
            ],
            &persisted,
        )
        .unwrap();
        let snapshot = options.snapshot();

        assert_eq!(snapshot.options[0].current_value.as_ref(), ENABLED_VALUE);
        assert_eq!(snapshot.options[1].current_value.as_ref(), DISABLED_VALUE);
    }
}
