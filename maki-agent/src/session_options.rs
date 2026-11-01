use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use event_listener::{Event, EventListener};
use maki_providers::ThinkingConfig;
use thiserror::Error;

pub const MODEL_OPTION_ID: &str = "model";
pub const YOLO_OPTION_ID: &str = "yolo";
pub const FAST_OPTION_ID: &str = "fast";
pub const WORKFLOW_OPTION_ID: &str = "workflow";
pub const THINKING_OPTION_ID: &str = "thinking";
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

/// The domain an option accepts beyond the values it can enumerate. Thinking
/// is the case that needs one: a client picks from the named efforts, but a
/// token budget is any number, so the list cannot be the whole domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreeValueDomain {
    /// Anything [`ThinkingConfig`] can read, which includes a token budget.
    ThinkingSetting,
}

impl FreeValueDomain {
    fn accepts(self, value: &str) -> bool {
        match self {
            Self::ThinkingSetting => value.parse::<ThinkingConfig>().is_ok(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionOptionDefinition {
    pub id: Arc<str>,
    pub owner: SessionOptionOwner,
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub category: SessionOptionCategory,
    pub values: Arc<[SessionOptionValue]>,
    /// `None` when `values` is the whole domain, which is every option but
    /// thinking.
    pub free_value: Option<FreeValueDomain>,
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

impl SessionOptionsSnapshot {
    /// Whether boolean option {id} is on; an absent option is off.
    pub fn enabled(&self, id: &str) -> bool {
        self.options.iter().any(|option| {
            option.definition.id.as_ref() == id && option.current_value.as_ref() == ENABLED_VALUE
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionOptionError {
    #[error("session options are reserved by another mutation")]
    Busy,
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
    #[error("Thinking is unsupported by the current model")]
    ThinkingUnsupported,
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

impl SessionOptionsCandidate {
    pub(crate) fn base_version(&self) -> u64 {
        self.base_version
    }
}

struct OptionsState {
    committed: State,
    reserved: bool,
}

impl Deref for OptionsState {
    type Target = State;

    fn deref(&self) -> &Self::Target {
        &self.committed
    }
}

type OptionsProjector = Box<dyn Fn(&SessionOptionsSnapshot) + Send + Sync>;

pub struct SessionOptions {
    state: Mutex<OptionsState>,
    changed: Event,
    projector: OnceLock<OptionsProjector>,
}

pub(crate) struct SessionOptionsReservation {
    options: Arc<SessionOptions>,
    base_version: u64,
}

impl SessionOptionsReservation {
    pub(crate) fn commit(self, candidate: SessionOptionsCandidate) -> SessionOptionsSnapshot {
        let mut state = lock(&self.options.state);
        assert_eq!(candidate.base_version, self.base_version);
        state.committed = candidate.state;
        let snapshot = snapshot(&state);
        drop(state);
        self.options.published(&snapshot);
        snapshot
    }
}

impl Drop for SessionOptionsReservation {
    fn drop(&mut self) {
        lock(&self.options.state).reserved = false;
        self.options.changed.notify(usize::MAX);
    }
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
            || self.free_value.is_some_and(|domain| domain.accepts(value))
    }
}

impl SessionOptions {
    /// Applies {project} to the current snapshot and then to every committed
    /// one, synchronously and before subscribers wake. One projector per
    /// store; a second is ignored.
    pub fn project(&self, project: impl Fn(&SessionOptionsSnapshot) + Send + Sync + 'static) {
        project(&self.snapshot());
        let _ = self.projector.set(Box::new(project));
    }

    fn published(&self, snapshot: &SessionOptionsSnapshot) {
        if let Some(project) = self.projector.get() {
            project(snapshot);
        }
        self.changed.notify(usize::MAX);
    }

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
            state: Mutex::new(OptionsState {
                committed: State {
                    version: 1,
                    definitions,
                    values,
                },
                reserved: false,
            }),
            changed: Event::new(),
            projector: OnceLock::new(),
        }))
    }

    pub fn snapshot(&self) -> SessionOptionsSnapshot {
        let state = lock(&self.state);
        snapshot(&state)
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        expected_version: Option<u64>,
    ) -> Result<SessionOptionsReservation, SessionOptionError> {
        let mut state = lock(&self.state);
        if expected_version.is_some_and(|version| version != state.version) {
            return Err(SessionOptionError::StaleHandle(Arc::from(
                "session option snapshot changed during validation",
            )));
        }
        if state.reserved {
            return Err(SessionOptionError::Busy);
        }
        state.reserved = true;
        Ok(SessionOptionsReservation {
            options: Arc::clone(self),
            base_version: state.version,
        })
    }

    pub(crate) fn reserve(
        self: &Arc<Self>,
        candidate: &SessionOptionsCandidate,
    ) -> Result<SessionOptionsReservation, SessionOptionError> {
        self.try_reserve(Some(candidate.base_version()))
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
        Self::prepare_set_values_from_state(&state, values)
    }

    pub(crate) fn prepare_actor_config(
        &self,
        model: &str,
        fast: &str,
        thinking: &str,
        workflow: &str,
    ) -> Result<Option<SessionOptionsCandidate>, SessionOptionError> {
        let state = lock(&self.state);
        let mut candidate = state.clone();
        let definition = candidate
            .definitions
            .iter_mut()
            .find(|definition| definition.id.as_ref() == MODEL_OPTION_ID)
            .ok_or_else(|| SessionOptionError::UnknownId(Arc::from(MODEL_OPTION_ID)))?;
        if !definition.accepts(model) {
            let mut choices = definition.values.to_vec();
            choices.push(SessionOptionValue {
                value: Arc::from(model),
                name: Arc::from(model),
            });
            definition.values = choices.into();
        }
        Self::prepare_set_values_from_state(
            &candidate,
            &[
                (MODEL_OPTION_ID, model),
                (FAST_OPTION_ID, fast),
                (THINKING_OPTION_ID, thinking),
                (WORKFLOW_OPTION_ID, workflow),
            ],
        )
    }

    fn prepare_set_values_from_state(
        state: &State,
        values: &[(&str, &str)],
    ) -> Result<Option<SessionOptionsCandidate>, SessionOptionError> {
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
        candidates: Vec<(&SessionOptionsReservation, SessionOptionsCandidate)>,
    ) -> Result<Vec<SessionOptionsSnapshot>, SessionOptionError> {
        if candidates
            .iter()
            .enumerate()
            .any(|(index, (reservation, _))| {
                candidates[..index]
                    .iter()
                    .any(|(previous, _)| Arc::ptr_eq(&reservation.options, &previous.options))
            })
        {
            return Err(SessionOptionError::Busy);
        }
        let mut guards = candidates
            .iter()
            .map(|(reservation, _)| lock(&reservation.options.state))
            .collect::<Vec<_>>();
        for (guard, (reservation, candidate)) in guards.iter().zip(&candidates) {
            if guard.version != candidate.base_version
                || reservation.base_version != candidate.base_version
            {
                return Err(SessionOptionError::StaleHandle(Arc::from(
                    "session option snapshot changed during commit",
                )));
            }
        }
        let changed = candidates
            .iter()
            .map(|(reservation, _)| Arc::clone(&reservation.options))
            .collect::<Vec<_>>();
        let mut snapshots = Vec::with_capacity(candidates.len());
        for (guard, (_, candidate)) in guards.iter_mut().zip(&candidates) {
            guard.committed = candidate.state.clone();
            snapshots.push(snapshot(guard));
        }
        drop(guards);
        for (options, snapshot) in changed.iter().zip(&snapshots) {
            options.published(snapshot);
        }
        Ok(snapshots)
    }

    pub(crate) fn restore_reserved_batch_if_versions(
        snapshots: Vec<(&SessionOptionsReservation, u64, SessionOptionsSnapshot)>,
    ) -> bool {
        if snapshots
            .iter()
            .enumerate()
            .any(|(index, (reservation, _, _))| {
                snapshots[..index]
                    .iter()
                    .any(|(previous, _, _)| Arc::ptr_eq(&reservation.options, &previous.options))
            })
        {
            return false;
        }
        let changed = snapshots
            .iter()
            .map(|(reservation, _, _)| Arc::clone(&reservation.options))
            .collect::<Vec<_>>();
        let mut guards = changed
            .iter()
            .map(|options| lock(&options.state))
            .collect::<Vec<_>>();
        if guards
            .iter()
            .zip(&snapshots)
            .any(|(guard, (_, expected_version, _))| guard.version != *expected_version)
        {
            return false;
        }
        for (guard, (_, _, snapshot)) in guards.iter_mut().zip(snapshots) {
            guard.committed = State {
                version: snapshot.version,
                definitions: snapshot
                    .options
                    .iter()
                    .map(|option| option.definition.clone())
                    .collect(),
                values: snapshot
                    .options
                    .iter()
                    .map(|option| {
                        (
                            Arc::clone(&option.definition.id),
                            Arc::clone(&option.current_value),
                        )
                    })
                    .collect(),
            };
        }
        drop(guards);
        for options in changed {
            options.published(&options.snapshot());
        }
        true
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

    pub(crate) async fn wait_unreserved(&self) {
        loop {
            let listener: EventListener = self.changed.listen();
            if !lock(&self.state).reserved {
                return;
            }
            listener.await;
        }
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
            free_value: None,
            initial_value: Arc::from(initial),
            persistent: true,
        }
    }

    fn set(
        options: &Arc<SessionOptions>,
        id: &str,
        value: &str,
    ) -> Result<SessionOptionsSnapshot, SessionOptionError> {
        let reservation = options.try_reserve(None)?;
        Ok(match options.prepare_set(id, value)? {
            Some(candidate) => reservation.commit(candidate),
            None => options.snapshot(),
        })
    }

    #[test]
    fn session_option_values_are_isolated_per_session() {
        let definitions = vec![definition(YOLO_OPTION_ID, DISABLED_VALUE)];
        let first = SessionOptions::new(definitions.clone(), &BTreeMap::new()).unwrap();
        let second = SessionOptions::new(definitions, &BTreeMap::new()).unwrap();

        set(&first, YOLO_OPTION_ID, ENABLED_VALUE).unwrap();

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
    fn model_definition_and_settings_are_staged_in_one_candidate() {
        let mut model = definition(MODEL_OPTION_ID, "old/model");
        model.values = Arc::from([SessionOptionValue {
            value: Arc::from("old/model"),
            name: Arc::from("old/model"),
        }]);
        let options = SessionOptions::new(
            vec![
                model,
                definition(FAST_OPTION_ID, DISABLED_VALUE),
                definition(THINKING_OPTION_ID, DISABLED_VALUE),
                definition(WORKFLOW_OPTION_ID, DISABLED_VALUE),
            ],
            &BTreeMap::new(),
        )
        .unwrap();
        let before = options.snapshot();

        let candidate = options
            .prepare_actor_config("new/model", ENABLED_VALUE, ENABLED_VALUE, DISABLED_VALUE)
            .unwrap()
            .unwrap();
        let staged = SessionOptions::candidate_snapshot(&candidate);

        assert_eq!(options.snapshot(), before);
        assert_eq!(staged.version, before.version + 1);
        assert_eq!(staged.options[0].current_value.as_ref(), "new/model");
        assert!(staged.options[0].definition.accepts("new/model"));
        assert_eq!(staged.options[1].current_value.as_ref(), ENABLED_VALUE);
        assert_eq!(staged.options[2].current_value.as_ref(), ENABLED_VALUE);
        assert_eq!(
            options.reserve(&candidate).unwrap().commit(candidate),
            staged
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
            set(&options, FAST_OPTION_ID, "maybe"),
            Err(SessionOptionError::InvalidValue { .. })
        ));
        assert_eq!(options.snapshot(), before);
        let candidate = options
            .prepare_set(FAST_OPTION_ID, ENABLED_VALUE)
            .unwrap()
            .unwrap();
        let reservation = options.reserve(&candidate).unwrap();
        let concurrent = Arc::clone(&options);
        std::thread::spawn(move || {
            for value in [ENABLED_VALUE, DISABLED_VALUE] {
                assert!(matches!(
                    set(&concurrent, FAST_OPTION_ID, value),
                    Err(SessionOptionError::Busy)
                ));
            }
        })
        .join()
        .unwrap();
        assert_eq!(options.snapshot(), before);
        drop(reservation);
        let reservation = options.reserve(&candidate).unwrap();
        let committed = reservation.commit(candidate.clone());
        assert_eq!(committed.version, before.version + 1);
        assert!(matches!(
            options.reserve(&candidate),
            Err(SessionOptionError::StaleHandle(_))
        ));
        set(&options, FAST_OPTION_ID, DISABLED_VALUE).unwrap();
    }

    #[test]
    fn rollback_does_not_overwrite_a_newer_option_update() {
        let options = SessionOptions::new(
            vec![definition(YOLO_OPTION_ID, DISABLED_VALUE)],
            &BTreeMap::new(),
        )
        .unwrap();
        let previous = options.snapshot();
        let candidate = options
            .prepare_set(YOLO_OPTION_ID, ENABLED_VALUE)
            .unwrap()
            .unwrap();
        let reservation = options.reserve(&candidate).unwrap();
        let committed = SessionOptions::commit_batch(vec![(&reservation, candidate)]).unwrap();
        drop(reservation);
        let newer = set(&options, YOLO_OPTION_ID, DISABLED_VALUE).unwrap();
        let reservation = options.try_reserve(None).unwrap();

        assert!(!SessionOptions::restore_reserved_batch_if_versions(vec![(
            &reservation,
            committed[0].version,
            previous,
        )]));
        assert_eq!(options.snapshot(), newer);
    }

    #[test]
    fn duplicate_restore_target_is_rejected() {
        let options = SessionOptions::new(
            vec![definition(YOLO_OPTION_ID, DISABLED_VALUE)],
            &BTreeMap::new(),
        )
        .unwrap();
        let snapshot = options.snapshot();
        let reservation = options.try_reserve(None).unwrap();

        assert!(!SessionOptions::restore_reserved_batch_if_versions(vec![
            (&reservation, snapshot.version, snapshot.clone()),
            (&reservation, snapshot.version, snapshot),
        ]));
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
