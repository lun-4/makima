use crate::{
    AgentId, TurnId,
    journal::{JournalChange, JournalError, SessionJournal, SessionJournalSnapshot},
};
use makima_storage::{
    id::MakimaId,
    session_state::{StateChange, StoredAgentLifecycle, StoredAgentRecord, StoredStateRecord},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    marker::PhantomData,
    sync::Arc,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateScope {
    Agent,
    Session,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSlotSpec {
    pub key: String,
    pub scope: StateScope,
    pub version: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StateRevision(pub u64);

#[derive(Debug, Clone, PartialEq)]
pub struct StateSnapshot<T> {
    pub value: Option<T>,
    pub revision: StateRevision,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawStateSnapshot {
    pub value: Option<Value>,
    pub revision: StateRevision,
    pub version: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateChangeReason {
    Commit,
    Resume,
    Rewind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateChangeEvent {
    pub key: String,
    pub agent: Option<AgentId>,
    pub revision: StateRevision,
    pub reason: StateChangeReason,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("state_conflict")]
    Conflict,
    #[error("state_stale_access")]
    StaleAccess,
    #[error("state_read_only")]
    ReadOnly,
    #[error("state_invalid_registration: {0}")]
    Registration(String),
    #[error("state_newer_version: stored {stored}, registered {registered}")]
    NewerVersion { stored: u32, registered: u32 },
    #[error("state_migration_required: stored {stored}, registered {registered}")]
    MigrationRequired { stored: u32, registered: u32 },
    #[error("state_invalid_value: {0}")]
    InvalidValue(String),
    #[error("state_journal: {0}")]
    Journal(String),
}

#[derive(Clone)]
pub struct StateAccess {
    pub(crate) journal: Arc<SessionJournal>,
    pub(crate) target: AgentId,
    pub(crate) session_epoch: MakimaId,
    pub(crate) agent_epoch: MakimaId,
    pub(crate) turn: Option<(TurnId, u64)>,
    pub(crate) writable: bool,
}

impl fmt::Debug for StateAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateAccess")
            .field("target", &self.target)
            .field("turn", &self.turn)
            .finish_non_exhaustive()
    }
}

impl StateAccess {
    pub fn target(&self) -> AgentId {
        self.target
    }
    pub fn read_only(mut self) -> Self {
        self.writable = false;
        self
    }
    pub fn journal(&self) -> Arc<SessionJournal> {
        Arc::clone(&self.journal)
    }
}

type Migration = dyn Fn(u32, Value) -> Result<Value, StateError> + Send + Sync;

pub struct StateSlot<T> {
    journal: Arc<SessionJournal>,
    spec: StateSlotSpec,
    migrate: Option<Arc<Migration>>,
    marker: PhantomData<fn() -> T>,
}

impl<T> Clone for StateSlot<T> {
    fn clone(&self) -> Self {
        Self {
            journal: Arc::clone(&self.journal),
            spec: self.spec.clone(),
            migrate: self.migrate.clone(),
            marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StateEdit {
    pub(crate) access: StateAccess,
    pub(crate) spec: StateSlotSpec,
    pub(crate) value: Option<Value>,
    pub(crate) expect: Option<StateRevision>,
}

impl StateEdit {
    pub(crate) fn address(&self) -> (Option<AgentId>, String) {
        (
            self.spec.scope.agent(self.access.target),
            self.spec.key.clone(),
        )
    }
    pub(crate) fn change(&self) -> JournalChange {
        JournalChange {
            agent: self.address().0,
            generation: 0,
            change: StateChange::Plugin {
                key: self.spec.key.clone(),
                version: self.spec.version,
                value: self.value.clone(),
            },
        }
    }
}

impl StateScope {
    fn agent(self, target: AgentId) -> Option<AgentId> {
        match self {
            Self::Agent => Some(target),
            Self::Session => None,
        }
    }
}

#[derive(Clone)]
struct StateNotice {
    event: StateChangeEvent,
    subscribers: Vec<flume::Sender<StateChangeEvent>>,
}

#[derive(Clone)]
pub(crate) struct StateRuntime {
    pub(crate) session_epoch: MakimaId,
    pub(crate) agent_epochs: BTreeMap<AgentId, MakimaId>,
    pub(crate) active_turns: BTreeMap<(AgentId, TurnId), u64>,
    pub(crate) registrations: BTreeMap<String, StateSlotSpec>,
    pub(crate) reservations: BTreeMap<(Option<AgentId>, String), u64>,
    pub(crate) subscribers: Vec<flume::Sender<StateChangeEvent>>,
    notices: VecDeque<StateNotice>,
    draining_notices: bool,
    #[cfg(test)]
    dispatch_gate: Option<(flume::Sender<()>, flume::Receiver<()>)>,
    pub(crate) valid: bool,
}

impl Default for StateRuntime {
    fn default() -> Self {
        Self {
            session_epoch: MakimaId::generate(),
            agent_epochs: BTreeMap::new(),
            active_turns: BTreeMap::new(),
            registrations: BTreeMap::new(),
            reservations: BTreeMap::new(),
            subscribers: Vec::new(),
            notices: VecDeque::new(),
            draining_notices: false,
            #[cfg(test)]
            dispatch_gate: None,
            valid: true,
        }
    }
}

impl StateRuntime {
    pub(crate) fn enqueue_notices(&mut self, events: impl IntoIterator<Item = StateChangeEvent>) {
        self.subscribers.retain(|sender| !sender.is_disconnected());
        self.notices
            .extend(events.into_iter().map(|event| StateNotice {
                event,
                subscribers: self.subscribers.clone(),
            }));
    }
}

impl<T> StateSlot<T> {
    pub fn spec(&self) -> &StateSlotSpec {
        &self.spec
    }
    pub fn with_migration(
        mut self,
        migrate: impl Fn(u32, Value) -> Result<Value, StateError> + Send + Sync + 'static,
    ) -> Self {
        self.migrate = Some(Arc::new(migrate));
        self
    }
    pub fn get_raw(&self, access: &StateAccess) -> Result<RawStateSnapshot, StateError> {
        if !Arc::ptr_eq(&self.journal, &access.journal) {
            return Err(StateError::StaleAccess);
        }
        let state = self.journal.lock();
        validate_access(
            &state.plugin_state,
            &state.snapshot,
            access,
            self.spec.scope,
            false,
            None,
        )?;
        Ok(read_slot(
            &state.snapshot,
            self.spec.scope.agent(access.target),
            &self.spec.key,
            self.spec.version,
        ))
    }
    pub fn edit_json(
        &self,
        access: &StateAccess,
        value: Option<Value>,
        expect: Option<StateRevision>,
    ) -> Result<StateEdit, StateError> {
        if !Arc::ptr_eq(&self.journal, &access.journal) {
            return Err(StateError::StaleAccess);
        }
        let edit = StateEdit {
            access: access.clone(),
            spec: self.spec.clone(),
            value,
            expect,
        };
        validate_record(&edit.change(), 1)?;
        Ok(edit)
    }
    pub fn delete(
        &self,
        access: &StateAccess,
        expect: Option<StateRevision>,
    ) -> Result<StateRevision, StateError> {
        self.commit(self.edit_json(access, None, expect)?)
    }
    fn commit(&self, edit: StateEdit) -> Result<StateRevision, StateError> {
        self.journal
            .commit_state_edit(edit)
            .map_err(|error| match error {
                JournalError::State(error) => error,
                error => StateError::Journal(error.to_string()),
            })
    }
}

impl<T: Serialize + DeserializeOwned> StateSlot<T> {
    pub fn get(&self, access: &StateAccess) -> Result<StateSnapshot<T>, StateError> {
        self.get_migrated(access, |version, value| match &self.migrate {
            Some(migrate) => migrate(version, value),
            None => Err(StateError::MigrationRequired {
                stored: version,
                registered: self.spec.version,
            }),
        })
    }
    pub fn get_migrated(
        &self,
        access: &StateAccess,
        migrate: impl FnOnce(u32, Value) -> Result<Value, StateError>,
    ) -> Result<StateSnapshot<T>, StateError> {
        let raw = self.get_raw(access)?;
        if raw.version > self.spec.version {
            return Err(StateError::NewerVersion {
                stored: raw.version,
                registered: self.spec.version,
            });
        }
        let mut revision = raw.revision;
        let value = if raw.version < self.spec.version {
            match raw.value {
                Some(value) => {
                    let value = migrate(raw.version, value)?;
                    let decoded = serde_json::from_value(value.clone())
                        .map_err(|error| StateError::InvalidValue(error.to_string()))?;
                    revision =
                        self.commit(self.edit_json(access, Some(value), Some(revision))?)?;
                    Some(decoded)
                }
                None => None,
            }
        } else {
            raw.value
                .map(serde_json::from_value)
                .transpose()
                .map_err(|error| StateError::InvalidValue(error.to_string()))?
        };
        Ok(StateSnapshot { value, revision })
    }
    pub fn edit(
        &self,
        access: &StateAccess,
        value: Option<T>,
        expect: Option<StateRevision>,
    ) -> Result<StateEdit, StateError> {
        self.edit_json(
            access,
            value
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| StateError::InvalidValue(error.to_string()))?,
            expect,
        )
    }
    pub fn set(
        &self,
        access: &StateAccess,
        value: T,
        expect: Option<StateRevision>,
    ) -> Result<StateRevision, StateError> {
        self.commit(self.edit(access, Some(value), expect)?)
    }
    pub fn update(
        &self,
        access: &StateAccess,
        update: impl FnOnce(Option<T>) -> Result<Option<T>, StateError>,
    ) -> Result<StateSnapshot<T>, StateError> {
        let current = self.get(access)?;
        let value = update(current.value)?;
        let edit = self.edit_json(
            access,
            value
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| StateError::InvalidValue(error.to_string()))?,
            Some(current.revision),
        )?;
        let revision = self.commit(edit)?;
        Ok(StateSnapshot { value, revision })
    }
}

pub(crate) fn read_slot(
    snapshot: &SessionJournalSnapshot,
    agent: Option<AgentId>,
    key: &str,
    version: u32,
) -> RawStateSnapshot {
    snapshot
        .state
        .iter()
        .rev()
        .find_map(|record| match &record.change {
            StateChange::Plugin {
                key: saved,
                version,
                value,
            } if record.agent == agent && saved == key => Some(RawStateSnapshot {
                value: value.clone(),
                revision: StateRevision(record.seq),
                version: *version,
            }),
            _ => None,
        })
        .unwrap_or(RawStateSnapshot {
            value: None,
            revision: StateRevision(0),
            version,
        })
}

pub(crate) fn validate_record(change: &JournalChange, seq: u64) -> Result<(), StateError> {
    let record = StoredStateRecord {
        seq,
        agent: change.agent,
        generation: change.generation,
        change: change.change.clone(),
    };
    let bytes =
        serde_json::to_vec(&record).map_err(|error| StateError::InvalidValue(error.to_string()))?;
    let decoded: StoredStateRecord = serde_json::from_slice(&bytes)
        .map_err(|error| StateError::InvalidValue(error.to_string()))?;
    if decoded != record {
        return Err(StateError::InvalidValue(
            "stored record does not round trip".into(),
        ));
    }
    makima_storage::session_state::SessionStateView::fold(&[decoded], &[], seq)
        .map_err(|error| StateError::InvalidValue(error.to_string()))?;
    Ok(())
}

pub(crate) fn validate_access(
    runtime: &StateRuntime,
    snapshot: &SessionJournalSnapshot,
    access: &StateAccess,
    scope: StateScope,
    writing: bool,
    target_record: Option<&StoredAgentRecord>,
) -> Result<(), StateError> {
    if !runtime.valid
        || runtime.session_epoch != access.session_epoch
        || (scope == StateScope::Agent
            && runtime.agent_epochs.get(&access.target) != Some(&access.agent_epoch))
    {
        return Err(StateError::StaleAccess);
    }
    if writing {
        if !access.writable {
            return Err(StateError::ReadOnly);
        }
        if let Some((turn, nonce)) = access.turn
            && runtime.active_turns.get(&(access.target, turn)) != Some(&nonce)
        {
            return Err(StateError::StaleAccess);
        }
        if target_record
            .or_else(|| {
                snapshot
                    .agent_revisions
                    .iter()
                    .rev()
                    .find(|(_, record)| record.id == access.target)
                    .map(|(_, record)| record)
            })
            .is_none_or(|record| record.lifecycle == StoredAgentLifecycle::Retired)
        {
            return Err(StateError::StaleAccess);
        }
    }
    Ok(())
}

impl SessionJournal {
    pub fn register_state<T>(
        self: &Arc<Self>,
        spec: StateSlotSpec,
    ) -> Result<StateSlot<T>, StateError> {
        let Some((plugin, name)) = spec.key.split_once('/') else {
            return Err(StateError::Registration(spec.key));
        };
        if plugin.is_empty() || name.is_empty() || name.contains('/') || spec.version == 0 {
            return Err(StateError::Registration(spec.key));
        }
        let mut state = self.lock();
        if state
            .plugin_state
            .registrations
            .get(&spec.key)
            .is_some_and(|saved| saved.scope != spec.scope)
        {
            return Err(StateError::Registration(spec.key));
        }
        state
            .plugin_state
            .registrations
            .insert(spec.key.clone(), spec.clone());
        Ok(StateSlot {
            journal: Arc::clone(self),
            spec,
            migrate: None,
            marker: PhantomData,
        })
    }
    pub fn subscribe_state_changes(&self) -> flume::Receiver<StateChangeEvent> {
        let (sender, receiver) = flume::unbounded();
        let mut state = self.lock();
        state
            .plugin_state
            .subscribers
            .retain(|sender| !sender.is_disconnected());
        state.plugin_state.subscribers.push(sender);
        receiver
    }
    pub fn invalidate_state_access(&self) {
        self.lock().plugin_state.valid = false;
    }
    pub(crate) fn state_access(
        self: &Arc<Self>,
        target: AgentId,
        turn: Option<(TurnId, u64)>,
    ) -> StateAccess {
        let mut state = self.lock();
        let agent_epoch = *state
            .plugin_state
            .agent_epochs
            .entry(target)
            .or_insert_with(MakimaId::generate);
        StateAccess {
            journal: Arc::clone(self),
            target,
            turn,
            agent_epoch,
            session_epoch: state.plugin_state.session_epoch,
            writable: true,
        }
    }
    pub(crate) fn register_state_turn(&self, agent: AgentId, turn: TurnId, nonce: u64) {
        self.lock()
            .plugin_state
            .active_turns
            .insert((agent, turn), nonce);
    }
    pub(crate) fn revoke_state_turn(&self, agent: AgentId, turn: TurnId) {
        self.lock().plugin_state.active_turns.remove(&(agent, turn));
    }
    pub(crate) fn revoke_all_state_turns(&self) {
        self.lock().plugin_state.active_turns.clear();
    }
    pub(crate) fn revoke_state_agent(&self, agent: AgentId) {
        self.lock()
            .plugin_state
            .active_turns
            .retain(|(id, _), _| *id != agent);
    }
    pub fn notify_state_transition(&self, reason: StateChangeReason) {
        let agents = self
            .snapshot()
            .agent_revisions
            .iter()
            .map(|(_, agent)| agent.id)
            .collect();
        self.notify_state_transition_scoped(reason, &agents, true);
    }

    pub fn notify_state_transition_scoped(
        &self,
        reason: StateChangeReason,
        agents: &BTreeSet<AgentId>,
        include_session: bool,
    ) {
        {
            let mut state = self.lock();
            let mut seen = BTreeSet::new();
            let events = state
                .snapshot
                .state
                .iter()
                .rev()
                .filter(|record| {
                    record
                        .agent
                        .map_or(include_session, |agent| agents.contains(&agent))
                })
                .filter_map(|record| match &record.change {
                    StateChange::Plugin { key, value, .. }
                        if seen.insert((record.agent, key.clone())) =>
                    {
                        value.as_ref().map(|_| StateChangeEvent {
                            key: key.clone(),
                            agent: record.agent,
                            revision: StateRevision(record.seq),
                            reason,
                        })
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            state.plugin_state.enqueue_notices(events);
        }
        self.drain_state_notices();
    }

    pub(crate) fn drain_state_notices(&self) {
        {
            let mut state = self.lock();
            if state.plugin_state.draining_notices {
                return;
            }
            state.plugin_state.draining_notices = true;
        }
        loop {
            let notice = {
                let mut state = self.lock();
                match state.plugin_state.notices.pop_front() {
                    Some(notice) => notice,
                    None => {
                        state.plugin_state.draining_notices = false;
                        return;
                    }
                }
            };
            #[cfg(test)]
            {
                let gate = self.lock().plugin_state.dispatch_gate.take();
                if let Some((entered, release)) = gate {
                    entered.send(()).unwrap();
                    release.recv().unwrap();
                }
            }
            for subscriber in notice.subscribers {
                let _ = subscriber.send(notice.event.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        StateChangeReason, StateError, StateRevision, StateScope, StateSlot, StateSlotSpec,
    };
    use crate::{
        AgentId, CancelToken, History, HistorySnapshot, TurnId,
        journal::{JournalChange, JournalCommit, SessionJournal},
    };
    use makima_storage::session_state::{StateChange, StoredAgentLifecycle, StoredAgentRecord};
    use serde_json::{Value, json};
    use std::sync::Arc;
    use test_case::test_case;

    const KEY: &str = "fixture/state";
    const OTHER_KEY: &str = "fixture/other";
    const MIGRATION_FAILED: &str = "migration failed";

    fn record(agent: AgentId) -> StoredAgentRecord {
        StoredAgentRecord {
            id: agent,
            parent: None,
            depth: 0,
            transcript_key: String::new(),
            lifecycle: StoredAgentLifecycle::Open,
            ancestry_unknown: false,
            label: None,
            spawned_by_tool_use_id: None,
            spec: None,
            restorable: Ok(()),
            usage: Default::default(),
            created_seq: 0,
            updated_seq: 0,
            legacy: false,
        }
    }

    fn fixture() -> (Arc<SessionJournal>, AgentId) {
        let journal = Arc::new(SessionJournal::default());
        let agent = AgentId::generate();
        journal
            .commit(JournalCommit {
                agents: vec![record(agent)],
                ..Default::default()
            })
            .unwrap();
        (journal, agent)
    }

    fn slot(journal: &Arc<SessionJournal>, scope: StateScope, version: u32) -> StateSlot<Value> {
        journal
            .register_state(StateSlotSpec {
                key: KEY.into(),
                scope,
                version,
            })
            .unwrap()
    }

    #[test]
    fn agent_identity_and_initial_state_are_published_in_one_commit() {
        let journal = Arc::new(SessionJournal::default());
        let agent = AgentId::generate();
        let access = journal.state_access(agent, None);
        let slot = slot(&journal, StateScope::Session, 1);
        let edit = slot
            .edit(&access, Some(json!(1)), Some(StateRevision(0)))
            .unwrap();
        journal
            .commit(JournalCommit {
                agents: vec![record(agent)],
                state_edits: vec![edit],
                ..Default::default()
            })
            .unwrap();
        assert!(journal.agent(agent).is_some());
        assert_eq!(slot.get(&access).unwrap().value, Some(json!(1)));
    }

    #[test_case(StateScope::Agent; "agent")]
    #[test_case(StateScope::Session; "session")]
    fn edits_are_revision_checked_and_tombstones_are_retained(scope: StateScope) {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let slot = slot(&journal, scope, 1);
        let receiver = journal.subscribe_state_changes();
        assert_eq!(slot.get(&access).unwrap().revision, StateRevision(0));
        let revision = slot
            .set(&access, Value::Null, Some(StateRevision(0)))
            .unwrap();
        assert_eq!(slot.get(&access).unwrap().value, Some(Value::Null));
        assert_eq!(receiver.try_recv().unwrap().revision, revision);
        assert!(matches!(
            slot.set(&access, json!(1), Some(StateRevision(0))),
            Err(StateError::Conflict)
        ));
        let deleted = slot.delete(&access, Some(revision)).unwrap();
        let current = slot.get(&access).unwrap();
        assert_eq!(current.value, None);
        assert_eq!(current.revision, deleted);
        assert!(deleted.0 > revision.0);
        assert_eq!(receiver.try_recv().unwrap().revision, deleted);
        assert!(receiver.try_recv().is_err());
        let before = journal.snapshot().revision;
        slot.set(&access, json!(2), None).unwrap();
        assert!(journal.snapshot().revision > before);
    }

    #[test]
    fn update_runs_once_without_a_lock_and_conflicts_with_reentrant_commit() {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let slot = slot(&journal, StateScope::Agent, 1);
        let mut calls = 0;
        let result = slot.update(&access, |_| {
            calls += 1;
            slot.set(&access, json!(2), None)?;
            Ok(Some(json!(3)))
        });
        assert!(matches!(result, Err(StateError::Conflict)));
        assert_eq!(calls, 1);
        assert_eq!(slot.get(&access).unwrap().value, Some(json!(2)));
    }

    #[test]
    fn migrations_are_cas_checked_and_failed_or_newer_values_are_unchanged() {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let old = slot(&journal, StateScope::Agent, 1);
        let revision = old.set(&access, json!(1), None).unwrap();
        let new = slot(&journal, StateScope::Agent, 2);
        assert!(matches!(
            new.get_migrated(&access, |_, _| Err(StateError::InvalidValue(
                MIGRATION_FAILED.into()
            ))),
            Err(StateError::InvalidValue(_))
        ));
        assert_eq!(old.get(&access).unwrap().revision, revision);
        let migrated = new
            .get_migrated(&access, |version, value| {
                assert_eq!(version, 1);
                assert_eq!(value, json!(1));
                Ok(json!(2))
            })
            .unwrap();
        assert_eq!(migrated.value, Some(json!(2)));
        assert!(migrated.revision.0 > revision.0);
        assert!(matches!(
            old.get(&access),
            Err(StateError::NewerVersion {
                stored: 2,
                registered: 1
            })
        ));
        assert_eq!(new.get(&access).unwrap(), migrated);
    }

    #[test]
    fn racing_migration_does_not_overwrite_the_winner_or_rerun_callback() {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let old = slot(&journal, StateScope::Agent, 1);
        old.set(&access, json!(1), None).unwrap();
        let new = slot(&journal, StateScope::Agent, 2);
        let mut calls = 0;
        let result = new.get_migrated(&access, |_, _| {
            calls += 1;
            new.set(&access, json!(2), None)?;
            Ok(json!(3))
        });
        assert!(matches!(result, Err(StateError::Conflict)));
        assert_eq!(calls, 1);
        assert_eq!(new.get(&access).unwrap().value, Some(json!(2)));
    }

    #[test]
    fn revoked_nonce_read_only_and_reset_reject_writes() {
        let (journal, agent) = fixture();
        let turn = TurnId::generate();
        journal.register_state_turn(agent, turn, 1);
        let access = journal.state_access(agent, Some((turn, 1)));
        let slot = slot(&journal, StateScope::Agent, 1);
        slot.set(&access, json!(1), None).unwrap();
        assert!(matches!(
            slot.set(&access.clone().read_only(), json!(2), None),
            Err(StateError::ReadOnly)
        ));
        journal.revoke_state_turn(agent, turn);
        journal.register_state_turn(agent, turn, 2);
        assert!(matches!(
            slot.set(&access, json!(2), None),
            Err(StateError::StaleAccess)
        ));
        let trusted = journal.state_access(agent, None);
        slot.set(&trusted, json!(3), None).unwrap();
        journal.invalidate_state_access();
        assert!(matches!(
            slot.set(&trusted, json!(4), None),
            Err(StateError::StaleAccess)
        ));
        assert!(matches!(slot.get(&trusted), Err(StateError::StaleAccess)));
    }

    #[test_case(true; "install")]
    #[test_case(false; "drop")]
    fn prepared_state_reserves_only_its_address_until_install_or_drop(install: bool) {
        smol::block_on(async {
            let (journal, agent) = fixture();
            let access = journal.state_access(agent, None);
            let slot = slot(&journal, StateScope::Agent, 1);
            let other = journal
                .register_state::<Value>(StateSlotSpec {
                    key: OTHER_KEY.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            let receiver = journal.subscribe_state_changes();
            let edit = slot
                .edit(&access, Some(json!(1)), Some(StateRevision(0)))
                .unwrap();
            let prepared = journal
                .prepare(JournalCommit {
                    state_edits: vec![edit],
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(receiver.try_recv().is_err());
            assert!(matches!(
                slot.set(&access, json!(2), None),
                Err(StateError::Conflict)
            ));
            other.set(&access, json!(3), None).unwrap();
            assert_eq!(receiver.try_recv().unwrap().key, OTHER_KEY);
            if install {
                prepared.commit().unwrap();
                assert_eq!(receiver.try_recv().unwrap().key, KEY);
                assert_eq!(slot.get(&access).unwrap().value, Some(json!(1)));
            } else {
                drop(prepared);
                assert!(receiver.try_recv().is_err());
                assert_eq!(slot.get(&access).unwrap().value, None);
            }
            slot.set(&access, json!(4), None).unwrap();
        });
    }

    #[test_case(false, false; "persistent_identical_replacement")]
    #[test_case(true, false; "persistent_compaction")]
    #[test_case(false, true; "nonpersistent_identical_replacement")]
    #[test_case(true, true; "nonpersistent_compaction")]
    fn identical_history_replacement_fences_only_its_agent_scope(
        compact: bool,
        nonpersistent: bool,
    ) {
        let (journal, agent) = fixture();
        let journal = if nonpersistent {
            let runtime = Arc::new(SessionJournal::nonpersistent());
            runtime
                .commit(JournalCommit {
                    agents: vec![record(agent)],
                    ..Default::default()
                })
                .unwrap();
            runtime
        } else {
            journal
        };
        let binding = journal.incarnation(agent);
        let mut history =
            History::new(Vec::new()).with_journal_bound(binding, Arc::clone(&journal), true);
        let other = AgentId::generate();
        let access = journal.state_access(agent, None);
        let other_access = journal.state_access(other, None);
        let agent_slot = slot(&journal, StateScope::Agent, 1);
        let session_slot = journal
            .register_state::<Value>(StateSlotSpec {
                key: OTHER_KEY.into(),
                scope: StateScope::Session,
                version: 1,
            })
            .unwrap();
        if compact {
            history.replace_compacted(Vec::new()).unwrap();
        } else {
            history.replace_snapshot(HistorySnapshot::default());
        }
        assert!(matches!(
            agent_slot.get(&access),
            Err(StateError::StaleAccess)
        ));
        assert!(agent_slot.get(&other_access).is_ok());
        agent_slot
            .set(&journal.state_access(agent, None), json!(2), None)
            .unwrap();
        assert!(session_slot.set(&access, json!(1), None).is_ok());
        if nonpersistent {
            assert!(!journal.snapshot().managed_histories.contains(&agent));
            assert!(!journal.snapshot().state.iter().any(|record| matches!(
                record.change,
                StateChange::Boundary { .. }
                    | StateChange::Turn { .. }
                    | StateChange::Delivery { .. }
            )));
        }
    }

    #[test]
    fn prepared_history_epoch_is_not_reminted_by_runtime_adoption() {
        smol::block_on(async {
            let (journal, agent) = fixture();
            let mut history = History::new(Vec::new()).with_journal(agent, Arc::clone(&journal));
            let slot = slot(&journal, StateScope::Agent, 1);
            let old_access = journal.state_access(agent, None);
            let snapshot = history.snapshot().clone();
            journal
                .prepare(JournalCommit {
                    histories: [(agent, snapshot.clone())].into(),
                    state_epoch_agents: [agent].into(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .commit()
                .unwrap();
            assert!(matches!(
                slot.get(&old_access),
                Err(StateError::StaleAccess)
            ));
            let installed_access = journal.state_access(agent, None);
            history.adopt_prepared_snapshot(snapshot);
            slot.set(&installed_access, json!(1), None).unwrap();
            assert_eq!(slot.get(&installed_access).unwrap().value, Some(json!(1)));
        });
    }

    #[test]
    fn raw_prepared_plugin_records_reserve_their_slot_address() {
        smol::block_on(async {
            let (journal, agent) = fixture();
            let access = journal.state_access(agent, None);
            let slot = slot(&journal, StateScope::Agent, 1);
            let mut prepared = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(agent),
                        generation: 0,
                        change: StateChange::Plugin {
                            key: KEY.into(),
                            version: 1,
                            value: Some(json!(1)),
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(matches!(
                slot.set(&access, json!(2), None),
                Err(StateError::Conflict)
            ));
            let receiver = journal.subscribe_state_changes();
            prepared.install().unwrap();
            assert_eq!(receiver.try_recv().unwrap().key, KEY);
            prepared.install().unwrap();
            assert!(receiver.try_recv().is_err());
            assert_eq!(slot.get(&access).unwrap().value, Some(json!(1)));
        });
    }

    #[test]
    fn staged_history_rotation_preserves_pending_conversion_epoch() {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let slot = slot(&journal, StateScope::Agent, 1);
        let epoch = journal.snapshot().epoch;
        let pending = journal
            .stage(
                JournalCommit {
                    state_epoch_agents: [agent].into(),
                    ..Default::default()
                },
                1,
            )
            .unwrap();
        assert!(matches!(slot.get(&access), Err(StateError::StaleAccess)));
        journal
            .complete(
                pending,
                vec![JournalChange {
                    agent: Some(agent),
                    generation: 0,
                    change: StateChange::Compaction { epoch: 1 },
                }],
            )
            .unwrap();
        assert_eq!(journal.snapshot().epoch, epoch);
        assert!(
            slot.set(&journal.state_access(agent, None), json!(1), None)
                .is_ok()
        );
    }

    #[test]
    fn prepared_revision_conflict_fails_before_any_slot_is_reserved() {
        smol::block_on(async {
            let (journal, agent) = fixture();
            let access = journal.state_access(agent, None);
            let slot = slot(&journal, StateScope::Agent, 1);
            let edit = slot
                .edit(&access, Some(json!(1)), Some(StateRevision(0)))
                .unwrap();
            slot.set(&access, json!(2), None).unwrap();
            let before = journal.snapshot();
            assert!(matches!(
                journal
                    .prepare(JournalCommit {
                        state_edits: vec![edit],
                        ..Default::default()
                    })
                    .await,
                Err(crate::journal::JournalError::State(StateError::Conflict))
            ));
            assert_eq!(journal.snapshot().revision, before.revision);
            slot.set(&access, json!(3), None).unwrap();
        });
    }

    #[test_case(false; "ordinary_commit")]
    #[test_case(true; "prepared_install")]
    fn concurrent_commit_and_transition_notices_follow_install_order(prepared: bool) {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let state_slot = slot(&journal, StateScope::Agent, 1);
        let receiver = journal.subscribe_state_changes();
        let (entered, entry) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        journal.lock().plugin_state.dispatch_gate = Some((entered, released));
        let first_access = access.clone();
        let first_slot = state_slot.clone();
        let first_journal = Arc::clone(&journal);
        let first = std::thread::spawn(move || {
            if prepared {
                let edit = first_slot
                    .edit(&first_access, Some(json!(1)), Some(StateRevision(0)))
                    .unwrap();
                smol::block_on(first_journal.prepare(JournalCommit {
                    state_edits: vec![edit],
                    ..Default::default()
                }))
                .unwrap()
                .commit()
                .unwrap();
            } else {
                first_slot.set(&first_access, json!(1), None).unwrap();
            }
        });
        entry.recv().unwrap();
        let first_revision = state_slot.get(&access).unwrap().revision;
        journal.notify_state_transition(StateChangeReason::Resume);
        let second_revision = state_slot.set(&access, json!(2), None).unwrap();
        journal.notify_state_transition(StateChangeReason::Rewind);
        assert!(receiver.try_recv().is_err());
        release.send(()).unwrap();
        first.join().unwrap();
        let events = receiver.try_iter().collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|event| (event.revision, event.reason))
                .collect::<Vec<_>>(),
            vec![
                (first_revision, StateChangeReason::Commit),
                (first_revision, StateChangeReason::Resume),
                (second_revision, StateChangeReason::Commit),
                (second_revision, StateChangeReason::Rewind),
            ]
        );
    }

    #[test]
    fn transition_receipts_follow_install_and_skip_tombstones() {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let slot = slot(&journal, StateScope::Agent, 1);
        slot.set(&access, json!(1), None).unwrap();
        let receiver = journal.subscribe_state_changes();
        for reason in [StateChangeReason::Resume, StateChangeReason::Rewind] {
            journal.notify_state_transition(reason);
            assert_eq!(receiver.try_recv().unwrap().reason, reason);
            assert_eq!(slot.get(&access).unwrap().value, Some(json!(1)));
        }
        slot.delete(&access, None).unwrap();
        receiver.try_recv().unwrap();
        journal.notify_state_transition(StateChangeReason::Resume);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn full_record_validation_rejects_values_the_decoder_cannot_read() {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let slot = slot(&journal, StateScope::Agent, 1);
        let mut value = Value::Null;
        for _ in 0..=makima_storage::session_state::MAX_PLUGIN_VALUE_DEPTH {
            value = json!([value]);
        }
        assert!(matches!(
            slot.set(&access, value, None),
            Err(StateError::InvalidValue(_))
        ));
        assert_eq!(slot.get(&access).unwrap().value, None);
        journal
            .commit(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(agent),
                    generation: 0,
                    change: StateChange::Plugin {
                        key: KEY.into(),
                        version: 1,
                        value: Some(Value::Null),
                    },
                }],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(slot.get(&access).unwrap().value, Some(Value::Null));
    }

    #[test]
    fn cancellation_observes_the_completed_synchronous_state_effect() {
        let (journal, agent) = fixture();
        let access = journal.state_access(agent, None);
        let state_slot = slot(&journal, StateScope::Agent, 1);
        let (cancel, token) = CancelToken::new();
        let effect_token = token.clone();
        let (entered, entry) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let effect = std::thread::spawn(move || {
            CancelToken::with_effect(&[&effect_token], || {
                entered.send(()).unwrap();
                released.recv().unwrap();
                state_slot.set(&access, json!(1), None).unwrap()
            })
        });
        entry.recv().unwrap();
        let (cancelling, started) = flume::bounded(1);
        let cancellation = std::thread::spawn(move || {
            cancelling.send(()).unwrap();
            cancel.cancel();
        });
        started.recv().unwrap();
        assert!(!token.is_cancelled());
        release.send(()).unwrap();
        assert!(effect.join().unwrap().is_some());
        cancellation.join().unwrap();
        assert!(token.is_cancelled());
        assert_eq!(
            slot(&journal, StateScope::Agent, 1)
                .get(&journal.state_access(agent, None))
                .unwrap()
                .value,
            Some(json!(1))
        );
    }

    #[test]
    fn cancellation_effect_deduplicates_parent_child_gates() {
        let (cancel, token) = CancelToken::new();
        let (_child_cancel, child) = token.child();
        assert_eq!(
            CancelToken::with_effect(&[&token, &child, &token], || 1),
            Some(1)
        );
        cancel.cancel();
        assert_eq!(CancelToken::with_effect(&[&token, &child], || 2), None);
    }
}
