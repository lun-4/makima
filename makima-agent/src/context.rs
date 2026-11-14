use crate::{
    AgentEvent, AgentId, EventSender,
    agent::{InstructionSource, Instructions},
    journal::{JournalChange, SessionJournalSnapshot},
    prompt::{PromptId, PromptSource, ResolvedSlots, Slot},
    state::{StateAccess, StateError, StateScope, validate_access},
};
use makima_domain::{Post, PostLevel};
use makima_storage::session_state::{StateChange, StoredAgentLifecycle};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

pub const CONTEXT_CHANGED_NOTICE: &str = "instructions changed on disk, run /reload-context";
pub(crate) const CONTEXT_MANIFEST: &str = "makima/context-manifest";
const MANIFEST_VERSION: u32 = 1;

#[derive(Clone)]
pub enum ContextPolicy {
    Admission,
    Reload,
}

#[derive(Clone)]
pub struct RootContext {
    pub instructions: Instructions,
    pub slots: Arc<ResolvedSlots>,
}

#[derive(Clone)]
pub struct ContextBinding {
    pub(crate) access: StateAccess,
    recorded: RecordedContext,
}

#[derive(Clone, Debug)]
pub struct ContextUpdate {
    pub(crate) access: StateAccess,
    pub(crate) expected: Option<String>,
    pub(crate) changes: Vec<JournalChange>,
    pub(crate) notice: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    sources: Vec<Source>,
    hashes: BTreeMap<String, String>,
    seen_observations: Vec<String>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Source {
    Instruction {
        path: PathBuf,
        label: String,
    },
    Prompt {
        prompt: PromptId,
        slot: Slot,
        plugin: String,
        registration: usize,
    },
}

#[derive(Serialize)]
enum SourceKey<'a> {
    Instruction {
        path: &'a PathBuf,
    },
    Prompt {
        prompt: PromptId,
        slot: Slot,
        plugin: &'a str,
        registration: usize,
    },
}

#[derive(Serialize)]
enum Observation<'a> {
    Source {
        key: &'a str,
        observed: Option<(&'a Source, &'a Option<String>)>,
    },
    Order {
        group: &'a str,
        keys: &'a [String],
    },
}

#[derive(Clone)]
struct RecordedContext {
    expected: Option<String>,
    manifest: Manifest,
    content: BTreeMap<String, Option<String>>,
}

impl Source {
    fn key(&self) -> Result<String, StateError> {
        let key = match self {
            Self::Instruction { path, .. } => SourceKey::Instruction { path },
            Self::Prompt {
                prompt,
                slot,
                plugin,
                registration,
            } => SourceKey::Prompt {
                prompt: *prompt,
                slot: *slot,
                plugin,
                registration: *registration,
            },
        };
        encode(&key)
    }

    fn is_prompt(&self) -> bool {
        matches!(self, Self::Prompt { .. })
    }
}

impl ContextBinding {
    pub fn capture(access: StateAccess) -> Result<Self, StateError> {
        let recorded = {
            let state = access.journal.lock();
            validate_access(
                &state.plugin_state,
                &state.snapshot,
                &access,
                StateScope::Agent,
                false,
                None,
            )?;
            if root_agent(&state.snapshot, access.target)? != access.target {
                return Err(StateError::InvalidValue(
                    "only root agents can reconcile context".into(),
                ));
            }
            RecordedContext::read(&state.snapshot, access.target)?
        };
        Ok(Self { access, recorded })
    }

    pub fn prepare(
        &self,
        instructions: &Instructions,
        slots: &ResolvedSlots,
        slots_complete: bool,
        policy: ContextPolicy,
    ) -> Result<(Arc<RootContext>, ContextUpdate), StateError> {
        let observed = collect_sources(instructions, slots, slots_complete);
        let (effective, changes, notice) =
            self.recorded
                .reconcile(observed, slots_complete, policy, self.access.target)?;
        Ok((
            Arc::new(effective),
            ContextUpdate {
                access: self.access.clone(),
                expected: self.recorded.expected.clone(),
                changes,
                notice,
            },
        ))
    }
}

impl ContextUpdate {
    pub fn notify(&self, sender: &EventSender) {
        if self.notice {
            let _ = sender.send(AgentEvent::Post(Post {
                text: CONTEXT_CHANGED_NOTICE.into(),
                level: PostLevel::Warn,
                agent: None,
            }));
        }
    }
}

pub(crate) fn manifest_hash(snapshot: &SessionJournalSnapshot, root: AgentId) -> Option<String> {
    snapshot.state.iter().rev().find_map(|record| {
        if record.agent == Some(root)
            && let StateChange::Context { source, hash, .. } = &record.change
            && source == CONTEXT_MANIFEST
        {
            return Some(hash.clone());
        }
        None
    })
}

pub fn root_context(access: &StateAccess) -> Result<Option<Arc<RootContext>>, StateError> {
    let state = access.journal.lock();
    validate_access(
        &state.plugin_state,
        &state.snapshot,
        access,
        StateScope::Agent,
        false,
        None,
    )?;
    let root = root_agent(&state.snapshot, access.target)?;
    let recorded = RecordedContext::read(&state.snapshot, root)?;
    recorded
        .expected
        .as_ref()
        .map(|_| recorded.effective().map(Arc::new))
        .transpose()
}

fn root_agent(snapshot: &SessionJournalSnapshot, target: AgentId) -> Result<AgentId, StateError> {
    let mut current = target;
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(current) {
            return Err(StateError::InvalidValue("cyclic context ancestry".into()));
        }
        let record = snapshot
            .agent_revisions
            .iter()
            .rev()
            .find(|(_, record)| record.id == current)
            .map(|(_, record)| record)
            .ok_or(StateError::StaleAccess)?;
        if record.lifecycle == StoredAgentLifecycle::Retired {
            return Err(StateError::StaleAccess);
        }
        match record.parent {
            Some(parent) => current = parent,
            None => return Ok(current),
        }
    }
}

impl RecordedContext {
    fn read(snapshot: &SessionJournalSnapshot, root: AgentId) -> Result<Self, StateError> {
        let mut records = BTreeMap::new();
        for record in &snapshot.state {
            if record.agent == Some(root)
                && let StateChange::Context {
                    source,
                    hash,
                    content,
                } = &record.change
            {
                records.insert(source.as_str(), (hash, content));
            }
        }
        let Some((hash, content)) = records.get(CONTEXT_MANIFEST) else {
            return Ok(Self {
                expected: None,
                manifest: Manifest {
                    version: MANIFEST_VERSION,
                    sources: Vec::new(),
                    hashes: BTreeMap::new(),
                    seen_observations: Vec::new(),
                },
                content: BTreeMap::new(),
            });
        };
        validate_hash(hash, content)?;
        let manifest: Manifest = serde_json::from_str(content).map_err(invalid_value)?;
        if manifest.version != MANIFEST_VERSION {
            return Err(StateError::InvalidValue(format!(
                "unsupported context manifest version {}",
                manifest.version
            )));
        }
        let mut active = BTreeMap::new();
        for source in &manifest.sources {
            let key = source.key()?;
            let (hash, content) = records.get(key.as_str()).ok_or_else(|| {
                StateError::InvalidValue(format!("missing recorded context source {key}"))
            })?;
            validate_hash(hash, content)?;
            if manifest.hashes.get(&key) != Some(*hash) {
                return Err(StateError::InvalidValue(
                    "recorded context source differs from manifest hash".into(),
                ));
            }
            let payload = if source.is_prompt() {
                serde_json::from_str::<Option<String>>(content).map_err(invalid_value)?
            } else {
                Some((*content).clone())
            };
            if active.insert(key, payload).is_some() {
                return Err(StateError::InvalidValue(
                    "duplicate recorded context source".into(),
                ));
            }
        }
        if manifest.hashes.len() != active.len() {
            return Err(StateError::InvalidValue(
                "context manifest hash set differs from active sources".into(),
            ));
        }
        Ok(Self {
            expected: Some((*hash).clone()),
            manifest,
            content: active,
        })
    }

    fn effective(&self) -> Result<RootContext, StateError> {
        let mut instructions = Vec::new();
        let mut slots = ResolvedSlots::default();
        for source in &self.manifest.sources {
            let key = source.key()?;
            let content = self.content.get(&key).ok_or_else(|| {
                StateError::InvalidValue(format!("missing recorded context source {key}"))
            })?;
            match source {
                Source::Instruction { path, label } => instructions.push(InstructionSource {
                    path: path.clone(),
                    label: label.clone(),
                    content: content.clone().ok_or_else(|| {
                        StateError::InvalidValue("missing instruction content".into())
                    })?,
                }),
                Source::Prompt {
                    prompt,
                    slot,
                    plugin,
                    registration,
                } => slots.observe(PromptSource {
                    prompt: *prompt,
                    slot: *slot,
                    plugin: Arc::from(plugin.as_str()),
                    registration: *registration,
                    content: content.clone(),
                }),
            }
        }
        Ok(RootContext {
            instructions: Instructions::from_sources(instructions),
            slots: Arc::new(slots),
        })
    }

    fn observe(&mut self, observation: Observation<'_>) -> Result<bool, StateError> {
        let hash = digest(&encode(&observation)?);
        if self.manifest.seen_observations.contains(&hash) {
            return Ok(false);
        }
        self.manifest.seen_observations.push(hash);
        Ok(true)
    }

    fn reconcile(
        &self,
        observed: Vec<(Source, Option<String>)>,
        slots_complete: bool,
        policy: ContextPolicy,
        root: AgentId,
    ) -> Result<(RootContext, Vec<JournalChange>, bool), StateError> {
        let keys: Vec<_> = observed
            .iter()
            .map(|(source, _)| source.key())
            .collect::<Result<_, _>>()?;
        let observed_keys: BTreeSet<_> = keys.iter().cloned().collect();
        if observed_keys.len() != keys.len() {
            return Err(StateError::InvalidValue(
                "duplicate observed context source".into(),
            ));
        }
        let reload = matches!(policy, ContextPolicy::Reload);
        let mut next = self.clone();
        let mut changes = Vec::new();
        let mut notice = false;
        match policy {
            ContextPolicy::Reload => {
                next.manifest.sources = observed
                    .iter()
                    .filter(|(_, content)| content.is_some())
                    .map(|(source, _)| source.clone())
                    .collect();
                if !slots_complete {
                    next.manifest.sources.extend(
                        self.manifest
                            .sources
                            .iter()
                            .filter(|source| source.is_prompt())
                            .cloned(),
                    );
                }
                next.manifest.seen_observations.clear();
            }
            ContextPolicy::Admission => {
                let old: BTreeMap<_, _> = self
                    .manifest
                    .sources
                    .iter()
                    .map(|source| Ok((source.key()?, source)))
                    .collect::<Result<_, StateError>>()?;
                for ((source, content), key) in observed.iter().zip(&keys) {
                    if old.get(key).is_some_and(|previous| {
                        *previous != source || self.content.get(key) != Some(content)
                    }) {
                        notice |= next.observe(Observation::Source {
                            key,
                            observed: Some((source, content)),
                        })?;
                    }
                }
                for (key, source) in &old {
                    if (slots_complete || !source.is_prompt()) && !observed_keys.contains(key) {
                        notice |= next.observe(Observation::Source {
                            key,
                            observed: None,
                        })?;
                    }
                }
                let common: BTreeSet<_> = keys
                    .iter()
                    .filter(|key| old.contains_key(*key))
                    .cloned()
                    .collect();
                let recorded_order = source_order(self.manifest.sources.iter(), &common)?;
                let observed_order =
                    source_order(observed.iter().map(|(source, _)| source), &common)?;
                for (group, keys) in &observed_order {
                    if recorded_order.get(group) != Some(keys) {
                        notice |= next.observe(Observation::Order { group, keys })?;
                    }
                }
                if observed
                    .iter()
                    .zip(&keys)
                    .any(|((_, content), key)| content.is_some() && !old.contains_key(key))
                {
                    let all = observed
                        .iter()
                        .zip(&keys)
                        .filter(|((_, content), key)| content.is_some() || old.contains_key(*key))
                        .map(|(_, key)| key.clone())
                        .collect();
                    for (group, keys) in
                        source_order(observed.iter().map(|(source, _)| source), &all)?
                    {
                        next.observe(Observation::Order {
                            group: &group,
                            keys: &keys,
                        })?;
                    }
                }
                next.manifest.sources.extend(
                    observed
                        .iter()
                        .zip(&keys)
                        .filter(|((_, content), key)| content.is_some() && !old.contains_key(*key))
                        .map(|((source, _), _)| source.clone()),
                );
            }
        }
        for ((source, content), key) in observed.iter().zip(keys) {
            if content.is_some() && (reload || !self.content.contains_key(&key)) {
                if self.content.get(&key) != Some(content) {
                    let payload = source_payload(source, content)?;
                    changes.push(context_change(root, key.clone(), payload));
                }
                next.content.insert(key, content.clone());
            }
        }
        next.manifest.hashes = next
            .manifest
            .sources
            .iter()
            .map(|source| {
                let key = source.key()?;
                let content = next.content.get(&key).ok_or_else(|| {
                    StateError::InvalidValue(format!("missing recorded context source {key}"))
                })?;
                Ok((key, digest(&source_payload(source, content)?)))
            })
            .collect::<Result<_, StateError>>()?;
        let content = encode(&next.manifest)?;
        if self.expected.as_ref() != Some(&digest(&content)) {
            changes.push(context_change(root, CONTEXT_MANIFEST.into(), content));
        }
        Ok((next.effective()?, changes, notice))
    }
}

fn collect_sources(
    instructions: &Instructions,
    slots: &ResolvedSlots,
    slots_complete: bool,
) -> Vec<(Source, Option<String>)> {
    let mut sources: Vec<_> = instructions
        .sources
        .iter()
        .map(|source| {
            (
                Source::Instruction {
                    path: source.path.clone(),
                    label: source.label.clone(),
                },
                Some(source.content.clone()),
            )
        })
        .collect();
    if slots_complete {
        sources.extend(slots.sources().into_iter().map(|source| {
            (
                Source::Prompt {
                    prompt: source.prompt,
                    slot: source.slot,
                    plugin: source.plugin.to_string(),
                    registration: source.registration,
                },
                source.content,
            )
        }));
    }
    sources
}

fn source_order<'a>(
    sources: impl Iterator<Item = &'a Source>,
    common: &BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<String>>, StateError> {
    let mut order = BTreeMap::<String, Vec<String>>::new();
    for source in sources {
        let key = source.key()?;
        if common.contains(&key) {
            let group = match source {
                Source::Instruction { .. } => String::new(),
                Source::Prompt { prompt, slot, .. } => encode(&(prompt, slot))?,
            };
            order.entry(group).or_default().push(key);
        }
    }
    Ok(order)
}

fn source_payload(source: &Source, content: &Option<String>) -> Result<String, StateError> {
    if source.is_prompt() {
        encode(content)
    } else {
        content
            .clone()
            .ok_or_else(|| StateError::InvalidValue("missing instruction content".into()))
    }
}

fn context_change(root: AgentId, source: String, content: String) -> JournalChange {
    JournalChange {
        agent: Some(root),
        generation: 0,
        change: StateChange::Context {
            source,
            hash: digest(&content),
            content,
        },
    }
}

fn encode(value: &impl Serialize) -> Result<String, StateError> {
    serde_json::to_string(value).map_err(invalid_value)
}

fn digest(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

fn validate_hash(hash: &str, content: &str) -> Result<(), StateError> {
    if hash != digest(content) {
        return Err(StateError::InvalidValue(
            "recorded context hash mismatch".into(),
        ));
    }
    Ok(())
}

fn invalid_value(error: serde_json::Error) -> StateError {
    StateError::InvalidValue(format!("invalid context manifest: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        CONTEXT_CHANGED_NOTICE, CONTEXT_MANIFEST, ContextBinding, ContextPolicy, ContextUpdate,
        MANIFEST_VERSION, Manifest, RootContext, Source, context_change, encode, manifest_hash,
        root_context,
    };
    use crate::{
        AgentEvent, AgentId, EventSender,
        agent::{InstructionSource, Instructions},
        journal::{JournalCommit, JournalError, SessionJournal},
        prompt::{PromptId, PromptSource, ResolvedSlots, Slot},
        state::StateError,
    };
    use makima_domain::PostLevel;
    use makima_storage::session_state::{StateChange, StoredAgentLifecycle, StoredAgentRecord};
    use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
    use test_case::test_case;

    const FIRST: &str = "first";
    const SECOND: &str = "second";
    const THIRD: &str = "third";
    const CHANGED: &str = "changed";
    const LABEL: &str = "Project instructions";
    const OTHER_LABEL: &str = "Other instructions";
    const PLUGIN: &str = "fixture/plugin";
    const INVALID_HASH: &str = "invalid hash";

    struct Fixture {
        journal: Arc<SessionJournal>,
        root: AgentId,
    }

    fn record(id: AgentId, parent: Option<AgentId>) -> StoredAgentRecord {
        StoredAgentRecord {
            id,
            parent,
            depth: u32::from(parent.is_some()),
            transcript_key: id.to_string(),
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

    impl Fixture {
        fn new() -> Self {
            let journal = Arc::new(SessionJournal::nonpersistent());
            let root = AgentId::generate();
            journal
                .commit(JournalCommit {
                    agents: vec![record(root, None)],
                    ..Default::default()
                })
                .unwrap();
            Self { journal, root }
        }

        fn prepare(
            &self,
            instructions: &Instructions,
            slots: &ResolvedSlots,
            complete: bool,
            policy: ContextPolicy,
        ) -> (Arc<RootContext>, ContextUpdate) {
            ContextBinding::capture(self.journal.state_access(self.root, None))
                .unwrap()
                .prepare(instructions, slots, complete, policy)
                .unwrap()
        }

        fn commit(&self, update: ContextUpdate) {
            self.journal
                .commit(JournalCommit {
                    context_updates: vec![update],
                    ..Default::default()
                })
                .unwrap();
        }

        fn read(&self) -> Arc<RootContext> {
            root_context(&self.journal.state_access(self.root, None))
                .unwrap()
                .unwrap()
        }
    }

    fn instructions(sources: &[(&str, &str, &str)]) -> Instructions {
        Instructions::from_sources(
            sources
                .iter()
                .map(|(path, label, content)| InstructionSource {
                    path: PathBuf::from(path),
                    label: (*label).into(),
                    content: (*content).into(),
                })
                .collect(),
        )
    }

    fn slots(sources: &[(usize, Option<&str>)]) -> ResolvedSlots {
        let mut slots = ResolvedSlots::default();
        for (registration, content) in sources {
            slots.observe(PromptSource {
                prompt: PromptId::System,
                slot: Slot::Conventions,
                plugin: Arc::from(PLUGIN),
                registration: *registration,
                content: content.map(Into::into),
            });
        }
        slots
    }

    #[test]
    fn empty_manifest_is_recorded_and_prepare_does_not_publish() {
        let fixture = Fixture::new();
        let access = fixture.journal.state_access(fixture.root, None);
        assert!(root_context(&access).unwrap().is_none());
        let (effective, update) = fixture.prepare(
            &Instructions::default(),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        assert!(effective.instructions.text.is_empty());
        assert!(!update.notice);
        assert_eq!(update.expected, None);
        assert_eq!(update.changes.len(), 1);
        assert!(root_context(&access).unwrap().is_none());
        fixture.commit(update);
        assert!(fixture.read().instructions.text.is_empty());
        let (_, update) = fixture.prepare(
            &Instructions::default(),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        assert!(update.expected.is_some());
        assert!(update.changes.is_empty());
    }

    #[test]
    fn changed_and_removed_sources_keep_recorded_text_and_labels() {
        let fixture = Fixture::new();
        let original = instructions(&[(FIRST, LABEL, FIRST), (SECOND, LABEL, SECOND)]);
        let (_, initial) = fixture.prepare(
            &original,
            &slots(&[(0, Some(FIRST))]),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(initial);
        let observed = instructions(&[(FIRST, OTHER_LABEL, CHANGED), (THIRD, LABEL, THIRD)]);
        let (effective, update) = fixture.prepare(
            &observed,
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        assert!(update.notice);
        assert_eq!(effective.instructions.sources.len(), 3);
        assert_eq!(effective.instructions.sources[0].content, FIRST);
        assert_eq!(effective.instructions.sources[0].label, LABEL);
        assert_eq!(effective.instructions.sources[1].content, SECOND);
        assert_eq!(effective.instructions.sources[2].content, THIRD);
        assert_eq!(effective.slots.sources()[0].content.as_deref(), Some(FIRST));
        assert!(
            effective
                .instructions
                .loaded
                .contains_or_insert(FIRST.into())
        );
        fixture.commit(update);
        let (_, repeated) = fixture.prepare(
            &observed,
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        assert!(!repeated.notice);
        assert!(repeated.changes.is_empty());
        assert_eq!(
            fixture.read().instructions.text,
            effective.instructions.text
        );
    }

    #[test]
    fn notice_dedup_is_per_source_and_observed_hash_across_recapture() {
        let fixture = Fixture::new();
        let original = instructions(&[(FIRST, LABEL, FIRST)]);
        let (_, update) = fixture.prepare(
            &original,
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(update);
        for (observed, expected_notice) in [
            (vec![(FIRST, LABEL, CHANGED)], true),
            (
                vec![(FIRST, LABEL, CHANGED), (SECOND, LABEL, SECOND)],
                false,
            ),
            (vec![(FIRST, LABEL, THIRD), (SECOND, LABEL, SECOND)], true),
            (
                vec![(FIRST, LABEL, CHANGED), (SECOND, LABEL, SECOND)],
                false,
            ),
        ] {
            let (_, update) = fixture.prepare(
                &instructions(&observed),
                &ResolvedSlots::default(),
                true,
                ContextPolicy::Admission,
            );
            assert_eq!(update.notice, expected_notice);
            fixture.commit(update);
        }
        assert_eq!(fixture.read().instructions.sources[0].content, FIRST);
    }

    #[test]
    fn appended_first_sight_sources_do_not_create_later_order_notices() {
        let fixture = Fixture::new();
        for order in [
            vec![(FIRST, LABEL, FIRST), (THIRD, LABEL, THIRD)],
            vec![
                (FIRST, LABEL, FIRST),
                (SECOND, LABEL, SECOND),
                (THIRD, LABEL, THIRD),
            ],
            vec![
                (FIRST, LABEL, FIRST),
                (SECOND, LABEL, SECOND),
                (THIRD, LABEL, THIRD),
            ],
        ] {
            let (_, update) = fixture.prepare(
                &instructions(&order),
                &ResolvedSlots::default(),
                true,
                ContextPolicy::Admission,
            );
            assert!(!update.notice);
            fixture.commit(update);
        }
        let recorded = fixture.read();
        assert_eq!(recorded.instructions.sources[1].content, THIRD);
        assert_eq!(recorded.instructions.sources[2].content, SECOND);
        let (_, reordered) = fixture.prepare(
            &instructions(&[
                (SECOND, LABEL, SECOND),
                (FIRST, LABEL, FIRST),
                (THIRD, LABEL, THIRD),
            ]),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        assert!(reordered.notice);
        fixture.commit(reordered);
        let (_, repeated) = fixture.prepare(
            &instructions(&[
                (SECOND, LABEL, SECOND),
                (FIRST, LABEL, FIRST),
                (THIRD, LABEL, THIRD),
            ]),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        assert!(!repeated.notice);
    }

    #[test_case(Some(""); "empty_first_content")]
    #[test_case(Some(FIRST); "nonempty_first_content")]
    fn unseen_absence_is_not_recorded_and_first_content_is_admitted(content: Option<&str>) {
        let fixture = Fixture::new();
        let (_, initial) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, None)]),
            true,
            ContextPolicy::Admission,
        );
        assert!(!initial.notice);
        assert!(initial.changes.iter().all(|change| matches!(
            &change.change,
            StateChange::Context { source, .. } if source == CONTEXT_MANIFEST
        )));
        fixture.commit(initial);
        assert!(fixture.read().slots.sources().is_empty());
        let (_, repeated) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, None)]),
            true,
            ContextPolicy::Admission,
        );
        assert!(!repeated.notice);
        assert!(repeated.changes.is_empty());
        let (effective, first_content) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, content)]),
            true,
            ContextPolicy::Admission,
        );
        assert!(!first_content.notice);
        assert_eq!(effective.slots.sources()[0].content.as_deref(), content);
        fixture.commit(first_content);
        let (effective, absent) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, None)]),
            true,
            ContextPolicy::Admission,
        );
        assert!(absent.notice);
        assert_eq!(effective.slots.sources()[0].content.as_deref(), content);
    }

    #[test_case(false; "instructions")]
    #[test_case(true; "prompts")]
    fn content_only_reload_changes_manifest_and_rejects_stale_no_write_update(prompt: bool) {
        let fixture = Fixture::new();
        let baseline_instructions = if prompt {
            Instructions::default()
        } else {
            instructions(&[(FIRST, LABEL, FIRST)])
        };
        let baseline_slots = if prompt {
            slots(&[(0, Some(FIRST))])
        } else {
            ResolvedSlots::default()
        };
        let (_, initial) = fixture.prepare(
            &baseline_instructions,
            &baseline_slots,
            true,
            ContextPolicy::Reload,
        );
        fixture.commit(initial);
        let binding =
            ContextBinding::capture(fixture.journal.state_access(fixture.root, None)).unwrap();
        let (_, unchanged_reload) = binding
            .prepare(
                &baseline_instructions,
                &baseline_slots,
                true,
                ContextPolicy::Reload,
            )
            .unwrap();
        assert!(unchanged_reload.changes.is_empty());
        let (_, stale) = binding
            .prepare(
                &baseline_instructions,
                &baseline_slots,
                true,
                ContextPolicy::Admission,
            )
            .unwrap();
        assert!(stale.changes.is_empty());
        let changed_instructions = if prompt {
            Instructions::default()
        } else {
            instructions(&[(FIRST, LABEL, CHANGED)])
        };
        let changed_slots = if prompt {
            slots(&[(0, Some(CHANGED))])
        } else {
            ResolvedSlots::default()
        };
        let (_, competing) = binding
            .prepare(
                &changed_instructions,
                &changed_slots,
                true,
                ContextPolicy::Reload,
            )
            .unwrap();
        assert_eq!(competing.expected, stale.expected);
        assert!(competing.changes.iter().any(|change| matches!(
            &change.change,
            StateChange::Context { source, .. } if source == CONTEXT_MANIFEST
        )));
        fixture.commit(competing);
        assert_ne!(
            manifest_hash(&fixture.journal.snapshot(), fixture.root),
            stale.expected
        );
        assert!(matches!(
            fixture.journal.commit(JournalCommit {
                context_updates: vec![stale],
                ..Default::default()
            }),
            Err(JournalError::State(StateError::Conflict))
        ));
        let recorded = fixture.read();
        if prompt {
            assert_eq!(
                recorded.slots.sources()[0].content.as_deref(),
                Some(CHANGED)
            );
        } else {
            assert_eq!(recorded.instructions.sources[0].content, CHANGED);
        }
    }

    #[test]
    fn internally_valid_source_replacement_must_match_manifest_payload_hash() {
        let fixture = Fixture::new();
        let (_, initial) = fixture.prepare(
            &instructions(&[(FIRST, LABEL, FIRST)]),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(initial);
        let source = Source::Instruction {
            path: FIRST.into(),
            label: LABEL.into(),
        };
        fixture
            .journal
            .commit(JournalCommit {
                changes: vec![context_change(
                    fixture.root,
                    source.key().unwrap(),
                    CHANGED.into(),
                )],
                ..Default::default()
            })
            .unwrap();
        assert!(matches!(
            root_context(&fixture.journal.state_access(fixture.root, None)),
            Err(StateError::InvalidValue(_))
        ));
    }

    #[test_case(Some(""), None; "empty_to_absent")]
    #[test_case(Some(FIRST), None; "content_to_absent")]
    fn prompt_absence_is_frozen_distinct_from_empty_and_reloadable(
        recorded: Option<&str>,
        observed: Option<&str>,
    ) {
        let fixture = Fixture::new();
        let (_, initial) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, recorded)]),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(initial);
        let (effective, changed) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, observed)]),
            true,
            ContextPolicy::Admission,
        );
        assert!(changed.notice);
        assert_eq!(effective.slots.sources()[0].content.as_deref(), recorded);
        assert_eq!(
            effective
                .slots
                .get(PromptId::System, Slot::Conventions)
                .len(),
            usize::from(recorded.is_some())
        );
        fixture.commit(changed);
        let (effective, reload) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, observed)]),
            true,
            ContextPolicy::Reload,
        );
        assert!(!reload.notice);
        assert!(effective.slots.sources().is_empty());
        fixture.commit(reload);
        assert!(fixture.read().slots.sources().is_empty());
        let (_, changed_again) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, recorded)]),
            true,
            ContextPolicy::Admission,
        );
        assert!(!changed_again.notice);
    }

    #[test_case(ContextPolicy::Admission; "admission")]
    #[test_case(ContextPolicy::Reload; "reload")]
    fn incomplete_collection_preserves_all_prompt_sources(policy: ContextPolicy) {
        let fixture = Fixture::new();
        let (_, update) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, Some(FIRST)), (1, None)]),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(update);
        let (effective, update) = fixture.prepare(
            &instructions(&[(FIRST, LABEL, FIRST)]),
            &slots(&[(0, Some(CHANGED)), (2, Some(SECOND))]),
            false,
            policy,
        );
        assert!(!update.notice);
        assert_eq!(effective.instructions.sources.len(), 1);
        let sources = effective.slots.sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].content.as_deref(), Some(FIRST));
        fixture.commit(update);
        assert_eq!(fixture.read().slots.sources().len(), 1);
    }

    #[test]
    fn reload_exact_empty_manifest_drops_orphans_and_resets_notice_dedup() {
        let fixture = Fixture::new();
        let (_, initial) = fixture.prepare(
            &instructions(&[(FIRST, LABEL, FIRST)]),
            &slots(&[(0, Some(FIRST))]),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(initial);
        let (_, reload) = fixture.prepare(
            &Instructions::default(),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Reload,
        );
        fixture.commit(reload);
        assert!(fixture.read().instructions.sources.is_empty());
        assert!(fixture.read().slots.sources().is_empty());
        let (_, first_sight) = fixture.prepare(
            &instructions(&[(FIRST, OTHER_LABEL, CHANGED)]),
            &slots(&[(0, None)]),
            true,
            ContextPolicy::Admission,
        );
        assert!(!first_sight.notice);
        fixture.commit(first_sight);
        assert_eq!(fixture.read().instructions.sources[0].content, CHANGED);
        assert!(fixture.read().slots.sources().is_empty());
    }

    #[test]
    fn descendant_reads_root_after_validating_own_access_and_latest_ancestry() {
        let fixture = Fixture::new();
        let child = AgentId::generate();
        fixture
            .journal
            .commit(JournalCommit {
                agents: vec![record(child, Some(fixture.root))],
                ..Default::default()
            })
            .unwrap();
        let (_, update) = fixture.prepare(
            &instructions(&[(FIRST, LABEL, FIRST)]),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(update);
        let child_access = fixture.journal.state_access(child, None);
        assert_eq!(
            root_context(&child_access)
                .unwrap()
                .unwrap()
                .instructions
                .sources[0]
                .content,
            FIRST
        );
        assert!(matches!(
            ContextBinding::capture(child_access.clone()),
            Err(StateError::InvalidValue(_))
        ));
        let read_only = child_access.clone().read_only();
        assert!(root_context(&read_only).unwrap().is_some());
        fixture
            .journal
            .commit(JournalCommit {
                state_epoch_agents: [child].into(),
                ..Default::default()
            })
            .unwrap();
        assert!(matches!(
            root_context(&child_access),
            Err(StateError::StaleAccess)
        ));
        let refreshed = fixture.journal.state_access(child, None);
        let mut retired = record(child, Some(fixture.root));
        retired.lifecycle = StoredAgentLifecycle::Retired;
        fixture
            .journal
            .commit(JournalCommit {
                agents: vec![retired],
                ..Default::default()
            })
            .unwrap();
        assert!(matches!(
            root_context(&refreshed),
            Err(StateError::StaleAccess)
        ));
    }

    #[test_case(true; "manifest")]
    #[test_case(false; "source")]
    fn corrupted_active_hashes_are_rejected_but_raw_baseline_remains_available(manifest: bool) {
        let fixture = Fixture::new();
        let (_, update) = fixture.prepare(
            &instructions(&[(FIRST, LABEL, FIRST)]),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(update);
        let mut state = fixture.journal.lock();
        let change = state
            .snapshot
            .state
            .iter_mut()
            .rev()
            .find_map(|record| {
                if let StateChange::Context { source, hash, .. } = &mut record.change
                    && (source == CONTEXT_MANIFEST) == manifest
                {
                    return Some(hash);
                }
                None
            })
            .unwrap();
        *change = INVALID_HASH.into();
        drop(state);
        assert!(matches!(
            root_context(&fixture.journal.state_access(fixture.root, None)),
            Err(StateError::InvalidValue(_))
        ));
        if manifest {
            assert_eq!(
                manifest_hash(&fixture.journal.lock().snapshot, fixture.root).as_deref(),
                Some(INVALID_HASH)
            );
        }
    }

    #[test]
    fn unknown_manifest_versions_and_missing_active_sources_are_rejected() {
        let fixture = Fixture::new();
        let access = fixture.journal.state_access(fixture.root, None);
        for manifest in [
            Manifest {
                version: u32::MAX,
                sources: Vec::new(),
                hashes: BTreeMap::new(),
                seen_observations: Vec::new(),
            },
            Manifest {
                version: MANIFEST_VERSION,
                sources: vec![Source::Instruction {
                    path: FIRST.into(),
                    label: LABEL.into(),
                }],
                hashes: BTreeMap::new(),
                seen_observations: Vec::new(),
            },
        ] {
            fixture
                .journal
                .commit(JournalCommit {
                    changes: vec![context_change(
                        fixture.root,
                        CONTEXT_MANIFEST.into(),
                        encode(&manifest).unwrap(),
                    )],
                    ..Default::default()
                })
                .unwrap();
            assert!(matches!(
                root_context(&access),
                Err(StateError::InvalidValue(_))
            ));
        }
    }

    #[test]
    fn prompt_order_is_compared_per_bucket_and_retains_registration_identity() {
        let fixture = Fixture::new();
        let mut initial = slots(&[(0, Some(FIRST)), (1, Some(SECOND))]);
        initial.observe(PromptSource {
            prompt: PromptId::General,
            slot: Slot::Tone,
            plugin: Arc::from(PLUGIN),
            registration: 0,
            content: Some(SECOND.into()),
        });
        let (_, update) = fixture.prepare(
            &Instructions::default(),
            &initial,
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(update);
        let mut with_new_bucket = initial.clone();
        with_new_bucket.observe(PromptSource {
            prompt: PromptId::System,
            slot: Slot::Identity,
            plugin: Arc::from(PLUGIN),
            registration: 0,
            content: Some(THIRD.into()),
        });
        for _ in 0..2 {
            let (_, update) = fixture.prepare(
                &Instructions::default(),
                &with_new_bucket,
                true,
                ContextPolicy::Admission,
            );
            assert!(!update.notice);
            fixture.commit(update);
        }
        let mut reversed = slots(&[(1, Some(SECOND)), (0, Some(FIRST))]);
        for source in with_new_bucket.sources() {
            if source.slot != Slot::Conventions {
                reversed.observe(source);
            }
        }
        let (effective, update) = fixture.prepare(
            &Instructions::default(),
            &reversed,
            true,
            ContextPolicy::Admission,
        );
        assert!(update.notice);
        let conventions: Vec<_> = effective
            .slots
            .sources()
            .into_iter()
            .filter(|source| source.slot == Slot::Conventions)
            .collect();
        assert_eq!(conventions[0].registration, 0);
        assert_eq!(conventions[1].registration, 1);
        assert_eq!(conventions[1].content.as_deref(), Some(SECOND));
        fixture.commit(update);
        let (_, repeated) = fixture.prepare(
            &Instructions::default(),
            &reversed,
            true,
            ContextPolicy::Admission,
        );
        assert!(!repeated.notice);
    }

    #[test]
    fn aborted_preparation_does_not_consume_notice_and_duplicates_are_rejected() {
        let fixture = Fixture::new();
        let (_, initial) = fixture.prepare(
            &Instructions::default(),
            &slots(&[(0, Some(FIRST))]),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(initial);
        let binding =
            ContextBinding::capture(fixture.journal.state_access(fixture.root, None)).unwrap();
        let (_, first) = binding
            .prepare(
                &Instructions::default(),
                &slots(&[(0, Some(CHANGED))]),
                true,
                ContextPolicy::Admission,
            )
            .unwrap();
        let (_, repeated) = binding
            .prepare(
                &Instructions::default(),
                &slots(&[(0, Some(CHANGED))]),
                true,
                ContextPolicy::Admission,
            )
            .unwrap();
        assert!(first.notice);
        assert!(repeated.notice);
        assert_eq!(first.expected, repeated.expected);
        assert!(matches!(
            binding.prepare(
                &Instructions::default(),
                &slots(&[(0, Some(FIRST)), (0, None)]),
                true,
                ContextPolicy::Admission,
            ),
            Err(StateError::InvalidValue(_))
        ));
    }

    #[test]
    fn unreferenced_context_records_are_ignored_even_with_invalid_hashes() {
        let fixture = Fixture::new();
        let (_, initial) = fixture.prepare(
            &Instructions::default(),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        fixture.commit(initial);
        let mut orphan = context_change(fixture.root, FIRST.into(), CHANGED.into());
        if let StateChange::Context { hash, .. } = &mut orphan.change {
            *hash = INVALID_HASH.into();
        }
        fixture
            .journal
            .commit(JournalCommit {
                changes: vec![orphan],
                ..Default::default()
            })
            .unwrap();
        assert!(fixture.read().instructions.sources.is_empty());
        assert!(fixture.read().slots.sources().is_empty());
    }

    #[test]
    fn notice_is_an_unscoped_root_post_and_silent_updates_emit_nothing() {
        let fixture = Fixture::new();
        let (_, initial) = fixture.prepare(
            &instructions(&[(FIRST, LABEL, FIRST)]),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        let (tx, rx) = flume::unbounded();
        let sender = EventSender::new(tx);
        initial.notify(&sender);
        assert!(rx.try_recv().is_err());
        fixture.commit(initial);
        let (_, changed) = fixture.prepare(
            &instructions(&[(FIRST, LABEL, CHANGED)]),
            &ResolvedSlots::default(),
            true,
            ContextPolicy::Admission,
        );
        changed.notify(&sender);
        let envelope = rx.try_recv().unwrap();
        assert!(envelope.subagent.is_none());
        let AgentEvent::Post(post) = envelope.event else {
            panic!("expected context notice post");
        };
        assert_eq!(post.text, CONTEXT_CHANGED_NOTICE);
        assert_eq!(post.level, PostLevel::Warn);
        assert!(post.agent.is_none());
        assert!(rx.try_recv().is_err());
    }
}
