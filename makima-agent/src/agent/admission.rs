//! One turn setup for every backend: what a turn admits (mode definition, cwd,
//! instructions, tool bindings) and what it resolves before its first request
//! (prompt slots, MCP prompt messages).

use std::sync::Arc;

use makima_providers::{ContentBlock, Message, Role};
use tracing::warn;

use super::instructions::{Instructions, load_instructions};
use super::run::{ResolvedPromptInputs, TurnAdmissionSnapshot, TurnPromptInputs};
use crate::actor::{ActorError, AdmissionPreparation, EffectiveAgentConfig, PreparedReadiness};
use crate::context::{ContextBinding, ContextPolicy, ContextUpdate, RootContext};
use crate::mcp::McpSession;
use crate::mcp::protocol::{PromptMessage, PromptRole};
use crate::prompt::ResolvedSlots;
use crate::state::{StateAccess, StateError};
use crate::tools::{LocalTools, ToolRegistry, TurnToolBindings};
use crate::{AgentError, AgentInput, AgentMode, ModeRegistry};

pub const PROMPT_SLOTS_CLOSED: &str =
    "the Lua plugin runtime stopped before returning prompt slots";
pub const PROMPT_INPUTS_CLOSED: &str = "prompt input preparation stopped before it finished";
const MCP_PROMPT_TOOL: &str = "mcp_prompt";
const PROMPT_INPUTS_TOOL: &str = "prompt_inputs";

type SlotRequest = dyn Fn() -> flume::Receiver<ResolvedSlots> + Send + Sync;

/// Asks the plugin host for a turn's prompt slots. The default source has no
/// host and answers with empty slots.
#[derive(Clone, Default)]
pub struct PromptSlotSource(Option<Arc<SlotRequest>>);

impl PromptSlotSource {
    pub fn new(
        request: impl Fn() -> flume::Receiver<ResolvedSlots> + Send + Sync + 'static,
    ) -> Self {
        Self(Some(Arc::new(request)))
    }

    fn request(&self) -> Option<flume::Receiver<ResolvedSlots>> {
        self.0.as_ref().map(|request| request())
    }

    /// The slots as they are now, empty with a warning when the host is gone.
    pub async fn current(&self) -> Arc<ResolvedSlots> {
        let (slots, error) = Self::collect(self.request()).await;
        if let Some(error) = error {
            warn!(%error, "using empty prompt slots after collection failed");
        }
        slots
    }

    async fn collect(
        request: Option<flume::Receiver<ResolvedSlots>>,
    ) -> (Arc<ResolvedSlots>, Option<String>) {
        let Some(request) = request else {
            return (Arc::default(), None);
        };
        match request.recv_async().await {
            Ok(slots) => (Arc::new(slots), None),
            Err(_) => (Arc::default(), Some(PROMPT_SLOTS_CLOSED.into())),
        }
    }
}

#[derive(Clone, Default)]
pub struct PromptContext {
    binding: Option<ContextBinding>,
    effective: Option<Arc<RootContext>>,
    update: Option<ContextUpdate>,
}

#[derive(Clone)]
pub struct TurnSetup {
    pub registry: Arc<ToolRegistry>,
    pub local_tools: LocalTools,
    pub mcp: Option<McpSession>,
    pub slots: PromptSlotSource,
    pub modes: Arc<ModeRegistry>,
    /// Lua agents build their own prompt and never inject instruction files.
    pub instructions: bool,
}

impl TurnSetup {
    /// A turn with no committed config admits no working directory, so its
    /// tools fail instead of running against the process cwd.
    pub fn snapshot(
        &self,
        input: &AgentInput,
        config: Option<&EffectiveAgentConfig>,
    ) -> TurnAdmissionSnapshot {
        let mode = config.map_or(&AgentMode::Build, |config| &config.mode);
        let mode_def = config
            .and_then(|config| config.mode_def.clone())
            .unwrap_or_else(|| self.modes.current(mode));
        TurnAdmissionSnapshot {
            mode_def: Some(Arc::new(mode_def)),
            prompt_inputs: config.map(|config| Arc::new(self.prompt_inputs(input, config))),
            bindings: Arc::new(TurnToolBindings::capture(
                &self.registry,
                &self.local_tools,
                self.mcp.as_ref(),
            )),
            mcp_startup_notice: self.mcp.as_ref().and_then(|mcp| {
                let count = mcp.connecting_count();
                (count > 0).then_some(count)
            }),
        }
    }

    fn prompt_inputs(&self, input: &AgentInput, config: &EffectiveAgentConfig) -> TurnPromptInputs {
        let cwd = config.context.cwd().to_path_buf();
        let instructions = if self.instructions {
            load_instructions(&cwd.to_string_lossy())
        } else {
            Instructions::default()
        };
        let binding = input
            .prompt
            .as_ref()
            .and_then(|prompt| self.mcp.as_ref()?.prompt_binding(&prompt.qualified_name));
        let (tx, rx) = flume::bounded(1);
        let slot_request = self.slots.request();
        let prompt = input.prompt.clone();
        let pinned = binding.clone();
        let mcp = self.mcp.clone();
        smol::spawn(async move {
            let messages = async {
                match (prompt, pinned, mcp) {
                    (Some(prompt), Some(binding), Some(mcp)) => mcp
                        .get_bound_prompt(&binding, &prompt.arguments)
                        .await
                        .map(|messages| Some(messages.into_iter().map(prompt_message).collect()))
                        .map_err(|error| error.to_string()),
                    (Some(prompt), _, _) => Err(unknown_prompt(&prompt.qualified_name)),
                    (None, _, _) => Ok(None),
                }
            };
            let (messages, (slots, slots_error)) =
                futures_lite::future::zip(messages, PromptSlotSource::collect(slot_request)).await;
            let _ = tx.send(messages.map(|mcp_messages| ResolvedPromptInputs {
                slots,
                slots_error,
                mcp_messages,
            }));
        })
        .detach();
        TurnPromptInputs {
            cwd,
            instructions,
            mcp_prompt: binding,
            resolved: Some(rx),
            ready: None,
            context: self.instructions.then(PromptContext::default),
        }
    }

    pub fn preparation(self) -> AdmissionPreparation {
        Arc::new(move |input, config| self.snapshot(input, config))
    }

    /// Prepared (approval) admissions fail fast on prompt-slot errors, while
    /// ordinary turns fall back to empty slots with a warning: an
    /// implementation handoff must not silently start with wrong prompt slots.
    pub fn readiness() -> PreparedReadiness {
        Arc::new(|snapshot| Box::pin(prepare_prompt_inputs(snapshot)))
    }
}

async fn prepare_prompt_inputs(
    snapshot: TurnAdmissionSnapshot,
) -> Result<TurnAdmissionSnapshot, ActorError> {
    resolve_prompt_inputs(snapshot, true, ContextPolicy::Admission).await
}

pub(crate) async fn resolve_prompt_inputs(
    mut snapshot: TurnAdmissionSnapshot,
    strict: bool,
    policy: ContextPolicy,
) -> Result<TurnAdmissionSnapshot, ActorError> {
    if let Some(prompt) = snapshot.prompt_inputs.as_mut() {
        let prompt = Arc::make_mut(prompt);
        if prompt.ready.is_none()
            && let Some(receiver) = prompt.resolved.take()
        {
            prompt.ready = Some(
                receiver
                    .recv_async()
                    .await
                    .map_err(|_| ActorError::InvalidConfig(PROMPT_INPUTS_CLOSED.into()))?
                    .map_err(ActorError::InvalidConfig)?,
            );
        }
        if strict
            && let Some(error) = prompt
                .ready
                .as_ref()
                .and_then(|ready| ready.slots_error.as_ref())
        {
            return Err(ActorError::InvalidConfig(error.clone()));
        }
        if let Some(context) = &mut prompt.context
            && context.update.is_none()
            && let Some(binding) = &context.binding
            && let Some(ready) = &mut prompt.ready
        {
            let (effective, update) = binding
                .prepare(
                    &prompt.instructions,
                    &ready.slots,
                    ready.slots_error.is_none(),
                    policy,
                )
                .map_err(|error| ActorError::InvalidConfig(error.to_string()))?;
            prompt.instructions = effective.instructions.clone();
            ready.slots = Arc::clone(&effective.slots);
            context.effective = Some(effective);
            context.update = Some(update);
        }
    }
    Ok(snapshot)
}

impl TurnPromptInputs {
    pub fn bind_context(&mut self, access: StateAccess) -> Result<(), StateError> {
        if let Some(context) = &mut self.context {
            context.binding = Some(ContextBinding::capture(access)?);
        }
        Ok(())
    }

    pub fn root_context(&self) -> Option<Arc<RootContext>> {
        self.context.as_ref()?.effective.clone()
    }

    pub fn context_update(&self) -> Option<ContextUpdate> {
        self.context.as_ref()?.update.clone()
    }

    /// Waits for the slots and MCP prompt admitted with this turn and expands
    /// the prompt into the input's preamble.
    pub async fn resolve(
        &self,
        input: &mut AgentInput,
        mcp: Option<&McpSession>,
    ) -> Result<Arc<ResolvedSlots>, AgentError> {
        if let Some(prompt) = &input.prompt {
            let current = self
                .mcp_prompt
                .as_ref()
                .zip(mcp)
                .is_some_and(|(binding, mcp)| mcp.prompt_is_current(binding));
            if !current {
                return Err(mcp_prompt_error(if self.mcp_prompt.is_some() {
                    format!(
                        "MCP prompt is no longer available: {}",
                        prompt.qualified_name
                    )
                } else {
                    unknown_prompt(&prompt.qualified_name)
                }));
            }
        }
        let resolved = match (&self.ready, &self.resolved) {
            (Some(ready), _) => ready.clone(),
            (None, Some(receiver)) => receiver
                .recv_async()
                .await
                .map_err(|error| prompt_inputs_error(error.to_string()))?
                .map_err(prompt_inputs_error)?,
            (None, None) => ResolvedPromptInputs {
                slots: Arc::default(),
                slots_error: None,
                mcp_messages: None,
            },
        };
        input
            .preamble
            .extend(resolved.mcp_messages.unwrap_or_default());
        if let Some(error) = &resolved.slots_error {
            warn!(%error, "using empty prompt slots after collection failed");
        }
        Ok(resolved.slots)
    }
}

fn unknown_prompt(name: &str) -> String {
    format!("unknown MCP prompt: {name}")
}

fn mcp_prompt_error(message: String) -> AgentError {
    AgentError::Tool {
        tool: MCP_PROMPT_TOOL.into(),
        message,
    }
}

fn prompt_inputs_error(message: String) -> AgentError {
    AgentError::Tool {
        tool: PROMPT_INPUTS_TOOL.into(),
        message,
    }
}

fn prompt_message(message: PromptMessage) -> Message {
    let text = message.content.text.unwrap_or_default();
    match message.role {
        PromptRole::Assistant => Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text }],
            ..Default::default()
        },
        PromptRole::User => Message::user(text),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_lite::future::poll_once;
    use makima_providers::Model;
    use makima_storage::session_state::{StateChange, StoredAgentLifecycle, StoredAgentRecord};
    use test_case::test_case;

    use super::{
        PROMPT_SLOTS_CLOSED, PromptSlotSource, TurnSetup, mcp_prompt_error, resolve_prompt_inputs,
        unknown_prompt,
    };
    use crate::actor::{ActorError, AgentContextSnapshot, EffectiveAgentConfig};
    use crate::agent::{InstructionSource, Instructions, TurnAdmissionSnapshot};
    use crate::context::{CONTEXT_CHANGED_NOTICE, ContextPolicy, ContextUpdate, RootContext};
    use crate::journal::{JournalCommit, SessionJournal};
    use crate::mcp::test_support::stub_prompt_session;
    use crate::prompt::{PromptId, PromptSource, ResolvedSlots, Slot, SlotEntry};
    use crate::state::StateAccess;
    use crate::tools::{LocalTools, NullProvider, ToolRegistry};
    use crate::{
        AgentEvent, AgentId, AgentInput, AgentMode, EventSender, HistorySnapshot, McpPromptRef,
        ModeRegistry, RunSettings,
    };

    const PROMPT: &str = "srv.review";
    const MISSING_PROMPT: &str = "srv.missing";
    const PROMPT_TEXT: &str = "review the diff";
    const CWD: &str = "/tmp/admitted";
    const MODEL: &str = "anthropic/claude-sonnet-4-20250514";
    const INSTRUCTION_PATH: &str = "/admission-context/project/AGENTS.md";
    const INSTRUCTION_LABEL: &str = "project instructions";
    const RECORDED_INSTRUCTIONS: &str = "recorded instructions";
    const CHANGED_INSTRUCTIONS: &str = "changed instructions";
    const RECORDED_SLOT: &str = "recorded identity";
    const CHANGED_SLOT: &str = "changed identity";
    const CONTEXT_PLUGIN: &str = "admission-context";

    fn setup(slots: PromptSlotSource) -> TurnSetup {
        TurnSetup {
            registry: Arc::new(ToolRegistry::default()),
            local_tools: LocalTools::default(),
            mcp: Some(stub_prompt_session(PROMPT, PROMPT_TEXT)),
            slots,
            modes: Arc::new(ModeRegistry::builtin()),
            instructions: false,
        }
    }

    fn config() -> EffectiveAgentConfig {
        EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::new(NullProvider),
                model: Model::from_spec(MODEL).unwrap(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
        .with_context(AgentContextSnapshot::new(PathBuf::from(CWD)))
    }

    fn prompted(name: &str) -> AgentInput {
        let mut input = AgentInput::new(format!("/{name}"), Vec::new());
        input.prompt = Some(Box::new(McpPromptRef {
            qualified_name: name.into(),
            arguments: HashMap::new(),
        }));
        input
    }

    #[test]
    fn mcp_prompt_expands_into_the_preamble() {
        let mut input = prompted(PROMPT);
        let setup = setup(PromptSlotSource::default());
        let inputs = setup
            .snapshot(&input, Some(&config()))
            .prompt_inputs
            .unwrap();
        assert_eq!(inputs.cwd, Path::new(CWD));
        smol::block_on(inputs.resolve(&mut input, setup.mcp.as_ref())).unwrap();
        assert_eq!(input.preamble.len(), 1);
        assert_eq!(input.preamble[0].user_text(), Some(PROMPT_TEXT));
    }

    #[test]
    fn unknown_mcp_prompt_fails_the_turn() {
        let mut input = prompted(MISSING_PROMPT);
        let setup = setup(PromptSlotSource::default());
        let inputs = setup
            .snapshot(&input, Some(&config()))
            .prompt_inputs
            .unwrap();
        let Err(error) = smol::block_on(inputs.resolve(&mut input, setup.mcp.as_ref())) else {
            panic!("an unknown MCP prompt must fail the turn");
        };
        assert_eq!(
            error.to_string(),
            mcp_prompt_error(unknown_prompt(MISSING_PROMPT)).to_string()
        );
        assert!(input.preamble.is_empty());
    }

    #[test]
    fn a_turn_without_config_admits_no_cwd() {
        let snapshot = setup(PromptSlotSource::default())
            .snapshot(&AgentInput::new(String::new(), Vec::new()), None);
        assert!(snapshot.prompt_inputs.is_none());
        assert_eq!(snapshot.mode_def.unwrap().id, crate::ModeId::Build);
    }

    #[test_case(true ; "prepared_admission_fails")]
    #[test_case(false ; "ordinary_turn_warns")]
    fn closed_slot_source(prepared: bool) {
        let setup = setup(PromptSlotSource::new(|| flume::bounded(1).1));
        let mut input = AgentInput::new(String::new(), Vec::new());
        let snapshot = setup.snapshot(&input, Some(&config()));
        if prepared {
            let Err(ActorError::InvalidConfig(message)) =
                smol::block_on(TurnSetup::readiness()(snapshot))
            else {
                panic!("a closed slot channel must fail a prepared admission");
            };
            assert_eq!(message, PROMPT_SLOTS_CLOSED);
        } else {
            let inputs = snapshot.prompt_inputs.unwrap();
            let slots = smol::block_on(inputs.resolve(&mut input, setup.mcp.as_ref())).unwrap();
            assert!(slots.get(PromptId::System, Slot::Identity).is_empty());
        }
    }

    fn root_journal() -> (Arc<SessionJournal>, StateAccess) {
        let journal = Arc::new(SessionJournal::default());
        let root = AgentId::generate();
        journal
            .commit(JournalCommit {
                agents: vec![StoredAgentRecord {
                    id: root,
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
                }],
                histories: [(root, HistorySnapshot::default())].into(),
                ..Default::default()
            })
            .unwrap();
        let access = journal.state_access(root, None);
        (journal, access)
    }

    fn context_snapshot(
        access: &StateAccess,
        slots: PromptSlotSource,
        content: &str,
    ) -> TurnAdmissionSnapshot {
        let mut setup = setup(slots);
        setup.instructions = true;
        let mut snapshot =
            setup.snapshot(&AgentInput::new(String::new(), Vec::new()), Some(&config()));
        let prompt = Arc::make_mut(snapshot.prompt_inputs.as_mut().unwrap());
        prompt.instructions = Instructions::from_sources(vec![InstructionSource {
            path: INSTRUCTION_PATH.into(),
            label: INSTRUCTION_LABEL.into(),
            content: content.into(),
        }]);
        prompt.bind_context(access.clone()).unwrap();
        snapshot
    }

    fn context_slots(content: Option<&str>) -> ResolvedSlots {
        let mut slots = ResolvedSlots::default();
        if let Some(content) = content {
            slots.insert(
                PromptId::System,
                Slot::Identity,
                SlotEntry {
                    plugin: CONTEXT_PLUGIN.into(),
                    registration: 0,
                    content: content.into(),
                },
            );
        } else {
            slots.observe(PromptSource {
                prompt: PromptId::System,
                slot: Slot::Identity,
                plugin: CONTEXT_PLUGIN.into(),
                registration: 0,
                content: None,
            });
        }
        slots
    }

    fn slot_source(content: Option<&'static str>) -> PromptSlotSource {
        PromptSlotSource::new(move || {
            let (sender, receiver) = flume::bounded(1);
            sender.send(context_slots(content)).unwrap();
            receiver
        })
    }

    fn effective_context(snapshot: &TurnAdmissionSnapshot) -> (Arc<RootContext>, ContextUpdate) {
        let prompt = snapshot.prompt_inputs.as_ref().unwrap();
        (
            prompt.root_context().unwrap(),
            prompt.context_update().unwrap(),
        )
    }

    fn assert_recorded_context(context: &RootContext) {
        assert_eq!(context.instructions.sources.len(), 1);
        let source = &context.instructions.sources[0];
        assert_eq!(source.path, Path::new(INSTRUCTION_PATH));
        assert_eq!(source.label, INSTRUCTION_LABEL);
        assert_eq!(source.content, RECORDED_INSTRUCTIONS);
        let slots = context.slots.get(PromptId::System, Slot::Identity);
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].plugin.as_ref(), CONTEXT_PLUGIN);
        assert_eq!(slots[0].registration, 0);
        assert_eq!(slots[0].content, RECORDED_SLOT);
    }

    #[test_case(Some(CHANGED_SLOT); "changed_slot")]
    #[test_case(None; "empty_slot_observation")]
    fn context_preparation_is_pinned_and_notifies_only_after_commit(
        observed_slot: Option<&'static str>,
    ) {
        smol::block_on(async {
            let (journal, access) = root_journal();
            let before = journal.snapshot();
            let requests = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&requests);
            let snapshot = context_snapshot(
                &access,
                PromptSlotSource::new(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    let (sender, receiver) = flume::bounded(1);
                    sender.send(context_slots(Some(RECORDED_SLOT))).unwrap();
                    receiver
                }),
                RECORDED_INSTRUCTIONS,
            );
            let prepared = resolve_prompt_inputs(snapshot, true, ContextPolicy::Admission)
                .await
                .unwrap();
            let (effective, update) = effective_context(&prepared);
            assert_recorded_context(&effective);
            assert_eq!(journal.snapshot().seq, before.seq);
            assert!(
                journal
                    .snapshot()
                    .state
                    .iter()
                    .all(|record| { !matches!(&record.change, StateChange::Context { .. }) })
            );
            let repeated = resolve_prompt_inputs(prepared, true, ContextPolicy::Admission)
                .await
                .unwrap();
            assert!(Arc::ptr_eq(&effective, &effective_context(&repeated).0));
            assert_eq!(requests.load(Ordering::SeqCst), 1);
            let (sender, events) = flume::unbounded();
            let sender = EventSender::new(sender);
            journal
                .commit(JournalCommit {
                    context_updates: vec![update.clone()],
                    ..Default::default()
                })
                .unwrap();
            update.notify(&sender);
            assert!(events.try_recv().is_err());
            assert!(journal.snapshot().state.iter().any(|record| {
                record.agent == Some(access.target())
                    && matches!(&record.change, StateChange::Context { .. })
            }));
            let committed = journal.snapshot().seq;
            let changed = resolve_prompt_inputs(
                context_snapshot(&access, slot_source(observed_slot), CHANGED_INSTRUCTIONS),
                true,
                ContextPolicy::Admission,
            )
            .await
            .unwrap();
            let (effective, update) = effective_context(&changed);
            assert_recorded_context(&effective);
            assert_eq!(journal.snapshot().seq, committed);
            assert!(events.try_recv().is_err());
            journal
                .commit(JournalCommit {
                    context_updates: vec![update.clone()],
                    ..Default::default()
                })
                .unwrap();
            assert!(events.try_recv().is_err());
            update.notify(&sender);
            assert!(matches!(
                events.try_recv().unwrap().event,
                AgentEvent::Post(post) if post.text == CONTEXT_CHANGED_NOTICE
            ));
            assert!(events.try_recv().is_err());
            let repeated = resolve_prompt_inputs(
                context_snapshot(&access, slot_source(observed_slot), CHANGED_INSTRUCTIONS),
                true,
                ContextPolicy::Admission,
            )
            .await
            .unwrap();
            let (effective, update) = effective_context(&repeated);
            assert_recorded_context(&effective);
            assert!(update.changes.is_empty());
            update.notify(&sender);
            assert!(events.try_recv().is_err());
        });
    }

    #[test_case(true; "strict_rejects_failed_slots")]
    #[test_case(false; "tolerant_preserves_recorded_slots")]
    fn failed_slots_do_not_replace_recorded_context(strict: bool) {
        smol::block_on(async {
            let (journal, access) = root_journal();
            let recorded = resolve_prompt_inputs(
                context_snapshot(
                    &access,
                    slot_source(Some(RECORDED_SLOT)),
                    RECORDED_INSTRUCTIONS,
                ),
                true,
                ContextPolicy::Admission,
            )
            .await
            .unwrap();
            journal
                .commit(JournalCommit {
                    context_updates: vec![effective_context(&recorded).1],
                    ..Default::default()
                })
                .unwrap();
            let before = journal.snapshot().seq;
            let result = resolve_prompt_inputs(
                context_snapshot(
                    &access,
                    PromptSlotSource::new(|| flume::bounded(1).1),
                    RECORDED_INSTRUCTIONS,
                ),
                strict,
                ContextPolicy::Admission,
            )
            .await;
            if strict {
                assert!(matches!(
                    result,
                    Err(ActorError::InvalidConfig(message)) if message == PROMPT_SLOTS_CLOSED
                ));
            } else {
                let snapshot = result.unwrap();
                let (effective, update) = effective_context(&snapshot);
                assert_recorded_context(&effective);
                assert!(update.changes.is_empty());
                let mut input = AgentInput::new(String::new(), Vec::new());
                let slots = snapshot
                    .prompt_inputs
                    .as_ref()
                    .unwrap()
                    .resolve(&mut input, None)
                    .await
                    .unwrap();
                assert!(Arc::ptr_eq(&slots, &effective.slots));
            }
            assert_eq!(journal.snapshot().seq, before);
        });
    }

    #[test]
    fn dropping_unresolved_or_prepared_context_leaves_the_journal_unchanged() {
        smol::block_on(async {
            let (journal, access) = root_journal();
            let before = journal.snapshot();
            let (sender, receiver) = flume::bounded(1);
            let snapshot = context_snapshot(
                &access,
                PromptSlotSource::new(move || receiver.clone()),
                RECORDED_INSTRUCTIONS,
            );
            let mut resolving = Box::pin(resolve_prompt_inputs(
                snapshot,
                true,
                ContextPolicy::Admission,
            ));
            assert!(poll_once(resolving.as_mut()).await.is_none());
            drop(resolving);
            drop(sender);
            let prepared = resolve_prompt_inputs(
                context_snapshot(
                    &access,
                    slot_source(Some(RECORDED_SLOT)),
                    RECORDED_INSTRUCTIONS,
                ),
                true,
                ContextPolicy::Admission,
            )
            .await
            .unwrap();
            let (effective, update) = effective_context(&prepared);
            assert_recorded_context(&effective);
            drop(update);
            drop(prepared);
            let after = journal.snapshot();
            assert_eq!(after.seq, before.seq);
            assert_eq!(after.revision, before.revision);
            assert!(
                after
                    .state
                    .iter()
                    .all(|record| { !matches!(&record.change, StateChange::Context { .. }) })
            );
        });
    }
}
