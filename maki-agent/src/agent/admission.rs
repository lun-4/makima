//! One turn setup for every backend: what a turn admits (mode definition, cwd,
//! instructions, tool bindings) and what it resolves before its first request
//! (prompt slots, MCP prompt messages).

use std::sync::Arc;

use maki_providers::{ContentBlock, Message, Role};
use tracing::warn;

use super::instructions::{Instructions, load_instructions};
use super::run::{ResolvedPromptInputs, TurnAdmissionSnapshot, TurnPromptInputs};
use crate::actor::{ActorError, AdmissionPreparation, EffectiveAgentConfig, PreparedReadiness};
use crate::mcp::McpSession;
use crate::mcp::protocol::{PromptMessage, PromptRole};
use crate::prompt::ResolvedSlots;
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
    mut snapshot: TurnAdmissionSnapshot,
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
        if let Some(error) = prompt
            .ready
            .as_ref()
            .and_then(|ready| ready.slots_error.as_ref())
        {
            return Err(ActorError::InvalidConfig(error.clone()));
        }
    }
    Ok(snapshot)
}

impl TurnPromptInputs {
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

    use maki_providers::Model;
    use test_case::test_case;

    use super::*;
    use crate::actor::AgentContextSnapshot;
    use crate::mcp::test_support::stub_prompt_session;
    use crate::prompt::{PromptId, Slot};
    use crate::tools::NullProvider;
    use crate::{McpPromptRef, RunSettings};

    const PROMPT: &str = "srv.review";
    const MISSING_PROMPT: &str = "srv.missing";
    const PROMPT_TEXT: &str = "review the diff";
    const CWD: &str = "/tmp/admitted";
    const MODEL: &str = "anthropic/claude-sonnet-4-20250514";

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
}
