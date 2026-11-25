mod admission;
mod compaction;
mod history;
mod instructions;
pub mod isolated_turn;
mod run;
mod streaming;
pub mod tool_dispatch;

pub(crate) use admission::resolve_prompt_inputs;
pub use admission::{
    PROMPT_INPUTS_CLOSED, PROMPT_SLOTS_CLOSED, PromptContext, PromptSlotSource, TurnSetup,
};
pub use compaction::compact;
pub use history::{
    History, HistorySnapshot, RESTART_TOOL_INTERRUPTION, SharedMessages, UNAVAILABLE_RESULT,
    USER_TOOL_INTERRUPTION, close_dangling_tool_calls,
};
pub use instructions::{
    InstructionSource, Instructions, LoadedInstructions, build_mode_system_prompt,
    build_system_prompt, build_system_prompt_with_def, find_subdirectory_instructions,
    is_instruction_file, load_instruction_text, load_instructions,
};
pub use run::{
    Agent, AgentParams, AgentRunParams, NO_ADMITTED_CWD_ERR, ResolvedPromptInputs, RunSettings,
    TurnAdmissionSnapshot, TurnPromptInputs, estimate_message_tokens, resolve_compaction_model,
};
pub use streaming::end_conversation_detached;
