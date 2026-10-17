//! Errors the Lua agent API reports, shared with tests that assert them.

pub const INVOCATION_EXPIRED: &str = "context invocation expired";
pub const NO_PLUGIN_AUTHORITY: &str = "no active plugin authority";
pub const NO_TASK_AUTHORITY: &str = "no active task authority";
pub const MANAGED_AUTHORITY_INACTIVE: &str =
    "managed agent authority is not active in this invocation";
pub const UNMANAGED_AUTHORITY_INACTIVE: &str =
    "unmanaged agent authority is not active in this invocation";
pub const OTHER_PLUGIN_GENERATION: &str = "agent belongs to another plugin generation";
pub const AGENT_CONFIG_UNAVAILABLE: &str = "agent configuration is unavailable";
pub const OTHER_RUNTIME: &str = "agent belongs to another runtime";
pub const NO_ORCHESTRATION_SERVICES: &str = "no host orchestration services";
pub const ORCHESTRATION_SERVICES_EXPIRED: &str = "host orchestration services expired";
pub const TRUSTED_SUBSCRIPTION_REQUIRED: &str = "agent subscriptions require trusted authority";
pub const TRUSTED_RUN_COMMAND_REQUIRED: &str = "run_command requires trusted authority";
pub const TRUSTED_TIMER_REQUIRED: &str = "timers and deferred callbacks require trusted authority";
pub const DELEGATED_REGISTRATION: &str = "delegated tool handlers cannot register host callbacks";
pub const DELEGATED_HOST_MUTATION: &str = "delegated tool handlers cannot change the focused session or global modes; name the tool's own session or address an agent explicitly";
pub const DELEGATED_FOREIGN_SESSION: &str =
    "delegated tool handlers can only change the session their turn belongs to";
pub const DELEGATED_SESSION_LIFECYCLE: &str =
    "delegated tool handlers cannot create, focus or delete sessions";
pub const DELEGATED_FOCUS: &str = "delegated tool handlers cannot read UI focus";
pub const SELF_TURN_WAIT: &str = "cannot wait for a turn on the calling agent";
pub const NO_FOCUSED_AGENT: &str = "no agent has UI focus";
pub const TURN_WAIT_TIMEOUT: &str = "agent turn wait timed out";
pub const EXPLICIT_MODEL_TARGET_REQUIRED: &str =
    "no interactive UI attached; pass ctx or use the Agent API";
pub const TRUSTED_MODEL_MUTATION_REQUIRED: &str =
    "host model mutation requires trusted handler authority";
pub const UNKNOWN_SPAWN_OPTION: &str = "unknown spawn option";
pub const AGENT_BUSY: &str = "agent has pending work";
pub const STALE_TURN: &str = "after_turn is not the agent's latest settled turn";
pub const RESTRICTED_MODE_CHANGE: &str = "restricted agents cannot change mode";
pub const NOT_DESCENDANT_OF_CALLER: &str = "agent is not a descendant of the caller";
pub const FOREIGN_AGENT_REFERENCE: &str = "agent reference belongs to another runtime";
pub const ORCHESTRATION_CONTEXT_EXPIRED: &str = "orchestration context expired";
pub const OUTPUT_SCHEMA_NOT_OBJECT: &str = "output_schema must have type object";
pub const STRUCTURED_OUTPUT_CONFLICT: &str =
    "output_schema conflicts with existing structured_output tool";
pub const SHOW_SILENT_AGENT: &str = "silent agents cannot be shown";
pub const SHOW_TOOL_AGENT: &str = "agents spawned by a tool call are shown by that call";
pub const SHOW_UNTRUSTED_PARENT: &str = "showing an agent requires a trusted parent";
pub const BLANK_POST: &str = "post text must not be blank";
pub const UNKNOWN_POST_OPTION: &str = "unknown post option";
pub const UNKNOWN_POST_LEVEL: &str = "unknown post level";
pub const UNLINKABLE_POST_AGENT: &str = "only agents spawned from Lua can be linked from a post";
pub const POST_TARGET_SILENT: &str = "silent agents have no chat to post to";
pub const POST_TARGET_NOT_STARTED: &str = "agent has no chat before its first turn";
