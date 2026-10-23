use maki_storage::sessions::StoredThinking;

/// The `always_*` knobs that seed a session's own toggles, combined with the
/// session and the saved preference by `maki_agent::session_defaults::resolve`.
///
/// One value, so every entry point that starts a session (TUI, `-p`, the SDK,
/// ACP) takes the whole set at once. A knob that lives here cannot be honoured
/// by one frontend and dropped by another, which is how `always_thinking` once
/// reached the TUI and nobody else.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionDefaults {
    pub fast: bool,
    pub workflow: bool,
    pub thinking: Option<StoredThinking>,
}
