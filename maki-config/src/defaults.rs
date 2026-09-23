use maki_storage::sessions::{SessionMeta, StoredThinking};

/// The `always_*` knobs that seed a session's own toggles.
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

impl SessionDefaults {
    /// Seeds a session that has not spoken yet. An unset knob means "no
    /// opinion" and leaves the session's own toggle alone, so a `/fast` from an
    /// earlier run survives a config that never mentions it.
    ///
    /// The model is nobody's business here. `RequestOptions::clamped` is the
    /// single gate for what the model can actually do, and the model can still
    /// change after this runs.
    pub fn seed(self, meta: &mut SessionMeta) {
        meta.fast |= self.fast;
        meta.workflow |= self.workflow;
        if self.thinking.is_some() {
            meta.thinking = self.thinking;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_storage::sessions::Effort;
    use test_case::test_case;

    type Toggles = (bool, bool, Option<StoredThinking>);

    const HIGH: Option<StoredThinking> = Some(StoredThinking::Effort {
        level: Effort::High,
    });

    #[test_case((true, true, Some(StoredThinking::Adaptive)), (false, false, None)
        => (true, true, Some(StoredThinking::Adaptive)) ; "blank_session_takes_every_knob")]
    #[test_case((false, false, None), (true, true, HIGH)
        => (true, true, HIGH) ; "silence_leaves_the_session_alone")]
    #[test_case((false, false, Some(StoredThinking::Off)), (false, false, Some(StoredThinking::Adaptive))
        => (false, false, Some(StoredThinking::Off)) ; "explicit_off_still_wins")]
    fn seed_applies_only_the_knobs_config_sets(defaults: Toggles, session: Toggles) -> Toggles {
        let mut meta = SessionMeta {
            fast: session.0,
            workflow: session.1,
            thinking: session.2,
            ..Default::default()
        };
        SessionDefaults {
            fast: defaults.0,
            workflow: defaults.1,
            thinking: defaults.2,
        }
        .seed(&mut meta);

        (meta.fast, meta.workflow, meta.thinking)
    }

    #[test]
    fn test_session_defaults_unification() {
        let defaults = SessionDefaults {
            fast: true,
            workflow: true,
            thinking: Some(StoredThinking::Adaptive),
        };
        let mut meta = SessionMeta::default();
        defaults.seed(&mut meta);
        assert!(meta.fast);
        assert!(meta.workflow);
        assert_eq!(meta.thinking, Some(StoredThinking::Adaptive));
    }
}
