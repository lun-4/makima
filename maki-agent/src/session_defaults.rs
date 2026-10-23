use maki_config::SessionDefaults;
use maki_providers::Model;
use maki_storage::sessions::{Prefs, SessionMeta};

/// The toggles a session starts on, for every frontend. A session that has
/// spoken keeps its own; a {blank} one takes config on top of what it saved,
/// with the saved `/thinking` preference ahead of `always_thinking`. A stored
/// thinking (off included) wins on a spoken session; one that never recorded
/// a level falls back to the preference, then config. What {model} cannot
/// do starts off. The returned thinking is `None` for off.
pub fn resolve(
    config: SessionDefaults,
    prefs: &Prefs,
    meta: &SessionMeta,
    blank: bool,
    model: &Model,
) -> SessionDefaults {
    let configured = prefs.default_thinking.or(config.thinking);
    let thinking = if blank {
        configured.or(meta.thinking)
    } else {
        meta.thinking.or(configured)
    };
    SessionDefaults {
        fast: (meta.fast || blank && config.fast) && model.supports_fast(),
        workflow: meta.workflow || blank && config.workflow,
        thinking: thinking.filter(|_| model.supports_thinking()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maki_providers::model::{FastSupport, ThinkingSupport};
    use maki_storage::sessions::{Effort, StoredThinking};
    use test_case::test_case;

    const SPEC: &str = "anthropic/claude-opus-5";
    const HIGH: Option<StoredThinking> = Some(StoredThinking::Effort {
        level: Effort::High,
    });
    const LOW: Option<StoredThinking> = Some(StoredThinking::Effort { level: Effort::Low });
    const OFF: Option<StoredThinking> = Some(StoredThinking::Off);

    struct Case {
        config: SessionDefaults,
        saved: Option<StoredThinking>,
        meta: SessionDefaults,
        blank: bool,
    }

    fn knobs(fast: bool, workflow: bool, thinking: Option<StoredThinking>) -> SessionDefaults {
        SessionDefaults {
            fast,
            workflow,
            thinking,
        }
    }

    fn model(supported: bool) -> Model {
        let mut model = Model::from_spec(SPEC).unwrap();
        model.supports_fast_override = Some(if supported {
            FastSupport::Supported
        } else {
            FastSupport::Unsupported
        });
        model.thinking_override = Some(if supported {
            ThinkingSupport::Yes
        } else {
            ThinkingSupport::No
        });
        model
    }

    #[test_case(Case { config: knobs(true, true, HIGH), saved: None, meta: knobs(false, false, None), blank: true }
        => knobs(true, true, HIGH) ; "blank_session_takes_config")]
    #[test_case(Case { config: knobs(false, false, None), saved: None, meta: knobs(true, true, HIGH), blank: true }
        => knobs(true, true, HIGH) ; "silent_config_keeps_blank_session_toggles")]
    #[test_case(Case { config: knobs(false, false, HIGH), saved: LOW, meta: knobs(false, false, None), blank: true }
        => knobs(false, false, LOW) ; "saved_preference_beats_config")]
    #[test_case(Case { config: knobs(false, false, OFF), saved: None, meta: knobs(false, false, HIGH), blank: true }
        => knobs(false, false, OFF) ; "config_off_wins_on_blank_session")]
    #[test_case(Case { config: knobs(true, true, HIGH), saved: LOW, meta: knobs(false, false, OFF), blank: false }
        => knobs(false, false, OFF) ; "spoken_session_keeps_stored_off")]
    #[test_case(Case { config: knobs(false, false, HIGH), saved: LOW, meta: knobs(false, false, None), blank: false }
        => knobs(false, false, LOW) ; "spoken_session_without_level_takes_preference")]
    #[test_case(Case { config: knobs(false, false, HIGH), saved: None, meta: knobs(false, false, None), blank: false }
        => knobs(false, false, HIGH) ; "spoken_session_without_level_takes_config")]
    fn resolve_orders_session_preference_and_config(case: Case) -> SessionDefaults {
        let meta = SessionMeta {
            fast: case.meta.fast,
            workflow: case.meta.workflow,
            thinking: case.meta.thinking,
            ..Default::default()
        };
        let prefs = Prefs {
            default_thinking: case.saved,
        };
        resolve(case.config, &prefs, &meta, case.blank, &model(true))
    }

    #[test]
    fn resolve_turns_off_what_the_model_cannot_do() {
        let resolved = resolve(
            knobs(true, true, HIGH),
            &Prefs::default(),
            &SessionMeta::default(),
            true,
            &model(false),
        );
        assert_eq!(resolved, knobs(false, true, None));
    }
}
