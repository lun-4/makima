use crate::{Model, RequestOptions, ThinkingConfig, ThinkingSupport};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub use makima_domain::{
    FAST_VALUES, MODEL_FAST_KEY, MODEL_SPEC_KEY, MODEL_THINKING_KEY, ModelOption, ModelSelection,
    ModelSelectionError, parse_fast, parse_thinking,
};

const THINKING_REQUIRED_NOTE: &str = "off uses minimal effort";

/// The [`ModelOption`]s a provider-adjusted model accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelCapabilities {
    pub thinking: ThinkingSupport,
    pub fast: bool,
}

impl ModelCapabilities {
    pub const NONE: Self = Self {
        thinking: ThinkingSupport::No,
        fast: false,
    };

    pub fn supports(self, option: ModelOption) -> bool {
        match option {
            ModelOption::Thinking => self.thinking != ThinkingSupport::No,
            ModelOption::Fast => self.fast,
        }
    }

    /// A caveat worth showing beside a supported option.
    pub fn note(self, option: ModelOption) -> Option<&'static str> {
        match option {
            ModelOption::Thinking => {
                (self.thinking == ThinkingSupport::Required).then_some(THINKING_REQUIRED_NOTE)
            }
            ModelOption::Fast => None,
        }
    }
}

impl From<&Model> for ModelCapabilities {
    fn from(model: &Model) -> Self {
        Self {
            thinking: if model.requires_thinking() {
                ThinkingSupport::Required
            } else if model.supports_thinking() == Some(true) {
                ThinkingSupport::Yes
            } else {
                ThinkingSupport::No
            },
            fast: model.supports_fast(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteModelSelection {
    pub spec: Arc<str>,
    #[serde(with = "makima_domain::selection::thinking")]
    pub thinking: ThinkingConfig,
    pub fast: bool,
}

impl CompleteModelSelection {
    pub fn from_model(model: &Model, options: RequestOptions) -> Self {
        let options = options.clamped(model);
        Self {
            spec: Arc::from(model.spec()),
            thinking: options.thinking,
            fast: options.fast,
        }
    }
}

impl From<CompleteModelSelection> for ModelSelection {
    fn from(selection: CompleteModelSelection) -> Self {
        Self {
            spec: Some(selection.spec),
            thinking: Some(selection.thinking),
            fast: Some(selection.fast),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CompleteModelSelection, ModelCapabilities, ModelOption, ModelSelection};
    use crate::model::FastSupport;
    use crate::{Effort, Model, ThinkingConfig, ThinkingSupport};
    use serde_json::json;
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(ThinkingConfig::Off, json!("off"); "disabled")]
    #[test_case(ThinkingConfig::Adaptive, json!("adaptive"); "adaptive")]
    #[test_case(ThinkingConfig::Effort(Effort::High), json!("high"); "effort")]
    #[test_case(ThinkingConfig::Budget(2048), json!(2048); "budget")]
    fn complete_selection_round_trips_as_patch(
        thinking: ThinkingConfig,
        expected: serde_json::Value,
    ) {
        let selection = CompleteModelSelection {
            spec: Arc::from("openai/gpt-5"),
            thinking,
            fast: false,
        };
        let value = serde_json::to_value(&selection).unwrap();
        assert_eq!(
            value,
            json!({"spec": "openai/gpt-5", "thinking": expected, "fast": false})
        );
        assert_eq!(
            serde_json::from_value::<CompleteModelSelection>(value.clone()).unwrap(),
            selection
        );
        assert_eq!(
            serde_json::from_value::<ModelSelection>(value).unwrap(),
            ModelSelection::from(selection)
        );
    }

    #[test_case(ThinkingSupport::No, FastSupport::Supported, false, true, None; "fast_only")]
    #[test_case(ThinkingSupport::Required, FastSupport::Pending, true, false, Some("off uses minimal effort"); "required_thinking_pending_fast")]
    fn capabilities_follow_provider_adjusted_model(
        thinking: ThinkingSupport,
        fast: FastSupport,
        supports_thinking: bool,
        supports_fast: bool,
        thinking_note: Option<&str>,
    ) {
        let mut model = Model::from_spec("anthropic/claude-opus-4-6").unwrap();
        model.thinking_support = Some(thinking);
        model.fast_support = Some(fast);
        let capabilities = ModelCapabilities::from(&model);
        assert_eq!(
            capabilities.supports(ModelOption::Thinking),
            supports_thinking
        );
        assert_eq!(capabilities.supports(ModelOption::Fast), supports_fast);
        assert_eq!(capabilities.note(ModelOption::Thinking), thinking_note);
    }
}
