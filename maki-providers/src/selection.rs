use crate::{Model, RequestOptions, ThinkingConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub use maki_domain::{
    FAST_VALUES, MODEL_FAST_KEY, MODEL_OPTION_KEYS, MODEL_SPEC_KEY, MODEL_THINKING_KEY,
    ModelSelection, ModelSelectionError, parse_fast, parse_thinking,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteModelSelection {
    pub spec: Arc<str>,
    #[serde(with = "maki_domain::selection::thinking")]
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
    use super::{CompleteModelSelection, ModelSelection};
    use crate::{Effort, ThinkingConfig};
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
}
