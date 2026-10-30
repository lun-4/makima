use crate::{Model, RequestOptions, ThinkingConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub use maki_domain::{
    FAST_VALUES, FastCapability, MODEL_CAPABILITIES_UNRESOLVED, MODEL_FAST_KEY, MODEL_FAST_PENDING,
    MODEL_FAST_UNSUPPORTED, MODEL_OPTION_KEYS, MODEL_OPTION_SPECS, MODEL_SPEC_KEY,
    MODEL_THINKING_KEY, MODEL_THINKING_REQUIRED, MODEL_THINKING_UNSUPPORTED, ModelCapabilities,
    ModelOptionAvailability, ModelOptionPreset, ModelOptionSpec, ModelOptionValueKind,
    ModelSelection, ModelSelectionError, ThinkingCapability, model_option_spec, parse_fast,
    parse_thinking,
};

impl From<&Model> for ModelCapabilities {
    fn from(model: &Model) -> Self {
        Self {
            resolved: true,
            thinking: if model.requires_thinking() {
                ThinkingCapability::Required
            } else if model.supports_thinking() {
                ThinkingCapability::Supported
            } else {
                ThinkingCapability::Unsupported
            },
            fast: if model.supports_fast() {
                FastCapability::Supported
            } else if model.fast_pending() {
                FastCapability::Pending
            } else {
                FastCapability::Unsupported
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteModelSelection {
    pub spec: Arc<str>,
    #[serde(with = "maki_domain::selection::thinking")]
    pub thinking: ThinkingConfig,
    pub fast: bool,
}

impl CompleteModelSelection {
    /// A complete display value, not a draft of requested overrides.
    pub fn as_selection(&self) -> ModelSelection {
        ModelSelection {
            spec: Some(Arc::clone(&self.spec)),
            thinking: Some(self.thinking),
            fast: Some(self.fast),
        }
    }

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
    use super::{
        CompleteModelSelection, FastCapability, MODEL_CAPABILITIES_UNRESOLVED, MODEL_FAST_KEY,
        MODEL_FAST_PENDING, MODEL_FAST_UNSUPPORTED, MODEL_OPTION_SPECS, MODEL_THINKING_KEY,
        MODEL_THINKING_REQUIRED, MODEL_THINKING_UNSUPPORTED, ModelCapabilities,
        ModelOptionAvailability, ModelSelection, ThinkingCapability, model_option_spec,
    };
    use crate::model::FastSupport;
    use crate::{
        Effort, Model, ModelInfo, RequestOptions, ThinkingConfig, ThinkingSupport, model_registry,
    };
    use serde_json::json;
    use std::sync::Arc;
    use test_case::test_case;

    const BASE_MODEL_SPEC: &str = "anthropic/claude-opus-5";

    #[test_case(ThinkingSupport::No, FastSupport::Unsupported, ThinkingCapability::Unsupported, FastCapability::Unsupported; "unsupported")]
    #[test_case(ThinkingSupport::Yes, FastSupport::Supported, ThinkingCapability::Supported, FastCapability::Supported; "supported")]
    #[test_case(ThinkingSupport::Required, FastSupport::Pending, ThinkingCapability::Required, FastCapability::Pending; "required_pending")]
    fn model_option_capabilities_follow_resolved_model(
        thinking: ThinkingSupport,
        fast: FastSupport,
        expected_thinking: ThinkingCapability,
        expected_fast: FastCapability,
    ) {
        let mut model = Model::from_spec(BASE_MODEL_SPEC).unwrap();
        model.thinking_override = Some(thinking);
        model.supports_fast_override = Some(fast);
        let selection = ModelSelection {
            thinking: Some(ThinkingConfig::Effort(Effort::XHigh)),
            fast: Some(false),
            ..ModelSelection::default()
        };
        let original = selection.clone();
        let capabilities = ModelCapabilities::from(&model);
        assert_eq!(
            capabilities,
            ModelCapabilities {
                resolved: true,
                thinking: expected_thinking,
                fast: expected_fast
            }
        );
        let thinking_availability = match thinking {
            ThinkingSupport::No => ModelOptionAvailability::Unavailable {
                reason: MODEL_THINKING_UNSUPPORTED,
            },
            ThinkingSupport::Yes => ModelOptionAvailability::Available { note: None },
            ThinkingSupport::Required => ModelOptionAvailability::Available {
                note: Some(MODEL_THINKING_REQUIRED),
            },
        };
        let fast_availability = match fast {
            FastSupport::Unsupported => ModelOptionAvailability::Unavailable {
                reason: MODEL_FAST_UNSUPPORTED,
            },
            FastSupport::Supported => ModelOptionAvailability::Available { note: None },
            FastSupport::Pending => ModelOptionAvailability::Pending {
                reason: MODEL_FAST_PENDING,
            },
        };
        assert_eq!(
            model_option_spec(MODEL_THINKING_KEY)
                .unwrap()
                .availability(&capabilities),
            thinking_availability
        );
        assert_eq!(
            model_option_spec(MODEL_FAST_KEY)
                .unwrap()
                .availability(&capabilities),
            fast_availability
        );
        assert_eq!(selection, original);
        assert_eq!(
            selection.option_assignments().collect::<Vec<_>>(),
            ["thinking=xhigh", "fast=false"]
        );
    }

    #[test_case("anthropic/claude-opus-5", true, true; "static_fast_pricing")]
    #[test_case("google/gemini-2.5-pro", true, false; "static_thinking")]
    #[test_case("synthetic/model-option-fallback", true, false; "provider_fallback")]
    fn model_capabilities_preserve_model_fallbacks(spec: &str, thinking: bool, fast: bool) {
        let model = Model::from_spec(spec).unwrap();
        let capabilities = ModelCapabilities::from(&model);
        assert_eq!(
            model_option_spec(MODEL_THINKING_KEY)
                .unwrap()
                .availability(&capabilities)
                .is_available(),
            thinking
        );
        assert_eq!(
            model_option_spec(MODEL_FAST_KEY)
                .unwrap()
                .availability(&capabilities)
                .is_available(),
            fast
        );
    }

    #[test]
    fn discovered_and_provider_adjusted_models_project_their_own_capabilities() {
        const PROVIDER: &str = "llama-cpp";
        const MODEL_ID: &str = "model-option-discovered-thinking";
        model_registry::set_known_models(
            PROVIDER,
            vec![ModelInfo {
                supports_thinking: Some(true),
                ..ModelInfo::id_only(MODEL_ID.into())
            }],
        );
        let mut model = Model::from_spec(&format!("{PROVIDER}/{MODEL_ID}")).unwrap();
        assert_eq!(
            ModelCapabilities::from(&model).thinking,
            ThinkingCapability::Supported
        );
        model.provider = Arc::from("aperture");
        model.thinking_override = Some(ThinkingSupport::Required);
        model.supports_fast_override = Some(FastSupport::Supported);
        assert_eq!(
            ModelCapabilities::from(&model),
            ModelCapabilities {
                resolved: true,
                thinking: ThinkingCapability::Required,
                fast: FastCapability::Supported
            }
        );
    }

    #[test]
    fn unavailable_resolution_exposes_no_controls() {
        let capabilities = ModelCapabilities::default();
        for spec in MODEL_OPTION_SPECS {
            assert_eq!(
                spec.availability(&capabilities),
                ModelOptionAvailability::Pending {
                    reason: MODEL_CAPABILITIES_UNRESOLVED
                }
            );
        }
    }

    #[test_case(ThinkingConfig::Off, ThinkingConfig::Effort(Effort::Minimal); "required_thinking")]
    #[test_case(ThinkingConfig::Effort(Effort::XHigh), ThinkingConfig::Effort(Effort::XHigh); "canonical_effort_unchanged")]
    fn complete_display_exposes_normalized_typed_values(
        thinking: ThinkingConfig,
        expected: ThinkingConfig,
    ) {
        let mut model = Model::from_spec(BASE_MODEL_SPEC).unwrap();
        model.thinking_override = Some(ThinkingSupport::Required);
        let complete = CompleteModelSelection::from_model(
            &model,
            RequestOptions {
                thinking,
                fast: true,
            },
        );
        let display = complete.as_selection();
        assert_eq!(display.spec.as_deref(), Some(BASE_MODEL_SPEC));
        assert_eq!(display.thinking, Some(expected));
        assert_eq!(display.fast, Some(true));
        assert_eq!(
            display
                .option_values()
                .map(|(spec, value)| (spec.key, value))
                .collect::<Vec<_>>(),
            [
                (MODEL_THINKING_KEY, expected.to_string()),
                (MODEL_FAST_KEY, true.to_string())
            ]
        );
    }

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
