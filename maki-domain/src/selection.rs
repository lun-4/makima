use crate::{THINKING_OPTIONS, ThinkingConfig};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

pub const MODEL_SPEC_KEY: &str = "spec";
pub const MODEL_THINKING_KEY: &str = "thinking";
pub const MODEL_FAST_KEY: &str = "fast";

const THINKING_PRESETS: [ModelOptionPreset; THINKING_OPTIONS.len()] = {
    let mut presets = [const {
        ModelOptionPreset {
            value: "",
            label: "",
        }
    }; THINKING_OPTIONS.len()];
    let mut index = 0;
    while index < THINKING_OPTIONS.len() {
        presets[index] = ModelOptionPreset {
            value: THINKING_OPTIONS[index],
            label: THINKING_OPTIONS[index],
        };
        index += 1;
    }
    presets
};
const FAST_PRESETS: [ModelOptionPreset; 2] = [
    ModelOptionPreset {
        value: "true",
        label: "True",
    },
    ModelOptionPreset {
        value: "false",
        label: "False",
    },
];
pub const FAST_VALUES: [&str; FAST_PRESETS.len()] = [FAST_PRESETS[0].value, FAST_PRESETS[1].value];
/// Defines model-option parsing, editing, display, and capability availability.
///
/// An option using an existing control kind adds a typed `ModelSelection` field and
/// descriptor here, a complete/runtime field, capability projection, and one mapping
/// in `ConfigPatch::from_model_selection`. Provider and persistence support remain
/// explicit. Picker, completion, and summary consumers iterate these descriptors.
pub const MODEL_OPTION_SPECS: &[ModelOptionSpec] = &[
    ModelOptionSpec {
        key: MODEL_THINKING_KEY,
        label: "Thinking",
        description: "Reasoning effort used for future requests",
        presets: &THINKING_PRESETS,
        value_kind: ModelOptionValueKind::FreeInput,
        read: |selection| selection.thinking.map(|value| value.to_string()),
        replace: |selection, value| {
            selection.thinking = value.map(parse_thinking).transpose()?;
            Ok(())
        },
        availability: thinking_availability,
    },
    ModelOptionSpec {
        key: MODEL_FAST_KEY,
        label: "Fast",
        description: "Use the model's fast service tier",
        presets: &FAST_PRESETS,
        value_kind: ModelOptionValueKind::Choice,
        read: |selection| selection.fast.map(|value| value.to_string()),
        replace: |selection, value| {
            selection.fast = value.map(parse_fast).transpose()?;
            Ok(())
        },
        availability: fast_availability,
    },
];
pub const MODEL_OPTION_KEYS: [&str; MODEL_OPTION_SPECS.len()] = {
    let mut keys = [""; MODEL_OPTION_SPECS.len()];
    let mut index = 0;
    while index < MODEL_OPTION_SPECS.len() {
        keys[index] = MODEL_OPTION_SPECS[index].key;
        index += 1;
    }
    keys
};

pub const MODEL_CAPABILITIES_UNRESOLVED: &str = "Model capabilities have not been resolved";
pub const MODEL_THINKING_UNSUPPORTED: &str = "Thinking is unsupported by this model";
pub const MODEL_THINKING_REQUIRED: &str = "Thinking is required; off uses minimal effort";
pub const MODEL_FAST_UNSUPPORTED: &str = "Fast mode is unsupported by this model";
pub const MODEL_FAST_PENDING: &str = "Fast mode support is pending discovery";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelOptionPreset {
    pub value: &'static str,
    pub label: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelOptionValueKind {
    FreeInput,
    Choice,
}

#[derive(Debug, Clone)]
pub struct ModelOptionSpec {
    pub key: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub presets: &'static [ModelOptionPreset],
    pub value_kind: ModelOptionValueKind,
    pub read: fn(&ModelSelection) -> Option<String>,
    pub replace: fn(&mut ModelSelection, Option<&str>) -> Result<(), ModelSelectionError>,
    pub availability: fn(&ModelCapabilities) -> ModelOptionAvailability,
}

impl ModelOptionSpec {
    pub fn read(&self, selection: &ModelSelection) -> Option<String> {
        (self.read)(selection)
    }

    pub fn replace(
        &self,
        selection: &mut ModelSelection,
        value: Option<&str>,
    ) -> Result<(), ModelSelectionError> {
        (self.replace)(selection, value.filter(|value| !value.is_empty()))
    }

    pub fn availability(&self, capabilities: &ModelCapabilities) -> ModelOptionAvailability {
        (self.availability)(capabilities)
    }
}

pub fn model_option_spec(key: &str) -> Option<&'static ModelOptionSpec> {
    MODEL_OPTION_SPECS.iter().find(|spec| spec.key == key)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ThinkingCapability {
    #[default]
    Unsupported,
    Supported,
    Required,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FastCapability {
    #[default]
    Unsupported,
    Supported,
    Pending,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelCapabilities {
    pub resolved: bool,
    pub thinking: ThinkingCapability,
    pub fast: FastCapability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelOptionAvailability {
    Available { note: Option<&'static str> },
    Pending { reason: &'static str },
    Unavailable { reason: &'static str },
}

impl ModelOptionAvailability {
    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available { .. })
    }

    pub fn explanation(&self) -> Option<&'static str> {
        match self {
            Self::Available { note } => *note,
            Self::Pending { reason } | Self::Unavailable { reason } => Some(reason),
        }
    }
}

fn thinking_availability(capabilities: &ModelCapabilities) -> ModelOptionAvailability {
    if !capabilities.resolved {
        return ModelOptionAvailability::Pending {
            reason: MODEL_CAPABILITIES_UNRESOLVED,
        };
    }
    match capabilities.thinking {
        ThinkingCapability::Unsupported => ModelOptionAvailability::Unavailable {
            reason: MODEL_THINKING_UNSUPPORTED,
        },
        ThinkingCapability::Supported => ModelOptionAvailability::Available { note: None },
        ThinkingCapability::Required => ModelOptionAvailability::Available {
            note: Some(MODEL_THINKING_REQUIRED),
        },
    }
}

fn fast_availability(capabilities: &ModelCapabilities) -> ModelOptionAvailability {
    if !capabilities.resolved {
        return ModelOptionAvailability::Pending {
            reason: MODEL_CAPABILITIES_UNRESOLVED,
        };
    }
    match capabilities.fast {
        FastCapability::Unsupported => ModelOptionAvailability::Unavailable {
            reason: MODEL_FAST_UNSUPPORTED,
        },
        FastCapability::Supported => ModelOptionAvailability::Available { note: None },
        FastCapability::Pending => ModelOptionAvailability::Pending {
            reason: MODEL_FAST_PENDING,
        },
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ModelSelection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<Arc<str>>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "optional_thinking::serialize"
    )]
    pub thinking: Option<ThinkingConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fast: Option<bool>,
}

impl<'de> Deserialize<'de> for ModelSelection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Setting {
            Text(String),
            Budget(u32),
            Flag(bool),
        }

        impl Setting {
            fn text(self) -> String {
                match self {
                    Self::Text(text) => text,
                    Self::Budget(tokens) => tokens.to_string(),
                    Self::Flag(flag) => flag.to_string(),
                }
            }
        }

        struct SelectionVisitor;

        impl<'de> Visitor<'de> for SelectionVisitor {
            type Value = ModelSelection;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a model selection")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut selection = ModelSelection::default();
                let mut seen = Vec::with_capacity(MODEL_OPTION_SPECS.len() + 1);
                while let Some(key) = map.next_key::<String>()? {
                    if key != MODEL_SPEC_KEY && model_option_spec(&key).is_none() {
                        return Err(de::Error::custom(ModelSelectionError::Unknown(key)));
                    }
                    if seen.contains(&key) {
                        return Err(if key == MODEL_SPEC_KEY {
                            de::Error::custom(ModelSelectionError::MultipleSpecs)
                        } else {
                            de::Error::custom(ModelSelectionError::Duplicate(key))
                        });
                    }
                    if key == MODEL_SPEC_KEY {
                        if let Some(spec) = map.next_value::<Option<String>>()? {
                            selection.set_spec(spec).map_err(de::Error::custom)?;
                        }
                    } else if let Some(value) = map.next_value::<Option<Setting>>()? {
                        selection
                            .set_option(&key, &value.text())
                            .map_err(de::Error::custom)?;
                    }
                    seen.push(key);
                }
                Ok(selection)
            }
        }

        deserializer.deserialize_map(SelectionVisitor)
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ModelSelectionError {
    #[error("duplicate model option: {0}")]
    Duplicate(String),
    #[error("unknown model option: {0}")]
    Unknown(String),
    #[error("model spec must not be empty")]
    EmptySpec,
    #[error("expected at most one model spec")]
    MultipleSpecs,
    #[error("fast must be true or false")]
    InvalidFast,
    #[error("{0}")]
    InvalidThinking(String),
}

impl ModelSelection {
    pub fn set_spec(&mut self, spec: impl Into<Arc<str>>) -> Result<(), ModelSelectionError> {
        let spec = spec.into();
        if spec.is_empty() {
            return Err(ModelSelectionError::EmptySpec);
        }
        if self.spec.is_some() {
            return Err(ModelSelectionError::MultipleSpecs);
        }
        self.spec = Some(spec);
        Ok(())
    }

    pub fn set_option(&mut self, key: &str, value: &str) -> Result<(), ModelSelectionError> {
        let spec =
            model_option_spec(key).ok_or_else(|| ModelSelectionError::Unknown(key.into()))?;
        if spec.read(self).is_some() {
            return Err(ModelSelectionError::Duplicate(key.into()));
        }
        (spec.replace)(self, Some(value))
    }

    pub fn replace_option(
        &mut self,
        key: &str,
        value: Option<&str>,
    ) -> Result<(), ModelSelectionError> {
        model_option_spec(key)
            .ok_or_else(|| ModelSelectionError::Unknown(key.into()))?
            .replace(self, value)
    }

    pub fn clear_option(&mut self, key: &str) -> Result<(), ModelSelectionError> {
        self.replace_option(key, None)
    }

    pub fn option_values(&self) -> impl Iterator<Item = (&'static ModelOptionSpec, String)> + '_ {
        MODEL_OPTION_SPECS
            .iter()
            .filter_map(|spec| spec.read(self).map(|value| (spec, value)))
    }

    pub fn option_assignments(&self) -> impl Iterator<Item = String> + '_ {
        self.option_values()
            .map(|(spec, value)| format!("{}={value}", spec.key))
    }

    pub fn has_option_overrides(&self) -> bool {
        self.option_values().next().is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.spec.is_none() && !self.has_option_overrides()
    }
}

pub fn parse_thinking(value: &str) -> Result<ThinkingConfig, ModelSelectionError> {
    ThinkingConfig::parse_setting(value)
        .map_err(|error| ModelSelectionError::InvalidThinking(error.to_string()))
}

pub fn parse_fast(value: &str) -> Result<bool, ModelSelectionError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ModelSelectionError::InvalidFast),
    }
}

pub mod thinking {
    use super::{Deserialize, Deserializer, Serializer, ThinkingConfig, de, parse_thinking};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Setting(String),
        Budget(u32),
    }

    pub fn serialize<S: Serializer>(
        value: &ThinkingConfig,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            ThinkingConfig::Budget(tokens) => serializer.serialize_u32(*tokens),
            value => serializer.serialize_str(&value.to_string()),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ThinkingConfig, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let setting = match value {
            Value::Setting(value) => value,
            Value::Budget(tokens) => tokens.to_string(),
        };
        parse_thinking(&setting).map_err(de::Error::custom)
    }
}

mod optional_thinking {
    use super::{Serializer, ThinkingConfig, thinking};

    pub fn serialize<S: Serializer>(
        value: &Option<ThinkingConfig>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => thinking::serialize(value, serializer),
            None => serializer.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FAST_VALUES, FastCapability, MODEL_CAPABILITIES_UNRESOLVED, MODEL_FAST_KEY,
        MODEL_FAST_PENDING, MODEL_FAST_UNSUPPORTED, MODEL_OPTION_KEYS, MODEL_OPTION_SPECS,
        MODEL_SPEC_KEY, MODEL_THINKING_KEY, MODEL_THINKING_REQUIRED, MODEL_THINKING_UNSUPPORTED,
        ModelCapabilities, ModelOptionAvailability, ModelOptionValueKind, ModelSelection,
        ModelSelectionError, ThinkingCapability, model_option_spec,
    };
    use crate::{Effort, ThinkingConfig};
    use serde_json::{Value, json};
    use test_case::test_case;

    const BUDGET_ZERO_ERROR: &str = "thinking budget must be greater than zero";
    const MODEL_SPEC: &str = "openai/gpt-5";

    fn built(
        spec: Option<&str>,
        options: &[(&str, &str)],
    ) -> Result<ModelSelection, ModelSelectionError> {
        let mut selection = ModelSelection::default();
        if let Some(spec) = spec {
            selection.set_spec(spec)?;
        }
        for (key, value) in options {
            selection.set_option(key, value)?;
        }
        Ok(selection)
    }

    #[test_case(json!({"spec": "openai/gpt-5"}), Some("openai/gpt-5"), &[]; "spec_only")]
    #[test_case(json!({"spec": ""}), Some(""), &[]; "empty_spec")]
    #[test_case(json!({"thinking": "high"}), None, &[(MODEL_THINKING_KEY, "high")]; "thinking_text")]
    #[test_case(json!({"thinking": 2048}), None, &[(MODEL_THINKING_KEY, "2048")]; "thinking_budget")]
    #[test_case(json!({"thinking": true}), None, &[(MODEL_THINKING_KEY, "true")]; "thinking_flag")]
    #[test_case(json!({"thinking": "nope"}), None, &[(MODEL_THINKING_KEY, "nope")]; "thinking_invalid")]
    #[test_case(json!({"fast": true}), None, &[(MODEL_FAST_KEY, "true")]; "fast_flag")]
    #[test_case(json!({"fast": "false"}), None, &[(MODEL_FAST_KEY, "false")]; "fast_text")]
    #[test_case(json!({"fast": "yes"}), None, &[(MODEL_FAST_KEY, "yes")]; "fast_invalid")]
    #[test_case(json!({"spec": null, "thinking": null, "fast": null}), None, &[]; "nulls_omit")]
    #[test_case(json!({"thinking": false, "fast": false}), None, &[(MODEL_THINKING_KEY, "false"), (MODEL_FAST_KEY, "false")]; "explicit_disabled")]
    #[test_case(json!({"thinking": "on"}), None, &[(MODEL_THINKING_KEY, "on")]; "thinking_alias")]
    #[test_case(json!({"thinking": ""}), None, &[(MODEL_THINKING_KEY, "")]; "empty_thinking")]
    #[test_case(json!({"fast": ""}), None, &[(MODEL_FAST_KEY, "")]; "empty_fast")]
    #[test_case(json!({"fast": 1}), None, &[(MODEL_FAST_KEY, "1")]; "numeric_fast_invalid")]
    #[test_case(json!({"thinking": 0}), None, &[(MODEL_THINKING_KEY, "0")]; "zero_budget")]
    fn structured_input_matches_command_options(
        input: Value,
        spec: Option<&str>,
        options: &[(&str, &str)],
    ) {
        let structured = serde_json::from_value::<ModelSelection>(input).map_err(|e| e.to_string());
        let command = built(spec, options).map_err(|e| e.to_string());
        assert_eq!(structured, command);
    }

    #[test]
    fn model_option_specs_parse_replace_clear_and_format() {
        for spec in MODEL_OPTION_SPECS {
            for preset in spec.presets {
                let mut selection = ModelSelection::default();
                spec.replace(&mut selection, Some(preset.value)).unwrap();
                assert_eq!(spec.read(&selection).as_deref(), Some(preset.value));
                assert!(selection.has_option_overrides());
                assert!(!selection.is_empty());
                assert_eq!(
                    selection.option_assignments().collect::<Vec<_>>(),
                    [format!("{}={}", spec.key, preset.value)]
                );
                assert_eq!(
                    selection.set_option(spec.key, preset.value),
                    Err(ModelSelectionError::Duplicate(spec.key.into()))
                );
                spec.replace(&mut selection, Some(preset.value)).unwrap();
                assert_eq!(spec.read(&selection).as_deref(), Some(preset.value));
                selection.clear_option(spec.key).unwrap();
                assert!(selection.is_empty());
                selection.set_option(spec.key, preset.value).unwrap();
                selection.replace_option(spec.key, Some("")).unwrap();
                assert!(selection.is_empty());
            }
        }
        let mut selection = built(
            Some(MODEL_SPEC),
            &[(MODEL_FAST_KEY, "false"), (MODEL_THINKING_KEY, "off")],
        )
        .unwrap();
        assert_eq!(
            selection.option_assignments().collect::<Vec<_>>(),
            ["thinking=off", "fast=false"]
        );
        for spec in MODEL_OPTION_SPECS {
            spec.replace(&mut selection, None).unwrap();
        }
        assert!(!selection.has_option_overrides());
        assert!(!selection.is_empty());
    }

    #[test_case(MODEL_THINKING_KEY, "2048", "2048"; "free_budget")]
    #[test_case(MODEL_THINKING_KEY, "on", "adaptive"; "enabled_alias")]
    #[test_case(MODEL_THINKING_KEY, "false", "off"; "disabled_alias")]
    #[test_case(MODEL_THINKING_KEY, " high ", "high"; "trimmed_effort")]
    fn model_option_values_are_canonical(key: &str, value: &str, expected: &str) {
        let spec = model_option_spec(key).unwrap();
        let mut selection = ModelSelection::default();
        spec.replace(&mut selection, Some(value)).unwrap();
        assert_eq!(spec.read(&selection).as_deref(), Some(expected));
    }

    #[test_case(MODEL_THINKING_KEY, "0", ModelSelectionError::InvalidThinking(BUDGET_ZERO_ERROR.into()); "zero_budget")]
    #[test_case(MODEL_FAST_KEY, "enabled", ModelSelectionError::InvalidFast; "legacy_boolean_not_command_value")]
    fn invalid_replacement_preserves_existing_value(
        key: &str,
        value: &str,
        expected: ModelSelectionError,
    ) {
        let spec = model_option_spec(key).unwrap();
        let mut selection = ModelSelection::default();
        spec.replace(&mut selection, Some(spec.presets[0].value))
            .unwrap();
        let original = selection.clone();
        assert_eq!(spec.replace(&mut selection, Some(value)), Err(expected));
        assert_eq!(selection, original);
    }

    #[test]
    fn compatibility_keys_and_value_kinds_follow_descriptors() {
        assert_eq!(
            MODEL_OPTION_KEYS.as_slice(),
            MODEL_OPTION_SPECS
                .iter()
                .map(|spec| spec.key)
                .collect::<Vec<_>>()
        );
        let fast = model_option_spec(MODEL_FAST_KEY).unwrap();
        assert_eq!(
            FAST_VALUES.as_slice(),
            fast.presets
                .iter()
                .map(|preset| preset.value)
                .collect::<Vec<_>>()
        );
        assert_eq!(fast.value_kind, ModelOptionValueKind::Choice);
        assert_eq!(
            model_option_spec(MODEL_THINKING_KEY).unwrap().value_kind,
            ModelOptionValueKind::FreeInput
        );
    }

    #[test_case(ThinkingCapability::Unsupported, FastCapability::Unsupported,
        ModelOptionAvailability::Unavailable { reason: MODEL_THINKING_UNSUPPORTED },
        ModelOptionAvailability::Unavailable { reason: MODEL_FAST_UNSUPPORTED }; "unsupported")]
    #[test_case(ThinkingCapability::Supported, FastCapability::Supported,
        ModelOptionAvailability::Available { note: None },
        ModelOptionAvailability::Available { note: None }; "supported")]
    #[test_case(ThinkingCapability::Required, FastCapability::Pending,
        ModelOptionAvailability::Available { note: Some(MODEL_THINKING_REQUIRED) },
        ModelOptionAvailability::Pending { reason: MODEL_FAST_PENDING }; "required_pending")]
    fn descriptor_availability_uses_typed_capabilities(
        thinking: ThinkingCapability,
        fast: FastCapability,
        expected_thinking: ModelOptionAvailability,
        expected_fast: ModelOptionAvailability,
    ) {
        let capabilities = ModelCapabilities {
            resolved: true,
            thinking,
            fast,
        };
        assert_eq!(
            model_option_spec(MODEL_THINKING_KEY)
                .unwrap()
                .availability(&capabilities),
            expected_thinking
        );
        assert_eq!(
            model_option_spec(MODEL_FAST_KEY)
                .unwrap()
                .availability(&capabilities),
            expected_fast
        );
    }

    #[test]
    fn unresolved_capabilities_expose_no_available_options() {
        for spec in MODEL_OPTION_SPECS {
            assert_eq!(
                spec.availability(&ModelCapabilities::default()),
                ModelOptionAvailability::Pending {
                    reason: MODEL_CAPABILITIES_UNRESOLVED
                }
            );
        }
    }

    #[test_case("null", "null"; "null_null")]
    #[test_case("null", "value"; "null_value")]
    #[test_case("value", "null"; "value_null")]
    #[test_case("value", "value"; "value_value")]
    fn structured_selection_rejects_duplicate_wire_keys(first: &str, second: &str) {
        let fields = [(MODEL_SPEC_KEY, "\"openai/gpt-5\"")].into_iter().chain(
            MODEL_OPTION_SPECS.iter().map(|spec| {
                (
                    spec.key,
                    if spec.key == MODEL_THINKING_KEY {
                        "\"high\""
                    } else {
                        "true"
                    },
                )
            }),
        );
        for (key, value) in fields {
            let first = if first == "value" { value } else { first };
            let second = if second == "value" { value } else { second };
            let input = format!("{{\"{key}\":{first},\"{key}\":{second}}}");
            let error = serde_json::from_str::<ModelSelection>(&input).unwrap_err();
            let expected = if key == MODEL_SPEC_KEY {
                ModelSelectionError::MultipleSpecs
            } else {
                ModelSelectionError::Duplicate(key.into())
            };
            assert!(
                error.to_string().starts_with(&expected.to_string()),
                "{input}: {error}"
            );
        }
    }

    #[test_case(json!({"spec": true}); "boolean_identity")]
    #[test_case(json!({"thinking": -1}); "negative_budget")]
    #[test_case(json!({"thinking": 1.5}); "fractional_budget")]
    #[test_case(json!({"fast": []}); "array_setting")]
    #[test_case(json!({"thinking": {"effort": "high"}}); "object_setting")]
    fn structured_input_rejects_invalid_scalar_types(input: Value) {
        assert!(serde_json::from_value::<ModelSelection>(input).is_err());
    }

    #[test_case(json!({"model_spec": "a/b"}); "unknown_value")]
    #[test_case(json!({"model_spec": null}); "unknown_null")]
    fn structured_input_rejects_unknown_keys(input: Value) {
        let error = serde_json::from_value::<ModelSelection>(input).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&ModelSelectionError::Unknown("model_spec".into()).to_string())
        );
    }

    #[test_case(None, json!({"spec": MODEL_SPEC, "fast": false}); "omitted_thinking")]
    #[test_case(Some(ThinkingConfig::Off), json!({"spec": MODEL_SPEC, "thinking": "off", "fast": false}); "off")]
    #[test_case(Some(ThinkingConfig::Effort(Effort::XHigh)), json!({"spec": MODEL_SPEC, "thinking": "xhigh", "fast": false}); "effort")]
    #[test_case(Some(ThinkingConfig::Budget(2048)), json!({"spec": MODEL_SPEC, "thinking": 2048, "fast": false}); "numeric_budget")]
    fn structured_serialization_preserves_wire_shape(
        thinking: Option<ThinkingConfig>,
        expected: Value,
    ) {
        let selection = ModelSelection {
            spec: Some(MODEL_SPEC.into()),
            thinking,
            fast: Some(false),
        };
        assert_eq!(serde_json::to_value(&selection).unwrap(), expected);
        assert_eq!(
            serde_json::from_value::<ModelSelection>(expected).unwrap(),
            selection
        );
    }
}
