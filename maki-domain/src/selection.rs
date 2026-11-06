use crate::{THINKING_OPTIONS, ThinkingConfig};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::sync::Arc;
use thiserror::Error;

pub const MODEL_SPEC_KEY: &str = "spec";
pub const MODEL_THINKING_KEY: &str = "thinking";
pub const MODEL_FAST_KEY: &str = "fast";
pub const FAST_VALUES: [&str; 2] = ["true", "false"];

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

/// The options a [`ModelSelection`] can override besides the model itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelOption {
    Thinking,
    Fast,
}

impl ModelOption {
    pub const ALL: [Self; 2] = [Self::Thinking, Self::Fast];

    pub fn from_key(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|option| option.key() == key)
    }

    pub fn key(self) -> &'static str {
        match self {
            Self::Thinking => MODEL_THINKING_KEY,
            Self::Fast => MODEL_FAST_KEY,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Thinking => "Thinking",
            Self::Fast => "Fast",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Thinking => "Reasoning effort used for future requests",
            Self::Fast => "Use the model's fast service tier",
        }
    }

    pub fn presets(self) -> &'static [&'static str] {
        match self {
            Self::Thinking => &THINKING_OPTIONS,
            Self::Fast => &FAST_VALUES,
        }
    }

    /// Whether values outside [`Self::presets`] are accepted, such as token budgets.
    pub fn accepts_free_input(self) -> bool {
        match self {
            Self::Thinking => true,
            Self::Fast => false,
        }
    }

    pub fn get(self, selection: &ModelSelection) -> Option<String> {
        match self {
            Self::Thinking => selection.thinking.map(|value| value.to_string()),
            Self::Fast => selection.fast.map(|value| value.to_string()),
        }
    }

    /// Parses `value` into `selection`; `None` clears the override.
    pub fn set(
        self,
        selection: &mut ModelSelection,
        value: Option<&str>,
    ) -> Result<(), ModelSelectionError> {
        match self {
            Self::Thinking => selection.thinking = value.map(parse_thinking).transpose()?,
            Self::Fast => selection.fast = value.map(parse_fast).transpose()?,
        }
        Ok(())
    }
}

/// Structured input goes through the same `set_spec` and `set_option`
/// validation as `/model` arguments.
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

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(default)]
            spec: Option<String>,
            #[serde(default)]
            thinking: Option<Setting>,
            #[serde(default)]
            fast: Option<Setting>,
        }

        let raw = Raw::deserialize(deserializer)?;
        let mut selection = Self::default();
        if let Some(spec) = raw.spec {
            selection.set_spec(spec).map_err(de::Error::custom)?;
        }
        if let Some(thinking) = raw.thinking {
            selection
                .set_option(MODEL_THINKING_KEY, &thinking.text())
                .map_err(de::Error::custom)?;
        }
        if let Some(fast) = raw.fast {
            selection
                .set_option(MODEL_FAST_KEY, &fast.text())
                .map_err(de::Error::custom)?;
        }
        Ok(selection)
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
        let option =
            ModelOption::from_key(key).ok_or_else(|| ModelSelectionError::Unknown(key.into()))?;
        if option.get(self).is_some() {
            return Err(ModelSelectionError::Duplicate(key.into()));
        }
        option.set(self, Some(value))
    }

    pub fn has_option_overrides(&self) -> bool {
        self.thinking.is_some() || self.fast.is_some()
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
        MODEL_FAST_KEY, MODEL_THINKING_KEY, ModelOption, ModelSelection, ModelSelectionError,
    };
    use serde_json::{Value, json};
    use test_case::test_case;

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

    /// Structured input and `/model` arguments agree on every value.
    #[test_case(json!({"spec": "openai/gpt-5"}), Some("openai/gpt-5"), &[]; "spec_only")]
    #[test_case(json!({"spec": ""}), Some(""), &[]; "empty_spec")]
    #[test_case(json!({"thinking": "high"}), None, &[(MODEL_THINKING_KEY, "high")]; "thinking_text")]
    #[test_case(json!({"thinking": 2048}), None, &[(MODEL_THINKING_KEY, "2048")]; "thinking_budget")]
    #[test_case(json!({"thinking": true}), None, &[(MODEL_THINKING_KEY, "true")]; "thinking_flag")]
    #[test_case(json!({"thinking": "nope"}), None, &[(MODEL_THINKING_KEY, "nope")]; "thinking_invalid")]
    #[test_case(json!({"fast": true}), None, &[(MODEL_FAST_KEY, "true")]; "fast_flag")]
    #[test_case(json!({"fast": "false"}), None, &[(MODEL_FAST_KEY, "false")]; "fast_text")]
    #[test_case(json!({"fast": "yes"}), None, &[(MODEL_FAST_KEY, "yes")]; "fast_invalid")]
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
    fn structured_input_rejects_unknown_keys() {
        assert!(serde_json::from_value::<ModelSelection>(json!({"model_spec": "a/b"})).is_err());
    }

    #[test_case(ModelOption::Thinking, "on", "adaptive"; "thinking_alias_is_canonical")]
    #[test_case(ModelOption::Thinking, "2048", "2048"; "thinking_budget")]
    #[test_case(ModelOption::Fast, "false", "false"; "explicit_disabled_fast")]
    fn option_set_get_round_trips_canonically(option: ModelOption, value: &str, expected: &str) {
        let mut selection = ModelSelection::default();
        option.set(&mut selection, Some(value)).unwrap();
        assert_eq!(option.get(&selection).as_deref(), Some(expected));
        option.set(&mut selection, None).unwrap();
        assert!(selection.is_empty());
    }
}
