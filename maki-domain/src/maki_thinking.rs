use std::fmt;
use std::str::FromStr;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};
use thiserror::Error;

pub const MIN_THINKING_BUDGET: u32 = 1_024;
pub const FALLBACK_MAX_THINKING_BUDGET: u32 = 32_768;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
}

impl Effort {
    pub const ALL: [Self; 6] = [
        Self::Minimal,
        Self::Low,
        Self::Medium,
        Self::High,
        Self::XHigh,
        Self::Max,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Max => "max",
        }
    }

    pub const fn percent(self) -> u32 {
        match self {
            Self::Minimal => 10,
            Self::Low => 20,
            Self::Medium => 40,
            Self::High => 60,
            Self::XHigh => 80,
            Self::Max => 100,
        }
    }

    pub fn budget(self, max: u32) -> u32 {
        let max = max.max(MIN_THINKING_BUDGET);
        let tokens = (u64::from(max) * u64::from(self.percent()) / 100) as u32;
        tokens.clamp(MIN_THINKING_BUDGET, max)
    }

    pub fn from_budget(tokens: u32, max: u32) -> Self {
        let percent = u64::from(tokens).saturating_mul(100) / u64::from(max.max(1));
        Self::ALL
            .into_iter()
            .find(|effort| u64::from(effort.percent()) >= percent)
            .unwrap_or(Self::Max)
    }

    pub fn snap(self, supported: &[Self]) -> Self {
        if supported.is_empty() || supported.contains(&self) {
            return self;
        }
        supported
            .iter()
            .rev()
            .find(|&&effort| effort < self)
            .copied()
            .unwrap_or(supported[0])
    }
}

impl fmt::Display for Effort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ThinkingParseError {
    #[error(
        "unknown thinking value {0:?} (use off, adaptive, minimal, low, medium, high, xhigh, max, or a token budget)"
    )]
    Unknown(String),
    #[error("thinking budget must be greater than zero")]
    BudgetZero,
    #[error("thinking option {0} is not supported")]
    Unsupported(&'static str),
}

impl FromStr for Effort {
    type Err = ThinkingParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|effort| effort.as_str() == value)
            .ok_or_else(|| ThinkingParseError::Unknown(value.to_owned()))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ThinkingConfig {
    #[default]
    Off,
    Adaptive,
    Effort(Effort),
    Budget(u32),
}

impl ThinkingConfig {
    pub fn parse_setting(value: &str) -> Result<Self, ThinkingParseError> {
        Self::parse_with_options(value, ThinkingOptions::default())
    }

    pub fn options() -> &'static [&'static str; 8] {
        &THINKING_OPTIONS
    }

    pub fn parse_with_options(
        value: &str,
        options: ThinkingOptions,
    ) -> Result<Self, ThinkingParseError> {
        let value = value.trim();
        let config = match value {
            "off" | "false" => Self::Off,
            "adaptive" | "on" | "true" => Self::Adaptive,
            value => match value.parse() {
                Ok(effort) => Self::Effort(effort),
                Err(_) => match value.parse::<u32>() {
                    Ok(0) => return Err(ThinkingParseError::BudgetZero),
                    Ok(tokens) => Self::Budget(tokens),
                    Err(_) => return Err(ThinkingParseError::Unknown(value.to_owned())),
                },
            },
        };
        if matches!(config, Self::Adaptive) && !options.allow_adaptive {
            return Err(ThinkingParseError::Unsupported("adaptive"));
        }
        if matches!(config, Self::Budget(_)) && !options.allow_budget {
            return Err(ThinkingParseError::Unsupported("budget"));
        }
        Ok(config)
    }

    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }

    pub fn budget(self, max: Option<u32>) -> Option<u32> {
        match self {
            Self::Off => None,
            Self::Adaptive => None,
            Self::Effort(effort) => {
                Some(effort.budget(max.unwrap_or(FALLBACK_MAX_THINKING_BUDGET)))
            }
            Self::Budget(tokens) => Some(match max {
                Some(max) => tokens.clamp(MIN_THINKING_BUDGET, max.max(MIN_THINKING_BUDGET)),
                None => tokens.max(MIN_THINKING_BUDGET),
            }),
        }
    }

    pub fn snap(self, supported: &[Effort], max: Option<u32>) -> Self {
        match self {
            Self::Effort(effort) => Self::Effort(effort.snap(supported)),
            Self::Budget(tokens) if !supported.is_empty() => Self::Effort(
                Effort::from_budget(tokens, max.unwrap_or(FALLBACK_MAX_THINKING_BUDGET))
                    .snap(supported),
            ),
            _ => self,
        }
    }
}

impl fmt::Display for ThinkingConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Off => formatter.write_str("off"),
            Self::Adaptive => formatter.write_str("adaptive"),
            Self::Effort(effort) => effort.fmt(formatter),
            Self::Budget(tokens) => tokens.fmt(formatter),
        }
    }
}

impl FromStr for ThinkingConfig {
    type Err = ThinkingParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse_setting(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "kind")]
pub enum StoredThinking {
    Off,
    Adaptive,
    Effort { level: Effort },
    Budget { tokens: u32 },
}

impl From<StoredThinking> for ThinkingConfig {
    fn from(value: StoredThinking) -> Self {
        match value {
            StoredThinking::Off => Self::Off,
            StoredThinking::Adaptive => Self::Adaptive,
            StoredThinking::Effort { level } => Self::Effort(level),
            StoredThinking::Budget { tokens } => Self::Budget(tokens),
        }
    }
}

impl From<ThinkingConfig> for StoredThinking {
    fn from(value: ThinkingConfig) -> Self {
        match value {
            ThinkingConfig::Off => Self::Off,
            ThinkingConfig::Adaptive => Self::Adaptive,
            ThinkingConfig::Effort(level) => Self::Effort { level },
            ThinkingConfig::Budget(tokens) => Self::Budget { tokens },
        }
    }
}

impl StoredThinking {
    pub fn parse_setting(input: &str) -> Result<Self, ThinkingParseError> {
        match input.trim() {
            "off" | "false" => Ok(Self::Off),
            "adaptive" | "on" | "true" => Ok(Self::Adaptive),
            value => match value.parse() {
                Ok(level) => Ok(Self::Effort { level }),
                Err(_) => match value.parse() {
                    Ok(0) => Err(ThinkingParseError::BudgetZero),
                    Ok(tokens) => Ok(Self::Budget { tokens }),
                    Err(_) => Err(ThinkingParseError::Unknown(value.to_owned())),
                },
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThinkingMetadata {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub description: &'static str,
}

pub const THINKING_METADATA: ThinkingMetadata = ThinkingMetadata {
    name: "thinking",
    aliases: &["reasoning"],
    description: "Configure thinking effort or token budget",
};

pub const THINKING_OPTIONS: [&str; 8] = [
    "off", "adaptive", "minimal", "low", "medium", "high", "xhigh", "max",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThinkingOptions {
    pub allow_adaptive: bool,
    pub allow_budget: bool,
}

impl Default for ThinkingOptions {
    fn default() -> Self {
        Self {
            allow_adaptive: true,
            allow_budget: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase", tag = "kind")]
enum StoredThinkingConfig {
    Off,
    Adaptive,
    Effort { level: Effort },
    Budget { tokens: u32 },
}

impl Serialize for ThinkingConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stored = match self {
            Self::Off => StoredThinkingConfig::Off,
            Self::Adaptive => StoredThinkingConfig::Adaptive,
            Self::Effort(level) => StoredThinkingConfig::Effort { level: *level },
            Self::Budget(tokens) => StoredThinkingConfig::Budget { tokens: *tokens },
        };
        stored.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ThinkingConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let stored = StoredThinkingRepr::deserialize(deserializer)?;
        match stored {
            StoredThinkingRepr::Tagged {
                kind,
                level,
                tokens,
            } => match kind.as_str() {
                "off" if level.is_none() && tokens.is_none() => Ok(Self::Off),
                "adaptive" if level.is_none() && tokens.is_none() => Ok(Self::Adaptive),
                "effort" => level
                    .map(Self::Effort)
                    .ok_or_else(|| de::Error::custom("effort thinking requires level")),
                "budget" => tokens
                    .map(Self::Budget)
                    .ok_or_else(|| de::Error::custom("budget thinking requires tokens")),
                _ => Err(de::Error::custom("invalid thinking configuration")),
            },
            StoredThinkingRepr::Legacy(value) => {
                Self::parse_setting(&value).map_err(de::Error::custom)
            }
            StoredThinkingRepr::LegacyObject { mode } => {
                Self::parse_setting(&mode).map_err(de::Error::custom)
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StoredThinkingRepr {
    Tagged {
        kind: String,
        level: Option<Effort>,
        tokens: Option<u32>,
    },
    LegacyObject {
        mode: String,
    },
    Legacy(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thinking_config_round_trips_legacy_tagged_schema() {
        let json = serde_json::to_string(&ThinkingConfig::Effort(Effort::High)).unwrap();
        assert_eq!(json, r#"{"kind":"effort","level":"high"}"#);
        assert_eq!(
            serde_json::from_str::<ThinkingConfig>(&json).unwrap(),
            ThinkingConfig::Effort(Effort::High)
        );
        assert_eq!(
            serde_json::from_str::<ThinkingConfig>(r#""high""#).unwrap(),
            ThinkingConfig::Effort(Effort::High)
        );
        assert_eq!(
            serde_json::from_str::<ThinkingConfig>(r#"{"mode":"high"}"#).unwrap(),
            ThinkingConfig::Effort(Effort::High)
        );
    }

    #[test]
    fn thinking_config_parses_aliases_and_budgets() {
        assert_eq!("on".parse::<ThinkingConfig>(), Ok(ThinkingConfig::Adaptive));
        assert_eq!(
            ThinkingConfig::parse_with_options(
                "adaptive",
                ThinkingOptions {
                    allow_adaptive: false,
                    ..ThinkingOptions::default()
                }
            ),
            Err(ThinkingParseError::Unsupported("adaptive"))
        );
        assert_eq!(
            "reasoning".parse::<ThinkingConfig>(),
            Err(ThinkingParseError::Unknown("reasoning".into()))
        );
        assert_eq!(
            "  4096 ".parse::<ThinkingConfig>(),
            Ok(ThinkingConfig::Budget(4096))
        );
        assert_eq!(
            "0".parse::<ThinkingConfig>(),
            Err(ThinkingParseError::BudgetZero)
        );
    }

    #[test]
    fn effort_budget_and_snap_helpers_follow_contract() {
        assert_eq!(Effort::Medium.budget(32_768), 13_107);
        assert_eq!(
            Effort::Max.snap(&[Effort::High, Effort::XHigh]),
            Effort::XHigh
        );
        assert_eq!(
            ThinkingConfig::Budget(20_000).snap(&[Effort::Low, Effort::High], Some(32_768)),
            ThinkingConfig::Effort(Effort::High)
        );
    }
}
