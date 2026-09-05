//! Canonical domain contracts shared by storage, providers, and agent consumers.

use serde::{Deserialize, Serialize};

pub mod maki_thinking;

pub use maki_thinking::{
    Effort, FALLBACK_MAX_THINKING_BUDGET, MIN_THINKING_BUDGET, StoredThinking, THINKING_METADATA,
    THINKING_OPTIONS, ThinkingConfig, ThinkingMetadata, ThinkingParseError,
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TokenUsage {
    #[serde(default)]
    pub input: u32,
    #[serde(default)]
    pub output: u32,
    #[serde(default)]
    pub cache_creation: u32,
    #[serde(default)]
    pub cache_read: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
}

impl TokenUsage {
    pub fn total_input(&self) -> u32 {
        self.input
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_creation)
    }

    pub fn total(&self) -> u32 {
        self.total_input().saturating_add(self.output)
    }
}

impl std::ops::AddAssign for TokenUsage {
    fn add_assign(&mut self, rhs: Self) {
        self.input = self.input.saturating_add(rhs.input);
        self.output = self.output.saturating_add(rhs.output);
        self.cache_creation = self.cache_creation.saturating_add(rhs.cache_creation);
        self.cache_read = self.cache_read.saturating_add(rhs.cache_read);
        if let Some(cost) = rhs.cost {
            self.cost = Some(self.cost.unwrap_or_default() + cost);
        }
    }
}
