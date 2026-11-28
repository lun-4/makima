//! Canonical domain contracts shared by storage, providers, and agent consumers.

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde::{Deserialize, Serialize};

pub mod makima_thinking;
pub mod post;
pub mod selection;

pub use post::{POST_LEVELS, Post, PostLevel};

pub use selection::{
    FAST_VALUES, MODEL_FAST_KEY, MODEL_SPEC_KEY, MODEL_THINKING_KEY, ModelOption, ModelSelection,
    ModelSelectionError, parse_fast, parse_thinking,
};

pub use makima_thinking::{
    Effort, FALLBACK_MAX_THINKING_BUDGET, MIN_THINKING_BUDGET, StoredThinking, THINKING_METADATA,
    THINKING_OPTIONS, ThinkingConfig, ThinkingMetadata, ThinkingParseError,
};

pub trait LockExt<T: ?Sized> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T>;
}

impl<T: ?Sized> LockExt<T> for Mutex<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub trait RwLockExt<T: ?Sized> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T>;
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T: ?Sized> RwLockExt<T> for RwLock<T> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}

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
