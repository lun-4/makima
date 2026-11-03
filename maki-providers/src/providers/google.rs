//! The `google` protocol base's model table, removed with the protocol bases
//! in commit 9. The `google` provider is registered by `plugins/providers`.

use crate::model::{ModelEntry, ModelFamily, ModelPricing, ModelTier};

pub(crate) const fn models() -> &'static [ModelEntry] {
    &[
        ModelEntry {
            prefixes: &["gemini-2.5-pro"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gemini,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 1.25,
                output: 5.00,
                cache_write: 0.00,
                cache_read: 0.31,
                fast: None,
            },
            max_output_tokens: Some(65_536),
            context_window: 1_048_576,
        },
        ModelEntry {
            prefixes: &["gemini-2.5-flash"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gemini,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 0.15,
                output: 0.60,
                cache_write: 0.00,
                cache_read: 0.04,
                fast: None,
            },
            max_output_tokens: Some(65_536),
            context_window: 1_048_576,
        },
        ModelEntry {
            prefixes: &["gemini-2.0-flash-lite"],
            tier: ModelTier::Weak,
            family: ModelFamily::Gemini,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 0.075,
                output: 0.30,
                cache_write: 0.00,
                cache_read: 0.01,
                fast: None,
            },
            max_output_tokens: Some(65_536),
            context_window: 1_048_576,
        },
    ]
}
