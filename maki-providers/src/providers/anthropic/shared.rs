use crate::model::{FastPricing, ModelEntry, ModelFamily, ModelPricing, ModelTier};

/// A `-1m` suffix is our own convention for asking Anthropic for the 1M context
/// window.
const LONG_CONTEXT_SUFFIX: &str = "-1m";
const LONG_CONTEXT_WINDOW: u32 = 1_000_000;

/// A `-1m` model is just its base entry with a wider window.
pub(crate) fn long_context_window(model_id: &str) -> Option<u32> {
    model_id
        .ends_with(LONG_CONTEXT_SUFFIX)
        .then_some(LONG_CONTEXT_WINDOW)
}

pub(crate) const fn models() -> &'static [ModelEntry] {
    const MODELS: &[ModelEntry] = &[
        ModelEntry {
            prefixes: &["claude-haiku-4-5"],
            tier: ModelTier::Weak,
            family: ModelFamily::Claude,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 1.00,
                output: 5.00,
                cache_write: 1.25,
                cache_read: 0.10,
                fast: None,
            },
            max_output_tokens: Some(64000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-sonnet-4-5"],
            tier: ModelTier::Medium,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 3.00,
                output: 15.00,
                cache_write: 3.75,
                cache_read: 0.30,
                fast: None,
            },
            max_output_tokens: Some(64000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-sonnet-4-6"],
            tier: ModelTier::Medium,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 3.00,
                output: 15.00,
                cache_write: 3.75,
                cache_read: 0.30,
                fast: None,
            },
            max_output_tokens: Some(64000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-sonnet-5"],
            tier: ModelTier::Medium,
            family: ModelFamily::Claude,
            vision: true,
            default: true,
            // Introductory rates until 2026-09-01, then 3.00 / 15.00 / 3.75 / 0.30.
            pricing: ModelPricing {
                input: 2.00,
                output: 10.00,
                cache_write: 2.50,
                cache_read: 0.20,
                fast: None,
            },
            max_output_tokens: Some(128000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-sonnet-4"],
            tier: ModelTier::Medium,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 3.00,
                output: 15.00,
                cache_write: 3.75,
                cache_read: 0.30,
                fast: None,
            },
            max_output_tokens: Some(64000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-opus-4-5"],
            tier: ModelTier::Strong,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 5.00,
                output: 25.00,
                cache_write: 6.25,
                cache_read: 0.50,
                fast: None,
            },
            max_output_tokens: Some(64000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-opus-4-6"],
            tier: ModelTier::Strong,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 5.00,
                output: 25.00,
                cache_write: 6.25,
                cache_read: 0.50,
                // Fast mode withdrawn on 2026-06-29.
                fast: None,
            },
            max_output_tokens: Some(128000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-opus-4-7"],
            tier: ModelTier::Strong,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 5.00,
                output: 25.00,
                cache_write: 6.25,
                cache_read: 0.50,
                // Fast mode withdrawn on 2026-07-24.
                fast: None,
            },
            max_output_tokens: Some(128000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-opus-4-8"],
            tier: ModelTier::Strong,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 5.00,
                output: 25.00,
                cache_write: 6.25,
                cache_read: 0.50,
                fast: Some(FastPricing {
                    input: 10.00,
                    output: 50.00,
                }),
            },
            max_output_tokens: Some(128000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-opus-5"],
            tier: ModelTier::Strong,
            family: ModelFamily::Claude,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 5.00,
                output: 25.00,
                cache_write: 6.25,
                cache_read: 0.50,
                fast: Some(FastPricing {
                    input: 10.00,
                    output: 50.00,
                }),
            },
            max_output_tokens: Some(128000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-fable-5"],
            tier: ModelTier::Strong,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 10.00,
                output: 50.00,
                cache_write: 12.50,
                cache_read: 1.00,
                fast: None,
            },
            max_output_tokens: Some(128000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["claude-opus-4-0", "claude-opus-4-1"],
            tier: ModelTier::Strong,
            family: ModelFamily::Claude,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 15.00,
                output: 75.00,
                cache_write: 18.75,
                cache_read: 1.50,
                fast: None,
            },
            max_output_tokens: Some(32000),
            context_window: 200_000,
        },
    ];
    MODELS
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::{LONG_CONTEXT_WINDOW, long_context_window};

    #[test_case("claude-opus-4-8-1m", Some(LONG_CONTEXT_WINDOW) ; "suffix_opts_in")]
    #[test_case("claude-opus-4-8", None ; "plain_id_keeps_base")]
    fn long_context_window_follows_suffix(model_id: &str, expected: Option<u32>) {
        assert_eq!(long_context_window(model_id), expected);
    }
}
