//! Resolved models and token accounting. `context_tokens()` sums input + output
//! + cache reads/writes because the context window limit applies to all of them combined.

use std::cmp::Ordering;
use std::fmt;
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::Arc;

use jiff::Timestamp;
use maki_storage::sessions::StoredTokenUsage;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::model_registry;
use crate::pricing::PricingSchedule;

const PER_MILLION: f64 = 1_000_000.0;
const FAST_PENDING: &str = "pending";
pub const UNKNOWN_CONTEXT_WINDOW: u32 = 128_000;

#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    #[error("model must be in 'provider/model' format")]
    InvalidFormat,
    #[error("unsupported provider '{0}'")]
    UnsupportedProvider(String),
    #[error("invalid model tier '{0}' (expected: strong, medium, weak)")]
    InvalidTier(String),
    #[error("no allowed model for {0}/{1}")]
    NoAllowedModel(String, ModelTier),
    #[error("model '{0}' is not allowed by provider model policy")]
    NotAllowed(String),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelPricing {
    pub input: f64,
    pub output: f64,
    pub cache_write: f64,
    pub cache_read: f64,
    /// Fast mode charges a premium that differs per model. `None`
    /// means the model has no fast tier, so asking for fast mode quietly falls
    /// back to standard rates instead of overcharging.
    #[serde(default)]
    pub fast: Option<FastPricing>,
}

/// What is known about one model, from a curated descriptor or discovery.
/// Absent fields stay unknown.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub context_window: Option<u32>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub pricing: Option<ModelPricing>,
    /// Scales `pricing` by the wall clock, for providers with peak hours.
    #[serde(default)]
    pub pricing_schedule: Option<PricingSchedule>,
    #[serde(default)]
    pub supports_thinking: Option<bool>,
    #[serde(default)]
    pub requires_thinking: Option<bool>,
    #[serde(default)]
    pub supports_vision: Option<bool>,
    /// `true`, `false`, or `"pending"` while the provider cannot tell yet.
    #[serde(default)]
    pub supports_fast: Option<FastSupport>,
    #[serde(default)]
    pub supports_tool_examples: Option<bool>,
    #[serde(default)]
    pub tier: Option<ModelTier>,
    /// Offered at no charge; the model picker marks it.
    #[serde(default)]
    pub free: Option<bool>,
    #[serde(default)]
    pub provider_data: Option<Value>,
}

impl ModelInfo {
    pub fn id_only(id: String) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }
}

/// Cache rates are missing on purpose: they derive from `input` with the same
/// multipliers as standard pricing, so storing them would just invite the two
/// copies to drift apart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FastPricing {
    pub input: f64,
    pub output: f64,
}

impl ModelPricing {
    pub const ZERO: Self = Self {
        input: 0.0,
        output: 0.0,
        cache_write: 0.0,
        cache_read: 0.0,
        fast: None,
    };

    pub fn is_zero(&self) -> bool {
        self.input == 0.0 && self.output == 0.0 && self.cache_write == 0.0 && self.cache_read == 0.0
    }

    /// Cache multipliers applied on top of the fast input rate.
    const CACHE_WRITE_MULTIPLIER: f64 = 1.25;
    const CACHE_READ_MULTIPLIER: f64 = 0.10;
}

/// Ordering is a cost guarantee (a subagent may never run on a pricier tier
/// than its parent), so the strength is written down in [`ModelTier::strength`]
/// instead of being inherited from declaration order, where inserting or moving
/// a variant would silently redefine "stronger". `Ord` stays because the tier is
/// also a `BTreeMap` key in `model_registry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelTier {
    Weak,
    Medium,
    Strong,
    Compaction,
}

impl ModelTier {
    /// `Compaction` is not a capability tier: it is a user-assigned slot for the
    /// cheap model that rewrites history. It therefore ranks below every agent
    /// tier, which makes it the tightest ceiling a parent can impose - a session
    /// on a compaction model hands its children that same model rather than
    /// letting them escalate to a capability tier.
    const fn strength(self) -> u8 {
        match self {
            Self::Compaction => 0,
            Self::Weak => 1,
            Self::Medium => 2,
            Self::Strong => 3,
        }
    }

    /// The single named way to cap a requested tier, so no call site re-derives
    /// the cost rule with an ad-hoc comparison.
    pub fn capped_at(self, ceiling: Self) -> Self {
        if self.strength() <= ceiling.strength() {
            self
        } else {
            ceiling
        }
    }
}

impl Ord for ModelTier {
    fn cmp(&self, other: &Self) -> Ordering {
        self.strength().cmp(&other.strength())
    }
}

impl PartialOrd for ModelTier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for ModelTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Weak => "weak",
            Self::Medium => "medium",
            Self::Strong => "strong",
            Self::Compaction => "compaction",
        })
    }
}

impl FromStr for ModelTier {
    type Err = ModelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "weak" => Ok(Self::Weak),
            "medium" => Ok(Self::Medium),
            "strong" => Ok(Self::Strong),
            "compaction" => Ok(Self::Compaction),
            other => Err(ModelError::InvalidTier(other.to_string())),
        }
    }
}

/// `Required` marks APIs that reject requests with thinking disabled;
/// [`crate::RequestOptions::clamped`] raises `Off` to minimal effort for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingSupport {
    No,
    Yes,
    Required,
}

impl ThinkingSupport {
    /// `requires` wins: an API that rejects thinking-off requests
    /// necessarily supports thinking.
    pub fn from_flags(supports: Option<bool>, requires: bool) -> Option<Self> {
        match (requires, supports) {
            (true, _) => Some(Self::Required),
            (false, Some(true)) => Some(Self::Yes),
            (false, Some(false)) => Some(Self::No),
            (false, None) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastSupport {
    /// The provider learns it from discovery, which has not answered yet.
    Pending,
    Supported,
    Unsupported,
}

impl Serialize for FastSupport {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Pending => serializer.serialize_str(FAST_PENDING),
            Self::Supported => serializer.serialize_bool(true),
            Self::Unsupported => serializer.serialize_bool(false),
        }
    }
}

impl<'de> Deserialize<'de> for FastSupport {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Declared {
            Known(bool),
            Word(String),
        }
        match Declared::deserialize(deserializer)? {
            Declared::Known(true) => Ok(Self::Supported),
            Declared::Known(false) => Ok(Self::Unsupported),
            Declared::Word(word) if word == FAST_PENDING => Ok(Self::Pending),
            Declared::Word(word) => Err(D::Error::custom(format!(
                "supports_fast must be a boolean or \"{FAST_PENDING}\", not \"{word}\""
            ))),
        }
    }
}

impl From<bool> for FastSupport {
    fn from(supported: bool) -> Self {
        if supported {
            Self::Supported
        } else {
            Self::Unsupported
        }
    }
}

#[derive(Debug, Clone)]
pub struct Model {
    pub id: String,
    pub provider: Arc<str>,
    pub tier: ModelTier,
    pub tool_examples: Option<bool>,
    /// `None` when the descriptor does not say, which reads as no thinking.
    pub thinking_support: Option<ThinkingSupport>,
    pub vision: Option<bool>,
    pub fast_support: Option<FastSupport>,
    pub pricing: ModelPricing,
    pub pricing_schedule: Option<PricingSchedule>,
    /// What the model declares it can generate, `None` when unknown.
    pub max_output_tokens: Option<u32>,
    pub context_window: u32,
    /// Offered at no charge, as the provider's descriptor says.
    pub free: bool,
}

impl Model {
    /// A model described by a registered provider's descriptor. A capability
    /// the descriptor leaves out reads as unsupported.
    pub fn from_info(provider: &str, info: &ModelInfo) -> Self {
        let spec = format!("{provider}/{}", info.id);
        Self {
            id: info.id.clone(),
            provider: Arc::from(provider),
            tier: model_registry::tier_for(&spec, info.tier),
            tool_examples: info.supports_tool_examples,
            thinking_support: ThinkingSupport::from_flags(
                info.supports_thinking,
                info.requires_thinking == Some(true),
            ),
            vision: info.supports_vision,
            fast_support: info.supports_fast,
            pricing: info.pricing.clone().unwrap_or_default(),
            pricing_schedule: info.pricing_schedule.clone(),
            max_output_tokens: info.max_output_tokens,
            context_window: info.context_window.unwrap_or(UNKNOWN_CONTEXT_WINDOW),
            free: info.free == Some(true),
        }
    }

    pub fn supports_thinking(&self) -> bool {
        self.thinking_support
            .is_some_and(|thinking| thinking != ThinkingSupport::No)
    }

    pub fn requires_thinking(&self) -> bool {
        self.thinking_support == Some(ThinkingSupport::Required)
    }

    pub fn supports_vision(&self) -> bool {
        self.vision == Some(true)
    }

    pub fn supports_tool_examples(&self) -> bool {
        self.tool_examples == Some(true)
    }

    pub fn supports_fast(&self) -> bool {
        self.fast_support == Some(FastSupport::Supported)
    }

    /// Discovery has not answered yet, so the `false` from [`Self::supports_fast`]
    /// is provisional. Frontends park the user's wish instead of rejecting it.
    pub fn fast_pending(&self) -> bool {
        self.fast_support == Some(FastSupport::Pending)
    }

    pub fn spec(&self) -> String {
        format!("{}/{}", self.provider, self.id)
    }

    /// What the provider charges right now, so it is only ever correct for a
    /// turn that just finished: under a
    /// [`PricingSchedule`] the answer moves
    /// with the clock. Anything historical wants [`Self::list_cost`].
    ///
    /// `None` on an unpriced model (oauth, local), so callers can hide the cost
    /// instead of showing a misleading "$0.000".
    pub fn billed_cost(&self, usage: &TokenUsage, fast: bool) -> Option<f64> {
        let cost = self.list_cost(usage, fast)?;
        Some(
            self.pricing_schedule
                .as_ref()
                .map_or(cost, |s| cost * s.multiplier_at(Timestamp::now())),
        )
    }

    /// The quoted rates, with no wall-clock surcharge. Deterministic, which is
    /// what makes it right for re-pricing a session whose turns never recorded
    /// what they paid: the rate back then is unknown, and the table price is
    /// the honest guess.
    pub fn list_cost(&self, usage: &TokenUsage, fast: bool) -> Option<f64> {
        (!self.pricing.is_zero()).then(|| usage.cost(&self.pricing, fast && self.supports_fast()))
    }

    pub fn parse_spec(spec: &str) -> Result<(&str, &str), ModelError> {
        let (slug, model_id) = spec.split_once('/').ok_or(ModelError::InvalidFormat)?;
        if slug.is_empty() || model_id.is_empty() {
            return Err(ModelError::InvalidFormat);
        }
        Ok((slug, model_id))
    }

    /// A model that supports thinking, vision, fast mode and tool examples,
    /// for tests that need one without a registry. Production code resolves
    /// through `ProviderRegistry`.
    #[cfg(any(test, feature = "test-util"))]
    pub fn from_spec(spec: &str) -> Result<Self, ModelError> {
        let (provider, model_id) = Self::parse_spec(spec)?;
        Ok(Self::from_info(
            provider,
            &ModelInfo {
                supports_thinking: Some(true),
                supports_vision: Some(true),
                supports_fast: Some(FastSupport::Supported),
                supports_tool_examples: Some(true),
                ..ModelInfo::id_only(model_id.to_string())
            },
        ))
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TokenUsage {
    /// Non-cached input tokens. Total input = `input + cache_read + cache_creation`.
    #[serde(rename = "input_tokens")]
    pub input: u32,
    #[serde(rename = "output_tokens")]
    pub output: u32,
    #[serde(rename = "cache_creation_input_tokens")]
    pub cache_creation: u32,
    #[serde(rename = "cache_read_input_tokens")]
    pub cache_read: u32,
}

impl From<TokenUsage> for StoredTokenUsage {
    fn from(s: TokenUsage) -> Self {
        Self {
            input: s.input,
            output: s.output,
            cache_creation: s.cache_creation,
            cache_read: s.cache_read,
            cost: None,
        }
    }
}

impl From<StoredTokenUsage> for TokenUsage {
    fn from(s: StoredTokenUsage) -> Self {
        Self {
            input: s.input,
            output: s.output,
            cache_creation: s.cache_creation,
            cache_read: s.cache_read,
        }
    }
}

impl TokenUsage {
    /// Ready to store, with what the turn was billed. No `From<TokenUsage>` on
    /// purpose: a caller that forgets the cost quietly loses money from the
    /// session total, so saying it out loud is mandatory.
    pub fn billed(&self, cost: Option<f64>) -> StoredTokenUsage {
        StoredTokenUsage {
            input: self.input,
            output: self.output,
            cache_creation: self.cache_creation,
            cache_read: self.cache_read,
            cost,
        }
    }

    pub fn total_input(&self) -> u32 {
        self.input
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_creation)
    }

    pub fn context_tokens(&self) -> u32 {
        self.total_input().saturating_add(self.output)
    }

    pub fn format(&self, cost: Option<f64>) -> String {
        self.format_cost(cost, "")
    }

    /// Like [`format`](Self::format), but marks the cost as a running total.
    pub fn format_sum_cost(&self, cost: Option<f64>) -> String {
        self.format_cost(cost, "Σ")
    }

    fn format_cost(&self, cost: Option<f64>, prefix: &str) -> String {
        let tokens = format!(
            "{}↑ {}↓",
            format_tokens(self.total_input()),
            format_tokens(self.output)
        );
        match cost {
            Some(cost) => format!("{tokens} {prefix}${cost:.3}"),
            None => tokens,
        }
    }

    /// Crate-private on purpose: pricing outside [`Model`] skips the provider's
    /// schedule.
    pub(crate) fn cost(&self, pricing: &ModelPricing, fast: bool) -> f64 {
        let (input, output, cache_write, cache_read) = match &pricing.fast {
            Some(f) if fast => (
                f.input,
                f.output,
                f.input * ModelPricing::CACHE_WRITE_MULTIPLIER,
                f.input * ModelPricing::CACHE_READ_MULTIPLIER,
            ),
            _ => (
                pricing.input,
                pricing.output,
                pricing.cache_write,
                pricing.cache_read,
            ),
        };
        self.input as f64 * input / PER_MILLION
            + self.output as f64 * output / PER_MILLION
            + self.cache_creation as f64 * cache_write / PER_MILLION
            + self.cache_read as f64 * cache_read / PER_MILLION
    }
}

pub fn format_tokens(tokens: u32) -> String {
    match tokens {
        0..1_000 => tokens.to_string(),
        1_000..1_000_000 => format!("{:.1}k", f64::from(tokens) / 1_000.0),
        _ => format!("{:.1}m", f64::from(tokens) / 1_000_000.0),
    }
}

impl AddAssign for TokenUsage {
    fn add_assign(&mut self, rhs: Self) {
        self.input = self.input.saturating_add(rhs.input);
        self.output = self.output.saturating_add(rhs.output);
        self.cache_creation = self.cache_creation.saturating_add(rhs.cache_creation);
        self.cache_read = self.cache_read.saturating_add(rhs.cache_read);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use test_case::test_case;

    const TIERS: [ModelTier; 4] = [
        ModelTier::Weak,
        ModelTier::Medium,
        ModelTier::Strong,
        ModelTier::Compaction,
    ];

    const EPSILON: f64 = 1e-10;

    /// Declaration index of `Compaction`, which a derived `Ord` would read as
    /// the strongest tier.
    const COMPACTION_DECLARED_LAST: u8 = 3;
    const SCHEDULED_PROVIDER: &str = "scheduled";
    const SCHEDULED_MODEL: &str = "peak-priced";
    const UNKNOWN_FAST_WORD: &str = "maybe";
    const SCHEDULED_INPUT_RATE: f64 = 0.66;
    const MILLION: u32 = 1_000_000;
    const INPUT_ONLY: TokenUsage = TokenUsage {
        input: MILLION,
        output: 0,
        cache_creation: 0,
        cache_read: 0,
    };
    /// Four counters that cannot be confused with each other.
    const COUNTERS: TokenUsage = TokenUsage {
        input: 11,
        output: 22,
        cache_creation: 33,
        cache_read: 44,
    };
    const RECORDED_COST: f64 = 0.25;
    const FAST_RATE: f64 = 30.0;

    #[test_case(999, "999"         ; "under_thousand")]
    #[test_case(1_000, "1.0k"      ; "thousand")]
    #[test_case(999_999, "1000.0k" ; "just_under_million")]
    #[test_case(1_000_000, "1.0m"  ; "million")]
    fn format_tokens_display(tokens: u32, expected: &str) {
        assert_eq!(format_tokens(tokens), expected);
    }

    #[test_case(TokenUsage { input: 12_000, output: 456, cache_creation: 200, cache_read: 100 }, None, "12.3k↑ 456↓" ; "without_cost")]
    #[test_case(TokenUsage { input: 1_000_000, output: 100_000, cache_creation: 200_000, cache_read: 500_000 }, Some(5.4), "1.7m↑ 100.0k↓ $5.400" ; "with_cost")]
    #[test_case(TokenUsage { input: u32::MAX, output: 1, cache_creation: 1, cache_read: 1 }, None, "4295.0m↑ 1↓" ; "input_saturates")]
    fn usage_formatting(usage: TokenUsage, cost: Option<f64>, expected: &str) {
        assert_eq!(usage.format(cost), expected);
    }

    #[test]
    fn sum_marker_applies_only_to_the_cost() {
        let usage = TokenUsage {
            input: 12_000,
            output: 456,
            cache_creation: 200,
            cache_read: 100,
        };
        assert_eq!(usage.format_sum_cost(Some(1.5)), "12.3k↑ 456↓ Σ$1.500");
        assert_eq!(usage.format_sum_cost(None), usage.format(None));
    }

    #[test_case("no-slash-here", ModelError::InvalidFormat ; "invalid_format")]
    #[test_case("/gpt-4", ModelError::InvalidFormat ; "missing_provider")]
    #[test_case("foobar/", ModelError::InvalidFormat ; "missing_model")]
    fn from_spec_errors(spec: &str, expected: ModelError) {
        let err = Model::from_spec(spec).unwrap_err();
        assert_eq!(
            std::mem::discriminant(&err),
            std::mem::discriminant(&expected)
        );
    }

    #[test]
    fn total_input_includes_cached_tokens() {
        let usage = TokenUsage {
            input: 5_000,
            output: 1_000,
            cache_creation: 10_000,
            cache_read: 150_000,
        };
        assert_eq!(usage.total_input(), 165_000);
    }

    #[test]
    fn cost_computes_all_token_types() {
        let pricing = ModelPricing {
            input: 3.00,
            output: 15.00,
            cache_write: 3.75,
            cache_read: 0.30,
            fast: None,
        };
        let usage = TokenUsage {
            input: 1_000_000,
            output: 100_000,
            cache_creation: 200_000,
            cache_read: 500_000,
        };
        let cost = usage.cost(&pricing, false);
        let expected = 3.0 + 1.5 + 0.75 + 0.15;
        assert!((cost - expected).abs() < 1e-10);
    }

    #[test]
    fn fast_mode_applies_premium_rates() {
        let pricing = ModelPricing {
            input: 5.00,
            output: 25.00,
            cache_write: 6.25,
            cache_read: 0.50,
            fast: Some(FastPricing {
                input: 30.00,
                output: 150.00,
            }),
        };
        let usage = TokenUsage {
            input: 1_000_000,
            output: 1_000_000,
            cache_creation: 1_000_000,
            cache_read: 1_000_000,
        };
        let fast = usage.cost(&pricing, true);
        let expected = 30.0 + 150.0 + 37.5 + 3.0;
        assert!((fast - expected).abs() < 1e-10);
        assert!(fast > usage.cost(&pricing, false));
    }

    #[test]
    fn fast_flag_ignored_without_fast_tier() {
        let pricing = ModelPricing {
            input: 3.00,
            output: 15.00,
            cache_write: 3.75,
            cache_read: 0.30,
            fast: None,
        };
        let usage = TokenUsage {
            input: 1_000_000,
            output: 1_000_000,
            cache_creation: 0,
            cache_read: 0,
        };
        assert_eq!(usage.cost(&pricing, true), usage.cost(&pricing, false));
    }

    #[test_case("p/nvidia/openai/gpt-oss-120b", "nvidia/openai/gpt-oss-120b" ; "four_levels")]
    #[test_case("p/opencode/big-pickle", "opencode/big-pickle" ; "three_levels")]
    fn from_spec_keeps_nested_model_ids(spec: &str, expected_id: &str) {
        let model = Model::from_spec(spec).unwrap();
        assert_eq!(model.provider, Arc::<str>::from("p"));
        assert_eq!(model.id, expected_id);
        assert_eq!(model.spec(), spec);
    }

    #[test_case(ModelTier::Strong, ModelTier::Weak, ModelTier::Weak ; "strong_child_capped_to_weak_parent")]
    #[test_case(ModelTier::Weak, ModelTier::Strong, ModelTier::Weak ; "weak_child_stays_weak_under_strong_parent")]
    #[test_case(ModelTier::Medium, ModelTier::Medium, ModelTier::Medium ; "equal_tiers_pass_through")]
    #[test_case(ModelTier::Strong, ModelTier::Compaction, ModelTier::Compaction ; "strong_child_capped_to_compaction_parent")]
    #[test_case(ModelTier::Medium, ModelTier::Compaction, ModelTier::Compaction ; "medium_child_capped_to_compaction_parent")]
    #[test_case(ModelTier::Compaction, ModelTier::Strong, ModelTier::Compaction ; "compaction_child_is_not_escalated")]
    fn capped_at_never_exceeds_ceiling(
        requested: ModelTier,
        ceiling: ModelTier,
        expected: ModelTier,
    ) {
        assert_eq!(requested.capped_at(ceiling), expected);
        assert!(requested.capped_at(ceiling) <= ceiling);
    }

    #[test]
    fn every_tier_under_a_compaction_ceiling_stays_at_compaction() {
        for &tier in &TIERS {
            assert_eq!(tier.capped_at(ModelTier::Compaction), ModelTier::Compaction);
        }
    }

    /// Fails if the variants get reordered (the hand-written strength table
    /// would no longer be the thing that disagrees with declaration order) or if
    /// the explicit `Ord` is ever replaced by a derive, which would rank
    /// `Compaction` above `Strong` and turn the subagent cap into a no-op.
    #[test]
    fn tier_order_is_explicit_not_declaration_order() {
        assert_eq!(ModelTier::Compaction as u8, COMPACTION_DECLARED_LAST);
        assert!(ModelTier::Compaction < ModelTier::Weak);

        let mut tiers = TIERS;
        tiers.sort();
        assert_eq!(
            tiers,
            [
                ModelTier::Compaction,
                ModelTier::Weak,
                ModelTier::Medium,
                ModelTier::Strong
            ]
        );
    }

    #[test]
    fn tier_display_roundtrip() {
        for &tier in &TIERS {
            let s = tier.to_string();
            assert_eq!(s.parse::<ModelTier>().unwrap(), tier);
        }
        assert!(matches!(
            "turbo".parse::<ModelTier>(),
            Err(ModelError::InvalidTier(_))
        ));
    }

    #[test_case(json!(true), FastSupport::Supported ; "supported")]
    #[test_case(json!(false), FastSupport::Unsupported ; "unsupported")]
    #[test_case(json!(FAST_PENDING), FastSupport::Pending ; "pending")]
    fn fast_support_round_trips(declared: Value, expected: FastSupport) {
        let info: ModelInfo =
            serde_json::from_value(json!({ "id": SCHEDULED_MODEL, "supports_fast": declared }))
                .unwrap();
        assert_eq!(info.supports_fast, Some(expected));
        assert_eq!(serde_json::to_value(expected).unwrap(), declared);
        let model = Model::from_info(SCHEDULED_PROVIDER, &info);
        assert_eq!(model.fast_pending(), expected == FastSupport::Pending);
    }

    #[test]
    fn fast_support_rejects_other_words() {
        let declared = json!({ "id": SCHEDULED_MODEL, "supports_fast": UNKNOWN_FAST_WORD });
        assert!(serde_json::from_value::<ModelInfo>(declared).is_err());
    }

    #[test_case(None, false ; "undeclared_is_unsupported")]
    #[test_case(Some(true), true ; "declared")]
    #[test_case(Some(false), false ; "declared_off")]
    fn fast_follows_the_descriptor(declared: Option<bool>, expected: bool) {
        let info = ModelInfo {
            supports_fast: declared.map(FastSupport::from),
            pricing: Some(ModelPricing {
                fast: Some(FastPricing {
                    input: FAST_RATE,
                    output: FAST_RATE,
                }),
                ..ModelPricing::ZERO
            }),
            ..ModelInfo::id_only(SCHEDULED_MODEL.into())
        };
        assert_eq!(
            Model::from_info(SCHEDULED_PROVIDER, &info).supports_fast(),
            expected
        );
    }

    #[test]
    fn fast_without_fast_pricing_uses_standard_rates() {
        let model = Model {
            fast_support: Some(FastSupport::Supported),
            pricing: ModelPricing {
                input: SCHEDULED_INPUT_RATE,
                ..ModelPricing::ZERO
            },
            ..Model::from_spec("p/m").unwrap()
        };
        assert_eq!(
            model.list_cost(&COUNTERS, true),
            Some(COUNTERS.cost(&model.pricing, false))
        );
    }

    /// A descriptor quoting off-peak rates with a peak-hours schedule.
    fn scheduled_model(input_rate: Option<f64>) -> Model {
        let pricing = input_rate.map(
            |input| json!({"input": input, "output": 0.0, "cache_write": 0.0, "cache_read": 0.0}),
        );
        let info: ModelInfo = serde_json::from_value(json!({
            "id": SCHEDULED_MODEL,
            "pricing": pricing,
            "pricing_schedule": {
                "windows": [{"start": 1, "end": 4}, {"start": 6, "end": 10}],
                "weekdays_only": true,
                "multiplier": 2.0,
            },
        }))
        .unwrap();
        Model::from_info(SCHEDULED_PROVIDER, &info)
    }

    /// `list_cost` stays out of the schedule, `billed_cost` applies it.
    /// `billed_cost` reads the real clock, so the expectation is sampled either
    /// side of the call in case the hour ticks over mid-test.
    #[test]
    fn a_scheduled_model_bills_its_peak_surcharge_on_top_of_the_table() {
        let model = scheduled_model(Some(SCHEDULED_INPUT_RATE));
        let schedule = model.pricing_schedule.as_ref().unwrap();

        let list = model.list_cost(&INPUT_ONLY, false).unwrap();
        let table_price = f64::from(INPUT_ONLY.input) * model.pricing.input / PER_MILLION;
        assert!(
            (list - table_price).abs() < EPSILON,
            "list_cost {list} must be the table price {table_price}, surcharge free"
        );

        let before = schedule.multiplier_at(Timestamp::now());
        let billed = model.billed_cost(&INPUT_ONLY, false).unwrap();
        let after = schedule.multiplier_at(Timestamp::now());
        assert!(
            [before, after]
                .iter()
                .any(|multiplier| (billed - list * multiplier).abs() < EPSILON),
            "billed {billed} is not {list} scaled by the schedule ({before} or {after})"
        );
    }

    /// A schedule must not turn "no price" into "$0.000". Callers hide `None`,
    /// and any multiple of nothing is still nothing.
    #[test]
    fn unpriced_models_stay_unpriced_under_a_schedule() {
        let model = scheduled_model(None);
        assert!(model.pricing.is_zero());
        assert_eq!(model.list_cost(&INPUT_ONLY, false), None);
        assert_eq!(model.billed_cost(&INPUT_ONLY, false), None);
    }

    /// Every later total is rebuilt from what was stored, so storing a turn must
    /// not shuffle the counters, invent one, or drop the cost.
    #[test]
    fn billed_stores_every_counter_and_the_cost() {
        assert_eq!(
            COUNTERS.billed(Some(RECORDED_COST)),
            StoredTokenUsage {
                input: COUNTERS.input,
                output: COUNTERS.output,
                cache_creation: COUNTERS.cache_creation,
                cache_read: COUNTERS.cache_read,
                cost: Some(RECORDED_COST),
            }
        );
        assert_eq!(COUNTERS.billed(None).cost, None);
    }
}
