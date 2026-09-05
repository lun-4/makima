use crate::types::ThinkingConfigExt;
use std::sync::{Arc, Mutex};

use flume::Sender;
use maki_storage::id::SessionRef;
use serde::Deserialize;
use serde_json::Value;

use crate::model::{Model, ModelEntry, ModelFamily, ModelPricing, ModelTier};
use crate::provider::{BoxFuture, Provider};
use crate::{
    AgentError, Message, ProviderEvent, ProviderUsage, RequestOptions, StreamResponse, UsageLimit,
    UsageWindow, dialect,
};

use super::openai_compat::{OpenAiCompatConfig, OpenAiCompatProvider};
use super::{KeyPool, ResolvedAuth};

static CONFIG: OpenAiCompatConfig = OpenAiCompatConfig {
    slug: "synthetic",
    api_key_env: "SYNTHETIC_API_KEY",
    base_url: "https://api.synthetic.new/openai/v1",
    max_tokens_field: "max_completion_tokens",
    include_stream_usage: false,
    provider_name: "Synthetic",
};

const QUOTA_URL: &str = "https://api.synthetic.new/v2/quotas";
const EMPTY_USAGE_ERROR: &str = "Synthetic usage response contained no recognisable quota lane; the endpoint schema likely changed";

inventory::submit!(maki_config::providers::BuiltInProvider {
    slug: "synthetic",
    display_name: "Synthetic",
    protocol: maki_config::providers::Protocol::Openai,
    default_base_url: "https://api.synthetic.new/openai/v1",
    default_api_key_env: "SYNTHETIC_API_KEY",
    default_model: "synthetic/hf:moonshotai/Kimi-K2.5",
    plans: None,
    login_url: Some("https://synthetic.new"),
    needs_url: false,
});

pub(crate) const fn models() -> &'static [ModelEntry] {
    &[
        ModelEntry {
            prefixes: &["hf:moonshotai/Kimi-K2.5"],
            tier: ModelTier::Strong,
            family: ModelFamily::Synthetic,
            vision: false,
            default: true,
            pricing: ModelPricing {
                input: 0.45,
                output: 3.40,
                cache_write: 0.00,
                cache_read: 0.00,
                fast: None,
            },
            max_output_tokens: Some(131072),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["hf:deepseek-ai/DeepSeek-V3.2"],
            tier: ModelTier::Medium,
            family: ModelFamily::Synthetic,
            vision: false,
            default: true,
            pricing: ModelPricing {
                input: 0.56,
                output: 1.68,
                cache_write: 0.00,
                cache_read: 0.00,
                fast: None,
            },
            max_output_tokens: Some(131072),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["hf:zai-org/GLM-4.7-Flash"],
            tier: ModelTier::Weak,
            family: ModelFamily::Synthetic,
            vision: false,
            default: true,
            pricing: ModelPricing {
                input: 0.10,
                output: 0.50,
                cache_write: 0.00,
                cache_read: 0.00,
                fast: None,
            },
            max_output_tokens: Some(131072),
            context_window: 200_000,
        },
    ]
}

pub struct Synthetic {
    compat: OpenAiCompatProvider,
    auth: Arc<Mutex<ResolvedAuth>>,
    key_pool: Option<KeyPool>,
    system_prefix: Option<String>,
}

impl Synthetic {
    pub fn new(timeouts: super::Timeouts) -> Result<Self, AgentError> {
        let pool = KeyPool::resolve("synthetic", CONFIG.api_key_env)?;
        Ok(Self {
            compat: OpenAiCompatProvider::new(&CONFIG, timeouts),
            auth: Arc::new(Mutex::new(ResolvedAuth::bearer(pool.current()))),
            key_pool: Some(pool),
            system_prefix: None,
        })
    }

    pub(crate) fn with_auth(auth: Arc<Mutex<ResolvedAuth>>, timeouts: super::Timeouts) -> Self {
        Self {
            compat: OpenAiCompatProvider::new(&CONFIG, timeouts),
            auth,
            key_pool: None,
            system_prefix: None,
        }
    }

    pub(crate) fn with_system_prefix(mut self, prefix: Option<String>) -> Self {
        self.system_prefix = prefix;
        self
    }
}

impl Provider for Synthetic {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let auth = self.auth.lock().unwrap().clone();
            let mut buf = String::new();
            let system = super::with_prefix(&self.system_prefix, system, &mut buf);
            let mut body = self.compat.build_body(model, messages, system, tools);
            opts.thinking
                .apply_reasoning_effort(&mut body, &dialect::STANDARD, model);
            self.compat
                .do_stream(model, &[], &body, event_tx, &auth)
                .await
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<crate::model::ModelInfo>, AgentError>> {
        Box::pin(async move {
            let auth = self.auth.lock().unwrap().clone();
            self.compat.do_list_models(&auth).await
        })
    }

    fn rotate_key(&self) -> BoxFuture<'_, Result<bool, AgentError>> {
        Box::pin(async {
            Ok(self
                .key_pool
                .as_ref()
                .is_some_and(|p| p.rotate_auth(&self.auth, ResolvedAuth::bearer)))
        })
    }

    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        Box::pin(async move {
            let auth = self.auth.lock().unwrap().clone();
            let body = self.compat.get_text(&auth, QUOTA_URL).await?;
            Ok(Some(parse_quotas(&body)?))
        })
    }
}

/// Legacy v2 `subscription` lane, used as a `5h` fallback when the v3 rolling
/// lanes are absent.
#[derive(Deserialize)]
struct QuotaLane {
    limit: f64,
    requests: f64,
    #[serde(default, rename = "renewsAt")]
    renews_at: Option<String>,
}

/// v3 rolling five-hour request lane: counts down `remaining` within `max`.
#[derive(Deserialize)]
struct FiveHourLimit {
    remaining: f64,
    max: f64,
    #[serde(default, rename = "nextTickAt")]
    next_tick_at: Option<String>,
}

/// v3 weekly credit lane: reports a *remaining* percentage directly.
#[derive(Deserialize)]
struct WeeklyLimit {
    #[serde(rename = "percentRemaining")]
    percent_remaining: f64,
    #[serde(default, rename = "nextRegenAt")]
    next_regen_at: Option<String>,
}

#[derive(Deserialize)]
struct QuotasResponse {
    #[serde(default)]
    subscription: Option<QuotaLane>,
    #[serde(default, rename = "rollingFiveHourLimit")]
    rolling_five_hour: Option<FiveHourLimit>,
    #[serde(default, rename = "weeklyTokenLimit")]
    weekly_token: Option<WeeklyLimit>,
}

fn usage_percentage(used: f64, limit: f64) -> Option<u32> {
    if limit <= 0.0 || !used.is_finite() || !limit.is_finite() {
        return None;
    }
    Some((100.0 * used / limit).round().clamp(0.0, 100.0) as u32)
}

/// `/v2/quotas` timestamps are RFC 3339 (e.g. `renewsAt`); the UI expects
/// epoch milliseconds.
fn parse_reset(rfc3339: &str) -> Option<u64> {
    let ts: jiff::Timestamp = rfc3339.parse().ok()?;
    u64::try_from(ts.as_millisecond()).ok()
}

fn parse_quotas(response: &str) -> Result<ProviderUsage, AgentError> {
    let resp: QuotasResponse = serde_json::from_str(response)?;
    let mut limits = Vec::with_capacity(2);
    // v3 counts the five-hour request lane down from `max`; v2 only exposes
    // `subscription` (requests `limit`). Prefer the v3 key, fall back.
    if let Some(lane) = resp.rolling_five_hour.as_ref() {
        limits.push(UsageLimit {
            kind: UsageWindow::Hours(5),
            percentage: usage_percentage(lane.max - lane.remaining, lane.max),
            reset_at: lane.next_tick_at.as_deref().and_then(parse_reset),
            detail: None,
        });
    } else if let Some(lane) = resp.subscription.as_ref() {
        limits.push(UsageLimit {
            kind: UsageWindow::Hours(5),
            percentage: usage_percentage(lane.requests, lane.limit),
            reset_at: lane.renews_at.as_deref().and_then(parse_reset),
            detail: None,
        });
    }
    if let Some(lane) = resp.weekly_token.as_ref() {
        limits.push(UsageLimit {
            kind: UsageWindow::Weekly { model: None },
            percentage: usage_percentage(100.0 - lane.percent_remaining, 100.0),
            reset_at: lane.next_regen_at.as_deref().and_then(parse_reset),
            detail: None,
        });
    }
    if limits.is_empty() {
        return Err(AgentError::Config {
            message: EMPTY_USAGE_ERROR.into(),
        });
    }
    Ok(ProviderUsage { plan: None, limits })
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    /// 2000-01-01T00:00:00Z as epoch milliseconds, the expected `reset_at`.
    const EPOCH_MS: u64 = 946_684_800_000;

    const V3_BODY: &str = r#"{
        "subscription": {"limit": 500, "requests": 0, "renewsAt": "2030-01-01T00:00:00.000Z"},
        "rollingFiveHourLimit": {"remaining": 300, "max": 1000, "limited": false, "nextTickAt": "2000-01-01T00:00:00.000Z"},
        "weeklyTokenLimit": {"percentRemaining": 50.0, "maxCredits": "$24.00", "nextRegenAt": "2000-01-01T00:00:00.000Z"}
    }"#;

    const V2_BODY: &str = r#"{
        "subscription": {"limit": 500, "requests": 250, "renewsAt": "2000-01-01T00:00:00.000Z"},
        "search": {"hourly": {"limit": 10, "requests": 2}}
    }"#;

    #[test]
    fn parses_v3_rolling_lanes() {
        let usage = parse_quotas(V3_BODY).unwrap();
        assert!(usage.plan.is_none());
        // 1000 max, 300 remaining → 700 used → 70%;
        // weekly reports 50% remaining → 50% used.
        assert_eq!(
            usage.limits,
            vec![
                UsageLimit {
                    kind: UsageWindow::Hours(5),
                    percentage: Some(70),
                    reset_at: Some(EPOCH_MS),
                    detail: None,
                },
                UsageLimit {
                    kind: UsageWindow::Weekly { model: None },
                    percentage: Some(50),
                    reset_at: Some(EPOCH_MS),
                    detail: None,
                },
            ]
        );
    }

    #[test]
    fn falls_back_to_v2_subscription_lane() {
        let usage = parse_quotas(V2_BODY).unwrap();
        assert_eq!(usage.limits.len(), 1);
        assert_eq!(
            usage.limits[0],
            UsageLimit {
                kind: UsageWindow::Hours(5),
                percentage: Some(50),
                reset_at: Some(EPOCH_MS),
                detail: None,
            }
        );
    }

    #[test_case("not json"; "garbage")]
    #[test_case("{}"; "empty object")]
    #[test_case(r#"{"rollingFiveHourLimit": {"remaining": 1}}"#; "missing max")]
    fn rejects_malformed_or_empty_bodies(body: &str) {
        assert!(parse_quotas(body).is_err());
    }

    #[test]
    fn empty_body_reports_helpful_error() {
        assert_eq!(
            parse_quotas("{}").unwrap_err().to_string(),
            EMPTY_USAGE_ERROR
        );
    }

    #[test_case(300.0, 1000.0, Some(30); "rounded down")]
    #[test_case(250.0, 1000.0, Some(25); "exact")]
    #[test_case(999.0, 1000.0, Some(100); "clamped at top")]
    #[test_case(1500.0, 1000.0, Some(100); "over 100")]
    #[test_case(1.0, 0.0, None; "zero limit")]
    fn usage_percentage_clamps(used: f64, limit: f64, expected: Option<u32>) {
        assert_eq!(usage_percentage(used, limit), expected);
    }
}
