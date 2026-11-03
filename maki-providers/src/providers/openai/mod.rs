//! The OpenAI protocol that dynamic providers with an `openai` base and the
//! custom and local providers still route through: Chat Completions, and
//! Responses for Codex models. The `openai` provider itself is registered by
//! `plugins/providers`.

pub(crate) mod responses;

use std::sync::{Arc, Mutex};

use flume::Sender;
use maki_storage::id::SessionRef;
use serde_json::Value;

use crate::model::{Model, ModelEntry, ModelFamily, ModelInfo, ModelPricing, ModelTier};
use crate::provider::{BoxFuture, NativeProvider, Provider};
use crate::providers::openai_compat::{OpenAiCompatConfig, OpenAiCompatProvider};
use crate::providers::{ResolvedAuth, Timeouts, with_prefix};
use crate::types::ThinkingConfigExt;
use crate::{
    AgentError, Invocation, Message, ProviderEvent, RequestOptions, StreamResponse, dialect,
};

const GPT_5_6_CONTEXT_WINDOW: u32 = 372_000;
const GPT_6_1_CONTEXT_WINDOW: u32 = 500_000;
const GPT_6_CONTEXT_WINDOW: u32 = 272_000;
const GPT_5_6_MAX_OUTPUT_TOKENS: u32 = 128_000;
const CODEX_MARKER: &str = "-codex";
/// ChatGPT plan models that speak Responses besides the `-codex` ones.
const PLAN_MODELS: &[&str] = &[
    "gpt-6.1-sol",
    "gpt-6.1-terra",
    "gpt-6-astra",
    "gpt-6-sol",
    "gpt-6-luna",
    "gpt-5.6-luna",
    "gpt-5.6-terra",
    "gpt-5.6-sol",
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.2",
];

static CONFIG: OpenAiCompatConfig = OpenAiCompatConfig {
    slug: "openai",
    base_url: "https://api.openai.com/v1",
    max_tokens_field: "max_completion_tokens",
    include_stream_usage: true,
    provider_name: "OpenAI",
};

fn speaks_responses(model_id: &str) -> bool {
    model_id.contains(CODEX_MARKER) || PLAN_MODELS.contains(&model_id)
}

pub(crate) struct OpenAiProtocol {
    compat: OpenAiCompatProvider,
    auth: Arc<Mutex<ResolvedAuth>>,
    system_prefix: Option<String>,
}

impl OpenAiProtocol {
    pub(crate) fn with_auth(auth: Arc<Mutex<ResolvedAuth>>, timeouts: Timeouts) -> Self {
        Self {
            compat: OpenAiCompatProvider::new(&CONFIG, timeouts),
            auth,
            system_prefix: None,
        }
    }

    pub(crate) fn with_system_prefix(mut self, prefix: Option<String>) -> Self {
        self.system_prefix = prefix;
        self
    }
}

impl Provider for OpenAiProtocol {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        event_tx: &'a Sender<ProviderEvent>,
        opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let mut buf = String::new();
            let system = with_prefix(&self.system_prefix, system, &mut buf);
            let mut auth = self.auth.lock().unwrap().clone();
            if speaks_responses(&model.id) {
                auth.base_url = Some(self.compat.base_url(&auth));
                let body = responses::build_body(responses::ResponsesRequestArgs {
                    model,
                    messages,
                    system,
                    tools,
                    thinking: Some((opts.thinking, &dialect::STANDARD)),
                });
                return responses::do_stream(
                    self.compat.client(),
                    model,
                    &body,
                    event_tx,
                    &auth,
                    self.compat.stream_timeout(),
                )
                .await;
            }
            let mut body = self.compat.build_body(model, messages, system, tools);
            opts.thinking
                .apply_reasoning_effort(&mut body, &dialect::STANDARD, model);
            self.compat
                .do_stream(model, &[], &body, event_tx, &auth)
                .await
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        let auth = self.auth.lock().unwrap().clone();
        Box::pin(async move { self.compat.do_list_models(&auth).await })
    }
}

impl NativeProvider for OpenAiProtocol {}

pub(crate) const fn models() -> &'static [ModelEntry] {
    &[
        ModelEntry {
            prefixes: &["gpt-6.1-sol"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 2.00,
                output: 10.00,
                cache_write: 2.50,
                cache_read: 0.10,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_6_1_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-6.1-terra"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 1.00,
                output: 5.00,
                cache_write: 1.25,
                cache_read: 0.05,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_6_1_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-6-astra"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 10.00,
                output: 50.00,
                cache_write: 12.50,
                cache_read: 1.00,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_6_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-6-sol"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 2.00,
                output: 10.00,
                cache_write: 2.50,
                cache_read: 0.20,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_6_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-6-luna"],
            tier: ModelTier::Weak,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 0.10,
                output: 0.50,
                cache_write: 0.125,
                cache_read: 0.01,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_6_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-5.6-luna"],
            tier: ModelTier::Weak,
            family: ModelFamily::Gpt,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 1.00,
                output: 6.00,
                cache_write: 1.25,
                cache_read: 0.10,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_5_6_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-5.6-terra"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 2.50,
                output: 15.00,
                cache_write: 3.125,
                cache_read: 0.25,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_5_6_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-5.6-sol"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: true,
            pricing: ModelPricing {
                input: 5.00,
                output: 30.00,
                cache_write: 6.25,
                cache_read: 0.50,
                fast: None,
            },
            max_output_tokens: Some(GPT_5_6_MAX_OUTPUT_TOKENS),
            context_window: GPT_5_6_CONTEXT_WINDOW,
        },
        ModelEntry {
            prefixes: &["gpt-5.4-nano"],
            tier: ModelTier::Weak,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 0.20,
                output: 1.25,
                cache_write: 0.00,
                cache_read: 0.02,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 400_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.4-mini"],
            tier: ModelTier::Weak,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 0.75,
                output: 4.50,
                cache_write: 0.00,
                cache_read: 0.075,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 400_000,
        },
        ModelEntry {
            prefixes: &["gpt-4.1-nano"],
            tier: ModelTier::Weak,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 0.10,
                output: 0.40,
                cache_write: 0.00,
                cache_read: 0.025,
                fast: None,
            },
            max_output_tokens: Some(32_768),
            context_window: 1_047_576,
        },
        ModelEntry {
            prefixes: &["gpt-4.1-mini"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 0.40,
                output: 1.60,
                cache_write: 0.00,
                cache_read: 0.10,
                fast: None,
            },
            max_output_tokens: Some(32_768),
            context_window: 1_047_576,
        },
        ModelEntry {
            prefixes: &["gpt-4.1"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 2.00,
                output: 8.00,
                cache_write: 0.00,
                cache_read: 0.50,
                fast: None,
            },
            max_output_tokens: Some(32_768),
            context_window: 1_047_576,
        },
        ModelEntry {
            prefixes: &["o4-mini"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 1.10,
                output: 4.40,
                cache_write: 0.00,
                cache_read: 0.275,
                fast: None,
            },
            max_output_tokens: Some(100_000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.5"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 5.00,
                output: 30.00,
                cache_write: 0.00,
                cache_read: 0.50,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 1_050_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.4"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 2.50,
                output: 15.00,
                cache_write: 0.00,
                cache_read: 0.25,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 1_050_000,
        },
        ModelEntry {
            prefixes: &["o3"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 2.00,
                output: 8.00,
                cache_write: 0.00,
                cache_read: 1.00,
                fast: None,
            },
            max_output_tokens: Some(100_000),
            context_window: 200_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.3-codex"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 1.75,
                output: 14.00,
                cache_write: 0.00,
                cache_read: 0.175,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 400_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.2-codex"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 1.75,
                output: 14.00,
                cache_write: 0.00,
                cache_read: 0.175,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 400_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.1-codex-mini"],
            tier: ModelTier::Medium,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 0.25,
                output: 2.00,
                cache_write: 0.00,
                cache_read: 0.025,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 400_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.1-codex-max"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 1.25,
                output: 10.00,
                cache_write: 0.00,
                cache_read: 0.125,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 400_000,
        },
        ModelEntry {
            prefixes: &["gpt-5.1-codex"],
            tier: ModelTier::Strong,
            family: ModelFamily::Gpt,
            vision: true,
            default: false,
            pricing: ModelPricing {
                input: 1.25,
                output: 10.00,
                cache_write: 0.00,
                cache_read: 0.125,
                fast: None,
            },
            max_output_tokens: Some(128_000),
            context_window: 400_000,
        },
    ]
}
