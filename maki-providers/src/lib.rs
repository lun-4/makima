pub(crate) mod error;
pub(crate) mod image;
pub mod login;
pub mod model;
pub mod model_registry;
pub mod pricing;
pub mod provider;
pub mod registry;
pub(crate) mod request_delivery;
pub mod retry;
pub mod selection;
#[cfg(any(test, feature = "test-util"))]
pub mod test_util;
pub mod tokens;
pub(crate) mod types;

pub use error::AgentError;
pub use maki_domain::{Effort, ThinkingConfig};
pub use maki_storage::sessions::add_cost;
pub use model::{
    FastPricing, FastSupport, Model, ModelError, ModelInfo, ModelPricing, ModelTier,
    ThinkingSupport, TokenUsage, format_tokens,
};
pub use pricing::{model_cost, settle_session};
pub use provider::Timeouts;
pub use registry::{
    PreparedProvider, ProviderBackend, ProviderDocs, ProviderListing, ProviderRegistry,
    RegisteredProvider,
};
pub use request_delivery::{RequestDelivery, RequestDeliveryMarker, RequestDeliveryTracker};
pub use retry::{RetryKind, RetryPolicy, RetryState};
pub use selection::{
    CompleteModelSelection, FAST_VALUES, MODEL_FAST_KEY, MODEL_SPEC_KEY, MODEL_THINKING_KEY,
    ModelCapabilities, ModelOption, ModelSelection, ModelSelectionError, parse_fast,
    parse_thinking,
};
pub use tokens::{ContextGauge, estimate_message_tokens, estimate_prompt_tokens};
pub use types::{
    ContentBlock, ConversationEnd, ConversationLink, ConversationRelation, EMPTY_RESPONSE_MARKER,
    IMAGE_EVICTED_NOTE, IMAGE_OMITTED_NOTE, IMAGE_PLACEHOLDER, IMAGE_UNUSABLE_NOTE, ImageMediaType,
    ImageSource, Invocation, InvocationPurpose, Message, MessageKind, ModelUsageRow,
    ObservationSource, ProviderEvent, ProviderUsage, RequestOptions, Role, StopReason,
    StreamResponse, THINKING_USAGE, ThinkingConfigExt, UsageLimit, UsageWindow,
    adapt_images_for_model,
};
