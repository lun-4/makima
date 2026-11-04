//! Message and content types for provider communication.
//! `Message.display_text`: `Some("")` marks a message as synthetic (sent to the API but hidden
//! from the UI). `user_text()` returns `None` for these, so system-injected messages
//! (cancel markers, compaction prompts) stay invisible without a separate type.
//! `Message.kind` answers a different question. Synthetic text is ours and
//! trusted, it is just not worth showing. An observation comes from outside,
//! belongs in model context, and must never be mistaken for the user talking.

use std::borrow::Cow;
use std::fmt;
use std::sync::{Arc, OnceLock};

pub use maki_domain::{Effort, ThinkingConfig};
pub use maki_storage::conversation::{ConversationLink, ConversationRelation};
use maki_storage::sessions::TitleSource;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strum::{Display, IntoStaticStr};

use crate::TokenUsage;
use crate::image::{Fix, MAX_IMAGES, fix_for_wire};
use crate::model::Model;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageMediaType {
    Png,
    Jpeg,
    Gif,
    Webp,
}

impl ImageMediaType {
    pub const ALL: [Self; 4] = [Self::Png, Self::Jpeg, Self::Gif, Self::Webp];

    /// Single source of truth for media-type strings: serde, data URLs,
    /// wire formats, and the Lua bridge all go through here.
    pub const fn mime(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Gif => "image/gif",
            Self::Webp => "image/webp",
        }
    }

    pub fn from_mime(mime: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|m| m.mime() == mime)
    }
}

impl Serialize for ImageMediaType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.mime())
    }
}

impl<'de> Deserialize<'de> for ImageMediaType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_mime(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("unknown image media type '{s}'")))
    }
}

#[derive(Clone)]
pub struct ImageSource {
    pub media_type: ImageMediaType,
    pub data: Arc<str>,
    /// What [`adapt_images_for_model`] has to do with `data` before a provider
    /// will take it, decided at most once. It describes the payload, so it
    /// rides with it: every clone shares one verdict, and it dies with the
    /// pixels instead of outliving them in a side table.
    pub(crate) verdict: Arc<OnceLock<Fix>>,
}

/// The payload is the largest string a session holds, and a verdict can carry
/// a second one, so neither belongs in a log line.
impl fmt::Debug for ImageSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageSource")
            .field("media_type", &self.media_type)
            .field("base64_len", &self.data.len())
            .finish()
    }
}

impl PartialEq for ImageSource {
    fn eq(&self, other: &Self) -> bool {
        self.media_type == other.media_type
            && (Arc::ptr_eq(&self.data, &other.data) || self.data == other.data)
    }
}

impl Eq for ImageSource {}

impl<'de> Deserialize<'de> for ImageSource {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Wire {
            media_type: ImageMediaType,
            data: String,
        }
        let wire = Wire::deserialize(deserializer)?;
        // Base64 image payloads are the largest strings a session holds, and a
        // load decodes each one once per record that carries it.
        Ok(Self::new(
            wire.media_type,
            maki_storage::intern::shared_str(wire.data),
        ))
    }
}

impl Serialize for ImageSource {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ImageSource", 3)?;
        state.serialize_field("type", "base64")?;
        state.serialize_field("media_type", &self.media_type)?;
        state.serialize_field("data", &self.data)?;
        state.end()
    }
}

impl ImageSource {
    pub fn new(media_type: ImageMediaType, data: Arc<str>) -> Self {
        Self {
            media_type,
            data,
            verdict: Arc::default(),
        }
    }

    pub fn to_data_url(&self) -> String {
        format!("data:{};base64,{}", self.media_type.mime(), self.data)
    }
}

pub const IMAGE_OMITTED_NOTE: &str =
    "[image omitted: the current model does not support image input]";
/// A broken payload has to leave the request, or every turn after it fails.
pub const IMAGE_UNUSABLE_NOTE: &str = "[image omitted: the image could not be decoded]";
/// Past [`MAX_IMAGES`] the request itself is refused, so the oldest pixels
/// make way rather than taking the whole session down.
pub const IMAGE_EVICTED_NOTE: &str = "[image omitted: too many images in this conversation]";
/// Stands in for the text of a message that carries only images, both in model
/// context and in the transcript. One const so the two can never drift apart.
pub const IMAGE_PLACEHOLDER: &str = "[image]";
/// See [`Message::empty_marker`].
pub const EMPTY_RESPONSE_MARKER: &str = "(empty)";

/// The last stop before the wire for every image in a request, whatever put
/// it there. For models without vision, image blocks become a text note
/// instead of a block the API would reject, and history keeps the pixels, so
/// switching back to a vision-capable model restores them. For the rest,
/// oversized payloads are rewritten to fit provider limits, because one image
/// a provider refuses would otherwise fail every later request in the session
/// too.
pub async fn adapt_images_for_model<'a>(
    model: &Model,
    messages: &'a [Message],
) -> Cow<'a, [Message]> {
    // Newest first: the stale screenshots are the ones a long session can
    // spare once the request runs out of room for them. Collected rather than
    // walked lazily, since a borrow of `messages` held across the await below
    // leaves callers unable to prove their own futures `Send`.
    let images: Vec<(usize, usize, ImageSource)> = messages
        .iter()
        .enumerate()
        .rev()
        .flat_map(|(m, message)| {
            message
                .content
                .iter()
                .enumerate()
                .rev()
                .filter_map(move |(b, block)| match block {
                    ContentBlock::Image { source } => Some((m, b, source.clone())),
                    _ => None,
                })
        })
        .collect();
    let note = |text: &str| ContentBlock::Text { text: text.into() };
    let vision = model.supports_vision();
    let mut edits: Vec<(usize, usize, ContentBlock)> = Vec::new();
    // Counts survivors, not blocks, or an image nobody can read would cost a
    // good one its place. Nothing past the cap is decoded at all.
    let mut kept = 0;
    for (m, b, source) in images {
        if !vision {
            edits.push((m, b, note(IMAGE_OMITTED_NOTE)));
        } else if kept == MAX_IMAGES {
            edits.push((m, b, note(IMAGE_EVICTED_NOTE)));
        } else {
            match fix_for_wire(&source).await {
                Fix::Keep => kept += 1,
                Fix::Replace(source) => {
                    kept += 1;
                    edits.push((m, b, ContentBlock::Image { source }));
                }
                Fix::Drop => edits.push((m, b, note(IMAGE_UNUSABLE_NOTE))),
            }
        }
    }
    if edits.is_empty() {
        return Cow::Borrowed(messages);
    }
    // A settled image hands its cached verdict back, so once anything here
    // needed repairing `edits` is non-empty on every later turn too. The owned
    // slice a provider takes has to carry every message, so the untouched ones
    // are still cloned, but only a message an edit lands on is rebuilt, and it
    // is rebuilt out of the blocks it keeps rather than cloned whole and then
    // written over. History keeps the originals on purpose: see above.
    // The walk above ran newest-first, so reversing the edits lines them up
    // with a single forward pass.
    let mut edits = edits.into_iter().rev().peekable();
    let mut adapted = Vec::with_capacity(messages.len());
    for (m, message) in messages.iter().enumerate() {
        if edits.peek().is_none_or(|&(edited, ..)| edited != m) {
            adapted.push(message.clone());
            continue;
        }
        let mut content = Vec::with_capacity(message.content.len());
        for (b, block) in message.content.iter().enumerate() {
            match edits.next_if(|&(edited, at, _)| (edited, at) == (m, b)) {
                Some((.., replacement)) => content.push(replacement),
                None => content.push(block.clone()),
            }
        }
        adapted.push(Message {
            role: message.role.clone(),
            content,
            display_text: message.display_text.clone(),
            kind: message.kind,
            observation_source: message.observation_source.clone(),
        });
    }
    Cow::Owned(adapted)
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    #[default]
    User,
    Assistant,
}

impl Role {
    fn is_user(&self) -> bool {
        matches!(self, Self::User)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    RedactedThinking {
        data: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thought_signature: Option<String>,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
    },
    Image {
        source: ImageSource,
    },
}

impl ContentBlock {
    pub fn is_thinking(&self) -> bool {
        matches!(self, Self::Thinking { .. } | Self::RedactedThinking { .. })
    }
}

/// Who a message came from, which `role` cannot say. Providers only
/// accept user and assistant, so anything the host wants to report has to
/// travel as a user message, and without this there is no way to tell it
/// apart from the user actually typing. A prefix in the text would not do:
/// a log line can print one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Someone said this, the user or the model.
    #[default]
    Turn,
    /// The host noticed it and passed it to the model. It stays in session
    /// history for conversation order but is hidden from user-facing views.
    Observation,
}

impl MessageKind {
    fn is_turn(&self) -> bool {
        matches!(self, Self::Turn)
    }
}

impl ContentBlock {
    pub fn tool_use(id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self::ToolUse {
            id: id.into(),
            name: name.into(),
            input,
            thought_signature: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObservationSource {
    ChildResult { agent_id: String, turn_id: String },
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_text: Option<String>,
    /// Skipped when it is `Turn`, so sessions written before this existed
    /// load unchanged.
    #[serde(default, skip_serializing_if = "MessageKind::is_turn")]
    pub kind: MessageKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_source: Option<ObservationSource>,
}

impl Message {
    /// Stands in for an assistant turn with no text, thinking alone or empty,
    /// which providers reject as the trailing message. Never a real response:
    /// readers mining history for model text must skip it.
    pub fn empty_marker() -> Self {
        Self {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: EMPTY_RESPONSE_MARKER.into(),
            }],
            ..Default::default()
        }
    }

    /// Something the host saw, reported to the model without pretending
    /// the user said it.
    pub fn observation(text: String) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentBlock::Text { text }],
            kind: MessageKind::Observation,
            ..Default::default()
        }
    }

    pub fn is_observation(&self) -> bool {
        self.kind == MessageKind::Observation
    }

    pub fn user(text: String) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentBlock::Text { text }],
            ..Default::default()
        }
    }

    pub fn user_display(ai_text: String, display: String) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentBlock::Text { text: ai_text }],
            display_text: Some(display),
            ..Default::default()
        }
    }

    pub fn user_with_images(text: String, images: Vec<ImageSource>) -> Self {
        let mut content: Vec<ContentBlock> = images
            .into_iter()
            .map(|source| ContentBlock::Image { source })
            .collect();
        if !text.is_empty() {
            content.push(ContentBlock::Text { text });
        }
        Self {
            role: Role::User,
            content,
            ..Default::default()
        }
    }

    pub fn synthetic(text: String) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentBlock::Text { text }],
            display_text: Some(String::new()),
            ..Default::default()
        }
    }

    pub fn user_text(&self) -> Option<&str> {
        match &self.display_text {
            Some(t) if t.is_empty() => None,
            Some(t) => Some(t),
            None => self.first_text_content(),
        }
    }

    pub fn first_text_content(&self) -> Option<&str> {
        self.content.iter().find_map(|b| match b {
            ContentBlock::Text { text } if !text.trim().is_empty() => Some(text.as_str()),
            _ => None,
        })
    }

    pub fn tool_uses(&self) -> impl Iterator<Item = (&str, &str, &Value)> {
        self.content.iter().filter_map(|b| match b {
            ContentBlock::ToolUse {
                id, name, input, ..
            } => Some((id.as_str(), name.as_str(), input)),
            _ => None,
        })
    }

    pub fn has_tool_calls(&self) -> bool {
        self.content
            .iter()
            .any(|b| matches!(b, ContentBlock::ToolUse { .. }))
    }
}

impl TitleSource for Message {
    fn first_user_text(&self) -> Option<&str> {
        if !self.role.is_user() || self.is_observation() {
            return None;
        }
        self.user_text()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderEvent {
    TextDelta {
        text: String,
    },
    ThinkingDelta {
        text: String,
    },
    /// The current reasoning block is complete; the next `ThinkingDelta`
    /// belongs to a new block.
    ThinkingBlockEnd,
    ToolUseStart {
        id: String,
        name: String,
    },
    PromptProgress {
        processed: u32,
        total: u32,
        cache: u32,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationPurpose {
    Agent,
    Compaction,
    Isolated,
}

/// Who a stream call belongs to. `(operation_id, attempt)` names one attempt;
/// `conversation_id` names the remote state it may build on.
#[derive(Debug, Clone, Serialize)]
pub struct Invocation {
    pub conversation_id: Arc<str>,
    pub operation_id: Arc<str>,
    pub attempt: u32,
    pub purpose: InvocationPurpose,
    pub agent_id: Option<Arc<str>>,
    /// The conversation this one started from, on every request under it.
    pub previous: Option<ConversationLink>,
}

/// Why the host will never send another request for a conversation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Display)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum ConversationEnd {
    Cancelled,
    Closed,
    Compacted,
    Rewound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
}

pub const THINKING_USAGE: &str =
    "Usage: /thinking [off|adaptive|minimal|low|medium|high|xhigh|max|<budget>]";

pub trait ThinkingConfigExt {
    fn parse(input: &str, current: Self) -> Result<Self, &'static str>
    where
        Self: Sized;
    fn status_label(self) -> Option<Cow<'static, str>>;
    fn enabled(self) -> bool;
}

impl ThinkingConfigExt for ThinkingConfig {
    fn enabled(self) -> bool {
        !matches!(self, Self::Off)
    }

    fn parse(input: &str, current: Self) -> Result<Self, &'static str> {
        if input.is_empty() {
            return Ok(if current.enabled() {
                Self::Off
            } else {
                Self::Adaptive
            });
        }
        ThinkingConfig::parse_setting(input).map_err(|_| THINKING_USAGE)
    }

    fn status_label(self) -> Option<Cow<'static, str>> {
        match self {
            Self::Off => None,
            Self::Adaptive => Some(Cow::Borrowed("thinking")),
            Self::Effort(e) => Some(Cow::Owned(format!("thinking: {e}"))),
            Self::Budget(n) => Some(Cow::Owned(format!("thinking: {n}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct RequestOptions {
    pub thinking: ThinkingConfig,
    /// Raw user preference, reconciled by [`RequestOptions::clamped`] before use.
    pub fast: bool,
}

impl RequestOptions {
    /// Reconciles options with the model's capabilities. Called once before
    /// every request so UI state, restored sessions, and subagent flags all go
    /// through the same gate. Despite the name, thinking clamps both ways:
    /// down to `Off` when unsupported, up to minimal effort when required.
    pub fn clamped(self, model: &crate::model::Model) -> Self {
        Self {
            thinking: if !model.supports_thinking() {
                ThinkingConfig::Off
            } else if model.requires_thinking() && !self.thinking.enabled() {
                ThinkingConfig::Effort(Effort::Minimal)
            } else {
                self.thinking
            },
            fast: self.fast && model.supports_fast(),
        }
    }
}

#[derive(Debug)]
pub struct StreamResponse {
    pub message: Message,
    pub usage: TokenUsage,
    pub stop_reason: Option<StopReason>,
}

/// Provider-reported usage quota, independent of local token accounting. Not every
/// provider exposes a programmatic quota endpoint; check `Provider::fetch_usage`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderUsage {
    /// Subscription/plan level when the provider reports one (e.g. "lite").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    pub limits: Vec<UsageLimit>,
    /// Per-model breakdown of the current UTC day, when the provider exposes
    /// one. The window is part of the contract: the modal labels these rows as
    /// today's, so a provider reporting a month or a lifetime needs its own
    /// field rather than this one. Sorted by the provider (typically spend
    /// desc).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub by_model_today: Vec<ModelUsageRow>,
}

/// One row of a provider-reported per-model usage table. Money is kept as
/// integer micro-dollars to keep `ProviderUsage` in `Eq` territory for tests;
/// callers format at the UI layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsageRow {
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    /// Spend in micro-dollars (1_000_000 = $1.00).
    pub spend_microdollars: u64,
}

/// The kind of quota window a limit refers to, normalized across providers so
/// the UI renders one vocabulary instead of each provider's native wording.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UsageWindow {
    /// A rolling window of N hours (e.g. the 5-hour request lane).
    Hours(u64),
    /// A window bounded by whole days (daily or an N-day ticket).
    Days(u64),
    /// A billing-month window, resetting on the subscription renewal date.
    Monthly,
    /// The weekly/7-day window, optionally scoped to a model/tier.
    Weekly { model: Option<String> },
    /// A monetary credit / usage allowance.
    Credits,
    /// A subscription reset period, not a token quantity.
    Subscription,
    /// A kind with no shared semantics; renders verbatim provider text.
    Other(String),
}

/// A single quota window (e.g. a 5-hour or weekly token quota).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageLimit {
    /// Which quota window/quota kind this limit describes.
    pub kind: UsageWindow,
    /// Usage percentage within the window, 0-100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentage: Option<u32>,
    /// When the window resets, as epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<u64>,
    /// Extra provider-supplied context, e.g. "$2.33 spent" for usage credits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::*;
    use crate::model::ThinkingSupport as Support;
    use serde_json::json;
    use test_case::test_case;

    #[test]
    fn user_with_images_text_and_images() {
        let source = ImageSource::new(ImageMediaType::Png, Arc::from("abc123"));
        let msg = Message::user_with_images("hello".into(), vec![source]);
        assert_eq!(msg.content.len(), 2);
        assert!(matches!(&msg.content[0], ContentBlock::Image { .. }));
        assert!(matches!(&msg.content[1], ContentBlock::Text { text } if text == "hello"));
    }

    #[test]
    fn user_with_images_empty_text_only_images() {
        let source = ImageSource::new(ImageMediaType::Png, Arc::from("abc123"));
        let msg = Message::user_with_images(String::new(), vec![source]);
        assert_eq!(msg.content.len(), 1);
        assert!(matches!(&msg.content[0], ContentBlock::Image { .. }));
    }

    #[test]
    fn message_kind_is_backward_compatible() {
        let old: Message = serde_json::from_value(json!({
            "role": "user",
            "content": [{ "type": "text", "text": "hello" }]
        }))
        .unwrap();
        assert_eq!(old.kind, MessageKind::Turn);

        let turn = serde_json::to_value(Message::user("hello".into())).unwrap();
        assert!(turn.get("kind").is_none());

        let observation = Message::observation("built".into());
        assert_eq!(observation.first_user_text(), None);
        let observation = serde_json::to_value(observation).unwrap();
        assert_eq!(observation["kind"], "observation");
    }

    #[test_case(ImageMediaType::Png,  "image/png"  ; "png")]
    #[test_case(ImageMediaType::Jpeg, "image/jpeg" ; "jpeg")]
    #[test_case(ImageMediaType::Gif,  "image/gif"  ; "gif")]
    #[test_case(ImageMediaType::Webp, "image/webp" ; "webp")]
    fn image_source_data_url(media: ImageMediaType, mime: &str) {
        let source = ImageSource::new(media, Arc::from("dGVzdA=="));
        assert_eq!(source.to_data_url(), format!("data:{mime};base64,dGVzdA=="));
    }

    #[test_case("image/png",  Some(ImageMediaType::Png)  ; "png")]
    #[test_case("image/webp", Some(ImageMediaType::Webp) ; "webp")]
    #[test_case("image/bmp",  None                       ; "unsupported")]
    fn media_type_from_mime(mime: &str, expected: Option<ImageMediaType>) {
        assert_eq!(ImageMediaType::from_mime(mime), expected);
    }

    fn png_block(edge: usize) -> ContentBlock {
        let data = crate::image::png_base64(edge as u32, edge as u32);
        ContentBlock::Image {
            source: ImageSource::new(ImageMediaType::Png, Arc::from(data)),
        }
    }

    fn unreadable_block() -> ContentBlock {
        ContentBlock::Image {
            source: ImageSource::new(ImageMediaType::Png, Arc::from("abc123")),
        }
    }

    fn adapt(model: &Model, content: Vec<ContentBlock>) -> Vec<ContentBlock> {
        let messages = vec![Message {
            role: Role::User,
            content,
            ..Default::default()
        }];
        smol::block_on(adapt_images_for_model(model, &messages))[0]
            .content
            .clone()
    }

    fn image_count(blocks: &[ContentBlock]) -> usize {
        blocks
            .iter()
            .filter(|b| matches!(b, ContentBlock::Image { .. }))
            .count()
    }

    #[test]
    fn adapt_images_borrows_when_nothing_has_to_change() {
        let model = test_model();
        let with_image = vec![Message {
            role: Role::User,
            content: vec![png_block(32)],
            ..Default::default()
        }];
        assert!(matches!(
            smol::block_on(adapt_images_for_model(&model, &with_image)),
            Cow::Borrowed(_)
        ));

        let mut text_only_model = model;
        text_only_model.vision = Some(false);
        let no_images = vec![Message::user("hi".into())];
        assert!(matches!(
            smol::block_on(adapt_images_for_model(&text_only_model, &no_images)),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn adapt_images_rebuilds_only_the_messages_that_change() {
        const CAPTION: &str = "look";
        const OVERSIZED: u32 = 2600;
        let model = test_model();
        let fine = ImageSource::new(
            ImageMediaType::Png,
            Arc::from(crate::image::png_base64(32, 32)),
        );
        let carries = |content: Vec<ContentBlock>| Message {
            role: Role::User,
            content,
            ..Default::default()
        };
        let history = vec![
            Message::user(CAPTION.into()),
            carries(vec![ContentBlock::Image {
                source: fine.clone(),
            }]),
            carries(vec![
                ContentBlock::Text {
                    text: CAPTION.into(),
                },
                ContentBlock::Image {
                    source: ImageSource::new(
                        ImageMediaType::Png,
                        Arc::from(crate::image::png_base64(OVERSIZED, 30)),
                    ),
                },
            ]),
        ];

        let adapted = smol::block_on(adapt_images_for_model(&model, &history));
        assert!(matches!(adapted, Cow::Owned(_)), "the repair needs a copy");
        assert_eq!(adapted[0].first_user_text(), Some(CAPTION));
        let ContentBlock::Image { source } = &adapted[1].content[0] else {
            panic!("an image nothing is wrong with must stay an image");
        };
        assert!(
            Arc::ptr_eq(&source.data, &fine.data),
            "an untouched image must not be re-encoded"
        );
        assert!(
            matches!(&adapted[2].content[0], ContentBlock::Text { text } if text == CAPTION),
            "the blocks beside a repaired one must survive"
        );
        let ContentBlock::Image { source } = &history[2].content[1] else {
            panic!("history must keep its image block");
        };
        assert_eq!(
            crate::image::dimensions(source),
            (OVERSIZED, 30),
            "only the copy going out is rewritten"
        );
    }

    #[test]
    fn adapt_images_shrinks_what_a_provider_would_refuse() {
        let model = test_model();
        let oversized = ContentBlock::Image {
            source: ImageSource::new(
                ImageMediaType::Png,
                Arc::from(crate::image::png_base64(2600, 30)),
            ),
        };
        let text = ContentBlock::Text {
            text: "look".into(),
        };
        let blocks = adapt(&model, vec![text, oversized]);
        assert!(matches!(&blocks[0], ContentBlock::Text { .. }));
        let ContentBlock::Image { source } = &blocks[1] else {
            panic!("an image block must stay an image block");
        };
        let (width, height) = crate::image::dimensions(source);
        assert!(
            width.max(height) <= crate::image::MAX_EDGE,
            "{width}x{height}"
        );
    }

    #[test]
    fn adapt_images_evicts_the_oldest_past_the_request_cap() {
        const EXTRA: usize = 3;
        let model = test_model();
        let blocks = adapt(&model, (1..=MAX_IMAGES + EXTRA).map(png_block).collect());
        assert_eq!(image_count(&blocks), MAX_IMAGES);
        assert!(
            matches!(&blocks[EXTRA - 1], ContentBlock::Text { text } if text == IMAGE_EVICTED_NOTE),
            "the oldest images are the ones that make way"
        );
        assert!(matches!(&blocks[EXTRA], ContentBlock::Image { .. }));
    }

    #[test]
    fn adapt_images_drops_what_it_cannot_read_without_spending_the_cap() {
        let model = test_model();
        let mut content = vec![png_block(1), unreadable_block()];
        content.extend((2..=MAX_IMAGES).map(png_block));
        let blocks = adapt(&model, content);
        assert_eq!(image_count(&blocks), MAX_IMAGES);
        assert!(matches!(&blocks[1], ContentBlock::Text { text } if text == IMAGE_UNUSABLE_NOTE));
    }

    #[test]
    fn adapt_images_replaces_blocks_for_text_only_model() {
        let mut model = test_model();
        model.vision = Some(false);
        let tool_result = ContentBlock::ToolResult {
            tool_use_id: "t1".into(),
            content: "[image: pic.png 1KB]".into(),
            is_error: false,
        };
        let blocks = adapt(&model, vec![tool_result, unreadable_block()]);
        assert_eq!(blocks.len(), 2);
        assert!(matches!(&blocks[0], ContentBlock::ToolResult { .. }));
        assert!(matches!(&blocks[1], ContentBlock::Text { text } if text == IMAGE_OMITTED_NOTE));
    }

    #[test]
    fn image_source_serde_injects_type_base64() {
        let source = ImageSource::new(ImageMediaType::Png, Arc::from("abc123"));
        let json = serde_json::to_value(&source).unwrap();
        assert_eq!(json["type"], "base64");
        assert_eq!(json["media_type"], "image/png");
        assert_eq!(json["data"], "abc123");
        let deserialized: ImageSource = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.media_type, ImageMediaType::Png);
        assert_eq!(&*deserialized.data, "abc123");
    }

    /// The payloads a session repeats across records collapse to one
    /// allocation, and only while a load is in flight.
    #[test]
    fn image_source_deserialize_shares_identical_payloads_within_a_load() {
        const INTERNED_DATA: &str = "aW50ZXJuZWQtcGF5bG9hZA==";
        const OTHER_DATA: &str = "b3RoZXItcGF5bG9hZA==";
        let wire = |data: &str| json!({"media_type": "image/png", "data": data}).to_string();
        let read = |data: &str| serde_json::from_str::<ImageSource>(&wire(data)).unwrap();

        let (first, other) = {
            let _scope = maki_storage::intern::Scope::enter();
            let first = read(INTERNED_DATA);
            assert!(Arc::ptr_eq(&first.data, &read(INTERNED_DATA).data));
            (first, read(OTHER_DATA))
        };

        assert!(!Arc::ptr_eq(&first.data, &other.data));
        assert_eq!(&*first.data, INTERNED_DATA);
        assert!(!Arc::ptr_eq(&first.data, &read(INTERNED_DATA).data));
    }

    use Effort::{High, Max, Minimal};

    fn test_model() -> Model {
        Model {
            vision: Some(true),
            ..Model::from_spec("test/test-model").unwrap()
        }
    }

    #[test_case(None,                    ThinkingConfig::Adaptive, ThinkingConfig::Off             ; "undeclared_clamps_off")]
    #[test_case(Some(Support::No),       ThinkingConfig::Adaptive, ThinkingConfig::Off             ; "unsupported_clamps_off")]
    #[test_case(Some(Support::Yes),      ThinkingConfig::Off,      ThinkingConfig::Off             ; "supported_keeps_off")]
    #[test_case(Some(Support::Required), ThinkingConfig::Off,      ThinkingConfig::Effort(Minimal) ; "required_raises_off_to_minimal")]
    #[test_case(Some(Support::Required), ThinkingConfig::Adaptive, ThinkingConfig::Adaptive        ; "required_keeps_enabled")]
    fn request_options_clamped_thinking(
        thinking_support: Option<Support>,
        thinking: ThinkingConfig,
        expected: ThinkingConfig,
    ) {
        let mut model = test_model();
        model.thinking_support = thinking_support;
        let opts = RequestOptions {
            thinking,
            fast: false,
        };
        assert_eq!(opts.clamped(&model).thinking, expected);
    }

    #[test]
    fn request_options_clamped_fast_requires_model_support() {
        let model = Model {
            fast_support: None,
            ..test_model()
        };
        let opts = RequestOptions {
            thinking: ThinkingConfig::Off,
            fast: true,
        };
        assert!(!opts.clamped(&model).fast);
    }

    #[test_case("",         ThinkingConfig::Off,      Ok(ThinkingConfig::Adaptive)  ; "toggle_on")]
    #[test_case("",         ThinkingConfig::Adaptive, Ok(ThinkingConfig::Off)       ; "toggle_off")]
    #[test_case("off",      ThinkingConfig::Adaptive, Ok(ThinkingConfig::Off)       ; "explicit_off")]
    #[test_case("adaptive", ThinkingConfig::Off,      Ok(ThinkingConfig::Adaptive)  ; "explicit_adaptive")]
    #[test_case("high",     ThinkingConfig::Off,      Ok(ThinkingConfig::Effort(High)) ; "explicit_effort")]
    #[test_case("8192",     ThinkingConfig::Off,      Ok(ThinkingConfig::Budget(8192)) ; "explicit_budget")]
    #[test_case("512",      ThinkingConfig::Off,      Ok(ThinkingConfig::Budget(512)) ; "small_budget")]
    #[test_case("0",        ThinkingConfig::Off,      Err(())                       ; "budget_zero")]
    #[test_case("garbage",  ThinkingConfig::Off,      Err(())                       ; "invalid_input")]
    fn thinking_parse(input: &str, current: ThinkingConfig, expected: Result<ThinkingConfig, ()>) {
        let result = ThinkingConfig::parse(input, current).map_err(|_| ());
        assert_eq!(result, expected);
    }

    #[test_case(ThinkingConfig::Off      ; "off")]
    #[test_case(ThinkingConfig::Adaptive ; "adaptive")]
    #[test_case(ThinkingConfig::Effort(Max) ; "effort")]
    #[test_case(ThinkingConfig::Budget(8192) ; "budget")]
    fn thinking_display_round_trip(config: ThinkingConfig) {
        let s = config.to_string();
        let parsed = ThinkingConfig::parse(&s, ThinkingConfig::Off).unwrap();
        assert_eq!(parsed, config);
    }

    #[test]
    fn thinking_serde_no_signature_omits_field() {
        let block = ContentBlock::Thinking {
            thinking: "x".into(),
            signature: None,
        };
        let json = serde_json::to_value(&block).unwrap();
        assert!(json.get("signature").is_none());
    }
}
