use std::env;

use maki_config::AgentConfig;
use maki_providers::{
    ContentBlock, IMAGE_PLACEHOLDER, Message, Model, RequestOptions, Role, StreamResponse,
    TokenUsage,
};
use maki_storage::id::SessionRef;
use tracing::info;

use super::history::{History, remove_orphaned_tool_results};
use super::streaming::{StreamError, stream_with_retry};
use crate::cancel::CancelToken;
use crate::{AgentError, AgentEvent, EventSender, TurnCompleteEvent};

const CONTINUE_AFTER_COMPACT: &str = "Continue if you have next steps, or stop and ask for clarification if you are unsure how to proceed. If the summary contains a todo list, restore it with todo_write and keep it updated. If you learned important project context during this session, consider saving it to memory before it's lost.";

const MAX_RESERVED_PERCENT: u32 = 50;

fn percent_of(tokens: u32, percent: u32) -> u32 {
    (u64::from(tokens) * u64::from(percent) / 100) as u32
}

fn normalize(text: Option<&str>) -> Option<&str> {
    text.map(str::trim).filter(|t| !t.is_empty())
}

/// Config instructions steer every compaction, `request` only the one the user
/// asked for with `/compact <guidance>`, so both are kept and neither wins.
fn summary_prompt(config: &AgentConfig, request: Option<&str>) -> String {
    let extras = [
        normalize(config.compaction_instructions.as_deref()),
        normalize(request),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n");
    if extras.is_empty() {
        return crate::prompt::COMPACTION_USER.to_string();
    }
    format!(
        "{}\n\nAdditional instructions:\n{extras}",
        crate::prompt::COMPACTION_USER
    )
}

pub(super) fn continue_message(config: &AgentConfig) -> String {
    match normalize(config.post_compaction_instructions.as_deref()) {
        Some(extra) => format!("{CONTINUE_AFTER_COMPACT}\n\n{extra}"),
        None => CONTINUE_AFTER_COMPACT.to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn compact_history(
    provider: &dyn maki_providers::provider::Provider,
    model: &Model,
    history: &mut History,
    event_tx: &EventSender,
    cancel: &CancelToken,
    config: &AgentConfig,
    instructions: Option<&str>,
    session_id: Option<&SessionRef>,
) -> Result<TokenUsage, AgentError> {
    let compact_start = std::time::Instant::now();
    let mut compaction_history: Vec<Message> = history.as_slice().to_vec();
    remove_orphaned_tool_results(&mut compaction_history);
    strip_images(&mut compaction_history);
    strip_thinking(&mut compaction_history);
    strip_old_tool_results(&mut compaction_history);
    compaction_history.push(Message::user(summary_prompt(config, instructions)));

    let empty_tools = serde_json::json!([]);
    let max_attempts = 3;
    let mut last_error = None;

    for attempt in 0..max_attempts {
        match stream_with_retry(
            provider,
            model,
            &compaction_history,
            crate::prompt::COMPACTION_SYSTEM,
            &empty_tools,
            event_tx,
            cancel,
            RequestOptions::default(),
            session_id,
            maki_providers::retry::RetryPolicy::default(),
        )
        .await
        {
            Ok(response) => {
                if attempt > 0 {
                    info!(
                        attempt,
                        "compaction succeeded after truncating oldest rounds"
                    );
                }
                return finish_compact(response, history, event_tx, compact_start, model);
            }
            Err(StreamError::Other(e)) if e.is_context_overflow() && attempt < max_attempts - 1 => {
                last_error = Some(e);
                truncate_oldest_round(&mut compaction_history);
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err(last_error.unwrap())
}

fn finish_compact(
    response: StreamResponse,
    history: &mut History,
    event_tx: &EventSender,
    compact_start: std::time::Instant,
    model: &Model,
) -> Result<TokenUsage, AgentError> {
    let _ = event_tx.send(AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
        message: response.message.clone(),
        usage: response.usage,
        model: model.id.clone(),
        cost: model.billed_cost(&response.usage, false),
        context_size: Some(response.usage.output),
        context_window: model.context_window,
    })));

    // Swapping the history for a summary the model never wrote would throw the
    // session away for nothing.
    if response.message.first_text_content().is_none() {
        return Err(AgentError::EmptySummary);
    }

    let new_history = vec![
        Message::user("What did we do so far?".into()),
        response.message,
    ];
    history.replace(new_history);
    info!(
        model = %model.id,
        duration_ms = compact_start.elapsed().as_millis() as u64,
        "compaction completed"
    );

    Ok(response.usage)
}

#[allow(clippy::too_many_arguments)]
pub async fn compact(
    provider: &dyn maki_providers::provider::Provider,
    model: &Model,
    history: &mut History,
    event_tx: &EventSender,
    cancel: &CancelToken,
    config: &AgentConfig,
    instructions: Option<&str>,
    session_id: Option<&SessionRef>,
) -> Result<(), AgentError> {
    let usage = compact_history(
        provider,
        model,
        history,
        event_tx,
        cancel,
        config,
        instructions,
        session_id,
    )
    .await?;
    if let Some(post) = normalize(config.post_compaction_instructions.as_deref()) {
        history.push(Message::synthetic(post.to_string()));
    }

    event_tx.send(AgentEvent::ControlComplete { usage })?;

    Ok(())
}

const MIN_OUTPUT_TOKENS: u32 = 4096;

pub(super) fn min_output(model: &Model) -> u32 {
    MIN_OUTPUT_TOKENS.min(model.max_output_tokens.unwrap_or(MIN_OUTPUT_TOKENS))
}

/// Reserving a whole [`AgentConfig::max_turn_output`] would guarantee more and
/// cost more: on a small window that is half the context, and compacting that
/// early hurts worse than the odd turn whose output budget gets trimmed.
///
/// Whatever the floor and the buffer work out to, [`MAX_RESERVED_PERCENT`] has
/// the last word, because a reservation that eats the window leaves compaction
/// nothing to compact into.
pub(super) fn reserved(model: &Model, config: &AgentConfig) -> u32 {
    config
        .compaction_buffer
        .resolve(model.context_window)
        .max(min_output(model))
        .min(percent_of(model.context_window, MAX_RESERVED_PERCENT))
}

/// What [`reserved`] leaves the transcript. Always a real number of tokens, so
/// `>=` against it is a threshold a session can sit below.
pub(super) fn usable(model: &Model, config: &AgentConfig) -> u32 {
    model.context_window - reserved(model, config)
}

pub(super) fn is_overflow(usage: &TokenUsage, model: &Model, config: &AgentConfig) -> bool {
    usage.context_tokens() >= usable(model, config)
}

fn strip_images(messages: &mut [Message]) {
    for msg in messages {
        for block in &mut msg.content {
            if matches!(block, ContentBlock::Image { .. }) {
                *block = ContentBlock::Text {
                    text: IMAGE_PLACEHOLDER.into(),
                };
            }
        }
    }
}

fn strip_thinking(messages: &mut [Message]) {
    for msg in messages {
        msg.content.retain(|block| !block.is_thinking());
    }
}

const TOOL_RESULT_PLACEHOLDER: &str = "[tool result]";
const KEEP_LAST_TOOL_RESULTS: usize = 3;

fn strip_old_tool_results(messages: &mut [Message]) {
    let total: usize = messages
        .iter()
        .flat_map(|m| &m.content)
        .filter(|b| matches!(b, ContentBlock::ToolResult { .. }))
        .count();

    let mut seen = 0;
    for msg in messages {
        for block in &mut msg.content {
            if let ContentBlock::ToolResult { content, .. } = block {
                if seen < total.saturating_sub(KEEP_LAST_TOOL_RESULTS) {
                    *content = TOOL_RESULT_PLACEHOLDER.into();
                }
                seen += 1;
            }
        }
    }
}

fn truncate_oldest_round(messages: &mut Vec<Message>) {
    if messages.len() <= 1 {
        return;
    }

    let removed_user = matches!(messages.remove(0).role, Role::User);
    if removed_user
        && messages.len() > 1
        && matches!(
            messages.first().map(|message| &message.role),
            Some(Role::Assistant)
        )
    {
        messages.remove(0);
    }
    remove_orphaned_tool_results(messages);

    while messages.len() > 1
        && matches!(
            messages.first().map(|message| &message.role),
            Some(Role::Assistant)
        )
    {
        messages.remove(0);
        remove_orphaned_tool_results(messages);
    }
}

pub(super) fn auto_compact_enabled() -> bool {
    env::var("MAKIMA_DISABLE_AUTOCOMPACT")
        .map(|v| v != "1" && v != "true")
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use maki_providers::provider::{BoxFuture, Provider};
    use maki_providers::{
        ContentBlock, Message, Model, ProviderEvent, RequestOptions, Role, StopReason,
        StreamResponse, TokenUsage,
    };
    use maki_storage::id::SessionRef;
    use serde_json::Value;
    use test_case::test_case;

    use super::*;
    use crate::AgentConfig;
    use maki_config::CompactionBuffer;

    struct MockProvider {
        responses: Mutex<Vec<Result<StreamResponse, AgentError>>>,
        requests: Mutex<Vec<Vec<Message>>>,
        sessions: Mutex<Vec<Option<SessionRef>>>,
    }

    impl MockProvider {
        fn new(responses: Vec<Result<StreamResponse, AgentError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                requests: Mutex::new(Vec::new()),
                sessions: Mutex::new(Vec::new()),
            }
        }
    }

    impl Provider for MockProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a Value,
            _: &'a flume::Sender<ProviderEvent>,
            _: RequestOptions,
            session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(messages.to_vec());
                self.sessions.lock().unwrap().push(session_id.cloned());
                let mut responses = self.responses.lock().unwrap();
                assert!(!responses.is_empty(), "MockProvider: no more responses");
                responses.remove(0)
            })
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<maki_providers::ModelInfo>, AgentError>> {
            Box::pin(async { unimplemented!() })
        }
    }

    fn default_model() -> Model {
        Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap()
    }

    fn small_context_model(context_window: u32) -> Model {
        let mut model = default_model();
        model.context_window = context_window;
        model
    }

    fn text_response(stop_reason: StopReason) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "response".into(),
                }],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(stop_reason),
        }
    }

    #[test]
    fn compact_forwards_session_to_provider() {
        smol::block_on(async {
            let provider = MockProvider::new(vec![Ok(text_response(StopReason::EndTurn))]);
            let session = SessionRef::generate();
            let mut history = History::new(vec![Message::user("work".into())]);
            let (raw_tx, _rx) = flume::unbounded();

            compact(
                &provider,
                &default_model(),
                &mut history,
                &EventSender::new(raw_tx, 0),
                &CancelToken::none(),
                &AgentConfig::default(),
                None,
                Some(&session),
            )
            .await
            .unwrap();

            assert_eq!(
                provider.sessions.lock().unwrap().as_slice(),
                &[Some(session)]
            );
        });
    }

    #[test]
    fn compact_replaces_history_with_summary() {
        smol::block_on(async {
            let provider: std::sync::Arc<dyn Provider> = std::sync::Arc::new(MockProvider::new(
                vec![Ok(text_response(StopReason::EndTurn))],
            ));
            let model = default_model();
            let (raw_tx, _rx) = flume::unbounded();
            let mut history = History::new(vec![
                Message::user("first".into()),
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: "reply".into(),
                    }],
                    ..Default::default()
                },
            ]);

            compact(
                &*provider,
                &model,
                &mut history,
                &EventSender::new(raw_tx, 0),
                &CancelToken::none(),
                &AgentConfig::default(),
                None,
                None,
            )
            .await
            .unwrap();

            let msgs = history.as_slice();
            assert_eq!(msgs.len(), 2);
            assert!(matches!(msgs[0].role, Role::User));
            assert!(matches!(msgs[1].role, Role::Assistant));
        });
    }

    #[test_case(vec![] ; "no_content")]
    #[test_case(vec![ContentBlock::Text { text: " \n".into() }] ; "blank_text")]
    fn compact_keeps_history_when_summary_has_no_text(content: Vec<ContentBlock>) {
        smol::block_on(async {
            let provider = MockProvider::new(vec![Ok(StreamResponse {
                message: Message {
                    role: Role::Assistant,
                    content,
                    ..Default::default()
                },
                usage: TokenUsage::default(),
                stop_reason: Some(StopReason::EndTurn),
            })]);
            const KEPT: &str = "first";
            let mut history = History::new(vec![Message::user(KEPT.into())]);
            let (raw_tx, _rx) = flume::unbounded();

            let err = compact(
                &provider,
                &default_model(),
                &mut history,
                &EventSender::new(raw_tx, 0),
                &CancelToken::none(),
                &AgentConfig::default(),
                None,
                None,
            )
            .await
            .expect_err("empty summary must fail");

            assert!(matches!(err, AgentError::EmptySummary));
            assert_eq!(history.len(), 1);
            assert_eq!(history.as_slice()[0].user_text(), Some(KEPT));
        });
    }

    const CONFIG_EXTRA: &str = "Record anything that belongs in plan.md";
    const REQUEST_EXTRA: &str = "Keep the failing test names";
    const POST: &str = "Re-read plan.md and agent.md";

    /// `summary_prompt_merges_instructions` covers the merge, this one the
    /// wiring around it.
    #[test]
    fn compact_sends_instructions_and_appends_post() {
        smol::block_on(async {
            let provider = MockProvider::new(vec![Ok(text_response(StopReason::EndTurn))]);
            let mut history = History::new(vec![Message::user("work".into())]);
            let (raw_tx, _rx) = flume::unbounded();
            let config = AgentConfig {
                compaction_instructions: Some(CONFIG_EXTRA.into()),
                post_compaction_instructions: Some(POST.into()),
                ..Default::default()
            };

            compact(
                &provider,
                &default_model(),
                &mut history,
                &EventSender::new(raw_tx, 0),
                &CancelToken::none(),
                &config,
                Some(REQUEST_EXTRA),
                None,
            )
            .await
            .unwrap();

            let requests = provider.requests.lock().unwrap();
            assert!(matches!(
                &requests[0].last().unwrap().content[0],
                ContentBlock::Text { text }
                    if text.contains(CONFIG_EXTRA) && text.contains(REQUEST_EXTRA)
            ));
            assert!(matches!(
                &history.as_slice().last().unwrap().content[0],
                ContentBlock::Text { text } if text == POST
            ));
        });
    }

    #[test_case(None, None, false, false ; "no_instructions")]
    #[test_case(Some(CONFIG_EXTRA), None, true, false ; "config_only")]
    #[test_case(None, Some(REQUEST_EXTRA), false, true ; "request_only")]
    #[test_case(Some(CONFIG_EXTRA), Some(REQUEST_EXTRA), true, true ; "both_kept")]
    #[test_case(Some(CONFIG_EXTRA), Some("   "), true, false ; "blank_request_ignored")]
    #[test_case(Some(" \n "), Some(REQUEST_EXTRA), false, true ; "blank_config_ignored")]
    fn summary_prompt_merges_instructions(
        config_extra: Option<&str>,
        request: Option<&str>,
        has_config: bool,
        has_request: bool,
    ) {
        let config = AgentConfig {
            compaction_instructions: config_extra.map(str::to_string),
            ..Default::default()
        };
        let prompt = summary_prompt(&config, request);

        assert!(prompt.starts_with(crate::prompt::COMPACTION_USER));
        assert_eq!(
            prompt.len() > crate::prompt::COMPACTION_USER.len(),
            has_config || has_request
        );
        assert_eq!(prompt.contains(CONFIG_EXTRA), has_config);
        assert_eq!(prompt.contains(REQUEST_EXTRA), has_request);
    }

    #[test]
    fn compact_preparation_removes_orphan_result_and_tool_image() {
        use std::sync::Arc;

        use maki_providers::{ImageMediaType, ImageSource};

        smol::block_on(async {
            let provider = MockProvider::new(vec![Ok(text_response(StopReason::EndTurn))]);
            let image = ContentBlock::Image {
                source: ImageSource::new(ImageMediaType::Png, Arc::from("aGVsbG8=")),
            };
            let mut orphan = Message {
                role: Role::User,
                content: vec![tool_result("orphan"), image.clone()],
                ..Default::default()
            };
            orphan.content.push(ContentBlock::Text {
                text: "keep text".into(),
            });
            let chat_image = Message {
                role: Role::User,
                content: vec![image],
                ..Default::default()
            };
            let mut history = History::new(vec![orphan, chat_image]);
            let (raw_tx, _rx) = flume::unbounded();

            compact_history(
                &provider,
                &default_model(),
                &mut history,
                &EventSender::new(raw_tx, 0),
                &CancelToken::none(),
                &AgentConfig::default(),
                None,
                None,
            )
            .await
            .unwrap();

            let requests = provider.requests.lock().unwrap();
            let request = &requests[0];
            assert!(
                !request
                    .iter()
                    .flat_map(|message| &message.content)
                    .any(|block| matches!(
                        block,
                        ContentBlock::ToolResult { .. } | ContentBlock::Image { .. }
                    ))
            );
            assert!(
                request.iter().flat_map(|message| &message.content).any(
                    |block| matches!(block, ContentBlock::Text { text } if text == "keep text")
                )
            );
            assert!(request.iter().flat_map(|message| &message.content).any(
                |block| matches!(block, ContentBlock::Text { text } if text == IMAGE_PLACEHOLDER)
            ));
        });
    }

    #[test_case(159_999, 0,       0,       0,      200_000, false ; "below_threshold")]
    #[test_case(160_000, 0,       0,       0,      200_000, true  ; "at_threshold")]
    #[test_case(100,     0,       0,       0,      100,     true  ; "tiny_context_window")]
    #[test_case(5_000,   165_000, 10_000,  0,      200_000, true  ; "cached_tokens_count_toward_overflow")]
    #[test_case(100_000, 0,       0,       80_000, 200_000, true  ; "output_tokens_count_toward_overflow")]
    #[test_case(262_144, 0,       0,       0,      262_144, true  ; "equal_context_and_max_output")]
    #[test_case(51_199,  0,       0,       0,      64_000,  false ; "small_window_below_scaled_threshold")]
    #[test_case(51_200,  0,       0,       0,      64_000,  true  ; "small_window_at_scaled_threshold")]
    #[test_case(2_047,   0,       0,       0,      4_096,   false ; "llama_cpp_default_window_is_usable")]
    #[test_case(2_048,   0,       0,       0,      4_096,   true  ; "llama_cpp_default_window_still_compacts")]
    #[test_case(0,       0,       0,       0,      1_024,   false ; "an_empty_transcript_never_overflows")]
    fn overflow_detection(
        input: u32,
        cache_read: u32,
        cache_creation: u32,
        output: u32,
        ctx_window: u32,
        expected: bool,
    ) {
        let model = small_context_model(ctx_window);
        let usage = TokenUsage {
            input,
            output,
            cache_read,
            cache_creation,
        };
        assert_eq!(
            is_overflow(&usage, &model, &AgentConfig::default()),
            expected
        );
    }

    #[test_case(CompactionBuffer::Tokens(10_000), 53_999, false ; "explicit_tokens_below")]
    #[test_case(CompactionBuffer::Tokens(10_000), 54_000, true  ; "explicit_tokens_honored")]
    #[test_case(CompactionBuffer::Percent(50),    32_000, true  ; "explicit_percent_at_threshold")]
    fn overflow_with_explicit_buffer(buffer: CompactionBuffer, input: u32, expected: bool) {
        let model = small_context_model(64_000);
        let usage = TokenUsage {
            input,
            ..Default::default()
        };
        let config = AgentConfig {
            compaction_buffer: buffer,
            ..Default::default()
        };
        assert_eq!(is_overflow(&usage, &model, &config), expected);
    }

    #[test]
    fn strip_images_replaces_with_placeholder() {
        use maki_providers::{ImageMediaType, ImageSource};
        use std::sync::Arc;
        let source = ImageSource::new(ImageMediaType::Png, Arc::from("abc"));
        let mut messages = vec![Message::user_with_images("hello".into(), vec![source])];
        strip_images(&mut messages);
        assert_eq!(messages[0].content.len(), 2);
        assert!(
            matches!(&messages[0].content[0], ContentBlock::Text { text } if text == IMAGE_PLACEHOLDER)
        );
        assert!(matches!(&messages[0].content[1], ContentBlock::Text { text } if text == "hello"));
    }

    #[test]
    fn strip_thinking_removes_thinking_blocks() {
        let mut messages = vec![Message {
            role: Role::Assistant,
            content: vec![
                ContentBlock::Thinking {
                    thinking: "hmm".into(),
                    signature: Some("sig".into()),
                },
                ContentBlock::Text {
                    text: "hello".into(),
                },
                ContentBlock::RedactedThinking {
                    data: "opaque".into(),
                },
            ],
            ..Default::default()
        }];
        strip_thinking(&mut messages);
        assert_eq!(messages[0].content.len(), 1);
        assert!(matches!(&messages[0].content[0], ContentBlock::Text { text } if text == "hello"));
    }

    #[test]
    fn strip_old_tool_results_keeps_newest() {
        let mut messages = vec![Message {
            role: Role::User,
            content: vec![
                ContentBlock::ToolResult {
                    tool_use_id: "t1".into(),
                    content: "old result 1".into(),
                    is_error: false,
                },
                ContentBlock::ToolResult {
                    tool_use_id: "t2".into(),
                    content: "old result 2".into(),
                    is_error: false,
                },
                ContentBlock::ToolResult {
                    tool_use_id: "t3".into(),
                    content: "keep 1".into(),
                    is_error: false,
                },
                ContentBlock::ToolResult {
                    tool_use_id: "t4".into(),
                    content: "keep 2".into(),
                    is_error: false,
                },
                ContentBlock::ToolResult {
                    tool_use_id: "t5".into(),
                    content: "keep 3".into(),
                    is_error: false,
                },
                ContentBlock::Text {
                    text: "keep me".into(),
                },
            ],
            ..Default::default()
        }];
        strip_old_tool_results(&mut messages);
        assert_eq!(messages[0].content.len(), 6);
        assert!(
            matches!(&messages[0].content[0], ContentBlock::ToolResult { content, tool_use_id, .. } if content == TOOL_RESULT_PLACEHOLDER && tool_use_id == "t1")
        );
        assert!(
            matches!(&messages[0].content[1], ContentBlock::ToolResult { content, tool_use_id, .. } if content == TOOL_RESULT_PLACEHOLDER && tool_use_id == "t2")
        );
        assert!(
            matches!(&messages[0].content[2], ContentBlock::ToolResult { content, tool_use_id, .. } if content == "keep 1" && tool_use_id == "t3")
        );
        assert!(
            matches!(&messages[0].content[3], ContentBlock::ToolResult { content, tool_use_id, .. } if content == "keep 2" && tool_use_id == "t4")
        );
        assert!(
            matches!(&messages[0].content[4], ContentBlock::ToolResult { content, tool_use_id, .. } if content == "keep 3" && tool_use_id == "t5")
        );
        assert!(
            matches!(&messages[0].content[5], ContentBlock::Text { text } if text == "keep me")
        );
    }

    fn tool_use(id: &str) -> Message {
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use(id, "bash", serde_json::json!({}))],
            ..Default::default()
        }
    }

    fn tool_result(id: &str) -> ContentBlock {
        ContentBlock::ToolResult {
            tool_use_id: id.into(),
            content: "output".into(),
            is_error: false,
        }
    }

    #[track_caller]
    fn assert_tool_results_have_calls(messages: &[Message]) {
        for (index, message) in messages.iter().enumerate() {
            for block in &message.content {
                let ContentBlock::ToolResult { tool_use_id, .. } = block else {
                    continue;
                };
                assert!(matches!(message.role, Role::User));
                assert!(index > 0);
                assert!(
                    messages[index - 1]
                        .tool_uses()
                        .any(|(id, _, _)| id == tool_use_id)
                );
            }
        }
    }

    #[test]
    fn compact_history_retries_without_reproduced_orphan() {
        smol::block_on(async {
            const TOOL_USE_ID: &str = "call_dMZDTpEfz2JxMvFbqFHua1Zy";

            let provider = MockProvider::new(vec![
                Err(AgentError::api(413, "prompt is too long")),
                Ok(text_response(StopReason::EndTurn)),
            ]);
            let mut history = History::new(vec![
                Message::user("request".into()),
                tool_use(TOOL_USE_ID),
                Message {
                    role: Role::User,
                    content: vec![tool_result(TOOL_USE_ID)],
                    ..Default::default()
                },
                Message::user("prompt".into()),
            ]);
            let (raw_tx, _rx) = flume::unbounded();

            compact_history(
                &provider,
                &default_model(),
                &mut history,
                &EventSender::new(raw_tx, 0),
                &CancelToken::none(),
                &AgentConfig::default(),
                None,
                None,
            )
            .await
            .unwrap();

            let requests = provider.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            assert!(requests[0]
                .iter()
                .flat_map(|message| &message.content)
                .any(|block| matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == TOOL_USE_ID)));
            assert!(
                !requests[1]
                    .iter()
                    .flat_map(|message| &message.content)
                    .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
            );
        });
    }

    #[test]
    fn compaction_keeps_observation_before_dependent_reply() {
        smol::block_on(async {
            let provider = MockProvider::new(vec![Ok(text_response(StopReason::EndTurn))]);
            let mut history = History::new(vec![
                Message::observation("[monitor] build failed".into()),
                Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: "I will fix it".into(),
                    }],
                    ..Default::default()
                },
            ]);
            let (raw_tx, _rx) = flume::unbounded();

            compact_history(
                &provider,
                &default_model(),
                &mut history,
                &EventSender::new(raw_tx, 0),
                &CancelToken::none(),
                &AgentConfig::default(),
                None,
                None,
            )
            .await
            .unwrap();

            let requests = provider.requests.lock().unwrap();
            assert!(requests[0][0].is_observation());
            assert!(matches!(requests[0][1].role, Role::Assistant));
        });
    }

    #[test]
    fn truncate_oldest_round_preserves_text_beside_orphan() {
        let mut messages = vec![
            Message::user("request".into()),
            tool_use("expected"),
            Message {
                role: Role::User,
                content: vec![
                    tool_result("mismatched"),
                    ContentBlock::Text {
                        text: "keep me".into(),
                    },
                ],
                ..Default::default()
            },
            Message::user("prompt".into()),
        ];

        truncate_oldest_round(&mut messages);
        assert_tool_results_have_calls(&messages);

        assert_eq!(messages.len(), 2);
        assert!(
            matches!(&messages[0].content[..], [ContentBlock::Text { text }] if text == "keep me")
        );
        assert_tool_results_have_calls(&messages);
    }

    #[test]
    fn truncate_oldest_round_removes_single_user_message() {
        let mut messages = vec![
            Message::user("first".into()),
            Message::user("second".into()),
        ];
        truncate_oldest_round(&mut messages);
        assert_tool_results_have_calls(&messages);
        assert_eq!(messages.len(), 1);
        assert!(matches!(&messages[0].content[0], ContentBlock::Text { text } if text == "second"));
    }

    #[test]
    fn truncate_oldest_round_removes_assistant_tool_pair() {
        let mut messages = vec![
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use("t1", "bash", serde_json::json!({}))],
                ..Default::default()
            },
            Message {
                role: Role::User,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "t1".into(),
                    content: "output".into(),
                    is_error: false,
                }],
                ..Default::default()
            },
            Message::user("keep me".into()),
        ];
        truncate_oldest_round(&mut messages);
        assert_tool_results_have_calls(&messages);
        assert_eq!(messages.len(), 1);
        assert!(
            matches!(&messages[0].content[0], ContentBlock::Text { text } if text == "keep me")
        );
    }

    #[test]
    fn truncate_oldest_round_removes_assistant_without_matching_tool_result() {
        let mut messages = vec![
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use("t1", "bash", serde_json::json!({}))],
                ..Default::default()
            },
            Message::user("no tool result".into()),
        ];
        truncate_oldest_round(&mut messages);
        assert_tool_results_have_calls(&messages);
        assert_eq!(messages.len(), 1);
        assert!(
            matches!(&messages[0].content[0], ContentBlock::Text { text } if text == "no tool result")
        );
    }

    #[test]
    fn truncate_oldest_round_noop_on_single_message() {
        let mut messages = vec![Message::user("only".into())];
        truncate_oldest_round(&mut messages);
        assert_tool_results_have_calls(&messages);
        assert_eq!(messages.len(), 1);
    }

    #[test]
    fn truncate_oldest_round_removes_plain_assistant() {
        let mut messages = vec![
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "reply".into(),
                }],
                ..Default::default()
            },
            Message::user("keep me".into()),
        ];
        truncate_oldest_round(&mut messages);
        assert_tool_results_have_calls(&messages);
        assert_eq!(messages.len(), 1);
        assert!(
            matches!(&messages[0].content[0], ContentBlock::Text { text } if text == "keep me")
        );
    }

    #[test]
    fn truncate_oldest_round_consecutive_assistants_drains_until_user() {
        // [User, Assistant(no tools), Assistant(tools), User(results)] drains 2,
        // leaving Assistant-first — keep draining until first is User.
        let mut messages = vec![
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "plain reply".into(),
                }],
                ..Default::default()
            },
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use("t1", "bash", serde_json::json!({}))],
                ..Default::default()
            },
            Message {
                role: Role::User,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "t1".into(),
                    content: "output".into(),
                    is_error: false,
                }],
                ..Default::default()
            },
            Message::user("keep me".into()),
        ];
        truncate_oldest_round(&mut messages);
        assert_tool_results_have_calls(&messages);
        assert_eq!(messages.len(), 1);
        assert!(
            matches!(&messages[0].content[..], [ContentBlock::Text { text }] if text == "keep me")
        );
    }
}
