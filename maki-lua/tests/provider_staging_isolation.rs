use std::future::pending;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use maki_agent::{cancel::CancelToken, tools::ToolRegistry};
use maki_lua::{
    PluginHost,
    provider_adapter::{LuaProviderFactory, ProviderDispatch},
};
use maki_providers::{
    AgentError, Model, ProviderEvent, RequestOptions, StreamResponse, provider::BoxFuture,
    provider_registry::ProviderFactory,
};
use serde_json::{Value, json};

struct PendingDispatch {
    tokens: Mutex<Vec<CancelToken>>,
}

impl ProviderDispatch for PendingDispatch {
    fn prepare<'a>(
        &'a self,
        _: &'a str,
        model: &'a Model,
        _: &'a Value,
        cancel: CancelToken,
    ) -> BoxFuture<'a, Result<(String, Model, usize), AgentError>> {
        self.tokens.lock().unwrap().push(cancel);
        Box::pin(async move { Ok(("instance".into(), model.clone(), 1)) })
    }

    fn release(&self, _: &str) {}

    fn hook<'a>(
        &'a self,
        _: &'a str,
        _: &'static str,
        _: CancelToken,
    ) -> BoxFuture<'a, Result<Value, AgentError>> {
        Box::pin(async { Ok(Value::Null) })
    }

    fn stream<'a>(
        &'a self,
        _: &'a str,
        _: Value,
        _: flume::Sender<ProviderEvent>,
        cancel: CancelToken,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        self.tokens.lock().unwrap().push(cancel);
        Box::pin(pending())
    }
}

#[test]
fn dropping_stream_future_cancels_only_its_operation() {
    let dispatch = Arc::new(PendingDispatch {
        tokens: Mutex::new(Vec::new()),
    });
    let factory = LuaProviderFactory {
        provider: "fixture".into(),
        models: vec![],
        dispatch: dispatch.clone(),
    };
    let model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    let (model, provider) = smol::block_on(factory.prepare(&model, &json!({}))).unwrap();
    let (events, _) = flume::unbounded();
    let tools = json!([]);
    let mut first = provider.stream_message(
        &model,
        &[],
        "",
        &tools,
        &events,
        RequestOptions::default(),
        None,
    );
    let mut second = provider.stream_message(
        &model,
        &[],
        "",
        &tools,
        &events,
        RequestOptions::default(),
        None,
    );
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));
    assert!(matches!(second.as_mut().poll(&mut context), Poll::Pending));
    let tokens = dispatch.tokens.lock().unwrap().clone();
    assert_eq!(tokens.len(), 3);
    assert!(tokens[0].is_cancelled());
    assert!(!tokens[1].is_cancelled() && !tokens[2].is_cancelled());
    drop(first);
    assert!(tokens[1].is_cancelled());
    assert!(!tokens[2].is_cancelled());
    drop(second);
    assert!(tokens[2].is_cancelled());
}

#[test]
fn replacement_generation_cannot_reuse_old_factory_or_instance() {
    const PROVIDER: &str = r#"
        maki.api.register_provider({
            id = 'shared', models = {{ id = 'gpt-5.6-sol' }},
            prepare = function(_, model, config)
                return { model = model, stream = function()
                    return { message = { role = 'assistant', content = {{ type = 'text', text = config.marker }} },
                        usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }, stop_reason = 'end_turn' }
                end }
            end,
        })
    "#;
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("owner", PROVIDER).unwrap();
    let registry = host.provider_registry();
    let old = registry.snapshot();
    let model = Model::from_spec("openai/gpt-5.6-sol").unwrap();
    let (model, instance) = smol::block_on(
        old.get("shared")
            .unwrap()
            .factory
            .prepare(&model, &json!({"marker": "old"})),
    )
    .unwrap();
    host.load_source("owner", PROVIDER).unwrap();
    let current = registry.snapshot();
    assert!(
        smol::block_on(
            old.get("shared")
                .unwrap()
                .factory
                .prepare(&model, &json!({}))
        )
        .is_err()
    );
    let (new_model, new_instance) = smol::block_on(
        current
            .get("shared")
            .unwrap()
            .factory
            .prepare(&model, &json!({"marker": "new"})),
    )
    .unwrap();
    let (events, _) = flume::unbounded();
    let tools = json!([]);
    assert!(
        smol::block_on(instance.stream_message(
            &model,
            &[],
            "",
            &tools,
            &events,
            RequestOptions::default(),
            None
        ))
        .is_err()
    );
    let result = smol::block_on(new_instance.stream_message(
        &new_model,
        &[],
        "",
        &tools,
        &events,
        RequestOptions::default(),
        None,
    ))
    .unwrap();
    assert_eq!(result.message.first_text_content(), Some("new"));
}

#[test]
fn staged_provider_ownership_is_isolated_between_hosts() {
    const PROVIDER: &str = "maki.api.register_provider({ id = 'shared' })";
    let first = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    let second = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();

    first.load_source("owner", PROVIDER).unwrap();
    second.load_source("owner", PROVIDER).unwrap();
    let first_registry = first.provider_registry();
    let second_registry = second.provider_registry();
    assert!(first_registry.snapshot().get("shared").is_some());
    assert!(second_registry.snapshot().get("shared").is_some());
    first.unload("owner").unwrap();
    assert!(first_registry.snapshot().get("shared").is_none());
    assert!(second_registry.snapshot().get("shared").is_some());
    first.load_source("replacement", PROVIDER).unwrap();
    assert!(second.load_source("replacement", PROVIDER).is_err());
    second.unload("owner").unwrap();
    second.load_source("replacement", PROVIDER).unwrap();
}

#[test]
fn failed_reload_keeps_provider_ownership_until_successful_replacement() {
    const ORIGINAL: &str = "maki.api.register_provider({ id = 'original' })";
    const FAILED: &str = "maki.api.register_provider({ id = 'candidate' }); error('load failed')";
    const CANDIDATE: &str = "maki.api.register_provider({ id = 'candidate' })";
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();

    let registry = host.provider_registry();
    host.load_source("owner", ORIGINAL).unwrap();
    let original = registry.snapshot();
    assert!(original.get("original").is_some());
    assert!(host.load_source("owner", FAILED).is_err());
    assert!(Arc::ptr_eq(&original, &registry.snapshot()));
    assert!(registry.snapshot().get("candidate").is_none());
    assert!(host.load_source("other", ORIGINAL).is_err());
    host.load_source("other", CANDIDATE).unwrap();
    host.unload("other").unwrap();
    host.load_source("owner", CANDIDATE).unwrap();
    let replacement = registry.snapshot();
    assert!(replacement.get("original").is_none());
    assert!(replacement.get("candidate").is_some());
    assert!(original.get("original").is_some());
    host.load_source("other", ORIGINAL).unwrap();
}
