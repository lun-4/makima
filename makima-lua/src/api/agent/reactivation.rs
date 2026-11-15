use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use makima_agent::McpSession;
use makima_agent::actor::{EffectiveAgentConfig, ModeCeiling};
use makima_agent::agent::PromptSlotSource;
use makima_agent::cancel::CancelToken;
use makima_agent::manager::ReactivationError;
use makima_agent::mcp::{TOOL_SEARCH_TOOL_NAME, internal_tool_name};
use makima_agent::modes;
use makima_agent::plan_state;
use makima_agent::tools::{
    Deadline, RequestTools, ToolAudience, ToolContext, TurnToolBindings, TurnToolRoute,
};
use makima_providers::{ModelError, ModelSelection, RequestOptions};
use makima_storage::session_state::{
    SessionStateView, StoredAgentConfig, StoredAgentRecord, StoredAgentSpec,
};
use mlua::Lua;
use serde_json::Value;

use super::super::util::ctx::AgentContext;
use super::{
    ChildSettings, SessionOptions, SessionProviderPreparer, SessionTools, allowed_model,
    definition_name, prepare_model, session_model_preparer,
    structured_output::{self, StructuredOutput},
    validate_required_output_mode,
};
use crate::runtime::{Request, RequestTx};

pub(super) struct PreparedReactivation {
    pub(super) options: SessionOptions,
    pub(super) tools: SessionTools,
    pub(super) child: ChildSettings,
    pub(super) config: EffectiveAgentConfig,
    pub(super) template: ToolContext,
    pub(super) context: AgentContext,
    pub(super) commit: Arc<Mutex<Option<Value>>>,
    pub(super) warning: Option<String>,
    pub(super) prompt_slots: PromptSlotSource,
}

pub(super) async fn prepare(
    lua: &Lua,
    record: &StoredAgentRecord,
    stored: Option<&StoredAgentConfig>,
    context: AgentContext,
    view: &SessionStateView,
) -> Result<PreparedReactivation, ReactivationError> {
    record
        .restorable
        .as_ref()
        .map_err(|reason| not_restorable(reason.clone()))?;
    let spec = record
        .spec
        .as_ref()
        .ok_or_else(|| not_restorable("saved spawn specification is unavailable"))?;
    let stored = stored.ok_or_else(|| not_restorable("saved configuration is unavailable"))?;
    let model = allowed_model(&context, &stored.model).map_err(|error| match error {
        ModelError::UnsupportedProvider(provider) => {
            ReactivationError::ProviderUnavailable { provider }
        }
        _ => ReactivationError::ModelUnavailable {
            model: stored.model.clone(),
        },
    })?;
    let mut discovery_failed = false;
    context
        .providers
        .fetch_all(&context.model_policy, true, |batch| {
            if batch.provider.as_deref() == Some(model.provider.as_ref())
                && !batch.warnings.is_empty()
            {
                discovery_failed = true;
            }
        })
        .await;
    if discovery_failed {
        return Err(ReactivationError::ProviderUnavailable {
            provider: model.provider.to_string(),
        });
    }
    if !context
        .providers
        .offline_specs(&context.model_policy)
        .iter()
        .any(|available| available == &stored.model)
    {
        return Err(ReactivationError::ModelUnavailable {
            model: stored.model.clone(),
        });
    }
    let prepare = lua
        .app_data_ref::<SessionProviderPreparer>()
        .map(|prepare| Arc::clone(&prepare));
    let preparer = session_model_preparer(&context, prepare);
    let prepared = prepare_model(&stored.model, &context.model_policy, &preparer)
        .await
        .map_err(|_| ReactivationError::ProviderUnavailable {
            provider: model.provider.to_string(),
        })?;
    let modes::RestoredMode {
        mode,
        definition,
        warning,
    } = plan_state::prepare_restore_for_agent(view, record.id, &stored.mode, &context.modes)
        .map_err(|error| not_restorable(error.to_string()))?;
    let ceiling = ModeCeiling::from(spec.mode_ceiling.clone());
    ceiling
        .validate(&mode, Some(&definition))
        .map_err(|error| ReactivationError::ModeCeiling(error.to_string()))?;
    validate_required_output_mode(spec.output_schema.is_some(), Some(&definition))
        .map_err(not_restorable)?;
    let config = EffectiveAgentConfig::from_stored(stored, prepared, (mode, definition))
        .map_err(|error| not_restorable(error.to_string()))?
        .with_mode_authority(ceiling);
    let opts = RequestOptions {
        thinking: config.thinking,
        fast: config.fast,
    };
    let audience = ToolAudience::parse_name(&spec.audience)
        .ok_or_else(|| not_restorable(format!("unknown saved audience: {}", spec.audience)))?;
    let options = SessionOptions {
        selection: ModelSelection {
            spec: Some(config.model.spec().into()),
            thinking: Some(opts.thinking),
            fast: Some(opts.fast),
        },
        system: Some(spec.system.clone()),
        tools: None,
        local_tools: None,
        name: spec.name.clone().unwrap_or_default(),
        audience,
        inherit_provider: false,
        fast: opts.fast,
        mcp: spec.mcp,
        silent: spec.silent,
        auto_deliver: spec.auto_deliver,
        show: None,
    };
    let child = ChildSettings {
        model: config.model.clone(),
        provider: Arc::clone(&config.provider),
        opts,
        mode: config.mode.clone(),
    };
    let commit = Arc::new(Mutex::new(None));
    let mut template = context.to_tool_context();
    template.apply_effective_config(&config);
    template.execution_agent_id = Some(record.id);
    template.audience = audience;
    template.managed_turn = None;
    template.cancel = CancelToken::none();
    template.deadline = Deadline::None;
    template.pending_output_limits = None;
    template.local_tool_capture = Some(Arc::clone(&commit));
    template.mcp = context
        .mcp
        .as_ref()
        .filter(|_| spec.mcp)
        .map(McpSession::fresh);
    if let Some(mcp) = &template.mcp {
        mcp.ready().await;
    }
    let tools = restore_tools(spec, &mut template)?;
    let prompt_slots = lua
        .app_data_ref::<RequestTx>()
        .map(|requests| {
            let requests = requests.0.clone();
            PromptSlotSource::new(move || {
                let (reply, slots) = flume::bounded(1);
                let _ = requests.send(Request::CollectPromptSlots { reply });
                slots
            })
        })
        .unwrap_or_default();
    Ok(PreparedReactivation {
        options,
        tools,
        child,
        config,
        context: AgentContext::from(&template),
        template,
        commit,
        warning,
        prompt_slots,
    })
}

fn restore_tools(
    spec: &StoredAgentSpec,
    template: &mut ToolContext,
) -> Result<SessionTools, ReactivationError> {
    let structured = spec
        .output_schema
        .clone()
        .map(StructuredOutput::compile)
        .transpose()
        .map_err(not_restorable)?;
    let mut local = HashMap::new();
    let mut definitions = spec.tools.clone();
    if let Some(structured) = &structured {
        let count = definitions
            .iter()
            .filter(|definition| definition_name(definition) == Some(structured_output::TOOL_NAME))
            .count();
        if count > 1 {
            return Err(not_restorable(
                "duplicate saved structured_output definitions",
            ));
        }
        if count == 0 {
            definitions.push(structured.definition());
        }
        local.insert(
            structured_output::TOOL_NAME.to_owned(),
            structured.local_tool(),
        );
    }
    let request =
        RequestTools::assembled(Value::Array(definitions), &template.config, &template.model);
    template.local_tools = Arc::new(local.clone());
    template.tool_filter = Arc::clone(request.filter());
    template.request_tools = Some(request.clone());
    template.turn_bindings = Arc::new(TurnToolBindings::capture(
        &template.registry,
        &template.local_tools,
        template.mcp.as_ref(),
    ));
    let mut names = HashSet::new();
    for definition in request.definitions().as_array().into_iter().flatten() {
        let name = definition_name(definition)
            .ok_or_else(|| not_restorable("saved tool definition has no name"))?;
        if !names.insert(name) {
            return Err(not_restorable(format!(
                "duplicate saved tool definition: {name}"
            )));
        }
        let available = match template.resolve_turn_route(name) {
            Some(TurnToolRoute::Native(entry)) => entry.tool.audience().contains(template.audience),
            Some(TurnToolRoute::Local(tool)) => tool.audience.contains(template.audience),
            Some(TurnToolRoute::Mcp(_) | TurnToolRoute::ToolSearch) => true,
            None => false,
        };
        if !available {
            return Err(unavailable_tool(name));
        }
    }
    Ok(SessionTools {
        request,
        local,
        structured,
    })
}

fn unavailable_tool(name: &str) -> ReactivationError {
    let internal = internal_tool_name(name);
    if name == TOOL_SEARCH_TOOL_NAME || internal.contains('.') {
        ReactivationError::McpUnavailable {
            server: internal
                .split_once('.')
                .map_or(name, |(server, _)| server)
                .to_owned(),
        }
    } else {
        ReactivationError::ToolUnavailable {
            tool: name.to_owned(),
        }
    }
}

fn not_restorable(reason: impl Into<String>) -> ReactivationError {
    ReactivationError::NotRestorable {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use makima_agent::manager::ReactivationError;
    use makima_agent::mcp::test_support::stub_session;
    use makima_agent::modes::{ModeDef, ModeId};
    use makima_agent::tools::registry::{ToolRegistry, ToolSource};
    use makima_agent::tools::test_support::{mock_tool, stub_ctx};
    use makima_agent::tools::{
        ToolAudience, ToolFilter, TurnToolBindings, TurnToolRoute, local_tool,
    };
    use makima_agent::{AgentId, AgentMode};
    use makima_providers::{ModelInfo, test_util::offline_registry};
    use makima_storage::session_state::{
        SessionStateView, StoredAgentConfig, StoredAgentLifecycle, StoredAgentRecord,
        StoredAgentSpec, StoredModeCeiling,
    };
    use mlua::Lua;
    use serde_json::{Value, json};

    use super::{prepare, restore_tools, structured_output};
    use crate::api::agent::SessionProviderPreparer;
    use crate::api::util::ctx::AgentContext;

    const TOOL: &str = "restored_tool";
    const ROOT_LOCAL: &str = "root_local";
    const MCP_TOOL: &str = "server__lookup";
    const SERVER: &str = "server";
    const SAVED_DESCRIPTION: &str = "saved description";
    const ROOT_REPLY: &str = "root handler must not be inherited";

    fn spec(tools: Vec<Value>, output_schema: Option<Value>) -> StoredAgentSpec {
        StoredAgentSpec {
            model: Default::default(),
            system: String::new(),
            tools,
            audience: ToolAudience::MAIN.name().unwrap().into(),
            mcp: true,
            output_schema,
            name: None,
            silent: false,
            auto_deliver: true,
            inherit_provider: true,
            mode_ceiling: StoredModeCeiling {
                restrict_write_to: None,
                tools: None,
            },
            plugin: None,
        }
    }

    #[test_case::test_case(false; "falls_back_before_admission")]
    #[test_case::test_case(true; "ceiling_rejects_widening")]
    fn unavailable_plan_restore_uses_shared_fallback_and_revalidates_ceiling(restricted: bool) {
        let directory = tempfile::tempdir().unwrap();
        let blocker = directory.path().join("blocked");
        std::fs::write(&blocker, ROOT_REPLY).unwrap();
        let plan_path = blocker.join("plan.md");
        let mut template = stub_ctx(&AgentMode::Build);
        template.cwd = directory.path().into();
        template.providers = Arc::new(offline_registry(vec![(
            template.model.provider.as_ref(),
            vec![ModelInfo::id_only(template.model.id.clone())],
        )]));
        let mut definition = ModeDef::default_for(ModeId::Plan);
        definition.restrict_write_to = Some(plan_path.clone());
        let stored = StoredAgentConfig {
            cwd: directory.path().to_string_lossy().into_owned(),
            model: template.model.spec(),
            thinking: None,
            fast: false,
            workflow: false,
            mode: definition.stored_snapshot(&AgentMode::Plan(plan_path.clone())),
            options: Default::default(),
        };
        let mut saved_spec = spec(Vec::new(), None);
        saved_spec.mode_ceiling.restrict_write_to = restricted.then_some(plan_path);
        let record = StoredAgentRecord {
            id: AgentId::generate(),
            parent: None,
            depth: 0,
            transcript_key: String::new(),
            lifecycle: StoredAgentLifecycle::Open,
            ancestry_unknown: false,
            label: None,
            spawned_by_tool_use_id: None,
            spec: Some(saved_spec),
            restorable: Ok(()),
            usage: Default::default(),
            created_seq: 0,
            updated_seq: 0,
            legacy: false,
        };
        let before = serde_json::to_vec(&record).unwrap();
        let lua = Lua::new();
        let provider = Arc::clone(&template.provider);
        let preparer: SessionProviderPreparer =
            Arc::new(move |model| Ok((model, Arc::clone(&provider))));
        lua.set_app_data(preparer);
        let restored = smol::block_on(prepare(
            &lua,
            &record,
            Some(&stored),
            AgentContext::from(&template),
            &SessionStateView::default(),
        ));
        if restricted {
            assert!(matches!(restored, Err(ReactivationError::ModeCeiling(_))));
        } else {
            let restored = restored.unwrap();
            assert_eq!(restored.config.mode, AgentMode::Build);
            assert_eq!(restored.config.mode_def.unwrap().id, ModeId::Build);
            assert!(restored.warning.is_some());
            assert_eq!(restored.template.mode, AgentMode::Build);
        }
        assert_eq!(serde_json::to_vec(&record).unwrap(), before);
    }

    #[test]
    fn rebinds_current_registry_handler_without_root_restrictions_or_definition_changes() {
        let mut template = stub_ctx(&AgentMode::Build);
        template.audience = ToolAudience::MODEL;
        template.registry = Arc::new(ToolRegistry::new());
        template
            .registry
            .register(
                mock_tool(TOOL, ToolAudience::MODEL),
                ToolSource::Lua {
                    plugin: TOOL.into(),
                },
            )
            .unwrap();
        template.turn_bindings = Arc::new(TurnToolBindings::capture(
            &template.registry,
            &template.local_tools,
            None,
        ));
        let stale = Arc::clone(&template.turn_bindings);
        template.registry.clear_plugin(TOOL);
        template
            .registry
            .register(
                mock_tool(TOOL, ToolAudience::MODEL),
                ToolSource::Lua {
                    plugin: TOOL.into(),
                },
            )
            .unwrap();
        template.tool_filter = Arc::new(ToolFilter::Only(Vec::new()));
        let definitions = vec![
            json!({"name": TOOL, "description": SAVED_DESCRIPTION, "input_schema": {"type": "object", "properties": {"old_field": {"type": "string"}}}}),
        ];
        let tools = restore_tools(&spec(definitions.clone(), None), &mut template).unwrap();
        assert_eq!(tools.request.definitions(), &Value::Array(definitions));
        assert!(!stale.is_current(TOOL, &template.registry, &template.local_tools, None));
        assert!(matches!(
            template.resolve_turn_route(TOOL),
            Some(TurnToolRoute::Native(_))
        ));
        assert!(template.tool_filter.matches(TOOL));
    }

    #[test]
    fn inherited_local_handler_is_not_a_restoration_source() {
        let mut template = stub_ctx(&AgentMode::Build);
        template.local_tools = Arc::new(
            [(
                ROOT_LOCAL.into(),
                local_tool(ToolAudience::MODEL, |_, _| {
                    Box::pin(async { Ok(ROOT_REPLY.into()) })
                }),
            )]
            .into(),
        );
        template.turn_bindings = Arc::new(TurnToolBindings::capture(
            &template.registry,
            &template.local_tools,
            None,
        ));
        let result = restore_tools(
            &spec(vec![json!({"name": ROOT_LOCAL})], None),
            &mut template,
        );
        assert!(
            matches!(result, Err(ReactivationError::ToolUnavailable { tool }) if tool == ROOT_LOCAL)
        );
        assert!(template.local_tools.is_empty());
    }

    #[test_case::test_case(false; "append_missing_definition")]
    #[test_case::test_case(true; "preserve_saved_definition")]
    fn structured_output_rebuilds_one_handler_and_one_definition(saved_definition: bool) {
        let mut template = stub_ctx(&AgentMode::Build);
        template.audience = ToolAudience::MODEL;
        let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["answer"]});
        let definitions = if saved_definition {
            vec![
                json!({"name": structured_output::TOOL_NAME, "description": SAVED_DESCRIPTION, "input_schema": schema}),
            ]
        } else {
            Vec::new()
        };
        let tools = restore_tools(&spec(definitions.clone(), Some(schema)), &mut template).unwrap();
        assert_eq!(tools.request.definitions().as_array().unwrap().len(), 1);
        if saved_definition {
            assert_eq!(tools.request.definitions(), &Value::Array(definitions));
        }
        assert!(tools.structured.is_some());
        assert_eq!(tools.local.len(), 1);
        assert!(matches!(
            template.resolve_turn_route(structured_output::TOOL_NAME),
            Some(TurnToolRoute::Local(_))
        ));
        tools
            .structured
            .as_ref()
            .unwrap()
            .begin_turn(Default::default());
        let handler = &tools.local[structured_output::TOOL_NAME].handler;
        assert!(smol::block_on(handler(json!({"answer": "done"}), template.clone())).is_ok());
        assert_eq!(
            tools.structured.unwrap().reports(),
            vec![json!({"answer": "done"})]
        );
    }

    #[test_case::test_case(false; "missing_mcp")]
    #[test_case::test_case(true; "current_mcp")]
    fn rebinds_mcp_names_or_returns_server_error(available: bool) {
        let mut template = stub_ctx(&AgentMode::Build);
        if available {
            template.mcp = Some(stub_session(&[("server.lookup", SAVED_DESCRIPTION)]).fresh());
        }
        let definitions = vec![json!({"name": MCP_TOOL, "description": SAVED_DESCRIPTION})];
        let result = restore_tools(&spec(definitions.clone(), None), &mut template);
        if available {
            assert_eq!(
                result.unwrap().request.definitions(),
                &Value::Array(definitions)
            );
            assert!(matches!(
                template.resolve_turn_route(MCP_TOOL),
                Some(TurnToolRoute::Mcp(_))
            ));
        } else {
            assert!(
                matches!(result, Err(ReactivationError::McpUnavailable { server }) if server == SERVER)
            );
        }
    }

    #[test]
    fn structured_output_without_saved_schema_cannot_reuse_root_handler() {
        let mut template = stub_ctx(&AgentMode::Build);
        let root_structured =
            structured_output::StructuredOutput::compile(json!({"type": "object"})).unwrap();
        template.local_tools = Arc::new(
            [(
                structured_output::TOOL_NAME.into(),
                root_structured.local_tool(),
            )]
            .into(),
        );
        let result = restore_tools(
            &spec(vec![json!({"name": structured_output::TOOL_NAME})], None),
            &mut template,
        );
        assert!(
            matches!(result, Err(ReactivationError::ToolUnavailable { tool }) if tool == structured_output::TOOL_NAME)
        );
    }
}
