use maki_agent::{EffectiveAgentConfig, TurnOrigin, TurnOutcome, TurnProvenance, TurnResult};
use maki_providers::{CompleteModelSelection, RequestOptions};
use mlua::{Lua, Table, Value as LuaValue};
use serde_json::{Value as JsonValue, json};

use crate::api::util::convert::json_to_lua;

const SELECTION_NOT_TABLE: &str = "selection projection is not a table";
const TURN_RESULT_NOT_TABLE: &str = "turn result projection is not a table";

fn expect_table(value: LuaValue, error: &'static str) -> mlua::Result<Table> {
    match value {
        LuaValue::Table(table) => Ok(table),
        _ => Err(mlua::Error::runtime(error)),
    }
}

pub(super) fn selection_table(lua: &Lua, config: &EffectiveAgentConfig) -> mlua::Result<Table> {
    expect_table(
        json_to_lua(lua, &selection_json(config))?,
        SELECTION_NOT_TABLE,
    )
}

pub(in crate::api::agent) fn result_table(lua: &Lua, result: &TurnResult) -> mlua::Result<Table> {
    expect_table(
        json_to_lua(lua, &result_json(result))?,
        TURN_RESULT_NOT_TABLE,
    )
}

pub(crate) fn selection_json(config: &EffectiveAgentConfig) -> JsonValue {
    let options = RequestOptions {
        thinking: config.thinking,
        fast: config.fast,
    };
    json!(CompleteModelSelection::from_model(&config.model, options))
}

pub(crate) fn origin_json(origin: &TurnProvenance) -> JsonValue {
    let kind = match origin.origin {
        TurnOrigin::Internal => "internal",
        TurnOrigin::User => "user",
        TurnOrigin::Plugin => "plugin",
    };
    json!({
        "kind": kind,
        "plugin": origin.plugin,
        "plugin_generation": origin.plugin_generation,
        "source_agent": origin.source_agent.map(|id| id.to_string()),
        "source_turn": origin.source_turn.map(|id| id.to_string()),
    })
}

pub(crate) fn result_json(result: &TurnResult) -> JsonValue {
    let outcome = &result.outcome;
    let usage = outcome.usage();
    let mut value = json!({
        "agent_id": outcome.agent_id().to_string(),
        "turn_id": outcome.turn_id().to_string(),
        "text": result.text,
        "usage": usage,
        "output": result.output,
        "origin": origin_json(&result.provenance),
        "duration_ms": result.duration_ms,
        "input_tokens": usage.total_input(),
        "output_tokens": usage.output,
    });
    let status = match outcome {
        TurnOutcome::Completed { .. } => "completed",
        TurnOutcome::Failed { failure, .. } => {
            value["error"] = failure.diagnostic.clone().into();
            value["user_message"] = failure.user_message.clone().into();
            value["retryable"] = failure.retryable.into();
            "failed"
        }
        TurnOutcome::Cancelled { reason, .. } => {
            value["cancellation_reason"] = reason.to_string().into();
            "cancelled"
        }
    };
    value["status"] = status.into();
    if let Some(output) = result.output.last() {
        value["captured"] = output.clone();
    }
    value
}
