use std::sync::{Arc, Mutex};

use maki_agent::{
    TurnOutput,
    tools::{LocalTool, ToolAudience, local_tool},
};
use serde_json::{Value, json};

pub(super) const TOOL_NAME: &str = "structured_output";
const ACK: &str = "Output recorded.";
const MAX_NUDGES: usize = 2;
const MAX_ERRORS: usize = 3;
const MAX_ERROR_BYTES: usize = 2048;
const MISSING: &str = "agent finished without calling structured_output";
const NUDGE: &str = "You did not call the structured_output tool. Call it now with your final result matching its input schema.";

#[derive(Default)]
struct ReportState {
    output: Option<TurnOutput>,
    reports: Vec<Value>,
    last_errors: Option<String>,
    nudges: usize,
}

pub(super) struct StructuredOutput {
    schema: Value,
    validator: jsonschema::Validator,
    state: Mutex<ReportState>,
}

impl StructuredOutput {
    pub(super) fn compile(schema: Value) -> Result<Arc<Self>, String> {
        if schema.get("type").and_then(Value::as_str) != Some("object") {
            return Err("output_schema must have type object".into());
        }
        let validator = jsonschema::validator_for(&schema)
            .map_err(|error| format!("invalid output_schema: {error}"))?;
        Ok(Arc::new(Self {
            schema,
            validator,
            state: Mutex::new(ReportState::default()),
        }))
    }

    pub(super) fn definition(&self) -> Value {
        json!({ "name": TOOL_NAME, "description": "Report your final result when your task is complete.", "input_schema": self.schema })
    }

    pub(super) fn local_tool(self: &Arc<Self>) -> LocalTool {
        let reports = Arc::clone(self);
        local_tool(ToolAudience::MODEL, move |input, _ctx| {
            let result = reports.record(input);
            Box::pin(async move { result })
        })
    }

    pub(super) fn begin_turn(&self, output: TurnOutput) {
        *self.state.lock().unwrap_or_else(|error| error.into_inner()) = ReportState {
            output: Some(output),
            ..Default::default()
        };
    }

    fn record(&self, value: Value) -> Result<String, String> {
        let errors = self
            .validator
            .iter_errors(&value)
            .take(MAX_ERRORS)
            .map(|error| format!("at {}: {error}", error.instance_path))
            .collect::<Vec<_>>()
            .join("\n");
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !errors.is_empty() {
            let mut end = errors.len().min(MAX_ERROR_BYTES);
            while !errors.is_char_boundary(end) {
                end -= 1;
            }
            let errors = errors[..end].to_owned();
            state.last_errors = Some(errors.clone());
            return Err(format!(
                "Input does not match the required schema. Fix the errors and call structured_output again:\n{errors}"
            ));
        }
        let output = state
            .output
            .as_ref()
            .ok_or("structured_output has no active turn")?;
        output.append_output(value.clone());
        state.reports.push(value);
        state.last_errors = None;
        Ok(ACK.into())
    }

    pub(super) fn reports(&self) -> Vec<Value> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .reports
            .clone()
    }

    pub(super) fn completion_check(&self) -> Result<Option<String>, String> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.reports.is_empty() {
            return Ok(None);
        }
        if state.nudges == MAX_NUDGES {
            return Err(state.last_errors.as_ref().map_or_else(
                || MISSING.into(),
                |errors| format!("agent result does not match output_schema:\n{errors}"),
            ));
        }
        state.nudges += 1;
        Ok(Some(NUDGE.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_reports_recovers_and_resets_between_turns() {
        let reports = StructuredOutput::compile(json!({"type":"object", "properties":{"answer":{"type":"string"}}, "required":["answer"]})).unwrap();
        reports.begin_turn(TurnOutput::default());
        assert!(reports.record(json!({"answer": 3})).is_err());
        assert!(reports.reports().is_empty());
        assert_eq!(reports.completion_check().unwrap(), Some(NUDGE.into()));
        assert_eq!(reports.record(json!({"answer":"yes"})).unwrap(), ACK);
        assert_eq!(reports.record(json!({"answer":"also"})).unwrap(), ACK);
        assert_eq!(
            reports.reports(),
            vec![json!({"answer":"yes"}), json!({"answer":"also"})]
        );
        assert_eq!(reports.completion_check().unwrap(), None);
        reports.begin_turn(TurnOutput::default());
        assert!(reports.reports().is_empty());
        for _ in 0..MAX_NUDGES {
            assert_eq!(reports.completion_check().unwrap(), Some(NUDGE.into()));
        }
        assert_eq!(reports.completion_check().unwrap_err(), MISSING);
    }

    #[test]
    fn accepted_reports_are_captured_before_settlement_and_cannot_change_frozen_results() {
        use maki_agent::{
            AgentActorHandle, AgentId, AgentInput, History, TurnCancellationReason, TurnOutcome,
            actor::{ActorBackend, BackendResult, TurnContext, WorkKind},
        };
        use maki_providers::TokenUsage;
        struct Backend;
        impl ActorBackend for Backend {
            fn run_turn<'a>(
                &'a mut self,
                _history: &'a mut History,
                context: TurnContext,
                _input: AgentInput,
                _work: WorkKind,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
            {
                Box::pin(async move {
                    context.cancel.cancelled().await;
                    BackendResult::EnteredRun(TurnOutcome::cancelled(
                        context.agent_id,
                        context.turn_id.unwrap(),
                        TokenUsage::default(),
                        0,
                        TurnCancellationReason::Closed,
                    ))
                })
            }
            fn run_control<'a>(
                &'a mut self,
                _history: &'a mut History,
                _context: TurnContext,
                _control: &'a maki_agent::actor::ControlWork,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
            {
                Box::pin(async { BackendResult::ControlDone })
            }
            fn run_compact<'a>(
                &'a mut self,
                _history: &'a mut History,
                _context: TurnContext,
                _instructions: Option<&'a str>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = BackendResult> + Send + 'a>>
            {
                Box::pin(async { BackendResult::CompactDone })
            }
        }
        // The actor exposes ticket settlement without making the helper depend on ticket internals.
        let (actor, task) =
            AgentActorHandle::spawn(AgentId::generate(), Vec::new(), None, Box::new(Backend));
        let ticket = actor
            .admit_turn(
                AgentInput::from_defaults(
                    "report".into(),
                    maki_agent::AgentMode::Build,
                    Vec::new(),
                    Default::default(),
                ),
                None,
                "report".into(),
            )
            .unwrap();
        let reports = StructuredOutput::compile(json!({"type":"object"})).unwrap();
        reports.begin_turn(ticket.output());
        reports.record(json!({"answer":"before close"})).unwrap();
        actor.close();
        let result = smol::block_on(ticket.wait_result());
        reports.record(json!({"answer":"late"})).unwrap();
        assert_eq!(result.output, vec![json!({"answer":"before close"})]);
        assert_eq!(ticket.peek_result().unwrap().output, result.output);
        smol::block_on(task);
    }

    #[test]
    fn rejects_nonobject_and_invalid_schemas() {
        assert!(StructuredOutput::compile(json!({"type":"string"})).is_err());
        assert!(
            StructuredOutput::compile(
                json!({"type":"object", "properties":{"a":{"type":"unknown"}}})
            )
            .is_err()
        );
    }
}
