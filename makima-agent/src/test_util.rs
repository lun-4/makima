use crate::{
    ActorBackend, AgentInput, BackendResult, ControlWork, TurnContext, WorkKind,
    actor::ActorHistory,
};
use std::{future::Future, pin::Pin};

const UNEXPECTED_OPERATION: &str = "StubBackend must not execute work";

/// An inert backend for fixtures that must not execute turns, controls, or compaction.
pub struct StubBackend;

impl ActorBackend for StubBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { panic!("{UNEXPECTED_OPERATION}") })
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { panic!("{UNEXPECTED_OPERATION}") })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { panic!("{UNEXPECTED_OPERATION}") })
    }
}
