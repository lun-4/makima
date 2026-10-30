use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use maki_agent::command::{StandardCommands, StandardCompletions};
use maki_agent::config_change::prepare_selection;
use maki_agent::session_coordinator::ModelPreparer;
use maki_agent::{
    ActorBackend, AgentInput, BackendResult, ControlWork, History, TurnContext, WorkKind,
    actor::PreparedModel,
};
use maki_commands::{
    BuiltinOperation, CommandError, CommandFuture, CommandHost, CommandOutcome, CommandRegistry,
    CompletionInput, CompletionResult, HostContextRequest, HostContextResponse, HostRequest,
    HostResponse, InputDispatch, MODEL_COMMAND_NAME, TargetCapabilities, TargetHandle,
};
use maki_config::ModelPolicy;
use maki_domain::ModelSelection;
use maki_lua::orchestration::TrustedTarget;
use maki_providers::model::FastSupport;
use test_case::test_case;

use super::ModelArgSource;
use crate::model_capabilities::ModelCapabilityResolver;
use crate::model_capabilities::test_support::{CURRENT_SPEC, FixtureProvider, model, target};

const PRIVATE_SPEC: &str = "openai/private-model";
const COMPETING_SPEC: &str = "openai/other-private-model";
const UNIQUE_SPEC: &str = "openai/gpt-5";
const SECOND_GPT_SPEC: &str = "openai/gpt-4";
const FAST_ARGUMENT: &str = "fast=";
const FAST_ENABLED: &str = "fast=true";
const FAST_DISABLED: &str = "fast=false";
const AMBIGUOUS_IDENTITY: &str = "ambiguous model: gpt";
const UNKNOWN_IDENTITY: &str = "no model matches: absent";
const MALFORMED_IDENTITY: &str =
    "model must be in 'provider/model' format (e.g. anthropic/claude-sonnet-4-20250514)";
const POLICY_REJECTED: &str = "session option rejected by policy: openai/private-model";

struct RecordingHost {
    admission: TrustedTarget,
    specs: Arc<[Arc<str>]>,
    model_specs_requests: AtomicUsize,
    operations: Mutex<Vec<BuiltinOperation>>,
    prepare: ModelPreparer,
}

impl CommandHost for RecordingHost {
    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        match request {
            HostRequest::Context(HostContextRequest::ModelSpecs) => {
                self.model_specs_requests.fetch_add(1, Ordering::SeqCst);
                let specs = Arc::clone(&self.specs);
                Box::pin(
                    async move { Ok(HostResponse::Context(HostContextResponse::Values(specs))) },
                )
            }
            HostRequest::Builtin(operation) => {
                self.operations.lock().unwrap().push(operation.clone());
                let selection = match operation {
                    BuiltinOperation::SetModel { spec } => ModelSelection {
                        spec: Some(spec),
                        ..Default::default()
                    },
                    BuiltinOperation::SelectModel(selection) => selection,
                    operation => panic!("unexpected model operation: {operation:?}"),
                };
                let policy = Arc::clone(&self.admission.template.model_policy);
                let prepare = Arc::clone(&self.prepare);
                Box::pin(async move {
                    prepare_selection(&selection, &policy, &prepare)
                        .await
                        .map_err(|error| CommandError::Producer(Arc::from(error.to_string())))?;
                    Ok(HostResponse::Completed)
                })
            }
            request => panic!("unexpected host request: {request:?}"),
        }
    }
}

struct IdentityFixture {
    _commands: StandardCommands,
    registry: CommandRegistry,
    command_target: TargetHandle,
    host: Arc<RecordingHost>,
    prepared: Arc<Mutex<Vec<String>>>,
}

impl IdentityFixture {
    fn new(admission: TrustedTarget, discovered: &[&str]) -> Self {
        let registry = CommandRegistry::new();
        let prepared = Arc::new(Mutex::new(Vec::new()));
        let prepare: ModelPreparer = Arc::new({
            let prepared = Arc::clone(&prepared);
            move |candidate| {
                prepared.lock().unwrap().push(candidate.spec());
                Box::pin(async move {
                    Ok(PreparedModel {
                        provider: Arc::new(FixtureProvider),
                        model: model(&candidate.spec(), true, FastSupport::Supported),
                    })
                })
            }
        });
        let host = Arc::new(RecordingHost {
            specs: discovered
                .iter()
                .filter(|spec| admission.template.model_policy.allows(spec))
                .map(|spec| Arc::from(*spec))
                .collect(),
            admission: admission.clone(),
            model_specs_requests: AtomicUsize::new(0),
            operations: Mutex::new(Vec::new()),
            prepare: Arc::clone(&prepare),
        });
        let command_target = registry.bind_target(TargetCapabilities::ALL, host.clone());
        let source = ModelArgSource::with_resolver(
            Arc::new(ArcSwapOption::from(Some(Arc::new(
                discovered.iter().map(|spec| (*spec).to_owned()).collect(),
            )))),
            Arc::new(ModelCapabilityResolver::new(prepare)),
        )
        .with_targets(Arc::new(Mutex::new(HashMap::from([(
            command_target.id(),
            admission,
        )]))));
        let commands = StandardCommands::register(
            &registry,
            &[],
            StandardCompletions {
                model: Some(Arc::new(source)),
                ..Default::default()
            },
        )
        .unwrap();
        Self {
            _commands: commands,
            registry,
            command_target,
            host,
            prepared,
        }
    }

    async fn execute(&self, arguments: &str) -> InputDispatch {
        self.registry
            .dispatch_input(
                &self.command_target,
                format!("/model {arguments}").as_str().into(),
            )
            .await
    }

    async fn complete(&self, identity: &str, embedded: bool) -> Vec<String> {
        let arguments = if embedded {
            format!("{identity}{{{FAST_ARGUMENT}")
        } else if identity.is_empty() {
            FAST_ARGUMENT.to_owned()
        } else {
            format!("{identity} {FAST_ARGUMENT}")
        };
        let command = self
            .registry
            .resolve_for(&self.command_target, "/model")
            .unwrap();
        let session = self
            .registry
            .open_completion(command, self.command_target.id())
            .unwrap();
        let result = session
            .complete_input(
                CompletionInput {
                    arguments: Arc::from(arguments.as_str()),
                    argument: Arc::from(FAST_ARGUMENT),
                    argument_index: usize::from(embedded || !identity.is_empty()),
                    argument_range: Some(arguments.len() - FAST_ARGUMENT.len()..arguments.len()),
                    mode: Arc::from("insert"),
                }
                .normalized(MODEL_COMMAND_NAME, arguments.len()),
            )
            .await;
        match result {
            CompletionResult::Items(items) => items
                .iter()
                .map(|item| item.item().insertion.to_string())
                .collect(),
            result => panic!("unexpected model completion result: {result:?}"),
        }
    }

    fn take_prepared(&self) -> Vec<String> {
        std::mem::take(&mut *self.prepared.lock().unwrap())
    }

    fn take_operations(&self) -> Vec<BuiltinOperation> {
        std::mem::take(&mut *self.host.operations.lock().unwrap())
    }
}

impl Drop for IdentityFixture {
    fn drop(&mut self) {
        let _ = self.host.admission.target.close_subtree();
    }
}

fn option_arguments(identity: &str, embedded: bool) -> String {
    if embedded {
        format!("{identity}{{{FAST_DISABLED}}}")
    } else {
        format!("{identity} {FAST_DISABLED}")
    }
}

fn assert_completed(dispatch: InputDispatch) {
    assert!(matches!(
        dispatch,
        InputDispatch::Dispatched(CommandOutcome::Completed)
    ));
}

fn assert_failed(dispatch: InputDispatch, expected: &str) {
    assert!(matches!(
        dispatch,
        InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::Producer(message)))
            if message.as_ref() == expected
    ));
}

#[test_case(PRIVATE_SPEC, &[], PRIVATE_SPEC, 0; "undiscovered_exact")]
#[test_case(PRIVATE_SPEC, &[COMPETING_SPEC], PRIVATE_SPEC, 0; "slash_identity_not_fuzzy")]
#[test_case("gpt-5", &[UNIQUE_SPEC, CURRENT_SPEC], UNIQUE_SPEC, 2; "unique_unqualified")]
fn execution_and_completion_prepare_the_expected_identity(
    identity: &str,
    discovered: &[&str],
    expected: &str,
    expected_context_requests: usize,
) {
    smol::block_on(async {
        for embedded in [false, true] {
            let fixture = IdentityFixture::new(
                target(model(CURRENT_SPEC, true, FastSupport::Supported)),
                discovered,
            );
            assert_completed(fixture.execute(identity).await);
            assert_eq!(
                fixture.take_operations(),
                vec![BuiltinOperation::SetModel {
                    spec: Arc::from(expected),
                }]
            );
            assert_eq!(fixture.take_prepared(), [expected]);

            assert_completed(fixture.execute(&option_arguments(identity, embedded)).await);
            assert_eq!(
                fixture.take_operations(),
                vec![BuiltinOperation::SelectModel(ModelSelection {
                    spec: Some(Arc::from(expected)),
                    fast: Some(false),
                    ..Default::default()
                })]
            );
            assert_eq!(fixture.take_prepared(), [expected]);
            assert_eq!(
                fixture.host.model_specs_requests.load(Ordering::SeqCst),
                expected_context_requests
            );

            let items = fixture.complete(identity, embedded).await;
            assert_eq!(items, [FAST_ENABLED, FAST_DISABLED]);
            assert_eq!(fixture.take_prepared(), [expected]);
            assert!(fixture.take_operations().is_empty());
            assert_eq!(
                fixture.host.model_specs_requests.load(Ordering::SeqCst),
                expected_context_requests
            );
        }
    });
}

#[test_case("gpt", AMBIGUOUS_IDENTITY, 1; "ambiguous")]
#[test_case("absent", UNKNOWN_IDENTITY, 1; "unmatched")]
#[test_case("openai/", MALFORMED_IDENTITY, 0; "malformed_exact")]
fn invalid_identity_neither_dispatches_nor_completes_options(
    identity: &str,
    expected_error: &str,
    expected_context_requests: usize,
) {
    smol::block_on(async {
        for embedded in [false, true] {
            let fixture = IdentityFixture::new(
                target(model(CURRENT_SPEC, true, FastSupport::Supported)),
                &[UNIQUE_SPEC, SECOND_GPT_SPEC],
            );
            assert_failed(
                fixture.execute(&option_arguments(identity, embedded)).await,
                expected_error,
            );
            assert!(fixture.take_operations().is_empty());
            assert!(fixture.take_prepared().is_empty());
            assert!(fixture.complete(identity, embedded).await.is_empty());
            assert!(fixture.take_operations().is_empty());
            assert!(fixture.take_prepared().is_empty());
            assert_eq!(
                fixture.host.model_specs_requests.load(Ordering::SeqCst),
                expected_context_requests
            );
        }
    });
}

#[test]
fn options_only_execution_and_completion_skip_discovery_and_preparation() {
    smol::block_on(async {
        let fixture = IdentityFixture::new(
            target(model(CURRENT_SPEC, true, FastSupport::Supported)),
            &[],
        );
        assert_completed(fixture.execute(FAST_DISABLED).await);
        assert_eq!(
            fixture.take_operations(),
            vec![BuiltinOperation::SelectModel(ModelSelection {
                fast: Some(false),
                ..Default::default()
            })]
        );
        assert_eq!(
            fixture.complete("", false).await,
            [FAST_ENABLED, FAST_DISABLED]
        );
        assert!(fixture.take_prepared().is_empty());
        assert!(fixture.take_operations().is_empty());
        assert_eq!(fixture.host.model_specs_requests.load(Ordering::SeqCst), 0);
    });
}

struct IdleBackend;

impl ActorBackend for IdleBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(std::future::pending())
    }

    fn run_control<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

#[test_case(false; "root_allowed_child_denied")]
#[test_case(true; "root_denied_child_allowed")]
fn exact_identity_uses_addressed_root_or_child_policy(child_allowed: bool) {
    smol::block_on(async {
        for embedded in [false, true] {
            let mut root = target(model(CURRENT_SPEC, true, FastSupport::Supported));
            let child = root
                .target
                .manager()
                .spawn_child_trusted_with_config(
                    &root.target,
                    (*root.target.effective_config().unwrap().unwrap()).clone(),
                    Default::default(),
                    Vec::new(),
                    None,
                    |_| Ok::<_, String>(Box::new(IdleBackend)),
                )
                .unwrap();
            let mut child = TrustedTarget {
                target: child,
                template: root.template.clone(),
            };
            let denied = Arc::new(ModelPolicy::new(&[CURRENT_SPEC.into()], &[]).unwrap());
            if child_allowed {
                root.template.model_policy = denied;
            } else {
                child.template.model_policy = denied;
            }
            for (admission, allowed) in [(child, child_allowed), (root, !child_allowed)] {
                let fixture = IdentityFixture::new(admission, &[PRIVATE_SPEC, COMPETING_SPEC]);
                let dispatch = fixture
                    .execute(&option_arguments(PRIVATE_SPEC, embedded))
                    .await;
                if allowed {
                    assert_completed(dispatch);
                    assert_eq!(fixture.take_prepared(), [PRIVATE_SPEC]);
                } else {
                    assert_failed(dispatch, POLICY_REJECTED);
                    assert!(fixture.take_prepared().is_empty());
                }
                assert_eq!(
                    fixture.take_operations(),
                    vec![BuiltinOperation::SelectModel(ModelSelection {
                        spec: Some(Arc::from(PRIVATE_SPEC)),
                        fast: Some(false),
                        ..Default::default()
                    })]
                );
                let items = fixture.complete(PRIVATE_SPEC, embedded).await;
                if allowed {
                    assert_eq!(items, [FAST_ENABLED, FAST_DISABLED]);
                    assert_eq!(fixture.take_prepared(), [PRIVATE_SPEC]);
                } else {
                    assert!(items.is_empty());
                    assert!(fixture.take_prepared().is_empty());
                }
                assert!(fixture.take_operations().is_empty());
                assert_eq!(fixture.host.model_specs_requests.load(Ordering::SeqCst), 0);
            }
        }
    });
}
