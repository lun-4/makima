use super::{
    AgentLimits, AgentManagerHandle, AgentResidency, GraphLifecycle, ManagerError, ResidentAgent,
};
use crate::{
    AgentId, AgentInput, AgentMode, DoneReason, History, RunSettings, TurnCancellationReason,
    TurnId, TurnOutcome, TurnProvenance, TurnResult, TurnTicket,
    actor::EffectiveAgentConfig,
    journal::{JournalChange, JournalCommit, SessionJournal},
    test_util::StubBackend,
    tools::offload::OffloadStore,
};
use futures_lite::future::or;
use makima_providers::{Message, Model, provider::unconfigured};
use makima_storage::{
    id::{MakimaId, SessionRef},
    session_state::{
        MAX_INLINE_TURN_RESULT_BYTES, StateChange, StoredOffloadRef, StoredPayload,
        StoredTurnResult,
    },
    test_util::test_record,
};
use serde_json::json;
use smol::Timer;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
#[cfg(unix)]
use std::{fs, os::unix::fs::symlink};
use tempfile::TempDir;
use test_case::test_case;

const MODEL: &str = "test/model";
const HISTORY_TEXT: &str = "retained conversation";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const POP_GATE_TIMEOUT: &str = "turn did not reach the pop gate";
#[cfg(unix)]
const REPLACEMENT: &str = "replacement";

#[cfg_attr(
    not(unix),
    expect(
        dead_code,
        reason = "root and stored are read only by unix symlink tests"
    )
)]
struct Fixture {
    root: TempDir,
    store: Arc<OffloadStore>,
    manager: AgentManagerHandle,
    expected: TurnResult,
    stored: StoredTurnResult,
}

impl Fixture {
    fn new(mutate: impl FnOnce(&mut StoredTurnResult)) -> Self {
        let root = TempDir::new().unwrap();
        let session = MakimaId::generate();
        let store = Arc::new(OffloadStore::for_session(
            root.path(),
            &SessionRef::from(session),
        ));
        let agent_id = AgentId::generate();
        let turn_id = TurnId::generate();
        let expected = TurnResult {
            outcome: TurnOutcome::completed(
                agent_id,
                turn_id,
                Default::default(),
                1,
                DoneReason::EndTurn,
            ),
            text: "text".repeat(MAX_INLINE_TURN_RESULT_BYTES),
            output: vec![json!({ "answer": "output".repeat(MAX_INLINE_TURN_RESULT_BYTES) })],
            provenance: TurnProvenance::default(),
            duration_ms: 7,
        };
        let mut stored = expected.to_stored(session, Some(&store)).unwrap();
        assert!(matches!(stored.text, StoredPayload::Offloaded(_)));
        assert!(matches!(stored.output, StoredPayload::Offloaded(_)));
        mutate(&mut stored);
        let journal = Arc::new(SessionJournal::default());
        journal.bind_session(session).unwrap();
        journal
            .commit(JournalCommit {
                agents: vec![test_record(agent_id, None)],
                changes: vec![JournalChange {
                    agent: Some(agent_id),
                    generation: 0,
                    change: StateChange::Turn {
                        turn: turn_id,
                        result: stored.clone(),
                    },
                }],
                histories: BTreeMap::from([(
                    agent_id,
                    History::new(vec![Message::user(HISTORY_TEXT.into())])
                        .snapshot()
                        .clone(),
                )]),
                ..Default::default()
            })
            .unwrap();
        let snapshot = journal.checkpoint_snapshot().unwrap();
        let view = snapshot.fold().unwrap();
        let manager = AgentManagerHandle::restore_graph(
            AgentLimits::default(),
            journal,
            view,
            snapshot.histories,
        )
        .unwrap();
        Self {
            root,
            store,
            manager,
            expected,
            stored,
        }
    }

    fn agent_id(&self) -> AgentId {
        self.expected.outcome.agent_id()
    }

    fn turn_id(&self) -> TurnId {
        self.expected.outcome.turn_id()
    }

    fn ticket(&self) -> Result<TurnTicket, ManagerError> {
        self.manager
            .retained_turn_ticket(&self.manager.reference(self.agent_id()), self.turn_id())
    }

    fn bind(&self) {
        self.manager
            .bind_result_store(Arc::clone(&self.store))
            .unwrap();
    }

    fn assert_unavailable(&self, result: Result<TurnTicket, ManagerError>) {
        assert!(matches!(
            result,
            Err(ManagerError::TurnResultUnavailable { agent_id, turn_id })
                if agent_id == self.agent_id() && turn_id == self.turn_id()
        ));
    }

    fn assert_dormant(&self) {
        let node = self.manager.node(self.agent_id()).unwrap();
        assert_eq!(node.graph_lifecycle, GraphLifecycle::Dormant);
        assert_eq!(node.residency, AgentResidency::Dormant);
        assert!(node.actor.is_none());
    }

    async fn activate(&self) {
        self.manager
            .ensure_resident_with(self.agent_id(), |_| async {
                Ok(ResidentAgent {
                    backend: Box::new(StubBackend),
                    config: EffectiveAgentConfig::new(
                        RunSettings {
                            provider: unconfigured(),
                            model: Model::from_spec(MODEL).unwrap(),
                            fast: true,
                            workflow: false,
                            thinking: Default::default(),
                        },
                        AgentMode::Build,
                    ),
                    shared_messages: None,
                    start: None,
                    initialize: None,
                })
            })
            .await
            .unwrap();
    }

    async fn assert_unavailable_after_hydration(&self) {
        self.activate().await;
        assert!(
            self.manager
                .actor(self.agent_id())
                .unwrap()
                .turn_ticket(self.turn_id())
                .is_none()
        );
        self.assert_unavailable(self.ticket());
        self.shutdown().await;
    }

    async fn shutdown(&self) {
        assert!(
            self.manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    }
}

fn offloaded<T>(payload: &mut StoredPayload<T>) -> &mut StoredOffloadRef {
    match payload {
        StoredPayload::Offloaded(reference) => reference,
        StoredPayload::Inline(_) => panic!("oversized fixture payload was not offloaded"),
    }
}

#[test]
fn bound_store_resolves_dormant_results_and_hydrates_actor_owned_tickets() {
    smol::block_on(async {
        let fixture = Fixture::new(|_| {});
        fixture.assert_unavailable(fixture.ticket());
        fixture.assert_dormant();
        fixture.bind();
        fixture.bind();
        let dormant = fixture.ticket().unwrap();
        assert_eq!(dormant.peek_result(), Some(fixture.expected.clone()));
        let latest = fixture
            .manager
            .latest_retained_turn_ticket(&fixture.manager.reference(fixture.agent_id()))
            .unwrap()
            .unwrap();
        assert_eq!(latest.peek_result(), Some(fixture.expected.clone()));
        fixture.assert_dormant();
        fixture.activate().await;
        let actor = fixture.manager.actor(fixture.agent_id()).unwrap();
        let hydrated = actor.turn_ticket(fixture.turn_id()).unwrap();
        assert!(actor.owns_ticket(&hydrated));
        assert!(!actor.owns_ticket(&dormant));
        assert_eq!(hydrated.peek_result(), Some(fixture.expected.clone()));
        let retained = fixture.ticket().unwrap();
        assert!(actor.owns_ticket(&retained));
        assert_eq!(retained.peek_result(), Some(fixture.expected.clone()));
        fixture.shutdown().await;
    });
}

#[test]
fn latest_ticket_survives_close_before_terminal_result_is_published() {
    smol::block_on(async {
        let fixture = Fixture::new(|_| {});
        fixture.bind();
        fixture.activate().await;
        let agent = fixture.manager.reference(fixture.agent_id());
        let actor = fixture.manager.actor(fixture.agent_id()).unwrap();
        let (popped, release) = actor.pause_after_next_pop();
        let pending = actor
            .admit_turn(
                AgentInput {
                    message: HISTORY_TEXT.into(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
                },
                None,
                HISTORY_TEXT.into(),
            )
            .unwrap();
        or(async { popped.recv_async().await.unwrap() }, async {
            Timer::after(SHUTDOWN_TIMEOUT).await;
            panic!("{POP_GATE_TIMEOUT}");
        })
        .await;
        fixture.manager.close_subtree(fixture.agent_id()).unwrap();
        assert!(matches!(
            fixture.manager.actor(fixture.agent_id()),
            Err(ManagerError::NonLiveAgent(_))
        ));
        let snapshot = fixture.manager.journal().checkpoint_snapshot().unwrap();
        let view = snapshot.fold().unwrap();
        let results = &view.scopes[&Some(fixture.agent_id())].results;
        assert_eq!(results.back().unwrap().0, fixture.turn_id());
        assert!(results.iter().all(|(turn, _)| *turn != pending.turn_id()));
        let latest = fixture
            .manager
            .latest_retained_turn_ticket(&agent)
            .unwrap()
            .unwrap();
        assert_eq!(latest.turn_id(), pending.turn_id());
        assert!(actor.owns_ticket(&latest));
        assert!(latest.peek().is_none());
        release.send(()).unwrap();
        fixture.shutdown().await;
        assert!(matches!(
            latest.peek(),
            Some(TurnOutcome::Cancelled {
                reason: TurnCancellationReason::Closed,
                ..
            })
        ));
        let retained = fixture
            .manager
            .latest_retained_turn_ticket(&agent)
            .unwrap()
            .unwrap();
        assert_eq!(retained.peek_result(), latest.peek_result());
    });
}

#[test_case(false; "foreign_text")]
#[test_case(true; "foreign_output")]
fn foreign_session_is_rejected_before_resolver_and_during_hydration(output: bool) {
    smol::block_on(async {
        let fixture = Fixture::new(|stored| {
            if output {
                stored.text = StoredPayload::Inline(String::new());
                offloaded(&mut stored.output).session = MakimaId::generate();
            } else {
                offloaded(&mut stored.text).session = MakimaId::generate();
            }
        });
        fixture.bind();
        let mut reads = 0;
        fixture.assert_unavailable(fixture.manager.retained_turn_ticket_with(
            &fixture.manager.reference(fixture.agent_id()),
            fixture.turn_id(),
            |reference| {
                reads += 1;
                fixture
                    .store
                    .read(reference.path.to_str().unwrap(), reference.bytes)
            },
        ));
        assert_eq!(reads, 0);
        fixture.assert_unavailable(fixture.ticket());
        fixture.assert_dormant();
        fixture.assert_unavailable_after_hydration().await;
    });
}

#[cfg(unix)]
#[test_case(Some(false); "text_artifact")]
#[test_case(Some(true); "output_artifact")]
#[test_case(None; "session_directory")]
fn symlink_is_rejected_by_dormant_lookup_and_hydration(output: Option<bool>) {
    smol::block_on(async {
        let mut fixture = Fixture::new(|_| {});
        fixture.bind();
        let artifact = match output {
            Some(output) => {
                let reference = if output {
                    offloaded(&mut fixture.stored.output)
                } else {
                    offloaded(&mut fixture.stored.text)
                };
                fixture.store.dir().join(&reference.path)
            }
            None => fixture.store.dir(),
        };
        let replacement = fixture.root.path().join(REPLACEMENT);
        fs::rename(&artifact, &replacement).unwrap();
        symlink(&replacement, &artifact).unwrap();
        fixture.assert_unavailable(fixture.ticket());
        fixture.assert_dormant();
        fixture.assert_unavailable_after_hydration().await;
    });
}
