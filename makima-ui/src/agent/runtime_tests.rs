use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use makima_agent::actor::{ActorBackend, ActorEvent, AgentContextSnapshot, DeliveryStatus};
use makima_agent::context::{CONTEXT_CHANGED_NOTICE, root_context};
use makima_agent::headless::{FrontendProfile, HeadlessBackend, HeadlessBackendOptions};
use makima_agent::journal::SessionJournal;
use makima_agent::manager::ResidentAgent;
use makima_agent::permissions::PermissionManager;
use makima_agent::tools::{
    FileReadTracker, FileWriteLocks, QuestionMode, ToolAudience, ToolRegistry,
};
use makima_agent::{
    AgentConfig, AgentEvent, AgentId, AgentInput, AgentManagerHandle, AgentMode, AgentParams,
    AgentRef, CancelToken, EffectiveAgentConfig, Envelope, ModeRegistry, RunSettings, ToolOutput,
    ToolOutputLines, TurnAdmissionOptions, TurnCancellationReason, TurnId, TurnOutcome,
};
use makima_config::{ModelPolicy, PermissionsConfig, ProjectConfig};
use makima_lua::orchestration::TrustedTarget;
use makima_lua::test_support::run_command;
use makima_providers::{
    AgentError, ContentBlock, Invocation, Message, Model, RequestDelivery, TokenUsage,
};
use makima_storage::id::SessionRef;
use makima_storage::sessions::Session;
use test_case::test_case;

use super::shared_queue::{QueueItem, actor_queue};

const TEST_CWD: &str = "/tmp";
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const TOOL_BATCH_CALL_ID: &str = "tool-batch-call";
const TOOL_BATCH_TOOL: &str = "absent_tool";
const EXPLICIT_RESPONSE: &str = "visible explicit response";

/// Waits for the actor's own settlement publication and asserts it names
/// the presentation that settled last. Settlement is the actor's, never
/// re-derived from a frontend mirror.
async fn await_settlement(
    events: &flume::Receiver<makima_agent::actor::ActorEvent>,
    presentation: u64,
) -> makima_agent::ActorWorkIdentity {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::clone(&seen);
    // Earlier settlements stay published, so skip past them rather than
    // assuming the next idle belongs to the work under test.
    futures_lite::future::or(
        async {
            loop {
                let event = events.recv_async().await.unwrap();
                if let makima_agent::actor::ActorEvent::Idle { settled, .. } = event {
                    observed.lock().unwrap().push(settled);
                    if let Some(settled) = settled
                        && settled.presentation_id == presentation
                    {
                        break settled;
                    }
                }
            }
        },
        async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!(
                "waiting for settlement of presentation {presentation}; saw {:?}",
                seen.lock().unwrap()
            );
        },
    )
    .await
}

#[test_case(false, false; "explicit_complete")]
#[test_case(true, false; "explicit_cancel")]
#[test_case(false, true; "mixed_fifo_complete")]
#[test_case(true, true; "mixed_fifo_cancel")]
fn explicit_actual_root_turn_uses_current_ui_run_and_cancel_alias(cancel: bool, deferred: bool) {
    smol::block_on(async {
        const RUN_ID: u64 = 7;
        const RESPONSE: &str = "visible explicit response";
        let (backend, events) = BackendFixture::new();
        let input = AgentInput::new("hello".into(), Vec::new());
        let (release, released) = flume::bounded(1);
        let provider = Arc::new(ExplicitProvider { release: released });
        let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
        let root = manager
            .create_root_with_config(
                Some(
                    makima_agent::EffectiveAgentConfig::new(
                        makima_agent::RunSettings {
                            provider,
                            model: crate::components::test_model(),
                            fast: false,
                            workflow: false,
                            thinking: Default::default(),
                        },
                        AgentMode::Build,
                    )
                    .with_mode_def(
                        makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build),
                    ),
                ),
                Vec::new(),
                None,
                |agent_id| Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(agent_id))),
            )
            .await
            .unwrap();
        let actor = root.actor().unwrap();
        // The prior UI turn already displayed RUN_ID - 1, so the explicit
        // turn after it is the one that allocates its own presentation.
        actor.observe_presentation(RUN_ID - 1);
        let mut app = crate::app::tests::actor_presentation_app(root.clone(), RUN_ID - 1);
        let make_input = |text: &str| AgentInput::new(text.into(), Vec::new());
        if deferred {
            actor.observe_presentation(RUN_ID - 2);
            // The UI already drew this deferred submission, so admission
            // keeps the id the bubble is under.
            actor
                .submit(
                    make_input("successor"),
                    None,
                    String::new(),
                    TurnAdmissionOptions {
                        already_displayed: true,
                        displayed_presentation: Some(RUN_ID - 1),
                        ..Default::default()
                    },
                )
                .unwrap();
            // The deferred successor is admitted with the presentation the
            // app already drew, so its stream keeps that id.
            loop {
                let envelope = events.recv_async().await.unwrap();
                if matches!(envelope.event, AgentEvent::TextDelta { .. }) {
                    assert_eq!(
                        envelope.presentation(),
                        Some(RUN_ID - 1),
                        "the deferred successor streams under the id the UI drew"
                    );
                    break;
                }
            }
            let active_policy_generation = actor.config_snapshot().unwrap().generation;
            actor
                .submit(
                    make_input("folded user input"),
                    None,
                    String::new(),
                    TurnAdmissionOptions {
                        already_displayed: false,
                        ..Default::default()
                    },
                )
                .unwrap();
            assert_eq!(
                actor.config_snapshot().unwrap().generation,
                active_policy_generation
            );
            assert!(makima_agent::batchable(&make_input("successor")));
            assert_eq!(
                actor
                    .pending_submissions()
                    .iter()
                    .map(|entry| entry.text.as_str())
                    .collect::<Vec<_>>(),
                ["folded user input"]
            );
            release.send(()).unwrap();
            let mut folded_bubble = false;
            loop {
                let envelope = events.recv_async().await.unwrap();
                assert_eq!(envelope.presentation(), Some(RUN_ID - 1));
                folded_bubble |= matches!(envelope.event, AgentEvent::QueueItemConsumed { .. });
                if matches!(envelope.event, AgentEvent::TextDelta { .. }) {
                    break;
                }
            }
            assert!(folded_bubble);
            release.send(()).unwrap();
            events.drain().for_each(drop);
        }
        let ticket = actor
            .admit_turn_with_options(
                input,
                None,
                String::new(),
                TurnAdmissionOptions {
                    provenance: makima_agent::actor::TurnProvenance {
                        origin: makima_agent::actor::TurnOrigin::Plugin,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        let (successor, successor_presentation) = if deferred {
            let barrier = actor.reserve_prepared_operation(None).unwrap();
            let queue = crate::agent::shared_queue::actor_queue(Arc::new(actor.clone()));
            for text in ["earlier deferred", "final deferred"] {
                let input = make_input(text);
                queue.push(crate::agent::shared_queue::QueueItem {
                    input,
                    displayed: None,
                });
            }
            (None, barrier.presentation_id() + 1)
        } else {
            (
                Some(
                    actor
                        .admit_turn(make_input("successor"), None, String::new())
                        .unwrap(),
                ),
                RUN_ID + 1,
            )
        };
        // The explicit turn is admitted right after the prior UI turn, so
        // the actor stamps it with RUN_ID; the successor queued behind it
        // takes the next presentation after any barrier's reservation.
        let mut explicit_presentation = None;
        let mut visible = Vec::new();
        loop {
            let envelope = futures_lite::future::or(events.recv_async(), async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!(
                    "waiting for explicit streaming: actor={:?}, outcome={:?}, events={visible:?}",
                    actor.snapshot(),
                    ticket.peek()
                );
            })
            .await
            .unwrap();
            // Identity names the turn, so the deferred successor sharing
            // this stream is never mistaken for the explicit turn.
            let identity = envelope.identity.expect("admitted work is stamped");
            if identity.turn_id() == Some(ticket.turn_id()) {
                let presentation = identity.presentation_id;
                assert_eq!(
                    *explicit_presentation.get_or_insert(presentation),
                    presentation,
                    "one turn never spans two presentations"
                );
                visible.push(matches!(
                    envelope.event,
                    AgentEvent::QueueItemConsumed { .. }
                ));
                let streamed = matches!(
                    &envelope.event,
                    AgentEvent::TextDelta { text } if text == RESPONSE
                );
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streamed {
                    break;
                }
            }
        }
        assert!(visible.iter().any(|consumed| *consumed));
        let explicit_presentation =
            explicit_presentation.expect("the explicit turn emitted events");
        if cancel {
            // Cancel the explicit turn under the presentation it was
            // admitted with, not the successor queued behind it.
            actor.cancel_turn(ticket.turn_id()).unwrap();
        } else {
            release.send(()).unwrap();
        }
        let outcome = futures_lite::future::or(ticket.wait(), async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!(
                "waiting for explicit settlement: actor={:?}, events={:?}",
                actor.snapshot(),
                events.drain().collect::<Vec<_>>()
            );
        })
        .await;
        assert_eq!(
            matches!(
                outcome,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::User,
                    ..
                }
            ),
            cancel
        );
        let mut terminal = false;
        let mut successor_bubbles = Vec::new();
        loop {
            let envelope = futures_lite::future::or(events.recv_async(), async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!(
                    "waiting for successor streaming: actor={:?}",
                    actor.snapshot()
                );
            })
            .await
            .unwrap();
            if envelope.presentation() == Some(successor_presentation)
                && let AgentEvent::QueueItemConsumed { text, .. } = &envelope.event
            {
                successor_bubbles.push(text.clone());
            }
            terminal |= envelope.presentation() == Some(explicit_presentation)
                && matches!(&envelope.event, AgentEvent::TurnOutcome(_));
            let streamed = envelope.presentation() == Some(successor_presentation)
                && matches!(&envelope.event, AgentEvent::TextDelta { .. });
            app.update(crate::app::Msg::Agent(Box::new(envelope)));
            if streamed {
                break;
            }
        }
        assert!(terminal);
        crate::app::tests::assert_actor_presentation(
            &mut app,
            successor_presentation,
            RESPONSE,
            if deferred {
                "final deferred"
            } else {
                "successor"
            },
        );
        if deferred {
            assert_eq!(successor_bubbles, ["earlier deferred", "final deferred"]);
        }
        assert_eq!(explicit_presentation, RUN_ID);
        assert_eq!(
            actor.presentation_id(),
            successor_presentation,
            "the actor owns the presentation counter"
        );
        if let Some(successor) = &successor {
            assert!(successor.peek().is_none());
        }
        if deferred && cancel {
            actor
                .cancel_turn(actor.snapshot().active_turn.unwrap())
                .unwrap();
        } else {
            release.send(()).unwrap();
        }
        let successor_outcome = match successor {
            Some(successor) => {
                // The successor makes its own request, so it needs its own
                // release before it can produce an outcome.
                release.send(()).unwrap();
                Some(
                    futures_lite::future::or(successor.wait(), async {
                        smol::Timer::after(TEST_TIMEOUT).await;
                        panic!(
                            "waiting for successor settlement: actor={:?}",
                            actor.snapshot()
                        );
                    })
                    .await,
                )
            }
            None => None,
        };
        // The queued successor settles into its own outcome under its own
        // presentation; the shared stream only ever carries the explicit
        // turn's terminal event.
        if let Some(outcome) = successor_outcome {
            assert!(
                matches!(
                    outcome,
                    TurnOutcome::Cancelled {
                        reason: TurnCancellationReason::User,
                        ..
                    }
                ) == (deferred && cancel),
                "{outcome:?}"
            );
        }
        // Every event the shared stream carried for the explicit turn was
        // stamped with its one presentation; the outcome itself arrived on
        // the ticket the actor settles.
        // The app is only idle once every admitted turn has settled, so
        // keep folding the stream until it stops streaming.
        futures_lite::future::or(
            async {
                while app.status == crate::components::Status::Streaming {
                    let Ok(envelope) = events.recv_async().await else {
                        break;
                    };
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                }
            },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!(
                    "app never left the streaming state: actor={:?}",
                    actor.snapshot()
                );
            },
        )
        .await;
        assert_eq!(app.status, crate::components::Status::Idle);
        if !(deferred && cancel) {
            crate::app::tests::assert_actor_response(&mut app, RESPONSE);
        }
        actor.close();
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false; "explicit_turn")]
#[test_case(true; "undisplayed_deferred_root")]
fn ui_cancel_targets_buffered_active_root_without_cancelling_future_turn(deferred: bool) {
    const FIRST_RUN: u64 = 7;
    const SHARED_CORRELATION: &str = "plugin-shared-correlation";

    smol::block_on(futures_lite::future::or(
        async {
            let (backend, events) = BackendFixture::new();
            let make_input = || AgentInput::new("explicit root".into(), Vec::new());
            let (release, released) = flume::bounded(1);
            let provider = Arc::new(ExplicitProvider {
                release: released.clone(),
            });
            let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let root = manager
                .create_root_with_config(
                    Some(
                        makima_agent::EffectiveAgentConfig::new(
                            makima_agent::RunSettings {
                                provider,
                                model: crate::components::test_model(),
                                fast: false,
                                workflow: false,
                                thinking: Default::default(),
                            },
                            AgentMode::Build,
                        )
                        .with_mode_def(
                            makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build),
                        ),
                    ),
                    Vec::new(),
                    None,
                    |agent_id| {
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(agent_id)))
                    },
                )
                .await
                .unwrap();
            let actor = root.actor().unwrap();
            // Seat the actor where the app believes the display is, so the
            // first admission lands on FIRST_RUN.
            actor.observe_presentation(FIRST_RUN - 1);
            let queue = crate::agent::shared_queue::actor_queue(Arc::new(actor.clone()));
            let mut app = crate::app::tests::actor_presentation_app(root.clone(), FIRST_RUN - 1);
            let first = actor.admit_turn(make_input(), None, String::new()).unwrap();
            loop {
                let envelope = events.recv_async().await.unwrap();
                assert_eq!(envelope.presentation(), Some(FIRST_RUN));
                let streaming = matches!(envelope.event, AgentEvent::TextDelta { .. });
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streaming {
                    break;
                }
            }
            let (second, second_run) = if deferred {
                let boundary = actor.reserve_prepared_operation(None).unwrap();
                queue.push(crate::agent::shared_queue::QueueItem {
                    input: make_input(),
                    displayed: None,
                });
                (None, boundary.presentation_id() + 1)
            } else {
                (
                    Some(
                        actor
                            .admit_turn(make_input(), None, SHARED_CORRELATION.into())
                            .unwrap(),
                    ),
                    FIRST_RUN + 1,
                )
            };
            release.send(()).unwrap();
            assert!(matches!(first.wait().await, TurnOutcome::Completed { .. }));

            let mut buffered = Vec::new();
            loop {
                let envelope = events.recv_async().await.unwrap();
                let second_streaming = envelope.presentation() == Some(second_run)
                    && matches!(envelope.event, AgentEvent::TextDelta { .. });
                buffered.push(envelope);
                if second_streaming {
                    break;
                }
            }
            let second = second.unwrap_or_else(|| {
                actor
                    .turn_ticket(actor.snapshot().active_turn.unwrap())
                    .unwrap()
            });
            assert!(buffered.iter().any(|envelope| {
                envelope.presentation() == Some(second_run)
                    && matches!(envelope.event, AgentEvent::QueueItemConsumed { .. })
            }));
            assert_eq!(app.run_id, FIRST_RUN);
            assert_eq!(app.status, crate::components::Status::Streaming);
            assert!(second.peek().is_none());
            // A displayed bubble pushes under the id the UI already shows,
            // so admission keeps it and the counter does not move.
            let displayed_presentation = queue.current_run_id();
            queue.push(crate::agent::shared_queue::QueueItem {
                input: make_input(),
                displayed: Some(displayed_presentation),
            });
            assert_eq!(queue.current_run_id(), displayed_presentation);
            assert_eq!(actor.snapshot().active_turn, Some(second.turn_id()));
            assert!(
                actor
                    .pending_submissions()
                    .iter()
                    .any(|entry| { entry.text == "explicit root" && entry.already_displayed })
            );
            let actions = app.update(crate::app::Msg::Key(crossterm::event::KeyEvent::new(
                crossterm::event::KeyCode::Char('c'),
                crossterm::event::KeyModifiers::CONTROL,
            )));
            let [crate::components::Action::CancelAgent { turn_id, run_id }] = actions.as_slice()
            else {
                panic!("UI cancel did not dispatch cancellation");
            };
            assert_eq!(*turn_id, Some(second.turn_id()));
            assert_eq!(*run_id, FIRST_RUN);
            let cancelled_turn_id = turn_id.unwrap();
            // Cancellation fences every presentation at or below the id the
            // actor held when it cut, so the app never lands behind it.
            assert!(
                app.run_id >= displayed_presentation,
                "cancel must fence at or past the pending presentation"
            );
            assert!(matches!(
                second.wait().await,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::User,
                    ..
                }
            ));
            assert!(queue.is_empty());
            assert!(released.is_empty());
            for envelope in buffered {
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
            }
            assert_eq!(app.status, crate::components::Status::Idle);

            let future = if deferred {
                queue.push(crate::agent::shared_queue::QueueItem {
                    input: make_input(),
                    displayed: None,
                });
                None
            } else {
                Some(
                    actor
                        .admit_turn(make_input(), None, SHARED_CORRELATION.into())
                        .unwrap(),
                )
            };
            let future_presentation = queue.current_run_id();
            loop {
                let envelope = events.recv_async().await.unwrap();
                let streaming = envelope.presentation() == Some(future_presentation)
                    && matches!(envelope.event, AgentEvent::TextDelta { .. });
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streaming {
                    break;
                }
            }
            let future = future.unwrap_or_else(|| {
                actor
                    .turn_ticket(actor.snapshot().active_turn.unwrap())
                    .unwrap()
            });
            assert_eq!(app.status, crate::components::Status::Streaming);
            assert!(actor.cancel_turn(cancelled_turn_id).is_err());
            assert!(future.peek().is_none());
            release.send(()).unwrap();
            assert!(matches!(future.wait().await, TurnOutcome::Completed { .. }));
            actor.close();
            assert!(
                manager
                    .shutdown(SHUTDOWN_TIMEOUT)
                    .await
                    .timed_out
                    .is_empty()
            );
        },
        async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("buffered-root cancellation regression timed out");
        },
    ));
}

#[test_case(false; "root")]
#[test_case(true; "child")]
fn ordinary_submission_paths_have_runtime_parity(child: bool) {
    smol::block_on(futures_lite::future::or(
        async {
            const INITIAL: &str = "initial user instruction";
            const FOLLOWUPS: [&str; 2] =
                ["first deferred instruction", "second deferred instruction"];
            const IMAGE_BYTES: usize = 256 * 1024 + 1;
            const SHELL_CONTEXT: &str = "shell preamble output";
            const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYIIA";
            let image = makima_agent::ImageSource::new(
                makima_agent::ImageMediaType::Png,
                Arc::from(format!("{PNG}{}", "AAAA".repeat(IMAGE_BYTES / 4))),
            );
            let (backend, events) = BackendFixture::new();
            let (requests_tx, requests) = flume::unbounded();
            let (release, released) = flume::bounded(1);
            let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let config = makima_agent::EffectiveAgentConfig::new(
                makima_agent::RunSettings {
                    provider: Arc::new(QueueProvider {
                        requests: requests_tx,
                        release: released,
                    }),
                    model: crate::components::test_model(),
                    fast: false,
                    workflow: false,
                    thinking: Default::default(),
                },
                AgentMode::Build,
            )
            .with_mode_def(makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build));
            let target = if child {
                let (parent_backend, _parent_events) = BackendFixture::new();
                let parent = manager
                    .create_root_with_config(Some(config.clone()), Vec::new(), None, |id| {
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(parent_backend.build(id)))
                    })
                    .await
                    .unwrap();
                manager
                    .spawn_child_trusted_with_config(
                        &parent,
                        config,
                        Default::default(),
                        Vec::new(),
                        None,
                        |agent_id| {
                            Ok::<Box<dyn ActorBackend>, String>(Box::new(
                                backend.build_child(agent_id),
                            ))
                        },
                    )
                    .await
                    .unwrap()
            } else {
                manager
                    .create_root_with_config(Some(config), Vec::new(), None, |agent_id| {
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(agent_id)))
                    })
                    .await
                    .unwrap()
            };
            let actor = target.actor().unwrap();
            let queue = actor_queue(Arc::new(actor.clone()));
            let settlements = actor.subscribe_events();
            let mut app = crate::app::tests::actor_presentation_app(target.clone(), 0);
            let submission = |text: &str| {
                let mut input = AgentInput::new(text.into(), vec![image.clone()]);
                input.preamble = vec![Message::observation(format!("{SHELL_CONTEXT}: {text}"))];
                QueueItem {
                    input,
                    displayed: None,
                }
            };
            queue.push(submission(INITIAL));
            let first = requests.recv_async().await.unwrap();
            let submitted_run = actor.presentation_id();
            assert!(serde_json::to_string(&first).unwrap().contains(INITIAL));
            let ticket = actor
                .turn_ticket(actor.snapshot().active_turn.unwrap())
                .unwrap();
            assert_eq!(
                ticket.provenance().origin,
                makima_agent::actor::TurnOrigin::User
            );
            for text in FOLLOWUPS {
                queue.push(submission(text));
            }
            assert_eq!(queue.text_messages(), FOLLOWUPS);
            release.send(()).unwrap();
            let next = requests.recv_async().await.unwrap();
            let serialized = serde_json::to_string(&next).unwrap();
            let mut previous = 0;
            for text in [INITIAL, FOLLOWUPS[0], FOLLOWUPS[1]] {
                let preamble = format!("{SHELL_CONTEXT}: {text}");
                assert_eq!(serialized.matches(&preamble).count(), 1);
                let preamble_index = next
                    .iter()
                    .position(|message| {
                        message
                            .first_text_content()
                            .is_some_and(|content| content.contains(&preamble))
                    })
                    .unwrap();
                let (index, message) = next
                    .iter()
                    .enumerate()
                    .find(|(_, message)| {
                        message
                            .content
                            .iter()
                            .any(|block| matches!(block, ContentBlock::Image { .. }))
                            && message
                                .first_text_content()
                                .is_some_and(|content| content.contains(text))
                    })
                    .expect("image prompt missing from the follow-up request");
                assert_eq!(index, preamble_index + 1);
                assert!(index > previous);
                previous = index;
                if text != INITIAL {
                    let wrapped = message.first_text_content().unwrap();
                    assert!(wrapped.starts_with("<user-interrupt>\n"));
                    assert!(wrapped.ends_with("\n</user-interrupt>"));
                    assert_eq!(message.display_text.as_deref(), Some(text));
                }
            }
            assert_eq!(serialized.matches(image.data.as_ref()).count(), 3);
            assert_eq!(
                serde_json::to_string(&first)
                    .unwrap()
                    .matches(image.data.as_ref())
                    .count(),
                1
            );
            assert_eq!(actor.snapshot().active_turn, Some(ticket.turn_id()));
            assert!(queue.is_empty());
            release.send(()).unwrap();
            assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
            // The actor itself publishes the settled identity once its queue
            // is empty; the frontend never re-derives it from a mirror.
            let settled = await_settlement(&settlements, submitted_run).await;
            assert_eq!(
                settled.agent_id,
                actor.agent_id(),
                "settlement names the actor that ran the work"
            );
            for envelope in events.drain() {
                assert_eq!(envelope.presentation(), Some(submitted_run));
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
            }
            let chat = app.main_chat();
            for text in [INITIAL, FOLLOWUPS[0], FOLLOWUPS[1]] {
                assert_eq!(
                    (0..chat.message_count())
                        .filter(|&i| chat
                            .message_at(i)
                            .is_some_and(|message| message.text == text))
                        .count(),
                    1
                );
            }
            actor.close();
            assert!(
                manager
                    .shutdown(SHUTDOWN_TIMEOUT)
                    .await
                    .timed_out
                    .is_empty()
            );
        },
        async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("production queue folding timed out");
        },
    ));
}

async fn provider_target(
    manager: &AgentManagerHandle,
    backend: BackendFixture,
    provider: Arc<dyn makima_providers::provider::Provider>,
    child: bool,
) -> AgentRef {
    let config = EffectiveAgentConfig::new(
        RunSettings {
            provider,
            model: crate::components::test_model(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        },
        AgentMode::Build,
    )
    .with_mode_def(makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build));
    if child {
        let (parent_backend, _parent_events) = BackendFixture::new();
        let parent = manager
            .create_root_with_config(Some(config.clone()), Vec::new(), None, |id| {
                Ok::<Box<dyn ActorBackend>, String>(Box::new(parent_backend.build(id)))
            })
            .await
            .unwrap();
        manager
            .spawn_child_trusted_with_config(
                &parent,
                config,
                Default::default(),
                Vec::new(),
                None,
                |id| Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build_child(id))),
            )
            .await
            .unwrap()
    } else {
        manager
            .create_root_with_config(Some(config), Vec::new(), None, |id| {
                Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(id)))
            })
            .await
            .unwrap()
    }
}

#[test_case(false, false; "root_provider_error")]
#[test_case(true, false; "child_provider_error")]
#[test_case(false, true; "root_request_budget")]
#[test_case(true, true; "child_request_budget")]
fn shared_instruction_failure_and_budget_outcomes(child: bool, budget: bool) {
    const INITIAL: &str = "request before steering";
    const STEERING: &str = "accepted but omitted steering";
    const FAILURE: &str = "gated provider failure";
    smol::block_on(futures_lite::future::or(
        async {
            let (mut backend, events) = BackendFixture::new();
            backend.params.config.max_turns = budget.then_some(1);
            let input = AgentInput::new(INITIAL.into(), Vec::new());
            let (requests_tx, requests) = flume::unbounded();
            let (release, released) = flume::bounded(1);
            let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let target = provider_target(
                &manager,
                backend,
                Arc::new(FailingQueueProvider {
                    requests: requests_tx,
                    release: released,
                    fail: !budget,
                }),
                child,
            )
            .await;
            let actor = target.actor().unwrap();
            let first = actor
                .submit(input, None, String::new(), Default::default())
                .unwrap();
            let request = requests.recv_async().await.unwrap();
            assert!(serde_json::to_string(&request).unwrap().contains(INITIAL));
            let joined = actor
                .submit(
                    AgentInput::new(STEERING.into(), Vec::new()),
                    None,
                    String::new(),
                    Default::default(),
                )
                .unwrap();
            assert_eq!(joined.turn_id(), first.turn_id());
            assert!(!serde_json::to_string(&request).unwrap().contains(STEERING));
            release.send(()).unwrap();
            let outcome = first.wait().await;
            assert!(matches!(&outcome, TurnOutcome::Failed { failure, .. }
            if if budget { failure.diagnostic.contains("request budget") }
               else { failure.diagnostic.contains(FAILURE) }));
            assert_eq!(joined.wait().await, outcome);
            assert!(requests.try_recv().is_err());
            let outcomes: Vec<_> = events
                .try_iter()
                .filter_map(|envelope| match envelope.event {
                    AgentEvent::TurnOutcome(outcome) => Some(outcome),
                    _ => None,
                })
                .collect();
            assert_eq!(outcomes, [outcome]);
            assert!(manager.shutdown(TEST_TIMEOUT).await.timed_out.is_empty());
        },
        async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("accepted steering failure did not settle");
        },
    ));

    struct FailingQueueProvider {
        requests: flume::Sender<Vec<Message>>,
        release: flume::Receiver<()>,
        fail: bool,
    }
    impl makima_providers::provider::Provider for FailingQueueProvider {
        fn stream_message<'a>(
            &'a self,
            _: &'a Model,
            messages: &'a [Message],
            _: &'a str,
            _: &'a serde_json::Value,
            _: &'a flume::Sender<makima_providers::ProviderEvent>,
            _: makima_providers::RequestOptions,
            _: Option<&'a SessionRef>,
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> makima_providers::provider::BoxFuture<
            'a,
            Result<makima_providers::StreamResponse, AgentError>,
        > {
            Box::pin(async move {
                self.requests.send(messages.to_vec()).unwrap();
                self.release.recv_async().await.unwrap();
                if self.fail {
                    return Err(AgentError::Config {
                        message: FAILURE.into(),
                    });
                }
                Ok(makima_providers::StreamResponse {
                    message: Message {
                        role: makima_providers::Role::Assistant,
                        content: vec![ContentBlock::Text {
                            text: EXPLICIT_RESPONSE.into(),
                        }],
                        ..Default::default()
                    },
                    usage: Default::default(),
                    stop_reason: Some(makima_providers::StopReason::EndTurn),
                })
            })
        }
    }
}

#[test_case(false; "main_does_not_join")]
#[test_case(true; "nonmain_joins")]
fn completion_policy_main_vs_nonmain(child: bool) {
    smol::block_on(futures_lite::future::or(
        async {
            let manager = makima_agent::AgentManagerHandle::new(makima_agent::AgentLimits {
                max_concurrent_agent_turns: 1,
                ..Default::default()
            })
            .unwrap();
            let (backend, _events) = BackendFixture::new();
            let input = AgentInput::new("hello".into(), Vec::new());
            let (release, released) = flume::bounded(1);
            let (requests_tx, requests) = flume::unbounded();
            let target = provider_target(
                &manager,
                backend,
                Arc::new(CompletionProvider {
                    inner: ExplicitProvider { release: released },
                    requests: requests_tx,
                }),
                child,
            )
            .await;
            let actor = target.actor().unwrap();
            let ticket = actor
                .submit(input, None, String::new(), Default::default())
                .unwrap();
            requests.recv_async().await.unwrap();
            let (reporter_backend, _reporter_events) = BackendFixture::new();
            let reporter_input = AgentInput::new("hello".into(), Vec::new());
            let (report_release, report_released) = flume::bounded(1);
            let (report_requests_tx, report_requests) = flume::unbounded();
            let mut config = (*actor.effective_config().unwrap()).clone();
            config.settings.provider = Arc::new(CompletionProvider {
                inner: ExplicitProvider {
                    release: report_released,
                },
                requests: report_requests_tx,
            });
            let reporter = manager
                .spawn_child_trusted_with_config(
                    &target,
                    config,
                    Default::default(),
                    Vec::new(),
                    None,
                    |id| {
                        Ok::<Box<dyn ActorBackend>, String>(Box::new(
                            reporter_backend.build_child(id),
                        ))
                    },
                )
                .await
                .unwrap();
            let reporter_actor = reporter.actor().unwrap();
            reporter_actor.configure_parent_delivery(true).unwrap();
            let report_ticket = reporter_actor
                .submit(reporter_input, None, String::new(), Default::default())
                .unwrap();
            assert!(report_requests.try_recv().is_err());
            release.send(()).unwrap();
            report_requests.recv_async().await.unwrap();
            assert!(report_ticket.peek().is_none());
            if child {
                assert!(
                    ticket.peek().is_none(),
                    "non-main settled before its reporting child"
                );
            } else {
                assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
            }
            report_release.send(()).unwrap();
            assert!(matches!(
                report_ticket.wait().await,
                TurnOutcome::Completed { .. }
            ));
            let continuation = requests.recv_async().await.unwrap();
            let serialized = serde_json::to_string(&continuation).unwrap();
            assert!(serialized.contains(&report_ticket.turn_id().to_string()));
            assert!(serialized.contains(EXPLICIT_RESPONSE));
            release.send(()).unwrap();
            assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
            assert!(manager.shutdown(TEST_TIMEOUT).await.timed_out.is_empty());
            assert!(requests.try_recv().is_err());
        },
        async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("main/non-main completion policy did not settle");
        },
    ));
}

struct QueueProvider {
    requests: flume::Sender<Vec<Message>>,
    release: flume::Receiver<()>,
}

impl makima_providers::provider::Provider for QueueProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a serde_json::Value,
        _: &'a flume::Sender<makima_providers::ProviderEvent>,
        _: makima_providers::RequestOptions,
        _: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
        _delivery: RequestDelivery,
    ) -> makima_providers::provider::BoxFuture<
        'a,
        Result<makima_providers::StreamResponse, AgentError>,
    > {
        Box::pin(async move {
            self.requests.send(messages.to_vec()).unwrap();
            self.release.recv_async().await.unwrap();
            Ok(makima_providers::StreamResponse {
                message: Message {
                    role: makima_providers::Role::Assistant,
                    content: vec![makima_providers::ContentBlock::Text {
                        text: "request completed".into(),
                    }],
                    ..Default::default()
                },
                usage: Default::default(),
                stop_reason: Some(makima_providers::StopReason::EndTurn),
            })
        })
    }
}

#[test]
fn ui_submission_fires_one_turn_start() {
    smol::block_on(async {
        const PROMPT: &str = "immediate UI prompt";
        const RESPONSE: &str = "visible explicit response";
        let (backend, events) = BackendFixture::new();
        let (release, released) = flume::bounded(1);
        let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
        let root = manager
            .create_root_with_config(
                Some(
                    makima_agent::EffectiveAgentConfig::new(
                        makima_agent::RunSettings {
                            provider: Arc::new(ExplicitProvider { release: released }),
                            model: crate::components::test_model(),
                            fast: false,
                            workflow: false,
                            thinking: Default::default(),
                        },
                        AgentMode::Build,
                    )
                    .with_mode_def(
                        makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build),
                    ),
                ),
                Vec::new(),
                None,
                |agent_id| Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(agent_id))),
            )
            .await
            .unwrap();
        let actor = root.actor().unwrap();
        let lifecycle = actor.subscribe_events();
        let manager = root.manager();
        let mut app = crate::app::tests::actor_presentation_app(root, 0);
        let host = makima_lua::PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
        host.load_source(
            "turn_start_count",
            r#"
            local starts = 0
            makima.api.create_autocmd("TurnStart", { callback = function() starts += 1 end })
            makima.api.register_command({ name = "/assert_single_start", tui_only = false, handler = function() assert(starts == 1, "TurnStart count: " .. starts) end })
            "#,
        )
        .unwrap();
        app.lua_event_handle = host.event_handle();
        app.lua_event_handle.observe_agents(&manager);
        host.wait_for_worker_barrier_for_test();
        app.update(crate::app::Msg::Paste(PROMPT.into()));
        let actions = app.update(crate::app::Msg::Key(crate::components::key(
            crossterm::event::KeyCode::Enter,
        )));
        assert!(actions.is_empty());
        let submitted_run = app.run_id;
        loop {
            let envelope = futures_lite::future::or(events.recv_async(), async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("waiting for immediate response");
            })
            .await
            .unwrap();
            assert_eq!(envelope.presentation(), Some(submitted_run));
            let streamed = matches!(&envelope.event, AgentEvent::TextDelta { .. });
            app.update(crate::app::Msg::Agent(Box::new(envelope)));
            if streamed {
                break;
            }
        }
        release.send(()).unwrap();
        // Settlement is the actor's own publication: wait for its End
        // rather than racing a non-blocking drain.
        futures_lite::future::or(
            async {
                loop {
                    match lifecycle.recv_async().await {
                        Ok(makima_agent::actor::ActorEvent::End(_)) => break,
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
            },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("waiting for actor settlement: actor={:?}", actor.snapshot());
            },
        )
        .await;
        for envelope in events.drain() {
            assert_eq!(envelope.presentation(), Some(submitted_run));
            app.update(crate::app::Msg::Agent(Box::new(envelope)));
        }
        assert_eq!(app.run_id, submitted_run);
        host.drain_tasks_for_test();
        assert_eq!(
            run_command(&host.command_registry(), "/assert_single_start", 0, None)
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        let chat = app.main_chat();
        assert_eq!(
            (0..chat.message_count())
                .filter(|&index| chat.message_at(index).is_some_and(|m| m.text == PROMPT))
                .count(),
            1
        );
        crate::app::tests::assert_actor_response(&mut app, RESPONSE);
        actor.close();
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(true, false, false; "enabled")]
#[test_case(false, false, false; "disabled")]
#[test_case(true, true, false; "restricted_parent")]
#[test_case(true, true, true; "plan_parent")]
#[test_case(false, true, true; "plan_parent_mcp_disabled")]
fn command_spawn_uses_production_mcp_template(enabled: bool, restricted: bool, plan: bool) {
    smol::block_on(async {
        const COMMAND: &str = "/ui-mcp-spawn";
        const MCP_EXECUTED: &str = "fake MCP executed";
        const MCP_DENIED: &str = "MCP denied";
        let expected = if enabled { MCP_EXECUTED } else { MCP_DENIED };
        let (mut backend, _events) = BackendFixture::new();
        let host = makima_lua::PluginHost::new(Arc::clone(ToolRegistry::global_arc())).unwrap();
        backend.options.prompt_slots = host.event_handle().prompt_slot_source();
        backend.params.modes = host.event_handle().mode_registry();
        backend.params.permissions.set_yolo(true);
        backend.options.mcp = Some(makima_agent::mcp::test_support::stub_session_with_result(
            &[("stub.echo", "echo")],
            MCP_EXECUTED,
        ));
        let mut config = EffectiveAgentConfig::new(
            makima_agent::RunSettings {
                provider: Arc::new(CommandMcpProvider),
                model: crate::components::test_model(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        )
        .with_mode_def(ModeRegistry::builtin().get(&makima_agent::ModeId::Build))
        .with_context(AgentContextSnapshot::new(PathBuf::from(TEST_CWD)));
        if restricted {
            let mut mode = makima_agent::ModeRegistry::builtin()
                .get(&makima_agent::ModeId::Build)
                .unwrap();
            mode.restrict_write_to = Some(PathBuf::from("/tmp/ui-mcp-plan.md"));
            config = config.with_mode_def(Some(mode));
        }
        if plan {
            config.mode = AgentMode::Plan(PathBuf::from("/tmp/ui-mcp-plan.md"));
            config = config.with_mode_def(
                makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Plan),
            );
        }
        let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
        let root_id = AgentId::generate();
        let backend = backend.build(root_id);
        let template = backend.template(&config);
        let root = manager
            .create_root_with_id_and_config(root_id, Some(config), Vec::new(), None, |_| {
                Ok::<Box<dyn ActorBackend>, String>(Box::new(backend))
            })
            .await
            .unwrap();
        let orchestration = Arc::new(makima_lua::orchestration::AgentOrchestration::new(
            Arc::new(arc_swap::ArcSwapOption::empty()),
        ));
        let pinned = TrustedTarget {
            target: root.clone(),
            template,
        };
        orchestration.register_root(pinned.clone(), None);
        let pinned = TrustedTarget {
            target: root.clone(),
            template: makima_lua::orchestration::OrchestrationServices::template(
                &*orchestration,
                &root,
            )
            .unwrap(),
        };
        orchestration.select(Some(root.clone()));
        host.event_handle()
            .install_orchestration_services(orchestration);
        host.load_source(
            "ui_mcp_spawn",
            &format!(
                r#"
            makima.api.register_command({{
                name = "{COMMAND}", tui_only = false,
                handler = function(_, ctx)
                    local ok, err = pcall(function()
                        assert(ctx ~= nil)
                        local child = assert(makima.agent.spawn(ctx, {{
                            inherit_provider = true, silent = true, mcp = {enabled},
                        }}))
                        local result = assert(assert(child:send(ctx, "invoke MCP", {{ when = "idle" }})):wait(ctx))
                        assert(result.text == "{expected}", makima.json.encode(result))
                        assert(child:close(ctx))
                        makima.ui.flash(result.text)
                    end)
                    if not ok then makima.ui.flash(tostring(err)) end
                end,
            }})
        "#,
            ),
        )
        .unwrap();
        let actions = host.ui_action_rx();
        let commands = host.command_registry();
        let target = commands.bind_target(
            makima_commands::TargetCapabilities::ALL,
            Arc::new(McpCommandHost(pinned)),
        );
        let outcome =
            futures_lite::future::or(commands.dispatch_input(&target, COMMAND.into()), async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("command-origin MCP spawn timed out");
            })
            .await;
        assert!(
            matches!(
                outcome,
                makima_commands::InputDispatch::Dispatched(
                    makima_commands::CommandOutcome::Completed
                )
            ),
            "{outcome:?}"
        );
        let action = futures_lite::future::or(actions.recv_async(), async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("command-origin MCP handler did not finish");
        })
        .await
        .unwrap();
        let makima_lua::UiAction::Flash(text) = action else {
            panic!("unexpected command-origin MCP action");
        };
        assert_eq!(text, expected);
        root.actor().unwrap().close();
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

struct McpCommandHost(makima_lua::orchestration::TrustedTarget);

impl makima_commands::CommandHost for McpCommandHost {
    fn admission_context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        Some(Arc::new(self.0.clone()))
    }

    fn request(
        &self,
        _: makima_commands::HostRequest,
    ) -> makima_commands::CommandFuture<
        Result<makima_commands::HostResponse, makima_commands::CommandError>,
    > {
        Box::pin(async { Ok(makima_commands::HostResponse::Completed) })
    }
}

struct CommandMcpProvider;

impl makima_providers::provider::Provider for CommandMcpProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a serde_json::Value,
        events: &'a flume::Sender<makima_providers::ProviderEvent>,
        _: makima_providers::RequestOptions,
        _: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
        _delivery: RequestDelivery,
    ) -> makima_providers::provider::BoxFuture<
        'a,
        Result<makima_providers::StreamResponse, AgentError>,
    > {
        Box::pin(async move {
            let serialized = serde_json::to_string(messages).unwrap();
            let answered = serialized.contains("tool_result");
            let text = if serialized.contains("fake MCP executed") {
                "fake MCP executed"
            } else {
                "MCP denied"
            };
            if answered {
                events
                    .send(makima_providers::ProviderEvent::TextDelta { text: text.into() })
                    .unwrap();
            }
            Ok(makima_providers::StreamResponse {
                message: Message {
                    role: makima_providers::Role::Assistant,
                    content: if answered {
                        vec![makima_providers::ContentBlock::Text { text: text.into() }]
                    } else {
                        vec![makima_providers::ContentBlock::ToolUse {
                            id: "ui-mcp-call".into(),
                            name: "stub.echo".into(),
                            input: serde_json::json!({}),
                            thought_signature: None,
                        }]
                    },
                    ..Default::default()
                },
                usage: Default::default(),
                stop_reason: Some(if answered {
                    makima_providers::StopReason::EndTurn
                } else {
                    makima_providers::StopReason::ToolUse
                }),
            })
        })
    }
}

#[test_case(false; "idle_continuation")]
#[test_case(true; "active_parent_boundary")]
fn child_completion_reaches_production_tui_presentation(active: bool) {
    smol::block_on(async {
        const RUN_ID: u64 = 7;
        const CHILD_INPUT: &str = "child work";
        futures_lite::future::or(async {
            let (backend, events) = BackendFixture::new();
            let input = AgentInput::new("hello".into(), Vec::new());
            let (release, released) = flume::unbounded();
            let (requests, request_rx) = flume::unbounded();
            let config = makima_agent::EffectiveAgentConfig::new(
                makima_agent::RunSettings {
                    provider: Arc::new(CompletionProvider {
                        inner: ExplicitProvider { release: released }, requests,
                    }),
                    model: crate::components::test_model(),
                    fast: false, workflow: false, thinking: Default::default(),
                }, AgentMode::Build,
            ).with_mode_def(makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build));
            let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let root = manager.create_root_with_config(
                Some(config.clone()), Vec::new(), None, |agent_id| {
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(agent_id)))
                },
            ).await.unwrap();
            let actor = root.actor().unwrap();
            actor.observe_presentation(RUN_ID - 1);
            let mut app =
                crate::app::tests::actor_presentation_app(root.clone(), RUN_ID - 1);
            let actor_events = actor.subscribe_events();
            let parent = active.then(|| actor.admit_turn(input, None, String::new()).unwrap());
            if active {
                request_rx.recv_async().await.unwrap();
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    let streamed = matches!(envelope.event, AgentEvent::TextDelta { .. });
                    app.update(crate::app::Msg::Agent(Box::new(envelope)));
                    if streamed { break; }
                }
            }
            let (child_backend, _child_events) = BackendFixture::new();
    let child_input = AgentInput::new(CHILD_INPUT.into(), Vec::new());
            let (child_release, child_released) = flume::unbounded();
            child_release.send(()).unwrap();
            let mut child_config = config;
            child_config.settings.provider = Arc::new(ExplicitProvider { release: child_released });
            let child = manager.spawn_child_trusted_with_config(
                &root, child_config, Default::default(), Vec::new(), None, |agent_id| {
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(child_backend.build_child(agent_id)))
                },
            ).await.unwrap();
            let child_actor = child.actor().unwrap();
            child_actor.configure_parent_delivery(true).unwrap();
            let child_ticket = child_actor.admit_turn(child_input, None, String::new()).unwrap();
            let outcome = child_ticket.wait().await;
            assert!(matches!(outcome, TurnOutcome::Completed { .. }), "{outcome:?}");
            if active { release.send(()).unwrap(); }
            let request = serde_json::to_string(&request_rx.recv_async().await.unwrap()).unwrap();
            assert!(request.contains(EXPLICIT_RESPONSE), "missing child observation: {request}");
            assert!(request.contains(&child_ticket.turn_id().to_string()), "missing child identity: {request}");
            assert!(!request.contains(CHILD_INPUT));
            let mut bubbles = 0;
            loop {
                let envelope = events.recv_async().await.unwrap();
                assert_eq!(envelope.presentation(), Some(RUN_ID));
                bubbles += usize::from(matches!(envelope.event, AgentEvent::QueueItemConsumed { .. }));
                let streamed = matches!(envelope.event, AgentEvent::TextDelta { .. });
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if streamed { break; }
            }
            assert_eq!(bubbles, 0);
            assert_eq!(app.run_id, RUN_ID);
            assert_eq!(app.status, crate::components::Status::Streaming);
            release.send(()).unwrap();
            loop {
                let envelope = events.recv_async().await.unwrap();
                let terminal = matches!(envelope.event, AgentEvent::TurnOutcome(_));
                app.update(crate::app::Msg::Agent(Box::new(envelope)));
                if terminal { break; }
            }
            let expected = if active {
                EXPLICIT_RESPONSE.repeat(2)
            } else {
                EXPLICIT_RESPONSE.into()
            };
            crate::app::tests::assert_actor_response(&mut app, &expected);
            if let Some(parent) = parent {
                assert!(matches!(parent.wait().await, TurnOutcome::Completed { .. }));
            }
            let mut consumed = 0;
            for event in actor_events.try_iter() {
                if matches!(&event, makima_agent::actor::ActorEvent::Delivery(delivery)
                    if matches!(delivery.status, makima_agent::actor::DeliveryStatus::Executed { .. })) {
                    consumed += 1;
                }
                app.handle_actor_event(event);
            }
            assert_eq!(consumed, 1);
            manager.shutdown(TEST_TIMEOUT).await;
        }, async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("child completion did not reach the production TUI path");
        }).await;
    });
}

#[test_case(false; "root")]
#[test_case(true; "child")]
fn shared_consumption_wrapper_and_request_order(child: bool) {
    smol::block_on(async {
        futures_lite::future::or(async {
            let (backend, _events) = BackendFixture::new();
            let input = AgentInput::new("hello".into(), Vec::new());
            let (release, released) = flume::unbounded();
            let (requests, request_rx) = flume::unbounded();
            let config = makima_agent::EffectiveAgentConfig::new(
                makima_agent::RunSettings {
                    provider: Arc::new(ToolBatchProvider { release: released, requests }),
                    model: crate::components::test_model(),
                    fast: false, workflow: false, thinking: Default::default(),
                }, AgentMode::Build,
            ).with_mode_def(makima_agent::ModeRegistry::builtin().get(&makima_agent::ModeId::Build));
            let manager = makima_agent::AgentManagerHandle::new(Default::default()).unwrap();
            let root = provider_target(&manager, backend, Arc::clone(&config.settings.provider), child).await;
            let actor = root.actor().unwrap();
            let actor_events = actor.subscribe_events();
            let parent = actor.submit(input, None, String::new(), Default::default()).unwrap();
            request_rx.recv_async().await.unwrap();
            const STEERING: &str = "steering during tool batch";
            let steering = actor.submit(AgentInput::new(STEERING.into(), Vec::new()), None, String::new(), Default::default()).unwrap();
            assert_eq!(steering.turn_id(), parent.turn_id());

            let (child_backend, _child_events) = BackendFixture::new();
    let child_input = AgentInput::new("hello".into(), Vec::new());
            let (child_release, child_released) = flume::unbounded();
            child_release.send(()).unwrap();
            let mut child_config = config;
            child_config.settings.provider = Arc::new(ExplicitProvider { release: child_released });
            let child = manager.spawn_child_trusted_with_config(
                &root, child_config, Default::default(), Vec::new(), None, |agent_id| {
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(child_backend.build_child(agent_id)))
                },
            ).await.unwrap();
            let child_actor = child.actor().unwrap();
            child_actor.configure_parent_delivery(true).unwrap();
            let child_ticket = child_actor.admit_turn(child_input, None, String::new()).unwrap();
            let outcome = child_ticket.wait().await;
            assert!(matches!(outcome, TurnOutcome::Completed { .. }), "{outcome:?}");
            let mut seen = Vec::new();
            loop {
                let event = actor_events.recv_async().await.unwrap();
                let accepted = matches!(&event, ActorEvent::Delivery(delivery)
                    if delivery.status == DeliveryStatus::Accepted
                        && delivery.delivery.result.outcome.turn_id() == child_ticket.turn_id());
                seen.push(event);
                if accepted { break; }
            }
            release.send(()).unwrap();

            let request = request_rx.recv_async().await.unwrap();
            let child_turn = child_ticket.turn_id().to_string();
            let tool_result = request.iter().position(|message| message.content.iter().any(|block| {
                matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == TOOL_BATCH_CALL_ID)
            })).expect("tool result missing from the follow-up request");
            let observation = request.iter().position(|message| {
                let serialized = serde_json::to_string(message).unwrap();
                serialized.contains(&child_turn) && serialized.contains(EXPLICIT_RESPONSE)
            }).expect("child observation missing from the follow-up request");
            let wrapped: Vec<_> = request.iter().enumerate().filter(|(_, message)| {
                message.display_text.as_deref() == Some(STEERING)
            }).collect();
            let [(steering_index, steering_message)] = wrapped.as_slice() else {
                panic!("steering was not delivered exactly once");
            };
            let text = steering_message.first_text_content().unwrap();
            assert!(text.starts_with("<user-interrupt>\n"));
            assert!(text.ends_with("\n</user-interrupt>"));
            assert!(tool_result < *steering_index);
            assert!(observation > *steering_index, "observation must follow tool results and steering: {request:?}");

            assert!(matches!(parent.wait().await, TurnOutcome::Completed { .. }));
            assert!(matches!(steering.wait().await, TurnOutcome::Completed { .. }));
            manager.shutdown(TEST_TIMEOUT).await;
            assert!(request_rx.try_recv().is_err(), "parent made an extra provider request");
            seen.extend(actor_events.try_iter());
            let started: Vec<TurnId> = seen.iter().filter_map(|event| match event {
                ActorEvent::Start { turn_id, .. } => Some(*turn_id),
                _ => None,
            }).collect();
            assert_eq!(started, [parent.turn_id()]);
            let executed: Vec<TurnId> = seen.iter().filter_map(|event| match event {
                ActorEvent::Delivery(delivery) => match delivery.status {
                    DeliveryStatus::Executed { parent_turn_id } => Some(parent_turn_id),
                    _ => None,
                },
                _ => None,
            }).collect();
            assert_eq!(executed, [parent.turn_id()]);
        }, async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("child completion did not reach the parent's follow-up request");
        }).await;
    });
}

struct ToolBatchProvider {
    release: flume::Receiver<()>,
    requests: flume::Sender<Vec<Message>>,
}

impl makima_providers::provider::Provider for ToolBatchProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a serde_json::Value,
        _: &'a flume::Sender<makima_providers::ProviderEvent>,
        _: makima_providers::RequestOptions,
        _: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
        _delivery: RequestDelivery,
    ) -> makima_providers::provider::BoxFuture<
        'a,
        Result<makima_providers::StreamResponse, AgentError>,
    > {
        Box::pin(async move {
            self.requests.send(messages.to_vec()).unwrap();
            let answered = messages.iter().any(|message| {
                message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
            });
            let (content, stop_reason) = if answered {
                (
                    ContentBlock::Text {
                        text: EXPLICIT_RESPONSE.into(),
                    },
                    makima_providers::StopReason::EndTurn,
                )
            } else {
                self.release.recv_async().await.unwrap();
                (
                    ContentBlock::ToolUse {
                        id: TOOL_BATCH_CALL_ID.into(),
                        name: TOOL_BATCH_TOOL.into(),
                        input: serde_json::json!({}),
                        thought_signature: None,
                    },
                    makima_providers::StopReason::ToolUse,
                )
            };
            Ok(makima_providers::StreamResponse {
                message: Message {
                    role: makima_providers::Role::Assistant,
                    content: vec![content],
                    ..Default::default()
                },
                usage: Default::default(),
                stop_reason: Some(stop_reason),
            })
        })
    }
}

struct CompletionProvider {
    inner: ExplicitProvider,
    requests: flume::Sender<Vec<Message>>,
}

impl makima_providers::provider::Provider for CompletionProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a serde_json::Value,
        events: &'a flume::Sender<makima_providers::ProviderEvent>,
        options: makima_providers::RequestOptions,
        session: Option<&'a SessionRef>,
        invocation: &'a Invocation,
        delivery: RequestDelivery,
    ) -> makima_providers::provider::BoxFuture<
        'a,
        Result<makima_providers::StreamResponse, AgentError>,
    > {
        self.requests.send(messages.to_vec()).unwrap();
        self.inner.stream_message(
            model, messages, system, tools, events, options, session, invocation, delivery,
        )
    }
}

struct ExplicitProvider {
    release: flume::Receiver<()>,
}

impl makima_providers::provider::Provider for ExplicitProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [Message],
        _: &'a str,
        _: &'a serde_json::Value,
        events: &'a flume::Sender<makima_providers::ProviderEvent>,
        _: makima_providers::RequestOptions,
        _: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
        _delivery: RequestDelivery,
    ) -> makima_providers::provider::BoxFuture<
        'a,
        Result<makima_providers::StreamResponse, AgentError>,
    > {
        Box::pin(async move {
            events
                .send(makima_providers::ProviderEvent::TextDelta {
                    text: EXPLICIT_RESPONSE.into(),
                })
                .unwrap();
            self.release.recv_async().await.unwrap();
            Ok(makima_providers::StreamResponse {
                message: Message {
                    role: makima_providers::Role::Assistant,
                    content: vec![makima_providers::ContentBlock::Text {
                        text: EXPLICIT_RESPONSE.into(),
                    }],
                    ..Default::default()
                },
                usage: Default::default(),
                stop_reason: Some(makima_providers::StopReason::EndTurn),
            })
        })
    }
}

#[test_case(false; "success")]
#[test_case(true; "cancelled")]
fn idle_compaction_adopts_presentation_and_displays_progress(cancel: bool) {
    smol::block_on(async {
        const PREVIOUS_PRESENTATION: u64 = 7;
        let (mut backend, events) = BackendFixture::new();
        backend.params.model_policy = Arc::new(ModelPolicy::new(&[], &["*".into()]).unwrap());
        let (release, released) = flume::bounded(1);
        let config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::new(ExplicitProvider { release: released }),
                model: crate::components::test_model(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            AgentMode::Build,
        );
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let root = manager
            .create_root_with_config(
                Some(config),
                vec![Message::user("summarize this conversation".into())],
                None,
                |agent_id| Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(agent_id))),
            )
            .await
            .unwrap();
        let actor = root.actor().unwrap();
        actor.observe_presentation(PREVIOUS_PRESENTATION);
        let actor_events = actor.subscribe_events();
        let mut app =
            crate::app::tests::actor_presentation_app(root.clone(), PREVIOUS_PRESENTATION);
        actor.push_compact(PREVIOUS_PRESENTATION, None).unwrap();
        let presentation = PREVIOUS_PRESENTATION + 1;
        loop {
            let envelope = events.recv_timeout(TEST_TIMEOUT).unwrap();
            assert_eq!(envelope.presentation(), Some(presentation));
            let streamed = matches!(envelope.event, AgentEvent::TextDelta { .. });
            app.update(crate::app::Msg::Agent(Box::new(envelope)));
            assert_eq!(app.run_id, presentation);
            assert_eq!(app.status, crate::components::Status::Streaming);
            if streamed {
                assert!(!app.main_chat().streaming_text_is_empty());
                break;
            }
        }
        if cancel {
            actor.cancel_existing();
        } else {
            release.send(()).unwrap();
        }
        loop {
            let envelope = events.recv_timeout(TEST_TIMEOUT).unwrap();
            let finished = matches!(
                envelope.event,
                AgentEvent::ControlComplete { .. } | AgentEvent::ControlError { .. }
            );
            app.update(crate::app::Msg::Agent(Box::new(envelope)));
            if finished {
                break;
            }
        }
        if cancel {
            assert!(matches!(
                app.status,
                crate::components::Status::Error { .. }
            ));
        } else {
            assert_eq!(app.status, crate::components::Status::Idle);
            crate::app::tests::assert_actor_response(&mut app, EXPLICIT_RESPONSE);
        }
        await_settlement(&actor_events, presentation).await;
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

#[test_case(false, false; "changed_agents")]
#[test_case(true, false; "removed_agents")]
#[test_case(false, true; "changed_agents_resumed")]
#[test_case(true, true; "removed_agents_resumed")]
fn managed_tui_turns_keep_recorded_instructions_and_notice_once(remove: bool, resume: bool) {
    const INSTRUCTION_FILE: &str = "AGENTS.md";
    const RECORDED: &str = "recorded TUI project instructions";
    const CHANGED: &str = "changed TUI project instructions";

    smol::block_on(futures_lite::future::or(
        async {
            let directory = tempfile::tempdir().unwrap();
            let instruction_path = directory.path().join(INSTRUCTION_FILE);
            smol::fs::write(&instruction_path, RECORDED).await.unwrap();
            let canonical_path = instruction_path.canonicalize().unwrap();
            let (backend, mut events) = BackendFixture::at(directory.path().to_path_buf());
            let (requests_tx, requests) = flume::bounded(1);
            let (release, released) = flume::bounded(1);
            let mut config = stub_policy()
                .with_context(AgentContextSnapshot::new(directory.path().to_path_buf()));
            config.settings.provider = Arc::new(ContextProvider {
                inner: ExplicitProvider { release: released },
                systems: requests_tx,
            });
            let mut manager = AgentManagerHandle::new(Default::default()).unwrap();
            let mut root = manager
                .create_root_with_config(Some(config.clone()), Vec::new(), None, |agent_id| {
                    Ok::<Box<dyn ActorBackend>, String>(Box::new(backend.build(agent_id)))
                })
                .await
                .unwrap();
            let mut actor = root.actor().unwrap();
            let mut settlements = actor.subscribe_events();
            assert!(root_context(&root.state_access()).unwrap().is_none());
            for turn in 0..3 {
                if turn == 2 && resume {
                    let root_id = root.id();
                    let generation = manager.generation();
                    let history = actor.committed_history();
                    let mut session = Session::<Message, TokenUsage, ToolOutput>::new(
                        &config.model.spec(),
                        &directory.path().to_string_lossy(),
                    );
                    session.bind_root_id(root_id).unwrap();
                    manager
                        .journal()
                        .checkpoint_snapshot()
                        .unwrap()
                        .apply_to(&mut session)
                        .unwrap();
                    session.save_to(directory.path()).unwrap();
                    actor.close();
                    assert!(
                        manager
                            .shutdown(SHUTDOWN_TIMEOUT)
                            .await
                            .timed_out
                            .is_empty()
                    );
                    let session = Session::load_from(session.id, directory.path()).unwrap();
                    let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
                    let snapshot = journal.checkpoint_snapshot().unwrap();
                    manager = AgentManagerHandle::restore_graph(
                        Default::default(),
                        journal,
                        session.latest_state().unwrap(),
                        snapshot.histories,
                    )
                    .unwrap();
                    assert_ne!(manager.generation(), generation);
                    let (backend, resumed_events) =
                        BackendFixture::at(directory.path().to_path_buf());
                    events = resumed_events;
                    root = manager
                        .ensure_resident_with(root_id, |_| async {
                            Ok(ResidentAgent {
                                backend: Box::new(backend.build(root_id)),
                                config: config.clone(),
                                shared_messages: None,
                                start: None,
                                initialize: None,
                            })
                        })
                        .await
                        .unwrap();
                    actor = root.actor().unwrap();
                    settlements = actor.subscribe_events();
                    assert_eq!(actor.committed_history(), history);
                    let recorded = root_context(&root.state_access()).unwrap().unwrap();
                    assert!(recorded.instructions.text.contains(RECORDED));
                    assert!(!recorded.instructions.text.contains(CHANGED));
                    assert!(requests.is_empty());
                }
                if turn == 1 {
                    if remove {
                        smol::fs::remove_file(&instruction_path).await.unwrap();
                    } else {
                        smol::fs::write(&instruction_path, CHANGED).await.unwrap();
                    }
                }
                let ticket = actor
                    .admit_turn(
                        AgentInput::new(format!("context turn {turn}"), Vec::new()),
                        None,
                        String::new(),
                    )
                    .unwrap();
                let system = requests.recv_async().await.unwrap();
                assert_eq!(system.matches(RECORDED).count(), 1);
                assert!(!system.contains(CHANGED));
                let recorded = root_context(&root.state_access()).unwrap().unwrap();
                let source = recorded
                    .instructions
                    .sources
                    .iter()
                    .find(|source| source.path == canonical_path)
                    .unwrap();
                assert_eq!(source.content, RECORDED);
                release.send_async(()).await.unwrap();
                assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
                await_settlement(&settlements, actor.presentation_id()).await;
                manager
                    .journal()
                    .wait_terminal_conversion(root.id(), ticket.turn_id())
                    .await
                    .unwrap();
                assert!(requests.is_empty());
                let notices: Vec<_> = events
                    .drain()
                    .filter_map(|envelope| match envelope.event {
                        AgentEvent::Post(post) => Some(post.text),
                        AgentEvent::ControlError { message } => panic!("{message}"),
                        _ => None,
                    })
                    .collect();
                let expected = if turn == 1 {
                    vec![CONTEXT_CHANGED_NOTICE.to_owned()]
                } else {
                    Vec::new()
                };
                assert_eq!(notices, expected);
            }
            actor.close();
            assert!(
                manager
                    .shutdown(SHUTDOWN_TIMEOUT)
                    .await
                    .timed_out
                    .is_empty()
            );
        },
        async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!("managed TUI context regression timed out");
        },
    ));
}

struct ContextProvider {
    inner: ExplicitProvider,
    systems: flume::Sender<String>,
}

impl makima_providers::provider::Provider for ContextProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a serde_json::Value,
        events: &'a flume::Sender<makima_providers::ProviderEvent>,
        options: makima_providers::RequestOptions,
        session: Option<&'a SessionRef>,
        invocation: &'a Invocation,
        delivery: RequestDelivery,
    ) -> makima_providers::provider::BoxFuture<
        'a,
        Result<makima_providers::StreamResponse, AgentError>,
    > {
        self.systems.send(system.to_owned()).unwrap();
        self.inner.stream_message(
            model, messages, system, tools, events, options, session, invocation, delivery,
        )
    }
}

fn stub_policy() -> EffectiveAgentConfig {
    EffectiveAgentConfig::new(
        makima_agent::RunSettings {
            provider: Arc::new(StubProvider),
            model: crate::components::test_model(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        },
        AgentMode::Build,
    )
}

struct StubProvider;

impl makima_providers::provider::Provider for StubProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        _messages: &'a [Message],
        _system: &'a str,
        _tools: &'a serde_json::Value,
        _event_tx: &'a flume::Sender<makima_providers::ProviderEvent>,
        _opts: makima_providers::RequestOptions,
        _session_id: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
        _delivery: RequestDelivery,
    ) -> makima_providers::provider::BoxFuture<
        'a,
        Result<makima_providers::StreamResponse, AgentError>,
    > {
        Box::pin(std::future::pending())
    }
}

struct BackendFixture {
    params: AgentParams,
    options: HeadlessBackendOptions,
    events: flume::Sender<Envelope>,
}

impl BackendFixture {
    fn new() -> (Self, flume::Receiver<Envelope>) {
        Self::at(PathBuf::from(TEST_CWD))
    }

    fn at(cwd: PathBuf) -> (Self, flume::Receiver<Envelope>) {
        let (events, received) = flume::unbounded();
        let params = AgentParams {
            agent_id: AgentId::generate(),
            provider: Arc::new(StubProvider),
            model: crate::components::test_model(),
            config: AgentConfig::default(),
            tool_output_lines: ToolOutputLines::default(),
            permissions: Arc::new(PermissionManager::new(
                PermissionsConfig::default(),
                cwd.clone(),
                ProjectConfig::for_project(&cwd),
                Arc::default(),
            )),
            session_id: None,
            mailbox: None,
            timeouts: Default::default(),
            file_tracker: FileReadTracker::fresh(),
            prompt_slots: Arc::default(),
            root_context: None,
            modes: Arc::new(ModeRegistry::builtin()),
            subagent_cancels: Arc::default(),
            registry: Arc::clone(ToolRegistry::global_arc()),
            audience: ToolAudience::MAIN,
            question_mode: QuestionMode::Tui,
            interactive_reauth: true,
            model_policy: Arc::default(),
            providers: Arc::default(),
            file_write_locks: Arc::new(FileWriteLocks::new()),
            offload: None,
            managed_turn: None,
        };
        let options = HeadlessBackendOptions {
            persist_turn_results: true,
            ..Default::default()
        };
        (
            Self {
                params,
                options,
                events,
            },
            received,
        )
    }

    fn build_child(mut self, agent_id: AgentId) -> HeadlessBackend {
        self.options.child = true;
        self.build(agent_id)
    }

    fn build(mut self, agent_id: AgentId) -> HeadlessBackend {
        self.params.agent_id = agent_id;
        let profile = FrontendProfile::tui(self.params.tool_output_lines, CancelToken::none());
        HeadlessBackend::new(self.params, profile, self.events, self.options)
    }
}
