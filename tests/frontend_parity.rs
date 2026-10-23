//! Every headless frontend (`--print`, SDK stream-json, ACP) must send the
//! provider the same thing for the same scenario. Runs the real binary
//! against a loopback OpenAI-compatible server and inspects the request
//! bodies. The scenarios: session defaults (`always_thinking` from
//! `maki.setup`, and the saved `/thinking` preference ahead of it), a model
//! switch between turns through each frontend's own mechanism, a mode set by
//! a Lua command, and a Lua command whose UI round-trip answers "no UI" at
//! once. SDK and ACP must also finish a turn whose input ended right after
//! the prompt, wait for the subagents a command left running and the turns
//! their results start, keep reading the answer a Lua command's turn waits
//! on, and fold a message sent mid-turn into the running turn; a resumed SDK
//! session comes back in the mode it was saved in.
//!
//! The TUI cannot join without a pty. Its in-process tests in maki-ui check
//! the same scenarios at the provider:
//! `event_loop::tests::live_session_requests_remain_independent_during_changes_and_restore`
//! (model switch between turns),
//! `event_loop::focus_tests::lua_mode_command_reaches_the_provider` (mode set
//! by a Lua command) and
//! `event_loop::focus_tests::blank_session_sends_always_thinking_to_the_provider`
//! (session defaults) and
//! `agent::agent_loop::tests::ordinary_submission_paths_have_runtime_parity`
//! (a message sent mid-turn joins it). The UI round-trip has no TUI counterpart: the TUI is
//! the UI.
//!
//! Unix only: the isolated environment points `HOME` and the XDG dirs at a
//! temp dir, which is not how Windows finds its config.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use maki_agent::ToolOutput;
use maki_lua::ui_sink::NO_UI_ERR;
use maki_providers::{Message, TokenUsage};
use maki_storage::StateDir;
use maki_storage::sessions::{Effort, Prefs, Session, StoredMode, StoredThinking};
use serde_json::{Value, json};
use tempfile::TempDir;
use test_case::test_case;

const PROVIDER: &str = "mock";
const API_KEY_ENV: &str = "MOCK_API_KEY";
const MODEL: &str = "thinker";
const OTHER_MODEL: &str = "other";
const FIRST: &str = "first turn";
const SECOND: &str = "second turn";
const SWITCH_COMMAND: &str = "/parity_switch";
const PLAN_COMMAND: &str = "/parity_plan";
const UI_COMMAND: &str = "/parity_ui";
const THINKING_COMMAND: &str = "/thinking high";
const BAD_THINKING_COMMAND: &str = "/thinking bogus";
const PARITY_THINKING_COMMAND: &str = "/parity_thinking";
const UI_ANSWER: &str = "ui answered: ";
const ASK_COMMAND: &str = "/parity_ask";
const ASK_PROMPT: &str = "run the asking tool";
const CHILD_ASK_COMMAND: &str = "/parity_child_ask";
const BACKGROUND_COMMAND: &str = "/parity_background";
const BACKGROUND_PROMPT: &str = "background job";
const FOLLOW_UP: &str = "and one more thing";
const FOLLOW_UP_ID: u64 = PROMPT_ID + 1;
/// The toolset the bundled plan mode swaps in.
const PLAN_TOOLS: [&str; 11] = [
    "edit",
    "glob",
    "grep",
    "plan_submit",
    "read",
    "task_despawn",
    "task_send",
    "task_spawn",
    "task_wait",
    "webfetch",
    "write",
];
const SWITCH_REQUEST: &str = "switch";
const PROMPT: &str = "say ok";
const HIGH: &str = "high";
const LOW: &str = "low";
const OFF_EFFORT: &str = "none";
const APP_DIR: &str = "makima";
const PREFS_FILE: &str = "prefs.json";
const CATALOG_CACHE_FILE: &str = "models-dev-catalog.json";
/// Any request that leaves loopback dies here instead of reaching the network.
const DEAD_PROXY: &str = "http://127.0.0.1:1";
const LOOPBACK: &str = "127.0.0.1,localhost";
/// A failure bound only: every case answers as soon as the request lands.
const TIMEOUT: Duration = Duration::from_secs(60);
const NO_REQUEST: &str = "no chat request reached the mock server";
const NO_RESULT: &str = "the turn's result never arrived";
const NO_EXIT: &str = "makima did not exit after its input ended";
const NO_RESPONSE: &str = "makima never answered the request";
const REPLY: &str = "ok";
const PROMPT_ID: u64 = 3;
const NO_INIT: &str = "the SDK never emitted its init event";
const PLAN_FILE: &str = "saved-plan.md";
const SSE_REPLY: &str = concat!(
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{},\"finish_reason\":\"stop\"}],",
    "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
);
const SSE_TOOL_CALL: &str = concat!(
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",",
    "\"type\":\"function\",\"function\":{\"name\":\"bash\",",
    "\"arguments\":\"{\\\"command\\\":\\\"touch asked\\\"}\"}}]},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{},\"finish_reason\":\"tool_calls\"}],",
    "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
);

#[derive(Debug, Clone, Copy)]
enum Frontend {
    Print,
    Sdk,
    Acp,
}

#[derive(Debug, Clone, Copy, Default)]
struct Setup {
    always_thinking: Option<&'static str>,
    saved_thinking: Option<Effort>,
}

const ALWAYS_HIGH: Setup = Setup {
    always_thinking: Some(HIGH),
    saved_thinking: None,
};
const BLANK: Setup = Setup {
    always_thinking: None,
    saved_thinking: None,
};
const SAVED_LOW_OVER_ALWAYS_HIGH: Setup = Setup {
    always_thinking: Some(HIGH),
    saved_thinking: Some(Effort::Low),
};

#[test_case(Frontend::Print, ALWAYS_HIGH => HIGH ; "print_sends_always_thinking")]
#[test_case(Frontend::Print, BLANK => OFF_EFFORT ; "print_without_always_thinking_stays_off")]
#[test_case(Frontend::Print, SAVED_LOW_OVER_ALWAYS_HIGH => LOW ; "print_sends_saved_thinking")]
#[test_case(Frontend::Sdk, ALWAYS_HIGH => HIGH ; "sdk_sends_always_thinking")]
#[test_case(Frontend::Sdk, BLANK => OFF_EFFORT ; "sdk_without_always_thinking_stays_off")]
#[test_case(Frontend::Sdk, SAVED_LOW_OVER_ALWAYS_HIGH => LOW ; "sdk_sends_saved_thinking")]
#[test_case(Frontend::Acp, ALWAYS_HIGH => HIGH ; "acp_sends_always_thinking")]
#[test_case(Frontend::Acp, BLANK => OFF_EFFORT ; "acp_without_always_thinking_stays_off")]
#[test_case(Frontend::Acp, SAVED_LOW_OVER_ALWAYS_HIGH => LOW ; "acp_sends_saved_thinking")]
fn first_request_carries_session_thinking(frontend: Frontend, setup: Setup) -> String {
    let body = first_request(frontend, setup);
    let effort = body["reasoning_effort"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if effort != OFF_EFFORT {
        assert_eq!(
            body["thinking"],
            Value::Null,
            "thinking must not be disabled"
        );
    }
    effort
}

/// Runs one turn through {frontend} with an isolated home and returns the
/// first chat-completions body it sent.
fn first_request(frontend: Frontend, setup: Setup) -> Value {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, setup);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();

    let mut child = spawn_makima(home.path(), &work, frontend_args(frontend));
    let mut stdin = child.stdin.take().unwrap();
    match frontend {
        Frontend::Print => drop(stdin),
        Frontend::Sdk => send(&mut stdin, sdk_user(PROMPT)),
        Frontend::Acp => drop(acp_prompt(&mut child, &mut stdin, &work)),
    }
    let body = bodies.recv_timeout(TIMEOUT);
    let _ = child.kill();
    let output = child.wait_with_output().unwrap();
    body.unwrap_or_else(|_| {
        panic!(
            "{NO_REQUEST}; makima stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[derive(Debug, Clone, Copy)]
enum Scenario {
    ModelSwitch,
    LuaMode,
    LuaUiRoundTrip,
    ThinkingCommand,
}

enum Step {
    Prompt(String),
    SwitchModel,
}

impl Scenario {
    /// What the client does. Print takes a single prompt, so its scenarios
    /// run inside one Lua command.
    fn steps(self, frontend: Frontend) -> Vec<Step> {
        let prompt = |text: &str| Step::Prompt(text.to_owned());
        match (self, frontend) {
            (Self::ModelSwitch, Frontend::Print) => vec![prompt(SWITCH_COMMAND)],
            (Self::ModelSwitch, _) => vec![prompt(FIRST), Step::SwitchModel, prompt(SECOND)],
            (Self::LuaMode, Frontend::Print) => vec![prompt(&format!("{PLAN_COMMAND} {PROMPT}"))],
            (Self::LuaMode, _) => vec![prompt(PLAN_COMMAND), prompt(PROMPT)],
            (Self::LuaUiRoundTrip, _) => vec![prompt(UI_COMMAND)],
            (Self::ThinkingCommand, Frontend::Print) => vec![prompt(PARITY_THINKING_COMMAND)],
            (Self::ThinkingCommand, _) => vec![prompt(THINKING_COMMAND), prompt(PROMPT)],
        }
    }

    fn check(self, bodies: &[Value], home: &Path) {
        match self {
            Self::ModelSwitch => {
                let models: Vec<_> = bodies.iter().map(|body| &body["model"]).collect();
                assert_eq!(models, [MODEL, OTHER_MODEL], "{bodies:?}");
                assert_eq!(
                    bodies.iter().map(last_user_text).collect::<Vec<_>>(),
                    [FIRST, SECOND]
                );
            }
            Self::LuaMode => {
                let [body] = bodies else {
                    panic!("expected one request: {bodies:?}")
                };
                assert_eq!(last_user_text(body), PROMPT);
                assert_eq!(tool_names(body), PLAN_TOOLS);
                let plans = home
                    .join(".local/state")
                    .join(APP_DIR)
                    .join(maki_storage::plans::PLANS_DIR);
                assert!(
                    system_text(body).contains(plans.to_string_lossy().as_ref()),
                    "the plan file is not in {}",
                    plans.display()
                );
            }
            Self::LuaUiRoundTrip => {
                let [body] = bodies else {
                    panic!("expected one request: {bodies:?}")
                };
                assert_eq!(last_user_text(body), format!("{UI_ANSWER}{NO_UI_ERR}"));
            }
            Self::ThinkingCommand => {
                let [body] = bodies else {
                    panic!("expected one request: {bodies:?}")
                };
                assert_eq!(last_user_text(body), PROMPT);
                assert_eq!(body["reasoning_effort"], HIGH, "{body}");
            }
        }
    }
}

#[test_case(Frontend::Print, Scenario::ModelSwitch ; "print_switches_model_between_turns")]
#[test_case(Frontend::Sdk, Scenario::ModelSwitch ; "sdk_switches_model_between_turns")]
#[test_case(Frontend::Acp, Scenario::ModelSwitch ; "acp_switches_model_between_turns")]
#[test_case(Frontend::Print, Scenario::LuaMode ; "print_lua_command_sets_plan_mode")]
#[test_case(Frontend::Sdk, Scenario::LuaMode ; "sdk_lua_command_sets_plan_mode")]
#[test_case(Frontend::Acp, Scenario::LuaMode ; "acp_lua_command_sets_plan_mode")]
#[test_case(Frontend::Print, Scenario::LuaUiRoundTrip ; "print_lua_ui_round_trip_answers_no_ui")]
#[test_case(Frontend::Sdk, Scenario::LuaUiRoundTrip ; "sdk_lua_ui_round_trip_answers_no_ui")]
#[test_case(Frontend::Acp, Scenario::LuaUiRoundTrip ; "acp_lua_ui_round_trip_answers_no_ui")]
#[test_case(Frontend::Print, Scenario::ThinkingCommand ; "print_thinking_command_sets_the_level")]
#[test_case(Frontend::Sdk, Scenario::ThinkingCommand ; "sdk_thinking_command_sets_the_level")]
#[test_case(Frontend::Acp, Scenario::ThinkingCommand ; "acp_thinking_command_sets_the_level")]
fn scenario_reaches_the_provider_alike(frontend: Frontend, scenario: Scenario) {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let steps = scenario.steps(frontend);

    let mut child = match (frontend, steps.as_slice()) {
        (Frontend::Print, [Step::Prompt(prompt)]) => {
            spawn_makima(home.path(), &work, print_args(prompt))
        }
        (Frontend::Print, _) => unreachable!("print runs one prompt"),
        _ => spawn_makima(home.path(), &work, frontend_args(frontend)),
    };
    let stderr = drain(child.stderr.take().unwrap());
    let mut stdin = child.stdin.take().unwrap();
    let mut sdk_output = None;
    let mut print_output = None;
    match frontend {
        Frontend::Print => {
            print_output = Some(drain(child.stdout.take().unwrap()));
            drop(stdin);
        }
        Frontend::Sdk => {
            let output = sdk_output.insert(json_lines(child.stdout.take().unwrap()));
            for step in &steps {
                match step {
                    Step::Prompt(text) => {
                        send(&mut stdin, sdk_user(text));
                        let result = expect(output, |message| message["type"] == "result");
                        assert_eq!(result["is_error"], false, "{result}");
                    }
                    Step::SwitchModel => {
                        send(
                            &mut stdin,
                            json!({"type": "control_request", "request_id": SWITCH_REQUEST,
                                   "request": {"subtype": "set_model",
                                               "model": format!("{PROVIDER}/{OTHER_MODEL}")}}),
                        );
                        let response = expect(output, |message| {
                            message["response"]["request_id"] == SWITCH_REQUEST
                        });
                        assert_eq!(response["response"]["subtype"], "success", "{response}");
                    }
                }
            }
            drop(stdin);
        }
        Frontend::Acp => {
            let (output, session) = acp_session(&mut child, &mut stdin, &work);
            for (id, step) in (PROMPT_ID..).zip(&steps) {
                match step {
                    Step::Prompt(text) => send(
                        &mut stdin,
                        json!({"jsonrpc": "2.0", "id": id, "method": "session/prompt",
                               "params": {"sessionId": session,
                                          "prompt": [{"type": "text", "text": text}]}}),
                    ),
                    Step::SwitchModel => send(
                        &mut stdin,
                        json!({"jsonrpc": "2.0", "id": id, "method": "session/set_config_option",
                               "params": {"sessionId": session, "configId": "model",
                                          "value": format!("{PROVIDER}/{OTHER_MODEL}")}}),
                    ),
                }
                let response = expect(&output, |message| message["id"] == id);
                assert!(response.get("error").is_none(), "{response}");
            }
            drop(stdin);
        }
    }
    let status = wait_exit(child);
    let log = stderr.join().unwrap();
    assert!(status.success(), "{log}");
    if let Some(output) = sdk_output {
        assert_one_result_per_message(&output);
    }
    if let Some(output) = print_output {
        assert_eq!(output.join().unwrap(), REPLY, "{log}");
        assert!(
            !prefs_path(home.path()).exists(),
            "print saves no preferences"
        );
    }
    scenario.check(&bodies.try_iter().collect::<Vec<_>>(), home.path());
}

/// `/thinking` with a level the model cannot take fails the command in every
/// frontend, rather than only flashing the reason.
#[test_case(Frontend::Print ; "print_fails_a_bad_thinking_level")]
#[test_case(Frontend::Sdk ; "sdk_fails_a_bad_thinking_level")]
#[test_case(Frontend::Acp ; "acp_fails_a_bad_thinking_level")]
fn bad_thinking_level_fails_the_command(frontend: Frontend) {
    let (base_url, _bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let args = match frontend {
        Frontend::Print => print_args(BAD_THINKING_COMMAND),
        _ => frontend_args(frontend),
    };
    let mut child = spawn_makima(home.path(), &work, args);
    let stderr = drain(child.stderr.take().unwrap());
    let mut stdin = child.stdin.take().unwrap();
    match frontend {
        Frontend::Print => {
            drop(stdin);
            assert!(!wait_exit(child).success(), "{}", stderr.join().unwrap());
            return;
        }
        Frontend::Sdk => {
            let output = json_lines(child.stdout.take().unwrap());
            send(&mut stdin, sdk_user(BAD_THINKING_COMMAND));
            let result = expect(&output, |message| message["type"] == "result");
            assert_eq!(result["is_error"], true, "{result}");
        }
        Frontend::Acp => {
            let (output, session) = acp_session(&mut child, &mut stdin, &work);
            send(
                &mut stdin,
                json!({"jsonrpc": "2.0", "id": PROMPT_ID, "method": "session/prompt",
                       "params": {"sessionId": session,
                                  "prompt": [{"type": "text", "text": BAD_THINKING_COMMAND}]}}),
            );
            let response = expect(&output, |message| message["id"] == PROMPT_ID);
            assert!(response.get("error").is_some(), "{response}");
        }
    }
    drop(stdin);
    assert!(wait_exit(child).success(), "{}", stderr.join().unwrap());
    assert!(!prefs_path(home.path()).exists());
}

fn prefs_path(home: &Path) -> PathBuf {
    home.join(".local/state").join(APP_DIR).join(PREFS_FILE)
}

/// `echo prompt | makima`: input ends right after the prompt, yet the turn
/// runs to its end, its result arrives, and only then does the process exit.
#[test_case(Frontend::Sdk ; "sdk_finishes_a_piped_turn")]
#[test_case(Frontend::Acp ; "acp_finishes_a_piped_turn")]
fn closed_input_still_delivers_the_turn(frontend: Frontend) {
    let (base_url, _bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();

    let mut child = spawn_makima(home.path(), &work, frontend_args(frontend));
    let mut stdin = child.stdin.take().unwrap();
    let output = match frontend {
        Frontend::Sdk => {
            let output = json_lines(child.stdout.take().unwrap());
            send(&mut stdin, sdk_user(PROMPT));
            output
        }
        Frontend::Acp => acp_prompt(&mut child, &mut stdin, &work),
        Frontend::Print => unreachable!("print reads its whole input before it starts"),
    };
    drop(stdin);
    let result = loop {
        let message = output.recv_timeout(TIMEOUT).expect(NO_RESULT);
        match frontend {
            Frontend::Sdk if message["type"] == "result" => break message,
            Frontend::Acp if message["id"] == PROMPT_ID => break message,
            _ => {}
        }
    };
    match frontend {
        Frontend::Sdk => {
            assert_eq!(result["subtype"], "success", "{result}");
            assert_eq!(result["result"], REPLY);
        }
        _ => assert_eq!(result["result"]["stopReason"], "end_turn", "{result}"),
    }
    assert!(wait_exit(child).success());
}

/// A command whose handler runs two turns gets one result, the last turn's
/// reply, as print prints it.
#[test]
fn sdk_command_running_turns_reports_once() {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut child = spawn_makima(home.path(), &work, frontend_args(Frontend::Sdk));
    let stderr = drain(child.stderr.take().unwrap());
    let mut stdin = child.stdin.take().unwrap();
    let output = json_lines(child.stdout.take().unwrap());
    send(&mut stdin, sdk_user(SWITCH_COMMAND));
    let result = expect(&output, |message| message["type"] == "result");
    assert_eq!(result["is_error"], false, "{result}");
    assert_eq!(result["result"], REPLY, "{result}");
    drop(stdin);
    let status = wait_exit(child);
    assert!(status.success(), "{}", stderr.join().unwrap());
    assert_one_result_per_message(&output);
    assert_eq!(bodies.try_iter().count(), 2);
}

/// A Lua command whose turn asks the client for permission: the input loop
/// keeps reading while the handler waits, so the answer gets through, the
/// turn ends and the command reports.
#[test_case(Frontend::Sdk ; "sdk_reads_the_answer_a_command_waits_on")]
#[test_case(Frontend::Acp ; "acp_reads_the_answer_a_command_waits_on")]
fn command_waiting_on_an_answer_still_gets_it(frontend: Frontend) {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut child = spawn_makima(home.path(), &work, asking_args(frontend, PROMPT));
    let stderr = drain(child.stderr.take().unwrap());
    let mut stdin = child.stdin.take().unwrap();
    let mut sdk_output = None;
    match frontend {
        Frontend::Sdk => {
            let output = json_lines(child.stdout.take().unwrap());
            send(&mut stdin, sdk_user(ASK_COMMAND));
            let ask = expect(&output, |message| {
                message["request"]["subtype"] == "can_use_tool"
            });
            send(
                &mut stdin,
                json!({"type": "control_response",
                       "response": {"request_id": ask["request_id"], "behavior": "deny"}}),
            );
            let result = expect(&output, |message| message["type"] == "result");
            assert_eq!(result["is_error"], false, "{result}");
            sdk_output = Some(output);
        }
        Frontend::Acp => {
            let (output, session) = acp_session(&mut child, &mut stdin, &work);
            send(
                &mut stdin,
                json!({"jsonrpc": "2.0", "id": PROMPT_ID, "method": "session/prompt",
                       "params": {"sessionId": session,
                                  "prompt": [{"type": "text", "text": ASK_COMMAND}]}}),
            );
            let ask = expect(&output, |message| {
                message["method"] == "session/request_permission"
            });
            send(
                &mut stdin,
                json!({"jsonrpc": "2.0", "id": ask["id"],
                       "result": {"outcome": {"outcome": "cancelled"}}}),
            );
            let response = expect(&output, |message| message["id"] == PROMPT_ID);
            assert!(response.get("error").is_none(), "{response}");
        }
        Frontend::Print => unreachable!("print answers no questions"),
    }
    drop(stdin);
    let status = wait_exit(child);
    let log = stderr.join().unwrap();
    assert!(status.success(), "{log}");
    if let Some(output) = sdk_output {
        assert_one_result_per_message(&output);
    }
    let bodies: Vec<_> = bodies.try_iter().collect();
    assert_eq!(bodies.len(), 2, "{bodies:?}");
    assert_eq!(
        bodies[1]["messages"].as_array().unwrap().last().unwrap()["role"],
        "tool"
    );
}

/// Input ends right after a command left a subagent working in the
/// background: the subagent's turn and the root turn its result starts both
/// run before the process exits.
#[test_case(Frontend::Sdk ; "sdk_finishes_background_work")]
#[test_case(Frontend::Acp ; "acp_finishes_background_work")]
fn closed_input_waits_for_background_subagents(frontend: Frontend) {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut child = spawn_makima(home.path(), &work, frontend_args(frontend));
    let stderr = drain(child.stderr.take().unwrap());
    let mut stdin = child.stdin.take().unwrap();
    match frontend {
        Frontend::Sdk => {
            let output = json_lines(child.stdout.take().unwrap());
            send(&mut stdin, sdk_user(BACKGROUND_COMMAND));
            expect(&output, |message| message["type"] == "result");
        }
        Frontend::Acp => {
            let (output, session) = acp_session(&mut child, &mut stdin, &work);
            send(
                &mut stdin,
                json!({"jsonrpc": "2.0", "id": PROMPT_ID, "method": "session/prompt",
                       "params": {"sessionId": session,
                                  "prompt": [{"type": "text", "text": BACKGROUND_COMMAND}]}}),
            );
            expect(&output, |message| message["id"] == PROMPT_ID);
        }
        Frontend::Print => unreachable!("print reads its whole input before it starts"),
    }
    drop(stdin);
    let status = wait_exit(child);
    let log = stderr.join().unwrap();
    assert!(status.success(), "{log}");
    let bodies: Vec<_> = bodies.try_iter().collect();
    assert_eq!(bodies.len(), 2, "{bodies:?}");
    assert_eq!(last_user_text(&bodies[0]), BACKGROUND_PROMPT);
}

/// A message sent while a turn waits on a permission answer joins that turn,
/// as typing does in the TUI, and the one turn answers both messages.
#[test_case(Frontend::Sdk ; "sdk_message_joins_the_running_turn")]
#[test_case(Frontend::Acp ; "acp_prompt_joins_the_running_turn")]
fn message_sent_mid_turn_joins_it(frontend: Frontend) {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut child = spawn_makima(home.path(), &work, asking_args(frontend, ASK_PROMPT));
    let stderr = drain(child.stderr.take().unwrap());
    let mut stdin = child.stdin.take().unwrap();
    match frontend {
        Frontend::Sdk => {
            let output = json_lines(child.stdout.take().unwrap());
            send(&mut stdin, sdk_user(ASK_PROMPT));
            let ask = expect(&output, |message| {
                message["request"]["subtype"] == "can_use_tool"
            });
            send(&mut stdin, sdk_user(FOLLOW_UP));
            send(
                &mut stdin,
                json!({"type": "control_response",
                       "response": {"request_id": ask["request_id"], "behavior": "deny"}}),
            );
            let result = expect(&output, |message| message["type"] == "result");
            assert_eq!(result["result"], REPLY, "{result}");
            drop(stdin);
            let results: Vec<_> = output
                .iter()
                .filter(|message| message["type"] == "result")
                .collect();
            assert!(results.is_empty(), "{results:?}");
        }
        Frontend::Acp => {
            let (output, session) = acp_session(&mut child, &mut stdin, &work);
            let prompt = |id, text| {
                json!({"jsonrpc": "2.0", "id": id, "method": "session/prompt",
                       "params": {"sessionId": session,
                                  "prompt": [{"type": "text", "text": text}]}})
            };
            send(&mut stdin, prompt(PROMPT_ID, ASK_PROMPT));
            let ask = expect(&output, |message| {
                message["method"] == "session/request_permission"
            });
            send(&mut stdin, prompt(FOLLOW_UP_ID, FOLLOW_UP));
            send(
                &mut stdin,
                json!({"jsonrpc": "2.0", "id": ask["id"],
                       "result": {"outcome": {"outcome": "cancelled"}}}),
            );
            for id in [PROMPT_ID, FOLLOW_UP_ID] {
                let response = expect(&output, |message| message["id"] == id);
                assert_eq!(response["result"]["stopReason"], "end_turn", "{response}");
            }
            drop(stdin);
        }
        Frontend::Print => unreachable!("print reads its whole input before it starts"),
    }
    let status = wait_exit(child);
    let log = stderr.join().unwrap();
    assert!(status.success(), "{log}");
    // The follow-up reaches the turn's next request, or the one after when
    // the answer got there first; either way inside the turn, not as a turn
    // of its own.
    let bodies: Vec<_> = bodies.try_iter().collect();
    let follow_up = last_user_text(bodies.last().unwrap());
    assert!(
        follow_up.contains(FOLLOW_UP) && follow_up != FOLLOW_UP,
        "{bodies:?}"
    );
}

/// Print answers no questions, for children too: a child's permission ask
/// is refused at once, so the run ends instead of waiting on an answer.
#[test]
fn print_refuses_a_child_ask_at_once() {
    let (base_url, bodies) = mock_server();
    let home = TempDir::new().unwrap();
    write_config(home.path(), &base_url, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let mut child = spawn_makima(
        home.path(),
        &work,
        asking_args(Frontend::Print, CHILD_ASK_COMMAND),
    );
    let stderr = drain(child.stderr.take().unwrap());
    drop(child.stdin.take());
    let status = wait_exit(child);
    let log = stderr.join().unwrap();
    assert!(status.success(), "{log}");
    let bodies: Vec<_> = bodies.try_iter().collect();
    assert_eq!(bodies.len(), 2, "{bodies:?}");
    assert_eq!(
        bodies[1]["messages"].as_array().unwrap().last().unwrap()["role"],
        "tool"
    );
}

/// Like the TUI and ACP, an SDK resume puts the session back in the mode it
/// was saved in, here plan mode with its plan file.
#[test]
fn sdk_resume_restores_the_saved_mode() {
    let home = TempDir::new().unwrap();
    write_config(home.path(), DEAD_PROXY, BLANK);
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let state = home.path().join(".local/state").join(APP_DIR);
    let plan = state.join(PLAN_FILE);
    std::fs::write(&plan, "").unwrap();
    let mut session: Session<Message, TokenUsage, ToolOutput> =
        Session::new(&format!("{PROVIDER}/{MODEL}"), &work.to_string_lossy());
    session.replace_messages(vec![Message::user(PROMPT.into())]);
    session.meta.mode = Some(StoredMode::Plan);
    session.meta.plan_path = Some(plan.to_string_lossy().into_owned());
    session.save(&StateDir::from_path(state)).unwrap();

    let mut args = frontend_args(Frontend::Sdk);
    args.extend(["-c".to_owned(), session.id.to_string()]);
    let mut child = spawn_makima(home.path(), &work, args);
    let output = json_lines(child.stdout.take().unwrap());
    let init = loop {
        let message = output.recv_timeout(TIMEOUT).expect(NO_INIT);
        if message["subtype"] == "init" {
            break message;
        }
    };
    drop(child.stdin.take());
    assert_eq!(init["permissionMode"], "plan", "{init}");
    assert!(wait_exit(child).success());
}

/// Waits for {child} to exit, killing it if it outlives the bound.
fn wait_exit(mut child: Child) -> ExitStatus {
    let pid = child.id().to_string();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait());
    });
    match rx.recv_timeout(TIMEOUT) {
        Ok(status) => status.unwrap(),
        Err(_) => {
            let _ = Command::new("kill").args(["-KILL", &pid]).status();
            panic!("{NO_EXIT}");
        }
    }
}

/// Every SDK result was already read, one per user message, so none is left
/// once the process has exited.
fn assert_one_result_per_message(output: &Receiver<Value>) {
    let extra: Vec<_> = output
        .iter()
        .filter(|message| message["type"] == "result")
        .collect();
    assert!(extra.is_empty(), "{extra:?}");
}

/// The first message on {output} that {matches}.
fn expect(output: &Receiver<Value>, matches: impl Fn(&Value) -> bool) -> Value {
    loop {
        let message = output.recv_timeout(TIMEOUT).expect(NO_RESPONSE);
        if matches(&message) {
            return message;
        }
    }
}

fn sdk_user(text: &str) -> Value {
    json!({"type": "user", "message": {"role": "user", "content": text}})
}

fn last_user_text(body: &Value) -> String {
    let message = body["messages"]
        .as_array()
        .and_then(|messages| {
            messages
                .iter()
                .rev()
                .find(|message| message["role"] == "user")
        })
        .unwrap_or_else(|| panic!("no user message: {body}"));
    content_text(&message["content"])
}

fn tool_names(body: &Value) -> Vec<&str> {
    let mut names: Vec<_> = body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .collect();
    names.sort_unstable();
    names
}

fn system_text(body: &Value) -> String {
    body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|message| message["role"] == "system")
        .map(|message| content_text(&message["content"]))
        .collect()
}

fn content_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect(),
        _ => String::new(),
    }
}

fn print_args(prompt: &str) -> Vec<String> {
    let spec = format!("{PROVIDER}/{MODEL}");
    [
        "--print",
        "--yolo",
        "--max-turns",
        "1",
        "-m",
        spec.as_str(),
        "-p",
        prompt,
    ]
    .iter()
    .map(|arg| arg.to_string())
    .collect()
}

/// {frontend}'s arguments without YOLO or a turn limit, so a tool asks
/// permission and the turn goes on after the answer. Print runs {prompt}.
fn asking_args(frontend: Frontend, prompt: &str) -> Vec<String> {
    let args = match frontend {
        Frontend::Print => print_args(prompt),
        _ => frontend_args(frontend),
    };
    args.into_iter()
        .filter(|arg| !matches!(arg.as_str(), "--yolo" | "--max-turns" | "1"))
        .collect()
}

fn frontend_args(frontend: Frontend) -> Vec<String> {
    let spec = format!("{PROVIDER}/{MODEL}");
    let args: &[&str] = match frontend {
        Frontend::Print => return print_args(PROMPT),
        Frontend::Sdk => &[
            "--print",
            "--yolo",
            "--max-turns",
            "1",
            "-m",
            spec.as_str(),
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
        ],
        Frontend::Acp => &["acp", "--yolo", "-m", spec.as_str()],
    };
    args.iter().map(|arg| arg.to_string()).collect()
}

/// `initialize`, `session/new`, then `session/prompt` on the new session id.
/// Returns the rest of the server's output.
fn acp_prompt(child: &mut Child, stdin: &mut ChildStdin, cwd: &Path) -> Receiver<Value> {
    let (responses, session) = acp_session(child, stdin, cwd);
    send(
        stdin,
        json!({"jsonrpc": "2.0", "id": PROMPT_ID, "method": "session/prompt",
               "params": {"sessionId": session, "prompt": [{"type": "text", "text": PROMPT}]}}),
    );
    responses
}

/// `initialize` and `session/new`. Returns the server's output and the new
/// session id.
fn acp_session(child: &mut Child, stdin: &mut ChildStdin, cwd: &Path) -> (Receiver<Value>, Value) {
    let responses = json_lines(child.stdout.take().unwrap());
    send(
        stdin,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
               "params": {"protocolVersion": 1, "clientCapabilities": {}}}),
    );
    send(
        stdin,
        json!({"jsonrpc": "2.0", "id": 2, "method": "session/new",
               "params": {"cwd": cwd, "mcpServers": []}}),
    );
    let session = expect(&responses, |message| message["id"] == 2)["result"]["sessionId"].clone();
    (responses, session)
}

fn send(stdin: &mut ChildStdin, message: Value) {
    writeln!(stdin, "{message}").unwrap();
    stdin.flush().unwrap();
}

/// Reads all of {pipe} so the child never blocks on a full pipe.
fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut log = String::new();
        let _ = pipe.read_to_string(&mut log);
        log
    })
}

fn json_lines(stdout: impl Read + Send + 'static) -> Receiver<Value> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(message) = serde_json::from_str(&line)
                && tx.send(message).is_err()
            {
                break;
            }
        }
    });
    rx
}

fn write_config(home: &Path, base_url: &str, setup: Setup) {
    let config = home.join(".config").join(APP_DIR);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("providers.toml"),
        format!(
            "[{PROVIDER}]\nprotocol = \"openai\"\nbase_url = \"{base_url}\"\n\
             api_key_env = \"{API_KEY_ENV}\"\ndiscover_models = false\n\n\
             [[{PROVIDER}.models]]\nid = \"{MODEL}\"\ntier = \"strong\"\n\
             context_window = 32768\nmax_output_tokens = 4096\nsupports_thinking = true\n\n\
             [[{PROVIDER}.models]]\nid = \"{OTHER_MODEL}\"\ntier = \"medium\"\n\
             context_window = 32768\nmax_output_tokens = 4096\n"
        ),
    )
    .unwrap();
    let always = setup
        .always_thinking
        .map(|level| format!("always_thinking = \"{level}\""))
        .unwrap_or_default();
    std::fs::write(
        config.join("init.lua"),
        format!("maki.setup({{ {always} }})\n{}", parity_commands()),
    )
    .unwrap();

    let state = home.join(".local/state").join(APP_DIR);
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(state.join(CATALOG_CACHE_FILE), "{}").unwrap();
    if let Some(level) = setup.saved_thinking {
        let prefs = Prefs {
            default_thinking: Some(StoredThinking::Effort { level }),
        };
        std::fs::write(state.join(PREFS_FILE), serde_json::to_vec(&prefs).unwrap()).unwrap();
    }
}

/// The Lua commands the scenarios run: switch the model between two turns,
/// set plan mode (then prompt with any argument), prompt for a tool that
/// asks permission (on the root, or on a child), and prompt with what a UI
/// round-trip answered.
fn parity_commands() -> String {
    format!(
        r#"
local function prompt(ctx, text)
  local _, err = assert(assert(ctx:agent()):send(ctx, text, {{ when = "idle" }})):wait(ctx)
  assert(err == nil, tostring(err))
end
maki.api.register_command({{
  name = "{SWITCH_COMMAND}", tui_only = false,
  handler = function(_, ctx)
    prompt(ctx, "{FIRST}")
    assert(assert(ctx:agent()):set_model(ctx, "{PROVIDER}/{OTHER_MODEL}"))
    prompt(ctx, "{SECOND}")
  end,
}})
maki.api.register_command({{
  name = "{PLAN_COMMAND}", tui_only = false, arguments = {{ raw = true }},
  handler = function(opts, ctx)
    assert(assert(ctx:agent()):set_mode(ctx, "plan"))
    if opts.args ~= "" then prompt(ctx, opts.args) end
  end,
}})
maki.api.register_command({{
  name = "{ASK_COMMAND}", tui_only = false,
  handler = function(_, ctx) prompt(ctx, "{ASK_PROMPT}") end,
}})
maki.api.register_command({{
  name = "{CHILD_ASK_COMMAND}", tui_only = false,
  handler = function(_, ctx)
    local child = assert(maki.agent.spawn(ctx, {{ auto_deliver = false }}))
    local _, err = assert(child:send(ctx, "{ASK_PROMPT}", {{ when = "idle" }})):wait(ctx)
    assert(err == nil, tostring(err))
    child:close(ctx)
  end,
}})
maki.api.register_command({{
  name = "{BACKGROUND_COMMAND}", tui_only = false,
  handler = function(_, ctx)
    local child = assert(maki.agent.spawn(ctx, {{}}))
    assert(child:send(ctx, "{BACKGROUND_PROMPT}", {{ when = "join" }}))
  end,
}})
maki.api.register_command({{
  name = "{PARITY_THINKING_COMMAND}", tui_only = false,
  handler = function(_, ctx)
    assert(maki.api.run_command(ctx, "{THINKING_COMMAND}"))
    prompt(ctx, "{PROMPT}")
  end,
}})
maki.api.register_command({{
  name = "{UI_COMMAND}", tui_only = false,
  handler = function(_, ctx)
    local _, err = maki.fn.winsaveview()
    prompt(ctx, "{UI_ANSWER}" .. tostring(err))
  end,
}})
"#
    )
}

fn spawn_makima(home: &Path, work: &Path, args: Vec<String>) -> Child {
    Command::new(env!("CARGO_BIN_EXE_makima"))
        .args(args)
        .current_dir(work)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_STATE_HOME", home.join(".local/state"))
        .env("XDG_DATA_HOME", home.join(".local/share"))
        .env("XDG_CACHE_HOME", home.join(".cache"))
        .env("http_proxy", DEAD_PROXY)
        .env("https_proxy", DEAD_PROXY)
        .env("all_proxy", DEAD_PROXY)
        .env("no_proxy", LOOPBACK)
        .env("OLLAMA_HOST", DEAD_PROXY)
        .env(API_KEY_ENV, "test")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// A tool call for a request that ends on {ASK_PROMPT}, text otherwise.
fn reply_to(body: &Value) -> &'static str {
    let last = body["messages"]
        .as_array()
        .and_then(|messages| messages.last());
    match last {
        Some(message)
            if message["role"] == "user" && content_text(&message["content"]) == ASK_PROMPT =>
        {
            SSE_TOOL_CALL
        }
        _ => SSE_REPLY,
    }
}

/// Serves chat completions on loopback and forwards each request body.
fn mock_server() -> (String, Receiver<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut length = 0;
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                    break;
                }
                if let Some((name, value)) = header.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            if request_line.contains("/chat/completions") {
                let json: Value = serde_json::from_slice(&body).unwrap_or_default();
                let reply = reply_to(&json);
                let _ = tx.send(json);
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{reply}"
                );
            } else {
                let _ = write!(
                    stream,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        }
    });
    (base, rx)
}
