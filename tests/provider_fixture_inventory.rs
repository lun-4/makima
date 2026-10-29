#[path = "support/http_contract.rs"]
mod http_contract;

use std::collections::{BTreeMap, BTreeSet};
use std::process::{Command, Stdio};
use std::time::Duration;

use http_contract::{HttpContract, Response, Step};
use maki_providers::KeyPool;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FixtureId {
    provider: &'static str,
    route: &'static str,
    operation: &'static str,
    case: &'static str,
}

struct Fixture {
    id: FixtureId,
    check: fn() -> Result<(), String>,
}

fn run_inventory(
    fixtures: &[Fixture],
    required: &[FixtureId],
) -> Result<BTreeSet<FixtureId>, Vec<String>> {
    let mut counts = BTreeMap::new();
    for fixture in fixtures {
        *counts.entry(fixture.id).or_insert(0usize) += 1;
    }
    let mut errors = Vec::new();
    let mut passed = BTreeSet::new();
    for (id, count) in &counts {
        if *count != 1 {
            errors.push(format!("duplicate fixture: {id:?}"));
        }
    }
    for fixture in fixtures {
        if counts[&fixture.id] != 1 {
            continue;
        }
        match (fixture.check)() {
            Ok(()) => {
                passed.insert(fixture.id);
            }
            Err(error) => errors.push(format!("{:?}: {error}", fixture.id)),
        }
    }
    for id in required {
        if !counts.contains_key(id) {
            errors.push(format!("missing fixture: {id:?}"));
        }
    }
    if errors.is_empty() {
        Ok(passed)
    } else {
        Err(errors)
    }
}

const KEY_ROTATION: FixtureId = FixtureId {
    provider: "shared-key-pool",
    route: "auth",
    operation: "rotate",
    case: "wraps-in-order",
};

fn key_rotation() -> Result<(), String> {
    let pool = KeyPool::from_keys(vec!["first".into(), "second".into()]);
    if pool.current() != "first"
        || !pool.rotate()
        || pool.current() != "second"
        || !pool.rotate()
        || pool.current() != "first"
    {
        return Err("key rotation did not wrap in order".into());
    }
    Ok(())
}

const CUSTOM_CHAT: FixtureId = FixtureId {
    provider: "custom",
    route: "openai-chat",
    operation: "stream",
    case: "user-message",
};

fn custom_chat_request() -> Result<(), String> {
    let server = HttpContract::start(vec![Response {
        status: "200 OK".into(),
        headers: vec![("Content-Type".into(), "text/event-stream".into())],
        steps: vec![Step::Chunk(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        ).as_bytes().to_vec())],
    }]).map_err(|error| error.to_string())?;
    let home = tempfile::tempdir().map_err(|error| error.to_string())?;
    let config = home.path().join(".config/makima");
    std::fs::create_dir_all(&config).map_err(|error| error.to_string())?;
    std::fs::write(config.join("providers.toml"), format!(
        "[fixture]\nprotocol = \"openai\"\nbase_url = \"http://{}/v1\"\napi_key_env = \"FIXTURE_KEY\"\ndiscover_models = false\n\n[[fixture.models]]\nid = \"example\"\ntier = \"strong\"\ncontext_window = 32768\nmax_output_tokens = 4096\n", server.addr
    )).map_err(|error| error.to_string())?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_makima"))
        .args([
            "--print",
            "--yolo",
            "--max-turns",
            "1",
            "-m",
            "fixture/example",
            "-p",
            "fixture prompt",
        ])
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", home.path().join(".config"))
        .env("XDG_STATE_HOME", home.path().join(".local/state"))
        .env("XDG_DATA_HOME", home.path().join(".local/share"))
        .env("XDG_CACHE_HOME", home.path().join(".cache"))
        .env("FIXTURE_KEY", "fixture-key")
        .env("OLLAMA_HOST", "http://127.0.0.1:1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;
    let request = server.requests.recv_timeout(Duration::from_secs(10));
    let _ = child.kill();
    let _ = child.wait();
    let request = request
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    let body: Value = serde_json::from_slice(&request.body).map_err(|error| error.to_string())?;
    if request.method != "POST"
        || request.target != "/v1/chat/completions"
        || request.header("authorization") != Some("Bearer fixture-key")
        || body["model"] != "example"
        || !body["messages"].as_array().is_some_and(|messages| {
            messages
                .iter()
                .any(|message| message["role"] == "user" && message["content"] == "fixture prompt")
        })
    {
        return Err(format!(
            "unexpected request: {} {} {body}",
            request.method, request.target
        ));
    }
    Ok(())
}

const REQUIRED_RUST_FIXTURES: &[FixtureId] = &[KEY_ROTATION, CUSTOM_CHAT];

const RUST_FIXTURES: &[Fixture] = &[
    Fixture {
        id: KEY_ROTATION,
        check: key_rotation,
    },
    Fixture {
        id: CUSTOM_CHAT,
        check: custom_chat_request,
    },
];

#[cfg(test)]
mod tests {
    use super::{
        CUSTOM_CHAT, Fixture, FixtureId, KEY_ROTATION, REQUIRED_RUST_FIXTURES, RUST_FIXTURES,
        run_inventory,
    };

    const OTHER: FixtureId = FixtureId {
        case: "other",
        ..KEY_ROTATION
    };
    const FAILURE: &str = "contract failed";

    fn pass() -> Result<(), String> {
        Ok(())
    }
    fn fail() -> Result<(), String> {
        Err(FAILURE.into())
    }

    #[test]
    fn rust_inventory_executes_real_key_rotation() {
        let passed = run_inventory(RUST_FIXTURES, REQUIRED_RUST_FIXTURES).unwrap();
        assert_eq!(passed.len(), 2);
        assert!(passed.contains(&KEY_ROTATION));
        assert!(passed.contains(&CUSTOM_CHAT));
    }

    #[test]
    fn failed_checks_do_not_report_coverage() {
        let fixtures = [Fixture {
            id: OTHER,
            check: fail,
        }];
        let errors = run_inventory(&fixtures, &[]).unwrap_err();
        assert!(errors[0].contains(FAILURE));
    }

    #[test]
    fn missing_required_fixture_is_rejected() {
        let fixtures = [Fixture {
            id: KEY_ROTATION,
            check: pass,
        }];
        let errors = run_inventory(&fixtures, &[KEY_ROTATION, OTHER]).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("missing fixture") && error.contains("other"))
        );
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let fixtures = [
            Fixture {
                id: OTHER,
                check: pass,
            },
            Fixture {
                id: OTHER,
                check: pass,
            },
        ];
        let errors = run_inventory(&fixtures, &[]).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("duplicate fixture"))
        );
    }
}
