use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

use maki_storage::{
    StateDir,
    auth::{CredentialError, CredentialLifecycle, CredentialScope, RefreshOutcome},
};
use serde_json::{Value, json};

const SCOPE: &str = "refresh_test";
const CHILD_DIR: &str = "MAKI_CREDENTIAL_REFRESH_CHILD_DIR";
const READY: &str = "credential-child-ready";
const LIFECYCLE_CHILD_DIR: &str = "MAKI_CREDENTIAL_LIFECYCLE_CHILD_DIR";

fn scope(path: &std::path::Path) -> CredentialScope {
    CredentialScope::new(StateDir::from_path(path.to_owned()), SCOPE).unwrap()
}

fn rotate(current: Option<Value>) -> RefreshOutcome {
    if current == Some(json!({ "token": "old" })) {
        RefreshOutcome::Updated(json!({ "token": "new" }))
    } else {
        assert_eq!(current, Some(json!({ "token": "new" })));
        RefreshOutcome::Unchanged
    }
}

#[test]
fn concurrent_refresh_reloads_under_lock() {
    const WORKERS: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let scope = scope(dir.path());
    smol::block_on(scope.write(&json!({ "token": "old" }))).unwrap();
    let barrier = Arc::new(Barrier::new(WORKERS));
    let rotations = Arc::new(AtomicUsize::new(0));
    let workers: Vec<_> = (0..WORKERS)
        .map(|_| {
            let scope = scope.clone();
            let barrier = barrier.clone();
            let rotations = rotations.clone();
            thread::spawn(move || {
                barrier.wait();
                smol::block_on(scope.refresh(|current| async move {
                    let outcome = rotate(current);
                    if matches!(outcome, RefreshOutcome::Updated(_)) {
                        rotations.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(outcome)
                }))
                .unwrap()
            })
        })
        .collect();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), Some(json!({ "token": "new" })));
    }
    assert_eq!(rotations.load(Ordering::SeqCst), 1);
    assert_eq!(scope.read().unwrap(), Some(json!({ "token": "new" })));
}

#[test]
fn invalidation_fences_stale_generation_across_instances() {
    let dir = tempfile::tempdir().unwrap();
    let state = StateDir::from_path(dir.path().to_owned());
    let first = CredentialLifecycle::new(state.clone(), SCOPE).unwrap();
    let second = CredentialLifecycle::new(state.clone(), SCOPE).unwrap();
    let scope = CredentialScope::new(state, SCOPE).unwrap();
    let old = smol::block_on(first.snapshot()).unwrap();
    let current = smol::block_on(second.invalidate()).unwrap();
    let result = smol::block_on(first.refresh(&old, &scope, |_| async {
        Ok(RefreshOutcome::Updated(json!({"token": "stale"})))
    }));
    assert!(matches!(result, Err(CredentialError::StaleGeneration)));
    assert!(scope.read().unwrap().is_none());
    smol::block_on(second.refresh(&current, &scope, |_| async {
        Ok(RefreshOutcome::Updated(json!({"token": "fresh"})))
    }))
    .unwrap();
    assert_eq!(scope.read().unwrap(), Some(json!({"token": "fresh"})));
}

#[test]
fn lifecycle_stale_child() {
    let Some(path) = std::env::var_os(LIFECYCLE_CHILD_DIR) else {
        return;
    };
    let state = StateDir::from_path(path.into());
    let lifecycle = CredentialLifecycle::new(state.clone(), SCOPE).unwrap();
    let scope = CredentialScope::new(state, SCOPE).unwrap();
    let generation = smol::block_on(lifecycle.snapshot()).unwrap();
    println!("{READY}");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    let result = smol::block_on(lifecycle.refresh(&generation, &scope, |_| async {
        Ok(RefreshOutcome::Updated(json!({"token": "stale"})))
    }));
    assert!(matches!(result, Err(CredentialError::StaleGeneration)));
}

#[test]
fn logout_fences_login_from_another_process() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lifecycle_stale_child", "--nocapture"])
        .env(LIFECYCLE_CHILD_DIR, dir.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(output.read_line(&mut line).unwrap(), 0);
        if line.contains(READY) {
            break;
        }
    }
    let lifecycle =
        CredentialLifecycle::new(StateDir::from_path(dir.path().to_owned()), SCOPE).unwrap();
    smol::block_on(lifecycle.invalidate()).unwrap();
    writeln!(child.stdin.take().unwrap(), "continue").unwrap();
    assert!(child.wait().unwrap().success());
    assert!(scope(dir.path()).read().unwrap().is_none());
}

#[test]
fn credential_refresh_child() {
    let Some(path) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    println!("{READY}");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    let scope = scope(std::path::Path::new(&path));
    let result = smol::block_on(scope.refresh(|current| async move {
        let outcome = rotate(current);
        if matches!(outcome, RefreshOutcome::Updated(_)) {
            println!("credential-rotated");
        }
        Ok(outcome)
    }))
    .unwrap();
    assert_eq!(result, Some(json!({ "token": "new" })));
}

#[test]
fn cross_process_refresh_reloads_under_lock() {
    let dir = tempfile::tempdir().unwrap();
    let scope = scope(dir.path());
    smol::block_on(scope.write(&json!({ "token": "old" }))).unwrap();
    let mut children: Vec<_> = (0..2)
        .map(|_| {
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "credential_refresh_child", "--nocapture"])
                .env(CHILD_DIR, dir.path())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut output = BufReader::new(child.stdout.take().unwrap());
            loop {
                let mut line = String::new();
                assert_ne!(
                    output.read_line(&mut line).unwrap(),
                    0,
                    "child exited before ready"
                );
                if line.contains(READY) {
                    break;
                }
            }
            (child, output)
        })
        .collect();
    for (child, _) in &mut children {
        writeln!(child.stdin.take().unwrap(), "go").unwrap();
    }
    let mut rotations = 0;
    for (mut child, mut output) in children {
        let mut remaining = String::new();
        std::io::Read::read_to_string(&mut output, &mut remaining).unwrap();
        assert!(child.wait().unwrap().success());
        rotations += remaining.matches("credential-rotated").count();
    }
    assert_eq!(rotations, 1);
    assert_eq!(scope.read().unwrap(), Some(json!({ "token": "new" })));
}
