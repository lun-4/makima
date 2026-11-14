use std::process::ExitCode;

fn main() -> ExitCode {
    #[cfg(target_os = "linux")]
    match boundary_pin_profile::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("boundary pin profiling requires Linux /proc/self/status");
        ExitCode::FAILURE
    }
}

#[cfg(target_os = "linux")]
mod boundary_pin_profile {
    use makima_agent::{
        AgentId, AgentLimits, AgentManagerHandle, HistorySnapshot,
        file_restore::StoredSession,
        journal::{JournalChange, JournalCommit, SessionJournal},
        rewind::RewindBoundary,
        session_recovery::RecoveryCandidate,
    };
    use makima_providers::{ContentBlock, Message, Role};
    use makima_storage::{
        StateDir,
        restore_digest::session_digest,
        session_state::{StateChange, StoredAgentLifecycle, StoredAgentRecord},
        sessions::SESSIONS_DIR,
    };
    use serde_json::json;
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
        collections::BTreeMap,
        env, fs,
        path::Path,
        process::Command,
        sync::Arc,
        time::{Duration, Instant},
    };

    const MODEL: &str = "test/rewind-model";
    const TRANSCRIPT: &str = "rewind-child";
    const PROFILE_SIZES: [usize; 3] = [10, 100, 500];
    const RESPONSE_BYTES: usize = 1024;
    const PROC_STATUS: &str = "/proc/self/status";
    const KIB: u64 = 1024;
    const ROOT_TOPIC: &str = "root implementation review";
    const CHILD_TOPIC: &str = "rewound implementation child";
    const SIBLING_TOPIC: &str = "outside verification sibling";
    const RESPONSE_BODY: &str =
        "Reviewed file contents, preserved caller state, and checked the resulting changes. ";
    const OUTSIDE_AGENTS: usize = 2;
    const INITIAL_TURN: usize = 0;
    const CHILD_TAIL_TURNS: usize = 4;
    const COMMIT_SAMPLES: usize = 32;
    const COMMIT_SLOT: &str = "profile/commit";
    const COMMIT_VALUE: &str = "fixed-size normal journal mutation";

    pub fn run() -> Result<(), String> {
        let args: Vec<_> = env::args().skip(1).filter(|arg| arg != "--bench").collect();
        if let [child, profile, size] = args.as_slice()
            && child == "--child"
        {
            let size = size.parse::<usize>().map_err(|error| error.to_string())?;
            if !PROFILE_SIZES.contains(&size) {
                return Err("unsupported profile size".into());
            }
            return match profile.as_str() {
                "commits" => {
                    journal_commit_growth_profile(size);
                    Ok(())
                }
                "pins" => {
                    boundary_pin_growth_profile(size);
                    Ok(())
                }
                _ => Err("unknown child profile".into()),
            };
        }
        let profiles: &[&str] = match args.as_slice() {
            [] => &["commits", "pins"],
            [profile] if profile == "all" => &["commits", "pins"],
            [profile] if profile == "commits" => &["commits"],
            [profile] if profile == "pins" => &["pins"],
            _ => return Err("usage: boundary_pin_profile [commits|pins|all]".into()),
        };
        for profile in profiles {
            for size in PROFILE_SIZES {
                let status = Command::new(env::current_exe().map_err(|error| error.to_string())?)
                    .args(["--child", profile, &size.to_string()])
                    .status()
                    .map_err(|error| error.to_string())?;
                if !status.success() {
                    return Err(format!(
                        "{profile} profile ({size} boundaries) failed: {status}"
                    ));
                }
            }
        }
        Ok(())
    }

    fn graph() -> (AgentManagerHandle, AgentId, AgentId) {
        let root = AgentId::generate();
        let child = AgentId::generate();
        let records = [root, child]
            .into_iter()
            .map(|id| StoredAgentRecord {
                id,
                parent: (id == child).then_some(root),
                depth: u32::from(id == child),
                transcript_key: if id == child {
                    TRANSCRIPT.into()
                } else {
                    String::new()
                },
                lifecycle: StoredAgentLifecycle::Open,
                ancestry_unknown: false,
                label: None,
                spawned_by_tool_use_id: None,
                spec: None,
                restorable: Ok(()),
                usage: Default::default(),
                created_seq: 0,
                updated_seq: 0,
                legacy: false,
            })
            .collect();
        let histories: BTreeMap<_, _> = [root, child]
            .into_iter()
            .map(|id| (id, HistorySnapshot::new(Vec::<Message>::new())))
            .collect();
        let journal = Arc::new(SessionJournal::default());
        journal
            .commit(JournalCommit {
                agents: records,
                histories: histories.clone(),
                ..Default::default()
            })
            .unwrap();
        let snapshot = journal.snapshot();
        let view = snapshot.fold(snapshot.seq).unwrap();
        let manager =
            AgentManagerHandle::restore_graph(AgentLimits::default(), journal, view, histories)
                .unwrap();
        (manager, root, child)
    }

    #[derive(Default)]
    struct AllocationCounts {
        calls: u64,
        bytes: u64,
    }

    thread_local! {
        static ALLOCATION_COUNTS: Cell<Option<AllocationCounts>> = const { Cell::new(None) };
    }

    struct ProfileAllocator;

    #[global_allocator]
    static PROFILE_ALLOCATOR: ProfileAllocator = ProfileAllocator;

    fn count_allocation(bytes: usize) {
        let _ = ALLOCATION_COUNTS.try_with(|counts| {
            if let Some(mut value) = counts.take() {
                value.calls += 1;
                value.bytes += bytes as u64;
                counts.set(Some(value));
            }
        });
    }

    unsafe impl GlobalAlloc for ProfileAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                count_allocation(layout.size());
            }
            pointer
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc_zeroed(layout) };
            if !pointer.is_null() {
                count_allocation(layout.size());
            }
            pointer
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let pointer = unsafe { System.realloc(pointer, layout, size) };
            if !pointer.is_null() {
                count_allocation(size);
            }
            pointer
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) }
        }
    }

    fn profile_commit(root: AgentId) -> JournalCommit {
        JournalCommit {
            changes: vec![JournalChange {
                agent: Some(root),
                generation: 0,
                change: StateChange::Plugin {
                    key: COMMIT_SLOT.into(),
                    version: 1,
                    value: Some(json!(COMMIT_VALUE)),
                },
            }],
            ..Default::default()
        }
    }

    fn commit_profile_fixture(boundaries: usize) -> (Arc<SessionJournal>, AgentId) {
        smol::block_on(async {
            let (manager, root, child) = graph();
            let journal = manager.journal();
            drop(manager);
            let mut histories: BTreeMap<_, _> = journal
                .snapshot()
                .histories
                .iter()
                .map(|(agent, history)| (*agent, history.clone()))
                .collect();
            append_turn(
                histories.get_mut(&root).unwrap(),
                root,
                ROOT_TOPIC,
                INITIAL_TURN,
            );
            append_turn(
                histories.get_mut(&child).unwrap(),
                child,
                CHILD_TOPIC,
                INITIAL_TURN,
            );
            journal
                .commit(JournalCommit {
                    histories: histories.clone(),
                    boundary: Some((child, 0)),
                    ..Default::default()
                })
                .unwrap();
            let boundary = RewindBoundary {
                agent: child,
                seq: journal.snapshot().seq,
                epoch: journal.snapshot().epoch,
            };
            let mut root_record = journal.agent(root).unwrap();
            for turn in 1..=boundaries {
                append_turn(histories.get_mut(&root).unwrap(), root, ROOT_TOPIC, turn);
                root_record.label = Some(format!("{ROOT_TOPIC} {turn}"));
                journal
                    .commit(JournalCommit {
                        agents: vec![root_record.clone()],
                        histories: histories.clone(),
                        boundary: Some((root, 0)),
                        changes: profile_commit(root).changes,
                        ..Default::default()
                    })
                    .unwrap();
            }
            let mut replacement = journal.prepare_rewind(boundary).await.unwrap();
            replacement.install().unwrap();
            replacement.notify();
            drop(replacement);
            (journal, root)
        })
    }

    fn journal_commit_growth_profile(boundaries: usize) {
        let (journal, root) = commit_profile_fixture(boundaries);
        let fixture = journal.snapshot();
        let pool_count = fixture
            .state
            .iter()
            .filter(|record| matches!(record.change, StateChange::BoundarySnapshot { .. }))
            .count();
        assert_eq!(pool_count, 1);
        let covered: usize = fixture
            .state
            .iter()
            .filter_map(|record| match &record.change {
                StateChange::BoundarySnapshot { snapshot, .. } => {
                    Some(snapshot["boundaries"].as_array().unwrap().len())
                }
                _ => None,
            })
            .sum();
        assert_eq!(covered, boundaries);
        let state_records = fixture.state.iter().len();
        let agent_revisions = fixture.agent_revisions.iter().len();
        let state_json_bytes = serde_json::to_vec(&fixture.state.to_vec()).unwrap().len();
        let pool_json_bytes: usize = fixture
            .state
            .iter()
            .filter_map(|record| match &record.change {
                StateChange::BoundarySnapshot { snapshot, .. } => {
                    Some(serde_json::to_vec(snapshot).unwrap().len())
                }
                _ => None,
            })
            .sum();
        let initial_revision = fixture.revision;
        drop(fixture);
        let mut elapsed = Duration::ZERO;
        for _ in 0..COMMIT_SAMPLES {
            let commit = profile_commit(root);
            let started = Instant::now();
            let result = journal.commit(commit);
            elapsed += started.elapsed();
            result.unwrap();
        }
        let mut allocations = AllocationCounts::default();
        for _ in 0..COMMIT_SAMPLES {
            let commit = profile_commit(root);
            ALLOCATION_COUNTS.with(|counts| counts.set(Some(AllocationCounts::default())));
            let result = journal.commit(commit);
            let measured = ALLOCATION_COUNTS.with(|counts| counts.replace(None).unwrap());
            result.unwrap();
            allocations.calls += measured.calls;
            allocations.bytes += measured.bytes;
        }
        let completed = journal.snapshot();
        assert_eq!(
            completed.revision,
            initial_revision + (COMMIT_SAMPLES * 2) as u64
        );
        assert_eq!(
            completed.state.iter().len(),
            state_records + COMMIT_SAMPLES * 2
        );
        assert!(completed.state.iter().rev().take(COMMIT_SAMPLES * 2).all(|record| matches!(&record.change, StateChange::Plugin { key, value: Some(value), .. } if key == COMMIT_SLOT && value == &json!(COMMIT_VALUE))));
        println!(
            "{}",
            json!({
                "benchmark": "normal_journal_commit_growth", "outside_boundaries": boundaries,
                "initial_state_records": state_records, "initial_agent_revisions": agent_revisions,
                "initial_state_json_bytes": state_json_bytes, "pool_json_bytes": pool_json_bytes,
                "boundary_pools": pool_count, "covered_boundaries": covered,
                "samples_per_pass": COMMIT_SAMPLES, "commit_total_us": elapsed.as_micros(),
                "commit_mean_ns": elapsed.as_nanos() / COMMIT_SAMPLES as u128,
                "allocation_calls_total": allocations.calls, "allocated_bytes_total": allocations.bytes,
                "allocation_calls_per_commit": allocations.calls / COMMIT_SAMPLES as u64,
                "allocated_bytes_per_commit": allocations.bytes / COMMIT_SAMPLES as u64,
                "timing_scope": "journal_commit_only_counter_disabled_inputs_prebuilt",
                "allocation_scope": "journal_commit_only_current_thread_inputs_prebuilt_realloc_counts_requested_size_not_live_bytes",
                "passes": "timing_then_allocations_32_new_state_records_between_passes",
                "debug_assertions": cfg!(debug_assertions),
                "revision": option_env!("MAKIMA_PROFILE_REVISION").unwrap_or("not_supplied"),
            })
        );
    }

    fn append_turn(history: &mut HistorySnapshot, agent: AgentId, topic: &str, turn: usize) {
        let messages = Arc::make_mut(&mut history.messages);
        messages.push(Message::user(format!(
            "{topic}: inspect revision {turn:04} for {agent}"
        )));
        let mut content = format!(
            "{topic}: revision {turn:04}, agent {agent}. Findings and implementation details: "
        );
        while content.len() < RESPONSE_BYTES {
            content.push_str(RESPONSE_BODY);
        }
        content.truncate(RESPONSE_BYTES);
        messages.push(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text: content }],
            ..Default::default()
        });
    }

    fn process_memory() -> (u64, u64) {
        let status = fs::read_to_string(PROC_STATUS).unwrap();
        let bytes = |name| {
            status
                .lines()
                .find_map(|line| {
                    let mut fields = line.split_whitespace();
                    (fields.next()? == name)
                        .then(|| fields.next().unwrap().parse::<u64>().unwrap() * KIB)
                })
                .unwrap()
        };
        (bytes("VmRSS:"), bytes("VmHWM:"))
    }

    fn sample(boundaries: usize, phase: &str, started: Instant, log: &Path, pins: usize) {
        let (rss, peak_rss) = process_memory();
        println!(
            "{}",
            json!({
                "benchmark": "boundary_pin_growth", "outside_boundaries": boundaries,
                "phase": phase, "elapsed_us": started.elapsed().as_micros(),
                "rss_bytes": rss, "peak_rss_bytes_process_lifetime": peak_rss,
                "peak_rss_scope": "includes_fixture_construction_and_all_prior_phases",
                "session_log_bytes": fs::metadata(log).unwrap().len(), "boundary_pins": pins,
                "response_bytes_per_outside_agent_turn": RESPONSE_BYTES,
                "outside_agents": OUTSIDE_AGENTS, "debug_assertions": cfg!(debug_assertions),
                "revision": option_env!("MAKIMA_PROFILE_REVISION").unwrap_or("not_supplied"),
            })
        );
    }

    fn pin_count(session: &StoredSession) -> usize {
        session
            .state()
            .iter()
            .filter_map(|record| match &record.change {
                StateChange::BoundarySnapshot { snapshot, .. } => Some(
                    snapshot
                        .get("boundaries")
                        .and_then(|value| value.as_array())
                        .map_or(1, Vec::len),
                ),
                _ => None,
            })
            .sum()
    }

    fn boundary_pin_growth_profile(boundaries: usize) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let storage = StateDir::from_path(directory.path().canonicalize().unwrap());
            let sessions_dir = storage.path().join(SESSIONS_DIR);
            let (manager, root, child) = graph();
            let journal = manager.journal();
            drop(manager);
            let sibling = AgentId::generate();
            let mut record = journal.agent(child).unwrap();
            record.id = sibling;
            record.transcript_key = sibling.to_string();
            journal
                .commit(JournalCommit {
                    agents: vec![record],
                    histories: [(sibling, HistorySnapshot::new(Vec::new()))].into(),
                    managed_histories: [root, child, sibling].into(),
                    ..Default::default()
                })
                .unwrap();
            let mut histories: BTreeMap<_, _> = journal
                .snapshot()
                .histories
                .iter()
                .map(|(agent, history)| (*agent, history.clone()))
                .collect();
            for (agent, topic) in [
                (root, ROOT_TOPIC),
                (child, CHILD_TOPIC),
                (sibling, SIBLING_TOPIC),
            ] {
                append_turn(
                    histories.get_mut(&agent).unwrap(),
                    agent,
                    topic,
                    INITIAL_TURN,
                );
            }
            journal
                .commit(JournalCommit {
                    histories: histories.clone(),
                    boundary: Some((child, 0)),
                    ..Default::default()
                })
                .unwrap();
            let boundary = RewindBoundary {
                agent: child,
                seq: journal.snapshot().seq,
                epoch: journal.snapshot().epoch,
            };
            for turn in 1..=boundaries {
                for (agent, topic) in [(root, ROOT_TOPIC), (sibling, SIBLING_TOPIC)] {
                    append_turn(histories.get_mut(&agent).unwrap(), agent, topic, turn);
                }
                if turn <= CHILD_TAIL_TURNS {
                    append_turn(histories.get_mut(&child).unwrap(), child, CHILD_TOPIC, turn);
                }
                journal
                    .commit(JournalCommit {
                        histories: histories.clone(),
                        boundary: Some((
                            if turn % OUTSIDE_AGENTS == 0 {
                                root
                            } else {
                                sibling
                            },
                            0,
                        )),
                        ..Default::default()
                    })
                    .unwrap();
            }
            let baseline = journal.checkpoint_snapshot().unwrap();
            let child_prefix_len = baseline.histories[&child].messages.len();
            let outside_lens: BTreeMap<_, _> = [root, sibling]
                .into_iter()
                .map(|agent| (agent, baseline.histories[&agent].messages.len()))
                .collect();
            let mut session = StoredSession::new(MODEL, storage.path().to_str().unwrap());
            session.bind_root_id(root).unwrap();
            baseline.apply_to(&mut session).unwrap();
            session.save(&storage).unwrap();
            journal.acknowledge(&baseline).unwrap();
            let log = sessions_dir.join(format!("{}.jsonl", session.id));
            assert_eq!(pin_count(&session), 0);
            drop((baseline, histories));
            sample(boundaries, "baseline", Instant::now(), &log, 0);

            let started = Instant::now();
            let mut replacement = journal.prepare_rewind(boundary).await.unwrap();
            let prepare_elapsed = started.elapsed();
            let pins = replacement
                .snapshot()
                .state
                .iter()
                .filter(|record| matches!(&record.change, StateChange::BoundarySnapshot { .. }))
                .count();
            assert_eq!(pins, 1);
            assert!(replacement.snapshot().histories[&child].messages.len() < child_prefix_len);
            for (agent, len) in &outside_lens {
                assert_eq!(replacement.snapshot().histories[agent].messages.len(), *len);
            }
            sample(boundaries, "prepared", started, &log, pins);
            let publish_started = Instant::now();
            replacement.snapshot().apply_to(&mut session).unwrap();
            session.save(&storage).unwrap();
            replacement.install().unwrap();
            replacement.notify();
            let transition_elapsed = prepare_elapsed + publish_started.elapsed();
            sample(boundaries, "published", publish_started, &log, pins);
            println!(
                "{}",
                json!({
                    "benchmark": "boundary_pin_growth", "outside_boundaries": boundaries,
                    "phase": "transition_summary", "prepare_us": prepare_elapsed.as_micros(),
                    "transition_us": transition_elapsed.as_micros(),
                    "transition_scope": "prepare_plus_publish_install_excludes_sampling_and_reporting",
                })
            );
            let id = session.id;
            drop((replacement, session, journal));
            sample(boundaries, "before_restart", Instant::now(), &log, pins);

            let restart_started = Instant::now();
            let loaded = StoredSession::load(id, &storage).unwrap();
            let load_elapsed = restart_started.elapsed();
            let recovery_started = Instant::now();
            let recovery = RecoveryCandidate::prepare(loaded).unwrap();
            let recovery_elapsed = recovery_started.elapsed();
            assert_eq!(pin_count(recovery.session()), boundaries);
            let publication_started = Instant::now();
            let mut published = recovery.session().clone();
            published.save(&storage).unwrap();
            let publication_elapsed = publication_started.elapsed();
            let reload_started = Instant::now();
            let reloaded = StoredSession::load(id, &storage).unwrap();
            assert_eq!(pin_count(&reloaded), boundaries);
            assert_eq!(
                session_digest(&reloaded).unwrap(),
                session_digest(&published).unwrap()
            );
            let reload_elapsed = reload_started.elapsed();
            let recovered = recovery.finalize_after_publication();
            let restarted = recovered.journal();
            let restart_elapsed = restart_started.elapsed();
            sample(boundaries, "restarted", restart_started, &log, pins);
            println!(
                "{}",
                json!({
                    "benchmark": "boundary_pin_growth", "outside_boundaries": boundaries,
                    "phase": "restart_summary", "load_us": load_elapsed.as_micros(),
                    "recovery_us": recovery_elapsed.as_micros(),
                    "publication_us": publication_elapsed.as_micros(), "reload_check_us": reload_elapsed.as_micros(),
                    "restart_us": restart_elapsed.as_micros(),
                })
            );
            let snapshot = restarted.snapshot();
            let oldest_outside = snapshot
                .state
                .iter()
                .find(|record| {
                    matches!(&record.change, StateChange::Boundary { .. })
                        && record
                            .agent
                            .is_some_and(|agent| outside_lens.contains_key(&agent))
                })
                .map(|record| RewindBoundary {
                    agent: record.agent.unwrap(),
                    seq: record.seq,
                    epoch: snapshot.epoch,
                })
                .unwrap();
            let pinned_agent = oldest_outside.agent;
            for (agent, len) in &outside_lens {
                assert_eq!(snapshot.histories[agent].messages.len(), *len);
            }
            let mut outside_rewind = restarted.prepare_rewind(oldest_outside).await.unwrap();
            assert!(
                outside_rewind.snapshot().histories[&pinned_agent]
                    .messages
                    .len()
                    < outside_lens[&pinned_agent]
            );
            outside_rewind.install().unwrap();
            outside_rewind.notify();
            assert!(
                restarted.snapshot().histories[&pinned_agent].messages.len()
                    < outside_lens[&pinned_agent]
            );
        });
    }
}
