//! Dispatch-level regression for same-process per-path write serialization.
//!
//! Loads the real `batch`, `edit`, `write`, `memory`, and `skill` plugins
//! onto an in-memory backend and routes every child through
//! `maki-agent::tool_dispatch::run` via the real `maki.agent.call_tool`, so
//! the keyed write lock surrounds the complete Lua read-modify-write
//! handler. The bypass-lock reproducer below proves the green test would
//! catch the old last-writer-wins interleaving: identical inputs, no lock,
//! deterministic lost update.

#![cfg(test)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use maki_agent::agent::tool_dispatch::{self, Emit};
use maki_agent::tools::{ToolContext, ToolRegistry};
use maki_agent::{AgentMode, ToolDoneEvent};
use maki_config::PluginsConfig;
use serde_json::{Map, Value, json};

use crate::api::fs::{FsBackend, InMemoryFs};

const FILE: &str = "/tmp/writelock/file.txt";
const STATE_DIR: &str = crate::test_support::TEST_STATE_DIR;

fn boot_with_backend(
    plugins: &[&str],
    backend: Arc<dyn FsBackend>,
    opts: HashMap<String, Map<String, Value>>,
) -> (Arc<ToolRegistry>, crate::PluginHost) {
    let registry = Arc::new(ToolRegistry::new());
    let mut host = crate::PluginHost::with_fs_for_tests(
        Arc::clone(&registry),
        backend,
        PathBuf::from(STATE_DIR),
    )
    .unwrap();
    let config = PluginsConfig {
        enabled: true,
        names: plugins.iter().map(|s| s.to_string()).collect(),
        opts,
    };
    host.load_builtins(&config).unwrap();
    (registry, host)
}

fn boot(fs: Arc<InMemoryFs>, plugins: &[&str]) -> (Arc<ToolRegistry>, crate::PluginHost) {
    boot_with_backend(plugins, fs as _, HashMap::new())
}

fn shared_ctx(registry: &Arc<ToolRegistry>) -> ToolContext {
    let mut ctx = maki_agent::tools::test_support::stub_ctx(&AgentMode::Build);
    ctx.registry = Arc::clone(registry);
    ctx
}

fn dispatch(ctx: &ToolContext, id: &str, name: &str, input: Value) -> ToolDoneEvent {
    smol::block_on(tool_dispatch::run(
        &ctx.registry,
        None,
        id.into(),
        name,
        &input,
        ctx,
        Emit::Silent,
    ))
}

fn file_content(fs: &InMemoryFs, path: &str) -> String {
    fs.files()
        .into_iter()
        .find(|(p, _)| p.to_string_lossy() == path)
        .map(|(_, bytes)| String::from_utf8(bytes).expect("utf8 content"))
        .expect("file exists on the backend")
}

fn edit_opts() -> HashMap<String, Map<String, Value>> {
    HashMap::from([(
        "edit".to_string(),
        Map::from_iter([
            ("edit_lines".to_string(), json!(true)),
            ("insert_lines".to_string(), json!(true)),
        ]),
    )])
}

#[test]
fn batch_edits_same_file_preserve_all_replacements() {
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(std::path::Path::new(FILE), b"alpha\nbeta\ngamma\n".to_vec());
    let (registry, _host) = boot(Arc::clone(&fs), &["batch", "edit", "write"]);
    let ctx = shared_ctx(&registry);

    let done = dispatch(
        &ctx,
        "batch1",
        "batch",
        json!({
            "tool_calls": [
                {"tool": "edit", "parameters": {
                    "path": FILE, "old_string": "alpha", "new_string": "ALPHA"}},
                {"tool": "edit", "parameters": {
                    "path": FILE, "old_string": "gamma", "new_string": "GAMMA"}},
            ]
        }),
    );

    assert!(!done.is_error, "batch failed: {}", done.output.as_text());
    assert!(
        done.output
            .as_text()
            .contains("All 2 tools executed successfully."),
        "every child must report success: {}",
        done.output.as_text()
    );
    assert_eq!(file_content(&fs, FILE), "ALPHA\nbeta\nGAMMA\n");
}

/// Manual red light for the batch regression: same real plugins, but the
/// two edit invocations bypass the dispatch lock and hit a backend that
/// holds both reads until they share one snapshot. The last write wins,
/// exactly the race the lock prevents. If the green batch test ever stops
/// exercising serialization (lock removed, or handlers called directly),
/// this fixture still proves the mechanism that would catch it.
#[test]
fn edit_handlers_bypassing_lock_lose_updates() {
    let barrier = ReadBarrierFs::new();
    barrier
        .fs
        .seed(std::path::Path::new(FILE), b"alpha\nbeta\ngamma\n".to_vec());
    let (registry, _host) = boot_with_backend(&["edit"], Arc::clone(&barrier) as _, HashMap::new());
    let ctx = shared_ctx(&registry);

    let entry = registry.get("edit").expect("edit tool registered");
    let inv_a = entry
        .tool
        .parse(&json!({"path": FILE, "old_string": "alpha", "new_string": "ALPHA"}))
        .expect("parse");
    let inv_b = entry
        .tool
        .parse(&json!({"path": FILE, "old_string": "gamma", "new_string": "GAMMA"}))
        .expect("parse");

    let ctx_a = ctx.clone();
    let ctx_b = ctx.clone();
    let a = smol::spawn(async move { inv_a.execute(&ctx_a).await });
    let b = smol::spawn(async move { inv_b.execute(&ctx_b).await });
    let both = smol::block_on(barrier.both_read());
    assert_eq!(both, 2, "both edits must read before either writes");
    barrier.release_reads();
    let done = smol::block_on(async { (a.await, b.await) });

    assert!(!done.0.output.as_ref().is_err());
    assert!(!done.1.output.as_ref().is_err());

    let content = file_content(&barrier.fs, FILE);
    assert_ne!(
        content, "ALPHA\nbeta\nGAMMA\n",
        "the bypassed race must lose one update"
    );
    assert!(
        content == "ALPHA\nbeta\ngamma\n" || content == "alpha\nbeta\nGAMMA\n",
        "exactly one replacement survives the race: {content:?}"
    );
}

#[test]
fn mutable_tools_share_path_lock() {
    // edit + multiedit over disjoint regions of one file.
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(std::path::Path::new(FILE), b"aaa\nbbb\nccc\nddd\n".to_vec());
    let (registry, _host) =
        boot_with_backend(&["edit", "write"], Arc::clone(&fs) as _, edit_opts());
    let ctx = shared_ctx(&registry);
    let d1 = dispatch(
        &ctx,
        "m1",
        "edit",
        json!({"path": FILE, "old_string": "aaa", "new_string": "AAA"}),
    );
    let d2 = dispatch(
        &ctx,
        "m2",
        "multiedit",
        json!({"path": FILE, "edits": [
            {"old_string": "ccc", "new_string": "CCC"},
            {"old_string": "ddd", "new_string": "DDD"},
        ]}),
    );
    assert!(!d1.is_error, "edit failed: {}", d1.output.as_text());
    assert!(!d2.is_error, "multiedit failed: {}", d2.output.as_text());
    assert_eq!(file_content(&fs, FILE), "AAA\nbbb\nCCC\nDDD\n");

    // edit + edit_lines: the line tool shares the same key.
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(std::path::Path::new(FILE), b"one\ntwo\nthree\n".to_vec());
    let (registry, _host) =
        boot_with_backend(&["edit", "write"], Arc::clone(&fs) as _, edit_opts());
    let ctx = shared_ctx(&registry);
    let d1 = dispatch(
        &ctx,
        "e1",
        "edit",
        json!({"path": FILE, "old_string": "one", "new_string": "ONE"}),
    );
    let d2 = dispatch(
        &ctx,
        "e2",
        "edit_lines",
        json!({"path": FILE, "start": 3, "end": 3, "new_string": "THREE"}),
    );
    assert!(!d1.is_error, "edit failed: {}", d1.output.as_text());
    assert!(!d2.is_error, "edit_lines failed: {}", d2.output.as_text());
    assert_eq!(file_content(&fs, FILE), "ONE\ntwo\nTHREE\n");

    // edit + write overlap: a serialized outcome is entirely one handler's
    // result; a raced one would be a torn mix.
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(std::path::Path::new(FILE), b"old\n".to_vec());
    let (registry, _host) =
        boot_with_backend(&["edit", "write"], Arc::clone(&fs) as _, edit_opts());
    let ctx = shared_ctx(&registry);
    let d1 = dispatch(
        &ctx,
        "w1",
        "edit",
        json!({"path": FILE, "old_string": "old", "new_string": "new"}),
    );
    let d2 = dispatch(
        &ctx,
        "w2",
        "write",
        json!({"path": FILE, "content": "written\n"}),
    );
    assert!(!d1.is_error, "edit failed: {}", d1.output.as_text());
    assert!(!d2.is_error, "write failed: {}", d2.output.as_text());
    let content = file_content(&fs, FILE);
    assert!(
        content == "new\n" || content == "written\n",
        "serialized outcome only, got: {content:?}"
    );

    // insert_lines joins the same namespace.
    let fs = Arc::new(InMemoryFs::new());
    fs.seed(std::path::Path::new(FILE), b"a\nc\n".to_vec());
    let (registry, _host) =
        boot_with_backend(&["edit", "write"], Arc::clone(&fs) as _, edit_opts());
    let ctx = shared_ctx(&registry);
    let d1 = dispatch(
        &ctx,
        "i1",
        "edit",
        json!({"path": FILE, "old_string": "c", "new_string": "C"}),
    );
    let d2 = dispatch(
        &ctx,
        "i2",
        "insert_lines",
        json!({"path": FILE, "line": 1, "new_string": "b"}),
    );
    assert!(!d1.is_error, "edit failed: {}", d1.output.as_text());
    assert!(!d2.is_error, "insert_lines failed: {}", d2.output.as_text());
    assert_eq!(file_content(&fs, FILE), "a\nb\nC\n");
}

/// The built-in write and edit handlers must pick the atomic whole-file
/// write, never a plain one.
#[test]
fn edit_and_write_handlers_use_atomic_write() {
    let watch = Watch::new();
    watch
        .fs
        .seed(std::path::Path::new(FILE), b"before\n".to_vec());
    let (registry, _host) = boot_with_watch(&watch, &["edit", "write"]);
    let ctx = shared_ctx(&registry);

    let edit = dispatch(
        &ctx,
        "e1",
        "edit",
        json!({"path": FILE, "old_string": "before", "new_string": "after"}),
    );
    assert!(!edit.is_error, "edit failed: {}", edit.output.as_text());

    let write = dispatch(
        &ctx,
        "w1",
        "write",
        json!({"path": FILE, "content": "next\n"}),
    );
    assert!(!write.is_error, "write failed: {}", write.output.as_text());

    assert!(
        watch.atomic.load(Ordering::SeqCst),
        "handlers must use atomic_write"
    );
    assert!(
        !watch.plain.load(Ordering::SeqCst),
        "handlers must not use plain write"
    );
}

#[test]
fn memory_handler_uses_atomic_write() {
    let watch = Watch::new();
    let (registry, _host) = boot_with_watch(&watch, &["memory"]);
    let ctx = shared_ctx(&registry);

    let done = dispatch(
        &ctx,
        "m1",
        "memory",
        json!({"command": "write", "path": "notes.md", "content": "body", "tags": ["t"]}),
    );
    assert!(!done.is_error, "memory failed: {}", done.output.as_text());
    assert!(
        watch.atomic.load(Ordering::SeqCst),
        "memory must use atomic_write"
    );
    assert!(
        !watch.plain.load(Ordering::SeqCst),
        "memory must not use plain write"
    );
}

#[test]
fn skill_handler_uses_atomic_write() {
    let watch = Watch::new();
    let (registry, _host) = boot_with_watch(&watch, &["skill"]);
    let ctx = shared_ctx(&registry);

    let done = dispatch(&ctx, "s1", "skill", json!({"name": "maki-plugin-dev"}));
    assert!(!done.is_error, "skill failed: {}", done.output.as_text());
    assert!(
        watch.atomic.load(Ordering::SeqCst),
        "skill must use atomic_write"
    );
    assert!(
        !watch.plain.load(Ordering::SeqCst),
        "skill must not use plain write"
    );
}

fn boot_with_watch(watch: &Watch, plugins: &[&str]) -> (Arc<ToolRegistry>, crate::PluginHost) {
    boot_with_backend(plugins, Arc::new(watch.clone()) as _, HashMap::new())
}

/// Records which whole-file write method a handler picked, delegating all
/// I/O to an in-memory backend.
#[derive(Clone)]
struct Watch {
    fs: Arc<InMemoryFs>,
    atomic: Arc<AtomicBool>,
    plain: Arc<AtomicBool>,
}

impl Watch {
    fn new() -> Self {
        Self {
            fs: Arc::new(InMemoryFs::new()),
            atomic: Arc::new(AtomicBool::new(false)),
            plain: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl FsBackend for Watch {
    fn read(&self, path: PathBuf) -> crate::api::fs::BoxFuture<'_, std::io::Result<String>> {
        self.fs.read(path)
    }
    fn read_bytes(&self, path: PathBuf) -> crate::api::fs::BoxFuture<'_, std::io::Result<Vec<u8>>> {
        self.fs.read_bytes(path)
    }
    fn stat(
        &self,
        path: PathBuf,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<crate::api::fs::FsMeta>> {
        self.fs.stat(path)
    }
    fn write(
        &self,
        path: PathBuf,
        content: Vec<u8>,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.plain.store(true, Ordering::SeqCst);
        self.fs.write(path, content)
    }
    fn atomic_write(
        &self,
        path: PathBuf,
        content: Vec<u8>,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.atomic.store(true, Ordering::SeqCst);
        self.fs.atomic_write(path, content)
    }
    fn rm(
        &self,
        path: PathBuf,
        recursive: bool,
        force: bool,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.fs.rm(path, recursive, force)
    }
    fn mkdir(
        &self,
        path: PathBuf,
        parents: bool,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.fs.mkdir(path, parents)
    }
    fn dir(
        &self,
        path: PathBuf,
        max_depth: u32,
    ) -> crate::api::fs::BoxFuture<'_, Result<Vec<(String, &'static str)>, crate::api::fs::FsError>>
    {
        self.fs.dir(path, max_depth)
    }
    fn glob(
        &self,
        patterns: Vec<String>,
        path: Option<String>,
        limit: Option<usize>,
        gitignore: bool,
        sort_mtime: bool,
    ) -> crate::api::fs::BoxFuture<'_, Result<Vec<String>, crate::api::fs::FsError>> {
        self.fs.glob(patterns, path, limit, gitignore, sort_mtime)
    }
    fn grep(
        &self,
        params: maki_agent::tools::grep::GrepParams,
    ) -> crate::api::fs::BoxFuture<
        '_,
        Result<(PathBuf, Vec<maki_agent::GrepFileEntry>), crate::api::fs::FsError>,
    > {
        self.fs.grep(params)
    }
}

/// Read barrier for the bypass-lock reproducer: the first two reads park
/// until both have arrived, then both are handed the exact snapshot the
/// second read captured, so the two handlers transform identical input no
/// matter which write lands first. Later reads (restores) bypass the gate.
struct ReadBarrierFs {
    fs: InMemoryFs,
    reads: Arc<AtomicUsize>,
    both_tx: Arc<flume::Sender<()>>,
    both_rx: Arc<flume::Receiver<()>>,
    release_tx: Arc<flume::Sender<()>>,
    release_rx: Arc<flume::Receiver<()>>,
    snapshot: std::sync::Mutex<Option<String>>,
}

impl ReadBarrierFs {
    fn new() -> Arc<Self> {
        let (both_tx, both_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        Arc::new(Self {
            fs: InMemoryFs::new(),
            reads: Arc::new(AtomicUsize::new(0)),
            both_tx: Arc::new(both_tx),
            both_rx: Arc::new(both_rx),
            release_tx: Arc::new(release_tx),
            release_rx: Arc::new(release_rx),
            snapshot: std::sync::Mutex::new(None),
        })
    }

    async fn both_read(&self) -> usize {
        let _ = self.both_rx.recv_async().await;
        self.reads.load(Ordering::SeqCst)
    }

    fn release_reads(&self) {
        // One token per parked reader; both readers get the snapshot the
        // second read captured, so identical input regardless of order.
        self.release_tx.send(()).ok();
        self.release_tx.send(()).ok();
    }
}

impl FsBackend for ReadBarrierFs {
    fn read(&self, path: PathBuf) -> crate::api::fs::BoxFuture<'_, std::io::Result<String>> {
        let reads = Arc::clone(&self.reads);
        let both_tx = Arc::clone(&self.both_tx);
        let release_rx = Arc::clone(&self.release_rx);
        let snapshot = &self.snapshot;
        let fs = &self.fs;
        Box::pin(async move {
            let n = reads.fetch_add(1, Ordering::SeqCst) + 1;
            if n <= 2 {
                if n == 2 {
                    let content = fs.read(path).await?;
                    *snapshot.lock().expect("barrier snapshot poisoned") = Some(content);
                    both_tx.send(()).ok();
                }
                let _ = release_rx.recv_async().await;
                return Ok(snapshot
                    .lock()
                    .expect("barrier snapshot poisoned")
                    .clone()
                    .unwrap());
            }
            fs.read(path).await
        })
    }
    fn read_bytes(&self, path: PathBuf) -> crate::api::fs::BoxFuture<'_, std::io::Result<Vec<u8>>> {
        self.fs.read_bytes(path)
    }
    fn stat(
        &self,
        path: PathBuf,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<crate::api::fs::FsMeta>> {
        self.fs.stat(path)
    }
    fn write(
        &self,
        path: PathBuf,
        content: Vec<u8>,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.fs.write(path, content)
    }
    fn atomic_write(
        &self,
        path: PathBuf,
        content: Vec<u8>,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.fs.atomic_write(path, content)
    }
    fn rm(
        &self,
        path: PathBuf,
        recursive: bool,
        force: bool,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.fs.rm(path, recursive, force)
    }
    fn mkdir(
        &self,
        path: PathBuf,
        parents: bool,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        self.fs.mkdir(path, parents)
    }
    fn dir(
        &self,
        path: PathBuf,
        max_depth: u32,
    ) -> crate::api::fs::BoxFuture<'_, Result<Vec<(String, &'static str)>, crate::api::fs::FsError>>
    {
        self.fs.dir(path, max_depth)
    }
    fn glob(
        &self,
        patterns: Vec<String>,
        path: Option<String>,
        limit: Option<usize>,
        gitignore: bool,
        sort_mtime: bool,
    ) -> crate::api::fs::BoxFuture<'_, Result<Vec<String>, crate::api::fs::FsError>> {
        self.fs.glob(patterns, path, limit, gitignore, sort_mtime)
    }
    fn grep(
        &self,
        params: maki_agent::tools::grep::GrepParams,
    ) -> crate::api::fs::BoxFuture<
        '_,
        Result<(PathBuf, Vec<maki_agent::GrepFileEntry>), crate::api::fs::FsError>,
    > {
        self.fs.grep(params)
    }
}

fn dispatch_async(
    ctx: &ToolContext,
    id: &'static str,
    name: &'static str,
    input: Value,
) -> impl std::future::Future<Output = ToolDoneEvent> {
    let ctx = ctx.clone();
    let input = input.clone();
    async move {
        tool_dispatch::run(
            &ctx.registry,
            None,
            id.into(),
            name,
            &input,
            &ctx,
            Emit::Silent,
        )
        .await
    }
}

/// Records the order of backend operations and can park the first `read`
/// (proof that the first handler is mid-mutation) or the first `stat` (the
/// memory delete's existence check) behind a release token. Everything else
/// delegates to the in-memory backend. The parked handler holds its write
/// lock, so a second same-path dispatch must stay blocked at the gate: the
/// event log then proves the lock, not luck, ordered the mutations.
struct ProbeFs {
    fs: InMemoryFs,
    events: Arc<Mutex<Vec<String>>>,
    read_armed: Arc<AtomicBool>,
    read_arrival_tx: flume::Sender<()>,
    read_arrival_rx: flume::Receiver<()>,
    read_release_tx: flume::Sender<()>,
    read_release_rx: flume::Receiver<()>,
    stat_armed: Arc<AtomicBool>,
    stat_arrival_tx: flume::Sender<()>,
    stat_arrival_rx: flume::Receiver<()>,
    stat_release_tx: flume::Sender<()>,
    stat_release_rx: flume::Receiver<()>,
}

impl ProbeFs {
    fn new() -> Arc<Self> {
        let (read_arrival_tx, read_arrival_rx) = flume::unbounded();
        let (read_release_tx, read_release_rx) = flume::unbounded();
        let (stat_arrival_tx, stat_arrival_rx) = flume::unbounded();
        let (stat_release_tx, stat_release_rx) = flume::unbounded();
        Arc::new(Self {
            fs: InMemoryFs::new(),
            events: Arc::new(Mutex::new(Vec::new())),
            read_armed: Arc::new(AtomicBool::new(false)),
            read_arrival_tx,
            read_arrival_rx,
            read_release_tx,
            read_release_rx,
            stat_armed: Arc::new(AtomicBool::new(false)),
            stat_arrival_tx,
            stat_arrival_rx,
            stat_release_tx,
            stat_release_rx,
        })
    }

    fn arm_read_gate(&self) {
        self.read_armed.store(true, Ordering::SeqCst);
    }

    fn arm_stat_gate(&self) {
        self.stat_armed.store(true, Ordering::SeqCst);
    }

    async fn wait_read(&self) {
        self.read_arrival_rx.recv_async().await.ok();
    }

    async fn wait_stat(&self) {
        self.stat_arrival_rx.recv_async().await.ok();
    }

    fn release_read_gate(&self) {
        self.read_release_tx.send(()).ok();
    }

    fn release_stat_gate(&self) {
        self.stat_release_tx.send(()).ok();
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().expect("probe events poisoned").clone()
    }

    fn clear_events(&self) {
        self.events.lock().expect("probe events poisoned").clear();
    }
}

impl FsBackend for ProbeFs {
    fn read(&self, path: PathBuf) -> crate::api::fs::BoxFuture<'_, std::io::Result<String>> {
        let fs = &self.fs;
        let armed = Arc::clone(&self.read_armed);
        let arrival = self.read_arrival_tx.clone();
        let release = self.read_release_rx.clone();
        let events = Arc::clone(&self.events);
        Box::pin(async move {
            events
                .lock()
                .expect("probe events poisoned")
                .push("read".into());
            if armed.swap(false, Ordering::SeqCst) {
                arrival.send(()).ok();
                release.recv_async().await.ok();
            }
            fs.read(path).await
        })
    }
    fn read_bytes(&self, path: PathBuf) -> crate::api::fs::BoxFuture<'_, std::io::Result<Vec<u8>>> {
        self.fs.read_bytes(path)
    }
    fn stat(
        &self,
        path: PathBuf,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<crate::api::fs::FsMeta>> {
        let fs = &self.fs;
        let armed = Arc::clone(&self.stat_armed);
        let arrival = self.stat_arrival_tx.clone();
        let release = self.stat_release_rx.clone();
        let events = Arc::clone(&self.events);
        Box::pin(async move {
            events
                .lock()
                .expect("probe events poisoned")
                .push("stat".into());
            if armed.swap(false, Ordering::SeqCst) {
                arrival.send(()).ok();
                release.recv_async().await.ok();
            }
            fs.stat(path).await
        })
    }
    fn write(
        &self,
        path: PathBuf,
        content: Vec<u8>,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        let fs = &self.fs;
        let events = Arc::clone(&self.events);
        Box::pin(async move {
            events
                .lock()
                .expect("probe events poisoned")
                .push("write".into());
            fs.write(path, content).await
        })
    }
    fn atomic_write(
        &self,
        path: PathBuf,
        content: Vec<u8>,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        let fs = &self.fs;
        let events = Arc::clone(&self.events);
        Box::pin(async move {
            events
                .lock()
                .expect("probe events poisoned")
                .push("write".into());
            fs.atomic_write(path, content).await
        })
    }
    fn rm(
        &self,
        path: PathBuf,
        recursive: bool,
        force: bool,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        let fs = &self.fs;
        let events = Arc::clone(&self.events);
        Box::pin(async move {
            events
                .lock()
                .expect("probe events poisoned")
                .push("rm".into());
            fs.rm(path, recursive, force).await
        })
    }
    fn mkdir(
        &self,
        path: PathBuf,
        parents: bool,
    ) -> crate::api::fs::BoxFuture<'_, std::io::Result<()>> {
        let fs = &self.fs;
        let events = Arc::clone(&self.events);
        Box::pin(async move {
            events
                .lock()
                .expect("probe events poisoned")
                .push("mkdir".into());
            fs.mkdir(path, parents).await
        })
    }
    fn dir(
        &self,
        path: PathBuf,
        max_depth: u32,
    ) -> crate::api::fs::BoxFuture<'_, Result<Vec<(String, &'static str)>, crate::api::fs::FsError>>
    {
        self.fs.dir(path, max_depth)
    }
    fn glob(
        &self,
        patterns: Vec<String>,
        path: Option<String>,
        limit: Option<usize>,
        gitignore: bool,
        sort_mtime: bool,
    ) -> crate::api::fs::BoxFuture<'_, Result<Vec<String>, crate::api::fs::FsError>> {
        self.fs.glob(patterns, path, limit, gitignore, sort_mtime)
    }
    fn grep(
        &self,
        params: maki_agent::tools::grep::GrepParams,
    ) -> crate::api::fs::BoxFuture<
        '_,
        Result<(PathBuf, Vec<maki_agent::GrepFileEntry>), crate::api::fs::FsError>,
    > {
        self.fs.grep(params)
    }
}

/// Two real dispatches through the lock against a gated backend: while the
/// first handler is parked inside its read (holding the lock), the second
/// same-path dispatch must not reach its read, and the event log must show a
/// strict read/write alternation. Fails if lock acquisition is removed.
#[test]
fn dispatched_handlers_serialize_on_the_lock() {
    smol::block_on(async {
        let probe = ProbeFs::new();
        probe
            .fs
            .seed(std::path::Path::new(FILE), b"alpha\nbeta\ngamma\n".to_vec());
        let (registry, _host) = boot_with_backend(&["edit"], Arc::clone(&probe) as _, edit_opts());
        let ctx = shared_ctx(&registry);
        probe.arm_read_gate();

        let ctx_a = ctx.clone();
        let a = smol::spawn(async move {
            dispatch_async(
                &ctx_a,
                "a1",
                "edit",
                json!({"path": FILE, "old_string": "alpha", "new_string": "ALPHA"}),
            )
            .await
        });
        probe.wait_read().await;

        let (done_tx, done_rx) = flume::unbounded();
        let ctx_b = ctx.clone();
        let b = smol::spawn(async move {
            let done = dispatch_async(
                &ctx_b,
                "b1",
                "edit",
                json!({"path": FILE, "old_string": "gamma", "new_string": "GAMMA"}),
            )
            .await;
            let _ = done_tx.send(done);
        });
        for _ in 0..200 {
            smol::future::yield_now().await;
        }
        let events = probe.events();
        assert!(
            events.iter().filter(|e| *e == "read").count() == 1,
            "the second handler must not reach its read while the first holds the lock: {events:?}"
        );
        assert!(
            done_rx.is_empty(),
            "the second handler must not complete while the first holds the lock"
        );

        probe.release_read_gate();
        let _ = b.await;
        let done = done_rx
            .recv_async()
            .await
            .expect("second dispatch finished");
        let a_done = a.await;
        assert!(
            !a_done.is_error,
            "edit A failed: {}",
            a_done.output.as_text()
        );
        assert!(!done.is_error, "edit B failed: {}", done.output.as_text());
        assert_eq!(
            probe.events(),
            ["read", "write", "read", "write"],
            "handlers must run strictly non-overlapping"
        );
        assert_eq!(file_content(&probe.fs, FILE), "ALPHA\nbeta\nGAMMA\n");
    });
}

/// memory write and delete share the lock namespace through the computed
/// `mutable_path` callback. With the delete's existence check parked, a
/// concurrent same-note write must stay blocked; release the delete and its
/// rm lands strictly before the write, so the fresh note survives. Without
/// the lock the write would land first and the rm would delete it.
#[test]
fn memory_write_and_delete_serialize_on_shared_lock() {
    smol::block_on(async {
        let probe = ProbeFs::new();
        let (registry, _host) =
            boot_with_backend(&["memory"], Arc::clone(&probe) as _, HashMap::new());
        let ctx = shared_ctx(&registry);

        let seed = dispatch(
            &ctx,
            "s1",
            "memory",
            json!({"command": "write", "path": "notes.md", "content": "seed", "tags": ["t"]}),
        );
        assert!(!seed.is_error, "seed failed: {}", seed.output.as_text());
        let note_path = probe
            .fs
            .files()
            .into_iter()
            .map(|(p, _)| p)
            .find(|p| p.to_string_lossy().ends_with("memories/notes.md"))
            .expect("seeded note exists")
            .to_path_buf();
        probe.clear_events();
        probe.arm_stat_gate();

        let ctx_a = ctx.clone();
        let a = smol::spawn(async move {
            dispatch_async(
                &ctx_a,
                "a1",
                "memory",
                json!({"command": "delete", "path": "notes.md"}),
            )
            .await
        });
        probe.wait_stat().await;

        let (done_tx, done_rx) = flume::unbounded();
        let ctx_b = ctx.clone();
        let b = smol::spawn(async move {
            let done = dispatch_async(
                &ctx_b,
                "b1",
                "memory",
                json!({"command": "write", "path": "notes.md", "content": "body", "tags": ["t"]}),
            )
            .await;
            let _ = done_tx.send(done);
        });
        for _ in 0..200 {
            smol::future::yield_now().await;
        }
        let events = probe.events();
        assert!(
            !events.iter().any(|e| e == "write"),
            "the write must not run while the delete holds the lock: {events:?}"
        );
        assert!(
            done_rx.is_empty(),
            "the write must not complete while the delete holds the lock"
        );

        probe.release_stat_gate();
        let _ = b.await;
        let done = done_rx
            .recv_async()
            .await
            .expect("second dispatch finished");
        let a_done = a.await;
        assert!(
            !a_done.is_error,
            "delete failed: {}",
            a_done.output.as_text()
        );
        assert!(!done.is_error, "write failed: {}", done.output.as_text());
        let events = probe.events();
        let stat_at = events
            .iter()
            .position(|e| e == "stat")
            .expect("delete stat");
        let rm_at = events.iter().position(|e| e == "rm").expect("delete rm");
        let write_at = events
            .iter()
            .position(|e| e == "write")
            .expect("note write");
        assert!(
            stat_at < rm_at && rm_at < write_at,
            "delete must complete before the write starts: {events:?}"
        );
        assert!(
            probe.fs.files().iter().any(|(p, _)| p == &note_path),
            "the note must exist after a write that follows a completed delete"
        );
    });
}

/// The memory tool's computed `mutable_path` resolves to the note's real
/// location (state dir + project memory suffix + relative path) for mutating
/// commands only, so its lock key matches edit/write on the same file.
#[test]
fn memory_computed_mutable_path_locks_the_real_note() {
    let fs = Arc::new(InMemoryFs::new());
    let (registry, _host) = boot_with_backend(&["memory"], fs as _, HashMap::new());
    let entry = registry.get("memory").expect("memory tool registered");

    let write = entry
        .tool
        .parse(&json!({"command": "write", "path": "notes.md", "content": "x"}))
        .expect("parse");
    let target = write
        .mutable_path()
        .map(|p| p.to_path_buf())
        .expect("memory write declares a mutable path");
    assert!(
        target.is_absolute(),
        "lock key must be absolute: {target:?}"
    );
    assert!(
        target.to_string_lossy().ends_with("memories/notes.md"),
        "lock key must be the note's real location: {target:?}"
    );

    let read = entry
        .tool
        .parse(&json!({"command": "read", "path": "notes.md"}))
        .expect("parse");
    assert!(
        read.mutable_path().is_none(),
        "memory read must not participate in write serialization"
    );
}
