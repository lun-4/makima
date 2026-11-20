//! Ordered, retrying durability queue for one session log.

use crate::{ToolOutput, journal::SessionJournal};
use makima_providers::{Message, TokenUsage};
use makima_storage::{
    StorageError,
    id::MakimaId,
    session_lock::SessionPublicationGuard,
    sessions::{Session, SessionError, migrate_v2_before_append, plan_session_append_with_aliases},
};
use serde_json::Value;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, create_dir_all},
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf, absolute},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock, Weak},
    task::{Context, Poll},
    time::Duration,
};
use tracing::warn;

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(10);
const CLOSED: &str = "session writer closed";
type WriterSlot = Arc<OnceLock<Result<Arc<WriterHandle>, WriterError>>>;
static WRITERS: OnceLock<Mutex<HashMap<PathBuf, WriterSlot>>> = OnceLock::new();

pub type OpenedSessionIo = (Box<dyn SessionIo>, u64, Vec<u8>);

pub trait SessionIo: Send + 'static {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize>;
    fn sync(&mut self) -> io::Result<()>;
}

impl SessionIo for File {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        Write::write(self, bytes)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

struct SessionFile {
    file: File,
    directory: File,
    directory_synced: bool,
}

impl SessionIo for SessionFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        Write::write(&mut self.file, bytes)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        if !self.directory_synced {
            self.directory.sync_all()?;
            self.directory_synced = true;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("session {session_id} was not saved: {message}")]
pub struct WriterError {
    pub session_id: MakimaId,
    pub message: Arc<str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendAck {
    pub next_seq: u64,
}

#[derive(Clone, Debug, Default)]
pub struct WriterStatus {
    pub unsaved_records: usize,
    pub error: Option<WriterError>,
}

type ReceiptFuture = Pin<Box<dyn Future<Output = Result<AppendAck, WriterError>> + Send>>;

pub struct Receipt(ReceiptFuture);

impl Future for Receipt {
    type Output = Result<AppendAck, WriterError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

struct Request {
    payload: Payload,
    guard: Option<SessionPublicationGuard>,
    reply: flume::Sender<Result<AppendAck, WriterError>>,
}

enum Payload {
    Close,
    Records(Vec<Value>),
    Checkpoint(Box<StoredSession>),
}

pub type StoredSession = Session<Message, TokenUsage, ToolOutput>;

struct WriterHandle {
    session_id: MakimaId,
    tx: flume::Sender<Request>,
    status: Arc<Mutex<WriterStatus>>,
    journals: Arc<Mutex<Vec<Weak<SessionJournal>>>>,
}

#[derive(Clone)]
pub struct SessionWriter(Arc<WriterHandle>);

impl SessionWriter {
    /// Opens a JSONL log once per canonical path. The directory must exist.
    pub fn open(path: &Path, session_id: MakimaId) -> Result<Self, WriterError> {
        Self::open_with_snapshot(path, session_id, None)
    }

    pub fn open_session(directory: &Path, session: &StoredSession) -> Result<Self, WriterError> {
        Self::open_with_snapshot(
            &directory.join(format!("{}.jsonl", session.id)),
            session.id,
            Some(session),
        )
    }

    fn open_with_snapshot(
        path: &Path,
        session_id: MakimaId,
        session: Option<&StoredSession>,
    ) -> Result<Self, WriterError> {
        let path = absolute(path).map_err(|error| writer_error(session_id, error))?;
        let slot = {
            let mut writers = WRITERS
                .get_or_init(Mutex::default)
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            Arc::clone(writers.entry(path.clone()).or_default())
        };
        let session = session.cloned();
        slot.get_or_init(|| {
            Ok(Self::with_factory(session_id, move || {
                let directory = path.parent().unwrap_or_else(|| Path::new("."));
                create_dir_all(directory)?;
                if let Some(session) = &session {
                    migrate_v2_before_append(directory, session).map_err(io::Error::other)?;
                }
                let (file, next_seq, history) = open_log(&path)?;
                let directory = File::open(directory)?;
                Ok((
                    Box::new(SessionFile {
                        file,
                        directory,
                        directory_synced: false,
                    }),
                    next_seq,
                    history,
                ))
            })
            .0)
        })
        .clone()
        .map(Self)
    }

    pub fn with_io(session_id: MakimaId, io: impl SessionIo, next_seq: u64) -> Self {
        let mut opened = Some((Box::new(io) as Box<dyn SessionIo>, next_seq, Vec::new()));
        Self::with_factory(session_id, move || {
            opened.take().ok_or_else(|| io::Error::other(CLOSED))
        })
    }

    pub fn with_factory(
        session_id: MakimaId,
        factory: impl FnMut() -> io::Result<OpenedSessionIo> + Send + 'static,
    ) -> Self {
        let (tx, rx) = flume::unbounded();
        let status = Arc::new(Mutex::new(WriterStatus::default()));
        let journals = Arc::new(Mutex::new(Vec::new()));
        smol::spawn(initialize(
            session_id,
            Box::new(factory),
            rx,
            Arc::clone(&status),
            Arc::clone(&journals),
        ))
        .detach();
        Self(Arc::new(WriterHandle {
            session_id,
            tx,
            status,
            journals,
        }))
    }

    /// Enqueues immediately, even when the returned receipt is not polled.
    pub fn append(&self, records: Vec<Value>) -> Receipt {
        self.enqueue(Payload::Records(records), None)
    }

    pub fn checkpoint(&self, session: StoredSession) -> Receipt {
        self.enqueue(Payload::Checkpoint(Box::new(session)), None)
    }

    fn enqueue(&self, payload: Payload, guard: Option<SessionPublicationGuard>) -> Receipt {
        let (reply, response) = flume::bounded(1);
        let sent = self.0.tx.send(Request {
            payload,
            guard,
            reply,
        });
        let session_id = self.0.session_id;
        Receipt(Box::pin(async move {
            sent.map_err(|_| writer_error(session_id, CLOSED))?;
            response
                .recv_async()
                .await
                .unwrap_or_else(|_| Err(writer_error(session_id, CLOSED)))
        }))
    }

    pub fn attach_journal(&self, journal: &Arc<SessionJournal>) {
        let mut journals = self
            .0
            .journals
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        journals.retain(|journal| journal.strong_count() > 0);
        if !journals
            .iter()
            .any(|current| current.ptr_eq(&Arc::downgrade(journal)))
        {
            journals.push(Arc::downgrade(journal));
        }
        journal.set_writer_error(self.status().error.map(|error| error.to_string()));
    }

    pub fn status(&self) -> WriterStatus {
        self.0
            .status
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

fn writer_error(session_id: MakimaId, error: impl ToString) -> WriterError {
    WriterError {
        session_id,
        message: error.to_string().into(),
    }
}

fn open_log(path: &Path) -> io::Result<(File, u64, Vec<u8>)> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let complete = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    let mut next_seq = 0;
    for line in bytes[..complete]
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: Value = serde_json::from_slice(line)?;
        if let Some(seq) = record.get("seq").and_then(Value::as_u64) {
            next_seq = next_seq.max(
                seq.checked_add(1)
                    .ok_or_else(|| io::Error::other("session sequence exhausted"))?,
            );
        }
    }
    file.set_len(complete as u64)?;
    file.seek(SeekFrom::End(0))?;
    bytes.truncate(complete);
    Ok((file, next_seq, bytes))
}

async fn initialize(
    session_id: MakimaId,
    mut factory: Box<dyn FnMut() -> io::Result<OpenedSessionIo> + Send>,
    rx: flume::Receiver<Request>,
    status: Arc<Mutex<WriterStatus>>,
    journals: Arc<Mutex<Vec<Weak<SessionJournal>>>>,
) {
    let Ok(first) = rx.recv_async().await else {
        return;
    };
    let mut pending = vec![first];
    let mut backoff = INITIAL_BACKOFF;
    loop {
        pending.extend(rx.try_iter());
        if let Some(index) = pending
            .iter()
            .position(|request| matches!(request.payload, Payload::Close))
        {
            let request = pending.swap_remove(index);
            let _ = request.reply.send(Ok(AppendAck { next_seq: 0 }));
            return;
        }
        let guard = pending.iter().find_map(|request| request.guard.clone());
        let (returned_factory, opened) = smol::unblock(move || {
            let result = match guard {
                Some(guard) => guard.publish(&mut factory).and_then(|result| {
                    result.unwrap_or_else(|| Err(io::Error::other("session ownership lost")))
                }),
                None => factory(),
            };
            (factory, result)
        })
        .await;
        factory = returned_factory;
        match opened {
            Ok((io, next_seq, history)) => {
                run(
                    session_id,
                    (io, next_seq, history),
                    rx,
                    status,
                    journals,
                    pending,
                )
                .await;
                return;
            }
            Err(error) => {
                let error = writer_error(session_id, error);
                *status.lock().unwrap_or_else(|error| error.into_inner()) = WriterStatus {
                    unsaved_records: pending
                        .iter()
                        .map(|request| match &request.payload {
                            Payload::Records(records) => records.len(),
                            Payload::Checkpoint(_) => 1,
                            Payload::Close => 0,
                        })
                        .sum(),
                    error: Some(error.clone()),
                };
                update_journals(&journals, Some(error.to_string()));
                for request in &mut pending {
                    let _ = request.reply.try_send(Err(error.clone()));
                    let (reply, receiver) = flume::bounded(1);
                    drop(receiver);
                    request.reply = reply;
                }
                warn!(%session_id, %error, "session initialization failed; retrying retained records");
                smol::Timer::after(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn run(
    session_id: MakimaId,
    opened: OpenedSessionIo,
    rx: flume::Receiver<Request>,
    status: Arc<Mutex<WriterStatus>>,
    journals: Arc<Mutex<Vec<Weak<SessionJournal>>>>,
    mut pending: Vec<Request>,
) {
    let (mut io, mut next_seq, mut history) = opened;
    let mut bytes = Vec::new();
    let mut written = 0;
    let mut retained = 0;
    let mut replies = Vec::new();
    let mut pending_aliases = Vec::new();
    let mut publication_guard: Option<SessionPublicationGuard> = None;
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let request = if retained == 0 && pending.is_empty() {
            match rx.recv_async().await {
                Ok(request) => Some(request),
                Err(_) => break,
            }
        } else {
            None
        };
        for request in pending.drain(..).chain(request).chain(rx.try_iter()) {
            if request.guard.is_some() {
                publication_guard = request.guard;
            }
            let mut encoded = Vec::new();
            let mut seq = next_seq;
            let records = match request.payload {
                Payload::Close => {
                    drop(io);
                    let _ = request.reply.send(Ok(AppendAck { next_seq }));
                    return;
                }
                Payload::Records(records) => Ok(records),
                Payload::Checkpoint(session) => {
                    plan_session_append_with_aliases(&history, session.as_ref())
                        .map_err(|error| writer_error(session_id, error))
                        .and_then(|planned| {
                            let records = planned
                                .bytes
                                .split(|byte| *byte == b'\n')
                                .filter(|line| !line.is_empty())
                                .map(|line| {
                                    serde_json::from_slice(line)
                                        .map_err(|error| writer_error(session_id, error))
                                })
                                .collect::<Result<Vec<Value>, _>>()?;
                            pending_aliases.push((session, planned.aliases));
                            Ok(records)
                        })
                }
            };
            let records: Vec<Value> = match records {
                Ok(records) => records,
                Err(error) => {
                    let _ = request.reply.send(Err(error));
                    continue;
                }
            };
            let result = records.iter().try_for_each(|record| {
                let mut record = record.clone();
                let object = record
                    .as_object_mut()
                    .ok_or_else(|| io::Error::other("session records must be JSON objects"))?;
                if object.get("t").and_then(Value::as_str) == Some("agent")
                    && let Some(journal_seq) = object.remove("seq")
                {
                    object.insert("journal_seq".into(), journal_seq);
                }
                object.insert("seq".into(), Value::from(seq));
                seq = seq
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("session sequence exhausted"))?;
                serde_json::to_writer(&mut encoded, &record)?;
                encoded.push(b'\n');
                Ok::<_, io::Error>(())
            });
            if let Err(error) = result {
                let _ = request.reply.send(Err(writer_error(session_id, error)));
                continue;
            }
            next_seq = seq;
            retained += records.len();
            history.extend_from_slice(&encoded);
            bytes.extend(encoded);
            replies.push(request.reply);
        }
        status
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .unsaved_records = retained;
        let result = smol::unblock(move || {
            let mut write = || {
                while written < bytes.len() {
                    match io.write(&bytes[written..]) {
                        Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                        Ok(count) => written += count,
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => return Err(error),
                    }
                }
                io.sync()
            };
            let result = match &publication_guard {
                Some(guard) => guard.publish(&mut write).and_then(|result| {
                    result.unwrap_or_else(|| Err(io::Error::other("session ownership lost")))
                }),
                None => write(),
            };
            (io, bytes, written, publication_guard, result)
        })
        .await;
        (io, bytes, written, publication_guard) = (result.0, result.1, result.2, result.3);
        match result.4 {
            Ok(()) => {
                *status.lock().unwrap_or_else(|error| error.into_inner()) = WriterStatus::default();
                for (session, aliases) in pending_aliases.drain(..) {
                    session.commit_message_aliases(&aliases);
                }
                update_journals(&journals, None);
                for reply in replies.drain(..) {
                    let _ = reply.send(Ok(AppendAck { next_seq }));
                }
                bytes.clear();
                written = 0;
                retained = 0;
                publication_guard = None;
                backoff = INITIAL_BACKOFF;
            }
            Err(error) => {
                let error = writer_error(session_id, error);
                status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .error = Some(error.clone());
                update_journals(&journals, Some(error.to_string()));
                warn!(%session_id, %error, retained, "session has unsaved records; retrying");
                for reply in replies.drain(..) {
                    let _ = reply.send(Err(error.clone()));
                }
                smol::Timer::after(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

fn update_journals(journals: &Mutex<Vec<Weak<SessionJournal>>>, error: Option<String>) {
    let mut journals = journals.lock().unwrap_or_else(|error| error.into_inner());
    journals.retain(|journal| {
        if let Some(journal) = journal.upgrade() {
            journal.set_writer_error(error.clone());
            true
        } else {
            false
        }
    });
}

pub struct StagedCheckpoint {
    directory: PathBuf,
    session: StoredSession,
}

pub fn stage_checkpoint(
    directory: &Path,
    session: &StoredSession,
) -> Result<StagedCheckpoint, SessionError> {
    Ok(StagedCheckpoint {
        directory: directory.to_owned(),
        session: session.clone(),
    })
}

pub fn forget(directory: &Path, session_id: MakimaId) -> Result<(), SessionError> {
    let path =
        absolute(directory.join(format!("{session_id}.jsonl"))).map_err(StorageError::from)?;
    let slot = WRITERS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&path)
        .cloned();
    if let Some(slot) = slot {
        if let Some(Ok(handle)) = slot.get() {
            smol::block_on(SessionWriter(Arc::clone(handle)).enqueue(Payload::Close, None))
                .map_err(|error| StorageError::from(io::Error::other(error)))?;
        }
        let mut writers = WRITERS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if writers
            .get(&path)
            .is_some_and(|current| Arc::ptr_eq(current, &slot))
        {
            writers.remove(&path);
        }
    }
    Ok(())
}

pub fn checkpoint(directory: &Path, session: &StoredSession) -> Result<(), SessionError> {
    save_snapshot(directory, session)
}

pub fn save_snapshot(directory: &Path, session: &StoredSession) -> Result<(), SessionError> {
    let writer = SessionWriter::open_session(directory, session)
        .map_err(|error| StorageError::from(io::Error::other(error)))?;
    smol::block_on(writer.checkpoint(session.clone()))
        .map_err(|error| StorageError::from(io::Error::other(error)))?;
    Ok(())
}

impl StagedCheckpoint {
    pub fn publish(
        self,
        guard: &SessionPublicationGuard,
        claim: impl FnOnce() -> bool,
    ) -> Result<bool, SessionError> {
        let receipt = guard
            .publish(|| {
                if !claim() {
                    return Ok(None);
                }
                let writer = SessionWriter::open_session(&self.directory, &self.session)
                    .map_err(|error| StorageError::from(io::Error::other(error)))?;
                Ok::<_, SessionError>(Some(writer.enqueue(
                    Payload::Checkpoint(Box::new(self.session)),
                    Some(guard.clone()),
                )))
            })
            .map_err(StorageError::from)?
            .transpose()?
            .flatten();
        let Some(receipt) = receipt else {
            return Ok(false);
        };
        smol::block_on(receipt).map_err(|error| StorageError::from(io::Error::other(error)))?;
        Ok(true)
    }

    pub fn publish_unowned(self, claim: impl FnOnce() -> bool) -> Result<bool, SessionError> {
        if !claim() {
            return Ok(false);
        }
        save_snapshot(&self.directory, &self.session)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SessionIo, SessionWriter, StoredSession, WriterStatus, checkpoint, open_log,
        stage_checkpoint,
    };
    use crate::journal::{JournalChange, JournalCommit, SessionJournal};
    use makima_providers::Message;
    use makima_storage::id::MakimaId;
    use makima_storage::session_state::StateChange;
    use makima_storage::sessions::{encode_records, plan_session_append};
    use serde_json::{Value, json};
    use std::{
        fs::{read, remove_file, write},
        io,
        sync::{Arc, Mutex},
    };
    use test_case::test_case;

    const DISK_FULL: i32 = libc::ENOSPC;

    struct FakeIo {
        bytes: Arc<Mutex<Vec<u8>>>,
        fail_write: bool,
        fail_sync: bool,
        partial: bool,
        synced: flume::Sender<()>,
        retry_gate: Option<flume::Receiver<()>>,
    }

    impl SessionIo for FakeIo {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.partial {
                self.partial = false;
                let count = bytes.len().min(3);
                self.bytes
                    .lock()
                    .unwrap()
                    .extend_from_slice(&bytes[..count]);
                return Ok(count);
            }
            if self.fail_write {
                self.fail_write = false;
                return Err(io::Error::from_raw_os_error(DISK_FULL));
            }
            if !self.fail_sync
                && let Some(gate) = self.retry_gate.take()
            {
                gate.recv().unwrap();
            }
            self.bytes.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn sync(&mut self) -> io::Result<()> {
            if self.fail_sync {
                self.fail_sync = false;
                return Err(io::Error::from_raw_os_error(DISK_FULL));
            }
            if let Some(gate) = self.retry_gate.take() {
                gate.recv().unwrap();
            }
            let _ = self.synced.send(());
            Ok(())
        }
    }

    #[test_case(true, false, false; "enospc")]
    #[test_case(false, true, false; "fsync")]
    #[test_case(true, false, true; "partial_write")]
    fn failed_receipt_retains_records_and_retries(
        fail_write: bool,
        fail_sync: bool,
        partial: bool,
    ) {
        smol::block_on(async {
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let (synced, durable) = flume::unbounded();
            let (release_retry, retry_gate) = flume::bounded(1);
            let writer = SessionWriter::with_io(
                MakimaId::generate(),
                FakeIo {
                    bytes: Arc::clone(&bytes),
                    fail_write,
                    fail_sync,
                    partial,
                    synced,
                    retry_gate: Some(retry_gate),
                },
                0,
            );
            let journal = Arc::new(
                SessionJournal::from_session(&StoredSession::new("test", "/test")).unwrap(),
            );
            writer.attach_journal(&journal);
            let receipt = writer.append(vec![json!({"t": "test", "value": 1})]);
            assert!(receipt.await.is_err());
            let WriterStatus {
                unsaved_records,
                error,
            } = writer.status();
            assert_eq!(unsaved_records, 1);
            assert!(error.is_some());
            assert!(journal.persistence_error().is_some());
            journal.set_checkpoint_error(None);
            assert!(journal.persistence_error().is_some());
            release_retry.send(()).unwrap();
            durable.recv_async().await.unwrap();
            writer.append(Vec::new()).await.unwrap();
            assert!(writer.status().error.is_none());
            assert!(journal.persistence_error().is_none());
            let bytes = bytes.lock().unwrap();
            let records: Vec<Value> = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice(line).unwrap())
                .collect();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["value"], 1);
        });
    }

    #[test]
    fn initial_open_failure_retains_payload_and_retries_without_resubmission() {
        smol::block_on(async {
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let (synced, durable) = flume::unbounded();
            let mut first = true;
            let (release_retry, retry_gate) = flume::bounded(1);
            let written = Arc::clone(&bytes);
            let writer = SessionWriter::with_factory(MakimaId::generate(), move || {
                if first {
                    first = false;
                    return Err(io::Error::from_raw_os_error(DISK_FULL));
                }
                retry_gate.recv().unwrap();
                Ok((
                    Box::new(FakeIo {
                        bytes: Arc::clone(&written),
                        fail_write: false,
                        fail_sync: false,
                        partial: false,
                        synced: synced.clone(),
                        retry_gate: None,
                    }),
                    17,
                    Vec::new(),
                ))
            });
            let receipt = writer.append(vec![json!({"value": "retained"})]);
            assert!(receipt.await.is_err());
            assert_eq!(writer.status().unsaved_records, 1);
            assert!(writer.status().error.is_some());
            release_retry.send(()).unwrap();
            durable.recv_async().await.unwrap();
            writer.append(Vec::new()).await.unwrap();
            let bytes = bytes.lock().unwrap();
            let record: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(record["seq"], 17);
            assert_eq!(record["value"], "retained");
            assert!(writer.status().error.is_none());
        });
    }

    #[test]
    fn snapshot_bridge_appends_without_rewriting_or_duplicating_records() {
        const MODEL: &str = "writer-test-model";
        const CWD: &str = "/writer-test";
        const FIRST: &str = "first message";
        const SECOND: &str = "second message";
        let dir = tempfile::tempdir().unwrap();
        let mut session = StoredSession::new(MODEL, CWD);
        session.push_message(Message::user(FIRST.into()));
        checkpoint(dir.path(), &session).unwrap();
        let path = dir.path().join(format!("{}.jsonl", session.id));
        let prefix = read(&path).unwrap();
        checkpoint(dir.path(), &session).unwrap();
        assert_eq!(read(&path).unwrap(), prefix);
        session.push_message(Message::user(SECOND.into()));
        assert!(
            stage_checkpoint(dir.path(), &session)
                .unwrap()
                .publish_unowned(|| true)
                .unwrap()
        );
        let bytes = read(&path).unwrap();
        assert!(bytes.starts_with(&prefix));
        assert!(bytes.len() > prefix.len());
        let loaded = StoredSession::load_from(session.id, dir.path()).unwrap();
        assert_eq!(loaded.messages().len(), 2);
    }

    #[test]
    fn first_writer_open_archives_and_migrates_v2_once() {
        let dir = tempfile::tempdir().unwrap();
        let session = StoredSession::new("writer-test-model", "/writer-test");
        let mut records: Vec<Value> = plan_session_append(&[], &session)
            .unwrap()
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        records[0]["v"] = json!(2);
        let legacy = encode_records(&records).unwrap();
        let path = dir.path().join(format!("{}.jsonl", session.id));
        write(&path, &legacy).unwrap();
        checkpoint(dir.path(), &session).unwrap();
        let archive = dir
            .path()
            .join("archive")
            .join(session.id.to_string())
            .join("v2.jsonl");
        assert_eq!(read(archive).unwrap(), legacy);
        let migrated = read(&path).unwrap();
        let header: Value =
            serde_json::from_slice(migrated.split(|byte| *byte == b'\n').next().unwrap()).unwrap();
        assert_eq!(header["v"], 3);
        checkpoint(dir.path(), &session).unwrap();
        assert_eq!(read(path).unwrap(), migrated);
    }

    #[test]
    fn explicitly_forgotten_session_can_be_recreated_at_same_path() {
        let dir = tempfile::tempdir().unwrap();
        let session = StoredSession::new("writer-test-model", "/writer-test");
        checkpoint(dir.path(), &session).unwrap();
        super::forget(dir.path(), session.id).unwrap();
        let path = dir.path().join(format!("{}.jsonl", session.id));
        remove_file(&path).unwrap();
        checkpoint(dir.path(), &session).unwrap();
        assert_eq!(
            StoredSession::load_from(session.id, dir.path()).unwrap().id,
            session.id
        );
    }

    #[test]
    fn rejected_stage_never_opens_or_creates_a_log() {
        let dir = tempfile::tempdir().unwrap();
        let session = StoredSession::new("writer-test-model", "/writer-test");
        let path = dir.path().join(format!("{}.jsonl", session.id));
        assert!(
            !stage_checkpoint(dir.path(), &session)
                .unwrap()
                .publish_unowned(|| false)
                .unwrap()
        );
        assert!(!path.exists());
    }

    struct BlockedIo {
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl SessionIo for BlockedIo {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            Ok(bytes.len())
        }

        fn sync(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn blocked_io_does_not_hold_status_or_admission_locks() {
        smol::block_on(async {
            let (entered, blocked) = flume::unbounded();
            let (release, wait) = flume::unbounded();
            let writer = SessionWriter::with_io(
                MakimaId::generate(),
                BlockedIo {
                    entered,
                    release: wait,
                },
                0,
            );
            let first = writer.append(vec![json!({"value": 1})]);
            blocked.recv_async().await.unwrap();
            assert_eq!(writer.status().unsaved_records, 1);
            let journal =
                SessionJournal::from_session(&StoredSession::new("test", "/test")).unwrap();
            let before = journal.snapshot().revision;
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Plugin {
                            key: "writer-test".into(),
                            version: 1,
                            value: Some(json!(true)),
                        },
                    }],
                    ..JournalCommit::default()
                })
                .unwrap();
            assert!(journal.snapshot().revision > before);
            let second = writer.append(vec![json!({"value": 2})]);
            release.send(()).unwrap();
            first.await.unwrap();
            blocked.recv_async().await.unwrap();
            release.send(()).unwrap();
            second.await.unwrap();
        });
    }

    #[test]
    fn torn_tail_is_removed_without_losing_complete_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        let complete = b"{\"t\":\"test\",\"seq\":7}\n";
        let mut bytes = complete.to_vec();
        bytes.extend_from_slice(b"{\"t\":");
        write(&path, bytes).unwrap();
        let (_, next_seq, history) = open_log(&path).unwrap();
        assert_eq!(next_seq, 8);
        assert_eq!(history, complete);
        assert_eq!(read(&path).unwrap(), complete);
    }

    #[test]
    fn enqueue_order_does_not_depend_on_receipt_poll_order() {
        smol::block_on(async {
            let bytes = Arc::new(Mutex::new(Vec::new()));
            let (synced, _) = flume::unbounded();
            let writer = SessionWriter::with_io(
                MakimaId::generate(),
                FakeIo {
                    bytes: Arc::clone(&bytes),
                    fail_write: false,
                    fail_sync: false,
                    partial: false,
                    synced,
                    retry_gate: None,
                },
                12,
            );
            let first = writer.append(vec![json!({"value": "first"})]);
            let second = writer.append(vec![json!({"value": "second"})]);
            second.await.unwrap();
            first.await.unwrap();
            let bytes = bytes.lock().unwrap();
            let records: Vec<Value> = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice(line).unwrap())
                .collect();
            assert_eq!(records[0]["seq"], 12);
            assert_eq!(records[1]["seq"], 13);
            assert_eq!(records[0]["value"], "first");
        });
    }
}
