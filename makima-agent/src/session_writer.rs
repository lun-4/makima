//! Ordered, retrying durability queue for one session log.

use crate::{ToolOutput, journal::SessionJournal};
use makima_providers::{Message, TokenUsage};
use makima_storage::{
    StorageError,
    id::MakimaId,
    session_lock::SessionPublicationGuard,
    sessions::{Session, SessionError, SessionRecord, append_record, initialize_session_log},
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
    thread,
    time::Duration,
};
use tracing::warn;

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(10);
const CLOSED: &str = "session writer closed";
type WriterSlot = Arc<OnceLock<Result<Arc<WriterHandle>, WriterError>>>;
static WRITERS: OnceLock<Mutex<HashMap<PathBuf, WriterSlot>>> = OnceLock::new();

pub type OpenedSessionIo = (Box<dyn SessionIo>, u64);

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
    Records(Vec<Record>),
}

pub type StoredSession = Session<Message, TokenUsage, ToolOutput>;
pub type Record = SessionRecord<Message, TokenUsage, ToolOutput>;

struct WriterHandle {
    session_id: MakimaId,
    tx: flume::Sender<Request>,
    status: Arc<Mutex<WriterStatus>>,
    journals: Arc<Mutex<Vec<Weak<SessionJournal>>>>,
    publication_guard: Mutex<Option<SessionPublicationGuard>>,
}

#[derive(Clone)]
pub struct SessionWriter(Arc<WriterHandle>);

impl SessionWriter {
    pub fn session_id(&self) -> MakimaId {
        self.0.session_id
    }

    /// Opens a JSONL log once per absolute path.
    pub fn open(path: &Path, session_id: MakimaId) -> Result<Self, WriterError> {
        Self::open_initialized(path, session_id, None)
    }

    pub fn open_session(directory: &Path, session: &StoredSession) -> Result<Self, WriterError> {
        Self::open_initialized(
            &directory.join(format!("{}.jsonl", session.id)),
            session.id,
            Some(session),
        )
    }

    fn open_initialized(
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
                    initialize_session_log(directory, session).map_err(io::Error::other)?;
                }
                let (file, next_seq) = open_log(&path)?;
                let directory = File::open(directory)?;
                Ok((
                    Box::new(SessionFile {
                        file,
                        directory,
                        directory_synced: false,
                    }),
                    next_seq,
                ))
            })
            .0)
        })
        .clone()
        .map(Self)
    }

    pub fn with_io(session_id: MakimaId, io: impl SessionIo, next_seq: u64) -> Self {
        let mut opened = Some((Box::new(io) as Box<dyn SessionIo>, next_seq));
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
        let worker_status = Arc::clone(&status);
        let worker_journals = Arc::clone(&journals);
        thread::spawn(move || {
            smol::block_on(initialize(
                session_id,
                Box::new(factory),
                rx,
                worker_status,
                worker_journals,
            ));
        });
        Self(Arc::new(WriterHandle {
            session_id,
            tx,
            status,
            journals,
            publication_guard: Mutex::new(None),
        }))
    }

    pub fn bind_publication_guard(&self, guard: Option<SessionPublicationGuard>) {
        *self
            .0
            .publication_guard
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = guard;
    }

    /// Enqueues immediately, even when the returned receipt is not polled.
    pub fn append(&self, records: Vec<Record>) -> Receipt {
        self.enqueue(Payload::Records(records), None)
    }

    /// Discards unsaved records and stops this writer. Use only before deletion.
    pub fn close(&self) -> Receipt {
        self.enqueue(Payload::Close, None)
    }

    pub fn append_owned(&self, records: Vec<Record>, guard: SessionPublicationGuard) -> Receipt {
        self.enqueue(Payload::Records(records), Some(guard))
    }

    fn enqueue(&self, payload: Payload, guard: Option<SessionPublicationGuard>) -> Receipt {
        let guard = guard.or_else(|| {
            self.0
                .publication_guard
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
        });
        let (reply, response) = flume::bounded(1);
        let count = match &payload {
            Payload::Records(records) => records.len(),
            Payload::Close => 0,
        };
        let mut status = self
            .0
            .status
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        status.unsaved_records += count;
        let sent = self.0.tx.send(Request {
            payload,
            guard,
            reply,
        });
        if sent.is_err() {
            status.unsaved_records -= count;
        }
        drop(status);
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
        journals.clear();
        journals.push(Arc::downgrade(journal));
        drop(journals);
        journal.set_writer_error(self.status().error.map(|error| error.to_string()));
    }

    pub(crate) fn journals(&self) -> Vec<Arc<SessionJournal>> {
        self.0
            .journals
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .collect()
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

fn open_log(path: &Path) -> io::Result<(File, u64)> {
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
    Ok((file, next_seq))
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
            Ok((io, next_seq)) => {
                drop(factory);
                run(session_id, (io, next_seq), rx, status, journals, pending).await;
                return;
            }
            Err(error) => {
                let error = writer_error(session_id, error);
                status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .error = Some(error.clone());
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
    let (mut io, mut next_seq) = opened;
    let mut bytes = Vec::new();
    let mut written = 0;
    let mut retained = 0;
    let mut needs_sync = false;
    let mut replies = Vec::new();
    let mut publication_guard: Option<SessionPublicationGuard> = None;
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let request = if !needs_sync && pending.is_empty() {
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
                Payload::Records(records) => records,
            };
            let result = records.iter().try_for_each(|record| {
                let mut record = serde_json::to_value(record)?;
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
                append_record(&mut encoded, &record).map_err(io::Error::other)?;
                Ok::<_, io::Error>(())
            });
            if let Err(error) = result {
                status
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .unsaved_records -= records.len();
                let _ = request.reply.send(Err(writer_error(session_id, error)));
                continue;
            }
            next_seq = seq;
            retained += records.len();
            bytes.extend(encoded);
            replies.push(request.reply);
            needs_sync = true;
        }
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
                {
                    let mut status = status.lock().unwrap_or_else(|error| error.into_inner());
                    status.unsaved_records -= retained;
                    status.error = None;
                }
                update_journals(&journals, None);
                for reply in replies.drain(..) {
                    let _ = reply.send(Ok(AppendAck { next_seq }));
                }
                bytes.clear();
                written = 0;
                retained = 0;
                needs_sync = false;
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

pub fn delete(directory: &Path, session_id: MakimaId) -> Result<(), SessionError> {
    forget(directory, session_id)?;
    StoredSession::delete_from(session_id, directory)
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

#[cfg(test)]
mod tests {
    use super::{Record, SessionIo, SessionWriter, StoredSession, WriterStatus, open_log};
    use crate::journal::{JournalChange, JournalCommit, SessionJournal};
    use makima_providers::Message;
    use makima_storage::id::MakimaId;
    use makima_storage::session_state::StateChange;
    use makima_storage::sessions::encode_records;
    use serde_json::{Value, json};
    use std::{
        fs::{read, remove_file, write},
        io,
        sync::{Arc, Mutex},
    };
    use test_case::test_case;

    const DISK_FULL: i32 = libc::ENOSPC;

    fn output(id: &str) -> Record {
        Record::Out {
            id: id.into(),
            d: crate::ToolOutput::Plain(id.into()),
        }
    }

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

    #[test]
    fn synchronous_receipt_wait_does_not_need_the_global_executor() {
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
            0,
        );
        let ack = smol::block_on(writer.append(vec![output("synchronous")])).unwrap();
        assert_eq!(ack.next_seq, 1);
        assert!(!bytes.lock().unwrap().is_empty());
        smol::block_on(writer.close()).unwrap();
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
            let receipt = writer.append(vec![output("first")]);
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
            assert_eq!(records[0]["id"], "first");
        });
    }

    #[test_case(false; "lost_before_initialization")]
    #[test_case(true; "lost_after_initialization")]
    fn bound_publication_guard_fences_plain_appends(initialized: bool) {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let session = StoredSession::new("writer-test-model", "/writer-test");
            let lease = makima_storage::session_lock::claim(dir.path(), &session.id)
                .unwrap()
                .unwrap();
            let writer = SessionWriter::open_session(dir.path(), &session).unwrap();
            writer.bind_publication_guard(Some(lease.publication_guard()));
            if initialized {
                writer.append(Vec::new()).await.unwrap();
            }
            let path = dir.path().join(format!("{}.jsonl", session.id));
            let before = read(&path).ok();
            lease.release().unwrap();
            let error = writer
                .clone()
                .append(vec![output("stale")])
                .await
                .unwrap_err();
            assert!(error.message.contains("session ownership lost"));
            assert_eq!(read(&path).ok(), before);
            super::forget(dir.path(), session.id).unwrap();
        });
    }

    #[test]
    fn barrier_sync_failure_retries_without_another_append() {
        smol::block_on(async {
            let (synced, durable) = flume::unbounded();
            let (release_retry, retry_gate) = flume::bounded(1);
            let writer = SessionWriter::with_io(
                MakimaId::generate(),
                FakeIo {
                    bytes: Arc::new(Mutex::new(Vec::new())),
                    fail_write: false,
                    fail_sync: true,
                    partial: false,
                    synced,
                    retry_gate: Some(retry_gate),
                },
                0,
            );
            assert!(writer.append(Vec::new()).await.is_err());
            assert!(writer.status().error.is_some());
            release_retry.send(()).unwrap();
            durable.recv_async().await.unwrap();
            writer.append(Vec::new()).await.unwrap();
            assert!(writer.status().error.is_none());
        });
    }

    #[test]
    fn successful_initialization_releases_factory_captures() {
        smol::block_on(async {
            let captured = Arc::new(());
            let weak = Arc::downgrade(&captured);
            let (synced, _) = flume::unbounded();
            let writer = SessionWriter::with_factory(MakimaId::generate(), move || {
                let _ = &captured;
                Ok((
                    Box::new(FakeIo {
                        bytes: Arc::new(Mutex::new(Vec::new())),
                        fail_write: false,
                        fail_sync: false,
                        partial: false,
                        synced: synced.clone(),
                        retry_gate: None,
                    }),
                    0,
                ))
            });
            writer.append(Vec::new()).await.unwrap();
            assert!(weak.upgrade().is_none());
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
                ))
            });
            let receipt = writer.append(vec![output("retained")]);
            assert!(receipt.await.is_err());
            assert_eq!(writer.status().unsaved_records, 1);
            assert!(writer.status().error.is_some());
            release_retry.send(()).unwrap();
            durable.recv_async().await.unwrap();
            writer.append(Vec::new()).await.unwrap();
            let bytes = bytes.lock().unwrap();
            let record: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(record["seq"], 17);
            assert_eq!(record["id"], "retained");
            assert!(writer.status().error.is_none());
        });
    }

    #[test]
    fn typed_root_child_compaction_posts_and_draft_reload() {
        use makima_domain::{Post, PostLevel};
        use makima_storage::{
            id::AgentId,
            sessions::{LegacyPlan, SessionMeta, StoredPost},
        };
        use std::collections::{BTreeMap, HashMap};

        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let mut session = StoredSession::new("writer-test-model", "/writer-test");
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            let child = AgentId::generate();
            let writer = SessionWriter::open_session(dir.path(), &session).unwrap();
            writer.append(Vec::new()).await.unwrap();
            let path = dir.path().join(format!("{}.jsonl", session.id));
            let prefix = read(&path).unwrap();
            let original = MakimaId::generate();
            let summary = MakimaId::generate();
            let child_message = MakimaId::generate();
            let mut child_record = session.agents()[&root].clone();
            child_record.id = child;
            child_record.parent = Some(root);
            child_record.depth = 1;
            child_record.transcript_key = "child".into();
            child_record.created_seq = session.next_state_seq();
            child_record.updated_seq = session.next_state_seq();
            let records = vec![
                Record::Agent {
                    seq: session.next_state_seq(),
                    d: child_record,
                },
                Record::Msg {
                    id: original,
                    agent: root,
                    d: Message::user("original".into()),
                },
                Record::Msg {
                    id: child_message,
                    agent: child,
                    d: Message::user("child".into()),
                },
                Record::Compaction {
                    agent: root,
                    messages: vec![Message::user("summary".into())],
                    ids: vec![summary],
                    covered: Some(original),
                },
                Record::Post {
                    d: StoredPost {
                        chat: None,
                        after_message: Some(original),
                        post: Post {
                            text: "post".into(),
                            level: PostLevel::Info,
                            agent: None,
                        },
                    },
                },
                Record::Meta {
                    title: session.title.clone(),
                    parent: None,
                    parent_recorded: true,
                    message_count: 1,
                    token_usage: session.token_usage,
                    updated_at: session.updated_at,
                    sequence_high_water: None,
                    history_epochs: BTreeMap::new(),
                    subagents: Vec::new(),
                    subagent_conversations: HashMap::new(),
                    usage_by_model: HashMap::new(),
                    legacy_config: None,
                    meta: Box::new(SessionMeta {
                        input_draft: Some("draft".into()),
                        queued_messages: vec!["queued".into()],
                        ..SessionMeta::default()
                    }),
                    legacy_plan: LegacyPlan::default(),
                },
                Record::History {
                    agent: root,
                    compacted: true,
                    conversation: Some(makima_storage::conversation::ConversationState::new(
                        "root-conversation".into(),
                    )),
                },
                Record::History {
                    agent: child,
                    compacted: true,
                    conversation: Some(makima_storage::conversation::ConversationState::new(
                        "child-conversation".into(),
                    )),
                },
            ];
            let receipt = writer.append(records);
            receipt.await.unwrap();
            let bytes = read(&path).unwrap();
            assert!(bytes.starts_with(&prefix));
            let loaded = StoredSession::load_from(session.id, dir.path()).unwrap();
            assert_eq!(loaded.last_message_id(), Some(summary));
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.subagent_messages()["child"].len(), 1);
            assert_eq!(loaded.chat_message_ids(Some("child")), vec![child_message]);
            assert_eq!(loaded.posts()[0].after_message, Some(original));
            assert_eq!(loaded.meta.input_draft.as_deref(), Some("draft"));
            assert_eq!(loaded.meta.queued_messages, ["queued"]);
            assert!(loaded.history_snapshot().compacted);
            assert_eq!(
                loaded.history_snapshot().conversation.unwrap().id.as_ref(),
                "root-conversation"
            );
            let child_history = loaded.subagent_history_snapshot("child").unwrap();
            assert!(child_history.compacted);
            assert_eq!(
                child_history.conversation.unwrap().id.as_ref(),
                "child-conversation"
            );
            assert!(
                loaded
                    .context_events()
                    .iter()
                    .any(|event| event["id"] == json!(original))
            );
            let reopened = SessionWriter::open_session(dir.path(), &session).unwrap();
            reopened.append(Vec::new()).await.unwrap();
            assert_eq!(read(&path).unwrap(), bytes);
        });
    }

    #[test]
    fn first_writer_open_archives_and_migrates_v2_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut session = StoredSession::new("writer-test-model", "/writer-test");
        session.save_to(dir.path()).unwrap();
        let path = dir.path().join(format!("{}.jsonl", session.id));
        let bytes = read(&path).unwrap();
        let mut records: Vec<Value> = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        records[0]["v"] = json!(2);
        let legacy = encode_records(&records).unwrap();
        let path = dir.path().join(format!("{}.jsonl", session.id));
        write(&path, &legacy).unwrap();
        smol::block_on(
            SessionWriter::open_session(dir.path(), &session)
                .unwrap()
                .append(Vec::new()),
        )
        .unwrap();
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
        smol::block_on(
            SessionWriter::open_session(dir.path(), &session)
                .unwrap()
                .append(Vec::new()),
        )
        .unwrap();
        assert_eq!(read(path).unwrap(), migrated);
    }

    #[test]
    fn explicitly_forgotten_session_can_be_recreated_at_same_path() {
        let dir = tempfile::tempdir().unwrap();
        let session = StoredSession::new("writer-test-model", "/writer-test");
        smol::block_on(
            SessionWriter::open_session(dir.path(), &session)
                .unwrap()
                .append(Vec::new()),
        )
        .unwrap();
        super::forget(dir.path(), session.id).unwrap();
        let path = dir.path().join(format!("{}.jsonl", session.id));
        remove_file(&path).unwrap();
        smol::block_on(
            SessionWriter::open_session(dir.path(), &session)
                .unwrap()
                .append(Vec::new()),
        )
        .unwrap();
        assert_eq!(
            StoredSession::load_from(session.id, dir.path()).unwrap().id,
            session.id
        );
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
            let first = writer.append(vec![output("first")]);
            assert_eq!(writer.status().unsaved_records, 1);
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
            let second = writer.append(vec![output("second")]);
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
        let (_, next_seq) = open_log(&path).unwrap();
        assert_eq!(next_seq, 8);
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
            let first = writer.append(vec![output("first")]);
            let second = writer.append(vec![output("second")]);
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
            assert_eq!(records[0]["id"], "first");
        });
    }
}
