//! Ordered, retrying durability queue for one session log.

use crate::{ToolOutput, journal::SessionJournal};
use makima_domain::LockExt;
use makima_providers::{Message, TokenUsage};
use makima_storage::{
    StorageError,
    id::MakimaId,
    session_lock::SessionPublicationGuard,
    sessions::{Session, SessionError, SessionRecord, append_record, initialize_session_log},
    sync_directory,
};
use serde_json::Value;
use std::{
    collections::{HashMap, VecDeque},
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

pub(crate) type OpenedSessionIo = (Box<dyn SessionIo>, u64);

pub trait SessionIo: Send + 'static {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize>;
    fn sync(&mut self) -> io::Result<()>;

    /// Returns the end of the log and positions the next write there.
    fn position(&mut self) -> io::Result<u64> {
        Err(io::ErrorKind::Unsupported.into())
    }

    /// Truncates the log and positions the next write at `len`. The caller syncs separately.
    fn truncate(&mut self, _len: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

struct SessionFile {
    file: File,
    unsynced_directory: Option<PathBuf>,
}

impl SessionIo for SessionFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        Write::write(&mut self.file, bytes)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        if let Some(directory) = &self.unsynced_directory {
            sync_directory(directory)?;
            self.unsynced_directory = None;
        }
        Ok(())
    }

    fn position(&mut self) -> io::Result<u64> {
        self.file.seek(SeekFrom::End(0))
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.file.seek(SeekFrom::Start(len))?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("session {session_id} was not saved: {message}")]
pub struct WriterError {
    pub session_id: MakimaId,
    pub message: Arc<str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendAck {
    pub next_seq: u64,
    pub(crate) warning: Option<WriterError>,
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
    Fork(Box<Record>),
    Recovery(Vec<Record>),
}

impl Payload {
    fn count(&self) -> usize {
        match self {
            Self::Close => 0,
            Self::Records(records) | Self::Recovery(records) => records.len(),
            Self::Fork(_) => 1,
        }
    }

    fn is_atomic(&self) -> bool {
        matches!(self, Self::Fork(_) | Self::Recovery(_))
    }
}

#[derive(Debug, thiserror::Error)]
enum AtomicWriteError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("atomic append failed ({write}); rollback failed ({rollback}); writer fenced")]
    Rollback {
        write: io::Error,
        rollback: io::Error,
        batch_visible: bool,
    },
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
            let mut writers = WRITERS.get_or_init(Mutex::default).lock_unpoisoned();
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
                Ok((
                    Box::new(SessionFile {
                        file,
                        unsynced_directory: Some(directory.to_owned()),
                    }),
                    next_seq,
                ))
            })
            .0)
        })
        .clone()
        .map(Self)
    }

    pub(crate) fn with_factory(
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
        *self.0.publication_guard.lock_unpoisoned() = guard;
    }

    /// Enqueues immediately, even when the returned receipt is not polled.
    pub fn append(&self, records: Vec<Record>) -> Receipt {
        self.enqueue(Payload::Records(records), None)
    }

    /// A visible fork whose durability cannot be confirmed returns an acknowledgment with a warning.
    pub(crate) fn append_fork(&self, record: Record) -> Receipt {
        self.enqueue(Payload::Fork(Box::new(record)), None)
    }

    pub(crate) fn append_recovery(
        &self,
        records: Vec<Record>,
        guard: SessionPublicationGuard,
    ) -> Receipt {
        self.enqueue(Payload::Recovery(records), Some(guard))
    }

    /// Discards unsaved records and stops this writer. Use only before deletion.
    pub fn close(&self) -> Receipt {
        self.enqueue(Payload::Close, None)
    }

    pub fn append_owned(&self, records: Vec<Record>, guard: SessionPublicationGuard) -> Receipt {
        self.enqueue(Payload::Records(records), Some(guard))
    }

    fn enqueue(&self, payload: Payload, guard: Option<SessionPublicationGuard>) -> Receipt {
        let guard = guard.or_else(|| self.0.publication_guard.lock_unpoisoned().clone());
        let (reply, response) = flume::bounded(1);
        let count = payload.count();
        let mut status = self.0.status.lock_unpoisoned();
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
        let mut journals = self.0.journals.lock_unpoisoned();
        journals.clear();
        journals.push(Arc::downgrade(journal));
        drop(journals);
        journal.set_writer_error(self.status().error.map(|error| error.to_string()));
    }

    pub(crate) fn journals(&self) -> Vec<Arc<SessionJournal>> {
        self.0
            .journals
            .lock_unpoisoned()
            .iter()
            .filter_map(Weak::upgrade)
            .collect()
    }

    pub fn status(&self) -> WriterStatus {
        self.0.status.lock_unpoisoned().clone()
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
    let mut pending = Vec::new();
    let mut backoff = INITIAL_BACKOFF;
    let mut ordinary_error: Option<WriterError> = None;
    loop {
        if pending.is_empty() {
            let Ok(request) = rx.recv_async().await else {
                return;
            };
            pending.push(request);
            backoff = INITIAL_BACKOFF;
        }
        pending.extend(rx.try_iter());
        if let Some(index) = pending
            .iter()
            .position(|request| matches!(request.payload, Payload::Close))
        {
            let request = pending.swap_remove(index);
            drop(rx);
            status.lock_unpoisoned().unsaved_records = 0;
            let _ = request.reply.send(Ok(AppendAck {
                next_seq: 0,
                warning: None,
            }));
            return;
        }
        if let Some(error) = &ordinary_error {
            pending.retain(|request| {
                if request.payload.is_atomic() {
                    status.lock_unpoisoned().unsaved_records -= request.payload.count();
                    let _ = request.reply.send(Err(error.clone()));
                    return false;
                }
                true
            });
        }
        let guard = pending.iter().find_map(|request| request.guard.clone());
        let (returned_factory, opened) = smol::unblock(move || {
            let result = publish(guard.as_ref(), &mut factory);
            (factory, result)
        })
        .await;
        factory = returned_factory;
        match opened {
            Ok((io, next_seq)) => {
                drop(factory);
                run(
                    session_id,
                    (io, next_seq),
                    rx,
                    status,
                    journals,
                    pending.into(),
                )
                .await;
                return;
            }
            Err(error) => {
                let error = writer_error(session_id, error);
                status.lock_unpoisoned().error = Some(error.clone());
                update_journals(&journals, Some(error.to_string()));
                pending.retain_mut(|request| {
                    let atomic = request.payload.is_atomic();
                    if atomic {
                        status.lock_unpoisoned().unsaved_records -= request.payload.count();
                    }
                    let _ = request.reply.try_send(Err(error.clone()));
                    if !atomic {
                        let (reply, receiver) = flume::bounded(1);
                        drop(receiver);
                        request.reply = reply;
                    }
                    !atomic
                });
                if !pending.is_empty() {
                    warn!(%session_id, %error, "session initialization failed; retrying retained records");
                    ordinary_error = Some(error);
                    smol::Timer::after(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
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
    mut pending: VecDeque<Request>,
) {
    let (mut io, mut next_seq) = opened;
    let mut bytes = Vec::new();
    let mut written = 0;
    let mut retained = 0;
    let mut needs_sync = false;
    let mut replies = Vec::new();
    let mut publication_guard = None;
    let mut backoff = INITIAL_BACKOFF;
    let mut ordinary_error: Option<WriterError> = None;
    loop {
        if !needs_sync && pending.is_empty() {
            match rx.recv_async().await {
                Ok(request) => pending.push_back(request),
                Err(_) => return,
            }
        }
        while let Some(request) = pending.pop_front().or_else(|| rx.try_recv().ok()) {
            let Request {
                payload,
                guard,
                reply,
            } = request;
            match payload {
                Payload::Close => {
                    drop(io);
                    drop(rx);
                    status.lock_unpoisoned().unsaved_records = 0;
                    let _ = reply.send(Ok(AppendAck {
                        next_seq,
                        warning: None,
                    }));
                    return;
                }
                payload @ (Payload::Fork(_) | Payload::Recovery(_)) => {
                    let count = payload.count();
                    if let Some(error) = &ordinary_error {
                        status.lock_unpoisoned().unsaved_records -= count;
                        let _ = reply.send(Err(error.clone()));
                        continue;
                    }
                    if needs_sync {
                        pending.push_front(Request {
                            payload,
                            guard,
                            reply,
                        });
                        break;
                    }
                    let mut candidate_seq = next_seq;
                    let mut encoded = Vec::new();
                    let fork = matches!(payload, Payload::Fork(_));
                    let records = match &payload {
                        Payload::Fork(record) => std::slice::from_ref(record.as_ref()),
                        Payload::Recovery(records) => records.as_slice(),
                        _ => unreachable!(),
                    };
                    let encoded_result =
                        records.iter().try_for_each(|record| match (fork, record) {
                            (false, Record::Fork { .. }) => {
                                Err(io::Error::other("fork records require append_fork"))
                            }
                            (true, Record::Fork { .. }) | (false, _) => {
                                encode_record(record, &mut candidate_seq, &mut encoded)
                            }
                            (true, _) => {
                                Err(io::Error::other("append_fork requires a fork record"))
                            }
                        });
                    if let Err(error) = encoded_result {
                        status.lock_unpoisoned().unsaved_records -= count;
                        let _ = reply.send(Err(writer_error(session_id, error)));
                        continue;
                    }
                    let (returned_io, result) = smol::unblock(move || {
                        let result = publish(guard.as_ref(), || write_atomic(&mut *io, &encoded));
                        (io, result)
                    })
                    .await;
                    io = returned_io;
                    let fenced = matches!(&result, Err(AtomicWriteError::Rollback { .. }));
                    let visible_fork = fork
                        && matches!(
                            &result,
                            Err(AtomicWriteError::Rollback {
                                batch_visible: true,
                                ..
                            })
                        );
                    let error = result.err().map(|error| writer_error(session_id, error));
                    let receipt = if let Some(error) = &error
                        && !visible_fork
                    {
                        Err(error.clone())
                    } else {
                        next_seq = candidate_seq;
                        Ok(AppendAck {
                            next_seq,
                            warning: error.clone(),
                        })
                    };
                    {
                        let mut status = status.lock_unpoisoned();
                        status.unsaved_records -= count;
                        status.error = error.clone();
                    }
                    update_journals(&journals, error.as_ref().map(ToString::to_string));
                    let _ = reply.send(receipt);
                    if fenced && let Some(error) = error {
                        drop(io);
                        // The durable tail is unknown. Keep this writer fenced, including against close/reopen.
                        warn!(%session_id, %error, "session writer fenced");
                        loop {
                            let request = match pending.pop_front() {
                                Some(request) => request,
                                None => match rx.recv_async().await {
                                    Ok(request) => request,
                                    Err(_) => return,
                                },
                            };
                            status.lock_unpoisoned().unsaved_records -= request.payload.count();
                            let _ = request.reply.send(Err(error.clone()));
                        }
                    }
                }
                Payload::Records(records) => {
                    if guard.is_some() {
                        publication_guard = guard;
                    }
                    let mut encoded = Vec::new();
                    let mut seq = next_seq;
                    let result = records.iter().try_for_each(|record| {
                        if matches!(record, Record::Fork { .. }) {
                            return Err(io::Error::other("fork records require append_fork"));
                        }
                        encode_record(record, &mut seq, &mut encoded)
                    });
                    if let Err(error) = result {
                        status.lock_unpoisoned().unsaved_records -= records.len();
                        let _ = reply.send(Err(writer_error(session_id, error)));
                        continue;
                    }
                    next_seq = seq;
                    retained += records.len();
                    bytes.extend(encoded);
                    replies.push(reply);
                    needs_sync = true;
                }
            }
        }
        if !needs_sync {
            continue;
        }
        let result = smol::unblock(move || {
            let result = publish(publication_guard.as_ref(), || {
                write_and_sync(&mut *io, &bytes, &mut written)
            });
            (io, bytes, written, publication_guard, result)
        })
        .await;
        (io, bytes, written, publication_guard) = (result.0, result.1, result.2, result.3);
        match result.4 {
            Ok(()) => {
                {
                    let mut status = status.lock_unpoisoned();
                    status.unsaved_records -= retained;
                    status.error = None;
                }
                update_journals(&journals, None);
                for reply in replies.drain(..) {
                    let _ = reply.send(Ok(AppendAck {
                        next_seq,
                        warning: None,
                    }));
                }
                bytes.clear();
                written = 0;
                retained = 0;
                needs_sync = false;
                publication_guard = None;
                ordinary_error = None;
                backoff = INITIAL_BACKOFF;
            }
            Err(error) => {
                let error = writer_error(session_id, error);
                status.lock_unpoisoned().error = Some(error.clone());
                update_journals(&journals, Some(error.to_string()));
                warn!(%session_id, %error, retained, "session has unsaved records; retrying");
                for reply in replies.drain(..) {
                    let _ = reply.send(Err(error.clone()));
                }
                ordinary_error = Some(error);
                smol::Timer::after(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

fn encode_record(record: &Record, seq: &mut u64, encoded: &mut Vec<u8>) -> io::Result<()> {
    let mut record = serde_json::to_value(record)?;
    let object = record
        .as_object_mut()
        .ok_or_else(|| io::Error::other("session records must be JSON objects"))?;
    if object.get("t").and_then(Value::as_str) == Some("agent")
        && let Some(journal_seq) = object.remove("seq")
    {
        object.insert("journal_seq".into(), journal_seq);
    }
    object.insert("seq".into(), Value::from(*seq));
    *seq = seq
        .checked_add(1)
        .ok_or_else(|| io::Error::other("session sequence exhausted"))?;
    append_record(encoded, &record).map_err(io::Error::other)
}

fn publish<T, E: From<io::Error>>(
    guard: Option<&SessionPublicationGuard>,
    write: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    match guard {
        Some(guard) => guard
            .publish(write)?
            .unwrap_or_else(|| Err(io::Error::other("session ownership lost").into())),
        None => write(),
    }
}

fn write_and_sync(io: &mut dyn SessionIo, bytes: &[u8], written: &mut usize) -> io::Result<()> {
    while *written < bytes.len() {
        match io.write(&bytes[*written..]) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(count) => *written += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    io.sync()
}

fn write_atomic(io: &mut dyn SessionIo, bytes: &[u8]) -> Result<(), AtomicWriteError> {
    let position = io.position()?;
    let mut written = 0;
    if let Err(write) = write_and_sync(io, bytes, &mut written) {
        let (rollback, batch_visible) = match io.truncate(position) {
            Ok(()) => (io.sync(), false),
            Err(error) => {
                let batch_visible = io.position().map_or(written == bytes.len(), |end| {
                    end.saturating_sub(position) >= bytes.len() as u64
                });
                (Err(error), batch_visible)
            }
        };
        return match rollback {
            Ok(()) => Err(write.into()),
            Err(rollback) => Err(AtomicWriteError::Rollback {
                write,
                rollback,
                batch_visible,
            }),
        };
    }
    Ok(())
}

fn update_journals(journals: &Mutex<Vec<Weak<SessionJournal>>>, error: Option<String>) {
    let attached = {
        let mut journals = journals.lock_unpoisoned();
        let mut attached = Vec::new();
        journals.retain(|journal| {
            if let Some(journal) = journal.upgrade() {
                attached.push(journal);
                true
            } else {
                false
            }
        });
        attached
    };
    for journal in attached {
        journal.set_writer_error(error.clone());
    }
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
        .lock_unpoisoned()
        .get(&path)
        .cloned();
    if let Some(slot) = slot {
        if let Some(Ok(handle)) = slot.get() {
            smol::block_on(SessionWriter(Arc::clone(handle)).enqueue(Payload::Close, None))
                .map_err(|error| StorageError::from(io::Error::other(error)))?;
        }
        let mut writers = WRITERS.get_or_init(Mutex::default).lock_unpoisoned();
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
    use super::{Record, SessionIo, SessionWriter, StoredSession, WriterStatus, forget, open_log};
    use crate::journal::{JournalChange, JournalCommit, SessionJournal};
    use makima_providers::Message;
    use makima_storage::conversation::ConversationState;
    use makima_storage::id::{AgentId, MakimaId};
    use makima_storage::session_lock;
    use makima_storage::session_state::StateChange;
    use makima_storage::sessions::encode_records;
    use serde_json::{Value, json};
    use std::{
        fs::{create_dir, read, remove_file, write},
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

        fn position(&mut self) -> io::Result<u64> {
            Ok(self.bytes.lock().unwrap().len() as u64)
        }

        fn truncate(&mut self, len: u64) -> io::Result<()> {
            self.bytes.lock().unwrap().truncate(len as usize);
            Ok(())
        }
    }

    #[test_case(false, false; "fork_partial_write")]
    #[test_case(true, false; "fork_fsync")]
    #[test_case(false, true; "recovery_partial_write")]
    #[test_case(true, true; "recovery_fsync")]
    fn failed_atomic_append_rolls_back_without_retrying(fail_sync: bool, recovery: bool) {
        smol::block_on(async {
            const PREFIX: &[u8] = b"{\"t\":\"out\",\"id\":\"prior\",\"seq\":7,\"d\":\"prior\"}\n";
            const NEXT_SEQ: u64 = 8;
            const FOLLOWING: &str = "following";
            let bytes = Arc::new(Mutex::new(PREFIX.to_vec()));
            let (synced, durable) = flume::unbounded();
            let session_id = MakimaId::generate();
            let writer = SessionWriter::with_io(
                session_id,
                FakeIo {
                    bytes: Arc::clone(&bytes),
                    fail_write: !fail_sync,
                    fail_sync,
                    partial: !fail_sync,
                    synced,
                    retry_gate: None,
                },
                NEXT_SEQ,
            );
            let dir = tempfile::tempdir().unwrap();
            let lease = session_lock::claim(dir.path(), &session_id)
                .unwrap()
                .unwrap();
            let receipt = if recovery {
                writer.append_recovery(
                    vec![output("first"), output("second")],
                    lease.publication_guard(),
                )
            } else {
                writer.append_fork(Record::Fork {
                    agent: AgentId::generate(),
                    target: None,
                    note_id: MakimaId::generate(),
                    note: "rewind".into(),
                    conversation: ConversationState::fresh(None),
                    input_draft: None,
                    cleared_turns: Vec::new(),
                })
            };
            let error = receipt.await.unwrap_err();
            assert_eq!(
                error.message.as_ref(),
                io::Error::from_raw_os_error(DISK_FULL).to_string()
            );
            assert_eq!(bytes.lock().unwrap().as_slice(), PREFIX);
            assert!(durable.try_recv().is_ok());
            assert_eq!(writer.status().unsaved_records, 0);

            let ack = writer.append(vec![output(FOLLOWING)]).await.unwrap();
            assert_eq!(ack.next_seq, NEXT_SEQ + 1);
            let bytes = bytes.lock().unwrap();
            assert_eq!(&bytes[..PREFIX.len()], PREFIX);
            let record: Value = serde_json::from_slice(&bytes[PREFIX.len()..]).unwrap();
            assert_eq!(record["id"], FOLLOWING);
            assert_eq!(record["seq"], NEXT_SEQ);
            assert!(writer.status().error.is_none());
        });
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
                        agent: None,
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
                    usage_by_model: HashMap::new(),
                    legacy_config: None,
                    meta: Box::new(SessionMeta {
                        input_draft: Some("draft".into()),
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
        const AFTER_MIGRATION: &str = "after-migration";
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
        write(&path, &legacy).unwrap();
        let lease = session_lock::claim(dir.path(), &session.id)
            .unwrap()
            .unwrap();
        let writer = SessionWriter::open_session(dir.path(), &session).unwrap();
        writer.bind_publication_guard(Some(lease.publication_guard()));
        smol::block_on(writer.append(Vec::new())).unwrap();
        let archive = dir
            .path()
            .join("archive")
            .join(session.id.to_string())
            .join("v2.jsonl");
        assert_eq!(read(&archive).unwrap(), legacy);
        let migrated = read(&path).unwrap();
        let header: Value =
            serde_json::from_slice(migrated.split(|byte| *byte == b'\n').next().unwrap()).unwrap();
        assert_eq!(header["v"], 3);
        forget(dir.path(), session.id).unwrap();
        lease.release().unwrap();

        let lease = session_lock::claim(dir.path(), &session.id)
            .unwrap()
            .unwrap();
        let loaded = StoredSession::load_from(session.id, dir.path()).unwrap();
        let writer = SessionWriter::open_session(dir.path(), &loaded).unwrap();
        writer.bind_publication_guard(Some(lease.publication_guard()));
        smol::block_on(writer.append(vec![output(AFTER_MIGRATION)])).unwrap();
        assert!(read(&path).unwrap().starts_with(&migrated));
        assert_eq!(read(&archive).unwrap(), legacy);
        let loaded = StoredSession::load_from(session.id, dir.path()).unwrap();
        assert!(loaded.tool_outputs().contains_key(AFTER_MIGRATION));
        forget(dir.path(), session.id).unwrap();
    }

    #[test_case(false; "new_sessions_directory")]
    #[test_case(true; "existing_sessions_directory")]
    fn appended_records_survive_close_and_reopen(directory_exists: bool) {
        const FIRST: &str = "first";
        const SECOND: &str = "second";
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("sessions");
        if directory_exists {
            create_dir(&directory).unwrap();
        }
        let session = StoredSession::new("writer-test-model", "/writer-test");
        let writer = SessionWriter::open_session(&directory, &session).unwrap();
        let first_ack = smol::block_on(writer.append(vec![output(FIRST)])).unwrap();
        forget(&directory, session.id).unwrap();

        let reopened = StoredSession::load_from(session.id, &directory).unwrap();
        assert!(reopened.tool_outputs().contains_key(FIRST));
        let writer = SessionWriter::open_session(&directory, &reopened).unwrap();
        let second_ack = smol::block_on(writer.append(vec![output(SECOND)])).unwrap();
        assert_eq!(second_ack.next_seq, first_ack.next_seq + 1);
        forget(&directory, session.id).unwrap();

        let reopened = StoredSession::load_from(session.id, &directory).unwrap();
        assert_eq!(reopened.tool_outputs().len(), 2);
        assert!(reopened.tool_outputs().contains_key(FIRST));
        assert!(reopened.tool_outputs().contains_key(SECOND));
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
