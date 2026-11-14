use std::{io, path::PathBuf, string::FromUtf8Error};

use makima_domain::TokenUsage as StoredTokenUsage;
use makima_providers::TokenUsage;

use makima_storage::{
    id::MakimaId,
    session_state::{
        MAX_INLINE_TURN_RESULT_BYTES, StoredCancellationReason, StoredDoneReason, StoredOffloadRef,
        StoredPayload, StoredTurnFailure, StoredTurnFailureKind, StoredTurnOrigin,
        StoredTurnOutcome, StoredTurnProvenance, StoredTurnResult, StoredTurnStatus,
    },
};
use serde_json::Value;
use thiserror::Error;

use super::{TurnOrigin, TurnProvenance, TurnResult};
use crate::{
    tools::offload::{OffloadError, OffloadStore},
    types::{DoneReason, TurnCancellationReason, TurnFailure, TurnFailureKind, TurnOutcome},
};

#[derive(Debug, Error)]
pub enum TurnResultStorageError {
    #[error("a session offload store is required for oversized turn results")]
    MissingOffloadStore,
    #[error(transparent)]
    Offload(#[from] OffloadError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("result_unavailable: {0}")]
    Unavailable(#[source] io::Error),
    #[error("result_unavailable: invalid offload artifact reference")]
    InvalidReference,
    #[error("result_unavailable: expected {expected} artifact bytes, found {actual}")]
    LengthMismatch { expected: u64, actual: u64 },
    #[error("result_unavailable: {0}")]
    Utf8(#[from] FromUtf8Error),
}

#[derive(Debug, Clone, Default)]
pub struct StoredTurnProgress {
    text: Option<StoredPayload<String>>,
    output: Option<StoredPayload<Vec<Value>>>,
}

impl TurnResult {
    pub fn to_stored(
        &self,
        session: MakimaId,
        store: Option<&OffloadStore>,
    ) -> Result<StoredTurnResult, TurnResultStorageError> {
        self.to_stored_with_progress(&mut StoredTurnProgress::default(), session, store)
    }

    pub fn to_stored_with_progress(
        &self,
        progress: &mut StoredTurnProgress,
        session: MakimaId,
        store: Option<&OffloadStore>,
    ) -> Result<StoredTurnResult, TurnResultStorageError> {
        let text = match &progress.text {
            Some(text) => text.clone(),
            None => {
                let text = match offload(&self.text, session, store)? {
                    Some(reference) => StoredPayload::Offloaded(reference),
                    None => StoredPayload::Inline(self.text.clone()),
                };
                progress.text = Some(text.clone());
                text
            }
        };
        let output = match &progress.output {
            Some(output) => output.clone(),
            None => {
                let encoded = serde_json::to_string(&self.output)?;
                let output = match offload(&encoded, session, store)? {
                    Some(reference) => StoredPayload::Offloaded(reference),
                    None => StoredPayload::Inline(self.output.clone()),
                };
                progress.output = Some(output.clone());
                output
            }
        };
        Ok(StoredTurnResult {
            outcome: self.outcome.clone().into(),
            text,
            output,
            provenance: self.provenance.clone().into(),
            duration_ms: self.duration_ms,
        })
    }

    pub fn from_stored(
        stored: StoredTurnResult,
        mut resolve: impl FnMut(&StoredOffloadRef) -> io::Result<Vec<u8>>,
    ) -> Result<Self, TurnResultStorageError> {
        let text = match stored.text {
            StoredPayload::Inline(text) => text,
            StoredPayload::Offloaded(reference) => {
                String::from_utf8(resolve_payload(&reference, &mut resolve)?)?
            }
        };
        let output = match stored.output {
            StoredPayload::Inline(output) => output,
            StoredPayload::Offloaded(reference) => {
                serde_json::from_slice(&resolve_payload(&reference, &mut resolve)?)?
            }
        };
        Ok(Self {
            outcome: stored.outcome.into(),
            text,
            output,
            provenance: stored.provenance.into(),
            duration_ms: stored.duration_ms,
        })
    }
}

fn offload(
    body: &str,
    session: MakimaId,
    store: Option<&OffloadStore>,
) -> Result<Option<StoredOffloadRef>, TurnResultStorageError> {
    if body.len() <= MAX_INLINE_TURN_RESULT_BYTES {
        return Ok(None);
    }
    let saved = store
        .ok_or(TurnResultStorageError::MissingOffloadStore)?
        .put_exact(body)?;
    Ok(Some(StoredOffloadRef {
        session,
        path: PathBuf::from(saved.name),
        bytes: saved.saved_bytes as u64,
    }))
}

fn resolve_payload(
    reference: &StoredOffloadRef,
    resolve: &mut impl FnMut(&StoredOffloadRef) -> io::Result<Vec<u8>>,
) -> Result<Vec<u8>, TurnResultStorageError> {
    let Some(name) = reference.path.to_str() else {
        return Err(TurnResultStorageError::InvalidReference);
    };
    if name.is_empty() || matches!(name, "." | "..") || name.contains(['/', '\\', ':', '\0']) {
        return Err(TurnResultStorageError::InvalidReference);
    }
    let bytes = resolve(reference).map_err(TurnResultStorageError::Unavailable)?;
    let actual = bytes.len() as u64;
    if actual != reference.bytes {
        return Err(TurnResultStorageError::LengthMismatch {
            expected: reference.bytes,
            actual,
        });
    }
    Ok(bytes)
}

impl From<TurnOrigin> for StoredTurnOrigin {
    fn from(origin: TurnOrigin) -> Self {
        match origin {
            TurnOrigin::Internal => Self::Internal,
            TurnOrigin::User => Self::User,
            TurnOrigin::Plugin => Self::Plugin,
        }
    }
}

impl From<StoredTurnOrigin> for TurnOrigin {
    fn from(origin: StoredTurnOrigin) -> Self {
        match origin {
            StoredTurnOrigin::Internal => Self::Internal,
            StoredTurnOrigin::User => Self::User,
            StoredTurnOrigin::Plugin => Self::Plugin,
        }
    }
}

impl From<TurnProvenance> for StoredTurnProvenance {
    fn from(provenance: TurnProvenance) -> Self {
        Self {
            origin: provenance.origin.into(),
            plugin: provenance.plugin,
            plugin_generation: provenance.plugin_generation,
            source_agent: provenance.source_agent,
            source_turn: provenance.source_turn,
        }
    }
}

impl From<StoredTurnProvenance> for TurnProvenance {
    fn from(provenance: StoredTurnProvenance) -> Self {
        Self {
            origin: provenance.origin.into(),
            plugin: provenance.plugin,
            plugin_generation: provenance.plugin_generation,
            source_agent: provenance.source_agent,
            source_turn: provenance.source_turn,
        }
    }
}

impl From<DoneReason> for StoredDoneReason {
    fn from(reason: DoneReason) -> Self {
        match reason {
            DoneReason::EndTurn => Self::EndTurn,
            DoneReason::MaxTokens => Self::MaxTokens,
            DoneReason::MaxTurns => Self::MaxTurns,
            DoneReason::Compact => Self::Compact,
        }
    }
}

impl From<StoredDoneReason> for DoneReason {
    fn from(reason: StoredDoneReason) -> Self {
        match reason {
            StoredDoneReason::EndTurn => Self::EndTurn,
            StoredDoneReason::MaxTokens => Self::MaxTokens,
            StoredDoneReason::MaxTurns => Self::MaxTurns,
            StoredDoneReason::Compact => Self::Compact,
        }
    }
}

impl From<TurnCancellationReason> for StoredCancellationReason {
    fn from(reason: TurnCancellationReason) -> Self {
        match reason {
            TurnCancellationReason::User => Self::User,
            TurnCancellationReason::Closed => Self::Closed,
            TurnCancellationReason::Shutdown => Self::Shutdown,
            TurnCancellationReason::Interrupted => Self::Interrupted,
            TurnCancellationReason::Withdrawn => Self::Withdrawn,
        }
    }
}

impl From<StoredCancellationReason> for TurnCancellationReason {
    fn from(reason: StoredCancellationReason) -> Self {
        match reason {
            StoredCancellationReason::User => Self::User,
            StoredCancellationReason::Closed => Self::Closed,
            StoredCancellationReason::Shutdown => Self::Shutdown,
            StoredCancellationReason::Interrupted => Self::Interrupted,
            StoredCancellationReason::Withdrawn => Self::Withdrawn,
        }
    }
}

impl From<TurnFailureKind> for StoredTurnFailureKind {
    fn from(kind: TurnFailureKind) -> Self {
        match kind {
            TurnFailureKind::Provider => Self::Provider,
            TurnFailureKind::Authentication => Self::Authentication,
            TurnFailureKind::Timeout => Self::Timeout,
            TurnFailureKind::Tool => Self::Tool,
            TurnFailureKind::Transport => Self::Transport,
            TurnFailureKind::InvalidResponse => Self::InvalidResponse,
            TurnFailureKind::Internal => Self::Internal,
            TurnFailureKind::Compaction => Self::Compaction,
        }
    }
}

impl From<StoredTurnFailureKind> for TurnFailureKind {
    fn from(kind: StoredTurnFailureKind) -> Self {
        match kind {
            StoredTurnFailureKind::Provider => Self::Provider,
            StoredTurnFailureKind::Authentication => Self::Authentication,
            StoredTurnFailureKind::Timeout => Self::Timeout,
            StoredTurnFailureKind::Tool => Self::Tool,
            StoredTurnFailureKind::Transport => Self::Transport,
            StoredTurnFailureKind::InvalidResponse => Self::InvalidResponse,
            StoredTurnFailureKind::Internal => Self::Internal,
            StoredTurnFailureKind::Compaction => Self::Compaction,
        }
    }
}

impl From<TurnFailure> for StoredTurnFailure {
    fn from(failure: TurnFailure) -> Self {
        Self {
            kind: failure.kind.into(),
            diagnostic: failure.diagnostic,
            user_message: failure.user_message,
            retryable: failure.retryable,
        }
    }
}

impl From<StoredTurnFailure> for TurnFailure {
    fn from(failure: StoredTurnFailure) -> Self {
        Self {
            kind: failure.kind.into(),
            diagnostic: failure.diagnostic,
            user_message: failure.user_message,
            retryable: failure.retryable,
        }
    }
}

impl From<TurnOutcome> for StoredTurnOutcome {
    fn from(outcome: TurnOutcome) -> Self {
        let usage = outcome.usage();
        Self {
            agent_id: outcome.agent_id(),
            turn_id: outcome.turn_id(),
            usage: StoredTokenUsage {
                input: usage.input,
                output: usage.output,
                cache_creation: usage.cache_creation,
                cache_read: usage.cache_read,
                cost: None,
            },
            num_turns: outcome.num_turns(),
            cost: outcome.cost(),
            list_cost: outcome.list_cost(),
            context_size: outcome.context_size(),
            context_window: outcome.context_window(),
            status: match outcome {
                TurnOutcome::Completed { reason, .. } => StoredTurnStatus::Completed {
                    reason: reason.into(),
                },
                TurnOutcome::Failed { failure, .. } => StoredTurnStatus::Failed {
                    failure: failure.into(),
                },
                TurnOutcome::Cancelled { reason, .. } => StoredTurnStatus::Cancelled {
                    reason: reason.into(),
                },
            },
        }
    }
}

impl From<StoredTurnOutcome> for TurnOutcome {
    fn from(outcome: StoredTurnOutcome) -> Self {
        let StoredTurnOutcome {
            agent_id,
            turn_id,
            usage,
            num_turns,
            cost,
            list_cost,
            context_size,
            context_window,
            status,
        } = outcome;
        let usage = TokenUsage {
            input: usage.input,
            output: usage.output,
            cache_creation: usage.cache_creation,
            cache_read: usage.cache_read,
        };
        match status {
            StoredTurnStatus::Completed { reason } => Self::Completed {
                agent_id,
                turn_id,
                usage,
                num_turns,
                cost,
                list_cost,
                context_size,
                context_window,
                reason: reason.into(),
            },
            StoredTurnStatus::Failed { failure } => Self::Failed {
                agent_id,
                turn_id,
                usage,
                num_turns,
                cost,
                list_cost,
                context_size,
                context_window,
                failure: failure.into(),
            },
            StoredTurnStatus::Cancelled { reason } => Self::Cancelled {
                agent_id,
                turn_id,
                usage,
                num_turns,
                cost,
                list_cost,
                context_size,
                context_window,
                reason: reason.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs, io,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use makima_providers::TokenUsage;
    use makima_storage::{
        id::{AgentId, MakimaId, TurnId},
        session_state::{
            MAX_INLINE_TURN_RESULT_BYTES, StoredOffloadRef, StoredPayload, StoredTurnOutcome,
            StoredTurnStatus,
        },
    };
    use serde_json::json;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::{StoredTurnProgress, TurnResultStorageError};
    use crate::{
        TurnOrigin, TurnProvenance, TurnResult,
        tools::offload::{MAX_OFFLOAD_FILE_BYTES, OffloadBackend, OffloadError, OffloadStore},
        types::{DoneReason, TurnCancellationReason, TurnFailure, TurnFailureKind, TurnOutcome},
    };

    const DIAGNOSTIC: &str = "provider diagnostic";
    const USER_MESSAGE: &str = "please retry";
    const PLUGIN: &str = "task";
    const MISSING_ARTIFACT: &str = "artifact missing";
    const ARTIFACT: &str = "result.txt";
    const SECOND_WRITE_ERROR: &str = "second artifact write failed";

    #[derive(Default)]
    struct RetryBackend {
        files: Mutex<BTreeMap<String, Vec<u8>>>,
        writes: AtomicUsize,
    }

    impl OffloadBackend for Arc<RetryBackend> {
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            let attempt = self.writes.fetch_add(1, Ordering::Relaxed);
            if attempt == 1 {
                return Err(io::Error::other(SECOND_WRITE_ERROR));
            }
            let mut files = self.files.lock().unwrap();
            if files.contains_key(name) {
                return Ok(false);
            }
            files.insert(name.into(), bytes.into());
            Ok(true)
        }

        fn total_bytes(&self) -> io::Result<u64> {
            Ok(self
                .files
                .lock()
                .unwrap()
                .values()
                .map(|bytes| bytes.len() as u64)
                .sum())
        }

        fn remove_all(&self) -> io::Result<()> {
            self.files.lock().unwrap().clear();
            Ok(())
        }

        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from(name)
        }
    }

    #[test]
    fn partial_offload_retry_reuses_successful_payload() {
        let backend = Arc::new(RetryBackend::default());
        let store = OffloadStore::new(Box::new(Arc::clone(&backend)));
        let session = MakimaId::generate();
        let mut expected = result(outcome());
        expected.text = "t".repeat(MAX_INLINE_TURN_RESULT_BYTES + 1);
        expected.output = vec![json!("o".repeat(MAX_INLINE_TURN_RESULT_BYTES + 1))];
        let mut progress = StoredTurnProgress::default();
        let failed = expected.to_stored_with_progress(&mut progress, session, Some(&store));
        assert!(
            matches!(failed, Err(TurnResultStorageError::Offload(OffloadError::Io(error))) if error.to_string() == SECOND_WRITE_ERROR)
        );
        assert_eq!(backend.files.lock().unwrap().len(), 1);
        let StoredPayload::Offloaded(first_reference) = progress.text.clone().unwrap() else {
            panic!("oversized text was not offloaded");
        };
        assert!(progress.output.is_none());
        let stored = expected
            .to_stored_with_progress(&mut progress, session, Some(&store))
            .unwrap();
        assert_eq!(stored.text, StoredPayload::Offloaded(first_reference));
        assert_eq!(backend.files.lock().unwrap().len(), 2);
        assert_eq!(backend.writes.load(Ordering::Relaxed), 3);
        assert_eq!(
            expected
                .to_stored_with_progress(&mut progress, session, Some(&store))
                .unwrap(),
            stored
        );
        assert_eq!(backend.writes.load(Ordering::Relaxed), 3);
        let restored = TurnResult::from_stored(stored, |reference| {
            Ok(backend.files.lock().unwrap()[reference.path.to_str().unwrap()].clone())
        })
        .unwrap();
        assert_eq!(restored, expected);
    }

    fn result(outcome: TurnOutcome) -> TurnResult {
        TurnResult {
            outcome,
            text: USER_MESSAGE.into(),
            output: vec![json!({ "answer": [true, null, 1] })],
            provenance: TurnProvenance {
                origin: TurnOrigin::Plugin,
                plugin: Some(PLUGIN.into()),
                plugin_generation: Some(7),
                source_agent: Some(AgentId::generate()),
                source_turn: Some(TurnId::generate()),
            },
            duration_ms: 1234,
        }
    }

    fn outcome() -> TurnOutcome {
        TurnOutcome::completed(
            AgentId::generate(),
            TurnId::generate(),
            TokenUsage {
                input: 13,
                output: 17,
                cache_creation: 19,
                cache_read: 23,
            },
            3,
            DoneReason::EndTurn,
        )
    }

    #[test_case(DoneReason::EndTurn)]
    #[test_case(DoneReason::MaxTokens)]
    #[test_case(DoneReason::MaxTurns)]
    #[test_case(DoneReason::Compact)]
    fn completed_outcome_round_trips(reason: DoneReason) {
        let mut outcome = outcome();
        if let TurnOutcome::Completed {
            reason: saved_reason,
            cost,
            list_cost,
            context_size,
            context_window,
            ..
        } = &mut outcome
        {
            *saved_reason = reason;
            *cost = Some(0.125);
            *list_cost = Some(0.25);
            *context_size = 127;
            *context_window = 1024;
        }
        round_trip(result(outcome));
    }

    #[test_case(TurnCancellationReason::User)]
    #[test_case(TurnCancellationReason::Closed)]
    #[test_case(TurnCancellationReason::Shutdown)]
    #[test_case(TurnCancellationReason::Interrupted)]
    #[test_case(TurnCancellationReason::Withdrawn)]
    fn cancelled_outcome_round_trips(reason: TurnCancellationReason) {
        let mut stored = StoredTurnOutcome::from(outcome());
        stored.status = StoredTurnStatus::Cancelled {
            reason: reason.into(),
        };
        round_trip(result(stored.into()));
    }

    #[test_case(TurnFailureKind::Provider)]
    #[test_case(TurnFailureKind::Authentication)]
    #[test_case(TurnFailureKind::Timeout)]
    #[test_case(TurnFailureKind::Tool)]
    #[test_case(TurnFailureKind::Transport)]
    #[test_case(TurnFailureKind::InvalidResponse)]
    #[test_case(TurnFailureKind::Internal)]
    #[test_case(TurnFailureKind::Compaction)]
    fn failed_outcome_round_trips(kind: TurnFailureKind) {
        let expected = TurnOutcome::failed(
            AgentId::generate(),
            TurnId::generate(),
            outcome().usage(),
            3,
            TurnFailure {
                kind,
                diagnostic: DIAGNOSTIC.into(),
                user_message: USER_MESSAGE.into(),
                retryable: true,
            },
        );
        round_trip(result(expected));
    }

    #[test_case(TurnOrigin::Internal)]
    #[test_case(TurnOrigin::User)]
    #[test_case(TurnOrigin::Plugin)]
    fn origins_round_trip(origin: TurnOrigin) {
        let mut expected = result(outcome());
        expected.provenance.origin = origin;
        round_trip(expected);
    }

    fn round_trip(expected: TurnResult) {
        let mut expected = expected;
        match &mut expected.outcome {
            TurnOutcome::Completed {
                cost,
                list_cost,
                context_size,
                context_window,
                ..
            }
            | TurnOutcome::Failed {
                cost,
                list_cost,
                context_size,
                context_window,
                ..
            }
            | TurnOutcome::Cancelled {
                cost,
                list_cost,
                context_size,
                context_window,
                ..
            } => {
                *cost = Some(0.125);
                *list_cost = Some(0.25);
                *context_size = 127;
                *context_window = 1024;
            }
        }
        let stored = expected.to_stored(MakimaId::generate(), None).unwrap();
        let encoded = serde_json::to_vec(&stored).unwrap();
        let stored = serde_json::from_slice(&encoded).unwrap();
        let restored =
            TurnResult::from_stored(stored, |_| panic!("inline result read an artifact")).unwrap();
        assert_eq!(restored, expected);
    }

    #[test_case(MAX_INLINE_TURN_RESULT_BYTES, MAX_INLINE_TURN_RESULT_BYTES; "at_limit")]
    #[test_case(MAX_INLINE_TURN_RESULT_BYTES + 1, MAX_INLINE_TURN_RESULT_BYTES; "text_only")]
    #[test_case(MAX_INLINE_TURN_RESULT_BYTES, MAX_INLINE_TURN_RESULT_BYTES + 1; "output_only")]
    #[test_case(MAX_INLINE_TURN_RESULT_BYTES + 1, MAX_INLINE_TURN_RESULT_BYTES + 1; "both")]
    fn payloads_obey_independent_inline_limits(text_bytes: usize, output_bytes: usize) {
        const JSON_STRING_ARRAY_OVERHEAD: usize = 4;
        let dir = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(dir.path().to_path_buf());
        let session = MakimaId::generate();
        let mut expected = result(outcome());
        expected.text = "é".repeat(text_bytes / 2);
        if !text_bytes.is_multiple_of(2) {
            expected.text.push('!');
        }
        expected.output = vec![json!("x".repeat(output_bytes - JSON_STRING_ARRAY_OVERHEAD))];
        let stored = expected.to_stored(session, Some(&store)).unwrap();
        assert_eq!(
            matches!(stored.text, StoredPayload::Offloaded(_)),
            text_bytes > MAX_INLINE_TURN_RESULT_BYTES
        );
        assert_eq!(
            matches!(stored.output, StoredPayload::Offloaded(_)),
            output_bytes > MAX_INLINE_TURN_RESULT_BYTES
        );
        let restored = TurnResult::from_stored(stored, |reference| {
            assert_eq!(reference.session, session);
            fs::read(dir.path().join(&reference.path))
        })
        .unwrap();
        assert_eq!(restored, expected);
    }

    #[test]
    fn result_larger_than_preview_cap_restores_losslessly() {
        let dir = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(dir.path().to_path_buf());
        let mut expected = result(outcome());
        expected.text = "x".repeat(MAX_OFFLOAD_FILE_BYTES + 1);
        let stored = expected
            .to_stored(MakimaId::generate(), Some(&store))
            .unwrap();
        let StoredPayload::Offloaded(reference) = &stored.text else {
            panic!("oversized result was stored inline");
        };
        assert_eq!(reference.bytes, expected.text.len() as u64);
        let restored = TurnResult::from_stored(stored, |reference| {
            fs::read(dir.path().join(&reference.path))
        })
        .unwrap();
        assert_eq!(restored, expected);
    }

    #[test]
    fn oversized_result_requires_a_working_offload_store() {
        let mut expected = result(outcome());
        expected.text = "x".repeat(MAX_INLINE_TURN_RESULT_BYTES + 1);
        let session = MakimaId::generate();
        assert!(matches!(
            expected.to_stored(session, None),
            Err(TurnResultStorageError::MissingOffloadStore)
        ));
        let dir = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(dir.path().to_path_buf());
        store.request_close();
        assert!(matches!(
            expected.to_stored(session, Some(&store)),
            Err(TurnResultStorageError::Offload(OffloadError::Closed))
        ));
    }

    #[test_case("../result.txt"; "parent_escape")]
    #[test_case("/result.txt"; "absolute_path")]
    #[test_case("result/nested.txt"; "nested_path")]
    #[test_case("result\\nested.txt"; "windows_separator")]
    #[test_case("C:result.txt"; "windows_prefix")]
    #[test_case(""; "empty")]
    fn invalid_reference_is_rejected_before_resolution(path: &str) {
        let mut stored = result(outcome())
            .to_stored(MakimaId::generate(), None)
            .unwrap();
        stored.text = StoredPayload::Offloaded(StoredOffloadRef {
            session: MakimaId::generate(),
            path: path.into(),
            bytes: 0,
        });
        assert!(matches!(
            TurnResult::from_stored(stored, |_| panic!("invalid reference was resolved")),
            Err(TurnResultStorageError::InvalidReference)
        ));
    }

    #[test]
    fn unavailable_or_corrupt_payloads_return_typed_errors() {
        let mut stored = result(outcome())
            .to_stored(MakimaId::generate(), None)
            .unwrap();
        let reference = StoredOffloadRef {
            session: MakimaId::generate(),
            path: ARTIFACT.into(),
            bytes: 1,
        };
        stored.text = StoredPayload::Offloaded(reference.clone());
        let missing = TurnResult::from_stored(stored.clone(), |_| {
            Err(io::Error::new(io::ErrorKind::NotFound, MISSING_ARTIFACT))
        });
        assert!(
            matches!(missing, Err(TurnResultStorageError::Unavailable(error)) if error.to_string() == MISSING_ARTIFACT)
        );
        assert!(matches!(
            TurnResult::from_stored(stored.clone(), |_| Ok(Vec::new())),
            Err(TurnResultStorageError::LengthMismatch {
                expected: 1,
                actual: 0
            })
        ));
        assert!(matches!(
            TurnResult::from_stored(stored.clone(), |_| Ok(vec![0xff])),
            Err(TurnResultStorageError::Utf8(_))
        ));
        stored.text = StoredPayload::Inline(USER_MESSAGE.into());
        stored.output = StoredPayload::Offloaded(reference);
        assert!(matches!(
            TurnResult::from_stored(stored, |_| Ok(vec![b'!'])),
            Err(TurnResultStorageError::Json(_))
        ));
    }
}
