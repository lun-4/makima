use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::id::MakiId;

pub type CheckpointFuture =
    Pin<Box<dyn Future<Output = Result<CheckpointAck, CheckpointError>> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointVersion {
    pub revision: u64,
    pub epoch: u64,
}

#[derive(Debug)]
pub struct CheckpointRequest<S> {
    pub session_id: MakiId,
    pub version: CheckpointVersion,
    pub snapshot: Arc<S>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointAck {
    pub session_id: MakiId,
    pub version: CheckpointVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckpointError {
    #[error("checkpoint writer closed for session {0}")]
    Closed(MakiId),
    #[error("checkpoint failed for session {session_id}: {message}")]
    Save {
        session_id: MakiId,
        message: Arc<str>,
    },
}

pub trait CheckpointWriter<S>: Send + Sync + 'static {
    fn checkpoint(&self, request: CheckpointRequest<S>) -> CheckpointFuture;
}

impl<S, F> CheckpointWriter<S> for F
where
    S: Send + Sync + 'static,
    F: Fn(CheckpointRequest<S>) -> CheckpointFuture + Send + Sync + 'static,
{
    fn checkpoint(&self, request: CheckpointRequest<S>) -> CheckpointFuture {
        self(request)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn acknowledgement_identifies_the_durable_revision() {
        smol::block_on(async {
            let observed = Arc::new(Mutex::new(Vec::new()));
            let writer: Arc<dyn CheckpointWriter<String>> = Arc::new({
                let observed = Arc::clone(&observed);
                move |request: CheckpointRequest<String>| {
                    let observed = Arc::clone(&observed);
                    Box::pin(async move {
                        observed.lock().unwrap().push(request.version);
                        Ok(CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as CheckpointFuture
                }
            });
            let session_id = MakiId::generate();
            let version = CheckpointVersion {
                revision: 7,
                epoch: 3,
            };

            let ack = writer
                .checkpoint(CheckpointRequest {
                    session_id,
                    version,
                    snapshot: Arc::new("state".into()),
                })
                .await
                .unwrap();

            assert_eq!(ack.session_id, session_id);
            assert_eq!(ack.version, version);
            assert_eq!(observed.lock().unwrap().as_slice(), &[version]);
        });
    }

    #[test]
    fn save_failure_is_returned_to_the_requester() {
        smol::block_on(async {
            let writer: Arc<dyn CheckpointWriter<String>> =
                Arc::new(|request: CheckpointRequest<String>| {
                    Box::pin(async move {
                        Err(CheckpointError::Save {
                            session_id: request.session_id,
                            message: Arc::from("disk full"),
                        })
                    }) as CheckpointFuture
                });
            let session_id = MakiId::generate();

            assert!(matches!(
                writer
                    .checkpoint(CheckpointRequest {
                        session_id,
                        version: CheckpointVersion {
                            revision: 1,
                            epoch: 1,
                        },
                        snapshot: Arc::new("state".into()),
                    })
                    .await,
                Err(CheckpointError::Save { session_id: failed, .. }) if failed == session_id
            ));
        });
    }
}
