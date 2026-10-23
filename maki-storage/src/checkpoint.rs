use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use event_listener::Event;

use crate::id::MakiId;

const PENDING: u8 = 0;
const CANCELLED: u8 = 1;
const PUBLISHING: u8 = 2;

#[derive(Default)]
struct PublicationState {
    phase: AtomicU8,
    cancelled: Event,
}

/// Arbitrates cancellation against the final durable replacement, not its waiter.
#[derive(Clone, Default)]
pub struct CheckpointPublication(Arc<PublicationState>);

impl CheckpointPublication {
    pub fn cancel(&self) -> bool {
        match self
            .0
            .phase
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                self.0.cancelled.notify(usize::MAX);
                true
            }
            Err(phase) => phase == CANCELLED,
        }
    }

    pub fn same_request(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.phase.load(Ordering::Acquire) == CANCELLED
    }

    pub fn try_publish(&self) -> bool {
        self.0
            .phase
            .compare_exchange(PENDING, PUBLISHING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub async fn cancelled(&self) {
        loop {
            let listener = self.0.cancelled.listen();
            if self.is_cancelled() {
                return;
            }
            listener.await;
        }
    }
}

/// Validates captured authority and claims publication synchronously at replacement.
/// The callback must not acquire actor, manager, or option-catalog locks.
pub type CheckpointClaim =
    Arc<dyn Fn(&CheckpointPublication) -> Result<(), Arc<str>> + Send + Sync + 'static>;

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

impl CheckpointAck {
    pub fn for_request<S>(request: &CheckpointRequest<S>) -> Self {
        Self {
            session_id: request.session_id,
            version: request.version,
        }
    }
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

    fn checkpoint_guarded(
        &self,
        request: CheckpointRequest<S>,
        _publication: CheckpointPublication,
        _claim: CheckpointClaim,
    ) -> CheckpointFuture {
        let session_id = request.session_id;
        Box::pin(async move {
            Err(CheckpointError::Save {
                session_id,
                message: Arc::from("checkpoint writer does not support guarded publication"),
            })
        })
    }
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

/// Acknowledges every ordinary checkpoint without saving it. Guarded
/// publication still fails closed through the trait default.
pub fn accept_all<S: Send + Sync + 'static>() -> Arc<dyn CheckpointWriter<S>> {
    Arc::new(|request: CheckpointRequest<S>| {
        let ack = CheckpointAck::for_request(&request);
        Box::pin(async move { Ok(ack) }) as CheckpointFuture
    })
}

/// Runs the publication claim, then delegates to an ordinary writer.
pub struct ClaimingWriter<S>(pub Arc<dyn CheckpointWriter<S>>);

impl<S: Send + Sync + 'static> ClaimingWriter<S> {
    /// Publishes everything, guarded or not, and saves nothing.
    pub fn accept_all() -> Arc<dyn CheckpointWriter<S>> {
        Arc::new(Self(accept_all()))
    }
}

impl<S: Send + Sync + 'static> CheckpointWriter<S> for ClaimingWriter<S> {
    fn checkpoint(&self, request: CheckpointRequest<S>) -> CheckpointFuture {
        self.0.checkpoint(request)
    }

    fn checkpoint_guarded(
        &self,
        request: CheckpointRequest<S>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> CheckpointFuture {
        let writer = Arc::clone(&self.0);
        Box::pin(async move {
            claim(&publication).map_err(|message| CheckpointError::Save {
                session_id: request.session_id,
                message,
            })?;
            writer.checkpoint(request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use test_case::test_case;

    #[test_case(false; "publication_wins")]
    #[test_case(true; "cancellation_wins")]
    fn publication_has_one_owner(cancel_first: bool) {
        let publication = CheckpointPublication::default();
        if cancel_first {
            assert!(publication.cancel());
            assert!(!publication.try_publish());
            assert!(publication.cancel());
        } else {
            assert!(publication.try_publish());
            assert!(!publication.cancel());
            assert!(!publication.try_publish());
        }
    }

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
