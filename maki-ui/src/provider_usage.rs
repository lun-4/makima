use maki_providers::ProviderUsage;

const FIRST_FETCH_ID: u64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProviderInstanceGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProviderAuthGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProviderIdentity {
    pub instance: ProviderInstanceGeneration,
    pub auth: ProviderAuthGeneration,
}

impl ProviderIdentity {
    pub const fn new(instance: ProviderInstanceGeneration, auth: ProviderAuthGeneration) -> Self {
        Self { instance, auth }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProviderUsageFetchId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderUsageFetch {
    pub id: ProviderUsageFetchId,
    pub provider: ProviderIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderUsageRequestKind {
    Ordinary,
    Forced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderUsageFetchResult {
    Ready(ProviderUsage),
    Unsupported,
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderUsageResolution {
    Completed(ProviderUsageFetchResult),
    CompletedWithIdentity {
        provider: ProviderIdentity,
        result: ProviderUsageFetchResult,
    },
    ProviderChanged {
        previous: ProviderIdentity,
        current: ProviderIdentity,
    },
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderUsageInput<W> {
    Request {
        provider: ProviderIdentity,
        kind: ProviderUsageRequestKind,
        waiter: W,
    },
    Completed {
        fetch_id: ProviderUsageFetchId,
        provider: ProviderIdentity,
        result: ProviderUsageFetchResult,
    },
    Transition {
        provider: ProviderIdentity,
    },
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderUsageOutput<W> {
    StartFetch(ProviderUsageFetch),
    Resolve {
        waiters: Vec<W>,
        resolution: ProviderUsageResolution,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderUsageCoordinatorState {
    Idle,
    Fetching {
        active: ProviderUsageFetchId,
        follow_up_queued: bool,
    },
    Shutdown,
}

struct ActiveFetch<W> {
    fetch: ProviderUsageFetch,
    waiters: Vec<W>,
}

/// Pure state machine for coalescing provider quota requests.
///
/// The caller owns task execution and waiter delivery. It submits inputs and
/// performs the returned outputs in order. Fetch ids make completion delivery
/// safe across provider and authentication transitions.
pub struct ProviderUsageCoordinator<W> {
    provider: ProviderIdentity,
    active: Option<ActiveFetch<W>>,
    pending: Option<Vec<W>>,
    next_fetch_id: u64,
    shutdown: bool,
}

impl<W> ProviderUsageCoordinator<W> {
    pub fn new(provider: ProviderIdentity) -> Self {
        Self {
            provider,
            active: None,
            pending: None,
            next_fetch_id: FIRST_FETCH_ID,
            shutdown: false,
        }
    }

    pub fn provider(&self) -> &ProviderIdentity {
        &self.provider
    }

    pub fn state(&self) -> ProviderUsageCoordinatorState {
        if self.shutdown {
            return ProviderUsageCoordinatorState::Shutdown;
        }
        match &self.active {
            Some(active) => ProviderUsageCoordinatorState::Fetching {
                active: active.fetch.id,
                follow_up_queued: self.pending.is_some(),
            },
            None => ProviderUsageCoordinatorState::Idle,
        }
    }

    pub fn handle(&mut self, input: ProviderUsageInput<W>) -> Vec<ProviderUsageOutput<W>> {
        match input {
            ProviderUsageInput::Request {
                provider,
                kind,
                waiter,
            } => self.request(provider, kind, waiter),
            ProviderUsageInput::Completed {
                fetch_id,
                provider,
                result,
            } => self.complete(fetch_id, provider, result),
            ProviderUsageInput::Transition { provider } => self.transition(provider),
            ProviderUsageInput::Shutdown => self.shutdown(),
        }
    }

    fn request(
        &mut self,
        provider: ProviderIdentity,
        kind: ProviderUsageRequestKind,
        waiter: W,
    ) -> Vec<ProviderUsageOutput<W>> {
        if self.shutdown {
            return vec![Self::resolve(
                vec![waiter],
                ProviderUsageResolution::Shutdown,
            )];
        }
        if provider != self.provider {
            return vec![Self::resolve(
                vec![waiter],
                ProviderUsageResolution::ProviderChanged {
                    previous: provider,
                    current: self.provider,
                },
            )];
        }
        if let Some(pending) = self.pending.as_mut() {
            pending.push(waiter);
            return Vec::new();
        }
        if let Some(active) = self.active.as_mut() {
            if active.fetch.provider != self.provider {
                self.pending = Some(vec![waiter]);
            } else {
                match kind {
                    ProviderUsageRequestKind::Ordinary => active.waiters.push(waiter),
                    ProviderUsageRequestKind::Forced => self.pending = Some(vec![waiter]),
                }
            }
            return Vec::new();
        }
        vec![ProviderUsageOutput::StartFetch(
            self.start_fetch(vec![waiter]),
        )]
    }

    fn complete(
        &mut self,
        fetch_id: ProviderUsageFetchId,
        provider: ProviderIdentity,
        result: ProviderUsageFetchResult,
    ) -> Vec<ProviderUsageOutput<W>> {
        if self.shutdown || self.active.as_ref().map(|active| active.fetch.id) != Some(fetch_id) {
            return Vec::new();
        }
        let Some(active) = self.active.take() else {
            return Vec::new();
        };
        let stale = provider != active.fetch.provider;
        let mut outputs = Vec::new();
        if !stale && !active.waiters.is_empty() {
            outputs.push(Self::resolve(
                active.waiters,
                ProviderUsageResolution::CompletedWithIdentity {
                    provider: active.fetch.provider,
                    result,
                },
            ));
        }
        if let Some(waiters) = self.pending.take() {
            outputs.push(ProviderUsageOutput::StartFetch(self.start_fetch(waiters)));
        }
        outputs
    }

    fn transition(&mut self, provider: ProviderIdentity) -> Vec<ProviderUsageOutput<W>> {
        if self.shutdown || provider == self.provider {
            return Vec::new();
        }
        let previous = std::mem::replace(&mut self.provider, provider);
        let mut waiters = self
            .active
            .take()
            .map(|active| active.waiters)
            .unwrap_or_default();
        if let Some(mut pending) = self.pending.take() {
            waiters.append(&mut pending);
        }
        if waiters.is_empty() {
            return Vec::new();
        }
        vec![Self::resolve(
            waiters,
            ProviderUsageResolution::ProviderChanged {
                previous,
                current: provider,
            },
        )]
    }

    fn shutdown(&mut self) -> Vec<ProviderUsageOutput<W>> {
        if std::mem::replace(&mut self.shutdown, true) {
            return Vec::new();
        }
        let waiters = self.take_waiters();
        if waiters.is_empty() {
            return Vec::new();
        }
        vec![Self::resolve(waiters, ProviderUsageResolution::Shutdown)]
    }

    fn start_fetch(&mut self, waiters: Vec<W>) -> ProviderUsageFetch {
        let fetch = ProviderUsageFetch {
            id: ProviderUsageFetchId(self.next_fetch_id),
            provider: self.provider,
        };
        self.next_fetch_id = self.next_fetch_id.wrapping_add(1);
        self.active = Some(ActiveFetch {
            fetch: fetch.clone(),
            waiters,
        });
        fetch
    }

    fn take_waiters(&mut self) -> Vec<W> {
        let mut waiters = self
            .active
            .take()
            .map(|active| active.waiters)
            .unwrap_or_default();
        if let Some(mut pending) = self.pending.take() {
            waiters.append(&mut pending);
        }
        waiters
    }

    fn resolve(waiters: Vec<W>, resolution: ProviderUsageResolution) -> ProviderUsageOutput<W> {
        ProviderUsageOutput::Resolve {
            waiters,
            resolution,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ProviderAuthGeneration, ProviderIdentity, ProviderInstanceGeneration,
        ProviderUsageCoordinator, ProviderUsageCoordinatorState, ProviderUsageFetch,
        ProviderUsageFetchId, ProviderUsageFetchResult, ProviderUsageInput, ProviderUsageOutput,
        ProviderUsageRequestKind, ProviderUsageResolution,
    };

    const INSTANCE: ProviderInstanceGeneration = ProviderInstanceGeneration(1);
    const NEXT_INSTANCE: ProviderInstanceGeneration = ProviderInstanceGeneration(2);
    const AUTH: ProviderAuthGeneration = ProviderAuthGeneration(3);
    const NEXT_AUTH: ProviderAuthGeneration = ProviderAuthGeneration(4);
    const FIRST_FETCH: ProviderUsageFetchId = ProviderUsageFetchId(0);
    const SECOND_FETCH: ProviderUsageFetchId = ProviderUsageFetchId(1);
    const FIRST_WAITER: u8 = 10;
    const SECOND_WAITER: u8 = 11;
    const THIRD_WAITER: u8 = 12;

    fn provider() -> ProviderIdentity {
        ProviderIdentity::new(INSTANCE, AUTH)
    }

    fn next_provider() -> ProviderIdentity {
        ProviderIdentity::new(NEXT_INSTANCE, NEXT_AUTH)
    }

    fn request(
        provider: ProviderIdentity,
        kind: ProviderUsageRequestKind,
        waiter: u8,
    ) -> ProviderUsageInput<u8> {
        ProviderUsageInput::Request {
            provider,
            kind,
            waiter,
        }
    }

    fn completed(fetch_id: ProviderUsageFetchId) -> ProviderUsageInput<u8> {
        completed_for(fetch_id, provider())
    }

    fn completed_for(
        fetch_id: ProviderUsageFetchId,
        identity: ProviderIdentity,
    ) -> ProviderUsageInput<u8> {
        ProviderUsageInput::Completed {
            fetch_id,
            provider: identity,
            result: ProviderUsageFetchResult::Unsupported,
        }
    }

    fn start(
        fetch_id: ProviderUsageFetchId,
        provider: ProviderIdentity,
    ) -> ProviderUsageOutput<u8> {
        ProviderUsageOutput::StartFetch(ProviderUsageFetch {
            id: fetch_id,
            provider,
        })
    }

    fn resolved(waiters: Vec<u8>, resolution: ProviderUsageResolution) -> ProviderUsageOutput<u8> {
        ProviderUsageOutput::Resolve {
            waiters,
            resolution,
        }
    }

    #[test]
    fn ordinary_requests_join_active_fetch() {
        let identity = provider();
        let mut coordinator = ProviderUsageCoordinator::new(identity);

        assert_eq!(
            coordinator.handle(request(
                identity,
                ProviderUsageRequestKind::Ordinary,
                FIRST_WAITER,
            )),
            vec![start(FIRST_FETCH, identity)]
        );
        assert!(
            coordinator
                .handle(request(
                    identity,
                    ProviderUsageRequestKind::Ordinary,
                    SECOND_WAITER,
                ))
                .is_empty()
        );
        assert_eq!(
            coordinator.handle(completed(FIRST_FETCH)),
            vec![resolved(
                vec![FIRST_WAITER, SECOND_WAITER],
                ProviderUsageResolution::CompletedWithIdentity {
                    provider: provider(),
                    result: ProviderUsageFetchResult::Unsupported,
                },
            )]
        );
        assert_eq!(coordinator.state(), ProviderUsageCoordinatorState::Idle);
    }

    #[test]
    fn forced_request_queues_one_follow_up() {
        let identity = provider();
        let mut coordinator = ProviderUsageCoordinator::new(identity);

        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Ordinary,
            FIRST_WAITER,
        ));
        assert!(
            coordinator
                .handle(request(
                    identity,
                    ProviderUsageRequestKind::Forced,
                    SECOND_WAITER,
                ))
                .is_empty()
        );
        assert!(
            coordinator
                .handle(request(
                    identity,
                    ProviderUsageRequestKind::Forced,
                    THIRD_WAITER,
                ))
                .is_empty()
        );

        assert_eq!(
            coordinator.handle(completed(FIRST_FETCH)),
            vec![
                resolved(
                    vec![FIRST_WAITER],
                    ProviderUsageResolution::CompletedWithIdentity {
                        provider: identity,
                        result: ProviderUsageFetchResult::Unsupported
                    },
                ),
                start(SECOND_FETCH, identity),
            ]
        );
        assert_eq!(
            coordinator.handle(completed_for(SECOND_FETCH, identity)),
            vec![resolved(
                vec![SECOND_WAITER, THIRD_WAITER],
                ProviderUsageResolution::CompletedWithIdentity {
                    provider: identity,
                    result: ProviderUsageFetchResult::Unsupported
                },
            )]
        );
    }

    #[test]
    fn ordinary_request_after_forced_joins_pending_fetch() {
        let identity = provider();
        let mut coordinator = ProviderUsageCoordinator::new(identity);

        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Ordinary,
            FIRST_WAITER,
        ));
        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Forced,
            SECOND_WAITER,
        ));
        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Ordinary,
            THIRD_WAITER,
        ));

        assert_eq!(
            coordinator.handle(completed(FIRST_FETCH)),
            vec![
                resolved(
                    vec![FIRST_WAITER],
                    ProviderUsageResolution::CompletedWithIdentity {
                        provider: identity,
                        result: ProviderUsageFetchResult::Unsupported
                    },
                ),
                start(SECOND_FETCH, identity),
            ]
        );
        assert_eq!(
            coordinator.handle(completed_for(SECOND_FETCH, identity)),
            vec![resolved(
                vec![SECOND_WAITER, THIRD_WAITER],
                ProviderUsageResolution::CompletedWithIdentity {
                    provider: identity,
                    result: ProviderUsageFetchResult::Unsupported
                },
            )]
        );
    }

    #[test]
    fn provider_transition_resolves_active_and_pending_old_waiters() {
        let identity = provider();
        let next = next_provider();
        let mut coordinator = ProviderUsageCoordinator::new(identity);

        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Ordinary,
            FIRST_WAITER,
        ));
        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Forced,
            SECOND_WAITER,
        ));

        assert_eq!(
            coordinator.handle(ProviderUsageInput::Transition { provider: next }),
            vec![resolved(
                vec![FIRST_WAITER, SECOND_WAITER],
                ProviderUsageResolution::ProviderChanged {
                    previous: identity,
                    current: next,
                },
            )]
        );
        assert_eq!(coordinator.provider(), &next);
        assert_eq!(coordinator.state(), ProviderUsageCoordinatorState::Idle);
        assert_eq!(
            coordinator.handle(request(
                next,
                ProviderUsageRequestKind::Ordinary,
                THIRD_WAITER,
            )),
            vec![start(SECOND_FETCH, next)]
        );
    }

    #[test]
    fn obsolete_completion_is_discarded() {
        let identity = provider();
        let next = next_provider();
        let mut coordinator = ProviderUsageCoordinator::new(identity);

        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Ordinary,
            FIRST_WAITER,
        ));
        coordinator.handle(ProviderUsageInput::Transition { provider: next });
        assert_eq!(
            coordinator.handle(request(
                next,
                ProviderUsageRequestKind::Ordinary,
                SECOND_WAITER,
            )),
            vec![start(SECOND_FETCH, next)]
        );

        assert!(coordinator.handle(completed(FIRST_FETCH)).is_empty());
        assert_eq!(
            coordinator.state(),
            ProviderUsageCoordinatorState::Fetching {
                active: SECOND_FETCH,
                follow_up_queued: false,
            }
        );
        assert_eq!(
            coordinator.handle(completed_for(SECOND_FETCH, next)),
            vec![resolved(
                vec![SECOND_WAITER],
                ProviderUsageResolution::CompletedWithIdentity {
                    provider: next,
                    result: ProviderUsageFetchResult::Unsupported
                },
            )]
        );
    }

    #[test]
    fn shutdown_resolves_active_and_pending_waiters() {
        let identity = provider();
        let mut coordinator = ProviderUsageCoordinator::new(identity);

        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Ordinary,
            FIRST_WAITER,
        ));
        coordinator.handle(request(
            identity,
            ProviderUsageRequestKind::Forced,
            SECOND_WAITER,
        ));

        assert_eq!(
            coordinator.handle(ProviderUsageInput::Shutdown),
            vec![resolved(
                vec![FIRST_WAITER, SECOND_WAITER],
                ProviderUsageResolution::Shutdown,
            )]
        );
        assert_eq!(coordinator.state(), ProviderUsageCoordinatorState::Shutdown);
        assert!(coordinator.handle(completed(FIRST_FETCH)).is_empty());
        assert_eq!(
            coordinator.handle(request(
                identity,
                ProviderUsageRequestKind::Ordinary,
                THIRD_WAITER,
            )),
            vec![resolved(
                vec![THIRD_WAITER],
                ProviderUsageResolution::Shutdown,
            )]
        );
    }
}
