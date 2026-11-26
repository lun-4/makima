use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use flume::{Receiver, Sender};
use makima_domain::LockExt;

use crate::{AgentEvent, Envelope};

#[derive(Clone)]
pub enum SessionEvent {
    Agent(Envelope),
}

type Observer = Box<dyn FnMut(&Envelope) + Send>;

struct PumpState {
    observer: Option<Observer>,
    subscribers: Option<Vec<Sender<SessionEvent>>>,
}

impl PumpState {
    fn publish(&mut self, envelope: Option<Envelope>) {
        let Some(subscribers) = self.subscribers.as_mut() else {
            return;
        };
        let Some(envelope) = envelope else {
            self.subscribers = None;
            return;
        };
        if let Some(observe) = &mut self.observer {
            observe(&envelope);
        }
        let closed = matches!(envelope.event, AgentEvent::StreamClosed);
        subscribers.retain(|tx| !tx.is_disconnected());
        if let Some((last, others)) = subscribers.split_last() {
            let event = SessionEvent::Agent(envelope);
            for tx in others {
                let _ = tx.send(event.clone());
            }
            let _ = last.send(event);
        }
        if closed {
            self.subscribers = None;
        }
    }
}

/// A single reader and ordered fan-out for a session. Subscribers drive
/// the pump so a synchronous drain also sees events already in the raw FIFO.
pub struct SessionEventPump {
    raw: Receiver<Envelope>,
    state: Mutex<PumpState>,
}

impl SessionEventPump {
    pub fn new(raw: Receiver<Envelope>) -> Arc<Self> {
        Arc::new(Self {
            raw,
            state: Mutex::new(PumpState {
                observer: None,
                subscribers: Some(Vec::new()),
            }),
        })
    }

    pub fn set_observer(&self, observe: impl FnMut(&Envelope) + Send + 'static) {
        self.state.lock_unpoisoned().observer = Some(Box::new(observe));
    }

    pub fn subscribe(self: &Arc<Self>) -> SessionSubscription {
        let (tx, rx) = flume::unbounded();
        if let Some(subscribers) = &mut self.state.lock_unpoisoned().subscribers {
            subscribers.push(tx);
        }
        SessionSubscription {
            pump: Arc::clone(self),
            rx,
        }
    }
}

pub struct SessionSubscription {
    pump: Arc<SessionEventPump>,
    rx: Receiver<SessionEvent>,
}

impl SessionSubscription {
    /// Checks both the subscriber backlog and events that have not been published yet.
    pub fn is_empty(&self) -> bool {
        let state = self.pump.state.lock_unpoisoned();
        self.rx.is_empty() && (state.subscribers.is_none() || self.pump.raw.is_empty())
    }

    pub fn try_next(&self) -> Option<SessionEvent> {
        let mut state = self.pump.state.lock_unpoisoned();
        if let Ok(event) = self.rx.try_recv() {
            return Some(event);
        }
        state.subscribers.as_ref()?;
        match self.pump.raw.try_recv() {
            Ok(envelope) => state.publish(Some(envelope)),
            Err(flume::TryRecvError::Disconnected) => state.publish(None),
            Err(flume::TryRecvError::Empty) => return None,
        }
        self.rx.try_recv().ok()
    }

    pub async fn next(&self) -> Option<SessionEvent> {
        let mut raw = pin!(self.pump.raw.recv_async());
        let mut output = pin!(self.rx.recv_async());
        poll_fn(|cx| {
            let mut state = self.pump.state.lock_unpoisoned();
            if let Poll::Ready(event) = output.as_mut().poll(cx) {
                return Poll::Ready(event.ok());
            }
            if let Poll::Ready(event) = raw.as_mut().poll(cx) {
                state.publish(event.ok());
                return output.as_mut().poll(cx).map(Result::ok);
            }
            Poll::Pending
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::future::poll_once;

    use super::{SessionEvent, SessionEventPump};
    use crate::{AgentEvent, event_stream};

    const TEXT: [&str; 2] = ["first", "second"];

    #[test]
    fn observes_before_ordered_fanout() {
        smol::block_on(async {
            let (guard, raw) = event_stream();
            let pump = SessionEventPump::new(raw.into_receiver());
            let (observed, observations) = flume::unbounded();
            pump.set_observer(move |event| {
                if let AgentEvent::TextDelta { text } = &event.event {
                    observed.send(text.clone()).unwrap();
                }
            });
            let first = pump.subscribe();
            let second = pump.subscribe();
            let mut waiting = Box::pin(second.next());
            assert!(poll_once(&mut waiting).await.is_none());
            for text in TEXT {
                guard
                    .sender()
                    .send(AgentEvent::TextDelta { text: text.into() })
                    .unwrap();
            }
            assert!(!first.is_empty());
            assert!(!second.is_empty());
            for text in TEXT {
                let SessionEvent::Agent(event) = first.try_next().unwrap();
                assert!(
                    matches!(event.event, AgentEvent::TextDelta { text: actual } if actual == text)
                );
                assert_eq!(observations.try_recv().unwrap(), text);
            }
            assert!(first.is_empty());
            assert!(!second.is_empty());
            let mut event = waiting.await;
            for text in TEXT {
                let Some(SessionEvent::Agent(envelope)) = event else {
                    panic!("missing text event");
                };
                assert!(
                    matches!(envelope.event, AgentEvent::TextDelta { text: actual } if actual == text)
                );
                event = second.try_next();
            }
            assert!(event.is_none());
            let retained_sender = guard.sender();
            drop(guard);
            retained_sender.send(AgentEvent::QueueSettled).unwrap();
            for sub in [&first, &second] {
                assert!(!sub.is_empty());
                assert!(
                    matches!(sub.next().await, Some(SessionEvent::Agent(event)) if matches!(event.event, AgentEvent::StreamClosed))
                );
                assert!(sub.is_empty());
                assert!(sub.next().await.is_none());
            }
        });
    }
}
