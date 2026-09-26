//! Queue of work handed from the UI to the agent actor.
//!
//! Production normally delegates to the actor's scheduling queue. During a
//! plan transition, a shared gate holds new UI work outside the actor until
//! settlement, while still projecting it in the queue panel and persistence.
//!
//! The `#[cfg(test)]` variant is a presentation-only deque used exclusively
//! by App unit tests that need deterministic queue-panel behavior without an
//! actor runner racing their assertions. It never compiles into production.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_agent::actor::{ActorError, AgentActorHandle, QueueProjection, RootWork};
use maki_agent::{AgentInput, ImageSource};
use maki_commands::COMPACT_COMMAND_NAME;
use tracing::warn;

use crate::components::input::Submission;
use crate::components::queue_panel::QueueEntry;
use crate::theme;

pub(crate) struct QueuedMessage {
    pub(crate) text: String,
    pub(crate) images: Vec<ImageSource>,
}

impl From<Submission> for QueuedMessage {
    fn from(sub: Submission) -> Self {
        Self {
            text: sub.text,
            images: sub.images,
        }
    }
}

pub(crate) enum QueueItem {
    Message {
        text: String,
        image_count: usize,
        input: AgentInput,
        run_id: u64,
        /// `true` when the UI already drew the bubble (immediate dispatch).
        /// The agent then skips `QueueItemConsumed` so we don't draw it twice.
        /// `false` when the user typed while the agent was busy: the UI waits
        /// for `QueueItemConsumed` before drawing.
        displayed: bool,
    },
    Compact {
        run_id: u64,
        instructions: Option<String>,
    },
}

impl QueueItem {
    pub(crate) fn run_id(&self) -> u64 {
        match self {
            Self::Message { run_id, .. } | Self::Compact { run_id, .. } => *run_id,
        }
    }
}

/// The scheduling backend behind a [`QueueSender`]. The test variant is
/// compiled out of production builds.
#[derive(Clone)]
pub(crate) enum QueueBackend {
    /// The actor's scheduling queue, via its handle.
    Actor(Arc<AgentActorHandle>),
    /// Presentation-only deque for deterministic App unit tests; never
    /// compiled into production and never a scheduling source.
    #[cfg(test)]
    Test(Arc<Mutex<VecDeque<QueueItem>>>),
}

/// Actor-backed queue facade shared with the app. Clones share the gate so
/// work cannot pass held messages during a plan transition.
#[derive(Clone)]
pub(crate) struct QueueSender {
    backend: QueueBackend,
    gate: Arc<Mutex<QueueGate>>,
    /// App-visible run id of the most recent message/compact push, shared so
    /// the backend can stamp controls/compacts that carry no correlation.
    last_run_id: Arc<AtomicU64>,
}

#[derive(Default)]
struct QueueGate {
    gated: bool,
    held: VecDeque<QueueItem>,
}

/// Actor-backed facade used by production `AgentHandles`.
pub(crate) fn actor_queue(actor: Arc<AgentActorHandle>, run_id: Arc<AtomicU64>) -> QueueSender {
    QueueSender {
        backend: QueueBackend::Actor(actor),
        gate: Arc::new(Mutex::new(QueueGate::default())),
        last_run_id: run_id,
    }
}

/// Test-only fixture: a deterministic presentation deque with no actor.
/// Returns a sender App tests can drive synchronously.
#[cfg(test)]
pub(crate) fn queue() -> QueueSender {
    let items: Arc<Mutex<VecDeque<QueueItem>>> = Arc::new(Mutex::new(VecDeque::new()));
    QueueSender {
        backend: QueueBackend::Test(items),
        gate: Arc::new(Mutex::new(QueueGate::default())),
        last_run_id: Arc::new(AtomicU64::new(0)),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Correlation the TUI stamps on actor root/turn admissions. Purely
/// presentation: the actor owns identity and the TUI only correlates events.
pub(crate) fn correlation(run_id: u64) -> String {
    format!("r{run_id}")
}

impl QueueSender {
    pub(crate) fn set_gated(&self, gated: bool) {
        let mut gate = lock(&self.gate);
        gate.gated = gated;
        if !gated {
            while let Some(entry) = gate.held.pop_front() {
                self.push_to_backend(entry);
            }
        }
    }

    pub(crate) fn clear_held(&self) {
        lock(&self.gate).held.clear();
    }

    pub(crate) fn push(&self, entry: QueueItem) {
        let mut gate = lock(&self.gate);
        self.last_run_id.store(entry.run_id(), Ordering::Relaxed);
        if gate.gated {
            gate.held.push_back(entry);
        } else {
            self.push_to_backend(entry);
        }
    }

    fn push_to_backend(&self, entry: QueueItem) {
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test(items) => lock(items).push_back(entry),
            QueueBackend::Actor(actor) => {
                if let Err(error) = push_to_actor(actor, entry) {
                    warn!(error = %error, "agent queue push failed");
                }
            }
        }
    }

    /// Removes the panel-visible item at `index`, including held work.
    pub(crate) fn remove(&self, index: usize) -> bool {
        let mut gate = lock(&self.gate);
        let backend_visible = match &self.backend {
            #[cfg(test)]
            QueueBackend::Test(items) => lock(items)
                .iter()
                .filter(|item| visible_in_panel(&(*item).into()))
                .count(),
            QueueBackend::Actor(actor) => actor
                .snapshot()
                .queue
                .iter()
                .filter(|entry| visible_in_panel(entry))
                .count(),
        };
        if index >= backend_visible {
            let held_index = gate
                .held
                .iter()
                .enumerate()
                .filter(|(_, item)| visible_in_panel(&(*item).into()))
                .nth(index - backend_visible)
                .map(|(index, _)| index);
            return held_index
                .and_then(|index| gate.held.remove(index))
                .is_some();
        }
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test(items) => {
                let mut items = lock(items);
                let raw_index = items
                    .iter()
                    .enumerate()
                    .filter(|(_, item)| visible_in_panel(&(*item).into()))
                    .nth(index)
                    .map(|(index, _)| index);
                raw_index.and_then(|index| items.remove(index)).is_some()
            }
            QueueBackend::Actor(actor) => actor.remove_visible_at(index).is_some(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        let gate = lock(&self.gate);
        gate.held.len()
            + match &self.backend {
                #[cfg(test)]
                QueueBackend::Test(items) => lock(items).len(),
                QueueBackend::Actor(actor) => actor.snapshot().queued,
            }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn clear(&self) {
        let mut gate = lock(&self.gate);
        gate.held.clear();
        match &self.backend {
            #[cfg(test)]
            QueueBackend::Test(items) => lock(items).clear(),
            QueueBackend::Actor(actor) => {
                let _ = actor.clear();
            }
        }
    }

    pub(crate) fn text_messages(&self) -> Vec<String> {
        self.projections()
            .into_iter()
            .filter_map(|entry| match entry {
                QueueProjection::Message {
                    text,
                    displayed: false,
                    ..
                } => Some(text),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn panel_len(&self) -> usize {
        self.projections()
            .into_iter()
            .filter(visible_in_panel)
            .count()
    }

    pub(crate) fn panel_entries(&self) -> Vec<QueueEntry<'static>> {
        self.projections()
            .into_iter()
            .filter(visible_in_panel)
            .map(|entry| as_queue_entry(&entry))
            .collect()
    }

    fn projections(&self) -> Vec<QueueProjection> {
        let gate = lock(&self.gate);
        let mut projections = match &self.backend {
            #[cfg(test)]
            QueueBackend::Test(items) => lock(items).iter().map(Into::into).collect(),
            QueueBackend::Actor(actor) => actor.snapshot().queue,
        };
        projections.extend(gate.held.iter().map(Into::into));
        projections
    }
}

/// Translates a TUI [`QueueItem`] into actor work. Deferred messages become
/// root inputs with no `TurnId` until the scheduler starts or folds them;
/// compacts become actor compact commands. Immediate-dispatch messages are
/// admitted as real turns (the actor retains their outcome).
fn push_to_actor(actor: &AgentActorHandle, entry: QueueItem) -> Result<(), ActorError> {
    match entry {
        QueueItem::Message {
            text,
            image_count: _,
            input,
            run_id,
            displayed,
        } => {
            let images = input.images.clone();
            if displayed {
                let _ticket = actor.admit_turn(input, None, correlation(run_id))?;
                Ok(())
            } else {
                actor.rush(RootWork::new(
                    input,
                    run_id,
                    displayed,
                    text,
                    images,
                    correlation(run_id),
                ))
            }
        }
        QueueItem::Compact {
            run_id,
            instructions,
        } => actor.push_compact(run_id, instructions),
    }
}

fn visible_in_panel(entry: &QueueProjection) -> bool {
    match entry {
        QueueProjection::Message { displayed, .. } => !displayed,
        // Admitted turns project as `Turn`; they are already running or
        // already drawn, so the panel never reserves a row for them.
        QueueProjection::Compact(_) => true,
        QueueProjection::Control(_) | QueueProjection::Turn(_) => false,
    }
}

fn as_queue_entry(entry: &QueueProjection) -> QueueEntry<'static> {
    match entry {
        QueueProjection::Message { text, .. } => QueueEntry {
            text: Cow::Owned(text.clone()),
            color: theme::current().foreground,
        },
        QueueProjection::Compact(instructions) => QueueEntry {
            text: match instructions {
                Some(extra) => Cow::Owned(format!("{COMPACT_COMMAND_NAME} {extra}")),
                None => Cow::Borrowed(COMPACT_COMMAND_NAME),
            },
            color: theme::current()
                .queue
                .fg
                .unwrap_or(theme::current().foreground),
        },
        QueueProjection::Control(name) | QueueProjection::Turn(name) => QueueEntry {
            text: Cow::Owned(name.clone()),
            color: theme::current().foreground,
        },
    }
}

impl From<&QueueItem> for QueueProjection {
    fn from(item: &QueueItem) -> Self {
        match item {
            QueueItem::Message {
                text,
                image_count,
                displayed,
                ..
            } => QueueProjection::Message {
                text: text.clone(),
                image_count: *image_count,
                displayed: *displayed,
            },
            QueueItem::Compact { instructions, .. } => {
                QueueProjection::Compact(instructions.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    fn msg(displayed: bool) -> QueueItem {
        QueueItem::Message {
            text: "t".into(),
            image_count: 0,
            input: AgentInput {
                message: String::new(),
                mode: Default::default(),
                images: Vec::new(),
                preamble: Vec::new(),
                thinking: Default::default(),
                fast: false,
                workflow: false,
                prompt: None,
                cancel: None,
                lease_committer: None,
            },
            run_id: 0,
            displayed,
        }
    }

    #[test_case(msg(false), true  ; "deferred_message_visible")]
    #[test_case(msg(true),  false ; "displayed_message_hidden")]
    #[test_case(QueueItem::Compact { run_id: 0, instructions: None }, true  ; "compact_visible")]
    fn panel_visibility(item: QueueItem, visible: bool) {
        let tx = queue();
        tx.push(item);
        let expected = usize::from(visible);
        assert_eq!(tx.panel_len(), expected);
        assert_eq!(tx.panel_entries().len(), expected);
    }

    #[test]
    fn remove_reports_panel_row_removal() {
        let tx = queue();
        assert!(!tx.remove(0), "empty queue removes nothing");
        tx.push(msg(false));
        assert!(tx.remove(0), "visible row removed");
        assert!(tx.is_empty());
        assert!(!tx.remove(0), "queue is empty again");
    }

    #[test]
    fn gate_releases_fifo_across_clones() {
        let tx = queue();
        let clone = tx.clone();
        tx.push(msg(false));
        tx.set_gated(true);
        clone.push(QueueItem::Compact {
            run_id: 1,
            instructions: None,
        });
        tx.push(msg(true));
        clone.push(QueueItem::Compact {
            run_id: 2,
            instructions: Some("last".into()),
        });
        assert_eq!(lock(&tx.gate).held.len(), 3);
        let QueueBackend::Test(items) = &tx.backend else {
            unreachable!()
        };
        assert_eq!(lock(items).len(), 1);
        assert_eq!(tx.len(), 4);
        assert_eq!(tx.panel_len(), 3);
        assert_eq!(tx.text_messages(), ["t"]);
        tx.set_gated(false);
        assert!(lock(&tx.gate).held.is_empty());
        assert_eq!(
            tx.projections(),
            vec![
                QueueProjection::from(&msg(false)),
                QueueProjection::Compact(None),
                QueueProjection::from(&msg(true)),
                QueueProjection::Compact(Some("last".into())),
            ]
        );
    }

    #[test]
    fn gated_clear_drops_held_work() {
        let tx = queue();
        tx.set_gated(true);
        tx.push(msg(false));
        tx.push(QueueItem::Compact {
            run_id: 1,
            instructions: None,
        });
        tx.push(msg(false));
        assert!(tx.remove(1));
        assert_eq!(tx.panel_entries()[1].text, "t");
        tx.clear();
        tx.set_gated(false);
        assert!(tx.is_empty());
    }
}
