//! Cooperative host-side event scheduler.
//!
//! The reactor is deliberately independent of MCU firmware. The embedding
//! host drives it by calling run_due(); callbacks execute serially and never
//! while the scheduler's internal state lock is held.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub type TaskId = u64;
type Callback = Box<dyn FnMut() + Send + 'static>;

struct Task {
    callback: Callback,
    interval: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Scheduled {
    deadline: Instant,
    id: TaskId,
}

impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct ReactorState {
    next_id: TaskId,
    queue: BinaryHeap<Reverse<Scheduled>>,
    tasks: HashMap<TaskId, Task>,
    running: HashSet<TaskId>,
    cancelled: HashSet<TaskId>,
}

/// A lightweight, cooperative scheduler for host callbacks and timers.
#[derive(Default)]
pub struct Reactor {
    state: Mutex<ReactorState>,
}

impl Reactor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a callback for the next run_due() call.
    pub fn register_callback<F>(&self, callback: F) -> TaskId
    where
        F: FnMut() + Send + 'static,
    {
        self.insert_task(Instant::now(), None, callback)
    }

    /// Register a one-shot or periodic timer.
    ///
    /// A zero interval is rejected to prevent a timer from continuously
    /// rescheduling at the same deadline.
    pub fn register_timer<F>(
        &self,
        delay: Duration,
        interval: Option<Duration>,
        callback: F,
    ) -> Result<TaskId, ReactorError>
    where
        F: FnMut() + Send + 'static,
    {
        if interval.is_some_and(|value| value.is_zero()) {
            return Err(ReactorError::ZeroInterval);
        }
        Ok(self.insert_task(Instant::now() + delay, interval, callback))
    }

    fn insert_task<F>(
        &self,
        deadline: Instant,
        interval: Option<Duration>,
        callback: F,
    ) -> TaskId
    where
        F: FnMut() + Send + 'static,
    {
        let mut state = self.state.lock().expect("reactor lock poisoned");
        state.next_id = state.next_id.wrapping_add(1).max(1);
        let id = state.next_id;
        state.tasks.insert(
            id,
            Task {
                callback: Box::new(callback),
                interval,
            },
        );
        state.queue.push(Reverse(Scheduled { deadline, id }));
        id
    }

    /// Cancel a pending callback or timer. Returns false for an unknown task.
    pub fn unregister(&self, id: TaskId) -> bool {
        let mut state = self.state.lock().expect("reactor lock poisoned");
        if state.tasks.remove(&id).is_some() {
            return true;
        }
        if state.running.contains(&id) {
            state.cancelled.insert(id);
            return true;
        }
        false
    }

    /// Execute all tasks due at the supplied monotonic time.
    ///
    /// Periodic timers are rescheduled relative to their previous deadline,
    /// avoiding drift caused by callback execution time.
    pub fn run_due(&self, now: Instant) -> usize {
        let mut executed = 0;
        loop {
            let next = {
                let mut state = self.state.lock().expect("reactor lock poisoned");
                let Some(Reverse(entry)) = state.queue.peek().copied() else {
                    break;
                };
                if entry.deadline > now {
                    break;
                }
                state.queue.pop();
                let Some(task) = state.tasks.remove(&entry.id) else {
                    continue;
                };
                state.running.insert(entry.id);
                (entry, task)
            };

            let (entry, mut task) = next;
            (task.callback)();
            executed += 1;

            let mut state = self.state.lock().expect("reactor lock poisoned");
            state.running.remove(&entry.id);
            let cancelled = state.cancelled.remove(&entry.id);
            if let Some(interval) = task.interval {
                if !cancelled {
                    let deadline = entry.deadline + interval;
                    state.tasks.insert(entry.id, task);
                    state.queue.push(Reverse(Scheduled {
                        deadline,
                        id: entry.id,
                    }));
                }
            }
        }
        executed
    }

    /// Return the earliest pending deadline, discarding cancelled queue entries.
    pub fn next_deadline(&self) -> Option<Instant> {
        let mut state = self.state.lock().expect("reactor lock poisoned");
        loop {
            let Reverse(entry) = state.queue.peek().copied()?;
            if state.tasks.contains_key(&entry.id) {
                return Some(entry.deadline);
            }
            state.queue.pop();
        }
    }

    /// Return the duration until the next task is due, if any.
    pub fn time_until_next(&self, now: Instant) -> Option<Duration> {
        self.next_deadline()
            .map(|deadline| deadline.saturating_duration_since(now))
    }

    pub fn pending_tasks(&self) -> usize {
        self.state
            .lock()
            .expect("reactor lock poisoned")
            .tasks
            .len()
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ReactorError {
    #[error("timer interval must be greater than zero")]
    ZeroInterval,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn callback_runs_once_and_is_removed() {
        let reactor = Reactor::new();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor.register_callback(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(reactor.run_due(Instant::now() + Duration::from_secs(1)), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(reactor.pending_tasks(), 0);
    }


    #[test]
    fn callbacks_with_the_same_deadline_run_in_registration_order() {
        let reactor = Reactor::new();
        let order = std::sync::Arc::new(Mutex::new(Vec::new()));
        for value in 0..3 {
            let order = order.clone();
            reactor.register_callback(move || {
                order.lock().unwrap().push(value);
            });
        }

        reactor.run_due(Instant::now() + Duration::from_secs(1));
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn delayed_timer_waits_until_its_deadline() {
        let reactor = Reactor::new();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor
            .register_timer(Duration::from_secs(10), None, move || {
                observed.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();

        assert_eq!(reactor.run_due(Instant::now()), 0);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(
            reactor.run_due(Instant::now() + Duration::from_secs(11)),
            1
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn periodic_timer_catches_up_from_previous_deadline() {
        let reactor = Reactor::new();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor
            .register_timer(
                Duration::ZERO,
                Some(Duration::from_secs(1)),
                move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                },
            )
            .unwrap();

        let now = Instant::now() + Duration::from_secs(3);
        assert_eq!(reactor.run_due(now), 4);
        assert_eq!(count.load(Ordering::SeqCst), 4);
        assert_eq!(reactor.pending_tasks(), 1);
    }

    #[test]
    fn cancelled_timer_never_runs() {
        let reactor = Reactor::new();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let id = reactor
            .register_timer(Duration::ZERO, Some(Duration::from_secs(1)), move || {
                observed.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();

        assert!(reactor.unregister(id));
        assert!(!reactor.unregister(id));
        assert_eq!(reactor.run_due(Instant::now() + Duration::from_secs(2)), 0);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }


    #[test]
    fn periodic_timer_can_cancel_itself_during_callback() {
        let reactor = std::sync::Arc::new(Reactor::new());
        let id_slot = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed_reactor = reactor.clone();
        let observed_id = id_slot.clone();
        let observed_count = count.clone();
        let id = reactor
            .register_timer(Duration::ZERO, Some(Duration::from_secs(1)), move || {
                observed_count.fetch_add(1, Ordering::SeqCst);
                let id = observed_id.load(Ordering::SeqCst);
                assert!(observed_reactor.unregister(id));
            })
            .unwrap();
        id_slot.store(id, Ordering::SeqCst);

        assert_eq!(
            reactor.run_due(Instant::now() + Duration::from_secs(2)),
            1
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(reactor.pending_tasks(), 0);
    }


    #[test]
    fn next_deadline_tracks_pending_tasks_and_ignores_cancelled_entries() {
        let reactor = Reactor::new();
        let id = reactor
            .register_timer(Duration::from_secs(30), None, || {})
            .unwrap();
        assert!(reactor.next_deadline().is_some());
        assert!(reactor.unregister(id));
        assert_eq!(reactor.next_deadline(), None);
        assert_eq!(reactor.time_until_next(Instant::now()), None);
    }

    #[test]
    fn callback_can_schedule_another_callback_without_locking_the_reactor() {
        let reactor = std::sync::Arc::new(Reactor::new());
        let nested = reactor.clone();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor.register_callback(move || {
            let observed = observed.clone();
            nested.register_callback(move || {
                observed.fetch_add(1, Ordering::SeqCst);
            });
        });

        assert_eq!(reactor.run_due(Instant::now() + Duration::from_secs(1)), 1);
        assert_eq!(reactor.run_due(Instant::now() + Duration::from_secs(1)), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn zero_periodic_interval_is_rejected() {
        let reactor = Reactor::new();
        let result = reactor.register_timer(
            Duration::ZERO,
            Some(Duration::ZERO),
            || {},
        );
        assert_eq!(result, Err(ReactorError::ZeroInterval));
    }
}
