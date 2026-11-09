//! Cooperative host-side event scheduler.
//!
//! The reactor is deliberately independent of MCU firmware. The embedding
//! host drives it by calling run_due(); callbacks execute serially and never
//! while the scheduler's internal state lock is held.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
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
        self.state
            .lock()
            .expect("reactor lock poisoned")
            .tasks
            .remove(&id)
            .is_some()
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
                (entry, task)
            };

            let (entry, mut task) = next;
            (task.callback)();
            executed += 1;

            if let Some(interval) = task.interval {
                let deadline = entry.deadline + interval;
                let mut state = self.state.lock().expect("reactor lock poisoned");
                state.tasks.insert(entry.id, task);
                state.queue.push(Reverse(Scheduled {
                    deadline,
                    id: entry.id,
                }));
            }
        }
        executed
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
