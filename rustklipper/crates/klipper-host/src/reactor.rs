//! Cooperative host-side event scheduler.
//!
//! The reactor is deliberately independent of MCU firmware. The embedding
//! host drives it by calling run_due(); callbacks execute serially and never
//! while the scheduler's internal state lock is held.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub type TaskId = u64;
type Callback = Box<dyn FnMut(Instant) -> Option<Instant> + Send + 'static>;

struct Task {
    callback: Callback,
    deadline: Instant,
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


/// A thread-safe, single-assignment result used to observe asynchronous work.
///
/// Unlike Klipper's greenlet-aware completion, this primitive does not suspend
/// the reactor. Callers may poll it or wait from a separate host thread.
pub struct Completion<T> {
    state: Arc<(Mutex<Option<T>>, Condvar)>,
}

impl<T> Clone for Completion<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Default for Completion<T> {
    fn default() -> Self {
        Self {
            state: Arc::new((Mutex::new(None), Condvar::new())),
        }
    }
}

impl<T> Completion<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Complete once. A second completion returns the unused result.
    pub fn complete(&self, result: T) -> Result<(), T> {
        let (lock, ready) = &*self.state;
        let mut current = lock.lock().expect("completion lock poisoned");
        if current.is_some() {
            return Err(result);
        }
        *current = Some(result);
        ready.notify_all();
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.state.0.lock().expect("completion lock poisoned").is_some()
    }

    /// Clone the result if it has already been produced.
    pub fn result(&self) -> Option<T>
    where
        T: Clone,
    {
        self.state.0.lock().expect("completion lock poisoned").clone()
    }

    /// Wait for completion. Do not call this on the reactor dispatch thread.
    pub fn wait(&self) -> T
    where
        T: Clone,
    {
        let (lock, ready) = &*self.state;
        let mut current = lock.lock().expect("completion lock poisoned");
        while current.is_none() {
            current = ready.wait(current).expect("completion lock poisoned");
        }
        current.as_ref().expect("completion was checked").clone()
    }

    /// Wait up to the given duration; returns None if no result arrives in time.
    pub fn wait_timeout(&self, timeout: Duration) -> Option<T>
    where
        T: Clone,
    {
        let (lock, ready) = &*self.state;
        let current = lock.lock().expect("completion lock poisoned");
        let (current, _) = ready
            .wait_timeout_while(current, timeout, |value| value.is_none())
            .expect("completion lock poisoned");
        current.clone()
    }
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
    pub fn register_callback<F>(&self, mut callback: F) -> TaskId
    where
        F: FnMut() + Send + 'static,
    {
        self.insert_task(Instant::now(), move |_| {
            callback();
            None
        })
    }


    /// Queue a one-shot callback for an explicit monotonic deadline.
    pub fn register_callback_at<F>(&self, deadline: Instant, callback: F) -> TaskId
    where
        F: FnOnce(Instant) + Send + 'static,
    {
        let mut callback = Some(callback);
        self.insert_task(deadline, move |eventtime| {
            callback
                .take()
                .expect("one-shot callback invoked once")(eventtime);
            None
        })
    }

    /// Queue a one-shot callback and return a handle for its result.
    /// The callback runs on the next due dispatch.
    pub fn register_callback_with_completion<F, T>(
        &self,
        callback: F,
    ) -> (TaskId, Completion<T>)
    where
        F: FnOnce(Instant) -> T + Send + 'static,
        T: Send + 'static,
    {
        self.register_callback_with_completion_at(Instant::now(), callback)
    }

    /// Queue a one-shot callback at a specific deadline and observe its result.
    pub fn register_callback_with_completion_at<F, T>(
        &self,
        deadline: Instant,
        callback: F,
    ) -> (TaskId, Completion<T>)
    where
        F: FnOnce(Instant) -> T + Send + 'static,
        T: Send + 'static,
    {
        let completion = Completion::new();
        let callback_completion = completion.clone();
        let mut callback = Some(callback);
        let id = self.insert_task(deadline, move |eventtime| {
            let callback = callback.take().expect("one-shot callback invoked once");
            let result = callback(eventtime);
            let _ = callback_completion.complete(result);
            None
        });
        (id, completion)
    }

    /// Register a one-shot or periodic timer.
    ///
    /// A zero interval is rejected to prevent a timer from continuously
    /// rescheduling at the same deadline.
    pub fn register_timer<F>(
        &self,
        delay: Duration,
        interval: Option<Duration>,
        mut callback: F,
    ) -> Result<TaskId, ReactorError>
    where
        F: FnMut() + Send + 'static,
    {
        if interval.is_some_and(|value| value.is_zero()) {
            return Err(ReactorError::ZeroInterval);
        }
        let deadline = Instant::now() + delay;
        let next_interval = interval;
        Ok(self.insert_task(deadline, move |eventtime| {
            callback();
            next_interval.map(|value| eventtime + value)
        }))
    }

    /// Register a Klipper-style timer whose callback chooses its next deadline.
    /// Returning `None` unregisters the timer; returning a deadline reschedules it.
    pub fn register_timer_at<F>(&self, deadline: Instant, callback: F) -> TaskId
    where
        F: FnMut(Instant) -> Option<Instant> + Send + 'static,
    {
        self.insert_task(deadline, callback)
    }

    /// Update a pending timer deadline. Running timers cannot be updated.
    /// Returns false when the timer is unknown or currently executing.
    pub fn update_timer(&self, id: TaskId, deadline: Instant) -> bool {
        let mut state = self.state.lock().expect("reactor lock poisoned");
        if state.running.contains(&id) {
            return false;
        }
        let Some(task) = state.tasks.get_mut(&id) else {
            return false;
        };
        task.deadline = deadline;
        state.queue.push(Reverse(Scheduled { deadline, id }));
        true
    }

    fn insert_task<F>(&self, deadline: Instant, callback: F) -> TaskId
    where
        F: FnMut(Instant) -> Option<Instant> + Send + 'static,
    {
        let mut state = self.state.lock().expect("reactor lock poisoned");
        state.next_id = state.next_id.wrapping_add(1).max(1);
        let id = state.next_id;
        state.tasks.insert(
            id,
            Task {
                callback: Box::new(callback),
                deadline,
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
    /// Timer callbacks receive the current dispatch time and choose their next
    /// deadline. This matches Klipper's event-time based timer semantics.
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
                let Some(task) = state.tasks.get(&entry.id) else {
                    continue;
                };
                if task.deadline != entry.deadline {
                    continue;
                }
                let task = state
                    .tasks
                    .remove(&entry.id)
                    .expect("task was just checked");
                state.running.insert(entry.id);
                (entry, task)
            };

            let (entry, mut task) = next;
            let next_deadline = (task.callback)(now);
            executed += 1;

            let mut state = self.state.lock().expect("reactor lock poisoned");
            state.running.remove(&entry.id);
            let cancelled = state.cancelled.remove(&entry.id);
            if !cancelled {
                if let Some(deadline) = next_deadline {
                    task.deadline = deadline;
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
            if state
                .tasks
                .get(&entry.id)
                .is_some_and(|task| task.deadline == entry.deadline)
            {
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
    fn periodic_timer_reschedules_from_current_dispatch_time() {
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
        assert_eq!(reactor.run_due(now), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(reactor.next_deadline(), Some(now + Duration::from_secs(1)));
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

        assert_eq!(reactor.run_due(Instant::now() + Duration::from_secs(1)), 2);
        assert_eq!(reactor.run_due(Instant::now() + Duration::from_secs(1)), 0);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }


    #[test]
    fn timer_callback_can_choose_its_next_deadline() {
        let reactor = Reactor::new();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let start = Instant::now();
        reactor.register_timer_at(start, move |eventtime| {
            let current = observed.fetch_add(1, Ordering::SeqCst);
            if current == 0 { Some(eventtime + Duration::from_secs(5)) } else { None }
        });
        let first_dispatch = start + Duration::from_secs(1);
        assert_eq!(reactor.run_due(first_dispatch), 1);
        assert_eq!(reactor.pending_tasks(), 1);
        assert_eq!(reactor.run_due(start + Duration::from_secs(5)), 0);
        assert_eq!(reactor.run_due(start + Duration::from_secs(7)), 1);
        assert_eq!(reactor.pending_tasks(), 0);
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn update_timer_replaces_the_pending_deadline() {
        let reactor = Reactor::new();
        let start = Instant::now();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let id = reactor.register_timer_at(start + Duration::from_secs(30), move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            None
        });
        assert!(reactor.update_timer(id, start + Duration::from_secs(2)));
        assert_eq!(reactor.next_deadline(), Some(start + Duration::from_secs(2)));
        assert_eq!(reactor.run_due(start + Duration::from_secs(3)), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(!reactor.update_timer(id, start + Duration::from_secs(4)));
    }



    #[test]
    fn callback_at_waits_for_its_requested_deadline() {
        let reactor = Reactor::new();
        let start = Instant::now();
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor.register_callback_at(start + Duration::from_secs(5), move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(reactor.run_due(start + Duration::from_secs(4)), 0);
        assert_eq!(reactor.run_due(start + Duration::from_secs(6)), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn callback_completion_publishes_result() {
        let reactor = Reactor::new();
        let (id, completion) = reactor.register_callback_with_completion(|_| 42usize);
        assert!(!completion.is_complete());
        assert_eq!(completion.result(), None);
        assert_eq!(reactor.run_due(Instant::now() + Duration::from_secs(1)), 1);
        assert!(completion.is_complete());
        assert_eq!(completion.result(), Some(42));
        assert_eq!(completion.wait_timeout(Duration::from_millis(1)), Some(42));
        assert!(!reactor.unregister(id));
    }


    #[test]
    fn callback_completion_at_respects_requested_deadline() {
        let reactor = Reactor::new();
        let start = Instant::now();
        let (id, completion) = reactor.register_callback_with_completion_at(
            start + Duration::from_secs(4),
            |_| String::from("finished"),
        );
        assert_eq!(reactor.run_due(start + Duration::from_secs(3)), 0);
        assert!(!completion.is_complete());
        assert_eq!(reactor.run_due(start + Duration::from_secs(5)), 1);
        assert_eq!(completion.result().as_deref(), Some("finished"));
        assert!(!reactor.unregister(id));
    }

    #[test]
    fn completion_timeout_returns_none_without_consuming_state() {
        let completion = Completion::<u8>::new();
        assert_eq!(
            completion.wait_timeout(Duration::from_millis(1)),
            None
        );
        assert!(!completion.is_complete());
        assert_eq!(completion.complete(7), Ok(()));
        assert_eq!(completion.result(), Some(7));
    }

    #[test]
    fn completion_is_single_assignment_and_wakes_waiters() {
        let completion = Completion::new();
        let waiter = completion.clone();
        let thread = std::thread::spawn(move || waiter.wait());
        assert_eq!(completion.complete(String::from("ready")), Ok(()));
        assert_eq!(thread.join().unwrap(), "ready");
        assert_eq!(
            completion.complete(String::from("duplicate")),
            Err(String::from("duplicate"))
        );
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
