//! Cooperative host-side event scheduler.
//!
//! The reactor is deliberately independent of MCU firmware. The embedding
//! host drives it by calling run_due(); callbacks execute serially and never
//! while the scheduler's internal state lock is held.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
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
    cancel_hooks: HashMap<TaskId, Box<dyn FnOnce() + Send + 'static>>,
    running: HashSet<TaskId>,
    cancelled: HashSet<TaskId>,
}

/// Allocate an ID that cannot replace a pending or currently executing task.
/// Wrapping is supported so a long-lived host cannot silently overwrite work
/// when the monotonically increasing counter reaches its integer limit.
fn allocate_task_id(state: &mut ReactorState) -> TaskId {
    loop {
        state.next_id = state.next_id.wrapping_add(1).max(1);
        let id = state.next_id;
        if !state.tasks.contains_key(&id)
            && !state.running.contains(&id)
            && !state.cancelled.contains(&id)
        {
            return id;
        }
    }
}


/// A thread-safe, single-assignment result used to observe asynchronous work.
///
/// Unlike Klipper's greenlet-aware completion, this primitive does not suspend
/// the reactor. Callers may poll it or wait from a separate host thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionCancelled;

pub struct Completion<T> {
    state: Arc<(Mutex<Option<Result<T, CompletionCancelled>>>, Condvar)>,
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
    fn lock_result(&self) -> MutexGuard<'_, Option<Result<T, CompletionCancelled>>> {
        self.state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn new() -> Self {
        Self::default()
    }

    /// Complete once. A second completion returns the unused result.
    pub fn complete(&self, result: T) -> Result<(), T> {
        let ready = &self.state.1;
        let mut current = self.lock_result();
        if current.is_some() {
            return Err(result);
        }
        *current = Some(Ok(result));
        ready.notify_all();
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.lock_result().is_some()
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(
            self.lock_result().as_ref(),
            Some(Err(_))
        )
    }

    /// Cancel a pending completion and wake all waiters.
    pub fn cancel(&self) -> bool {
        let ready = &self.state.1;
        let mut current = self.lock_result();
        if current.is_some() {
            return false;
        }
        *current = Some(Err(CompletionCancelled));
        ready.notify_all();
        true
    }

    /// Clone the result if it has already been produced.
    pub fn result(&self) -> Option<T>
    where
        T: Clone,
    {
        self.lock_result().as_ref()
            .and_then(|value| value.as_ref().ok())
            .cloned()
    }

    /// Wait for completion or cancellation.
    ///
    /// Do not call this on the reactor dispatch thread.
    pub fn wait(&self) -> Result<T, CompletionCancelled>
    where
        T: Clone,
    {
        let ready = &self.state.1;
        let mut current = self.lock_result();
        while current.is_none() {
            current = ready.wait(current).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        current.as_ref().expect("completion was checked").clone()
    }

    /// Wait up to the given duration; returns None if no result or cancellation arrives.
    pub fn wait_timeout(&self, timeout: Duration) -> Option<Result<T, CompletionCancelled>>
    where
        T: Clone,
    {
        let ready = &self.state.1;
        let current = self.lock_result();
        let (current, _) = ready
            .wait_timeout_while(current, timeout, |value| value.is_none())
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.clone()
    }
}

/// A lightweight, cooperative scheduler for host callbacks and timers.
#[derive(Default)]
pub struct Reactor {
    state: Mutex<ReactorState>,
    // Only one thread may dispatch callbacks at a time. try_lock in run_due
    // keeps nested or competing dispatch attempts from running callbacks in
    // parallel (or deadlocking when a callback re-enters the reactor).
    dispatching: Mutex<()>,
    wake: Condvar,
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
        let mut callback = callback;
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
        let cancelled_completion = completion.clone();
        let id = self.insert_task_with_cancel(
            deadline,
            move |eventtime| {
                let callback = callback.take().expect("one-shot callback invoked once");
                let result = callback(eventtime);
                let _ = callback_completion.complete(result);
                None
            },
            move || {
                cancelled_completion.cancel();
            },
        );
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
        callback: F,
    ) -> Result<TaskId, ReactorError>
    where
        F: FnMut() + Send + 'static,
    {
        if interval.is_some_and(|value| value.is_zero()) {
            return Err(ReactorError::ZeroInterval);
        }
        let deadline = Instant::now()
            .checked_add(delay)
            .ok_or(ReactorError::DeadlineOutOfRange)?;
        let next_interval = interval;
        let mut callback = callback;
        Ok(self.insert_task(deadline, move |eventtime| {
            callback();
            next_interval.and_then(|value| eventtime.checked_add(value))
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
        self.wake.notify_one();
        true
    }

    fn insert_task<F>(&self, deadline: Instant, callback: F) -> TaskId
    where
        F: FnMut(Instant) -> Option<Instant> + Send + 'static,
    {
        self.insert_task_with_cancel(deadline, callback, || {})
    }

    fn insert_task_with_cancel<F, C>(
        &self,
        deadline: Instant,
        callback: F,
        on_cancel: C,
    ) -> TaskId
    where
        F: FnMut(Instant) -> Option<Instant> + Send + 'static,
        C: FnOnce() + Send + 'static,
    {
        let mut state = self.state.lock().expect("reactor lock poisoned");
        let id = allocate_task_id(&mut state);
        state.tasks.insert(
            id,
            Task {
                callback: Box::new(callback),
                deadline,
            },
        );
        state.queue.push(Reverse(Scheduled { deadline, id }));
        state.cancel_hooks.insert(id, Box::new(on_cancel));
        self.wake.notify_one();
        id
    }

    /// Cancel a pending callback or timer. Returns false for an unknown task.
    pub fn unregister(&self, id: TaskId) -> bool {
        let on_cancel = {
            let mut state = self.state.lock().expect("reactor lock poisoned");
            if state.tasks.remove(&id).is_some() {
                state.cancel_hooks.remove(&id)
            } else if state.running.contains(&id) {
                state.cancelled.insert(id);
                // A running callback cannot be stopped, but its cancellation
                // hook must still run so completion waiters are notified and
                // any late callback result is rejected.
                state.cancel_hooks.remove(&id)
            } else {
                return false;
            }
        };
        if let Some(on_cancel) = on_cancel {
            on_cancel();
        }
        self.wake.notify_one();
        true
    }

    /// Queue a callback from any thread and wake a thread blocked in run_once().
    pub fn register_async_callback<F>(&self, callback: F) -> TaskId
    where
        F: FnOnce(Instant) + Send + 'static,
    {
        self.register_callback_at(Instant::now(), callback)
    }

    /// Wait for work or the next timer deadline, then dispatch due callbacks.
    ///
    /// Returns immediately when work is already due. The timeout bounds idle
    /// waiting; registering or updating a task from another thread wakes this
    /// method so the new deadline can be observed.
    pub fn run_once(&self, max_wait: Duration) -> usize {
        let started = Instant::now();
        loop {
            let mut state = self.state.lock().expect("reactor lock poisoned");
            let next_deadline = loop {
                let Some(Reverse(entry)) = state.queue.peek().copied() else {
                    break None;
                };
                if state
                    .tasks
                    .get(&entry.id)
                    .is_some_and(|task| task.deadline == entry.deadline)
                {
                    break Some(entry.deadline);
                }
                state.queue.pop();
            };

            let now = Instant::now();
            let elapsed = now.saturating_duration_since(started);
            let remaining = max_wait.saturating_sub(elapsed);
            if next_deadline.is_some_and(|deadline| deadline <= now)
                || remaining.is_zero()
            {
                drop(state);
                return self.run_due(Instant::now());
            }

            let wait = next_deadline
                .map(|deadline| deadline.saturating_duration_since(now).min(remaining))
                .unwrap_or(remaining);
            let (guard, _) = self
                .wake
                .wait_timeout(state, wait)
                .expect("reactor lock poisoned");
            drop(guard);
        }
    }

    /// Execute all tasks due at the supplied monotonic time.
    ///
    /// Timer callbacks receive the current dispatch time and choose their next
    /// deadline. This matches Klipper's event-time based timer semantics.
    pub fn run_due(&self, now: Instant) -> usize {
        let _dispatch_guard = match self.dispatching.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return 0,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
        };
        let mut executed = 0;
        let mut executed_ids = HashSet::new();
        let mut deferred = Vec::new();
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
                // A timer may choose an already-due deadline. Run each task
                // at most once per cycle, deferring repeats so other due work
                // is not starved and the reactor cannot spin forever.
                if executed_ids.contains(&entry.id) {
                    deferred.push(entry);
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
            let callback_result = catch_unwind(AssertUnwindSafe(|| (task.callback)(now)));
            executed += 1;
            executed_ids.insert(entry.id);

            let mut state = self.state.lock().expect("reactor lock poisoned");
            state.running.remove(&entry.id);
            let cancelled = state.cancelled.remove(&entry.id);
            let cancel_hook = state.cancel_hooks.remove(&entry.id);

            let next_deadline = match callback_result {
                Ok(deadline) => deadline,
                Err(payload) => {
                    // A panic aborts this dispatch cycle. Restore tasks that
                    // were temporarily removed from the heap to enforce the
                    // once-per-cycle limit; otherwise their task records
                    // would remain pending but become unreachable by deadline.
                    for deferred_entry in deferred.drain(..) {
                        if state
                            .tasks
                            .get(&deferred_entry.id)
                            .is_some_and(|task| task.deadline == deferred_entry.deadline)
                        {
                            state.queue.push(Reverse(deferred_entry));
                        }
                    }
                    drop(state);
                    if let Some(cancel_hook) = cancel_hook {
                        cancel_hook();
                    }
                    resume_unwind(payload);
                }
            };

            if !cancelled {
                if let Some(deadline) = next_deadline {
                    task.deadline = deadline;
                    state.tasks.insert(entry.id, task);
                    state.queue.push(Reverse(Scheduled {
                        deadline,
                        id: entry.id,
                    }));
                    self.wake.notify_one();
                }
            }
        }
        if !deferred.is_empty() {
            let mut state = self.state.lock().expect("reactor lock poisoned");
            for entry in deferred {
                if state
                    .tasks
                    .get(&entry.id)
                    .is_some_and(|task| task.deadline == entry.deadline)
                {
                    state.queue.push(Reverse(entry));
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
    #[error("timer deadline is outside the supported monotonic clock range")]
    DeadlineOutOfRange,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn timer_rejects_delay_outside_instant_range() {
        let reactor = Reactor::new();
        let result = reactor.register_timer(Duration::MAX, None, || {});
        assert_eq!(result, Err(ReactorError::DeadlineOutOfRange));
        assert_eq!(reactor.pending_tasks(), 0);
    }

    #[test]
    fn periodic_timer_stops_if_next_deadline_is_out_of_range() {
        let reactor = Reactor::new();
        let start = Instant::now();
        let id = reactor.register_timer_at(start, move |_| {
            start.checked_add(Duration::MAX)
        });
        assert_eq!(reactor.run_due(start), 1);
        assert_eq!(reactor.pending_tasks(), 0);
        assert!(!reactor.unregister(id));
    }

    #[test]
    fn task_id_wrap_skips_pending_running_and_cancelled_ids() {
        let mut state = ReactorState {
            next_id: u64::MAX - 1,
            queue: BinaryHeap::new(),
            tasks: HashMap::new(),
            cancel_hooks: HashMap::new(),
            running: HashSet::from([1]),
            cancelled: HashSet::from([2]),
        };
        // Occupy u64::MAX without needing to construct a callback task.
        state.tasks.insert(
            u64::MAX,
            Task {
                callback: Box::new(|_| None),
                deadline: Instant::now(),
            },
        );

        assert_eq!(allocate_task_id(&mut state), 3);
        assert_eq!(state.next_id, 3);
    }

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
    fn concurrent_dispatch_does_not_run_callbacks_in_parallel() {
        use std::sync::mpsc;

        let reactor = Arc::new(Reactor::new());
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let second_count = Arc::new(AtomicUsize::new(0));
        let observed_second = second_count.clone();
        let deadline = Instant::now();

        reactor.register_callback_at(deadline, move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        reactor.register_callback_at(deadline, move |_| {
            observed_second.fetch_add(1, Ordering::SeqCst);
        });

        let first_reactor = reactor.clone();
        let first = std::thread::spawn(move || {
            first_reactor.run_due(deadline + Duration::from_secs(1))
        });
        started_rx.recv().unwrap();

        // A competing dispatch attempt must not execute the second callback
        // while the first callback is still running.
        assert_eq!(
            reactor.run_due(deadline + Duration::from_secs(1)),
            0
        );
        assert_eq!(second_count.load(Ordering::SeqCst), 0);

        release_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap(), 2);
        assert_eq!(second_count.load(Ordering::SeqCst), 1);
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
    fn overdue_rescheduling_timer_runs_once_per_dispatch_cycle() {
        let reactor = Reactor::new();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let start = Instant::now();
        reactor.register_timer_at(start, move |eventtime| {
            observed.fetch_add(1, Ordering::SeqCst);
            Some(eventtime)
        });
        let dispatch_time = start + Duration::from_secs(1);

        assert_eq!(reactor.run_due(dispatch_time), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(reactor.pending_tasks(), 1);
        assert_eq!(reactor.next_deadline(), Some(dispatch_time));

        assert_eq!(reactor.run_due(dispatch_time), 1);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(reactor.pending_tasks(), 1);
    }

    #[test]
    fn panic_restores_deferred_due_timer_entries() {
        let reactor = Reactor::new();
        let timer_count = Arc::new(AtomicUsize::new(0));
        let observed_timer = timer_count.clone();
        let start = Instant::now();

        reactor.register_timer_at(start, move |eventtime| {
            if observed_timer.fetch_add(1, Ordering::SeqCst) == 0 {
                Some(eventtime)
            } else {
                None
            }
        });
        reactor.register_callback_at(start, |_| panic!("stop dispatch cycle"));

        let dispatch_time = start + Duration::from_secs(1);
        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            reactor.run_due(dispatch_time);
        }));
        assert!(panic.is_err());

        // The timer's repeated entry was deferred before the later callback
        // panicked. It must remain reachable for the next dispatch cycle.
        assert_eq!(reactor.pending_tasks(), 1);
        assert_eq!(reactor.next_deadline(), Some(dispatch_time));
        assert_eq!(reactor.run_due(dispatch_time), 1);
        assert_eq!(timer_count.load(Ordering::SeqCst), 2);
        assert_eq!(reactor.pending_tasks(), 0);
    }

    #[test]
    fn overdue_rescheduling_timer_does_not_starve_other_due_callbacks() {
        let reactor = Reactor::new();
        let timer_count = Arc::new(AtomicUsize::new(0));
        let observed_timer = timer_count.clone();
        let callback_count = Arc::new(AtomicUsize::new(0));
        let observed_callback = callback_count.clone();
        let start = Instant::now();

        reactor.register_timer_at(start, move |eventtime| {
            observed_timer.fetch_add(1, Ordering::SeqCst);
            Some(eventtime)
        });
        reactor.register_callback_at(start, move |_| {
            observed_callback.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(reactor.run_due(start + Duration::from_secs(1)), 2);
        assert_eq!(timer_count.load(Ordering::SeqCst), 1);
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert_eq!(reactor.pending_tasks(), 1);
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
    fn run_once_accepts_maximum_wait_without_instant_overflow() {
        let reactor = Reactor::new();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor.register_callback(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(reactor.run_once(Duration::MAX), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn run_once_returns_after_idle_timeout() {
        let reactor = Reactor::new();
        let start = Instant::now();
        assert_eq!(reactor.run_once(Duration::from_millis(5)), 0);
        assert!(start.elapsed() >= Duration::from_millis(4));
    }

    #[test]
    fn run_once_waits_for_timer_deadline() {
        let reactor = Reactor::new();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor
            .register_timer(Duration::from_millis(5), None, move || {
                observed.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        assert_eq!(reactor.run_once(Duration::from_secs(1)), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn async_callback_wakes_blocked_reactor() {
        let reactor = Arc::new(Reactor::new());
        let dispatch_reactor = reactor.clone();
        let dispatcher = std::thread::spawn(move || {
            dispatch_reactor.run_once(Duration::from_secs(2))
        });
        std::thread::sleep(Duration::from_millis(10));
        let completed = Arc::new(AtomicUsize::new(0));
        let observed = completed.clone();
        reactor.register_async_callback(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(dispatcher.join().unwrap(), 1);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
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
        assert_eq!(completion.wait_timeout(Duration::from_millis(1)), Some(Ok(42)));
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
    fn cancelling_callback_cancels_completion_and_wakes_waiters() {
        let reactor = Reactor::new();
        let (id, completion) = reactor.register_callback_with_completion(|_| 99usize);
        assert!(reactor.unregister(id));
        assert!(completion.is_complete());
        assert!(completion.is_cancelled());
        assert_eq!(completion.wait(), Err(CompletionCancelled));
        assert_eq!(completion.result(), None);
        assert!(!reactor.unregister(id));
    }

    #[test]
    fn cancelling_running_callback_cancels_completion_before_it_returns() {
        use std::sync::mpsc;
        use std::sync::Arc;

        let reactor = Arc::new(Reactor::new());
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (id, completion) = reactor.register_callback_with_completion(move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            123usize
        });

        let dispatch_reactor = reactor.clone();
        let dispatch = std::thread::spawn(move || {
            dispatch_reactor.run_due(Instant::now() + Duration::from_secs(1))
        });
        started_rx.recv().unwrap();

        assert!(reactor.unregister(id));
        assert!(completion.is_complete());
        assert!(completion.is_cancelled());
        assert_eq!(completion.wait(), Err(CompletionCancelled));

        release_tx.send(()).unwrap();
        assert_eq!(dispatch.join().unwrap(), 1);
        assert_eq!(completion.result(), None);
        assert!(!reactor.unregister(id));
    }

    #[test]
    fn completion_recovers_after_result_lock_poisoning() {
        let completion = Completion::<u8>::new();
        let poisoned = completion.clone();
        let panic = std::thread::spawn(move || {
            let _guard = poisoned.state.0.lock().unwrap();
            panic!("poison completion result lock");
        })
        .join();
        assert!(panic.is_err());

        // The condition-variable wait path must also recover the poisoned
        // mutex when it reacquires the guard.
        assert_eq!(
            completion.wait_timeout(Duration::from_millis(1)),
            None
        );

        assert_eq!(completion.complete(7), Ok(()));
        assert!(completion.is_complete());
        assert!(!completion.is_cancelled());
        assert_eq!(completion.result(), Some(7));
        assert_eq!(completion.wait(), Ok(7));
        assert_eq!(completion.wait_timeout(Duration::from_millis(1)), Some(Ok(7)));
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
    fn cancelled_completion_rejects_late_result() {
        let completion = Completion::<u32>::new();
        assert!(completion.cancel());
        assert!(!completion.cancel());
        assert_eq!(completion.complete(5), Err(5));
        assert_eq!(
            completion.wait_timeout(Duration::from_millis(1)),
            Some(Err(CompletionCancelled))
        );
    }

    #[test]
    fn completion_is_single_assignment_and_wakes_waiters() {
        let completion = Completion::new();
        let waiter = completion.clone();
        let thread = std::thread::spawn(move || waiter.wait());
        assert_eq!(completion.complete(String::from("ready")), Ok(()));
        assert_eq!(thread.join().unwrap(), Ok("ready".to_string()));
        assert_eq!(
            completion.complete(String::from("duplicate")),
            Err(String::from("duplicate"))
        );
    }

    #[test]
    fn panicking_completion_callback_cancels_waiters_and_cleans_task() {
        let reactor = Reactor::new();
        let (id, completion) =
            reactor.register_callback_with_completion::<_, ()>(|_| panic!("callback failed"));

        let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            reactor.run_due(Instant::now() + Duration::from_secs(1));
        }));
        assert!(panic.is_err());
        assert!(completion.is_complete());
        assert!(completion.is_cancelled());
        assert_eq!(completion.wait(), Err(CompletionCancelled));
        assert_eq!(reactor.pending_tasks(), 0);
        assert!(!reactor.unregister(id));

        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        reactor.register_callback(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        });
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
