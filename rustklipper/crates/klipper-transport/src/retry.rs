//! Klipper SerialRetryCommand retry timing and transaction lifecycle.
use std::time::Duration;

/// Add a retry delay without producing an infinite monotonic deadline.
fn saturating_deadline(now: f64, delay: Duration) -> f64 {
    let deadline = now + delay.as_secs_f64();
    if deadline.is_finite() { deadline } else { f64::MAX }
}

/// Initial attempt plus five retries at 10, 20, 40, 80, and 160 milliseconds.
#[derive(Debug, Clone)]
pub struct RetryPolicy { retries_remaining: u8, next_delay: Duration, completed: bool }
impl Default for RetryPolicy {
    fn default() -> Self {
        Self { retries_remaining: 5, next_delay: Duration::from_millis(10), completed: false }
    }
}
impl RetryPolicy {
    pub fn new(retries: u8, initial_delay: Duration) -> Self {
        Self { retries_remaining: retries, next_delay: initial_delay, completed: false }
    }
    pub fn complete(&mut self) { self.completed = true; }
    pub fn next_retry_delay(&mut self) -> Option<Duration> {
        if self.completed || self.retries_remaining == 0 { return None; }
        let delay = self.next_delay;
        self.retries_remaining -= 1;
        self.next_delay = self.next_delay.saturating_mul(2);
        Some(delay)
    }
    pub fn retries_remaining(&self) -> u8 { self.retries_remaining }
    pub fn is_complete(&self) -> bool { self.completed }
}

/// Outcome after the ACK for one application-level command attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckRetryDecision {
    /// No response was observed; wait this long before the next attempt.
    RetryAfter(Duration),
    /// All configured attempts have been used without a response.
    Exhausted,
    /// A matching response already completed the transaction.
    Complete,
}

/// State for one logical SerialRetryCommand transaction.
///
/// The transaction is separate from per-frame ACK notification IDs: callers
/// create a fresh notify ID for every transmission attempt, but retain this
/// state and the response waiter until a response arrives or retries exhaust.
/// Times are seconds on the session's monotonic clock.
#[derive(Debug, Clone)]
pub struct RetryTransaction {
    policy: RetryPolicy,
    retry_at: Option<f64>,
    complete: bool,
}
impl Default for RetryTransaction {
    fn default() -> Self {
        Self { policy: RetryPolicy::default(), retry_at: None, complete: false }
    }
}
impl RetryTransaction {
    pub fn new(retries: u8, initial_delay: Duration) -> Self {
        Self {
            policy: RetryPolicy::new(retries, initial_delay),
            retry_at: None,
            complete: false,
        }
    }

    /// Mark the logical response as received. Any scheduled retry is cancelled.
    pub fn response_received(&mut self) {
        self.complete = true;
        self.policy.complete();
        self.retry_at = None;
    }

    /// Called only after the current command attempt has been ACKed and no
    /// matching response has been routed, matching Klipper's ACK-then-check loop.
    pub fn attempt_acked(&mut self, now: f64) -> AckRetryDecision {
        if self.complete {
            return AckRetryDecision::Complete;
        }
        if let Some(delay) = self.policy.next_retry_delay() {
            self.retry_at = Some(saturating_deadline(now, delay));
            AckRetryDecision::RetryAfter(delay)
        } else {
            self.complete = true;
            AckRetryDecision::Exhausted
        }
    }

    /// Consume a due retry exactly once. The caller must enqueue a fresh frame
    /// with a fresh ACK notification ID and call attempt_acked after its ACK.
    pub fn take_due_retry(&mut self, now: f64) -> bool {
        if self.complete {
            return false;
        }
        match self.retry_at {
            Some(deadline) if now >= deadline => {
                self.retry_at = None;
                true
            }
            _ => false,
        }
    }

    pub fn is_complete(&self) -> bool { self.complete }
    pub fn retry_deadline(&self) -> Option<f64> { self.retry_at }
    pub fn retries_remaining(&self) -> u8 { self.policy.retries_remaining() }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn retry_deadline_stays_finite_near_f64_limit() {
        let mut transaction = RetryTransaction::new(1, Duration::from_secs(10));
        transaction.attempt_acked(f64::MAX - 1.0);
        assert_eq!(transaction.retry_deadline(), Some(f64::MAX));
        assert!(transaction.retry_deadline().unwrap().is_finite());
        // At this magnitude, subtracting one is rounded back to f64::MAX,
        // so the representable deadline is already due at the current time.
        assert!(transaction.take_due_retry(f64::MAX));
        assert_eq!(transaction.retry_deadline(), None);
    }

    #[test]
    fn matches_klipper_exponential_retry_schedule() {
        let mut policy = RetryPolicy::default();
        let delays: Vec<_> = std::iter::from_fn(|| policy.next_retry_delay()).collect();
        assert_eq!(delays, [10, 20, 40, 80, 160].map(Duration::from_millis));
        assert!(policy.next_retry_delay().is_none());
    }

    #[test]
    fn completed_transaction_never_retries() {
        let mut policy = RetryPolicy::default();
        policy.complete();
        assert!(policy.next_retry_delay().is_none());
    }

    #[test]
    fn transaction_starts_retry_delay_after_each_ack() {
        let mut transaction = RetryTransaction::default();
        assert_eq!(transaction.attempt_acked(1.0), AckRetryDecision::RetryAfter(Duration::from_millis(10)));
        assert!(!transaction.take_due_retry(1.009));
        assert!(transaction.take_due_retry(1.010));
        assert!(!transaction.take_due_retry(1.010));
        assert_eq!(transaction.attempt_acked(2.0), AckRetryDecision::RetryAfter(Duration::from_millis(20)));
        assert_eq!(transaction.retry_deadline(), Some(2.020));
    }

    #[test]
    fn response_cancels_waiting_retry() {
        let mut transaction = RetryTransaction::default();
        transaction.attempt_acked(3.0);
        transaction.response_received();
        assert!(transaction.is_complete());
        assert_eq!(transaction.retry_deadline(), None);
        assert!(!transaction.take_due_retry(100.0));
        assert_eq!(transaction.attempt_acked(100.0), AckRetryDecision::Complete);
    }

    #[test]
    fn retry_exhaustion_is_reported_after_five_retry_acknowledgements() {
        let mut transaction = RetryTransaction::default();
        for _ in 0..5 {
            assert!(matches!(transaction.attempt_acked(1.0), AckRetryDecision::RetryAfter(_)));
        }
        assert_eq!(transaction.retries_remaining(), 0);
        assert_eq!(transaction.attempt_acked(1.0), AckRetryDecision::Exhausted);
        assert!(transaction.is_complete());
    }
    #[test]
    fn response_before_first_ack_prevents_any_application_retry() {
        let mut transaction = RetryTransaction::default();
        transaction.response_received();

        assert_eq!(transaction.attempt_acked(1.0), AckRetryDecision::Complete);
        assert_eq!(transaction.retry_deadline(), None);
        assert!(!transaction.take_due_retry(100.0));
    }

    #[test]
    fn zero_retry_policy_exhausts_after_the_initial_attempt_ack() {
        let mut transaction = RetryTransaction::new(0, Duration::from_millis(10));

        assert_eq!(transaction.attempt_acked(4.0), AckRetryDecision::Exhausted);
        assert!(transaction.is_complete());
        assert_eq!(transaction.retries_remaining(), 0);
        assert_eq!(transaction.retry_deadline(), None);
        assert!(!transaction.take_due_retry(100.0));
    }

    #[test]
    fn response_during_retry_delay_cancels_the_due_attempt() {
        let mut transaction = RetryTransaction::default();
        assert_eq!(transaction.attempt_acked(5.0), AckRetryDecision::RetryAfter(Duration::from_millis(10)));

        transaction.response_received();

        assert!(!transaction.take_due_retry(5.010));
        assert_eq!(transaction.attempt_acked(5.010), AckRetryDecision::Complete);
    }

    #[test]
    fn consuming_a_due_retry_clears_its_deadline_exactly_once() {
        let mut transaction = RetryTransaction::default();
        assert_eq!(
            transaction.attempt_acked(10.0),
            AckRetryDecision::RetryAfter(Duration::from_millis(10)),
        );
        assert_eq!(transaction.retry_deadline(), Some(10.010));

        assert!(!transaction.take_due_retry(10.009));
        assert_eq!(transaction.retry_deadline(), Some(10.010));

        assert!(transaction.take_due_retry(10.010));
        assert_eq!(transaction.retry_deadline(), None);
        assert!(!transaction.take_due_retry(10.010));
    }

    #[test]
    fn each_ack_arms_only_the_next_exponential_retry_deadline() {
        let mut transaction = RetryTransaction::default();

        for (ack_time, delay_ms) in [
            (1.0, 10),
            (2.0, 20),
            (3.0, 40),
            (4.0, 80),
            (5.0, 160),
        ] {
            assert_eq!(
                transaction.attempt_acked(ack_time),
                AckRetryDecision::RetryAfter(Duration::from_millis(delay_ms)),
            );
            let deadline = ack_time + Duration::from_millis(delay_ms).as_secs_f64();
            assert_eq!(transaction.retry_deadline(), Some(deadline));
            assert!(transaction.take_due_retry(deadline));
            assert_eq!(transaction.retry_deadline(), None);
        }

        assert_eq!(transaction.attempt_acked(6.0), AckRetryDecision::Exhausted);
        assert_eq!(transaction.retry_deadline(), None);
    }

}
