//! Klipper SerialRetryCommand retry timing policy.
use std::time::Duration;

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
#[cfg(test)]
mod tests {
    use super::*;
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
}
