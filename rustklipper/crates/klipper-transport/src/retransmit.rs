//! MCU serial retransmission tracking, separate from command scheduling and UART I/O.
//!
//! Sequence numbers here are monotonically extended host-side values. The layer
//! that decodes the MCU's four-bit sequence field must extend it before calling
//! `acknowledge`. ACKs are cumulative: an ACK value retires every frame whose
//! sequence is strictly less than that value.
use std::collections::VecDeque;

pub const MIN_RTO_SECONDS: f64 = 0.025;
pub const MAX_RTO_SECONDS: f64 = 5.0;
pub const DEFAULT_RTO_SECONDS: f64 = 0.250;
pub const DUP_ACK_THRESHOLD: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedFrame {
    pub sequence: u64,
    pub bytes: Vec<u8>,
    /// Number of transmissions, including the original send.
    pub transmissions: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Retransmission {
    pub sequence: u64,
    pub bytes: Vec<u8>,
    pub transmissions: u32,
    pub timeout_seconds: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AckResult {
    pub acknowledged: Vec<TrackedFrame>,
    /// Set when duplicate ACKs trigger fast retransmission.
    pub fast_retransmit: Option<Retransmission>,
    /// RTT sample used for the estimator, if this ACK provided an unambiguous sample.
    pub rtt_sample_seconds: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetransmitError {
    InvalidTime,
    NonMonotonicTime,
    SequenceNotIncreasing,
    EmptyFrame,
}

#[derive(Debug, Clone)]
struct OutstandingFrame {
    frame: TrackedFrame,
    first_sent_at: f64,
    last_sent_at: f64,
    retransmitted: bool,
}

#[derive(Debug, Clone)]
pub struct RetransmitState {
    outstanding: VecDeque<OutstandingFrame>,
    srtt: Option<f64>,
    rttvar: Option<f64>,
    rto: f64,
    last_ack: u64,
    ignore_nak_sequence: u64,
    retransmit_sequence: u64,
    last_now: Option<f64>,
}

impl Default for RetransmitState {
    fn default() -> Self {
        Self {
            outstanding: VecDeque::new(),
            srtt: None,
            rttvar: None,
            rto: DEFAULT_RTO_SECONDS,
            last_ack: 0,
            ignore_nak_sequence: 0,
            retransmit_sequence: 0,
            last_now: None,
        }
    }
}

impl RetransmitState {
    pub fn new() -> Self { Self::default() }
    pub fn srtt(&self) -> Option<f64> { self.srtt }
    pub fn rttvar(&self) -> Option<f64> { self.rttvar }
    pub fn rto(&self) -> f64 { self.rto }
    pub fn outstanding_len(&self) -> usize { self.outstanding.len() }
    pub fn is_empty(&self) -> bool { self.outstanding.is_empty() }

    /// Validate a monotonic timestamp without changing the retry clock.
    pub fn check_time(&self, now: f64) -> Result<(), RetransmitError> {
        if !now.is_finite() || now < 0.0 {
            return Err(RetransmitError::InvalidTime);
        }
        if self.last_now.is_some_and(|last| now < last) {
            return Err(RetransmitError::NonMonotonicTime);
        }
        Ok(())
    }

    /// Record a frame immediately after its first successful transmission.
    pub fn track_sent(&mut self, sequence: u64, bytes: Vec<u8>, now: f64)
        -> Result<(), RetransmitError>
    {
        self.check_time(now)?;
        self.last_now = Some(now);
        if bytes.is_empty() { return Err(RetransmitError::EmptyFrame); }
        if self.outstanding.back().is_some_and(|entry| sequence <= entry.frame.sequence) {
            return Err(RetransmitError::SequenceNotIncreasing);
        }
        self.outstanding.push_back(OutstandingFrame {
            frame: TrackedFrame { sequence, bytes, transmissions: 1 },
            first_sent_at: now,
            last_sent_at: now,
            retransmitted: false,
        });
        Ok(())
    }

    /// Apply a cumulative ACK. The sequence must already be extended from the
    /// MCU's four-bit wire sequence into the host's monotonically increasing space.
    pub fn acknowledge(&mut self, ack_sequence: u64, now: f64)
        -> Result<AckResult, RetransmitError>
    {
        self.validate_time(now)?;
        if ack_sequence < self.last_ack {
            return Ok(AckResult { acknowledged: Vec::new(), fast_retransmit: None, rtt_sample_seconds: None });
        }

        let mut acknowledged = Vec::new();
        let mut rtt_candidate = None;
        while self.outstanding.front().is_some_and(|entry| entry.frame.sequence < ack_sequence) {
            let entry = self.outstanding.pop_front().expect("front checked");
            if !entry.retransmitted {
                rtt_candidate = Some((now - entry.first_sent_at).max(0.0));
            } else {
                rtt_candidate = None;
            }
            acknowledged.push(entry.frame);
        }

        // A cumulative ACK covering multiple frames does not identify which
        // transmission caused the ACK; only a single-frame ACK is an RTT sample.
        let rtt_sample_seconds = if acknowledged.len() == 1 { rtt_candidate } else { None };
        if let Some(sample) = rtt_sample_seconds { self.update_rtt(sample); }

        if ack_sequence > self.last_ack {
            self.last_ack = ack_sequence;
        }

        // Duplicate ACK handling is deliberately separate: only an empty MCU
        // message block is a NAK. A data frame with the same sequence is not.
        Ok(AckResult { acknowledged, fast_retransmit: None, rtt_sample_seconds })
    }

    /// Process an empty duplicate ACK (MCU NAK). Klipper retransmits every
    /// outstanding host frame immediately, while suppressing repeated NAKs
    /// for the same receive sequence until sequence progress is observed.
    pub fn nak(&mut self, now: f64) -> Result<Vec<Retransmission>, RetransmitError> {
        self.validate_time(now)?;
        if self.last_ack <= self.ignore_nak_sequence || self.outstanding.is_empty() {
            return Ok(Vec::new());
        }
        self.ignore_nak_sequence = self.last_ack;
        let next_send_sequence = self.outstanding.back()
            .map(|entry| entry.frame.sequence.saturating_add(1))
            .unwrap_or(self.last_ack);
        if self.last_ack < self.retransmit_sequence {
            self.ignore_nak_sequence = self.retransmit_sequence;
        }
        self.retransmit_sequence = next_send_sequence;
        Ok(self.retransmit_all(now))
    }

    /// Retransmit the complete outstanding window, preserving each encoded frame.
    pub fn retransmit_all(&mut self, now: f64) -> Vec<Retransmission> {
        let mut retries = Vec::with_capacity(self.outstanding.len());
        for entry in &mut self.outstanding {
            entry.frame.transmissions = entry.frame.transmissions.saturating_add(1);
            entry.retransmitted = true;
            entry.last_sent_at = now;
            retries.push(Retransmission {
                sequence: entry.frame.sequence,
                bytes: entry.frame.bytes.clone(),
                transmissions: entry.frame.transmissions,
                timeout_seconds: self.rto,
            });
        }
        retries
    }

    /// Poll the oldest outstanding deadline and retransmit the full window on timeout.
    pub fn poll_timeout_all(
        &mut self,
        now: f64,
    ) -> Result<Vec<Retransmission>, RetransmitError> {
        self.check_time(now)?;
        self.last_now = Some(now);
        let Some(front) = self.outstanding.front() else { return Ok(Vec::new()); };
        if now - front.last_sent_at < self.rto { return Ok(Vec::new()); }
        self.rto = (self.rto * 2.0).min(MAX_RTO_SECONDS);
        Ok(self.retransmit_all(now))
    }

    /// Return a retransmission when the oldest outstanding frame's RTO expires.
    /// Timeout backoff doubles the current RTO, capped at five seconds.
    pub fn poll_timeout(&mut self, now: f64)
        -> Result<Option<Retransmission>, RetransmitError>
    {
        self.validate_time(now)?;
        let Some(front) = self.outstanding.front() else { return Ok(None); };
        if now - front.last_sent_at < self.rto { return Ok(None); }
        self.rto = (self.rto * 2.0).min(MAX_RTO_SECONDS);
        Ok(self.retransmit_front(now))
    }

    fn retransmit_front(&mut self, now: f64) -> Option<Retransmission> {
        let entry = self.outstanding.front_mut()?;
        entry.frame.transmissions = entry.frame.transmissions.saturating_add(1);
        entry.retransmitted = true;
        entry.last_sent_at = now;
        Some(Retransmission {
            sequence: entry.frame.sequence,
            bytes: entry.frame.bytes.clone(),
            transmissions: entry.frame.transmissions,
            timeout_seconds: self.rto,
        })
    }

    fn update_rtt(&mut self, sample: f64) {
        match (self.srtt, self.rttvar) {
            (None, _) => {
                self.srtt = Some(sample);
                self.rttvar = Some(sample / 2.0);
            }
            (Some(srtt), Some(rttvar)) => {
                self.rttvar = Some(0.75 * rttvar + 0.25 * (srtt - sample).abs());
                self.srtt = Some(0.875 * srtt + 0.125 * sample);
            }
            _ => unreachable!("SRTT and RTTVAR are initialized together"),
        }
        let srtt = self.srtt.unwrap_or(sample);
        let rttvar = self.rttvar.unwrap_or(sample / 2.0);
        self.rto = (srtt + (4.0 * rttvar).max(0.001))
            .clamp(MIN_RTO_SECONDS, MAX_RTO_SECONDS);
    }

    fn validate_time(&mut self, now: f64) -> Result<(), RetransmitError> {
        if !now.is_finite() || now < 0.0 { return Err(RetransmitError::InvalidTime); }
        if self.last_now.is_some_and(|last| now < last) {
            return Err(RetransmitError::NonMonotonicTime);
        }
        self.last_now = Some(now);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_frame_ack_initializes_rtt_estimator() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![1, 2], 1.0).unwrap();
        let ack = state.acknowledge(2, 1.05).unwrap();
        assert_eq!(ack.acknowledged.len(), 1);
        assert!((state.srtt().unwrap() - 0.05).abs() < 1e-9);
        assert!((state.rttvar().unwrap() - 0.025).abs() < 1e-9);
        assert_eq!(state.outstanding_len(), 0);
    }

    #[test]
    fn multi_frame_cumulative_ack_does_not_make_ambiguous_rtt_sample() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![1], 1.0).unwrap();
        state.track_sent(2, vec![2], 1.01).unwrap();
        let ack = state.acknowledge(3, 1.05).unwrap();
        assert_eq!(ack.acknowledged.len(), 2);
        assert_eq!(ack.rtt_sample_seconds, None);
        assert_eq!(state.srtt(), None);
    }

    #[test]
    fn timeout_retransmits_entire_outstanding_window() {
        let mut state = RetransmitState::new();
        state.track_sent(4, vec![4], 0.0).unwrap();
        state.track_sent(5, vec![5], 0.001).unwrap();
        let retries = state.poll_timeout_all(0.250).unwrap();
        assert_eq!(retries.len(), 2);
        assert_eq!(retries[0].sequence, 4);
        assert_eq!(retries[1].sequence, 5);
    }

    #[test]
    fn timeout_retransmits_oldest_and_exponentially_backs_off() {
        let mut state = RetransmitState::new();
        state.track_sent(7, vec![0xaa], 0.0).unwrap();
        assert!(state.poll_timeout(0.249).unwrap().is_none());
        let retry = state.poll_timeout(0.250).unwrap().unwrap();
        assert_eq!(retry.sequence, 7);
        assert_eq!(retry.transmissions, 2);
        assert_eq!(state.rto(), 0.5);
        assert!(state.poll_timeout(0.749).unwrap().is_none());
        assert!(state.poll_timeout(0.750).unwrap().is_some());
        assert_eq!(state.rto(), 1.0);
    }

    #[test]
    fn retransmitted_frame_does_not_contribute_rtt_sample_karns_rule() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![1], 0.0).unwrap();
        state.poll_timeout(0.250).unwrap();
        let ack = state.acknowledge(2, 0.300).unwrap();
        assert_eq!(ack.rtt_sample_seconds, None);
        assert_eq!(state.srtt(), None);
    }

    #[test]
    fn duplicate_empty_ack_nak_retransmits_all_and_suppresses_repeat() {
        let mut state = RetransmitState::new();
        state.track_sent(10, vec![0x10], 0.0).unwrap();
        state.track_sent(11, vec![0x11], 0.001).unwrap();
        state.acknowledge(10, 0.010).unwrap();
        let retries = state.nak(0.011).unwrap();
        assert_eq!(retries.len(), 2);
        assert_eq!(retries[0].bytes, vec![0x10]);
        assert_eq!(retries[1].bytes, vec![0x11]);
        assert!(state.nak(0.012).unwrap().is_empty());
        state.acknowledge(11, 0.013).unwrap();
        assert_eq!(state.nak(0.014).unwrap().len(), 2);
    }

    #[test]
    fn stale_ack_is_ignored_and_invalid_clock_rejected() {
        let mut state = RetransmitState::new();
        state.track_sent(2, vec![1], 1.0).unwrap();
        assert!(state.acknowledge(1, 1.1).unwrap().acknowledged.is_empty());
        assert_eq!(state.acknowledge(3, 0.9), Err(RetransmitError::NonMonotonicTime));
    }

    #[test]
    fn rto_is_clamped_to_protocol_bounds() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![1], 0.0).unwrap();
        state.acknowledge(2, 0.000_001).unwrap();
        assert!(state.rto() >= MIN_RTO_SECONDS);
        assert!(state.rto() <= MAX_RTO_SECONDS);
    }
}
