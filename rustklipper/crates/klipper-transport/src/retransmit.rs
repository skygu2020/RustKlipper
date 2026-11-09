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
    /// Estimated UART-completion timestamp of the most recently acknowledged frame, retained across duplicate ACKs.
    pub last_receive_sent_time: f64,
    /// Set when duplicate ACKs trigger fast retransmission.
    pub fast_retransmit: Option<Retransmission>,
    /// RTT sample used for the estimator, if this ACK provided an unambiguous sample.
    pub rtt_sample_seconds: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RetransmitError {
    #[error("invalid retransmission timestamp")]
    InvalidTime,
    #[error("retransmission timestamp moved backwards")]
    NonMonotonicTime,
    #[error("frame sequence is not increasing")]
    SequenceNotIncreasing,
    #[error("cannot track an empty frame")]
    EmptyFrame,
    #[error("unknown outstanding frame sequence {0}")]
    UnknownSequence(u64),
}

#[derive(Debug, Clone)]
struct OutstandingFrame {
    frame: TrackedFrame,
    first_receive_at: f64,
    retransmitted: bool,
}

#[derive(Debug, Clone)]
pub struct RetransmitState {
    outstanding: VecDeque<OutstandingFrame>,
    srtt: Option<f64>,
    rttvar: Option<f64>,
    rto: f64,
    last_ack_seq: u64,
    ignore_nak_sequence: u64,
    retransmit_sequence: u64,
    last_now: Option<f64>,
    rtt_sample_seq: Option<u64>,
    retransmit_deadline: Option<f64>,
    wire_time_per_byte: f64,
    last_receive_sent_time: f64,
}

impl Default for RetransmitState {
    fn default() -> Self {
        Self {
            outstanding: VecDeque::new(),
            srtt: None,
            rttvar: None,
            rto: DEFAULT_RTO_SECONDS,
            last_ack_seq: 0,
            ignore_nak_sequence: 0,
            retransmit_sequence: 0,
            last_now: None,
            rtt_sample_seq: None,
            retransmit_deadline: None,
            wire_time_per_byte: 0.0,
            last_receive_sent_time: 0.0,
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
    pub fn last_receive_sent_time(&self) -> f64 { self.last_receive_sent_time }
    pub fn retransmit_sequence(&self) -> u64 { self.retransmit_sequence }
    pub fn retransmit_deadline(&self) -> Option<f64> { self.retransmit_deadline }

    /// Configure UART 8N1 transmission time per byte for ACK deadline margin.
    pub fn set_wire_time_per_byte(&mut self, seconds: f64) -> Result<(), RetransmitError> {
        if !seconds.is_finite() || seconds < 0.0 {
            return Err(RetransmitError::InvalidTime);
        }
        self.wire_time_per_byte = seconds;
        Ok(())
    }

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

    /// Record a frame after its initial write. The default receive timestamp
    /// is the estimated end of this frame's UART transmission.
    pub fn track_sent(&mut self, sequence: u64, bytes: Vec<u8>, now: f64)
        -> Result<(), RetransmitError>
    {
        let receive_time = now + self.wire_time_per_byte * bytes.len() as f64;
        self.track_sent_with_receive_time(sequence, bytes, now, receive_time)
    }

    /// Record a frame with the session's estimated UART-completion timestamp.
    /// This is the serialqueue receive_time used for RTT samples and retry
    /// deadlines; sent_at remains the common command_event timestamp.
    /// Later frames in one command batch include preceding bytes.
    pub fn track_sent_with_receive_time(
        &mut self,
        sequence: u64,
        bytes: Vec<u8>,
        sent_at: f64,
        receive_time: f64,
    ) -> Result<(), RetransmitError> {
        self.check_time(sent_at)?;
        if !receive_time.is_finite() || receive_time < sent_at {
            return Err(RetransmitError::InvalidTime);
        }
        if bytes.is_empty() { return Err(RetransmitError::EmptyFrame); }
        if let Some(previous) = self.outstanding.back() {
            if sequence <= previous.frame.sequence {
                return Err(RetransmitError::SequenceNotIncreasing);
            }
            if receive_time < previous.first_receive_at {
                return Err(RetransmitError::InvalidTime);
            }
        }
        self.last_now = Some(sent_at);
        if self.outstanding.is_empty() {
            // serialqueue.c starts the initial retry timer after the first
            // frame's estimated UART wire time, not at write() completion.
            self.retransmit_deadline = Some(receive_time + self.rto);
        }
        if self.rtt_sample_seq.is_none() {
            self.rtt_sample_seq = Some(sequence);
        }
        self.outstanding.push_back(OutstandingFrame {
            frame: TrackedFrame { sequence, bytes, transmissions: 1 },
            first_receive_at: receive_time,
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
        if ack_sequence < self.last_ack_seq {
            return Ok(AckResult { acknowledged: Vec::new(), last_receive_sent_time: self.last_receive_sent_time, fast_retransmit: None, rtt_sample_seconds: None });
        }

        let had_outstanding = !self.outstanding.is_empty();
        let mut acknowledged = Vec::new();
        let mut last_receive_sent_time = None;
        let mut last_frame_retransmitted = false;
        while self.outstanding.front().is_some_and(|entry| entry.frame.sequence < ack_sequence) {
            let entry = self.outstanding.pop_front().expect("front checked");
            // serialqueue retains the original queue_message.receive_time across retries.
            last_receive_sent_time = Some(entry.first_receive_at);
            last_frame_retransmitted = entry.retransmitted;
            acknowledged.push(entry.frame);
        }

        // serialqueue.c samples once per RTT window, using the transmission
        // completion time of the block immediately preceding the received seq.
        if let Some(sent_time) = last_receive_sent_time {
            self.last_receive_sent_time = sent_time;
        } else if !had_outstanding {
            // serialqueue resets this value when an advancing sequence arrives
            // while its sent queue is empty (connection/sequence resynchronization).
            self.last_receive_sent_time = 0.0;
        }

        let rtt_sample_seconds = if self.rtt_sample_seq.is_some_and(|seq| ack_sequence > seq) {
            self.rtt_sample_seq = None;
            if last_frame_retransmitted {
                None
            } else {
                last_receive_sent_time.map(|sent| (now - sent).max(0.0))
            }
        } else {
            None
        };
        if let Some(sample) = rtt_sample_seconds {
            self.update_rtt(sample);
        }

        if self.outstanding.is_empty() {
            self.retransmit_deadline = None;
        } else if !acknowledged.is_empty() {
            let oldest_wire_time = self.outstanding.front().map_or(0.0, |entry| {
                self.wire_time_per_byte * entry.frame.bytes.len() as f64
            });
            self.retransmit_deadline = Some(now + self.rto + oldest_wire_time);
        }

        Ok(AckResult { acknowledged, last_receive_sent_time: self.last_receive_sent_time, fast_retransmit: None, rtt_sample_seconds })
    }

    /// Process an empty MCU message block using serialqueue.c's NAK rules.
    pub fn handle_empty_ack(
        &mut self, receive_seq: u64, send_seq: u64, now: f64,
    ) -> Result<Vec<Retransmission>, RetransmitError> {
        self.validate_time(now)?;
        if self.last_ack_seq < receive_seq {
            self.last_ack_seq = receive_seq;
            return Ok(Vec::new());
        }
        if receive_seq <= self.ignore_nak_sequence || self.outstanding.is_empty() {
            return Ok(Vec::new());
        }
        // State update performed by the retransmit timer after a NAK.
        self.ignore_nak_sequence = receive_seq;
        if receive_seq < self.retransmit_sequence {
            self.ignore_nak_sequence = self.retransmit_sequence;
        }
        self.retransmit_sequence = send_seq;
        self.rtt_sample_seq = None;
        Ok(self.retransmit_all(now))
    }

    /// Retransmit the complete outstanding window, preserving each encoded frame.
    pub fn retransmit_all(&mut self, now: f64) -> Vec<Retransmission> {
        let mut retries = Vec::with_capacity(self.outstanding.len());
        self.rtt_sample_seq = None;
        let first_frame_wire_time = self.outstanding.front().map_or(0.0, |entry| {
            self.wire_time_per_byte * (entry.frame.bytes.len() + 1) as f64
        });
        self.retransmit_deadline = Some(now + self.rto + first_frame_wire_time);
        for entry in &mut self.outstanding {
            entry.frame.transmissions = entry.frame.transmissions.saturating_add(1);
            entry.retransmitted = true;
            retries.push(Retransmission {
                sequence: entry.frame.sequence,
                bytes: entry.frame.bytes.clone(),
                transmissions: entry.frame.transmissions,
                timeout_seconds: self.rto,
            });
        }
        retries
    }

    /// Record a retransmission batch using its shared write-start timestamp.
    /// The timeout includes the sync byte and first cached frame's wire time,
    /// matching serialqueue.c's retransmit_event() deadline.
    pub fn mark_retransmission_sent(
        &mut self, sequence: u64, sent_at: f64,
    ) -> Result<(), RetransmitError> {
        self.validate_time(sent_at)?;
        let is_oldest = self.outstanding.front()
            .is_some_and(|entry| entry.frame.sequence == sequence);
        let entry = self.outstanding.iter_mut()
            .find(|entry| entry.frame.sequence == sequence)
            .ok_or(RetransmitError::UnknownSequence(sequence))?;
        // Only the oldest outstanding entry controls the window deadline.
        // Every frame in the retry batch shares one write-start timestamp, so
        // later entries must not move that deadline.
        if is_oldest {
            let first_frame_wire_time =
                self.wire_time_per_byte * (entry.frame.bytes.len() + 1) as f64;
            self.retransmit_deadline =
                Some(sent_at + self.rto + first_frame_wire_time);
        }
        Ok(())
    }

    /// Poll the oldest outstanding deadline and retransmit the full window on timeout.
    pub fn poll_timeout_all(
        &mut self,
        now: f64,
    ) -> Result<Vec<Retransmission>, RetransmitError> {
        self.check_time(now)?;
        self.last_now = Some(now);
        if self.outstanding.is_empty() { return Ok(Vec::new()); }
        if self.retransmit_deadline.is_some_and(|deadline| now < deadline) {
            return Ok(Vec::new());
        }
        self.rto = (self.rto * 2.0).min(MAX_RTO_SECONDS);
        self.ignore_nak_sequence = self.outstanding.back()
            .map_or(self.ignore_nak_sequence, |entry| entry.frame.sequence.saturating_add(1));
        self.retransmit_sequence = self.ignore_nak_sequence;
        self.rtt_sample_seq = None;
        Ok(self.retransmit_all(now))
    }

    fn update_rtt(&mut self, sample: f64) {
        match (self.srtt, self.rttvar) {
            (None, _) => {
                self.srtt = Some(sample * 10.0);
                self.rttvar = Some(sample / 2.0);
            }
            (Some(srtt), Some(rttvar)) => {
                self.rttvar = Some(0.75 * rttvar + 0.25 * (srtt - sample).abs());
                self.srtt = Some(0.875 * srtt + 0.125 * sample);
            }
            _ => unreachable!("SRTT and RTTVAR are initialized together"),
        }
        let srtt = self.srtt.unwrap_or(sample * 10.0);
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
        assert!((state.srtt().unwrap() - 0.5).abs() < 1e-9);
        assert!((state.rttvar().unwrap() - 0.025).abs() < 1e-9);
        assert_eq!(state.outstanding_len(), 0);
    }

    #[test]
    fn multi_frame_cumulative_ack_samples_last_acked_block_in_rtt_window() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![1], 1.0).unwrap();
        state.track_sent(2, vec![2], 1.01).unwrap();
        let ack = state.acknowledge(3, 1.05).unwrap();
        assert_eq!(ack.acknowledged.len(), 2);
        assert!((ack.rtt_sample_seconds.unwrap() - 0.04).abs() < 1e-9);
        assert!((state.srtt().unwrap() - 0.4).abs() < 1e-9);
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
        assert!(state.poll_timeout_all(0.249).unwrap().is_empty());
        let retry = state.poll_timeout_all(0.250).unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].sequence, 7);
        assert_eq!(retry[0].transmissions, 2);
        assert_eq!(state.rto(), 0.5);
        assert!(state.poll_timeout_all(0.749).unwrap().is_empty());
        assert_eq!(state.poll_timeout_all(0.750).unwrap().len(), 1);
        assert_eq!(state.rto(), 1.0);
    }



    #[test]
    fn empty_sent_window_resets_last_receive_sent_timestamp() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![0x10], 1.0).unwrap();
        assert_eq!(state.acknowledge(2, 1.1).unwrap().last_receive_sent_time, 1.0);

        // A new advancing sequence with no host frames is the connection
        // initialization/resynchronization case in serialqueue.c.
        assert_eq!(state.acknowledge(3, 1.2).unwrap().last_receive_sent_time, 0.0);
    }

    #[test]
    fn receive_timestamps_must_be_finite_monotonic_and_after_write_start() {
        let mut state = RetransmitState::new();
        assert_eq!(
            state.track_sent_with_receive_time(1, vec![1], 1.0, 0.9),
            Err(RetransmitError::InvalidTime),
        );
        state.track_sent_with_receive_time(1, vec![1], 1.0, 1.1).unwrap();
        assert_eq!(
            state.track_sent_with_receive_time(2, vec![2], 1.01, 1.05),
            Err(RetransmitError::InvalidTime),
        );
    }

    #[test]
    fn rtt_sample_uses_uart_completion_timestamp_not_write_start() {
        let mut state = RetransmitState::new();
        state.set_wire_time_per_byte(0.001).unwrap();
        state.track_sent(1, vec![0; 10], 1.0).unwrap();

        let ack = state.acknowledge(2, 1.2).unwrap();
        assert!((ack.last_receive_sent_time - 1.01).abs() < 1e-12);
        assert!((ack.rtt_sample_seconds.unwrap() - 0.19).abs() < 1e-9);
    }

    #[test]
    fn cumulative_ack_uses_the_last_frame_completion_time_in_batch() {
        let mut state = RetransmitState::new();
        state.track_sent_with_receive_time(1, vec![1; 5], 1.0, 1.005).unwrap();
        state.track_sent_with_receive_time(2, vec![2; 5], 1.0, 1.010).unwrap();

        let ack = state.acknowledge(3, 1.2).unwrap();
        assert_eq!(ack.acknowledged.len(), 2);
        assert!((ack.last_receive_sent_time - 1.010).abs() < 1e-12);
        assert!((ack.rtt_sample_seconds.unwrap() - 0.190).abs() < 1e-9);
    }

    #[test]
    fn explicit_receive_timestamp_includes_prior_batch_bytes() {
        let mut state = RetransmitState::new();
        state.track_sent_with_receive_time(1, vec![0; 10], 1.0, 1.025).unwrap();
        let ack = state.acknowledge(2, 1.2).unwrap();
        assert_eq!(ack.last_receive_sent_time, 1.025);
        assert!((ack.rtt_sample_seconds.unwrap() - 0.175).abs() < 1e-9);
    }

    #[test]
    fn acknowledged_timestamp_remains_original_after_retransmission() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![0x10, 0x20], 1.0).unwrap();
        let retry = state.retransmit_all(1.3);
        assert_eq!(retry.len(), 1);
        state.mark_retransmission_sent(1, 1.4).unwrap();

        let ack = state.acknowledge(2, 1.5).unwrap();
        assert_eq!(ack.last_receive_sent_time, 1.0);
    }

    #[test]
    fn ack_deadline_includes_oldest_remaining_frame_wire_time() {
        let mut state = RetransmitState::new();
        state.set_wire_time_per_byte(0.001).unwrap();
        state.track_sent(1, vec![0; 4], 0.0).unwrap();
        state.track_sent(2, vec![0; 20], 0.001).unwrap();
        state.acknowledge(2, 0.010).unwrap();

        let deadline = 0.010 + state.rto() + 0.020;
        assert!(state.poll_timeout_all(deadline - 0.000_001).unwrap().is_empty());
        assert_eq!(state.poll_timeout_all(deadline).unwrap().len(), 1);
    }

    #[test]
    fn initial_retry_deadline_uses_explicit_batch_completion_time() {
        let mut state = RetransmitState::new();
        state.track_sent_with_receive_time(1, vec![0; 10], 1.0, 1.030).unwrap();

        let deadline = 1.030 + state.rto();
        assert!(state.poll_timeout_all(deadline - 0.000_001).unwrap().is_empty());
        assert_eq!(state.poll_timeout_all(deadline).unwrap().len(), 1);
    }

    #[test]
    fn initial_deadline_includes_first_frame_wire_time() {
        let mut state = RetransmitState::new();
        state.set_wire_time_per_byte(0.001).unwrap();
        state.track_sent(1, vec![0; 20], 1.0).unwrap();

        let deadline = 1.0 + state.rto() + 0.020;
        assert!(state.poll_timeout_all(deadline - 0.000_001).unwrap().is_empty());
        assert_eq!(state.poll_timeout_all(deadline).unwrap().len(), 1);
    }

    #[test]
    fn retransmit_window_deadline_uses_oldest_frame_and_sync_wire_time() {
        let mut state = RetransmitState::new();
        state.set_wire_time_per_byte(0.001).unwrap();
        state.track_sent(1, vec![1], 0.0).unwrap();
        state.track_sent(2, vec![2], 0.001).unwrap();
        let retries = state.poll_timeout_all(0.251).unwrap();
        assert_eq!(retries.len(), 2);

        state.mark_retransmission_sent(1, 0.260).unwrap();
        let first_deadline = 0.260 + state.rto() + 0.002;
        state.mark_retransmission_sent(2, 0.300).unwrap();

        assert!(state.poll_timeout_all(first_deadline - 0.000_001).unwrap().is_empty());
        assert_eq!(state.poll_timeout_all(first_deadline).unwrap().len(), 2);
    }

    #[test]
    fn retransmission_deadline_includes_sync_and_first_frame_wire_time() {
        let mut state = RetransmitState::new();
        state.set_wire_time_per_byte(0.001).unwrap();
        state.track_sent(1, vec![0; 4], 0.0).unwrap();
        state.retransmit_all(1.0);
        state.mark_retransmission_sent(1, 1.1).unwrap();

        let deadline = 1.1 + state.rto() + 0.005;
        assert!(state.poll_timeout_all(deadline - 0.000_001).unwrap().is_empty());
        assert_eq!(state.poll_timeout_all(deadline).unwrap().len(), 1);
    }

    #[test]
    fn retransmitted_frame_does_not_contribute_rtt_sample_karns_rule() {
        let mut state = RetransmitState::new();
        state.track_sent(1, vec![1], 0.0).unwrap();
        state.poll_timeout_all(0.250).unwrap();
        let ack = state.acknowledge(2, 0.300).unwrap();
        assert_eq!(ack.rtt_sample_seconds, None);
        assert_eq!(state.srtt(), None);
    }

    #[test]
    fn duplicate_empty_ack_nak_retransmits_all_and_suppresses_repeat() {
        let mut state = RetransmitState::new();
        state.track_sent(10, vec![0x10], 0.0).unwrap();
        state.track_sent(11, vec![0x11], 0.001).unwrap();
        assert!(state.handle_empty_ack(10, 12, 0.010).unwrap().is_empty());
        let retries = state.handle_empty_ack(10, 12, 0.011).unwrap();
        assert_eq!(retries.len(), 2);
        assert_eq!(retries[0].bytes, vec![0x10]);
        assert_eq!(retries[1].bytes, vec![0x11]);
        // The MCU advances its cumulative ACK before a later duplicate ACK
        // can signal a NAK for the remaining frame.
        assert_eq!(state.acknowledge(11, 0.012).unwrap().acknowledged.len(), 1);
        assert!(state.handle_empty_ack(11, 12, 0.013).unwrap().is_empty());
        let next_nak = state.handle_empty_ack(11, 12, 0.014).unwrap();
        assert_eq!(next_nak.len(), 1);
        assert_eq!(next_nak[0].sequence, 11);
        assert!(state.handle_empty_ack(11, 12, 0.015).unwrap().is_empty());
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
