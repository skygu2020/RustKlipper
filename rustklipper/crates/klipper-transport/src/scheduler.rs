//! Clock-aware command queue scheduling modeled on Klipper serialqueue.c.
//! This layer decides command eligibility and packs commands into MCU frames;
//! UART I/O, ACK/retransmit state, and clock estimation remain separate.
use std::collections::{BTreeMap, VecDeque};
use klipper_protocol::{encode_frame, Frame, ProtocolError, MESSAGE_MAX, MESSAGE_MIN};
use crate::retransmit::{AckResult, Retransmission, RetransmitError, RetransmitState};

pub const MAX_PENDING_BLOCKS: u64 = 12;
pub const BACKGROUND_PRIORITY_CLOCK: u64 = u64::MAX;
pub const MESSAGE_PAYLOAD_MAX: usize = MESSAGE_MAX - MESSAGE_MIN;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledCommand {
    pub bytes: Vec<u8>,
    pub min_clock: u64,
    pub req_clock: u64,
    pub notify_id: Option<u64>,
}
impl ScheduledCommand {
    pub fn new(bytes: Vec<u8>, min_clock: u64, req_clock: u64) -> Self {
        Self { bytes, min_clock, req_clock, notify_id: None }
    }
    pub fn with_notify(mut self, notify_id: u64) -> Self {
        self.notify_id = Some(notify_id); self
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledFrame {
    pub send_sequence: u64,
    pub bytes: Vec<u8>,
    pub notify_ids: Vec<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleError {
    #[error("scheduled command is empty")]
    EmptyCommand,
    #[error("scheduled command exceeds payload limit: {0} bytes")]
    CommandTooLarge(usize),
    #[error(transparent)]
    Frame(#[from] ProtocolError),
    #[error(transparent)]
    Retransmit(#[from] RetransmitError),
    #[error("invalid ACK sequence {received}; highest sent sequence is {sent}")]
    InvalidAckSequence { received: u64, sent: u64 },
    #[error("MCU send sequence space is exhausted")]
    SequenceExhausted,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReceivedFrameResult {
    pub receive_sequence: u64,
    /// Matches serialqueue's last_receive_sent_time for ACK notifications.
    pub last_receive_sent_time: f64,
    /// Timestamp attached to a data response (zero after retransmit cutoff).
    pub response_sent_time: f64,
    pub has_payload: bool,
    pub acknowledged: Vec<crate::retransmit::TrackedFrame>,
    pub retransmissions: Vec<Retransmission>,
}

#[derive(Debug, Default)]
struct QueueState { order: u64, upcoming: VecDeque<ScheduledCommand>, ready: VecDeque<ScheduledCommand> }

#[derive(Debug)]
pub struct CommandScheduler {
    queues: BTreeMap<u64, QueueState>,
    next_queue_order: u64,
    send_sequence: u64,
    receive_sequence: u64,
    receive_window: usize,
    need_ack_bytes: usize,
    last_ack_bytes: usize,
    last_ack_sequence: u64,
    sent_blocks: VecDeque<(u64, usize)>,
    retransmit: RetransmitState,
}
impl Default for CommandScheduler {
    fn default() -> Self {
        Self { queues: BTreeMap::new(), next_queue_order: 0, send_sequence: 1, receive_sequence: 1,
            receive_window: 0, need_ack_bytes: 0, last_ack_bytes: 0, last_ack_sequence: 0, sent_blocks: VecDeque::new(), retransmit: RetransmitState::new(),
        }
    }
}
impl CommandScheduler {
    pub fn new() -> Self { Self::default() }
    pub fn set_receive_window(&mut self, bytes: usize) { self.receive_window = bytes; }

    /// Configure UART 8N1 timing used for the post-ACK retransmit margin.
    pub fn set_wire_time_per_byte(&mut self, seconds: f64) -> Result<(), ScheduleError> {
        self.retransmit.set_wire_time_per_byte(seconds)?;
        Ok(())
    }
    pub fn send_sequence(&self) -> u64 { self.send_sequence }

    /// Return the minimum effective request clock among ready queue heads.
    /// Klipper substitutes a moving clock for BACKGROUND_PRIORITY_CLOCK only
    /// while deciding when to wake; frame packing itself retains raw req_clock
    /// ordering, which places background traffic after finite-clock commands.
    pub fn minimum_ready_request_clock(&self, background_clock: u64) -> Option<u64> {
        self.queues.values()
            .filter_map(|queue| queue.ready.front())
            .map(|command| {
                if command.req_clock == BACKGROUND_PRIORITY_CLOCK {
                    background_clock
                } else {
                    command.req_clock
                }
            })
            .min()
    }

    /// Earliest minimum-clock barrier among the heads of upcoming queues.
    /// Used to wake the scheduler when a command becomes eligible.
    pub fn minimum_upcoming_min_clock(&self) -> Option<u64> {
        self.queues.values()
            .filter_map(|queue| queue.upcoming.front().map(|command| command.min_clock))
            .min()
    }

    pub fn receive_sequence(&self) -> u64 { self.receive_sequence }

    /// Commands within one command queue retain FIFO order.
    pub fn enqueue(&mut self, queue_id: u64, mut command: ScheduledCommand)
        -> Result<(), ScheduleError>
    {
        if command.bytes.is_empty() { return Err(ScheduleError::EmptyCommand); }
        if command.bytes.len() > MESSAGE_PAYLOAD_MAX {
            return Err(ScheduleError::CommandTooLarge(command.bytes.len()));
        }
        if command.req_clock != BACKGROUND_PRIORITY_CLOCK
            && command.min_clock.wrapping_add(1u64 << 31) < command.req_clock
        {
            command.min_clock = command.req_clock - (1u64 << 31);
        }
        let queue_is_empty = self.queues.get(&queue_id)
            .map_or(true, |queue| queue.upcoming.is_empty() && queue.ready.is_empty());
        if queue_is_empty {
            let order = self.next_queue_order;
            self.next_queue_order = self.next_queue_order.wrapping_add(1);
            self.queues.entry(queue_id).or_default().order = order;
        }
        self.queues.entry(queue_id).or_default().upcoming.push_back(command);
        Ok(())
    }

    /// Remove a command carrying this notification ID if it has not yet
    /// been packed into a frame. Returns true when a queued command was removed.
    pub fn cancel_notify(&mut self, notify_id: u64) -> bool {
        let mut removed = false;
        for queue in self.queues.values_mut() {
            let before = queue.upcoming.len();
            queue.upcoming.retain(|command| command.notify_id != Some(notify_id));
            removed |= queue.upcoming.len() != before;

            let before = queue.ready.len();
            queue.ready.retain(|command| command.notify_id != Some(notify_id));
            removed |= queue.ready.len() != before;
        }
        removed
    }

    /// Release commands whose min_clock is no later than ack_clock.
    /// Returns the earliest clock at which a queued command remains stalled.
    pub fn promote_ready(&mut self, ack_clock: u64) -> Option<u64> {
        let mut min_stalled: Option<u64> = None;
        for queue in self.queues.values_mut() {
            while let Some(command) = queue.upcoming.front() {
                if command.min_clock > ack_clock {
                    min_stalled = Some(min_stalled.map_or(command.min_clock, |v| v.min(command.min_clock)));
                    break;
                }
                let command = queue.upcoming.pop_front().expect("front was present");
                queue.ready.push_back(command);
            }
        }
        min_stalled
    }

    /// Whether serialqueue's pending-block and receive-window limits permit
    /// another frame to be built.
    pub fn can_send_frame(&self) -> bool {
        if self.send_sequence.saturating_sub(self.receive_sequence) >= MAX_PENDING_BLOCKS
            && self.receive_sequence != u64::MAX
        {
            return false;
        }
        if self.send_sequence > self.receive_sequence && self.receive_window > 0 {
            let mut need = self.need_ack_bytes + MESSAGE_MAX;
            if self.last_ack_sequence < self.receive_sequence {
                need += self.last_ack_bytes;
            }
            if need > self.receive_window {
                return false;
            }
        }
        true
    }

    /// Pack a frame. Call mark_frame_sent only after the UART write succeeds.
    pub fn build_frame(&mut self) -> Result<Option<ScheduledFrame>, ScheduleError> {
        self.build_frame_inner()
    }

    fn build_frame_inner(&mut self) -> Result<Option<ScheduledFrame>, ScheduleError> {
        // Extended send sequences must remain strictly increasing for ACK and
        // retransmission tracking. Never wrap back to zero and reuse old IDs.
        if self.send_sequence == u64::MAX {
            return Err(ScheduleError::SequenceExhausted);
        }
        if !self.can_send_frame() {
            return Ok(None);
        }
        let mut payload = Vec::with_capacity(MESSAGE_PAYLOAD_MAX);
        let mut notify_ids = Vec::new();
        loop {
            let selected = self.queues.iter()
                .filter_map(|(id, q)| q.ready.front().map(|cmd| {
                    (*id, q.order, cmd.req_clock, cmd.bytes.len())
                }))
                .min_by_key(|(_, order, priority, _)| (*priority, *order));
            let Some((queue_id, _, _, size)) = selected else { break; };
            if payload.len() + size > MESSAGE_PAYLOAD_MAX { break; }
            let command = self.queues.get_mut(&queue_id)
                .and_then(|q| q.ready.pop_front()).expect("selected queue has a ready command");
            payload.extend_from_slice(&command.bytes);
            if let Some(id) = command.notify_id { notify_ids.push(id); }
        }
        if payload.is_empty() { return Ok(None); }
        let seq = self.send_sequence;
        let bytes = encode_frame((seq as u8) & 0x0f, &payload)?;
        self.send_sequence = self.send_sequence.checked_add(1).expect("sequence exhaustion checked before frame construction");
        self.need_ack_bytes = self.need_ack_bytes.saturating_add(bytes.len());
        self.sent_blocks.push_back((seq, bytes.len()));
        Ok(Some(ScheduledFrame { send_sequence: seq, bytes, notify_ids }))
    }

    /// Register a frame after its complete encoded bytes were successfully
    /// written to the serial device. This starts its retransmission timer.
    pub fn mark_frame_sent(
        &mut self, frame: &ScheduledFrame, sent_at: f64,
    ) -> Result<(), ScheduleError> {
        self.retransmit.track_sent(frame.send_sequence, frame.bytes.clone(), sent_at)?;
        Ok(())
    }

    /// Register a frame with its predicted UART-completion timestamp.
    pub fn mark_frame_sent_with_receive_time(
        &mut self,
        frame: &ScheduledFrame,
        sent_at: f64,
        receive_time: f64,
    ) -> Result<(), ScheduleError> {
        self.retransmit.track_sent_with_receive_time(
            frame.send_sequence, frame.bytes.clone(), sent_at, receive_time,
        )?;
        Ok(())
    }

    /// Update state after the caller extends an MCU four-bit sequence.
    pub fn acknowledge(&mut self, receive_sequence: u64, acknowledged_frame_bytes: usize) {
        if receive_sequence <= self.receive_sequence { return; }
        self.receive_sequence = receive_sequence;
        let mut last_acked = 0;
        while self.sent_blocks.front().is_some_and(|(seq, _)| *seq < receive_sequence) {
            if let Some((_seq, bytes)) = self.sent_blocks.pop_front() {
                self.need_ack_bytes = self.need_ack_bytes.saturating_sub(bytes);
                last_acked = bytes;
            }
        }
        self.last_ack_bytes = if last_acked == 0 { acknowledged_frame_bytes } else { last_acked };
    }

    /// Apply a cumulative extended ACK to scheduler flow control and retry state.
    /// Duplicate ACKs are passed to the retry tracker for fast retransmit.
    pub fn acknowledge_at(
        &mut self,
        receive_sequence: u64,
        acknowledged_frame_bytes: usize,
        now: f64,
    ) -> Result<AckResult, ScheduleError> {
        let retry_result = self.retransmit.acknowledge(receive_sequence, now)?;
        self.acknowledge(receive_sequence, acknowledged_frame_bytes);
        Ok(retry_result)
    }

    /// Process an empty duplicate MCU frame (NAK) and return all cached
    /// outstanding frames that must be retransmitted immediately.
    pub fn handle_nak_at(&mut self, now: f64) -> Result<Vec<Retransmission>, ScheduleError> {
        Ok(self.retransmit.handle_empty_ack(self.receive_sequence, self.send_sequence, now)?)
    }

    /// Process a decoded MCU frame, extending its four-bit sequence and
    /// distinguishing data from empty ACK/NAK blocks.
    pub fn handle_received_frame(
        &mut self, frame: &Frame, now: f64,
    ) -> Result<ReceivedFrameResult, ScheduleError> {
        let receive_sequence = crate::ack::AckTracker::extend_wire_sequence(
            self.receive_sequence, frame.sequence
        );
        if receive_sequence > self.send_sequence {
            // serialqueue.c permits the first MCU response to establish the
            // initial sequence when no host frame has been sent yet.
            if self.receive_sequence == 1 && self.sent_blocks.is_empty() {
                self.send_sequence = receive_sequence;
            } else {
                return Err(ScheduleError::InvalidAckSequence {
                    received: receive_sequence, sent: self.send_sequence,
                });
            }
        }
        let mut acknowledged = Vec::new();
        if receive_sequence > self.receive_sequence {
            let ack_result = self.retransmit.acknowledge(receive_sequence, now)?;
            acknowledged = ack_result.acknowledged;
            self.acknowledge(receive_sequence, 0);
        }
        let retransmissions = if frame.payload.is_empty() {
            // serialqueue.c advances last_ack_seq only for empty ACK blocks.
            // Data frames still leave last_ack_bytes charged in the receive window.
            if self.last_ack_sequence < receive_sequence {
                self.last_ack_sequence = receive_sequence;
            }
            self.retransmit.handle_empty_ack(receive_sequence, self.send_sequence, now)?
        } else { Vec::new() };
        let last_receive_sent_time = self.retransmit.last_receive_sent_time();
        let response_sent_time = if receive_sequence > self.retransmit.retransmit_sequence() {
            last_receive_sent_time
        } else { 0.0 };
        Ok(ReceivedFrameResult {
            receive_sequence, last_receive_sent_time, response_sent_time,
            has_payload: !frame.payload.is_empty(),
            acknowledged, retransmissions,
        })
    }

    /// Poll timeout and return the complete outstanding window for retransmission.
    pub fn poll_retransmissions(&mut self, now: f64)
        -> Result<Vec<Retransmission>, ScheduleError>
    {
        Ok(self.retransmit.poll_timeout_all(now)?)
    }

    pub fn retransmit_state(&self) -> &RetransmitState { &self.retransmit }
    pub fn next_retransmit_at(&self) -> Option<f64> {
        self.retransmit.retransmit_deadline()
    }
    pub(crate) fn retransmit_state_mut(&mut self) -> &mut RetransmitState { &mut self.retransmit }

    /// Total bytes currently eligible for frame construction.
    pub fn ready_bytes(&self) -> usize {
        self.queues.values().flat_map(|queue| queue.ready.iter())
            .map(|command| command.bytes.len()).sum()
    }

    pub fn queued_counts(&self) -> (usize, usize) {
        self.queues.values().fold((0, 0), |(up, ready), q| {
            (up + q.upcoming.len(), ready + q.ready.len())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klipper_protocol::FrameDecoder;

    #[test]
    fn request_clock_normalization_uses_klippers_half_range() {
        let mut scheduler = CommandScheduler::new();
        let req_clock = 10_000_000_000;
        let min_clock = req_clock - (1u64 << 31) - 1;
        scheduler.enqueue(1, ScheduledCommand::new(vec![0x11], min_clock, req_clock)).unwrap();
        assert_eq!(scheduler.promote_ready(min_clock), Some(min_clock + 1));
        assert_eq!(scheduler.queued_counts(), (1, 0));
        assert_eq!(scheduler.promote_ready(min_clock + 1), None);
        assert_eq!(scheduler.queued_counts(), (0, 1));
    }

    #[test]
    fn send_sequence_exhaustion_rejects_frame_without_consuming_queued_commands() {
        let mut scheduler = CommandScheduler::new();
        scheduler.send_sequence = u64::MAX;
        scheduler.enqueue(1, ScheduledCommand::new(vec![0x5a], 0, 1)).unwrap();
        scheduler.promote_ready(0);

        assert_eq!(scheduler.build_frame(), Err(ScheduleError::SequenceExhausted.into()));
        assert_eq!(scheduler.send_sequence(), u64::MAX);
        assert_eq!(scheduler.queued_counts(), (0, 1));
        assert_eq!(scheduler.ready_bytes(), 1);
    }

    #[test]
    fn can_send_frame_obeys_pending_block_limit() {
        let mut scheduler = CommandScheduler::new();
        for index in 0..MAX_PENDING_BLOCKS {
            scheduler.enqueue(index, ScheduledCommand::new(vec![index as u8], 0, 0)).unwrap();
            scheduler.promote_ready(0);
            assert!(scheduler.build_frame().unwrap().is_some());
        }
        assert!(!scheduler.can_send_frame());
    }

    #[test]
    fn can_send_frame_obeys_receive_window_limit() {
        let mut scheduler = CommandScheduler::new();
        scheduler.enqueue(1, ScheduledCommand::new(vec![0x11], 0, 0)).unwrap();
        scheduler.promote_ready(0);
        let frame = scheduler.build_frame().unwrap().unwrap();
        scheduler.mark_frame_sent(&frame, 1.0).unwrap();
        scheduler.set_receive_window(1);
        assert!(!scheduler.can_send_frame());
    }

    #[test]
    fn minimum_upcoming_clock_reports_only_queue_heads() {
        let mut scheduler = CommandScheduler::new();
        scheduler.enqueue(1, ScheduledCommand::new(vec![1], 300, 400)).unwrap();
        scheduler.enqueue(1, ScheduledCommand::new(vec![2], 100, 200)).unwrap();
        scheduler.enqueue(2, ScheduledCommand::new(vec![3], 250, 350)).unwrap();
        assert_eq!(scheduler.minimum_upcoming_min_clock(), Some(250));
        scheduler.promote_ready(250);
        assert_eq!(scheduler.minimum_upcoming_min_clock(), Some(300));
    }

    #[test]
    fn commands_wait_for_min_clock_then_become_ready() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![0x11], 100, 200)).unwrap();
        assert_eq!(s.promote_ready(99), Some(100));
        assert_eq!(s.queued_counts(), (1, 0));
        assert_eq!(s.promote_ready(100), None);
        assert_eq!(s.queued_counts(), (0, 1));
    }
    #[test]
    fn lowest_request_clock_wins_across_queues_and_fifo_is_kept() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![1], 0, 30)).unwrap();
        s.enqueue(1, ScheduledCommand::new(vec![2], 0, 10)).unwrap();
        s.enqueue(2, ScheduledCommand::new(vec![3], 0, 20)).unwrap();
        s.promote_ready(0);
        let frame = s.build_frame().unwrap().unwrap();
        let mut decoder = FrameDecoder::new();
        let parsed = decoder.push(&frame.bytes);
        assert_eq!(parsed.len(), 1);
        // Queue 1 retains FIFO (30 before 10); queue 2's 20-clock command wins first.
        assert_eq!(parsed[0].payload, vec![3, 1, 2]);
    }
    #[test]
    fn cancelling_notify_removes_upcoming_and_ready_commands_only() {
        let mut scheduler = CommandScheduler::new();
        scheduler.enqueue(1, ScheduledCommand::new(
            vec![1], 100, BACKGROUND_PRIORITY_CLOCK,
        ).with_notify(10)).unwrap();
        // A stalled command blocks later commands in its own FIFO queue.
        // Put ready commands on a separate queue to exercise both states.
        // Use background priority so this test isolates min_clock promotion
        // from request-clock normalization and cross-queue frame ordering.
        scheduler.enqueue(2, ScheduledCommand::new(
            vec![2], 0, BACKGROUND_PRIORITY_CLOCK,
        ).with_notify(11)).unwrap();
        scheduler.enqueue(2, ScheduledCommand::new(
            vec![3], 0, BACKGROUND_PRIORITY_CLOCK,
        ).with_notify(12)).unwrap();
        scheduler.promote_ready(0);

        assert_eq!(scheduler.queued_counts(), (1, 2));
        assert!(scheduler.cancel_notify(10)); // upcoming
        assert!(scheduler.cancel_notify(11)); // ready
        assert!(!scheduler.cancel_notify(10));
        assert_eq!(scheduler.queued_counts(), (0, 1));
        let frame = scheduler.build_frame().unwrap().unwrap();
        assert_eq!(frame.notify_ids, vec![12]);
        assert!(!frame.bytes.is_empty());
    }

    #[test]
    fn equal_priority_queues_follow_pending_queue_insertion_order() {
        let mut scheduler = CommandScheduler::new();
        scheduler.enqueue(99, ScheduledCommand::new(vec![0xA1], 0, 500)).unwrap();
        scheduler.enqueue(1, ScheduledCommand::new(vec![0xB2], 0, 500)).unwrap();
        scheduler.promote_ready(0);

        let first = scheduler.build_frame().unwrap().unwrap();
        let mut decoder = FrameDecoder::new();
        let parsed = decoder.push(&first.bytes);
        assert_eq!(parsed[0].payload, vec![0xA1, 0xB2]);

        scheduler.enqueue(1, ScheduledCommand::new(vec![0xC3], 0, 500)).unwrap();
        scheduler.enqueue(99, ScheduledCommand::new(vec![0xD4], 0, 500)).unwrap();
        scheduler.promote_ready(0);
        let second = scheduler.build_frame().unwrap().unwrap();
        let parsed = decoder.push(&second.bytes);
        assert_eq!(parsed[0].payload, vec![0xC3, 0xD4]);
    }

    #[test]
    fn frame_packing_keeps_background_commands_after_finite_req_clock() {
        let mut scheduler = CommandScheduler::new();
        scheduler.enqueue(1, ScheduledCommand::new(
            vec![0xA1], 0, BACKGROUND_PRIORITY_CLOCK,
        )).unwrap();
        scheduler.enqueue(2, ScheduledCommand::new(
            vec![0xB2], 0, 500,
        )).unwrap();
        scheduler.promote_ready(0);

        let frame = scheduler.build_frame().unwrap().unwrap();
        let mut decoder = FrameDecoder::new();
        let parsed = decoder.push(&frame.bytes);
        assert_eq!(parsed.len(), 1);
        // serialqueue.c uses the moving background clock for wake-up timing,
        // but raw req_clock ordering during frame construction remains MAX_CLOCK.
        assert_eq!(parsed[0].payload, vec![0xB2, 0xA1]);
    }

    #[test]
    fn moving_background_clock_only_affects_send_wakeup_priority() {
        let mut scheduler = CommandScheduler::new();
        scheduler.enqueue(1, ScheduledCommand::new(
            vec![0xA1], 0, BACKGROUND_PRIORITY_CLOCK,
        )).unwrap();
        scheduler.enqueue(2, ScheduledCommand::new(
            vec![0xB2], 0, 150,
        )).unwrap();
        scheduler.promote_ready(0);
        assert_eq!(scheduler.minimum_ready_request_clock(100), Some(100));
        assert_eq!(scheduler.minimum_ready_request_clock(200), Some(150));
    }

    #[test]
    fn frame_contains_notifications_for_commands_packed_into_it() {
        let mut s = CommandScheduler::new();
        s.enqueue(4, ScheduledCommand::new(vec![0x22], 0, 1).with_notify(99)).unwrap();
        s.promote_ready(0);
        let frame = s.build_frame().unwrap().unwrap();
        assert_eq!(frame.notify_ids, vec![99]);
        assert_eq!(frame.send_sequence, 1);
    }
    #[test]
    fn does_not_exceed_mcu_payload_limit() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![1; 40], 0, 1)).unwrap();
        s.enqueue(2, ScheduledCommand::new(vec![2; 30], 0, 2)).unwrap();
        s.promote_ready(0);
        let frame = s.build_frame().unwrap().unwrap();
        assert!(frame.bytes[0] as usize <= MESSAGE_MAX);
        assert_eq!(s.queued_counts(), (0, 1));
    }
    #[test]
    fn cumulative_ack_retires_only_confirmed_frames() {
        let mut s = CommandScheduler::new();
        for id in 0..3 {
            s.enqueue(id, ScheduledCommand::new(vec![id as u8 + 1; 50], 0, id)).unwrap();
        }
        s.promote_ready(0);
        let first = s.build_frame().unwrap().unwrap();
        let second = s.build_frame().unwrap().unwrap();
        assert_eq!(s.sent_blocks.len(), 2);
        let outstanding = s.need_ack_bytes;
        s.acknowledge(2, first.bytes.len());
        assert_eq!(s.sent_blocks.len(), 1);
        assert_eq!(s.need_ack_bytes, outstanding - first.bytes.len());
        assert_eq!(s.sent_blocks.front().unwrap().0, second.send_sequence);
    }


    #[test]
    fn integrated_scheduler_tracks_frames_and_returns_original_bytes_for_retry() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![0x31, 0x32], 0, 1)).unwrap();
        s.promote_ready(0);
        let frame = s.build_frame().unwrap().unwrap();
        s.mark_frame_sent(&frame, 10.0).unwrap();
        assert_eq!(s.retransmit_state().outstanding_len(), 1);
        assert!(s.poll_retransmissions(10.249).unwrap().is_empty());
        let retries = s.poll_retransmissions(10.250).unwrap();
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].sequence, frame.send_sequence);
        assert_eq!(retries[0].bytes, frame.bytes);
        assert_eq!(retries[0].transmissions, 2);
        let ack = s.acknowledge_at(frame.send_sequence + 1, frame.bytes.len(), 10.300).unwrap();
        assert_eq!(ack.acknowledged.len(), 1);
        assert!(s.retransmit_state().is_empty());
    }

    #[test]
    fn cancelling_response_notify_after_send_preserves_uart_retransmission() {
        let mut scheduler = CommandScheduler::new();
        scheduler.enqueue(
            1,
            ScheduledCommand::new(vec![0x41, 0x42], 0, 1).with_notify(900),
        ).unwrap();
        scheduler.promote_ready(0);

        let sent = scheduler.build_frame().unwrap().unwrap();
        scheduler.mark_frame_sent(&sent, 1.0).unwrap();
        assert_eq!(sent.notify_ids, vec![900]);
        assert_eq!(scheduler.retransmit_state().outstanding_len(), 1);

        // Once packed and written, cancelling the application response
        // notification must not erase the independent UART frame retry state.
        assert!(!scheduler.cancel_notify(900));
        assert_eq!(scheduler.retransmit_state().outstanding_len(), 1);
        let retry = scheduler.poll_retransmissions(1.250).unwrap();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].bytes, sent.bytes);

        let ack = scheduler.acknowledge_at(
            sent.send_sequence + 1,
            sent.bytes.len(),
            1.300,
        ).unwrap();
        assert_eq!(ack.acknowledged.len(), 1);
        assert!(scheduler.retransmit_state().is_empty());
    }

    #[test]
    fn incoming_empty_frame_triggers_nak_after_first_ack() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![0x41], 0, 1)).unwrap();
        s.promote_ready(0);
        let sent = s.build_frame().unwrap().unwrap();
        s.mark_frame_sent(&sent, 1.0).unwrap();
        let ack = Frame { sequence: 1, payload: vec![] };
        let first = s.handle_received_frame(&ack, 1.01).unwrap();
        assert!(first.retransmissions.is_empty());
        let duplicate = s.handle_received_frame(&ack, 1.02).unwrap();
        assert_eq!(duplicate.retransmissions.len(), 1);
        assert_eq!(duplicate.retransmissions[0].bytes, sent.bytes);
        let repeated = s.handle_received_frame(&ack, 1.03).unwrap();
        assert!(repeated.retransmissions.is_empty());
    }

    #[test]
    fn first_mcu_frame_can_synchronize_initial_sequence() {
        let mut s = CommandScheduler::new();
        let first = Frame { sequence: 4, payload: vec![0x55] };
        let received = s.handle_received_frame(&first, 0.01).unwrap();
        assert_eq!(received.receive_sequence, 4);
        assert!(received.has_payload);
        assert_eq!(s.receive_sequence(), 4);
        assert_eq!(s.send_sequence(), 4);
    }

    #[test]
    fn sequence_ahead_of_sent_window_is_rejected_after_initial_sync() {
        let mut s = CommandScheduler::new();
        let first = Frame { sequence: 4, payload: vec![0x55] };
        s.handle_received_frame(&first, 0.01).unwrap();
        let invalid = Frame { sequence: 5, payload: vec![0x66] };
        assert!(matches!(
            s.handle_received_frame(&invalid, 0.02),
            Err(ScheduleError::InvalidAckSequence { received: 5, sent: 4 })
        ));
    }

    #[test]
    fn only_empty_ack_advances_last_ack_sequence_for_window_accounting() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![0x41], 0, 1)).unwrap();
        s.promote_ready(0);
        let sent = s.build_frame().unwrap().unwrap();

        let data = Frame { sequence: 2, payload: vec![0x55] };
        s.handle_received_frame(&data, 0.01).unwrap();
        assert_eq!(s.last_ack_sequence, 0);
        assert_eq!(s.last_ack_bytes, sent.bytes.len());

        let empty_ack = Frame { sequence: 2, payload: vec![] };
        s.handle_received_frame(&empty_ack, 0.02).unwrap();
        assert_eq!(s.last_ack_sequence, 2);
    }

    #[test]
    fn data_ack_keeps_last_frame_charged_until_empty_ack_releases_window() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![0x41], 0, 1)).unwrap();
        s.promote_ready(0);
        let first = s.build_frame().unwrap().unwrap();

        s.enqueue(1, ScheduledCommand::new(vec![0x42], 0, 2)).unwrap();
        s.promote_ready(0);
        let second = s.build_frame().unwrap().unwrap();

        s.enqueue(1, ScheduledCommand::new(vec![0x43], 0, 3)).unwrap();
        s.promote_ready(0);
        s.set_receive_window(MESSAGE_MAX + second.bytes.len());

        let data_ack = Frame { sequence: 2, payload: vec![0x55] };
        s.handle_received_frame(&data_ack, 0.01).unwrap();
        assert_eq!(s.need_ack_bytes, second.bytes.len());
        assert_eq!(s.last_ack_bytes, first.bytes.len());
        assert_eq!(s.last_ack_sequence, 0);
        assert!(s.build_frame().unwrap().is_none());

        let empty_ack = Frame { sequence: 2, payload: Vec::new() };
        s.handle_received_frame(&empty_ack, 0.02).unwrap();
        assert_eq!(s.last_ack_sequence, 2);
        assert!(s.build_frame().unwrap().is_some());
    }

    #[test]
    fn cumulative_data_ack_charges_only_the_last_acked_block() {
        let mut s = CommandScheduler::new();
        let mut frames = Vec::new();
        for value in 0x51..=0x53 {
            s.enqueue(1, ScheduledCommand::new(vec![value], 0, value as u64)).unwrap();
            s.promote_ready(0);
            frames.push(s.build_frame().unwrap().unwrap());
        }

        let data_ack = Frame { sequence: 3, payload: vec![0x55] };
        s.handle_received_frame(&data_ack, 0.01).unwrap();
        assert_eq!(s.need_ack_bytes, frames[2].bytes.len());
        assert_eq!(s.last_ack_bytes, frames[1].bytes.len());
        assert_eq!(s.last_ack_sequence, 0);
        assert_eq!(s.sent_blocks.len(), 1);
        assert_eq!(s.sent_blocks.front().unwrap().0, frames[2].send_sequence);
    }

    #[test]
    fn data_response_uses_last_acknowledged_frame_timestamp() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![0x61], 0, 1)).unwrap();
        s.promote_ready(0);
        let sent = s.build_frame().unwrap().unwrap();
        s.mark_frame_sent(&sent, 1.0).unwrap();

        let response = Frame { sequence: 2, payload: vec![0x55] };
        let first = s.handle_received_frame(&response, 1.5).unwrap();
        assert_eq!(first.last_receive_sent_time, 1.0);
        assert_eq!(first.response_sent_time, 1.0);

        let next = Frame { sequence: 2, payload: vec![0x56] };
        let second = s.handle_received_frame(&next, 1.6).unwrap();
        assert_eq!(second.last_receive_sent_time, 1.0);
        assert_eq!(second.response_sent_time, 1.0);
    }

    #[test]
    fn requeued_retry_is_eligible_at_the_existing_ack_clock() {
        let mut scheduler = CommandScheduler::new();
        let command = ScheduledCommand::new(vec![0x71], 100, 200);

        // The first attempt becomes eligible when the MCU clock reaches 100.
        scheduler.enqueue(7, command.clone()).unwrap();
        assert_eq!(scheduler.promote_ready(100), None);
        let first = scheduler.build_frame().unwrap().unwrap();
        assert_eq!(first.notify_ids, Vec::<u64>::new());

        // A response retry reuses the original absolute clock constraints.
        // It must not need a new clock update to become eligible.
        scheduler.enqueue(7, command).unwrap();
        assert_eq!(scheduler.promote_ready(100), None);
        let retry = scheduler.build_frame().unwrap().unwrap();
        assert_eq!(retry.bytes.len(), first.bytes.len());
        assert_eq!(retry.send_sequence, first.send_sequence + 1);
    }

    #[test]
    fn pending_block_limit_stops_additional_transmission() {
        let mut s = CommandScheduler::new();
        s.send_sequence = 13; s.receive_sequence = 1;
        s.enqueue(1, ScheduledCommand::new(vec![1], 0, 0)).unwrap();
        s.promote_ready(0);
        assert!(s.build_frame().unwrap().is_none());
    }
}
