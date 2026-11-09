//! Clock-aware command queue scheduling modeled on Klipper serialqueue.c.
//! This layer decides command eligibility and packs commands into MCU frames;
//! UART I/O, ACK/retransmit state, and clock estimation remain separate.
use std::collections::{BTreeMap, VecDeque};
use klipper_protocol::{encode_frame, ProtocolError, MESSAGE_MAX, MESSAGE_MIN};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError { EmptyCommand, CommandTooLarge(usize), Frame(ProtocolError), Retransmit(RetransmitError) }
impl From<RetransmitError> for ScheduleError {
    fn from(value: RetransmitError) -> Self { Self::Retransmit(value) }
}
impl From<ProtocolError> for ScheduleError {
    fn from(value: ProtocolError) -> Self { Self::Frame(value) }
}
#[derive(Debug, Default)]
struct QueueState { upcoming: VecDeque<ScheduledCommand>, ready: VecDeque<ScheduledCommand> }

#[derive(Debug)]
pub struct CommandScheduler {
    queues: BTreeMap<u64, QueueState>,
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
        Self { queues: BTreeMap::new(), send_sequence: 1, receive_sequence: 1,
            receive_window: 0, need_ack_bytes: 0, last_ack_bytes: 0, last_ack_sequence: 0, sent_blocks: VecDeque::new(), retransmit: RetransmitState::new() }
    }
}
impl CommandScheduler {
    pub fn new() -> Self { Self::default() }
    pub fn set_receive_window(&mut self, bytes: usize) { self.receive_window = bytes; }
    pub fn send_sequence(&self) -> u64 { self.send_sequence }
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
            && command.min_clock.wrapping_add(3u64 << 29) < command.req_clock
        {
            command.min_clock = command.req_clock - (3u64 << 29);
        }
        self.queues.entry(queue_id).or_default().upcoming.push_back(command);
        Ok(())
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

    /// Pack a frame without registering retransmission timing. Prefer build_frame_at
    /// in a live transport so every transmitted frame is tracked for retry.
    pub fn build_frame(&mut self) -> Result<Option<ScheduledFrame>, ScheduleError> {
        self.build_frame_inner(None)
    }

    /// Pack and register a frame at the caller's monotonic host timestamp (seconds).
    pub fn build_frame_at(&mut self, now: f64) -> Result<Option<ScheduledFrame>, ScheduleError> {
        self.retransmit.check_time(now)?;
        self.build_frame_inner(Some(now))
    }

    fn build_frame_inner(&mut self, now: Option<f64>) -> Result<Option<ScheduledFrame>, ScheduleError> {
        if self.send_sequence.saturating_sub(self.receive_sequence) >= MAX_PENDING_BLOCKS
            && self.receive_sequence != u64::MAX
        { return Ok(None); }
        if self.send_sequence > self.receive_sequence && self.receive_window > 0 {
            let mut need = self.need_ack_bytes + MESSAGE_MAX;
            if self.last_ack_sequence < self.receive_sequence { need += self.last_ack_bytes; }
            if need > self.receive_window { return Ok(None); }
        }
        let mut payload = Vec::with_capacity(MESSAGE_PAYLOAD_MAX);
        let mut notify_ids = Vec::new();
        loop {
            let selected = self.queues.iter()
                .filter_map(|(id, q)| q.ready.front().map(|cmd| {
                    (*id, cmd.req_clock, cmd.bytes.len())
                }))
                .min_by_key(|(_, priority, _)| *priority);
            let Some((queue_id, _, size)) = selected else { break; };
            if payload.len() + size > MESSAGE_PAYLOAD_MAX { break; }
            let command = self.queues.get_mut(&queue_id)
                .and_then(|q| q.ready.pop_front()).expect("selected queue has a ready command");
            payload.extend_from_slice(&command.bytes);
            if let Some(id) = command.notify_id { notify_ids.push(id); }
        }
        if payload.is_empty() { return Ok(None); }
        let seq = self.send_sequence;
        let bytes = encode_frame((seq as u8) & 0x0f, &payload)?;
        if let Some(now) = now {
            self.retransmit.track_sent(seq, bytes.clone(), now)?;
        }
        self.send_sequence = self.send_sequence.wrapping_add(1);
        self.need_ack_bytes = self.need_ack_bytes.saturating_add(bytes.len());
        self.sent_blocks.push_back((seq, bytes.len()));
        Ok(Some(ScheduledFrame { send_sequence: seq, bytes, notify_ids }))
    }

    /// Update state after the caller extends an MCU four-bit sequence.
    pub fn acknowledge(&mut self, receive_sequence: u64, acknowledged_frame_bytes: usize) {
        if receive_sequence <= self.receive_sequence { return; }
        self.receive_sequence = receive_sequence;
        self.last_ack_sequence = receive_sequence;
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
        Ok(self.retransmit.nak(now)?)
    }

    /// Poll timeout and return the complete outstanding window for retransmission.
    pub fn poll_retransmissions(&mut self, now: f64)
        -> Result<Vec<Retransmission>, ScheduleError>
    {
        Ok(self.retransmit.poll_timeout_all(now)?)
    }

    /// Poll the oldest unacknowledged frame's retransmission deadline.
    pub fn poll_retransmission(&mut self, now: f64)
        -> Result<Option<Retransmission>, ScheduleError>
    {
        Ok(self.retransmit.poll_timeout(now)?)
    }

    pub fn retransmit_state(&self) -> &RetransmitState { &self.retransmit }

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
        let frame = s.build_frame_at(10.0).unwrap().unwrap();
        assert_eq!(s.retransmit_state().outstanding_len(), 1);
        assert!(s.poll_retransmission(10.249).unwrap().is_none());
        let retry = s.poll_retransmission(10.250).unwrap().unwrap();
        assert_eq!(retry.sequence, frame.send_sequence);
        assert_eq!(retry.bytes, frame.bytes);
        assert_eq!(retry.transmissions, 2);
        let ack = s.acknowledge_at(frame.send_sequence + 1, frame.bytes.len(), 10.300).unwrap();
        assert_eq!(ack.acknowledged.len(), 1);
        assert!(s.retransmit_state().is_empty());
    }

    #[test]
    fn duplicate_ack_can_request_fast_retransmit_through_scheduler() {
        let mut s = CommandScheduler::new();
        s.enqueue(1, ScheduledCommand::new(vec![0x41], 0, 1)).unwrap();
        s.promote_ready(0);
        let frame = s.build_frame_at(1.0).unwrap().unwrap();
        for t in [1.01, 1.02] {
            let ack = s.acknowledge_at(frame.send_sequence, 0, t).unwrap();
            assert!(ack.fast_retransmit.is_none());
        }
        let ack = s.acknowledge_at(frame.send_sequence, 0, 1.03).unwrap();
        assert_eq!(ack.fast_retransmit.unwrap().bytes, frame.bytes);
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
