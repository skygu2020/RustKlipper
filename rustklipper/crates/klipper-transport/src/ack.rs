//! ACK sequence tracking for serialqueue notification IDs.
use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AckEvent { pub sent_time: f64, pub receive_time: f64 }
#[derive(Debug)]
struct PendingAck { required_send_seq: Option<u64>, sent_time: Option<f64>, sender: Sender<AckEvent> }

/// Tracks notify IDs against extended (not four-bit wire) send sequence numbers.
#[derive(Debug, Default)]
pub struct AckTracker { next_id: u64, pending: BTreeMap<u64, PendingAck> }
impl AckTracker {
    pub fn new() -> Self { Self::default() }
    pub fn register(&mut self, required_send_seq: u64) -> (u64, Receiver<AckEvent>) {
        loop {
            self.next_id = self.next_id.wrapping_add(1).max(1);
            if !self.pending.contains_key(&self.next_id) { break; }
        }
        let id = self.next_id;
        let (tx, rx) = mpsc::channel();
        self.pending.insert(id, PendingAck { required_send_seq: Some(required_send_seq), sent_time: None, sender: tx });
        (id, rx)
    }
    /// Reserve a caller-selected notify ID before enqueueing its command.
    /// The actual sequence and send timestamp are attached only after UART write succeeds.
    pub fn register_notify(&mut self, id: u64) -> Option<Receiver<AckEvent>> {
        if self.pending.contains_key(&id) { return None; }
        let (tx, rx) = mpsc::channel();
        self.pending.insert(id, PendingAck { required_send_seq: None, sent_time: None, sender: tx });
        Some(rx)
    }

    pub fn mark_sent(&mut self, id: u64, required_send_seq: u64, sent_time: f64) -> bool {
        let Some(pending) = self.pending.get_mut(&id) else { return false; };
        if pending.required_send_seq.is_some() { return false; }
        pending.required_send_seq = Some(required_send_seq);
        pending.sent_time = Some(sent_time);
        true
    }

    /// Klipper wake_seq is receive_seq - 1 for an empty ACK, - 2 for data.
    pub fn acknowledge_events(&mut self, receive_seq: u64, has_payload: bool,
                              fallback_sent_time: f64, receive_time: f64) -> Vec<(u64, AckEvent)> {
        let wake_seq = receive_seq.saturating_sub(if has_payload { 2 } else { 1 });
        let completed: Vec<u64> = self.pending.iter()
            .filter_map(|(id, p)| p.required_send_seq
                .is_some_and(|seq| seq <= wake_seq).then_some(*id)).collect();
        let mut events = Vec::with_capacity(completed.len());
        for id in completed {
            if let Some(p) = self.pending.remove(&id) {
                let event = AckEvent {
                    sent_time: p.sent_time.unwrap_or(fallback_sent_time),
                    receive_time,
                };
                let _ = p.sender.send(event);
                events.push((id, event));
            }
        }
        events
    }

    pub fn acknowledge(&mut self, receive_seq: u64, has_payload: bool,
                       sent_time: f64, receive_time: f64) -> Vec<u64> {
        self.acknowledge_events(receive_seq, has_payload, sent_time, receive_time)
            .into_iter().map(|(id, _)| id).collect()
    }
    pub fn cancel_notify(&mut self, id: u64) -> bool { self.pending.remove(&id).is_some() }
    pub fn cancel_all(&mut self) -> usize {
        let count = self.pending.len(); self.pending.clear(); count
    }
    pub fn pending_count(&self) -> usize { self.pending.len() }
    pub fn contains_notify(&self, id: u64) -> bool { self.pending.contains_key(&id) }
    pub fn extend_wire_sequence(previous: u64, wire_sequence: u8) -> u64 {
        let delta = u64::from(wire_sequence & 0x0f).wrapping_sub(previous & 0x0f) & 0x0f;
        previous + delta
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn data_response_has_one_more_sequence_slot_than_empty_ack() {
        let mut tracker = AckTracker::new();
        let (_id, rx) = tracker.register(10);
        assert!(tracker.acknowledge(11, true, 1.0, 2.0).is_empty());
        assert_eq!(tracker.pending_count(), 1);
        assert_eq!(tracker.acknowledge(11, false, 1.1, 2.1).len(), 1);
        assert_eq!(rx.recv().unwrap(), AckEvent { sent_time: 1.1, receive_time: 2.1 });
    }
    #[test]
    fn notify_uses_its_own_successful_send_timestamp() {
        let mut tracker = AckTracker::new();
        let first = tracker.register_notify(41).unwrap();
        let second = tracker.register_notify(42).unwrap();
        assert!(tracker.mark_sent(41, 10, 1.25));
        assert!(tracker.mark_sent(42, 11, 1.75));
        let completed = tracker.acknowledge_events(12, false, 99.0, 2.0);
        assert_eq!(completed, vec![
            (41, AckEvent { sent_time: 1.25, receive_time: 2.0 }),
            (42, AckEvent { sent_time: 1.75, receive_time: 2.0 }),
        ]);
        assert_eq!(first.recv().unwrap().sent_time, 1.25);
        assert_eq!(second.recv().unwrap().sent_time, 1.75);
    }

    #[test]
    fn duplicate_notify_id_is_rejected() {
        let mut tracker = AckTracker::new();
        let _rx = tracker.register_notify(7).unwrap();
        assert!(tracker.register_notify(7).is_none());
    }

    #[test]
    fn extends_four_bit_sequence_across_wrap() {
        assert_eq!(AckTracker::extend_wire_sequence(14, 1), 17);
        assert_eq!(AckTracker::extend_wire_sequence(17, 2), 18);
    }
    #[test]
    fn cancel_all_drops_pending_notifications() {
        let mut tracker = AckTracker::new();
        let (_, rx) = tracker.register(3);
        assert_eq!(tracker.cancel_all(), 1);
        assert!(rx.recv().is_err());
    }
}
