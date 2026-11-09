//! ACK sequence tracking for serialqueue notification IDs.
use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AckEvent { pub sent_time: f64, pub receive_time: f64 }
#[derive(Debug)]
struct PendingAck { required_send_seq: u64, sender: Sender<AckEvent> }

/// Tracks notify IDs against extended (not four-bit wire) send sequence numbers.
#[derive(Debug, Default)]
pub struct AckTracker { next_id: u64, pending: BTreeMap<u64, PendingAck> }
impl AckTracker {
    pub fn new() -> Self { Self::default() }
    pub fn register(&mut self, required_send_seq: u64) -> (u64, Receiver<AckEvent>) {
        self.next_id = self.next_id.wrapping_add(1).max(1);
        let id = self.next_id;
        let (tx, rx) = mpsc::channel();
        self.pending.insert(id, PendingAck { required_send_seq, sender: tx });
        (id, rx)
    }
    /// Klipper wake_seq is receive_seq - 1 for an empty ACK, - 2 for data.
    pub fn acknowledge(&mut self, receive_seq: u64, has_payload: bool,
                       sent_time: f64, receive_time: f64) -> Vec<u64> {
        let wake_seq = receive_seq.saturating_sub(if has_payload { 2 } else { 1 });
        let completed: Vec<u64> = self.pending.iter()
            .filter_map(|(id, p)| (p.required_send_seq <= wake_seq).then_some(*id)).collect();
        for id in &completed {
            if let Some(p) = self.pending.remove(id) {
                let _ = p.sender.send(AckEvent { sent_time, receive_time });
            }
        }
        completed
    }
    pub fn cancel_all(&mut self) -> usize {
        let count = self.pending.len(); self.pending.clear(); count
    }
    pub fn pending_count(&self) -> usize { self.pending.len() }
    pub fn extend_wire_sequence(previous: u64, wire_sequence: u8) -> u64 {
        let delta = (u64::from(wire_sequence & 0x0f) - (previous & 0x0f)) & 0x0f;
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
