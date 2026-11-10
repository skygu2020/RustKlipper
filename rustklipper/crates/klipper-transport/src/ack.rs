//! ACK sequence tracking for serialqueue notification IDs.
use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AckEvent { pub sent_time: f64, pub receive_time: f64 }
#[derive(Debug)]
struct PendingAck { required_send_seq: Option<u64>, send_order: u64, sender: Sender<AckEvent> }

/// Tracks notify IDs against extended (not four-bit wire) send sequence numbers.
#[derive(Debug, Default)]
pub struct AckTracker { next_id: u64, next_send_order: u64, pending: BTreeMap<u64, PendingAck> }
impl AckTracker {
    /// Allocate a monotonically increasing command-order key. If the counter
    /// reaches its limit, compact the still-pending sent notifications first
    /// so wraparound cannot reorder ACK delivery.
    fn allocate_send_order(&mut self) -> u64 {
        if self.next_send_order == u64::MAX {
            let mut ordered: Vec<(u64, u64)> = self.pending.iter()
                .filter_map(|(id, pending)| pending.required_send_seq
                    .map(|_| (pending.send_order, *id)))
                .collect();
            ordered.sort_unstable();
            for (order, (_, id)) in ordered.iter().enumerate() {
                if let Some(pending) = self.pending.get_mut(id) {
                    pending.send_order = order as u64;
                }
            }
            self.next_send_order = ordered.len() as u64;
        }
        let order = self.next_send_order;
        self.next_send_order += 1;
        order
    }

    pub fn new() -> Self { Self::default() }
    pub fn register(&mut self, required_send_seq: u64) -> (u64, Receiver<AckEvent>) {
        loop {
            self.next_id = self.next_id.wrapping_add(1).max(1);
            if !self.pending.contains_key(&self.next_id) { break; }
        }
        let id = self.next_id;
        let (tx, rx) = mpsc::channel();
        let send_order = self.allocate_send_order();
        self.pending.insert(id, PendingAck { required_send_seq: Some(required_send_seq), send_order, sender: tx });
        (id, rx)
    }
    /// Reserve a caller-selected notify ID before enqueueing its command.
    /// The actual sequence and send timestamp are attached only after UART write succeeds.
    pub fn register_notify(&mut self, id: u64) -> Option<Receiver<AckEvent>> {
        if self.pending.contains_key(&id) { return None; }
        let (tx, rx) = mpsc::channel();
        self.pending.insert(id, PendingAck { required_send_seq: None, send_order: u64::MAX, sender: tx });
        Some(rx)
    }

    /// Bind a reserved notification to its sent frame. `_sent_time` is retained
    /// for API compatibility; Klipper timestamps the event from the ACKed frame.
    pub fn mark_sent(&mut self, id: u64, required_send_seq: u64, _sent_time: f64) -> bool {
        let Some(pending) = self.pending.get(&id) else { return false; };
        if pending.required_send_seq.is_some() { return false; }
        let send_order = self.allocate_send_order();
        let pending = self.pending.get_mut(&id).expect("reserved notification must remain registered");
        pending.required_send_seq = Some(required_send_seq);
        pending.send_order = send_order;
        true
    }

    /// Klipper wake_seq is receive_seq - 1 for an empty ACK, - 2 for data.
    pub fn acknowledge_events(&mut self, receive_seq: u64, has_payload: bool,
                              last_receive_sent_time: f64, receive_time: f64) -> Vec<(u64, AckEvent)> {
        let wake_seq = receive_seq.saturating_sub(if has_payload { 2 } else { 1 });
        let mut completed: Vec<(u64, u64, u64)> = self.pending.iter()
            .filter_map(|(id, p)| p.required_send_seq
                .filter(|seq| *seq <= wake_seq)
                .map(|seq| (seq, p.send_order, *id))).collect();
        // Klipper's notify_queue is FIFO by frame sequence and then by command
        // order within that frame; notify IDs are caller-selected and unrelated.
        completed.sort_unstable_by_key(|(seq, order, _)| (*seq, *order));
        let mut events = Vec::with_capacity(completed.len());
        for (_, _, id) in completed {
            if let Some(p) = self.pending.remove(&id) {
                let event = AckEvent {
                    sent_time: last_receive_sent_time,
                    receive_time,
                };
                let _ = p.sender.send(event);
                events.push((id, event));
            }
        }
        events
    }

    pub fn acknowledge(&mut self, receive_seq: u64, has_payload: bool,
                       last_receive_sent_time: f64, receive_time: f64) -> Vec<u64> {
        self.acknowledge_events(receive_seq, has_payload, last_receive_sent_time, receive_time)
            .into_iter().map(|(id, _)| id).collect()
    }
    pub fn cancel_notify(&mut self, id: u64) -> bool { self.pending.remove(&id).is_some() }
    pub fn cancel_all(&mut self) -> usize {
        let count = self.pending.len(); self.pending.clear(); count
    }
    pub fn pending_count(&self) -> usize { self.pending.len() }
    pub fn contains_notify(&self, id: u64) -> bool { self.pending.contains_key(&id) }
    pub fn extend_wire_sequence(previous: u64, wire_sequence: u8) -> u64 {
        // Reconstruct the candidate in the current 16-sequence epoch, then
        // choose the nearest representation in the +/- 8 sequence window.
        // This is important for duplicate/stale ACKs: a wire sequence just
        // behind the current value must not be mistaken for one 15 steps ahead.
        let candidate = (previous & !0x0f) | u64::from(wire_sequence & 0x0f);
        if candidate < previous.saturating_sub(8) {
            candidate.saturating_add(16)
        } else if candidate > previous.saturating_add(8) {
            candidate.saturating_sub(16)
        } else {
            candidate
        }
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
    fn notify_uses_acknowledged_frame_timestamp_for_all_notifications() {
        let mut tracker = AckTracker::new();
        let first = tracker.register_notify(41).unwrap();
        let second = tracker.register_notify(42).unwrap();
        assert!(tracker.mark_sent(41, 10, 1.25));
        assert!(tracker.mark_sent(42, 11, 1.75));
        let completed = tracker.acknowledge_events(12, false, 99.0, 2.0);
        assert_eq!(completed, vec![
            (41, AckEvent { sent_time: 99.0, receive_time: 2.0 }),
            (42, AckEvent { sent_time: 99.0, receive_time: 2.0 }),
        ]);
        assert_eq!(first.recv().unwrap().sent_time, 99.0);
        assert_eq!(second.recv().unwrap().sent_time, 99.0);
    }


    #[test]
    fn cumulative_ack_emits_notifications_in_actual_send_order_not_id_order() {
        let mut tracker = AckTracker::new();
        let later_id = tracker.register_notify(2).unwrap();
        let earlier_id = tracker.register_notify(900).unwrap();
        // The higher ID is sent first; the lower ID is sent in the next frame.
        assert!(tracker.mark_sent(900, 10, 1.0));
        assert!(tracker.mark_sent(2, 11, 1.1));

        let completed = tracker.acknowledge_events(12, false, 99.0, 2.0);
        assert_eq!(completed.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![900, 2]);
        assert_eq!(earlier_id.recv().unwrap().sent_time, 99.0);
        assert_eq!(later_id.recv().unwrap().sent_time, 99.0);
    }


    #[test]
    fn notifications_in_one_frame_keep_command_order_not_notify_id_order() {
        let mut tracker = AckTracker::new();
        let later_id = tracker.register_notify(3).unwrap();
        let earlier_id = tracker.register_notify(700).unwrap();

        // Both commands share one UART frame. Their notification IDs are
        // caller-selected, so FIFO completion must use the order in which
        // the frame's notify IDs are bound after the write.
        assert!(tracker.mark_sent(700, 25, 1.0));
        assert!(tracker.mark_sent(3, 25, 1.0));

        let completed = tracker.acknowledge_events(26, false, 2.0, 2.1);
        assert_eq!(
            completed.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![700, 3],
        );
        assert!(earlier_id.recv().is_ok());
        assert!(later_id.recv().is_ok());
    }

    #[test]
    fn send_order_counter_wrap_compacts_pending_notifications_without_reordering() {
        let mut tracker = AckTracker::new();
        let later = tracker.register_notify(2).unwrap();
        let earlier = tracker.register_notify(900).unwrap();

        tracker.next_send_order = u64::MAX;
        assert!(tracker.mark_sent(900, 10, 1.0));
        assert!(tracker.mark_sent(2, 11, 1.1));

        let completed = tracker.acknowledge_events(12, false, 99.0, 2.0);
        assert_eq!(completed.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![900, 2]);
        assert!(earlier.recv().is_ok());
        assert!(later.recv().is_ok());
    }

    #[test]
    fn reserved_notification_is_not_acknowledged_until_its_frame_is_sent() {
        let mut tracker = AckTracker::new();
        let receiver = tracker.register_notify(73).unwrap();

        // A reserved ID has no frame sequence yet and must not be completed
        // by an ACK that predates its actual UART write.
        assert!(tracker.acknowledge_events(100, false, 1.0, 1.1).is_empty());
        assert!(tracker.contains_notify(73));
        assert!(receiver.try_recv().is_err());

        assert!(tracker.mark_sent(73, 100, 1.2));
        // ACK sequence 100 acknowledges frames strictly before 100.
        assert!(tracker.acknowledge_events(100, false, 1.3, 1.4).is_empty());
        assert!(tracker.contains_notify(73));

        let completed = tracker.acknowledge_events(101, false, 1.5, 1.6);
        assert_eq!(completed.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![73]);
        assert_eq!(receiver.recv().unwrap(), AckEvent {
            sent_time: 1.5,
            receive_time: 1.6,
        });
        assert_eq!(tracker.pending_count(), 0);
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
    fn stale_wire_sequence_is_extended_behind_current_sequence() {
        assert_eq!(AckTracker::extend_wire_sequence(1, 0), 0);
        assert_eq!(AckTracker::extend_wire_sequence(17, 15), 15);
        assert_eq!(AckTracker::extend_wire_sequence(8, 0), 0);
    }

    #[test]
    fn forward_wire_sequence_uses_nearest_epoch_boundary() {
        assert_eq!(AckTracker::extend_wire_sequence(0, 8), 8);
        assert_eq!(AckTracker::extend_wire_sequence(0, 9), 0);
        assert_eq!(AckTracker::extend_wire_sequence(14, 15), 15);
    }
    #[test]
    fn cancelled_notify_id_reuse_is_not_completed_by_a_late_ack() {
        let mut tracker = AckTracker::new();

        let old_receiver = tracker.register_notify(77).unwrap();
        assert!(tracker.mark_sent(77, 10, 1.0));
        assert!(tracker.cancel_notify(77));
        assert!(old_receiver.recv().is_err());

        // Reusing an ID must bind it to the new frame sequence, not the
        // abandoned request's already-written frame.
        let new_receiver = tracker.register_notify(77).unwrap();
        assert!(tracker.mark_sent(77, 11, 2.0));

        // Empty ACK for sequence 11 acknowledges frames strictly before 11.
        assert!(tracker.acknowledge_events(11, false, 3.0, 3.1).is_empty());
        assert!(tracker.contains_notify(77));

        let completed = tracker.acknowledge_events(12, false, 4.0, 4.1);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, 77);
        assert_eq!(new_receiver.recv().unwrap(), AckEvent {
            sent_time: 4.0,
            receive_time: 4.1,
        });
    }

    #[test]
    fn extended_sequence_saturates_instead_of_wrapping_at_u64_limit() {
        assert_eq!(AckTracker::extend_wire_sequence(u64::MAX - 1, 15), u64::MAX);
        assert_eq!(AckTracker::extend_wire_sequence(u64::MAX, 0), u64::MAX);
        assert_eq!(AckTracker::extend_wire_sequence(u64::MAX, 1), u64::MAX);
    }

    #[test]
    fn cancel_all_drops_pending_notifications() {
        let mut tracker = AckTracker::new();
        let (_, rx) = tracker.register(3);
        assert_eq!(tracker.cancel_all(), 1);
        assert!(rx.recv().is_err());
    }
}
