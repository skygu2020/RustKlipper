//! Name/oid based routing for decoded MCU responses.
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    pub name: String,
    pub oid: Option<u32>,
    pub params: BTreeMap<String, Value>,
    pub sent_time: f64,
    pub receive_time: f64,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResponseKey { name: String, oid: Option<u32> }
#[derive(Debug)]
struct PendingResponse { id: u64, claimed: bool, sender: mpsc::Sender<Response> }
#[derive(Debug, Default)]
struct RouterState {
    next_waiter_id: u64,
    pending: HashMap<ResponseKey, VecDeque<PendingResponse>>,
}

/// Cloneable response router. Register a waiter before sending its command.
#[derive(Clone, Debug, Default)]
pub struct ResponseRouter { state: Arc<Mutex<RouterState>> }
#[derive(Debug)]
pub struct ResponseWaiter {
    receiver: mpsc::Receiver<Response>,
    state: Arc<Mutex<RouterState>>,
    key: ResponseKey,
    id: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError { Timeout, Disconnected }

impl ResponseRouter {
    pub fn new() -> Self { Self::default() }
    pub fn register(&self, name: impl Into<String>, oid: Option<u32>) -> ResponseWaiter {
        let key = ResponseKey { name: name.into(), oid };
        let (tx, rx) = mpsc::channel();
        let mut state = self.state.lock().expect("response router poisoned");
        state.next_waiter_id = state.next_waiter_id.wrapping_add(1).max(1);
        let id = state.next_waiter_id;
        state.pending.entry(key.clone()).or_default().push_back(PendingResponse { id, claimed: false, sender: tx });
        ResponseWaiter { receiver: rx, state: self.state.clone(), key, id }
    }
    /// Dispatch to the oldest waiter matching the exact name and oid.
    pub fn dispatch(&self, response: Response) -> bool {
        let key = ResponseKey { name: response.name.clone(), oid: response.oid };
        let mut state = self.state.lock().expect("response router poisoned");
        let Some(queue) = state.pending.get_mut(&key) else { return false; };
        // A waiter may have timed out or been dropped. Skip disconnected
        // receivers without allowing one stale waiter to block later FIFO waiters.
        let mut delivered = false;
        while let Some(waiter) = queue.pop_front() {
            if waiter.sender.send(response.clone()).is_ok() {
                delivered = true;
                break;
            }
        }
        if queue.is_empty() {
            state.pending.remove(&key);
        }
        delivered
    }

    /// Dispatch a response to the exact waiter reserved for the request that
    /// was actually sent. This matters when scheduler queue order differs from
    /// waiter registration order for requests sharing the same response key.
    pub fn dispatch_to_waiter(&self, response: Response, waiter_id: u64) -> bool {
        let key = ResponseKey { name: response.name.clone(), oid: response.oid };
        let mut state = self.state.lock().expect("response router poisoned");
        let Some(queue) = state.pending.get_mut(&key) else { return false; };
        let Some(index) = queue.iter().position(|waiter| waiter.id == waiter_id) else {
            return false;
        };
        let waiter = queue.remove(index).expect("located response waiter must exist");
        let delivered = waiter.sender.send(response).is_ok();
        if queue.is_empty() {
            state.pending.remove(&key);
        }
        delivered
    }
    /// Associate the oldest unclaimed waiter with a queued request.
    /// The returned ID lets timeout cleanup target this exact waiter even when
    /// other requests with the same response name and OID remain active.
    pub fn claim_oldest(&self, name: &str, oid: Option<u32>) -> Option<u64> {
        let key = ResponseKey { name: name.to_owned(), oid };
        let mut state = self.state.lock().expect("response router poisoned");
        let queue = state.pending.get_mut(&key)?;
        let waiter = queue.iter_mut().find(|waiter| !waiter.claimed)?;
        waiter.claimed = true;
        Some(waiter.id)
    }

    /// Remove one exact waiter by its stable registration ID.
    pub fn cancel_waiter(&self, name: &str, oid: Option<u32>, id: u64) -> bool {
        let key = ResponseKey { name: name.to_owned(), oid };
        let mut state = self.state.lock().expect("response router poisoned");
        let Some(queue) = state.pending.get_mut(&key) else { return false; };
        let before = queue.len();
        queue.retain(|waiter| waiter.id != id);
        let removed = queue.len() != before;
        if queue.is_empty() {
            state.pending.remove(&key);
        }
        removed
    }

    /// Check whether one exact waiter is still registered. Group counts are
    /// insufficient when multiple requests share the same response key.
    pub fn contains_waiter(&self, name: &str, oid: Option<u32>, id: u64) -> bool {
        let key = ResponseKey { name: name.to_owned(), oid };
        self.state.lock().expect("response router poisoned").pending
            .get(&key)
            .is_some_and(|queue| queue.iter().any(|waiter| waiter.id == id))
    }

    pub fn pending_count(&self, name: &str, oid: Option<u32>) -> usize {
        self.state.lock().expect("response router poisoned").pending
            .get(&ResponseKey { name: name.into(), oid }).map_or(0, VecDeque::len)
    }
}
impl ResponseWaiter {
    /// Explicitly remove this waiter from the router without waiting for a response.
    pub fn cancel(self) {}

    pub fn wait(self, timeout: Duration) -> Result<Response, WaitError> {
        self.receiver.recv_timeout(timeout).map_err(|e| match e {
            mpsc::RecvTimeoutError::Timeout => WaitError::Timeout,
            mpsc::RecvTimeoutError::Disconnected => WaitError::Disconnected,
        })
    }
    pub fn try_recv(&self) -> Result<Option<Response>, WaitError> {
        match self.receiver.try_recv() {
            Ok(r) => Ok(Some(r)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(WaitError::Disconnected),
        }
    }
}
impl Drop for ResponseWaiter {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("response router poisoned");
        if let Some(queue) = state.pending.get_mut(&self.key) {
            queue.retain(|waiter| waiter.id != self.id);
            if queue.is_empty() {
                state.pending.remove(&self.key);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn response(name: &str, oid: Option<u32>) -> Response {
        Response { name: name.into(), oid, params: BTreeMap::new(), sent_time: 1.0, receive_time: 2.0 }
    }
    #[test]
    fn routes_by_name_and_oid_fifo() {
        let router = ResponseRouter::new();
        let first = router.register("config", Some(7));
        let second = router.register("config", Some(7));
        let other = router.register("config", Some(8));
        assert!(router.dispatch(response("config", Some(7))));
        assert!(router.dispatch(response("config", Some(7))));
        assert!(router.dispatch(response("config", Some(8))));
        assert_eq!(first.wait(Duration::from_millis(1)).unwrap().oid, Some(7));
        assert_eq!(second.wait(Duration::from_millis(1)).unwrap().oid, Some(7));
        assert_eq!(other.wait(Duration::from_millis(1)).unwrap().oid, Some(8));
    }

    #[test]
    fn dispatch_to_waiter_preserves_request_identity_when_send_order_differs() {
        let router = ResponseRouter::new();
        let registered_first = router.register("status", Some(5));
        let sent_first = router.register("status", Some(5));
        let registered_first_id = router.claim_oldest("status", Some(5)).unwrap();
        let sent_first_id = router.claim_oldest("status", Some(5)).unwrap();

        let mut params = BTreeMap::new();
        params.insert("sequence".into(), Value::from(1));
        assert!(router.dispatch_to_waiter(Response {
            name: "status".into(), oid: Some(5), params,
            sent_time: 1.0, receive_time: 2.0,
        }, sent_first_id));

        assert_eq!(
            sent_first.wait(Duration::from_millis(1)).unwrap().params["sequence"],
            Value::from(1),
        );
        assert_eq!(router.pending_count("status", Some(5)), 1);
        assert!(router.contains_waiter("status", Some(5), registered_first_id));
        drop(registered_first);
    }

    #[test]
    fn dispatch_to_missing_waiter_does_not_consume_another_registration() {
        let router = ResponseRouter::new();
        let abandoned = router.register("status", Some(6));
        let live = router.register("status", Some(6));
        let claimed_id = router.claim_oldest("status", Some(6)).unwrap();
        assert!(router.cancel_waiter("status", Some(6), claimed_id));

        assert!(!router.dispatch_to_waiter(response("status", Some(6)), claimed_id));
        assert_eq!(router.pending_count("status", Some(6)), 1);
        assert!(router.dispatch(response("status", Some(6))));
        assert_eq!(live.wait(Duration::from_millis(1)).unwrap().oid, Some(6));
        assert_eq!(abandoned.wait(Duration::from_millis(1)), Err(WaitError::Disconnected));
    }

    #[test]
    fn response_router_keeps_fifo_waiters_for_same_name_and_oid() {
        let router = ResponseRouter::new();
        let first = router.register("config", Some(7));
        let second = router.register("config", Some(7));
        let mut params = BTreeMap::new();
        params.insert("oid".into(), Value::from(7));
        assert!(router.dispatch(Response {
            name: "config".into(), oid: Some(7), params: params.clone(),
            sent_time: 1.25, receive_time: 2.0,
        }));
        assert!(router.dispatch(Response {
            name: "config".into(), oid: Some(7), params,
            sent_time: 1.75, receive_time: 2.1,
        }));
        assert_eq!(first.wait(std::time::Duration::from_millis(1)).unwrap().sent_time, 1.25);
        assert_eq!(second.wait(std::time::Duration::from_millis(1)).unwrap().sent_time, 1.75);
    }

    #[test]
    fn dropping_waiter_removes_pending_registration() {
        let router = ResponseRouter::new();
        let waiter = router.register("config", Some(12));
        assert_eq!(router.pending_count("config", Some(12)), 1);
        drop(waiter);
        assert_eq!(router.pending_count("config", Some(12)), 0);
        assert!(!router.dispatch(response("config", Some(12))));
    }

    #[test]
    fn dropped_waiter_does_not_block_next_fifo_waiter() {
        let router = ResponseRouter::new();
        let dropped = router.register("config", Some(9));
        let live = router.register("config", Some(9));
        drop(dropped);
        assert!(router.dispatch(response("config", Some(9))));
        assert_eq!(live.wait(Duration::from_millis(1)).unwrap().oid, Some(9));
        assert_eq!(router.pending_count("config", Some(9)), 0);
    }

    #[test]
    fn cancelling_specific_claimed_waiter_preserves_other_fifo_waiter() {
        let router = ResponseRouter::new();
        let failed = router.register("status", Some(4));
        let next = router.register("status", Some(4));
        let failed_id = router.claim_oldest("status", Some(4)).unwrap();
        let next_id = router.claim_oldest("status", Some(4)).unwrap();
        assert!(router.cancel_waiter("status", Some(4), failed_id));
        assert!(router.contains_waiter("status", Some(4), next_id));
        assert_eq!(failed.wait(Duration::from_millis(1)), Err(WaitError::Disconnected));
        assert_eq!(router.pending_count("status", Some(4)), 1);
        assert!(router.dispatch(response("status", Some(4))));
        assert_eq!(next.wait(Duration::from_millis(1)).unwrap().oid, Some(4));
    }

    #[test]
    fn exact_waiter_presence_is_independent_of_same_key_waiters() {
        let router = ResponseRouter::new();
        let first = router.register("status", Some(4));
        let second = router.register("status", Some(4));
        let first_id = router.claim_oldest("status", Some(4)).unwrap();
        let second_id = router.claim_oldest("status", Some(4)).unwrap();

        assert!(router.contains_waiter("status", Some(4), first_id));
        drop(first);
        assert!(!router.contains_waiter("status", Some(4), first_id));
        assert!(router.contains_waiter("status", Some(4), second_id));
        assert_eq!(router.pending_count("status", Some(4)), 1);
        drop(second);
    }

    #[test]
    fn unmatched_oid_does_not_consume_waiter() {
        let router = ResponseRouter::new();
        let waiter = router.register("identify_response", None);
        assert!(!router.dispatch(response("identify_response", Some(2))));
        assert_eq!(router.pending_count("identify_response", None), 1);
        assert!(router.dispatch(response("identify_response", None)));
        assert!(waiter.wait(Duration::from_millis(1)).is_ok());
    }
}
