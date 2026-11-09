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
#[derive(Debug, Default)]
struct RouterState { pending: HashMap<ResponseKey, VecDeque<mpsc::Sender<Response>>> }

/// Cloneable response router. Register a waiter before sending its command.
#[derive(Clone, Debug, Default)]
pub struct ResponseRouter { state: Arc<Mutex<RouterState>> }
#[derive(Debug)]
pub struct ResponseWaiter { receiver: mpsc::Receiver<Response> }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError { Timeout, Disconnected }

impl ResponseRouter {
    pub fn new() -> Self { Self::default() }
    pub fn register(&self, name: impl Into<String>, oid: Option<u32>) -> ResponseWaiter {
        let key = ResponseKey { name: name.into(), oid };
        let (tx, rx) = mpsc::channel();
        self.state.lock().expect("response router poisoned").pending.entry(key).or_default().push_back(tx);
        ResponseWaiter { receiver: rx }
    }
    /// Dispatch to the oldest waiter matching the exact name and oid.
    pub fn dispatch(&self, response: Response) -> bool {
        let key = ResponseKey { name: response.name.clone(), oid: response.oid };
        let mut state = self.state.lock().expect("response router poisoned");
        let Some(queue) = state.pending.get_mut(&key) else { return false; };
        // A waiter may have timed out or been dropped. Skip disconnected
        // receivers without allowing one stale waiter to block later FIFO waiters.
        let mut delivered = false;
        while let Some(sender) = queue.pop_front() {
            if sender.send(response.clone()).is_ok() {
                delivered = true;
                break;
            }
        }
        if queue.is_empty() {
            state.pending.remove(&key);
        }
        delivered
    }
    pub fn pending_count(&self, name: &str, oid: Option<u32>) -> usize {
        self.state.lock().expect("response router poisoned").pending
            .get(&ResponseKey { name: name.into(), oid }).map_or(0, VecDeque::len)
    }
}
impl ResponseWaiter {
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
    fn unmatched_oid_does_not_consume_waiter() {
        let router = ResponseRouter::new();
        let waiter = router.register("identify_response", None);
        assert!(!router.dispatch(response("identify_response", Some(2))));
        assert_eq!(router.pending_count("identify_response", None), 1);
        assert!(router.dispatch(response("identify_response", None)));
        assert!(waiter.wait(Duration::from_millis(1)).is_ok());
    }
}
