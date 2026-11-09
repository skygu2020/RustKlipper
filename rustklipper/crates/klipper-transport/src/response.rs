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
        let sender = {
            let mut state = self.state.lock().expect("response router poisoned");
            let queue = match state.pending.get_mut(&key) { Some(q) => q, None => return false };
            let sender = queue.pop_front();
            if queue.is_empty() { state.pending.remove(&key); }
            sender
        };
        sender.is_some_and(|tx| tx.send(response).is_ok())
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
    fn unmatched_oid_does_not_consume_waiter() {
        let router = ResponseRouter::new();
        let waiter = router.register("identify_response", None);
        assert!(!router.dispatch(response("identify_response", Some(2))));
        assert_eq!(router.pending_count("identify_response", None), 1);
        assert!(router.dispatch(response("identify_response", None)));
        assert!(waiter.wait(Duration::from_millis(1)).is_ok());
    }
}
