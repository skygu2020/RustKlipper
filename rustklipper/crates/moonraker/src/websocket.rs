//! WebSocket connection registry and server-to-client notifications.
//!
//! This is the transport-level foundation of Moonraker's WebsocketManager.
//! Authentication and the full upstream identify handshake are layered on
//! top of this registry as their corresponding API components are ported.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde_json::{json, Value};
use tokio::sync::broadcast;

/// Maximum number of queued server notifications retained for slow clients.
const NOTIFICATION_CAPACITY: usize = 256;

/// Shared WebSocket client registry.
pub struct WebSocketManager {
    next_id: AtomicU64,
    clients: Mutex<HashMap<u64, ClientInfo>>,
    notifications: broadcast::Sender<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientInfo {
    pub id: u64,
    pub name: String,
    pub version: String,
    pub client_type: String,
    pub url: String,
}

impl Default for ClientInfo {
    fn default() -> Self {
        Self {
            id: 0,
            name: "unknown".to_owned(),
            version: String::new(),
            client_type: String::new(),
            url: String::new(),
        }
    }
}

/// A registered connection. Dropping it removes the client from the manager.
pub struct WebSocketConnection {
    id: u64,
    manager: Weak<WebSocketManager>,
    notifications: broadcast::Receiver<String>,
}

impl WebSocketManager {
    pub fn new() -> Arc<Self> {
        let (notifications, _) = broadcast::channel(NOTIFICATION_CAPACITY);
        Arc::new(Self {
            next_id: AtomicU64::new(1),
            clients: Mutex::new(HashMap::new()),
            notifications,
        })
    }

    pub fn connect(self: &Arc<Self>) -> WebSocketConnection {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut info = ClientInfo::default();
        info.id = id;
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, info);
        WebSocketConnection {
            id,
            manager: Arc::downgrade(self),
            notifications: self.notifications.subscribe(),
        }
    }

    pub fn client_count(&self) -> usize {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    pub fn clients(&self) -> Vec<ClientInfo> {
        let mut clients: Vec<_> = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        clients.sort_by_key(|client| client.id);
        clients
    }

    pub fn identify(
        &self,
        id: u64,
        name: impl Into<String>,
        version: impl Into<String>,
        client_type: impl Into<String>,
        url: impl Into<String>,
    ) -> bool {
        let mut clients = self
            .clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(client) = clients.get_mut(&id) else {
            return false;
        };
        client.name = name.into();
        client.version = version.into();
        client.client_type = client_type.into();
        client.url = url.into();
        true
    }

    /// Broadcast a Moonraker notification to all connected WebSocket clients.
    /// The method is encoded once and shared by the broadcast channel.
    pub fn notify_all(&self, name: &str, params: Vec<Value>) -> usize {
        let method = if name.starts_with("notify_") {
            name.to_owned()
        } else {
            format!("notify_{name}")
        };
        let mut notification = json!({
            "jsonrpc": "2.0",
            "method": method,
        });
        if !params.is_empty() {
            notification["params"] = Value::Array(params);
        }
        let Ok(message) = serde_json::to_string(&notification) else {
            return 0;
        };
        self.notifications.send(message).unwrap_or(0)
    }

    fn disconnect(&self, id: u64) {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }
}

impl WebSocketConnection {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub async fn recv_notification(&mut self) -> Result<String, broadcast::error::RecvError> {
        self.notifications.recv().await
    }

    pub fn identify(
        &self,
        name: impl Into<String>,
        version: impl Into<String>,
        client_type: impl Into<String>,
        url: impl Into<String>,
    ) -> bool {
        self.manager.upgrade().is_some_and(|manager| {
            manager.identify(self.id, name, version, client_type, url)
        })
    }
}

impl Drop for WebSocketConnection {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            manager.disconnect(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tracks_connections_and_removes_them_on_drop() {
        let manager = WebSocketManager::new();
        let first = manager.connect();
        let second = manager.connect();
        assert_ne!(first.id(), second.id());
        assert_eq!(manager.client_count(), 2);
        drop(first);
        assert_eq!(manager.client_count(), 1);
        drop(second);
        assert_eq!(manager.client_count(), 0);
    }

    #[tokio::test]
    async fn identifies_clients_and_lists_metadata() {
        let manager = WebSocketManager::new();
        let connection = manager.connect();
        assert!(connection.identify("fluidd", "1.2.3", "web", "http://printer"));
        let clients = manager.clients();
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].name, "fluidd");
        assert_eq!(clients[0].version, "1.2.3");
        assert_eq!(clients[0].client_type, "web");
        assert_eq!(clients[0].url, "http://printer");
    }

    #[tokio::test]
    async fn broadcasts_json_rpc_notifications() {
        let manager = WebSocketManager::new();
        let mut connection = manager.connect();
        assert_eq!(
            manager.notify_all("klippy_ready", vec![json!({"ready": true})]),
            1
        );
        let message = connection.recv_notification().await.unwrap();
        let value: Value = serde_json::from_str(&message).unwrap();
        assert_eq!(value["jsonrpc"], "2.0");
        assert_eq!(value["method"], "notify_klippy_ready");
        assert_eq!(value["params"][0]["ready"], true);
    }
}
