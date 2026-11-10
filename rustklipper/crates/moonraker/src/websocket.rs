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
    pub identified: bool,
}

impl Default for ClientInfo {
    fn default() -> Self {
        Self {
            id: 0,
            name: "unknown".to_owned(),
            version: String::new(),
            client_type: String::new(),
            url: String::new(),
            identified: false,
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
        if client.identified { return false; }
        let client_type = client_type.into().to_lowercase();
        if !["web", "mobile", "desktop", "display", "bot", "agent", "other"].contains(&client_type.as_str()) { return false; }
        client.name = name.into();
        client.version = version.into();
        client.client_type = client_type;
        client.url = url.into();
        client.identified = true;
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

    /// Handle Moonraker's built-in WebSocket control methods.
    /// Returns None when the method belongs to the application router.
    pub fn handle_control_message(&self, input: &str, connection: &WebSocketConnection) -> Option<Option<String>> {
        let value: Value = serde_json::from_str(input).ok()?;
        let Value::Object(object) = value else { return None; };
        let method = object.get("method")?.as_str()?;
        if method != "server.websocket.id" && method != "server.connection.identify" { return None; }
        let request_id = object.get("id").cloned();
        let result: Result<Value, (i64, String)> = if method == "server.websocket.id" {
            Ok(json!({"websocket_id": connection.id()}))
        } else {
            let params = object.get("params").and_then(Value::as_object);
            let required = |key: &str| params.and_then(|p| p.get(key)).and_then(Value::as_str).map(str::to_owned);
            match (required("client_name"), required("version"), required("type"), required("url")) {
                (Some(name), Some(version), Some(client_type), Some(url)) => {
                    if connection.identify(name, version, client_type, url) {
                        Ok(json!({"connection_id": connection.id()}))
                    } else {
                        Err((-32602, "Invalid client type or connection already identified".to_owned()))
                    }
                }
                _ => Err((-32602, "Missing client_name, version, type, or url".to_owned())),
            }
        };
        let Some(id) = request_id else { return Some(None); };
        let response = match result {
            Ok(result) => json!({"jsonrpc":"2.0", "id":id, "result":result}),
            Err((code, message)) => json!({"jsonrpc":"2.0", "id":id, "error":{"code":code,"message":message}}),
        };
        Some(serde_json::to_string(&response).ok())
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
