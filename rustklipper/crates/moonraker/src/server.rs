//! Cross-platform HTTP and WebSocket transports for the Moonraker API.
//!
//! HTTP JSON-RPC and the primary `/websocket` endpoint share one Tokio
//! listener and one JSON-RPC router. The transport is platform independent.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_tungstenite::{tungstenite::Message, HyperWebsocket};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use klipper_host::klippy_api::{KlippyApiCodec, KlippyApiDispatcher, MESSAGE_TERMINATOR, serve_async_stream};
use crate::{JsonRpcRouter, WebSocketManager};

const MAX_JSON_RPC_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

/// Moonraker HTTP/WebSocket API listener.
pub struct MoonrakerHttpServer {
    listener: TcpListener,
    router: Arc<JsonRpcRouter>,
    websocket_manager: Arc<WebSocketManager>,
    klippy_dispatcher: Option<Arc<KlippyApiDispatcher>>,
}

impl MoonrakerHttpServer {
    /// Bind the API listener to an address such as `0.0.0.0:7125`.
    pub async fn bind(address: SocketAddr, router: Arc<JsonRpcRouter>) -> io::Result<Self> {
        Self::bind_with_integrations(address, router, WebSocketManager::new(), None).await
    }

    /// Bind with the live Klippy API dispatcher enabled for /klippysocket.
    pub async fn bind_with_klippy_dispatcher(
        address: SocketAddr,
        router: Arc<JsonRpcRouter>,
        klippy_dispatcher: Arc<KlippyApiDispatcher>,
    ) -> io::Result<Self> {
        Self::bind_with_integrations(address, router, WebSocketManager::new(), Some(klippy_dispatcher)).await
    }

    /// Bind using a caller-owned manager so host components can publish notifications.
    pub async fn bind_with_websocket_manager(
        address: SocketAddr,
        router: Arc<JsonRpcRouter>,
        websocket_manager: Arc<WebSocketManager>,
    ) -> io::Result<Self> {
        Self::bind_with_integrations(address, router, websocket_manager, None).await
    }

    /// Bind with caller-owned notifications and an optional Klippy API dispatcher.
    pub async fn bind_with_integrations(
        address: SocketAddr,
        router: Arc<JsonRpcRouter>,
        websocket_manager: Arc<WebSocketManager>,
        klippy_dispatcher: Option<Arc<KlippyApiDispatcher>>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        Ok(Self { listener, router, websocket_manager, klippy_dispatcher })
    }

    /// Clone the shared manager before moving the server into its run loop.
    pub fn websocket_manager(&self) -> Arc<WebSocketManager> {
        Arc::clone(&self.websocket_manager)
    }

    /// Return the actual bound address (useful when binding port zero in tests).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept HTTP/1 and WebSocket connections until the listener fails.
    pub async fn run(self) -> io::Result<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let router = Arc::clone(&self.router);
            let websocket_manager = Arc::clone(&self.websocket_manager);
            let klippy_dispatcher = self.klippy_dispatcher.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let router = Arc::clone(&router);
                    let websocket_manager = Arc::clone(&websocket_manager);
                    let klippy_dispatcher = klippy_dispatcher.clone();
                    async move {
                        Ok::<_, Infallible>(handle_request(request, router, websocket_manager, klippy_dispatcher).await)
                    }
                });
                let connection = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades();
                let _ = connection.await;
            });
        }
    }
}

async fn handle_request(
    mut request: Request<Incoming>,
    router: Arc<JsonRpcRouter>,
    websocket_manager: Arc<WebSocketManager>,
    klippy_dispatcher: Option<Arc<KlippyApiDispatcher>>,
) -> Response<Full<Bytes>> {
    if request.uri().path() == "/klippysocket" {
        let Some(dispatcher) = klippy_dispatcher else {
            return response(StatusCode::SERVICE_UNAVAILABLE, "application/json", Bytes::from_static(br#"{"error":"Klippy API is not configured"}"#));
        };
        if !hyper_tungstenite::is_upgrade_request(&request) {
            return response(StatusCode::BAD_REQUEST, "application/json", Bytes::from_static(br#"{"error":"WebSocket upgrade required"}"#));
        }
        return match hyper_tungstenite::upgrade(&mut request, None) {
            Ok((response, websocket)) => {
                tokio::spawn(async move { let _ = serve_klippy_websocket(websocket, dispatcher).await; });
                response
            }
            Err(_) => response(StatusCode::BAD_REQUEST, "application/json", Bytes::from_static(br#"{"error":"Invalid WebSocket upgrade"}"#)),
        };
    }

    if request.uri().path() == "/websocket" {
        if !hyper_tungstenite::is_upgrade_request(&request) {
            return response(
                StatusCode::BAD_REQUEST,
                "application/json",
                Bytes::from_static(br#"{"error":"WebSocket upgrade required"}"#),
            );
        }

        return match hyper_tungstenite::upgrade(&mut request, None) {
            Ok((response, websocket)) => {
                tokio::spawn(async move {
                    let _ = serve_websocket(websocket, router, websocket_manager).await;
                });
                response
            }
            Err(_) => response(
                StatusCode::BAD_REQUEST,
                "application/json",
                Bytes::from_static(br#"{"error":"Invalid WebSocket upgrade"}"#),
            ),
        };
    }

    if request.uri().path() != "/server/jsonrpc" {
        return response(
            StatusCode::NOT_FOUND,
            "application/json",
            Bytes::from_static(br#"{"error":"Not Found"}"#),
        );
    }

    if request.method() != Method::POST {
        return response(
            StatusCode::METHOD_NOT_ALLOWED,
            "application/json",
            Bytes::from_static(br#"{"error":"Method Not Allowed"}"#),
        );
    }

    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(_) => {
            return response(
                StatusCode::BAD_REQUEST,
                "application/json",
                Bytes::from_static(br#"{"error":"Invalid request body"}"#),
            );
        }
    };

    if body.len() > MAX_JSON_RPC_BODY_BYTES {
        return response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "application/json",
            Bytes::from_static(br#"{"error":"Request body too large"}"#),
        );
    }

    let input = String::from_utf8_lossy(&body);
    match router.handle_json(&input) {
        Some(output) => response(StatusCode::OK, "application/json", Bytes::from(output)),
        None => response(StatusCode::NO_CONTENT, "application/json", Bytes::new()),
    }
}

async fn serve_websocket(
    websocket: HyperWebsocket,
    router: Arc<JsonRpcRouter>,
    manager: Arc<WebSocketManager>,
) -> Result<(), hyper_tungstenite::tungstenite::Error> {
    let mut websocket = websocket.await?;
    let mut connection = manager.connect();

    loop {
        tokio::select! {
            incoming = websocket.next() => {
                let Some(message) = incoming else { break; };
                match message? {
                    Message::Text(text) => {
                        if text.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
                            websocket.send(Message::Close(Some(
                                hyper_tungstenite::tungstenite::protocol::CloseFrame {
                                    code: hyper_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Size,
                                    reason: "Message too large".into(),
                                },
                            ))).await?;
                            break;
                        }
                        let output = match manager.handle_control_message(&text, &connection) {
                            Some(output) => output,
                            None => router.handle_json(&text),
                        };
                        if let Some(output) = output {
                            websocket.send(Message::Text(output.into())).await?;
                        }
                    }
                    Message::Binary(_) => {
                        // Moonraker's JSON-RPC WebSocket protocol is text-frame based.
                    }
                    Message::Ping(payload) => websocket.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    Message::Frame(_) => unreachable!("tungstenite handles raw frames internally"),
                }
            }
            notification = connection.recv_notification() => {
                match notification {
                    Ok(message) => websocket.send(Message::Text(message.into())).await?,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}

/// Bridge the Klippy WebSocket directly to the host dispatcher using ETX framing.
async fn serve_klippy_websocket(
    websocket: HyperWebsocket,
    dispatcher: Arc<KlippyApiDispatcher>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let websocket = websocket.await?;
    let (mut websocket_sink, mut websocket_stream) = websocket.split();
    let (bridge, mut host_stream) = tokio::io::duplex(64 * 1024);
    let (mut bridge_reader, mut bridge_writer) = tokio::io::split(bridge);
    let host_task = tokio::spawn(async move {
        let _ = serve_async_stream(&mut host_stream, &dispatcher).await;
    });

    let mut codec = KlippyApiCodec::new(MAX_WEBSOCKET_MESSAGE_BYTES);
    let mut chunk = [0u8; 8192];
    loop {
        tokio::select! {
            incoming = websocket_stream.next() => {
                let Some(message) = incoming else { break; };
                match message? {
                    Message::Text(text) => {
                        if text.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
                            websocket_sink.send(Message::Close(Some(
                                hyper_tungstenite::tungstenite::protocol::CloseFrame {
                                    code: hyper_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Size,
                                    reason: "Message too large".into(),
                                },
                            ))).await?;
                            break;
                        }
                        bridge_writer.write_all(text.as_bytes()).await?;
                        bridge_writer.write_all(&[MESSAGE_TERMINATOR]).await?;
                    }
                    Message::Binary(_) => {}
                    Message::Ping(payload) => websocket_sink.send(Message::Pong(payload)).await?,
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    Message::Frame(_) => unreachable!("tungstenite handles raw frames internally"),
                }
            }
            read = bridge_reader.read(&mut chunk) => {
                let count = read?;
                if count == 0 { break; }
                for message in codec.push(&chunk[..count])? {
                    websocket_sink.send(Message::Text(serde_json::to_string(&message)?.into())).await?;
                }
            }
        }
    }
    let _ = bridge_writer.shutdown().await;
    let _ = host_task.await;
    Ok(())
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: Bytes,
) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, content_type)
        .body(Full::new(body))
        .expect("static HTTP response headers are valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio_tungstenite::{connect_async, tungstenite::Message as ClientMessage};

    async fn start_test_server() -> (SocketAddr, tokio::task::JoinHandle<io::Result<()>>) {
        let mut router = JsonRpcRouter::new();
        router
            .register("printer.test_echo", |params| Ok(params))
            .unwrap();
        let server = MoonrakerHttpServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(router),
        )
        .await
        .unwrap();
        let address = server.local_addr().unwrap();
        (address, tokio::spawn(server.run()))
    }

    async fn post_json_rpc(address: SocketAddr, payload: &str) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let request = format!(
            "POST /server/jsonrpc HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            payload.len(),
            payload,
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    #[tokio::test]
    async fn serves_json_rpc_over_http() {
        let (address, task) = start_test_server().await;
        let response = post_json_rpc(
            address,
            r#"{"jsonrpc":"2.0","method":"printer.test_echo","params":{"value":42},"id":9}"#,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"));
        let body = response.split("\r\n\r\n").nth(1).unwrap();
        let value: Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["id"], json!(9));
        assert_eq!(value["result"]["value"], json!(42));
        task.abort();
    }

    #[tokio::test]
    async fn returns_json_rpc_parse_error_for_invalid_json() {
        let (address, task) = start_test_server().await;
        let response = post_json_rpc(address, "{").await;
        assert!(response.starts_with("HTTP/1.1 200"));
        let body = response.split("\r\n\r\n").nth(1).unwrap();
        let value: Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["error"]["code"], json!(-32700));
        task.abort();
    }

    #[tokio::test]
    async fn rejects_non_post_requests() {
        let (address, task) = start_test_server().await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /server/jsonrpc HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8(response).unwrap().starts_with("HTTP/1.1 405"));
        task.abort();
    }

    #[tokio::test]
    async fn upgrades_websocket_and_dispatches_json_rpc() {
        let (address, task) = start_test_server().await;
        let (mut websocket, response) = connect_async(format!("ws://{address}/websocket"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

        websocket
            .send(ClientMessage::Text(
                r#"{"jsonrpc":"2.0","method":"printer.test_echo","params":{"source":"ws"},"id":17}"#
                    .into(),
            ))
            .await
            .unwrap();

        let message = websocket.next().await.unwrap().unwrap();
        let ClientMessage::Text(text) = message else {
            panic!("expected a text JSON-RPC response");
        };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["id"], json!(17));
        assert_eq!(value["result"]["source"], json!("ws"));

        websocket.close(None).await.unwrap();
        task.abort();
    }

    #[tokio::test]
    async fn klippysocket_forwards_etx_framed_requests_to_host_dispatcher() {
        let mut router = JsonRpcRouter::new();
        router.register("printer.test_echo", |params| Ok(params)).unwrap();

        let dispatcher = Arc::new(KlippyApiDispatcher::new());
        dispatcher.register("info", |_| Ok(json!({"state": "ready"}))).unwrap();

        let server = MoonrakerHttpServer::bind_with_klippy_dispatcher(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(router),
            dispatcher,
        ).await.unwrap();
        let address = server.local_addr().unwrap();
        let task = tokio::spawn(server.run());

        let (mut websocket, response) = connect_async(format!("ws://{address}/klippysocket"))
            .await.unwrap();
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);

        websocket.send(ClientMessage::Text(
            r#"{"id":41,"method":"info","params":{}}"#.into(),
        )).await.unwrap();

        let message = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            websocket.next(),
        ).await.unwrap().unwrap().unwrap();
        let ClientMessage::Text(text) = message else {
            panic!("expected a Klippy API JSON response");
        };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value, json!({"id": 41, "result": {"state": "ready"}}));

        websocket.close(None).await.unwrap();
        task.abort();
    }

    #[tokio::test]
    async fn websocket_exposes_id_and_identification_methods() {
        let mut router = JsonRpcRouter::new();
        router.register("printer.test_echo", |params| Ok(params)).unwrap();
        let manager = WebSocketManager::new();
        let server = MoonrakerHttpServer::bind_with_websocket_manager(
            "127.0.0.1:0".parse().unwrap(), Arc::new(router), Arc::clone(&manager),
        ).await.unwrap();
        let address = server.local_addr().unwrap();
        let task = tokio::spawn(server.run());
        let (mut websocket, _) = connect_async(format!("ws://{address}/websocket")).await.unwrap();
        websocket.send(ClientMessage::Text(
            r#"{"jsonrpc":"2.0","method":"server.websocket.id","id":1}"#.into(),
        )).await.unwrap();
        let message = websocket.next().await.unwrap().unwrap();
        let ClientMessage::Text(text) = message else { panic!("expected id response"); };
        let value: Value = serde_json::from_str(&text).unwrap();
        let connection_id = value["result"]["websocket_id"].as_u64().unwrap();
        websocket.send(ClientMessage::Text(
            r#"{"jsonrpc":"2.0","method":"server.connection.identify","params":{"client_name":"fluidd","version":"1.0","type":"web","url":"http://localhost"},"id":2}"#.into(),
        )).await.unwrap();
        let message = websocket.next().await.unwrap().unwrap();
        let ClientMessage::Text(text) = message else { panic!("expected identify response"); };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["result"]["connection_id"], connection_id);
        let clients = manager.clients();
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].name, "fluidd");
        assert!(clients[0].identified);
        websocket.close(None).await.unwrap();
        task.abort();
    }
    #[tokio::test]
    async fn websocket_receives_server_notifications() {
        let mut router = JsonRpcRouter::new();
        router.register("printer.test_echo", |params| Ok(params)).unwrap();
        let manager = WebSocketManager::new();
        let server = MoonrakerHttpServer::bind_with_websocket_manager(
            "127.0.0.1:0".parse().unwrap(), Arc::new(router), Arc::clone(&manager),
        ).await.unwrap();
        let address = server.local_addr().unwrap();
        let task = tokio::spawn(server.run());
        let (mut websocket, _) = connect_async(format!("ws://{address}/websocket")).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while manager.client_count() == 0 { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(manager.notify_all("klippy_ready", vec![json!({"ready": true})]), 1);
        let message = tokio::time::timeout(std::time::Duration::from_secs(1), websocket.next())
            .await.unwrap().unwrap().unwrap();
        let ClientMessage::Text(text) = message else { panic!("expected a notification text frame"); };
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["method"], "notify_klippy_ready");
        assert_eq!(value["params"][0]["ready"], true);
        websocket.close(None).await.unwrap();
        task.abort();
    }
    #[tokio::test]
    async fn websocket_notifications_do_not_generate_responses() {
        let (address, task) = start_test_server().await;
        let (mut websocket, _) = connect_async(format!("ws://{address}/websocket"))
            .await
            .unwrap();

        websocket
            .send(ClientMessage::Text(
                r#"{"jsonrpc":"2.0","method":"printer.test_echo","params":{"ignored":true}}"#
                    .into(),
            ))
            .await
            .unwrap();

        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            websocket.next()
        )
        .await
        .is_err());

        websocket.close(None).await.unwrap();
        task.abort();
    }

    #[tokio::test]
    async fn websocket_connection_handles_multiple_requests() {
        let (address, task) = start_test_server().await;
        let (mut websocket, _) = connect_async(format!("ws://{address}/websocket"))
            .await
            .unwrap();

        for id in [1, 2, 3] {
            websocket
                .send(ClientMessage::Text(
                    json!({"jsonrpc":"2.0","method":"printer.test_echo","params":{"n":id},"id":id})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let message = websocket.next().await.unwrap().unwrap();
            let ClientMessage::Text(text) = message else {
                panic!("expected a text response");
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["id"], json!(id));
            assert_eq!(value["result"]["n"], json!(id));
        }

        websocket.close(None).await.unwrap();
        task.abort();
    }
}
