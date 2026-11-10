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
use tokio::net::TcpListener;

use crate::JsonRpcRouter;

const MAX_JSON_RPC_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

/// Moonraker HTTP/WebSocket API listener.
pub struct MoonrakerHttpServer {
    listener: TcpListener,
    router: Arc<JsonRpcRouter>,
}

impl MoonrakerHttpServer {
    /// Bind the API listener to an address such as `0.0.0.0:7125`.
    pub async fn bind(address: SocketAddr, router: Arc<JsonRpcRouter>) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        Ok(Self { listener, router })
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
            tokio::spawn(async move {
                let service = service_fn(move |request| {
                    let router = Arc::clone(&router);
                    async move {
                        Ok::<_, Infallible>(handle_request(request, router).await)
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
) -> Response<Full<Bytes>> {
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
                    let _ = serve_websocket(websocket, router).await;
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
) -> Result<(), hyper_tungstenite::tungstenite::Error> {
    let mut websocket = websocket.await?;

    while let Some(message) = websocket.next().await {
        match message? {
            Message::Text(text) => {
                if text.len() > MAX_WEBSOCKET_MESSAGE_BYTES {
                    websocket
                        .send(Message::Close(Some(
                            hyper_tungstenite::tungstenite::protocol::CloseFrame {
                                code: hyper_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Size,
                                reason: "Message too large".into(),
                            },
                        )))
                        .await?;
                    break;
                }

                if let Some(output) = router.handle_json(&text) {
                    websocket.send(Message::Text(output.into())).await?;
                }
            }
            Message::Binary(_) => {
                // Moonraker's JSON-RPC WebSocket protocol is text-frame based.
                // Ignore binary frames without terminating an otherwise valid session.
            }
            Message::Ping(payload) => websocket.send(Message::Pong(payload)).await?,
            Message::Pong(_) => {}
            Message::Close(_) => break,
            Message::Frame(_) => unreachable!("tungstenite handles raw frames internally"),
        }
    }

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
