//! Cross-platform HTTP transport for the Moonraker API.
//!
//! This starts with Moonraker's JSON-RPC-over-HTTP endpoint. WebSocket and
//! bridge transports are layered on the same router in subsequent modules.
//! The listener uses Tokio and Hyper without platform-specific socket APIs.

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::JsonRpcRouter;

const MAX_JSON_RPC_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Moonraker HTTP API listener.
///
/// The router is shared by all accepted connections. Request handlers are
/// synchronous and thread-safe, while connection I/O is driven by Tokio.
pub struct MoonrakerHttpServer {
    listener: TcpListener,
    router: Arc<JsonRpcRouter>,
}

impl MoonrakerHttpServer {
    /// Bind the HTTP listener to an address such as `0.0.0.0:7125`.
    pub async fn bind(
        address: SocketAddr,
        router: Arc<JsonRpcRouter>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        Ok(Self { listener, router })
    }

    /// Return the actual bound address (useful when binding port zero in tests).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept HTTP/1 connections until the listener fails or the task is aborted.
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
                    .serve_connection(TokioIo::new(stream), service);
                let _ = connection.await;
            });
        }
    }
}

async fn handle_request(
    request: Request<Incoming>,
    router: Arc<JsonRpcRouter>,
) -> Response<Full<Bytes>> {
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
        Some(output) => response(
            StatusCode::OK,
            "application/json",
            Bytes::from(output),
        ),
        // JSON-RPC notifications do not produce a response payload.
        None => response(StatusCode::NO_CONTENT, "application/json", Bytes::new()),
    }
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
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

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
            "POST /server/jsonrpc HTTP/1.1\\r\\nHost: localhost\\r\\nContent-Type: application/json\\r\\nContent-Length: {}\\r\\nConnection: close\\r\\n\\r\\n{}",
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
        let body = response.split("\\r\\n\\r\\n").nth(1).unwrap();
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["id"], json!(9));
        assert_eq!(value["result"]["value"], json!(42));
        task.abort();
    }

    #[tokio::test]
    async fn returns_json_rpc_parse_error_for_invalid_json() {
        let (address, task) = start_test_server().await;
        let response = post_json_rpc(address, "{").await;
        assert!(response.starts_with("HTTP/1.1 200"));
        let body = response.split("\\r\\n\\r\\n").nth(1).unwrap();
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["error"]["code"], json!(-32700));
        task.abort();
    }

    #[tokio::test]
    async fn rejects_non_post_requests() {
        let (address, task) = start_test_server().await;
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /server/jsonrpc HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8(response).unwrap().starts_with("HTTP/1.1 405"));
        task.abort();
    }
}
