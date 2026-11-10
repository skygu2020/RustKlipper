//! Rust Moonraker API server implementation.
//!
//! This crate is the Rust-side API service that will bridge remote clients to
//! the Rust Klipper host. The implementation is developed incrementally against
//! the upstream Moonraker HTTP, WebSocket, and JSON-RPC behavior.

pub mod jsonrpc;
pub mod server;
pub mod websocket;

pub use jsonrpc::{JsonRpcRouter, RpcError, RpcId, RpcRequest, RpcResponse};
pub use server::MoonrakerHttpServer;
pub use websocket::{ClientInfo, WebSocketConnection, WebSocketManager};
