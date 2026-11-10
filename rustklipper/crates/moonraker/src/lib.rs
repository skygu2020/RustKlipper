//! Standalone, cross-platform Rust implementation of the Moonraker API server.
//!
//! This crate deliberately does not depend on the Klipper Host crate. Runtime
//! communication with the host happens over the stable JSON + ETX TCP protocol
//! at the configured Klippy API endpoint. Moonraker can therefore run, restart,
//! and upgrade as an independent process without sharing host memory or types.

pub mod jsonrpc;
pub mod server;
pub mod websocket;

pub use jsonrpc::{JsonRpcRouter, RpcError, RpcId, RpcRequest, RpcResponse};
pub use server::MoonrakerHttpServer;
pub use websocket::{ClientInfo, WebSocketConnection, WebSocketManager};
