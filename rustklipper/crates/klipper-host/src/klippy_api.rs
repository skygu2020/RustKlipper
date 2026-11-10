//! Portable Klippy API message framing shared by Unix sockets and Windows named pipes.
//!
//! Klippy's API stream is a sequence of JSON values, each terminated by ETX
//! (0x03). This framing layer is independent of the socket type so it can be
//! reused above Unix sockets, Windows named pipes, or an in-process transport.

use serde_json::{Map, Value};
use klipper_gcode::DispatchEvent;
use thiserror::Error;

pub const MESSAGE_TERMINATOR: u8 = 0x03;
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 20 * 1024 * 1024;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum KlippyApiCodecError {
    #[error("Klippy API message exceeds the {limit}-byte limit")]
    MessageTooLarge { limit: usize },
    #[error("Klippy API message is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("Klippy API message must be a JSON object")]
    ExpectedObject,
    #[error("Klippy API request is missing a string method")]
    MissingMethod,
    #[error("Klippy API request params must be a JSON object")]
    ExpectedParamsObject,
    #[error("Klippy API request id must be a string, number, or null")]
    InvalidId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KlippyApiRequest {
    pub id: Option<Value>,
    pub method: String,
    pub params: Value,
}

impl KlippyApiRequest {
    pub fn from_value(value: Value) -> Result<Self, KlippyApiCodecError> {
        let object = value.as_object().ok_or(KlippyApiCodecError::ExpectedObject)?;
        let method = object.get("method").and_then(Value::as_str)
            .ok_or(KlippyApiCodecError::MissingMethod)?.to_owned();
        let id = object.get("id").cloned();
        if id.as_ref().is_some_and(|id| {
            !id.is_null() && !id.is_string() && !id.is_number()
        }) {
            return Err(KlippyApiCodecError::InvalidId);
        }
        let params = match object.get("params") {
            None => Value::Object(Map::new()),
            Some(Value::Object(params)) => Value::Object(params.clone()),
            Some(_) => return Err(KlippyApiCodecError::ExpectedParamsObject),
        };
        Ok(Self {
            id,
            method,
            params,
        })
    }

    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }
}

/// Stateful decoder accepting arbitrary stream chunks and retaining incomplete
/// trailing data between calls.
#[derive(Debug, Clone)]
pub struct KlippyApiCodec {
    buffer: Vec<u8>,
    max_message_size: usize,
}

impl Default for KlippyApiCodec {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_MESSAGE_SIZE)
    }
}

impl KlippyApiCodec {
    pub fn new(max_message_size: usize) -> Self {
        Self { buffer: Vec::new(), max_message_size }
    }

    pub fn buffered_len(&self) -> usize { self.buffer.len() }
    pub fn is_empty(&self) -> bool { self.buffer.is_empty() }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Value>, KlippyApiCodecError> {
        self.buffer.extend_from_slice(bytes);
        let mut messages = Vec::new();
        let mut consumed = 0;
        while let Some(relative_end) = self.buffer[consumed..]
            .iter().position(|byte| *byte == MESSAGE_TERMINATOR)
        {
            let end = consumed + relative_end;
            if end - consumed > self.max_message_size {
                self.buffer.drain(..=end);
                return Err(KlippyApiCodecError::MessageTooLarge { limit: self.max_message_size });
            }
            let parsed = match serde_json::from_slice::<Value>(&self.buffer[consumed..end]) {
                Ok(parsed) => parsed,
                Err(error) => {
                    // Consume the malformed frame (and any earlier frames from
                    // this push) so a caller that reports the error can recover
                    // or close the stream without repeatedly parsing the same
                    // invalid bytes on every subsequent read.
                    self.buffer.drain(..=end);
                    return Err(KlippyApiCodecError::InvalidJson(error.to_string()));
                }
            };
            messages.push(parsed);
            consumed = end + 1;
        }
        if consumed > 0 { self.buffer.drain(..consumed); }
        if self.buffer.len() > self.max_message_size {
            self.buffer.clear();
            return Err(KlippyApiCodecError::MessageTooLarge { limit: self.max_message_size });
        }
        Ok(messages)
    }

    pub fn encode(value: &Value) -> Result<Vec<u8>, KlippyApiCodecError> {
        let mut bytes = serde_json::to_vec(value)
            .map_err(|error| KlippyApiCodecError::InvalidJson(error.to_string()))?;
        bytes.push(MESSAGE_TERMINATOR);
        Ok(bytes)
    }

    pub fn encode_request(id: Value, method: &str, params: Value) -> Result<Vec<u8>, KlippyApiCodecError> {
        Self::encode(&serde_json::json!({"id": id, "method": method, "params": params}))
    }

    pub fn encode_response(id: Value, result: Value) -> Result<Vec<u8>, KlippyApiCodecError> {
        Self::encode(&serde_json::json!({"id": id, "result": result}))
    }

    pub fn encode_notification(method: &str, params: Value) -> Result<Vec<u8>, KlippyApiCodecError> {
        Self::encode(&serde_json::json!({"method": method, "params": params}))
    }
}


/// A method implementation registered with the host-side Klippy API dispatcher.
pub type KlippyApiMethod = std::sync::Arc<
    dyn Fn(Value) -> Result<Value, KlippyApiRpcError> + Send + Sync + 'static,
>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KlippyApiRpcError {
    pub code: i64,
    pub message: String,
}

impl KlippyApiRpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, format!("Method not found: {method}"))
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(-32600, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(-32000, message)
    }
}

/// Thread-safe method registry and JSON-RPC-style request dispatcher.
///
/// It deliberately owns no socket or runtime. A transport adapter can call
/// `dispatch_value` for messages received over Unix sockets, Windows named
/// pipes, TCP, or an in-process channel.
pub struct KlippyApiDispatcher {
    methods: std::sync::RwLock<std::collections::HashMap<String, KlippyApiMethod>>,
    gcode_output: tokio::sync::broadcast::Sender<String>,
    status_printer: std::sync::RwLock<Option<std::sync::Arc<crate::Printer>>>,
}

impl Default for KlippyApiDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl KlippyApiDispatcher {
    pub fn new() -> Self {
        let (gcode_output, _) = tokio::sync::broadcast::channel(256);
        let dispatcher = Self {
            methods: std::sync::RwLock::new(std::collections::HashMap::new()),
            gcode_output,
            status_printer: std::sync::RwLock::new(None),
        };
        // Output subscription is connection-local in the async transport.
        // Validate the optional response template here; the stream adapter
        // retains it for this connection and applies it to future messages.
        dispatcher.register("gcode/subscribe_output", |params| {
            gcode_output_response_template(&params)?;
            Ok(serde_json::json!({}))
        }).expect("built-in endpoint name is unique");
        dispatcher
    }

    fn subscribe_gcode_output(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.gcode_output.subscribe()
    }

    /// Register Klippy's `objects/list` endpoint from the printer status registry.
    ///
    /// Internal Rust component instances are excluded; only JSON-backed status objects are queryable.
    pub fn register_object_listing(
        &self,
        printer: std::sync::Arc<crate::Printer>,
    ) -> Result<(), KlippyApiRpcError> {
        self.register("objects/list", move |_params| {
            Ok(serde_json::json!({
                "objects": printer.status_object_names(),
            }))
        })
    }

    /// Register a conservative `objects/query` endpoint.
    ///
    /// The current printer registry stores arbitrary Rust values, so only
    /// objects explicitly registered as `serde_json::Value` are queryable.
    /// Non-JSON host objects are omitted rather than being stringified or
    /// exposing implementation details as if they were Klipper status fields.
    pub fn register_object_query(
        &self,
        printer: std::sync::Arc<crate::Printer>,
    ) -> Result<(), KlippyApiRpcError> {
        self.register("objects/query", move |params| {
            query_printer_status(&printer, &params)
        })
    }

    /// Register Klipper's `objects/subscribe` endpoint.
    ///
    /// The initial response has the same eventtime/status shape as
    /// `objects/query`. The async connection layer uses the Printer's
    /// status-update stream to publish subsequent `notify_status_update`
    /// notifications filtered to this connection's requested objects/fields.
    pub fn register_object_subscription(
        &self,
        printer: std::sync::Arc<crate::Printer>,
    ) -> Result<(), KlippyApiRpcError> {
        *self.status_printer.write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(printer.clone());
        self.register("objects/subscribe", move |params| {
            object_status_response_template(&params)?;
            query_printer_status(&printer, &params)
        })
    }

    /// Register Klipper's `gcode/help` endpoint from the active printer dispatcher.
    ///
    /// Only commands available in the current printer state are included.
    pub fn register_gcode_help(
        &self,
        printer: std::sync::Arc<crate::Printer>,
    ) -> Result<(), KlippyApiRpcError> {
        self.register("gcode/help", move |_params| {
            Ok(serde_json::json!(printer.gcode_help()))
        })
    }

    /// Register Klipper's `gcode/script` endpoint.
    ///
    /// Scripts are executed in order by the host-side G-code dispatcher. The
    /// endpoint returns an empty object on success, matching Klippy's API
    /// response shape; the first command error is returned as an RPC error.
    pub fn register_gcode_script(
        &self,
        printer: std::sync::Arc<crate::Printer>,
    ) -> Result<(), KlippyApiRpcError> {
        let output = self.gcode_output.clone();
        self.register("gcode/script", move |params| {
            let script = params
                .get("script")
                .and_then(Value::as_str)
                .ok_or_else(|| KlippyApiRpcError::invalid_request(
                    "gcode/script requires a string script parameter"
                ))?;
            // Dispatch one line at a time so the first command error stops
            // the script immediately. Processing the whole script up front
            // would execute later commands before the RPC layer sees an error.
            for line in script.split('\n') {
                let Some(outcome) = printer.process_gcode_line(line, false) else {
                    continue;
                };
                for event in &outcome.events {
                    let response = match event {
                        DispatchEvent::Raw(message) => Some(message.clone()),
                        DispatchEvent::Info(message) => Some(format!("// {message}")),
                        DispatchEvent::Error(message) => Some(format!("!! {message}")),
                        DispatchEvent::Ack(_) => None,
                    };
                    if let Some(response) = response {
                        let _ = output.send(response);
                    }
                }
                if let Some(DispatchEvent::Error(message)) = outcome.events.iter()
                    .find(|event| matches!(event, DispatchEvent::Error(_)))
                {
                    return Err(KlippyApiRpcError::internal(message.clone()));
                }
            }
            Ok(serde_json::json!({}))
        })
    }

    /// Register Klipper's emergency-stop endpoint.
    ///
    /// The call transitions the printer into shutdown and returns an empty
    /// result object, matching the Klippy API response shape.
    pub fn register_emergency_stop(
        &self,
        printer: std::sync::Arc<crate::Printer>,
    ) -> Result<(), KlippyApiRpcError> {
        self.register("emergency_stop", move |_params| {
            printer.invoke_shutdown("Emergency stop");
            Ok(serde_json::json!({}))
        })
    }

    /// Register the Klippy `info` endpoint from the live printer lifecycle.
    ///
    /// `software_version` identifies this Rust host implementation; it is not
    /// presented as an upstream Klipper firmware version.
    pub fn register_printer_info(
        &self,
        printer: std::sync::Arc<crate::Printer>,
        software_version: impl Into<String>,
    ) -> Result<(), KlippyApiRpcError> {
        let software_version = software_version.into();
        self.register("info", move |params| {
            // Klipper accepts arbitrary client metadata, but the top-level
            // client_info value must be a JSON object when supplied.
            if params.get("client_info").is_some_and(|value| !value.is_object()) {
                return Err(KlippyApiRpcError::invalid_request(
                    "info client_info parameter must be an object"
                ));
            }
            let status = printer.status();
            let state = match status.state {
                crate::PrinterState::Startup | crate::PrinterState::Configuring => "startup",
                crate::PrinterState::Ready => "ready",
                crate::PrinterState::Shutdown => "shutdown",
                crate::PrinterState::Disconnected => "error",
            };
            Ok(serde_json::json!({
                "state": state,
                "state_message": status.message,
                "software_version": software_version,
                "process_id": std::process::id(),
            }))
        })
    }

    /// Register the standard printer-backed Klippy API surface in one place.
    ///
    /// The dispatcher must be shared with the Host runtime and transport
    /// listener. This helper wires the endpoints to the supplied live Printer
    /// without introducing any dependency on Moonraker or a particular socket.
    pub fn register_standard_endpoints(
        self: &std::sync::Arc<Self>,
        printer: std::sync::Arc<crate::Printer>,
        software_version: impl Into<String>,
    ) -> Result<(), KlippyApiRpcError> {
        self.register_printer_info(printer.clone(), software_version)?;
        self.register_object_listing(printer.clone())?;
        self.register_object_query(printer.clone())?;
        self.register_object_subscription(printer.clone())?;
        self.register_gcode_help(printer.clone())?;
        self.register_gcode_script(printer.clone())?;
        self.register_emergency_stop(printer)?;
        self.register_endpoint_listing()?;
        Ok(())
    }

    /// Register Klippy's endpoint-discovery method used by Moonraker clients.
    ///
    /// The response shape matches Klippy's `list_endpoints` contract:
    /// `{"endpoints": ["method/name", ...]}`.
    pub fn register_endpoint_listing(
        self: &std::sync::Arc<Self>,
    ) -> Result<(), KlippyApiRpcError> {
        let dispatcher = std::sync::Arc::downgrade(self);
        self.register("list_endpoints", move |_| {
            let dispatcher = dispatcher
                .upgrade()
                .ok_or_else(|| KlippyApiRpcError::internal("Klippy API dispatcher is unavailable"))?;
            Ok(serde_json::json!({
                "endpoints": dispatcher.method_names(),
            }))
        })
    }

    pub fn register<F>(&self, name: impl Into<String>, handler: F) -> Result<(), KlippyApiRpcError>
    where
        F: Fn(Value) -> Result<Value, KlippyApiRpcError> + Send + Sync + 'static,
    {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(KlippyApiRpcError::invalid_request("method name cannot be empty"));
        }
        let mut methods = self.methods.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        if methods.contains_key(&name) {
            return Err(KlippyApiRpcError::invalid_request(format!("Method already registered: {name}")));
        }
        methods.insert(name, std::sync::Arc::new(handler));
        Ok(())
    }

    pub fn unregister(&self, name: &str) -> bool {
        self.methods.write().unwrap_or_else(std::sync::PoisonError::into_inner).remove(name).is_some()
    }

    pub fn method_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.methods.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys().cloned().collect();
        names.sort();
        names
    }

    /// Dispatch one decoded JSON object. Notifications execute their handler
    /// but intentionally produce no response, matching Moonruster's client.
    pub fn dispatch_value(&self, value: Value) -> Option<Value> {
        let id = value.as_object().and_then(|object| object.get("id")).cloned();
        let request = match KlippyApiRequest::from_value(value) {
            Ok(request) => request,
            Err(error) => {
                return Some(rpc_error_response(id.unwrap_or(Value::Null),
                    KlippyApiRpcError::invalid_request(error.to_string())));
            }
        };
        let notification = request.is_notification();
        let handler = self.methods.read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&request.method).cloned();
        let result = match handler {
            Some(handler) => handler(request.params),
            None => Err(KlippyApiRpcError::method_not_found(&request.method)),
        };
        if notification { return None; }
        let id = request.id.unwrap_or(Value::Null);
        Some(match result {
            Ok(result) => serde_json::json!({"id": id, "result": result}),
            Err(error) => rpc_error_response(id, error),
        })
    }
}

fn object_status_response_template(
    params: &Value,
) -> Result<Map<String, Value>, KlippyApiRpcError> {
    match params.get("response_template") {
        None => Ok(Map::new()),
        Some(Value::Object(template)) => Ok(template.clone()),
        Some(_) => Err(KlippyApiRpcError::invalid_request(
            "objects/subscribe response_template must be an object"
        )),
    }
}

fn query_printer_status(
    printer: &crate::Printer,
    params: &Value,
) -> Result<Value, KlippyApiRpcError> {
    let requested = params
        .get("objects")
        .and_then(Value::as_object)
        .ok_or_else(|| KlippyApiRpcError::invalid_request(
            "object status request requires an objects mapping"
        ))?;
    let mut status = serde_json::Map::new();
    for (object_name, requested_fields) in requested {
        // Klipper treats an absent object (or one without status support)
        // as an empty status map, then applies the requested field selection.
        // Consequently explicit fields resolve to null while a null selection
        // produces an empty object.
        let fields = match printer.lookup_object_optional::<Value>(object_name) {
            Ok(Some(object)) => object.as_object().cloned().unwrap_or_default(),
            Ok(None) | Err(crate::PrinterError::ObjectTypeMismatch { .. }) => Map::new(),
            Err(error) => return Err(KlippyApiRpcError::internal(error.to_string())),
        };
        let selected = match requested_fields {
            Value::Array(names) => {
                let mut selected = serde_json::Map::new();
                for name in names {
                    let Some(name) = name.as_str() else {
                        return Err(KlippyApiRpcError::invalid_request(
                            "object field names must be strings"
                        ));
                    };
                    // Klipper includes every requested field in query results;
                    // fields absent from an object's status are represented as null.
                    selected.insert(
                        name.to_owned(),
                        fields.get(name).cloned().unwrap_or(Value::Null),
                    );
                }
                selected
            }
            Value::Null => fields,
            _ => return Err(KlippyApiRpcError::invalid_request(
                "each objects entry must be an array of field names or null"
            )),
        };
        status.insert(object_name.clone(), Value::Object(selected));
    }
    Ok(serde_json::json!({
        "eventtime": printer.eventtime(),
        "status": status
    }))
}

/// Keep only status fields whose values differ from the last snapshot sent
/// to this connection. Klipper's objects/subscribe notifications are deltas,
/// not repeated full snapshots.
fn changed_status_snapshot(
    snapshot: &Value,
    previous: &mut Map<String, Value>,
) -> Option<Value> {
    let status = snapshot.get("status")?.as_object()?;
    let mut changed_status = Map::new();

    for (object_name, current_value) in status {
        let current_fields = current_value.as_object()?;
        let previous_fields = previous.get(object_name).and_then(Value::as_object);
        let mut changed_fields = Map::new();

        for (field_name, value) in current_fields {
            if previous_fields.and_then(|fields| fields.get(field_name)) != Some(value) {
                changed_fields.insert(field_name.clone(), value.clone());
            }
        }

        previous.insert(object_name.clone(), current_value.clone());
        if !changed_fields.is_empty() {
            changed_status.insert(object_name.clone(), Value::Object(changed_fields));
        }
    }

    if changed_status.is_empty() {
        None
    } else {
        Some(serde_json::json!({
            "eventtime": snapshot.get("eventtime").cloned().unwrap_or(Value::Null),
            "status": changed_status
        }))
    }
}

fn status_cache_from_snapshot(snapshot: &Value) -> Map<String, Value> {
    snapshot.get("status")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

/// Klipper resolves a null field selection to the fields present in the
/// initial status query, then keeps that concrete field list for the life of
/// the subscription. This prevents later-added fields from silently joining
/// an existing subscription while allowing removed fields to be reported null.
fn normalize_object_subscription(
    requested: &Map<String, Value>,
    snapshot: &Value,
) -> Map<String, Value> {
    let status = snapshot.get("status").and_then(Value::as_object);
    requested.iter().map(|(object_name, fields)| {
        let fields = match fields {
            Value::Null => {
                let names = status
                    .and_then(|status| status.get(object_name))
                    .and_then(Value::as_object)
                    .map(|fields| fields.keys().cloned().map(Value::String).collect())
                    .unwrap_or_default();
                Value::Array(names)
            }
            fields => fields.clone(),
        };
        (object_name.clone(), fields)
    }).collect()
}

fn gcode_output_response_template(
    params: &Value,
) -> Result<serde_json::Map<String, Value>, KlippyApiRpcError> {
    match params.get("response_template") {
        None => Ok(serde_json::Map::new()),
        Some(Value::Object(template)) => Ok(template.clone()),
        Some(_) => Err(KlippyApiRpcError::invalid_request(
            "gcode/subscribe_output response_template must be an object"
        )),
    }
}

fn rpc_error_response(id: Value, error: KlippyApiRpcError) -> Value {
    serde_json::json!({
        "id": id,
        "error": {
            "code": error.code,
            "message": error.message,
        }
    })
}

/// Errors returned while serving one bidirectional Klippy API byte stream.
#[derive(Debug, Error)]
pub enum KlippyApiStreamError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Codec(#[from] KlippyApiCodecError),
    #[error("Klippy API stream ended with {buffered} unterminated message bytes")]
    TruncatedMessage { buffered: usize },
}

/// Serve requests on any blocking read/write stream.
///
/// This is deliberately transport-neutral: standard Unix streams, TCP streams,
/// Windows named-pipe handles, and test streams can all use the same handler.
/// A stream is considered finished cleanly only when EOF occurs between frames.
pub fn serve_stream<S>(
    stream: &mut S,
    dispatcher: &KlippyApiDispatcher,
) -> Result<(), KlippyApiStreamError>
where
    S: std::io::Read + std::io::Write,
{
    let mut codec = KlippyApiCodec::default();
    let mut chunk = [0u8; 8192];
    loop {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return if codec.is_empty() {
                Ok(())
            } else {
                Err(KlippyApiStreamError::TruncatedMessage {
                    buffered: codec.buffered_len(),
                })
            };
        }

        for message in codec.push(&chunk[..count])? {
            if let Some(response) = dispatcher.dispatch_value(message) {
                let encoded = KlippyApiCodec::encode(&response)?;
                stream.write_all(&encoded)?;
                stream.flush()?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_message_split_across_chunks() {
        let mut codec = KlippyApiCodec::default();
        assert!(codec.push(br#"{"id":1,"method":"info","#).unwrap().is_empty());
        let messages = codec.push(b"\"params\":{}}\x03").unwrap();
        assert_eq!(messages, vec![json!({"id":1,"method":"info","params":{}})]);
        assert!(codec.is_empty());
    }

    #[test]
    fn decodes_multiple_messages_from_one_chunk() {
        let mut codec = KlippyApiCodec::default();
        let mut bytes = KlippyApiCodec::encode(&json!({"id":1,"result":{}})).unwrap();
        bytes.extend(KlippyApiCodec::encode(&json!({"method":"notify","params":{}})).unwrap());
        assert_eq!(codec.push(&bytes).unwrap().len(), 2);
    }

    #[test]
    fn preserves_incomplete_suffix_after_complete_message() {
        let mut codec = KlippyApiCodec::default();
        let mut bytes = KlippyApiCodec::encode(&json!({"id":1})).unwrap();
        bytes.extend_from_slice(br#"{"id":2"#);
        assert_eq!(codec.push(&bytes).unwrap(), vec![json!({"id":1})]);
        assert_eq!(codec.buffered_len(), 7);
        assert_eq!(codec.push(b"}\x03").unwrap(), vec![json!({"id":2})]);
    }

    #[test]
    fn discards_malformed_frame_without_poisoning_following_messages() {
        let mut codec = KlippyApiCodec::default();
        assert!(matches!(
            codec.push(b"{invalid}\x03"),
            Err(KlippyApiCodecError::InvalidJson(_))
        ));
        assert!(codec.is_empty());

        let valid = KlippyApiCodec::encode(&json!({"id":4,"result":"ok"})).unwrap();
        assert_eq!(codec.push(&valid).unwrap(), vec![json!({"id":4,"result":"ok"})]);
    }

    #[test]
    fn rejects_oversized_unterminated_message() {
        let mut codec = KlippyApiCodec::new(4);
        assert_eq!(codec.push(b"12345"), Err(KlippyApiCodecError::MessageTooLarge { limit: 4 }));
        assert!(codec.is_empty());
    }

    #[test]
    fn parses_requests_and_notifications() {
        let request = KlippyApiRequest::from_value(json!({
            "id":42,"method":"gcode/script","params":{"script":"G28"}
        })).unwrap();
        assert_eq!(request.id, Some(json!(42)));
        assert_eq!(request.method, "gcode/script");
        assert_eq!(request.params, json!({"script":"G28"}));
        assert!(!request.is_notification());

        let notification = KlippyApiRequest::from_value(json!({"method":"process_status_update"})).unwrap();
        assert!(notification.is_notification());
        assert_eq!(notification.params, json!({}));
    }

    #[test]
    fn rejects_non_object_request_params() {
        for params in [
            json!(null),
            json!("invalid"),
            json!([1, 2]),
            json!(42),
        ] {
            let error = KlippyApiRequest::from_value(json!({
                "id": 1,
                "method": "info",
                "params": params
            })).unwrap_err();
            assert_eq!(error, KlippyApiCodecError::ExpectedParamsObject);
        }
    }

    #[test]
    fn rejects_request_ids_outside_json_rpc_scalar_types() {
        for id in [
            json!(true),
            json!(false),
            json!([]),
            json!({ "nested": 1 }),
        ] {
            let error = KlippyApiRequest::from_value(json!({
                "id": id,
                "method": "info"
            })).unwrap_err();
            assert_eq!(error, KlippyApiCodecError::InvalidId);
        }
    }

    #[test]
    fn accepts_string_numeric_and_null_request_ids() {
        for id in [json!("client-1"), json!(42), json!(1.5), Value::Null] {
            let request = KlippyApiRequest::from_value(json!({
                "id": id,
                "method": "info"
            })).unwrap();
            assert!(request.id.is_some());
        }
    }

    #[test]
    fn missing_request_params_default_to_an_empty_object() {
        let request = KlippyApiRequest::from_value(json!({
            "id": 1,
            "method": "info"
        })).unwrap();
        assert_eq!(request.params, json!({}));
    }

    #[test]
    fn encodes_request_response_and_notification_with_etx() {
        let request = KlippyApiCodec::encode_request(json!(7), "info", json!({})).unwrap();
        assert_eq!(request.last(), Some(&MESSAGE_TERMINATOR));
        let mut decoder = KlippyApiCodec::default();
        assert_eq!(decoder.push(&request).unwrap(), vec![json!({"id":7,"method":"info","params":{}})]);

        let response = KlippyApiCodec::encode_response(json!(7), json!({"state":"ready"})).unwrap();
        assert_eq!(decoder.push(&response).unwrap(), vec![json!({"id":7,"result":{"state":"ready"}})]);
        let notification = KlippyApiCodec::encode_notification("process_gcode_response", json!({"response":"ok"})).unwrap();
        assert_eq!(decoder.push(&notification).unwrap(), vec![json!({"method":"process_gcode_response","params":{"response":"ok"}})]);
    }

    struct TestStream {
        input: std::io::Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl TestStream {
        fn new(input: Vec<u8>) -> Self {
            Self { input: std::io::Cursor::new(input), output: Vec::new() }
        }
    }

    impl std::io::Read for TestStream {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            std::io::Read::read(&mut self.input, buffer)
        }
    }

    impl std::io::Write for TestStream {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.output.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }

    #[test]
    fn serves_requests_and_suppresses_notification_frames() {
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register("info", |_| Ok(json!({"state":"ready"}))).unwrap();
        dispatcher.register("notify", |_| Ok(Value::Null)).unwrap();

        let mut input = KlippyApiCodec::encode(&json!({"id":1,"method":"info"})).unwrap();
        input.extend(KlippyApiCodec::encode(&json!({"method":"notify"})).unwrap());
        let mut stream = TestStream::new(input);

        serve_stream(&mut stream, &dispatcher).unwrap();

        let mut decoder = KlippyApiCodec::default();
        assert_eq!(
            decoder.push(&stream.output).unwrap(),
            vec![json!({"id":1,"result":{"state":"ready"}})]
        );
    }

    #[test]
    fn reports_eof_inside_a_message() {
        let mut stream = TestStream::new(br#"{"id":1"#.to_vec());
        let error = serve_stream(&mut stream, &KlippyApiDispatcher::new()).unwrap_err();
        assert!(matches!(error, KlippyApiStreamError::TruncatedMessage { buffered: 7 }));
    }

    #[test]
    fn standard_endpoint_registration_exposes_live_printer_api() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.set_ready();
        printer.register_status_object("toolhead", serde_json::json!({
            "position": [1.0, 2.0, 3.0],
            "homed_axes": "xyz"
        })).unwrap();

        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        dispatcher.register_standard_endpoints(printer, "rustklipper-test").unwrap();

        let info = dispatcher.dispatch_value(json!({
            "id": 1, "method": "info", "params": {}
        })).unwrap();
        assert_eq!(info["result"]["state"], "ready");
        assert_eq!(info["result"]["software_version"], "rustklipper-test");

        let objects = dispatcher.dispatch_value(json!({
            "id": 2, "method": "objects/list", "params": {}
        })).unwrap();
        assert_eq!(objects["result"]["objects"], json!(["toolhead"]));

        let status = dispatcher.dispatch_value(json!({
            "id": 3, "method": "objects/query",
            "params": { "objects": { "toolhead": ["position"] } }
        })).unwrap();
        assert_eq!(status["result"]["status"]["toolhead"]["position"], json!([1.0, 2.0, 3.0]));
        assert!(status["result"]["eventtime"].as_f64().is_some());

        let endpoints = dispatcher.dispatch_value(json!({
            "id": 4, "method": "list_endpoints", "params": {}
        })).unwrap();
        assert_eq!(endpoints["result"]["endpoints"], json!([
            "emergency_stop",
            "gcode/help",
            "gcode/script",
            "gcode/subscribe_output",
            "info",
            "list_endpoints",
            "objects/list",
            "objects/query",
            "objects/subscribe"
        ]));
    }

    #[test]
    fn dispatcher_registers_methods_and_returns_results() {
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register("info", |_| Ok(json!({"state":"ready"}))).unwrap();
        let response = dispatcher.dispatch_value(json!({"id":9,"method":"info","params":{}})).unwrap();
        assert_eq!(response, json!({"id":9,"result":{"state":"ready"}}));
        assert_eq!(dispatcher.method_names(), vec!["gcode/subscribe_output", "info"]);
    }

    #[test]
    fn gcode_script_endpoint_executes_commands_in_order() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.set_ready();
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        for name in ["FIRST", "SECOND"] {
            let calls = calls.clone();
            printer.register_gcode_command(name, false, None, move |_| {
                calls.lock().unwrap().push(name.to_owned());
                Ok(Vec::new())
            }).unwrap();
        }

        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_gcode_script(printer).unwrap();
        let response = dispatcher.dispatch_value(json!({
            "id": 21,
            "method": "gcode/script",
            "params": { "script": "FIRST\nSECOND" }
        })).unwrap();

        assert_eq!(response, json!({ "id": 21, "result": {} }));
        assert_eq!(*calls.lock().unwrap(), vec!["FIRST", "SECOND"]);
    }

    #[test]
    fn gcode_script_endpoint_stops_after_the_first_command_error() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.set_ready();
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        printer.register_gcode_command("FAIL", false, None, |_| {
            Err(klipper_gcode::GCodeError::MalformedCommand {
                line: "FAIL".to_owned(),
            })
        }).unwrap();

        let observed = calls.clone();
        printer.register_gcode_command("AFTER", false, None, move |_| {
            observed.lock().unwrap().push("AFTER".to_owned());
            Ok(Vec::new())
        }).unwrap();

        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_gcode_script(printer).unwrap();
        let response = dispatcher.dispatch_value(json!({
            "id": 23,
            "method": "gcode/script",
            "params": { "script": "FAIL\nAFTER" }
        })).unwrap();

        assert_eq!(response["error"]["code"], json!(-32000));
        assert_eq!(*calls.lock().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn gcode_script_endpoint_rejects_missing_or_non_string_script() {
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_gcode_script(std::sync::Arc::new(crate::Printer::new())).unwrap();
        for params in [json!({}), json!({ "script": 42 })] {
            let response = dispatcher.dispatch_value(json!({
                "id": 22,
                "method": "gcode/script",
                "params": params
            })).unwrap();
            assert_eq!(response["error"]["code"], json!(-32600));
        }
    }

    #[test]
    fn emergency_stop_endpoint_shuts_down_the_printer() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.set_ready();
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_emergency_stop(printer.clone()).unwrap();

        let response = dispatcher
            .dispatch_value(json!({"id":3,"method":"emergency_stop"}))
            .unwrap();
        assert_eq!(response, json!({"id":3,"result":{}}));
        assert_eq!(printer.state(), crate::PrinterState::Shutdown);
        assert_eq!(printer.state_message(), "Emergency stop");
    }

    #[test]
    fn object_listing_endpoint_exposes_only_registered_status_objects() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", json!({"position": [0.0, 0.0, 0.0]})).unwrap();
        printer.register_object("internal_component", 2_u8).unwrap();

        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_object_listing(printer.clone()).unwrap();
        let request = json!({"id":2,"method":"objects/list"});
        let response = dispatcher.dispatch_value(request.clone()).unwrap();
        assert_eq!(response["result"]["objects"], json!(["toolhead"]));

        printer.remove_object("toolhead");
        let response = dispatcher.dispatch_value(request).unwrap();
        assert_eq!(response["result"]["objects"], json!([]));
    }

    #[test]
    fn object_query_returns_only_requested_fields_from_json_objects() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", serde_json::json!({
            "position": [1.0, 2.0, 3.0],
            "homed_axes": "xyz",
            "max_velocity": 300.0
        })).unwrap();
        // Arbitrary Rust objects are deliberately not serialized implicitly.
        printer.register_object("opaque", 42_u8).unwrap();

        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_object_query(printer.clone()).unwrap();
        let response = dispatcher.dispatch_value(json!({
            "id": 8,
            "method": "objects/query",
            "params": { "objects": {
                "toolhead": ["position", "homed_axes"],
                "opaque": ["anything"],
                "missing": ["field"]
            }}
        })).unwrap();
        assert_eq!(response["result"]["status"], json!({
            "toolhead": {
                "position": [1.0, 2.0, 3.0],
                "homed_axes": "xyz"
            },
            // Klipper first resolves unsupported/missing objects to an
            // empty status map, then returns null for explicitly requested fields.
            "opaque": {"anything": null},
            "missing": {"field": null}
        }));

        printer.update_status_object("toolhead", json!({
            "position": [4.0, 5.0, 6.0],
            "homed_axes": "xy"
        })).unwrap();
        let updated = dispatcher.dispatch_value(json!({
            "id": 10,
            "method": "objects/query",
            "params": { "objects": { "toolhead": ["position"] } }
        })).unwrap();
        assert_eq!(updated["result"]["status"]["toolhead"]["position"], json!([4.0, 5.0, 6.0]));
    }

    #[test]
    fn object_subscription_returns_initial_eventtime_and_selected_status() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", serde_json::json!({
            "position": [1.0, 2.0, 3.0],
            "homed_axes": "xyz"
        })).unwrap();

        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_object_subscription(printer).unwrap();
        let response = dispatcher.dispatch_value(json!({
            "id": 12,
            "method": "objects/subscribe",
            "params": { "objects": { "toolhead": ["position"] } }
        })).unwrap();

        assert_eq!(response["result"]["status"]["toolhead"], json!({
            "position": [1.0, 2.0, 3.0]
        }));
        assert!(response["result"]["eventtime"].as_f64().is_some());
    }

    #[test]
    fn object_query_resolves_requested_fields_on_missing_objects_to_null() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", json!({"position": [1.0, 2.0, 3.0]})).unwrap();
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_object_query(printer).unwrap();

        let response = dispatcher.dispatch_value(json!({
            "id": 31,
            "method": "objects/query",
            "params": {"objects": {
                "toolhead": ["position", "missing_field"],
                "not_loaded_yet": null
            }}
        })).unwrap();

        assert_eq!(response["result"]["status"], json!({
            "toolhead": {
                "position": [1.0, 2.0, 3.0],
                "missing_field": null
            },
            "not_loaded_yet": {}
        }));
    }

    #[test]
    fn null_subscription_fields_are_fixed_from_initial_snapshot() {
        let requested = serde_json::from_value(json!({
            "toolhead": null,
            "missing": null,
            "explicit": ["position"]
        })).unwrap();
        let snapshot = json!({"status": {
            "toolhead": {"position": [0.0, 0.0, 0.0], "homed_axes": "xyz"},
            "missing": {}
        }});

        assert_eq!(
            normalize_object_subscription(&requested, &snapshot),
            serde_json::from_value(json!({
                "toolhead": ["homed_axes", "position"],
                "missing": [],
                "explicit": ["position"]
            })).unwrap()
        );
    }

    #[test]
    fn object_query_rejects_malformed_field_selection() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_object("toolhead", serde_json::json!({"position": [0, 0, 0]})).unwrap();
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_object_query(printer).unwrap();
        let response = dispatcher.dispatch_value(json!({
            "id": 9,
            "method": "objects/query",
            "params": { "objects": { "toolhead": "position" } }
        })).unwrap();
        assert_eq!(response["error"]["code"], json!(-32600));
    }

    #[test]
    fn gcode_help_endpoint_tracks_commands_available_in_printer_state() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_gcode_command("M115", true, Some("Firmware version"), |_| {
            Ok(Vec::new())
        }).unwrap();
        printer.register_gcode_command("G1", false, Some("Move"), |_| {
            Ok(Vec::new())
        }).unwrap();

        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_gcode_help(printer.clone()).unwrap();
        let request = json!({"id": 11, "method": "gcode/help"});

        let before_ready = dispatcher.dispatch_value(request.clone()).unwrap();
        assert_eq!(before_ready["result"], json!({
            "M115": "Firmware version"
        }));

        printer.set_ready();
        let ready = dispatcher.dispatch_value(request).unwrap();
        assert_eq!(ready["result"], json!({
            "G1": "Move",
            "M115": "Firmware version"
        }));
    }

    #[test]
    fn printer_info_endpoint_tracks_lifecycle_state() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher
            .register_printer_info(printer.clone(), "RustKlipper 0.1.0")
            .unwrap();

        let request = json!({"id":1,"method":"info"});
        let startup = dispatcher.dispatch_value(request.clone()).unwrap();
        assert_eq!(startup["result"]["state"], "startup");
        assert_eq!(startup["result"]["state_message"], "Printer is not ready");
        assert_eq!(startup["result"]["software_version"], "RustKlipper 0.1.0");
        assert!(startup["result"]["process_id"].as_u64().is_some());

        printer.set_configuring();
        let configuring = dispatcher.dispatch_value(request.clone()).unwrap();
        assert_eq!(configuring["result"]["state"], "startup");
        assert_eq!(configuring["result"]["state_message"], "Printer is configuring");

        printer.set_ready();
        let ready = dispatcher.dispatch_value(request.clone()).unwrap();
        assert_eq!(ready["result"]["state"], "ready");
        assert_eq!(ready["result"]["state_message"], "Printer is ready");

        printer.invoke_shutdown("test shutdown");
        let shutdown = dispatcher.dispatch_value(request.clone()).unwrap();
        assert_eq!(shutdown["result"]["state"], "shutdown");
        assert_eq!(shutdown["result"]["state_message"], "test shutdown");

        printer.disconnect();
        let disconnected = dispatcher.dispatch_value(request).unwrap();
        assert_eq!(disconnected["result"]["state"], "error");
        assert_eq!(disconnected["result"]["state_message"], "Printer is disconnected");
    }

    #[test]
    fn printer_info_accepts_arbitrary_client_metadata_objects() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher
            .register_printer_info(printer, "RustKlipper 0.1.0")
            .unwrap();

        let response = dispatcher.dispatch_value(json!({
            "id": 1,
            "method": "info",
            "params": {
                "client_info": {
                    "name": "test-client",
                    "version": "1.2.3",
                    "custom": { "feature": true }
                }
            }
        })).unwrap();
        assert_eq!(response["result"]["software_version"], "RustKlipper 0.1.0");
    }

    #[test]
    fn printer_info_rejects_non_object_client_metadata() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher
            .register_printer_info(printer, "RustKlipper 0.1.0")
            .unwrap();

        let response = dispatcher.dispatch_value(json!({
            "id": 1,
            "method": "info",
            "params": { "client_info": "not-an-object" }
        })).unwrap();
        assert_eq!(response["error"]["code"], -32600);
        assert_eq!(
            response["error"]["message"],
            "info client_info parameter must be an object"
        );
    }

    #[test]
    fn endpoint_listing_returns_registered_method_names() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        dispatcher.register("info", |_| Ok(json!({"state":"ready"}))).unwrap();
        dispatcher.register("gcode/script", |_| Ok(json!("ok"))).unwrap();
        dispatcher.register_endpoint_listing().unwrap();

        let response = dispatcher
            .dispatch_value(json!({"id":5,"method":"list_endpoints","params":{}}))
            .unwrap();
        assert_eq!(
            response,
            json!({
                "id":5,
                "result":{"endpoints":["gcode/script","gcode/subscribe_output","info","list_endpoints"]}
            })
        );
    }

    #[test]
    fn dispatcher_returns_method_not_found_error() {
        let dispatcher = KlippyApiDispatcher::new();
        let response = dispatcher.dispatch_value(json!({"id":3,"method":"missing"})).unwrap();
        assert_eq!(response["id"], json!(3));
        assert_eq!(response["error"]["code"], json!(-32601));
    }

    #[test]
    fn dispatcher_suppresses_notification_responses() {
        let dispatcher = KlippyApiDispatcher::new();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        dispatcher.register("notify", move |_| {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Value::Null)
        }).unwrap();
        assert!(dispatcher.dispatch_value(json!({"method":"notify","params":{}})).is_none());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn dispatcher_rejects_duplicate_method_names() {
        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register("info", |_| Ok(Value::Null)).unwrap();
        assert!(dispatcher.register("info", |_| Ok(Value::Null)).is_err());
        assert!(dispatcher.unregister("info"));
        assert!(!dispatcher.unregister("info"));
    }
}


/// Serve Klippy API requests on any Tokio asynchronous byte stream.
///
/// The same handler can be used with Tokio TCP streams, Unix streams, Windows
/// named-pipe clients, and in-memory duplex streams. Platform-specific listener
/// creation remains the responsibility of the transport adapter.
pub async fn serve_async_stream<S>(
    stream: &mut S,
    dispatcher: &KlippyApiDispatcher,
) -> Result<(), KlippyApiStreamError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut codec = KlippyApiCodec::default();
    let mut chunk = [0u8; 8192];
    let mut output = dispatcher.subscribe_gcode_output();
    let mut output_template: Option<Map<String, Value>> = None;
    let printer = dispatcher.status_printer.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner).clone();
    // Subscribe before handling requests to avoid a gap after the initial snapshot.
    let mut status_updates = printer.as_ref().map(|printer| printer.subscribe_status_updates());
    let mut object_subscription: Option<Map<String, Value>> = None;
    let mut object_subscription_template: Option<Map<String, Value>> = None;
    // Last full status snapshot observed by this connection, used to emit
    // only changed fields in subsequent status notifications.
    let mut object_subscription_cache: Map<String, Value> = Map::new();

    loop {
        tokio::select! {
            read = reader.read(&mut chunk) => {
                let count = read?;
                if count == 0 {
                    return if codec.is_empty() {
                        Ok(())
                    } else {
                        Err(KlippyApiStreamError::TruncatedMessage {
                            buffered: codec.buffered_len(),
                        })
                    };
                }

                for message in codec.push(&chunk[..count])? {
                    let method = message.get("method").and_then(Value::as_str);
                    let output_template_request = if method == Some("gcode/subscribe_output") {
                        let params = message.get("params").cloned().unwrap_or_else(|| Value::Object(Map::new()));
                        Some(gcode_output_response_template(&params))
                    } else { None };
                    let object_subscription_request = if method == Some("objects/subscribe") {
                        Some(message.get("params").cloned().unwrap_or_else(|| Value::Object(Map::new())))
                    } else { None };

                    let response = dispatcher.dispatch_value(message);
                    if let Some(response) = response {
                        let accepted = response.get("error").is_none();
                        let encoded = KlippyApiCodec::encode(&response)?;
                        writer.write_all(&encoded).await?;
                        writer.flush().await?;

                        if accepted {
                            if let Some(Ok(template)) = output_template_request {
                                output = dispatcher.subscribe_gcode_output();
                                output_template = Some(template);
                            }
                            if let (Some(params), Some(_)) = (object_subscription_request, status_updates.as_ref()) {
                                if let (Ok(template), Some(objects)) = (
                                    object_status_response_template(&params),
                                    params.get("objects").and_then(Value::as_object),
                                ) {
                                    if objects.is_empty() {
                                        // An empty objects map cancels this connection's
                                        // previous subscription.
                                        object_subscription = None;
                                        object_subscription_template = None;
                                        object_subscription_cache.clear();
                                    } else {
                                        let snapshot = response.get("result").unwrap_or(&Value::Null);
                                        object_subscription = Some(normalize_object_subscription(objects, snapshot));
                                        object_subscription_template = Some(template);
                                        object_subscription_cache = status_cache_from_snapshot(snapshot);
                                    }
                                }
                            }
                        }
                    } else {
                        if let Some(Ok(template)) = output_template_request {
                            output = dispatcher.subscribe_gcode_output();
                            output_template = Some(template);
                        }
                        // JSON-RPC notifications have no response frame. Still
                        // activate a valid objects/subscribe notification after
                        // validating it, so clients may replace subscriptions
                        // without allocating a request id.
                        if let (Some(params), Some(printer)) =
                            (object_subscription_request, printer.as_ref())
                        {
                            if let Ok(snapshot) = query_printer_status(printer, &params) {
                                if let (Ok(template), Some(objects)) = (
                                    object_status_response_template(&params),
                                    params.get("objects").and_then(Value::as_object),
                                ) {
                                    if objects.is_empty() {
                                        object_subscription = None;
                                        object_subscription_template = None;
                                        object_subscription_cache.clear();
                                    } else {
                                        object_subscription = Some(normalize_object_subscription(objects, &snapshot));
                                        object_subscription_template = Some(template);
                                        object_subscription_cache = status_cache_from_snapshot(&snapshot);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            event = async {
                match status_updates.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending().await,
                }
            }, if status_updates.is_some() && object_subscription.is_some() => {
                match event {
                    Ok(object_name) => {
                        if let (Some(printer), Some(subscription)) = (printer.as_ref(), object_subscription.as_ref()) {
                            if let Some(fields) = subscription.get(&object_name) {
                                let requested = serde_json::json!({
                                    "objects": { (object_name.clone()): fields.clone() }
                                });
                                if let Ok(snapshot) = query_printer_status(printer, &requested) {
                                    if let Some(params) = changed_status_snapshot(
                                        &snapshot,
                                        &mut object_subscription_cache,
                                    ) {
                                        let mut notification = object_subscription_template.clone()
                                            .expect("object response template is present while subscribed");
                                        notification.insert("params".to_owned(), params);
                                        let encoded = KlippyApiCodec::encode(&Value::Object(notification))?;
                                        writer.write_all(&encoded).await?;
                                        writer.flush().await?;
                                    }
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        if let (Some(printer), Some(subscription)) = (printer.as_ref(), object_subscription.as_ref()) {
                            let requested = Value::Object(Map::from_iter([
                                ("objects".to_owned(), Value::Object(subscription.clone()))
                            ]));
                            if let Ok(snapshot) = query_printer_status(printer, &requested) {
                                if let Some(params) = changed_status_snapshot(
                                    &snapshot,
                                    &mut object_subscription_cache,
                                ) {
                                    let mut notification = object_subscription_template.clone()
                                        .expect("object response template is present while subscribed");
                                    notification.insert("params".to_owned(), params);
                                    let encoded = KlippyApiCodec::encode(&Value::Object(notification))?;
                                    writer.write_all(&encoded).await?;
                                    writer.flush().await?;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => status_updates = None,
                }
            }
            event = output.recv(), if output_template.is_some() => {
                match event {
                    Ok(response) => {
                        let mut notification = output_template.clone().expect("output template is present while subscribed");
                        notification.insert("params".to_owned(), serde_json::json!({ "response": response }));
                        let encoded = KlippyApiCodec::encode(&Value::Object(notification))?;
                        writer.write_all(&encoded).await?;
                        writer.flush().await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => output_template = None,
                }
            }
        }
    }
}

#[cfg(test)]
mod async_stream_tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn object_subscription_notifications_include_only_changed_fields() {
        let initial = json!({
            "eventtime": 1.0,
            "status": {
                "toolhead": {
                    "position": [0.0, 0.0, 0.0],
                    "homed_axes": ""
                },
                "motion_report": {
                    "live_position": [1.0, 2.0, 3.0]
                }
            }
        });
        let mut cache = status_cache_from_snapshot(&initial);

        let update = json!({
            "eventtime": 2.0,
            "status": {
                "toolhead": {
                    "position": [0.0, 0.0, 0.0],
                    "homed_axes": "xyz",
                    "new_field": true
                },
                "motion_report": {
                    "live_position": [1.0, 2.0, 3.0]
                }
            }
        });
        assert_eq!(
            changed_status_snapshot(&update, &mut cache),
            Some(json!({
                "eventtime": 2.0,
                "status": {
                    "toolhead": {
                        "homed_axes": "xyz",
                        "new_field": true
                    }
                }
            }))
        );

        // Re-reading the same snapshot must not generate a duplicate event.
        assert_eq!(changed_status_snapshot(&update, &mut cache), None);
    }

    #[tokio::test]
    async fn serves_requests_and_suppresses_notifications() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        dispatcher
            .register("info", |_| Ok(json!({"state":"ready"})))
            .unwrap();
        dispatcher
            .register("notify", |_| Ok(Value::Null))
            .unwrap();

        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });

        let mut input = KlippyApiCodec::encode(&json!({"id":1,"method":"info"})).unwrap();
        input.extend(KlippyApiCodec::encode(&json!({"method":"notify"})).unwrap());
        client.write_all(&input).await.unwrap();
        client.shutdown().await.unwrap();

        let mut output = Vec::new();
        client.read_to_end(&mut output).await.unwrap();
        server_task.await.unwrap().unwrap();

        let mut decoder = KlippyApiCodec::default();
        assert_eq!(
            decoder.push(&output).unwrap(),
            vec![json!({"id":1,"result":{"state":"ready"}})]
        );
    }

    #[tokio::test]
    async fn subscribed_client_receives_gcode_script_output_notifications() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.set_ready();
        printer.register_gcode_command("REPORT", false, None, |_| {
            Ok(vec![DispatchEvent::Info("temperature is 205".to_owned())])
        }).unwrap();
        dispatcher.register_gcode_script(printer).unwrap();

        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });

        let subscribe = KlippyApiCodec::encode(&json!({
            "id": 1,
            "method": "gcode/subscribe_output",
            "params": {
                "response_template": { "method": "process_gcode_response", "source": "terminal" }
            }
        })).unwrap();
        client.write_all(&subscribe).await.unwrap();

        let mut response_bytes = vec![0u8; 256];
        let response_len = client.read(&mut response_bytes).await.unwrap();
        let mut decoder = KlippyApiCodec::default();
        let responses = decoder.push(&response_bytes[..response_len]).unwrap();
        assert_eq!(responses, vec![json!({"id":1,"result":{}})]);

        let script = KlippyApiCodec::encode(&json!({
            "id": 2,
            "method": "gcode/script",
            "params": { "script": "REPORT" }
        })).unwrap();
        client.write_all(&script).await.unwrap();

        let mut messages: Vec<Value> = Vec::new();
        while !messages.iter().any(|message| {
            message["method"] == "process_gcode_response"
        }) || !messages.iter().any(|message| message["id"] == 2) {
            let mut chunk = [0u8; 256];
            let count = client.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "connection closed before script output arrived");
            messages.extend(decoder.push(&chunk[..count]).unwrap());
        }

        assert!(messages.contains(&json!({
            "method": "process_gcode_response",
            "source": "terminal",
            "params": { "response": "// temperature is 205" }
        })));
        assert!(messages.contains(&json!({"id":2,"result":{}})));

        client.shutdown().await.unwrap();
        server_task.abort();
    }

    #[tokio::test]
    async fn subscribed_output_uses_empty_default_response_template() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.set_ready();
        printer.register_gcode_command("REPORT", false, None, |_| {
            Ok(vec![DispatchEvent::Raw("raw output".to_owned())])
        }).unwrap();
        dispatcher.register_gcode_script(printer).unwrap();

        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });

        let subscribe = KlippyApiCodec::encode(&json!({
            "id": 1,
            "method": "gcode/subscribe_output"
        })).unwrap();
        client.write_all(&subscribe).await.unwrap();

        let mut decoder = KlippyApiCodec::default();
        let mut received = Vec::<Value>::new();
        while !received.iter().any(|message| message["id"] == 1) {
            let mut chunk = [0u8; 256];
            let count = client.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0);
            received.extend(decoder.push(&chunk[..count]).unwrap());
        }

        let script = KlippyApiCodec::encode(&json!({
            "id": 2,
            "method": "gcode/script",
            "params": { "script": "REPORT" }
        })).unwrap();
        client.write_all(&script).await.unwrap();

        while !received.iter().any(|message| {
            message["params"]["response"] == "raw output"
        }) || !received.iter().any(|message| message["id"] == 2) {
            let mut chunk = [0u8; 256];
            let count = client.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0);
            received.extend(decoder.push(&chunk[..count]).unwrap());
        }

        assert!(received.contains(&json!({
            "params": { "response": "raw output" }
        })));

        client.shutdown().await.unwrap();
        server_task.abort();
    }

    #[tokio::test]
    async fn rejects_non_object_output_response_template() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let (mut client, mut server) = tokio::io::duplex(1024);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });

        let request = KlippyApiCodec::encode(&json!({
            "id": 7,
            "method": "gcode/subscribe_output",
            "params": { "response_template": "invalid" }
        })).unwrap();
        client.write_all(&request).await.unwrap();

        let mut bytes = [0u8; 256];
        let count = client.read(&mut bytes).await.unwrap();
        let mut decoder = KlippyApiCodec::default();
        let messages = decoder.push(&bytes[..count]).unwrap();
        assert_eq!(messages[0]["id"], json!(7));
        assert_eq!(messages[0]["error"]["code"], json!(-32600));

        client.shutdown().await.unwrap();
        server_task.abort();
    }

    #[tokio::test]
    async fn objects_subscribe_streams_status_update_notifications() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", json!({
            "position": [0.0, 0.0, 0.0],
            "homed_axes": ""
        })).unwrap();
        dispatcher.register_standard_endpoints(printer.clone(), "test").unwrap();

        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });

        let subscribe = KlippyApiCodec::encode(&json!({
            "id": 10,
            "method": "objects/subscribe",
            "params": {
                "objects": { "toolhead": ["position"] },
                "response_template": { "method": "notify_status_update", "source": "printer" }
            }
        })).unwrap();
        client.write_all(&subscribe).await.unwrap();

        let mut decoder = KlippyApiCodec::default();
        let mut messages = Vec::<Value>::new();
        while !messages.iter().any(|message| message["id"] == 10) {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0, "connection closed before initial subscription response");
            messages.extend(decoder.push(&chunk[..count]).unwrap());
        }
        assert_eq!(
            messages.iter().find(|message| message["id"] == 10).unwrap()["result"]["status"]["toolhead"]["position"],
            json!([0.0, 0.0, 0.0])
        );

        printer.update_status_object("toolhead", json!({
            "position": [12.0, 34.0, 56.0],
            "homed_axes": "xyz"
        })).unwrap();

        let notification = loop {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0, "connection closed before status notification");
            let decoded = decoder.push(&chunk[..count]).unwrap();
            if let Some(notification) = decoded.into_iter().find(|message| {
                message["method"] == "notify_status_update"
            }) {
                break notification;
            }
        };

        assert_eq!(
            notification["params"]["status"]["toolhead"]["position"],
            json!([12.0, 34.0, 56.0])
        );
        assert!(notification["params"]["status"]["toolhead"].get("homed_axes").is_none());
        assert!(notification["params"]["eventtime"].as_f64().is_some());
        assert_eq!(notification["method"], "notify_status_update");
        assert_eq!(notification["source"], "printer");

        client.shutdown().await.unwrap();
        server_task.abort();
    }

    #[tokio::test]
    async fn objects_subscribe_notification_replaces_connection_subscription() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", json!({
            "position": [0.0, 0.0, 0.0]
        })).unwrap();
        dispatcher.register_standard_endpoints(printer.clone(), "test").unwrap();

        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });

        let mut decoder = KlippyApiCodec::default();
        client.write_all(&KlippyApiCodec::encode(&json!({
            "method": "objects/subscribe",
            "params": {
                "objects": { "toolhead": ["position"] }
            }
        })).unwrap()).await.unwrap();

        // Notifications have no response. A following request/response pair
        // acts as a synchronization barrier so the subscription is installed
        // before the test publishes its status update.
        client.write_all(&KlippyApiCodec::encode(&json!({
            "id": 99,
            "method": "info"
        })).unwrap()).await.unwrap();

        let mut barrier_received = false;
        while !barrier_received {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0, "connection closed before subscription synchronization response");
            barrier_received = decoder.push(&chunk[..count]).unwrap().into_iter().any(|message| {
                message["id"] == json!(99) && message["result"]["state"] == json!("startup")
            });
        }

        printer.update_status_object("toolhead", json!({
            "position": [4.0, 5.0, 6.0]
        })).unwrap();

        let notification = loop {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0, "connection closed before notification subscription became active");
            if let Some(message) = decoder.push(&chunk[..count]).unwrap().into_iter().find(|message| {
                message["params"]["status"]["toolhead"]["position"] == json!([4.0, 5.0, 6.0])
            }) {
                break message;
            }
        };
        assert!(notification.get("method").is_none());
        assert_eq!(
            notification["params"]["status"]["toolhead"]["position"],
            json!([4.0, 5.0, 6.0])
        );

        client.shutdown().await.unwrap();
        server_task.abort();
    }

    #[tokio::test]
    async fn empty_objects_subscription_cancels_previous_subscription() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", json!({
            "position": [0.0, 0.0, 0.0]
        })).unwrap();
        dispatcher.register_standard_endpoints(printer.clone(), "test").unwrap();

        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });
        let mut decoder = KlippyApiCodec::default();

        // A following request is the synchronization barrier: subscription
        // activation happens after its own response has been written.
        let subscribe = KlippyApiCodec::encode(&json!({
            "id": 1,
            "method": "objects/subscribe",
            "params": { "objects": { "toolhead": ["position"] } }
        })).unwrap();
        client.write_all(&subscribe).await.unwrap();
        let barrier = KlippyApiCodec::encode(&json!({
            "id": 10,
            "method": "info"
        })).unwrap();
        client.write_all(&barrier).await.unwrap();

        let mut received_barrier = false;
        while !received_barrier {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0);
            received_barrier = decoder.push(&chunk[..count]).unwrap().into_iter().any(|message| {
                message["id"] == 10
            });
        }

        let unsubscribe = KlippyApiCodec::encode(&json!({
            "id": 2,
            "method": "objects/subscribe",
            "params": { "objects": {} }
        })).unwrap();
        client.write_all(&unsubscribe).await.unwrap();
        let barrier = KlippyApiCodec::encode(&json!({
            "id": 20,
            "method": "info"
        })).unwrap();
        client.write_all(&barrier).await.unwrap();

        let mut received_unsubscribe_barrier = false;
        while !received_unsubscribe_barrier {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0);
            received_unsubscribe_barrier = decoder.push(&chunk[..count]).unwrap().into_iter().any(|message| {
                message["id"] == 20
            });
        }

        printer.update_status_object("toolhead", json!({
            "position": [9.0, 8.0, 7.0]
        })).unwrap();
        let mut chunk = [0u8; 512];
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            client.read(&mut chunk)
        ).await.is_err(), "status notification was sent after the subscription was cancelled");

        client.shutdown().await.unwrap();
        server_task.abort();
    }

    #[tokio::test]
    async fn removing_status_object_notifies_subscribers_that_fields_are_missing() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", json!({
            "position": [0.0, 0.0, 0.0]
        })).unwrap();
        dispatcher.register_standard_endpoints(printer.clone(), "test").unwrap();

        let (mut client, mut server) = tokio::io::duplex(4096);
        let server_dispatcher = dispatcher.clone();
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &server_dispatcher).await
        });
        let mut decoder = KlippyApiCodec::default();

        client.write_all(&KlippyApiCodec::encode(&json!({
            "id": 1,
            "method": "objects/subscribe",
            "params": {
                "objects": { "toolhead": ["position"] },
                "response_template": { "method": "notify_status_update" }
            }
        })).unwrap()).await.unwrap();

        let mut subscribed = false;
        while !subscribed {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0);
            subscribed = decoder.push(&chunk[..count]).unwrap().into_iter().any(|message| {
                message["id"] == 1
            });
        }

        // Wait until the server has installed the subscription after writing
        // the initial response.
        client.write_all(&KlippyApiCodec::encode(&json!({
            "id": 2,
            "method": "info"
        })).unwrap()).await.unwrap();
        let mut synchronized = false;
        while !synchronized {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0);
            synchronized = decoder.push(&chunk[..count]).unwrap().into_iter().any(|message| {
                message["id"] == 2
            });
        }

        assert!(printer.remove_status_object("toolhead"));
        let notification = loop {
            let mut chunk = [0u8; 512];
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read(&mut chunk)
            ).await.unwrap().unwrap();
            assert_ne!(count, 0, "connection closed before removal notification");
            if let Some(message) = decoder.push(&chunk[..count]).unwrap().into_iter().find(|message| {
                message["method"] == "notify_status_update"
            }) {
                break message;
            }
        };

        assert_eq!(
            notification["params"]["status"]["toolhead"]["position"],
            Value::Null
        );
        assert_eq!(notification["params"]["status"]["toolhead"].as_object().unwrap().len(), 1);

        client.shutdown().await.unwrap();
        server_task.abort();
    }

    #[test]
    fn objects_subscribe_rejects_non_object_response_templates() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_status_object("toolhead", json!({"position": [0.0, 0.0, 0.0]})).unwrap();
        dispatcher.register_standard_endpoints(printer, "test").unwrap();

        let response = dispatcher.dispatch_value(json!({
            "id": 9,
            "method": "objects/subscribe",
            "params": {
                "objects": { "toolhead": ["position"] },
                "response_template": []
            }
        })).unwrap();
        assert_eq!(response["id"], 9);
        assert_eq!(response["error"]["code"], -32600);
    }

    #[tokio::test]
    async fn reports_eof_inside_an_async_message() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let server_task = tokio::spawn(async move {
            serve_async_stream(&mut server, &KlippyApiDispatcher::new()).await
        });

        client.write_all(br#"{"id":1"#).await.unwrap();
        client.shutdown().await.unwrap();

        assert!(matches!(
            server_task.await.unwrap(),
            Err(KlippyApiStreamError::TruncatedMessage { buffered: 7 })
        ));
    }
}


/// Accept TCP clients and serve each connection independently.
///
/// TCP provides a cross-platform transport for the Klippy API. On Windows,
/// this can be used directly; Unix-domain sockets and named pipes can be
/// supplied to `serve_async_stream` through their respective adapters.
pub async fn serve_tcp_listener(
    listener: tokio::net::TcpListener,
    dispatcher: std::sync::Arc<KlippyApiDispatcher>,
) -> std::io::Result<()> {
    loop {
        let (mut stream, peer) = listener.accept().await?;
        let dispatcher = dispatcher.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_async_stream(&mut stream, &dispatcher).await {
                tracing::debug!(%peer, %error, "Klippy API TCP connection ended");
            }
        });
    }
}

#[cfg(test)]
mod tcp_listener_tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn accepts_tcp_clients_and_dispatches_requests() {
        let dispatcher = std::sync::Arc::new(KlippyApiDispatcher::new());
        dispatcher
            .register("info", |_| Ok(json!({"state":"ready"})))
            .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_task = tokio::spawn(serve_tcp_listener(listener, dispatcher));

        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = KlippyApiCodec::encode(&json!({"id":17,"method":"info"})).unwrap();
        client.write_all(&request).await.unwrap();
        client.shutdown().await.unwrap();

        let mut output = Vec::new();
        client.read_to_end(&mut output).await.unwrap();
        server_task.abort();

        let mut decoder = KlippyApiCodec::default();
        assert_eq!(
            decoder.push(&output).unwrap(),
            vec![json!({"id":17,"result":{"state":"ready"}})]
        );
    }
}
