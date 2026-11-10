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
        Ok(Self {
            id: object.get("id").cloned(),
            method,
            params: object.get("params").cloned()
                .unwrap_or_else(|| Value::Object(Map::new())),
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
        };
        // Output subscription is connection-local in the async transport.
        // The method itself is a no-op; the stream adapter enables delivery
        // only for the connection that requested it.
        dispatcher.register("gcode/subscribe_output", |_| Ok(serde_json::json!({})))
            .expect("built-in endpoint name is unique");
        dispatcher
    }

    fn subscribe_gcode_output(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.gcode_output.subscribe()
    }

    /// Register Klippy's `objects/list` endpoint from the printer object registry.
    ///
    /// Only objects currently registered by loaded host components are exposed.
    pub fn register_object_listing(
        &self,
        printer: std::sync::Arc<crate::Printer>,
    ) -> Result<(), KlippyApiRpcError> {
        self.register("objects/list", move |_params| {
            Ok(serde_json::json!({
                "objects": printer.object_names(),
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
            let requested = params
                .get("objects")
                .and_then(Value::as_object)
                .ok_or_else(|| KlippyApiRpcError::invalid_request(
                    "objects/query requires an objects mapping"
                ))?;
            let mut status = serde_json::Map::new();
            for (object_name, requested_fields) in requested {
                let object = match printer.lookup_object_optional::<Value>(object_name) {
                    Ok(Some(object)) => object,
                    Ok(None) | Err(crate::PrinterError::ObjectTypeMismatch { .. }) => continue,
                    Err(error) => return Err(KlippyApiRpcError::internal(error.to_string())),
                };
                let Some(fields) = object.as_object() else {
                    continue;
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
                            if let Some(value) = fields.get(name) {
                                selected.insert(name.to_owned(), value.clone());
                            }
                        }
                        selected
                    }
                    Value::Null => fields.clone(),
                    _ => return Err(KlippyApiRpcError::invalid_request(
                        "each objects entry must be an array of field names or null"
                    )),
                };
                status.insert(object_name.clone(), Value::Object(selected));
            }
            Ok(serde_json::json!({ "status": status }))
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
            for outcome in printer.process_gcode_script(script) {
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
        self.register("info", move |_params| {
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
    fn object_listing_endpoint_tracks_registered_objects() {
        let printer = std::sync::Arc::new(crate::Printer::new());
        printer.register_object("toolhead", 1_u8).unwrap();
        printer.register_object("gcode", 2_u8).unwrap();

        let dispatcher = KlippyApiDispatcher::new();
        dispatcher.register_object_listing(printer.clone()).unwrap();
        let request = json!({"id":2,"method":"objects/list"});
        let response = dispatcher.dispatch_value(request.clone()).unwrap();
        assert_eq!(response["result"]["objects"], json!(["gcode", "toolhead"]));

        printer.remove_object("gcode");
        let response = dispatcher.dispatch_value(request).unwrap();
        assert_eq!(response["result"]["objects"], json!(["toolhead"]));
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
            }
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
    let mut subscribed = false;

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
                    if message.get("method").and_then(Value::as_str)
                        == Some("gcode/subscribe_output")
                    {
                        // Start at the subscription point so output emitted
                        // before this request is never replayed to the client.
                        output = dispatcher.subscribe_gcode_output();
                        subscribed = true;
                    }
                    if let Some(response) = dispatcher.dispatch_value(message) {
                        let encoded = KlippyApiCodec::encode(&response)?;
                        writer.write_all(&encoded).await?;
                        writer.flush().await?;
                    }
                }
            }
            event = output.recv(), if subscribed => {
                match event {
                    Ok(response) => {
                        let notification = serde_json::json!({
                            "method": "process_gcode_response",
                            "params": { "response": response },
                        });
                        let encoded = KlippyApiCodec::encode(&notification)?;
                        writer.write_all(&encoded).await?;
                        writer.flush().await?;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // A slow client may miss old console lines; keep the
                        // connection alive and deliver subsequent output.
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        subscribed = false;
                    }
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
            "params": {}
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
            "params": { "response": "// temperature is 205" }
        })));
        assert!(messages.contains(&json!({"id":2,"result":{}})));

        client.shutdown().await.unwrap();
        server_task.abort();
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
