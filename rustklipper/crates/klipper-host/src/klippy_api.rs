//! Portable Klippy API message framing shared by Unix sockets and Windows named pipes.
//!
//! Klippy's API stream is a sequence of JSON values, each terminated by ETX
//! (0x03). This framing layer is independent of the socket type so it can be
//! reused above Unix sockets, Windows named pipes, or an in-process transport.

use serde_json::{Map, Value};
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
#[derive(Default)]
pub struct KlippyApiDispatcher {
    methods: std::sync::RwLock<std::collections::HashMap<String, KlippyApiMethod>>,
}

impl KlippyApiDispatcher {
    pub fn new() -> Self { Self::default() }

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
            self.input.read(buffer)
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
        assert_eq!(dispatcher.method_names(), vec!["info"]);
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
