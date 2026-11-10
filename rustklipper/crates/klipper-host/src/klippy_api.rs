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
            let parsed = serde_json::from_slice::<Value>(&self.buffer[consumed..end])
                .map_err(|error| KlippyApiCodecError::InvalidJson(error.to_string()))?;
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
}
