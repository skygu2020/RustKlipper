use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

/// JSON-RPC request identifier. A missing identifier denotes a notification;
/// an explicit null identifier is represented by `RpcId::Null`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    String(String),
    Number(Number),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RpcRequest {
    pub method: String,
    pub params: Value,
    pub id: Option<RpcId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), data: None }
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn parse_error() -> Self {
        Self::new(-32700, "Parse error")
    }

    pub fn invalid_request() -> Self {
        Self::new(-32600, "Invalid Request")
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, format!("Method not found: {method}"))
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, message)
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(-32603, message)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: RpcId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    fn success(id: RpcId, result: Value) -> Self {
        Self { jsonrpc: "2.0".into(), id, result: Some(result), error: None }
    }

    fn failure(id: RpcId, error: RpcError) -> Self {
        Self { jsonrpc: "2.0".into(), id, result: None, error: Some(error) }
    }
}

type RpcHandler = Arc<dyn Fn(Value) -> Result<Value, RpcError> + Send + Sync>;

/// Method registry and JSON-RPC 2.0 request dispatcher.
///
/// Handlers receive either an object or array parameter value. Notifications
/// are dispatched normally but never produce a response, including on error.
#[derive(Default)]
pub struct JsonRpcRouter {
    methods: HashMap<String, RpcHandler>,
}

impl JsonRpcRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, method: impl Into<String>, handler: F) -> Result<(), RpcError>
    where
        F: Fn(Value) -> Result<Value, RpcError> + Send + Sync + 'static,
    {
        let method = method.into();
        if method.trim().is_empty() {
            return Err(RpcError::invalid_params("RPC method name cannot be empty"));
        }
        if self.methods.contains_key(&method) {
            return Err(RpcError::invalid_params(format!(
                "RPC method already registered: {method}"
            )));
        }
        self.methods.insert(method, Arc::new(handler));
        Ok(())
    }

    pub fn unregister(&mut self, method: &str) -> bool {
        self.methods.remove(method).is_some()
    }

    pub fn contains_method(&self, method: &str) -> bool {
        self.methods.contains_key(method)
    }

    /// Parse and dispatch one JSON document. Invalid JSON always produces a
    /// parse-error response; a valid notification-only document returns None.
    pub fn handle_json(&self, input: &str) -> Option<String> {
        match serde_json::from_str::<Value>(input) {
            Ok(value) => self.handle_value(value)
                .and_then(|response| serde_json::to_string(&response).ok()),
            Err(_) => serde_json::to_string(&RpcResponse::failure(
                RpcId::Null,
                RpcError::parse_error(),
            )).ok(),
        }
    }

    /// Dispatch an already decoded JSON value, including JSON-RPC batches.
    pub fn handle_value(&self, value: Value) -> Option<Value> {
        match value {
            Value::Array(items) if items.is_empty() => Some(
                serde_json::to_value(RpcResponse::failure(
                    RpcId::Null,
                    RpcError::invalid_request(),
                )).expect("RPC response serialization is infallible"),
            ),
            Value::Array(items) => {
                let responses: Vec<Value> = items.into_iter()
                    .filter_map(|item| self.handle_single(item))
                    .map(|response| {
                        serde_json::to_value(response)
                            .expect("RPC response serialization is infallible")
                    })
                    .collect();
                if responses.is_empty() {
                    None
                } else {
                    Some(Value::Array(responses))
                }
            }
            item => self.handle_single(item).map(|response| {
                serde_json::to_value(response)
                    .expect("RPC response serialization is infallible")
            }),
        }
    }

    fn handle_single(&self, value: Value) -> Option<RpcResponse> {
        let Value::Object(mut object) = value else {
            return Some(RpcResponse::failure(
                RpcId::Null,
                RpcError::invalid_request(),
            ));
        };

        let id = match object.remove("id") {
            None => None,
            Some(Value::String(value)) => Some(RpcId::String(value)),
            Some(Value::Number(value)) => Some(RpcId::Number(value)),
            Some(Value::Null) => Some(RpcId::Null),
            Some(_) => return Some(RpcResponse::failure(
                RpcId::Null,
                RpcError::invalid_request(),
            )),
        };

        let invalid = || RpcResponse::failure(
            id.clone().unwrap_or(RpcId::Null),
            RpcError::invalid_request(),
        );

        if object.get("jsonrpc") != Some(&Value::String("2.0".into())) {
            return Some(invalid());
        }
        let Some(Value::String(method)) = object.remove("method") else {
            return Some(invalid());
        };
        if method.is_empty() {
            return Some(invalid());
        }
        let params = match object.remove("params") {
            None => Value::Null,
            Some(value @ Value::Array(_)) | Some(value @ Value::Object(_)) => value,
            Some(_) => return Some(invalid()),
        };

        let Some(id) = id else {
            // JSON-RPC notifications must not receive any response, even if
            // the method is unknown or its handler returns an error.
            if let Some(handler) = self.methods.get(&method) {
                let _ = handler(params);
            }
            return None;
        };

        match self.methods.get(&method) {
            Some(handler) => Some(match handler(params) {
                Ok(result) => RpcResponse::success(id, result),
                Err(error) => RpcResponse::failure(id, error),
            }),
            None => Some(RpcResponse::failure(
                id,
                RpcError::method_not_found(&method),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn router() -> JsonRpcRouter {
        let mut router = JsonRpcRouter::new();
        router.register("sum", |params| {
            let values = params.as_array()
                .ok_or_else(|| RpcError::invalid_params("expected an array"))?;
            let sum: i64 = values.iter().map(|value| value.as_i64().unwrap_or(0)).sum();
            Ok(json!(sum))
        }).unwrap();
        router
    }

    #[test]
    fn dispatches_request_and_preserves_identifier() {
        let response: RpcResponse = serde_json::from_str(
            &router().handle_json(r#"{"jsonrpc":"2.0","method":"sum","params":[2,3],"id":"req-1"}"#).unwrap()
        ).unwrap();
        assert_eq!(response.id, RpcId::String("req-1".into()));
        assert_eq!(response.result, Some(json!(5)));
        assert!(response.error.is_none());
    }

    #[test]
    fn notifications_never_produce_responses() {
        assert!(router().handle_json(r#"{"jsonrpc":"2.0","method":"sum","params":[2,3]}"#).is_none());
        assert!(router().handle_json(r#"{"jsonrpc":"2.0","method":"missing"}"#).is_none());
    }

    #[test]
    fn reports_standard_request_and_method_errors() {
        let router = router();
        let invalid: RpcResponse = serde_json::from_str(
            &router.handle_json(r#"{"jsonrpc":"1.0","method":"sum","id":1}"#).unwrap()
        ).unwrap();
        assert_eq!(invalid.error.unwrap().code, -32600);

        let missing: RpcResponse = serde_json::from_str(
            &router.handle_json(r#"{"jsonrpc":"2.0","method":"missing","id":2}"#).unwrap()
        ).unwrap();
        assert_eq!(missing.error.unwrap().code, -32601);
    }

    #[test]
    fn handles_batches_and_suppresses_notification_entries() {
        let response: Value = serde_json::from_str(
            &router().handle_json(concat!(
                r#"[{"jsonrpc":"2.0","method":"sum","params":[1,4],"id":7},"#,
                r#"{"jsonrpc":"2.0","method":"sum","params":[9]}]"#
            )).unwrap()
        ).unwrap();
        assert_eq!(response, json!([{
            "jsonrpc": "2.0",
            "id": 7,
            "result": 5
        }]));
    }

    #[test]
    fn parse_errors_and_empty_batches_receive_errors() {
        let parse: RpcResponse = serde_json::from_str(
            &router().handle_json("{").unwrap()
        ).unwrap();
        assert_eq!(parse.error.unwrap().code, -32700);

        let empty: RpcResponse = serde_json::from_value(
            router().handle_value(json!([])).unwrap()
        ).unwrap();
        assert_eq!(empty.error.unwrap().code, -32600);
    }

    #[test]
    fn rejects_null_and_scalar_params_but_allows_omitted_params() {
        let router = router();

        for input in [
            r#"{"jsonrpc":"2.0","method":"sum","params":null,"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"sum","params":5,"id":2}"#,
            r#"{"jsonrpc":"2.0","method":"sum","params":"invalid","id":3}"#,
            r#"{"jsonrpc":"2.0","method":"sum","params":true,"id":4}"#,
        ] {
            let response: RpcResponse =
                serde_json::from_str(&router.handle_json(input).unwrap()).unwrap();
            assert_eq!(response.error.unwrap().code, -32600);
        }

        let omitted: RpcResponse = serde_json::from_str(
            &router.handle_json(r#"{"jsonrpc":"2.0","method":"missing","id":5}"#).unwrap()
        ).unwrap();
        assert_eq!(omitted.error.unwrap().code, -32601);
    }

    #[test]
    fn rejects_duplicate_and_empty_method_names() {
        let mut router = router();
        assert!(router.register("sum", |_| Ok(Value::Null)).is_err());
        assert!(router.register("  ", |_| Ok(Value::Null)).is_err());
        assert!(router.unregister("sum"));
        assert!(!router.contains_method("sum"));
    }
}
