//! Klipper host-side G-code parsing primitives.
//!
//! The tokenization follows the command/parameter splitting strategy used by
//! `klippy/gcode.py`. Dispatch, printer lifecycle, and macro evaluation are
//! intentionally implemented in higher layers.

pub mod dispatch;
pub use dispatch::{DispatchEvent, DispatchOutcome, GCodeDispatcher};

use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;
use thiserror::Error;

fn argument_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r"([A-Z_]+|[A-Z*])").expect("static G-code regex"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLine {
    /// Parsed command name (for example `G1` or `SET_HEATER_TEMPERATURE`).
    pub command: String,
    /// Original trimmed input line, including comments and line number.
    pub original: String,
    /// Legacy letter parameters, matching Klipper's regex-based parser.
    pub parameters: HashMap<String, String>,
    /// Optional leading line number from `N123 ...`.
    pub line_number: Option<u32>,
}

impl ParsedLine {
    /// Return the unparsed parameter text following the command.
    pub fn raw_parameters(&self) -> &str {
        let mut start = self.original.len();
        if let Some(position) = self.original.to_ascii_uppercase().find(&self.command) {
            start = position + self.command.len();
        }
        let mut end = self.original.len();
        if let Some(star) = self.original.rfind('*') {
            if self.original[star + 1..].trim().chars().all(|ch| ch.is_ascii_digit())
                && !self.original[star + 1..].trim().is_empty()
            {
                end = star;
            }
        }
        let raw = &self.original[start.min(end)..end];
        raw.strip_prefix(char::is_whitespace).unwrap_or(raw)
    }

    /// Parse extended `KEY=value` parameters, including quoted values.
    pub fn extended_parameters(&self) -> Result<HashMap<String, String>, GCodeError> {
        let tokens = shell_tokens(self.raw_parameters()).map_err(|_| {
            GCodeError::MalformedCommand { line: self.original.clone() }
        })?;
        let mut result = HashMap::new();
        for token in tokens {
            let Some((key, value)) = token.split_once('=') else {
                return Err(GCodeError::MalformedCommand { line: self.original.clone() });
            };
            result.insert(key.to_ascii_uppercase(), value.to_owned());
        }
        Ok(result)
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.parameters.get(&name.to_ascii_uppercase()).map(String::as_str)
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum GCodeError {
    #[error("Error on '{line}': missing {parameter}")]
    MissingParameter { line: String, parameter: String },
    #[error("Error on '{line}': unable to parse {parameter} value '{value}'")]
    InvalidParameter { line: String, parameter: String, value: String },
    #[error("Error on '{line}': {parameter} must be {constraint}")]
    OutOfRange { line: String, parameter: String, constraint: String },
    #[error("The value '{value}' is not valid for {key}{hint}")]
    InvalidMuxValue { key: String, value: String, hint: String },
    #[error("Malformed command '{line}'")]
    MalformedCommand { line: String },
    #[error("G-code command handler for '{command}' panicked")]
    HandlerPanicked { command: String },
}

#[derive(Debug, Clone)]
pub struct GCodeCommand {
    parsed: ParsedLine,
}

impl GCodeCommand {
    pub fn new(parsed: ParsedLine) -> Self { Self { parsed } }

    /// Build the command object exactly as dispatch expects: extended commands
    /// replace the legacy letter-token map with shell-style KEY=value fields.
    pub fn new_for_dispatch(mut parsed: ParsedLine) -> Result<Self, GCodeError> {
        if !is_traditional_command(&parsed.command) {
            parsed.parameters = parsed.extended_parameters()?;
        }
        Ok(Self { parsed })
    }

    pub fn command(&self) -> &str { &self.parsed.command }
    pub fn commandline(&self) -> &str { &self.parsed.original }
    pub fn parameters(&self) -> &HashMap<String, String> { &self.parsed.parameters }
    pub fn raw_parameters(&self) -> &str { self.parsed.raw_parameters() }
    pub fn extended_parameters(&self) -> Result<HashMap<String, String>, GCodeError> {
        self.parsed.extended_parameters()
    }

    pub fn get(&self, name: &str) -> Option<&str> { self.parsed.get(name) }

    pub fn require(&self, name: &str) -> Result<&str, GCodeError> {
        self.get(name).ok_or_else(|| GCodeError::MissingParameter {
            line: self.commandline().to_owned(),
            parameter: name.to_owned(),
        })
    }

    pub fn get_i64(&self, name: &str, default: Option<i64>) -> Result<i64, GCodeError> {
        let Some(value) = self.get(name) else {
            return default.ok_or_else(|| GCodeError::MissingParameter {
                line: self.commandline().to_owned(),
                parameter: name.to_owned(),
            });
        };
        value.parse().map_err(|_| GCodeError::InvalidParameter {
            line: self.commandline().to_owned(),
            parameter: name.to_owned(),
            value: value.to_owned(),
        })
    }

    pub fn get_f64(&self, name: &str, default: Option<f64>) -> Result<f64, GCodeError> {
        let Some(value) = self.get(name) else {
            return default.ok_or_else(|| GCodeError::MissingParameter {
                line: self.commandline().to_owned(),
                parameter: name.to_owned(),
            });
        };
        let parsed: f64 = value.parse().map_err(|_| GCodeError::InvalidParameter {
            line: self.commandline().to_owned(),
            parameter: name.to_owned(),
            value: value.to_owned(),
        })?;
        if !parsed.is_finite() {
            return Err(GCodeError::InvalidParameter {
                line: self.commandline().to_owned(),
                parameter: name.to_owned(),
                value: value.to_owned(),
            });
        }
        Ok(parsed)
    }

    pub fn check_range(
        &self,
        name: &str,
        value: f64,
        min: Option<f64>,
        max: Option<f64>,
        above: Option<f64>,
        below: Option<f64>,
    ) -> Result<(), GCodeError> {
        let constraint = if let Some(min) = min.filter(|bound| value < *bound) {
            Some(format!("at least {min}"))
        } else if let Some(max) = max.filter(|bound| value > *bound) {
            Some(format!("at most {max}"))
        } else if let Some(bound) = above.filter(|bound| value <= *bound) {
            Some(format!("above {bound}"))
        } else if let Some(bound) = below.filter(|bound| value >= *bound) {
            Some(format!("below {bound}"))
        } else {
            None
        };
        if let Some(constraint) = constraint {
            return Err(GCodeError::OutOfRange {
                line: self.commandline().to_owned(),
                parameter: name.to_owned(),
                constraint,
            });
        }
        Ok(())
    }
}


/// Klipper treats a command as traditional when its first character is a
/// letter and its second character is a digit (for example G1 or M105).
pub fn is_traditional_command(command: &str) -> bool {
    let command = command.trim();
    let mut chars = command.chars();
    let (Some(first), Some(second)) = (chars.next(), chars.next()) else {
        return false;
    };
    first.is_ascii_alphabetic()
        && second.is_ascii_digit()
        && command.get(1..).is_some_and(|suffix| suffix.parse::<f64>().is_ok())
}

/// Parse one input line. Empty lines and comment-only lines return `None`.
pub fn parse_line(input: &str) -> Option<ParsedLine> {
    let original = input.trim();
    let command_text = original.split_once(';').map_or(original, |(head, _)| head).trim();
    if command_text.is_empty() {
        return None;
    }

    let upper = command_text.to_ascii_uppercase();
    let mut parts = Vec::new();
    let mut cursor = 0;
    for matched in argument_regex().find_iter(&upper) {
        parts.push(upper[cursor..matched.start()].to_owned());
        parts.push(matched.as_str().to_owned());
        cursor = matched.end();
    }
    parts.push(upper[cursor..].to_owned());

    let (command, line_number) = if parts.len() >= 5 && parts[0].is_empty() && parts[1] == "N" {
        let number = parts[2].trim().parse::<u32>().ok();
        (parts[3..5].join("").trim().to_owned(), number)
    } else {
        (parts.iter().take(3).map(String::as_str).collect::<Vec<_>>().join("").trim().to_owned(), None)
    };

    let mut parameters = HashMap::new();
    for index in (1..parts.len()).step_by(2) {
        if let Some(key) = parts.get(index) {
            if let Some(value) = parts.get(index + 1) {
                parameters.insert(key.clone(), value.trim().to_owned());
            }
        }
    }

    Some(ParsedLine {
        command,
        original: original.to_owned(),
        parameters,
        line_number,
    })
}

fn shell_tokens(input: &str) -> Result<Vec<String>, ()> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut escaped_in_double_quote = false;
    let mut active = false;

    for ch in input.chars() {
        if escaped {
            if escaped_in_double_quote && ch != '"' && ch != '\\' {
                token.push('\\');
            }
            token.push(ch);
            escaped = false;
            escaped_in_double_quote = false;
            active = true;
            continue;
        }
        // POSIX shlex removes escapes outside quotes, but inside double
        // quotes only an escaped quote or backslash loses its backslash.
        if ch == '\\' && quote != Some('\'') {
            escaped = true;
            escaped_in_double_quote = quote == Some('"');
            continue;
        }
        if let Some(delimiter) = quote {
            if ch == delimiter {
                quote = None;
            } else {
                token.push(ch);
            }
            active = true;
            continue;
        }
        match ch {
            '\'' | '"' => {
                quote = Some(ch);
                active = true;
            }
            ';' => break,
            c if c.is_whitespace() => {
                if active {
                    tokens.push(std::mem::take(&mut token));
                    active = false;
                }
            }
            _ => {
                token.push(ch);
                active = true;
            }
        }
    }
    if quote.is_some() || escaped {
        return Err(());
    }
    if active {
        tokens.push(token);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traditional_command_detection_matches_klipper_name_rules() {
        assert!(is_traditional_command("G1"));
        assert!(is_traditional_command("M105"));
        assert!(is_traditional_command("G1.1"));
        assert!(!is_traditional_command("G"));
        assert!(!is_traditional_command("G1X"));
        assert!(!is_traditional_command("SET_HEATER_TEMPERATURE"));
        assert!(!is_traditional_command("1G"));
    }

    #[test]
    fn parses_traditional_commands_and_parameters() {
        let parsed = parse_line("G1 X10.5 Y-2.0 F1200 ; move").unwrap();
        assert_eq!(parsed.command, "G1");
        assert_eq!(parsed.get("x"), Some("10.5"));
        assert_eq!(parsed.get("Y"), Some("-2.0"));
        assert_eq!(parsed.get("F"), Some("1200"));
    }

    #[test]
    fn parses_numbered_lines() {
        let parsed = parse_line("N123 G1 X10*45").unwrap();
        assert_eq!(parsed.line_number, Some(123));
        assert_eq!(parsed.command, "G1");
        assert_eq!(parsed.get("X"), Some("10"));
    }

    #[test]
    fn parses_extended_command_parameters_with_quotes() {
        let parsed = parse_line("SET_HEATER_TEMPERATURE HEATER=hotend TARGET=205").unwrap();
        assert_eq!(parsed.command, "SET_HEATER_TEMPERATURE");
        let params = parsed.extended_parameters().unwrap();
        assert_eq!(params.get("HEATER").map(String::as_str), Some("hotend"));
        assert_eq!(params.get("TARGET").map(String::as_str), Some("205"));
        let quoted = parse_line("RESPOND MSG=\"hello world\"").unwrap();
        assert_eq!(quoted.extended_parameters().unwrap().get("MSG").map(String::as_str), Some("hello world"));
    }

    #[test]
    fn extended_parameters_support_unquoted_escapes_and_hash_values() {
        let escaped = parse_line(r#"RESPOND MSG=hello\ world"#).unwrap();
        assert_eq!(
            escaped.extended_parameters().unwrap().get("MSG").map(String::as_str),
            Some("hello world")
        );

        let hash = parse_line("RESPOND MSG=#status").unwrap();
        assert_eq!(
            hash.extended_parameters().unwrap().get("MSG").map(String::as_str),
            Some("#status")
        );

        let double_quoted = parse_line(r#"RESPOND MSG="hello\ world""#).unwrap();
        assert_eq!(
            double_quoted.extended_parameters().unwrap().get("MSG").map(String::as_str),
            Some(r#"hello\ world"#)
        );
        let single_quoted = parse_line(r#"RESPOND MSG='hello\ world'"#).unwrap();
        assert_eq!(
            single_quoted.extended_parameters().unwrap().get("MSG").map(String::as_str),
            Some(r#"hello\ world"#)
        );
    }

    #[test]
    fn rejects_unterminated_extended_parameter_quotes() {
        let parsed = parse_line("RESPOND MSG=\"unterminated").unwrap();
        assert!(matches!(
            parsed.extended_parameters(),
            Err(GCodeError::MalformedCommand { .. })
        ));
    }

    #[test]
    fn ignores_blank_and_comment_lines() {
        assert!(parse_line("  ").is_none());
        assert!(parse_line("; comment").is_none());
    }

    #[test]
    fn validates_typed_parameters_and_bounds() {
        let command = GCodeCommand::new(parse_line("M220 S125").unwrap());
        assert_eq!(command.get_i64("S", None).unwrap(), 125);
        let value = command.get_f64("S", None).unwrap();
        assert!(command.check_range("S", value, Some(0.0), Some(100.0), None, None).is_err());
        assert!(command.get_i64("missing", None).is_err());
    }
}
