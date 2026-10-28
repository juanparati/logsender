//! Serde model of a log format definition file (YAML).

use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormatFile {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// Extra grok pattern definitions usable from `match`.
    #[serde(default)]
    pub patterns: BTreeMap<String, String>,
    /// Grok expressions for a header line, tried in order.
    #[serde(rename = "match")]
    pub match_: Vec<String>,
    #[serde(default)]
    pub timestamp: TimestampSpec,
    #[serde(default)]
    pub level: LevelSpec,
    #[serde(default)]
    pub message: MessageSpec,
    #[serde(default)]
    pub environment: EnvironmentSpec,
    #[serde(default)]
    pub multiline: MultilineSpec,
    /// Extract `{...}` JSON chunks from the message (laralog behaviour).
    #[serde(default)]
    pub json_context: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampSpec {
    /// Capture holding the time. `null` disables and uses the read time.
    #[serde(default = "default_timestamp_field")]
    pub field: Option<String>,
    /// chrono strftime format, or `rfc3339`, `epoch`, `epoch_ms`.
    #[serde(default = "default_timestamp_format")]
    pub format: String,
    /// The format carries no year (syslog): assume the current one.
    #[serde(default)]
    pub assume_current_year: bool,
    /// Capture holding an IANA zone name or `±HH:MM` offset.
    #[serde(default)]
    pub zone_field: Option<String>,
}

impl Default for TimestampSpec {
    fn default() -> Self {
        TimestampSpec {
            field: default_timestamp_field(),
            format: default_timestamp_format(),
            assume_current_year: false,
            zone_field: None,
        }
    }
}

fn default_timestamp_field() -> Option<String> {
    Some("timestamp".into())
}

fn default_timestamp_format() -> String {
    "rfc3339".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelSpec {
    #[serde(default = "default_level_field")]
    pub field: Option<String>,
    #[serde(default = "default_level")]
    pub default: String,
    /// Case-insensitive aliases → canonical level names.
    #[serde(default)]
    pub map: BTreeMap<String, String>,
}

impl Default for LevelSpec {
    fn default() -> Self {
        LevelSpec {
            field: default_level_field(),
            default: default_level(),
            map: BTreeMap::new(),
        }
    }
}

fn default_level_field() -> Option<String> {
    Some("level".into())
}

fn default_level() -> String {
    "INFO".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageSpec {
    #[serde(default = "default_message_field")]
    pub field: String,
}

impl Default for MessageSpec {
    fn default() -> Self {
        MessageSpec {
            field: default_message_field(),
        }
    }
}

fn default_message_field() -> String {
    "message".into()
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentSpec {
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub default: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MultilineMode {
    /// Any non-header line appends to the pending entry.
    #[default]
    Continuation,
    /// Non-header lines are dropped.
    None,
    /// Only lines matching `pattern` append; others are dropped.
    Pattern,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultilineSpec {
    #[serde(default)]
    pub mode: MultilineMode,
    #[serde(default)]
    pub pattern: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_file_uses_defaults() {
        let f: FormatFile =
            serde_yaml_ng::from_str("match:\n  - '%{GREEDYDATA:message}'\n").unwrap();
        assert_eq!(f.match_.len(), 1);
        assert_eq!(f.timestamp.field.as_deref(), Some("timestamp"));
        assert_eq!(f.timestamp.format, "rfc3339");
        assert_eq!(f.level.default, "INFO");
        assert_eq!(f.message.field, "message");
        assert_eq!(f.multiline.mode, MultilineMode::Continuation);
        assert!(!f.json_context);
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let err = serde_yaml_ng::from_str::<FormatFile>("match: ['x']\nmatches: []\n")
            .err()
            .unwrap();
        assert!(err.to_string().contains("matches"));
    }

    #[test]
    fn null_timestamp_field_disables_it() {
        let f: FormatFile =
            serde_yaml_ng::from_str("match: ['x']\ntimestamp:\n  field: null\n").unwrap();
        assert!(f.timestamp.field.is_none());
    }
}
