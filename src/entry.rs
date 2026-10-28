use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::level::LogLevel;

/// One parsed Laravel log entry, possibly spanning several physical lines.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LogEntry {
    /// Original log time converted to UTC.
    pub timestamp: DateTime<Utc>,
    /// Laravel environment, e.g. `production`.
    pub environment: String,
    pub level: LogLevel,
    /// Message with any JSON context removed and trimmed.
    pub message: String,
    /// Raw JSON chunks found in the line, in order of appearance.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub data: Vec<String>,
    /// Parsed JSON context when `--smart` is enabled: a single object, or an
    /// array of objects when the line carried several chunks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    pub hostname: String,
    /// Continuation lines (stack trace) following the header line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stacktrace: Option<String>,
    /// Extra named captures from the log format (e.g. `client.address`).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, String>,
}

impl LogEntry {
    /// Approximate size in bytes, used for the multi-line cap.
    pub fn byte_len(&self) -> usize {
        self.message.len()
            + self.data.iter().map(String::len).sum::<usize>()
            + self.stacktrace.as_ref().map_or(0, String::len)
    }
}

/// How a back-end wants the timestamp rendered in a JSON document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampRepr {
    /// Milliseconds since the Unix epoch (Datadog, stdout).
    EpochMillis,
    /// `Y-m-d\TH:i:sP` in the given zone (Elasticsearch).
    Iso8601,
}

impl LogEntry {
    /// `data` as laralog serialised it: one chunk → string, several → array.
    pub fn data_value(&self) -> Value {
        match self.data.len() {
            0 => Value::Null,
            1 => Value::String(self.data[0].clone()),
            _ => Value::Array(self.data.iter().cloned().map(Value::String).collect()),
        }
    }

    /// The flat document shape used by the Datadog, Elasticsearch and stdout
    /// senders. `to_tz` only applies to the ISO representation.
    pub fn document(
        &self,
        ts: TimestampRepr,
        to_tz: Option<chrono_tz::Tz>,
    ) -> serde_json::Map<String, Value> {
        let mut doc = serde_json::Map::new();
        let timestamp = match ts {
            TimestampRepr::EpochMillis => Value::from(self.timestamp.timestamp_millis()),
            TimestampRepr::Iso8601 => Value::String(self.formatted_timestamp(to_tz)),
        };
        doc.insert("timestamp".into(), timestamp);
        doc.insert(
            "environment".into(),
            Value::String(self.environment.clone()),
        );
        doc.insert(
            "level".into(),
            Value::String(self.level.as_str().to_string()),
        );
        doc.insert("message".into(), Value::String(self.message.clone()));
        doc.insert("data".into(), self.data_value());
        if let Some(p) = &self.params {
            doc.insert("params".into(), p.clone());
        }
        doc.insert("hostname".into(), Value::String(self.hostname.clone()));
        if let Some(st) = &self.stacktrace {
            doc.insert("stacktrace".into(), Value::String(st.clone()));
        }
        if !self.fields.is_empty() {
            let fields: serde_json::Map<String, Value> = self
                .fields
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();
            doc.insert("fields".into(), Value::Object(fields));
        }
        doc
    }

    /// `2016-04-16T17:25:45+00:00` style, in `to_tz` or UTC.
    pub fn formatted_timestamp(&self, to_tz: Option<chrono_tz::Tz>) -> String {
        match to_tz {
            Some(tz) => self
                .timestamp
                .with_timezone(&tz)
                .format("%Y-%m-%dT%H:%M:%S%:z")
                .to_string(),
            None => self.timestamp.format("%Y-%m-%dT%H:%M:%S%:z").to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::{Level, LogLevel};
    use chrono::TimeZone;

    fn entry() -> LogEntry {
        LogEntry {
            timestamp: Utc.with_ymd_and_hms(2016, 4, 16, 17, 25, 45).unwrap(),
            environment: "local".into(),
            level: LogLevel::Known(Level::Info),
            message: "hi".into(),
            data: vec![],
            params: None,
            hostname: "h".into(),
            stacktrace: None,
            fields: Default::default(),
        }
    }

    #[test]
    fn document_epoch_and_iso() {
        let e = entry();
        let d = e.document(TimestampRepr::EpochMillis, None);
        assert_eq!(d["timestamp"], Value::from(1460827545000i64));
        assert_eq!(d["data"], Value::Null);
        assert!(!d.contains_key("params"));
        let d = e.document(TimestampRepr::Iso8601, Some(chrono_tz::Europe::Madrid));
        assert_eq!(
            d["timestamp"],
            Value::String("2016-04-16T19:25:45+02:00".into())
        );
    }

    #[test]
    fn data_value_shapes() {
        let mut e = entry();
        e.data = vec!["{\"a\":1}".into()];
        assert_eq!(e.data_value(), Value::String("{\"a\":1}".into()));
        e.data.push("{\"b\":2}".into());
        assert!(e.data_value().is_array());
    }
}
