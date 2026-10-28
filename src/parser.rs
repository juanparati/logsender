//! Parse header lines into [`LogEntry`] values using a [`LogFormat`].
//!
//! The JSON-context extraction (`json_context: true`) is a port of laralog's
//! `Model_LogParser`: same bracket/quote state machine, same `--smart` semantics.

use chrono::{DateTime, Local, LocalResult, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::Value;
use std::sync::Arc;

use crate::entry::LogEntry;
use crate::format::LogFormat;

/// Timezone the log file was written in.
#[derive(Debug, Clone, Copy)]
pub enum SourceTz {
    /// An IANA zone such as `Europe/Madrid`.
    Named(Tz),
    /// The operating system's local zone.
    Local,
}

impl SourceTz {
    /// Resolve the source timezone: explicit name, else `TZ` env var, else local.
    pub fn resolve(name: Option<&str>) -> Result<Self, String> {
        match name.map(str::trim).filter(|s| !s.is_empty()) {
            Some(n) => n.parse::<Tz>().map(SourceTz::Named).map_err(|_| {
                format!("unknown timezone '{n}' (expected an IANA name such as Europe/Madrid)")
            }),
            None => match std::env::var("TZ") {
                Ok(tz) if !tz.trim().is_empty() => Ok(tz
                    .trim()
                    .parse::<Tz>()
                    .map(SourceTz::Named)
                    .unwrap_or(SourceTz::Local)),
                _ => Ok(SourceTz::Local),
            },
        }
    }

    /// Interpret a naive wall-clock time in this zone and return it in UTC.
    /// Ambiguous times (DST fall-back) take the earliest instant; nonexistent
    /// times (DST spring-forward) take the instant after the gap.
    pub fn to_utc(&self, naive: NaiveDateTime) -> DateTime<Utc> {
        fn pick<T: TimeZone>(
            r: LocalResult<DateTime<T>>,
            naive: NaiveDateTime,
            tz: &T,
        ) -> DateTime<Utc> {
            match r {
                LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => dt.with_timezone(&Utc),
                LocalResult::None => {
                    // Try one hour later; if still invalid, fall back to UTC.
                    let shifted = naive + chrono::Duration::hours(1);
                    match tz.from_local_datetime(&shifted) {
                        LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => {
                            dt.with_timezone(&Utc)
                        }
                        LocalResult::None => Utc.from_utc_datetime(&naive),
                    }
                }
            }
        }
        match self {
            SourceTz::Named(tz) => pick(tz.from_local_datetime(&naive), naive, tz),
            SourceTz::Local => pick(Local.from_local_datetime(&naive), naive, &Local),
        }
    }

    pub fn name(&self) -> String {
        match self {
            SourceTz::Named(tz) => tz.name().to_string(),
            SourceTz::Local => "local".to_string(),
        }
    }
}

/// A JSON chunk located inside a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonSpan {
    pub start: usize,
    pub end: usize, // exclusive
}

impl JsonSpan {
    pub fn text<'a>(&self, s: &'a str) -> &'a str {
        &s[self.start..self.end]
    }
}

/// Locate `{...}` chunks that contain at least one double quote, matching
/// braces so that nested objects and `{{ moustache }}` inside strings survive.
///
/// This is a port of laralog's `jsonExtract`, with one fix: a brace group
/// without quotes (e.g. `{foo}`) no longer poisons the start offset of the
/// next real JSON chunk.
pub fn json_extract(s: &str) -> Vec<JsonSpan> {
    let mut depth: i32 = -1;
    let mut init: Option<usize> = None;
    let mut found: Option<usize> = None;
    let mut spans = Vec::new();

    for (i, b) in s.bytes().enumerate() {
        match b {
            b'{' => {
                depth += 1;
                if init.is_none() {
                    init = Some(i);
                }
            }
            b'}' if init.is_some() => {
                depth -= 1;
                if depth == -1 {
                    if let Some(start) = found {
                        spans.push(JsonSpan { start, end: i + 1 });
                    }
                    init = None;
                    found = None;
                }
            }
            b'"' if depth > -1 && found.is_none() => {
                found = init;
            }
            _ => {}
        }
    }
    spans
}

/// Turns header lines into [`LogEntry`] values according to a [`LogFormat`].
#[derive(Debug, Clone)]
pub struct Parser {
    format: Arc<LogFormat>,
    pub from_tz: SourceTz,
    pub smart: bool,
    pub hostname: String,
    /// `--environment`: used when the format captures no environment.
    pub environment: Option<String>,
}

impl Parser {
    pub fn new(
        format: Arc<LogFormat>,
        from_tz: SourceTz,
        smart: bool,
        hostname: impl Into<String>,
    ) -> Self {
        Parser {
            format,
            from_tz,
            smart,
            hostname: hostname.into(),
            environment: None,
        }
    }

    pub fn with_environment(mut self, environment: Option<String>) -> Self {
        self.environment = environment.filter(|e| !e.trim().is_empty());
        self
    }

    pub fn format(&self) -> &LogFormat {
        &self.format
    }

    /// True when the line starts a new entry.
    pub fn is_header(&self, line: &str) -> bool {
        self.format.is_header(line)
    }

    /// Whether a non-header line may be appended to the pending entry.
    pub fn accepts_continuation(&self, line: &str) -> bool {
        self.format.accepts_continuation(line)
    }

    /// Parse a header line. Returns `None` for empty or non-header lines, and
    /// for header lines whose timestamp does not parse.
    pub fn parse(&self, line: &str) -> Option<LogEntry> {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            return None;
        }
        let ex = self.format.extract(line)?;

        let timestamp = match ex.timestamp.as_deref() {
            Some(raw) => match self.format.timestamp.parse(
                raw,
                ex.zone.as_deref(),
                &self.from_tz,
                Utc::now(),
            ) {
                Some(ts) => ts,
                None => {
                    self.format.warn_bad_timestamp(raw);
                    return None;
                }
            },
            None => Utc::now(),
        };

        let environment = ex
            .environment
            .clone()
            .or_else(|| self.environment.clone())
            .unwrap_or_else(|| self.format.environment_default.clone());

        let (message, data, params) = if self.format.json_context {
            let raw_message = ex.message.as_str();
            let spans = json_extract(raw_message);
            let data: Vec<String> = spans
                .iter()
                .map(|sp| sp.text(raw_message).to_string())
                .collect();
            let mut message = String::with_capacity(raw_message.len());
            let mut cursor = 0;
            for sp in &spans {
                message.push_str(&raw_message[cursor..sp.start]);
                cursor = sp.end;
            }
            message.push_str(&raw_message[cursor..]);
            let params = if self.smart && !data.is_empty() {
                smart_params(&data)
            } else {
                None
            };
            (message.trim().to_string(), data, params)
        } else {
            (ex.message.trim().to_string(), Vec::new(), None)
        };

        Some(LogEntry {
            timestamp,
            environment,
            level: self.format.normalize_level(ex.level.as_deref()),
            message,
            data,
            params,
            hostname: self.hostname.clone(),
            stacktrace: None,
            fields: ex.fields,
        })
    }

    /// The built-in Laravel format, for tests and as the default.
    pub fn laravel(from_tz: SourceTz, smart: bool, hostname: impl Into<String>) -> Self {
        let f = LogFormat::from_yaml(
            crate::format::builtin("laravel").expect("laravel built-in"),
            "laravel",
            "built-in",
        )
        .expect("built-in laravel format compiles");
        Parser::new(Arc::new(f), from_tz, smart, hostname)
    }
}

/// `--smart`: one chunk becomes the object itself, several become an array of
/// the chunks that parsed. Returns `None` when nothing parsed.
fn smart_params(data: &[String]) -> Option<Value> {
    let decoded: Vec<Value> = data
        .iter()
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .collect();
    match (data.len(), decoded.len()) {
        (_, 0) => None,
        (1, 1) => decoded.into_iter().next(),
        _ => Some(Value::Array(decoded)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::{Level, LogLevel};

    fn parser(smart: bool) -> Parser {
        Parser::laravel(SourceTz::Named(chrono_tz::UTC), smart, "host")
    }

    #[test]
    fn laralog_fixture_first_three_lines() {
        // Mirrors test/Tests/Models/LogParserTest.php from laralog.
        let p = parser(false);
        let e = p
            .parse("[2016-04-16 17:25:45] local.INFO: Hello logstash")
            .unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2016-04-16T17:25:45+00:00");
        assert_eq!(e.environment, "local");
        assert_eq!(e.level, LogLevel::Known(Level::Info));
        assert_eq!(e.message, "Hello logstash");
        assert!(e.data.is_empty());

        let e = p
            .parse(r#"[2016-04-16 17:26:00] local.INFO: Example data {"foo": "Test", "bar": true}"#)
            .unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2016-04-16T17:26:00+00:00");
        assert_eq!(e.message, "Example data");
        assert_eq!(e.data, vec![r#"{"foo": "Test", "bar": true}"#.to_string()]);

        let e = p
            .parse(r#"[2016-04-16 17:26:15] local.INFO: Example data2åñ {"foo": "Test", "bar": "{{ Moustache }}"}"#)
            .unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2016-04-16T17:26:15+00:00");
        assert_eq!(e.message, "Example data2åñ");
        assert_eq!(
            e.data,
            vec![r#"{"foo": "Test", "bar": "{{ Moustache }}"}"#.to_string()]
        );
    }

    #[test]
    fn non_header_lines_are_rejected() {
        let p = parser(false);
        assert!(p.parse("").is_none());
        assert!(p.parse("Stack trace:").is_none());
        assert!(p.parse("#0 [internal function]: foo()").is_none());
        assert!(
            p.parse("[2016-13-99 17:25:45] local.INFO: bad date")
                .is_none()
        );
    }

    #[test]
    fn multiple_json_chunks() {
        let p = parser(true);
        let e = p
            .parse(r#"[2019-11-24 06:30:56] production.INFO: Job finished {"a":1} then {"b":{"c":2}} end"#)
            .unwrap();
        assert_eq!(e.message, "Job finished  then  end");
        assert_eq!(e.data.len(), 2);
        assert_eq!(
            e.params.unwrap(),
            serde_json::json!([{"a":1},{"b":{"c":2}}])
        );
    }

    #[test]
    fn smart_single_chunk_is_object() {
        let p = parser(true);
        let e = p
            .parse(r#"[2019-11-24 06:30:56] production.INFO: Job finished {"connection":"database","queue":"default"}"#)
            .unwrap();
        assert_eq!(
            e.params.unwrap(),
            serde_json::json!({"connection":"database","queue":"default"})
        );
    }

    #[test]
    fn smart_invalid_json_gives_no_params_but_keeps_data() {
        let p = parser(true);
        let e = p
            .parse(r#"[2019-11-24 06:30:56] production.INFO: x {"broken": }"#)
            .unwrap();
        assert!(e.params.is_none());
        assert_eq!(e.data, vec![r#"{"broken": }"#.to_string()]);
    }

    #[test]
    fn braces_without_quotes_are_not_json() {
        assert!(json_extract("Route {id} not found").is_empty());
        // The quote-less group must not shift the start of a later real chunk.
        let s = r#"Route {id} not found {"id": 5}"#;
        let spans = json_extract(s);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].text(s), r#"{"id": 5}"#);
    }

    #[test]
    fn unterminated_brace_yields_nothing() {
        assert!(json_extract(r#"oops {"a": 1"#).is_empty());
    }

    #[test]
    fn utf8_message_with_json_after_multibyte_chars() {
        let s = r#"ñandú {"k": "v"}"#;
        let spans = json_extract(s);
        assert_eq!(spans[0].text(s), r#"{"k": "v"}"#);
    }

    #[test]
    fn from_timezone_converts_to_utc() {
        let p = Parser::laravel(SourceTz::Named(chrono_tz::Europe::Madrid), false, "h");
        let e = p.parse("[2024-07-01 12:00:00] prod.ERROR: boom").unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2024-07-01T10:00:00+00:00");
        // Winter: +01:00
        let e = p.parse("[2024-01-01 12:00:00] prod.ERROR: boom").unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2024-01-01T11:00:00+00:00");
    }

    #[test]
    fn dst_gap_and_overlap_do_not_panic() {
        let p = Parser::laravel(SourceTz::Named(chrono_tz::Europe::Madrid), false, "h");
        // 2024-03-31 02:30 does not exist in Madrid (clocks jump 02:00 -> 03:00).
        let e = p.parse("[2024-03-31 02:30:00] prod.INFO: gap").unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2024-03-31T01:30:00+00:00");
        // 2024-10-27 02:30 happens twice; earliest (CEST, +02:00) wins.
        let e = p.parse("[2024-10-27 02:30:00] prod.INFO: overlap").unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2024-10-27T00:30:00+00:00");
    }

    #[test]
    fn resolve_rejects_garbage() {
        assert!(SourceTz::resolve(Some("Mars/Olympus")).is_err());
        assert!(matches!(
            SourceTz::resolve(Some("UTC")),
            Ok(SourceTz::Named(_))
        ));
    }
}
