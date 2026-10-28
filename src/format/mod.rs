//! Log format definitions: YAML files with grok expressions, compiled once
//! with the PCRE2 engine.

pub mod schema;
pub mod timestamp;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use grok::{Grok, Pattern};

use crate::error::{Error, Result};
use crate::level::LogLevel;
pub use schema::MultilineMode;
use schema::{FormatFile, MultilineSpec};
use timestamp::TimestampParser;

/// Formats shipped with the binary (`log_formats/*.yaml`).
pub const BUILTINS: &[(&str, &str)] = &[
    ("laravel", include_str!("../../log_formats/laravel.yaml")),
    (
        "nginx_error",
        include_str!("../../log_formats/nginx_error.yaml"),
    ),
    (
        "nginx_access",
        include_str!("../../log_formats/nginx_access.yaml"),
    ),
    (
        "apache_error",
        include_str!("../../log_formats/apache_error.yaml"),
    ),
    (
        "apache_access",
        include_str!("../../log_formats/apache_access.yaml"),
    ),
    ("syslog", include_str!("../../log_formats/syslog.yaml")),
    ("php", include_str!("../../log_formats/php.yaml")),
];

pub fn builtin(name: &str) -> Option<&'static str> {
    BUILTINS.iter().find(|(n, _)| *n == name).map(|(_, y)| *y)
}

pub fn builtin_names() -> Vec<&'static str> {
    BUILTINS.iter().map(|(n, _)| *n).collect()
}

/// Level aliases understood by every format, applied after the file's own `map`.
const LEVEL_ALIASES: &[(&str, &str)] = &[
    ("warn", "WARNING"),
    ("err", "ERROR"),
    ("crit", "CRITICAL"),
    ("emerg", "EMERGENCY"),
    ("fatal", "CRITICAL"),
    ("panic", "EMERGENCY"),
    ("severe", "CRITICAL"),
    ("trace", "DEBUG"),
];

/// What a header line yielded, before timestamp/level normalisation.
#[derive(Debug, Default)]
pub struct Extracted {
    pub timestamp: Option<String>,
    pub zone: Option<String>,
    pub level: Option<String>,
    pub message: String,
    pub environment: Option<String>,
    /// Every other named capture.
    pub fields: BTreeMap<String, String>,
}

pub struct LogFormat {
    pub name: String,
    /// Where it came from: a file path or `built-in`.
    pub source: String,
    patterns: Vec<Pattern>,
    continuation: Option<Pattern>,
    pub multiline: MultilineMode,
    timestamp_field: Option<String>,
    pub timestamp: TimestampParser,
    zone_field: Option<String>,
    level_field: Option<String>,
    level_default: String,
    level_map: HashMap<String, String>,
    message_field: String,
    environment_field: Option<String>,
    pub environment_default: String,
    pub json_context: bool,
    warned_timestamp: AtomicBool,
}

impl std::fmt::Debug for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFormat")
            .field("name", &self.name)
            .field("source", &self.source)
            .field("patterns", &self.patterns.len())
            .field("multiline", &self.multiline)
            .finish()
    }
}

impl LogFormat {
    /// Resolve the `<LOG_FORMAT>` argument: a path to a YAML file, else a built-in name.
    pub fn resolve(spec: &str) -> Result<Self> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Err(Error::Config("LOG_FORMAT must not be empty".into()));
        }
        let path = Path::new(spec);
        if path.is_file() {
            return Self::load_file(path);
        }
        if let Some(yaml) = builtin(spec) {
            return Self::from_yaml(yaml, spec, "built-in");
        }
        Err(Error::Config(format!(
            "log format '{spec}' not found: not a file, and not a built-in name (built-ins: {})",
            builtin_names().join(", ")
        )))
    }

    pub fn load_file(path: &Path) -> Result<Self> {
        let yaml = std::fs::read_to_string(path).map_err(|e| {
            Error::Config(format!("cannot read log format {}: {e}", path.display()))
        })?;
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "custom".into());
        Self::from_yaml(&yaml, &name, &path.display().to_string())
    }

    pub fn from_yaml(yaml: &str, name: &str, source: &str) -> Result<Self> {
        let file: FormatFile = serde_yaml_ng::from_str(yaml)
            .map_err(|e| Error::Config(format!("invalid log format {source}: {e}")))?;
        Self::compile(file, name, source)
    }

    fn compile(file: FormatFile, name: &str, source: &str) -> Result<Self> {
        let cfg = |msg: String| Error::Config(format!("log format {source}: {msg}"));

        if file.match_.is_empty() {
            return Err(cfg("`match` must list at least one grok expression".into()));
        }
        let mut grok = Grok::with_default_patterns();
        for (pname, pdef) in &file.patterns {
            grok.add_pattern(pname.clone(), pdef.clone());
        }

        let mut patterns = Vec::with_capacity(file.match_.len());
        let mut captures_timestamp = file.timestamp.field.is_none();
        for (i, expr) in file.match_.iter().enumerate() {
            let p = grok
                .compile(expr, true)
                .map_err(|e| cfg(format!("match[{i}] does not compile: {e:?}")))?;
            let names: Vec<&str> = p.capture_names().collect();
            if !names.contains(&file.message.field.as_str()) {
                return Err(cfg(format!(
                    "match[{i}] has no `%{{...:{}}}` capture (message.field)",
                    file.message.field
                )));
            }
            if let Some(ts) = &file.timestamp.field
                && names.contains(&ts.as_str())
            {
                captures_timestamp = true;
            }
            patterns.push(p);
        }
        if !captures_timestamp {
            return Err(cfg(format!(
                "no `match` expression captures timestamp.field `{}` (set `timestamp: {{field: null}}` to use the read time)",
                file.timestamp.field.as_deref().unwrap_or_default()
            )));
        }

        let MultilineSpec { mode, pattern } = file.multiline;
        let continuation = match (mode, pattern) {
            (MultilineMode::Pattern, Some(expr)) => Some(
                grok.compile(&expr, true)
                    .map_err(|e| cfg(format!("multiline.pattern does not compile: {e:?}")))?,
            ),
            (MultilineMode::Pattern, None) => {
                return Err(cfg(
                    "multiline.mode is `pattern` but multiline.pattern is missing".into(),
                ));
            }
            _ => None,
        };

        let level_map = file
            .level
            .map
            .into_iter()
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();

        Ok(LogFormat {
            name: file.name.unwrap_or_else(|| name.to_string()),
            source: source.to_string(),
            patterns,
            continuation,
            multiline: mode,
            timestamp_field: file.timestamp.field,
            timestamp: TimestampParser::new(
                &file.timestamp.format,
                file.timestamp.assume_current_year,
            ),
            zone_field: file.timestamp.zone_field,
            level_field: file.level.field,
            level_default: file.level.default,
            level_map,
            message_field: file.message.field,
            environment_field: file.environment.field,
            environment_default: file.environment.default,
            json_context: file.json_context,
            warned_timestamp: AtomicBool::new(false),
        })
    }

    pub fn is_header(&self, line: &str) -> bool {
        self.patterns
            .iter()
            .any(|p| p.match_against(line).is_some())
    }

    /// Whether a non-header line may be appended to the pending entry.
    pub fn accepts_continuation(&self, line: &str) -> bool {
        match self.multiline {
            MultilineMode::Continuation => true,
            MultilineMode::None => false,
            MultilineMode::Pattern => self
                .continuation
                .as_ref()
                .is_some_and(|p| p.match_against(line).is_some()),
        }
    }

    /// Run the `match` expressions and split captures into mapped fields and extras.
    pub fn extract(&self, line: &str) -> Option<Extracted> {
        let matches = self.patterns.iter().find_map(|p| p.match_against(line))?;
        let mut ex = Extracted::default();
        for (key, value) in matches.iter() {
            let value = value.to_string();
            if self.timestamp_field.as_deref() == Some(key) {
                ex.timestamp = Some(value);
            } else if self.zone_field.as_deref() == Some(key) {
                ex.zone = Some(value);
            } else if self.level_field.as_deref() == Some(key) {
                ex.level = Some(value);
            } else if self.message_field == key {
                ex.message = value;
            } else if self.environment_field.as_deref() == Some(key) {
                ex.environment = Some(value);
            } else if !value.is_empty() && value != "-" {
                // A lone dash is the conventional "no value" in access logs.
                ex.fields.insert(key.to_string(), value);
            }
        }
        Some(ex)
    }

    /// Apply the file's `map`, then the built-in aliases, then PSR-3 parsing.
    pub fn normalize_level(&self, raw: Option<&str>) -> LogLevel {
        let raw = raw
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.level_default);
        let lower = raw.to_ascii_lowercase();
        if let Some(mapped) = self.level_map.get(&lower) {
            return LogLevel::parse(mapped);
        }
        if let Some((_, canonical)) = LEVEL_ALIASES.iter().find(|(alias, _)| *alias == lower) {
            return LogLevel::parse(canonical);
        }
        LogLevel::parse(raw)
    }

    /// Report an unparsable timestamp once per format.
    pub fn warn_bad_timestamp(&self, raw: &str) {
        if !self.warned_timestamp.swap(true, Ordering::Relaxed) {
            tracing::warn!(format = %self.name, value = raw, "timestamp does not match timestamp.format; such entries are dropped");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Parser, SourceTz};
    use std::sync::Arc;

    fn parser(name: &str) -> Parser {
        let f = LogFormat::from_yaml(builtin(name).unwrap(), name, "built-in").unwrap();
        Parser::new(Arc::new(f), SourceTz::Named(chrono_tz::UTC), false, "host")
    }

    fn fixture(name: &str) -> Vec<String> {
        let p = format!(
            "{}/tests/fixtures/formats/{name}.log",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::read_to_string(p)
            .unwrap()
            .lines()
            .map(String::from)
            .collect()
    }

    #[test]
    fn every_builtin_compiles() {
        for (name, yaml) in BUILTINS {
            LogFormat::from_yaml(yaml, name, "built-in").unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }

    #[test]
    fn resolve_path_then_builtin() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("custom.yaml");
        std::fs::write(
            &file,
            "match: ['^%{GREEDYDATA:message}$']\ntimestamp: {field: null}\n",
        )
        .unwrap();
        let f = LogFormat::resolve(file.to_str().unwrap()).unwrap();
        assert_eq!(f.name, "custom");
        assert!(f.source.ends_with("custom.yaml"));
        let f = LogFormat::resolve("syslog").unwrap();
        assert_eq!(f.source, "built-in");
    }

    #[test]
    fn resolve_errors_are_actionable() {
        let e = LogFormat::resolve("nope").err().unwrap().to_string();
        assert!(e.contains("'nope'") && e.contains("laravel"), "{e}");
        let e = LogFormat::resolve("/no/such/file.yaml")
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("not found"), "{e}");
        assert!(LogFormat::resolve("  ").is_err());
    }

    #[test]
    fn validation_errors() {
        let e = LogFormat::from_yaml("match: ['%{NOPE:message}']\n", "x", "x.yaml")
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("match[0]"), "{e}");
        let e = LogFormat::from_yaml("match: ['%{GREEDYDATA:msg}']\n", "x", "x.yaml")
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("message.field"), "{e}");
        let e = LogFormat::from_yaml("match: ['%{GREEDYDATA:message}']\n", "x", "x.yaml")
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("timestamp.field"), "{e}");
        let e = LogFormat::from_yaml("match: ['%{GREEDYDATA:message}']\ntimestamp: {field: null}\nmultiline: {mode: pattern}\n", "x", "x.yaml")
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("multiline.pattern"), "{e}");
        let e = LogFormat::from_yaml("match: ['x']\nbogus: 1\n", "x", "x.yaml")
            .err()
            .unwrap()
            .to_string();
        assert!(e.contains("bogus"), "{e}");
    }

    #[test]
    fn level_normalisation() {
        let f = LogFormat::from_yaml(
            "match: ['%{GREEDYDATA:message}']\ntimestamp: {field: null}\nlevel:\n  default: notice\n  map: {'Fatal error': CRITICAL}\n",
            "x",
            "x",
        )
        .unwrap();
        use crate::level::Level;
        assert_eq!(f.normalize_level(None), LogLevel::Known(Level::Notice));
        assert_eq!(
            f.normalize_level(Some("fatal error")),
            LogLevel::Known(Level::Critical)
        );
        assert_eq!(
            f.normalize_level(Some("WARN")),
            LogLevel::Known(Level::Warning)
        );
        assert_eq!(
            f.normalize_level(Some("emerg")),
            LogLevel::Known(Level::Emergency)
        );
        assert_eq!(
            f.normalize_level(Some("custom")),
            LogLevel::Unknown("custom".into())
        );
    }

    /// Every fixture line is either a header that parses (timestamp included)
    /// or a continuation line; a pattern regression that demotes a header to a
    /// continuation shows up as a wrong count.
    fn assert_fixture_parses(name: &str, expect_continuations: usize) {
        let p = parser(name);
        let mut continuations = 0;
        for (i, line) in fixture(name).iter().enumerate() {
            if p.is_header(line) {
                assert!(p.parse(line).is_some(), "{name}:{}: {line}", i + 1);
            } else {
                assert!(p.accepts_continuation(line), "{name}:{}: {line}", i + 1);
                continuations += 1;
            }
        }
        assert_eq!(continuations, expect_continuations, "{name}");
    }

    #[test]
    fn web_server_fixtures_parse_completely() {
        assert_fixture_parses("nginx_access", 0);
        assert_fixture_parses("apache_access", 0);
        assert_fixture_parses("nginx_error", 3);
        assert_fixture_parses("apache_error", 2);
    }

    #[test]
    fn nginx_error_format() {
        let p = parser("nginx_error");
        let lines = fixture("nginx_error");
        let e = p.parse(&lines[0]).unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2026-10-27T10:15:01+00:00");
        assert_eq!(e.level.as_str(), "ERROR");
        assert!(e.message.starts_with("connect() failed"));
        assert!(
            e.message
                .ends_with(r#"upstream: "http://127.0.0.1:9000/index.php""#)
        );
        assert_eq!(e.fields["process.pid"], "1234");
        assert_eq!(e.fields["process.thread.id"], "1234");
        assert_eq!(e.fields["nginx.connection_id"], "77");
        let e = p.parse(&lines[1]).unwrap();
        assert_eq!(e.level.as_str(), "CRITICAL");
        assert!(!p.is_header(&lines[2]));
        assert!(p.accepts_continuation(&lines[2]));

        for (i, level) in [
            (3, "NOTICE"),
            (4, "WARNING"),
            (5, "EMERGENCY"),
            (6, "ALERT"),
            (7, "INFO"),
            (8, "ERROR"),
        ] {
            let e = p.parse(&lines[i]).unwrap_or_else(|| panic!("line {i}"));
            assert_eq!(e.level.as_str(), level, "line {i}");
        }
        // Master-process lines carry no connection id.
        let e = p.parse(&lines[5]).unwrap();
        assert_eq!(e.fields["process.pid"], "1");
        assert!(!e.fields.contains_key("nginx.connection_id"));
        assert_eq!(
            e.message,
            "bind() to 0.0.0.0:80 failed (98: Address already in use)"
        );
        let e = p.parse(&lines[8]).unwrap();
        assert!(
            e.message
                .starts_with(r#"open() "/var/www/html/favicon.ico" failed"#)
        );
        // FastCGI stderr with embedded newlines: the tail lines are continuations.
        let e = p.parse(&lines[11]).unwrap();
        assert_eq!(e.fields["nginx.connection_id"], "84");
        assert!(e.message.starts_with("FastCGI sent in stderr"));
        for line in &lines[12..14] {
            assert!(!p.is_header(line) && p.accepts_continuation(line), "{line}");
        }
    }

    #[test]
    fn access_log_formats() {
        for name in ["nginx_access", "apache_access"] {
            let p = parser(name);
            let lines = fixture(name);
            let e = p.parse(&lines[0]).unwrap_or_else(|| panic!("{name}"));
            assert_eq!(
                e.timestamp.to_rfc3339(),
                "2000-10-10T20:55:36+00:00",
                "{name}"
            );
            assert_eq!(e.message, "GET /apache_pb.gif HTTP/1.0");
            assert_eq!(e.level.as_str(), "INFO");
            assert_eq!(e.fields["client.address"], "127.0.0.1");
            assert_eq!(e.fields["http.request.method"], "GET");
            assert_eq!(e.fields["url.path"], "/apache_pb.gif");
            assert_eq!(e.fields["network.protocol.version"], "1.0");
            assert_eq!(e.fields["http.response.status_code"], "200");
            assert_eq!(e.fields["http.response.body.size"], "2326");
            assert_eq!(
                e.fields["http.request.referrer"],
                "http://www.example.com/start.html"
            );
            assert_eq!(
                e.fields["user_agent.original"],
                "Mozilla/4.08 [en] (Win98; I ;Nav)"
            );
            assert_eq!(e.fields["user.name"], "frank");
            assert!(!e.fields.contains_key("client.ident"));
            assert!(!e.fields.contains_key("http.request.raw"));
            // Common (no referrer / agent) and a dash body size.
            let e = p.parse(&lines[1]).unwrap();
            assert_eq!(e.fields["http.response.status_code"], "401");
            assert!(!e.fields.contains_key("user.name"));
            assert!(!e.fields.contains_key("http.response.body.size"));
            assert!(!e.fields.contains_key("user_agent.original"));
            assert!(!p.accepts_continuation("garbage"));
        }
    }

    #[test]
    fn nginx_access_format() {
        let p = parser("nginx_access");
        let lines = fixture("nginx_access");
        // IPv6 clients, full and compressed.
        let e = p.parse(&lines[2]).unwrap();
        assert_eq!(
            e.fields["client.address"],
            "2001:4801:7824:102:8bee:6e66:ff10:6aa2"
        );
        assert_eq!(e.fields["http.response.body.size"], "85619205");
        assert!(!e.fields.contains_key("http.request.referrer"));
        assert_eq!(
            e.fields["user_agent.original"],
            "Chef Client/12.0.3 (ruby-2.1.4-p265; ohai-8.0.1; x86_64-linux; +http://opscode.com)"
        );
        let e = p.parse(&lines[3]).unwrap();
        assert_eq!(e.fields["client.address"], "2a01:7e00::f03c:91ff:fe70:a4cc");
        assert_eq!(e.timestamp.to_rfc3339(), "2015-05-18T16:05:29+00:00");
        // HEAD with a zero body is a size, not a missing value.
        let e = p.parse(&lines[4]).unwrap();
        assert_eq!(e.fields["http.request.method"], "HEAD");
        assert_eq!(e.fields["http.response.body.size"], "0");
        for (i, status) in [(5, "304"), (6, "404"), (7, "206"), (8, "403"), (9, "416")] {
            let e = p.parse(&lines[i]).unwrap();
            assert_eq!(e.fields["http.response.status_code"], status, "line {i}");
            assert_eq!(e.level.as_str(), "INFO", "line {i}");
        }
        // Connection closed before a request: nginx logs "-".
        let e = p.parse(&lines[10]).unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2026-10-27T08:20:01+00:00");
        assert_eq!(e.message, "-");
        assert_eq!(e.fields["http.response.status_code"], "400");
        assert!(!e.fields.contains_key("http.request.method"));
        assert!(!e.fields.contains_key("url.path"));
        assert!(!e.fields.contains_key("user_agent.original"));
        // TLS handshake on a plain-HTTP port, escaped by nginx.
        let e = p.parse(&lines[11]).unwrap();
        assert_eq!(e.message, r"\x16\x03\x01\x02\x00\x01\x00\x01\xFC\x03\x03");
        assert_eq!(e.fields["http.request.raw"], e.message);
        assert!(!e.fields.contains_key("http.request.method"));
        // HTTP/2, query string, and nginx's \x22 quote escape.
        let e = p.parse(&lines[12]).unwrap();
        assert_eq!(e.fields["network.protocol.version"], "2.0");
        assert_eq!(e.fields["url.path"], "/api/v1/items?page=2&sort=-created");
        assert_eq!(
            e.fields["http.request.referrer"],
            "https://app.example.com/items"
        );
        assert_eq!(
            e.fields["user_agent.original"],
            r"curl/8.4.0 \x22quoted\x22"
        );
    }

    #[test]
    fn apache_access_format() {
        let p = parser("apache_access");
        let lines = fixture("apache_access");
        let e = p.parse(&lines[2]).unwrap();
        assert_eq!(e.fields["http.request.method"], "POST");
        assert_eq!(e.fields["network.protocol.version"], "1.0");
        assert_eq!(
            e.fields["http.request.referrer"],
            "http://zoomq.qiniudn.com/ZQScrapBook/ZqFLOSS/data/20110811235550/"
        );
        let e = p.parse(&lines[3]).unwrap();
        assert_eq!(e.fields["http.request.method"], "HEAD");
        assert!(!e.fields.contains_key("http.response.body.size"));
        let e = p.parse(&lines[4]).unwrap();
        assert_eq!(e.message, "OPTIONS /projects/xdotool/ HTTP/1.1");
        assert_eq!(e.fields["http.request.method"], "OPTIONS");
        assert_eq!(e.fields["http.response.status_code"], "500");
        let e = p.parse(&lines[5]).unwrap();
        assert!(!e.fields.contains_key("http.response.body.size"));
        assert!(e.fields["user_agent.original"].ends_with("webmasters.htm#07)"));
        let e = p.parse(&lines[6]).unwrap();
        assert_eq!(e.fields["http.response.status_code"], "301");
        // Long referrer with a query string and percent-encoding stays intact.
        let e = p.parse(&lines[7]).unwrap();
        assert!(
            e.fields["http.request.referrer"]
                .starts_with("http://www.google.fr/url?sa=t&rct=j&q=&")
        );
        assert!(e.fields["http.request.referrer"].ends_with("&bvm=bv.61535280,d.d2k"));
        assert_eq!(
            e.fields["user_agent.original"],
            "Mozilla/5.0 (Windows NT 5.1; rv:26.0) Gecko/20100101 Firefox/26.0"
        );
        let e = p.parse(&lines[8]).unwrap();
        assert_eq!(
            e.fields["url.path"],
            "/doc/index.html?org/elasticsearch/action/search/SearchResponse.html"
        );
        assert!(
            p.parse(&lines[9]).unwrap().fields["user_agent.original"].contains("Googlebot/2.1")
        );
        // HostnameLookups On, an authenticated user, a -0500 offset and Apache's \" escape.
        let e = p.parse(&lines[10]).unwrap();
        assert_eq!(e.fields["client.address"], "crawler-7.example.net");
        assert_eq!(e.fields["user.name"], "frank@example.com");
        assert_eq!(e.timestamp.to_rfc3339(), "2026-10-27T10:30:00+00:00");
        assert_eq!(
            e.fields["user_agent.original"],
            r#"Mozilla/5.0 \"Quoted\" Agent"#
        );
        // A request line that is not METHOD PATH VERSION is kept raw.
        let e = p.parse(&lines[11]).unwrap();
        assert_eq!(e.message, "GET /a path/with spaces HTTP/1.1");
        assert_eq!(e.fields["http.request.raw"], e.message);
        assert!(!e.fields.contains_key("http.request.method"));
        assert!(!e.fields.contains_key("url.path"));
    }

    #[test]
    fn access_logs_reject_malformed() {
        let malformed = [
            // Truncated user agent, verbatim from the Elastic apache_logs sample.
            r#"46.118.127.106 - - [20/May/2015:12:05:17 +0000] "GET /scripts/grok-py-test/configlib.py HTTP/1.1" 200 235 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html"#,
            r#"10.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1" OK 12"#,
            r#"10.0.0.1 - - [10/Oct/2000 13:55:36] "GET / HTTP/1.1" 200 12"#,
            "",
        ];
        for name in ["nginx_access", "apache_access"] {
            let p = parser(name);
            for line in malformed {
                assert!(!p.is_header(line), "{name}: {line}");
                assert!(p.parse(line).is_none(), "{name}: {line}");
                assert!(!p.accepts_continuation(line), "{name}: {line}");
            }
        }
    }

    #[test]
    fn apache_error_format() {
        let p = parser("apache_error");
        let lines = fixture("apache_error");
        let e = p.parse(&lines[0]).unwrap();
        assert_eq!(
            e.timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "2024-01-01T12:00:00.123456Z"
        );
        assert_eq!(e.level.as_str(), "ERROR");
        assert_eq!(e.fields["apache.module"], "core");
        assert_eq!(e.fields["process.pid"], "1234");
        assert_eq!(e.fields["process.thread.id"], "5678");
        assert_eq!(e.fields["client.address"], "192.168.1.5:51234");
        assert!(e.message.starts_with("AH00124"));
        // Apache 2.2 style without microseconds or module.
        let e = p.parse(&lines[1]).unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2024-01-01T12:00:01+00:00");
        assert_eq!(e.level.as_str(), "NOTICE");
        assert!(!e.fields.contains_key("apache.module"));
        assert!(!e.fields.contains_key("process.pid"));
        let e = p.parse(&lines[2]).unwrap();
        assert_eq!(e.level.as_str(), "DEBUG"); // trace3
        assert!(!e.fields.contains_key("process.thread.id"));

        // mod_php5 under 2.4 logs an empty module: "[:error]".
        let e = p.parse(&lines[3]).expect("[:error] is a header");
        assert_eq!(e.level.as_str(), "ERROR");
        assert!(!e.fields.contains_key("apache.module"));
        assert_eq!(e.fields["process.pid"], "2345");
        assert!(e.message.starts_with("PHP Fatal error:"));
        let e = p.parse(&lines[4]).unwrap();
        assert_eq!(e.fields["apache.module"], "proxy_fcgi");
        assert_eq!(e.fields["process.thread.id"], "140234567890123");
        assert!(e.message.ends_with(", referer: https://example.com/"));
        let e = p.parse(&lines[5]).unwrap();
        assert_eq!(e.fields["apache.module"], "mpm_event");
        assert!(!e.fields.contains_key("client.address"));
        let e = p.parse(&lines[6]).unwrap();
        assert_eq!(e.fields["apache.module"], "authz_core");
        assert_eq!(e.fields["client.address"], "::1:51234");
        assert!(e.message.starts_with("AH01630"));
        for (i, level) in [(7, "WARNING"), (8, "ALERT"), (9, "EMERGENCY")] {
            assert_eq!(
                p.parse(&lines[i]).unwrap().level.as_str(),
                level,
                "line {i}"
            );
        }
        let e = p.parse(&lines[10]).unwrap();
        assert_eq!(e.fields["client.address"], "1.2.3.4");
        assert_eq!(e.message, "File does not exist: /var/www/favicon.ico");
        let e = p.parse(&lines[11]).unwrap();
        assert_eq!(e.fields["apache.module"], "php");
        for line in &lines[12..14] {
            assert!(!p.is_header(line) && p.accepts_continuation(line), "{line}");
        }
    }

    #[test]
    fn syslog_format() {
        let p = parser("syslog");
        let lines = fixture("syslog");
        let e = p.parse(&lines[0]).unwrap();
        assert_eq!(
            e.timestamp.format("%m-%d %H:%M:%S").to_string(),
            "10-27 10:00:01"
        );
        assert_eq!(e.fields["host.name"], "web01");
        assert_eq!(e.fields["process.name"], "sshd");
        assert_eq!(e.fields["process.pid"], "4242");
        assert_eq!(
            e.message,
            "Accepted publickey for deploy from 10.0.0.9 port 51234 ssh2"
        );
        assert_eq!(e.level.as_str(), "INFO");
        let e = p.parse(&lines[1]).unwrap();
        assert_eq!(e.timestamp.format("%m-%d").to_string(), "10-07");
        assert!(!e.fields.contains_key("process.pid"));
        assert!(!p.accepts_continuation("anything"));
    }

    #[test]
    fn php_format() {
        let p = parser("php");
        let lines = fixture("php");
        let e = p.parse(&lines[0]).unwrap();
        assert_eq!(e.timestamp.to_rfc3339(), "2026-10-27T09:00:00+00:00"); // Europe/Madrid
        assert_eq!(e.level.as_str(), "WARNING");
        assert_eq!(
            e.message,
            "Undefined variable $x in /app/index.php on line 3"
        );
        assert!(!e.fields.contains_key("tz"));
        let e = p.parse(&lines[1]).unwrap();
        assert_eq!(e.level.as_str(), "CRITICAL");
        assert!(p.accepts_continuation(&lines[2]));
        assert!(p.accepts_continuation(&lines[3]));
        assert!(p.accepts_continuation(&lines[4]));
        let e = p.parse(&lines[5]).unwrap(); // php-fpm master line, no zone
        assert_eq!(e.level.as_str(), "NOTICE");
        assert_eq!(e.timestamp.to_rfc3339(), "2026-10-27T10:00:01+00:00");
        assert!(!p.accepts_continuation("random text"));
    }

    #[test]
    fn read_time_when_timestamp_disabled() {
        let f = LogFormat::from_yaml(
            "match: ['^%{GREEDYDATA:message}$']\ntimestamp: {field: null}\n",
            "raw",
            "x",
        )
        .unwrap();
        let p = Parser::new(Arc::new(f), SourceTz::Named(chrono_tz::UTC), false, "h");
        let before = chrono::Utc::now();
        let e = p.parse("anything at all").unwrap();
        assert!(e.timestamp >= before);
        assert_eq!(e.message, "anything at all");
    }
}
