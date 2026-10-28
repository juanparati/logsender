//! OpenTelemetry OTLP log exporter (gRPC or HTTP/protobuf).
//!
//! Records are built by hand from parsed entries so the original timestamp,
//! severity and attributes are preserved. Batching and retries are handled
//! by the pipeline, so the exporter is driven directly instead of through an
//! SDK processor.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider};
use opentelemetry::{InstrumentationScope, Key, KeyValue};
use opentelemetry_otlp::{
    Compression, Protocol, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{
    LogBatch, LogExporter as _, SdkLogRecord, SdkLogger, SdkLoggerProvider,
};
use serde_json::Value;

use super::Sender;
use crate::cli::OtlpProtocol;
use crate::entry::LogEntry;
use crate::error::{Error, Result};

const BACKEND: &str = "otlp";

pub struct OtlpSender {
    exporter: opentelemetry_otlp::LogExporter,
    logger: SdkLogger,
    scope: InstrumentationScope,
    // Keeps the provider (and its record factory) alive.
    _provider: SdkLoggerProvider,
    smart: bool,
}

pub struct OtlpConfig<'a> {
    pub endpoint: &'a str,
    pub protocol: OtlpProtocol,
    pub service_name: &'a str,
    pub hostname: &'a str,
    pub check_cert: bool,
    pub timeout: Duration,
    pub smart: bool,
}

impl OtlpSender {
    pub fn new(cfg: OtlpConfig<'_>) -> Result<Self> {
        super::ensure_crypto_provider();
        let endpoint = cfg.endpoint.trim();
        let is_https = endpoint.starts_with("https://");

        let mut exporter = match cfg.protocol {
            OtlpProtocol::Grpc => {
                if !cfg.check_cert {
                    return Err(Error::Config(
                        "--no-check-cert is not supported for OTLP over gRPC; use --otlp-protocol=http".into(),
                    ));
                }
                let mut b = opentelemetry_otlp::LogExporter::builder()
                    .with_tonic()
                    .with_endpoint(endpoint)
                    .with_timeout(cfg.timeout)
                    .with_compression(Compression::Gzip);
                if is_https {
                    b = b.with_tls_config(
                        tonic::transport::ClientTlsConfig::new().with_enabled_roots(),
                    );
                }
                b.build()
                    .map_err(|e| Error::Config(format!("OTLP gRPC exporter: {e}")))?
            }
            OtlpProtocol::Http => {
                let url = if endpoint.ends_with("/v1/logs") {
                    endpoint.to_string()
                } else {
                    super::join_url(endpoint, "v1/logs")
                };
                let client = reqwest::Client::builder()
                    .timeout(cfg.timeout)
                    .danger_accept_invalid_certs(!cfg.check_cert)
                    .build()
                    .map_err(|e| Error::sender(BACKEND, e))?;
                opentelemetry_otlp::LogExporter::builder()
                    .with_http()
                    .with_protocol(Protocol::HttpBinary)
                    .with_endpoint(url)
                    .with_timeout(cfg.timeout)
                    .with_http_client(client)
                    .with_compression(Compression::Gzip)
                    .build()
                    .map_err(|e| Error::Config(format!("OTLP HTTP exporter: {e}")))?
            }
        };

        let resource = Resource::builder()
            .with_service_name(cfg.service_name.to_string())
            .with_attributes([
                KeyValue::new("host.name", cfg.hostname.to_string()),
                KeyValue::new("telemetry.sdk.language", "rust"),
            ])
            .build();
        exporter.set_resource(&resource);

        let provider = SdkLoggerProvider::builder().with_resource(resource).build();
        let logger = provider.logger("logsender");
        let scope = InstrumentationScope::builder("logsender")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build();

        Ok(OtlpSender {
            exporter,
            logger,
            scope,
            _provider: provider,
            smart: cfg.smart,
        })
    }

    pub fn record(&self, e: &LogEntry) -> SdkLogRecord {
        let mut r = self.logger.create_log_record();
        r.set_timestamp(SystemTime::from(e.timestamp));
        r.set_observed_timestamp(SystemTime::now());
        r.set_severity_number(e.level.severity());
        r.set_severity_text(static_str(e.level.as_str()));
        r.set_body(AnyValue::from(e.message.clone()));
        r.set_target("laravel");

        r.add_attribute("laravel.environment", e.environment.clone());
        r.add_attribute("deployment.environment.name", e.environment.clone());
        r.add_attribute("laravel.level", e.level.as_str().to_string());
        if let Some(st) = &e.stacktrace {
            r.add_attribute("exception.stacktrace", st.clone());
        }
        for (k, v) in &e.fields {
            r.add_attribute(Key::new(k.clone()), v.clone());
        }
        match (&e.params, self.smart) {
            (Some(params), true) => {
                let mut attrs = Vec::new();
                flatten("params", params, &mut attrs);
                r.add_attributes(attrs);
            }
            _ => {
                if !e.data.is_empty() {
                    let v: AnyValue = if e.data.len() == 1 {
                        AnyValue::from(e.data[0].clone())
                    } else {
                        e.data.iter().cloned().map(AnyValue::from).collect()
                    };
                    r.add_attribute("log.data", v);
                }
            }
        }
        r
    }
}

/// Flatten a JSON value into dotted attribute keys.
/// Objects recurse, arrays become a JSON string, nulls are skipped.
pub fn flatten(prefix: &str, v: &Value, out: &mut Vec<(Key, AnyValue)>) {
    match v {
        Value::Null => {}
        Value::Bool(b) => out.push((Key::new(prefix.to_string()), AnyValue::from(*b))),
        Value::Number(n) => {
            let av = if let Some(i) = n.as_i64() {
                AnyValue::from(i)
            } else {
                AnyValue::from(n.as_f64().unwrap_or(0.0))
            };
            out.push((Key::new(prefix.to_string()), av));
        }
        Value::String(s) => out.push((Key::new(prefix.to_string()), AnyValue::from(s.clone()))),
        Value::Array(items) => {
            // Top-level array = several JSON chunks on the line: index them.
            if prefix == "params" {
                for (i, item) in items.iter().enumerate() {
                    flatten(&format!("{prefix}.{i}"), item, out);
                }
            } else {
                out.push((Key::new(prefix.to_string()), AnyValue::from(v.to_string())));
            }
        }
        Value::Object(map) => {
            for (k, val) in map {
                flatten(&format!("{prefix}.{k}"), val, out);
            }
        }
    }
}

/// `set_severity_text` wants a `&'static str`. Known levels already are;
/// unknown custom levels are interned once (bounded) and otherwise reported
/// as UNKNOWN.
fn static_str(s: &str) -> &'static str {
    use crate::level::Level;
    if let Ok(l) = s.parse::<Level>() {
        return l.as_str();
    }
    static INTERNED: Mutex<Option<HashSet<&'static str>>> = Mutex::new(None);
    let mut guard = INTERNED.lock().expect("intern mutex");
    let set = guard.get_or_insert_with(HashSet::new);
    if let Some(existing) = set.get(s) {
        return existing;
    }
    if set.len() >= 64 {
        return "UNKNOWN";
    }
    let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
    set.insert(leaked);
    leaked
}

#[async_trait]
impl Sender for OtlpSender {
    fn name(&self) -> &'static str {
        BACKEND
    }

    async fn send(&mut self, entries: &[LogEntry]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let records: Vec<SdkLogRecord> = entries.iter().map(|e| self.record(e)).collect();
        let data: Vec<(&SdkLogRecord, &InstrumentationScope)> =
            records.iter().map(|r| (r, &self.scope)).collect();
        self.exporter
            .export(LogBatch::new(&data))
            .await
            .map_err(|e| Error::sender(BACKEND, e))?;
        Ok(0)
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.exporter
            .shutdown()
            .map_err(|e| Error::sender(BACKEND, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::{Level, LogLevel};
    use chrono::TimeZone;
    use opentelemetry::logs::Severity;

    fn sender(smart: bool) -> OtlpSender {
        OtlpSender::new(OtlpConfig {
            endpoint: "http://127.0.0.1:4318",
            protocol: OtlpProtocol::Http,
            service_name: "svc",
            hostname: "web1",
            check_cert: true,
            timeout: Duration::from_secs(1),
            smart,
        })
        .unwrap()
    }

    fn entry() -> LogEntry {
        LogEntry {
            timestamp: chrono::Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            environment: "production".into(),
            level: LogLevel::Known(Level::Critical),
            message: "boom".into(),
            data: vec![r#"{"a":1}"#.into()],
            params: Some(serde_json::json!({"a": 1, "n": {"b": true, "c": [1,2]}, "z": null})),
            hostname: "web1".into(),
            stacktrace: Some("#0 main".into()),
            fields: [("http.response.status_code".to_string(), "503".to_string())]
                .into_iter()
                .collect(),
        }
    }

    fn attrs(r: &SdkLogRecord) -> std::collections::HashMap<String, AnyValue> {
        r.attributes_iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[tokio::test]
    async fn record_mapping_smart() {
        let s = sender(true);
        let r = s.record(&entry());
        assert_eq!(
            r.timestamp(),
            Some(SystemTime::from(
                chrono::Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
            ))
        );
        assert!(r.observed_timestamp().is_some());
        assert_eq!(r.severity_number(), Some(Severity::Fatal));
        assert_eq!(r.severity_text(), Some("CRITICAL"));
        assert_eq!(r.body(), Some(&AnyValue::from("boom")));
        let a = attrs(&r);
        assert_eq!(a["laravel.environment"], AnyValue::from("production"));
        assert_eq!(
            a["deployment.environment.name"],
            AnyValue::from("production")
        );
        assert_eq!(a["exception.stacktrace"], AnyValue::from("#0 main"));
        assert_eq!(a["http.response.status_code"], AnyValue::from("503"));
        assert_eq!(a["params.a"], AnyValue::Int(1));
        assert_eq!(a["params.n.b"], AnyValue::Boolean(true));
        assert_eq!(a["params.n.c"], AnyValue::from("[1,2]"));
        assert!(!a.contains_key("params.z"));
        assert!(!a.contains_key("log.data"));
    }

    #[tokio::test]
    async fn record_mapping_not_smart_keeps_raw_data() {
        let s = sender(false);
        let r = s.record(&entry());
        let a = attrs(&r);
        assert_eq!(a["log.data"], AnyValue::from(r#"{"a":1}"#));
        assert!(!a.contains_key("params.a"));
    }

    #[tokio::test]
    async fn several_chunks_are_indexed() {
        let s = sender(true);
        let mut e = entry();
        e.params = Some(serde_json::json!([{"a": 1}, {"b": 2}]));
        let a = attrs(&s.record(&e));
        assert_eq!(a["params.0.a"], AnyValue::Int(1));
        assert_eq!(a["params.1.b"], AnyValue::Int(2));
    }

    #[test]
    fn unknown_level_text_is_interned() {
        assert_eq!(static_str("VERBOSE"), "VERBOSE");
        assert_eq!(static_str("error"), "ERROR");
    }

    #[test]
    fn grpc_rejects_no_check_cert() {
        let err = OtlpSender::new(OtlpConfig {
            endpoint: "https://x:4317",
            protocol: OtlpProtocol::Grpc,
            service_name: "s",
            hostname: "h",
            check_cert: false,
            timeout: Duration::from_secs(1),
            smart: false,
        })
        .err()
        .unwrap();
        assert!(err.to_string().contains("--otlp-protocol=http"));
    }
    #[tokio::test]
    async fn http_export_end_to_end_decodes_as_otlp() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;
        use std::io::Read;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/logs"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let mut s = OtlpSender::new(OtlpConfig {
            endpoint: &server.uri(),
            protocol: OtlpProtocol::Http,
            service_name: "svc",
            hostname: "web1",
            check_cert: true,
            timeout: Duration::from_secs(5),
            smart: true,
        })
        .unwrap();
        assert_eq!(s.send(&[entry()]).await.unwrap(), 0);

        let req = &server.received_requests().await.unwrap()[0];
        assert_eq!(
            req.headers.get("content-type").unwrap(),
            "application/x-protobuf"
        );
        let mut body = Vec::new();
        if req
            .headers
            .get("content-encoding")
            .map(|v| v == "gzip")
            .unwrap_or(false)
        {
            flate2::read::GzDecoder::new(&req.body[..])
                .read_to_end(&mut body)
                .unwrap();
        } else {
            body = req.body.clone();
        }
        let decoded = ExportLogsServiceRequest::decode(&body[..]).unwrap();
        let rl = &decoded.resource_logs[0];
        let res_attrs: std::collections::HashMap<_, _> = rl
            .resource
            .as_ref()
            .unwrap()
            .attributes
            .iter()
            .map(|kv| {
                (
                    kv.key.clone(),
                    format!("{:?}", kv.value.as_ref().unwrap().value),
                )
            })
            .collect();
        assert!(res_attrs["service.name"].contains("svc"));
        assert!(res_attrs["host.name"].contains("web1"));

        let record = &rl.scope_logs[0].log_records[0];
        assert_eq!(record.time_unix_nano, 1_577_836_800_000_000_000);
        assert!(record.observed_time_unix_nano > record.time_unix_nano);
        assert_eq!(record.severity_number, 21); // FATAL
        assert_eq!(record.severity_text, "CRITICAL");
        assert!(format!("{:?}", record.body).contains("boom"));
        let keys: Vec<&str> = record.attributes.iter().map(|kv| kv.key.as_str()).collect();
        assert!(keys.contains(&"params.a"));
        assert!(keys.contains(&"exception.stacktrace"));
        assert!(keys.contains(&"laravel.environment"));
    }
}
