//! Datadog Logs API v2: `POST {site}/api/v2/logs` with `DD-API-KEY`.
//! Payload: JSON array, gzip-compressed, at most 1000 entries / 5 MB.

use async_trait::async_trait;
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::Value;
use std::io::Write;

use super::{Sender, join_url};
use crate::entry::{LogEntry, TimestampRepr};
use crate::error::{Error, Result};

pub const MAX_BATCH: usize = 1000;
const MAX_BODY_BYTES: usize = 5 * 1024 * 1024;
const V2_PATH: &str = "/api/v2/logs";
const BACKEND: &str = "datadog";

pub struct DatadogSender {
    client: reqwest::Client,
    url: String,
    api_key: String,
    service: String,
}

impl DatadogSender {
    pub fn new(
        host: &str,
        api_key: impl Into<String>,
        service: impl Into<String>,
        timeout: std::time::Duration,
    ) -> Result<Self> {
        let host = host.trim();
        if host.contains("/v1/input") {
            return Err(Error::Config(
                "Datadog v1 intake URLs (/v1/input/<KEY>) are no longer supported. \
                 Pass the site as HOST (e.g. https://http-intake.logs.datadoghq.eu) \
                 and the key with --api-key or DD_API_KEY."
                    .into(),
            ));
        }
        super::ensure_crypto_provider();
        let url = if host.ends_with(V2_PATH) {
            host.to_string()
        } else {
            join_url(host, V2_PATH)
        };
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(concat!("logsender/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::sender(BACKEND, e))?;
        Ok(DatadogSender {
            client,
            url,
            api_key: api_key.into(),
            service: service.into(),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn document(&self, e: &LogEntry) -> Value {
        let mut doc = e.document(TimestampRepr::EpochMillis, None);
        doc.insert("ddsource".into(), Value::String("laravel".into()));
        doc.insert("service".into(), Value::String(self.service.clone()));
        doc.insert("status".into(), Value::String(e.level.datadog_status()));
        Value::Object(doc)
    }

    async fn post(&self, docs: &[Value]) -> Result<()> {
        let json = serde_json::to_vec(docs).expect("serializable");
        if json.len() > MAX_BODY_BYTES && docs.len() > 1 {
            let (a, b) = docs.split_at(docs.len() / 2);
            Box::pin(self.post(a)).await?;
            return Box::pin(self.post(b)).await;
        }
        let mut gz = GzEncoder::new(Vec::with_capacity(json.len() / 4), Compression::default());
        gz.write_all(&json).map_err(Error::Io)?;
        let body = gz.finish().map_err(Error::Io)?;

        let resp = self
            .client
            .post(&self.url)
            .header("DD-API-KEY", &self.api_key)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(body)
            .send()
            .await
            .map_err(|e| Error::sender(BACKEND, e))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(Error::Http {
            backend: BACKEND,
            status: status.as_u16(),
            body: body.chars().take(512).collect(),
        })
    }
}

#[async_trait]
impl Sender for DatadogSender {
    fn name(&self) -> &'static str {
        BACKEND
    }

    fn max_batch(&self) -> usize {
        MAX_BATCH
    }

    async fn send(&mut self, entries: &[LogEntry]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let docs: Vec<Value> = entries.iter().map(|e| self.document(e)).collect();
        self.post(&docs).await?;
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::{Level, LogLevel};
    use chrono::TimeZone;
    use std::io::Read;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn entry(msg: &str) -> LogEntry {
        LogEntry {
            timestamp: chrono::Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            environment: "production".into(),
            level: LogLevel::Known(Level::Error),
            message: msg.into(),
            data: vec![],
            params: Some(serde_json::json!({"job": "x"})),
            hostname: "web1".into(),
            stacktrace: Some("#0 main".into()),
            fields: Default::default(),
        }
    }

    #[test]
    fn rejects_v1_url() {
        let err = DatadogSender::new(
            "https://http-intake.logs.datadoghq.eu/v1/input/abc",
            "k",
            "s",
            std::time::Duration::from_secs(1),
        )
        .err()
        .unwrap();
        assert!(matches!(err, Error::Config(_)));
        assert!(err.to_string().contains("--api-key"));
    }

    #[test]
    fn builds_v2_url() {
        let s = DatadogSender::new(
            "https://http-intake.logs.datadoghq.eu/",
            "k",
            "s",
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(s.url(), "https://http-intake.logs.datadoghq.eu/api/v2/logs");
        let s = DatadogSender::new(
            "https://x/api/v2/logs",
            "k",
            "s",
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(s.url(), "https://x/api/v2/logs");
    }

    #[tokio::test]
    async fn posts_gzipped_json_with_headers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v2/logs"))
            .and(header("DD-API-KEY", "secret"))
            .and(header("Content-Encoding", "gzip"))
            .and(header("Content-Type", "application/json"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;

        let mut s = DatadogSender::new(
            &server.uri(),
            "secret",
            "myapp",
            std::time::Duration::from_secs(5),
        )
        .unwrap();
        s.send(&[entry("boom")]).await.unwrap();

        let req = &server.received_requests().await.unwrap()[0];
        let mut gz = flate2::read::GzDecoder::new(&req.body[..]);
        let mut json = String::new();
        gz.read_to_string(&mut json).unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        let doc = &v[0];
        assert_eq!(doc["message"], "boom");
        assert_eq!(doc["service"], "myapp");
        assert_eq!(doc["ddsource"], "laravel");
        assert_eq!(doc["hostname"], "web1");
        assert_eq!(doc["status"], "error");
        assert_eq!(doc["level"], "ERROR");
        assert_eq!(doc["timestamp"], 1577836800000i64);
        assert_eq!(doc["params"]["job"], "x");
        assert_eq!(doc["stacktrace"], "#0 main");
    }

    #[tokio::test]
    async fn http_error_is_reported_with_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
            .mount(&server)
            .await;
        let mut s =
            DatadogSender::new(&server.uri(), "bad", "s", std::time::Duration::from_secs(5))
                .unwrap();
        let err = s.send(&[entry("x")]).await.err().unwrap();
        match err {
            Error::Http { status, .. } => assert_eq!(status, 403),
            other => panic!("unexpected {other:?}"),
        }
        assert!(!err.is_retryable());
    }
}
