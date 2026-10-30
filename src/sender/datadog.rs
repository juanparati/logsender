//! Datadog Logs API v2: `POST {site}/api/v2/logs` with `DD-API-KEY`.
//! Payload: JSON array, gzip-compressed, at most 1000 entries / 5 MB.
//! Datadog only accepts entries up to 18 hours old; older ones are not sent.

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
/// `ddsource`: the integration the logs come from.
const SOURCE: &str = "logsender";
/// Oldest timestamp the intake accepts.
const MAX_AGE: chrono::TimeDelta = chrono::TimeDelta::hours(18);

pub struct DatadogSender {
    client: reqwest::Client,
    url: String,
    api_key: String,
    service: String,
    /// Entries skipped for being older than `MAX_AGE`.
    too_old: u64,
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
            too_old: 0,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn document(&self, e: &LogEntry) -> Value {
        let mut doc = e.document(TimestampRepr::EpochMillis, None);
        doc.insert("ddsource".into(), Value::String(SOURCE.into()));
        doc.insert("service".into(), Value::String(self.service.clone()));
        let env = e.environment.trim();
        if !env.is_empty() {
            // Tags are comma-separated, so a comma would split the value.
            doc.insert(
                "ddtags".into(),
                Value::String(format!("env:{}", env.replace(',', "_"))),
            );
        }
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
        let cutoff = chrono::Utc::now() - MAX_AGE;
        let docs: Vec<Value> = entries
            .iter()
            .filter(|e| e.timestamp >= cutoff)
            .map(|e| self.document(e))
            .collect();
        if !docs.is_empty() {
            self.post(&docs).await?;
        }
        // Counted after the post so a retried batch is not counted twice.
        let skipped = entries.len() - docs.len();
        if skipped > 0 {
            if self.too_old == 0 {
                tracing::warn!(
                    "skipping entries older than 18 hours: Datadog does not accept them \
                     (they are counted as failed deliveries)"
                );
            }
            self.too_old += skipped as u64;
        }
        Ok(skipped)
    }

    async fn shutdown(&mut self) -> Result<()> {
        if self.too_old > 0 {
            tracing::warn!(
                skipped = self.too_old,
                "entries older than 18 hours were not sent to Datadog"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::{Level, LogLevel};
    use chrono::{TimeDelta, Utc};
    use std::io::Read;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn entry(msg: &str) -> LogEntry {
        LogEntry {
            timestamp: Utc::now(),
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
        let e = entry("boom");
        s.send(std::slice::from_ref(&e)).await.unwrap();

        let req = &server.received_requests().await.unwrap()[0];
        let mut gz = flate2::read::GzDecoder::new(&req.body[..]);
        let mut json = String::new();
        gz.read_to_string(&mut json).unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        let doc = &v[0];
        assert_eq!(doc["message"], "boom");
        assert_eq!(doc["service"], "myapp");
        assert_eq!(doc["ddsource"], "logsender");
        assert_eq!(doc["ddtags"], "env:production");
        assert_eq!(doc["hostname"], "web1");
        assert_eq!(doc["status"], "error");
        assert_eq!(doc["level"], "ERROR");
        assert_eq!(doc["timestamp"], e.timestamp.timestamp_millis());
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

    async fn received(server: &MockServer) -> Vec<Value> {
        let mut docs = Vec::new();
        for req in server.received_requests().await.unwrap() {
            let mut gz = flate2::read::GzDecoder::new(&req.body[..]);
            let mut json = String::new();
            gz.read_to_string(&mut json).unwrap();
            docs.extend(serde_json::from_str::<Vec<Value>>(&json).unwrap());
        }
        docs
    }

    #[tokio::test]
    async fn skips_entries_older_than_18_hours() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let mut s =
            DatadogSender::new(&server.uri(), "k", "s", std::time::Duration::from_secs(5)).unwrap();
        let mut old = entry("old");
        old.timestamp = Utc::now() - TimeDelta::hours(19);
        let skipped = s.send(&[old.clone(), entry("new")]).await.unwrap();
        assert_eq!(skipped, 1);
        let docs = received(&server).await;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["message"], "new");

        // A batch with only old entries makes no request at all.
        assert_eq!(s.send(&[old]).await.unwrap(), 1);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        assert_eq!(s.too_old, 2);
    }

    #[tokio::test]
    async fn no_ddtags_without_environment() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let mut s =
            DatadogSender::new(&server.uri(), "k", "s", std::time::Duration::from_secs(5)).unwrap();
        let mut a = entry("a");
        a.environment = String::new();
        let mut b = entry("b");
        b.environment = "prod,eu".into();
        s.send(&[a, b]).await.unwrap();
        let docs = received(&server).await;
        assert!(docs[0].get("ddtags").is_none());
        assert_eq!(docs[1]["ddtags"], "env:prod_eu");
    }
}
