//! Elasticsearch 8+ Bulk API. One `_bulk` request per batch, NDJSON body,
//! hosts rotated on connection failure.

use async_trait::async_trait;
use chrono_tz::Tz;
use serde_json::Value;

use super::{Sender, join_url};
use crate::entry::{LogEntry, TimestampRepr};
use crate::error::{Error, Result};

const BACKEND: &str = "elastic_search";

struct Host {
    url: String,
    basic: super::BasicAuth,
}

pub struct ElasticSender {
    client: reqwest::Client,
    hosts: Vec<Host>,
    current: usize,
    index: String,
    api_key: Option<String>,
    to_tz: Option<Tz>,
}

impl ElasticSender {
    pub fn new(
        hosts: &[String],
        index: impl Into<String>,
        api_key: Option<String>,
        to_tz: Option<Tz>,
        check_cert: bool,
        timeout: std::time::Duration,
    ) -> Result<Self> {
        if hosts.is_empty() {
            return Err(Error::Config(
                "at least one Elasticsearch host is required".into(),
            ));
        }
        super::ensure_crypto_provider();
        let hosts = hosts
            .iter()
            .map(|h| parse_host(h))
            .collect::<Result<Vec<_>>>()?;
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .danger_accept_invalid_certs(!check_cert)
            .user_agent(concat!("logsender/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::sender(BACKEND, e))?;
        Ok(ElasticSender {
            client,
            hosts,
            current: 0,
            index: index.into(),
            api_key,
            to_tz,
        })
    }

    fn body(&self, entries: &[LogEntry]) -> Vec<u8> {
        let mut body = Vec::with_capacity(entries.len() * 256);
        let action = serde_json::json!({"index": {"_index": self.index}});
        for e in entries {
            let mut doc = e.document(TimestampRepr::Iso8601, self.to_tz);
            doc.insert(
                "@timestamp".into(),
                Value::String(
                    e.timestamp
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                ),
            );
            serde_json::to_writer(&mut body, &action).expect("serializable");
            body.push(b'\n');
            serde_json::to_writer(&mut body, &Value::Object(doc)).expect("serializable");
            body.push(b'\n');
        }
        body
    }

    fn rotate(&mut self) {
        if self.hosts.len() > 1 {
            self.current = (self.current + 1) % self.hosts.len();
            tracing::warn!(host = %self.hosts[self.current].url, "switching Elasticsearch host");
        }
    }
}

fn parse_host(raw: &str) -> Result<Host> {
    let (url, basic) = super::split_credentials(raw, "Elasticsearch")?;
    Ok(Host { url, basic })
}

#[async_trait]
impl Sender for ElasticSender {
    fn name(&self) -> &'static str {
        BACKEND
    }

    async fn send(&mut self, entries: &[LogEntry]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let host = &self.hosts[self.current];
        let mut req = self
            .client
            .post(join_url(&host.url, "_bulk"))
            .header("Content-Type", "application/x-ndjson")
            .body(self.body(entries));
        if let Some((u, p)) = &host.basic {
            req = req.basic_auth(u, p.as_deref());
        }
        if let Some(key) = &self.api_key {
            req = req.header("Authorization", format!("ApiKey {key}"));
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                self.rotate();
                return Err(Error::sender(BACKEND, e));
            }
        };
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            if status.is_server_error() {
                self.rotate();
            }
            return Err(Error::Http {
                backend: BACKEND,
                status: status.as_u16(),
                body: body.chars().take(512).collect(),
            });
        }

        let parsed: Value = resp.json().await.map_err(|e| Error::sender(BACKEND, e))?;
        if parsed["errors"].as_bool() != Some(true) {
            return Ok(0);
        }
        // Partial failure: items that were rejected will not succeed on retry.
        let mut rejected = 0;
        if let Some(items) = parsed["items"].as_array() {
            for item in items {
                if let Some(err) = item["index"]["error"].as_object() {
                    rejected += 1;
                    tracing::warn!(error = ?err.get("type"), reason = ?err.get("reason"), "Elasticsearch rejected a document");
                }
            }
        }
        Ok(rejected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::{Level, LogLevel};
    use chrono::TimeZone;
    use std::time::Duration;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn entry(msg: &str) -> LogEntry {
        LogEntry {
            timestamp: chrono::Utc
                .with_ymd_and_hms(2016, 4, 16, 17, 25, 45)
                .unwrap(),
            environment: "local".into(),
            level: LogLevel::Known(Level::Info),
            message: msg.into(),
            data: vec![],
            params: None,
            hostname: "h".into(),
            stacktrace: None,
            fields: Default::default(),
        }
    }

    #[test]
    fn strips_credentials_from_url() {
        let h = parse_host("https://elastic:p%40ss@es.lan:9200/").unwrap();
        assert_eq!(h.url, "https://es.lan:9200/");
        assert_eq!(h.basic, Some(("elastic".into(), Some("p@ss".into()))));
        let h = parse_host("http://es:9200").unwrap();
        assert!(h.basic.is_none());
    }

    #[tokio::test]
    async fn bulk_body_shape_and_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("Content-Type", "application/x-ndjson"))
            .and(header("Authorization", "Basic dXNlcjpwdw=="))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"errors": false, "items": []})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let uri = server.uri().replace("http://", "http://user:pw@");
        let mut s = ElasticSender::new(
            &[uri],
            "laravel",
            None,
            Some(chrono_tz::Europe::Madrid),
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(s.send(&[entry("a"), entry("b")]).await.unwrap(), 0);

        let req = &server.received_requests().await.unwrap()[0];
        let body = String::from_utf8(req.body.clone()).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0], r#"{"index":{"_index":"laravel"}}"#);
        let doc: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(doc["timestamp"], "2016-04-16T19:25:45+02:00");
        assert_eq!(doc["@timestamp"], "2016-04-16T17:25:45.000Z");
        assert_eq!(doc["message"], "a");
        assert_eq!(doc["level"], "INFO");
    }

    #[tokio::test]
    async fn api_key_header() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("Authorization", "ApiKey abc"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"errors": false})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut s = ElasticSender::new(
            &[server.uri()],
            "i",
            Some("abc".into()),
            None,
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        s.send(&[entry("a")]).await.unwrap();
    }

    #[tokio::test]
    async fn partial_failures_are_counted_not_retried() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "errors": true,
                "items": [
                    {"index": {"status": 201}},
                    {"index": {"status": 400, "error": {"type": "mapper_parsing_exception", "reason": "bad"}}}
                ]
            })))
            .mount(&server)
            .await;
        let mut s = ElasticSender::new(
            &[server.uri()],
            "i",
            None,
            None,
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(s.send(&[entry("a"), entry("b")]).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn fails_over_to_next_host_on_connection_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"errors": false})),
            )
            .expect(1)
            .mount(&server)
            .await;
        // Port 9 (discard) refuses connections on most systems.
        let dead = "http://127.0.0.1:9".to_string();
        let mut s = ElasticSender::new(
            &[dead, server.uri()],
            "i",
            None,
            None,
            true,
            Duration::from_secs(2),
        )
        .unwrap();
        let first = s.send(&[entry("a")]).await;
        assert!(first.is_err() && first.err().unwrap().is_retryable());
        s.send(&[entry("a")]).await.unwrap();
    }
}
