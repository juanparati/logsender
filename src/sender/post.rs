//! Generic HTTP POST: one request per batch with a JSON array of entry
//! documents, for webhooks and custom ingestion endpoints.

use async_trait::async_trait;
use serde_json::Value;

use super::{Sender, split_credentials};
use crate::entry::{LogEntry, TimestampRepr};
use crate::error::{Error, Result};

const BACKEND: &str = "post";

pub struct PostSender {
    client: reqwest::Client,
    url: String,
    basic: Option<(String, Option<String>)>,
    headers: Vec<(String, String)>,
    bearer: Option<String>,
}

impl PostSender {
    pub fn new(
        host: &str,
        headers: &[(String, String)],
        api_key: Option<String>,
        check_cert: bool,
        timeout: std::time::Duration,
    ) -> Result<Self> {
        super::ensure_crypto_provider();
        let (url, basic) = split_credentials(host, BACKEND)?;
        // Exactly one Authorization header: an explicit --header wins, then URL
        // credentials (Basic), then --api-key (Bearer).
        let has_authorization = headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("authorization"));
        let basic = if has_authorization { None } else { basic };
        let bearer =
            api_key.filter(|k| !k.trim().is_empty() && !has_authorization && basic.is_none());
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .danger_accept_invalid_certs(!check_cert)
            .user_agent(concat!("logsender/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::sender(BACKEND, e))?;
        Ok(PostSender {
            client,
            url,
            basic,
            headers: headers.to_vec(),
            bearer,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

#[async_trait]
impl Sender for PostSender {
    fn name(&self) -> &'static str {
        BACKEND
    }

    async fn send(&mut self, entries: &[LogEntry]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let docs: Vec<Value> = entries
            .iter()
            .map(|e| Value::Object(e.document(TimestampRepr::EpochMillis, None)))
            .collect();

        let mut req = self
            .client
            .post(&self.url)
            .header("Content-Type", "application/json");
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        if let Some(token) = &self.bearer {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        if let Some((u, p)) = &self.basic {
            req = req.basic_auth(u, p.as_deref());
        }

        let resp = req
            .body(serde_json::to_vec(&docs).expect("serializable"))
            .send()
            .await
            .map_err(|e| Error::sender(BACKEND, e))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(0);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(Error::Http {
            backend: BACKEND,
            status: status.as_u16(),
            body: body.chars().take(512).collect(),
        })
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
            timestamp: chrono::Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            environment: "production".into(),
            level: LogLevel::Known(Level::Error),
            message: msg.into(),
            data: vec![],
            params: Some(serde_json::json!({"job": "x"})),
            hostname: "web1".into(),
            stacktrace: Some("#0 main".into()),
            fields: [("client.address".to_string(), "10.0.0.1".to_string())]
                .into_iter()
                .collect(),
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn posts_json_array_with_custom_header_and_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/hooks/logs"))
            .and(header("Content-Type", "application/json"))
            .and(header("X-Token", "abc"))
            .and(header("Authorization", "Bearer secret"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        let url = format!("{}/hooks/logs", server.uri());
        let mut s = PostSender::new(
            &url,
            &headers(&[("X-Token", "abc")]),
            Some("secret".into()),
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(s.url(), url);
        assert_eq!(s.send(&[entry("one"), entry("two")]).await.unwrap(), 0);

        let req = &server.received_requests().await.unwrap()[0];
        let v: Value = serde_json::from_slice(&req.body).unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["message"], "one");
        assert_eq!(arr[0]["level"], "ERROR");
        assert_eq!(arr[0]["timestamp"], 1577836800000i64);
        assert_eq!(arr[0]["hostname"], "web1");
        assert_eq!(arr[0]["params"]["job"], "x");
        assert_eq!(arr[0]["stacktrace"], "#0 main");
        assert_eq!(arr[0]["fields"]["client.address"], "10.0.0.1");
        assert_eq!(arr[1]["message"], "two");
    }

    #[tokio::test]
    async fn explicit_authorization_header_wins_over_api_key() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("Authorization", "Token xyz"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let mut s = PostSender::new(
            &server.uri(),
            &headers(&[("authorization", "Token xyz")]),
            Some("ignored".into()),
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        s.send(&[entry("a")]).await.unwrap();
    }

    #[tokio::test]
    async fn url_credentials_become_basic_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("Authorization", "Basic dXNlcjpwdw=="))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let url = server.uri().replace("http://", "http://user:pw@");
        let mut s = PostSender::new(&url, &[], None, true, Duration::from_secs(5)).unwrap();
        assert!(!s.url().contains("user:pw"));
        s.send(&[entry("a")]).await.unwrap();
    }

    #[tokio::test]
    async fn only_one_authorization_header_is_sent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(header("Authorization", "Basic dXNlcjpwdw=="))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let url = server.uri().replace("http://", "http://user:pw@");
        let mut s = PostSender::new(
            &url,
            &[],
            Some("token".into()),
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        s.send(&[entry("a")]).await.unwrap();
        let req = &server.received_requests().await.unwrap()[0];
        assert_eq!(req.headers.get_all("authorization").iter().count(), 1);
    }

    #[tokio::test]
    async fn status_codes_map_to_retryability() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/500"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/400"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad"))
            .mount(&server)
            .await;

        let mut s = PostSender::new(
            &format!("{}/500", server.uri()),
            &[],
            None,
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        let err = s.send(&[entry("a")]).await.err().unwrap();
        assert!(err.is_retryable());

        let mut s = PostSender::new(
            &format!("{}/400", server.uri()),
            &[],
            None,
            true,
            Duration::from_secs(5),
        )
        .unwrap();
        let err = s.send(&[entry("a")]).await.err().unwrap();
        assert!(!err.is_retryable());
        assert!(err.to_string().contains("400") && err.to_string().contains("bad"));
    }

    #[test]
    fn invalid_url_is_config_error() {
        let err = PostSender::new("not a url", &[], None, true, Duration::from_secs(1))
            .err()
            .unwrap();
        assert!(matches!(err, Error::Config(_)));
    }
}
