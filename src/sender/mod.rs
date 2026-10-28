//! Delivery back-ends.

pub mod datadog;
pub mod elastic;
pub mod otlp;
pub mod post;
pub mod stdout;

use async_trait::async_trait;

use crate::entry::LogEntry;
use crate::error::{Error, Result};

#[async_trait]
pub trait Sender: Send {
    fn name(&self) -> &'static str;

    /// Largest batch this back-end accepts in one request.
    fn max_batch(&self) -> usize {
        usize::MAX
    }

    /// Deliver a batch. Returns how many entries the back-end rejected
    /// permanently (they are counted as dropped, not retried).
    async fn send(&mut self, entries: &[LogEntry]) -> Result<usize>;

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Install the process-wide rustls crypto provider. Both reqwest and tonic
/// link rustls without a default provider, so this must run before any TLS
/// client is built. Safe to call repeatedly.
pub fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Read the `--to` argument: a URL, or a file with one URL per line.
pub fn resolve_hosts(arg: &str) -> std::io::Result<Vec<String>> {
    let path = std::path::Path::new(arg);
    if path.is_file() {
        let content = std::fs::read_to_string(path)?;
        let hosts: Vec<String> = content
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(String::from)
            .collect();
        return Ok(hosts);
    }
    Ok(vec![arg.trim().to_string()])
}

/// Decoded Basic-auth credentials: user and optional password.
pub type BasicAuth = Option<(String, Option<String>)>;

/// Split `scheme://user:pass@host/path` into the URL without credentials and
/// the decoded Basic-auth pair, if any.
pub(crate) fn split_credentials(raw: &str, backend: &str) -> Result<(String, BasicAuth)> {
    let mut url = url::Url::parse(raw.trim())
        .map_err(|e| Error::Config(format!("invalid {backend} URL '{raw}': {e}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(Error::Config(format!(
            "invalid {backend} URL '{raw}': expected http or https"
        )));
    }
    let basic = if url.username().is_empty() {
        None
    } else {
        let user = percent_decode(url.username());
        let pass = url.password().map(percent_decode);
        url.set_username("").ok();
        url.set_password(None).ok();
        Some((user, pass))
    };
    Ok((url.to_string(), basic))
}

fn percent_decode(s: &str) -> String {
    url::form_urlencoded::parse(s.as_bytes())
        .map(|(k, v)| format!("{k}{v}"))
        .collect::<Vec<_>>()
        .join("")
}

/// Parse a `--header "Name: value"` argument.
pub fn parse_header(raw: &str) -> Result<(String, String)> {
    let bad = || {
        Error::Config(format!(
            "invalid --header '{raw}': expected \"Name: value\""
        ))
    };
    let (name, value) = raw.split_once(':').ok_or_else(bad)?;
    let (name, value) = (name.trim(), value.trim());
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(bad());
    }
    if value.bytes().any(|b| b == b'\r' || b == b'\n') {
        return Err(bad());
    }
    Ok((name.to_string(), value.to_string()))
}

/// A URL safe to log: the password and query values are replaced with `***`.
/// Values that are not URLs (a hosts-file path, `x` for stdout) are returned unchanged.
pub fn redact_url(raw: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw.trim()) else {
        return raw.to_string();
    };
    if url.password().is_some() {
        url.set_password(Some("***")).ok();
    }
    if url.query().is_some() {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, _)| (k.into_owned(), "***".into()))
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    url.to_string()
}

/// Join a base URL and a path without doubling or losing slashes.
pub(crate) fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("hosts.txt");
        std::fs::write(&p, "http://a:9200\n\n# comment\n http://b:9200 \n").unwrap();
        assert_eq!(
            resolve_hosts(p.to_str().unwrap()).unwrap(),
            vec!["http://a:9200", "http://b:9200"]
        );
        assert_eq!(resolve_hosts("http://c").unwrap(), vec!["http://c"]);
    }

    #[test]
    fn credentials_are_split_and_decoded() {
        let (url, basic) = split_credentials("https://elastic:p%40ss@es.lan:9200/", "x").unwrap();
        assert_eq!(url, "https://es.lan:9200/");
        assert_eq!(basic, Some(("elastic".into(), Some("p@ss".into()))));
        let (url, basic) = split_credentials("http://es:9200", "x").unwrap();
        assert_eq!(url, "http://es:9200/");
        assert!(basic.is_none());
        assert!(split_credentials("ftp://x", "x").is_err());
        assert!(split_credentials("nope", "x").is_err());
    }

    #[test]
    fn redact_url_hides_password_and_query_values() {
        assert_eq!(
            redact_url("https://elastic:pw@es:9200/"),
            "https://elastic:***@es:9200/"
        );
        assert_eq!(
            redact_url("https://h/hook?token=abc&x=1"),
            "https://h/hook?token=***&x=***"
        );
        assert_eq!(
            redact_url("http://localhost:4317"),
            "http://localhost:4317/"
        );
        assert_eq!(redact_url("x"), "x");
    }

    #[test]
    fn headers_parse() {
        assert_eq!(
            parse_header("X-Token: abc").unwrap(),
            ("X-Token".into(), "abc".into())
        );
        assert_eq!(
            parse_header("Authorization:Bearer a:b").unwrap(),
            ("Authorization".into(), "Bearer a:b".into())
        );
        assert!(parse_header("no colon").is_err());
        assert!(parse_header(": v").is_err());
        assert!(parse_header("Bad Name: v").is_err());
        assert!(parse_header("X: a\nb").is_err());
    }

    #[test]
    fn join() {
        assert_eq!(join_url("http://x/", "/_bulk"), "http://x/_bulk");
        assert_eq!(join_url("http://x", "_bulk"), "http://x/_bulk");
    }
}
