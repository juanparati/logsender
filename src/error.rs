use thiserror::Error;

/// Library-level error type. The binary wraps these with `anyhow` for context.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{backend} sender failed: {message}")]
    Sender {
        backend: &'static str,
        message: String,
    },

    #[error("{backend} sender failed with HTTP {status}: {body}")]
    Http {
        backend: &'static str,
        status: u16,
        body: String,
    },

    /// Only part of a batch failed. Every entry not listed in `retry`
    /// (indices into the batch) is settled: delivered, or among the
    /// `rejected` ones. Only the `retry` entries must be sent again.
    #[error("{source} ({} entries to retry)", .retry.len())]
    Partial {
        retry: Vec<usize>,
        rejected: usize,
        source: Box<Error>,
    },
}

impl Error {
    pub fn sender(backend: &'static str, err: impl std::fmt::Display) -> Self {
        Error::Sender {
            backend,
            message: err.to_string(),
        }
    }

    /// True when retrying the same request may succeed (network errors, 5xx, 429).
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Http { status, .. } => retryable_status(*status),
            Error::Io(e) => e.kind() != std::io::ErrorKind::BrokenPipe,
            Error::Sender { .. } => true,
            Error::Config(_) => false,
            Error::Partial { source, .. } => source.is_retryable(),
        }
    }
}

/// HTTP statuses worth retrying: timeouts, throttling and server errors.
pub fn retryable_status(status: u16) -> bool {
    status == 429 || status == 408 || status >= 500
}

pub type Result<T> = std::result::Result<T, Error>;
