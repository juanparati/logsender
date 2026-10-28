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
            Error::Http { status, .. } => *status == 429 || *status == 408 || *status >= 500,
            Error::Io(e) => e.kind() != std::io::ErrorKind::BrokenPipe,
            Error::Sender { .. } => true,
            Error::Config(_) => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
