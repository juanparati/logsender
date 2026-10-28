//! LogSender: tail Laravel log files and ship entries to OTLP, Datadog or Elasticsearch.
//!
//! The crate is split so the parser, readers and senders can be unit tested
//! without going through the command-line layer in `main.rs`.

pub mod cli;
pub mod entry;
pub mod error;
pub mod folder;
pub mod format;
pub mod level;
pub mod parser;
pub mod pipeline;
pub mod queue;
pub mod reader;
pub mod sender;

pub use entry::LogEntry;
pub use error::Error;
pub use level::Level;
