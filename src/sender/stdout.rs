//! Print each entry as one JSON line (JSON Lines / ndjson) so the output can
//! be piped into `jq` or saved as-is. The `--index` value is included in every
//! object as the `index` field.

use async_trait::async_trait;
use serde_json::Value;
use std::io::Write;

use super::Sender;
use crate::entry::{LogEntry, TimestampRepr};
use crate::error::Result;

pub struct StdoutSender {
    index: String,
}

impl StdoutSender {
    pub fn new(index: impl Into<String>) -> Self {
        StdoutSender {
            index: index.into(),
        }
    }
}

#[async_trait]
impl Sender for StdoutSender {
    fn name(&self) -> &'static str {
        "stdout"
    }

    async fn send(&mut self, entries: &[LogEntry]) -> Result<usize> {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        for e in entries {
            let mut doc = e.document(TimestampRepr::EpochMillis, None);
            doc.insert("index".into(), Value::String(self.index.clone()));
            writeln!(
                out,
                "{}",
                serde_json::to_string(&doc).expect("serializable")
            )?;
        }
        out.flush()?;
        Ok(0)
    }
}
