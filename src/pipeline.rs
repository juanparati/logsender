//! Wire reader thread → folder → queue → sender task, with signals,
//! retries and shutdown draining.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::entry::LogEntry;
use crate::folder::Folder;
use crate::queue::{Queue, StatsSnapshot};
use crate::reader::LineSource;
use crate::sender::Sender;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub read_freq: Duration,
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub retries: u32,
    pub shutdown_timeout: Duration,
    /// Print each entry as JSON to stdout.
    pub echo_entries: bool,
    pub ignore_levels: Vec<String>,
    /// Report stats on stderr at this interval (None = only at exit).
    pub stats_interval: Option<Duration>,
}

/// Why the pipeline stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Input reached EOF (stdin) and everything was delivered.
    Eof,
    /// A stop was requested (signal) and the queue drained in time.
    Stopped,
    /// A stop was requested but the queue did not drain before the timeout.
    StoppedWithLoss(u64),
    /// The reader failed with an I/O error.
    ReaderFailed,
}

pub struct Pipeline {
    pub queue: Arc<Queue>,
    pub stop: Arc<AtomicBool>,
    cfg: PipelineConfig,
}

impl Pipeline {
    pub fn new(cfg: PipelineConfig, max_queue: usize) -> Self {
        Pipeline {
            queue: Arc::new(Queue::new(max_queue)),
            stop: Arc::new(AtomicBool::new(false)),
            cfg,
        }
    }

    /// Request a graceful stop from a signal handler.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Spawn the blocking reader thread. It flushes and closes the queue when
    /// the source hits EOF or a stop is requested.
    pub fn spawn_reader(
        &self,
        mut source: Box<dyn LineSource>,
        mut folder: Folder,
    ) -> std::thread::JoinHandle<std::io::Result<()>> {
        let queue = self.queue.clone();
        let stop = self.stop.clone();
        let cfg = self.cfg.clone();

        std::thread::Builder::new()
            .name("logsender-reader".into())
            .spawn(move || {
                let mut consecutive_errors = 0u32;
                loop {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let batch = match source.poll() {
                        Ok(b) => {
                            consecutive_errors = 0;
                            b
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            tracing::error!(error = %e, "reading input failed");
                            if consecutive_errors >= 10 {
                                if let Some(entry) = folder.flush() {
                                    accept(&cfg, &queue, entry);
                                }
                                queue.close();
                                return Err(e);
                            }
                            std::thread::sleep(cfg.read_freq);
                            continue;
                        }
                    };

                    for line in &batch.lines {
                        if let Some(entry) = folder.push_line(line) {
                            accept(&cfg, &queue, entry);
                        }
                    }
                    if let Some(entry) = folder.tick() {
                        accept(&cfg, &queue, entry);
                    }
                    if batch.eof {
                        break;
                    }
                    std::thread::sleep(cfg.read_freq);
                }
                if let Some(entry) = folder.flush() {
                    accept(&cfg, &queue, entry);
                }
                queue.close();
                Ok(())
            })
            .expect("spawn reader thread")
    }

    /// Spawn the async sender task. Finishes when the queue is closed and drained.
    pub fn spawn_sender(&self, mut sender: Box<dyn Sender>) -> JoinHandle<Box<dyn Sender>> {
        let queue = self.queue.clone();
        let cfg = self.cfg.clone();
        let max = cfg.batch_size.clamp(1, sender.max_batch());
        tokio::spawn(async move {
            while let Some(batch) = queue.next_batch(max, cfg.flush_interval).await {
                deliver(&cfg, &queue, sender.as_mut(), &batch).await;
            }
            sender
        })
    }

    /// Run everything to completion. Returns the outcome and final stats.
    pub async fn run(
        &self,
        source: Box<dyn LineSource>,
        folder: Folder,
        sender: Box<dyn Sender>,
    ) -> (Outcome, StatsSnapshot) {
        let reader = self.spawn_reader(source, folder);
        let sender_task = self.spawn_sender(sender);

        let stats_task = self.cfg.stats_interval.map(|every| {
            let queue = self.queue.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(every);
                tick.tick().await;
                loop {
                    tick.tick().await;
                    log_stats(&queue, "stats");
                }
            })
        });

        // Wait for the reader thread without blocking the runtime.
        let reader_result = tokio::task::spawn_blocking(move || reader.join())
            .await
            .expect("join reader")
            .expect("reader thread panicked");

        let stop_requested = self.stop.load(Ordering::SeqCst);

        // The queue is closed now; give the sender time to drain.
        let drained = tokio::time::timeout(self.cfg.shutdown_timeout, sender_task).await;
        let timed_out = drained.is_err();
        let mut sender = match drained {
            Ok(Ok(s)) => Some(s),
            Ok(Err(e)) => {
                tracing::error!(error = %e, "sender task panicked");
                None
            }
            Err(_) => {
                let lost = self.queue.discard_remaining();
                tracing::warn!(lost, "shutdown timeout reached before the queue drained");
                None
            }
        };
        if let Some(s) = sender.as_mut()
            && let Err(e) = s.shutdown().await
        {
            tracing::warn!(error = %e, "sender shutdown reported an error");
        }
        if let Some(t) = stats_task {
            t.abort();
        }
        log_stats(&self.queue, "final");

        let stats = self.queue.stats.snapshot();
        let outcome = if reader_result.is_err() {
            Outcome::ReaderFailed
        } else if !stop_requested {
            Outcome::Eof
        } else if timed_out {
            Outcome::StoppedWithLoss(self.queue.stats.snapshot().dropped_queue)
        } else {
            Outcome::Stopped
        };
        (outcome, stats)
    }
}

fn accept(cfg: &PipelineConfig, queue: &Queue, entry: LogEntry) {
    if cfg.ignore_levels.iter().any(|l| entry.level.matches(l)) {
        return;
    }
    if cfg.echo_entries {
        println!("{}", serde_json::to_string(&entry).expect("serializable"));
    }
    queue.push(entry);
}

async fn deliver(cfg: &PipelineConfig, queue: &Queue, sender: &mut dyn Sender, batch: &[LogEntry]) {
    let mut attempt = 0u32;
    loop {
        match sender.send(batch).await {
            Ok(rejected) => {
                let rejected = rejected.min(batch.len()) as u64;
                queue
                    .stats
                    .sent
                    .fetch_add(batch.len() as u64 - rejected, Ordering::Relaxed);
                queue
                    .stats
                    .dropped_send
                    .fetch_add(rejected, Ordering::Relaxed);
                return;
            }
            Err(e) => {
                queue.stats.send_failures.fetch_add(1, Ordering::Relaxed);
                if !e.is_retryable() || attempt >= cfg.retries {
                    tracing::error!(
                        backend = sender.name(),
                        entries = batch.len(),
                        attempts = attempt + 1,
                        error = %e,
                        "giving up on batch"
                    );
                    queue
                        .stats
                        .dropped_send
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    return;
                }
                let backoff = Duration::from_millis(250 * 2u64.pow(attempt.min(6)));
                tracing::warn!(backend = sender.name(), attempt = attempt + 1, error = %e, "send failed, retrying in {:?}", backoff);
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
        }
    }
}

fn log_stats(queue: &Queue, label: &str) {
    let s = queue.stats.snapshot();
    tracing::info!(
        received = s.received,
        sent = s.sent,
        dropped = s.dropped(),
        failures = s.send_failures,
        queued = queue.len(),
        "{label}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Parser, SourceTz};
    use crate::reader::Batch;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct VecSource {
        batches: Vec<Vec<String>>,
    }
    impl LineSource for VecSource {
        fn poll(&mut self) -> std::io::Result<Batch> {
            if self.batches.is_empty() {
                return Ok(Batch {
                    lines: vec![],
                    eof: true,
                });
            }
            Ok(Batch {
                lines: self.batches.remove(0),
                eof: self.batches.is_empty(),
            })
        }
    }

    #[derive(Clone, Default)]
    struct MemSender {
        got: Arc<Mutex<Vec<LogEntry>>>,
        fail_first: Arc<Mutex<u32>>,
    }
    #[async_trait]
    impl Sender for MemSender {
        fn name(&self) -> &'static str {
            "mem"
        }
        fn max_batch(&self) -> usize {
            2
        }
        async fn send(&mut self, entries: &[LogEntry]) -> crate::error::Result<usize> {
            let mut f = self.fail_first.lock().unwrap();
            if *f > 0 {
                *f -= 1;
                return Err(crate::error::Error::sender("mem", "flaky"));
            }
            self.got.lock().unwrap().extend_from_slice(entries);
            Ok(0)
        }
    }

    fn cfg() -> PipelineConfig {
        PipelineConfig {
            read_freq: Duration::from_millis(5),
            batch_size: 10,
            flush_interval: Duration::from_millis(20),
            retries: 2,
            shutdown_timeout: Duration::from_secs(2),
            echo_entries: false,
            ignore_levels: vec!["debug".into()],
            stats_interval: None,
        }
    }

    #[tokio::test]
    async fn end_to_end_with_folding_ignore_and_retry() {
        let lines = vec![
            "[2016-04-16 17:25:45] local.INFO: one".to_string(),
            "[2016-04-16 17:25:46] local.DEBUG: skipped".to_string(),
            "[2016-04-16 17:25:47] local.ERROR: two".to_string(),
            "Stack trace:".to_string(),
            "#0 main".to_string(),
        ];
        let source = VecSource {
            batches: vec![lines],
        };
        let folder = Folder::new(Parser::laravel(SourceTz::Named(chrono_tz::UTC), false, "h"));
        let sender = MemSender::default();
        *sender.fail_first.lock().unwrap() = 1;
        let got = sender.got.clone();

        let p = Pipeline::new(cfg(), 100);
        let (outcome, stats) = p.run(Box::new(source), folder, Box::new(sender)).await;

        assert_eq!(outcome, Outcome::Eof);
        let got = got.lock().unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].message, "one");
        assert_eq!(got[1].stacktrace.as_deref(), Some("Stack trace:\n#0 main"));
        assert_eq!(stats.sent, 2);
        assert_eq!(stats.received, 2);
        assert_eq!(stats.send_failures, 1);
        assert_eq!(stats.dropped(), 0);
    }

    #[tokio::test]
    async fn gives_up_after_retries() {
        let source = VecSource {
            batches: vec![vec!["[2016-04-16 17:25:45] local.INFO: one".to_string()]],
        };
        let folder = Folder::new(Parser::laravel(SourceTz::Named(chrono_tz::UTC), false, "h"));
        let sender = MemSender::default();
        *sender.fail_first.lock().unwrap() = 10;
        let mut c = cfg();
        c.retries = 1;
        let p = Pipeline::new(c, 100);
        let (_, stats) = p.run(Box::new(source), folder, Box::new(sender)).await;
        assert_eq!(stats.dropped_send, 1);
        assert_eq!(stats.send_failures, 2);
    }
}
