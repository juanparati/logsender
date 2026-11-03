//! Wire reader thread → folder → queue → sender task, with signals,
//! retries and shutdown draining.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::entry::LogEntry;
use crate::error::Error;
use crate::folder::Folder;
use crate::queue::{OnFull, Queue, StatsSnapshot};
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
    /// Entries taken off the queue whose delivery is still unsettled.
    in_flight: Arc<AtomicU64>,
    cfg: PipelineConfig,
}

impl Pipeline {
    pub fn new(cfg: PipelineConfig, max_queue: usize, on_full: OnFull) -> Self {
        Pipeline {
            queue: Arc::new(Queue::new(max_queue, on_full)),
            stop: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicU64::new(0)),
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
                                    accept(&cfg, &queue, &stop, entry);
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
                            accept(&cfg, &queue, &stop, entry);
                        }
                    }
                    if batch.eof {
                        break;
                    }
                    // More input is ready: no time has passed, so neither
                    // tick the folder nor wait.
                    if batch.more {
                        continue;
                    }
                    if let Some(entry) = folder.tick() {
                        accept(&cfg, &queue, &stop, entry);
                    }
                    std::thread::sleep(cfg.read_freq);
                }
                if let Some(entry) = folder.flush() {
                    accept(&cfg, &queue, &stop, entry);
                }
                queue.close();
                Ok(())
            })
            .expect("spawn reader thread")
    }

    /// Spawn the async sender task. Finishes when the queue is closed and drained.
    pub fn spawn_sender(&self, mut sender: Box<dyn Sender>) -> JoinHandle<Box<dyn Sender>> {
        let queue = self.queue.clone();
        let in_flight = self.in_flight.clone();
        let cfg = self.cfg.clone();
        let max = cfg.batch_size.clamp(1, sender.max_batch());
        tokio::spawn(async move {
            while let Some(batch) = queue.next_batch(max, cfg.flush_interval).await {
                deliver(&cfg, &queue, &in_flight, sender.as_mut(), batch).await;
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
        let mut sender_task = self.spawn_sender(sender);

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

        // The queue is closed now. After EOF the sender drains for as long
        // as it needs; once a stop is requested it gets `shutdown_timeout`.
        let drained = if self.stop.load(Ordering::SeqCst) {
            tokio::time::timeout(self.cfg.shutdown_timeout, &mut sender_task).await
        } else {
            tokio::select! {
                r = &mut sender_task => Ok(r),
                _ = wait_for(&self.stop) => {
                    tokio::time::timeout(self.cfg.shutdown_timeout, &mut sender_task).await
                }
            }
        };
        let stop_requested = self.stop.load(Ordering::SeqCst);
        let timed_out = drained.is_err();
        let mut sender = match drained {
            Ok(Ok(s)) => Some(s),
            Ok(Err(e)) => {
                tracing::error!(error = %e, "sender task panicked");
                None
            }
            Err(_) => {
                // Wait for the abort to land so the in-flight count is final.
                sender_task.abort();
                let _ = (&mut sender_task).await;
                let in_flight = self.in_flight.swap(0, Ordering::SeqCst);
                self.queue
                    .stats
                    .dropped_shutdown
                    .fetch_add(in_flight, Ordering::Relaxed);
                let lost = in_flight + self.queue.discard_remaining();
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
            Outcome::StoppedWithLoss(self.queue.stats.snapshot().dropped_shutdown)
        } else {
            Outcome::Stopped
        };
        (outcome, stats)
    }
}

fn accept(cfg: &PipelineConfig, queue: &Queue, stop: &AtomicBool, entry: LogEntry) {
    if cfg.ignore_levels.iter().any(|l| entry.level.matches(l)) {
        return;
    }
    if cfg.echo_entries {
        println!("{}", serde_json::to_string(&entry).expect("serializable"));
    }
    queue.push(entry, stop);
}

async fn deliver(
    cfg: &PipelineConfig,
    queue: &Queue,
    in_flight: &AtomicU64,
    sender: &mut dyn Sender,
    mut batch: Vec<LogEntry>,
) {
    let stats = &queue.stats;
    let mut attempt = 0u32;
    loop {
        in_flight.store(batch.len() as u64, Ordering::SeqCst);
        let e = match sender.send(&batch).await {
            Ok(rejected) => {
                let rejected = rejected.min(batch.len()) as u64;
                stats
                    .sent
                    .fetch_add(batch.len() as u64 - rejected, Ordering::Relaxed);
                stats.dropped_send.fetch_add(rejected, Ordering::Relaxed);
                in_flight.store(0, Ordering::SeqCst);
                return;
            }
            Err(Error::Partial {
                retry,
                rejected,
                source,
            }) => {
                // Settle what went through; keep only the entries to retry.
                let mut keep = vec![false; batch.len()];
                for i in retry {
                    if let Some(k) = keep.get_mut(i) {
                        *k = true;
                    }
                }
                let settled = keep.iter().filter(|k| !**k).count();
                let rejected = rejected.min(settled) as u64;
                stats
                    .sent
                    .fetch_add(settled as u64 - rejected, Ordering::Relaxed);
                stats.dropped_send.fetch_add(rejected, Ordering::Relaxed);
                let mut keep = keep.into_iter();
                batch.retain(|_| keep.next().unwrap_or(false));
                *source
            }
            Err(e) => e,
        };
        stats.send_failures.fetch_add(1, Ordering::Relaxed);
        if batch.is_empty() {
            in_flight.store(0, Ordering::SeqCst);
            return;
        }
        if !e.is_retryable() || attempt >= cfg.retries {
            tracing::error!(
                backend = sender.name(),
                entries = batch.len(),
                attempts = attempt + 1,
                error = %e,
                "giving up on batch"
            );
            stats
                .dropped_send
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            in_flight.store(0, Ordering::SeqCst);
            return;
        }
        let backoff = Duration::from_millis(250 * 2u64.pow(attempt.min(6)));
        tracing::warn!(backend = sender.name(), attempt = attempt + 1, error = %e, "send failed, retrying in {:?}", backoff);
        tokio::time::sleep(backoff).await;
        attempt += 1;
    }
}

/// Resolve once `flag` is set.
async fn wait_for(flag: &AtomicBool) {
    while !flag.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(100)).await;
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
                    eof: true,
                    ..Batch::default()
                });
            }
            Ok(Batch {
                lines: self.batches.remove(0),
                eof: self.batches.is_empty(),
                more: false,
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

        let p = Pipeline::new(cfg(), 100, OnFull::Block);
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
        let p = Pipeline::new(c, 100, OnFull::Block);
        let (_, stats) = p.run(Box::new(source), folder, Box::new(sender)).await;
        assert_eq!(stats.dropped_send, 1);
        assert_eq!(stats.send_failures, 2);
    }

    /// Delivers everything but the first entry of a batch on the first
    /// call, then accepts whatever it gets. Sleeps `delay` per call.
    #[derive(Clone, Default)]
    struct PartialSender {
        got: Arc<Mutex<Vec<String>>>,
        calls: Arc<Mutex<u32>>,
        delay: Duration,
    }
    #[async_trait]
    impl Sender for PartialSender {
        fn name(&self) -> &'static str {
            "partial"
        }
        async fn send(&mut self, entries: &[LogEntry]) -> crate::error::Result<usize> {
            tokio::time::sleep(self.delay).await;
            let first = {
                let mut c = self.calls.lock().unwrap();
                *c += 1;
                *c == 1
            };
            let mut got = self.got.lock().unwrap();
            if first && entries.len() > 1 {
                got.extend(entries[1..].iter().map(|e| e.message.clone()));
                return Err(Error::Partial {
                    retry: vec![0],
                    rejected: 0,
                    source: Box::new(Error::sender("partial", "throttled")),
                });
            }
            got.extend(entries.iter().map(|e| e.message.clone()));
            Ok(0)
        }
    }

    fn lines(n: usize) -> Vec<String> {
        (0..n)
            .map(|i| format!("[2016-04-16 17:25:{:02}] local.INFO: m{i}", i % 60))
            .collect()
    }

    fn laravel_folder() -> Folder {
        Folder::new(Parser::laravel(SourceTz::Named(chrono_tz::UTC), false, "h"))
    }

    #[tokio::test]
    async fn partial_delivery_retries_only_the_unsettled_entries() {
        let source = VecSource {
            batches: vec![lines(3)],
        };
        let sender = PartialSender::default();
        let got = sender.got.clone();
        let p = Pipeline::new(cfg(), 100, OnFull::Block);
        let (outcome, stats) = p
            .run(Box::new(source), laravel_folder(), Box::new(sender))
            .await;
        assert_eq!(outcome, Outcome::Eof);
        assert_eq!(*got.lock().unwrap(), vec!["m1", "m2", "m0"]);
        assert_eq!(stats.sent, 3);
        assert_eq!(stats.send_failures, 1);
        assert_eq!(stats.dropped(), 0);
    }

    #[tokio::test]
    async fn eof_drains_past_the_shutdown_timeout() {
        let source = VecSource {
            batches: vec![lines(4)],
        };
        let sender = PartialSender {
            delay: Duration::from_millis(100),
            ..Default::default()
        };
        // Retry everything at once so the partial path is not involved.
        *sender.calls.lock().unwrap() = 1;
        let got = sender.got.clone();
        let mut c = cfg();
        c.batch_size = 1;
        c.shutdown_timeout = Duration::from_millis(50);
        let p = Pipeline::new(c, 100, OnFull::Block);
        let (outcome, stats) = p
            .run(Box::new(source), laravel_folder(), Box::new(sender))
            .await;
        assert_eq!(outcome, Outcome::Eof);
        assert_eq!(got.lock().unwrap().len(), 4);
        assert_eq!(stats.dropped(), 0);
    }

    #[tokio::test]
    async fn shutdown_timeout_counts_the_in_flight_batch() {
        let source = VecSource {
            batches: vec![lines(3)],
        };
        let sender = PartialSender {
            delay: Duration::from_secs(30),
            ..Default::default()
        };
        let mut c = cfg();
        c.shutdown_timeout = Duration::from_millis(50);
        let p = Pipeline::new(c, 100, OnFull::Block);
        let stop = p.stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            stop.store(true, Ordering::SeqCst);
        });
        let (outcome, stats) = p
            .run(Box::new(source), laravel_folder(), Box::new(sender))
            .await;
        assert_eq!(outcome, Outcome::StoppedWithLoss(3));
        assert_eq!(stats.dropped_shutdown, 3);
        assert_eq!(stats.sent, 0);
    }
}
