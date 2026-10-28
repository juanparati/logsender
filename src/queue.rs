//! Bounded entry queue between the reader thread and the sender task.
//! When full, the oldest entry is dropped and counted.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until};

use crate::entry::LogEntry;

#[derive(Debug, Default)]
pub struct Stats {
    pub received: AtomicU64,
    pub sent: AtomicU64,
    pub dropped_queue: AtomicU64,
    pub dropped_send: AtomicU64,
    pub send_failures: AtomicU64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            received: self.received.load(Ordering::Relaxed),
            sent: self.sent.load(Ordering::Relaxed),
            dropped_queue: self.dropped_queue.load(Ordering::Relaxed),
            dropped_send: self.dropped_send.load(Ordering::Relaxed),
            send_failures: self.send_failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub received: u64,
    pub sent: u64,
    pub dropped_queue: u64,
    pub dropped_send: u64,
    pub send_failures: u64,
}

impl StatsSnapshot {
    pub fn dropped(&self) -> u64 {
        self.dropped_queue + self.dropped_send
    }
}

#[derive(Debug)]
struct Inner {
    items: VecDeque<LogEntry>,
    closed: bool,
}

#[derive(Debug)]
pub struct Queue {
    inner: Mutex<Inner>,
    notify: Notify,
    capacity: usize,
    pub stats: Stats,
}

impl Queue {
    pub fn new(capacity: usize) -> Self {
        Queue {
            inner: Mutex::new(Inner {
                items: VecDeque::with_capacity(capacity.min(1024)),
                closed: false,
            }),
            notify: Notify::new(),
            capacity: capacity.max(1),
            stats: Stats::default(),
        }
    }

    /// Enqueue an entry, evicting the oldest when the queue is full.
    pub fn push(&self, entry: LogEntry) {
        self.stats.received.fetch_add(1, Ordering::Relaxed);
        {
            let mut inner = self.inner.lock().expect("queue mutex");
            if inner.closed {
                self.stats.dropped_queue.fetch_add(1, Ordering::Relaxed);
                return;
            }
            if inner.items.len() >= self.capacity {
                inner.items.pop_front();
                self.stats.dropped_queue.fetch_add(1, Ordering::Relaxed);
            }
            inner.items.push_back(entry);
        }
        self.notify.notify_one();
    }

    /// No more entries will be pushed. Consumers drain and then stop.
    pub fn close(&self) {
        self.inner.lock().expect("queue mutex").closed = true;
        self.notify.notify_one();
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().expect("queue mutex").closed
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("queue mutex").items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop everything still queued and count it. Used when shutdown times out.
    pub fn discard_remaining(&self) -> u64 {
        let mut inner = self.inner.lock().expect("queue mutex");
        let n = inner.items.len() as u64;
        inner.items.clear();
        self.stats.dropped_queue.fetch_add(n, Ordering::Relaxed);
        n
    }

    fn take(&self, max: usize) -> (Vec<LogEntry>, bool) {
        let mut inner = self.inner.lock().expect("queue mutex");
        let n = inner.items.len().min(max);
        let batch: Vec<LogEntry> = inner.items.drain(..n).collect();
        (batch, inner.closed)
    }

    /// Wait for a batch. Returns up to `max` entries once at least one is
    /// available and either the batch is full or `flush_interval` has passed
    /// since the first entry was seen. Returns `None` once closed and empty.
    pub async fn next_batch(&self, max: usize, flush_interval: Duration) -> Option<Vec<LogEntry>> {
        let max = max.max(1);
        let mut batch: Vec<LogEntry> = Vec::new();
        let mut deadline: Option<Instant> = None;

        loop {
            let (mut taken, closed) = self.take(max - batch.len());
            batch.append(&mut taken);

            if batch.len() >= max {
                return Some(batch);
            }
            if closed {
                return if batch.is_empty() { None } else { Some(batch) };
            }
            if !batch.is_empty() && deadline.is_none() {
                deadline = Some(Instant::now() + flush_interval);
            }

            match deadline {
                None => self.notify.notified().await,
                Some(d) => {
                    tokio::select! {
                        _ = self.notify.notified() => {}
                        _ = sleep_until(d) => return Some(batch),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::level::{Level, LogLevel};
    use chrono::Utc;

    fn entry(msg: &str) -> LogEntry {
        LogEntry {
            timestamp: Utc::now(),
            environment: "t".into(),
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
    fn drops_oldest_when_full() {
        let q = Queue::new(2);
        q.push(entry("a"));
        q.push(entry("b"));
        q.push(entry("c"));
        assert_eq!(q.len(), 2);
        assert_eq!(q.stats.snapshot().dropped_queue, 1);
        let (batch, _) = q.take(10);
        let msgs: Vec<_> = batch.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(msgs, vec!["b", "c"]);
    }

    #[tokio::test]
    async fn full_batch_returns_immediately() {
        let q = Queue::new(10);
        for i in 0..5 {
            q.push(entry(&i.to_string()));
        }
        let b = q.next_batch(3, Duration::from_secs(10)).await.unwrap();
        assert_eq!(b.len(), 3);
    }

    #[tokio::test]
    async fn partial_batch_flushes_after_interval() {
        let q = Queue::new(10);
        q.push(entry("x"));
        let start = Instant::now();
        let b = q.next_batch(100, Duration::from_millis(50)).await.unwrap();
        assert_eq!(b.len(), 1);
        assert!(start.elapsed() >= Duration::from_millis(45));
    }

    #[tokio::test]
    async fn closed_and_empty_returns_none() {
        let q = Queue::new(10);
        q.push(entry("x"));
        q.close();
        assert_eq!(
            q.next_batch(100, Duration::from_secs(1))
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(q.next_batch(100, Duration::from_secs(1)).await.is_none());
    }

    #[tokio::test]
    async fn wakes_up_on_push() {
        let q = std::sync::Arc::new(Queue::new(10));
        let q2 = q.clone();
        let h = tokio::spawn(async move { q2.next_batch(1, Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.push(entry("late"));
        let b = h.await.unwrap().unwrap();
        assert_eq!(b[0].message, "late");
    }
}
