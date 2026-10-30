//! Bounded entry queue between the reader thread and the sender task.
//! When full, the producer either waits for room (`OnFull::Block`) or the
//! oldest entry is dropped and counted (`OnFull::Drop`).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until};

pub use crate::cli::OnFull;
use crate::entry::LogEntry;

/// How often a blocked producer re-checks the stop flag.
const STOP_POLL: Duration = Duration::from_millis(100);

#[derive(Debug, Default)]
pub struct Stats {
    pub received: AtomicU64,
    pub sent: AtomicU64,
    pub dropped_queue: AtomicU64,
    pub dropped_send: AtomicU64,
    pub dropped_shutdown: AtomicU64,
    pub send_failures: AtomicU64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            received: self.received.load(Ordering::Relaxed),
            sent: self.sent.load(Ordering::Relaxed),
            dropped_queue: self.dropped_queue.load(Ordering::Relaxed),
            dropped_send: self.dropped_send.load(Ordering::Relaxed),
            dropped_shutdown: self.dropped_shutdown.load(Ordering::Relaxed),
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
    pub dropped_shutdown: u64,
    pub send_failures: u64,
}

impl StatsSnapshot {
    pub fn dropped(&self) -> u64 {
        self.dropped_queue + self.dropped_send + self.dropped_shutdown
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
    /// Signalled when the consumer takes entries, freeing room.
    space: Condvar,
    capacity: usize,
    on_full: OnFull,
    pub stats: Stats,
}

impl Queue {
    pub fn new(capacity: usize, on_full: OnFull) -> Self {
        Queue {
            inner: Mutex::new(Inner {
                items: VecDeque::with_capacity(capacity.min(1024)),
                closed: false,
            }),
            notify: Notify::new(),
            space: Condvar::new(),
            capacity: capacity.max(1),
            on_full,
            stats: Stats::default(),
        }
    }

    /// Enqueue an entry. When the queue is full, `OnFull::Block` waits for
    /// room; once `stop` is set it stops waiting and falls back to evicting
    /// the oldest entry so shutdown is never held up by a stalled sender.
    /// Blocks the calling thread: call it from the reader thread, not async code.
    pub fn push(&self, entry: LogEntry, stop: &AtomicBool) {
        self.stats.received.fetch_add(1, Ordering::Relaxed);
        {
            let mut inner = self.inner.lock().expect("queue mutex");
            if self.on_full == OnFull::Block {
                while inner.items.len() >= self.capacity
                    && !inner.closed
                    && !stop.load(Ordering::SeqCst)
                {
                    inner = self
                        .space
                        .wait_timeout(inner, STOP_POLL)
                        .expect("queue mutex")
                        .0;
                }
            }
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
        self.space.notify_all();
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
        self.stats.dropped_shutdown.fetch_add(n, Ordering::Relaxed);
        self.space.notify_all();
        n
    }

    fn take(&self, max: usize) -> (Vec<LogEntry>, bool) {
        let mut inner = self.inner.lock().expect("queue mutex");
        let n = inner.items.len().min(max);
        let batch: Vec<LogEntry> = inner.items.drain(..n).collect();
        if n > 0 {
            self.space.notify_all();
        }
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
    use std::sync::Arc;

    static RUNNING: AtomicBool = AtomicBool::new(false);

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
        let q = Queue::new(2, OnFull::Drop);
        q.push(entry("a"), &RUNNING);
        q.push(entry("b"), &RUNNING);
        q.push(entry("c"), &RUNNING);
        assert_eq!(q.len(), 2);
        assert_eq!(q.stats.snapshot().dropped_queue, 1);
        let (batch, _) = q.take(10);
        let msgs: Vec<_> = batch.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(msgs, vec!["b", "c"]);
    }

    #[tokio::test]
    async fn full_batch_returns_immediately() {
        let q = Queue::new(10, OnFull::Drop);
        for i in 0..5 {
            q.push(entry(&i.to_string()), &RUNNING);
        }
        let b = q.next_batch(3, Duration::from_secs(10)).await.unwrap();
        assert_eq!(b.len(), 3);
    }

    #[tokio::test]
    async fn partial_batch_flushes_after_interval() {
        let q = Queue::new(10, OnFull::Drop);
        q.push(entry("x"), &RUNNING);
        let start = Instant::now();
        let b = q.next_batch(100, Duration::from_millis(50)).await.unwrap();
        assert_eq!(b.len(), 1);
        assert!(start.elapsed() >= Duration::from_millis(45));
    }

    #[tokio::test]
    async fn closed_and_empty_returns_none() {
        let q = Queue::new(10, OnFull::Drop);
        q.push(entry("x"), &RUNNING);
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
        let q = std::sync::Arc::new(Queue::new(10, OnFull::Drop));
        let q2 = q.clone();
        let h = tokio::spawn(async move { q2.next_batch(1, Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        q.push(entry("late"), &RUNNING);
        let b = h.await.unwrap().unwrap();
        assert_eq!(b[0].message, "late");
    }

    #[test]
    fn block_waits_for_room_and_loses_nothing() {
        let q = Arc::new(Queue::new(2, OnFull::Block));
        let q2 = q.clone();
        let producer = std::thread::spawn(move || {
            for i in 0..50 {
                q2.push(entry(&i.to_string()), &RUNNING);
            }
            q2.close();
        });
        let mut got = Vec::new();
        loop {
            let (batch, closed) = q.take(3);
            assert!(q.len() <= 2);
            got.extend(batch.into_iter().map(|e| e.message));
            if closed && q.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        producer.join().unwrap();
        let want: Vec<String> = (0..50).map(|i| i.to_string()).collect();
        assert_eq!(got, want);
        assert_eq!(q.stats.snapshot().dropped(), 0);
    }

    #[test]
    fn block_falls_back_to_drop_once_stopped() {
        let q = Arc::new(Queue::new(1, OnFull::Block));
        let stop = Arc::new(AtomicBool::new(false));
        q.push(entry("a"), &stop);
        let (q2, stop2) = (q.clone(), stop.clone());
        let producer = std::thread::spawn(move || q2.push(entry("b"), &stop2));
        std::thread::sleep(Duration::from_millis(50));
        assert!(!producer.is_finished(), "push should block while full");
        stop.store(true, Ordering::SeqCst);
        producer.join().unwrap();
        let (batch, _) = q.take(10);
        assert_eq!(batch[0].message, "b");
        assert_eq!(q.stats.snapshot().dropped_queue, 1);
    }

    #[test]
    fn shutdown_discard_is_counted_separately() {
        let q = Queue::new(10, OnFull::Block);
        q.push(entry("a"), &RUNNING);
        assert_eq!(q.discard_remaining(), 1);
        let s = q.stats.snapshot();
        assert_eq!((s.dropped_queue, s.dropped_shutdown), (0, 1));
    }
}
