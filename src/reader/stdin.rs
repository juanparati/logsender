use std::io::{self, BufRead, BufReader, Read};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};

use super::{Batch, LineSource};

/// Lines handed over per poll, so the folder's tick still runs on large pipes.
const MAX_LINES_PER_POLL: usize = 4096;

/// Reads stdin until EOF. The blocking reads happen on a helper thread, so
/// `poll` returns at once with whatever lines are ready: a quiet pipe
/// neither holds entries back nor delays a stop request.
pub struct StdinReader {
    rx: Receiver<io::Result<String>>,
    /// A read failed: reported on every poll from then on.
    failed: Option<(io::ErrorKind, String)>,
    done: bool,
}

impl StdinReader {
    pub fn new(max_line: usize) -> Self {
        Self::from_reader(io::stdin(), max_line)
    }

    /// Read lines from any blocking reader (stdin in production).
    pub fn from_reader(reader: impl Read + Send + 'static, max_line: usize) -> Self {
        // Bounded, so a blocked queue still slows down the writer into the pipe.
        let (tx, rx) = sync_channel(MAX_LINES_PER_POLL);
        std::thread::Builder::new()
            .name("logsender-stdin".into())
            .spawn(move || read_lines(BufReader::new(reader), max_line, tx))
            .expect("spawn stdin thread");
        StdinReader {
            rx,
            failed: None,
            done: false,
        }
    }
}

/// Runs on the helper thread until EOF, a read error, or the reader is dropped.
fn read_lines(mut reader: impl BufRead, max_line: usize, tx: SyncSender<io::Result<String>>) {
    loop {
        let mut buf = Vec::new();
        let line = match (&mut reader)
            .take(max_line as u64 + 1)
            .read_until(b'\n', &mut buf)
        {
            Ok(0) => return,
            Ok(_) => {
                if buf.last() == Some(&b'\n') {
                    buf.pop();
                }
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
                buf.truncate(max_line);
                Ok(String::from_utf8_lossy(&buf).into_owned())
            }
            Err(e) => Err(e),
        };
        let failed = line.is_err();
        if tx.send(line).is_err() || failed {
            return;
        }
    }
}

impl LineSource for StdinReader {
    fn poll(&mut self) -> io::Result<Batch> {
        if let Some((kind, msg)) = &self.failed {
            return Err(io::Error::new(*kind, msg.clone()));
        }
        let mut batch = Batch::default();
        if self.done {
            batch.eof = true;
            return Ok(batch);
        }
        while batch.lines.len() < MAX_LINES_PER_POLL {
            match self.rx.try_recv() {
                Ok(Ok(line)) => batch.lines.push(line),
                Ok(Err(e)) => {
                    self.failed = Some((e.kind(), e.to_string()));
                    // Hand over the lines read before the failure first.
                    if batch.lines.is_empty() {
                        return Err(e);
                    }
                    return Ok(batch);
                }
                Err(TryRecvError::Empty) => return Ok(batch),
                Err(TryRecvError::Disconnected) => {
                    self.done = true;
                    batch.eof = true;
                    return Ok(batch);
                }
            }
        }
        batch.more = true;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::{Duration, Instant};

    /// Poll until `done` accepts the lines gathered so far, or give up.
    fn poll_until(
        r: &mut StdinReader,
        done: impl Fn(&[String], bool) -> bool,
    ) -> (Vec<String>, bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut lines = Vec::new();
        loop {
            let b = r.poll().unwrap();
            lines.extend(b.lines);
            if done(&lines, b.eof) || Instant::now() > deadline {
                return (lines, b.eof);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn lines_arrive_while_the_pipe_stays_open() {
        let (rx, mut tx) = io::pipe().unwrap();
        let mut r = StdinReader::from_reader(rx, 1024);
        tx.write_all(b"one\r\ntwo\n").unwrap();
        let (lines, eof) = poll_until(&mut r, |l, _| l.len() == 2);
        assert_eq!(lines, vec!["one", "two"]);
        assert!(!eof);
        // Nothing ready: poll returns at once instead of blocking.
        let start = Instant::now();
        assert!(r.poll().unwrap().lines.is_empty());
        assert!(start.elapsed() < Duration::from_millis(500));
        drop(tx);
        let (_, eof) = poll_until(&mut r, |_, eof| eof);
        assert!(eof);
    }

    #[test]
    fn large_input_is_handed_over_in_bounded_polls() {
        let input = "x\n".repeat(MAX_LINES_PER_POLL + 10);
        let mut r = StdinReader::from_reader(io::Cursor::new(input), 1024);
        // Let the helper thread fill the channel.
        std::thread::sleep(Duration::from_millis(300));
        let b = r.poll().unwrap();
        assert_eq!(b.lines.len(), MAX_LINES_PER_POLL);
        assert!(b.more && !b.eof);
        let (rest, eof) = poll_until(&mut r, |_, eof| eof);
        assert_eq!(rest.len(), 10);
        assert!(eof);
    }

    #[test]
    fn long_lines_are_cut() {
        let mut r = StdinReader::from_reader(io::Cursor::new("abcdefgh\n"), 4);
        let (lines, _) = poll_until(&mut r, |_, eof| eof);
        assert_eq!(lines[0], "abcd");
    }
}
