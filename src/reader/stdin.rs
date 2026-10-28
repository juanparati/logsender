use std::io::{self, BufRead, BufReader, Read, Stdin};

use super::{Batch, LineSource};

/// Reads stdin until EOF. Blocks inside `poll`, which is fine on the
/// dedicated reader thread.
pub struct StdinReader {
    reader: BufReader<Stdin>,
    max_line: usize,
    done: bool,
}

impl StdinReader {
    pub fn new(max_line: usize) -> Self {
        StdinReader {
            reader: BufReader::new(io::stdin()),
            max_line,
            done: false,
        }
    }
}

impl LineSource for StdinReader {
    fn poll(&mut self) -> io::Result<Batch> {
        if self.done {
            return Ok(Batch {
                lines: vec![],
                eof: true,
            });
        }
        let mut batch = Batch::default();
        // Read what is available, up to a bounded number of lines per poll so
        // the folder's tick still runs on large pipes.
        for _ in 0..4096 {
            let mut buf = Vec::new();
            let n = (&mut self.reader)
                .take(self.max_line as u64 + 1)
                .read_until(b'\n', &mut buf)?;
            if n == 0 {
                self.done = true;
                batch.eof = true;
                break;
            }
            if buf.last() == Some(&b'\n') {
                buf.pop();
            }
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            buf.truncate(self.max_line);
            batch.lines.push(String::from_utf8_lossy(&buf).into_owned());
        }
        Ok(batch)
    }
}
