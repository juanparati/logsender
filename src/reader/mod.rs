//! Line sources: a rotation-safe file tail and stdin.

pub mod file;
pub mod stdin;

pub use file::FileTail;
pub use stdin::StdinReader;

/// Result of one poll on a source.
#[derive(Debug, Default)]
pub struct Batch {
    pub lines: Vec<String>,
    /// The source will never produce more lines (stdin closed).
    pub eof: bool,
}

/// Blocking line source polled from the reader thread.
pub trait LineSource: Send {
    fn poll(&mut self) -> std::io::Result<Batch>;
}

/// Split a byte buffer into complete lines, leaving the trailing partial
/// line in `carry`. Lines longer than `max_line` are cut and emitted.
pub(crate) fn split_lines(
    carry: &mut Vec<u8>,
    chunk: &[u8],
    max_line: usize,
    out: &mut Vec<String>,
) {
    carry.extend_from_slice(chunk);
    let mut start = 0;
    while let Some(rel) = carry[start..].iter().position(|&b| b == b'\n') {
        let end = start + rel;
        out.push(to_line(&carry[start..end]));
        start = end + 1;
    }
    carry.drain(..start);
    if carry.len() > max_line {
        let cut: Vec<u8> = carry.drain(..max_line).collect();
        out.push(to_line(&cut));
        carry.clear();
    }
}

fn to_line(bytes: &[u8]) -> String {
    let bytes = bytes.strip_suffix(b"\r").unwrap_or(bytes);
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_and_carries_partial() {
        let mut carry = Vec::new();
        let mut out = Vec::new();
        split_lines(&mut carry, b"a\r\nb\nc", 1024, &mut out);
        assert_eq!(out, vec!["a", "b"]);
        assert_eq!(carry, b"c");
        split_lines(&mut carry, b"d\n", 1024, &mut out);
        assert_eq!(out, vec!["a", "b", "cd"]);
        assert!(carry.is_empty());
    }

    #[test]
    fn long_partial_is_cut() {
        let mut carry = Vec::new();
        let mut out = Vec::new();
        split_lines(&mut carry, &[b'x'; 10], 4, &mut out);
        assert_eq!(out, vec!["xxxx"]);
        assert!(carry.is_empty());
    }
}
