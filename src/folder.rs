//! Fold physical lines into logical entries.
//!
//! A header line starts an entry; following non-header lines (stack traces)
//! are appended to it. The pending entry is released when the next header
//! arrives, when a read tick passes with no new lines, or on flush.
//! Consecutive identical entries are dropped, as laralog did.

use crate::entry::LogEntry;
use crate::parser::Parser;

pub const DEFAULT_MAX_ENTRY_BYTES: usize = 64 * 1024;
const TRUNCATION_MARKER: &str = "\n… [truncated]";

#[derive(Debug)]
pub struct Folder {
    parser: Parser,
    pending: Option<LogEntry>,
    /// Set when the pending entry received a line since the last tick.
    touched: bool,
    truncated: bool,
    last_emitted: Option<LogEntry>,
    max_bytes: usize,
    dedup: bool,
}

impl Folder {
    pub fn new(parser: Parser) -> Self {
        Folder {
            parser,
            pending: None,
            touched: false,
            truncated: false,
            last_emitted: None,
            max_bytes: DEFAULT_MAX_ENTRY_BYTES,
            dedup: true,
        }
    }

    pub fn with_max_bytes(mut self, max: usize) -> Self {
        self.max_bytes = max;
        self
    }

    pub fn with_dedup(mut self, dedup: bool) -> Self {
        self.dedup = dedup;
        self
    }

    /// Feed one physical line. Returns an entry that became complete.
    pub fn push_line(&mut self, line: &str) -> Option<LogEntry> {
        let line = line.trim_end_matches(['\r', '\n']);
        if self.parser.is_header(line) {
            let completed = self.take_pending();
            match self.parser.parse(line) {
                Some(entry) => {
                    self.pending = Some(entry);
                    self.touched = true;
                    self.truncated = false;
                }
                None => {
                    // Header-shaped but unparsable (bad date): treat as noise.
                }
            }
            return completed.and_then(|e| self.emit(e));
        }

        if !self.parser.accepts_continuation(line) {
            return None;
        }
        if let Some(pending) = self.pending.as_mut() {
            self.touched = true;
            if self.truncated {
                return None;
            }
            let budget = self.max_bytes.saturating_sub(pending.byte_len());
            let trace = pending.stacktrace.get_or_insert_with(String::new);
            if !trace.is_empty() {
                trace.push('\n');
            }
            if line.len() + 1 > budget {
                let cut = floor_char_boundary(line, budget.saturating_sub(1));
                trace.push_str(&line[..cut]);
                trace.push_str(TRUNCATION_MARKER);
                self.truncated = true;
            } else {
                trace.push_str(line);
            }
        }
        // Continuation line with nothing pending: dropped (laralog behavior).
        None
    }

    /// Call once per read interval. Releases a pending entry that saw no
    /// lines during the previous interval.
    pub fn tick(&mut self) -> Option<LogEntry> {
        if self.pending.is_some() && !self.touched {
            let e = self.take_pending()?;
            return self.emit(e);
        }
        self.touched = false;
        None
    }

    /// Release whatever is pending (EOF / shutdown).
    pub fn flush(&mut self) -> Option<LogEntry> {
        let e = self.take_pending()?;
        self.emit(e)
    }

    fn take_pending(&mut self) -> Option<LogEntry> {
        self.touched = false;
        self.truncated = false;
        self.pending.take()
    }

    fn emit(&mut self, entry: LogEntry) -> Option<LogEntry> {
        if self.dedup && self.last_emitted.as_ref() == Some(&entry) {
            return None;
        }
        if self.dedup {
            self.last_emitted = Some(entry.clone());
        }
        Some(entry)
    }
}

fn floor_char_boundary(s: &str, idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    let mut i = idx;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SourceTz;

    fn folder() -> Folder {
        Folder::new(Parser::laravel(SourceTz::Named(chrono_tz::UTC), false, "h"))
    }

    fn folder_with(yaml: &str) -> Folder {
        let f = crate::format::LogFormat::from_yaml(yaml, "t", "t").unwrap();
        Folder::new(Parser::new(
            std::sync::Arc::new(f),
            SourceTz::Named(chrono_tz::UTC),
            false,
            "h",
        ))
    }

    const LINE_FMT: &str = "match: ['^%{TIMESTAMP_ISO8601:timestamp} %{GREEDYDATA:message}$']\ntimestamp: {format: '%Y-%m-%d %H:%M:%S'}\n";

    #[test]
    fn multiline_none_drops_continuations() {
        let mut f = folder_with(&format!("{LINE_FMT}multiline: {{mode: none}}\n"));
        f.push_line("2016-04-16 17:25:45 one");
        f.push_line("  more");
        let e = f.flush().unwrap();
        assert_eq!(e.message, "one");
        assert!(e.stacktrace.is_none());
    }

    #[test]
    fn multiline_pattern_appends_only_matching_lines() {
        let mut f = folder_with(&format!(
            "{LINE_FMT}multiline: {{mode: pattern, pattern: '^#%{{NONNEGINT}} '}}\n"
        ));
        f.push_line("2016-04-16 17:25:45 one");
        f.push_line("#0 frame");
        f.push_line("noise");
        f.push_line("#1 frame");
        let e = f.flush().unwrap();
        assert_eq!(e.stacktrace.as_deref(), Some("#0 frame\n#1 frame"));
    }

    const H1: &str = "[2016-04-16 17:25:45] local.INFO: one";
    const H2: &str = "[2016-04-16 17:25:46] local.ERROR: two";

    #[test]
    fn header_releases_previous_entry() {
        let mut f = folder();
        assert!(f.push_line(H1).is_none());
        let e = f.push_line(H2).unwrap();
        assert_eq!(e.message, "one");
        assert!(e.stacktrace.is_none());
        assert_eq!(f.flush().unwrap().message, "two");
        assert!(f.flush().is_none());
    }

    #[test]
    fn continuation_lines_become_stacktrace() {
        let mut f = folder();
        f.push_line(H2);
        f.push_line("Stack trace:");
        f.push_line("#0 foo()");
        f.push_line("#1 {main}");
        let e = f.push_line(H1).unwrap();
        assert_eq!(
            e.stacktrace.as_deref(),
            Some("Stack trace:\n#0 foo()\n#1 {main}")
        );
    }

    #[test]
    fn orphan_continuation_is_dropped() {
        let mut f = folder();
        assert!(f.push_line("Stack trace:").is_none());
        assert!(f.flush().is_none());
    }

    #[test]
    fn tick_releases_idle_entry_after_one_quiet_interval() {
        let mut f = folder();
        f.push_line(H1);
        // Interval in which the line arrived: not yet.
        assert!(f.tick().is_none());
        // Quiet interval: released.
        assert_eq!(f.tick().unwrap().message, "one");
        assert!(f.tick().is_none());
    }

    #[test]
    fn tick_waits_while_lines_keep_coming() {
        let mut f = folder();
        f.push_line(H2);
        assert!(f.tick().is_none());
        f.push_line("#0 still writing");
        assert!(f.tick().is_none());
        let e = f.tick().unwrap();
        assert_eq!(e.stacktrace.as_deref(), Some("#0 still writing"));
    }

    #[test]
    fn dedup_drops_consecutive_identical_entries() {
        let mut f = folder();
        f.push_line(H1);
        assert!(f.push_line(H1).is_some());
        assert!(f.push_line(H1).is_none());
        assert!(f.push_line(H2).is_none()); // H1 dup dropped, H2 pending
        assert_eq!(f.flush().unwrap().message, "two");
    }

    #[test]
    fn dedup_can_be_disabled() {
        let mut f = folder().with_dedup(false);
        f.push_line(H1);
        assert!(f.push_line(H1).is_some());
        assert!(f.push_line(H1).is_some());
    }

    #[test]
    fn oversized_entry_is_truncated_once() {
        let mut f = folder().with_max_bytes(64);
        f.push_line(H2);
        f.push_line(&"x".repeat(40));
        f.push_line(&"y".repeat(40));
        f.push_line(&"z".repeat(40));
        let e = f.flush().unwrap();
        let trace = e.stacktrace.unwrap();
        assert!(trace.ends_with(TRUNCATION_MARKER));
        assert_eq!(trace.matches("[truncated]").count(), 1);
        assert!(!trace.contains('z'));
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        let mut f = folder().with_max_bytes(16);
        f.push_line(H2);
        f.push_line("ñññññññññññññ");
        let e = f.flush().unwrap();
        assert!(e.stacktrace.is_some());
    }
}
