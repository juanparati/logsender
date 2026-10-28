//! Rotation-safe file tail, port of laralog's "safe file handler".
//!
//! Every poll:
//! 1. (Re)open the file if we have no handle.
//! 2. Detect rotation (inode changed or file unlinked): drain the old handle,
//!    then switch to the new file from offset 0.
//! 3. Detect truncation (size < position): rewind.
//! 4. Read everything new and split into lines.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use super::{Batch, LineSource, split_lines};

pub const DEFAULT_MAX_LINE: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    dev: u64,
    ino: u64,
}

pub struct FileTail {
    path: PathBuf,
    file: Option<File>,
    id: Option<FileId>,
    pos: u64,
    carry: Vec<u8>,
    max_line: usize,
    /// Seek to the end on the first successful open.
    start_at_end: bool,
    first_open: bool,
    read_buf: Vec<u8>,
}

impl FileTail {
    pub fn new(path: impl AsRef<Path>, start_at_end: bool, max_line: usize) -> Self {
        FileTail {
            path: path.as_ref().to_path_buf(),
            file: None,
            id: None,
            pos: 0,
            carry: Vec::new(),
            max_line,
            start_at_end,
            first_open: true,
            read_buf: vec![0; 64 * 1024],
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn open(&mut self) -> io::Result<bool> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e),
        };
        let meta = file.metadata()?;
        self.id = Some(FileId {
            dev: meta.dev(),
            ino: meta.ino(),
        });
        self.pos = if self.first_open && self.start_at_end {
            meta.len()
        } else {
            0
        };
        self.first_open = false;
        self.carry.clear();
        self.file = Some(file);
        Ok(true)
    }

    /// Read from `pos` to EOF on the current handle.
    fn drain(&mut self, out: &mut Vec<String>) -> io::Result<()> {
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        file.seek(SeekFrom::Start(self.pos))?;
        loop {
            let n = file.read(&mut self.read_buf)?;
            if n == 0 {
                break;
            }
            self.pos += n as u64;
            split_lines(&mut self.carry, &self.read_buf[..n], self.max_line, out);
        }
        Ok(())
    }

    fn rotated(&self) -> io::Result<bool> {
        let Some(file) = self.file.as_ref() else {
            return Ok(false);
        };
        let open_meta = file.metadata()?;
        if open_meta.nlink() == 0 {
            return Ok(true);
        }
        match std::fs::metadata(&self.path) {
            Ok(m) => Ok(Some(FileId {
                dev: m.dev(),
                ino: m.ino(),
            }) != self.id),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(e),
        }
    }
}

impl LineSource for FileTail {
    fn poll(&mut self) -> io::Result<Batch> {
        let mut batch = Batch::default();

        if self.file.is_none() && !self.open()? {
            return Ok(batch);
        }

        if self.rotated()? {
            // Finish the old file, then move on. A partial last line in the old
            // file is emitted as-is.
            self.drain(&mut batch.lines)?;
            if !self.carry.is_empty() {
                let rest = std::mem::take(&mut self.carry);
                batch
                    .lines
                    .push(String::from_utf8_lossy(&rest).into_owned());
            }
            self.file = None;
            if !self.open()? {
                return Ok(batch);
            }
        }

        let size = self.file.as_ref().expect("open").metadata()?.len();
        if size < self.pos {
            self.pos = 0;
            self.carry.clear();
        }

        self.drain(&mut batch.lines)?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn lines(t: &mut FileTail) -> Vec<String> {
        t.poll().unwrap().lines
    }

    #[test]
    fn starts_at_end_and_reads_appends() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        std::fs::write(&p, "old1\nold2\n").unwrap();
        let mut t = FileTail::new(&p, true, 1024);
        assert!(lines(&mut t).is_empty());
        let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
        writeln!(f, "new1").unwrap();
        write!(f, "part").unwrap();
        assert_eq!(lines(&mut t), vec!["new1"]);
        writeln!(f, "ial").unwrap();
        assert_eq!(lines(&mut t), vec!["partial"]);
    }

    #[test]
    fn from_start_reads_existing_content() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        std::fs::write(&p, "a\nb\n").unwrap();
        let mut t = FileTail::new(&p, false, 1024);
        assert_eq!(lines(&mut t), vec!["a", "b"]);
    }

    #[test]
    fn missing_file_appears_later() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("later.log");
        let mut t = FileTail::new(&p, true, 1024);
        assert!(lines(&mut t).is_empty());
        std::fs::write(&p, "x\n").unwrap();
        // First open with start_at_end skips existing content.
        assert!(lines(&mut t).is_empty());
        std::fs::OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap()
            .write_all(b"y\n")
            .unwrap();
        assert_eq!(lines(&mut t), vec!["y"]);
    }

    #[test]
    fn truncation_rewinds() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        std::fs::write(&p, "one\ntwo\nthree\n").unwrap();
        let mut t = FileTail::new(&p, false, 1024);
        assert_eq!(lines(&mut t).len(), 3);
        std::fs::write(&p, "a\n").unwrap(); // truncate + rewrite
        assert_eq!(lines(&mut t), vec!["a"]);
    }

    #[test]
    fn rotation_by_rename_drains_old_and_follows_new() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        std::fs::write(&p, "").unwrap();
        let mut t = FileTail::new(&p, true, 1024);
        assert!(lines(&mut t).is_empty());
        let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
        writeln!(f, "before-rotate").unwrap();
        std::fs::rename(&p, dir.path().join("app.log.1")).unwrap();
        writeln!(f, "written-after-rename").unwrap();
        std::fs::write(&p, "fresh\n").unwrap();
        let got = lines(&mut t);
        assert_eq!(got, vec!["before-rotate", "written-after-rename", "fresh"]);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap()
            .write_all(b"more\n")
            .unwrap();
        assert_eq!(lines(&mut t), vec!["more"]);
    }

    #[test]
    fn rotation_by_unlink_and_recreate() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("app.log");
        std::fs::write(&p, "").unwrap();
        let mut t = FileTail::new(&p, true, 1024);
        assert!(lines(&mut t).is_empty());
        std::fs::remove_file(&p).unwrap();
        assert!(lines(&mut t).is_empty());
        std::fs::write(&p, "recreated\n").unwrap();
        assert_eq!(lines(&mut t), vec!["recreated"]);
    }
}
