use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Append-only write-ahead log. Events land in `current.ndjson`; the compactor
/// periodically seals it (rename) and turns sealed files into Parquet.
pub struct Wal {
    dir: PathBuf,
    current: Mutex<File>,
}

impl Wal {
    pub fn new(dir: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let current = open_current(&dir)?;
        Ok(Self {
            dir,
            current: Mutex::new(current),
        })
    }

    /// Append pre-serialized NDJSON lines and fsync once for the batch.
    /// 202 is only returned to clients after this succeeds.
    pub fn append(&self, lines: &[String]) -> anyhow::Result<()> {
        let mut buf = Vec::with_capacity(lines.iter().map(|l| l.len() + 1).sum());
        for line in lines {
            buf.extend_from_slice(line.as_bytes());
            buf.push(b'\n');
        }
        let mut f = self.current.lock().expect("wal lock poisoned");
        f.write_all(&buf)?;
        f.sync_data()?;
        Ok(())
    }

    /// Seal the current file (if non-empty) and return all sealed files,
    /// oldest first.
    pub fn rotate_and_list_sealed(&self) -> anyhow::Result<Vec<PathBuf>> {
        {
            let mut f = self.current.lock().expect("wal lock poisoned");
            if f.metadata()?.len() > 0 {
                let sealed = self.dir.join(format!(
                    "sealed-{}.ndjson",
                    chrono::Utc::now().timestamp_millis()
                ));
                std::fs::rename(current_path(&self.dir), &sealed)?;
                *f = open_current(&self.dir)?;
            }
        }
        let mut sealed: Vec<PathBuf> = std::fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("sealed-") && n.ends_with(".ndjson"))
            })
            .collect();
        sealed.sort();
        Ok(sealed)
    }
}

fn current_path(dir: &Path) -> PathBuf {
    dir.join("current.ndjson")
}

fn open_current(dir: &Path) -> anyhow::Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .open(current_path(dir))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_rotate_cycle() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = Wal::new(tmp.path().to_path_buf()).unwrap();

        // Empty current → nothing sealed.
        assert!(wal.rotate_and_list_sealed().unwrap().is_empty());

        wal.append(&[r#"{"a":1}"#.to_string(), r#"{"a":2}"#.to_string()])
            .unwrap();
        let sealed = wal.rotate_and_list_sealed().unwrap();
        assert_eq!(sealed.len(), 1);
        let content = std::fs::read_to_string(&sealed[0]).unwrap();
        assert_eq!(content, "{\"a\":1}\n{\"a\":2}\n");

        // New writes go to a fresh current file; sealed list accumulates.
        wal.append(&[r#"{"a":3}"#.to_string()]).unwrap();
        assert_eq!(
            std::fs::read_to_string(current_path(tmp.path())).unwrap(),
            "{\"a\":3}\n"
        );
    }
}
