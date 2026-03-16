//! Translog (Write-Ahead Log)
//!
//! Provides crash durability for indexed documents.
//! Every index operation is appended here BEFORE being written to the Tantivy buffer.
//! On crash + restart, uncommitted entries are replayed into the engine.
//! After a successful flush (Tantivy commit), the translog is truncated.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

/// A single translog entry, representing one indexing operation.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TranslogEntry {
    /// Monotonically increasing sequence number for ordering
    pub seq_no: u64,
    /// The operation type (currently only "index")
    pub op: String,
    /// The full document payload
    pub payload: serde_json::Value,
}

/// Append-only Write-Ahead Log for durability.
///
/// Stored as newline-delimited JSON (JSONL) at `<data_dir>/translog.jsonl`.
/// Each line is a serialized `TranslogEntry`. The file is fsynced on every write
/// (request-level durability) to guarantee no ops are lost on crash.
pub struct Translog {
    file: Mutex<File>,
    seq_no: Mutex<u64>,
}

impl Translog {
    /// Open or create the translog at the given directory.
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let path = data_dir.as_ref().join("translog.jsonl");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        // Figure out the highest sequence number already on disk
        let existing = Self::read_entries_from_path(&path)?;
        let next_seq = existing.last().map(|e| e.seq_no + 1).unwrap_or(0);

        tracing::info!(
            "Translog opened at {:?} ({} pending entries, next seq_no: {})",
            path,
            existing.len(),
            next_seq
        );

        Ok(Self {
            file: Mutex::new(file),
            seq_no: Mutex::new(next_seq),
        })
    }

    /// Append an operation to the translog and fsync for durability.
    pub fn append(&self, op: &str, payload: serde_json::Value) -> Result<TranslogEntry> {
        let mut seq = self.seq_no.lock().unwrap();
        let entry = TranslogEntry {
            seq_no: *seq,
            op: op.to_string(),
            payload,
        };
        *seq += 1;

        let mut line = serde_json::to_string(&entry)?;
        line.push('\n');

        let mut file = self.file.lock().unwrap();
        file.write_all(line.as_bytes())?;
        // fsync — guarantee this is durable on disk before we confirm the write
        file.sync_data()?;

        Ok(entry)
    }

    /// Append multiple operations to the translog with a single fsync.
    /// Much cheaper than calling append() in a loop for bulk indexing.
    pub fn append_bulk(&self, ops: &[(&str, serde_json::Value)]) -> Result<Vec<TranslogEntry>> {
        let mut seq = self.seq_no.lock().unwrap();
        let mut entries = Vec::with_capacity(ops.len());
        let mut buf = String::new();

        for (op, payload) in ops {
            let entry = TranslogEntry {
                seq_no: *seq,
                op: op.to_string(),
                payload: payload.clone(),
            };
            *seq += 1;
            buf.push_str(&serde_json::to_string(&entry)?);
            buf.push('\n');
            entries.push(entry);
        }

        let mut file = self.file.lock().unwrap();
        file.write_all(buf.as_bytes())?;
        // Single fsync for the entire batch
        file.sync_data()?;

        Ok(entries)
    }

    /// Read all translog entries (used for replay on startup).
    pub fn read_all(&self) -> Result<Vec<TranslogEntry>> {
        let file = self.file.lock().unwrap();
        let reader = BufReader::new(&*file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<TranslogEntry>(&line) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Truncate the translog to zero bytes after a successful commit.
    /// The data is now safely committed to Tantivy segments, so we no longer need it.
    pub fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;
        file.sync_data()?;
        // Reset seq_no since we have a clean slate
        drop(file);
        let mut seq = self.seq_no.lock().unwrap();
        *seq = 0;
        tracing::info!("Translog truncated after flush.");
        Ok(())
    }

    /// Helper to read entries directly from a path (used during open to check existing state)
    fn read_entries_from_path<P: AsRef<Path>>(path: P) -> Result<Vec<TranslogEntry>> {
        let file = OpenOptions::new().read(true).open(&path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(entry) = serde_json::from_str::<TranslogEntry>(&line) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}
