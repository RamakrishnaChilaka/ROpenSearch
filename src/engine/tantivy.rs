use anyhow::Result;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, Value, STORED, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy};

use crate::wal::Translog;

pub struct TantivyEngine {
    index: Index,
    reader: IndexReader,
    writer: Arc<RwLock<IndexWriter>>,
    schema: Schema,
    body_field: Field,
    /// The per-index refresh interval (e.g. 5s default, matches OpenSearch's index.refresh_interval)
    pub refresh_interval: Duration,
    /// Write-ahead log for crash durability
    translog: Arc<Mutex<Translog>>,
}

impl TantivyEngine {
    pub fn new<P: AsRef<Path>>(data_dir: P, refresh_interval: Duration) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let index_path = data_dir.join("index");
        std::fs::create_dir_all(&index_path)?;

        let mut schema_builder = Schema::builder();
        let body_field = schema_builder.add_text_field("body", TEXT | STORED);
        let schema = schema_builder.build();

        let mmap_dir = tantivy::directory::MmapDirectory::open(&index_path)?;
        let index = Index::open_or_create(mmap_dir, schema.clone())?;

        let writer = index.writer(50_000_000)?; // 50MB heap
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        // Open or create the translog in the data directory (not index_path)
        let translog = Translog::open(data_dir)?;

        let engine = Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(writer)),
            schema,
            body_field,
            refresh_interval,
            translog: Arc::new(Mutex::new(translog)),
        };

        // Replay any uncommitted translog entries from before a crash
        engine.replay_translog()?;

        Ok(engine)
    }

    /// Replays all pending translog entries into the Tantivy buffer.
    /// Called on startup to recover from an unclean shutdown.
    fn replay_translog(&self) -> Result<()> {
        let entries = {
            let tl = self.translog.lock().unwrap();
            tl.read_all()?
        };

        if entries.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            "Replaying {} translog entries from unclean shutdown...",
            entries.len()
        );

        let mut writer = self.writer.write().unwrap();
        for entry in &entries {
            let json_str = entry.payload.to_string();
            let doc = doc!(self.body_field => json_str);
            writer.add_document(doc)?;
        }
        writer.commit()?;
        self.reader.reload()?;

        tracing::info!("Translog replay complete. {} documents recovered.", entries.len());
        Ok(())
    }

    /// Starts the per-index background refresh loop.
    /// Called by the Node after wrapping the engine in an Arc.
    pub fn start_refresh_loop(engine: Arc<Self>) {
        let interval = engine.refresh_interval;
        tokio::spawn(async move {
            tracing::info!("Index refresh loop started (interval: {:?})", interval);
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = engine.refresh() {
                    tracing::error!("Background refresh failed: {}", e);
                }
            }
        });
    }

    /// Index a document.
    /// Writes to the translog FIRST for durability, then adds to the Tantivy buffer.
    pub fn add_document(&self, payload: serde_json::Value) -> Result<String> {
        // 1. Write to translog — durable before anything else
        {
            let tl = self.translog.lock().unwrap();
            tl.append("index", payload.clone())?;
        }

        // 2. Write to Tantivy in-memory buffer
        let writer = self.writer.write().unwrap();
        let doc_id = uuid::Uuid::new_v4().to_string();
        let json_str = payload.to_string();
        let doc = doc!(self.body_field => json_str);
        writer.add_document(doc)?;

        Ok(doc_id)
    }

    /// Bulk-index documents in a single lock acquisition and single translog fsync.
    /// Returns the list of generated document IDs (one per document).
    pub fn bulk_add_documents(&self, payloads: Vec<serde_json::Value>) -> Result<Vec<String>> {
        // 1. Write all ops to translog with a single fsync
        let ops: Vec<(&str, serde_json::Value)> = payloads
            .iter()
            .map(|p| ("index", p.clone()))
            .collect();
        {
            let tl = self.translog.lock().unwrap();
            tl.append_bulk(&ops)?;
        }

        // 2. Write all docs to Tantivy in-memory buffer under one lock
        let writer = self.writer.write().unwrap();
        let mut doc_ids = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            let doc_id = uuid::Uuid::new_v4().to_string();
            let json_str = payload.to_string();
            let doc = doc!(self.body_field => json_str);
            writer.add_document(doc)?;
            doc_ids.push(doc_id);
        }

        Ok(doc_ids)
    }

    /// Refresh: commit the in-memory buffer and reload the reader so new docs become searchable.
    pub fn refresh(&self) -> Result<()> {
        let mut writer = self.writer.write().unwrap();
        writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    /// Flush: commit the in-memory buffer to disk, then truncate the translog.
    /// After this call, all data is safe in Tantivy segments — the translog is no longer needed.
    pub fn flush(&self) -> Result<()> {
        let mut writer = self.writer.write().unwrap();
        writer.commit()?;
        drop(writer); // release lock before translog truncate
        let tl = self.translog.lock().unwrap();
        tl.truncate()?;
        Ok(())
    }

    /// Search using a simple query string (e.g. from `?q=` param)
    pub fn search(&self, query_str: &str) -> Result<Vec<serde_json::Value>> {
        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.body_field]);
        let query = query_parser.parse_query(query_str)?;
        self.execute_search(searcher, &*query)
    }

    /// Search using the OpenSearch Query DSL body
    pub fn search_query(&self, req: &crate::search::SearchRequest) -> Result<Vec<serde_json::Value>> {
        use crate::search::QueryClause;
        use tantivy::query::{AllQuery, TermQuery};
        use tantivy::schema::IndexRecordOption;
        use tantivy::Term;

        let searcher = self.reader.searcher();

        match &req.query {
            QueryClause::MatchAll(_) => {
                self.execute_search(searcher, &AllQuery)
            }
            QueryClause::Match(fields) => {
                // Pull first field/value pair and do a full-text query
                if let Some((_, value)) = fields.iter().next() {
                    let query_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let query_parser = QueryParser::for_index(&self.index, vec![self.body_field]);
                    let query = query_parser.parse_query(&query_str)?;
                    self.execute_search(searcher, &*query)
                } else {
                    Ok(vec![])
                }
            }
            QueryClause::Term(fields) => {
                // Exact term match on the body field
                if let Some((_, value)) = fields.iter().next() {
                    let term_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let term = Term::from_field_text(self.body_field, &term_str);
                    let query = TermQuery::new(term, IndexRecordOption::Basic);
                    self.execute_search(searcher, &query)
                } else {
                    Ok(vec![])
                }
            }
        }
    }

    /// Shared search execution helper
    fn execute_search(&self, searcher: tantivy::Searcher, query: &dyn tantivy::query::Query) -> Result<Vec<serde_json::Value>> {
        let top_docs = searcher.search(query, &TopDocs::with_limit(10))?;

        let mut results = Vec::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc::<tantivy::TantivyDocument>(doc_address)?;
            for value in retrieved_doc.get_all(self.body_field) {
                if let Some(text) = value.as_str() {
                    if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(text) {
                        results.push(json_val);
                    }
                }
            }
        }
        Ok(results)
    }
}
