use crate::api::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub struct SearchParams {
    q: String,
}

/// GET /{index}/_search?q=... — query-string search across all local shards for this index.
pub async fn search_documents(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Query(params): Query<SearchParams>,
) -> Json<Value> {
    // Query all local shard engines for this index directly (no scatter to remotes for now)
    let shards = state.shard_manager.get_index_shards(&index_name);

    let mut all_hits = Vec::new();
    let mut successful_shards = 0usize;
    let mut failed_shards = 0usize;

    for (shard_id, engine) in shards {
        match engine.search(&params.q) {
            Ok(hits) => {
                successful_shards += 1;
                for source in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name,
                        "_shard": shard_id,
                        "_source": source
                    }));
                }
            }
            Err(e) => {
                tracing::error!("Shard {}/{} search failed: {}", index_name, shard_id, e);
                failed_shards += 1;
            }
        }
    }

    Json(serde_json::json!({
        "_shards": {
            "total": successful_shards + failed_shards,
            "successful": successful_shards,
            "failed": failed_shards
        },
        "hits": {
            "total": { "value": all_hits.len(), "relation": "eq" },
            "hits": all_hits
        }
    }))
}
