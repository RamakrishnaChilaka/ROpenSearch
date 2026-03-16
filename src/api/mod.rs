//! HTTP REST API Layer.
//! Long-term goal: Implement OpenSearch-compatible REST APIs for searching, indexing, and cluster management.

pub mod cluster;
pub mod index;
pub mod search;

use crate::cluster::ClusterManager;
use crate::shard::ShardManager;
use axum::{
    routing::{get, post, put},
    Json, Router,
};
use serde::Serialize;
use std::sync::Arc;

#[derive(Serialize)]
struct NodeInfoResponse {
    name: String,
    version: String,
    engine: String,
}

async fn handle_root() -> Json<NodeInfoResponse> {
    Json(NodeInfoResponse {
        name: "ropensearch-node".into(),
        version: "0.1.0".into(),
        engine: "tantivy".into(),
    })
}

#[derive(Clone)]
pub struct AppState {
    pub cluster_manager: Arc<ClusterManager>,
    pub shard_manager: Arc<ShardManager>,
    /// The local node ID — needed for routing decisions
    pub local_node_id: String,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(handle_root))
        .route("/_cluster/health", get(cluster::get_health))
        .route("/_cluster/state", get(cluster::get_state))
        // Index management
        .route("/{index}", put(index::create_index))
        // Document operations
        .route("/{index}/_doc", post(index::index_document))
        .route("/{index}/_bulk", post(index::bulk_index))
        // Search
        .route("/{index}/_search", get(search::search_documents))
        .route("/{index}/_search", post(index::search_documents_dsl))
        // Maintenance
        .route("/{index}/_refresh", post(index::refresh_index))
        .route("/{index}/_flush", post(index::flush_index))
        .with_state(state)
}
