use crate::api::AppState;
use crate::cluster::state::{ClusterState, NodeInfo};
use axum::{
    extract::{Json, Path, Query, State},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::Value;
use tracing::info;

/// When a node wants to join the cluster, it sends its NodeInfo to a seed host
pub async fn handle_join(
    State(state): State<AppState>,
    Json(node_info): Json<NodeInfo>,
) -> Json<ClusterState> {
    info!("Received join request from node: {}", node_info.id);
    state.cluster_manager.add_node(node_info);
    Json(state.cluster_manager.get_state())
}

/// When the master updates the state, it pushes the new state to all nodes
pub async fn handle_state_update(
    State(state): State<AppState>,
    Json(new_state): Json<ClusterState>,
) {
    info!("Received cluster state update, new version: {}", new_state.version);
    state.cluster_manager.update_state(new_state);
}

pub async fn handle_ping(
    State(state): State<AppState>,
    Json(source_node_id): Json<String>,
) {
    state.cluster_manager.ping_node(&source_node_id);
}

// ─── Shard-level internal endpoints ──────────────────────────────────────────

/// POST /_internal/index/{index}/shard/{shard_id}/_doc
/// Index a single document into a specific shard engine on this node.
pub async fn handle_shard_doc(
    State(state): State<AppState>,
    Path((index_name, shard_id)): Path<(String, u32)>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let engine = match state.shard_manager.get_shard(&index_name, shard_id) {
        Some(e) => e,
        None => {
            // Auto-open the shard if it lands here but wasn't pre-created
            match state.shard_manager.open_shard(&index_name, shard_id) {
                Ok(e) => e,
                Err(err) => return Json(serde_json::json!({ "error": err.to_string() })),
            }
        }
    };

    info!("Internal: index doc into {}/shard_{}", index_name, shard_id);
    match engine.add_document(payload) {
        Ok(id) => Json(serde_json::json!({
            "_index": index_name,
            "_id": id,
            "_shard": shard_id,
            "result": "created"
        })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// POST /_internal/index/{index}/shard/{shard_id}/_bulk
/// Bulk index documents into a specific shard.
pub async fn handle_shard_bulk(
    State(state): State<AppState>,
    Path((index_name, shard_id)): Path<(String, u32)>,
    Json(docs): Json<Vec<Value>>,
) -> Json<Value> {
    let engine = match state.shard_manager.get_shard(&index_name, shard_id) {
        Some(e) => e,
        None => match state.shard_manager.open_shard(&index_name, shard_id) {
            Ok(e) => e,
            Err(err) => return Json(serde_json::json!({ "error": err.to_string() })),
        }
    };

    info!("Internal: bulk {} docs into {}/shard_{}", docs.len(), index_name, shard_id);
    match engine.bulk_add_documents(docs) {
        Ok(ids) => Json(serde_json::json!({
            "took": 0,
            "errors": false,
            "items": ids.iter().map(|id| serde_json::json!({ "index": { "_id": id, "result": "created" } })).collect::<Vec<_>>()
        })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

#[derive(Deserialize)]
pub struct SearchParams {
    q: String,
}

/// GET /_internal/index/{index}/shard/{shard_id}/_search?q=...
pub async fn handle_shard_search(
    State(state): State<AppState>,
    Path((index_name, shard_id)): Path<(String, u32)>,
    Query(params): Query<SearchParams>,
) -> Json<Value> {
    let engine = match state.shard_manager.get_shard(&index_name, shard_id) {
        Some(e) => e,
        None => return Json(serde_json::json!({ "error": "Shard not found on this node" })),
    };

    match engine.search(&params.q) {
        Ok(hits) => {
            let os_hits: Vec<Value> = hits.into_iter().map(|source| serde_json::json!({
                "_index": index_name, "_shard": shard_id, "_source": source
            })).collect();
            Json(serde_json::json!(os_hits))
        }
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// POST /_internal/index/{index}/shard/{shard_id}/_search (DSL)
pub async fn handle_shard_search_dsl(
    State(state): State<AppState>,
    Path((index_name, shard_id)): Path<(String, u32)>,
    Json(req): Json<crate::search::SearchRequest>,
) -> Json<Value> {
    let engine = match state.shard_manager.get_shard(&index_name, shard_id) {
        Some(e) => e,
        None => return Json(serde_json::json!({ "error": "Shard not found on this node" })),
    };

    match engine.search_query(&req) {
        Ok(hits) => {
            let os_hits: Vec<Value> = hits.into_iter().map(|source| serde_json::json!({
                "_index": index_name, "_shard": shard_id, "_source": source
            })).collect();
            Json(serde_json::json!(os_hits))
        }
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// POST /_internal/index/{index}/shard/{shard_id}/_refresh
pub async fn handle_shard_refresh(
    State(state): State<AppState>,
    Path((index_name, shard_id)): Path<(String, u32)>,
) -> Json<Value> {
    let engine = match state.shard_manager.get_shard(&index_name, shard_id) {
        Some(e) => e,
        None => return Json(serde_json::json!({ "error": "Shard not found" })),
    };
    match engine.refresh() {
        Ok(_) => Json(serde_json::json!({"_shards": {"total": 1, "successful": 1, "failed": 0}})),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

/// POST /_internal/index/{index}/shard/{shard_id}/_flush
pub async fn handle_shard_flush(
    State(state): State<AppState>,
    Path((index_name, shard_id)): Path<(String, u32)>,
) -> Json<Value> {
    let engine = match state.shard_manager.get_shard(&index_name, shard_id) {
        Some(e) => e,
        None => return Json(serde_json::json!({ "error": "Shard not found" })),
    };
    match engine.flush() {
        Ok(_) => Json(serde_json::json!({"_shards": {"total": 1, "successful": 1, "failed": 0}})),
        Err(e) => Json(serde_json::json!({ "error": e.to_string() })),
    }
}

pub fn create_transport_router(state: AppState) -> Router {
    Router::new()
        // Cluster coordination (still use ArC<ClusterManager> implicitly via AppState)
        .route("/_internal/discovery/join", post(handle_join))
        .route("/_internal/cluster/state", post(handle_state_update))
        .route("/_internal/cluster/ping", post(handle_ping))
        // Shard-level document operations
        .route("/_internal/index/{index}/shard/{shard_id}/_doc", post(handle_shard_doc))
        .route("/_internal/index/{index}/shard/{shard_id}/_bulk", post(handle_shard_bulk))
        .route("/_internal/index/{index}/shard/{shard_id}/_search", get(handle_shard_search))
        .route("/_internal/index/{index}/shard/{shard_id}/_search", post(handle_shard_search_dsl))
        .route("/_internal/index/{index}/shard/{shard_id}/_refresh", post(handle_shard_refresh))
        .route("/_internal/index/{index}/shard/{shard_id}/_flush", post(handle_shard_flush))
        .with_state(state)
}
