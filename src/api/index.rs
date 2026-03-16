use crate::api::AppState;
use crate::cluster::state::IndexMetadata;
use axum::{
    extract::{Path, State},
    Json,
};
use futures::future::join_all;
use serde_json::Value;
use std::collections::HashMap;

/// PUT /{index} — Create an index with shard settings.
/// Body: `{ "settings": { "number_of_shards": 3, "number_of_replicas": 1 } }`
pub async fn create_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    body: axum::body::Bytes,
) -> Json<Value> {
    let settings: Value = serde_json::from_slice(&body).unwrap_or(serde_json::json!({}));
    let num_shards = settings
        .pointer("/settings/number_of_shards")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;

    let cluster_state = state.cluster_manager.get_state();

    if cluster_state.indices.contains_key(&index_name) {
        return Json(serde_json::json!({
            "error": { "type": "resource_already_exists_exception", "index": index_name }
        }));
    }

    // Build shard assignment: distribute shards round-robin across Data nodes
    let data_nodes: Vec<String> = cluster_state.nodes.values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .map(|n| n.id.clone())
        .collect();

    if data_nodes.is_empty() {
        return Json(serde_json::json!({ "error": "No data nodes available to assign shards" }));
    }

    let mut shard_assignment: HashMap<u32, String> = HashMap::new();
    for shard_id in 0..num_shards {
        let node_id = data_nodes[(shard_id as usize) % data_nodes.len()].clone();
        shard_assignment.insert(shard_id, node_id);
    }

    let metadata = IndexMetadata {
        name: index_name.clone(),
        number_of_shards: num_shards,
        shards: shard_assignment.clone(),
    };

    // Register in cluster state (this node is master for now)
    let mut new_state = cluster_state.clone();
    new_state.add_index(metadata);
    state.cluster_manager.update_state(new_state.clone());

    // Open local shard engines for shards assigned to this node
    for (shard_id, node_id) in &shard_assignment {
        if *node_id == state.local_node_id {
            if let Err(e) = state.shard_manager.open_shard(&index_name, *shard_id) {
                tracing::error!("Failed to open shard {} for {}: {}", shard_id, index_name, e);
            }
        }
    }

    tracing::info!("Created index '{}' with {} shards: {:?}", index_name, num_shards, shard_assignment);

    Json(serde_json::json!({
        "acknowledged": true,
        "shards_acknowledged": true,
        "index": index_name
    }))
}

/// POST /{index}/_doc — Index a single document with shard routing.
pub async fn index_document(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(mut payload): Json<Value>,
) -> Json<Value> {
    // Ensure the document has an _id
    let doc_id = if let Some(id) = payload.get("_id").and_then(|v| v.as_str()) {
        id.to_string()
    } else {
        let new_id = uuid::Uuid::new_v4().to_string();
        if let Some(obj) = payload.as_object_mut() {
            obj.insert("_id".to_string(), Value::String(new_id.clone()));
        }
        new_id
    };

    let cluster_state = state.cluster_manager.get_state();

    // Auto-create index with 1 shard if it doesn't exist (like OpenSearch)
    let metadata = if let Some(m) = cluster_state.indices.get(&index_name) {
        m.clone()
    } else {
        tracing::warn!("Index '{}' not found, auto-creating with 1 shard", index_name);
        let mut shard_assignment = HashMap::new();
        shard_assignment.insert(0u32, state.local_node_id.clone());
        let m = IndexMetadata {
            name: index_name.clone(),
            number_of_shards: 1,
            shards: shard_assignment,
        };
        let mut new_state = cluster_state.clone();
        new_state.add_index(m.clone());
        state.cluster_manager.update_state(new_state);
        let _ = state.shard_manager.open_shard(&index_name, 0);
        m
    };

    // Route document to the correct shard
    let shard_id = crate::engine::routing::calculate_shard(&doc_id, metadata.number_of_shards);
    let target_node_id = match metadata.shards.get(&shard_id) {
        Some(id) => id.clone(),
        None => return Json(serde_json::json!({ "error": "Shard has no assigned node" })),
    };

    // Forward to the node owning the shard (may be ourselves)
    let target_node = match cluster_state.nodes.get(&target_node_id) {
        Some(n) => n.clone(),
        None => return Json(serde_json::json!({ "error": "Target node not in cluster state" })),
    };

    let transport = crate::transport::client::TransportClient::new();
    match transport.forward_index_to_shard(&target_node, &index_name, shard_id, &payload).await {
        Ok(res) => Json(res),
        Err(e) => Json(serde_json::json!({ "error": format!("Forward failed: {}", e) })),
    }
}

/// POST /{index}/_refresh — Scatter refresh to all local shard engines for this index.
pub async fn refresh_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> Json<Value> {
    let shards = state.shard_manager.get_index_shards(&index_name);
    let mut successful = 0;
    let mut failed = 0;

    for (_, engine) in shards {
        match engine.refresh() {
            Ok(_) => successful += 1,
            Err(e) => { tracing::error!("Refresh failed: {}", e); failed += 1; }
        }
    }

    Json(serde_json::json!({
        "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
    }))
}

/// POST /{index}/_flush — Scatter flush to all local shard engines for this index.
pub async fn flush_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> Json<Value> {
    let shards = state.shard_manager.get_index_shards(&index_name);
    let mut successful = 0;
    let mut failed = 0;

    for (_, engine) in shards {
        match engine.flush() {
            Ok(_) => successful += 1,
            Err(e) => { tracing::error!("Flush failed: {}", e); failed += 1; }
        }
    }

    Json(serde_json::json!({
        "_shards": { "total": successful + failed, "successful": successful, "failed": failed }
    }))
}

/// POST /{index}/_bulk — Parse NDJSON, route each doc to the correct shard node.
pub async fn bulk_index(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    body: axum::body::Bytes,
) -> Json<Value> {
    let text = match std::str::from_utf8(&body) {
        Ok(t) => t,
        Err(_) => return Json(serde_json::json!({ "error": "Invalid UTF-8 body" })),
    };

    // Parse NDJSON: alternating action/doc lines
    let mut docs: Vec<(String, Value)> = Vec::new(); // (doc_id, payload)
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    while let Some(_action_line) = lines.next() {
        if let Some(doc_line) = lines.next() {
            if let Ok(mut doc) = serde_json::from_str::<Value>(doc_line) {
                let doc_id = if let Some(id) = doc.get("_id").and_then(|v| v.as_str()) {
                    id.to_string()
                } else {
                    let new_id = uuid::Uuid::new_v4().to_string();
                    if let Some(obj) = doc.as_object_mut() {
                        obj.insert("_id".to_string(), Value::String(new_id.clone()));
                    }
                    new_id
                };
                docs.push((doc_id, doc));
            }
        }
    }

    if docs.is_empty() {
        return Json(serde_json::json!({ "took": 0, "errors": false, "items": [] }));
    }

    let cluster_state = state.cluster_manager.get_state();

    // Auto-create index if it doesn't exist
    let metadata = if let Some(m) = cluster_state.indices.get(&index_name) {
        m.clone()
    } else {
        let mut shard_assignment = HashMap::new();
        shard_assignment.insert(0u32, state.local_node_id.clone());
        let m = IndexMetadata {
            name: index_name.clone(),
            number_of_shards: 1,
            shards: shard_assignment,
        };
        let mut new_state = cluster_state.clone();
        new_state.add_index(m.clone());
        state.cluster_manager.update_state(new_state);
        let _ = state.shard_manager.open_shard(&index_name, 0);
        m
    };

    // Group docs by target node (based on shard routing)
    let mut node_batches: HashMap<String, Vec<Value>> = HashMap::new();
    let mut node_shard_map: HashMap<String, u32> = HashMap::new();

    for (doc_id, payload) in &docs {
        let shard_id = crate::engine::routing::calculate_shard(doc_id, metadata.number_of_shards);
        if let Some(node_id) = metadata.shards.get(&shard_id) {
            node_batches.entry(node_id.clone()).or_default().push(payload.clone());
            node_shard_map.insert(node_id.clone(), shard_id);
        }
    }

    // Forward each batch to the owning node
    let mut all_futures = Vec::new();
    let transport = crate::transport::client::TransportClient::new();

    for (node_id, batch) in node_batches {
        if let Some(node_info) = cluster_state.nodes.get(&node_id) {
            let shard_id = *node_shard_map.get(&node_id).unwrap_or(&0);
            let client = transport.clone();
            let node_info = node_info.clone();
            let index = index_name.clone();
            all_futures.push(tokio::spawn(async move {
                client.forward_bulk_to_shard(&node_info, &index, shard_id, &batch).await
            }));
        }
    }

    let results = join_all(all_futures).await;
    let total = docs.len();
    let successful = results.iter().filter(|r| matches!(r, Ok(Ok(_)))).count();

    Json(serde_json::json!({
        "took": 0,
        "errors": successful < total,
        "items": docs.iter().map(|(id, _)| serde_json::json!({ "index": { "_id": id, "result": "created" } })).collect::<Vec<_>>()
    }))
}

/// POST /{index}/_search — DSL search across all local shards for this index.
pub async fn search_documents_dsl(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(req): Json<Value>,
) -> Json<Value> {
    let search_req: crate::search::SearchRequest = match serde_json::from_value(req) {
        Ok(r) => r,
        Err(e) => return Json(serde_json::json!({ "error": format!("Invalid query DSL: {}", e) })),
    };

    // Query all local shards for this index
    let shards = state.shard_manager.get_index_shards(&index_name);
    let mut all_hits = Vec::new();
    let mut successful = 0;
    let mut failed = 0;

    for (_, engine) in shards {
        match engine.search_query(&search_req) {
            Ok(hits) => {
                successful += 1;
                for source in hits {
                    all_hits.push(serde_json::json!({
                        "_index": index_name,
                        "_source": source
                    }));
                }
            }
            Err(e) => { tracing::error!("Shard search failed: {}", e); failed += 1; }
        }
    }

    Json(serde_json::json!({
        "_shards": { "total": successful + failed, "successful": successful, "failed": failed },
        "hits": { "total": { "value": all_hits.len(), "relation": "eq" }, "hits": all_hits }
    }))
}
