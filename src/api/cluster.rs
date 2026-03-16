use crate::api::AppState;
use crate::cluster::state::ClusterState;
use axum::{extract::State, Json};
use serde::Serialize;

#[derive(Serialize)]
pub struct ClusterHealth {
    pub cluster_name: String,
    pub status: String,
    pub timed_out: bool,
    pub number_of_nodes: usize,
    pub number_of_data_nodes: usize,
}

/// Handler for `GET /_cluster/health`
pub async fn get_health(State(state): State<AppState>) -> Json<ClusterHealth> {
    let cs = state.cluster_manager.get_state();
    let data_nodes = cs.nodes.values()
        .filter(|n| n.roles.contains(&crate::cluster::state::NodeRole::Data))
        .count();
    Json(ClusterHealth {
        cluster_name: cs.cluster_name,
        status: "green".into(),
        timed_out: false,
        number_of_nodes: cs.nodes.len(),
        number_of_data_nodes: data_nodes,
    })
}

/// Handler for `GET /_cluster/state`
pub async fn get_state(State(state): State<AppState>) -> Json<ClusterState> {
    Json(state.cluster_manager.get_state())
}
