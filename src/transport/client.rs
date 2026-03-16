use crate::cluster::state::{ClusterState, NodeInfo};
use reqwest::{Client, Error};
use std::time::Duration;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct TransportClient {
    client: Client,
}

impl Default for TransportClient {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportClient {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        }
    }

    /// Attempts to join the cluster by contacting the seed hosts
    pub async fn join_cluster(
        &self,
        seed_hosts: &[String],
        local_node: &NodeInfo,
    ) -> Option<ClusterState> {
        for host in seed_hosts {
            debug!("Attempting to join cluster via seed host: {}", host);
            let url = format!("http://{}/_internal/discovery/join", host);

            match self.client.post(&url).json(local_node).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        if let Ok(state) = response.json::<ClusterState>().await {
                            info!("Successfully joined cluster via {}", host);
                            return Some(state);
                        }
                    } else {
                        debug!("Seed {} returned status: {}", host, response.status());
                    }
                }
                Err(e) => debug!("Failed to connect to seed {}: {}", host, e),
            }
        }

        error!("Could not join cluster; no seed hosts responded affirmatively.");
        None
    }

    /// (Master Only) Broadcasts state to all active nodes
    pub async fn publish_state(&self, state: &ClusterState) {
        for node in state.nodes.values() {
            let url = format!(
                "http://{}:{}/_internal/cluster/state",
                node.host, node.transport_port
            );
            if let Err(e) = self.client.post(&url).json(state).send().await {
                error!("Failed to publish state to node {}: {}", node.id, e);
            }
        }
    }

    /// Forward a single document to the specific shard on a node
    pub async fn forward_index_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let url = format!(
            "http://{}:{}/_internal/index/{}/shard/{}/_doc",
            node.host, node.transport_port, index_name, shard_id
        );
        let response = self.client.post(&url).json(payload).send().await?;
        response.json().await
    }

    /// Forward a bulk batch to a specific shard on a node
    pub async fn forward_bulk_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        docs: &[serde_json::Value],
    ) -> Result<serde_json::Value, Error> {
        let url = format!(
            "http://{}:{}/_internal/index/{}/shard/{}/_bulk",
            node.host, node.transport_port, index_name, shard_id
        );
        let response = self.client.post(&url).json(docs).send().await?;
        response.json().await
    }

    /// Forward a DSL search to a specific shard
    pub async fn forward_search_dsl_to_shard(
        &self,
        node: &NodeInfo,
        index_name: &str,
        shard_id: u32,
        req: &crate::search::SearchRequest,
    ) -> Result<Vec<serde_json::Value>, Error> {
        let url = format!(
            "http://{}:{}/_internal/index/{}/shard/{}/_search",
            node.host, node.transport_port, index_name, shard_id
        );
        let response = self.client.post(&url).json(req).send().await?;
        response.json().await
    }

    /// Sends a heartbeat ping to another node
    pub async fn send_ping(&self, target_node: &NodeInfo, local_node_id: &str) -> Result<(), Error> {
        let url = format!(
            "http://{}:{}/_internal/cluster/ping",
            target_node.host, target_node.transport_port
        );
        self.client.post(&url).json(&local_node_id).send().await?;
        Ok(())
    }
}
