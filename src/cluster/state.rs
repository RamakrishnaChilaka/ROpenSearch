use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Represents a unique node in the cluster
pub type NodeId = String;

/// The role a node plays in the OpenSearch cluster
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Master,
    Data,
    Client,
}

/// Information about a specific node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub name: String,
    pub host: String,
    pub transport_port: u16,
    pub http_port: u16,
    pub roles: Vec<NodeRole>,
}

/// Metadata defining how an index is sharded across the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub number_of_shards: u32,
    // Maps Shard ID (e.g., 0, 1, 2) to the Node ID that holds it
    pub shards: HashMap<u32, NodeId>,
}

/// The globally agreed-upon state of the entire cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub cluster_name: String,
    pub version: u64,
    pub master_node: Option<NodeId>,
    pub nodes: HashMap<NodeId, NodeInfo>,
    pub indices: HashMap<String, IndexMetadata>,
    #[serde(skip, default)]
    pub last_seen: HashMap<NodeId, Instant>,
}

impl ClusterState {
    pub fn new(cluster_name: String) -> Self {
        Self {
            cluster_name,
            version: 0,
            master_node: None,
            nodes: HashMap::new(),
            indices: HashMap::new(),
            last_seen: HashMap::new(),
        }
    }

    /// Add or update a node in the cluster state
    pub fn add_node(&mut self, node: NodeInfo) {
        self.last_seen.insert(node.id.clone(), Instant::now());
        self.nodes.insert(node.id.clone(), node);
        self.version += 1; // Any change bumps the cluster state version
    }

    /// Remove a node from the cluster state
    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeInfo> {
        let removed = self.nodes.remove(node_id);
        if removed.is_some() {
            self.last_seen.remove(node_id);
            self.version += 1;
            
            // If the master was removed, we need a new election (simplified here)
            if self.master_node.as_ref() == Some(node_id) {
                self.master_node = None;
            }
        }
        removed
    }

    /// Update the last seen timestamp for a node
    pub fn ping_node(&mut self, node_id: &NodeId) {
        if self.nodes.contains_key(node_id) {
            self.last_seen.insert(node_id.clone(), Instant::now());
        }
    }

    /// Add an index to the cluster state
    pub fn add_index(&mut self, metadata: IndexMetadata) {
        self.indices.insert(metadata.name.clone(), metadata);
        self.version += 1;
    }
}
