//! Cluster management.
//! Long-term goal: Handles node discovery, membership, and cluster state via gossip or consensus.

pub mod manager;
pub mod state;

pub use manager::ClusterManager;
