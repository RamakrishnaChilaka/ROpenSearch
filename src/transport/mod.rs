//! Node-to-node transport.
//! Long-term goal: Handles node-to-node communication using TCP or HTTP for cluster operations.

pub mod client;
pub mod server;

pub use client::TransportClient;
