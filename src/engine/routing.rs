use crate::cluster::state::IndexMetadata;
use std::hash::{DefaultHasher, Hash, Hasher};

/// A simple routing function inspired by OpenSearch/Elasticsearch hashing
/// to determine which shard a specific document ID belongs to.
pub fn calculate_shard(document_id: &str, number_of_shards: u32) -> u32 {
    let mut hasher = DefaultHasher::new();
    document_id.hash(&mut hasher);
    let hash = hasher.finish();
    
    // Murmur-like modulo
    (hash % number_of_shards as u64) as u32
}

/// Given a document ID and the index metadata, determine exactly which Node ID should hold this document
pub fn route_document(document_id: &str, index_metadata: &IndexMetadata) -> Option<String> {
    if index_metadata.number_of_shards == 0 {
        return None;
    }
    
    let shard_id = calculate_shard(document_id, index_metadata.number_of_shards);
    index_metadata.shards.get(&shard_id).cloned()
}
