# ROpenSearch

## Description
ROpenSearch is a distributed search engine written in Rust and built on top of Tantivy. It aims to provide OpenSearch-compatible APIs while offering a modern Rust-native architecture.

## Overview
ROpenSearch is designed as a drop-in replacement for OpenSearch hot-node clusters. It combines the blazing-fast search capabilities of Tantivy with a distributed architecture to provide highly scalable search and indexing.

## Architecture
At a high level, the architecture consists of:
- **API layer**: Implements OpenSearch-compatible REST APIs utilizing `axum`.
- **cluster management**: Handles node discovery, membership, and replication.
- **shards**: Manages individual index shards (primary or replica) and their lifecycles.
- **tantivy engine**: The core search and indexing engine powering each shard.
- **distributed search**: Coordinates scatter-gather query execution across nodes and shards.

## Project Structure
- `src/api`: REST APIs
- `src/cluster`: Cluster management and membership
- `src/config`: Configuration handling
- `src/engine`: Tantivy search engine wrapper
- `src/search`: Distributed query execution
- `src/shard`: Shard abstraction
- `src/storage`: Filesystem/Blob storage abstraction
- `src/transport`: Node-to-node communication
- `src/wal`: Write-ahead logging for durability

## Getting Started
To run the ROpenSearch node:
```bash
cargo run
```
Then query the basic API:
```bash
curl http://localhost:9200/
```

## Roadmap
- [ ] Implement OpenSearch-compatible index mapping APIs
- [ ] Distributed shard allocation and rebalancing
- [ ] Consensus mechanism for cluster state
- [ ] Replication and recovery procedures

## License
Apache-2.0
