#!/usr/bin/env bash

# dev_cluster_release.sh — same as dev_cluster.sh but uses the pre-built release binary

if [ -z "$1" ]; then
    echo "Usage: ./dev_cluster_release.sh <node_id>"
    exit 1
fi

NODE_ID=$1
OFFSET=$((NODE_ID - 1))

export FERRISSEARCH_NODE_NAME="node-$NODE_ID"
export FERRISSEARCH_HTTP_PORT=$((9200 + OFFSET))
export FERRISSEARCH_TRANSPORT_PORT=$((9300 + OFFSET))
export FERRISSEARCH_DATA_DIR="./data/node-$NODE_ID"
export FERRISSEARCH_RAFT_NODE_ID=$NODE_ID

echo "Starting FerrisSearch Node $NODE_ID (RELEASE)..."
echo "HTTP Port: $FERRISSEARCH_HTTP_PORT | Transport Port: $FERRISSEARCH_TRANSPORT_PORT"

./target/release/ferrissearch
