#!/usr/bin/env python3
"""
Ingest ~1 GB of synthetic data into FerrisSearch via opensearch-py.

Usage:
    # With venv (recommended):
    /tmp/osenv/bin/python3 scripts/ingest_1gb.py

    # Or install opensearch-py first:
    pip install opensearch-py
    python3 scripts/ingest_1gb.py

    # Custom host/port:
    /tmp/osenv/bin/python3 scripts/ingest_1gb.py --host localhost --port 9200

Each document is ~500 bytes, so ~2M docs ≈ 1 GB.
Uses opensearch-py helpers.bulk with chunked batches of 5000 docs.
"""

import argparse
import hashlib
import random
import string
import time

from opensearchpy import OpenSearch, helpers

INDEX_NAME = "benchmark-1gb"
DOC_SIZE_BYTES = 500
TARGET_BYTES = 1_000_000_000  # 1 GB
BATCH_SIZE = 5000

CATEGORIES = ["electronics", "books", "clothing", "sports", "home", "toys", "food", "health"]
WORDS = [
    "search", "engine", "distributed", "cluster", "shard", "replica", "index",
    "document", "query", "filter", "aggregation", "vector", "embedding", "node",
    "raft", "consensus", "tantivy", "rust", "performance", "benchmark", "test",
    "product", "review", "rating", "price", "shipping", "warehouse", "inventory",
    "customer", "order", "payment", "delivery", "tracking", "analytics", "report",
    "dashboard", "metric", "monitor", "alert", "notification", "service", "api",
    "endpoint", "request", "response", "latency", "throughput", "bandwidth",
]


def random_text(length: int) -> str:
    """Generate random text of approximately the given length."""
    words = []
    total = 0
    while total < length:
        word = random.choice(WORDS)
        words.append(word)
        total += len(word) + 1
    return " ".join(words)[:length]


def generate_doc(doc_id: int) -> dict:
    """Generate a single document of ~DOC_SIZE_BYTES."""
    # Fixed fields ~100 bytes, fill rest with text
    text_len = DOC_SIZE_BYTES - 120
    return {
        "_index": INDEX_NAME,
        "_id": str(doc_id),
        "_source": {
            "title": f"Product {doc_id} {random.choice(WORDS)} {random.choice(WORDS)}",
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(0.99, 999.99), 2),
            "rating": round(random.uniform(1.0, 5.0), 1),
            "in_stock": random.choice([True, False]),
            "description": random_text(text_len),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest ~1 GB into FerrisSearch")
    parser.add_argument("--host", default="localhost", help="FerrisSearch host")
    parser.add_argument("--port", type=int, default=9200, help="FerrisSearch port")
    parser.add_argument("--target-gb", type=float, default=1.0, help="Target size in GB")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Docs per bulk batch")
    args = parser.parse_args()

    target_bytes = int(args.target_gb * 1_000_000_000)
    total_docs = target_bytes // DOC_SIZE_BYTES

    client = OpenSearch(
        hosts=[{"host": args.host, "port": args.port}],
        use_ssl=False,
        verify_certs=False,
        timeout=120,
        max_retries=3,
        retry_on_timeout=True,
    )

    # Verify connectivity
    info = client.info()
    print(f"Connected to {info['name']} v{info['version']}")

    # Create index with mappings
    if client.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists, deleting...")
        client.indices.delete(index=INDEX_NAME)

    print(f"Creating index '{INDEX_NAME}'...")
    client.indices.create(
        index=INDEX_NAME,
        body={
            "settings": {"number_of_shards": 3, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "category": {"type": "keyword"},
                    "price": {"type": "float"},
                    "rating": {"type": "float"},
                    "in_stock": {"type": "boolean"},
                    "description": {"type": "text"},
                }
            },
        },
    )

    print(f"Ingesting ~{total_docs:,} docs ({args.target_gb} GB) in batches of {args.batch_size}...")
    start = time.time()
    ingested = 0
    bytes_sent = 0
    errors_total = 0

    def doc_generator():
        for i in range(total_docs):
            yield generate_doc(i)

    batch = []
    for doc in doc_generator():
        batch.append(doc)
        if len(batch) >= args.batch_size:
            try:
                success, errs = helpers.bulk(client, batch, raise_on_error=False)
                ingested += success
                if isinstance(errs, list):
                    errors_total += len(errs)
                else:
                    errors_total += errs
                bytes_sent += len(batch) * DOC_SIZE_BYTES
            except Exception as e:
                print(f"\nBulk error at doc {ingested}: {e}")
                errors_total += len(batch)

            elapsed = time.time() - start
            rate = ingested / elapsed if elapsed > 0 else 0
            mb_sent = bytes_sent / (1024 * 1024)
            pct = (ingested / total_docs) * 100
            print(
                f"\r  {ingested:>10,} / {total_docs:,} docs "
                f"({pct:5.1f}%) | {mb_sent:,.0f} MB | "
                f"{rate:,.0f} docs/s | errors: {errors_total}",
                end="",
                flush=True,
            )
            batch = []

    # Final batch
    if batch:
        try:
            success, errs = helpers.bulk(client, batch, raise_on_error=False)
            ingested += success
            if isinstance(errs, list):
                errors_total += len(errs)
            else:
                errors_total += errs
        except Exception as e:
            print(f"\nFinal bulk error: {e}")
            errors_total += len(batch)

    elapsed = time.time() - start
    rate = ingested / elapsed if elapsed > 0 else 0
    mb_total = (ingested * DOC_SIZE_BYTES) / (1024 * 1024)

    print(f"\n\n{'=' * 60}")
    print(f"Ingestion complete!")
    print(f"  Documents: {ingested:,} indexed, {errors_total:,} errors")
    print(f"  Data:      ~{mb_total:,.0f} MB")
    print(f"  Time:      {elapsed:.1f}s")
    print(f"  Rate:      {rate:,.0f} docs/s")
    print(f"{'=' * 60}")

    # Refresh and verify count
    print("\nRefreshing index...")
    try:
        client.indices.refresh(index=INDEX_NAME)
    except Exception:
        pass

    try:
        resp = client.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 0})
        print(f"Verified: {resp['hits']['total']['value']:,} docs in index")
    except Exception as e:
        print(f"Verify error: {e}")


if __name__ == "__main__":
    main()
