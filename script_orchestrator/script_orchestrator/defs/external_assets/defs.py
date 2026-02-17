"""
External assets representing systems Dagster observes but doesn't execute.

For Polymarket use case: Streaming pipelines that run continuously.
Dagster doesn't run these, but observes them for monitoring and lineage.
"""

import dagster as dg

# Streaming ingestion pipeline (runs in Kafka/Flink/Kinesis)
order_book_stream = dg.AssetSpec(
    key="order_book_stream",
    description="Real-time order book snapshots ingested at ~100ms intervals via Kafka/Flink. "
                "Dagster observes this stream but doesn't execute it. "
                "Downstream batch jobs consume from this stream.",
    group_name="streaming_infrastructure",
    tags={
        "dagster/kind/kafka": "",
        "dagster/kind/streaming": "",
        "dagster/kind/data-ingestion": "",
    },
    metadata={
        "system": "kafka",
        "frequency": "100ms",
        "type": "streaming",
        "docs": "https://docs.polymarket.com/order-book-stream",
        "kafka_topic": "order-books-raw",
        "retention": "7 days",
        "throughput": "10,000 messages/sec"
    }
)

# ClickHouse table receiving stream
order_book_clickhouse = dg.AssetSpec(
    key="order_book_clickhouse",
    description="ClickHouse table with billions of order book snapshots. "
                "Fed by streaming pipeline, consumed by batch aggregations.",
    deps=[order_book_stream],  # Downstream of stream
    group_name="data_warehouse",
    tags={
        "dagster/kind/clickhouse": "",
        "dagster/kind/database": "",
        "dagster/kind/data-warehouse": "",
    },
    metadata={
        "system": "clickhouse",
        "table": "order_books",
        "size": "billions_of_rows",
        "cluster": "production-clickhouse",
        "database": "markets",
        "partition_key": "snapshot_date"
    }
)

# External Spark cluster
spark_cluster = dg.AssetSpec(
    key="spark_cluster",
    description="Spark cluster for distributed compute. "
                "Dagster submits jobs to this cluster but doesn't manage the cluster itself.",
    group_name="compute_infrastructure",
    tags={
        "dagster/kind/spark": "",
        "dagster/kind/compute": "",
        "dagster/kind/infrastructure": "",
    },
    metadata={
        "system": "spark",
        "type": "compute",
        "cluster_url": "spark://production-spark:7077",
        "workers": "20",
        "total_cores": "160",
        "total_memory": "640GB"
    }
)

# Define as module-level list for auto-discovery
defs = dg.Definitions(assets=[order_book_stream, order_book_clickhouse, spark_cluster])
