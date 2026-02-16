"""
Sensors for event-driven orchestration.

This demonstrates the pattern for Polymarket's use case:
- Streaming ingestion runs continuously (Kafka/Flink/etc) - 100ms snapshots
- Dagster OBSERVES the stream as an external asset
- Sensors check for thresholds (row count, time window, etc.)
- When threshold met, triggers Spark job for batch aggregation
"""

import dagster as dg
from datetime import datetime
import random


@dg.sensor(
    name="order_book_data_sensor",
    minimum_interval_seconds=30,  # Check every 30 seconds
    description="Monitors ClickHouse for new order book snapshots and triggers market maker rule processing"
)
def order_book_data_sensor(context: dg.SensorEvaluationContext):
    """
    Production implementation would:
    1. Query ClickHouse for latest snapshot timestamp
    2. Compare to last processed timestamp (cursor)
    3. Check row count threshold (e.g., 10M new snapshots)
    4. Trigger Spark job when threshold met

    For demo: Simulates checking for new data.
    """

    # Simulate checking ClickHouse for new data
    # In production:
    # cursor = context.cursor or "2024-01-01T00:00:00"
    # query = f"SELECT max(snapshot_timestamp), count(*) FROM order_books WHERE snapshot_timestamp > '{cursor}'"
    # result = clickhouse_client.execute(query)

    # Demo simulation
    has_new_data = random.random() > 0.7  # 30% chance of new data

    if not has_new_data:
        return dg.SkipReason("No new order book data above threshold")

    # Simulate new data details
    snapshot_count = random.randint(5_000_000, 15_000_000)
    latest_timestamp = datetime.now().isoformat()

    context.log.info(f"New order book data detected:")
    context.log.info(f"  Snapshots: {snapshot_count:,}")
    context.log.info(f"  Latest timestamp: {latest_timestamp}")
    context.log.info(f"  Triggering Spark job for market maker rule processing...")

    # Trigger the Spark job
    yield dg.RunRequest(
        run_key=f"orderbook_{latest_timestamp}",
        tags={
            "snapshot_count": str(snapshot_count),
            "triggered_by": "order_book_sensor",
            "data_timestamp": latest_timestamp
        },
        # In a real setup, you'd target specific assets:
        # asset_selection=[AssetKey("script_spark_job_example")]
    )

    # Update cursor for next run
    # context.update_cursor(latest_timestamp)


# Additional sensor for high-priority market events
@dg.sensor(
    name="market_event_priority_sensor",
    minimum_interval_seconds=10,  # Check more frequently
    description="High-priority sensor for urgent market events requiring immediate processing"
)
def market_event_priority_sensor(context: dg.SensorEvaluationContext):
    """
    Demonstrates priority/urgent processing pattern.

    Use case: Critical market events (price anomalies, large trades)
    need immediate processing, bypassing normal queue.
    """

    # Simulate checking for urgent events
    # In production: Check monitoring system, alert queue, or event stream

    has_urgent_event = random.random() > 0.95  # 5% chance

    if not has_urgent_event:
        return dg.SkipReason("No urgent market events")

    event_type = random.choice(["price_anomaly", "large_trade", "liquidity_spike"])

    context.log.warning(f"URGENT: {event_type} detected - triggering priority processing")

    yield dg.RunRequest(
        run_key=f"urgent_{event_type}_{datetime.now().timestamp()}",
        tags={
            "priority": "urgent",
            "event_type": event_type,
            "triggered_by": "priority_sensor"
        }
    )


# Define for auto-discovery
defs = dg.Definitions(sensors=[order_book_data_sensor, market_event_priority_sensor])
