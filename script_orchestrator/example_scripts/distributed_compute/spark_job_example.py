#!/usr/bin/env python3
"""
Example Spark job showing distributed compute pattern for Polymarket use case.

Uses actual PySpark running locally in this demo.
For production: Deploy to actual Spark cluster with spark-submit.

Pattern shown:
- Process large-scale order book snapshots
- Distribute work across Spark executors
- Apply market maker rules at scale
- Demonstrate aggregations and filtering
"""

from datetime import datetime
import sys

def run_spark_job():
    """
    Spark job for processing order book data with actual PySpark.

    Uses local Spark session for demo (production would connect to cluster).
    Demonstrates real Spark operations: DataFrames, aggregations, filtering.
    """
    print(f"[{datetime.now()}] Starting Spark job with PySpark...")
    print("\n=== RUNNING ACTUAL SPARK (Local Mode) ===\n")

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, avg, sum as spark_sum, when
        import random

        # Initialize Spark session (local mode for demo)
        print("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("Polymarket-MarketMakerRules") \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        print(f"  Spark version: {spark.version}")
        print(f"  Master: {spark.sparkContext.master}")
        print(f"  App name: {spark.sparkContext.appName}")

        # Create synthetic order book data (simulating ClickHouse read)
        print("\n[Stage 1/3] Generating synthetic order book snapshots...")

        num_snapshots = 10000  # Reduced for speed
        num_markets = 20

        data = []
        for i in range(num_snapshots):
            market_id = f"market_{random.randint(1, num_markets)}"
            data.append({
                "snapshot_id": i,
                "market_id": market_id,
                "timestamp": datetime.now().isoformat(),
                "best_bid": round(random.uniform(0.3, 0.7), 4),
                "best_ask": round(random.uniform(0.3, 0.7), 4),
                "bid_volume": random.randint(100, 10000),
                "ask_volume": random.randint(100, 10000),
                "total_volume": random.randint(5000, 50000),
            })

        df = spark.createDataFrame(data)
        print(f"  Snapshots loaded: {df.count():,}")

        # Calculate spread and apply market maker rules
        print("\n[Stage 2/3] Computing spreads and aggregating by market...")

        df_with_spread = df.withColumn(
            "spread_pct",
            ((col("best_ask") - col("best_bid")) / col("best_bid")) * 100
        )

        # Aggregate by market
        market_stats = df_with_spread.groupBy("market_id").agg(
            count("*").alias("snapshot_count"),
            avg("spread_pct").alias("avg_spread_pct"),
            spark_sum("total_volume").alias("total_market_volume")
        )

        print(f"  Markets analyzed: {market_stats.count()}")

        # Apply market maker rules
        print("\n[Stage 3/3] Applying market maker rules...")

        violations = market_stats.filter(
            (col("avg_spread_pct") < 0.5) |
            (col("total_market_volume") < 50000)
        )

        violation_count = violations.count()
        print(f"  Violations detected: {violation_count}")

        # Cleanup
        spark.stop()

        print(f"\n[{datetime.now()}] Spark job completed successfully!")
        print(f"Total snapshots processed: {num_snapshots:,}")
        print(f"Markets analyzed: {num_markets}")
        print(f"Violations identified: {violation_count}")

        return 0

    except Exception as e:
        print(f"ERROR: Spark job failed: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    exit_code = run_spark_job()
    sys.exit(exit_code)
