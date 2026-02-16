#!/usr/bin/env python3
"""
Example Dask analysis showing parallel processing pattern for analytics.

Uses actual Dask running locally in this demo.
For production: Connect to actual Dask cluster scheduler.

Pattern shown:
- Parallel data processing with Dask DataFrames
- Statistical analysis across markets
- Anomaly detection patterns
- Report generation
"""

from datetime import datetime
import sys
import logging

#  Suppress Dask verbose logging
logging.getLogger('distributed').setLevel(logging.ERROR)
logging.getLogger('tornado').setLevel(logging.ERROR)

def run_dask_analysis():
    """
    Dask analysis for analytics on market data with actual Dask.

    Uses local Dask cluster for demo (production would connect to remote cluster).
    Demonstrates real Dask operations: DataFrames, parallel compute, aggregations.
    """
    print(f"[{datetime.now()}] Starting Dask analysis with actual Dask...")
    print("\n=== RUNNING ACTUAL DASK (Local Cluster) ===\n")

    try:
        import dask
        import dask.dataframe as dd
        import pandas as pd
        import numpy as np
        from dask.distributed import Client, LocalCluster

        # Start local Dask cluster
        print("Starting local Dask cluster...")
        cluster = LocalCluster(
            n_workers=2,
            threads_per_worker=2,
            memory_limit='500MB',
            silence_logs=True
        )
        client = Client(cluster)

        print(f"  Dashboard: {client.dashboard_link}")
        print(f"  Workers: {len(client.scheduler_info()['workers'])}")

        # Generate synthetic market data
        print("\n[Task 1/3] Generating synthetic market data...")

        num_markets = 50
        num_snapshots_per_market = 500

        # Create synthetic data
        data_list = []
        for market_id in range(num_markets):
            for snap in range(num_snapshots_per_market):
                data_list.append({
                    'market_id': f'market_{market_id}',
                    'snapshot': snap,
                    'price': 0.5 + np.random.normal(0, 0.1),
                    'volume': np.random.randint(1000, 10000),
                    'spread': np.random.uniform(0.001, 0.05),
                    'liquidity_depth': np.random.randint(10000, 100000)
                })

        pdf = pd.DataFrame(data_list)
        ddf = dd.from_pandas(pdf, npartitions=8)

        print(f"  Records: {len(ddf):,}")
        print(f"  Markets: {num_markets}")

        # Parallel aggregations
        print("\n[Task 2/3] Computing market statistics (parallel)...")

        market_stats = ddf.groupby('market_id').agg({
            'price': ['mean', 'std'],
            'volume': ['sum', 'mean'],
            'spread': ['mean', 'min', 'max'],
            'liquidity_depth': 'mean'
        }).compute()

        print(f"  Statistics computed for {len(market_stats)} markets")

        # Anomaly detection
        print("\n[Task 3/3] Detecting anomalies...")

        market_stats.columns = ['_'.join(col).strip('_') for col in market_stats.columns]

        anomalies = market_stats[
            (market_stats['spread_mean'] > 0.03) |
            (market_stats['liquidity_depth_mean'] < 20000)
        ]

        print(f"  Anomalies detected: {len(anomalies)}")

        # Cleanup
        client.close()
        cluster.close()

        print(f"\n[{datetime.now()}] Dask analysis completed!")
        print(f"Markets analyzed: {num_markets}")
        print(f"Total snapshots: {len(ddf):,}")
        print(f"Anomalies detected: {len(anomalies)}")

        return 0

    except Exception as e:
        print(f"ERROR: Dask analysis failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = run_dask_analysis()
    sys.exit(exit_code)
