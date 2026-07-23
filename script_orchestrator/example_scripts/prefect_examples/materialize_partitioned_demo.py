# /// script
# dependencies = ["prefect>=3.4"]
# ///

"""Prefect @materialize demo exercising the four-pack:

  1. Partitioned backfills — the flow takes a `run_date` parameter, and
     because the adjacent prefect.yaml declares a `parameters:` block with
     that key AND a schedule, the orchestrator emits a
     DailyPartitionsDefinition so the whole thing supports
     drag-select backfills in Dagster's UI.

  2. Deployment parameters → Dagster Config — the deployment's `max_price`
     and `notify_email` fields become a Dagster Config class; the
     multi_asset accepts them and passes matching values to the flow.

  3. Python assert statements → asset checks — the `@materialize` bodies
     contain regular Python asserts; each becomes an AssetCheckSpec on the
     asset, and successful runs yield AssetCheckResult(passed=True).

  4. Auto data previews — return values are `list[dict]`; the orchestrator
     captures the first 10 rows and emits them as a MetadataValue.md
     table on the asset's materialization.
"""

from prefect import flow
from prefect.assets import add_asset_metadata, materialize


@materialize("s3://lake/daily/orders.parquet", tags=["etl", "daily"])
def load_daily_orders(run_date: str = "2024-01-01"):
    """Load orders for a specific date. row_count and preview auto-captured."""
    add_asset_metadata({"run_date": run_date, "source": "s3://raw/orders"})
    rows = [
        {"order_id": f"{run_date}-{i}", "amount": 10 * i, "region": "US" if i % 2 else "EU"}
        for i in range(1, 8)
    ]
    assert len(rows) > 0, "orders must not be empty"
    assert all(r["amount"] > 0 for r in rows), "all amounts must be positive"
    return rows


@materialize("s3://lake/daily/order_summary.parquet", tags=["mart", "daily"])
def summarize_orders(orders, max_price: int = 100, notify_email: str = "team@example.com"):
    """Summarize orders by region. Uses config-driven max_price threshold."""
    filtered = [o for o in orders if o["amount"] <= max_price]
    add_asset_metadata({
        "max_price_threshold": max_price,
        "notify_email": notify_email,
        "kept_count": len(filtered),
        "dropped_count": len(orders) - len(filtered),
    })
    summary = [
        {"region": "US", "total": sum(o["amount"] for o in filtered if o["region"] == "US")},
        {"region": "EU", "total": sum(o["amount"] for o in filtered if o["region"] == "EU")},
    ]
    assert all(row["total"] >= 0 for row in summary), "region totals must be non-negative"
    return summary


@flow(log_prints=True)
def daily_orders_pipeline(
    run_date: str = "2024-01-01",
    max_price: int = 100,
    notify_email: str = "team@example.com",
):
    """End-to-end: load orders for run_date, summarize with max_price filter."""
    orders = load_daily_orders(run_date=run_date)
    summarize_orders(orders, max_price=max_price, notify_email=notify_email)


if __name__ == "__main__":
    daily_orders_pipeline()
