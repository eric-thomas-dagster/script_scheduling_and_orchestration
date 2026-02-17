"""
Asset chain example - demonstrates the difference between:
- Tasks with outlets (assets, even if they depend on other assets)
- Tasks without outlets (op jobs, terminal operations)
"""

import json
import tempfile
from datetime import datetime


def _get_data_path(filename: str) -> str:
    """Get path for data files."""
    return f"{tempfile.gettempdir()}/{filename}"


def _fetch_raw_data() -> dict:
    """
    Fetch raw data from API.

    Airflow: Has outlets: [raw_data]
    Dagster: @asset (produces data)
    """
    # Simulate API fetch
    raw_data = {
        "timestamp": datetime.now().isoformat(),
        "records": [
            {"id": 1, "value": 100, "valid": True},
            {"id": 2, "value": None, "valid": False},  # Needs cleaning
            {"id": 3, "value": 200, "valid": True},
            {"id": 4, "value": -50, "valid": False},  # Needs cleaning
        ]
    }

    # Write to file (this IS the asset)
    path = _get_data_path("raw_data.json")
    with open(path, "w") as f:
        json.dump(raw_data, f)

    print(f"✅ Fetched {len(raw_data['records'])} raw records")
    return raw_data


def _clean_data() -> dict:
    """
    Clean raw data (remove invalid records).

    Airflow: Has outlets: [cleaned_data] + schedule: [raw_data]
    Dagster: @asset(deps=[raw_data]) - STILL AN ASSET because it has outlets!
    """
    # Read raw data
    path = _get_data_path("raw_data.json")
    with open(path, "r") as f:
        raw_data = json.load(f)

    # Clean: remove invalid records
    cleaned_records = [
        r for r in raw_data["records"]
        if r["valid"] and r["value"] is not None and r["value"] >= 0
    ]

    cleaned_data = {
        "timestamp": datetime.now().isoformat(),
        "records": cleaned_records,
        "source": "raw_data",
        "records_removed": len(raw_data["records"]) - len(cleaned_records)
    }

    # Write to file (this IS the asset)
    path = _get_data_path("cleaned_data.json")
    with open(path, "w") as f:
        json.dump(cleaned_data, f)

    print(f"✅ Cleaned data: {len(cleaned_records)} valid records")
    return cleaned_data


def _aggregate_data() -> dict:
    """
    Aggregate cleaned data.

    Airflow: Has outlets: [aggregated_data] + schedule: [cleaned_data]
    Dagster: @asset(deps=[cleaned_data]) - STILL AN ASSET because it has outlets!
    """
    # Read cleaned data
    path = _get_data_path("cleaned_data.json")
    with open(path, "r") as f:
        cleaned_data = json.load(f)

    # Aggregate
    values = [r["value"] for r in cleaned_data["records"]]
    aggregated = {
        "timestamp": datetime.now().isoformat(),
        "count": len(values),
        "sum": sum(values),
        "avg": sum(values) / len(values) if values else 0,
        "min": min(values) if values else 0,
        "max": max(values) if values else 0,
    }

    # Write to file (this IS the asset)
    path = _get_data_path("aggregated_data.json")
    with open(path, "w") as f:
        json.dump(aggregated, f)

    print(f"✅ Aggregated: {aggregated}")
    return aggregated


def _send_alert() -> None:
    """
    Send alert email when aggregation completes.

    Airflow: NO outlets + schedule: [aggregated_data]
    Dagster: @op in @job (NOT an asset - no data produced!)
    """
    # Read aggregated data
    path = _get_data_path("aggregated_data.json")
    with open(path, "r") as f:
        aggregated = json.load(f)

    # Send alert (just print for demo)
    print("=" * 60)
    print("📧 ALERT: Data Pipeline Completed")
    print("=" * 60)
    print(f"Records processed: {aggregated['count']}")
    print(f"Sum: {aggregated['sum']}")
    print(f"Average: {aggregated['avg']:.2f}")
    print("=" * 60)

    # In production: send_email(to="team@company.com", body=message)
    # This is a side effect - no data produced, so it's an op job, not an asset!
