#!/usr/bin/env python3
"""
Example data transformation script.
This depends on the extraction script and transforms the extracted data.
"""

import json
import time
from datetime import datetime
from pathlib import Path

def main():
    print(f"[{datetime.now()}] Starting data transformation...")

    data_dir = Path("/tmp/dagster_scripts_demo")
    input_file = data_dir / "extracted_data.json"

    # Check if input file exists
    if not input_file.exists():
        print(f"❌ Error: Input file not found: {input_file}")
        print("   Make sure extract_data.py has run first!")
        exit(1)

    # Read extracted data
    data = json.loads(input_file.read_text())
    print(f"📖 Loaded {len(data['records'])} records from {input_file}")

    # Simulate some transformation work
    time.sleep(1)

    # Transform data (e.g., calculate totals, aggregate, etc.)
    transformed_data = {
        "transformed_at": datetime.now().isoformat(),
        "source_timestamp": data["extracted_at"],
        "total_value": sum(r["value"] for r in data["records"]),
        "record_count": len(data["records"]),
        "transformed_records": [
            {
                **record,
                "value_doubled": record["value"] * 2,
                "value_category": "high" if record["value"] > 150 else "low"
            }
            for record in data["records"]
        ]
    }

    output_file = data_dir / "transformed_data.json"
    output_file.write_text(json.dumps(transformed_data, indent=2))

    print(f"✅ Transformed {len(transformed_data['transformed_records'])} records")
    print(f"✅ Total value: {transformed_data['total_value']}")
    print(f"✅ Data saved to: {output_file}")
    print(f"[{datetime.now()}] Data transformation complete!")

if __name__ == "__main__":
    main()
