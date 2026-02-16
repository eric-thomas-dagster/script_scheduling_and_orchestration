#!/usr/bin/env python3
"""
Example data extraction script.
This simulates extracting data from an API or database.
"""

import json
import time
from datetime import datetime
from pathlib import Path

def main():
    print(f"[{datetime.now()}] Starting data extraction...")

    # Simulate some work
    time.sleep(1)

    # Create output directory
    output_dir = Path("/tmp/dagster_scripts_demo")
    output_dir.mkdir(exist_ok=True)

    # Simulate extracting data
    data = {
        "extracted_at": datetime.now().isoformat(),
        "records": [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300},
        ]
    }

    output_file = output_dir / "extracted_data.json"
    output_file.write_text(json.dumps(data, indent=2))

    print(f"✅ Extracted {len(data['records'])} records")
    print(f"✅ Data saved to: {output_file}")
    print(f"[{datetime.now()}] Data extraction complete!")

if __name__ == "__main__":
    main()
