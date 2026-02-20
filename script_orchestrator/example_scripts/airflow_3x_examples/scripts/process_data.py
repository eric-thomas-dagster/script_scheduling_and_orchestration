#!/usr/bin/env python3
"""
Simple data processing script for Bash operator demo.
Takes an input file path and processes it.
"""
import sys
import json
from pathlib import Path


def process_data(input_file: str):
    """Process data from input file."""
    print(f"Processing data from: {input_file}")

    # Simulate data processing
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"⚠️  Input file doesn't exist (demo mode): {input_file}")
        print("Creating mock data for demonstration...")
        data = {
            "customers": [
                {"id": 1, "name": "Acme Corp", "revenue": 50000},
                {"id": 2, "name": "TechStart Inc", "revenue": 35000},
                {"id": 3, "name": "Global Industries", "revenue": 75000},
            ]
        }
    else:
        print(f"✅ Reading data from: {input_file}")
        # In real scenario, would read the actual file
        data = {"note": "Would read actual file here"}

    # Process the data
    processed_data = {
        "processed": True,
        "record_count": len(data.get("customers", [])),
        "total_revenue": sum(c.get("revenue", 0) for c in data.get("customers", [])),
        "source_file": str(input_file)
    }

    print("\n📊 Processing Results:")
    print(json.dumps(processed_data, indent=2))

    # Write output
    output_file = input_path.parent / f"{input_path.stem}_processed.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(processed_data, f, indent=2)

    print(f"\n✅ Processed data written to: {output_file}")
    return processed_data


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python process_data.py <input_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    process_data(input_file)
