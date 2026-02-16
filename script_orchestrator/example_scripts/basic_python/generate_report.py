#!/usr/bin/env python3
"""
Example reporting script.
This depends on the transformation script and generates a summary report.
"""

import json
import time
from datetime import datetime
from pathlib import Path

def main():
    print(f"[{datetime.now()}] Starting report generation...")

    data_dir = Path("/tmp/dagster_scripts_demo")
    input_file = data_dir / "transformed_data.json"

    # Check if input file exists
    if not input_file.exists():
        print(f"❌ Error: Input file not found: {input_file}")
        print("   Make sure transform_data.py has run first!")
        exit(1)

    # Read transformed data
    data = json.loads(input_file.read_text())
    print(f"📖 Loaded transformed data from {input_file}")

    # Simulate some report generation work
    time.sleep(1)

    # Generate report
    report = f"""
    ================================================
    DATA PIPELINE SUMMARY REPORT
    ================================================
    Generated: {datetime.now().isoformat()}
    Data Source: {data['source_timestamp']}
    Transform Time: {data['transformed_at']}

    SUMMARY STATISTICS:
    - Total Records: {data['record_count']}
    - Total Value: {data['total_value']}
    - Average Value: {data['total_value'] / data['record_count']:.2f}

    CATEGORY BREAKDOWN:
    """

    # Add category breakdown
    categories = {}
    for record in data['transformed_records']:
        cat = record['value_category']
        categories[cat] = categories.get(cat, 0) + 1

    for category, count in categories.items():
        report += f"    - {category.upper()}: {count} records\n"

    report += f"""
    DETAILED RECORDS:
    """
    for record in data['transformed_records']:
        report += f"    - ID {record['id']}: Original={record['value']}, Doubled={record['value_doubled']}, Category={record['value_category']}\n"

    report += """
    ================================================
    """

    # Save report
    output_file = data_dir / "summary_report.txt"
    output_file.write_text(report)

    print(report)
    print(f"✅ Report saved to: {output_file}")
    print(f"[{datetime.now()}] Report generation complete!")

if __name__ == "__main__":
    main()
