#!/usr/bin/env python
"""Test script demonstrating daily partitioning with a date parameter."""

import argparse
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Daily data processing script")

    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='Processing date in YYYY-MM-DD format'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='/tmp/output',
        help='Output directory for processed data'
    )

    args = parser.parse_args()

    # Validate date format
    try:
        process_date = datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print(f"ERROR: Invalid date format: {args.date}. Expected YYYY-MM-DD")
        return 1

    print(f"Processing data for date: {args.date}")
    print(f"Output directory: {args.output_dir}")
    print(f"Day of week: {process_date.strftime('%A')}")

    # Simulate data processing
    print(f"✓ Loaded data for {args.date}")
    print(f"✓ Processed 1,234 records")
    print(f"✓ Saved results to {args.output_dir}")

    print(f"✅ Successfully processed data for {args.date}!")

    return 0


if __name__ == "__main__":
    exit(main())
