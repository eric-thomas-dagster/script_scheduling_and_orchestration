#!/usr/bin/env python
"""Regional report generator demonstrating static partitions.

This script processes data for different regions passed as a partition key.
"""

import sys


def main():
    # Get region from partition key (passed as first argument)
    if len(sys.argv) < 2:
        print("ERROR: Region partition key required")
        print("Usage: python regional_report.py <region>")
        return 1

    region = sys.argv[1]

    print(f"=" * 60)
    print(f"Regional Report Generator - {region.upper()}")
    print(f"=" * 60)

    # Simulate region-specific processing
    region_data = {
        "us": {"customers": 15000, "revenue": 2500000, "currency": "USD"},
        "uk": {"customers": 8000, "revenue": 1800000, "currency": "GBP"},
        "de": {"customers": 12000, "revenue": 2200000, "currency": "EUR"},
        "jp": {"customers": 9500, "revenue": 3100000, "currency": "JPY"},
        "au": {"customers": 5000, "revenue": 900000, "currency": "AUD"},
    }

    if region not in region_data:
        print(f"WARNING: Unknown region '{region}', using default values")
        data = {"customers": 0, "revenue": 0, "currency": "USD"}
    else:
        data = region_data[region]

    print(f"\nProcessing data for region: {region}")
    print(f"  • Customers: {data['customers']:,}")
    print(f"  • Revenue: {data['currency']} {data['revenue']:,}")
    print(f"  • Average per customer: {data['currency']} {data['revenue'] // data['customers'] if data['customers'] > 0 else 0:,}")

    print(f"\n✅ Regional report for {region} completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
