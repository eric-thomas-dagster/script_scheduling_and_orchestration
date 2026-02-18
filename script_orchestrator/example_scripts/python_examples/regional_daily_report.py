#!/usr/bin/env python
"""Regional daily report demonstrating config + partition together.

This script shows how to use both Dagster config (via argparse) AND partitions.
- Config: output_dir, email_recipients (from Dagster config)
- Partition: region (passed as --region argument)
"""

import argparse
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Regional daily report with config")

    # Config parameters (from Dagster config)
    parser.add_argument(
        '--output-dir',
        type=str,
        default='/tmp/reports',
        help='Output directory for reports'
    )

    parser.add_argument(
        '--email-recipients',
        type=str,
        default='team@example.com',
        help='Comma-separated email recipients'
    )

    # Partition parameter (from Dagster partition key)
    parser.add_argument(
        '--region',
        type=str,
        required=True,
        help='Region to process (from partition key)'
    )

    args = parser.parse_args()

    print(f"=" * 70)
    print(f"Regional Daily Report - {args.region.upper()}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"=" * 70)

    # Simulate region-specific processing
    region_data = {
        "us": {"sales": 125000, "orders": 450},
        "uk": {"sales": 98000, "orders": 320},
        "de": {"sales": 112000, "orders": 380},
        "jp": {"sales": 145000, "orders": 520},
    }

    data = region_data.get(args.region, {"sales": 0, "orders": 0})

    print(f"\n📊 Report for {args.region}:")
    print(f"  • Sales: ${data['sales']:,}")
    print(f"  • Orders: {data['orders']:,}")
    print(f"  • Avg order value: ${data['sales'] // data['orders'] if data['orders'] > 0 else 0:,}")

    print(f"\n💾 Output:")
    print(f"  • Directory: {args.output_dir}")
    print(f"  • File: {args.output_dir}/report_{args.region}_{datetime.now().strftime('%Y%m%d')}.pdf")

    print(f"\n📧 Email:")
    recipients = args.email_recipients.split(',')
    for recipient in recipients:
        print(f"  • Sending to: {recipient.strip()}")

    print(f"\n✅ Regional report for {args.region} completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
