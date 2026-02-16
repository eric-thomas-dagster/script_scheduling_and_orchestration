#!/usr/bin/env python
"""Test script demonstrating argparse parameter extraction."""

import argparse


def main():
    parser = argparse.ArgumentParser(description="Test script with argparse")

    parser.add_argument(
        '--input-file',
        type=str,
        default='data.csv',
        help='Input data file path'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum number of records to process'
    )

    parser.add_argument(
        '--verbose',
        type=bool,
        default=False,
        help='Enable verbose output'
    )

    args = parser.parse_args()

    print(f"Processing {args.input_file}")
    print(f"Limit: {args.limit}")
    print(f"Verbose: {args.verbose}")
    print("Script completed successfully!")


if __name__ == "__main__":
    main()
