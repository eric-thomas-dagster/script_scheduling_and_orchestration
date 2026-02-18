#!/usr/bin/env python
"""Test script demonstrating sys.argv parameter extraction."""

import sys


def main():
    # Script expects: python script.py <input_file> <num_records>

    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = "default.txt"

    if len(sys.argv) > 2:
        num_records = int(sys.argv[2])
    else:
        num_records = 10

    print(f"Processing file: {input_file}")
    print(f"Number of records: {num_records}")
    print("Script completed successfully!")


if __name__ == "__main__":
    main()
