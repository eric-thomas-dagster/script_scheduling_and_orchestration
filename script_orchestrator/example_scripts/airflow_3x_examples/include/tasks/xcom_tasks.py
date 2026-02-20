"""
Tasks demonstrating XCom data passing.

These functions show how data flows between tasks using the + syntax in YAML.
"""

from typing import List


def _extract_data_from_a() -> List[int]:
    """Extract data from source A.

    Returns:
        List of integers representing data from source A
    """
    print("Extracting data from source A...")
    data = [1, 2, 3, 4, 5]
    print(f"Extracted {len(data)} items from source A")
    return data


def _extract_data_from_b() -> List[int]:
    """Extract data from source B.

    Returns:
        List of integers representing data from source B
    """
    print("Extracting data from source B...")
    data = [10, 20, 30, 40, 50]
    print(f"Extracted {len(data)} items from source B")
    return data


def _process_combined_data(data_a: List[int], data_b: List[int]) -> dict:
    """Process data from both sources.

    This function receives data via XCom from upstream tasks.

    Args:
        data_a: Data from extract_data_a task (via +extract_data_a)
        data_b: Data from extract_data_b task (via +extract_data_b)

    Returns:
        Dictionary with processed results
    """
    print(f"Processing data - A: {len(data_a)} items, B: {len(data_b)} items")

    # Combine the data
    combined = data_a + data_b
    total = sum(combined)
    average = total / len(combined)

    result = {
        'count_a': len(data_a),
        'count_b': len(data_b),
        'total_count': len(combined),
        'sum': total,
        'average': average,
    }

    print(f"Processed results: {result}")
    return result


def _store_results(processed_data: dict) -> None:
    """Store the processed results.

    Args:
        processed_data: Results from process_data task (via +process_data)
    """
    print("Storing results...")
    print(f"Results: {processed_data}")
    print(f"Total items processed: {processed_data['total_count']}")
    print(f"Average value: {processed_data['average']:.2f}")
    print("✅ Results stored successfully")
