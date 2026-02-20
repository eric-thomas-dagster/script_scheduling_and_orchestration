"""
Example tasks for basic_example_dag.yaml

These are the Python callables referenced in the dag-factory YAML file.
They demonstrate:
- Simple data extraction returning lists
- XCom-style data passing between tasks
- Jinja template parameter usage
"""


def _extract_data() -> list[int]:
    """Extract data from a source.

    This simulates extracting data from a database or API.
    In a real scenario, this would connect to a data source.

    Returns:
        List of integers representing extracted data
    """
    return [1, 2, 3, 4]


def _store_data(processed_at: str, data_a: list[int], data_b: list[int]) -> None:
    """Store combined data from multiple sources.

    This receives data from two upstream extraction tasks via XCom
    (data_a and data_b parameters with + syntax in YAML).

    Args:
        processed_at: Timestamp from Jinja template {{ logical_date }}
        data_a: Data from extract_data_from_a task
        data_b: Data from extract_data_from_b task
    """
    combined_records = data_a + data_b
    print(f"Storing {len(combined_records)} records at {processed_at}")
    print(f"  Data A: {data_a}")
    print(f"  Data B: {data_b}")
    print(f"  Combined: {combined_records}")

    # In a real implementation, this would write to a database or data warehouse
    # For example:
    # db.insert_many('processed_data', combined_records, timestamp=processed_at)
