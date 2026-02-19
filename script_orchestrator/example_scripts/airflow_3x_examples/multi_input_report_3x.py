"""
Test DAG with multiple inlet datasets (multiple upstream dependencies).
This DAG waits for BOTH customer data AND sales data before generating a combined report.
"""
from datetime import datetime

from airflow.sdk import Asset, dag, task


# Define the inlet assets from upstream DAGs
customer_data_asset = Asset("processed_customer_data")
sales_data_asset = Asset("processed_sales_data")
financial_report_asset = Asset("financial_report")


@dag(
    dag_id="multi_input_report_3x",
    start_date=datetime(2024, 1, 1),
    schedule=[customer_data_asset, sales_data_asset],  # Wait for BOTH assets
    catchup=False,
    tags=["multi-input", "3.x", "reporting", "test"],
)
def generate_combined_report():
    """Consumer DAG: Generate report that requires multiple upstream datasets.

    This demonstrates multi-input lineage:
    - Waits for processed_customer_data AND processed_sales_data
    - Generates combined financial report
    """

    @task
    def merge_datasets():
        """Merge customer and sales data."""
        print("Merging customer and sales datasets...")

        # Simulate merging
        merged_data = {
            "customers": 150,
            "sales": 45000,
            "average_order_value": 300
        }

        print(f"  • Merged {merged_data['customers']} customers")
        print(f"  • Total sales: ${merged_data['sales']}")

        return merged_data

    @task(outlets=[financial_report_asset])
    def create_financial_report(merged_data: dict):
        """Create financial report and produce asset."""
        print("Generating financial report from merged data...")

        report = f"""
        ╔═══════════════════════════════════════╗
        ║    COMBINED FINANCIAL REPORT          ║
        ╚═══════════════════════════════════════╝

        Customer Metrics:
          • Total Customers: {merged_data['customers']}

        Sales Metrics:
          • Total Sales: ${merged_data['sales']}
          • Average Order Value: ${merged_data['average_order_value']}

        ═══════════════════════════════════════
        """

        print(report)
        print("✅ Financial report generated")
        print("📦 Produced asset: financial_report")

        return report

    # Define task flow
    merged = merge_datasets()
    create_financial_report(merged)


# Create DAG instance
generate_combined_report()
