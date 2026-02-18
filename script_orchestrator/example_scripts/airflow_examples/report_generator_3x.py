"""
Report generator using Airflow 3.x syntax with task dependencies.
Demonstrates parallel task execution, downstream aggregation, and asset production.
"""
from datetime import datetime

from airflow.sdk import Asset, dag, task


# Define the output asset
daily_report_asset = Asset("daily_report")


@dag(
    dag_id="daily_report_3x",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # Daily at 6 AM
    catchup=False,
    tags=["reporting", "3.x", "parallel"],
)
def daily_report_pipeline():
    """Generate daily report by gathering data from multiple sources."""

    @task
    def fetch_sales_data() -> dict:
        """Fetch sales data for the report."""
        print("Fetching sales data...")
        return {
            "source": "sales",
            "total": 15000,
            "count": 42,
        }

    @task
    def fetch_customer_data() -> dict:
        """Fetch customer data for the report."""
        print("Fetching customer data...")
        return {
            "source": "customers",
            "new_signups": 18,
            "active_users": 1250,
        }

    @task
    def fetch_inventory_data() -> dict:
        """Fetch inventory data for the report."""
        print("Fetching inventory data...")
        return {
            "source": "inventory",
            "low_stock_items": 5,
            "total_items": 320,
        }

    @task(outlets=[daily_report_asset])
    def generate_report(sales: dict, customers: dict, inventory: dict) -> str:
        """Generate the final report from all data sources and produce asset."""
        print("Generating daily report...")

        report = f"""
        ╔═══════════════════════════════════════╗
        ║         DAILY REPORT                  ║
        ╚═══════════════════════════════════════╝

        Sales Summary:
          • Total Revenue: ${sales['total']}
          • Transactions: {sales['count']}

        Customer Metrics:
          • New Signups: {customers['new_signups']}
          • Active Users: {customers['active_users']}

        Inventory Status:
          • Low Stock Items: {inventory['low_stock_items']}
          • Total Items: {inventory['total_items']}
        """

        print(report)
        print("✅ Report generated successfully")
        print("📦 Produced asset: daily_report")
        return report

    @task
    def send_report(report: str) -> None:
        """Send the report to stakeholders."""
        print("Sending report to stakeholders...")
        print("  📧 Sent to: management@example.com")
        print("  📧 Sent to: operations@example.com")
        print("✅ Report delivery complete")

    # Define task dependencies
    # Three parallel data fetching tasks
    sales = fetch_sales_data()
    customers = fetch_customer_data()
    inventory = fetch_inventory_data()

    # Aggregate results
    report = generate_report(sales, customers, inventory)

    # Send report
    send_report(report)


# Create the DAG instance
daily_report_pipeline()
