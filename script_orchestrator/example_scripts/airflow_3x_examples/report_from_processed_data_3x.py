"""
Extended data pipeline showing multi-stage asset lineage.
This DAG consumes the processed_customer_data asset and generates a report.
"""
from datetime import datetime

from airflow.sdk import Asset, dag, task


# Import the asset from the previous stage
processed_data_asset = Asset("processed_customer_data")
customer_report_asset = Asset("customer_insights_report")


@dag(
    dag_id="generate_customer_report_3x",
    start_date=datetime(2024, 1, 1),
    schedule=[processed_data_asset],  # Triggered when processed_customer_data updates
    catchup=False,
    tags=["consumer", "3.x", "reporting", "cross-dag"],
)
def generate_customer_report():
    """Consumer DAG: Generate report from processed customer data.

    This demonstrates cross-DAG lineage:
    extract_customer_data_3x → process_customer_data_3x → generate_customer_report_3x
    """

    @task
    def analyze_customers():
        """Analyze processed customer data."""
        print("Analyzing customer patterns...")

        # Simulate analysis
        insights = {
            "total_customers": 150,
            "high_value_customers": 42,
            "churn_risk": 8,
            "growth_rate": "12%"
        }

        print(f"  • Total customers: {insights['total_customers']}")
        print(f"  • High-value customers: {insights['high_value_customers']}")
        print(f"  • Churn risk: {insights['churn_risk']}")

        return insights

    @task(outlets=[customer_report_asset])
    def create_report(insights: dict):
        """Create customer insights report and produce asset."""
        print("Generating customer insights report...")

        report = f"""
        ╔═══════════════════════════════════════╗
        ║    CUSTOMER INSIGHTS REPORT           ║
        ╚═══════════════════════════════════════╝

        Customer Metrics:
          • Total Customers: {insights['total_customers']}
          • High-Value Segment: {insights['high_value_customers']} customers
          • At-Risk Customers: {insights['churn_risk']}
          • Growth Rate: {insights['growth_rate']}

        ═══════════════════════════════════════
        """

        print(report)
        print("✅ Customer insights report generated")
        print("📦 Produced asset: customer_insights_report")

        return report

    # Define task flow
    insights = analyze_customers()
    create_report(insights)


# Create DAG instance
generate_customer_report()
