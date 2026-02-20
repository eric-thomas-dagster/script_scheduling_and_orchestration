"""
Consumer flow: Generates combined report from customer and sales data.
This flow consumes data produced by both customer and sales producer flows.
"""
from prefect import flow, task


@task(log_prints=True)
def merge_customer_and_sales_data():
    """Merge customer and sales data from upstream flows.

    In a real implementation, this would read from shared storage or a database.
    For this example, we simulate having access to the upstream data.
    """
    print("Merging customer and sales data...")

    # Simulate reading from upstream assets
    # In production, this would read from S3, database, or other shared storage
    customer_data = {
        "total_count": 3,
        "enterprise_count": 2,
    }

    sales_data = {
        "metrics": {
            "total_revenue": 200000,
            "total_deals": 4,
            "average_deal_size": 50000,
        }
    }

    merged_data = {
        "customers": customer_data,
        "sales": sales_data,
    }

    print(f"  ✅ Merged data from both upstream sources")
    return merged_data


@task(log_prints=True)
def calculate_customer_metrics(merged_data: dict):
    """Calculate per-customer metrics."""
    print("Calculating customer metrics...")

    customer_count = merged_data["customers"]["total_count"]
    total_revenue = merged_data["sales"]["metrics"]["total_revenue"]

    metrics = {
        "revenue_per_customer": total_revenue / customer_count if customer_count > 0 else 0,
        "customer_count": customer_count,
        "total_revenue": total_revenue,
    }

    print(f"  💰 Revenue per customer: ${metrics['revenue_per_customer']:,.2f}")

    return metrics


@task(log_prints=True)
def generate_executive_report(customer_metrics: dict, merged_data: dict):
    """Generate executive summary report."""
    print("Generating executive report...")

    sales_metrics = merged_data["sales"]["metrics"]
    enterprise_count = merged_data["customers"]["enterprise_count"]

    report = f"""
    ╔═══════════════════════════════════════════════╗
    ║       EXECUTIVE SUMMARY REPORT               ║
    ╚═══════════════════════════════════════════════╝

    CUSTOMER OVERVIEW:
      • Total Customers: {customer_metrics['customer_count']}
      • Enterprise Customers: {enterprise_count}
      • Revenue per Customer: ${customer_metrics['revenue_per_customer']:,.2f}

    SALES PERFORMANCE:
      • Total Revenue: ${sales_metrics['total_revenue']:,}
      • Total Deals: {sales_metrics['total_deals']}
      • Average Deal Size: ${sales_metrics['average_deal_size']:,.2f}

    KEY INSIGHTS:
      • Enterprise customers represent {enterprise_count}/{customer_metrics['customer_count']} of customer base
      • Average revenue per customer: ${customer_metrics['revenue_per_customer']:,.2f}

    ═══════════════════════════════════════════════
    """

    print(report)
    print("✅ Executive report generated")
    print("📦 Produced asset: executive_summary_report")

    return report


@flow(log_prints=True)
def generate_combined_report():
    """Consumer flow: Generate executive report from customer and sales data.

    This flow depends on:
    - processed_customer_data (from produce_customer_data flow)
    - processed_sales_data (from produce_sales_data flow)

    It demonstrates multi-input dependencies in the lineage graph.
    """
    print("╔═══════════════════════════════════════╗")
    print("║   COMBINED REPORT CONSUMER FLOW      ║")
    print("╚═══════════════════════════════════════╝\n")

    # Execute pipeline
    merged_data = merge_customer_and_sales_data()
    customer_metrics = calculate_customer_metrics(merged_data)
    report = generate_executive_report(customer_metrics, merged_data)

    print("\n✅ Combined report generation complete!")

    return report


if __name__ == "__main__":
    generate_combined_report()
