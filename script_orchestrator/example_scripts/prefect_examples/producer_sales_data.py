"""
Producer flow: Extracts and processes sales data.
This flow generates sales data that downstream flows can consume.
"""
from prefect import flow, task


@task(log_prints=True)
def extract_sales_data():
    """Extract raw sales data from source."""
    print("Extracting sales data from API...")

    # Simulate data extraction
    raw_data = {
        "sales": [
            {"id": 1, "customer_id": 1, "amount": 50000, "quarter": "Q1"},
            {"id": 2, "customer_id": 2, "amount": 75000, "quarter": "Q1"},
            {"id": 3, "customer_id": 1, "amount": 60000, "quarter": "Q2"},
            {"id": 4, "customer_id": 3, "amount": 15000, "quarter": "Q2"},
        ]
    }

    print(f"  ✅ Extracted {len(raw_data['sales'])} sales records")
    return raw_data


@task(log_prints=True)
def aggregate_sales_data(raw_data: dict):
    """Aggregate sales data by customer and quarter."""
    print("Aggregating sales data...")

    sales = raw_data["sales"]

    # Aggregate by customer
    customer_totals = {}
    for sale in sales:
        customer_id = sale["customer_id"]
        if customer_id not in customer_totals:
            customer_totals[customer_id] = {"total": 0, "count": 0}
        customer_totals[customer_id]["total"] += sale["amount"]
        customer_totals[customer_id]["count"] += 1

    # Calculate overall metrics
    total_revenue = sum(sale["amount"] for sale in sales)
    average_deal_size = total_revenue / len(sales) if sales else 0

    aggregated_data = {
        "sales": sales,
        "customer_totals": customer_totals,
        "metrics": {
            "total_revenue": total_revenue,
            "total_deals": len(sales),
            "average_deal_size": average_deal_size,
        }
    }

    print(f"  ✅ Aggregated sales for {len(customer_totals)} customers")
    print(f"  💰 Total revenue: ${total_revenue:,}")

    return aggregated_data


@flow(log_prints=True)
def produce_sales_data():
    """Producer flow: Extract and aggregate sales data.

    This flow produces processed sales data that can be consumed by downstream flows.
    """
    print("╔═══════════════════════════════════════╗")
    print("║     SALES DATA PRODUCER FLOW         ║")
    print("╚═══════════════════════════════════════╝\n")

    # Execute pipeline
    raw_data = extract_sales_data()
    aggregated_data = aggregate_sales_data(raw_data)

    print("\n✅ Sales data processing complete!")
    print(f"📦 Produced asset: processed_sales_data")

    return aggregated_data


if __name__ == "__main__":
    produce_sales_data()
