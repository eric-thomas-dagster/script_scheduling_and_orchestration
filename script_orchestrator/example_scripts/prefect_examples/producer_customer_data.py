"""
Producer flow: Extracts and processes customer data.
This flow generates customer data that downstream flows can consume.
"""
from prefect import flow, task


@task(log_prints=True)
def extract_customer_data():
    """Extract raw customer data from source."""
    print("Extracting customer data from database...")

    # Simulate data extraction
    raw_data = {
        "customers": [
            {"id": 1, "name": "Acme Corp", "region": "US", "tier": "enterprise"},
            {"id": 2, "name": "Global Industries", "region": "EU", "tier": "enterprise"},
            {"id": 3, "name": "Tech Startup", "region": "US", "tier": "standard"},
        ]
    }

    print(f"  ✅ Extracted {len(raw_data['customers'])} customers")
    return raw_data


@task(log_prints=True)
def validate_customer_data(raw_data: dict):
    """Validate customer data quality."""
    print("Validating customer data...")

    customers = raw_data["customers"]

    # Check for required fields
    required_fields = {"id", "name", "region", "tier"}
    for customer in customers:
        missing = required_fields - set(customer.keys())
        if missing:
            raise ValueError(f"Customer {customer.get('id')} missing fields: {missing}")

    print(f"  ✅ Validated {len(customers)} customers")
    return raw_data


@task(log_prints=True)
def process_customer_data(validated_data: dict):
    """Process and enrich customer data."""
    print("Processing customer data...")

    customers = validated_data["customers"]

    # Add calculated fields
    for customer in customers:
        # Add customer score based on tier
        customer["score"] = 100 if customer["tier"] == "enterprise" else 50
        # Add region code
        customer["region_code"] = customer["region"].lower()

    processed_data = {
        "customers": customers,
        "total_count": len(customers),
        "enterprise_count": sum(1 for c in customers if c["tier"] == "enterprise"),
    }

    print(f"  ✅ Processed {len(customers)} customers")
    print(f"  📊 Enterprise customers: {processed_data['enterprise_count']}")

    return processed_data


@flow(log_prints=True)
def produce_customer_data():
    """Producer flow: Extract, validate, and process customer data.

    This flow produces processed customer data that can be consumed by downstream flows.
    """
    print("╔═══════════════════════════════════════╗")
    print("║    CUSTOMER DATA PRODUCER FLOW       ║")
    print("╚═══════════════════════════════════════╝\n")

    # Execute pipeline
    raw_data = extract_customer_data()
    validated_data = validate_customer_data(raw_data)
    processed_data = process_customer_data(validated_data)

    print("\n✅ Customer data processing complete!")
    print(f"📦 Produced asset: processed_customer_data")

    return processed_data


if __name__ == "__main__":
    produce_customer_data()
