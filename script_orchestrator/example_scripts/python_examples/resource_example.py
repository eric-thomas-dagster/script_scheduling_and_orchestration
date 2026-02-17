"""
Example script demonstrating resource auto-detection.

This script uses multiple external services that will be automatically
detected and converted to Dagster resources.

Owner: engineering@company.com
Tags: example, resources, database, storage, api
"""

import psycopg2
import boto3
import requests
import redis

def main():
    """Main function demonstrating resource usage."""

    # Database operations
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='mydb',
        user='user',
        password='password'
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users LIMIT 10")
    users = cursor.fetchall()
    print(f"Fetched {len(users)} users from database")

    # S3 operations
    print("Accessing S3...")
    s3 = boto3.client('s3',
        aws_access_key_id='YOUR_KEY',
        aws_secret_access_key='YOUR_SECRET',
        region_name='us-east-1'
    )

    # List buckets
    response = s3.list_buckets()
    print(f"Found {len(response['Buckets'])} S3 buckets")

    # API calls
    print("Making HTTP request...")
    response = requests.get('https://api.example.com/data')
    data = response.json()
    print(f"Received {len(data)} items from API")

    # Cache operations
    print("Connecting to Redis...")
    r = redis.Redis(host='localhost', port=6379, password='password')
    r.set('processed_count', len(users))
    print(f"Cached result in Redis")

    # Data quality checks
    assert len(users) > 0, "No users found in database"
    assert len(data) > 0, "No data received from API"

    print("✅ Script completed successfully")

if __name__ == '__main__':
    main()
