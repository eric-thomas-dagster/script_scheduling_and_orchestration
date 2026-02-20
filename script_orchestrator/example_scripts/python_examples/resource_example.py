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
    """Main function demonstrating resource usage (demo mode with graceful failures)."""
    print("🔍 Resource Example - Demonstrating resource detection")

    users = []
    data = []

    # Database operations (will fail gracefully if DB not running)
    print("\nConnecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='mydb',
            user='user',
            password='password',
            connect_timeout=3
        )
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users LIMIT 10")
        users = cursor.fetchall()
        print(f"✅ Fetched {len(users)} users from database")
        conn.close()
    except Exception as e:
        print(f"⚠️  PostgreSQL connection failed (demo mode): {str(e)[:80]}")
        users = [('user1',), ('user2',), ('user3',)]  # Mock data
        print(f"   Using mock data: {len(users)} users")

    # S3 operations (will fail gracefully without credentials)
    print("\nAccessing S3...")
    try:
        s3 = boto3.client('s3',
            aws_access_key_id='YOUR_KEY',
            aws_secret_access_key='YOUR_SECRET',
            region_name='us-east-1'
        )
        response = s3.list_buckets()
        print(f"✅ Found {len(response['Buckets'])} S3 buckets")
    except Exception as e:
        print(f"⚠️  S3 connection failed (demo mode): {str(e)[:80]}")
        print(f"   Would list S3 buckets with valid credentials")

    # API calls
    print("\nMaking HTTP request...")
    try:
        response = requests.get('https://httpbin.org/json', timeout=5)
        data = response.json()
        print(f"✅ Received data from API: {list(data.keys())}")
    except Exception as e:
        print(f"⚠️  HTTP request failed (demo mode): {str(e)[:80]}")
        data = {'items': [1, 2, 3]}  # Mock data
        print(f"   Using mock data")

    # Cache operations (will fail gracefully if Redis not running)
    print("\nConnecting to Redis...")
    try:
        r = redis.Redis(host='localhost', port=6379, password='password', socket_connect_timeout=3)
        r.set('processed_count', len(users))
        print(f"✅ Cached result in Redis")
    except Exception as e:
        print(f"⚠️  Redis connection failed (demo mode): {str(e)[:80]}")
        print(f"   Would cache {len(users)} items with Redis running")

    # Data quality checks
    assert len(users) > 0, "No users found"
    assert len(data) > 0, "No data received"

    print("\n✅ Script completed successfully")
    print(f"   Processed {len(users)} users")
    print(f"   Resources detected: psycopg2, boto3, requests, redis")

if __name__ == '__main__':
    main()
