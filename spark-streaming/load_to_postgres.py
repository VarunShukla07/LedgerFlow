from pyspark.sql import SparkSession
import sys
import psycopg2

print("🚀 Starting load job...")

spark = SparkSession.builder \
    .appName("LoadToPostgres") \
    .getOrCreate()

try:
    parquet_path = "/opt/data/raw_transactions"

    df = spark.read \
        .option("basePath", parquet_path) \
        .parquet(f"{parquet_path}/year=*/month=*/day=*")

    # Add Lightweight Dedup Before Write
    df = df.dropDuplicates(["transaction_id"])
    count = df.count()
    print(f"Loaded {count} records from parquet")

    if count == 0:
        print("No data found. Exiting.")
        spark.stop()
        sys.exit(0)

    # --- Ensure schema exists ---
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.close()
    conn.close()

    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    df.write \
        .mode("append") \
        .jdbc(
            url=jdbc_url,
            table="raw.raw_transactions",
            properties=properties
        )

    print("Data written to PostgreSQL successfully")

    spark.stop()
    sys.exit(0)

except Exception as e:
    print(f"Error: {e}")
    spark.stop()
    sys.exit(1)