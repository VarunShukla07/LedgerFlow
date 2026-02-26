from pyspark.sql import SparkSession
import sys
import psycopg2
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("🚀 Starting load job...")

spark = SparkSession.builder \
    .appName("LoadToPostgres") \
    .getOrCreate()

try:
    parquet_path = "/opt/data/raw_transactions"

    # --------------------------------------
    # CONNECT FIRST (so we get result first)
    # --------------------------------------
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Ensure schemas
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS metadata;")

    # Ensure audit table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS metadata.load_audit (
        id SERIAL PRIMARY KEY,
        table_name TEXT NOT NULL,
        last_loaded_year INT,
        last_loaded_month INT,
        last_loaded_day INT,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        rows_loaded INT,
        status TEXT
    );
    """)

    # Get last successful load
    cur.execute("""
    SELECT last_loaded_year, last_loaded_month, last_loaded_day
    FROM metadata.load_audit
    WHERE table_name = 'raw_transactions'
    AND status = 'SUCCESS'
    ORDER BY id DESC
    LIMIT 1;
    """)

    result = cur.fetchone()

    # --------------------------------------
    # NOW READ PARQUET
    # --------------------------------------
    df = spark.read \
        .option("basePath", parquet_path) \
        .parquet(f"{parquet_path}/year=*/month=*/day=*")

    # Apply incremental filter
    if result:
        last_year, last_month, last_day = result
        df = df.filter(
            (df.year > last_year) |
            ((df.year == last_year) & (df.month > last_month)) |
            ((df.year == last_year) & (df.month == last_month) & (df.day > last_day))
        )
        logger.info(f"Loading only partitions after {last_year}-{last_month}-{last_day}")
    else:
        logger.info("No previous load found. Full load.")

    # Dedup
    df = df.dropDuplicates(["transaction_id"])

    count = df.count()
    logger.info(f"Loaded {count} records from parquet")

    if count == 0:
        logger.info("No data found. Exiting.")
        spark.stop()
        conn.close()
        sys.exit(0)

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

    # Insert audit record
    max_partition = df.select("year", "month", "day") \
        .orderBy("year", "month", "day", ascending=False) \
        .first()

    cur.execute("""
        INSERT INTO metadata.load_audit (
            table_name,
            last_loaded_year,
            last_loaded_month,
            last_loaded_day,
            rows_loaded,
            status
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """, (
        "raw_transactions",
        int(max_partition.year),
        int(max_partition.month),
        int(max_partition.day),
        count,
        "SUCCESS"
    ))

    logger.info("Data written to PostgreSQL successfully")

    cur.close()
    conn.close()
    spark.stop()
    sys.exit(0)

except Exception as e:
    logger.error(f"Error: {e}")
    spark.stop()
    sys.exit(1)