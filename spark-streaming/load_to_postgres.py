from pyspark.sql import SparkSession

print("🚀 Starting Parquet to PostgreSQL loader...")

spark = SparkSession.builder \
    .appName("LoadToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

df = spark.read.parquet("../data/raw_transactions")

count_parquet = df.count()
print(f"✅ Loaded {count_parquet} records from Parquet")

jdbc_url = "jdbc:postgresql://postgres:5432/airflow"

properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

df.write \
    .mode("overwrite") \
    .jdbc(
        url=jdbc_url,
        table="raw.raw_transactions",
        properties=properties
    )

print("✅ Data written to raw.raw_transactions")

df_verify = spark.read.jdbc(
    url=jdbc_url,
    table="raw.raw_transactions",
    properties=properties
)

print(f"✅ Verified {df_verify.count()} records")

spark.stop()