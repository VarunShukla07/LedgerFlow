## Component Decisions

### Kafka for ingestion — not direct database writes

Direct DB write pattern: `producer → INSERT INTO postgres`. Simple, but fragile.

With Kafka: `producer → Kafka topic → Spark consumer`. The topic is a durable buffer. If Spark goes down for 2 hours, Kafka holds every message. Spark resumes from its last checkpoint — no data loss, no re-sending from the producer. In production, multiple upstream services (mobile app, payment gateway, fraud detection) all publish to the same topic independently. That's not possible with direct writes.

The other benefit: replayability. Kafka retains messages for 7 days. If we discover a Spark bug that wrote corrupted data, we reset the consumer offset and reprocess without touching the producer.

### Spark Structured Streaming — not Flink or Kafka Streams

Flink is a legitimate alternative with lower latency. The decision to use Spark comes down to one thing for this project: Spark provides a **unified API for both streaming and batch**. The same DataFrame operations work on a streaming micro-batch and on a historical Parquet file. This matters because our Airflow batch jobs also use Spark for the load step — one library to understand instead of two.

Kafka Streams was ruled out because it runs inside the Java process — harder to containerize cleanly alongside a Python-first stack.

### Parquet for Bronze — not JSON or CSV

Raw Kafka messages are JSON. It would be natural to just write them as `.json` files. We don't, because:

Parquet is **columnar**. To calculate daily revenue, you read the `amount` column only — skipping `customer_email`, `ip_address`, `device_type`. With JSON (row format), you parse every field of every row to extract one column. At 100M rows, that difference is 10–50x query speed.

Parquet also supports **partition pruning**. `WHERE year=2024 AND month=1` reads only January files. The Airflow daily job processes only today's partition — never touches 3 years of history.

### PostgreSQL — not Redshift or BigQuery

Deliberate simplification for this project. A dedicated OLAP warehouse (Redshift, BigQuery, DuckDB) would give better analytical query performance at large scale. PostgreSQL gives the same logical structure — schemas, tables, indexes — without requiring cloud credentials or incurring cost during a portfolio demo.

The abstraction layers are already in place. Moving to Redshift means updating `profiles.yml` in dbt and the connection string in `load_to_postgres.py`. The SQL stays identical.

### dbt — not custom Python ETL scripts

Python ETL scripts are hard to test, hard to document, and impossible to lineage-trace. The classic pattern: a 400-line Python file that nobody wants to touch because no one knows what it does. dbt solves all three problems:

Testing is declarative — write `unique: true` in YAML and dbt generates and runs the test. Documentation lives in the same YAML file as the model definition. Lineage is automatically computed from `{{ ref() }}` tags — dbt generates a DAG of model dependencies that you can browse visually.

The other advantage: SQL is the lingua franca of data. Any analyst can read and contribute to a dbt model. A custom Python ETL script requires a Python developer.

### Incremental models with 3-day lookback

The naive approach: run dbt full refresh every night, reprocess all 50M rows. This takes hours and gets slower every day as data grows.

Incremental: process only records where `processing_date >= today - 3 days`. This stays fast as data grows — processing time is proportional to daily volume, not total history.

Why 3 days and not 1? Payment settlements are not instant. When you pay a merchant via UPI, your bank debits immediately. The merchant's bank may not receive the settlement for 24–72 hours. During that window, the transaction is `PENDING` in our system. When settlement arrives, we need to update it to `SUCCESS`. Without the 3-day lookback, `PENDING` records are frozen forever.

The dbt merge strategy (`unique_key = transaction_id`) handles this: if a transaction already exists in Gold → UPDATE it. If it's new → INSERT it.

---

## Failure Scenarios

### Spark crashes mid-batch

Before committing a batch, Spark writes the Kafka consumer offsets to `spark-streaming/checkpoints/`. If Spark crashes after writing some Parquet files but before committing the checkpoint, it restarts, reads the checkpoint, and reprocesses the entire batch from the last committed offset. Parquet writes are atomic per file (write to temp → rename). Partial files are abandoned and invisible to readers.

### Producer crashes mid-stream

Kafka retains all unread messages. When the producer recovers and resumes, Spark continues from where it left off. The Airflow `check_data_freshness` task detects if no new Parquet files appear within 2 hours and fails the DAG — alerting the team before anyone notices stale dashboards.

### PostgreSQL load fails halfway

`load_to_postgres.py` commits every 1,000 rows. A mid-job failure leaves a partial load. On Airflow retry, `ON CONFLICT (transaction_id) DO NOTHING` skips already-loaded rows. The job picks up where it left off. The `metadata.load_audit` table records `status = 'FAILED'` with the error message — always, even if the audit write itself fails (logged as `CRITICAL` in that case).

### dbt model fails

Airflow retries with exponential backoff (5 min, 10 min). The `on_failure_callback` logs the failure. Crucially: Gold tables from the previous successful run remain intact. dbt uses staging tables and swaps them atomically — a failed run never leaves Gold in a partially overwritten state.

### Duplicate Kafka messages (network retry)

Two deduplication layers. First: dbt `stg_transactions` uses `ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingestion_timestamp DESC)` — keeps only the latest copy per transaction. Second: PostgreSQL `ON CONFLICT (transaction_id) DO NOTHING` at insert time. Either layer alone would be sufficient; both together is belt-and-suspenders.

---

## Scaling Strategy

**10x traffic** — no architectural changes needed:
- Kafka: add partitions to the `transactions` topic. Each Spark executor reads one partition. 10 partitions = 10 parallel readers.
- Spark: increase `spark.executor.instances`. No code changes.
- PostgreSQL: add read replicas for analytical queries. Write path unchanged.

**100x traffic** — storage layer upgrade:
- Replace local Parquet with S3. Change the path prefix in `config.py`. Spark, Airflow, and dbt are unaware.
- Replace PostgreSQL with Redshift. Update `profiles.yml`. dbt SQL is unchanged.
- Move Airflow from LocalExecutor to CeleryExecutor + Redis. No DAG code changes.

**1000x traffic** — architecture change:
- Separate the real-time serving path (low-latency fraud scoring) from the batch analytics path (this pipeline). 
- Kafka remains the backbone. Add a second consumer group for real-time processing.
- Consider Apache Iceberg over Parquet for ACID transactions and time-travel queries on the data lake.
- Dedicated OLAP engine (ClickHouse or Redshift) for sub-second dashboard queries.

---

## Observability

| What to watch | Where to find it | Alert threshold |
|---|---|---|
| Kafka consumer lag | Kafka consumer group metrics | > 10,000 messages |
| Spark batch duration | Spark UI — http://localhost:4040 | > 5 minutes per batch |
| Load job row count | `SELECT * FROM metadata.load_audit ORDER BY created_at DESC` | `rows_loaded = 0` |
| Load job failures | Same table, `WHERE status = 'FAILED'` | Any failure |
| dbt test failures | Airflow task status — `dbt_test_*` tasks | Any red task |
| Fraud rate anomaly | `metadata.data_quality_metrics` | `fraud_count / rows_loaded > 0.10` |
| Pipeline SLA | Airflow DAG run duration | > 60 minutes |

---

## Trade-offs Made

| Decision | What we gained | What we gave up |
|---|---|---|
| Docker Compose over Kubernetes | Simpler local setup, no cloud needed | No auto-scaling, no rolling deployments |
| PostgreSQL over Redshift/BigQuery | Zero cloud dependency, no cost | Lower analytical query performance at large scale |
| Single Kafka broker | Simpler setup | No replication fault tolerance |
| LocalExecutor in Airflow | No Redis/Celery dependency | Can't scale to parallel workers |
| No Schema Registry | Less moving parts | No contract enforcement on Kafka messages |

These are deliberate, documented trade-offs — not oversights. Every one of them has a clear upgrade path.
