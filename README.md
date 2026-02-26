# Fintech Real-Time Analytics Platform

A production-grade data engineering pipeline simulating how a fintech company ingests, processes, and analyzes payment transactions in real time. Built with Kafka, Spark Structured Streaming, dbt, and Airflow — all running locally via Docker Compose.

---

## What This Project Does

A Python producer simulates a payment gateway by streaming transactions into Kafka. Apache Spark consumes the stream in 30-second micro-batches and writes partitioned Parquet files to a local data lake (Bronze layer). Airflow triggers a dbt pipeline that cleans, enriches, and aggregates the data through Silver and Gold layers in PostgreSQL. Every run is audited — row counts, duration, and failures are recorded in a metadata schema.

---

## Architecture

> See `docs/architecture.md` for the visual diagram layout.
> See `docs/component_decision.md` for full component decisions, failure scenarios, and scaling strategy.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                            │
│  Kaggle fraudTrain.csv (70%)  +  Faker synthetic Indian payments (30%)   │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │ producer.py  (kafka-python)
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  KAFKA  —  topic: transactions                                           │
│  Single broker · Port 9092 · 7-day retention                             │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │ Spark Structured Streaming
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Parquet · partitioned by year/month/day)                 │
│  data/raw_transactions/year=*/month=*/day=*/                             │
│  data/dead_letter_queue/  ← malformed records                            │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │ load_to_postgres.py  (Airflow task)
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (PostgreSQL · schema: raw)                                │
│  raw.transactions  ← ON CONFLICT DO NOTHING (idempotent)                 │
│  metadata.load_audit          ← every run logged (SUCCESS / FAILED)      │
│  metadata.data_quality_metrics ← duplicates, nulls, invalids per run     │
└─────────────────────────────┬────────────────────────────────────────────┘
                              │ dbt run  (Airflow TaskGroups)
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (PostgreSQL · schema: marts)                                │
│  staging/   stg_transactions · stg_merchants                             │
│  intermediate/  int_transactions_enriched  (time features · city tier)   │
│  marts/     fact_transactions  ← incremental · 3-day lookback            │
│             dim_customers · dim_merchants · daily_revenue_summary        │
└──────────────────────────────────────────────────────────────────────────┘
                              ▲
                    Airflow orchestrates everything
                    right of the Bronze write
```

---

## Tech Stack

| Layer | Technology | Version | Why This Choice |
|---|---|---|---|
| Event Streaming | Apache Kafka (Confluent) | 7.5.0 | Durable buffer, replayable, decouples producer from consumers |
| Stream Processing | Apache Spark Structured Streaming | 3.5.0 | Exactly-once semantics, unified batch+stream API |
| Orchestration | Apache Airflow | 2.7.0 | DAG scheduling, retry logic, TaskGroup UI organization |
| Transformation | dbt (dbt-postgres) | 1.7.x | Version-controlled SQL, built-in testing, lineage graph |
| Warehouse | PostgreSQL | 14 | Sufficient for this scale, zero cloud dependency |
| Storage Format | Apache Parquet | — | Columnar, partition pruning, Spark-native |
| Data Generation | kafka-python + Faker | — | Realistic Indian payment context |
| Infrastructure | Docker Compose | — | Fully reproducible local setup |

---

## Data Flow: Bronze → Silver → Gold

**Bronze** is immutable. Exactly what Kafka delivered — duplicates, nulls, wrong types and all. Stored as Parquet partitioned by date so Spark and Airflow never scan historical data they don't need. If transformation bugs corrupt downstream layers, Bronze is the recovery point.

**Silver** is cleaned. `load_to_postgres.py` validates (no negative amounts, no null customer IDs), deduplicates within each batch, and loads idempotently. Every run — success or failure — is recorded in `metadata.load_audit` with row counts and duration. This prevents silent failures.

**Gold** is business-ready. dbt models join, enrich, and aggregate. `fact_transactions` is incremental with a 3-day lookback to handle late-arriving payment settlements (banks often settle T+1 to T+2 days after the transaction). The merge strategy on `transaction_id` corrects PENDING → SUCCESS status updates automatically.

---

## Project Structure

```
fintech-analytics/
├── data-generator/
│   ├── producer.py          # Hybrid Kafka producer (Kaggle + Faker)
│   ├── kaggle_loader.py     # Loads and iterates fraudTrain.csv
│   └── data/fraudTrain.csv  # Download separately (see Setup)
│
├── spark-streaming/
│   ├── stream_processor.py  # Kafka → Parquet with dead letter queue
│   ├── load_to_postgres.py  # Bronze → Silver with audit + quality metrics
│   └── config.py
│
├── transformation/          # dbt project (equiv. to dbt/)
│   └── models/
│       ├── staging/         # stg_transactions, stg_merchants
│       ├── intermediate/    # int_transactions_enriched
│       └── marts/           # fact_transactions, dims, daily_revenue_summary
│
├── airflow/
│   └── dags/
│       └── pipeline.py  # Main DAG with 4 TaskGroups
│
├── data/
│   ├── raw_transactions/    # Bronze Parquet files
│   ├── batch_files/         # Reconciliation CSVs
│   └── dead_letter_queue/   # Malformed Kafka messages
│
├── docs/
│   ├── architecture.md
│   └── component_decision.md
│
├── scripts/
│   ├── setup.sh
│   ├── start.sh
│   └── stop.sh
│
└── docker-compose.yml
```

---

## Setup

### Prerequisites
- Docker Desktop (minimum 4 GB RAM allocated)
- Python 3.11+
- Kaggle account and API key

### Step 1 — Clone and configure

```bash
git clone <repo-url>
cd fintech-analytics
cp .env.example .env
```

### Step 2 — Download the Kaggle dataset

```bash
pip install kaggle
mkdir -p ~/.kaggle
# Place your kaggle.json API token in ~/.kaggle/kaggle.json
kaggle datasets download -d kartik2112/fraud-detection
unzip fraud-detection.zip -d data-generator/data/
```

### Step 3 — First-time setup

```bash
chmod +x scripts/*.sh
./scripts/setup.sh
# Pulls Docker images, creates Kafka topic, initialises Airflow DB
```

### Step 4 — Start the pipeline

```bash
# Terminal 1: Start infrastructure
./scripts/start.sh

# Terminal 2: Start Kafka producer
cd data-generator && pip install -r requirements.txt
python producer.py --rate 2.0

# Terminal 3: Start Spark streaming
cd spark-streaming && pip install -r requirements.txt
python stream_processor.py
```

Airflow runs the dbt pipeline automatically at 2 AM. To trigger manually: Airflow UI → `fintech_analytics_pipeline` → **Trigger DAG**.

---

## Accessing Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark Streaming UI | http://localhost:4040 | — |
| PostgreSQL | localhost:5432 | airflow / airflow / airflow (db: airflow) |
| Kafka Broker | localhost:9092 | — |

---

## Key Engineering Highlights

**Incremental processing with late data handling.**
`fact_transactions` uses dbt’s `merge` strategy with a 3-day lookback window. In real payment systems, settlement updates can arrive 1–3 days after the original transaction. Without this window, `PENDING` records would never be updated to `SUCCESS`.

**Silent failure prevention.**
Every load writes to `metadata.load_audit`, even on failure. This allows detecting both explicit failures (`status = 'FAILED'`) and silent ones (`rows_loaded = 0 AND status = 'SUCCESS'`).

**Layered data quality checks.**
Validation exists at multiple levels:

* PostgreSQL constraints (`CHECK amount > 0`)
* dbt schema + custom tests
* `data_quality_metrics` table for monitoring trends

**Multi-layer deduplication.**
Duplicates are handled at three stages:

* Spark checkpointing
* dbt staging (`ROW_NUMBER()` logic)
* PostgreSQL `ON CONFLICT DO NOTHING`

Each layer protects against different failure scenarios.

---

## Potential Enhancements

* Use **Apache Iceberg** instead of Parquet for ACID and time travel.
* Add **Schema Registry** for Kafka message validation.
* Deploy to **AWS (S3 + Redshift + MWAA)**.
* Add **Prometheus + Grafana** for pipeline monitoring.
* Integrate **Great Expectations** for deeper data profiling.

---

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — Visual Diagram Layout
- [`docs/component_decision.md`](docs/component_decision.md) — Component decisions, failure scenarios, scaling strategy
