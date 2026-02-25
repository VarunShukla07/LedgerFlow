"""
Fintech Analytics Pipeline - CLEAN VERSION
Orchestrates existing scripts, doesn't embed logic
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import os

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'fintech_analytics_pipeline',
    default_args=default_args,
    description='Orchestrates Parquet → Postgres → dbt transformations',
    schedule_interval=None,  # Run once per day
    catchup=False,
    max_active_runs=1,
    tags=['fintech', 'etl', 'dbt'],
)


def check_parquet_files(**context):
    """Check if Parquet files exist"""
    parquet_path = '/opt/project/data/raw_transactions'
    
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_path}")
    
    parquet_files = []
    for root, dirs, files in os.walk(parquet_path):
        parquet_files.extend([f for f in files if f.endswith('.parquet')])
    
    file_count = len(parquet_files)
    logging.info(f"✅ Found {file_count} Parquet files")
    
    if file_count == 0:
        raise ValueError("❌ No Parquet files found!")
    
    return file_count


# Task 1: Check for data
check_data = PythonOperator(
    task_id='check_parquet_files',
    python_callable=check_parquet_files,
    dag=dag,
)

# Task 2: Load Parquet to PostgreSQL (calls existing script)
load_data = BashOperator(
    task_id='load_parquet_to_postgres',
    bash_command='docker exec spark-loader python3 /opt/spark-streaming/load_to_postgres.py',
    dag=dag,
)

# Task 3: Run dbt models
dbt_run = BashOperator(
    task_id='dbt_run_all',
    bash_command='cd /opt/project/transformation && dbt run --profiles-dir ./profiles',
    dag=dag,
)

# Task 4: Run dbt tests
dbt_test = BashOperator(
    task_id='dbt_test_all',
    bash_command='cd /opt/project/transformation && dbt test --profiles-dir ./profiles',
    dag=dag,
)

# Task 5: Generate summary (optional)
def generate_summary(**context):
    """Log pipeline completion"""
    logging.info("="*50)
    logging.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
    logging.info(f"Execution Date: {context['execution_date']}")
    logging.info("="*50)
    return "SUCCESS"

summary = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag,
)

# Define dependencies (clean and simple)
check_data >> load_data >> dbt_run >> dbt_test >> summary