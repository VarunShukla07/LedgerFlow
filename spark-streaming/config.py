"""
Configuration for Spark Streaming Application
"""

import os
import logging
from pathlib import Path

#loggin setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
DATA_LAKE_PATH = PROJECT_ROOT / "data"
CHECKPOINTS_PATH = PROJECT_ROOT / "spark-streaming" / "checkpoints"

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

# Output Paths
RAW_TRANSACTIONS_PATH = DATA_LAKE_PATH / "raw_transactions"
DEAD_LETTER_PATH = DATA_LAKE_PATH / "dead_letter_queue"

# Spark Configuration
SPARK_APP_NAME = "FinTech-Transaction-Streaming"
TRIGGER_INTERVAL_SECONDS = 30
STATS_INTERVAL_SECONDS = 60

# Logging
LOG_LEVEL = "INFO"

# Transaction Schema
EXPECTED_SCHEMA = {
    "transaction_id": "string",
    "customer_id": "string",
    "customer_name": "string",
    "customer_email": "string",
    "merchant_name": "string",
    "merchant_category": "string",
    "amount": "double",
    "payment_method": "string",
    "status": "string",
    "failure_reason": "string",
    "timestamp": "string",
    "city": "string",
    "state": "string",
    "country": "string",
    "currency": "string",
    "device_type": "string",
    "ip_address": "string",
    "is_fraud": "integer"
}


class Config:
    """Configuration manager for Spark Streaming"""
    
    def __init__(self):
        self.project_root = PROJECT_ROOT
        self.data_lake_path = DATA_LAKE_PATH
        self.checkpoints_path = CHECKPOINTS_PATH
        self.raw_transactions_path = RAW_TRANSACTIONS_PATH
        self.dead_letter_path = DEAD_LETTER_PATH
        
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.kafka_topic = KAFKA_TOPIC
        
        self.spark_app_name = SPARK_APP_NAME
        self.trigger_interval_seconds = TRIGGER_INTERVAL_SECONDS
        self.stats_interval_seconds = STATS_INTERVAL_SECONDS
        
        self.log_level = LOG_LEVEL
        self.expected_schema = EXPECTED_SCHEMA
    
    def get_spark_config(self):
        """Get Spark configuration dictionary"""
        return {
            "spark.app.name": self.spark_app_name,
            "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "spark.sql.shuffle.partitions": "2",
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.streaming.checkpointLocation.allowMismatch": "true"
        }
    
    def get_kafka_options(self):
        """Get Kafka connection options"""
        return {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            "subscribe": self.kafka_topic,
            "startingOffsets": "latest",
            "failOnDataLoss": "false"
        }
    
    def validate(self):
        """Validate configuration"""
        logger.info(f"✓ Kafka Servers: {self.kafka_bootstrap_servers}")
        logger.info(f"✓ Kafka Topic: {self.kafka_topic}")
        logger.info(f"✓ Output Path: {self.raw_transactions_path}")
        logger.info(f"✓ Checkpoint Path: {self.checkpoints_path}")
        return True
    
    def ensure_directories(self):
        """Create required directories"""
        self.raw_transactions_path.mkdir(parents=True, exist_ok=True)
        self.dead_letter_path.mkdir(parents=True, exist_ok=True)
        self.checkpoints_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ Created directories")
    
    def print_config(self):
        """Logging configuration summary"""
        logger.info("=" * 60)
        logger.info("SPARK STREAMING CONFIGURATION")
        logger.info("=" * 60)
        logger.info(f"App Name: {self.spark_app_name}")
        logger.info(f"Kafka: {self.kafka_bootstrap_servers}")
        logger.info(f"Topic: {self.kafka_topic}")
        logger.info(f"Output: {self.raw_transactions_path}")
        logger.info(f"Trigger: Every {self.trigger_interval_seconds} seconds")
        logger.info("=" * 60)


# Create global config instance
config = Config()