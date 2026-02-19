"""
Spark Structured Streaming Processor for Fintech Analytics Platform

This module processes real-time payment transactions from Kafka,
validates the data, adds metadata, and writes to the data lake
in partitioned Parquet format.
"""

import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, 
    date_format, lit, when, isnull, count, sum, avg,
    year, month, dayofmonth, hour, minute, second
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)
from pyspark.sql.streaming import StreamingQuery
import pyspark.sql.functions as F

from config import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionStreamProcessor:
    """Processes transaction streams from Kafka using Spark Structured Streaming."""
    
    def __init__(self):
        """Initialize the stream processor."""
        self.spark = None
        self.query = None
        self.running = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def _create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.
        
        Returns:
            Configured SparkSession
        """
        logger.info("Creating Spark session...")
        
        # Build Spark session with configuration
        builder = SparkSession.builder
        
        # Apply all Spark configurations
        spark_config = config.get_spark_config()
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        # Create session
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel(config.log_level)
        
        logger.info("Spark session created successfully")
        return spark
    
    def _get_transaction_schema(self) -> StructType:
        schema_fields = []

        type_mapping = {
            "string": StringType(),
            "double": DoubleType(),
            "integer": IntegerType(),
            "timestamp": TimestampType()
        }

        for field_name, field_type in config.expected_schema.items():
            spark_type = type_mapping.get(field_type, StringType())
            schema_fields.append(StructField(field_name, spark_type, True))

        return StructType(schema_fields)
        
    def _parse_json_data(self, df):
        """
        Parse JSON data from Kafka messages.
        
        Args:
            df: Input DataFrame with JSON string in 'value' column
            
        Returns:
            DataFrame with parsed transaction data
        """
        # Define the expected schema
        transaction_schema = self._get_transaction_schema()
        
        # Parse JSON and apply schema
        parsed_df = df.select(
            from_json(col("value").cast("string"), transaction_schema).alias("transaction"),
            col("timestamp").alias("kafka_timestamp"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition")
        ).select(
            "transaction.*",
            "kafka_timestamp",
            "kafka_offset", 
            "kafka_partition"
        )
        
        return parsed_df
    
    def _validate_and_clean_data(self, df):
        """
        Validate and clean the transaction data.
        
        Args:
            df: Input DataFrame with parsed transaction data
            
        Returns:
            Cleaned DataFrame with validation flags
        """
        # Add validation flags
        validated_df = df.withColumn("is_valid", lit(True))
        
        # Check for required fields
        required_fields = ['transaction_id', 'customer_id', 'amount', 'timestamp']
        
        for field in required_fields:
            validated_df = validated_df.withColumn(
                "is_valid",
                when(col(field).isNull() | (col(field) == ""), False)
                .otherwise(col("is_valid"))
            )
        
        # Validate amount is positive
        validated_df = validated_df.withColumn(
            "is_valid",
            when(col("amount") <= 0, False)
            .otherwise(col("is_valid"))
        )
        
        # Add validation timestamp
        validated_df = validated_df.withColumn(
            "validation_timestamp",
            current_timestamp()
        )
        
        # Add error reason for invalid records
        validated_df = validated_df.withColumn(
            "error_reason",
            when(col("is_valid"), lit(None))
            .otherwise(lit("Validation failed: missing required fields or invalid amount"))
        )
        
        return validated_df
    
    def _add_metadata(self, df):
        """
        Add processing metadata to the transaction data.
        
        Args:
            df: Input DataFrame with validated transaction data
            
        Returns:
            DataFrame with additional metadata columns
        """
        # Add ingestion timestamp
        enriched_df = df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        
        # Add processing date (for partitioning)
        enriched_df = enriched_df.withColumn(
            "processing_date",
            to_timestamp(col("timestamp"))
        )
        
        # Add partition columns
        enriched_df = enriched_df.withColumn("year", year(col("processing_date")))
        enriched_df = enriched_df.withColumn("month", month(col("processing_date")))
        enriched_df = enriched_df.withColumn("day", dayofmonth(col("processing_date")))
        enriched_df = enriched_df.withColumn("hour", hour(col("processing_date")))
        
        # Add batch timestamp
        enriched_df = enriched_df.withColumn(
            "batch_timestamp",
            current_timestamp()
        )
        
        # Add data source
        enriched_df = enriched_df.withColumn("data_source", lit("kafka_stream"))
        
        # Add processing version
        enriched_df = enriched_df.withColumn("processing_version", lit("1.0"))
        
        return enriched_df
    
    def _write_to_data_lake(self, df):
        """
        Write the processed data to the data lake in partitioned format.
        
        Args:
            df: DataFrame with processed transaction data
            
        Returns:
            StreamingQuery object
        """
        logger.info(f"Setting up data lake write to {config.raw_transactions_path}")
        
        # Write only valid records to main data lake
        query = df.filter(col("is_valid") == True).writeStream \
            .format("parquet") \
            .outputMode("append") \
            .partitionBy("year", "month", "day") \
            .option("path", str(config.raw_transactions_path)) \
            .option("checkpointLocation", str(config.checkpoints_path / "transactions")) \
            .trigger(processingTime=f"{config.trigger_interval_seconds} seconds") \
            .start()
        
        return query
    
    def _write_dead_letter_queue(self, df):
        """
        Write invalid records to dead letter queue.
        
        Args:
            df: DataFrame with invalid transaction data
            
        Returns:
            StreamingQuery object
        """
        logger.info("Setting up dead letter queue")
        
        # Write invalid records to separate location
        dead_letter_path = config.data_lake_path / "dead_letter_queue"
        
        query = df.filter(col("is_valid") == False).writeStream \
            .format("parquet") \
            .outputMode("append") \
            .partitionBy("year", "month", "day") \
            .option("path", str(dead_letter_path)) \
            .option("checkpointLocation", str(config.checkpoints_path / "dead_letter")) \
            .trigger(processingTime=f"{config.trigger_interval_seconds} seconds") \
            .start()
        
        return query
    
    def _setup_monitoring(self, df):
        """
        Set up monitoring and metrics collection.
        
        Args:
            df: Input DataFrame for monitoring
            
        Returns:
            StreamingQuery object for monitoring
        """
        logger.info("Setting up monitoring query")
        
        # Create metrics DataFrame
        metrics_df = df.groupBy(
            date_format(col("ingestion_timestamp"), "yyyy-MM-dd HH:mm").alias("minute")
        ).agg(
            count("*").alias("total_transactions"),
            sum(when(col("is_valid") == True, 1).otherwise(0)).alias("valid_transactions"),
            sum(when(col("is_valid") == False, 1).otherwise(0)).alias("invalid_transactions"),
            sum(col("amount")).alias("total_amount"),
            avg(col("amount")).alias("avg_amount"),
            sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_transactions")
        ).withColumn(
            "valid_rate",
            col("valid_transactions") / col("total_transactions")
        ).withColumn(
            "fraud_rate",
            col("fraud_transactions") / col("total_transactions")
        )
        
        # Write metrics to console for monitoring
        query = metrics_df.writeStream \
            .format("console") \
            .outputMode("complete") \
            .trigger(processingTime=f"{config.stats_interval_seconds} seconds") \
            .option("truncate", "false") \
            .start()
        
        return query
    
    def start_processing(self):
        """Start the stream processing pipeline."""
        logger.info("Starting transaction stream processing...")
        
        try:
            # Validate configuration
            if not config.validate():
                logger.error("Configuration validation failed")
                sys.exit(1)
            
            # Ensure directories exist
            config.ensure_directories()
            
            # Print configuration
            config.print_config()
            
            # Create Spark session
            self.spark = self._create_spark_session()
            
            # Read from Kafka
            logger.info(f"Reading from Kafka topic: {config.kafka_topic}")
            kafka_df = self.spark.readStream \
                .format("kafka") \
                .options(**config.get_kafka_options()) \
                .load()
            
            # Process the stream
            logger.info("Processing stream...")
            
            # Step 1: Parse JSON data
            parsed_df = self._parse_json_data(kafka_df)
            
            # Step 2: Validate and clean data
            validated_df = self._validate_and_clean_data(parsed_df)
            
            # Step 3: Add metadata
            enriched_df = self._add_metadata(validated_df)
            
            # Step 4: Set up queries
            queries = []
            
            # Main data lake write
            main_query = self._write_to_data_lake(enriched_df)
            queries.append(main_query)
            
            # Dead letter queue
            dead_letter_query = self._write_dead_letter_queue(enriched_df)
            queries.append(dead_letter_query)
            
            # Monitoring
            monitoring_query = self._setup_monitoring(enriched_df)
            queries.append(monitoring_query)
            
            self.running = True
            logger.info("Stream processing started successfully!")
            logger.info(f"Processing data to: {config.raw_transactions_path}")
            logger.info(f"Trigger interval: {config.trigger_interval_seconds} seconds")
            
            # Wait for termination
            while self.running:
                try:
                    # Check query status
                    for i, query in enumerate(queries):
                        if query.status['isDataAvailable']:
                            logger.debug(f"Query {i} has data available")
                        
                        if query.status['isTriggerActive']:
                            logger.debug(f"Query {i} trigger is active")
                    
                    # Sleep before next check
                    import time
                    time.sleep(5)
                    
                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt")
                    break
                except Exception as e:
                    logger.error(f"Error in processing loop: {e}")
                    break
            
        except Exception as e:
            logger.error(f"Failed to start stream processing: {e}")
            raise
        finally:
            self._shutdown()
    
    def _shutdown(self):
        """Gracefully shutdown the stream processor."""
        logger.info("Shutting down stream processor...")
        self.running = False
        
        if self.spark:
            try:
                logger.info("Stopping all streaming queries...")
                for query in self.spark.streams.active:
                    query.stop()

                logger.info("Stopping Spark session...")
                self.spark.stop()
                
                logger.info("Stream processor shutdown complete")
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")


def main():
    """Main function to run the stream processor."""
    logger.info("Starting Fintech Transaction Stream Processor")
    
    processor = TransactionStreamProcessor()
    
    try:
        processor.start_processing()
    except Exception as e:
        logger.error(f"Stream processor failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
