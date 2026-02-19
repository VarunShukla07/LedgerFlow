"""
Kafka Transaction Producer
Sends transaction data to Kafka topic
"""

import json
import time
import random
import logging
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker
from kaggle_loader import KaggleDataLoader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker for synthetic data
fake = Faker('en_IN')  # Indian locale


class TransactionProducer:
    """Produces transaction events to Kafka"""
    
    def __init__(self, 
                 bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'transactions'):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka broker address
            topic: Kafka topic name
        """
        self.topic = topic
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        self.kaggle_loader = KaggleDataLoader()
        self.total_sent = 0
        self.kaggle_exhausted = False
        
    def connect(self) -> bool:
        """
        Connect to Kafka broker
        
        Returns:
            bool: True if connected successfully
        """
        try:
            logger.info(f"Connecting to Kafka at {self.bootstrap_servers}...")
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas
                retries=3
            )
            logger.info("✅ Connected to Kafka successfully!")
            return True
        except KafkaError as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            logger.error("Make sure Docker containers are running: docker-compose ps")
            return False
    
    def generate_synthetic_transaction(self) -> dict:
        """
        Generate a synthetic transaction using Faker
        
        Returns:
            dict: Synthetic transaction
        """
        payment_methods = ['UPI', 'Card', 'NetBanking', 'Wallet']
        statuses = ['SUCCESS', 'SUCCESS', 'SUCCESS', 'FAILED', 'PENDING']  # 60% success
        categories = ['Food', 'Retail', 'Entertainment', 'Travel', 'Bills', 'Shopping']
        device_types = ['Mobile', 'Web', 'App']
        
        status = random.choice(statuses)
        
        transaction = {
            "transaction_id": f"TXN_{fake.uuid4()}",
            "customer_id": f"CUST_{fake.uuid4()}",
            "customer_name": fake.name(),
            "customer_email": fake.email(),
            "merchant_name": fake.company(),
            "merchant_category": random.choice(categories),
            "amount": round(random.uniform(10, 5000), 2),
            "payment_method": random.choice(payment_methods),
            "status": status,
            "failure_reason": fake.sentence() if status == 'FAILED' else None,
            "timestamp": datetime.now().isoformat(),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "currency": "INR",
            "device_type": random.choice(device_types),
            "ip_address": fake.ipv4(),
            "is_fraud": random.choice([0, 0, 0, 0, 0, 0, 0, 0, 0, 1])  # 10% fraud
        }
        
        return transaction
    
    def send_transaction(self, transaction: dict):
        """
        Send a single transaction to Kafka
        
        Args:
            transaction: Transaction dictionary
        """
        try:
            future = self.producer.send(self.topic, value=transaction)
            future.get(timeout=10)  # Wait for confirmation
            self.total_sent += 1
            
            # Log progress every 10 transactions
            if self.total_sent % 10 == 0:
                logger.info(f"✅ Sent {self.total_sent} transactions | "
                           f"Latest: {transaction['merchant_name']} | "
                           f"Amount: {transaction['currency']} {transaction['amount']}")
                
        except Exception as e:
            logger.error(f"❌ Failed to send transaction: {e}")
    
    def run(self, rate: float = 1.0, max_count: int = None):
        """
        Start producing transactions
        
        Args:
            rate: Transactions per second
            max_count: Maximum number of transactions (None = infinite)
        """
        # Load Kaggle data
        if self.kaggle_loader.load_data():
            logger.info(f"📊 Loaded {self.kaggle_loader.get_total_count()} Kaggle transactions")
        else:
            logger.warning("⚠️  Could not load Kaggle data, using 100% synthetic data")
            self.kaggle_exhausted = True
        
        logger.info(f"🚀 Starting producer | Rate: {rate} txn/sec | Max: {max_count or 'unlimited'}")
        logger.info("Press Ctrl+C to stop\n")
        
        delay = 1.0 / rate
        
        try:
            while True:
                # Check if we've reached max count
                if max_count and self.total_sent >= max_count:
                    logger.info(f"✅ Reached max count of {max_count} transactions")
                    break
                
                # Decide: Use Kaggle (70%) or Synthetic (30%)
                use_kaggle = random.random() < 0.7 and not self.kaggle_exhausted
                
                if use_kaggle:
                    transaction = self.kaggle_loader.get_next_transaction()
                    if transaction is None:
                        logger.info("📊 Kaggle dataset exhausted, switching to 100% synthetic")
                        self.kaggle_exhausted = True
                        continue
                else:
                    transaction = self.generate_synthetic_transaction()
                
                # Send to Kafka
                self.send_transaction(transaction)
                
                # Wait before next transaction
                time.sleep(delay)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping producer...")
        finally:
            self.close()
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(f"✅ Producer closed. Total sent: {self.total_sent}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Kafka Transaction Producer')
    parser.add_argument('--rate', type=float, default=1.0,
                       help='Transactions per second (default: 1.0)')
    parser.add_argument('--total', type=int, default=None,
                       help='Maximum number of transactions (default: unlimited)')
    parser.add_argument('--broker', type=str, default='localhost:9092',
                       help='Kafka broker address (default: localhost:9092)')
    
    args = parser.parse_args()
    
    # Create and run producer
    producer = TransactionProducer(bootstrap_servers=args.broker)
    
    if producer.connect():
        producer.run(rate=args.rate, max_count=args.total)
    else:
        logger.error("Failed to start producer")


if __name__ == "__main__":
    main()