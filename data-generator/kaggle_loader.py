"""
Kaggle Dataset Loader
Loads and preprocesses the fraud detection dataset
"""

import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KaggleDataLoader:
    """Loads and processes Kaggle fraud detection dataset"""
    
    def __init__(self, csv_path: str = "data/fraudTrain.csv"):
        """
        Initialize the loader
        
        Args:
            csv_path: Path to the fraudTrain.csv file
        """
        self.csv_path = csv_path
        self.df = None
        self.current_index = 0
        
    def load_data(self) -> bool:
        """
        Load the CSV file into memory
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Loading dataset from {self.csv_path}...")
            self.df = pd.read_csv(self.csv_path)
            logger.info(f"Loaded {len(self.df)} transactions")
            logger.info(f"Columns: {list(self.df.columns)}")
            return True
        except FileNotFoundError:
            logger.error(f"File not found: {self.csv_path}")
            logger.error("fraudTrain.csv not found. Expected at: data/fraudTrain.csv")
            return False
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def get_next_transaction(self) -> Dict[str, Any]:
        """
        Get the next transaction from the dataset
        
        Returns:
            dict: Transaction data in our schema format
        """
        if self.df is None or self.current_index >= len(self.df):
            return None
        
        # Get current row
        row = self.df.iloc[self.current_index]
        self.current_index += 1
        
        # Convert to our schema
        transaction = {
            "transaction_id": f"TXN_{self.current_index}_{int(datetime.now().timestamp())}",
            "customer_id": str(row.get('cc_num', 'UNKNOWN')),
            "customer_name": f"{row.get('first', '')} {row.get('last', '')}".strip(),
            "customer_email": f"{row.get('first', 'user').lower()}.{row.get('last', 'example').lower()}@email.com",
            "merchant_name": str(row.get('merchant', 'Unknown Merchant')),
            "merchant_category": str(row.get('category', 'misc_net')),
            "amount": float(row.get('amt', 0.0)),
            "payment_method": "Card",  # Kaggle data is all card transactions
            "status": "SUCCESS",  # We'll randomly add failures later
            "failure_reason": None,
            "timestamp": str(row.get('trans_date_trans_time', datetime.now().isoformat())),
            "city": str(row.get('city', 'Unknown')),
            "state": str(row.get('state', 'Unknown')),
            "country": "USA",  # Kaggle dataset is US-based
            "currency": "USD",
            "device_type": "Mobile",  # We'll randomize this later
            "ip_address": "192.168.1.1",  # We'll generate fake IPs later
            "is_fraud": int(row.get('is_fraud', 0))
        }
        
        return transaction
    
    def get_total_count(self) -> int:
        """Get total number of transactions in dataset"""
        return len(self.df) if self.df is not None else 0
    
    def reset(self):
        """Reset the iterator to the beginning"""
        self.current_index = 0


# Test the loader
if __name__ == "__main__":
    loader = KaggleDataLoader()
    
    if loader.load_data():
        # Logging first 3 transactions
        logger.info("=" * 50)
        logger.info("SAMPLE TRANSACTIONS")
        logger.info("=" * 50)
        
        for i in range(3):
            txn = loader.get_next_transaction()
            if txn:
                logger.info(f"Transaction {i+1}:")
                logger.info(f"  ID: {txn['transaction_id']}")
                logger.info(f"  Customer: {txn['customer_name']}")
                logger.info(f"  Merchant: {txn['merchant_name']}")
                logger.info(f"  Amount: ${txn['amount']:.2f}")
                logger.info(f"  Fraud: {'YES' if txn['is_fraud'] else 'NO'}")