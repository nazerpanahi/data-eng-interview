#!/usr/bin/env python3
"""
Combined Data Manager for Azki Insurance Analytics
Handles both MySQL data loading and Kafka event production
"""

import csv
import json
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataManager:
    """Combined data manager for MySQL and Kafka operations"""
    
    def __init__(self):
        # MySQL connection configuration
        self.mysql_engine = create_engine(
            'mysql+pymysql://insurance_user:insurance_pass@localhost:3306/insurance_db',
            echo=False
        )
        
        # Kafka producer configuration
        self.producer = None
        self.kafka_bootstrap_servers = 'localhost:9092'
        self.kafka_topic = 'insurance.raw_events'
        self.batch_size = 100
        self.batch_delay_ms = 50
    
        
    def load_mysql_data(self):
        """Load users data from CSV to MySQL using pandas"""
        
        csv_file = 'data/users.csv'
        
        try:
            # Check existing data
            print("Checking existing MySQL data...")
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM users"))
                existing_count = result.fetchone()[0]
                print(f"Existing users count: {existing_count}")
                
                if existing_count > 0:
                    print("✓ Users table already has data")
                    return True
            
            # Read CSV
            print(f"Reading {csv_file}...")
            df = pd.read_csv(csv_file)
            print(f"✓ Read {len(df)} records")
            
            # Add timestamp columns
            current_time = datetime.now()
            df['created_at'] = current_time
            df['updated_at'] = current_time
            
            # Insert into MySQL
            print("Loading data into MySQL...")
            df.to_sql('users', self.mysql_engine, if_exists='append', index=False, chunksize=1000)
            
            # Verify
            print("Verifying data...")
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) as total, MIN(signup_date) as min_date, MAX(signup_date) as max_date,
                           COUNT(DISTINCT city) as cities, COUNT(DISTINCT device_type) as devices
                    FROM users
                """))
                stats = result.fetchone()
                print(f"✓ Loaded {stats[0]} users")
                print(f"  Date range: {stats[1]} to {stats[2]}")
                print(f"  Cities: {stats[3]}, Devices: {stats[4]}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error loading MySQL data: {e}")
            return False
    
    def create_kafka_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=5,
                compression_type='gzip',
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432
            )
            print("✓ Kafka producer initialized successfully")
            return True
        except Exception as e:
            print(f"✗ Failed to create Kafka producer: {e}")
            return False
    
    def produce_events(self):
        """Produce events to Kafka from CSV file"""
        
        csv_file = 'data/user_events.csv'
        
        try:
            if not os.path.exists(csv_file):
                print(f"✗ Error: CSV file not found at {csv_file}")
                return False
            
            print(f"Reading events from {csv_file}...")
            
            # Read CSV and produce events in batches
            with open(csv_file, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                next(csv_reader)  # Skip header row
                
                batch_data = []
                total_rows = 0
                failed_rows = 0
                
                for row in csv_reader:
                    if len(row) >= 6:
                        try:
                            # Parse event data
                            event_data = {
                                'event_time': row[0],
                                'user_id': int(row[1]),
                                'session_id': row[2],
                                'event_type': row[3],
                                'channel': row[4],
                                'premium_amount': int(row[5]) if row[5] else 0
                            }
                            
                            batch_data.append((event_data['event_time'], event_data['user_id'], event_data))
                            total_rows += 1
                            
                            # Send batch when it reaches the size limit
                            if len(batch_data) >= self.batch_size:
                                success = self._send_batch(batch_data)
                                if not success:
                                    failed_rows += len(batch_data)
                                batch_data = []
                                
                                # Progress report
                                print(f"Batch {int(total_rows/self.batch_size)}: Sent {total_rows} events, Failed: {failed_rows}")
                                
                        except (ValueError, IndexError) as e:
                            print(f"Skipping invalid row: {row} - Error: {e}")
                            failed_rows += 1
                            continue
                
                # Send remaining records
                if batch_data:
                    success = self._send_batch(batch_data)
                    if not success:
                        failed_rows += len(batch_data)
                
                # Final report
                success_rate = ((total_rows - failed_rows) / total_rows * 100) if total_rows > 0 else 0
                print("\n" + "="*60)
                print("Event production completed!")
                print(f"Total events sent: {total_rows}")
                print(f"Total events failed: {failed_rows}")
                print(f"Success rate: {success_rate:.2f}%")
                print("="*60)
                
                return failed_rows == 0
            
        except Exception as e:
            print(f"✗ Error producing events: {e}")
            return False
    
    def _send_batch(self, batch_data):
        """Send a batch of events to Kafka"""
        
        futures = []
        
        for event_time, user_id, event_data in batch_data:
            try:
                future = self.producer.send(
                    self.kafka_topic,
                    key=user_id,
                    value=event_data
                )
                futures.append(future)
                
            except Exception as e:
                print(f"Failed to send event for user {user_id}: {e}")
                return False
        
        # Wait for all messages to be sent
        for future in futures:
            try:
                future.get(timeout=10)  # Wait up to 10 seconds per message
            except Exception as e:
                print(f"Failed to confirm message delivery: {e}")
                return False
        
        return True
    
    def close_kafka_producer(self):
        """Close Kafka producer properly"""
        if self.producer:
            self.producer.close()
            print("✓ Kafka producer closed")
    
    def run_all(self):
        """Run all data operations"""
        print("="*60)
        print("Azki Insurance Data Manager - Full Data Initialization")
        print("="*60)
        
        success = True
        
        # Step 1: Load MySQL data
        print("\nStep 1: Loading MySQL users data...")
        if not self.load_mysql_data():
            success = False
        
        # Step 2: Produce Kafka events
        if success:
            print("\nStep 2: Producing Kafka events...")
            if not self.create_kafka_producer():
                success = False
            
            if success:
                start_time = time.time()
                if not self.produce_events():
                    success = False
                end_time = time.time()
                
                if success:
                    print(f"\n✓ Data initialization completed successfully!")
                    print(f"⏱️  Total execution time: {int(end_time - start_time)} seconds")
        
        # Cleanup
        self.close_kafka_producer()
        
        return success

def main():
    """Main entry point"""
    try:
        manager = DataManager()
        
        # Run all operations
        if manager.run_all():
            print("\n✓ Data initialization completed successfully!")
            sys.exit(0)
        else:
            print("\n✗ Data initialization failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️  Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()