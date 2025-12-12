"""
Insurance Data Engineering - Spark ETL Pipeline
Batch processing: Kafka → MySQL Join → Kafka Output
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, hour, dayofweek,
    when, datediff, row_number, monotonically_increasing_id,
    to_json, struct, from_json, to_timestamp, sum, count, countDistinct, avg
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, DateType
)
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InsuranceETLPipeline:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self, app_name="InsuranceETL"):
        """Initialize Spark session with optimized configurations"""
        self.spark = self._create_spark_session(app_name)
        self.kafka_bootstrap_servers = "kafka:29092"
        self.kafka_output_topic = "insurance.processed_events"
        self.mysql_url = "jdbc:mysql://mysql:3306/insurance_db"
        self.mysql_properties = {
            "user": "insurance_user",
            "password": "insurance_pass",
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    
    def _create_spark_session(self, app_name):
        """Create optimized Spark session"""
        try:
            spark = (SparkSession.builder
                .appName(app_name)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.default.parallelism", "8")
                .config("spark.sql.files.maxPartitionBytes", "128MB")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.broadcastTimeout", "600")
                .config("spark.network.timeout", "600s")
                .config("spark.executor.heartbeatInterval", "60s")
                # MySQL JDBC
                .config("spark.jars.packages", 
                       "mysql:mysql-connector-java:8.0.33,"
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                       "com.clickhouse:clickhouse-jdbc:0.4.6")
                .getOrCreate())
            
            spark.sparkContext.setLogLevel("WARN")
            logger.info(f"Spark session created: {app_name}")
            logger.info(f"Spark version: {spark.version}")
            return spark
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def extract_kafka_events(self, topic="insurance.raw_events", 
                            start_offset="earliest"):
        """
        Extract events from Kafka topic
        
        Args:
            topic: Kafka topic name
            start_offset: Starting offset (earliest/latest)
        
        Returns:
            DataFrame with parsed events
        """
        logger.info(f"Extracting data from Kafka topic: {topic}")
        
        try:
            # Define event schema
            event_schema = StructType([
                StructField("event_time", StringType(), True),
                StructField("user_id", IntegerType(), True),
                StructField("session_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("channel", StringType(), True),
                StructField("premium_amount", LongType(), True)
            ])
            
            # Read from Kafka
            raw_df = (self.spark.read
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", topic)
                .option("startingOffsets", start_offset)
                .option("endingOffsets", "latest")
                .option("maxOffsetsPerTrigger", 10000)
                .load())
            
            # Parse JSON value
            events_df = (raw_df
                .selectExpr("CAST(value AS STRING) as json_value")
                .select(from_json(col("json_value"), event_schema).alias("data"))
                .select("data.*")
                .withColumn("event_time", 
                           to_timestamp(col("event_time"), 
                                      "yyyy-MM-dd HH:mm:ss")))
            
            count = events_df.count()
            logger.info(f"Extracted {count} events from Kafka")
            
            return events_df
        
        except Exception as e:
            logger.error(f"Failed to extract from Kafka: {e}")
            raise
    
    def extract_mysql_users(self):
        """
        Extract user dimension from MySQL
        
        Returns:
            DataFrame with user attributes
        """
        logger.info("Extracting users from MySQL")
        
        try:
            users_df = (self.spark.read
                .jdbc(url=self.mysql_url,
                     table="users",
                     properties=self.mysql_properties)
                .withColumn("signup_date", 
                           to_date(col("signup_date"))))
            
            count = users_df.count()
            logger.info(f"Extracted {count} users from MySQL")
            
            return users_df
        
        except Exception as e:
            logger.error(f"Failed to extract from MySQL: {e}")
            raise
    
    def transform_enrich_data(self, events_df, users_df):
        """
        Transform and enrich events with user dimension
        
        Args:
            events_df: Raw events DataFrame
            users_df: Users dimension DataFrame
        
        Returns:
            Enriched and transformed DataFrame
        """
        logger.info("Starting data transformation and enrichment")
        
        try:
            # Data quality: Remove nulls and invalid records
            events_clean = events_df.filter(
                col("user_id").isNotNull() &
                col("event_time").isNotNull() &
                col("session_id").isNotNull() &
                col("premium_amount").isNotNull() &
                (col("premium_amount") > 0)
            )
            
            # Broadcast small dimension table for efficient join
            users_broadcast = broadcast(users_df)
            
            # Enrich events with user attributes
            enriched_df = events_clean.join(
                users_broadcast,
                on="user_id",
                how="left"
            )
            
            # Derive additional attributes
            transformed_df = (enriched_df
                # Event temporal attributes
                .withColumn("event_date", to_date(col("event_time")))
                .withColumn("event_hour", hour(col("event_time")))
                .withColumn("event_day_of_week", dayofweek(col("event_time")))
                
                # User tenure calculation
                .withColumn("user_tenure_days",
                           datediff(col("event_date"), col("signup_date")))
                
                # Business logic flags
                .withColumn("is_purchase",
                           when(col("event_type") == "purchase", 1)
                           .otherwise(0))
                
                # Generate unique event_id
                .withColumn("event_id", monotonically_increasing_id())
                
                # Processing metadata
                .withColumn("processing_time", current_timestamp())
                .withColumn("data_source", lit("spark_etl"))
            )
            
            # Select final columns
            final_df = transformed_df.select(
                "event_id",
                "event_time",
                "user_id",
                "session_id",
                "event_type",
                "channel",
                "premium_amount",
                "signup_date",
                "city",
                "device_type",
                "user_tenure_days",
                "event_date",
                "event_hour",
                "event_day_of_week",
                "is_purchase",
                "processing_time",
                "data_source"
            )
            
            count = final_df.count()
            logger.info(f"Transformed {count} records successfully")
            
            # Data quality report
            null_counts = {}
            for column_name in final_df.columns:
                null_count = final_df.select(
                    sum(when(col(column_name).isNull(), 1).otherwise(0))
                ).collect()[0][0]
                null_counts[column_name] = null_count
            
            logger.info("Data Quality Check - Null Counts:")
            for col_name, null_count in null_counts.items():
                if null_count > 0:
                    logger.warning(f"  {col_name}: {null_count} nulls")
            
            return final_df
        
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def load_to_kafka(self, df):
        """
        Load transformed data to Kafka
        
        Args:
            df: DataFrame to load
        """
        logger.info(f"Loading data to Kafka topic: {self.kafka_output_topic}")
        
        try:
            # Convert DataFrame to JSON and write to Kafka
            df_json = df.selectExpr("to_json(struct(*)) AS value")
            
            (df_json.write
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("topic", self.kafka_output_topic)
                .option("checkpointLocation", "/tmp/spark/kafka_checkpoint")
                .mode("append")
                .save())
            
            logger.info(f"Successfully loaded data to Kafka topic: {self.kafka_output_topic}")
        
        except Exception as e:
            logger.error(f"Failed to load to Kafka: {e}")
            raise
    
        
    def run_pipeline(self, kafka_topic="insurance.raw_events"):
        """
        Execute complete ETL pipeline
        
        Args:
            kafka_topic: Source Kafka topic
        """
        logger.info("="*70)
        logger.info("STARTING INSURANCE ETL PIPELINE")
        logger.info("="*70)
        
        pipeline_start = datetime.now()
        
        try:
            # EXTRACT
            logger.info("PHASE 1: EXTRACTION")
            events_df = self.extract_kafka_events(kafka_topic)
            users_df = self.extract_mysql_users()
            
            # TRANSFORM
            logger.info("PHASE 2: TRANSFORMATION")
            processed_df = self.transform_enrich_data(events_df, users_df)
            
            # Cache for multiple loads
            processed_df.cache()
            
            # LOAD - Processed Events (Final Step)
            logger.info("PHASE 3: LOADING - Processed Events")
            self.load_to_kafka(processed_df)
            
            # Unpersist cache
            processed_df.unpersist()
            
            pipeline_end = datetime.now()
            duration = (pipeline_end - pipeline_start).total_seconds()
            
            logger.info("="*70)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("="*70)
            
            return True
        
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    try:
        pipeline = InsuranceETLPipeline()
        pipeline.run_pipeline()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
