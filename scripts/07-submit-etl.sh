#!/bin/bash

set -e

echo "========================================"
echo "Submitting Spark ETL Job"
echo "========================================"

# Configuration
SPARK_MASTER="spark://spark-master:7077"
APP_NAME="InsuranceETL"
MAIN_FILE="/opt/spark-apps/insurance_etl.py"
DRIVER_MEMORY="2g"
EXECUTOR_MEMORY="2g"
EXECUTOR_CORES="2"
NUM_EXECUTORS="1"

# JAR dependencies - using simpler approach without remote jars
# MySQL and Kafka support is included in Spark core

echo "Spark Master: $SPARK_MASTER"
echo "Application: $APP_NAME"
echo "Main File: $MAIN_FILE"
echo ""

# Submit job
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master $SPARK_MASTER \
  --name $APP_NAME \
  --driver-memory $DRIVER_MEMORY \
  --executor-memory $EXECUTOR_MEMORY \
  --executor-cores $EXECUTOR_CORES \
  --num-executors $NUM_EXECUTORS \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/opt/spark-conf/log4j.properties -Divy.cache.dir=/tmp -Divy.home=/tmp" \
  --conf spark.executor.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,mysql:mysql-connector-java:8.0.33 \
  $MAIN_FILE

echo ""
echo "========================================"
echo "Spark ETL Job Completed"
echo "========================================"
