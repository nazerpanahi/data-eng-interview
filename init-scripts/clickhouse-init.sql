-- ============================================================================
-- ClickHouse Initialization Script
-- Insurance Data Engineering Assignment
-- ============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS insurance_analytics;

USE insurance_analytics;

-- ------------------------------------------------------------------------------
-- USERS DIMENSION TABLE (CDC from MySQL)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id UInt32,
    signup_date Date,
    city LowCardinality(String),
    device_type LowCardinality(String),
    created_at DateTime,
    updated_at DateTime,
    version UInt32 DEFAULT 1
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(signup_date)
ORDER BY user_id
SETTINGS index_granularity = 8192;

-- ------------------------------------------------------------------------------
-- PROCESSED EVENTS TABLE (Main storage table from Kafka) - Optimized Types
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_events_storage (
    event_id UInt64,
    event_time DateTime,
    user_id UInt16,
    session_id String,
    event_type LowCardinality(String),
    channel LowCardinality(String),
    premium_amount UInt32,
    signup_date Date,
    city LowCardinality(String),
    device_type LowCardinality(String),
    user_tenure_days UInt16,
    event_date Date,
    event_hour UInt8,
    event_day_of_week UInt8,
    is_purchase UInt8,
    processing_time DateTime,
    data_source LowCardinality(FixedString(10)),
    ingestion_time DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_id)
TTL event_time + toIntervalDay(365)
SETTINGS index_granularity = 8192;

-- ------------------------------------------------------------------------------
-- KAFKA ENGINE TABLE (for streaming processed events from Spark ETL) - Optimized Types
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_events_kafka (
    event_id UInt64,
    event_time String,
    user_id UInt16,
    session_id String,
    event_type String,
    channel String,
    premium_amount UInt32,
    signup_date String,
    city String,
    device_type String,
    user_tenure_days UInt16,
    event_date String,
    event_hour UInt8,
    event_day_of_week UInt8,
    is_purchase UInt8,
    processing_time String,
    data_source String
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'insurance.processed_events',
    kafka_group_name = 'clickhouse_processed_consumer_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 100;

-- ------------------------------------------------------------------------------
-- MATERIALIZED VIEW (Kafka → processed_events_storage) - With data cleaning
-- ------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS processed_events_mv TO processed_events_storage AS
SELECT 
    event_id,
    parseDateTimeBestEffort(event_time) as event_time,
    user_id,
    session_id,
    event_type,
    channel,
    premium_amount,
    parseDateTimeBestEffort(signup_date) as signup_date,
    city,
    trimBoth(device_type) as device_type,
    user_tenure_days,
    parseDateTimeBestEffort(event_date) as event_date,
    event_hour,
    event_day_of_week,
    is_purchase,
    parseDateTimeBestEffort(processing_time) as processing_time,
    data_source,
    now() as ingestion_time
FROM processed_events_kafka;

-- ------------------------------------------------------------------------------
-- DAILY EVENT METRICS TABLE (based on processed_events_storage)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_event_metrics (
    metric_date Date,
    channel LowCardinality(String),
    event_type LowCardinality(String),
    city LowCardinality(String),
    device_type LowCardinality(String),
    
    total_events UInt64,
    unique_users UInt64,
    unique_sessions UInt64,
    total_premium UInt64,
    avg_premium Float64,
    
    calculated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, channel, event_type, city, device_type)
SETTINGS index_granularity = 8192;

-- ------------------------------------------------------------------------------
-- DAILY AGGREGATE METRICS TABLE (overall daily stats)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_aggregate_metrics (
    metric_date Date,
    channel LowCardinality(String),
    city LowCardinality(String),
    device_type LowCardinality(String),
    
    total_events UInt64,
    unique_users UInt64,
    unique_sessions UInt64,
    total_premium UInt64,
    purchase_events UInt64,
    total_users_with_premium UInt64,
    
    calculated_at DateTime DEFAULT now()
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, channel, city, device_type)
SETTINGS index_granularity = 8192;

-- ------------------------------------------------------------------------------
-- SESSION AGGREGATES TABLE (session-level metrics)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS session_metrics (
    session_id String,
    user_id UInt16,
    first_event_time DateTime,
    last_event_time DateTime,
    total_events UInt32,
    unique_event_types UInt8,
    has_purchase UInt8,
    total_premium UInt32,
    first_channel LowCardinality(String),
    first_city LowCardinality(String),
    first_device_type LowCardinality(String),
    event_sequence Array(String),
    session_duration_seconds UInt32,
    calculated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(first_event_time)
ORDER BY session_id
TTL first_event_time + toIntervalDay(365)
SETTINGS index_granularity = 8192;

-- ------------------------------------------------------------------------------
-- MATERIALIZED VIEW FOR DAILY EVENT METRICS
-- ------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_event_metrics_mv TO daily_event_metrics AS
SELECT 
    event_date as metric_date,
    channel,
    event_type,
    city,
    device_type,
    toUInt64(count()) as total_events,
    toUInt64(countDistinct(user_id)) as unique_users,
    toUInt64(countDistinct(session_id)) as unique_sessions,
    toUInt64(sum(premium_amount)) as total_premium,
    toFloat64(avg(premium_amount)) as avg_premium,
    now() as calculated_at
FROM processed_events_storage
GROUP BY event_date, channel, event_type, city, device_type;

-- ------------------------------------------------------------------------------
-- MATERIALIZED VIEW FOR DAILY AGGREGATE METRICS
-- ------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_aggregate_metrics_mv TO daily_aggregate_metrics AS
SELECT 
    event_date as metric_date,
    channel,
    city,
    device_type,
    toUInt64(count()) as total_events,
    toUInt64(countDistinct(user_id)) as unique_users,
    toUInt64(countDistinct(session_id)) as unique_sessions,
    toUInt64(sum(premium_amount)) as total_premium,
    toUInt32(sum(is_purchase)) as purchase_events,
    toUInt32(countIf(premium_amount > 0)) as total_users_with_premium,
    now() as calculated_at
FROM processed_events_storage
GROUP BY event_date, channel, city, device_type;

-- ------------------------------------------------------------------------------
-- MATERIALIZED VIEW FOR SESSION METRICS (simplified version)
-- ------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS session_metrics_mv TO session_metrics AS
SELECT 
    session_id,
    user_id,
    min(event_time) as first_event_time,
    max(event_time) as last_event_time,
    toUInt32(count()) as total_events,
    toUInt8(countDistinct(event_type)) as unique_event_types,
    toUInt8(max(is_purchase)) as has_purchase,
    toUInt32(sum(premium_amount)) as total_premium,
    any(channel) as first_channel,
    any(city) as first_city,
    any(device_type) as first_device_type,
    groupArray(event_type) as event_sequence,
    toUInt32(dateDiff('second', min(event_time), max(event_time))) as session_duration_seconds,
    now() as calculated_at
FROM processed_events_storage
GROUP BY session_id, user_id;

-- ------------------------------------------------------------------------------
-- CREATE INDEXES FOR OPTIMIZATION
-- ------------------------------------------------------------------------------
-- Bloom filter for session_id lookups
ALTER TABLE processed_events_storage ADD INDEX IF NOT EXISTS idx_session_bloom session_id TYPE bloom_filter(0.01) GRANULARITY 1;

-- Min-max index for premium amounts
ALTER TABLE processed_events_storage ADD INDEX IF NOT EXISTS idx_premium_minmax premium_amount TYPE minmax GRANULARITY 4;

-- ------------------------------------------------------------------------------
-- COMPLETION MESSAGE
-- ------------------------------------------------------------------------------
SELECT 'ClickHouse initialization completed successfully!' AS status;