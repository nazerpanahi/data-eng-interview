-- ============================================================================
-- Data Quality Validation Infrastructure
-- Azki Insurance Analytics Platform
-- ============================================================================

USE insurance_analytics;

-- ------------------------------------------------------------------------------
-- DATA QUALITY MONITORING TABLES
-- ------------------------------------------------------------------------------

-- Pipeline Health Status Dashboard
CREATE TABLE IF NOT EXISTS pipeline_health_status (
    check_time DateTime DEFAULT now(),
    component String,
    metric_name String,
    metric_value Float64,
    status LowCardinality(String),  -- 'healthy', 'warning', 'critical'
    threshold_value Float64,
    details String,
    alert_level UInt8 DEFAULT 0    -- 0: info, 1: warning, 2: critical
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(check_time)
ORDER BY (component, metric_name, check_time)
TTL check_time + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- Data Quality Checks Log
CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id UUID DEFAULT generateUUIDv4(),
    check_time DateTime DEFAULT now(),
    table_name String,
    check_type LowCardinality(String),  -- 'completeness', 'accuracy', 'consistency', 'timeliness', 'validity'
    check_name String,
    expected_value Float64,
    actual_value Float64,
    deviation_percent Float64,
    status LowCardinality(String),      -- 'pass', 'fail', 'warning'
    severity LowCardinality(String),    -- 'low', 'medium', 'high', 'critical'
    error_count UInt64,
    total_records UInt64,
    details String,
    affected_rows Array(UInt64)        -- Sample of problematic record IDs
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(check_time)
ORDER BY (table_name, check_type, check_time)
TTL check_time + toIntervalDay(180)
SETTINGS index_granularity = 8192;

-- Schema Change Tracking
CREATE TABLE IF NOT EXISTS schema_drift_monitoring (
    change_time DateTime DEFAULT now(),
    table_name String,
    column_name String,
    old_type String,
    new_type String,
    change_type LowCardinality(String),  -- 'type_change', 'column_added', 'column_removed', 'nullability_change'
    impact_level LowCardinality(String),  -- 'low', 'medium', 'high', 'critical'
    detected_by String,                  -- 'automated_check', 'manual_review', 'etl_error'
    details String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(change_time)
ORDER BY (table_name, column_name, change_time)
TTL change_time + toIntervalDay(365)
SETTINGS index_granularity = 8192;

-- Sync Lag Monitoring (CDC and Pipeline)
CREATE TABLE IF NOT EXISTS sync_lag_metrics (
    metric_time DateTime DEFAULT now(),
    source_system String,           -- 'mysql_cdc', 'kafka_consumer', 'etl_pipeline'
    target_table String,
    lag_seconds UInt64,
    source_timestamp DateTime,
    target_timestamp DateTime,
    records_behind UInt64,
    status LowCardinality(String),  -- 'synced', 'lagging', 'critical_lag'
    alert_threshold UInt64 DEFAULT 300
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(metric_time)
ORDER BY (source_system, target_table, metric_time)
TTL metric_time + toIntervalDay(30)
SETTINGS index_granularity = 8192;

-- Missing Event Detection
CREATE TABLE IF NOT EXISTS missing_event_alerts (
    alert_time DateTime DEFAULT now(),
    alert_type LowCardinality(String),  -- 'gap_detection', 'sequence_break', 'expected_event_missing'
    table_name String,
    time_gap_start DateTime,
    time_gap_end DateTime,
    missing_duration_seconds UInt64,
    expected_event_type LowCardinality(String),
    estimated_missing_count UInt64,
    status LowCardinality(String),      -- 'active', 'investigating', 'resolved'
    investigation_notes String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(alert_time)
ORDER BY (table_name, alert_type, alert_time)
TTL alert_time + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- Load Performance Monitoring
CREATE TABLE IF NOT EXISTS load_performance_metrics (
    load_time DateTime DEFAULT now(),
    table_name String,
    source_system String,
    batch_id String,
    records_processed UInt64,
    records_failed UInt64,
    processing_time_seconds Float64,
    throughput_records_per_second Float64,
    cpu_usage_percent Float64,
    memory_usage_mb UInt64,
    error_rate_percent Float64,
    status LowCardinality(String)       -- 'success', 'partial_failure', 'complete_failure'
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(load_time)
ORDER BY (table_name, batch_id, load_time)
TTL load_time + toIntervalDay(60)
SETTINGS index_granularity = 8192;

-- Data Lineage and Impact Tracking
CREATE TABLE IF NOT EXISTS data_lineage_tracking (
    lineage_time DateTime DEFAULT now(),
    source_table String,
    source_records UInt64,
    transformation_step String,
    target_table String,
    target_records UInt64,
    record_loss_count UInt64,
    record_loss_percent Float64,
    transformation_type LowCardinality(String),  -- 'filter', 'join', 'aggregation', 'enrichment'
    status LowCardinality(String),
    details String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(lineage_time)
ORDER BY (source_table, transformation_step, lineage_time)
TTL lineage_time + toIntervalDay(90)
SETTINGS index_granularity = 8192;

-- ------------------------------------------------------------------------------
-- DATA QUALITY VALIDATION FUNCTIONS
-- ------------------------------------------------------------------------------

-- Function to calculate completeness percentage
CREATE OR REPLACE FUNCTION completeness_check AS (target_table, column_list) -> round(
    100 * (
        sum(if(assumeNotNull(arrayAll(x -> x != '', arrayMap(x -> JSONExtractString(toString(JSONExtract(toString(toJSONString(*), '')), x)), splitByString(',', column_list)))) 1 else 0 end)) / 
        count()
    ), 2
);

-- Function to detect outliers using IQR method
CREATE OR REPLACE FUNCTION detect_outliers_iqr AS (column_values) -> arrayFilter(x -> 
    x < quantileExact(0.25)(column_values) - 1.5 * (quantileExact(0.75)(column_values) - quantileExact(0.25)(column_values)) or 
    x > quantileExact(0.75)(column_values) + 1.5 * (quantileExact(0.75)(column_values) - quantileExact(0.25)(column_values)), 
    column_values
);

-- Function to calculate data freshness in seconds
CREATE OR REPLACE FUNCTION data_freshness_seconds AS (timestamp_column) -> toUInt64(
    dateDiff('second', max(timestamp_column), now())
);

-- ------------------------------------------------------------------------------
-- MATERIALIZED VIEWS FOR AUTOMATED MONITORING
-- ------------------------------------------------------------------------------

-- Real-time completeness monitoring
CREATE MATERIALIZED VIEW IF NOT EXISTS completeness_monitoring_mv TO data_quality_checks AS
SELECT 
    'processed_events_storage' as table_name,
    'completeness' as check_type,
    'user_id_completeness' as check_name,
    100.0 as expected_value,
    round(100.0 * countIf(user_id > 0) / count(), 2) as actual_value,
    round(100.0 - (100.0 * countIf(user_id > 0) / count()), 2) as deviation_percent,
    if(round(100.0 * countIf(user_id > 0) / count()) >= 99.5, 'pass', if(round(100.0 * countIf(user_id > 0) / count()) >= 95.0, 'warning', 'fail')) as status,
    if(round(100.0 * countIf(user_id > 0) / count()) < 95.0, 'high', 'low') as severity,
    countIf(user_id = 0) as error_count,
    count() as total_records,
    concat('Missing user_id count: ', toString(countIf(user_id = 0))) as details,
    [] as affected_rows
FROM processed_events_storage
WHERE event_date >= today() - interval 1 day;

-- Duplicate detection monitoring
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_monitoring_mv TO data_quality_checks AS
SELECT 
    'processed_events_storage' as table_name,
    'accuracy' as check_type,
    'duplicate_events' as check_name,
    0.0 as expected_value,
    round(count() - count(DISTINCT event_id), 2) as actual_value,
    0.0 as deviation_percent,
    if(count() = count(DISTINCT event_id), 'pass', 'fail') as status,
    'medium' as severity,
    count() - count(DISTINCT event_id) as error_count,
    count() as total_records,
    concat('Duplicate event count: ', toString(count() - count(DISTINCT event_id))) as details,
    [] as affected_rows
FROM processed_events_storage
WHERE event_date >= today() - interval 1 day
HAVING count() != count(DISTINCT event_id);

-- Data freshness monitoring
CREATE MATERIALIZED VIEW IF NOT EXISTS freshness_monitoring_mv TO sync_lag_metrics AS
SELECT 
    'processed_events_storage' as target_table,
    data_freshness_seconds(event_time) as lag_seconds,
    max(event_time) as source_timestamp,
    now() as target_timestamp,
    countIf(event_time < now() - interval 1 hour) as records_behind,
    if(data_freshness_seconds(event_time) <= 300, 'synced', if(data_freshness_seconds(event_time) <= 3600, 'lagging', 'critical_lag')) as status
FROM processed_events_storage;

-- ------------------------------------------------------------------------------
-- STORED PROCEDURES FOR COMPREHENSIVE DATA QUALITY CHECKS
-- ------------------------------------------------------------------------------

-- Comprehensive pipeline health check
CREATE OR REPLACE PROCEDURE comprehensive_pipeline_health()
BEGIN
    -- 1. System Components Health
    INSERT INTO pipeline_health_status (component, metric_name, metric_value, status, threshold_value, details, alert_level)
    SELECT 
        'kafka_raw_events' as component,
        'consumer_lag' as metric_name,
        max(lag_seconds) as metric_value,
        if(max(lag_seconds) <= 300, 'healthy', if(max(lag_seconds) <= 1800, 'warning', 'critical')) as status,
        300.0 as threshold_value,
        concat('Kafka consumer lag: ', toString(max(lag_seconds)), ' seconds') as details,
        if(max(lag_seconds) > 1800, 2, if(max(lag_seconds) > 300, 1, 0)) as alert_level
    FROM sync_lag_metrics 
    WHERE source_system = 'kafka_consumer' 
      AND target_table = 'processed_events_storage'
      AND metric_time >= now() - interval 5 minute;

    -- 2. CDC Sync Health
    INSERT INTO pipeline_health_status (component, metric_name, metric_value, status, threshold_value, details, alert_level)
    SELECT 
        'mysql_cdc' as component,
        'sync_delay' as metric_name,
        max(lag_seconds) as metric_value,
        if(max(lag_seconds) <= 60, 'healthy', if(max(lag_seconds) <= 300, 'warning', 'critical')) as status,
        60.0 as threshold_value,
        concat('MySQL CDC sync delay: ', toString(max(lag_seconds)), ' seconds') as details,
        if(max(lag_seconds) > 300, 2, if(max(lag_seconds) > 60, 1, 0)) as alert_level
    FROM sync_lag_metrics 
    WHERE source_system = 'mysql_cdc' 
      AND metric_time >= now() - interval 5 minute;

    -- 3. Data Volume Health
    INSERT INTO pipeline_health_status (component, metric_name, metric_value, status, threshold_value, details, alert_level)
    SELECT 
        'processed_events' as component,
        'hourly_volume' as metric_value,
        count() as metric_value,
        if(count() >= 50, 'healthy', if(count() >= 20, 'warning', 'critical')) as status,
        50.0 as threshold_value,
        concat('Events in last hour: ', toString(count())) as details,
        if(count() < 20, 2, if(count() < 50, 1, 0)) as alert_level
    FROM processed_events_storage 
    WHERE event_time >= now() - interval 1 hour;

    -- 4. ClickHouse Materialized Views Health
    INSERT INTO pipeline_health_status (component, metric_name, metric_value, status, threshold_value, details, alert_level)
    SELECT 
        'materialized_views' as component,
        'active_views' as metric_name,
        count() as metric_value,
        if(count() >= 4, 'healthy', 'critical') as status,
        4.0 as threshold_value,
        concat('Active materialized views: ', toString(count())) as details,
        if(count() < 4, 2, 0) as alert_level
    FROM system.views 
    WHERE database = 'insurance_analytics';
END;

-- Schema drift detection procedure
CREATE OR REPLACE PROCEDURE detect_schema_drift()
BEGIN
    -- Check for new columns in processed_events_storage
    INSERT INTO schema_drift_monitoring (table_name, column_name, change_type, impact_level, detected_by, details)
    SELECT 
        'processed_events_storage' as table_name,
        name as column_name,
        'column_added' as change_type,
        if(name IN ('event_id', 'user_id', 'event_time'), 'high', 'low') as impact_level,
        'automated_check' as detected_by,
        concat('New column detected: ', name, ' of type: ', type) as details
    FROM system.columns 
    WHERE database = 'insurance_analytics' 
      AND table = 'processed_events_storage'
      AND name NOT IN (
          'event_id', 'event_time', 'user_id', 'session_id', 'event_type', 'channel', 
          'premium_amount', 'signup_date', 'city', 'device_type', 'user_tenure_days',
          'event_date', 'event_hour', 'event_day_of_week', 'is_purchase', 'processing_time',
          'data_source', 'ingestion_time'
      );
END;

-- Missing event gap detection
CREATE OR REPLACE PROCEDURE detect_missing_events()
BEGIN
    -- Detect gaps in event timestamps (potential missing events)
    INSERT INTO missing_event_alerts (alert_type, table_name, time_gap_start, time_gap_end, missing_duration_seconds, estimated_missing_count, status)
    SELECT 
        'gap_detection' as alert_type,
        'processed_events_storage' as table_name,
        min(event_time) as time_gap_start,
        max(event_time) as time_gap_end,
        toUInt64(dateDiff('second', min(event_time), max(event_time))) as missing_duration_seconds,
        toUInt64(dateDiff('second', min(event_time), max(event_time)) / 60) as estimated_missing_count,  -- Estimate: 1 event per minute
        'active' as status
    FROM (
        SELECT 
            event_time,
            lead(event_time) OVER (ORDER BY event_time) as next_event_time,
            dateDiff('second', event_time, lead(event_time) OVER (ORDER BY event_time)) as time_diff
        FROM processed_events_storage 
        WHERE event_time >= now() - interval 24 hour
        HAVING time_diff > 3600  -- Gaps larger than 1 hour
    )
    WHERE time_diff > 3600;
END;

-- Load performance analysis
CREATE OR REPLACE PROCEDURE analyze_load_performance()
BEGIN
    -- Calculate hourly load performance metrics
    INSERT INTO load_performance_metrics (table_name, source_system, batch_id, records_processed, records_failed, processing_time_seconds, throughput_records_per_second, error_rate_percent, status)
    SELECT 
        'processed_events_storage' as table_name,
        'kafka_consumer' as source_system,
        concat(toString(toHour(now())), '_', toString(toDate(now()))) as batch_id,
        count() as records_processed,
        0 as records_failed,  -- We'll calculate this from error logs in production
        3600.0 as processing_time_seconds,
        round(count() / 3600.0, 2) as throughput_records_per_second,
        0.0 as error_rate_percent,
        if(count() > 0, 'success', 'critical') as status
    FROM processed_events_storage 
    WHERE event_time >= now() - interval 1 hour;
END;

-- Data lineage tracking
CREATE OR REPLACE PROCEDURE track_data_lineage()
BEGIN
    -- Track data flow from raw events to aggregated tables
    INSERT INTO data_lineage_tracking (source_table, source_records, transformation_step, target_table, target_records, record_loss_count, record_loss_percent, transformation_type, status, details)
    SELECT 
        'processed_events_storage' as source_table,
        count() as source_records,
        'daily_aggregation' as transformation_step,
        'daily_aggregate_metrics' as target_table,
        countDistinct(metric_date) as target_records,  -- Simplified - actual count would be more complex
        count() - countDistinct(metric_date) as record_loss_count,
        round((count() - countDistinct(metric_date)) * 100.0 / count(), 2) as record_loss_percent,
        'aggregation' as transformation_type,
        if(count() > 0, 'success', 'critical') as status,
        concat('Events aggregated to daily metrics. Source: ', toString(count()), ', Target: ', toString(countDistinct(metric_date))) as details
    FROM processed_events_storage 
    WHERE event_date >= today() - interval 1 day;
END;

-- ------------------------------------------------------------------------------
-- ALERTING MATERIALIZED VIEWS
-- ------------------------------------------------------------------------------

-- Critical alerts dashboard
CREATE MATERIALIZED VIEW IF NOT EXISTS critical_alerts_mv TO pipeline_health_status AS
SELECT 
    component,
    'critical_alert' as metric_name,
    count() as metric_value,
    'critical' as status,
    0.0 as threshold_value,
    concat('Critical alert count: ', toString(count())) as details,
    2 as alert_level
FROM data_quality_checks 
WHERE severity = 'critical' 
  AND status = 'fail'
  AND check_time >= now() - interval 1 hour
GROUP BY component
HAVING count() > 0;

-- ------------------------------------------------------------------------------
-- SAMPLE DQ VALIDATION QUERIES
-- ------------------------------------------------------------------------------

/* 1. Comprehensive Pipeline Health Dashboard */
SELECT 
    component,
    groupArray((metric_name, metric_value, status, alert_level)) as metrics,
    max(alert_level) as max_severity
FROM pipeline_health_status 
WHERE check_time >= now() - interval 1 hour
GROUP BY component
ORDER BY max_severity DESC;

/* 2. Recent Data Quality Issues */
SELECT 
    table_name,
    check_type,
    check_name,
    status,
    severity,
    deviation_percent,
    check_time,
    details
FROM data_quality_checks 
WHERE status != 'pass'
  AND check_time >= now() - interval 24 hour
ORDER BY severity DESC, check_time DESC;

/* 3. Sync Lag Analysis */
SELECT 
    source_system,
    target_table,
    lag_seconds,
    status,
    records_behind,
    metric_time
FROM sync_lag_metrics 
WHERE lag_seconds > alert_threshold
  AND metric_time >= now() - interval 6 hour
ORDER BY lag_seconds DESC;

/* 4. Missing Event Patterns */
SELECT 
    alert_type,
    table_name,
    time_gap_start,
    time_gap_end,
    missing_duration_seconds,
    estimated_missing_count,
    status
FROM missing_event_alerts 
WHERE status = 'active'
  AND alert_time >= now() - interval 24 hour
ORDER BY missing_duration_seconds DESC;

/* 5. Schema Drift Summary */
SELECT 
    table_name,
    count() as changes_count,
    groupArray(DISTINCT change_type) as change_types,
    max(impact_level) as max_impact,
    max(change_time) as latest_change
FROM schema_drift_monitoring 
WHERE change_time >= now() - interval 7 day
GROUP BY table_name
ORDER BY changes_count DESC;

/* 6. Load Performance Trends */
SELECT 
    table_name,
    date_trunc('hour', load_time) as hour,
    sum(records_processed) as total_records,
    avg(throughput_records_per_second) as avg_throughput,
    max(error_rate_percent) as max_error_rate,
    count() as batch_count
FROM load_performance_metrics 
WHERE load_time >= now() - interval 24 hour
GROUP BY table_name, date_trunc('hour', load_time)
ORDER BY hour DESC, total_records DESC;

/* 7. Data Lineage Analysis */
SELECT 
    source_table,
    transformation_step,
    target_table,
    sum(source_records) as total_source_records,
    sum(target_records) as total_target_records,
    avg(record_loss_percent) as avg_loss_percent,
    count() as transformation_count
FROM data_lineage_tracking 
WHERE lineage_time >= now() - interval 24 hour
GROUP BY source_table, transformation_step, target_table
ORDER BY avg_loss_percent DESC;
*/