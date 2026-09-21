-- 운영 지표·알럿 이력 (docs/32, 2026-09-20). Prometheus 대신 5분 cron 이 한 행씩 넣는다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.ops_metrics_5m
(
    ts DateTime, load1 Float32, load5 Float32, mem_used_mb UInt32, mem_avail_mb UInt32, swap_used_mb UInt32, disk_used_mb UInt32, disk_free_mb UInt32,
    cpu_kafka Float32, cpu_tm Float32, cpu_ch Float32, cpu_mysql Float32, cpu_collectors Float32,
    mem_kafka_mb UInt32, mem_tm_mb UInt32, mem_ch_mb UInt32, mem_mysql_mb UInt32, mem_scheduler_mb UInt32,
    upbit_trades_5m UInt32, binance_trades_5m UInt32, orderbook_5m UInt32, upbit_e2e_p95_s Float32, binance_e2e_p95_s Float32,
    flink_running UInt8, flink_busy_max Float32
)
ENGINE = MergeTree ORDER BY ts TTL ts + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS cdc_pipeline.alert_events
(
    fired_at DateTime, source LowCardinality(String), name LowCardinality(String), severity LowCardinality(String), message String, dedup_key String
)
ENGINE = MergeTree ORDER BY (fired_at, name) TTL fired_at + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS cdc_pipeline.ops_digest
(
    week_start Date, generated_at DateTime, body String
)
ENGINE = ReplacingMergeTree(generated_at) ORDER BY week_start;
