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

-- 2026-09-24 (docs/46): 호스트 cron 의 생존 신호. 전이만 적재하는 표(upbit_market_state_events)는 "조용한 것" 과
-- "죽은 것" 을 구분 못 해 Cron Freshness 가 매일 오경보를 냈다(09-20 06:09 이후 상태 변화 0 = 5,441분 정지로 판정).
-- 폴링이 성공할 때마다 한 행. 판정은 이 표의 max(ts) 로 한다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.cron_heartbeats
(
    job    LowCardinality(String),   -- market_state 등 cron 이름
    ts     DateTime,                 -- 폴링 성공 시각(UTC)
    detail String                    -- 그 폴링의 요약 (마켓 수 등)
)
ENGINE = MergeTree ORDER BY (job, ts) TTL ts + INTERVAL 30 DAY;

-- 2026-09-25 (docs/48 §7, docs/46): 수집기 자기 지표. 09-23 급등 때 수집기가 12.8초 뒤처져 3.6% 를 잃었는데
-- deliv_err·buf_err 는 0 이었고 대조만 잡았다. STATS 줄의 lag·queue 를 5분마다 남겨 "지금 잃고 있다" 를 health_check 가 보게 한다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.collector_stats_5m
(
    ts DateTime, collector LowCardinality(String),
    recv UInt64, produced UInt64, deliv_err UInt32, buf_err UInt32, queue UInt32, conns UInt8, reconnects UInt32,
    lag_p50_ms UInt32, lag_p95_ms UInt32
)
ENGINE = MergeTree ORDER BY (collector, ts) TTL ts + INTERVAL 90 DAY;
