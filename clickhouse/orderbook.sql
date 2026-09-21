-- 호가(orderbook) 테이블 (2026-09-09, 3차). 라이브 적용: docker exec cdc-clickhouse clickhouse-client --multiquery < clickhouse/orderbook.sql
-- 원본: Upbit WS orderbook .15 (15단 전체 스냅샷), 수집기 → Kafka upbit.orderbook.v1 → Flink OrderbookJob → 여기
CREATE TABLE IF NOT EXISTS cdc_pipeline.orderbook_raw
(
    market          LowCardinality(String),
    ts              DateTime64(3),          -- 업비트 호가 타임스탬프(tms)
    level           Float64,                -- 호가 모아보기 단위 (0 = 기본)
    total_ask_size  Float64,
    total_bid_size  Float64,
    ask_prices      Array(Float64),         -- 15단, 최우선 순
    ask_sizes       Array(Float64),
    bid_prices      Array(Float64),
    bid_sizes       Array(Float64),
    stream_type     LowCardinality(String), -- SNAPSHOT / REALTIME
    recv_ts         DateTime64(3),          -- 수집기 수신 시각
    flink_ts        DateTime64(3),          -- Flink sink 시각
    inserted_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (market, ts)
TTL toDateTime(ts) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- 1분 파생지표 (365일 보관). imbalance = (bid_size - ask_size) / (bid_size + ask_size), 상위 N단 합
CREATE TABLE IF NOT EXISTS cdc_pipeline.orderbook_1m
(
    market          LowCardinality(String),
    window_start    DateTime,
    window_end      DateTime,
    snapshots       UInt32,
    mid_open        Float64,
    mid_close       Float64,
    mid_min         Float64,
    mid_max         Float64,
    spread_avg      Float64,
    spread_bp_avg   Float64,
    spread_bp_max   Float64,
    imb1_avg        Float64,
    imb5_avg        Float64,
    imb15_avg       Float64,
    ask_depth15_avg Float64,
    bid_depth15_avg Float64,
    total_ask_avg   Float64,
    total_bid_avg   Float64,
    recv_lag_ms_avg Float64,                -- recv_ts - ts 평균 (수집 지연)
    flink_ts        DateTime64(3),
    inserted_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (market, window_start)
TTL window_start + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;
