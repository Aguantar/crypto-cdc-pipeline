CREATE DATABASE IF NOT EXISTS cdc_pipeline;
USE cdc_pipeline;

-- ============================================
-- 1. Raw 체결 이벤트 (Flink → ClickHouse)
-- ============================================
CREATE TABLE IF NOT EXISTS crypto_trades
(
    op              LowCardinality(String),
    trade_id        UInt64,
    market          LowCardinality(String),   -- KRW-BTC, KRW-ETH 등
    trade_price     Float64,          -- 체결 가격 (KRW)
    trade_volume    Float64,          -- 체결 수량
    trade_amount    Float64,          -- 체결 금액 (price × volume)
    ask_bid         LowCardinality(String),   -- ASK(매도) / BID(매수)
    upbit_timestamp Int64,            -- Upbit 체결 시각 (Unix ms) = 이벤트 시각. 파티션·정렬·TTL 의 기준
    sequential_id   Int64,            -- Upbit 체결 고유 ID (market 과 함께 체결의 정체성)
    source_ts       DateTime64(3),    -- MySQL 변경 시각 (binlog)
    cdc_ts          DateTime64(3),    -- Debezium 처리 시각
    cdc_latency_ms  Int64,            -- CDC 레이턴시
    flink_ts        DateTime64(3),    -- Flink 처리 시각 (RMT 버전)
    best_ask_price  Nullable(Float64),
    best_ask_size   Nullable(Float64),
    best_bid_price  Nullable(Float64),
    best_bid_size   Nullable(Float64),
    inserted_at     DateTime64(3) DEFAULT now64(3),
    recv_ms         Nullable(Int64),                              -- producer WS 수신 epoch ms (Flink 가 채우는 것은 Kafka 창2부터)
    ingest_source   LowCardinality(String) DEFAULT 'ws',          -- ws | gapfill | backfill
    stream_type     LowCardinality(String) DEFAULT 'REALTIME'     -- Upbit stream_type (REALTIME | SNAPSHOT)
)
ENGINE = ReplacingMergeTree(flink_ts)   -- 2026-09-18 (docs/25): Connect 재시작 재전송 중복(27+239 실측) 을 저장 층에서 제거. 중복 키 = ORDER BY, 최신 flink_ts 유지
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(upbit_timestamp))   -- 2026-09-19 (docs/28 A-5·A-7): 조회·삭제 기준 = 체결 시각. 옛 binlog 월 파티션은 프루닝이 안 됐다(1시간 조회 39/39 파트 → 5/56)
ORDER BY (market, upbit_timestamp, sequential_id)                   -- 중복 키 = 업무 정체성. 옛 (market, source_ts, trade_id) 는 재스냅샷 중복(2월 4행)을 못 걸렀다
TTL toDateTime(fromUnixTimestamp64Milli(upbit_timestamp)) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;


-- ============================================
-- 2. 5분 윈도우 집계 (Flink Window → ClickHouse)
-- ============================================
CREATE TABLE IF NOT EXISTS trade_aggregations
(
    market          String,
    window_start    DateTime64(3),
    window_end      DateTime64(3),
    trade_count     UInt64,
    bid_count       UInt64,
    ask_count       UInt64,
    total_amount    Float64,
    total_volume    Float64,
    avg_price       Float64,
    min_price       Float64,
    max_price       Float64,
    vwap            Float64,          -- 거래량 가중 평균 가격
    inserted_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (market, window_start)
TTL toDateTime(window_start) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;


-- ============================================
-- 3. 이상 탐지 알림 (Flink Anomaly → ClickHouse)
-- ============================================
CREATE TABLE IF NOT EXISTS anomaly_alerts
(
    alert_type      String,           -- LARGE_TRADE, PRICE_SPIKE, VOLUME_SURGE, RAPID_TRADES
    market          String,
    trade_id        UInt64,
    message         String,
    value           Float64,
    threshold       Float64,
    detected_at     DateTime64(3),
    inserted_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (market, detected_at, alert_type)
TTL toDateTime(detected_at) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;


-- ============================================
-- 4. E2E 레이턴시 모니터링용 Materialized View
-- ============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latency_stats
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(minute)
ORDER BY (minute)
AS
SELECT
    toStartOfMinute(source_ts) AS minute,
    avgState(cdc_latency_ms)   AS avg_latency,
    maxState(cdc_latency_ms)   AS max_latency,
    minState(cdc_latency_ms)   AS min_latency,
    countState()               AS event_count
FROM crypto_trades
WHERE op IN ('c', 'u', 'd')
GROUP BY minute;
