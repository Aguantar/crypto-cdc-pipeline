-- Binance 체결 (docs/31 §3-2, 2026-09-20). 시세이지 원장이 아니다 - Kafka 직행 → Flink → 여기.
-- RMT(recv_ms): 수집기 재연결(23시간 선제·오류)에서 같은 체결이 두 번 올 수 있다(at-least-once). 키 = (symbol, trade_id) 가 거래소 정체성.
-- 일 파티션: 3,100만 행/일. TTL 30일: 1GB/일 이라 1년은 375GB(디스크 466GB) → 안 된다. 조회는 전부 체결 시각(trade_ms) 기준.
CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_trades
(
    symbol          LowCardinality(String),
    trade_id        UInt64,
    price           Decimal(20, 8),    -- 2026-09-20 Float64 → Decimal (docs/34 #5). 이 파일이 옛 타입으로 남아 있던 것을 09-28 에 실물과 맞췄다
    qty             Decimal(20, 8),
    quote_qty       Decimal(38, 16),
    is_buyer_maker  UInt8,
    trade_ms        Int64 COMMENT 'T 거래소 체결 시각',
    event_ms        Int64 COMMENT 'E 거래소 이벤트 시각',
    recv_ms         Int64 COMMENT '수집기 수신 시각',
    flink_ts        DateTime64(3),
    inserted_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(recv_ms)
PARTITION BY toDate(fromUnixTimestamp64Milli(trade_ms))
ORDER BY (symbol, trade_ms, trade_id)
TTL toDateTime(fromUnixTimestamp64Milli(trade_ms)) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- 대조 정답: REST klines 1h 의 "체결 수"(n). reconcile_binance DAG 가 일 1회 적재.
CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_hourly_candles
(
    symbol      LowCardinality(String),
    hour_utc    DateTime,
    open        Float64, high Float64, low Float64, close Float64,
    volume      Float64,
    quote_volume Float64,
    trade_count UInt32,
    fetched_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (symbol, hour_utc)
TTL hour_utc + INTERVAL 60 DAY;

-- 2단계 호가장 재구성 산출물 (docs/31 §3-3): Upbit 호가와 같은 스키마 → 두 거래소 비교가 바로 된다. level=20 (재구성 호가장의 상위 20).
CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_orderbook_raw AS cdc_pipeline.orderbook_raw
ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (market, ts) TTL toDateTime(ts) + INTERVAL 7 DAY;
CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_orderbook_1m AS cdc_pipeline.orderbook_1m
ENGINE = MergeTree PARTITION BY toYYYYMM(window_start) ORDER BY (market, window_start) TTL window_start + INTERVAL 365 DAY;

-- 2026-09-20 (docs/34 #4): Binance 심볼 마스터(exchangeInfo). dim_coins 의 원천 - base/quote 를 문자열 치환이 아니라 거래소가 준 값으로.
CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_symbols
(
    symbol LowCardinality(String), base_asset LowCardinality(String), quote_asset LowCardinality(String), status LowCardinality(String),
    is_spot UInt8, fetched_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at) ORDER BY symbol;


-- 2026-09-28 (docs/50): Binance 체결 적재를 Flink 잡에서 ClickHouse Kafka 엔진 + MV 로 옮겼다. 파싱·적재만 하는 상태 없는 잡이라
-- 47시간 병행 대조에서 값이 같았고(4,930만 행, 셀 불일치 0), 파트 −59%·머지 −72%, 대가는 지연 +0.7초와 기준선 메모리 +40 MiB.
-- 주의: 새 컨슈머 그룹은 표를 만들기 전에 오프셋을 latest 로 만들어 둔다. 없으면 earliest 부터(보존 3일치) 되감아 읽는다.
--   docker exec cdc-kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:29092 --group clickhouse-binance-trades --topic binance.trades.v1 --reset-offsets --to-latest --execute
CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_trades_queue (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', kafka_topic_list = 'binance.trades.v1', kafka_group_name = 'clickhouse-binance-trades',
                        kafka_format = 'JSONAsString', kafka_num_consumers = 2, kafka_flush_interval_ms = 3000;   -- flush 3초 = 옛 JDBC 싱크의 배치 간격

-- 파서의 DLQ 에 해당: 키 필드가 없는 원문을 남긴다
CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_trades_mv_dlq (raw String, seen_at DateTime64(3) DEFAULT now64(3))
ENGINE = MergeTree ORDER BY seen_at TTL toDateTime(seen_at) + INTERVAL 7 DAY;
CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_binance_trades_dlq TO cdc_pipeline.binance_trades_mv_dlq AS
SELECT raw FROM cdc_pipeline.binance_trades_queue
WHERE NOT (JSONExtractString(raw, 's') != '' AND JSONExtractUInt(raw, 't') > 0 AND JSONExtractInt(raw, 'T') > 0);

-- 본체. 문자열 → Decimal 은 정확하다(Float 경유 금지, docs/34 #5). quote_qty = price × qty 는 Decimal(38,16) 로 파서와 같은 스케일.
-- flink_ts 컬럼 이름은 유지한다(dbt·대시보드가 읽는다). 뜻은 "적재 시각".
CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_binance_trades_live TO cdc_pipeline.binance_trades AS
SELECT JSONExtractString(raw, 's') AS symbol, JSONExtractUInt(raw, 't') AS trade_id,
       toDecimal128(JSONExtractString(raw, 'p'), 8) AS price, toDecimal128(JSONExtractString(raw, 'q'), 8) AS qty,
       price * qty AS quote_qty, toUInt8(JSONExtractBool(raw, 'm')) AS is_buyer_maker,
       JSONExtractInt(raw, 'T') AS trade_ms, if(JSONHas(raw, 'E'), JSONExtractInt(raw, 'E'), trade_ms) AS event_ms, JSONExtractInt(raw, 'recv_ms') AS recv_ms,
       now64(3) AS flink_ts
FROM cdc_pipeline.binance_trades_queue
WHERE symbol != '' AND trade_id > 0 AND trade_ms > 0 AND price >= 0 AND qty >= 0;
