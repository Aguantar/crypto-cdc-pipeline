-- 2층 원장 소비 (docs/28 B, 2026-09-19): Kafka 엔진 + MV 로 Debezium 거울 이벤트(c/u/d)를 ReplacingMergeTree(version, is_deleted) 에 적재.
-- 왜 Flink 잡이 아닌가: 하루 수백 행에 잡 하나를 더 두면 슬롯·상태·재배포 비용만 는다. circuit-connect 와 같은 방식.
-- 봉투는 value.converter.schemas.enable=false 라 payload 래퍼 없이 최상위에 before/after/source/op/ts_ms 가 온다 (첫 스모크에서 'payload' 경로로 짜서 0행 - 정정).
-- 왜 JSONAsString: Debezium 봉투(before/after/op)를 한 스키마로 못 박지 않고 MV 에서 op 별로 after/before 를 고른다. 파싱 실패는 행이 안 생기므로 대조(3자)에서 드러난다.
-- 삭제: op=d 는 before 를 값으로, is_deleted=1, version 은 before.version+1 → 같은 키의 마지막 갱신보다 크므로 FINAL 에서 사라진다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.virtual_orders
(
    order_id        UInt64,
    symbol          LowCardinality(String),
    client_order_id String,
    side            LowCardinality(String),
    order_type      LowCardinality(String),
    time_in_force   LowCardinality(String),
    price           Decimal(20, 8),
    orig_qty        Decimal(20, 8),
    executed_qty    Decimal(20, 8),
    cum_quote_qty   Decimal(24, 8),
    status          LowCardinality(String),
    last_exec_type  LowCardinality(String),
    reject_reason   String,
    strategy        LowCardinality(String),
    fill_count      UInt32,
    created_ms      Int64,
    updated_ms      Int64,
    last_exec_id    Int64,
    version         UInt32,
    reset_epoch     UInt32,
    is_deleted      UInt8,
    op              LowCardinality(String),
    source_ts_ms    Int64 COMMENT 'binlog 시각',
    dbz_ts_ms       Int64 COMMENT 'Debezium 처리 시각',
    kafka_ts_ms     Int64 COMMENT 'Kafka append 시각 (엔진 가상 컬럼 _timestamp_ms)',
    ch_inserted_at  DateTime64(3) DEFAULT now64(3) COMMENT '폴링 시작 시각이라 행보다 최대 7.5초 앞선다 - 지연 계산에 쓰지 말 것(실측 09-19)'
)
ENGINE = ReplacingMergeTree(version, is_deleted)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(created_ms))
ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS cdc_pipeline.virtual_fills
(
    symbol           LowCardinality(String),
    fill_id          UInt64,
    order_id         UInt64,
    side             LowCardinality(String),
    price            Decimal(20, 8),
    qty              Decimal(20, 8),
    quote_qty        Decimal(24, 8),
    commission       Decimal(20, 8),
    commission_asset LowCardinality(String),
    is_maker         UInt8,
    filled_ms        Int64,
    exec_id          Int64,
    strategy         LowCardinality(String),
    version          Int64 COMMENT 'source_ts_ms - 불변 행이라 삭제(리셋)만 버전이 오른다',
    is_deleted       UInt8,
    op               LowCardinality(String),
    source_ts_ms     Int64,
    dbz_ts_ms        Int64,
    ch_inserted_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version, is_deleted)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(filled_ms))
ORDER BY (symbol, fill_id);

CREATE TABLE IF NOT EXISTS cdc_pipeline.virtual_positions
(
    as_of_day    Date,
    asset        LowCardinality(String),
    free         Decimal(24, 8),
    locked       Decimal(24, 8),
    snapshot_ms  Int64,
    reset_epoch  UInt32,
    is_deleted   UInt8,
    op           LowCardinality(String),
    source_ts_ms Int64,
    ch_inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(snapshot_ms, is_deleted)
ORDER BY (as_of_day, asset);

CREATE TABLE IF NOT EXISTS cdc_pipeline.binance_user_events
(
    event_id     UInt64,
    event_type   LowCardinality(String),
    dedup_key    String,
    event_ms     Int64,
    symbol       LowCardinality(String),
    order_id     UInt64,
    exec_type    LowCardinality(String),
    order_status LowCardinality(String),
    event_raw    String COMMENT '거래소 원문 JSON (큐 컬럼 raw 와 이름 충돌 → event_raw)',
    recv_ms      Int64,
    source_ts_ms Int64,
    kafka_ts_ms  Int64 COMMENT 'Kafka append 시각 (_timestamp_ms)',
    ch_inserted_at DateTime64(3) DEFAULT now64(3) COMMENT '폴링 시작 시각 - 지연 계산에 쓰지 말 것'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(event_ms))
ORDER BY (event_ms, event_id)
TTL toDateTime(fromUnixTimestamp64Milli(event_ms)) + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS cdc_pipeline.ledger_orders_queue (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', kafka_topic_list = 'ledger.crypto_db.virtual_orders', kafka_group_name = 'clickhouse-ledger-orders', kafka_format = 'JSONAsString', kafka_num_consumers = 1;
CREATE TABLE IF NOT EXISTS cdc_pipeline.ledger_fills_queue (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', kafka_topic_list = 'ledger.crypto_db.virtual_fills', kafka_group_name = 'clickhouse-ledger-fills', kafka_format = 'JSONAsString', kafka_num_consumers = 1;
CREATE TABLE IF NOT EXISTS cdc_pipeline.ledger_positions_queue (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', kafka_topic_list = 'ledger.crypto_db.virtual_positions', kafka_group_name = 'clickhouse-ledger-positions', kafka_format = 'JSONAsString', kafka_num_consumers = 1;
CREATE TABLE IF NOT EXISTS cdc_pipeline.ledger_events_queue (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', kafka_topic_list = 'ledger.crypto_db.binance_user_events', kafka_group_name = 'clickhouse-ledger-events', kafka_format = 'JSONAsString', kafka_num_consumers = 1;

-- op=d 이면 before, 아니면 after 를 행 값으로. r = 그 객체의 JSON 문자열.
CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_ledger_orders TO cdc_pipeline.virtual_orders AS
WITH JSONExtractString(raw, 'op') AS o,
     if(o = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS r
SELECT
    JSONExtractUInt(r, 'order_id') AS order_id, JSONExtractString(r, 'symbol') AS symbol, JSONExtractString(r, 'client_order_id') AS client_order_id,
    JSONExtractString(r, 'side') AS side, JSONExtractString(r, 'order_type') AS order_type, JSONExtractString(r, 'time_in_force') AS time_in_force,
    toDecimal64OrZero(JSONExtractString(r, 'price'), 8) AS price, toDecimal64OrZero(JSONExtractString(r, 'orig_qty'), 8) AS orig_qty,
    toDecimal64OrZero(JSONExtractString(r, 'executed_qty'), 8) AS executed_qty, toDecimal128OrZero(JSONExtractString(r, 'cum_quote_qty'), 8) AS cum_quote_qty,
    JSONExtractString(r, 'status') AS status, JSONExtractString(r, 'last_exec_type') AS last_exec_type, JSONExtractString(r, 'reject_reason') AS reject_reason,
    JSONExtractString(r, 'strategy') AS strategy, JSONExtractUInt(r, 'fill_count') AS fill_count,
    JSONExtractInt(r, 'created_ms') AS created_ms, JSONExtractInt(r, 'updated_ms') AS updated_ms, JSONExtractInt(r, 'last_exec_id') AS last_exec_id,
    toUInt32(JSONExtractUInt(r, 'version') + if(o = 'd', 1, 0)) AS version, JSONExtractUInt(r, 'reset_epoch') AS reset_epoch,
    if(o = 'd', 1, 0) AS is_deleted, o AS op,
    JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms, JSONExtractInt(raw, 'ts_ms') AS dbz_ts_ms, toUnixTimestamp64Milli(_timestamp_ms) AS kafka_ts_ms
FROM cdc_pipeline.ledger_orders_queue WHERE o IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_ledger_fills TO cdc_pipeline.virtual_fills AS
WITH JSONExtractString(raw, 'op') AS o,
     if(o = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS r
SELECT
    JSONExtractString(r, 'symbol') AS symbol, JSONExtractUInt(r, 'fill_id') AS fill_id, JSONExtractUInt(r, 'order_id') AS order_id, JSONExtractString(r, 'side') AS side,
    toDecimal64OrZero(JSONExtractString(r, 'price'), 8) AS price, toDecimal64OrZero(JSONExtractString(r, 'qty'), 8) AS qty, toDecimal128OrZero(JSONExtractString(r, 'quote_qty'), 8) AS quote_qty,
    toDecimal64OrZero(JSONExtractString(r, 'commission'), 8) AS commission, JSONExtractString(r, 'commission_asset') AS commission_asset,
    toUInt8(JSONExtractInt(r, 'is_maker')) AS is_maker, JSONExtractInt(r, 'filled_ms') AS filled_ms, JSONExtractInt(r, 'exec_id') AS exec_id, JSONExtractString(r, 'strategy') AS strategy,
    JSONExtractInt(raw, 'source', 'ts_ms') AS version, if(o = 'd', 1, 0) AS is_deleted, o AS op,
    JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms, JSONExtractInt(raw, 'ts_ms') AS dbz_ts_ms
FROM cdc_pipeline.ledger_fills_queue WHERE o IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_ledger_positions TO cdc_pipeline.virtual_positions AS
WITH JSONExtractString(raw, 'op') AS o,
     if(o = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS r
SELECT
    toDate(JSONExtractInt(r, 'as_of_day')) AS as_of_day, JSONExtractString(r, 'asset') AS asset,
    toDecimal128OrZero(JSONExtractString(r, 'free'), 8) AS free, toDecimal128OrZero(JSONExtractString(r, 'locked'), 8) AS locked,
    JSONExtractInt(r, 'snapshot_ms') AS snapshot_ms, JSONExtractUInt(r, 'reset_epoch') AS reset_epoch,
    if(o = 'd', 1, 0) AS is_deleted, o AS op, JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms
FROM cdc_pipeline.ledger_positions_queue WHERE o IN ('c', 'u', 'd', 'r');

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_ledger_events TO cdc_pipeline.binance_user_events AS
WITH JSONExtractRaw(raw, 'after') AS r
SELECT
    JSONExtractUInt(r, 'event_id') AS event_id, JSONExtractString(r, 'event_type') AS event_type, JSONExtractString(r, 'dedup_key') AS dedup_key,
    JSONExtractInt(r, 'event_ms') AS event_ms, JSONExtractString(r, 'symbol') AS symbol, JSONExtractUInt(r, 'order_id') AS order_id,
    JSONExtractString(r, 'exec_type') AS exec_type, JSONExtractString(r, 'order_status') AS order_status, JSONExtractString(r, 'raw') AS event_raw,
    JSONExtractInt(r, 'recv_ms') AS recv_ms, JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms, toUnixTimestamp64Milli(_timestamp_ms) AS kafka_ts_ms
FROM cdc_pipeline.ledger_events_queue WHERE JSONExtractString(raw, 'op') IN ('c', 'r');

-- 3자 대조 행 (생성기가 시간당 거래소 vs MySQL 을 기록, CDC 로 도착). dbt dq_ledger_daily 가 ClickHouse FINAL 수를 붙여 3자를 완성.
CREATE TABLE IF NOT EXISTS cdc_pipeline.ledger_reconcile
(
    reconciled_ms Int64, as_of_day Date, symbol LowCardinality(String),
    ex_orders UInt32, ex_filled UInt32, ex_canceled UInt32, ex_open UInt32, ex_exec_qty Decimal(24, 8), ex_trades UInt32, ex_trade_qty Decimal(24, 8),
    my_orders UInt32, my_filled UInt32, my_canceled UInt32, my_open UInt32, my_exec_qty Decimal(24, 8), my_trades UInt32, my_trade_qty Decimal(24, 8),
    mismatch UInt8, detail String, source_ts_ms Int64, ch_inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (as_of_day, symbol, reconciled_ms);

CREATE TABLE IF NOT EXISTS cdc_pipeline.ledger_reconcile_queue (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', kafka_topic_list = 'ledger.crypto_db.ledger_reconcile', kafka_group_name = 'clickhouse-ledger-reconcile', kafka_format = 'JSONAsString', kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_ledger_reconcile TO cdc_pipeline.ledger_reconcile AS
WITH JSONExtractRaw(raw, 'after') AS r
SELECT
    JSONExtractInt(r, 'reconciled_ms') AS reconciled_ms, toDate(JSONExtractInt(r, 'as_of_day')) AS as_of_day, JSONExtractString(r, 'symbol') AS symbol,
    JSONExtractUInt(r, 'ex_orders') AS ex_orders, JSONExtractUInt(r, 'ex_filled') AS ex_filled, JSONExtractUInt(r, 'ex_canceled') AS ex_canceled, JSONExtractUInt(r, 'ex_open') AS ex_open,
    toDecimal128OrZero(JSONExtractString(r, 'ex_exec_qty'), 8) AS ex_exec_qty, JSONExtractUInt(r, 'ex_trades') AS ex_trades, toDecimal128OrZero(JSONExtractString(r, 'ex_trade_qty'), 8) AS ex_trade_qty,
    JSONExtractUInt(r, 'my_orders') AS my_orders, JSONExtractUInt(r, 'my_filled') AS my_filled, JSONExtractUInt(r, 'my_canceled') AS my_canceled, JSONExtractUInt(r, 'my_open') AS my_open,
    toDecimal128OrZero(JSONExtractString(r, 'my_exec_qty'), 8) AS my_exec_qty, JSONExtractUInt(r, 'my_trades') AS my_trades, toDecimal128OrZero(JSONExtractString(r, 'my_trade_qty'), 8) AS my_trade_qty,
    toUInt8(JSONExtractInt(r, 'mismatch')) AS mismatch, JSONExtractString(r, 'detail') AS detail, JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms
FROM cdc_pipeline.ledger_reconcile_queue WHERE JSONExtractString(raw, 'op') IN ('c', 'r');

-- C. 케이스 (거울 모드 두 번째 사례): 사람이 바꾸는 status·verdict 가 CDC 로 온다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.cases
(
    case_id UInt64, case_type LowCardinality(String), subject LowCardinality(String), evidence_key String, evidence String,
    opened_ms Int64, status LowCardinality(String), verdict LowCardinality(String), note String, assignee String, updated_ms Int64,
    version UInt32, is_deleted UInt8, op LowCardinality(String), source_ts_ms Int64, kafka_ts_ms Int64, ch_inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (case_id);

CREATE TABLE IF NOT EXISTS cdc_pipeline.ledger_cases_queue (raw String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka-1:29092', kafka_topic_list = 'ledger.crypto_db.cases', kafka_group_name = 'clickhouse-ledger-cases', kafka_format = 'JSONAsString', kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_pipeline.mv_ledger_cases TO cdc_pipeline.cases AS
WITH JSONExtractString(raw, 'op') AS o,
     if(o = 'd', JSONExtractRaw(raw, 'before'), JSONExtractRaw(raw, 'after')) AS r
SELECT
    JSONExtractUInt(r, 'case_id') AS case_id, JSONExtractString(r, 'case_type') AS case_type, JSONExtractString(r, 'subject') AS subject,
    JSONExtractString(r, 'evidence_key') AS evidence_key, JSONExtractString(r, 'evidence') AS evidence, JSONExtractInt(r, 'opened_ms') AS opened_ms,
    JSONExtractString(r, 'status') AS status, JSONExtractString(r, 'verdict') AS verdict, JSONExtractString(r, 'note') AS note, JSONExtractString(r, 'assignee') AS assignee,
    JSONExtractInt(r, 'updated_ms') AS updated_ms, toUInt32(JSONExtractUInt(r, 'version') + if(o = 'd', 1, 0)) AS version, if(o = 'd', 1, 0) AS is_deleted, o AS op,
    JSONExtractInt(raw, 'source', 'ts_ms') AS source_ts_ms, toUnixTimestamp64Milli(_timestamp_ms) AS kafka_ts_ms
FROM cdc_pipeline.ledger_cases_queue WHERE o IN ('c', 'u', 'd', 'r');
