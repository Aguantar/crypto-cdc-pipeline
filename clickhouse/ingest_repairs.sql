-- 수리 계보: 유입 공백을 REST 원장으로 메운 기록(창 단위). docs/19 #14.
-- 왜 행 단위 플래그가 아닌가: 행 플래그는 MySQL·Debezium·Flink·ClickHouse 네 곳 스키마 변경이 필요하고,
-- 우리가 답해야 하는 질문("이 시간대가 수리된 구간인가")은 창 단위로 충분히 답한다.
-- 기록 주체: producer(재연결·기동 시 gap-fill), scripts/observe/backfill_trades.py(수동 백필).
CREATE TABLE IF NOT EXISTS cdc_pipeline.ingest_repairs
(
    repaired_at   DateTime,                 -- 수리 실행 시각(UTC)
    reason        LowCardinality(String),   -- reconnect | startup | manual
    window_start  DateTime64(3),            -- 대상 창(체결 시각 기준, UTC)
    window_end    DateTime64(3),
    markets       UInt16,                   -- 조회한 마켓 수
    rest_rows     UInt32,                   -- 원장에서 받은 체결 수
    inserted_rows UInt32,                   -- 실제 삽입(=누락이었던) 수
    elapsed_s     Float32,
    note          String DEFAULT ''
)
ENGINE = MergeTree
ORDER BY (window_start, reason);
