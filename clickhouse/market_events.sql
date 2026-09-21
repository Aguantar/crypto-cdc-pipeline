-- 업비트 시장경보(주의/유의 종목) 지정 상태 라벨. scripts/labels/poll_market_events.py 가 1분 폴링으로 전이만 적재.
-- 용도: 이상탐지 규칙의 정답(ground truth). 이력 API가 없어 폴링 시작 시점(2026-09-16)부터만 존재.
CREATE TABLE IF NOT EXISTS cdc_pipeline.upbit_market_events
(
    observed_at DateTime,                 -- 폴링 시각(UTC), 전이가 관측된 분
    market      LowCardinality(String),
    flag        LowCardinality(String),   -- WARNING | PRICE_FLUCTUATIONS | TRADING_VOLUME_SOARING | DEPOSIT_AMOUNT_SOARING | GLOBAL_PRICE_DIFFERENCES | CONCENTRATION_OF_SMALL_ACCOUNTS
    state       UInt8,                    -- 1 지정, 0 해제
    kind        LowCardinality(String)    -- 'transition' | 'snapshot'(폴러 시작·일 1회 전체 상태)
)
ENGINE = MergeTree
ORDER BY (market, flag, observed_at);
