-- 업비트 시장경보 지정/해제 이력(거래소 원본). 출처: 업비트 웹 "시장경보 현황" 페이지가 쓰는
-- https://crix-api-cdn.upbit.com/v1/crix/market-event-records (조회 창 최대 30일, size 최대 200, 이력 2026-03-20~).
-- scripts/labels/fetch_market_event_records.py 가 백필·증분 동기화. 이상탐지 규칙의 정답(ground truth).
CREATE TABLE IF NOT EXISTS cdc_pipeline.upbit_market_event_records
(
    market            LowCardinality(String),   -- KRW-BTC (code 의 CRIX.UPBIT. 접두 제거)
    event_type        LowCardinality(String),   -- PRICE_FLUCTUATIONS | TRADING_VOLUME_SOARING | DEPOSIT_AMOUNT_SOARING | GLOBAL_PRICE_DIFFERENCES | CONCENTRATION_OF_SMALL_ACCOUNTS
    warning_level     LowCardinality(String),   -- LEVEL_1 주의 | LEVEL_2 경고 | LEVEL_3 위험
    trigger_type      LowCardinality(String),   -- TRIGGER = 현재 지정 중(미해제) | RELEASE = 해제됨(expiration_time_utc 가 해제 시각)
    trigger_time_utc  DateTime,
    expiration_time_utc Nullable(DateTime),
    fetched_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (market, event_type, warning_level, trigger_time_utc);  -- 한 에피소드 = 한 행. TRIGGER(진행 중) → RELEASE(해제 시각 확정) 갱신은 fetched_at 버전으로 대체
