-- 이상탐지 v2 (docs/16 §5, docs/22). 구 anomaly_alerts(LARGE_TRADE·PRICE_SPIKE·VOLUME_SURGE)는 근거 없음으로 폐기, 이력 보존용으로만 유지.
-- 왜 새 테이블인가: 의미가 다르다. 구 테이블은 "체결 1건에 대한 판정", 이 테이블은 "마켓의 상태(등급) 전이". 섀도 기간엔 발송 없이 여기에만 쓴다.
-- rule_version: 섀도('v2-shadow') → 승격('v2') 을 데이터로 구분. 평가 모델(dbt dq_rule_eval_daily)이 거래소 지정 이력과 대조한다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.market_alerts
(
    alert_type    LowCardinality(String),   -- PRICE_24H (Flink, 체결마다) | VOLUME_24H (dbt, 일 1회)
    market        LowCardinality(String),
    level         UInt8,                    -- 0 해제 | 1 주의(±50%) | 2 경고(±100%) | 3 위험(±200%)
    prev_level    UInt8,
    event_time    DateTime64(3),            -- 판정 근거가 된 체결의 거래소 시각 (이벤트 시각)
    detected_at   DateTime64(3),            -- 처리 시각
    value         Float64,                  -- PRICE_24H: 24h 변동률(%) / VOLUME_24H: 7일 평균 대비 배수
    threshold     Float64,                  -- 이 등급의 임계 (50/100/200 또는 4)
    ref_price     Float64,                  -- 24h 전 분 종가 (PRICE_24H)
    price         Float64,                  -- 현재가
    trade_id      UInt64,
    rule_version  LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (market, event_time);
