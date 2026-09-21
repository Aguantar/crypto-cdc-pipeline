-- 원장 대조 참조 데이터: 업비트 REST 시간봉(거래소 진실값). Airflow reconcile_trades DAG 가 매일 적재.
-- 왜 테이블로 남기나: 대조 결과만 남기면 재계산·감사가 불가. 참조값 원본을 보관해야 dbt 모델이 비율을 재현할 수 있다.
-- ReplacingMergeTree(fetched_at): 같은 (market, hour) 재적재 시 최신본으로 대체 → 재실행·백필이 멱등.
-- TTL 90일: 대조는 최근 구간이 목적. 장기 보관은 일봉(upbit_daily_candles)이 담당.
CREATE TABLE IF NOT EXISTS cdc_pipeline.upbit_hourly_candles
(
    market      LowCardinality(String),
    hour_utc    DateTime,          -- 시간봉 시작(UTC)
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      Float64,           -- candle_acc_trade_volume (코인 수량)
    amount      Float64,           -- candle_acc_trade_price (KRW)
    fetched_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (market, hour_utc)
TTL hour_utc + INTERVAL 90 DAY;
