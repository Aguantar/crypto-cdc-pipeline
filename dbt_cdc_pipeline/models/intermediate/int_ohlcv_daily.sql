{{
    config(
        materialized='table',
        order_by='market, day_kst'
    )
}}

-- 2026-09-20 (docs/34 #5) 타입 규약: 금액·수량 합계는 Decimal, 비율은 Float64.
--   이유: Decimal 나눗셈은 분모가 0 이면 예외를 던져 모델 전체가 실패한다(Float64 는 조용히 inf). if(v>0, a/v, 0) 가드도 ClickHouse 가 양쪽 분기를 다 계산해 소용없다(09-20 실측).
--   비율은 어차피 근사라 Float64 가 의미상으로도 맞다.
-- 일봉 OHLCV 집계
SELECT
    market,
    day_kst,
    argMin(trade_price, trade_time_kst) AS open,
    max(trade_price) AS high,
    min(trade_price) AS low,
    argMax(trade_price, trade_time_kst) AS close,
    sum(trade_volume) AS volume,
    sum(trade_amount) AS amount,
    count(*) AS trade_count,
    countIf(ask_bid = 'BID') AS bid_count,
    countIf(ask_bid = 'ASK') AS ask_count,
    ifNull(toFloat64(sum(trade_amount)) / nullIf(toFloat64(sum(trade_volume)), 0), 0) AS vwap,
    -- 일중 변동폭 (%)
    ifNull(round((toFloat64(max(trade_price)) - toFloat64(min(trade_price))) / nullIf(toFloat64(min(trade_price)), 0) * 100, 2), 0) AS daily_range_pct
FROM {{ ref('stg_trades') }}
GROUP BY market, day_kst
