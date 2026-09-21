{{
    config(
        materialized='table',
        order_by='market, hour_kst'
    )
}}

-- 2026-09-20 (docs/34 #5) 타입 규약: 금액·수량 합계는 Decimal, 비율은 Float64.
--   이유: Decimal 나눗셈은 분모가 0 이면 예외를 던져 모델 전체가 실패한다(Float64 는 조용히 inf). if(v>0, a/v, 0) 가드도 ClickHouse 가 양쪽 분기를 다 계산해 소용없다(09-20 실측).
--   비율은 어차피 근사라 Float64 가 의미상으로도 맞다.
-- 1시간봉 OHLCV 집계
-- 2026-09-19 Flink 5분 처리 시간 집계는 폐기(정지 뒤 따라붙는 행이 '지금' 창을 왜곡, docs/29 §7), 표는 09-20 삭제. 이벤트 시각 집계는 여기서만 만든다:
--   Flink = 실시간 5분 윈도우 (스트리밍)
--   DBT  = 배치 1시간봉 (raw 틱 데이터 기반, 더 정확한 OHLCV)
SELECT
    market,
    toStartOfHour(trade_time_kst) AS hour_kst,
    argMin(trade_price, trade_time_kst) AS open,
    max(trade_price) AS high,
    min(trade_price) AS low,
    argMax(trade_price, trade_time_kst) AS close,
    sum(trade_volume) AS volume,
    sum(trade_amount) AS amount,
    count(*) AS trade_count,
    countIf(ask_bid = 'BID') AS bid_count,
    countIf(ask_bid = 'ASK') AS ask_count,
    -- VWAP (거래량 가중 평균 가격)
    ifNull(toFloat64(sum(trade_amount)) / nullIf(toFloat64(sum(trade_volume)), 0), 0) AS vwap
FROM {{ ref('stg_trades') }}
GROUP BY market, toStartOfHour(trade_time_kst)
