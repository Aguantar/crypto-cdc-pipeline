{{
    config(
        materialized='table',
        order_by='day_kst, market'
    )
}}

-- 2026-09-20 (docs/34 #5) 타입 규약: 금액·수량 합계는 Decimal, 비율은 Float64.
--   이유: Decimal 나눗셈은 분모가 0 이면 예외를 던져 모델 전체가 실패한다(Float64 는 조용히 inf). if(v>0, a/v, 0) 가드도 ClickHouse 가 양쪽 분기를 다 계산해 소용없다(09-20 실측).
--   비율은 어차피 근사라 Float64 가 의미상으로도 맞다.
-- 일별 종합 리포트 (종목별 고가/저가/거래량/VWAP)
-- Grafana 일별 코인 시세 대시보드 데이터소스
SELECT
    d.market,
    d.day_kst,
    d.open,
    d.high,
    d.low,
    d.close,
    d.volume,
    d.amount,
    d.trade_count,
    d.bid_count,
    d.ask_count,
    d.vwap,
    d.daily_range_pct,
    -- 매수/매도 비율
    if(d.trade_count > 0,
       round(d.bid_count / d.trade_count * 100, 1),
       0
    ) AS bid_ratio_pct,
    -- 전일 대비 종가 변동률
    ifNull(round((toFloat64(d.close) - toFloat64(prev.close)) / nullIf(toFloat64(prev.close), 0) * 100, 2), 0) AS close_change_pct,
    -- 전일 대비 거래량 변동률
    ifNull(round((toFloat64(d.volume) - toFloat64(prev.volume)) / nullIf(toFloat64(prev.volume), 0) * 100, 2), 0) AS volume_change_pct
FROM {{ ref('int_ohlcv_daily') }} AS d
LEFT JOIN {{ ref('int_ohlcv_daily') }} AS prev
    ON d.market = prev.market
    AND d.day_kst = prev.day_kst + 1
ORDER BY d.day_kst DESC, d.amount DESC
