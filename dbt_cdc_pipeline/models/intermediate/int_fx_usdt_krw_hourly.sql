{{ config(materialized='table', order_by='hour_utc') }}
-- 2026-09-20 (docs/34 #5) 타입 규약: 금액·수량 합계는 Decimal, 비율은 Float64.
--   이유: Decimal 나눗셈은 분모가 0 이면 예외를 던져 모델 전체가 실패한다(Float64 는 조용히 inf). if(v>0, a/v, 0) 가드도 ClickHouse 가 양쪽 분기를 다 계산해 소용없다(09-20 실측).
--   비율은 어차피 근사라 Float64 가 의미상으로도 맞다.
-- USDT/KRW 시간 환율 (docs/34 #4): 외부 FX 소스 대신 Upbit KRW-USDT 마켓(하루 58k 체결)의 시간 종가·VWAP. 우리 데이터라 조건이 같다.
SELECT toStartOfHour(fromUnixTimestamp64Milli(upbit_timestamp)) AS hour_utc,
       argMax(trade_price, upbit_timestamp) AS usdt_krw_close,
       ifNull(toFloat64(sum(trade_amount)) / nullIf(toFloat64(sum(trade_volume)), 0), 0) AS usdt_krw_vwap, count() AS trades
-- 2026-09-20 (docs/40 ①): 원본 직접 읽기 → stg_trades 경유. 술어가 모델마다 달랐던 것을 한 곳으로 모은다.
-- 날짜 기준은 바꾸지 않았다 - 한 번에 한 가지만 바꾼다(변화가 섞이면 무엇 때문인지 못 가린다).
FROM {{ ref('stg_trades') }}
WHERE market = 'KRW-USDT' AND upbit_timestamp >= toUnixTimestamp(now() - INTERVAL 14 DAY) * 1000 AND trade_volume > 0
GROUP BY hour_utc
