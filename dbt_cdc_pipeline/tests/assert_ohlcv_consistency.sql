-- OHLCV 정합성 검증: 시가/종가는 고가/저가 범위 안에 있어야 함
-- 위반 행이 0이어야 테스트 통과
SELECT *
FROM {{ ref('int_ohlcv_1h') }}
WHERE low > high
   OR open > high
   OR open < low
   OR close > high
   OR close < low
