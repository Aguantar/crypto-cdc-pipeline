-- 거래량/거래대금 양수 검증
-- staging에서 필터링했지만, 집계 후 재검증
-- 2026-09-20 발견(docs/34 #3) → #5 에서 해결: MySQL trade_amount 가 DECIMAL(20,4) 라 먼지 체결이 0 으로 저장됐었다(30일 64,669행).
-- 이제 Flink 가 price×volume(BigDecimal, 스케일 16)을 계산해 넣으므로 수량이 양수면 금액도 반드시 양수다. 느슨하게 뒀던 예외를 되돌린다.
SELECT *
FROM {{ ref('int_ohlcv_1h') }}
WHERE volume <= 0
   OR amount <= 0
   OR trade_count <= 0
