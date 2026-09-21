-- SCD2 의 핵심 계약: 마켓마다 '지금' 행은 정확히 하나여야 한다.
-- 둘이면 구간이 겹친 것이고, 커버리지 판정이 어느 상태를 볼지 결정할 수 없다(조인이 행을 불린다).
-- 이 테스트가 없으면 구간 계산(leadInFrame)이 깨져도 조용히 지나간다 - 값이 그럴듯해 보이기 때문이다.
SELECT market, countIf(is_current = 1) AS current_rows
FROM {{ ref('dim_market_state_scd') }}
GROUP BY market
HAVING current_rows != 1
