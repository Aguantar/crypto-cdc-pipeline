-- 입도 계약: 이 표의 한 행 = (하루, 수리 이유) 하나다. GROUP BY 가 그렇게 돼 있으니 지금은 참이지만,
-- 나중에 누가 reason 을 빼거나 열을 더하면 조용히 깨지고 하류 합계가 중복 집계된다.
-- day_utc 단독 unique 를 못 쓰는 이유: 하루에 reason 이 여러 개면 정상이다.
SELECT day_utc, reason, count() AS n
FROM {{ ref('dq_repairs_daily') }}
GROUP BY day_utc, reason
HAVING n > 1
