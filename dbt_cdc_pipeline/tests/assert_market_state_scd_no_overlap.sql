-- 구간이 겹치지 않아야 한다: 한 마켓의 valid_to 는 다음 구간의 valid_from 과 같거나 그보다 앞서야 한다.
-- 겹치면 "그때 그 마켓은 어떤 상태였나"에 답이 둘이 된다.
SELECT market, valid_from, valid_to, next_from
FROM (
    SELECT market, valid_from, valid_to,
           leadInFrame(valid_from) OVER (PARTITION BY market ORDER BY valid_from
                                         ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_from
    FROM {{ ref('dim_market_state_scd') }}
)
WHERE valid_to IS NOT NULL AND next_from > valid_from AND valid_to > next_from
