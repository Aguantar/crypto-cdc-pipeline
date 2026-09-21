{{ config(materialized='table', order_by='(market, valid_from)') }}
-- 마켓 거래 상태의 이력(SCD type 2): "이 마켓이 언제부터 언제까지 어떤 상태였나" (docs/34 #9).
--
-- 왜 필요한가: 마켓이 상장폐지되면 거래소 목록에서 사라진다. 그 순간 대조 분모와 커버리지 마켓 집합이
-- 줄어드는데, 기록이 없으면 "어제 289개 오늘 287개"가 폐지인지 유실인지 구분할 수 없다.
-- 폐지는 정상이고 유실은 사고다. 이 표가 그 둘을 가르는 근거다.
--
-- 원천: upbit_market_state_events (10분 폴링, 전이만 적재). 첫 snapshot 의 valid_from 은
-- 관측 시작(2026-09-20)이라 '적어도 그때부터'이지 '그때부터'가 아니다 - dim_market_flag_scd 와 같은 한계.
WITH changes AS (
    SELECT market, observed_at, market_state, is_trading_suspended, delisting_date, kind,
           -- 직전 행과 상태가 같으면 구간이 안 바뀐 것이다. 폴러가 전이만 넣지만, --snapshot 재실행이
           -- 같은 상태를 다시 넣을 수 있어 읽는 쪽에서도 접는다(멱등).
           lagInFrame(market_state, 1, '') OVER (PARTITION BY market ORDER BY observed_at
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_state,
           lagInFrame(is_trading_suspended, 1, toUInt8(255)) OVER (PARTITION BY market ORDER BY observed_at
                                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_susp
    FROM {{ source('reference', 'upbit_market_state_events') }}
),
intervals AS (
    SELECT market, observed_at AS valid_from, market_state, is_trading_suspended, delisting_date, kind,
           leadInFrame(observed_at) OVER (PARTITION BY market ORDER BY observed_at
                                          ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_at
    FROM changes
    WHERE market_state != prev_state OR is_trading_suspended != prev_susp
)
SELECT market,
       market_state,
       is_trading_suspended,
       delisting_date,
       kind,
       valid_from,
       if(next_at > valid_from, next_at, NULL)                      AS valid_to,
       toUInt8(next_at <= valid_from OR next_at IS NULL)            AS is_current,
       -- 거래 가능 여부: 대조 분모·커버리지 판정이 쓰는 단 하나의 열.
       -- PREDELISTING 은 아직 거래되므로 tradable 이다. 폐지일이 지나야 빠진다.
       toUInt8(market_state IN ('ACTIVE', 'PREDELISTING') AND is_trading_suspended = 0) AS is_tradable
FROM intervals
