{{ config(materialized='table', order_by='(market, flag, valid_from)') }}
-- Upbit 시장경보 플래그의 이력(SCD type 2): "이 마켓의 이 플래그가 언제부터 언제까지 켜져 있었나" (docs/28 C-1).
-- 두 원천을 합친다:
--   ① upbit_market_event_records - 거래소가 공개하는 지정/해제 이력(2026-03-23~). 한 행 = 한 구간 [trigger_time, expiration_time]. 주의(caution) 5종만 있다.
--   ② upbit_market_events       - 우리 1분 폴링(2026-09-16~). WARNING(유의) 은 여기에만 있고 transition 행이 아직 없어 snapshot(시작·일 1회)까지 넣어 상태 변화만 추린다. 첫 구간의 valid_from 은 관측 시작(2026-09-16 09:45)이라 '적어도 그때부터'.
-- 왜 SCD 인가: 지금 상태만 있는 dim_markets 로는 "그때 그 코인이 경보 중이었나"에 답할 수 없다. 규칙 평가·교차 거래소 신호가 그 질문을 한다.
WITH records AS (
    SELECT market, event_type AS flag, warning_level AS level, trigger_time_utc AS valid_from, expiration_time_utc AS valid_to, 'exchange_records' AS source
    FROM {{ source('reference', 'upbit_market_event_records') }} FINAL
),
changes AS (
    -- 폴링 행(snapshot 은 시작·일 1회 전체 상태, transition 은 변화)에서 "직전과 다른 상태"만 남긴다. 첫 snapshot 의 1 은 "적어도 그때부터"(관측 시작).
    SELECT market, flag, observed_at, state,
           lagInFrame(state, 1, 255) OVER (PARTITION BY market, flag ORDER BY observed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_state
    FROM {{ source('reference', 'upbit_market_events') }}
),
polled AS (
    SELECT market, flag, observed_at, state,
           leadInFrame(observed_at) OVER (PARTITION BY market, flag ORDER BY observed_at ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_at
    FROM changes WHERE state != prev_state
),
polled_intervals AS (
    SELECT market, flag, 'polled' AS level, observed_at AS valid_from, if(next_at > observed_at, next_at, NULL) AS valid_to, 'polled' AS source
    FROM polled WHERE state = 1
)
SELECT market, flag, level, valid_from, valid_to, source,
       toUInt8(valid_to IS NULL OR valid_to > now()) AS is_current,
       if(valid_to IS NULL, NULL, dateDiff('minute', valid_from, valid_to)) AS duration_min
FROM (
    SELECT * FROM records
    UNION ALL
    SELECT * FROM polled_intervals WHERE flag = 'WARNING'   -- caution 5종은 거래소 이력이 더 길고 정확하므로 폴링분은 WARNING 만 채택
)
