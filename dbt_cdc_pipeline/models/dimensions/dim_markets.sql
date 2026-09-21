{{ config(materialized='table', order_by='market') }}
-- 2026-09-20 (docs/34 #2): crypto_trades 는 ReplacingMergeTree - 중복은 '결국' 지워지므로 읽는 쪽이 FINAL 로 보장한다(재시작 뒤 머지 전 배치가 중복을 세지 않게)
-- 마켓 마스터 (docs/26 §4). 거래소 목록·이름·경보 플래그 + 상장일 근사 + 우리가 처음/마지막으로 본 체결 시각.
-- 쓰임: 규칙 평가의 신규 상장 96h 제외(거래소 예외 규정), 대조 분모(추후 통일), 커버리지 사유("상장 N일째, 우리 체결 없음").
-- seen_gap_days = 우리 첫 체결일 − 상장일: BFC 사고(상장 후 6일 무수집)가 이 숫자로 남는다.
WITH master AS (
    SELECT * FROM {{ source('reference', 'upbit_market_master') }} FINAL
),
ours AS (
    SELECT market, min(source_ts) AS first_seen_ours, max(source_ts) AS last_seen_ours
    -- 2026-09-20 (docs/40 ①): stg_trades 경유. 최초 관측 시각이 스냅샷(op='r') 행까지 포함해 더 정확해진다.
    FROM {{ ref('stg_trades') }} GROUP BY market
),
state AS (
    -- 2026-09-20 (docs/34 #9): 현재 거래 상태. is_active(거래소 목록에 있나) 만으로는
    -- "폐지 예정이라 곧 사라진다"를 알 수 없어 커버리지 감소를 유실과 구분하지 못했다.
    SELECT market, market_state, delisting_date, is_tradable
    FROM {{ ref('dim_market_state_scd') }} WHERE is_current = 1
),
flags AS (
    SELECT market, groupArrayIf(flag, state = 1) AS active_flags
    FROM (SELECT market, flag, argMax(state, observed_at) AS state FROM {{ source('reference', 'upbit_market_events') }} GROUP BY market, flag)
    GROUP BY market
)
SELECT m.market AS market, m.korean_name AS korean_name, m.english_name AS english_name, m.market_warning AS market_warning,
       m.listing_date_est AS listing_date_est, m.listing_date_source AS listing_date_source, m.lookback_days AS lookback_days,
       nullIf(o.first_seen_ours, toDateTime64(0, 3)) AS first_seen_ours,
       nullIf(o.last_seen_ours, toDateTime64(0, 3)) AS last_seen_ours,
       if(m.listing_date_est IS NULL OR o.market = '', NULL, dateDiff('day', m.listing_date_est, toDate(o.first_seen_ours))) AS seen_gap_days,
       -- 커버리지 공백: 우리가 전 마켓 수집을 시작한 2026-09-09 이후 상장(일봉 창 안)에만 의미가 있다. 그 전 상장은 우리 수집 시작일이 첫 체결이라 190 처럼 나온다(BTC 등 5코인은 02-13 부터)
       if(m.listing_date_source = 'daily_candle_first' AND m.listing_date_est >= toDate('2026-09-09') AND o.market != '',
          dateDiff('day', m.listing_date_est, toDate(o.first_seen_ours)), NULL) AS coverage_gap_days,
       coalesce(f.active_flags, []) AS active_flags,
       -- 상태를 모르는 마켓(폴링 시작 전에 이미 사라진 것)은 UNKNOWN. '' 로 두면 하류가 ACTIVE 와 헷갈린다
       if(s.market = '', 'UNKNOWN', s.market_state) AS market_state,
       s.delisting_date AS delisting_date,
       toUInt8(s.market != '' AND s.is_tradable = 1) AS is_tradable,
       m.is_active AS is_active, m.fetched_at AS fetched_at
FROM master AS m
LEFT JOIN ours AS o ON o.market = m.market
LEFT JOIN flags AS f ON f.market = m.market
LEFT JOIN state AS s ON s.market = m.market
