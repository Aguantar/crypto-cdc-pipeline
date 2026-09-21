{{ config(order_by='window_start') }}
-- 수집 공백에 설명을 붙인다 (docs/33 → docs/22 이후 보강, 2026-09-20).
--
-- 왜: 지금까지 수집 공백은 전부 '유실'로 보였다. 그런데 업비트 서버 점검 중에는 전 마켓 거래가 멈추고,
-- 리브랜딩·토큰 스왑 때는 그 코인의 거래지원이 멈춘다. 그 시간엔 체결이 없는 것이 정상이다.
-- 정상과 사고를 사람 기억이 아니라 데이터로 가른다 - 이 표가 "그때 공지가 있었나"에 답한다.
--
-- 입력: ingest_repairs(우리가 메운 공백 창) × dim_exchange_notices(거래를 멈추는 공지만).
-- 매칭 두 가지:
--   ① 전체 점검: 본문에서 창을 뽑았으므로 구간이 겹치는가로 본다(정확한 판단).
--   ② 코인별 중단: 본문에 재개 시각만 있는 경우가 있어 창을 만들지 않았다 → 게시 시각이 공백 ±24시간 안이면
--      후보로만 올린다. 지어낸 창으로 '설명됨' 도장을 찍는 것보다 후보를 주고 사람이 닫는 편이 낫다.
--
-- 구현 메모: ClickHouse 는 JOIN ON 에 범위 조건(s < e)을 못 쓴다(code 403). 거래를 멈추는 공지는
-- 15개월에 8건뿐이므로 배열 한 줄로 만들어 CROSS JOIN 한 뒤 arrayFilter 로 겹침을 본다 - 조인보다 싸고 명확하다.
WITH gaps AS (
    SELECT window_start, window_end, reason, markets, inserted_rows,
           dateDiff('second', window_start, window_end) AS gap_seconds
    FROM {{ source('reference', 'ingest_repairs') }}
),
wide AS (
    SELECT groupArray(window_start_utc) AS w_start,
           groupArray(window_end_utc)   AS w_end,
           groupArray(title)            AS w_title,
           groupArray(url)              AS w_url
    FROM (SELECT window_start_utc, window_end_utc, title, url
          FROM {{ ref('dim_exchange_notices') }}
          WHERE scope = 'exchange_wide' AND window_start_utc IS NOT NULL)
),
per_market AS (
    SELECT groupArray(listed_at) AS m_at,
           groupArray(title)     AS m_title,
           groupArray(url)       AS m_url
    FROM (SELECT listed_at, title, url FROM {{ ref('dim_exchange_notices') }} WHERE scope = 'market')
)
SELECT
    g.window_start    AS window_start,
    g.window_end      AS window_end,
    g.reason          AS reason,
    g.gap_seconds     AS gap_seconds,
    g.markets         AS markets,
    g.inserted_rows   AS inserted_rows,
    -- 전체 점검: 구간이 겹치면 이 공백은 설명된다
    arrayFilter((t, s, e) -> s < g.window_end AND e > g.window_start, w.w_title, w.w_start, w.w_end) AS maintenance_notices,
    arrayFilter((u, s, e) -> s < g.window_end AND e > g.window_start, w.w_url,   w.w_start, w.w_end) AS maintenance_urls,
    -- 코인별 중단: ±24시간 안의 공지를 후보로만
    arrayFilter((t, a) -> a > g.window_start - INTERVAL 24 HOUR AND a < g.window_end + INTERVAL 24 HOUR, m.m_title, m.m_at) AS market_notice_candidates,
    toUInt8(length(arrayFilter((t, s, e) -> s < g.window_end AND e > g.window_start, w.w_title, w.w_start, w.w_end)) > 0) AS explained_by_maintenance
FROM gaps AS g
CROSS JOIN wide AS w
CROSS JOIN per_market AS m
