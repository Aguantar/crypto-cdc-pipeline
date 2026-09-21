{{ config(order_by='(day_utc)', query_settings={'max_memory_usage': 1200000000, 'max_bytes_before_external_group_by': 600000000, 'max_bytes_before_external_sort': 600000000}) }}
-- PRICE_24H 구현 동등성 (docs/22 §4 정정): Flink 탐지기(링·forward-fill·늦은 이벤트 가드)가 낸 등급 전이를
-- 같은 입력(crypto_trades)에서 SQL 로 다시 계산한 전이와 매일 대조한다. 임계 자체의 근거는 docs/16(6개월 역검증)이고,
-- 섀도가 확인할 것은 "구현이 그 정의와 같은 답을 내는가"다. 이 표에서 전이 10건 이상이 전부 일치하면 승격한다.
-- 재계산 정의(Flink 와 동일): 분 종가 = 그 분의 마지막 체결가(upbit_timestamp, sequential_id 순), 빈 분은 직전 종가로 채움,
--   참조 = 체결 분 − 1,440분의 종가, 변화율 = 체결가/참조 − 1, 등급 = |변화율| ≥ 2.0→3, ≥1.0→2, ≥0.5→1, 그 외 0,
--   늦은 이벤트(source_ts − upbit_timestamp > 60s)는 종가·판정 모두에서 제외, op='c' 만.
-- 알려진 차이: Flink 는 도착 순서로 상태를 갱신하므로 재정렬(최대 4.8s, docs/20)된 체결은 분 경계에서 참조가 한 칸 다를 수 있다 → 매칭 허용 ±60초.
-- 창: 최근 {{ var('parity_days', 2) }}일(UTC). 창 시작 시점의 등급은 Flink 의 마지막 전이(market_alerts)로 시드한다 - 대조 대상은 창 안의 전이.
{% set v2_start = var('flink_v2_start', '2026-09-17 01:46:00') %}
{% set days = var('parity_days', 2) %}
{# 창 경계는 컴파일 시점에 상수화한다 - ClickHouse 스칼라 서브쿼리는 Nullable 이라 numbers()·배열 인덱스에 쓸 수 없다 #}
{% set q = "SELECT toString(greatest(toDateTime('" ~ v2_start ~ "') + INTERVAL 1 DAY, toStartOfDay(now()) - INTERVAL " ~ days ~ " DAY)) AS f, toString(toStartOfDay(now())) AS t, toString(toUnixTimestamp(greatest(toDateTime('" ~ v2_start ~ "') + INTERVAL 1 DAY, toStartOfDay(now()) - INTERVAL " ~ days ~ " DAY))) AS fu, toString(toUnixTimestamp(toStartOfDay(now()))) AS tu" %}
{% if execute %}
  {% set r = run_query(q) %}
  {% set win_from = r.columns[0][0] %}{% set win_to = r.columns[1][0] %}
  {% set from_unix = r.columns[2][0] | int %}{% set to_unix = r.columns[3][0] | int %}
{% else %}
  {% set win_from = '2026-01-01 00:00:00' %}{% set win_to = '2026-01-02 00:00:00' %}{% set from_unix = 1767225600 %}{% set to_unix = 1767312000 %}
{% endif %}
{% set m0 = ((from_unix - 86400) // 60) %}
{% set m1 = (to_unix // 60) %}
{# 워밍업(배포 후 24h)이 끝나기 전엔 창이 비어 있다(win_from > win_to). 그 경우 격자 0 → 빈 표 #}
{% set span = (m1 - m0) if m1 > m0 else 0 %}

WITH
sql_transitions AS (
    SELECT market, upbit_timestamp, prev, lvl FROM {{ ref('int_alert_transitions_recomputed') }}
),
flink_transitions AS (
    SELECT market, toUnixTimestamp64Milli(event_time) AS upbit_timestamp, toInt16(prev_level) AS prev, toInt16(level) AS lvl
    FROM {{ source('reference', 'market_alerts') }}
    WHERE alert_type = 'PRICE_24H'
      AND event_time >= toDateTime('{{ win_from }}') AND event_time < toDateTime('{{ win_to }}')
),
-- 4) 매칭: 같은 마켓·같은 (prev, lvl)·±60초
matched AS (
    SELECT DISTINCT s.market, s.upbit_timestamp AS s_ts, f.upbit_timestamp AS f_ts, s.prev, s.lvl
    FROM sql_transitions s
    INNER JOIN flink_transitions f ON f.market = s.market AND f.prev = s.prev AND f.lvl = s.lvl
    WHERE abs(f.upbit_timestamp - s.upbit_timestamp) <= 60000
),
per_day AS (
    SELECT day_utc,
           countIf(src = 's') AS sql_transitions,
           countIf(src = 'f') AS flink_transitions
    FROM (
        SELECT toDate(fromUnixTimestamp64Milli(upbit_timestamp)) AS day_utc, 's' AS src FROM sql_transitions
        UNION ALL
        SELECT toDate(fromUnixTimestamp64Milli(upbit_timestamp)) AS day_utc, 'f' AS src FROM flink_transitions
    )
    GROUP BY day_utc
),
matched_day AS (
    SELECT day_utc, uniqExact(market, f_ts, prev, lvl) AS matched
    FROM (SELECT toDate(fromUnixTimestamp64Milli(f_ts)) AS day_utc, market, f_ts, prev, lvl FROM matched)
    GROUP BY day_utc
),
unmatched_f AS (
    SELECT day_utc, groupArray(5)(txt) AS sample
    FROM (
        SELECT toDate(fromUnixTimestamp64Milli(f.upbit_timestamp)) AS day_utc,
               concat(f.market, ' ', toString(f.prev), '→', toString(f.lvl), ' @', toString(fromUnixTimestamp64Milli(f.upbit_timestamp))) AS txt
        FROM flink_transitions AS f
        LEFT ANTI JOIN matched AS mt ON mt.market = f.market AND mt.f_ts = f.upbit_timestamp AND mt.prev = f.prev AND mt.lvl = f.lvl
    )
    GROUP BY day_utc
),
unmatched_s AS (
    SELECT day_utc, groupArray(5)(txt) AS sample
    FROM (
        SELECT toDate(fromUnixTimestamp64Milli(s.upbit_timestamp)) AS day_utc,
               concat(s.market, ' ', toString(s.prev), '→', toString(s.lvl), ' @', toString(fromUnixTimestamp64Milli(s.upbit_timestamp))) AS txt
        FROM sql_transitions AS s
        LEFT ANTI JOIN matched AS mt ON mt.market = s.market AND mt.s_ts = s.upbit_timestamp AND mt.prev = s.prev AND mt.lvl = s.lvl
    )
    GROUP BY day_utc
)
SELECT p.day_utc AS day_utc,                       -- 별칭 필수: ClickHouse 는 p.day_utc 를 열 이름으로 남겨 ORDER BY (day_utc) 가 실패한다
       p.sql_transitions AS sql_transitions,
       p.flink_transitions AS flink_transitions,
       coalesce(md.matched, 0) AS matched,
       (p.sql_transitions = coalesce(md.matched, 0) AND p.flink_transitions = coalesce(md.matched, 0)) AS parity_ok,
       coalesce(uf.sample, []) AS unmatched_flink_sample,
       coalesce(us.sample, []) AS unmatched_sql_sample,
       toDateTime('{{ win_from }}') AS window_from,
       now() AS computed_at
FROM per_day p
LEFT JOIN matched_day md ON md.day_utc = p.day_utc
LEFT JOIN unmatched_f uf ON uf.day_utc = p.day_utc
LEFT JOIN unmatched_s us ON us.day_utc = p.day_utc
ORDER BY p.day_utc
