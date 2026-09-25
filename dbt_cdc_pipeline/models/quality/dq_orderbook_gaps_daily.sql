{{ config(materialized='incremental', incremental_strategy='delete+insert', on_schema_change='fail', unique_key='day_utc', order_by='day_utc') }}
-- 호가 유실 창 기록 (2026-09-17, docs/23 §6 → 브로커 축소 선행 조건). 호가는 거래소 이력 API 도, 우리 원장도 없어서 못 받은 순간 영구 유실이다.
-- 체결의 ingest_repairs 처럼 "언제 얼마나 비었나"를 표로 남겨 하류가 그 구간을 알고 쓰게 한다.
-- 정의: 수집기 수신 시각(recv_ts) 기준 초당 스냅샷 수. 평시엔 전 마켓 합계가 초당 100 건 이상(09-16: p01 109, min 43, 0 인 초 0) 이라
--   "한 초에 0 건" 은 정상 변동이 아니라 수집·전송 중단이다. 3초 이상 이어지는 0 구간을 gap 창으로 잡고(재연결 손실 실측 5초, docs/17),
--   추정 유실 = 그날 초당 중앙값 × 빈 초. 마켓별로 안 세는 이유: 뜸한 마켓은 분당 2건이라 마켓 단위 0 은 정상이고, 수집기·Kafka 장애는 전 마켓에 동시에 온다.
WITH days AS (
    SELECT DISTINCT toDate(recv_ts) AS day_utc
    FROM {{ source('raw', 'orderbook_raw') }}
    {# 2026-09-20 (docs/40 ⑩): 백필 가능하게 run_anchor() 로. 기본값 now() #}
    {% if is_incremental() %} WHERE recv_ts >= toStartOfDay({{ run_anchor() }} - INTERVAL 1 DAY)
      {% if var('run_date', none) %} AND recv_ts < toStartOfDay({{ run_anchor() }} + INTERVAL 1 DAY) {% endif %}
    {% else %} WHERE recv_ts >= toStartOfDay({{ run_anchor() }} - INTERVAL 30 DAY) {% endif %}
),
per_sec AS (
    SELECT toStartOfSecond(recv_ts) AS s, count() AS c
    FROM {{ source('raw', 'orderbook_raw') }}
    WHERE toDate(recv_ts) IN (SELECT day_utc FROM days)
    GROUP BY s
),
first_ts AS (
    SELECT toStartOfSecond(min(recv_ts)) AS s0 FROM {{ source('raw', 'orderbook_raw') }}   -- 수집 시작(09-10 04:44) 전은 유실이 아니다
),
grid AS (
    SELECT d.day_utc, toDateTime(d.day_utc) + toIntervalSecond(n.number) AS s
    FROM days AS d CROSS JOIN numbers(86400) AS n
    WHERE toDateTime(d.day_utc) + toIntervalSecond(n.number) < now() - INTERVAL 120 SECOND   -- 아직 도착 중인 마지막 2분은 세지 않는다
      AND toDateTime(d.day_utc) + toIntervalSecond(n.number) >= (SELECT s0 FROM first_ts)
),
joined AS (
    SELECT g.day_utc, g.s, coalesce(p.c, 0) AS c
    FROM grid AS g LEFT JOIN per_sec AS p ON p.s = g.s
),
zero_runs AS (
    SELECT day_utc, min(s) AS gap_start, count() AS gap_seconds
    FROM (
        SELECT day_utc, s, s - toIntervalSecond(row_number() OVER (PARTITION BY day_utc ORDER BY s)) AS grp
        FROM joined WHERE c = 0
    )
    GROUP BY day_utc, grp
),
day_median AS (
    SELECT day_utc, median(c) AS median_per_sec FROM joined GROUP BY day_utc
),
day_stats AS (
    SELECT j.day_utc, any(dm.median_per_sec) AS median_per_sec, countIf(j.c = 0) AS zero_seconds,
           countIf(j.c > 0 AND j.c < 0.2 * dm.median_per_sec) AS low_seconds, sum(j.c) AS snapshots
    FROM joined AS j INNER JOIN day_median AS dm ON dm.day_utc = j.day_utc
    GROUP BY j.day_utc
),
gaps AS (
    SELECT day_utc, count() AS gap_windows, sum(gap_seconds) AS gap_seconds_total, max(gap_seconds) AS gap_longest_s,
           groupArray(10)(concat(toString(gap_start), ' +', toString(gap_seconds), 's')) AS gap_sample
    FROM zero_runs WHERE gap_seconds >= 3 GROUP BY day_utc
)
SELECT ds.day_utc AS day_utc,
       ds.snapshots AS snapshots,
       round(ds.median_per_sec) AS median_per_sec,
       ds.zero_seconds AS zero_seconds,
       ds.low_seconds AS low_seconds,
       coalesce(g.gap_windows, 0) AS gap_windows,
       coalesce(g.gap_seconds_total, 0) AS gap_seconds_total,
       coalesce(g.gap_longest_s, 0) AS gap_longest_s,
       round(ds.median_per_sec * ds.zero_seconds) AS est_lost_snapshots,
       coalesce(g.gap_sample, []) AS gap_sample
FROM day_stats AS ds LEFT JOIN gaps AS g ON g.day_utc = ds.day_utc
