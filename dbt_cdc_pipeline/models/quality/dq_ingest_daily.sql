{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='day_utc', order_by='day_utc') }}
-- 2026-09-20 (docs/34 #2): crypto_trades 는 ReplacingMergeTree - 중복은 '결국' 지워지므로 읽는 쪽이 FINAL 로 보장한다(재시작 뒤 머지 전 배치가 중복을 세지 않게)
-- 일별 적재 품질: 지연 분위수, 늦은 행(가드 대상), 마켓 수. 증분(전날~오늘)만 재계산 - 1억 행 전체를 매일 훑지 않기 위해.
-- 지연 = source_ts(MySQL 적재) − upbit_timestamp(체결). 관찰 주간 기준값: p50 1.2s, p95 2.2s, max 7.75s (docs/14).
SELECT
    toDate(source_ts)                                                              AS day_utc,
    count()                                                                        AS rows,
    uniqExact(market)                                                              AS markets,
    -- 실시간 지연은 수리 행(지연 > 60초, 백필·gap-fill)을 제외하고 잰다. 09-16 은 BFC 백필 261,713행 때문에 포함 시 p95 가 51만 초로 왜곡됐다.
    round(quantileIf(0.5)(toUnixTimestamp64Milli(source_ts) - upbit_timestamp, toUnixTimestamp64Milli(source_ts) - upbit_timestamp <= 60000) / 1000, 2)  AS lag_p50_s,
    round(quantileIf(0.95)(toUnixTimestamp64Milli(source_ts) - upbit_timestamp, toUnixTimestamp64Milli(source_ts) - upbit_timestamp <= 60000) / 1000, 2) AS lag_p95_s,
    round(maxIf(toUnixTimestamp64Milli(source_ts) - upbit_timestamp, toUnixTimestamp64Milli(source_ts) - upbit_timestamp <= 60000) / 1000, 1)      AS lag_max_live_s,
    countIf(toUnixTimestamp64Milli(source_ts) - upbit_timestamp > 60000)           AS late_rows_gt_60s,
    countIf(best_bid_price IS NULL)                                                AS rows_without_bbo
-- 2026-09-20 (docs/40 ①): 원본 직접 읽기 → stg_trades 경유. 술어가 모델마다 달랐던 것을 한 곳으로 모은다.
-- 날짜 기준은 바꾸지 않았다 - 한 번에 한 가지만 바꾼다(변화가 섞이면 무엇 때문인지 못 가린다).
FROM {{ ref('stg_trades') }}
WHERE 1 = 1
{% if is_incremental() %}
  {# 2026-09-20 (docs/40 ⑩): now() → run_anchor(). 기본값이 now() 라 평소 동작은 그대로이고,
     --vars '{"run_date": "YYYY-MM-DD"}' 를 주면 그 날(과 전날)만 다시 만든다. #}
  AND source_ts >= toStartOfDay({{ run_anchor() }} - INTERVAL 1 DAY)
  {% if var('run_date', none) %}AND source_ts < toStartOfDay({{ run_anchor() }} + INTERVAL 1 DAY){% endif %}
{% else %}
  AND source_ts >= toStartOfDay({{ run_anchor() }} - INTERVAL 30 DAY)
{% endif %}
GROUP BY day_utc
