{{ config(order_by='day_utc') }}
-- 수리 계보 일별 요약: 공백을 얼마나 자주, 얼마나 메웠나. reason 별로 나눈다(reconnect / startup / manual).
SELECT
    toDate(window_start)            AS day_utc,
    reason,
    count()                         AS repairs,
    sum(rest_rows)                  AS exchange_rows_in_windows,
    sum(inserted_rows)              AS rows_recovered,
    round(sum(dateDiff('second', window_start, window_end)), 1) AS gap_seconds,
    round(avg(elapsed_s), 1)        AS avg_elapsed_s
FROM {{ source('reference', 'ingest_repairs') }}
GROUP BY day_utc, reason
