{{ config(materialized='incremental', incremental_strategy='delete+insert', on_schema_change='fail', unique_key='day_utc', order_by='day_utc') }}
-- 2026-10-01 (docs/44 §12): 매일 전체(3.9억 행)를 다시 대조하던 것을 최근 이틀(UTC 하루 단위)로. 1.09 GiB -> 수십 MiB.
--   대조 결과는 날짜별로 확정되므로 옛 날짜를 매일 다시 계산할 이유가 없다. run_date 를 주면 그 날(과 전날)만.
{%- set lookback = "toStartOfDay(" ~ run_anchor() ~ ") - INTERVAL 1 DAY" -%}
{%- set upper    = "toStartOfDay(" ~ run_anchor() ~ ") + INTERVAL 1 DAY" -%}
-- Binance 체결 대조 (docs/31 §3-2): 거래소 1h 캔들의 체결 수(n) vs 우리 binance_trades FINAL count, (symbol, hour) 셀.
-- Upbit 대조(dq_reconcile_daily)와 같은 원리: 가중 비율 + 셀 최소값 + 0행 셀. 우리 > 거래소 는 중복(RMT 미머지)이나 시각 경계 문제, 우리 < 거래소 는 유실.
WITH ours AS (
    -- 2026-09-20 (docs/40 ⑥): 원본 직접 → stg_binance_trades 경유. FINAL·중복 제거 규칙을 한 곳에서만 정의한다.
    SELECT symbol, trade_hour_utc AS hour_utc, count() AS ours_n
    FROM {{ ref('stg_binance_trades') }}
    {% if is_incremental() %}WHERE trade_ms >= toUnixTimestamp({{ lookback }}) * 1000 {% if var('run_date', none) %}AND trade_ms < toUnixTimestamp({{ upper }}) * 1000{% endif %}{% endif %}
    GROUP BY symbol, hour_utc
),
ex AS (
    SELECT symbol, hour_utc, trade_count AS ex_n FROM {{ source('reference', 'binance_hourly_candles') }} FINAL
    {% if is_incremental() %}WHERE hour_utc >= {{ lookback }} {% if var('run_date', none) %}AND hour_utc < {{ upper }}{% endif %}{% endif %}
),
cells AS (
    SELECT e.symbol AS symbol, e.hour_utc AS hour_utc, e.ex_n AS ex_n, coalesce(o.ours_n, 0) AS ours_n,
           if(e.ex_n > 0, 100.0 * coalesce(o.ours_n, 0) / e.ex_n, NULL) AS ratio_pct
    FROM ex AS e LEFT JOIN ours AS o ON o.symbol = e.symbol AND o.hour_utc = e.hour_utc
)
SELECT toDate(hour_utc) AS day_utc,
       round(100 * sum(ours_n) / sum(ex_n), 3) AS weighted_pct,
       count() AS cells, countIf(ratio_pct < 99) AS cells_below_99, countIf(ratio_pct > 101) AS cells_above_101, countIf(ours_n = 0 AND ex_n > 0) AS cells_no_rows,
       uniqExactIf(symbol, ours_n = 0 AND ex_n > 0) AS symbols_no_rows,
       round(min(ratio_pct), 2) AS min_cell_pct, argMin(concat(symbol, ' ', toString(hour_utc)), ratio_pct) AS worst_cell,
       now() AS computed_at
FROM cells WHERE ex_n > 0
GROUP BY day_utc
