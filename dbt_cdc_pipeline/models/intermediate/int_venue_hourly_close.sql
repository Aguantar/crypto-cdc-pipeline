{{ config(materialized='incremental', incremental_strategy='delete+insert', on_schema_change='fail', unique_key='hour_utc', order_by='(coin_id, venue, hour_utc)') }}
-- 2026-10-01 (docs/44 §12): table -> incremental. 14일 창이 Binance 493심볼 확장 뒤 매일 커져 1.46 GiB / 91초가 됐고,
--   서버 총 한도(1.57 GiB) 안에서 사흘 중 이틀 죽었다. 최근 이틀(UTC)만 다시 만들어 그 시간(hour_utc)의 행을 교체한다.
--   표는 이제 14일을 넘어 쌓인다(소비자 sig_kimchi_premium_hourly 는 전체를 읽으므로 더 긴 이력이 손해가 아니다).
{%- set lookback = "toStartOfDay(" ~ run_anchor() ~ ") - INTERVAL 1 DAY" -%}
{%- set upper    = "toStartOfDay(" ~ run_anchor() ~ ") + INTERVAL 1 DAY" -%}
-- 거래소별 시간 종가 (docs/34 #4): 교차 거래소 가격 비교의 재료. 같은 코인·같은 UTC 시간의 마지막 체결가.
-- 최근 14일만(비교는 최근이 목적, 전체 재계산은 1.2억 행).
WITH u AS (
    SELECT c.coin_id AS coin_id, 'upbit' AS venue, toStartOfHour(fromUnixTimestamp64Milli(t.upbit_timestamp)) AS hour_utc,
           argMax(t.trade_price, t.upbit_timestamp) AS close, count() AS trades, sum(t.trade_amount) AS amount_quote
    -- 2026-09-20 (docs/40 ①): 원본 직접 읽기 → stg_trades 경유. 술어가 모델마다 달랐던 것을 한 곳으로 모은다.
    -- 날짜 기준은 바꾸지 않았다 - 한 번에 한 가지만 바꾼다(변화가 섞이면 무엇 때문인지 못 가린다).
    FROM {{ ref('stg_trades') }} AS t
    INNER JOIN {{ ref('dim_coins') }} AS c ON c.upbit_market = t.market
    {% if is_incremental() %}WHERE t.upbit_timestamp >= toUnixTimestamp({{ lookback }}) * 1000 {% if var('run_date', none) %}AND t.upbit_timestamp < toUnixTimestamp({{ upper }}) * 1000{% endif %}
    {% else %}WHERE t.upbit_timestamp >= toUnixTimestamp(now() - INTERVAL 14 DAY) * 1000{% endif %}
    GROUP BY coin_id, hour_utc
),
b AS (
    -- 2026-09-20 (docs/40 ⑥): 원본 직접 → stg_binance_trades 경유
    SELECT c.coin_id AS coin_id, 'binance' AS venue, t.trade_hour_utc AS hour_utc,
           argMax(t.price, t.trade_ms) AS close, count() AS trades, sum(t.quote_qty) AS amount_quote
    FROM {{ ref('stg_binance_trades') }} AS t
    INNER JOIN {{ ref('dim_coins') }} AS c ON c.binance_symbol = t.symbol
    {% if is_incremental() %}WHERE t.trade_ms >= toUnixTimestamp({{ lookback }}) * 1000 {% if var('run_date', none) %}AND t.trade_ms < toUnixTimestamp({{ upper }}) * 1000{% endif %}
    {% else %}WHERE t.trade_ms >= toUnixTimestamp(now() - INTERVAL 14 DAY) * 1000{% endif %}
    GROUP BY coin_id, hour_utc
)
SELECT * FROM u UNION ALL SELECT * FROM b
