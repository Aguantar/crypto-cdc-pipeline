{{ config(order_by='(market, hour_utc)') }}
-- 2026-09-20 (docs/34 #2): crypto_trades 는 ReplacingMergeTree - 중복은 '결국' 지워지므로 읽는 쪽이 FINAL 로 보장한다(재시작 뒤 머지 전 배치가 중복을 세지 않게)
-- materialized 는 dbt_project.yml 의 intermediate 계층 설정(table)을 따른다. 처음엔 별도 reconcile/ 폴더에 두었으나
-- int_ 접두어와 계층 규칙(staging→intermediate→marts)이 어긋나 intermediate 로 옮김 (2026-09-16).

-- 원장 대조: 마켓×시간(UTC) 단위로 거래소 시간봉 거래량(진실값) 대비 ClickHouse 체결 거래량 비율.
-- 기준 시각은 체결시각(upbit_timestamp). 적재시각(source_ts) 기준으로 비교하면 지연이 유실로 보인다 (docs/08).
-- 범위: 참조 시간봉이 있는 구간(최근 90일, upbit_hourly_candles TTL). 매일 전체 재계산 - 수십만 행이라 증분보다 단순함이 이득.
-- FINAL: 참조 테이블은 ReplacingMergeTree 라 재적재분 중복을 읽기 시점에 제거.

WITH candles AS (
    SELECT market, hour_utc, volume AS candle_vol, amount AS candle_amt
    FROM {{ source('reference', 'upbit_hourly_candles') }} FINAL
),
bounds AS (
    SELECT min(hour_utc) AS h_min, max(hour_utc) + INTERVAL 1 HOUR AS h_max FROM candles
),
trades AS (
    SELECT
        market,
        toStartOfHour(fromUnixTimestamp64Milli(upbit_timestamp)) AS hour_utc,
        sum(trade_volume) AS ch_vol,
        count() AS ch_n
    -- 2026-09-20 (docs/40 ①): 원본 직접 읽기 → stg_trades 경유(술어를 한 곳에서만 정의). 날짜·필터 기준은 그대로.
    FROM {{ ref('stg_trades') }}
    WHERE 1 = 1
      AND source_ts >= (SELECT h_min FROM bounds) - INTERVAL 1 DAY          -- 정렬키 프루닝용 (적재 지연 여유 1일)
      AND upbit_timestamp >= toUnixTimestamp64Milli(toDateTime64((SELECT h_min FROM bounds), 3))
      AND upbit_timestamp <  toUnixTimestamp64Milli(toDateTime64((SELECT h_max FROM bounds), 3))
    GROUP BY market, hour_utc
)
SELECT
    c.market,
    c.hour_utc,
    ifNull(t.ch_vol, 0)                                            AS ch_vol,
    ifNull(t.ch_n, 0)                                              AS ch_n,
    c.candle_vol,
    c.candle_amt,
    if(c.candle_vol > 0, 100 * ifNull(t.ch_vol, 0) / c.candle_vol, NULL) AS ratio_pct,
    now()                                                          AS reconciled_at
FROM candles c
LEFT JOIN trades t ON c.market = t.market AND c.hour_utc = t.hour_utc
