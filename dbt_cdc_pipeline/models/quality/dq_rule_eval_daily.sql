{{ config(order_by='(day_utc, rule)') }}
-- 규칙 검증 기반 (docs/16 §5, docs/22): 우리 규칙의 출력을 거래소 지정 이력(정답)과 매일 대조해 정밀도·재현율·선행 시간을 낸다.
-- 이 표가 규칙의 존재 이유다. "임계값이 왜 그 값인가" 에 대한 답이 매일 갱신되는 숫자로 남는다. 섀도 → 승격 판단은 여기서 한다.
-- 매칭 정의: 같은 마켓, 우리 지정(level 0→N 전이)과 거래소 지정(TRIGGER 시각)이 ±10분 안. 선행 시간 = 거래소 − 우리 (양수면 우리가 먼저).
WITH -- 신규 상장 96시간은 거래소가 지정하지 않는다(docs/16 공식 기준). 우리 출력도 그 구간은 평가에서 뺀다 - dim_markets 의 상장일 근사 기준 (2026-09-18)
new_listing AS (
    SELECT market, toDateTime(listing_date_est) + INTERVAL 96 HOUR AS eligible_from
    FROM {{ ref('dim_markets') }} WHERE listing_date_est IS NOT NULL AND listing_date_source = 'daily_candle_first'
),
ours_price AS (
    SELECT a.market, a.event_time AS t, a.event_time - INTERVAL 10 MINUTE AS t_lo
    FROM {{ source('reference', 'market_alerts') }} AS a
    LEFT JOIN new_listing AS n ON n.market = a.market
    WHERE a.alert_type = 'PRICE_24H' AND a.prev_level = 0 AND a.level > 0
      AND (n.market = '' OR a.event_time >= n.eligible_from)
),
ex_price AS (
    SELECT market, trigger_time_utc AS t FROM {{ source('reference', 'upbit_market_event_records') }} FINAL
    WHERE event_type = 'PRICE_FLUCTUATIONS'
),
price_match AS (
    SELECT o.market, o.t AS ours_t, e.t AS ex_t
    FROM ours_price o ASOF LEFT JOIN ex_price e ON o.market = e.market AND e.t >= o.t_lo
),
price_day AS (
    SELECT toDate(ours_t) AS day_utc, 'PRICE_24H' AS rule,
           count() AS ours, countIf(ex_t IS NOT NULL AND ex_t <= ours_t + INTERVAL 10 MINUTE) AS matched,
           CAST(round(medianIf(dateDiff('second', ours_t, ex_t), ex_t IS NOT NULL AND ex_t <= ours_t + INTERVAL 10 MINUTE), 0) AS Nullable(Float64)) AS lead_median_s
    FROM price_match GROUP BY day_utc
),
-- 상태 기준 재현율 (2026-09-19): 거래소는 경계에서 몇 분마다 재지정한다(KRW-G 하루 39건). 전이 기준(0→N 만 셈)은 그 습관을 우리에게 요구하는 것이라
-- "지정 시각(+10분 안)에 우리 등급이 이미 > 0 이었나"도 같이 센다. 09-18 실측: 전이 기준 0.283 vs 상태 기준 0.817
ex_price_state AS (
    SELECT toDate(e.t) AS day_utc, countIf(o.level > 0) AS exchange_with_our_state
    FROM (SELECT market, t, t + INTERVAL 10 MINUTE AS t_hi FROM ex_price) AS e
    ASOF LEFT JOIN (SELECT market, event_time, level FROM {{ source('reference', 'market_alerts') }} WHERE alert_type = 'PRICE_24H') AS o
        ON o.market = e.market AND o.event_time <= e.t_hi
    GROUP BY day_utc
),
ex_price_day AS (
    SELECT toDate(t) AS day_utc, count() AS exchange FROM ex_price GROUP BY day_utc
),
ours_vol AS (
    SELECT v.market, v.day_utc FROM {{ ref('int_volume_surge_daily') }} AS v
    LEFT JOIN new_listing AS n ON n.market = v.market
    WHERE v.flagged AND (n.market = '' OR toDateTime(v.day_utc) + INTERVAL 1 DAY >= n.eligible_from)
),
ex_vol AS (
    SELECT market, toDate(trigger_time_utc) - 1 AS day_utc FROM {{ source('reference', 'upbit_market_event_records') }} FINAL
    WHERE event_type = 'TRADING_VOLUME_SOARING' AND toHour(trigger_time_utc) = 1
    GROUP BY market, day_utc
),
vol_day AS (
    SELECT o.day_utc, 'VOLUME_24H' AS rule, count() AS ours, countIf(e.market != '') AS matched, CAST(NULL AS Nullable(Float64)) AS lead_median_s
    FROM ours_vol o LEFT JOIN ex_vol e ON o.market = e.market AND o.day_utc = e.day_utc GROUP BY o.day_utc
),
ex_vol_day AS (SELECT day_utc, count() AS exchange FROM ex_vol GROUP BY day_utc)
SELECT p.day_utc AS day_utc, p.rule AS rule, p.ours AS ours, p.matched AS matched, x.exchange AS exchange,   -- 별칭 필수: p.day_utc 는 열 이름이 'p.day_utc' 가 되어 ORDER BY (day_utc, rule) 실패
       round(if(p.ours > 0, p.matched / p.ours, 0), 3)      AS precision,
       round(if(x.exchange > 0, p.matched / x.exchange, 0), 3) AS recall,
       CAST(round(if(x.exchange > 0, st.exchange_with_our_state / x.exchange, 0), 3) AS Nullable(Float64)) AS state_recall,
       p.lead_median_s AS lead_median_s
FROM price_day AS p LEFT JOIN ex_price_day AS x ON x.day_utc = p.day_utc LEFT JOIN ex_price_state AS st ON st.day_utc = p.day_utc   -- ClickHouse: USING 두 번 불가
UNION ALL
SELECT v.day_utc AS day_utc, v.rule AS rule, v.ours AS ours, v.matched AS matched, x.exchange AS exchange,
       round(if(v.ours > 0, v.matched / v.ours, 0), 3) AS precision,
       round(if(x.exchange > 0, v.matched / x.exchange, 0), 3) AS recall,
       CAST(NULL AS Nullable(Float64)) AS state_recall,
       v.lead_median_s AS lead_median_s
FROM vol_day AS v LEFT JOIN ex_vol_day AS x ON x.day_utc = v.day_utc
