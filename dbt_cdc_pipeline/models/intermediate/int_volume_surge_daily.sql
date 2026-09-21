{{ config(
    order_by='(day_utc, market)',
    post_hook="INSERT INTO cdc_pipeline.market_alerts (alert_type, market, level, prev_level, event_time, detected_at, value, threshold, ref_price, price, trade_id, rule_version)
               SELECT 'VOLUME_24H', market, 1, 0, toDateTime64(toDateTime(day_utc + 1) + INTERVAL 1 HOUR, 3), now64(3), ratio, 4, 0, 0, 0, 'v2-shadow'
               FROM {{ this }}
-- 2026-09-20 (docs/34 #5) 타입 규약: 금액·수량 합계는 Decimal, 비율은 Float64.
--   이유: Decimal 나눗셈은 분모가 0 이면 예외를 던져 모델 전체가 실패한다(Float64 는 조용히 inf). if(v>0, a/v, 0) 가드도 ClickHouse 가 양쪽 분기를 다 계산해 소용없다(09-20 실측).
--   비율은 어차피 근사라 Float64 가 의미상으로도 맞다.
-- 2026-09-20 (docs/34 #2): crypto_trades 는 ReplacingMergeTree - 중복은 '결국' 지워지므로 읽는 쪽이 FINAL 로 보장한다(재시작 뒤 머지 전 배치가 중복을 세지 않게)
               WHERE flagged AND day_utc >= today() - 2
                 AND (market, day_utc) NOT IN (SELECT market, toDate(event_time - INTERVAL 1 HOUR) - 1 FROM cdc_pipeline.market_alerts WHERE alert_type = 'VOLUME_24H')"
) }}
-- post_hook: 플래그를 market_alerts 에 '지정' 전이로 남긴다(event_time = 다음날 01:00 UTC, 거래소 지정 시각과 같은 단위). 재실행 멱등(NOT IN).
-- VOLUME_24H 규칙 (docs/16 §4-2, docs/22): 전일 거래대금 ≥ 직전 7일 평균 × 4 AND ≥ 10억.
-- 배치 규칙을 dbt 모델로 두는 이유: 거래소도 매일 01:00 UTC 일괄 지정한다(하루 단위 사건). 스트림이 낄 자리가 없다.
-- 임계 근거: 6개월 1,981 라벨 대비 7일 평균 ×4 + 하한 10억 → 정밀도 0.79 / 재현율 0.80. 30일 평균은 0.57/0.60 이라 기각.
-- 데이터 출처는 우리 체결(crypto_trades) 이고 거래소 일봉이 아니다 - 우리 파이프라인이 낸 값으로 판정해야 파이프라인의 규칙이다.
WITH daily AS (
    SELECT market, toDate(source_ts) AS day_utc, sum(trade_amount) AS amount
    -- 2026-09-20 (docs/40 ①): 원본 직접 읽기 → stg_trades 경유. 술어가 모델마다 달랐던 것을 한 곳으로 모은다.
    -- 날짜 기준은 바꾸지 않았다 - 한 번에 한 가지만 바꾼다(변화가 섞이면 무엇 때문인지 못 가린다).
    FROM {{ ref('stg_trades') }}
    WHERE source_ts >= toStartOfDay(now() - INTERVAL 40 DAY)
    GROUP BY market, day_utc
),
w AS (
    SELECT
        market, day_utc, amount,
        avg(amount) OVER (PARTITION BY market ORDER BY day_utc ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg7,
        count() OVER (PARTITION BY market ORDER BY day_utc ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)     AS days_in_window
    FROM daily
)
SELECT
    market, day_utc, amount, round(avg7, 0) AS avg7,
    round(ifNull(toFloat64(amount) / nullIf(toFloat64(avg7), 0), 0), 2)       AS ratio,
    days_in_window,
    (days_in_window >= 5 AND avg7 > 0 AND ifNull(toFloat64(amount) / nullIf(toFloat64(avg7), 0), 0) >= 4 AND amount >= 1e9) AS flagged,
    4.0                                                                        AS threshold_ratio,
    1e9                                                                        AS threshold_amount
FROM w
WHERE day_utc < today()
