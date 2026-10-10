-- 2026-10-10 (docs/44 §13): severity warn. 이 테스트가 09-28·10-01·10-07 세 번 일일 리포트를 막았는데 셋 다 거짓 경보였다.
--   걸린 행은 KRW-HP·ELF·ASTR·EURC 의 새벽 3~6시간 공백이고, 그 시간엔 거래소 캔들에도 체결이 없었다(그날 대조 0행 셀 0, 최소 셀 100%).
--   하루 1,000건 마켓도 새벽엔 서너 시간 조용할 수 있다. 파이프라인 공백인지는 거래소 캔들이 있어야 가르고,
--   그 캔들은 다음날 06:35Z 대조가 받는다. 이 테스트가 도는 16:00Z 에는 그날 캔들이 없으므로 여기서 리포트를 막을 근거가 없다.
--   그래서 경고로 낮춘다. 유실 판정은 대조(dq_reconcile_daily)의 몫이다.
{{ config(severity='warn') }}
-- 최근 24시간 내 연속 3시간 이상 데이터 누락 체크 - 유동성 있는 마켓만(2026-09-20 정정, docs/34 #3)
-- 원래 전제 '코인은 24시간 거래'는 5코인 시절 것. 287마켓엔 하루 100~300건짜리 스테이블(KRW-USDS·RLUSD·USDE)이 있어 3~5시간 공백이 정상이다.
-- 파이프라인 공백은 커버리지 체크(거래소 대비)와 대조가 잡는다. 이 테스트는 하루 1,000건 이상 마켓의 3시간 공백만 본다.
-- 첫 행(이전 데이터 없음)은 제외
-- 검사 범위: int_ohlcv_1h에 실제 존재하는 마지막 시각까지 (dbt run 이후 미집계 구간 제외)
WITH max_hour AS (
    SELECT max(hour_kst) AS last_hour
    FROM {{ ref('int_ohlcv_1h') }}
),
liquid AS (
    SELECT market FROM {{ ref('int_ohlcv_1h') }}
    WHERE hour_kst >= (SELECT last_hour FROM max_hour) - INTERVAL 24 HOUR
    GROUP BY market HAVING sum(trade_count) >= 1000
),
hourly_exists AS (
    SELECT
        market,
        hour_kst,
        dateDiff('hour', lagInFrame(hour_kst) OVER (
            PARTITION BY market ORDER BY hour_kst
        ), hour_kst) AS gap_hours
    FROM {{ ref('int_ohlcv_1h') }}
    WHERE hour_kst >= (SELECT last_hour FROM max_hour) - INTERVAL 24 HOUR
      AND hour_kst <= (SELECT last_hour FROM max_hour)
      AND market IN (SELECT market FROM liquid)
)
SELECT *
FROM hourly_exists
WHERE gap_hours >= 3
  AND gap_hours < 10000
