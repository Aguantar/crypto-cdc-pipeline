-- 유실 게이트: 대상일(var reconcile_date, 기본 어제 UTC)에 거래소 거래량 대비 99% 미만인 마켓×시간 셀이 없어야 한다.
-- 99%: docs/13 수정 이후 6일 연속 가중 100.0%, 셀 단위 최소 99% 이상 관측. 거래소 봉 경계 오차 허용치.
-- 반환 행 = 실패 셀. 원인 조사 순서: 수집기 구독 누락 → producer 버퍼 → MySQL 유니크 충돌 → CDC 이후 구간(docs/13 §단계별 좁히기).
{% set day = var('reconcile_date', (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).isoformat()) %}
SELECT market, hour_utc, ch_vol, candle_vol, round(ratio_pct, 2) AS ratio_pct, ch_n
FROM {{ ref('int_reconcile_hourly') }}
WHERE toDate(hour_utc) = toDate('{{ day }}')
  AND candle_vol > 0
  AND ratio_pct < 99
