-- 커버리지 게이트: 대상일에 거래소에서 거래가 있었는데(candle_vol > 0) 우리 체결이 한 건도 없는 마켓×시간이 없어야 한다.
-- 유실 비율 테스트와 별도인 이유: 구독 누락(신규 상장·재연결 후 목록 불일치)은 비율이 아니라 '부재'로 나타난다 (docs/14 튜닝 5번).
{% set day = var('reconcile_date', (modules.datetime.date.today() - modules.datetime.timedelta(days=1)).isoformat()) %}
SELECT market, hour_utc, candle_vol
FROM {{ ref('int_reconcile_hourly') }}
WHERE toDate(hour_utc) = toDate('{{ day }}')
  AND candle_vol > 0
  AND ch_n = 0
