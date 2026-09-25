{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='hour_kst',
        on_schema_change='fail',
        order_by='market, hour_kst'
    )
}}

-- 2026-09-25 (docs/44 §8): table -> incremental. 매일 전체 이력(1.28억 행, FINAL)을 다시 묶는 것이 500MiB 였고,
--   서버 총 한도(1.57GiB) = 기준선 700MiB + 머지 버스트(최대 600MiB) + 이 쿼리가 겹치는 순간 하루 두 번 죽었다
--   (09-24 00:49, 16:01). 최근 KST 이틀치 시간봉만 다시 만들고 그 시간(hour_kst)의 행을 지우고 넣는다.
--   --vars run_date 를 주면 그 KST 날(과 전날)만. 결과는 전체 재계산과 같아야 한다 - 교체 전 실측으로 확인.

-- 2026-09-24 (docs/44 §4): 메모리 처방을 붙였다가 뗐다. 09-24 00:49 실행이 code 241(서버 총 한도)로 죽어
--   max_bytes_before_external_group_by 스필을 붙였는데, 그 다음 실행(01:14)은 스필 조각 11,562개를 다시 합치다
--   1.12GiB/105초로 더 크게 죽었다. 설정 없는 실행은 query_log 기준 450~620MiB/15~19초로 통과한다(09-20 이후 12회).
--   00:49 의 초과분은 이 쿼리가 아니라 같은 시각의 다른 부하(머지 버스트)였다. spill 은 답이 아니었고 답은 위의 증분이다.
--   참고: 09-11~09-19 는 55MiB/10초였고 09-20 stg_trades 에 FINAL 이 붙은 뒤 10배가 됐다(docs/44 §4).
--   (주석을 config 블록 안에 넣으면 Jinja 가 깨진다 - docs/34 §2 에 적어 두고 09-24 에 또 그랬다)

-- 2026-09-20 (docs/34 #5) 타입 규약: 금액·수량 합계는 Decimal, 비율은 Float64.
--   이유: Decimal 나눗셈은 분모가 0 이면 예외를 던져 모델 전체가 실패한다(Float64 는 조용히 inf). if(v>0, a/v, 0) 가드도 ClickHouse 가 양쪽 분기를 다 계산해 소용없다(09-20 실측).
--   비율은 어차피 근사라 Float64 가 의미상으로도 맞다.
-- 1시간봉 OHLCV 집계
-- 2026-09-19 Flink 5분 처리 시간 집계는 폐기(정지 뒤 따라붙는 행이 '지금' 창을 왜곡, docs/29 §7), 표는 09-20 삭제. 이벤트 시각 집계는 여기서만 만든다:
--   Flink = 실시간 5분 윈도우 (스트리밍)
--   DBT  = 배치 1시간봉 (raw 틱 데이터 기반, 더 정확한 OHLCV)
SELECT
    market,
    toStartOfHour(trade_time_kst) AS hour_kst,
    argMin(trade_price, trade_time_kst) AS open,
    max(trade_price) AS high,
    min(trade_price) AS low,
    argMax(trade_price, trade_time_kst) AS close,
    sum(trade_volume) AS volume,
    sum(trade_amount) AS amount,
    count(*) AS trade_count,
    countIf(ask_bid = 'BID') AS bid_count,
    countIf(ask_bid = 'ASK') AS ask_count,
    -- VWAP (거래량 가중 평균 가격)
    ifNull(toFloat64(sum(trade_amount)) / nullIf(toFloat64(sum(trade_volume)), 0), 0) AS vwap
FROM {{ ref('stg_trades') }}
{% if is_incremental() %}
-- 파티션 키(upbit_timestamp) 로 잘라야 월 파티션이 통째로 읽히지 않는다. KST 날 D = [D-1 15:00Z, D 15:00Z).
--   run_anchor() 는 run_date 의 00:00Z(없으면 now()). 33시간 전 = 전날 KST 시작, 시간 경계에 정확히 맞는다.
WHERE upbit_timestamp >= toUnixTimestamp(toStartOfDay({{ run_anchor() }}) - INTERVAL 33 HOUR) * 1000
  {% if var('run_date', none) %}AND upbit_timestamp < toUnixTimestamp(toStartOfDay({{ run_anchor() }}) + INTERVAL 15 HOUR) * 1000{% endif %}
{% endif %}
GROUP BY market, toStartOfHour(trade_time_kst)
