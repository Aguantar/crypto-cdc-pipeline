{{ config(order_by='(market, upbit_timestamp)', query_settings={'max_memory_usage': 1200000000, 'max_bytes_before_external_group_by': 600000000, 'max_bytes_before_external_sort': 600000000}) }}
-- 2026-09-20 (docs/34 #2): crypto_trades 는 ReplacingMergeTree - 중복은 '결국' 지워지므로 읽는 쪽이 FINAL 로 보장한다(재시작 뒤 머지 전 배치가 중복을 세지 않게)
-- PRICE_24H 를 SQL 로 재계산한 등급 전이 (docs/22 §4 정정). dq_alert_parity_daily 가 이것을 Flink 출력과 대조한다.
-- 왜 별도 테이블인가: ClickHouse 는 CTE 를 인라인해 무거운 창 함수를 대조 단계마다 다시 계산한다(실측 메모리 1.12GiB 초과). 한 번 계산해 저장. 임계 자체의 근거는 docs/16(6개월 역검증)이고,
-- 섀도가 확인할 것은 "구현이 그 정의와 같은 답을 내는가"다. 이 표에서 전이 10건 이상이 전부 일치하면 승격한다.
-- 재계산 정의(Flink 와 동일): 분 종가 = 그 분의 마지막 체결가(upbit_timestamp, sequential_id 순), 빈 분은 직전 종가로 채움,
--   참조 = 체결 분 − 1,440분의 종가, 변화율 = 체결가/참조 − 1, 등급 = |변화율| ≥ 2.0→3, ≥1.0→2, ≥0.5→1, 그 외 0,
--   늦은 이벤트(source_ts − upbit_timestamp > 60s)는 종가·판정 모두에서 제외, op='c' 만.
-- 알려진 차이: Flink 는 도착 순서로 상태를 갱신하므로 재정렬(최대 4.8s, docs/20)된 체결은 분 경계에서 참조가 한 칸 다를 수 있다 → 매칭 허용 ±60초.
-- 창: 최근 {{ var('parity_days', 2) }}일(UTC). 창 시작 시점의 등급은 Flink 의 마지막 전이(market_alerts)로 시드한다 - 대조 대상은 창 안의 전이.
{% set v2_start = var('flink_v2_start', '2026-09-17 01:46:00') %}
{% set days = var('parity_days', 2) %}
{# 창 경계는 컴파일 시점에 상수화한다 - ClickHouse 스칼라 서브쿼리는 Nullable 이라 numbers()·배열 인덱스에 쓸 수 없다 #}
{% set q = "SELECT toString(greatest(toDateTime('" ~ v2_start ~ "') + INTERVAL 1 DAY, toStartOfDay(now()) - INTERVAL " ~ days ~ " DAY)) AS f, toString(toStartOfDay(now())) AS t, toString(toUnixTimestamp(greatest(toDateTime('" ~ v2_start ~ "') + INTERVAL 1 DAY, toStartOfDay(now()) - INTERVAL " ~ days ~ " DAY))) AS fu, toString(toUnixTimestamp(toStartOfDay(now()))) AS tu" %}
{% if execute %}
  {% set r = run_query(q) %}
  {% set win_from = r.columns[0][0] %}{% set win_to = r.columns[1][0] %}
  {% set from_unix = r.columns[2][0] | int %}{% set to_unix = r.columns[3][0] | int %}
{% else %}
  {% set win_from = '2026-01-01 00:00:00' %}{% set win_to = '2026-01-02 00:00:00' %}{% set from_unix = 1767225600 %}{% set to_unix = 1767312000 %}
{% endif %}
{% set m0 = ((from_unix - 86400) // 60) %}
{% set m1 = (to_unix // 60) %}
{# 워밍업(배포 후 24h)이 끝나기 전엔 창이 비어 있다(win_from > win_to). 그 경우 격자 0 → 빈 표 #}
{% set span = (m1 - m0) if m1 > m0 else 0 %}

WITH
-- 1) 분 종가 (창 시작 24h 전부터)
closes AS (
    SELECT market, intDiv(upbit_timestamp, 60000) AS m,
           argMax(trade_price, (upbit_timestamp, sequential_id)) AS close
    -- 2026-09-20 (docs/40 ①): 원본 직접 읽기 → stg_trades 경유(술어를 한 곳에서만 정의). 날짜·필터 기준은 그대로.
    FROM {{ ref('stg_trades') }}
    WHERE 1 = 1
      AND toUnixTimestamp64Milli(source_ts) - upbit_timestamp <= 60000
      AND upbit_timestamp >= {{ (from_unix - 86400) * 1000 }}
      AND upbit_timestamp <  {{ to_unix * 1000 }}
    GROUP BY market, m
),
-- 2) 마켓별 분 격자 + forward fill (arrayFill: 값 없는 분(0)은 직전 값으로)
filled AS (
    SELECT market,
           toInt64({{ m0 }}) AS m0,
           arrayFill(x -> x > 0, arrayMap(t -> t.2, arraySort(t -> t.1, groupArray((m, c))))) AS cs
    FROM (
        SELECT g.market, g.m, coalesce(c.close, 0) AS c
        FROM (SELECT mk.market, toInt64({{ m0 }}) + toInt64(n.number) AS m
              FROM (SELECT DISTINCT market FROM closes) AS mk
              CROSS JOIN numbers({{ span }}) AS n) AS g
        LEFT JOIN closes c ON c.market = g.market AND c.m = g.m
    )
    GROUP BY market
),
-- 2b) 배열을 다시 행으로: (market, 분, 채워진 종가). 배열을 체결에 조인하면 행마다 배열이 복제돼 메모리가 터진다(실측 1.84GiB) → 행으로 풀어 조인
refs AS (
    SELECT market, m0 + toInt64(idx) - 1 AS m, c AS close
    FROM filled
    ARRAY JOIN cs AS c, arrayEnumerate(cs) AS idx
    WHERE c > 0
),
-- 3) v2.1: 판정 단위 = 분 종가 (docs/22 §4-2). 창 안의 분마다 (채워진 종가 / 24h 전 채워진 종가 − 1) 로 등급.
--    체결 단위(v2)는 임계 근처에서 초 단위로 플래핑했다(LSK 하루 0→1 전이 86회 vs 거래소 9회). 정답 해상도(분)·임계 근거(1분봉)·참조(분 종가)에 맞춘다.
leveled AS (
    SELECT cur.market, cur.m, cur.close, r.close AS ref,
           toInt16(multiIf(abs(cur.close / r.close - 1) >= 2.0, 3,
                           abs(cur.close / r.close - 1) >= 1.0, 2,
                           abs(cur.close / r.close - 1) >= 0.5, 1, 0)) AS lvl
    FROM refs AS cur
    INNER JOIN refs AS r ON r.market = cur.market AND r.m = cur.m - 1440
    WHERE cur.m >= intDiv({{ from_unix }}, 60) AND cur.m < intDiv({{ to_unix }}, 60)
),
-- 창 시작 시점 등급 시드 = Flink 의 마지막 전이
seed AS (
    SELECT market, toInt16(argMax(level, event_time)) AS lvl0
    FROM {{ source('reference', 'market_alerts') }}
    WHERE alert_type = 'PRICE_24H' AND event_time < toDateTime('{{ win_from }}')
    GROUP BY market
),
sql_transitions AS (
    SELECT market, upbit_timestamp, prev, lvl
    FROM (
        -- 첫 행의 prev 는 시드(창 이전 Flink 마지막 등급, 없으면 0). lagInFrame 의 기본값은 NULL 이 아니라 0 이라 센티널(-99)로 구분
        SELECT l.market, (l.m + 1) * 60000 AS upbit_timestamp, l.lvl,        -- event_time = 분 끝 (Flink 와 동일)
               if(lagv = -99, coalesce(sd.lvl0, toInt16(0)), lagv) AS prev
        FROM (
            SELECT market, m, lvl,
                   lagInFrame(lvl, 1, toInt16(-99)) OVER (PARTITION BY market ORDER BY m
                                                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lagv
            FROM leveled
        ) AS l
        LEFT JOIN seed AS sd ON sd.market = l.market
    )
    WHERE lvl != prev
)
SELECT market, upbit_timestamp, fromUnixTimestamp64Milli(upbit_timestamp) AS event_time, prev, lvl,
       toDateTime('{{ win_from }}') AS window_from, now() AS computed_at
FROM sql_transitions
