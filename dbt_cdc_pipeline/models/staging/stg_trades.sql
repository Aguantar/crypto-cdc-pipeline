{{
    config(
        materialized='view'
    )
}}

-- 시간 규약(docs/34 #3): 리포트·OHLCV 는 KST 하루(day_kst·hour_kst), 대조·품질(dq_*)·마트 분 단위는 UTC 하루(day_utc). 같은 이름 'day' 를 두 뜻으로 쓰지 않는다.
-- 2026-09-20 (docs/34 #2): crypto_trades 는 ReplacingMergeTree - 중복은 '결국' 지워지므로 읽는 쪽이 FINAL 로 보장한다(재시작 뒤 머지 전 배치가 중복을 세지 않게)
-- raw 틱 데이터에서 필요한 필드 추출 + 타입 정제
-- upbit_timestamp는 Int64 (Unix ms)이므로 fromUnixTimestamp64Milli()로 변환
--
-- 2026-09-20 (docs/40 ①) 열 확장: 이 모델을 쓰는 모델이 2개뿐이고 9개가 원본을 직접 읽고 있었다.
-- 이유는 단순했다 - 여기에 UTC 축과 원시 타임스탬프·op·source_ts 가 없어서 쓸 수가 없었다.
-- 필요한 열을 여기서 다 내주면 우회할 이유가 사라진다. 뷰라서 열을 늘려도 저장 비용은 0이다.
SELECT
    trade_id,
    market,
    trade_price,
    trade_volume,
    trade_amount,
    ask_bid,
    ask_bid AS taker_side,   -- docs/34 #4: Upbit ask_bid 는 테이커 방향. Binance 와 같은 뜻으로 부르는 열
    toTimeZone(
        fromUnixTimestamp64Milli(upbit_timestamp),
        'Asia/Seoul'
    ) AS trade_time_kst,
    toDate(
        toTimeZone(fromUnixTimestamp64Milli(upbit_timestamp), 'Asia/Seoul')
    ) AS day_kst,
    toHour(
        toTimeZone(fromUnixTimestamp64Milli(upbit_timestamp), 'Asia/Seoul')
    ) AS trade_hour,
    sequential_id,
    cdc_latency_ms,
    inserted_at,
    -- ↓ 우회하던 모델들이 필요로 하던 열 (UTC 축 · 원시 시각 · CDC 메타)
    op,                                                        -- 필터는 위에서 이미 걸었지만 진단용으로 남긴다
    source_ts,                                                 -- binlog 시각 = 적재 지연 계산의 기준
    cdc_ts,
    flink_ts,                                                  -- ClickHouse 적재 시각
    recv_ms, ingest_source, stream_type,                       -- 수집 경로 메타(docs/28 A)
    best_bid_price, best_bid_size, best_ask_price, best_ask_size,  -- 체결 순간의 최우선 호가
    upbit_timestamp,                                           -- 거래소 체결 시각(Unix ms). 분·시 버킷은 읽는 쪽이 만든다
    fromUnixTimestamp64Milli(upbit_timestamp) AS trade_time_utc,
    -- 이름에 trade_ 를 붙이는 이유: 하류에 `toDate(source_ts) AS day_utc`(적재 기준 하루)를 쓰는 모델이 있고,
    -- ClickHouse 는 SELECT 별칭이 같은 이름의 원본 열을 가린다(docs/34 #3 에서 한 번 당했다).
    -- 이름이 겹치지 않으면 그 함정 자체가 없어진다. 체결 기준 하루인지 적재 기준 하루인지도 이름으로 구분된다.
    toDate(fromUnixTimestamp64Milli(upbit_timestamp))       AS trade_day_utc,
    toStartOfHour(fromUnixTimestamp64Milli(upbit_timestamp)) AS trade_hour_utc
FROM {{ source('raw', 'crypto_trades') }} FINAL
WHERE trade_price > 0
  AND trade_volume > 0
  -- 2026-09-20 (docs/40 ①): 술어를 op = 'c' 에서 아래 매크로로 바꿨다.
  -- 예전 술어는 스냅샷(r 23,100)과 gap-fill(backfill 7,288)을 진짜 체결인데도 버렸다.
  -- 실측: 둘 다 c 와 trade_id 가 한 건도 안 겹친다 = 그 행에만 있는 체결.
  -- 2026-06-27 하루 −2.77%, 2026-02-13 −5.6% 가 조용히 사라지고 있었다.
  AND {{ real_trade_filter() }}
