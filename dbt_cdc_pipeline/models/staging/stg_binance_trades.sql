{{ config(materialized='view') }}
-- Binance 체결 스테이징 (docs/34 #4). FINAL: RMT(recv_ms) 재연결 중복 제거는 읽는 쪽 책임.
-- taker_side: Binance 는 is_buyer_maker(메이커 방향) 로 주므로 테이커 방향으로 뒤집는다 → Upbit ask_bid 와 같은 뜻(BID = 테이커 매수).
SELECT
    symbol, trade_id, price, qty, quote_qty,
    if(is_buyer_maker = 1, 'ASK', 'BID') AS taker_side,
    fromUnixTimestamp64Milli(trade_ms) AS trade_ts, trade_ms, recv_ms, flink_ts,
    -- 2026-09-20 (docs/40 ⑥): 이 모델은 만들어 놓고 하류가 하나도 없었다(소비자들이 원본을 직접 읽었다).
    -- 이유는 Upbit 쪽과 같았다 - 소비자가 필요한 시간 축이 여기 없었다. 내주면 우회할 이유가 사라진다.
    -- 이름에 trade_ 를 붙이는 것도 같은 이유(별칭이 원본 열을 가리는 함정 회피).
    toStartOfHour(fromUnixTimestamp64Milli(trade_ms)) AS trade_hour_utc,
    toDate(fromUnixTimestamp64Milli(trade_ms))        AS trade_day_utc
FROM {{ source('raw', 'binance_trades') }} FINAL
