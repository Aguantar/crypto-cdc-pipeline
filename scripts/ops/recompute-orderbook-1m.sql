-- orderbook_1m 재계산 (docs/58 §5-2, 2026-11-20 검증)
-- Flink OrderbookAggregator 와 같은 식. 정상 구간 04:30~04:40 에서 2,946행 19컬럼 전부 일치 확인 뒤 사용.
-- 사용: {FROM}/{TO} 를 UTC 시각으로 치환해 임시 테이블에 넣고 마켓 수·분 수 확인 → 대상 창 DELETE(mutations_sync=2) → INSERT.
--   orderbook_1m 은 MergeTree 라 삭제 없이 넣으면 중복이 생긴다.
--   flink_ts 는 재계산 시각(now64)으로 들어가므로 그 구간 e2e 지연 지표는 원래 값이 아니다.
SELECT
  market,
  toStartOfMinute(ts) AS window_start,
  window_start + INTERVAL 1 MINUTE AS window_end,
  toUInt32(count()) AS snapshots,
  argMin(mid, ts) AS mid_open,
  argMax(mid, ts) AS mid_close,
  min(mid) AS mid_min, max(mid) AS mid_max,
  avg(spread) AS spread_avg, avg(spread_bp) AS spread_bp_avg, max(spread_bp) AS spread_bp_max,
  avg(imb1) AS imb1_avg, avg(imb5) AS imb5_avg, avg(imb15) AS imb15_avg,
  avg(ask15) AS ask_depth15_avg, avg(bid15) AS bid_depth15_avg,
  avg(total_ask_size) AS total_ask_avg, avg(total_bid_size) AS total_bid_avg,
  avg(lag_ms) AS recv_lag_ms_avg,
  now64(3) AS flink_ts
FROM (
  SELECT market, ts, total_ask_size, total_bid_size,
    ask_prices[1] AS ask, bid_prices[1] AS bid,
    (ask + bid) / 2 AS mid, ask - bid AS spread, spread / mid * 10000 AS spread_bp,
    arraySum(arraySlice(ask_sizes, 1, 1)) AS a1, arraySum(arraySlice(bid_sizes, 1, 1)) AS b1,
    arraySum(arraySlice(ask_sizes, 1, 5)) AS a5, arraySum(arraySlice(bid_sizes, 1, 5)) AS b5,
    arraySum(arraySlice(ask_sizes, 1, 15)) AS ask15, arraySum(arraySlice(bid_sizes, 1, 15)) AS bid15,
    if(b1 + a1 > 0, (b1 - a1) / (b1 + a1), 0) AS imb1,
    if(b5 + a5 > 0, (b5 - a5) / (b5 + a5), 0) AS imb5,
    if(bid15 + ask15 > 0, (bid15 - ask15) / (bid15 + ask15), 0) AS imb15,
    toUnixTimestamp64Milli(recv_ts) - toUnixTimestamp64Milli(ts) AS lag_ms
  FROM cdc_pipeline.orderbook_raw
  WHERE ts >= toDateTime64('{FROM}', 3) AND ts < toDateTime64('{TO}', 3)
    AND length(ask_prices) > 0 AND length(bid_prices) > 0 AND ask_prices[1] > 0 AND bid_prices[1] > 0
)
GROUP BY market, window_start
