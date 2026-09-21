-- 마켓 마스터 스냅샷 (2026-09-18, docs/26 §4). reconcile_trades DAG 가 매일 06:35 적재. dbt dim_markets 가 읽는다.
-- listing_date_est: 일봉 200일 창의 첫 봉 = 상장일 근사. 창이 꽉 차면(200봉) "그 이전"이라 source='before_window'.
CREATE TABLE IF NOT EXISTS cdc_pipeline.upbit_market_master (
  market LowCardinality(String), korean_name String, english_name String,
  market_warning LowCardinality(String), event_warning UInt8, caution_json String,
  listing_date_est Nullable(Date), listing_date_source LowCardinality(String), lookback_days UInt16,
  is_active UInt8, fetched_at DateTime
) ENGINE = ReplacingMergeTree(fetched_at) ORDER BY market;
