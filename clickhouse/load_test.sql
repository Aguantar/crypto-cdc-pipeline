-- 부하 실험 격리 테이블 (docs/15 §4). 구조는 프로덕션과 동일(AS), TTL 만 1일로 짧게 - 실험 결과는 measure.py CSV 와 결과 문서에 남고 원본 행은 보존 가치가 없다.
-- 프로덕션 테이블·마트·dbt 는 이 테이블을 읽지 않는다 (source 미등록). 권한은 pipeline 의 cdc_pipeline.* 등급 grant 가 이미 덮는다(docs/21).
CREATE TABLE IF NOT EXISTS cdc_pipeline.load_test_crypto_trades AS cdc_pipeline.crypto_trades;
ALTER TABLE cdc_pipeline.load_test_crypto_trades MODIFY TTL toDateTime(source_ts) + INTERVAL 1 DAY;
CREATE TABLE IF NOT EXISTS cdc_pipeline.load_test_trade_aggregations AS cdc_pipeline.trade_aggregations;
ALTER TABLE cdc_pipeline.load_test_trade_aggregations MODIFY TTL toDateTime(window_start) + INTERVAL 1 DAY;
CREATE TABLE IF NOT EXISTS cdc_pipeline.load_test_orderbook_raw AS cdc_pipeline.orderbook_raw;
ALTER TABLE cdc_pipeline.load_test_orderbook_raw MODIFY TTL toDateTime(ts) + INTERVAL 1 DAY;
CREATE TABLE IF NOT EXISTS cdc_pipeline.load_test_orderbook_1m AS cdc_pipeline.orderbook_1m;
ALTER TABLE cdc_pipeline.load_test_orderbook_1m MODIFY TTL toDateTime(window_start) + INTERVAL 1 DAY;
