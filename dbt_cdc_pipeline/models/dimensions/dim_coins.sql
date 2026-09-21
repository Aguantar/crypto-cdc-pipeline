{{ config(materialized='table', order_by='coin_id') }}
-- 코인 차원 (docs/34 #4): 두 거래소의 마켓 키(KRW-BTC / BTCUSDT)를 하나의 coin_id 로 잇는다.
-- 왜: 교차 거래소 조인을 문자열 치환으로 하면 스테이블(USDC/USD1)·리브랜딩(MANTRA↔OM)에서 깨진다. base/quote 는 거래소가 준 값(binance_symbols)과 마켓 마스터에서.
-- coin_id = Upbit 기준 자산 코드(KRW- 뒤). 별칭은 seed coin_alias 로 명시(사람이 검토한 것만).
WITH upbit AS (
    SELECT market AS upbit_market, replaceOne(market, 'KRW-', '') AS upbit_base, english_name, korean_name, is_active AS upbit_active, listing_date_est
    FROM {{ source('reference', 'upbit_market_master') }} FINAL WHERE market LIKE 'KRW-%'
),
alias AS (SELECT upbit_base, binance_base FROM {{ ref('coin_alias') }}),
binance AS (
    SELECT symbol AS binance_symbol, base_asset AS binance_base, status AS binance_status
    FROM {{ source('reference', 'binance_symbols') }} FINAL WHERE quote_asset = 'USDT' AND is_spot = 1
),
joined AS (
    SELECT u.upbit_market, u.upbit_base, u.english_name, u.korean_name, u.upbit_active, u.listing_date_est,
           -- ClickHouse 의 LEFT JOIN 은 NULL 이 아니라 빈 문자열을 주므로 coalesce 가 아니라 if(!= '')
           if(a.binance_base != '', a.binance_base, u.upbit_base) AS binance_base_expected
    FROM upbit AS u LEFT JOIN alias AS a ON a.upbit_base = u.upbit_base
)
SELECT
    j.upbit_base AS coin_id, j.upbit_market AS upbit_market, b.binance_symbol AS binance_symbol,
    j.upbit_base AS upbit_base, b.binance_base AS binance_base, j.english_name AS english_name, j.korean_name AS korean_name,
    -- CAST(... AS UInt8): LowCardinality 열의 비교 결과는 LowCardinality(UInt8) 가 되어 테이블 생성이 거부된다(docs/22 와 같은 함정)
    CAST(j.upbit_active AS UInt8) AS upbit_active, CAST(toString(b.binance_status) = 'TRADING' AS UInt8) AS binance_active,
    CAST(toString(b.binance_symbol) != '' AS UInt8) AS on_both_venues, j.listing_date_est AS upbit_listing_date_est,
    CAST(j.upbit_base IN (SELECT upbit_base FROM alias) AS UInt8) AS has_alias
FROM joined AS j
LEFT JOIN binance AS b ON b.binance_base = j.binance_base_expected
