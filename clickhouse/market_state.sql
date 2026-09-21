-- 업비트 마켓 거래 상태 라벨. scripts/labels/poll_market_state.py 가 10분 폴링으로 전이만 적재.
--
-- 왜 필요한가: 마켓이 상장폐지되면 거래소 목록에서 사라진다. 그 순간 우리 대조 분모와 커버리지
-- 마켓 집합이 조용히 줄어드는데, 기록이 없으면 "유실"과 구분이 안 된다. 폐지는 정상이고 유실은 사고다.
-- 2026-09-20 첫 스냅샷에서 KRW-RVN(폐지 예정 10-12)·KRW-ICX(10-19) 가 이미 PREDELISTING 이었다.
--
-- 왜 REST 가 아니라 웹소켓인가 (2026-09-20 실측):
--   GET /v1/market/all?isDetails=true → market_event(warning·caution) 만. 상태 없음.
--   GET /v1/ticker, /v1/ticker/all    → 시세 26열. market_state·delisting_date 없음.
--   웹소켓 ticker                      → market_state·delisting_date·is_trading_suspended·market_warning 있음.
-- 즉 이 정보는 웹소켓에만 있다. 구독 시 코드마다 SNAPSHOT 이 한 번 오므로 체결이 없는 마켓도 상태를 준다
-- (289 요청 → 289 응답, 미응답 0 실측).
--
-- 왜 10분인가: 이 값을 쓰는 쪽(health_check 의 마켓 커버리지 판정)이 10분마다 돈다.
-- 소비자보다 오래된 상태를 주면 판정이 옛 사실로 내려진다.
CREATE TABLE IF NOT EXISTS cdc_pipeline.upbit_market_state_events
(
    observed_at          DateTime,                 -- 폴링 시각(UTC)
    market               LowCardinality(String),
    market_state         LowCardinality(String),   -- ACTIVE | PREDELISTING | DELISTED | PREVIEW (거래소 정의)
    is_trading_suspended UInt8,                    -- 거래 정지 여부
    delisting_date       Nullable(Date),           -- 폐지 예정일 (없으면 NULL)
    kind                 LowCardinality(String)    -- 'transition' | 'snapshot'(폴러 시작·하루 1회 전체) | 'gone'(목록에서 사라짐)
)
ENGINE = MergeTree
ORDER BY (market, observed_at);
