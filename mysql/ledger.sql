-- ============================================
--   2층 원장 (docs/28 B): Binance Spot Testnet 주문 생애주기를 담는 트랜잭션 테이블 4개 (2026-09-19)
--   왜: 1층 체결은 INSERT 만 있어 CDC 가 큐와 같았다. 상태가 바뀌는 행(주문)을 캡처해 하류에서 최종 상태를 재구성하는 것이 2층의 목적.
--   상태를 바꾸는 주체는 거래소 매칭 엔진(실돈 없는 테스트넷). 우리가 만든 것은 주문 규칙뿐.
--   파티션 없음: 하루 수백 행. 시각 포함 키(A 설계)는 파티션 프루닝용이었으므로 여기선 PK 만. 보존 = 테스트넷 리셋(약 월 1회, 물리 DELETE) 에 따른다.
-- ============================================
USE crypto_db;

-- 거래소가 보낸 원문 (append). 재구성·대조의 정답 원천. dedup_key 로 재수신(at-least-once) 흡수.
CREATE TABLE IF NOT EXISTS binance_user_events (
    event_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_type    VARCHAR(32)  NOT NULL COMMENT 'executionReport | outboundAccountPosition | balanceUpdate | listStatus | ...',
    dedup_key     VARCHAR(64)  NOT NULL COMMENT 'executionReport: exec:<I> / 그 외: <type>:<E>',
    event_ms      BIGINT       NOT NULL COMMENT 'E (거래소 이벤트 시각)',
    symbol        VARCHAR(20)  NULL,
    order_id      BIGINT       NULL,
    exec_type     VARCHAR(24)  NULL COMMENT 'x: NEW|TRADE|CANCELED|REPLACED|REJECTED|EXPIRED|TRADE_PREVENTION',
    order_status  VARCHAR(24)  NULL COMMENT 'X',
    raw           JSON         NOT NULL,
    recv_ms       BIGINT       NOT NULL COMMENT '우리 수신 시각',
    created_at    TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (event_id),
    UNIQUE KEY uq_dedup (dedup_key),
    KEY idx_order (order_id),
    KEY idx_event_ms (event_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 주문 = 변경되는 행 (거울 모드). 상태 전이가 있으므로 CDC 가 UPDATE·DELETE 를 실어 나른다.
CREATE TABLE IF NOT EXISTS virtual_orders (
    order_id        BIGINT        NOT NULL COMMENT 'Binance orderId (i)',
    symbol          VARCHAR(20)   NOT NULL,
    client_order_id VARCHAR(64)   NOT NULL COMMENT 'c - 우리가 만든 id: <strategy>-<yyyymmdd>-<n>',
    side            ENUM('BUY','SELL') NOT NULL,
    order_type      VARCHAR(20)   NOT NULL,
    time_in_force   VARCHAR(8)    NULL,
    price           DECIMAL(20,8) NOT NULL,
    orig_qty        DECIMAL(20,8) NOT NULL,
    executed_qty    DECIMAL(20,8) NOT NULL DEFAULT 0 COMMENT 'z 누적',
    cum_quote_qty   DECIMAL(24,8) NOT NULL DEFAULT 0 COMMENT 'Z 누적',
    status          VARCHAR(24)   NOT NULL COMMENT 'X: NEW|PARTIALLY_FILLED|FILLED|CANCELED|EXPIRED|REJECTED|EXPIRED_IN_MATCH',
    last_exec_type  VARCHAR(24)   NOT NULL COMMENT 'x 마지막',
    reject_reason   VARCHAR(32)   NULL,
    strategy        VARCHAR(32)   NOT NULL,
    fill_count      INT           NOT NULL DEFAULT 0,
    created_ms      BIGINT        NOT NULL COMMENT 'O 주문 생성 시각(거래소)',
    updated_ms      BIGINT        NOT NULL COMMENT 'E 마지막 반영 이벤트 시각',
    last_exec_id    BIGINT        NOT NULL COMMENT 'I 마지막 반영 이벤트',
    version         INT UNSIGNED  NOT NULL DEFAULT 1 COMMENT '우리 갱신 순번 - ClickHouse ReplacingMergeTree 버전',
    reset_epoch     INT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '테스트넷 리셋 세대(리셋마다 +1)',
    created_at      TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at      TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (order_id),
    UNIQUE KEY uq_client (client_order_id),
    KEY idx_symbol_created (symbol, created_ms),
    KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 체결 = 불변 행 (보관소 모드). 1층 체결과 같은 성격.
CREATE TABLE IF NOT EXISTS virtual_fills (
    symbol           VARCHAR(20)   NOT NULL,
    fill_id          BIGINT        NOT NULL COMMENT 't Binance trade id (심볼 내 유일)',
    order_id         BIGINT        NOT NULL,
    side             ENUM('BUY','SELL') NOT NULL,
    price            DECIMAL(20,8) NOT NULL COMMENT 'L',
    qty              DECIMAL(20,8) NOT NULL COMMENT 'l',
    quote_qty        DECIMAL(24,8) NOT NULL COMMENT 'Y',
    commission       DECIMAL(20,8) NOT NULL COMMENT 'n',
    commission_asset VARCHAR(10)   NULL     COMMENT 'N',
    is_maker         TINYINT(1)    NOT NULL COMMENT 'm',
    filled_ms        BIGINT        NOT NULL COMMENT 'T',
    exec_id          BIGINT        NOT NULL COMMENT 'I',
    strategy         VARCHAR(32)   NOT NULL,
    created_at       TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (symbol, fill_id),
    KEY idx_order (order_id),
    KEY idx_filled (filled_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 잔고 스냅샷 (일 1회 + 리셋 감지). 대조용.
CREATE TABLE IF NOT EXISTS virtual_positions (
    as_of_day    DATE          NOT NULL,
    asset        VARCHAR(32)   NOT NULL COMMENT '테스트넷 자산명은 10자를 넘는다(실측 09-19)',
    free         DECIMAL(24,8) NOT NULL,
    locked       DECIMAL(24,8) NOT NULL,
    snapshot_ms  BIGINT        NOT NULL,
    reset_epoch  INT UNSIGNED  NOT NULL DEFAULT 0,
    created_at   TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (as_of_day, asset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3자 대조 결과 (거래소 REST vs MySQL, 생성기가 시간당 기록). ClickHouse 쪽 수는 dbt 가 이 행에 붙여 3자를 완성한다 (docs/28 B-5).
-- 왜 생성기가 하나: 거래소 조회에 키가 필요하고 키는 한 컨테이너에만 둔다. 결과는 원장처럼 CDC 로 흘러 ClickHouse 에 닿는다.
CREATE TABLE IF NOT EXISTS ledger_reconcile (
    reconciled_ms   BIGINT       NOT NULL,
    as_of_day       DATE         NOT NULL,
    symbol          VARCHAR(20)  NOT NULL,
    ex_orders       INT          NOT NULL COMMENT '거래소 allOrders(당일) 수',
    ex_filled       INT          NOT NULL,
    ex_canceled     INT          NOT NULL,
    ex_open         INT          NOT NULL,
    ex_exec_qty     DECIMAL(24,8) NOT NULL COMMENT '거래소 executedQty 합',
    ex_trades       INT          NOT NULL COMMENT '거래소 myTrades(당일) 수',
    ex_trade_qty    DECIMAL(24,8) NOT NULL,
    my_orders       INT          NOT NULL,
    my_filled       INT          NOT NULL,
    my_canceled     INT          NOT NULL,
    my_open         INT          NOT NULL,
    my_exec_qty     DECIMAL(24,8) NOT NULL,
    my_trades       INT          NOT NULL,
    my_trade_qty    DECIMAL(24,8) NOT NULL,
    mismatch        TINYINT(1)   NOT NULL,
    detail          VARCHAR(512) NULL,
    PRIMARY KEY (as_of_day, symbol, reconciled_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- C. 케이스 (docs/28 C, 2026-09-19): 확인이 필요한 건을 자동 생성(Airflow cases_hourly)하고 사람이 판정한다. 상태가 바뀌는 행 → CDC 거울 모드 두 번째 사례.
-- 판정은 SQL 한 줄: UPDATE cases SET status='closed', verdict='true_positive', note='...', updated_ms=UNIX_TIMESTAMP()*1000, version=version+1 WHERE case_id=N;
CREATE TABLE IF NOT EXISTS cases (
    case_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    case_type    VARCHAR(40)  NOT NULL COMMENT 'LEDGER_MISMATCH | TESTNET_RESET | MARKET_FLAG_ON_TRADED_COIN',
    subject      VARCHAR(40)  NOT NULL COMMENT '심볼·마켓',
    evidence_key VARCHAR(120) NOT NULL COMMENT '같은 근거로 두 번 열지 않는다',
    evidence     JSON         NOT NULL,
    opened_ms    BIGINT       NOT NULL,
    status       ENUM('open','reviewing','closed') NOT NULL DEFAULT 'open',
    verdict      ENUM('unknown','true_positive','false_positive') NOT NULL DEFAULT 'unknown',
    note         VARCHAR(500) NULL,
    assignee     VARCHAR(40)  NULL,
    updated_ms   BIGINT       NOT NULL,
    version      INT UNSIGNED NOT NULL DEFAULT 1,
    created_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (case_id),
    UNIQUE KEY uq_evidence (evidence_key),
    KEY idx_status (status), KEY idx_opened (opened_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2026-09-20 (docs/34 #1): producer 전용 사용자 - root 대신 crypto_trades INSERT/SELECT 만. 비밀번호는 .env PRODUCER_MYSQL_PASSWORD (여기엔 안 적는다)
-- CREATE USER 'producer'@'%' IDENTIFIED BY '<.env>'; GRANT SELECT, INSERT ON crypto_db.crypto_trades TO 'producer'@'%';
