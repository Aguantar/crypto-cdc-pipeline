-- ============================================
--   CDC Realtime Pipeline - MySQL 스키마
--   암호화폐 실시간 체결 데이터
-- ============================================

-- Debezium CDC 사용자
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz_pass_2025';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- 데이터베이스
CREATE DATABASE IF NOT EXISTS crypto_db;
USE crypto_db;

-- ============================================
-- 암호화폐 체결(Trade) 테이블
-- ============================================
-- Upbit WebSocket에서 수신한 실시간 체결 데이터를 저장한다.
-- Debezium이 이 테이블의 binlog를 감시하여 CDC 이벤트를 생성한다.
--
-- 설계 포인트:
-- 1. trade_id: AUTO_INCREMENT로 CDC에서 고유 식별 가능
-- 2. DECIMAL(20,8): 암호화폐의 소수점 정밀도 보장 (BTC: 0.00000001)
-- 3. trade_amount: Producer에서 미리 계산하여 저장 (Flink 부담 감소)
-- 4. upbit_timestamp: Upbit 서버 체결 시각 (E2E 레이턴시 측정 기준)
-- 5. sequential_id: Upbit 고유 체결 ID (중복 INSERT 방지용)

CREATE TABLE IF NOT EXISTS crypto_trades (
    trade_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    market          VARCHAR(20)     NOT NULL COMMENT '마켓 코드 (KRW-BTC, KRW-ETH 등)',
    trade_price     DECIMAL(20,8)   NOT NULL COMMENT '체결 가격 (KRW)',
    trade_volume    DECIMAL(20,8)   NOT NULL COMMENT '체결 수량',
    trade_amount    DECIMAL(20,4)   NOT NULL COMMENT '체결 금액 (price × volume, KRW)',
    ask_bid         VARCHAR(3)      NOT NULL COMMENT 'ASK(매도) / BID(매수)',
    upbit_timestamp BIGINT          NOT NULL COMMENT 'Upbit 체결 시각 (Unix ms)',
    sequential_id   BIGINT          NOT NULL COMMENT 'Upbit 체결 고유 ID',
    created_at      TIMESTAMP(3)    DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'MySQL INSERT 시각',

    INDEX idx_market_created (market, created_at),
    INDEX idx_sequential (sequential_id),

    UNIQUE KEY uk_seq_id (sequential_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Upbit 실시간 암호화폐 체결 데이터';

-- UNIQUE KEY uk_seq_id:
--   Upbit에서 동일 체결이 WebSocket 재연결 시 중복 수신될 수 있다.
--   sequential_id를 UNIQUE로 설정하면 INSERT IGNORE로 중복을 방지할 수 있다.
--
-- INDEX idx_market_created:
--   "특정 마켓의 최근 체결 조회"가 가장 빈번한 쿼리 패턴이므로 복합 인덱스 설정.
